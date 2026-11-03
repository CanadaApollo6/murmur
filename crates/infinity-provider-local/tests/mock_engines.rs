use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::Response,
    routing::{get, post},
};
use futures_util::StreamExt;
use infinity_provider_local::{LocalProvider, config::Config};
use infinity_provider_protocol::{
    AssistantContent, CompletionRequest, ErrorClass, Message, ModelProvider, StreamChunk, ToolCall,
    ToolResult, ToolResultContent, UserContent,
    remote::{RemoteModelProvider, serve_provider},
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Clone)]
enum Reply {
    Http(u16, Value),
    Sse(Vec<(u64, String)>),
    HeadersNever,
    Keepalive,
}
struct EngineState {
    context: AtomicU64,
    models: Mutex<Vec<String>>,
    requests: Mutex<Vec<Value>>,
    replies: Mutex<VecDeque<Reply>>,
    discovery_delay: AtomicU64,
    missing_context: bool,
    no_models_route: std::sync::atomic::AtomicBool,
}
fn event(value: Value) -> String {
    format!("data: {value}\n\n")
}
fn successful() -> Reply {
    Reply::Sse(vec![
        (
            0,
            event(json!({"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]})),
        ),
        (
            0,
            event(json!({"usage":{"prompt_tokens":10,"completion_tokens":3}})),
        ),
        (0, "data: [DONE]\n\n".into()),
    ])
}
async fn models(State(s): State<Arc<EngineState>>) -> Result<Json<Value>, axum::http::StatusCode> {
    tokio::time::sleep(Duration::from_millis(
        s.discovery_delay.load(Ordering::SeqCst),
    ))
    .await;
    if s.no_models_route.load(Ordering::SeqCst) {
        return Err(axum::http::StatusCode::NOT_FOUND);
    }
    Ok(Json(
        json!({"data":s.models.lock().expect("models").iter().map(|id| {
        let mut model=json!({"id":id});
        if !s.missing_context { model["max_model_len"]=json!(s.context.load(Ordering::SeqCst)); model["context_length"]=json!(s.context.load(Ordering::SeqCst)); }
        model
    }).collect::<Vec<_>>()}),
    ))
}
async fn auxiliary(State(s): State<Arc<EngineState>>) -> Json<Value> {
    Json(
        json!({"default_generation_settings":{"n_ctx":s.context.load(Ordering::SeqCst)},"total_slots":1,"max_batch_size":1,"max_running_requests":1}),
    )
}
async fn chat(State(s): State<Arc<EngineState>>, Json(body): Json<Value>) -> Response<Body> {
    s.requests.lock().expect("requests").push(body);
    let reply = s
        .replies
        .lock()
        .expect("replies")
        .pop_front()
        .unwrap_or_else(successful);
    match reply {
        Reply::HeadersNever => std::future::pending().await,
        Reply::Http(status,value) => Response::builder().status(status).header("content-type","application/json").body(Body::from(value.to_string())).expect("HTTP error"),
        Reply::Sse(events) => Response::builder().header("content-type","text/event-stream").body(Body::from_stream(async_stream::stream! {
            for (ms,event) in events { tokio::time::sleep(Duration::from_millis(ms)).await; yield Ok::<_,std::io::Error>(Bytes::from(event)); }
        })).expect("SSE response"),
        Reply::Keepalive => Response::builder().header("content-type","text/event-stream").body(Body::from_stream(async_stream::stream! {
            loop { tokio::time::sleep(Duration::from_millis(20)).await; yield Ok::<_,std::io::Error>(Bytes::from_static(b": alive\n\ndata: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")); }
        })).expect("keepalive response"),
    }
}
struct Harness {
    state: Arc<EngineState>,
    remote: Arc<RemoteModelProvider>,
    socket: PathBuf,
    engine_task: tokio::task::JoinHandle<()>,
    provider_task: tokio::task::JoinHandle<()>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.engine_task.abort();
        self.provider_task.abort();
        if let Err(e) = std::fs::remove_file(&self.socket)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("remove socket: {e}");
        }
    }
}
impl Harness {
    async fn new(engine: &str, python: bool, extra: &str) -> Self {
        Self::with_context(engine, python, extra, 128).await
    }
    async fn with_context(
        engine: &str,
        python: bool,
        extra: &str,
        configured_context: u64,
    ) -> Self {
        let state = Arc::new(EngineState {
            context: AtomicU64::new(256),
            models: Mutex::new(vec!["weights".into()]),
            requests: Mutex::new(vec![]),
            replies: Mutex::new(VecDeque::new()),
            discovery_delay: AtomicU64::new(0),
            missing_context: python,
            no_models_route: std::sync::atomic::AtomicBool::new(false),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock listener");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let router = Router::new()
            .route("/v1/models", get(models))
            .route("/props", get(auxiliary))
            .route("/health", get(auxiliary))
            .route("/server_info", get(auxiliary))
            .route("/v1/chat/completions", post(chat))
            .with_state(state.clone());
        let engine_task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("mock engine");
        });
        let config=Config::parse(&format!("[[server]]\nname='s'\nengine='{engine}'\nbase_url='{url}'\ncontext_window={configured_context}\nslots=1\nfirst_token_timeout_secs=1\nprogress_timeout_secs=1\ndiscovery_timeout_secs=1\n{extra}\n[[server.profile]]\nid='p'\ndisplay_name='P'\n")).expect("mock config");
        let provider = Arc::new(LocalProvider::new(config).expect("provider"));
        let (socket, serve) = serve_provider(provider).expect("provider socket");
        let provider_task = tokio::spawn(serve);
        Self {
            state,
            remote: Arc::new(RemoteModelProvider::new(&socket)),
            socket,
            engine_task,
            provider_task,
        }
    }
    fn replies(&self, replies: impl IntoIterator<Item = Reply>) {
        self.state.replies.lock().expect("replies").extend(replies);
    }
    async fn wait_requests(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while self.state.requests.lock().expect("requests").len() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("request dispatched");
    }
    async fn run(
        &self,
        request: CompletionRequest,
    ) -> Result<Vec<StreamChunk>, infinity_provider_protocol::CompletionError> {
        let mut stream = self.remote.invoke_model("s/p", request).await?;
        let mut chunks = vec![];
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk?);
        }
        Ok(chunks)
    }
}
fn request() -> CompletionRequest {
    CompletionRequest {
        chat_history: vec![Message::user("hello")],
        ..Default::default()
    }
}

#[tokio::test]
async fn every_engine_flavour_crosses_the_provider_protocol() {
    for engine in ["llama.cpp", "vllm", "sglang", "tensorfold", "generic"] {
        let h = Harness::new(engine, false, "").await;
        let models = h.remote.list_models().await.expect("catalog");
        assert_eq!(models[0].context_window, 128);
        assert!(models[0].keeps_unsigned_reasoning);
        let chunks = h.run(request()).await.expect("completion");
        assert_eq!(
            chunks
                .iter()
                .filter(|c| matches!(c, StreamChunk::Final(_)))
                .count(),
            1
        );
        assert_eq!(
            h.state.requests.lock().expect("requests")[0]["model"],
            "weights"
        );
    }
}
#[tokio::test]
async fn capped_overflow_retries_inside_one_slot_with_fresh_seed() {
    let h = Harness::new("tensorfold", true, "").await;
    // TensorFold Python cuda/server.py:251-253. Formatted request and context values.
    h.replies([Reply::Http(400,json!({"error":{"message":"the rendered prompt has 100 tokens and requests 100 reply tokens, exceeding the server's 128-token context window; reduce the prompt or reply length"}})),successful()]);
    let mut req = request();
    req.max_tokens = Some(100);
    let chunks = tokio::time::timeout(Duration::from_secs(3), h.run(req))
        .await
        .expect("one permit suffices")
        .expect("uncapped success");
    assert!(matches!(chunks.last(), Some(StreamChunk::Final(_))));
    let requests = h.state.requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["max_tokens"], 100);
    assert!(requests[1].get("max_tokens").is_none());
    assert_ne!(requests[0]["seed"], requests[1]["seed"]);
}
#[tokio::test]
async fn overflow_rechecks_a_smaller_server_including_tool_results() {
    let h = Harness::new("vllm", false, "").await;
    h.run(request()).await.expect("first pass");
    h.state.context.store(64, Ordering::SeqCst);
    // vLLM v0.30.0 vllm/v1/engine/input_processor.py:481-484.
    h.replies([Reply::Http(400,json!({"error":{"message":"The decoder prompt (length 200) plus the number of requested output tokens (at least 1) is longer than the maximum model length of 64. Make sure that `max_model_len` is no smaller than the number of text tokens (prompt + requested output tokens)."}}))]);
    let mut req = request();
    req.chat_history.extend([
        Message::Assistant {
            content: vec![AssistantContent::ToolCall(ToolCall::new(
                "call",
                "read",
                json!({}),
            ))],
        },
        Message::User {
            content: vec![UserContent::ToolResult(ToolResult {
                id: "call".into(),
                call_id: None,
                content: vec![ToolResultContent::text("huge result")],
            })],
        },
    ]);
    let err = h.run(req).await.expect_err("smaller server");
    assert_eq!(err.class(), ErrorClass::Fatal);
    assert!(err.to_string().contains("64") && err.to_string().contains("128"));
    h.state.context.store(256, Ordering::SeqCst);
    h.run(request()).await.expect("mismatch not cached");
}
#[tokio::test]
async fn tensorfold_refusal_limit_overrides_missing_discovery_and_feeds_length_diagnostics() {
    let h = Harness::new("tensorfold", true, "").await;
    // TensorFold Python cuda/server.py:251-253. Formatted request and context values.
    h.replies([Reply::Http(400,json!({"error":{"message":"the rendered prompt has 100 tokens and requests 100 reply tokens, exceeding the server's 100-token safe cache capacity; reduce the prompt or reply length"}})),
        Reply::Sse(vec![(0,event(json!({"choices":[{"delta":{"content":"reply"},"finish_reason":"length"}],"usage":{"prompt_tokens":99,"completion_tokens":1}})))])]);
    let mut req = request();
    req.max_tokens = Some(100);
    let err = h
        .run(req)
        .await
        .expect_err("reply filled actual fitted window");
    assert_eq!(err.class(), ErrorClass::Fatal);
    assert!(err.to_string().contains("context window filled"));
    // TensorFold Python server/app.py:365-367.
    h.replies([Reply::Http(400,json!({"error":{"message":"This server's maximum context length is 64 tokens, but the rendered prompt has 200 tokens and leaves no room for a reply. Compact or shorten the conversation."}}))]);
    assert_eq!(
        h.run(request())
            .await
            .expect_err("undersized Python server")
            .class(),
        ErrorClass::Fatal
    );
}
#[tokio::test]
async fn discovery_timeout_and_automatic_model_resolution_are_retried() {
    let h = Harness::new("generic", false, "").await;
    h.state.discovery_delay.store(1500, Ordering::SeqCst);
    assert_eq!(
        h.run(request())
            .await
            .expect_err("discovery timeout")
            .class(),
        ErrorClass::Transient
    );
    h.state.discovery_delay.store(0, Ordering::SeqCst);
    h.state.models.lock().expect("models").push("other".into());
    let err = h.run(request()).await.expect_err("ambiguous models");
    assert_eq!(err.class(), ErrorClass::Fatal);
    assert!(err.to_string().contains("other"));
    *h.state.models.lock().expect("models") = vec!["weights".into()];
    h.run(request()).await.expect("resolve again");
    h.replies([Reply::Http(
        404,
        json!({"error":{"message":"The model `weights` does not exist.","type":"NotFoundError"}}),
    )]);
    assert!(
        h.run(request())
            .await
            .expect_err("model changed")
            .to_string()
            .contains("next request resolves it again")
    );
    *h.state.models.lock().expect("models") = vec!["new".into()];
    h.run(request()).await.expect("new model");
    assert_eq!(
        h.state
            .requests
            .lock()
            .expect("requests")
            .last()
            .expect("request")["model"],
        "new"
    );
}
#[tokio::test]
async fn queued_cancel_before_headers_and_midstream_release_permits() {
    let h = Harness::new("generic", false, "").await;
    h.replies([
        Reply::HeadersNever,
        Reply::Sse(vec![
            (
                0,
                event(json!({"choices":[{"delta":{"content":"partial"}}]})),
            ),
            (10000, event(json!({"choices":[{"finish_reason":"stop"}]}))),
        ]),
        successful(),
    ]);
    let first = h
        .remote
        .invoke_model("s/p", request())
        .await
        .expect("first");
    h.wait_requests(1).await;
    let remote = h.remote.clone();
    let queued = tokio::spawn(async move { remote.invoke_model("s/p", request()).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    queued.abort();
    drop(first);
    let mut second = tokio::time::timeout(
        Duration::from_secs(2),
        h.remote.invoke_model("s/p", request()),
    )
    .await
    .expect("released before headers")
    .expect("second");
    assert!(matches!(
        second.next().await,
        Some(Ok(StreamChunk::Text(_)))
    ));
    drop(second);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), h.run(request()))
            .await
            .expect("released midstream")
            .is_ok()
    );
}
#[tokio::test]
async fn header_keepalive_and_progress_timeouts_free_the_slot() {
    for reply in [
        Reply::HeadersNever,
        Reply::Keepalive,
        Reply::Sse(vec![
            (
                0,
                event(json!({"choices":[{"delta":{"content":"partial"}}]})),
            ),
            (10000, ": idle\n\n".into()),
        ]),
    ] {
        let h = Harness::new("tensorfold", false, "").await;
        h.replies([reply, successful()]);
        let err = tokio::time::timeout(Duration::from_secs(3), h.run(request()))
            .await
            .expect("bounded clock")
            .expect_err("timeout");
        assert_eq!(err.class(), ErrorClass::Transient);
        h.run(request()).await.expect("permit freed after timeout");
    }
}
#[tokio::test]
async fn stream_errors_keep_the_same_class_and_final_waits_for_usage() {
    let h = Harness::new("sglang", false, "").await;
    h.replies([
        Reply::Sse(vec![(
            0,
            event(json!({"object":"error","code":503,"message":"Generation aborted."})),
        )]),
        Reply::Sse(vec![(0, "data: [DONE]\n\n".into())]),
    ]);
    assert_eq!(
        h.run(request())
            .await
            .expect_err("in-stream overload")
            .class(),
        ErrorClass::Throttled
    );
    assert_eq!(
        h.run(request())
            .await
            .expect_err("DONE without finish")
            .class(),
        ErrorClass::Transient
    );
    let h = Harness::new("generic", false, "").await;
    h.replies([Reply::Sse(vec![
        (
            0,
            event(json!({"choices":[{"delta":{},"finish_reason":"stop"}]})),
        ),
        (
            0,
            event(json!({"usage":{"prompt_tokens":4,"completion_tokens":2}})),
        ),
    ])]);
    let chunks = h.run(request()).await.expect("EOF with finish is success");
    assert!(
        matches!(chunks.as_slice(),[StreamChunk::Final(f)] if f.usage.expect("usage").total_tokens==6)
    );
}

#[tokio::test]
async fn local_reasoning_round_trip_reaches_the_second_request_body() {
    use infinity_agent_core::{
        event_processor::HistoryManager,
        message::InfinityMessage,
        stores::{InMemoryConversationStore, InMemoryStateStore},
    };
    let h = Harness::new("tensorfold", false, "").await;
    h.replies([Reply::Sse(vec![(0,event(json!({"choices":[{"delta":{"reasoning_content":"local thought"}}]}))),
        (0,event(json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":"read","arguments":{}}}]},"finish_reason":"stop"}]}))),
        (0,"data: [DONE]\n\n".into())])]);
    let history = HistoryManager::new_with_history(
        InMemoryConversationStore::new(),
        InMemoryStateStore::new(),
        "local".into(),
    )
    .await
    .expect("history");
    history
        .handle_content(
            InfinityMessage::User {
                content: UserContent::text("hello"),
            },
            "user".into(),
        )
        .expect("user input");
    let chunks = h
        .run(CompletionRequest {
            chat_history: history.get_history(false, true),
            ..Default::default()
        })
        .await
        .expect("local turn");
    for (i, chunk) in chunks.into_iter().enumerate() {
        history.mark_inputs_model_validated();
        history.handle_completion(&chunk, format!("chunk-{i}"), None);
    }
    history.flush_turn();
    history
        .handle_content(
            InfinityMessage::ToolResult {
                result: ToolResult {
                    id: "call".into(),
                    call_id: None,
                    content: vec![ToolResultContent::text("result")],
                },
                display_segments: None,
            },
            "result".into(),
        )
        .expect("tool result");
    h.run(CompletionRequest {
        chat_history: history.get_history(false, true),
        ..Default::default()
    })
    .await
    .expect("second local turn");
    let requests = h.state.requests.lock().expect("requests");
    assert_eq!(
        requests[1]["messages"][1]["reasoning_content"],
        "local thought"
    );
    assert_eq!(requests[1]["messages"][2]["role"], "tool");
}

#[tokio::test]
async fn all_python_refusal_limits_classify_below_and_above_the_config() {
    for n in ["65,536", "262,144"] {
        let messages = [
            // TensorFold 0.3.6.3 cuda/server.py:240-243,149-151. Restart and model-window values are decoys.
            format!(
                "the rendered prompt has 300000 tokens and leaves no room for a reply in the server's {n}-token safe cache capacity (model window: 999999 tokens); shorten the prompt, or restart with --context 300001 or more (this memory admits up to 888888)"
            ),
            // cuda/server.py:251-253,149-151.
            format!(
                "the rendered prompt has 60000 tokens and requests 16384 reply tokens, exceeding the server's {n}-token context window; reduce the prompt or reply length, or restart with --context 76384 or more (this memory admits up to 888888)"
            ),
            // server/app.py:365-367.
            format!(
                "This server's maximum context length is {n} tokens, the most this server's memory budget fits, but the rendered prompt has 300,000 tokens and leaves no room for a reply. Compact or shorten the conversation."
            ),
            // server/app.py:370-373. Comma formatting also tests the provider's tolerant N parser.
            format!(
                "the rendered prompt has 60000 tokens and requests 16384 reply tokens; this server's context window is {n}. Reduce the prompt to at most 49152 prompt tokens or request at most 5536 reply tokens, including chat template and thinking tokens."
            ),
        ];
        for message in messages {
            let h = Harness::with_context("tensorfold", true, "", 131072).await;
            h.replies([Reply::Http(400, json!({"error":{"message":message}}))]);
            let err = h.run(request()).await.expect_err("uncapped refusal");
            assert_eq!(
                err.class(),
                if n == "65,536" {
                    ErrorClass::Fatal
                } else {
                    ErrorClass::ContextOverflow
                },
                "{err}"
            );
        }
    }
}
#[tokio::test]
async fn length_uses_discovered_actual_context_rather_than_catalog_context() {
    let h = Harness::with_context("vllm", false, "", 131072).await;
    h.state.context.store(262144, Ordering::SeqCst);
    h.replies([Reply::Sse(vec![(0,event(json!({"choices":[{"finish_reason":"length","delta":{"content":"long reply"}}],"usage":{"prompt_tokens":250000,"completion_tokens":12144}})))])]);
    let err = h.run(request()).await.expect_err("actual context filled");
    assert_eq!(err.class(), ErrorClass::Fatal);
    assert!(err.to_string().contains("context window filled"));
}
#[tokio::test]
async fn queue_time_is_excluded_from_generation_clocks() {
    let h = Harness::new("generic", false, "").await;
    let mut output = vec![(0, event(json!({"choices":[{"delta":{"content":"start"}}]})))];
    output.extend((0..12).map(|_| (100, event(json!({"choices":[{"delta":{"content":"."}}]})))));
    output.push((
        0,
        event(json!({"choices":[{"finish_reason":"stop","delta":{}}]})),
    ));
    h.replies([Reply::Sse(output), successful()]);
    let remote = h.remote.clone();
    let first = tokio::spawn(async move {
        let mut stream = remote.invoke_model("s/p", request()).await.expect("first");
        while let Some(c) = stream.next().await {
            c.expect("active stream progresses");
        }
    });
    h.wait_requests(1).await;
    // This invocation waits longer than the one-second first-token timeout.
    h.run(request())
        .await
        .expect("queued time does not consume the clock");
    first.await.expect("first completed");
}
#[test]
fn invalid_binary_config_exits_before_printing_a_socket_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "server=[]").expect("invalid config");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_infinity-provider-local"))
        .arg("--config")
        .arg(path)
        .output()
        .expect("run provider binary");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}
#[tokio::test]
async fn parsed_tensorfold_mismatch_is_rechecked_on_the_next_invocation() {
    let h = Harness::new("tensorfold", true, "").await;
    // TensorFold Python cuda/server.py:251-253. Formatted request and context values.
    h.replies([Reply::Http(400,json!({"error":{"message":"the rendered prompt has 100 tokens and requests 1 reply tokens, exceeding the server's 64-token context window; reduce the prompt or reply length"}}))]);
    let err = h.run(request()).await.expect_err("undersized server");
    assert_eq!(err.class(), ErrorClass::Fatal);
    assert!(err.to_string().contains("64") && err.to_string().contains("128"));
    // A cached pass would skip discovery and dispatch; a recheck hits the slow route.
    h.state.discovery_delay.store(1500, Ordering::SeqCst);
    assert_eq!(
        h.run(request())
            .await
            .expect_err("discovery runs again")
            .class(),
        ErrorClass::Transient
    );
    assert_eq!(h.state.requests.lock().expect("requests").len(), 1);
}
#[tokio::test]
async fn capped_overflow_after_emitted_output_is_not_retried() {
    for delta in [
        json!({"content":"partial"}),
        json!({"reasoning_content":"think"}),
    ] {
        let h = Harness::new("vllm", false, "").await;
        // vLLM v0.30.0 vllm/v1/engine/input_processor.py:481-484, sent in-stream.
        h.replies([Reply::Sse(vec![
            (0, event(json!({"choices":[{"delta":delta}]}))),
            (0, event(json!({"error":{"message":"The decoder prompt (length 200) plus the number of requested output tokens (at least 1) is longer than the maximum model length of 256. Make sure that `max_model_len` is no smaller than the number of text tokens (prompt + requested output tokens)."}}))),
        ])]);
        let mut req = request();
        req.max_tokens = Some(100);
        let err = h.run(req).await.expect_err("no retry after output");
        assert_eq!(err.class(), ErrorClass::ContextOverflow);
        assert_eq!(h.state.requests.lock().expect("requests").len(), 1);
    }
}
#[tokio::test]
async fn configured_model_not_found_names_the_config() {
    let h = Harness::new("vllm", false, "model='weights'").await;
    h.replies([Reply::Http(
        404,
        json!({"error":{"message":"The model `weights` does not exist.","type":"NotFoundError"}}),
    )]);
    let err = h.run(request()).await.expect_err("model gone");
    assert_eq!(err.class(), ErrorClass::Fatal);
    assert!(err.to_string().contains("fix model in the config"));
}
#[tokio::test]
async fn configured_model_survives_a_missing_models_route() {
    for engine in ["generic", "llama.cpp"] {
        let h = Harness::new(engine, false, "model='weights'").await;
        h.state.no_models_route.store(true, Ordering::SeqCst);
        h.run(request()).await.expect("chat without /v1/models");
        assert_eq!(
            h.state.requests.lock().expect("requests")[0]["model"],
            "weights"
        );
    }
    let h = Harness::new("generic", false, "").await;
    h.state.no_models_route.store(true, Ordering::SeqCst);
    assert_eq!(
        h.run(request())
            .await
            .expect_err("no model to resolve")
            .class(),
        ErrorClass::Transient
    );
    let h = Harness::new("vllm", false, "model='weights'").await;
    h.state.no_models_route.store(true, Ordering::SeqCst);
    assert_eq!(
        h.run(request())
            .await
            .expect_err("vLLM context comes from /v1/models")
            .class(),
        ErrorClass::Transient
    );
}
