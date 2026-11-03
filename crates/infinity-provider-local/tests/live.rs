use futures_util::StreamExt;
use infinity_provider_local::{LocalProvider, config::Config};
use infinity_provider_protocol::{
    CompletionRequest, ErrorClass, Message, ModelProvider, StreamChunk, ToolDefinition,
};
use serde_json::json;

async fn probe(engine: &str, prefix: &str) {
    let key = format!("LOCAL_PROBE_{prefix}_URL");
    let url = std::env::var(&key).unwrap_or_else(|_| {
        panic!("set {key} to an existing loopback server; this test never starts one")
    });
    let context =
        std::env::var(format!("LOCAL_PROBE_{prefix}_CONTEXT")).unwrap_or_else(|_| "1024".into());
    let model = std::env::var(format!("LOCAL_PROBE_{prefix}_MODEL"))
        .ok()
        .map(|s| format!("model={}\n", json!(s)))
        .unwrap_or_default();
    let api_env = format!("LOCAL_PROBE_{prefix}_API_KEY");
    let auth = if std::env::var_os(&api_env).is_some() {
        format!("api_key_env='{api_env}'\n")
    } else {
        String::new()
    };
    let config=Config::parse(&format!("[[server]]\nname='probe'\nengine='{engine}'\nbase_url={}\ncontext_window={context}\nslots=1\n{model}{auth}[[server.profile]]\nid='p'\ndisplay_name='Live probe'\nthinking=true\n",json!(url))).expect("live config");
    let provider = LocalProvider::new(config).expect("provider");
    let tool_request = CompletionRequest {
        chat_history: vec![Message::user(
            "Call echo with text hello. You must use the tool.",
        )],
        tools: vec![ToolDefinition {
            name: "echo".into(),
            description: "Echo the supplied text".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
        }],
        ..Default::default()
    };
    let mut tool = provider
        .invoke_model("probe/p", tool_request)
        .await
        .expect("tool dispatch");
    let mut saw_call = false;
    while let Some(c) = tool.next().await {
        saw_call |= matches!(c.expect("tool stream"), StreamChunk::ToolCall(_));
    }
    assert!(saw_call, "engine needs its documented tool parser flags");
    let mut reasoning = provider
        .invoke_model(
            "probe/p",
            CompletionRequest {
                chat_history: vec![Message::user(
                    "Think step by step about why 17 * 19 = 323. Explain briefly.",
                )],
                ..Default::default()
            },
        )
        .await
        .expect("reasoning dispatch");
    let mut saw_reasoning = false;
    while let Some(c) = reasoning.next().await {
        saw_reasoning |= matches!(c.expect("reasoning stream"), StreamChunk::Reasoning(_));
    }
    assert!(
        saw_reasoning,
        "engine needs its documented reasoning parser flags"
    );
    let mut overflow = provider
        .invoke_model(
            "probe/p",
            CompletionRequest {
                chat_history: vec![Message::user("overflow ".repeat(1_000_000))],
                ..Default::default()
            },
        )
        .await
        .expect("overflow dispatch");
    let mut overflow_class = None;
    while let Some(c) = overflow.next().await {
        if let Err(e) = c {
            overflow_class = Some(e.class());
            break;
        }
    }
    assert_eq!(overflow_class, Some(ErrorClass::ContextOverflow));
    if engine == "vllm" {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(600))
            .build()
            .expect("probe client");
        let mut discovery = client.get(format!("{}/v1/models", url.trim_end_matches('/')));
        if let Ok(key) = std::env::var(&api_env) {
            discovery = discovery.bearer_auth(key);
        }
        let models: serde_json::Value = discovery
            .send()
            .await
            .expect("models")
            .error_for_status()
            .expect("model-list status")
            .json()
            .await
            .expect("model list");
        let raw_model = std::env::var(format!("LOCAL_PROBE_{prefix}_MODEL"))
            .map(serde_json::Value::String)
            .unwrap_or_else(|_| models["data"][0]["id"].clone());
        let mut request=client.post(format!("{}/v1/chat/completions",url.trim_end_matches('/'))).json(&json!({"model":raw_model,"messages":[{"role":"user","content":"Think through 17 * 19 step by step."}],"stream":true,"chat_template_kwargs":{"enable_thinking":true}}));
        if let Ok(key) = std::env::var(&api_env) {
            request = request.bearer_auth(key);
        }
        let text = request
            .send()
            .await
            .expect("raw reasoning probe")
            .error_for_status()
            .expect("reasoning status")
            .text()
            .await
            .expect("SSE");
        let fields: Vec<_> = ["reasoning_content", "reasoning"]
            .into_iter()
            .filter(|f| text.contains(&format!("\"{f}\":")))
            .collect();
        eprintln!("vLLM reasoning fields observed: {fields:?}");
        assert!(!fields.is_empty());
    }
}

#[tokio::test]
#[ignore = "requires LOCAL_PROBE_TENSORFOLD_NATIVE_URL and an already-running server"]
async fn tensorfold_native() {
    probe("tensorfold", "TENSORFOLD_NATIVE").await;
}
#[tokio::test]
#[ignore = "requires LOCAL_PROBE_TENSORFOLD_CUDA_URL and an already-running Python cuda/server.py"]
async fn tensorfold_cuda() {
    probe("tensorfold", "TENSORFOLD_CUDA").await;
}
#[tokio::test]
#[ignore = "requires LOCAL_PROBE_LLAMACPP_URL and an already-running llama-server"]
async fn llama_cpp() {
    probe("llama.cpp", "LLAMACPP").await;
}
#[tokio::test]
#[ignore = "requires LOCAL_PROBE_VLLM_URL and an already-running vLLM"]
async fn vllm() {
    probe("vllm", "VLLM").await;
}
#[tokio::test]
#[ignore = "requires LOCAL_PROBE_SGLANG_URL and an already-running SGLang"]
async fn sglang() {
    probe("sglang", "SGLANG").await;
}
