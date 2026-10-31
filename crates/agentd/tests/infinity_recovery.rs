//! Integration tests for the RAP callback path: callbacks flow straight into
//! the agent system's input queue (the router owns admission),
//! the session activity watcher owns the idle flag, and the daemon handles
//! only view updates itself.

use std::sync::Arc;
use std::time::Duration;

use infinity_daemon::client_handler::handle_client_channels;
use infinity_daemon::ids::SequentialIdSource;
use infinity_daemon::rap_callback;
use infinity_daemon::session::{SessionManager, SessionManagerConfig, SharedSessionManager};
use infinity_protocol::{ClientMessage, DaemonMessage, SessionStatus};
use infinity_provider_protocol::mock::{MockModelController, mock_model};
use infinity_provider_protocol::{ModelEntry, ModelProvider, SingleModelProvider};
use tokio::sync::mpsc;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[test]
fn r5_restart_settles_external_executor_and_rejects_late_result() {
    let external = tokio::runtime::Runtime::new().expect("external runtime");
    let (inv_tx, mut inv_rx) = mpsc::unbounded_channel::<rap_protocol::RapInvocation>();
    let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel::<serde_json::Value>();
    let port = external.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("external listener");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                let tx = inv_tx.clone();
                let cancel_tx = cancel_tx.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let tx = tx.clone();
                        let cancel_tx = cancel_tx.clone();
                        async move {
                            use http_body_util::BodyExt;
                            let response = if req.uri().path() == "/.well-known/rap-toolset" {
                                serde_json::json!({"name":"external", "endpoint":format!("http://127.0.0.1:{port}/invoke"), "tools":[{"name":"external_work","description":"work","inputSchema":{"type":"object"}}]}).to_string()
                            } else if req.uri().path() == "/cancel_tool_call" {
                                let body = req.into_body().collect().await.expect("cancellation body").to_bytes();
                                cancel_tx.send(serde_json::from_slice(&body).expect("cancellation")).expect("observe cancellation");
                                // The executor stays alive and never acknowledges cancellation.
                                std::future::pending::<String>().await
                            } else {
                                let body = req.into_body().collect().await.expect("invocation body").to_bytes();
                                tx.send(serde_json::from_slice(&body).expect("invocation")).expect("observe invocation");
                                "{}".into()
                            };
                            Ok::<_, std::convert::Infallible>(hyper::Response::new(http_body_util::Full::new(bytes::Bytes::from(response))))
                        }
                    });
                    if let Err(error) = hyper::server::conn::http1::Builder::new().serve_connection(hyper_util::rt::TokioIo::new(stream), service).await {
                        eprintln!("external RAP connection ended: {error}");
                    }
                });
            }
        });
        port
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (state, cwd, root, inv) = runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        rap_test_servers::write_rap_config(h.cwd.path(), port).expect("config");
        let root = create_session_and_chat(&mut h).await;
        h.client_tx
            .send(ClientMessage::UserInput {
                thread_id: root.clone(),
                text: "run external work".into(),
            })
            .expect("input");
        h.ctrl.next_request().await;
        h.ctrl
            .send_tool_call("external-call", "external_work", serde_json::json!({}));
        h.ctrl.finish();
        let inv = tokio::time::timeout(Duration::from_secs(3), inv_rx.recv())
            .await
            .expect("invocation timeout")
            .expect("invocation");
        wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m, DaemonMessage::ResponseDone { .. })
        })
        .await;
        (h._state_dir, h.cwd, root, inv)
    }));
    drop(runtime);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("replacement");
    runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness_at(state, cwd).await.expect("replacement harness");
        assert_ne!(h.callback_url, inv.callback_url);
        h.client_tx.send(ClientMessage::Connect { root_thread_id: root.clone(), thread_id:None, keeps_session_alive:true }).expect("connect");
        let replay = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
        assert!(matches!(replay, DaemonMessage::Replay { idle:false, .. }), "ordinary core load preserves upstream pending-call behavior");
        let (broker, _agent) = broker_for_root(&h, &root, "previous-boot").await;
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;
        let callback = rap_protocol::RapCallback::ToolResult(rap_protocol::RapToolResult {
            group_id:inv.group_id.clone(), id:inv.id.clone(), call_id:inv.call_id.clone(), text:Some("external work finished".into()), content:None, display_as:None, subscription:None,
        });
        let delivery = reqwest::Client::new().post(&inv.callback_url).header("content-type", "application/json").body(serde_json::to_vec(&callback).expect("callback")).timeout(Duration::from_millis(300)).send().await;
        assert!(delivery.is_err(), "old callback destination must be gone");
        eprintln!("R5 external executor survived, but callback {} is gone; replacement is {}", inv.callback_url, h.callback_url);
        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id:root.clone(), message_id:"human-after-restart".into(), from:"human".into(), text:"human after restart".into() }).expect("mail");
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("external tool recovery must not strand human mail indefinitely");

        let cancelled = tokio::time::timeout(Duration::from_secs(1), cancel_rx.recv()).await.expect("bounded restart notification").expect("cancellation");
        assert_eq!(cancelled["thread_id"], inv.group_id.as_str());
        assert_eq!(cancelled["tool_call_id"], inv.id.as_str());
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("human after restart") && history.contains("Tool call interrupted by user"));
        h.ctrl.send_text("recovered");
        h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::InputCommitted { dedup_id, .. } if dedup_id == "peer-human-after-restart")).await;
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;

        let status = reqwest_post(&h.callback_url, &serde_json::to_string(&callback).expect("late callback")).await;
        assert_eq!(status, 200);
        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id:root.clone(), message_id:"after-late-result".into(), from:"human".into(), text:"check late result".into() }).expect("mail after late result");
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("mail after late result");
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("check late result"), "late result must not start its own inference");
        assert_eq!(history.matches("Tool call interrupted by user").count(), 1);
        assert!(!history.contains("external work finished"));
        h.ctrl.send_text("still settled");
        h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;
        let payloads = std::fs::read_dir(h._state_dir.path().join("threads")).expect("threads")
            .map(|entry| std::fs::read_to_string(entry.expect("entry").path()).expect("thread payload")).collect::<String>();
        assert_eq!(payloads.matches("Tool call interrupted by user").count(), 1);
        assert!(!payloads.contains("external work finished"), "late callback cannot append a second result");
        assert!(inv_rx.try_recv().is_err(), "restart must not re-invoke the executor");
        broker.shutdown().await.expect("broker shutdown");
    }));
}

struct TestHarness {
    manager: SharedSessionManager,
    callback_url: String,
    ctrl: MockModelController,
    client_tx: mpsc::UnboundedSender<ClientMessage>,
    daemon_rx: mpsc::UnboundedReceiver<DaemonMessage>,
    _state_dir: tempfile::TempDir,
    cwd: tempfile::TempDir,
}

/// Boot a session manager with a mock model, serve its RAP callback listener,
/// and connect one client. Must run inside a `LocalSet`.
async fn start_harness() -> Result<TestHarness, BoxError> {
    start_harness_at(tempfile::tempdir()?, tempfile::tempdir()?).await
}

async fn start_harness_at(
    state_dir: tempfile::TempDir,
    cwd: tempfile::TempDir,
) -> Result<TestHarness, BoxError> {
    let (model, ctrl) = mock_model();
    let entry = ModelEntry {
        model_id: "mock-model".to_owned(),
        display_name: "Mock Model".to_owned(),
        context_window: 100_000,
        max_output_tokens: None,
        supports_image_input: false,
    };

    let bridge = infinity_rap_bridge::RapCallbackBridge::bind().await?;
    let callback_url = bridge.callback_url().to_owned();
    let manager = SessionManager::with_providers(
        SessionManagerConfig {
            state_dir: state_dir.path().to_path_buf(),
            callback_url: callback_url.clone(),
            user_rap_config: None,
            id_source: Arc::new(SequentialIdSource::new()),
        },
        vec![(
            "mock".to_owned(),
            Arc::new(SingleModelProvider::new(entry, model)) as Arc<dyn ModelProvider>,
        )],
        vec![],
    )
    .await?;
    let manager = rap_callback::serve_callbacks(bridge, manager);

    let (client_tx, client_rx) = mpsc::unbounded_channel::<ClientMessage>();
    let (daemon_tx, daemon_rx) = mpsc::unbounded_channel::<DaemonMessage>();
    tokio::task::spawn_local(handle_client_channels(
        client_rx,
        daemon_tx,
        manager.clone(),
    ));

    Ok(TestHarness {
        manager,
        callback_url,
        ctrl,
        client_tx,
        daemon_rx,
        _state_dir: state_dir,
        cwd,
    })
}

/// Receive daemon messages until the predicate matches, with a timeout.
async fn wait_for_message(
    rx: &mut mpsc::UnboundedReceiver<DaemonMessage>,
    mut pred: impl FnMut(&DaemonMessage) -> bool,
) -> DaemonMessage {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let msg = rx
                .recv()
                .await
                .expect("daemon channel closed while waiting");
            if pred(&msg) {
                return msg;
            }
        }
    })
    .await
    .expect("timed out waiting for daemon message")
}

/// Create a session, run one chat round, and return the session id.
async fn create_session_and_chat(h: &mut TestHarness) -> infinity_protocol::ThreadRef {
    h.client_tx
        .send(ClientMessage::CreateSession {
            cwd: h.cwd.path().to_path_buf(),
            location: None,
            model: None,
            keeps_session_alive: true,
        })
        .expect("send CreateSession");

    let connected = wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::Connected { .. })
    })
    .await;
    let DaemonMessage::Connected {
        root_thread_id: session_id,
        ..
    } = connected
    else {
        unreachable!()
    };

    h.client_tx
        .send(ClientMessage::UserInput {
            thread_id: session_id.clone(),
            text: "hello".to_owned(),
        })
        .expect("send UserInput");

    let _req = h.ctrl.next_request().await;
    h.ctrl.send_text("hi there");
    h.ctrl.finish();

    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::ResponseDone { .. })
    })
    .await;

    session_id
}

/// Wait until a `SessionsUpdated` broadcast reports the given status.
async fn wait_for_status(
    rx: &mut mpsc::UnboundedReceiver<DaemonMessage>,
    session_id: &infinity_protocol::ThreadRef,
    status: SessionStatus,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let msg = rx
                .recv()
                .await
                .expect("broadcast channel closed while waiting");
            if let DaemonMessage::SessionsUpdated { sessions } = msg
                && sessions.get(session_id).map(|info| &info.status) == Some(&status)
            {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for session status {status:?}"))
}

/// The session activity watcher owns the stored idle flag: it clears it when
/// a thread driver spawns (observed through the core lifecycle channel, with
/// no bookkeeping in the input path) and restores it once the driver exits.
/// Status broadcasts therefore follow thread activity: Running while the
/// model round is in flight, Idle after it completes.
#[tokio::test(flavor = "current_thread")]
async fn session_status_follows_thread_activity() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut h = start_harness().await.expect("start harness");

            // Subscribe to status broadcasts before doing anything.
            let (status_tx, mut status_rx) = mpsc::unbounded_channel();
            h.manager.lock().await.list_sessions(Some(status_tx)).await;

            h.client_tx
                .send(ClientMessage::CreateSession {
                    cwd: h.cwd.path().to_path_buf(),
                    location: None,
                    model: None,
                    keeps_session_alive: true,
                })
                .expect("send CreateSession");
            let connected = wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::Connected { .. })
            })
            .await;
            let DaemonMessage::Connected {
                root_thread_id: session_id,
                ..
            } = connected
            else {
                unreachable!()
            };

            h.client_tx
                .send(ClientMessage::UserInput {
                    thread_id: session_id.clone(),
                    text: "hello".to_owned(),
                })
                .expect("send UserInput");

            // The driver spawn flips the session to Running before the model
            // has produced anything.
            wait_for_status(&mut status_rx, &session_id, SessionStatus::Running).await;

            let _req = h.ctrl.next_request().await;
            h.ctrl.send_text("hi there");
            h.ctrl.finish();
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::ResponseDone { .. })
            })
            .await;

            // The driver exits once nothing is pending; the idle evaluation
            // restores the stored flag.
            wait_for_status(&mut status_rx, &session_id, SessionStatus::Idle).await;
        })
        .await;
}

/// A view-update callback never enters agent history: it is stored and
/// broadcast to the session's subscribers, and the session stays idle
/// because no driver wakes.
#[tokio::test(flavor = "current_thread")]
async fn view_update_is_broadcast_without_waking_the_session() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut h = start_harness().await.expect("start harness");
            let session_id = create_session_and_chat(&mut h).await;

            let callback = serde_json::json!({
                "type": "view_update",
                "group_id": session_id,
                "view_type": "diff",
                "content": { "lines": 3 },
            });
            let status = reqwest_post(&h.callback_url, &callback.to_string()).await;
            assert_eq!(status, 200, "callback must be accepted");

            let update = wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::ViewUpdate { .. })
            })
            .await;
            let DaemonMessage::ViewUpdate {
                thread_id,
                view_type,
                ..
            } = update
            else {
                unreachable!()
            };
            assert_eq!(thread_id.as_ref(), Some(&session_id));
            assert_eq!(view_type, "diff");

            let mgr = h.manager.lock().await;
            assert!(
                mgr.is_session_idle(&session_id.id),
                "a view update must not wake any thread driver"
            );
        })
        .await;
}

/// POST a JSON body and return the HTTP status code.
async fn reqwest_post(url: &str, body: &str) -> u16 {
    use rap_client::http::HttpClient;
    rap_client::http::SimpleHttpClient::new()
        .post(url, body)
        .await
        .expect("POST to the callback listener")
}

#[tokio::test(flavor = "current_thread")]
async fn peer_message_over_socket_reaches_thread_and_replays_with_receipts() {
    use bytes::Bytes;
    use futures_util::SinkExt;
    use infinity_provider_protocol::message::{AssistantContent, Message, UserContent};
    use tokio_util::codec::Framed;

    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("start harness");
        let thread_id = create_session_and_chat(&mut h).await;
        let (client, server) = tokio::net::UnixStream::pair().expect("client socket pair");
        tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(server, h.manager.clone()));
        let mut client = Framed::new(client, infinity_protocol::length_delimited_codec());
        let peer = ClientMessage::DeliverPeerMessage {
            thread_id: thread_id.clone(), message_id: "mail-1".into(), from: "@planner".into(), text: "peer payload".into(),
        };
        let wire = Bytes::from(serde_json::to_vec(&peer).expect("serialize peer"));
        client.send(wire.clone()).await.expect("send peer");
        let req = tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request()).await.expect("peer reaches model");
        let pair = &req.chat_history[req.chat_history.len()-2..];
        assert!(matches!(&pair[0], Message::Assistant { content }
            if matches!(&content[0], AssistantContent::ToolCall(call) if call.id == "peer-mail-1"
                && call.function.name == "receive_event__injected" && call.function.arguments["from"] == "@planner")));
        assert!(matches!(&pair[1], Message::User { content }
            if matches!(&content[0], UserContent::ToolResult(result) if result.id == "peer-mail-1")));
        let display = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::SubscriptionEvent { .. })).await;
        assert!(matches!(display, DaemonMessage::SubscriptionEvent { name, text, .. } if name == "message from @planner" && text == "peer payload"));
        h.ctrl.send_text("received");
        h.ctrl.finish();
        for delivery in 0..2 {
            let receipt = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::InputCommitted { dedup_id, .. } if dedup_id == "peer-mail-1")).await;
            assert!(matches!(receipt, DaemonMessage::InputCommitted { thread_id: Some(id), .. } if id == thread_id));
            if delivery == 0 {
                client.send(wire.clone()).await.expect("redeliver peer");
            }
        }
        assert!(h.ctrl.try_next_request().is_none(), "duplicate must not run inference");
        h.client_tx.send(ClientMessage::Connect { root_thread_id: thread_id, thread_id: None, keeps_session_alive: true }).expect("reconnect for replay");
        let replay = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
        let DaemonMessage::Replay { history, .. } = replay else { unreachable!() };
        let peers: Vec<_> = history.iter().filter(|m| matches!(m, DaemonMessage::SubscriptionEvent { .. })).collect();
        assert_eq!(peers.len(), 1);
        assert!(matches!(peers[0], DaemonMessage::SubscriptionEvent { name, text, .. } if name == "message from @planner" && text == "peer payload"));
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn protocol_interrupt_cancels_in_flight_completion() {
    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("start harness");
        let thread_id = create_session_and_chat(&mut h).await;
        h.client_tx.send(ClientMessage::UserInput { thread_id: thread_id.clone(), text: "keep working".into() }).expect("send input");
        h.ctrl.next_request().await;
        h.ctrl.send_text("partial");
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::TextChunk { chunk, .. } if chunk == "partial")).await;
        h.client_tx.send(ClientMessage::Interrupt { thread_id: thread_id.clone() }).expect("send interrupt");
        let done = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ResponseDone { .. })).await;
        assert!(matches!(done, DaemonMessage::ResponseDone { thread_id: Some(id), .. } if id == thread_id));
        assert!(h.ctrl.try_next_request().is_none());
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn broker_permission_timeout_durably_cancels_waiting_choice() {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Event, Phase, TurnOutcome};
    use infinity_agent_core::message::{InputMessage, InputMessageContent, UserChoiceRequired};

    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let socket = h.cwd.path().join("broker-infinity.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
        let manager = h.manager.clone();
        let accept = tokio::task::spawn_local(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(stream, manager.clone()));
            }
        });
        let mailroom = Arc::new(Mailroom::open(h.cwd.path().join("broker.db"), Default::default()).await.expect("mailroom"));
        let mut kind = agentd::KindConfig::infinity(socket);
        kind.cancel_timeout = Duration::from_millis(200);
        let broker = Broker::new(mailroom.clone(), vec![kind], Default::default()).expect("broker");
        let mut events = broker.events();
        let spec = AgentSpec {
            name: AgentName::parse("model").expect("name"), kind: "infinity".into(), cwd: h.cwd.path().into(), model: None,
            permissions: PermissionPolicy { timeout: Some(Duration::from_millis(30)), ..Default::default() }, idle_after: None,
        };
        let agent = broker.spawn(spec.clone()).await.expect("spawn");
        let mut phases = agent.phases();
        tokio::time::timeout(Duration::from_secs(5), async {
            while *phases.borrow_and_update() != Phase::Idle { phases.changed().await.expect("phase"); }
        }).await.expect("initial idle");
        let root = h.manager.lock().await.list_sessions(None).await.into_keys().next().expect("root");
        broker.send(NewMessage {
            from: Sender::Human, to: agent.id, body: Body::new("wait for a choice").expect("body"), reply_to: None,
            client_key: ClientKey("choice-input".into()), act: agentd::swarm::Act::Inform, task: None,
        }).await.expect("send");
        h.ctrl.next_request().await;
        h.ctrl.send_tool_call("waiting", "sleep_until_event_or_input", serde_json::json!({})); h.ctrl.finish();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(events.recv().await.expect("event"), Event::Consumed { .. }) {}
        }).await.expect("receipt");
        assert!(h.manager.lock().await.send_input((InputMessage {
            content: InputMessageContent::UserChoice(UserChoiceRequired {
                content_type: "user_choice_required".into(), id: "waiting".into(), call_id: None,
                prompt: "Choose".into(), choices: vec!["A".into(), "B".into()], default: 0,
                response_url: "http://example.test/choice".into(),
            }), group_id: root.id.clone(), metadata: None, synthetic: None, display_as: None, subscription: false,
        }, None)).await);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(events.recv().await.expect("event"), Event::PermissionPending(_)) {}
            loop {
                match events.recv().await.expect("event") {
                    Event::TurnEnded { outcome: TurnOutcome::Cancelled, .. } => break,
                    Event::PhaseChanged { phase: Phase::Crashed { .. } | Phase::Failed { .. }, .. } => panic!("timeout failed to cancel persisted choice"),
                    _ => {}
                }
            }
        }).await.expect("durable cancellation");
        agent.stop().await.expect("stop");
        let resumed = broker.spawn(spec).await.expect("resume");
        let mut phases = resumed.phases();
        tokio::time::timeout(Duration::from_secs(5), async {
            while *phases.borrow_and_update() != Phase::Idle { phases.changed().await.expect("phase"); }
        }).await.expect("resumed idle without pending work");
        assert!(resumed.pending_permissions().await.expect("approvals").is_empty());
        assert!(h.ctrl.try_next_request().is_none(), "cancellation must not start inference");
        h.client_tx.send(ClientMessage::Connect { root_thread_id: root, thread_id: None, keeps_session_alive: true }).expect("inspect replay");
        let replay = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
        assert!(matches!(replay, DaemonMessage::Replay { pending_choices, history, in_progress: false, .. }
            if pending_choices.is_empty() && matches!(history.last(), Some(DaemonMessage::ToolResult { .. }))));
        broker.shutdown().await.expect("shutdown"); drop(broker);
        Arc::try_unwrap(mailroom).ok().expect("sole owner").close().await.expect("close");
        accept.abort();
    }).await;
}

#[derive(Clone, Copy)]
enum ToolSettlementProbe {
    PeerWake,
    Image,
    Empty,
    CancelRealResult,
    InterruptWriteFailure { permanent: bool },
}

async fn tool_settlement_probe(probe: ToolSettlementProbe) {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Event, Phase, TurnOutcome};
    use infinity_agent_core::message::{InputMessage, InputMessageContent};
    use infinity_provider_protocol::message::{
        Image, ImageMediaType, ImageSource, ToolResult, ToolResultContent, UserContent,
    };

    let mut h = start_harness().await.expect("harness");
    let socket = h.cwd.path().join("broker-infinity.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
    let manager = h.manager.clone();
    let accept = tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                stream,
                manager.clone(),
            ));
        }
    });
    let mailroom = Arc::new(
        Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let broker = Broker::new(
        mailroom.clone(),
        vec![{
            let mut kind = agentd::KindConfig::infinity(socket);
            kind.cancel_timeout =
                if matches!(probe, ToolSettlementProbe::InterruptWriteFailure { .. }) {
                    agentd::KindConfig::infinity(std::path::PathBuf::from("unused")).cancel_timeout
                } else {
                    Duration::from_millis(100)
                };
            kind
        }],
        agentd::supervisor::Config {
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("broker");
    let mut events = broker.events();
    let spec = AgentSpec {
        name: AgentName::parse("model").expect("name"),
        kind: "infinity".into(),
        cwd: h.cwd.path().into(),
        model: None,
        permissions: PermissionPolicy::default(),
        idle_after: None,
    };
    let mut agent = broker.spawn(spec.clone()).await.expect("spawn");
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("initial idle");
    let root = h
        .manager
        .lock()
        .await
        .list_sessions(None)
        .await
        .into_keys()
        .next()
        .expect("root");
    h.client_tx
        .send(ClientMessage::Connect {
            root_thread_id: root.clone(),
            thread_id: None,
            keeps_session_alive: true,
        })
        .expect("connect observer");
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::Replay { .. })
    })
    .await;
    let human = |body: &str| NewMessage {
        from: Sender::Human,
        to: agent.id,
        body: Body::new(body).expect("body"),
        reply_to: None,
        client_key: ClientKey(body.into()),
        act: agentd::swarm::Act::Inform,
        task: None,
    };
    broker.send(human("run tool")).await.expect("send");
    h.ctrl.next_request().await;
    if matches!(
        probe,
        ToolSettlementProbe::CancelRealResult | ToolSettlementProbe::InterruptWriteFailure { .. }
    ) {
        h.ctrl
            .send_tool_call("waiting", "sleep", serde_json::json!({"seconds": 0.05}));
    } else {
        h.ctrl.send_tool_call(
            "waiting",
            "sleep_until_event_or_input",
            serde_json::json!({}),
        );
    }
    h.ctrl.finish();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(events.recv().await.expect("event"), Event::Consumed { .. }) {}
    })
    .await
    .expect("receipt");
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::ResponseDone { .. })
    })
    .await;
    match probe {
        ToolSettlementProbe::PeerWake => {
            let sender = mailroom
                .register(AgentName::parse("peer").expect("name"))
                .await
                .expect("register");
            broker
                .send(NewMessage {
                    from: Sender::Agent(sender),
                    ..human("wake up")
                })
                .await
                .expect("peer");
        }
        ToolSettlementProbe::Image | ToolSettlementProbe::Empty => {
            let content = if matches!(probe, ToolSettlementProbe::Image) {
                vec![ToolResultContent::Image(Image {
                    data: ImageSource::Base64("AA==".into()),
                    media_type: Some(ImageMediaType::PNG),
                })]
            } else {
                vec![]
            };
            assert!(
                h.manager
                    .lock()
                    .await
                    .send_input((
                        InputMessage {
                            content: InputMessageContent::User(UserContent::ToolResult(
                                ToolResult {
                                    id: "waiting".into(),
                                    call_id: None,
                                    content
                                }
                            )),
                            group_id: root.id.clone(),
                            metadata: None,
                            synthetic: None,
                            display_as: None,
                            subscription: false,
                        },
                        None
                    ))
                    .await
            );
        }
        ToolSettlementProbe::CancelRealResult
        | ToolSettlementProbe::InterruptWriteFailure { .. } => {}
    }
    tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
        .await
        .expect("tool followup");
    h.manager.lock().await.broadcast(DaemonMessage::Info {
        thread_id: Some(root.clone()),
        text: "followup-start-barrier".into(),
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(events.recv().await.expect("barrier"), Event::Activity { text, .. } if text == "followup-start-barrier") {}
    }).await.expect("broker observed followup start before cancel");
    if let ToolSettlementProbe::InterruptWriteFailure { permanent } = probe {
        // Exercise the same filesystem fault seam as memory_store's write-error tests.
        let fault = ThreadWriteFault::new(h._state_dir.path(), &root);
        agent.cancel().await.expect("cancel followup before output");
        wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m,
            DaemonMessage::Info { text, .. } if text.contains("Interrupt failed"))
        })
        .await;
        broker
            .send(human("human after write failure"))
            .await
            .expect("queue human");
        let mut crashes = 0;
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                match events.recv().await.expect("event") {
                    Event::PhaseChanged {
                        phase: Phase::Crashed { .. },
                        ..
                    } => {
                        crashes += 1;
                        if crashes == if permanent { 30 } else { 2 } {
                            break;
                        }
                    }
                    Event::PhaseChanged {
                        phase: Phase::Failed { .. },
                        ..
                    } => panic!("storage outage exhausted crash budget"),
                    Event::PhaseChanged {
                        phase: Phase::NeedsRecovery { .. },
                        ..
                    } => panic!("short storage outage requires manual recovery"),
                    Event::TurnEnded {
                        outcome: TurnOutcome::Cancelled,
                        ..
                    } => panic!("failed writes must not acknowledge cancellation"),
                    _ => {}
                }
            }
        })
        .await
        .expect("cancellation recovery must remain bounded after reconnect");
        assert!(
            h.ctrl.try_next_request().is_none(),
            "settlement retries must not run inference"
        );
        if permanent {
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::Error { text, .. } if text.contains("settlement exhausted"))
            }).await;
            assert!(!matches!(*phases.borrow(), Phase::Failed { .. }));
        }
        drop(fault);
        tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
            .await
            .expect("queued human must progress after storage recovery");
        h.ctrl.send_text("recovered human answer");
        h.ctrl.finish();
    } else if matches!(probe, ToolSettlementProbe::CancelRealResult) {
        agent.cancel().await.expect("cancel followup before output");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(
                events.recv().await.expect("event"),
                Event::TurnEnded {
                    outcome: TurnOutcome::Cancelled,
                    ..
                }
            ) {}
        })
        .await
        .expect("cancel ack");
    } else {
        let settlement = wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m, DaemonMessage::ToolResult { .. })
        })
        .await;
        match probe {
            ToolSettlementProbe::PeerWake => {
                assert!(format!("{settlement:?}").contains("interrupted"))
            }
            _ => assert!(
                matches!(settlement, DaemonMessage::ToolResult { segments, .. } if segments.is_empty())
            ),
        }
        h.ctrl.send_text("finished the work");
        h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m, DaemonMessage::ResponseDone { .. })
        })
        .await;
        broker.send(human("next human")).await.expect("human");
        tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
            .await
            .expect("human mail must progress after settled tool and final response");
        h.ctrl.send_text("human answered");
        h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m, DaemonMessage::ResponseDone { .. })
        })
        .await;
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("idle after settlement");
    agent.stop().await.expect("stop");
    agent = broker.spawn(spec).await.expect("resume");
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while matches!(
            *phases.borrow_and_update(),
            Phase::Dormant | Phase::Starting
        ) {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("attach");
    assert_eq!(
        agent.phase(),
        Phase::Idle,
        "reconnect must preserve settled tool state"
    );
    h.client_tx
        .send(ClientMessage::Connect {
            root_thread_id: root.clone(),
            thread_id: None,
            keeps_session_alive: true,
        })
        .expect("inspect durable replay");
    let replay = wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::Replay { .. })
    })
    .await;
    eprintln!("settled replay: {replay:?}");
    let DaemonMessage::Replay {
        history,
        pending_choices,
        ..
    } = replay
    else {
        unreachable!()
    };
    assert!(pending_choices.is_empty());
    let results: Vec<_> = history
        .iter()
        .filter_map(|m| match m {
            DaemonMessage::ToolResult { segments, .. } => Some(segments),
            _ => None,
        })
        .collect();
    assert_eq!(
        results.len(),
        1,
        "replay must include exactly one tool settlement"
    );
    match probe {
        ToolSettlementProbe::CancelRealResult
        | ToolSettlementProbe::InterruptWriteFailure { .. } => assert!(
            format!("{:?}", results[0]).contains("Slept for 0.05 seconds"),
            "preserve real result rather than substitute interruption"
        ),
        ToolSettlementProbe::PeerWake => {
            assert!(format!("{:?}", results[0]).contains("interrupted"))
        }
        _ => assert!(results[0].is_empty()),
    }
    if matches!(probe, ToolSettlementProbe::InterruptWriteFailure { .. }) {
        let persisted = std::fs::read_to_string(
            h._state_dir
                .path()
                .join("threads")
                .join(format!("{}.json", root.id)),
        )
        .expect("read durable thread");
        assert_eq!(
            persisted.matches("Slept for 0.05 seconds").count(),
            1,
            "accepted real result must be written exactly once"
        );
    }
    broker.shutdown().await.expect("shutdown");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("sole owner")
        .close()
        .await
        .expect("close");
    accept.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn review_peer_wakes_sleep() {
    tokio::task::LocalSet::new()
        .run_until(tool_settlement_probe(ToolSettlementProbe::PeerWake))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn review_image_tool_result() {
    tokio::task::LocalSet::new()
        .run_until(tool_settlement_probe(ToolSettlementProbe::Image))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn review_empty_tool_result() {
    tokio::task::LocalSet::new()
        .run_until(tool_settlement_probe(ToolSettlementProbe::Empty))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn review_real_result_racing_interrupt() {
    tokio::task::LocalSet::new()
        .run_until(tool_settlement_probe(ToolSettlementProbe::CancelRealResult))
        .await;
}

/// Block actual thread payload writes without changing store caches or state writes.
/// Restoring the old file also ensures a retry must append the accepted result.
struct ThreadWriteFault {
    path: std::path::PathBuf,
    saved: std::path::PathBuf,
}

impl ThreadWriteFault {
    fn new(state_dir: &std::path::Path, root: &infinity_protocol::ThreadRef) -> Self {
        let path = state_dir.join("threads").join(format!("{}.json", root.id));
        let saved = path.with_extension("json.saved");
        std::fs::rename(&path, &saved).expect("save durable thread");
        std::fs::create_dir(&path).expect("inject write failure");
        Self { path, saved }
    }
}

impl Drop for ThreadWriteFault {
    fn drop(&mut self) {
        std::fs::remove_dir(&self.path).expect("remove write fault");
        std::fs::rename(&self.saved, &self.path).expect("restore writable thread");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn interrupt_write_failure_retries_settlement_across_reconnects() {
    tokio::task::LocalSet::new()
        .run_until(tool_settlement_probe(
            ToolSettlementProbe::InterruptWriteFailure { permanent: false },
        ))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r11_storage_repair_recovers_without_restarting_failed_agent() {
    tokio::task::LocalSet::new()
        .run_until(tool_settlement_probe(
            ToolSettlementProbe::InterruptWriteFailure { permanent: true },
        ))
        .await;
}

async fn queued_attachment_mail_probe(resume: bool) {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Event, Phase};
    let mut h = start_harness().await.expect("harness");
    let socket = h.cwd.path().join("broker-infinity.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
    let manager = h.manager.clone();
    let accept = tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                stream,
                manager.clone(),
            ));
        }
    });
    let mailroom = Arc::new(
        Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let broker = Broker::new(
        mailroom.clone(),
        vec![{
            let mut kind = agentd::KindConfig::infinity(socket);
            kind.request_timeout = Duration::from_secs(2);
            kind
        }],
        agentd::supervisor::Config {
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("broker");
    let mut events = broker.events();
    let spec = AgentSpec {
        name: AgentName::parse("model").expect("name"),
        kind: "infinity".into(),
        cwd: h.cwd.path().into(),
        model: None,
        permissions: PermissionPolicy::default(),
        idle_after: None,
    };

    if resume {
        let agent = broker.spawn(spec.clone()).await.expect("initial spawn");
        let mut phases = agent.phases();
        tokio::time::timeout(Duration::from_secs(5), async {
            while *phases.borrow_and_update() != Phase::Idle {
                phases.changed().await.expect("phase");
            }
        })
        .await
        .expect("initial idle");
        agent.stop().await.expect("stop before queued resume");
    }
    let id = mailroom
        .register(spec.name.clone())
        .await
        .expect("register");
    let (_, envelope) = mailroom
        .accept(NewMessage {
            from: Sender::Human,
            to: id,
            body: Body::new("initial slow request").expect("body"),
            reply_to: None,
            client_key: ClientKey("attachment".into()),
            act: agentd::swarm::Act::Inform,
            task: None,
        })
        .await
        .expect("prequeue");
    let agent = broker.spawn(spec).await.expect("spawn with queued mail");
    tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
        .await
        .expect("inference starts");
    // The provider has accepted the request but intentionally sends no output or receipt.
    let crashed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                events.recv().await.expect("event"),
                Event::PhaseChanged {
                    phase: Phase::Crashed { .. } | Phase::Failed { .. },
                    ..
                }
            ) {
                break;
            }
        }
    })
    .await
    .is_ok();
    assert!(
        !crashed,
        "attachment idle must not time out a later streaming turn"
    );
    assert_eq!(agent.phase(), Phase::Busy);
    h.ctrl.send_text("done");
    h.ctrl.finish();
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("turn settles");
    assert!(matches!(
        mailroom
            .delivery(envelope.id())
            .await
            .expect("delivery")
            .state,
        DeliveryState::Consumed
    ));
    broker.shutdown().await.expect("shutdown");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("sole owner")
        .close()
        .await
        .expect("close");
    accept.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn queued_startup_mail_outlives_attachment_idle_receipt_timeout() {
    tokio::task::LocalSet::new()
        .run_until(queued_attachment_mail_probe(false))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn queued_resume_mail_outlives_attachment_idle_receipt_timeout() {
    tokio::task::LocalSet::new()
        .run_until(queued_attachment_mail_probe(true))
        .await;
}

async fn r4_human_sleep_probe() {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Event, Phase};

    let mut h = start_harness().await.expect("harness");
    let socket = h.cwd.path().join("broker-infinity.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
    let manager = h.manager.clone();
    let accept = tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                stream,
                manager.clone(),
            ));
        }
    });
    let mailroom = Arc::new(
        Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let broker = Broker::new(
        mailroom.clone(),
        vec![{
            let mut kind = agentd::KindConfig::infinity(socket);
            kind.cancel_timeout = Duration::from_millis(100);
            kind
        }],
        agentd::supervisor::Config {
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("broker");
    let mut events = broker.events();
    let spec = AgentSpec {
        name: AgentName::parse("model").expect("name"),
        kind: "infinity".into(),
        cwd: h.cwd.path().into(),
        model: None,
        permissions: PermissionPolicy::default(),
        idle_after: None,
    };
    let agent = broker.spawn(spec.clone()).await.expect("spawn");
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("initial idle");
    let root = h
        .manager
        .lock()
        .await
        .list_sessions(None)
        .await
        .into_keys()
        .next()
        .expect("root");
    h.client_tx
        .send(ClientMessage::Connect {
            root_thread_id: root.clone(),
            thread_id: None,
            keeps_session_alive: true,
        })
        .expect("connect observer");
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::Replay { .. })
    })
    .await;
    let human = |body: &str| NewMessage {
        from: Sender::Human,
        to: agent.id,
        body: Body::new(body).expect("body"),
        reply_to: None,
        client_key: ClientKey(body.into()),
        act: agentd::swarm::Act::Inform,
        task: None,
    };
    broker.send(human("run tool")).await.expect("send");
    h.ctrl.next_request().await;
    h.ctrl.send_tool_call(
        "waiting",
        "sleep_until_event_or_input",
        serde_json::json!({}),
    );
    h.ctrl.finish();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(events.recv().await.expect("event"), Event::Consumed { .. }) {}
    })
    .await
    .expect("receipt");
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::ResponseDone { .. })
    })
    .await;
    let (_, envelope) = broker
        .send(human("wake from human"))
        .await
        .expect("human wake");
    let request = tokio::time::timeout(Duration::from_millis(600), h.ctrl.next_request())
        .await
        .expect(
            "human mail must wake passive sleep without requiring another agent or manual cancel",
        );
    let history = format!("{:?}", request.chat_history);
    assert!(history.contains("human operator") && history.contains("wake from human"));
    h.ctrl.send_text("awake");
    h.ctrl.finish();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(events.recv().await.expect("event"), Event::Consumed { ids, .. } if ids.contains(&envelope.id())) {}
    }).await.expect("human receipt");
    broker.shutdown().await.expect("shutdown");
    accept.abort();
}
#[tokio::test(flavor = "current_thread")]
async fn r4_human_wakes_passive_sleep() {
    tokio::task::LocalSet::new()
        .run_until(r4_human_sleep_probe())
        .await;
}

async fn r4_before_restart() -> (tempfile::TempDir, tempfile::TempDir) {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Event, Phase};

    let mut h = start_harness().await.expect("harness");
    let socket = h.cwd.path().join("broker-infinity.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
    let manager = h.manager.clone();
    let accept = tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                stream,
                manager.clone(),
            ));
        }
    });
    let mailroom = Arc::new(
        Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let broker = Broker::new(
        mailroom.clone(),
        vec![{
            let mut kind = agentd::KindConfig::infinity(socket);
            kind.cancel_timeout = Duration::from_millis(100);
            kind
        }],
        agentd::supervisor::Config {
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("broker");
    let mut events = broker.events();
    let spec = AgentSpec {
        name: AgentName::parse("model").expect("name"),
        kind: "infinity".into(),
        cwd: h.cwd.path().into(),
        model: None,
        permissions: PermissionPolicy::default(),
        idle_after: None,
    };
    let agent = broker.spawn(spec.clone()).await.expect("spawn");
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("initial idle");
    let root = h
        .manager
        .lock()
        .await
        .list_sessions(None)
        .await
        .into_keys()
        .next()
        .expect("root");
    h.client_tx
        .send(ClientMessage::Connect {
            root_thread_id: root.clone(),
            thread_id: None,
            keeps_session_alive: true,
        })
        .expect("connect observer");
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::Replay { .. })
    })
    .await;
    let human = |body: &str| NewMessage {
        from: Sender::Human,
        to: agent.id,
        body: Body::new(body).expect("body"),
        reply_to: None,
        client_key: ClientKey(body.into()),
        act: agentd::swarm::Act::Inform,
        task: None,
    };
    broker.send(human("run tool")).await.expect("send");
    h.ctrl.next_request().await;
    h.ctrl
        .send_tool_call("waiting", "sleep", serde_json::json!({"seconds": 60}));
    h.ctrl.finish();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(events.recv().await.expect("event"), Event::Consumed { .. }) {}
    })
    .await
    .expect("receipt");
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::ResponseDone { .. })
    })
    .await;
    broker.shutdown().await.expect("shutdown broker");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("sole owner")
        .close()
        .await
        .expect("close");
    accept.abort();
    (h._state_dir, h.cwd)
}
async fn r4_after_restart(state_dir: tempfile::TempDir, cwd: tempfile::TempDir) {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Event, Phase};

    let mut h = start_harness_at(state_dir, cwd).await.expect("harness");
    let socket = h.cwd.path().join("broker-infinity.sock");
    std::fs::remove_file(&socket).expect("remove stale scratch socket");
    let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
    let manager = h.manager.clone();
    let accept = tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                stream,
                manager.clone(),
            ));
        }
    });
    let mailroom = Arc::new(
        Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let broker = Broker::new(
        mailroom.clone(),
        vec![{
            let mut kind = agentd::KindConfig::infinity(socket);
            kind.cancel_timeout = Duration::from_millis(100);
            kind
        }],
        agentd::supervisor::Config {
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("broker");
    let mut events = broker.events();
    let spec = AgentSpec {
        name: AgentName::parse("model").expect("name"),
        kind: "infinity".into(),
        cwd: h.cwd.path().into(),
        model: None,
        permissions: PermissionPolicy::default(),
        idle_after: None,
    };
    let agent = broker.spawn(spec.clone()).await.expect("spawn");
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("initial idle");
    let (_, envelope) = broker
        .send(NewMessage {
            from: Sender::Human,
            to: agent.id,
            body: Body::new("human after daemon restart").expect("body"),
            reply_to: None,
            client_key: ClientKey("after-restart".into()),
            act: agentd::swarm::Act::Inform,
            task: None,
        })
        .await
        .expect("send");
    let request = tokio::time::timeout(Duration::from_secs(1), h.ctrl.next_request())
        .await
        .expect("daemon restart must not strand mail behind an orphaned timer");
    assert!(format!("{:?}", request.chat_history).contains("Tool call interrupted by user"));
    h.ctrl.send_text("recovered");
    h.ctrl.finish();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(events.recv().await.expect("event"), Event::Consumed { ids, .. } if ids.contains(&envelope.id())) {}
    }).await.expect("restart receipt");
    let payloads = std::fs::read_dir(h._state_dir.path().join("threads"))
        .expect("threads")
        .map(|entry| std::fs::read_to_string(entry.expect("entry").path()).expect("thread payload"))
        .collect::<String>();
    assert_eq!(payloads.matches("Tool call interrupted by user").count(), 1);
    broker.shutdown().await.expect("shutdown");
    accept.abort();
}
#[test]
fn r4_daemon_restart_orphaned_tool() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (state_dir, cwd) =
        runtime.block_on(tokio::task::LocalSet::new().run_until(r4_before_restart()));
    drop(runtime);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("replacement runtime");
    runtime.block_on(tokio::task::LocalSet::new().run_until(r4_after_restart(state_dir, cwd)));
}

#[tokio::test(flavor = "current_thread")]
async fn r9_broker_mail_after_completed_rap_tool_and_provider_failure() {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Phase};
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let port = rap_test_servers::start_stub_diff_server()
                .await
                .expect("RAP tool");
            rap_test_servers::write_rap_config(h.cwd.path(), port).expect("RAP config");
            let socket = h.cwd.path().join("broker-infinity.sock");
            let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
            let manager = h.manager.clone();
            let accept = tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = listener.accept().await.expect("accept");
                    tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                        stream,
                        manager.clone(),
                    ));
                }
            });
            let mailroom = Arc::new(
                Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
                    .await
                    .expect("mailroom"),
            );
            let broker = Broker::new(
                mailroom.clone(),
                vec![{
                    let mut kind = agentd::KindConfig::infinity(socket);
                    kind.cancel_timeout = Duration::from_millis(100);
                    kind.request_timeout = Duration::from_millis(200);
                    kind
                }],
                agentd::supervisor::Config {
                    backoff_initial: Duration::from_millis(10),
                    backoff_max: Duration::from_millis(10),
                    ..Default::default()
                },
            )
            .expect("broker");
            let spec = AgentSpec {
                name: AgentName::parse("model").expect("name"),
                kind: "infinity".into(),
                cwd: h.cwd.path().into(),
                model: None,
                permissions: PermissionPolicy::default(),
                idle_after: None,
            };
            let agent = broker.spawn(spec.clone()).await.expect("spawn");
            let mut phases = agent.phases();
            tokio::time::timeout(Duration::from_secs(5), async {
                while *phases.borrow_and_update() != Phase::Idle {
                    phases.changed().await.expect("phase");
                }
            })
            .await
            .expect("initial idle");
            let root = h
                .manager
                .lock()
                .await
                .list_sessions(None)
                .await
                .into_keys()
                .next()
                .expect("root");
            h.client_tx
                .send(ClientMessage::Connect {
                    root_thread_id: root.clone(),
                    thread_id: None,
                    keeps_session_alive: true,
                })
                .expect("connect observer");
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::Replay { .. })
            })
            .await;
            let human = |body: &str| NewMessage {
                from: Sender::Human,
                to: agent.id,
                body: Body::new(body).expect("body"),
                reply_to: None,
                client_key: ClientKey(body.into()),
                act: agentd::swarm::Act::Inform,
                task: None,
            };

            broker.send(human("run tool")).await.expect("send");
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call(
                "completed",
                "edit_file",
                serde_json::json!({"path":"demo.rs"}),
            );
            h.ctrl.finish();
            let followup = tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
                .await
                .expect("RAP callback followup");
            assert!(format!("{:?}", followup.chat_history).contains("Replaced text in demo.rs"));
            h.ctrl
                .send_error(infinity_provider_protocol::CompletionError::provider(
                    infinity_provider_protocol::ErrorClass::Fatal,
                    "one failed provider request",
                ));
            h.ctrl.drop_stream();
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::ThreadIdle { .. })
            })
            .await;
            tokio::time::timeout(Duration::from_secs(5), async {
                while *phases.borrow_and_update() != Phase::Idle {
                    phases.changed().await.expect("idle phase");
                }
            })
            .await
            .expect("idle after failed model round");
            let stored = std::fs::read_to_string(
                h._state_dir
                    .path()
                    .join("threads")
                    .join(format!("{}.json", root.id)),
            )
            .expect("history");
            broker
                .send(human("human after provider recovery"))
                .await
                .expect("later human mail");
            let next = tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request()).await;
            let next = next.expect(
                "completed RAP tool must not strand broker Busy after one provider failure",
            );
            assert!(format!("{:?}", next.chat_history).contains("human after provider recovery"));
            assert_eq!(stored.matches("Replaced text in demo.rs").count(), 1);
            assert!(!stored.contains("Tool call interrupted by user"));
            assert!(format!("{:?}", next.chat_history).contains("Replaced text in demo.rs"));
            h.ctrl.send_text("done");
            h.ctrl.finish();
            broker.shutdown().await.expect("shutdown");
            accept.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r10a_driver_panic_has_bounded_broker_recovery() {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Phase};
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let port = rap_test_servers::start_stub_diff_server()
                .await
                .expect("RAP tool");
            rap_test_servers::write_rap_config(h.cwd.path(), port).expect("RAP config");
            let socket = h.cwd.path().join("broker-infinity.sock");
            let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
            let manager = h.manager.clone();
            let accept = tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = listener.accept().await.expect("accept");
                    tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                        stream,
                        manager.clone(),
                    ));
                }
            });
            let mailroom = Arc::new(
                Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
                    .await
                    .expect("mailroom"),
            );
            let broker = Broker::new(
                mailroom.clone(),
                vec![{
                    let mut kind = agentd::KindConfig::infinity(socket);
                    kind.cancel_timeout = Duration::from_millis(100);
                    kind.request_timeout = Duration::from_millis(200);
                    kind
                }],
                agentd::supervisor::Config {
                    backoff_initial: Duration::from_millis(10),
                    backoff_max: Duration::from_millis(10),
                    ..Default::default()
                },
            )
            .expect("broker");
            let spec = AgentSpec {
                name: AgentName::parse("model").expect("name"),
                kind: "infinity".into(),
                cwd: h.cwd.path().into(),
                model: None,
                permissions: PermissionPolicy {
                    timeout: Some(Duration::from_millis(30)),
                    ..Default::default()
                },
                idle_after: None,
            };
            let agent = broker.spawn(spec.clone()).await.expect("spawn");
            let mut phases = agent.phases();
            tokio::time::timeout(Duration::from_secs(5), async {
                while *phases.borrow_and_update() != Phase::Idle {
                    phases.changed().await.expect("phase");
                }
            })
            .await
            .expect("initial idle");
            let root = h
                .manager
                .lock()
                .await
                .list_sessions(None)
                .await
                .into_keys()
                .next()
                .expect("root");
            h.client_tx
                .send(ClientMessage::Connect {
                    root_thread_id: root.clone(),
                    thread_id: None,
                    keeps_session_alive: true,
                })
                .expect("connect observer");
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::Replay { .. })
            })
            .await;
            let human = |body: &str| NewMessage {
                from: Sender::Human,
                to: agent.id,
                body: Body::new(body).expect("body"),
                reply_to: None,
                client_key: ClientKey(body.into()),
                act: agentd::swarm::Act::Inform,
                task: None,
            };

            let trigger = broker
                .send(human("run malformed spawn"))
                .await
                .expect("send");
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call(
                "panic-spawn",
                "spawn_thread",
                serde_json::json!({"child_of":[root.id.as_str()]}),
            );
            h.ctrl.finish();
            let request = tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request())
                .await
                .expect("malformed spawn must return a tool error instead of losing the driver");
            assert!(format!("{:?}", request.chat_history).contains("requires string instructions"));
            h.ctrl.send_text("recovered");
            h.ctrl.finish();
            tokio::time::timeout(Duration::from_secs(4), async {
                while *phases.borrow_and_update() != Phase::Idle {
                    phases.changed().await.expect("phase");
                }
            })
            .await
            .expect("malformed spawn must finish");
            assert!(matches!(
                mailroom
                    .delivery(trigger.1.id())
                    .await
                    .expect("delivery")
                    .state,
                DeliveryState::Consumed
            ));
            broker.shutdown().await.expect("shutdown");
            accept.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r10a_choice_write_failure_has_bounded_recovery() {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentSpec, Broker, Phase};
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let port = r10a_waiting_server()
                .await
                .expect("RAP tool");
            rap_test_servers::write_rap_config(h.cwd.path(), port).expect("RAP config");
            let socket = h.cwd.path().join("broker-infinity.sock");
            let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
            let manager = h.manager.clone();
            let accept = tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = listener.accept().await.expect("accept");
                    tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                        stream,
                        manager.clone(),
                    ));
                }
            });
            let mailroom = Arc::new(
                Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
                    .await
                    .expect("mailroom"),
            );
            let broker = Broker::new(
                mailroom.clone(),
                vec![{
                    let mut kind = agentd::KindConfig::infinity(socket);
                    kind.cancel_timeout = Duration::from_millis(100);
                    kind.request_timeout = Duration::from_millis(200);
                    kind
                }],
                agentd::supervisor::Config {
                    backoff_initial: Duration::from_millis(10),
                    backoff_max: Duration::from_millis(10),
                    ..Default::default()
                },
            )
            .expect("broker");
            let spec = AgentSpec {
                name: AgentName::parse("model").expect("name"),
                kind: "infinity".into(),
                cwd: h.cwd.path().into(),
                model: None,
                permissions: PermissionPolicy { timeout: Some(Duration::from_millis(30)), ..Default::default() },
                idle_after: None,
            };
            let agent = broker.spawn(spec.clone()).await.expect("spawn");
            let mut phases = agent.phases();
            tokio::time::timeout(Duration::from_secs(5), async {
                while *phases.borrow_and_update() != Phase::Idle {
                    phases.changed().await.expect("phase");
                }
            })
            .await
            .expect("initial idle");
            let root = h
                .manager
                .lock()
                .await
                .list_sessions(None)
                .await
                .into_keys()
                .next()
                .expect("root");
            h.client_tx
                .send(ClientMessage::Connect {
                    root_thread_id: root.clone(),
                    thread_id: None,
                    keeps_session_alive: true,
                })
                .expect("connect observer");
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::Replay { .. })
            })
            .await;
            let human = |body: &str| NewMessage {
                from: Sender::Human,
                to: agent.id,
                body: Body::new(body).expect("body"),
                reply_to: None,
                client_key: ClientKey(body.into()),
                act: agentd::swarm::Act::Inform,
                task: None,
            };

            broker.send(human("run choice tool")).await.expect("send");
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call("waiting-choice", "waiting_choice", serde_json::json!({}));
            h.ctrl.finish();
            wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ResponseDone { .. })).await;
            let state_path = h._state_dir.path().join("state").join(format!("{}.state.json", root.id));
            let saved = state_path.with_extension("json.saved");
            std::fs::rename(&state_path, &saved).expect("save state");
            std::fs::create_dir(&state_path).expect("block choice write");
            let choice = rap_protocol::RapCallback::UserChoice(rap_protocol::RapUserChoice {
                group_id: root.id.clone(), id: "waiting-choice".into(), call_id: None,
                prompt: "Choose".into(), choices: vec!["A".into()], default: 0,
                response_url: format!("http://127.0.0.1:{port}/answer"),
            });
            assert_eq!(reqwest_post(&h.callback_url, &serde_json::to_string(&choice).expect("choice JSON")).await, 200);
            let error = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Info { text, .. } if text.starts_with("Error:"))).await;
            eprintln!("observed sole failed write: {error:?}");
            std::fs::remove_dir(&state_path).expect("remove fault");
            std::fs::rename(&saved, &state_path).expect("repair state");
            let sent = broker.send(human("mail after repaired choice write")).await.expect("later mail");
            let next = tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request()).await;
            eprintln!("phase after repair: {:?}; permissions: {:?}; delivery: {:?}", phases.borrow().clone(), agent.pending_permissions().await.expect("permissions"), mailroom.delivery(sent.1.id()).await.expect("delivery").state);
            assert!(next.is_ok(), "single failed choice write must not leave all later broker mail blocked after storage is repaired");
            h.ctrl.send_text("done"); h.ctrl.finish();
            broker.shutdown().await.expect("shutdown"); accept.abort();
        }).await;
}

async fn r10a_waiting_server() -> std::io::Result<u16> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(async move {
                let service = hyper::service::service_fn(
                    move |req: hyper::Request<hyper::body::Incoming>| async move {
                        use http_body_util::BodyExt;
                        let response = if req.uri().path() == "/.well-known/rap-toolset" {
                            serde_json::json!({"name":"choice", "endpoint":format!("http://127.0.0.1:{port}/invoke"), "tools":[{"name":"waiting_choice","description":"waits for choice","inputSchema":{"type":"object"}}]}).to_string()
                        } else {
                            req.into_body().collect().await.expect("body");
                            "{}".into()
                        };
                        Ok::<_, std::convert::Infallible>(hyper::Response::new(
                            http_body_util::Full::new(bytes::Bytes::from(response)),
                        ))
                    },
                );
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await
                    .expect("serve");
            });
        }
    });
    Ok(port)
}

async fn broker_for_root(
    h: &TestHarness,
    root: &infinity_protocol::ThreadRef,
    old_boot: &str,
) -> (agentd::supervisor::Broker, agentd::supervisor::AgentHandle) {
    use agentd::{
        mailroom::Mailroom,
        model::AgentName,
        supervisor::{AgentSpec, Broker, Phase},
    };
    let socket = h.cwd.path().join("recovery.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
    let manager = h.manager.clone();
    tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                stream,
                manager.clone(),
            ));
        }
    });
    let db = h.cwd.path().join("recovery.db");
    let mailroom = Mailroom::open(&db, Default::default())
        .await
        .expect("mailroom");
    let name = AgentName::parse("recovery").expect("name");
    let agent = mailroom.register(name.clone()).await.expect("register");
    mailroom.close().await.expect("close for fixture");
    let token =
        serde_json::json!({"kind":"infinity", "socket":socket, "root":root, "boot_id":old_boot})
            .to_string();
    rusqlite::Connection::open(&db)
        .expect("fixture DB")
        .execute(
            "INSERT INTO actor_state(agent,incarnation,kind,resume) VALUES (?,1,'infinity',?)",
            rusqlite::params![
                serde_json::to_string(&agent).expect("agent"),
                serde_json::to_string(&token).expect("token")
            ],
        )
        .expect("persist prior incarnation resume token");
    let mailroom = Arc::new(
        Mailroom::open(db, Default::default())
            .await
            .expect("mailroom"),
    );
    let broker = Broker::new(
        mailroom,
        vec![agentd::KindConfig::infinity(socket)],
        Default::default(),
    )
    .expect("broker");
    let agent = broker
        .spawn(AgentSpec {
            name,
            kind: "infinity".into(),
            cwd: h.cwd.path().into(),
            model: None,
            permissions: Default::default(),
            idle_after: None,
        })
        .await
        .expect("spawn");
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("broker boot recovery settles");
    (broker, agent)
}

#[test]
fn broker_boot_rule_recovers_parent_spawn_without_recreating_child() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (state, cwd, root, child_payloads) = runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let root = create_session_and_chat(&mut h).await;
        h.client_tx.send(ClientMessage::UserInput { thread_id:root.clone(), text:"spawn".into() }).expect("input");
        h.ctrl.next_request().await;
        h.ctrl.send_tool_call("parent-spawn", "spawn_thread", serde_json::json!({"child_of":[root.id.as_str()], "instructions":"child payload survives"}));
        h.ctrl.finish();
        h.ctrl.next_request().await;
        let payloads: Vec<_> = std::fs::read_dir(h._state_dir.path().join("threads")).expect("threads")
            .map(|entry| { let path = entry.expect("entry").path(); let bytes = std::fs::read(&path).expect("payload"); (path.file_name().expect("name").to_owned(), bytes) })
            .filter(|(name, _)| name.to_string_lossy().ends_with(".json") && !name.to_string_lossy().starts_with(root.id.as_str())).collect();
        assert!(!payloads.is_empty(), "spawn created child before restart");
        (h._state_dir, h.cwd, root, payloads)
    }));
    drop(runtime);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("replacement runtime");
    runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness_at(state, cwd).await.expect("restart");
        let (broker, _agent) = broker_for_root(&h, &root, "previous-boot").await;
        assert!(h.ctrl.try_next_request().is_none(), "recovery runs no inference");
        h.client_tx.send(ClientMessage::Connect { root_thread_id:root.clone(), thread_id:None, keeps_session_alive:true }).expect("observe");
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
        for (name, bytes) in child_payloads { assert_eq!(std::fs::read(h._state_dir.path().join("threads").join(name)).expect("child payload"), bytes); }
        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id:root.clone(), message_id:"after-parent-spawn".into(), from:"human".into(), text:"mail after parent spawn restart".into() }).expect("mail");
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("mail progresses");
        assert!(format!("{:?}", request.chat_history).contains("Tool call interrupted by user"));
        h.ctrl.send_text("done"); h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::InputCommitted { dedup_id, .. } if dedup_id == "peer-after-parent-spawn")).await;
        broker.shutdown().await.expect("shutdown");
    }));
}

#[test]
fn broker_boot_rule_preserves_subscription_payload() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (state, cwd, root) = runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let root = create_session_and_chat(&mut h).await;
        h.client_tx.send(ClientMessage::UserInput { thread_id:root.clone(), text:"subscribe".into() }).expect("input");
        h.ctrl.next_request().await;
        h.ctrl.send_tool_call("subscription", "sleep_until_event_or_input", serde_json::json!({})); h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ResponseDone { .. })).await;
        assert!(h.manager.lock().await.send_input((infinity_agent_core::message::InputMessage {
            content: infinity_agent_core::message::InputMessageContent::User(infinity_provider_protocol::message::UserContent::ToolResult(infinity_provider_protocol::message::ToolResult {
                id:"subscription".into(), call_id:None, content:vec![infinity_provider_protocol::message::ToolResultContent::text("unique subscription payload")],
            })), group_id:root.id.clone(), metadata:None, synthetic:None, display_as:None, subscription:true,
        }, Some("subscription-result".into()))).await);
        h.ctrl.next_request().await;
        h.ctrl.send_tool_call("orphaned-sleep", "sleep", serde_json::json!({"seconds":60})); h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ResponseDone { .. })).await;
        (h._state_dir, h.cwd, root)
    }));
    drop(runtime);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("replacement");
    runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness_at(state, cwd).await.expect("restart");
        let (broker, _agent) = broker_for_root(&h, &root, "previous-boot").await;
        h.client_tx.send(ClientMessage::Connect { root_thread_id:root.clone(), thread_id:None, keeps_session_alive:true }).expect("observe");
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id:root, message_id:"after-subscription".into(), from:"human".into(), text:"after restart".into() }).expect("mail");
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("mail after recovery");
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("unique subscription payload") && history.contains("Tool call interrupted by user"));
        h.ctrl.send_text("done"); h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::InputCommitted { dedup_id, .. } if dedup_id == "peer-after-subscription")).await;
        broker.shutdown().await.expect("shutdown");
    }));
}

async fn r11_broker(
    h: &TestHarness,
) -> (
    agentd::supervisor::Broker,
    agentd::supervisor::AgentHandle,
    infinity_protocol::ThreadRef,
) {
    use agentd::{
        mailroom::Mailroom,
        model::AgentName,
        supervisor::{AgentSpec, Broker, Phase},
    };
    let socket = h.cwd.path().join("r11.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
    let manager = h.manager.clone();
    tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                stream,
                manager.clone(),
            ));
        }
    });
    let mailroom = Arc::new(
        Mailroom::open(h.cwd.path().join("r11.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let mut kind = agentd::KindConfig::infinity(socket);
    kind.request_timeout = Duration::from_millis(300);
    kind.cancel_timeout = Duration::from_millis(300);
    let broker = Broker::new(
        mailroom,
        vec![kind],
        agentd::supervisor::Config {
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .expect("broker");
    let agent = broker
        .spawn(AgentSpec {
            name: AgentName::parse("r11").expect("name"),
            kind: "infinity".into(),
            cwd: h.cwd.path().into(),
            model: None,
            permissions: Default::default(),
            idle_after: None,
        })
        .await
        .expect("spawn");
    let mut phases = agent.phases();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *phases.borrow_and_update() != Phase::Idle {
            phases.changed().await.expect("phase");
        }
    })
    .await
    .expect("initial idle");
    let root = h
        .manager
        .lock()
        .await
        .list_sessions(None)
        .await
        .into_keys()
        .next()
        .expect("root");
    (broker, agent, root)
}

async fn r11_send(broker: &agentd::supervisor::Broker, agent: agentd::model::AgentId, text: &str) {
    use agentd::{mailroom::NewMessage, model::*};
    broker
        .send(NewMessage {
            from: Sender::Human,
            to: agent,
            body: Body::new(text).expect("body"),
            reply_to: None,
            client_key: ClientKey(text.into()),
            act: agentd::swarm::Act::Inform,
            task: None,
        })
        .await
        .expect("mail");
}

async fn r11_shutdown_command_tool(archive: bool) {
    use agentd::supervisor::Phase;
    let mut h = start_harness().await.expect("harness");
    let config_dir = h.cwd.path().join(".infinity");
    std::fs::create_dir(&config_dir).expect("config directory");
    let started = h.cwd.path().join("tool-started");
    let script =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/delayed_rap.py");
    std::fs::write(config_dir.join("rap.json"), serde_json::json!({"tool_sets":[{
        "type":"toolset_command", "command":format!("python3 '{}' '{}'", script.display(), started.display())
    }]}).to_string()).expect("config");
    let (broker, agent, root) = r11_broker(&h).await;
    let mut events = broker.events();
    h.client_tx
        .send(ClientMessage::Connect {
            root_thread_id: root.clone(),
            thread_id: None,
            keeps_session_alive: true,
        })
        .expect("observer connect");
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::Replay { .. })
    })
    .await;
    r11_send(&broker, agent.id, "start delayed work").await;
    h.ctrl.next_request().await;
    h.ctrl
        .send_tool_call("delayed-call", "delayed_work", serde_json::json!({}));
    h.ctrl.finish();
    wait_for_message(&mut h.daemon_rx, |m| {
        matches!(m, DaemonMessage::ResponseDone { .. })
    })
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("command tool invoked");
    let port: u16 = std::fs::read_to_string(&started)
        .expect("port")
        .parse()
        .expect("port number");
    h.client_tx
        .send(if archive {
            ClientMessage::ArchiveSession {
                root_thread_id: root.clone(),
            }
        } else {
            ClientMessage::ShutdownSession {
                root_thread_id: root.clone(),
            }
        })
        .expect("CLI quit/archive");
    tokio::time::timeout(Duration::from_secs(3), async {
        while tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("shutdown kills command RAP server before callback");
    r11_send(&broker, agent.id, "mail after CLI quit").await;
    if archive {
        let mut phases = agent.phases();
        tokio::time::timeout(Duration::from_secs(8), async {
            while !matches!(*phases.borrow_and_update(), Phase::NeedsRecovery { .. }) {
                phases.changed().await.expect("phase");
            }
        })
        .await
        .expect("archived root must require recovery");
        while let Ok(event) = events.try_recv() {
            assert!(
                !matches!(
                    event,
                    agentd::supervisor::Event::PhaseChanged {
                        phase: Phase::Crashed { .. } | Phase::Failed { .. },
                        ..
                    }
                ),
                "archive must enter NeedsRecovery directly"
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(300), h.ctrl.next_request())
                .await
                .is_err(),
            "archived settlement must not run inference"
        );
        let stored = std::fs::read_to_string(
            h._state_dir
                .path()
                .join("threads")
                .join(format!("{}.json", root.id)),
        )
        .expect("history");
        assert!(stored.contains("tool server stopped by session shutdown"));
        assert!(
            !stored.contains("mail after CLI quit"),
            "archive retains broker mail"
        );
    } else {
        let request = tokio::time::timeout(Duration::from_secs(8), h.ctrl.next_request())
            .await
            .expect("shutdown must not strand mail behind killed tool");
        let mut history = format!("{:?}", request.chat_history);
        if !history.contains("mail after CLI quit") {
            assert!(history.contains("tool server stopped by session shutdown"));
            h.ctrl.send_text("shutdown handled");
            h.ctrl.finish();
            let next = tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
                .await
                .expect("deferred mail");
            history = format!("{:?}", next.chat_history);
        }
        assert!(
            history.contains("mail after CLI quit")
                && history.contains("tool server stopped by session shutdown")
        );
        h.ctrl.send_text("recovered");
        h.ctrl.finish();
    }
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "current_thread")]
async fn r11_shutdown_during_command_tool_recovers_mail() {
    tokio::task::LocalSet::new()
        .run_until(r11_shutdown_command_tool(false))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r11_archive_during_command_tool_needs_recovery() {
    tokio::task::LocalSet::new()
        .run_until(r11_shutdown_command_tool(true))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r11a_detached_executor_panic() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let (broker, agent, _) = r11_broker(&h).await;
            r11_send(&broker, agent.id, "sleep invalid duration").await;
            h.ctrl.next_request().await;
            h.ctrl
                .send_tool_call("panic-sleep", "sleep", serde_json::json!({"seconds":1e100}));
            h.ctrl.finish();
            let request = tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request())
                .await
                .expect("invalid duration must deliver a tool error and resume inference");
            assert!(format!("{:?}", request.chat_history).contains("Error:"));
            h.ctrl.send_text("invalid duration handled");
            h.ctrl.finish();
            broker.shutdown().await.expect("shutdown");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r12a_shutdown_recovery_with_usage_must_not_loop() {
    r12a_shutdown_recovery_usage_case(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r12a_shutdown_recovery_without_usage_control() {
    r12a_shutdown_recovery_usage_case(false).await;
}

async fn r12a_shutdown_recovery_usage_case(report_usage: bool) {
    use agentd::supervisor::Phase;
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let marker = r12o::slow_rap(&h);
            let (broker, agent, root) = r11_broker(&h).await;
            h.client_tx
                .send(ClientMessage::Connect {
                    root_thread_id: root.clone(),
                    thread_id: None,
                    keeps_session_alive: true,
                })
                .expect("observer connect");
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::Replay { .. })
            })
            .await;
            r11_send(&broker, agent.id, "start work before shutdown").await;
            h.ctrl.next_request().await;
            h.ctrl
                .send_tool_call("pre-shutdown", "delayed_work", serde_json::json!({}));
            h.ctrl.finish();
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::ResponseDone { .. })
            })
            .await;
            r12o::invocations(&marker).await;
            r11_send(&broker, agent.id, "mail pending during shutdown").await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            h.client_tx
                .send(ClientMessage::ShutdownSession {
                    root_thread_id: root.clone(),
                })
                .expect("one shutdown");
            tokio::time::timeout(Duration::from_secs(2), h.ctrl.next_request())
                .await
                .expect("first recovery");
            for round in 0..6 {
                let status = h.manager.lock().await.list_sessions(None).await[&root]
                    .status
                    .clone();
                eprintln!(
                    "R12 recovery round={round}, daemon status={status:?}, agent phase={:?}",
                    *agent.phases().borrow()
                );
                r11_send(
                    &broker,
                    agent.id,
                    &format!("mail during healthy round {round}"),
                )
                .await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                h.ctrl.send_tool_call(
                    &format!("post-shutdown-{round}"),
                    "delayed_work",
                    serde_json::json!({}),
                );
                h.ctrl.finish_with_usage(report_usage.then_some(
                    infinity_provider_protocol::Usage {
                        input_tokens: 10,
                        output_tokens: 5,
                        total_tokens: 15,
                        ..Default::default()
                    },
                ));
                wait_for_message(&mut h.daemon_rx, |message| {
                    matches!(message, DaemonMessage::ResponseDone { .. })
                })
                .await;
                assert_ne!(
                    h.manager.lock().await.list_sessions(None).await[&root].status,
                    SessionStatus::Stopped,
                    "recovered live driver must stay resumed after token usage"
                );
                if tokio::time::timeout(Duration::from_secs(1), h.ctrl.next_request())
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let phase = agent.phases().borrow().clone();
            eprintln!(
                "R12 after one shutdown and ordinary usage-bearing tool completions: {phase:?}"
            );
            broker.shutdown().await.expect("shutdown broker");
            assert!(
                !matches!(phase, Phase::Failed { .. }),
                "one shutdown must not exhaust crash budget during healthy tool turns: {phase:?}"
            );
        })
        .await;
}

mod r12o {
    use super::*;
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::supervisor::{AgentHandle, AgentSpec, Broker, Config, Event, Phase, TurnOutcome};
    use std::cell::RefCell;
    use std::rc::Rc;

    pub(super) struct Rig {
        pub broker: Broker,
        pub agent: AgentHandle,
        pub root: infinity_protocol::ThreadRef,
        pub mailroom: Arc<Mailroom>,
        pub log: Rc<RefCell<Vec<Event>>>,
    }

    pub(super) async fn wait_phase(
        agent: &AgentHandle,
        f: impl Fn(&Phase) -> bool,
        t: Duration,
    ) -> bool {
        let mut phases = agent.phases();
        tokio::time::timeout(t, async {
            while !f(&phases.borrow_and_update()) {
                phases.changed().await.expect("phase");
            }
        })
        .await
        .is_ok()
    }

    pub(super) async fn rig(
        h: &TestHarness,
        request: Duration,
        cancel: Duration,
        cfg: Config,
        perm: Option<Duration>,
    ) -> Rig {
        rig_with_stall(h, request, cancel, cfg, perm, Duration::from_secs(600)).await
    }

    pub(super) async fn rig_with_stall(
        h: &TestHarness,
        request: Duration,
        cancel: Duration,
        cfg: Config,
        perm: Option<Duration>,
        stall: Duration,
    ) -> Rig {
        let socket = h.cwd.path().join("r12o.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
        let manager = h.manager.clone();
        tokio::task::spawn_local(async move {
            loop {
                let (s, _) = listener.accept().await.expect("accept");
                tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                    s,
                    manager.clone(),
                ));
            }
        });
        let mailroom = Arc::new(
            Mailroom::open(h.cwd.path().join("r12o.db"), Default::default())
                .await
                .expect("mailroom"),
        );
        let mut kind = agentd::KindConfig::infinity(socket);
        kind.stall_timeout = stall;
        kind.request_timeout = request;
        kind.cancel_timeout = cancel;
        let broker = Broker::new(mailroom.clone(), vec![kind], cfg).expect("broker");
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut events = broker.events();
        let sink = log.clone();
        tokio::task::spawn_local(async move {
            loop {
                match events.recv().await {
                    Ok(e) => sink.borrow_mut().push(e),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("R12O lagged {n}")
                    }
                    Err(_) => break,
                }
            }
        });
        let agent = broker
            .spawn(AgentSpec {
                name: AgentName::parse("r12o").expect("name"),
                kind: "infinity".into(),
                cwd: h.cwd.path().into(),
                model: None,
                permissions: PermissionPolicy {
                    timeout: perm,
                    ..Default::default()
                },
                idle_after: None,
            })
            .await
            .expect("spawn");
        assert!(
            wait_phase(&agent, |p| *p == Phase::Idle, Duration::from_secs(5)).await,
            "initial idle"
        );
        let root = h
            .manager
            .lock()
            .await
            .list_sessions(None)
            .await
            .into_keys()
            .next()
            .expect("root");
        Rig {
            broker,
            agent,
            root,
            mailroom,
            log,
        }
    }

    pub(super) async fn send(r: &Rig, text: &str) -> MessageId {
        let (_, env) = r
            .broker
            .send(NewMessage {
                from: Sender::Human,
                to: r.agent.id,
                body: Body::new(text).expect("body"),
                reply_to: None,
                client_key: ClientKey(text.into()),
                act: agentd::swarm::Act::Inform,
                task: None,
            })
            .await
            .expect("mail");
        env.id()
    }

    pub(super) fn summary(r: &Rig) -> String {
        let log = r.log.borrow();
        let n = |f: &dyn Fn(&Event) -> bool| log.iter().filter(|e| f(e)).count();
        format!(
            "crashed={} failed={} needs_recovery={} cancelled={} completed={} phase={:?}",
            n(&|e| matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::Crashed { .. },
                    ..
                }
            )),
            n(&|e| matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::Failed { .. },
                    ..
                }
            )),
            n(&|e| matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::NeedsRecovery { .. },
                    ..
                }
            )),
            n(&|e| matches!(
                e,
                Event::TurnEnded {
                    outcome: TurnOutcome::Cancelled,
                    ..
                }
            )),
            n(&|e| matches!(
                e,
                Event::TurnEnded {
                    outcome: TurnOutcome::Completed,
                    ..
                }
            )),
            r.agent.phase(),
        )
    }

    pub(super) async fn drain(h: &mut TestHarness, dur: Duration) -> Vec<String> {
        let mut out = Vec::new();
        let end = tokio::time::Instant::now() + dur;
        while let Ok(req) = tokio::time::timeout_at(end, h.ctrl.next_request()).await {
            out.push(format!("{:?}", req.chat_history));
            h.ctrl.send_text("ok");
            h.ctrl.finish();
        }
        out
    }

    pub(super) fn slow_rap(h: &TestHarness) -> std::path::PathBuf {
        let dir = h.cwd.path().join(".infinity");
        std::fs::create_dir(&dir).expect("config dir");
        let marker = h.cwd.path().join("invoked");
        let script =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/delayed_rap.py");
        std::fs::write(
            dir.join("rap.json"),
            serde_json::json!({"tool_sets":[{"type":"toolset_command",
                "command":format!("python3 '{}' '{}'", script.display(), marker.display())}]})
            .to_string(),
        )
        .expect("rap config");
        marker
    }

    pub(super) async fn invocations(marker: &std::path::Path) -> Vec<u16> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(s) = std::fs::read_to_string(marker) {
                    let ports: Vec<u16> = s
                        .lines()
                        .map(|l| {
                            l.split(' ')
                                .next()
                                .expect("port")
                                .parse()
                                .expect("port number")
                        })
                        .collect();
                    if !ports.is_empty() {
                        return ports;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("slow_work invoked")
    }

    pub(super) async fn port_dead(port: u16) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ShutdownSession kills the command RAP server");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn r12o_dangling_choice_cancel_loop() {
    use agentd::supervisor::{Event, Phase, TurnOutcome};
    use infinity_agent_core::message::{InputMessage, InputMessageContent, UserChoiceRequired};
    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let cfg = agentd::supervisor::Config { backoff_initial: Duration::from_millis(10), backoff_max: Duration::from_millis(20), ..Default::default() };
        let r = r12o::rig(&h, Duration::from_secs(1), Duration::from_millis(300), cfg, Some(Duration::from_millis(30))).await;
        r12o::send(&r, "hello").await;
        h.ctrl.next_request().await;
        h.ctrl.send_text("hi");
        h.ctrl.finish();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !r.log.borrow().iter().any(|e| matches!(e, Event::TurnEnded { outcome: TurnOutcome::Completed, .. })) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("first turn completes");
        tokio::time::sleep(Duration::from_millis(200)).await;
        // A late RAP user_choice callback whose tool call already has a result.
        assert!(h.manager.lock().await.send_input((InputMessage {
            content: InputMessageContent::UserChoice(UserChoiceRequired {
                content_type: "user_choice_required".into(), id: "late-choice".into(), call_id: None,
                prompt: "Choose".into(), choices: vec!["A".into(), "B".into()], default: 0,
                response_url: "http://127.0.0.1:9/choice".into(),
            }),
            group_id: r.root.id.clone(), metadata: None, synthetic: None, display_as: None, subscription: false,
        }, None)).await);
        tokio::time::sleep(Duration::from_secs(6)).await;
        eprintln!("R12O dangling choice after 6s: {}", r12o::summary(&r));
        let after = r12o::send(&r, "mail after choice").await;
        let reqs = r12o::drain(&mut h, Duration::from_secs(4)).await;
        eprintln!("R12O dangling choice after 10s: {} after={:?} inference_requests={}", r12o::summary(&r), r.mailroom.delivery(after).await.expect("after"), reqs.len());
        h.client_tx.send(ClientMessage::Connect { root_thread_id: r.root.clone(), thread_id: None, keeps_session_alive: false }).expect("inspect");
        let replay = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
        if let DaemonMessage::Replay { pending_choices, idle, in_progress, .. } = replay {
            eprintln!("R12O dangling choice replay: pending_choices={} idle={idle} in_progress={in_progress}", pending_choices.len());
        }
        let failed = r.log.borrow().iter().any(|e| matches!(e, Event::PhaseChanged { phase: Phase::Failed { .. }, .. }));
        let cancelled = r.log.borrow().iter().any(|e| matches!(e, Event::TurnEnded { outcome: TurnOutcome::Cancelled, .. }));
        assert!(cancelled && !failed && !reqs.is_empty(), "agent neither failed nor recovered: {}", r12o::summary(&r));
        r.broker.shutdown().await.expect("shutdown");
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r12o_repeated_cli_quit_crash_budget() {
    use agentd::supervisor::Phase;
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let marker = r12o::slow_rap(&h);
            let cfg = agentd::supervisor::Config {
                backoff_initial: Duration::from_millis(10),
                backoff_max: Duration::from_millis(20),
                ..Default::default()
            };
            let r = r12o::rig(
                &h,
                Duration::from_secs(1),
                Duration::from_secs(1),
                cfg,
                None,
            )
            .await;
            let mut quits = 0;
            for i in 0..6 {
                r12o::send(&r, &format!("work item {i}")).await;
                let Ok(_) =
                    tokio::time::timeout(Duration::from_secs(15), h.ctrl.next_request()).await
                else {
                    eprintln!(
                        "R12O quit budget: no inference for item {i}; {}",
                        r12o::summary(&r)
                    );
                    break;
                };
                h.ctrl
                    .send_tool_call(&format!("slow-{i}"), "delayed_work", serde_json::json!({}));
                h.ctrl.finish();
                let port = r12o::invocations(&marker).await[0];
                std::fs::remove_file(&marker).expect("clear invocation");
                h.client_tx
                    .send(ClientMessage::ShutdownSession {
                        root_thread_id: r.root.clone(),
                    })
                    .expect("quit");
                quits += 1;
                r12o::port_dead(port).await;
                let request = tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
                    .await
                    .expect("daemon settles killed call");
                assert!(
                    format!("{:?}", request.chat_history)
                        .contains("tool server stopped by session shutdown")
                );
                h.ctrl.send_text("shutdown handled");
                h.ctrl.finish();
                let settled = r12o::wait_phase(
                    &r.agent,
                    |p| matches!(p, Phase::Idle | Phase::Failed { .. }),
                    Duration::from_secs(10),
                )
                .await;
                eprintln!(
                    "R12O quit budget: after quit {quits} settled={settled} {}",
                    r12o::summary(&r)
                );
                if matches!(r.agent.phase(), Phase::Failed { .. }) {
                    break;
                }
            }
            let failed = matches!(r.agent.phase(), Phase::Failed { .. });
            eprintln!(
                "R12O quit budget: quits={quits} failed={failed} {}",
                r12o::summary(&r)
            );
            assert!(
                !failed,
                "routine CLI quits drove the agent to Failed after {quits} quits"
            );
            assert_eq!(quits, 6, "all six quits must recover");
            r.broker.shutdown().await.expect("shutdown");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r12o_quit_during_plain_completion() {
    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let cfg = agentd::supervisor::Config { backoff_initial: Duration::from_millis(10), backoff_max: Duration::from_millis(20), ..Default::default() };
        let r = r12o::rig(&h, Duration::from_secs(1), Duration::from_secs(1), cfg, None).await;
        let first = r12o::send(&r, "write a long answer").await;
        h.ctrl.next_request().await;
        h.ctrl.send_text("partial answer ");
        tokio::time::sleep(Duration::from_millis(300)).await;
        h.client_tx.send(ClientMessage::ShutdownSession { root_thread_id: r.root.clone() }).expect("quit");
        tokio::time::sleep(Duration::from_secs(3)).await;
        eprintln!("R12O plain quit after 3s: {} first={:?}", r12o::summary(&r), r.mailroom.delivery(first).await.expect("first"));
        h.ctrl.send_text("rest of answer");
        h.ctrl.finish();
        let late = r12o::drain(&mut h, Duration::from_secs(2)).await;
        let next = r12o::send(&r, "follow-up").await;
        let reqs = r12o::drain(&mut h, Duration::from_secs(5)).await;
        eprintln!(
            "R12O plain quit: extra_requests_before_followup={} followup_requests={} followup_sees_rest={} followup_sees_partial={} next={:?} {}",
            late.len(), reqs.len(),
            reqs.last().is_some_and(|h| h.contains("rest of answer")),
            reqs.last().is_some_and(|h| h.contains("partial answer")),
            r.mailroom.delivery(next).await.expect("next"), r12o::summary(&r)
        );
        assert!(reqs.last().is_some_and(|history| history.contains("rest of answer")), "CLI quit abandoned the streaming reply");
        assert!(!r.log.borrow().iter().any(|event| matches!(event, agentd::supervisor::Event::TurnEnded { outcome: agentd::supervisor::TurnOutcome::Cancelled, .. })), "CLI quit cancelled the live completion");
        r.broker.shutdown().await.expect("shutdown");
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r12_post_shutdown_mail_does_not_wait_for_receipt_expiry() {
    use agentd::supervisor::{Config, Phase};
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let marker = r12o::slow_rap(&h);
            let r = r12o::rig(
                &h,
                Duration::from_secs(10),
                Duration::from_secs(2),
                Config {
                    backoff_initial: Duration::from_millis(10),
                    backoff_max: Duration::from_millis(20),
                    ..Default::default()
                },
                None,
            )
            .await;
            r12o::send(&r, "start command").await;
            h.ctrl.next_request().await;
            h.ctrl
                .send_tool_call("command", "delayed_work", serde_json::json!({}));
            h.ctrl
                .finish_with_usage(Some(infinity_provider_protocol::Usage {
                    total_tokens: 15,
                    ..Default::default()
                }));
            let port = r12o::invocations(&marker).await[0];
            h.client_tx
                .send(ClientMessage::ShutdownSession {
                    root_thread_id: r.root.clone(),
                })
                .expect("quit");
            r12o::port_dead(port).await;
            let request = tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
                .await
                .expect("daemon settles killed call");
            assert!(
                format!("{:?}", request.chat_history)
                    .contains("tool server stopped by session shutdown")
            );
            h.ctrl.send_text("shutdown handled");
            h.ctrl.finish();
            assert!(
                r12o::wait_phase(&r.agent, |p| *p == Phase::Idle, Duration::from_secs(5)).await
            );
            let id = r12o::send(&r, "follow-up after shutdown").await;
            let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request())
                .await
                .expect("follow-up before ten-second receipt expiry");
            assert!(format!("{:?}", request.chat_history).contains("follow-up after shutdown"));
            h.ctrl.send_text("delivered promptly");
            h.ctrl
                .finish_with_usage(Some(infinity_provider_protocol::Usage {
                    total_tokens: 30,
                    ..Default::default()
                }));
            assert!(
                r12o::wait_phase(&r.agent, |p| *p == Phase::Idle, Duration::from_secs(5)).await
            );
            let delivery = r.mailroom.delivery(id).await.expect("delivery");
            assert_eq!(delivery.state, agentd::model::DeliveryState::Consumed);
            assert_eq!(delivery.attempts, 1);
            assert!(!matches!(
                h.manager.lock().await.list_sessions(None).await[&r.root].status,
                SessionStatus::Stopped
            ));
            r.broker.shutdown().await.expect("shutdown");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r13a_quick_reconnect_must_not_hide_executor_shutdown() {
    use agentd::supervisor::Config;
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let marker = r12o::slow_rap(&h);
            let cfg = Config {
                backoff_initial: Duration::from_millis(10),
                backoff_max: Duration::from_millis(20),
                ..Default::default()
            };
            let r = r12o::rig(
                &h,
                Duration::from_millis(300),
                Duration::from_millis(300),
                cfg,
                None,
            )
            .await;
            r12o::send(&r, "start work before quick reconnect").await;
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call(
                "quick-reconnect-call",
                "delayed_work",
                serde_json::json!({}),
            );
            h.ctrl.finish();
            let port = r12o::invocations(&marker).await[0];
            let waiting = r12o::send(&r, "mail waiting on killed executor").await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            h.client_tx
                .send(ClientMessage::ShutdownSession {
                    root_thread_id: r.root.clone(),
                })
                .expect("shutdown");
            h.client_tx
                .send(ClientMessage::Connect {
                    root_thread_id: r.root.clone(),
                    thread_id: None,
                    keeps_session_alive: true,
                })
                .expect("immediate connect");
            r12o::port_dead(port).await;
            let recovered =
                tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request()).await;
            let phase = r.agent.phase();
            let delivery = r.mailroom.delivery(waiting).await.expect("delivery");
            eprintln!(
                "R13 quick reconnect: recovered={} phase={phase:?} delivery={delivery:?} {}",
                recovered.is_ok(),
                r12o::summary(&r)
            );
            let request =
                recovered.expect("shutdown plus immediate Connect must deliver deferred mail");
            let history = format!("{:?}", request.chat_history);
            assert!(history.contains("tool server stopped by session shutdown"));
            assert_eq!(
                history.matches("mail waiting on killed executor").count(),
                1
            );
            h.ctrl.send_text("recovered");
            h.ctrl.finish();
            assert!(
                r12o::wait_phase(
                    &r.agent,
                    |p| *p == agentd::supervisor::Phase::Idle,
                    Duration::from_secs(4)
                )
                .await
            );
            let delivery = r.mailroom.delivery(waiting).await.expect("delivery");
            assert_eq!(delivery.state, agentd::model::DeliveryState::Consumed);
            assert_eq!(delivery.attempts, 1);
            r.broker.shutdown().await.expect("broker shutdown");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r13a_shutdown_preserved_stream_delivers_followup() {
    use agentd::supervisor::{Config, Event, TurnOutcome};
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let marker = r12o::slow_rap(&h);
            let cfg = Config {
                backoff_initial: Duration::from_millis(10),
                backoff_max: Duration::from_millis(20),
                ..Default::default()
            };
            let r = r12o::rig(
                &h,
                Duration::from_secs(2),
                Duration::from_millis(100),
                cfg,
                None,
            )
            .await;
            r12o::send(&r, "start stream").await;
            h.ctrl.next_request().await;
            h.ctrl.send_text("partial answer ");
            tokio::time::sleep(Duration::from_millis(100)).await;
            h.client_tx
                .send(ClientMessage::ShutdownSession {
                    root_thread_id: r.root.clone(),
                })
                .expect("shutdown");
            tokio::time::sleep(Duration::from_millis(300)).await;
            let mail = r12o::send(&r, "follow-up after stream").await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            h.ctrl.send_text("rest of answer");
            h.ctrl
                .send_tool_call("after-stream", "delayed_work", serde_json::json!({}));
            h.ctrl.finish();
            r12o::invocations(&marker).await;
            r.agent.stop().await.expect("detach during next tool");
            let agent = r
                .broker
                .spawn(agentd::supervisor::AgentSpec {
                    name: agentd::model::AgentName::parse("r12o").expect("name"),
                    kind: "infinity".into(),
                    cwd: h.cwd.path().into(),
                    model: None,
                    permissions: Default::default(),
                    idle_after: None,
                })
                .await
                .expect("reattach during next tool");
            let request = tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
                .await
                .expect("mail after completed stream");
            let history = format!("{:?}", request.chat_history);
            assert!(
                history.contains("Delayed work complete"),
                "quit must not leave a stale Interrupt marker that cancels the next tool: {history}"
            );
            assert!(
                history.contains("partial answer")
                    && history.contains("rest of answer")
                    && history.contains("follow-up after stream")
            );
            h.ctrl.send_text("delivered");
            h.ctrl.finish();
            assert!(
                r12o::wait_phase(
                    &agent,
                    |p| *p == agentd::supervisor::Phase::Idle,
                    Duration::from_secs(4)
                )
                .await
            );
            assert_eq!(
                r.mailroom.delivery(mail).await.expect("mail").state,
                agentd::model::DeliveryState::Consumed
            );
            assert!(!r.log.borrow().iter().any(|e| matches!(
                e,
                Event::TurnEnded {
                    outcome: TurnOutcome::Cancelled,
                    ..
                }
            )));
            r.broker.shutdown().await.expect("shutdown");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r13a_interrupt_must_not_resume_archived_thread() {
    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let root = create_session_and_chat(&mut h).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        h.client_tx.send(ClientMessage::ArchiveSession { root_thread_id: root.clone() }).expect("archive");
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::SessionsUpdated { sessions } if sessions.get(&root).is_some_and(|s| s.status == SessionStatus::Archived))).await;
        let before = h.manager.lock().await.session_store.lock().await.is_shut_down(&root.id);
        assert!(before);
        h.client_tx.send(ClientMessage::Interrupt { thread_id: root.clone() }).expect("interrupt archived root");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let after = h.manager.lock().await.session_store.lock().await.is_shut_down(&root.id);
        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id: root.clone(), message_id: "archived-mail".into(), from: "peer".into(), text: "must not awaken archived thread".into() }).expect("late peer mail");
        let inference = tokio::time::timeout(Duration::from_millis(500), h.ctrl.next_request()).await;
        eprintln!("R13 archived interrupt: shut_down before={before} after={after} inference={}", inference.is_ok());
        if inference.is_ok() { h.ctrl.send_text("unexpected archived inference"); h.ctrl.finish(); }
        assert!(after && inference.is_err(), "orphan Interrupt must preserve the archived root's event admission gate");
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r13o_shutdown_while_agentd_detached_recovers_tool() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use agentd::model::AgentName;
            use agentd::supervisor::{AgentSpec, Phase};
            let mut h = start_harness().await.expect("harness");
            let config_dir = h.cwd.path().join(".infinity");
            std::fs::create_dir(&config_dir).expect("config directory");
            let started = h.cwd.path().join("tool-started");
            let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/support/delayed_rap.py");
            std::fs::write(config_dir.join("rap.json"), serde_json::json!({"tool_sets":[{
                "type":"toolset_command", "command":format!("python3 '{}' '{}'", script.display(), started.display())
            }]}).to_string()).expect("config");
            let (broker, agent, root) = r11_broker(&h).await;
            h.client_tx
                .send(ClientMessage::Connect { root_thread_id: root.clone(), thread_id: None, keeps_session_alive: true })
                .expect("observer connect");
            wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
            r11_send(&broker, agent.id, "start delayed work").await;
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call("delayed-call", "delayed_work", serde_json::json!({}));
            h.ctrl.finish();
            wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ResponseDone { .. })).await;
            tokio::time::timeout(Duration::from_secs(3), async {
                while !started.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("command tool invoked");
            let port: u16 = std::fs::read_to_string(&started).expect("port").parse().expect("port");
            agent.stop().await.expect("agentd detaches");
            h.client_tx
                .send(ClientMessage::ShutdownSession { root_thread_id: root.clone() })
                .expect("CLI quit while agentd is detached");
            tokio::time::timeout(Duration::from_secs(3), async {
                while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("shutdown kills command RAP server before callback");
            let status = h.manager.lock().await.list_sessions(None).await.get(&root).map(|s| s.status.clone());
            eprintln!("r13o status before reattach: {status:?}");
            let agent = broker
                .spawn(AgentSpec {
                    name: AgentName::parse("r11").expect("name"),
                    kind: "infinity".into(),
                    cwd: h.cwd.path().into(),
                    model: None,
                    permissions: Default::default(),
                    idle_after: None,
                })
                .await
                .expect("reattach");
            r11_send(&broker, agent.id, "mail after detached quit").await;
            let request = tokio::time::timeout(Duration::from_secs(10), h.ctrl.next_request()).await;
            let status = h.manager.lock().await.list_sessions(None).await.get(&root).map(|s| s.status.clone());
            eprintln!("r13o after 10 s: phase={:?} status={status:?} inference={}", agent.phase(), request.is_ok());
            let request = request.expect("quit while agentd was detached must not strand mail behind the killed tool");
            let mut history = format!("{:?}", request.chat_history);
            if !history.contains("mail after detached quit") {
                assert!(history.contains("tool server stopped by session shutdown"));
                h.ctrl.send_text("tool shutdown handled"); h.ctrl.finish();
                let next = tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request()).await.expect("deferred mail");
                history = format!("{:?}", next.chat_history);
            }
            assert!(history.contains("mail after detached quit") && history.contains("tool server stopped by session shutdown"));
            assert!(!matches!(agent.phase(), Phase::Failed { .. }));
            h.ctrl.send_text("recovered");
            h.ctrl.finish();
            broker.shutdown().await.expect("shutdown");
        })
        .await;
}

/// A client Interrupt for an archived root with no live driver.
#[tokio::test(flavor = "current_thread")]
async fn r13o_interrupt_on_archived_root_reopens_stopped_gate() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let root = create_session_and_chat(&mut h).await;
            h.client_tx
                .send(ClientMessage::ArchiveSession { root_thread_id: root.clone() })
                .expect("archive");
            tokio::time::sleep(Duration::from_millis(200)).await;
            let shut = |h: &TestHarness| {
                let m = h.manager.clone();
                let id = root.id.clone();
                async move { m.lock().await.session_store.lock().await.is_shut_down(&id) }
            };
            assert!(shut(&h).await, "archive marks the session shut down");
            h.client_tx
                .send(ClientMessage::Interrupt { thread_id: root.clone() })
                .expect("interrupt");
            tokio::time::sleep(Duration::from_millis(300)).await;
            let after_interrupt = shut(&h).await;
            h.client_tx
                .send(ClientMessage::DeliverPeerMessage {
                    thread_id: root.clone(),
                    message_id: "late-peer".into(),
                    from: "agent:late".into(),
                    text: "late event for archived root".into(),
                })
                .expect("peer");
            let inference = tokio::time::timeout(Duration::from_secs(2), h.ctrl.next_request()).await.is_ok();
            let status = h.manager.lock().await.list_sessions(None).await.get(&root).map(|s| s.status.clone());
            eprintln!("r13o archived: shut_down_after_interrupt={after_interrupt} event_input_ran_inference={inference} status={status:?}");
            assert!(after_interrupt, "an Interrupt must not reopen an archived session's stopped gate");
            assert!(!inference, "event-style input must not run inference on an archived session");
        })
        .await;
}

/// A client Connect to an archived root.
#[tokio::test(flavor = "current_thread")]
async fn r13o_connect_on_archived_root_reopens_stopped_gate() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let root = create_session_and_chat(&mut h).await;
            h.client_tx
                .send(ClientMessage::ArchiveSession { root_thread_id: root.clone() })
                .expect("archive");
            tokio::time::sleep(Duration::from_millis(200)).await;
            let shut = |h: &TestHarness| {
                let m = h.manager.clone();
                let id = root.id.clone();
                async move { m.lock().await.session_store.lock().await.is_shut_down(&id) }
            };
            assert!(shut(&h).await, "archive marks the session shut down");
            h.client_tx
                .send(ClientMessage::Connect { root_thread_id: root.clone(), thread_id: None, keeps_session_alive: false })
                .expect("connect");
            wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Connected { .. } | DaemonMessage::Error { .. })).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let after_connect = shut(&h).await;
            h.client_tx
                .send(ClientMessage::DeliverPeerMessage {
                    thread_id: root.clone(),
                    message_id: "late-peer".into(),
                    from: "agent:late".into(),
                    text: "late event for archived root".into(),
                })
                .expect("peer");
            let inference = tokio::time::timeout(Duration::from_secs(2), h.ctrl.next_request()).await.is_ok();
            eprintln!("r13o archived connect: shut_down_after_connect={after_connect} event_input_ran_inference={inference}");
            assert!(after_connect, "a Connect must not reopen an archived session's stopped gate");
            assert!(!inference, "event-style input must not run inference on an archived session");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r13_shutdown_settles_calls_from_retained_evicted_toolset() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut h = start_harness().await.expect("harness");
            let marker = r12o::slow_rap(&h);
            let (broker, agent, root) = r11_broker(&h).await;
            r11_send(&broker, agent.id, "keep the same driver alive").await;
            h.ctrl.next_request().await;
            for i in 0..2 {
                h.ctrl.send_tool_call(
                    &format!("retained-{i}"),
                    "delayed_work",
                    serde_json::json!({}),
                );
                h.ctrl.finish();
                let port = r12o::invocations(&marker).await[0];
                std::fs::remove_file(&marker).expect("clear marker");
                h.client_tx
                    .send(ClientMessage::ShutdownSession {
                        root_thread_id: root.clone(),
                    })
                    .expect("quit");
                r12o::port_dead(port).await;
                let request = tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request())
                    .await
                    .expect("each killed call settles");
                let history = format!("{:?}", request.chat_history);
                assert_eq!(
                    history
                        .matches("Error: tool server stopped by session shutdown")
                        .count(),
                    i + 1
                );
            }
            h.ctrl.send_text("both calls settled");
            h.ctrl.finish();
            broker.shutdown().await.expect("shutdown");
        })
        .await;
}

mod r14 {
    use super::*;
    use agentd::model::PermissionAnswer;
    use agentd::supervisor::Phase;
    #[tokio::test(flavor = "current_thread")]
    async fn r13a_shutdown_preserved_stream_needs_automatic_exit() {
        use agentd::supervisor::Config;
        tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let cfg = Config { backoff_initial: Duration::from_millis(10), backoff_max: Duration::from_millis(20), ..Default::default() };
        let r = r12o::rig_with_stall(&h, Duration::from_millis(100), Duration::from_millis(100), cfg, None, Duration::from_millis(300)).await;
        r12o::send(&r, "start stream that remains open").await;
        h.ctrl.next_request().await;
        h.ctrl.send_text("partial answer");
        tokio::time::sleep(Duration::from_millis(100)).await;
        h.client_tx.send(ClientMessage::ShutdownSession { root_thread_id: r.root.clone() }).expect("shutdown");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mail = r12o::send(&r, "follow-up waiting for stalled stream").await;
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await;
        let escaped = request.is_ok();
        if let Ok(request) = request { assert!(format!("{:?}", request.chat_history).contains("follow-up waiting for stalled stream")); h.ctrl.send_text("recovered"); h.ctrl.finish(); }
        assert!(r.log.borrow().iter().any(|event| matches!(event, agentd::supervisor::Event::Activity { text, .. } if text.contains("mail-starvation stall timeout"))), "must exit through stall timer");
        eprintln!("R13 preserved stream: escaped={escaped} phase={:?} mail={:?} {}", r.agent.phase(), r.mailroom.delivery(mail).await.expect("mail"), r12o::summary(&r));
        r.broker.shutdown().await.expect("broker shutdown");
        assert!(escaped, "recovered streaming session has no deadline and no operator-free exit");
    }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r11a_shutdown_busy() {
        use agentd::mailroom::{Mailroom, NewMessage};
        use agentd::model::*;
        use agentd::supervisor::{AgentSpec, Broker, Phase};
        tokio::task::LocalSet::new()
            .run_until(async {
                let mut h = start_harness().await.expect("harness");
                let port = r10a_waiting_server().await.expect("RAP tool");
                rap_test_servers::write_rap_config(h.cwd.path(), port).expect("RAP config");
                let socket = h.cwd.path().join("broker-infinity.sock");
                let listener = tokio::net::UnixListener::bind(&socket).expect("socket");
                let manager = h.manager.clone();
                let accept = tokio::task::spawn_local(async move {
                    loop {
                        let (stream, _) = listener.accept().await.expect("accept");
                        tokio::task::spawn_local(infinity_daemon::client_handler::handle_client(
                            stream,
                            manager.clone(),
                        ));
                    }
                });
                let mailroom = Arc::new(
                    Mailroom::open(h.cwd.path().join("broker.db"), Default::default())
                        .await
                        .expect("mailroom"),
                );
                let broker = Broker::new(
                    mailroom.clone(),
                    vec![{
                        let mut kind = agentd::KindConfig::infinity(socket);
                        kind.stall_timeout = Duration::from_millis(300);
                        kind.cancel_timeout = Duration::from_millis(100);
                        kind.request_timeout = Duration::from_millis(200);
                        kind
                    }],
                    agentd::supervisor::Config {
                        backoff_initial: Duration::from_millis(10),
                        backoff_max: Duration::from_millis(10),
                        ..Default::default()
                    },
                )
                .expect("broker");
                let spec = AgentSpec {
                    name: AgentName::parse("model").expect("name"),
                    kind: "infinity".into(),
                    cwd: h.cwd.path().into(),
                    model: None,
                    permissions: PermissionPolicy {
                        timeout: Some(Duration::from_millis(30)),
                        ..Default::default()
                    },
                    idle_after: None,
                };
                let agent = broker.spawn(spec.clone()).await.expect("spawn");
                let mut phases = agent.phases();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while *phases.borrow_and_update() != Phase::Idle {
                        phases.changed().await.expect("phase");
                    }
                })
                .await
                .expect("initial idle");
                let root = h
                    .manager
                    .lock()
                    .await
                    .list_sessions(None)
                    .await
                    .into_keys()
                    .next()
                    .expect("root");
                h.client_tx
                    .send(ClientMessage::Connect {
                        root_thread_id: root.clone(),
                        thread_id: None,
                        keeps_session_alive: true,
                    })
                    .expect("connect observer");
                wait_for_message(&mut h.daemon_rx, |m| {
                    matches!(m, DaemonMessage::Replay { .. })
                })
                .await;
                let human = |body: &str| NewMessage {
                    from: Sender::Human,
                    to: agent.id,
                    body: Body::new(body).expect("body"),
                    reply_to: None,
                    client_key: ClientKey(body.into()),
                    act: agentd::swarm::Act::Inform,
                    task: None,
                };

                let mut timer_events = broker.events();
            let _trigger = broker
                    .send(human("start external work"))
                    .await
                    .expect("send");
                h.ctrl.next_request().await;
                h.ctrl
                    .send_tool_call("active-external", "waiting_choice", serde_json::json!({}));
                h.ctrl.finish();
                wait_for_message(&mut h.daemon_rx, |m| {
                    matches!(m, DaemonMessage::ResponseDone { .. })
                })
                .await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                h.client_tx
                    .send(ClientMessage::ShutdownSession {
                        root_thread_id: root.clone(),
                    })
                    .expect("shutdown");
                tokio::time::sleep(Duration::from_millis(100)).await;
                let mail = broker
                    .send(human("after CLI shutdown"))
                    .await
                    .expect("send")
                    .1
                    .id();
                let recovered =
                    tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request()).await;
                let state = mailroom.delivery(mail).await.expect("state").state;
                eprintln!(
                    "R11 busy shutdown: request={}, phase={:?}, delivery={:?}",
                    recovered.is_ok(),
                    *phases.borrow(),
                    state
                );
                let request = recovered
                    .as_ref()
                    .expect("stall timer releases external call");
                assert!(format!("{:?}", request.chat_history).contains("after CLI shutdown"));
                h.ctrl.send_text("recovered automatically");
                h.ctrl.finish();
                assert!(r12o::wait_phase(&agent, |p| *p == Phase::Idle, Duration::from_secs(3)).await);
            assert_eq!(mailroom.delivery(mail).await.expect("delivery").state, DeliveryState::Consumed);
            assert!(std::iter::from_fn(|| timer_events.try_recv().ok()).any(|event| matches!(event, agentd::supervisor::Event::Activity { text, .. } if text.contains("mail-starvation stall timeout"))), "must exit through stall timer");
            broker.shutdown().await.expect("broker shutdown");
                accept.abort();
                assert!(
                    recovered.is_ok(),
                    "one CLI shutdown stranded later mail: {:?}",
                    state
                );
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r14a_reused_call_id_shutdown_settles_each_invocation() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let mut h = start_harness().await.expect("harness");
                let marker = r12o::slow_rap(&h);
                let (broker, agent, root) = r11_broker(&h).await;
                r11_send(&broker, agent.id, "keep the same driver alive").await;
                h.ctrl.next_request().await;
                for i in 0..2 {
                    h.ctrl
                        .send_tool_call("reused-call", "delayed_work", serde_json::json!({}));
                    h.ctrl.finish();
                    let port = r12o::invocations(&marker).await[0];
                    std::fs::remove_file(&marker).expect("clear marker");
                    h.client_tx
                        .send(ClientMessage::ShutdownSession {
                            root_thread_id: root.clone(),
                        })
                        .expect("quit");
                    r12o::port_dead(port).await;
                    eprintln!(
                        "R14 reused id killed generation {i}; phase {:?}",
                        agent.phase()
                    );
                    if i == 1 {
                        r11_send(&broker, agent.id, "mail behind second killed reused call").await;
                    }
                    let request =
                        tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request())
                            .await
                            .expect("each killed call settles");
                    let history = format!("{:?}", request.chat_history);
                    assert_eq!(
                        history
                            .matches("Error: tool server stopped by session shutdown")
                            .count(),
                        i + 1
                    );
                }
                h.ctrl.send_text("both calls settled");
                h.ctrl.finish();
                broker.shutdown().await.expect("shutdown");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r14a_archived_live_driver_rejects_broker_mail() {
        tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let root = create_session_and_chat(&mut h).await;
        h.client_tx.send(ClientMessage::UserInput { thread_id: root.clone(), text: "start finite stream".into() }).expect("start");
        h.ctrl.next_request().await;
        h.ctrl.send_text("partial response");
        h.client_tx.send(ClientMessage::ArchiveSession { root_thread_id: root.clone() }).expect("archive");
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::SessionsUpdated { sessions } if sessions.get(&root).is_some_and(|s| s.status == SessionStatus::Archived))).await;
        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id: root.clone(), message_id: "r14-late-mail".into(), from: "agent:peer".into(), text: "mail must stay queued after archive".into() }).expect("in-flight broker mail");
        tokio::time::sleep(Duration::from_millis(100)).await;
        h.ctrl.send_text("finished response"); h.ctrl.finish();
        let request = tokio::time::timeout(Duration::from_secs(2), h.ctrl.next_request()).await;
        if let Ok(ref r) = request { eprintln!("R14 archived new inference: {:?}", r.chat_history); h.ctrl.send_text("unexpected"); h.ctrl.finish(); }
        assert!(request.is_err(), "mail sent after archive must not start a new inference on the existing driver");
    }).await;
    }

    pub struct ChoiceServer {
        pub port: u16,
        pub answers: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        _runtime: tokio::runtime::Runtime,
    }

    /// An external RAP server whose only tool asks the user a choice and
    /// reports the answer as its result.
    pub fn choice_server() -> ChoiceServer {
        let runtime = tokio::runtime::Runtime::new().expect("external runtime");
        let answers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let invocations = Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
        let port = runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let port = listener.local_addr().expect("addr").port();
            let answers = answers.clone();
            let invocations = invocations.clone();
            tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await.expect("accept");
                    let answers = answers.clone();
                    let invocations = invocations.clone();
                    tokio::spawn(async move {
                        let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                            let answers = answers.clone();
                            let invocations = invocations.clone();
                            async move {
                                use http_body_util::BodyExt;
                                let path = req.uri().path().to_owned();
                                let body = req.into_body().collect().await.expect("body").to_bytes();
                                let response = if path == "/.well-known/rap-toolset" {
                                    serde_json::json!({"name":"external", "endpoint":format!("http://127.0.0.1:{port}/invoke"), "tools":[{"name":"external_choice","description":"ask","inputSchema":{"type":"object"}}]}).to_string()
                                } else if path == "/invoke" {
                                    let inv: serde_json::Value = serde_json::from_slice(&body).expect("invocation");
                                    invocations.lock().expect("lock").push(inv.clone());
                                    tokio::spawn(async move {
                                        let choice = serde_json::json!({"type":"user_choice","group_id":inv["group_id"],"id":inv["id"],"call_id":inv["call_id"],"prompt":"Pick","choices":["A","B"],"default":0,"response_url":format!("http://127.0.0.1:{port}/choice")});
                                        let _ = reqwest::Client::new().post(inv["callback_url"].as_str().expect("callback")).json(&choice).send().await;
                                    });
                                    "{}".into()
                                } else if path == "/choice" {
                                    let answer: serde_json::Value = serde_json::from_slice(&body).expect("answer");
                                    answers.lock().expect("lock").push(answer.clone());
                                    let inv = invocations.lock().expect("lock").last().cloned().expect("invocation");
                                    tokio::spawn(async move {
                                        let result = serde_json::json!({"type":"tool_result","group_id":inv["group_id"],"id":inv["id"],"call_id":inv["call_id"],"text":format!("chose {}", answer["selected"])});
                                        let _ = reqwest::Client::new().post(inv["callback_url"].as_str().expect("callback")).json(&result).send().await;
                                    });
                                    "{}".into()
                                } else {
                                    "{}".into()
                                };
                                Ok::<_, std::convert::Infallible>(hyper::Response::new(http_body_util::Full::new(bytes::Bytes::from(response))))
                            }
                        });
                        let _ = hyper::server::conn::http1::Builder::new().serve_connection(hyper_util::rt::TokioIo::new(stream), service).await;
                    });
                }
            });
            port
        });
        ChoiceServer {
            port,
            answers,
            _runtime: runtime,
        }
    }

    pub async fn wait_permission(
        agent: &agentd::supervisor::AgentHandle,
        secs: u64,
    ) -> Option<agentd::model::PermissionRequest> {
        tokio::time::timeout(Duration::from_secs(secs), async {
            loop {
                if let Ok(p) = agent.pending_permissions().await
                    && let Some(p) = p.into_iter().next()
                {
                    return p;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .ok()
    }

    #[test]
    fn r14o_quit_with_external_choice_then_answer() {
        let server = choice_server();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(tokio::task::LocalSet::new().run_until(async {
            let mut h = start_harness().await.expect("harness");
            rap_test_servers::write_rap_config(h.cwd.path(), server.port).expect("config");
            let (broker, agent, root) = r11_broker(&h).await;
            r11_send(&broker, agent.id, "ask the external choice").await;
            h.ctrl.next_request().await;
            h.ctrl
                .send_tool_call("ext-choice", "external_choice", serde_json::json!({}));
            h.ctrl.finish();
            let first = wait_permission(&agent, 5)
                .await
                .expect("choice reaches agentd");
            assert_eq!(first.title, "Pick");
            h.client_tx
                .send(ClientMessage::ShutdownSession {
                    root_thread_id: root.clone(),
                })
                .expect("quit");
            tokio::time::sleep(Duration::from_millis(200)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let after = agent.pending_permissions().await.expect("pending");
            eprintln!(
                "r14o after quit+2s: phase={:?} pending={} store_has_choice={}",
                agent.phase(),
                after.len(),
                h.manager
                    .lock()
                    .await
                    .state_store()
                    .has_pending_choices(&root.id)
            );
            if let Some(p) = after.into_iter().next() {
                let answered = agent
                    .answer_permission(p.id, p.incarnation, PermissionAnswer::Choose("1".into()))
                    .await;
                eprintln!("r14o answered re-offered choice: {answered:?}");
            }
            let request = tokio::time::timeout(Duration::from_secs(8), h.ctrl.next_request()).await;
            eprintln!(
                "r14o after answer+8s: phase={:?} server_answers={:?} inference={}",
                agent.phase(),
                server.answers.lock().expect("lock"),
                request.is_ok()
            );
            let recovered = request.is_ok() || agent.phase() == Phase::Idle;
            if let Ok(request) = &request {
                eprintln!(
                    "r14o history has real result: {}",
                    format!("{:?}", request.chat_history).contains("chose 1")
                );
                h.ctrl.send_text("done");
                h.ctrl.finish();
            }
            assert!(
                recovered,
                "a quit must not leave an answered external choice stranded: phase={:?}",
                agent.phase()
            );
            broker.shutdown().await.expect("shutdown");
        }));
        drop(runtime);
    }

    #[test]
    fn r14o_quit_with_external_choice_exits_via_stall_timer() {
        let server = choice_server();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(tokio::task::LocalSet::new().run_until(async {
            let mut h = start_harness().await.expect("harness");
            rap_test_servers::write_rap_config(h.cwd.path(), server.port).expect("config");
            let r = r12o::rig_with_stall(&h, Duration::from_millis(300), Duration::from_millis(300), agentd::supervisor::Config::default(), None, Duration::from_millis(300)).await;
            let agent = r.agent.clone(); let root = r.root.clone(); let broker = &r.broker;
            r11_send(broker, agent.id, "ask the external choice").await;
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call("ext-choice", "external_choice", serde_json::json!({}));
            h.ctrl.finish();
            let first = wait_permission(&agent, 5).await.expect("choice reaches agentd");
            assert_eq!(first.title, "Pick");
            h.client_tx.send(ClientMessage::ShutdownSession { root_thread_id: root.clone() }).expect("quit");
            tokio::time::sleep(Duration::from_millis(200)).await;
            r12o::send(&r, "mail waiting behind external choice").await;
            let request = tokio::time::timeout(Duration::from_secs(8), h.ctrl.next_request()).await;
            eprintln!("r14o after answer+8s: phase={:?} server_answers={:?} inference={}", agent.phase(), server.answers.lock().expect("lock"), request.is_ok());
            let recovered = request.is_ok();
            assert!(r.log.borrow().iter().any(|event| matches!(event, agentd::supervisor::Event::Activity { text, .. } if text.contains("mail-starvation stall timeout"))), "must exit through stall timer");
            assert!(server.answers.lock().expect("answers").is_empty());
            if let Ok(request) = &request { eprintln!("r14o history has real result: {}", format!("{:?}", request.chat_history).contains("chose 1")); h.ctrl.send_text("done"); h.ctrl.finish(); }
            assert!(recovered, "a quit must not leave an answered external choice stranded: phase={:?}", agent.phase());
            broker.shutdown().await.expect("shutdown");
        }));
        drop(runtime);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r14o_archive_during_command_tool_keeps_running_tools() {
        tokio::task::LocalSet::new().run_until(async {
            let mut h = start_harness().await.expect("harness");
            let marker = r12o::slow_rap(&h);
            let (broker, agent, root) = r11_broker(&h).await;
            r11_send(&broker, agent.id, "start work before archive").await;
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call("before-archive", "delayed_work", serde_json::json!({}));
            h.ctrl.finish();
            let port = r12o::invocations(&marker).await[0];
            std::fs::remove_file(&marker).expect("clear marker");
            h.client_tx.send(ClientMessage::ArchiveSession { root_thread_id: root.clone() }).expect("archive");
            r12o::port_dead(port).await;
            let status = h.manager.lock().await.list_sessions(None).await.get(&root).map(|s| s.status.clone());
            let cont = tokio::time::timeout(Duration::from_secs(4), h.ctrl.next_request()).await;
            eprintln!("r14o archive: status={status:?} continuation_after_archive={}", cont.is_ok());
            if cont.is_err() {
                assert!(!marker.exists(), "rebooted_server_after_archive=false");
                let stored = std::fs::read_to_string(h._state_dir.path().join("threads").join(format!("{}.json", root.id))).expect("history");
                assert!(stored.contains("tool server stopped by session shutdown"), "terminal result persists without completion");
                eprintln!("rebooted_server_after_archive=false");
                broker.shutdown().await.expect("shutdown");
                return;
            }
            h.ctrl.send_tool_call("after-archive", "delayed_work", serde_json::json!({}));
            h.ctrl.finish();
            let rebooted = tokio::time::timeout(Duration::from_secs(4), async {
                while !marker.exists() { tokio::time::sleep(Duration::from_millis(20)).await; }
            }).await.is_ok();
            let second = tokio::time::timeout(Duration::from_secs(6), h.ctrl.next_request()).await;
            let real = second.as_ref().map(|r| format!("{:?}", r.chat_history).contains("Delayed work complete")).unwrap_or(false);
            let status = h.manager.lock().await.list_sessions(None).await.get(&root).map(|s| s.status.clone());
            eprintln!("r14o archive: rebooted_server_after_archive={rebooted} second_round={} real_result={real} status={status:?} agent={:?}", second.is_ok(), agent.phase());
            if second.is_ok() { h.ctrl.send_text("archived agent still working"); h.ctrl.finish(); }
            broker.shutdown().await.expect("shutdown");
            assert!(cont.is_err(), "continuation_after_archive must be false");
            assert!(!rebooted, "an archived session must not boot its servers and run new tool calls");
        }).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_choice_answer_receives_error() {
    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        create_session_and_chat(&mut h).await;
        h.client_tx.send(ClientMessage::UserChoiceAnswered { choice_id: "unknown-choice".into(), selected: 0 }).expect("answer");
        let error = wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Error { text, .. } if text.contains("Unknown or stale choice"))).await;
        assert!(format!("{error:?}").contains("unknown-choice"));
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn command_invocation_scope_rejects_late_callback_with_reused_id() {
    tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let marker = r12o::slow_rap(&h);
        let captures = h.cwd.path().join("invocations.jsonl");
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/delayed_rap.py");
        std::fs::write(h.cwd.path().join(".infinity/rap.json"), serde_json::json!({"tool_sets":[{"type":"toolset_command", "command":format!("python3 '{}' '{}' '{}'", script.display(), marker.display(), captures.display())}]}).to_string()).expect("config");
        let (broker, agent, root) = r11_broker(&h).await;
        h.client_tx.send(ClientMessage::Connect { root_thread_id: root.clone(), thread_id: None, keeps_session_alive: true }).expect("observer");
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })).await;
        r11_send(&broker, agent.id, "two calls reusing one id").await;
        h.ctrl.next_request().await;
        let mut calls = Vec::new();
        for generation in 0..2 {
            h.ctrl.send_tool_call("reused", "delayed_work", serde_json::json!({})); h.ctrl.finish();
            r12o::invocations(&marker).await;
            std::fs::remove_file(&marker).expect("marker");
            let all = std::fs::read_to_string(&captures).expect("invocations");
            let invocation: serde_json::Value = serde_json::from_str(all.lines().last().expect("invocation")).expect("json");
            calls.push(invocation);
            if generation == 0 {
                h.client_tx.send(ClientMessage::Interrupt { thread_id: root.clone() }).expect("cancel first invocation");
                wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;
                r11_send(&broker, agent.id, "start replacement invocation").await;
                h.ctrl.next_request().await;
            }
        }
        assert_ne!(calls[0]["callback_url"], calls[1]["callback_url"]);
        let callback = |inv: &serde_json::Value, text: &str| serde_json::json!({"type":"tool_result", "group_id":inv["group_id"], "id":inv["id"], "call_id":inv["call_id"], "text":text, "subscription":true}).to_string();
        assert_eq!(reqwest_post(calls[0]["callback_url"].as_str().expect("url"), &callback(&calls[0], "stale result must not settle replacement")).await, 200);
        assert!(tokio::time::timeout(Duration::from_millis(200), h.ctrl.next_request()).await.is_err(), "old callback must not settle newer call");
        assert_eq!(reqwest_post(calls[1]["callback_url"].as_str().expect("url"), &callback(&calls[1], "replacement real result")).await, 200);
        h.client_tx.send(ClientMessage::ShutdownSession { root_thread_id: root }).expect("quit after accepted real result");
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("real continuation");
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("replacement real result"));
        assert!(!history.contains("stale result must not settle replacement"));
        assert!(!history.contains("tool server stopped by session shutdown"), "locally accepted real result wins over later shutdown");
        let event = serde_json::json!({"type":"subscription_event", "group_id":calls[1]["group_id"], "tool_call_id":calls[1]["id"], "text":"subscription after terminal result", "associative":false}).to_string();
        assert_eq!(reqwest_post(calls[1]["callback_url"].as_str().expect("url"), &event).await, 200);
        h.ctrl.send_text("done"); h.ctrl.finish();
        let event_request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("subscription survives registry retirement");
        assert!(format!("{:?}", event_request.chat_history).contains("subscription after terminal result"));
        h.ctrl.send_text("event handled"); h.ctrl.finish();
        broker.shutdown().await.expect("shutdown");
    }).await;
}
