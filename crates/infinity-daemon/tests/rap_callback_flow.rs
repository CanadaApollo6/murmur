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
        let callback = rap_protocol::RapCallback::ToolResult(rap_protocol::RapToolResult {
            group_id:inv.group_id.clone(), id:inv.id.clone(), call_id:inv.call_id.clone(), text:Some("external work finished".into()), content:None, display_as:None, subscription:None,
        });
        let delivery = reqwest::Client::new().post(&inv.callback_url).header("content-type", "application/json").body(serde_json::to_vec(&callback).expect("callback")).timeout(Duration::from_millis(300)).send().await;
        assert!(delivery.is_err(), "old callback destination must be gone");
        eprintln!("R5 external executor survived, but callback {} is gone; replacement is {}", inv.callback_url, h.callback_url);
        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id:root.clone(), message_id:"human-after-restart".into(), from:"human".into(), text:"human after restart".into() }).expect("mail");
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("external tool recovery must not strand human mail indefinitely");
        assert!(matches!(replay, DaemonMessage::Replay { idle:true, .. }), "restart must settle before replay");
        let cancelled = tokio::time::timeout(Duration::from_secs(1), cancel_rx.recv()).await.expect("bounded restart notification").expect("cancellation");
        assert_eq!(cancelled["thread_id"], inv.group_id.as_str());
        assert_eq!(cancelled["tool_call_id"], inv.id.as_str());
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("human after restart") && history.contains("interrupted: daemon restarted"));
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
        assert_eq!(history.matches("interrupted: daemon restarted").count(), 1);
        assert!(!history.contains("external work finished"));
        h.ctrl.send_text("still settled");
        h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;
        let payloads = std::fs::read_dir(h._state_dir.path().join("threads")).expect("threads")
            .map(|entry| std::fs::read_to_string(entry.expect("entry").path()).expect("thread payload")).collect::<String>();
        assert_eq!(payloads.matches("interrupted: daemon restarted").count(), 1);
        assert!(!payloads.contains("external work finished"), "late callback cannot append a second result");
        assert!(inv_rx.try_recv().is_err(), "restart must not re-invoke the executor");
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
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match events.recv().await.expect("event") {
                    Event::PhaseChanged {
                        phase: Phase::Crashed { .. },
                        ..
                    } => {
                        crashes += 1;
                        if !permanent && crashes == 2 {
                            break;
                        }
                    }
                    Event::PhaseChanged {
                        phase: Phase::Failed { .. },
                        ..
                    } if permanent => break,
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
            assert_eq!(crashes, 4, "fifth failed cancellation must reach Failed");
            agent.stop().await.expect("stop failed actor");
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::Error { text, .. } if text.contains("settlement exhausted"))
            }).await;
            tokio::time::sleep(Duration::from_millis(250)).await;
            while let Ok(message) = h.daemon_rx.try_recv() {
                assert!(
                    !matches!(message, DaemonMessage::Info { text, .. } if text.contains("Interrupt failed")),
                    "held settlement must stop automatic write retries"
                );
            }
        }
        drop(fault);
        if permanent {
            h.client_tx
                .send(ClientMessage::Interrupt {
                    thread_id: root.clone(),
                })
                .expect("retry settlement after repair");
            wait_for_message(&mut h.daemon_rx, |m| {
                matches!(m, DaemonMessage::ThreadIdle { .. })
            })
            .await;
        }
        if permanent {
            agent = broker
                .spawn(spec.clone())
                .await
                .expect("resume after storage recovery");
            phases = agent.phases();
        }
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
async fn interrupt_write_failure_exhausts_broker_recovery_without_losing_result() {
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
            kind.request_timeout = Duration::from_millis(100);
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
    let crashed = tokio::time::timeout(Duration::from_millis(350), async {
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
    assert!(format!("{:?}", request.chat_history).contains("interrupted: daemon restarted"));
    h.ctrl.send_text("recovered");
    h.ctrl.finish();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(events.recv().await.expect("event"), Event::Consumed { ids, .. } if ids.contains(&envelope.id())) {}
    }).await.expect("restart receipt");
    let payloads = std::fs::read_dir(h._state_dir.path().join("threads"))
        .expect("threads")
        .map(|entry| std::fs::read_to_string(entry.expect("entry").path()).expect("thread payload"))
        .collect::<String>();
    assert_eq!(payloads.matches("interrupted: daemon restarted").count(), 1);
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

#[test]
fn r6_restart_with_stalled_manifest() {
    let manifest_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (release_manifest, manifest_released) = tokio::sync::watch::channel(false);
    let external = tokio::runtime::Runtime::new().expect("external runtime");
    let (inv_tx, mut inv_rx) = mpsc::unbounded_channel::<rap_protocol::RapInvocation>();
    let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel::<serde_json::Value>();
    let port = external.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("external listener");
        let port = listener.local_addr().expect("addr").port();
        let manifest_calls = manifest_calls.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                let manifest_calls = manifest_calls.clone();
                let manifest_released = manifest_released.clone();
                let tx = inv_tx.clone();
                let cancel_tx = cancel_tx.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let tx = tx.clone();
                        let cancel_tx = cancel_tx.clone();
                        let manifest_calls = manifest_calls.clone();
                        let mut manifest_released = manifest_released.clone();
                        async move {
                            use http_body_util::BodyExt;
                            let response = if req.uri().path() == "/.well-known/rap-toolset" {
                                if manifest_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                                    manifest_released.wait_for(|released| *released).await.expect("release manifest");
                                }
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
        assert!(
            h.manager
                .lock()
                .await
                .send_input((
                    infinity_agent_core::message::InputMessage {
                        content: infinity_agent_core::message::InputMessageContent::UserChoice(
                            infinity_agent_core::message::UserChoiceRequired {
                                content_type: "user_choice_required".into(),
                                id: "external-call".into(),
                                call_id: None,
                                prompt: "obsolete".into(),
                                choices: vec!["yes".into()],
                                default: 0,
                                response_url: "http://unused.test/choice".into(),
                            }
                        ),
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
        wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m, DaemonMessage::UserChoiceRequired { .. })
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
        let replay = tokio::time::timeout(Duration::from_secs(3), wait_for_message(
            &mut h.daemon_rx, |m| matches!(m, DaemonMessage::Replay { .. })
        )).await.expect("restart attachment must finish without fetching a manifest");
        assert!(matches!(replay, DaemonMessage::Replay { idle:true, ref pending_choices, .. } if pending_choices.is_empty()), "restart must settle before replay");
        assert_eq!(manifest_calls.load(std::sync::atomic::Ordering::SeqCst), 1,
            "recovery must not fetch a manifest");
        let payloads = std::fs::read_dir(h._state_dir.path().join("threads")).expect("threads")
            .map(|entry| std::fs::read_to_string(entry.expect("entry").path()).expect("thread payload")).collect::<String>();
        assert_eq!(payloads.matches("interrupted: daemon restarted").count(), 1,
            "settlement must be durable before attachment returns");
        release_manifest.send(true).expect("normal config resolution may now proceed");
        let callback = rap_protocol::RapCallback::ToolResult(rap_protocol::RapToolResult {
            group_id:inv.group_id.clone(), id:inv.id.clone(), call_id:inv.call_id.clone(), text:Some("external work finished".into()), content:None, display_as:None, subscription:None,
        });
        let delivery = reqwest::Client::new().post(&inv.callback_url).header("content-type", "application/json").body(serde_json::to_vec(&callback).expect("callback")).timeout(Duration::from_millis(300)).send().await;
        assert!(delivery.is_err(), "old callback destination must be gone");

        h.client_tx.send(ClientMessage::DeliverPeerMessage { thread_id:root.clone(), message_id:"human-after-restart".into(), from:"human".into(), text:"human after restart".into() }).expect("mail");
        let request = tokio::time::timeout(Duration::from_secs(3), h.ctrl.next_request()).await.expect("external tool recovery must not strand human mail indefinitely");
        assert!(matches!(replay, DaemonMessage::Replay { idle:true, ref pending_choices, .. } if pending_choices.is_empty()), "restart must settle before replay");
        let cancelled = tokio::time::timeout(Duration::from_secs(1), cancel_rx.recv()).await.expect("bounded restart notification").expect("cancellation");
        assert_eq!(cancelled["thread_id"], inv.group_id.as_str());
        assert_eq!(cancelled["tool_call_id"], inv.id.as_str());
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("human after restart") && history.contains("interrupted: daemon restarted"));
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
        assert_eq!(history.matches("interrupted: daemon restarted").count(), 1);
        assert!(!history.contains("external work finished"));
        h.ctrl.send_text("still settled");
        h.ctrl.finish();
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;
        let payloads = std::fs::read_dir(h._state_dir.path().join("threads")).expect("threads")
            .map(|entry| std::fs::read_to_string(entry.expect("entry").path()).expect("thread payload")).collect::<String>();
        assert_eq!(payloads.matches("interrupted: daemon restarted").count(), 1);
        assert!(!payloads.contains("external work finished"), "late callback cannot append a second result");
        assert!(inv_rx.try_recv().is_err(), "restart must not re-invoke the executor");
    }));
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
            assert!(!stored.contains("interrupted:"));
            assert!(format!("{:?}", next.chat_history).contains("Replaced text in demo.rs"));
            h.ctrl.send_text("done");
            h.ctrl.finish();
            broker.shutdown().await.expect("shutdown");
            accept.abort();
        })
        .await;
}
