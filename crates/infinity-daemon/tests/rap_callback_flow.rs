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
    let (model, ctrl) = mock_model();
    let entry = ModelEntry {
        model_id: "mock-model".to_owned(),
        display_name: "Mock Model".to_owned(),
        context_window: 100_000,
        max_output_tokens: None,
        supports_image_input: false,
    };
    let state_dir = tempfile::tempdir()?;
    let cwd = tempfile::tempdir()?;

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
        vec![agentd::KindConfig::infinity(socket)],
        Default::default(),
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
    if matches!(probe, ToolSettlementProbe::CancelRealResult) {
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
        ToolSettlementProbe::CancelRealResult => {}
    }
    tokio::time::timeout(Duration::from_secs(5), h.ctrl.next_request())
        .await
        .expect("tool followup");
    if matches!(probe, ToolSettlementProbe::CancelRealResult) {
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
            root_thread_id: root,
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
        ToolSettlementProbe::CancelRealResult => assert!(
            format!("{:?}", results[0]).contains("Slept for 0.05 seconds"),
            "preserve real result rather than substitute interruption"
        ),
        ToolSettlementProbe::PeerWake => {
            assert!(format!("{:?}", results[0]).contains("interrupted"))
        }
        _ => assert!(results[0].is_empty()),
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
