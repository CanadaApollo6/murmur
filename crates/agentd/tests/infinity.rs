use std::sync::Arc;
use std::time::Duration;

use agentd::KindConfig;
use agentd::mailroom::{Mailroom, NewMessage};
use agentd::model::*;
use agentd::supervisor::{AgentHandle, AgentSpec, Broker, Config, Error, Event, Phase};
use agentd::swarm::Act;
use infinity_protocol::{ClientMessage, DaemonMessage};
use tempfile::TempDir;
use tokio::sync::broadcast;

#[path = "support/fake_infinity.rs"]
mod fake;

struct Test {
    root: TempDir,
    daemon: fake::FakeDaemon,
    mailroom: Arc<Mailroom>,
    broker: Broker,
    agent: AgentHandle,
    events: broadcast::Receiver<Event>,
}
fn spec(root: &std::path::Path) -> AgentSpec {
    AgentSpec {
        name: AgentName::parse("model").expect("name"),
        kind: "infinity".into(),
        cwd: root.into(),
        model: None,
        permissions: Default::default(),
        idle_after: Some(Duration::from_millis(5)),
    }
}
impl Test {
    async fn new() -> Self {
        Self::with_policy(Default::default()).await
    }
    async fn with_policy(policy: PermissionPolicy) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        let socket = root.path().join("daemon.sock");
        let daemon = fake::FakeDaemon::bind(&socket);
        let mailroom = Arc::new(
            Mailroom::open(root.path().join("mail.db"), Default::default())
                .await
                .expect("mailroom"),
        );
        let mut kind = KindConfig::infinity(socket);
        kind.model = Some("fake/small".into());
        kind.request_timeout = Duration::from_millis(200);
        kind.cancel_timeout = Duration::from_millis(200);
        let broker = Broker::new(
            mailroom.clone(),
            vec![kind],
            Config {
                backoff_initial: Duration::from_millis(10),
                backoff_max: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .expect("broker");
        let events = broker.events();
        let mut requested = spec(root.path());
        requested.permissions = policy;
        let agent = broker.spawn(requested).await.expect("spawn");
        let mut t = Self {
            root,
            daemon,
            mailroom,
            broker,
            agent,
            events,
        };
        let create = t.daemon.next().await;
        assert!(
            matches!(create.message, ClientMessage::CreateSession { model: Some(m), keeps_session_alive: true, .. }
            if m.provider_id == "fake" && m.model_id == "small")
        );
        t.event(|e| {
            matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::Idle,
                    ..
                }
            )
        })
        .await;
        t
    }
    async fn send(&self, peer: bool, body: &str) -> MessageId {
        let from = if peer {
            Sender::Agent(
                self.mailroom
                    .register(AgentName::parse("sender").expect("name"))
                    .await
                    .expect("sender"),
            )
        } else {
            Sender::Human
        };
        self.broker
            .send(NewMessage {
                from,
                to: self.agent.id,
                body: Body::new(body).expect("body"),
                reply_to: None,
                client_key: ClientKey(ulid::Ulid::new().to_string()),
                act: Act::Inform,
                task: None,
            })
            .await
            .expect("send")
            .1
            .id()
    }
    async fn event(&mut self, predicate: impl Fn(&Event) -> bool) -> Event {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = self.events.recv().await.expect("event");
                if predicate(&event) {
                    return event;
                }
            }
        })
        .await
        .expect("supervisor event timeout")
    }
    async fn consumed(&mut self, id: MessageId) {
        self.event(|e| matches!(e, Event::Consumed { ids, .. } if ids.contains(&id)))
            .await;
        assert_eq!(
            self.mailroom.delivery(id).await.expect("delivery").state,
            DeliveryState::Consumed
        );
    }
    async fn idle(&mut self) {
        self.event(|e| {
            matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::Idle,
                    ..
                }
            )
        })
        .await;
    }
    async fn finish(self) {
        self.broker.shutdown().await.expect("shutdown");
        drop(self.broker);
        Arc::try_unwrap(self.mailroom)
            .ok()
            .expect("sole mailroom owner")
            .close()
            .await
            .expect("close");
    }
}

#[tokio::test]
async fn peer_delivery_only_commits_on_the_matching_root_receipt() {
    let mut t = Test::new().await;
    let id = t.send(true, "peer body").await;
    let request = t.daemon.next().await;
    assert!(
        matches!(&request.message, ClientMessage::DeliverPeerMessage { thread_id, message_id, from, text }
        if *thread_id == fake::root() && message_id == &id.to_string()
        && from.starts_with("agent:") && text.contains("peer body") && text.contains("@sender"))
    );
    request.emit(fake::start());
    t.event(|e| matches!(e, Event::Started { ids, .. } if ids.contains(&id)))
        .await;
    request.emit(DaemonMessage::InputCommitted {
        thread_id: Some("child".into()),
        dedup_id: format!("peer-{id}"),
    });
    request.emit(fake::receipt("peer-unrelated".into()));
    request.emit(fake::round_done());
    request.emit(fake::idle());
    // An activity event is a barrier proving the preceding false evidence was processed.
    request.emit(DaemonMessage::Info {
        thread_id: fake::thread(),
        text: "barrier".into(),
    });
    t.event(|e| matches!(e, Event::Activity { text, .. } if text == "barrier"))
        .await;
    assert!(matches!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Offered { .. }
    ));
    request.emit(fake::receipt(format!("peer-{id}")));
    request.emit(fake::idle());
    t.consumed(id).await;
    t.idle().await;
    t.finish().await;
}

#[tokio::test]
async fn human_receipt_requires_echo_and_a_non_peer_commit() {
    let mut t = Test::new().await;
    let id = t.send(false, "human body").await;
    let request = t.daemon.next().await;
    assert!(
        matches!(&request.message, ClientMessage::UserInput { thread_id, text }
        if *thread_id == fake::root() && text == "human body")
    );
    request.emit(fake::receipt("before-echo".into()));
    request.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "different human".into(),
    });
    request.emit(fake::receipt("wrong-echo".into()));
    request.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "human body".into(),
    });
    request.emit(fake::receipt("peer-other".into()));
    request.emit(DaemonMessage::Info {
        thread_id: fake::thread(),
        text: "barrier".into(),
    });
    t.event(|e| matches!(e, Event::Activity { .. })).await;
    assert!(matches!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Offered { .. }
    ));
    request.emit(fake::receipt("generated-user-id".into()));
    t.consumed(id).await;
    assert_eq!(
        t.agent.phase(),
        Phase::Busy,
        "receipt is independent of response completion"
    );
    request.emit(fake::round_done());
    request.emit(fake::idle());
    t.idle().await;
    t.finish().await;
}

#[tokio::test]
async fn reoffer_after_daemon_death_is_deduplicated_and_receipted() {
    let mut t = Test::new().await;
    let id = t.send(true, "durable but receipt lost").await;
    let request = t.daemon.next().await;
    request.emit(fake::start());
    let Event::PhaseChanged {
        incarnation: old, ..
    } = t
        .event(|e| {
            matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::Busy,
                    ..
                }
            )
        })
        .await
    else {
        panic!("busy")
    };
    t.event(|e| matches!(e, Event::Started { .. })).await;
    t.daemon
        .state
        .lock()
        .expect("state")
        .durable
        .insert(id.to_string());
    request.die();
    let connect = t.daemon.next().await;
    assert!(
        matches!(connect.message, ClientMessage::Connect { root_thread_id, .. } if root_thread_id == fake::root())
    );
    let reoffer = t.daemon.next().await;
    assert!(
        matches!(reoffer.message, ClientMessage::DeliverPeerMessage { message_id, .. } if message_id == id.to_string())
    );
    let Event::PhaseChanged {
        incarnation: new, ..
    } = t
        .event(|e| {
            matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::Starting,
                    ..
                }
            )
        })
        .await
    else {
        panic!("starting")
    };
    assert!(new > old);
    t.consumed(id).await;
    assert_eq!(t.daemon.state.lock().expect("state").durable.len(), 1);
    assert!(
        t.mailroom
            .held(Some(t.agent.id))
            .await
            .expect("held")
            .is_empty()
    );
    t.finish().await;
}

#[tokio::test]
async fn no_receipt_never_consumes_and_exhausted_retries_are_held() {
    let mut t = Test::new().await;
    let id = t.send(true, "never durable").await;
    for attempt in 1..=5 {
        let request = t.daemon.next().await;
        assert!(matches!(
            request.message,
            ClientMessage::DeliverPeerMessage { .. }
        ));
        request.emit(fake::start());
        request.emit(fake::round_done());
        request.emit(fake::idle());
        if attempt < 5 {
            t.event(|e| matches!(e, Event::PhaseChanged { phase: Phase::Crashed { attempt: n, .. }, .. } if *n == attempt)).await;
            assert!(matches!(
                t.daemon.next().await.message,
                ClientMessage::Connect { .. }
            ));
        }
    }
    t.event(|e| {
        matches!(
            e,
            Event::PhaseChanged {
                phase: Phase::Failed { .. },
                ..
            }
        )
    })
    .await;
    assert!(matches!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Held { .. }
    ));
    assert_eq!(t.mailroom.delivery(id).await.expect("attempts").attempts, 5);
    assert!(
        t.mailroom
            .held(Some(t.agent.id))
            .await
            .expect("held mail")
            .iter()
            .any(|mail| mail.envelope.id() == id)
    );
    assert_ne!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Consumed
    );
    while let Ok(event) = t.events.try_recv() {
        assert!(!matches!(event, Event::Consumed { .. }));
    }
    t.finish().await;
}

#[tokio::test]
async fn interrupt_is_a_daemon_control_message() {
    let mut t = Test::new().await;
    let id = t.send(false, "hold").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "hold".into(),
    });
    input.emit(fake::start());
    t.event(|e| matches!(e, Event::Started { .. })).await;
    t.agent.cancel().await.expect("cancel");
    let cancel = t.daemon.next().await;
    assert!(
        matches!(cancel.message, ClientMessage::Interrupt { thread_id } if thread_id == fake::root())
    );
    input.emit(fake::receipt("human-id".into()));
    input.emit(fake::round_done());
    input.emit(fake::idle());
    t.consumed(id).await;
    t.event(|e| {
        matches!(
            e,
            Event::TurnEnded {
                outcome: agentd::supervisor::TurnOutcome::Cancelled,
                ..
            }
        )
    })
    .await;
    t.finish().await;
}

#[tokio::test]
async fn permission_round_trip_preserves_choices_and_fences_incarnations() {
    let mut t = Test::new().await;
    let id = t.send(true, "ask").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "approval".into(),
        prompt: "Allow this edit?".into(),
        choices: vec!["Allow".into(), "Deny".into()],
        default: 1,
    });
    let Event::PermissionPending(request) =
        t.event(|e| matches!(e, Event::PermissionPending(_))).await
    else {
        panic!("permission")
    };
    assert_eq!(request.agent, t.agent.id);
    assert_eq!(request.tool, "infinity.user_choice");
    assert_eq!(request.options[0].label, "Allow");
    assert!(
        request
            .options
            .iter()
            .all(|o| o.effect == PermissionEffect::UserChoice)
    );
    assert_eq!(t.agent.phase(), Phase::Busy);
    assert!(matches!(
        t.agent
            .answer_permission(
                request.id,
                Incarnation(request.incarnation.0 + 1),
                PermissionAnswer::Choose("0".into())
            )
            .await,
        Err(Error::StalePermission)
    ));
    assert!(
        t.agent
            .answer_permission(
                request.id,
                request.incarnation,
                PermissionAnswer::Choose("2".into())
            )
            .await
            .is_err()
    );
    t.agent
        .answer_permission(
            request.id,
            request.incarnation,
            PermissionAnswer::Choose("1".into()),
        )
        .await
        .expect("answer");
    let answer = t.daemon.next().await;
    assert!(
        matches!(answer.message, ClientMessage::UserChoiceAnswered { choice_id, selected: 1 } if choice_id.as_str() == "approval")
    );
    input.emit(fake::receipt(format!("peer-{id}")));
    input.emit(fake::round_done());
    input.emit(fake::idle());
    t.consumed(id).await;
    t.finish().await;
}

#[tokio::test]
async fn resume_reuses_root_and_ignores_history_receipts() {
    let mut t = Test::new().await;
    t.agent.stop().await.expect("stop");
    t.agent = t.broker.spawn(spec(t.root.path())).await.expect("respawn");
    let connect = t.daemon.next().await;
    assert!(
        matches!(connect.message, ClientMessage::Connect { root_thread_id, keeps_session_alive: true, .. } if root_thread_id == fake::root())
    );
    t.idle().await;
    let id = t.send(true, "new mail").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::Replay {
        history: vec![
            fake::start(),
            fake::receipt(format!("peer-{id}")),
            fake::round_done(),
        ],
        pending_choices: vec![],
        views: Default::default(),
        in_progress: false,
        idle: true,
    });
    input.emit(DaemonMessage::Info {
        thread_id: fake::thread(),
        text: "barrier".into(),
    });
    t.event(|e| matches!(e, Event::Activity { .. })).await;
    assert!(matches!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Offered { .. }
    ));
    input.emit(fake::receipt(format!("peer-{id}")));
    input.emit(fake::round_done());
    input.emit(fake::idle());
    t.consumed(id).await;
    t.finish().await;
}

#[tokio::test]
async fn refused_or_missing_resume_needs_recovery_and_never_creates_a_thread() {
    for missing in [false, true] {
        let mut t = Test::new().await;
        t.agent.stop().await.expect("stop");
        {
            let mut state = t.daemon.state.lock().expect("state");
            state.fail_resume = true;
            state.missing = missing;
        }
        t.agent = t.broker.spawn(spec(t.root.path())).await.expect("respawn");
        if !missing {
            assert!(matches!(
                t.daemon.next().await.message,
                ClientMessage::Connect { .. }
            ));
        }
        t.event(|e| {
            matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::NeedsRecovery { .. },
                    ..
                }
            )
        })
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
                .await
                .is_err()
        );
        t.finish().await;
    }
}

#[tokio::test]
async fn model_agents_never_idle_suspend_and_idle_eof_reopens() {
    let mut t = Test::new().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(t.agent.phase(), Phase::Idle);
    let id = t.send(true, "dedup").await;
    let input = t.daemon.next().await;
    input.emit(fake::receipt(format!("peer-{id}")));
    input.emit(fake::idle());
    t.consumed(id).await;
    t.idle().await;
    input.die();
    assert!(matches!(
        t.daemon.next().await.message,
        ClientMessage::Connect { .. }
    ));
    t.idle().await;
    t.finish().await;
}

#[tokio::test]
async fn peers_reach_a_busy_model_while_human_mail_waits() {
    let mut t = Test::new().await;
    let first = t.send(false, "working").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "working".into(),
    });
    input.emit(fake::start());
    t.event(|e| matches!(e, Event::Started { .. })).await;
    let human = t.send(false, "later human").await;
    let peer = t.send(true, "wake from passive sleep").await;
    let peer_request = t.daemon.next().await;
    assert!(
        matches!(&peer_request.message, ClientMessage::DeliverPeerMessage { message_id, .. } if message_id == &peer.to_string())
    );
    peer_request.emit(fake::receipt(format!("peer-{peer}")));
    t.consumed(peer).await;
    assert_eq!(
        t.mailroom
            .delivery(human)
            .await
            .expect("human delivery")
            .state,
        DeliveryState::Queued
    );
    input.emit(fake::receipt("working-id".into()));
    input.emit(fake::round_done());
    input.emit(fake::idle());
    t.consumed(first).await;
    let human_request = t.daemon.next().await;
    assert!(
        matches!(&human_request.message, ClientMessage::UserInput { text, .. } if text == "later human")
    );
    human_request.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "later human".into(),
    });
    human_request.emit(fake::receipt("later-id".into()));
    human_request.emit(fake::round_done());
    human_request.emit(fake::idle());
    t.consumed(human).await;
    t.finish().await;
}

#[tokio::test]
#[ignore = "build infinity-agent-cli first; uses a local Python fake provider without credentials"]
async fn live_infinity_daemon_with_fake_provider() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    let root = tempfile::tempdir().expect("tempdir");
    let state = root.path().join(".infinity");
    std::fs::create_dir(&state).expect("state dir");
    let provider =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake_provider.py");
    std::fs::write(
        state.join("providers.json"),
        serde_json::to_vec(&serde_json::json!({
            "fake": { "command": ["python3", provider, root.path().join("provider.sock")] }
        }))
        .expect("provider JSON"),
    )
    .expect("write providers");
    let binary = std::env::var_os("AGENTD_INFINITY_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/infinity")
        });
    let mut command = Command::new(binary);
    command
        .arg("daemon")
        .env("HOME", root.path())
        .env("INFINITY_WS_PORT", "0")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    // A failing test must also kill the provider process spawned by the daemon.
    command.process_group(0);
    let mut daemon = command.spawn().expect("spawn infinity daemon");
    struct ProcessGroup(i32);
    impl Drop for ProcessGroup {
        fn drop(&mut self) {
            if let Err(error) = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(self.0),
                nix::sys::signal::Signal::SIGKILL,
            ) && error != nix::errno::Errno::ESRCH
            {
                eprintln!("live process group cleanup failed: {error}");
            }
        }
    }
    let group = ProcessGroup(i32::try_from(daemon.id().expect("daemon pid")).expect("pid range"));
    let mut output = BufReader::new(daemon.stdout.take().expect("daemon stdout")).lines();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let line = output
                .next_line()
                .await
                .expect("daemon stdout read")
                .expect("daemon ready line");
            if line == "infinity-daemon ready" {
                break;
            }
        }
    })
    .await
    .expect("daemon startup timeout");
    let mailroom = Arc::new(
        Mailroom::open(root.path().join("mail.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let mut kind = KindConfig::infinity(state.join("daemon.sock"));
    kind.model = Some("fake/echo".into());
    let broker = Broker::new(mailroom.clone(), vec![kind], Default::default()).expect("broker");
    let mut events = broker.events();
    let mut agent = broker.spawn(spec(root.path())).await.expect("spawn");
    for peer in [true, false, true] {
        let sender = if peer {
            Sender::Agent(
                mailroom
                    .register(AgentName::parse("sender").expect("name"))
                    .await
                    .expect("sender"),
            )
        } else {
            Sender::Human
        };
        let id = broker
            .send(NewMessage {
                from: sender,
                to: agent.id,
                body: Body::new("live receipt test").expect("body"),
                reply_to: None,
                client_key: ClientKey(ulid::Ulid::new().to_string()),
                act: Act::Inform,
                task: None,
            })
            .await
            .expect("send")
            .1
            .id();
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut consumed = false;
            loop {
                match events.recv().await.expect("live event") {
                    Event::Consumed { ids, .. } if ids.contains(&id) => consumed = true,
                    Event::PhaseChanged {
                        phase: Phase::Idle, ..
                    } if consumed => break,
                    Event::PhaseChanged {
                        phase: Phase::NeedsRecovery { reason } | Phase::Failed { last: reason },
                        ..
                    } => panic!("live failure {reason}"),
                    _ => {}
                }
            }
        })
        .await
        .expect("live receipt timeout");
        assert_eq!(
            mailroom.delivery(id).await.expect("delivery").state,
            DeliveryState::Consumed
        );
        agent.stop().await.expect("stop for resume");
        agent = broker
            .spawn(spec(root.path()))
            .await
            .expect("resume live root");
    }
    let requests =
        std::fs::read_to_string(root.path().join("requests.jsonl")).expect("provider requests");
    assert!(
        requests.contains("receive_event__injected"),
        "peer must reach provider as an injected exchange"
    );
    assert!(requests.contains("live receipt test"));
    broker.shutdown().await.expect("shutdown broker");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("sole mailroom owner")
        .close()
        .await
        .expect("close");
    daemon.kill().await.expect("kill daemon");
    daemon.wait().await.expect("reap daemon");
    drop(group);
}

#[tokio::test]
async fn permission_timeout_interrupts_without_guessing_an_arbitrary_choice() {
    let mut t = Test::with_policy(PermissionPolicy {
        rules: vec![PermissionRule {
            tool: "*".into(),
            decision: RuleDecision::Allow,
        }],
        timeout: Some(Duration::from_millis(20)),
        ..Default::default()
    })
    .await;
    let id = t.send(true, "choice").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "timeout-choice".into(),
        prompt: "Choose a destination".into(),
        choices: vec!["Production".into(), "Local".into()],
        default: 0,
    });
    t.event(|e| matches!(e, Event::PermissionPending(_))).await;
    let cancel = t.daemon.next().await;
    assert!(
        matches!(cancel.message, ClientMessage::Interrupt { .. }),
        "must not guess what Allow means"
    );
    input.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "late-choice".into(),
        prompt: "Late choice".into(),
        choices: vec!["Yes".into()],
        default: 0,
    });
    input.emit(fake::receipt(format!("peer-{id}")));
    input.emit(DaemonMessage::UserChoiceComplete {
        choice_id: "timeout-choice".into(),
    });
    input.emit(fake::round_done());
    input.emit(fake::idle());
    t.consumed(id).await;
    t.idle().await;
    assert!(
        t.agent
            .pending_permissions()
            .await
            .expect("permissions")
            .is_empty()
    );
    t.finish().await;
}

#[tokio::test]
async fn permission_from_a_dead_incarnation_cannot_answer_its_replacement() {
    let mut t = Test::new().await;
    let id = t.send(true, "permission then daemon death").await;
    let input = t.daemon.next().await;
    let prompt = || DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "persisted-choice".into(),
        prompt: "Continue?".into(),
        choices: vec!["Yes".into(), "No".into()],
        default: 1,
    };
    input.emit(prompt());
    let Event::PermissionPending(old) = t.event(|e| matches!(e, Event::PermissionPending(_))).await
    else {
        panic!("old permission")
    };
    input.die();
    assert!(matches!(
        t.daemon.next().await.message,
        ClientMessage::Connect { .. }
    ));
    let replacement = t.daemon.next().await;
    replacement.emit(DaemonMessage::Replay {
        history: vec![],
        pending_choices: vec![prompt()],
        views: Default::default(),
        in_progress: false,
        idle: false,
    });
    let Event::PermissionPending(current) =
        t.event(|e| matches!(e, Event::PermissionPending(_))).await
    else {
        panic!("replacement permission")
    };
    assert!(current.incarnation > old.incarnation);
    assert!(matches!(
        t.agent
            .answer_permission(
                old.id,
                old.incarnation,
                PermissionAnswer::Choose("0".into())
            )
            .await,
        Err(Error::StalePermission)
    ));
    t.agent
        .answer_permission(
            current.id,
            current.incarnation,
            PermissionAnswer::Choose("1".into()),
        )
        .await
        .expect("answer replacement");
    assert!(matches!(
        t.daemon.next().await.message,
        ClientMessage::UserChoiceAnswered { selected: 1, .. }
    ));
    replacement.emit(fake::receipt(format!("peer-{id}")));
    replacement.emit(fake::round_done());
    replacement.emit(fake::idle());
    t.consumed(id).await;
    t.finish().await;
}

#[tokio::test]
async fn pending_choice_holds_peer_mail_until_answered() {
    let mut t = Test::new().await;
    let first = t.send(true, "permission").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "blocking-choice".into(),
        prompt: "Choose".into(),
        choices: vec!["Continue".into()],
        default: 0,
    });
    let Event::PermissionPending(permission) =
        t.event(|e| matches!(e, Event::PermissionPending(_))).await
    else {
        panic!("permission")
    };
    let next = t.send(true, "wait behind permission").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
            .await
            .is_err()
    );
    assert_eq!(
        t.mailroom.delivery(next).await.expect("queued mail").state,
        DeliveryState::Queued
    );
    t.agent
        .answer_permission(
            permission.id,
            permission.incarnation,
            PermissionAnswer::Choose("0".into()),
        )
        .await
        .expect("answer");
    assert!(matches!(
        t.daemon.next().await.message,
        ClientMessage::UserChoiceAnswered { .. }
    ));
    let peer = t.daemon.next().await;
    assert!(
        matches!(&peer.message, ClientMessage::DeliverPeerMessage { message_id, .. } if message_id == &next.to_string())
    );
    peer.emit(fake::receipt(format!("peer-{next}")));
    t.consumed(next).await;
    input.emit(fake::receipt(format!("peer-{first}")));
    input.emit(fake::round_done());
    input.emit(fake::idle());
    t.consumed(first).await;
    t.finish().await;
}

#[tokio::test]
async fn busy_peer_receipt_drains_its_completion_before_new_human_mail() {
    let mut t = Test::new().await;
    let first = t.send(false, "first").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "first".into(),
    });
    input.emit(fake::start());
    t.event(|e| matches!(e, Event::Started { .. })).await;
    let peer = t.send(true, "busy peer").await;
    let peer_input = t.daemon.next().await;
    input.emit(fake::receipt("first-id".into()));
    input.emit(fake::round_done());
    t.consumed(first).await;
    peer_input.emit(fake::start());
    peer_input.emit(fake::receipt(format!("peer-{peer}")));
    t.consumed(peer).await;
    let human = t.send(false, "next human").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), t.daemon.next())
            .await
            .is_err(),
        "peer receipt must not release human before peer completion"
    );
    peer_input.emit(fake::round_done());
    peer_input.emit(fake::idle());
    let next = t.daemon.next().await;
    assert!(matches!(&next.message, ClientMessage::UserInput { text, .. } if text == "next human"));
    next.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "next human".into(),
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        t.agent.phase(),
        Phase::Busy,
        "old completion must not time out new input"
    );
    next.emit(fake::receipt("next-id".into()));
    next.emit(fake::round_done());
    next.emit(fake::idle());
    t.consumed(human).await;
    t.idle().await;
    t.finish().await;
}

fn tool_call() -> DaemonMessage {
    DaemonMessage::ToolCall {
        thread_id: fake::thread(),
        name: "async_tool".into(),
        args: "{}".into(),
        display_as: None,
    }
}
fn tool_result() -> DaemonMessage {
    DaemonMessage::ToolResult {
        thread_id: fake::thread(),
        segments: vec![],
    }
}

#[tokio::test]
async fn tool_round_completion_keeps_human_mail_queued_until_followup_finishes() {
    let mut t = Test::new().await;
    let first = t.send(true, "run tool").await;
    let input = t.daemon.next().await;
    input.emit(fake::start());
    input.emit(tool_call());
    input.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    let human = t.send(false, "wait for tool").await;
    input.emit(fake::round_done());
    assert!(
        tokio::time::timeout(Duration::from_millis(40), t.daemon.next())
            .await
            .is_err(),
        "round end must not interrupt waiting tool"
    );
    input.emit(tool_result());
    input.emit(fake::start());
    assert!(
        tokio::time::timeout(Duration::from_millis(40), t.daemon.next())
            .await
            .is_err(),
        "tool result must not release human before followup"
    );
    input.emit(fake::round_done());
    input.emit(fake::idle());
    let next = t.daemon.next().await;
    assert!(matches!(next.message, ClientMessage::UserInput { .. }));
    next.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "wait for tool".into(),
    });
    next.emit(fake::receipt("human-id".into()));
    next.emit(fake::round_done());
    next.emit(fake::idle());
    t.consumed(human).await;
    t.finish().await;
}

#[tokio::test]
async fn resume_waits_for_live_replay_before_pumping_human_mail() {
    for pending_tool in [false, true] {
        let mut t = Test::new().await;
        t.agent.stop().await.expect("stop");
        t.daemon.state.lock().expect("state").manual_replay = true;
        t.agent = t.broker.spawn(spec(t.root.path())).await.expect("resume");
        let connect = t.daemon.next().await;
        let human = t.send(false, "after resumed work").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(40), t.daemon.next())
                .await
                .is_err(),
            "attachment must wait for replay"
        );
        connect.emit(DaemonMessage::Replay {
            history: if pending_tool {
                vec![tool_call()]
            } else {
                vec![fake::start()]
            },
            pending_choices: vec![],
            views: Default::default(),
            in_progress: !pending_tool,
            idle: false,
        });
        connect.emit(DaemonMessage::Info {
            thread_id: fake::thread(),
            text: "replay barrier".into(),
        });
        t.event(|e| matches!(e, Event::Activity { text, .. } if text == "replay barrier"))
            .await;
        assert_eq!(t.agent.phase(), Phase::Busy);
        assert_eq!(
            t.mailroom.delivery(human).await.expect("queued").state,
            DeliveryState::Queued
        );
        if pending_tool {
            connect.emit(tool_result());
            connect.emit(fake::start());
        }
        connect.emit(fake::round_done());
        connect.emit(fake::idle());
        let next = t.daemon.next().await;
        assert!(matches!(next.message, ClientMessage::UserInput { .. }));
        next.emit(DaemonMessage::UserInputEcho {
            thread_id: fake::thread(),
            text: "after resumed work".into(),
        });
        next.emit(fake::receipt("human-id".into()));
        next.emit(fake::round_done());
        next.emit(fake::idle());
        t.consumed(human).await;
        t.finish().await;
    }
}

#[tokio::test]
async fn permission_timeout_keeps_approval_until_durable_interrupt_completion() {
    let mut t = Test::with_policy(PermissionPolicy {
        timeout: Some(Duration::from_millis(20)),
        ..Default::default()
    })
    .await;
    let id = t.send(true, "choice").await;
    let input = t.daemon.next().await;
    input.emit(fake::start());
    input.emit(tool_call());
    input.emit(fake::receipt(format!("peer-{id}")));
    input.emit(fake::round_done());
    input.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "waiting".into(),
        prompt: "Pick".into(),
        choices: vec!["A".into()],
        default: 0,
    });
    t.event(|e| matches!(e, Event::PermissionPending(_))).await;
    assert!(matches!(
        t.daemon.next().await.message,
        ClientMessage::Interrupt { .. }
    ));
    tokio::time::sleep(Duration::from_millis(30)).await;
    while let Ok(event) = t.events.try_recv() {
        assert!(
            !matches!(event, Event::PermissionAnswered { .. }),
            "approval withdrawn before core acknowledged cancellation"
        );
    }
    input.emit(tool_result());
    input.emit(DaemonMessage::UserChoiceComplete {
        choice_id: "waiting".into(),
    });
    input.emit(fake::round_done());
    input.emit(fake::idle());
    t.idle().await;
    assert!(
        t.agent
            .pending_permissions()
            .await
            .expect("permissions")
            .is_empty()
    );
    input.die();
    assert!(matches!(
        t.daemon.next().await.message,
        ClientMessage::Connect { .. }
    ));
    t.idle().await;
    assert!(
        t.agent
            .pending_permissions()
            .await
            .expect("permissions")
            .is_empty()
    );
    t.finish().await;
}

#[tokio::test]
async fn core_idle_settles_tools_without_display_results() {
    let mut t = Test::new().await;
    let first = t.send(true, "run tool").await;
    let input = t.daemon.next().await;
    input.emit(fake::start());
    input.emit(tool_call());
    input.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    let human = t.send(false, "next human").await;
    input.emit(fake::round_done());
    input.emit(fake::start());
    input.emit(fake::round_done());
    input.emit(DaemonMessage::ThreadIdle {
        thread_id: Some("child".into()),
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(40), t.daemon.next())
            .await
            .is_err(),
        "round completion and child idle cannot release root human mail"
    );
    input.emit(fake::idle());
    let next = t.daemon.next().await;
    assert!(matches!(&next.message, ClientMessage::UserInput { text, .. } if text == "next human"));
    next.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "next human".into(),
    });
    next.emit(fake::receipt("human-id".into()));
    next.emit(fake::round_done());
    next.emit(fake::idle());
    t.consumed(human).await;
    t.idle().await;
    t.finish().await;
}

#[tokio::test]
async fn resume_uses_core_idle_instead_of_display_history() {
    let mut t = Test::new().await;
    t.agent.stop().await.expect("stop");
    t.daemon.state.lock().expect("state").manual_replay = true;
    t.agent = t.broker.spawn(spec(t.root.path())).await.expect("resume");
    let connect = t.daemon.next().await;
    connect.emit(DaemonMessage::Replay {
        history: vec![tool_call()],
        pending_choices: vec![],
        views: Default::default(),
        in_progress: false,
        idle: true,
    });
    t.idle().await;
    let id = t.send(false, "human after replay").await;
    let input = t.daemon.next().await;
    assert!(
        matches!(&input.message, ClientMessage::UserInput { text, .. } if text == "human after replay")
    );
    input.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "human after replay".into(),
    });
    input.emit(fake::receipt("human-id".into()));
    input.emit(fake::idle());
    t.consumed(id).await;
    t.idle().await;
    t.finish().await;
}
