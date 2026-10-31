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
        Self::with_stall(policy, Duration::from_secs(600)).await
    }
    async fn with_stall(policy: PermissionPolicy, stall: Duration) -> Self {
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
        kind.stall_timeout = stall;
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
async fn human_mail_has_a_dedup_id_and_only_the_matching_receipt_consumes_it() {
    let mut t = Test::new().await;
    let id = t.send(false, "human body").await;
    let input = t.daemon.next().await;
    assert!(matches!(&input.message,
        ClientMessage::DeliverPeerMessage { thread_id, message_id, from, text }
        if *thread_id == fake::root() && message_id == &id.to_string() && from == "human"
            && text.contains("human body") && text.contains("human operator")));
    input.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "human body".into(),
    });
    input.emit(fake::receipt("unrelated-user-id".into()));
    input.emit(fake::receipt("peer-other".into()));
    input.emit(DaemonMessage::InputCommitted {
        thread_id: Some("child".into()),
        dedup_id: format!("peer-{id}"),
    });
    input.emit(fake::round_done());
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
    t.consumed(id).await;
    assert_eq!(t.agent.phase(), Phase::Busy);
    input.emit(fake::idle());
    t.idle().await;
    t.finish().await;
}

#[tokio::test]
async fn reoffer_after_daemon_death_is_deduplicated_and_receipted() {
    for peer in [false, true] {
        let mut t = Test::new().await;
        let id = t.send(peer, "durable but receipt lost").await;
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
}

#[tokio::test]
async fn no_receipt_never_consumes_and_exhausted_retries_are_held() {
    let mut t = Test::new().await;
    let id = t.send(true, "never durable").await;
    for _ in 1..=5 {
        let request = t.daemon.next().await;
        assert!(matches!(
            request.message,
            ClientMessage::DeliverPeerMessage { .. }
        ));
        request.emit(fake::start());
        request.emit(fake::round_done());
        request.emit(fake::idle());
        t.event(|e| {
            matches!(
                e,
                Event::PhaseChanged {
                    phase: Phase::Crashed { .. },
                    ..
                }
            )
        })
        .await;
        assert!(matches!(
            t.daemon.next().await.message,
            ClientMessage::Connect { .. }
        ));
    }
    t.idle().await;
    assert!(!matches!(*t.agent.phases().borrow(), Phase::Failed { .. }));
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
    input.emit(fake::receipt(format!("peer-{id}")));
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
async fn only_missing_resume_needs_recovery_transient_refusal_retries() {
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
                    phase: Phase::NeedsRecovery { .. }, ..
                } if missing
            ) || matches!(e, Event::PhaseChanged { phase: Phase::Crashed { .. }, .. } if !missing)
        })
        .await;
        if missing {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
                    .await
                    .is_err()
            );
        } else {
            t.daemon.state.lock().expect("state").fail_resume = false;
            assert!(matches!(
                t.daemon.next().await.message,
                ClientMessage::Connect { .. }
            ));
        }
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
async fn human_and_peer_mail_reach_a_busy_model_without_idle() {
    let mut t = Test::new().await;
    let first = t.send(false, "working").await;
    let input = t.daemon.next().await;
    input.emit(fake::start());
    t.event(|e| matches!(e, Event::Started { .. })).await;
    for peer in [false, true] {
        let id = t.send(peer, "busy mail").await;
        let mail = t.daemon.next().await;
        assert!(
            matches!(&mail.message, ClientMessage::DeliverPeerMessage { message_id, .. } if message_id == &id.to_string())
        );
        mail.emit(fake::receipt(format!("peer-{id}")));
        t.consumed(id).await;
        assert_eq!(t.agent.phase(), Phase::Busy);
    }
    input.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    input.emit(fake::idle());
    t.idle().await;
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
async fn resume_waits_for_replay_then_delivers_human_mail_while_busy() {
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
        let next = t.daemon.next().await;
        assert!(
            matches!(&next.message, ClientMessage::DeliverPeerMessage { message_id, .. } if message_id == &human.to_string())
        );
        next.emit(fake::receipt(format!("peer-{human}")));
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
        matches!(&input.message, ClientMessage::DeliverPeerMessage { text, .. } if text.contains("human after replay"))
    );
    input.emit(DaemonMessage::UserInputEcho {
        thread_id: fake::thread(),
        text: "human after replay".into(),
    });
    input.emit(fake::receipt(format!("peer-{id}")));
    input.emit(fake::idle());
    t.consumed(id).await;
    t.idle().await;
    t.finish().await;
}

#[tokio::test]
async fn boot_change_and_driver_failure_recover_only_busy_replays() {
    for (boot_change, driver_failure, idle) in [
        (true, false, false),
        (false, true, false),
        (false, false, false),
        (true, false, true),
    ] {
        let mut t = Test::new().await;
        let id = t.send(true, "before disconnect").await;
        let input = t.daemon.next().await;
        {
            let mut state = t.daemon.state.lock().expect("state");
            state.manual_replay = true;
            if boot_change {
                state.boot_id = "replacement-boot".into();
            }
        }
        if driver_failure {
            input.emit(DaemonMessage::Error {
                thread_id: fake::thread(),
                text: "Thread driver failed: panic".into(),
            });
        } else {
            input.die();
        }
        let connect = t.daemon.next().await;
        assert!(matches!(connect.message, ClientMessage::Connect { .. }));
        connect.emit(DaemonMessage::Replay {
            history: vec![],
            pending_choices: vec![],
            views: Default::default(),
            in_progress: false,
            idle,
        });
        if (boot_change || driver_failure) && !idle {
            let interrupt = t.daemon.next().await;
            assert!(matches!(interrupt.message, ClientMessage::Interrupt { .. }));
            interrupt.emit(fake::idle());
        }
        let reoffer = t.daemon.next().await;
        assert!(matches!(
            reoffer.message,
            ClientMessage::DeliverPeerMessage { .. }
        ));
        reoffer.emit(fake::receipt(format!("peer-{id}")));
        reoffer.emit(fake::idle());
        t.consumed(id).await;
        t.finish().await;
    }
}

#[tokio::test]
async fn silently_dropped_idle_input_has_a_receipt_deadline() {
    let mut t = Test::new().await;
    let id = t.send(false, "silently dropped").await;
    let _input = t.daemon.next().await;
    t.event(|e| {
        matches!(
            e,
            Event::PhaseChanged {
                phase: Phase::Crashed { .. },
                ..
            }
        )
    })
    .await;
    assert!(!matches!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Consumed
    ));
    t.finish().await;
}

#[tokio::test]
async fn stale_choice_answer_does_not_end_the_session() {
    let mut t = Test::new().await;
    let id = t.send(true, "choice").await;
    let input = t.daemon.next().await;
    input.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "old".into(),
        prompt: "Continue?".into(),
        choices: vec!["Yes".into()],
        default: 0,
    });
    let Event::PermissionPending(choice) =
        t.event(|e| matches!(e, Event::PermissionPending(_))).await
    else {
        panic!("permission");
    };
    input.emit(DaemonMessage::UserChoiceComplete {
        choice_id: "old".into(),
    });
    input.emit(DaemonMessage::Info {
        thread_id: fake::thread(),
        text: "choice gone".into(),
    });
    t.event(|e| matches!(e, Event::Activity { text, .. } if text == "choice gone"))
        .await;
    assert!(matches!(
        t.agent
            .answer_permission(
                choice.id,
                choice.incarnation,
                PermissionAnswer::Choose("0".into())
            )
            .await,
        Err(Error::StalePermission)
    ));
    input.emit(fake::receipt(format!("peer-{id}")));
    input.emit(fake::idle());
    t.consumed(id).await;
    t.idle().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
            .await
            .is_err()
    );
    t.finish().await;
}

#[tokio::test]
async fn boot_recovery_survives_disconnect_before_settlement() {
    let mut t = Test::new().await;
    let id = t.send(true, "old work").await;
    let input = t.daemon.next().await;
    {
        let mut state = t.daemon.state.lock().expect("state");
        state.manual_replay = true;
        state.boot_id = "replacement".into();
    }
    input.die();
    for disconnect in [true, false] {
        let connect = t.daemon.next().await;
        assert!(matches!(connect.message, ClientMessage::Connect { .. }));
        connect.emit(DaemonMessage::Replay {
            history: vec![],
            pending_choices: vec![],
            views: Default::default(),
            in_progress: false,
            idle: false,
        });
        let interrupt = t.daemon.next().await;
        assert!(matches!(interrupt.message, ClientMessage::Interrupt { .. }));
        if disconnect {
            interrupt.die();
        } else {
            interrupt.emit(fake::idle());
        }
    }
    let reoffer = t.daemon.next().await;
    assert!(matches!(
        reoffer.message,
        ClientMessage::DeliverPeerMessage { .. }
    ));
    reoffer.emit(fake::receipt(format!("peer-{id}")));
    reoffer.emit(fake::idle());
    t.consumed(id).await;
    t.finish().await;
}

#[tokio::test]
async fn r11_child_driver_failure_recovers_parent() {
    let mut t = Test::new().await;
    let id = t.send(false, "parent waiting on child").await;
    let input = t.daemon.next().await;
    t.daemon.state.lock().expect("state").manual_replay = true;
    input.emit(DaemonMessage::Error {
        thread_id: Some(infinity_protocol::ThreadRef::local("child".into())),
        text: "Thread driver failed: child panic".into(),
    });
    let connect = t.daemon.next().await;
    assert!(matches!(connect.message, ClientMessage::Connect { .. }));
    connect.emit(DaemonMessage::Replay {
        history: vec![],
        pending_choices: vec![],
        views: Default::default(),
        in_progress: false,
        idle: false,
    });
    let interrupt = t.daemon.next().await;
    assert!(matches!(interrupt.message, ClientMessage::Interrupt { .. }));
    interrupt.emit(fake::idle());
    let mail = t.daemon.next().await;
    mail.emit(fake::receipt(format!("peer-{id}")));
    mail.emit(fake::idle());
    t.consumed(id).await;
    t.finish().await;
}

#[tokio::test]
async fn r11_receipt_expiry_holds_mail_without_failing_agent() {
    let mut t = Test::new().await;
    let id = t.send(false, "provider keeps failing before output").await;
    for attempt in 0..5 {
        let input = t.daemon.next().await;
        assert!(matches!(
            input.message,
            ClientMessage::DeliverPeerMessage { .. }
        ));
        // A responsive daemon reports idle after the provider fails without a receipt.
        input.emit(fake::idle());
        let connect = t.daemon.next().await;
        assert!(
            matches!(connect.message, ClientMessage::Connect { .. }),
            "attempt {attempt}"
        );
    }
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
    assert!(matches!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Held { .. }
    ));
    let fresh = t.send(false, "provider repaired").await;
    let input = t.daemon.next().await;
    assert!(matches!(
        input.message,
        ClientMessage::DeliverPeerMessage { .. }
    ));
    input.emit(fake::receipt(format!("peer-{fresh}")));
    input.emit(fake::idle());
    t.consumed(fresh).await;
    assert!(!matches!(*t.agent.phases().borrow(), Phase::Failed { .. }));
    t.finish().await;
}

#[tokio::test]
async fn r11_settlement_outage_does_not_spend_crash_budget() {
    let mut t = Test::new().await;
    let id = t.send(false, "awaiting storage repair").await;
    let mut input = t.daemon.next().await;
    t.daemon.state.lock().expect("state").manual_replay = true;
    for _ in 0..6 {
        input.emit(DaemonMessage::Error {
            thread_id: fake::thread(),
            text: "Interrupt settlement held; will retry on input or timer after storage repair"
                .into(),
        });
        let connect = t.daemon.next().await;
        assert!(matches!(connect.message, ClientMessage::Connect { .. }));
        connect.emit(DaemonMessage::Replay {
            history: vec![],
            pending_choices: vec![],
            views: Default::default(),
            in_progress: false,
            idle: false,
        });
        input = t.daemon.next().await;
        assert!(matches!(input.message, ClientMessage::Interrupt { .. }));
    }
    input.emit(fake::idle());
    // Repeated unreceipted offers can be Held independently of session recovery.
    let fresh = t.send(false, "storage repaired").await;
    loop {
        let mail = t.daemon.next().await;
        let ClientMessage::DeliverPeerMessage { message_id, .. } = &mail.message else {
            panic!("mail")
        };
        mail.emit(fake::receipt(format!("peer-{message_id}")));
        mail.emit(fake::idle());
        if message_id == &fresh.to_string() {
            break;
        }
    }
    t.consumed(fresh).await;
    assert!(!matches!(*t.agent.phases().borrow(), Phase::Failed { .. }));
    assert!(!matches!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Offered { .. }
    ));
    t.finish().await;
}

#[tokio::test]
async fn r11_cleared_recovery_marker_survives_unclean_exit() {
    const CHILD: &str = "R11_MARKER_CHILD";
    if let Ok(path_file) = std::env::var(CHILD) {
        let mut t = Test::new().await;
        let id = t.send(false, "old work").await;
        let input = t.daemon.next().await;
        {
            let mut state = t.daemon.state.lock().expect("state");
            state.boot_id = "new-boot".into();
            state.manual_replay = true;
        }
        input.die();
        let connect = t.daemon.next().await;
        connect.emit(DaemonMessage::Replay {
            history: vec![],
            pending_choices: vec![],
            views: Default::default(),
            in_progress: false,
            idle: false,
        });
        let interrupt = t.daemon.next().await;
        assert!(matches!(interrupt.message, ClientMessage::Interrupt { .. }));
        interrupt.emit(fake::idle());
        let input = t.daemon.next().await;
        input.emit(fake::receipt(format!("peer-{id}")));
        input.emit(fake::idle());
        t.consumed(id).await;
        t.idle().await;
        std::fs::write(path_file, t.root.path().to_str().expect("path")).expect("path file");
        // Skip every destructor, including the actor's suspend/stop cleanup.
        std::process::exit(0);
    }
    let dir = tempfile::tempdir().expect("parent tempdir");
    let path_file = dir.path().join("child-path");
    let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "r11_cleared_recovery_marker_survives_unclean_exit",
            "--nocapture",
        ])
        .env(CHILD, &path_file)
        .status()
        .expect("child");
    assert!(status.success());
    let root = std::path::PathBuf::from(std::fs::read_to_string(path_file).expect("child path"));
    let db = rusqlite::Connection::open(root.join("mail.db")).expect("database");
    let token: String = db
        .query_row("SELECT resume FROM actor_state", [], |row| row.get(0))
        .expect("token");
    let encoded: String = serde_json::from_str(&token).expect("opaque token");
    let token: serde_json::Value = serde_json::from_str(&encoded).expect("Infinity token");
    std::fs::remove_dir_all(root).expect("child cleanup");
    assert_eq!(
        token["needs_interrupt"], false,
        "a settled recovery must not interrupt newer CLI work after agentd dies"
    );
}

#[tokio::test]
async fn r12_repeated_interrupt_expiry_is_bounded_without_receipts() {
    let mut t = Test::with_stall(Default::default(), Duration::from_millis(250)).await;
    let id = t.send(false, "stuck work").await;
    let input = t.daemon.next().await;
    input.emit(fake::start());
    t.event(|e| matches!(e, Event::Started { .. })).await;
    t.daemon.state.lock().expect("state").manual_replay = true;
    t.send(true, "waiting behind stuck work").await;
    let _waiting_offer = t.daemon.next().await;
    t.event(|e| matches!(e, Event::Activity { text, .. } if text.contains("mail-starvation stall timeout"))).await;
    assert!(matches!(
        t.daemon.next().await.message,
        ClientMessage::Interrupt { .. }
    ));
    for attempt in 1..7 {
        let event = t
            .event(|e| {
                matches!(
                    e,
                    Event::PhaseChanged {
                        phase: Phase::Crashed { .. },
                        ..
                    }
                )
            })
            .await;
        assert!(
            matches!(event, Event::PhaseChanged { phase: Phase::Crashed { attempt: actual, .. }, .. } if actual == attempt),
            "successful open must not reset backoff: {event:?}"
        );
        let connect = t.daemon.next().await;
        assert!(matches!(connect.message, ClientMessage::Connect { .. }));
        connect.emit(DaemonMessage::Replay {
            history: vec![],
            pending_choices: vec![],
            views: Default::default(),
            in_progress: false,
            idle: false,
        });
        assert!(matches!(
            t.daemon.next().await.message,
            ClientMessage::Interrupt { .. }
        ));
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(10 * 60)).await;
    tokio::time::resume();
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
    assert_ne!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Consumed
    );
    let queued = t.send(false, "waiting for operator recovery").await;
    let delivery = t.mailroom.delivery(queued).await.expect("queued delivery");
    assert_eq!(delivery.state, DeliveryState::Queued);
    assert_eq!(delivery.attempts, 0);
    t.finish().await;
}

#[tokio::test]
async fn r11a_failure_marker_clears_and_rearms() {
    for in_progress in [false, true] {
        let mut t = Test::new().await;
        t.daemon.state.lock().expect("state").manual_replay = true;
        for failure in [true, true, false] {
            let id = t.send(true, "work").await;
            let input = t.daemon.next().await;
            input.emit(fake::start());
            if failure {
                input.emit(DaemonMessage::Error {
                    thread_id: fake::thread(),
                    text: "Thread driver failed: panic".into(),
                });
            } else {
                input.die();
            }
            let connect = t.daemon.next().await;
            assert!(matches!(connect.message, ClientMessage::Connect { .. }));
            connect.emit(DaemonMessage::Replay {
                history: vec![],
                pending_choices: vec![],
                views: Default::default(),
                in_progress,
                idle: false,
            });
            if failure {
                let interrupt = t.daemon.next().await;
                assert!(matches!(interrupt.message, ClientMessage::Interrupt { .. }));
                interrupt.emit(fake::start());
            }
            let reoffer = t.daemon.next().await;
            assert!(matches!(
                reoffer.message,
                ClientMessage::DeliverPeerMessage { .. }
            ));
            reoffer.emit(fake::receipt(format!("peer-{id}")));
            reoffer.emit(fake::idle());
            t.consumed(id).await;
            t.idle().await;
        }
        t.finish().await;
    }
}

#[tokio::test]
async fn stall_timer_only_cancels_waiting_mail_and_activity_resets_it() {
    let mut t = Test::with_stall(Default::default(), Duration::from_millis(250)).await;
    let first = t.send(false, "first").await;
    let active = t.daemon.next().await;
    active.emit(fake::start());
    active.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(350), t.daemon.next())
            .await
            .is_err(),
        "no mail waiting, no stall deadline"
    );
    active.emit(DaemonMessage::TextChunk {
        thread_id: fake::thread(),
        chunk: "resumed work".into(),
    });
    t.event(|e| matches!(e, Event::Activity { text, .. } if text == "resumed work"))
        .await;
    let waiting = t.send(true, "waiting").await;
    let offer = t.daemon.next().await;
    for _ in 0..4 {
        active.emit(DaemonMessage::TextChunk {
            thread_id: fake::thread(),
            chunk: "still working".into(),
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), t.daemon.next())
                .await
                .is_err(),
            "activity resets the deadline"
        );
    }
    active.emit(DaemonMessage::ToolResult {
        thread_id: fake::thread(),
        segments: vec![],
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(150), t.daemon.next())
            .await
            .is_err(),
        "tool end resets the deadline too"
    );
    let cancel = t.daemon.next().await;
    assert!(matches!(cancel.message, ClientMessage::Interrupt { .. }));
    offer.emit(fake::receipt(format!("peer-{waiting}")));
    offer.emit(fake::idle());
    t.consumed(waiting).await;
    t.finish().await;
}

#[tokio::test]
async fn zero_stall_timeout_disables_cancellation() {
    let mut t = Test::with_stall(Default::default(), Duration::ZERO).await;
    t.send(false, "silently busy").await;
    let active = t.daemon.next().await;
    active.emit(fake::start());
    assert!(
        tokio::time::timeout(Duration::from_millis(350), t.daemon.next())
            .await
            .is_err()
    );
    t.finish().await;
}

#[tokio::test]
async fn r15a_replayed_choice_must_not_refresh_stall_clock() {
    let mut t = Test::with_stall(Default::default(), Duration::from_secs(600)).await;
    t.daemon.state.lock().expect("state").manual_replay = true;
    let first = t.send(false, "start choice").await;
    let mut connection = t.daemon.next().await;
    connection.emit(fake::start());
    connection.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    let choice = || DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "unchanged-choice".into(),
        prompt: "choose".into(),
        choices: vec!["yes".into(), "no".into()],
        default: 1,
    };
    let waiting = t.send(true, "waiting behind work").await;
    let _offer = t.daemon.next().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(250)).await;
    tokio::time::resume();
    connection.die();
    connection = t.daemon.next().await;
    connection.emit(DaemonMessage::Replay {
        history: vec![],
        pending_choices: vec![choice()],
        views: Default::default(),
        in_progress: false,
        idle: false,
    });
    t.event(|e| matches!(e, Event::PermissionPending(_))).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(349)).await;
    tokio::time::resume();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
            .await
            .is_err()
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    let cancel = tokio::time::timeout(Duration::from_millis(150), t.daemon.next()).await;
    assert!(
        matches!(
            cancel.map(|r| r.message),
            Ok(ClientMessage::Interrupt { .. })
        ),
        "reconnect replays must preserve the 250 seconds of real inactivity before the choice"
    );
    assert_ne!(
        t.mailroom.delivery(waiting).await.expect("mail").state,
        DeliveryState::Consumed
    );
    t.finish().await;
}

#[tokio::test]
async fn r15a_replay_without_choice_preserves_stall_deadline() {
    let mut t = Test::with_stall(Default::default(), Duration::from_secs(600)).await;
    t.daemon.state.lock().expect("state").manual_replay = true;
    let first = t.send(false, "start silent work").await;
    let active = t.daemon.next().await;
    active.emit(fake::start());
    active.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    let waiting = t.send(true, "waiting").await;
    let offer = t.daemon.next().await;
    for _ in 0..2 {
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(250)).await;
        tokio::time::resume();
    }
    offer.die();
    let connect = t.daemon.next().await;
    assert!(matches!(connect.message, ClientMessage::Connect { .. }));
    connect.emit(DaemonMessage::Replay {
        history: vec![],
        pending_choices: vec![],
        views: Default::default(),
        in_progress: true,
        idle: false,
    });
    let reoffer = t.daemon.next().await;
    assert!(matches!(
        reoffer.message,
        ClientMessage::DeliverPeerMessage { .. }
    ));
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(101)).await;
    tokio::time::resume();
    let cancel = t.daemon.next().await;
    assert!(matches!(cancel.message, ClientMessage::Interrupt { .. }));
    assert_ne!(
        t.mailroom.delivery(waiting).await.expect("mail").state,
        DeliveryState::Consumed
    );
    t.finish().await;
}

#[tokio::test]
async fn stale_choice_error_does_not_crash_root_session() {
    let mut t = Test::new().await;
    let first = t.send(false, "start work").await;
    let connection = t.daemon.next().await;
    connection.emit(fake::start());
    connection.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    connection.emit(DaemonMessage::UserChoiceError {
        choice_id: "stale".into(),
        text: "Unknown or stale choice: stale".into(),
    });
    connection.emit(fake::idle());
    t.idle().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), t.daemon.next())
            .await
            .is_err()
    );
    t.finish().await;
}

mod r16o {
    use super::*;

    /// agentd is Idle when the daemon starts root work agentd did not submit: a CLI
    /// turn on the same root, or a subscription event respawning the driver. Mail
    /// offered from Idle then queues behind that work inside the daemon. The adapter
    /// sets no receipt deadline because the thread is busy. Nothing commits it.
    #[tokio::test]
    async fn r16o_mail_offered_from_idle_behind_daemon_work_arms_stall() {
        let mut t = Test::with_stall(Default::default(), Duration::from_millis(300)).await;
        let first = t.send(false, "first turn").await;
        let connection = t.daemon.next().await;
        connection.emit(fake::start());
        connection.emit(fake::receipt(format!("peer-{first}")));
        t.consumed(first).await;
        connection.emit(fake::idle());
        t.idle().await;
        connection.emit(fake::start());
        connection.emit(DaemonMessage::ToolCall {
            thread_id: fake::thread(),
            name: "cli_long_tool".into(),
            args: "{}".into(),
            display_as: None,
        });
        t.event(|e| matches!(e, Event::Activity { text, .. } if text == "cli_long_tool"))
            .await;
        let phase_before = t.agent.phase();
        let waiting = t.send(true, "mail behind daemon work").await;
        let offer = t.daemon.next().await;
        assert!(matches!(
            offer.message,
            ClientMessage::DeliverPeerMessage { .. }
        ));
        let next = tokio::time::timeout(Duration::from_secs(2), t.daemon.next()).await;
        let interrupted = next
            .as_ref()
            .is_ok_and(|r| matches!(r.message, ClientMessage::Interrupt { .. }));
        let state = t.mailroom.delivery(waiting).await.expect("delivery").state;
        eprintln!(
            "r16o idle-offer behind daemon work: phase_before_offer={phase_before:?} interrupted_within_2s={interrupted} phase={:?} mail={state:?}",
            t.agent.phase()
        );
        offer.emit(fake::receipt(format!("peer-{waiting}")));
        offer.emit(fake::idle());
        t.finish().await;
        assert_eq!(phase_before, Phase::Idle, "probe precondition");
        assert!(
            interrupted,
            "mail waiting behind daemon work must arm the stall timer"
        );
    }

    #[tokio::test]
    async fn r16o_replayed_answered_choice_must_not_suspend_stall() {
        let mut t = Test::with_stall(Default::default(), Duration::from_millis(1500)).await;
        t.daemon.state.lock().expect("state").manual_replay = true;
        let first = t.send(false, "start choice").await;
        let connection = t.daemon.next().await;
        connection.emit(fake::start());
        connection.emit(fake::receipt(format!("peer-{first}")));
        t.consumed(first).await;
        let choice = || DaemonMessage::UserChoiceRequired {
            thread_id: fake::thread(),
            id: "answered-choice".into(),
            prompt: "choose".into(),
            choices: vec!["yes".into(), "no".into()],
            default: 1,
        };
        connection.emit(choice());
        let Event::PermissionPending(request) =
            t.event(|e| matches!(e, Event::PermissionPending(_))).await
        else {
            unreachable!()
        };
        let waiting = t.send(true, "waiting behind answered choice").await;
        t.agent
            .answer_permission(
                request.id,
                request.incarnation,
                PermissionAnswer::Choose("0".into()),
            )
            .await
            .expect("human answers");
        let answered = t.daemon.next().await;
        assert!(matches!(
            answered.message,
            ClientMessage::UserChoiceAnswered { .. }
        ));
        let offer = t.daemon.next().await;
        assert!(matches!(
            offer.message,
            ClientMessage::DeliverPeerMessage { .. }
        ));
        offer.die();
        let connect = t.daemon.next().await;
        assert!(matches!(connect.message, ClientMessage::Connect { .. }));
        connect.emit(DaemonMessage::Replay {
            history: vec![],
            pending_choices: vec![choice()],
            views: Default::default(),
            in_progress: false,
            idle: false,
        });
        let replayed = t.event(|e| matches!(e, Event::PermissionPending(_))).await;
        let mut next = None;
        let end = tokio::time::Instant::now() + Duration::from_secs(4);
        while let Ok(request) = tokio::time::timeout_at(end, t.daemon.next()).await {
            if matches!(request.message, ClientMessage::Interrupt { .. }) {
                next = Some(request);
                break;
            }
        }
        let pending = t.agent.pending_permissions().await.expect("pending");
        eprintln!(
            "r16o replayed answered choice: replayed={} interrupted_within_4s={} pending_after={} phase={:?} mail={:?}",
            matches!(replayed, Event::PermissionPending(_)),
            next.is_some(),
            pending.len(),
            t.agent.phase(),
            t.mailroom.delivery(waiting).await.expect("delivery").state
        );
        t.finish().await;
        assert!(
            next.is_some(),
            "an already-answered choice must not suspend the stall timer after replay"
        );
    }
}

#[tokio::test]
async fn r16a_finite_human_timeout_survives_reconnects() {
    finite_human_timeout_survives_reconnects(false).await;
}

#[tokio::test]
async fn r17_answered_choice_replay_keeps_original_deadline() {
    finite_human_timeout_survives_reconnects(true).await;
}

async fn finite_human_timeout_survives_reconnects(answer_before_replay: bool) {
    let policy = PermissionPolicy {
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    };
    // Disable starvation cancellation to isolate the human deadline.
    let mut t = Test::with_stall(policy, Duration::ZERO).await;
    t.daemon.state.lock().expect("state").manual_replay = true;
    let first = t.send(false, "start choice").await;
    let mut connection = t.daemon.next().await;
    connection.emit(fake::start());
    connection.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    let choice = || DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "finite-choice".into(),
        prompt: "choose".into(),
        choices: vec!["yes".into(), "no".into()],
        default: 1,
    };
    connection.emit(choice());
    let Event::PermissionPending(mut request) =
        t.event(|e| matches!(e, Event::PermissionPending(_))).await
    else {
        unreachable!()
    };
    let started = tokio::time::Instant::now();
    for _ in 0..2 {
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(250)).await;
        tokio::time::resume();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
                .await
                .is_err()
        );
        if answer_before_replay {
            t.agent
                .answer_permission(
                    request.id,
                    request.incarnation,
                    PermissionAnswer::Choose("0".into()),
                )
                .await
                .expect("answer before replay");
            let answer = t.daemon.next().await;
            assert!(matches!(
                answer.message,
                ClientMessage::UserChoiceAnswered { .. }
            ));
        }
        connection.die();
        connection = t.daemon.next().await;
        connection.emit(DaemonMessage::Replay {
            history: vec![],
            pending_choices: vec![choice()],
            views: Default::default(),
            in_progress: false,
            idle: false,
        });
        let Event::PermissionPending(restored) =
            t.event(|e| matches!(e, Event::PermissionPending(_))).await
        else {
            unreachable!()
        };
        request = restored;
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(99)).await;
    tokio::time::resume();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
            .await
            .is_err()
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    let cancel = tokio::time::timeout(Duration::from_millis(150), t.daemon.next()).await;
    eprintln!(
        "finite human deadline: answered_before_replay={answer_before_replay} elapsed={:?} expired={}",
        started.elapsed(),
        cancel.is_ok()
    );
    assert!(matches!(
        cancel.map(|r| r.message),
        Ok(ClientMessage::Interrupt { .. })
    ));
    assert!(started.elapsed() < Duration::from_secs(603));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
            .await
            .is_err(),
        "expires once"
    );
    connection.emit(fake::start());
    connection.emit(fake::idle());
    t.finish().await;
}

#[tokio::test]
async fn r16_pending_choice_with_queued_mail_has_starvation_bound() {
    let mut t = Test::with_stall(Default::default(), Duration::from_secs(600)).await;
    let first = t.send(false, "start choice").await;
    let connection = t.daemon.next().await;
    connection.emit(fake::start());
    connection.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    connection.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: "unanswered-choice".into(),
        prompt: "choose".into(),
        choices: vec!["yes".into()],
        default: 0,
    });
    t.event(|e| matches!(e, Event::PermissionPending(_))).await;
    // A choice without other mail does not arm the stall timer.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(100)).await;
    tokio::time::resume();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
            .await
            .is_err()
    );
    let waiting = t.send(true, "waiting behind human").await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(499)).await;
    tokio::time::resume();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
            .await
            .is_err()
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    let cancel = tokio::time::timeout(Duration::from_millis(150), t.daemon.next()).await;
    assert!(
        matches!(
            cancel.map(|r| r.message),
            Ok(ClientMessage::Interrupt { .. })
        ),
        "human wait counts toward stall_timeout once mail waits"
    );
    assert_eq!(
        t.mailroom.delivery(waiting).await.expect("mail").state,
        DeliveryState::Queued
    );
    t.finish().await;
}

async fn r17_choice_reuse_probe(reuse: bool, end_turn: bool) {
    let policy = PermissionPolicy {
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    };
    let mut t = Test::with_stall(policy, Duration::ZERO).await;
    let first = t.send(false, "work").await;
    let connection = t.daemon.next().await;
    connection.emit(fake::start());
    connection.emit(fake::receipt(format!("peer-{first}")));
    t.consumed(first).await;
    let choice = |id: &str| DaemonMessage::UserChoiceRequired {
        thread_id: fake::thread(),
        id: id.into(),
        prompt: "choose".into(),
        choices: vec!["yes".into()],
        default: 0,
    };
    connection.emit(choice("reused"));
    t.event(|e| matches!(e, Event::PermissionPending(_))).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(550)).await;
    tokio::time::resume();
    connection.emit(DaemonMessage::UserChoiceComplete {
        choice_id: "reused".into(),
    });
    t.event(|e| matches!(e, Event::PermissionAnswered { .. }))
        .await;
    if end_turn {
        connection.emit(fake::idle());
        t.idle().await;
    }
    connection.emit(DaemonMessage::ToolResult {
        thread_id: fake::thread(),
        segments: vec![],
    });
    connection.emit(choice(if reuse { "reused" } else { "different" }));
    t.event(|e| matches!(e, Event::PermissionPending(_))).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(51)).await;
    tokio::time::resume();
    let next = tokio::time::timeout(Duration::from_millis(30), t.daemon.next()).await;
    eprintln!(
        "R17 reused={reuse} ended_turn={end_turn} new_choice_age=51s budget=600s premature_interrupt={}",
        next.as_ref()
            .is_ok_and(|r| matches!(r.message, ClientMessage::Interrupt { .. }))
    );
    t.finish().await;
    assert!(
        next.is_err(),
        "a new choice must receive its own permission budget"
    );
}
#[tokio::test]
async fn r17_settled_choice_id_reuse_gets_new_deadline() {
    r17_choice_reuse_probe(true, false).await;
}
#[tokio::test]
async fn r17_distinct_choice_id_gets_new_deadline() {
    r17_choice_reuse_probe(false, false).await;
}
#[tokio::test]
async fn r17_choice_id_reuse_after_turn_gets_new_deadline() {
    r17_choice_reuse_probe(true, true).await;
}

#[cfg(test)]
mod r17o {
    use super::*;

    /// The daemon has begun root work that agentd did not submit (a CLI turn), but its
    /// first StartOutput has not reached agentd. agentd is Idle and offers mail as its own
    /// turn. The completion that follows is the CLI's: it commits only the CLI input and
    /// leaves agentd's input queued behind a tool that never returns.
    #[tokio::test]
    async fn r17o_offer_before_foreign_start_output_must_arm_stall() {
        let mut t = Test::with_stall(Default::default(), Duration::from_millis(300)).await;
        let first = t.send(false, "first turn").await;
        let connection = t.daemon.next().await;
        connection.emit(fake::start());
        connection.emit(fake::receipt(format!("peer-{first}")));
        t.consumed(first).await;
        connection.emit(fake::idle());
        t.idle().await;
        let phase_before = t.agent.phase();
        let waiting = t.send(true, "mail racing a cli start").await;
        let offer = t.daemon.next().await;
        assert!(matches!(
            offer.message,
            ClientMessage::DeliverPeerMessage { .. }
        ));
        offer.emit(DaemonMessage::UserInputEcho {
            thread_id: fake::thread(),
            text: "cli input".into(),
        });
        offer.emit(fake::start());
        offer.emit(DaemonMessage::ToolCall {
            thread_id: fake::thread(),
            name: "cli_long_tool".into(),
            args: "{}".into(),
            display_as: None,
        });
        offer.emit(fake::receipt("cli-input".into()));
        offer.emit(fake::round_done());
        t.event(|e| matches!(e, Event::Activity { text, .. } if text == "cli_long_tool"))
            .await;
        let next = tokio::time::timeout(Duration::from_secs(2), t.daemon.next()).await;
        let interrupted = next
            .as_ref()
            .is_ok_and(|r| matches!(r.message, ClientMessage::Interrupt { .. }));
        let state = t.mailroom.delivery(waiting).await.expect("delivery").state;
        eprintln!(
            "r17o offer before foreign StartOutput: phase_before_offer={phase_before:?} interrupted_within_2s={interrupted} phase={:?} mail={state:?}",
            t.agent.phase()
        );
        if interrupted {
            offer.emit(fake::start());
        }
        offer.emit(fake::receipt(format!("peer-{waiting}")));
        offer.emit(fake::idle());
        t.finish().await;
        assert_eq!(phase_before, Phase::Idle, "probe precondition");
        assert!(
            interrupted,
            "mail queued behind foreign work must arm the stall timer"
        );
    }

    /// RAP keys a choice by its tool call ID, so a tool that asks two questions in one call
    /// reuses the wire ID. agentd answers the first; the second must get its own timeout.
    #[tokio::test]
    async fn r17o_second_question_on_same_call_keeps_full_human_deadline() {
        let policy = PermissionPolicy {
            timeout: Some(Duration::from_secs(600)),
            ..Default::default()
        };
        let mut t = Test::with_stall(policy, Duration::ZERO).await;
        let first = t.send(false, "ask twice").await;
        let connection = t.daemon.next().await;
        connection.emit(fake::start());
        connection.emit(fake::receipt(format!("peer-{first}")));
        t.consumed(first).await;
        let question = |prompt: &str| DaemonMessage::UserChoiceRequired {
            thread_id: fake::thread(),
            id: "call-1".into(),
            prompt: prompt.into(),
            choices: vec!["yes".into(), "no".into()],
            default: 1,
        };
        connection.emit(question("first question"));
        let Event::PermissionPending(q1) =
            t.event(|e| matches!(e, Event::PermissionPending(_))).await
        else {
            unreachable!()
        };
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(590)).await;
        tokio::time::resume();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), t.daemon.next())
                .await
                .is_err()
        );
        t.agent
            .answer_permission(q1.id, q1.incarnation, PermissionAnswer::Choose("0".into()))
            .await
            .expect("human answers the first question");
        let answered = t.daemon.next().await;
        assert!(matches!(
            answered.message,
            ClientMessage::UserChoiceAnswered { .. }
        ));
        connection.emit(question("second question"));
        let Event::PermissionPending(q2) =
            t.event(|e| matches!(e, Event::PermissionPending(_))).await
        else {
            unreachable!()
        };
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(20)).await;
        tokio::time::resume();
        let early = tokio::time::timeout(Duration::from_millis(150), t.daemon.next()).await;
        let cancelled = early
            .as_ref()
            .is_ok_and(|r| matches!(r.message, ClientMessage::Interrupt { .. }));
        let pending = t.agent.pending_permissions().await.expect("pending");
        eprintln!(
            "r17o second question on same call: q2_title={:?} cancelled_20s_after_q2={cancelled} pending_after={} phase={:?}",
            q2.title,
            pending.len(),
            t.agent.phase()
        );
        if cancelled {
            connection.emit(fake::start());
        }
        connection.emit(fake::idle());
        t.finish().await;
        assert_ne!(q1.id, q2.id, "probe precondition: a new permission");
        assert!(
            !cancelled,
            "a second question must not inherit the first question's deadline"
        );
    }

    /// Control: the second question comes from a different call ID.
    #[tokio::test]
    async fn r17o_control_second_question_new_call_keeps_full_deadline() {
        let policy = PermissionPolicy {
            timeout: Some(Duration::from_secs(600)),
            ..Default::default()
        };
        let mut t = Test::with_stall(policy, Duration::ZERO).await;
        let first = t.send(false, "ask twice").await;
        let connection = t.daemon.next().await;
        connection.emit(fake::start());
        connection.emit(fake::receipt(format!("peer-{first}")));
        t.consumed(first).await;
        let question = |id: &str| DaemonMessage::UserChoiceRequired {
            thread_id: fake::thread(),
            id: id.into(),
            prompt: "question".into(),
            choices: vec!["yes".into(), "no".into()],
            default: 1,
        };
        connection.emit(question("call-1"));
        let Event::PermissionPending(q1) =
            t.event(|e| matches!(e, Event::PermissionPending(_))).await
        else {
            unreachable!()
        };
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(590)).await;
        tokio::time::resume();
        t.agent
            .answer_permission(q1.id, q1.incarnation, PermissionAnswer::Choose("0".into()))
            .await
            .expect("answer");
        let _answered = t.daemon.next().await;
        connection.emit(question("call-2"));
        t.event(|e| matches!(e, Event::PermissionPending(_))).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(20)).await;
        tokio::time::resume();
        let early = tokio::time::timeout(Duration::from_millis(150), t.daemon.next()).await;
        eprintln!(
            "r17o control second question new call: cancelled_20s_after_q2={}",
            early.is_ok()
        );
        connection.emit(fake::idle());
        t.finish().await;
        assert!(
            early.is_err(),
            "control: a different call keeps its full deadline"
        );
    }
}
