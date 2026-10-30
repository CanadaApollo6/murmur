use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use agentd::KindConfig;
use agentd::mailroom::{Mailroom, NewMessage};
use agentd::model::*;
use agentd::supervisor::{
    AgentHandle, AgentSpec, Broker, Config, Error, Event, Phase, TurnOutcome,
};
use agentd::swarm::Act;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::broadcast;

struct Test {
    root: TempDir,
    mailroom: Arc<Mailroom>,
    broker: Broker,
    events: broadcast::Receiver<Event>,
    agent: AgentHandle,
}
fn kind(root: &Path, codex: bool) -> KindConfig {
    let bin = env!("CARGO_BIN_EXE_agentd-fake-acp");
    let mut kind = if codex {
        KindConfig::codex(bin)
    } else {
        KindConfig::claude(bin)
    };
    kind.env
        .insert("FAKE_ROOT".into(), root.display().to_string());
    kind.env.insert(
        "FAKE_MODE".into(),
        if codex { "read-only" } else { "default" }.into(),
    );
    kind.mcp_servers = vec![json!({"name":"later","command":"unused","args":[],"env":[]})];
    kind.idle_after = None;
    kind.request_timeout = Duration::from_secs(3);
    kind
}
fn spec(root: &Path, kind: &str, policy: PermissionPolicy) -> AgentSpec {
    AgentSpec {
        name: AgentName::parse("worker").expect("name"),
        kind: kind.into(),
        cwd: root.into(),
        model: None,
        permissions: policy,
        idle_after: None,
    }
}
fn config() -> Config {
    Config {
        backoff_initial: Duration::from_millis(20),
        backoff_max: Duration::from_millis(60),
        ..Config::default()
    }
}
impl Test {
    async fn new(policy: PermissionPolicy) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        Self::at(root, policy, config(), false, None).await
    }
    async fn at(
        root: TempDir,
        policy: PermissionPolicy,
        config: Config,
        codex: bool,
        idle: Option<Duration>,
    ) -> Self {
        let mailroom = Arc::new(
            Mailroom::open(root.path().join("mail.db"), Default::default())
                .await
                .expect("mailroom"),
        );
        let mut kind = kind(root.path(), codex);
        kind.idle_after = idle;
        let name = kind.name.clone();
        let broker = Broker::new(Arc::clone(&mailroom), vec![kind], config).expect("broker");
        let events = broker.events();
        let agent = broker
            .spawn(spec(root.path(), &name, policy))
            .await
            .expect("spawn");
        Self {
            root,
            mailroom,
            broker,
            events,
            agent,
        }
    }
    async fn send(&self, body: &str) -> MessageId {
        self.send_from(body, Sender::Human).await
    }
    async fn send_from(&self, body: &str, from: Sender) -> MessageId {
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
    async fn event(&mut self, matches: impl Fn(&Event) -> bool) -> Event {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = self.events.recv().await.expect("event");
                if matches(&event) {
                    return event;
                }
            }
        })
        .await
        .expect("event timeout")
    }
    async fn consumed(&mut self, id: MessageId) {
        self.event(|e| matches!(e,Event::Consumed { ids,.. } if ids.contains(&id)))
            .await;
        assert_eq!(
            self.mailroom.delivery(id).await.expect("delivery").state,
            DeliveryState::Consumed
        );
    }
    async fn started(&mut self, id: MessageId) {
        self.event(|e| matches!(e,Event::Started { ids,.. } if ids.contains(&id)))
            .await;
    }
    async fn pending(&mut self) -> PermissionRequest {
        let Event::PermissionPending(r) = self
            .event(|e| matches!(e, Event::PermissionPending(_)))
            .await
        else {
            panic!("permission")
        };
        r
    }
    fn release(&self) {
        std::fs::write(self.root.path().join("release"), "").expect("release");
    }
    fn wire(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("wire.jsonl"))
            .expect("wire")
            .lines()
            .map(|l| serde_json::from_str(l).expect("wire JSON"))
            .collect()
    }
    async fn finish(self) {
        self.broker.shutdown().await.expect("shutdown");
        drop(self.broker);
        Arc::try_unwrap(self.mailroom)
            .ok()
            .expect("mailroom sole owner")
            .close()
            .await
            .expect("close");
    }
}

#[tokio::test]
async fn open_mode_prompt_receipt_and_response_only() {
    for codex in [false, true] {
        let root = tempfile::tempdir().expect("tempdir");
        let mut t = Test::at(root, Default::default(), config(), codex, None).await;
        let id = t.send("response-only").await;
        t.started(id).await;
        t.consumed(id).await;
        let wire = t.wire();
        let methods = wire
            .iter()
            .filter_map(|m| m["method"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "initialize",
                "session/new",
                "session/set_mode",
                "session/prompt"
            ]
        );
        assert_eq!(
            wire[2]["params"]["mcpServers"],
            json!([{"name":"later","command":"unused","args":[],"env":[]}])
        );
        assert_eq!(wire[4]["params"]["prompt"][0]["text"], "response-only");
        t.finish().await;
    }
}

#[tokio::test]
async fn busy_mail_waits_then_batches_peer_framing_and_human_input() {
    let mut t = Test::new(Default::default()).await;
    let first = t.send("hold").await;
    t.started(first).await;
    let peer = t
        .mailroom
        .register(AgentName::parse("reviewer").expect("name"))
        .await
        .expect("peer");
    let second = t.send_from("hello <world>", Sender::Agent(peer)).await;
    let third = t.send("human mail").await;
    assert_eq!(
        t.mailroom.delivery(second).await.expect("delivery").state,
        DeliveryState::Queued
    );
    assert_eq!(
        t.mailroom.delivery(third).await.expect("delivery").state,
        DeliveryState::Queued
    );
    assert_eq!(t.agent.phase(), Phase::Busy);
    assert_eq!(
        t.wire()
            .iter()
            .filter(|m| m["method"] == "session/prompt")
            .count(),
        1
    );
    t.release();
    t.consumed(third).await;
    assert_eq!(
        t.mailroom.delivery(second).await.expect("delivery").state,
        DeliveryState::Consumed
    );
    let wire = t.wire();
    let prompts = wire
        .iter()
        .filter(|m| m["method"] == "session/prompt")
        .collect::<Vec<_>>();
    assert_eq!(prompts.len(), 2);
    assert_eq!(
        prompts[1]["params"]["prompt"]
            .as_array()
            .expect("blocks")
            .len(),
        2
    );
    let text = prompts[1]["params"]["prompt"][0]["text"]
        .as_str()
        .expect("text");
    assert!(text.contains(&format!(
        "<agent-message id=\"{second}\" from=\"@reviewer\" kind=\"unknown\""
    )));
    assert!(text.contains("hello &lt;world&gt;"));
    assert_eq!(prompts[1]["params"]["prompt"][1]["text"], "human mail");
    t.finish().await;
}

#[tokio::test]
async fn idle_suspend_resumes_and_load_failure_needs_recovery() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut t = Test::at(
        root,
        Default::default(),
        config(),
        false,
        Some(Duration::from_millis(25)),
    )
    .await;
    let id = t.send("first").await;
    t.consumed(id).await;
    t.event(|e| {
        matches!(
            e,
            Event::PhaseChanged {
                phase: Phase::Dormant,
                ..
            }
        )
    })
    .await;
    let id = t.send("second").await;
    t.consumed(id).await;
    assert_eq!(
        t.wire()
            .iter()
            .filter(|m| m["method"] == "session/new")
            .count(),
        1
    );
    assert_eq!(
        t.wire()
            .iter()
            .filter(|m| m["method"] == "session/load")
            .count(),
        1
    );
    t.event(|e| {
        matches!(
            e,
            Event::PhaseChanged {
                phase: Phase::Dormant,
                ..
            }
        )
    })
    .await;
    std::fs::write(t.root.path().join("fail-load"), "").expect("flag");
    let queued = t.send("third").await;
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
    assert_eq!(
        t.mailroom.delivery(queued).await.expect("delivery").state,
        DeliveryState::Queued
    );
    assert_eq!(
        t.wire()
            .iter()
            .filter(|m| m["method"] == "session/new")
            .count(),
        1
    );
    t.finish().await;
}

#[tokio::test]
async fn death_before_start_requeues_and_after_start_is_uncertain() {
    for (body, started) in [
        ("die-before", false),
        ("die-after", true),
        ("protocol-error", false),
    ] {
        let mut t = Test::at(
            tempfile::tempdir().expect("tempdir"),
            Default::default(),
            Config {
                crash_limit: 1,
                ..config()
            },
            false,
            None,
        )
        .await;
        let id = t.send(body).await;
        if started {
            t.started(id).await;
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
        let state = t.mailroom.delivery(id).await.expect("delivery").state;
        assert!(if started {
            matches!(state, DeliveryState::Uncertain { .. })
        } else {
            state == DeliveryState::Queued
        });
        t.finish().await;
    }
}

#[tokio::test]
async fn crash_backoff_and_five_crashes_fail() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("crash-open"), "").expect("flag");
    let mut t = Test::at(root, Default::default(), config(), false, None).await;
    let mut previous = None;
    for expected in 1..=4 {
        let Event::PhaseChanged {
            phase: Phase::Crashed { attempt, retry_at },
            ..
        } = t
            .event(|e| {
                matches!(
                    e,
                    Event::PhaseChanged {
                        phase: Phase::Crashed { .. },
                        ..
                    }
                )
            })
            .await
        else {
            panic!("crash")
        };
        assert_eq!(attempt, expected);
        if let Some(previous) = previous {
            assert!(tokio::time::Instant::now() >= previous);
        }
        previous = Some(retry_at);
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
    assert!(tokio::time::Instant::now() >= previous.expect("retry"));
    assert_eq!(
        t.wire().iter().filter(|m| m.get("pid").is_some()).count(),
        5
    );
    t.finish().await;
}

#[tokio::test]
async fn permission_escalation_answer_and_stale_incarnation() {
    let mut t = Test::new(Default::default()).await;
    let id = t.send("permission").await;
    let request = t.pending().await;
    assert_eq!(request.tool, "edit");
    assert_eq!(t.agent.pending_permissions().await.expect("list").len(), 1);
    assert_eq!(t.agent.phase(), Phase::Busy);
    let queued = t.send("queued").await;
    assert_eq!(
        t.mailroom.delivery(queued).await.expect("delivery").state,
        DeliveryState::Queued
    );
    assert!(matches!(
        t.agent
            .answer_permission(
                request.id,
                Incarnation(request.incarnation.0 + 1),
                PermissionAnswer::Choose("yes".into())
            )
            .await,
        Err(Error::StalePermission)
    ));
    assert!(
        t.agent
            .answer_permission(
                request.id,
                request.incarnation,
                PermissionAnswer::Choose("bad".into())
            )
            .await
            .is_err()
    );
    t.agent
        .answer_permission(
            request.id,
            request.incarnation,
            PermissionAnswer::Choose("yes".into()),
        )
        .await
        .expect("answer");
    t.consumed(queued).await;
    assert_eq!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Consumed
    );
    assert!(
        t.agent
            .pending_permissions()
            .await
            .expect("list")
            .is_empty()
    );
    assert!(matches!(
        t.agent
            .answer_permission(
                request.id,
                request.incarnation,
                PermissionAnswer::Choose("yes".into())
            )
            .await,
        Err(Error::StalePermission)
    ));
    assert!(
        t.wire()
            .iter()
            .any(|m| m["id"] == "permission-1" && m["result"]["outcome"]["optionId"] == "yes")
    );
    t.finish().await;
}

#[tokio::test]
async fn permission_timeout_and_per_agent_rules() {
    for decision in [None, Some(RuleDecision::Allow), Some(RuleDecision::Deny)] {
        let policy = PermissionPolicy {
            timeout: Some(Duration::from_millis(25)),
            rules: decision
                .map(|d| {
                    vec![PermissionRule {
                        tool: "edit".into(),
                        decision: d,
                    }]
                })
                .unwrap_or_default(),
            ..Default::default()
        };
        let mut t = Test::new(policy).await;
        let id = t.send("permission").await;
        if decision.is_none() {
            t.pending().await;
        }
        t.consumed(id).await;
        let expected = if matches!(decision, Some(RuleDecision::Allow)) {
            "yes"
        } else {
            "no"
        };
        assert!(
            t.wire().iter().any(
                |m| m["id"] == "permission-1" && m["result"]["outcome"]["optionId"] == expected
            )
        );
        assert!(
            t.agent
                .pending_permissions()
                .await
                .expect("list")
                .is_empty()
        );
        t.finish().await;
    }
}

#[tokio::test]
async fn cancel_is_a_notification_and_turn_completes() {
    let mut t = Test::new(Default::default()).await;
    let id = t.send("hold").await;
    t.started(id).await;
    t.agent.cancel().await.expect("cancel");
    t.consumed(id).await;
    t.event(|e| {
        matches!(
            e,
            Event::TurnEnded {
                outcome: TurnOutcome::Cancelled,
                ..
            }
        )
    })
    .await;
    assert!(
        t.wire()
            .iter()
            .any(|m| m["method"] == "session/cancel" && m.get("id").is_none())
    );
    t.finish().await;
}

#[tokio::test]
async fn startup_settles_old_incarnation_and_duplicate_spawn_is_rejected() {
    let mut t = Test::new(Default::default()).await;
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
    assert!(
        t.broker
            .spawn(spec(t.root.path(), "claude", Default::default()))
            .await
            .is_err()
    );
    let second_broker = Broker::new(
        Arc::clone(&t.mailroom),
        vec![kind(t.root.path(), false)],
        config(),
    )
    .expect("broker");
    assert!(
        second_broker
            .spawn(spec(t.root.path(), "claude", Default::default()))
            .await
            .is_err()
    );
    drop(second_broker);
    t.agent.stop().await.expect("stop");
    let unstarted = t.send("unstarted").await;
    let started = t.send("started").await;
    t.mailroom
        .mark_offered(&[unstarted, started], Incarnation(1))
        .await
        .expect("offer");
    t.mailroom
        .mark_started(&[started], Incarnation(1))
        .await
        .expect("start");
    t.agent = t
        .broker
        .spawn(spec(t.root.path(), "claude", Default::default()))
        .await
        .expect("respawn");
    t.consumed(unstarted).await;
    assert_eq!(
        t.mailroom.delivery(started).await.expect("delivery").state,
        DeliveryState::Uncertain {
            incarnation: Incarnation(1)
        }
    );
    assert!(t.wire().iter().any(|m| m["method"] == "session/load"));
    t.finish().await;
}

#[tokio::test]
async fn database_reopen_recovers_prior_offers_and_resume_token() {
    let mut t = Test::new(Default::default()).await;
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
    t.agent.stop().await.expect("stop");
    let queued = t.send("recover me").await;
    let uncertain = t.send("hold for human").await;
    t.mailroom
        .mark_offered(&[queued, uncertain], Incarnation(1))
        .await
        .expect("offer");
    t.mailroom
        .mark_started(&[uncertain], Incarnation(1))
        .await
        .expect("started");
    drop(t.agent);
    drop(t.events);
    drop(t.broker);
    Arc::try_unwrap(t.mailroom)
        .ok()
        .expect("sole owner")
        .close()
        .await
        .expect("close");
    let mut reopened = Test::at(t.root, Default::default(), config(), false, None).await;
    reopened.consumed(queued).await;
    assert!(matches!(
        reopened
            .mailroom
            .delivery(uncertain)
            .await
            .expect("delivery")
            .state,
        DeliveryState::Uncertain { .. }
    ));
    let wire = reopened.wire();
    assert_eq!(
        wire.iter().filter(|m| m["method"] == "session/new").count(),
        1
    );
    assert_eq!(
        wire.iter()
            .filter(|m| m["method"] == "session/load")
            .count(),
        1
    );
    reopened.finish().await;
}

#[cfg(unix)]
#[tokio::test]
async fn permission_from_dead_incarnation_cannot_answer_replacement() {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    let mut t = Test::new(Default::default()).await;
    let old = t.send("permission").await;
    let request = t.pending().await;
    let next = t.send("permission again").await;
    let pid = t
        .wire()
        .iter()
        .find_map(|m| m["pid"].as_i64())
        .expect("pid");
    kill(
        Pid::from_raw(i32::try_from(pid).expect("pid range")),
        Signal::SIGKILL,
    )
    .expect("kill fixture");
    let replacement = t.pending().await;
    assert!(replacement.incarnation > request.incarnation);
    assert!(matches!(
        t.agent
            .answer_permission(
                request.id,
                request.incarnation,
                PermissionAnswer::Choose("yes".into())
            )
            .await,
        Err(Error::StalePermission)
    ));
    assert!(matches!(
        t.mailroom.delivery(old).await.expect("delivery").state,
        DeliveryState::Uncertain { .. }
    ));
    t.agent
        .answer_permission(
            replacement.id,
            replacement.incarnation,
            PermissionAnswer::Choose("no".into()),
        )
        .await
        .expect("answer current");
    t.consumed(next).await;
    t.finish().await;
}

#[tokio::test]
async fn unsupported_resume_is_recovery_and_nonresumable_agents_do_not_suspend() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("no-resume"), "").expect("flag");
    let mut t = Test::at(
        root,
        Default::default(),
        config(),
        false,
        Some(Duration::from_millis(10)),
    )
    .await;
    let id = t.send("first").await;
    t.consumed(id).await;
    // Waiting beyond the configured idle interval is the behavior being tested.
    tokio::time::sleep(Duration::from_millis(35)).await;
    assert_eq!(t.agent.phase(), Phase::Idle);
    t.agent.stop().await.expect("stop");
    t.agent = t
        .broker
        .spawn(spec(t.root.path(), "claude", Default::default()))
        .await
        .expect("spawn");
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
    assert_eq!(
        t.wire()
            .iter()
            .filter(|m| m["method"] == "session/new")
            .count(),
        1
    );
    t.finish().await;
}

async fn live(env: &str, codex: bool) {
    let command = std::env::var(env).expect("set shim path environment variable");
    let root = tempfile::tempdir().expect("tempdir");
    let mailroom = Arc::new(
        Mailroom::open(root.path().join("mail.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let mut kind = if codex {
        KindConfig::codex(command)
    } else {
        KindConfig::claude(command)
    };
    kind.idle_after = Some(Duration::from_millis(100));
    kind.request_timeout = Duration::from_secs(120);
    let name = kind.name.clone();
    let broker =
        Broker::new(Arc::clone(&mailroom), vec![kind], Default::default()).expect("broker");
    let mut events = broker.events();
    let agent = broker
        .spawn(spec(root.path(), &name, Default::default()))
        .await
        .expect("spawn");
    for text in [
        "Remember the secret fruit is pomegranate. Reply with only pomegranate. Do not use tools.",
        "What is the secret fruit I gave you? Reply with only the fruit. Do not use tools.",
    ] {
        let id = broker
            .send(NewMessage {
                from: Sender::Human,
                to: agent.id,
                body: Body::new(text).expect("body"),
                reply_to: None,
                client_key: ClientKey(ulid::Ulid::new().to_string()),
                act: Act::Inform,
                task: None,
            })
            .await
            .expect("send")
            .1
            .id();
        let result = tokio::time::timeout(Duration::from_secs(180), async {
            let mut output = String::new();
            let mut consumed = false;
            loop {
                match events.recv().await.expect("event") {
                    Event::Activity { text, .. } => output.push_str(&text),
                    Event::Consumed { ids, .. } if ids.contains(&id) => {
                        consumed = true;
                        assert!(
                            output.to_lowercase().contains("pomegranate"),
                            "unexpected output {output}"
                        );
                        assert_eq!(
                            mailroom.delivery(id).await.expect("delivery").state,
                            DeliveryState::Consumed
                        );
                    }
                    Event::PhaseChanged {
                        phase: Phase::Dormant,
                        ..
                    } => {
                        assert!(consumed, "session suspended without consuming live mail");
                        break;
                    }
                    Event::PhaseChanged {
                        phase: Phase::NeedsRecovery { reason } | Phase::Failed { last: reason },
                        ..
                    } => panic!("live failure {reason}"),
                    Event::PermissionPending(r) => panic!("unexpected permission {}", r.title),
                    Event::TurnEnded { outcome, .. } => assert_eq!(outcome, TurnOutcome::Completed),
                    _ => {}
                }
            }
        })
        .await;
        if result.is_err() {
            broker.shutdown().await.expect("shutdown after timeout");
            panic!("live shim timed out");
        }
    }
    broker.shutdown().await.expect("shutdown");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("sole owner")
        .close()
        .await
        .expect("close");
}

#[tokio::test]
#[ignore = "requires logged-in Claude shim at AGENTD_CLAUDE_ACP"]
async fn live_claude_acp() {
    live("AGENTD_CLAUDE_ACP", false).await;
}

#[tokio::test]
#[ignore = "requires logged-in Codex shim at AGENTD_CODEX_ACP"]
async fn live_codex_acp() {
    live("AGENTD_CODEX_ACP", true).await;
}

#[tokio::test]
async fn cancel_answers_permissions_arriving_after_cancel() {
    for allow in [false, true] {
        let policy = PermissionPolicy {
            rules: if allow {
                vec![PermissionRule {
                    tool: "edit".into(),
                    decision: RuleDecision::Allow,
                }]
            } else {
                vec![]
            },
            ..Default::default()
        };
        let mut t = Test::new(policy).await;
        let id = t.send("late-permissions").await;
        t.started(id).await;
        t.agent.cancel().await.expect("cancel");
        match t
            .event(|e| {
                matches!(
                    e,
                    Event::PermissionPending(_)
                        | Event::TurnEnded { .. }
                        | Event::PhaseChanged {
                            phase: Phase::Crashed { .. } | Phase::Failed { .. },
                            ..
                        }
                )
            })
            .await
        {
            Event::TurnEnded { outcome, .. } => {
                assert_eq!(outcome, TurnOutcome::Cancelled);
            }
            event => panic!("permission escaped cancellation: {event:?}"),
        }
        assert!(
            t.agent
                .pending_permissions()
                .await
                .expect("pending")
                .is_empty()
        );
        for wire_id in ["permission-late-1", "permission-late-2"] {
            assert!(
                t.wire().iter().any(|m| m["id"] == wire_id
                    && m["result"]["outcome"] == json!({"outcome":"cancelled"}))
            );
        }
        assert_eq!(
            t.mailroom.delivery(id).await.expect("delivery").state,
            DeliveryState::Consumed
        );
        let next = t.send("permission").await;
        if !allow {
            let request = t.pending().await;
            t.agent
                .answer_permission(
                    request.id,
                    request.incarnation,
                    PermissionAnswer::Choose("yes".into()),
                )
                .await
                .expect("answer next turn");
        }
        t.consumed(next).await;
        t.finish().await;
    }
}

async fn tail_update_crash(update: Value, started: bool) {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join("tail-update.json"), update.to_string()).expect("tail update");
    let mut t = Test::at(
        root,
        Default::default(),
        Config {
            crash_limit: 1,
            ..config()
        },
        false,
        None,
    )
    .await;
    let first = t.send("hold").await;
    t.started(first).await;
    let next = t.send("die-before").await;
    t.release();
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
    let state = t.mailroom.delivery(next).await.expect("delivery").state;
    if started {
        assert!(
            matches!(state, DeliveryState::Uncertain { .. }),
            "{update}: {state:?}"
        );
    } else {
        assert_eq!(
            state,
            DeliveryState::Queued,
            "metadata must not start a turn: {update}"
        );
    }
    assert_eq!(
        t.mailroom.delivery(first).await.expect("delivery").state,
        DeliveryState::Consumed
    );
    t.finish().await;
}

#[tokio::test]
async fn session_metadata_does_not_start_next_turn() {
    for update in [
        json!({"sessionUpdate":"available_commands_update","availableCommands":[]}),
        json!({"sessionUpdate":"current_mode_update","currentModeId":"default"}),
        json!({"sessionUpdate":"config_option_update","configOptions":[]}),
        json!({"sessionUpdate":"session_info_update","title":"Previous turn"}),
        json!({"sessionUpdate":"usage_update","used":10,"size":100}),
        json!({"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"echo"}}),
    ] {
        tail_update_crash(update, false).await;
    }
}

#[tokio::test]
async fn execution_updates_start_turn_before_crash() {
    for update in [
        json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"reply"}}),
        json!({"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking"}}),
        json!({"sessionUpdate":"plan","entries":[]}),
        json!({"sessionUpdate":"tool_call","toolCallId":"call","title":"Read file","kind":"read","status":"pending"}),
        json!({"sessionUpdate":"tool_call_update","toolCallId":"call","status":"completed"}),
    ] {
        tail_update_crash(update, true).await;
    }
}

#[tokio::test]
async fn simultaneous_permission_timeouts_cancel_without_failing_actor() {
    let mut t = Test::new(PermissionPolicy {
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    })
    .await;
    let id = t.send("two-permissions").await;
    let first = t.pending().await;
    let second = t.pending().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    let ended = t
        .event(|e| {
            matches!(
                e,
                Event::TurnEnded { .. }
                    | Event::PhaseChanged {
                        phase: Phase::Failed { .. },
                        ..
                    }
            )
        })
        .await;
    assert!(
        matches!(
            ended,
            Event::TurnEnded {
                outcome: TurnOutcome::Cancelled,
                ..
            }
        ),
        "expired requests must cancel without failing: {ended:?}"
    );
    assert_eq!(
        t.agent.phase(),
        Phase::Idle,
        "expired requests must not fail the actor"
    );
    assert_eq!(
        t.mailroom.delivery(id).await.expect("delivery").state,
        DeliveryState::Consumed
    );
    for request in [first, second] {
        assert!(matches!(
            t.agent
                .answer_permission(
                    request.id,
                    request.incarnation,
                    PermissionAnswer::Choose("yes".into())
                )
                .await,
            Err(Error::StalePermission)
        ));
    }
    assert!(
        t.agent
            .pending_permissions()
            .await
            .expect("pending")
            .is_empty()
    );
    let next = t.send("response-only").await;
    t.consumed(next).await;
    t.finish().await;
}

#[tokio::test]
async fn ignored_cancel_crashes_and_settles_delivery() {
    for (body, started) in [("ignore-cancel silent", false), ("ignore-cancel", true)] {
        let mut t = Test::at(
            tempfile::tempdir().expect("tempdir"),
            Default::default(),
            Config {
                crash_limit: 1,
                ..config()
            },
            false,
            None,
        )
        .await;
        let id = t.send(body).await;
        if started {
            t.started(id).await;
        } else {
            t.event(|e| matches!(e, Event::Offered { .. })).await;
        }
        t.agent.cancel().await.expect("cancel");
        let queued = t.send("response-only").await;
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
        assert_eq!(
            t.wire()
                .iter()
                .filter(|m| m["method"] == "session/prompt")
                .count(),
            1,
            "mail must wait while cancelling"
        );
        assert_eq!(
            t.mailroom.delivery(queued).await.expect("delivery").state,
            DeliveryState::Queued
        );
        let state = t.mailroom.delivery(id).await.expect("delivery").state;
        assert!(
            if started {
                matches!(state, DeliveryState::Uncertain { .. })
            } else {
                state == DeliveryState::Queued
            },
            "cancel timeout: {state:?}"
        );
        t.finish().await;
    }
}
