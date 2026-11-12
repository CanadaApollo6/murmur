//! The verifier runner in process, with fake ACP agents and no sandbox. The
//! daemon refuses an unsandboxed verifier kind; these tests build the
//! runner's settings directly so they can run anywhere.
#![cfg(unix)]
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agentd::KindConfig;
use agentd::mailroom::{Mailroom, NewMessage, SendOutcome, Verification, VerificationState};
use agentd::model::*;
use agentd::supervisor::{AgentSpec, Broker, Config};
use agentd::swarm::{Act, Budget, Evidence, Finding, Outcome, Stakes, TaskId, TaskState};
use agentd::verify::{Runner, Settings};
use serde_json::Value;
use tempfile::TempDir;

fn kind(root: &Path, name: &str, family: &str) -> KindConfig {
    let bin = env!("CARGO_BIN_EXE_agentd-fake-acp");
    let mut kind = if name == "codex" {
        KindConfig::codex(bin)
    } else {
        KindConfig::claude(bin)
    };
    kind.isolation = agentd::sandbox::Isolation::None;
    kind.family = Some(family.into());
    let wire = root.join(format!("wire-{name}"));
    std::fs::create_dir_all(&wire).expect("wire dir");
    kind.env
        .insert("FAKE_ROOT".into(), wire.display().to_string());
    // A claimant runs read-only; a verifier runs with workspace write.
    kind.env.insert(
        "FAKE_MODE".into(),
        if name == "codex" {
            "workspace-write"
        } else {
            "default"
        }
        .into(),
    );
    kind.mcp_servers =
        vec![serde_json::json!({"name":"extra","command":"unused","args":[],"env":[]})];
    kind.idle_after = None;
    kind.request_timeout = Duration::from_secs(3);
    kind
}

fn settings(verifiers: &[&str], max_running: usize) -> Settings {
    Settings {
        kinds: verifiers.iter().map(|k| (*k).to_owned()).collect(),
        check_done: Some(Stakes::Normal),
        families: BTreeMap::from([
            ("claude".into(), Some("anthropic".into())),
            ("codex".into(), Some("openai".into())),
        ]),
        budget: Budget {
            tokens: None,
            wall_secs: 900,
            messages: 1,
            hops: 0,
        },
        max_running,
        max_snapshot_bytes: 1 << 20,
        max_snapshot_entries: 100_000,
    }
}

struct Rig {
    root: TempDir,
    mailroom: Arc<Mailroom>,
    broker: Arc<Broker>,
    runner: Option<tokio::task::JoinHandle<()>>,
}

impl Rig {
    /// `verifiers` empty means no runner, as without `[verify]`.
    async fn new(verifiers: &[&str], max_running: usize) -> Self {
        let root = tempfile::tempdir().expect("root");
        std::fs::create_dir(root.path().join("state")).expect("state");
        Self::open(root, verifiers, max_running, true).await
    }
    async fn open(root: TempDir, verifiers: &[&str], max_running: usize, run: bool) -> Self {
        let mailroom = Arc::new(
            Mailroom::open(
                root.path().join("state/mail.db"),
                agentd::mailroom::Config {
                    verifier_available: !verifiers.is_empty(),
                    check_done: Some(Stakes::Normal),
                    ..Default::default()
                },
            )
            .await
            .expect("mailroom"),
        );
        let kinds = vec![
            kind(root.path(), "claude", "anthropic"),
            kind(root.path(), "codex", "openai"),
        ];
        let mut broker = Broker::new(
            Arc::clone(&mailroom),
            kinds,
            Config {
                backoff_initial: Duration::from_millis(20),
                backoff_max: Duration::from_millis(60),
                ..Config::default()
            },
        )
        .expect("broker");
        if !verifiers.is_empty() {
            broker = broker.with_verifier(
                Runner::new(settings(verifiers, max_running), &root.path().join("state"))
                    .expect("runner"),
            );
        }
        let broker = Arc::new(broker);
        let mut rig = Self {
            root,
            mailroom,
            broker,
            runner: None,
        };
        if run {
            rig.run();
        }
        rig
    }
    fn run(&mut self) {
        let broker = Arc::clone(&self.broker);
        self.runner = Some(tokio::spawn(
            async move { broker.run_verifications().await },
        ));
    }
    async fn stop_runner(&mut self) {
        if let Some(task) = self.runner.take() {
            task.abort();
            // Without a runner the loop has already returned.
            match task.await {
                Ok(()) => {}
                Err(e) => assert!(e.is_cancelled(), "{e}"),
            }
        }
    }
    async fn close(mut self) -> TempDir {
        self.stop_runner().await;
        self.broker.shutdown().await.expect("shutdown");
        drop(self.broker);
        // Actors drop their mailroom handles as they exit.
        let mut mailroom = self.mailroom;
        for _ in 0..500 {
            match Arc::try_unwrap(mailroom) {
                Ok(room) => {
                    room.close().await.expect("close");
                    return self.root;
                }
                Err(shared) => {
                    mailroom = shared;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        panic!("mailroom still shared after shutdown");
    }
    fn state(&self) -> PathBuf {
        self.root.path().join("state")
    }
    /// A claimant with a workspace of its own.
    async fn claimant(&self, name: &str) -> AgentId {
        let cwd = self.root.path().join(name);
        std::fs::create_dir_all(cwd.join("src")).expect("workspace");
        std::fs::write(cwd.join("src/lib.rs"), "fn main() {}").expect("file");
        std::fs::write(cwd.join("CLAUDE.md"), "claimant notes").expect("file");
        std::fs::create_dir_all(cwd.join(".git")).expect("git");
        std::fs::write(cwd.join(".git/HEAD"), "ref").expect("git head");
        self.broker
            .spawn(AgentSpec {
                name: AgentName::parse(name).expect("name"),
                kind: "claude".into(),
                cwd,
                workspace: None,
                model: None,
                permissions: Default::default(),
                idle_after: None,
            })
            .await
            .expect("spawn")
            .id
    }
    async fn send(&self, from: Sender, to: AgentId, act: Act) -> Envelope {
        self.broker
            .send(NewMessage {
                from,
                to,
                body: Body::new("text").expect("body"),
                reply_to: None,
                client_key: ClientKey(ulid::Ulid::new().to_string()),
                act,
                task: None,
            })
            .await
            .expect("send")
            .1
    }
    /// The operator asks `agent` for something; it answers `infeasible`.
    async fn claim(&self, agent: AgentId) -> (TaskId, MessageId) {
        self.report(
            agent,
            Stakes::Normal,
            Outcome::Infeasible {
                evidence: Evidence {
                    tried: "everything".into(),
                    obstacle: "a wall".into(),
                    reproduction: None,
                },
            },
        )
        .await
    }
    async fn report(
        &self,
        agent: AgentId,
        stakes: Stakes,
        outcome: Outcome,
    ) -> (TaskId, MessageId) {
        let request = self
            .send(
                Sender::Human,
                agent,
                Act::Request {
                    deliverable: "a passing test".into(),
                    budget: Budget {
                        tokens: None,
                        wall_secs: 600,
                        messages: 5,
                        hops: 1,
                    },
                    stakes,
                },
            )
            .await;
        let task = request.task().expect("task");
        let report = self
            .send(
                Sender::Agent(agent),
                agent,
                Act::Report {
                    task,
                    outcome,
                    verdict: None,
                },
            )
            .await;
        (task, report.id())
    }
    async fn verdict(&self, v: &Verification) {
        let verifier = v.verifier.expect("verifier");
        self.send(
            Sender::Agent(verifier),
            verifier,
            Act::Report {
                task: v.task.expect("verification task"),
                outcome: Outcome::Done { artifacts: vec![] },
                verdict: Some(agentd::swarm::VerifierVerdict::Confirmed {
                    tried: "the same wall".into(),
                }),
            },
        )
        .await;
    }
    async fn until<T>(&self, what: &str, mut check: impl AsyncFnMut() -> Option<T>) -> T {
        for _ in 0..1000 {
            if let Some(value) = check().await {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }
    async fn row(&self, report: MessageId) -> Verification {
        self.mailroom.verification(report).await.expect("row")
    }
    async fn running(&self, report: MessageId) -> Verification {
        self.until("a running verification", async || {
            let v = self.row(report).await;
            (v.state == VerificationState::Running).then_some(v)
        })
        .await
    }
    async fn task_state(&self, task: TaskId) -> TaskState {
        self.mailroom.task(task).await.expect("task").state
    }
    /// The processes the verifier kind started, as their first wire lines.
    fn verifier_processes(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("wire-codex/wire.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|v| v.get("pid").is_some())
            .collect()
    }
    async fn retired_and_gone(&self, verifier: AgentId, report: MessageId) {
        let dir = self.state().join("verify").join(report.to_string());
        self.until(
            "the verifier retired and its snapshot deleted",
            async || {
                (self.mailroom.retired(verifier).await.expect("retired") && !dir.exists())
                    .then_some(())
            },
        )
        .await;
    }
}

#[tokio::test]
async fn a_confirmed_claim_closes_and_the_verifier_is_cleaned_up() {
    let rig = Rig::new(&["claude", "codex"], 2).await;
    let a = rig.claimant("a").await;
    let (task, report) = rig.claim(a).await;
    let v = rig.running(report).await;
    assert_eq!(v.verifier_family.as_deref(), Some("openai"));
    assert_eq!(v.claimant_family.as_deref(), Some("anthropic"));
    assert_eq!(
        v.snapshot_clean,
        Some(true),
        "taken before the send returned"
    );
    let snapshot = rig
        .state()
        .join("verify")
        .join(report.to_string())
        .join("workspace");
    // The verifier runs in the snapshot, with the kind of the other family.
    let process = rig
        .until("the verifier process", async || {
            rig.verifier_processes().into_iter().next()
        })
        .await;
    // The process reports its directory with symlinks resolved.
    assert_eq!(
        process["cwd"],
        snapshot
            .canonicalize()
            .expect("snapshot")
            .display()
            .to_string()
    );
    assert!(snapshot.join("src/lib.rs").exists());
    assert!(snapshot.join("CLAUDE.md.claimant-copy").exists());
    assert!(!snapshot.join("CLAUDE.md").exists());
    assert!(!snapshot.join(".git").exists());
    let spec = rig
        .mailroom
        .agent_spec(v.verifier.expect("verifier"))
        .await
        .expect("spec")
        .expect("saved spec");
    assert_eq!(spec.kind, "codex");
    assert!(matches!(
        spec.permissions.sandbox,
        SandboxLevel::WorkspaceWrite
    ));
    // The kind's extra MCP server reaches the claimant but not the verifier.
    // This broker has no MCP server of its own.
    let servers = |wire: &str| -> Option<Vec<String>> {
        let log = rig.root.path().join(wire).join("wire.jsonl");
        let first = std::fs::read_to_string(log)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .find(|v| v["method"] == "session/new")?;
        Some(
            first["params"]["mcpServers"]
                .as_array()
                .expect("servers")
                .iter()
                .map(|s| s["name"].as_str().expect("name").to_owned())
                .collect(),
        )
    };
    let verifier_servers = rig
        .until("the verifier's session", async || servers("wire-codex"))
        .await;
    assert!(verifier_servers.is_empty(), "{verifier_servers:?}");
    assert_eq!(servers("wire-claude").expect("claimant session"), ["extra"]);
    assert_eq!(
        spec.name.as_str(),
        format!("v-{}", report.to_string().to_ascii_lowercase())
    );
    // The brief reached the verifier.
    rig.until("the brief", async || {
        rig.root
            .path()
            .join("wire-codex/prompt-1")
            .exists()
            .then_some(())
    })
    .await;
    rig.verdict(&v).await;
    let closed = rig
        .until("the claim to close", async || {
            match rig.task_state(task).await {
                state @ TaskState::Closed { .. } => Some(state),
                _ => None,
            }
        })
        .await;
    assert!(matches!(
        closed,
        TaskState::Closed {
            outcome: Outcome::Infeasible { .. },
            success: true
        }
    ));
    rig.retired_and_gone(v.verifier.expect("verifier"), report)
        .await;
    rig.close().await;
}

#[tokio::test]
async fn done_checks_use_a_clean_snapshot_and_skip_low_stakes() {
    let rig = Rig::new(&["claude", "codex"], 2).await;
    let a = rig.claimant("a").await;
    let (task, report) = rig
        .report(a, Stakes::Normal, Outcome::Done { artifacts: vec![] })
        .await;
    let v = rig.running(report).await;
    assert_eq!(rig.task_state(task).await, TaskState::Verifying { report });
    assert_eq!(v.verifier_family.as_deref(), Some("openai"));
    assert_eq!(v.claimant_family.as_deref(), Some("anthropic"));
    assert_eq!(v.snapshot_clean, Some(true));
    let snapshot = rig
        .state()
        .join("verify")
        .join(report.to_string())
        .join("workspace");
    assert!(snapshot.join("src/lib.rs").exists());
    assert!(!snapshot.join(".git").exists());
    let process = rig
        .until("the verifier in the snapshot", async || {
            rig.verifier_processes().into_iter().next()
        })
        .await;
    assert_eq!(
        process["cwd"],
        snapshot
            .canonicalize()
            .expect("snapshot")
            .display()
            .to_string()
    );
    rig.verdict(&v).await;
    assert_eq!(
        rig.task_state(task).await,
        TaskState::Closed {
            outcome: Outcome::Done { artifacts: vec![] },
            success: true
        }
    );
    let vtask = v.task.expect("verification task");
    assert_eq!(
        rig.task_state(vtask).await,
        TaskState::Closed {
            outcome: Outcome::Done { artifacts: vec![] },
            success: true
        }
    );
    assert_eq!(rig.row(report).await.state, VerificationState::Settled);
    rig.retired_and_gone(v.verifier.expect("verifier"), report)
        .await;

    let (low, low_report) = rig
        .report(a, Stakes::Low, Outcome::Done { artifacts: vec![] })
        .await;
    assert_eq!(
        rig.task_state(low).await,
        TaskState::Closed {
            outcome: Outcome::Done { artifacts: vec![] },
            success: true
        }
    );
    assert!(matches!(
        rig.mailroom.verification(low_report).await,
        Err(agentd::mailroom::Error::NotFound(_))
    ));
    assert!(
        !rig.state()
            .join("verify")
            .join(low_report.to_string())
            .exists()
    );
    assert!(
        rig.mailroom
            .pending_verifications()
            .await
            .expect("pending")
            .is_empty()
    );
    assert_eq!(rig.verifier_processes().len(), 1);
    rig.close().await;
}

#[tokio::test]
async fn a_claim_with_no_other_family_goes_to_review_unverified() {
    let rig = Rig::new(&["claude"], 2).await;
    let a = rig.claimant("a").await;
    let (task, report) = rig.claim(a).await;
    let state = rig
        .until("review", async || match rig.task_state(task).await {
            state @ TaskState::Review { .. } => Some(state),
            _ => None,
        })
        .await;
    let TaskState::Review {
        finding: Finding::Unverified { reason },
        ..
    } = state
    else {
        panic!("{state:?}");
    };
    assert_eq!(
        reason,
        "no verifier kind has a family other than the claimant's family anthropic"
    );
    assert_eq!(rig.row(report).await.verifier, None);
    assert!(!rig.state().join("verify").join(report.to_string()).exists());
    assert!(rig.verifier_processes().is_empty());
    rig.close().await;
}

#[tokio::test]
async fn at_most_max_running_verifiers_run_at_once() {
    let rig = Rig::new(&["codex"], 1).await;
    let a = rig.claimant("a").await;
    let b = rig.claimant("b").await;
    let (_, first) = rig.claim(a).await;
    let (second_task, second) = rig.claim(b).await;
    let v = rig.running(first).await;
    // Several passes run; the second claim stays pending.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(rig.row(second).await.state, VerificationState::Pending);
    assert!(matches!(
        rig.task_state(second_task).await,
        TaskState::Verifying { .. }
    ));
    rig.verdict(&v).await;
    rig.running(second).await;
    rig.close().await;
}

#[tokio::test]
async fn a_claim_whose_verifier_fails_to_start_three_times_is_settled() {
    let rig = Rig::new(&["codex"], 2).await;
    let a = rig.claimant("a").await;
    // Nothing can be copied from a workspace that is gone.
    std::fs::remove_dir_all(rig.root.path().join("a")).expect("remove workspace");
    let (task, report) = rig.claim(a).await;
    let state = rig
        .until("review", async || match rig.task_state(task).await {
            state @ TaskState::Review { .. } => Some(state),
            _ => None,
        })
        .await;
    let TaskState::Review {
        finding: Finding::Unverified { reason },
        ..
    } = state
    else {
        panic!("{state:?}");
    };
    assert!(
        reason.starts_with("the verifier did not start after 3 attempts: ")
            && reason.contains("No such file or directory"),
        "{reason}"
    );
    assert_eq!(rig.row(report).await.verifier, None);
    assert!(!rig.state().join("verify").join(report.to_string()).exists());
    rig.close().await;
}

#[tokio::test]
async fn a_running_verifier_is_started_again_after_a_restart() {
    let rig = Rig::new(&["codex"], 2).await;
    let a = rig.claimant("a").await;
    let (task, report) = rig.claim(a).await;
    rig.running(report).await;
    rig.until("the first verifier process", async || {
        (rig.verifier_processes().len() == 1).then_some(())
    })
    .await;
    let root = rig.close().await;
    // The new daemon run restores no spec by hand; the runner restarts it.
    let rig = Rig::open(root, &["codex"], 2, true).await;
    rig.until("the second verifier process", async || {
        (rig.verifier_processes().len() == 2).then_some(())
    })
    .await;
    let v = rig.row(report).await;
    assert_eq!(v.state, VerificationState::Running);
    rig.verdict(&v).await;
    rig.until("the claim to close", async || {
        matches!(rig.task_state(task).await, TaskState::Closed { .. }).then_some(())
    })
    .await;
    rig.retired_and_gone(v.verifier.expect("verifier"), report)
        .await;
    rig.close().await;
}

#[tokio::test]
async fn a_settled_verifier_left_unretired_is_retired_after_a_restart() {
    let mut rig = Rig::new(&["codex"], 2).await;
    let a = rig.claimant("a").await;
    let (task, report) = rig.claim(a).await;
    let v = rig.running(report).await;
    // The runner stops before it can clean up, as in a crash.
    rig.stop_runner().await;
    rig.verdict(&v).await;
    assert!(matches!(
        rig.task_state(task).await,
        TaskState::Closed { .. }
    ));
    let verifier = v.verifier.expect("verifier");
    assert!(!rig.mailroom.retired(verifier).await.expect("retired"));
    let dir = rig.state().join("verify").join(report.to_string());
    assert!(dir.exists());
    let root = rig.close().await;
    let rig = Rig::open(root, &["codex"], 2, true).await;
    rig.retired_and_gone(verifier, report).await;
    rig.close().await;
}

#[tokio::test]
async fn a_claim_left_pending_by_a_restart_gets_a_snapshot_that_is_not_clean() {
    // The report is accepted while no runner runs, as just before a crash.
    let root = tempfile::tempdir().expect("root");
    std::fs::create_dir(root.path().join("state")).expect("state");
    let rig = Rig::open(root, &["codex"], 2, false).await;
    let a = rig.claimant("a").await;
    let request = rig
        .send(
            Sender::Human,
            a,
            Act::Request {
                deliverable: "a passing test".into(),
                budget: Budget {
                    tokens: None,
                    wall_secs: 600,
                    messages: 5,
                    hops: 1,
                },
                stakes: Stakes::Normal,
            },
        )
        .await;
    let task = request.task().expect("task");
    // Bypass the broker: no snapshot is taken at report time.
    let (_, report) = rig
        .mailroom
        .accept(NewMessage {
            from: Sender::Agent(a),
            to: a,
            body: Body::new("text").expect("body"),
            reply_to: None,
            client_key: ClientKey("claim".into()),
            act: Act::Report {
                task,
                outcome: Outcome::Infeasible {
                    evidence: Evidence {
                        tried: "everything".into(),
                        obstacle: "a wall".into(),
                        reproduction: None,
                    },
                },
                verdict: None,
            },
            task: None,
        })
        .await
        .expect("report");
    let root = rig.close().await;
    let rig = Rig::open(root, &["codex"], 2, true).await;
    let v = rig.running(report.id()).await;
    assert_eq!(v.snapshot_clean, Some(false));
    rig.verdict(&v).await;
    // A confirmation from a snapshot that is not clean goes to the operator.
    rig.until("review", async || {
        matches!(
            rig.task_state(task).await,
            TaskState::Review {
                finding: Finding::Confirmed,
                ..
            }
        )
        .then_some(())
    })
    .await;
    rig.close().await;
}

#[tokio::test]
async fn a_retried_report_returns_only_after_the_snapshot_is_complete() {
    let root = tempfile::tempdir().expect("root");
    std::fs::create_dir(root.path().join("state")).expect("state");
    // No reconcile loop: only the path that accepts the report copies.
    let rig = Rig::open(root, &["codex"], 2, false).await;
    let a = rig.claimant("a").await;
    // Enough entries that the copy outlasts one more send.
    let many = rig.root.path().join("a/many");
    std::fs::create_dir(&many).expect("many");
    for n in 0..20_000 {
        std::fs::write(many.join(n.to_string()), "").expect("empty");
    }
    let request = rig
        .send(
            Sender::Human,
            a,
            Act::Request {
                deliverable: "a passing test".into(),
                budget: Budget {
                    tokens: None,
                    wall_secs: 600,
                    messages: 5,
                    hops: 1,
                },
                stakes: Stakes::Normal,
            },
        )
        .await;
    let task = request.task().expect("task");
    let report = || NewMessage {
        from: Sender::Agent(a),
        to: a,
        body: Body::new("text").expect("body"),
        reply_to: None,
        client_key: ClientKey("claim".into()),
        act: Act::Report {
            task,
            outcome: Outcome::Infeasible {
                evidence: Evidence {
                    tried: "everything".into(),
                    obstacle: "a wall".into(),
                    reproduction: None,
                },
            },
            verdict: None,
        },
        task: None,
    };
    let first = tokio::spawn({
        let broker = Arc::clone(&rig.broker);
        let message = report();
        async move { broker.send(message).await }
    });
    let id = loop {
        if let Some(v) = rig
            .mailroom
            .pending_verifications()
            .await
            .expect("pending")
            .first()
        {
            break v.report;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    let (outcome, _) = rig.broker.send(report()).await.expect("retry");
    assert!(matches!(outcome, SendOutcome::Duplicate { id: again } if again == id));
    let dir = agentd::verify::snapshot_dir(&rig.state(), id);
    assert_eq!(
        agentd::verify::read_marker(&dir).expect("marker"),
        Some(true),
        "the retry returned while the snapshot was still being copied"
    );
    let (outcome, _) = first.await.expect("join").expect("first send");
    assert!(matches!(
        outcome,
        SendOutcome::Queued { .. } | SendOutcome::Held { .. }
    ));
    rig.close().await;
}

#[tokio::test]
async fn without_verify_settings_a_claim_goes_to_review_and_nothing_is_copied() {
    let rig = Rig::new(&[], 2).await;
    let a = rig.claimant("a").await;
    let (task, _) = rig.claim(a).await;
    let TaskState::Review {
        finding: Finding::Unverified { reason },
        ..
    } = rig.task_state(task).await
    else {
        panic!("not in review");
    };
    assert_eq!(reason, "no verifier is available");
    assert!(!rig.state().join("verify").exists());
    rig.broker.run_verifications().await;
    assert!(rig.verifier_processes().is_empty());
    rig.close().await;
}

/// The daemon with real bwrap sandboxes: a claude claimant and a codex
/// verifier, both the fake ACP agent.
#[cfg(target_os = "linux")]
mod sandboxed {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::time::Duration;

    use serde_json::{Value, json};
    use tokio::process::{Child, Command};

    fn supported() -> bool {
        let output = std::process::Command::new("bwrap")
            .args([
                "--unshare-user",
                "--unshare-pid",
                "--unshare-net",
                "--ro-bind",
                "/",
                "/",
                "--proc",
                "/proc",
                "--",
                "/bin/true",
            ])
            .output();
        match output {
            Ok(out) if out.status.success() => true,
            other => {
                assert!(
                    std::env::var("AGENTD_REQUIRE_SANDBOX").as_deref() != Ok("1"),
                    "required real sandbox unavailable: {other:?}"
                );
                eprintln!("SKIP verifier e2e: bwrap unavailable: {other:?}");
                false
            }
        }
    }

    struct Daemon {
        root: tempfile::TempDir,
        child: Child,
    }

    impl Daemon {
        /// `codex_family` is the verifier kind's family.
        async fn start(codex_family: &str) -> Self {
            let root = tempfile::tempdir().expect("root");
            for dir in ["human", "a", "state"] {
                std::fs::create_dir(root.path().join(dir)).expect("dir");
            }
            std::fs::set_permissions(
                root.path().join("state"),
                std::fs::Permissions::from_mode(0o700),
            )
            .expect("private state");
            let a = root.path().join("a");
            std::fs::write(a.join("CLAUDE.md"), "claimant instructions").expect("file");
            std::fs::create_dir(a.join(".git")).expect("git");
            std::fs::write(a.join("notes.txt"), "the claimant's work").expect("file");
            std::os::unix::fs::symlink(root.path().join("human"), a.join("home-link"))
                .expect("link");
            let fake = env!("CARGO_BIN_EXE_agentd-fake-acp");
            let config = format!(
                r#"
sandbox = "bwrap"
[bwrap]
ro_binds = [{bin}]
[kind.claude]
command = [{fake}]
family = "anthropic"
mode = "default"
credentials = []
idle_after_secs = 0
[kind.claude.env]
FAKE_ROOT_FROM_CWD = "1"
FAKE_MODE = "default"
FAKE_PROBE = {probe}
[kind.codex]
command = [{fake}]
family = {codex_family}
credentials = []
idle_after_secs = 0
[kind.codex.env]
FAKE_ROOT_FROM_HOME = "1"
FAKE_MODE = "workspace-write"
[verify]
kinds = ["codex"]
"#,
                bin = json!(Path::new(fake).parent().expect("bin dir")),
                fake = json!(fake),
                probe = json!(root.path().join("state/verify")),
                codex_family = json!(codex_family),
            );
            std::fs::write(root.path().join("kinds.toml"), config).expect("config");
            let child = Command::new(env!("CARGO_BIN_EXE_agentd"))
                .env("HOME", root.path().join("human"))
                .env("RUST_LOG", "agentd=debug")
                .arg("--state-dir")
                .arg(root.path().join("state"))
                .arg("--config")
                .arg(root.path().join("kinds.toml"))
                .stdout(Stdio::null())
                .stderr(Stdio::from(
                    std::fs::File::create(root.path().join("daemon.log")).expect("log"),
                ))
                .kill_on_drop(true)
                .spawn()
                .expect("daemon");
            let daemon = Self { root, child };
            for _ in 0..500 {
                if daemon.socket().exists() {
                    return daemon;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("daemon did not listen: {}", daemon.log());
        }
        fn socket(&self) -> PathBuf {
            self.root.path().join("state/agentd.sock")
        }
        fn log(&self) -> String {
            std::fs::read_to_string(self.root.path().join("daemon.log")).unwrap_or_default()
        }
        async fn ctl(&self, args: &[&str]) -> Value {
            let output = Command::new(env!("CARGO_BIN_EXE_agentctl"))
                .arg("--socket")
                .arg(self.socket())
                .args(args)
                .output()
                .await
                .expect("agentctl");
            assert!(
                output.status.success(),
                "agentctl {args:?}: {}\n{}",
                String::from_utf8_lossy(&output.stderr),
                self.log()
            );
            serde_json::from_slice(&output.stdout)
                .unwrap_or_else(|_| json!(String::from_utf8_lossy(&output.stdout).trim()))
        }
        /// The operator asks `a` for a deliverable; the fake claims infeasible.
        async fn claim(&self) {
            let a = self.root.path().join("a");
            self.ctl(&[
                "spawn",
                "a",
                "--kind",
                "claude",
                "--cwd",
                a.to_str().expect("cwd"),
            ])
            .await;
            self.ctl(&[
                "send",
                "a",
                "fake-claim:infeasible",
                "--act",
                r#"{"kind":"request","deliverable":"a passing test","budget":{"wall_secs":600,"messages":5,"hops":1}}"#,
            ])
            .await;
        }
        /// The operator's task, once `done` accepts its state.
        async fn task(&self, done: impl Fn(&Value) -> bool) -> Value {
            for _ in 0..1500 {
                let tasks = self.ctl(&["tasks"]).await;
                if let Some(task) = tasks
                    .as_array()
                    .expect("tasks")
                    .iter()
                    .find(|t| t["requester"] == "human" || t["parent"].is_null())
                    && done(task)
                {
                    return task.clone();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!(
                "task never reached the expected state: {}\n{}",
                self.ctl(&["tasks"]).await,
                self.log()
            );
        }
        async fn until(&self, what: &str, check: impl Fn() -> bool) {
            for _ in 0..1500 {
                if check() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("timed out waiting for {what}\n{}", self.log());
        }
        fn verify_root(&self) -> PathBuf {
            self.root.path().join("state/verify")
        }
        fn snapshots(&self) -> usize {
            std::fs::read_dir(self.verify_root())
                .map(|d| d.count())
                .unwrap_or(0)
        }
        /// What the verifier wrote in its private home.
        fn verifier_seen(&self) -> Option<Value> {
            let agents = std::fs::read_dir(self.root.path().join("state/agents")).ok()?;
            agents.filter_map(Result::ok).find_map(|agent| {
                let bytes = std::fs::read(agent.path().join("home/verifier-seen.json")).ok()?;
                serde_json::from_slice(&bytes).ok()
            })
        }
        async fn stop(mut self) {
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(self.child.id().expect("pid") as i32),
                nix::sys::signal::Signal::SIGTERM,
            )
            .expect("signal");
            let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
                .await
                .expect("shutdown deadline")
                .expect("wait");
            assert!(status.success(), "daemon exit {status}: {}", self.log());
        }
    }

    #[tokio::test]
    async fn a_verifier_of_another_family_confirms_the_claim_in_a_snapshot() {
        if !supported() {
            return;
        }
        let d = Daemon::start("openai").await;
        let verify_mode = std::fs::metadata(d.verify_root()).expect("verify root");
        assert_eq!(verify_mode.permissions().mode() & 0o777, 0o700);
        d.claim().await;
        let a = d.root.path().join("a");
        d.until("the claimant's report", || {
            a.join("claim-result.json").exists()
        })
        .await;
        let claim: Value = serde_json::from_slice(
            &std::fs::read(a.join("claim-result.json")).expect("claim result"),
        )
        .expect("claim JSON");
        assert_ne!(claim["result"]["isError"], json!(true), "{claim}");
        // The claimant's sandbox cannot see the verify root.
        d.until("the probe", || a.join("probe.txt").exists()).await;
        let probe = std::fs::read_to_string(a.join("probe.txt")).expect("probe");
        assert!(probe.starts_with("error: "), "{probe}");

        d.until("the verifier's listing", || d.verifier_seen().is_some())
            .await;
        let seen = d.verifier_seen().expect("seen");
        let cwd = seen["cwd"].as_str().expect("cwd");
        assert!(
            cwd.starts_with(d.verify_root().to_str().expect("root")) && cwd.ends_with("/workspace"),
            "{cwd}"
        );
        let files = seen["files"]
            .as_array()
            .expect("files")
            .iter()
            .map(|f| f.as_str().expect("file"))
            .collect::<Vec<_>>();
        assert!(files.contains(&"notes.txt"), "{files:?}");
        assert!(files.contains(&"CLAUDE.md.claimant-copy"), "{files:?}");
        assert!(!files.contains(&"CLAUDE.md"), "{files:?}");
        assert!(!files.iter().any(|f| f.starts_with(".git")), "{files:?}");
        // Taken before the claimant's send returned.
        assert!(
            !files.contains(&"after-report.txt"),
            "{files:?}\n{}",
            d.log()
        );
        assert!(
            files.iter().any(|f| f.starts_with("home-link -> ")),
            "{files:?}"
        );
        assert!(
            seen["brief"]
                .as_str()
                .expect("brief")
                .contains(agentd::swarm::WORKSPACE_RULE)
        );

        let task = d.task(|t| t["state"] == "closed").await;
        assert_eq!(task["outcome"]["kind"], "infeasible", "{task}");
        d.until("the snapshot to be deleted", || d.snapshots() == 0)
            .await;
        // Only the claimant is left; the verifier is retired.
        let ls = d.ctl(&["ls"]).await;
        assert!(!ls.to_string().contains("\"v-"), "{ls}");
        d.stop().await;
    }

    #[tokio::test]
    async fn without_another_family_the_claim_goes_to_review_unverified() {
        if !supported() {
            return;
        }
        let d = Daemon::start("anthropic").await;
        d.claim().await;
        let task = d.task(|t| t["state"] == "review").await;
        assert_eq!(task["finding"]["kind"], "unverified", "{task}");
        assert!(
            task["finding"]["reason"]
                .as_str()
                .expect("reason")
                .contains("no verifier kind has a family other than"),
            "{task}"
        );
        assert_eq!(d.snapshots(), 0);
        assert!(d.verifier_seen().is_none());
        d.stop().await;
    }
}
