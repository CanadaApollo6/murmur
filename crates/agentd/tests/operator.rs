//! `agentctl stop` and `agentctl restart` against the real supervisor, mailroom and fake ACP agent.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use agentd::KindConfig;
use agentd::mailroom::{Mailroom, NewMessage};
use agentd::model::*;
use agentd::supervisor::{AgentHandle, AgentSpec, Broker, Config, Event, Operation, Phase};
use agentd::swarm::Act;
use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::broadcast;

const CANCEL_TIMEOUT: Duration = Duration::from_millis(300);

fn kind(root: &Path, codex: bool) -> KindConfig {
    let bin = env!("CARGO_BIN_EXE_agentd-fake-acp");
    let mut kind = if codex {
        KindConfig::codex(bin)
    } else {
        KindConfig::claude(bin)
    };
    kind.isolation = agentd::sandbox::Isolation::None;
    kind.env
        .insert("FAKE_ROOT".into(), root.display().to_string());
    kind.env.insert(
        "FAKE_MODE".into(),
        if codex { "read-only" } else { "default" }.into(),
    );
    kind.idle_after = None;
    kind.request_timeout = Duration::from_secs(3);
    kind.cancel_timeout = CANCEL_TIMEOUT;
    kind
}
fn spec(root: &Path, name: &str, kind: &str, idle_after: Option<Duration>) -> AgentSpec {
    AgentSpec {
        workspace: None,
        name: AgentName::parse(name).expect("name"),
        kind: kind.into(),
        cwd: root.into(),
        model: None,
        permissions: Default::default(),
        idle_after,
    }
}
fn config() -> Config {
    Config {
        backoff_initial: Duration::from_millis(20),
        backoff_max: Duration::from_millis(60),
        ..Config::default()
    }
}
async fn open_mailroom(root: &Path) -> Arc<Mailroom> {
    Arc::new(
        Mailroom::open(root.join("mail.db"), Default::default())
            .await
            .expect("mailroom"),
    )
}

struct Test {
    root: TempDir,
    mailroom: Arc<Mailroom>,
    broker: Broker,
    events: broadcast::Receiver<Event>,
    agent: AgentHandle,
}
impl Test {
    async fn new(idle_after: Option<Duration>) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        let mailroom = open_mailroom(root.path()).await;
        let broker = Broker::new(
            Arc::clone(&mailroom),
            vec![kind(root.path(), false)],
            config(),
        )
        .expect("broker");
        let events = broker.events();
        let agent = broker
            .spawn(spec(root.path(), "worker", "claude", idle_after))
            .await
            .expect("spawn");
        let mut t = Self {
            root,
            mailroom,
            broker,
            events,
            agent,
        };
        t.phase(|p| *p == Phase::Idle).await;
        t
    }
    async fn send(&self, body: &str) -> MessageId {
        send(&self.broker, self.agent.id, body).await
    }
    async fn operate(&self, op: Operation) {
        tokio::time::timeout(CANCEL_TIMEOUT * 10, self.broker.operate(self.agent.id, op))
            .await
            .expect("operator command must finish within the cancel bound")
            .expect("operator command");
    }
    async fn event(&mut self, matches: impl Fn(&Event) -> bool) -> Event {
        next_event(&mut self.events, matches).await
    }
    async fn phase(&mut self, matches: impl Fn(&Phase) -> bool) {
        let id = self.agent.id;
        self.event(|e| matches!(e, Event::PhaseChanged { agent, phase, .. } if *agent == id && matches(phase)))
            .await;
    }
    async fn consumed(&mut self, id: MessageId) {
        self.event(|e| matches!(e, Event::Consumed { ids, .. } if ids.contains(&id)))
            .await;
        assert_eq!(self.state(id).await, DeliveryState::Consumed);
    }
    async fn state(&self, id: MessageId) -> DeliveryState {
        self.mailroom.delivery(id).await.expect("delivery").state
    }
    /// Every command queued before this one, a mail nudge included, has been handled.
    async fn barrier(&self) {
        barrier(&self.broker, self.agent.id).await;
    }
    fn wire(&self) -> Vec<Value> {
        wire(self.root.path())
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
async fn send(broker: &Broker, to: AgentId, body: &str) -> MessageId {
    broker
        .send(NewMessage {
            from: Sender::Human,
            to,
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
async fn barrier(broker: &Broker, id: AgentId) {
    broker
        .handle(id)
        .await
        .expect("handle")
        .pending_permissions()
        .await
        .expect("actor round trip");
}
async fn next_event(
    events: &mut broadcast::Receiver<Event>,
    matches: impl Fn(&Event) -> bool,
) -> Event {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.expect("event");
            if matches(&event) {
                return event;
            }
        }
    })
    .await
    .expect("event timeout")
}
fn wire(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("wire.jsonl"))
        .expect("wire")
        .lines()
        .map(|l| serde_json::from_str(l).expect("wire JSON"))
        .collect()
}
fn count(wire: &[Value], method: &str) -> usize {
    wire.iter().filter(|m| m["method"] == method).count()
}
fn pids(wire: &[Value]) -> Vec<i32> {
    wire.iter()
        .filter_map(|m| m["pid"].as_i64())
        .map(|p| i32::try_from(p).expect("pid fits i32"))
        .collect()
}
fn alive(pid: i32) -> bool {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
}

#[tokio::test]
async fn restart_leaves_needs_recovery_with_a_fresh_session() {
    let mut t = Test::new(Some(Duration::from_millis(25))).await;
    let first = t.send("first").await;
    t.consumed(first).await;
    t.phase(|p| *p == Phase::Dormant).await;
    std::fs::write(t.root.path().join("fail-load"), "").expect("flag");
    let waiting = t.send("second").await;
    t.phase(|p| matches!(p, Phase::NeedsRecovery { .. })).await;
    assert_eq!(t.state(waiting).await, DeliveryState::Queued);
    assert_eq!(count(&t.wire(), "session/new"), 1);

    t.operate(Operation::Restart).await;
    t.consumed(waiting).await;
    let wire = t.wire();
    assert_eq!(
        count(&wire, "session/new"),
        2,
        "restart from NeedsRecovery opens a fresh session instead of loading the failed one"
    );
    assert_eq!(
        count(&wire, "session/load"),
        1,
        "only the failed resume loads"
    );
    t.finish().await;
}

#[tokio::test]
async fn stopped_idle_agent_keeps_mail_queued_and_starts_no_session() {
    let t = Test::new(None).await;
    let first_pid = pids(&t.wire())[0];

    t.operate(Operation::Stop).await;
    assert_eq!(t.agent.phase(), Phase::Stopped);
    assert!(!alive(first_pid), "stop ends the agent process");
    let queued = t.send("while stopped").await;
    t.barrier().await;
    assert_eq!(t.agent.phase(), Phase::Stopped);
    assert_eq!(t.state(queued).await, DeliveryState::Queued);
    let wire = t.wire();
    assert_eq!(pids(&wire).len(), 1, "mail must not start a stopped agent");
    assert_eq!(count(&wire, "session/prompt"), 0);
    assert!(t.mailroom.stopped(t.agent.id).await.expect("stop flag"));
    t.finish().await;
}

#[tokio::test]
async fn stop_leaves_other_agents_running() {
    let root = tempfile::tempdir().expect("tempdir");
    let (a_root, b_root) = (root.path().join("a"), root.path().join("b"));
    std::fs::create_dir(&a_root).expect("a");
    std::fs::create_dir(&b_root).expect("b");
    let mailroom = open_mailroom(root.path()).await;
    let broker = Broker::new(
        Arc::clone(&mailroom),
        vec![kind(&a_root, false), kind(&b_root, true)],
        config(),
    )
    .expect("broker");
    let mut events = broker.events();
    let a = broker
        .spawn(spec(&a_root, "a", "claude", None))
        .await
        .expect("spawn a");
    let b = broker
        .spawn(spec(&b_root, "b", "codex", None))
        .await
        .expect("spawn b");
    broker.operate(a.id, Operation::Stop).await.expect("stop a");
    let to_b = send(&broker, b.id, "for b").await;
    next_event(
        &mut events,
        |e| matches!(e, Event::Consumed { ids, .. } if ids.contains(&to_b)),
    )
    .await;
    assert_eq!(a.phase(), Phase::Stopped);
    assert_eq!(b.phase(), Phase::Idle);
    assert!(!mailroom.stopped(b.id).await.expect("b flag"));
    broker.shutdown().await.expect("shutdown");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("mailroom sole owner")
        .close()
        .await
        .expect("close");
}

#[tokio::test]
async fn stop_during_a_busy_turn_cancels_within_the_cancel_bound() {
    // "hold" acknowledges the cancel; "ignore-cancel" never does, so only the bound ends it.
    for (body, expected) in [
        ("hold", DeliveryState::Consumed),
        (
            "ignore-cancel",
            DeliveryState::Uncertain {
                incarnation: Incarnation(1),
            },
        ),
    ] {
        let mut t = Test::new(None).await;
        let busy = t.send(body).await;
        t.event(|e| matches!(e, Event::Started { ids, .. } if ids.contains(&busy)))
            .await;
        assert_eq!(t.agent.phase(), Phase::Busy);
        let pid = pids(&t.wire())[0];

        t.operate(Operation::Stop).await;
        assert_eq!(t.agent.phase(), Phase::Stopped, "{body}");
        assert_eq!(t.state(busy).await, expected, "{body}");
        assert!(!alive(pid), "{body}: stop ends the agent process");
        assert_eq!(
            t.wire()
                .iter()
                .filter(|m| m["method"] == "session/cancel" && m.get("id").is_none())
                .count(),
            1,
            "{body}: stop cancels the running turn through session/cancel"
        );
        t.finish().await;
    }
}

#[tokio::test]
async fn restart_after_stop_delivers_mail_queued_while_stopped() {
    let mut t = Test::new(None).await;
    t.operate(Operation::Stop).await;
    let queued = t.send("while stopped").await;
    t.barrier().await;
    assert_eq!(t.state(queued).await, DeliveryState::Queued);

    t.operate(Operation::Restart).await;
    t.consumed(queued).await;
    let wire = t.wire();
    assert_eq!(pids(&wire).len(), 2);
    assert_eq!(
        count(&wire, "session/load"),
        1,
        "restart from Stopped resumes the saved session"
    );
    assert!(!t.mailroom.stopped(t.agent.id).await.expect("stop flag"));
    t.finish().await;
}

#[tokio::test]
async fn stopped_survives_an_agentd_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let mailroom = open_mailroom(root.path()).await;
    let broker = Broker::new(
        Arc::clone(&mailroom),
        vec![kind(root.path(), false)],
        config(),
    )
    .expect("broker");
    let agent = broker
        .spawn(spec(root.path(), "worker", "claude", None))
        .await
        .expect("spawn");
    broker
        .operate(agent.id, Operation::Stop)
        .await
        .expect("stop");
    let queued = send(&broker, agent.id, "before daemon restart").await;
    broker.shutdown().await.expect("shutdown");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("mailroom sole owner")
        .close()
        .await
        .expect("close");

    let mailroom = open_mailroom(root.path()).await;
    let broker = Broker::new(
        Arc::clone(&mailroom),
        vec![kind(root.path(), false)],
        config(),
    )
    .expect("broker");
    let mut events = broker.events();
    for (_, spec) in mailroom.specs().await.expect("specs") {
        broker.restore(spec).await.expect("restore");
    }
    let agent = broker.handle(agent.id).await.expect("restored handle");
    assert_eq!(agent.phase(), Phase::Stopped);
    barrier(&broker, agent.id).await;
    assert_eq!(agent.phase(), Phase::Stopped);
    assert_eq!(
        pids(&wire(root.path())).len(),
        1,
        "a stopped agent starts no session on restore"
    );
    assert_eq!(
        mailroom.delivery(queued).await.expect("delivery").state,
        DeliveryState::Queued
    );

    broker
        .operate(agent.id, Operation::Restart)
        .await
        .expect("restart");
    next_event(
        &mut events,
        |e| matches!(e, Event::Consumed { ids, .. } if ids.contains(&queued)),
    )
    .await;
    broker.shutdown().await.expect("shutdown");
    drop(broker);
    Arc::try_unwrap(mailroom)
        .ok()
        .expect("mailroom sole owner")
        .close()
        .await
        .expect("close");
}

#[tokio::test]
async fn stop_and_restart_are_idempotent() {
    let mut t = Test::new(None).await;
    t.operate(Operation::Stop).await;
    t.phase(|p| *p == Phase::Stopped).await;
    t.operate(Operation::Stop).await;
    t.barrier().await;
    assert_eq!(t.agent.phase(), Phase::Stopped);
    while let Ok(event) = t.events.try_recv() {
        assert!(
            !matches!(event, Event::PhaseChanged { .. }),
            "a second stop changes nothing: {event:?}"
        );
    }
    assert_eq!(pids(&t.wire()).len(), 1);

    t.operate(Operation::Restart).await;
    t.operate(Operation::Restart).await;
    assert_eq!(t.agent.phase(), Phase::Idle);
    let pids = pids(&t.wire());
    assert_eq!(pids.len(), 3, "each restart opens one session");
    let running = pids.iter().filter(|p| alive(**p)).collect::<Vec<_>>();
    assert_eq!(
        running,
        [pids.last().expect("pid")],
        "two restarts leave exactly one agent process"
    );
    let id = t.send("after restarts").await;
    t.consumed(id).await;
    t.finish().await;
}
