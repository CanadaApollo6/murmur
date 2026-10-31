//! Review probe (scratch only): a stopped Infinity session strands agentd mail.
use std::sync::Arc;
use std::time::Duration;

use agentd::KindConfig;
use agentd::mailroom::{Mailroom, NewMessage};
use agentd::model::*;
use agentd::supervisor::{AgentSpec, Broker, Event, Phase};
use agentd::swarm::Act;
use futures_util::{SinkExt, StreamExt};
use infinity_protocol::{ClientMessage, DaemonMessage};

fn spec(root: &std::path::Path) -> AgentSpec {
    AgentSpec {
        name: AgentName::parse("model").expect("name"),
        kind: "infinity".into(),
        cwd: root.into(),
        model: None,
        permissions: Default::default(),
        idle_after: None,
    }
}

#[tokio::test]
#[ignore = "review probe; needs target/debug/infinity"]
async fn r10b_shutdown_session_strands_mail() {
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
    let binary =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/infinity");
    let mut command = Command::new(binary);
    command
        .arg("daemon")
        .env("HOME", root.path())
        .env("INFINITY_WS_PORT", "0")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    command.process_group(0);
    let mut daemon = command.spawn().expect("spawn infinity daemon");
    let pgid = daemon.id().expect("pid") as i32;
    let mut output = BufReader::new(daemon.stdout.take().expect("stdout")).lines();
    tokio::time::timeout(Duration::from_secs(30), async {
        while output.next_line().await.expect("read").expect("line") != "infinity-daemon ready" {}
    })
    .await
    .expect("daemon ready");

    let mailroom = Arc::new(
        Mailroom::open(root.path().join("mail.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let mut kind = KindConfig::infinity(state.join("daemon.sock"));
    kind.model = Some("fake/echo".into());
    kind.request_timeout = Duration::from_secs(3);
    let broker = Broker::new(mailroom.clone(), vec![kind], Default::default()).expect("broker");
    let mut events = broker.events();
    let agent = broker.spawn(spec(root.path())).await.expect("spawn");
    let sender = mailroom
        .register(AgentName::parse("sender").expect("name"))
        .await
        .expect("sender");
    let send = |body: &'static str| {
        let broker = &broker;
        let to = agent.id;
        async move {
            broker
                .send(NewMessage {
                    from: Sender::Agent(sender),
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
    };

    let first = send("first").await;
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut consumed = false;
        loop {
            match events.recv().await.expect("event") {
                Event::Consumed { ids, .. } if ids.contains(&first) => consumed = true,
                Event::PhaseChanged {
                    phase: Phase::Idle, ..
                } if consumed => break,
                _ => {}
            }
        }
    })
    .await
    .expect("first mail consumed");

    // An ordinary Infinity CLI user attaches, then quits without keep-running.
    let stream = tokio::net::UnixStream::connect(state.join("daemon.sock"))
        .await
        .expect("cli connect");
    let mut cli =
        tokio_util::codec::Framed::new(stream, infinity_protocol::length_delimited_codec());
    let session = loop {
        let bytes = cli.next().await.expect("frame").expect("bytes");
        if let DaemonMessage::Welcome { sessions, .. } =
            serde_json::from_slice(&bytes).expect("json")
        {
            break sessions.keys().next().expect("one session").clone();
        }
    };
    cli.send(
        serde_json::to_vec(&ClientMessage::ShutdownSession {
            root_thread_id: session,
        })
        .expect("encode")
        .into(),
    )
    .await
    .expect("shutdown");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let second = send("second").await;
    let mut phase = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let consumed = loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Err(_) => break false,
            Ok(Ok(Event::Consumed { ids, .. })) if ids.contains(&second) => break true,
            Ok(Ok(Event::PhaseChanged { phase: p, .. })) => phase = Some(p),
            Ok(_) => {}
        }
    };
    let state = mailroom.delivery(second).await.expect("delivery").state;
    eprintln!("PROBE consumed={consumed} delivery={state:?} last_phase={phase:?}");
    let _ = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pgid),
        nix::sys::signal::Signal::SIGKILL,
    );
    assert!(
        consumed,
        "mail stranded after ShutdownSession: {state:?}, phase {phase:?}"
    );
}

async fn start_daemon(root: &std::path::Path) -> (tokio::process::Child, i32) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let binary =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/infinity");
    let mut command = tokio::process::Command::new(binary);
    command
        .arg("daemon")
        .env("HOME", root)
        .env("INFINITY_WS_PORT", "0")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    command.process_group(0);
    let mut daemon = command.spawn().expect("spawn infinity daemon");
    let pgid = daemon.id().expect("pid") as i32;
    let mut output = BufReader::new(daemon.stdout.take().expect("stdout")).lines();
    tokio::time::timeout(Duration::from_secs(30), async {
        while output.next_line().await.expect("read").expect("line") != "infinity-daemon ready" {}
    })
    .await
    .expect("daemon ready");
    tokio::spawn(async move { while let Ok(Some(_)) = output.next_line().await {} });
    (daemon, pgid)
}

#[tokio::test]
#[ignore = "review probe; needs target/debug/infinity"]
async fn r10b_daemon_outage_fails_agent() {
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
    let (_daemon, pgid) = start_daemon(root.path()).await;
    let mailroom = Arc::new(
        Mailroom::open(root.path().join("mail.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let mut kind = KindConfig::infinity(state.join("daemon.sock"));
    kind.model = Some("fake/echo".into());
    let broker = Broker::new(mailroom.clone(), vec![kind], Default::default()).expect("broker");
    let mut events = broker.events();
    let agent = broker.spawn(spec(root.path())).await.expect("spawn");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Event::PhaseChanged {
                phase: Phase::Idle, ..
            } = events.recv().await.expect("event")
            {
                break;
            }
        }
    })
    .await
    .expect("idle");
    // Daemon dies and an operator restarts it 18 s later.
    let _ = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pgid),
        nix::sys::signal::Signal::SIGKILL,
    );
    let _ = std::fs::remove_file(root.path().join("provider.sock"));
    let mut phases = Vec::new();
    let until = tokio::time::Instant::now() + Duration::from_secs(18);
    while let Ok(Ok(event)) = tokio::time::timeout_at(until, events.recv()).await {
        if let Event::PhaseChanged { phase, .. } = event {
            phases.push(format!("{phase:?}").chars().take(40).collect::<String>());
        }
    }
    let (_daemon2, pgid2) = start_daemon(root.path()).await;
    let sender = mailroom
        .register(AgentName::parse("sender").expect("name"))
        .await
        .expect("sender");
    let id = broker
        .send(NewMessage {
            from: Sender::Agent(sender),
            to: agent.id,
            body: Body::new("after restart").expect("body"),
            reply_to: None,
            client_key: ClientKey(ulid::Ulid::new().to_string()),
            act: Act::Inform,
            task: None,
        })
        .await
        .expect("send")
        .1
        .id();
    let until = tokio::time::Instant::now() + Duration::from_secs(70);
    let consumed = loop {
        match tokio::time::timeout_at(until, events.recv()).await {
            Err(_) => break false,
            Ok(Ok(Event::Consumed { ids, .. })) if ids.contains(&id) => break true,
            Ok(Ok(Event::PhaseChanged { phase, .. })) => {
                phases.push(format!("{phase:?}").chars().take(40).collect())
            }
            Ok(_) => {}
        }
    };
    let state = mailroom.delivery(id).await.expect("delivery").state;
    eprintln!("PROBE consumed={consumed} delivery={state:?} phases={phases:?}");
    let _ = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pgid2),
        nix::sys::signal::Signal::SIGKILL,
    );
    assert!(
        consumed,
        "mail stranded after an 18 s daemon outage: {state:?}"
    );
}

#[tokio::test]
#[ignore = "review probe; needs target/debug/infinity"]
async fn r10b_cancel_with_queued_mail_crashes() {
    let root = tempfile::tempdir().expect("tempdir");
    let state = root.path().join(".infinity");
    std::fs::create_dir(&state).expect("state dir");
    std::fs::write(
        state.join("providers.json"),
        serde_json::to_vec(&serde_json::json!({
            "fake": { "command": ["python3", std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/slow_provider.py"), root.path().join("provider.sock")] }
        }))
        .expect("provider JSON"),
    )
    .expect("write providers");
    let (_daemon, pgid) = start_daemon(root.path()).await;
    let mailroom = Arc::new(
        Mailroom::open(root.path().join("mail.db"), Default::default())
            .await
            .expect("mailroom"),
    );
    let mut kind = KindConfig::infinity(state.join("daemon.sock"));
    kind.model = Some("fake/echo".into());
    let broker = Broker::new(mailroom.clone(), vec![kind], Default::default()).expect("broker");
    let mut events = broker.events();
    let agent = broker.spawn(spec(root.path())).await.expect("spawn");
    let sender = mailroom
        .register(AgentName::parse("sender").expect("name"))
        .await
        .expect("sender");
    let send = |body: &'static str| {
        let broker = &broker;
        let to = agent.id;
        async move {
            broker
                .send(NewMessage {
                    from: Sender::Agent(sender),
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
    };
    let _first = send("runaway").await;
    tokio::time::sleep(Duration::from_secs(2)).await; // completion in flight
    let second = send("queued while busy").await;
    tokio::time::sleep(Duration::from_millis(500)).await; // busy-delivered into core
    agent.cancel().await.expect("cancel");
    let mut phases = Vec::new();
    let until = tokio::time::Instant::now() + Duration::from_secs(30);
    while let Ok(Ok(event)) = tokio::time::timeout_at(until, events.recv()).await {
        match event {
            Event::PhaseChanged { phase, .. } => {
                phases.push(format!("{phase:?}").chars().take(60).collect::<String>())
            }
            Event::Consumed { ids, .. } => phases.push(format!(
                "consumed {}",
                if ids.contains(&second) {
                    "second"
                } else {
                    "first"
                }
            )),
            _ => {}
        }
    }
    eprintln!(
        "PROBE second={:?} events={phases:?}",
        mailroom.delivery(second).await.expect("d").state
    );
    let _ = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pgid),
        nix::sys::signal::Signal::SIGKILL,
    );
    assert!(
        !phases.iter().any(|p| p.starts_with("Crashed")),
        "cancel crashed the session"
    );
}
