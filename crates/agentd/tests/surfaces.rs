use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use agentd::surfaces::{read_frame, write_frame};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};

struct Daemon {
    root: tempfile::TempDir,
    child: Child,
}
impl Daemon {
    async fn new(live: Option<&str>) -> Self {
        let root = tempfile::tempdir().expect("root");
        for name in ["a", "b", "state"] {
            std::fs::create_dir(root.path().join(name)).expect("dir");
        }
        std::fs::set_permissions(
            root.path().join("state"),
            std::fs::Permissions::from_mode(0o700),
        )
        .expect("private state");
        let fake = env!("CARGO_BIN_EXE_agentd-fake-acp");
        let config = format!(
            r#"
[kind.claude]
command = [{}]
mode = "default"
idle_after_secs = 1
[kind.claude.env]
FAKE_ROOT = {}
FAKE_MODE = "default"
[kind.codex]
command = [{}]
mode = "read-only"
idle_after_secs = 0
[kind.codex.sandbox_args]
workspace_write = ["--fake-workspace"]
[kind.codex.env]
FAKE_ROOT = {}
FAKE_MODE = "read-only"
"#,
            json!(live.unwrap_or(fake)),
            json!(root.path().join("a")),
            json!(fake),
            json!(root.path().join("b"))
        );
        std::fs::write(root.path().join("kinds.toml"), config).expect("config");
        let child = Self::start(&root);
        let mut daemon = Self { root, child };
        daemon.ready().await;
        daemon
    }
    fn start(root: &tempfile::TempDir) -> Child {
        Self::start_with_limit(root, None)
    }
    fn start_with_limit(root: &tempfile::TempDir, file_limit: Option<u32>) -> Child {
        let mut command = if let Some(limit) = file_limit {
            let mut command = Command::new("bash");
            command.args([
                "-c",
                &format!("ulimit -n {limit}; exec \"$@\""),
                "agentd-test",
            ]);
            command.arg(env!("CARGO_BIN_EXE_agentd"));
            command
        } else {
            Command::new(env!("CARGO_BIN_EXE_agentd"))
        };
        command
            .env("AGENTD_SOCKET", "/daemon-only/control.sock")
            .env("AGENTD_MCP_TOKEN", "daemon-only-token")
            .env("AGENTD_CONTROL_TEST", "daemon-only-setting")
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
            .expect("daemon")
    }
    fn socket(&self) -> PathBuf {
        self.root.path().join("state/agentd.sock")
    }
    async fn ready(&mut self) {
        for _ in 0..300 {
            assert!(
                self.child.try_wait().expect("daemon status").is_none(),
                "daemon exited: {}",
                std::fs::read_to_string(self.root.path().join("daemon.log")).expect("log")
            );
            if UnixStream::connect(self.socket()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("daemon did not listen");
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
            "agentctl {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&output.stdout).trim()))
    }
    async fn spawn(&self, name: &str, kind: &str, first: Option<&str>) {
        let cwd = self.root.path().join(name);
        let mut args = vec![
            "spawn",
            name,
            "--kind",
            kind,
            "--cwd",
            cwd.to_str().expect("cwd"),
        ];
        if let Some(first) = first {
            args.push(first);
        }
        self.ctl(&args).await;
    }
    async fn relay(&self, token: &str, request: Value) -> Value {
        let mut stream = UnixStream::connect(self.root.path().join("state/mcp.sock"))
            .await
            .expect("MCP socket");
        write_frame(
            &mut stream,
            &json!({"token":token,"session":"a".repeat(64),"request":request}),
        )
        .await
        .expect("MCP request");
        serde_json::from_slice(
            &read_frame(&mut BufReader::new(stream))
                .await
                .expect("frame")
                .expect("response"),
        )
        .expect("JSON")
    }
    async fn wait_wire(&self, name: &str, predicate: impl Fn(&[Value]) -> bool) -> Vec<Value> {
        for _ in 0..1000 {
            let wire = std::fs::read_to_string(self.root.path().join(name).join("wire.jsonl"))
                .unwrap_or_default();
            let messages = wire
                .lines()
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect::<Vec<_>>();
            if predicate(&messages) {
                return messages;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("wire predicate timed out for {name}");
    }
    async fn stop(&mut self, signal: nix::sys::signal::Signal) {
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.child.id().expect("pid") as i32),
            signal,
        )
        .expect("signal");
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("shutdown deadline")
            .expect("wait");
        assert!(
            status.success(),
            "daemon exit {status}: {}",
            std::fs::read_to_string(self.root.path().join("daemon.log")).expect("log")
        );
        assert!(!self.socket().exists(), "socket removed on shutdown");
    }
}
fn prompts(messages: &[Value]) -> Vec<&Value> {
    messages
        .iter()
        .filter(|v| v["method"] == "session/prompt")
        .collect()
}
fn session_token(messages: &[Value]) -> &str {
    messages
        .iter()
        .find(|v| v["method"] == "session/new")
        .expect("session new")["params"]["mcpServers"][0]["env"][0]["value"]
        .as_str()
        .expect("token")
}
fn send_call(id: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"send_message","arguments":{"to":"@b","body":"forged payload"}}})
}

async fn shim_exchange(d: &Daemon, token: &str, requests: &[String]) -> Vec<Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentd"))
        .args(["mcp", "--socket"])
        .arg(d.root.path().join("state/mcp.sock"))
        .env("AGENTD_MCP_TOKEN", token)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("MCP shim");
    let mut input = child.stdin.take().expect("MCP stdin");
    let mut output = BufReader::new(child.stdout.take().expect("MCP stdout"));
    let mut responses = Vec::new();
    for request in requests {
        input
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("MCP request");
        let line = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut output))
            .await
            .expect("MCP deadline")
            .expect("MCP frame")
            .expect("MCP response");
        responses.push(serde_json::from_slice(&line).expect("MCP JSON"));
    }
    drop(input);
    assert!(child.wait().await.expect("MCP exit").success());
    responses
}

#[tokio::test]
async fn shim_restart_preserves_new_mail_and_same_session_retries_deduplicate() {
    use agentd::mailroom::Mailroom;
    use agentd::model::{Recipient, Sender};

    let mut d = Daemon::new(None).await;
    d.spawn("b", "codex", Some("hold")).await;
    d.wait_wire("b", |v| !prompts(v).is_empty()).await;
    d.spawn("a", "claude", Some("hold")).await;
    let wire = d.wait_wire("a", |v| !prompts(v).is_empty()).await;
    let token = session_token(&wire);
    let mut first = send_call(json!(1));
    first["params"]["arguments"]["body"] = json!("first shim mail");
    let results = shim_exchange(&d, token, &[first.to_string(), first.to_string()]).await;
    assert_eq!(results[0]["result"]["isError"], false);
    assert!(
        results[1]["result"]["content"][0]["text"]
            .as_str()
            .expect("retry status")
            .contains("Already queued")
    );
    let mut second = send_call(json!(1));
    second["params"]["arguments"]["body"] = json!("second shim mail");
    let results = shim_exchange(&d, token, &[second.to_string()]).await;
    assert_eq!(results[0]["result"]["isError"], false);
    assert!(
        !results[0]["result"]["content"][0]["text"]
            .as_str()
            .expect("new send status")
            .contains("Already queued"),
        "new shim mail was silently deduplicated: {}",
        results[0]
    );
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
    let mail = Mailroom::open(d.root.path().join("state/mail.db"), Default::default())
        .await
        .expect("mailroom");
    let a = mail
        .resolve(Recipient::parse("@a").expect("a"))
        .await
        .expect("sender");
    let b = mail
        .resolve(Recipient::parse("@b").expect("b"))
        .await
        .expect("recipient");
    let pending = mail.pending_for(b).await.expect("pending mail");
    assert_eq!(pending.len(), 2, "both shims must produce an envelope");
    assert_eq!(pending[0].body().as_str(), "first shim mail");
    assert_eq!(pending[1].body().as_str(), "second shim mail");
    assert!(pending.iter().all(|e| e.from() == &Sender::Agent(a)));
    mail.close().await.expect("close");
}

#[tokio::test]
async fn held_cli_reads_more_than_one_mib_of_mail() {
    use agentd::mailroom::{Mailroom, NewMessage};
    use agentd::model::*;
    use agentd::swarm::Act;

    let mut d = Daemon::new(None).await;
    d.spawn("b", "codex", None).await;
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
    let mail = Mailroom::open(
        d.root.path().join("state/mail.db"),
        agentd::mailroom::Config {
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .expect("mailroom");
    let b = mail
        .resolve(Recipient::parse("@b").expect("b"))
        .await
        .expect("recipient");
    let mut ids = Vec::new();
    for i in 0..20 {
        let (_, envelope) = mail
            .accept(NewMessage {
                from: Sender::Human,
                to: b,
                body: Body::new(format!("{i:02}{}", "\u{0001}".repeat(MAX_BODY_BYTES - 2)))
                    .expect("large body"),
                reply_to: None,
                client_key: ClientKey(format!("large-{i}")),
                act: Act::Inform,
                task: None,
            })
            .await
            .expect("accept");
        ids.push(envelope.id());
    }
    mail.mark_offered(&ids, Incarnation(1))
        .await
        .expect("offer");
    mail.session_ended(b, Incarnation(1), false)
        .await
        .expect("requeue");
    assert_eq!(
        mail.held(Some(b)).await.expect("held at settlement").len(),
        ids.len()
    );
    let expected =
        serde_json::to_value(mail.held(Some(b)).await.expect("held mail")).expect("held JSON");
    assert!(expected.to_string().len() > 1024 * 1024);
    mail.close().await.expect("close");
    d.child = Daemon::start(&d.root);
    d.ready().await;
    assert_eq!(d.ctl(&["held", "@b"]).await, expected);
    assert_eq!(d.ctl(&["held"]).await, expected);
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[tokio::test]
async fn malformed_mcp_json_returns_parse_error_and_shim_continues() {
    let mut d = Daemon::new(None).await;
    d.spawn("a", "claude", Some("hold")).await;
    let wire = d.wait_wire("a", |v| !prompts(v).is_empty()).await;
    let responses = shim_exchange(
        &d,
        session_token(&wire),
        &[
            "{broken".into(),
            json!({"jsonrpc":"2.0", "id":1, "method":"ping"}).to_string(),
        ],
    )
    .await;
    assert_eq!(responses[0]["error"]["code"], -32700);
    assert_eq!(responses[0]["id"], Value::Null);
    assert_eq!(responses[1]["result"], json!({}));
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[tokio::test]
async fn agent_children_do_not_inherit_agentd_environment() {
    let mut d = Daemon::new(None).await;
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
    let path = d.root.path().join("kinds.toml");
    let config = std::fs::read_to_string(&path).expect("config").replace(
        "[kind.claude.env]",
        "[kind.claude.env]\nAGENTD_SOCKET = '/configured/control.sock'\nAGENTD_OVERRIDE = 'configured'",
    );
    std::fs::write(path, config).expect("config overrides");
    d.child = Daemon::start(&d.root);
    d.ready().await;
    d.spawn("a", "claude", Some("hold")).await;
    let wire = d.wait_wire("a", |v| !prompts(v).is_empty()).await;
    assert_eq!(
        wire[0]["agentd_env"],
        json!([]),
        "daemon control environment leaked"
    );
    let response = shim_exchange(
        &d,
        session_token(&wire),
        &[json!({"jsonrpc":"2.0", "id":1, "method":"ping"}).to_string()],
    )
    .await;
    assert_eq!(response[0]["result"], json!({}), "MCP binding still works");
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[tokio::test]
async fn connection_caps_are_per_socket_and_slots_recover() {
    let mut d = Daemon::new(None).await;
    for path in [d.root.path().join("state/mcp.sock"), d.socket()] {
        let mut peers = Vec::new();
        for _ in 0..64 {
            peers.push(UnixStream::connect(&path).await.expect("idle connection"));
        }
        let extra = UnixStream::connect(&path).await.expect("excess connection");
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            read_frame(&mut BufReader::new(extra)),
        )
        .await
        .expect("excess connection closes")
        .expect("closed frame");
        assert!(closed.is_none(), "connection cap must close excess peers");
        if path != d.socket() {
            assert_eq!(
                d.ctl(&["ls"]).await,
                json!([]),
                "MCP cap must leave control usable"
            );
        } else {
            let response = d
                .relay("invalid", json!({"jsonrpc":"2.0", "id":1, "method":"ping"}))
                .await;
            assert_eq!(
                response["error"]["code"], -32001,
                "control cap must leave MCP usable"
            );
        }
        drop(peers);
        // Retry until the daemon observes EOF and releases the idle slots.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut stream = UnixStream::connect(&path).await.expect("reconnect");
                if write_frame(&mut stream, &json!({"command":"ls"}))
                    .await
                    .is_ok()
                    && let Ok(Some(_)) = read_frame(&mut BufReader::new(stream)).await
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("released connection slot");
    }
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[tokio::test]
async fn daemon_recovers_from_emfile_accept_errors() {
    let mut d = Daemon::new(None).await;
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
    d.child = Daemon::start_with_limit(&d.root, Some(96));
    d.ready().await;
    d.spawn("b", "codex", Some("hold")).await;
    d.wait_wire("b", |v| !prompts(v).is_empty()).await;
    let mut peers = Vec::new();
    for i in 0..110 {
        let path = if i % 2 == 0 {
            d.socket()
        } else {
            d.root.path().join("state/mcp.sock")
        };
        let stream = tokio::time::timeout(Duration::from_secs(1), UnixStream::connect(path))
            .await
            .expect("connect deadline")
            .expect("idle connection");
        peers.push(stream);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let log =
                std::fs::read_to_string(d.root.path().join("daemon.log")).expect("daemon log");
            if log.contains("accept failed: Too many open files") {
                break;
            }
            assert!(
                d.child.try_wait().expect("daemon status").is_none(),
                "daemon died under EMFILE: {log}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("EMFILE must be exercised");
    drop(peers);
    let agents = d.ctl(&["ls"]).await;
    assert_eq!(agents[0]["phase"], "busy", "agent survives EMFILE");
    d.ctl(&["send", "@b", "after EMFILE"]).await;
    std::fs::write(d.root.path().join("b/release"), "release").expect("release busy agent");
    d.wait_wire("b", |v| {
        prompts(v)
            .iter()
            .any(|p| p.to_string().contains("after EMFILE"))
    })
    .await;
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[tokio::test]
async fn binary_cli_mcp_delivery_security_retry_and_restart() {
    let mut d = Daemon::new(None).await;
    assert_eq!(
        std::fs::metadata(d.socket())
            .expect("socket metadata")
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(d.root.path().join("state/mcp.sock"))
            .expect("MCP metadata")
            .mode()
            & 0o777,
        0o600
    );
    let mut watch = Command::new(env!("CARGO_BIN_EXE_agentctl"))
        .arg("--socket")
        .arg(d.socket())
        .arg("watch")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("watch");
    let mut events = BufReader::new(watch.stdout.take().expect("watch stdout")).lines();
    let first = events
        .next_line()
        .await
        .expect("snapshot line")
        .expect("snapshot");
    assert_eq!(
        serde_json::from_str::<Value>(&first).expect("snapshot JSON")["event"],
        "snapshot"
    );
    d.spawn("b", "codex", Some("hold")).await;
    d.wait_wire("b", |v| prompts(v).len() == 1).await;
    d.spawn("a", "claude", Some("mcp-send")).await;
    let wire = d
        .wait_wire("a", |_| d.root.path().join("a/mcp-result.json").exists())
        .await;
    let retry: Value = serde_json::from_slice(
        &std::fs::read(d.root.path().join("a/mcp-retry.json")).expect("retry result"),
    )
    .expect("retry JSON");
    assert!(
        retry["result"]["content"][0]["text"]
            .as_str()
            .expect("retry text")
            .contains("Already queued"),
        "retry must recover the original mail: {retry}"
    );
    let token = session_token(&wire).to_owned();
    let result: Value = serde_json::from_slice(
        &std::fs::read(d.root.path().join("a/mcp-result.json")).expect("MCP result"),
    )
    .expect("result");
    assert!(
        result["result"]["content"][0]["text"]
            .as_str()
            .expect("status")
            .contains("busy")
    );
    let ls = d.ctl(&["ls"]).await;
    assert_eq!(ls.as_array().expect("agents").len(), 2);
    assert_eq!(ls[1]["name"], "@b");
    assert_eq!(ls[1]["kind"], "codex");
    assert_eq!(ls[1]["phase"], "busy");
    assert_eq!(ls[1]["queued"], 1, "retry created no second mail");
    let forged = d.relay("forged-token", send_call(json!(1))).await;
    assert_eq!(forged["error"]["code"], -32001);
    let mut claimed_sender = send_call(json!("forged-sender"));
    claimed_sender["params"]["arguments"]["from"] = json!("@b");
    assert_eq!(
        d.relay(&token, claimed_sender).await["result"]["isError"],
        true
    );
    std::fs::write(d.root.path().join("b/release"), "").expect("release b");
    let wire_b = d.wait_wire("b", |v| prompts(v).len() >= 2).await;
    let peer = prompts(&wire_b)[1]["params"]["prompt"].to_string();
    assert!(peer.contains("<agent-message"), "{peer}");
    assert!(peer.contains("@a"), "{peer}");
    assert!(peer.contains("peer payload"));
    d.ctl(&["send", "@b", "human payload"]).await;
    let wire_b = d.wait_wire("b", |v| prompts(v).len() >= 3).await;
    let human = prompts(&wire_b)[2]["params"]["prompt"].to_string();
    assert!(human.contains("human payload"));
    assert!(!human.contains("<agent-message"));
    let mut saw_consumed = false;
    for _ in 0..80 {
        let line = tokio::time::timeout(Duration::from_secs(5), events.next_line())
            .await
            .expect("watch event deadline")
            .expect("event line")
            .expect("event");
        assert!(!line.contains(&token));
        if line.contains("consumed") {
            saw_consumed = true;
            break;
        }
    }
    assert!(saw_consumed, "watch streams consumed events");
    d.ctl(&["send", "@a", "echo-token"]).await;
    let mut redacted = false;
    for _ in 0..80 {
        let line = tokio::time::timeout(Duration::from_secs(5), events.next_line())
            .await
            .expect("redaction event deadline")
            .expect("line")
            .expect("event");
        assert!(!line.contains(&token), "binding leaked into watch");
        if line.contains("[redacted]") {
            redacted = true;
            break;
        }
    }
    assert!(redacted, "agent's echoed binding is redacted");
    assert!(
        !std::fs::read_to_string(d.root.path().join("daemon.log"))
            .expect("daemon log")
            .contains(&token)
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        d.relay(&token, send_call(json!(2))).await["error"]["code"],
        -32001,
        "suspended token revoked"
    );
    d.ctl(&["send", "@a", "resume payload"]).await;
    let loaded = d
        .wait_wire("a", |v| v.iter().any(|v| v["method"] == "session/load"))
        .await;
    let new_token = loaded
        .iter()
        .find(|v| v["method"] == "session/load")
        .expect("load")["params"]["mcpServers"][0]["env"][0]["value"]
        .as_str()
        .expect("new token")
        .to_owned();
    assert_ne!(token, new_token);
    assert_eq!(
        d.relay(&token, send_call(json!(3))).await["error"]["code"],
        -32001
    );
    watch.kill().await.expect("stop watch");
    watch.wait().await.expect("reap watch");
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
    d.child = Daemon::start(&d.root);
    d.ready().await;
    assert_eq!(d.ctl(&["ls"]).await.as_array().expect("restored").len(), 2);
    assert_eq!(
        d.relay(&new_token, send_call(json!(4))).await["error"]["code"],
        -32001,
        "daemon restart revokes old token"
    );
    d.ctl(&["send", "@b", "after restart"]).await;
    d.wait_wire("b", |v| {
        prompts(v)
            .iter()
            .any(|v| v.to_string().contains("after restart"))
    })
    .await;
    d.stop(nix::sys::signal::Signal::SIGINT).await;
}

#[tokio::test]
async fn operator_approvals_cancel_held_release_and_drop() {
    let mut d = Daemon::new(None).await;
    d.spawn("b", "codex", Some("permission")).await;
    let pending = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let requests = d.ctl(&["approvals"]).await;
            if !requests.as_array().expect("requests").is_empty() {
                break requests;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pending approval deadline");
    d.ctl(&[
        "approve",
        pending[0]["id"].as_str().expect("permission id"),
        "yes",
    ])
    .await;
    assert_eq!(d.ctl(&["approvals"]).await, json!([]));
    d.ctl(&["send", "@b", "hold"]).await;
    d.wait_wire("b", |v| prompts(v).len() >= 2).await;
    d.ctl(&["cancel", "@b"]).await;
    d.wait_wire("b", |v| v.iter().any(|v| v["method"] == "session/cancel"))
        .await;
    d.ctl(&["send", "@b", "die-after"]).await;
    let held = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let held = d.ctl(&["held"]).await;
            if !held.as_array().expect("held").is_empty() {
                break held;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("uncertain mail");
    let id = held[0]["envelope"]["id"].as_str().expect("held id");
    d.ctl(&["release", id]).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !d
                .ctl(&["held", "@b"])
                .await
                .as_array()
                .expect("held")
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("re-offer becomes uncertain");
    d.ctl(&["drop", id]).await;
    assert_eq!(d.ctl(&["held"]).await, json!([]));
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[tokio::test]
#[ignore = "requires a logged-in Claude ACP shim in AGENTD_CLAUDE_ACP"]
async fn live_claude_sends_through_broker_mcp() {
    let shim = std::env::var("AGENTD_CLAUDE_ACP").expect("AGENTD_CLAUDE_ACP");
    let mut d = Daemon::new(Some(&shim)).await;
    d.spawn("b", "codex", None).await;
    d.spawn("a","claude",Some("Use the agentd send_message MCP tool to send exactly live-mcp-proof to @b. Do this now. Do not use any other tools. Then stop.")).await;
    let result = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            let wire =
                std::fs::read_to_string(d.root.path().join("b/wire.jsonl")).unwrap_or_default();
            if wire
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .any(|v| {
                    v["method"] == "session/prompt" && v.to_string().contains("live-mcp-proof")
                })
            {
                break;
            }
            for request in d.ctl(&["approvals"]).await.as_array().expect("approvals") {
                let option = request["options"]
                    .as_array()
                    .expect("options")
                    .iter()
                    .find(|o| o["effect"] == "AllowOnce")
                    .expect("allow once");
                d.ctl(&[
                    "approve",
                    request["id"].as_str().expect("id"),
                    option["key"].as_str().expect("option"),
                ])
                .await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    std::fs::copy(
        d.root.path().join("daemon.log"),
        "/tmp/agentd-live-daemon.log",
    )
    .expect("live diagnostic log");
    result.expect("live MCP delivery");
    println!(
        "LIVE PASS: real Claude called send_message; fake B received live-mcp-proof through broker"
    );
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[tokio::test]
async fn private_socket_reclaims_stale_paths_and_preserves_live_or_regular_paths() {
    use agentd::surfaces::Socket;
    if let Some(path) = std::env::var_os("AGENTD_STALE_SOCKET_FIXTURE") {
        let _listener = std::os::unix::net::UnixListener::bind(path).expect("stale socket");
        return;
    }
    let root = tempfile::tempdir().expect("root");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private root");
    let path = root.path().join("control.sock");
    // Concurrent subprocess forks can briefly inherit a listener even with
    // CLOEXEC. Waiting for its creating process to exit makes it truly stale.
    let status = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "private_socket_reclaims_stale_paths_and_preserves_live_or_regular_paths",
        ])
        .env("AGENTD_STALE_SOCKET_FIXTURE", &path)
        .status()
        .await
        .expect("create stale socket in child");
    assert!(status.success());
    let socket = Socket::bind(&path).expect("reclaim stale socket");
    assert_eq!(
        std::fs::metadata(&path).expect("metadata").mode() & 0o777,
        0o600
    );
    assert!(Socket::bind(&path).is_err(), "live socket preserved");
    let stream = UnixStream::connect(&path).await.expect("local peer");
    Socket::check_peer(&stream).expect("owner accepted");
    drop(socket);
    std::fs::write(&path, "preserve me").expect("regular file");
    assert!(Socket::bind(&path).is_err());
    assert_eq!(std::fs::read_to_string(&path).expect("file"), "preserve me");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755))
        .expect("public dir");
    assert!(
        Socket::bind(&root.path().join("other.sock")).is_err(),
        "public parent refused"
    );
}

#[tokio::test]
async fn cli_sandbox_and_model_select_acp_modes_and_launch_arguments() {
    let mut d = Daemon::new(None).await;
    std::fs::write(d.root.path().join("b/expected-mode"), "workspace-write")
        .expect("expected mode");
    let cwd = d.root.path().join("b");
    d.ctl(&[
        "spawn",
        "b",
        "--kind",
        "codex",
        "--cwd",
        cwd.to_str().expect("cwd"),
        "--sandbox",
        "workspace-write",
        "--model",
        "test-model",
        "human first turn",
    ])
    .await;
    let wire = d.wait_wire("b", |v| !prompts(v).is_empty()).await;
    assert!(
        wire.iter().any(
            |v| v["method"] == "session/set_mode" && v["params"]["modeId"] == "workspace-write"
        )
    );
    assert!(
        wire.iter()
            .any(|v| v["method"] == "session/set_model" && v["params"]["modelId"] == "test-model")
    );
    assert_eq!(wire[0]["args"][1], "--fake-workspace");
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}

#[path = "support/fake_infinity.rs"]
mod fake_infinity;

#[tokio::test]
async fn agentctl_spawns_an_infinity_kind_and_relays_approval() {
    use infinity_protocol::{ClientMessage, DaemonMessage};
    let mut d = Daemon::new(None).await;
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
    let socket = d.root.path().join("infinity.sock");
    let mut infinity = fake_infinity::FakeDaemon::bind(&socket);
    let path = d.root.path().join("kinds.toml");
    let mut config = std::fs::read_to_string(&path).expect("config");
    config.push_str(&format!(
        "\n[kind.infinity]\nwire = 'infinity'\nsocket = {}\nmodel = 'fake/small'\n",
        json!(socket)
    ));
    std::fs::write(path, config).expect("Infinity config");
    d.child = Daemon::start(&d.root);
    d.ready().await;
    d.spawn("a", "infinity", Some("hello model")).await;
    let create = infinity.next().await;
    assert!(infinity.state.lock().expect("state").created);
    assert!(
        matches!(create.message, ClientMessage::CreateSession { model: Some(m), .. } if m.provider_id == "fake" && m.model_id == "small")
    );
    let input = infinity.next().await;
    assert!(
        matches!(&input.message, ClientMessage::UserInput { text, .. } if text == "hello model")
    );
    input.emit(DaemonMessage::UserInputEcho {
        thread_id: fake_infinity::thread(),
        text: "hello model".into(),
    });
    input.emit(fake_infinity::start());
    input.emit(DaemonMessage::UserChoiceRequired {
        thread_id: fake_infinity::thread(),
        id: "choice".into(),
        prompt: "Choose".into(),
        choices: vec!["Allow".into(), "Deny".into()],
        default: 1,
    });
    let approvals = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let approvals = d.ctl(&["approvals"]).await;
            if !approvals.as_array().expect("approvals").is_empty() {
                break approvals;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("approvals timeout");
    let id = approvals[0]["id"].as_str().expect("permission id");
    d.ctl(&["approve", id, "0"]).await;
    assert!(matches!(
        infinity.next().await.message,
        ClientMessage::UserChoiceAnswered { selected: 0, .. }
    ));
    input.emit(fake_infinity::receipt("human-id".into()));
    input.emit(fake_infinity::round_done());
    input.emit(fake_infinity::idle());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let agents = d.ctl(&["ls"]).await;
            if agents[0]["phase"] == "idle" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("idle timeout");
    input.die();
    d.stop(nix::sys::signal::Signal::SIGTERM).await;
}
