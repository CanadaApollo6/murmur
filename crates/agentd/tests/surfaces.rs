use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use agentd::surfaces::{read_frame, write_frame};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
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
        Command::new(env!("CARGO_BIN_EXE_agentd"))
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
        write_frame(&mut stream, &json!({"token":token,"request":request}))
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
    let root = tempfile::tempdir().expect("root");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private root");
    let path = root.path().join("control.sock");
    let stale = std::os::unix::net::UnixListener::bind(&path).expect("stale socket");
    drop(stale);
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
