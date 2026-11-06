#[path = "support/max_snapshot.rs"]
mod maximum_fixture;
use agentd::surfaces::{events::command, read_frame_limit, write_frame};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
    process::{Child, Command},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue},
};

struct Daemon {
    root: tempfile::TempDir,
    child: Child,
    port: u16,
}
impl Daemon {
    async fn new(key: &str, title: usize) -> Self {
        let root = tempfile::Builder::new()
            .prefix("m5 ui ")
            .tempdir()
            .expect("dir");
        std::fs::create_dir(root.path().join("state")).expect("state");
        std::fs::set_permissions(
            root.path().join("state"),
            std::fs::Permissions::from_mode(0o700),
        )
        .expect("private state");
        std::fs::create_dir(root.path().join("assets")).expect("assets");
        std::fs::write(
            root.path().join("assets/index.html"),
            "<!doctype html><script src='/old.js'></script>",
        )
        .expect("index");
        std::fs::write(root.path().join("assets/old.js"), "old").expect("asset");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("reserve")
            .local_addr()
            .expect("addr")
            .port();
        let cfg = format!(
            "sandbox='none'\n[ui]\nlisten='127.0.0.1:{port}'\nassets={}\n[kind.claude]\ncommand=[{}]\n[kind.claude.env]\nFAKE_ROOT={}\nFAKE_MODE='default'\nFAKE_OPTION_KEY={}\nFAKE_TITLE_BYTES='{title}'\n",
            json!(root.path().join("assets")),
            json!(env!("CARGO_BIN_EXE_agentd-fake-acp")),
            json!(root.path()),
            json!(key)
        );
        std::fs::write(root.path().join("config.toml"), cfg).expect("config");
        let child = Self::start(&root);
        let mut d = Self { root, child, port };
        d.ready().await;
        d
    }
    fn start(root: &tempfile::TempDir) -> Child {
        Command::new(env!("CARGO_BIN_EXE_agentd"))
            .arg("--state-dir")
            .arg(root.path().join("state"))
            .arg("--config")
            .arg(root.path().join("config.toml"))
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                std::fs::File::create(root.path().join("daemon.log")).expect("log"),
            ))
            .kill_on_drop(true)
            .spawn()
            .expect("start")
    }
    fn socket(&self) -> PathBuf {
        self.root.path().join("state/agentd.sock")
    }
    async fn ready(&mut self) {
        for _ in 0..500 {
            assert!(
                self.child.try_wait().expect("status").is_none(),
                "{}",
                std::fs::read_to_string(self.root.path().join("daemon.log")).expect("log")
            );
            if UnixStream::connect(self.socket()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("startup timeout");
    }
    fn token(&self) -> String {
        std::fs::read_to_string(self.root.path().join("state/ui.token")).expect("token")
    }
    async fn ctl(&self, args: &[&str]) -> Value {
        let o = Command::new(env!("CARGO_BIN_EXE_agentctl"))
            .arg("--socket")
            .arg(self.socket())
            .args(args)
            .output()
            .await
            .expect("ctl");
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        serde_json::from_slice(&o.stdout)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&o.stdout).trim()))
    }
    async fn spawn(&self) {
        self.ctl(&[
            "spawn",
            "a",
            "--kind",
            "claude",
            "--cwd",
            self.root.path().to_str().expect("cwd"),
        ])
        .await;
    }
    async fn raw(&self, path: &str, headers: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", self.port))
            .await
            .expect("tcp");
        s.write_all(format!("GET {path} HTTP/1.1\r\n{headers}\r\n").as_bytes())
            .await
            .expect("request");
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.expect("response");
        String::from_utf8(out).expect("utf8")
    }
    fn host(&self) -> String {
        format!("Host: 127.0.0.1:{}\r\n", self.port)
    }
    async fn get(&self, path: &str) -> String {
        self.raw(
            path,
            &format!("{}Authorization: Bearer {}\r\n", self.host(), self.token()),
        )
        .await
    }
    fn ws_request(&self) -> tokio_tungstenite::tungstenite::http::Request<()> {
        let mut r = format!("ws://127.0.0.1:{}/events", self.port)
            .into_client_request()
            .expect("request");
        r.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&format!("murmur.events.v1, murmur.token.{}", self.token()))
                .expect("protocol"),
        );
        r
    }
    async fn stop(&mut self) {
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.child.id().expect("pid") as i32),
            nix::sys::signal::Signal::SIGTERM,
        )
        .expect("term");
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("shutdown deadline")
            .expect("wait");
        assert!(
            status.success(),
            "shutdown: {}",
            std::fs::read_to_string(self.root.path().join("daemon.log")).expect("log")
        );
    }
}
fn body(response: &str) -> Value {
    serde_json::from_str(response.split_once("\r\n\r\n").expect("HTTP body").1).expect("JSON")
}
async fn next(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) -> Value {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("frame deadline")
            .expect("frame")
            .expect("websocket");
        if m.is_text() {
            return serde_json::from_str(m.to_text().expect("text")).expect("json");
        }
    }
}

#[tokio::test]
async fn real_surface_auth_assets_caps_and_shutdown() {
    let mut d = Daemon::new("yes", 10).await;
    assert_eq!(
        std::fs::metadata(d.root.path().join("state/ui.token"))
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(
        d.ctl(&["ui"])
            .await
            .as_str()
            .expect("url")
            .contains("/#token=")
    );
    assert!(
        d.raw("/", "Host: attacker.example\r\n")
            .await
            .starts_with("HTTP/1.1 403")
    );
    for path in ["/mail", "/messages/00000000000000000000000001"] {
        assert!(d.raw(path, &d.host()).await.starts_with("HTTP/1.1 401"));
    }
    assert!(d.get("/mail").await.contains("Cache-Control: no-store"));
    assert!(d.raw("/", &d.host()).await.contains("script-src 'self'"));
    assert!(
        d.raw("/old.js", &d.host())
            .await
            .starts_with("HTTP/1.1 200")
    );
    for path in [
        "/../config.toml",
        "/%2e%2e/config.toml",
        "/assets/../../config.toml",
    ] {
        assert!(d.raw(path, &d.host()).await.starts_with("HTTP/1.1 404"));
    }
    std::fs::write(d.root.path().join("assets/new.js"), "new").expect("rebuild");
    assert!(
        d.raw("/new.js", &d.host())
            .await
            .starts_with("HTTP/1.1 404")
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    d.raw("/", &d.host()).await;
    assert!(d.raw("/new.js", &d.host()).await.ends_with("new"));
    for case in [
        "no token",
        "wrong token",
        "Host",
        "Origin",
        "empty Origin",
        "key",
        "version",
    ] {
        let mut r = d.ws_request();
        match case {
            "no token" => {
                r.headers_mut().remove("Sec-WebSocket-Protocol");
            }
            "wrong token" => {
                r.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_static("murmur.events.v1,murmur.token.wrong"),
                );
            }
            "Host" => {
                r.headers_mut()
                    .insert("Host", HeaderValue::from_static("evil.example"));
            }
            "Origin" => {
                r.headers_mut()
                    .insert("Origin", HeaderValue::from_static("https://evil.example"));
            }
            "empty Origin" => {
                r.headers_mut()
                    .insert("Origin", HeaderValue::from_static(""));
            }
            "key" => {
                r.headers_mut()
                    .insert("Sec-WebSocket-Key", HeaderValue::from_static("bad"));
            }
            _ => {
                r.headers_mut()
                    .insert("Sec-WebSocket-Version", HeaderValue::from_static("12"));
            }
        }
        let error = connect_async(r).await.expect_err(case);
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("{case}: {error}");
        };
        assert_ne!(response.status(), 101, "{case}");
    }
    let missing_key=d.raw("/events",&format!("{}Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: murmur.events.v1,murmur.token.{}\r\n",d.host(),d.token())).await;
    assert!(missing_key.starts_with("HTTP/1.1 400"));
    let mut watchers = Vec::new();
    for _ in 0..7 {
        let (mut ws, response) = connect_async(d.ws_request()).await.expect("upgrade");
        assert_eq!(
            response.headers()["Sec-WebSocket-Protocol"],
            "murmur.events.v1"
        );
        let frame = next(&mut ws).await;
        assert_eq!(frame["event"], "snapshot");
        watchers.push(ws);
    }
    let mut unix = UnixStream::connect(d.socket()).await.expect("unix");
    write_frame(&mut unix, &json!({"command":"watch"}))
        .await
        .expect("watch");
    let mut unix = BufReader::new(unix);
    read_frame_limit(&mut unix, 64 * 1024 * 1024)
        .await
        .expect("read")
        .expect("snapshot");
    let tokio_tungstenite::tungstenite::Error::Http(r) =
        connect_async(d.ws_request()).await.expect_err("ninth")
    else {
        panic!("expected 503");
    };
    assert_eq!(r.status(), 503);
    assert!(
        std::fs::read_to_string(d.root.path().join("daemon.log"))
            .expect("log")
            .contains("UI enabled with unsandboxed kind")
    );
    d.stop().await;
}

#[tokio::test]
async fn real_watch_resume_restart_and_body_routes() {
    let mut d = Daemon::new("yes", 10).await;
    d.spawn().await;
    let (mut ws, _) = connect_async(d.ws_request()).await.expect("ws");
    let initial = next(&mut ws).await;
    assert_eq!(initial["mail_cursor"], 0);
    assert!(initial["mail"]["anchor"].is_null());
    let stream = UnixStream::connect(d.socket()).await.expect("control");
    let (r, mut w) = stream.into_split();
    let mut r = BufReader::new(r);
    for n in 0..300 {
        write_frame(
            &mut w,
            &json!({"command":"send","to":"@a","body":"mail","key":format!("key-{n}")}),
        )
        .await
        .expect("send");
        read_frame_limit(&mut r, 64 * 1024 * 1024)
            .await
            .expect("response")
            .expect("response");
    }
    let mut anchor = None;
    for _ in 0..100 {
        let v = next(&mut ws).await;
        if v["event"] == "mail" {
            anchor = Some((
                v["seq"].as_i64().expect("seq"),
                v["nonce"].as_str().expect("nonce").to_owned(),
            ));
            break;
        }
    }
    let (cursor, nonce) = anchor.expect("mail after empty snapshot");
    drop(r);
    drop(w);
    drop(ws);
    let page = body(
        &d.get("/mail?states=consumed,offered,queued&limit=1000")
            .await,
    );
    let rows = page["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 300);
    assert!(rows.iter().all(|r| r.get("body").is_none()));
    let id = rows[0]["id"].as_str().expect("id");
    let message = body(&d.get(&format!("/messages/{id}")).await);
    assert_eq!(message["body"], "mail");
    assert_eq!(message["bytes"], 4);
    assert!(!message["sent_at"].is_null());
    let token = d.token();
    d.stop().await;
    d.child = Daemon::start(&d.root);
    d.ready().await;
    assert_eq!(token, d.token());
    let mut request = d.ws_request();
    *request.uri_mut() = format!(
        "ws://127.0.0.1:{}/events?cursor={cursor}&nonce={nonce}",
        d.port
    )
    .parse()
    .expect("URI");
    let (mut ws, _) = connect_async(request).await.expect("resume");
    let snap = next(&mut ws).await;
    assert!(snap["mail"].is_null());
    assert_eq!(snap["mail_cursor"], cursor);
    assert!(snap.get("anchor").is_none());
    let mut seen = 0;
    let mut last = cursor;
    while seen < 300 {
        let v = next(&mut ws).await;
        if v["event"] == "mail" {
            let seq = v["seq"].as_i64().expect("seq");
            assert_eq!(seq, last + 1);
            last = seq;
            seen += 1;
        }
    }
    ws.close(None).await.expect("close");
    d.stop().await;
}

#[tokio::test]
async fn copied_commands_reach_real_actor_and_large_approvals() {
    for key in [
        "quote'key",
        "semi;colon",
        "$(touch INJECTION)",
        "`touch INJECTION`",
        "-leading",
        "--help",
        "space key",
    ] {
        let mut d = Daemon::new(key, if key == "--help" { 7 * 1024 * 1024 } else { 10 }).await;
        d.spawn().await;
        d.ctl(&["send", "@a", "permission"]).await;
        let mut request = None;
        for _ in 0..200 {
            let rows = d.ctl(&["approvals"]).await;
            if rows.as_array().expect("approvals").len() == 1 {
                request = Some(rows[0].clone());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let r = request.expect("permission");
        assert_eq!(r["options"][0]["key"], key);
        if key == "--help" {
            assert!(serde_json::to_vec(&r).expect("serialize").len() > 1024 * 1024);
            let (mut ws, _) = connect_async(d.ws_request()).await.expect("ws");
            let s = next(&mut ws).await;
            assert!(
                serde_json::to_string(&s["approvals"][0]["title"])
                    .expect("title")
                    .len()
                    <= 4096
            );
        }
        let generated = r["options"][0]["command"].as_str().expect("command");
        let output = Command::new("sh")
            .arg("-c")
            .arg(generated)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    std::path::Path::new(env!("CARGO_BIN_EXE_agentctl"))
                        .parent()
                        .expect("bin")
                        .display(),
                    std::env::var("PATH").expect("PATH")
                ),
            )
            .current_dir(d.root.path())
            .output()
            .await
            .expect("sh");
        assert!(
            output.status.success(),
            "key={key}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        // The fake agent appends to the log while this reads it, so only whole lines count.
        let mut exact = false;
        for _ in 0..1000 {
            let wire = std::fs::read_to_string(d.root.path().join("wire.jsonl")).expect("wire");
            let complete = wire.rfind('\n').map_or("", |end| &wire[..end]);
            exact = complete
                .lines()
                .map(|l| serde_json::from_str::<Value>(l).expect("wire JSON"))
                .any(|v| v["result"]["outcome"]["optionId"] == key);
            if exact {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(exact, "actor received exact key {key}");
        assert!(!d.root.path().join("INJECTION").exists());
        assert!(command(&d.socket(), "approve", &["id", "line\nkey"]).is_none());
        for hidden in ["\u{202e}", "\u{2066}", "\u{200b}", "\u{2028}", "\u{feff}"] {
            let key = format!("yes{hidden}key");
            assert!(command(&d.socket(), "approve", &["id", &key]).is_none());
        }
        d.stop().await;
    }
}

#[tokio::test]
async fn invalid_ui_addresses_fail_startup() {
    for address in ["0.0.0.0:7434", "127.0.0.1:0"] {
        let root = tempfile::tempdir().expect("dir");
        let config = root.path().join("config");
        std::fs::write(&config, format!("[ui]\nlisten='{address}'")).expect("config");
        let out = Command::new(env!("CARGO_BIN_EXE_agentd"))
            .arg("--state-dir")
            .arg(root.path().join("state"))
            .arg("--config")
            .arg(config)
            .output()
            .await
            .expect("run");
        assert!(!out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("loopback address and a nonzero port")
        );
    }
}

#[tokio::test]
async fn real_agentctl_watch_reads_large_snapshot_and_held_commands_execute() {
    let mut d = Daemon::new("yes", 10).await;
    d.spawn().await;
    d.stop().await;
    let mut db = rusqlite::Connection::open(d.root.path().join("state/mail.db")).expect("db");
    let recipient: String = db
        .query_row("SELECT id FROM agents LIMIT 1", [], |r| r.get(0))
        .expect("recipient");
    let tx = db.transaction().expect("transaction");
    for n in 1..=1100 {
        let id = format!("\"{n:026}\"");
        tx.execute("INSERT INTO messages(id,sender,client_key,recipient,conversation,hop,act,body,sent_at) VALUES (?,'\"Human\"',?,?,'\"00000000000000000000000001\"',0,'\"Inform\"','body','[2026,1,0,0,0,0,0,0,0]')",rusqlite::params![id,format!("large-{n}"),recipient]).expect("message");
        let seq = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO deliveries(message_id,state,reason,msg_seq) VALUES (?,'held',?,?)",
            rusqlite::params![id, "\0'\\\"".repeat(1024), seq],
        )
        .expect("delivery");
    }
    tx.commit().expect("commit");
    drop(db);
    d.child = Daemon::start(&d.root);
    d.ready().await;
    let mut watch = Command::new(env!("CARGO_BIN_EXE_agentctl"))
        .arg("--socket")
        .arg(d.socket())
        .arg("watch")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("watch");
    let mut stdout = BufReader::new(watch.stdout.take().expect("stdout"));
    let frame = read_frame_limit(&mut stdout, 64 * 1024 * 1024)
        .await
        .expect("frame")
        .expect("snapshot");
    assert!(frame.len() > 1024 * 1024, "snapshot bytes {}", frame.len());
    assert!(frame.len() < 64 * 1024 * 1024);
    let snapshot: Value = serde_json::from_slice(&frame).expect("snapshot");
    assert_eq!(snapshot["mail"]["stuck_truncated"], true);
    assert_eq!(
        snapshot["mail"]["stuck"].as_array().expect("stuck").len(),
        1000
    );
    for (row, verb) in [(0, "release"), (1, "drop")] {
        let command = snapshot["mail"]["stuck"][row]["commands"][verb]
            .as_str()
            .expect("command");
        let o = Command::new("sh")
            .arg("-c")
            .arg(command)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    std::path::Path::new(env!("CARGO_BIN_EXE_agentctl"))
                        .parent()
                        .expect("bin")
                        .display(),
                    std::env::var("PATH").expect("path")
                ),
            )
            .output()
            .await
            .expect("sh");
        assert!(
            o.status.success(),
            "{verb}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
    watch.kill().await.expect("kill watch");
    watch.wait().await.expect("reap");
    d.stop().await;
}

#[tokio::test]
async fn assets_never_follow_swapped_roots_or_symlink_entries() {
    let mut d = Daemon::new("yes", 10).await;
    let assets = d.root.path().join("assets");
    let state = d.root.path().join("state");
    std::os::unix::fs::symlink(state.join("ui.token"), assets.join("file-token"))
        .expect("file symlink");
    std::os::unix::fs::symlink(&state, assets.join("linked-state")).expect("directory symlink");
    d.raw("/", &d.host()).await;
    for path in ["/file-token", "/linked-state/ui.token"] {
        let r = d.raw(path, &d.host()).await;
        assert!(
            !r.contains(&d.token()),
            "symlink entry served ui.token at {path}"
        );
        assert!(r.starts_with("HTTP/1.1 404"));
    }
    std::fs::rename(&assets, d.root.path().join("old-assets")).expect("move root");
    std::os::unix::fs::symlink(&state, &assets).expect("root symlink");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    d.raw("/", &d.host()).await;
    let r = d.raw("/ui.token", &d.host()).await;
    assert!(
        !r.contains(&d.token()),
        "swapped assets root served ui.token"
    );
    assert!(r.starts_with("HTTP/1.1 404"));
    d.stop().await;
}

#[tokio::test]
async fn assets_reject_state_roots_and_oversized_trees() {
    let mut d = Daemon::new("yes", 10).await;
    let huge = std::fs::File::create(d.root.path().join("assets/huge.js")).expect("asset");
    huge.set_len(64 * 1024 * 1024 + 1).expect("large asset");
    assert!(
        d.raw("/", &d.host()).await.starts_with("HTTP/1.1 503"),
        "oversized asset tree must be refused"
    );
    d.ctl(&["ls"]).await;
    d.stop().await;
    for suffix in ["state", "state/nested"] {
        std::fs::create_dir_all(d.root.path().join(suffix)).expect("nested");
        let path = d.root.path().join("config.toml");
        let config = std::fs::read_to_string(&path).expect("config").replace(
            &json!(d.root.path().join("assets")).to_string(),
            &json!(d.root.path().join(suffix)).to_string(),
        );
        std::fs::write(&path, config).expect("config");
        let out = Command::new(env!("CARGO_BIN_EXE_agentd"))
            .arg("--state-dir")
            .arg(d.root.path().join("state"))
            .arg("--config")
            .arg(&path)
            .output()
            .await
            .expect("daemon");
        assert!(!out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("ui.assets must be outside state_dir")
        );
        let config = std::fs::read_to_string(&path).expect("config").replace(
            &json!(d.root.path().join(suffix)).to_string(),
            &json!(d.root.path().join("assets")).to_string(),
        );
        std::fs::write(&path, config).expect("restore config");
    }
}

#[tokio::test]
async fn ui_accept_errors_and_connection_caps_do_not_stop_daemon() {
    let mut d = Daemon::new("yes", 10).await;
    d.stop().await;
    d.child = Command::new("bash")
        .args(["-c", "ulimit -n 40; exec \"$@\"", "m5-ui-fd-limit"])
        .arg(env!("CARGO_BIN_EXE_agentd"))
        .arg("--state-dir")
        .arg(d.root.path().join("state"))
        .arg("--config")
        .arg(d.root.path().join("config.toml"))
        .env("RUST_LOG", "agentd=debug")
        .stderr(Stdio::from(
            std::fs::File::create(d.root.path().join("daemon.log")).expect("log"),
        ))
        .kill_on_drop(true)
        .spawn()
        .expect("limited daemon");
    d.ready().await;
    let mut peers = Vec::new();
    for _ in 0..60 {
        peers.push(
            TcpStream::connect(("127.0.0.1", d.port))
                .await
                .expect("idle UI stream"),
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let log = std::fs::read_to_string(d.root.path().join("daemon.log")).expect("log");
            if log.contains("UI accept failed") {
                break;
            }
            assert!(
                d.child.try_wait().expect("status").is_none(),
                "UI accept failure killed daemon"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("exercise UI accept failure");
    drop(peers);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(d.get("/mail").await.starts_with("HTTP/1.1 200"));
    d.ctl(&["ls"]).await;
    d.stop().await;
}

#[tokio::test]
async fn watch_requires_both_resume_arguments() {
    for args in [["--cursor", "1"], ["--nonce", "abc"]] {
        let out = Command::new(env!("CARGO_BIN_EXE_agentctl"))
            .arg("watch")
            .args(args)
            .output()
            .await
            .expect("CLI");
        assert_eq!(
            out.status.code(),
            Some(2),
            "partial resume must fail argument validation"
        );
        assert!(String::from_utf8_lossy(&out.stderr).contains("required"));
    }
}

#[tokio::test]
async fn ui_ipv6_loopback_and_invalid_addresses() {
    for address in ["localhost:7434", "[2001:db8::1]:7434"] {
        let root = tempfile::tempdir().expect("dir");
        let config = root.path().join("config");
        std::fs::write(&config, format!("[ui]\nlisten='{address}'")).expect("config");
        let out = Command::new(env!("CARGO_BIN_EXE_agentd"))
            .arg("--state-dir")
            .arg(root.path().join("state"))
            .arg("--config")
            .arg(config)
            .output()
            .await
            .expect("run");
        assert!(!out.status.success(), "accepted {address}");
    }
    let mut d = Daemon::new("yes", 10).await;
    d.stop().await;
    let reserve = std::net::TcpListener::bind("[::1]:0").expect("IPv6 loopback available");
    let port = reserve.local_addr().expect("addr").port();
    drop(reserve);
    let path = d.root.path().join("config.toml");
    let cfg = std::fs::read_to_string(&path)
        .expect("config")
        .replace(&format!("127.0.0.1:{}", d.port), &format!("[::1]:{port}"));
    std::fs::write(&path, cfg).expect("IPv6 config");
    d.child = Daemon::start(&d.root);
    d.ready().await;
    let mut stream = TcpStream::connect(("::1", port)).await.expect("IPv6 UI");
    stream
        .write_all(
            format!(
                "GET /mail HTTP/1.1\r\nHost: [::1]:{port}\r\nAuthorization: Bearer {}\r\n\r\n",
                d.token()
            )
            .as_bytes(),
        )
        .await
        .expect("request");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.expect("response");
    assert!(bytes.starts_with(b"HTTP/1.1 200"));
    d.stop().await;
}

#[tokio::test]
async fn maximum_bounds_snapshot_crosses_real_daemon_and_both_clients() {
    use agentd::{model::*, supervisor::AgentSpec};
    let mut d = Daemon::new("yes", 10).await;
    d.stop().await;
    let cfg = d.root.path().join("config.toml");
    let mut config = std::fs::read_to_string(&cfg).expect("config");
    config.push_str("FAKE_BOUND_OPTIONS='1'\n[kind.infinity]\nwire='infinity'\nsocket='/tmp/m5-unused-infinity.sock'\n");
    std::fs::write(cfg, config).expect("config");
    let mut db = rusqlite::Connection::open(d.root.path().join("state/mail.db")).expect("db");
    db.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")
        .expect("pragmas");
    let tx = db.transaction().expect("transaction");
    let ids = (0..1024).map(|_| AgentId::new()).collect::<Vec<_>>();
    for (n, id) in ids.iter().enumerate() {
        let name = AgentName::parse(&format!("agent-{n:026}")).expect("name");
        let spec = AgentSpec {
            name: name.clone(),
            kind: if n == 0 { "claude" } else { "infinity" }.into(),
            cwd: d.root.path().into(),
            workspace: None,
            model: None,
            permissions: Default::default(),
            idle_after: None,
        };
        let key = serde_json::to_string(id).expect("id");
        tx.execute(
            "INSERT INTO agents(id,name) VALUES (?,?)",
            rusqlite::params![key, name.as_str()],
        )
        .expect("agent");
        tx.execute(
            "INSERT INTO agent_specs(agent,spec) VALUES (?,?)",
            rusqlite::params![key, serde_json::to_string(&spec).expect("spec")],
        )
        .expect("spec");
    }
    let mut insert=tx.prepare("INSERT INTO messages(id,sender,client_key,recipient,conversation,hop,act,body,sent_at) VALUES (?,?,?,?,?,65535,'\"Inform\"','body','[2026,1,0,0,0,0,0,0,0]')").expect("message statement");
    let reason = "\0'\\\"".repeat(1024);
    for n in 0..67536 {
        let id = format!("\"{:026}\"", n + 1);
        let from = Sender::Agent(ids[1 + (n / 256) % 256]);
        let to = ids[1 + n % 256];
        insert
            .execute(rusqlite::params![
                id,
                serde_json::to_string(&from).expect("sender"),
                format!("max-{n}"),
                serde_json::to_string(&to).expect("recipient"),
                format!("\"{:026}\"", n + 1)
            ])
            .expect("message");
        let seq = tx.last_insert_rowid();
        let state = if n < 65536 {
            "consumed"
        } else if n < 66536 {
            "held"
        } else {
            "queued"
        };
        tx.execute(
            "INSERT INTO deliveries(message_id,state,reason,msg_seq) VALUES (?,?,?,?)",
            rusqlite::params![
                id,
                state,
                if state == "held" { Some(&reason) } else { None },
                seq
            ],
        )
        .expect("delivery");
    }
    drop(insert);
    tx.commit().expect("commit");
    drop(db);
    d.child = Daemon::start(&d.root);
    d.ready().await;
    d.ctl(&[
        "send",
        "@agent-00000000000000000000000000",
        "max-permissions",
    ])
    .await;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if d.ctl(&["approvals"])
                .await
                .as_array()
                .expect("approvals")
                .len()
                == 64
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all approvals pending");
    fn check(v: &Value, bytes: usize) {
        assert!(
            bytes < 64 * 1024 * 1024,
            "maximum snapshot exceeds reader bound: {bytes}"
        );
        assert_eq!(v["agents"].as_array().expect("agents").len(), 1024);
        assert_eq!(v["approvals"].as_array().expect("approvals").len(), 64);
        assert_eq!(v["mail"]["edges"].as_array().expect("edges").len(), 65536);
        for list in ["stuck", "moving"] {
            assert_eq!(v["mail"][list].as_array().expect("list").len(), 1000);
        }
        for r in v["approvals"].as_array().expect("approvals") {
            assert_eq!(r["options"].as_array().expect("options").len(), 16);
            assert!(serde_json::to_vec(&r["title"]).expect("title").len() <= 4096);
            assert!(serde_json::to_vec(&r["detail"]).expect("detail").len() <= 8192);
        }
    }
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentctl"))
        .arg("--socket")
        .arg(d.socket())
        .arg("watch")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("watch");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));
    let frame = tokio::time::timeout(
        Duration::from_secs(60),
        read_frame_limit(&mut reader, 64 * 1024 * 1024),
    )
    .await
    .expect("snapshot deadline")
    .expect("frame")
    .expect("snapshot");
    let v: Value = serde_json::from_slice(&frame).expect("snapshot JSON");
    check(&v, frame.len());
    println!("MAXIMUM real_agentctl_snapshot_bytes={}", frame.len());
    let mut request = d.ws_request();
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_str(&format!("http://127.0.0.1:{}", d.port)).expect("origin"),
    );
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(64 * 1024 * 1024))
        .max_message_size(Some(64 * 1024 * 1024));
    let (mut ws, _) = tokio_tungstenite::connect_async_with_config(request, Some(config), false)
        .await
        .expect("WebSocket");
    let v = next(&mut ws).await;
    let size = serde_json::to_vec(&v).expect("snapshot").len();
    check(&v, size);
    println!("MAXIMUM real_websocket_snapshot_bytes={size}");
    child.kill().await.expect("stop watch");
    child.wait().await.expect("wait");
    drop(ws);
    d.stop().await;
}

#[tokio::test]
async fn manifest_refresh_is_throttled_and_slow_writes_release_its_lock() {
    let mut d = Daemon::new("yes", 10).await;
    let large = std::fs::File::create(d.root.path().join("assets/large.js")).expect("large");
    large.set_len(8 * 1024 * 1024).expect("size");
    assert!(d.raw("/", &d.host()).await.starts_with("HTTP/1.1 200"));
    std::fs::write(d.root.path().join("assets/later.js"), "later").expect("asset");
    for _ in 0..8 {
        d.raw("/", &d.host()).await;
    }
    assert!(
        d.raw("/later.js", &d.host())
            .await
            .starts_with("HTTP/1.1 404"),
        "rebuilt more than once per second"
    );
    let mut slow = TcpStream::connect(("127.0.0.1", d.port))
        .await
        .expect("slow client");
    slow.write_all(format!("GET /large.js HTTP/1.1\r\n{}\r\n", d.host()).as_bytes())
        .await
        .expect("request");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let response = tokio::time::timeout(Duration::from_secs(1), d.raw("/old.js", &d.host()))
        .await
        .expect("slow response retained manifest lock");
    assert!(response.ends_with("old"));
    d.ctl(&["ls"]).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    d.raw("/", &d.host()).await;
    assert!(d.raw("/later.js", &d.host()).await.ends_with("later"));
    drop(slow);
    d.stop().await;
}

#[tokio::test]
async fn assets_cannot_enclose_state_directory() {
    let mut d = Daemon::new("yes", 10).await;
    d.stop().await;
    let path = d.root.path().join("config.toml");
    let config = std::fs::read_to_string(&path).expect("config").replace(
        &json!(d.root.path().join("assets")).to_string(),
        &json!(d.root.path()).to_string(),
    );
    std::fs::write(path, config).expect("config");
    d.child = Daemon::start(&d.root);
    let status = tokio::time::timeout(Duration::from_secs(3), d.child.wait()).await;
    let rejected = matches!(status,Ok(Ok(status)) if !status.success());
    if !rejected {
        d.raw("/", &d.host()).await;
        let response = d.raw("/state/ui.token", &d.host()).await;
        println!(
            "ENCLOSING_ASSETS unauthenticated_token_exposed={}",
            response.contains(d.token().trim())
        );
        d.child.kill().await.expect("kill unsafe daemon");
    }
    assert!(rejected, "assets root enclosing state_dir was accepted");
    let log = std::fs::read_to_string(d.root.path().join("daemon.log")).expect("log");
    assert!(log.contains("ui.assets must"), "{log}");
}

#[tokio::test]
async fn maximum_all_fields_fixture_crosses_real_agentctl_watch() {
    let dir = tempfile::tempdir().expect("fixture dir");
    let socket = dir.path().join("watch.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("fixture socket");
    let (frame, expected) = tokio::task::spawn_blocking(|| {
        let frame = maximum_fixture::maximum_snapshot();
        let expected = serde_json::to_vec(&frame).expect("serialize fixture");
        (frame, expected)
    })
    .await
    .expect("maximum fixture");
    assert!(expected.len() > 16 * 1024 * 1024 && expected.len() < 64 * 1024 * 1024);
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("watch connection");
        let request = agentd::surfaces::read_frame(&mut BufReader::new(&mut stream))
            .await
            .expect("request")
            .expect("watch");
        assert!(matches!(
            serde_json::from_slice::<agentd::surfaces::ControlRequest>(&request)
                .expect("request JSON"),
            agentd::surfaces::ControlRequest::Watch { .. }
        ));
        let sent = write_frame(&mut stream, &frame).await;
        finish_rx.await.expect("client done");
        sent
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_agentctl"))
        .arg("--socket")
        .arg(socket)
        .arg("watch")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("real watch");
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        read_frame_limit(
            &mut BufReader::new(child.stdout.take().expect("stdout")),
            64 * 1024 * 1024,
        ),
    )
    .await;
    if child.try_wait().expect("watch status").is_none() {
        child.kill().await.expect("stop watch");
    }
    let mut stderr = Vec::new();
    child
        .stderr
        .take()
        .expect("stderr")
        .read_to_end(&mut stderr)
        .await
        .expect("stderr");
    finish_tx.send(()).expect("finish fixture");
    if let Err(error) = server.await.expect("fixture server") {
        eprintln!("fixture transport: {error}");
    }
    let actual = result
        .expect("watch deadline")
        .expect("watch output")
        .unwrap_or_else(|| {
            panic!(
                "real agentctl rejected maximum all-fields frame: {}",
                String::from_utf8_lossy(&stderr)
            )
        });
    assert!(
        actual.strip_suffix(b"\n") == Some(expected.as_slice()),
        "all fields survive the real client"
    );
    println!("MAXIMUM all_fields_real_agentctl_bytes={}", actual.len());
}

#[cfg(not(feature = "bundled-web"))]
#[tokio::test]
async fn without_a_bundle_or_assets_the_root_explains_how_to_build() {
    let mut d = Daemon::new("yes", 10).await;
    d.stop().await;
    let config = std::fs::read_to_string(d.root.path().join("config.toml")).expect("config");
    let config = config
        .lines()
        .filter(|line| !line.starts_with("assets="))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(d.root.path().join("config.toml"), config).expect("config");
    d.child = Daemon::start(&d.root);
    d.ready().await;
    let root = d.raw("/", &d.host()).await;
    assert!(root.starts_with("HTTP/1.1 200"), "{root}");
    assert!(root.contains("npm ci"), "{root}");
    assert!(root.contains("--features bundled-web"), "{root}");
    assert!(root.contains("script-src 'self'"), "{root}");
    assert!(
        d.raw("/assets/index.js", &d.host())
            .await
            .starts_with("HTTP/1.1 404")
    );
    assert!(d.get("/mail").await.starts_with("HTTP/1.1 200"));
    d.stop().await;
}
