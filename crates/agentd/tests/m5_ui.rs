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
        let wire = std::fs::read_to_string(d.root.path().join("wire.jsonl")).expect("wire");
        let exact = wire
            .lines()
            .map(|l| serde_json::from_str::<Value>(l).expect("wire JSON"))
            .any(|v| v["result"]["outcome"]["optionId"] == key);
        assert!(exact, "actor received exact key {key}");
        assert!(!d.root.path().join("INJECTION").exists());
        assert!(command(&d.socket(), "approve", &["id", "line\nkey"]).is_none());
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
