use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};
use std::time::Duration;

use agentd::mailroom::Mailroom;
use agentd::model::AgentName;
use agentd::surfaces::{read_frame, write_frame};
use infinity_protocol::{ClientMessage, RapClaimRecord, ThreadRef};
use serde_json::{Value, json};
use tokio::io::BufReader;
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use uuid::Uuid;

#[expect(
    dead_code,
    reason = "shared fake exposes controls used by other suites"
)]
#[path = "support/fake_infinity.rs"]
mod fake;

struct Test {
    dir: tempfile::TempDir,
    daemon: fake::FakeDaemon,
    child: Child,
    base: String,
    http: reqwest::Client,
    callback_base: String,
    callbacks: mpsc::UnboundedReceiver<Value>,
    callback_status: Arc<AtomicU16>,
    callback_task: tokio::task::JoinHandle<()>,
}
impl Drop for Test {
    fn drop(&mut self) {
        self.callback_task.abort();
    }
}
impl Test {
    async fn new(mail: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("state")).expect("state");
        std::fs::set_permissions(
            dir.path().join("state"),
            std::fs::Permissions::from_mode(0o700),
        )
        .expect("private state");
        let room = Mailroom::open(dir.path().join("state/mail.db"), Default::default())
            .await
            .expect("mailroom");
        room.register(AgentName::parse("receiver").expect("name"))
            .await
            .expect("register receiver");
        room.close().await.expect("close seed mailroom");
        let daemon = fake::FakeDaemon::bind(&dir.path().join("infinity.sock"));
        let address = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("port reservation")
            .local_addr()
            .expect("addr");
        let base = format!("http://{address}");
        let config = format!(
            r#"sandbox = "none"
{mail}
[rap]
listen = "{address}"
[kind.infinity]
wire = "infinity"
socket = {}
"#,
            json!(dir.path().join("infinity.sock"))
        );
        std::fs::write(dir.path().join("kinds.toml"), config).expect("config");
        let child = Self::start(dir.path());
        let (tx, callbacks) = mpsc::unbounded_channel();
        let status = Arc::new(AtomicU16::new(200));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("callback listener");
        let callback_base = format!(
            "http://{}",
            listener.local_addr().expect("callback address")
        );
        let callback_status = status.clone();
        let callback_task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    stream = listener.accept() => {
                        let (stream, _) = stream.expect("callback accept");
                        let tx = tx.clone();
                        let status = status.clone();
                        connections.spawn(async move {
                            let svc = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                                let tx = tx.clone(); let status = status.clone();
                                async move {
                                    use http_body_util::BodyExt;
                                    let body = req.into_body().collect().await.expect("callback body").to_bytes();
                                    if tx.send(serde_json::from_slice(&body).expect("callback JSON")).is_err() { return Err("callback receiver closed"); }
                                    Ok(hyper::Response::builder().status(status.load(Ordering::SeqCst)).body(http_body_util::Full::new(bytes::Bytes::new())).expect("response"))
                                }
                            });
                            if let Err(error) = hyper::server::conn::http1::Builder::new().serve_connection(hyper_util::rt::TokioIo::new(stream), svc).await { eprintln!("callback connection: {error}"); }
                        });
                    }
                    done = connections.join_next(), if !connections.is_empty() => { done.expect("completion").expect("callback connection"); }
                }
            }
        });
        let mut t = Self {
            dir,
            daemon,
            child,
            base,
            callback_base,
            callbacks,
            callback_status,
            callback_task,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("HTTP"),
        };
        t.ready().await;
        let result = t.ctl(json!({"command":"spawn", "spec":{"name":"sender","kind":"infinity","cwd":t.dir.path(),"model":null,"permissions":agentd::model::PermissionPolicy::default(),"idle_after":null},"first_message":null,"key":"spawn"})).await;
        assert!(result.get("error").is_none(), "{result}");
        t.idle().await;
        t
    }
    fn start(dir: &Path) -> Child {
        Command::new(env!("CARGO_BIN_EXE_agentd"))
            .arg("--state-dir")
            .arg(dir.join("state"))
            .arg("--config")
            .arg(dir.join("kinds.toml"))
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join("agentd.log"))
                    .expect("log"),
            ))
            .kill_on_drop(true)
            .spawn()
            .expect("agentd")
    }
    async fn ready(&mut self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    self.child.try_wait().expect("status").is_none(),
                    "{}",
                    std::fs::read_to_string(self.dir.path().join("agentd.log")).expect("log")
                );
                if UnixStream::connect(self.dir.path().join("state/agentd.sock"))
                    .await
                    .is_ok()
                    && self
                        .http
                        .get(format!("{}/.well-known/rap-toolset", self.base))
                        .send()
                        .await
                        .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("agentd ready");
    }
    async fn ctl(&self, value: Value) -> Value {
        let mut wire = UnixStream::connect(self.dir.path().join("state/agentd.sock"))
            .await
            .expect("control");
        write_frame(&mut wire, &value).await.expect("write control");
        let frame = read_frame(&mut BufReader::new(wire))
            .await
            .expect("read control")
            .expect("frame");
        serde_json::from_slice(&frame).expect("control JSON")
    }
    async fn idle(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = self.ctl(json!({"command":"ls"})).await;
                if state["result"][0]["phase"] == "idle" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("sender idle");
    }
    fn record(&self, subthread: bool, body: &str) -> (Uuid, RapClaimRecord) {
        let id = Uuid::new_v4();
        let mut stack = vec![fake::root()];
        if subthread {
            stack.push(ThreadRef::local("child".into()));
        }
        let record = RapClaimRecord {
            thread_stack: stack,
            operation: "send_message".into(),
            arguments: json!({"to":"@receiver","body":body}),
            endpoint: format!("{}/invoke", self.base),
            callback_url: format!("{}/managed/{id}", self.callback_base),
            id: "provider-reuses-this".into(),
            call_id: Some("provider-audit".into()),
        };
        self.daemon
            .state
            .lock()
            .expect("fake state")
            .claims
            .insert(id.to_string(), record.clone());
        (id, record)
    }
    fn body(&self, id: Uuid) -> Value {
        json!({"operation":"forged", "arguments":{"to":"@attacker","body":"tampered"}, "group_id":"forged-root", "thread_ancestors":["forged-ancestor"], "id":"forged-provider-id", "callback_url":format!("http://arbitrary.invalid/managed/{id}")})
    }
    async fn invoke(&self, id: Uuid) -> u16 {
        self.http
            .post(format!("{}/invoke", self.base))
            .json(&self.body(id))
            .send()
            .await
            .expect("invoke")
            .status()
            .as_u16()
    }
    async fn callback(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(5), self.callbacks.recv())
            .await
            .expect("callback timeout")
            .expect("callback")
    }
    fn rows(&self) -> Vec<(String, String, String)> {
        let snapshot = tempfile::tempdir().expect("database snapshot");
        for file in ["mail.db", "mail.db-wal"] {
            std::fs::copy(
                self.dir.path().join("state").join(file),
                snapshot.path().join(file),
            )
            .expect("copy settled database");
        }
        let db =
            rusqlite::Connection::open(snapshot.path().join("mail.db")).expect("database snapshot");
        let mut query = db
            .prepare(
                "SELECT sender,body,id FROM messages WHERE client_key LIKE '%rap:%' ORDER BY seq",
            )
            .expect("query");
        query
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("rows")
    }
    async fn restart(&mut self) {
        self.child.kill().await.expect("kill agentd");
        self.child.wait().await.expect("reap agentd");
        self.child = Self::start(self.dir.path());
        self.ready().await;
        self.idle().await;
    }
}

#[tokio::test]
async fn rap_canonical_root_and_subthread_sends_survive_reconnect() {
    let mut t = Test::new("").await;
    let manifest: Value = t
        .http
        .get(format!("{}/.well-known/rap-toolset", t.base))
        .send()
        .await
        .expect("manifest")
        .json()
        .await
        .expect("JSON");
    assert_eq!(manifest["tools"].as_array().expect("tools").len(), 1);
    assert_eq!(manifest["tools"][0], agentd::surfaces::agent_tools()[0]);
    for path in ["/close_thread", "/cancel_tool_call", "/elsewhere"] {
        assert_eq!(
            t.http
                .post(format!("{}{path}", t.base))
                .send()
                .await
                .expect("unknown route")
                .status(),
            404
        );
    }
    let (root_id, _) = t.record(false, "canonical root body");
    assert_eq!(t.invoke(root_id).await, 200);
    let root = t.callback().await;
    assert_eq!(root["group_id"], "fake-root");
    assert_eq!(root["id"], "provider-reuses-this");
    assert_eq!(root["call_id"], "provider-audit");
    assert!(root["text"].as_str().expect("text").starts_with("Queued"));
    let sender = t.ctl(json!({"command":"ls"})).await["result"][0]["id"]
        .as_str()
        .expect("sender ID")
        .trim_start_matches("agent:")
        .to_owned();
    assert_eq!(t.rows()[0].0, json!({"Agent":sender}).to_string());
    assert_eq!(t.rows()[0].1, "canonical root body");
    t.restart().await;
    let (child_id, _) = t.record(true, "canonical child body");
    assert_eq!(t.invoke(child_id).await, 200);
    let child = t.callback().await;
    assert_eq!(child["group_id"], "child");
    assert_eq!(t.rows().len(), 2);
    assert_eq!(t.rows()[1].0, t.rows()[0].0);
    assert_eq!(t.rows()[1].1, "canonical child body");
}

#[tokio::test]
async fn rap_unverified_calls_and_revoked_bindings_cannot_send() {
    let mut t = Test::new("").await;
    let mut body = t.body(Uuid::new_v4());
    body["callback_url"] = json!("http://127.0.0.1/arbitrary");
    assert_eq!(
        t.http
            .post(format!("{}/invoke", t.base))
            .json(&body)
            .send()
            .await
            .expect("invoke")
            .status(),
        403
    );
    assert_eq!(t.invoke(Uuid::new_v4()).await, 403);
    for change in ["endpoint", "root", "operation"] {
        let (id, mut record) = t.record(false, "rejected");
        match change {
            "endpoint" => record.endpoint = "http://127.0.0.1:1/invoke".into(),
            "root" => record.thread_stack[0] = ThreadRef::local("not-hosted".into()),
            _ => record.operation = "list_agents".into(),
        }
        t.daemon
            .state
            .lock()
            .expect("state")
            .claims
            .insert(id.to_string(), record);
        assert_eq!(t.invoke(id).await, 403, "{change}");
    }
    let (cached_id, _) = t.record(false, "accepted before revocation");
    assert_eq!(t.invoke(cached_id).await, 200);
    t.callback().await;
    let (id, _) = t.record(false, "revoked");
    let session = loop {
        let request = t.daemon.next().await;
        if matches!(request.message, ClientMessage::CreateSession { .. }) {
            break request;
        }
    };
    session.emit(infinity_protocol::DaemonMessage::SessionsUpdated {
        sessions: std::collections::HashMap::from([(
            fake::root(),
            infinity_protocol::SessionInfo {
                title: None,
                last_updated: String::new(),
                total_tokens_used: 0,
                status: infinity_protocol::SessionStatus::Archived,
                threads: vec![],
                remote: None,
            },
        )]),
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if t.ctl(json!({"command":"ls"})).await["result"][0]["phase"] == "needs-recovery" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("binding revoked");
    assert_eq!(t.invoke(id).await, 403);
    assert_eq!(t.invoke(cached_id).await, 403);
    assert_eq!(t.rows().len(), 1);
    assert!(t.callbacks.try_recv().is_err());
}

#[tokio::test]
async fn rap_concurrent_duplicates_replay_and_restart_recover_the_receipt() {
    let mut t = Test::new("").await;
    let (id, _) = t.record(false, "send exactly once");
    let (a, b, c) = tokio::join!(t.invoke(id), t.invoke(id), t.invoke(id));
    assert_eq!((a, b, c), (200, 200, 200));
    let first = t.callback().await;
    assert_eq!(t.callback().await, first);
    assert_eq!(t.callback().await, first);
    // The daemon has settled and no longer has a claim. The completed cache still retries.
    t.daemon
        .state
        .lock()
        .expect("state")
        .claims
        .remove(&id.to_string());
    assert_eq!(t.invoke(id).await, 200);
    assert_eq!(t.callback().await, first);
    assert_eq!(t.rows().len(), 1);
    let (crash_id, record) = t.record(true, "accepted before callback");
    t.callback_status.store(503, Ordering::SeqCst);
    let http = t.http.clone();
    let endpoint = format!("{}/invoke", t.base);
    let body = t.body(crash_id);
    let invoke = tokio::spawn(async move { http.post(endpoint).json(&body).send().await });
    let accepted = t.callback().await;
    assert_eq!(t.rows().len(), 2);
    let receipt = t.rows()[1].2.clone();
    t.restart().await;
    if let Err(error) = invoke.await.expect("invoke task") {
        eprintln!("expected request loss during crash: {error}");
    }
    assert_eq!(
        record.thread_stack.last().expect("child").id.as_str(),
        "child"
    );
    t.callback_status.store(200, Ordering::SeqCst);
    assert_eq!(t.invoke(crash_id).await, 200);
    let recovered = loop {
        let cb = t.callback().await;
        if cb["text"]
            .as_str()
            .expect("text")
            .starts_with("Already queued")
        {
            break cb;
        }
    };
    assert!(
        recovered["text"]
            .as_str()
            .expect("receipt")
            .contains(receipt.trim_matches('"'))
    );
    assert_eq!(accepted["id"], recovered["id"]);
    assert_eq!(t.rows().len(), 2);
}

#[tokio::test]
async fn rap_mail_rejections_return_model_visible_error_text() {
    let mut t = Test::new("[mail]\nper_pair = 1").await;
    let (id, _) = t.record(false, "first");
    assert_eq!(t.invoke(id).await, 200);
    t.callback().await;
    let (id, _) = t.record(true, "over limit");
    assert_eq!(t.invoke(id).await, 200);
    assert!(
        t.callback().await["text"]
            .as_str()
            .expect("error")
            .starts_with("mail limit: per_pair cap 1")
    );
    let (id, mut record) = t.record(false, "missing recipient");
    record.arguments["to"] = json!("@missing");
    t.daemon
        .state
        .lock()
        .expect("state")
        .claims
        .insert(id.to_string(), record);
    assert_eq!(t.invoke(id).await, 200);
    assert!(
        t.callback().await["text"]
            .as_str()
            .expect("error")
            .contains("not found")
    );
    assert_eq!(t.rows().len(), 1);
}

#[tokio::test]
async fn rap_callback_exhaustion_retains_the_completed_result() {
    let mut t = Test::new("").await;
    let (id, _) = t.record(false, "accepted despite failed callbacks");
    t.callback_status.store(503, Ordering::SeqCst);
    assert_eq!(t.invoke(id).await, 200);
    let result = t.callback().await;
    assert_eq!(t.callback().await, result);
    assert_eq!(t.callback().await, result);
    t.daemon
        .state
        .lock()
        .expect("state")
        .claims
        .remove(&id.to_string());
    t.callback_status.store(200, Ordering::SeqCst);
    assert_eq!(t.invoke(id).await, 200);
    assert_eq!(t.callback().await, result);
    assert_eq!(t.rows().len(), 1);
}

#[tokio::test]
async fn rap_configuration_rejects_unsafe_addresses_extra_keys_and_bind_conflicts() {
    use agentd::surfaces::rap::{RapConfig, RapServer};
    for address in ["0.0.0.0:7433", "127.0.0.1:0"] {
        let config = RapConfig {
            listen: address.parse().expect("address"),
        };
        assert!(RapServer::bind(&config, vec![]).await.is_err());
    }
    assert!(
        toml::from_str::<agentd::surfaces::FileConfig>(
            "[rap]\nlisten='127.0.0.1:7433'\nunknown=true"
        )
        .is_err()
    );
    let occupied = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("occupied port");
    let config = RapConfig {
        listen: occupied.local_addr().expect("address"),
    };
    let Err(error) = RapServer::bind(&config, vec![]).await else {
        panic!("must fail to bind")
    };
    assert!(error.to_string().contains("cannot bind RAP listener"));
}
