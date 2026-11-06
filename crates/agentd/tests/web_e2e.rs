//! Browser tests of the bundled graph UI against a real `agentd` process.
//!
//! Each test runs the real binary with `agentd-fake-acp` agents and drives headless
//! Chromium through Playwright, so every response carries the real CSP header and
//! every frame comes from the real feed.
//!
//! This needs the `e2e-web` feature, which builds the UI through npm, and Playwright
//! 1.60's browser (`npx playwright@1.60.0 install chromium`). If the driver download
//! of `playwright-rs` is unusable, follow the note at the top of
//! `crates/infinity-daemon/tests/web_e2e.rs`.
//!
//! Set `AGENTD_UI_SHOTS=<directory>` to write screenshots while the tests run.
#![cfg(feature = "e2e-web")]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use futures_util::StreamExt;
use playwright_rs::protocol::BrowserContextOptions;
use playwright_rs::{Browser, BrowserContext, Locator, Page, Playwright, Viewport, expect};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::process::{Child, Command};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

/// Markup that would run, or fetch, if any surface treated text as HTML.
const HOSTILE: &str =
    "<img src=x onerror=\"window.__pwned=1\"><script>window.__pwned=1</script><b>bold</b>";

const DESKTOP: (u32, u32) = (1280, 800);
const WIDE: (u32, u32) = (1920, 1080);
const PHONE: (u32, u32) = (390, 844);

#[derive(Default)]
struct Setup {
    /// Part of the state directory's path. A control character makes every command null.
    path_prefix: Option<&'static str>,
    env: Vec<(&'static str, String)>,
}

struct Daemon {
    root: tempfile::TempDir,
    child: Child,
    port: u16,
}

impl Daemon {
    async fn new() -> Self {
        Self::with(Setup::default()).await
    }

    async fn with(setup: Setup) -> Self {
        let root = tempfile::Builder::new()
            .prefix(setup.path_prefix.unwrap_or("m5 web "))
            .tempdir()
            .expect("temporary directory");
        std::fs::create_dir(root.path().join("state")).expect("state directory");
        std::fs::set_permissions(
            root.path().join("state"),
            std::fs::Permissions::from_mode(0o700),
        )
        .expect("private state directory");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("reserve a port")
            .local_addr()
            .expect("reserved address")
            .port();
        let mut env = format!("FAKE_ROOT={}\nFAKE_MODE='default'\n", json!(root.path()));
        if !setup.env.iter().any(|(key, _)| *key == "FAKE_OPTION_KEY") {
            env.push_str("FAKE_OPTION_KEY='yes'\n");
        }
        for (key, value) in &setup.env {
            env.push_str(&format!("{key}={}\n", json!(value)));
        }
        let config = format!(
            "sandbox='none'\n[ui]\nlisten='127.0.0.1:{port}'\n[kind.claude]\ncommand=[{}]\n[kind.claude.env]\n{env}",
            json!(env!("CARGO_BIN_EXE_agentd-fake-acp")),
        );
        std::fs::write(root.path().join("config.toml"), config).expect("config");
        let child = Self::launch(&root);
        let mut daemon = Self { root, child, port };
        daemon.ready().await;
        daemon
    }

    fn launch(root: &tempfile::TempDir) -> Child {
        Command::new(env!("CARGO_BIN_EXE_agentd"))
            .arg("--state-dir")
            .arg(root.path().join("state"))
            .arg("--config")
            .arg(root.path().join("config.toml"))
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                std::fs::File::create(root.path().join("daemon.log")).expect("daemon log"),
            ))
            .kill_on_drop(true)
            .spawn()
            .expect("start agentd")
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.root.path().join("daemon.log")).expect("read daemon log")
    }

    fn socket(&self) -> PathBuf {
        self.root.path().join("state/agentd.sock")
    }

    async fn ready(&mut self) {
        for _ in 0..1000 {
            assert!(
                self.child.try_wait().expect("agentd status").is_none(),
                "agentd exited: {}",
                self.log()
            );
            if UnixStream::connect(self.socket()).await.is_ok()
                && TcpStream::connect(("127.0.0.1", self.port)).await.is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("agentd did not start: {}", self.log());
    }

    async fn stop(&mut self) {
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.child.id().expect("agentd pid") as i32),
            nix::sys::signal::Signal::SIGTERM,
        )
        .expect("signal agentd");
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("agentd shutdown deadline")
            .expect("wait for agentd");
        assert!(status.success(), "agentd shutdown: {}", self.log());
    }

    /// Keeps every connection open while agentd answers nothing, so a page can be
    /// watched between losing its feed and getting it back.
    fn signal(&self, signal: &str) {
        let pid = self.child.id().expect("agentd is running").to_string();
        let status = std::process::Command::new("kill")
            .args([signal, &pid])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill {signal} {pid}: {status}");
    }

    async fn restart(&mut self) {
        self.child = Self::launch(&self.root);
        self.ready().await;
    }

    fn token(&self) -> String {
        std::fs::read_to_string(self.root.path().join("state/ui.token"))
            .expect("read ui.token")
            .trim()
            .to_owned()
    }

    fn url(&self, host: &str) -> String {
        format!("http://{host}:{}/#token={}", self.port, self.token())
    }

    async fn ctl(&self, args: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_agentctl"))
            .arg("--socket")
            .arg(self.socket())
            .args(args)
            .output()
            .await
            .expect("run agentctl");
        assert!(
            output.status.success(),
            "agentctl {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&output.stdout).trim()))
    }

    async fn spawn(&self, name: &str) {
        self.ctl(&[
            "spawn",
            name,
            "--kind",
            "claude",
            "--cwd",
            self.root.path().to_str().expect("UTF-8 path"),
        ])
        .await;
    }

    /// The response head of `GET /`, as the listener sends it for this `Host`.
    async fn index_head(&self, host: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port))
            .await
            .expect("connect");
        stream
            .write_all(format!("GET / HTTP/1.1\r\nHost: {host}:{}\r\n\r\n", self.port).as_bytes())
            .await
            .expect("request");
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.expect("response");
        let response = String::from_utf8(out).expect("UTF-8 response");
        let (head, body) = response.split_once("\r\n\r\n").expect("response head");
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        assert!(
            body.contains("/assets/"),
            "the bundle is not embedded: {body}"
        );
        assert!(!body.contains("<style"), "{body}");
        head.to_owned()
    }

    /// The UI's `agentctl ui` URL really is the one this test opens.
    async fn printed_url(&self) -> String {
        self.ctl(&["ui"])
            .await
            .as_str()
            .expect("agentctl ui prints a URL")
            .to_owned()
    }

    fn feed_request(&self) -> tokio_tungstenite::tungstenite::http::Request<()> {
        let mut request = format!("ws://127.0.0.1:{}/events", self.port)
            .into_client_request()
            .expect("feed request");
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&format!("murmur.events.v1, murmur.token.{}", self.token()))
                .expect("feed protocol header"),
        );
        request
    }

    /// A fresh snapshot, read by a client that is not the browser.
    async fn snapshot(&self) -> Value {
        let (mut feed, _) = connect_async(self.feed_request()).await.expect("feed");
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(20), feed.next())
                .await
                .expect("snapshot deadline")
                .expect("snapshot frame")
                .expect("snapshot read");
            if frame.is_text() {
                return serde_json::from_str(frame.to_text().expect("text frame"))
                    .expect("snapshot JSON");
            }
        }
    }

    /// Exact counts per edge, in the form the graph writes to `data-counts`.
    async fn fresh_edges(&self) -> BTreeMap<String, String> {
        let snapshot = self.snapshot().await;
        snapshot["mail"]["edges"]
            .as_array()
            .expect("edges")
            .iter()
            .map(|edge| {
                let counts = ["held", "uncertain", "queued", "offered", "consumed", "dead"]
                    .map(|state| format!("{state}={}", edge[state].as_i64().expect("count")))
                    .join(",");
                (
                    format!(
                        "{}>{}",
                        edge["from"].as_str().expect("from"),
                        edge["to"].as_str().expect("to")
                    ),
                    counts,
                )
            })
            .collect()
    }

    /// Writes held rows directly, with the daemon stopped, as `m5_ui.rs` does. The
    /// database triggers still record their events.
    fn seed_held(&self, range: std::ops::RangeInclusive<usize>, reason: &str) {
        self.seed(range, "held", Some(reason), "body");
    }

    fn seed(
        &self,
        range: std::ops::RangeInclusive<usize>,
        state: &str,
        reason: Option<&str>,
        body: &str,
    ) {
        let mut db = rusqlite::Connection::open(self.root.path().join("state/mail.db"))
            .expect("open mail.db");
        let recipient: String = db
            .query_row("SELECT id FROM agents ORDER BY name LIMIT 1", [], |r| {
                r.get(0)
            })
            .expect("a recipient");
        let tx = db.transaction().expect("transaction");
        for n in range {
            let id = format!("\"{n:026}\"");
            tx.execute(
                "INSERT INTO messages(id,sender,client_key,recipient,conversation,hop,act,body,sent_at) VALUES (?,'\"Human\"',?,?,'\"00000000000000000000000001\"',0,'\"Inform\"',?,'[2026,1,0,0,0,0,0,0,0]')",
                rusqlite::params![id, format!("seed-{n}"), recipient, body],
            )
            .expect("insert message");
            let seq = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO deliveries(message_id,state,reason,msg_seq) VALUES (?,?,?,?)",
                rusqlite::params![id, state, reason, seq],
            )
            .expect("insert delivery");
        }
        tx.commit().expect("commit seed");
    }

    /// Runs a server-built command through a real shell.
    async fn run_command(&self, command: &str) {
        let bin = std::path::Path::new(env!("CARGO_BIN_EXE_agentctl"))
            .parent()
            .expect("agentctl directory");
        let output = Command::new("sh")
            .arg("-c")
            .arg(command)
            .env(
                "PATH",
                format!("{}:{}", bin.display(), std::env::var("PATH").expect("PATH")),
            )
            .output()
            .await
            .expect("run command");
        assert!(
            output.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// Records what a page's own code cannot hide: CSP violations, sockets and frames.
/// This runs through the browser's debugging channel, so CSP does not apply to it.
const INSTRUMENT: &str = r#"
window.__csp = [];
document.addEventListener("securitypolicyviolation", (e) => {
  window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`);
});
window.__sockets = [];
window.__frames = [];
window.__pwned = 0;
window.__dropMail = 0;
window.__dropped = [];
window.__lastSocket = null;
window.__fetches = [];
const realFetch = window.fetch.bind(window);
window.fetch = (target, init) => {
  window.__fetches.push(String(target));
  return realFetch(target, init);
};
const RealWebSocket = window.WebSocket;
window.WebSocket = class extends RealWebSocket {
  constructor(url, protocols) {
    super(url, protocols);
    const socket = window.__sockets.length;
    window.__sockets.push(url);
    window.__lastSocket = this;
    // Registered before the page's own handler, so it can hide a frame from it.
    this.addEventListener("message", (event) => {
      const frame = JSON.parse(event.data);
      if (frame.event === "mail" && window.__dropMail > 0) {
        window.__dropMail -= 1;
        window.__dropped.push(frame.seq);
        event.stopImmediatePropagation();
        return;
      }
      window.__frames.push({
        socket,
        event: frame.event,
        seq: frame.seq ?? null,
        fullMail: frame.event === "snapshot" ? frame.mail !== null : null,
      });
    });
  }
};
"#;

struct Harness {
    _playwright: Playwright,
    browser: Browser,
}

impl Harness {
    async fn launch() -> Self {
        let playwright = Playwright::launch().await.expect("start Playwright");
        let browser = playwright
            .chromium()
            .launch()
            .await
            .expect("start Chromium");
        Self {
            _playwright: playwright,
            browser,
        }
    }

    async fn context(&self, (width, height): (u32, u32), scheme: &str) -> BrowserContext {
        let context = self
            .browser
            .new_context_with_options(
                BrowserContextOptions::builder()
                    .viewport(Viewport { width, height })
                    .device_scale_factor(1.0)
                    .locale("en-US".to_owned())
                    .timezone_id("UTC".to_owned())
                    .color_scheme(scheme.to_owned())
                    .reduced_motion("reduce".to_owned())
                    .build(),
            )
            .await
            .expect("browser context");
        context
            .add_init_script(INSTRUMENT)
            .await
            .expect("instrument pages");
        context
    }

    async fn page(&self, size: (u32, u32)) -> Page {
        let page = self
            .context(size, "dark")
            .await
            .new_page()
            .await
            .expect("new page");
        page.set_default_timeout(30_000.0).await;
        page
    }

    async fn open(&self, url: &str, size: (u32, u32)) -> Page {
        let page = self.page(size).await;
        page.goto(url, None).await.expect("open the UI");
        page
    }
}

async fn eval<T: serde::de::DeserializeOwned>(page: &Page, expression: &str) -> T {
    page.evaluate::<(), T>(expression, None)
        .await
        .unwrap_or_else(|error| panic!("evaluate {expression}: {error}"))
}

async fn id(page: &Page, testid: &str) -> Locator {
    page.get_by_test_id(testid).await
}

async fn visible(page: &Page, testid: &str) {
    expect(id(page, testid).await.first())
        .with_timeout(Duration::from_secs(30))
        .to_be_visible()
        .await
        .unwrap_or_else(|error| panic!("{testid} should be visible: {error}"));
}

async fn app_live(page: &Page) {
    expect(id(page, "app").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_attribute("data-link", "live")
        .await
        .expect("the feed should go live");
}

/// No script from the page ran, nothing was blocked, and nothing threw.
async fn assert_clean(page: &Page, screen: &str) {
    let violations: Vec<String> = eval(page, "window.__csp").await;
    assert_eq!(
        violations,
        Vec::<String>::new(),
        "CSP violations on {screen}"
    );
    assert_eq!(
        page.page_errors(),
        Vec::<String>::new(),
        "page errors on {screen}"
    );
    let pwned: i64 = eval(page, "window.__pwned").await;
    assert_eq!(pwned, 0, "agent text ran as a script on {screen}");
}

/// No horizontal scrolling at this size.
async fn assert_fits(page: &Page, screen: &str) {
    let overflow: i64 = eval(
        page,
        "document.documentElement.scrollWidth - document.documentElement.clientWidth",
    )
    .await;
    assert!(overflow <= 0, "{screen} scrolls sideways by {overflow}px");
}

async fn shot(page: &Page, name: &str) {
    let Some(dir) = std::env::var_os("AGENTD_UI_SHOTS") else {
        return;
    };
    let path = PathBuf::from(dir).join(format!("{name}.png"));
    page.screenshot_to_file(&path, None)
        .await
        .unwrap_or_else(|error| panic!("screenshot {name}: {error}"));
}

async fn dom_edges(page: &Page) -> BTreeMap<String, String> {
    let rows: Vec<(String, String)> = eval(
        page,
        r#"[...document.querySelectorAll('[data-testid="edge"]')].map((e) => [
            `${e.dataset.from}>${e.dataset.to}`, e.dataset.counts,
        ])"#,
    )
    .await;
    rows.into_iter().collect()
}

/// Waits until the graph's counts equal a fresh snapshot's, and returns them.
async fn edges_converge(page: &Page, daemon: &Daemon) -> BTreeMap<String, String> {
    let mut seen = BTreeMap::new();
    let mut expected = BTreeMap::new();
    for _ in 0..200 {
        expected = daemon.fresh_edges().await;
        seen = dom_edges(page).await;
        if seen == expected {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("graph edges {seen:?} never matched a fresh snapshot's {expected:?}");
}

async fn text(page: &Page, testid: &str) -> String {
    id(page, testid)
        .await
        .first()
        .text_content()
        .await
        .unwrap_or_else(|error| panic!("text of {testid}: {error}"))
        .unwrap_or_default()
}

/// Each node's place in the layout, and on screen, as its transform.
async fn positions(page: &Page) -> BTreeMap<String, (f64, f64, String)> {
    let rows: Vec<(String, f64, f64, String)> = eval(
        page,
        r#"[...document.querySelectorAll('[data-testid="node"]')].map((n) => [
            n.dataset.nodeId, Number(n.dataset.x), Number(n.dataset.y), n.getAttribute("transform"),
        ])"#,
    )
    .await;
    rows.into_iter()
        .map(|(node, x, y, transform)| (node, (x, y, transform)))
        .collect()
}

/// The text as rendered, which CSS can change and `textContent` cannot show. A list
/// item has `content-visibility: auto`, so its contents have no rendered text until a
/// frame has been painted near the viewport, and an empty result only means not yet.
async fn rendered_text(locator: &Locator) -> String {
    locator
        .scroll_into_view_if_needed()
        .await
        .expect("scroll into view");
    for _ in 0..600 {
        let rendered = locator.inner_text().await.expect("rendered text");
        if !rendered.is_empty() {
            return rendered;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no rendered text after 30s");
}

/// What a person sees and what they get: the text on screen and the clipboard after a
/// real click.
async fn assert_shows_and_copies(page: &Page, row: &Locator, command: &str) {
    let rendered = rendered_text(&row.locator("[data-testid=command-text]")).await;
    assert_eq!(rendered, command, "the text on screen");
    let copy = row.locator("[data-testid=copy]");
    copy.click(None).await.expect("copy");
    expect(copy)
        .to_contain_text("Copied")
        .await
        .expect("copy feedback");
    let copied: String = eval(page, "navigator.clipboard.readText()").await;
    assert_eq!(copied, command, "the clipboard");
}

async fn edge_rows(page: &Page) -> Vec<(String, String)> {
    eval(
        page,
        r#"[...document.querySelectorAll('[data-testid="edge-message"]')].map(
            (row) => [row.dataset.id, row.querySelector("button span").textContent])"#,
    )
    .await
}

async fn socket_count(page: &Page) -> usize {
    eval::<Vec<String>>(page, "window.__sockets").await.len()
}

/// Hands the page a frame as if it came from its socket.
async fn inject(page: &Page, data: &str) {
    let _: bool = eval(
        page,
        &format!(
            "window.__lastSocket.dispatchEvent(new MessageEvent('message', {{ data: {} }}))",
            json!(data)
        ),
    )
    .await;
}

async fn wait_for_approvals(daemon: &Daemon, count: usize) -> Value {
    for _ in 0..3000 {
        let rows = daemon.ctl(&["approvals"]).await;
        if rows.as_array().expect("approvals").len() >= count {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{count} approvals never appeared");
}

// ── Token handling ───────────────────────────────────────────────────────────

#[tokio::test]
async fn token_moves_to_session_storage_at_both_hosts_with_the_real_csp() {
    let mut daemon = Daemon::new().await;
    let harness = Harness::launch().await;
    let printed = daemon.printed_url().await;
    assert_eq!(printed, daemon.url("127.0.0.1"));
    for host in ["127.0.0.1", "localhost"] {
        let url = daemon.url(host);
        let page = harness.page(DESKTOP).await;
        let response = page
            .goto(&url, None)
            .await
            .expect("open the UI")
            .expect("a response");
        assert_eq!(response.status(), 200);
        let head = daemon.index_head(host).await;
        assert!(head.contains("default-src 'none'"), "{head}");
        assert!(head.contains("script-src 'self'"), "{head}");
        assert!(head.contains("style-src 'self'"), "{head}");
        assert!(
            head.contains(&format!("ws://{host}:{}", daemon.port)),
            "{head}"
        );
        app_live(&page).await;

        let hash: String = eval(&page, "location.hash").await;
        assert_eq!(hash, "", "the token must leave the URL");
        assert!(!page.url().contains(&daemon.token()));
        let stored: String = eval(&page, "sessionStorage.getItem('murmur.ui.token')").await;
        assert_eq!(stored, daemon.token());
        let sockets: Vec<String> = eval(&page, "window.__sockets").await;
        assert_eq!(sockets, [format!("ws://{host}:{}/events", daemon.port)]);
        // No inline script or style reaches the page, and the styles did apply.
        let inline: i64 = eval(
            &page,
            "document.querySelectorAll('script:not([src]), style').length",
        )
        .await;
        assert_eq!(inline, 0);
        let styled: String = eval(
            &page,
            "getComputedStyle(document.querySelector('[data-testid=app]')).display",
        )
        .await;
        assert_ne!(styled, "inline", "the stylesheet did not load");
        assert_clean(&page, host).await;

        page.reload(None).await.expect("reload");
        app_live(&page).await;
        assert!(!page.url().contains('#'));
        assert_clean(&page, "a reload").await;
    }
    daemon.stop().await;
}

#[tokio::test]
async fn an_assets_directory_replaces_the_bundle() {
    let mut daemon = Daemon::new().await;
    daemon.stop().await;
    let assets = daemon.root.path().join("assets");
    std::fs::create_dir(&assets).expect("assets");
    std::fs::write(
        assets.join("index.html"),
        "<!doctype html><title>custom</title>",
    )
    .expect("index");
    let config = daemon.root.path().join("config.toml");
    let text = std::fs::read_to_string(&config).expect("config");
    let text = text.replace("[ui]\n", &format!("[ui]\nassets={}\n", json!(assets)));
    std::fs::write(&config, text).expect("config");
    daemon.restart().await;
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    assert_eq!(page.title().await.expect("title"), "custom");
    assert_eq!(id(&page, "app").await.count().await.expect("app"), 0);
    daemon.stop().await;
}

#[tokio::test]
async fn rejected_or_missing_tokens_tell_the_operator_to_run_agentctl_ui() {
    let mut daemon = Daemon::new().await;
    let harness = Harness::launch().await;
    let base = format!("http://127.0.0.1:{}/", daemon.port);
    let cases = [
        (format!("{base}#token={}", "0".repeat(64)), "rejected"),
        (base.clone(), "missing"),
        (format!("{base}#token=short"), "rejected"),
    ];
    for (url, reason) in cases {
        let page = harness.open(&url, DESKTOP).await;
        visible(&page, "token-screen").await;
        expect(id(&page, "token-screen").await)
            .to_have_attribute("data-reason", reason)
            .await
            .expect("token screen reason");
        expect(id(&page, "token-screen").await)
            .to_contain_text("agentctl ui")
            .await
            .expect("token screen advice");
        let hash: String = eval(&page, "location.hash").await;
        assert_eq!(hash, "", "{reason}: the value must leave the URL");
        let stored: Option<String> = eval(&page, "sessionStorage.getItem('murmur.ui.token')").await;
        assert_eq!(stored, None, "{reason}: nothing should stay stored");
        assert_clean(&page, reason).await;
        shot(&page, &format!("token-{reason}")).await;
    }

    // A token that was valid, then rotated while the page was open.
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    daemon.stop().await;
    std::fs::remove_file(daemon.root.path().join("state/ui.token")).expect("rotate token");
    daemon.restart().await;
    visible(&page, "token-screen").await;
    expect(id(&page, "token-screen").await)
        .to_contain_text("agentctl ui")
        .await
        .expect("rotated token advice");
    assert_clean(&page, "a rotated token").await;
    daemon.stop().await;
}

// ── Hostile content ──────────────────────────────────────────────────────────

#[tokio::test]
async fn agent_authored_text_is_inert_on_every_surface() {
    let mut daemon = Daemon::with(Setup {
        env: vec![
            ("FAKE_TITLE_TEXT", HOSTILE.to_owned()),
            ("FAKE_ACTIVITY_TEXT", HOSTILE.to_owned()),
        ],
        ..Setup::default()
    })
    .await;
    daemon.spawn("a").await;
    daemon.stop().await;
    daemon.seed_held(1..=1, HOSTILE);
    daemon.restart().await;

    // Activity is volatile, so the page has to be open when the turn starts.
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    daemon
        .ctl(&["send", "@a", &format!("permission {HOSTILE}")])
        .await;
    wait_for_approvals(&daemon, 1).await;
    visible(&page, "permission").await;

    // Title and reason, as visible text.
    assert_eq!(text(&page, "permission-title").await, HOSTILE);
    assert_eq!(text(&page, "stuck-reason").await, HOSTILE);

    // Activity, through the agent's detail.
    let agent: String = eval(
        &page,
        r#"document.querySelector('[data-testid="node"]:not([data-node-id="human"])').dataset.nodeId"#,
    )
    .await;
    page.locator(&format!("[data-testid=node][data-node-id='{agent}']"))
        .await
        .click(None)
        .await
        .expect("select the agent");
    visible(&page, "activity").await;
    expect(id(&page, "activity").await)
        .to_contain_text(HOSTILE)
        .await
        .expect("activity text");
    shot(&page, "hostile-agent").await;

    // A body, fetched on demand through an edge.
    page.locator(&format!(
        "[data-testid=edge][data-from=human][data-to='{agent}']"
    ))
    .await
    .click(None)
    .await
    .expect("select the edge");
    visible(&page, "edge-detail").await;
    id(&page, "edge-message")
        .await
        .first()
        .click(None)
        .await
        .expect("open a message");
    visible(&page, "message-body").await;
    expect(id(&page, "message-body").await)
        .to_contain_text("<img src=x onerror=")
        .await
        .expect("body text");
    shot(&page, "hostile-edge").await;

    let elements: i64 = eval(
        &page,
        r#"document.querySelectorAll(
            '[data-testid=app] img, [data-testid=app] script, [data-testid=app] b, [data-testid=app] iframe'
        ).length"#,
    )
    .await;
    assert_eq!(elements, 0, "agent text became elements");
    assert_clean(&page, "hostile content").await;
    daemon.stop().await;
}

#[tokio::test]
async fn the_csp_listener_would_catch_a_violation() {
    // A control for `assert_clean`: without this, an absent listener passes everything.
    let mut daemon = Daemon::new().await;
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    let _: () = eval(
        &page,
        r#"(() => {
            const img = document.createElement("img");
            img.src = "http://127.0.0.2:9/blocked.png";
            document.body.append(img);
            const style = document.createElement("style");
            style.textContent = "body { color: red }";
            document.head.append(style);
        })()"#,
    )
    .await;
    for _ in 0..100 {
        let violations: Vec<String> = eval(&page, "window.__csp").await;
        if violations.len() >= 2 {
            assert!(
                violations.iter().any(|v| v.starts_with("img-src")),
                "{violations:?}"
            );
            assert!(
                violations.iter().any(|v| v.starts_with("style-src")),
                "{violations:?}"
            );
            daemon.stop().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the real CSP did not report the planted violations");
}

// ── Attention panel ──────────────────────────────────────────────────────────

#[tokio::test]
async fn permission_and_held_mail_show_the_servers_commands_and_they_work() {
    // HTML collapses a run of spaces unless told not to, which would make a command
    // on screen differ from the one copied. One run is in an option key, and the
    // state directory's path puts one in every command's socket path.
    let mut daemon = Daemon::with(Setup {
        path_prefix: Some("m5  web "),
        env: vec![("FAKE_OPTION_KEY", "allow  once".to_owned())],
    })
    .await;
    daemon.spawn("a").await;
    daemon.stop().await;
    daemon.seed_held(1..=2, "hop limit reached");
    daemon.restart().await;
    daemon.ctl(&["send", "@a", "permission"]).await;
    let approvals = wait_for_approvals(&daemon, 1).await;

    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    visible(&page, "permission").await;
    visible(&page, "stuck-held").await;
    shot(&page, "attention-desktop").await;
    assert!(text(&page, "stat-held").await.starts_with('2'));

    page.context()
        .expect("page context")
        .grant_permissions(&["clipboard-read", "clipboard-write"], None)
        .await
        .expect("clipboard permission");

    // Each command on screen, under the option it belongs to, is exactly what the
    // server sent for that option, and copying it gives those bytes.
    let snapshot = daemon.snapshot().await;
    let options = snapshot["approvals"][0]["options"]
        .as_array()
        .expect("options");
    assert_eq!(
        options.len(),
        2,
        "this test needs more than the first option"
    );
    assert_eq!(approvals[0]["options"][0]["command"], options[0]["command"]);
    assert!(
        options[0]["command"]
            .as_str()
            .expect("command")
            .contains("  "),
        "this test needs a run of spaces in a command: {options:?}"
    );
    let permission = id(&page, "permission").await;
    let rows = permission.locator("[data-testid=command]");
    assert_eq!(rows.count().await.expect("command rows"), options.len());
    for (index, (option, effect)) in options
        .iter()
        .zip(["allow once", "reject once"])
        .enumerate()
    {
        let row = rows.nth(i32::try_from(index).expect("bug: a small index"));
        // The authoritative effect and the ACP server's own words are separate.
        assert_eq!(
            rendered_text(&row.locator("[data-testid=option-effect]")).await,
            effect
        );
        assert_eq!(
            rendered_text(&row.locator("bdi[data-testid=option-label]")).await,
            option["label"].as_str().expect("label")
        );
        assert_shows_and_copies(&page, &row, option["command"].as_str().expect("command")).await;
    }

    // The same for held mail, by message and by verb. These rows scroll instead of
    // wrapping, which must not change what they show either.
    let stuck = snapshot["mail"]["stuck"].as_array().expect("stuck rows");
    assert_eq!(stuck.len(), 2);
    for held in stuck {
        let item = page
            .locator(&format!(
                "[data-testid=stuck-held][data-id='{}']",
                held["id"].as_str().expect("message id")
            ))
            .await;
        let rows = item.locator("[data-testid=command]");
        assert_eq!(rows.count().await.expect("held command rows"), 2);
        for (index, (verb, label)) in [("release", "Release"), ("drop", "Drop")]
            .into_iter()
            .enumerate()
        {
            let row = rows.nth(i32::try_from(index).expect("bug: a small index"));
            expect(row.locator("span").first())
                .to_have_text(label)
                .await
                .expect("the verb on this row");
            let command = held["commands"][verb].as_str().expect("held command");
            assert!(
                command.contains("  "),
                "this test needs a run of spaces in a socket path: {command}"
            );
            assert_shows_and_copies(&page, &row, command).await;
        }
    }
    assert_eq!(
        id(&page, "no-command")
            .await
            .count()
            .await
            .expect("no commands"),
        0
    );

    // Running them through a shell clears the rows, with no action from the page.
    let release = snapshot["mail"]["stuck"][0]["commands"]["release"]
        .as_str()
        .expect("release command");
    daemon.run_command(release).await;
    let approve = snapshot["approvals"][0]["options"][0]["command"]
        .as_str()
        .expect("approve command");
    daemon.run_command(approve).await;
    expect(id(&page, "permission").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_count(0)
        .await
        .expect("the answered permission should leave");
    expect(id(&page, "stuck-held").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_count(1)
        .await
        .expect("the released row should leave");
    edges_converge(&page, &daemon).await;
    assert_clean(&page, "the attention panel").await;
    daemon.stop().await;
}

#[tokio::test]
async fn a_null_command_has_no_copy_button() {
    // A newline in the socket path makes the server refuse to build any command.
    let mut daemon = Daemon::with(Setup {
        path_prefix: Some("m5\nweb"),
        ..Setup::default()
    })
    .await;
    daemon.spawn("a").await;
    daemon.stop().await;
    daemon.seed_held(1..=1, "hop limit reached");
    daemon.restart().await;
    daemon.ctl(&["send", "@a", "permission"]).await;
    wait_for_approvals(&daemon, 1).await;
    let snapshot = daemon.snapshot().await;
    assert!(snapshot["approvals"][0]["options"][0]["command"].is_null());
    assert!(snapshot["mail"]["stuck"][0]["commands"]["release"].is_null());

    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    visible(&page, "permission").await;
    visible(&page, "stuck-held").await;
    visible(&page, "no-command").await;
    assert_eq!(
        id(&page, "copy").await.count().await.expect("copy buttons"),
        0
    );
    assert_eq!(
        id(&page, "command-text")
            .await
            .count()
            .await
            .expect("commands"),
        0
    );
    assert_eq!(
        id(&page, "command").await.count().await.expect("commands"),
        0
    );
    shot(&page, "attention-null-commands").await;
    assert_clean(&page, "null commands").await;
    daemon.stop().await;
}

// ── Reconnect ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_reconnect_echoes_its_anchor_and_loses_nothing() {
    let mut daemon = Daemon::new().await;
    daemon.spawn("a").await;
    daemon.stop().await;
    daemon.seed_held(1..=3, "before the outage");
    daemon.restart().await;

    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    edges_converge(&page, &daemon).await;
    let anchor = daemon.snapshot().await["mail"]["anchor"].clone();
    let cursor = anchor["seq"].as_i64().expect("anchor seq");
    let nonce = anchor["nonce"].as_str().expect("anchor nonce").to_owned();
    assert!(cursor > 0);
    expect(id(&page, "app").await)
        .to_have_attribute("data-cursor", &cursor.to_string())
        .await
        .expect("the client's anchor");

    daemon.stop().await;
    visible(&page, "disconnected").await;
    shot(&page, "disconnected").await;
    assert_clean(&page, "the disconnected screen").await;

    // Every one of these commits while the page cannot see them.
    daemon.seed_held(4..=9, "during the outage");
    daemon.restart().await;
    edges_converge(&page, &daemon).await;
    app_live(&page).await;
    assert_eq!(
        id(&page, "disconnected")
            .await
            .count()
            .await
            .expect("banner"),
        0
    );

    let sockets: Vec<String> = eval(&page, "window.__sockets").await;
    let expected = format!(
        "ws://127.0.0.1:{}/events?cursor={cursor}&nonce={nonce}",
        daemon.port
    );
    assert_eq!(
        sockets.iter().find(|url| url.contains("cursor=")),
        Some(&expected),
        "resume must echo the anchor verbatim: {sockets:?}"
    );

    // The server accepted the resume: no full snapshot, and each missed event once.
    let frames: Vec<Value> = eval(&page, "window.__frames").await;
    let last_socket = frames.last().expect("frames")["socket"].as_i64();
    let resumed: Vec<&Value> = frames
        .iter()
        .filter(|f| f["socket"].as_i64() == last_socket)
        .collect();
    assert_eq!(resumed[0]["event"], "snapshot");
    assert_eq!(resumed[0]["fullMail"], false, "{resumed:?}");
    let seqs: Vec<i64> = resumed
        .iter()
        .filter(|f| f["event"] == "mail")
        .map(|f| f["seq"].as_i64().expect("seq"))
        .collect();
    assert_eq!(seqs, ((cursor + 1)..=(cursor + 6)).collect::<Vec<_>>());

    assert!(text(&page, "stat-held").await.contains('9'));
    expect(id(&page, "stuck-held").await)
        .to_have_count(9)
        .await
        .expect("all nine rows");
    assert_clean(&page, "the resumed screen").await;
    daemon.stop().await;
}

#[tokio::test]
async fn a_lost_mail_frame_costs_one_full_snapshot_and_no_reconnect_loop() {
    let mut daemon = Daemon::new().await;
    daemon.spawn("a").await;
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    daemon.ctl(&["send", "@a", "before"]).await;
    edges_converge(&page, &daemon).await;
    assert_eq!(socket_count(&page).await, 1);

    // The page never sees one frame of this traffic.
    let _: i64 = eval(&page, "window.__dropMail = 1").await;
    daemon
        .ctl(&["send", "@a", "the page loses part of this"])
        .await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while socket_count(&page).await < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the page should notice the gap and reconnect");
    app_live(&page).await;
    edges_converge(&page, &daemon).await;
    let dropped: Vec<i64> = eval(&page, "window.__dropped").await;
    assert_eq!(dropped.len(), 1);
    let sockets: Vec<String> = eval(&page, "window.__sockets").await;
    assert_eq!(
        sockets[1],
        format!("ws://127.0.0.1:{}/events", daemon.port),
        "a resync must not echo the anchor: {sockets:?}"
    );
    let frames: Vec<Value> = eval(&page, "window.__frames").await;
    let first = frames
        .iter()
        .find(|f| f["socket"].as_i64() == Some(1))
        .expect("a frame on the new socket");
    assert_eq!(first["event"], "snapshot");
    assert_eq!(first["fullMail"], true);

    // Real traffic after that follows on, with no more reconnects.
    for n in 0..5 {
        daemon.ctl(&["send", "@a", &format!("after {n}")]).await;
    }
    edges_converge(&page, &daemon).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(socket_count(&page).await, 2, "one resync, then none");
    assert_clean(&page, "a resync").await;
    daemon.stop().await;
}

#[tokio::test]
async fn a_malformed_frame_says_so_and_recovers_and_an_unknown_one_is_ignored() {
    let mut daemon = Daemon::new().await;
    daemon.spawn("a").await;
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    daemon.ctl(&["send", "@a", "before"]).await;
    edges_converge(&page, &daemon).await;

    // From a newer server: nothing happens.
    inject(&page, r#"{"event":"from_the_future","anything":[1]}"#).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(socket_count(&page).await, 1);
    expect(id(&page, "feed-problem").await)
        .to_have_count(0)
        .await
        .expect("an unknown event is not a problem");

    // A frame this page cannot use. agentd answers nothing for now, so the page stays
    // between losing its feed and getting it back, and the operator can see why.
    daemon.signal("-STOP");
    inject(&page, r#"{"event":"snapshot"}"#).await;
    visible(&page, "feed-problem").await;
    shot(&page, "feed-problem").await;
    assert_clean(&page, "the feed problem screen").await;

    // It reconnects without the anchor, and the snapshot clears the problem.
    daemon.signal("-CONT");
    expect(id(&page, "feed-problem").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_count(0)
        .await
        .expect("the problem clears with the snapshot");
    app_live(&page).await;
    edges_converge(&page, &daemon).await;
    let sockets: Vec<String> = eval(&page, "window.__sockets").await;
    assert_eq!(
        sockets.last().expect("sockets"),
        &format!("ws://127.0.0.1:{}/events", daemon.port),
        "{sockets:?}"
    );
    assert_clean(&page, "a malformed frame").await;
    daemon.stop().await;
}

#[tokio::test]
async fn an_edges_rows_keep_up_with_traffic_and_with_changes_no_count_shows() {
    let mut daemon = Daemon::new().await;
    daemon.spawn("a").await;
    daemon.stop().await;
    daemon.seed_held(1..=1, "held for this test");
    daemon.restart().await;
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    visible(&page, "stuck-held").await;
    let agent: String = eval(
        &page,
        r#"document.querySelector('[data-testid="node"]:not([data-node-id="human"])').dataset.nodeId"#,
    )
    .await;
    page.locator(&format!(
        "[data-testid=edge][data-from=human][data-to='{agent}']"
    ))
    .await
    .click(None)
    .await
    .expect("select the edge");
    visible(&page, "edge-message").await;
    assert_eq!(edge_rows(&page).await.len(), 1);

    // A change while the connection is down that moves neither the edge's total nor
    // its stuck count. A history that no longer has the page's anchor means a full
    // snapshot, so no frame arrives for this edge: only the reconnect can say to look.
    let held = edge_rows(&page)
        .await
        .into_iter()
        .find(|(_, state)| state == "held")
        .expect("the held row")
        .0;
    daemon.stop().await;
    visible(&page, "disconnected").await;
    let db =
        rusqlite::Connection::open(daemon.root.path().join("state/mail.db")).expect("open mail.db");
    let changed = db
        .execute(
            "UPDATE deliveries SET state='uncertain',incarnation=1,reason=NULL WHERE message_id=?",
            [serde_json::to_string(&held).expect("bug: a string serializes")],
        )
        .expect("change the delivery");
    assert_eq!(changed, 1);
    db.execute(
        "UPDATE mail_events SET nonce=lower(hex(randomblob(16)))",
        [],
    )
    .expect("replace the history's nonces");
    drop(db);
    daemon.restart().await;
    app_live(&page).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows = edge_rows(&page).await;
            if rows
                .iter()
                .any(|(id, state)| *id == held && state == "uncertain")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the row should follow a change that keeps every count");
    let frames: Vec<Value> = eval(&page, "window.__frames").await;
    let last_socket = frames.last().expect("frames")["socket"].clone();
    let resumed: Vec<&Value> = frames
        .iter()
        .filter(|f| f["socket"] == last_socket)
        .collect();
    assert_eq!(resumed[0]["fullMail"], true, "{resumed:?}");
    assert!(
        resumed.iter().all(|f| f["event"] != "mail"),
        "this needs a reconnect with no mail frame: {resumed:?}"
    );

    // Traffic with no quiet moment: the list follows it anyway. A debounce would
    // still show one row when this loop ends.
    const SENT: usize = 16;
    for n in 0..SENT {
        daemon.ctl(&["send", "@a", &format!("traffic {n}")]).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let during = edge_rows(&page).await.len();
    assert!(
        during >= 5,
        "only {during} rows after {SENT} sends in continuous traffic"
    );

    // An invalidation during the last interval is not lost: the list ends up exact.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows = edge_rows(&page).await;
            let consumed = rows.iter().filter(|(_, state)| state == "consumed").count();
            if rows.len() == SENT + 1 && consumed == SENT {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the rows should settle on what the server has");

    // The same on a live connection. Nothing this frame changes can be seen in those
    // counts, so its arrival is what must cause the next read.
    let cursor: i64 = eval(
        &page,
        r#"Number(document.querySelector('[data-testid="app"]').dataset.cursor)"#,
    )
    .await;
    let reads = |fetches: Vec<String>| {
        fetches
            .iter()
            .filter(|target| target.contains("/mail?from="))
            .count()
    };
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let before = reads(eval(&page, "window.__fetches").await);
    let frame = json!({
        "event": "mail",
        "seq": cursor + 1,
        "nonce": "0".repeat(32),
        "msg_seq": 1,
        "id": held,
        "from": "human",
        "to": agent,
        "conversation": "00000000000000000000000001",
        "in_reply_to": null,
        "hop": 0,
        "act": "inform",
        "task": null,
        "exposure": null,
        "state": "held",
        "turn_started": false,
        "incarnation": null,
        "attempts": 0,
        "reason": "a reason",
        "prev_state": "uncertain",
        "at": "2026-01-01T00:00:00.000Z",
    });
    inject(&page, &frame.to_string()).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while reads(eval(&page, "window.__fetches").await) == before {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a frame on this edge should cause a read");
    assert_clean(&page, "an edge under traffic").await;
    daemon.stop().await;
}

#[tokio::test]
async fn a_feed_refused_at_capacity_says_so_and_recovers() {
    let mut daemon = Daemon::new().await;
    let mut feeds = Vec::new();
    for _ in 0..8 {
        let (feed, _) = connect_async(daemon.feed_request()).await.expect("feed");
        feeds.push(feed);
    }
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    visible(&page, "disconnected").await;
    expect(id(&page, "disconnected").await)
        .with_timeout(Duration::from_secs(30))
        .to_contain_text("refused")
        .await
        .expect("a refusal should say so");
    assert_eq!(
        id(&page, "token-screen")
            .await
            .count()
            .await
            .expect("token"),
        0
    );
    drop(feeds);
    app_live(&page).await;
    assert_clean(&page, "a refused feed").await;
    daemon.stop().await;
}

// ── Graph ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_graph_does_not_jump_when_traffic_or_agents_change() {
    let mut daemon = Daemon::new().await;
    for name in ["a", "b", "c", "d", "e"] {
        daemon.spawn(name).await;
    }
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), WIDE).await;
    app_live(&page).await;
    visible(&page, "node").await;
    expect(id(&page, "node").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_count(6)
        .await
        .expect("five agents and the human");
    let before = positions(&page).await;
    shot(&page, "graph-5-agents").await;

    // New edges, phase changes, counts.
    for name in ["@a", "@b", "@c", "@d", "@e", "@a", "@b"] {
        daemon.ctl(&["send", name, "hello"]).await;
    }
    let edges = edges_converge(&page, &daemon).await;
    assert_eq!(edges.len(), 5);
    assert_eq!(positions(&page).await, before, "traffic moved a node");

    daemon.ctl(&["send", "@a", "hold"]).await;
    expect(page.locator("[data-testid=node][data-phase=busy]").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_count(1)
        .await
        .expect("one busy agent");
    assert_eq!(
        positions(&page).await,
        before,
        "a phase change moved a node"
    );

    // A new agent keeps every known node's place in the layout.
    daemon.spawn("f").await;
    expect(id(&page, "node").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_count(7)
        .await
        .expect("the new agent");
    let after = positions(&page).await;
    for (node, place) in &before {
        let moved = after.get(node).expect("known node stays");
        assert_eq!(moved, place, "{node} moved");
    }

    assert_clean(&page, "the graph").await;
    std::fs::write(daemon.root.path().join("release"), "release").expect("release agent");
    daemon.stop().await;
}

#[tokio::test]
async fn an_empty_daemon_says_how_to_start() {
    let mut daemon = Daemon::new().await;
    let harness = Harness::launch().await;
    for (name, size) in [("desktop", DESKTOP), ("phone", PHONE)] {
        let page = harness.open(&daemon.url("127.0.0.1"), size).await;
        app_live(&page).await;
        visible(&page, "empty").await;
        visible(&page, "attention-empty").await;
        expect(id(&page, "empty").await)
            .to_contain_text("agentctl spawn")
            .await
            .expect("advice");
        assert_fits(&page, name).await;
        assert_clean(&page, "the empty screen").await;
        shot(&page, &format!("empty-{name}")).await;
    }
    daemon.stop().await;
}

// ── Failure states ───────────────────────────────────────────────────────────

#[tokio::test]
async fn failed_and_needs_recovery_are_unmistakable() {
    let mut daemon = Daemon::with(Setup {
        env: vec![("FAKE_LOAD_ERROR", format!("load refused {HOSTILE}"))],
        ..Setup::default()
    })
    .await;
    daemon.spawn("healthy").await;
    daemon.spawn("recovering").await;
    daemon.ctl(&["send", "@recovering", "first"]).await;
    for _ in 0..500 {
        if daemon.root.path().join("prompt-1").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;

    // A crash forces a session load, which the agent refuses.
    std::fs::write(daemon.root.path().join("fail-load"), "").expect("fail load");
    daemon.ctl(&["send", "@recovering", "die-before"]).await;
    let recovery = page
        .locator("[data-testid=node][data-phase=needs-recovery]")
        .await;
    expect(recovery.clone())
        .with_timeout(Duration::from_secs(60))
        .to_be_visible()
        .await
        .expect("needs recovery");
    let words = recovery.text_content().await.expect("node text");
    assert!(
        words
            .as_deref()
            .is_some_and(|w| w.contains("NEEDS RECOVERY")),
        "a state in words, not only color: {words:?}"
    );
    visible(&page, "broken-agent").await;
    expect(id(&page, "agent-error").await)
        .to_contain_text("load refused <img")
        .await
        .expect("the reason, as text");

    // Five crashes while opening fail an agent for good.
    std::fs::remove_file(daemon.root.path().join("fail-load")).expect("clear flag");
    std::fs::write(daemon.root.path().join("crash-open"), "").expect("crash on open");
    daemon.spawn("broken").await;
    let failed = page.locator("[data-testid=node][data-phase=failed]").await;
    expect(failed.clone())
        .with_timeout(Duration::from_secs(90))
        .to_be_visible()
        .await
        .expect("failed");
    let words = failed.text_content().await.expect("node text");
    assert!(
        words.as_deref().is_some_and(|w| w.contains("FAILED")),
        "a state in words, not only color: {words:?}"
    );
    assert!(text(&page, "stat-failed").await.starts_with('1'));
    assert!(text(&page, "stat-recovery").await.starts_with('1'));
    shot(&page, "failed-and-recovery").await;
    assert_clean(&page, "broken agents").await;
    daemon.stop().await;
}

// ── Truncated states ─────────────────────────────────────────────────────────

#[tokio::test]
async fn a_truncated_stuck_list_shows_the_exact_count_and_follows_a_refetch() {
    let mut daemon = Daemon::new().await;
    daemon.spawn("a").await;
    daemon.stop().await;
    daemon.seed_held(1..=1100, "hop limit reached");
    daemon.restart().await;
    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    expect(id(&page, "app").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_attribute("data-stuck-mode", "view")
        .await
        .expect("view mode");
    visible(&page, "view-note-held").await;
    expect(id(&page, "view-note-held").await)
        .to_contain_text("1,100")
        .await
        .expect("the exact count");
    assert!(text(&page, "stat-held").await.contains("1,100"));
    shot(&page, "truncated-held").await;

    // A fresh read decides the newest row. Removing it shows through a refetch.
    let snapshot = daemon.snapshot().await;
    let newest = &snapshot["mail"]["stuck"][0];
    let newest_id = newest["id"].as_str().expect("id").to_owned();
    let first_row = id(&page, "stuck-held").await.first();
    expect(first_row.clone())
        .with_timeout(Duration::from_secs(30))
        .to_have_attribute("data-id", &newest_id)
        .await
        .expect("the newest row");
    daemon
        .run_command(newest["commands"]["drop"].as_str().expect("drop"))
        .await;
    edges_converge(&page, &daemon).await;
    expect(id(&page, "view-note-held").await)
        .with_timeout(Duration::from_secs(30))
        .to_contain_text("1,099")
        .await
        .expect("the exact count after a drop");
    let next_id = daemon.snapshot().await["mail"]["stuck"][0]["id"]
        .as_str()
        .expect("next id")
        .to_owned();
    expect(first_row)
        .with_timeout(Duration::from_secs(30))
        .to_have_attribute("data-id", &next_id)
        .await
        .expect("the list refetched");
    assert_clean(&page, "a truncated list").await;
    daemon.stop().await;
}

#[tokio::test]
async fn truncated_moving_and_approval_lists_are_labelled() {
    let mut daemon = Daemon::new().await;
    daemon.spawn("a").await;
    daemon.spawn("b").await;
    daemon.spawn("c").await;
    // An agent held in its first turn leaves the rest of its queue waiting.
    daemon.stop().await;
    daemon.seed(1..=1100, "queued", None, "hold");
    daemon.restart().await;
    daemon.ctl(&["send", "@b", "max-permissions"]).await;
    daemon.ctl(&["send", "@c", "max-permissions"]).await;
    wait_for_approvals(&daemon, 128).await;
    let snapshot = daemon.snapshot().await;
    assert_eq!(snapshot["approvals_truncated"], true);
    assert_eq!(snapshot["mail"]["moving_truncated"], true);

    let harness = Harness::launch().await;
    let page = harness.open(&daemon.url("127.0.0.1"), DESKTOP).await;
    app_live(&page).await;
    expect(id(&page, "app").await)
        .with_timeout(Duration::from_secs(30))
        .to_have_attribute("data-moving-mode", "view")
        .await
        .expect("moving view mode");
    visible(&page, "approvals-truncated").await;
    expect(id(&page, "approvals-truncated").await)
        .to_contain_text("agentctl approvals")
        .await
        .expect("where the rest are");

    // 64 permissions, each with its commands, start as a page and grow on request.
    expect(id(&page, "permission").await)
        .to_have_count(20)
        .await
        .expect("the first page of permissions");
    let more = id(&page, "permissions")
        .await
        .locator("button:text('more of')");
    expect(more.clone())
        .to_have_text("Show 20 more of 44")
        .await
        .expect("what the button will add");
    more.click(None).await.expect("show more permissions");
    expect(id(&page, "permission").await)
        .to_have_count(40)
        .await
        .expect("the next page of permissions");

    // The first agent by name has the queue, so its detail shows an exact count.
    let recipient = snapshot["mail"]["moving"]
        .as_array()
        .expect("moving rows")
        .iter()
        .find(|row| row["id"].as_str().is_some_and(|id| id.starts_with("0000")))
        .expect("a seeded row")["to"]
        .as_str()
        .expect("recipient")
        .to_owned();
    page.locator(&format!("[data-testid=node][data-node-id='{recipient}']"))
        .await
        .click(None)
        .await
        .expect("select the agent");
    edges_converge(&page, &daemon).await;
    let queued: i64 = daemon.snapshot().await["mail"]["edges"]
        .as_array()
        .expect("edges")
        .iter()
        .filter(|edge| edge["to"] == recipient.as_str())
        .map(|edge| {
            edge["queued"].as_i64().expect("queued") + edge["offered"].as_i64().expect("offered")
        })
        .sum();
    assert!(queued > 1000, "{queued}");
    expect(id(&page, "agent-queue-count").await)
        .with_timeout(Duration::from_secs(30))
        .to_contain_text(&format!("{},{:03}", queued / 1000, queued % 1000))
        .await
        .expect("the exact queue count, from the edges");
    shot(&page, "truncated-moving").await;
    assert_clean(&page, "truncated lists").await;
    std::fs::write(daemon.root.path().join("release"), "release").expect("release agent");
    daemon.stop().await;
}

// ── Scale and layout ─────────────────────────────────────────────────────────

#[tokio::test]
async fn two_hundred_agents_stay_readable_at_every_size() {
    let mut daemon = Daemon::new().await;
    daemon.spawn("agent-000").await;
    daemon.stop().await;
    daemon.seed_held(1..=12, "hop limit reached");
    daemon.restart().await;
    for n in 1..200 {
        daemon.spawn(&format!("agent-{n:03}")).await;
    }
    for n in 0..40 {
        daemon
            .ctl(&["send", &format!("@agent-{:03}", n * 5), "hello"])
            .await;
    }
    daemon.ctl(&["send", "@agent-050", "hold"]).await;
    daemon.ctl(&["send", "@agent-090", "permission"]).await;
    wait_for_approvals(&daemon, 1).await;

    let harness = Harness::launch().await;
    for (name, size) in [("desktop", DESKTOP), ("wide", WIDE), ("phone", PHONE)] {
        let page = harness.open(&daemon.url("127.0.0.1"), size).await;
        app_live(&page).await;
        expect(id(&page, "node").await)
            .with_timeout(Duration::from_secs(30))
            .to_have_count(201)
            .await
            .expect("every agent and the human");
        expect(id(&page, "stat-agents").await)
            .to_contain_text("200")
            .await
            .expect("agent count");
        // The first thing on screen answers "what is waiting on me?".
        visible(&page, "permission").await;
        visible(&page, "stuck-held").await;
        assert_fits(&page, name).await;
        assert_clean(&page, name).await;
        shot(&page, &format!("graph-200-{name}")).await;

        id(&page, "tab-agents")
            .await
            .click(None)
            .await
            .expect("agents tab");
        visible(&page, "agent-row").await;
        assert_fits(&page, name).await;
        shot(&page, &format!("agents-200-{name}")).await;
    }

    // These fake agents share a root, so a restart leaves nearly all of them unable
    // to resume: a mass outage. The panel must still reach what waits on the operator.
    std::fs::write(daemon.root.path().join("release"), "release").expect("release agent");
    daemon.stop().await;
    std::fs::write(daemon.root.path().join("fail-load"), "").expect("fail load");
    daemon.restart().await;
    let page = harness.open(&daemon.url("127.0.0.1"), WIDE).await;
    app_live(&page).await;
    expect(
        page.locator("[data-testid=node][data-phase=needs-recovery]")
            .await
            .first(),
    )
    .with_timeout(Duration::from_secs(60))
    .to_be_visible()
    .await
    .expect("agents needing recovery");
    let broken: Vec<usize> = eval(
        &page,
        r#"[document.querySelectorAll('[data-testid="node"][data-phase="needs-recovery"]').length,
            document.querySelectorAll('[data-testid="broken-agent"]').length]"#,
    )
    .await;
    assert!(
        broken[0] > 24,
        "an outage needs many broken agents: {broken:?}"
    );
    assert_eq!(broken[1], 8, "the list must not bury the rest");
    visible(&page, "stuck-held").await;
    assert!(
        text(&page, "stat-recovery")
            .await
            .contains(&broken[0].to_string()),
        "the header must count them"
    );
    // Words on every node would overlap here. The shape still says it.
    let tags: i64 = eval(
        &page,
        r#"[...document.querySelectorAll('[data-testid="node"] text')]
            .filter((t) => t.textContent === "NEEDS RECOVERY").length"#,
    )
    .await;
    assert_eq!(tags, 0);
    shot(&page, "graph-200-outage").await;
    assert_clean(&page, "an outage").await;
    daemon.stop().await;
}

#[tokio::test]
async fn every_screen_is_clean_in_both_themes_and_at_narrow_widths() {
    let mut daemon = Daemon::with(Setup {
        env: vec![("FAKE_ACTIVITY_TEXT", "reading the repository".to_owned())],
        ..Setup::default()
    })
    .await;
    for name in ["planner", "builder", "reviewer", "tester", "docs"] {
        daemon.spawn(name).await;
    }
    daemon.stop().await;
    daemon.seed_held(1..=3, "hop limit reached");
    daemon.restart().await;
    daemon.ctl(&["send", "@builder", "hold"]).await;
    daemon.ctl(&["send", "@reviewer", "permission"]).await;
    daemon.ctl(&["send", "@tester", "hello"]).await;
    wait_for_approvals(&daemon, 1).await;

    let harness = Harness::launch().await;
    for (scheme, size, label) in [
        ("dark", DESKTOP, "dark-desktop"),
        ("light", DESKTOP, "light-desktop"),
        ("dark", WIDE, "dark-wide"),
        ("dark", PHONE, "dark-phone"),
        ("light", PHONE, "light-phone"),
        ("dark", (768, 1024), "dark-tablet"),
    ] {
        let page = harness
            .context(size, scheme)
            .await
            .new_page()
            .await
            .expect("page");
        page.goto(&daemon.url("127.0.0.1"), None)
            .await
            .expect("open");
        app_live(&page).await;
        visible(&page, "permission").await;
        assert_fits(&page, label).await;
        assert_clean(&page, label).await;
        shot(&page, &format!("console-{label}")).await;

        // Reduced motion stops every animation.
        let running: i64 = eval(
            &page,
            r#"document.getAnimations().filter((a) => a.playState === "running").length"#,
        )
        .await;
        assert_eq!(running, 0, "{label}: animation under reduced motion");

        // Selecting a node shows its detail, and it fits.
        let busy = page.locator("[data-testid=node][data-phase=busy]").await;
        expect(busy.first())
            .with_timeout(Duration::from_secs(30))
            .to_be_visible()
            .await
            .expect("a busy agent");
        busy.first()
            .click(None)
            .await
            .expect("select the busy agent");
        visible(&page, "agent-detail").await;
        assert_fits(&page, label).await;
        shot(&page, &format!("detail-{label}")).await;
        assert_clean(&page, label).await;
    }

    std::fs::write(daemon.root.path().join("release"), "release").expect("release agent");
    daemon.stop().await;
}
