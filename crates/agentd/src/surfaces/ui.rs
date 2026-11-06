//! Read-only, authenticated loopback surface.
use super::*;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use subtle::ConstantTimeEq;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::derive_accept_key,
        protocol::{Role, WebSocketConfig},
    },
};

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UiConfig {
    pub listen: Option<SocketAddr>,
    pub assets: Option<PathBuf>,
}

const MAX_ASSET_FILES: usize = 4096;
const MAX_ASSET_BYTES: usize = 64 * 1024 * 1024;
type Manifest = HashMap<String, (Arc<[u8]>, &'static str)>;
struct ManifestState {
    current: Arc<Manifest>,
    last_attempt: Option<tokio::time::Instant>,
    rebuilding: bool,
    failed: bool,
}

pub struct UiServer {
    listener: TcpListener,
    config: UiConfig,
    host: String,
    token: String,
    manifest: Mutex<ManifestState>,
}
impl UiServer {
    pub async fn bind(mut config: UiConfig, state: &Path) -> Result<Option<Self>> {
        let Some(addr) = config.listen else {
            return Ok(None);
        };
        if !addr.ip().is_loopback() || addr.port() == 0 {
            return Err("ui.listen requires a loopback address and a nonzero port".into());
        }
        if let Some(assets) = config.assets.take() {
            let assets =
                tokio::task::spawn_blocking(move || std::fs::canonicalize(assets)).await??;
            if assets.starts_with(std::fs::canonicalize(state)?) {
                return Err("ui.assets must be outside state_dir".into());
            }
            crate::private_fs::Dir::open(&assets, false)?;
            config.assets = Some(assets);
        }
        let dir = crate::private_fs::Dir::open(state, false)?;
        let name = std::ffi::OsStr::new("ui.token");
        let token = match dir.read(name)? {
            Some(bytes) => String::from_utf8(bytes)?,
            None => {
                let mut bytes = [0_u8; 32];
                getrandom::fill(&mut bytes).map_err(|e| e.to_string())?;
                let token = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
                if !dir.replace(name, None, Some(token.as_bytes()))? {
                    return Err("ui.token changed during creation".into());
                }
                token
            }
        };
        if token.len() != 64 || !token.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err("invalid ui.token".into());
        }
        let mode = std::fs::symlink_metadata(state.join("ui.token"))?
            .permissions()
            .mode()
            & 0o777;
        if mode != 0o600 {
            return Err("ui.token must have mode 0600".into());
        }
        Ok(Some(Self {
            listener: TcpListener::bind(addr).await?,
            host: addr.to_string(),
            config,
            token,
            manifest: Mutex::new(ManifestState {
                current: Arc::new(HashMap::new()),
                last_attempt: None,
                rebuilding: false,
                failed: false,
            }),
        }))
    }
    pub fn url(&self) -> String {
        format!("http://{}/#token={}", self.host, self.token)
    }
    fn hosts(&self, host: &str) -> bool {
        host == self.host
            || host
                == format!(
                    "localhost:{}",
                    self.config.listen.expect("bug: UI bound").port()
                )
    }
    fn authorized(&self, token: &str) -> bool {
        bool::from(self.token.as_bytes().ct_eq(token.as_bytes()))
    }
    pub async fn serve(
        self,
        api: Arc<ControlApi>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        let server = Arc::new(self);
        let slots = Arc::new(Semaphore::new(64));
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _=cancel.cancelled()=>break,
                accepted=server.listener.accept()=>{
                    let (stream,_)=match accepted {
                        Ok(stream)=>stream,
                        Err(error)=>{tracing::warn!(%error,"UI accept failed");tokio::time::sleep(Duration::from_millis(100)).await;continue;}
                    };
                    let Ok(permit)=slots.clone().try_acquire_owned() else {tracing::debug!("UI connection limit reached; dropping stream");continue;};
                    let server=server.clone();let api=api.clone();
                    connections.spawn(async move {let _permit=permit;if let Err(error)=server.connection(stream,api).await {tracing::debug!(%error,"UI connection closed");}});
                },
                done=connections.join_next(),if !connections.is_empty()=>{if let Some(Err(error))=done {tracing::error!(%error,"UI task failed");}}
            }
        }
        connections.shutdown().await;
        Ok(())
    }
    async fn refresh_manifest(&self) -> bool {
        let Some(root) = self.config.assets.clone() else {
            return true;
        };
        {
            let mut state = self.manifest.lock().await;
            if state.rebuilding
                || state
                    .last_attempt
                    .is_some_and(|last| last.elapsed() < Duration::from_secs(1))
            {
                return !state.failed;
            }
            state.rebuilding = true;
            state.last_attempt = Some(tokio::time::Instant::now());
        }
        let result = tokio::task::spawn_blocking(move || load_manifest(&root)).await;
        let mut state = self.manifest.lock().await;
        state.rebuilding = false;
        match result {
            Ok(Ok(manifest)) => {
                state.current = Arc::new(manifest);
                state.failed = false;
            }
            Ok(Err(error)) => {
                tracing::error!(%error,"refusing UI asset tree");
                state.current = Arc::new(HashMap::new());
                state.failed = true;
            }
            Err(error) => {
                tracing::error!(%error,"UI asset rebuild task failed");
                state.current = Arc::new(HashMap::new());
                state.failed = true;
            }
        }
        !state.failed
    }
    async fn connection(&self, mut stream: TcpStream, api: Arc<ControlApi>) -> Result<()> {
        let header = tokio::time::timeout(Duration::from_secs(5), async {
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                if bytes.len() == 16384 {
                    return Err("HTTP header too large".into());
                }
                bytes.push(stream.read_u8().await?);
            }
            Ok::<_, Error>(String::from_utf8(bytes)?)
        })
        .await??;
        let mut lines = header.split("\r\n");
        let request = lines
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect::<Vec<_>>();
        let mut headers = HashMap::new();
        let mut malformed = false;
        for line in lines.filter(|l| !l.is_empty()) {
            if let Some((k, v)) = line.split_once(':') {
                if headers
                    .insert(k.to_ascii_lowercase(), v.trim().to_owned())
                    .is_some()
                {
                    malformed = true;
                }
            } else {
                malformed = true;
            }
        }
        let get = |name: &str| headers.get(name).map(String::as_str).unwrap_or("");
        let host = get("host");
        if !self.hosts(host) {
            return response(
                &mut stream,
                403,
                "Forbidden",
                b"bad Host",
                "text/plain",
                &self.host,
                false,
            )
            .await;
        }
        if malformed || request.len() != 3 || request[2] != "HTTP/1.1" {
            return response(
                &mut stream,
                400,
                "Bad Request",
                b"malformed HTTP",
                "text/plain",
                host,
                false,
            )
            .await;
        }
        let target = request[1];
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let auth = matches!(path, "/events" | "/mail") || path.starts_with("/messages/");
        if request[0] != "GET" {
            return response(
                &mut stream,
                405,
                "Method Not Allowed",
                b"read-only surface",
                "text/plain",
                host,
                auth,
            )
            .await;
        }
        if path == "/events" {
            let origin = get("origin");
            if headers.contains_key("origin")
                && ![
                    format!("http://{}", self.host),
                    format!(
                        "http://localhost:{}",
                        self.config.listen.expect("bug: UI bound").port()
                    ),
                ]
                .contains(&origin.to_owned())
            {
                return response(
                    &mut stream,
                    403,
                    "Forbidden",
                    b"bad Origin",
                    "text/plain",
                    host,
                    true,
                )
                .await;
            }
            let protocols = get("sec-websocket-protocol")
                .split(',')
                .map(str::trim)
                .collect::<Vec<_>>();
            if !protocols.contains(&"murmur.events.v1")
                || !protocols.iter().any(|p| {
                    p.strip_prefix("murmur.token.")
                        .is_some_and(|t| self.authorized(t))
                })
            {
                return response(
                    &mut stream,
                    401,
                    "Unauthorized",
                    b"token required",
                    "text/plain",
                    host,
                    true,
                )
                .await;
            }
            let key = get("sec-websocket-key");
            if get("sec-websocket-version") != "13"
                || !base64::engine::general_purpose::STANDARD
                    .decode(key)
                    .is_ok_and(|b| b.len() == 16)
                || !get("upgrade").eq_ignore_ascii_case("websocket")
                || !get("connection")
                    .split(',')
                    .any(|s| s.trim().eq_ignore_ascii_case("upgrade"))
            {
                return response(
                    &mut stream,
                    400,
                    "Bad Request",
                    b"bad WebSocket handshake",
                    "text/plain",
                    host,
                    true,
                )
                .await;
            }
            let Ok(_subscriber) = events::subscribe() else {
                return response(
                    &mut stream,
                    503,
                    "Service Unavailable",
                    b"feed subscriber limit",
                    "text/plain",
                    host,
                    true,
                )
                .await;
            };
            let accept = derive_accept_key(key.as_bytes());
            let reply = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: murmur.events.v1\r\n{}\r\n",
                security(host, true)
            );
            stream.write_all(reply.as_bytes()).await?;
            let config = WebSocketConfig::default()
                .max_message_size(Some(4096))
                .max_frame_size(Some(4096));
            let ws = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
            let (write, mut read) = ws.split();
            let write = Arc::new(Mutex::new(write));
            let mut sink = WsSink(write.clone());
            let params = parameters(query)?;
            let resume = params
                .get("cursor")
                .and_then(|s| s.parse().ok())
                .zip(params.get("nonce").cloned())
                .map(|(seq, nonce)| crate::mailroom::Anchor { seq, nonce });
            let inbound = async {
                let mut ping = tokio::time::interval(Duration::from_secs(30));
                let mut pong = tokio::time::Instant::now();
                loop {
                    tokio::select! {
                        _=ping.tick()=>{
                            if pong.elapsed()>=Duration::from_secs(60) {return Ok::<_,Error>(());}
                            tokio::time::timeout(Duration::from_secs(10),write.lock().await.send(Message::Ping(Vec::new().into()))).await??;
                        },
                        message=read.next()=>match message {
                            Some(Ok(Message::Pong(_)))=>pong=tokio::time::Instant::now(),
                            Some(Ok(Message::Close(_)))|None=>return Ok(()),
                            Some(Err(e))=>return Err(e.into()),
                            Some(Ok(Message::Ping(data)))=>{tokio::time::timeout(Duration::from_secs(10),write.lock().await.send(Message::Pong(data))).await??;},
                            _=>{}
                        }
                    }
                }
            };
            return tokio::select! {r=events::feed(&api,&mut sink,resume)=>r,r=inbound=>r};
        }
        if auth
            && !get("authorization")
                .strip_prefix("Bearer ")
                .is_some_and(|t| self.authorized(t))
        {
            return response(
                &mut stream,
                401,
                "Unauthorized",
                b"token required",
                "text/plain",
                host,
                true,
            )
            .await;
        }
        if path == "/mail" || path.starts_with("/messages/") {
            let result = async {
                if path == "/mail" {
                    let p = parameters(query)?;
                    let limit = p
                        .get("limit")
                        .map(|s| s.parse::<usize>())
                        .transpose()?
                        .unwrap_or(1000);
                    if limit == 0 || limit > 1000 {
                        return Err("limit must be 1..1000".into());
                    }
                    let q = crate::mailroom::MailQuery {
                        from: p.get("from").cloned(),
                        to: p.get("to").cloned(),
                        states: p
                            .get("states")
                            .map(|s| s.split(',').map(str::to_owned).collect())
                            .unwrap_or_default(),
                        before_msg_seq: p.get("before_msg_seq").map(|s| s.parse()).transpose()?,
                        limit,
                    };
                    let value = api.mailroom.mail_query(q).await?;
                    events::mail_response(value, api.control_path.clone()).await
                } else {
                    let message = api
                        .mailroom
                        .observed_message(MessageId(path.trim_start_matches("/messages/").parse()?))
                        .await?;
                    Ok(serde_json::to_vec(&message)?)
                }
            }
            .await;
            return match result {
                Ok(bytes) => {
                    response(
                        &mut stream,
                        200,
                        "OK",
                        &bytes,
                        "application/json",
                        host,
                        true,
                    )
                    .await
                }
                Err(e) => {
                    response(
                        &mut stream,
                        400,
                        "Bad Request",
                        e.to_string().as_bytes(),
                        "text/plain",
                        host,
                        true,
                    )
                    .await
                }
            };
        }
        if path == "/" && !self.refresh_manifest().await {
            return response(
                &mut stream,
                503,
                "Service Unavailable",
                b"assets unavailable",
                "text/plain",
                host,
                false,
            )
            .await;
        }
        let manifest = self.manifest.lock().await.current.clone();
        let key = if path == "/" { "/index.html" } else { path };
        if let Some((bytes, mime)) = manifest.get(key) {
            return response(&mut stream, 200, "OK", bytes, mime, host, false).await;
        }
        if path == "/" && self.config.assets.is_none() {
            return response(&mut stream,200,"OK",b"<!doctype html><title>murmur</title><p>Build the graph UI with npm ci and npm run build in agentd-web, then set [ui] assets to its dist directory. The read-only API is available.</p>","text/html",host,false).await;
        }
        response(
            &mut stream,
            404,
            "Not Found",
            b"not found",
            "text/plain",
            host,
            false,
        )
        .await
    }
}
type WsWrite = futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>;
struct WsSink(Arc<Mutex<WsWrite>>);
#[async_trait::async_trait]
impl events::Sink for WsSink {
    async fn send(&mut self, v: Value) -> Result<()> {
        self.0
            .lock()
            .await
            .send(Message::Text(serde_json::to_string(&v)?.into()))
            .await?;
        Ok(())
    }
    async fn send_snapshot(&mut self, json: String, _full_mail: bool) -> Result<()> {
        self.0.lock().await.send(Message::Text(json.into())).await?;
        Ok(())
    }
}
fn security(host: &str, auth: bool) -> String {
    // An unvalidated Host is never reflected into response headers.
    let host = if host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b".:-[]".contains(&b))
    {
        host
    } else {
        "localhost"
    };
    format!(
        "Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self' ws://{host}; base-uri 'none'; form-action 'none'; frame-ancestors 'none'\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n{}",
        if auth {
            "Cache-Control: no-store\r\n"
        } else {
            ""
        }
    )
}
async fn response(
    stream: &mut TcpStream,
    status: u16,
    label: &str,
    body: &[u8],
    mime: &str,
    host: &str,
    auth: bool,
) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status} {label}\r\nContent-Length: {}\r\nContent-Type: {mime}\r\nConnection: close\r\n{}\r\n",
        body.len(),
        security(host, auth)
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body).await
    })
    .await??;
    Ok(())
}
fn parameters(query: &str) -> Result<HashMap<String, String>> {
    Ok(url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect())
}
fn load_manifest(root: &Path) -> Result<Manifest> {
    use crate::private_fs::{Asset, Dir};
    use std::{io::Read, os::unix::ffi::OsStrExt};
    fn walk(
        dir: Dir,
        relative: &Path,
        out: &mut Manifest,
        bytes: &mut usize,
        entries: &mut usize,
        depth: usize,
    ) -> Result<()> {
        if depth > 32 {
            return Err("asset tree exceeds 32 directories in depth".into());
        }
        for entry in dir.entries()? {
            let entry = entry?;
            let name = std::ffi::OsStr::from_bytes(entry.file_name().to_bytes());
            if name == "." || name == ".." {
                continue;
            }
            *entries += 1;
            if *entries > MAX_ASSET_FILES * 2 {
                return Err("asset tree exceeds entry limit".into());
            }
            let path = relative.join(name);
            match dir.asset(name) {
                Ok(Asset::Directory(child)) => walk(child, &path, out, bytes, entries, depth + 1)?,
                Ok(Asset::File(file)) => {
                    if out.len() >= MAX_ASSET_FILES {
                        return Err("asset tree exceeds file limit".into());
                    }
                    let remaining = MAX_ASSET_BYTES - *bytes;
                    if file.metadata()?.len() > remaining as u64 {
                        return Err("asset tree exceeds byte limit".into());
                    }
                    let mut data = Vec::new();
                    file.take(remaining as u64 + 1).read_to_end(&mut data)?;
                    if data.len() > remaining {
                        return Err("asset tree exceeds byte limit".into());
                    }
                    *bytes += data.len();
                    let mime = match path.extension().and_then(|s| s.to_str()) {
                        Some("html") => "text/html",
                        Some("js") => "text/javascript",
                        Some("css") => "text/css",
                        Some("svg") => "image/svg+xml",
                        Some("png") => "image/png",
                        Some("woff2") => "font/woff2",
                        Some("json") => "application/json",
                        _ => "application/octet-stream",
                    };
                    out.insert(
                        format!("/{}", path.to_str().ok_or("non-UTF8 asset name")?),
                        (Arc::from(data), mime),
                    );
                }
                Err(error) => {
                    tracing::warn!(path=%path.display(),%error,"skipping unsafe or unavailable UI asset")
                }
            }
        }
        Ok(())
    }
    let mut out = HashMap::new();
    walk(
        Dir::open(root, false)?,
        Path::new(""),
        &mut out,
        &mut 0,
        &mut 0,
        0,
    )?;
    Ok(out)
}
