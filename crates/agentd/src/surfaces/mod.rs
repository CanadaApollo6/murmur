use std::collections::BTreeMap;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{UnixListener, UnixStream};

use crate::KindConfig;
use crate::mailroom::{Mailroom, NewMessage, SendOutcome};
use crate::model::*;
use crate::supervisor::{AgentSpec, Broker, Operation, Phase, phase_error};
use crate::swarm::Act;

type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub mod events;
pub mod rap;
pub mod ui;

pub fn default_state_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
        .join(".local/state/agentd")
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub state_dir: Option<PathBuf>,
    #[serde(default)]
    pub mail: crate::mailroom::MailLimits,
    #[serde(default)]
    pub sandbox: crate::sandbox::SandboxMode,
    #[serde(default)]
    pub bwrap: crate::sandbox::SandboxConfig,
    #[serde(default)]
    pub listen: ListenConfig,
    #[serde(default)]
    pub rap: rap::RapConfig,
    #[serde(default)]
    pub ui: ui::UiConfig,
    #[serde(default)]
    kind: BTreeMap<String, KindFile>,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenConfig {
    pub control_socket: Option<PathBuf>,
    pub mcp_socket: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KindFile {
    sandbox: Option<crate::sandbox::SandboxMode>,
    home: Option<Vec<String>>,
    credentials: Option<Vec<PathBuf>>,
    egress: Option<Vec<String>>,
    wire: Option<String>,
    #[serde(default)]
    command: Vec<String>,
    socket: Option<PathBuf>,
    model: Option<String>,
    mode: Option<String>,
    workspace_write_mode: Option<String>,
    full_access_mode: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    idle_after_secs: Option<u64>,
    stall_timeout_secs: Option<u64>,
    #[serde(default)]
    sandbox_args: crate::SandboxArgs,
}
impl FileConfig {
    pub fn load(path: &Path) -> Result<Self> {
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
    }
    pub fn kinds(self) -> Result<Vec<KindConfig>> {
        let mut kinds = BTreeMap::from([
            ("claude".to_owned(), KindConfig::claude("claude-agent-acp")),
            ("codex".to_owned(), KindConfig::codex("codex-acp")),
        ]);
        for kind in kinds.values_mut() {
            kind.isolation =
                crate::sandbox::Isolation::Bwrap(crate::sandbox::KindSandbox::defaults(&kind.name));
        }
        let mut overrides = Vec::new();
        for (name, entry) in self.kind {
            let mut kind = match entry.wire.as_deref() {
                Some("infinity") => {
                    if !entry.command.is_empty()
                        || !entry.env.is_empty()
                        || entry.mode.is_some()
                        || entry.workspace_write_mode.is_some()
                        || entry.full_access_mode.is_some()
                        || !entry.sandbox_args.read_only.is_empty()
                        || !entry.sandbox_args.workspace_write.is_empty()
                        || !entry.sandbox_args.full_access.is_empty()
                    {
                        return Err("Infinity uses daemon configuration, not launch flags".into());
                    }
                    let socket = entry.socket.ok_or("Infinity kind needs a socket path")?;
                    if !socket.is_absolute() {
                        return Err("Infinity socket must be absolute".into());
                    }
                    KindConfig::infinity(socket)
                }
                None | Some("acp") => {
                    if entry.socket.is_some() {
                        return Err("ACP kind cannot have an Infinity socket".into());
                    }
                    let mut kind = kinds
                        .remove(&name)
                        .ok_or("only claude and codex ACP kinds are supported")?;
                    let (command, args) =
                        entry.command.split_first().ok_or("empty kind command")?;
                    kind.command = command.clone();
                    kind.args = args.to_vec();
                    kind
                }
                Some(_) => return Err("unknown adapter wire".into()),
            };
            if entry.sandbox == Some(crate::sandbox::SandboxMode::Bwrap) {
                overrides.push(name.clone());
            }
            if matches!(kind.wire, crate::Wire::Infinity { .. }) {
                if entry.sandbox == Some(crate::sandbox::SandboxMode::Bwrap)
                    || entry.credentials.is_some()
                    || entry.home.is_some()
                    || entry.egress.is_some()
                {
                    return Err(
                        "Infinity is unsandboxed; configure the shared daemon separately".into(),
                    );
                }
            } else if entry.sandbox == Some(crate::sandbox::SandboxMode::None) {
                kind.isolation = crate::sandbox::Isolation::None;
            } else {
                if entry.sandbox == Some(crate::sandbox::SandboxMode::Bwrap) {
                    kind.isolation = crate::sandbox::Isolation::Bwrap(
                        crate::sandbox::KindSandbox::defaults(&name),
                    );
                }
                if let crate::sandbox::Isolation::Bwrap(sb) = &mut kind.isolation {
                    if let Some(home) = entry.home {
                        sb.home = home;
                    }
                    if let Some(credentials) = entry.credentials {
                        sb.credentials = credentials
                            .iter()
                            .map(|p| crate::sandbox::expand(p))
                            .collect();
                    }
                    if let Some(egress) = entry.egress {
                        sb.egress = egress;
                    }
                }
            }
            let effective_none = entry.sandbox == Some(crate::sandbox::SandboxMode::None)
                || (entry.sandbox.is_none() && self.sandbox == crate::sandbox::SandboxMode::None);
            if effective_none && matches!(kind.wire, crate::Wire::Acp) {
                eprintln!(
                    "agentd: kind {name} is unsandboxed; home, credentials and egress settings are ignored"
                );
            }
            kind.name = name.clone();
            kind.model = entry.model;
            kind.env = entry.env;
            if let Some(mode) = entry.mode {
                kind.mode = mode;
            }
            if let Some(mode) = entry.workspace_write_mode {
                kind.workspace_write_mode = Some(mode);
            }
            if let Some(mode) = entry.full_access_mode {
                kind.full_access_mode = Some(mode);
            }
            if let Some(seconds) = entry.idle_after_secs {
                kind.idle_after = if seconds == 0 {
                    None
                } else {
                    Some(Duration::from_secs(seconds))
                };
            }
            if let Some(seconds) = entry.stall_timeout_secs {
                kind.stall_timeout = Duration::from_secs(seconds);
            }
            kind.sandbox_args = entry.sandbox_args;
            kinds.insert(name, kind);
        }
        for kind in kinds.values_mut() {
            if self.sandbox == crate::sandbox::SandboxMode::None && !overrides.contains(&kind.name)
            {
                kind.isolation = crate::sandbox::Isolation::None;
            }
        }
        Ok(kinds.into_values().collect())
    }
}

mod bindings;
use bindings::AuthenticatedSender;
pub use bindings::{Bindings, BoundListener, SessionIdentity};

#[derive(Clone)]
pub enum Surface {
    Control,
    SharedMcp,
    AgentMcp(SessionIdentity),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlRequest {
    Spawn {
        spec: AgentSpec,
        first_message: Option<Body>,
        key: ClientKey,
    },
    Ls,
    Send {
        to: String,
        body: Body,
        key: ClientKey,
    },
    Approvals,
    Approve {
        id: PermissionId,
        option: String,
    },
    Cancel {
        to: String,
    },
    Stop {
        to: String,
    },
    Restart {
        to: String,
    },
    Held {
        to: Option<String>,
    },
    Release {
        id: MessageId,
    },
    Drop {
        id: MessageId,
    },
    Watch {
        cursor: Option<i64>,
        nonce: Option<String>,
    },
    Ui,
}

pub struct ControlApi {
    pub broker: Arc<Broker>,
    pub mailroom: Arc<Mailroom>,
    pub bindings: Arc<Bindings>,
    pub control_path: PathBuf,
    pub ui_url: Option<String>,
}
impl ControlApi {
    async fn agents(&self) -> Result<Value> {
        let mut rows = Vec::new();
        for (id, spec, queued) in self.mailroom.observed_agents().await? {
            let handle = self.broker.handle(id).await;
            let phase = handle.as_ref().map_or(Phase::Ended, |h| h.phase());
            let error = handle.as_ref().and_then(|h| h.error());
            let isolation = handle.as_ref().map_or("none", |h| h.isolation());
            rows.push(
                json!({"id":format!("agent:{id}"),"name":format!("@{}",spec.name.as_str()),
                "kind":spec.kind,"isolation":isolation,"phase":phase_name(&phase),"error":error,
                "queued":queued}),
            );
        }
        Ok(json!(rows))
    }
    fn approvals(&self) -> Result<Vec<PermissionRequest>> {
        Ok(self.broker.event_hub().permissions())
    }

    async fn send(
        &self,
        from: AuthenticatedSender,
        to: &str,
        body: Body,
        reply_to: Option<MessageId>,
        key: ClientKey,
    ) -> Result<Value> {
        let id = self.mailroom.resolve(Recipient::parse(to)?).await?;
        let (outcome, env) = self
            .broker
            .send(NewMessage {
                from: from.sender(),
                to: id,
                body,
                reply_to,
                client_key: key,
                act: Act::Inform,
                task: None,
            })
            .await?;
        let status = match outcome {
            SendOutcome::Queued { .. } => "Queued".to_owned(),
            SendOutcome::Duplicate { .. } => "Already queued".to_owned(),
            SendOutcome::Held { reason, .. } => format!("Held ({reason})"),
        };
        let suffix = match self.broker.handle(id).await.map(|h| h.phase()) {
            Some(Phase::Busy) => " (busy; it sees this when its turn ends)",
            Some(Phase::Stopped { .. }) => " (stopped; it sees this after agentctl restart)",
            _ => "",
        };
        Ok(json!({"id":env.id(),"status":format!("{status} {} for {to}{suffix}.",env.id())}))
    }
    async fn control(&self, request: ControlRequest) -> Result<Value> {
        match request {
            ControlRequest::Spawn {
                spec,
                first_message,
                key,
            } => {
                if !spec.cwd.is_absolute() || !spec.cwd.is_dir() {
                    return Err("cwd must be an absolute directory".into());
                }
                let to = format!("@{}", spec.name.as_str());
                let handle = self.broker.spawn(spec).await?;
                if let Some(body) = first_message {
                    self.send(Bindings::human(), &to, body, None, key).await?;
                }
                Ok(json!({"id":format!("agent:{}",handle.id),"name":to,"cwd":handle.cwd}))
            }
            ControlRequest::Ls => self.agents().await,
            ControlRequest::Send { to, body, key } => {
                self.send(Bindings::human(), &to, body, None, key).await
            }
            ControlRequest::Approvals => Ok(json!(
                self.approvals()?
                    .into_iter()
                    .map(|r| {
                        let mut value =
                            serde_json::to_value(&r).expect("bug: permission serializes");
                        for (option, raw) in value["options"]
                            .as_array_mut()
                            .expect("bug: permission options")
                            .iter_mut()
                            .zip(&r.options)
                        {
                            option["command"] = json!(events::command(
                                &self.control_path,
                                "approve",
                                &[&r.id.to_string(), &raw.key]
                            ));
                        }
                        value
                    })
                    .collect::<Vec<_>>()
            )),
            ControlRequest::Approve { id, option } => {
                let request = self
                    .approvals()?
                    .into_iter()
                    .find(|r| r.id == id)
                    .ok_or("stale permission")?;
                self.broker
                    .handle(request.agent)
                    .await
                    .ok_or("agent stopped")?
                    .answer_permission(id, request.incarnation, PermissionAnswer::Choose(option))
                    .await?;
                Ok(json!({"approved":id}))
            }
            ControlRequest::Cancel { to } => {
                let id = self.mailroom.resolve(Recipient::parse(&to)?).await?;
                self.broker
                    .handle(id)
                    .await
                    .ok_or("agent stopped")?
                    .cancel()
                    .await?;
                Ok(json!({"cancelled":to}))
            }
            ControlRequest::Stop { to } => {
                let id = self.mailroom.resolve(Recipient::parse(&to)?).await?;
                self.broker.operate(id, Operation::Stop).await?;
                Ok(json!({"stopped":to}))
            }
            ControlRequest::Restart { to } => {
                let id = self.mailroom.resolve(Recipient::parse(&to)?).await?;
                self.broker.operate(id, Operation::Restart).await?;
                Ok(json!({"restarted":to}))
            }
            ControlRequest::Held { to } => {
                let agent = match to {
                    Some(to) => Some(self.mailroom.resolve(Recipient::parse(&to)?).await?),
                    None => None,
                };
                Ok(json!(self.mailroom.held(agent).await?))
            }
            ControlRequest::Release { id } => {
                let agent = self.mailroom.release(id).await?;
                self.broker.nudge(agent).await;
                Ok(json!({"released":id}))
            }
            ControlRequest::Drop { id } => {
                self.mailroom.drop_held(id, "dropped by human").await?;
                Ok(json!({"dropped":id}))
            }
            ControlRequest::Watch { .. } => Err("watch requires streaming".into()),
            ControlRequest::Ui => Ok(
                json!({"url":self.ui_url,"status":self.ui_url.clone().unwrap_or_else(|| "Add to kinds.toml:\n[ui]\nlisten = \"127.0.0.1:7434\"".into())}),
            ),
        }
    }
    async fn mcp(
        &self,
        surface: &Surface,
        bearer: Option<&str>,
        session: &str,
        request: Value,
    ) -> Value {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        if matches!(surface, Surface::AgentMcp(_)) && bearer.is_some() {
            return rpc_error(id, -32600, "token on per-agent listener");
        }
        let binding = match surface {
            Surface::AgentMcp(identity) => self.bindings.authenticate_agent(identity).await,
            Surface::SharedMcp => self.bindings.authenticate_mcp(bearer.unwrap_or("")).await,
            Surface::Control => None,
        };
        let Some(binding) = binding else {
            return rpc_error(id, -32001, "unknown or revoked MCP token");
        };
        if session.len() != 64 || !session.bytes().all(|b| b.is_ascii_hexdigit()) {
            return rpc_error(id, -32600, "invalid MCP shim session");
        }
        if request["jsonrpc"] != "2.0" || !(id.is_string() || id.is_i64() || id.is_u64()) {
            return rpc_error(id, -32600, "invalid MCP request");
        }
        let result = match request["method"].as_str() {
            Some("initialize") => Ok(
                json!({"protocolVersion":request["params"]["protocolVersion"].as_str().unwrap_or("2024-11-05"),
                "capabilities":{"tools":{}},"serverInfo":{"name":"agentd","version":env!("CARGO_PKG_VERSION")}}),
            ),
            Some("ping") => Ok(json!({})),
            Some("tools/list") => Ok(json!({"tools":agent_tools()})),
            Some("tools/call") => {
                let args = request["params"]
                    .get("arguments")
                    .cloned()
                    .unwrap_or(json!({}));
                let result = match request["params"]["name"].as_str() {
                    Some("send_message") => match serde_json::from_value::<SendArgs>(args) {
                        Ok(args) => self
                            .send(
                                binding.sender(),
                                &args.to,
                                args.body,
                                args.reply_to,
                                binding.key(session, &id),
                            )
                            .await
                            .map(|v| v["status"].as_str().unwrap_or("Queued").to_owned()),
                        Err(_) => Err("invalid send_message arguments".into()),
                    },
                    Some("list_agents") if args == json!({}) => {
                        self.agents().await.map(|v| v.to_string())
                    }
                    _ => Err("unknown tool or invalid arguments".into()),
                };
                Ok(match result {
                    Ok(text) => json!({"content":[{"type":"text","text":text}],"isError":false}),
                    Err(error) => {
                        json!({"content":[{"type":"text","text":error.to_string()}],"isError":true})
                    }
                })
            }
            _ => Err("method not found"),
        };
        match result {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err(e) => rpc_error(id, -32601, e),
        }
    }
    pub async fn connection(&self, stream: UnixStream, surface: Surface) -> Result<()> {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        while let Some(line) = read_frame(&mut read).await? {
            if !matches!(surface, Surface::Control) {
                let value: Value =
                    serde_json::from_slice(&line).map_err(|_| "invalid MCP relay frame")?;
                let token = value.get("token").map(|v| v.as_str().unwrap_or(""));
                let session = value["session"].as_str().unwrap_or("");
                let response = self
                    .mcp(&surface, token, session, value["request"].clone())
                    .await;
                write_frame(&mut write, &response).await?;
                continue;
            }
            let request: ControlRequest = match serde_json::from_slice(&line) {
                Ok(r) => r,
                Err(_) => {
                    write_frame(&mut write, &json!({"error":"invalid control request"})).await?;
                    continue;
                }
            };
            if let ControlRequest::Watch { cursor, nonce } = request {
                let _permit = match events::subscribe() {
                    Ok(p) => p,
                    Err(e) => {
                        write_frame(&mut write, &json!({"error":e.to_string(),"status":503}))
                            .await?;
                        return Ok(());
                    }
                };
                let resume = cursor
                    .zip(nonce)
                    .map(|(seq, nonce)| crate::mailroom::Anchor { seq, nonce });
                let mut sink = events::UnixSink(write);
                return tokio::select! {
                    result=events::feed(self,&mut sink,resume)=>result,
                    frame=read_frame(&mut read)=>{frame?;Ok(())}
                };
            }
            let held = matches!(request, ControlRequest::Held { .. });
            let response = match self.control(request).await {
                Ok(Value::Array(rows)) if held => {
                    for row in rows {
                        write_frame(&mut write, &json!({"held":row})).await?;
                    }
                    json!({"result":null})
                }
                Ok(v) => json!({"result":v}),
                Err(e) => json!({"error":e.to_string()}),
            };
            write_frame(&mut write, &response).await?;
        }
        Ok(())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendArgs {
    to: String,
    body: Body,
    reply_to: Option<MessageId>,
}
fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
pub fn agent_tools() -> Value {
    json!([
        {"name":"send_message","description":"Send a message to another agent. Busy agents receive it when their current turn ends.",
         "inputSchema":{"type":"object","properties":{"to":{"type":"string"},"body":{"type":"string","minLength":1,"maxLength":65536},"reply_to":{"type":["string","null"]}},"required":["to","body"],"additionalProperties":false}},
        {"name":"list_agents","description":"List agent names, kinds, phases and queued mail.","inputSchema":{"type":"object","properties":{},"additionalProperties":false}}
    ])
}
fn phase_name(phase: &Phase) -> &'static str {
    match phase {
        Phase::Dormant => "dormant",
        Phase::Starting => "starting",
        Phase::Idle => "idle",
        Phase::Busy => "busy",
        Phase::Suspending => "suspending",
        Phase::Ended => "ended",
        Phase::Crashed { .. } => "crashed",
        Phase::Failed { .. } => "failed",
        Phase::NeedsRecovery { .. } => "needs-recovery",
        Phase::Stopped { .. } => "stopped",
    }
}
pub async fn read_frame(read: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<Vec<u8>>> {
    read_frame_limit(read, 1024 * 1024).await
}
pub async fn read_frame_limit(
    read: &mut (impl AsyncBufRead + Unpin),
    limit: u64,
) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let n = read.take(limit).read_until(b'\n', &mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    if line.last() != Some(&b'\n') {
        return Err("frame too large or truncated".into());
    }
    Ok(Some(line))
}
pub async fn write_frame(
    write: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    write.write_all(&bytes).await?;
    write.flush().await?;
    Ok(())
}

#[derive(Clone)]
pub struct SocketIdentity {
    path: PathBuf,
    inode: u64,
    private: Option<Arc<crate::private_fs::Dir>>,
}
impl SocketIdentity {
    pub fn unlink(&self) {
        let _pin = &self.private;
        if std::fs::symlink_metadata(&self.path).is_ok_and(|m| m.ino() == self.inode)
            && let Err(error) = std::fs::remove_file(&self.path)
        {
            tracing::error!(%error,"socket cleanup failed");
        }
    }
}
pub struct Socket {
    pub listener: UnixListener,
    path: PathBuf,
    inode: u64,
    private: Option<Arc<crate::private_fs::Dir>>,
}
impl Socket {
    pub(crate) fn bind_private(dir: Arc<crate::private_fs::Dir>, name: &str) -> Result<Self> {
        Self::bind_at(&dir.socket_path(name)?, Some(dir))
    }
    pub fn bind(path: &Path) -> Result<Self> {
        Self::bind_at(path, None)
    }
    fn bind_at(path: &Path, private: Option<Arc<crate::private_fs::Dir>>) -> Result<Self> {
        if !path.is_absolute() {
            return Err("socket path must be absolute".into());
        }
        let parent = path.parent().ok_or("socket needs parent directory")?;
        if private.is_none() {
            private_dir(parent)?;
        }
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            if !meta.file_type().is_socket() || meta.uid() != nix::unistd::geteuid().as_raw() {
                return Err("refusing to replace a non-socket or foreign socket".into());
            }
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => return Err("socket is already in use".into()),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)?
                }
                Err(e) => return Err(e.into()),
            }
        }
        let listener = UnixListener::bind(path)?;
        if private.is_none() {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let inode = std::fs::symlink_metadata(path)?.ino();
        Ok(Self {
            listener,
            path: path.into(),
            inode,
            private,
        })
    }
    pub fn identity(&self) -> SocketIdentity {
        SocketIdentity {
            path: self.path.clone(),
            inode: self.inode,
            private: self.private.clone(),
        }
    }
    pub fn check_peer(stream: &UnixStream) -> Result<()> {
        if stream.peer_cred()?.uid() != nix::unistd::geteuid().as_raw() {
            return Err("socket peer belongs to another user".into());
        }
        Ok(())
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.identity().unlink();
    }
}
pub fn private_dir(path: &Path) -> Result<()> {
    match std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
    {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != nix::unistd::geteuid().as_raw() || meta.mode() & 0o077 != 0 {
        return Err(
            "state and socket directories must be owned by this user with mode 0700".into(),
        );
    }
    Ok(())
}

pub async fn mcp_stdio(socket: &Path, token: Option<&str>) -> Result<()> {
    let session = bindings::random_namespace()?;
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    while let Some(line) = read_frame(&mut input).await? {
        let request: Value = match serde_json::from_slice(&line) {
            Ok(request) => request,
            Err(_) => {
                write_frame(&mut output, &rpc_error(Value::Null, -32700, "Parse error")).await?;
                continue;
            }
        };
        if request.get("id").is_none() {
            continue;
        }
        let stream = UnixStream::connect(socket).await?;
        Socket::check_peer(&stream)?;
        let (read, mut write) = stream.into_split();
        let mut frame = json!({"session":session,"request":request});
        if let Some(token) = token {
            frame["token"] = json!(token);
        }
        write_frame(&mut write, &frame).await?;
        let response = read_frame(&mut BufReader::new(read))
            .await?
            .ok_or("MCP relay closed")?;
        output.write_all(&response).await?;
        output.flush().await?;
    }
    Ok(())
}

#[cfg(test)]
mod config_tests {
    use super::*;
    use crate::sandbox::{Isolation, SandboxConfig};
    #[test]
    fn isolation_defaults_overrides_and_infinity() {
        for kind in FileConfig::default().kinds().expect("default kinds") {
            assert_eq!(
                matches!(kind.isolation, Isolation::None),
                !cfg!(target_os = "linux")
            );
        }
        let explicit: FileConfig = toml::from_str("sandbox = 'bwrap'").expect("explicit bwrap");
        assert!(
            explicit
                .kinds()
                .expect("explicit kinds")
                .iter()
                .all(|kind| matches!(kind.isolation, Isolation::Bwrap(_)))
        );

        let config: FileConfig = toml::from_str(
            r#"
sandbox = "none"
[kind.claude]
command = ["claude-agent-acp"]
sandbox = "bwrap"
credentials = []
[kind.infinity]
wire = "infinity"
socket = "/host/infinity.sock"
"#,
        )
        .expect("config");
        let kinds = config.kinds().expect("kinds");
        assert!(matches!(
            kinds
                .iter()
                .find(|k| k.name == "claude")
                .expect("claude")
                .isolation,
            Isolation::Bwrap(_)
        ));
        for name in ["codex", "infinity"] {
            assert!(matches!(
                kinds
                    .iter()
                    .find(|k| k.name == name)
                    .expect("kind")
                    .isolation,
                Isolation::None
            ));
        }
        let config: FileConfig = toml::from_str(
            r#"[kind.infinity]
wire = "infinity"
socket = "/host/infinity.sock"
sandbox = "bwrap"
"#,
        )
        .expect("config");
        assert!(config.kinds().is_err());
    }
    #[test]
    fn configured_control_environment_is_rejected() {
        for name in [
            "AGENTD_OVERRIDE",
            "SSH_AUTH_SOCK",
            "DBUS_SESSION_BUS_ADDRESS",
        ] {
            let config: FileConfig = toml::from_str(&format!(
                "[kind.claude]\ncommand = ['cli']\n[kind.claude.env]\n{name} = 'host-control'\n"
            ))
            .expect("config");
            let mut kinds = config.kinds().expect("kinds");
            assert!(
                SandboxConfig::default()
                    .validate(
                        Path::new("/state"),
                        Path::new("/state/a.sock"),
                        Path::new("/state/m.sock"),
                        &mut kinds
                    )
                    .is_err()
            );
        }
    }
}
