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
use crate::supervisor::{AgentSpec, Broker, Phase};
use crate::swarm::Act;

type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

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
    pub listen: ListenConfig,
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
    wire: Option<String>,
    command: Vec<String>,
    mode: Option<String>,
    workspace_write_mode: Option<String>,
    full_access_mode: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    idle_after_secs: Option<u64>,
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
        for (name, entry) in self.kind {
            if !matches!(entry.wire.as_deref(), None | Some("acp")) {
                return Err("only ACP kinds are supported".into());
            }
            let mut kind = kinds
                .remove(&name)
                .ok_or("only claude and codex ACP kinds are supported")?;
            let (command, args) = entry.command.split_first().ok_or("empty kind command")?;
            kind.command = command.clone();
            kind.args = args.to_vec();
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
            kind.sandbox_args = entry.sandbox_args;
            kinds.insert(name, kind);
        }
        Ok(kinds.into_values().collect())
    }
}

mod bindings;
use bindings::AuthenticatedSender;
pub use bindings::Bindings;

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
    Held {
        to: Option<String>,
    },
    Release {
        id: MessageId,
    },
    Drop {
        id: MessageId,
    },
    Watch,
}

pub struct ControlApi {
    pub broker: Arc<Broker>,
    pub mailroom: Arc<Mailroom>,
    pub bindings: Arc<Bindings>,
}
impl ControlApi {
    async fn agents(&self) -> Result<Value> {
        let mut rows = Vec::new();
        for (id, spec) in self.mailroom.specs().await? {
            let phase = self
                .broker
                .handle(id)
                .await
                .map_or(Phase::Ended, |h| h.phase());
            rows.push(
                json!({"id":format!("agent:{id}"),"name":format!("@{}",spec.name.as_str()),
                "kind":spec.kind,"phase":phase_name(&phase),
                "queued":self.mailroom.pending_for(id).await?.len()}),
            );
        }
        Ok(json!(rows))
    }
    async fn approvals(&self) -> Result<Vec<PermissionRequest>> {
        let mut requests = Vec::new();
        for (id, _) in self.mailroom.specs().await? {
            if let Some(handle) = self.broker.handle(id).await {
                requests.extend(handle.pending_permissions().await?);
            }
        }
        Ok(requests)
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
        let busy = self
            .broker
            .handle(id)
            .await
            .is_some_and(|h| h.phase() == Phase::Busy);
        let suffix = if busy {
            " (busy; it sees this when its turn ends)"
        } else {
            ""
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
                Ok(json!({"id":format!("agent:{}",handle.id),"name":to}))
            }
            ControlRequest::Ls => self.agents().await,
            ControlRequest::Send { to, body, key } => {
                self.send(Bindings::human(), &to, body, None, key).await
            }
            ControlRequest::Approvals => Ok(json!(self.approvals().await?)),
            ControlRequest::Approve { id, option } => {
                let request = self
                    .approvals()
                    .await?
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
            ControlRequest::Watch => Err("watch requires streaming".into()),
        }
    }
    async fn mcp(&self, bearer: &str, request: Value) -> Value {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let Some(binding) = self.bindings.authenticate_mcp(bearer).await else {
            return rpc_error(id, -32001, "unknown or revoked MCP token");
        };
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
                                binding.key(&id),
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
    pub async fn connection(&self, stream: UnixStream, mcp: bool) -> Result<()> {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        while let Some(line) = read_frame(&mut read).await? {
            if mcp {
                let value: Value =
                    serde_json::from_slice(&line).map_err(|_| "invalid MCP relay frame")?;
                let token = value["token"].as_str().unwrap_or("");
                let response = self.mcp(token, value["request"].clone()).await;
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
            if matches!(request, ControlRequest::Watch) {
                let mut events = self.broker.events();
                write_frame(&mut write,&json!({"event":"snapshot","agents":self.agents().await?,"approvals":self.approvals().await?})).await?;
                loop {
                    tokio::select! {
                        event = events.recv() => {
                            let value = match event {
                                Ok(event) => event_json(event),
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => json!({"event":"lagged","skipped":n}),
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            };
                            write_frame(&mut write,&value).await?;
                        }
                        frame = read_frame(&mut read) => { frame?; break; }
                    }
                }
                return Ok(());
            }
            let response = match self.control(request).await {
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
    }
}
fn event_json(event: crate::supervisor::Event) -> Value {
    use crate::supervisor::Event;
    match event {
        Event::PhaseChanged {
            agent,
            incarnation,
            phase,
        } => {
            json!({"event":"phase_changed","agent":agent,"incarnation":incarnation,"phase":phase_name(&phase)})
        }
        Event::Offered { agent, ids } => json!({"event":"offered","agent":agent,"ids":ids}),
        Event::Started { agent, ids } => json!({"event":"started","agent":agent,"ids":ids}),
        Event::Consumed { agent, ids } => json!({"event":"consumed","agent":agent,"ids":ids}),
        Event::Activity { agent, text } => json!({"event":"activity","agent":agent,"text":text}),
        Event::PermissionPending(request) => {
            json!({"event":"permission_pending","request":request})
        }
        Event::PermissionAnswered { agent, id } => {
            json!({"event":"permission_answered","agent":agent,"id":id})
        }
        Event::TurnEnded { agent, outcome } => {
            json!({"event":"turn_ended","agent":agent,"outcome":format!("{outcome:?}")})
        }
    }
}

pub async fn read_frame(read: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let n = read.take(1024 * 1024).read_until(b'\n', &mut line).await?;
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

pub struct Socket {
    pub listener: UnixListener,
    path: PathBuf,
    inode: u64,
}
impl Socket {
    pub fn bind(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err("socket path must be absolute".into());
        }
        let parent = path.parent().ok_or("socket needs parent directory")?;
        private_dir(parent)?;
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
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let inode = std::fs::symlink_metadata(path)?.ino();
        Ok(Self {
            listener,
            path: path.into(),
            inode,
        })
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
        if std::fs::symlink_metadata(&self.path).is_ok_and(|m| m.ino() == self.inode)
            && let Err(e) = std::fs::remove_file(&self.path)
        {
            tracing::error!(%e,"socket cleanup failed");
        }
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

pub async fn mcp_stdio(socket: &Path, token: &str) -> Result<()> {
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    while let Some(line) = read_frame(&mut input).await? {
        let request: Value = serde_json::from_slice(&line).map_err(|_| "invalid MCP request")?;
        if request.get("id").is_none() {
            continue;
        }
        let stream = UnixStream::connect(socket).await?;
        let (read, mut write) = stream.into_split();
        write_frame(&mut write, &json!({"token":token,"request":request})).await?;
        let response = read_frame(&mut BufReader::new(read))
            .await?
            .ok_or("MCP relay closed")?;
        output.write_all(&response).await?;
        output.flush().await?;
    }
    Ok(())
}
