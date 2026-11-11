use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{Inbound, Inbox, inbox};
use crate::adapter::*;
use crate::model::*;

pub(crate) struct AcpAdapter(pub KindConfig);

#[derive(serde::Serialize, serde::Deserialize)]
struct AcpResume {
    kind: String,
    session_id: String,
}

struct Flight {
    request: u64,
    ids: Vec<MessageId>,
    started: bool,
    cancel_deadline: Option<Instant>,
}
struct Pending {
    wire_id: Value,
    keys: Vec<String>,
}

struct AcpSession {
    child: Child,
    #[cfg(unix)]
    process_group: Option<i32>,
    stdin: ChildStdin,
    reader: JoinHandle<()>,
    stderr_reader: JoinHandle<()>,
    stderr: Arc<Mutex<VecDeque<u8>>>,
    secret: Option<String>,
    rx: Inbox<Result<Value, String>>,
    events: VecDeque<SessionEvent>,
    kind: KindConfig,
    session_id: String,
    agent: AgentId,
    incarnation: Incarnation,
    resumable: bool,
    next_id: u64,
    flight: Option<Flight>,
    pending: HashMap<PermissionId, Pending>,
    /// Titles of this turn's tool calls, newest last, for permission requests that omit one.
    tool_titles: VecDeque<(String, String)>,
    /// Name of agentd's own MCP server in this session, unless a kind server shares it.
    own_server: Option<String>,
    ended: bool,
}

/// Enough recent tool calls to cover one whose approval is pending.
const TOOL_TITLES: usize = 64;

/// Rule name for agentd's own `send_message` tool. No ACP tool kind may claim it.
const SEND_MESSAGE: &str = "send_message";
/// The Claude CLI's `mcpServer.source` for a server passed in for the session, which is how
/// ACP `mcpServers` reach it. A server from user, project or plugin configuration carries
/// that scope instead. Read from CLI 2.1.296; only the `dynamic` case is confirmed live.
const CLAUDE_DYNAMIC_SOURCE: &str = "dynamic";

/// The name of agentd's MCP server in a session. `None` when one of the kind's own servers
/// has the same name, because the adapter's reports could then mean either server.
fn own_server(launch: Option<&Value>, kind_servers: &[Value]) -> Option<String> {
    launch
        .and_then(|server| server["name"].as_str())
        .filter(|name| !kind_servers.iter().any(|s| s["name"] == *name))
        .map(str::to_owned)
}

fn protocol(e: impl std::fmt::Display) -> AdapterError {
    AdapterError::Protocol(e.to_string())
}

#[async_trait]
impl Adapter for AcpAdapter {
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError> {
        let secret = req
            .launch
            .mcp_server
            .as_ref()
            .and_then(|v| v["env"][0]["value"].as_str())
            .map(str::to_owned);
        let own_server = own_server(req.launch.mcp_server.as_ref(), &self.0.mcp_servers);
        let mut cmd = Command::new(&req.launch.program);
        req.launch.inherit_mount_fds(&mut cmd);
        cmd.args(&req.launch.args)
            .current_dir(if req.launch.mount_fds.is_empty() {
                req.spec.cwd.as_path()
            } else {
                std::path::Path::new("/")
            })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        match &req.launch.env {
            crate::sandbox::Env::Clear(env) => {
                cmd.env_clear().envs(env.iter().cloned());
            }
            crate::sandbox::Env::Inherit(env) => {
                cmd.envs(env);
                for name in std::env::vars_os()
                    .map(|(name, _)| name)
                    .chain(env.keys().map(Into::into))
                {
                    if name.to_string_lossy().starts_with("AGENTD_") {
                        cmd.env_remove(name);
                    }
                }
            }
        }
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = cmd
            .spawn()
            .map_err(|e| AdapterError::Spawn(e.to_string()))?;
        let stdin = child.stdin.take().ok_or(AdapterError::Ended)?;
        let stdout = child.stdout.take().ok_or(AdapterError::Ended)?;
        let mut stderr_pipe = child.stderr.take().ok_or(AdapterError::Ended)?;
        let (tx, rx) = inbox(64);
        let stdout_secret = secret.clone();
        let reader = tokio::spawn(async move {
            let mut input = BufReader::new(stdout);
            loop {
                let mut frame = Vec::new();
                // Limit before allocating an unbounded line from a faulty child.
                let read = (&mut input)
                    .take(8 * 1024 * 1024)
                    .read_until(b'\n', &mut frame)
                    .await;
                let result = match read {
                    Ok(0) => break,
                    Ok(_) if frame.last() != Some(&b'\n') => {
                        Err("ACP frame too large or truncated".into())
                    }
                    Ok(_) => {
                        let text = String::from_utf8_lossy(&frame);
                        let redacted = stdout_secret
                            .as_ref()
                            .map_or_else(|| text.to_string(), |s| text.replace(s, "[redacted]"));
                        serde_json::from_str(&redacted).map_err(|e| e.to_string())
                    }
                    Err(e) => Err(e.to_string()),
                };
                let failed = result.is_err();
                if tx.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        let stderr = Arc::new(Mutex::new(VecDeque::new()));
        let tail = Arc::clone(&stderr);
        let stderr_reader = tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                match stderr_pipe.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let mut tail = tail.lock().expect("bug: stderr lock poisoned");
                        tail.extend(&buf[..n]);
                        while tail.len() > 8192 {
                            tail.pop_front();
                        }
                    }
                    Err(e) => {
                        tracing::debug!(%e, "ACP stderr closed");
                        break;
                    }
                }
            }
        });
        let mut session = Box::new(AcpSession {
            #[cfg(unix)]
            process_group: child.id().and_then(|id| i32::try_from(id).ok()),
            child,
            stdin,
            reader,
            stderr_reader,
            stderr,
            secret,
            rx,
            events: VecDeque::new(),
            kind: self.0.clone(),
            session_id: String::new(),
            agent: req.agent,
            incarnation: req.incarnation,
            resumable: false,
            next_id: 1,
            flight: None,
            pending: HashMap::new(),
            tool_titles: VecDeque::new(),
            own_server,
            ended: false,
        });
        let result = session.initialize(&req).await;
        if let Err(error) = result {
            session.terminate().await;
            return Err(if req.resume.is_some() {
                AdapterError::ResumeFailed(error.to_string())
            } else {
                error
            });
        }
        Ok(session)
    }
}

impl AcpSession {
    async fn write(&mut self, value: Value) -> Result<(), AdapterError> {
        let mut bytes = serde_json::to_vec(&value).map_err(protocol)?;
        bytes.push(b'\n');
        tokio::time::timeout(self.kind.request_timeout, self.stdin.write_all(&bytes))
            .await
            .map_err(protocol)?
            .map_err(protocol)
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, AdapterError> {
        let id = self.next_id;
        self.next_id += 1;
        self.write(json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}))
            .await?;
        let timeout = self.kind.request_timeout;
        tokio::time::timeout(timeout, async {
            loop {
                let msg = self
                    .rx
                    .recv()
                    .await
                    .ok_or(AdapterError::Ended)?
                    .map_err(protocol)?;
                if msg.get("method").is_some() {
                    if msg.get("id").is_some() {
                        self.reject_method(&msg).await?;
                    }
                    // History replay during load is display-only, not turn evidence.
                    continue;
                }
                if msg["id"] != id {
                    return Err(protocol("unexpected response during open"));
                }
                if let Some(e) = msg.get("error") {
                    return Err(protocol(e));
                }
                return msg
                    .get("result")
                    .cloned()
                    .ok_or_else(|| protocol("missing result"));
            }
        })
        .await
        .map_err(protocol)?
    }

    async fn initialize(&mut self, req: &OpenRequest<'_>) -> Result<(), AdapterError> {
        let init = self.request("initialize", json!({"protocolVersion":1,
            "clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false},
            "clientInfo":{"name":"agentd","version":env!("CARGO_PKG_VERSION")}})).await?;
        if init["protocolVersion"] != 1 {
            return Err(protocol("unsupported ACP version"));
        }
        self.resumable = init["agentCapabilities"]["loadSession"]
            .as_bool()
            .unwrap_or(false);
        let mut servers = self.kind.mcp_servers.clone();
        servers.extend(req.launch.mcp_server.clone());
        let mut params = json!({"cwd":req.spec.cwd,"mcpServers":servers});
        if let Some(token) = req.resume {
            let saved: AcpResume = token.decode().map_err(protocol)?;
            if saved.kind != self.kind.name {
                return Err(protocol("resume token kind mismatch"));
            }
            if !self.resumable {
                return Err(AdapterError::ResumeUnsupported);
            }
            params["sessionId"] = json!(saved.session_id);
            self.request("session/load", params).await?;
            self.session_id = saved.session_id;
        } else {
            let result = self.request("session/new", params).await?;
            self.session_id = string(&result, "sessionId")?;
        }
        let mode = match req.spec.permissions.sandbox {
            SandboxLevel::ReadOnly => &self.kind.mode,
            SandboxLevel::WorkspaceWrite => self
                .kind
                .workspace_write_mode
                .as_ref()
                .unwrap_or(&self.kind.mode),
            SandboxLevel::FullAccess => self
                .kind
                .full_access_mode
                .as_ref()
                .unwrap_or(&self.kind.mode),
        };
        self.request(
            "session/set_mode",
            json!({"sessionId":self.session_id,"modeId":mode}),
        )
        .await?;
        if let Some(model) = &req.spec.model {
            self.request(
                "session/set_model",
                json!({"sessionId":self.session_id,"modelId":model}),
            )
            .await?;
        }
        Ok(())
    }

    async fn reject_method(&mut self, msg: &Value) -> Result<(), AdapterError> {
        self.write(json!({"jsonrpc":"2.0","id":msg["id"],"error":{"code":-32601,"message":"client capability disabled"}})).await
    }

    fn started(&mut self) {
        if let Some(flight) = &mut self.flight
            && !flight.started
        {
            flight.started = true;
            self.events.push_back(SessionEvent::TurnStarted {
                ids: flight.ids.clone(),
            });
        }
    }

    async fn receive(&mut self, msg: Value) -> Result<(), AdapterError> {
        if let Some(method) = msg["method"].as_str() {
            if method == "session/update" || method == "session/request_permission" {
                if msg["params"]["sessionId"] != self.session_id {
                    return Err(protocol("wrong session ID"));
                }
                if method == "session/request_permission" && self.flight.is_none() {
                    return Err(protocol("permission outside a turn"));
                }
            }
            match method {
                "session/update" => {
                    let update = &msg["params"]["update"];
                    // Session metadata and user echoes can arrive between turns.
                    if matches!(
                        update["sessionUpdate"].as_str(),
                        Some(
                            "agent_message_chunk"
                                | "agent_thought_chunk"
                                | "plan"
                                | "tool_call"
                                | "tool_call_update"
                        )
                    ) {
                        self.started();
                    }
                    let text = match update["sessionUpdate"].as_str() {
                        Some("agent_message_chunk" | "agent_thought_chunk") => {
                            update["content"]["text"].as_str()
                        }
                        Some("tool_call" | "tool_call_update") => {
                            self.remember_title(update);
                            Some(update["title"].as_str().unwrap_or("tool activity"))
                        }
                        Some("plan") => Some("plan updated"),
                        _ => None,
                    };
                    if let Some(text) = text {
                        self.events.push_back(SessionEvent::Activity(text.into()));
                    }
                }
                "session/request_permission" => {
                    self.started();
                    if self
                        .flight
                        .as_ref()
                        .is_some_and(|f| f.cancel_deadline.is_some())
                    {
                        self.write(json!({"jsonrpc":"2.0","id":msg["id"],"result":{"outcome":{"outcome":"cancelled"}}})).await?;
                        return Ok(());
                    }
                    let params = &msg["params"];
                    let options = params["options"]
                        .as_array()
                        .ok_or_else(|| protocol("missing permission options"))?
                        .iter()
                        .map(|o| {
                            Ok(PermissionOption {
                                key: string(o, "optionId")?,
                                label: string(o, "name")?,
                                effect: match o["kind"].as_str() {
                                    Some("allow_once") => PermissionEffect::AllowOnce,
                                    Some("allow_always") => PermissionEffect::AllowAlways,
                                    Some("reject_once") => PermissionEffect::RejectOnce,
                                    Some("reject_always") => PermissionEffect::RejectAlways,
                                    _ => return Err(protocol("unknown permission effect")),
                                },
                            })
                        })
                        .collect::<Result<Vec<_>, AdapterError>>()?;
                    if !params["toolCall"].is_object() {
                        return Err(protocol("missing toolCall"));
                    }
                    let id = PermissionId::new();
                    let title = self.permission_title(&params["toolCall"]);
                    let tool = self.permission_tool(params).into();
                    self.pending.insert(
                        id,
                        Pending {
                            wire_id: msg
                                .get("id")
                                .cloned()
                                .ok_or_else(|| protocol("permission missing id"))?,
                            keys: options.iter().map(|o| o.key.clone()).collect(),
                        },
                    );
                    self.events
                        .push_back(SessionEvent::PermissionRequested(PermissionRequest {
                            id,
                            agent: self.agent,
                            incarnation: self.incarnation,
                            tool,
                            title,
                            detail: params["toolCall"].to_string(),
                            options,
                        }));
                }
                _ if msg.get("id").is_some() => self.reject_method(&msg).await?,
                _ => {}
            }
        } else {
            let flight = self
                .flight
                .as_ref()
                .ok_or_else(|| protocol("unexpected response"))?;
            if msg["id"] != flight.request {
                return Err(protocol("wrong prompt response ID"));
            }
            if let Some(error) = msg.get("error") {
                return Err(protocol(error));
            }
            let outcome = match msg["result"]["stopReason"].as_str() {
                Some("end_turn") => TurnOutcome::Completed,
                Some("cancelled") => TurnOutcome::Cancelled,
                Some("refusal") => TurnOutcome::Refused,
                Some("max_tokens" | "max_turn_requests") => TurnOutcome::MaxTokens,
                _ => return Err(protocol("unknown prompt result")),
            };
            self.started();
            let flight = self.flight.take().ok_or(AdapterError::Ended)?;
            self.pending.clear();
            self.tool_titles.clear();
            self.events
                .push_back(SessionEvent::Committed { ids: flight.ids });
            self.events.push_back(SessionEvent::TurnEnded { outcome });
        }
        Ok(())
    }

    fn remember_title(&mut self, update: &Value) {
        let (Some(id), Some(title)) = (
            update["toolCallId"].as_str(),
            update["title"].as_str().filter(|t| !t.is_empty()),
        ) else {
            return;
        };
        self.tool_titles.retain(|(known, _)| known != id);
        if self.tool_titles.len() == TOOL_TITLES {
            self.tool_titles.pop_front();
        }
        self.tool_titles.push_back((id.into(), title.into()));
    }

    /// Names agentd's own `send_message` from fields claude-agent-acp sets itself, never from
    /// the title or arguments, which the model or another MCP server controls.
    ///
    /// codex-acp gets no such name. It identifies an MCP call by server name alone, and it
    /// drops the server agentd passes in when Codex's own configuration defines one with the
    /// same name. The name then belongs to a server agentd did not start.
    fn permission_tool<'a>(&self, params: &'a Value) -> &'a str {
        let call = &params["toolCall"];
        let claude = &call["_meta"]["claudeCode"];
        let own = self.own_server.as_deref().is_some_and(|server| {
            claude["mcpServer"]["name"] == server
                && claude["mcpServer"]["source"] == CLAUDE_DYNAMIC_SOURCE
                && claude["toolName"] == format!("mcp__{server}__{SEND_MESSAGE}")
        });
        if own {
            return SEND_MESSAGE;
        }
        match call["kind"].as_str() {
            Some("execute") => "shell",
            Some(SEND_MESSAGE) => "other",
            Some(other) => other,
            None => "unknown",
        }
    }

    /// ACP makes `title` optional here, and codex-acp omits it when it approves an MCP call it
    /// already announced, so fall back to that call's title, then to `name` and `kind`.
    fn permission_title(&self, tool_call: &Value) -> String {
        let text = |key| tool_call[key].as_str().filter(|s: &&str| !s.is_empty());
        if let Some(title) = text("title") {
            return title.into();
        }
        if let Some(id) = text("toolCallId")
            && let Some((_, title)) = self.tool_titles.iter().rev().find(|(known, _)| known == id)
        {
            return title.clone();
        }
        match (text("name"), text("kind")) {
            (Some(name), Some(kind)) => format!("{name} ({kind})"),
            (Some(name), None) => name.into(),
            (None, Some(kind)) => format!("{kind} tool call"),
            (None, None) => "unnamed tool call".into(),
        }
    }

    fn kill_group(&self) {
        #[cfg(unix)]
        if let Some(pid) = self.process_group {
            use nix::sys::signal::{Signal, killpg};
            use nix::unistd::Pid;
            if let Err(e) = killpg(Pid::from_raw(pid), Signal::SIGKILL)
                && e != nix::errno::Errno::ESRCH
            {
                tracing::error!(%e, "ACP group kill failed");
            }
        }
    }
    async fn terminate(&mut self) {
        self.kill_group();
        if let Err(e) = self.child.kill().await {
            tracing::debug!(%e, "ACP process already exited");
        }
        if let Err(e) = self.child.wait().await {
            tracing::error!(%e, "ACP reap failed");
        }
        #[cfg(unix)]
        {
            self.process_group = None;
        }
        self.reader.abort();
        self.stderr_reader.abort();
        self.ended = true;
    }
}

#[async_trait]
impl Session for AcpSession {
    fn resume_token(&self) -> ResumeToken {
        ResumeToken::encode(&AcpResume {
            kind: self.kind.name.clone(),
            session_id: self.session_id.clone(),
        })
    }
    fn resumable(&self) -> bool {
        self.resumable
    }
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError> {
        if self.ended {
            return Err(AdapterError::Ended);
        }
        if self.flight.is_some() || turn.items.is_empty() {
            return Err(protocol("prompt requires idle session and nonempty turn"));
        }
        let mut ids = Vec::new();
        let prompt: Vec<_> = turn
            .items
            .into_iter()
            .map(|item| {
                let text = match item {
                    TurnItem::User(env) => {
                        ids.push(env.id());
                        let content = crate::surfaces::acts::content(&env);
                        match crate::surfaces::acts::task_note(&env) {
                            Some(note) => format!("{content}\n\n{note}"),
                            None => content,
                        }
                    }
                    TurnItem::Peer { envelope, rendered } => {
                        ids.push(envelope.id());
                        rendered
                    }
                };
                json!({"type":"text","text":text})
            })
            .collect();
        let request = self.next_id;
        self.next_id += 1;
        self.flight = Some(Flight {
            request,
            ids,
            started: false,
            cancel_deadline: None,
        });
        self.write(
            json!({"jsonrpc":"2.0","id":request,"method":"session/prompt",
            "params":{"sessionId":self.session_id,"prompt":prompt}}),
        )
        .await
    }
    async fn cancel(&mut self) -> Result<(), AdapterError> {
        if let Some(flight) = &mut self.flight
            && flight.cancel_deadline.is_none()
        {
            flight.cancel_deadline = Some(Instant::now() + self.kind.cancel_timeout);
            self.events
                .retain(|event| !matches!(event, SessionEvent::PermissionRequested(_)));
            for (_, pending) in std::mem::take(&mut self.pending) {
                self.write(json!({"jsonrpc":"2.0","id":pending.wire_id,"result":{"outcome":{"outcome":"cancelled"}}})).await?;
            }
            self.write(json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":self.session_id}})).await?;
        }
        Ok(())
    }
    async fn answer_permission(&mut self, id: PermissionId, key: &str) -> Result<(), AdapterError> {
        let pending = self
            .pending
            .get(&id)
            .ok_or_else(|| protocol("stale permission"))?;
        if !pending.keys.iter().any(|k| k == key) {
            return Err(protocol("invalid permission option"));
        }
        self.write(json!({"jsonrpc":"2.0","id":pending.wire_id,
            "result":{"outcome":{"outcome":"selected","optionId":key}}}))
            .await?;
        self.pending.remove(&id);
        Ok(())
    }
    async fn next_event(&mut self) -> Option<SessionEvent> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Some(event);
            }
            if self.ended {
                return None;
            }
            let cancel_deadline = self.flight.as_ref().and_then(|f| f.cancel_deadline);
            let incoming = tokio::select! {
                biased;
                inbound = self.rx.recv_by(cancel_deadline) => match inbound {
                    Inbound::Message(message) => message,
                    Inbound::Closed => Err("ACP stdout closed".into()),
                    Inbound::Expired => Err("ACP cancel completion deadline exceeded".into()),
                },
                status = self.child.wait() => Err(format!("ACP exited: {status:?}")),
            };
            let result = match incoming {
                Ok(msg) => self.receive(msg).await,
                Err(e) => Err(protocol(e)),
            };
            if let Err(e) = result {
                self.terminate().await;
                let tail = self
                    .stderr
                    .lock()
                    .expect("bug: stderr lock poisoned")
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                let tail = String::from_utf8_lossy(&tail);
                let tail = self
                    .secret
                    .as_ref()
                    .map_or_else(|| tail.to_string(), |s| tail.replace(s, "[redacted]"));
                // Deliver evidence queued before the failure (e.g. TurnStarted) first.
                self.events.push_back(SessionEvent::Ended {
                    reason: format!("{e}; {}", tail),
                });
                self.ended = true;
            }
        }
    }
    async fn suspend(mut self: Box<Self>) -> Result<ResumeToken, AdapterError> {
        self.terminate().await;
        Ok(self.resume_token())
    }
}

impl Drop for AcpSession {
    fn drop(&mut self) {
        self.kill_group();
        self.reader.abort();
        self.stderr_reader.abort();
    }
}

fn string(value: &Value, key: &str) -> Result<String, AdapterError> {
    value[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(Into::into)
        .ok_or_else(|| protocol(format!("missing {key}")))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::adapters::InboxSender;

    type Incoming = InboxSender<Result<Value, String>>;

    const PROMPT: u64 = 1;

    /// A session that asked its agent to cancel `PROMPT`, with the agent's stdout in the test's hands.
    fn cancelling_session() -> (AcpSession, Incoming) {
        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn stand-in agent");
        let stdin = child.stdin.take().expect("stand-in stdin");
        let (incoming, rx) = inbox(64);
        let kind = KindConfig::claude("cat");
        let session = AcpSession {
            child,
            process_group: None,
            stdin,
            reader: tokio::spawn(async {}),
            stderr_reader: tokio::spawn(async {}),
            stderr: Arc::new(Mutex::new(VecDeque::new())),
            secret: None,
            rx,
            events: VecDeque::new(),
            session_id: "session".into(),
            agent: AgentId::new(),
            incarnation: Incarnation(1),
            resumable: false,
            next_id: PROMPT + 1,
            flight: Some(Flight {
                request: PROMPT,
                ids: Vec::new(),
                started: true,
                cancel_deadline: Some(Instant::now() + kind.cancel_timeout),
            }),
            pending: HashMap::new(),
            tool_titles: VecDeque::new(),
            own_server: None,
            ended: false,
            kind,
        };
        (session, incoming)
    }

    /// A session mid-turn whose agentd MCP server is registered as `agentd`.
    fn turn_session() -> AcpSession {
        let (mut session, _incoming) = cancelling_session();
        session.own_server = Some("agentd".into());
        if let Some(flight) = &mut session.flight {
            flight.cancel_deadline = None;
        }
        session
    }

    /// The rule name agentd gives a permission request after the agent sent `updates` first.
    async fn rule_tool(session: &mut AcpSession, updates: &[Value], params: Value) -> String {
        for update in updates {
            session
                .receive(json!({"jsonrpc":"2.0","method":"session/update",
                    "params":{"sessionId":"session","update":update}}))
                .await
                .expect("tool call update");
        }
        let mut params = params;
        params["sessionId"] = json!("session");
        params["options"] = json!([{"optionId":"yes","name":"Allow once","kind":"allow_once"}]);
        session
            .receive(json!({"jsonrpc":"2.0","id":"permission-1",
                "method":"session/request_permission","params":params}))
            .await
            .expect("permission request");
        session
            .events
            .drain(..)
            .find_map(|event| match event {
                SessionEvent::PermissionRequested(request) => Some(request.tool),
                _ => None,
            })
            .expect("permission event")
    }

    /// The `toolCall` claude-agent-acp sends for an MCP tool: `presentation.js` plus the
    /// `_meta.claudeCode` that `acp-agent.js` adds when the CLI names the tool's server.
    fn claude_call(server: &str, source: &str, tool: &str) -> Value {
        let name = format!("mcp__{server}__{tool}");
        json!({"toolCall":{"toolCallId":"toolu_1","name":name,"status":"pending",
            "rawInput":{"to":"@b","body":"hi"},"title":name,"kind":"other","content":[],
            "_meta":{"claudeCode":{"toolName":name,"mcpServer":{"name":server,"source":source}}}}})
    }

    /// The `tool_call` codex-acp announces for an MCP call (`McpToolReporter.started`).
    fn codex_started(server: &str, tool: &str) -> Value {
        json!({"sessionUpdate":"tool_call","toolCallId":"call-1","kind":"execute",
            "title":format!("mcp.{server}.{tool}"),"status":"pending",
            "rawInput":{"server":server,"tool":tool,"arguments":{"to":"@b","body":"hi"}}})
    }

    /// codex-acp's approval of an announced MCP call (`buildMcpPermissionRequest`).
    fn codex_approval() -> Value {
        json!({"toolCall":{"toolCallId":"call-1","kind":"execute","status":"pending"},
            "_meta":{"is_mcp_tool_approval":true}})
    }

    #[tokio::test]
    async fn claude_own_send_message_gets_the_send_message_rule_name() {
        let mut session = turn_session();
        let call = claude_call("agentd", "dynamic", "send_message");
        assert_eq!(rule_tool(&mut session, &[], call).await, "send_message");
    }

    /// Codex's own configuration can define a server named `agentd`, and codex-acp then
    /// skips the one agentd passes in. The server name in the announcement proves nothing.
    #[tokio::test]
    async fn codex_send_message_approval_keeps_the_shell_rule_name() {
        let mut session = turn_session();
        let started = codex_started("agentd", "send_message");
        assert_eq!(
            rule_tool(&mut session, &[started], codex_approval()).await,
            "shell"
        );
    }

    #[tokio::test]
    async fn other_mcp_servers_send_message_does_not_get_the_rule_name() {
        let mut other_server = claude_call("agentd", "dynamic", "send_message");
        other_server["toolCall"]["_meta"]["claudeCode"]["mcpServer"]["name"] = json!("mail");
        let cases = [
            (
                "claude, tool name says agentd but the server is another",
                vec![],
                other_server,
            ),
            (
                "claude, other server",
                vec![],
                claude_call("mail", "dynamic", "send_message"),
            ),
            (
                "claude, agentd from project config",
                vec![],
                claude_call("agentd", "project", "send_message"),
            ),
            (
                "claude, agentd from a plugin",
                vec![],
                claude_call("agentd", "plugin", "send_message"),
            ),
            (
                "claude, other agentd tool",
                vec![],
                claude_call("agentd", "dynamic", "list_agents"),
            ),
        ];
        for (case, updates, params) in cases {
            let mut session = turn_session();
            let tool = rule_tool(&mut session, &updates, params).await;
            assert_ne!(tool, "send_message", "{case}");
        }
    }

    #[tokio::test]
    async fn text_that_mentions_send_message_does_not_get_the_rule_name() {
        let shell = json!({"toolCall":{"toolCallId":"call-1","kind":"execute",
            "title":"mcp__agentd__send_message","name":"send_message",
            "rawInput":{"command":"echo mcp.agentd.send_message","server":"agentd","tool":"send_message"}},
            "_meta":{"is_mcp_tool_approval":true}});
        let spoofed_args = json!({"sessionUpdate":"tool_call","toolCallId":"toolu_1",
            "title":"mcp.agentd.send_message","kind":"other",
            "rawInput":{"server":"agentd","tool":"send_message","arguments":{}}});
        let other_tool = json!({"toolCall":{"toolCallId":"toolu_1","kind":"other",
            "name":"mcp__mail__relay","title":"mcp.agentd.send_message",
            "_meta":{"claudeCode":{"toolName":"mcp__mail__relay","mcpServer":{"name":"mail","source":"dynamic"}}}}});
        let cases = [
            ("shell command", vec![], shell),
            (
                "another server's tool with agentd-shaped arguments",
                vec![spoofed_args],
                other_tool,
            ),
            (
                "kind named send_message",
                vec![],
                json!({"toolCall":{"toolCallId":"x","kind":"send_message"}}),
            ),
        ];
        for (case, updates, params) in cases {
            let mut session = turn_session();
            let tool = rule_tool(&mut session, &updates, params).await;
            assert_ne!(tool, "send_message", "{case}");
        }
    }

    #[tokio::test]
    async fn incomplete_or_malformed_tool_calls_do_not_get_the_rule_name() {
        let mut no_server = claude_call("agentd", "dynamic", "send_message");
        no_server["toolCall"]["_meta"]["claudeCode"]["mcpServer"] = json!(null);
        let mut no_tool_name = claude_call("agentd", "dynamic", "send_message");
        no_tool_name["toolCall"]["_meta"]["claudeCode"]["toolName"] = json!(null);
        let mut string_meta = claude_call("agentd", "dynamic", "send_message");
        string_meta["toolCall"]["_meta"] = json!("mcp__agentd__send_message");
        let cases = [
            ("claude without mcpServer", vec![], no_server),
            ("claude without toolName", vec![], no_tool_name),
            ("claude with string _meta", vec![], string_meta),
            (
                "bare tool call",
                vec![],
                json!({"toolCall":{"toolCallId":"call-1"}}),
            ),
            ("empty tool call", vec![], json!({"toolCall":{}})),
        ];
        for (case, updates, params) in cases {
            let mut session = turn_session();
            let tool = rule_tool(&mut session, &updates, params).await;
            assert_ne!(tool, "send_message", "{case}");
        }
    }

    #[test]
    fn a_kind_server_with_agentds_name_leaves_no_own_server() {
        let launch = json!({"name":"agentd","command":"agentd"});
        assert_eq!(own_server(Some(&launch), &[]), Some("agentd".into()));
        assert_eq!(
            own_server(Some(&launch), &[json!({"name":"mail"})]),
            Some("agentd".into())
        );
        assert_eq!(
            own_server(
                Some(&launch),
                &[json!({"name":"mail"}), json!({"name":"agentd"})]
            ),
            None
        );
        assert_eq!(own_server(None, &[]), None);
        assert_eq!(own_server(Some(&json!({"command":"agentd"})), &[]), None);
    }

    #[tokio::test]
    async fn without_an_unambiguous_agentd_server_nothing_gets_the_rule_name() {
        let mut session = turn_session();
        session.own_server = None;
        let call = claude_call("agentd", "dynamic", "send_message");
        assert_ne!(rule_tool(&mut session, &[], call).await, "send_message");
    }

    fn cancelled() -> Result<Value, String> {
        Ok(json!({"jsonrpc":"2.0","id":PROMPT,"result":{"stopReason":"cancelled"}}))
    }

    #[tokio::test(start_paused = true)]
    async fn response_that_arrived_before_the_cancel_deadline_counts_when_polled_late() {
        let (mut session, incoming) = cancelling_session();
        incoming.send(cancelled()).await.expect("queue response");
        tokio::time::advance(session.kind.cancel_timeout * 2).await;
        let event = session.next_event().await;
        assert!(
            matches!(event, Some(SessionEvent::Committed { .. })),
            "the agent answered in time, but the session reported {event:?}"
        );
        let event = session.next_event().await;
        assert!(
            matches!(
                event,
                Some(SessionEvent::TurnEnded {
                    outcome: TurnOutcome::Cancelled
                })
            ),
            "the agent answered in time, but the session reported {event:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn response_that_arrived_after_the_cancel_deadline_does_not_count() {
        let (mut session, incoming) = cancelling_session();
        tokio::time::advance(session.kind.cancel_timeout * 2).await;
        incoming.send(cancelled()).await.expect("queue response");
        let event = session.next_event().await;
        assert!(
            matches!(&event, Some(SessionEvent::Ended { reason }) if reason.contains("cancel completion deadline exceeded")),
            "the agent answered late, but the session reported {event:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn agent_that_exits_during_a_cancel_ends_the_session_before_the_deadline() {
        let (mut session, _incoming) = cancelling_session();
        session.child.start_kill().expect("kill stand-in agent");
        session.child.wait().await.expect("reap stand-in agent");
        let event = session.next_event().await;
        assert!(
            matches!(&event, Some(SessionEvent::Ended { reason }) if reason.contains("ACP exited")),
            "the agent exited, but the session reported {event:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn silent_agent_times_out_the_cancel() {
        let (mut session, _incoming) = cancelling_session();
        let event = session.next_event().await;
        assert!(
            matches!(&event, Some(SessionEvent::Ended { reason }) if reason.contains("cancel completion deadline exceeded")),
            "a silent agent must time out, but the session reported {event:?}"
        );
    }
}
