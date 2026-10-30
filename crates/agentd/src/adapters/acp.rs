use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

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
    rx: mpsc::Receiver<Result<Value, String>>,
    events: VecDeque<SessionEvent>,
    kind: KindConfig,
    session_id: String,
    agent: AgentId,
    incarnation: Incarnation,
    resumable: bool,
    next_id: u64,
    flight: Option<Flight>,
    pending: HashMap<PermissionId, Pending>,
    ended: bool,
}

fn protocol(e: impl std::fmt::Display) -> AdapterError {
    AdapterError::Protocol(e.to_string())
}

#[async_trait]
impl Adapter for AcpAdapter {
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError> {
        let mut cmd = Command::new(&self.0.command);
        cmd.args(&self.0.args)
            .envs(&self.0.env)
            .current_dir(&req.spec.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        cmd.args(match req.spec.permissions.sandbox {
            SandboxLevel::ReadOnly => &self.0.sandbox_args.read_only,
            SandboxLevel::WorkspaceWrite => &self.0.sandbox_args.workspace_write,
            SandboxLevel::FullAccess => &self.0.sandbox_args.full_access,
        });
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = cmd
            .spawn()
            .map_err(|e| AdapterError::Spawn(e.to_string()))?;
        let stdin = child.stdin.take().ok_or(AdapterError::Ended)?;
        let stdout = child.stdout.take().ok_or(AdapterError::Ended)?;
        let mut stderr_pipe = child.stderr.take().ok_or(AdapterError::Ended)?;
        let (tx, rx) = mpsc::channel(64);
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
                    Ok(_) => serde_json::from_slice(&frame).map_err(|e| e.to_string()),
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
        let mut params = json!({"cwd":req.spec.cwd,"mcpServers":self.kind.mcp_servers});
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
        self.request(
            "session/set_mode",
            json!({"sessionId":self.session_id,"modeId":self.kind.mode}),
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
                        Some("tool_call" | "tool_call_update") => update["title"].as_str(),
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
                    let id = PermissionId::new();
                    let title = string(&params["toolCall"], "title")?;
                    let tool = match params["toolCall"]["kind"].as_str() {
                        Some("execute") => "shell",
                        Some("edit") => "edit",
                        Some("fetch") => "fetch",
                        Some(other) => other,
                        None => "unknown",
                    }
                    .into();
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
            self.events
                .push_back(SessionEvent::Committed { ids: flight.ids });
            self.events.push_back(SessionEvent::TurnEnded { outcome });
        }
        Ok(())
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
                        env.body().as_str().into()
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
                () = async {
                    match cancel_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => Err("ACP cancel completion deadline exceeded".into()),
                msg = self.rx.recv() => msg.unwrap_or_else(|| Err("ACP stdout closed".into())),
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
                // Deliver evidence queued before the failure (e.g. TurnStarted) first.
                self.events.push_back(SessionEvent::Ended {
                    reason: format!("{e}; {}", String::from_utf8_lossy(&tail)),
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
