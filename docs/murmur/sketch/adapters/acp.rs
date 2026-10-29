//! CLI agents over ACP, as the ACP *client*, using the `agent-client-protocol 0.11.1`
//! crate Infinity already depends on (`role::acp::Client`; schema types from
//! `agent-client-protocol-schema 0.12.0`).
//!
//! One implementation serves Prime Agent (native `--mode acp`) and Claude, Codex and Pi
//! through their registry shims. Differences between them are data (`KindConfig`) or
//! are discovered from the agent's own `initialize` response, never hard-coded per kind.

use std::collections::HashMap;
use std::sync::Arc;

use agent_client_protocol::schema::{
    CancelNotification, ClientCapabilities, FileSystemCapabilities, LoadSessionRequest, McpServer,
    McpServerHttp, McpServerStdio, NewSessionRequest, PermissionOption as AcpOption,
    PermissionOptionKind, PromptRequest, RequestPermissionOutcome, RequestPermissionRequest,
    RequestPermissionResponse, SessionId, SessionNotification, SessionUpdate, StopReason,
};
use async_trait::async_trait;

use crate::adapter::{
    Activity, Adapter, AdapterError, DeliveryGate, EndReason, KindConfig, OpenRequest, Session,
    SessionEvent, Turn, TurnItem, TurnOutcome,
};
use crate::model::{
    Incarnation, MessageId, PermissionEffect, PermissionId, PermissionRequest, ResumeToken,
    SandboxLevel,
};
use crate::surfaces::McpEndpoints;

pub struct AcpAdapter {
    kind: KindConfig,
    mcp: Arc<McpEndpoints>,
}

/// Private token shape; encoded with `ResumeToken::encode`, read only here.
#[derive(serde::Serialize, serde::Deserialize)]
struct AcpResume {
    session_id: String,
}

#[async_trait]
impl Adapter for AcpAdapter {
    /// ACP input is a prompt turn; there is no mid-turn injection channel.
    fn delivery_gate(&self) -> DeliveryGate {
        DeliveryGate::AtTurnBoundary
    }

    /// ```text
    /// argv = kind.command ++ kind.sandbox_args[spec.permissions.sandbox]
    /// child = spawn(argv, cwd = spec.cwd, own process group, kill_on_drop, stderr -> ring buffer)
    /// conn  = Client.builder().connect_with(stdio ByteStreams(child), handlers) // background task
    ///   handlers: session/request_permission -> PermissionReq into the event channel,
    ///             awaits the oneshot that answer_permission fills
    ///             fs/*, terminal/*              -> method_not_found (never advertised)
    /// init  = initialize { protocol V1,
    ///           client_capabilities: ClientCapabilities {
    ///             fs: FileSystemCapabilities { read_text_file: false, write_text_file: false },
    ///             terminal: false } }
    ///   // Advertising no fs/terminal makes every agent use its own tools, so the broker
    ///   // never becomes a file system or terminal host. Its only policy surface is
    ///   // permission requests.
    /// mcp   = self.mcp.endpoint_for(req.agent)
    /// server = if init.agent_capabilities.mcp_capabilities.http
    ///            { McpServer::Http(McpServerHttp { name: "agentd", url: mcp.url,
    ///                                              headers: [Authorization: Bearer mcp.token] }) }
    ///          else { McpServer::Stdio(McpServerStdio { name: "agentd", command: "agentd",
    ///                   args: ["mcp-shim", "--agent", id, "--token", token] }) }
    /// session_id = match req.resume {
    ///   Some(t) => {
    ///     let r: AcpResume = t.decode().ok_or(ResumeFailed("not an ACP token"))?
    ///     if !init.agent_capabilities.load_session { return Err(ResumeUnsupported) }
    ///     session/load { session_id: r.session_id, cwd, mcp_servers: [server] }
    ///       .map_err(|e| ResumeFailed(e))?     // unknown session, corrupt transcript, ..
    ///     // replayed session/update notifications are swallowed until the load returns
    ///     r.session_id
    ///   }
    ///   None => session/new { cwd, mcp_servers: [server] }.session_id
    /// }
    /// // Every resume failure returns Err: the actor moves to NeedsRecovery. There is no
    /// // fallback to session/new, so a lost conversation is never silently replaced.
    /// resumable = init.agent_capabilities.load_session
    /// if let Some(m) = spec.model { session/set_model when the agent offers it; else
    ///   it was passed as a launch flag via kind config }
    /// ```
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError> {
        todo!()
    }
}

pub struct AcpSession {
    session_id: SessionId,
    incarnation: Incarnation,
    /// `initialize` advertised `loadSession`.
    resumable: bool,
    /// Ids carried by the in-flight prompt turn, for `TurnStarted` and `Committed`.
    in_flight: Vec<MessageId>,
    child: tokio::process::Child,
    /// Fed by the connection task: notifications, permission requests, prompt results.
    rx: tokio::sync::mpsc::Receiver<SessionEvent>,
    outbound: AcpOutbound,
    /// ACP option ids per pending request, for validation and answering.
    pending: HashMap<PermissionId, tokio::sync::oneshot::Sender<RequestPermissionOutcome>>,
}

/// Handle to the background connection task (send requests, notifications).
pub struct AcpOutbound {/* agent_client_protocol connection handle */}

#[async_trait]
impl Session for AcpSession {
    /// `ResumeToken::encode(&AcpResume { session_id })`.
    fn resume_token(&self) -> ResumeToken {
        todo!()
    }

    fn resumable(&self) -> bool {
        self.resumable
    }

    /// One prompt per turn, all items as text blocks in order:
    /// `User(env)` -> `env.body` verbatim (it is the user); `Peer{rendered}` -> `rendered`.
    /// `in_flight` = the turn's ids. The prompt request runs in the connection task.
    ///
    /// Receipts: the first `session/update` (or the response, whichever is first) is
    /// turn-start evidence -> `TurnStarted { ids: in_flight }`. The `PromptResponse`
    /// (any stop reason) -> `Committed { ids: in_flight }` then `TurnEnded`. A child
    /// exit in between leaves the ids started but uncommitted: `Uncertain`.
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError> {
        todo!()
    }

    /// `session/cancel` notification. The agent must answer the pending prompt with
    /// `StopReason::Cancelled` and any pending permission requests with `Cancelled`.
    async fn cancel(&mut self) -> Result<(), AdapterError> {
        todo!()
    }

    async fn answer_permission(&mut self, id: PermissionId, key: &str) -> Result<(), AdapterError> {
        // pending.remove(id).send(RequestPermissionOutcome::Selected { option_id: key })
        todo!()
    }

    /// ```text
    /// (every Activity is display-only)
    /// SessionUpdate::AgentMessageChunk    -> Activity::Text
    /// SessionUpdate::AgentThoughtChunk    -> Activity::Thinking
    /// SessionUpdate::ToolCall / Update    -> Activity::ToolCall / ToolResult
    /// RequestPermissionRequest            -> Permission(PermissionRequest {
    ///     incarnation: self.incarnation,
    ///     tool: normalize(tool_call.kind) // read|edit|delete|move|search|execute|fetch|other
    ///     options: map AcpOption.kind: AllowOnce|AllowAlways|RejectOnce|RejectAlways -> effect })
    /// first update / response of a turn   -> TurnStarted { ids: in_flight }
    /// PromptResponse.stop_reason          -> Committed { ids: in_flight.take() },
    ///                                        TurnEnded(EndTurn->Completed, Cancelled,
    ///                                         Refusal->Refused, MaxTokens|MaxTurnRequests->MaxTokens)
    /// child exit                          -> Ended(Exited { code, stderr_tail })
    /// ```
    async fn next_event(&mut self) -> Option<SessionEvent> {
        todo!()
    }

    /// SIGINT the process group, wait up to 5 s, SIGKILL. The session id is the token.
    async fn suspend(self: Box<Self>) -> Result<ResumeToken, AdapterError> {
        todo!()
    }
}
