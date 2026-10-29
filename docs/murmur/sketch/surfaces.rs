//! Everything that talks to the outside. Four skins over the same five `Broker`
//! operations; no surface has logic of its own beyond parsing and caller identity.
//!
//! | surface        | who calls it            | transport                         | caller identity (`bindings`)        |
//! |----------------|-------------------------|-----------------------------------|-------------------------------------|
//! | control API    | `agentctl`, scripts     | Unix socket, 0600, JSON lines     | `Bindings::human` (socket is 0600)  |
//! | event bus      | graph UI, watchdog      | WS `127.0.0.1:<port>/events`      | read-only                           |
//! | RAP toolset    | Infinity model agents   | HTTP, registered in rap.json      | `Bindings::authenticate` (bound root thread) |
//! | MCP endpoint   | CLI agents              | Streamable HTTP or stdio shim     | `Bindings::authenticate_mcp` (session token) |
//!
//! Sender identity never comes from a tool argument: `Broker::send` takes an
//! `AuthenticatedSender`, and only `bindings` can mint one.

use std::sync::Arc;

use rap_protocol::{RapCallback, RapInvocation, RapToolResult, ToolDef, ToolsetManifest};

use crate::model::{AgentId, AgentName, ClientKey, Incarnation, KindName, MessageId, PermissionId, Sender};
use crate::Broker;

pub use bindings::{AuthError, AuthenticatedSender, Bindings, McpEndpoint, McpEndpoints};

impl ClientKey {
    /// RAP retries reuse the tool call id, so a retried `send_message` is a `Duplicate`.
    pub fn from_tool_call(inv: &RapInvocation) -> Self {
        todo!() // ClientKey(inv.id as string)
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct ListenConfig {
    pub control_socket: std::path::PathBuf, // ~/.local/state/agentd/agentd.sock
    pub http_port: u16,                     // RAP + MCP + WS events, bound to 127.0.0.1
}

// ---------------------------------------------------------------- agent-facing tools

/// The agent-facing tool set, defined once and rendered into both a RAP
/// `ToolsetManifest` and an MCP tool list, so model agents and CLI agents see the same
/// tools with the same descriptions.
///
/// - `send_message { to: "@name" | "agent:<ulid>", body, reply_to? }`
///   -> "Queued 01J.. for @reviewer (busy; it sees this when its turn ends)."
/// - `list_agents {}` -> name, kind, state, one-line purpose for each agent.
/// - `answer_permission { permission_id, option }` -> `Broker::answer_permission` with
///   the caller's `AuthenticatedSender`; the actor accepts it only from the agent named
///   as `Escalation::Agent` for the requester, and only for the live incarnation.
pub fn agent_tools() -> Vec<ToolDef> {
    todo!()
}

/// RAP toolset for Infinity. Registered once in `~/.infinity/rap.json`, zero fork change:
///
/// ```json
/// { "type": "toolset_server", "id": "agentd", "server_url": "http://127.0.0.1:7717/rap" }
/// ```
///
/// `toolset_server` entries are never stopped by the daemon's idle logic, which suits a
/// separately supervised process.
pub struct RapSurface {
    broker: Arc<Broker>,
    bindings: Arc<Bindings>,
}

impl RapSurface {
    /// `GET /rap/.well-known/rap-toolset`
    pub fn manifest(&self) -> ToolsetManifest {
        todo!()
    }

    /// `POST /rap/invoke`. Returns 200 at once, works in a task, then POSTs
    /// `RapCallback::ToolResult(RapToolResult { id: inv.id, group_id: inv.group_id, .. })`
    /// to `inv.callback_url` (the protocol's normal fire-and-forget shape).
    ///
    /// ```text
    /// let from = bindings.authenticate(&inv)?      // AuthenticatedSender, unforgeable
    ///     // else -> error result "this Infinity session is not an agentd agent"
    /// broker.send(from, Recipient::parse(&to)?, Body::new(body)?, reply_to,
    ///             ClientKey::from_tool_call(&inv))  // idempotent on retry
    /// ```
    ///
    /// Child threads of a model agent send as that agent (the root is the addressable
    /// unit; Infinity's own tree messaging stays internal).
    pub async fn invoke(&self, inv: RapInvocation) {
        todo!()
    }
}

/// Caller identity. The only module that can construct an `AuthenticatedSender`: its
/// field is private here, so `Broker::send` cannot be called with a sender a surface
/// made up.
pub mod bindings {
    use std::sync::Arc;

    use rap_protocol::RapInvocation;

    use crate::adapters::infinity::InfinityAdapter;
    use crate::model::{AgentId, Sender};

    /// A `Sender` proven by the transport, not claimed by an argument.
    #[derive(Clone, Debug)]
    pub struct AuthenticatedSender(Sender);

    impl AuthenticatedSender {
        pub fn sender(&self) -> &Sender {
            &self.0
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum AuthError {
        #[error("this Infinity session is not an agentd agent")]
        UnboundThread,
        #[error("unknown or revoked MCP token")]
        BadToken,
    }

    pub struct Bindings {
        /// Root thread -> agent, read from the Infinity adapter (only it can decode
        /// Infinity resume tokens).
        infinity: Arc<InfinityAdapter>,
        mcp: Arc<McpEndpoints>,
    }

    impl Bindings {
        /// RAP: the invocation's bound thread. `root = inv.thread_ancestors
        /// .as_ref().and_then(|a| a.first()).unwrap_or(&inv.group_id)`, then
        /// `infinity.agent_for_root(root)` -> `Sender::Agent(id)`.
        pub fn authenticate(&self, inv: &RapInvocation) -> Result<AuthenticatedSender, AuthError> {
            todo!()
        }

        /// MCP: the bearer token minted for one agent's session.
        pub async fn authenticate_mcp(&self, bearer: &str) -> Result<AuthenticatedSender, AuthError> {
            todo!()
        }

        /// Control socket: mode 0600 in the user's state dir, so any peer is the user.
        pub fn human(&self) -> AuthenticatedSender {
            AuthenticatedSender(Sender::Human)
        }
    }

    /// Per-agent MCP endpoints for CLI agents. MCP carries no caller identity, so each
    /// agent session gets its own bearer token, minted when its session opens and
    /// revoked when it ends. The stdio shim (`agentd mcp-shim --agent <id> --token <t>`)
    /// forwards to the control socket for agents without HTTP MCP support.
    pub struct McpEndpoints {
        tokens: tokio::sync::RwLock<std::collections::HashMap<String, AgentId>>,
    }

    pub struct McpEndpoint {
        pub url: String, // http://127.0.0.1:7717/mcp
        pub token: String,
    }

    impl McpEndpoints {
        pub async fn endpoint_for(&self, agent: AgentId) -> McpEndpoint {
            todo!()
        }
        pub async fn revoke(&self, agent: AgentId) {
            todo!()
        }
    }
}

// ---------------------------------------------------------------- operator surface

/// `agentctl` over the control socket. Every request runs as `bindings.human()`.
///
/// ```text
/// spawn | send | cancel | stop | approve   -> the five Broker methods
/// ls | approvals | watch                   -> registry / event bus snapshot
/// restart @x                               -> Command::Restart (Failed, NeedsRecovery)
/// held [@x] | release <id> | drop <id>     -> Broker::{held, release, drop_held}
///                                             (pub(crate) operator methods, not facade)
/// ```
pub struct ControlApi {
    broker: Arc<Broker>,
    bindings: Arc<Bindings>,
}

// ---------------------------------------------------------------- UI and watchdog

/// Snapshot-then-live stream, the same pattern as Infinity's `Replay` then events.
/// A client connecting to `/events` first gets `Snapshot`, then every event.
#[derive(Clone)]
pub struct EventBus {
    tx: tokio::sync::broadcast::Sender<BrokerEvent>,
}

impl EventBus {
    pub fn publish(&self, ev: BrokerEvent) {
        let _ = self.tx.send(ev);
    }
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BrokerEvent> {
        self.tx.subscribe()
    }
}

/// Nodes are agents, edges are messages. Enough for the live graph UI and for
/// activity-based watchdog detectors without either reaching into adapters.
#[derive(Clone, serde::Serialize)]
pub enum BrokerEvent {
    Snapshot { agents: Vec<AgentNode>, pending_permissions: Vec<PermissionId> },
    AgentChanged(AgentNode),
    MessageAccepted { id: MessageId, from: Sender, to: AgentId, conversation: String, hop: u16 },
    MessageOffered { id: MessageId, incarnation: Incarnation },
    MessageConsumed { id: MessageId },
    /// Interceptor hold, or an `Uncertain` CLI turn awaiting the human.
    MessageHeld { id: MessageId, reason: String },
    PermissionPending { id: PermissionId, agent: AgentId, incarnation: Incarnation, title: String },
    /// Answered, or withdrawn because its session ended.
    PermissionAnswered { id: PermissionId },
    /// Normalized `adapter::Activity`, rate-limited per agent. Display-only: for the UI
    /// and activity-based watchdog detectors; never delivered to any agent.
    Activity { agent: AgentId, summary: String },
}

#[derive(Clone, serde::Serialize)]
pub struct AgentNode {
    pub id: AgentId,
    pub name: AgentName,
    pub kind: KindName,
    /// `supervisor::Phase` rendered:
    /// dormant|starting|idle|busy|awaiting-permission|suspending|crashed|failed|needs-recovery|retired
    pub phase: &'static str,
    pub queued: u32,
    pub held: u32,
}
