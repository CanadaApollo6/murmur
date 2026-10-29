//! agentd: a local broker that hosts Infinity model agents and CLI agents as peers.
//!
//! Lives in its own workspace (`~/src/agentd`), NOT inside the Infinity fork. It depends
//! on the fork only through wire crates: `infinity-protocol` (daemon socket protocol),
//! `rap-protocol` (toolset wire), and, for the provider binary, `infinity-provider-protocol`.
//! It never links `infinity-agent-core` or `infinity-daemon`.
//!
//! Module map (calls flow top to bottom; no module reaches upward):
//!
//! ```text
//!   surfaces   control API (agentctl), event bus (graph UI), RAP toolset (Infinity
//!              agents' send_message), MCP endpoint (CLI agents' send_message).
//!              `surfaces::bindings` is the only place an `AuthenticatedSender` is
//!              minted (RAP bound thread, MCP session token, 0600 control socket).
//!      |  all four call Broker::{spawn, send, cancel, stop, answer_permission};
//!      |  the control API also calls the crate-private held-mail operator methods
//!      v
//!   Broker     facade; owns Registry + Mailroom + one AgentActor per agent
//!      |
//!   mailroom   durable mailbox (SQLite). Single owner of messages + delivery state
//!              (Queued | Offered | Consumed | Uncertain | Held | Dead).
//!              Interceptor chain (watchdog attaches here).
//!   supervisor AgentActor: lifecycle state machine + the delivery gate. Single owner
//!              of an agent's Phase, live Session and Incarnation.
//!      |
//!   adapter    Adapter / Session traits (domain types only, no wire types)
//!      |
//!   adapters/  infinity (daemon protocol + peer-message patch), acp (Prime, Claude,
//!              Codex, Pi via ACP), msp (Muse). One `Wire` enum lists them.
//!   services   ServiceSupervisor: `infinity daemon`, llama-server / vLLM. Not agents.
//!
//!   provider_local   separate binary `infinity-provider-local` (OpenAI-compatible
//!                    ModelProvider + GPU gate), spawned by the Infinity daemon.
//!   infinity_patch   the Infinity fork diff: two additive commits (peer messages,
//!                    provider cancellation).
//! ```

// Only the facade, the domain model and the event types are public. Adapters,
// sessions, the mailroom and actors are unreachable from outside the crate, so a
// surface (or a future plugin) cannot bypass the actor and call `Session::submit`.
mod adapter;
mod adapters;
mod mailroom;
pub mod model;
mod services;
mod supervisor;
pub mod surfaces;
pub mod swarm;

pub use adapter::{KindConfig, Wire};
pub use mailroom::SendOutcome;
pub use services::ServiceSpec;
pub use surfaces::AuthenticatedSender;

// `provider_local` and `infinity_patch` are separate compilation units (a binary crate
// and a diff against the fork); they live in this sketch only for review.

use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::AdapterSet;
use crate::mailroom::{Mailroom, SendOutcome};
use crate::model::{
    AgentId, AgentRecord, AgentSpec, Body, ClientKey, HeldMail, MessageId, PermissionAnswer,
    PermissionId, Recipient,
};
use crate::supervisor::ActorHandle;

/// The one public entry point. Every surface (CLI, UI, RAP, MCP) calls these five
/// operations and nothing else. Callers never see sessions, wires, incarnations,
/// receipts or delivery state: that is the complexity this facade hides. (The control
/// API additionally reaches three `pub(crate)` held-mail methods; they are operator
/// commands, not part of the facade.)
pub struct Broker {
    registry: Arc<Registry>,
    mailroom: Arc<Mailroom>,
    adapters: Arc<AdapterSet>,
    /// One actor per non-retired agent. Created at startup from the registry and on spawn.
    actors: tokio::sync::RwLock<HashMap<AgentId, ActorHandle>>,
    events: surfaces::EventBus,
}

impl Broker {
    /// Open the state dir (`~/.local/state/agentd/`), run migrations, start services in
    /// dependency order (inference -> infinity daemon), then start one actor per agent.
    /// Crash recovery happens here and only here: `Mailroom::session_ended` runs for every
    /// agent's last incarnation, so `Offered` mail without turn-start evidence goes back
    /// to `Queued` and mail whose CLI turn had started becomes `Uncertain`.
    pub async fn open(cfg: BrokerConfig) -> Result<Arc<Self>, BrokerError> {
        todo!()
    }

    /// Create an agent, or return the existing one with the same name (idempotent by
    /// name, so a retried `agentctl spawn` does not create twins). Does not start any
    /// process: the agent starts Dormant and wakes on its first message.
    pub async fn spawn(&self, spec: AgentSpec) -> Result<AgentRecord, BrokerError> {
        todo!()
    }

    /// Accept a message for durable delivery. Returns once the message is committed to
    /// the mailbox, never after delivery. Idempotent on `(sender, client_key)`.
    ///
    /// `from` is an `AuthenticatedSender`: a surface cannot name a sender, only present
    /// the binding it was minted from.
    pub async fn send(
        &self,
        from: AuthenticatedSender,
        to: Recipient,
        body: Body,
        reply_to: Option<model::MessageId>,
        client_key: ClientKey,
    ) -> Result<SendOutcome, BrokerError> {
        todo!()
    }

    /// Stop the agent's current turn: ACP `session/cancel`, the native equivalent, or
    /// `ClientMessage::Interrupt` for Infinity. Never user text. Queued mail stays queued.
    pub async fn cancel(&self, agent: &AgentId) -> Result<(), BrokerError> {
        todo!()
    }

    /// Retire the agent: stop its session for good, dead-letter its queued mail.
    pub async fn stop(&self, agent: &AgentId) -> Result<(), BrokerError> {
        todo!()
    }

    /// Answer a pending permission request. `by` must be the human or the agent the
    /// policy escalated to. Rejected with `StalePermission` when the request's
    /// incarnation is no longer the agent's live session.
    pub async fn answer_permission(
        &self,
        by: AuthenticatedSender,
        id: &PermissionId,
        answer: PermissionAnswer,
    ) -> Result<(), BrokerError> {
        todo!()
    }

    // ------------------------------------------------ operator (control API only)

    /// `agentctl held [@name]`: `Held` and `Uncertain` mail.
    pub(crate) async fn held(&self, agent: Option<&Recipient>) -> Result<Vec<HeldMail>, BrokerError> {
        todo!()
    }

    /// `agentctl release <id>`: back to `Queued`, then nudge the recipient's actor.
    pub(crate) async fn release(&self, id: MessageId) -> Result<(), BrokerError> {
        todo!()
    }

    /// `agentctl drop <id>`: `Dead{"dropped by human"}`.
    pub(crate) async fn drop_held(&self, id: MessageId) -> Result<(), BrokerError> {
        todo!()
    }
}

/// Agent records and name aliases. Single owner of the `agents` table. Phase is NOT
/// stored here (the actor owns it); only what must survive a broker restart:
/// spec, opaque resume token, incarnation counter, placement, retired flag.
pub struct Registry {/* rusqlite handle shared with Mailroom's db file, separate tables */}

impl Registry {
    pub fn resolve(&self, r: &Recipient) -> Result<AgentId, BrokerError> {
        todo!()
    }
    // The Infinity root -> agent lookup that `surfaces::bindings` needs lives in
    // `InfinityAdapter::agent_for_root`, because only that adapter can read its tokens.
    pub(crate) fn save_resume(&self, id: AgentId, token: &model::ResumeToken) -> Result<(), BrokerError> {
        todo!()
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct BrokerConfig {
    pub state_dir: std::path::PathBuf,
    /// `kinds.toml`: one entry per agent kind. Built-in defaults for infinity, claude,
    /// codex, muse, pi, prime ship in the binary; the user file overrides by kind name.
    pub kinds: Vec<adapter::KindConfig>,
    pub services: Vec<services::ServiceSpec>,
    pub listen: surfaces::ListenConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("no agent named {0}")]
    UnknownAgent(String),
    #[error("agent {0} is retired")]
    Retired(AgentId),
    #[error("held by interceptor: {0}")]
    Held(String),
    #[error("permission {0:?} belongs to an ended session")]
    StalePermission(PermissionId),
    #[error("not allowed: {0}")]
    Forbidden(String),
    #[error(transparent)]
    Store(#[from] rusqlite::Error),
    #[error(transparent)]
    Adapter(#[from] adapter::AdapterError),
}
