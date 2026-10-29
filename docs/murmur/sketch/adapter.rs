//! The adapter contract: the one abstraction over every agent wire.
//!
//! An adapter turns "a kind of agent" into live `Session`s that speak domain types.
//! It owns everything wire-specific: process spawn and flags, handshake, resume (and the
//! private shape of its `ResumeToken`), translating turns in and activity out, mapping
//! permission prompts, and deriving delivery receipts from its wire. It owns no
//! policy: when to deliver, when to suspend, how to answer a permission prompt and
//! when to restart are decided by the actor (`supervisor.rs`), identically for every kind.
//!
//! The contract is modelled on ACP's session semantics (new/load, prompt turn, cancel,
//! permission request, stop reason), because ACP is already the closest thing to a
//! common denominator: Prime Agent speaks it natively, Claude/Codex/Pi have ACP shims,
//! and Infinity's own ACP server proves the mapping from Infinity's events. But no ACP
//! type crosses this boundary: the ACP adapter is one implementation, not the contract.
//!
//! Two rules every adapter keeps:
//! - A failed resume is an error (`ResumeFailed` / `ResumeUnsupported`), never a
//!   silently fresh session. The actor turns it into `Phase::NeedsRecovery`.
//! - Live output (`SessionEvent::Activity`) is display-only. The actor publishes it on
//!   the event bus and nothing else; it never becomes input to any agent.

use std::sync::Arc;

use async_trait::async_trait;

use crate::model::{
    AgentId, AgentSpec, Envelope, Incarnation, KindName, MessageId, PermissionId,
    PermissionRequest, ResumeToken,
};

// ---------------------------------------------------------------- the one list

/// Every wire the broker can drive. This enum is the single list of adapter
/// implementations: `AdapterSet::build` matches on it, so adding a variant without an
/// adapter fails to compile.
///
/// Adding a sixth agent:
/// - it speaks ACP natively or has an ACP shim -> a `kinds.toml` entry, no code;
/// - it speaks something else -> one new variant, one new `adapters/<wire>.rs`.
///
/// Native fallbacks for agents whose ACP shim proves lossy (Claude `stream-json`, Codex
/// `app-server`, Pi `--mode rpc`) slot in the same way, per kind, without touching the
/// broker core. They are not built in v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Wire {
    /// Infinity daemon protocol over `~/.infinity/daemon.sock` (plus the peer-message
    /// patch). Model agents.
    Infinity,
    /// Agent Client Protocol over the child's stdio. Prime (native), Claude
    /// (`claude-agent-acp`), Codex (`codex-acp`), Pi (`pi-acp`): versions pinned as in the
    /// ACP registry cache (`~/.t3/caches/acp-registry/registry.json`).
    Acp,
    /// Muse Session Protocol over `muse serve` stdio.
    Msp,
}

/// One `kinds.toml` entry. Built-in defaults:
///
/// ```toml
/// [kind.infinity]  wire = "infinity"                                  # idle handled by Infinity
/// [kind.prime]     wire = "acp"  command = ["prime-agent", "--mode", "acp"]           idle_after = "10m"
/// [kind.claude]    wire = "acp"  command = ["npx", "-y", "@agentclientprotocol/claude-agent-acp@0.90.0"] idle_after = "10m"
/// [kind.codex]     wire = "acp"  command = ["npx", "-y", "@agentclientprotocol/codex-acp@2.2.2"] idle_after = "10m"
/// [kind.pi]        wire = "acp"  command = ["npx", "-y", "pi-acp@0.0.34"] idle_after = "10m"
/// [kind.muse]      wire = "msp"  command = ["muse", "serve"]                                 idle_after = "10m"
/// ```
#[derive(Clone, Debug, serde::Deserialize)]
pub struct KindConfig {
    pub name: KindName,
    pub wire: Wire,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// Suspend an idle session after this long. `None`: never suspend (Infinity, whose
    /// threads already hibernate themselves at zero cost).
    #[serde(default, with = "humantime_serde")]
    pub idle_after: Option<std::time::Duration>,
    /// Extra argv derived from `SandboxLevel`, per level. Lets one wire serve kinds with
    /// different permission flags without code.
    #[serde(default)]
    pub sandbox_args: SandboxArgs,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct SandboxArgs {
    pub read_only: Vec<String>,
    pub workspace_write: Vec<String>,
    pub full_access: Vec<String>,
}

/// Built once at startup from `Wire` + `kinds.toml`.
pub struct AdapterSet {
    by_kind: std::collections::HashMap<KindName, Arc<dyn Adapter>>,
}

impl AdapterSet {
    pub fn build(kinds: &[KindConfig], deps: AdapterDeps) -> Result<Self, AdapterError> {
        // for k in kinds:
        //   let a: Arc<dyn Adapter> = match k.wire {
        //       Wire::Infinity => deps.infinity.clone(),       // one shared daemon link
        //       Wire::Acp => Arc::new(AcpAdapter::new(k.clone(), deps.mcp.clone())),
        //       Wire::Msp => Arc::new(MspAdapter::new(k.clone(), deps.mcp.clone())),
        //   };
        todo!()
    }
    pub fn get(&self, kind: &KindName) -> Option<Arc<dyn Adapter>> {
        todo!()
    }
}

pub struct AdapterDeps {
    pub infinity: Arc<crate::adapters::infinity::InfinityAdapter>,
    /// Mints per-agent MCP endpoints so CLI agents can call `send_message`.
    pub mcp: Arc<crate::surfaces::McpEndpoints>,
}

// ---------------------------------------------------------------- the contract

/// When the actor may hand this wire a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryGate {
    /// The runtime itself guarantees peer events never interrupt (patched Infinity defers
    /// `PeerMessage` synthetics while a non-passive tool call is pending, and settles a
    /// tool result before any peer in the same batch). Submit immediately.
    Anytime,
    /// The agent only accepts input as a new turn. Submit only while Idle; batch
    /// everything that queued up during the turn into the next one.
    AtTurnBoundary,
}

#[async_trait]
pub trait Adapter: Send + Sync {
    fn delivery_gate(&self) -> DeliveryGate;

    /// Start or resume the agent's live session.
    ///
    /// - `resume = None`: create a fresh session (Infinity `CreateSession`; ACP
    ///   `session/new`).
    /// - `resume = Some(t)`: reattach (Infinity `Connect` to the root thread; ACP
    ///   `session/load`). If the token does not decode, the agent cannot load sessions
    ///   (`ResumeUnsupported`), or the load fails (`ResumeFailed`), return the error.
    ///   Never fall back to a fresh session: the actor moves to `NeedsRecovery`.
    ///
    /// Must be idempotent for a given `resume`: opening twice after a crash must not
    /// fork the conversation.
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError>;
}

pub struct OpenRequest<'a> {
    pub agent: AgentId,
    pub spec: &'a AgentSpec,
    pub resume: Option<&'a ResumeToken>,
    /// Stamped on every `PermissionRequest` this session raises.
    pub incarnation: Incarnation,
}

/// One live session. Owned by exactly one actor; never shared.
#[async_trait]
pub trait Session: Send {
    /// Persist this so the agent can be resumed. May change during the session
    /// (reported through `SessionEvent::ResumeToken`). Built with `ResumeToken::encode`
    /// from the adapter's private struct.
    fn resume_token(&self) -> ResumeToken;

    /// Whether `suspend` + a later `open(Some(token))` can restore this session (ACP:
    /// the agent advertised `loadSession`). The actor never suspends a session that
    /// answers `false`, so an idle timeout cannot strand it in `NeedsRecovery`.
    fn resumable(&self) -> bool;

    /// Hand the agent one turn. Precondition (enforced by the actor, not re-checked
    /// here): if `delivery_gate() == AtTurnBoundary`, the session is Idle.
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError>;

    /// Stop the in-flight turn without injecting input (ACP `session/cancel`, Infinity
    /// `ClientMessage::Interrupt`). No-op when Idle.
    async fn cancel(&mut self) -> Result<(), AdapterError>;

    /// Answer a permission request this session raised, with one of its option keys.
    async fn answer_permission(&mut self, id: PermissionId, key: &str) -> Result<(), AdapterError>;

    /// Next event. After `SessionEvent::Ended` it returns `None` forever.
    async fn next_event(&mut self) -> Option<SessionEvent>;

    /// Release the live session (stop the process) but keep it resumable.
    async fn suspend(self: Box<Self>) -> Result<ResumeToken, AdapterError>;
}

/// What the actor hands a session. Items keep their arrival order.
pub struct Turn {
    pub items: Vec<TurnItem>,
}

pub enum TurnItem {
    /// From the human: becomes real user input (Infinity `UserInput`, ACP prompt text).
    User(Envelope),
    /// From an agent: Infinity `DeliverPeerMessage` (never user text); for CLI agents
    /// the rendered envelope inside a prompt turn, at a turn boundary.
    Peer { envelope: Envelope, rendered: String },
}

/// Normalized output of every wire.
pub enum SessionEvent {
    /// First evidence that a turn carrying `ids` started: ACP the first
    /// `session/update` or the prompt response; Infinity a `UserInputEcho` for a human
    /// item. The actor records it durably (`Mailroom::mark_started`): if the session now
    /// dies before the turn ends, these deliveries become `Uncertain` instead of being
    /// re-offered. Infinity peer messages never appear here (their receive is
    /// idempotent, so re-offer is always safe).
    TurnStarted { ids: Vec<MessageId> },
    /// Delivery receipt: these messages are durably part of the recipient's own
    /// history, and the only event that makes the mailroom mark them `Consumed`.
    /// Infinity: `DaemonMessage::InputCommitted` (core emits it after `sync()` persisted
    /// the input); dedup id `peer-{MessageId}` for peers, see `adapters/infinity.rs` for
    /// human items. CLI wires: the turn that carried them ended. Neither an echo nor
    /// `ResponseDone` is a receipt.
    Committed { ids: Vec<MessageId> },
    /// Display-only: the actor forwards it to the event bus (UI, watchdog) and never
    /// into any agent's input.
    Activity(Activity),
    /// Carries the session's `Incarnation` (from `OpenRequest`).
    Permission(PermissionRequest),
    TurnEnded(TurnOutcome),
    ResumeToken(ResumeToken),
    /// The session is gone: process exit, daemon disconnect. Always the last event.
    Ended(EndReason),
}

pub enum Activity {
    Text(String),
    Thinking(String),
    ToolCall { name: String, summary: String },
    ToolResult { name: String, ok: bool },
    Usage { input_tokens: u64, output_tokens: u64 },
}

pub enum TurnOutcome {
    Completed,
    Cancelled,
    Refused,
    MaxTokens,
    Failed(String),
}

pub enum EndReason {
    /// We asked for it (suspend, stop).
    Requested,
    Exited { code: Option<i32>, stderr_tail: String },
    /// The shared Infinity daemon went away; every Infinity session ends with this.
    DaemonGone,
}

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// A resume token exists but the agent cannot load sessions. -> `NeedsRecovery`.
    #[error("agent does not support resume")]
    ResumeUnsupported,
    /// The token did not decode, or the agent/daemon refused to load it. -> `NeedsRecovery`.
    #[error("resume failed: {0}")]
    ResumeFailed(String),
    #[error("spawn failed: {0}")]
    Spawn(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("session ended")]
    Ended,
}
