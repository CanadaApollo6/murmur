//! Domain types. No wire or transport types appear here (per boundary-discipline):
//! adapters parse their protocols into these.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------- identity

/// The address of an agent. A ULID minted by the broker at spawn, printed `agent:01J...`.
///
/// Invariant: an `AgentId` never changes for the life of the agent. It is not derived
/// from anything that changes: not a PID, not an Infinity `ThreadId`, not an ACP
/// `SessionId`, not a hostname. Those live in the agent's `ResumeToken` and `Placement`,
/// which the broker may rewrite (after a restart, a re-created session, a migration)
/// without any sender noticing. This is what makes addresses stable across hibernation,
/// process restarts and, later, machines.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub ulid::Ulid);

/// Human-friendly alias, `@reviewer`. Unique among non-retired agents on this broker.
/// Resolved to an `AgentId` at the API boundary (`Registry::resolve`); nothing below
/// the facade sees a name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentName(String);

impl AgentName {
    /// Validates `[a-z0-9][a-z0-9-]{0,31}`; strips a leading `@`.
    pub fn parse(s: &str) -> Result<Self, InvalidName> {
        todo!()
    }
}
#[derive(Debug)]
pub struct InvalidName;

/// What a sender writes in `to`. Either form; the facade resolves it once.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Recipient {
    Id(AgentId),
    Name(AgentName),
}

impl Recipient {
    /// `agent:<ulid>` or `@name` / `name`.
    pub fn parse(s: &str) -> Result<Self, InvalidName> {
        todo!()
    }
}

/// Which broker node hosts the agent's session. v1 always `Local`. A future federation
/// layer routes `Remote(node)` over Tailscale; `AgentId` stays the same when it moves.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Placement {
    Local,
    Remote(NodeId),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

/// Kind name from `kinds.toml`: "infinity", "claude", "codex", "muse", "pi", "prime", ...
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KindName(pub String);

/// Opaque, adapter-owned resume handle. The broker persists it and hands it back; only
/// the adapter that produced it can read it, so no wire detail (Infinity `ThreadId`,
/// ACP `SessionId`, Muse ref) leaks into the registry or any surface. There is no public
/// field and no public constructor: each adapter encodes and decodes its own private
/// struct (`InfinityResume`, `AcpResume`, `MspResume`) through the two `pub(crate)`
/// helpers below, which only `adapters/*` call.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResumeToken(String);

impl ResumeToken {
    /// Adapter-only. Serializes the adapter's private resume struct.
    pub(crate) fn encode<T: Serialize>(inner: &T) -> Self {
        todo!()
    }

    /// Adapter-only. `None` when the blob is not this adapter's shape; the adapter
    /// turns that into `AdapterError::ResumeFailed` (never a fresh session).
    pub(crate) fn decode<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        todo!()
    }
}

/// Bumped by the agent's actor every time a new live session begins (process spawn,
/// or Infinity daemon restart). Deliveries record the incarnation they were offered to,
/// which is how the broker knows what to re-offer after a crash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Incarnation(pub u64);

// ---------------------------------------------------------------- spec

/// Everything needed to (re)create an agent's session. Persisted verbatim.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentSpec {
    pub name: AgentName,
    pub kind: KindName,
    pub cwd: std::path::PathBuf,
    /// Infinity: a `ModelRef` string `provider/model` (e.g. `local/qwen3-coder`).
    /// CLI agents: passed to the kind's model flag / ACP `session/set_model`, if any.
    pub model: Option<String>,
    /// First instruction, delivered as a message from `Sender::Human` when the agent is
    /// created. Not special-cased anywhere else.
    pub task: Option<Body>,
    pub permissions: PermissionPolicy,
    /// Override of the kind's default idle time before a CLI session is suspended.
    pub idle_after: Option<std::time::Duration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentRecord {
    pub id: AgentId,
    pub spec: AgentSpec,
    pub placement: Placement,
    pub resume: Option<ResumeToken>,
    pub incarnation: Incarnation,
    pub retired: bool,
}

// ---------------------------------------------------------------- messages

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub ulid::Ulid);

/// Groups an exchange. A fresh send starts a conversation; a reply inherits it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConversationId(pub ulid::Ulid);

/// Sender-chosen idempotency key. Infinity agents: the RAP `RapInvocation::id` (tool
/// call id) of the `send_message` call (`ClientKey::from_tool_call`, in `surfaces`).
/// CLI agents: the MCP request id scoped by session. Humans: a random key per
/// `agentctl send`. `(Sender, ClientKey)` is unique in the mailbox.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientKey(pub String);

/// Who sent a message, as stored. Surfaces never construct one for `Broker::send`:
/// they hold a `surfaces::AuthenticatedSender`, which only the bindings module mints.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Sender {
    /// The user, through agentctl or the UI.
    Human,
    Agent(AgentId),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Body(String);

impl Body {
    /// Non-empty, at most 64 KiB.
    pub fn new(s: impl Into<String>) -> Result<Self, InvalidBody> {
        todo!()
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug)]
pub struct InvalidBody;

/// A committed message. Created only by `Mailroom::accept`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub id: MessageId,
    pub from: Sender,
    pub to: AgentId,
    pub conversation: ConversationId,
    pub in_reply_to: Option<MessageId>,
    /// Number of agent-to-agent hops since the last human message in this conversation.
    /// Derived at accept time from `in_reply_to`'s hop; never supplied by the sender.
    pub hop: u16,
    /// What the message does (`swarm::Act`). Sender-chosen, validated at accept time:
    /// `Report` needs task ownership, `Decide` needs the decision right.
    pub act: crate::swarm::Act,
    /// The task this message belongs to, if any. Budgets are charged to it.
    pub task: Option<crate::swarm::TaskId>,
    /// The sender's context position at send time; lineage derives from it.
    /// Set by `accept`, never supplied by the sender.
    pub exposure: crate::swarm::ExposureRef,
    pub body: Body,
    pub sent_at: time::OffsetDateTime,
}

/// How a message enters the recipient. Derived, never stored: a human's message is a
/// user turn (it may interrupt, the way typing into a TUI does); an agent's message is
/// a peer event (it must never interrupt). This is the "no fake user text" rule as a
/// function of the sender, so no call site can choose wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryClass {
    UserTurn,
    PeerEvent,
}

impl Envelope {
    pub fn class(&self) -> DeliveryClass {
        match self.from {
            Sender::Human => DeliveryClass::UserTurn,
            Sender::Agent(_) => DeliveryClass::PeerEvent,
        }
    }

    /// The text a recipient sees for a peer event. Defined once, here. Shape:
    ///
    /// ```text
    /// <agent-message id="01J.." from="@planner" kind="infinity" conversation="01J.." in-reply-to="01J..">
    /// ...body...
    /// </agent-message>
    /// This is a message from another agent, not from your user. Reply with send_message.
    /// ```
    ///
    /// The broker never parses this back: Infinity receipts carry the dedup id
    /// `peer-{MessageId}` (`DaemonMessage::InputCommitted`), and CLI receipts are tied
    /// to the turn the actor submitted.
    pub fn render_peer(&self, sender_name: &AgentName, sender_kind: &KindName) -> String {
        todo!()
    }
}

// ---------------------------------------------------------------- delivery

/// Per-message delivery state, one row in `deliveries`. Owner: `Mailroom`.
///
/// - `Offered{incarnation}`: written ahead of `Session::submit`. Re-offered when that
///   incarnation ends before a receipt, unless the turn had started (CLI rule below).
/// - `Consumed`: only on `SessionEvent::Committed` (terminal).
/// - `Uncertain{incarnation}`: a CLI turn carrying this message started in
///   `incarnation` and its session died before the turn ended. It may have had side
///   effects, so it is held for the human (`agentctl held | release | drop`).
/// - `Held{reason}`: an interceptor held it at accept time.
/// - `Dead{reason}`: recipient retired, or the human dropped it (terminal).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryState {
    Queued,
    Offered { incarnation: Incarnation },
    Consumed,
    Uncertain { incarnation: Incarnation },
    Held { reason: String },
    Dead { reason: String },
}

/// One row of `agentctl held`: an envelope in `Held` or `Uncertain`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeldMail {
    pub envelope: Envelope,
    pub state: DeliveryState,
}

// ---------------------------------------------------------------- permissions

/// Per-agent policy, applied by the actor to every `PermissionRequest` its session
/// raises. Kind-level launch flags (Codex `-s workspace-write`, Muse `--approval-mode`,
/// Claude `--permission-mode`) are derived from `sandbox` by the adapter, so most
/// prompts never reach the broker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionPolicy {
    pub sandbox: SandboxLevel,
    pub rules: Vec<PermissionRule>,
    /// Where an unmatched request goes. Default `Human`.
    pub escalate_to: Escalation,
    /// Auto-reject an unanswered request after this long (None = wait forever).
    pub timeout: Option<std::time::Duration>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SandboxLevel {
    ReadOnly,
    WorkspaceWrite,
    FullAccess,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionRule {
    /// Glob over the normalized tool name (`shell`, `edit`, `fetch`, `mcp:agentd/*`, ...).
    pub tool: String,
    pub decision: RuleDecision,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum RuleDecision {
    Allow,
    Deny,
    Ask,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Escalation {
    Human,
    /// A supervising agent receives the request as a peer message and answers with the
    /// `answer_permission` tool. Never the requesting agent itself (checked at spawn).
    Agent(AgentId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PermissionId(pub ulid::Ulid);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub id: PermissionId,
    pub agent: AgentId,
    /// The session incarnation that raised it. The actor rejects an answer when this is
    /// no longer the live incarnation (the session that asked is gone), so a late
    /// "allow" can never reach a restarted session.
    pub incarnation: Incarnation,
    /// Normalized tool name, matched against `PermissionRule::tool`.
    pub tool: String,
    pub title: String,
    /// Command line, diff, URL: whatever the agent showed.
    pub detail: String,
    /// The options the agent offered, in its order. Answers must pick one of these.
    pub options: Vec<PermissionOption>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionOption {
    /// Adapter-private option id (ACP `PermissionOptionId`, Infinity choice index, ...).
    pub key: String,
    pub label: String,
    pub effect: PermissionEffect,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermissionEffect {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PermissionAnswer {
    /// Must be one of the request's option keys; validated by the actor.
    Choose(String),
    /// Cancel the turn instead of answering.
    CancelTurn,
}
