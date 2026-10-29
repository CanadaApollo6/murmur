//! The durable mailbox. Single owner of every message and every delivery state.
//!
//! Storage: SQLite (WAL) at `~/.local/state/agentd/broker.db`, one writer (this module).
//!
//! ```sql
//! messages(id PK, sender, client_key, recipient, conversation, in_reply_to, hop, body,
//!          sent_at, UNIQUE(sender, client_key))
//! deliveries(message_id PK -> messages, state, incarnation, turn_started, attempts, reason)
//!   -- state: queued | offered | consumed | uncertain | held | dead   (model::DeliveryState)
//!   -- turn_started: set on an Offered row when the session showed turn-start evidence
//! ```
//!
//! Delivery state machine (per message; one recipient per message):
//!
//! ```text
//!   accept --> Queued --mark_offered(inc)--> Offered{inc} --Committed--> Consumed
//!                ^                              |
//!                |      session inc ended,      | session inc ended,
//!                +----- no turn-start evidence -+ turn started, not ended
//!                |                              v
//!                +------------ release ---- Uncertain{inc} --drop--> Dead
//!   accept --interceptor Hold--> Held{reason} --release--> Queued
//!                                     +--------drop------> Dead
//!   Queued|Offered|Held|Uncertain --recipient retired--> Dead
//! ```
//!
//! `Consumed` is written only for `SessionEvent::Committed`: on Infinity, the adapter
//! derives it from `DaemonMessage::InputCommitted` (core emits it after `sync()` has
//! persisted the input); on CLI wires, when the turn that carried the message ends.
//!
//! Guarantees: at-least-once with idempotent receive on Infinity (dedup id
//! `peer-{MessageId}`, persisted with the thread's processed ids, so a re-offer is
//! absorbed and still answered with `InputCommitted`); at-least-once on CLI agents, with
//! a human decision (`Uncertain`) whenever a turn may have run and its outcome is lost.
//!
//! Infinity's own queues (router channel, `InMemoryDeferQueue`) are caches: anything
//! they lose on a daemon crash is still `Offered` here and gets re-offered.
//!
//! Held mail is an operator concern, not a facade method: the control API (`agentctl
//! held | release | drop`) calls `held`, `release` and `drop_held` through the
//! crate-private `Broker` operator methods.

use crate::model::{
    AgentId, Body, ClientKey, ConversationId, Envelope, HeldMail, Incarnation, MessageId, Sender,
};

pub struct Mailroom {
    db: tokio::sync::Mutex<rusqlite::Connection>,
    interceptors: Vec<Box<dyn Interceptor>>,
    events: crate::surfaces::EventBus,
}

/// Crate-internal. `Broker::send` builds this from an `AuthenticatedSender`; the actor
/// builds it directly for the spawn task (`Sender::Human`) and for permission
/// escalations (`Sender::Agent(self)`). No surface reaches it.
pub struct NewMessage {
    pub from: Sender,
    pub to: AgentId,
    pub body: Body,
    pub reply_to: Option<MessageId>,
    pub client_key: ClientKey,
    /// `send_message`'s `act` argument; `Inform` with no claims when omitted.
    pub act: crate::swarm::Act,
}

pub enum SendOutcome {
    /// Committed and queued. `recipient_busy` lets the sender's tool result say
    /// "it will see this when its current turn ends".
    Queued { id: MessageId, recipient_busy: bool },
    /// Committed but held by an interceptor; the human must release it.
    Held { id: MessageId, reason: String },
    /// Same `(from, client_key)` as an earlier send: the earlier id, nothing new written.
    Duplicate { id: MessageId },
}

/// Result of `session_ended`: how many deliveries go back to `Queued`, and which
/// became `Uncertain` (already published as `MessageHeld` by the mailroom).
pub struct SessionEndOutcome {
    pub requeued: u64,
    pub uncertain: Vec<MessageId>,
}

impl Mailroom {
    /// Commit a message. Pure decisions first, then one transaction:
    ///
    /// ```text
    /// if let Some(id) = select id where (sender, client_key) = ..  -> Duplicate{id}
    /// let parent = reply_to.map(load)            // must exist and involve `from`
    /// let conversation = parent.map(|p| p.conversation).unwrap_or(new)
    /// let hop = match from { Human => 0, Agent(_) => parent.map(|p| p.hop + 1).unwrap_or(1) }
    /// let env = Envelope { .. }
    /// let verdict = fold interceptors over (env, self.view(conversation))
    /// insert message + delivery(state = Queued | Held)
    /// emit BrokerEvent::MessageAccepted / MessageHeld
    /// ```
    ///
    /// The caller (Broker) nudges the recipient's actor after this returns.
    pub async fn accept(&self, msg: NewMessage) -> Result<(SendOutcome, Envelope), rusqlite::Error> {
        todo!()
    }

    /// Everything deliverable to `agent`, oldest first.
    pub async fn queued_for(&self, agent: AgentId) -> Result<Vec<Envelope>, rusqlite::Error> {
        todo!()
    }

    /// Write-ahead: called by the actor BEFORE `Session::submit`. `Queued -> Offered{inc}`.
    /// Idempotent. Emits `MessageOffered`.
    pub async fn mark_offered(&self, ids: &[MessageId], inc: Incarnation) -> Result<(), rusqlite::Error> {
        todo!()
    }

    /// Turn-start evidence (`SessionEvent::TurnStarted{ids}`): set `turn_started` on these
    /// `Offered{inc}` rows. Durable, so a broker crash mid-turn still ends in `Uncertain`
    /// rather than a blind re-offer. Never called for Infinity peer messages.
    pub async fn mark_started(&self, ids: &[MessageId], inc: Incarnation) -> Result<(), rusqlite::Error> {
        todo!()
    }

    /// The receipt. Called only for `SessionEvent::Committed`. Idempotent; unknown,
    /// already-consumed or dead ids are ignored. Emits `MessageConsumed`.
    pub async fn mark_consumed(&self, ids: &[MessageId]) -> Result<(), rusqlite::Error> {
        todo!()
    }

    /// A session of `agent` with incarnation `ended` is gone. In one transaction:
    ///
    /// ```text
    /// Offered{inc <= ended} and not turn_started -> Queued            (re-offer)
    /// Offered{inc <= ended} and turn_started     -> Uncertain{inc}    (held for the human)
    /// ```
    ///
    /// Emits `MessageHeld` for each new `Uncertain`. Idempotent; also run for every agent
    /// at broker startup with its last persisted incarnation.
    pub async fn session_ended(&self, agent: AgentId, ended: Incarnation) -> Result<SessionEndOutcome, rusqlite::Error> {
        todo!()
    }

    /// `Held` and `Uncertain` deliveries, oldest first; all agents when `agent` is None.
    pub async fn held(&self, agent: Option<AgentId>) -> Result<Vec<HeldMail>, rusqlite::Error> {
        todo!()
    }

    /// Human decision: `Held | Uncertain -> Queued`. Returns the recipient so the caller
    /// can nudge its actor. Any other state is `Ok(None)`.
    pub async fn release(&self, id: MessageId) -> Result<Option<AgentId>, rusqlite::Error> {
        todo!()
    }

    /// Human decision: `Held | Uncertain -> Dead{reason}`.
    pub async fn drop_held(&self, id: MessageId, reason: &str) -> Result<(), rusqlite::Error> {
        todo!()
    }

    pub async fn dead_letter_all(&self, agent: AgentId, reason: &str) -> Result<(), rusqlite::Error> {
        todo!()
    }

    /// Read model for interceptors and the UI: the last N messages of a conversation,
    /// per-pair exchange counts, hop of the newest message.
    pub async fn view(&self, conversation: ConversationId) -> Result<ConversationView, rusqlite::Error> {
        todo!()
    }
}

// ---------------------------------------------------------------- watchdog seam

/// Every message passes `accept`, so this is where loop and back-and-forth detection
/// attaches. Interceptors are pure: they see the candidate envelope and a read-only
/// view, and return a verdict. They never write state (the mailroom applies the verdict).
///
/// v1 ships one: `HopLimit { max: 24 }` (Hold when `env.hop > max`). The future
/// watchdog adds e.g. `PingPong` (same pair, same conversation, > N exchanges with low
/// novelty) and `Bureaucracy` (many agents, no tool activity, only messages) using the
/// activity stream from `surfaces::EventBus` as extra input.
pub trait Interceptor: Send + Sync {
    fn name(&self) -> &'static str;
    fn inspect(&self, env: &Envelope, view: &ConversationView) -> Verdict;
}

pub enum Verdict {
    Deliver,
    Hold { reason: String },
    Reject { reason: String },
}

pub struct ConversationView {
    pub conversation: ConversationId,
    pub recent: Vec<Envelope>,
    pub pair_exchanges: std::collections::HashMap<(AgentId, AgentId), u32>,
}

pub struct HopLimit {
    pub max: u16,
}

impl Interceptor for HopLimit {
    fn name(&self) -> &'static str {
        "hop-limit"
    }
    fn inspect(&self, env: &Envelope, _view: &ConversationView) -> Verdict {
        if env.hop > self.max {
            Verdict::Hold { reason: format!("hop {} exceeds {}", env.hop, self.max) }
        } else {
            Verdict::Deliver
        }
    }
}
