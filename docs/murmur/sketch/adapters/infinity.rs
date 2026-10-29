//! Model agents: one Infinity root thread per agent, driven through the Infinity
//! daemon's own client protocol (`infinity_protocol::{ClientMessage, DaemonMessage}`
//! over `~/.infinity/daemon.sock`, length-delimited JSON).
//!
//! The daemon stays the owner of everything Infinity already owns: thread stores,
//! providers, RAP servers, compaction, hibernation (driver exit). The broker only
//! creates sessions, feeds them input, and watches their events, like `infinity acp`
//! does (`crates/infinity-agent-cli/src/acp_server.rs`), minus ACP.
//!
//! Why the daemon and not an embedded `SessionManager`: the daemon is a separate
//! failure domain (a broker restart does not kill model agents mid-completion), its
//! `!Send` single-threaded runtime stays out of the broker, and the broker touches only
//! the public wire protocol, which upstream already keeps stable for its TUI, web UI,
//! ACP server and Slack bot. `handle_client_channels` is transport-agnostic, so an
//! embedded mode is a transport swap inside `DaemonLink`, not a second adapter.
//!
//! Uses three variants from the fork's peer-message commit (`infinity_patch.rs`):
//! `ClientMessage::DeliverPeerMessage` (peer mail as a tool exchange, never user text),
//! `ClientMessage::Interrupt` (cancel), and `DaemonMessage::InputCommitted` (the only
//! source of `SessionEvent::Committed` on this wire).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use infinity_protocol::{ChoiceId, ClientMessage, DaemonMessage, ModelRef, ThreadId, ThreadRef};

use crate::adapter::{
    Activity, Adapter, AdapterError, DeliveryGate, EndReason, OpenRequest, Session, SessionEvent,
    Turn, TurnItem, TurnOutcome,
};
use crate::model::{AgentId, Incarnation, MessageId, PermissionId, ResumeToken};

/// Shared by every Infinity agent. Owns the daemon epoch: when the control connection
/// drops, every open `InfinitySession` gets `Ended(DaemonGone)`, the actors requeue and
/// reopen after `services` has the daemon back, and the reopened sessions run under a
/// new `Incarnation`. Daemon death therefore uses the exact crash path a CLI agent's
/// process exit uses.
pub struct InfinityAdapter {
    link: DaemonLink,
    /// Root thread -> agent, for `surfaces::bindings`. Filled from every agent's token
    /// at startup (`index`) and on every `open`, so a RAP call from a thread that kept
    /// running across a broker restart still authenticates.
    roots: std::sync::RwLock<HashMap<ThreadId, AgentId>>,
}

impl InfinityAdapter {
    /// Decode each persisted token (privately) and record its root.
    pub fn index(&self, tokens: &[(AgentId, ResumeToken)]) {
        todo!()
    }
    pub fn agent_for_root(&self, root: &ThreadId) -> Option<AgentId> {
        todo!()
    }
}

/// Connection factory for the daemon socket. One control connection (Welcome,
/// SessionsUpdated) plus one connection per open session, the same layout
/// `acp_server::run` uses.
pub struct DaemonLink {
    socket: std::path::PathBuf, // infinity_protocol::socket_path()
    epoch: tokio::sync::watch::Receiver<u64>,
}

/// Private token shape; encoded with `ResumeToken::encode`, read only here.
#[derive(serde::Serialize, serde::Deserialize)]
struct InfinityResume {
    root: ThreadId,
}

#[async_trait]
impl Adapter for InfinityAdapter {
    /// Patched Infinity core defers `PeerMessage` synthetics while a non-passive tool call
    /// is pending (`is_deferrable_synthetic_event`, `InMemoryDeferQueue`), settles a tool
    /// result before any peer in the same batch (`Thread::filter_deferrable`), and the
    /// driver queues non-user input during a completion, so peer events never
    /// interrupt. Passive tools (the sleep tools) are interrupted, which is what "sleep
    /// until a reply arrives" wants.
    fn delivery_gate(&self) -> DeliveryGate {
        DeliveryGate::Anytime
    }

    /// ```text
    /// let conn = link.connect()
    /// match req.resume {
    ///   None => conn.send(CreateSession { cwd: spec.cwd, location: None,
    ///                         model: spec.model.map(parse ModelRef),
    ///                         keeps_session_alive: false })
    ///           // false: the broker's observer connection must not pin the session's
    ///           // RAP servers up; the daemon idles them out as usual.
    ///   Some(t) => {
    ///     let r: InfinityResume = t.decode().ok_or(ResumeFailed("not an infinity token"))?
    ///     conn.send(Connect { root_thread_id: ThreadRef::local(r.root), thread_id: None,
    ///                         keeps_session_alive: false })
    ///     // DaemonMessage::Error { text: "failed to resume session: .." } -> Err(ResumeFailed)
    ///     // Never fall back to CreateSession.
    ///   }
    /// }
    /// await Connected { root_thread_id, .. }; roots.insert(root, req.agent)
    /// on Replay { pending_choices, .. }: each UserChoiceRequired -> Permission with
    ///   req.incarnation (requests from the old incarnation are already withdrawn)
    /// ```
    ///
    /// Reopen needs no receipt archaeology: re-offered peers that the old incarnation
    /// already committed hit core dedup and are answered with `InputCommitted` again.
    ///
    /// Never sends `SoftDetach` or `ShutdownSession` for a live agent: a shut-down
    /// session's router admission drops non-user input, which would silently discard
    /// peer messages.
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError> {
        todo!()
    }
}

pub struct InfinitySession {
    conn: DaemonConn,
    root: ThreadRef,
    incarnation: Incarnation,
    /// Human items submitted as `UserInput`, oldest first, waiting for their
    /// `UserInputEcho` (turn-start evidence). The echo text may carry the driver's
    /// `<interrupt>` prefix (`system/local/driver.rs`), so match modulo that prefix.
    awaiting_echo: VecDeque<(MessageId, String)>,
    /// Human items echoed but not yet committed.
    started: VecDeque<MessageId>,
    /// `UserChoiceRequired.id` and its choices per PermissionId, to answer with
    /// `UserChoiceAnswered { choice_id, selected }`.
    choices: HashMap<PermissionId, (ChoiceId, Vec<String>)>,
}

pub struct DaemonConn {/* framed UnixStream: Sink<ClientMessage> + Stream<DaemonMessage> */}

#[async_trait]
impl Session for InfinitySession {
    /// `ResumeToken::encode(&InfinityResume { root })`.
    fn resume_token(&self) -> ResumeToken {
        todo!()
    }

    /// Always: `Connect` reattaches any root thread the daemon still stores.
    fn resumable(&self) -> bool {
        true
    }

    /// ```text
    /// for item in turn.items:
    ///   User(env)            => ClientMessage::UserInput { thread_id: root, text: env.body }
    ///                           awaiting_echo.push_back((env.id, env.body))
    ///   Peer{envelope, rendered} => ClientMessage::DeliverPeerMessage {     // patched variant
    ///                               thread_id: root, message_id: envelope.id.to_string(),
    ///                               from: sender name, text: rendered }
    ///                           // daemon dedup id: format!("peer-{message_id}")
    /// ```
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError> {
        todo!()
    }

    /// `ClientMessage::Interrupt { thread_id: root }` (patched variant). The daemon routes
    /// it to the root's driver, which cancels its in-flight step (`InFlightStep::cancel`)
    /// without injecting user text. No in-flight step: no-op.
    async fn cancel(&mut self) -> Result<(), AdapterError> {
        todo!()
    }

    /// RAP tools that gate on approval (sandbox `execute_command`) raise
    /// `UserChoiceRequired`; the daemon answers the tool's `response_url` itself when it
    /// receives `UserChoiceAnswered { choice_id, selected }`. Incarnation fencing happens
    /// in the actor before this is called.
    async fn answer_permission(&mut self, id: PermissionId, key: &str) -> Result<(), AdapterError> {
        todo!()
    }

    /// ```text
    /// match daemon_msg {
    ///   TextChunk|ThinkingChunk{thread_id}  -> Activity (root and children alike;
    ///                                          children inherit the root's subscribers)
    ///   ToolCall / ToolResult / SubscriptionEvent -> Activity (display only)
    ///   UserInputEcho{thread_id: root, text}
    ///        front of awaiting_echo matches -> started.push(id); TurnStarted { ids: [id] }
    ///   InputCommitted{thread_id: root, dedup_id}                     // patched variant
    ///        dedup_id = "peer-{ulid}"       -> Committed { ids: [MessageId(ulid)] }
    ///        any other dedup_id             -> Committed { ids: started.drain(..) }
    ///          // a human item's dedup id is minted by the daemon, so it cannot be named;
    ///          // but core validates and syncs inputs together, in order, so any commit
    ///          // after an item's echo implies that item is durable too.
    ///   UserChoiceRequired{thread_id, id, prompt, choices, default}
    ///                                       -> Permission(PermissionRequest {
    ///                                            incarnation: self.incarnation,
    ///                                            tool: "infinity:" + prompt's tool,
    ///                                            options: choices mapped by index })
    ///   ResponseDone{thread_id: root, token_usage}
    ///                                       -> Activity(Usage), TurnEnded(Completed)
    ///                                          (not a receipt: core emits it even when
    ///                                           inference failed before any output)
    ///   Error{..}                           -> TurnEnded(Failed)
    ///   connection closed                   -> Ended(DaemonGone)
    /// }
    /// first StartOutput for root after an idle stretch -> TurnStarted { ids: [] } (phase only)
    /// ```
    async fn next_event(&mut self) -> Option<SessionEvent> {
        todo!()
    }

    /// Nothing to stop: an idle Infinity thread has no driver. Send `Disconnect` and
    /// return the token. Not called in practice (`idle_after = None`).
    async fn suspend(self: Box<Self>) -> Result<ResumeToken, AdapterError> {
        todo!()
    }
}
