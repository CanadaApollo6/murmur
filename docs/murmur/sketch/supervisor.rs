//! One actor per agent: lifecycle, the delivery gate, permission policy, restart.
//!
//! The actor is the single owner of the agent's `Phase`, live `Session` and
//! `Incarnation`. Nothing else holds a session handle, so "never deliver into a busy
//! CLI agent" and "never run two sessions for one agent" are true by construction:
//! there is exactly one place that can call `submit` or `open`.
//!
//! Same code for both agent kinds. The only per-wire inputs are
//! `Adapter::delivery_gate()`, `Session::resumable()` and `KindConfig::idle_after`.
//!
//! Delivery accounting, identical for every wire: `mark_offered` before `submit`;
//! `SessionEvent::TurnStarted{ids}` -> `mark_started`; `SessionEvent::Committed{ids}` ->
//! `mark_consumed` (the only path to `Consumed`); session end -> `session_ended`, which
//! re-offers mail without turn-start evidence and holds the rest as `Uncertain`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::adapter::{
    Adapter, AdapterError, DeliveryGate, EndReason, OpenRequest, Session, SessionEvent, Turn,
    TurnItem, TurnOutcome,
};
use crate::mailroom::Mailroom;
use crate::model::{
    AgentRecord, DeliveryClass, Envelope, Incarnation, MessageId, PermissionAnswer, PermissionId,
    PermissionRequest, ResumeToken, RuleDecision,
};
use crate::surfaces::AuthenticatedSender;
use crate::BrokerError;

/// Lifecycle. Persisted only as `(resume, incarnation, retired)` in the registry;
/// after a broker restart every actor starts in `Dormant`.
///
/// ```text
///            nudge (mail queued)
///  Dormant ---------------------> Starting --open ok--> Idle <--TurnEnded-- Busy
///     ^                            |    |                |  \---submit------->^
///     |                    open err|    |resume failed   | idle_after elapsed (resumable)
///     |                            v    v                v
///     +-- backoff elapsed ---- Crashed  NeedsRecovery   Suspending --> Dormant
///                               ^   |
///          Busy --Ended---------+   +-- 5 crashes in 10 min --> Failed
///
///  Failed | NeedsRecovery --`agentctl restart`--> Starting (resume again, never fresh)
///  any --stop--> Retired (terminal)
/// ```
///
/// Infinity agents never pass through Suspending: their `idle_after` is `None`, and an
/// idle Infinity thread already costs nothing (its driver has exited; the session is
/// just an observed daemon connection).
pub enum Phase {
    Dormant,
    Starting,
    Idle { since: Instant },
    Busy { pending_permission: Option<PermissionRequest> },
    Suspending,
    Crashed { attempt: u32, retry_at: Instant },
    Failed { last: String },
    /// A resume token exists and could not be restored (`ResumeFailed` /
    /// `ResumeUnsupported`). Mail keeps queuing; nothing starts a fresh session behind
    /// the human's back.
    NeedsRecovery { reason: String },
    Retired,
}

pub enum Command {
    /// Mail was queued (or released) for this agent. Cheap and coalescable.
    Nudge,
    Cancel,
    Stop,
    AnswerPermission {
        by: AuthenticatedSender,
        id: PermissionId,
        answer: PermissionAnswer,
        reply: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
    /// From `Failed` or `NeedsRecovery`: try `start` again (resume when a token exists).
    Restart,
}

#[derive(Clone)]
pub struct ActorHandle {
    pub tx: tokio::sync::mpsc::Sender<Command>,
}

pub struct AgentActor {
    record: AgentRecord,
    adapter: Arc<dyn Adapter>,
    idle_after: Option<Duration>,
    mailroom: Arc<Mailroom>,
    registry: Arc<crate::Registry>,
    events: crate::surfaces::EventBus,
    phase: Phase,
    session: Option<Box<dyn Session>>,
    /// Ids offered to the live incarnation and not yet `Committed`. Used only to keep
    /// `TurnStarted` honest (an adapter may name only ids it was given).
    offered: Vec<MessageId>,
    commands: tokio::sync::mpsc::Receiver<Command>,
}

impl AgentActor {
    /// Main loop: `select!` over commands, `session.next_event()`, and the phase timer
    /// (idle deadline or crash backoff). Every branch ends in `self.pump()`.
    pub async fn run(mut self) {
        todo!()
    }

    /// The delivery gate. Called after every state change; idempotent.
    ///
    /// ```text
    /// let mail = mailroom.queued_for(id)            // oldest first
    /// if mail.is_empty() { return }
    /// match phase {
    ///   Dormant | Crashed{retry_at <= now}  => self.start().await   // then pump again
    ///   Starting | Suspending | Failed | NeedsRecovery | Retired | Crashed{..} => return
    ///   Busy{pending_permission: Some(_)} => return                 // mail queues
    ///   Busy{..} if gate == AtTurnBoundary => return                // wait for TurnEnded
    ///   Idle{..} | Busy{..} => {
    ///     let turn = build_turn(mail)            // UserTurn -> TurnItem::User,
    ///                                            // PeerEvent -> TurnItem::Peer{rendered}
    ///     mailroom.mark_offered(ids, incarnation) // write-ahead
    ///     offered.extend(ids)
    ///     session.submit(turn)                    // Err(Ended) -> handle as Ended
    ///     phase = Busy{None}
    ///   }
    /// }
    /// ```
    ///
    /// Batching falls out: an AtTurnBoundary agent that receives five messages during a
    /// long turn gets one turn with five items.
    async fn pump(&mut self) {
        todo!()
    }

    /// Open (or resume) the session with `incarnation + 1`, persisted before `open` so a
    /// crash during open still advances it. Resume token from the registry when present.
    ///
    /// ```text
    /// match adapter.open(req) {
    ///   Ok(s)                                         => phase = Idle; session = s
    ///   Err(ResumeFailed(e) | ResumeUnsupported)      => phase = NeedsRecovery{e}
    ///   Err(Spawn | Protocol | Ended)                 => phase = Crashed{backoff}
    /// }
    /// if !session.resumable() { idle_after = None }   // never suspend what cannot resume
    /// ```
    async fn start(&mut self) -> Result<(), AdapterError> {
        todo!()
    }

    async fn on_event(&mut self, ev: SessionEvent) {
        // TurnStarted{ids}   -> phase = Busy; mailroom.mark_started(ids, incarnation)
        // Committed{ids}     -> mailroom.mark_consumed(ids); offered.retain(not in ids)
        // Activity(a)        -> events.publish(BrokerEvent::Activity)   display only:
        //                       never queued as mail, never put in a Turn
        // Permission(req)    -> debug_assert(req.incarnation == record.incarnation);
        //                       self.on_permission(req)
        // TurnEnded(o)       -> phase = Idle{now}; events.publish; pump()
        // ResumeToken(t)     -> registry.save_resume(id, t)
        // Ended(r)           -> self.on_ended(r)
        todo!()
    }

    /// Apply policy; escalate what policy does not decide.
    ///
    /// ```text
    /// match first rule in record.spec.permissions.rules matching req.tool {
    ///   Allow => session.answer_permission(req.id, option with effect AllowOnce)
    ///   Deny  => session.answer_permission(req.id, option with effect RejectOnce)
    ///   Ask | no match => match escalate_to {
    ///     Human     => events.publish(PermissionPending(req)); store in phase
    ///     Agent(id) => mailroom.accept(NewMessage { from: Sender::Agent(self.id), to: id,
    ///                  body: render(req) }) and store in phase; the supervisor answers
    ///                  with the answer_permission tool
    ///   }
    /// }
    /// if timeout set -> timer; on expiry answer RejectOnce
    /// ```
    ///
    /// While a request is pending the agent is Busy; mail keeps queuing.
    async fn on_permission(&mut self, req: PermissionRequest) {
        todo!()
    }

    /// `Command::AnswerPermission`.
    ///
    /// ```text
    /// let Busy{ pending_permission: Some(req) } = phase, req.id == id
    ///     else -> Err(StalePermission(id))        // unknown, answered, or session gone
    /// if req.incarnation != record.incarnation -> Err(StalePermission(id))
    /// match (by.sender(), escalate_to) {
    ///   (Human, _) | (Agent(a), Escalation::Agent(a)) => ok
    ///   _ => Err(Forbidden)
    /// }
    /// Choose(key) -> key must be one of req.options; session.answer_permission(id, key)
    /// CancelTurn  -> session.cancel()
    /// events.publish(PermissionAnswered{id})
    /// ```
    async fn on_answer(&mut self, by: AuthenticatedSender, id: PermissionId, answer: PermissionAnswer) -> Result<(), BrokerError> {
        todo!()
    }

    /// Session gone.
    ///
    /// ```text
    /// mailroom.session_ended(id, incarnation)
    ///   // Offered, no turn-start evidence -> Queued (re-offer)
    ///   // Offered, turn started           -> Uncertain (held for the human)
    /// pending_permission.take() -> events.publish(PermissionAnswered{id})  // withdrawn;
    ///                              // a late answer now fails as StalePermission
    /// offered.clear()
    /// match (reason, phase) {
    ///   (Requested, Suspending)     => Dormant
    ///   (Requested, Retired)        => Retired
    ///   (_, Idle{..})               => Dormant           // died idle: just resume on next mail
    ///   (Exited|DaemonGone, Busy{..}) => Crashed{attempt+1, backoff(attempt)}
    ///                                    // 1s,2s,4s..60s; 5 crashes in 10 min -> Failed
    /// }
    /// pump()   // re-offers requeued mail once the agent is back
    /// ```
    async fn on_ended(&mut self, reason: EndReason) {
        todo!()
    }

    async fn suspend(&mut self) {
        // only when session.resumable(): phase = Suspending;
        // token = session.take().suspend(); registry.save_resume; phase = Dormant.
        todo!()
    }
}

pub fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.min(6)).min(Duration::from_secs(60))
}

fn build_turn(mail: Vec<Envelope>, names: &crate::Registry) -> Turn {
    // for env in mail:
    //   match env.class() {
    //     DeliveryClass::UserTurn  => TurnItem::User(env),
    //     DeliveryClass::PeerEvent => TurnItem::Peer { rendered: env.render_peer(..), envelope: env },
    //   }
    todo!()
}
