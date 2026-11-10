use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::{Arc, mpsc};

#[path = "mail_feed.rs"]
mod feed;
pub use feed::{Anchor, MailQuery};
use std::thread;

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use time::OffsetDateTime;

use crate::model::*;
use crate::swarm::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] rusqlite::Error),
    #[error(transparent)]
    Codec(#[from] serde_json::Error),
    #[error("mailroom worker stopped")]
    Closed,
    #[error("mailroom worker panicked")]
    Panicked,
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("mail limit: {0}")]
    MailLimit(String),
    #[error("illegal delivery transition from {0:?}")]
    Transition(DeliveryState),
    #[error("task budget exceeded")]
    BudgetExceeded,
}
pub type Result<T> = std::result::Result<T, Error>;

pub struct NewMessage {
    pub from: Sender,
    pub to: AgentId,
    pub body: Body,
    pub reply_to: Option<MessageId>,
    pub client_key: ClientKey,
    pub act: Act,
    /// Current task, or the parent of a new Request. Replies inherit it if omitted.
    pub task: Option<TaskId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Queued { id: MessageId },
    Held { id: MessageId, reason: String },
    Duplicate { id: MessageId },
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct OfferOutcome {
    /// Only these IDs may be submitted to the session.
    pub offered: Vec<MessageId>,
    pub held: Vec<MessageId>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SessionEndOutcome {
    pub requeued: u64,
    pub uncertain: Vec<MessageId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub state: DeliveryState,
    pub attempts: u32,
    pub turn_started: bool,
}

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
    pub pair_exchanges: HashMap<(AgentId, AgentId), u32>,
}

pub struct HopLimit {
    pub max: u16,
}

impl Interceptor for HopLimit {
    fn name(&self) -> &'static str {
        "hop-limit"
    }
    fn inspect(&self, env: &Envelope, _view: &ConversationView) -> Verdict {
        if env.hop() > self.max {
            Verdict::Hold {
                reason: format!("hop {} exceeds {}", env.hop(), self.max),
            }
        } else {
            Verdict::Deliver
        }
    }
}

/// Bounds on mail one agent can queue and one turn can carry. `None` disables a limit.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MailLimits {
    /// Undelivered mail from one agent across all recipients.
    #[serde(deserialize_with = "mail_limit")]
    pub per_sender: Option<NonZeroU32>,
    /// Undelivered mail from one agent to one recipient.
    #[serde(deserialize_with = "mail_limit")]
    pub per_pair: Option<NonZeroU32>,
    /// Envelopes offered to a session in one pump.
    #[serde(deserialize_with = "mail_limit")]
    pub per_turn: Option<NonZeroU32>,
}

impl Default for MailLimits {
    fn default() -> Self {
        Self {
            per_sender: NonZeroU32::new(64),
            per_pair: NonZeroU32::new(16),
            per_turn: NonZeroU32::new(16),
        }
    }
}

fn mail_limit<'de, D: Deserializer<'de>>(
    de: D,
) -> std::result::Result<Option<NonZeroU32>, D::Error> {
    Ok(NonZeroU32::new(u32::deserialize(de)?))
}

pub struct Config {
    pub max_attempts: u32,
    pub limits: MailLimits,
    pub interceptors: Vec<Box<dyn Interceptor>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            limits: MailLimits::default(),
            interceptors: vec![Box::new(HopLimit { max: 24 })],
        }
    }
}

#[cfg(test)]
#[path = "mail_trace.rs"]
mod trace;

type Job = Box<dyn FnOnce(&mut Store) + Send>;

/// One thread owns the connection. SQLite's exclusive WAL lock also excludes a
/// second mailroom opening this file. Call `close` to drain work and release it.
pub struct Mailroom {
    jobs: mpsc::Sender<Job>,
    worker: thread::JoinHandle<Result<()>>,
    observers: Arc<tokio::sync::Semaphore>,
    changes: tokio::sync::watch::Receiver<i64>,
    #[cfg(test)]
    pub(crate) jobs_run: Arc<std::sync::atomic::AtomicUsize>,
}

pub(crate) struct ActorLease {
    agent: AgentId,
    owner: ulid::Ulid,
    jobs: mpsc::Sender<Job>,
}

impl Drop for ActorLease {
    fn drop(&mut self) {
        let agent = self.agent;
        let owner = self.owner;
        if self
            .jobs
            .send(Box::new(move |store| {
                if store.actors.get(&agent) == Some(&owner) {
                    store.actors.remove(&agent);
                }
            }))
            .is_err()
        {
            tracing::debug!("actor lease released after mailroom close");
        }
    }
}

impl Mailroom {
    pub async fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let mut store = tokio::task::spawn_blocking(move || Store::open(&path, config)).await??;
        let (jobs, rx) = mpsc::channel::<Job>();
        let (changes_tx, changes) = tokio::sync::watch::channel(feed::head(&store.db)?);
        #[cfg(test)]
        let jobs_run = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        #[cfg(test)]
        let counter = jobs_run.clone();
        let worker = thread::Builder::new()
            .name("agentd-mailroom".into())
            .spawn(move || {
                let mut pending_notification=false;
                let mut retry_at:Option<std::time::Instant>=None;
                let mut backoff=std::time::Duration::from_millis(20);
                loop {
                    let job=if let Some(deadline)=retry_at {
                        match rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
                            Ok(job)=>Some(job),
                            Err(mpsc::RecvTimeoutError::Timeout)=>None,
                            Err(mpsc::RecvTimeoutError::Disconnected)=>break,
                        }
                    } else {
                        match rx.recv() {Ok(job)=>Some(job),Err(mpsc::RecvError)=>break}
                    };
                    if let Some(job)=job {
                        #[cfg(test)]
                        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let before = store.db.total_changes();
                        #[cfg(test)]
                        let cache = trace::cache(&store.db);
                        #[cfg(test)]
                        let _job = trace::job();
                        job(&mut store);
                        #[cfg(test)]
                        trace::cache_delta(cache, &store.db);
                        pending_notification |= store.db.total_changes()!=before;
                    }
                    if pending_notification && retry_at.is_none_or(|deadline|std::time::Instant::now()>=deadline) {
                        #[cfg(test)]
                        let _head = trace::maintenance("post_accept_head");
                        let head = match feed::head(&store.db) {
                            Ok(head)=>head,
                            Err(error)=>{
                                tracing::error!(%error, retry_ms=backoff.as_millis(), "mail event head maintenance failed; retrying pending notification");
                                retry_at=Some(std::time::Instant::now()+backoff);
                                backoff=(backoff*2).min(std::time::Duration::from_secs(1));
                                continue;
                            }
                        };
                        pending_notification=false;
                        retry_at=None;
                        backoff=std::time::Duration::from_millis(20);
                        changes_tx.send_if_modified(|seq| {
                            if head > *seq {
                                *seq = head;
                                true
                            } else {
                                false
                            }
                        });
                        #[cfg(test)]
                        drop(_head);
                        if head - store.last_prune >= 1024 {
                            #[cfg(test)]
                            let _prune = trace::maintenance("post_accept_prune_retention");
                            match feed::prune(&mut store.db) {
                                Ok(deleted)=>{store.last_prune=head;#[cfg(test)] store.prunes.push((head,deleted));#[cfg(not(test))] tracing::trace!(deleted,"mail event retention pass");},
                                Err(error)=>tracing::error!(%error,"mail event pruning failed; worker will retry"),
                            }
                        }
                    }
                }
                store.db.close().map_err(|(_, e)| Error::Store(e))
            })?;
        Ok(Self {
            jobs,
            worker,
            observers: Arc::new(tokio::sync::Semaphore::new(1)),
            changes,
            #[cfg(test)]
            jobs_run,
        })
    }

    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        #[cfg(test)]
        let job_name = std::any::type_name_of_val(&f);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.jobs
            .send(Box::new(move |store| {
                #[cfg(test)]
                trace::label(job_name);
                if let Err(result) = tx.send(f(store)) {
                    match result {
                        Ok(_) => tracing::debug!("mailroom caller cancelled after submission"),
                        Err(error) => tracing::error!(%error, "mailroom operation failed after caller cancellation"),
                    }
                }
            }))
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    pub fn changes(&self) -> tokio::sync::watch::Receiver<i64> {
        self.changes.clone()
    }

    pub(crate) async fn observe<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .observers
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        #[cfg(test)]
        let job_name = std::any::type_name_of_val(&f);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.jobs
            .send(Box::new(move |store| {
                #[cfg(test)]
                trace::label(job_name);
                let _permit = permit;
                if tx.is_closed() {
                    #[cfg(test)]
                    trace::label("cancelled_observer_before_read");
                    return;
                }
                #[cfg(test)]
                trace::mark_observer();
                if let Err(result) = tx.send(f(store)) {
                    match result {
                        Ok(value) => {
                            #[cfg(test)]
                            let _cleanup = trace::query("cancelled_observer_result_drop");
                            drop(value);
                        }
                        Err(error) => tracing::error!(%error, "observer failed after cancellation"),
                    }
                }
            }))
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    pub async fn close(self) -> Result<()> {
        drop(self.jobs);
        tokio::task::spawn_blocking(move || self.worker.join().map_err(|_| Error::Panicked)?)
            .await?
    }

    pub(crate) async fn claim_actor(
        &self,
        agent: AgentId,
        kind: String,
    ) -> Result<(ActorLease, Incarnation, Option<crate::adapter::ResumeToken>)> {
        let owner = ulid::Ulid::new();
        let lease = ActorLease {
            agent,
            owner,
            jobs: self.jobs.clone(),
        };
        let (incarnation, resume) = self.allocate_incarnation(agent, kind, Some(owner)).await?;
        Ok((lease, incarnation, resume))
    }

    pub(crate) async fn next_incarnation(
        &self,
        agent: AgentId,
        kind: String,
    ) -> Result<(Incarnation, Option<crate::adapter::ResumeToken>)> {
        self.allocate_incarnation(agent, kind, None).await
    }

    async fn allocate_incarnation(
        &self,
        agent: AgentId,
        kind: String,
        claim: Option<ulid::Ulid>,
    ) -> Result<(Incarnation, Option<crate::adapter::ResumeToken>)> {
        self.call(move |s| {
            active(&s.db, agent)?;
            if claim.is_some() && s.actors.contains_key(&agent) {
                return Err(Error::Rejected("agent already supervised".into()));
            }
            if claim.is_none() && !s.actors.contains_key(&agent) {
                return Err(Error::Rejected("actor lease missing".into()));
            }
            let tx = s.db.transaction()?;
            tx.execute(
                "INSERT OR IGNORE INTO actor_state(agent,incarnation,kind) VALUES (?,0,?)",
                params![json(&agent)?, kind],
            )?;
            let (previous, resume, saved_kind): (u64, Option<String>, String) = tx.query_row(
                "SELECT incarnation,resume,kind FROM actor_state WHERE agent=?",
                [json(&agent)?],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            if saved_kind != kind {
                return Err(Error::Rejected("agent kind cannot change".into()));
            }
            let next = previous
                .checked_add(1)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or_else(|| Error::Rejected("incarnation overflow".into()))?;
            tx.execute(
                "UPDATE actor_state SET incarnation=? WHERE agent=?",
                params![next, json(&agent)?],
            )?;
            tx.commit()?;
            if let Some(owner) = claim {
                s.actors.insert(agent, owner);
            }
            Ok((
                Incarnation(next),
                resume.map(|s| serde_json::from_str(&s)).transpose()?,
            ))
        })
        .await
    }

    /// Resume tokens of agents that are not retired. An agent keeps its token
    /// across restarts, replacement and recovery.
    pub(crate) async fn live_resume_tokens(&self) -> Result<Vec<crate::adapter::ResumeToken>> {
        self.call(|s| {
            let mut q = s.db.prepare(
                "SELECT resume FROM actor_state JOIN agents ON agent=agents.id \
                 WHERE retired=0 AND resume IS NOT NULL",
            )?;
            Ok(q.query_map([], |r| json_col(r, 0))?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
    }

    pub(crate) async fn save_resume(
        &self,
        agent: AgentId,
        token: crate::adapter::ResumeToken,
    ) -> Result<()> {
        self.call(move |s| {
            s.db.execute(
                "UPDATE actor_state SET resume=? WHERE agent=?",
                params![json(&token)?, json(&agent)?],
            )?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn sender_label(&self, agent: AgentId) -> Result<(String, String)> {
        self.call(move |s| Ok(s.db.query_row("SELECT a.name,COALESCE(s.kind,'unknown') FROM agents a LEFT JOIN actor_state s ON s.agent=a.id WHERE a.id=?", [json(&agent)?], |r| Ok((r.get(0)?,r.get(1)?)))?)).await
    }

    pub(crate) async fn save_spec(
        &self,
        id: AgentId,
        spec: crate::supervisor::AgentSpec,
    ) -> Result<()> {
        self.call(move |s| {
            s.db.execute("INSERT INTO agent_specs(agent,spec) VALUES (?,?) ON CONFLICT(agent) DO UPDATE SET spec=excluded.spec", params![json(&id)?, json(&spec)?])?;
            Ok(())
        }).await
    }

    /// An operator stop outlives the actor and the daemon, so the actor reads it at startup.
    pub(crate) async fn set_stopped(&self, agent: AgentId, stopped: bool) -> Result<()> {
        self.call(move |s| {
            if s.db.execute(
                "UPDATE agent_specs SET stopped=? WHERE agent=?",
                params![stopped, json(&agent)?],
            )? == 0
            {
                return Err(Error::NotFound(agent.to_string()));
            }
            Ok(())
        })
        .await
    }

    pub async fn stopped(&self, agent: AgentId) -> Result<bool> {
        self.call(move |s| {
            Ok(s.db
                .query_row(
                    "SELECT stopped FROM agent_specs WHERE agent=?",
                    [json(&agent)?],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(false))
        })
        .await
    }

    pub(crate) async fn forget_resume(&self, agent: AgentId) -> Result<()> {
        self.call(move |s| {
            s.db.execute(
                "UPDATE actor_state SET resume=NULL WHERE agent=?",
                [json(&agent)?],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn specs(&self) -> Result<Vec<(AgentId, crate::supervisor::AgentSpec)>> {
        self.call(|s| {
            let mut q = s.db.prepare("SELECT agent,spec FROM agent_specs JOIN agents ON agent=agents.id WHERE retired=0 ORDER BY name")?;
            Ok(q.query_map([], |r| Ok((json_col(r,0)?, json_col(r,1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?)
        }).await
    }

    pub async fn register(&self, name: AgentName) -> Result<AgentId> {
        self.call(move |s| {
            if let Some(id) =
                s.db.query_row(
                    "SELECT id FROM agents WHERE name=? AND retired=0",
                    [name.as_str()],
                    |r| json_col(r, 0),
                )
                .optional()?
            {
                return Ok(id);
            }
            let id = AgentId::new();
            s.db.execute(
                "INSERT INTO agents(id,name) VALUES (?,?)",
                params![json(&id)?, name.as_str()],
            )?;
            Ok(id)
        })
        .await
    }

    pub async fn resolve(&self, recipient: Recipient) -> Result<AgentId> {
        self.call(move |s| {
            let id = match recipient {
                Recipient::Id(id) => id,
                Recipient::Name(name) => {
                    s.db.query_row(
                        "SELECT id FROM agents WHERE name=? AND retired=0",
                        [name.as_str()],
                        |r| json_col(r, 0),
                    )
                    .optional()?
                    .ok_or_else(|| Error::NotFound(name.as_str().into()))?
                }
            };
            active(&s.db, id)?;
            Ok(id)
        })
        .await
    }

    pub async fn accept(&self, msg: NewMessage) -> Result<(SendOutcome, Envelope)> {
        self.call(move |s| s.accept(msg)).await
    }

    pub async fn message(&self, id: MessageId) -> Result<Envelope> {
        self.call(move |s| message(&s.db, id)).await
    }

    pub async fn pending_for(&self, agent: AgentId) -> Result<Vec<Envelope>> {
        self.call(move |s| envelopes(&s.db, "SELECT m.* FROM messages m JOIN deliveries d ON d.message_id=m.id WHERE m.recipient=? AND d.state='queued' ORDER BY m.seq", [json(&agent)?])).await
    }

    pub async fn next_batch(&self, agent: AgentId) -> Result<Vec<Envelope>> {
        self.call(move |s| {
            let limit = s.config.limits.per_turn.map_or(-1, |n| i64::from(n.get()));
            envelopes(&s.db, "SELECT m.* FROM messages m JOIN deliveries d ON d.message_id=m.id WHERE m.recipient=? AND d.state='queued' ORDER BY m.seq LIMIT ?", params![json(&agent)?, limit])
        }).await
    }

    pub(crate) async fn has_pending(&self, agent: AgentId) -> Result<bool> {
        self.call(move |s| Ok(s.db.query_row("SELECT EXISTS(SELECT 1 FROM messages m JOIN deliveries d ON d.message_id=m.id WHERE m.recipient=? AND d.state='queued')", [json(&agent)?], |r| r.get(0))?)).await
    }

    pub async fn delivery(&self, id: MessageId) -> Result<Delivery> {
        self.call(move |s| delivery(&s.db, id)).await
    }

    pub async fn mark_offered(
        &self,
        ids: &[MessageId],
        incarnation: Incarnation,
    ) -> Result<OfferOutcome> {
        let ids = unique(ids)?;
        self.call(move |s| {
            let tx = s.db.transaction()?;
            let mut outcome = OfferOutcome::default();
            for id in ids {
                let mut d = delivery(&tx, id)?;
                // A repeated offer under the same incarnation is a retry, not a new attempt.
                if d.state == (DeliveryState::Offered { incarnation }) {
                    outcome.offered.push(id);
                    continue;
                }
                if d.state != DeliveryState::Queued {
                    return Err(Error::Transition(d.state));
                }
                if d.attempts >= s.config.max_attempts {
                    d.state = DeliveryState::Held {
                        reason: "maximum delivery attempts reached".into(),
                    };
                    outcome.held.push(id);
                } else {
                    d.attempts += 1;
                    d.state = DeliveryState::Offered { incarnation };
                    outcome.offered.push(id);
                }
                d.turn_started = false;
                save_delivery(&tx, id, &d)?;
            }
            tx.commit()?;
            Ok(outcome)
        })
        .await
    }

    pub async fn mark_started(&self, ids: &[MessageId], incarnation: Incarnation) -> Result<()> {
        let ids = unique(ids)?;
        self.call(move |s| {
            let tx = s.db.transaction()?;
            for id in ids {
                let mut d = delivery(&tx, id)?;
                require_offered(&d, incarnation)?;
                d.turn_started = true;
                save_delivery(&tx, id, &d)?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn mark_consumed(&self, ids: &[MessageId], incarnation: Incarnation) -> Result<()> {
        let ids = unique(ids)?;
        self.call(move |s| {
            let tx = s.db.transaction()?;
            for id in ids {
                let mut d = delivery(&tx, id)?;
                // Receipts can repeat (core answers a re-offered input again).
                if d.state == DeliveryState::Consumed {
                    continue;
                }
                require_offered(&d, incarnation)?;
                let env = message(&tx, id)?;
                let position = exposure_ref(&tx, env.to)?;
                let seq = position.seq.checked_add(1).ok_or_else(|| Error::Rejected("exposure sequence overflow".into()))?;
                tx.execute("INSERT INTO exposures(agent,epoch,seq,source_kind,source_id) VALUES (?,?,?,'message',?)", params![json(&env.to)?, position.epoch.0, seq, json(&id)?])?;
                d.state = DeliveryState::Consumed;
                save_delivery(&tx, id, &d)?;
            }
            tx.commit()?;
            Ok(())
        }).await
    }

    /// The argument supplements durable `mark_started` evidence. Recovery passes
    /// false; persisted evidence still makes the delivery Uncertain.
    pub async fn session_ended(
        &self,
        agent: AgentId,
        ended: Incarnation,
        turn_started: bool,
    ) -> Result<SessionEndOutcome> {
        self.settle_session(agent, ended, turn_started, false).await
    }

    pub(crate) async fn settle_session(
        &self,
        agent: AgentId,
        ended: Incarnation,
        turn_started: bool,
        retry_started: bool,
    ) -> Result<SessionEndOutcome> {
        self.call(move |s| {
            let tx = s.db.transaction()?;
            let ids = {
                let mut stmt = tx.prepare("SELECT d.message_id FROM deliveries d JOIN messages m ON m.id=d.message_id WHERE m.recipient=? AND d.state='offered' AND d.incarnation<=? ORDER BY m.seq")?;
                stmt.query_map(params![json(&agent)?, ended.0], |r| json_col(r, 0))?.collect::<rusqlite::Result<Vec<MessageId>>>()?
            };
            let mut result = SessionEndOutcome::default();
            for id in ids {
                let mut d = delivery(&tx, id)?;
                let DeliveryState::Offered { incarnation } = d.state else { return Err(Error::Transition(d.state)); };
                if !retry_started && (turn_started || d.turn_started) {
                    d.state = DeliveryState::Uncertain { incarnation };
                    result.uncertain.push(id);
                } else if d.attempts >= s.config.max_attempts {
                    d.state = DeliveryState::Held {
                        reason: "maximum delivery attempts reached".into(),
                    };
                } else {
                    d.state = DeliveryState::Queued;
                    result.requeued += 1;
                }
                save_delivery(&tx, id, &d)?;
            }
            tx.commit()?;
            Ok(result)
        }).await
    }

    pub async fn held(&self, agent: Option<AgentId>) -> Result<Vec<HeldMail>> {
        self.call(move |s| {
            let mail = envelopes(&s.db, "SELECT m.* FROM messages m JOIN deliveries d ON m.id=d.message_id WHERE d.state IN ('held','uncertain') AND (?1 IS NULL OR m.recipient=?1) ORDER BY m.seq", [agent.map(|a| json(&a)).transpose()?])?;
            mail.into_iter().map(|envelope| Ok(HeldMail { state: delivery(&s.db, envelope.id)?.state, envelope })).collect()
        }).await
    }

    pub async fn release(&self, id: MessageId) -> Result<AgentId> {
        self.call(move |s| {
            let tx = s.db.transaction()?;
            let mut d = delivery(&tx, id)?;
            require_held(&d)?;
            let to = message(&tx, id)?.to;
            active(&tx, to)?;
            d.state = DeliveryState::Queued;
            d.turn_started = false;
            d.attempts = 0;
            save_delivery(&tx, id, &d)?;
            tx.commit()?;
            Ok(to)
        })
        .await
    }

    pub async fn drop_held(&self, id: MessageId, reason: &str) -> Result<()> {
        let reason = reason.to_owned();
        self.call(move |s| {
            let mut d = delivery(&s.db, id)?;
            require_held(&d)?;
            d.state = DeliveryState::Dead { reason };
            save_delivery(&s.db, id, &d)
        })
        .await
    }

    pub async fn retire(&self, agent: AgentId) -> Result<()> {
        self.call(move |s| {
            let tx = s.db.transaction()?;
            if tx.execute("UPDATE agents SET retired=1 WHERE id=?", [json(&agent)?])? == 0 { return Err(Error::NotFound(agent.to_string())); }
            tx.execute("UPDATE deliveries SET state='dead',turn_started=0,incarnation=NULL,reason='recipient retired' WHERE state IN ('queued','offered','held','uncertain') AND message_id IN (SELECT id FROM messages WHERE recipient=?)", [json(&agent)?])?;
            tx.commit()?;
            Ok(())
        }).await
    }

    pub async fn view(&self, conversation: ConversationId) -> Result<ConversationView> {
        self.call(move |s| view(&s.db, conversation)).await
    }

    pub async fn task(&self, id: TaskId) -> Result<Task> {
        self.call(move |s| task(&s.db, id)).await
    }

    /// Every task in the ledger, oldest first.
    pub async fn tasks(&self) -> Result<Vec<Task>> {
        self.call(|s| {
            Ok(s.db
                .prepare(&format!("SELECT {TASK_COLUMNS} FROM tasks ORDER BY rowid"))?
                .query_map([], task_row)?
                .collect::<rusqlite::Result<_>>()?)
        })
        .await
    }

    pub async fn record_spend(&self, id: TaskId, spend: Budget) -> Result<()> {
        self.call(move |s| {
            let tx = s.db.transaction()?;
            let mut t = task(&tx, id)?;
            charge(&mut t, spend)?;
            save_task(&tx, &t)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn new_epoch(&self, agent: AgentId) -> Result<Epoch> {
        self.call(move |s| {
            active(&s.db, agent)?;
            s.db.execute(
                "UPDATE agents SET epoch=epoch+1 WHERE id=? AND epoch<9223372036854775807",
                [json(&agent)?],
            )
            .and_then(|count| {
                if count == 1 {
                    Ok(())
                } else {
                    Err(rusqlite::Error::InvalidQuery)
                }
            })?;
            Ok(exposure_ref(&s.db, agent)?.epoch)
        })
        .await
    }

    pub async fn exposures(
        &self,
        agent: AgentId,
        epoch: Epoch,
    ) -> Result<Vec<(u64, ExposureSource)>> {
        self.call(move |s| {
            let mut stmt = s.db.prepare(
                "SELECT seq,source_id FROM exposures WHERE agent=? AND epoch=? ORDER BY seq",
            )?;
            Ok(stmt
                .query_map(params![json(&agent)?, epoch.0], |r| {
                    Ok((r.get(0)?, ExposureSource::Message(json_col(r, 1)?)))
                })?
                .collect::<rusqlite::Result<_>>()?)
        })
        .await
    }
}

pub(crate) struct Store {
    pub(crate) db: Connection,
    config: Config,
    actors: HashMap<AgentId, ulid::Ulid>,
    last_prune: i64,
    #[cfg(test)]
    prunes: Vec<(i64, usize)>,
}

impl Store {
    fn open(path: &Path, config: Config) -> Result<Self> {
        if config.max_attempts == 0 {
            return Err(Error::Rejected("max_attempts must be positive".into()));
        }
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_millis(100))?;
        db.execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        let mode: String = db.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        if mode != "wal" {
            return Err(Error::Rejected(
                "mailroom requires a WAL database file".into(),
            ));
        }
        let version: u32 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > 3 {
            return Err(Error::Rejected(format!(
                "unsupported schema version {version}"
            )));
        }
        db.execute_batch(include_str!("schema.sql"))?;
        if version < 2 {
            db.execute_batch(include_str!("schema-v2.sql"))?;
        }
        if version < 3 {
            db.execute_batch(include_str!("schema-v3.sql"))?;
        }
        let last_prune = feed::head(&db)?;
        Ok(Self {
            db,
            config,
            actors: HashMap::new(),
            last_prune,
            #[cfg(test)]
            prunes: Vec::new(),
        })
    }

    fn accept(&mut self, msg: NewMessage) -> Result<(SendOutcome, Envelope)> {
        let tx = self.db.transaction()?;
        let sender = json(&msg.from)?;
        if let Some(id) = tx
            .query_row(
                "SELECT id FROM messages WHERE sender=? AND client_key=?",
                params![sender, msg.client_key.0],
                |r| json_col(r, 0),
            )
            .optional()?
        {
            let stored = message(&tx, id)?;
            let reused =
                || Error::Rejected("client_key was already used for a different message".into());
            if stored.to != msg.to
                || stored.body != msg.body
                || stored.in_reply_to != msg.reply_to
                || stored.act != msg.act
            {
                return Err(reused());
            }
            let incoming_task = match (msg.task, msg.reply_to) {
                (Some(t), _) => Some(t),
                (None, Some(parent)) => message(&tx, parent)?.task,
                (None, None) => None,
            };
            let same_task = match stored.act {
                Act::Request { .. } => {
                    let opened = stored.task.map(|t| task(&tx, t)).transpose()?;
                    incoming_task == opened.and_then(|t| t.parent)
                }
                // The act pins the task, and the first send accepted either no
                // context task or this one.
                Act::Report { task: reported, .. } => incoming_task.is_none_or(|t| t == reported),
                _ => incoming_task == stored.task,
            };
            if !same_task {
                return Err(reused());
            }
            return Ok((SendOutcome::Duplicate { id }, stored));
        }
        // A report's recipient is checked against the task below.
        if !matches!(msg.act, Act::Report { .. }) {
            active(&tx, msg.to)?;
        }
        if let Sender::Agent(id) = msg.from {
            active(&tx, id)?;
            if let Some(limit) = self.config.limits.per_sender {
                let count: u64 = tx.query_row(
                    "SELECT COUNT(*) FROM messages m JOIN deliveries d ON d.message_id=m.id WHERE m.sender=? AND d.state IN ('queued','offered','uncertain','held')",
                    [&sender], |r| r.get(0),
                )?;
                if count >= u64::from(limit.get()) {
                    return Err(Error::MailLimit(format!(
                        "per_sender cap {limit} reached with {count} undelivered messages; wait for delivery before sending more"
                    )));
                }
            }
            if let Some(limit) = self.config.limits.per_pair {
                let count: u64 = tx.query_row(
                    "SELECT COUNT(*) FROM messages m JOIN deliveries d ON d.message_id=m.id WHERE m.sender=? AND m.recipient=? AND d.state IN ('queued','offered','uncertain','held')",
                    params![sender, json(&msg.to)?], |r| r.get(0),
                )?;
                if count >= u64::from(limit.get()) {
                    return Err(Error::MailLimit(format!(
                        "per_pair cap {limit} reached with {count} undelivered messages to {}; wait for delivery before sending more",
                        msg.to
                    )));
                }
            }
        }
        let parent = msg.reply_to.map(|id| message(&tx, id)).transpose()?;
        if let Some(parent) = &parent {
            let involved = match msg.from {
                Sender::Human => true,
                Sender::Agent(a) => parent.to == a || parent.from == Sender::Agent(a),
            };
            if !involved {
                return Err(Error::Rejected("reply must involve sender".into()));
            }
        }
        let hop = match msg.from {
            Sender::Human => 0,
            Sender::Agent(_) => parent
                .as_ref()
                .map_or(Some(1), |p| p.hop.checked_add(1))
                .ok_or_else(|| Error::Rejected("hop overflow".into()))?,
        };
        let current_task = msg.task.or_else(|| parent.as_ref().and_then(|p| p.task));
        let mut env = Envelope {
            id: MessageId::new(),
            from: msg.from.clone(),
            to: msg.to,
            conversation: parent
                .as_ref()
                .map_or_else(ConversationId::new, |p| p.conversation),
            in_reply_to: msg.reply_to,
            hop,
            act: msg.act,
            task: current_task,
            exposure: match msg.from {
                Sender::Human => None,
                Sender::Agent(a) => Some(exposure_ref(&tx, a)?),
            },
            body: msg.body,
            sent_at: OffsetDateTime::now_utc(),
        };
        if env.act != Act::Inform
            && env.body.as_str().len() + json(&env.act)?.len() > MAX_BODY_BYTES
        {
            return Err(Error::Rejected(format!(
                "body and act together exceed {MAX_BODY_BYTES} bytes"
            )));
        }
        let mut new_task = None;
        let mut updated_task = None;
        match &env.act {
            Act::Request {
                deliverable,
                budget,
                stakes,
            } => {
                if deliverable.trim().is_empty() {
                    return Err(Error::Rejected("Request needs a deliverable".into()));
                }
                // `charge` adds zero for free, so a zero budget would let one
                // parent open unlimited children.
                if budget.wall_secs == 0 || budget.messages == 0 || budget.tokens == Some(0) {
                    return Err(Error::Rejected(
                        "Request budget needs nonzero wall_secs, messages and tokens".into(),
                    ));
                }
                let parent = match env.from {
                    Sender::Human => {
                        if current_task.is_some() {
                            return Err(Error::Rejected(
                                "human root Request cannot name a parent".into(),
                            ));
                        }
                        None
                    }
                    Sender::Agent(owner) => {
                        let id = current_task.ok_or_else(|| {
                            Error::Rejected("agent Request needs a parent task".into())
                        })?;
                        let mut t = owned_task(&tx, id, owner)?;
                        charge(&mut t, *budget)?;
                        updated_task = Some(t);
                        Some(id)
                    }
                };
                let id = TaskId::new();
                env.task = Some(id);
                new_task = Some(Task {
                    id,
                    parent,
                    owner: env.to,
                    requested_by: env.from.clone(),
                    request: env.id,
                    deliverable: deliverable.clone(),
                    stakes: *stakes,
                    budget: *budget,
                    spent: Budget {
                        tokens: budget.tokens.map(|_| 0),
                        ..Budget::default()
                    },
                    state: TaskState::Open,
                });
            }
            Act::Report { task: id, outcome } => {
                let Sender::Agent(owner) = env.from else {
                    return Err(Error::Rejected("Report needs a task owner".into()));
                };
                if current_task.is_some_and(|current| current != *id) {
                    return Err(Error::Rejected("Report task mismatch".into()));
                }
                let mut t = owned_task(&tx, *id, owner)?;
                // Agents cannot mail the operator or a retired agent, so those
                // reports go to the assignee itself.
                let report_to = match t.requested_by {
                    Sender::Agent(requester) if !retired(&tx, requester)? => requester,
                    Sender::Agent(requester) if env.to == requester => {
                        return Err(Error::Rejected(format!(
                            "requester agent:{requester} is retired; address the report to yourself to close the task"
                        )));
                    }
                    Sender::Agent(_) | Sender::Human => owner,
                };
                if env.to != report_to {
                    return Err(Error::Rejected(format!(
                        "report must be addressed to agent:{report_to}"
                    )));
                }
                validate_outcome(outcome)?;
                t.state = match outcome {
                    Outcome::Partial { .. } | Outcome::Infeasible { .. } => {
                        TaskState::Verifying { report: env.id }
                    }
                    _ => TaskState::Closed {
                        outcome: outcome.clone(),
                        success: matches!(outcome, Outcome::Done { .. }),
                    },
                };
                env.task = Some(*id);
                updated_task = Some(t);
            }
            Act::Propose { .. } | Act::Decide { .. } => {
                return Err(Error::Rejected(
                    "decision rights are not implemented in this slice".into(),
                ));
            }
            Act::Inform => {
                if let Some(id) = current_task {
                    let t = task(&tx, id)?;
                    if env.from != Sender::Human
                        && env.from != Sender::Agent(t.owner)
                        && env.from != t.requested_by
                    {
                        return Err(Error::Rejected("sender is unrelated to task".into()));
                    }
                }
            }
        }
        let conversation = view(&tx, env.conversation)?;
        let mut held_reason = None;
        for interceptor in &self.config.interceptors {
            match interceptor.inspect(&env, &conversation) {
                Verdict::Deliver => {}
                Verdict::Hold { reason } => {
                    if held_reason.is_none() {
                        held_reason = Some(reason);
                    }
                }
                Verdict::Reject { reason } => return Err(Error::Rejected(reason)),
            }
        }
        tx.execute("INSERT INTO messages(id,sender,client_key,recipient,conversation,in_reply_to,hop,act,task,exposure,body,sent_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", params![json(&env.id)?, sender, msg.client_key.0, json(&env.to)?, json(&env.conversation)?, env.in_reply_to.map(|v| json(&v)).transpose()?, env.hop, json(&env.act)?, env.task.map(|v| json(&v)).transpose()?, env.exposure.map(|v| json(&v)).transpose()?, env.body.as_str(), json(&env.sent_at)?])?;
        let msg_seq = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO deliveries(message_id,state,reason,msg_seq) VALUES (?,?,?,?)",
            params![
                json(&env.id)?,
                if held_reason.is_some() {
                    "held"
                } else {
                    "queued"
                },
                held_reason,
                msg_seq
            ],
        )?;
        if let Some(t) = updated_task {
            save_task(&tx, &t)?;
        }
        if let Some(t) = new_task {
            tx.execute("INSERT INTO tasks(id,parent,owner,requested_by,request_msg,deliverable,stakes,budget,spent,state) VALUES (?,?,?,?,?,?,?,?,?,?)", params![json(&t.id)?, t.parent.map(|id| json(&id)).transpose()?, json(&t.owner)?, json(&t.requested_by)?, json(&t.request)?, t.deliverable, json(&t.stakes)?, json(&t.budget)?, json(&t.spent)?, json(&t.state)?])?;
        }
        let outcome = match held_reason {
            Some(reason) => SendOutcome::Held { id: env.id, reason },
            None => SendOutcome::Queued { id: env.id },
        };
        tx.commit()?;
        Ok((outcome, env))
    }
}

fn json(value: &impl Serialize) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}
fn json_col<T: DeserializeOwned>(row: &Row<'_>, index: usize) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    serde_json::from_str(&text).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(e))
    })
}
fn optional_json_col<T: DeserializeOwned>(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<T>> {
    let text: Option<String> = row.get(index)?;
    text.map(|s| {
        serde_json::from_str(&s).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })
    })
    .transpose()
}
fn envelope_row(r: &Row<'_>) -> rusqlite::Result<Envelope> {
    let body: String = r.get(11)?;
    Ok(Envelope {
        id: json_col(r, 1)?,
        from: json_col(r, 2)?,
        to: json_col(r, 4)?,
        conversation: json_col(r, 5)?,
        in_reply_to: optional_json_col(r, 6)?,
        hop: r.get(7)?,
        act: json_col(r, 8)?,
        task: optional_json_col(r, 9)?,
        exposure: optional_json_col(r, 10)?,
        body: Body::new(body).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(e))
        })?,
        sent_at: json_col(r, 12)?,
    })
}
fn envelopes(db: &Connection, sql: &str, params: impl rusqlite::Params) -> Result<Vec<Envelope>> {
    Ok(db
        .prepare(sql)?
        .query_map(params, envelope_row)?
        .collect::<rusqlite::Result<_>>()?)
}
fn message(db: &Connection, id: MessageId) -> Result<Envelope> {
    db.query_row(
        "SELECT * FROM messages WHERE id=?",
        [json(&id)?],
        envelope_row,
    )
    .optional()?
    .ok_or_else(|| Error::NotFound(id.to_string()))
}
fn retired(db: &Connection, id: AgentId) -> Result<bool> {
    db.query_row("SELECT retired FROM agents WHERE id=?", [json(&id)?], |r| {
        r.get::<_, bool>(0)
    })
    .optional()?
    .ok_or_else(|| Error::NotFound(id.to_string()))
}
fn active(db: &Connection, id: AgentId) -> Result<()> {
    if retired(db, id)? {
        return Err(Error::Rejected(format!("agent {id} is retired")));
    }
    Ok(())
}
fn delivery(db: &Connection, id: MessageId) -> Result<Delivery> {
    let (state, incarnation, reason, attempts, turn_started): (String, Option<u64>, Option<String>, u32, bool) = db.query_row("SELECT state,incarnation,reason,attempts,turn_started FROM deliveries WHERE message_id=?", [json(&id)?], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?.ok_or_else(|| Error::NotFound(id.to_string()))?;
    let state = match state.as_str() {
        "queued" => DeliveryState::Queued,
        "offered" => DeliveryState::Offered {
            incarnation: Incarnation(incarnation.ok_or(rusqlite::Error::InvalidQuery)?),
        },
        "consumed" => DeliveryState::Consumed,
        "uncertain" => DeliveryState::Uncertain {
            incarnation: Incarnation(incarnation.ok_or(rusqlite::Error::InvalidQuery)?),
        },
        "held" => DeliveryState::Held {
            reason: reason.ok_or(rusqlite::Error::InvalidQuery)?,
        },
        "dead" => DeliveryState::Dead {
            reason: reason.ok_or(rusqlite::Error::InvalidQuery)?,
        },
        _ => return Err(Error::Store(rusqlite::Error::InvalidQuery)),
    };
    Ok(Delivery {
        state,
        attempts,
        turn_started,
    })
}
fn save_delivery(db: &Connection, id: MessageId, d: &Delivery) -> Result<()> {
    let (state, incarnation, reason) = match &d.state {
        DeliveryState::Queued => ("queued", None, None),
        DeliveryState::Offered { incarnation } => ("offered", Some(incarnation.0), None),
        DeliveryState::Consumed => ("consumed", None, None),
        DeliveryState::Uncertain { incarnation } => ("uncertain", Some(incarnation.0), None),
        DeliveryState::Held { reason } => ("held", None, Some(reason)),
        DeliveryState::Dead { reason } => ("dead", None, Some(reason)),
    };
    db.execute("UPDATE deliveries SET state=?,incarnation=?,reason=?,attempts=?,turn_started=? WHERE message_id=?", params![state,incarnation,reason,d.attempts,d.turn_started,json(&id)?])?;
    Ok(())
}
fn require_offered(d: &Delivery, incarnation: Incarnation) -> Result<()> {
    if d.state == (DeliveryState::Offered { incarnation }) {
        Ok(())
    } else {
        Err(Error::Transition(d.state.clone()))
    }
}
fn require_held(d: &Delivery) -> Result<()> {
    if matches!(
        d.state,
        DeliveryState::Held { .. } | DeliveryState::Uncertain { .. }
    ) {
        Ok(())
    } else {
        Err(Error::Transition(d.state.clone()))
    }
}
fn unique(ids: &[MessageId]) -> Result<Vec<MessageId>> {
    if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
        return Err(Error::Rejected("duplicate IDs in batch".into()));
    }
    Ok(ids.to_vec())
}
fn view(db: &Connection, conversation: ConversationId) -> Result<ConversationView> {
    let mut recent = envelopes(
        db,
        "SELECT * FROM messages WHERE conversation=? ORDER BY seq DESC LIMIT 64",
        [json(&conversation)?],
    )?;
    recent.reverse();
    let mut pair_exchanges = HashMap::new();
    let mut stmt = db.prepare(
        "SELECT sender, recipient, COUNT(*) FROM messages WHERE conversation=? GROUP BY sender, recipient",
    )?;
    let rows = stmt.query_map([json(&conversation)?], |r| {
        Ok((
            json_col::<Sender>(r, 0)?,
            json_col::<AgentId>(r, 1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;
    for row in rows {
        if let (Sender::Agent(from), to, count) = row? {
            pair_exchanges.insert((from, to), u32::try_from(count).unwrap_or(u32::MAX));
        }
    }
    Ok(ConversationView {
        conversation,
        recent,
        pair_exchanges,
    })
}
fn exposure_ref(db: &Connection, agent: AgentId) -> Result<ExposureRef> {
    let epoch = db.query_row(
        "SELECT epoch FROM agents WHERE id=?",
        [json(&agent)?],
        |r| r.get(0),
    )?;
    let seq = db.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM exposures WHERE agent=? AND epoch=?",
        params![json(&agent)?, epoch],
        |r| r.get(0),
    )?;
    Ok(ExposureRef {
        sender: agent,
        epoch: Epoch(epoch),
        seq,
    })
}
const TASK_COLUMNS: &str =
    "id,parent,owner,requested_by,request_msg,deliverable,stakes,budget,spent,state";
fn task_row(r: &Row<'_>) -> rusqlite::Result<Task> {
    Ok(Task {
        id: json_col(r, 0)?,
        parent: optional_json_col(r, 1)?,
        owner: json_col(r, 2)?,
        requested_by: json_col(r, 3)?,
        request: json_col(r, 4)?,
        deliverable: r.get(5)?,
        stakes: json_col(r, 6)?,
        budget: json_col(r, 7)?,
        spent: json_col(r, 8)?,
        state: json_col(r, 9)?,
    })
}
fn task(db: &Connection, id: TaskId) -> Result<Task> {
    db.query_row(
        &format!("SELECT {TASK_COLUMNS} FROM tasks WHERE id=?"),
        [json(&id)?],
        task_row,
    )
    .optional()?
    .ok_or_else(|| Error::NotFound(id.to_string()))
}
fn owned_task(db: &Connection, id: TaskId, owner: AgentId) -> Result<Task> {
    let t = task(db, id)?;
    if t.owner != owner || t.state != TaskState::Open {
        return Err(Error::Rejected(
            "task must be open and owned by sender".into(),
        ));
    }
    Ok(t)
}
fn charge(t: &mut Task, spend: Budget) -> Result<()> {
    if t.state != TaskState::Open {
        return Err(Error::Rejected("task is not open".into()));
    }
    t.spent = t.spent.add(spend, t.budget).ok_or(Error::BudgetExceeded)?;
    Ok(())
}
fn save_task(db: &Connection, t: &Task) -> Result<()> {
    db.execute(
        "UPDATE tasks SET spent=?,state=? WHERE id=?",
        params![json(&t.spent)?, json(&t.state)?, json(&t.id)?],
    )?;
    Ok(())
}
fn validate_outcome(outcome: &Outcome) -> Result<()> {
    let valid = match outcome {
        Outcome::Partial {
            not_possible,
            evidence,
            ..
        } => !not_possible.trim().is_empty() && valid_evidence(evidence),
        Outcome::Infeasible { evidence } => valid_evidence(evidence),
        Outcome::Failed { tried } => !tried.trim().is_empty(),
        Outcome::Blocked { on } => !on.trim().is_empty(),
        Outcome::Done { artifacts } => artifacts.iter().all(|a| !a.locator.trim().is_empty()),
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Rejected(
            "outcome needs nonempty evidence or explanation".into(),
        ))
    }
}
fn valid_evidence(e: &Evidence) -> bool {
    !e.tried.trim().is_empty() && !e.obstacle.trim().is_empty()
}

#[cfg(test)]
mod actor_tests {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    use super::*;

    #[tokio::test]
    async fn cancelled_claim_releases_its_lease_after_transaction() {
        let root = tempfile::tempdir().expect("tempdir");
        let room = Mailroom::open(root.path().join("mail.db"), Config::default())
            .await
            .expect("mailroom");
        let agent = room
            .register(AgentName::parse("worker").expect("name"))
            .await
            .expect("register");
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = mpsc::channel();
        room.jobs
            .send(Box::new(move |_| {
                entered.send(()).expect("worker entered");
                blocked.recv().expect("release worker");
            }))
            .expect("submit blocker");
        ready.await.expect("worker ready");
        let mut claim = Box::pin(room.claim_actor(agent, "claude".into()));
        poll_fn(|cx| {
            assert!(claim.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(claim);
        release.send(()).expect("release");
        let (lease, incarnation, _) = room
            .claim_actor(agent, "claude".into())
            .await
            .expect("cancelled claim must release");
        assert_eq!(incarnation, Incarnation(2));
        assert!(room.claim_actor(agent, "claude".into()).await.is_err());
        drop(lease);
        let (lease, incarnation, _) = room
            .claim_actor(agent, "claude".into())
            .await
            .expect("failed claimant cannot release another owner");
        assert_eq!(incarnation, Incarnation(3));
        drop(lease);
        room.close().await.expect("close");
    }
}
