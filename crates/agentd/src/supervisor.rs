use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, broadcast, mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::adapter::*;
use crate::adapters::AdapterSet;
use crate::mailroom::{Mailroom, NewMessage, SendOutcome};
use crate::model::*;

pub use crate::adapter::TurnOutcome;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AgentSpec {
    pub name: AgentName,
    pub kind: String,
    pub cwd: PathBuf,
    pub model: Option<String>,
    pub permissions: PermissionPolicy,
    pub idle_after: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Dormant,
    Starting,
    Idle,
    Busy,
    Suspending,
    Ended,
    Crashed { attempt: u32, retry_at: Instant },
    Failed { last: String },
    NeedsRecovery { reason: String },
}

#[derive(Clone, Debug)]
pub enum Event {
    PhaseChanged {
        agent: AgentId,
        incarnation: Incarnation,
        phase: Phase,
    },
    Offered {
        agent: AgentId,
        ids: Vec<MessageId>,
    },
    Started {
        agent: AgentId,
        ids: Vec<MessageId>,
    },
    Consumed {
        agent: AgentId,
        ids: Vec<MessageId>,
    },
    Activity {
        agent: AgentId,
        text: String,
    },
    PermissionPending(PermissionRequest),
    PermissionAnswered {
        agent: AgentId,
        id: PermissionId,
    },
    TurnEnded {
        agent: AgentId,
        outcome: TurnOutcome,
    },
}

#[derive(Clone, Debug)]
pub struct Config {
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub crash_window: Duration,
    pub crash_limit: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            crash_window: Duration::from_secs(600),
            crash_limit: 5,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Mailroom(#[from] crate::mailroom::Error),
    #[error("{0}")]
    Rejected(String),
    #[error("stale permission or incarnation")]
    StalePermission,
    #[error("actor stopped")]
    Closed,
}
type Result<T> = std::result::Result<T, Error>;
type Reply = oneshot::Sender<Result<()>>;
enum Command {
    Nudge,
    Cancel(Reply),
    Pending(oneshot::Sender<Vec<PermissionRequest>>),
    Answer {
        id: PermissionId,
        incarnation: Incarnation,
        answer: PermissionAnswer,
        reply: Reply,
    },
    Stop(Reply),
}

#[derive(Clone)]
pub struct AgentHandle {
    pub id: AgentId,
    commands: mpsc::Sender<Command>,
    phase: watch::Receiver<Phase>,
}
impl AgentHandle {
    pub fn phase(&self) -> Phase {
        self.phase.borrow().clone()
    }
    pub fn phases(&self) -> watch::Receiver<Phase> {
        self.phase.clone()
    }
    pub async fn cancel(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Cancel(tx))
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }
    pub async fn pending_permissions(&self) -> Result<Vec<PermissionRequest>> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Pending(tx))
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }
    pub async fn answer_permission(
        &self,
        id: PermissionId,
        incarnation: Incarnation,
        answer: PermissionAnswer,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Answer {
                id,
                incarnation,
                answer,
                reply: tx,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }
    pub async fn stop(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Stop(tx))
            .await
            .map_err(|_| Error::Closed)?;
        let result = rx.await.map_err(|_| Error::Closed)?;
        self.commands.closed().await;
        result
    }
}

pub struct Broker {
    mailroom: Arc<Mailroom>,
    kinds: HashMap<String, KindConfig>,
    agents: Mutex<HashMap<AgentId, AgentHandle>>,
    events: broadcast::Sender<Event>,
    config: Config,
    mcp: Option<Arc<crate::surfaces::Bindings>>,
}
impl Broker {
    pub fn new(mailroom: Arc<Mailroom>, kinds: Vec<KindConfig>, config: Config) -> Result<Self> {
        if config.crash_limit == 0
            || config.backoff_initial.is_zero()
            || config.backoff_max < config.backoff_initial
        {
            return Err(Error::Rejected("invalid crash policy".into()));
        }
        let (events, _) = broadcast::channel(1024);
        let mut by_kind = HashMap::new();
        for kind in kinds {
            if (matches!(kind.wire, Wire::Acp) && kind.command.is_empty())
                || kind.mode.is_empty()
                || kind
                    .workspace_write_mode
                    .as_ref()
                    .is_some_and(String::is_empty)
                || kind.full_access_mode.as_ref().is_some_and(String::is_empty)
                || kind.request_timeout.is_zero()
                || kind.cancel_timeout.is_zero()
            {
                return Err(Error::Rejected(
                    "kind needs command, mode and timeout".into(),
                ));
            }
            if by_kind.insert(kind.name.clone(), kind).is_some() {
                return Err(Error::Rejected("duplicate kind".into()));
            }
        }
        Ok(Self {
            mailroom,
            kinds: by_kind,
            agents: Mutex::new(HashMap::new()),
            events,
            config,
            mcp: None,
        })
    }
    pub fn with_mcp(mut self, bindings: Arc<crate::surfaces::Bindings>) -> Self {
        self.mcp = Some(bindings);
        self
    }
    pub async fn handle(&self, id: AgentId) -> Option<AgentHandle> {
        self.agents.lock().await.get(&id).cloned()
    }
    pub(crate) async fn nudge(&self, id: AgentId) {
        if let Some(handle) = self.agents.lock().await.get(&id) {
            match handle.commands.try_send(Command::Nudge) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(agent=%id, "mail queued for stopped actor")
                }
            }
        }
    }
    /// The receiver is display-only and may lag. SQLite remains delivery authority.
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }
    pub async fn spawn(&self, spec: AgentSpec) -> Result<AgentHandle> {
        let kind = self
            .kinds
            .get(&spec.kind)
            .ok_or_else(|| Error::Rejected("unknown kind".into()))?
            .clone();
        let model_agent = matches!(kind.wire, Wire::Infinity { .. });
        let mut spec = spec;
        if spec.model.is_none() {
            spec.model = kind.model.clone();
        }
        let mut agents = self.agents.lock().await;
        let id = self.mailroom.register(spec.name.clone()).await?;
        if agents.get(&id).is_some_and(|h| !h.commands.is_closed()) {
            return Err(Error::Rejected("agent already supervised".into()));
        }
        let (lease, incarnation, resume) = self.mailroom.claim_actor(id, spec.kind.clone()).await?;
        self.mailroom.save_spec(id, spec.clone()).await?;
        let (tx, commands) = mpsc::channel(64);
        let (phase_tx, phase) = watch::channel(Phase::Dormant);
        let handle = AgentHandle {
            id,
            commands: tx,
            phase,
        };
        let actor = AgentActor {
            _lease: lease,
            mcp: self.mcp.clone(),
            id,
            idle_after: if model_agent {
                None
            } else {
                spec.idle_after.or(kind.idle_after)
            },
            model_agent,
            spec,
            adapter: AdapterSet::build(kind),
            mailroom: Arc::clone(&self.mailroom),
            events: self.events.clone(),
            config: self.config.clone(),
            commands,
            phase_tx,
            phase: Phase::Dormant,
            incarnation,
            resume,
            session: None,
            cancelling: false,
            offered: Vec::new(),
            pending: HashMap::new(),
            crashes: VecDeque::new(),
            unavailable_attempts: 0,
            interrupt_timeouts: 0,
            idle_since: Instant::now(),
        };
        tokio::spawn(actor.run());
        agents.insert(id, handle.clone());
        Ok(handle)
    }
    /// Trusted in-process API. External callers are authenticated by surfaces.
    pub async fn send(&self, message: NewMessage) -> Result<(SendOutcome, Envelope)> {
        let id = message.to;
        let outcome = self.mailroom.accept(message).await?;
        self.nudge(id).await;
        Ok(outcome)
    }
    pub async fn shutdown(&self) -> Result<()> {
        let handles = self
            .agents
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for handle in handles {
            if !handle.commands.is_closed() {
                handle.stop().await?;
            }
        }
        Ok(())
    }
}

struct AgentActor {
    mcp: Option<Arc<crate::surfaces::Bindings>>,
    _lease: crate::mailroom::ActorLease,
    id: AgentId,
    spec: AgentSpec,
    adapter: Arc<dyn Adapter>,
    mailroom: Arc<Mailroom>,
    config: Config,
    idle_after: Option<Duration>,
    model_agent: bool,
    events: broadcast::Sender<Event>,
    commands: mpsc::Receiver<Command>,
    phase_tx: watch::Sender<Phase>,
    phase: Phase,
    incarnation: Incarnation,
    resume: Option<ResumeToken>,
    session: Option<Box<dyn Session>>,
    cancelling: bool,
    offered: Vec<MessageId>,
    pending: HashMap<PermissionId, (PermissionRequest, Option<Instant>)>,
    crashes: VecDeque<Instant>,
    unavailable_attempts: u32,
    interrupt_timeouts: usize,
    idle_since: Instant,
}

impl AgentActor {
    fn emit(&self, event: Event) {
        if self.events.send(event).is_err() {
            tracing::trace!("no agentd event subscribers");
        }
    }
    fn phase(&mut self, phase: Phase) {
        self.phase = phase.clone();
        self.phase_tx.send_replace(phase.clone());
        self.emit(Event::PhaseChanged {
            agent: self.id,
            incarnation: self.incarnation,
            phase,
        });
    }
    fn session(&mut self) -> Result<&mut Box<dyn Session>> {
        self.session.as_mut().ok_or(Error::Closed)
    }
    async fn run(mut self) {
        let result = self.drive().await;
        if let Err(error) = &result {
            tracing::error!(agent=%self.id,%error,"agent actor failed");
        }
        if let Some(mcp) = &self.mcp {
            mcp.revoke(self.id).await;
        }
        if let Some(session) = self.session.take() {
            match session.suspend().await {
                Ok(token) if self.model_agent => {
                    if let Err(error) = self.mailroom.save_resume(self.id, token).await {
                        tracing::error!(%error,"session resume save failed");
                    }
                }
                Ok(_) => {}
                Err(error) => tracing::error!(%error,"session shutdown failed"),
            }
        }
        if let Err(error) = self
            .mailroom
            .settle_session(self.id, self.incarnation, false, self.model_agent)
            .await
        {
            tracing::error!(%error,"session cleanup failed");
        }
        if let Err(error) = result {
            self.phase(Phase::Failed {
                last: error.to_string(),
            });
        }
    }
    async fn drive(&mut self) -> Result<()> {
        self.mailroom
            .settle_session(
                self.id,
                Incarnation(self.incarnation.0 - 1),
                false,
                self.model_agent,
            )
            .await?;
        self.start().await?;
        loop {
            self.pump().await?;
            let deadline = self.deadline();
            tokio::select! {
                command = self.commands.recv() => match command {
                    None => return Ok(()),
                    Some(Command::Stop(reply)) => {
                        let result = self.stop_session().await;
                        self.phase(Phase::Ended);
                        respond(reply, result);
                        return Ok(());
                    }
                    Some(Command::Nudge) => {},
                    Some(Command::Pending(reply)) => { if reply.send(self.pending.values().map(|(r,_)| r.clone()).collect()).is_err() { tracing::trace!("pending caller dropped"); } },
                    Some(Command::Cancel(reply)) => {
                        let result = self.cancel().await;
                        if let Err(e) = &result { self.end(e.to_string()).await?; }
                        respond(reply,result);
                    },
                    Some(Command::Answer { id, incarnation, answer, reply }) => {
                        let result = self.answer(id,incarnation,answer).await;
                        respond(reply,result);
                    },
                },
                event = async { match &mut self.session { Some(s) => s.next_event().await, None => std::future::pending().await } } => {
                    self.on_event(event.unwrap_or(SessionEvent::Ended { reason: "session event stream closed".into() })).await?;
                },
                () = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending().await } } => self.timer().await?,
            }
        }
    }
    async fn start(&mut self) -> Result<()> {
        self.phase(Phase::Starting);
        let mcp_server = match &self.mcp {
            Some(mcp) => Some(
                mcp.mint(self.id, self.incarnation)
                    .await
                    .map_err(|e| Error::Rejected(e.to_string()))?,
            ),
            None => None,
        };
        match self
            .adapter
            .open(OpenRequest {
                agent: self.id,
                spec: &self.spec,
                resume: self.resume.as_ref(),
                incarnation: self.incarnation,
                mcp_server,
            })
            .await
        {
            Ok(mut session) => {
                let busy = session.busy();
                if self.cancelling {
                    if busy {
                        // Session deadlines die with the socket. Reissue cancellation
                        // so a busy replay cannot strand recovery without a deadline.
                        session
                            .cancel()
                            .await
                            .map_err(|e| Error::Rejected(e.to_string()))?;
                    } else {
                        self.cancelling = false;
                    }
                }
                let token = session.resume_token();
                self.session = Some(session);
                self.mailroom.save_resume(self.id, token.clone()).await?;
                self.resume = Some(token);
                self.idle_since = Instant::now();
                self.phase(if busy { Phase::Busy } else { Phase::Idle });
            }
            Err(error @ (AdapterError::ResumeFailed(_) | AdapterError::ResumeUnsupported)) => self
                .phase(Phase::NeedsRecovery {
                    reason: error.to_string(),
                }),
            Err(AdapterError::Unavailable(reason)) if self.model_agent => {
                tracing::warn!(agent=%self.id, %reason, "waiting for Infinity daemon");
                self.unavailable_attempts = self.unavailable_attempts.saturating_add(1);
                self.retry(self.unavailable_attempts);
            }
            Err(error) => self.crash(error.to_string()),
        }
        if self.session.is_none()
            && let Some(mcp) = &self.mcp
        {
            mcp.revoke(self.id).await;
        }
        Ok(())
    }
    async fn reopen(&mut self) -> Result<()> {
        let (incarnation, resume) = self
            .mailroom
            .next_incarnation(self.id, self.spec.kind.clone())
            .await?;
        self.incarnation = incarnation;
        self.resume = resume;
        self.start().await
    }
    async fn pump(&mut self) -> Result<()> {
        let busy_delivery = self.phase == Phase::Busy
            && self.pending.is_empty()
            && self.session.as_ref().is_some_and(|s| s.accepts_busy_mail());
        if !matches!(self.phase, Phase::Idle | Phase::Dormant) && !busy_delivery {
            return Ok(());
        }
        let mail = self.mailroom.pending_for(self.id).await?;
        if mail.is_empty() {
            return Ok(());
        }
        if self.phase == Phase::Dormant {
            self.reopen().await?;
            if self.phase != Phase::Idle {
                return Ok(());
            }
        }
        let ids = mail.iter().map(Envelope::id).collect::<Vec<_>>();
        let outcome = self.mailroom.mark_offered(&ids, self.incarnation).await?;
        let mut items = Vec::new();
        for envelope in mail {
            if !outcome.offered.contains(&envelope.id()) {
                continue;
            }
            items.push(match envelope.class() {
                DeliveryClass::UserTurn => TurnItem::User(envelope),
                DeliveryClass::PeerEvent => {
                    let Sender::Agent(from) = envelope.from() else {
                        return Err(Error::Rejected("invalid delivery class".into()));
                    };
                    let (name, kind) = self.mailroom.sender_label(*from).await?;
                    let rendered = render_peer(&envelope, &name, &kind);
                    TurnItem::Peer { envelope, rendered }
                }
            });
        }
        if items.is_empty() {
            return Ok(());
        }
        self.offered.extend(&outcome.offered);
        self.emit(Event::Offered {
            agent: self.id,
            ids: outcome.offered,
        });
        self.phase(Phase::Busy);
        if let Err(error) = self.session()?.submit(Turn { items }).await {
            self.end(error.to_string()).await?;
        }
        Ok(())
    }
    async fn on_event(&mut self, event: SessionEvent) -> Result<()> {
        match event {
            SessionEvent::TurnStarted { ids } => {
                self.validate_ids(&ids)?;
                self.mailroom.mark_started(&ids, self.incarnation).await?;
                self.emit(Event::Started {
                    agent: self.id,
                    ids,
                });
            }
            SessionEvent::Committed { ids } => {
                self.validate_ids(&ids)?;
                self.unavailable_attempts = 0;
                self.interrupt_timeouts = 0;
                self.mailroom.mark_consumed(&ids, self.incarnation).await?;
                self.offered.retain(|id| !ids.contains(id));
                self.emit(Event::Consumed {
                    agent: self.id,
                    ids,
                });
            }
            SessionEvent::CancelComplete => {
                self.unavailable_attempts = 0;
                self.interrupt_timeouts = 0;
                self.cancelling = false;
                self.clear_pending();
                self.emit(Event::TurnEnded {
                    agent: self.id,
                    outcome: TurnOutcome::Cancelled,
                });
                self.phase(Phase::Busy);
            }
            SessionEvent::TurnEnded { outcome } => {
                if !self.offered.is_empty() {
                    self.end("turn ended without receipt".into()).await?;
                } else {
                    self.unavailable_attempts = 0;
                    self.interrupt_timeouts = 0;
                    self.cancelling = false;
                    self.clear_pending();
                    self.emit(Event::TurnEnded {
                        agent: self.id,
                        outcome,
                    });
                    self.idle_since = Instant::now();
                    self.phase(Phase::Idle);
                }
            }
            SessionEvent::Activity(text) => self.emit(Event::Activity {
                agent: self.id,
                text,
            }),
            SessionEvent::PermissionRequested(request) => {
                if self.model_agent && self.phase == Phase::Idle {
                    self.phase(Phase::Busy);
                }
                self.permission(request).await?;
            }
            SessionEvent::ResumeUpdated => {
                if let Some(session) = &self.session {
                    let token = session.resume_token();
                    self.mailroom.save_resume(self.id, token.clone()).await?;
                    self.resume = Some(token);
                }
            }
            SessionEvent::CancelTimedOut => {
                self.stop_session().await?;
                self.unavailable_attempts = self.unavailable_attempts.saturating_add(1);
                self.interrupt_timeouts = self.interrupt_timeouts.saturating_add(1);
                if self.interrupt_timeouts >= self.config.crash_limit {
                    self.phase(Phase::NeedsRecovery {
                        reason: "Infinity interrupt deadline repeatedly exceeded without progress"
                            .into(),
                    });
                } else {
                    self.retry(self.unavailable_attempts);
                }
            }
            SessionEvent::Ended { reason } => self.end(reason).await?,
            SessionEvent::ResumeFailed { reason } => {
                self.stop_session().await?;
                self.phase(Phase::NeedsRecovery { reason });
            }
            SessionEvent::Unavailable { reason } => {
                tracing::warn!(agent=%self.id, %reason, "waiting for session recovery");
                self.stop_session().await?;
                self.unavailable_attempts = self.unavailable_attempts.saturating_add(1);
                self.retry(self.unavailable_attempts);
            }
        }
        Ok(())
    }
    fn validate_ids(&self, ids: &[MessageId]) -> Result<()> {
        if ids.iter().all(|id| self.offered.contains(id)) {
            Ok(())
        } else {
            Err(Error::Rejected("adapter named unoffered mail".into()))
        }
    }
    async fn permission(&mut self, request: PermissionRequest) -> Result<()> {
        if request.agent != self.id
            || request.incarnation != self.incarnation
            || self.phase != Phase::Busy
        {
            return Err(Error::StalePermission);
        }
        let decision = self
            .spec
            .permissions
            .rules
            .iter()
            .find(|r| r.tool == "*" || r.tool == request.tool)
            .map_or(RuleDecision::Ask, |r| r.decision);
        let effect = match decision {
            RuleDecision::Allow => Some(PermissionEffect::AllowOnce),
            RuleDecision::Deny => Some(PermissionEffect::RejectOnce),
            RuleDecision::Ask => None,
        };
        if let Some(option) =
            effect.and_then(|effect| request.options.iter().find(|o| o.effect == effect))
        {
            if let Err(error) = self
                .session()?
                .answer_permission(request.id, &option.key)
                .await
            {
                self.end(error.to_string()).await?;
            }
        } else {
            let deadline = self.spec.permissions.timeout.map(|d| Instant::now() + d);
            self.pending.insert(request.id, (request.clone(), deadline));
            self.emit(Event::PermissionPending(request));
        }
        Ok(())
    }
    async fn answer(
        &mut self,
        id: PermissionId,
        incarnation: Incarnation,
        answer: PermissionAnswer,
    ) -> Result<()> {
        let (request, _) = self.pending.get(&id).ok_or(Error::StalePermission)?;
        if request.incarnation != incarnation || incarnation != self.incarnation {
            return Err(Error::StalePermission);
        }
        if let PermissionAnswer::Choose(key) = &answer
            && !request.options.iter().any(|o| o.key == *key)
        {
            return Err(Error::Rejected("invalid permission option".into()));
        }
        let result = match answer {
            PermissionAnswer::Choose(key) => self.session()?.answer_permission(id, &key).await,
            PermissionAnswer::CancelTurn => {
                let result = self.cancel().await;
                if let Err(error) = &result {
                    self.end(error.to_string()).await?;
                }
                return result;
            }
        };
        if let Err(error) = result {
            if matches!(error, AdapterError::StaleChoice) {
                self.pending.remove(&id);
                self.emit(Event::PermissionAnswered { agent: self.id, id });
                return Err(Error::StalePermission);
            }
            self.end(error.to_string()).await?;
            return Err(Error::Closed);
        }
        self.pending.remove(&id);
        self.emit(Event::PermissionAnswered { agent: self.id, id });
        Ok(())
    }
    async fn cancel(&mut self) -> Result<()> {
        if self.model_agent {
            self.cancelling = true;
        }
        if let Some(session) = &mut self.session {
            session
                .cancel()
                .await
                .map_err(|e| Error::Rejected(e.to_string()))?;
        }
        if self.model_agent {
            for (_, deadline) in self.pending.values_mut() {
                *deadline = None;
            }
        } else {
            self.clear_pending();
        }
        Ok(())
    }
    fn clear_pending(&mut self) {
        for (id, _) in self.pending.drain().collect::<Vec<_>>() {
            self.emit(Event::PermissionAnswered { agent: self.id, id });
        }
    }
    async fn stop_session(&mut self) -> Result<()> {
        if let Some(mcp) = &self.mcp {
            mcp.revoke(self.id).await;
        }
        if let Some(session) = self.session.take() {
            let token = session
                .suspend()
                .await
                .map_err(|e| Error::Rejected(e.to_string()))?;
            if self.model_agent {
                self.mailroom.save_resume(self.id, token.clone()).await?;
                self.resume = Some(token);
            }
        }
        self.mailroom
            .settle_session(self.id, self.incarnation, false, self.model_agent)
            .await?;
        self.offered.clear();
        self.clear_pending();
        Ok(())
    }
    async fn end(&mut self, reason: String) -> Result<()> {
        let idle = self.phase == Phase::Idle;
        self.phase(Phase::Ended);
        self.stop_session().await?;
        if idle && !self.model_agent {
            self.phase(Phase::Dormant);
        } else {
            self.crash(reason);
        }
        Ok(())
    }
    fn crash(&mut self, reason: String) {
        tracing::warn!(agent=%self.id, %reason, "agent session ended");
        let now = Instant::now();
        while self
            .crashes
            .front()
            .is_some_and(|t| now.duration_since(*t) >= self.config.crash_window)
        {
            self.crashes.pop_front();
        }
        self.crashes.push_back(now);
        if self.crashes.len() >= self.config.crash_limit {
            self.phase(Phase::Failed { last: reason });
        } else {
            self.retry(self.crashes.len() as u32);
        }
    }
    fn retry(&mut self, attempt: u32) {
        let delay = self
            .config
            .backoff_initial
            .saturating_mul(1u32 << attempt.saturating_sub(1).min(31))
            .min(self.config.backoff_max)
            .min(if self.model_agent {
                Duration::from_secs(60)
            } else {
                self.config.backoff_max
            });
        self.phase(Phase::Crashed {
            attempt,
            retry_at: Instant::now() + delay,
        });
    }
    fn deadline(&self) -> Option<Instant> {
        let mut deadline = match self.phase {
            Phase::Crashed { retry_at, .. } => Some(retry_at),
            Phase::Idle if self.session.as_ref().is_some_and(|s| s.resumable()) => {
                self.idle_after.map(|d| self.idle_since + d)
            }
            _ => None,
        };
        for (_, timeout) in self.pending.values() {
            if let Some(t) = timeout {
                deadline = Some(deadline.map_or(*t, |d| d.min(*t)));
            }
        }
        deadline
    }
    async fn timer(&mut self) -> Result<()> {
        let now = Instant::now();
        let expired = self
            .pending
            .iter()
            .filter(|(_, (_, d))| d.is_some_and(|d| d <= now))
            .map(|(id, (r, _))| (*id, r.clone()))
            .collect::<Vec<_>>();
        for (id, request) in expired {
            // Cancelling one request withdraws every other request in this turn.
            if !self.pending.contains_key(&id) {
                continue;
            }
            let answer = request
                .options
                .iter()
                .find(|o| o.effect == PermissionEffect::RejectOnce)
                .map_or(PermissionAnswer::CancelTurn, |o| {
                    PermissionAnswer::Choose(o.key.clone())
                });
            self.answer(id, self.incarnation, answer).await?;
        }
        match self.phase {
            Phase::Crashed { retry_at, .. } if retry_at <= now => {
                self.reopen().await?;
            }
            Phase::Idle if self.idle_after.is_some_and(|d| self.idle_since + d <= now) => {
                self.phase(Phase::Suspending);
                self.stop_session().await?;
                self.phase(Phase::Dormant);
            }
            _ => {}
        }
        Ok(())
    }
}

fn respond(reply: Reply, result: Result<()>) {
    if reply.send(result).is_err() {
        tracing::trace!("actor command caller dropped");
    }
}
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
fn render_peer(env: &Envelope, name: &str, kind: &str) -> String {
    let reply = env
        .in_reply_to()
        .map_or(String::new(), |id| format!(" in-reply-to=\"{id}\""));
    format!(
        "<agent-message id=\"{}\" from=\"@{}\" kind=\"{}\" conversation=\"{}\"{}>\n{}\n</agent-message>\nThis is a message from another agent, not from your user. Reply with send_message.",
        env.id(),
        escape(name),
        escape(kind),
        env.conversation(),
        reply,
        escape(env.body().as_str())
    )
}
