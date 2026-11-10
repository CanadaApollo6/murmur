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
    #[serde(default)]
    pub workspace: Option<crate::sandbox::PinnedPath>,
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
    EgressDenied {
        agent: AgentId,
        host: String,
        port: u16,
    },
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
        incarnation: Incarnation,
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
    pub cwd: PathBuf,
    commands: mpsc::Sender<Command>,
    phase: watch::Receiver<Phase>,
    isolation: watch::Receiver<&'static str>,
    incarnation: watch::Receiver<Incarnation>,
}
impl AgentHandle {
    fn nudge(&self) {
        match self.commands.try_send(Command::Nudge) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!(agent=%self.id, "mail queued for stopped actor")
            }
        }
    }
    pub fn isolation(&self) -> &'static str {
        *self.isolation.borrow()
    }
    pub fn phase(&self) -> Phase {
        self.phase.borrow().clone()
    }
    pub fn incarnation(&self) -> Incarnation {
        *self.incarnation.borrow()
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
    agents: Arc<Mutex<HashMap<AgentId, AgentHandle>>>,
    replacements: Mutex<()>,
    #[cfg(test)]
    startup_pause: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>>,
    #[cfg(test)]
    test_adapter: Option<Arc<dyn Adapter>>,
    #[cfg(test)]
    actor_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    events: crate::event_hub::EventHub,
    config: Config,
    mcp: Option<Arc<crate::surfaces::Bindings>>,
    isolation: Option<(PathBuf, crate::sandbox::SandboxConfig)>,
    credentials: Arc<std::sync::Mutex<crate::credentials::Credentials>>,
}
impl Broker {
    pub fn new(mailroom: Arc<Mailroom>, kinds: Vec<KindConfig>, config: Config) -> Result<Self> {
        if config.crash_limit == 0
            || config.backoff_initial.is_zero()
            || config.backoff_max < config.backoff_initial
        {
            return Err(Error::Rejected("invalid crash policy".into()));
        }
        let events = crate::event_hub::EventHub::default();
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
            agents: Arc::new(Mutex::new(HashMap::new())),
            replacements: Mutex::new(()),
            #[cfg(test)]
            startup_pause: std::sync::Mutex::new(None),
            #[cfg(test)]
            test_adapter: None,
            #[cfg(test)]
            actor_tasks: Default::default(),
            events,
            config,
            mcp: None,
            isolation: None,
            credentials: Default::default(),
        })
    }
    pub fn with_mcp(
        mut self,
        bindings: Arc<crate::surfaces::Bindings>,
        state: PathBuf,
        sandbox: crate::sandbox::SandboxConfig,
    ) -> Self {
        self.isolation = Some((state, sandbox));
        self.mcp = Some(bindings);
        self
    }
    #[cfg(test)]
    pub(crate) async fn pause_sends(&self) -> impl Send + use<> {
        self.agents.clone().lock_owned().await
    }
    pub async fn handle(&self, id: AgentId) -> Option<AgentHandle> {
        self.agents.lock().await.get(&id).cloned()
    }
    pub(crate) async fn nudge(&self, id: AgentId) {
        if let Some(handle) = self.agents.lock().await.get(&id) {
            handle.nudge();
        }
    }
    /// The receiver is display-only and may lag. SQLite remains delivery authority.
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.events()
    }
    pub fn event_hub(&self) -> &crate::event_hub::EventHub {
        &self.events
    }
    pub async fn spawn(&self, spec: AgentSpec) -> Result<AgentHandle> {
        self.spawn_inner(spec, false).await
    }
    pub async fn restore(&self, spec: AgentSpec) -> Result<AgentHandle> {
        self.spawn_inner(spec, true).await
    }
    async fn spawn_inner(&self, mut spec: AgentSpec, restore: bool) -> Result<AgentHandle> {
        let kind = self
            .kinds
            .get(&spec.kind)
            .ok_or_else(|| Error::Rejected("unknown kind".into()))?
            .clone();
        let _replacement = self.replacements.lock().await;
        let saved_specs = self.mailroom.specs().await?;
        if !restore && let Some((_, cfg)) = &self.isolation {
            let workspaces = saved_specs
                .iter()
                .filter_map(|(_, s)| s.workspace.as_ref())
                .collect::<Vec<_>>();
            spec.cwd = cfg
                .spawn_cwd(&spec.cwd, &workspaces)
                .map_err(|e| Error::Rejected(e.to_string()))?;
        }
        if matches!(kind.isolation, crate::sandbox::Isolation::Bwrap(_)) {
            let (state, cfg) = self
                .isolation
                .as_ref()
                .ok_or_else(|| Error::Rejected("bwrap requires an MCP isolation context".into()))?;
            if self.mcp.is_none() {
                return Err(Error::Rejected(
                    "bwrap requires an MCP isolation context".into(),
                ));
            }
            if !restore {
                let pin = crate::sandbox::PinnedPath::capture(&spec.cwd, cfg, state)
                    .map_err(|e| Error::Rejected(e.to_string()))?;
                if pin.path != spec.cwd {
                    return Err(Error::Rejected(
                        "cwd changed while resolving; retry with the canonical path".into(),
                    ));
                }
                spec.workspace = Some(pin);
            }
        }
        let model_agent = matches!(kind.wire, Wire::Infinity { .. });
        let mut spec = spec;
        if spec.model.is_none() {
            spec.model = kind.model.clone();
        }
        let id = self.mailroom.register(spec.name.clone()).await?;
        let previous = self.agents.lock().await.get(&id).cloned();
        if let Some(handle) = previous.filter(|h| !h.commands.is_closed()) {
            if !restore && matches!(handle.phase(), Phase::Failed { .. }) {
                if let Some(pin) = saved_specs
                    .iter()
                    .find(|(saved, _)| *saved == id)
                    .and_then(|(_, s)| s.workspace.as_ref())
                    && spec.cwd != pin.path
                {
                    return Err(Error::Rejected(format!(
                        "failed agent cwd must still resolve to pinned path {}; resolved cwd is {}; spawn under a new name instead",
                        pin.path.display(),
                        spec.cwd.display()
                    )));
                }
                handle.stop().await?;
            } else {
                return Err(Error::Rejected("agent already supervised".into()));
            }
        }
        let (lease, incarnation, resume) = self.mailroom.claim_actor(id, spec.kind.clone()).await?;
        self.mailroom.save_spec(id, spec.clone()).await?;
        let (tx, commands) = mpsc::channel(64);
        let (phase_tx, phase) = watch::channel(Phase::Dormant);
        let (isolation_tx, isolation) = watch::channel("none");
        let (incarnation_tx, incarnation_rx) = watch::channel(incarnation);
        let handle = AgentHandle {
            id,
            cwd: spec.cwd.clone(),
            commands: tx,
            phase,
            isolation,
            incarnation: incarnation_rx,
        };
        #[cfg(test)]
        let adapter = self
            .test_adapter
            .clone()
            .unwrap_or_else(|| AdapterSet::build(kind.clone()));
        #[cfg(not(test))]
        let adapter = AdapterSet::build(kind.clone());
        let actor = AgentActor {
            permissions_guard: self.events.owner(),
            _lease: lease,
            mcp: self.mcp.clone(),
            isolation: self.isolation.clone(),
            credentials: self.credentials.clone(),
            isolation_tx,
            incarnation_tx,
            kind: kind.clone(),
            id,
            idle_after: if model_agent {
                None
            } else {
                spec.idle_after.or(kind.idle_after)
            },
            model_agent,
            stall_timeout: kind.stall_timeout,
            last_activity: Instant::now(),
            deferred_mail: false,
            waiting_offers: Vec::new(),
            choice_deadlines: HashMap::new(),
            spec,
            adapter,
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
            interrupt_since: None,
            idle_since: Instant::now(),
        };
        self.agents.lock().await.insert(id, handle.clone());
        let task = tokio::spawn(actor.run());
        #[cfg(test)]
        self.actor_tasks
            .lock()
            .expect("bug: actor task list poisoned")
            .push(task);
        #[cfg(not(test))]
        drop(task);
        #[cfg(test)]
        {
            let pause = self
                .startup_pause
                .lock()
                .expect("bug: startup pause poisoned")
                .take();
            if let Some(pause) = pause {
                pause.notified().await;
            }
        }
        Ok(handle)
    }
    /// Trusted in-process API. External callers are authenticated by surfaces.
    pub async fn send(&self, message: NewMessage) -> Result<(SendOutcome, Envelope)> {
        let agents = Arc::clone(&self.agents);
        let to = message.to;
        let mailroom = Arc::clone(&self.mailroom);
        // A dropped caller must not separate a durable accept from its wakeup.
        tokio::spawn(async move {
            let outcome = mailroom.accept(message).await;
            match &outcome {
                Ok(_) => {
                    if let Some(handle) = agents.lock().await.get(&to) {
                        handle.nudge();
                    }
                }
                Err(error) => tracing::debug!(%error, "broker send failed"),
            }
            outcome.map_err(Error::from)
        })
        .await
        .map_err(crate::mailroom::Error::from)?
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
    isolation: Option<(PathBuf, crate::sandbox::SandboxConfig)>,
    credentials: Arc<std::sync::Mutex<crate::credentials::Credentials>>,
    kind: KindConfig,
    mcp: Option<Arc<crate::surfaces::Bindings>>,
    _lease: crate::mailroom::ActorLease,
    permissions_guard: crate::event_hub::PermissionGuard,
    id: AgentId,
    spec: AgentSpec,
    adapter: Arc<dyn Adapter>,
    mailroom: Arc<Mailroom>,
    config: Config,
    idle_after: Option<Duration>,
    model_agent: bool,
    stall_timeout: Duration,
    last_activity: Instant,
    deferred_mail: bool,
    waiting_offers: Vec<MessageId>,
    // Wire choice deadlines survive socket and incarnation changes.
    choice_deadlines: HashMap<String, Option<Instant>>,
    events: crate::event_hub::EventHub,
    commands: mpsc::Receiver<Command>,
    phase_tx: watch::Sender<Phase>,
    isolation_tx: watch::Sender<&'static str>,
    incarnation_tx: watch::Sender<Incarnation>,
    phase: Phase,
    incarnation: Incarnation,
    resume: Option<ResumeToken>,
    session: Option<Box<dyn Session>>,
    cancelling: bool,
    offered: Vec<MessageId>,
    pending: HashMap<PermissionId, (PermissionRequest, Option<Instant>)>,
    crashes: VecDeque<Instant>,
    unavailable_attempts: u32,
    interrupt_since: Option<Instant>,
    idle_since: Instant,
}

impl AgentActor {
    fn emit(&self, event: Event) {
        self.permissions_guard.emit(event);
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
        self.clear_pending();
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
        self.finish_credentials();
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
        let mut credentials_tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            self.pump().await?;
            let deadline = self.deadline();
            tokio::select! {
                _ = credentials_tick.tick() => self.credentials.lock().expect("bug: credential lock poisoned").sync(self.id),
                command = self.commands.recv() => {
                    // A full command queue can coalesce a nudge. Observe queued mail
                    // on every command without adding database reads per stream chunk.
                    self.deferred_mail = self.mailroom.has_pending(self.id).await?;
                    match command {
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
                    }
                },
                event = async { match &mut self.session { Some(s) => s.next_event().await, None => std::future::pending().await } } => {
                    self.on_event(event.unwrap_or(SessionEvent::Ended { reason: "session event stream closed".into() })).await?;
                },
                () = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending().await } } => self.timer().await?,
            }
        }
    }
    fn finish_credentials(&self) {
        self.credentials
            .lock()
            .expect("bug: credential lock poisoned")
            .finish(self.id);
    }
    async fn start(&mut self) -> Result<()> {
        self.finish_credentials();
        self.isolation_tx.send_replace("none");
        self.phase(Phase::Starting);
        let mut launch = crate::sandbox::direct(&self.kind, &self.spec);
        if let Some(mcp) = &self.mcp {
            let prepared = async {
                if let (crate::sandbox::Isolation::Bwrap(sb), Some((state, cfg))) =
                    (&self.kind.isolation, &self.isolation)
                {
                    if self.spec.workspace.as_ref().is_none_or(|p| p.is_legacy()) {
                        let pin = match &self.spec.workspace {
                            Some(pin) => {
                                pin.upgrade(cfg, state, true).map_err(|e| pin.failure(e))?
                            }
                            None => {
                                crate::sandbox::PinnedPath::capture(&self.spec.cwd, cfg, state)?
                            }
                        };
                        self.spec.cwd = pin.path.clone();
                        self.spec.workspace = Some(pin);
                        self.mailroom.save_spec(self.id, self.spec.clone()).await?;
                    }
                    let mounts = cfg.launch_mounts(
                        self.spec.workspace.as_ref().expect("workspace captured"),
                        state,
                    )?;
                    let paths = crate::sandbox::AgentPaths::new(state, self.id);
                    crate::sandbox::seed(sb, &paths.home)?;
                    self.credentials
                        .lock()
                        .expect("bug: credential lock poisoned")
                        .launch(
                            self.id,
                            &self.kind.name,
                            &sb.credentials
                                .iter()
                                .map(|p| {
                                    Ok((
                                        cfg.credential_host(p)?,
                                        p.strip_prefix(crate::sandbox::host_home())
                                            .expect("validated credential path")
                                            .to_path_buf(),
                                    ))
                                })
                                .collect::<crate::surfaces::Result<Vec<_>>>()?,
                            &paths.home,
                        )?;
                    let mut facts = crate::sandbox::Facts::collect()?;
                    facts.pinned = mounts;
                    facts.validate_destinations(&paths)?;
                    launch = crate::sandbox::plan(cfg, &self.kind, &self.spec, &paths, &facts);
                    launch.mcp_server = Some(
                        mcp.bind(
                            crate::surfaces::SessionIdentity {
                                agent: self.id,
                                incarnation: self.incarnation,
                            },
                            &paths,
                            cfg,
                            sb,
                            &self.spec,
                            self.events.clone(),
                        )
                        .await?,
                    );
                } else if !self.model_agent {
                    launch.mcp_server = Some(mcp.mint(self.id, self.incarnation).await?);
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;
            if let Err(error) = prepared {
                mcp.revoke(self.id).await;
                tracing::error!(agent=%self.id, kind=%self.kind.name, %error, "sandbox preparation failed");
                self.phase(Phase::Failed {
                    last: error.to_string(),
                });
                return Ok(());
            }
        }
        match self
            .adapter
            .open(OpenRequest {
                agent: self.id,
                spec: &self.spec,
                resume: self.resume.as_ref(),
                incarnation: self.incarnation,
                launch,
                bindings: self.mcp.as_deref(),
            })
            .await
        {
            Ok(mut session) => {
                self.isolation_tx.send_replace(
                    self.isolation
                        .as_ref()
                        .map_or("none", |(_, cfg)| cfg.label(&self.kind.isolation)),
                );
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
                self.deferred_mail = self.mailroom.has_pending(self.id).await?;
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
        self.incarnation_tx.send_replace(incarnation);
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
        loop {
            let mail = self.mailroom.next_batch(self.id).await?;
            self.deferred_mail = false;
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
            self.deferred_mail = self.mailroom.has_pending(self.id).await?;
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
                continue;
            }
            let behind_work =
                self.phase == Phase::Busy || self.session.as_ref().is_some_and(|s| s.busy());
            if behind_work {
                self.waiting_offers.extend(&outcome.offered);
            }
            self.offered.extend(&outcome.offered);
            self.emit(Event::Offered {
                agent: self.id,
                ids: outcome.offered,
            });
            if !behind_work {
                self.last_activity = Instant::now();
            }
            self.phase(Phase::Busy);
            if let Err(error) = self.session()?.submit(Turn { items }).await {
                self.end(error.to_string()).await?;
            }
            return Ok(());
        }
    }
    async fn on_event(&mut self, event: SessionEvent) -> Result<()> {
        if matches!(
            event,
            SessionEvent::Activity(_)
                | SessionEvent::TurnStarted { .. }
                | SessionEvent::Admitted { .. }
                | SessionEvent::Committed { .. }
                | SessionEvent::TurnEnded { .. }
                | SessionEvent::PermissionRequested(_)
                | SessionEvent::CancelComplete
        ) {
            self.last_activity = Instant::now();
        }
        match event {
            SessionEvent::TurnStarted { ids } => {
                self.validate_ids(&ids)?;
                self.mailroom.mark_started(&ids, self.incarnation).await?;
                self.emit(Event::Started {
                    agent: self.id,
                    ids,
                });
            }
            SessionEvent::Admitted { ids } => {
                self.validate_ids(&ids)?;
                self.waiting_offers.retain(|id| !ids.contains(id));
            }
            SessionEvent::Waiting { ids } => {
                self.validate_ids(&ids)?;
                self.waiting_offers.retain(|id| !ids.contains(id));
                self.waiting_offers.extend(ids);
            }
            SessionEvent::Committed { ids } => {
                self.validate_ids(&ids)?;
                self.unavailable_attempts = 0;
                self.interrupt_since = None;
                self.mailroom.mark_consumed(&ids, self.incarnation).await?;
                self.offered.retain(|id| !ids.contains(id));
                self.waiting_offers.retain(|id| !ids.contains(id));
                self.emit(Event::Consumed {
                    agent: self.id,
                    ids,
                });
            }
            SessionEvent::CancelComplete => {
                self.unavailable_attempts = 0;
                self.interrupt_since = None;
                self.cancelling = false;
                self.clear_pending();
                self.choice_deadlines.clear();
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
                    self.interrupt_since = None;
                    self.cancelling = false;
                    self.clear_pending();
                    self.choice_deadlines.clear();
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
                incarnation: self.incarnation,
                text,
            }),
            SessionEvent::PermissionRequested(request) => {
                if self.model_agent && self.phase == Phase::Idle {
                    self.phase(Phase::Busy);
                }
                self.permission(request).await?;
            }
            SessionEvent::PermissionsRestored(requests) => {
                let keys = requests
                    .iter()
                    .filter_map(|r| {
                        self.session
                            .as_ref()?
                            .permission_key(r.id)
                            .map(str::to_owned)
                    })
                    .collect::<Vec<_>>();
                self.choice_deadlines.retain(|key, _| keys.contains(key));
                for request in requests {
                    if self.phase == Phase::Idle {
                        self.phase(Phase::Busy);
                    }
                    self.permission(request).await?;
                }
            }
            SessionEvent::PermissionSettled { id, key } => {
                self.choice_deadlines.remove(&key);
                if self.pending.remove(&id).is_some() {
                    self.emit(Event::PermissionAnswered { agent: self.id, id });
                }
            }
            SessionEvent::ResumeUpdated => {
                if let Some(session) = &self.session {
                    let token = session.resume_token();
                    self.mailroom.save_resume(self.id, token.clone()).await?;
                    self.resume = Some(token);
                }
            }
            SessionEvent::CancelTimedOut => {
                tracing::warn!(agent=%self.id, "cancel deadline expired, waiting for session recovery");
                self.stop_session().await?;
                self.unavailable_attempts = self.unavailable_attempts.saturating_add(1);
                let since = self.interrupt_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(10 * 60) {
                    self.phase(Phase::NeedsRecovery {
                        reason:
                            "Infinity interrupt deadlines expired for ten minutes without progress"
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
            let deadline = match self
                .session()?
                .permission_key(request.id)
                .map(str::to_owned)
            {
                Some(key) => *self.choice_deadlines.entry(key).or_insert(deadline),
                None => deadline,
            };
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
        self.cancelling = true;
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
        self.finish_credentials();
        self.mailroom
            .settle_session(self.id, self.incarnation, false, self.model_agent)
            .await?;
        self.offered.clear();
        self.waiting_offers.clear();
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
    fn stall_deadline(&self) -> Option<Instant> {
        (self.phase == Phase::Busy
            && (!self.waiting_offers.is_empty() || self.deferred_mail)
            && !self.cancelling
            && !self.stall_timeout.is_zero())
        .then(|| self.last_activity + self.stall_timeout)
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
        if let Some(stall) = self.stall_deadline() {
            deadline = Some(deadline.map_or(stall, |d| d.min(stall)));
        }
        deadline
    }
    async fn timer(&mut self) -> Result<()> {
        let now = Instant::now();
        if self.stall_deadline().is_some_and(|d| d <= now) {
            self.emit(Event::Activity {
                agent: self.id,
                incarnation: self.incarnation,
                text: "mail-starvation stall timeout; cancelling turn".into(),
            });
            if let Err(error) = self.cancel().await {
                self.end(error.to_string()).await?;
            }
        }
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
    let note =
        crate::surfaces::acts::task_note(env).map_or(String::new(), |note| format!("{note}\n"));
    format!(
        "<agent-message id=\"{}\" from=\"@{}\" kind=\"{}\" conversation=\"{}\"{}{}>\n{}\n</agent-message>\n{note}This is a message from another agent, not from your user. Reply with send_message.",
        env.id(),
        escape(name),
        escape(kind),
        env.conversation(),
        reply,
        crate::surfaces::acts::tag_attrs(env),
        escape(env.body().as_str())
    )
}

#[cfg(test)]
mod replacement_tests {
    use super::*;

    #[tokio::test]
    async fn replacement_does_not_block_sends_or_agent_lookup() {
        let dir = tempfile::tempdir().expect("directory");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("mailroom"),
        );
        let (bound, _) = mpsc::unbounded_channel();
        let bindings = Arc::new(crate::surfaces::Bindings::new(
            dir.path().join("mcp"),
            "agentd".into(),
            bound,
        ));
        let broker = Arc::new(
            Broker::new(
                room.clone(),
                vec![KindConfig::infinity(dir.path().join("missing.sock"))],
                Default::default(),
            )
            .expect("broker"),
        );
        let name = AgentName::parse("replacing").expect("name");
        let id = room.register(name.clone()).await.expect("register");
        let spec = AgentSpec {
            name,
            kind: "infinity".into(),
            cwd: dir.path().into(),
            workspace: None,
            model: None,
            permissions: Default::default(),
            idle_after: None,
        };
        for round in 0..8 {
            let (tx, mut commands) = mpsc::channel(64);
            let (_, phase) = watch::channel(Phase::Failed {
                last: "injected failed actor".into(),
            });
            let (_, isolation) = watch::channel("none");
            let (_, incarnation) = watch::channel(Incarnation(1));
            broker.agents.lock().await.insert(
                id,
                AgentHandle {
                    id,
                    cwd: dir.path().into(),
                    commands: tx,
                    phase,
                    isolation,
                    incarnation,
                },
            );
            let (stopping, stopped) = oneshot::channel();
            let old_bindings = bindings.clone();
            let old = tokio::spawn(async move {
                if let Some(Command::Stop(reply)) = commands.recv().await {
                    stopping.send(()).expect("observe replacement stop");
                    old_bindings.revoke(id).await;
                    reply.send(Ok(())).expect("stop reply");
                }
            });
            let read_guard = bindings.hold_for_test().await;
            let replacing = {
                let broker = broker.clone();
                let spec = spec.clone();
                tokio::spawn(async move { broker.spawn(spec).await })
            };
            stopped.await.expect("replacement reached stop");
            let mut sends = tokio::task::JoinSet::new();
            for number in 0..8 {
                let broker = broker.clone();
                sends.spawn(async move {
                    broker
                        .send(NewMessage {
                            from: Sender::Human,
                            to: id,
                            body: Body::new("mail during replacement").expect("body"),
                            reply_to: None,
                            client_key: ClientKey(format!("{round}:{number}")),
                            act: crate::swarm::Act::Inform,
                            task: None,
                        })
                        .await
                });
            }
            tokio::time::timeout(Duration::from_secs(1), async {
                while let Some(result) = sends.join_next().await {
                    result.expect("send task").expect("accepted send");
                }
            })
            .await
            .expect("sends must not deadlock with replacement revocation");
            drop(read_guard);
            let current = tokio::time::timeout(Duration::from_secs(1), replacing)
                .await
                .expect("replacement completes")
                .expect("task")
                .expect("replacement");
            tokio::time::timeout(Duration::from_secs(1), broker.handle(id))
                .await
                .expect("ls lookup remains responsive");
            current.stop().await.expect("stop replacement");
            old.await.expect("old actor task");
        }
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[tokio::test]
    async fn replacement_first_phase_finds_published_handle() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("room"),
        );
        let broker = Arc::new(
            Broker::new(
                room.clone(),
                vec![KindConfig::infinity(dir.path().join("absent.sock"))],
                Default::default(),
            )
            .expect("broker"),
        );
        let name = AgentName::parse("a").expect("name");
        let id = room.register(name.clone()).await.expect("register");
        let (commands, rx) = mpsc::channel(1);
        drop(rx);
        let (_, phase) = watch::channel(Phase::Failed {
            last: "old actor".into(),
        });
        let (_, isolation) = watch::channel("none");
        let (_, incarnation) = watch::channel(Incarnation(1));
        broker.agents.lock().await.insert(
            id,
            AgentHandle {
                id,
                cwd: dir.path().into(),
                commands: commands.clone(),
                phase,
                isolation,
                incarnation,
            },
        );
        let pause = Arc::new(tokio::sync::Notify::new());
        *broker.startup_pause.lock().expect("pause") = Some(pause.clone());
        let spec = AgentSpec {
            name,
            kind: "infinity".into(),
            cwd: dir.path().into(),
            workspace: None,
            model: None,
            permissions: Default::default(),
            idle_after: None,
        };
        let mut events = broker.event_hub().state();
        let b = broker.clone();
        let task = tokio::spawn(async move { b.spawn(spec).await });
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("first phase deadline")
            .expect("phase event");
        assert!(matches!(event, Event::PhaseChanged { .. }));
        let current = broker.handle(id).await.expect("new handle published");
        assert!(
            !current.commands.same_channel(&commands),
            "startup event found the old actor handle"
        );
        pause.notify_one();
        task.await.expect("spawn task").expect("spawn");
        broker.shutdown().await.expect("shutdown");
    }
}

#[cfg(test)]
mod stale_phase_tests {
    use super::*;
    use crate::surfaces::{
        Bindings, ControlApi,
        events::{self, Sink},
    };
    use serde_json::Value;
    #[tokio::test]
    async fn stale_busy_event_reads_current_failed_phase() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("room"),
        );
        let broker =
            Arc::new(Broker::new(room.clone(), vec![], Default::default()).expect("broker"));
        let id = AgentId::new();
        let (commands, _rx) = mpsc::channel(1);
        let (_, phase) = watch::channel(Phase::Failed {
            last: "current failure".into(),
        });
        let (_, isolation) = watch::channel("none");
        let (_, incarnation) = watch::channel(Incarnation(1));
        broker.agents.lock().await.insert(
            id,
            AgentHandle {
                id,
                cwd: dir.path().into(),
                commands,
                phase,
                isolation,
                incarnation,
            },
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        let api = ControlApi {
            broker: broker.clone(),
            mailroom: room,
            bindings: Arc::new(Bindings::new(
                dir.path().join("mcp.sock"),
                std::env::current_exe().expect("exe"),
                tx,
            )),
            control_path: dir.path().join("control.sock"),
            ui_url: None,
        };
        struct Check {
            broker: Arc<Broker>,
            id: AgentId,
            checked: bool,
        }
        #[async_trait::async_trait]
        impl Sink for Check {
            async fn send(&mut self, v: Value) -> crate::surfaces::Result<()> {
                if v["event"] == "snapshot" {
                    self.broker.event_hub().emit(Event::PhaseChanged {
                        agent: self.id,
                        incarnation: Incarnation(1),
                        phase: Phase::Busy,
                    });
                }
                if v["event"] == "agent" {
                    assert_eq!(v["phase"], "failed");
                    assert_eq!(v["error"], "current failure");
                    self.checked = true;
                    return Err("checked phase".into());
                }
                Ok(())
            }
        }
        let mut sink = Check {
            broker,
            id,
            checked: false,
        };
        assert!(events::feed(&api, &mut sink, None).await.is_err());
        assert!(sink.checked);
    }
}

#[cfg(test)]
mod cleanup_panic_tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct FaultAdapter {
        opens: AtomicUsize,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    struct FaultSession {
        events: VecDeque<SessionEvent>,
        panic_on_suspend: bool,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl Adapter for FaultAdapter {
        async fn open(
            &self,
            r: OpenRequest<'_>,
        ) -> std::result::Result<Box<dyn Session>, AdapterError> {
            let second = self.opens.fetch_add(1, Ordering::SeqCst) > 0;
            let events = if second {
                vec![
                    SessionEvent::PermissionRequested(PermissionRequest {
                        id: PermissionId::new(),
                        agent: r.agent,
                        incarnation: r.incarnation,
                        tool: "shell".into(),
                        title: "pending after reopen".into(),
                        detail: "detail".into(),
                        options: vec![PermissionOption {
                            key: "allow".into(),
                            label: "Allow".into(),
                            effect: PermissionEffect::AllowOnce,
                        }],
                    }),
                    SessionEvent::Admitted {
                        ids: vec![MessageId::new()],
                    },
                ]
            } else {
                vec![SessionEvent::Ended {
                    reason: "injected first incarnation crash".into(),
                }]
            };
            Ok(Box::new(FaultSession {
                events: events.into(),
                panic_on_suspend: second,
                entered: self.entered.clone(),
                release: self.release.clone(),
            }))
        }
    }
    #[async_trait::async_trait]
    impl Session for FaultSession {
        fn resume_token(&self) -> ResumeToken {
            ResumeToken::encode(&"fault fixture")
        }
        fn resumable(&self) -> bool {
            true
        }
        fn busy(&self) -> bool {
            true
        }
        async fn submit(&mut self, _: Turn) -> std::result::Result<(), AdapterError> {
            Ok(())
        }
        async fn cancel(&mut self) -> std::result::Result<(), AdapterError> {
            Ok(())
        }
        async fn answer_permission(
            &mut self,
            _: PermissionId,
            _: &str,
        ) -> std::result::Result<(), AdapterError> {
            Ok(())
        }
        async fn next_event(&mut self) -> Option<SessionEvent> {
            match self.events.pop_front() {
                Some(e) => Some(e),
                None => std::future::pending().await,
            }
        }
        async fn suspend(self: Box<Self>) -> std::result::Result<ResumeToken, AdapterError> {
            if self.panic_on_suspend {
                self.entered.notify_one();
                self.release.notified().await;
                panic!("injected cleanup panic after reopen");
            }
            Ok(self.resume_token())
        }
    }
    #[tokio::test]
    async fn running_actor_reopen_permission_is_cleared_before_cleanup_panics() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("room"),
        );
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let mut kind = KindConfig::claude("unused-fixture");
        kind.isolation = crate::sandbox::Isolation::None;
        let mut broker = Broker::new(
            room.clone(),
            vec![kind],
            Config {
                backoff_initial: Duration::from_millis(1),
                backoff_max: Duration::from_millis(1),
                ..Default::default()
            },
        )
        .expect("broker");
        broker.test_adapter = Some(Arc::new(FaultAdapter {
            opens: AtomicUsize::new(0),
            entered: entered.clone(),
            release: release.clone(),
        }));
        let broker = Arc::new(broker);
        let mut state = broker.events();
        let handle = broker
            .spawn(AgentSpec {
                name: AgentName::parse("a").expect("name"),
                kind: "claude".into(),
                cwd: dir.path().into(),
                workspace: None,
                model: None,
                permissions: Default::default(),
                idle_after: None,
            })
            .await
            .expect("spawn");
        let request = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Event::PermissionPending(r) = state.recv().await.expect("event") {
                    break r;
                }
            }
        })
        .await
        .expect("permission after reopen");
        assert_eq!(request.incarnation, Incarnation(2));
        // A reopen is a new life on the handle the feed reads, not only in events.
        assert_eq!(handle.incarnation(), Incarnation(2));
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("cleanup window");
        let (tx, _rx) = mpsc::unbounded_channel();
        let api = crate::surfaces::ControlApi {
            broker: broker.clone(),
            mailroom: room.clone(),
            bindings: Arc::new(crate::surfaces::Bindings::new(
                dir.path().join("mcp.sock"),
                std::env::current_exe().expect("exe"),
                tx,
            )),
            control_path: dir.path().join("control.sock"),
            ui_url: None,
        };
        struct Snapshot(Option<serde_json::Value>);
        #[async_trait::async_trait]
        impl crate::surfaces::events::Sink for Snapshot {
            async fn send(&mut self, v: serde_json::Value) -> crate::surfaces::Result<()> {
                self.0 = Some(v);
                Err("snapshot captured".into())
            }
        }
        let mut sink = Snapshot(None);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            crate::surfaces::events::feed(&api, &mut sink, None),
        )
        .await
        .expect("snapshot must not ask blocked actor");
        assert!(result.is_err());
        assert!(
            sink.0.expect("snapshot")["approvals"]
                .as_array()
                .expect("approvals")
                .is_empty(),
            "snapshot retained permission during cleanup"
        );
        release.notify_one();
        let task = broker
            .actor_tasks
            .lock()
            .expect("tasks")
            .pop()
            .expect("actor task");
        assert!(task.await.expect_err("injected panic").is_panic());
        handle.commands.closed().await;
        assert!(broker.event_hub().permissions().is_empty());
        let answered = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Event::PermissionAnswered { id, .. } =
                    state.recv().await.expect("answer event")
                    && id == request.id
                {
                    break;
                }
            }
        })
        .await;
        assert!(
            answered.is_ok(),
            "permission_answered missing after cleanup panic"
        );
    }
}
