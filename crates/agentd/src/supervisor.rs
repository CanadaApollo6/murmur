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

/// `Stopped` is set only by the operator. Mail queues until `restart`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Dormant,
    Starting,
    Idle,
    Busy,
    Suspending,
    Ended,
    Crashed {
        attempt: u32,
        retry_at: Instant,
        reason: String,
    },
    Failed {
        last: String,
    },
    NeedsRecovery {
        reason: String,
    },
    Stopped,
}

/// An operator command that ends the agent's session. A turn in flight is
/// cancelled first and waited for at most the kind's `cancel_timeout`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Stop,
    Restart,
    /// Stops, then marks the agent retired and removes its actor. A retire
    /// absorbs any later stop or restart sent while it waits.
    Retire,
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
    /// `AgentHandle::error` changed while the phase did not.
    ErrorChanged {
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
    /// The agent's actor and handle left the broker for good.
    Retired {
        agent: AgentId,
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
    Operate {
        op: Operation,
        reply: Reply,
    },
}

#[derive(Clone)]
pub struct AgentHandle {
    pub id: AgentId,
    pub cwd: PathBuf,
    commands: mpsc::Sender<Command>,
    phase: watch::Receiver<Phase>,
    isolation: watch::Receiver<&'static str>,
    incarnation: watch::Receiver<Incarnation>,
    /// Why the last stop or restart did not settle, until one settles.
    hint: watch::Receiver<Option<String>>,
    /// Infinity work a halt cancelled that may still run in the daemon. Each
    /// actor spawned for this id inherits it from the previous handle.
    warning: watch::Sender<Option<String>>,
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
    /// What `agentctl ls` and the feed show as the agent's error: the phase's
    /// own reason and whatever the last stop or restart left unresolved.
    pub fn error(&self) -> Option<String> {
        let parts = [
            phase_error(&self.phase()),
            self.hint.borrow().clone(),
            self.warning(),
        ];
        let text = parts.into_iter().flatten().collect::<Vec<_>>().join("; ");
        (!text.is_empty()).then_some(text)
    }
    fn warning(&self) -> Option<String> {
        self.warning.borrow().clone()
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
    async fn operate(&self, op: Operation) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Operate { op, reply: tx })
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
    closing: std::sync::atomic::AtomicBool,
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
    verify: Option<Arc<crate::verify::Runner>>,
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
            closing: Default::default(),
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
            verify: None,
        })
    }
    /// Starts verifiers for pending claims. Run `run_verifications` too.
    /// The mailroom needs `verifier_available` set.
    pub fn with_verifier(mut self, runner: crate::verify::Runner) -> Self {
        self.verify = Some(Arc::new(runner));
        self
    }
    /// The reconcile loop of the verifier runner. Returns at once without a
    /// runner, and otherwise when the mailroom closes.
    pub async fn run_verifications(&self) {
        if let Some(runner) = self.verify.clone() {
            crate::verify::run(&runner, self).await;
        }
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
    /// Stops, restarts or retires an agent. An actor that exited in `Failed` or
    /// panicked is respawned from its saved spec and reads the persisted stop
    /// flag, so all three work from any phase. An actor ended by shutdown or
    /// replacement exits in `Ended` and is never respawned.
    pub async fn operate(&self, id: AgentId, op: Operation) -> Result<()> {
        let mut respawned = false;
        loop {
            if let Some(handle) = self.handle(id).await {
                match handle.operate(op).await {
                    Err(Error::Closed) if !respawned => {}
                    result => return result,
                }
            }
            let _replacement = self.replacements.lock().await;
            let previous = self.handle(id).await;
            if previous.as_ref().is_some_and(|h| !h.commands.is_closed()) {
                // Another caller respawned the actor first. Ask that actor, so
                // a reply never comes from anywhere but an actor or `halt_reply`.
                continue;
            }
            let result = self.respawn(id, op, previous.as_ref()).await;
            if op == Operation::Retire && result.is_ok() {
                // The respawned actor starts stopped and performs the retire.
                respawned = true;
                continue;
            }
            return halt_reply(op, result, previous.and_then(|h| h.warning()).as_deref());
        }
    }
    /// Retires an agent from any phase. On return its session has ended, the
    /// mailroom holds it retired, and the broker no longer has its handle. A
    /// failed retire leaves the agent unretired, and repeating the call finishes it.
    pub async fn retire(&self, id: AgentId) -> Result<()> {
        let result = self.operate(id, Operation::Retire).await;
        if let Some(handle) = self.handle(id).await
            && self.mailroom.retired(id).await?
        {
            handle.commands.closed().await;
        }
        result
    }
    async fn respawn(
        &self,
        id: AgentId,
        op: Operation,
        previous: Option<&AgentHandle>,
    ) -> Result<()> {
        if self.closing.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Rejected("agentd is shutting down".into()));
        }
        if previous.is_some_and(|h| h.phase() == Phase::Ended) {
            return Err(Error::Closed);
        }
        self.mailroom.resolve(Recipient::Id(id)).await?;
        let (_, spec) = self
            .mailroom
            .specs()
            .await?
            .into_iter()
            .find(|(saved, _)| *saved == id)
            .ok_or_else(|| Error::Rejected("agent has never been spawned".into()))?;
        if !self.kinds.contains_key(&spec.kind) {
            return Err(Error::Rejected("unknown kind".into()));
        }
        // These writes are the end state the operator asked for. If the spawn
        // fails, the actor stays dead in Failed and a repeated command converges.
        self.mailroom
            .set_stopped(id, op != Operation::Restart)
            .await?;
        if previous.is_some_and(|h| {
            matches!(
                h.phase(),
                Phase::Failed { .. } | Phase::NeedsRecovery { .. }
            )
        }) {
            // Leaving these phases discards the resume token, as in the actor.
            self.mailroom.forget_resume(id).await?;
        }
        self.spawn_locked(spec, true).await.map(drop)
    }
    async fn spawn_inner(&self, spec: AgentSpec, restore: bool) -> Result<AgentHandle> {
        let _replacement = self.replacements.lock().await;
        if self.closing.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        self.spawn_locked(spec, restore).await
    }
    async fn spawn_locked(&self, mut spec: AgentSpec, restore: bool) -> Result<AgentHandle> {
        let kind = self
            .kinds
            .get(&spec.kind)
            .ok_or_else(|| Error::Rejected("unknown kind".into()))?
            .clone();
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
        let id = if restore {
            self.mailroom.register_saved(spec.name.clone()).await?
        } else {
            self.mailroom.register(spec.name.clone()).await?
        };
        let previous = self.agents.lock().await.get(&id).cloned();
        let warning = previous
            .as_ref()
            .map_or_else(|| watch::channel(None).0, |h| h.warning.clone());
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
        let verifier = self.mailroom.role(id).await? == Some(crate::swarm::AgentRole::Verifier);
        let kind = if verifier {
            // A verifier gets no MCP server but the broker's.
            KindConfig {
                mcp_servers: Vec::new(),
                ..kind
            }
        } else {
            kind
        };
        let (lease, incarnation, resume) = self.mailroom.claim_actor(id, spec.kind.clone()).await?;
        self.mailroom.save_spec(id, spec.clone()).await?;
        let initial = if self.mailroom.stopped(id).await? {
            Phase::Stopped
        } else {
            Phase::Dormant
        };
        let (tx, commands) = mpsc::channel(64);
        let (phase_tx, phase) = watch::channel(initial.clone());
        let (isolation_tx, isolation) = watch::channel("none");
        let (incarnation_tx, incarnation_rx) = watch::channel(incarnation);
        let (hint_tx, hint) = watch::channel(None);
        let handle = AgentHandle {
            id,
            cwd: spec.cwd.clone(),
            commands: tx,
            phase,
            isolation,
            incarnation: incarnation_rx,
            hint,
            warning: warning.clone(),
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
            agents: self.agents.clone(),
            retired: false,
            kept_credentials: None,
            _lease: lease,
            mcp: self.mcp.clone(),
            isolation: self.isolation.clone(),
            credentials: self.credentials.clone(),
            verifier,
            isolation_tx,
            incarnation_tx,
            hint_tx,
            warning,
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
            phase: initial,
            incarnation,
            resume,
            session: None,
            cancelling: false,
            halting: None,
            fresh: false,
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
        let runner = self.verify.clone().filter(|_| {
            matches!(
                &message.act,
                crate::swarm::Act::Report {
                    outcome: crate::swarm::Outcome::Partial { .. }
                        | crate::swarm::Outcome::Infeasible { .. },
                    ..
                }
            )
        });
        // A dropped caller must not separate a durable accept from its wakeup.
        tokio::spawn(async move {
            // A claim's snapshot is taken here, before the claimant's send
            // returns, so the claimant cannot change its workspace first.
            let gate = match &runner {
                Some(runner) => Some(runner.gate.read().await),
                None => None,
            };
            let outcome = mailroom.accept(message).await;
            let claimed = match (&runner, &outcome) {
                (
                    Some(runner),
                    Ok((SendOutcome::Queued { id } | SendOutcome::Held { id, .. }, _)),
                ) if runner
                    .inflight
                    .lock()
                    .expect("bug: inflight set poisoned")
                    .insert(*id) =>
                {
                    Some((runner, *id))
                }
                _ => None,
            };
            drop(gate);
            if let Some((runner, id)) = claimed {
                runner.snapshot_accepted(&mailroom, id).await;
            }
            let wake = match &outcome {
                Ok(_) => vec![to],
                Err(crate::mailroom::Error::TaskExpired { notified, .. }) => notified.clone(),
                Err(error) => {
                    tracing::debug!(%error, "broker send failed");
                    Vec::new()
                }
            };
            let agents = agents.lock().await;
            for id in wake {
                if let Some(handle) = agents.get(&id) {
                    handle.nudge();
                }
            }
            outcome.map_err(Error::from)
        })
        .await
        .map_err(crate::mailroom::Error::from)?
    }
    /// Expires tasks at their deadlines and wakes the agents told about it.
    /// Sleeps until the earliest deadline or the next task change, so a newly
    /// opened or reopened task with an earlier deadline is seen. Returns when
    /// the mailroom closes.
    pub async fn expire_tasks(&self) {
        let mut changes = self.mailroom.task_changes();
        loop {
            changes.borrow_and_update();
            let next = match self
                .mailroom
                .expire_tasks(time::OffsetDateTime::now_utc())
                .await
            {
                Ok(sweep) => {
                    for agent in sweep.notified {
                        self.nudge(agent).await;
                    }
                    sweep.next
                }
                Err(crate::mailroom::Error::Closed) => return,
                Err(error) => {
                    tracing::error!(%error, "task expiry failed; retrying in one second");
                    // The failed sweep's own rolled-back writes announce a
                    // task change. Waiting on that channel here would retry
                    // at once, without end.
                    tokio::time::sleep(EXPIRY_RETRY).await;
                    continue;
                }
            };
            let wait = async {
                match next {
                    Some(at) => {
                        let left: Duration = (at - time::OffsetDateTime::now_utc())
                            .try_into()
                            .unwrap_or(Duration::ZERO);
                        // The deadline is wall time and this sleep is not. A
                        // suspend or a clock step moves only one of them, so a
                        // long sleep is cut short and the deadline read again.
                        tokio::time::sleep(left.min(EXPIRY_RECHECK)).await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = wait => {}
                changed = changes.changed() => if changed.is_err() { return; },
            }
        }
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.closing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // A respawn that began before `closing` publishes its handle before this lock frees.
        drop(self.replacements.lock().await);
        let handles = self
            .agents
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for handle in handles {
            if !handle.commands.is_closed() {
                match handle.stop().await {
                    // A retired actor exits on its own.
                    Ok(()) | Err(Error::Closed) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::verify::Host for Broker {
    fn mailroom(&self) -> &Mailroom {
        &self.mailroom
    }
    async fn verifier_down(&self, id: AgentId) -> bool {
        self.handle(id)
            .await
            .is_none_or(|h| h.commands.is_closed() || matches!(h.phase(), Phase::Failed { .. }))
    }
    async fn start_verifier(&self, spec: AgentSpec) -> std::result::Result<(), String> {
        self.restore(spec)
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }
    async fn restart_verifier(&self, id: AgentId) -> std::result::Result<(), String> {
        self.operate(id, Operation::Restart)
            .await
            .map_err(|e| e.to_string())
    }
    async fn retire_verifier(&self, id: AgentId) -> std::result::Result<(), String> {
        self.retire(id).await.map_err(|e| e.to_string())
    }
    fn pin_snapshot(
        &self,
        kind: &str,
        workspace: &std::path::Path,
    ) -> std::result::Result<Option<crate::sandbox::PinnedPath>, String> {
        let kind = self
            .kinds
            .get(kind)
            .ok_or_else(|| format!("verifier kind {kind} is not configured"))?;
        if !matches!(kind.isolation, crate::sandbox::Isolation::Bwrap(_)) {
            return Ok(None);
        }
        let (state, cfg) = self
            .isolation
            .as_ref()
            .ok_or("bwrap requires an MCP isolation context")?;
        crate::sandbox::PinnedPath::capture_snapshot(workspace, cfg, state)
            .map(Some)
            .map_err(|e| e.to_string())
    }
}

struct AgentActor {
    agents: Arc<Mutex<HashMap<AgentId, AgentHandle>>>,
    retired: bool,
    /// What a retire tells the operator when it had to keep a credential copy.
    kept_credentials: Option<String>,
    isolation: Option<(PathBuf, crate::sandbox::SandboxConfig)>,
    credentials: Arc<std::sync::Mutex<crate::credentials::Credentials>>,
    kind: KindConfig,
    /// A verifier's workspace is its snapshot, and it gets no writable bind.
    verifier: bool,
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
    hint_tx: watch::Sender<Option<String>>,
    warning: watch::Sender<Option<String>>,
    phase: Phase,
    incarnation: Incarnation,
    resume: Option<ResumeToken>,
    session: Option<Box<dyn Session>>,
    cancelling: bool,
    halting: Option<Halting>,
    /// The next settled stop or restart discards the resume token. Kept here,
    /// not on `Halting`, so a settlement that fails does not forget it.
    fresh: bool,
    offered: Vec<MessageId>,
    pending: HashMap<PermissionId, (PermissionRequest, Option<Instant>)>,
    crashes: VecDeque<Instant>,
    unavailable_attempts: u32,
    interrupt_since: Option<Instant>,
    idle_since: Instant,
}

struct Halting {
    op: Operation,
    replies: Vec<Reply>,
    deadline: Instant,
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
    fn set_error(&self, cell: &watch::Sender<Option<String>>, value: Option<String>) {
        if cell.send_if_modified(|v| std::mem::replace(v, value.clone()) != value) {
            self.emit(Event::ErrorChanged {
                agent: self.id,
                incarnation: self.incarnation,
                phase: self.phase.clone(),
            });
        }
    }
    /// A halt cancelled an Infinity turn. Suspend only detaches from its
    /// thread, so the turn may run on until evidence says it ended.
    fn warn(&self) {
        if self.model_agent {
            self.set_error(&self.warning, Some(UNACKNOWLEDGED.into()));
        }
    }
    /// Evidence that the thread's turn is over: it ended, or an attached
    /// session reports the thread idle.
    fn turn_over(&self) {
        self.set_error(&self.warning, None);
    }
    /// The reply to a stop or restart always carries the warning.
    fn reply(&self, op: Operation, result: Result<()>) -> Result<()> {
        halt_reply(op, result, self.warning.borrow().as_deref())
    }
    fn session(&mut self) -> Result<&mut Box<dyn Session>> {
        self.session.as_mut().ok_or(Error::Closed)
    }
    async fn run(mut self) {
        let result = self.drive().await;
        if let Some(halting) = self.halting.take() {
            let reason = match &result {
                Ok(()) => "the agent actor was stopped before the command finished".to_owned(),
                Err(error) => format!("the agent actor failed: {error}"),
            };
            for reply in halting.replies {
                respond(
                    reply,
                    self.reply(halting.op, Err(Error::Rejected(reason.clone()))),
                );
            }
        }
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
        if self.retired {
            self.agents.lock().await.remove(&self.id);
            self.emit(Event::Retired { agent: self.id });
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
        if self.phase == Phase::Stopped {
            self.phase(self.phase.clone());
        } else {
            self.start().await?;
        }
        let mut credentials_tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            self.settle_halt().await?;
            if self.retired {
                return Ok(());
            }
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
                    Some(Command::Operate { op, reply }) => self.operate(op, reply).await?,
                    }
                },
                event = async { match &mut self.session { Some(s) => s.next_event().await, None => std::future::pending().await } } => {
                    self.on_event(event.unwrap_or(SessionEvent::Ended { reason: "session event stream closed".into() })).await?;
                },
                () = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending().await } } => self.timer().await?,
            }
        }
    }
    /// A retired agent never launches again, so its credential copies go.
    /// Returns a note for the operator when a copy had to stay.
    fn discard_credentials(&self) -> Result<Option<String>> {
        let (crate::sandbox::Isolation::Bwrap(sb), Some((state, _))) =
            (&self.kind.isolation, &self.isolation)
        else {
            return Ok(None);
        };
        let home = crate::sandbox::AgentPaths::new(state, self.id).home;
        let relatives = sb
            .credentials
            .iter()
            .map(|path| {
                path.strip_prefix(crate::sandbox::host_home())
                    .map(std::path::Path::to_path_buf)
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::Rejected(format!("credential path: {error}")))?;
        let kept = self
            .credentials
            .lock()
            .expect("bug: credential lock poisoned")
            .discard(self.id, &home, &relatives)
            .map_err(|error| {
                Error::Rejected(format!(
                    "retire could not delete the credential copy under {}: {error}; repeat `agentctl retire` to finish",
                    home.display()
                ))
            })?;
        if kept.is_empty() {
            return Ok(None);
        }
        let kept = kept
            .iter()
            .map(|relative| home.join(relative).display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        tracing::warn!(agent=%self.id, %kept, "credential copy kept on retire");
        Ok(Some(format!(
            "agent retired, but its last credential refresh did not reach the host, so the copy was kept: {kept}"
        )))
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
                        self.verifier,
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
                if !busy && self.resume.is_some() {
                    self.turn_over();
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
                self.retry(self.unavailable_attempts, reason);
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
        if self.halting.is_some() {
            return Ok(());
        }
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
            // A new completion began after the interrupt, so the thread is still busy.
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
                self.turn_over();
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
                if self.halting.is_some() {
                    // The pending stop or restart publishes the outcome.
                    return Ok(());
                }
                self.unavailable_attempts = self.unavailable_attempts.saturating_add(1);
                let since = self.interrupt_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(10 * 60) {
                    self.phase(Phase::NeedsRecovery {
                        reason:
                            "Infinity interrupt deadlines expired for ten minutes without progress"
                                .into(),
                    });
                } else {
                    self.retry(
                        self.unavailable_attempts,
                        "cancel deadline expired, waiting for session recovery".into(),
                    );
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
                self.retry(self.unavailable_attempts, reason);
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
    async fn operate(&mut self, mut op: Operation, reply: Reply) -> Result<()> {
        if self
            .halting
            .as_ref()
            .is_some_and(|h| h.op == Operation::Retire)
        {
            op = Operation::Retire;
        }
        if op == Operation::Stop && self.halting.is_none() && self.phase == Phase::Stopped {
            respond(reply, self.reply(op, Ok(())));
            return Ok(());
        }
        if let Err(error) = self
            .mailroom
            .set_stopped(self.id, op != Operation::Restart)
            .await
        {
            respond(reply, self.reply(op, Err(error.into())));
            return Ok(());
        }
        self.fresh |= self.needs_fresh_session();
        if let Some(halting) = &mut self.halting {
            halting.op = op;
            halting.replies.push(reply);
            return Ok(());
        }
        self.halting = Some(Halting {
            op,
            replies: vec![reply],
            deadline: Instant::now() + self.kind.cancel_timeout,
        });
        if self.turn_in_flight() {
            self.warn();
            if let Err(error) = self.cancel().await {
                self.end(error.to_string()).await?;
            }
        }
        Ok(())
    }
    /// Leaving NeedsRecovery or Failed discards the resume token.
    fn needs_fresh_session(&self) -> bool {
        matches!(
            self.phase,
            Phase::NeedsRecovery { .. } | Phase::Failed { .. }
        )
    }
    /// An Infinity thread can be busy with work agentd did not submit.
    fn turn_in_flight(&self) -> bool {
        self.session
            .as_ref()
            .is_some_and(|s| self.phase == Phase::Busy || s.busy())
    }
    /// Finishes a stop or restart once the cancelled turn has ended or its deadline passed.
    async fn settle_halt(&mut self) -> Result<()> {
        let Some(halting) = &self.halting else {
            return Ok(());
        };
        if self.turn_in_flight() && halting.deadline > Instant::now() {
            return Ok(());
        }
        let Halting { op, replies, .. } = self.halting.take().expect("bug: halting checked above");
        if self.session.as_ref().is_some_and(|s| s.busy()) {
            self.warn();
        }
        if let Err(error) = self.stop_session().await {
            // The session is gone but its mail may still be offered. A repeated
            // command settles again, so the agent waits in Ended until then.
            let command = match op {
                Operation::Stop => "stop",
                Operation::Restart => "restart",
                Operation::Retire => "retire",
            };
            let reason = format!(
                "{command} did not settle the session: {error}; repeat `agentctl {command}` to settle it"
            );
            self.set_error(&self.hint_tx, Some(reason.clone()));
            self.phase(Phase::Ended);
            for reply in replies {
                respond(reply, self.reply(op, Err(Error::Rejected(reason.clone()))));
            }
            return Ok(());
        }
        self.set_error(&self.hint_tx, None);
        let fresh = std::mem::take(&mut self.fresh) || self.needs_fresh_session();
        let result = match op {
            Operation::Stop => self.stopped(fresh).await,
            Operation::Restart => self.restart(fresh).await,
            Operation::Retire => self.retire().await,
        };
        for reply in replies {
            let shown = match &result {
                // A restart that could not open its session did not restart.
                Ok(()) => phase_error(&self.phase)
                    .or_else(|| self.kept_credentials.clone())
                    .map_or(Ok(()), |e| Err(Error::Rejected(e))),
                Err(error) => Err(Error::Rejected(error.to_string())),
            };
            respond(reply, self.reply(op, shown));
        }
        result
    }
    async fn stopped(&mut self, fresh: bool) -> Result<()> {
        if fresh {
            // A later restart leaves NeedsRecovery or Failed fresh, as restart does directly.
            self.mailroom.forget_resume(self.id).await?;
        }
        self.phase(Phase::Stopped);
        Ok(())
    }
    /// The stop flag is already persisted, so a failure here leaves a stopped
    /// agent after a daemon restart, and the actor exits only once the row is retired.
    /// The credential copies go first, so a retired agent never keeps one by accident.
    async fn retire(&mut self) -> Result<()> {
        self.kept_credentials = self.discard_credentials()?;
        self.mailroom.retire(self.id).await?;
        self.retired = true;
        self.phase(Phase::Ended);
        Ok(())
    }
    async fn restart(&mut self, fresh: bool) -> Result<()> {
        if fresh {
            self.mailroom.forget_resume(self.id).await?;
        }
        self.crashes.clear();
        self.unavailable_attempts = 0;
        self.interrupt_since = None;
        self.reopen().await
    }
    async fn stop_session(&mut self) -> Result<()> {
        if let Some(mcp) = &self.mcp {
            mcp.revoke(self.id).await;
        }
        if let Some(session) = self.session.take() {
            if self.phase == Phase::Busy && !session.busy() {
                // The thread reported idle after this session's turn began,
                // whatever mail receipts are still missing. A session that ran
                // no turn is no evidence: it can be a fresh thread, and the
                // warning is about the thread before it.
                self.turn_over();
            }
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
        if self.halting.is_some() {
            // The pending stop or restart publishes the outcome. This is not a crash.
            tracing::warn!(agent=%self.id, %reason, "agent session ended during a stop or restart");
            return self.stop_session().await;
        }
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
            self.retry(self.crashes.len() as u32, reason);
        }
    }
    fn retry(&mut self, attempt: u32, reason: String) {
        if self.halting.is_some() {
            // The pending stop or restart publishes the outcome.
            return;
        }
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
            reason,
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
        if let Some(halting) = &self.halting {
            deadline = Some(deadline.map_or(halting.deadline, |d| d.min(halting.deadline)));
        }
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

/// The longest the expiry loop sleeps before it reads the wall clock again.
const EXPIRY_RECHECK: Duration = Duration::from_secs(30);
const EXPIRY_RETRY: Duration = Duration::from_secs(1);
const UNACKNOWLEDGED: &str =
    "the Infinity turn has not acknowledged the cancel and may still be running in the daemon";

fn halt_reply(op: Operation, result: Result<()>, warning: Option<&str>) -> Result<()> {
    let Some(warning) = warning else {
        return result;
    };
    let done = match op {
        Operation::Stop => "stopped",
        Operation::Restart => "restarted",
        Operation::Retire => "retired",
    };
    Err(Error::Rejected(match result {
        Ok(()) => format!("agent {done}, but {warning}"),
        Err(error) => format!("{error}; {warning}"),
    }))
}
/// The reason a phase carries, as `agentctl ls` and the feed show it.
pub fn phase_error(phase: &Phase) -> Option<String> {
    match phase {
        Phase::Failed { last } => Some(last.clone()),
        Phase::Crashed { reason, .. } | Phase::NeedsRecovery { reason } => Some(reason.clone()),
        _ => None,
    }
}
fn respond(reply: Reply, result: Result<()>) {
    if reply.send(result).is_err() {
        tracing::trace!("actor command caller dropped");
    }
}
pub(crate) fn escape(text: &str) -> String {
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
        "<agent-message id=\"{}\" from=\"@{}\" kind=\"{}\" conversation=\"{}\"{}{}>\n{}\n</agent-message>\nThis is a message from another agent, not from your user. {}",
        env.id(),
        escape(name),
        escape(kind),
        env.conversation(),
        reply,
        crate::surfaces::acts::tag_attrs(env),
        escape(&crate::surfaces::acts::content(env)),
        crate::surfaces::acts::task_note(env).unwrap_or_else(|| "Reply with send_message.".into())
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
                    hint: watch::channel(None).1,
                    warning: watch::channel(None).0,
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
                hint: watch::channel(None).1,
                warning: watch::channel(None).0,
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
                hint: watch::channel(None).1,
                warning: watch::channel(None).0,
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

#[cfg(test)]
mod halt_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    const CANCEL: Duration = Duration::from_millis(100);

    /// A session whose cancel is never acknowledged, so only the halt deadline ends a turn.
    #[derive(Default)]
    struct Script {
        opens: AtomicUsize,
        resumed: AtomicUsize,
        submits: AtomicUsize,
        cancels: AtomicUsize,
        failing_suspends: AtomicUsize,
        /// Work the agent runs that agentd did not submit.
        thread_busy: AtomicBool,
        accepts_busy_mail: AtomicBool,
        cancel_times_out: AtomicBool,
        /// What a cancel does to the session besides counting.
        on_cancel: std::sync::Mutex<OnCancel>,
        panic_on_submit: AtomicBool,
        latest: std::sync::Mutex<Option<mpsc::UnboundedSender<SessionEvent>>>,
    }
    #[derive(Clone, Copy, Debug, Default, PartialEq)]
    enum OnCancel {
        #[default]
        Nothing,
        /// The session reports that it ended.
        EndSession,
        /// The event stream closes, as when the daemon socket drops.
        Disconnect,
        /// The cancel cannot be sent.
        Fail,
        /// The actor dies before it answers the command.
        Panic,
    }
    struct Scripted(Arc<Script>);
    struct ScriptedSession {
        script: Arc<Script>,
        tx: mpsc::UnboundedSender<SessionEvent>,
        events: mpsc::UnboundedReceiver<SessionEvent>,
    }
    #[async_trait::async_trait]
    impl Adapter for Scripted {
        async fn open(
            &self,
            r: OpenRequest<'_>,
        ) -> std::result::Result<Box<dyn Session>, AdapterError> {
            self.0.opens.fetch_add(1, SeqCst);
            if r.resume.is_some() {
                self.0.resumed.fetch_add(1, SeqCst);
            }
            let (tx, events) = mpsc::unbounded_channel();
            *self.0.latest.lock().expect("bug: script lock") = Some(tx.clone());
            Ok(Box::new(ScriptedSession {
                script: self.0.clone(),
                tx,
                events,
            }))
        }
    }
    #[async_trait::async_trait]
    impl Session for ScriptedSession {
        fn resume_token(&self) -> ResumeToken {
            ResumeToken::encode(&"scripted")
        }
        fn resumable(&self) -> bool {
            true
        }
        fn busy(&self) -> bool {
            self.script.thread_busy.load(SeqCst)
        }
        fn accepts_busy_mail(&self) -> bool {
            self.script.accepts_busy_mail.load(SeqCst)
        }
        async fn submit(&mut self, _: Turn) -> std::result::Result<(), AdapterError> {
            self.script.submits.fetch_add(1, SeqCst);
            assert!(
                !self.script.panic_on_submit.load(SeqCst),
                "injected actor panic"
            );
            Ok(())
        }
        async fn cancel(&mut self) -> std::result::Result<(), AdapterError> {
            self.script.cancels.fetch_add(1, SeqCst);
            if self.script.cancel_times_out.load(SeqCst) {
                self.tx
                    .send(SessionEvent::CancelTimedOut)
                    .expect("bug: session owns its receiver");
            }
            // Copied out so an injected panic does not poison the lock.
            let mode = *self.script.on_cancel.lock().expect("bug: script lock");
            match mode {
                OnCancel::Nothing => {}
                OnCancel::EndSession => self
                    .tx
                    .send(SessionEvent::Ended {
                        reason: "injected session end".into(),
                    })
                    .expect("bug: session owns its receiver"),
                OnCancel::Disconnect => self.events.close(),
                OnCancel::Fail => return Err(AdapterError::Spawn("injected dead socket".into())),
                OnCancel::Panic => panic!("injected actor panic on cancel"),
            }
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
            self.events.recv().await
        }
        async fn suspend(self: Box<Self>) -> std::result::Result<ResumeToken, AdapterError> {
            if self
                .script
                .failing_suspends
                .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(AdapterError::Spawn("injected suspend failure".into()));
            }
            Ok(self.resume_token())
        }
    }

    struct Harness {
        _dir: tempfile::TempDir,
        room: Arc<Mailroom>,
        broker: Arc<Broker>,
        script: Arc<Script>,
        agent: AgentHandle,
    }
    async fn harness(infinity: bool) -> Harness {
        harness_with(infinity, CANCEL).await
    }
    async fn harness_with(infinity: bool, cancel_timeout: Duration) -> Harness {
        harness_built(infinity, cancel_timeout, false).await
    }
    /// The credential path a sandboxed harness agent is configured with.
    const CREDENTIAL: &str = ".agentd-retire-test/auth.json";
    /// `sandboxed` gives the kind a credential copy under `<dir>/state` and
    /// restores the agent stopped, so no sandbox is ever launched.
    async fn harness_built(infinity: bool, cancel_timeout: Duration, sandboxed: bool) -> Harness {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("room"),
        );
        let mut kind = if infinity {
            KindConfig::infinity(dir.path().join("unused.sock"))
        } else {
            KindConfig::claude("unused-fixture")
        };
        kind.isolation = if sandboxed {
            crate::sandbox::Isolation::Bwrap(crate::sandbox::KindSandbox {
                home: vec![],
                credentials: vec![crate::sandbox::host_home().join(CREDENTIAL)],
                egress: vec![],
            })
        } else {
            crate::sandbox::Isolation::None
        };
        kind.idle_after = None;
        kind.cancel_timeout = cancel_timeout;
        let mut broker = Broker::new(
            room.clone(),
            vec![kind.clone()],
            Config {
                backoff_initial: Duration::from_millis(1),
                backoff_max: Duration::from_millis(1),
                ..Default::default()
            },
        )
        .expect("broker");
        if sandboxed {
            let (bound, _) = mpsc::unbounded_channel();
            broker = broker.with_mcp(
                Arc::new(crate::surfaces::Bindings::new(
                    dir.path().join("mcp"),
                    "agentd".into(),
                    bound,
                )),
                state_root(&dir),
                Default::default(),
            );
        }
        let script = Arc::new(Script::default());
        broker.test_adapter = Some(Arc::new(Scripted(script.clone())));
        let broker = Arc::new(broker);
        let spec = spec(&kind.name, dir.path(), "a");
        let agent = if sandboxed {
            let id = room.register(spec.name.clone()).await.expect("register");
            room.save_spec(id, spec.clone()).await.expect("saved spec");
            room.set_stopped(id, true).await.expect("stop flag");
            broker.restore(spec).await.expect("restore")
        } else {
            broker.spawn(spec).await.expect("spawn")
        };
        let h = Harness {
            _dir: dir,
            room,
            broker,
            script,
            agent,
        };
        let ready = if sandboxed {
            Phase::Stopped
        } else {
            Phase::Idle
        };
        h.wait(|p| *p == ready).await;
        h
    }
    /// The state directory of a sandboxed harness. Credential paths are never
    /// followed through a link, and a macOS temp directory sits behind one.
    fn state_root(dir: &tempfile::TempDir) -> PathBuf {
        dir.path()
            .canonicalize()
            .expect("canonical temp directory")
            .join("state")
    }
    fn spec(kind: &str, cwd: &std::path::Path, name: &str) -> AgentSpec {
        AgentSpec {
            name: AgentName::parse(name).expect("name"),
            kind: kind.into(),
            cwd: cwd.into(),
            workspace: None,
            model: None,
            permissions: Default::default(),
            idle_after: None,
        }
    }
    impl Harness {
        async fn wait(&self, matches: impl Fn(&Phase) -> bool) {
            let mut phases = self.agent.phases();
            tokio::time::timeout(Duration::from_secs(5), phases.wait_for(|p| matches(p)))
                .await
                .expect("phase timeout")
                .expect("phase channel");
        }
        async fn send(&self, body: &str) -> MessageId {
            self.broker
                .send(NewMessage {
                    from: Sender::Human,
                    to: self.agent.id,
                    body: Body::new(body).expect("body"),
                    reply_to: None,
                    client_key: ClientKey(ulid::Ulid::new().to_string()),
                    act: crate::swarm::Act::Inform,
                    task: None,
                })
                .await
                .expect("send")
                .1
                .id()
        }
        /// Offers one message and leaves the turn running.
        async fn busy(&self) -> MessageId {
            let id = self.send("work").await;
            self.wait(|p| *p == Phase::Busy).await;
            assert!(matches!(
                self.state(id).await,
                DeliveryState::Offered { .. }
            ));
            id
        }
        async fn operate(&self, op: Operation) -> Result<()> {
            let id = self.agent.id;
            tokio::time::timeout(CANCEL * 20, async {
                match op {
                    Operation::Retire => self.broker.retire(id).await,
                    op => self.broker.operate(id, op).await,
                }
            })
            .await
            .expect("operator command must finish within the halt deadline")
        }
        /// The actor is gone, the broker forgot it and its mail is dead.
        async fn assert_retired(&self, mail: Option<MessageId>, case: &str) {
            assert!(self.agent.commands.is_closed(), "{case}: actor exited");
            assert!(
                self.broker.handle(self.agent.id).await.is_none(),
                "{case}: handle removed"
            );
            assert!(
                self.room.retired(self.agent.id).await.expect("retired"),
                "{case}"
            );
            if let Some(mail) = mail {
                assert!(
                    matches!(self.state(mail).await, DeliveryState::Dead { .. }),
                    "{case}: inbound mail is dead"
                );
            }
        }
        /// Every command sent before this one has been handled.
        async fn barrier(&self) {
            self.broker
                .handle(self.agent.id)
                .await
                .expect("handle")
                .pending_permissions()
                .await
                .expect("actor round trip");
        }
        async fn state(&self, id: MessageId) -> DeliveryState {
            self.room.delivery(id).await.expect("delivery").state
        }
        async fn sql(&self, sql: &'static str) {
            self.room
                .observe(move |s| Ok(s.db.execute_batch(sql)?))
                .await
                .expect("inject SQL");
        }
        fn inject(&self, event: SessionEvent) {
            self.script
                .latest
                .lock()
                .expect("bug: script lock")
                .as_ref()
                .expect("a session was opened")
                .send(event)
                .expect("session is listening");
        }
        fn on_cancel(&self, mode: OnCancel) {
            *self.script.on_cancel.lock().expect("bug: script lock") = mode;
        }
        fn count(&self, counter: fn(&Script) -> &AtomicUsize) -> usize {
            counter(&self.script).load(SeqCst)
        }
    }
    /// Polls `operation` once so its command is queued, then waits until the actor took it.
    async fn queue(h: &Harness, operation: std::pin::Pin<&mut impl Future<Output = Result<()>>>) {
        tokio::select! {
            biased;
            result = operation => panic!("operation finished before its deadline: {result:?}"),
            () = h.barrier() => {}
        }
    }

    #[tokio::test]
    async fn halt_deadline_ends_a_turn_whose_cancel_is_never_acknowledged() {
        for op in [Operation::Stop, Operation::Restart, Operation::Retire] {
            let h = harness(false).await;
            let mail = h.busy().await;
            h.operate(op).await.expect("operator command");
            assert_eq!(h.count(|s| &s.cancels), 1, "{op:?} cancels the turn");
            match op {
                Operation::Stop => {
                    assert_eq!(h.agent.phase(), Phase::Stopped);
                    assert_eq!(h.state(mail).await, DeliveryState::Queued);
                    assert_eq!(h.count(|s| &s.opens), 1);
                }
                Operation::Restart => {
                    assert_eq!(h.count(|s| &s.opens), 2);
                    assert_eq!(
                        h.count(|s| &s.resumed),
                        1,
                        "restart of a busy agent resumes its session"
                    );
                }
                Operation::Retire => {
                    h.assert_retired(Some(mail), "retire").await;
                    assert_eq!(h.count(|s| &s.opens), 1);
                }
            }
            h.broker.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test]
    async fn mail_sent_during_a_halt_is_not_offered_to_the_halting_session() {
        for op in [Operation::Stop, Operation::Restart, Operation::Retire] {
            let h = harness(false).await;
            h.script.accepts_busy_mail.store(true, SeqCst);
            h.busy().await;
            let halt = h.broker.operate(h.agent.id, op);
            tokio::pin!(halt);
            queue(&h, halt.as_mut()).await;
            let late = h.send("late").await;
            h.barrier().await;
            assert_eq!(h.state(late).await, DeliveryState::Queued, "{op:?}");
            assert_eq!(h.count(|s| &s.submits), 1, "{op:?}");
            tokio::time::timeout(CANCEL * 20, halt)
                .await
                .expect("halt deadline")
                .expect("operator command");
            h.broker.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test]
    async fn a_halt_that_cannot_settle_says_so_and_a_repeat_repairs_it() {
        const REFUSE: &str = "CREATE TEMP TRIGGER refuse_settle BEFORE UPDATE OF state ON deliveries WHEN OLD.state='offered' BEGIN SELECT RAISE(ABORT,'injected settlement failure'); END;";
        for op in [Operation::Stop, Operation::Restart, Operation::Retire] {
            for suspend_fails in [true, false] {
                let case = format!("{op:?}, suspend fails: {suspend_fails}");
                let h = harness(false).await;
                let mail = h.busy().await;
                if suspend_fails {
                    h.script.failing_suspends.store(1, SeqCst);
                } else {
                    h.sql(REFUSE).await;
                }
                assert!(h.operate(op).await.is_err(), "{case}");
                assert_ne!(h.agent.phase(), Phase::Stopped, "{case}");
                assert_eq!(
                    h.count(|s| &s.opens),
                    1,
                    "{case}: no session before settlement"
                );
                assert!(
                    matches!(h.state(mail).await, DeliveryState::Offered { .. }),
                    "{case}"
                );
                assert!(
                    !h.room.retired(h.agent.id).await.expect("retired"),
                    "{case}: an unsettled retire leaves a normal agent"
                );
                if !suspend_fails {
                    h.sql("DROP TRIGGER temp.refuse_settle").await;
                }
                h.operate(op).await.expect("a repeated command settles");
                match op {
                    Operation::Stop => {
                        assert_eq!(h.agent.phase(), Phase::Stopped, "{case}");
                        assert_eq!(h.state(mail).await, DeliveryState::Queued, "{case}");
                    }
                    Operation::Restart => {
                        assert_eq!(h.count(|s| &s.opens), 2, "{case}");
                        assert_eq!(h.count(|s| &s.resumed), 1, "{case}");
                        assert_ne!(
                            h.state(mail).await,
                            DeliveryState::Offered {
                                incarnation: Incarnation(1)
                            },
                            "{case}"
                        );
                    }
                    Operation::Retire => h.assert_retired(Some(mail), &case).await,
                }
                h.broker.shutdown().await.expect("shutdown");
            }
        }
    }

    #[tokio::test]
    async fn stop_reports_infinity_work_that_may_still_be_running() {
        for timed_out in [false, true] {
            let h = harness(true).await;
            h.script.cancel_times_out.store(timed_out, SeqCst);
            h.script.thread_busy.store(true, SeqCst);
            let mut events = h.broker.events();
            let result = h.operate(Operation::Stop).await;
            assert_eq!(crashes(&mut events), vec![], "a halt is not a crash");
            assert_eq!(
                h.count(|s| &s.cancels),
                1,
                "stop interrupts daemon work agentd did not submit"
            );
            let error = result.expect_err("an unacknowledged interrupt is not a confirmed stop");
            assert!(
                error.to_string().contains("may still be running"),
                "{error}"
            );
            assert_eq!(h.agent.phase(), Phase::Stopped);
            let note = h.agent.error().expect("ls shows the warning");
            assert!(note.contains("may still be running"), "{note}");
            assert_eq!(
                h.operate(Operation::Stop).await.map_err(|e| e.to_string()),
                Err(format!("agent stopped, but {note}")),
                "a repeated stop repeats the note"
            );
            h.broker.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test]
    async fn operate_respawns_a_failed_actor_and_nothing_else() {
        let h = harness(false).await;
        h.sql("CREATE TEMP TRIGGER refuse_offer BEFORE UPDATE OF state ON deliveries WHEN NEW.state='offered' BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;").await;
        h.send("kills the actor").await;
        tokio::time::timeout(Duration::from_secs(5), h.agent.commands.closed())
            .await
            .expect("actor dies");
        assert!(matches!(h.agent.phase(), Phase::Failed { .. }));
        h.sql("DROP TRIGGER temp.refuse_offer").await;

        h.operate(Operation::Stop)
            .await
            .expect("stop respawns a failed actor");
        let current = h.broker.handle(h.agent.id).await.expect("handle");
        assert!(!current.commands.same_channel(&h.agent.commands));
        assert_eq!(current.phase(), Phase::Stopped);
        assert_eq!(h.count(|s| &s.opens), 1, "a stopped respawn opens nothing");
        h.operate(Operation::Restart).await.expect("restart");
        assert_eq!(h.count(|s| &s.opens), 2);
        assert_eq!(
            h.count(|s| &s.resumed),
            0,
            "leaving Failed through stop and restart is a fresh session"
        );

        h.broker.shutdown().await.expect("shutdown");
        assert!(
            h.operate(Operation::Restart).await.is_err(),
            "an actor ended by shutdown is not respawned"
        );
        assert_eq!(h.count(|s| &s.opens), 2);
    }

    #[tokio::test]
    async fn a_second_command_replaces_a_pending_halt_and_both_callers_hear() {
        use Operation::{Restart, Retire, Stop};
        for (first, second) in [
            (Stop, Stop),
            (Stop, Restart),
            (Restart, Stop),
            (Restart, Restart),
            (Stop, Retire),
            (Retire, Stop),
            (Retire, Restart),
        ] {
            let case = format!("{first:?} then {second:?}");
            let h = harness(false).await;
            h.busy().await;
            let a = h.broker.operate(h.agent.id, first);
            tokio::pin!(a);
            queue(&h, a.as_mut()).await;
            let b = h.broker.operate(h.agent.id, second);
            let (a, b) = tokio::time::timeout(CANCEL * 20, async { tokio::join!(a, b) })
                .await
                .expect("the first command's deadline bounds both");
            a.expect("first caller");
            b.expect("second caller");
            assert_eq!(h.count(|s| &s.cancels), 1, "{case}");
            match (first, second) {
                (Retire, _) | (_, Retire) => {
                    h.agent.commands.closed().await;
                    h.assert_retired(None, &case).await;
                    assert_eq!(h.count(|s| &s.opens), 1, "{case}");
                }
                (_, Stop) => {
                    assert_eq!(h.agent.phase(), Phase::Stopped, "{case}");
                    assert_eq!(h.count(|s| &s.opens), 1, "{case}");
                }
                (_, Restart) => {
                    assert_eq!(h.count(|s| &s.opens), 2, "{case}");
                    assert!(!h.room.stopped(h.agent.id).await.expect("flag"), "{case}");
                }
            }
            h.broker.shutdown().await.expect("shutdown");
        }
    }

    const REFUSE_SETTLE: &str = "CREATE TEMP TRIGGER refuse_settle BEFORE UPDATE OF state ON deliveries WHEN OLD.state='offered' BEGIN SELECT RAISE(ABORT,'injected settlement failure'); END;";

    fn assert_detached(result: Result<()>, prefix: &str, case: &str) -> String {
        let error = result
            .expect_err(&format!(
                "{case}: an unacknowledged interrupt is not a clean halt"
            ))
            .to_string();
        assert!(error.starts_with(prefix), "{case}: {error}");
        assert!(error.contains("may still be running"), "{case}: {error}");
        error
    }

    /// The phases a crash publishes. A session that goes away during a halt publishes none.
    fn crashes(events: &mut broadcast::Receiver<Event>) -> Vec<Phase> {
        let mut seen = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let Event::PhaseChanged {
                phase: phase @ (Phase::Ended | Phase::Crashed { .. } | Phase::Failed { .. }),
                ..
            } = event
            {
                seen.push(phase);
            }
        }
        seen
    }

    #[tokio::test]
    async fn stop_keeps_the_detached_note_when_the_session_goes_away_first() {
        let mut wrong = Vec::new();
        for mode in [OnCancel::EndSession, OnCancel::Disconnect, OnCancel::Fail] {
            for timed_out in [false, true] {
                let case = format!("{mode:?}, cancel times out: {timed_out}");
                let h = harness(true).await;
                h.script.thread_busy.store(true, SeqCst);
                h.script.cancel_times_out.store(timed_out, SeqCst);
                h.on_cancel(mode);
                let mut events = h.broker.events();
                let reply = h.operate(Operation::Stop).await.map_err(|e| e.to_string());
                let phase = h.agent.phase();
                let crashed = crashes(&mut events);
                if !reply.as_ref().is_err_and(|e| {
                    e.starts_with("agent stopped, but ") && e.contains("may still be running")
                }) || phase != Phase::Stopped
                    || h.agent.error().is_none()
                    || !crashed.is_empty()
                {
                    wrong.push(format!(
                        "{case}: reply {reply:?}, phase {phase:?}, crashes {crashed:?}"
                    ));
                }
                h.broker.shutdown().await.expect("shutdown");
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn a_repeated_stop_after_a_failed_settlement_keeps_the_detached_note() {
        let h = harness(true).await;
        h.script.thread_busy.store(true, SeqCst);
        h.script.failing_suspends.store(1, SeqCst);
        let first = h
            .operate(Operation::Stop)
            .await
            .expect_err("settlement failed")
            .to_string();
        assert!(first.contains("did not settle"), "{first}");
        assert_eq!(h.agent.phase(), Phase::Ended);
        assert_eq!(
            h.agent.error().as_deref(),
            Some(first.as_str()),
            "ls and the feed show why the agent is Ended and that the turn may still run"
        );
        assert!(first.contains("repeat `agentctl stop`"), "{first}");
        assert!(first.contains("may still be running"), "{first}");
        let result = h.operate(Operation::Stop).await;
        assert_detached(result, "agent stopped, but ", "repeat");
        assert_eq!(h.agent.phase(), Phase::Stopped);
        assert!(
            h.agent
                .error()
                .is_some_and(|e| e.contains("may still be running"))
        );
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn restart_reports_infinity_work_that_may_still_be_running() {
        let h = harness(true).await;
        h.script.thread_busy.store(true, SeqCst);
        let result = h.operate(Operation::Restart).await;
        let error = assert_detached(result, "agent restarted, but ", "restart");
        assert_eq!(h.count(|s| &s.opens), 2, "restart still reattaches");
        assert_eq!(
            h.agent.error().as_deref(),
            error.strip_prefix("agent restarted, but "),
            "ls and the feed show the note"
        );
        h.inject(SessionEvent::TurnEnded {
            outcome: TurnOutcome::Completed,
        });
        h.wait(|p| *p == Phase::Idle).await;
        assert_eq!(h.agent.error(), None, "the note clears when the turn ends");
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn restart_from_needs_recovery_stays_fresh_after_a_failed_settlement() {
        let h = harness(false).await;
        h.inject(SessionEvent::ResumeFailed {
            reason: "injected rejected token".into(),
        });
        h.wait(|p| matches!(p, Phase::NeedsRecovery { .. })).await;
        let mail = h.send("queued during recovery").await;
        h.barrier().await;
        h.sql("UPDATE deliveries SET state='offered', incarnation=1")
            .await;
        h.sql(REFUSE_SETTLE).await;
        assert!(matches!(h.state(mail).await, DeliveryState::Offered { .. }));
        let failure = h
            .operate(Operation::Restart)
            .await
            .expect_err("settlement failed")
            .to_string();
        assert_eq!(h.agent.phase(), Phase::Ended);
        assert!(failure.contains("repeat `agentctl restart`"), "{failure}");
        assert_eq!(h.agent.error(), Some(failure));
        h.sql("DROP TRIGGER temp.refuse_settle").await;
        h.operate(Operation::Restart)
            .await
            .expect("a repeated restart settles");
        assert_eq!(h.count(|s| &s.opens), 2);
        assert_eq!(
            h.count(|s| &s.resumed),
            0,
            "the rejected token is not resumed again"
        );
        assert!(!matches!(h.agent.phase(), Phase::NeedsRecovery { .. }));
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn retire_works_from_stopped_failed_and_needs_recovery() {
        for from in ["stopped", "failed", "needs-recovery"] {
            let h = harness(false).await;
            let mut events = h.broker.events();
            match from {
                "stopped" => h.operate(Operation::Stop).await.expect("stop"),
                "failed" => {
                    h.sql("CREATE TEMP TRIGGER refuse_offer BEFORE UPDATE OF state ON deliveries WHEN NEW.state='offered' BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;").await;
                    h.send("kills the actor").await;
                    tokio::time::timeout(Duration::from_secs(5), h.agent.commands.closed())
                        .await
                        .expect("actor dies");
                    assert!(matches!(h.agent.phase(), Phase::Failed { .. }));
                    h.sql("DROP TRIGGER temp.refuse_offer").await;
                }
                _ => {
                    h.inject(SessionEvent::ResumeFailed {
                        reason: "injected rejected token".into(),
                    });
                    h.wait(|p| matches!(p, Phase::NeedsRecovery { .. })).await;
                }
            }
            let queued = h.send("never delivered").await;
            h.operate(Operation::Retire)
                .await
                .unwrap_or_else(|e| panic!("retire from {from}: {e}"));
            let current = h.broker.handle(h.agent.id).await;
            assert!(
                current.is_none(),
                "{from}: a respawned actor is removed too"
            );
            h.assert_retired(Some(queued), from).await;
            assert_eq!(h.count(|s| &s.opens), 1, "{from}: retire opens nothing");
            let mut retired = false;
            while let Ok(event) = events.try_recv() {
                retired |= matches!(event, Event::Retired { agent } if agent == h.agent.id);
            }
            assert!(retired, "{from}: the feed hears the removal");
            h.broker.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test]
    async fn retire_deletes_the_credential_copy_before_it_commits() {
        let h = harness_built(false, CANCEL, true).await;
        let state = state_root(&h._dir);
        let home = crate::sandbox::AgentPaths::new(&state, h.agent.id).home;
        let copy = home.join(CREDENTIAL);
        let cli = copy.parent().expect("CLI dir");
        let outside = state.with_file_name("outside");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir(&outside).expect("outside");
        std::fs::write(outside.join("auth.json"), b"{}").expect("target");
        // The agent could have linked its CLI directory elsewhere. Nothing is
        // deleted through a link, so the retire fails before it commits.
        std::os::unix::fs::symlink(&outside, cli).expect("link");
        let error = h
            .operate(Operation::Retire)
            .await
            .expect_err("a removal that is refused");
        assert!(
            error.to_string().contains("repeat `agentctl retire`"),
            "{error}"
        );
        assert!(
            !h.room.retired(h.agent.id).await.expect("retired"),
            "a failed removal leaves the agent unretired"
        );
        assert!(outside.join("auth.json").exists(), "the target survives");
        std::fs::remove_file(cli).expect("unlink");
        std::fs::create_dir(cli).expect("CLI dir");
        std::fs::write(&copy, b"{\"token\":1}").expect("copy");
        h.operate(Operation::Retire).await.expect("repeated retire");
        assert!(!copy.exists(), "the copy is gone");
        h.assert_retired(None, "credentials").await;
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn retire_keeps_a_credential_refresh_the_host_never_received() {
        let h = harness_built(false, CANCEL, true).await;
        let state = state_root(&h._dir);
        let home = crate::sandbox::AgentPaths::new(&state, h.agent.id).home;
        let copy = home.join(CREDENTIAL);
        let human = state.with_file_name("human");
        let host = human.join(CREDENTIAL);
        let cli = host.parent().expect("CLI dir");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(cli).expect("host CLI dir");
        std::fs::write(&host, b"{\"token\":0}").expect("host credential");
        h.broker
            .credentials
            .lock()
            .expect("credential lock")
            .launch(
                h.agent.id,
                "claude",
                &[(host.clone(), CREDENTIAL.into())],
                &home,
            )
            .expect("launch");
        std::fs::write(&copy, b"{\"token\":1}").expect("refresh");
        let moved = human.join("moved");
        std::fs::rename(cli, &moved).expect("unavailable host");
        let error = h
            .operate(Operation::Retire)
            .await
            .expect_err("a retire that kept a copy says so");
        assert!(error.to_string().contains("agent retired, but "), "{error}");
        assert!(
            error.to_string().contains(&copy.display().to_string()),
            "{error}"
        );
        assert_eq!(
            std::fs::read(&copy).expect("kept copy"),
            b"{\"token\":1}",
            "the only refreshed credential survives"
        );
        h.assert_retired(None, "kept credential").await;
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn a_saved_verifier_restores_and_retires_but_its_name_cannot_be_spawned() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(
                dir.path().join("mail.db"),
                crate::mailroom::Config {
                    verifier_available: true,
                    ..Default::default()
                },
            )
            .await
            .expect("room"),
        );
        let mut kind = KindConfig::claude("unused-fixture");
        kind.isolation = crate::sandbox::Isolation::None;
        kind.idle_after = None;
        let broker =
            Broker::new(room.clone(), vec![kind.clone()], Default::default()).expect("broker");
        let a = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("register");
        let message = |from, act| NewMessage {
            from,
            to: a,
            body: Body::new("work").expect("body"),
            reply_to: None,
            client_key: ClientKey(ulid::Ulid::new().to_string()),
            act,
            task: None,
        };
        let budget = crate::swarm::Budget {
            tokens: None,
            wall_secs: 600,
            messages: 5,
            hops: 1,
        };
        let (_, request) = room
            .accept(message(
                Sender::Human,
                crate::swarm::Act::Request {
                    deliverable: "work".into(),
                    budget,
                    stakes: crate::swarm::Stakes::Normal,
                },
            ))
            .await
            .expect("task");
        let (_, claim) = room
            .accept(message(
                Sender::Agent(a),
                crate::swarm::Act::Report {
                    task: request.task().expect("task"),
                    outcome: crate::swarm::Outcome::Infeasible {
                        evidence: crate::swarm::Evidence {
                            tried: "every mirror".into(),
                            obstacle: "the archive is gone".into(),
                            reproduction: None,
                        },
                    },
                    verdict: None,
                },
            ))
            .await
            .expect("claim");
        let verifier = room
            .open_verification(
                claim.id(),
                crate::mailroom::VerifierStart {
                    spec: spec(&kind.name, dir.path(), "checker"),
                    budget,
                    verifier_family: "gpt".into(),
                    claimant_family: "claude".into(),
                    snapshot_clean: true,
                },
            )
            .await
            .expect("open verification")
            .verifier
            .expect("verifier");
        room.set_stopped(verifier, true).await.expect("stop flag");

        let taken = broker.spawn(spec(&kind.name, dir.path(), "checker")).await;
        assert!(
            matches!(taken, Err(ref e) if e.to_string().contains("belongs to a verifier")),
            "{:?}",
            taken.map(|h| h.id)
        );

        // A daemon start restores every saved spec, the verifier's included.
        let saved = room.specs().await.expect("specs");
        let (_, saved) = saved
            .into_iter()
            .find(|(id, _)| *id == verifier)
            .expect("the verifier's spec is saved");
        let handle = broker.restore(saved).await.expect("restore");
        assert_eq!(handle.id, verifier);
        let mut phases = handle.phases();
        tokio::time::timeout(
            Duration::from_secs(5),
            phases.wait_for(|p| *p == Phase::Stopped),
        )
        .await
        .expect("phase timeout")
        .expect("phase channel");
        broker.retire(verifier).await.expect("retire");
        assert!(room.retired(verifier).await.expect("retired"));
        let reused = room
            .register(AgentName::parse("checker").expect("name"))
            .await
            .expect("a retired verifier frees its name");
        assert_ne!(reused, verifier);
        broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn a_retired_agent_stays_retired_and_its_name_is_free() {
        let h = harness(false).await;
        h.operate(Operation::Retire).await.expect("retire");
        let again = h
            .operate(Operation::Retire)
            .await
            .expect_err("second retire");
        assert!(again.to_string().contains("is retired"), "{again}");
        let restart = h.operate(Operation::Restart).await.expect_err("restart");
        assert!(restart.to_string().contains("is retired"), "{restart}");
        let unknown = h
            .broker
            .retire(AgentId::new())
            .await
            .expect_err("unknown agent");
        assert!(unknown.to_string().contains("not found"), "{unknown}");
        let error = h
            .room
            .resolve(Recipient::Id(h.agent.id))
            .await
            .expect_err("mail to a retired agent");
        assert!(error.to_string().contains("is retired"), "{error}");
        let next = h
            .broker
            .spawn(spec("claude", h._dir.path(), "a"))
            .await
            .expect("the name is free");
        assert_ne!(next.id, h.agent.id);
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn retire_reports_infinity_work_that_may_still_be_running() {
        let h = harness(true).await;
        h.script.thread_busy.store(true, SeqCst);
        let result = h.operate(Operation::Retire).await;
        assert_detached(result, "agent retired, but ", "retire");
        assert_eq!(h.count(|s| &s.cancels), 1, "retire interrupts the thread");
        h.assert_retired(None, "infinity").await;
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn a_retire_waiting_on_a_slow_cancel_does_not_block_sends_or_lookups() {
        let h = harness_with(false, Duration::from_secs(30)).await;
        let session = h
            .script
            .latest
            .lock()
            .expect("bug: script lock")
            .clone()
            .expect("a's session");
        let other = h
            .broker
            .spawn(spec("claude", h._dir.path(), "b"))
            .await
            .expect("spawn b");
        h.busy().await;
        let retire = h.broker.retire(h.agent.id);
        tokio::pin!(retire);
        queue(&h, retire.as_mut()).await;
        let unblocked = async {
            for to in [other.id, h.agent.id] {
                h.broker
                    .send(NewMessage {
                        from: Sender::Human,
                        to,
                        body: Body::new("during retire").expect("body"),
                        reply_to: None,
                        client_key: ClientKey(ulid::Ulid::new().to_string()),
                        act: crate::swarm::Act::Inform,
                        task: None,
                    })
                    .await
                    .expect("send");
            }
            assert!(h.broker.handle(other.id).await.is_some());
            assert!(h.broker.handle(h.agent.id).await.is_some());
        };
        tokio::select! {
            biased;
            result = retire.as_mut() => panic!("retire finished before its cancel: {result:?}"),
            done = tokio::time::timeout(Duration::from_secs(5), unblocked) => {
                done.expect("sends and lookups must not wait for the retire");
            }
        }
        session
            .send(SessionEvent::TurnEnded {
                outcome: TurnOutcome::Cancelled,
            })
            .expect("a's session is listening");
        tokio::time::timeout(Duration::from_secs(5), retire)
            .await
            .expect("retire settles once the turn ends")
            .expect("retire");
        h.assert_retired(None, "slow cancel").await;
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn spawn_after_shutdown_is_refused() {
        let h = harness(false).await;
        h.broker.shutdown().await.expect("shutdown");
        let late = h
            .broker
            .spawn(AgentSpec {
                name: AgentName::parse("late").expect("name"),
                kind: "claude".into(),
                cwd: h._dir.path().into(),
                workspace: None,
                model: None,
                permissions: Default::default(),
                idle_after: None,
            })
            .await;
        assert!(
            matches!(late, Err(Error::Closed)),
            "{:?}",
            late.map(|h| h.id)
        );
    }

    #[tokio::test]
    async fn operate_respawns_an_actor_that_panicked() {
        let h = harness(false).await;
        h.script.panic_on_submit.store(true, SeqCst);
        h.send("panics the actor").await;
        tokio::time::timeout(Duration::from_secs(5), h.agent.commands.closed())
            .await
            .expect("actor dies");
        h.script.panic_on_submit.store(false, SeqCst);
        h.operate(Operation::Restart)
            .await
            .expect("restart respawns a panicked actor");
        let current = h.broker.handle(h.agent.id).await.expect("handle");
        assert!(!current.commands.same_channel(&h.agent.commands));
        h.broker.shutdown().await.expect("shutdown");
    }

    const WARNED: &str = "may still be running";
    const REFUSE_STOPPED: &str = "CREATE TEMP TRIGGER refuse_stopped BEFORE UPDATE OF stopped ON agent_specs BEGIN SELECT RAISE(ABORT,'injected flag failure'); END;";
    const REFUSE_FORGET: &str = "CREATE TEMP TRIGGER refuse_forget BEFORE UPDATE OF resume ON actor_state WHEN NEW.resume IS NULL BEGIN SELECT RAISE(ABORT,'injected forget failure'); END;";
    const REFUSE_REOPEN: &str = "CREATE TEMP TRIGGER refuse_reopen BEFORE UPDATE OF incarnation ON actor_state BEGIN SELECT RAISE(ABORT,'injected reopen failure'); END;";
    const REFUSE_OFFER: &str = "CREATE TEMP TRIGGER refuse_offer BEFORE UPDATE OF state ON deliveries WHEN NEW.state='offered' BEGIN SELECT RAISE(ABORT,'injected offer failure'); END;";

    #[derive(Clone, Copy, Debug)]
    enum Fault {
        SessionEndsAndSuspendFails,
        SuspendFails,
        SettleFails,
        SetStoppedFails,
        ForgetResumeFails,
        ReopenFails,
        OfferFails,
        Panic,
        RepeatedStop,
        RestartAfterStop,
    }

    /// Records every step at which the operator would lose the warning.
    struct Probe<'a> {
        h: &'a Harness,
        fault: Fault,
        wrong: &'a mut Vec<String>,
    }
    impl Probe<'_> {
        async fn shown(&mut self, step: &str) {
            let error = self.h.current().await.error();
            if !error.as_ref().is_some_and(|e| e.contains(WARNED)) {
                self.wrong
                    .push(format!("{:?}, {step}: error {error:?}", self.fault));
            }
        }
        async fn replied(&mut self, step: &str, reply: Result<()>) {
            let reply = reply.map_err(|e| e.to_string());
            if !reply.as_ref().is_err_and(|e| e.contains(WARNED)) {
                self.wrong
                    .push(format!("{:?}, {step}: reply {reply:?}", self.fault));
            }
            self.shown(step).await;
        }
        async fn operate(&mut self, step: &str, op: Operation) {
            let reply = self.h.operate(op).await;
            self.replied(step, reply).await;
        }
    }

    impl Harness {
        async fn current(&self) -> AgentHandle {
            self.broker.handle(self.agent.id).await.expect("handle")
        }
        async fn dies(&self) {
            let current = self.current().await;
            tokio::time::timeout(Duration::from_secs(5), current.commands.closed())
                .await
                .expect("actor dies");
        }
        /// Restarts onto a thread that is still busy, leaving the warning on a running agent.
        async fn restarted_busy(&self, p: &mut Probe<'_>) {
            p.operate("restart onto the busy thread", Operation::Restart)
                .await;
            let mut phases = self.current().await.phases();
            tokio::time::timeout(
                Duration::from_secs(5),
                phases.wait_for(|p| *p == Phase::Busy),
            )
            .await
            .expect("phase timeout")
            .expect("phase channel");
        }
    }

    #[tokio::test]
    async fn a_caller_that_lost_the_respawn_race_still_gets_the_warning() {
        let h = harness(true).await;
        h.script.thread_busy.store(true, SeqCst);
        h.busy().await;
        let stopped = h.operate(Operation::Stop).await;
        assert!(stopped.is_err_and(|e| e.to_string().contains(WARNED)));
        h.operate(Operation::Restart)
            .await
            .expect_err("the thread is still busy");
        let mut phases = h.current().await.phases();
        tokio::time::timeout(
            Duration::from_secs(5),
            phases.wait_for(|p| *p == Phase::Busy),
        )
        .await
        .expect("phase timeout")
        .expect("phase channel");
        h.script.panic_on_submit.store(true, SeqCst);
        h.script.accepts_busy_mail.store(true, SeqCst);
        h.send("panics the actor").await;
        h.dies().await;
        h.script.panic_on_submit.store(false, SeqCst);
        h.script.accepts_busy_mail.store(false, SeqCst);

        // The first caller respawns the dead actor. The second waited for that,
        // then asks the new actor, which dies on the cancel before it answers.
        h.on_cancel(OnCancel::Panic);
        let id = h.agent.id;
        let (first, second) = tokio::join!(
            h.broker.operate(id, Operation::Restart),
            h.broker.operate(id, Operation::Restart),
        );
        for (caller, reply) in [("first", first), ("second", second)] {
            let reply = reply.expect_err("the turn is unacknowledged").to_string();
            assert!(reply.contains(WARNED), "{caller}: {reply}");
        }
        assert!(
            h.script.cancels.load(SeqCst) >= 2,
            "the second caller reached the respawned actor"
        );
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn no_single_fault_loses_the_unacknowledged_turn_warning() {
        use Fault::*;
        let mut wrong = Vec::new();
        for fault in [
            SessionEndsAndSuspendFails,
            SuspendFails,
            SettleFails,
            SetStoppedFails,
            ForgetResumeFails,
            ReopenFails,
            OfferFails,
            Panic,
            RepeatedStop,
            RestartAfterStop,
        ] {
            let h = harness(true).await;
            h.script.thread_busy.store(true, SeqCst);
            h.busy().await;
            let mut p = Probe {
                h: &h,
                fault,
                wrong: &mut wrong,
            };
            match fault {
                SessionEndsAndSuspendFails => {
                    h.on_cancel(OnCancel::EndSession);
                    h.script.failing_suspends.store(1, SeqCst);
                    p.operate("stop", Operation::Stop).await;
                    h.dies().await;
                    p.shown("dead actor").await;
                    p.operate("stop respawns", Operation::Stop).await;
                }
                SuspendFails => {
                    h.script.failing_suspends.store(1, SeqCst);
                    p.operate("unsettled stop", Operation::Stop).await;
                    p.operate("repeated stop", Operation::Stop).await;
                }
                SettleFails => {
                    h.sql(REFUSE_SETTLE).await;
                    p.operate("unsettled stop", Operation::Stop).await;
                    h.sql("DROP TRIGGER temp.refuse_settle").await;
                    p.operate("repeated stop", Operation::Stop).await;
                }
                SetStoppedFails => {
                    p.operate("stop", Operation::Stop).await;
                    h.sql(REFUSE_STOPPED).await;
                    p.operate("restart without the flag", Operation::Restart)
                        .await;
                    h.sql("DROP TRIGGER temp.refuse_stopped").await;
                    p.operate("repeated restart", Operation::Restart).await;
                }
                ForgetResumeFails => {
                    p.operate("stop", Operation::Stop).await;
                    h.restarted_busy(&mut p).await;
                    h.inject(SessionEvent::ResumeFailed {
                        reason: "injected rejected token".into(),
                    });
                    let mut phases = h.current().await.phases();
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        phases.wait_for(|p| matches!(p, Phase::NeedsRecovery { .. })),
                    )
                    .await
                    .expect("phase timeout")
                    .expect("phase channel");
                    p.shown("needs recovery").await;
                    h.sql(REFUSE_FORGET).await;
                    p.operate("stop that cannot forget", Operation::Stop).await;
                    h.dies().await;
                    p.shown("dead actor").await;
                    h.sql("DROP TRIGGER temp.refuse_forget").await;
                    p.operate("stop respawns", Operation::Stop).await;
                }
                ReopenFails => {
                    p.operate("stop", Operation::Stop).await;
                    h.sql(REFUSE_REOPEN).await;
                    p.operate("restart that cannot reopen", Operation::Restart)
                        .await;
                    h.dies().await;
                    p.shown("dead actor").await;
                    h.sql("DROP TRIGGER temp.refuse_reopen").await;
                    p.operate("restart respawns", Operation::Restart).await;
                }
                OfferFails => {
                    p.operate("stop", Operation::Stop).await;
                    h.restarted_busy(&mut p).await;
                    h.sql(REFUSE_OFFER).await;
                    h.script.accepts_busy_mail.store(true, SeqCst);
                    h.send("cannot be offered").await;
                    h.dies().await;
                    p.shown("dead actor").await;
                    h.sql("DROP TRIGGER temp.refuse_offer").await;
                    p.operate("stop respawns", Operation::Stop).await;
                }
                Panic => {
                    p.operate("stop", Operation::Stop).await;
                    h.restarted_busy(&mut p).await;
                    h.script.panic_on_submit.store(true, SeqCst);
                    h.script.accepts_busy_mail.store(true, SeqCst);
                    h.send("panics the actor").await;
                    h.dies().await;
                    h.script.panic_on_submit.store(false, SeqCst);
                    p.shown("dead actor").await;
                    p.operate("restart respawns", Operation::Restart).await;
                }
                RepeatedStop => {
                    p.operate("stop", Operation::Stop).await;
                    p.operate("repeated stop", Operation::Stop).await;
                }
                RestartAfterStop => {
                    p.operate("stop", Operation::Stop).await;
                    p.operate("restart", Operation::Restart).await;
                }
            }
            h.broker.shutdown().await.expect("shutdown");
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn a_cancel_acknowledged_by_a_new_completion_is_not_a_turn_end() {
        let h = harness(true).await;
        h.script.thread_busy.store(true, SeqCst);
        h.busy().await;
        let stop = h.broker.operate(h.agent.id, Operation::Stop);
        tokio::pin!(stop);
        queue(&h, stop.as_mut()).await;
        h.inject(SessionEvent::CancelComplete);
        h.inject(SessionEvent::Ended {
            reason: "injected session end".into(),
        });
        let result = tokio::time::timeout(CANCEL * 20, stop)
            .await
            .expect("halt deadline");
        assert_detached(result, "agent stopped, but ", "cancel complete, then end");
        assert!(h.agent.error().is_some_and(|e| e.contains(WARNED)));
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn a_restarted_actor_that_panicked_while_busy_resumes_its_session() {
        let h = harness(false).await;
        h.busy().await;
        h.script.panic_on_submit.store(true, SeqCst);
        h.script.accepts_busy_mail.store(true, SeqCst);
        h.send("panics the actor").await;
        h.dies().await;
        assert_eq!(h.agent.phase(), Phase::Busy, "the actor died busy");
        h.script.panic_on_submit.store(false, SeqCst);
        h.operate(Operation::Restart).await.expect("restart");
        let mut phases = h.current().await.phases();
        tokio::time::timeout(
            Duration::from_secs(5),
            phases.wait_for(|p| matches!(p, Phase::Idle | Phase::Busy)),
        )
        .await
        .expect("phase timeout")
        .expect("phase channel");
        assert_eq!(h.count(|s| &s.opens), 2);
        assert_eq!(
            h.count(|s| &s.resumed),
            1,
            "a respawned busy actor resumes, as a live busy restart does"
        );
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn a_daemon_that_goes_unavailable_during_a_halt_is_not_a_crash() {
        let h = harness(true).await;
        h.script.thread_busy.store(true, SeqCst);
        h.busy().await;
        let mut events = h.broker.events();
        let stop = h.broker.operate(h.agent.id, Operation::Stop);
        tokio::pin!(stop);
        queue(&h, stop.as_mut()).await;
        h.inject(SessionEvent::Unavailable {
            reason: "injected unavailable daemon".into(),
        });
        let result = tokio::time::timeout(CANCEL * 20, stop)
            .await
            .expect("halt deadline");
        assert_detached(result, "agent stopped, but ", "unavailable");
        assert_eq!(crashes(&mut events), vec![], "a halt is not a crash");
        assert_eq!(h.agent.phase(), Phase::Stopped);
        assert_eq!(h.count(|s| &s.opens), 1, "a stop reopens nothing");
        h.broker.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn the_feed_shows_the_warning_when_it_changes_without_a_phase_change() {
        let h = harness(true).await;
        let (bound, _) = mpsc::unbounded_channel();
        let api = crate::surfaces::ControlApi {
            broker: h.broker.clone(),
            mailroom: h.room.clone(),
            bindings: Arc::new(crate::surfaces::Bindings::new(
                h._dir.path().join("mcp"),
                "agentd".into(),
                bound,
            )),
            control_path: h._dir.path().join("control"),
            ui_url: None,
        };
        let mut states = h.broker.event_hub().state();
        let mut cache = HashMap::new();
        let mut rows = async || {
            let mut rows = Vec::new();
            while let Ok(event) = states.try_recv() {
                if let Some(row) = crate::surfaces::events::state(&api, event, &mut cache)
                    .await
                    .expect("row")
                    && row["event"] == "agent"
                {
                    rows.push((row["phase"].clone(), row["error"].clone()));
                }
            }
            rows
        };
        // The daemon thread runs work agentd did not submit, so the phase stays Idle.
        h.script.thread_busy.store(true, SeqCst);
        rows().await;
        let stop = h.broker.operate(h.agent.id, Operation::Stop);
        tokio::pin!(stop);
        queue(&h, stop.as_mut()).await;
        assert_eq!(h.agent.phase(), Phase::Idle);
        let pending = rows().await;
        assert!(
            pending.iter().any(|(phase, error)| phase == "idle"
                && error.as_str().is_some_and(|e| e.contains(WARNED))),
            "the warning reaches the feed while the phase is unchanged: {pending:?}"
        );
        tokio::time::timeout(CANCEL * 20, stop)
            .await
            .expect("halt deadline")
            .expect_err("unacknowledged");
        let stopped = rows().await;
        assert!(
            stopped
                .last()
                .is_some_and(|(phase, error)| phase == "stopped"
                    && error.as_str().is_some_and(|e| e.contains(WARNED))),
            "{stopped:?}"
        );
        h.script.thread_busy.store(false, SeqCst);
        h.operate(Operation::Restart)
            .await
            .expect("the reattached thread is idle, which clears the warning");
        assert_eq!(h.agent.error(), None);
        let cleared = rows().await;
        assert!(
            cleared.last().is_some_and(|(_, error)| error.is_null()),
            "{cleared:?}"
        );
        h.broker.shutdown().await.expect("shutdown");
    }
}

#[cfg(test)]
mod expiry_tests {
    use std::sync::atomic::Ordering::SeqCst;

    use super::*;
    use crate::swarm::{Act, Budget, Evidence, Outcome, Stakes, TaskState};

    #[tokio::test]
    async fn the_expiry_loop_wakes_for_an_earlier_deadline_opened_while_it_sleeps() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("mailroom"),
        );
        let broker =
            Arc::new(Broker::new(room.clone(), vec![], Default::default()).expect("broker"));
        let a = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("register");
        let request = |wall_secs| NewMessage {
            from: Sender::Human,
            to: a,
            body: Body::new("work").expect("body"),
            reply_to: None,
            client_key: ClientKey(MessageId::new().to_string()),
            act: Act::Request {
                deliverable: "work".into(),
                budget: Budget {
                    tokens: None,
                    wall_secs,
                    messages: 1,
                    hops: 1,
                },
                stakes: Stakes::Normal,
            },
            task: None,
        };
        let (_, late) = room.accept(request(3600)).await.expect("late task");
        let before = room.jobs_run.load(SeqCst);
        let sweeper = tokio::spawn({
            let broker = broker.clone();
            async move { broker.expire_tasks().await }
        });
        // The first sweep has started, so it sees only the late task and
        // would sleep for an hour unless the next accept wakes it.
        while room.jobs_run.load(SeqCst) == before {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (_, early) = room.accept(request(1)).await.expect("early task");
        let early = early.task().expect("task");
        tokio::time::timeout(Duration::from_secs(10), async {
            while room.task(early).await.expect("task").state != TaskState::Expired {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the early task expires within its one second budget");
        let late = room.task(late.task().expect("task")).await.expect("late");
        assert_eq!(late.state, TaskState::Open);
        sweeper.abort();
        match sweeper.await {
            Err(error) if error.is_cancelled() => {}
            other => panic!("expiry loop ended unexpectedly: {other:?}"),
        }
        drop(broker);
        Arc::try_unwrap(room)
            .map_err(|_| "mailroom still shared")
            .expect("sole owner")
            .close()
            .await
            .expect("close");
    }

    #[tokio::test]
    async fn a_failing_expiry_is_retried_once_a_second() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("mail.db");
        let room = Mailroom::open(&path, Default::default())
            .await
            .expect("mailroom");
        let a = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("register");
        room.accept(NewMessage {
            from: Sender::Human,
            to: a,
            body: Body::new("work").expect("body"),
            reply_to: None,
            client_key: ClientKey(MessageId::new().to_string()),
            act: Act::Request {
                deliverable: "work".into(),
                budget: Budget {
                    tokens: None,
                    wall_secs: 1,
                    messages: 1,
                    hops: 1,
                },
                stakes: Stakes::Normal,
            },
            task: None,
        })
        .await
        .expect("task");
        room.close().await.expect("close");
        // Every expiry now updates its task and then fails on the notice.
        rusqlite::Connection::open(&path)
            .expect("db")
            .execute_batch(
                "CREATE TRIGGER no_mail BEFORE INSERT ON messages \
                 BEGIN SELECT RAISE(ABORT,'injected'); END;",
            )
            .expect("trigger");
        let room = Arc::new(
            Mailroom::open(&path, Default::default())
                .await
                .expect("mailroom"),
        );
        let broker =
            Arc::new(Broker::new(room.clone(), vec![], Default::default()).expect("broker"));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let before = room.jobs_run.load(SeqCst);
        let sweeper = tokio::spawn({
            let broker = broker.clone();
            async move { broker.expire_tasks().await }
        });
        while room.jobs_run.load(SeqCst) == before {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // The first sweep has started. Half the retry interval later, no
        // second one has.
        tokio::time::sleep(EXPIRY_RETRY / 2).await;
        let sweeps = room.jobs_run.load(SeqCst) - before;
        assert_eq!(sweeps, 1, "one failed sweep, then a wait");
        sweeper.abort();
        match sweeper.await {
            Err(error) if error.is_cancelled() => {}
            other => panic!("expiry loop ended unexpectedly: {other:?}"),
        }
        drop(broker);
        Arc::try_unwrap(room)
            .map_err(|_| "mailroom still shared")
            .expect("sole owner")
            .close()
            .await
            .expect("close");
    }

    #[tokio::test]
    async fn the_expiry_loop_wakes_for_a_task_reopened_without_mail() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("mailroom"),
        );
        let broker =
            Arc::new(Broker::new(room.clone(), vec![], Default::default()).expect("broker"));
        let a = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("register");
        let message = |from, act| NewMessage {
            from,
            to: a,
            body: Body::new("work").expect("body"),
            reply_to: None,
            client_key: ClientKey(MessageId::new().to_string()),
            act,
            task: None,
        };
        let (_, request) = room
            .accept(message(
                Sender::Human,
                Act::Request {
                    deliverable: "work".into(),
                    budget: Budget {
                        tokens: None,
                        wall_secs: 1,
                        messages: 1,
                        hops: 1,
                    },
                    stakes: Stakes::Normal,
                },
            ))
            .await
            .expect("task");
        let task = request.task().expect("task");
        room.accept(message(
            Sender::Agent(a),
            Act::Report {
                task,
                outcome: Outcome::Infeasible {
                    evidence: Evidence {
                        tried: "every mirror".into(),
                        obstacle: "the archive is gone".into(),
                        reproduction: None,
                    },
                },
                verdict: None,
            },
        ))
        .await
        .expect("claim");
        // A retired owner gets no notice, so the reopen below stores no mail.
        room.retire(a).await.expect("retire");
        let before = room.jobs_run.load(SeqCst);
        let sweeper = tokio::spawn({
            let broker = broker.clone();
            async move { broker.expire_tasks().await }
        });
        // The first sweep has started. It sees no open task and would sleep
        // until the next change.
        while room.jobs_run.load(SeqCst) == before {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let notified = room
            .resolve_review(task, crate::mailroom::ReviewDecision::Refute)
            .await
            .expect("refute");
        assert_eq!(notified, None);
        tokio::time::timeout(Duration::from_secs(10), async {
            while room.task(task).await.expect("task").state != TaskState::Expired {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the reopened task expires within its one second budget");
        sweeper.abort();
        match sweeper.await {
            Err(error) if error.is_cancelled() => {}
            other => panic!("expiry loop ended unexpectedly: {other:?}"),
        }
        drop(broker);
        Arc::try_unwrap(room)
            .map_err(|_| "mailroom still shared")
            .expect("sole owner")
            .close()
            .await
            .expect("close");
    }
}
