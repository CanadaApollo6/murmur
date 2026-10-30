use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use infinity_protocol::{ClientMessage, DaemonMessage, ModelRef, ThreadRef};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::codec::Framed;

use crate::adapter::*;
use crate::model::*;

pub(crate) struct InfinityAdapter(pub KindConfig);

#[derive(serde::Serialize, serde::Deserialize)]
struct InfinityResume {
    kind: String,
    socket: std::path::PathBuf,
    root: ThreadRef,
}

struct Flight {
    id: MessageId,
    human_text: Option<String>,
    echoed: bool,
    started: bool,
    committed: bool,
    receipt_deadline: Option<Instant>,
}

struct Choice {
    wire_id: infinity_protocol::ChoiceId,
    count: usize,
}

struct InfinitySession {
    kind: KindConfig,
    root: ThreadRef,
    agent: AgentId,
    incarnation: Incarnation,
    connection: JoinHandle<()>,
    tx: mpsc::UnboundedSender<ClientMessage>,
    rx: mpsc::Receiver<Result<DaemonMessage, String>>,
    events: VecDeque<SessionEvent>,
    queue: VecDeque<TurnItem>,
    flight: Option<Flight>,
    thread_busy: bool,
    peers: HashSet<MessageId>,
    peer_deadline: Option<Instant>,
    choices: HashMap<PermissionId, Choice>,
    cancel_deadline: Option<Instant>,
    ended: bool,
}

fn protocol(error: impl std::fmt::Display) -> AdapterError {
    AdapterError::Protocol(error.to_string())
}

#[async_trait]
impl Adapter for InfinityAdapter {
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError> {
        let Wire::Infinity { socket } = &self.0.wire else {
            return Err(protocol("Infinity adapter requires Infinity wire"));
        };
        let stream = tokio::time::timeout(self.0.request_timeout, UnixStream::connect(socket))
            .await
            .map_err(|e| AdapterError::Spawn(e.to_string()))?
            .map_err(|e| AdapterError::Spawn(format!("Infinity daemon socket: {e}")))?;
        let (tx, mut outgoing) = mpsc::unbounded_channel();
        let (incoming, rx) = mpsc::channel(64);
        let timeout = self.0.request_timeout;
        let connection = tokio::spawn(async move {
            let mut framed = Framed::new(stream, infinity_protocol::length_delimited_codec());
            let reason = loop {
                tokio::select! {
                    message = outgoing.recv() => {
                        let Some(message) = message else { return };
                        let bytes = match serde_json::to_vec(&message) {
                            Ok(bytes) => bytes,
                            Err(error) => break error.to_string(),
                        };
                        match tokio::time::timeout(timeout, framed.send(bytes.into())).await {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => break error.to_string(),
                            Err(error) => break error.to_string(),
                        }
                    }
                    frame = framed.next() => {
                        let message = match frame {
                            Some(Ok(bytes)) => serde_json::from_slice(&bytes).map_err(|e| e.to_string()),
                            Some(Err(error)) => break error.to_string(),
                            None => break "socket EOF".into(),
                        };
                        if incoming.send(message).await.is_err() { return }
                    }
                }
            };
            if incoming
                .send(Err(format!("Infinity daemon gone: {reason}")))
                .await
                .is_err()
            {
                tracing::debug!("Infinity session already dropped");
            }
        });
        let mut session = Box::new(InfinitySession {
            kind: self.0.clone(),
            root: ThreadRef::local("opening".into()),
            agent: req.agent,
            incarnation: req.incarnation,
            connection,
            tx,
            rx,
            events: VecDeque::new(),
            queue: VecDeque::new(),
            flight: None,
            thread_busy: false,
            peers: HashSet::new(),
            peer_deadline: None,
            choices: HashMap::new(),
            cancel_deadline: None,
            ended: false,
        });
        if let Err(error) = tokio::time::timeout(timeout, session.initialize(&req))
            .await
            .map_err(protocol)
            .and_then(|r| r)
        {
            return Err(if req.resume.is_some() {
                AdapterError::ResumeFailed(error.to_string())
            } else {
                error
            });
        }
        Ok(session)
    }
}

impl InfinitySession {
    fn send(&self, message: ClientMessage) -> Result<(), AdapterError> {
        self.tx.send(message).map_err(|_| AdapterError::Ended)
    }

    async fn initialize(&mut self, req: &OpenRequest<'_>) -> Result<(), AdapterError> {
        let saved = req
            .resume
            .map(ResumeToken::decode::<InfinityResume>)
            .transpose()
            .map_err(protocol)?;
        let sessions = loop {
            match self
                .rx
                .recv()
                .await
                .ok_or(AdapterError::Ended)?
                .map_err(protocol)?
            {
                DaemonMessage::Welcome { sessions, .. } => break sessions,
                DaemonMessage::Error { text, .. } => return Err(protocol(text)),
                _ => {}
            }
        };
        if let Some(saved) = &saved {
            let Wire::Infinity { socket } = &self.kind.wire else {
                unreachable!()
            };
            if saved.kind != self.kind.name
                || &saved.socket != socket
                || saved.root.remote.is_some()
            {
                return Err(protocol("Infinity resume token kind or daemon mismatch"));
            }
            if !sessions.get(&saved.root).is_some_and(|session| {
                session.status != infinity_protocol::SessionStatus::Archived
                    && session.remote.is_none()
            }) {
                return Err(protocol("Infinity root thread is missing or archived"));
            }
            self.send(ClientMessage::Connect {
                root_thread_id: saved.root.clone(),
                thread_id: None,
                keeps_session_alive: true,
            })?;
        } else {
            let model = req
                .spec
                .model
                .as_deref()
                .map(|model| {
                    let (provider, model) = model
                        .split_once('/')
                        .filter(|(p, m)| !p.is_empty() && !m.is_empty())
                        .ok_or_else(|| protocol("Infinity model must be provider/model"))?;
                    Ok(ModelRef {
                        provider_id: provider.into(),
                        model_id: model.into(),
                    })
                })
                .transpose()?;
            self.send(ClientMessage::CreateSession {
                cwd: req.spec.cwd.clone(),
                location: None,
                model,
                keeps_session_alive: true,
            })?;
        }
        loop {
            match self
                .rx
                .recv()
                .await
                .ok_or(AdapterError::Ended)?
                .map_err(protocol)?
            {
                DaemonMessage::Connected {
                    root_thread_id,
                    thread_id,
                    ..
                } => {
                    if root_thread_id != thread_id
                        || root_thread_id.remote.is_some()
                        || saved.as_ref().is_some_and(|s| s.root != root_thread_id)
                    {
                        return Err(protocol("Infinity connected to the wrong root thread"));
                    }
                    self.root = root_thread_id;
                    if saved.is_none() {
                        return Ok(());
                    }
                    break;
                }
                DaemonMessage::Error { text, .. } => return Err(protocol(text)),
                _ => {}
            }
        }
        loop {
            let message = self
                .rx
                .recv()
                .await
                .ok_or(AdapterError::Ended)?
                .map_err(protocol)?;
            let replay = matches!(message, DaemonMessage::Replay { .. });
            self.receive(message)?;
            if replay {
                return Ok(());
            }
        }
    }

    fn root_event(&self, thread: &Option<ThreadRef>) -> bool {
        thread.as_ref() == Some(&self.root)
    }

    fn started(&mut self) {
        if let Some(flight) = &mut self.flight
            && !flight.started
        {
            flight.started = true;
            self.events.push_back(SessionEvent::TurnStarted {
                ids: vec![flight.id],
            });
        }
    }

    fn submit_next(&mut self) -> Result<(), AdapterError> {
        let Some(item) = self.queue.pop_front() else {
            return Ok(());
        };
        let (id, human_text, message) = match item {
            TurnItem::User(envelope) => {
                let text = envelope.body().as_str().to_owned();
                (
                    envelope.id(),
                    Some(text.clone()),
                    ClientMessage::UserInput {
                        thread_id: self.root.clone(),
                        text,
                    },
                )
            }
            TurnItem::Peer { envelope, rendered } => {
                let Sender::Agent(from) = envelope.from() else {
                    return Err(protocol("peer message needs an agent sender"));
                };
                // The daemon prefixes this raw ID with `peer-` exactly once.
                (
                    envelope.id(),
                    None,
                    ClientMessage::DeliverPeerMessage {
                        thread_id: self.root.clone(),
                        message_id: envelope.id().to_string(),
                        from: format!("agent:{from}"),
                        text: rendered,
                    },
                )
            }
        };
        self.flight = Some(Flight {
            id,
            human_text,
            echoed: false,
            started: false,
            committed: false,
            receipt_deadline: None,
        });
        self.thread_busy = true;
        self.send(message)
    }

    fn finish_input(&mut self) -> Result<(), AdapterError> {
        if self.thread_busy || !self.choices.is_empty() {
            return Ok(());
        }
        if self.flight.as_ref().is_some_and(|f| !f.committed) || !self.peers.is_empty() {
            return Ok(());
        }
        self.flight = None;
        if !self.queue.is_empty() && self.cancel_deadline.is_none() {
            return self.submit_next();
        }
        self.queue.clear();
        let outcome = if self.cancel_deadline.take().is_some() {
            TurnOutcome::Cancelled
        } else {
            TurnOutcome::Completed
        };
        self.events.push_back(SessionEvent::TurnEnded { outcome });
        Ok(())
    }

    fn receive(&mut self, message: DaemonMessage) -> Result<(), AdapterError> {
        match message {
            DaemonMessage::InputCommitted {
                thread_id,
                dedup_id,
            } if self.root_event(&thread_id) => {
                if let Some(id) = self
                    .peers
                    .iter()
                    .find(|id| dedup_id == format!("peer-{id}"))
                    .copied()
                {
                    self.peers.remove(&id);
                    self.events
                        .push_back(SessionEvent::TurnStarted { ids: vec![id] });
                    self.events
                        .push_back(SessionEvent::Committed { ids: vec![id] });
                    if self.peers.is_empty() {
                        self.peer_deadline = None;
                    }
                    return Ok(());
                }
                let matches = self.flight.as_ref().is_some_and(|f| {
                    !f.committed
                        && if f.human_text.is_some() {
                            f.echoed && !dedup_id.starts_with("peer-")
                        } else {
                            dedup_id == format!("peer-{}", f.id)
                        }
                });
                if matches {
                    self.started();
                    let flight = self
                        .flight
                        .as_mut()
                        .expect("bug: matched receipt without flight");
                    flight.committed = true;
                    flight.receipt_deadline = None;
                    self.events.push_back(SessionEvent::Committed {
                        ids: vec![flight.id],
                    });
                }
            }
            DaemonMessage::UserInputEcho { thread_id, text } if self.root_event(&thread_id) => {
                if let Some(flight) = &mut self.flight
                    && flight.human_text.as_ref() == Some(&text)
                {
                    flight.echoed = true;
                    self.started();
                }
            }
            DaemonMessage::StartOutput { thread_id }
            | DaemonMessage::ThinkingStart { thread_id }
                if self.root_event(&thread_id) =>
            {
                self.thread_busy = true;
                self.started()
            }
            DaemonMessage::ToolResult { thread_id, .. } if self.root_event(&thread_id) => {
                self.thread_busy = true;
                self.started();
            }
            DaemonMessage::ToolCall {
                thread_id, name, ..
            } if self.root_event(&thread_id) => {
                self.started();
                self.thread_busy = true;
                self.events.push_back(SessionEvent::Activity(name));
            }
            DaemonMessage::Info { thread_id, text } if self.root_event(&thread_id) => {
                self.events.push_back(SessionEvent::Activity(text));
            }
            DaemonMessage::TextChunk { thread_id, chunk }
            | DaemonMessage::ThinkingChunk { thread_id, chunk }
                if self.root_event(&thread_id) =>
            {
                self.started();
                self.thread_busy = true;
                self.events.push_back(SessionEvent::Activity(chunk));
            }
            DaemonMessage::ThreadIdle { thread_id } if self.root_event(&thread_id) => {
                self.thread_busy = false;
                if !self.peers.is_empty() {
                    self.peer_deadline = Some(Instant::now() + self.kind.request_timeout);
                }
                if let Some(flight) = &mut self.flight {
                    flight.receipt_deadline =
                        (!flight.committed).then(|| Instant::now() + self.kind.request_timeout);
                }
                self.finish_input()?;
            }
            DaemonMessage::UserChoiceRequired {
                thread_id,
                id,
                prompt,
                choices,
                ..
            } if self.root_event(&thread_id) => {
                self.started();
                if self.cancel_deadline.is_some()
                    || self.choices.values().any(|choice| choice.wire_id == id)
                {
                    return Ok(());
                }
                let permission = PermissionId::new();
                let options = choices
                    .iter()
                    .enumerate()
                    .map(|(index, label)| PermissionOption {
                        key: index.to_string(),
                        label: label.clone(),
                        // Infinity supplies arbitrary choices, not authorization effects.
                        // UserChoice keeps rules and timeouts from guessing an answer.
                        effect: PermissionEffect::UserChoice,
                    })
                    .collect();
                self.choices.insert(
                    permission,
                    Choice {
                        wire_id: id,
                        count: choices.len(),
                    },
                );
                self.events
                    .push_back(SessionEvent::PermissionRequested(PermissionRequest {
                        id: permission,
                        agent: self.agent,
                        incarnation: self.incarnation,
                        tool: "infinity.user_choice".into(),
                        title: prompt.clone(),
                        detail: prompt,
                        options,
                    }));
            }
            DaemonMessage::UserChoiceComplete { choice_id } => {
                self.choices.retain(|_, choice| choice.wire_id != choice_id);
            }
            DaemonMessage::Replay {
                pending_choices,
                idle,
                ..
            } => {
                // Replay restores core liveness, never delivery evidence.
                self.thread_busy = !idle;
                for choice in pending_choices {
                    self.receive(choice)?;
                }
            }
            DaemonMessage::Error { thread_id, text }
                if thread_id.is_none() || self.root_event(&thread_id) =>
            {
                return Err(protocol(text));
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl Session for InfinitySession {
    fn resume_token(&self) -> ResumeToken {
        let Wire::Infinity { socket } = &self.kind.wire else {
            unreachable!()
        };
        ResumeToken::encode(&InfinityResume {
            kind: self.kind.name.clone(),
            socket: socket.clone(),
            root: self.root.clone(),
        })
    }
    fn resumable(&self) -> bool {
        true
    }
    fn busy(&self) -> bool {
        self.thread_busy || !self.choices.is_empty()
    }
    fn accepts_peer_mail(&self) -> bool {
        self.events.is_empty() && self.cancel_deadline.is_none()
    }
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError> {
        if self.ended {
            return Err(AdapterError::Ended);
        }
        if turn.items.is_empty() {
            return Err(protocol("Infinity submit requires mail"));
        }
        if self.thread_busy || self.flight.is_some() || !self.peers.is_empty() {
            for item in turn.items {
                let TurnItem::Peer { envelope, rendered } = item else {
                    return Err(protocol("only peer mail can enter a busy Infinity session"));
                };
                let Sender::Agent(from) = envelope.from() else {
                    return Err(protocol("peer message needs an agent sender"));
                };
                self.peers.insert(envelope.id());
                self.thread_busy = true;
                self.send(ClientMessage::DeliverPeerMessage {
                    thread_id: self.root.clone(),
                    message_id: envelope.id().to_string(),
                    from: format!("agent:{from}"),
                    text: rendered,
                })?;
            }
            return Ok(());
        }
        self.queue.extend(turn.items);
        self.submit_next()
    }
    async fn cancel(&mut self) -> Result<(), AdapterError> {
        let active = self.busy() || self.flight.is_some() || !self.peers.is_empty();
        self.events
            .retain(|e| !matches!(e, SessionEvent::PermissionRequested(_)));
        if self.cancel_deadline.is_none() {
            let deadline = Instant::now() + self.kind.cancel_timeout;
            if active {
                self.cancel_deadline = Some(deadline);
            }
            self.send(ClientMessage::Interrupt {
                thread_id: self.root.clone(),
            })?;
        }
        Ok(())
    }
    async fn answer_permission(&mut self, id: PermissionId, key: &str) -> Result<(), AdapterError> {
        let choice = self
            .choices
            .get(&id)
            .ok_or_else(|| protocol("stale Infinity choice"))?;
        let selected: usize = key.parse().map_err(protocol)?;
        if selected >= choice.count {
            return Err(protocol("invalid Infinity choice"));
        }
        self.send(ClientMessage::UserChoiceAnswered {
            choice_id: choice.wire_id.clone(),
            selected,
        })?;
        self.choices.remove(&id);
        Ok(())
    }
    async fn next_event(&mut self) -> Option<SessionEvent> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Some(event);
            }
            if self.ended {
                return None;
            }
            let deadline = self
                .cancel_deadline
                .or_else(|| self.flight.as_ref().and_then(|f| f.receipt_deadline))
                .or(self.peer_deadline);
            let incoming = tokio::select! {
                biased;
                () = async { match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                } } => Err("Infinity receipt or interrupt deadline exceeded".into()),
                msg = self.rx.recv() => msg.unwrap_or_else(|| Err("Infinity daemon connection closed".into())),
            };
            let result = incoming.map_err(protocol).and_then(|msg| self.receive(msg));
            if let Err(error) = result {
                self.ended = true;
                self.events.push_back(SessionEvent::Ended {
                    reason: error.to_string(),
                });
            }
        }
    }
    async fn suspend(self: Box<Self>) -> Result<ResumeToken, AdapterError> {
        // Dropping the socket detaches. The shared daemon and thread remain alive.
        Ok(self.resume_token())
    }
}

impl Drop for InfinitySession {
    fn drop(&mut self) {
        self.connection.abort();
    }
}
