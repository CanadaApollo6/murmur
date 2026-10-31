use std::collections::{HashMap, VecDeque};

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
    #[serde(default)]
    boot_id: String,
    #[serde(default)]
    needs_interrupt: bool,
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
    thread_busy: bool,
    pending: HashMap<MessageId, bool>,
    receipt_deadline: Option<Instant>,
    choices: HashMap<PermissionId, Choice>,
    cancel_deadline: Option<Instant>,
    ended: bool,
    boot_id: String,
    needs_interrupt: bool,
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
            .map_err(|e| AdapterError::Unavailable(e.to_string()))?
            .map_err(|e| AdapterError::Unavailable(format!("Infinity daemon socket: {e}")))?;
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
            thread_busy: false,
            pending: HashMap::new(),
            receipt_deadline: None,
            choices: HashMap::new(),
            cancel_deadline: None,
            ended: false,
            boot_id: String::new(),
            needs_interrupt: false,
        });
        tokio::time::timeout(timeout, session.initialize(&req))
            .await
            .map_err(protocol)??;
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
                DaemonMessage::Welcome {
                    sessions, boot_id, ..
                } => {
                    self.boot_id = boot_id;
                    break sessions;
                }
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
            let Some(session) = sessions.get(&saved.root) else {
                return Err(AdapterError::ResumeFailed(
                    "Infinity root thread does not exist".into(),
                ));
            };
            if session.status == infinity_protocol::SessionStatus::Archived {
                return Err(AdapterError::ResumeFailed(
                    "Infinity root thread is archived".into(),
                ));
            }
            if session.remote.is_some() {
                return Err(protocol("Infinity root thread is remote"));
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
                if saved
                    .as_ref()
                    .is_some_and(|s| s.needs_interrupt || s.boot_id != self.boot_id)
                    && self.thread_busy
                {
                    self.needs_interrupt = true;
                    self.cancel().await?;
                }
                return Ok(());
            }
        }
    }

    fn root_event(&self, thread: &Option<ThreadRef>) -> bool {
        thread.as_ref() == Some(&self.root)
    }

    fn started(&mut self) {
        let ids = self
            .pending
            .iter_mut()
            .filter_map(|(id, started)| {
                if *started {
                    None
                } else {
                    *started = true;
                    Some(*id)
                }
            })
            .collect::<Vec<_>>();
        if !ids.is_empty() {
            self.events.push_back(SessionEvent::TurnStarted { ids });
        }
    }

    fn finish_input(&mut self) {
        if self.thread_busy || !self.choices.is_empty() || !self.pending.is_empty() {
            return;
        }
        let outcome = if self.cancel_deadline.take().is_some() {
            TurnOutcome::Cancelled
        } else {
            TurnOutcome::Completed
        };
        self.events.push_back(SessionEvent::TurnEnded { outcome });
    }

    fn receive(&mut self, message: DaemonMessage) -> Result<(), AdapterError> {
        match message {
            DaemonMessage::InputCommitted {
                thread_id,
                dedup_id,
            } if self.root_event(&thread_id) => {
                if let Some(id) = self
                    .pending
                    .keys()
                    .find(|id| dedup_id == format!("peer-{id}"))
                    .copied()
                {
                    if !self
                        .pending
                        .remove(&id)
                        .expect("bug: matched missing receipt")
                    {
                        self.events
                            .push_back(SessionEvent::TurnStarted { ids: vec![id] });
                    }
                    self.events
                        .push_back(SessionEvent::Committed { ids: vec![id] });
                    if self.pending.is_empty() {
                        self.receipt_deadline = None;
                        self.finish_input();
                    }
                }
            }
            DaemonMessage::StartOutput { thread_id }
            | DaemonMessage::ThinkingStart { thread_id }
                if self.root_event(&thread_id) =>
            {
                self.thread_busy = true;
                self.needs_interrupt = false;
                // Attachment idle can precede this newer completion in the socket.
                // Receipt recovery applies at idle, never during valid inference.
                self.receipt_deadline = None;
                if self.cancel_deadline.take().is_some() {
                    self.choices.clear();
                    self.events.push_back(SessionEvent::CancelComplete);
                }
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
                self.needs_interrupt = false;
                self.receipt_deadline =
                    (!self.pending.is_empty()).then(|| Instant::now() + self.kind.request_timeout);
                self.finish_input();
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
            DaemonMessage::SessionsUpdated { sessions } => {
                if let Some(session) = sessions.get(&self.root) {
                    match session.status {
                        infinity_protocol::SessionStatus::Stopped => {
                            self.needs_interrupt = true;
                            return Err(protocol("Infinity root thread stopped"));
                        }
                        infinity_protocol::SessionStatus::Archived => {
                            return Err(AdapterError::ResumeFailed(
                                "Infinity root thread is archived".into(),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            DaemonMessage::Error { text, .. } if text.starts_with("Thread driver failed:") => {
                self.needs_interrupt = true;
                return Err(protocol(text));
            }
            DaemonMessage::Error { text, .. } if text.starts_with("Interrupt settlement ") => {
                self.needs_interrupt = true;
                return Err(AdapterError::Unavailable(text));
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
            boot_id: self.boot_id.clone(),
            needs_interrupt: self.needs_interrupt,
        })
    }
    fn resumable(&self) -> bool {
        true
    }
    fn busy(&self) -> bool {
        self.thread_busy || !self.choices.is_empty()
    }
    fn accepts_busy_mail(&self) -> bool {
        self.events.is_empty() && self.cancel_deadline.is_none()
    }
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError> {
        if self.ended {
            return Err(AdapterError::Ended);
        }
        if turn.items.is_empty() {
            return Err(protocol("Infinity submit requires mail"));
        }
        if !self.thread_busy && self.receipt_deadline.is_none() {
            self.receipt_deadline = Some(Instant::now() + self.kind.request_timeout);
        }
        for item in turn.items {
            let (envelope, from, text) = match item {
                TurnItem::User(envelope) => {
                    let text = format!(
                        "<human-message id=\"{}\" from=\"human\">\n{}\n</human-message>\nThis is deferrable mail from your human operator. Reply to the human operator.",
                        envelope.id(),
                        envelope.body().as_str()
                    );
                    (envelope, "human".to_owned(), text)
                }
                TurnItem::Peer { envelope, rendered } => {
                    let Sender::Agent(from) = envelope.from() else {
                        return Err(protocol("peer message needs an agent sender"));
                    };
                    let from = format!("agent:{from}");
                    (envelope, from, rendered)
                }
            };
            self.pending.insert(envelope.id(), false);
            self.thread_busy = true;
            // The daemon adds `peer-` exactly once, for human and agent mail.
            self.send(ClientMessage::DeliverPeerMessage {
                thread_id: self.root.clone(),
                message_id: envelope.id().to_string(),
                from,
                text,
            })?;
        }
        Ok(())
    }
    async fn cancel(&mut self) -> Result<(), AdapterError> {
        let active = self.busy() || !self.pending.is_empty();
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
        let choice = self.choices.get(&id).ok_or(AdapterError::StaleChoice)?;
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
            let deadline = self.cancel_deadline.or(self.receipt_deadline);
            let incoming = tokio::select! {
                biased;
                () = async { match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                } } => {
                    self.ended = true;
                    return Some(SessionEvent::Unavailable {
                        reason: "Infinity receipt or interrupt deadline exceeded".into(),
                    });
                },
                msg = self.rx.recv() => msg.unwrap_or_else(|| Err("Infinity daemon connection closed".into())),
            };
            let needs_interrupt = self.needs_interrupt;
            let result = incoming.map_err(protocol).and_then(|msg| self.receive(msg));
            if needs_interrupt != self.needs_interrupt {
                self.events.push_back(SessionEvent::ResumeUpdated);
            }
            if let Err(error) = result {
                self.ended = true;
                self.events.push_back(match error {
                    AdapterError::ResumeFailed(reason) => SessionEvent::ResumeFailed { reason },
                    AdapterError::Unavailable(reason) => SessionEvent::Unavailable { reason },
                    _ => SessionEvent::Ended {
                        reason: error.to_string(),
                    },
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
