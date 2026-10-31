//! Scriptable length-delimited Infinity daemon with durable peer dedup across connections.
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use infinity_protocol::{ClientMessage, DaemonMessage, SessionInfo, SessionStatus, ThreadRef};
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::codec::Framed;

pub fn root() -> ThreadRef {
    ThreadRef::local("fake-root".into())
}
pub fn thread() -> Option<ThreadRef> {
    Some(root())
}

pub struct Request {
    pub message: ClientMessage,
    reply: mpsc::UnboundedSender<Option<DaemonMessage>>,
}
impl Request {
    pub fn emit(&self, message: DaemonMessage) {
        self.reply
            .send(Some(message))
            .expect("fake daemon connected");
    }
    pub fn die(&self) {
        self.reply.send(None).expect("fake daemon connected");
    }
}

#[derive(Default)]
pub struct State {
    pub boot_id: String,
    pub created: bool,
    pub manual_replay: bool,
    pub fail_resume: bool,
    pub missing: bool,
    pub durable: HashSet<String>,
}

pub struct FakeDaemon {
    pub state: Arc<Mutex<State>>,
    requests: mpsc::UnboundedReceiver<Request>,
    task: JoinHandle<()>,
}
impl FakeDaemon {
    pub fn bind(path: &Path) -> Self {
        let listener = UnixListener::bind(path).expect("bind fake Infinity daemon");
        let state = Arc::new(Mutex::new(State::default()));
        let saved = state.clone();
        let (tx, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.expect("accept fake Infinity client");
                        let state = saved.clone();
                        let requests = tx.clone();
                        clients.spawn(async move {
                            let mut wire = Framed::new(stream, infinity_protocol::length_delimited_codec());
                            let sessions = {
                                let s = state.lock().expect("state");
                                if s.created && !s.missing {
                                    HashMap::from([(root(), SessionInfo {
                                        title: None, last_updated: String::new(), total_tokens_used: 0,
                                        status: SessionStatus::Idle, threads: vec![], remote: None,
                                    })])
                                } else { HashMap::new() }
                            };
                            let welcome = DaemonMessage::Welcome {
                                boot_id: state.lock().expect("state").boot_id.clone(),
                                sessions, available_models: vec![], default_model_name: "fake".into(),
                                default_context_window: 1000, provider_name: "fake".into(), remotes: vec![],
                            };
                            wire.send(serde_json::to_vec(&welcome).expect("welcome JSON").into()).await.expect("welcome");
                            let (reply, mut replies) = mpsc::unbounded_channel();
                            loop {
                                let output = tokio::select! {
                                    output = replies.recv() => match output { Some(Some(msg)) => Some(msg), _ => break },
                                    frame = wire.next() => {
                                        let Some(frame) = frame else { break };
                                        let message: ClientMessage = serde_json::from_slice(&frame.expect("frame")).expect("client JSON");
                                        let automatic = match &message {
                                            ClientMessage::CreateSession { .. } => {
                                                state.lock().expect("state").created = true;
                                                Some(connected())
                                            }
                                            ClientMessage::Connect { .. } => {
                                                if state.lock().expect("state").fail_resume {
                                                    Some(DaemonMessage::Error { thread_id: thread(), text: "resume refused".into() })
                                                } else {
                                                    if !state.lock().expect("state").manual_replay {
                                                        reply.send(Some(replay())).expect("replay");
                                                    }
                                                    Some(connected())
                                                }
                                            }
                                            ClientMessage::DeliverPeerMessage { message_id, .. }
                                                if state.lock().expect("state").durable.contains(message_id) => {
                                                reply.send(Some(idle())).expect("dedup idle");
                                                Some(receipt(format!("peer-{message_id}")))
                                            }
                                            _ => None,
                                        };
                                        if requests.send(Request { message, reply: reply.clone() }).is_err() { break }
                                        automatic
                                    }
                                };
                                if let Some(message) = output
                                    && wire.send(serde_json::to_vec(&message).expect("daemon JSON").into()).await.is_err()
                                { break }
                            }
                        });
                    }
                    result = clients.join_next(), if !clients.is_empty() => {
                        result.expect("client result").expect("fake client task");
                    }
                }
            }
        });
        Self {
            state,
            requests,
            task,
        }
    }
    pub async fn next(&mut self) -> Request {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.requests.recv())
            .await
            .expect("fake request timeout")
            .expect("fake request")
    }
}
impl Drop for FakeDaemon {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn connected() -> DaemonMessage {
    DaemonMessage::Connected {
        root_thread_id: root(),
        thread_id: root(),
        model_name: "fake".into(),
        context_window: 1000,
        title: None,
        total_tokens_used: 0,
        provider_id: "fake".into(),
    }
}
pub fn receipt(dedup_id: String) -> DaemonMessage {
    DaemonMessage::InputCommitted {
        thread_id: thread(),
        dedup_id,
    }
}
pub fn start() -> DaemonMessage {
    DaemonMessage::StartOutput {
        thread_id: thread(),
    }
}
pub fn round_done() -> DaemonMessage {
    DaemonMessage::ResponseDone {
        thread_id: thread(),
        token_usage: None,
    }
}

pub fn idle() -> DaemonMessage {
    DaemonMessage::ThreadIdle {
        thread_id: thread(),
    }
}

fn replay() -> DaemonMessage {
    DaemonMessage::Replay {
        history: vec![],
        pending_choices: vec![],
        views: Default::default(),
        in_progress: false,
        idle: true,
    }
}
