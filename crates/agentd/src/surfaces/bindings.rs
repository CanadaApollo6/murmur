use super::Result;
use crate::model::*;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[derive(Clone)]
pub(super) struct AuthenticatedSender(Sender);
impl AuthenticatedSender {
    pub(super) fn sender(&self) -> Sender {
        self.0.clone()
    }
}
pub(super) struct SessionBinding {
    _guard: OwnedRwLockReadGuard<HashMap<BindingKey, Binding>>,
    binding: Binding,
}
impl SessionBinding {
    pub(super) fn sender(&self) -> AuthenticatedSender {
        AuthenticatedSender(Sender::Agent(self.binding.agent))
    }
    pub(super) fn identity(&self) -> &SessionIdentity {
        &self.binding
    }
    pub(super) fn key(&self, session: &str, id: &Value) -> ClientKey {
        ClientKey(format!("mcp:{}:{session}:{id}", self.binding.incarnation.0))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    pub agent: AgentId,
    pub incarnation: Incarnation,
}
type Binding = SessionIdentity;
#[derive(Hash, PartialEq, Eq)]
enum BindingKey {
    Token(String),
    Agent(AgentId),
    Infinity {
        socket: PathBuf,
        root: infinity_protocol::ThreadRef,
    },
}

pub struct BoundListener {
    pub socket: super::Socket,
    pub identity: SessionIdentity,
    pub cancel: tokio_util::sync::CancellationToken,
}
struct AgentListeners {
    cancel: tokio_util::sync::CancellationToken,
    paths: Vec<super::SocketIdentity>,
    egress: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for AgentListeners {
    fn drop(&mut self) {
        self.cancel.cancel();
        for path in &self.paths {
            path.unlink();
        }
        if let Some(task) = &self.egress {
            task.abort();
        }
    }
}
pub struct Bindings {
    tokens: std::sync::Arc<RwLock<HashMap<BindingKey, Binding>>>,
    pub(super) rap_calls: tokio::sync::Mutex<HashMap<uuid::Uuid, super::rap::Execution>>,
    pub(super) rap_delivery_slots: tokio::sync::Semaphore,
    socket: PathBuf,
    executable: PathBuf,
    listeners: tokio::sync::Mutex<HashMap<AgentId, AgentListeners>>,
    bound: tokio::sync::mpsc::UnboundedSender<BoundListener>,
}
impl Bindings {
    pub fn new(
        socket: PathBuf,
        executable: PathBuf,
        bound: tokio::sync::mpsc::UnboundedSender<BoundListener>,
    ) -> Self {
        Self {
            tokens: Default::default(),
            rap_calls: Default::default(),
            rap_delivery_slots: tokio::sync::Semaphore::new(32),
            listeners: Default::default(),
            bound,
            socket,
            executable,
        }
    }
    #[cfg(test)]
    pub(crate) async fn hold_for_test(&self) -> impl Send + use<> {
        self.tokens.clone().read_owned().await
    }
    pub(crate) async fn mint(&self, agent: AgentId, incarnation: Incarnation) -> Result<Value> {
        let token = random_namespace()?;
        let mut tokens = self.tokens.write().await;
        tokens.retain(|_, b| b.agent != agent);
        tokens.insert(
            BindingKey::Token(token.clone()),
            Binding { agent, incarnation },
        );
        Ok(json!({"name":"agentd", "command":self.executable,
            "args":["mcp", "--socket", self.socket],
            "env":[{"name":"AGENTD_MCP_TOKEN", "value":token}]}))
    }
    pub(crate) async fn bind(
        &self,
        identity: SessionIdentity,
        paths: &crate::sandbox::AgentPaths,
        cfg: &crate::sandbox::SandboxConfig,
        sb: &crate::sandbox::KindSandbox,
        spec: &crate::supervisor::AgentSpec,
        events: tokio::sync::broadcast::Sender<crate::supervisor::Event>,
    ) -> Result<Value> {
        let SessionIdentity { agent, incarnation } = identity;
        self.revoke(agent).await;
        let io = std::sync::Arc::new(crate::private_fs::Dir::open(&paths.io, true)?);
        let cleanup = io.clone();
        tokio::task::spawn_blocking(move || {
            cleanup.clear("mcp.sock".as_ref())?;
            cleanup.clear("egress.sock".as_ref())
        })
        .await??;
        let socket = super::Socket::bind_private(io.clone(), "mcp.sock")?;
        let mut socket_paths = vec![socket.identity()];
        let cancel = tokio_util::sync::CancellationToken::new();
        let egress = if cfg.network == crate::sandbox::Network::Proxy {
            let socket = super::Socket::bind_private(io, "egress.sock")?;
            socket_paths.push(socket.identity());
            let policy = crate::egress::Policy {
                hosts: sb.egress.clone(),
                ports: cfg.egress_ports.clone(),
            };
            let name = spec.name.as_str().to_owned();
            Some(tokio::spawn(async move {
                if let Err(error) = crate::egress::serve(socket, policy, agent, name, events).await
                {
                    tracing::error!(%error,"egress listener stopped");
                }
            }))
        } else {
            None
        };
        self.tokens
            .write()
            .await
            .insert(BindingKey::Agent(agent), Binding { agent, incarnation });
        self.listeners.lock().await.insert(
            agent,
            AgentListeners {
                cancel: cancel.clone(),
                paths: socket_paths,
                egress,
            },
        );
        if self
            .bound
            .send(BoundListener {
                socket,
                identity: SessionIdentity { agent, incarnation },
                cancel,
            })
            .is_err()
        {
            self.revoke(agent).await;
            return Err("MCP listener receiver closed".into());
        }
        Ok(
            json!({"name":"agentd","command":"/run/agentd/bin/agentd","args":["mcp","--socket","/run/agentd/io/mcp.sock"],"env":[]}),
        )
    }
    pub(super) async fn authenticate_agent(
        &self,
        identity: &SessionIdentity,
    ) -> Option<SessionBinding> {
        let guard = self.tokens.clone().read_owned().await;
        let binding = guard.get(&BindingKey::Agent(identity.agent))?.clone();
        if binding.incarnation != identity.incarnation {
            return None;
        }
        Some(SessionBinding {
            _guard: guard,
            binding,
        })
    }
    pub(super) fn human() -> AuthenticatedSender {
        AuthenticatedSender(Sender::Human)
    }
    pub(super) async fn authenticate_mcp(&self, token: &str) -> Option<SessionBinding> {
        let guard = self.tokens.clone().read_owned().await;
        let binding = guard.get(&BindingKey::Token(token.to_owned()))?.clone();
        Some(SessionBinding {
            _guard: guard,
            binding,
        })
    }
    pub(crate) async fn bind_infinity(
        &self,
        socket: &std::path::Path,
        root: infinity_protocol::ThreadRef,
        agent: AgentId,
        incarnation: Incarnation,
    ) {
        self.revoke(agent).await;
        self.tokens.write().await.insert(
            BindingKey::Infinity {
                socket: socket.into(),
                root,
            },
            Binding { agent, incarnation },
        );
    }

    pub(super) async fn authenticate_rap(
        &self,
        socket: &std::path::Path,
        endpoint: &str,
        record: &infinity_protocol::RapClaimRecord,
    ) -> Option<SessionBinding> {
        if record.endpoint != endpoint || record.operation != "send_message" {
            return None;
        }
        let root = record.thread_stack.first()?;
        if root.remote.is_some() {
            return None;
        }
        let guard = self.tokens.clone().read_owned().await;
        let binding = guard
            .get(&BindingKey::Infinity {
                socket: socket.into(),
                root: root.clone(),
            })?
            .clone();
        Some(SessionBinding {
            _guard: guard,
            binding,
        })
    }

    pub(crate) async fn revoke(&self, agent: AgentId) {
        self.tokens.write().await.retain(|_, b| b.agent != agent);
        self.rap_calls
            .lock()
            .await
            .retain(|_, call| call.retain_on_revoke(agent));
        self.listeners.lock().await.remove(&agent);
    }
}

pub(super) fn random_namespace() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| "OS random source unavailable")?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
