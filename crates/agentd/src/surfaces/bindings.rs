use super::Result;
use crate::model::*;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{RwLock, RwLockReadGuard};

#[derive(Clone)]
pub(super) struct AuthenticatedSender(Sender);
impl AuthenticatedSender {
    pub(super) fn sender(&self) -> Sender {
        self.0.clone()
    }
}
pub(super) struct SessionBinding<'a> {
    _guard: RwLockReadGuard<'a, HashMap<BindingKey, Binding>>,
    binding: Binding,
}
impl SessionBinding<'_> {
    pub(super) fn sender(&self) -> AuthenticatedSender {
        AuthenticatedSender(Sender::Agent(self.binding.agent))
    }
    pub(super) fn key(&self, session: &str, id: &Value) -> ClientKey {
        ClientKey(format!("mcp:{}:{session}:{id}", self.binding.incarnation.0))
    }
}

#[derive(Clone)]
pub struct SessionIdentity {
    pub agent: AgentId,
    pub incarnation: Incarnation,
}
type Binding = SessionIdentity;
#[derive(Hash, PartialEq, Eq)]
enum BindingKey {
    Token(String),
    Agent(AgentId),
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
    tokens: RwLock<HashMap<BindingKey, Binding>>,
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
            tokens: RwLock::new(HashMap::new()),
            listeners: Default::default(),
            bound,
            socket,
            executable,
        }
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
        super::private_dir(&paths.io)?;
        let socket = super::Socket::bind(&paths.io.join("mcp.sock"))?;
        let mut socket_paths = vec![socket.identity()];
        let cancel = tokio_util::sync::CancellationToken::new();
        let egress = if cfg.network == crate::sandbox::Network::Proxy {
            let socket = super::Socket::bind(&paths.io.join("egress.sock"))?;
            socket_paths.push(socket.identity());
            let policy = crate::egress::Policy {
                hosts: sb.egress.iter().chain(&spec.egress).cloned().collect(),
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
    ) -> Option<SessionBinding<'_>> {
        let guard = self.tokens.read().await;
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
    pub(super) async fn authenticate_mcp(&self, token: &str) -> Option<SessionBinding<'_>> {
        let guard = self.tokens.read().await;
        let binding = guard.get(&BindingKey::Token(token.to_owned()))?.clone();
        Some(SessionBinding {
            _guard: guard,
            binding,
        })
    }
    pub(crate) async fn revoke(&self, agent: AgentId) {
        self.tokens.write().await.retain(|_, b| b.agent != agent);
        self.listeners.lock().await.remove(&agent);
    }
}

pub(super) fn random_namespace() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| "OS random source unavailable")?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
