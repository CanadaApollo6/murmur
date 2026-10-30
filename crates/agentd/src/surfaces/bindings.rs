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
    _guard: RwLockReadGuard<'a, HashMap<String, Binding>>,
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
struct Binding {
    agent: AgentId,
    incarnation: Incarnation,
}
pub struct Bindings {
    tokens: RwLock<HashMap<String, Binding>>,
    socket: PathBuf,
    executable: PathBuf,
}
impl Bindings {
    pub fn new(socket: PathBuf, executable: PathBuf) -> Self {
        Self {
            tokens: RwLock::new(HashMap::new()),
            socket,
            executable,
        }
    }
    pub(crate) async fn mint(&self, agent: AgentId, incarnation: Incarnation) -> Result<Value> {
        let token = random_namespace()?;
        let mut tokens = self.tokens.write().await;
        tokens.retain(|_, b| b.agent != agent);
        tokens.insert(token.clone(), Binding { agent, incarnation });
        Ok(json!({"name":"agentd", "command":self.executable,
            "args":["mcp", "--socket", self.socket],
            "env":[{"name":"AGENTD_MCP_TOKEN", "value":token}]}))
    }
    pub(super) fn human() -> AuthenticatedSender {
        AuthenticatedSender(Sender::Human)
    }
    pub(super) async fn authenticate_mcp(&self, token: &str) -> Option<SessionBinding<'_>> {
        let guard = self.tokens.read().await;
        let binding = guard.get(token)?.clone();
        Some(SessionBinding {
            _guard: guard,
            binding,
        })
    }
    pub(crate) async fn revoke(&self, agent: AgentId) {
        self.tokens.write().await.retain(|_, b| b.agent != agent);
    }
}

pub(super) fn random_namespace() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| "OS random source unavailable")?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
