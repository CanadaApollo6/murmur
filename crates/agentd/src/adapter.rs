use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;

use crate::model::*;
use crate::supervisor::AgentSpec;

#[derive(Clone, Debug)]
pub struct KindConfig {
    pub wire: Wire,
    pub model: Option<String>,
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub mode: String,
    pub workspace_write_mode: Option<String>,
    pub full_access_mode: Option<String>,
    /// Additional MCP servers; the actor appends its authenticated broker entry.
    pub mcp_servers: Vec<serde_json::Value>,
    pub sandbox_args: SandboxArgs,
    pub idle_after: Option<Duration>,
    pub request_timeout: Duration,
    /// Maximum wait for prompt completion after cancel, before crashing the session.
    pub cancel_timeout: Duration,
    /// Busy inactivity limit while mail waits. Zero disables cancellation.
    pub stall_timeout: Duration,
}

#[derive(Clone, Debug)]
pub enum Wire {
    Acp,
    Infinity { socket: std::path::PathBuf },
}

impl KindConfig {
    pub fn infinity(socket: impl Into<std::path::PathBuf>) -> Self {
        let mut kind = Self::new("infinity", String::new(), "default");
        kind.wire = Wire::Infinity {
            socket: socket.into(),
        };
        kind.idle_after = None;
        kind
    }
    pub fn claude(command: impl Into<String>) -> Self {
        Self::new("claude", command.into(), "default")
    }
    pub fn codex(command: impl Into<String>) -> Self {
        Self::new("codex", command.into(), "read-only")
    }
    fn new(name: &str, command: String, mode: &str) -> Self {
        Self {
            wire: Wire::Acp,
            model: None,
            name: name.into(),
            command,
            args: Vec::new(),
            env: BTreeMap::new(),
            mode: mode.into(),
            workspace_write_mode: Some(
                if name == "codex" {
                    "workspace-write"
                } else {
                    "acceptEdits"
                }
                .into(),
            ),
            full_access_mode: Some(
                if name == "codex" {
                    "agent-full-access"
                } else {
                    "bypassPermissions"
                }
                .into(),
            ),
            mcp_servers: Vec::new(),
            sandbox_args: SandboxArgs::default(),
            idle_after: Some(Duration::from_secs(600)),
            request_timeout: Duration::from_secs(60),
            cancel_timeout: Duration::from_secs(2),
            stall_timeout: Duration::from_secs(600),
        }
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SandboxArgs {
    pub read_only: Vec<String>,
    pub workspace_write: Vec<String>,
    pub full_access: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ResumeToken(String);

impl ResumeToken {
    pub(crate) fn encode(value: &impl serde::Serialize) -> Self {
        Self(serde_json::to_string(value).expect("bug: resume token serialization failed"))
    }
    pub(crate) fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(&self.0)
    }
}

#[async_trait]
pub(crate) trait Adapter: Send + Sync {
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError>;
}

pub(crate) struct OpenRequest<'a> {
    pub agent: AgentId,
    pub spec: &'a AgentSpec,
    pub resume: Option<&'a ResumeToken>,
    pub incarnation: Incarnation,
    pub mcp_server: Option<serde_json::Value>,
}

#[async_trait]
pub(crate) trait Session: Send {
    fn resume_token(&self) -> ResumeToken;
    fn resumable(&self) -> bool;
    fn busy(&self) -> bool {
        false
    }
    /// Busy delivery must not overtake buffered turn-end evidence.
    fn accepts_busy_mail(&self) -> bool {
        false
    }
    fn permission_key(&self, _id: PermissionId) -> Option<&str> {
        None
    }
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError>;
    async fn cancel(&mut self) -> Result<(), AdapterError>;
    async fn answer_permission(&mut self, id: PermissionId, key: &str) -> Result<(), AdapterError>;
    /// Cancellation safe: the actor selects this against commands and deadlines.
    async fn next_event(&mut self) -> Option<SessionEvent>;
    async fn suspend(self: Box<Self>) -> Result<ResumeToken, AdapterError>;
}

pub(crate) struct Turn {
    pub items: Vec<TurnItem>,
}
pub(crate) enum TurnItem {
    User(Envelope),
    Peer {
        envelope: Envelope,
        rendered: String,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum SessionEvent {
    ResumeUpdated,
    CancelComplete,
    CancelTimedOut,
    TurnStarted { ids: Vec<MessageId> },
    Committed { ids: Vec<MessageId> },
    Waiting { ids: Vec<MessageId> },
    Activity(String),
    PermissionRequested(PermissionRequest),
    PermissionsRestored(Vec<PermissionRequest>),
    PermissionSettled { id: PermissionId, key: String },
    TurnEnded { outcome: TurnOutcome },
    Ended { reason: String },
    ResumeFailed { reason: String },
    Unavailable { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnOutcome {
    Completed,
    Cancelled,
    Refused,
    MaxTokens,
    Failed(String),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdapterError {
    #[error("agent does not support resume")]
    ResumeUnsupported,
    #[error("resume failed: {0}")]
    ResumeFailed(String),
    #[error("daemon unavailable: {0}")]
    Unavailable(String),
    #[error("stale Infinity choice")]
    StaleChoice,
    #[error("spawn failed: {0}")]
    Spawn(String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("session ended")]
    Ended,
}
