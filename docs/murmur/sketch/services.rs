//! Long-lived processes that are not agents: the Infinity daemon and the inference
//! server. Started in dependency order, health-checked, restarted with backoff.
//!
//! ```toml
//! [[service]]
//! name = "inference"
//! command = ["llama-server", "-m", "~/models/qwen3-coder-30b-a3b-q4_k_m.gguf",
//!            "--jinja", "-fa", "--parallel", "2", "-c", "65536", "-ctk", "q8_0", "-ctv", "q8_0",
//!            "--host", "127.0.0.1", "--port", "8091"]
//! ready = { http = "http://127.0.0.1:8091/health" }
//!
//! [[service]]
//! name = "infinity"
//! command = ["infinity", "daemon"]
//! ready = { line = "<DAEMON_READY_LINE>" }      # or attach if ~/.infinity/daemon.sock answers
//! depends_on = ["inference"]
//! ```
//!
//! `infinity` depends on `inference` because the daemon calls each provider's
//! `list_models` once at startup, and `infinity-provider-local` reads the honest
//! per-slot context window from the running server (`/props` or `max_model_len`).

use std::time::Duration;

#[derive(Debug, serde::Deserialize)]
pub struct ServiceSpec {
    pub name: String,
    pub command: Vec<String>,
    pub ready: ReadyProbe,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadyProbe {
    /// First stdout line equals this (Infinity's `DAEMON_READY_LINE`).
    Line(String),
    Http(String),
}

pub struct ServiceSupervisor {/* name -> (Child, epoch watch::Sender<u64>) */}

impl ServiceSupervisor {
    /// Topological start. A service that is already up and answering its probe (the
    /// user started `infinity` from a terminal, which runs `ensure_daemon_running`) is
    /// attached, not spawned; the broker then watches it but cannot restart it until it
    /// exits.
    pub async fn start_all(specs: &[ServiceSpec]) -> Result<Self, ServiceError> {
        todo!()
    }

    /// Bumped on every (re)start. `InfinityAdapter`'s `DaemonLink` watches the
    /// "infinity" epoch; open Infinity sessions end with `DaemonGone` when it changes.
    /// Their actors reopen under a new `Incarnation`; mail never `Committed`
    /// (`InputCommitted`) is re-offered, and core dedup (`peer-{MessageId}`) absorbs
    /// anything the old daemon had already persisted.
    pub fn epoch(&self, name: &str) -> tokio::sync::watch::Receiver<u64> {
        todo!()
    }

    /// Restart policy: always, backoff 1 s .. 60 s. Restarting `inference` does not
    /// restart `infinity`: the provider maps connection-refused to `ErrorClass::Transient`
    /// and core retries.
    pub async fn supervise(self) {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("{0} not ready after {1:?}")]
    NotReady(String, Duration),
    #[error("dependency cycle")]
    Cycle,
}
