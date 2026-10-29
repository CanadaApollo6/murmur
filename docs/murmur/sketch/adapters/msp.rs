//! Muse Code over MSP (`muse serve`, stdio). Muse has no ACP mode and no registry shim,
//! so it gets a native adapter: the proof that the contract is not ACP-shaped.
//!
//! Reference implementation to port: T3 Code's native `muse` driver on this machine,
//! which already speaks MSP. The mapping below is the contract; message names are
//! TODO until read from that driver.

use async_trait::async_trait;

use crate::adapter::{Adapter, AdapterError, DeliveryGate, OpenRequest, Session, SessionEvent, Turn};
use crate::model::{Incarnation, MessageId, PermissionId, ResumeToken};

/// Private token shape; encoded with `ResumeToken::encode`, read only here.
#[derive(serde::Serialize, serde::Deserialize)]
struct MspResume {
    r#ref: String,
}

pub struct MspAdapter {
    kind: crate::adapter::KindConfig,
    mcp: std::sync::Arc<crate::surfaces::McpEndpoints>,
}

#[async_trait]
impl Adapter for MspAdapter {
    fn delivery_gate(&self) -> DeliveryGate {
        DeliveryGate::AtTurnBoundary
    }

    /// ```text
    /// argv = ["muse", "serve"] ++ ["--approval-mode", "on-request"]   // prompts reach us
    /// TODO(msp): handshake; create session vs resume `ref` (`muse resume <ref>` exists
    ///            on the CLI; find the MSP equivalent in T3's muse driver)
    /// resume: token does not decode or Muse refuses the ref -> Err(ResumeFailed);
    ///         never a fresh session in its place
    /// TODO(msp): register the agentd MCP server (session param, or Muse MCP config file
    ///            written to a per-agent config dir passed by env)
    /// ```
    async fn open(&self, req: OpenRequest<'_>) -> Result<Box<dyn Session>, AdapterError> {
        todo!()
    }
}

pub struct MspSession {
    incarnation: Incarnation,
    /// Ids carried by the in-flight turn, for `TurnStarted` and `Committed`.
    in_flight: Vec<MessageId>,
    /* child + framed stdio */
}

#[async_trait]
impl Session for MspSession {
    /// `ResumeToken::encode(&MspResume { ref })`.
    fn resume_token(&self) -> ResumeToken {
        todo!()
    }
    fn resumable(&self) -> bool {
        todo!() // TODO(msp): true once the resume message is confirmed
    }
    /// TODO(msp): one user turn per `Turn`, items concatenated like the ACP adapter.
    /// Receipts as ACP: first turn output -> `TurnStarted{in_flight}`; turn completion ->
    /// `Committed{in_flight}` then `TurnEnded`.
    async fn submit(&mut self, turn: Turn) -> Result<(), AdapterError> {
        todo!()
    }
    async fn cancel(&mut self) -> Result<(), AdapterError> {
        todo!()
    }
    /// TODO(msp): approval request/response pair -> PermissionRequest (with
    /// `self.incarnation`) / answer.
    async fn answer_permission(&mut self, id: PermissionId, key: &str) -> Result<(), AdapterError> {
        todo!()
    }
    async fn next_event(&mut self) -> Option<SessionEvent> {
        todo!()
    }
    async fn suspend(self: Box<Self>) -> Result<ResumeToken, AdapterError> {
        todo!()
    }
}
