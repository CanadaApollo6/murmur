use serde::{Deserialize, Serialize};

use crate::model::{AgentId, MessageId, id};
id!(TaskId);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Act {
    Request {
        deliverable: String,
        budget: Budget,
        stakes: Stakes,
    },
    #[default]
    Inform,
    Propose {
        scope: String,
    },
    Decide {
        proposal: MessageId,
        accepted: bool,
    },
    Report {
        task: TaskId,
        outcome: Outcome,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Done {
        artifacts: Vec<ArtifactRef>,
    },
    Partial {
        artifacts: Vec<ArtifactRef>,
        not_possible: String,
        evidence: Evidence,
    },
    Infeasible {
        evidence: Evidence,
    },
    Failed {
        tried: String,
    },
    Blocked {
        on: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub tried: String,
    pub obstacle: String,
    pub reproduction: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub locator: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub parent: Option<TaskId>,
    pub owner: AgentId,
    pub requested_by: crate::model::Sender,
    pub request: MessageId,
    pub deliverable: String,
    pub stakes: Stakes,
    pub budget: Budget,
    /// Includes child reservations, which are never refunded in v1.
    pub spent: Budget,
    pub state: TaskState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Open,
    Verifying { report: MessageId },
    Closed { outcome: Outcome, success: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Stakes {
    Low,
    Normal,
    High,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    /// None means token usage is unmetered. It cannot escape a finite parent cap.
    pub tokens: Option<u64>,
    pub wall_secs: u64,
    pub messages: u32,
    pub hops: u16,
}

impl Budget {
    pub(crate) fn add(self, charge: Self, limit: Self) -> Option<Self> {
        let tokens = match (limit.tokens, self.tokens, charge.tokens) {
            (Some(max), spent, Some(charge)) => Some(
                spent
                    .unwrap_or(0)
                    .checked_add(charge)
                    .filter(|v| *v <= max)?,
            ),
            (Some(_), _, None) => return None,
            (None, _, _) => None,
        };
        let result = Self {
            tokens,
            wall_secs: self.wall_secs.checked_add(charge.wall_secs)?,
            messages: self.messages.checked_add(charge.messages)?,
            hops: self.hops.checked_add(charge.hops)?,
        };
        (result.wall_secs <= limit.wall_secs
            && result.messages <= limit.messages
            && result.hops <= limit.hops)
            .then_some(result)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExposureRef {
    pub sender: AgentId,
    pub epoch: Epoch,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExposureSource {
    Message(MessageId),
    Artifact(ArtifactRef),
}
