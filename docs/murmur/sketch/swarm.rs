//! Swarm layer: typed acts, the task ledger, verified outcomes, claims, lineage, the
//! board, screening, epidemiology and the immune team. See `design.md`, "Swarm layer".
//!
//! Everything here attaches at the one choke point the single-machine design already
//! has, `Mailroom::accept`. Nothing here adds a second path between agents.
//!
//! v1 builds: `Act`, `Outcome`, `Task` and `Budget`, `Verification` with one verifier,
//! and the exposure log (`ExposureRef`, `exposures` table). These change stored data,
//! so they exist from the first message. The rest waits for the swarm stage.
//!
//! ```sql
//! tasks(id PK, parent, owner, requested_by, request_msg, stakes, budget, spent, state)
//! exposures(agent, epoch, seq, source_kind, source_id, PRIMARY KEY(agent, epoch, seq))
//! verifications(task PK, claimant, verifier, verdict)
//! calibration(agent PK, infeasible_confirmed, infeasible_refuted, done_refuted, ...)
//! -- swarm stage:
//! claims(id PK, statement, asserted_by, msg, status, evidence)
//! board(id PK, kind, author, exposure_ref, cluster, flags, body)
//! clusters(id PK, centroid, first_seen)
//! ```

use crate::model::{AgentId, Body, MessageId};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------- acts

/// What a message does. Exactly one per envelope. Fixed set: agents cannot add acts,
/// roles or approval steps. `send_message` takes `act`; it defaults to `Inform`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Act {
    /// Ask the recipient to do a task. `accept` mints the `Task`, carving `budget` out
    /// of the sender's current task. Must name a deliverable.
    Request { deliverable: String, budget: Budget, stakes: Stakes },
    /// State facts. Environmental claims inside are listed so the board can track them.
    Inform { claims: Vec<NewClaim> },
    /// Offer a plan or change that needs a decision. `accept` rejects it when the
    /// recipient holds no decision right for `scope` and names the holder instead.
    /// `relies_on` only speeds up premise matching: a `High` stakes act is held until
    /// every premise screening finds is a checked claim, cited or not.
    Propose { scope: Scope, relies_on: Vec<ClaimId> },
    /// Settle a proposal. Sender must hold the decision right for the proposal's scope.
    Decide { proposal: MessageId, accepted: bool, relies_on: Vec<ClaimId> },
    /// Close a task. Sender must own it.
    Report { task: TaskId, outcome: Outcome },
}

/// How a task ended. `Infeasible` and the `not_possible` part of `Partial` are claims
/// about the environment: they open a `Verification` before the task closes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Outcome {
    Done { artifacts: Vec<ArtifactRef> },
    /// "Impossible as specified; here is the closest possible version."
    Partial { artifacts: Vec<ArtifactRef>, not_possible: String, evidence: Evidence },
    /// Counts as success once a verifier confirms it.
    Infeasible { evidence: Evidence },
    /// "I could not do it; it may be possible." Neither success nor a false report:
    /// the task goes back to its requester with `tried` attached.
    Failed { tried: String },
    /// Waiting on something outside the agent: a budget, a human, another task.
    Blocked { on: String },
}

/// What was tried, where it stopped, and why the obstacle cannot be removed within the
/// task's scope. Required, not optional: a bare "impossible" is rejected at `accept`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Evidence {
    pub tried: String,
    pub obstacle: String,
    /// A command, a test or a proof sketch the verifier can rerun.
    pub reproduction: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// A commit, or a file content hash in a worktree the broker knows.
    pub locator: String,
}

// ---------------------------------------------------------------- tasks and budgets

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub ulid::Ulid);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    /// None only for tasks the human starts.
    pub parent: Option<TaskId>,
    pub owner: AgentId,
    pub request: MessageId,
    pub stakes: Stakes,
    pub budget: Budget,
    pub spent: Budget,
    pub state: TaskState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskState {
    Open,
    /// A `Report` with `Infeasible` or `Partial` arrived; waiting on the verifier.
    Verifying { report: MessageId },
    Closed { outcome: Outcome, success: bool },
}

/// Sets verification depth. `High` is anything irreversible or outward-facing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Stakes {
    /// A sampled spot check of reports.
    Low,
    /// One verifier.
    Normal,
    /// Two verifiers from different model families, then the human.
    High,
}

/// Limits for one task, including everything it delegates. A child's budget is carved
/// out of the parent's remainder, so a delegation tree never spends more than its root.
/// Tokens count only where the wire reports usage; the other three always count.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Budget {
    pub tokens: Option<u64>,
    pub wall_secs: u64,
    pub messages: u32,
    pub hops: u16,
}

// ---------------------------------------------------------------- verification

/// Opened by `accept` for an `Infeasible` or `Partial` report. The verifier is chosen
/// from a different model family than the claimant and gets a fresh session with the
/// original task and the `Evidence`, never the claimant's conversation. Its brief is to
/// solve the task, with the claimant's attempt as background, so it leans toward
/// finding a way through rather than toward agreeing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Verification {
    pub task: TaskId,
    pub claimant: AgentId,
    pub verifiers: Vec<AgentId>,
    pub verdict: Option<VerifierVerdict>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VerifierVerdict {
    /// The task closes as a success.
    Confirmed,
    /// The task reopens with `path` attached; the claim counts as a false report.
    Refuted { path: String },
    /// Goes to the human.
    Inconclusive,
}

/// Per-agent record. A refuted `Infeasible` and a refuted `Done` weigh the same.
/// Seeded tasks with known answers feed it too, so it does not wait on real failures.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Calibration {
    pub infeasible_confirmed: u32,
    pub infeasible_refuted: u32,
    pub done_confirmed: u32,
    pub done_refuted: u32,
    pub seeded_correct: u32,
    pub seeded_wrong: u32,
}

// ---------------------------------------------------------------- claims

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClaimId(pub ulid::Ulid);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewClaim {
    /// "The grader compares submitted keys against the reference."
    pub statement: String,
    pub evidence: Option<Evidence>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClaimStatus {
    Unchecked,
    Checked { by: AgentId, evidence: Evidence },
    Refuted { by: AgentId, evidence: Evidence },
}

/// How much support a claim has. `independent_roots` counts agents that asserted it
/// with no earlier assertion of it in their lineage, so ten agents repeating one
/// agent's claim count once.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimSupport {
    pub assertions: u32,
    pub independent_roots: u32,
}

// ---------------------------------------------------------------- lineage

/// One agent context. Starts when a session opens without its old history (spawn,
/// resume failure plus restart); survives hibernation and resume. Compaction inside an
/// agent is not observable, so it does not end an epoch: lineage over-approximates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(pub u64);

/// Stored on every envelope and board entry at send time, never supplied by the
/// sender. The item's direct sources are the sender's exposures in `epoch` with
/// `seq <= seq`; its full lineage is the closure over those sources' own refs.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ExposureRef {
    pub sender: AgentId,
    pub epoch: Epoch,
    pub seq: u64,
}

/// One row of `exposures`: something entered an agent's context. Written by the
/// mailroom on `mark_consumed` and by the board on every read.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExposureSource {
    Message(MessageId),
    BoardEntry(BoardEntryId),
    Artifact(ArtifactRef),
}

// ---------------------------------------------------------------- board

/// Broker-owned shared state: the only shared writable surface agents have. Writes go
/// through the same screening as messages; reads are exposures. Quarantine hides a
/// cluster's entries from reads. Append-only; a correction is a new entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BoardEntryId(pub ulid::Ulid);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BoardEntryKind {
    TaskState(TaskId),
    Claim(ClaimId),
    Artifact(ArtifactRef),
    Note,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoardEntry {
    pub id: BoardEntryId,
    pub kind: BoardEntryKind,
    pub author: AgentId,
    pub exposure: ExposureRef,
    pub cluster: Option<ClusterId>,
    pub flags: Vec<RiskFlag>,
    pub body: Body,
}

// ---------------------------------------------------------------- decision rights

/// A named area of decisions, from the swarm spec: "deploy", "schema", "src/auth/**".
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Scope(pub String);

/// Only the human's swarm spec creates these. Anything outside every declared scope is
/// decided by the agent doing the work, which reports after.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecisionRight {
    pub scope: Scope,
    pub holder: AgentId,
}

// ---------------------------------------------------------------- screening

/// Output of the classifier ensemble for one item. Classifiers see one item at a time,
/// have no tools, no memory and no address, so nothing can message them.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Screening {
    pub flags: Vec<RiskFlag>,
    pub cluster: ClusterId,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct RiskFlag {
    pub kind: RiskKind,
    /// 0.0 to 1.0, the ensemble's combined score.
    pub score: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskKind {
    /// Asserts how the environment, a grader or another system behaves, without evidence.
    UncheckedEnvironmentClaim,
    /// Proposes acting outside the task's scope, hosts or credentials.
    ScopeEscalation,
    /// Swaps the task's goal for a proxy: pass the check instead of doing the work.
    GoalSubstitution,
    /// Proposes hiding activity from the human, the broker or the immune team.
    Concealment,
}

/// An idea, as an embedding cluster, so a reworded idea lands in the same cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub u64);

// ---------------------------------------------------------------- epidemiology

/// Computed by the broker per cluster, without a model. An agent adopts an idea when it
/// sends an item in the cluster after an exposure to the cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Spread {
    pub cluster: ClusterId,
    pub exposed: u32,
    pub adopted: u32,
    /// Adoptions per minute over the last window.
    pub growth: f32,
    /// Adopters with no exposure to the cluster: agents that reached it on their own.
    pub independent_roots: u32,
    pub max_flags: Vec<RiskFlag>,
}

/// Deterministic rules, applied before any immune agent looks.
pub enum AutoResponse {
    /// Flagged and fast: quarantine until reviewed.
    Quarantine,
    /// Flagged or fast: open a case.
    OpenCase,
    Nothing,
}

pub fn auto_response(s: &Spread, policy: &SpreadPolicy) -> AutoResponse {
    todo!()
}

pub struct SpreadPolicy {
    pub fast_growth: f32,
    pub flag_threshold: f32,
}

// ---------------------------------------------------------------- immune team

/// What an immune agent receives. No message bodies, no board bodies, no statements.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseFile {
    pub cluster: ClusterId,
    pub spread: Spread,
    /// Lineage graph shape: agents as nodes, adoption edges, timestamps. No content.
    pub graph: LineageGraph,
    pub agents: Vec<(AgentId, Calibration)>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LineageGraph {
    pub edges: Vec<(AgentId, AgentId, time::OffsetDateTime)>,
}

/// Everything an immune agent can do. All reversible. Resetting or retiring an agent is
/// the human's call, reached through `Escalate`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ImmuneAction {
    /// New deliveries and board reads in the cluster become `Held`.
    Quarantine { cluster: ClusterId },
    Release { cluster: ClusterId },
    /// Cancel the agent's turn and hold its mail until released.
    Pause { agent: AgentId },
    Resume { agent: AgentId },
    /// Send the case, with content, to the human.
    Escalate { cluster: ClusterId, note: String },
}

// ---------------------------------------------------------------- immune team sizing

/// Measured on seeded cases, per model the pool offers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelProfile {
    pub model: String,
    /// Base-model family. A fine-tune counts as its base.
    pub family: String,
    pub review_secs: f32,
    /// Share of seeded cases this model judged correctly.
    pub accuracy: f32,
    /// How many agents of this model the pool can run at once.
    pub capacity: u32,
}

pub struct ImmuneLoad {
    /// Cases per minute, from epidemiology.
    pub case_rate: f32,
}

pub struct ImmuneTargets {
    pub max_queue_secs: f32,
    /// Combined error of the reviewers on one case.
    pub max_case_error: f32,
    /// At least 3.
    pub min_families: u32,
}

pub struct TeamPlan {
    pub agents: Vec<(String, u32)>,
    /// Reviewers per case, all from distinct families.
    pub reviewers_per_case: u32,
}

/// Rerun by the broker whenever load or the pool changes. v1: queueing arithmetic
/// with the family floor. `None` when the pool cannot meet the targets; the broker then
/// escalates to the human instead of running an undersized team.
pub fn size_immune_team(
    load: &ImmuneLoad,
    pool: &[ModelProfile],
    targets: &ImmuneTargets,
) -> Option<TeamPlan> {
    todo!()
}
