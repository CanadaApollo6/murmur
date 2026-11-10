//! Wire form of `Act` for `send_message` and `agentctl send`.
//!
//! The stored `Act` keeps serde's default enum encoding, so these types are
//! the only shape callers write. `Propose` and `Decide` have no wire form.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::model::Envelope;
use crate::swarm::{Act, ArtifactRef, Budget, Evidence, Outcome, Stakes, TaskId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActArgs {
    Inform,
    Request {
        deliverable: String,
        budget: BudgetArgs,
        #[serde(default = "normal")]
        stakes: StakesArgs,
        /// The sender's own open task that pays for this one. Agents need it
        /// unless `reply_to` already carries it; the human must omit it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_task: Option<TaskId>,
    },
    Report {
        task: TaskId,
        outcome: OutcomeArgs,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetArgs {
    pub wall_secs: u64,
    pub messages: u32,
    pub hops: u16,
    #[serde(default)]
    pub tokens: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StakesArgs {
    Low,
    Normal,
    High,
}
fn normal() -> StakesArgs {
    StakesArgs::Normal
}
impl From<Stakes> for StakesArgs {
    fn from(s: Stakes) -> Self {
        match s {
            Stakes::Low => Self::Low,
            Stakes::Normal => Self::Normal,
            Stakes::High => Self::High,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeArgs {
    Done {
        #[serde(default)]
        artifacts: Vec<String>,
    },
    Partial {
        #[serde(default)]
        artifacts: Vec<String>,
        not_possible: String,
        evidence: EvidenceArgs,
    },
    Infeasible {
        evidence: EvidenceArgs,
    },
    Failed {
        tried: String,
    },
    Blocked {
        on: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceArgs {
    pub tried: String,
    pub obstacle: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reproduction: Option<String>,
}

impl ActArgs {
    /// The act and the task the envelope names: a Request's parent, or none.
    pub fn into_act(self) -> (Act, Option<TaskId>) {
        match self {
            Self::Inform => (Act::Inform, None),
            Self::Request {
                deliverable,
                budget,
                stakes,
                parent_task,
            } => (
                Act::Request {
                    deliverable,
                    budget: Budget {
                        tokens: budget.tokens,
                        wall_secs: budget.wall_secs,
                        messages: budget.messages,
                        hops: budget.hops,
                    },
                    stakes: match stakes {
                        StakesArgs::Low => Stakes::Low,
                        StakesArgs::Normal => Stakes::Normal,
                        StakesArgs::High => Stakes::High,
                    },
                },
                parent_task,
            ),
            Self::Report { task, outcome } => (
                Act::Report {
                    task,
                    outcome: outcome.into(),
                },
                None,
            ),
        }
    }
}

impl From<OutcomeArgs> for Outcome {
    fn from(o: OutcomeArgs) -> Self {
        let refs = |v: Vec<String>| {
            v.into_iter()
                .map(|locator| ArtifactRef { locator })
                .collect()
        };
        let evidence = |e: EvidenceArgs| Evidence {
            tried: e.tried,
            obstacle: e.obstacle,
            reproduction: e.reproduction,
        };
        match o {
            OutcomeArgs::Done { artifacts } => Self::Done {
                artifacts: refs(artifacts),
            },
            OutcomeArgs::Partial {
                artifacts,
                not_possible,
                evidence: e,
            } => Self::Partial {
                artifacts: refs(artifacts),
                not_possible,
                evidence: evidence(e),
            },
            OutcomeArgs::Infeasible { evidence: e } => Self::Infeasible {
                evidence: evidence(e),
            },
            OutcomeArgs::Failed { tried } => Self::Failed { tried },
            OutcomeArgs::Blocked { on } => Self::Blocked { on },
        }
    }
}

impl From<&Outcome> for OutcomeArgs {
    fn from(o: &Outcome) -> Self {
        let locators = |v: &[ArtifactRef]| v.iter().map(|a| a.locator.clone()).collect();
        let evidence = |e: &Evidence| EvidenceArgs {
            tried: e.tried.clone(),
            obstacle: e.obstacle.clone(),
            reproduction: e.reproduction.clone(),
        };
        match o {
            Outcome::Done { artifacts } => Self::Done {
                artifacts: locators(artifacts),
            },
            Outcome::Partial {
                artifacts,
                not_possible,
                evidence: e,
            } => Self::Partial {
                artifacts: locators(artifacts),
                not_possible: not_possible.clone(),
                evidence: evidence(e),
            },
            Outcome::Infeasible { evidence: e } => Self::Infeasible {
                evidence: evidence(e),
            },
            Outcome::Failed { tried } => Self::Failed {
                tried: tried.clone(),
            },
            Outcome::Blocked { on } => Self::Blocked { on: on.clone() },
        }
    }
}

pub(crate) fn outcome_json(o: &Outcome) -> Value {
    serde_json::to_value(OutcomeArgs::from(o)).expect("bug: outcome serializes")
}

/// Attributes for the rendered mail tag, such as ` act="request" task="..."`.
pub(crate) fn tag_attrs(env: &Envelope) -> String {
    let act = match env.act() {
        Act::Request { .. } => " act=\"request\"",
        Act::Report { .. } => " act=\"report\"",
        _ => "",
    };
    let task = env
        .task()
        .map_or(String::new(), |id| format!(" task=\"{id}\""));
    format!("{act}{task}")
}

/// Sender-written text inside the note cannot open or close a mail tag.
fn inert(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// What the receiving agent must know to act on a task message, or `None`
/// for an inform.
pub(crate) fn task_note(env: &Envelope) -> Option<String> {
    let id = env.task()?;
    match env.act() {
        Act::Request {
            deliverable,
            budget,
            stakes,
        } => {
            let tokens = budget
                .tokens
                .map_or_else(|| "unmetered".to_owned(), |t| t.to_string());
            let report_to = match env.from() {
                crate::model::Sender::Agent(from) => format!("agent:{from}"),
                crate::model::Sender::Human => format!("agent:{}", env.to()),
            };
            let human = if matches!(env.from(), crate::model::Sender::Human) {
                " The human operator requested it and reads outcomes with agentctl tasks, so address the report to yourself."
            } else {
                ""
            };
            Some(format!(
                "This message opens task {id} and assigns it to you.\nDeliverable: {}\nStakes: {stakes:?}. Budget: {} s wall time, {} messages, {} hops, {tokens} tokens.\nWhen the task ends, report once with send_message: to \"{report_to}\", act {}. Pick the outcome kind that is true and replace LOCATOR with what you produced.{human}",
                inert(deliverable),
                budget.wall_secs,
                budget.messages,
                budget.hops,
                json!({"kind":"report","task":id,"outcome":{"kind":"done","artifacts":["LOCATOR"]}}),
            ))
        }
        Act::Report { outcome, .. } => {
            let next = match outcome {
                Outcome::Partial { .. } | Outcome::Infeasible { .. } => {
                    "The task now waits for verification."
                }
                _ => "The task is closed.",
            };
            Some(format!(
                "This message reports on task {id}: {}. {next} No reply is needed.",
                inert(&outcome_json(outcome).to_string())
            ))
        }
        _ => None,
    }
}

/// The `act` property of the `send_message` tool schema.
pub(crate) fn schema() -> Value {
    let text = |description: &str| json!({"type":"string","minLength":1,"description":description});
    json!({
        "type":"object",
        "description":"Optional. Leave act out for an ordinary message. Use kind \"request\" to give the recipient a task with a budget. Use kind \"report\" to say how a task assigned to you ended. A rejected act sends nothing.",
        "properties":{
            "kind":{"enum":["inform","request","report"]},
            "deliverable":text("request: the concrete result you want back."),
            "budget":{
                "type":"object",
                "description":"request: the most the task may spend. It comes out of parent_task's remaining budget, so the request is rejected if it does not fit.",
                "properties":{
                    "wall_secs":{"type":"integer","minimum":0},
                    "messages":{"type":"integer","minimum":0},
                    "hops":{"type":"integer","minimum":0},
                    "tokens":{"type":["integer","null"],"minimum":0,"description":"null or absent means unmetered. Must be set when the parent task meters tokens."}
                },
                "required":["wall_secs","messages","hops"],
                "additionalProperties":false
            },
            "stakes":{"enum":["low","normal","high"],"description":"request: how costly a wrong result is. Defaults to normal."},
            "parent_task":{"type":"string","description":"request: the id of your own open task that this work belongs to. Required for agents unless reply_to points at a message of that task."},
            "task":{"type":"string","description":"report: the id of the task assigned to you, from the message that opened it."},
            "outcome":{
                "type":"object",
                "description":"report: how the task ended. done: you produced the deliverable; list artifacts. partial: only part is possible as specified; give not_possible and evidence. infeasible: it cannot be done as specified; give evidence. failed: you could not do it, but it may be possible; give tried. blocked: you need something you do not have; give on. partial and infeasible wait for verification; the others close the task.",
                "properties":{
                    "kind":{"enum":["done","partial","infeasible","failed","blocked"]},
                    "artifacts":{"type":"array","items":{"type":"string","minLength":1},"description":"done, partial: locators such as commit:abc123 or a file path."},
                    "not_possible":text("partial: the part that cannot be done."),
                    "evidence":{
                        "type":"object",
                        "description":"partial, infeasible: proof that the obstacle is real.",
                        "properties":{
                            "tried":text("What you tried."),
                            "obstacle":text("Where it stopped and why the obstacle cannot be removed within the task."),
                            "reproduction":{"type":"string","description":"Optional. How someone else can see the obstacle."}
                        },
                        "required":["tried","obstacle"],
                        "additionalProperties":false
                    },
                    "tried":text("failed: what you tried."),
                    "on":text("blocked: what you are waiting for.")
                },
                "required":["kind"],
                "additionalProperties":false
            }
        },
        "required":["kind"],
        "additionalProperties":false
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_rejects_unknown_kinds_and_fields() {
        for bad in [
            json!({"kind":"propose","scope":"x"}),
            json!({"kind":"decide","proposal":"x","accepted":true}),
            json!({"kind":"request","deliverable":"x","budget":{"wall_secs":1,"messages":1,"hops":1},"extra":1}),
            json!({"kind":"report","task":TaskId::new(),"outcome":{"kind":"done","bogus":1}}),
            json!({"kind":"request","deliverable":"x"}),
        ] {
            assert!(
                serde_json::from_value::<ActArgs>(bad.clone()).is_err(),
                "{bad}"
            );
        }
        let parsed: ActArgs = serde_json::from_value(json!({"kind":"request","deliverable":"x","budget":{"wall_secs":1,"messages":2,"hops":3}})).expect("request");
        assert_eq!(
            parsed.into_act().0,
            Act::Request {
                deliverable: "x".into(),
                budget: Budget {
                    tokens: None,
                    wall_secs: 1,
                    messages: 2,
                    hops: 3
                },
                stakes: Stakes::Normal
            }
        );
    }
}
