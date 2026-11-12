//! Wire form of `Act` for `send_message` and `agentctl send`.
//!
//! The stored `Act` keeps serde's default enum encoding, so these types are
//! the only shape callers write. `Propose` and `Decide` have no wire form.
//! `schema()` publishes the same shape; `schema_and_parser_agree` holds the
//! two together.

use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::model::{Envelope, Sender};
use crate::swarm::{
    Act, ArtifactRef, Budget, Evidence, Finding, Outcome, Stakes, TaskId, VERIFICATION_DELIVERABLE,
    VerifierVerdict,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActArgs {
    /// A struct variant, because serde ignores unknown fields on a unit variant.
    Inform {},
    Request {
        deliverable: Text,
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
        /// Only a verifier sends one, on its verification task.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        verdict: Option<VerdictArgs>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerdictArgs {
    Confirmed { tried: Text },
    Refuted { path: Text, reproduction: Text },
}

impl From<VerdictArgs> for VerifierVerdict {
    fn from(v: VerdictArgs) -> Self {
        match v {
            VerdictArgs::Confirmed { tried } => Self::Confirmed { tried: tried.0 },
            VerdictArgs::Refuted { path, reproduction } => Self::Refuted {
                path: path.0,
                reproduction: reproduction.0,
            },
        }
    }
}

/// Displays a stored verdict. The mailroom validated its text on the way in.
impl From<&VerifierVerdict> for VerdictArgs {
    fn from(v: &VerifierVerdict) -> Self {
        match v {
            VerifierVerdict::Confirmed { tried } => Self::Confirmed {
                tried: Text(tried.clone()),
            },
            VerifierVerdict::Refuted { path, reproduction } => Self::Refuted {
                path: Text(path.clone()),
                reproduction: Text(reproduction.clone()),
            },
        }
    }
}

/// Sender-written text with at least one non-whitespace character.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Text(String);
impl TryFrom<String> for Text {
    type Error = &'static str;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.trim().is_empty() {
            Err("text must contain a non-whitespace character")
        } else {
            Ok(Self(s))
        }
    }
}
impl From<Text> for String {
    fn from(t: Text) -> Self {
        t.0
    }
}

/// A zero wall time, message count or token count would let a parent open
/// any number of children for free, so the wire form cannot express one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetArgs {
    pub wall_secs: NonZeroU64,
    pub messages: NonZeroU32,
    pub hops: u16,
    #[serde(default)]
    pub tokens: Option<NonZeroU64>,
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
        artifacts: Vec<Text>,
    },
    Partial {
        #[serde(default)]
        artifacts: Vec<Text>,
        not_possible: Text,
        evidence: EvidenceArgs,
    },
    Infeasible {
        evidence: EvidenceArgs,
    },
    Failed {
        tried: Text,
    },
    Blocked {
        on: Text,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceArgs {
    pub tried: Text,
    pub obstacle: Text,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reproduction: Option<Text>,
}

impl ActArgs {
    /// The act and the task the envelope names: a Request's parent, or none.
    pub fn into_act(self) -> (Act, Option<TaskId>) {
        match self {
            Self::Inform {} => (Act::Inform, None),
            Self::Request {
                deliverable,
                budget,
                stakes,
                parent_task,
            } => (
                Act::Request {
                    deliverable: deliverable.0,
                    budget: Budget {
                        tokens: budget.tokens.map(NonZeroU64::get),
                        wall_secs: budget.wall_secs.get(),
                        messages: budget.messages.get(),
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
            Self::Report {
                task,
                outcome,
                verdict,
            } => (
                Act::Report {
                    task,
                    outcome: outcome.into(),
                    verdict: verdict.map(Into::into),
                },
                None,
            ),
        }
    }
}

impl From<OutcomeArgs> for Outcome {
    fn from(o: OutcomeArgs) -> Self {
        let refs = |v: Vec<Text>| {
            v.into_iter()
                .map(|locator| ArtifactRef { locator: locator.0 })
                .collect()
        };
        let evidence = |e: EvidenceArgs| Evidence {
            tried: e.tried.0,
            obstacle: e.obstacle.0,
            reproduction: e.reproduction.map(String::from),
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
                not_possible: not_possible.0,
                evidence: evidence(e),
            },
            OutcomeArgs::Infeasible { evidence: e } => Self::Infeasible {
                evidence: evidence(e),
            },
            OutcomeArgs::Failed { tried } => Self::Failed { tried: tried.0 },
            OutcomeArgs::Blocked { on } => Self::Blocked { on: on.0 },
        }
    }
}

/// Displays a stored outcome. The mailroom validated its text on the way in.
impl From<&Outcome> for OutcomeArgs {
    fn from(o: &Outcome) -> Self {
        let text = |s: &String| Text(s.clone());
        let locators = |v: &[ArtifactRef]| v.iter().map(|a| text(&a.locator)).collect();
        let evidence = |e: &Evidence| EvidenceArgs {
            tried: text(&e.tried),
            obstacle: text(&e.obstacle),
            reproduction: e.reproduction.as_ref().map(text),
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
                not_possible: text(not_possible),
                evidence: evidence(e),
            },
            Outcome::Infeasible { evidence: e } => Self::Infeasible {
                evidence: evidence(e),
            },
            Outcome::Failed { tried } => Self::Failed { tried: text(tried) },
            Outcome::Blocked { on } => Self::Blocked { on: text(on) },
        }
    }
}

pub(crate) fn outcome_json(o: &Outcome) -> Value {
    serde_json::to_value(OutcomeArgs::from(o)).expect("bug: outcome serializes")
}

/// A review finding as `agentctl tasks` and the feed show it.
pub(crate) fn finding_json(f: &Finding) -> Value {
    match f {
        Finding::Unverified { reason } => json!({"kind":"unverified","reason":reason}),
        Finding::Confirmed => json!({"kind":"confirmed"}),
        Finding::Refuted { path, reproduction } => {
            json!({"kind":"refuted","path":path,"reproduction":reproduction})
        }
        Finding::Inconclusive => json!({"kind":"inconclusive"}),
    }
}

fn outcome_kind(o: &Outcome) -> &'static str {
    match o {
        Outcome::Done { .. } => "done",
        Outcome::Partial { .. } => "partial",
        Outcome::Infeasible { .. } => "infeasible",
        Outcome::Failed { .. } => "failed",
        Outcome::Blocked { .. } => "blocked",
    }
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

/// Everything the sender wrote: the body, then the act's free text. Callers
/// put it inside the mail tag and escape it as they escape a body.
pub(crate) fn content(env: &Envelope) -> String {
    let body = env.body().as_str();
    match env.act() {
        Act::Request { deliverable, .. } => format!("{body}\n\nDeliverable: {deliverable}"),
        Act::Report {
            outcome, verdict, ..
        } => {
            let mut text = format!("{body}\n\nOutcome: {}", outcome_json(outcome));
            if let Some(v) = verdict {
                text.push_str(&format!(
                    "\nVerdict: {}",
                    serde_json::to_value(VerdictArgs::from(v)).expect("bug: verdict serializes")
                ));
            }
            text
        }
        _ => body.to_owned(),
    }
}

/// What the receiving agent must do with a task message, or `None` for an
/// inform. It follows the mail tag, so it holds only values the broker
/// produced: ids, numbers, enum names and the sample act.
pub(crate) fn task_note(env: &Envelope) -> Option<String> {
    let id = env.task()?;
    match env.act() {
        Act::Request {
            budget,
            stakes,
            deliverable,
        } => {
            let tokens = budget
                .tokens
                .map_or_else(|| "unmetered".to_owned(), |t| t.to_string());
            let (report_to, human) = match env.from() {
                Sender::Agent(from) => (*from, ""),
                Sender::Human => (
                    env.to(),
                    " The human operator requested it and reads outcomes with agentctl tasks, so address the report to yourself.",
                ),
            };
            // A verification task is opened only by the broker, as the operator.
            let (sample, pick) = if *env.from() == Sender::Human
                && deliverable == VERIFICATION_DELIVERABLE
            {
                (
                    json!({"kind":"report","task":id,"outcome":{"kind":"done"},"verdict":{"kind":"refuted","path":"PATH","reproduction":"REPRODUCTION"}}),
                    "Send done with a refuted verdict if the claim does not hold, done with a confirmed verdict if it holds, or failed if you cannot decide.",
                )
            } else {
                (
                    json!({"kind":"report","task":id,"outcome":{"kind":"done","artifacts":["LOCATOR"]}}),
                    "Pick the outcome kind that is true and replace LOCATOR with what you produced.",
                )
            };
            Some(format!(
                "This message opens task {id} and assigns it to you. The deliverable is in the message above. Stakes: {stakes:?}. Budget: {} s wall time, {} messages, {} hops, {tokens} tokens.\nWhen the task ends, report once with send_message: to \"agent:{report_to}\", act {sample}. {pick}{human}",
                budget.wall_secs, budget.messages, budget.hops,
            ))
        }
        Act::Report {
            outcome, verdict, ..
        } => {
            let next = match outcome {
                Outcome::Partial { .. } | Outcome::Infeasible { .. } => {
                    "The task now waits for a verifier or the operator."
                }
                Outcome::Done { .. } if verdict.is_none() => {
                    "A verifier may check this report before the task closes."
                }
                _ => "The task is closed.",
            };
            let verdict = match verdict {
                Some(VerifierVerdict::Confirmed { .. }) => " and verdict confirmed",
                Some(VerifierVerdict::Refuted { .. }) => " and verdict refuted",
                None => "",
            };
            Some(format!(
                "This message reports on task {id} with outcome {}{verdict}. {next} No reply is needed.",
                outcome_kind(outcome)
            ))
        }
        _ => None,
    }
}

/// Matches a character outside Unicode `White_Space`, which `str::trim` strips.
/// Regex engines disagree on `\S`, so the class lists the code points.
const NON_WHITESPACE: &str = "[^\\u0009-\\u000D\\u0020\\u0085\\u00A0\\u1680\\u2000-\\u200A\\u2028\\u2029\\u202F\\u205F\\u3000]";

/// The `act` property of the `send_message` tool schema: one `oneOf` branch
/// per kind, each closed to the fields that kind takes.
pub(crate) fn schema() -> Value {
    let text = |description: &str| json!({"type":"string","pattern":NON_WHITESPACE,"description":description});
    let task_id = |description: &str| json!({"type":"string","pattern":"^[0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{26}$","description":description});
    let kind = |k: &str| json!({"const":k});
    let artifacts = json!({"type":"array","items":text("A locator such as commit:abc123 or a file path."),"description":"Locators of what you produced."});
    let evidence = json!({
        "type":"object",
        "description":"Proof that the obstacle is real.",
        "properties":{
            "tried":text("What you tried."),
            "obstacle":text("Where it stopped and why the obstacle cannot be removed within the task."),
            "reproduction":{"type":["string","null"],"pattern":NON_WHITESPACE,"description":"Optional. How someone else can see the obstacle."}
        },
        "required":["tried","obstacle"],
        "additionalProperties":false
    });
    let outcome = |k: &str, description: &str, properties: Value, required: &[&str]| {
        let mut properties = properties;
        properties["kind"] = kind(k);
        let mut required = required.to_vec();
        required.push("kind");
        json!({"type":"object","description":description,"properties":properties,"required":required,"additionalProperties":false})
    };
    json!({
        "description":"Optional. Leave act out for an ordinary message. Use kind \"request\" to give the recipient a task with a budget. Use kind \"report\" to say how a task assigned to you ended. The body and the act together must fit in 65536 bytes. A rejected act sends nothing.",
        "oneOf":[
            {
                "type":"object",
                "description":"An ordinary message, the same as leaving act out.",
                "properties":{"kind":kind("inform")},
                "required":["kind"],
                "additionalProperties":false
            },
            {
                "type":"object",
                "description":"Give the recipient a task.",
                "properties":{
                    "kind":kind("request"),
                    "deliverable":text("The concrete result you want back."),
                    "budget":{
                        "type":"object",
                        "description":"The most the task may spend. It comes out of parent_task's remaining budget, so the request is rejected if it does not fit.",
                        "properties":{
                            "wall_secs":{"type":"integer","minimum":1,"maximum":u64::MAX},
                            "messages":{"type":"integer","minimum":1,"maximum":u32::MAX},
                            "hops":{"type":"integer","minimum":0,"maximum":u16::MAX},
                            "tokens":{"type":["integer","null"],"minimum":1,"maximum":u64::MAX,"description":"null or absent means unmetered. Must be set when the parent task meters tokens."}
                        },
                        "required":["wall_secs","messages","hops"],
                        "additionalProperties":false
                    },
                    "stakes":{"enum":["low","normal","high"],"description":"How costly a wrong result is. Defaults to normal."},
                    "parent_task":{"type":["string","null"],"pattern":"^[0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{26}$","description":"The id of your own open task that this work belongs to. Required for agents unless reply_to points at a message of that task."}
                },
                "required":["kind","deliverable","budget"],
                "additionalProperties":false
            },
            {
                "type":"object",
                "description":"Say how a task assigned to you ended. Send it to the address the task message names.",
                "properties":{
                    "kind":kind("report"),
                    "task":task_id("The id of the task assigned to you, from the message that opened it."),
                    "verdict":{
                        "description":"Only for a verification task, and required there with outcome done. Leave it out or null otherwise. failed and blocked carry none and mean you could not decide.",
                        "oneOf":[
                            {"type":"null"},
                            outcome("confirmed", "The claim holds.", json!({"tried":text("What you tried.")}), &["tried"]),
                            outcome("refuted", "The claim does not hold.", json!({"path":text("What to change, or what is not met."),"reproduction":text("How to see it.")}), &["path","reproduction"])
                        ]
                    },
                    "outcome":{
                        "description":"partial and infeasible wait for a verifier or the operator. done may be checked by a verifier before the task closes. failed and blocked close the task.",
                        "oneOf":[
                            outcome("done", "You produced the deliverable.", json!({"artifacts":artifacts}), &[]),
                            outcome("partial", "Only part is possible as specified.", json!({"artifacts":artifacts,"not_possible":text("The part that cannot be done."),"evidence":evidence}), &["not_possible","evidence"]),
                            outcome("infeasible", "It cannot be done as specified.", json!({"evidence":evidence}), &["evidence"]),
                            outcome("failed", "You could not do it, but it may be possible.", json!({"tried":text("What you tried.")}), &["tried"]),
                            outcome("blocked", "You need something you do not have.", json!({"on":text("What you are waiting for.")}), &["on"])
                        ]
                    }
                },
                "required":["kind","task","outcome"],
                "additionalProperties":false
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One table for the published schema and the parser: each case is
    /// either accepted by both or rejected by both.
    #[test]
    fn schema_and_parser_agree() {
        let task = TaskId::new();
        let evidence = json!({"tried":"t","obstacle":"o"});
        let request = |extra: Value| {
            let mut act = json!({"kind":"request","deliverable":"x","budget":{"wall_secs":1,"messages":1,"hops":0}});
            for (k, v) in extra.as_object().expect("bug: extra is an object") {
                act[k] = v.clone();
            }
            act
        };
        let budget = |b: Value| request(json!({"budget":b}));
        let report = |outcome: Value| json!({"kind":"report","task":task,"outcome":outcome});
        let verdict =
            |v: Value| json!({"kind":"report","task":task,"outcome":{"kind":"done"},"verdict":v});
        let cases = [
            (json!({"kind":"inform"}), true),
            (request(json!({})), true),
            (
                request(
                    json!({"stakes":"high","parent_task":task,"budget":{"wall_secs":5,"messages":2,"hops":65535,"tokens":9}}),
                ),
                true,
            ),
            (
                request(
                    json!({"parent_task":null,"budget":{"wall_secs":5,"messages":2,"hops":1,"tokens":null}}),
                ),
                true,
            ),
            (report(json!({"kind":"done"})), true),
            (
                report(json!({"kind":"done","artifacts":["commit:a"]})),
                true,
            ),
            (
                report(
                    json!({"kind":"partial","not_possible":"n","evidence":{"tried":"t","obstacle":"o","reproduction":null}}),
                ),
                true,
            ),
            (
                report(
                    json!({"kind":"infeasible","evidence":{"tried":"t","obstacle":"o","reproduction":"r"}}),
                ),
                true,
            ),
            (report(json!({"kind":"failed","tried":"t"})), true),
            (report(json!({"kind":"blocked","on":"input"})), true),
            (verdict(json!({"kind":"confirmed","tried":"t"})), true),
            (
                verdict(json!({"kind":"refuted","path":"p","reproduction":"r"})),
                true,
            ),
            (verdict(Value::Null), true),
            (verdict(json!({"kind":"refuted","path":"p"})), false),
            (
                verdict(json!({"kind":"refuted","path":" ","reproduction":"r"})),
                false,
            ),
            (verdict(json!({"kind":"confirmed","tried":""})), false),
            (verdict(json!({"kind":"confirmed"})), false),
            (
                verdict(json!({"kind":"confirmed","tried":"t","x":1})),
                false,
            ),
            (verdict(json!({"kind":"inconclusive"})), false),
            (verdict(json!("confirmed")), false),
            (json!({"kind":"inform","typo":1}), false),
            (json!({"kind":"inform","outcome":{"kind":"done"}}), false),
            (json!({"kind":"request"}), false),
            (json!({}), false),
            (json!("inform"), false),
            (json!({"kind":"propose","scope":"x"}), false),
            (
                json!({"kind":"decide","proposal":"x","accepted":true}),
                false,
            ),
            (request(json!({"outcome":{"kind":"done"}})), false),
            (request(json!({"task":task})), false),
            (request(json!({"deliverable":""})), false),
            (request(json!({"deliverable":" \n"})), false),
            (request(json!({"deliverable":"\u{0085}"})), false),
            (request(json!({"deliverable":"\u{feff}"})), true),
            (request(json!({"stakes":"extreme"})), false),
            (request(json!({"stakes":null})), false),
            (request(json!({"parent_task":"not-a-task"})), false),
            (
                budget(json!({"wall_secs":1,"messages":1,"hops":70000})),
                false,
            ),
            (budget(json!({"wall_secs":0,"messages":1,"hops":1})), false),
            (budget(json!({"wall_secs":1,"messages":0,"hops":1})), false),
            (
                budget(json!({"wall_secs":1,"messages":1,"hops":1,"tokens":0})),
                false,
            ),
            (budget(json!({"wall_secs":-1,"messages":1,"hops":1})), false),
            (budget(json!({"wall_secs":1,"messages":1})), false),
            (
                budget(json!({"wall_secs":1,"messages":1,"hops":1,"extra":1})),
                false,
            ),
            (json!({"kind":"report","task":task}), false),
            (json!({"kind":"report","outcome":{"kind":"done"}}), false),
            (report(json!({"kind":"done","bogus":1})), false),
            (report(json!({"kind":"done","artifacts":[""]})), false),
            (report(json!({"kind":"done","on":"x"})), false),
            (report(json!({"kind":"partial","not_possible":"n"})), false),
            (report(json!({"kind":"partial","evidence":evidence})), false),
            (report(json!({"kind":"infeasible"})), false),
            (
                report(json!({"kind":"infeasible","evidence":{"tried":"t","obstacle":""}})),
                false,
            ),
            (
                report(json!({"kind":"infeasible","evidence":{"tried":"t","obstacle":"o","x":1}})),
                false,
            ),
            (report(json!({"kind":"failed"})), false),
            (report(json!({"kind":"failed","tried":""})), false),
            (report(json!({"kind":"blocked"})), false),
            (report(json!({"kind":"blocked","on":""})), false),
            (report(json!({"kind":"unknown"})), false),
        ];
        let schema = jsonschema::validator_for(&schema()).expect("bug: act schema compiles");
        for (case, valid) in cases {
            assert_eq!(schema.is_valid(&case), valid, "schema on {case}");
            let parsed = serde_json::from_value::<ActArgs>(case.clone());
            assert_eq!(parsed.is_ok(), valid, "parser on {case}: {parsed:?}");
        }
    }

    #[test]
    fn schema_text_pattern_matches_the_parser_for_every_bmp_char() {
        let schema = jsonschema::validator_for(&schema()).expect("bug: act schema compiles");
        for c in (0..=0xFFFF).filter_map(char::from_u32) {
            let act = json!({"kind":"request","deliverable":c.to_string(),"budget":{"wall_secs":1,"messages":1,"hops":0}});
            let parsed = serde_json::from_value::<ActArgs>(act.clone()).is_ok();
            assert_eq!(schema.is_valid(&act), parsed, "U+{:04X}", u32::from(c));
        }
    }

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
