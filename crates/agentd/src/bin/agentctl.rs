#![deny(clippy::unwrap_used)]
use std::path::PathBuf;

use agentd::model::*;
use agentd::supervisor::AgentSpec;
use agentd::surfaces::{
    ControlRequest, Result, Socket, default_state_dir, read_frame_limit, write_frame,
};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::Value;
use tokio::io::BufReader;
use tokio::net::UnixStream;

#[derive(Parser)]
#[command(about = "Control the local agentd broker")]
struct Args {
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Spawn {
        name: String,
        #[arg(long, value_enum)]
        kind: Kind,
        #[arg(long)]
        cwd: PathBuf,
        #[arg(long, value_enum, default_value = "read-only")]
        sandbox: Sandbox,
        #[arg(long)]
        model: Option<String>,
        /// A permission rule as TOOL=DECISION, for example `send_message=allow`. Repeatable;
        /// the first rule that names the tool, or `*`, wins. Tools no rule names ask.
        #[arg(long = "rule", value_name = "TOOL=DECISION", value_parser = parse_rule)]
        rules: Vec<PermissionRule>,
        first_message: Option<String>,
    },
    Ls,
    Send {
        to: String,
        text: String,
        /// The act as JSON, in the same shape as the send_message tool's `act`,
        /// for example '{"kind":"request","deliverable":"...","budget":{"wall_secs":600,"messages":20,"hops":2}}'.
        #[arg(long)]
        act: Option<String>,
    },
    Approvals,
    Approve {
        id: String,
        option: String,
    },
    Cancel {
        to: String,
    },
    /// End the agent's session and keep it stopped until `restart`.
    Stop {
        to: String,
    },
    /// Start a new session. Leaves stopped, needs-recovery and failed.
    Restart {
        to: String,
    },
    Held {
        to: Option<String>,
    },
    Release {
        id: String,
    },
    Drop {
        id: String,
    },
    Watch {
        #[arg(long, requires = "nonce")]
        cursor: Option<i64>,
        #[arg(long, requires = "cursor")]
        nonce: Option<String>,
    },
    Ui,
    /// List the task ledger.
    Tasks,
}
#[derive(Clone, ValueEnum)]
enum Kind {
    Infinity,
    Claude,
    Codex,
}
#[derive(Clone, ValueEnum)]
enum Sandbox {
    ReadOnly,
    WorkspaceWrite,
    FullAccess,
}
fn parse_rule(rule: &str) -> std::result::Result<PermissionRule, String> {
    let (tool, decision) = rule
        .split_once('=')
        .ok_or("expected TOOL=DECISION, for example send_message=allow")?;
    // ACP tool kinds and agentd's own names are lowercase ASCII, so other names never match.
    let named = !tool.is_empty()
        && tool
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if tool != "*" && !named {
        return Err("the tool is `*` or a name in lowercase letters, digits, `_` and `-`".into());
    }
    let decision = match decision {
        "allow" => RuleDecision::Allow,
        "deny" => RuleDecision::Deny,
        "ask" => RuleDecision::Ask,
        _ => return Err("decision must be allow, deny or ask".into()),
    };
    Ok(PermissionRule {
        tool: tool.into(),
        decision,
    })
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("agentctl: {error}");
        std::process::exit(1);
    }
}
async fn run(args: Args) -> Result<()> {
    let key = ClientKey(ulid::Ulid::new().to_string());
    let request = match args.command {
        Command::Spawn {
            name,
            kind,
            cwd,
            sandbox,
            model,
            rules,
            first_message,
        } => ControlRequest::Spawn {
            spec: AgentSpec {
                workspace: None,
                name: AgentName::parse(&name)?,
                kind: match kind {
                    Kind::Infinity => "infinity",
                    Kind::Claude => "claude",
                    Kind::Codex => "codex",
                }
                .into(),
                cwd: std::path::absolute(cwd)?,
                model,
                permissions: PermissionPolicy {
                    sandbox: match sandbox {
                        Sandbox::ReadOnly => SandboxLevel::ReadOnly,
                        Sandbox::WorkspaceWrite => SandboxLevel::WorkspaceWrite,
                        Sandbox::FullAccess => SandboxLevel::FullAccess,
                    },
                    rules,
                    ..Default::default()
                },
                idle_after: None,
            },
            first_message: first_message.map(Body::new).transpose()?,
            key,
        },
        Command::Ls => ControlRequest::Ls,
        Command::Send { to, text, act } => ControlRequest::Send {
            to,
            body: Body::new(text)?,
            key,
            act: act
                .map(|act| serde_json::from_str(&act).map_err(|e| format!("invalid --act: {e}")))
                .transpose()?,
        },
        Command::Approvals => ControlRequest::Approvals,
        Command::Approve { id, option } => ControlRequest::Approve {
            id: PermissionId(id.parse()?),
            option,
        },
        Command::Cancel { to } => ControlRequest::Cancel { to },
        Command::Stop { to } => ControlRequest::Stop { to },
        Command::Restart { to } => ControlRequest::Restart { to },
        Command::Held { to } => ControlRequest::Held { to },
        Command::Release { id } => ControlRequest::Release {
            id: MessageId(id.parse()?),
        },
        Command::Drop { id } => ControlRequest::Drop {
            id: MessageId(id.parse()?),
        },
        Command::Watch { cursor, nonce } => ControlRequest::Watch { cursor, nonce },
        Command::Ui => ControlRequest::Ui,
        Command::Tasks => ControlRequest::Tasks,
    };
    let watch = matches!(request, ControlRequest::Watch { .. });
    let held = matches!(request, ControlRequest::Held { .. });
    let mut held_mail = Vec::new();
    let socket = args
        .socket
        .or_else(|| std::env::var_os("AGENTD_SOCKET").map(PathBuf::from))
        .unwrap_or_else(|| default_state_dir().join("agentd.sock"));
    let stream = UnixStream::connect(socket).await?;
    Socket::check_peer(&stream)?;
    let (read, mut write) = stream.into_split();
    write_frame(&mut write, &request).await?;
    let mut read = BufReader::new(read);
    loop {
        let line = read_frame_limit(&mut read, 64 * 1024 * 1024)
            .await?
            .ok_or("broker closed connection")?;
        let value: Value = serde_json::from_slice(&line)?;
        if let Some(e) = value["error"].as_str() {
            return Err(e.to_owned().into());
        }
        if watch {
            println!("{value}");
            continue;
        }
        if held {
            if let Some(item) = value.get("held") {
                held_mail.push(item.clone());
                continue;
            }
            println!("{}", serde_json::to_string_pretty(&held_mail)?);
            return Ok(());
        }
        let result = &value["result"];
        if let Some(status) = result["status"].as_str() {
            println!("{status}");
        } else {
            println!("{}", serde_json::to_string_pretty(result)?);
        }
        return Ok(());
    }
}

#[cfg(test)]
mod rule_tests {
    use super::*;

    #[test]
    fn a_rule_is_a_tool_name_and_a_decision() {
        let rule = parse_rule("send_message=allow").expect("rule");
        assert_eq!(rule.tool, "send_message");
        assert!(matches!(rule.decision, RuleDecision::Allow));
        let rule = parse_rule("*=deny").expect("rule");
        assert_eq!(rule.tool, "*");
        assert!(matches!(rule.decision, RuleDecision::Deny));
        assert!(matches!(
            parse_rule("shell=ask").expect("rule").decision,
            RuleDecision::Ask
        ));
    }

    /// A rule that can never match would leave the operator believing it applies.
    #[test]
    fn a_rule_that_could_never_match_is_rejected() {
        for rule in [
            "send_message",
            "=allow",
            "send_message=",
            "send_message=Allow",
            "send_message=allow ",
            " send_message=allow",
            "send message=allow",
            "Send_Message=allow",
            "\u{200b}send_message=allow",
            "**=allow",
            "a=b=allow",
        ] {
            assert!(parse_rule(rule).is_err(), "{rule:?}");
        }
    }
}
