#![deny(clippy::unwrap_used)]
use std::path::PathBuf;

use agentd::model::*;
use agentd::supervisor::AgentSpec;
use agentd::surfaces::{ControlRequest, Result, default_state_dir, read_frame, write_frame};
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
        first_message: Option<String>,
    },
    Ls,
    Send {
        to: String,
        text: String,
    },
    Approvals,
    Approve {
        id: String,
        option: String,
    },
    Cancel {
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
    Watch,
}
#[derive(Clone, ValueEnum)]
enum Kind {
    Claude,
    Codex,
}
#[derive(Clone, ValueEnum)]
enum Sandbox {
    ReadOnly,
    WorkspaceWrite,
    FullAccess,
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
            first_message,
        } => ControlRequest::Spawn {
            spec: AgentSpec {
                name: AgentName::parse(&name)?,
                kind: match kind {
                    Kind::Claude => "claude",
                    Kind::Codex => "codex",
                }
                .into(),
                cwd: std::fs::canonicalize(cwd)?,
                model,
                permissions: PermissionPolicy {
                    sandbox: match sandbox {
                        Sandbox::ReadOnly => SandboxLevel::ReadOnly,
                        Sandbox::WorkspaceWrite => SandboxLevel::WorkspaceWrite,
                        Sandbox::FullAccess => SandboxLevel::FullAccess,
                    },
                    ..Default::default()
                },
                idle_after: None,
            },
            first_message: first_message.map(Body::new).transpose()?,
            key,
        },
        Command::Ls => ControlRequest::Ls,
        Command::Send { to, text } => ControlRequest::Send {
            to,
            body: Body::new(text)?,
            key,
        },
        Command::Approvals => ControlRequest::Approvals,
        Command::Approve { id, option } => ControlRequest::Approve {
            id: PermissionId(id.parse()?),
            option,
        },
        Command::Cancel { to } => ControlRequest::Cancel { to },
        Command::Held { to } => ControlRequest::Held { to },
        Command::Release { id } => ControlRequest::Release {
            id: MessageId(id.parse()?),
        },
        Command::Drop { id } => ControlRequest::Drop {
            id: MessageId(id.parse()?),
        },
        Command::Watch => ControlRequest::Watch,
    };
    let watch = matches!(request, ControlRequest::Watch);
    let socket = args
        .socket
        .or_else(|| std::env::var_os("AGENTD_SOCKET").map(PathBuf::from))
        .unwrap_or_else(|| default_state_dir().join("agentd.sock"));
    let stream = UnixStream::connect(socket).await?;
    let (read, mut write) = stream.into_split();
    write_frame(&mut write, &request).await?;
    let mut read = BufReader::new(read);
    loop {
        let line = read_frame(&mut read)
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
        let result = &value["result"];
        if let Some(status) = result["status"].as_str() {
            println!("{status}");
        } else {
            println!("{}", serde_json::to_string_pretty(result)?);
        }
        return Ok(());
    }
}
