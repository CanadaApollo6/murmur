#![deny(clippy::unwrap_used)]
use std::ffi::OsString;
use std::path::PathBuf;

use agentd::model::*;
use agentd::supervisor::AgentSpec;
use agentd::surfaces::{
    ControlRequest, Result, Socket, default_state_dir, read_frame_limit, write_frame,
};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use serde_json::Value;
use tokio::io::BufReader;
use tokio::net::UnixStream;

#[derive(Parser)]
#[command(name = "agentctl", about = "Control the local agentd broker")]
struct Args {
    /// Control socket path. Overrides `AGENTD_SOCKET`. Cannot be combined with `--state-dir`.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    /// The daemon's state directory. Selects `<DIR>/agentd.sock`, so it assumes the
    /// daemon uses the default socket name. Overrides `AGENTD_SOCKET`. Cannot be combined
    /// with `--socket`.
    #[arg(long, global = true, value_name = "DIR")]
    state_dir: Option<PathBuf>,
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
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = parse(std::env::args_os()).unwrap_or_else(|error| error.exit());
    if let Err(error) = run(args).await {
        eprintln!("agentctl: {error}");
        std::process::exit(1);
    }
}
/// Parses the command line and rejects `--socket` with `--state-dir`. clap's own
/// `conflicts_with` misses the pair when one flag is before the subcommand and the other
/// after it, and the command would then run against a socket the operator did not mean.
fn parse<I, T>(argv: I) -> std::result::Result<Args, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args = Args::try_parse_from(argv)?;
    if args.socket.is_some() && args.state_dir.is_some() {
        return Err(Args::command().error(
            clap::error::ErrorKind::ArgumentConflict,
            "the argument '--socket <SOCKET>' cannot be used with '--state-dir <DIR>'",
        ));
    }
    Ok(args)
}

fn socket_path(
    socket: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    env_socket: Option<OsString>,
) -> PathBuf {
    socket
        .or_else(|| state_dir.map(|dir| dir.join("agentd.sock")))
        .or_else(|| env_socket.map(PathBuf::from))
        .unwrap_or_else(|| default_state_dir().join("agentd.sock"))
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
    let socket = socket_path(
        args.socket,
        args.state_dir,
        std::env::var_os("AGENTD_SOCKET"),
    );
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
        // A feed event's own `error` field is data, not a failed request.
        if value.get("event").is_none()
            && let Some(e) = value["error"].as_str()
        {
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
mod tests {
    use super::*;

    fn resolve(argv: &[&str], env: Option<&str>) -> PathBuf {
        let args = parse(argv).expect("bug: test argv must parse");
        socket_path(args.socket, args.state_dir, env.map(OsString::from))
    }

    #[test]
    fn socket_flag_wins_over_env() {
        assert_eq!(
            resolve(
                &["agentctl", "--socket", "/s/a.sock", "ls"],
                Some("/env.sock")
            ),
            PathBuf::from("/s/a.sock")
        );
    }

    #[test]
    fn state_dir_selects_default_socket_name_over_env() {
        assert_eq!(
            resolve(
                &["agentctl", "ls", "--state-dir", "/run/state"],
                Some("/env.sock")
            ),
            PathBuf::from("/run/state/agentd.sock")
        );
    }

    #[test]
    fn env_socket_used_without_flags() {
        assert_eq!(
            resolve(&["agentctl", "ls"], Some("/env.sock")),
            PathBuf::from("/env.sock")
        );
    }

    #[test]
    fn default_state_dir_used_without_flags_or_env() {
        assert_eq!(
            resolve(&["agentctl", "ls"], None),
            default_state_dir().join("agentd.sock")
        );
    }

    #[test]
    fn socket_and_state_dir_together_are_a_usage_error_wherever_they_sit() {
        let (socket, dir) = (["--socket", "/s/a.sock"], ["--state-dir", "/run/state"]);
        let placements = [
            [&socket[..], &dir[..], &["ls"][..]].concat(),
            [&socket[..], &["ls"][..], &dir[..]].concat(),
            [&dir[..], &["ls"][..], &socket[..]].concat(),
            [&["ls"][..], &dir[..], &socket[..]].concat(),
        ];
        for placement in placements {
            let argv = [&["agentctl"][..], &placement[..]].concat();
            let error = parse(&argv)
                .err()
                .unwrap_or_else(|| panic!("{argv:?} must be rejected"));
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{argv:?}"
            );
        }
    }
}
