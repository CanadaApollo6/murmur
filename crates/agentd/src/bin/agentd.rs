#![deny(clippy::unwrap_used)]
use std::path::PathBuf;
use std::sync::Arc;

use agentd::mailroom::Mailroom;
use agentd::supervisor::Broker;
use agentd::surfaces::{
    Bindings, ControlApi, FileConfig, Result, Socket, default_state_dir, mcp_stdio, private_dir,
};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "Local ACP agent broker")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    /// MCP stdio relay. The broker injects AGENTD_MCP_TOKEN per session.
    Mcp {
        #[arg(long)]
        socket: PathBuf,
    },
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    if let Err(error) = run(Args::parse()).await {
        eprintln!("agentd: {error}");
        std::process::exit(1);
    }
}
async fn run(args: Args) -> Result<()> {
    if let Some(Command::Mcp { socket }) = args.command {
        let token = std::env::var("AGENTD_MCP_TOKEN").map_err(|_| "missing MCP session binding")?;
        return mcp_stdio(&socket, &token).await;
    }
    let initial_state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| initial_state.join("kinds.toml"));
    let cfg = if args.config.is_some() || config_path.exists() {
        FileConfig::load(&config_path)?
    } else {
        FileConfig::default()
    };
    let state = args
        .state_dir
        .or(cfg.state_dir.clone())
        .unwrap_or(initial_state);
    private_dir(&state)?;
    let state = std::fs::canonicalize(state)?;
    let control_path = args
        .socket
        .or(cfg.listen.control_socket.clone())
        .unwrap_or_else(|| state.join("agentd.sock"));
    let mcp_path = cfg
        .listen
        .mcp_socket
        .clone()
        .unwrap_or_else(|| state.join("mcp.sock"));
    let kinds = cfg.kinds()?;
    let mailroom = Arc::new(Mailroom::open(state.join("mail.db"), Default::default()).await?);
    let bindings = Arc::new(Bindings::new(mcp_path.clone(), std::env::current_exe()?));
    let broker = Arc::new(
        Broker::new(Arc::clone(&mailroom), kinds, Default::default())?
            .with_mcp(Arc::clone(&bindings)),
    );
    let result = serve(
        &control_path,
        &mcp_path,
        Arc::clone(&broker),
        Arc::clone(&mailroom),
        bindings,
    )
    .await;
    let shutdown = broker.shutdown().await;
    drop(broker);
    let close = Arc::try_unwrap(mailroom)
        .map_err(|_| "mailroom still has live owners")?
        .close()
        .await;
    result?;
    shutdown?;
    close?;
    Ok(())
}
async fn serve(
    control_path: &std::path::Path,
    mcp_path: &std::path::Path,
    broker: Arc<Broker>,
    mailroom: Arc<Mailroom>,
    bindings: Arc<Bindings>,
) -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let control = Socket::bind(control_path)?;
    let mcp = Socket::bind(mcp_path)?;
    for (_, spec) in mailroom.specs().await? {
        broker.spawn(spec).await?;
    }
    let api = Arc::new(ControlApi {
        broker,
        mailroom,
        bindings,
    });
    let mut connections = tokio::task::JoinSet::new();
    let result = loop {
        let accepted = tokio::select! {
            _ = interrupt.recv() => break Ok(()),
            _ = terminate.recv() => break Ok(()),
            stream = control.listener.accept() => stream.map(|(s,_)|(s,false)),
            stream = mcp.listener.accept() => stream.map(|(s,_)|(s,true)),
            result = connections.join_next(), if !connections.is_empty() => {
                match result {
                    Some(Ok(Err(e))) => eprintln!("agentd: connection closed: {e}"),
                    Some(Err(e)) => eprintln!("agentd: connection task failed: {e}"),
                    _ => {},
                }
                continue;
            }
        };
        let (stream, is_mcp) = match accepted {
            Ok(s) => s,
            Err(e) => break Err(e.into()),
        };
        if let Err(e) = Socket::check_peer(&stream) {
            eprintln!("agentd: {e}");
            continue;
        }
        let api = Arc::clone(&api);
        connections.spawn(async move { api.connection(stream, is_mcp).await });
    };
    connections.shutdown().await;
    result
}
