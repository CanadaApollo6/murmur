#![deny(clippy::unwrap_used)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use agentd::mailroom::Mailroom;
use agentd::supervisor::Broker;
use agentd::surfaces::{
    Bindings, BoundListener, ControlApi, FileConfig, Result, Socket, Surface, default_state_dir,
    mcp_stdio, private_dir,
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
    /// Re-capture configured bind identities after validating their sources.
    #[arg(long)]
    repin_binds: bool,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    /// MCP stdio relay. Sandboxed identity comes from the socket.
    #[command(hide = true)]
    SandboxInit {
        #[arg(long)]
        host_network: bool,
        #[arg(last = true, required = true)]
        cmd: Vec<std::ffi::OsString>,
    },
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
    match args.command {
        Some(Command::Mcp { socket }) => {
            return mcp_stdio(&socket, std::env::var("AGENTD_MCP_TOKEN").ok().as_deref()).await;
        }
        Some(Command::SandboxInit { host_network, cmd }) => {
            return agentd::sandbox::init(host_network, cmd).await;
        }
        None => {}
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
    let mut sandbox = cfg.bwrap.clone();
    sandbox.repin_binds = args.repin_binds;
    let mail_config = agentd::mailroom::Config {
        limits: cfg.mail.clone(),
        ..Default::default()
    };
    let rap_config = cfg.rap.clone();
    let ui_config = cfg.ui.clone();
    let ui = agentd::surfaces::ui::UiServer::bind(ui_config.clone(), &state).await?;
    let mut kinds = cfg.kinds()?;
    if ui.is_some() {
        for kind in &kinds {
            if matches!(kind.isolation, agentd::sandbox::Isolation::None) {
                eprintln!(
                    "agentd: WARNING UI enabled with unsandboxed kind {}: its agents can read ui.token",
                    kind.name
                );
            }
        }
    }
    let infinity_sockets: Vec<_> = kinds
        .iter()
        .filter_map(|kind| match &kind.wire {
            agentd::Wire::Infinity { socket } => Some(socket.clone()),
            _ => None,
        })
        .collect();
    let rap = if infinity_sockets.is_empty() {
        None
    } else {
        Some(agentd::surfaces::rap::RapServer::bind(&rap_config, infinity_sockets).await?)
    };
    sandbox.validate(&state, &control_path, &mcp_path, &mut kinds)?;
    if kinds
        .iter()
        .any(|k| matches!(k.isolation, agentd::sandbox::Isolation::Bwrap(_)))
    {
        for kind in &kinds {
            if matches!(kind.isolation, agentd::sandbox::Isolation::None) {
                eprintln!(
                    "agentd: WARNING mixed isolation: kind {} is unsandboxed and its agents can act as the human and as any agent",
                    kind.name
                );
            }
        }
    }
    if sandbox.network == agentd::sandbox::Network::Host {
        for kind in &kinds {
            if matches!(kind.isolation, agentd::sandbox::Isolation::Bwrap(_)) {
                eprintln!(
                    "agentd: WARNING kind {} uses bwrap with host network: host loopback, including local servers, is reachable",
                    kind.name
                );
            }
        }
    }
    agentd::sandbox::preflight(&sandbox, &kinds, &state).await?;
    let (bound_tx, bound_rx) = tokio::sync::mpsc::unbounded_channel();
    let mailroom = Arc::new(Mailroom::open(state.join("mail.db"), mail_config).await?);
    let bindings = Arc::new(Bindings::new(
        mcp_path.clone(),
        std::env::current_exe()?,
        bound_tx,
    ));
    let broker = Arc::new(
        Broker::new(Arc::clone(&mailroom), kinds, Default::default())?.with_mcp(
            Arc::clone(&bindings),
            state.clone(),
            sandbox,
        ),
    );
    let result = serve(
        &control_path,
        &mcp_path,
        Arc::clone(&broker),
        Arc::clone(&mailroom),
        bindings,
        bound_rx,
        (rap, ui),
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
    mut bound: tokio::sync::mpsc::UnboundedReceiver<BoundListener>,
    (rap, ui): (
        Option<agentd::surfaces::rap::RapServer>,
        Option<agentd::surfaces::ui::UiServer>,
    ),
) -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let control = Socket::bind(control_path)?;
    let mcp = Socket::bind(mcp_path)?;
    for (_, spec) in mailroom.specs().await? {
        broker.restore(spec).await?;
    }
    let api = Arc::new(ControlApi {
        broker,
        mailroom,
        bindings,
        control_path: control_path.to_owned(),
        ui_url: ui.as_ref().map(|ui| ui.url()),
    });
    let control_slots = Arc::new(Semaphore::new(64));
    let mcp_slots = Arc::new(Semaphore::new(64));
    let mut connections = tokio::task::JoinSet::new();
    let mut listeners = tokio::task::JoinSet::new();
    let rap_api = api.clone();
    let rap_server = async move {
        match rap {
            Some(rap) => rap.serve(rap_api).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(rap_server);
    let ui_api = api.clone();
    let ui_cancel = tokio_util::sync::CancellationToken::new();
    let cancel = ui_cancel.clone();
    let mut ui_server = tokio::spawn(async move {
        match ui {
            Some(ui) => ui.serve(ui_api, cancel).await,
            None => {
                cancel.cancelled().await;
                Ok(())
            }
        }
    });
    let mut ui_done = false;
    let result = loop {
        let accepted = tokio::select! {
            result = &mut rap_server => break result,
            result = &mut ui_server => {ui_done=true;break result?;},
            Some(listener) = bound.recv() => {
                let api = Arc::clone(&api);
                listeners.spawn(async move { serve_agent(api,listener).await });
                continue;
            }
            done = listeners.join_next(), if !listeners.is_empty() => {
                if let Some(result) = done {match result {Ok(Ok(()))=>{},other=>eprintln!("agentd: per-agent listener stopped: {other:?}")}}
                continue;
            }
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
            Err(e) => {
                eprintln!("agentd: accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        if let Err(e) = Socket::check_peer(&stream) {
            eprintln!("agentd: {e}");
            continue;
        }
        let slots = if is_mcp { &mcp_slots } else { &control_slots };
        let Ok(permit) = Arc::clone(slots).try_acquire_owned() else {
            eprintln!("agentd: connection limit reached (mcp={is_mcp})");
            continue;
        };
        let api = Arc::clone(&api);
        connections.spawn(async move {
            let _permit = permit;
            api.connection(
                stream,
                if is_mcp {
                    Surface::SharedMcp
                } else {
                    Surface::Control
                },
            )
            .await
        });
    };
    ui_cancel.cancel();
    if !ui_done {
        ui_server.await??;
    }
    listeners.shutdown().await;
    connections.shutdown().await;
    result
}

async fn serve_agent(api: Arc<ControlApi>, bound: BoundListener) -> Result<()> {
    let slots = Arc::new(Semaphore::new(64));
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = bound.cancel.cancelled() => break,
            accepted = bound.socket.listener.accept() => {
                let (stream,_) = accepted?;
                if let Err(error) = Socket::check_peer(&stream) {eprintln!("agentd: {error}"); continue;}
                let Ok(permit) = slots.clone().try_acquire_owned() else {continue;};
                let api = api.clone(); let surface = Surface::AgentMcp(bound.identity.clone());
                connections.spawn(async move {let _permit = permit; api.connection(stream,surface).await});
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = result {match result {Ok(Ok(()))=>{},other=>tracing::debug!(?other,"agent MCP connection closed")}}
            }
        }
    }
    connections.shutdown().await;
    Ok(())
}
