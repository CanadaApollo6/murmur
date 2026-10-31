//! Lazy, daemon-managed RAP tool servers.
//!
//! The daemon runs one agent system for its whole lifetime; sessions are just
//! groups of threads sharing a working directory. What *does* start and stop
//! with session activity are the session's RAP tool servers. This module
//! makes that lifecycle lazy:
//!
//! - A session's toolset is resolved the first time one of its threads runs a
//!   step (via [`SessionRapManager`]'s [`ThreadConfigSource`] impl): the
//!   configured servers are booted, their manifests fetched, and one
//!   [`ManagedRapTool`] built per remote tool definition.
//! - When the session goes idle (no live thread drivers, no keep-alive
//!   client), the daemon calls [`SessionRapManager::shutdown_session`], which
//!   stops the server processes but keeps the cached tool definitions.
//! - The next tool invocation boots the server back up transparently
//!   ([`ManagedRapServer::invoke_endpoint`]), so shutting servers down is
//!   transparent for non-archived sessions. Archived sessions cannot reboot
//!   servers or dispatch calls.
//!
//! Migration flows boot the same configured servers eagerly (outside any
//! session) via [`boot_migration_servers`], pairing source and destination
//! servers by config id and consulting each manifest's `needsMigration`
//! flag.

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::{InputMessage, InputMessageContent};
use infinity_agent_core::tools::ToolContext;
use infinity_agent_core::traits::InputSender;
use infinity_provider_protocol::message::UserContent;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use infinity_agent_core::system::local::ChannelSender;
use infinity_agent_core::system::{ThreadConfig, ThreadConfigSource};
use infinity_agent_core::tools::Tool;
use infinity_agent_core::tools::config::ToolSetConfig;
use infinity_agent_core::tools::rap_tool::{
    RapInvocationParams, RapToolDescriptor, invoke_rap_tool,
};
use infinity_protocol::DaemonMessage;
use rap_client::http::{HttpClient, SimpleHttpClient};
use rap_client::notifier::RapNotifier;
use rap_protocol::ToolsetManifest;

use crate::memory_store::PersistentConversationStore;
use crate::session::SessionStoreHandle;
use crate::session::observer::SubscriberMap;
use crate::{config, mcp_proxy, set_title_tool};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

type PendingCalls = Arc<Mutex<HashMap<(ThreadId, rap_protocol::ToolCallId), PendingCall>>>;

struct PendingCall {
    server_id: uuid::Uuid,
    invocation: uuid::Uuid,
    context: ToolContext<ChannelSender>,
    call_id: Option<rap_protocol::ProviderCallId>,
}

#[derive(Clone)]
pub(crate) struct RapInputSender {
    sender: ChannelSender,
    pending: PendingCalls,
    invocation: Option<uuid::Uuid>,
}

#[async_trait]
impl InputSender for RapInputSender {
    type Error = infinity_agent_core::system::local::ChannelSendError;

    async fn send_to_input_queue(
        &self,
        message: InputMessage,
        dedup_id: &str,
    ) -> Result<(), Self::Error> {
        let dedup_id = match self.invocation {
            Some(scope) => format!(
                "managed:{scope}:rap-tool-result-{}",
                match &message.content {
                    InputMessageContent::User(UserContent::ToolResult(result)) => &result.id,
                    _ => return self.sender.send_to_input_queue(message, dedup_id).await,
                }
            ),
            None => dedup_id.to_owned(),
        };
        if let Some(scoped) = dedup_id.strip_prefix("managed:")
            && message.synthetic.is_none()
        {
            let scope = scoped
                .split(':')
                .next()
                .and_then(|s| uuid::Uuid::parse_str(s).ok());
            let mut pending = self.pending.lock().expect("bug: pending calls poisoned");
            let key = pending
                .iter()
                .find(|(_, call)| Some(call.invocation) == scope)
                .map(|(key, _)| key.clone());
            let Some(key) = key else {
                tracing::debug!(%dedup_id, "ignoring callback for retired RAP invocation");
                return Ok(());
            };
            if matches!(
                message.content,
                InputMessageContent::User(UserContent::ToolResult(_))
            ) {
                pending.remove(&key);
            }
        }
        self.sender.send_to_input_queue(message, &dedup_id).await
    }
}

// ── ManagedRapServer ─────────────────────────────────────────────────────────

/// How to bring a configured RAP server up.
enum ServerSpec {
    /// A command spawned as a child process (`RAP_EMBEDDED=1`).
    Command { command: String, cwd: PathBuf },
    /// A stdio MCP server proxied behind an in-process RAP server. The proxy
    /// owns the child process; it is booted once and kept for the daemon's
    /// lifetime.
    Mcp {
        name: String,
        command: Vec<String>,
        env: HashMap<String, String>,
    },
    /// A remote MCP server proxied behind an in-process RAP server.
    HttpMcp {
        name: String,
        url: String,
        headers: HashMap<String, String>,
    },
    /// A pre-existing external server URL; never started or stopped by us.
    External { url: String },
}

#[expect(
    clippy::large_enum_variant,
    reason = "at most a handful of servers exist per session"
)]
enum ServerState {
    Down,
    Up {
        base_url: String,
        /// The server's manifest, fetched once per boot. (The invocation
        /// endpoint changes across reboots since command servers get a
        /// fresh port.)
        manifest: ToolsetManifest,
        /// The child process for command-based servers (killed on shutdown).
        child: Option<tokio::process::Child>,
        /// The accept-loop task of an in-process MCP proxy. Stored for
        /// ownership: unlike command servers, MCP servers may hold state
        /// that lives only as long as their process (RAP servers persist
        /// state via the thread ID, but MCP has no such contract), so
        /// [`shutdown`](ManagedRapServer::shutdown) leaves proxies running
        /// rather than restarting them across session idles. The task is
        /// only ever aborted to tear down a half-booted proxy (manifest
        /// fetch failure) before it is stored here.
        #[expect(dead_code, reason = "held for ownership; never polled")]
        task: Option<tokio::task::JoinHandle<()>>,
    },
}

/// A handle to one configured RAP server that the daemon can boot lazily and
/// shut down at will. Tools hold an `Arc` to it and call
/// [`invoke_endpoint`](Self::invoke_endpoint) per invocation, so a server
/// stopped while its session idled reboots transparently on the next call.
pub struct ManagedRapServer {
    /// Config id (for migration bookkeeping), when present.
    pub config_id: Option<String>,
    session_id: ThreadId,
    spec: ServerSpec,
    state: tokio::sync::Mutex<ServerState>,
    id: uuid::Uuid,
    pending: PendingCalls,
    session_store: Option<SessionStoreHandle>,
}

impl ManagedRapServer {
    fn new(session_id: ThreadId, config_id: Option<String>, spec: ServerSpec) -> Self {
        Self {
            config_id,
            session_id,
            spec,
            state: tokio::sync::Mutex::new(ServerState::Down),
            id: uuid::Uuid::new_v4(),
            session_store: None,
            pending: PendingCalls::default(),
        }
    }

    async fn archived(&self) -> bool {
        match &self.session_store {
            Some(store) => store
                .lock()
                .await
                .sessions
                .get(&self.session_id)
                .is_some_and(|entry| entry.archived),
            None => false,
        }
    }

    /// The server's base URL if it is currently up.
    pub async fn current_base_url(&self) -> Option<String> {
        match &*self.state.lock().await {
            ServerState::Up { base_url, .. } => Some(base_url.clone()),
            ServerState::Down => None,
        }
    }

    /// Boot the server if it is down and return its manifest (cached for the
    /// lifetime of the boot). If the server boots but its manifest cannot be
    /// fetched, the boot is torn down and the server stays down, so `Up`
    /// always has a usable manifest.
    pub async fn ensure_up(&self) -> Result<ToolsetManifest, BoxError> {
        let mut state = self.state.lock().await;
        if self.archived().await {
            return Err("archived session cannot boot RAP servers".into());
        }
        if let ServerState::Up { manifest, .. } = &*state {
            return Ok(manifest.clone());
        }

        let (base_url, mut child, task) = match &self.spec {
            ServerSpec::Command { command, cwd } => {
                tracing::info!(
                    "Booting RAP server for session {}: {command}",
                    self.session_id
                );
                let (child, port) = spawn_rap_server(command, cwd).await?;
                (format!("http://127.0.0.1:{port}"), Some(child), None)
            }
            ServerSpec::Mcp { name, command, env } => {
                let (port, task) =
                    mcp_proxy::start_mcp_proxy(name.clone(), command.clone(), env.clone()).await?;
                (format!("http://127.0.0.1:{port}"), None, Some(task))
            }
            ServerSpec::HttpMcp { name, url, headers } => {
                let (port, task) =
                    mcp_proxy::start_http_mcp_proxy(name.clone(), url.clone(), headers.clone())
                        .await?;
                (format!("http://127.0.0.1:{port}"), None, Some(task))
            }
            ServerSpec::External { url } => (url.clone(), None, None),
        };

        match fetch_manifest(&base_url).await {
            Ok(manifest) => {
                *state = ServerState::Up {
                    base_url,
                    manifest: manifest.clone(),
                    child,
                    task,
                };
                Ok(manifest)
            }
            Err(e) => {
                // Tear the boot down; the next call retries from scratch.
                if let Some(child) = &mut child {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
                if let Some(task) = task {
                    task.abort();
                }
                Err(e)
            }
        }
    }

    /// The invocation endpoint, booting the server first if necessary.
    pub async fn invoke_endpoint(&self) -> Result<String, BoxError> {
        Ok(self.ensure_up().await?.endpoint)
    }

    /// Stop the server. Safe to call at any time: the next invocation (or
    /// manifest fetch) boots it back up. Only command-based child processes
    /// are stopped — RAP servers persist their state keyed by thread ID, so
    /// a restart is transparent. MCP servers have no such contract (one may
    /// hold state that lives only as long as its process), so MCP proxies
    /// are left running; external servers are not ours to stop.
    pub async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        match &self.spec {
            ServerSpec::Command { .. } => {
                if let ServerState::Up { child, .. } = &mut *state
                    && let Some(mut child) = child.take()
                {
                    #[cfg(unix)]
                    {
                        use nix::sys::signal::{self, Signal};
                        use nix::unistd::Pid;
                        if let Some(id) = child.id() {
                            // Negative ID signals the entire process group.
                            let _ = signal::kill(Pid::from_raw(-(id as i32)), Signal::SIGINT);
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = child.start_kill();
                    }
                    let _ = child.wait().await;
                    tracing::info!("Stopped RAP server for session {}", self.session_id);
                }
                *state = ServerState::Down;
                let stopped = {
                    let mut pending = self.pending.lock().expect("bug: pending calls poisoned");
                    let keys: Vec<_> = pending
                        .iter()
                        .filter(|(_, call)| call.server_id == self.id)
                        .map(|(key, _)| key.clone())
                        .collect();
                    keys.into_iter()
                        .filter_map(|key| pending.remove(&key).map(|call| (key.1, call)))
                        .collect::<Vec<_>>()
                };
                for (id, call) in stopped {
                    let scope = call.invocation;
                    let infinity_rap_bridge::CallbackDelivery::Input { message, dedup_id } =
                        infinity_rap_bridge::prepare_callback(
                            rap_protocol::RapCallback::ToolResult(rap_protocol::RapToolResult {
                                group_id: call.context.group_id,
                                id,
                                call_id: call.call_id,
                                text: Some("Error: tool server stopped by session shutdown".into()),
                                content: None,
                                display_as: None,
                                subscription: None,
                            }),
                        )
                    else {
                        unreachable!()
                    };
                    if let Err(error) = call
                        .context
                        .message_sender
                        .send_to_input_queue(*message, &format!("managed:{scope}:{dedup_id}"))
                        .await
                    {
                        tracing::warn!(%error, "failed to settle stopped RAP call");
                    }
                }
            }
            // In-process proxies and external servers are not ours to stop.
            ServerSpec::Mcp { .. } | ServerSpec::HttpMcp { .. } | ServerSpec::External { .. } => {}
        }
    }
}

async fn fetch_manifest(base_url: &str) -> Result<ToolsetManifest, BoxError> {
    let http = SimpleHttpClient::new();
    let url = format!("{}/.well-known/rap-toolset", base_url.trim_end_matches('/'));
    let (status, body) = http
        .get(&url)
        .await
        .map_err(|e| format!("failed to fetch toolset manifest from {url}: {e}"))?;
    if !(200..300).contains(&status) {
        return Err(format!("toolset manifest fetch from {url} returned status {status}").into());
    }
    let manifest: ToolsetManifest = serde_json::from_slice(&body)
        .map_err(|e| format!("invalid toolset manifest from {url}: {e}"))?;
    Ok(manifest)
}

// ── Migration boots ──────────────────────────────────────────────────────────

/// A RAP server booted eagerly for a migration flow. Migration servers are
/// ordinary configured RAP servers — the same specs the session runtime
/// boots lazily — brought up outside any session so the migration protocol
/// can be spoken to the ones whose manifest declares `needsMigration`.
///
/// Owns the underlying server: dropping this kills a command server's child
/// process (`kill_on_drop`), which bounds the servers' lifetime to the
/// migration flow. MCP proxies detach and keep running, as everywhere else.
pub struct MigrationServer {
    /// The server's config id, when its config entry has one (migration
    /// pairing between source and destination is keyed by these ids).
    pub config_id: Option<String>,
    /// The server's reachable base URL.
    pub url: String,
    /// The local port, for servers we booted ourselves (`None` for external
    /// `toolset_server` URLs).
    pub port: Option<u16>,
    /// Whether the server's manifest declared `needsMigration: true`.
    pub needs_migration: bool,
    _server: ManagedRapServer,
}

/// Boot every server in `cwd`'s merged RAP config and fetch its manifest.
/// Servers that fail to boot are skipped with a warning, matching the
/// session runtime's tolerance for individual server failures.
pub async fn boot_migration_servers(
    cwd: &Path,
    user_config_path: Option<&Path>,
) -> Result<Vec<MigrationServer>, BoxError> {
    let (_source_info, specs) =
        collect_server_specs(&ThreadId::from("migration"), cwd, user_config_path)?;
    let mut servers = Vec::new();
    for server in specs {
        let manifest = match server.ensure_up().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("failed to boot RAP server for migration: {e}");
                continue;
            }
        };
        let url = server
            .current_base_url()
            .await
            .expect("bug: server is up right after ensure_up");
        // Only servers we booted ourselves have a meaningful local port
        // (external toolset_server URLs are excluded from port maps, since
        // migration tunnels forward local ports).
        let port = if matches!(server.spec, ServerSpec::External { .. }) {
            None
        } else {
            url.rsplit_once(':')
                .and_then(|(_, p)| p.trim_end_matches('/').parse().ok())
        };
        servers.push(MigrationServer {
            config_id: server.config_id.clone(),
            url,
            port,
            needs_migration: manifest.needs_migration,
            _server: server,
        });
    }
    Ok(servers)
}

/// The `config_id → port` map for the servers that need migration — the
/// payload of [`DaemonMessage::RapServersBooted`].
pub fn migration_server_ports(servers: &[MigrationServer]) -> HashMap<String, u16> {
    servers
        .iter()
        .filter(|s| s.needs_migration)
        .filter_map(|s| Some((s.config_id.clone()?, s.port?)))
        .collect()
}

/// Spawn a command-based RAP server (`RAP_EMBEDDED=1`) and wait for it to
/// report its port on stdout. Used both by the lazy per-session server
/// management and by the eager migration boots above.
async fn spawn_rap_server(
    command: &str,
    cwd: &Path,
) -> Result<(tokio::process::Child, u16), BoxError> {
    use infinity_agent_core::tools::config::CommandServerReady;
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt;

    let working_dir = cwd.join(".infinity");
    std::fs::create_dir_all(&working_dir).ok();

    let mut child = tokio::process::Command::new("sh")
        .args(["-c", command])
        .env("RAP_EMBEDDED", "1")
        .current_dir(&working_dir)
        // Ensure all children are in the the same process group. We will send SIGINT to the entire
        // group during shutdown.
        .process_group(0)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("failed to spawn '{command}': {e}"))?;

    let stdout = child.stdout.take().ok_or("no stdout")?;
    let mut reader = tokio::io::BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .map_err(|e| format!("failed to read startup line: {e}"))?;

    if line.is_empty() {
        let _ = child.kill().await;
        return Err("server exited before emitting port".into());
    }

    let ready: CommandServerReady = serde_json::from_str(line.trim())
        .map_err(|e| format!("invalid startup JSON: {e} (got: {line})"))?;
    Ok((child, ready.port))
}

// ── ManagedRapTool ───────────────────────────────────────────────────────────

/// A RAP tool bound to a [`ManagedRapServer`]: each invocation resolves the
/// server's current endpoint, booting it first if it was shut down.
pub struct ManagedRapTool {
    pub descriptor: RapToolDescriptor,
    pub server: Arc<ManagedRapServer>,
    pub http_client: SimpleHttpClient,
}

#[async_trait]
impl Tool<ChannelSender> for ManagedRapTool {
    fn name(&self) -> &str {
        &self.descriptor.name
    }

    fn description(&self) -> &str {
        &self.descriptor.description
    }

    fn parameters(&self) -> serde_json::Value {
        self.descriptor.parameters.clone()
    }

    fn display_script(&self) -> Option<&str> {
        self.descriptor.display_script.as_deref()
    }

    fn retire_call(&self, thread: &ThreadId<str>, id: &rap_protocol::ToolCallId<str>) {
        let mut pending = self
            .server
            .pending
            .lock()
            .expect("bug: pending calls poisoned");
        let key = (thread.to_owned(), id.to_owned());
        if let Some(invocation) = pending.get(&key).map(|call| call.invocation) {
            pending.retain(|_, call| call.invocation != invocation);
        }
    }

    async fn execute(
        &self,
        args: serde_json::Value,
        id: rap_protocol::ToolCallId,
        call_id: Option<rap_protocol::ProviderCallId>,
        context: &ToolContext<ChannelSender>,
    ) -> Result<(), BoxError> {
        // Register under the server lock so shutdown sees every dispatched call.
        let state = loop {
            self.server.ensure_up().await?;
            let state = self.server.state.lock().await;
            if matches!(*state, ServerState::Up { .. }) {
                break state;
            }
        };
        let ServerState::Up { manifest, .. } = &*state else {
            unreachable!()
        };
        if self.server.archived().await {
            return Err("archived session cannot dispatch RAP calls".into());
        }
        let key = (context.group_id.clone(), id.clone());
        let invocation =
            matches!(self.server.spec, ServerSpec::Command { .. }).then(uuid::Uuid::new_v4);
        if let Some(invocation) = invocation {
            self.server
                .pending
                .lock()
                .expect("bug: pending calls poisoned")
                .insert(
                    key,
                    PendingCall {
                        server_id: self.server.id,
                        invocation,
                        context: context.clone(),
                        call_id: call_id.clone(),
                    },
                );
        }
        let endpoint = manifest.endpoint.clone();
        drop(state);
        let tracked_context = ToolContext {
            message_sender: RapInputSender {
                sender: context.message_sender.clone(),
                pending: self.server.pending.clone(),
                invocation,
            },
            group_id: context.group_id.clone(),
            callback_url: invocation.map_or_else(
                || context.callback_url.clone(),
                |scope| {
                    format!(
                        "{}/managed/{scope}",
                        context.callback_url.trim_end_matches('/')
                    )
                },
            ),
            user_id: context.user_id.clone(),
            thread_stack: context.thread_stack.clone(),
            safe_spawn_point: context.safe_spawn_point,
        };
        let result = invoke_rap_tool(
            &self.http_client,
            RapInvocationParams {
                endpoint: &endpoint,
                operation: &self.descriptor.name,
                arguments: args,
                id: id.clone(),
                call_id: call_id.clone(),
                callback_url: None,
            },
            &tracked_context,
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "RAP invocation failed");
            let failure_id = id.to_string();
            // Command failures share the invocation scope with callbacks and shutdown.
            let infinity_rap_bridge::CallbackDelivery::Input { message, dedup_id } =
                infinity_rap_bridge::prepare_callback(rap_protocol::RapCallback::ToolResult(
                    rap_protocol::RapToolResult {
                        group_id: context.group_id.clone(),
                        id,
                        call_id,
                        text: Some("Error: Tool call failed".into()),
                        content: None,
                        display_as: None,
                        subscription: None,
                    },
                ))
            else {
                unreachable!()
            };
            tracked_context
                .message_sender
                .send_to_input_queue(
                    *message,
                    if invocation.is_some() {
                        &dedup_id
                    } else {
                        &failure_id
                    },
                )
                .await?;
        }
        Ok(())
    }
}

// ── SessionRapManager ────────────────────────────────────────────────────────

/// A session's resolved toolset: the tool instances shared by all of the
/// session's threads plus the managed servers behind them.
struct SessionToolset {
    tools: Vec<Rc<dyn Tool<ChannelSender>>>,
    servers: Vec<Arc<ManagedRapServer>>,
    extra_system_prompt: String,
}

/// Daemon-wide manager of per-session toolsets and their RAP servers. Also
/// the agent system's [`ThreadConfigSource`]: each thread resolves to its
/// session's toolset.
#[derive(Clone)]
pub struct SessionRapManager {
    toolsets: Rc<tokio::sync::Mutex<HashMap<ThreadId, Rc<SessionToolset>>>>,
    conversation_store: PersistentConversationStore,
    session_store: SessionStoreHandle,
    user_rap_config: Option<PathBuf>,
    /// Used to surface boot progress to the session's subscribers.
    subscriber_map: SubscriberMap,
    /// Sessions whose servers have been shut down at least once (observable
    /// for tests and diagnostics).
    shutdown_log: Rc<std::cell::RefCell<Vec<ThreadId>>>,
    pending: PendingCalls,
    servers: Rc<std::cell::RefCell<Vec<Weak<ManagedRapServer>>>>,
}

impl SessionRapManager {
    pub fn new(
        conversation_store: PersistentConversationStore,
        session_store: SessionStoreHandle,
        user_rap_config: Option<PathBuf>,
        subscriber_map: SubscriberMap,
    ) -> Self {
        Self {
            toolsets: Rc::new(tokio::sync::Mutex::new(HashMap::new())),
            conversation_store,
            session_store,
            user_rap_config,
            subscriber_map,
            shutdown_log: Rc::new(std::cell::RefCell::new(Vec::new())),
            pending: PendingCalls::default(),
            servers: Rc::default(),
        }
    }

    pub(crate) fn callback_sender(&self, sender: ChannelSender) -> RapInputSender {
        RapInputSender {
            sender,
            pending: self.pending.clone(),
            invocation: None,
        }
    }

    /// Send an informational message to the session's subscribers.
    fn info(&self, session_id: &ThreadId<str>, text: String) {
        let msg = DaemonMessage::Info {
            thread_id: Some(infinity_protocol::ThreadRef::local(session_id.to_owned())),
            text,
        };
        crate::session::observer::broadcast_to_thread(&self.subscriber_map, session_id, &msg, None);
    }

    /// Get (or lazily build) the toolset for a session: read the RAP config
    /// for the session's cwd, boot the servers, fetch their manifests, and
    /// build the tool instances.
    async fn get_or_init(
        &self,
        session_id: &ThreadId<str>,
    ) -> Result<Rc<SessionToolset>, BoxError> {
        let mut toolsets = self.toolsets.lock().await;
        if let Some(ts) = toolsets.get(session_id) {
            return Ok(ts.clone());
        }

        let cwd = {
            let store = self.session_store.lock().await;
            store.get_cwd(session_id).clone()
        };

        let mut servers: Vec<Arc<ManagedRapServer>> = Vec::new();
        match collect_server_specs(session_id, &cwd, self.user_rap_config.as_deref()) {
            Ok((source_info, specs)) => {
                self.info(session_id, source_info);
                for mut server in specs {
                    server.pending = self.pending.clone();
                    server.session_store = Some(self.session_store.clone());
                    servers.push(Arc::new(server));
                }
            }
            Err(e) => {
                self.info(
                    session_id,
                    format!("Warning: failed to read RAP config: {e}"),
                );
            }
        }

        // Boot each server once to discover its tools.
        let http_client = SimpleHttpClient::new();
        let mut tools: Vec<Rc<dyn Tool<ChannelSender>>> = Vec::new();
        let mut tool_count = 0usize;
        for server in &servers {
            match server.ensure_up().await {
                Ok(manifest) => {
                    for def in manifest.tools {
                        tool_count += 1;
                        tools.push(Rc::new(ManagedRapTool {
                            descriptor: def.into(),
                            server: server.clone(),
                            http_client: http_client.clone(),
                        }));
                    }
                }
                Err(e) => {
                    self.info(
                        session_id,
                        format!("Warning: failed to start RAP server: {e}"),
                    );
                }
            }
        }
        if tool_count > 0 {
            self.info(session_id, format!("Loaded {tool_count} RAP tool(s)"));
        }

        tools.push(Rc::new(set_title_tool::SetTitleTool {
            conversation_store: self.conversation_store.clone(),
        }));

        let extra_system_prompt = format!(
            "The user's current working directory is: {cwd:?}\n\n\
             Use the `set_title` tool to give the current thread a short, descriptive title. \
             Set it once at the start when the user's intent becomes clear, and update it only \
             when the overall scope of work changes significantly. Do not call it repeatedly \
             for minor follow-ups within the same task."
        );

        self.servers
            .borrow_mut()
            .extend(servers.iter().map(Arc::downgrade));
        let toolset = Rc::new(SessionToolset {
            tools,
            servers,
            extra_system_prompt,
        });
        toolsets.insert(session_id.to_owned(), toolset.clone());
        Ok(toolset)
    }

    /// Stop the session's RAP servers (keeping the cached toolset, so a later
    /// tool invocation reboots them transparently). Safe to call at any time.
    pub async fn shutdown_session(&self, session_id: &ThreadId<str>) {
        // Drivers can retain an evicted toolset and reboot its server handles.
        let servers: Vec<_> = {
            let _toolsets = self.toolsets.lock().await;
            let mut servers = self.servers.borrow_mut();
            servers.retain(|server| server.strong_count() > 0);
            servers
                .iter()
                .filter_map(Weak::upgrade)
                .filter(|server| server.session_id.as_str() == session_id.as_str())
                .collect()
        };
        for server in servers {
            server.shutdown().await;
        }
        self.shutdown_log.borrow_mut().push(session_id.to_owned());
    }

    /// Stop the session's servers and drop its cached toolset entirely, so
    /// the next use re-reads the RAP config and re-fetches manifests. Used
    /// when a session is explicitly shut down.
    pub async fn evict_session(&self, session_id: &ThreadId<str>) {
        self.shutdown_session(session_id).await;
        self.toolsets.lock().await.remove(session_id);
    }

    /// Stop every managed server (daemon exit).
    pub async fn shutdown_all(&self) {
        let session_ids: HashSet<ThreadId> = self
            .servers
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .map(|server| server.session_id.clone())
            .collect();
        for session_id in session_ids {
            self.shutdown_session(&session_id).await;
        }
    }

    /// How many times [`shutdown_session`](Self::shutdown_session) has been
    /// invoked for `session_id` (diagnostics / tests).
    pub fn times_shut_down(&self, session_id: &ThreadId<str>) -> usize {
        self.shutdown_log
            .borrow()
            .iter()
            .filter(|s| *s == session_id)
            .count()
    }
}

#[async_trait(?Send)]
impl ThreadConfigSource<ChannelSender, SimpleHttpClient> for SessionRapManager {
    async fn resolve(
        &self,
        thread_id: &ThreadId<str>,
    ) -> Result<ThreadConfig<ChannelSender, SimpleHttpClient>, BoxError> {
        let session_id = self.conversation_store.get_root_thread_id(thread_id);
        let toolset = self.get_or_init(&session_id).await?;

        // Best-effort lifecycle notifications go to the servers that are
        // currently up (a server that is down has nothing in flight).
        let mut urls = Vec::new();
        for server in &toolset.servers {
            if let Some(url) = server.current_base_url().await {
                urls.push(url);
            }
        }
        Ok(ThreadConfig {
            tools: toolset.tools.clone(),
            extra_system_prompt: Some(toolset.extra_system_prompt.clone()),
            rap_notifier: Some(RapNotifier::new(urls, SimpleHttpClient::new())),
        })
    }
}

/// Read the merged RAP config for `cwd` and build the (un-booted) server
/// handles. Also returns a human-readable line describing which config
/// source(s) were used, for display to the session's subscribers.
fn collect_server_specs(
    session_id: &ThreadId<str>,
    cwd: &Path,
    user_config_path: Option<&Path>,
) -> Result<(String, Vec<ManagedRapServer>), BoxError> {
    let (source_info, merged) = config::load_merged_rap_config(cwd, user_config_path)?;
    let Some(merged) = merged else {
        return Ok((source_info, Vec::new()));
    };

    let mut servers = Vec::new();
    for entry in merged.tool_sets {
        let (id, spec) = match entry {
            ToolSetConfig::ToolsetServer { server_url, id } => {
                (id, ServerSpec::External { url: server_url })
            }
            ToolSetConfig::ToolsetCommand { command, id, .. } => (
                id,
                ServerSpec::Command {
                    command,
                    cwd: cwd.to_path_buf(),
                },
            ),
            ToolSetConfig::McpServer {
                name,
                command,
                env,
                id,
            } => (id, ServerSpec::Mcp { name, command, env }),
            ToolSetConfig::HttpMcpServer {
                name,
                url,
                headers,
                id,
            } => (id, ServerSpec::HttpMcp { name, url, headers }),
        };
        servers.push(ManagedRapServer::new(session_id.to_owned(), id, spec));
    }
    Ok((source_info, servers))
}

#[cfg(test)]
mod r14a_registry {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::client_handler::handle_client_channels;
    use crate::ids::SequentialIdSource;
    use crate::rap_callback;
    use crate::session::{SessionManager, SessionManagerConfig, SharedSessionManager};
    use infinity_protocol::{ClientMessage, DaemonMessage};
    use infinity_provider_protocol::mock::{MockModelController, mock_model};
    use infinity_provider_protocol::{ModelEntry, ModelProvider, SingleModelProvider};
    use tokio::sync::mpsc;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    struct TestHarness {
        manager: SharedSessionManager,
        ctrl: MockModelController,
        client_tx: mpsc::UnboundedSender<ClientMessage>,
        daemon_rx: mpsc::UnboundedReceiver<DaemonMessage>,
        _state_dir: tempfile::TempDir,
        cwd: tempfile::TempDir,
    }

    /// Boot a session manager with a mock model, serve its RAP callback listener,
    /// and connect one client. Must run inside a `LocalSet`.
    async fn start_harness() -> Result<TestHarness, BoxError> {
        let (model, ctrl) = mock_model();
        let entry = ModelEntry {
            model_id: "mock-model".to_owned(),
            display_name: "Mock Model".to_owned(),
            context_window: 100_000,
            max_output_tokens: None,
            supports_image_input: false,
        };
        let state_dir = tempfile::tempdir()?;
        let cwd = tempfile::tempdir()?;

        let bridge = infinity_rap_bridge::RapCallbackBridge::bind().await?;
        let callback_url = bridge.callback_url().to_owned();
        let manager = SessionManager::with_providers(
            SessionManagerConfig {
                state_dir: state_dir.path().to_path_buf(),
                callback_url: callback_url.clone(),
                user_rap_config: None,
                id_source: Arc::new(SequentialIdSource::new()),
            },
            vec![(
                "mock".to_owned(),
                Arc::new(SingleModelProvider::new(entry, model)) as Arc<dyn ModelProvider>,
            )],
            vec![],
        )
        .await?;
        let manager = rap_callback::serve_callbacks(bridge, manager);

        let (client_tx, client_rx) = mpsc::unbounded_channel::<ClientMessage>();
        let (daemon_tx, daemon_rx) = mpsc::unbounded_channel::<DaemonMessage>();
        tokio::task::spawn_local(handle_client_channels(
            client_rx,
            daemon_tx,
            manager.clone(),
        ));

        Ok(TestHarness {
            manager,
            ctrl,
            client_tx,
            daemon_rx,
            _state_dir: state_dir,
            cwd,
        })
    }

    /// Receive daemon messages until the predicate matches, with a timeout.
    async fn wait_for_message(
        rx: &mut mpsc::UnboundedReceiver<DaemonMessage>,
        mut pred: impl FnMut(&DaemonMessage) -> bool,
    ) -> DaemonMessage {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let msg = rx
                    .recv()
                    .await
                    .expect("daemon channel closed while waiting");
                if pred(&msg) {
                    return msg;
                }
            }
        })
        .await
        .expect("timed out waiting for daemon message")
    }

    /// Create a session, run one chat round, and return the session id.
    async fn create_session_and_chat(h: &mut TestHarness) -> infinity_protocol::ThreadRef {
        h.client_tx
            .send(ClientMessage::CreateSession {
                cwd: h.cwd.path().to_path_buf(),
                location: None,
                model: None,
                keeps_session_alive: true,
                input_admitted: false,
            })
            .expect("send CreateSession");

        let connected = wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m, DaemonMessage::Connected { .. })
        })
        .await;
        let DaemonMessage::Connected {
            root_thread_id: session_id,
            ..
        } = connected
        else {
            unreachable!()
        };

        h.client_tx
            .send(ClientMessage::UserInput {
                thread_id: session_id.clone(),
                text: "hello".to_owned(),
            })
            .expect("send UserInput");

        let _req = h.ctrl.next_request().await;
        h.ctrl.send_text("hi there");
        h.ctrl.finish();

        wait_for_message(&mut h.daemon_rx, |m| {
            matches!(m, DaemonMessage::ResponseDone { .. })
        })
        .await;

        session_id
    }

    async fn r10a_waiting_server() -> std::io::Result<u16> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        tokio::task::spawn_local(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                tokio::task::spawn_local(async move {
                    let service = hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| async move {
                            use http_body_util::BodyExt;
                            let response = if req.uri().path() == "/.well-known/rap-toolset" {
                                serde_json::json!({"name":"choice", "endpoint":format!("http://127.0.0.1:{port}/invoke"), "tools":[{"name":"waiting_choice","description":"waits for choice","inputSchema":{"type":"object"}}]}).to_string()
                            } else {
                                req.into_body().collect().await.expect("body");
                                "{}".into()
                            };
                            Ok::<_, std::convert::Infallible>(hyper::Response::new(
                                http_body_util::Full::new(bytes::Bytes::from(response)),
                            ))
                        },
                    );
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await
                        .expect("serve");
                });
            }
        });
        Ok(port)
    }

    async fn cancellation_case(command: bool) {
        tokio::task::LocalSet::new().run_until(async {
        let mut h = start_harness().await.expect("harness");
        let marker = h.cwd.path().join("invoked");
        if command {
            std::fs::create_dir(h.cwd.path().join(".infinity")).expect("config dir");
            let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../agentd/tests/support/delayed_rap.py");
            std::fs::write(h.cwd.path().join(".infinity/rap.json"), serde_json::json!({"tool_sets":[{"type":"toolset_command", "command":format!("python3 '{}' '{}'", script.display(), marker.display())}]}).to_string()).expect("config");
        } else {
            let port = r10a_waiting_server().await.expect("external RAP");
            rap_test_servers::write_rap_config(h.cwd.path(), port).expect("config");
        }
        let root = create_session_and_chat(&mut h).await;
        wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;
        for i in 0..3 {
            h.client_tx.send(ClientMessage::UserInput { thread_id: root.clone(), text: "run work".into() }).expect("input");
            h.ctrl.next_request().await;
            h.ctrl.send_tool_call(&format!("external-{i}"), if command { "delayed_work" } else { "waiting_choice" }, serde_json::json!({})); h.ctrl.finish();
            wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ResponseDone { .. })).await;
            if command {
                tokio::time::timeout(Duration::from_secs(3), async { while !marker.exists() { tokio::time::sleep(Duration::from_millis(10)).await; } }).await.expect("dispatched");
                std::fs::remove_file(&marker).expect("marker");
            } else { tokio::time::sleep(Duration::from_millis(100)).await; }
            h.client_tx.send(ClientMessage::Interrupt { thread_id: root.clone() }).expect("interrupt");
            wait_for_message(&mut h.daemon_rx, |m| matches!(m, DaemonMessage::ThreadIdle { .. })).await;
        }
        assert!(h.manager.lock().await.rap_manager.pending.lock().expect("registry").is_empty(), "cancelled calls retire before shutdown");
        h.client_tx.send(ClientMessage::ShutdownSession { root_thread_id: root.clone() }).expect("quit");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let manager = h.manager.lock().await;
        let count = manager.rap_manager.pending.lock().expect("registry").len();
        eprintln!("R14 cancelled external calls still registered after quit: {count}");
        assert_eq!(count, 0, "settled calls must not leak for the daemon lifetime");
    }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r14a_cancelled_external_calls_retire() {
        cancellation_case(false).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn r14a_cancelled_command_calls_retire() {
        cancellation_case(true).await;
    }
}
