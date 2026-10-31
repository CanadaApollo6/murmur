//! Linux namespace launch plans. The adapter only sees a program and its environment.
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use crate::{
    KindConfig,
    model::*,
    supervisor::AgentSpec,
    surfaces::{Result, private_dir},
};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SandboxMode {
    Bwrap,
    None,
}
impl Default for SandboxMode {
    fn default() -> Self {
        if cfg!(target_os = "linux") {
            Self::Bwrap
        } else {
            Self::None
        }
    }
}
#[derive(Clone, Debug)]
pub enum Isolation {
    None,
    Bwrap(KindSandbox),
}
#[derive(Clone, Debug)]
pub struct KindSandbox {
    pub home: Vec<String>,
    pub credentials: Vec<PathBuf>,
    pub egress: Vec<String>,
}
impl KindSandbox {
    pub fn defaults(name: &str) -> Self {
        let (home, credentials, hosts) = if name == "claude" {
            (
                vec![".claude/", ".claude.json"],
                ".claude/.credentials.json",
                vec![
                    "api.anthropic.com",
                    "console.anthropic.com",
                    "platform.claude.com",
                ],
            )
        } else {
            (
                vec![".codex/"],
                ".codex/auth.json",
                vec!["chatgpt.com", "auth.openai.com", "api.openai.com"],
            )
        };
        Self {
            home: home.into_iter().map(str::to_owned).collect(),
            credentials: vec![host_home().join(credentials)],
            egress: hosts.into_iter().map(str::to_owned).collect(),
        }
    }
}
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    #[default]
    Proxy,
    Host,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SandboxConfig {
    pub path: PathBuf,
    pub network: Network,
    pub egress_ports: Vec<u16>,
    pub ro_binds: Vec<PathBuf>,
    pub rw_binds: Vec<PathBuf>,
    pub env: Vec<String>,
    #[serde(skip)]
    pub protected: Vec<PathBuf>,
}
impl Default for SandboxConfig {
    fn default() -> Self {
        let home = host_home();
        Self {
            path: "/usr/bin/bwrap".into(),
            network: Network::Proxy,
            egress_ports: vec![443],
            ro_binds: [
                ".cargo/bin",
                ".rustup/toolchains",
                ".local/share/mise/installs",
            ]
            .iter()
            .map(|p| home.join(p))
            .filter(|p| p.exists())
            .collect(),
            rw_binds: vec![],
            env: vec!["LANG".into(), "LC_ALL".into(), "TZ".into()],
            protected: vec![],
        }
    }
}
pub fn host_home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .expect("HOME is required")
}
pub fn forbidden_env(name: &str) -> bool {
    name.starts_with("AGENTD_")
        || matches!(
            name,
            "XDG_RUNTIME_DIR"
                | "DBUS_SESSION_BUS_ADDRESS"
                | "DBUS_SYSTEM_BUS_ADDRESS"
                | "SSH_AUTH_SOCK"
                | "GPG_AGENT_INFO"
                | "WAYLAND_DISPLAY"
                | "DISPLAY"
                | "BASH_ENV"
                | "ENV"
                | "LD_PRELOAD"
                | "LD_LIBRARY_PATH"
        )
}
pub fn host_pattern(host: &str) -> bool {
    let wildcard = host.starts_with("*.");
    let host = host.strip_prefix("*.").unwrap_or(host);
    (!wildcard || host.split('.').count() >= 2)
        && !host.is_empty()
        && host.parse::<std::net::IpAddr>().is_err()
        && host.split('.').all(|s| {
            !s.is_empty()
                && !s.starts_with('-')
                && !s.ends_with('-')
                && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}
pub fn expand(path: &Path) -> PathBuf {
    path.strip_prefix("~/")
        .map_or_else(|_| path.into(), |p| host_home().join(p))
}
// Resolve existing ancestors too, so a missing socket below a symlink is protected.
fn resolved(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!("unsafe sandbox path: {}", path.display()).into());
    }
    match std::fs::canonicalize(path) {
        Ok(p) => Ok(p),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(resolved(path.parent().ok_or("path has no parent")?)?
                .join(path.file_name().ok_or("path has no name")?))
        }
        Err(e) => Err(e.into()),
    }
}
impl SandboxConfig {
    pub fn check_bind(&self, path: &Path, state: &Path) -> Result<()> {
        let path = resolved(path)?;
        let state = resolved(state)?;
        let home = resolved(&host_home())?;
        if path.starts_with(&state)
            || state.starts_with(&path)
            || home.starts_with(&path)
            || Path::new("/run").starts_with(&path)
            || path.starts_with("/run/user")
            || self.protected.iter().any(|p| p.starts_with(&path))
        {
            return Err(format!("sandbox bind exposes protected paths: {}", path.display()).into());
        }
        Ok(())
    }
    pub fn validate(
        &mut self,
        state: &Path,
        control: &Path,
        shared: &Path,
        kinds: &mut [KindConfig],
    ) -> Result<()> {
        self.protected = [
            control,
            control.parent().ok_or("control socket needs parent")?,
            shared,
        ]
        .into_iter()
        .map(resolved)
        .collect::<Result<_>>()?;
        let socket = AgentPaths::new(state, AgentId(ulid::Ulid::new()))
            .io
            .join("egress.sock");
        use std::os::unix::ffi::OsStrExt;
        let limit = if cfg!(target_os = "linux") { 107 } else { 103 };
        if socket.as_os_str().as_bytes().len() > limit {
            return Err(format!(
                "per-agent socket path exceeds sun_path limit of {limit} bytes: {}",
                socket.display()
            )
            .into());
        }
        for path in self.ro_binds.iter_mut().chain(&mut self.rw_binds) {
            *path = expand(path);
        }
        for path in self.ro_binds.iter().chain(&self.rw_binds) {
            self.check_bind(path, state)?;
        }
        for name in &self.env {
            if forbidden_env(name) {
                return Err(format!("unsafe forwarded environment variable: {name}").into());
            }
        }
        for kind in kinds {
            for name in kind.env.keys() {
                if forbidden_env(name) {
                    return Err(format!("unsafe kind environment variable: {name}").into());
                }
            }
            if let Isolation::Bwrap(sb) = &mut kind.isolation {
                if sb.credentials.len() > 1 {
                    return Err("configure at most one credential file per kind".into());
                }
                for path in &sb.credentials {
                    if path.strip_prefix(host_home()).is_err() {
                        return Err("credentials must be files under HOME".into());
                    }
                    if path
                        .components()
                        .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
                    {
                        return Err("unsafe credential path".into());
                    }
                }
                for home in &sb.home {
                    if home.is_empty()
                        || Path::new(home)
                            .components()
                            .any(|c| !matches!(c, Component::Normal(_)))
                    {
                        return Err("home entries must be relative paths without traversal".into());
                    }
                }
                for host in &mut sb.egress {
                    if !host_pattern(host) {
                        return Err(
                            "egress entries must be hostnames or *.suffix with two suffix labels"
                                .into(),
                        );
                    }
                    host.make_ascii_lowercase();
                }
            }
        }
        Ok(())
    }
    pub fn label(&self, isolation: &Isolation) -> &'static str {
        match isolation {
            Isolation::None => "none",
            Isolation::Bwrap(_) if self.network == Network::Proxy => "bwrap+proxy",
            _ => "bwrap+host",
        }
    }
}
#[derive(Clone, Debug)]
pub struct AgentPaths {
    pub io: PathBuf,
    pub home: PathBuf,
}
impl AgentPaths {
    pub fn new(state: &Path, id: AgentId) -> Self {
        let base = state.join("agents").join(id.to_string());
        Self {
            io: base.join("io"),
            home: base.join("home"),
        }
    }
}
#[derive(Clone, Debug)]
pub enum Env {
    Clear(Vec<(OsString, OsString)>),
    Inherit(BTreeMap<String, String>),
}
#[derive(Clone, Debug)]
pub struct Launch {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub env: Env,
    pub mcp_server: Option<serde_json::Value>,
}
pub fn direct(kind: &KindConfig, spec: &AgentSpec) -> Launch {
    let mut args: Vec<OsString> = kind.args.iter().map(Into::into).collect();
    args.extend(sandbox_args(kind, spec).iter().map(OsString::from));
    Launch {
        program: kind.command.clone().into(),
        args,
        env: Env::Inherit(kind.env.clone()),
        mcp_server: None,
    }
}
fn sandbox_args<'a>(kind: &'a KindConfig, spec: &AgentSpec) -> &'a [String] {
    match spec.permissions.sandbox {
        SandboxLevel::ReadOnly => &kind.sandbox_args.read_only,
        SandboxLevel::WorkspaceWrite => &kind.sandbox_args.workspace_write,
        SandboxLevel::FullAccess => &kind.sandbox_args.full_access,
    }
}

// Filesystem facts are collected before building the pure launch plan.
pub struct Mount {
    source: PathBuf,
    target: PathBuf,
    readonly: bool,
}
pub struct Facts {
    mounts: Vec<Mount>,
    links: Vec<(PathBuf, PathBuf)>,
    home: PathBuf,
    exe: PathBuf,
    resolved_dns: bool,
    env: BTreeMap<String, String>,
}
impl Facts {
    pub fn collect() -> Result<Self> {
        let home = host_home();
        let mut facts = Self {
            mounts: vec![],
            links: vec![],
            home,
            exe: std::env::current_exe()?,
            resolved_dns: Path::new("/run/systemd/resolve").exists(),
            env: std::env::vars().collect(),
        };
        for path in ["/bin", "/sbin", "/lib", "/lib64"] {
            if let Ok(target) = std::fs::read_link(path) {
                facts.links.push((target, path.into()));
            } else if Path::new(path).exists() {
                facts.mounts.push(Mount {
                    source: path.into(),
                    target: path.into(),
                    readonly: true,
                });
            }
        }
        Ok(facts)
    }
}
pub fn seed(kind: &KindSandbox, home: &Path) -> Result<()> {
    let dir = crate::private_fs::Dir::open(home, true)?;
    for entry in &kind.home {
        if entry.ends_with('/') {
            dir.descend(Path::new(entry), true)?;
        } else {
            dir.seed(
                Path::new(entry),
                if entry == ".claude.json" {
                    b"{\"hasCompletedOnboarding\":true}"
                } else {
                    b""
                },
            )?;
        }
    }
    for (name, bytes) in [
        (".claude/settings.json", &b"{}"[..]),
        (".codex/config.toml", &b"sandbox_mode = \"read-only\"\n"[..]),
    ] {
        match dir.descend(Path::new(name).parent().ok_or("seed needs parent")?, false) {
            Ok(_) => dir.seed(Path::new(name), bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
pub fn plan(
    cfg: &SandboxConfig,
    kind: &KindConfig,
    spec: &AgentSpec,
    paths: &AgentPaths,
    facts: &Facts,
) -> Launch {
    let mut a: Vec<OsString> = Vec::new();
    let mut push = |parts: &[&std::ffi::OsStr]| a.extend(parts.iter().map(|p| p.to_os_string()));
    for flag in [
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--unshare-cgroup",
        "--die-with-parent",
        "--new-session",
    ] {
        push(&[flag.as_ref()]);
    }
    if cfg.network == Network::Proxy {
        push(&["--unshare-net".as_ref()]);
    }
    for path in ["/usr", "/etc"] {
        push(&["--ro-bind".as_ref(), path.as_ref(), path.as_ref()]);
    }
    for (target, path) in &facts.links {
        push(&["--symlink".as_ref(), target.as_os_str(), path.as_os_str()]);
    }
    for (flag, path) in [
        ("--proc", "/proc"),
        ("--dev", "/dev"),
        ("--tmpfs", "/tmp"),
        ("--tmpfs", "/run"),
    ] {
        push(&[flag.as_ref(), path.as_ref()]);
    }
    push(&[
        "--bind".as_ref(),
        paths.home.as_os_str(),
        facts.home.as_os_str(),
    ]);
    for (paths, flag) in [(&cfg.ro_binds, "--ro-bind"), (&cfg.rw_binds, "--bind")] {
        for path in paths {
            push(&[flag.as_ref(), path.as_os_str(), path.as_os_str()]);
        }
    }
    push(&[
        "--ro-bind".as_ref(),
        facts.exe.as_os_str(),
        "/run/agentd/bin/agentd".as_ref(),
    ]);
    push(&[
        "--bind".as_ref(),
        paths.io.as_os_str(),
        "/run/agentd/io".as_ref(),
    ]);
    for m in &facts.mounts {
        push(&[
            if m.readonly { "--ro-bind" } else { "--bind" }.as_ref(),
            m.source.as_os_str(),
            m.target.as_os_str(),
        ]);
    }
    push(&[
        "--bind".as_ref(),
        spec.cwd.as_os_str(),
        spec.cwd.as_os_str(),
    ]);
    push(&["--chdir".as_ref(), spec.cwd.as_os_str()]);
    if cfg.network == Network::Host && facts.resolved_dns {
        push(&[
            "--ro-bind".as_ref(),
            "/run/systemd/resolve".as_ref(),
            "/run/systemd/resolve".as_ref(),
        ]);
    }
    let mut env = BTreeMap::from([
        ("HOME".into(), facts.home.to_string_lossy().into_owned()),
        ("SHELL".into(), "/bin/bash".into()),
        ("TMPDIR".into(), "/tmp".into()),
        ("TERM".into(), "dumb".into()),
    ]);
    for name in ["USER", "LOGNAME", "PATH"]
        .into_iter()
        .chain(cfg.env.iter().map(String::as_str))
    {
        if let Some(value) = facts.env.get(name) {
            env.insert(name.into(), value.clone());
        }
    }
    env.extend(
        kind.env
            .iter()
            .filter(|(k, _)| !forbidden_env(k))
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    if cfg.network == Network::Proxy {
        for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
            env.insert(name.into(), "http://127.0.0.1:3128".into());
        }
        for name in ["NO_PROXY", "no_proxy"] {
            env.insert(name.into(), String::new());
        }
    }
    // Environment values, including optional provider tokens, travel in env, never argv.
    push(&[
        "--".as_ref(),
        "/run/agentd/bin/agentd".as_ref(),
        "sandbox-init".as_ref(),
    ]);
    if cfg.network == Network::Host {
        push(&["--host-network".as_ref()]);
    }
    push(&["--".as_ref()]);
    let direct = direct(kind, spec);
    a.push(direct.program);
    a.extend(direct.args);
    Launch {
        program: cfg.path.clone().into_os_string(),
        args: a,
        env: Env::Clear(env.into_iter().map(|(k, v)| (k.into(), v.into())).collect()),
        mcp_server: None,
    }
}

pub async fn preflight(cfg: &SandboxConfig, kinds: &[KindConfig], state: &Path) -> Result<()> {
    for kind in kinds {
        if let Isolation::Bwrap(sb) = &kind.isolation {
            let tmp = state.join("preflight");
            private_dir(&tmp)?;
            let paths = AgentPaths {
                io: tmp.join("io"),
                home: tmp.join("home"),
            };
            private_dir(&paths.io)?;
            seed(sb, &paths.home)?;
            let facts = Facts::collect()?;
            let spec = AgentSpec {
                name: AgentName::parse("preflight")?,
                kind: kind.name.clone(),
                cwd: paths.home.clone(),
                model: None,
                permissions: Default::default(),
                idle_after: None,
            };
            let mut launch = plan(cfg, kind, &spec, &paths, &facts);
            let pos = launch
                .args
                .iter()
                .position(|a| a == "sandbox-init")
                .expect("bug: sandbox-init absent");
            launch.args.truncate(pos - 1);
            launch.args.extend([
                OsString::from("/bin/sh"),
                OsString::from("-c"),
                OsString::from("command -v -- \"$1\""),
                OsString::from("preflight"),
                kind.command.clone().into(),
            ]);
            let mut cmd = tokio::process::Command::new(launch.program);
            cmd.args(launch.args);
            if let Env::Clear(env) = launch.env {
                cmd.env_clear().envs(env);
            }
            let out = cmd.output().await.map_err(|e| {
                format!(
                    "bwrap preflight for {} failed: {e}; install bwrap or set sandbox = \"none\"",
                    kind.name
                )
            })?;
            if !out.status.success() {
                return Err(format!("sandbox preflight failed for {} (command {}); check bwrap, user namespaces and ro_binds, or set sandbox = \"none\": {}",kind.name,kind.command,String::from_utf8_lossy(&out.stderr)).into());
            }
        }
    }
    Ok(())
}

async fn relay(listener: tokio::net::TcpListener) -> Result<()> {
    use tokio::net::UnixStream;
    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(64));
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (mut tcp, _) = accepted?;
                let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                connections.spawn(async move {
                    let _permit = permit;
                    let mut unix = UnixStream::connect("/run/agentd/io/egress.sock").await?;
                    tokio::io::copy_bidirectional(&mut tcp, &mut unix).await?;
                    Ok::<_,std::io::Error>(())
                });
            }
            done = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = done {
                    match result {
                        Ok(Ok(())) => {},
                        other => tracing::debug!(?other, "relay connection closed"),
                    }
                }
            }
        }
    }
}
pub async fn init(host_network: bool, cmd: Vec<OsString>) -> Result<()> {
    // Bind the relay before the child starts using HTTPS_PROXY.
    let mut tasks = tokio::task::JoinSet::new();
    if !host_network {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:3128").await?;
        tasks.spawn(relay(listener));
    }
    let (program, args) = cmd.split_first().ok_or("sandbox-init needs a command")?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut child = tokio::process::Command::new(program)
        .args(args)
        .kill_on_drop(true)
        .spawn()?;
    let status = tokio::select! {
        status = child.wait() => status?,
        done = tasks.join_next(), if !host_network => {
            return Err(format!("egress relay stopped: {done:?}").into());
        }
        _ = terminate.recv() => {
            if let Some(pid) = child.id() {
                nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32),nix::sys::signal::Signal::SIGTERM)?;
            }
            child.wait().await?
        }
    };
    tasks.shutdown().await;
    std::process::exit(status.code().unwrap_or(1));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn golden_launches_and_token_free_identity() {
        for kind in [
            KindConfig::claude("claude-agent-acp"),
            KindConfig::codex("codex-acp"),
        ] {
            for network in [Network::Proxy, Network::Host] {
                let cfg = SandboxConfig {
                    network: network.clone(),
                    ro_binds: vec!["/toolchain/bin".into()],
                    rw_binds: vec![],
                    ..Default::default()
                };
                let paths = AgentPaths {
                    io: "/state/agents/id/io".into(),
                    home: "/state/agents/id/home".into(),
                };
                let facts = Facts {
                    mounts: vec![Mount {
                        source: paths.home.join("private"),
                        target: "/home/person/private".into(),
                        readonly: false,
                    }],
                    links: vec![("usr/bin".into(), "/bin".into())],
                    home: "/home/person".into(),
                    exe: "/host/agentd".into(),
                    resolved_dns: false,
                    env: BTreeMap::from([
                        ("PATH".into(), "/usr/bin".into()),
                        ("AGENTD_MCP_TOKEN".into(), "do-not-forward".into()),
                        ("SSH_AUTH_SOCK".into(), "/host/ssh".into()),
                    ]),
                };
                let spec = AgentSpec {
                    name: AgentName::parse("test").expect("name"),
                    kind: kind.name.clone(),
                    cwd: "/workspace".into(),
                    model: None,
                    permissions: Default::default(),
                    idle_after: None,
                };
                let launch = plan(&cfg, &kind, &spec, &paths, &facts);
                let net = if network == Network::Proxy {
                    "--unshare-net\n"
                } else {
                    ""
                };
                let host = if network == Network::Host {
                    "--host-network\n"
                } else {
                    ""
                };
                let expected = format!(
                    "--unshare-user\n--unshare-pid\n--unshare-ipc\n--unshare-uts\n--unshare-cgroup\n--die-with-parent\n--new-session\n{net}--ro-bind\n/usr\n/usr\n--ro-bind\n/etc\n/etc\n--symlink\nusr/bin\n/bin\n--proc\n/proc\n--dev\n/dev\n--tmpfs\n/tmp\n--tmpfs\n/run\n--bind\n/state/agents/id/home\n/home/person\n--ro-bind\n/toolchain/bin\n/toolchain/bin\n--ro-bind\n/host/agentd\n/run/agentd/bin/agentd\n--bind\n/state/agents/id/io\n/run/agentd/io\n--bind\n/state/agents/id/home/private\n/home/person/private\n--bind\n/workspace\n/workspace\n--chdir\n/workspace\n--\n/run/agentd/bin/agentd\nsandbox-init\n{host}--\n{}",
                    kind.command
                );
                assert_eq!(
                    launch
                        .args
                        .iter()
                        .map(|s| s.to_str().expect("arg"))
                        .collect::<Vec<_>>()
                        .join("\n"),
                    expected
                );
                let Env::Clear(env) = launch.env else {
                    panic!("uncleared env")
                };
                assert!(
                    env.iter()
                        .all(|(k, v)| !k.to_string_lossy().contains("TOKEN")
                            && !v.to_string_lossy().contains("TOKEN")
                            && !forbidden_env(&k.to_string_lossy()))
                );
                let env: BTreeMap<_, _> = env.into_iter().collect();
                assert_eq!(
                    env.contains_key(std::ffi::OsStr::new("HTTPS_PROXY")),
                    network == Network::Proxy
                );
            }
        }
    }
    #[test]
    fn seeds_do_not_overwrite_private_state() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let home = tmp.path().join("home");
        let sb = KindSandbox::defaults("claude");
        seed(&sb, &home).expect("seed");
        assert_eq!(
            std::fs::read_to_string(home.join(".claude.json")).expect("onboarding"),
            "{\"hasCompletedOnboarding\":true}"
        );
        std::fs::write(home.join(".claude/settings.json"), "private state").expect("settings");
        seed(&sb, &home).expect("seed twice");
        assert_eq!(
            std::fs::read_to_string(home.join(".claude/settings.json")).expect("settings"),
            "private state"
        );
    }
    #[test]
    fn config_rejects_control_paths_and_environment() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state = tmp.path().join("state");
        std::fs::create_dir(&state).expect("state");
        for path in [
            state.clone(),
            tmp.path().into(),
            host_home(),
            "/run/user/1000".into(),
            "/x/../y".into(),
        ] {
            let mut cfg = SandboxConfig {
                ro_binds: vec![path],
                ..Default::default()
            };
            assert!(
                cfg.validate(
                    &state,
                    &state.join("agentd.sock"),
                    &state.join("mcp.sock"),
                    &mut []
                )
                .is_err()
            );
        }
        for name in [
            "AGENTD_MCP_TOKEN",
            "DBUS_SESSION_BUS_ADDRESS",
            "SSH_AUTH_SOCK",
            "DISPLAY",
        ] {
            let mut cfg = SandboxConfig {
                env: vec![name.into()],
                ..Default::default()
            };
            assert!(
                cfg.validate(
                    &state,
                    &state.join("agentd.sock"),
                    &state.join("mcp.sock"),
                    &mut []
                )
                .is_err()
            );
        }
        assert!(!host_pattern("127.0.0.1"));
        assert!(!host_pattern("*.com/evil"));
        assert!(host_pattern("*.example.com"));
    }
    #[test]
    fn seed_rejects_symlink_components_and_never_writes_through_leaf_links() {
        let root = tempfile::tempdir().expect("root");
        let home = root.path().join("home");
        let sb = KindSandbox::defaults("codex");
        seed(&sb, &home).expect("seed");
        let victim = root.path().join("victim");
        let config = home.join(".codex/config.toml");
        std::fs::remove_file(&config).expect("unlink");
        std::os::unix::fs::symlink(&victim, &config).expect("symlink");
        seed(&sb, &home).expect("exclusive seed treats existing entry as done");
        assert!(!victim.exists());
        std::fs::remove_dir_all(home.join(".codex")).expect("remove CLI dir");
        std::os::unix::fs::symlink(root.path(), home.join(".codex")).expect("parent symlink");
        assert!(seed(&sb, &home).is_err());
        assert!(!root.path().join("config.toml").exists());
    }
    #[test]
    fn custom_socket_ancestors_and_long_socket_paths_are_rejected() {
        let root = tempfile::tempdir().expect("root");
        let state = root.path().join("state");
        let work = root.path().join("work");
        std::fs::create_dir_all(&state).expect("state");
        std::fs::create_dir_all(&work).expect("work");
        let control = work.join("human.sock");
        let mut cfg = SandboxConfig {
            ro_binds: vec![],
            ..Default::default()
        };
        cfg.validate(&state, &control, &state.join("mcp.sock"), &mut [])
            .expect("validate");
        for path in [
            work,
            "/".into(),
            "/run".into(),
            host_home().parent().expect("home parent").into(),
            state.clone(),
        ] {
            assert!(cfg.check_bind(&path, &state).is_err(), "{}", path.display());
        }
        assert!(cfg.check_bind(&host_home().join("code/x"), &state).is_ok());
        let long = state.join("x".repeat(100));
        assert!(
            cfg.validate(&long, &control, &long.join("mcp.sock"), &mut [])
                .expect_err("sun_path")
                .to_string()
                .contains("sun_path")
        );
    }
    #[test]
    fn patterns_are_normalized_and_broad_wildcards_rejected() {
        let root = tempfile::tempdir().expect("root");
        let mut kinds = vec![KindConfig::claude("cli")];
        kinds[0].isolation = Isolation::Bwrap(KindSandbox {
            home: vec![],
            credentials: vec![],
            egress: vec!["API.Example.COM".into()],
        });
        let mut cfg = SandboxConfig {
            ro_binds: vec![],
            ..Default::default()
        };
        cfg.validate(
            root.path(),
            &root.path().join("a.sock"),
            &root.path().join("m.sock"),
            &mut kinds,
        )
        .expect("validate");
        let Isolation::Bwrap(sb) = &kinds[0].isolation else {
            panic!("bwrap");
        };
        assert_eq!(sb.egress, ["api.example.com"]);
        assert!(!host_pattern("*.com"));
        assert_eq!(
            SandboxMode::default(),
            if cfg!(target_os = "linux") {
                SandboxMode::Bwrap
            } else {
                SandboxMode::None
            }
        );
    }
    #[test]
    fn provider_tokens_never_enter_argv() {
        // The optional env-token route intentionally carries a provider token in env.
        let mut kind = KindConfig::claude("cli");
        kind.env
            .insert("CLAUDE_CODE_OAUTH_TOKEN".into(), "provider-secret".into());
        let facts = Facts {
            mounts: vec![],
            links: vec![],
            home: "/home/person".into(),
            exe: "/host/agentd".into(),
            resolved_dns: false,
            env: BTreeMap::new(),
        };
        let spec = AgentSpec {
            name: AgentName::parse("test").expect("name"),
            kind: "claude".into(),
            cwd: "/workspace".into(),
            model: None,
            permissions: Default::default(),
            idle_after: None,
        };
        let launch = plan(
            &SandboxConfig::default(),
            &kind,
            &spec,
            &AgentPaths {
                io: "/io".into(),
                home: "/home".into(),
            },
            &facts,
        );
        assert!(!format!("{:?}", launch.args).contains("provider-secret"));
        let Env::Clear(env) = launch.env else {
            panic!("clear")
        };
        assert!(env.contains(&("CLAUDE_CODE_OAUTH_TOKEN".into(), "provider-secret".into())));
    }
}
