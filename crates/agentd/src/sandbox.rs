//! Linux namespace launch plans. The adapter only sees a program and its environment.
use crate::seccomp;

use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use crate::{
    KindConfig,
    model::*,
    supervisor::AgentSpec,
    surfaces::{Result, private_dir},
};
use serde::{Deserialize, Serialize};
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

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
    #[serde(skip)]
    pub repin_binds: bool,
    #[serde(skip)]
    binds: Vec<(PinnedPath, PathBuf, bool)>,
    #[serde(skip)]
    bind_error: Option<String>,
    #[serde(skip)]
    pub credential_paths: BTreeMap<PathBuf, PathBuf>,
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
            repin_binds: false,
            binds: vec![],
            bind_error: None,
            credential_paths: BTreeMap::new(),
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
        || name.starts_with("LD_")
        || name.starts_with("MALLOC_")
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
                | "GCONV_PATH"
                | "GLIBC_TUNABLES"
                | "LOCPATH"
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
// Resolve future paths too, including dangling credential-parent links.
fn resolved(path: &Path) -> Result<PathBuf> {
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!("unsafe sandbox path: {}", path.display()).into());
    }
    resolve_path(path, |_| Ok(()))
}
fn resolve_path(path: &Path, mut symlink: impl FnMut(&Path) -> Result<()>) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(format!("unsafe sandbox path: {}", path.display()).into());
    }
    let mut pending: VecDeque<_> = path
        .components()
        .map(|c| c.as_os_str().to_owned())
        .collect();
    let mut result = PathBuf::new();
    let mut hops = 0;
    while let Some(part) = pending.pop_front() {
        match part.as_os_str() {
            p if p == "/" => result = PathBuf::from("/"),
            p if p == "." => {}
            p if p == ".." => {
                result.pop();
            }
            _ => {
                let next = result.join(&part);
                match std::fs::symlink_metadata(&next) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        hops += 1;
                        if hops > 40 {
                            return Err(format!("too many symlink hops: {}", path.display()).into());
                        }
                        symlink(&next)?;
                        let target = std::fs::read_link(&next)?;
                        for part in target.components().rev() {
                            pending.push_front(part.as_os_str().to_owned());
                        }
                    }
                    Ok(_) => {
                        // Do not follow a link installed between metadata and resolution.
                        if std::fs::canonicalize(&next)? != next {
                            return Err(format!(
                                "sandbox path changed while resolving: {}",
                                next.display()
                            )
                            .into());
                        }
                        result = next;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => result = next,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
    Ok(result)
}
impl SandboxConfig {
    pub fn spawn_cwd(&self, path: &Path, workspaces: &[&PinnedPath]) -> Result<PathBuf> {
        resolve_path(path, |link| {
            // Match by identity too: a bind-mount alias of a writable tree has another path prefix.
            let inside = |p: &PinnedPath| {
                link.starts_with(&p.path) || link.ancestors().skip(1).any(|a| p.identifies(a))
            };
            if workspaces.iter().any(|p| inside(p))
                || self
                    .binds
                    .iter()
                    .any(|(p, _, readonly)| !readonly && inside(p))
            {
                return Err(format!(
                    "cwd traverses agent-writable symlink {} -> {}; re-issue knowingly with the canonical path",
                    link.display(), resolved(link)?.display()
                ).into());
            }
            Ok(())
        })
    }
    pub fn check_bind(&self, path: &Path, state: &Path) -> Result<()> {
        if path.starts_with("/run") {
            return Err(format!("sandbox bind exposes runtime paths: {}", path.display()).into());
        }
        let path = resolved(path)?;
        let state = resolved(state)?;
        let home = resolved(&host_home())?;
        if path.starts_with(&state)
            || state.starts_with(&path)
            || home.starts_with(&path)
            || Path::new("/run").starts_with(&path)
            || path.starts_with("/run")
            || self
                .protected
                .iter()
                .any(|p| p.starts_with(&path) || resolved(p).is_ok_and(|p| p.starts_with(&path)))
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
        self.credential_paths.clear();
        self.protected = [
            control,
            control.parent().ok_or("control socket needs parent")?,
            shared,
        ]
        .into_iter()
        .map(resolved)
        .collect::<Result<_>>()?;
        for name in &self.env {
            if forbidden_env(name) {
                return Err(format!("unsafe forwarded environment variable: {name}").into());
            }
        }
        let uses_bwrap = kinds
            .iter()
            .any(|k| matches!(k.isolation, Isolation::Bwrap(_)));
        // A bare name would be resolved through PATH, which agents may be able to write.
        if uses_bwrap && !self.path.is_absolute() {
            return Err(format!("[bwrap] path must be absolute: {}", self.path.display()).into());
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
                    if path.strip_prefix(host_home()).is_err()
                        || path
                            .components()
                            .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
                    {
                        return Err(format!(
                            "kind {}: credentials must be files under HOME without traversal",
                            kind.name
                        )
                        .into());
                    }
                    let parent = path.parent().ok_or("credential needs parent")?;
                    self.protected.extend([parent.to_path_buf(), path.clone()]);
                    match resolved(parent) {
                        Ok(parent) => {
                            self.protected.push(
                                parent.join(path.file_name().ok_or("credential needs name")?),
                            );
                            self.protected.push(parent);
                        }
                        Err(error) => {
                            tracing::warn!(kind=%kind.name, %error, "credential path unavailable; checked again at launch")
                        }
                    }
                    match std::fs::canonicalize(parent) {
                        Ok(parent) => {
                            self.credential_paths.insert(
                                path.clone(),
                                parent.join(path.file_name().ok_or("credential needs name")?),
                            );
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            tracing::warn!(kind=%kind.name, %error, "credential parent unavailable; checked again at launch")
                        }
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
        self.binds.clear();
        self.bind_error = None;
        if uses_bwrap {
            self.validate_binds(state)?;
        }
        Ok(())
    }
    fn validate_binds(&mut self, state: &Path) -> Result<()> {
        let file = state.join("sandbox-binds.json");
        let previous_pins: BTreeMap<PathBuf, PinnedPath> = match std::fs::read(&file) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e.into()),
        };
        let mut current = BTreeMap::new();
        self.binds.clear();
        self.bind_error = None;
        for (paths, readonly) in [(&self.ro_binds, true), (&self.rw_binds, false)] {
            for path in paths {
                let path = expand(path);
                let previous = previous_pins.get(&path).filter(|_| !self.repin_binds);
                let captured = match previous {
                    Some(pin) if pin.is_legacy() => pin.upgrade(self, state, false),
                    Some(pin) => pin.open(self, state, false).map(|_| pin.clone()),
                    None => PinnedPath::capture_bind(&path, self, state),
                };
                let pin = match captured {
                    Ok(pin) => pin,
                    Err(error) => {
                        let error = self.bind_failure(&path, error);
                        self.bind_error = Some(error.clone());
                        tracing::error!(%error, "configured sandbox bind unavailable");
                        if let Some(pin) = previous_pins.get(&path) {
                            current.insert(path.clone(), pin.clone());
                        }
                        continue;
                    }
                };
                current.insert(path.clone(), pin.clone());
                self.binds.push((pin, path.clone(), readonly));
            }
        }
        let tmp = state.join("sandbox-binds.tmp");
        std::fs::write(&tmp, serde_json::to_vec(&current)?)?;
        std::fs::rename(tmp, file)?;
        Ok(())
    }
    fn bind_failure(&self, path: &Path, error: impl std::fmt::Display) -> String {
        format!(
            "sandbox-binds.json entry {}: {error}; validate configured sources and restart agentd with --repin-binds to authorize replacement",
            path.display()
        )
    }
    pub fn credential_host(&self, path: &Path) -> Result<PathBuf> {
        if let Some(host) = self.credential_paths.get(path) {
            return Ok(host.clone());
        }
        let parent = path.parent().ok_or("credential needs parent")?;
        let parent = resolved(parent).map_err(|e| format!("credential {}: {e}", path.display()))?;
        Ok(parent.join(path.file_name().ok_or("credential needs name")?))
    }
    pub fn launch_mounts(&self, workspace: &PinnedPath, state: &Path) -> Result<Vec<PinnedMount>> {
        if let Some(error) = &self.bind_error {
            return Err(error.clone().into());
        }
        self.binds
            .iter()
            .map(|(pin, target, readonly)| {
                Ok(PinnedMount {
                    fd: Arc::new(
                        pin.open(self, state, false)
                            .map_err(|e| self.bind_failure(target, e))?,
                    ),
                    target: target.clone(),
                    readonly: *readonly,
                })
            })
            .chain(std::iter::once_with(|| {
                Ok(PinnedMount {
                    fd: Arc::new(
                        workspace
                            .open(self, state, true)
                            .map_err(|e| workspace.failure(e))?,
                    ),
                    target: workspace.path.clone(),
                    readonly: false,
                })
            }))
            .collect()
    }
    pub fn label(&self, isolation: &Isolation) -> &'static str {
        match isolation {
            Isolation::None => "none",
            Isolation::Bwrap(_) if self.network == Network::Proxy => "bwrap+proxy",
            _ => "bwrap+host",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PinnedPath {
    pub path: PathBuf,
    #[serde(default)]
    identity: Option<FilesystemIdentity>,
    ino: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum FilesystemIdentity {
    Fsid([i32; 2]),
    Device(u64),
}
impl FilesystemIdentity {
    fn from_fsid(fsid: [i32; 2], dev: u64) -> Self {
        if fsid == [0, 0] {
            Self::Device(dev)
        } else {
            Self::Fsid(fsid)
        }
    }
    fn read(fd: &OwnedFd, dev: u64) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let fsid = nix::sys::statfs::fstatfs(fd)?.filesystem_id();
            // Linux fsid_t is two 32-bit words, with no padding or invalid bit patterns.
            let fsid = unsafe { std::mem::transmute::<nix::libc::fsid_t, [i32; 2]>(fsid) };
            Ok(Self::from_fsid(fsid, dev))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = fd;
            Ok(Self::from_fsid([0, 0], dev))
        }
    }
}
impl PinnedPath {
    fn resolved_fd(fd: &OwnedFd, path: &Path) -> Result<PathBuf> {
        #[cfg(target_os = "linux")]
        {
            let _ = path;
            Ok(std::fs::canonicalize(format!(
                "/proc/self/fd/{}",
                fd.as_raw_fd()
            ))?)
        }
        #[cfg(target_vendor = "apple")]
        {
            use std::os::unix::ffi::OsStrExt;
            let _ = path;
            let mut bytes = vec![0u8; nix::libc::PATH_MAX as usize];
            // nix's F_GETPATH wrapper requires UTF-8, but Unix paths need not be UTF-8.
            let result = unsafe {
                nix::libc::fcntl(fd.as_raw_fd(), nix::libc::F_GETPATH, bytes.as_mut_ptr())
            };
            nix::errno::Errno::result(result)?;
            let path = std::ffi::CStr::from_bytes_until_nul(&bytes)?;
            Ok(PathBuf::from(std::ffi::OsStr::from_bytes(path.to_bytes())))
        }
        #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
        {
            let _ = fd;
            Ok(std::fs::canonicalize(path)?)
        }
    }
    fn descriptor(path: &Path, directory: bool) -> Result<OwnedFd> {
        #[cfg(target_os = "linux")]
        let flags = nix::fcntl::OFlag::O_PATH;
        #[cfg(not(target_os = "linux"))]
        let flags = nix::fcntl::OFlag::O_RDONLY;
        let flags = if directory {
            flags | nix::fcntl::OFlag::O_DIRECTORY
        } else {
            flags
        };
        Ok(nix::fcntl::open(
            path,
            flags | nix::fcntl::OFlag::O_CLOEXEC,
            nix::sys::stat::Mode::empty(),
        )?)
    }
    pub fn capture(path: &Path, cfg: &SandboxConfig, state: &Path) -> Result<Self> {
        Self::capture_source(path, cfg, state, true)
    }
    fn capture_bind(path: &Path, cfg: &SandboxConfig, state: &Path) -> Result<Self> {
        Self::capture_source(path, cfg, state, false)
    }
    /// Whether `path` is this pinned object, reached by any path (including bind-mount aliases).
    fn identifies(&self, path: &Path) -> bool {
        use std::os::unix::fs::MetadataExt;
        let Some(identity) = &self.identity else {
            return false;
        };
        let Ok(fd) = Self::descriptor(path, true) else {
            return false;
        };
        let Ok(stat) = fd
            .try_clone()
            .map(std::fs::File::from)
            .and_then(|f| f.metadata())
        else {
            return false;
        };
        stat.ino() == self.ino
            && FilesystemIdentity::read(&fd, stat.dev()).is_ok_and(|i| &i == identity)
    }
    pub fn is_legacy(&self) -> bool {
        self.identity.is_none()
    }
    pub fn failure(&self, error: impl std::fmt::Display) -> String {
        format!(
            "{error}; recover with a human spawn of the failed agent name only if its cwd still resolves to {}; otherwise spawn under a new name with the canonical cwd",
            self.path.display()
        )
    }
    pub fn upgrade(&self, cfg: &SandboxConfig, state: &Path, directory: bool) -> Result<Self> {
        let captured = Self::capture_source(&self.path, cfg, state, directory)?;
        if captured.ino != self.ino || captured.path != self.path {
            return Err(format!("sandbox source identity changed: {}", self.path.display()).into());
        }
        Ok(captured)
    }
    fn capture_source(
        path: &Path,
        cfg: &SandboxConfig,
        state: &Path,
        directory: bool,
    ) -> Result<Self> {
        cfg.check_bind(path, state)?;
        let fd = Self::descriptor(path, directory)
            .map_err(|e| format!("sandbox source {}: {e}", path.display()))?;
        let path = Self::resolved_fd(&fd, path)?;
        cfg.check_bind(&path, state)?;
        use std::os::unix::fs::MetadataExt;
        let stat = std::fs::File::from(fd.try_clone()?).metadata()?;
        if !stat.is_file() && !stat.is_dir() {
            return Err(format!(
                "sandbox source {} must be a regular file or directory",
                path.display()
            )
            .into());
        }
        Ok(Self {
            path,
            identity: Some(FilesystemIdentity::read(&fd, stat.dev())?),
            ino: stat.ino(),
        })
    }
    fn open(&self, cfg: &SandboxConfig, state: &Path, directory: bool) -> Result<OwnedFd> {
        let fd = Self::descriptor(&self.path, directory)
            .map_err(|e| format!("sandbox source {}: {e}", self.path.display()))?;
        use std::os::unix::fs::MetadataExt;
        let stat = std::fs::File::from(fd.try_clone()?).metadata()?;
        if self.identity.as_ref() != Some(&FilesystemIdentity::read(&fd, stat.dev())?)
            || stat.ino() != self.ino
        {
            return Err(format!("sandbox source identity changed: {}", self.path.display()).into());
        }
        let resolved = Self::resolved_fd(&fd, &self.path)?;
        cfg.check_bind(&resolved, state)?;
        Ok(fd)
    }
}
#[derive(Clone, Debug)]
pub struct PinnedMount {
    fd: Arc<OwnedFd>,
    target: PathBuf,
    readonly: bool,
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
    pub mount_fds: Vec<Arc<OwnedFd>>,
}
impl Launch {
    pub fn inherit_mount_fds(&self, cmd: &mut tokio::process::Command) {
        let fds = self.mount_fds.clone();
        // Only the fork child clears CLOEXEC. bwrap consumes and closes these fds.
        unsafe {
            cmd.pre_exec(move || {
                for fd in &fds {
                    nix::fcntl::fcntl(
                        fd.as_ref(),
                        nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::empty()),
                    )?;
                }
                Ok(())
            });
        }
    }
}
pub fn direct(kind: &KindConfig, spec: &AgentSpec) -> Launch {
    let mut args: Vec<OsString> = kind.args.iter().map(Into::into).collect();
    args.extend(sandbox_args(kind, spec).iter().map(OsString::from));
    Launch {
        program: kind.command.clone().into(),
        args,
        env: Env::Inherit(kind.env.clone()),
        mcp_server: None,
        mount_fds: vec![],
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
    pub pinned: Vec<PinnedMount>,
    seccomp_fd: Arc<OwnedFd>,
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
            pinned: vec![],
            seccomp_fd: seccomp::filter_fd()?,
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
    pub fn validate_destinations(&self, paths: &AgentPaths) -> Result<()> {
        use crate::private_fs::Dir;
        let mut trees = vec![(self.home.clone(), Dir::open(&paths.home, false)?, false)];
        for (target, source, readonly) in self
            .mounts
            .iter()
            .map(|m| {
                (
                    &m.target,
                    PinnedPath::descriptor(&m.source, false),
                    m.readonly,
                )
            })
            .chain(
                self.pinned
                    .iter()
                    .map(|m| (&m.target, m.fd.try_clone().map_err(Into::into), m.readonly)),
            )
        {
            let source = source?;
            let directory = std::fs::File::from(source.try_clone()?)
                .metadata()?
                .is_dir();
            if let Some((root, dir, readonly)) = trees
                .iter()
                .rev()
                .find(|(root, _, _)| target.starts_with(root))
            {
                dir.mount_target(target.strip_prefix(root)?, directory, !readonly)
                    .map_err(|e| format!("sandbox mount destination {}: {e}", target.display()))?;
            }
            if directory {
                trees.push((target.clone(), Dir::from_fd(&source)?, readonly));
            }
        }
        Ok(())
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
    for m in &facts.mounts {
        push(&[
            if m.readonly { "--ro-bind" } else { "--bind" }.as_ref(),
            m.source.as_os_str(),
            m.target.as_os_str(),
        ]);
    }
    for mount in &facts.pinned {
        push(&[
            if mount.readonly {
                "--ro-bind-fd"
            } else {
                "--bind-fd"
            }
            .as_ref(),
            mount.fd.as_raw_fd().to_string().as_ref(),
            mount.target.as_os_str(),
        ]);
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
    push(&[
        "--add-seccomp-fd".as_ref(),
        facts.seccomp_fd.as_raw_fd().to_string().as_ref(),
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
        mount_fds: facts
            .pinned
            .iter()
            .map(|m| m.fd.clone())
            .chain(std::iter::once(facts.seccomp_fd.clone()))
            .collect(),
    }
}

pub async fn preflight(cfg: &SandboxConfig, kinds: &[KindConfig], state: &Path) -> Result<()> {
    if kinds
        .iter()
        .any(|k| matches!(k.isolation, Isolation::Bwrap(_)))
    {
        seccomp::check_arch()?;
        let out = tokio::process::Command::new(&cfg.path)
            .arg("--help")
            .output()
            .await
            .map_err(|e| {
                format!(
                    "bwrap preflight failed: {e}; install bwrap >= 0.10.0 or set sandbox = \"none\""
                )
            })?;
        let help = String::from_utf8_lossy(&out.stdout);
        if !out.status.success() || !help.contains("--bind-fd ") || !help.contains("--ro-bind-fd ")
        {
            return Err("bwrap preflight requires --bind-fd and --ro-bind-fd (bubblewrap >= 0.10.0); configure [bwrap] path = \"<bwrap >= 0.10.0>\" or sandbox = \"none\"".into());
        }
        if !help.contains("--add-seccomp-fd ") {
            return Err("bwrap preflight requires --add-seccomp-fd; sandbox launches require a seccomp filter".into());
        }
    }
    for kind in kinds {
        if let Isolation::Bwrap(sb) = &kind.isolation {
            if let Some(error) = &cfg.bind_error {
                tracing::error!(kind=%kind.name, %error, "sandbox launches disabled for configured bind");
                continue;
            }
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
                workspace: None,
                name: AgentName::parse("preflight")?,
                kind: kind.name.clone(),
                cwd: host_home(),
                model: None,
                permissions: Default::default(),
                idle_after: None,
            };
            let mut facts = facts;
            for (pin, target, readonly) in &cfg.binds {
                facts.pinned.push(PinnedMount {
                    fd: Arc::new(
                        pin.open(cfg, state, false)
                            .map_err(|e| cfg.bind_failure(target, e))?,
                    ),
                    target: target.clone(),
                    readonly: *readonly,
                });
            }
            facts.validate_destinations(&paths)?;
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
                OsString::from(
                    "command -v -- \"$1\" || { echo 'command not found on PATH inside the sandbox' >&2; exit 127; }",
                ),
                OsString::from("preflight"),
                kind.command.clone().into(),
            ]);
            let mut cmd = tokio::process::Command::new(&launch.program);
            cmd.args(&launch.args);
            launch.inherit_mount_fds(&mut cmd);
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
                return Err(preflight_failure(kind, out.status, &out.stderr).into());
            }
        }
    }
    Ok(())
}

fn preflight_failure(kind: &KindConfig, status: std::process::ExitStatus, stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let reason = match stderr.trim() {
        "" => format!("probe {status} with no stderr"),
        text => text.to_owned(),
    };
    format!(
        "sandbox preflight failed for {} (command {}); check bwrap, user namespaces and ro_binds, or set sandbox = \"none\": {reason}",
        kind.name, kind.command
    )
}

async fn relay(listener: tokio::net::TcpListener) -> Result<()> {
    use tokio::net::UnixStream;
    let slots = Arc::new(tokio::sync::Semaphore::new(64));
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
    fn sandbox_kinds() -> Vec<KindConfig> {
        let mut kind = KindConfig::claude("cli");
        kind.isolation = Isolation::Bwrap(KindSandbox {
            home: vec![],
            credentials: vec![],
            egress: vec![],
        });
        vec![kind]
    }
    #[test]
    fn preflight_failure_gives_exit_status_when_stderr_is_empty() {
        use std::os::unix::process::ExitStatusExt;
        let kind = &sandbox_kinds()[0];
        let error = preflight_failure(kind, std::process::ExitStatus::from_raw(9), b"");
        assert!(
            error.ends_with("set sandbox = \"none\": probe signal: 9 (SIGKILL) with no stderr"),
            "{error}"
        );
        let error = preflight_failure(
            kind,
            std::process::ExitStatus::from_raw(1 << 8),
            b"bwrap: No permissions to create new namespace\n",
        );
        assert!(
            error.ends_with(": bwrap: No permissions to create new namespace"),
            "{error}"
        );
    }
    #[test]
    fn fsid_identity_ignores_device_changes_and_falls_back_only_for_zero() {
        assert_eq!(
            FilesystemIdentity::from_fsid([12, 34], 1),
            FilesystemIdentity::from_fsid([12, 34], 2)
        );
        assert_ne!(
            FilesystemIdentity::from_fsid([12, 34], 1),
            FilesystemIdentity::from_fsid([12, 35], 1)
        );
        assert_eq!(
            FilesystemIdentity::from_fsid([0, 0], 7),
            FilesystemIdentity::Device(7)
        );
        assert_ne!(
            FilesystemIdentity::from_fsid([0, 0], 7),
            FilesystemIdentity::from_fsid([0, 0], 8)
        );
    }
    #[test]
    fn spawn_rejects_links_in_rw_binds_and_follows_operator_aliases() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonical tempdir");
        let tree = root.join("writable");
        let target = root.join("target");
        let state = root.join("state");
        for path in [&tree, &target, &state] {
            std::fs::create_dir(path).expect("directory");
        }
        let mut cfg = SandboxConfig::default();
        let pin = PinnedPath::capture(&tree, &cfg, &state).expect("bind pin");
        cfg.binds.push((pin, tree.clone(), false));
        let link = tree.join("planted");
        symlink(&target, &link).expect("planted link");
        let alias = root.join("operator-alias");
        symlink(&tree, &alias).expect("operator alias");
        for path in [link.clone(), alias.join("planted")] {
            let error = cfg
                .spawn_cwd(&path, &[])
                .expect_err("rw link refused")
                .to_string();
            assert!(error.contains(&link.display().to_string()), "{error}");
            assert!(error.contains(&target.display().to_string()), "{error}");
        }
        assert_eq!(cfg.spawn_cwd(&alias, &[]).expect("outside alias"), tree);
        assert!(
            cfg.binds[0].0.identifies(&tree),
            "pin identifies its own tree"
        );
        assert!(
            !cfg.binds[0].0.identifies(&target),
            "pin rejects another tree"
        );
        cfg.binds[0].2 = true;
        assert_eq!(cfg.spawn_cwd(&link, &[]).expect("read-only link"), target);
    }
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn descriptor_resolution_tracks_renames() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonical tempdir");
        let original = root.join("original");
        let moved = root.join("moved");
        std::fs::create_dir(&original).expect("original directory");
        let fd = PinnedPath::descriptor(&original, true).expect("directory descriptor");
        std::fs::rename(&original, &moved).expect("move original");
        std::fs::create_dir(&original).expect("replacement directory");
        assert_eq!(
            PinnedPath::resolved_fd(&fd, &original).expect("descriptor path"),
            moved
        );
    }
    #[test]
    fn future_path_resolution_limits_symlink_hops() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().expect("tempdir");
        let link = tmp.path().join("loop");
        symlink("loop", &link).expect("cycle");
        assert!(
            resolved(&link)
                .expect_err("bounded loop")
                .to_string()
                .contains("too many symlink hops")
        );
    }
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
                    pinned: vec![],
                    seccomp_fd: Arc::new(std::fs::File::open("/dev/null").expect("test fd").into()),
                    env: BTreeMap::from([
                        ("PATH".into(), "/usr/bin".into()),
                        ("AGENTD_MCP_TOKEN".into(), "do-not-forward".into()),
                        ("SSH_AUTH_SOCK".into(), "/host/ssh".into()),
                    ]),
                };
                let spec = AgentSpec {
                    workspace: None,
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
                    "--unshare-user\n--unshare-pid\n--unshare-ipc\n--unshare-uts\n--unshare-cgroup\n--die-with-parent\n--new-session\n{net}--ro-bind\n/usr\n/usr\n--ro-bind\n/etc\n/etc\n--symlink\nusr/bin\n/bin\n--proc\n/proc\n--dev\n/dev\n--tmpfs\n/tmp\n--tmpfs\n/run\n--bind\n/state/agents/id/home\n/home/person\n--bind\n/state/agents/id/home/private\n/home/person/private\n--ro-bind\n/host/agentd\n/run/agentd/bin/agentd\n--bind\n/state/agents/id/io\n/run/agentd/io\n--add-seccomp-fd\n{}\n--chdir\n/workspace\n--\n/run/agentd/bin/agentd\nsandbox-init\n{host}--\n{}",
                    facts.seccomp_fd.as_raw_fd(),
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
        let home = tmp
            .path()
            .canonicalize()
            .expect("canonical tempdir")
            .join("home");
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
                    &mut sandbox_kinds()
                )
                .is_ok()
            );
            assert!(cfg.bind_error.is_some());
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
                    &mut sandbox_kinds()
                )
                .is_err()
            );
        }
        assert!(!host_pattern("127.0.0.1"));
        assert!(!host_pattern("*.com/evil"));
        assert!(host_pattern("*.example.com"));
    }
    #[test]
    fn mount_destinations_reject_symlinks_in_home_and_prior_binds() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().expect("root");
        let root = tmp.path().canonicalize().expect("canonical root");
        let paths = AgentPaths {
            home: root.join("private"),
            io: root.join("io"),
        };
        std::fs::create_dir(&paths.home).expect("private home");
        let source = root.join("source");
        std::fs::create_dir(&source).expect("source");
        let file = root.join("file");
        std::fs::write(&file, "bind data").expect("file source");
        let mut facts = Facts {
            mounts: vec![],
            links: vec![],
            home: "/home/person".into(),
            exe: "/host/agentd".into(),
            resolved_dns: false,
            env: BTreeMap::new(),
            pinned: vec![],
            seccomp_fd: Arc::new(std::fs::File::open("/dev/null").expect("test fd").into()),
        };
        symlink("/run", paths.home.join("code")).expect("planted home parent");
        for (source, target) in [
            (&source, "/home/person/code/agentd"),
            (&file, "/home/person/code/file"),
        ] {
            facts.pinned = vec![PinnedMount {
                fd: Arc::new(PinnedPath::descriptor(source, false).expect("source fd")),
                target: target.into(),
                readonly: false,
            }];
            assert!(facts.validate_destinations(&paths).is_err());
        }
        std::fs::remove_file(paths.home.join("code")).expect("remove link");
        facts.pinned.clear();
        facts.pinned.push(PinnedMount {
            fd: Arc::new(PinnedPath::descriptor(&source, true).expect("directory fd")),
            target: "/shared".into(),
            readonly: false,
        });
        facts.pinned.push(PinnedMount {
            fd: Arc::new(PinnedPath::descriptor(&file, false).expect("file fd")),
            target: "/shared/nested/file".into(),
            readonly: true,
        });
        facts
            .validate_destinations(&paths)
            .expect("create safe destination parents");
        assert!(source.join("nested").is_dir());
        symlink("/run/agentd/bin/agentd", source.join("nested/file")).expect("planted file leaf");
        assert!(facts.validate_destinations(&paths).is_err());
        std::fs::remove_file(source.join("nested/file")).expect("remove leaf");
        facts.pinned[0].readonly = true;
        facts.pinned[1].target = "/shared/missing/file".into();
        assert!(facts.validate_destinations(&paths).is_err());
        assert!(
            !source.join("missing").exists(),
            "read-only grant must not create host paths"
        );
        facts.pinned[0].readonly = false;
        facts.pinned[1].target = "/shared/nested/file".into();
        std::fs::remove_dir(source.join("nested")).expect("remove parent");
        symlink("/run", source.join("nested")).expect("planted nested parent");
        assert!(facts.validate_destinations(&paths).is_err());
    }
    #[test]
    fn config_rejects_relative_bwrap_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state = tmp.path().join("state");
        std::fs::create_dir(&state).expect("state");
        let mut cfg = SandboxConfig {
            path: "bwrap".into(),
            ro_binds: vec![],
            ..Default::default()
        };
        let error = cfg
            .validate(
                &state,
                &state.join("agentd.sock"),
                &state.join("mcp.sock"),
                &mut sandbox_kinds(),
            )
            .expect_err("relative bwrap path must fail configuration")
            .to_string();
        assert!(error.contains("must be absolute"), "{error}");
    }
    #[test]
    fn config_rejects_loader_hooks_in_forwarded_and_kind_environment() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state = tmp.path().join("state");
        std::fs::create_dir(&state).expect("state");
        for name in [
            "LD_AUDIT",
            "LD_DEBUG",
            "GCONV_PATH",
            "MALLOC_TRACE",
            "GLIBC_TUNABLES",
            "LOCPATH",
        ] {
            for forwarded in [true, false] {
                let mut kinds = sandbox_kinds();
                let mut cfg = SandboxConfig {
                    ro_binds: vec![],
                    ..Default::default()
                };
                if forwarded {
                    cfg.env.push(name.into());
                } else {
                    kinds[0]
                        .env
                        .insert(name.into(), "/workspace/agent-library".into());
                }
                let result = cfg.validate(
                    &state,
                    &state.join("agentd.sock"),
                    &state.join("mcp.sock"),
                    &mut kinds,
                );
                let error = result
                    .expect_err("loader hook must fail configuration")
                    .to_string();
                assert!(error.contains(name), "{error}");
            }
        }
    }
    #[test]
    fn seed_rejects_symlink_components_and_never_writes_through_leaf_links() {
        let tmp = tempfile::tempdir().expect("root");
        let root = tmp.path().canonicalize().expect("canonical root");
        let home = root.join("home");
        let sb = KindSandbox::defaults("codex");
        seed(&sb, &home).expect("seed");
        let victim = root.join("victim");
        let config = home.join(".codex/config.toml");
        std::fs::remove_file(&config).expect("unlink");
        std::os::unix::fs::symlink(&victim, &config).expect("symlink");
        seed(&sb, &home).expect("exclusive seed treats existing entry as done");
        assert!(!victim.exists());
        std::fs::remove_dir_all(home.join(".codex")).expect("remove CLI dir");
        std::os::unix::fs::symlink(&root, home.join(".codex")).expect("parent symlink");
        assert!(seed(&sb, &home).is_err());
        assert!(!root.join("config.toml").exists());
    }
    #[test]
    fn custom_socket_ancestors_and_runtime_paths_are_rejected() {
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
            "/run/dbus".into(),
            host_home().parent().expect("home parent").into(),
            state.clone(),
        ] {
            assert!(cfg.check_bind(&path, &state).is_err(), "{}", path.display());
        }
        assert!(cfg.check_bind(&host_home().join("code/x"), &state).is_ok());
        let long = state.join("x".repeat(100));
        std::fs::create_dir_all(&long).expect("long state");
        cfg.validate(&long, &control, &long.join("mcp.sock"), &mut [])
            .expect("fd sockets permit long state paths");
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
    fn pinned_workspace_and_binds_reject_replacement_across_restart() {
        let root = tempfile::tempdir().expect("root");
        let state = root.path().join("state");
        let work = root.path().join("work");
        std::fs::create_dir(&state).expect("state");
        std::fs::create_dir(&work).expect("work");
        let mut cfg = SandboxConfig {
            ro_binds: vec![work.clone()],
            ..Default::default()
        };
        cfg.validate(
            &state,
            &state.join("a.sock"),
            &state.join("m.sock"),
            &mut sandbox_kinds(),
        )
        .expect("validate");
        let pin = PinnedPath::capture(&work, &cfg, &state).expect("workspace");
        let restored: PinnedPath =
            serde_json::from_slice(&serde_json::to_vec(&pin).expect("serialize"))
                .expect("persisted");
        let fd = restored.open(&cfg, &state, true).expect("open");
        assert!(
            nix::fcntl::FdFlag::from_bits_truncate(
                nix::fcntl::fcntl(&fd, nix::fcntl::FcntlArg::F_GETFD).expect("flags")
            )
            .contains(nix::fcntl::FdFlag::FD_CLOEXEC)
        );
        std::fs::rename(&work, root.path().join("old-work")).expect("rename");
        std::os::unix::fs::symlink(&state, &work).expect("swap");
        assert!(
            restored
                .open(&cfg, &state, true)
                .expect_err("identity mismatch")
                .to_string()
                .contains("identity changed")
        );
        let mut cfg = SandboxConfig {
            ro_binds: vec![work],
            ..Default::default()
        };
        cfg.validate(
            &state,
            &state.join("a.sock"),
            &state.join("m.sock"),
            &mut sandbox_kinds(),
        )
        .expect("daemon remains available");
        assert!(cfg.launch_mounts(&restored, &state).is_err());
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
            pinned: vec![],
            seccomp_fd: Arc::new(std::fs::File::open("/dev/null").expect("test fd").into()),
        };
        let spec = AgentSpec {
            workspace: None,
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
