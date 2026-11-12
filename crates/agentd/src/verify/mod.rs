//! The verifier runner: one sandboxed verifier per pending verification, from
//! another model family, in a private snapshot of the claimant's workspace.
//!
//! The snapshot code is plain functions over paths, so tests can call it on a
//! temporary directory. Selection is a pure function. The runner itself only
//! reads the mailroom through its API.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use nix::errno::Errno;
use nix::fcntl::{AtFlags, OFlag};
use nix::sys::stat::{Mode, SFlag};
use serde::{Deserialize, Serialize};

use crate::KindConfig;
use crate::mailroom::{Error as MailError, Mailroom, Verification, VerifierStart};
use crate::model::*;
use crate::swarm::Budget;

/// The directory under the state directory that holds every snapshot.
pub const VERIFY_DIR: &str = "verify";
/// The snapshot's working directory inside `<state>/verify/<report>`.
pub const WORKSPACE_DIR: &str = "workspace";
/// Written last, so a snapshot without it is a partial copy.
const COMPLETE: &str = "complete";

/// Names an agent CLI reads as instructions or configuration, matched at any
/// depth and without regard to case. The snapshot keeps them under a new name.
pub const INSTRUCTION_NAMES: &[&str] = &[
    "CLAUDE.md",
    "CLAUDE.local.md",
    "AGENTS.md",
    "GEMINI.md",
    ".mcp.json",
    ".claude",
    ".codex",
    ".cursor",
];
/// The suffix that neutralizes an instruction file in a snapshot.
pub const NEUTRALIZED_SUFFIX: &str = ".claimant-copy";

const DEFAULT_WALL_SECS: u64 = 900;
const DEFAULT_MAX_RUNNING: usize = 2;
const DEFAULT_MAX_SNAPSHOT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Failed starts of one pending row, in one daemon run, before it is settled.
const MAX_START_FAILURES: u32 = 3;
/// Restarts of one running row's verifier in one daemon run.
const MAX_RESTARTS: u32 = 3;
/// The pause after a pass that failed, before the next one.
const RETRY_PAUSE: Duration = Duration::from_secs(1);
/// A pass also runs this often with no wake.
const RECHECK: Duration = Duration::from_secs(30);

/// The `[verify]` table of `kinds.toml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyFile {
    /// Verifier kinds, in order of preference.
    #[serde(default)]
    pub kinds: Vec<String>,
    #[serde(default = "default_wall_secs")]
    pub wall_secs: u64,
    pub tokens: Option<u64>,
    #[serde(default = "default_max_running")]
    pub max_running: usize,
    #[serde(default = "default_max_snapshot_bytes")]
    pub max_snapshot_bytes: u64,
}
fn default_wall_secs() -> u64 {
    DEFAULT_WALL_SECS
}
fn default_max_running() -> usize {
    DEFAULT_MAX_RUNNING
}
fn default_max_snapshot_bytes() -> u64 {
    DEFAULT_MAX_SNAPSHOT_BYTES
}

/// Validated verifier settings. The daemon sets `verifier_available` only
/// when it has these.
#[derive(Clone, Debug)]
pub struct Settings {
    pub kinds: Vec<String>,
    /// The family of every configured kind, `None` for a kind without one.
    pub families: BTreeMap<String, Option<String>>,
    pub budget: Budget,
    pub max_running: usize,
    pub max_snapshot_bytes: u64,
}

impl Settings {
    /// `None` when there is no `[verify]` table or it lists no kinds. An
    /// error names the kind that cannot verify.
    pub fn from_file(
        file: Option<&VerifyFile>,
        kinds: &[KindConfig],
    ) -> Result<Option<Self>, String> {
        let Some(file) = file.filter(|f| !f.kinds.is_empty()) else {
            return Ok(None);
        };
        if file.wall_secs == 0 {
            return Err("[verify] wall_secs must be at least 1".into());
        }
        if file.tokens == Some(0) {
            return Err("[verify] tokens must be at least 1 or left out".into());
        }
        if file.max_running == 0 {
            return Err("[verify] max_running must be at least 1".into());
        }
        if file.max_snapshot_bytes == 0 {
            return Err("[verify] max_snapshot_bytes must be at least 1".into());
        }
        for name in &file.kinds {
            let Some(kind) = kinds.iter().find(|k| &k.name == name) else {
                return Err(format!(
                    "[verify] kinds lists {name}, which is not a configured kind"
                ));
            };
            if kind.family.is_none() {
                return Err(format!(
                    "[verify] kind {name} has no family; set family in [kind.{name}]"
                ));
            }
            if !matches!(kind.wire, crate::Wire::Acp) {
                return Err(format!(
                    "[verify] kind {name} is not an ACP kind; a verifier must be an ACP agent"
                ));
            }
            if matches!(kind.isolation, crate::sandbox::Isolation::None) {
                return Err(format!(
                    "[verify] kind {name} is unsandboxed; a verifier must run in bwrap"
                ));
            }
        }
        Ok(Some(Self {
            kinds: file.kinds.clone(),
            families: kinds
                .iter()
                .map(|k| (k.name.clone(), k.family.clone()))
                .collect(),
            budget: Budget {
                tokens: file.tokens,
                wall_secs: file.wall_secs,
                messages: 1,
                hops: 0,
            },
            max_running: file.max_running,
            max_snapshot_bytes: file.max_snapshot_bytes,
        }))
    }
}

/// Which verifier checks a claim, or why none can.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    Verifier {
        kind: String,
        verifier_family: String,
        claimant_family: String,
    },
    Unverifiable(String),
}

/// Picks the first preferred kind whose family differs from the claimant's.
/// `claimant_kind` is `None` when the claimant has no saved spec.
pub fn select(
    claimant_kind: Option<&str>,
    families: &BTreeMap<String, Option<String>>,
    preference: &[String],
) -> Selection {
    let Some(claimant_kind) = claimant_kind else {
        return Selection::Unverifiable(
            "the claimant's kind is unknown: it has no saved spec".into(),
        );
    };
    let claimant_family = match families.get(claimant_kind) {
        None => {
            return Selection::Unverifiable(format!(
                "the claimant's kind {claimant_kind} is not configured, so its family is unknown"
            ));
        }
        Some(None) => {
            return Selection::Unverifiable(format!(
                "the claimant's kind {claimant_kind} has no family"
            ));
        }
        Some(Some(family)) => family,
    };
    for kind in preference {
        if let Some(Some(family)) = families.get(kind)
            && family != claimant_family
        {
            return Selection::Verifier {
                kind: kind.clone(),
                verifier_family: family.clone(),
                claimant_family: claimant_family.clone(),
            };
        }
    }
    Selection::Unverifiable(format!(
        "no verifier kind has a family other than the claimant's family {claimant_family}"
    ))
}

/// The deterministic verifier name for a report. It fits `AgentName`.
pub fn verifier_name(report: MessageId) -> AgentName {
    AgentName::parse(&format!("v-{}", report.to_string().to_ascii_lowercase()))
        .expect("bug: a lowercase ULID with a prefix is a valid agent name")
}

/// The verifier's permissions. Its bwrap sandbox makes only its snapshot,
/// its private home and a private `/tmp` writable, and lets it reach only
/// its kind's egress hosts. Inside that boundary it may run commands and edit
/// files without asking. Its own report needs no approval. Any other tool is
/// refused, so a verifier never waits for the operator.
pub fn verifier_policy() -> PermissionPolicy {
    let rule = |tool: &str, decision| PermissionRule {
        tool: tool.into(),
        decision,
    };
    PermissionPolicy {
        sandbox: SandboxLevel::WorkspaceWrite,
        rules: vec![
            rule("shell", RuleDecision::Allow),
            rule("edit", RuleDecision::Allow),
            rule("send_message", RuleDecision::Allow),
            rule("*", RuleDecision::Deny),
        ],
        timeout: None,
    }
}

// ---------------------------------------------------------------- snapshot

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("the claimant's workspace is larger than max_snapshot_bytes ({limit} bytes)")]
    TooLarge { limit: u64 },
    #[error("snapshot of {}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}
fn io_at(path: &Path) -> impl FnOnce(std::io::Error) -> SnapshotError + '_ {
    move |source| SnapshotError::Io {
        path: path.to_owned(),
        source,
    }
}
fn errno_at(path: &Path) -> impl FnOnce(Errno) -> SnapshotError + '_ {
    move |e| SnapshotError::Io {
        path: path.to_owned(),
        source: e.into(),
    }
}

#[derive(Serialize, Deserialize)]
struct Marker {
    clean: bool,
}

/// `<state>/verify/<report>`.
pub fn snapshot_dir(state: &Path, report: MessageId) -> PathBuf {
    state.join(VERIFY_DIR).join(report.to_string())
}

/// Whether a complete snapshot exists in `dir`, and if so whether it was clean.
pub fn read_marker(dir: &Path) -> std::io::Result<Option<bool>> {
    match std::fs::read(dir.join(COMPLETE)) {
        Ok(bytes) => serde_json::from_slice::<Marker>(&bytes)
            .map(|m| Some(m.clean))
            .map_err(std::io::Error::other),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Deletes a snapshot directory. A missing one is not an error.
pub fn remove_snapshot(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Copies `source` to `<dir>/workspace` and marks it complete. Returns
/// whether the snapshot counts as clean. A complete snapshot that already
/// exists is reused with its recorded cleanliness. A partial one, left by a
/// crash, is deleted and copied again as not clean. On any error the partial
/// copy is deleted.
///
/// The copy runs no program and follows no symlink. It copies a symlink as a
/// symlink, skips sockets, FIFOs and devices, drops every `.git` entry, and
/// renames each name in `INSTRUCTION_NAMES` with `NEUTRALIZED_SUFFIX`. It
/// stops at `max_bytes` of file content.
pub fn take_snapshot(
    source: &Path,
    dir: &Path,
    max_bytes: u64,
    clean: bool,
) -> Result<bool, SnapshotError> {
    if let Some(clean) = read_marker(dir).map_err(io_at(dir))? {
        return Ok(clean);
    }
    let partial = match std::fs::symlink_metadata(dir) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(io_at(dir)(e)),
    };
    if partial {
        remove_snapshot(dir).map_err(io_at(dir))?;
    }
    let clean = clean && !partial;
    let result = (|| {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(io_at(dir))?;
        let workspace = dir.join(WORKSPACE_DIR);
        copy_tree(source, &workspace, max_bytes)?;
        write_marker(dir, clean)
    })();
    if let Err(error) = result {
        if let Err(cleanup) = remove_snapshot(dir) {
            tracing::error!(dir=%dir.display(), %cleanup, "could not delete a partial snapshot");
        }
        return Err(error);
    }
    Ok(clean)
}

fn write_marker(dir: &Path, clean: bool) -> Result<(), SnapshotError> {
    let tmp = dir.join(format!("{COMPLETE}.tmp"));
    let bytes = serde_json::to_vec(&Marker { clean }).expect("bug: a marker serializes");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(io_at(&tmp))?;
    std::io::Write::write_all(&mut file, &bytes).map_err(io_at(&tmp))?;
    file.sync_all().map_err(io_at(&tmp))?;
    std::fs::rename(&tmp, dir.join(COMPLETE)).map_err(io_at(dir))?;
    sync_dir(dir)
}

fn sync_dir(path: &Path) -> Result<(), SnapshotError> {
    std::fs::File::open(path)
        .and_then(|d| d.sync_all())
        .map_err(io_at(path))
}

fn is_named(name: &OsStr, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|c| name.as_bytes().eq_ignore_ascii_case(c.as_bytes()))
}

/// A race with the claimant removed or replaced the entry. Skip it.
fn raced(e: Errno) -> bool {
    matches!(e, Errno::ENOENT | Errno::ELOOP | Errno::ENOTDIR)
}

fn open_dir_at(parent: &OwnedFd, name: &OsStr) -> nix::Result<OwnedFd> {
    nix::fcntl::openat(
        parent,
        name,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
}

fn copy_tree(source: &Path, dest: &Path, max_bytes: u64) -> Result<(), SnapshotError> {
    let root = nix::fcntl::open(
        source,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(errno_at(source))?;
    let mut left = max_bytes;
    // A stack, not recursion, so a deep tree cannot overflow it. A directory's
    // descriptor stays open while entries under it wait.
    let mut stack = vec![(root, dest.to_owned(), source.to_owned())];
    while let Some((fd, dest, shown)) = stack.pop() {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dest)
            .map_err(io_at(&dest))?;
        let entries = list_dir(&fd).map_err(errno_at(&shown))?;
        for (from, to) in planned_names(entries) {
            let at = shown.join(&from);
            let target = dest.join(&to);
            let stat = match nix::sys::stat::fstatat(
                &fd,
                from.as_os_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            ) {
                Ok(stat) => stat,
                Err(e) if raced(e) => continue,
                Err(e) => return Err(errno_at(&at)(e)),
            };
            match SFlag::from_bits_truncate(stat.st_mode) & SFlag::S_IFMT {
                SFlag::S_IFDIR => match open_dir_at(&fd, &from) {
                    Ok(child) => stack.push((child, target, at)),
                    Err(e) if raced(e) => {}
                    Err(e) => return Err(errno_at(&at)(e)),
                },
                SFlag::S_IFREG => copy_file(&fd, &from, &at, &target, &mut left, max_bytes)?,
                SFlag::S_IFLNK => match nix::fcntl::readlinkat(&fd, from.as_os_str()) {
                    Ok(link) => {
                        std::os::unix::fs::symlink(link, &target).map_err(io_at(&target))?
                    }
                    Err(e) if raced(e) || e == Errno::EINVAL => {}
                    Err(e) => return Err(errno_at(&at)(e)),
                },
                // Sockets, FIFOs and devices.
                _ => {}
            }
        }
        sync_dir(&dest)?;
    }
    Ok(())
}

fn list_dir(fd: &OwnedFd) -> nix::Result<Vec<OsString>> {
    let mut dir = nix::dir::Dir::from_fd(
        fd.try_clone()
            .map_err(|e| e.raw_os_error().map_or(Errno::EIO, Errno::from_raw))?,
    )?;
    let mut names = Vec::new();
    for entry in dir.iter() {
        let name = entry?.file_name().to_bytes().to_vec();
        if name != b"." && name != b".." {
            names.push(OsString::from(OsStr::from_bytes(&name)));
        }
    }
    Ok(names)
}

/// Pairs each kept source name with its name in the snapshot. `.git` is
/// dropped. An instruction name gets the suffix, repeated until it collides
/// with no other entry of the directory.
fn planned_names(names: Vec<OsString>) -> Vec<(OsString, OsString)> {
    let kept = names
        .into_iter()
        .filter(|n| !is_named(n, &[".git"]))
        .collect::<Vec<_>>();
    let mut taken = kept
        .iter()
        .filter(|n| !is_named(n, INSTRUCTION_NAMES))
        .map(|n| n.as_bytes().to_ascii_lowercase())
        .collect::<HashSet<_>>();
    kept.into_iter()
        .map(|from| {
            if !is_named(&from, INSTRUCTION_NAMES) {
                return (from.clone(), from);
            }
            let mut to = from.clone();
            loop {
                to.push(NEUTRALIZED_SUFFIX);
                if taken.insert(to.as_bytes().to_ascii_lowercase()) {
                    break;
                }
            }
            (from, to)
        })
        .collect()
}

fn copy_file(
    parent: &OwnedFd,
    name: &OsStr,
    shown: &Path,
    target: &Path,
    left: &mut u64,
    max_bytes: u64,
) -> Result<(), SnapshotError> {
    // O_NONBLOCK: a FIFO swapped in after the stat must not block the open.
    let fd = match nix::fcntl::openat(
        parent,
        name,
        OFlag::O_RDONLY
            | OFlag::O_NOFOLLOW
            | OFlag::O_NONBLOCK
            | OFlag::O_NOCTTY
            | OFlag::O_CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(e) if raced(e) => return Ok(()),
        Err(e) => return Err(errno_at(shown)(e)),
    };
    let file = std::fs::File::from(fd);
    let meta = file.metadata().map_err(io_at(shown))?;
    if !meta.is_file() {
        return Ok(());
    }
    use std::os::unix::fs::PermissionsExt;
    let mode = 0o600 | (meta.permissions().mode() & 0o100);
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(target)
        .map_err(io_at(target))?;
    let copied = std::io::copy(&mut file.take(*left + 1), &mut out).map_err(io_at(shown))?;
    if copied > *left {
        return Err(SnapshotError::TooLarge { limit: max_bytes });
    }
    *left -= copied;
    out.sync_all().map_err(io_at(target))
}

// ------------------------------------------------------------------ runner

/// What the broker shares with the runner. The broker owns one.
pub struct Runner {
    pub(crate) settings: Settings,
    pub(crate) state: PathBuf,
    /// The accept path holds a read guard from before it accepts a claim
    /// until it has marked the report in flight. A pass holds the write guard
    /// while it lists pending rows, so it never sees a row whose snapshot the
    /// accept path is about to take.
    pub(crate) gate: tokio::sync::RwLock<()>,
    /// Reports whose snapshot someone is taking.
    pub(crate) inflight: std::sync::Mutex<HashSet<MessageId>>,
    pub(crate) wake: tokio::sync::Notify,
}

impl Runner {
    /// Creates the private `verify` root under `state`.
    pub fn new(settings: Settings, state: &Path) -> crate::surfaces::Result<Self> {
        crate::surfaces::private_dir(&state.join(VERIFY_DIR))?;
        Ok(Self {
            settings,
            state: state.to_owned(),
            gate: tokio::sync::RwLock::new(()),
            inflight: Default::default(),
            wake: tokio::sync::Notify::new(),
        })
    }
    pub fn settings(&self) -> &Settings {
        &self.settings
    }
    pub fn dir(&self, report: MessageId) -> PathBuf {
        snapshot_dir(&self.state, report)
    }

    fn claim(&self, report: MessageId) -> bool {
        self.inflight
            .lock()
            .expect("bug: inflight set poisoned")
            .insert(report)
    }
    fn release(&self, report: MessageId) {
        self.inflight
            .lock()
            .expect("bug: inflight set poisoned")
            .remove(&report);
    }
    fn is_inflight(&self, report: MessageId) -> bool {
        self.inflight
            .lock()
            .expect("bug: inflight set poisoned")
            .contains(&report)
    }

    /// The claimant's kind and working directory, read through the mailroom.
    async fn claimant(
        &self,
        mailroom: &Mailroom,
        v: &Verification,
    ) -> crate::mailroom::Result<Option<crate::supervisor::AgentSpec>> {
        let owner = mailroom.task(v.claimant_task).await?.owner;
        mailroom.agent_spec(owner).await
    }

    async fn copy(
        &self,
        source: PathBuf,
        report: MessageId,
        clean: bool,
    ) -> Result<bool, SnapshotError> {
        let dir = self.dir(report);
        let max = self.settings.max_snapshot_bytes;
        tokio::task::spawn_blocking(move || take_snapshot(&source, &dir, max, clean))
            .await
            .map_err(|e| SnapshotError::Io {
                path: PathBuf::new(),
                source: std::io::Error::other(e),
            })?
    }

    /// Called by the path that accepted `report`, before the claimant's send
    /// returns. The caller marked `report` in flight. Takes a clean snapshot
    /// when a verifier can take the claim. Settles a claim that is too large
    /// to copy. Any other failure is left to the reconcile loop.
    pub(crate) async fn snapshot_accepted(&self, mailroom: &Mailroom, report: MessageId) {
        if let Err(error) = self.snapshot_accepted_inner(mailroom, report).await {
            tracing::warn!(%report, %error, "snapshot at report time failed; the runner retries it as not clean");
        }
        self.release(report);
        self.wake.notify_one();
    }
    async fn snapshot_accepted_inner(
        &self,
        mailroom: &Mailroom,
        report: MessageId,
    ) -> Result<(), String> {
        let v = mailroom
            .verification(report)
            .await
            .map_err(|e| e.to_string())?;
        if v.state != crate::mailroom::VerificationState::Pending {
            tracing::debug!(%report, state=?v.state, "no snapshot at report time: the claim is not pending");
            return Ok(());
        }
        let claimant = self
            .claimant(mailroom, &v)
            .await
            .map_err(|e| e.to_string())?;
        let selection = select(
            claimant.as_ref().map(|s| s.kind.as_str()),
            &self.settings.families,
            &self.settings.kinds,
        );
        let (Some(claimant), Selection::Verifier { .. }) = (claimant, selection) else {
            tracing::debug!(%report, "no snapshot at report time: no verifier can take the claim");
            return Ok(());
        };
        match self.copy(claimant.cwd, report, true).await {
            Ok(_) => Ok(()),
            Err(e @ SnapshotError::TooLarge { .. }) => mailroom
                .unverifiable(report, e.to_string())
                .await
                .map(drop)
                .map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// What the broker gives the reconcile loop.
#[async_trait::async_trait]
pub(crate) trait Host: Send + Sync {
    fn mailroom(&self) -> &Mailroom;
    /// Whether the verifier has no live actor, or one that gave up.
    async fn verifier_down(&self, id: AgentId) -> bool;
    /// Starts a verifier from its saved spec.
    async fn start_verifier(&self, spec: crate::supervisor::AgentSpec) -> Result<(), String>;
    /// Starts a down verifier again from its saved spec.
    async fn restart_verifier(&self, id: AgentId) -> Result<(), String>;
    async fn retire_verifier(&self, id: AgentId) -> Result<(), String>;
    /// Pins the snapshot as the workspace of a sandboxed verifier kind.
    fn pin_snapshot(
        &self,
        kind: &str,
        workspace: &Path,
    ) -> Result<Option<crate::sandbox::PinnedPath>, String>;
}

/// State that lives for one daemon run.
#[derive(Default)]
pub(crate) struct Pass {
    failures: HashMap<MessageId, u32>,
    restarts: HashMap<MessageId, u32>,
}

enum Started {
    Running,
    Settled,
}

/// Runs passes until the mailroom closes: at startup, on a task change, on
/// the runner's wake, and every `RECHECK`. After a pass with an error it
/// pauses `RETRY_PAUSE` first, so it never spins.
pub(crate) async fn run(runner: &Runner, host: &dyn Host) {
    let mut changes = host.mailroom().task_changes();
    let mut pass = Pass::default();
    loop {
        changes.borrow_and_update();
        let failed = match reconcile(runner, host, &mut pass).await {
            Ok(failed) => failed,
            Err(MailError::Closed) => return,
            Err(error) => {
                tracing::error!(%error, "verifier pass failed");
                true
            }
        };
        if failed {
            // The pass's own failed writes announce a task change. Waiting
            // on that channel here would retry at once.
            tokio::time::sleep(RETRY_PAUSE).await;
            continue;
        }
        tokio::select! {
            () = tokio::time::sleep(RECHECK) => {}
            () = runner.wake.notified() => {}
            changed = changes.changed() => if changed.is_err() { return; },
        }
    }
}

/// One pass. Returns whether any step failed. Every step can repeat after a
/// crash, and a failure on one row does not stop the others.
pub(crate) async fn reconcile(
    runner: &Runner,
    host: &dyn Host,
    pass: &mut Pass,
) -> crate::mailroom::Result<bool> {
    let mailroom = host.mailroom();
    let mut failed = false;

    // Settled rows: retire the verifier, then delete its snapshot.
    for id in mailroom.settled_verifiers().await? {
        if let Err(error) = host.retire_verifier(id).await {
            tracing::error!(verifier=%id, %error, "could not retire a settled verifier; retrying next pass");
            failed = true;
        }
    }
    failed |= sweep(runner, mailroom).await?;

    // Running rows whose verifier is down.
    let running = mailroom.running_verifications().await?;
    for v in &running {
        let Some(verifier) = v.verifier else { continue };
        if !host.verifier_down(verifier).await {
            continue;
        }
        let tries = pass.restarts.entry(v.report).or_default();
        if *tries >= MAX_RESTARTS {
            continue;
        }
        *tries += 1;
        if let Err(error) = host.restart_verifier(verifier).await {
            // Its task expires and settles the claim as inconclusive.
            tracing::error!(report=%v.report, %verifier, %error, "could not restart a verifier");
        }
    }

    // Pending rows, up to `max_running`.
    let mut slots = runner.settings.max_running.saturating_sub(running.len());
    if slots == 0 {
        return Ok(failed);
    }
    let pending = {
        let _gate = runner.gate.write().await;
        mailroom.pending_verifications().await?
    };
    for v in pending {
        if slots == 0 {
            break;
        }
        if !runner.claim(v.report) {
            // The accept path is taking its snapshot and wakes us after.
            continue;
        }
        let started = start(runner, host, &v).await;
        runner.release(v.report);
        match started {
            Ok(Started::Running) => {
                slots -= 1;
                pass.failures.remove(&v.report);
            }
            Ok(Started::Settled) => {
                pass.failures.remove(&v.report);
            }
            Err(MailError::Closed) => return Err(MailError::Closed),
            Err(error) => {
                let n = pass.failures.entry(v.report).or_default();
                *n += 1;
                tracing::warn!(report=%v.report, attempt=*n, %error, "verifier start failed");
                if *n < MAX_START_FAILURES {
                    failed = true;
                    continue;
                }
                pass.failures.remove(&v.report);
                match mailroom
                    .unverifiable(
                        v.report,
                        format!("the verifier did not start after {MAX_START_FAILURES} attempts: {error}"),
                    )
                    .await
                {
                    Ok(_) => {
                        if let Err(error) = remove_snapshot(&runner.dir(v.report)) {
                            tracing::error!(report=%v.report, %error, "could not delete a snapshot; retrying next pass");
                            failed = true;
                        }
                    }
                    Err(error) => {
                        tracing::error!(report=%v.report, %error, "could not settle a claim whose verifier did not start");
                        failed = true;
                    }
                }
            }
        }
    }
    Ok(failed)
}

/// Deletes each snapshot whose row is settled and whose verifier is retired
/// or never existed, and each snapshot with no row.
async fn sweep(runner: &Runner, mailroom: &Mailroom) -> crate::mailroom::Result<bool> {
    let root = runner.state.join(VERIFY_DIR);
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::error!(root=%root.display(), %error, "could not list snapshots");
            return Ok(true);
        }
    };
    let mut failed = false;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::error!(%error, "could not list snapshots");
                failed = true;
                continue;
            }
        };
        let Some(report) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<ulid::Ulid>().ok())
            .map(MessageId)
        else {
            continue;
        };
        if runner.is_inflight(report) {
            continue;
        }
        let delete = match mailroom.verification(report).await {
            Ok(v) if v.state == crate::mailroom::VerificationState::Settled => match v.verifier {
                None => true,
                Some(verifier) => mailroom.retired(verifier).await?,
            },
            Ok(_) => false,
            Err(MailError::NotFound(_)) => true,
            Err(error) => return Err(error),
        };
        if delete && let Err(error) = remove_snapshot(&entry.path()) {
            tracing::error!(%report, %error, "could not delete a snapshot; retrying next pass");
            failed = true;
        }
    }
    Ok(failed)
}

async fn start(
    runner: &Runner,
    host: &dyn Host,
    v: &Verification,
) -> crate::mailroom::Result<Started> {
    let mailroom = host.mailroom();
    let claimant = runner.claimant(mailroom, v).await?;
    let (kind, verifier_family, claimant_family) = match select(
        claimant.as_ref().map(|s| s.kind.as_str()),
        &runner.settings.families,
        &runner.settings.kinds,
    ) {
        Selection::Unverifiable(reason) => {
            mailroom.unverifiable(v.report, reason).await?;
            if let Err(error) = remove_snapshot(&runner.dir(v.report)) {
                tracing::error!(report=%v.report, %error, "could not delete a snapshot; the next pass retries");
            }
            return Ok(Started::Settled);
        }
        Selection::Verifier {
            kind,
            verifier_family,
            claimant_family,
        } => (kind, verifier_family, claimant_family),
    };
    let claimant = claimant.expect("bug: selection needs the claimant's spec");
    let snapshot_clean = match runner.copy(claimant.cwd, v.report, false).await {
        Ok(clean) => clean,
        Err(e @ SnapshotError::TooLarge { .. }) => {
            mailroom.unverifiable(v.report, e.to_string()).await?;
            return Ok(Started::Settled);
        }
        Err(e) => return Err(MailError::Rejected(e.to_string())),
    };
    let workspace = runner.dir(v.report).join(WORKSPACE_DIR);
    let pin = host
        .pin_snapshot(&kind, &workspace)
        .map_err(MailError::Rejected)?;
    let spec = crate::supervisor::AgentSpec {
        name: verifier_name(v.report),
        kind,
        cwd: pin.as_ref().map_or(workspace, |p| p.path.clone()),
        workspace: pin,
        model: None,
        permissions: verifier_policy(),
        idle_after: None,
    };
    let row = mailroom
        .open_verification(
            v.report,
            VerifierStart {
                spec,
                budget: runner.settings.budget,
                verifier_family,
                claimant_family,
                snapshot_clean,
            },
        )
        .await?;
    let verifier = row
        .verifier
        .ok_or_else(|| MailError::Rejected("bug: an opened verification has no verifier".into()))?;
    let saved = mailroom
        .agent_spec(verifier)
        .await?
        .ok_or_else(|| MailError::Rejected("bug: an opened verifier has no saved spec".into()))?;
    if let Err(error) = host.start_verifier(saved).await {
        // The row is running now. The next pass restarts it, and its task
        // expires if it never runs.
        tracing::error!(report=%v.report, %verifier, %error, "verifier did not start");
    }
    Ok(Started::Running)
}

#[cfg(test)]
mod tests;
