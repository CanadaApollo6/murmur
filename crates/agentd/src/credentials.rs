//! Credential bytes are the only data copied across private CLI homes.
use crate::{model::AgentId, private_fs::Dir, surfaces::Result};
use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    path::{Path, PathBuf},
};

struct Copy {
    host: PathBuf,
    relative: PathBuf,
    home: PathBuf,
    kind: String,
    baseline: Option<Vec<u8>>,
    rejected: Option<[u8; 32]>,
    last_error: Option<String>,
}
#[derive(Default)]
pub(crate) struct Credentials {
    copies: HashMap<AgentId, Vec<Copy>>,
    retired: HashMap<PathBuf, HashSet<[u8; 32]>>,
    exhausted: HashSet<PathBuf>,
    accepted: HashMap<AgentId, std::time::Instant>,
    /// Copies whose last change never reached the host. `discard` keeps them.
    unsynced: HashMap<AgentId, HashSet<PathBuf>>,
}
const HISTORY_LIMIT: usize = 1024;
const COPY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
fn digest(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}
fn normalized_digest(bytes: &[u8]) -> [u8; 32] {
    digest(
        &serde_json::from_slice::<serde_json::Value>(bytes)
            .and_then(|v| serde_json::to_vec(&v))
            .unwrap_or_else(|_| bytes.to_vec()),
    )
}
fn host_file(path: &Path) -> Result<(Dir, OsString)> {
    Ok((
        Dir::open(path.parent().ok_or("credential needs parent")?, false)?,
        path.file_name().ok_or("credential needs name")?.into(),
    ))
}
impl Copy {
    /// The private file holds a change the host never took. Bytes the broker
    /// rejected do not count.
    fn unsynced(&self) -> bool {
        let read = Dir::open(&self.home, false)
            .and_then(|home| home.parent(&self.relative, false))
            .and_then(|(dir, name)| dir.read(&name));
        match read {
            Ok(Some(bytes)) => {
                Some(&bytes) != self.baseline.as_ref() && self.rejected != Some(digest(&bytes))
            }
            _ => false,
        }
    }
}
impl Credentials {
    pub fn launch(
        &mut self,
        id: AgentId,
        kind: &str,
        paths: &[(PathBuf, PathBuf)],
        home: &Path,
    ) -> Result<()> {
        self.finish(id);
        let private = Dir::open(home, false)?;
        let mut copies = vec![];
        for (host, relative) in paths {
            let bytes = match host_file(host) {
                Ok((dir, name)) => dir.read(&name),
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    Ok(None)
                }
                Err(error) => return Err(format!("credential {}: {error}", host.display()).into()),
            }
            .map_err(|e| format!("credential {}: {e}", host.display()))?;
            let (dir, name) = private.parent(relative, true)?;
            // Never trust persisted credentials from a previous incarnation.
            let old = dir.read(&name)?;
            if !dir.replace(&name, old.as_deref(), bytes.as_deref())? {
                return Err("private credential changed during launch".into());
            }
            // Only a copy this launch replaced has nothing left to keep. A
            // launch that fails earlier leaves the record for a later retire.
            if let Some(unsynced) = self.unsynced.get_mut(&id) {
                unsynced.remove(relative);
                if unsynced.is_empty() {
                    self.unsynced.remove(&id);
                }
            }
            copies.push(Copy {
                host: host.clone(),
                relative: relative.clone(),
                home: home.into(),
                kind: kind.into(),
                baseline: bytes,
                rejected: None,
                last_error: None,
            });
        }
        self.copies.insert(id, copies);
        Ok(())
    }
    pub fn sync(&mut self, id: AgentId) {
        self.sync_copies(id, false);
    }
    fn sync_copies(&mut self, id: AgentId, final_sync: bool) {
        let Some(mut copies) = self.copies.remove(&id) else {
            return;
        };
        for copy in &mut copies {
            if let Err(error) = self.refresh(id, copy, final_sync) {
                let message = error.to_string();
                if copy.last_error.as_ref() != Some(&message) {
                    tracing::warn!(%id, %error, "credential sync skipped");
                    copy.last_error = Some(message);
                }
            }
        }
        self.copies.insert(id, copies);
    }
    fn refresh(&mut self, id: AgentId, copy: &mut Copy, final_sync: bool) -> Result<()> {
        let private = Dir::open(&copy.home, false)?;
        let (dir, name) = private.parent(&copy.relative, false)?;
        let Some(bytes) = dir.read(&name)? else {
            return Ok(());
        };
        if Some(&bytes) == copy.baseline.as_ref() {
            return Ok(());
        }
        let raw_digest = digest(&bytes);
        if copy.rejected == Some(raw_digest) {
            return Ok(());
        }
        if self
            .retired
            .get(&copy.host)
            .is_some_and(|set| set.len() >= HISTORY_LIMIT)
        {
            if self.exhausted.insert(copy.host.clone()) {
                tracing::warn!(path=%copy.host.display(), "credential history full; copy-back disabled until broker restart");
            }
            return Ok(());
        }
        if !final_sync
            && self
                .accepted
                .get(&id)
                .is_some_and(|at| at.elapsed() < COPY_INTERVAL)
        {
            return Ok(());
        }
        let content = (|| -> Result<()> {
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            let baseline: serde_json::Value = serde_json::from_slice(
                copy.baseline
                    .as_deref()
                    .ok_or("no credential baseline to replace")?,
            )?;
            let (Some(value_object), Some(baseline)) = (value.as_object(), baseline.as_object())
            else {
                return Err("credential refresh and baseline must be JSON objects".into());
            };
            if value_object.len() != baseline.len()
                || value_object.keys().any(|key| !baseline.contains_key(key))
            {
                return Err(
                    "credential refresh must preserve the baseline top-level key set".into(),
                );
            }
            let canonical = digest(&serde_json::to_vec(&value)?);
            if self
                .retired
                .get(&copy.host)
                .is_some_and(|set| set.contains(&canonical))
            {
                return Err("refusing superseded credential bytes".into());
            }
            Ok(())
        })();
        if let Err(error) = content {
            copy.rejected = Some(raw_digest);
            return Err(error);
        }
        copy.rejected = None;
        let (host, name) = host_file(&copy.host)?;
        if !host.replace(&name, copy.baseline.as_deref(), Some(&bytes))? {
            self.retire(copy);
            return Err("host credential changed; compare-and-swap skipped".into());
        }
        self.retire(copy);
        for sibling in
            self.copies.values_mut().flatten().filter(|s| {
                s.kind == copy.kind && s.host == copy.host && s.baseline == copy.baseline
            })
        {
            let result = (|| -> Result<bool> {
                let home = Dir::open(&sibling.home, false)?;
                let (dir, name) = home.parent(&sibling.relative, false)?;
                Ok(dir.replace(&name, copy.baseline.as_deref(), Some(&bytes))?)
            })();
            match result {
                Ok(true) => sibling.baseline = Some(bytes.clone()),
                Ok(false) => tracing::warn!("sibling credential changed; compare-and-swap skipped"),
                Err(error) => tracing::warn!(%error, "sibling credential sync skipped"),
            }
        }
        copy.baseline = Some(bytes);
        copy.rejected = None;
        copy.last_error = None;
        self.accepted.insert(id, std::time::Instant::now());
        Ok(())
    }
    fn retire(&mut self, copy: &Copy) {
        if let Some(old) = &copy.baseline {
            let set = self.retired.entry(copy.host.clone()).or_default();
            if set.len() < HISTORY_LIMIT {
                set.insert(normalized_digest(old));
            }
        }
    }
    pub fn finish(&mut self, id: AgentId) {
        self.sync_copies(id, true);
        if let Some(copies) = self.copies.remove(&id) {
            let unsynced: HashSet<PathBuf> = copies
                .iter()
                .filter(|copy| copy.unsynced())
                .map(|copy| copy.relative.clone())
                .collect();
            if unsynced.is_empty() {
                self.unsynced.remove(&id);
            } else {
                self.unsynced.insert(id, unsynced);
            }
            for copy in copies {
                self.retire(&copy);
            }
        }
    }
    /// Deletes a retired agent's credential copies and returns the ones it
    /// kept. A copy stays when its last change never reached the host, so the
    /// only refreshed credential is not destroyed. The agent could write its
    /// home, so nothing is read and no path component is followed through a link.
    pub fn discard(
        &mut self,
        id: AgentId,
        home: &Path,
        relatives: &[PathBuf],
    ) -> std::io::Result<Vec<PathBuf>> {
        let mut kept = vec![];
        for relative in relatives {
            if self
                .unsynced
                .get(&id)
                .is_some_and(|set| set.contains(relative))
            {
                kept.push(relative.clone());
                continue;
            }
            match Dir::open(home, false).and_then(|home| home.parent(relative, false)) {
                Ok((dir, name)) => dir.clear(&name)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(kept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sources(host: &Path, human: &Path) -> Vec<(PathBuf, PathBuf)> {
        vec![(
            host.into(),
            host.strip_prefix(human).expect("relative").into(),
        )]
    }
    fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().canonicalize().expect("canonical root");
        let human = path.join("human");
        std::fs::create_dir_all(human.join(".codex")).expect("CLI dir");
        let host = human.join(".codex/auth.json");
        std::fs::write(&host, b"{\"token\":0}").expect("credential");
        let home = path.join("private");
        std::fs::create_dir(&home).expect("home");
        (root, human, host, home)
    }
    #[test]
    fn discard_removes_the_copy_and_never_follows_a_link() {
        let (_root, human, host, home) = fixture();
        let relative = PathBuf::from(".codex/auth.json");
        let paths = [relative.clone()];
        let id = AgentId::new();
        let mut c = Credentials::default();
        let none: Vec<PathBuf> = vec![];
        assert_eq!(
            c.discard(id, &home.join("absent"), &paths)
                .expect("no home"),
            none
        );
        assert_eq!(c.discard(id, &home, &paths).expect("no copy"), none);
        std::fs::create_dir(home.join(".codex")).expect("CLI dir");
        // Larger than a credential read accepts. Removal must not read it.
        std::fs::write(
            home.join(&relative),
            vec![b' '; crate::private_fs::LIMIT as usize + 1],
        )
        .expect("copy");
        assert_eq!(c.discard(id, &home, &paths).expect("discard"), none);
        assert!(!home.join(&relative).exists(), "the copy is gone");
        std::os::unix::fs::symlink(&host, home.join(&relative)).expect("leaf link");
        c.discard(id, &home, &paths)
            .expect("a leaf link is removed");
        assert!(
            home.join(&relative).symlink_metadata().is_err(),
            "the link is gone"
        );
        assert!(host.exists(), "the link's target survives");
        std::fs::remove_dir(home.join(".codex")).expect("remove CLI dir");
        std::os::unix::fs::symlink(human.join(".codex"), home.join(".codex")).expect("link");
        c.discard(id, &home, &paths)
            .expect_err("a linked CLI dir is refused");
        assert!(host.exists(), "the human's credential survives");
    }
    #[test]
    fn discard_keeps_a_refresh_that_never_reached_the_host() {
        let (_root, human, host, home) = fixture();
        let relative = PathBuf::from(".codex/auth.json");
        let paths = [relative.clone()];
        let private = home.join(&relative);
        let id = AgentId::new();
        let mut c = Credentials::default();
        c.launch(id, "codex", &sources(&host, &human), &home)
            .expect("launch");
        std::fs::write(&private, br#"{"token":1}"#).expect("refresh");
        let parent = host.parent().expect("parent");
        let moved = human.join("moved");
        std::fs::rename(parent, &moved).expect("unavailable host parent");
        c.finish(id);
        // A restart that cannot read the host replaces nothing, so the
        // refresh is still the only copy and stays protected.
        std::fs::create_dir(parent).expect("host parent");
        std::fs::create_dir(&host).expect("unreadable host credential");
        c.launch(id, "codex", &sources(&host, &human), &home)
            .expect_err("launch with an unreadable host credential");
        c.finish(id);
        std::fs::remove_dir(&host).expect("remove unreadable credential");
        std::fs::remove_dir(parent).expect("remove host parent");
        std::fs::rename(&moved, parent).expect("restore parent");
        assert_eq!(c.discard(id, &home, &paths).expect("discard"), paths);
        assert_eq!(
            std::fs::read(&private).expect("kept copy"),
            br#"{"token":1}"#,
            "the only refreshed credential survives"
        );
        assert_eq!(std::fs::read(&host).expect("host"), br#"{"token":0}"#);

        // A refresh the host took, and bytes the broker rejected, both go.
        for (bytes, host_after) in [
            (&br#"{"token":2}"#[..], &br#"{"token":2}"#[..]),
            (&br#"{"other":2}"#[..], &br#"{"token":0}"#[..]),
        ] {
            std::fs::write(&host, br#"{"token":0}"#).expect("reset host");
            c.launch(id, "codex", &sources(&host, &human), &home)
                .expect("launch");
            std::fs::write(&private, bytes).expect("change");
            c.finish(id);
            assert_eq!(std::fs::read(&host).expect("host"), host_after);
            assert!(c.discard(id, &home, &paths).expect("discard").is_empty());
            assert!(!private.exists(), "the copy is gone");
        }
    }
    #[test]
    fn host_io_and_cas_failures_retry_identical_valid_refresh_bytes() {
        for failure in ["io", "cas"] {
            let (_root, human, host, home) = fixture();
            let id = AgentId::new();
            let mut c = Credentials::default();
            c.launch(id, "codex", &sources(&host, &human), &home)
                .expect("launch");
            let private = home.join(".codex/auth.json");
            std::fs::write(&private, br#"{"token":1}"#).expect("refresh");
            let parent = host.parent().expect("parent");
            let moved = human.join("moved");
            if failure == "io" {
                std::fs::rename(parent, &moved).expect("unavailable host parent");
            } else {
                std::fs::write(&host, br#"{"token":2}"#).expect("external write");
            }
            c.sync(id);
            if failure == "io" {
                std::fs::rename(&moved, parent).expect("restore parent");
            } else {
                std::fs::write(&host, br#"{"token":0}"#).expect("restore baseline");
            }
            c.sync(id);
            assert_eq!(
                std::fs::read(&host).expect("retried host update"),
                br#"{"token":1}"#
            );
        }
    }
    #[test]
    fn refresh_cas_propagates_and_refuses_stale_or_external_updates() {
        let (root, human, host, home) = fixture();
        let other = root
            .path()
            .canonicalize()
            .expect("canonical root")
            .join("sibling");
        std::fs::create_dir(&other).expect("sibling");
        let mut credentials = Credentials::default();
        let a = AgentId(ulid::Ulid::new());
        let b = AgentId(ulid::Ulid::new());
        credentials
            .launch(a, "codex", &sources(&host, &human), &home)
            .expect("launch a");
        credentials
            .launch(b, "codex", &sources(&host, &human), &other)
            .expect("launch b");
        let private = home.join(".codex/auth.json");
        let sibling = other.join(".codex/auth.json");
        let update = home.join(".codex/tmp");
        std::fs::write(&update, b"{\"token\":1}").expect("refresh");
        std::fs::rename(&update, &private).expect("rename");
        credentials.sync(a);
        assert_eq!(std::fs::read(&host).expect("host"), b"{\"token\":1}");
        assert_eq!(std::fs::read(&sibling).expect("sibling"), b"{\"token\":1}");
        std::fs::write(&sibling, b"{ \"token\": 0 }").expect("cached stale token");
        credentials.sync(b);
        assert_eq!(std::fs::read(&host).expect("host"), b"{\"token\":1}");
        std::fs::write(&host, b"{\"token\":2}").expect("human refresh");
        std::fs::write(&private, b"{\"token\":3}").expect("agent refresh");
        credentials.finish(a);
        assert_eq!(std::fs::read(&host).expect("host"), b"{\"token\":2}");
    }
    #[test]
    fn credential_symlinks_and_non_json_cannot_cross_the_boundary() {
        let (root, human, host, home) = fixture();
        let secret = root.path().join("secret");
        std::fs::write(&secret, b"{\"secret\":true}").expect("secret");
        let mut credentials = Credentials::default();
        let id = AgentId(ulid::Ulid::new());
        credentials
            .launch(id, "codex", &sources(&host, &human), &home)
            .expect("launch");
        let private = home.join(".codex/auth.json");
        for bytes in [
            b"not JSON".as_slice(),
            &vec![b' '; crate::private_fs::LIMIT as usize + 1],
        ] {
            std::fs::write(&private, bytes).expect("private");
            credentials.sync(id);
            assert_eq!(std::fs::read(&host).expect("host"), b"{\"token\":0}");
        }
        std::fs::remove_file(&private).expect("unlink");
        std::os::unix::fs::symlink(&secret, &private).expect("symlink");
        credentials.finish(id);
        assert!(
            credentials
                .launch(id, "codex", &sources(&host, &human), &home)
                .is_err()
        );
        assert_eq!(std::fs::read(&host).expect("host"), b"{\"token\":0}");
        assert_eq!(
            std::fs::read(&secret).expect("secret"),
            b"{\"secret\":true}"
        );
        std::fs::remove_file(&host).expect("host unlink");
        std::os::unix::fs::symlink(&secret, &host).expect("host symlink");
        assert!(
            credentials
                .launch(id, "codex", &sources(&host, &human), &home)
                .is_err()
        );
    }
    #[test]
    fn rate_limit_coalesces_latest_and_survives_relaunch() {
        let (_root, human, host, home) = fixture();
        let id = AgentId::new();
        let mut c = Credentials::default();
        c.launch(id, "codex", &sources(&host, &human), &home)
            .expect("launch");
        let private = home.join(".codex/auth.json");
        std::fs::write(&private, br#"{"token":1}"#).expect("write");
        c.sync(id);
        for i in 2..20 {
            std::fs::write(&private, format!(r#"{{"token":{i}}}"#)).expect("write");
            c.sync(id);
        }
        assert_eq!(std::fs::read(&host).expect("host"), br#"{"token":1}"#);
        c.accepted
            .insert(id, std::time::Instant::now() - COPY_INTERVAL);
        c.sync(id);
        assert_eq!(std::fs::read(&host).expect("host"), br#"{"token":19}"#);
        c.finish(id);
        c.launch(id, "codex", &sources(&host, &human), &home)
            .expect("relaunch");
        std::fs::write(&private, br#"{"token":20}"#).expect("write");
        c.sync(id);
        assert_eq!(std::fs::read(&host).expect("host"), br#"{"token":19}"#);
    }
    #[test]
    fn finish_preserves_latest_refresh_inside_rate_window() {
        let (_root, human, host, home) = fixture();
        let id = AgentId::new();
        let mut c = Credentials::default();
        c.launch(id, "codex", &sources(&host, &human), &home)
            .expect("launch");
        let private = home.join(".codex/auth.json");
        std::fs::write(&private, br#"{"token":1}"#).expect("first rotation");
        c.sync(id);
        std::fs::write(&private, br#"{"token":2}"#).expect("final rotation");
        c.sync(id);
        assert_eq!(
            std::fs::read(&host).expect("rate limited host"),
            br#"{"token":1}"#
        );
        c.finish(id);
        assert_eq!(std::fs::read(&host).expect("final host"), br#"{"token":2}"#);
        c.launch(id, "codex", &sources(&host, &human), &home)
            .expect("relaunch");
        assert_eq!(
            std::fs::read(&private).expect("relaunch token"),
            br#"{"token":2}"#
        );
    }
    #[test]
    fn final_sync_keeps_content_guards_after_recent_acceptance() {
        for invalid in [
            br#"{"token":2,"extra":3}"#.to_vec(),
            vec![b' '; crate::private_fs::LIMIT as usize + 1],
            br#"{"token":0}"#.to_vec(),
        ] {
            let (_root, human, host, home) = fixture();
            let id = AgentId::new();
            let mut c = Credentials::default();
            c.launch(id, "codex", &sources(&host, &human), &home)
                .expect("launch");
            let private = home.join(".codex/auth.json");
            std::fs::write(&private, br#"{"token":1}"#).expect("rotation");
            c.sync(id);
            std::fs::write(&private, invalid).expect("invalid final rotation");
            c.finish(id);
            assert_eq!(std::fs::read(&host).expect("host"), br#"{"token":1}"#);
        }
    }
    #[test]
    fn history_is_bounded_without_eviction_and_bad_content_is_remembered() {
        let (_root, human, host, home) = fixture();
        let id = AgentId::new();
        let mut c = Credentials::default();
        c.launch(id, "codex", &sources(&host, &human), &home)
            .expect("launch");
        let private = home.join(".codex/auth.json");
        for bytes in [
            b"invalid".as_slice(),
            b"{}",
            b"[]",
            br#"{"token":1,"extra":2}"#,
        ] {
            std::fs::write(&private, bytes).expect("write");
            c.sync(id);
            assert_eq!(c.copies[&id][0].rejected, Some(digest(bytes)));
            c.sync(id);
            assert_eq!(std::fs::read(&host).expect("host"), br#"{"token":0}"#);
        }
        for i in 1..=HISTORY_LIMIT {
            c.accepted.remove(&id);
            std::fs::write(&private, format!(r#"{{"token":{i}}}"#)).expect("write");
            c.sync(id);
        }
        assert_eq!(c.retired[&host].len(), HISTORY_LIMIT);
        assert!(c.retired[&host].contains(&normalized_digest(br#"{ "token": 0 }"#)));
        let last = std::fs::read(&host).expect("last accepted");
        c.accepted.remove(&id);
        std::fs::write(&private, br#"{"token":99999}"#).expect("write");
        c.sync(id);
        c.sync(id);
        assert_eq!(std::fs::read(&host).expect("host"), last);
        assert_eq!(c.retired[&host].len(), HISTORY_LIMIT);
        assert!(c.exhausted.contains(&host));
    }
    #[test]
    fn missing_credentials_launch_without_a_persisted_token() {
        let (_root, human, host, home) = fixture();
        std::fs::remove_file(&host).expect("remove");
        let mut credentials = Credentials::default();
        let id = AgentId(ulid::Ulid::new());
        credentials
            .launch(id, "codex", &sources(&host, &human), &home)
            .expect("missing credential");
        assert!(!home.join(".codex/auth.json").exists());
    }
}
