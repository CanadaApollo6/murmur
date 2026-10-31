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
        let Some(mut copies) = self.copies.remove(&id) else {
            return;
        };
        for copy in &mut copies {
            if let Err(error) = self.refresh(id, copy) {
                let message = error.to_string();
                if copy.last_error.as_ref() != Some(&message) {
                    tracing::warn!(%id, %error, "credential sync skipped");
                    copy.last_error = Some(message);
                }
            }
        }
        self.copies.insert(id, copies);
    }
    fn refresh(&mut self, id: AgentId, copy: &mut Copy) -> Result<()> {
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
        if self
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
        self.sync(id);
        if let Some(copies) = self.copies.remove(&id) {
            for copy in copies {
                self.retire(&copy);
            }
        }
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
        let human = root.path().join("human");
        std::fs::create_dir_all(human.join(".codex")).expect("CLI dir");
        let host = human.join(".codex/auth.json");
        std::fs::write(&host, b"{\"token\":0}").expect("credential");
        let home = root.path().join("private");
        std::fs::create_dir(&home).expect("home");
        (root, human, host, home)
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
        let other = root.path().join("sibling");
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
