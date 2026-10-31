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
}
#[derive(Default)]
pub(crate) struct Credentials {
    copies: HashMap<AgentId, Vec<Copy>>,
    retired: HashMap<PathBuf, HashSet<Vec<u8>>>,
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
        paths: &[PathBuf],
        home: &Path,
        host_home: &Path,
    ) -> Result<()> {
        self.finish(id);
        let private = Dir::open(home, false)?;
        let mut copies = vec![];
        for host in paths {
            let relative = host.strip_prefix(host_home)?.to_path_buf();
            let bytes = match host_file(host) {
                Ok((dir, name)) => dir.read(&name)?,
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    None
                }
                Err(e) => return Err(e),
            };
            let (dir, name) = private.parent(&relative, true)?;
            // Never trust persisted credentials from a previous incarnation.
            let old = dir.read(&name)?;
            if !dir.replace(&name, old.as_deref(), bytes.as_deref())? {
                return Err("private credential changed during launch".into());
            }
            copies.push(Copy {
                host: host.clone(),
                relative,
                home: home.into(),
                kind: kind.into(),
                baseline: bytes,
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
            if let Err(error) = self.refresh(copy) {
                tracing::warn!(%id, %error, "credential sync skipped");
            }
        }
        self.copies.insert(id, copies);
    }
    fn refresh(&mut self, copy: &mut Copy) -> Result<()> {
        let private = Dir::open(&copy.home, false)?;
        let (dir, name) = private.parent(&copy.relative, false)?;
        let Some(bytes) = dir.read(&name)? else {
            return Ok(());
        };
        if Some(&bytes) == copy.baseline.as_ref() {
            return Ok(());
        }
        let canonical = serde_json::to_vec(&serde_json::from_slice::<serde_json::Value>(&bytes)?)?;
        if self
            .retired
            .get(&copy.host)
            .is_some_and(|set| set.contains(&canonical))
        {
            return Err("refusing superseded credential bytes".into());
        }
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
        Ok(())
    }
    fn retire(&mut self, copy: &Copy) {
        if let Some(old) = &copy.baseline {
            let canonical = serde_json::from_slice::<serde_json::Value>(old)
                .and_then(|value| serde_json::to_vec(&value))
                .unwrap_or_else(|_| old.clone());
            self.retired
                .entry(copy.host.clone())
                .or_default()
                .insert(canonical);
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
    fn refresh_cas_propagates_and_refuses_stale_or_external_updates() {
        let (root, human, host, home) = fixture();
        let other = root.path().join("sibling");
        std::fs::create_dir(&other).expect("sibling");
        let mut credentials = Credentials::default();
        let a = AgentId(ulid::Ulid::new());
        let b = AgentId(ulid::Ulid::new());
        credentials
            .launch(a, "codex", std::slice::from_ref(&host), &home, &human)
            .expect("launch a");
        credentials
            .launch(b, "codex", std::slice::from_ref(&host), &other, &human)
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
            .launch(id, "codex", std::slice::from_ref(&host), &home, &human)
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
                .launch(id, "codex", std::slice::from_ref(&host), &home, &human)
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
                .launch(id, "codex", &[host], &home, &human)
                .is_err()
        );
    }
    #[test]
    fn missing_credentials_launch_without_a_persisted_token() {
        let (_root, human, host, home) = fixture();
        std::fs::remove_file(&host).expect("remove");
        let mut credentials = Credentials::default();
        let id = AgentId(ulid::Ulid::new());
        credentials
            .launch(id, "codex", &[host], &home, &human)
            .expect("missing credential");
        assert!(!home.join(".codex/auth.json").exists());
    }
}
