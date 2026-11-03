//! No-follow access to broker files in agent-writable trees.
use nix::{
    fcntl::{OFlag, open, openat},
    sys::stat::{Mode, mkdirat},
    unistd::{UnlinkatFlags, unlinkat},
};
use std::{
    fs::File,
    io::{self, Read, Write},
    os::fd::{AsRawFd, OwnedFd},
    path::{Component, Path},
};

pub(crate) const LIMIT: u64 = 256 * 1024;
pub(crate) struct Dir(OwnedFd);
impl Dir {
    pub fn open(path: &Path, create: bool) -> io::Result<Self> {
        let root = Self(open(
            "/",
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )?);
        root.descend(path.strip_prefix("/").map_err(io::Error::other)?, create)
    }
    pub fn descend(&self, path: &Path, create: bool) -> io::Result<Self> {
        let mut fd = self.0.try_clone()?;
        for component in path.components() {
            let Component::Normal(name) = component else {
                return Err(io::Error::other("unsafe relative path"));
            };
            if create {
                match mkdirat(&fd, name, Mode::from_bits_truncate(0o700)) {
                    Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            fd = openat(
                &fd,
                name,
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty(),
            )?;
        }
        Ok(Self(fd))
    }
    pub fn parent(&self, path: &Path, create: bool) -> io::Result<(Self, std::ffi::OsString)> {
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::other("file needs name"))?
            .to_owned();
        Ok((
            self.descend(
                path.parent()
                    .ok_or_else(|| io::Error::other("file needs parent"))?,
                create,
            )?,
            name,
        ))
    }
    pub fn seed(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let (dir, name) = self.parent(path, true)?;
        match openat(
            &dir.0,
            name.as_os_str(),
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        ) {
            Ok(fd) => File::from(fd).write_all(bytes),
            Err(nix::errno::Errno::EEXIST) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    pub fn read(&self, name: &std::ffi::OsStr) -> io::Result<Option<Vec<u8>>> {
        let fd = match openat(
            &self.0,
            name,
            OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(nix::errno::Errno::ENOENT) => return Ok(None),
            Err(nix::errno::Errno::ELOOP) => {
                return Err(io::Error::other("credential leaf must not be a symlink"));
            }
            Err(e) => return Err(e.into()),
        };
        let file = File::from(fd);
        if !file.metadata()?.is_file() || file.metadata()?.len() > LIMIT {
            return Err(io::Error::other(
                "credential must be a regular file under 256 KiB",
            ));
        }
        let mut bytes = Vec::new();
        file.take(LIMIT + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > LIMIT {
            return Err(io::Error::other("credential exceeds 256 KiB"));
        }
        Ok(Some(bytes))
    }
    pub fn replace(
        &self,
        name: &std::ffi::OsStr,
        old: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> io::Result<bool> {
        if self.read(name)?.as_deref() != old {
            return Ok(false);
        }
        let Some(bytes) = bytes else {
            if old.is_some() {
                unlinkat(&self.0, name, UnlinkatFlags::NoRemoveDir)?;
            }
            return Ok(true);
        };
        let tmp = format!(".agentd-{}", ulid::Ulid::new());
        let fd = openat(
            &self.0,
            tmp.as_str(),
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )?;
        let result = (|| {
            let mut file = File::from(fd);
            file.write_all(bytes)?;
            file.sync_all()?;
            if self.read(name)?.as_deref() != old {
                return Ok(false);
            }
            nix::fcntl::renameat(&self.0, tmp.as_str(), &self.0, name)?;
            Ok(true)
        })();
        if !matches!(result, Ok(true)) {
            unlinkat(&self.0, tmp.as_str(), UnlinkatFlags::NoRemoveDir)?;
        }
        result
    }
    // Linux AF_UNIX has no bindat. Keep the directory descriptor alive with the listener.
    pub fn socket_path(&self, name: &str) -> std::path::PathBuf {
        Path::new("/proc/self/fd")
            .join(self.0.as_raw_fd().to_string())
            .join(name)
    }
    pub fn clear(&self, name: &std::ffi::OsStr) -> io::Result<()> {
        use std::os::unix::ffi::OsStrExt;
        const MAX_DEPTH: usize = 32;
        const MAX_ENTRIES: usize = 4096;
        let mut stack: Vec<(Self, nix::dir::OwningIter, std::ffi::OsString)> = Vec::new();
        let mut pending = Some(name.to_owned());
        let mut visited = 0;
        loop {
            if let Some(name) = pending.take() {
                visited += 1;
                if visited > MAX_ENTRIES {
                    return Err(io::Error::other("socket cleanup exceeds 4096 entries"));
                }
                let parent = stack.last().map_or(self, |(dir, _, _)| dir);
                match openat(
                    &parent.0,
                    name.as_os_str(),
                    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                    Mode::empty(),
                ) {
                    Ok(fd) => {
                        if stack.len() >= MAX_DEPTH {
                            return Err(io::Error::other(
                                "socket cleanup exceeds 32 directories deep",
                            ));
                        }
                        let dir = Self(fd);
                        let entries = nix::dir::Dir::from_fd(dir.0.try_clone()?)?.into_iter();
                        stack.push((dir, entries, name));
                    }
                    Err(nix::errno::Errno::ENOENT) => {}
                    Err(nix::errno::Errno::ENOTDIR | nix::errno::Errno::ELOOP) => {
                        unlinkat(&parent.0, name.as_os_str(), UnlinkatFlags::NoRemoveDir)?;
                    }
                    Err(e) => return Err(e.into()),
                }
                continue;
            }
            let Some((_, entries, _)) = stack.last_mut() else {
                return Ok(());
            };
            if let Some(entry) = entries.next() {
                let entry = entry?;
                let name = std::ffi::OsStr::from_bytes(entry.file_name().to_bytes());
                if name != "." && name != ".." {
                    pending = Some(name.to_owned());
                }
            } else {
                let (_, _, name) = stack.pop().expect("bug: cleanup directory absent");
                let parent = stack.last().map_or(self, |(dir, _, _)| dir);
                unlinkat(&parent.0, name.as_os_str(), UnlinkatFlags::RemoveDir)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_cleanup_rejects_excessive_depth() {
        let root = tempfile::tempdir().expect("root");
        let mut nested = root.path().join("mcp.sock");
        for _ in 0..40 {
            nested.push("d");
        }
        std::fs::create_dir_all(&nested).expect("deep socket tree");
        let dir = Dir::open(root.path(), false).expect("io directory");
        assert!(dir.clear("mcp.sock".as_ref()).is_err());
        assert!(nested.exists(), "over-limit tree is retained");
    }

    #[test]
    fn socket_cleanup_rejects_excessive_entries() {
        let root = tempfile::tempdir().expect("root");
        let socket = root.path().join("mcp.sock");
        std::fs::create_dir(&socket).expect("socket tree");
        for i in 0..5000 {
            std::fs::write(socket.join(i.to_string()), []).expect("entry");
        }
        let dir = Dir::open(root.path(), false).expect("io directory");
        assert!(dir.clear("mcp.sock".as_ref()).is_err());
        assert!(socket.exists(), "over-limit tree is retained");
    }

    #[test]
    fn socket_cleanup_removes_small_trees_without_following_links() {
        let root = tempfile::tempdir().expect("root");
        let outside = root.path().join("outside");
        std::fs::create_dir(&outside).expect("outside");
        std::fs::write(outside.join("keep"), "safe").expect("outside file");
        let socket = root.path().join("mcp.sock");
        std::fs::create_dir_all(socket.join("nested")).expect("socket tree");
        std::os::unix::fs::symlink(&outside, socket.join("nested/link")).expect("link");
        let dir = Dir::open(root.path(), false).expect("io directory");
        dir.clear("mcp.sock".as_ref()).expect("bounded cleanup");
        dir.clear("mcp.sock".as_ref()).expect("already absent");
        assert!(!socket.exists());
        assert_eq!(
            std::fs::read_to_string(outside.join("keep")).expect("outside file"),
            "safe"
        );
    }
}
