use std::{fs::OpenOptions, path::Path};

use anyhow::{bail, Context, Result};
use fs2::FileExt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum BackendAccessMode {
    #[default]
    Shared,
    Dedicated,
}

impl BackendAccessMode {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "shared" => Ok(Self::Shared),
            "dedicated" => Ok(Self::Dedicated),
            _ => bail!("backend access mode must be shared or dedicated"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BackendAccessGuard(std::fs::File);

impl BackendAccessGuard {
    pub(crate) fn acquire(root: &Path, mode: BackendAccessMode) -> Result<Self> {
        std::fs::create_dir_all(root).context("create backend access root")?;
        if mode == BackendAccessMode::Dedicated {
            ensure_local_filesystem(root)?;
        }
        let mut path = std::fs::canonicalize(root)?.into_os_string();
        path.push(".access.lock");
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let file = options.open(&path).context("open backend access lock")?;
        if !file.metadata()?.is_file() {
            bail!("backend access lock must be a regular file");
        }
        match mode {
            BackendAccessMode::Shared => FileExt::try_lock_shared(&file),
            BackendAccessMode::Dedicated => FileExt::try_lock_exclusive(&file),
        }
        .with_context(|| format!("backend access conflict or unsupported locking ({mode:?})"))?;
        Ok(Self(file))
    }
}

fn ensure_local_filesystem(root: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};
        let path = CString::new(root.as_os_str().as_bytes())?;
        let mut status = std::mem::MaybeUninit::<libc::statfs>::uninit();
        if unsafe { libc::statfs(path.as_ptr(), status.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error()).context("inspect backend filesystem");
        }
        let kind = unsafe { status.assume_init() }.f_type as u64;
        if !matches!(
            kind,
            0xef53 | 0x58465342 | 0x9123683e | 0x01021994 | 0x794c7630 | 0x2fc12fc1
        ) {
            bail!("dedicated access requires a supported local filesystem; network filesystems are not fenced");
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = root;
        bail!("dedicated backend access is currently supported only on Linux local filesystems")
    }
}

impl Drop for BackendAccessGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_access_mode_parser_is_strict() {
        assert_eq!(BackendAccessMode::default(), BackendAccessMode::Shared);
        assert_eq!(
            BackendAccessMode::parse("shared").unwrap(),
            BackendAccessMode::Shared
        );
        assert_eq!(
            BackendAccessMode::parse("dedicated").unwrap(),
            BackendAccessMode::Dedicated
        );
        for invalid in ["", "exclusive", "Dedicated", " shared"] {
            assert!(BackendAccessMode::parse(invalid).is_err());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backend_access_is_process_scoped_and_resolves_aliases() {
        const CHILD_ROOT: &str = "VAULTICDB_ACCESS_TEST_CHILD_ROOT";
        const CHILD_ALLOWED: &str = "VAULTICDB_ACCESS_TEST_CHILD_ALLOWED";
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            assert_eq!(
                BackendAccessGuard::acquire(Path::new(&root), BackendAccessMode::Shared).is_ok(),
                std::env::var(CHILD_ALLOWED).unwrap() == "true"
            );
            return;
        }
        let root = std::env::temp_dir().join(format!(
            "vaulticdb-process-access-{}",
            rand::random::<u64>()
        ));
        let alias = root.with_extension("alias");
        let dedicated = BackendAccessGuard::acquire(&root, BackendAccessMode::Dedicated).unwrap();
        std::os::unix::fs::symlink(&root, &alias).unwrap();
        let child = |allowed: bool| {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::access::tests::backend_access_is_process_scoped_and_resolves_aliases",
                    "--nocapture",
                ])
                .env(CHILD_ROOT, &alias)
                .env(CHILD_ALLOWED, allowed.to_string())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        };
        child(false);
        drop(dedicated);
        child(true);
        std::fs::remove_file(alias).unwrap();
        std::fs::remove_file(root.with_extension("access.lock")).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backend_access_modes_exclude_dedicated_and_release_on_drop() {
        let root = std::env::temp_dir().join(format!("vaulticdb-access-{}", rand::random::<u64>()));
        let first = BackendAccessGuard::acquire(&root, BackendAccessMode::Shared).unwrap();
        let second = BackendAccessGuard::acquire(&root, BackendAccessMode::Shared).unwrap();
        assert!(BackendAccessGuard::acquire(&root, BackendAccessMode::Dedicated).is_err());
        drop(first);
        assert!(BackendAccessGuard::acquire(&root, BackendAccessMode::Dedicated).is_err());
        drop(second);
        let dedicated = BackendAccessGuard::acquire(&root, BackendAccessMode::Dedicated).unwrap();
        assert!(BackendAccessGuard::acquire(&root, BackendAccessMode::Dedicated).is_err());
        assert!(BackendAccessGuard::acquire(&root, BackendAccessMode::Shared).is_err());
        drop(dedicated);
        assert!(BackendAccessGuard::acquire(&root, BackendAccessMode::Shared).is_ok());
        std::fs::remove_file(root.with_extension("access.lock")).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
