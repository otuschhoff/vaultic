use std::{
    fs,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tokio::net::UnixStream;

use super::protocol::PeerProcess;

pub fn inspect_peer(stream: &UnixStream) -> Result<PeerProcess> {
    let credentials = stream.peer_cred().context("read Unix peer credentials")?;
    let uid = credentials.uid();
    let pid = peer_pid(stream, &credentials)?;
    let executable = peer_executable(pid)?;
    let metadata = fs::metadata(&executable)
        .with_context(|| format!("inspect peer executable {}", executable.display()))?;
    let owned_by_root = metadata.uid() == 0;
    let installation_path_read_only = trusted_installation_path(&executable)?;
    let executable_sha256 = format!("{:x}", Sha256::digest(fs::read(&executable)?));
    Ok(PeerProcess {
        uid,
        executable_sha256,
        owned_by_root,
        installation_path_read_only,
    })
}

#[cfg(target_os = "linux")]
fn peer_pid(_stream: &UnixStream, credentials: &tokio::net::unix::UCred) -> Result<u32> {
    credentials
        .pid()
        .map(|pid| pid as u32)
        .context("Unix peer did not expose a process ID")
}

#[cfg(target_os = "macos")]
fn peer_pid(stream: &UnixStream, _credentials: &tokio::net::unix::UCred) -> Result<u32> {
    let mut pid: libc::pid_t = 0;
    let mut length = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&mut pid as *mut libc::pid_t).cast(),
            &mut length,
        )
    };
    if result != 0 || pid <= 0 {
        return Err(std::io::Error::last_os_error()).context("read Unix peer process ID");
    }
    Ok(pid as u32)
}

#[cfg(target_os = "linux")]
fn peer_executable(pid: u32) -> Result<PathBuf> {
    fs::read_link(format!("/proc/{pid}/exe")).context("resolve peer executable")
}

#[cfg(target_os = "macos")]
fn peer_executable(pid: u32) -> Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    let mut buffer = vec![0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    let length = unsafe {
        libc::proc_pidpath(
            pid as libc::pid_t,
            buffer.as_mut_ptr().cast(),
            buffer.len() as u32,
        )
    };
    if length <= 0 {
        return Err(std::io::Error::last_os_error()).context("resolve peer executable");
    }
    buffer.truncate(length as usize);
    Ok(PathBuf::from(std::ffi::OsString::from_vec(buffer)))
}

pub fn trusted_installation_path(executable: &Path) -> Result<bool> {
    let canonical = executable.canonicalize()?;
    for path in canonical.ancestors() {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0
        {
            return Ok(false);
        }
    }
    Ok(true)
}
