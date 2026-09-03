#![allow(clippy::result_large_err)]

use std::{
    collections::BTreeSet,
    env,
    fs,
    os::{
        fd::AsRawFd,
        unix::fs::{FileTypeExt, MetadataExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signer, SigningKey};
use rand::RngCore;
use rand08::rngs::OsRng as LegacyOsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::Mutex,
    sync::Notify,
};
use vaulticdb::{
    broker::{
        unix_time_ms, Capability, ClientAuthorization, ClientIdentity, EncryptedContribution,
        KeyBroker,
    },
    encryption::recovery_capsule::RecoveryCapsule,
};

const PROTOCOL_VERSION: &str = "vaultic-key-broker.v1";
const MAX_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerConfig {
    format: u32,
    capsule_directory: PathBuf,
    repository_id: String,
    identity_key_path: PathBuf,
    socket_path: PathBuf,
    #[serde(default)]
    maximum_unlocked_seconds: Option<u64>,
    authorizations: Vec<FileAuthorization>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAuthorization {
    component: String,
    minimum_version: u64,
    maximum_version: u64,
    release_identity: String,
    release_public_key: String,
    peer_uid: u32,
    capabilities: BTreeSet<Capability>,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum BrokerRequest {
    Negotiate { protocols: Vec<String> },
    Status,
    CreateSession { ttl_seconds: u64 },
    SubmitContribution { contribution: EncryptedContribution },
    AcquireLease {
        component: String,
        version: u64,
        release_identity: String,
        release_signature: String,
        capability: Capability,
        ttl_seconds: u64,
        challenge_response: String,
    },
    ReleaseLease { lease_id: String },
    Lock,
}

#[derive(Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum BrokerResponse {
    Negotiated {
        protocol: &'static str,
        challenge: String,
    },
    Status {
        protocol: &'static str,
        locked: bool,
        repository_id: String,
        capsule_generation: u64,
        capsule_logical_id: String,
        policy_hash: String,
        epoch_id: Option<String>,
        active_sessions: usize,
        active_leases: usize,
        minimum_custodians: usize,
        principal_verified: bool,
        hardware_verified: bool,
        custody_assumed: bool,
        compliant: bool,
        findings: Vec<String>,
    },
    Session {
        session: vaulticdb::broker::SignedSession,
    },
    Contribution {
        unlocked: bool,
    },
    Lease {
        lease_id: String,
        epoch_id: String,
        capability: Capability,
        expires_unix_ms: u64,
        key_version: u32,
        capsule_generation: u64,
        key: String,
    },
    Ok,
    Error {
        code: &'static str,
        message: String,
    },
}

struct PeerProcess {
    uid: u32,
    executable_sha256: String,
    owned_by_root: bool,
    installation_path_read_only: bool,
}

#[derive(Default)]
struct ConnectionProtocol {
    negotiated: bool,
    lease_challenge: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    disable_core_dumps();
    let arguments = env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.first().is_some_and(|argument| argument == "identity-init") {
        return identity_init(&arguments[1..]);
    }
    if arguments.first().is_some_and(|argument| argument == "release-sign") {
        return release_sign(&arguments[1..]);
    }
    let config_path = arguments
        .first()
        .map(PathBuf::from)
        .or_else(|| env::var_os("VAULTIC_KEY_BROKER_CONFIG").map(PathBuf::from))
        .context("usage: vaultic-key-broker CONFIG.json | identity-init PRIVATE PUBLIC | release-sign PRIVATE EXECUTABLE COMPONENT VERSION IDENTITY OUTPUT")?;
    require_private_regular_file(&config_path)?;
    let config: BrokerConfig =
        serde_json::from_slice(&fs::read(&config_path)?).context("decode broker config")?;
    if config.format != 1 || config.authorizations.is_empty() {
        bail!("unsupported broker config or empty authorization policy");
    }
    require_private_regular_file(&config.identity_key_path)?;
    let (_, capsule): (_, RecoveryCapsule) =
        vaulticdb::encryption::recovery_capsule::discover_latest(
            &config.capsule_directory,
            &config.repository_id,
        )?
        .context("no recovery capsule generation found")?;
    let identity_bytes = fs::read(&config.identity_key_path)?;
    let identity = SigningKey::from_bytes(
        &identity_bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("broker identity key must contain exactly 32 bytes"))?,
    );
    let authorizations = config
        .authorizations
        .into_iter()
        .map(|authorization| {
            let public_key = BASE64
                .decode(&authorization.release_public_key)
                .context("decode release public key")?;
            Ok(ClientAuthorization {
                component: authorization.component,
                minimum_version: authorization.minimum_version,
                maximum_version: authorization.maximum_version,
                release_identity: authorization.release_identity,
                release_public_key: public_key
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("release public key must be 32 bytes"))?,
                peer_uid: authorization.peer_uid,
                capabilities: authorization.capabilities,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let maximum_lifetime = config.maximum_unlocked_seconds.map(Duration::from_secs);
    let broker = Arc::new(Mutex::new(KeyBroker::new(
        capsule,
        identity,
        authorizations,
        maximum_lifetime,
    )?));
    let lock_notification = Arc::new(Notify::new());

    prepare_socket_parent(&config.socket_path)?;
    remove_stale_socket(&config.socket_path).await?;
    let listener = UnixListener::bind(&config.socket_path)
        .with_context(|| format!("bind broker socket {}", config.socket_path.display()))?;
    set_mode(&config.socket_path, 0o600)?;
    let endpoint_binding = format!("unix:{}", config.socket_path.display());

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let broker = broker.clone();
                let endpoint_binding = endpoint_binding.clone();
                let lock_notification = lock_notification.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_connection(stream, broker, endpoint_binding, lock_notification).await {
                        eprintln!("vaultic-key-broker: connection rejected: {error:#}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                broker.lock().await.lock();
                break;
            }
        }
    }
    drop(listener);
    let _ = fs::remove_file(&config.socket_path);
    Ok(())
}

fn identity_init(arguments: &[std::ffi::OsString]) -> Result<()> {
    if arguments.len() != 2 {
        bail!("usage: vaultic-key-broker identity-init PRIVATE PUBLIC");
    }
    let private_path = PathBuf::from(&arguments[0]);
    let public_path = PathBuf::from(&arguments[1]);
    let identity = SigningKey::generate(&mut LegacyOsRng);
    write_new_file(&private_path, identity.as_bytes(), 0o600)?;
    let mut public = BASE64.encode(identity.verifying_key().as_bytes()).into_bytes();
    public.push(b'\n');
    if let Err(error) = write_new_file(&public_path, &public, 0o644) {
        let _ = fs::remove_file(private_path);
        return Err(error);
    }
    Ok(())
}

fn release_sign(arguments: &[std::ffi::OsString]) -> Result<()> {
    if arguments.len() != 6 {
        bail!("usage: vaultic-key-broker release-sign PRIVATE EXECUTABLE COMPONENT VERSION IDENTITY OUTPUT");
    }
    let private_path = PathBuf::from(&arguments[0]);
    let executable = PathBuf::from(&arguments[1]);
    let component = arguments[2].to_string_lossy().into_owned();
    let version = arguments[3]
        .to_string_lossy()
        .parse::<u64>()
        .context("release version must be an unsigned integer")?;
    let release_identity = arguments[4].to_string_lossy().into_owned();
    let output = PathBuf::from(&arguments[5]);
    if component.is_empty() || release_identity.is_empty() || version == 0 {
        bail!("component, non-zero version, and release identity are required");
    }
    require_private_regular_file(&private_path)?;
    let mut private = zeroize::Zeroizing::new(fs::read(&private_path)?);
    let signing_key = SigningKey::from_bytes(
        &private
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("release signing key must contain exactly 32 bytes"))?,
    );
    private.fill(0);
    let executable_sha256 = format!("{:x}", Sha256::digest(fs::read(&executable)?));
    let manifest_bytes = serde_json::to_vec(&(
        "vaultic-client-release-v1",
        &component,
        version,
        &executable_sha256,
        &release_identity,
    ))?;
    let signature = BASE64.encode(signing_key.sign(&manifest_bytes).to_bytes());
    let output_value = serde_json::json!({
        "component": component,
        "version": version,
        "release_identity": release_identity,
        "executable_sha256": executable_sha256,
        "signature": signature,
    });
    let mut encoded = serde_json::to_vec_pretty(&output_value)?;
    encoded.push(b'\n');
    write_new_file(&output, &encoded, 0o644)
}

fn write_new_file(path: &Path, contents: &[u8], mode: u32) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    if let Err(error) = file.write_all(contents).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(path);
        return Err(error).with_context(|| format!("write {}", path.display()));
    }
    file.set_permissions(fs::Permissions::from_mode(mode))?;
    Ok(())
}

async fn serve_connection(
    stream: UnixStream,
    broker: Arc<Mutex<KeyBroker>>,
    endpoint_binding: String,
    lock_notification: Arc<Notify>,
) -> Result<()> {
    let peer = inspect_peer(&stream)?;
    let connection_id = random_id();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut request = Vec::new();
    let mut protocol = ConnectionProtocol::default();
    loop {
        request.clear();
        let read = tokio::select! {
            read = reader.read_until(b'\n', &mut request) => read.context("read broker request")?,
            _ = lock_notification.notified() => break,
        };
        if read == 0 {
            break;
        }
        let response = if request.len() > MAX_REQUEST_BYTES {
            BrokerResponse::Error {
                code: "request_too_large",
                message: "broker request exceeds size limit".to_owned(),
            }
        } else {
            match serde_json::from_slice::<BrokerRequest>(&request) {
                Ok(request) => {
                    let requests_lock = matches!(&request, BrokerRequest::Lock);
                    let response = handle_request(
                    request,
                    &broker,
                    &connection_id,
                    &peer,
                    &endpoint_binding,
                    &mut protocol,
                )
                    .await
                    .unwrap_or_else(|error| BrokerResponse::Error {
                    code: "request_rejected",
                    message: error.to_string(),
                    });
                    if requests_lock {
                        lock_notification.notify_waiters();
                    }
                    response
                }
                Err(error) => BrokerResponse::Error {
                    code: "invalid_request",
                    message: error.to_string(),
                },
            }
        };
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        writer.write_all(&encoded).await?;
        writer.flush().await?;
    }
    broker.lock().await.disconnect(&connection_id);
    Ok(())
}

async fn handle_request(
    request: BrokerRequest,
    broker: &Mutex<KeyBroker>,
    connection_id: &str,
    peer: &PeerProcess,
    endpoint_binding: &str,
    protocol: &mut ConnectionProtocol,
) -> Result<BrokerResponse> {
    let now = unix_time_ms()?;
    let mut broker = broker.lock().await;
    if let BrokerRequest::Negotiate { protocols } = &request {
        if protocol.negotiated {
            bail!("broker protocol is already negotiated");
        }
        if !protocols.iter().any(|value| value == PROTOCOL_VERSION) {
            bail!("no mutually supported broker protocol");
        }
        let challenge = random_id();
        protocol.negotiated = true;
        protocol.lease_challenge = Some(challenge.clone());
        return Ok(BrokerResponse::Negotiated {
            protocol: PROTOCOL_VERSION,
            challenge,
        });
    }
    if !protocol.negotiated {
        bail!("broker protocol negotiation is required");
    }
    match request {
        BrokerRequest::Negotiate { .. } => unreachable!(),
        BrokerRequest::Status => {
            let status = broker.status(now);
            Ok(BrokerResponse::Status {
                protocol: PROTOCOL_VERSION,
                locked: status.locked,
                repository_id: status.repository_id,
                capsule_generation: status.capsule_generation,
                capsule_logical_id: status.capsule_logical_id,
                policy_hash: status.policy_hash,
                epoch_id: status.epoch_id,
                active_sessions: status.active_sessions,
                active_leases: status.active_leases,
                minimum_custodians: status.minimum_custodians,
                principal_verified: status.principal_verified,
                hardware_verified: status.hardware_verified,
                custody_assumed: status.custody_assumed,
                compliant: status.compliant,
                findings: status.findings,
            })
        }
        BrokerRequest::CreateSession { ttl_seconds } => Ok(BrokerResponse::Session {
            session: broker.create_session(
                endpoint_binding,
                Duration::from_secs(ttl_seconds),
                now,
            )?,
        }),
        BrokerRequest::SubmitContribution { contribution } => {
            Ok(BrokerResponse::Contribution {
                unlocked: broker.submit_contribution(contribution, now)?,
            })
        }
        BrokerRequest::AcquireLease {
            component,
            version,
            release_identity,
            release_signature,
            capability,
            ttl_seconds,
            challenge_response,
        } => {
            let challenge = protocol
                .lease_challenge
                .take()
                .context("lease challenge is missing or already consumed")?;
            let expected = lease_challenge_response(&challenge, &peer.executable_sha256);
            if challenge_response != expected {
                bail!("lease challenge response is invalid");
            }
            let client = ClientIdentity {
                connection_id: connection_id.to_owned(),
                component,
                version,
                release_identity,
                executable_sha256: peer.executable_sha256.clone(),
                release_signature,
                peer_uid: peer.uid,
                executable_owned_by_root: peer.owned_by_root,
                installation_path_read_only: peer.installation_path_read_only,
            };
            let lease = broker.acquire_lease(
                &client,
                capability,
                Duration::from_secs(ttl_seconds),
                now,
            )?;
            Ok(BrokerResponse::Lease {
                lease_id: lease.lease_id,
                epoch_id: lease.epoch_id,
                capability: lease.capability,
                expires_unix_ms: lease.expires_unix_ms,
                key_version: lease.key_version,
                capsule_generation: lease.capsule_generation,
                key: BASE64.encode(lease.key.as_slice()),
            })
        }
        BrokerRequest::ReleaseLease { lease_id } => {
            broker.release_lease(&lease_id, connection_id)?;
            Ok(BrokerResponse::Ok)
        }
        BrokerRequest::Lock => {
            broker.lock();
            Ok(BrokerResponse::Ok)
        }
    }
}

fn inspect_peer(stream: &UnixStream) -> Result<PeerProcess> {
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

#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;

fn trusted_installation_path(executable: &Path) -> Result<bool> {
    let canonical = executable.canonicalize()?;
    for path in canonical.ancestors() {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink()
            || metadata.uid() != 0
            || metadata.mode() & 0o022 != 0
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn require_private_regular_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect private file {}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.mode() & 0o077 != 0
    {
        bail!("{} must be a non-symlink regular file with mode 0600 or stricter", path.display());
    }
    Ok(())
}

fn prepare_socket_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("broker socket has no parent directory")?;
    fs::create_dir_all(parent)?;
    set_mode(parent, 0o700)
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

async fn remove_stale_socket(path: &Path) -> Result<()> {
    match UnixStream::connect(path).await {
        Ok(_) => bail!("broker socket {} is already active", path.display()),
        Err(_) if path.exists() => {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.file_type().is_socket() {
                bail!("refusing to replace non-socket path {}", path.display());
            }
            fs::remove_file(path)?;
        }
        Err(_) => {}
    }
    Ok(())
}

fn random_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    BASE64.encode(bytes)
}

fn lease_challenge_response(challenge: &str, executable_sha256: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!(
            "vaultic-broker-lease-challenge-v1\0{PROTOCOL_VERSION}\0{challenge}\0{executable_sha256}"
        ))
    )
}

fn disable_core_dumps() {
    #[cfg(unix)]
    unsafe {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        libc::setrlimit(libc::RLIMIT_CORE, &limit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier};
    use std::ffi::OsString;

    #[test]
    fn provisioning_creates_pinned_identity_and_verifiable_release() {
        let root = env::temp_dir().join(format!(
            "vaultic-broker-provisioning-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let identity_private = root.join("identity.key");
        let identity_public = root.join("identity.pub");
        identity_init(&[
            identity_private.clone().into_os_string(),
            identity_public.clone().into_os_string(),
        ])
        .unwrap();
        assert_eq!(fs::read(&identity_private).unwrap().len(), 32);
        assert_eq!(
            BASE64
                .decode(String::from_utf8(fs::read(&identity_public).unwrap()).unwrap().trim())
                .unwrap()
                .len(),
            32
        );

        let release_private = root.join("release.key");
        let release_key = SigningKey::from_bytes(&[9; 32]);
        write_new_file(&release_private, release_key.as_bytes(), 0o600).unwrap();
        let executable = env::current_exe().unwrap();
        let manifest_path = root.join("release.json");
        release_sign(&[
            release_private.into_os_string(),
            executable.clone().into_os_string(),
            OsString::from("vaulticdb"),
            OsString::from("7"),
            OsString::from("release-a"),
            manifest_path.clone().into_os_string(),
        ])
        .unwrap();
        let manifest: vaulticdb::broker::ReleaseManifest =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
        assert_eq!(manifest.executable_sha256, format!("{:x}", Sha256::digest(fs::read(executable).unwrap())));
        let message = serde_json::to_vec(&(
            "vaultic-client-release-v1",
            &manifest.component,
            manifest.version,
            &manifest.executable_sha256,
            &manifest.release_identity,
        ))
        .unwrap();
        let signature = Signature::from_slice(&BASE64.decode(manifest.signature).unwrap()).unwrap();
        release_key.verifying_key().verify(&message, &signature).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lease_challenge_is_protocol_and_executable_bound() {
        let challenge = "challenge-a";
        let digest = "ab".repeat(32);
        let response = lease_challenge_response(challenge, &digest);
        assert_eq!(response.len(), 64);
        assert_eq!(response, lease_challenge_response(challenge, &digest));
        assert_ne!(response, lease_challenge_response("challenge-b", &digest));
        assert_ne!(response, lease_challenge_response(challenge, &"cd".repeat(32)));
    }

    #[test]
    fn connection_protocol_consumes_lease_challenge_once() {
        let mut protocol = ConnectionProtocol {
            negotiated: true,
            lease_challenge: Some("challenge-a".to_owned()),
        };
        assert_eq!(protocol.lease_challenge.take().as_deref(), Some("challenge-a"));
        assert!(protocol.lease_challenge.take().is_none());
    }
}