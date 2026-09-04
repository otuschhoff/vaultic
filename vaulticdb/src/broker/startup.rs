use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs,
    io::Write,
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signer, SigningKey};
use rand08::rngs::OsRng as LegacyOsRng;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::{net::UnixStream, sync::Mutex};
use zeroize::Zeroizing;

use crate::encryption::recovery_capsule::{discover_latest, RecoveryCapsule};

use super::{audit::emit_security_event, Capability, ClientAuthorization, KeyBroker};

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
    #[serde(default)]
    identity_recovery: bool,
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

pub struct BrokerStartup {
    pub broker: Arc<Mutex<KeyBroker>>,
    pub socket_path: PathBuf,
    pub endpoint_binding: String,
}

pub fn load(config_path: &Path) -> Result<BrokerStartup> {
    require_private_regular_file(config_path)?;
    let config: BrokerConfig =
        serde_json::from_slice(&fs::read(config_path)?).context("decode broker config")?;
    if config.format != 1 || config.authorizations.is_empty() {
        bail!("unsupported broker config or empty authorization policy");
    }
    require_private_regular_file(&config.identity_key_path)?;
    let (_, capsule): (_, RecoveryCapsule) =
        discover_latest(&config.capsule_directory, &config.repository_id)?
            .context("no recovery capsule generation found")?;
    emit_security_event(
        "notice",
        "integrity",
        "capsule_discovered",
        &[
            ("repository_id", config.repository_id.clone()),
            ("capsule_generation", capsule.header.generation.to_string()),
            ("capsule_logical_id", capsule.header.logical_id.clone()),
        ],
    );
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
    let broker = if config.identity_recovery {
        KeyBroker::new_identity_recovery(capsule, identity, authorizations, maximum_lifetime)?
    } else {
        KeyBroker::new(capsule, identity, authorizations, maximum_lifetime)?
    };
    if config.identity_recovery {
        emit_security_event(
            "critical",
            "auth",
            "broker_identity_recovery_mode_active",
            &[("identity_recovery", "true".to_owned())],
        );
    }
    prepare_socket_parent(&config.socket_path)?;
    let endpoint_binding = format!("unix:{}", config.socket_path.display());
    Ok(BrokerStartup {
        broker: Arc::new(Mutex::new(broker)),
        socket_path: config.socket_path,
        endpoint_binding,
    })
}

pub fn identity_init(arguments: &[OsString]) -> Result<()> {
    if arguments.len() != 2 {
        bail!("usage: vaultic-key-broker identity-init PRIVATE PUBLIC");
    }
    let private_path = PathBuf::from(&arguments[0]);
    let public_path = PathBuf::from(&arguments[1]);
    let identity = SigningKey::generate(&mut LegacyOsRng);
    write_new_file(&private_path, identity.as_bytes(), 0o600)?;
    let mut public = BASE64
        .encode(identity.verifying_key().as_bytes())
        .into_bytes();
    public.push(b'\n');
    if let Err(error) = write_new_file(&public_path, &public, 0o644) {
        let _ = fs::remove_file(private_path);
        return Err(error);
    }
    Ok(())
}

pub fn release_sign(arguments: &[OsString]) -> Result<()> {
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
    let mut private = Zeroizing::new(fs::read(&private_path)?);
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

pub fn write_new_file(path: &Path, contents: &[u8], mode: u32) -> Result<()> {
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

pub fn require_private_regular_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect private file {}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.mode() & 0o077 != 0
    {
        bail!(
            "{} must be a non-symlink regular file with mode 0600 or stricter",
            path.display()
        );
    }
    Ok(())
}

fn prepare_socket_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("broker socket has no parent directory")?;
    fs::create_dir_all(parent)?;
    set_mode(parent, 0o700)
}

pub fn set_mode(path: &Path, mode: u32) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

pub async fn remove_stale_socket(path: &Path) -> Result<()> {
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

pub fn disable_core_dumps() {
    #[cfg(unix)]
    unsafe {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        libc::setrlimit(libc::RLIMIT_CORE, &limit);
    }
}
