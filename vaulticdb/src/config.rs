use std::{
    collections::HashMap,
    env,
    fs::File,
    io::Read,
    net::SocketAddr,
    os::fd::{FromRawFd, RawFd},
    path::PathBuf,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use ipnet::IpNet;
use zeroize::Zeroizing;

use crate::storage::{
    BrokerLeaseConfig, ObjectStoreConfig, ReplicaConfig, ReplicaStoreConfig, StorageConfig,
};
use vaulticdb::encryption::envelope::{EncryptionConfig, EncryptionMode, ProviderCredentials};

const DEFAULT_TRANSACTION_IDLE_TIMEOUT_SECS: u64 = 300;

#[derive(Debug)]
pub(crate) enum TransportConfig {
    Unix(PathBuf),
    Tcp {
        address: SocketAddr,
        allowlist: Vec<IpNet>,
        metadata_path: PathBuf,
    },
}

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) repository_id: String,
    pub(crate) daemon_id: String,
    pub(crate) auth_token: Option<Zeroizing<String>>,
    pub(crate) transport: TransportConfig,
    pub(crate) minimum_writer_tenure: Duration,
    pub(crate) writer_idle_grace: Option<Duration>,
    pub(crate) writer_transition_timeout: Duration,
    pub(crate) storage: StorageConfig,
}

impl Config {
    pub(crate) fn native_smoke_requested() -> bool {
        env::var_os("VAULTICDB_NATIVE_SMOKE").is_some()
    }

    pub(crate) fn from_env() -> Result<Self> {
        let auth_token = read_auth_token()?;
        let repository_id = env::var("VAULTICDB_REPOSITORY_ID").unwrap_or_default();
        let runtime_dir =
            env::var("VAULTICDB_RUNTIME_DIR").unwrap_or_else(|_| "/tmp/vaulticdb".to_owned());
        let transport = transport_from_env(&repository_id, &runtime_dir, auth_token.is_some())?;
        let storage = storage_from_env()?;
        Ok(Self {
            repository_id,
            daemon_id: env::var("VAULTICDB_DAEMON_ID")
                .unwrap_or_else(|_| "vaulticdb-dev".to_owned()),
            auth_token,
            transport,
            minimum_writer_tenure: configured_duration(
                "VAULTICDB_WRITER_MINIMUM_TENURE",
                Duration::from_secs(30),
                false,
            )?
            .expect("minimum writer tenure has a default"),
            writer_idle_grace: configured_duration(
                "VAULTICDB_WRITER_IDLE_GRACE",
                Duration::ZERO,
                true,
            )?,
            writer_transition_timeout: configured_duration(
                "VAULTICDB_WRITER_TRANSITION_TIMEOUT",
                Duration::from_secs(30),
                false,
            )?
            .expect("writer transition timeout has a default"),
            storage,
        })
    }
}

fn storage_from_env() -> Result<StorageConfig> {
    let object_store = object_store_from_env()?;
    let fencing_replica = match &object_store {
        ObjectStoreConfig::Replicated { .. } => Some(
            env::var("VAULTICDB_FENCING_REPLICA")
                .context("replicated metadata requires VAULTICDB_FENCING_REPLICA")?,
        ),
        _ => None,
    };
    let metadata_rebuild_initialize = env_bool("VAULTICDB_METADATA_REBUILD_INITIALIZE")?;
    let broker = match env::var_os("VAULTICDB_BROKER_SOCKET") {
        Some(socket) => Some(BrokerLeaseConfig {
            socket: PathBuf::from(socket),
            release_manifest: PathBuf::from(
                env::var_os("VAULTICDB_RELEASE_MANIFEST").context(
                    "VAULTICDB_RELEASE_MANIFEST is required with VAULTICDB_BROKER_SOCKET",
                )?,
            ),
            lease_duration: Duration::from_secs(parse_u64("VAULTICDB_BROKER_LEASE_SECONDS", 3600)?),
        }),
        None => None,
    };
    if metadata_rebuild_initialize && broker.is_none() {
        bail!("metadata rebuild initialization requires a broker metadata-DEK lease");
    }
    let transaction_idle_timeout_seconds = parse_u64(
        "VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS",
        DEFAULT_TRANSACTION_IDLE_TIMEOUT_SECS,
    )?;
    if transaction_idle_timeout_seconds < 10 {
        bail!("VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS must be at least 10");
    }
    let transaction_idle_timeout_ms = transaction_idle_timeout_seconds
        .checked_mul(1_000)
        .context("VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS is too large")?;
    Ok(StorageConfig {
        object_store,
        fencing_replica,
        metadata_rebuild_initialize,
        broker,
        encryption: encryption_from_env()?,
        transaction_idle_timeout_ms,
    })
}

fn object_store_from_env() -> Result<ObjectStoreConfig> {
    match env::var("VAULTICDB_OBJECT_STORE")
        .unwrap_or_else(|_| "local".to_owned())
        .as_str()
    {
        "local" => Ok(ObjectStoreConfig::Local {
            root: env::var_os("VAULTICDB_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| env::temp_dir().join("vaulticdb").join("data")),
        }),
        "memory" => Ok(ObjectStoreConfig::Memory),
        "s3" => Ok(ObjectStoreConfig::S3 {
            bucket: env::var("VAULTICDB_S3_BUCKET")
                .context("VAULTICDB_S3_BUCKET is required for S3 storage")?,
            prefix: optional_nonempty("VAULTICDB_S3_PREFIX")?,
        }),
        "replicated" => {
            let replicas = env::var("VAULTICDB_REPLICATED_REPLICAS")
                .context("VAULTICDB_REPLICATED_REPLICAS is required for replicated storage")?
                .split(',')
                .map(str::trim)
                .map(replica_from_env)
                .collect::<Result<Vec<_>>>()?;
            if replicas.is_empty() {
                bail!("VAULTICDB_REPLICATED_REPLICAS must not be empty");
            }
            Ok(ObjectStoreConfig::Replicated { replicas })
        }
        value => bail!(
            "unsupported VAULTICDB_OBJECT_STORE {value:?}; expected local, memory, s3, or replicated"
        ),
    }
}

fn replica_from_env(id: &str) -> Result<ReplicaConfig> {
    if id.is_empty() {
        bail!("VAULTICDB_REPLICATED_REPLICAS contains an empty replica ID");
    }
    let prefix = format!("VAULTICDB_REPLICATED_{}", env_id(id));
    let store = match env::var(format!("{prefix}_OBJECT_STORE"))
        .with_context(|| format!("{prefix}_OBJECT_STORE is required"))?
        .as_str()
    {
        "local" => ReplicaStoreConfig::Local {
            root: PathBuf::from(env::var_os(format!("{prefix}_DATA_DIR")).with_context(|| {
                format!("{prefix}_DATA_DIR is required for local replica {id}")
            })?),
        },
        "memory" => ReplicaStoreConfig::Memory,
        "s3" => ReplicaStoreConfig::S3 {
            bucket: env::var(format!("{prefix}_S3_BUCKET"))
                .with_context(|| format!("{prefix}_S3_BUCKET is required for S3 replica {id}"))?,
            prefix: optional_nonempty_dynamic(&format!("{prefix}_S3_PREFIX"))?,
        },
        "azure" => ReplicaStoreConfig::Azure {
            account: env::var(format!("{prefix}_AZURE_ACCOUNT")).with_context(|| {
                format!("{prefix}_AZURE_ACCOUNT is required for Azure replica {id}")
            })?,
            container: env::var(format!("{prefix}_AZURE_CONTAINER")).with_context(|| {
                format!("{prefix}_AZURE_CONTAINER is required for Azure replica {id}")
            })?,
            prefix: optional_nonempty_dynamic(&format!("{prefix}_AZURE_PREFIX"))?,
            access_key: env::var(format!("{prefix}_AZURE_ACCESS_KEY")).ok(),
            bearer_token: env::var(format!("{prefix}_AZURE_BEARER_TOKEN")).ok(),
        },
        value => bail!(
            "unsupported {prefix}_OBJECT_STORE {value:?}; expected local, memory, s3, or azure"
        ),
    };
    Ok(ReplicaConfig {
        id: id.to_owned(),
        store,
    })
}

fn encryption_from_env() -> Result<EncryptionConfig> {
    let mode = match env::var("VAULTICDB_ENCRYPTION")
        .unwrap_or_else(|_| "off".to_owned())
        .as_str()
    {
        "off" => EncryptionMode::Off,
        "required" => EncryptionMode::Required,
        "initialize" => EncryptionMode::Initialize,
        value => bail!(
            "unsupported VAULTICDB_ENCRYPTION {value:?}; expected off, required, or initialize"
        ),
    };
    let passphrase_file = env::var_os("VAULTICDB_ENCRYPTION_PASSPHRASE_FILE").map(PathBuf::from);
    let mut token_files = HashMap::new();
    for (provider, variable) in [
        ("azure-key-vault", "VAULTICDB_AZURE_TOKEN_FILE"),
        ("gcp-kms", "VAULTICDB_GCP_TOKEN_FILE"),
        ("vault-transit", "VAULTICDB_VAULT_TOKEN_FILE"),
        ("pkcs11", "VAULTICDB_PKCS11_PIN_FILE"),
        ("yubikey-piv", "VAULTICDB_YUBIKEY_PIV_PIN_FILE"),
        ("fido2-hmac-secret", "VAULTICDB_FIDO2_SECRET_FILE"),
    ] {
        if let Some(path) = env::var_os(variable) {
            token_files.insert(provider.to_owned(), PathBuf::from(path));
        }
    }
    Ok(EncryptionConfig {
        mode,
        passphrase_file,
        recovery_acknowledged: env_bool("VAULTICDB_ENCRYPTION_RECOVERY_ACK")?,
        provider_credentials: ProviderCredentials::new(token_files),
    })
}

fn optional_nonempty(name: &str) -> Result<Option<String>> {
    optional_nonempty_dynamic(name)
}

fn optional_nonempty_dynamic(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim_matches('/').is_empty() => Ok(Some(value)),
        Ok(_) => bail!("{name} must not be empty"),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn parse_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("invalid {name}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_bool(name: &str) -> Result<bool> {
    match env::var(name) {
        Ok(value) => Ok(value == "true"),
        Err(env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn configured_duration(
    name: &str,
    default: Duration,
    allow_disabled: bool,
) -> Result<Option<Duration>> {
    let Ok(value) = env::var(name) else {
        return Ok((!allow_disabled || !default.is_zero()).then_some(default));
    };
    let value = value.trim();
    if allow_disabled && (value.is_empty() || value == "0" || value.eq_ignore_ascii_case("off")) {
        return Ok(None);
    }
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        bail!("{name} must use an ms, s, m, or h suffix")
    };
    let milliseconds = number
        .parse::<u64>()
        .with_context(|| format!("parse {name}"))?
        .checked_mul(multiplier)
        .with_context(|| format!("{name} is too large"))?;
    if milliseconds == 0 {
        bail!("{name} must be positive or explicitly disabled")
    }
    Ok(Some(Duration::from_millis(milliseconds)))
}

fn env_id(id: &str) -> String {
    id.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn read_auth_token() -> Result<Option<Zeroizing<String>>> {
    let Some(descriptor) = env::var_os("VAULTICDB_TCP_AUTH_TOKEN_FD") else {
        return Ok(None);
    };
    unsafe { env::remove_var("VAULTICDB_TCP_AUTH_TOKEN_FD") };
    let descriptor: RawFd = descriptor
        .to_string_lossy()
        .parse()
        .context("invalid TCP authentication-token descriptor")?;
    if descriptor < 3 {
        bail!("TCP authentication-token descriptor must not be a standard stream")
    }
    let mut input = unsafe { File::from_raw_fd(descriptor) }.take(64 * 1024 + 1);
    let mut token = Zeroizing::new(String::new());
    input
        .read_to_string(&mut token)
        .context("read TCP authentication token")?;
    if token.is_empty() || token.len() > 64 * 1024 {
        bail!("TCP authentication token must contain between 1 and 65536 bytes")
    }
    Ok(Some(token))
}

pub(crate) fn transport_from_env(
    repository_id: &str,
    runtime_dir: &str,
    has_auth_token: bool,
) -> Result<TransportConfig> {
    match env::var("VAULTICDB_TRANSPORT")
        .unwrap_or_else(|_| "unix".to_owned())
        .as_str()
    {
        "unix" => Ok(TransportConfig::Unix(PathBuf::from(
            env::var("VAULTICDB_SOCKET")
                .unwrap_or_else(|_| default_socket_path(runtime_dir, repository_id)),
        ))),
        "tcp" => {
            let raw_allowlist = env::var("VAULTICDB_TCP_ALLOWLIST").unwrap_or_default();
            if raw_allowlist.trim().is_empty() {
                bail!("VAULTICDB_TCP_ALLOWLIST is required when TCP transport is enabled")
            }
            if !has_auth_token {
                bail!("a TCP authentication token is required when TCP transport is enabled")
            }
            let address = env::var("VAULTICDB_TCP_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:50051".to_owned())
                .parse()
                .context("invalid VAULTICDB_TCP_ADDR")?;
            let allowlist = raw_allowlist
                .split(',')
                .map(|value| value.trim().parse().context("invalid IP allowlist entry"))
                .collect::<Result<Vec<IpNet>>>()?;
            let metadata_path = env::var("VAULTICDB_TCP_METADATA")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(runtime_dir).join("vaulticdb-tcp"));
            Ok(TransportConfig::Tcp {
                address,
                allowlist,
                metadata_path,
            })
        }
        other => bail!("unsupported VAULTICDB_TRANSPORT {other:?}; expected unix or tcp"),
    }
}

pub(crate) fn default_socket_path(runtime_dir: &str, repository_id: &str) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(if repository_id.is_empty() {
        b"default"
    } else {
        repository_id.as_bytes()
    });
    format!("{runtime_dir}/{digest:x}.sock")
}
