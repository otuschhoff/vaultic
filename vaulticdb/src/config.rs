//! Environment-backed daemon, transport, storage, and encryption configuration.

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
    TopologySource, WalStoreConfig,
};
use vaulticdb::encryption::envelope::{EncryptionConfig, EncryptionMode, ProviderCredentials};
use vaulticdb::ids::RepositoryId;

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
    pub(crate) repository_id: RepositoryId,
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
        let repository_id =
            RepositoryId::new(env::var("VAULTICDB_REPOSITORY_ID").unwrap_or_default());
        let runtime_dir = env::var("VAULTICDB_RUNTIME_DIR")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(default_runtime_directory);
        let transport =
            transport_from_env(repository_id.as_str(), &runtime_dir, auth_token.is_some())?;
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
            .context("minimum writer tenure must be enabled")?,
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
            .context("writer transition timeout must be enabled")?,
            storage,
        })
    }
}

fn storage_from_env() -> Result<StorageConfig> {
    let metadata_rebuild_initialize = env_bool("VAULTICDB_METADATA_REBUILD_INITIALIZE")?;
    let broker = match env::var_os("VAULTICDB_BROKER_SOCKET") {
        Some(socket) => {
            let storage_token_ttl = configured_duration(
                "VAULTICDB_STORAGE_TOKEN_TTL",
                Duration::from_secs(3600),
                false,
            )?
            .context("storage token TTL must be enabled")?;
            if storage_token_ttl > Duration::from_secs(3600) {
                bail!("VAULTICDB_STORAGE_TOKEN_TTL must not exceed 1h");
            }
            let default_margin = std::cmp::max(Duration::from_secs(1200), storage_token_ttl / 3);
            let storage_token_renew_margin = configured_duration(
                "VAULTICDB_STORAGE_TOKEN_RENEW_MARGIN",
                default_margin,
                false,
            )?
            .context("storage token renewal margin must be enabled")?;
            if storage_token_renew_margin >= storage_token_ttl {
                bail!("VAULTICDB_STORAGE_TOKEN_RENEW_MARGIN must be less than the token TTL");
            }
            let broker_outage_grace =
                configured_duration("VAULTICDB_BROKER_OUTAGE_GRACE", storage_token_ttl, false)?
                    .context("broker outage grace must be enabled")?;
            Some(BrokerLeaseConfig {
                socket: PathBuf::from(socket),
                release_manifest: PathBuf::from(
                    env::var_os("VAULTICDB_RELEASE_MANIFEST").context(
                        "VAULTICDB_RELEASE_MANIFEST is required with VAULTICDB_BROKER_SOCKET",
                    )?,
                ),
                lease_duration: Duration::from_secs(parse_u64(
                    "VAULTICDB_BROKER_LEASE_SECONDS",
                    3600,
                )?),
                storage_token_ttl,
                storage_token_renew_margin,
                broker_outage_grace,
            })
        }
        None => None,
    };
    let topology_source = match env::var("VAULTICDB_TOPOLOGY_SOURCE").ok().as_deref() {
        Some("capsule") => TopologySource::Capsule,
        Some("external") => TopologySource::External,
        Some(value) => {
            bail!("unsupported VAULTICDB_TOPOLOGY_SOURCE {value:?}; expected capsule or external")
        }
        None if broker.is_some() => TopologySource::Capsule,
        None => {
            bail!("VAULTICDB_TOPOLOGY_SOURCE=external is required for environment-backed topology")
        }
    };
    if topology_source == TopologySource::Capsule && broker.is_none() {
        bail!("capsule topology requires VAULTICDB_BROKER_SOCKET");
    }
    let topology_override_local = match env::var("VAULTICDB_TOPOLOGY_OVERRIDE").ok() {
        Some(value) => {
            let (selector, path) = value
                .split_once('=')
                .context("VAULTICDB_TOPOLOGY_OVERRIDE must be ID.data_dir=PATH")?;
            let id = selector
                .strip_suffix(".data_dir")
                .filter(|id| !id.is_empty())
                .context("only ID.data_dir topology overrides are supported")?;
            if path.is_empty() {
                bail!("topology override data directory must not be empty");
            }
            Some((id.to_owned(), PathBuf::from(path)))
        }
        None => None,
    };
    if topology_override_local.is_some() && topology_source != TopologySource::Capsule {
        bail!("topology overrides require capsule topology");
    }
    let object_store = if topology_source == TopologySource::Capsule {
        ObjectStoreConfig::Memory
    } else {
        object_store_from_env()?
    };
    let fencing_replica = match &object_store {
        ObjectStoreConfig::Replicated { .. } => Some(
            env::var("VAULTICDB_FENCING_REPLICA")
                .context("replicated metadata requires VAULTICDB_FENCING_REPLICA")?,
        ),
        _ => None,
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
        wal_store: wal_store_from_env()?,
        fencing_replica,
        metadata_rebuild_initialize,
        broker,
        encryption: encryption_from_env()?,
        transaction_idle_timeout_ms,
        topology_source,
        topology_override_local,
    })
}

fn wal_store_from_env() -> Result<WalStoreConfig> {
    let kind = env::var("VAULTICDB_WAL_STORE").unwrap_or_else(|_| "inherit".to_owned());
    let store = match kind.as_str() {
        "inherit" => return Ok(WalStoreConfig::Inherit),
        "local" => ReplicaStoreConfig::Local {
            root: env::var_os("VAULTICDB_WAL_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| env::temp_dir().join("vaulticdb").join("wal")),
        },
        "memory" => ReplicaStoreConfig::Memory,
        "s3" => ReplicaStoreConfig::S3 {
            bucket: env::var("VAULTICDB_WAL_S3_BUCKET")
                .context("VAULTICDB_WAL_S3_BUCKET is required for S3 WAL storage")?,
            prefix: optional_nonempty("VAULTICDB_WAL_S3_PREFIX")?,
            endpoint: optional_nonempty("VAULTICDB_WAL_S3_ENDPOINT")?,
            region: optional_nonempty("VAULTICDB_WAL_S3_REGION")?,
            provider: optional_nonempty("VAULTICDB_WAL_S3_PROVIDER")?,
            bucket_lookup: optional_nonempty("VAULTICDB_WAL_S3_BUCKET_LOOKUP")?,
            access_key_id: env::var("VAULTICDB_WAL_S3_ACCESS_KEY_ID")
                .ok()
                .map(Zeroizing::new),
            secret_access_key: env::var("VAULTICDB_WAL_S3_SECRET_ACCESS_KEY")
                .ok()
                .map(Zeroizing::new),
            session_token: env::var("VAULTICDB_WAL_S3_SESSION_TOKEN")
                .ok()
                .map(Zeroizing::new),
        },
        "rados" => ReplicaStoreConfig::Rados {
            monitors: env::var("VAULTICDB_WAL_RADOS_MONITORS")
                .context("VAULTICDB_WAL_RADOS_MONITORS is required for RADOS WAL storage")?,
            cluster_fsid: env::var("VAULTICDB_WAL_RADOS_CLUSTER_FSID")
                .context("VAULTICDB_WAL_RADOS_CLUSTER_FSID is required for RADOS WAL storage")?,
            pool: env::var("VAULTICDB_WAL_RADOS_POOL")
                .context("VAULTICDB_WAL_RADOS_POOL is required for RADOS WAL storage")?,
            namespace: env::var("VAULTICDB_WAL_RADOS_NAMESPACE")
                .context("VAULTICDB_WAL_RADOS_NAMESPACE is required for RADOS WAL storage")?,
            prefix: env::var("VAULTICDB_WAL_RADOS_PREFIX")
                .context("VAULTICDB_WAL_RADOS_PREFIX is required for RADOS WAL storage")?,
            client: env::var("VAULTICDB_WAL_RADOS_CLIENT")
                .context("VAULTICDB_WAL_RADOS_CLIENT is required for RADOS WAL storage")?,
            key: Zeroizing::new(
                env::var("VAULTICDB_WAL_RADOS_KEY")
                    .context("VAULTICDB_WAL_RADOS_KEY is required for RADOS WAL storage")?,
            ),
        },
        value => bail!(
            "unsupported VAULTICDB_WAL_STORE {value:?}; expected inherit, local, memory, s3, or rados"
        ),
    };
    Ok(WalStoreConfig::Store(store))
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
            endpoint: optional_nonempty("VAULTICDB_S3_ENDPOINT")?,
            region: optional_nonempty("VAULTICDB_S3_REGION")?,
            provider: optional_nonempty("VAULTICDB_S3_PROVIDER")?,
            bucket_lookup: optional_nonempty("VAULTICDB_S3_BUCKET_LOOKUP")?,
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
            endpoint: optional_nonempty_dynamic(&format!("{prefix}_S3_ENDPOINT"))?,
            region: optional_nonempty_dynamic(&format!("{prefix}_S3_REGION"))?,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            provider: optional_nonempty_dynamic(&format!("{prefix}_S3_PROVIDER"))?,
            bucket_lookup: optional_nonempty_dynamic(&format!("{prefix}_S3_BUCKET_LOOKUP"))?,
        },
        "azure" => {
            let account = env::var(format!("{prefix}_AZURE_ACCOUNT")).with_context(|| {
                format!("{prefix}_AZURE_ACCOUNT is required for Azure replica {id}")
            })?;
            ReplicaStoreConfig::Azure {
                endpoint: optional_nonempty_dynamic(&format!("{prefix}_AZURE_ENDPOINT"))?
                    .unwrap_or_else(|| format!("https://{account}.blob.core.windows.net")),
                account,
                container: env::var(format!("{prefix}_AZURE_CONTAINER")).with_context(|| {
                    format!("{prefix}_AZURE_CONTAINER is required for Azure replica {id}")
                })?,
                prefix: optional_nonempty_dynamic(&format!("{prefix}_AZURE_PREFIX"))?,
                access_key: env::var(format!("{prefix}_AZURE_ACCESS_KEY"))
                    .ok()
                    .map(Zeroizing::new),
                bearer_token: env::var(format!("{prefix}_AZURE_BEARER_TOKEN"))
                    .ok()
                    .map(Zeroizing::new),
                sas_token: env::var(format!("{prefix}_AZURE_SAS_TOKEN"))
                    .ok()
                    .map(Zeroizing::new),
            }
        }
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

fn default_runtime_directory() -> String {
    if let Ok(runtime_dir) = env::var("XDG_RUNTIME_DIR") {
        if !runtime_dir.is_empty() {
            return PathBuf::from(runtime_dir)
                .join("vaulticdb")
                .to_string_lossy()
                .into_owned();
        }
    }
    PathBuf::from("/tmp")
        .join(format!("vaulticdb-{}", unsafe { libc::geteuid() }))
        .to_string_lossy()
        .into_owned()
}
