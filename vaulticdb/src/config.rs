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
    cache::{
        CacheConfidentiality, CacheConfig, CacheTierConfig, CacheTierPolicy, DEFAULT_CACHE_TIMEOUT,
    },
    BrokerLeaseConfig, ObjectStoreConfig, ReplicaConfig, ReplicaStoreConfig, SlateDbTuning,
    StorageConfig, TopologySource, WalStoreConfig,
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
    let metadata_rebuild_reset = env_bool("VAULTICDB_METADATA_REBUILD_RESET")?;
    let slatedb_multiget = optional_bool("VAULTICDB_SLATEDB_MULTIGET", false)?;
    let attribution_disabled = attribution_disabled_from_env()?;
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
        slatedb_tuning: slatedb_tuning_from_env()?,
        cache: cache_from_env()?,
        fencing_replica,
        metadata_rebuild_initialize,
        metadata_rebuild_reset,
        bulk_import_local_wal_data_dir: env::var_os("VAULTICDB_BULK_IMPORT_LOCAL_WAL_DATA_DIR")
            .map(PathBuf::from),
        broker,
        encryption: encryption_from_env()?,
        transaction_idle_timeout_ms,
        slatedb_multiget,
        attribution_disabled,
        topology_source,
        topology_override_local,
    })
}

fn attribution_disabled_from_env() -> Result<bool> {
    #[cfg(feature = "test-failpoints")]
    {
        if env::var("VAULTICDB_TEST_CAPABILITY").as_deref() != Ok("vaulticdb-process-tests-v1") {
            return Ok(false);
        }
        return optional_bool("VAULTICDB_TEST_ATTRIBUTION_DISABLED", false);
    }
    #[cfg(not(feature = "test-failpoints"))]
    {
        Ok(false)
    }
}

fn slatedb_tuning_from_env() -> Result<SlateDbTuning> {
    Ok(SlateDbTuning {
        flush_interval: match env::var_os("VAULTICDB_WAL_FLUSH_INTERVAL") {
            Some(_) => configured_duration("VAULTICDB_WAL_FLUSH_INTERVAL", Duration::ZERO, false)?,
            None => None,
        },
        max_unflushed_bytes: optional_positive_usize("VAULTICDB_MAX_UNFLUSHED_BYTES")?,
        l0_sst_size_bytes: optional_positive_usize("VAULTICDB_L0_SST_SIZE_BYTES")?,
        block_cache_bytes: optional_positive_usize("VAULTICDB_BLOCK_CACHE_BYTES")?,
        meta_cache_bytes: optional_positive_usize("VAULTICDB_META_CACHE_BYTES")?,
    })
}

fn cache_from_env() -> Result<CacheConfig> {
    let raw_tiers = env::var("VAULTICDB_READ_CACHE_TIERS").unwrap_or_default();
    if raw_tiers.trim().is_empty() {
        return Ok(CacheConfig::default());
    }
    let part_size_bytes = parse_u64(
        "VAULTICDB_READ_CACHE_PART_SIZE_BYTES",
        crate::storage::cache::DEFAULT_PART_SIZE_BYTES,
    )?;
    let max_inflight_bytes = parse_u64(
        "VAULTICDB_READ_CACHE_MAX_INFLIGHT_BYTES",
        part_size_bytes.saturating_mul(8),
    )?;
    let default_background_tasks = max_inflight_bytes.div_ceil(part_size_bytes).max(1);
    let max_background_tasks = parse_u64(
        "VAULTICDB_READ_CACHE_MAX_BACKGROUND_TASKS",
        default_background_tasks,
    )?
    .try_into()
    .context("VAULTICDB_READ_CACHE_MAX_BACKGROUND_TASKS exceeds platform limits")?;
    let aggregate_max_bytes = optional_u64("VAULTICDB_READ_CACHE_AGGREGATE_MAX_BYTES")?;
    let mut ids = std::collections::HashSet::new();
    let mut environment_ids = std::collections::HashSet::new();
    let tiers = raw_tiers
        .split(',')
        .map(str::trim)
        .map(|id| {
            if id.is_empty() {
                bail!("VAULTICDB_READ_CACHE_TIERS contains an empty tier ID");
            }
            if !ids.insert(id.to_owned()) {
                bail!("VAULTICDB_READ_CACHE_TIERS contains duplicate tier ID {id:?}");
            }
            let environment_id = env_id(id);
            if !environment_ids.insert(environment_id.clone()) {
                bail!("read-cache tier IDs must have distinct environment IDs");
            }
            cache_tier_from_env(id, &environment_id)
        })
        .collect::<Result<Vec<_>>>()?;
    let config = CacheConfig {
        tiers,
        aggregate_max_bytes,
        part_size_bytes,
        max_inflight_bytes,
        max_background_tasks,
    };
    config.validate()?;
    Ok(config)
}

fn cache_tier_from_env(id: &str, environment_id: &str) -> Result<CacheTierConfig> {
    let prefix = format!("VAULTICDB_READ_CACHE_{environment_id}");
    let confidentiality_name = format!("{prefix}_CONFIDENTIALITY");
    let confidentiality = match env::var(&confidentiality_name) {
        Ok(value) if value == "encrypted" => CacheConfidentiality::Encrypted,
        Err(env::VarError::NotPresent) => CacheConfidentiality::Encrypted,
        Ok(value) if value == "decrypted" => {
            let acknowledgement_name = format!("{prefix}_ACKNOWLEDGE_PLAINTEXT");
            if !optional_bool(&acknowledgement_name, false)? {
                bail!(
                    "{acknowledgement_name}=true is required for decrypted read-cache tier {id:?}"
                );
            }
            CacheConfidentiality::DecryptedHighlyTrusted
        }
        Ok(value) => {
            bail!("unsupported {confidentiality_name} {value:?}; expected encrypted or decrypted")
        }
        Err(error) => return Err(error.into()),
    };
    let store = match env::var(format!("{prefix}_OBJECT_STORE"))
        .with_context(|| format!("{prefix}_OBJECT_STORE is required"))?
        .as_str()
    {
        "local" => ReplicaStoreConfig::Local {
            root: PathBuf::from(env::var_os(format!("{prefix}_DATA_DIR")).with_context(|| {
                format!("{prefix}_DATA_DIR is required for local cache tier {id}")
            })?),
        },
        "memory" => ReplicaStoreConfig::Memory,
        "s3" => ReplicaStoreConfig::S3 {
            bucket: env::var(format!("{prefix}_S3_BUCKET"))
                .with_context(|| format!("{prefix}_S3_BUCKET is required for S3 cache tier {id}"))?,
            prefix: optional_nonempty_dynamic(&format!("{prefix}_S3_PREFIX"))?,
            endpoint: optional_nonempty_dynamic(&format!("{prefix}_S3_ENDPOINT"))?,
            region: optional_nonempty_dynamic(&format!("{prefix}_S3_REGION"))?,
            access_key_id: env::var(format!("{prefix}_S3_ACCESS_KEY_ID"))
                .ok()
                .map(Zeroizing::new),
            secret_access_key: env::var(format!("{prefix}_S3_SECRET_ACCESS_KEY"))
                .ok()
                .map(Zeroizing::new),
            session_token: match env::var(format!("{prefix}_S3_SESSION_TOKEN")) {
                Ok(_) => bail!(
                    "{prefix}_S3_SESSION_TOKEN is not supported for read-cache tiers: cache credentials are static and expiring session credentials cannot be renewed"
                ),
                Err(env::VarError::NotPresent) => None,
                Err(error) => return Err(error.into()),
            },
            provider: optional_nonempty_dynamic(&format!("{prefix}_S3_PROVIDER"))?,
            bucket_lookup: optional_nonempty_dynamic(&format!("{prefix}_S3_BUCKET_LOOKUP"))?,
        },
        "rados" => ReplicaStoreConfig::Rados {
            monitors: required_cache_value(&prefix, "RADOS_MONITORS", id)?,
            cluster_fsid: required_cache_value(&prefix, "RADOS_CLUSTER_FSID", id)?,
            pool: required_cache_value(&prefix, "RADOS_POOL", id)?,
            namespace: required_cache_value(&prefix, "RADOS_NAMESPACE", id)?,
            prefix: required_cache_value(&prefix, "RADOS_PREFIX", id)?,
            client: required_cache_value(&prefix, "RADOS_CLIENT", id)?,
            key: Zeroizing::new(required_cache_value(&prefix, "RADOS_KEY", id)?),
        },
        "azure" | "gcs" => bail!(
            "unsupported {prefix}_OBJECT_STORE for Phase 29 read cache; expected local, memory, s3, or rados"
        ),
        value => bail!(
            "unsupported {prefix}_OBJECT_STORE {value:?}; expected local, memory, s3, or rados"
        ),
    };
    Ok(CacheTierConfig {
        id: id.to_owned(),
        store,
        confidentiality,
        policy: CacheTierPolicy {
            enabled: optional_bool(&format!("{prefix}_ENABLED"), true)?,
            max_bytes: required_u64(&format!("{prefix}_MAX_BYTES"))?,
            idle_age_ms: configured_duration(&format!("{prefix}_IDLE_AGE"), Duration::ZERO, true)?
                .map_or(0, duration_ms),
            absolute_age_ms: configured_duration(
                &format!("{prefix}_ABSOLUTE_AGE"),
                Duration::ZERO,
                true,
            )?
            .map(duration_ms),
            read_priority: parse_u32(&format!("{prefix}_READ_PRIORITY"), 100)?,
            admission_priority: parse_u32(&format!("{prefix}_ADMISSION_PRIORITY"), 100)?,
            timeout_ms: duration_ms(
                configured_duration(&format!("{prefix}_TIMEOUT"), DEFAULT_CACHE_TIMEOUT, false)?
                    .context("read-cache timeout must be enabled")?,
            ),
        },
    })
}

fn required_cache_value(prefix: &str, suffix: &str, id: &str) -> Result<String> {
    let name = format!("{prefix}_{suffix}");
    env::var(&name).with_context(|| format!("{name} is required for RADOS cache tier {id}"))
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
        "rados" => Ok(ObjectStoreConfig::Rados {
            monitors: env::var("VAULTICDB_RADOS_MONITORS")
                .context("VAULTICDB_RADOS_MONITORS is required for RADOS storage")?,
            cluster_fsid: env::var("VAULTICDB_RADOS_CLUSTER_FSID")
                .context("VAULTICDB_RADOS_CLUSTER_FSID is required for RADOS storage")?,
            pool: env::var("VAULTICDB_RADOS_POOL")
                .context("VAULTICDB_RADOS_POOL is required for RADOS storage")?,
            namespace: env::var("VAULTICDB_RADOS_NAMESPACE")
                .context("VAULTICDB_RADOS_NAMESPACE is required for RADOS storage")?,
            prefix: env::var("VAULTICDB_RADOS_PREFIX")
                .context("VAULTICDB_RADOS_PREFIX is required for RADOS storage")?,
            client: env::var("VAULTICDB_RADOS_CLIENT")
                .context("VAULTICDB_RADOS_CLIENT is required for RADOS storage")?,
            key: Zeroizing::new(
                env::var("VAULTICDB_RADOS_KEY")
                    .context("VAULTICDB_RADOS_KEY is required for RADOS storage")?,
            ),
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
            "unsupported VAULTICDB_OBJECT_STORE {value:?}; expected local, memory, s3, rados, or replicated"
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

fn required_u64(name: &str) -> Result<u64> {
    env::var(name)
        .with_context(|| format!("{name} is required"))?
        .parse()
        .with_context(|| format!("invalid {name}"))
}

fn optional_u64(name: &str) -> Result<Option<u64>> {
    match env::var(name) {
        Ok(value) => Ok(Some(
            value.parse().with_context(|| format!("invalid {name}"))?,
        )),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn parse_u32(name: &str, default: u32) -> Result<u32> {
    match env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("invalid {name}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn optional_positive_usize(name: &str) -> Result<Option<usize>> {
    optional_u64(name)?
        .map(|value| {
            if value == 0 {
                bail!("{name} must be positive");
            }
            usize::try_from(value).with_context(|| format!("{name} exceeds platform limits"))
        })
        .transpose()
}

fn optional_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Ok(_) => bail!("{name} must be true or false"),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis() as u64
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn environment_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn slatedb_multiget_flag_is_strict_and_defaults_off() {
        let _guard = environment_lock().lock().unwrap();
        unsafe { env::remove_var("VAULTICDB_SLATEDB_MULTIGET") };
        assert!(!optional_bool("VAULTICDB_SLATEDB_MULTIGET", false).unwrap());
        unsafe { env::set_var("VAULTICDB_SLATEDB_MULTIGET", "true") };
        assert!(optional_bool("VAULTICDB_SLATEDB_MULTIGET", false).unwrap());
        unsafe { env::set_var("VAULTICDB_SLATEDB_MULTIGET", "yes") };
        assert!(optional_bool("VAULTICDB_SLATEDB_MULTIGET", false).is_err());
        unsafe { env::remove_var("VAULTICDB_SLATEDB_MULTIGET") };
    }

    #[test]
    fn parses_independent_main_and_wal_rados_stores() {
        let _guard = environment_lock().lock().unwrap();
        let values = [
            ("VAULTICDB_OBJECT_STORE", "rados"),
            ("VAULTICDB_RADOS_MONITORS", "mon-a:3300"),
            (
                "VAULTICDB_RADOS_CLUSTER_FSID",
                "2f525d6a-8f31-4f79-b731-82a6acb235f5",
            ),
            ("VAULTICDB_RADOS_POOL", "db-sst"),
            ("VAULTICDB_RADOS_NAMESPACE", "vaultic-perf"),
            ("VAULTICDB_RADOS_PREFIX", "main"),
            ("VAULTICDB_RADOS_CLIENT", "client.amakura"),
            ("VAULTICDB_RADOS_KEY", "main-key"),
            ("VAULTICDB_WAL_STORE", "rados"),
            ("VAULTICDB_WAL_RADOS_MONITORS", "mon-a:3300"),
            (
                "VAULTICDB_WAL_RADOS_CLUSTER_FSID",
                "2f525d6a-8f31-4f79-b731-82a6acb235f5",
            ),
            ("VAULTICDB_WAL_RADOS_POOL", "db-wal"),
            ("VAULTICDB_WAL_RADOS_NAMESPACE", "vaultic-perf"),
            ("VAULTICDB_WAL_RADOS_PREFIX", "wal"),
            ("VAULTICDB_WAL_RADOS_CLIENT", "client.amakura"),
            ("VAULTICDB_WAL_RADOS_KEY", "wal-key"),
        ];
        for (name, value) in values {
            unsafe { env::set_var(name, value) };
        }

        let main = object_store_from_env().unwrap();
        let wal = wal_store_from_env().unwrap();
        assert!(matches!(
            main,
            ObjectStoreConfig::Rados { pool, key, .. }
                if pool == "db-sst" && key.as_str() == "main-key"
        ));
        assert!(matches!(
            wal,
            WalStoreConfig::Store(ReplicaStoreConfig::Rados { pool, key, .. })
                if pool == "db-wal" && key.as_str() == "wal-key"
        ));

        for (name, _) in values {
            unsafe { env::remove_var(name) };
        }
    }

    #[cfg(feature = "test-failpoints")]
    #[test]
    fn attribution_disable_requires_process_test_capability() {
        let _guard = environment_lock().lock().unwrap();
        unsafe {
            env::remove_var("VAULTICDB_TEST_CAPABILITY");
            env::remove_var("VAULTICDB_TEST_ATTRIBUTION_DISABLED");
        }
        assert!(!attribution_disabled_from_env().unwrap());
        unsafe { env::set_var("VAULTICDB_TEST_ATTRIBUTION_DISABLED", "true") };
        assert!(!attribution_disabled_from_env().unwrap());
        unsafe {
            env::set_var("VAULTICDB_TEST_CAPABILITY", "vaulticdb-process-tests-v1");
        }
        assert!(attribution_disabled_from_env().unwrap());
        unsafe { env::set_var("VAULTICDB_TEST_ATTRIBUTION_DISABLED", "yes") };
        assert!(attribution_disabled_from_env().is_err());
        unsafe {
            env::remove_var("VAULTICDB_TEST_ATTRIBUTION_DISABLED");
            env::remove_var("VAULTICDB_TEST_CAPABILITY");
        }
    }

    #[cfg(not(feature = "test-failpoints"))]
    #[test]
    fn attribution_disable_is_compiled_out_without_test_failpoints() {
        let _guard = environment_lock().lock().unwrap();
        unsafe {
            env::set_var("VAULTICDB_TEST_CAPABILITY", "vaulticdb-process-tests-v1");
            env::set_var("VAULTICDB_TEST_ATTRIBUTION_DISABLED", "true");
        }
        assert!(!attribution_disabled_from_env().unwrap());
        unsafe {
            env::remove_var("VAULTICDB_TEST_ATTRIBUTION_DISABLED");
            env::remove_var("VAULTICDB_TEST_CAPABILITY");
        }
    }

    fn clear_slatedb_tuning_environment() {
        for name in [
            "VAULTICDB_WAL_FLUSH_INTERVAL",
            "VAULTICDB_MAX_UNFLUSHED_BYTES",
            "VAULTICDB_L0_SST_SIZE_BYTES",
            "VAULTICDB_BLOCK_CACHE_BYTES",
            "VAULTICDB_META_CACHE_BYTES",
        ] {
            unsafe { env::remove_var(name) };
        }
    }

    #[test]
    fn parses_optional_slatedb_tuning() {
        let _guard = environment_lock().lock().unwrap();
        clear_slatedb_tuning_environment();
        let defaults = slatedb_tuning_from_env().unwrap();
        assert_eq!(defaults.flush_interval, None);
        assert_eq!(defaults.max_unflushed_bytes, None);
        assert_eq!(defaults.l0_sst_size_bytes, None);
        assert_eq!(defaults.block_cache_bytes, None);
        assert_eq!(defaults.meta_cache_bytes, None);

        unsafe {
            env::set_var("VAULTICDB_META_CACHE_BYTES", "1073741824");
        }
        let metadata_only = slatedb_tuning_from_env().unwrap();
        assert_eq!(metadata_only.meta_cache_bytes, Some(1_073_741_824));
        assert_eq!(metadata_only.block_cache_bytes, None);
        assert_eq!(metadata_only.flush_interval, None);
        assert_eq!(metadata_only.max_unflushed_bytes, None);
        assert_eq!(metadata_only.l0_sst_size_bytes, None);
        clear_slatedb_tuning_environment();

        unsafe {
            env::set_var("VAULTICDB_WAL_FLUSH_INTERVAL", "500ms");
            env::set_var("VAULTICDB_MAX_UNFLUSHED_BYTES", "4294967296");
            env::set_var("VAULTICDB_L0_SST_SIZE_BYTES", "268435456");
            env::set_var("VAULTICDB_BLOCK_CACHE_BYTES", "8589934592");
            env::set_var("VAULTICDB_META_CACHE_BYTES", "1073741824");
        }
        let tuning = slatedb_tuning_from_env().unwrap();
        assert_eq!(tuning.flush_interval, Some(Duration::from_millis(500)));
        assert_eq!(tuning.max_unflushed_bytes, Some(4_294_967_296));
        assert_eq!(tuning.l0_sst_size_bytes, Some(268_435_456));
        assert_eq!(tuning.block_cache_bytes, Some(8_589_934_592));
        assert_eq!(tuning.meta_cache_bytes, Some(1_073_741_824));
        clear_slatedb_tuning_environment();
    }

    #[test]
    fn rejects_zero_slatedb_tuning_values() {
        let _guard = environment_lock().lock().unwrap();
        for name in [
            "VAULTICDB_WAL_FLUSH_INTERVAL",
            "VAULTICDB_MAX_UNFLUSHED_BYTES",
            "VAULTICDB_L0_SST_SIZE_BYTES",
            "VAULTICDB_BLOCK_CACHE_BYTES",
            "VAULTICDB_META_CACHE_BYTES",
        ] {
            clear_slatedb_tuning_environment();
            unsafe { env::set_var(name, "0") };
            assert!(slatedb_tuning_from_env().is_err(), "accepted zero {name}");
        }
        clear_slatedb_tuning_environment();
    }

    fn clear_cache_environment() {
        let names = env::vars()
            .map(|(name, _)| name)
            .filter(|name| name.starts_with("VAULTICDB_READ_CACHE_"))
            .collect::<Vec<_>>();
        for name in names {
            unsafe { env::remove_var(name) };
        }
    }

    #[test]
    fn empty_cache_tier_list_disables_cache() {
        let _guard = environment_lock().lock().unwrap();
        clear_cache_environment();
        unsafe { env::set_var("VAULTICDB_READ_CACHE_TIERS", "  ") };
        assert!(cache_from_env().unwrap().tiers.is_empty());
        clear_cache_environment();
    }

    #[test]
    fn parses_memory_and_s3_cache_tiers() {
        let _guard = environment_lock().lock().unwrap();
        clear_cache_environment();
        unsafe {
            env::set_var("VAULTICDB_READ_CACHE_TIERS", "ram,remote-east");
            env::set_var("VAULTICDB_READ_CACHE_RAM_OBJECT_STORE", "memory");
            env::set_var("VAULTICDB_READ_CACHE_RAM_MAX_BYTES", "1048576");
            env::set_var("VAULTICDB_READ_CACHE_RAM_IDLE_AGE", "5m");
            env::set_var("VAULTICDB_READ_CACHE_RAM_READ_PRIORITY", "2");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_EAST_OBJECT_STORE", "s3");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_EAST_S3_BUCKET", "cache");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_EAST_MAX_BYTES", "2097152");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_EAST_ABSOLUTE_AGE", "2h");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_EAST_TIMEOUT", "750ms");
            env::set_var("VAULTICDB_READ_CACHE_AGGREGATE_MAX_BYTES", "2500000");
            env::set_var("VAULTICDB_READ_CACHE_MAX_BACKGROUND_TASKS", "128");
        }
        let cache = cache_from_env().unwrap();
        assert_eq!(cache.tiers.len(), 2);
        assert_eq!(cache.aggregate_max_bytes, Some(2_500_000));
        assert_eq!(cache.max_background_tasks, 128);
        assert_eq!(
            cache.tiers[0].confidentiality,
            CacheConfidentiality::Encrypted
        );
        assert_eq!(cache.tiers[0].policy.idle_age_ms, 300_000);
        assert_eq!(cache.tiers[0].policy.read_priority, 2);
        assert_eq!(cache.tiers[1].policy.absolute_age_ms, Some(7_200_000));
        assert_eq!(cache.tiers[1].policy.timeout_ms, 750);
        clear_cache_environment();
    }

    #[test]
    fn derives_fresh_import_background_task_limit() {
        let _guard = environment_lock().lock().unwrap();
        clear_cache_environment();
        unsafe {
            env::set_var("VAULTICDB_READ_CACHE_TIERS", "ram");
            env::set_var("VAULTICDB_READ_CACHE_RAM_OBJECT_STORE", "memory");
            env::set_var("VAULTICDB_READ_CACHE_RAM_MAX_BYTES", "68719476736");
            env::set_var("VAULTICDB_READ_CACHE_PART_SIZE_BYTES", "16777216");
            env::set_var("VAULTICDB_READ_CACHE_MAX_INFLIGHT_BYTES", "268435456");
        }
        assert_eq!(cache_from_env().unwrap().max_background_tasks, 16);
        clear_cache_environment();
    }

    #[test]
    fn decrypted_cache_tier_requires_explicit_plaintext_acknowledgement() {
        let _guard = environment_lock().lock().unwrap();
        clear_cache_environment();
        unsafe {
            env::set_var("VAULTICDB_READ_CACHE_TIERS", "trusted");
            env::set_var("VAULTICDB_READ_CACHE_TRUSTED_OBJECT_STORE", "memory");
            env::set_var("VAULTICDB_READ_CACHE_TRUSTED_MAX_BYTES", "1048576");
            env::set_var("VAULTICDB_READ_CACHE_TRUSTED_CONFIDENTIALITY", "decrypted");
        }
        let error = cache_from_env().unwrap_err().to_string();
        assert!(error.contains("ACKNOWLEDGE_PLAINTEXT=true"));

        unsafe {
            env::set_var("VAULTICDB_READ_CACHE_TRUSTED_ACKNOWLEDGE_PLAINTEXT", "true");
        }
        let cache = cache_from_env().unwrap();
        assert_eq!(
            cache.tiers[0].confidentiality,
            CacheConfidentiality::DecryptedHighlyTrusted
        );
        clear_cache_environment();
    }

    #[test]
    fn rejects_invalid_cache_confidentiality() {
        let _guard = environment_lock().lock().unwrap();
        clear_cache_environment();
        unsafe {
            env::set_var("VAULTICDB_READ_CACHE_TIERS", "ram");
            env::set_var("VAULTICDB_READ_CACHE_RAM_OBJECT_STORE", "memory");
            env::set_var("VAULTICDB_READ_CACHE_RAM_MAX_BYTES", "1048576");
            env::set_var("VAULTICDB_READ_CACHE_RAM_CONFIDENTIALITY", "plaintext");
        }
        let error = cache_from_env().unwrap_err().to_string();
        assert!(error.contains("expected encrypted or decrypted"));
        clear_cache_environment();
    }

    #[test]
    fn rejects_duplicate_malformed_and_unsupported_cache_tiers() {
        let _guard = environment_lock().lock().unwrap();
        for (tiers, backend) in [
            ("same,same", "memory"),
            ("bad.id", "memory"),
            ("archive", "azure"),
        ] {
            clear_cache_environment();
            unsafe {
                env::set_var("VAULTICDB_READ_CACHE_TIERS", tiers);
                env::set_var("VAULTICDB_READ_CACHE_SAME_OBJECT_STORE", backend);
                env::set_var("VAULTICDB_READ_CACHE_SAME_MAX_BYTES", "1");
                env::set_var("VAULTICDB_READ_CACHE_BAD_ID_OBJECT_STORE", backend);
                env::set_var("VAULTICDB_READ_CACHE_BAD_ID_MAX_BYTES", "1");
                env::set_var("VAULTICDB_READ_CACHE_ARCHIVE_OBJECT_STORE", backend);
                env::set_var("VAULTICDB_READ_CACHE_ARCHIVE_MAX_BYTES", "1");
            }
            assert!(
                cache_from_env().is_err(),
                "accepted invalid tiers {tiers:?}"
            );
        }
        clear_cache_environment();
    }

    #[test]
    fn rejects_expiring_cache_session_credentials() {
        let _guard = environment_lock().lock().unwrap();
        clear_cache_environment();
        unsafe {
            env::set_var("VAULTICDB_READ_CACHE_TIERS", "remote");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_OBJECT_STORE", "s3");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_S3_BUCKET", "cache");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_S3_SESSION_TOKEN", "temporary");
            env::set_var("VAULTICDB_READ_CACHE_REMOTE_MAX_BYTES", "1048576");
        }
        let error = cache_from_env().unwrap_err().to_string();
        assert!(error.contains("cannot be renewed"));
        clear_cache_environment();
    }
}
