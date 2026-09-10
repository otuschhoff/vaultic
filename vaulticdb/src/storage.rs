//! SlateDB storage, object-store coordination, transactions, and generation state.

use std::{
    collections::HashMap,
    ops::Bound::{Excluded, Unbounded},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures_util::{stream, stream::BoxStream, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::{
    config::DbReaderOptions,
    object_store::{
        aws::AmazonS3Builder,
        azure::{split_sas, MicrosoftAzureBuilder},
        gcp::GoogleCloudStorageBuilder,
        local::LocalFileSystem,
        memory::InMemory,
        path::Path as ObjectPath,
        prefix::PrefixStore,
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        UpdateVersion, UploadPart,
    },
    Db, DbIterator, DbReader, DbReaderMode, DbTransaction, ErrorKind, IsolationLevel, WriteBatch,
};
use tokio::sync::{Mutex, RwLock};
use tonic::Status;
use zeroize::Zeroizing;

use crate::{
    error::VaulticDbError,
    proto::{GetResponse, KeyValue, ScanResponse, WriteBatchRequest},
    replication::ReplicatedObjectStore,
};
use vaulticdb::broker::{acquire_metadata_lease, acquire_payload_lease, BrokerLeaseConnection};
use vaulticdb::encryption::{
    self,
    envelope::{self, EncryptionStatus, KeyManager},
};
use vaulticdb::ids::{Namespace, RepositoryId};
use vaulticdb::topology::{Credential, CredentialKind, Provider, ReplicaMode, TopologyDocument};

const MAX_ACTIVE_TRANSACTIONS: usize = 1_024;
const DONE_FIELD_ENCODED_LEN: usize = 2;
const MASTER_KEY_RECORD: &[u8] = b"meta:master-key";
const CAPSULE_MIGRATION_RECORD: &[u8] = b"meta:capsule-migration";
const CAPSULE_MIGRATION_FINALIZED_RECORD: &[u8] = b"meta:capsule-migration-finalized";
const ENCRYPTION_POLICY_RECORD: &[u8] = b"meta:encryption";
const METADATA_REBUILD_RECORD: &[u8] = b"meta:recovery-rebuild";
const MAX_MASTER_KEY_BYTES: usize = 4096;
const IDEMPOTENCY_PREFIX: &[u8] = b"meta:idempotency:";
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const WRITER_EPOCH_PREFIX: &str = "_vaultic/writer-epochs";
const ACTIVE_WRITER_PATH: &str = "_vaultic/active-writer";
const WAL_TARGET_PATH: &str = "_vaultic/wal-target-v1";
const ACTIVE_GENERATION_PATH: &str = "_vaultic/metadata-authority";
const GENERATION_DECISION_PREFIX: &str = "_vaultic/metadata-authority-decisions";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenerationAuthority {
    pub(crate) format: u32,
    pub(crate) repository_id: RepositoryId,
    pub(crate) decision: u64,
    pub(crate) active_generation: u64,
    pub(crate) namespace: Namespace,
    pub(crate) previous_generation: u64,
    pub(crate) previous_namespace: Namespace,
    pub(crate) state: String,
    pub(crate) report_sha256: String,
    pub(crate) decided_at_ms: u64,
    pub(crate) observation_until_ms: u64,
    pub(crate) retired_generation: u64,
}

struct TransactionSlot {
    transaction: Mutex<Option<DbTransaction>>,
    last_touched_ms: AtomicU64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncryptionPolicy {
    format: u32,
    required: bool,
    algorithm: String,
    object_format: u32,
    repository_id: RepositoryId,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdempotencyRecord {
    format: u32,
    operation: String,
    request_sha256: String,
    durable: bool,
}

pub(crate) struct Storage {
    database: RwLock<Database>,
    database_path: String,
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    wal_metrics: Option<Arc<WalMetrics>>,
    coordination_store: Arc<dyn ObjectStore>,
    encryption: EncryptionStatus,
    key_manager: Option<Arc<KeyManager>>,
    transactions: RwLock<HashMap<String, Arc<TransactionSlot>>>,
    next_transaction: AtomicU64,
    last_durable_sequence: AtomicU64,
    transaction_idle_timeout_ms: u64,
    credential_manager: Option<StorageCredentialManager>,
    broker_lease_metadata: Option<BrokerLeaseMetadata>,
    writer_epoch: AtomicU64,
    wal_target: &'static str,
    wal_durability: &'static str,
}

struct BrokerLeaseMetadata {
    epoch_id: String,
    key_version: u32,
    capsule_generation: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct BrokerLeaseConfig {
    pub(crate) socket: PathBuf,
    pub(crate) release_manifest: PathBuf,
    pub(crate) lease_duration: std::time::Duration,
    pub(crate) storage_token_ttl: std::time::Duration,
    pub(crate) storage_token_renew_margin: std::time::Duration,
    pub(crate) broker_outage_grace: std::time::Duration,
}

struct StorageCredentialManager {
    cancel: tokio::sync::watch::Sender<bool>,
    valid_until: tokio::sync::watch::Receiver<u64>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl StorageCredentialManager {
    async fn close(&self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct StorageConfig {
    pub(crate) object_store: ObjectStoreConfig,
    pub(crate) wal_store: WalStoreConfig,
    pub(crate) fencing_replica: Option<String>,
    pub(crate) metadata_rebuild_initialize: bool,
    pub(crate) broker: Option<BrokerLeaseConfig>,
    pub(crate) encryption: envelope::EncryptionConfig,
    pub(crate) transaction_idle_timeout_ms: u64,
    pub(crate) topology_source: TopologySource,
    pub(crate) topology_override_local: Option<(String, PathBuf)>,
}

#[derive(Clone, Debug)]
pub(crate) enum WalStoreConfig {
    Inherit,
    Store(ReplicaStoreConfig),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WalStatus {
    pub(crate) uploaded_bytes: u64,
    pub(crate) outstanding_flushes: u64,
    pub(crate) durability_failures: u64,
    pub(crate) last_flush_latency_ms: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) retained_segments: u64,
    pub(crate) oldest_segment_unix_ms: u64,
    pub(crate) cleanup_failures: u64,
}

async fn open_writer(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
) -> Result<Db> {
    let mut builder = Db::builder(path, object_store);
    if let Some(wal_store) = wal_object_store {
        builder = builder.with_wal_object_store(wal_store);
    }
    builder.build().await.map_err(Into::into)
}

async fn open_reader(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
) -> Result<DbReader> {
    let mut builder = DbReader::builder(path, object_store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(DbReaderOptions {
            skip_wal_replay: false,
            ..Default::default()
        });
    if let Some(wal_store) = wal_object_store {
        builder = builder.with_wal_object_store(wal_store);
    }
    builder.build().await.map_err(Into::into)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TopologySource {
    Capsule,
    External,
}

async fn storage_from_capsule(
    repository_id: &str,
    broker: &BrokerLeaseConfig,
    local_override: Option<&(String, PathBuf)>,
) -> Result<(
    ObjectStoreConfig,
    WalStoreConfig,
    Option<String>,
    Vec<BrokerLeaseConnection>,
)> {
    let socket = broker.socket.to_string_lossy();
    let (topology_lease, encoded) = acquire_payload_lease(
        &socket,
        &broker.release_manifest,
        "topology-read",
        None,
        None,
        broker.storage_token_ttl,
    )
    .await?;
    let topology = TopologyDocument::decode_redacted(encoded.as_slice())?;
    if topology.repository_id != repository_id {
        bail!("capsule topology repository identity does not match VaulticDB configuration");
    }
    let mut leases = vec![topology_lease];
    let mut replicas = Vec::with_capacity(topology.metadata_replicas.order.len());
    let mut override_applied = false;
    for id in &topology.metadata_replicas.order {
        let replica = topology
            .metadata_replicas
            .replicas
            .get(id)
            .with_context(|| format!("capsule topology is missing metadata replica {id:?}"))?;
        let mut credential = if replica.credential_policy.is_some() {
            let target = format!("metadata:{id}");
            let tier = if replica.read_only {
                "storage-read"
            } else {
                "storage-maintain"
            };
            let (lease, encoded) = acquire_payload_lease(
                &socket,
                &broker.release_manifest,
                "credential-lease",
                None,
                Some((&target, tier)),
                broker.storage_token_ttl,
            )
            .await?;
            let credential: Credential = serde_json::from_slice(encoded.as_slice())
                .with_context(|| format!("decode credential lease for metadata replica {id:?}"))?;
            credential.validate()?;
            leases.push(lease);
            Some(credential)
        } else {
            None
        };
        let endpoint = |name: &str| -> Result<String> {
            replica
                .endpoint
                .get(name)
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .with_context(|| format!("metadata replica {id:?} is missing endpoint {name:?}"))
        };
        let optional_endpoint = |name: &str| {
            replica
                .endpoint
                .get(name)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        };
        let store = match replica.provider {
            Provider::Local => {
                let root = match local_override {
                    Some((override_id, root)) if override_id == id => {
                        override_applied = true;
                        root.clone()
                    }
                    _ => PathBuf::from(endpoint("data_dir")?),
                };
                ReplicaStoreConfig::Local { root }
            }
            Provider::S3 => {
                let (access_key_id, secret_access_key, session_token) = match credential.as_mut() {
                    Some(value)
                        if matches!(
                            value.kind,
                            CredentialKind::AwsStatic
                                | CredentialKind::S3Static
                                | CredentialKind::S3Session
                        ) =>
                    {
                        (
                            value.access_key_id.take().map(Zeroizing::new),
                            value.secret_access_key.take().map(Zeroizing::new),
                            value.session_token.take().map(Zeroizing::new),
                        )
                    }
                    Some(_) => bail!("metadata replica {id:?} requires an S3 credential"),
                    None => (None, None, None),
                };
                ReplicaStoreConfig::S3 {
                    bucket: endpoint("bucket")?,
                    prefix: optional_endpoint("prefix"),
                    endpoint: optional_endpoint("url"),
                    region: optional_endpoint("region"),
                    access_key_id,
                    secret_access_key,
                    session_token,
                    provider: optional_endpoint("provider"),
                    bucket_lookup: optional_endpoint("bucket_lookup"),
                }
            }
            Provider::Rados => {
                let (client, key) = match credential.as_mut() {
                    Some(value) if value.kind == CredentialKind::CephxStatic => (
                        value
                            .client_id
                            .take()
                            .context("CephX credential is missing its client identity")?,
                        Zeroizing::new(
                            value
                                .client_secret
                                .take()
                                .context("CephX credential is missing its key")?,
                        ),
                    ),
                    Some(_) => bail!("metadata replica {id:?} requires a CephX credential"),
                    None => bail!("metadata replica {id:?} requires a CephX credential"),
                };
                ReplicaStoreConfig::Rados {
                    monitors: endpoint("monitors")?,
                    cluster_fsid: endpoint("cluster_fsid")?,
                    pool: endpoint("pool")?,
                    namespace: endpoint("namespace")?,
                    prefix: endpoint("prefix")?,
                    client,
                    key,
                }
            }
            Provider::Azure => {
                let raw_endpoint = endpoint("url")?;
                let account = endpoint("account")?;
                let (access_key, sas_token) = match credential.as_mut() {
                    Some(value) if value.kind == CredentialKind::AzureSharedKey => {
                        if value.account_name.as_deref() != Some(account.as_str()) {
                            bail!(
                                "metadata replica {id:?} Azure account does not match its endpoint"
                            );
                        }
                        (value.account_key.take().map(Zeroizing::new), None)
                    }
                    Some(value) if value.kind == CredentialKind::AzureSas => {
                        if value
                            .account_name
                            .as_deref()
                            .is_some_and(|declared| declared != account)
                        {
                            bail!(
                                "metadata replica {id:?} Azure account does not match its endpoint"
                            );
                        }
                        (None, value.sas_token.take().map(Zeroizing::new))
                    }
                    Some(_) => {
                        bail!("metadata replica {id:?} requires an Azure storage credential")
                    }
                    None => (None, None),
                };
                ReplicaStoreConfig::Azure {
                    account,
                    container: endpoint("container")?,
                    prefix: optional_endpoint("prefix"),
                    endpoint: raw_endpoint,
                    access_key,
                    bearer_token: None,
                    sas_token,
                }
            }
            Provider::Gcs => {
                let (bearer_token, service_account_key) = match credential.as_mut() {
                    Some(value) if value.kind == CredentialKind::GcpAccessToken => (
                        Some(Zeroizing::new(value.access_token.take().context(
                            "GCP access token credential is missing its token",
                        )?)),
                        None,
                    ),
                    Some(value) if value.kind == CredentialKind::GcpServiceAccountJson => (
                        None,
                        Some(Zeroizing::new(value.service_account_json.take().context(
                            "GCP service account credential is missing its JSON key",
                        )?)),
                    ),
                    Some(value) if value.kind == CredentialKind::None => (None, None),
                    Some(_) => {
                        bail!("metadata replica {id:?} requires a GCP storage credential")
                    }
                    None => (None, None),
                };
                ReplicaStoreConfig::Gcs {
                    bucket: endpoint("bucket")?,
                    prefix: optional_endpoint("prefix"),
                    bearer_token,
                    service_account_key,
                }
            }
            Provider::GoogleDrive => {
                bail!("unsupported VaulticDB metadata replica provider")
            }
        };
        replicas.push(ReplicaConfig {
            id: id.clone(),
            store,
        });
    }
    if local_override.is_some() && !override_applied {
        bail!("topology override does not select a local metadata replica");
    }
    let wal_store = match &topology.wal_target {
        None => WalStoreConfig::Inherit,
        Some(target) => {
            let mut credential = if target.credential_policy.is_some() {
                let (lease, encoded) = acquire_payload_lease(
                    &socket,
                    &broker.release_manifest,
                    "credential-lease",
                    None,
                    Some(("wal", "storage-maintain")),
                    broker.storage_token_ttl,
                )
                .await?;
                let credential: Credential = serde_json::from_slice(encoded.as_slice())
                    .context("decode credential lease for WAL target")?;
                credential.validate()?;
                leases.push(lease);
                Some(credential)
            } else {
                None
            };
            let endpoint = |name: &str| -> Result<String> {
                target
                    .endpoint
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .with_context(|| format!("WAL target is missing endpoint {name:?}"))
            };
            let optional_endpoint = |name: &str| {
                target
                    .endpoint
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
            };
            let store = match target.provider {
                Provider::Local => ReplicaStoreConfig::Local {
                    root: PathBuf::from(endpoint("data_dir")?),
                },
                Provider::S3 => {
                    let (access_key_id, secret_access_key, session_token) =
                        match credential.as_mut() {
                            Some(value)
                                if matches!(
                                    value.kind,
                                    CredentialKind::AwsStatic
                                        | CredentialKind::S3Static
                                        | CredentialKind::S3Session
                                ) =>
                            {
                                (
                                    value.access_key_id.take().map(Zeroizing::new),
                                    value.secret_access_key.take().map(Zeroizing::new),
                                    value.session_token.take().map(Zeroizing::new),
                                )
                            }
                            Some(_) => bail!("WAL target requires an S3 credential"),
                            None => (None, None, None),
                        };
                    ReplicaStoreConfig::S3 {
                        bucket: endpoint("bucket")?,
                        prefix: optional_endpoint("prefix"),
                        endpoint: optional_endpoint("url"),
                        region: optional_endpoint("region"),
                        access_key_id,
                        secret_access_key,
                        session_token,
                        provider: optional_endpoint("provider"),
                        bucket_lookup: optional_endpoint("bucket_lookup"),
                    }
                }
                Provider::Rados => {
                    let (client, key) = match credential.as_mut() {
                        Some(value) if value.kind == CredentialKind::CephxStatic => (
                            value
                                .client_id
                                .take()
                                .context("WAL CephX credential is missing its client identity")?,
                            Zeroizing::new(
                                value
                                    .client_secret
                                    .take()
                                    .context("WAL CephX credential is missing its key")?,
                            ),
                        ),
                        Some(_) => bail!("WAL target requires a CephX credential"),
                        None => bail!("WAL target requires a CephX credential"),
                    };
                    ReplicaStoreConfig::Rados {
                        monitors: endpoint("monitors")?,
                        cluster_fsid: endpoint("cluster_fsid")?,
                        pool: endpoint("pool")?,
                        namespace: endpoint("namespace")?,
                        prefix: endpoint("prefix")?,
                        client,
                        key,
                    }
                }
                _ => bail!("unsupported WAL target provider"),
            };
            WalStoreConfig::Store(store)
        }
    };
    let fencing = match topology.metadata_replicas.mode {
        ReplicaMode::Local => None,
        ReplicaMode::Replicated => Some(topology.metadata_replicas.fencing.clone()),
    };
    let store = match topology.metadata_replicas.mode {
        ReplicaMode::Local => replicas
            .into_iter()
            .next()
            .map(|replica| match replica.store {
                ReplicaStoreConfig::Local { root } => ObjectStoreConfig::Local { root },
                store => ObjectStoreConfig::Replicated {
                    replicas: vec![ReplicaConfig {
                        id: replica.id,
                        store,
                    }],
                },
            })
            .context("capsule topology has no local metadata replica")?,
        ReplicaMode::Replicated => ObjectStoreConfig::Replicated { replicas },
    };
    Ok((store, wal_store, fencing, leases))
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn start_storage_credential_manager(
    repository_id: String,
    database_path: String,
    capsule_topology: bool,
    broker: BrokerLeaseConfig,
    local_override: Option<(String, PathBuf)>,
    dek_sha256: Vec<u8>,
    initial_metadata_lease: BrokerLeaseConnection,
    initial_topology_leases: Vec<BrokerLeaseConnection>,
    renewable_object_store: Arc<RenewableObjectStore>,
    renewable_wal_store: Option<Arc<RenewableObjectStore>>,
    coordination_store: Arc<RenewableObjectStore>,
    wal_identity: String,
    initial_valid_until: u64,
) -> StorageCredentialManager {
    let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
    let (valid_until, valid_until_receiver) = tokio::sync::watch::channel(initial_valid_until);
    let task = tokio::spawn(async move {
        let mut metadata_lease = initial_metadata_lease;
        let mut topology_leases = initial_topology_leases;
        let mut failures = 0_u32;
        loop {
            let credential_expiry = topology_leases
                .iter()
                .map(|lease| lease.expires_unix_ms)
                .chain(std::iter::once(metadata_lease.expires_unix_ms))
                .min()
                .unwrap_or_default();
            let now = current_unix_ms();
            let margin_ms = broker.storage_token_renew_margin.as_millis() as u64;
            let grace_probe_ms = (broker.broker_outage_grace / 2).as_millis() as u64;
            let renewal_at = std::cmp::min(
                credential_expiry.saturating_sub(margin_ms),
                now.saturating_add(grace_probe_ms),
            );
            let delay = std::time::Duration::from_millis(renewal_at.saturating_sub(now));
            tokio::select! {
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() {
                        return;
                    }
                }
                () = tokio::time::sleep(delay) => {}
            }

            eprintln!(
                "{{\"category\":\"auth\",\"component\":\"vaulticdb\",\"event\":\"storage_credential_renewal_attempted\"}}"
            );
            let renewed = async {
                let (
                    next_path,
                    next_object_store,
                    next_wal_store,
                    next_coordination_store,
                    next_topology_leases,
                ) = if capsule_topology {
                        let (next_config, next_wal_config, next_fencing, next_topology_leases) =
                            storage_from_capsule(&repository_id, &broker, local_override.as_ref())
                                .await?;
                        let (next_path, next_object_store) =
                            object_store(&repository_id, &next_config)?;
                        let next_coordination_store = match &next_fencing {
                            Some(replica) => replicated_replica_store(
                                &next_config,
                                replica,
                                &crate::repository_key(&repository_id),
                            )?,
                            None => next_object_store.clone(),
                        };
                        if wal_store_identity(&next_wal_config) != wal_identity {
                            bail!("renewed topology changed the WAL target; drain and restart are required");
                        }
                        let next_wal_store = match &next_wal_config {
                            WalStoreConfig::Inherit => None,
                            WalStoreConfig::Store(config) => {
                                Some(wal_object_store(&repository_id, config)?)
                            }
                        };
                        (
                            next_path,
                            next_object_store,
                            next_wal_store,
                            next_coordination_store,
                            next_topology_leases,
                        )
                    } else {
                        (
                            database_path.clone(),
                            renewable_object_store.current(false)?,
                            renewable_wal_store
                                .as_ref()
                                .map(|store| store.current(false))
                                .transpose()?,
                            coordination_store.current(false)?,
                            Vec::new(),
                        )
                    };
                let (next_metadata_lease, next_dek) = acquire_metadata_lease(
                    broker.socket.to_string_lossy().as_ref(),
                    &broker.release_manifest,
                    broker.lease_duration,
                )
                .await?;
                if Sha256::digest(next_dek.as_slice()).as_slice() != dek_sha256.as_slice() {
                    bail!("renewed metadata DEK does not match the active database key");
                }
                if next_path != database_path {
                    bail!("renewed metadata topology changed the database path");
                }
                let credential_expiry = next_topology_leases
                    .iter()
                    .map(|lease| lease.expires_unix_ms)
                    .chain(std::iter::once(next_metadata_lease.expires_unix_ms))
                    .min()
                    .unwrap_or_default();
                let grace_expiry =
                    current_unix_ms().saturating_add(broker.broker_outage_grace.as_millis() as u64);
                let next_valid_until = std::cmp::min(credential_expiry, grace_expiry);
                Ok::<_, anyhow::Error>((
                    next_object_store,
                    next_wal_store,
                    next_coordination_store,
                    next_metadata_lease,
                    next_topology_leases,
                    next_valid_until,
                ))
            }
            .await;

            match renewed {
                Ok((
                    next_object_store,
                    next_wal_store,
                    next_coordination_store,
                    next_metadata_lease,
                    next_topology_leases,
                    next_valid_until,
                )) => {
                    if renewable_object_store
                        .replace(
                            next_object_store,
                            next_valid_until,
                            broker.storage_token_ttl,
                        )
                        .and_then(|()| match (&renewable_wal_store, next_wal_store) {
                            (Some(current), Some(next)) => {
                                current.replace(next, next_valid_until, broker.storage_token_ttl)
                            }
                            (None, None) => Ok(()),
                            _ => bail!("renewed topology changed separate WAL configuration"),
                        })
                        .and_then(|()| {
                            coordination_store.replace(
                                next_coordination_store,
                                next_valid_until,
                                broker.storage_token_ttl,
                            )
                        })
                        .is_err()
                    {
                        let _ = valid_until.send(current_unix_ms());
                        return;
                    }
                    metadata_lease = next_metadata_lease;
                    topology_leases = next_topology_leases;
                    failures = 0;
                    let _ = valid_until.send(next_valid_until);
                    eprintln!(
                        "{{\"category\":\"auth\",\"component\":\"vaulticdb\",\"event\":\"storage_credential_renewed\",\"fields\":{{\"valid_until_unix_ms\":{next_valid_until}}}}}"
                    );
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    let backoff_seconds = std::cmp::min(1_u64 << failures.min(6), 60);
                    let backoff_ms = backoff_seconds * 1_000;
                    let jitter_ms = current_unix_ms() % std::cmp::max(backoff_ms / 5, 1);
                    let retry_delay = std::time::Duration::from_millis(
                        backoff_ms.saturating_sub(backoff_ms / 10) + jitter_ms,
                    );
                    let event = if error.to_string().contains("(locked)") {
                        "storage_credential_renewal_locked"
                    } else {
                        "storage_credential_renewal_failed"
                    };
                    eprintln!(
                        "{{\"category\":\"auth\",\"component\":\"vaulticdb\",\"event\":\"{event}\",\"fields\":{{\"valid_until_unix_ms\":{}}}}}",
                        *valid_until.borrow()
                    );
                    tokio::select! {
                        _ = cancelled.changed() => return,
                        () = tokio::time::sleep(retry_delay) => {}
                    }
                }
            }
        }
    });
    StorageCredentialManager {
        cancel,
        valid_until: valid_until_receiver,
        task: Mutex::new(Some(task)),
    }
}

enum Database {
    Writer(Db),
    Reader(DbReader),
    Unavailable,
}

impl Database {
    fn as_writer(&self) -> Option<&Db> {
        match self {
            Self::Writer(db) => Some(db),
            Self::Reader(_) | Self::Unavailable => None,
        }
    }
}

impl Storage {
    pub(crate) async fn open(repository_id: &str, config: &StorageConfig) -> Result<Self> {
        let (effective_store, effective_wal_store, effective_fencing, topology_leases) =
            if config.topology_source == TopologySource::Capsule {
                let broker = config
                    .broker
                    .as_ref()
                    .context("capsule topology requires a broker")?;
                storage_from_capsule(
                    repository_id,
                    broker,
                    config.topology_override_local.as_ref(),
                )
                .await?
            } else {
                (
                    config.object_store.clone(),
                    config.wal_store.clone(),
                    config.fencing_replica.clone(),
                    Vec::new(),
                )
            };
        let (path, raw_object_store) = object_store(repository_id, &effective_store)?;
        let raw_control_store =
            repository_control_store(&effective_store, &path, raw_object_store.clone());
        let raw_wal_object_store = match &effective_wal_store {
            WalStoreConfig::Inherit => None,
            WalStoreConfig::Store(config) => Some(wal_object_store(repository_id, config)?),
        };
        let raw_coordination_store = match &effective_fencing {
            Some(replica) => replicated_replica_store(
                &effective_store,
                replica,
                &crate::repository_key(repository_id),
            )?,
            None => raw_control_store.clone(),
        };
        let metadata_exists =
            metadata_store_has_database_objects(raw_control_store.as_ref()).await?;
        let recovery_initialize = config.metadata_rebuild_initialize;
        if recovery_initialize && metadata_exists {
            bail!("metadata rebuild initialization requires an empty candidate metadata store");
        }
        ensure_wal_target_identity(
            raw_control_store.as_ref(),
            &wal_store_identity(&effective_wal_store),
            metadata_exists,
        )
        .await?;
        let mut credential_manager = None;
        let mut broker_lease_metadata = None;
        let (
            object_store,
            wal_object_store,
            coordination_store,
            encryption,
            key_manager,
            wal_metrics,
        ) = if let Some(broker) = &config.broker {
            let (lease, dek) = acquire_metadata_lease(
                broker.socket.to_string_lossy().as_ref(),
                &broker.release_manifest,
                broker.lease_duration,
            )
            .await?;
            let dek_sha256 = Sha256::digest(dek.as_slice()).to_vec();
            broker_lease_metadata = Some(BrokerLeaseMetadata {
                epoch_id: lease.epoch_id.clone(),
                key_version: lease.key_version,
                capsule_generation: lease.capsule_generation,
            });
            let all_expiries = topology_leases
                .iter()
                .map(|item| item.expires_unix_ms)
                .chain(std::iter::once(lease.expires_unix_ms));
            let credential_expiry = all_expiries.min().unwrap_or(lease.expires_unix_ms);
            let grace_expiry =
                current_unix_ms().saturating_add(broker.broker_outage_grace.as_millis() as u64);
            let valid_until = std::cmp::min(credential_expiry, grace_expiry);
            let renewable_object_store = Arc::new(RenewableObjectStore::new(
                raw_object_store,
                valid_until,
                broker.storage_token_ttl,
            ));
            let renewable_coordination_store = Arc::new(RenewableObjectStore::new(
                raw_coordination_store,
                valid_until,
                broker.storage_token_ttl,
            ));
            let renewable_wal_store = raw_wal_object_store.map(|store| {
                Arc::new(RenewableObjectStore::new(
                    store,
                    valid_until,
                    broker.storage_token_ttl,
                ))
            });
            let (monitored_wal_store, wal_metrics) = match &renewable_wal_store {
                Some(store) => {
                    let (store, metrics) = monitored_wal_store(store.clone()).await?;
                    (Some(store), Some(metrics))
                }
                None => (None, None),
            };
            let configured = envelope::configure_brokered(
                repository_id,
                renewable_object_store.clone(),
                &dek,
                lease.key_version,
                lease.capsule_generation,
                recovery_initialize,
            )?;
            let encrypted_wal_store = match monitored_wal_store {
                Some(store) => Some(envelope::wrap_brokered_object_store(
                    repository_id,
                    store,
                    &dek,
                    lease.key_version,
                )?),
                None => None,
            };
            credential_manager = Some(start_storage_credential_manager(
                repository_id.to_owned(),
                path.clone(),
                config.topology_source == TopologySource::Capsule,
                broker.clone(),
                config.topology_override_local.clone(),
                dek_sha256,
                lease,
                topology_leases,
                renewable_object_store,
                renewable_wal_store,
                renewable_coordination_store.clone(),
                wal_store_identity(&effective_wal_store),
                valid_until,
            ));
            (
                configured.0,
                encrypted_wal_store,
                renewable_coordination_store as Arc<dyn ObjectStore>,
                configured.1,
                configured.2,
                wal_metrics,
            )
        } else {
            let configured =
                envelope::configure(repository_id, raw_object_store, &config.encryption).await?;
            let (monitored_wal_store, wal_metrics) = match raw_wal_object_store {
                Some(store) => {
                    let (store, metrics) = monitored_wal_store(store).await?;
                    (Some(store), Some(metrics))
                }
                None => (None, None),
            };
            let wal_object_store = match monitored_wal_store {
                Some(store) if configured.1.enabled => Some(
                    configured
                        .2
                        .as_ref()
                        .context("metadata encryption key manager is unavailable")?
                        .wrap_object_store(store),
                ),
                store => store,
            };
            (
                configured.0,
                wal_object_store,
                raw_coordination_store,
                configured.1,
                configured.2,
                wal_metrics,
            )
        };
        let (database, writer_epoch) = match claim_writer_epoch(coordination_store.as_ref(), None)
            .await?
        {
            Some(epoch) => {
                let db = match open_writer(&path, object_store.clone(), wal_object_store.clone())
                    .await
                {
                    Ok(db) => db,
                    Err(error) => {
                        release_writer_claim(coordination_store.as_ref(), epoch)
                            .await
                            .context("release writer claim after database open failure")?;
                        return Err(error).context("open SlateDB database");
                    }
                };
                (Database::Writer(db), epoch)
            }
            None => (
                Database::Reader(
                    open_reader(&path, object_store.clone(), wal_object_store.clone())
                        .await
                        .context("open SlateDB database as non-fencing reader")?,
                ),
                latest_writer_epoch(coordination_store.as_ref()).await?,
            ),
        };
        let storage = Self {
            database: RwLock::new(database),
            database_path: path,
            object_store,
            wal_object_store,
            wal_metrics,
            coordination_store,
            encryption,
            key_manager,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: config.transaction_idle_timeout_ms,
            credential_manager,
            broker_lease_metadata,
            writer_epoch: AtomicU64::new(writer_epoch),
            wal_target: wal_store_kind(&effective_wal_store),
            wal_durability: wal_store_durability(&effective_wal_store),
        };
        let initialize = async {
            storage.ensure_encryption_policy(repository_id).await?;
            if recovery_initialize {
                storage
                    .record_metadata_rebuild_handoff(repository_id)
                    .await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = initialize {
            storage
                .close()
                .await
                .context("close storage after initialization failure")?;
            return Err(error).context("initialize VaulticDB storage");
        }
        Ok(storage)
    }

    pub(crate) fn broker_lease_monitor(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.credential_manager
            .as_ref()
            .map(|manager| manager.valid_until.clone())
    }

    pub(crate) fn encryption_status(&self) -> &EncryptionStatus {
        &self.encryption
    }

    pub(crate) fn wal_status(&self) -> (&str, &str, bool, bool, WalStatus) {
        (
            self.wal_target,
            self.wal_durability,
            self.wal_object_store.is_some(),
            self.encryption.enabled,
            self.wal_metrics
                .as_ref()
                .map_or_else(WalStatus::default, |metrics| metrics.snapshot()),
        )
    }

    pub(crate) async fn writer_status_epoch(&self) -> (bool, u64) {
        (
            matches!(&*self.database.read().await, Database::Writer(_)),
            self.writer_epoch.load(Ordering::Acquire),
        )
    }

    pub(crate) fn last_durable_sequence(&self) -> u64 {
        self.last_durable_sequence.load(Ordering::Acquire)
    }

    pub(crate) fn key_manager(&self) -> Result<&Arc<KeyManager>, Status> {
        self.key_manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("key management requires metadata encryption")
        })
    }

    async fn ensure_encryption_policy(&self, repository_id: &str) -> Result<()> {
        let database = self.database.read().await;
        let Database::Writer(db) = &*database else {
            bail!("metadata encryption policy requires a writer")
        };
        let existing = db
            .get(ENCRYPTION_POLICY_RECORD)
            .await
            .context("read metadata encryption policy")?;
        if !self.encryption.enabled {
            if existing.is_some() {
                bail!("metadata encryption policy exists but encryption is disabled");
            }
            return Ok(());
        }
        let expected = EncryptionPolicy {
            format: 1,
            required: true,
            algorithm: self.encryption.algorithm.to_owned(),
            object_format: 1,
            repository_id: repository_id.into(),
        };
        if let Some(value) = existing {
            let actual: EncryptionPolicy =
                serde_json::from_slice(&value).context("decode metadata encryption policy")?;
            if actual != expected {
                bail!(
                    "metadata encryption policy does not match the active encryption configuration"
                );
            }
            return Ok(());
        }
        if !self.encryption.initializing {
            bail!("metadata encryption policy is missing while encryption is required");
        }
        db.put(
            ENCRYPTION_POLICY_RECORD,
            serde_json::to_vec(&expected).context("encode metadata encryption policy")?,
        )
        .await
        .context("write metadata encryption policy")?
        .await_durable()
        .await
        .context("persist metadata encryption policy")
    }

    async fn record_metadata_rebuild_handoff(&self, repository_id: &str) -> Result<()> {
        let lease = self
            .broker_lease_metadata
            .as_ref()
            .context("metadata rebuild handoff requires a broker lease")?;
        let value = serde_json::to_vec(&serde_json::json!({
            "format": 1,
            "repository_id": repository_id,
            "capsule_generation": lease.capsule_generation,
            "metadata_dek_version": lease.key_version,
            "broker_epoch_id": lease.epoch_id,
        }))?;
        let database = self.database.read().await;
        let Database::Writer(db) = &*database else {
            bail!("metadata rebuild handoff requires a writer")
        };
        db.put(METADATA_REBUILD_RECORD, value)
            .await?
            .await_durable()
            .await
            .context("persist metadata rebuild handoff")
    }

    pub(crate) async fn get_master_key(&self) -> Result<Option<Vec<u8>>, Status> {
        if self.credential_manager.is_some() {
            return Err(Status::failed_precondition(
                "repository master key is authoritative only in the recovery capsule",
            ));
        }
        if !self.encryption.enabled {
            return Err(Status::failed_precondition(
                "master-key-in-DB requires metadata encryption",
            ));
        }
        self.read_value(MASTER_KEY_RECORD)
            .await
            .map(|value| value.map(|bytes| bytes.to_vec()))
    }

    pub(crate) async fn store_master_key(&self, master_key: &[u8]) -> Result<(), Status> {
        if self.credential_manager.is_some() {
            return Err(Status::failed_precondition(
                "master-key-in-DB is prohibited in brokered mode",
            ));
        }
        if !self.encryption.enabled {
            return Err(Status::failed_precondition(
                "master-key-in-DB requires metadata encryption",
            ));
        }
        if master_key.is_empty() || master_key.len() > MAX_MASTER_KEY_BYTES {
            return Err(Status::invalid_argument("invalid repository master key"));
        }
        if let Some(existing) = self.read_value(MASTER_KEY_RECORD).await? {
            if existing.as_ref() == master_key {
                return Ok(());
            }
            return Err(Status::already_exists(
                "a different repository master key is already stored",
            ));
        }
        let database = self.writer().await?;
        let db = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
        db.put(MASTER_KEY_RECORD, master_key)
            .await
            .map_err(storage_error)?
            .await_durable()
            .await
            .map_err(storage_error)
    }

    pub(crate) async fn record_capsule_migration(
        &self,
        capsule_sha256: &str,
    ) -> Result<(), Status> {
        if capsule_sha256.len() != 64
            || !capsule_sha256
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
        {
            return Err(Status::invalid_argument("invalid capsule digest"));
        }
        let (pending, finalized) = self.capsule_migration_status().await?;
        if finalized.is_some() {
            return Err(Status::failed_precondition(
                "capsule migration is already finalized",
            ));
        }
        if let Some(pending) = pending {
            if pending == capsule_sha256 {
                return Ok(());
            }
            return Err(Status::already_exists(
                "a different capsule migration is already pending",
            ));
        }
        let database = self.writer().await?;
        let db = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
        db.put(CAPSULE_MIGRATION_RECORD, capsule_sha256.as_bytes())
            .await
            .map_err(storage_error)?
            .await_durable()
            .await
            .map_err(storage_error)
    }

    pub(crate) async fn capsule_migration_status(
        &self,
    ) -> Result<(Option<String>, Option<String>), Status> {
        let pending = self
            .read_value(CAPSULE_MIGRATION_RECORD)
            .await?
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose()
            .map_err(|_| Status::data_loss("pending capsule migration digest is invalid"))?;
        let finalized = self
            .read_value(CAPSULE_MIGRATION_FINALIZED_RECORD)
            .await?
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose()
            .map_err(|_| Status::data_loss("finalized capsule migration digest is invalid"))?;
        Ok((pending, finalized))
    }

    pub(crate) async fn finalize_capsule_migration(
        &self,
        capsule_sha256: &str,
    ) -> Result<(), Status> {
        let pending = self.read_value(CAPSULE_MIGRATION_RECORD).await?;
        let Some(pending) = pending else {
            let finalized = self.read_value(CAPSULE_MIGRATION_FINALIZED_RECORD).await?;
            if finalized.as_deref() == Some(capsule_sha256.as_bytes()) {
                return Ok(());
            }
            return Err(Status::failed_precondition(
                "no matching prepared or finalized capsule migration",
            ));
        };
        if pending.as_ref() != capsule_sha256.as_bytes() {
            return Err(Status::failed_precondition(
                "prepared capsule digest mismatch",
            ));
        }
        let mut batch = slatedb::WriteBatch::new();
        batch.delete(MASTER_KEY_RECORD);
        batch.delete(CAPSULE_MIGRATION_RECORD);
        batch.put(
            CAPSULE_MIGRATION_FINALIZED_RECORD,
            capsule_sha256.as_bytes(),
        );
        let database = self.writer().await?;
        let db = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
        db.write(batch).await.map_err(storage_error)?;
        db.flush().await.map_err(storage_error)
    }

    pub(crate) async fn close(&self) -> Result<()> {
        if let Some(manager) = &self.credential_manager {
            manager.close().await;
        }
        self.transactions.write().await.clear();
        let database = self.database.write().await;
        let was_writer = matches!(&*database, Database::Writer(_));
        let database_close = match &*database {
            Database::Writer(db) => db.close().await.context("close SlateDB writer"),
            Database::Reader(reader) => reader.close().await.context("close SlateDB reader"),
            Database::Unavailable => Ok(()),
        };
        let writer_release = if database_close.is_ok() && was_writer {
            release_writer_claim(
                self.coordination_store.as_ref(),
                self.writer_epoch.load(Ordering::Acquire),
            )
            .await
        } else {
            Ok(())
        };
        database_close?;
        writer_release?;
        Ok(())
    }

    pub(crate) async fn demote(&self) -> Result<()> {
        if !self.transactions.read().await.is_empty() {
            bail!("active transactions prevent writer demotion")
        }
        let mut database = self.database.write().await;
        if !matches!(&*database, Database::Writer(_)) {
            bail!("VaulticDB is not the metadata writer")
        }
        let previous = std::mem::replace(&mut *database, Database::Unavailable);
        let Database::Writer(db) = previous else {
            *database = previous;
            return Err(VaulticDbError::WriterDemoted.into());
        };
        db.flush()
            .await
            .context("flush SlateDB writer before demotion")?;
        self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
        db.close()
            .await
            .context("close SlateDB writer before demotion")?;
        let reader = open_reader(
            self.database_path.as_str(),
            self.object_store.clone(),
            self.wal_object_store.clone(),
        )
        .await
        .context("open non-fencing SlateDB reader")?;
        *database = Database::Reader(reader);
        release_writer_claim(
            self.coordination_store.as_ref(),
            self.writer_epoch.load(Ordering::Acquire),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn promote(&self, takeover_epoch: Option<u64>) -> Result<u64> {
        let epoch = claim_writer_epoch(self.coordination_store.as_ref(), takeover_epoch)
            .await?
            .context("another VaulticDB instance acquired the next writer epoch")?;
        let mut database = self.database.write().await;
        if !matches!(&*database, Database::Reader(_)) {
            bail!("VaulticDB is not read-only")
        }
        let previous = std::mem::replace(&mut *database, Database::Unavailable);
        let Database::Reader(reader) = previous else {
            *database = previous;
            return Err(VaulticDbError::WriterTransitioning.into());
        };
        reader
            .close()
            .await
            .context("close SlateDB reader before promotion")?;
        let db = open_writer(
            self.database_path.as_str(),
            self.object_store.clone(),
            self.wal_object_store.clone(),
        )
        .await
        .context("open freshly fenced SlateDB writer")?;
        *database = Database::Writer(db);
        self.writer_epoch.store(epoch, Ordering::Release);
        Ok(epoch)
    }

    pub(crate) async fn active_transactions(&self) -> usize {
        self.transactions.read().await.len()
    }

    pub(crate) async fn refresh_writer_fence(&self) -> Result<u64> {
        let current = self.writer_epoch.load(Ordering::Acquire);
        let epoch = claim_writer_epoch(self.coordination_store.as_ref(), Some(current))
            .await?
            .context("writer changed before metadata generation fencing")?;
        self.writer_epoch.store(epoch, Ordering::Release);
        Ok(epoch)
    }

    pub(crate) async fn ensure_writer_fence(&self) -> Result<()> {
        let current = self.writer_epoch.load(Ordering::Acquire);
        if current == 0
            || active_writer_epoch(self.coordination_store.as_ref()).await? != Some(current)
        {
            bail!("writer epoch is stale")
        }
        Ok(())
    }

    pub(crate) async fn mutations_allowed(&self, repository_id: &str) -> Result<bool> {
        Ok(self.generation_authority(repository_id).await?.state == "healthy")
    }

    pub(crate) async fn generation_authority(
        &self,
        repository_id: &str,
    ) -> Result<GenerationAuthority> {
        Ok(
            read_generation_authority(self.coordination_store.as_ref(), repository_id)
                .await?
                .0,
        )
    }

    pub(crate) async fn quarantine_generation(
        &self,
        repository_id: &str,
        expected_generation: u64,
        diagnostic_sha256: String,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&diagnostic_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.active_generation != expected_generation {
            bail!("metadata generation changed since quarantine was authorized")
        }
        if current.state == "healing-required" && current.report_sha256 == diagnostic_sha256 {
            return Ok(current);
        }
        if current.state != "healthy" {
            bail!("metadata generation is already under a recovery interlock")
        }
        let mut authority = current;
        authority.decision = authority
            .decision
            .checked_add(1)
            .context("generation decision overflow")?;
        authority.state = "healing-required".to_owned();
        authority.report_sha256 = diagnostic_sha256;
        authority.decided_at_ms = unix_time_ms()?;
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub(crate) async fn activate_generation(
        &self,
        repository_id: &str,
        expected_generation: u64,
        candidate_generation: u64,
        namespace: Namespace,
        report_sha256: String,
        observation_window_ms: u64,
    ) -> Result<GenerationAuthority> {
        validate_generation_input(candidate_generation, &namespace, &report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.active_generation != expected_generation
            || candidate_generation <= current.active_generation
            || current.state != "healing-required"
        {
            bail!("metadata generation changed since activation was authorized")
        }
        let decided_at_ms = unix_time_ms()?;
        let authority = GenerationAuthority {
            format: 1,
            repository_id: repository_id.into(),
            decision: current
                .decision
                .checked_add(1)
                .context("generation decision overflow")?,
            active_generation: candidate_generation,
            namespace,
            previous_generation: current.active_generation,
            previous_namespace: current.namespace,
            state: "post-activation".to_owned(),
            report_sha256,
            decided_at_ms,
            observation_until_ms: decided_at_ms
                .checked_add(observation_window_ms)
                .context("generation observation deadline overflow")?,
            retired_generation: current.retired_generation,
        };
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub(crate) async fn verify_generation(
        &self,
        repository_id: &str,
        expected_decision: u64,
        report_sha256: String,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.decision != expected_decision || current.state != "post-activation" {
            bail!("metadata generation is not awaiting the authorized post-activation check")
        }
        if unix_time_ms()? < current.observation_until_ms {
            bail!("metadata generation observation window has not elapsed")
        }
        let mut authority = current;
        authority.decision = authority
            .decision
            .checked_add(1)
            .context("generation decision overflow")?;
        authority.state = "healthy".to_owned();
        authority.report_sha256 = report_sha256;
        authority.decided_at_ms = unix_time_ms()?;
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub(crate) async fn rollback_generation(
        &self,
        repository_id: &str,
        expected_decision: u64,
        report_sha256: String,
        observation_window_ms: u64,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.decision != expected_decision
            || current.state != "post-activation"
            || current.previous_generation == 0
            || current.previous_namespace.is_empty()
        {
            bail!("metadata generation rollback is no longer permitted")
        }
        let decided_at_ms = unix_time_ms()?;
        let authority = GenerationAuthority {
            format: 1,
            repository_id: repository_id.into(),
            decision: current
                .decision
                .checked_add(1)
                .context("generation decision overflow")?,
            active_generation: current.previous_generation,
            namespace: current.previous_namespace,
            previous_generation: current.active_generation,
            previous_namespace: current.namespace,
            state: "post-activation".to_owned(),
            report_sha256,
            decided_at_ms,
            observation_until_ms: decided_at_ms
                .checked_add(observation_window_ms)
                .context("generation observation deadline overflow")?,
            retired_generation: current.retired_generation,
        };
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub(crate) async fn retire_generation(
        &self,
        repository_id: &str,
        expected_decision: u64,
        generation: u64,
        report_sha256: String,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.decision != expected_decision
            || current.state != "healthy"
            || generation == current.active_generation
            || generation != current.previous_generation
        {
            bail!("metadata generation is not eligible for retirement")
        }
        let mut authority = current;
        authority.decision = authority
            .decision
            .checked_add(1)
            .context("generation decision overflow")?;
        authority.previous_generation = 0;
        authority.previous_namespace = Namespace::default();
        authority.retired_generation = generation;
        authority.report_sha256 = report_sha256;
        authority.decided_at_ms = unix_time_ms()?;
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    async fn writer(&self) -> Result<tokio::sync::RwLockReadGuard<'_, Database>, Status> {
        let database = self.database.read().await;
        if !matches!(&*database, Database::Writer(_)) {
            return Err(VaulticDbError::WriterDemoted.into());
        }
        Ok(database)
    }

    async fn read_value(&self, key: &[u8]) -> Result<Option<bytes::Bytes>, Status> {
        let database = self.database.read().await;
        match &*database {
            Database::Writer(db) => db.get(key).await.map_err(storage_error),
            Database::Reader(reader) => reader.get(key).await.map_err(storage_error),
            Database::Unavailable => Err(Status::unavailable("vaulticdb storage is transitioning")),
        }
    }

    pub(crate) async fn get(
        &self,
        key: &[u8],
        transaction_id: &str,
    ) -> Result<GetResponse, Status> {
        validate_key(key)?;
        let value = if transaction_id.is_empty() {
            self.read_value(key).await?
        } else {
            let transaction = self.transaction(transaction_id).await?;
            let transaction = transaction.transaction.lock().await;
            transaction
                .as_ref()
                .ok_or_else(|| Status::not_found("transaction was closed"))?
                .get(key)
                .await
                .map_err(storage_error)?
        };
        Ok(match value {
            Some(value) => GetResponse {
                found: true,
                value: value.to_vec(),
                key: key.to_vec(),
            },
            None => GetResponse {
                found: false,
                value: Vec::new(),
                key: key.to_vec(),
            },
        })
    }

    pub(crate) async fn scan(
        &self,
        prefix: &[u8],
        after_key: &[u8],
        page_size: usize,
        transaction_id: &str,
    ) -> Result<ScanResponse, Status> {
        if !after_key.is_empty() && !after_key.starts_with(prefix) {
            return Err(Status::invalid_argument(
                "scan cursor is outside the prefix",
            ));
        }
        let suffix = after_key.strip_prefix(prefix).unwrap_or_default();
        let database = self.database.read().await;
        let mut iterator = if transaction_id.is_empty() {
            match &*database {
                Database::Writer(db) => scan_prefix_db(db, prefix, suffix).await?,
                Database::Reader(reader) => scan_prefix_reader(reader, prefix, suffix).await?,
                Database::Unavailable => {
                    return Err(Status::unavailable("vaulticdb storage is transitioning"));
                }
            }
        } else {
            let transaction = self.transaction(transaction_id).await?;
            let transaction = transaction.transaction.lock().await;
            scan_prefix_transaction(
                transaction
                    .as_ref()
                    .ok_or_else(|| Status::not_found("transaction was closed"))?,
                prefix,
                suffix,
            )
            .await?
        };
        collect_page(&mut iterator, page_size).await
    }

    pub(crate) async fn write_batch(&self, request: &WriteBatchRequest) -> Result<bool, Status> {
        validate_mutations(request)?;
        self.assert_current_writer_epoch().await?;
        if request.transaction_id.is_empty() {
            let request_digest = write_batch_digest(request);
            let idempotency_key = idempotency_record_key(&request.idempotency_key)?;
            if let Some(key) = idempotency_key.as_ref() {
                if let Some(existing) = self.read_idempotency(key).await? {
                    if existing.operation != "write-batch"
                        || existing.request_sha256 != request_digest
                    {
                        return Err(Status::already_exists(
                            "idempotency key is bound to a different operation",
                        ));
                    }
                    return Ok(existing.durable);
                }
            }
            let mut batch = WriteBatch::new();
            for put in &request.puts {
                batch.put(&put.key, &put.value);
            }
            for key in &request.deletes {
                batch.delete(key);
            }
            if let Some(key) = idempotency_key {
                let record = IdempotencyRecord {
                    format: 1,
                    operation: "write-batch".to_owned(),
                    request_sha256: request_digest,
                    durable: true,
                };
                batch.put(
                    key,
                    serde_json::to_vec(&record).map_err(|error| {
                        Status::internal(format!("encode idempotency record: {error}"))
                    })?,
                );
            }
            let database = self.writer().await?;
            let db = database
                .as_writer()
                .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
            let handle = db.write(batch).await.map_err(storage_error)?;
            if request.await_durable || !request.idempotency_key.is_empty() {
                handle.await_durable().await.map_err(storage_error)?;
                self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
            }
            return Ok(request.await_durable || !request.idempotency_key.is_empty());
        }

        if request.await_durable {
            return Err(Status::invalid_argument(
                "transaction mutations become durable only at commit",
            ));
        }
        let transaction = self.transaction(&request.transaction_id).await?;
        let transaction = transaction.transaction.lock().await;
        let transaction = transaction
            .as_ref()
            .ok_or_else(|| Status::not_found("transaction was closed"))?;
        for put in &request.puts {
            transaction
                .put(&put.key, &put.value)
                .map_err(storage_error)?;
        }
        for key in &request.deletes {
            transaction.delete(key).map_err(storage_error)?;
        }
        Ok(false)
    }

    pub(crate) async fn begin(&self) -> Result<String, Status> {
        self.assert_current_writer_epoch().await?;
        let mut transactions = self.transactions.write().await;
        let now = unix_time_ms().map_err(storage_status)?;
        transactions.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || !transaction_expired(
                    slot.last_touched_ms.load(Ordering::Relaxed),
                    now,
                    self.transaction_idle_timeout_ms,
                )
        });
        if transactions.len() >= MAX_ACTIVE_TRANSACTIONS {
            return Err(Status::resource_exhausted(
                "active transaction limit exceeded",
            ));
        }
        let database = self.writer().await?;
        let writer = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
        let transaction = writer
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .map_err(storage_error)?;
        let id = format!(
            "txn-{}-{}",
            std::process::id(),
            self.next_transaction.fetch_add(1, Ordering::Relaxed)
        );
        transactions.insert(
            id.clone(),
            Arc::new(TransactionSlot {
                transaction: Mutex::new(Some(transaction)),
                last_touched_ms: AtomicU64::new(now),
            }),
        );
        Ok(id)
    }

    pub(crate) async fn commit(
        &self,
        transaction_id: &str,
        idempotency_key: &str,
    ) -> Result<(), Status> {
        self.assert_current_writer_epoch().await?;
        let request_digest = transaction_digest(transaction_id);
        let record_key = idempotency_record_key(idempotency_key)?;
        if let Some(key) = record_key.as_ref() {
            if let Some(existing) = self.read_idempotency(key).await? {
                if existing.operation != "transaction-commit"
                    || existing.request_sha256 != request_digest
                {
                    return Err(Status::already_exists(
                        "idempotency key is bound to a different operation",
                    ));
                }
                return Ok(());
            }
        }
        let transaction = self.remove_transaction(transaction_id).await?;
        let transaction = transaction
            .transaction
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::not_found("transaction was closed"))?;
        if let Some(key) = record_key {
            let record = IdempotencyRecord {
                format: 1,
                operation: "transaction-commit".to_owned(),
                request_sha256: request_digest,
                durable: true,
            };
            transaction
                .put(
                    key,
                    serde_json::to_vec(&record).map_err(|error| {
                        Status::internal(format!("encode idempotency record: {error}"))
                    })?,
                )
                .map_err(storage_error)?;
        }
        if let Some(handle) = transaction.commit().await.map_err(storage_error)? {
            handle.await_durable().await.map_err(storage_error)?;
        }
        self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub(crate) async fn rollback(&self, transaction_id: &str) -> Result<(), Status> {
        let transaction = self.remove_transaction(transaction_id).await?;
        let transaction = transaction
            .transaction
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::not_found("transaction was closed"))?;
        transaction.rollback();
        Ok(())
    }

    async fn transaction(&self, transaction_id: &str) -> Result<Arc<TransactionSlot>, Status> {
        if transaction_id.is_empty() {
            return Err(Status::invalid_argument("transaction ID is required"));
        }
        let transaction = self
            .transactions
            .read()
            .await
            .get(transaction_id)
            .cloned()
            .ok_or_else(|| Status::not_found("transaction was not found"))?;
        transaction
            .last_touched_ms
            .store(unix_time_ms().map_err(storage_status)?, Ordering::Relaxed);
        Ok(transaction)
    }

    async fn assert_current_writer_epoch(&self) -> Result<(), Status> {
        let claimed = self.writer_epoch.load(Ordering::Acquire);
        let observed = latest_writer_epoch(self.coordination_store.as_ref())
            .await
            .map_err(storage_status)?;
        let active = active_writer_epoch(self.coordination_store.as_ref())
            .await
            .map_err(storage_status)?;
        if claimed == 0 || observed != claimed || active != Some(claimed) {
            return Err(Status::failed_precondition(format!(
                "writer epoch is stale: claimed {claimed}, authoritative {observed}, active {active:?}"
            )));
        }
        Ok(())
    }

    async fn remove_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Arc<TransactionSlot>, Status> {
        if transaction_id.is_empty() {
            return Err(Status::invalid_argument("transaction ID is required"));
        }
        self.transactions
            .write()
            .await
            .remove(transaction_id)
            .ok_or_else(|| Status::not_found("transaction was not found"))
    }

    async fn read_idempotency(&self, key: &[u8]) -> Result<Option<IdempotencyRecord>, Status> {
        self.read_value(key)
            .await?
            .map(|value| {
                serde_json::from_slice(&value)
                    .map_err(|_| Status::data_loss("invalid durable idempotency record"))
            })
            .transpose()
    }
}

mod rados {
    include!("storage/rados.rs");
}

include!("storage/operations.rs");

include!("storage/role_tests.rs");

include!("storage/object_store.rs");

include!("storage/tests.rs");
