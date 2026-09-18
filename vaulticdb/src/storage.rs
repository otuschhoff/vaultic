//! SlateDB storage, object-store coordination, transactions, and generation state.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
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
use fs2::FileExt;
use futures_util::{stream, stream::BoxStream, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::{
    config::{DbReaderOptions, FlushOptions, FlushType, Settings},
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
    WriteHandle,
};
use slatedb_common::metrics::{
    DefaultMetricsRecorder, Metric, MetricValue, Metrics, MetricsRecorder, NoopMetricsRecorder,
};
use tokio::sync::{Mutex, RwLock};
use tonic::Status;
use zeroize::Zeroizing;

use crate::{
    attribution::StorageAttribution,
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

#[cfg(any(test, feature = "test-failpoints"))]
use std::sync::{LazyLock, Mutex as StdMutex};

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
const WAL_TARGET_LOCAL_HANDOFF_PREFIX: &[u8] = b"vaulticdb-local-wal-after-bulk-import-v1:";
const BULK_IMPORT_COMPLETE_RECORD: &[u8] = b"_vaultic/bulk-import-complete-v1";
const ACTIVE_GENERATION_PATH: &str = "_vaultic/metadata-authority";
const GENERATION_DECISION_PREFIX: &str = "_vaultic/metadata-authority-decisions";

#[cfg(any(test, feature = "test-failpoints"))]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum StorageFailpoint {
    AfterFlushBeforeHandoff(String),
    BeforeWriteBatch(String),
    BeforeTransactionCommit(String),
    BeforeTransactionDurability(String),
    CloseReader(String),
    CloseWriter(String),
    FinalizeCapsuleDurability(String),
    ObserveLatestWriterEpoch(String),
    OpenReader(String),
    OpenWriter(String),
    FlushWriter(String),
    InventoryWal,
    CloseCache,
    RefreshWriterFence(String),
    ReleaseWriterClaimAfterFailedOpen(String),
    ReleaseWriterClaim(usize),
    ReleaseWriterClaimAny,
    WriterClaimUnavailable(String),
}

#[cfg(test)]
static STORAGE_FAILPOINTS: LazyLock<StdMutex<std::collections::HashSet<StorageFailpoint>>> =
    LazyLock::new(|| StdMutex::new(std::collections::HashSet::new()));

#[cfg(feature = "test-failpoints")]
static PROCESS_STORAGE_FAILPOINTS: LazyLock<StdMutex<std::collections::HashSet<String>>> =
    LazyLock::new(|| {
        let configured = if std::env::var("VAULTICDB_TEST_CAPABILITY").as_deref()
            == Ok("vaulticdb-process-tests-v1")
        {
            std::env::var("VAULTICDB_TEST_FAILPOINTS").unwrap_or_default()
        } else {
            String::new()
        };
        StdMutex::new(
            configured
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect(),
        )
    });

#[cfg(test)]
fn arm_storage_failpoint(failpoint: StorageFailpoint) {
    STORAGE_FAILPOINTS
        .lock()
        .expect("storage failpoint lock")
        .insert(failpoint);
}

#[cfg(any(test, feature = "test-failpoints"))]
fn check_storage_failpoint(failpoint: StorageFailpoint) -> Result<()> {
    #[cfg(test)]
    if STORAGE_FAILPOINTS
        .lock()
        .expect("storage failpoint lock")
        .remove(&failpoint)
    {
        bail!("injected storage failure at {failpoint:?}");
    }
    #[cfg(feature = "test-failpoints")]
    {
        let name = match failpoint {
            StorageFailpoint::AfterFlushBeforeHandoff(_) => "after-flush-before-handoff",
            StorageFailpoint::BeforeWriteBatch(_) => "provider-unavailable",
            StorageFailpoint::BeforeTransactionCommit(_) => "before-transaction-commit",
            StorageFailpoint::BeforeTransactionDurability(_) => "before-transaction-durability",
            StorageFailpoint::CloseReader(_) => "close-reader",
            StorageFailpoint::CloseWriter(_) => "close-writer",
            StorageFailpoint::FinalizeCapsuleDurability(_) => "finalize-capsule-durability",
            StorageFailpoint::ObserveLatestWriterEpoch(_) => "observe-latest-writer-epoch",
            StorageFailpoint::OpenReader(_) => "open-reader",
            StorageFailpoint::OpenWriter(_) => "open-writer",
            StorageFailpoint::FlushWriter(_) => "flush-writer",
            StorageFailpoint::InventoryWal => "inventory-wal",
            StorageFailpoint::CloseCache => "close-cache",
            StorageFailpoint::RefreshWriterFence(_) => "refresh-writer-fence",
            StorageFailpoint::ReleaseWriterClaimAfterFailedOpen(_) => "release-writer-claim",
            StorageFailpoint::ReleaseWriterClaim(_) => "release-writer-claim",
            StorageFailpoint::ReleaseWriterClaimAny => "release-writer-claim-any",
            StorageFailpoint::WriterClaimUnavailable(_) => "writer-claim-unavailable",
        };
        if PROCESS_STORAGE_FAILPOINTS
            .lock()
            .expect("process storage failpoint lock")
            .remove(name)
        {
            bail!("injected storage failure at {name}");
        }
    }
    Ok(())
}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapsuleMigrationIntent {
    pub(crate) format: u32,
    pub(crate) repository_id: String,
    pub(crate) generation: u64,
    pub(crate) capsule_directory: String,
    pub(crate) request_sha256: String,
    pub(crate) capsule_sha256: String,
    pub(crate) capsule: Vec<u8>,
    pub(crate) local_path: Option<String>,
    pub(crate) mirror_path: Option<String>,
}

pub(crate) struct Storage {
    database: RwLock<Database>,
    database_path: String,
    object_store: Arc<dyn ObjectStore>,
    cache_manager: Option<Arc<cache::CacheManager>>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    wal_metrics: Option<Arc<WalMetrics>>,
    attribution: Arc<StorageAttribution>,
    engine_metrics: Option<Arc<DefaultMetricsRecorder>>,
    coordination_store: Arc<dyn ObjectStore>,
    encryption: EncryptionStatus,
    key_manager: Option<Arc<KeyManager>>,
    capsule_migration: Mutex<()>,
    transactions: RwLock<HashMap<String, Arc<TransactionSlot>>>,
    next_transaction: AtomicU64,
    last_durable_sequence: AtomicU64,
    last_applied_engine_sequence: AtomicU64,
    durable_engine_sequence: AtomicU64,
    latest_write_handle: Mutex<Option<WriteHandle>>,
    transaction_idle_timeout_ms: u64,
    slatedb_multiget: bool,
    slatedb_tuning: SlateDbTuning,
    metadata_rebuild_reset: bool,
    bulk_import_local_wal_data_dir: Option<PathBuf>,
    credential_manager: Option<StorageCredentialManager>,
    broker_lease_metadata: Option<BrokerLeaseMetadata>,
    writer_epoch: AtomicU64,
    wal_target: &'static str,
    wal_durability: &'static str,
}

#[derive(Default)]
pub(crate) struct EngineMetricsSnapshot {
    pub(crate) write_batches: u64,
    pub(crate) write_ops: u64,
    pub(crate) backpressure_count: u64,
    pub(crate) l0_stalls_sst_count: u64,
    pub(crate) l0_stalls_ssts_per_key: u64,
    pub(crate) immutable_memtable_flushes: u64,
    pub(crate) memtable_bytes: u64,
    pub(crate) memtable_write_bytes: u64,
    pub(crate) wal_flush_bytes: u64,
    pub(crate) l0_sst_count: u64,
    pub(crate) sst_count: u64,
    pub(crate) sorted_run_count: u64,
    pub(crate) l0_flush_bytes: u64,
    pub(crate) compacted_bytes: u64,
    pub(crate) compacted_ssts: u64,
    pub(crate) running_compactions: u64,
    pub(crate) backpressure: EngineTimingSnapshot,
    pub(crate) batch_write_queue_depth: u64,
    pub(crate) batch_write_queue: EngineTimingSnapshot,
    pub(crate) batch_write_service: EngineTimingSnapshot,
}

#[derive(Debug, Default)]
pub(crate) struct EngineTimingSnapshot {
    pub(crate) attempts: u64,
    pub(crate) failures: u64,
    pub(crate) total_us: u64,
    pub(crate) max_us: u64,
    pub(crate) completed: u64,
    pub(crate) successes: u64,
    pub(crate) cancellations: u64,
    pub(crate) timeouts: u64,
    pub(crate) active: u64,
    pub(crate) oldest_active_us: u64,
    pub(crate) latency_bucket_upper_us: Vec<u64>,
    pub(crate) latency_bucket_counts: Vec<u64>,
}

fn metric_value(metric: &Metric) -> u64 {
    match metric.value {
        MetricValue::Counter(value) => value,
        MetricValue::Gauge(value) | MetricValue::UpDownCounter(value) => {
            value.try_into().unwrap_or_default()
        }
        MetricValue::Histogram { count, .. } => count,
    }
}

fn engine_metric(metrics: &Metrics, name: &str) -> u64 {
    metrics.by_name(name).into_iter().fold(0, |total, metric| {
        total.saturating_add(metric_value(metric))
    })
}

fn engine_metric_with_labels(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> u64 {
    metrics
        .by_name_and_labels(name, labels)
        .map_or(0, metric_value)
}

fn engine_outcome_metric(metrics: &Metrics, name: &str, outcome: &str) -> u64 {
    engine_metric_with_labels(
        metrics,
        name,
        &[(slatedb::db_stats::OUTCOME_LABEL, outcome)],
    )
}

fn seconds_to_us(seconds: f64) -> u64 {
    if seconds.is_nan() || seconds <= 0.0 {
        return 0;
    }
    let micros = seconds * 1_000_000.0;
    if !micros.is_finite() || micros >= u64::MAX as f64 {
        u64::MAX
    } else {
        micros.round() as u64
    }
}

fn oldest_active_age_us(started_unix_ms: u64, now_unix_ms: u64) -> u64 {
    if started_unix_ms == 0 {
        return 0;
    }
    now_unix_ms
        .saturating_sub(started_unix_ms)
        .saturating_mul(1_000)
}

fn engine_timing(
    metrics: &Metrics,
    histogram_name: &str,
    active: u64,
    oldest_active_started_unix_ms: u64,
    now_unix_ms: u64,
    successes: u64,
    failures: u64,
    cancellations: u64,
    timeouts: u64,
) -> EngineTimingSnapshot {
    let mut snapshot = EngineTimingSnapshot {
        failures,
        successes,
        cancellations,
        timeouts,
        active,
        oldest_active_us: oldest_active_age_us(oldest_active_started_unix_ms, now_unix_ms),
        ..EngineTimingSnapshot::default()
    };
    if let Some(metric) = metrics.by_name_and_labels(histogram_name, &[]) {
        if let MetricValue::Histogram {
            count,
            sum,
            max,
            boundaries,
            bucket_counts,
            ..
        } = &metric.value
        {
            snapshot.completed = *count;
            snapshot.total_us = seconds_to_us(*sum);
            snapshot.max_us = seconds_to_us(*max);
            snapshot.latency_bucket_upper_us = boundaries
                .iter()
                .copied()
                .map(seconds_to_us)
                .chain(std::iter::once(u64::MAX))
                .collect();
            snapshot.latency_bucket_counts = bucket_counts.clone();
        }
    }
    snapshot.attempts = snapshot.completed.saturating_add(active);
    snapshot
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
    pub(crate) slatedb_tuning: SlateDbTuning,
    pub(crate) cache: cache::CacheConfig,
    pub(crate) fencing_replica: Option<String>,
    pub(crate) metadata_rebuild_initialize: bool,
    pub(crate) metadata_rebuild_reset: bool,
    pub(crate) bulk_import_local_wal_data_dir: Option<PathBuf>,
    pub(crate) broker: Option<BrokerLeaseConfig>,
    pub(crate) encryption: envelope::EncryptionConfig,
    pub(crate) transaction_idle_timeout_ms: u64,
    pub(crate) slatedb_multiget: bool,
    pub(crate) attribution_disabled: bool,
    pub(crate) topology_source: TopologySource,
    pub(crate) topology_override_local: Option<(String, PathBuf)>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SlateDbTuning {
    pub(crate) flush_interval: Option<std::time::Duration>,
    pub(crate) max_unflushed_bytes: Option<usize>,
    pub(crate) l0_sst_size_bytes: Option<usize>,
}

impl SlateDbTuning {
    fn settings(&self) -> Settings {
        let mut settings = Settings::default();
        if let Some(flush_interval) = self.flush_interval {
            settings.flush_interval = Some(flush_interval);
        }
        if let Some(max_unflushed_bytes) = self.max_unflushed_bytes {
            settings.max_unflushed_bytes = max_unflushed_bytes;
        }
        if let Some(l0_sst_size_bytes) = self.l0_sst_size_bytes {
            settings.l0_sst_size_bytes = l0_sst_size_bytes;
        }
        settings
    }
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

#[cfg(test)]
async fn open_writer(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    tuning: &SlateDbTuning,
) -> Result<Db> {
    open_writer_with_metrics(
        path,
        object_store,
        wal_object_store,
        tuning,
        Arc::new(DefaultMetricsRecorder::new()),
    )
    .await
}

async fn open_writer_with_metrics(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    tuning: &SlateDbTuning,
    engine_metrics: Arc<dyn MetricsRecorder>,
) -> Result<Db> {
    #[cfg(any(test, feature = "test-failpoints"))]
    check_storage_failpoint(StorageFailpoint::OpenWriter(path.to_owned()))?;
    let started = Instant::now();
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"slatedb_writer_open_started\"}}"
    );
    let mut builder = Db::builder(path, object_store)
        .with_settings(tuning.settings())
        .with_metrics_recorder(engine_metrics);
    if let Some(wal_store) = wal_object_store {
        builder = builder.with_wal_object_store(wal_store);
    }
    let db = builder.build().await?;
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"slatedb_wal_replay_completed\",\"fields\":{{\"elapsed_ms\":{}}}}}",
        started.elapsed().as_millis()
    );
    let flush_started = Instant::now();
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"slatedb_replayed_wal_flush_started\"}}"
    );
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .context("publish replayed SlateDB WAL")?;
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"slatedb_replayed_wal_flush_completed\",\"fields\":{{\"elapsed_ms\":{}}}}}",
        flush_started.elapsed().as_millis()
    );
    Ok(db)
}

async fn open_reader(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
) -> Result<DbReader> {
    #[cfg(any(test, feature = "test-failpoints"))]
    check_storage_failpoint(StorageFailpoint::OpenReader(path.to_owned()))?;
    let started = Instant::now();
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"slatedb_reader_open_started\",\"fields\":{{\"reason\":\"writer_claim_already_active\"}}}}"
    );
    let mut builder = DbReader::builder(path, object_store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(DbReaderOptions {
            skip_wal_replay: false,
            ..Default::default()
        });
    if let Some(wal_store) = wal_object_store {
        builder = builder.with_wal_object_store(wal_store);
    }
    let reader = builder.build().await?;
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"slatedb_reader_open_completed\",\"fields\":{{\"elapsed_ms\":{}}}}}",
        started.elapsed().as_millis()
    );
    Ok(reader)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseState {
    Reader,
    Writer,
    Unavailable,
}

impl Storage {
    pub(crate) fn cache_status(&self) -> Option<cache::CacheStatus> {
        self.cache_manager.as_ref().map(|cache| cache.status())
    }

    pub(crate) async fn update_cache_policy(
        &self,
        expected_revision: u64,
        updates: Vec<cache::CacheTierPolicyUpdate>,
    ) -> Result<cache::CacheStatus> {
        self.cache_manager
            .as_ref()
            .context("read cache is not configured")?
            .update_policy(expected_revision, updates)
            .await
    }

    pub(crate) async fn database_state(&self) -> DatabaseState {
        match &*self.database.read().await {
            Database::Reader(_) => DatabaseState::Reader,
            Database::Writer(_) => DatabaseState::Writer,
            Database::Unavailable => DatabaseState::Unavailable,
        }
    }
}

#[derive(Debug)]
pub(crate) struct StorageTransitionFailure {
    pub(crate) database: DatabaseState,
    pub(crate) claim_held: bool,
    pub(crate) epoch: u64,
    pub(crate) retryable: bool,
    pub(crate) error: anyhow::Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionOutcome {
    pub(crate) consumed: bool,
    pub(crate) applied_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WriteBatchOutcome {
    pub(crate) durable: bool,
    pub(crate) applied_sequence: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct BeginTransactionOutcome {
    pub(crate) transaction_id: String,
    pub(crate) expired: usize,
}

#[derive(Debug)]
pub(crate) struct BeginTransactionFailure {
    pub(crate) expired: usize,
    pub(crate) status: Status,
}

#[derive(Debug)]
pub(crate) enum WriterFenceFailure {
    Stale { observed_epoch: u64 },
    Unavailable(anyhow::Error),
}

#[derive(Debug)]
pub(crate) struct TransactionFailure {
    pub(crate) consumed: bool,
    pub(crate) status: Status,
}

impl TransactionFailure {
    fn before_consumption(status: Status) -> Self {
        Self {
            consumed: false,
            status,
        }
    }

    fn after_consumption(status: Status) -> Self {
        Self {
            consumed: true,
            status,
        }
    }
}

impl StorageTransitionFailure {
    fn new(database: DatabaseState, claim_held: bool, epoch: u64, error: anyhow::Error) -> Self {
        Self {
            database,
            claim_held,
            epoch,
            retryable: false,
            error,
        }
    }

    fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }
}

impl Database {
    fn as_writer(&self) -> Option<&Db> {
        match self {
            Self::Writer(db) => Some(db),
            Self::Reader(_) | Self::Unavailable => None,
        }
    }
}

#[derive(Debug)]
struct FailedOpenError {
    primary: anyhow::Error,
    cleanup_failures: Vec<FailedOpenCleanup>,
}

#[derive(Debug)]
struct FailedOpenCleanup {
    operation: &'static str,
    error: anyhow::Error,
}

impl std::fmt::Display for FailedOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.primary)?;
        formatter.write_str("; cleanup failures: ")?;
        for (index, failure) in self.cleanup_failures.iter().enumerate() {
            if index != 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{}: {:#}", failure.operation, failure.error)?;
        }
        Ok(())
    }
}

impl std::error::Error for FailedOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

fn preserve_primary_error(
    primary: anyhow::Error,
    cleanup_failures: Vec<FailedOpenCleanup>,
) -> anyhow::Error {
    if cleanup_failures.is_empty() {
        primary
    } else {
        anyhow::Error::new(FailedOpenError {
            primary,
            cleanup_failures,
        })
    }
}

async fn close_cache_manager(manager: &cache::CacheManager) -> Result<()> {
    let result = manager
        .close()
        .await
        .context("close read-cache quota coordinator");
    #[cfg(any(test, feature = "test-failpoints"))]
    check_storage_failpoint(StorageFailpoint::CloseCache)?;
    result
}

async fn failed_open_cleanup(
    primary: anyhow::Error,
    _database_path: &str,
    cache_manager: Option<&Arc<cache::CacheManager>>,
    reader: Option<DbReader>,
    writer_claim: Option<(&dyn ObjectStore, u64)>,
) -> anyhow::Error {
    if let Some(manager) = cache_manager {
        manager.begin_close();
    }
    let mut cleanup_failures = Vec::new();
    if let Some(reader) = reader {
        #[cfg(any(test, feature = "test-failpoints"))]
        let close_result =
            match check_storage_failpoint(StorageFailpoint::CloseReader(_database_path.to_owned()))
            {
                Ok(()) => reader.close().await.map_err(anyhow::Error::from),
                Err(error) => Err(error),
            };
        #[cfg(not(any(test, feature = "test-failpoints")))]
        let close_result = reader.close().await.map_err(anyhow::Error::from);
        if let Err(error) = close_result {
            cleanup_failures.push(FailedOpenCleanup {
                operation: "close SlateDB reader",
                error,
            });
        }
    }
    if let Some((coordination_store, epoch)) = writer_claim {
        #[cfg(test)]
        let release_result = match check_storage_failpoint(
            StorageFailpoint::ReleaseWriterClaimAfterFailedOpen(_database_path.to_owned()),
        ) {
            Ok(()) => release_writer_claim(coordination_store, epoch).await,
            Err(error) => Err(error),
        };
        #[cfg(not(test))]
        let release_result = release_writer_claim(coordination_store, epoch).await;
        if let Err(error) = release_result {
            cleanup_failures.push(FailedOpenCleanup {
                operation: "release writer claim",
                error,
            });
        }
    }
    if let Some(manager) = cache_manager {
        if let Err(error) = close_cache_manager(manager).await {
            cleanup_failures.push(FailedOpenCleanup {
                operation: "close read-cache quota coordinator",
                error,
            });
        }
    }
    preserve_primary_error(primary, cleanup_failures)
}

impl Storage {
    pub(crate) async fn open(repository_id: &str, config: &StorageConfig) -> Result<Self> {
        if config.metadata_rebuild_reset && !config.cache.is_volatile() {
            bail!("metadata rebuild reset permits only volatile memory read-cache tiers");
        }
        let (effective_store, mut effective_wal_store, effective_fencing, topology_leases) =
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
        if matches!(effective_wal_store, WalStoreConfig::Inherit) {
            if let Some(root) = local_wal_handoff_target(raw_control_store.as_ref()).await? {
                effective_wal_store = WalStoreConfig::Store(ReplicaStoreConfig::Local { root });
            }
        }
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
        if config.metadata_rebuild_reset {
            reset_metadata_store(raw_control_store.as_ref(), false).await?;
            if let Some(wal_store) = &raw_wal_object_store {
                reset_metadata_store(wal_store.as_ref(), true).await?;
            }
            eprintln!(
                "{{\"category\":\"integrity\",\"component\":\"vaulticdb\",\"event\":\"metadata_rebuild_candidate_reset\"}}"
            );
        }
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
            cache_manager,
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
            let cache_manager = if config.cache.tiers.is_empty() {
                None
            } else {
                Some(Arc::new(
                    cache::CacheManager::new(
                        renewable_object_store.clone(),
                        renewable_coordination_store.clone(),
                        config.cache.clone(),
                        repository_id,
                        &path,
                    )
                    .await
                    .context("open SlateDB read cache")?,
                ))
            };
            let encrypted_origin: Arc<dyn ObjectStore> = match &cache_manager {
                Some(cache)
                    if cache.has_confidentiality(cache::CacheConfidentiality::Encrypted) =>
                {
                    cache.clone()
                }
                None => renewable_object_store.clone(),
                Some(_) => renewable_object_store.clone(),
            };
            let configured = match envelope::configure_brokered(
                repository_id,
                encrypted_origin,
                &dek,
                lease.key_version,
                lease.capsule_generation,
                recovery_initialize,
            ) {
                Ok(configured) => configured,
                Err(error) => {
                    let primary = error.context("configure brokered metadata encryption");
                    return Err(failed_open_cleanup(
                        primary,
                        &path,
                        cache_manager.as_ref(),
                        None,
                        None,
                    )
                    .await);
                }
            };
            let object_store = match &cache_manager {
                Some(cache)
                    if cache.has_confidentiality(
                        cache::CacheConfidentiality::DecryptedHighlyTrusted,
                    ) =>
                {
                    cache.store(
                        configured.0.clone(),
                        cache::CacheConfidentiality::DecryptedHighlyTrusted,
                    )
                }
                _ => configured.0.clone(),
            };
            let encrypted_wal_store = match monitored_wal_store {
                Some(store) => match envelope::wrap_brokered_object_store(
                    repository_id,
                    store,
                    &dek,
                    lease.key_version,
                ) {
                    Ok(store) => Some(store),
                    Err(error) => {
                        let primary = error.context("wrap brokered WAL encryption");
                        return Err(failed_open_cleanup(
                            primary,
                            &path,
                            cache_manager.as_ref(),
                            None,
                            None,
                        )
                        .await);
                    }
                },
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
                object_store,
                cache_manager,
                encrypted_wal_store,
                renewable_coordination_store as Arc<dyn ObjectStore>,
                configured.1,
                configured.2,
                wal_metrics,
            )
        } else {
            let (monitored_wal_store, wal_metrics) = match raw_wal_object_store {
                Some(store) => {
                    let (store, metrics) = monitored_wal_store(store).await?;
                    (Some(store), Some(metrics))
                }
                None => (None, None),
            };
            let cache_manager = if config.cache.tiers.is_empty() {
                None
            } else {
                Some(Arc::new(
                    cache::CacheManager::new(
                        raw_object_store.clone(),
                        raw_coordination_store.clone(),
                        config.cache.clone(),
                        repository_id,
                        &path,
                    )
                    .await
                    .context("open SlateDB read cache")?,
                ))
            };
            let encrypted_origin: Arc<dyn ObjectStore> = match &cache_manager {
                Some(cache)
                    if cache.has_confidentiality(cache::CacheConfidentiality::Encrypted) =>
                {
                    cache.clone()
                }
                _ => raw_object_store.clone(),
            };
            let configured = match envelope::configure(
                repository_id,
                encrypted_origin,
                &config.encryption,
            )
            .await
            {
                Ok(configured) => configured,
                Err(error) => {
                    let primary = error.context("configure metadata encryption");
                    return Err(failed_open_cleanup(
                        primary,
                        &path,
                        cache_manager.as_ref(),
                        None,
                        None,
                    )
                    .await);
                }
            };
            if !configured.1.enabled
                && cache_manager.as_ref().is_some_and(|cache| {
                    cache.has_confidentiality(cache::CacheConfidentiality::Encrypted)
                })
            {
                let primary = anyhow::anyhow!(
                    "encrypted read-cache tiers require metadata encryption; use decrypted confidentiality with explicit plaintext acknowledgement when metadata encryption is off"
                );
                return Err(failed_open_cleanup(
                    primary,
                    &path,
                    cache_manager.as_ref(),
                    None,
                    None,
                )
                .await);
            }
            let wal_object_store = match monitored_wal_store {
                Some(store) if configured.1.enabled => {
                    let Some(key_manager) = configured.2.as_ref() else {
                        let primary =
                            anyhow::anyhow!("metadata encryption key manager is unavailable");
                        return Err(failed_open_cleanup(
                            primary,
                            &path,
                            cache_manager.as_ref(),
                            None,
                            None,
                        )
                        .await);
                    };
                    Some(key_manager.wrap_object_store(store))
                }
                store => store,
            };
            let object_store: Arc<dyn ObjectStore> = match &cache_manager {
                Some(cache)
                    if cache.has_confidentiality(
                        cache::CacheConfidentiality::DecryptedHighlyTrusted,
                    ) =>
                {
                    cache.store(
                        configured.0.clone(),
                        cache::CacheConfidentiality::DecryptedHighlyTrusted,
                    )
                }
                None => configured.0.clone(),
                Some(_) => configured.0.clone(),
            };
            (
                object_store,
                cache_manager,
                wal_object_store,
                raw_coordination_store,
                configured.1,
                configured.2,
                wal_metrics,
            )
        };
        let attribution = Arc::new(StorageAttribution::new(!config.attribution_disabled));
        let test_delay_profile = test_object_delay_profile_from_env(repository_id)?;
        if test_delay_profile.is_some() && config.attribution_disabled {
            bail!("test object delay profiles require storage attribution");
        }
        let (object_store, wal_object_store, coordination_store) = if config.attribution_disabled {
            (object_store, wal_object_store, coordination_store)
        } else {
            (
                role_aware_object_store(
                    object_store,
                    Arc::clone(&attribution.object_store_main),
                    Some(Arc::clone(&attribution.object_store_wal)),
                    ObjectStoreRole::Main,
                    test_delay_profile.clone(),
                ),
                wal_object_store.map(|store| {
                    role_aware_object_store(
                        store,
                        Arc::clone(&attribution.object_store_wal),
                        None,
                        ObjectStoreRole::Wal,
                        test_delay_profile.clone(),
                    )
                }),
                role_aware_object_store(
                    coordination_store,
                    Arc::clone(&attribution.object_store_coordination),
                    None,
                    ObjectStoreRole::Coordination,
                    test_delay_profile,
                ),
            )
        };
        let reset_takeover_epoch = if config.metadata_rebuild_reset {
            match active_writer_epoch(coordination_store.as_ref()).await {
                Ok(epoch) => epoch,
                Err(error) => {
                    let primary = error.context("read active writer claim for metadata reset");
                    return Err(failed_open_cleanup(
                        primary,
                        &path,
                        cache_manager.as_ref(),
                        None,
                        None,
                    )
                    .await);
                }
            }
        } else {
            None
        };
        #[cfg(any(test, feature = "test-failpoints"))]
        let writer_claim_result =
            match check_storage_failpoint(StorageFailpoint::WriterClaimUnavailable(path.clone())) {
                Ok(()) => {
                    claim_writer_epoch(coordination_store.as_ref(), reset_takeover_epoch).await
                }
                Err(_) => Ok(None),
            };
        #[cfg(not(any(test, feature = "test-failpoints")))]
        let writer_claim_result =
            claim_writer_epoch(coordination_store.as_ref(), reset_takeover_epoch).await;
        let writer_claim = match writer_claim_result {
            Ok(claim) => claim,
            Err(error) => {
                let primary = error.context("claim SlateDB writer epoch");
                return Err(failed_open_cleanup(
                    primary,
                    &path,
                    cache_manager.as_ref(),
                    None,
                    None,
                )
                .await);
            }
        };
        let engine_metrics =
            (!config.attribution_disabled).then(|| Arc::new(DefaultMetricsRecorder::new()));
        let engine_metrics_recorder: Arc<dyn MetricsRecorder> =
            engine_metrics.as_ref().map_or_else(
                || Arc::new(NoopMetricsRecorder::new()) as Arc<dyn MetricsRecorder>,
                |metrics| metrics.clone(),
            );
        let (database, writer_epoch) =
            match writer_claim {
                Some(epoch) => {
                    let db = match open_writer_with_metrics(
                        &path,
                        object_store.clone(),
                        wal_object_store.clone(),
                        &config.slatedb_tuning,
                        engine_metrics_recorder.clone(),
                    )
                    .await
                    {
                        Ok(db) => db,
                        Err(error) => {
                            let primary = error.context("open SlateDB database");
                            return Err(failed_open_cleanup(
                                primary,
                                &path,
                                cache_manager.as_ref(),
                                None,
                                Some((coordination_store.as_ref(), epoch)),
                            )
                            .await);
                        }
                    };
                    (Database::Writer(db), epoch)
                }
                None => {
                    let reader =
                        match open_reader(&path, object_store.clone(), wal_object_store.clone())
                            .await
                        {
                            Ok(reader) => reader,
                            Err(error) => {
                                let primary =
                                    error.context("open SlateDB database as non-fencing reader");
                                return Err(failed_open_cleanup(
                                    primary,
                                    &path,
                                    cache_manager.as_ref(),
                                    None,
                                    None,
                                )
                                .await);
                            }
                        };
                    #[cfg(any(test, feature = "test-failpoints"))]
                    let observed_epoch = match check_storage_failpoint(
                        StorageFailpoint::ObserveLatestWriterEpoch(path.clone()),
                    ) {
                        Ok(()) => latest_writer_epoch(coordination_store.as_ref()).await,
                        Err(error) => Err(error),
                    };
                    #[cfg(not(any(test, feature = "test-failpoints")))]
                    let observed_epoch = latest_writer_epoch(coordination_store.as_ref()).await;
                    let epoch = match observed_epoch {
                        Ok(epoch) => epoch,
                        Err(error) => {
                            let primary = error.context("observe latest SlateDB writer epoch");
                            return Err(failed_open_cleanup(
                                primary,
                                &path,
                                cache_manager.as_ref(),
                                Some(reader),
                                None,
                            )
                            .await);
                        }
                    };
                    (Database::Reader(reader), epoch)
                }
            };
        let storage = Self {
            database: RwLock::new(database),
            database_path: path,
            object_store,
            cache_manager,
            wal_object_store,
            wal_metrics,
            attribution,
            engine_metrics,
            coordination_store,
            encryption,
            key_manager,
            capsule_migration: Mutex::new(()),
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            last_applied_engine_sequence: AtomicU64::new(0),
            durable_engine_sequence: AtomicU64::new(0),
            latest_write_handle: Mutex::new(None),
            transaction_idle_timeout_ms: config.transaction_idle_timeout_ms,
            slatedb_multiget: config.slatedb_multiget,
            slatedb_tuning: config.slatedb_tuning.clone(),
            metadata_rebuild_reset: config.metadata_rebuild_reset,
            bulk_import_local_wal_data_dir: config.bulk_import_local_wal_data_dir.clone(),
            credential_manager,
            broker_lease_metadata,
            writer_epoch: AtomicU64::new(writer_epoch),
            wal_target: wal_store_kind(&effective_wal_store),
            wal_durability: wal_store_durability(&effective_wal_store),
        };
        eprintln!(
            "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"slatedb_multiget_configured\",\"fields\":{{\"enabled\":{}}}}}",
            storage.slatedb_multiget
        );
        let initialize = async {
            storage
                .ensure_encryption_policy(repository_id, config.metadata_rebuild_reset)
                .await?;
            if recovery_initialize {
                storage
                    .record_metadata_rebuild_handoff(repository_id)
                    .await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = initialize {
            let primary = error.context("initialize VaulticDB storage");
            let cleanup_failures = storage.close().await.err().map_or_else(Vec::new, |error| {
                vec![FailedOpenCleanup {
                    operation: "close storage after initialization failure",
                    error,
                }]
            });
            return Err(preserve_primary_error(primary, cleanup_failures));
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

    pub(crate) fn writer_epoch(&self) -> u64 {
        self.writer_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn supports_durability_tokens(&self) -> bool {
        self.wal_target != "memory"
    }

    pub(crate) async fn active_generation(&self, repository_id: &str) -> Result<u64, Status> {
        self.generation_authority(repository_id)
            .await
            .map(|authority| authority.active_generation)
            .map_err(storage_status)
    }

    pub(crate) async fn await_durable_through(
        &self,
        repository_id: &str,
        repository_generation: u64,
        writer_epoch: u64,
        applied_sequence: u64,
    ) -> Result<u64, Status> {
        if self.wal_target == "memory" {
            return Err(Status::failed_precondition(
                "durability tokens are unavailable with a memory WAL",
            ));
        }
        if applied_sequence == 0 {
            return Err(Status::invalid_argument(
                "durability token applied_sequence must be non-zero",
            ));
        }
        let active_generation = self.active_generation(repository_id).await?;
        if repository_generation != active_generation {
            return Err(Status::failed_precondition(format!(
                "durability token generation {repository_generation} is stale; active generation is {active_generation}"
            )));
        }
        self.assert_current_writer_epoch().await?;
        let active_epoch = self.writer_epoch();
        if writer_epoch != active_epoch {
            return Err(Status::failed_precondition(format!(
                "durability token writer epoch {writer_epoch} is stale; active epoch is {active_epoch}"
            )));
        }
        let last_applied = self.last_applied_engine_sequence.load(Ordering::Acquire);
        crate::service::process_test_barrier("VAULTICDB_TEST_DURABILITY_BEFORE_FENCE_BARRIER")
            .await?;
        let durable_sequence = await_engine_durable_through(
            &self.latest_write_handle,
            &self.durable_engine_sequence,
            last_applied,
            applied_sequence,
        )
        .await?;
        crate::service::process_test_barrier("VAULTICDB_TEST_DURABILITY_AFTER_FENCE_BARRIER")
            .await?;
        let completed_generation = self.active_generation(repository_id).await?;
        if repository_generation != completed_generation {
            return Err(Status::failed_precondition(format!(
                "durability token generation {repository_generation} became stale while waiting; active generation is {completed_generation}"
            )));
        }
        self.assert_current_writer_epoch().await?;
        let completed_epoch = self.writer_epoch();
        if writer_epoch != completed_epoch {
            return Err(Status::failed_precondition(format!(
                "durability token writer epoch {writer_epoch} became stale while waiting; active epoch is {completed_epoch}"
            )));
        }
        Ok(durable_sequence)
    }

    pub(crate) fn attribution(&self) -> &StorageAttribution {
        &self.attribution
    }

    pub(crate) fn engine_metrics_snapshot(&self) -> EngineMetricsSnapshot {
        let Some(engine_metrics) = &self.engine_metrics else {
            return EngineMetricsSnapshot::default();
        };
        let metrics = engine_metrics.snapshot();
        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let value = |name| engine_metric(&metrics, name);
        let labeled = |name, labels| engine_metric_with_labels(&metrics, name, labels);
        let outcome = |name, outcome| engine_outcome_metric(&metrics, name, outcome);
        EngineMetricsSnapshot {
            write_batches: value(slatedb::db_stats::WRITE_BATCH_COUNT),
            write_ops: value(slatedb::db_stats::WRITE_OPS),
            backpressure_count: value(slatedb::db_stats::BACKPRESSURE_COUNT),
            l0_stalls_sst_count: labeled(
                slatedb::db_stats::L0_STALL_COUNT,
                &[((
                    slatedb::db_stats::L0_STALL_TYPE_LABEL,
                    slatedb::db_stats::L0_STALL_TYPE_NUM_SSTS,
                ))],
            ),
            l0_stalls_ssts_per_key: labeled(
                slatedb::db_stats::L0_STALL_COUNT,
                &[((
                    slatedb::db_stats::L0_STALL_TYPE_LABEL,
                    slatedb::db_stats::L0_STALL_TYPE_NUM_SSTS_PER_KEY,
                ))],
            ),
            immutable_memtable_flushes: value(slatedb::db_stats::IMMUTABLE_MEMTABLE_FLUSHES),
            memtable_bytes: value(slatedb::db_stats::TOTAL_MEM_SIZE_BYTES),
            memtable_write_bytes: value(slatedb::db_stats::MEMTABLE_WRITE_BYTES),
            wal_flush_bytes: value(slatedb::wal_buffer_stats::WAL_FLUSH_BYTES),
            l0_sst_count: value(slatedb::db_stats::L0_SST_COUNT),
            sst_count: value(slatedb::db_stats::SST_COUNT),
            sorted_run_count: value(slatedb::db_stats::SORTED_RUN_COUNT),
            l0_flush_bytes: value(slatedb::db_stats::L0_FLUSH_BYTES),
            compacted_bytes: value(slatedb::compactor::stats::BYTES_COMPACTED),
            compacted_ssts: value(slatedb::compactor::stats::SSTS_WRITTEN),
            running_compactions: value(slatedb::compactor::stats::RUNNING_COMPACTIONS),
            backpressure: engine_timing(
                &metrics,
                slatedb::db_stats::BACKPRESSURE_WAIT_SECONDS,
                value(slatedb::db_stats::BACKPRESSURE_WAITERS),
                value(slatedb::db_stats::BACKPRESSURE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS),
                now_unix_ms,
                outcome(
                    slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_SUCCESS,
                ),
                outcome(
                    slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_FAILURE,
                ),
                outcome(
                    slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_CANCELLATION,
                ),
                outcome(
                    slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_TIMEOUT,
                ),
            ),
            batch_write_queue_depth: value(slatedb::db_stats::BATCH_WRITE_QUEUE_DEPTH),
            batch_write_queue: engine_timing(
                &metrics,
                slatedb::db_stats::BATCH_WRITE_QUEUE_WAIT_SECONDS,
                value(slatedb::db_stats::BATCH_WRITE_QUEUE_DEPTH),
                0,
                now_unix_ms,
                outcome(
                    slatedb::db_stats::BATCH_WRITE_QUEUE_OUTCOME_COUNT,
                    slatedb::db_stats::QUEUE_OUTCOME_PROCESSED,
                ),
                outcome(
                    slatedb::db_stats::BATCH_WRITE_QUEUE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_FAILURE,
                ),
                outcome(
                    slatedb::db_stats::BATCH_WRITE_QUEUE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_CANCELLATION,
                ),
                0,
            ),
            batch_write_service: engine_timing(
                &metrics,
                slatedb::db_stats::BATCH_WRITE_SERVICE_SECONDS,
                value(slatedb::db_stats::BATCH_WRITE_SERVICE_ACTIVE),
                value(slatedb::db_stats::BATCH_WRITE_SERVICE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS),
                now_unix_ms,
                outcome(
                    slatedb::db_stats::BATCH_WRITE_SERVICE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_SUCCESS,
                ),
                outcome(
                    slatedb::db_stats::BATCH_WRITE_SERVICE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_FAILURE,
                ),
                outcome(
                    slatedb::db_stats::BATCH_WRITE_SERVICE_OUTCOME_COUNT,
                    slatedb::db_stats::OUTCOME_CANCELLATION,
                ),
                0,
            ),
        }
    }

    pub(crate) fn key_manager(&self) -> Result<&Arc<KeyManager>, Status> {
        self.key_manager.as_ref().ok_or_else(|| {
            Status::from(VaulticDbError::Precondition {
                field: "encryption".to_owned(),
                message: "key management requires metadata encryption".to_owned(),
            })
        })
    }

    async fn ensure_encryption_policy(
        &self,
        repository_id: &str,
        allow_missing: bool,
    ) -> Result<()> {
        let database = self.database.read().await;
        let existing = match &*database {
            Database::Writer(db) => db.get(ENCRYPTION_POLICY_RECORD).await,
            Database::Reader(reader) => reader.get(ENCRYPTION_POLICY_RECORD).await,
            Database::Unavailable => bail!("metadata encryption policy storage is unavailable"),
        };
        let existing = existing.context("read metadata encryption policy")?;
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
        if !self.encryption.initializing && !allow_missing {
            bail!("metadata encryption policy is missing while encryption is required");
        }
        let Database::Writer(db) = &*database else {
            bail!("initializing metadata encryption policy requires a writer")
        };
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
            return Err(VaulticDbError::Precondition {
                field: "master_key".to_owned(),
                message: "repository master key is authoritative only in the recovery capsule"
                    .to_owned(),
            }
            .into());
        }
        if !self.encryption.enabled {
            return Err(VaulticDbError::Precondition {
                field: "master_key".to_owned(),
                message: "master-key-in-DB requires metadata encryption".to_owned(),
            }
            .into());
        }
        self.read_value(MASTER_KEY_RECORD)
            .await
            .map(|value| value.map(|bytes| bytes.to_vec()))
    }

    pub(crate) async fn store_master_key(&self, master_key: &[u8]) -> Result<(), Status> {
        if self.credential_manager.is_some() {
            return Err(VaulticDbError::Precondition {
                field: "master_key".to_owned(),
                message: "master-key-in-DB is prohibited in brokered mode".to_owned(),
            }
            .into());
        }
        if !self.encryption.enabled {
            return Err(VaulticDbError::Precondition {
                field: "master_key".to_owned(),
                message: "master-key-in-DB requires metadata encryption".to_owned(),
            }
            .into());
        }
        if master_key.is_empty() || master_key.len() > MAX_MASTER_KEY_BYTES {
            return Err(VaulticDbError::InvalidRequest {
                field: "master_key".to_owned(),
                message: "invalid repository master key".to_owned(),
            }
            .into());
        }
        if let Some(existing) = self.read_value(MASTER_KEY_RECORD).await? {
            if existing.as_ref() == master_key {
                return Ok(());
            }
            return Err(VaulticDbError::StorageConflict {
                message: "a different repository master key is already stored".to_owned(),
                retryable: false,
            }
            .into());
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

    #[cfg(test)]
    pub(crate) async fn record_capsule_migration(
        &self,
        capsule_sha256: &str,
    ) -> Result<(), Status> {
        if capsule_sha256.len() != 64
            || !capsule_sha256
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
        {
            return Err(VaulticDbError::InvalidRequest {
                field: "capsule_sha256".to_owned(),
                message: "invalid capsule digest".to_owned(),
            }
            .into());
        }
        let (pending, finalized) = self.capsule_migration_status().await?;
        if finalized.is_some() {
            return Err(VaulticDbError::Precondition {
                field: "capsule_migration".to_owned(),
                message: "capsule migration is already finalized".to_owned(),
            }
            .into());
        }
        if let Some(pending) = pending {
            if pending == capsule_sha256 {
                return Ok(());
            }
            return Err(VaulticDbError::StorageConflict {
                message: "a different capsule migration is already pending".to_owned(),
                retryable: false,
            }
            .into());
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

    pub(crate) async fn capsule_migration_intent(
        &self,
    ) -> Result<Option<CapsuleMigrationIntent>, Status> {
        let Some(value) = self.read_value(CAPSULE_MIGRATION_RECORD).await? else {
            return Ok(None);
        };
        if value.len() == 64 && value.iter().all(u8::is_ascii_hexdigit) {
            return Ok(None);
        }
        let intent: CapsuleMigrationIntent = serde_json::from_slice(&value).map_err(|_| {
            Status::from(VaulticDbError::StorageDataLoss {
                message: "pending capsule migration intention is invalid".to_owned(),
            })
        })?;
        if intent.format != 1
            || intent.capsule_sha256.len() != 64
            || !intent
                .capsule_sha256
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
            || format!("{:x}", Sha256::digest(&intent.capsule)) != intent.capsule_sha256
        {
            return Err(VaulticDbError::StorageDataLoss {
                message: "pending capsule migration intention failed validation".to_owned(),
            }
            .into());
        }
        Ok(Some(intent))
    }

    pub(crate) async fn store_capsule_migration_intent(
        &self,
        intent: &CapsuleMigrationIntent,
    ) -> Result<CapsuleMigrationIntent, Status> {
        let _migration = self.capsule_migration.lock().await;
        if let Some(existing) = self.capsule_migration_intent().await? {
            if existing.repository_id == intent.repository_id
                && existing.generation == intent.generation
                && existing.capsule_directory == intent.capsule_directory
                && existing.request_sha256 == intent.request_sha256
            {
                return Ok(existing);
            }
            return Err(VaulticDbError::StorageConflict {
                message: "a different capsule migration is already pending".to_owned(),
                retryable: false,
            }
            .into());
        }
        if self.read_value(CAPSULE_MIGRATION_RECORD).await?.is_some() {
            return Err(VaulticDbError::StorageConflict {
                message: "a legacy capsule migration is already pending".to_owned(),
                retryable: false,
            }
            .into());
        }
        self.write_capsule_migration_intent(intent).await?;
        Ok(intent.clone())
    }

    pub(crate) async fn write_capsule_migration_intent(
        &self,
        intent: &CapsuleMigrationIntent,
    ) -> Result<(), Status> {
        if intent.format != 1
            || intent.repository_id.is_empty()
            || intent.capsule_directory.is_empty()
            || intent.capsule_sha256.len() != 64
            || !intent
                .capsule_sha256
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
            || format!("{:x}", Sha256::digest(&intent.capsule)) != intent.capsule_sha256
        {
            return Err(VaulticDbError::InvalidRequest {
                field: "capsule_migration".to_owned(),
                message: "invalid capsule migration intention".to_owned(),
            }
            .into());
        }
        let encoded = serde_json::to_vec(intent)
            .map_err(|error| Status::internal(format!("encode capsule migration: {error}")))?;
        let database = self.writer().await?;
        let db = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
        db.put(CAPSULE_MIGRATION_RECORD, encoded)
            .await
            .map_err(storage_error)?
            .await_durable()
            .await
            .map_err(storage_error)
    }

    pub(crate) async fn capsule_migration_status(
        &self,
    ) -> Result<(Option<String>, Option<String>), Status> {
        let pending = match self.read_value(CAPSULE_MIGRATION_RECORD).await? {
            Some(value) if value.len() == 64 && value.iter().all(u8::is_ascii_hexdigit) => {
                Some(String::from_utf8(value.to_vec()).map_err(|_| {
                    Status::from(VaulticDbError::StorageDataLoss {
                        message: "pending capsule migration digest is invalid".to_owned(),
                    })
                })?)
            }
            Some(value) => Some(
                serde_json::from_slice::<CapsuleMigrationIntent>(&value)
                    .map_err(|_| {
                        Status::from(VaulticDbError::StorageDataLoss {
                            message: "pending capsule migration intention is invalid".to_owned(),
                        })
                    })?
                    .capsule_sha256,
            ),
            None => None,
        };
        let finalized = self
            .read_value(CAPSULE_MIGRATION_FINALIZED_RECORD)
            .await?
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose()
            .map_err(|_| {
                Status::from(VaulticDbError::StorageDataLoss {
                    message: "finalized capsule migration digest is invalid".to_owned(),
                })
            })?;
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
                let database = self.writer().await?;
                let db = database
                    .as_writer()
                    .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
                db.flush().await.map_err(storage_error)?;
                return Ok(());
            }
            return Err(VaulticDbError::Precondition {
                field: "capsule_sha256".to_owned(),
                message: "no matching prepared or finalized capsule migration".to_owned(),
            }
            .into());
        };
        let pending_digest = if pending.len() == 64 && pending.iter().all(u8::is_ascii_hexdigit) {
            pending.to_vec()
        } else {
            let intent =
                serde_json::from_slice::<CapsuleMigrationIntent>(&pending).map_err(|_| {
                    Status::from(VaulticDbError::StorageDataLoss {
                        message: "pending capsule migration intention is invalid".to_owned(),
                    })
                })?;
            if intent.local_path.is_none() || intent.mirror_path.is_none() {
                return Err(VaulticDbError::Precondition {
                    field: "capsule_migration".to_owned(),
                    message: "capsule migration publication is incomplete".to_owned(),
                }
                .into());
            }
            intent.capsule_sha256.into_bytes()
        };
        if pending_digest.as_slice() != capsule_sha256.as_bytes() {
            return Err(VaulticDbError::Precondition {
                field: "capsule_sha256".to_owned(),
                message: "prepared capsule digest mismatch".to_owned(),
            }
            .into());
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
        let handle = db.write(batch).await.map_err(storage_error)?;
        #[cfg(any(test, feature = "test-failpoints"))]
        check_storage_failpoint(StorageFailpoint::FinalizeCapsuleDurability(
            self.database_path.clone(),
        ))
        .map_err(storage_status)?;
        handle.await_durable().await.map_err(storage_error)
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let mut timer = self.attribution.finalization.timer();
        let result = async {
            if let Some(manager) = &self.cache_manager {
                manager.begin_close();
            }
            if let Some(manager) = &self.credential_manager {
                manager.close().await;
            }
            self.transactions.write().await.clear();
            let database = self.database.write().await;
            let was_writer = matches!(&*database, Database::Writer(_));
            let bulk_import_complete = match &*database {
                Database::Writer(db) => db
                    .get(BULK_IMPORT_COMPLETE_RECORD)
                    .await
                    .context("read bulk-import completion marker before shutdown")?
                    .is_some(),
                Database::Reader(_) | Database::Unavailable => false,
            };
            let database_close = match &*database {
                Database::Writer(db) => db.close().await.context("close SlateDB writer"),
                Database::Reader(reader) => reader.close().await.context("close SlateDB reader"),
                Database::Unavailable => Ok(()),
            };
            let wal_rebind = if database_close.is_ok()
                && was_writer
                && self.metadata_rebuild_reset
                && self.wal_target == "memory"
                && bulk_import_complete
            {
                match &self.bulk_import_local_wal_data_dir {
                    Some(root) => {
                        #[cfg(any(test, feature = "test-failpoints"))]
                        check_storage_failpoint(StorageFailpoint::AfterFlushBeforeHandoff(
                            self.database_path.clone(),
                        ))?;
                        mark_local_wal_handoff(self.coordination_store.as_ref(), root).await
                    }
                    None => Ok(()),
                }
            } else {
                Ok(())
            };
            let writer_release = if database_close.is_ok() && wal_rebind.is_ok() && was_writer {
                release_writer_claim(
                    self.coordination_store.as_ref(),
                    self.writer_epoch.load(Ordering::Acquire),
                )
                .await
            } else {
                Ok(())
            };
            let cache_close = match &self.cache_manager {
                Some(manager) => close_cache_manager(manager).await,
                None => Ok(()),
            };
            database_close?;
            wal_rebind?;
            writer_release?;
            cache_close?;
            Ok(())
        }
        .await;
        timer.record_result(&result);
        result
    }

    pub(crate) async fn demote(&self) -> Result<(), StorageTransitionFailure> {
        let epoch = self.writer_epoch.load(Ordering::Acquire);
        if !self.transactions.read().await.is_empty() {
            return Err(StorageTransitionFailure::new(
                DatabaseState::Writer,
                true,
                epoch,
                anyhow::anyhow!("active transactions prevent writer demotion"),
            ));
        }
        let mut database = self.database.write().await;
        if !matches!(&*database, Database::Writer(_)) {
            return Err(StorageTransitionFailure::new(
                match &*database {
                    Database::Reader(_) => DatabaseState::Reader,
                    Database::Writer(_) => DatabaseState::Writer,
                    Database::Unavailable => DatabaseState::Unavailable,
                },
                false,
                epoch,
                anyhow::anyhow!("VaulticDB is not the metadata writer"),
            ));
        }
        let previous = std::mem::replace(&mut *database, Database::Unavailable);
        let Database::Writer(db) = previous else {
            *database = previous;
            return Err(StorageTransitionFailure::new(
                DatabaseState::Unavailable,
                false,
                epoch,
                VaulticDbError::WriterDemoted.into(),
            ));
        };
        let flush_result = async {
            #[cfg(any(test, feature = "test-failpoints"))]
            check_storage_failpoint(StorageFailpoint::FlushWriter(self.database_path.clone()))?;
            db.flush().await.map_err(anyhow::Error::from)
        }
        .await;
        if let Err(error) = flush_result {
            *database = Database::Writer(db);
            return Err(StorageTransitionFailure::new(
                DatabaseState::Writer,
                true,
                epoch,
                error.context("flush SlateDB writer before demotion"),
            )
            .retryable());
        }
        self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
        let close_result = async {
            #[cfg(any(test, feature = "test-failpoints"))]
            check_storage_failpoint(StorageFailpoint::CloseWriter(self.database_path.clone()))?;
            db.close().await.map_err(anyhow::Error::from)
        }
        .await;
        if let Err(error) = close_result {
            return Err(StorageTransitionFailure::new(
                DatabaseState::Unavailable,
                true,
                epoch,
                error.context("close SlateDB writer before demotion"),
            ));
        }
        let reader = match open_reader(
            self.database_path.as_str(),
            self.object_store.clone(),
            self.wal_object_store.clone(),
        )
        .await
        {
            Ok(reader) => reader,
            Err(error) => {
                return Err(StorageTransitionFailure::new(
                    DatabaseState::Unavailable,
                    true,
                    epoch,
                    error.context("open non-fencing SlateDB reader"),
                ));
            }
        };
        *database = Database::Reader(reader);
        if let Err(error) = release_writer_claim(self.coordination_store.as_ref(), epoch).await {
            return Err(StorageTransitionFailure::new(
                DatabaseState::Reader,
                true,
                epoch,
                error,
            ));
        }
        Ok(())
    }

    pub(crate) async fn promote(
        &self,
        takeover_epoch: Option<u64>,
    ) -> Result<u64, StorageTransitionFailure> {
        let epoch = claim_writer_epoch(self.coordination_store.as_ref(), takeover_epoch)
            .await
            .map_err(|error| {
                StorageTransitionFailure::new(
                    DatabaseState::Reader,
                    false,
                    self.writer_epoch.load(Ordering::Acquire),
                    error,
                )
            })?
            .ok_or_else(|| {
                StorageTransitionFailure::new(
                    DatabaseState::Reader,
                    false,
                    self.writer_epoch.load(Ordering::Acquire),
                    anyhow::anyhow!("another VaulticDB instance acquired the next writer epoch"),
                )
            })?;
        let mut database = self.database.write().await;
        if !matches!(&*database, Database::Reader(_)) {
            let release = release_writer_claim(self.coordination_store.as_ref(), epoch).await;
            return Err(StorageTransitionFailure::new(
                match &*database {
                    Database::Reader(_) => DatabaseState::Reader,
                    Database::Writer(_) => DatabaseState::Writer,
                    Database::Unavailable => DatabaseState::Unavailable,
                },
                release.is_err(),
                epoch,
                release
                    .err()
                    .unwrap_or_else(|| anyhow::anyhow!("VaulticDB is not read-only")),
            ));
        }
        let previous = std::mem::replace(&mut *database, Database::Unavailable);
        let Database::Reader(reader) = previous else {
            *database = previous;
            return Err(StorageTransitionFailure::new(
                DatabaseState::Unavailable,
                true,
                epoch,
                VaulticDbError::WriterTransitioning.into(),
            ));
        };
        let close_result = async {
            #[cfg(any(test, feature = "test-failpoints"))]
            check_storage_failpoint(StorageFailpoint::CloseReader(self.database_path.clone()))?;
            reader.close().await.map_err(anyhow::Error::from)
        }
        .await;
        if let Err(error) = close_result {
            let released = release_writer_claim(self.coordination_store.as_ref(), epoch)
                .await
                .is_ok();
            let recovered_reader = if released {
                open_reader(
                    self.database_path.as_str(),
                    self.object_store.clone(),
                    self.wal_object_store.clone(),
                )
                .await
                .ok()
            } else {
                None
            };
            let state = if let Some(reader) = recovered_reader {
                *database = Database::Reader(reader);
                DatabaseState::Reader
            } else {
                DatabaseState::Unavailable
            };
            let failure = StorageTransitionFailure::new(
                state,
                !released,
                epoch,
                error.context("close SlateDB reader before promotion"),
            );
            return Err(if state == DatabaseState::Reader {
                failure.retryable()
            } else {
                failure
            });
        }
        let db = match open_writer_with_metrics(
            self.database_path.as_str(),
            self.object_store.clone(),
            self.wal_object_store.clone(),
            &self.slatedb_tuning,
            self.engine_metrics.as_ref().map_or_else(
                || Arc::new(NoopMetricsRecorder::new()) as Arc<dyn MetricsRecorder>,
                |metrics| metrics.clone(),
            ),
        )
        .await
        {
            Ok(db) => db,
            Err(error) => {
                let released = release_writer_claim(self.coordination_store.as_ref(), epoch)
                    .await
                    .is_ok();
                let recovered_reader = if released {
                    open_reader(
                        self.database_path.as_str(),
                        self.object_store.clone(),
                        self.wal_object_store.clone(),
                    )
                    .await
                    .ok()
                } else {
                    None
                };
                let state = if let Some(reader) = recovered_reader {
                    *database = Database::Reader(reader);
                    DatabaseState::Reader
                } else {
                    DatabaseState::Unavailable
                };
                let failure = StorageTransitionFailure::new(
                    state,
                    !released,
                    epoch,
                    error.context("open freshly fenced SlateDB writer"),
                );
                return Err(if state == DatabaseState::Reader && released {
                    failure.retryable()
                } else {
                    failure
                });
            }
        };
        *database = Database::Writer(db);
        self.writer_epoch.store(epoch, Ordering::Release);
        Ok(epoch)
    }

    pub(crate) async fn prune_expired_transactions(&self) -> (usize, usize) {
        let mut transactions = self.transactions.write().await;
        let before = transactions.len();
        let now = unix_time_ms().unwrap_or(u64::MAX);
        transactions.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || !transaction_expired(
                    slot.last_touched_ms.load(Ordering::Relaxed),
                    now,
                    self.transaction_idle_timeout_ms,
                )
        });
        (
            transactions.len(),
            before.saturating_sub(transactions.len()),
        )
    }

    pub(crate) async fn refresh_writer_fence(&self) -> Result<u64> {
        #[cfg(any(test, feature = "test-failpoints"))]
        check_storage_failpoint(StorageFailpoint::RefreshWriterFence(
            self.database_path.clone(),
        ))?;
        let current = self.writer_epoch.load(Ordering::Acquire);
        let epoch = claim_writer_epoch(self.coordination_store.as_ref(), Some(current))
            .await?
            .context("writer changed before metadata generation fencing")?;
        self.writer_epoch.store(epoch, Ordering::Release);
        Ok(epoch)
    }

    pub(crate) async fn ensure_writer_fence(&self) -> Result<(), WriterFenceFailure> {
        let current = self.writer_epoch.load(Ordering::Acquire);
        let active = active_writer_epoch(self.coordination_store.as_ref())
            .await
            .map_err(WriterFenceFailure::Unavailable)?;
        if current == 0 || active != Some(current) {
            let observed_epoch = latest_writer_epoch(self.coordination_store.as_ref())
                .await
                .map_err(WriterFenceFailure::Unavailable)?;
            return Err(WriterFenceFailure::Stale { observed_epoch });
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
        if current.decision != expected_decision
            || !matches!(
                current.state.as_str(),
                "post-activation" | "rollback-observation"
            )
        {
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
        if let Some(authority) = self
            .committed_generation_rollback(repository_id, expected_decision, &report_sha256)
            .await?
        {
            return Ok(authority);
        }
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
            state: "rollback-observation".to_owned(),
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

    pub(crate) async fn committed_generation_rollback(
        &self,
        repository_id: &str,
        expected_decision: u64,
        report_sha256: &str,
    ) -> Result<Option<GenerationAuthority>> {
        validate_report_sha256(report_sha256)?;
        let current = self.generation_authority(repository_id).await?;
        let committed_decision = expected_decision.checked_add(1);
        Ok((committed_decision == Some(current.decision)
            && current.state == "rollback-observation"
            && current.report_sha256 == report_sha256)
            .then_some(current))
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
            Database::Unavailable => Err(VaulticDbError::StorageUnavailable {
                message: "vaulticdb storage is transitioning".to_owned(),
            }
            .into()),
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
                .ok_or_else(|| transaction_not_found("transaction was closed"))?
                .get(key)
                .await
                .map_err(storage_error)?
        };
        Ok(Self::get_response(key, value))
    }

    pub(crate) async fn multi_get(
        &self,
        keys: &[Vec<u8>],
        transaction_id: &str,
        max_response_bytes: usize,
    ) -> Result<Vec<GetResponse>, Status> {
        for key in keys {
            validate_key(key)?;
        }

        let mut results = Vec::with_capacity(keys.len());
        if keys.is_empty() {
            return Ok(results);
        }
        let mut response_bytes = 0usize;
        if transaction_id.is_empty() {
            if self.slatedb_multiget {
                let database = self.database.read().await;
                let values = match &*database {
                    Database::Writer(db) => db.multi_get(keys).await.map_err(storage_error)?,
                    Database::Reader(reader) => {
                        reader.multi_get(keys).await.map_err(storage_error)?
                    }
                    Database::Unavailable => {
                        return Err(VaulticDbError::StorageUnavailable {
                            message: "vaulticdb storage is transitioning".to_owned(),
                        }
                        .into())
                    }
                };
                for (key, value) in keys.iter().zip(values) {
                    Self::push_multi_get_result(
                        &mut results,
                        &mut response_bytes,
                        max_response_bytes,
                        key,
                        value,
                    )?;
                }
            } else {
                for key in keys {
                    let value = self.read_value(key).await?;
                    Self::push_multi_get_result(
                        &mut results,
                        &mut response_bytes,
                        max_response_bytes,
                        key,
                        value,
                    )?;
                }
            }
        } else {
            let transaction = self.transaction(transaction_id).await?;
            let transaction = transaction.transaction.lock().await;
            let transaction = transaction
                .as_ref()
                .ok_or_else(|| transaction_not_found("transaction was closed"))?;
            if self.slatedb_multiget {
                let values = transaction.multi_get(keys).await.map_err(storage_error)?;
                for (key, value) in keys.iter().zip(values) {
                    Self::push_multi_get_result(
                        &mut results,
                        &mut response_bytes,
                        max_response_bytes,
                        key,
                        value,
                    )?;
                }
            } else {
                for key in keys {
                    let value = transaction.get(key).await.map_err(storage_error)?;
                    Self::push_multi_get_result(
                        &mut results,
                        &mut response_bytes,
                        max_response_bytes,
                        key,
                        value,
                    )?;
                }
            }
        }
        Ok(results)
    }

    fn get_response(key: &[u8], value: Option<bytes::Bytes>) -> GetResponse {
        match value {
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
        }
    }

    fn push_multi_get_result(
        results: &mut Vec<GetResponse>,
        response_bytes: &mut usize,
        max_response_bytes: usize,
        key: &[u8],
        value: Option<bytes::Bytes>,
    ) -> Result<(), Status> {
        let result = Self::get_response(key, value);
        *response_bytes = response_bytes
            .checked_add(repeated_message_encoded_len(result.encoded_len()))
            .ok_or_else(|| {
                Status::from(VaulticDbError::ResourceExhausted {
                    message: "multi-get response size overflow".to_owned(),
                    retryable: false,
                })
            })?;
        if *response_bytes > max_response_bytes {
            return Err(VaulticDbError::ResourceExhausted {
                message: "multi-get response byte limit exceeded".to_owned(),
                retryable: false,
            }
            .into());
        }
        results.push(result);
        Ok(())
    }

    pub(crate) async fn scan(
        &self,
        prefix: &[u8],
        after_key: &[u8],
        page_size: usize,
        transaction_id: &str,
    ) -> Result<ScanResponse, Status> {
        if !after_key.is_empty() && !after_key.starts_with(prefix) {
            return Err(VaulticDbError::InvalidRequest {
                field: "after_key".to_owned(),
                message: "scan cursor is outside the prefix".to_owned(),
            }
            .into());
        }
        let suffix = after_key.strip_prefix(prefix).unwrap_or_default();
        let database = self.database.read().await;
        let mut iterator = if transaction_id.is_empty() {
            match &*database {
                Database::Writer(db) => scan_prefix_db(db, prefix, suffix).await?,
                Database::Reader(reader) => scan_prefix_reader(reader, prefix, suffix).await?,
                Database::Unavailable => {
                    return Err(VaulticDbError::StorageUnavailable {
                        message: "vaulticdb storage is transitioning".to_owned(),
                    }
                    .into());
                }
            }
        } else {
            let transaction = self.transaction(transaction_id).await?;
            let transaction = transaction.transaction.lock().await;
            scan_prefix_transaction(
                transaction
                    .as_ref()
                    .ok_or_else(|| transaction_not_found("transaction was closed"))?,
                prefix,
                suffix,
            )
            .await?
        };
        collect_page(&mut iterator, page_size).await
    }

    pub(crate) async fn write_batch(
        &self,
        request: &WriteBatchRequest,
    ) -> Result<WriteBatchOutcome, Status> {
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
                        return Err(VaulticDbError::Idempotency {
                            message: "idempotency key is bound to a different operation".to_owned(),
                        }
                        .into());
                    }
                    return Ok(WriteBatchOutcome {
                        durable: existing.durable,
                        applied_sequence: None,
                    });
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
            let mut submit_timer = self.attribution.engine_submit.timer();
            #[cfg(any(test, feature = "test-failpoints"))]
            if let Err(error) = check_storage_failpoint(StorageFailpoint::BeforeWriteBatch(
                self.database_path.clone(),
            ))
            .map_err(storage_status)
            {
                submit_timer.failed();
                return Err(error);
            }
            let handle = match db.write(batch).await.map_err(storage_error) {
                Ok(handle) => {
                    submit_timer.succeeded();
                    drop(submit_timer);
                    handle
                }
                Err(error) => {
                    submit_timer.failed();
                    return Err(error);
                }
            };
            let applied_sequence = handle.seqnum();
            self.last_applied_engine_sequence
                .fetch_max(applied_sequence, Ordering::AcqRel);
            retain_latest_write_handle(&self.latest_write_handle, &handle).await;
            if request.await_durable || !request.idempotency_key.is_empty() {
                let mut durable_timer = self.attribution.durable_wait.timer();
                match handle.await_durable().await.map_err(storage_error) {
                    Ok(()) => durable_timer.succeeded(),
                    Err(error) => {
                        durable_timer.failed();
                        return Err(error);
                    }
                }
                self.durable_engine_sequence
                    .fetch_max(applied_sequence, Ordering::AcqRel);
                self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
            }
            return Ok(WriteBatchOutcome {
                durable: request.await_durable || !request.idempotency_key.is_empty(),
                applied_sequence: Some(applied_sequence),
            });
        }

        if request.await_durable {
            return Err(VaulticDbError::InvalidRequest {
                field: "await_durable".to_owned(),
                message: "transaction mutations become durable only at commit".to_owned(),
            }
            .into());
        }
        let transaction = self.transaction(&request.transaction_id).await?;
        let transaction = transaction.transaction.lock().await;
        let transaction = transaction
            .as_ref()
            .ok_or_else(|| transaction_not_found("transaction was closed"))?;
        for put in &request.puts {
            transaction
                .put(&put.key, &put.value)
                .map_err(storage_error)?;
        }
        for key in &request.deletes {
            transaction.delete(key).map_err(storage_error)?;
        }
        Ok(WriteBatchOutcome {
            durable: false,
            applied_sequence: None,
        })
    }

    pub(crate) async fn begin(&self) -> Result<BeginTransactionOutcome, BeginTransactionFailure> {
        self.assert_current_writer_epoch()
            .await
            .map_err(|status| BeginTransactionFailure { expired: 0, status })?;
        let mut timer = self.attribution.transaction_begin.timer();
        let result = async {
            let mut transactions = self.transactions.write().await;
            let now = unix_time_ms()
                .map_err(storage_status)
                .map_err(|status| BeginTransactionFailure { expired: 0, status })?;
            let count_before_expiry = transactions.len();
            transactions.retain(|_, slot| {
                Arc::strong_count(slot) > 1
                    || !transaction_expired(
                        slot.last_touched_ms.load(Ordering::Relaxed),
                        now,
                        self.transaction_idle_timeout_ms,
                    )
            });
            let expired = count_before_expiry.saturating_sub(transactions.len());
            if transactions.len() >= MAX_ACTIVE_TRANSACTIONS {
                return Err(BeginTransactionFailure {
                    expired,
                    status: VaulticDbError::ResourceExhausted {
                        message: "active transaction limit exceeded".to_owned(),
                        retryable: true,
                    }
                    .into(),
                });
            }
            let database = self
                .writer()
                .await
                .map_err(|status| BeginTransactionFailure { expired, status })?;
            let writer = database
                .as_writer()
                .ok_or_else(|| BeginTransactionFailure {
                    expired,
                    status: VaulticDbError::WriterDemoted.into(),
                })?;
            let transaction = writer
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .map_err(storage_error)
                .map_err(|status| BeginTransactionFailure { expired, status })?;
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
            Ok(BeginTransactionOutcome {
                transaction_id: id,
                expired,
            })
        }
        .await;
        timer.record_result(&result);
        result
    }

    pub(crate) async fn commit(
        &self,
        transaction_id: &str,
        idempotency_key: &str,
        defer_durability: bool,
        require_durability_token: bool,
    ) -> Result<TransactionOutcome, TransactionFailure> {
        if defer_durability && !self.metadata_rebuild_reset {
            return Err(TransactionFailure::before_consumption(
                VaulticDbError::Precondition {
                    field: "defer_durability".to_owned(),
                    message: "deferred durability requires metadata rebuild reset".to_owned(),
                }
                .into(),
            ));
        }
        if require_durability_token && (!defer_durability || !self.supports_durability_tokens()) {
            return Err(TransactionFailure::before_consumption(
                VaulticDbError::Precondition {
                    field: "require_durability_token".to_owned(),
                    message: "durability tokens require deferred commit with a persistent WAL"
                        .to_owned(),
                }
                .into(),
            ));
        }
        self.assert_current_writer_epoch()
            .await
            .map_err(TransactionFailure::before_consumption)?;
        let request_digest = transaction_digest(transaction_id);
        let record_key = idempotency_record_key(idempotency_key)
            .map_err(TransactionFailure::before_consumption)?;
        if let Some(key) = record_key.as_ref() {
            if let Some(existing) = self
                .read_idempotency(key)
                .await
                .map_err(TransactionFailure::before_consumption)?
            {
                if existing.operation != "transaction-commit"
                    || existing.request_sha256 != request_digest
                {
                    return Err(TransactionFailure::before_consumption(
                        VaulticDbError::Idempotency {
                            message: "idempotency key is bound to a different operation".to_owned(),
                        }
                        .into(),
                    ));
                }
                return Ok(TransactionOutcome {
                    consumed: false,
                    applied_sequence: None,
                });
            }
        }
        let transaction = self
            .remove_transaction(transaction_id)
            .await
            .map_err(TransactionFailure::before_consumption)?;
        let transaction = transaction.transaction.lock().await.take().ok_or_else(|| {
            TransactionFailure::after_consumption(transaction_not_found("transaction was closed"))
        })?;
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
                        TransactionFailure::after_consumption(Status::internal(format!(
                            "encode idempotency record: {error}"
                        )))
                    })?,
                )
                .map_err(storage_error)
                .map_err(TransactionFailure::after_consumption)?;
        }
        let mut submit_timer = self.attribution.engine_submit.timer();
        #[cfg(any(test, feature = "test-failpoints"))]
        if let Err(error) = check_storage_failpoint(StorageFailpoint::BeforeTransactionCommit(
            self.database_path.clone(),
        ))
        .map_err(storage_status)
        .map_err(TransactionFailure::after_consumption)
        {
            submit_timer.failed();
            return Err(error);
        }
        crate::service::process_test_barrier("VAULTICDB_TEST_TRANSACTION_BEFORE_APPLY_BARRIER")
            .await
            .map_err(TransactionFailure::after_consumption)?;
        let commit = transaction
            .commit()
            .await
            .map_err(storage_error)
            .map_err(TransactionFailure::after_consumption);
        let handle = match commit {
            Ok(handle) => {
                submit_timer.succeeded();
                drop(submit_timer);
                handle
            }
            Err(error) => {
                submit_timer.failed();
                return Err(error);
            }
        };
        if let Some(handle) = handle {
            let applied_sequence = handle.seqnum();
            self.last_applied_engine_sequence
                .fetch_max(applied_sequence, Ordering::AcqRel);
            retain_latest_write_handle(&self.latest_write_handle, &handle).await;
            crate::service::process_test_barrier("VAULTICDB_TEST_TRANSACTION_AFTER_APPLY_BARRIER")
                .await
                .map_err(TransactionFailure::after_consumption)?;
            if !defer_durability {
                let mut durable_timer = self.attribution.durable_wait.timer();
                #[cfg(any(test, feature = "test-failpoints"))]
                if let Err(error) = check_storage_failpoint(
                    StorageFailpoint::BeforeTransactionDurability(self.database_path.clone()),
                )
                .map_err(storage_status)
                .map_err(TransactionFailure::after_consumption)
                {
                    durable_timer.failed();
                    return Err(error);
                }
                let durability = handle
                    .await_durable()
                    .await
                    .map_err(storage_error)
                    .map_err(TransactionFailure::after_consumption);
                match durability {
                    Ok(()) => {
                        self.durable_engine_sequence
                            .fetch_max(applied_sequence, Ordering::AcqRel);
                        durable_timer.succeeded();
                    }
                    Err(error) => {
                        durable_timer.failed();
                        return Err(error);
                    }
                }
            }
            if !defer_durability {
                self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
            }
            return Ok(TransactionOutcome {
                consumed: true,
                applied_sequence: Some(applied_sequence),
            });
        }
        Ok(TransactionOutcome {
            consumed: true,
            applied_sequence: None,
        })
    }

    pub(crate) async fn rollback(
        &self,
        transaction_id: &str,
    ) -> Result<TransactionOutcome, TransactionFailure> {
        let transaction = self
            .remove_transaction(transaction_id)
            .await
            .map_err(TransactionFailure::before_consumption)?;
        let transaction = transaction.transaction.lock().await.take().ok_or_else(|| {
            TransactionFailure::after_consumption(transaction_not_found("transaction was closed"))
        })?;
        transaction.rollback();
        Ok(TransactionOutcome {
            consumed: true,
            applied_sequence: None,
        })
    }

    async fn transaction(&self, transaction_id: &str) -> Result<Arc<TransactionSlot>, Status> {
        if transaction_id.is_empty() {
            return Err(VaulticDbError::InvalidRequest {
                field: "transaction_id".to_owned(),
                message: "transaction ID is required".to_owned(),
            }
            .into());
        }
        let transaction = self
            .transactions
            .read()
            .await
            .get(transaction_id)
            .cloned()
            .ok_or_else(|| transaction_not_found("transaction was not found"))?;
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
            return Err(VaulticDbError::WriterFenced {
                generation: observed,
            }
            .into());
        }
        Ok(())
    }

    async fn remove_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Arc<TransactionSlot>, Status> {
        if transaction_id.is_empty() {
            return Err(VaulticDbError::InvalidRequest {
                field: "transaction_id".to_owned(),
                message: "transaction ID is required".to_owned(),
            }
            .into());
        }
        self.transactions
            .write()
            .await
            .remove(transaction_id)
            .ok_or_else(|| transaction_not_found("transaction was not found"))
    }

    async fn read_idempotency(&self, key: &[u8]) -> Result<Option<IdempotencyRecord>, Status> {
        self.read_value(key)
            .await?
            .map(|value| {
                serde_json::from_slice(&value).map_err(|_| {
                    Status::from(VaulticDbError::StorageDataLoss {
                        message: "invalid durable idempotency record".to_owned(),
                    })
                })
            })
            .transpose()
    }
}

async fn retain_latest_write_handle(
    latest_write_handle: &Mutex<Option<WriteHandle>>,
    candidate: &WriteHandle,
) {
    let mut latest = latest_write_handle.lock().await;
    if latest
        .as_ref()
        .is_none_or(|current| candidate.seqnum() > current.seqnum())
    {
        *latest = Some(candidate.clone());
    }
}

async fn await_engine_durable_through(
    latest_write_handle: &Mutex<Option<WriteHandle>>,
    durable_engine_sequence: &AtomicU64,
    last_applied: u64,
    applied_sequence: u64,
) -> Result<u64, Status> {
    if applied_sequence > last_applied {
        return Err(Status::failed_precondition(format!(
            "durability token sequence {applied_sequence} has not been applied; latest sequence is {last_applied}"
        )));
    }
    let already_durable = durable_engine_sequence.load(Ordering::Acquire);
    if applied_sequence <= already_durable {
        return Ok(already_durable);
    }
    let handle =
        latest_write_handle.lock().await.clone().ok_or_else(|| {
            Status::failed_precondition("durability token is no longer available")
        })?;
    if handle.seqnum() < applied_sequence {
        return Err(Status::failed_precondition(
            "durability token is no longer available",
        ));
    }
    handle.await_durable().await.map_err(storage_error)?;
    durable_engine_sequence.fetch_max(handle.seqnum(), Ordering::AcqRel);
    Ok(handle.seqnum())
}

mod rados {
    include!("storage/rados.rs");
}

#[path = "storage/cache.rs"]
pub(crate) mod cache;

include!("storage/operations.rs");

include!("storage/role_tests.rs");

include!("storage/object_store.rs");

include!("storage/tests.rs");
