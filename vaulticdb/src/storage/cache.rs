//! Disposable read-cache tiers for immutable SlateDB compacted SST reads.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    ops::Range,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex, RwLock as StdRwLock, Weak,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::{
    object_store::{
        path::Path as ObjectPath, Attribute, AttributeValue, Attributes, CopyOptions, Extensions,
        GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, UpdateVersion,
    },
    object_store_tag::{ObjectStoreCallTag, SstType, TableStoreKind},
};
use tokio::{
    sync::{watch, Mutex, Notify, Semaphore},
    task::JoinHandle,
};

use super::{replica_store, ReplicaStoreConfig};

pub(crate) const CACHE_FORMAT: u32 = 1;
pub(crate) const DEFAULT_PART_SIZE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CACHE_TIERS: usize = 63;
pub(crate) const DEFAULT_CACHE_TIMEOUT: Duration = Duration::from_millis(250);
const CACHE_PREFIX: &str = "_vaultic/read-cache";
const POLICY_PREFIX: &str = "_vaultic/read-cache-policy";
const QUOTA_PREFIX: &str = "_vaultic/read-cache-quota";
const QUOTA_FORMAT: u32 = 1;
const QUOTA_CAS_ATTEMPTS: usize = 8;
const COORDINATION_TIMEOUT: Duration = Duration::from_millis(250);
const QUOTA_LEASE_DURATION: Duration = Duration::from_secs(60);
const QUOTA_RENEW_INTERVAL: Duration = Duration::from_secs(20);
const CIRCUIT_FAILURES: u64 = 3;
const CIRCUIT_OPEN_MS: u64 = 5_000;
const DELETION_BACKOFF_INITIAL: Duration = Duration::from_millis(25);
const DELETION_BACKOFF_MAX: Duration = Duration::from_secs(5);
const DELETION_BATCH_SIZE: usize = 16;
const POLICY_ENFORCEMENT_BATCH_SIZE: usize = 16;
const CLOSE_DELETION_DEADLINE: Duration = Duration::from_millis(500);

#[cfg(test)]
static CACHE_TEST_INSPECTIONS: std::sync::LazyLock<StdMutex<HashMap<String, CacheTestInspection>>> =
    std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

#[derive(Clone, Debug)]
pub(crate) struct CacheConfig {
    pub(crate) tiers: Vec<CacheTierConfig>,
    pub(crate) aggregate_max_bytes: Option<u64>,
    pub(crate) part_size_bytes: u64,
    pub(crate) max_inflight_bytes: u64,
    pub(crate) max_background_tasks: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            tiers: Vec::new(),
            aggregate_max_bytes: None,
            part_size_bytes: DEFAULT_PART_SIZE_BYTES,
            max_inflight_bytes: DEFAULT_PART_SIZE_BYTES.saturating_mul(8),
            max_background_tasks: 8,
        }
    }
}

impl CacheConfig {
    pub(crate) fn is_volatile(&self) -> bool {
        self.tiers
            .iter()
            .all(|tier| matches!(tier.store, ReplicaStoreConfig::Memory))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.tiers.len() > MAX_CACHE_TIERS {
            bail!("read-cache tiers exceed monitoring limit {MAX_CACHE_TIERS}");
        }
        if self.part_size_bytes < 4 * 1024 || !self.part_size_bytes.is_multiple_of(1024) {
            bail!("read-cache part size must be a multiple of 1 KiB and at least 4 KiB");
        }
        if self.max_inflight_bytes < self.part_size_bytes {
            bail!("read-cache in-flight budget must hold at least one cache part");
        }
        if self.max_inflight_bytes > u32::MAX as u64 {
            bail!("read-cache in-flight budget must not exceed 4 GiB");
        }
        if self.max_background_tasks == 0 || self.max_background_tasks > 4096 {
            bail!("read-cache background task limit must be between 1 and 4096");
        }
        if self.aggregate_max_bytes == Some(0) {
            bail!("read-cache aggregate byte limit must be non-zero when configured");
        }
        let mut ids = HashSet::with_capacity(self.tiers.len());
        for tier in &self.tiers {
            tier.validate()?;
            if !ids.insert(tier.id.as_str()) {
                bail!("duplicate read-cache tier ID {:?}", tier.id);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheTierConfig {
    pub(crate) id: String,
    pub(crate) store: ReplicaStoreConfig,
    pub(crate) confidentiality: CacheConfidentiality,
    pub(crate) policy: CacheTierPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CacheConfidentiality {
    Encrypted,
    DecryptedHighlyTrusted,
}

impl CacheConfidentiality {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Encrypted => "encrypted",
            Self::DecryptedHighlyTrusted => "decrypted",
        }
    }
}

impl CacheTierConfig {
    fn validate(&self) -> Result<()> {
        if self.id.is_empty()
            || self.id.len() > 128
            || self.id == "slatedb"
            || !self
                .id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            bail!("read-cache tier ID must be 1-128 ASCII letters, digits, '-' or '_' and must not be 'slatedb'");
        }
        self.policy.validate(&self.id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheTierPolicy {
    pub(crate) enabled: bool,
    pub(crate) max_bytes: u64,
    pub(crate) idle_age_ms: u64,
    pub(crate) absolute_age_ms: Option<u64>,
    pub(crate) read_priority: u32,
    pub(crate) admission_priority: u32,
    pub(crate) timeout_ms: u64,
}

impl CacheTierPolicy {
    fn validate(&self, id: &str) -> Result<()> {
        if self.enabled && self.max_bytes == 0 {
            bail!("enabled read-cache tier {id:?} must have a non-zero byte limit");
        }
        if self.timeout_ms == 0 {
            bail!("read-cache tier {id:?} must have a non-zero timeout");
        }
        if self
            .absolute_age_ms
            .is_some_and(|absolute| self.idle_age_ms != 0 && absolute < self.idle_age_ms)
        {
            bail!("read-cache tier {id:?} absolute age must not be shorter than idle age");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheTierPolicyUpdate {
    pub(crate) id: String,
    pub(crate) policy: CacheTierPolicy,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CacheMetricsSnapshot {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) origin_reads: u64,
    pub(crate) origin_reads_avoided: u64,
    pub(crate) corruptions: u64,
    pub(crate) timeouts: u64,
    pub(crate) failures: u64,
    pub(crate) bypasses: u64,
    pub(crate) admissions: u64,
    pub(crate) admission_rejections: u64,
    pub(crate) admission_rejections_reservation: u64,
    pub(crate) admission_rejections_background_budget: u64,
    pub(crate) admission_rejections_background_task: u64,
    pub(crate) capacity_evictions: u64,
    pub(crate) idle_evictions: u64,
    pub(crate) absolute_evictions: u64,
    pub(crate) corruption_evictions: u64,
    pub(crate) read_latency_total_us: u64,
    pub(crate) read_latency_count: u64,
    pub(crate) write_latency_total_us: u64,
    pub(crate) write_latency_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CacheTierStatus {
    pub(crate) id: String,
    pub(crate) confidentiality: CacheConfidentiality,
    pub(crate) policy: CacheTierPolicy,
    pub(crate) metrics: CacheMetricsSnapshot,
    pub(crate) used_bytes: u64,
    pub(crate) reserved_bytes: u64,
    pub(crate) local_used_bytes: u64,
    pub(crate) local_reserved_bytes: u64,
    pub(crate) pinned_bytes: u64,
    pub(crate) requested_max_bytes: u64,
    pub(crate) deletion_pending_bytes: u64,
    pub(crate) deletion_pending_known: bool,
    pub(crate) pending_reclaim_bytes: u64,
    pub(crate) reconciliation_lag: u64,
    pub(crate) circuit_open: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CacheStatus {
    pub(crate) revision: u64,
    pub(crate) namespace: String,
    pub(crate) aggregate_max_bytes: Option<u64>,
    pub(crate) used_bytes: u64,
    pub(crate) reserved_bytes: u64,
    pub(crate) local_used_bytes: u64,
    pub(crate) local_reserved_bytes: u64,
    pub(crate) pinned_bytes: u64,
    pub(crate) inflight_bytes: u64,
    pub(crate) max_inflight_bytes: u64,
    pub(crate) deletion_pending_bytes: u64,
    pub(crate) deletion_pending_known: bool,
    pub(crate) pending_reclaim_bytes: u64,
    pub(crate) quota_coordination_healthy: bool,
    pub(crate) quota_ledger_revision: u64,
    pub(crate) quota_lease_expiry_ms: u64,
    pub(crate) unverified_stale_bytes: u64,
    pub(crate) quota_reconciliation_lag: u64,
    pub(crate) policy_sync_lag: u64,
    pub(crate) policy_sync_error: String,
    pub(crate) metrics: CacheMetricsSnapshot,
    pub(crate) tiers: Vec<CacheTierStatus>,
}

#[derive(Debug, Default)]
struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    origin_reads: AtomicU64,
    origin_reads_avoided: AtomicU64,
    corruptions: AtomicU64,
    timeouts: AtomicU64,
    failures: AtomicU64,
    bypasses: AtomicU64,
    admissions: AtomicU64,
    admission_rejections: AtomicU64,
    admission_rejections_reservation: AtomicU64,
    admission_rejections_background_budget: AtomicU64,
    admission_rejections_background_task: AtomicU64,
    capacity_evictions: AtomicU64,
    idle_evictions: AtomicU64,
    absolute_evictions: AtomicU64,
    corruption_evictions: AtomicU64,
    read_latency_total_us: AtomicU64,
    read_latency_count: AtomicU64,
    write_latency_total_us: AtomicU64,
    write_latency_count: AtomicU64,
}

impl CacheMetrics {
    fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Acquire),
            misses: self.misses.load(Ordering::Acquire),
            origin_reads: self.origin_reads.load(Ordering::Acquire),
            origin_reads_avoided: self.origin_reads_avoided.load(Ordering::Acquire),
            corruptions: self.corruptions.load(Ordering::Acquire),
            timeouts: self.timeouts.load(Ordering::Acquire),
            failures: self.failures.load(Ordering::Acquire),
            bypasses: self.bypasses.load(Ordering::Acquire),
            admissions: self.admissions.load(Ordering::Acquire),
            admission_rejections: self.admission_rejections.load(Ordering::Acquire),
            admission_rejections_reservation: self
                .admission_rejections_reservation
                .load(Ordering::Acquire),
            admission_rejections_background_budget: self
                .admission_rejections_background_budget
                .load(Ordering::Acquire),
            admission_rejections_background_task: self
                .admission_rejections_background_task
                .load(Ordering::Acquire),
            capacity_evictions: self.capacity_evictions.load(Ordering::Acquire),
            idle_evictions: self.idle_evictions.load(Ordering::Acquire),
            absolute_evictions: self.absolute_evictions.load(Ordering::Acquire),
            corruption_evictions: self.corruption_evictions.load(Ordering::Acquire),
            read_latency_total_us: self.read_latency_total_us.load(Ordering::Acquire),
            read_latency_count: self.read_latency_count.load(Ordering::Acquire),
            write_latency_total_us: self.write_latency_total_us.load(Ordering::Acquire),
            write_latency_count: self.write_latency_count.load(Ordering::Acquire),
        }
    }
}

#[derive(Default)]
struct CircuitState {
    failures: u64,
    open_until_ms: u64,
}

struct CacheTier {
    id: String,
    store: Arc<dyn ObjectStore>,
    confidentiality: CacheConfidentiality,
    metrics: CacheMetrics,
    circuit: StdMutex<CircuitState>,
    reconciliation_lag: AtomicU64,
    inventory_reconciliation_failed: AtomicBool,
}

impl fmt::Debug for CacheTier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CacheTier")
            .field("id", &self.id)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct EntryState {
    bytes: u64,
    created_ms: u64,
    accessed_ms: u64,
    data_present: bool,
    metadata_valid: bool,
    pins: u64,
    retiring: bool,
    generation: u64,
}

#[derive(Clone, Debug)]
struct Reservation {
    index: usize,
    bytes: u64,
    generation: u64,
    policy_revision: u64,
    policy_generation: u64,
    quota_entry_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionCommit {
    Active,
    Rejected,
    Retiring,
}

#[derive(Clone, Debug)]
struct PendingDeletion {
    index: usize,
    key: String,
    generation: Option<u64>,
    retry_at_ms: u64,
    backoff: Duration,
}

#[derive(Debug, Default)]
struct CapacityState {
    entries: HashMap<(usize, String), EntryState>,
    used_by_tier: HashMap<usize, u64>,
    reserved_by_tier: HashMap<usize, u64>,
    pinned_by_tier: HashMap<usize, u64>,
    aggregate_used: u64,
    aggregate_reserved: u64,
    inflight_reserved: u64,
}

#[derive(Clone, Debug)]
struct SharedOriginResult {
    payload: Bytes,
    meta: ObjectMeta,
    range: Range<u64>,
    attributes: Attributes,
    extensions: Extensions,
}

#[derive(Debug, Default)]
struct Flight {
    result: StdMutex<Option<SharedOriginResult>>,
    complete: AtomicBool,
    notify: Notify,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheSidecar {
    format: u32,
    namespace: String,
    origin_path: String,
    request_identity: String,
    location: String,
    last_modified: String,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
    response_start: u64,
    response_end: u64,
    sha256: String,
    length: u64,
    attributes: Vec<(String, String)>,
    created_ms: u64,
    accessed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPolicy {
    format: u32,
    namespace: String,
    revision: u64,
    tiers: Vec<CacheTierPolicyUpdate>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaManagerGrant {
    lease_expires_ms: u64,
    committed_by_tier: HashMap<String, u64>,
    reserved_by_tier: HashMap<String, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaEntry {
    key: String,
    tier_id: String,
    owner_manager_id: String,
    bytes: u64,
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    policy_revision: u64,
    #[serde(default)]
    expires_ms: u64,
    #[serde(default)]
    state: QuotaEntryState,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QuotaEntryState {
    #[default]
    Active,
    Admitting,
    Deleting,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaReconciler {
    manager_id: String,
    lease_expires_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaLedger {
    format: u32,
    namespace: String,
    revision: u64,
    #[serde(default)]
    next_generation: u64,
    policy_revision: u64,
    aggregate_max_bytes: Option<u64>,
    managers: HashMap<String, QuotaManagerGrant>,
    entries: HashMap<String, QuotaEntry>,
    reconciler: Option<QuotaReconciler>,
}

fn quota_ledger_accounting_valid(ledger: &QuotaLedger) -> bool {
    let committed = ledger
        .entries
        .values()
        .filter(|entry| entry.state != QuotaEntryState::Admitting)
        .try_fold(0u64, |total, entry| total.checked_add(entry.bytes));
    let reserved = ledger
        .managers
        .values()
        .flat_map(|grant| grant.reserved_by_tier.values())
        .try_fold(0u64, |total, bytes| total.checked_add(*bytes));
    let mut expected_reserved = HashMap::<(&str, &str), u64>::new();
    for entry in ledger
        .entries
        .values()
        .filter(|entry| entry.state == QuotaEntryState::Admitting)
    {
        if !ledger.managers.contains_key(&entry.owner_manager_id) {
            return false;
        }
        let reserved = expected_reserved
            .entry((&entry.owner_manager_id, &entry.tier_id))
            .or_default();
        let Some(total) = reserved.checked_add(entry.bytes) else {
            return false;
        };
        *reserved = total;
    }
    let reservations_match = ledger.managers.iter().all(|(manager_id, grant)| {
        grant.reserved_by_tier.iter().all(|(tier_id, bytes)| {
            expected_reserved
                .remove(&(manager_id.as_str(), tier_id.as_str()))
                .unwrap_or(0)
                == *bytes
        })
    }) && expected_reserved.is_empty();
    reservations_match
        && committed
            .zip(reserved)
            .and_then(|(committed, reserved)| committed.checked_add(reserved))
            .is_some()
}

fn next_quota_generation(next_generation: &mut u64) -> Option<u64> {
    let generation = next_generation.checked_add(1)?;
    *next_generation = generation;
    Some(generation)
}

fn advance_quota_revision(ledger: &mut QuotaLedger) -> Option<u64> {
    let revision = ledger.revision.checked_add(1)?;
    ledger.revision = revision;
    Some(revision)
}

fn next_policy_generation(generation: u64, changed: bool) -> Option<u64> {
    generation.checked_add(u64::from(changed))
}

#[derive(Clone, Copy, Debug)]
struct QuotaOptions {
    lease_duration: Duration,
    renew_interval: Duration,
}

impl Default for QuotaOptions {
    fn default() -> Self {
        Self {
            lease_duration: QUOTA_LEASE_DURATION,
            renew_interval: QUOTA_RENEW_INTERVAL,
        }
    }
}

struct QuotaCoordination {
    manager_id: String,
    ledger_path: ObjectPath,
    healthy: AtomicBool,
    ledger_revision: AtomicU64,
    lease_expiry_ms: AtomicU64,
    verified_bytes: AtomicU64,
    unverified_bytes: AtomicU64,
    reconciliation_lag: AtomicU64,
    reconciliation_pending: AtomicBool,
    reconciliation_retry_at_ms: AtomicU64,
    policy_sync_lag: AtomicU64,
    policy_sync_error: StdMutex<String>,
    latest_ledger: StdRwLock<Option<QuotaLedger>>,
    operation: Mutex<()>,
    cancel: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    options: QuotaOptions,
}

#[derive(Clone, Debug)]
struct PolicySnapshot {
    revision: u64,
    canonical: Vec<u8>,
    generation: u64,
    policies: Vec<CacheTierPolicy>,
}

impl fmt::Debug for QuotaCoordination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuotaCoordination")
            .field("manager_id", &self.manager_id)
            .field("ledger_path", &self.ledger_path)
            .finish_non_exhaustive()
    }
}

impl Drop for QuotaCoordination {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheManager {
    origin: Arc<dyn ObjectStore>,
    confidentiality: CacheConfidentiality,
    policy_store: Arc<dyn ObjectStore>,
    policy_path: ObjectPath,
    namespace: String,
    tiers: Vec<Arc<CacheTier>>,
    capacity: Arc<StdMutex<CapacityState>>,
    inflight: Arc<StdMutex<HashMap<String, Weak<Flight>>>>,
    policy_update: Arc<Mutex<()>>,
    policy_snapshot: Arc<StdRwLock<Arc<PolicySnapshot>>>,
    quota: Arc<QuotaCoordination>,
    aggregate_max_bytes: Option<u64>,
    max_inflight_bytes: u64,
    part_size_bytes: u64,
    background_budget: Arc<Semaphore>,
    background_task_budget: Arc<Semaphore>,
    background_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
    pending_deletions: Arc<StdMutex<HashMap<(usize, String), PendingDeletion>>>,
    deletion_wake: Arc<Notify>,
    deletion_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    closing: Arc<AtomicBool>,
    metrics: Arc<CacheMetrics>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct CacheTestInspection {
    tiers: Vec<Arc<CacheTier>>,
    quota: Arc<QuotaCoordination>,
    background_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
    deletion_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    closing: Arc<AtomicBool>,
}

#[cfg(test)]
pub(crate) struct CacheTestState {
    pub(crate) closing: bool,
    pub(crate) background_task_count: usize,
    pub(crate) ledger_lease_expired: bool,
    pub(crate) conservatively_fenced: bool,
    pub(crate) cache_write_count: u64,
}

#[cfg(test)]
impl CacheTestInspection {
    pub(crate) async fn state(&self) -> CacheTestState {
        let cache_fill_task_count = {
            self.background_tasks
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .iter()
                .filter(|task| !task.is_finished())
                .count()
        };
        let background_task_count = cache_fill_task_count
            + usize::from(
                self.quota
                    .task
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|task| !task.is_finished()),
            )
            + usize::from(
                self.deletion_task
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|task| !task.is_finished()),
            );
        let ledger_lease_expired = self
            .quota
            .latest_ledger
            .read()
            .unwrap_or_else(|lock| lock.into_inner())
            .as_ref()
            .and_then(|ledger| ledger.managers.get(&self.quota.manager_id))
            .is_none_or(|grant| grant.lease_expires_ms <= now_ms());
        CacheTestState {
            closing: self.closing.load(Ordering::Acquire),
            background_task_count,
            ledger_lease_expired,
            conservatively_fenced: !self.quota.healthy.load(Ordering::Acquire),
            cache_write_count: self
                .tiers
                .iter()
                .map(|tier| tier.metrics.write_latency_count.load(Ordering::Acquire))
                .sum(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_inspection(repository_identity: &str) -> CacheTestInspection {
    CACHE_TEST_INSPECTIONS
        .lock()
        .expect("cache test inspection lock")
        .get(repository_identity)
        .expect("cache test inspection")
        .clone()
}

#[cfg(test)]
pub(crate) fn has_test_inspection(repository_identity: &str) -> bool {
    CACHE_TEST_INSPECTIONS
        .lock()
        .expect("cache test inspection lock")
        .contains_key(repository_identity)
}

impl CacheManager {
    fn policy(&self) -> Arc<PolicySnapshot> {
        self.policy_snapshot
            .read()
            .unwrap_or_else(|lock| lock.into_inner())
            .clone()
    }

    fn disable_all_tiers(&self, error: &anyhow::Error) {
        let current = self.policy();
        let mut policies = current.policies.clone();
        for policy in &mut policies {
            policy.enabled = false;
        }
        *self
            .policy_snapshot
            .write()
            .unwrap_or_else(|lock| lock.into_inner()) = Arc::new(PolicySnapshot {
            revision: current.revision,
            canonical: current.canonical.clone(),
            generation: next_policy_generation(current.generation, true)
                .unwrap_or(current.generation),
            policies,
        });
        *self
            .quota
            .policy_sync_error
            .lock()
            .unwrap_or_else(|lock| lock.into_inner()) = format!("{error:#}");
        self.quota.policy_sync_lag.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) async fn new(
        origin: Arc<dyn ObjectStore>,
        policy_store: Arc<dyn ObjectStore>,
        config: CacheConfig,
        repository_identity: &str,
        database_identity: &str,
    ) -> Result<Self> {
        Self::new_with_quota_options(
            origin,
            policy_store,
            config,
            repository_identity,
            database_identity,
            QuotaOptions::default(),
        )
        .await
    }

    async fn new_with_quota_options(
        origin: Arc<dyn ObjectStore>,
        policy_store: Arc<dyn ObjectStore>,
        config: CacheConfig,
        repository_identity: &str,
        database_identity: &str,
        quota_options: QuotaOptions,
    ) -> Result<Self> {
        config.validate()?;
        let initial_policies = config
            .tiers
            .iter()
            .map(|tier| tier.policy.clone())
            .collect::<Vec<_>>();
        let namespace = cache_namespace(repository_identity, database_identity);
        let (cancel, _) = watch::channel(false);
        let quota = Arc::new(QuotaCoordination {
            manager_id: sha256_hex(rand::random::<[u8; 32]>()),
            ledger_path: quota_path(&namespace),
            healthy: AtomicBool::new(false),
            ledger_revision: AtomicU64::new(0),
            lease_expiry_ms: AtomicU64::new(0),
            verified_bytes: AtomicU64::new(0),
            unverified_bytes: AtomicU64::new(0),
            reconciliation_lag: AtomicU64::new(0),
            reconciliation_pending: AtomicBool::new(false),
            reconciliation_retry_at_ms: AtomicU64::new(0),
            policy_sync_lag: AtomicU64::new(0),
            policy_sync_error: StdMutex::new(String::new()),
            latest_ledger: StdRwLock::new(None),
            operation: Mutex::new(()),
            cancel,
            task: Mutex::new(None),
            options: quota_options,
        });
        let mut tiers = Vec::with_capacity(config.tiers.len());
        for tier in config.tiers {
            let tier_namespace = format!(
                "{namespace}/cache-data/{}/{}",
                tier.confidentiality.as_str(),
                tier.id
            );
            tiers.push(Arc::new(CacheTier {
                store: replica_store(&tier.store, &tier_namespace, &tier.id)?,
                id: tier.id,
                confidentiality: tier.confidentiality,
                metrics: CacheMetrics::default(),
                circuit: StdMutex::new(CircuitState::default()),
                reconciliation_lag: AtomicU64::new(0),
                inventory_reconciliation_failed: AtomicBool::new(false),
            }));
        }
        let initial_canonical = canonical_policy(0, &tiers, &initial_policies)?;
        let manager = Self {
            origin,
            confidentiality: CacheConfidentiality::Encrypted,
            policy_store,
            policy_path: policy_path(&namespace),
            namespace: namespace.clone(),
            tiers,
            capacity: Arc::new(StdMutex::new(CapacityState::default())),
            inflight: Arc::new(StdMutex::new(HashMap::new())),
            policy_update: Arc::new(Mutex::new(())),
            policy_snapshot: Arc::new(StdRwLock::new(Arc::new(PolicySnapshot {
                revision: 0,
                canonical: initial_canonical,
                generation: 0,
                policies: initial_policies.clone(),
            }))),
            quota,
            aggregate_max_bytes: config.aggregate_max_bytes,
            max_inflight_bytes: config.max_inflight_bytes,
            part_size_bytes: config.part_size_bytes,
            background_budget: Arc::new(Semaphore::new(config.max_inflight_bytes as usize)),
            background_task_budget: Arc::new(Semaphore::new(config.max_background_tasks)),
            background_tasks: Arc::new(StdMutex::new(Vec::new())),
            pending_deletions: Arc::new(StdMutex::new(HashMap::new())),
            deletion_wake: Arc::new(Notify::new()),
            deletion_task: Arc::new(Mutex::new(None)),
            closing: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(CacheMetrics::default()),
        };
        #[cfg(test)]
        CACHE_TEST_INSPECTIONS
            .lock()
            .expect("cache test inspection lock")
            .insert(
                repository_identity.to_owned(),
                CacheTestInspection {
                    tiers: manager.tiers.clone(),
                    quota: manager.quota.clone(),
                    background_tasks: manager.background_tasks.clone(),
                    deletion_task: manager.deletion_task.clone(),
                    closing: manager.closing.clone(),
                },
            );
        if let Err(error) = manager.initialize_policy().await {
            manager.disable_all_tiers(&error);
            manager.quota.healthy.store(false, Ordering::Release);
        }
        manager.initialize_quota().await;
        manager.enforce_limits().await;
        manager.enqueue_latest_pending_deletions();
        manager.start_quota_heartbeat().await;
        manager.start_deletion_worker().await;
        Ok(manager)
    }

    pub(crate) fn has_confidentiality(&self, confidentiality: CacheConfidentiality) -> bool {
        self.tiers
            .iter()
            .any(|tier| tier.confidentiality == confidentiality)
    }

    pub(crate) fn store(
        &self,
        origin: Arc<dyn ObjectStore>,
        confidentiality: CacheConfidentiality,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(Self {
            origin,
            confidentiality,
            policy_store: self.policy_store.clone(),
            policy_path: self.policy_path.clone(),
            namespace: self.namespace.clone(),
            tiers: self.tiers.clone(),
            capacity: self.capacity.clone(),
            inflight: self.inflight.clone(),
            policy_update: self.policy_update.clone(),
            policy_snapshot: self.policy_snapshot.clone(),
            quota: self.quota.clone(),
            aggregate_max_bytes: self.aggregate_max_bytes,
            max_inflight_bytes: self.max_inflight_bytes,
            part_size_bytes: self.part_size_bytes,
            background_budget: self.background_budget.clone(),
            background_task_budget: self.background_task_budget.clone(),
            background_tasks: self.background_tasks.clone(),
            pending_deletions: self.pending_deletions.clone(),
            deletion_wake: self.deletion_wake.clone(),
            deletion_task: self.deletion_task.clone(),
            closing: self.closing.clone(),
            metrics: self.metrics.clone(),
        })
    }

    pub(crate) fn status(&self) -> CacheStatus {
        let now = now_ms();
        let policy_snapshot = self.policy();
        let ledger_accounting = self
            .quota
            .latest_ledger
            .read()
            .unwrap_or_else(|lock| lock.into_inner())
            .as_ref()
            .map(|ledger| {
                let mut accounting = HashMap::<String, (u64, u64, u64)>::new();
                for entry in ledger.entries.values() {
                    let values = accounting.entry(entry.tier_id.clone()).or_default();
                    if entry.state != QuotaEntryState::Admitting {
                        values.0 = values.0.saturating_add(entry.bytes);
                    }
                    if entry.state == QuotaEntryState::Deleting {
                        values.2 = values.2.saturating_add(entry.bytes);
                    }
                }
                for grant in ledger.managers.values() {
                    for (tier_id, bytes) in &grant.reserved_by_tier {
                        let values = accounting.entry(tier_id.clone()).or_default();
                        values.1 = values.1.saturating_add(*bytes);
                    }
                }
                accounting
            });
        let state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let tiers = self
            .tiers
            .iter()
            .enumerate()
            .map(|(index, tier)| {
                let policy = policy_snapshot.policies[index].clone();
                let local_used = state.used_by_tier.get(&index).copied().unwrap_or(0);
                let local_reserved = state.reserved_by_tier.get(&index).copied().unwrap_or(0);
                let shared = ledger_accounting
                    .as_ref()
                    .and_then(|accounting| accounting.get(&tier.id));
                let used = ledger_accounting
                    .as_ref()
                    .map_or(local_used, |_| shared.map_or(0, |values| values.0));
                let reserved = ledger_accounting
                    .as_ref()
                    .map_or(local_reserved, |_| shared.map_or(0, |values| values.1));
                let deletion_pending_bytes = shared.map_or(0, |values| values.2);
                CacheTierStatus {
                    id: tier.id.clone(),
                    confidentiality: tier.confidentiality,
                    policy: policy.clone(),
                    metrics: tier.metrics.snapshot(),
                    used_bytes: used,
                    reserved_bytes: reserved,
                    local_used_bytes: local_used,
                    local_reserved_bytes: local_reserved,
                    pinned_bytes: state.pinned_by_tier.get(&index).copied().unwrap_or(0),
                    requested_max_bytes: policy.max_bytes,
                    deletion_pending_bytes,
                    deletion_pending_known: ledger_accounting.is_some(),
                    pending_reclaim_bytes: if policy.enabled {
                        used.saturating_sub(policy.max_bytes)
                    } else {
                        used
                    },
                    reconciliation_lag: tier.reconciliation_lag.load(Ordering::Acquire),
                    circuit_open: tier
                        .circuit
                        .lock()
                        .unwrap_or_else(|lock| lock.into_inner())
                        .open_until_ms
                        > now,
                }
            })
            .collect::<Vec<_>>();
        let shared_used = tiers
            .iter()
            .map(|tier| tier.used_bytes)
            .fold(0u64, u64::saturating_add);
        let shared_reserved = tiers
            .iter()
            .map(|tier| tier.reserved_bytes)
            .fold(0u64, u64::saturating_add);
        let deletion_pending_bytes = tiers
            .iter()
            .map(|tier| tier.deletion_pending_bytes)
            .fold(0u64, u64::saturating_add);
        let pending_reclaim_bytes = self
            .aggregate_max_bytes
            .map_or(0, |limit| shared_used.saturating_sub(limit))
            .max(
                tiers
                    .iter()
                    .map(|tier| tier.pending_reclaim_bytes)
                    .fold(0u64, u64::saturating_add),
            );
        CacheStatus {
            revision: policy_snapshot.revision,
            namespace: self.namespace.clone(),
            aggregate_max_bytes: self.aggregate_max_bytes,
            used_bytes: shared_used,
            reserved_bytes: shared_reserved,
            local_used_bytes: state.aggregate_used,
            local_reserved_bytes: state.aggregate_reserved,
            pinned_bytes: state
                .pinned_by_tier
                .values()
                .copied()
                .fold(0u64, u64::saturating_add),
            inflight_bytes: state.inflight_reserved,
            max_inflight_bytes: self.max_inflight_bytes,
            deletion_pending_bytes,
            deletion_pending_known: ledger_accounting.is_some(),
            pending_reclaim_bytes,
            quota_coordination_healthy: self.quota.healthy.load(Ordering::Acquire),
            quota_ledger_revision: self.quota.ledger_revision.load(Ordering::Acquire),
            quota_lease_expiry_ms: self.quota.lease_expiry_ms.load(Ordering::Acquire),
            unverified_stale_bytes: self.quota.unverified_bytes.load(Ordering::Acquire),
            quota_reconciliation_lag: self.quota.reconciliation_lag.load(Ordering::Acquire),
            policy_sync_lag: self.quota.policy_sync_lag.load(Ordering::Acquire),
            policy_sync_error: self
                .quota
                .policy_sync_error
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .clone(),
            metrics: self.metrics.snapshot(),
            tiers,
        }
    }

    pub(crate) async fn update_policy(
        &self,
        expected_revision: u64,
        updates: Vec<CacheTierPolicyUpdate>,
    ) -> Result<CacheStatus> {
        let _guard = self.policy_update.lock().await;
        let current = self.policy();
        if current.revision != expected_revision {
            bail!("read-cache policy revision mismatch");
        }
        let mut proposed = self
            .tiers
            .iter()
            .enumerate()
            .map(|(index, tier)| CacheTierPolicyUpdate {
                id: tier.id.clone(),
                policy: current.policies[index].clone(),
            })
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        for update in updates {
            update.policy.validate(&update.id)?;
            if !seen.insert(update.id.clone()) {
                bail!("duplicate read-cache tier policy update {:?}", update.id);
            }
            let current = proposed
                .iter_mut()
                .find(|current| current.id == update.id)
                .with_context(|| format!("unknown read-cache tier {:?}", update.id))?;
            *current = update;
        }
        let revision = expected_revision
            .checked_add(1)
            .context("read-cache policy revision overflow")?;
        let generation = next_policy_generation(current.generation, true)
            .context("read-cache policy generation exhausted")?;
        self.persist_policy(expected_revision, revision, &proposed)
            .await?;
        let next_policies = proposed
            .iter()
            .map(|update| update.policy.clone())
            .collect::<Vec<_>>();
        let canonical = canonical_policy(revision, &self.tiers, &next_policies)?;
        *self
            .policy_snapshot
            .write()
            .unwrap_or_else(|lock| lock.into_inner()) = Arc::new(PolicySnapshot {
            revision,
            canonical,
            generation,
            policies: next_policies,
        });
        if let Err(error) = self.sync_quota_policy(revision).await {
            self.quota.healthy.store(false, Ordering::Release);
            self.quota.policy_sync_lag.fetch_add(1, Ordering::AcqRel);
            *self
                .quota
                .policy_sync_error
                .lock()
                .unwrap_or_else(|lock| lock.into_inner()) = format!("{error:#}");
        }
        self.enforce_limits().await;
        Ok(self.status())
    }

    async fn initialize_policy(&self) -> Result<()> {
        match coordinated(self.policy_store.get(&self.policy_path)).await {
            Ok(_) => return self.load_policy().await,
            Err(slatedb::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error).context("read persisted cache policy"),
        }
        match coordinated(self.policy_store.get(&self.quota.ledger_path)).await {
            Err(slatedb::object_store::Error::NotFound { .. }) => {}
            Ok(result) => {
                let bytes = coordinated(result.bytes())
                    .await
                    .context("read cache quota ledger")?;
                let ledger: QuotaLedger =
                    serde_json::from_slice(&bytes).context("decode cache quota ledger")?;
                if ledger.format != QUOTA_FORMAT
                    || ledger.namespace != self.namespace
                    || ledger.revision != 0
                    || ledger.policy_revision != 0
                    || !ledger.managers.is_empty()
                    || !ledger.entries.is_empty()
                    || ledger.reconciler.is_some()
                {
                    bail!("missing read-cache policy is not safe to bootstrap");
                }
            }
            Err(error) => return Err(error).context("verify pristine cache quota ledger"),
        }
        let snapshot = self.policy();
        let tiers = self
            .tiers
            .iter()
            .enumerate()
            .map(|(index, tier)| CacheTierPolicyUpdate {
                id: tier.id.clone(),
                policy: snapshot.policies[index].clone(),
            })
            .collect();
        let bytes = serde_json::to_vec(&PersistedPolicy {
            format: CACHE_FORMAT,
            namespace: self.namespace.clone(),
            revision: 0,
            tiers,
        })
        .context("encode initial read-cache policy")?;
        match coordinated(self.policy_store.put_opts(
            &self.policy_path,
            bytes.into(),
            PutOptions::from(PutMode::Create),
        ))
        .await
        {
            Ok(_) => self.load_policy().await,
            Err(slatedb::object_store::Error::AlreadyExists { .. })
            | Err(slatedb::object_store::Error::Precondition { .. }) => self.load_policy().await,
            Err(error) => Err(error).context("persist initial read-cache policy"),
        }
    }

    async fn load_policy(&self) -> Result<()> {
        let _guard = self.policy_update.lock().await;
        self.load_policy_locked().await
    }

    async fn load_policy_locked(&self) -> Result<()> {
        let result = coordinated(self.policy_store.get(&self.policy_path))
            .await
            .context("read persisted cache policy")?;
        let bytes = coordinated(result.bytes())
            .await
            .context("read persisted cache policy bytes")?;
        let document: PersistedPolicy =
            serde_json::from_slice(&bytes).context("decode persisted cache policy")?;
        if document.format != CACHE_FORMAT || document.namespace != self.namespace {
            bail!("persisted read-cache policy identity mismatch");
        }
        if document.tiers.len() != self.tiers.len() {
            bail!("persisted policy does not contain every configured read-cache tier");
        }
        let document_revision = document.revision;
        let mut by_id = HashMap::with_capacity(document.tiers.len());
        for update in document.tiers {
            if by_id.insert(update.id.clone(), update.policy).is_some() {
                bail!("persisted policy contains duplicate read-cache tier");
            }
        }
        let mut policies = Vec::with_capacity(self.tiers.len());
        for tier in &self.tiers {
            let Some(policy) = by_id.remove(&tier.id) else {
                bail!("persisted policy contains unknown read-cache tier");
            };
            policy.validate(&tier.id)?;
            policies.push(policy);
        }
        let canonical = canonical_policy(document_revision, &self.tiers, &policies)?;
        let current = self.policy();
        let current_revision = current.revision;
        if document_revision < current_revision {
            bail!("persisted read-cache policy revision rolled back");
        }
        if document_revision == current_revision && current.canonical != canonical {
            bail!("persisted read-cache policy changed without a revision increase");
        }
        let changed = document_revision != current.revision || policies != current.policies;
        *self
            .policy_snapshot
            .write()
            .unwrap_or_else(|lock| lock.into_inner()) = Arc::new(PolicySnapshot {
            revision: document_revision,
            canonical,
            generation: next_policy_generation(current.generation, changed)
                .context("read-cache policy generation exhausted")?,
            policies,
        });
        self.quota.policy_sync_lag.store(0, Ordering::Release);
        self.quota
            .policy_sync_error
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .clear();
        Ok(())
    }

    async fn persist_policy(
        &self,
        expected_revision: u64,
        revision: u64,
        tiers: &[CacheTierPolicyUpdate],
    ) -> Result<()> {
        let result = match coordinated(self.policy_store.get(&self.policy_path)).await {
            Ok(result) => result,
            Err(slatedb::object_store::Error::NotFound { .. }) => {
                bail!("persisted read-cache policy is missing")
            }
            Err(error) => return Err(error).context("read persisted cache policy"),
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = coordinated(result.bytes())
            .await
            .context("read persisted cache policy bytes")?;
        let document: PersistedPolicy =
            serde_json::from_slice(&bytes).context("decode persisted cache policy")?;
        if document.format != CACHE_FORMAT
            || document.namespace != self.namespace
            || document.revision != expected_revision
            || document.tiers.len() != self.tiers.len()
        {
            bail!("read-cache policy revision or identity mismatch");
        }
        let mut by_id = HashMap::with_capacity(document.tiers.len());
        for update in document.tiers {
            update.policy.validate(&update.id)?;
            if by_id.insert(update.id, update.policy).is_some() {
                bail!("persisted policy contains duplicate read-cache tier");
            }
        }
        let mut current_policies = Vec::with_capacity(self.tiers.len());
        for tier in &self.tiers {
            let Some(policy) = by_id.remove(&tier.id) else {
                bail!("persisted policy does not contain every configured read-cache tier");
            };
            current_policies.push(policy);
        }
        if !by_id.is_empty() {
            bail!("persisted policy contains unknown read-cache tier");
        }
        let canonical = canonical_policy(expected_revision, &self.tiers, &current_policies)?;
        if self.policy().canonical != canonical {
            bail!("persisted read-cache policy differs from the installed snapshot");
        }
        let proposed = PersistedPolicy {
            format: CACHE_FORMAT,
            namespace: self.namespace.clone(),
            revision,
            tiers: tiers.to_vec(),
        };
        let bytes = serde_json::to_vec(&proposed).context("encode read-cache policy")?;
        match coordinated(self.policy_store.put_opts(
            &self.policy_path,
            bytes.into(),
            PutOptions::from(PutMode::Update(version)),
        ))
        .await
        {
            Ok(_) => Ok(()),
            Err(slatedb::object_store::Error::AlreadyExists { .. })
            | Err(slatedb::object_store::Error::Precondition { .. }) => {
                bail!("read-cache policy revision mismatch")
            }
            Err(error) => {
                let committed = match coordinated(self.policy_store.get(&self.policy_path)).await {
                    Ok(result) => match coordinated(result.bytes()).await {
                        Ok(bytes) => serde_json::from_slice::<PersistedPolicy>(&bytes)
                            .is_ok_and(|document| document == proposed),
                        Err(_) => false,
                    },
                    Err(_) => false,
                };
                if committed {
                    Ok(())
                } else {
                    Err(error).context("persist read-cache policy")
                }
            }
        }
    }

    async fn read_quota_ledger(&self) -> Result<(QuotaLedger, Option<UpdateVersion>)> {
        match coordinated(self.policy_store.get(&self.quota.ledger_path)).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = coordinated(result.bytes())
                    .await
                    .context("read cache quota ledger")?;
                let ledger: QuotaLedger =
                    serde_json::from_slice(&bytes).context("decode cache quota ledger")?;
                if ledger.format != QUOTA_FORMAT || ledger.namespace != self.namespace {
                    bail!("cache quota ledger identity mismatch");
                }
                if !quota_ledger_accounting_valid(&ledger) {
                    bail!("cache quota ledger accounting overflows");
                }
                if ledger.aggregate_max_bytes != self.aggregate_max_bytes {
                    bail!("cache quota aggregate limit does not match shared ledger");
                }
                *self
                    .quota
                    .latest_ledger
                    .write()
                    .unwrap_or_else(|lock| lock.into_inner()) = Some(ledger.clone());
                Ok((ledger, Some(version)))
            }
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok((
                QuotaLedger {
                    format: QUOTA_FORMAT,
                    namespace: self.namespace.clone(),
                    revision: 0,
                    next_generation: 0,
                    policy_revision: self.policy().revision,
                    aggregate_max_bytes: self.aggregate_max_bytes,
                    managers: HashMap::new(),
                    entries: HashMap::new(),
                    reconciler: None,
                },
                None,
            )),
            Err(error) => Err(error).context("read cache quota ledger"),
        }
    }

    async fn write_quota_ledger(
        &self,
        ledger: &QuotaLedger,
        version: Option<UpdateVersion>,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(ledger).context("encode cache quota ledger")?;
        let mode = version.map_or(PutMode::Create, PutMode::Update);
        coordinated(self.policy_store.put_opts(
            &self.quota.ledger_path,
            bytes.into(),
            PutOptions::from(mode),
        ))
        .await
        .context("persist cache quota ledger")?;
        *self
            .quota
            .latest_ledger
            .write()
            .unwrap_or_else(|lock| lock.into_inner()) = Some(ledger.clone());
        Ok(())
    }

    async fn initialize_quota(&self) {
        if self.initialize_quota_inner().await.is_err() {
            self.quota
                .reconciliation_pending
                .store(true, Ordering::Release);
            self.quota
                .reconciliation_retry_at_ms
                .store(0, Ordering::Release);
            self.quota.healthy.store(false, Ordering::Release);
            self.quota.reconciliation_lag.fetch_add(1, Ordering::AcqRel);
            self.update_unverified_bytes();
        }
    }

    async fn sync_quota_policy(&self, policy_revision: u64) -> Result<()> {
        let _operation = self.quota.operation.lock().await;
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            if ledger.policy_revision > policy_revision {
                bail!("cache quota policy revision is newer than local policy");
            }
            ledger.policy_revision = policy_revision;
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    if !self.quota.reconciliation_pending.load(Ordering::Acquire) {
                        self.quota.healthy.store(true, Ordering::Release);
                    }
                    return Ok(());
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota policy CAS retries exhausted")
    }

    pub(crate) fn begin_close(&self) {
        let _tasks = self
            .background_tasks
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.closing.store(true, Ordering::Release);
    }

    pub(crate) async fn close(&self) -> Result<()> {
        self.begin_close();
        let _ = self.quota.cancel.send(true);
        if let Some(task) = self.quota.task.lock().await.take() {
            let _ = task.await;
        }
        if let Some(task) = self.deletion_task.lock().await.take() {
            let _ = task.await;
        }
        self.rediscover_pending_deletions().await;
        let tasks = std::mem::take(
            &mut *self
                .background_tasks
                .lock()
                .unwrap_or_else(|lock| lock.into_inner()),
        );
        for task in tasks {
            let _ = task.await;
        }
        let _ = tokio::time::timeout(CLOSE_DELETION_DEADLINE, async {
            loop {
                self.process_pending_deletions(false).await;
                if self
                    .pending_deletions
                    .lock()
                    .unwrap_or_else(|lock| lock.into_inner())
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let pending_deletions = self.pending_deletions.lock().unwrap().len();
        let _operation = self.quota.operation.lock().await;
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            ledger.entries.retain(|_, entry| {
                entry.owner_manager_id != self.quota.manager_id
                    || entry.state != QuotaEntryState::Admitting
            });
            if let Some(grant) = ledger.managers.get_mut(&self.quota.manager_id) {
                grant.reserved_by_tier.clear();
                grant.lease_expires_ms = now_ms();
            }
            if ledger
                .reconciler
                .as_ref()
                .is_some_and(|reconciler| reconciler.manager_id == self.quota.manager_id)
            {
                ledger.reconciler = None;
            }
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    self.quota.lease_expiry_ms.store(0, Ordering::Release);
                    self.quota.healthy.store(false, Ordering::Release);
                    if pending_deletions != 0 {
                        bail!("cache shutdown left {pending_deletions} pending deletions");
                    }
                    return Ok(());
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota shutdown CAS retries exhausted")
    }

    async fn rediscover_pending_deletions(&self) {
        if let Ok((ledger, _)) = self.read_quota_ledger().await {
            enqueue_owned_deleting_entries(
                &self.tiers,
                &self.pending_deletions,
                &self.deletion_wake,
                &self.quota.manager_id,
                &ledger,
            );
        }
    }

    fn enqueue_latest_pending_deletions(&self) {
        let ledger = self
            .quota
            .latest_ledger
            .read()
            .unwrap_or_else(|lock| lock.into_inner())
            .clone();
        if let Some(ledger) = ledger {
            enqueue_owned_deleting_entries(
                &self.tiers,
                &self.pending_deletions,
                &self.deletion_wake,
                &self.quota.manager_id,
                &ledger,
            );
        }
    }

    async fn initialize_quota_inner(&self) -> Result<()> {
        let _operation = self.quota.operation.lock().await;
        let policy_snapshot = self.policy();
        let now = now_ms();
        let lease_expires_ms = now.saturating_add(duration_ms(self.quota.options.lease_duration));
        let mut acquired = false;
        let mut inventory_revision = 0;
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            if ledger.reconciler.as_ref().is_some_and(|reconciler| {
                reconciler.manager_id != self.quota.manager_id && reconciler.lease_expires_ms > now
            }) {
                bail!("cache quota reconciliation is owned by another manager");
            }
            ledger.reconciler = Some(QuotaReconciler {
                manager_id: self.quota.manager_id.clone(),
                lease_expires_ms,
            });
            ledger
                .managers
                .entry(self.quota.manager_id.clone())
                .or_insert_with(|| QuotaManagerGrant {
                    lease_expires_ms,
                    committed_by_tier: HashMap::new(),
                    reserved_by_tier: HashMap::new(),
                })
                .lease_expires_ms = lease_expires_ms;
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    inventory_revision = ledger.revision;
                    acquired = true;
                    break;
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        if !acquired {
            bail!("cache quota reconciliation CAS retries exhausted");
        }

        if !self.reconcile().await {
            bail!("cache quota inventory listing was incomplete");
        }
        let inventory = self.quota_inventory();
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            if ledger.revision != inventory_revision {
                bail!("cache quota ledger changed during inventory");
            }
            if !ledger.reconciler.as_ref().is_some_and(|reconciler| {
                reconciler.manager_id == self.quota.manager_id
                    && reconciler.lease_expires_ms >= now_ms()
            }) {
                bail!("cache quota reconciliation fence was lost");
            }
            let policy_revision = policy_snapshot.revision;
            if ledger.policy_revision > policy_revision {
                bail!("cache quota policy revision is newer than local policy");
            }
            for grant in ledger.managers.values_mut() {
                grant.committed_by_tier.clear();
                grant.reserved_by_tier.clear();
            }
            let previous_entries = ledger.entries.clone();
            let physical_ids = inventory.keys().cloned().collect::<HashSet<_>>();
            let mut entries = HashMap::with_capacity(inventory.len() + previous_entries.len());
            for (entry_id, previous) in &previous_entries {
                let preserve_admitting =
                    previous.state == QuotaEntryState::Admitting && previous.expires_ms > now;
                let preserve_cleanup = physical_ids.contains(entry_id)
                    && (previous.state == QuotaEntryState::Deleting
                        || inventory
                            .get(entry_id)
                            .is_some_and(|(_, _, _, complete)| !complete));
                if !preserve_admitting && !preserve_cleanup {
                    continue;
                }
                let mut preserved = previous.clone();
                if !preserve_admitting {
                    preserved.state = QuotaEntryState::Deleting;
                }
                let grant = ledger
                    .managers
                    .entry(preserved.owner_manager_id.clone())
                    .or_insert_with(|| QuotaManagerGrant {
                        lease_expires_ms: 0,
                        committed_by_tier: HashMap::new(),
                        reserved_by_tier: HashMap::new(),
                    });
                let accounting = if preserved.state == QuotaEntryState::Admitting {
                    &mut grant.reserved_by_tier
                } else {
                    &mut grant.committed_by_tier
                };
                *accounting.entry(preserved.tier_id.clone()).or_default() = accounting
                    .get(&preserved.tier_id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(preserved.bytes);
                entries.insert(entry_id.clone(), preserved);
            }
            for (entry_id, (tier_id, key, bytes, complete)) in &inventory {
                if entries.contains_key(entry_id) {
                    continue;
                }
                let owner_manager_id = ledger.entries.get(entry_id).map_or_else(
                    || self.quota.manager_id.clone(),
                    |entry| entry.owner_manager_id.clone(),
                );
                let grant = ledger
                    .managers
                    .entry(owner_manager_id.clone())
                    .or_insert_with(|| QuotaManagerGrant {
                        lease_expires_ms: 0,
                        committed_by_tier: HashMap::new(),
                        reserved_by_tier: HashMap::new(),
                    });
                *grant.committed_by_tier.entry(tier_id.clone()).or_default() = grant
                    .committed_by_tier
                    .get(tier_id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*bytes);
                entries.insert(
                    entry_id.clone(),
                    QuotaEntry {
                        key: key.clone(),
                        tier_id: tier_id.clone(),
                        owner_manager_id,
                        bytes: *bytes,
                        generation: if let Some(entry) = ledger.entries.get(entry_id) {
                            entry.generation
                        } else {
                            next_quota_generation(&mut ledger.next_generation)
                                .context("cache quota generation exhausted")?
                        },
                        policy_revision,
                        expires_ms: 0,
                        state: if *complete {
                            QuotaEntryState::Active
                        } else {
                            QuotaEntryState::Deleting
                        },
                    },
                );
            }
            ledger.entries = entries;
            ledger.policy_revision = policy_revision;
            ledger.reconciler = None;
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    self.quota
                        .lease_expiry_ms
                        .store(lease_expires_ms, Ordering::Release);
                    self.quota.verified_bytes.store(
                        ledger
                            .entries
                            .values()
                            .map(|entry| entry.bytes)
                            .fold(0u64, u64::saturating_add),
                        Ordering::Release,
                    );
                    self.quota.unverified_bytes.store(0, Ordering::Release);
                    self.quota.healthy.store(true, Ordering::Release);
                    return Ok(());
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota inventory CAS retries exhausted")
    }

    fn quota_inventory(&self) -> HashMap<String, (String, String, u64, bool)> {
        let state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        state
            .entries
            .iter()
            .map(|((index, key), entry)| {
                (
                    quota_entry_id(&self.tiers[*index], key),
                    (
                        self.tiers[*index].id.clone(),
                        key.clone(),
                        entry.bytes,
                        entry.data_present && entry.metadata_valid,
                    ),
                )
            })
            .collect()
    }

    fn update_unverified_bytes(&self) {
        let local_bytes = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .aggregate_used;
        let bytes = local_bytes.max(self.quota.verified_bytes.load(Ordering::Acquire));
        self.quota.unverified_bytes.store(bytes, Ordering::Release);
    }

    async fn start_quota_heartbeat(&self) {
        let weak_quota = Arc::downgrade(&self.quota);
        let store = self.policy_store.clone();
        let policy_path = self.policy_path.clone();
        let namespace = self.namespace.clone();
        let tier_ids = self
            .tiers
            .iter()
            .map(|tier| tier.id.clone())
            .collect::<Vec<_>>();
        let tiers = self.tiers.clone();
        let capacity = self.capacity.clone();
        let policy_snapshot = self.policy_snapshot.clone();
        let policy_update = self.policy_update.clone();
        let pending_deletions = self.pending_deletions.clone();
        let deletion_wake = self.deletion_wake.clone();
        let metrics = self.metrics.clone();
        let aggregate_max_bytes = self.aggregate_max_bytes;
        let mut cancelled = self.quota.cancel.subscribe();
        let interval = self.quota.options.renew_interval;
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = cancelled.changed() => {
                        if result.is_err() || *cancelled.borrow() {
                            return;
                        }
                    }
                    () = tokio::time::sleep(interval) => {}
                }
                let Some(quota) = weak_quota.upgrade() else {
                    return;
                };
                let policy_guard = policy_update.lock().await;
                let policy_revision = match read_policy_snapshot(
                    &store,
                    &policy_path,
                    &namespace,
                    &tier_ids,
                )
                .await
                {
                    Ok((next_revision, next_policies, canonical)) => {
                        let current = policy_snapshot
                            .read()
                            .unwrap_or_else(|lock| lock.into_inner())
                            .clone();
                        let current_revision = current.revision;
                        let equivocated =
                            next_revision == current_revision && current.canonical != canonical;
                        if next_revision < current_revision || equivocated {
                            let message = if equivocated {
                                "persisted read-cache policy changed without a revision increase"
                            } else {
                                "persisted read-cache policy revision rolled back"
                            };
                            let mut disabled = current.policies.clone();
                            for policy in &mut disabled {
                                policy.enabled = false;
                            }
                            *policy_snapshot
                                .write()
                                .unwrap_or_else(|lock| lock.into_inner()) =
                                Arc::new(PolicySnapshot {
                                    revision: current.revision,
                                    canonical: current.canonical.clone(),
                                    generation: next_policy_generation(current.generation, true)
                                        .unwrap_or(current.generation),
                                    policies: disabled,
                                });
                            quota.policy_sync_lag.fetch_add(1, Ordering::AcqRel);
                            *quota
                                .policy_sync_error
                                .lock()
                                .unwrap_or_else(|lock| lock.into_inner()) = message.to_owned();
                            None
                        } else {
                            let changed = next_revision != current.revision
                                || next_policies != current.policies;
                            let Some(generation) =
                                next_policy_generation(current.generation, changed)
                            else {
                                let mut disabled = current.policies.clone();
                                for policy in &mut disabled {
                                    policy.enabled = false;
                                }
                                *policy_snapshot
                                    .write()
                                    .unwrap_or_else(|lock| lock.into_inner()) =
                                    Arc::new(PolicySnapshot {
                                        revision: current.revision,
                                        canonical: current.canonical.clone(),
                                        generation: current.generation,
                                        policies: disabled,
                                    });
                                quota.policy_sync_lag.fetch_add(1, Ordering::AcqRel);
                                *quota
                                    .policy_sync_error
                                    .lock()
                                    .unwrap_or_else(|lock| lock.into_inner()) =
                                    "read-cache policy generation exhausted".to_owned();
                                drop(policy_guard);
                                continue;
                            };
                            let installed = Arc::new(PolicySnapshot {
                                revision: next_revision,
                                canonical,
                                generation,
                                policies: next_policies,
                            });
                            *policy_snapshot
                                .write()
                                .unwrap_or_else(|lock| lock.into_inner()) = installed.clone();
                            quota.policy_sync_lag.store(0, Ordering::Release);
                            quota
                                .policy_sync_error
                                .lock()
                                .unwrap_or_else(|lock| lock.into_inner())
                                .clear();
                            schedule_limit_enforcement(
                                &tiers,
                                &capacity,
                                &installed.policies,
                                aggregate_max_bytes,
                                &metrics,
                                &pending_deletions,
                                &deletion_wake,
                                POLICY_ENFORCEMENT_BATCH_SIZE,
                            );
                            Some(next_revision)
                        }
                    }
                    Err(error) => {
                        let current = policy_snapshot
                            .read()
                            .unwrap_or_else(|lock| lock.into_inner())
                            .clone();
                        let mut disabled = current.policies.clone();
                        for policy in &mut disabled {
                            policy.enabled = false;
                        }
                        *policy_snapshot
                            .write()
                            .unwrap_or_else(|lock| lock.into_inner()) = Arc::new(PolicySnapshot {
                            revision: current.revision,
                            canonical: current.canonical.clone(),
                            generation: next_policy_generation(current.generation, true)
                                .unwrap_or(current.generation),
                            policies: disabled,
                        });
                        quota.policy_sync_lag.fetch_add(1, Ordering::AcqRel);
                        *quota
                            .policy_sync_error
                            .lock()
                            .unwrap_or_else(|lock| lock.into_inner()) = format!("{error:#}");
                        None
                    }
                };
                drop(policy_guard);
                let renewal = renew_quota_lease(
                    &store,
                    &namespace,
                    aggregate_max_bytes,
                    policy_revision,
                    &quota,
                )
                .await;
                if matches!(renewal, Some(true)) {
                    quota.reconciliation_pending.store(true, Ordering::Release);
                }
                let retry_due =
                    quota.reconciliation_retry_at_ms.load(Ordering::Acquire) <= now_ms();
                if renewal.is_none()
                    || (quota.reconciliation_pending.load(Ordering::Acquire) && retry_due)
                {
                    let current = policy_snapshot
                        .read()
                        .unwrap_or_else(|lock| lock.into_inner())
                        .clone();
                    let recovered = recover_quota_coordination(
                        &store,
                        &namespace,
                        aggregate_max_bytes,
                        &tiers,
                        &current,
                        &capacity,
                        &quota,
                        &pending_deletions,
                        &deletion_wake,
                    )
                    .await;
                    if !recovered {
                        quota.reconciliation_pending.store(true, Ordering::Release);
                        quota.reconciliation_retry_at_ms.store(
                            now_ms().saturating_add(duration_ms(interval)),
                            Ordering::Release,
                        );
                    }
                }
                let latest_ledger = quota
                    .latest_ledger
                    .read()
                    .unwrap_or_else(|lock| lock.into_inner())
                    .clone();
                if let Some(ledger) = latest_ledger {
                    enqueue_owned_deleting_entries(
                        &tiers,
                        &pending_deletions,
                        &deletion_wake,
                        &quota.manager_id,
                        &ledger,
                    );
                }
            }
        });
        *self.quota.task.lock().await = Some(task);
    }

    async fn reconcile(&self) -> bool {
        let policy_snapshot = self.policy();
        let mut complete = true;
        {
            let mut state = self
                .capacity
                .lock()
                .unwrap_or_else(|lock| lock.into_inner());
            *state = CapacityState::default();
        }
        for (index, tier) in self.tiers.iter().enumerate() {
            let mut tier_complete = true;
            let namespace = cache_domain_namespace(&self.namespace, tier.confidentiality);
            let mut listed = tier.store.list(Some(&ObjectPath::from("entries")));
            loop {
                match tokio::time::timeout(
                    Duration::from_millis(policy_snapshot.policies[index].timeout_ms),
                    listed.next(),
                )
                .await
                {
                    Ok(Some(Ok(meta))) if meta.location.as_ref().ends_with(".meta") => {
                        let key = meta
                            .location
                            .as_ref()
                            .trim_end_matches(".meta")
                            .trim_start_matches("entries/")
                            .to_owned();
                        self.reconcile_object(index, &key, meta.size, None, false);
                        let result = tokio::time::timeout(
                            Duration::from_millis(policy_snapshot.policies[index].timeout_ms),
                            tier.store.get(&meta.location),
                        )
                        .await;
                        if let Ok(Ok(result)) = result {
                            if let Ok(Ok(bytes)) = tokio::time::timeout(
                                Duration::from_millis(policy_snapshot.policies[index].timeout_ms),
                                result.bytes(),
                            )
                            .await
                            {
                                if let Ok(sidecar) = serde_json::from_slice::<CacheSidecar>(&bytes)
                                {
                                    if sidecar.format == CACHE_FORMAT
                                        && sidecar.namespace == namespace
                                    {
                                        self.reconcile_object(
                                            index,
                                            &key,
                                            0,
                                            Some((sidecar.created_ms, sidecar.accessed_ms)),
                                            false,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Ok(Some(Ok(meta))) => {
                        let key = meta
                            .location
                            .as_ref()
                            .trim_end_matches(".data")
                            .trim_start_matches("entries/")
                            .to_owned();
                        self.reconcile_object(index, &key, meta.size, None, true);
                    }
                    Ok(Some(Err(_))) | Err(_) => {
                        tier.inventory_reconciliation_failed
                            .store(true, Ordering::Release);
                        tier.reconciliation_lag.store(1, Ordering::Release);
                        tier_complete = false;
                        complete = false;
                        break;
                    }
                    Ok(None) => break,
                }
            }
            let tier_has_pending_deletions = self
                .pending_deletions
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .values()
                .any(|pending| pending.index == index);
            if tier_complete && !tier_has_pending_deletions {
                tier.inventory_reconciliation_failed
                    .store(false, Ordering::Release);
                tier.reconciliation_lag.store(0, Ordering::Release);
            }
        }
        complete
    }

    fn reconcile_object(
        &self,
        index: usize,
        key: &str,
        bytes: u64,
        timestamps: Option<(u64, u64)>,
        data_present: bool,
    ) {
        let mut state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let entry = state
            .entries
            .entry((index, key.to_owned()))
            .or_insert(EntryState {
                bytes: 0,
                created_ms: 0,
                accessed_ms: 0,
                data_present: false,
                metadata_valid: false,
                pins: 0,
                retiring: false,
                generation: 0,
            });
        entry.bytes = entry.bytes.saturating_add(bytes);
        if let Some((created_ms, accessed_ms)) = timestamps {
            entry.created_ms = created_ms;
            entry.accessed_ms = accessed_ms;
            entry.metadata_valid = true;
        }
        entry.data_present |= data_present;
        state.aggregate_used = state.aggregate_used.saturating_add(bytes);
        let used = state.used_by_tier.entry(index).or_default();
        *used = used.saturating_add(bytes);
    }

    async fn get_cached(
        &self,
        confidentiality: CacheConfidentiality,
        namespace: &str,
        location: &ObjectPath,
        options: &GetOptions,
        key: &str,
        policy_snapshot: &PolicySnapshot,
    ) -> Option<(GetResult, usize)> {
        let mut order = self
            .tiers
            .iter()
            .enumerate()
            .filter_map(|(index, tier)| {
                let policy = &policy_snapshot.policies[index];
                (tier.confidentiality == confidentiality && policy.enabled && !circuit_open(tier))
                    .then_some((policy.read_priority, index))
            })
            .collect::<Vec<_>>();
        order.sort_by_key(|(priority, _)| *priority);
        for (_, index) in order {
            if self
                .capacity
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .entries
                .get(&(index, key.to_owned()))
                .is_some_and(|entry| entry.retiring)
            {
                continue;
            }
            if let Some(result) = self
                .read_tier(
                    index,
                    namespace,
                    location,
                    options,
                    key,
                    &policy_snapshot.policies[index],
                )
                .await
            {
                return Some((result, index));
            }
        }
        None
    }

    async fn read_tier(
        &self,
        index: usize,
        namespace: &str,
        location: &ObjectPath,
        options: &GetOptions,
        key: &str,
        policy: &CacheTierPolicy,
    ) -> Option<GetResult> {
        let tier = &self.tiers[index];
        let started = Instant::now();
        self.pin(index, key);
        let operation = async {
            let result = tier.store.get(&entry_path(key, "meta")).await?;
            let metadata = result.bytes().await?;
            let sidecar: CacheSidecar = serde_json::from_slice(&metadata)
                .map_err(|_| cache_error("invalid cache sidecar"))?;
            if sidecar.format != CACHE_FORMAT
                || sidecar.namespace != namespace
                || sidecar.origin_path != location.as_ref()
                || sidecar.request_identity != request_identity(options)
                || expired(
                    &sidecar,
                    policy,
                    now_ms(),
                    self.capacity
                        .lock()
                        .unwrap_or_else(|lock| lock.into_inner())
                        .entries
                        .get(&(index, key.to_owned()))
                        .map(|entry| entry.accessed_ms),
                )
            {
                return Err(cache_error("cache sidecar mismatch or expiry"));
            }
            let payload = tier
                .store
                .get(&entry_path(key, "data"))
                .await?
                .bytes()
                .await?;
            if payload.len() as u64 != sidecar.length || sha256_hex(&payload) != sidecar.sha256 {
                return Err(cache_error("cache payload integrity mismatch"));
            }
            Ok::<_, slatedb::object_store::Error>((sidecar, payload))
        };
        let result =
            tokio::time::timeout(Duration::from_millis(policy.timeout_ms), operation).await;
        if self.unpin(index, key) {
            self.retire_entry(index, key).await;
        }
        record_latency(
            &tier.metrics.read_latency_total_us,
            &tier.metrics.read_latency_count,
            started,
        );
        match result {
            Ok(Ok((mut sidecar, payload))) => {
                circuit_success(tier);
                sidecar.accessed_ms = now_ms();
                self.touch(index, key, sidecar.accessed_ms);
                tier.metrics.hits.fetch_add(1, Ordering::AcqRel);
                tier.metrics
                    .origin_reads_avoided
                    .fetch_add(1, Ordering::AcqRel);
                self.metrics.hits.fetch_add(1, Ordering::AcqRel);
                self.metrics
                    .origin_reads_avoided
                    .fetch_add(1, Ordering::AcqRel);
                let Ok(last_modified) = sidecar.last_modified.parse() else {
                    self.evict(index, key, EvictionReason::Corruption).await;
                    return None;
                };
                Some(GetResult {
                    payload: GetResultPayload::Stream(stream::once(async { Ok(payload) }).boxed()),
                    meta: ObjectMeta {
                        location: ObjectPath::from(sidecar.location),
                        last_modified,
                        size: sidecar.size,
                        e_tag: sidecar.e_tag,
                        version: sidecar.version,
                    },
                    range: sidecar.response_start..sidecar.response_end,
                    attributes: decode_attributes(&sidecar.attributes)?,
                    extensions: options.extensions.clone(),
                })
            }
            Ok(Err(slatedb::object_store::Error::NotFound { .. })) => {
                tier.metrics.misses.fetch_add(1, Ordering::AcqRel);
                None
            }
            Ok(Err(error)) => {
                tier.metrics.misses.fetch_add(1, Ordering::AcqRel);
                if matches!(
                    error,
                    slatedb::object_store::Error::Generic {
                        store: "vaultic read cache",
                        ..
                    }
                ) {
                    circuit_failure(tier, false);
                    tier.metrics.corruptions.fetch_add(1, Ordering::AcqRel);
                    self.metrics.corruptions.fetch_add(1, Ordering::AcqRel);
                    self.evict(index, key, EvictionReason::Corruption).await;
                } else {
                    circuit_failure(tier, false);
                    tier.metrics.failures.fetch_add(1, Ordering::AcqRel);
                    self.metrics.failures.fetch_add(1, Ordering::AcqRel);
                }
                None
            }
            Err(_) => {
                circuit_failure(tier, true);
                tier.metrics.misses.fetch_add(1, Ordering::AcqRel);
                tier.metrics.timeouts.fetch_add(1, Ordering::AcqRel);
                self.metrics.timeouts.fetch_add(1, Ordering::AcqRel);
                None
            }
        }
    }

    async fn admit(&self, index: usize, key: &str, sidecar: &CacheSidecar, payload: &Bytes) {
        let tier = &self.tiers[index];
        if !self.quota.healthy.load(Ordering::Acquire) || self.load_policy().await.is_err() {
            self.quota.healthy.store(false, Ordering::Release);
            self.update_unverified_bytes();
            return;
        }
        let policy_snapshot = self.policy();
        let policy = &policy_snapshot.policies[index];
        if !policy.enabled || circuit_open(tier) {
            return;
        }
        let Ok(metadata) = serde_json::to_vec(sidecar) else {
            return;
        };
        let permanent = (payload.len() as u64).saturating_add(metadata.len() as u64);
        let reservation_bytes = permanent;
        self.evict_until_fits(index, reservation_bytes, policy)
            .await;
        let Some(reservation) = self
            .reserve_with_policy(index, key, permanent, reservation_bytes, &policy_snapshot)
            .await
        else {
            tier.metrics
                .admission_rejections
                .fetch_add(1, Ordering::AcqRel);
            self.metrics
                .admission_rejections
                .fetch_add(1, Ordering::AcqRel);
            tier.metrics
                .admission_rejections_reservation
                .fetch_add(1, Ordering::AcqRel);
            self.metrics
                .admission_rejections_reservation
                .fetch_add(1, Ordering::AcqRel);
            return;
        };
        let started = Instant::now();
        let operation = async {
            tier.store
                .put(&entry_path(key, "data"), payload.clone().into())
                .await?;
            tier.store
                .put(&entry_path(key, "meta"), metadata.into())
                .await?;
            Ok::<_, slatedb::object_store::Error>(())
        };
        let result =
            tokio::time::timeout(Duration::from_millis(policy.timeout_ms), operation).await;
        record_latency(
            &tier.metrics.write_latency_total_us,
            &tier.metrics.write_latency_count,
            started,
        );
        match result {
            Ok(Ok(())) => {
                match self
                    .commit_reservation(
                        &reservation,
                        key,
                        permanent,
                        sidecar.created_ms,
                        sidecar.accessed_ms,
                    )
                    .await
                {
                    AdmissionCommit::Active => {
                        circuit_success(tier);
                        tier.metrics.admissions.fetch_add(1, Ordering::AcqRel);
                        self.metrics.admissions.fetch_add(1, Ordering::AcqRel);
                    }
                    AdmissionCommit::Rejected => {
                        self.abort_admission(reservation, key).await;
                    }
                    AdmissionCommit::Retiring => {}
                }
            }
            Ok(Err(_)) => {
                self.abort_admission(reservation, key).await;
                circuit_failure(tier, false);
                tier.metrics.failures.fetch_add(1, Ordering::AcqRel);
                self.metrics.failures.fetch_add(1, Ordering::AcqRel);
            }
            Err(_) => {
                self.abort_admission(reservation, key).await;
                circuit_failure(tier, true);
                tier.metrics.timeouts.fetch_add(1, Ordering::AcqRel);
                self.metrics.timeouts.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    async fn abort_admission(&self, reservation: Reservation, key: &str) {
        self.release_local_reservation(reservation.index, reservation.bytes);
        match self
            .mark_shared_entry_deleting_generation(
                reservation.index,
                key,
                Some(reservation.generation),
            )
            .await
        {
            Ok(Some(generation)) => {
                self.enqueue_pending_deletion(reservation.index, key, Some(generation));
            }
            Ok(None) | Err(_) => {
                self.quota.healthy.store(false, Ordering::Release);
                self.enqueue_pending_deletion(reservation.index, key, Some(reservation.generation));
                self.update_unverified_bytes();
            }
        }
    }

    async fn promote(
        &self,
        confidentiality: CacheConfidentiality,
        source_index: usize,
        key: &str,
        sidecar: &CacheSidecar,
        payload: &Bytes,
    ) {
        let policy_snapshot = self.policy();
        let source_priority = policy_snapshot.policies[source_index].admission_priority;
        let mut targets = self
            .tiers
            .iter()
            .enumerate()
            .filter_map(|(index, tier)| {
                let policy = &policy_snapshot.policies[index];
                (index != source_index
                    && tier.confidentiality == confidentiality
                    && policy.enabled
                    && policy.admission_priority < source_priority)
                    .then_some((policy.admission_priority, index))
            })
            .collect::<Vec<_>>();
        targets.sort_by_key(|(priority, _)| *priority);
        for (_, index) in targets {
            self.admit(index, key, sidecar, payload).await;
        }
    }

    async fn evict_until_fits(&self, index: usize, reservation: u64, policy: &CacheTierPolicy) {
        loop {
            let victim = {
                let state = self
                    .capacity
                    .lock()
                    .unwrap_or_else(|lock| lock.into_inner());
                let tier_used = state.used_by_tier.get(&index).copied().unwrap_or(0);
                let tier_reserved = state.reserved_by_tier.get(&index).copied().unwrap_or(0);
                let aggregate_fits = self.aggregate_max_bytes.is_none_or(|limit| {
                    state
                        .aggregate_used
                        .saturating_add(state.aggregate_reserved)
                        .saturating_add(reservation)
                        <= limit
                });
                if tier_used
                    .saturating_add(tier_reserved)
                    .saturating_add(reservation)
                    <= policy.max_bytes
                    && aggregate_fits
                {
                    None
                } else {
                    state
                        .entries
                        .iter()
                        .filter(|((tier_index, _), entry)| *tier_index == index && entry.pins == 0)
                        .min_by_key(|(_, entry)| entry.accessed_ms)
                        .map(|((_, key), _)| key.clone())
                }
            };
            let Some(victim) = victim else {
                break;
            };
            self.evict(index, &victim, EvictionReason::Capacity).await;
        }
    }

    #[cfg(test)]
    async fn reserve(
        &self,
        index: usize,
        key: &str,
        permanent: u64,
        reservation: u64,
        _policy: &CacheTierPolicy,
    ) -> Option<Reservation> {
        if !self.quota.healthy.load(Ordering::Acquire) || self.load_policy().await.is_err() {
            self.quota.healthy.store(false, Ordering::Release);
            self.update_unverified_bytes();
            return None;
        }
        let policy_snapshot = self.policy();
        self.reserve_with_policy(index, key, permanent, reservation, &policy_snapshot)
            .await
    }

    async fn reserve_with_policy(
        &self,
        index: usize,
        key: &str,
        permanent: u64,
        reservation: u64,
        policy_snapshot: &PolicySnapshot,
    ) -> Option<Reservation> {
        let current_policy = &policy_snapshot.policies[index];
        if permanent > current_policy.max_bytes
            || reservation > self.max_inflight_bytes
            || self
                .aggregate_max_bytes
                .is_some_and(|limit| permanent > limit)
        {
            return None;
        }
        if !current_policy.enabled || permanent > current_policy.max_bytes {
            return None;
        }
        let policy_generation = policy_snapshot.generation;
        let quota_entry_id = quota_entry_id(&self.tiers[index], key);
        let reserved_locally = {
            let mut state = self
                .capacity
                .lock()
                .unwrap_or_else(|lock| lock.into_inner());
            let tier_used = state.used_by_tier.get(&index).copied().unwrap_or(0);
            let tier_reserved = state.reserved_by_tier.get(&index).copied().unwrap_or(0);
            if state.inflight_reserved.saturating_add(reservation) > self.max_inflight_bytes
                || tier_used
                    .saturating_add(tier_reserved)
                    .saturating_add(reservation)
                    > current_policy.max_bytes
                || self.aggregate_max_bytes.is_some_and(|limit| {
                    state
                        .aggregate_used
                        .saturating_add(state.aggregate_reserved)
                        .saturating_add(reservation)
                        > limit
                })
            {
                false
            } else {
                state.inflight_reserved = state.inflight_reserved.saturating_add(reservation);
                state.aggregate_reserved = state.aggregate_reserved.saturating_add(reservation);
                *state.reserved_by_tier.entry(index).or_default() =
                    tier_reserved.saturating_add(reservation);
                true
            }
        };
        if !reserved_locally {
            return None;
        }
        let (generation, policy_revision) = match self
            .reserve_shared(index, key, &quota_entry_id, reservation, policy_snapshot)
            .await
        {
            Ok(Some(reservation)) => reservation,
            Ok(None) => {
                self.release_local_reservation(index, reservation);
                return None;
            }
            Err(_) => {
                self.release_local_reservation(index, reservation);
                self.quota.healthy.store(false, Ordering::Release);
                self.update_unverified_bytes();
                return None;
            }
        };
        if policy_generation != self.policy().generation {
            let stale = Reservation {
                index,
                bytes: reservation,
                generation,
                policy_revision,
                policy_generation,
                quota_entry_id,
            };
            self.release_reservation(stale).await;
            return None;
        }
        Some(Reservation {
            index,
            bytes: reservation,
            generation,
            policy_revision,
            policy_generation,
            quota_entry_id,
        })
    }

    fn release_local_reservation(&self, index: usize, bytes: u64) {
        let mut state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        state.inflight_reserved = state.inflight_reserved.saturating_sub(bytes);
        state.aggregate_reserved = state.aggregate_reserved.saturating_sub(bytes);
        let reserved = state.reserved_by_tier.entry(index).or_default();
        *reserved = reserved.saturating_sub(bytes);
    }

    async fn release_reservation(&self, reservation: Reservation) {
        self.release_local_reservation(reservation.index, reservation.bytes);
        if self.release_shared_reservation(&reservation).await.is_err() {
            self.quota.healthy.store(false, Ordering::Release);
        }
    }

    async fn reserve_shared(
        &self,
        index: usize,
        key: &str,
        entry_id: &str,
        bytes: u64,
        policy_snapshot: &PolicySnapshot,
    ) -> Result<Option<(u64, u64)>> {
        let _operation = self.quota.operation.lock().await;
        let tier_id = &self.tiers[index].id;
        let policy = &policy_snapshot.policies[index];
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            if policy_snapshot.generation != self.policy().generation {
                return Ok(None);
            }
            let (mut ledger, version) = self.read_quota_ledger().await?;
            let now = now_ms();
            if ledger.policy_revision != policy_snapshot.revision
                || ledger
                    .reconciler
                    .as_ref()
                    .is_some_and(|reconciler| reconciler.lease_expires_ms > now)
                || ledger.entries.contains_key(entry_id)
            {
                return Ok(None);
            }
            let lease_is_current = ledger
                .managers
                .get(&self.quota.manager_id)
                .is_some_and(|grant| grant.lease_expires_ms >= now);
            if !lease_is_current {
                return Ok(None);
            }
            let tier_committed = ledger
                .entries
                .values()
                .filter(|entry| {
                    entry.tier_id == *tier_id && entry.state != QuotaEntryState::Admitting
                })
                .map(|entry| entry.bytes)
                .try_fold(0u64, |total, bytes| total.checked_add(bytes));
            let tier_reserved = ledger
                .managers
                .values()
                .map(|grant| grant.reserved_by_tier.get(tier_id).copied().unwrap_or(0))
                .try_fold(0u64, |total, bytes| total.checked_add(bytes));
            let aggregate_committed = ledger
                .entries
                .values()
                .filter(|entry| entry.state != QuotaEntryState::Admitting)
                .map(|entry| entry.bytes)
                .try_fold(0u64, |total, bytes| total.checked_add(bytes));
            let aggregate_reserved = ledger
                .managers
                .values()
                .flat_map(|grant| grant.reserved_by_tier.values())
                .copied()
                .try_fold(0u64, |total, bytes| total.checked_add(bytes));
            let tier_requested = tier_committed
                .zip(tier_reserved)
                .and_then(|(committed, reserved)| committed.checked_add(reserved))
                .and_then(|used| used.checked_add(bytes));
            let aggregate_requested = aggregate_committed
                .zip(aggregate_reserved)
                .and_then(|(committed, reserved)| committed.checked_add(reserved))
                .and_then(|used| used.checked_add(bytes));
            if tier_requested.is_none_or(|used| used > policy.max_bytes)
                || aggregate_requested.is_none()
                || self
                    .aggregate_max_bytes
                    .is_some_and(|limit| aggregate_requested.is_none_or(|used| used > limit))
            {
                return Ok(None);
            }
            let grant = ledger
                .managers
                .get_mut(&self.quota.manager_id)
                .context("cache quota manager grant disappeared")?;
            let reserved = grant.reserved_by_tier.entry(tier_id.clone()).or_default();
            *reserved = reserved
                .checked_add(bytes)
                .context("cache quota reservation overflow")?;
            let generation = next_quota_generation(&mut ledger.next_generation)
                .context("cache quota generation exhausted")?;
            let policy_revision = ledger.policy_revision;
            ledger.entries.insert(
                entry_id.to_owned(),
                QuotaEntry {
                    key: key.to_owned(),
                    tier_id: tier_id.clone(),
                    owner_manager_id: self.quota.manager_id.clone(),
                    bytes,
                    generation,
                    policy_revision,
                    expires_ms: now.saturating_add(duration_ms(self.quota.options.lease_duration)),
                    state: QuotaEntryState::Admitting,
                },
            );
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    if !self.quota.reconciliation_pending.load(Ordering::Acquire) {
                        self.quota.healthy.store(true, Ordering::Release);
                    }
                    return Ok(Some((generation, policy_revision)));
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota reservation CAS retries exhausted")
    }

    async fn release_shared_reservation(&self, reservation: &Reservation) -> Result<()> {
        let _operation = self.quota.operation.lock().await;
        let tier_id = &self.tiers[reservation.index].id;
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            let releasable = ledger
                .entries
                .get(&reservation.quota_entry_id)
                .is_some_and(|entry| {
                    entry.owner_manager_id == self.quota.manager_id
                        && entry.generation == reservation.generation
                        && entry.state == QuotaEntryState::Admitting
                });
            if !releasable {
                return Ok(());
            }
            ledger.entries.remove(&reservation.quota_entry_id);
            if let Some(grant) = ledger.managers.get_mut(&self.quota.manager_id) {
                let reserved = grant.reserved_by_tier.entry(tier_id.clone()).or_default();
                *reserved = reserved.saturating_sub(reservation.bytes);
            }
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    self.quota.verified_bytes.store(
                        ledger
                            .entries
                            .values()
                            .map(|entry| entry.bytes)
                            .fold(0u64, u64::saturating_add),
                        Ordering::Release,
                    );
                    return Ok(());
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota release CAS retries exhausted")
    }

    async fn commit_shared_reservation(
        &self,
        reservation: &Reservation,
        bytes: u64,
    ) -> Result<u64> {
        let _operation = self.quota.operation.lock().await;
        let policy_snapshot = self.policy();
        let tier_id = self.tiers[reservation.index].id.clone();
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            if ledger
                .reconciler
                .as_ref()
                .is_some_and(|reconciler| reconciler.lease_expires_ms > now_ms())
            {
                tokio::task::yield_now().await;
                continue;
            }
            if ledger.policy_revision != policy_snapshot.revision {
                bail!("cache quota policy revision changed during admission");
            }
            if ledger.policy_revision != reservation.policy_revision {
                bail!("cache quota policy revision changed during admission");
            }
            let admitting = ledger
                .entries
                .get(&reservation.quota_entry_id)
                .is_some_and(|entry| {
                    entry.owner_manager_id == self.quota.manager_id
                        && entry.generation == reservation.generation
                        && entry.policy_revision == reservation.policy_revision
                        && entry.state == QuotaEntryState::Admitting
                });
            if !admitting {
                bail!("cache quota admission generation was lost before commit");
            }
            let grant = ledger
                .managers
                .get_mut(&self.quota.manager_id)
                .context("cache quota manager grant disappeared")?;
            let reserved = grant.reserved_by_tier.entry(tier_id.clone()).or_default();
            if *reserved < reservation.bytes {
                bail!("cache quota reservation was lost before commit");
            }
            *reserved -= reservation.bytes;
            let committed = grant.committed_by_tier.entry(tier_id.clone()).or_default();
            *committed = committed.saturating_add(bytes);
            let entry = ledger
                .entries
                .get_mut(&reservation.quota_entry_id)
                .expect("admitting entry checked above");
            entry.bytes = bytes;
            entry.expires_ms = 0;
            entry.state = QuotaEntryState::Active;
            let generation = entry.generation;
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    self.quota.verified_bytes.store(
                        ledger
                            .entries
                            .values()
                            .map(|entry| entry.bytes)
                            .fold(0u64, u64::saturating_add),
                        Ordering::Release,
                    );
                    return Ok(generation);
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota commit CAS retries exhausted")
    }

    #[cfg(test)]
    async fn mark_shared_entry_deleting(&self, index: usize, key: &str) -> Result<Option<u64>> {
        self.mark_shared_entry_deleting_generation(index, key, None)
            .await
    }

    async fn mark_shared_entry_deleting_generation(
        &self,
        index: usize,
        key: &str,
        expected_generation: Option<u64>,
    ) -> Result<Option<u64>> {
        let _operation = self.quota.operation.lock().await;
        let entry_id = quota_entry_id(&self.tiers[index], key);
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            if ledger
                .reconciler
                .as_ref()
                .is_some_and(|reconciler| reconciler.lease_expires_ms > now_ms())
            {
                bail!("cache quota reconciliation is active");
            }
            let Some(entry) = ledger.entries.get(&entry_id) else {
                return Ok(expected_generation);
            };
            if expected_generation.is_some_and(|expected| entry.generation != expected) {
                return Ok(None);
            }
            let owner_is_live = ledger
                .managers
                .get(&entry.owner_manager_id)
                .is_some_and(|owner| owner.lease_expires_ms >= now_ms());
            if entry.owner_manager_id != self.quota.manager_id && owner_is_live {
                return Ok(None);
            }
            let entry = ledger
                .entries
                .get_mut(&entry_id)
                .expect("entry checked above");
            let generation = entry.generation;
            let owner_manager_id = entry.owner_manager_id.clone();
            let tier_id = entry.tier_id.clone();
            let bytes = entry.bytes;
            let was_admitting = entry.state == QuotaEntryState::Admitting;
            entry.state = QuotaEntryState::Deleting;
            entry.expires_ms = 0;
            if was_admitting {
                let grant =
                    ledger
                        .managers
                        .entry(owner_manager_id)
                        .or_insert_with(|| QuotaManagerGrant {
                            lease_expires_ms: 0,
                            committed_by_tier: HashMap::new(),
                            reserved_by_tier: HashMap::new(),
                        });
                let reserved = grant.reserved_by_tier.entry(tier_id.clone()).or_default();
                *reserved = reserved.saturating_sub(bytes);
                let committed = grant.committed_by_tier.entry(tier_id).or_default();
                *committed = committed.saturating_add(bytes);
            }
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    return Ok(Some(generation));
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota delete-mark CAS retries exhausted")
    }

    async fn remove_shared_entry(&self, index: usize, key: &str, generation: u64) -> Result<bool> {
        let _operation = self.quota.operation.lock().await;
        let entry_id = quota_entry_id(&self.tiers[index], key);
        for _ in 0..QUOTA_CAS_ATTEMPTS {
            let (mut ledger, version) = self.read_quota_ledger().await?;
            if ledger
                .reconciler
                .as_ref()
                .is_some_and(|reconciler| reconciler.lease_expires_ms > now_ms())
            {
                bail!("cache quota reconciliation is active");
            }
            let Some(entry) = ledger.entries.get(&entry_id) else {
                return Ok(true);
            };
            if entry.generation != generation || entry.state != QuotaEntryState::Deleting {
                return Ok(false);
            }
            let previous = ledger
                .entries
                .remove(&entry_id)
                .expect("entry checked above");
            if let Some(owner) = ledger.managers.get_mut(&previous.owner_manager_id) {
                let committed = owner.committed_by_tier.entry(previous.tier_id).or_default();
                *committed = committed.saturating_sub(previous.bytes);
            }
            advance_quota_revision(&mut ledger).context("cache quota revision exhausted")?;
            match self.write_quota_ledger(&ledger, version).await {
                Ok(()) => {
                    self.quota
                        .ledger_revision
                        .store(ledger.revision, Ordering::Release);
                    self.quota.verified_bytes.store(
                        ledger
                            .entries
                            .values()
                            .map(|entry| entry.bytes)
                            .fold(0u64, u64::saturating_add),
                        Ordering::Release,
                    );
                    return Ok(true);
                }
                Err(error) if quota_conflict(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        bail!("cache quota deletion CAS retries exhausted")
    }

    async fn commit_reservation(
        &self,
        reservation: &Reservation,
        key: &str,
        bytes: u64,
        created_ms: u64,
        accessed_ms: u64,
    ) -> AdmissionCommit {
        let policy_snapshot = self.policy();
        let policy = &policy_snapshot.policies[reservation.index];
        if reservation.policy_generation != policy_snapshot.generation || !policy.enabled {
            return AdmissionCommit::Rejected;
        }
        let Ok(generation) = self.commit_shared_reservation(reservation, bytes).await else {
            return AdmissionCommit::Rejected;
        };
        self.finish_committed_reservation(
            reservation,
            key,
            bytes,
            created_ms,
            accessed_ms,
            generation,
        )
        .await
    }

    async fn finish_committed_reservation(
        &self,
        reservation: &Reservation,
        key: &str,
        bytes: u64,
        created_ms: u64,
        accessed_ms: u64,
        generation: u64,
    ) -> AdmissionCommit {
        let policy_snapshot = self.policy();
        let should_retire = {
            let mut state = self
                .capacity
                .lock()
                .unwrap_or_else(|lock| lock.into_inner());
            state.inflight_reserved = state.inflight_reserved.saturating_sub(reservation.bytes);
            state.aggregate_reserved = state.aggregate_reserved.saturating_sub(reservation.bytes);
            let reserved = state.reserved_by_tier.entry(reservation.index).or_default();
            *reserved = reserved.saturating_sub(reservation.bytes);
            let previous = state
                .entries
                .get(&(reservation.index, key.to_owned()))
                .map_or(0, |entry| entry.bytes);
            let tier_used = state
                .used_by_tier
                .get(&reservation.index)
                .copied()
                .unwrap_or(0);
            let next_tier = tier_used.saturating_sub(previous).saturating_add(bytes);
            let next_aggregate = state
                .aggregate_used
                .saturating_sub(previous)
                .saturating_add(bytes);
            let policy = &policy_snapshot.policies[reservation.index];
            let should_retire = reservation.policy_generation != policy_snapshot.generation
                || !policy.enabled
                || next_tier > policy.max_bytes
                || self
                    .aggregate_max_bytes
                    .is_some_and(|limit| next_aggregate > limit);
            self.commit_entry_locked(
                &mut state,
                reservation.index,
                key,
                bytes,
                created_ms,
                accessed_ms,
                generation,
            );
            should_retire
        };
        if should_retire {
            self.evict(reservation.index, key, EvictionReason::Capacity)
                .await;
            return AdmissionCommit::Retiring;
        }
        AdmissionCommit::Active
    }

    #[cfg(test)]
    fn commit_entry(&self, index: usize, key: &str, bytes: u64, created_ms: u64, accessed_ms: u64) {
        let mut state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.commit_entry_locked(&mut state, index, key, bytes, created_ms, accessed_ms, 0);
    }

    fn commit_entry_locked(
        &self,
        state: &mut CapacityState,
        index: usize,
        key: &str,
        bytes: u64,
        created_ms: u64,
        accessed_ms: u64,
        generation: u64,
    ) {
        if let Some(previous) = state.entries.insert(
            (index, key.to_owned()),
            EntryState {
                bytes,
                created_ms,
                accessed_ms,
                data_present: true,
                metadata_valid: true,
                pins: 0,
                retiring: false,
                generation,
            },
        ) {
            state.aggregate_used = state.aggregate_used.saturating_sub(previous.bytes);
            let used = state.used_by_tier.entry(index).or_default();
            *used = used.saturating_sub(previous.bytes);
        }
        state.aggregate_used = state.aggregate_used.saturating_add(bytes);
        let used = state.used_by_tier.entry(index).or_default();
        *used = used.saturating_add(bytes);
    }

    fn pin(&self, index: usize, key: &str) {
        let mut state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let bytes = state
            .entries
            .entry((index, key.to_owned()))
            .or_insert(EntryState {
                bytes: 0,
                created_ms: 0,
                accessed_ms: 0,
                data_present: false,
                metadata_valid: false,
                pins: 0,
                retiring: false,
                generation: 0,
            });
        let bytes = {
            let entry = bytes;
            entry.pins = entry.pins.saturating_add(1);
            entry.bytes
        };
        if bytes != 0 {
            let pinned = state.pinned_by_tier.entry(index).or_default();
            *pinned = pinned.saturating_add(bytes);
        }
    }

    fn unpin(&self, index: usize, key: &str) -> bool {
        let mut state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let entry_key = (index, key.to_owned());
        let (bytes, delete) = state
            .entries
            .get_mut(&entry_key)
            .map_or((0, false), |entry| {
                if entry.pins == 0 {
                    (0, false)
                } else {
                    entry.pins -= 1;
                    (entry.bytes, entry.pins == 0 && entry.retiring)
                }
            });
        if bytes != 0 {
            let pinned = state.pinned_by_tier.entry(index).or_default();
            *pinned = pinned.saturating_sub(bytes);
        }
        if delete {
            return true;
        }
        if state
            .entries
            .get(&entry_key)
            .is_some_and(|entry| entry.pins == 0 && entry.bytes == 0)
        {
            state.entries.remove(&entry_key);
        }
        false
    }

    fn touch(&self, index: usize, key: &str, accessed_ms: u64) {
        if let Some(entry) = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .entries
            .get_mut(&(index, key.to_owned()))
        {
            if !entry.retiring {
                entry.accessed_ms = accessed_ms;
            }
        }
    }

    async fn evict(&self, index: usize, key: &str, reason: EvictionReason) {
        let (marked, should_delete) = {
            let mut state = self
                .capacity
                .lock()
                .unwrap_or_else(|lock| lock.into_inner());
            let entry_key = (index, key.to_owned());
            match state.entries.get_mut(&entry_key) {
                Some(entry) => {
                    let marked = !entry.retiring;
                    entry.retiring = true;
                    (marked, entry.pins == 0)
                }
                None => (false, false),
            }
        };
        if marked {
            eviction_counter(&self.tiers[index].metrics, reason).fetch_add(1, Ordering::AcqRel);
            eviction_counter(&self.metrics, reason).fetch_add(1, Ordering::AcqRel);
        }
        if should_delete {
            self.retire_entry(index, key).await;
        }
    }

    async fn retire_entry(&self, index: usize, key: &str) {
        let generation = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .entries
            .get(&(index, key.to_owned()))
            .and_then(|entry| (entry.generation != 0).then_some(entry.generation));
        self.enqueue_pending_deletion(index, key, generation);
    }

    fn enqueue_pending_deletion(&self, index: usize, key: &str, generation: Option<u64>) {
        self.pending_deletions
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .entry((index, key.to_owned()))
            .or_insert_with(|| PendingDeletion {
                index,
                key: key.to_owned(),
                generation,
                retry_at_ms: 0,
                backoff: DELETION_BACKOFF_INITIAL,
            });
        self.deletion_wake.notify_one();
    }

    async fn start_deletion_worker(&self) {
        let manager = self.clone();
        let mut cancelled = self.quota.cancel.subscribe();
        let task = tokio::spawn(async move {
            loop {
                let retry_delay = manager.next_deletion_retry_delay();
                match retry_delay {
                    Some(delay) => {
                        tokio::select! {
                            result = cancelled.changed() => {
                                if result.is_err() || *cancelled.borrow() {
                                    return;
                                }
                            }
                            () = manager.deletion_wake.notified() => {}
                            () = tokio::time::sleep(delay) => {}
                        }
                    }
                    None => {
                        tokio::select! {
                            result = cancelled.changed() => {
                                if result.is_err() || *cancelled.borrow() {
                                    return;
                                }
                            }
                            () = manager.deletion_wake.notified() => {}
                        }
                    }
                }
                manager.process_pending_deletions(true).await;
            }
        });
        *self.deletion_task.lock().await = Some(task);
    }

    fn next_deletion_retry_delay(&self) -> Option<Duration> {
        let now = now_ms();
        self.pending_deletions
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .values()
            .map(|pending| Duration::from_millis(pending.retry_at_ms.saturating_sub(now)))
            .min()
    }

    async fn process_pending_deletions(&self, stop_on_cancel: bool) {
        let now = now_ms();
        let work = self
            .pending_deletions
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .values()
            .filter(|pending| pending.retry_at_ms <= now)
            .take(DELETION_BATCH_SIZE)
            .cloned()
            .collect::<Vec<_>>();
        for mut pending in work {
            if stop_on_cancel && *self.quota.cancel.borrow() {
                break;
            }
            let marked = self
                .mark_shared_entry_deleting_generation(
                    pending.index,
                    &pending.key,
                    pending.generation,
                )
                .await;
            let generation = match marked {
                Ok(Some(generation)) => generation,
                Ok(None) | Err(_) => {
                    self.defer_pending_deletion(&mut pending);
                    continue;
                }
            };
            if self.delete_objects(pending.index, &pending.key).await
                && self
                    .finalize_delete(pending.index, &pending.key, generation)
                    .await
            {
                let index = pending.index;
                self.pending_deletions
                    .lock()
                    .unwrap_or_else(|lock| lock.into_inner())
                    .remove(&(pending.index, pending.key));
                let tier_has_pending_deletions = self
                    .pending_deletions
                    .lock()
                    .unwrap_or_else(|lock| lock.into_inner())
                    .values()
                    .any(|pending| pending.index == index);
                if !tier_has_pending_deletions
                    && !self.tiers[index]
                        .inventory_reconciliation_failed
                        .load(Ordering::Acquire)
                {
                    self.tiers[index]
                        .reconciliation_lag
                        .store(0, Ordering::Release);
                }
            } else {
                pending.generation = Some(generation);
                self.defer_pending_deletion(&mut pending);
            }
        }
    }

    fn defer_pending_deletion(&self, pending: &mut PendingDeletion) {
        self.tiers[pending.index]
            .reconciliation_lag
            .store(1, Ordering::Release);
        pending.retry_at_ms = now_ms().saturating_add(duration_ms(pending.backoff));
        pending.backoff = pending.backoff.saturating_mul(2).min(DELETION_BACKOFF_MAX);
        self.pending_deletions
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .insert((pending.index, pending.key.clone()), pending.clone());
        self.update_unverified_bytes();
    }

    async fn finalize_delete(&self, index: usize, key: &str, generation: u64) -> bool {
        if !matches!(
            self.remove_shared_entry(index, key, generation).await,
            Ok(true)
        ) {
            self.quota.healthy.store(false, Ordering::Release);
            self.update_unverified_bytes();
            return false;
        }
        let mut state = self
            .capacity
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let entry_key = (index, key.to_owned());
        let matches_generation = state
            .entries
            .get(&entry_key)
            .is_some_and(|entry| entry.generation == generation || entry.generation == 0);
        if matches_generation {
            let entry = state
                .entries
                .remove(&entry_key)
                .expect("entry checked above");
            state.aggregate_used = state.aggregate_used.saturating_sub(entry.bytes);
            let used = state.used_by_tier.entry(index).or_default();
            *used = used.saturating_sub(entry.bytes);
        }
        true
    }

    async fn delete_objects(&self, index: usize, key: &str) -> bool {
        let tier = &self.tiers[index];
        let policy_snapshot = self.policy();
        let timeout = Duration::from_millis(policy_snapshot.policies[index].timeout_ms);
        let mut deleted = true;
        for suffix in ["data", "meta"] {
            let path = entry_path(key, suffix);
            match tokio::time::timeout(timeout, tier.store.delete(&path)).await {
                Ok(Ok(())) | Ok(Err(slatedb::object_store::Error::NotFound { .. })) => {}
                Ok(Err(_)) | Err(_) => deleted = false,
            }
            match tokio::time::timeout(timeout, tier.store.head(&path)).await {
                Ok(Err(slatedb::object_store::Error::NotFound { .. })) => {}
                Ok(Ok(_)) | Ok(Err(_)) | Err(_) => deleted = false,
            }
        }
        deleted
    }

    async fn evict_key(&self, key: &str) {
        for index in 0..self.tiers.len() {
            self.evict(index, key, EvictionReason::Corruption).await;
        }
    }

    async fn enforce_limits(&self) {
        let policy_snapshot = self.policy();
        loop {
            let victim = {
                let now = now_ms();
                let state = self
                    .capacity
                    .lock()
                    .unwrap_or_else(|lock| lock.into_inner());
                state
                    .entries
                    .iter()
                    .filter(|(_, entry)| !entry.retiring)
                    .filter_map(|((index, key), entry)| {
                        let policy = &policy_snapshot.policies[*index];
                        let used = state.used_by_tier.get(index).copied().unwrap_or(0);
                        let aggregate_over = self
                            .aggregate_max_bytes
                            .is_some_and(|limit| state.aggregate_used > limit);
                        let reason = if !policy.enabled
                            || policy
                                .absolute_age_ms
                                .is_some_and(|age| now.saturating_sub(entry.created_ms) >= age)
                        {
                            EvictionReason::Absolute
                        } else if policy.idle_age_ms != 0
                            && now.saturating_sub(entry.accessed_ms) >= policy.idle_age_ms
                        {
                            EvictionReason::Idle
                        } else if used > policy.max_bytes || aggregate_over {
                            EvictionReason::Capacity
                        } else {
                            return None;
                        };
                        Some((*index, key.clone(), reason, entry.accessed_ms))
                    })
                    .min_by_key(|(_, _, _, accessed)| *accessed)
            };
            let Some((index, key, reason, _)) = victim else {
                break;
            };
            self.evict(index, &key, reason).await;
        }
    }

    fn flight(&self, key: &str) -> (Arc<Flight>, bool) {
        let mut flights = self
            .inflight
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if let Some(flight) = flights.get(key).and_then(Weak::upgrade) {
            return (flight, false);
        }
        let flight = Arc::new(Flight::default());
        flights.insert(key.to_owned(), Arc::downgrade(&flight));
        (flight, true)
    }

    fn complete_flight(&self, flight: &Arc<Flight>, result: Option<SharedOriginResult>) {
        *flight
            .result
            .lock()
            .unwrap_or_else(|lock| lock.into_inner()) = result;
        flight.complete.store(true, Ordering::Release);
        flight.notify.notify_waiters();
    }

    fn remove_flight(&self, key: &str, flight: &Arc<Flight>) {
        let mut flights = self
            .inflight
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if flights
            .get(key)
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, flight))
        {
            flights.remove(key);
        }
        flights.retain(|_, weak| weak.strong_count() != 0);
    }

    async fn wait_for_flight(&self, flight: &Flight) -> Option<SharedOriginResult> {
        loop {
            if let Some(result) = flight
                .result
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .clone()
            {
                return Some(result);
            }
            if flight.complete.load(Ordering::Acquire) {
                return None;
            }
            flight.notify.notified().await;
        }
    }

    async fn spawn_background<F>(&self, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let Ok(permit) = self.background_task_budget.clone().try_acquire_owned() else {
            return false;
        };
        let mut tasks = self
            .background_tasks
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if self.closing.load(Ordering::Acquire) {
            return false;
        }
        let task = tokio::spawn(async move {
            let _permit = permit;
            future.await;
        });
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
        true
    }
}

impl fmt::Display for CacheManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "tiered read cache ({})", self.origin)
    }
}

#[async_trait]
impl ObjectStore for CacheManager {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        self.origin.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        self.origin.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        let operation_policy = self.policy();
        if self.closing.load(Ordering::Acquire) {
            self.metrics.bypasses.fetch_add(1, Ordering::AcqRel);
            self.metrics.origin_reads.fetch_add(1, Ordering::AcqRel);
            return self.origin.get_opts(location, options).await;
        }
        let confidentiality = self.confidentiality;
        let namespace = cache_domain_namespace(&self.namespace, confidentiality);
        let key = cache_key(&namespace, location, &options);
        if retry_read(&options) {
            self.evict_key(&key).await;
            self.metrics.bypasses.fetch_add(1, Ordering::AcqRel);
            self.metrics.origin_reads.fetch_add(1, Ordering::AcqRel);
            return self.origin.get_opts(location, options).await;
        }
        if !cacheable_read(&options) {
            self.metrics.bypasses.fetch_add(1, Ordering::AcqRel);
            self.metrics.origin_reads.fetch_add(1, Ordering::AcqRel);
            return self.origin.get_opts(location, options).await;
        }
        self.enforce_limits().await;
        if let Some((result, source_index)) = self
            .get_cached(
                confidentiality,
                &namespace,
                location,
                &options,
                &key,
                &operation_policy,
            )
            .await
        {
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = result.extensions.clone();
            let payload = result.bytes().await?;
            let sidecar = make_sidecar(
                &namespace,
                location,
                &options,
                &meta,
                range.clone(),
                &attributes,
                &payload,
            );
            if payload.len() as u64 <= self.part_size_bytes {
                if let Ok(permit) = self
                    .background_budget
                    .clone()
                    .try_acquire_many_owned(payload.len().try_into().unwrap_or(u32::MAX))
                {
                    let manager = self.clone();
                    let promotion_payload = payload.clone();
                    self.spawn_background(async move {
                        let _permit = permit;
                        manager
                            .promote(
                                confidentiality,
                                source_index,
                                &key,
                                &sidecar,
                                &promotion_payload,
                            )
                            .await;
                    })
                    .await;
                }
            }
            return Ok(result_from_bytes(
                payload, meta, range, attributes, extensions,
            ));
        }
        self.metrics.misses.fetch_add(1, Ordering::AcqRel);
        let (flight, leader) = self.flight(&key);
        if !leader {
            if let Some(shared) = self.wait_for_flight(&flight).await {
                self.metrics
                    .origin_reads_avoided
                    .fetch_add(1, Ordering::AcqRel);
                return Ok(result_from_bytes(
                    shared.payload,
                    shared.meta,
                    shared.range,
                    shared.attributes,
                    shared.extensions,
                ));
            }
        }
        if let Some((result, _)) = self
            .get_cached(
                confidentiality,
                &namespace,
                location,
                &options,
                &key,
                &operation_policy,
            )
            .await
        {
            return Ok(result);
        }
        self.metrics.origin_reads.fetch_add(1, Ordering::AcqRel);
        let result = match self.origin.get_opts(location, options.clone()).await {
            Ok(result) => result,
            Err(error) => {
                self.complete_flight(&flight, None);
                self.remove_flight(&key, &flight);
                return Err(error);
            }
        };
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let payload = match result.bytes().await {
            Ok(payload) => payload,
            Err(error) => {
                self.complete_flight(&flight, None);
                self.remove_flight(&key, &flight);
                return Err(error);
            }
        };
        self.complete_flight(
            &flight,
            Some(SharedOriginResult {
                payload: payload.clone(),
                meta: meta.clone(),
                range: range.clone(),
                attributes: attributes.clone(),
                extensions: extensions.clone(),
            }),
        );
        if payload.len() as u64 > self.part_size_bytes {
            self.remove_flight(&key, &flight);
            self.metrics.bypasses.fetch_add(1, Ordering::AcqRel);
            return Ok(result_from_bytes(
                payload, meta, range, attributes, extensions,
            ));
        }
        let sidecar = make_sidecar(
            &namespace,
            location,
            &options,
            &meta,
            range.clone(),
            &attributes,
            &payload,
        );
        let mut order = self
            .tiers
            .iter()
            .enumerate()
            .filter_map(|(index, tier)| {
                let policy = &operation_policy.policies[index];
                (tier.confidentiality == confidentiality && policy.enabled)
                    .then_some((policy.admission_priority, index))
            })
            .collect::<Vec<_>>();
        order.sort_by_key(|(priority, _)| *priority);
        if let Ok(permit) = self
            .background_budget
            .clone()
            .try_acquire_many_owned(payload.len().try_into().unwrap_or(u32::MAX))
        {
            let manager = self.clone();
            let fill_flight = flight.clone();
            let fill_key = key.clone();
            let admission_payload = payload.clone();
            if !self
                .spawn_background(async move {
                    let _permit = permit;
                    for (_, index) in order {
                        manager
                            .admit(index, &fill_key, &sidecar, &admission_payload)
                            .await;
                    }
                    manager.enforce_limits().await;
                    manager.remove_flight(&fill_key, &fill_flight);
                })
                .await
            {
                self.remove_flight(&key, &flight);
                self.metrics
                    .admission_rejections
                    .fetch_add(1, Ordering::AcqRel);
                self.metrics
                    .admission_rejections_background_task
                    .fetch_add(1, Ordering::AcqRel);
            }
        } else {
            self.remove_flight(&key, &flight);
            self.metrics
                .admission_rejections
                .fetch_add(1, Ordering::AcqRel);
            self.metrics
                .admission_rejections_background_budget
                .fetch_add(1, Ordering::AcqRel);
        }
        Ok(result_from_bytes(
            payload, meta, range, attributes, extensions,
        ))
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> slatedb::object_store::Result<Vec<Bytes>> {
        self.origin.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectPath>> {
        self.origin.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.origin.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.origin.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> slatedb::object_store::Result<ListResult> {
        self.origin.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.origin.copy_opts(from, to, options).await
    }
}

#[derive(Clone, Copy)]
enum EvictionReason {
    Capacity,
    Idle,
    Absolute,
    Corruption,
}

fn retry_read(options: &GetOptions) -> bool {
    ObjectStoreCallTag::from_extensions(&options.extensions).is_some_and(|tag| {
        tag.sst_type == SstType::Compacted
            && tag.retry.is_some()
            && matches!(tag.kind, TableStoreKind::Main | TableStoreKind::Reader)
    })
}

fn cache_namespace(repository_identity: &str, database_identity: &str) -> String {
    let identity = sha256_hex(format!(
        "format={CACHE_FORMAT}\0repository={repository_identity}\0database={database_identity}"
    ));
    format!("{CACHE_PREFIX}/v{CACHE_FORMAT}/{identity}")
}

fn cache_domain_namespace(namespace: &str, confidentiality: CacheConfidentiality) -> String {
    format!("{namespace}/{}", confidentiality.as_str())
}

fn policy_path(namespace: &str) -> ObjectPath {
    ObjectPath::from(format!("{POLICY_PREFIX}/{}.json", sha256_hex(namespace)))
}

fn quota_path(namespace: &str) -> ObjectPath {
    ObjectPath::from(format!("{QUOTA_PREFIX}/{}.json", sha256_hex(namespace)))
}

fn quota_entry_id(tier: &CacheTier, key: &str) -> String {
    format!("{}/{}/{key}", tier.confidentiality.as_str(), tier.id)
}

async fn coordinated<T, F>(future: F) -> slatedb::object_store::Result<T>
where
    F: Future<Output = slatedb::object_store::Result<T>>,
{
    tokio::time::timeout(COORDINATION_TIMEOUT, future)
        .await
        .map_err(|_| cache_error("cache coordination timeout"))?
}

fn canonical_policy(
    revision: u64,
    tiers: &[Arc<CacheTier>],
    policies: &[CacheTierPolicy],
) -> Result<Vec<u8>> {
    serde_json::to_vec(&(
        revision,
        tiers
            .iter()
            .map(|tier| tier.id.clone())
            .zip(policies.iter().cloned())
            .collect::<Vec<_>>(),
    ))
    .context("encode canonical read-cache policy")
}

async fn read_policy_snapshot(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    namespace: &str,
    tier_ids: &[String],
) -> Result<(u64, Vec<CacheTierPolicy>, Vec<u8>)> {
    let result = coordinated(store.get(path))
        .await
        .context("read persisted cache policy")?;
    let bytes = coordinated(result.bytes())
        .await
        .context("read persisted cache policy bytes")?;
    let document: PersistedPolicy =
        serde_json::from_slice(&bytes).context("decode persisted cache policy")?;
    if document.format != CACHE_FORMAT || document.namespace != namespace {
        bail!("persisted read-cache policy identity mismatch");
    }
    if document.tiers.len() != tier_ids.len() {
        bail!("persisted policy does not contain every configured read-cache tier");
    }
    let mut by_id = HashMap::with_capacity(document.tiers.len());
    for update in document.tiers {
        if by_id.insert(update.id.clone(), update.policy).is_some() {
            bail!("persisted policy contains duplicate read-cache tier");
        }
    }
    let mut policies = Vec::with_capacity(tier_ids.len());
    for id in tier_ids {
        let policy = by_id
            .remove(id)
            .with_context(|| format!("persisted policy is missing read-cache tier {id:?}"))?;
        policy.validate(id)?;
        policies.push(policy);
    }
    let canonical = serde_json::to_vec(&(
        document.revision,
        tier_ids
            .iter()
            .cloned()
            .zip(policies.iter().cloned())
            .collect::<Vec<_>>(),
    ))
    .context("encode canonical read-cache policy")?;
    Ok((document.revision, policies, canonical))
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn quota_conflict(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<slatedb::object_store::Error>()
            .is_some_and(|error| {
                matches!(
                    error,
                    slatedb::object_store::Error::AlreadyExists { .. }
                        | slatedb::object_store::Error::Precondition { .. }
                )
            })
    })
}

async fn renew_quota_lease(
    store: &Arc<dyn ObjectStore>,
    namespace: &str,
    aggregate_max_bytes: Option<u64>,
    policy_revision: Option<u64>,
    quota: &Arc<QuotaCoordination>,
) -> Option<bool> {
    let _operation = quota.operation.lock().await;
    let now = now_ms();
    for _ in 0..QUOTA_CAS_ATTEMPTS {
        let result = match coordinated(store.get(&quota.ledger_path)).await {
            Ok(result) => result,
            Err(_) => break,
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = match coordinated(result.bytes()).await {
            Ok(bytes) => bytes,
            Err(_) => break,
        };
        let mut ledger = match serde_json::from_slice::<QuotaLedger>(&bytes) {
            Ok(ledger)
                if ledger.format == QUOTA_FORMAT
                    && ledger.namespace == namespace
                    && ledger.aggregate_max_bytes == aggregate_max_bytes
                    && quota_ledger_accounting_valid(&ledger) =>
            {
                ledger
            }
            _ => break,
        };
        let Some(grant) = ledger.managers.get_mut(&quota.manager_id) else {
            break;
        };
        if grant.lease_expires_ms < now {
            break;
        }
        let lease_expires_ms = now.saturating_add(duration_ms(quota.options.lease_duration));
        grant.lease_expires_ms = lease_expires_ms;
        let reconciliation_needed = ledger.managers.iter().any(|(manager_id, grant)| {
            manager_id != &quota.manager_id && grant.lease_expires_ms < now
        }) || ledger
            .entries
            .values()
            .any(|entry| entry.state == QuotaEntryState::Admitting && entry.expires_ms <= now);
        if let Some(policy_revision) = policy_revision {
            if policy_revision < ledger.policy_revision {
                break;
            }
            ledger.policy_revision = policy_revision;
        }
        let Some(_) = advance_quota_revision(&mut ledger) else {
            break;
        };
        let encoded = match serde_json::to_vec(&ledger) {
            Ok(encoded) => encoded,
            Err(_) => break,
        };
        match coordinated(store.put_opts(
            &quota.ledger_path,
            encoded.into(),
            PutOptions::from(PutMode::Update(version)),
        ))
        .await
        {
            Ok(_) => {
                quota
                    .ledger_revision
                    .store(ledger.revision, Ordering::Release);
                quota
                    .lease_expiry_ms
                    .store(lease_expires_ms, Ordering::Release);
                quota.verified_bytes.store(
                    ledger
                        .entries
                        .values()
                        .map(|entry| entry.bytes)
                        .fold(0u64, u64::saturating_add),
                    Ordering::Release,
                );
                *quota
                    .latest_ledger
                    .write()
                    .unwrap_or_else(|lock| lock.into_inner()) = Some(ledger);
                if reconciliation_needed {
                    quota.reconciliation_pending.store(true, Ordering::Release);
                    quota.reconciliation_retry_at_ms.store(0, Ordering::Release);
                    quota.healthy.store(false, Ordering::Release);
                } else if !quota.reconciliation_pending.load(Ordering::Acquire) {
                    quota.healthy.store(true, Ordering::Release);
                }
                return Some(reconciliation_needed);
            }
            Err(slatedb::object_store::Error::AlreadyExists { .. })
            | Err(slatedb::object_store::Error::Precondition { .. }) => {
                tokio::task::yield_now().await;
            }
            Err(_) => break,
        }
    }
    quota.healthy.store(false, Ordering::Release);
    quota.unverified_bytes.store(
        quota.verified_bytes.load(Ordering::Acquire),
        Ordering::Release,
    );
    quota.reconciliation_lag.fetch_add(1, Ordering::AcqRel);
    None
}

async fn recover_quota_coordination(
    store: &Arc<dyn ObjectStore>,
    namespace: &str,
    aggregate_max_bytes: Option<u64>,
    tiers: &[Arc<CacheTier>],
    policy: &PolicySnapshot,
    capacity: &Arc<StdMutex<CapacityState>>,
    quota: &Arc<QuotaCoordination>,
    pending_deletions: &Arc<StdMutex<HashMap<(usize, String), PendingDeletion>>>,
    deletion_wake: &Arc<Notify>,
) -> bool {
    let _operation = quota.operation.lock().await;
    quota.reconciliation_pending.store(true, Ordering::Release);
    quota.healthy.store(false, Ordering::Release);
    let policy_revision = policy.revision;
    let now = now_ms();
    let lease_expires_ms = now.saturating_add(duration_ms(quota.options.lease_duration));
    let (mut ledger, mut version) = match read_quota_ledger_raw(
        store,
        &quota.ledger_path,
        namespace,
        aggregate_max_bytes,
        policy_revision,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => return false,
    };
    if ledger.policy_revision > policy_revision {
        return false;
    }
    if ledger.reconciler.as_ref().is_some_and(|reconciler| {
        reconciler.manager_id != quota.manager_id && reconciler.lease_expires_ms > now
    }) {
        return false;
    }
    ledger.reconciler = Some(QuotaReconciler {
        manager_id: quota.manager_id.clone(),
        lease_expires_ms,
    });
    ledger
        .managers
        .entry(quota.manager_id.clone())
        .or_insert_with(|| QuotaManagerGrant {
            lease_expires_ms,
            committed_by_tier: HashMap::new(),
            reserved_by_tier: HashMap::new(),
        })
        .lease_expires_ms = lease_expires_ms;
    if advance_quota_revision(&mut ledger).is_none() {
        return false;
    }
    if write_quota_ledger_raw(store, &quota.ledger_path, &ledger, version)
        .await
        .is_err()
    {
        return false;
    }
    let inventory_revision = ledger.revision;
    let mut rebuilt = CapacityState::default();
    for (index, tier) in tiers.iter().enumerate() {
        let domain = cache_domain_namespace(namespace, tier.confidentiality);
        let timeout = Duration::from_millis(policy.policies[index].timeout_ms);
        let mut listed = tier.store.list(Some(&ObjectPath::from("entries")));
        loop {
            let meta = match tokio::time::timeout(timeout, listed.next()).await {
                Ok(Some(Ok(meta))) => meta,
                Ok(None) => break,
                Ok(Some(Err(_))) | Err(_) => return false,
            };
            let is_sidecar = meta.location.as_ref().ends_with(".meta");
            let key = meta
                .location
                .as_ref()
                .trim_end_matches(if is_sidecar { ".meta" } else { ".data" })
                .trim_start_matches("entries/")
                .to_owned();
            reconcile_capacity_object(&mut rebuilt, index, &key, meta.size, None, !is_sidecar);
            if is_sidecar {
                let Ok(Ok(result)) =
                    tokio::time::timeout(timeout, tier.store.get(&meta.location)).await
                else {
                    continue;
                };
                let Ok(Ok(bytes)) = tokio::time::timeout(timeout, result.bytes()).await else {
                    continue;
                };
                let Ok(sidecar) = serde_json::from_slice::<CacheSidecar>(&bytes) else {
                    continue;
                };
                if sidecar.format == CACHE_FORMAT && sidecar.namespace == domain {
                    reconcile_capacity_object(
                        &mut rebuilt,
                        index,
                        &key,
                        0,
                        Some((sidecar.created_ms, sidecar.accessed_ms)),
                        false,
                    );
                }
            }
        }
    }
    let current = match read_quota_ledger_raw(
        store,
        &quota.ledger_path,
        namespace,
        aggregate_max_bytes,
        policy_revision,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => return false,
    };
    ledger = current.0;
    version = current.1;
    if ledger.revision != inventory_revision
        || !ledger.reconciler.as_ref().is_some_and(|reconciler| {
            reconciler.manager_id == quota.manager_id && reconciler.lease_expires_ms >= now_ms()
        })
    {
        return false;
    }
    if ledger.policy_revision > policy_revision {
        return false;
    }
    for grant in ledger.managers.values_mut() {
        grant.committed_by_tier.clear();
        grant.reserved_by_tier.clear();
    }
    let previous_managers = ledger.managers.clone();
    let previous_entries = ledger.entries.clone();
    let physical_ids = rebuilt
        .entries
        .keys()
        .map(|(index, key)| quota_entry_id(&tiers[*index], key))
        .collect::<HashSet<_>>();
    let complete_ids = rebuilt
        .entries
        .iter()
        .filter(|(_, entry)| entry.data_present && entry.metadata_valid)
        .map(|((index, key), _)| quota_entry_id(&tiers[*index], key))
        .collect::<HashSet<_>>();
    let mut entries = HashMap::with_capacity(rebuilt.entries.len() + previous_entries.len());
    for (entry_id, previous) in &previous_entries {
        let preserve_admitting =
            previous.state == QuotaEntryState::Admitting && previous.expires_ms > now;
        let preserve_cleanup = physical_ids.contains(entry_id)
            && (previous.state == QuotaEntryState::Deleting || !complete_ids.contains(entry_id));
        if !preserve_admitting && !preserve_cleanup {
            continue;
        }
        let mut preserved = previous.clone();
        if !preserve_admitting {
            preserved.state = QuotaEntryState::Deleting;
        }
        if previous_managers
            .get(&preserved.owner_manager_id)
            .is_none_or(|grant| grant.lease_expires_ms < now)
        {
            preserved.owner_manager_id.clone_from(&quota.manager_id);
        }
        let grant = ledger
            .managers
            .entry(preserved.owner_manager_id.clone())
            .or_insert_with(|| QuotaManagerGrant {
                lease_expires_ms: 0,
                committed_by_tier: HashMap::new(),
                reserved_by_tier: HashMap::new(),
            });
        let accounting = if preserved.state == QuotaEntryState::Admitting {
            &mut grant.reserved_by_tier
        } else {
            &mut grant.committed_by_tier
        };
        *accounting.entry(preserved.tier_id.clone()).or_default() = accounting
            .get(&preserved.tier_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(preserved.bytes);
        entries.insert(entry_id.clone(), preserved);
    }
    for ((index, key), entry) in &mut rebuilt.entries {
        let entry_id = quota_entry_id(&tiers[*index], key);
        if entries.contains_key(&entry_id) {
            continue;
        }
        let previous = ledger.entries.get(&entry_id).cloned();
        let generation = if let Some(previous) = previous.as_ref() {
            previous.generation
        } else {
            let Some(generation) = next_quota_generation(&mut ledger.next_generation) else {
                return false;
            };
            generation
        };
        entry.generation = generation;
        let owner_manager_id = previous.map_or_else(
            || quota.manager_id.clone(),
            |entry| {
                if previous_managers
                    .get(&entry.owner_manager_id)
                    .is_some_and(|grant| grant.lease_expires_ms >= now)
                {
                    entry.owner_manager_id
                } else {
                    quota.manager_id.clone()
                }
            },
        );
        let tier_id = tiers[*index].id.clone();
        let grant = ledger
            .managers
            .entry(owner_manager_id.clone())
            .or_insert_with(|| QuotaManagerGrant {
                lease_expires_ms: 0,
                committed_by_tier: HashMap::new(),
                reserved_by_tier: HashMap::new(),
            });
        *grant.committed_by_tier.entry(tier_id.clone()).or_default() = grant
            .committed_by_tier
            .get(&tier_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(entry.bytes);
        entries.insert(
            entry_id.clone(),
            QuotaEntry {
                key: key.clone(),
                tier_id,
                owner_manager_id,
                bytes: entry.bytes,
                generation,
                policy_revision,
                expires_ms: 0,
                state: if entry.data_present && entry.metadata_valid {
                    QuotaEntryState::Active
                } else {
                    QuotaEntryState::Deleting
                },
            },
        );
    }
    ledger.entries = entries;
    ledger.managers.retain(|manager_id, grant| {
        manager_id == &quota.manager_id
            || grant.lease_expires_ms >= now
            || grant.committed_by_tier.values().any(|bytes| *bytes != 0)
            || grant.reserved_by_tier.values().any(|bytes| *bytes != 0)
    });
    ledger.policy_revision = policy_revision;
    ledger.reconciler = None;
    if advance_quota_revision(&mut ledger).is_none() {
        return false;
    }
    if write_quota_ledger_raw(store, &quota.ledger_path, &ledger, version)
        .await
        .is_err()
    {
        return false;
    }
    *capacity.lock().unwrap_or_else(|lock| lock.into_inner()) = rebuilt;
    let mut scheduled = false;
    {
        let mut pending = pending_deletions
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        for entry in ledger.entries.values().filter(|entry| {
            entry.owner_manager_id == quota.manager_id && entry.state == QuotaEntryState::Deleting
        }) {
            let Some(index) = tiers.iter().position(|tier| tier.id == entry.tier_id) else {
                continue;
            };
            pending
                .entry((index, entry.key.clone()))
                .or_insert(PendingDeletion {
                    index,
                    key: entry.key.clone(),
                    generation: Some(entry.generation),
                    retry_at_ms: now_ms(),
                    backoff: DELETION_BACKOFF_INITIAL,
                });
            scheduled = true;
        }
    }
    if scheduled {
        deletion_wake.notify_one();
    }
    for (index, tier) in tiers.iter().enumerate() {
        tier.inventory_reconciliation_failed
            .store(false, Ordering::Release);
        let tier_has_pending_deletions = pending_deletions
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .values()
            .any(|pending| pending.index == index);
        if !tier_has_pending_deletions {
            if !tier.inventory_reconciliation_failed.load(Ordering::Acquire) {
                tier.reconciliation_lag.store(0, Ordering::Release);
            }
        }
    }
    quota
        .ledger_revision
        .store(ledger.revision, Ordering::Release);
    quota
        .lease_expiry_ms
        .store(lease_expires_ms, Ordering::Release);
    quota.verified_bytes.store(
        ledger
            .entries
            .values()
            .map(|entry| entry.bytes)
            .fold(0u64, u64::saturating_add),
        Ordering::Release,
    );
    quota.unverified_bytes.store(0, Ordering::Release);
    *quota
        .latest_ledger
        .write()
        .unwrap_or_else(|lock| lock.into_inner()) = Some(ledger);
    quota.reconciliation_pending.store(false, Ordering::Release);
    quota.reconciliation_retry_at_ms.store(0, Ordering::Release);
    quota.reconciliation_lag.store(0, Ordering::Release);
    quota.healthy.store(true, Ordering::Release);
    true
}

async fn read_quota_ledger_raw(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    namespace: &str,
    aggregate_max_bytes: Option<u64>,
    policy_revision: u64,
) -> Result<(QuotaLedger, Option<UpdateVersion>)> {
    match coordinated(store.get(path)).await {
        Ok(result) => {
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let bytes = coordinated(result.bytes())
                .await
                .context("read cache quota ledger")?;
            let ledger: QuotaLedger =
                serde_json::from_slice(&bytes).context("decode cache quota ledger")?;
            if ledger.format != QUOTA_FORMAT
                || ledger.namespace != namespace
                || ledger.aggregate_max_bytes != aggregate_max_bytes
            {
                bail!("cache quota ledger identity mismatch");
            }
            if !quota_ledger_accounting_valid(&ledger) {
                bail!("cache quota ledger accounting overflows");
            }
            Ok((ledger, Some(version)))
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok((
            QuotaLedger {
                format: QUOTA_FORMAT,
                namespace: namespace.to_owned(),
                revision: 0,
                next_generation: 0,
                policy_revision,
                aggregate_max_bytes,
                managers: HashMap::new(),
                entries: HashMap::new(),
                reconciler: None,
            },
            None,
        )),
        Err(error) => Err(error).context("read cache quota ledger"),
    }
}

async fn write_quota_ledger_raw(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    ledger: &QuotaLedger,
    version: Option<UpdateVersion>,
) -> Result<()> {
    let bytes = serde_json::to_vec(ledger).context("encode cache quota ledger")?;
    coordinated(store.put_opts(
        path,
        bytes.into(),
        PutOptions::from(version.map_or(PutMode::Create, PutMode::Update)),
    ))
    .await
    .context("persist cache quota ledger")?;
    Ok(())
}

fn reconcile_capacity_object(
    state: &mut CapacityState,
    index: usize,
    key: &str,
    bytes: u64,
    timestamps: Option<(u64, u64)>,
    data_present: bool,
) {
    let entry = state
        .entries
        .entry((index, key.to_owned()))
        .or_insert(EntryState {
            bytes: 0,
            created_ms: 0,
            accessed_ms: 0,
            data_present: false,
            metadata_valid: false,
            pins: 0,
            retiring: false,
            generation: 0,
        });
    entry.bytes = entry.bytes.saturating_add(bytes);
    if let Some((created_ms, accessed_ms)) = timestamps {
        entry.created_ms = created_ms;
        entry.accessed_ms = accessed_ms;
        entry.metadata_valid = true;
    }
    entry.data_present |= data_present;
    state.aggregate_used = state.aggregate_used.saturating_add(bytes);
    *state.used_by_tier.entry(index).or_default() = state
        .used_by_tier
        .get(&index)
        .copied()
        .unwrap_or(0)
        .saturating_add(bytes);
}

fn request_identity(options: &GetOptions) -> String {
    let range = match &options.range {
        None => "full".to_owned(),
        Some(GetRange::Bounded(range)) => format!("bounded:{}:{}", range.start, range.end),
        Some(GetRange::Offset(offset)) => format!("offset:{offset}"),
        Some(GetRange::Suffix(length)) => format!("suffix:{length}"),
    };
    format!(
        "{range}\0version={}",
        options.version.as_deref().unwrap_or_default()
    )
}

fn cache_key(namespace: &str, location: &ObjectPath, options: &GetOptions) -> String {
    sha256_hex(format!(
        "{namespace}\0{}\0{}",
        location.as_ref(),
        request_identity(options)
    ))
}

fn entry_path(key: &str, suffix: &str) -> ObjectPath {
    ObjectPath::from(format!("entries/{key}.{suffix}"))
}

fn make_sidecar(
    namespace: &str,
    location: &ObjectPath,
    options: &GetOptions,
    meta: &ObjectMeta,
    range: Range<u64>,
    attributes: &Attributes,
    payload: &Bytes,
) -> CacheSidecar {
    let now = now_ms();
    CacheSidecar {
        format: CACHE_FORMAT,
        namespace: namespace.to_owned(),
        origin_path: location.to_string(),
        request_identity: request_identity(options),
        location: meta.location.to_string(),
        last_modified: meta.last_modified.to_rfc3339(),
        size: meta.size,
        e_tag: meta.e_tag.clone(),
        version: meta.version.clone(),
        response_start: range.start,
        response_end: range.end,
        sha256: sha256_hex(payload),
        length: payload.len() as u64,
        attributes: encode_attributes(attributes),
        created_ms: now,
        accessed_ms: now,
    }
}

fn encode_attributes(attributes: &Attributes) -> Vec<(String, String)> {
    attributes
        .iter()
        .filter_map(|(attribute, value)| {
            let name = match attribute {
                Attribute::ContentDisposition => "content-disposition".to_owned(),
                Attribute::ContentEncoding => "content-encoding".to_owned(),
                Attribute::ContentLanguage => "content-language".to_owned(),
                Attribute::ContentType => "content-type".to_owned(),
                Attribute::CacheControl => "cache-control".to_owned(),
                Attribute::StorageClass => "storage-class".to_owned(),
                Attribute::Metadata(name) => format!("metadata:{name}"),
                _ => return None,
            };
            Some((name, value.as_ref().to_owned()))
        })
        .collect()
}

fn decode_attributes(values: &[(String, String)]) -> Option<Attributes> {
    let mut attributes = Attributes::new();
    for (name, value) in values {
        let attribute = match name.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            name if name.starts_with("metadata:") => {
                Attribute::Metadata(name.trim_start_matches("metadata:").to_owned().into())
            }
            _ => return None,
        };
        attributes.insert(attribute, AttributeValue::from(value.clone()));
    }
    Some(attributes)
}

fn result_from_bytes(
    payload: Bytes,
    meta: ObjectMeta,
    range: Range<u64>,
    attributes: Attributes,
    extensions: Extensions,
) -> GetResult {
    GetResult {
        payload: GetResultPayload::Stream(stream::once(async { Ok(payload) }).boxed()),
        meta,
        range,
        attributes,
        extensions,
    }
}

fn expired(
    sidecar: &CacheSidecar,
    policy: &CacheTierPolicy,
    now: u64,
    local_accessed_ms: Option<u64>,
) -> bool {
    let accessed_ms = local_accessed_ms.unwrap_or(sidecar.accessed_ms);
    (policy.idle_age_ms != 0 && now.saturating_sub(accessed_ms) >= policy.idle_age_ms)
        || policy
            .absolute_age_ms
            .is_some_and(|age| now.saturating_sub(sidecar.created_ms) >= age)
}

fn circuit_open(tier: &CacheTier) -> bool {
    tier.circuit
        .lock()
        .unwrap_or_else(|lock| lock.into_inner())
        .open_until_ms
        > now_ms()
}

fn circuit_success(tier: &CacheTier) {
    let mut circuit = tier.circuit.lock().unwrap_or_else(|lock| lock.into_inner());
    circuit.failures = 0;
    circuit.open_until_ms = 0;
}

fn circuit_failure(tier: &CacheTier, timeout: bool) {
    let mut circuit = tier.circuit.lock().unwrap_or_else(|lock| lock.into_inner());
    circuit.failures = circuit.failures.saturating_add(1);
    if timeout || circuit.failures >= CIRCUIT_FAILURES {
        circuit.open_until_ms = now_ms().saturating_add(CIRCUIT_OPEN_MS);
    }
}

fn eviction_counter(metrics: &CacheMetrics, reason: EvictionReason) -> &AtomicU64 {
    match reason {
        EvictionReason::Capacity => &metrics.capacity_evictions,
        EvictionReason::Idle => &metrics.idle_evictions,
        EvictionReason::Absolute => &metrics.absolute_evictions,
        EvictionReason::Corruption => &metrics.corruption_evictions,
    }
}

fn enqueue_owned_deleting_entries(
    tiers: &[Arc<CacheTier>],
    pending_deletions: &StdMutex<HashMap<(usize, String), PendingDeletion>>,
    deletion_wake: &Notify,
    manager_id: &str,
    ledger: &QuotaLedger,
) {
    let mut scheduled = false;
    let mut pending = pending_deletions
        .lock()
        .unwrap_or_else(|lock| lock.into_inner());
    for entry in ledger.entries.values().filter(|entry| {
        entry.owner_manager_id == manager_id && entry.state == QuotaEntryState::Deleting
    }) {
        let Some(index) = tiers.iter().position(|tier| tier.id == entry.tier_id) else {
            continue;
        };
        if let std::collections::hash_map::Entry::Vacant(slot) =
            pending.entry((index, entry.key.clone()))
        {
            slot.insert(PendingDeletion {
                index,
                key: entry.key.clone(),
                generation: Some(entry.generation),
                retry_at_ms: 0,
                backoff: DELETION_BACKOFF_INITIAL,
            });
            scheduled = true;
        }
    }
    drop(pending);
    if scheduled {
        deletion_wake.notify_one();
    }
}

fn schedule_limit_enforcement(
    tiers: &[Arc<CacheTier>],
    capacity: &StdMutex<CapacityState>,
    policies: &[CacheTierPolicy],
    aggregate_max_bytes: Option<u64>,
    metrics: &CacheMetrics,
    pending_deletions: &StdMutex<HashMap<(usize, String), PendingDeletion>>,
    deletion_wake: &Notify,
    limit: usize,
) {
    let now = now_ms();
    let scheduled = {
        let mut state = capacity.lock().unwrap_or_else(|lock| lock.into_inner());
        let aggregate_over = aggregate_max_bytes.is_some_and(|limit| state.aggregate_used > limit);
        let mut candidates = state
            .entries
            .iter()
            .filter(|(_, entry)| !entry.retiring)
            .filter_map(|((index, key), entry)| {
                let policy = &policies[*index];
                let used = state.used_by_tier.get(index).copied().unwrap_or(0);
                let reason = if !policy.enabled
                    || policy
                        .absolute_age_ms
                        .is_some_and(|age| now.saturating_sub(entry.created_ms) >= age)
                {
                    EvictionReason::Absolute
                } else if policy.idle_age_ms != 0
                    && now.saturating_sub(entry.accessed_ms) >= policy.idle_age_ms
                {
                    EvictionReason::Idle
                } else if used > policy.max_bytes || aggregate_over {
                    EvictionReason::Capacity
                } else {
                    return None;
                };
                Some((entry.accessed_ms, *index, key.clone(), reason))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(accessed_ms, _, _, _)| *accessed_ms);
        candidates.truncate(limit);
        candidates
            .into_iter()
            .filter_map(|(_, index, key, reason)| {
                let entry = state.entries.get_mut(&(index, key.clone()))?;
                entry.retiring = true;
                Some((index, key, reason, entry.pins == 0, entry.generation))
            })
            .collect::<Vec<_>>()
    };
    if scheduled.is_empty() {
        return;
    }
    let mut pending = pending_deletions
        .lock()
        .unwrap_or_else(|lock| lock.into_inner());
    for (index, key, reason, should_delete, generation) in scheduled {
        eviction_counter(&tiers[index].metrics, reason).fetch_add(1, Ordering::AcqRel);
        eviction_counter(metrics, reason).fetch_add(1, Ordering::AcqRel);
        if should_delete {
            pending
                .entry((index, key.clone()))
                .or_insert_with(|| PendingDeletion {
                    index,
                    key,
                    generation: (generation != 0).then_some(generation),
                    retry_at_ms: 0,
                    backoff: DELETION_BACKOFF_INITIAL,
                });
        }
    }
    drop(pending);
    deletion_wake.notify_one();
}

fn record_latency(total: &AtomicU64, count: &AtomicU64, started: Instant) {
    let elapsed = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
    total.fetch_add(elapsed, Ordering::AcqRel);
    count.fetch_add(1, Ordering::AcqRel);
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn sha256_hex(value: impl AsRef<[u8]>) -> String {
    Sha256::digest(value.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn cache_error(message: &str) -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "vaultic read cache",
        source: message.to_owned().into(),
    }
}

pub(crate) fn cacheable_read(options: &GetOptions) -> bool {
    let Some(tag) = ObjectStoreCallTag::from_extensions(&options.extensions) else {
        return false;
    };
    !options.head
        && options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && tag.sst_type == SstType::Compacted
        && tag.retry.is_none()
        && matches!(tag.kind, TableStoreKind::Main | TableStoreKind::Reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::{object_store::memory::InMemory, object_store_tag::RetryReason};
    use std::sync::atomic::AtomicBool;

    #[test]
    fn quota_ledger_rejects_overflowing_accounting() {
        let entry = |bytes| QuotaEntry {
            key: "key".to_owned(),
            tier_id: "tier".to_owned(),
            owner_manager_id: "manager".to_owned(),
            bytes,
            generation: 0,
            policy_revision: 0,
            expires_ms: 0,
            state: QuotaEntryState::Active,
        };
        let mut ledger = QuotaLedger {
            format: QUOTA_FORMAT,
            namespace: "test".to_owned(),
            revision: 0,
            next_generation: 0,
            policy_revision: 0,
            aggregate_max_bytes: None,
            managers: HashMap::new(),
            entries: HashMap::from([
                ("one".to_owned(), entry(u64::MAX)),
                ("two".to_owned(), entry(1)),
            ]),
            reconciler: None,
        };
        assert!(!quota_ledger_accounting_valid(&ledger));
        ledger.entries.clear();
        ledger.managers.insert(
            "manager".to_owned(),
            QuotaManagerGrant {
                lease_expires_ms: 0,
                committed_by_tier: HashMap::new(),
                reserved_by_tier: HashMap::from([
                    ("one".to_owned(), u64::MAX),
                    ("two".to_owned(), 1),
                ]),
            },
        );
        assert!(!quota_ledger_accounting_valid(&ledger));
        let mut admitting = entry(u64::MAX);
        admitting.state = QuotaEntryState::Admitting;
        ledger.entries = HashMap::from([("one".to_owned(), admitting)]);
        ledger.managers.get_mut("manager").unwrap().reserved_by_tier =
            HashMap::from([("tier".to_owned(), u64::MAX)]);
        assert!(quota_ledger_accounting_valid(&ledger));
        ledger.managers.get_mut("manager").unwrap().reserved_by_tier =
            HashMap::from([("tier".to_owned(), u64::MAX - 1)]);
        assert!(!quota_ledger_accounting_valid(&ledger));
        ledger
            .managers
            .get_mut("manager")
            .unwrap()
            .reserved_by_tier
            .clear();
        assert!(!quota_ledger_accounting_valid(&ledger));
        ledger.entries = HashMap::from([("one".to_owned(), entry(u64::MAX))]);
        ledger.managers.get_mut("manager").unwrap().reserved_by_tier =
            HashMap::from([("one".to_owned(), 1)]);
        assert!(!quota_ledger_accounting_valid(&ledger));
    }

    #[test]
    fn quota_generation_rejects_exhaustion() {
        let mut generation = u64::MAX;
        assert_eq!(next_quota_generation(&mut generation), None);
        assert_eq!(generation, u64::MAX);
        assert_eq!(next_policy_generation(u64::MAX, true), None);
        assert_eq!(next_policy_generation(u64::MAX, false), Some(u64::MAX));

        let mut ledger = QuotaLedger {
            format: QUOTA_FORMAT,
            namespace: "test".to_owned(),
            revision: u64::MAX,
            next_generation: 0,
            policy_revision: 0,
            aggregate_max_bytes: None,
            managers: HashMap::new(),
            entries: HashMap::new(),
            reconciler: None,
        };
        assert_eq!(advance_quota_revision(&mut ledger), None);
        assert_eq!(ledger.revision, u64::MAX);
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        reads: AtomicU64,
        writes: AtomicU64,
        write_delay_ms: AtomicU64,
        data_read_delay_ms: AtomicU64,
        list_delay_ms: AtomicU64,
        coordination_delay_ms: AtomicU64,
        list_error_after: AtomicU64,
        deny: AtomicBool,
        deny_delete: AtomicBool,
        fail_metadata_put_once: AtomicBool,
        fail_put_after_commit_once: AtomicBool,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                reads: AtomicU64::new(0),
                writes: AtomicU64::new(0),
                write_delay_ms: AtomicU64::new(0),
                data_read_delay_ms: AtomicU64::new(0),
                list_delay_ms: AtomicU64::new(0),
                coordination_delay_ms: AtomicU64::new(0),
                list_error_after: AtomicU64::new(u64::MAX),
                deny: AtomicBool::new(false),
                deny_delete: AtomicBool::new(false),
                fail_metadata_put_once: AtomicBool::new(false),
                fail_put_after_commit_once: AtomicBool::new(false),
            }
        }
    }

    impl fmt::Display for CountingStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("counting memory store")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> slatedb::object_store::Result<PutResult> {
            let coordination = location.as_ref().starts_with(POLICY_PREFIX)
                || location.as_ref().starts_with(QUOTA_PREFIX);
            let delay = if coordination {
                self.coordination_delay_ms.load(Ordering::Acquire)
            } else {
                self.write_delay_ms.load(Ordering::Acquire)
            };
            if delay != 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if self.deny.load(Ordering::Acquire) {
                return Err(cache_error("cache credentials denied"));
            }
            if location.as_ref().ends_with(".meta")
                && self.fail_metadata_put_once.swap(false, Ordering::AcqRel)
            {
                return Err(cache_error("cache metadata write failed"));
            }
            self.writes.fetch_add(1, Ordering::AcqRel);
            let result = self.inner.put_opts(location, payload, options).await?;
            if self
                .fail_put_after_commit_once
                .swap(false, Ordering::AcqRel)
            {
                return Err(cache_error("committed response lost"));
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> slatedb::object_store::Result<GetResult> {
            self.reads.fetch_add(1, Ordering::AcqRel);
            if location.as_ref().starts_with(POLICY_PREFIX)
                || location.as_ref().starts_with(QUOTA_PREFIX)
            {
                let delay = self.coordination_delay_ms.load(Ordering::Acquire);
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
            if location.as_ref().ends_with(".data") {
                let delay = self.data_read_delay_ms.load(Ordering::Acquire);
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
            if self.deny.load(Ordering::Acquire) {
                return Err(cache_error("cache credentials denied"));
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[Range<u64>],
        ) -> slatedb::object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, slatedb::object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectPath>> {
            if self.deny_delete.load(Ordering::Acquire) {
                return locations
                    .map(|location| {
                        location.and_then(|_| Err(cache_error("cache deletion denied")))
                    })
                    .boxed();
            }
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            let delay = self.list_delay_ms.load(Ordering::Acquire);
            let listed = self.inner.list(prefix).then(move |item| async move {
                if delay != 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                item
            });
            let error_after = self.list_error_after.load(Ordering::Acquire);
            if error_after == u64::MAX {
                listed.boxed()
            } else {
                listed
                    .take(error_after as usize)
                    .chain(stream::once(async {
                        Err(cache_error("cache listing interrupted"))
                    }))
                    .boxed()
            }
        }

        fn list_with_offset(
            &self,
            prefix: Option<&ObjectPath>,
            offset: &ObjectPath,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> slatedb::object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> slatedb::object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn options(kind: TableStoreKind, sst_type: SstType, retry: bool) -> GetOptions {
        let mut extensions = Extensions::new();
        extensions.insert(ObjectStoreCallTag {
            kind,
            sst_type,
            retry: retry.then_some(RetryReason::CrcMismatch),
            segment: None,
        });
        GetOptions {
            extensions,
            ..Default::default()
        }
    }

    fn policy(max_bytes: u64) -> CacheTierPolicy {
        CacheTierPolicy {
            enabled: true,
            max_bytes,
            idle_age_ms: 60_000,
            absolute_age_ms: Some(120_000),
            read_priority: 0,
            admission_priority: 0,
            timeout_ms: 1_000,
        }
    }

    async fn manager(max_bytes: u64) -> (Arc<CountingStore>, Arc<CacheManager>) {
        let origin = Arc::new(CountingStore::new());
        let cache = Arc::new(
            CacheManager::new(
                origin.clone(),
                Arc::new(InMemory::new()),
                CacheConfig {
                    tiers: vec![CacheTierConfig {
                        id: "memory".to_owned(),
                        store: ReplicaStoreConfig::Memory,
                        confidentiality: CacheConfidentiality::Encrypted,
                        policy: policy(max_bytes),
                    }],
                    aggregate_max_bytes: None,
                    part_size_bytes: 4096,
                    max_inflight_bytes: max_bytes.saturating_mul(2).max(4096),
                    max_background_tasks: max_bytes.saturating_mul(2).max(4096).div_ceil(4096)
                        as usize,
                },
                "repository",
                "database",
            )
            .await
            .unwrap(),
        );
        (origin, cache)
    }

    async fn manager_with_cache_store(
        max_bytes: u64,
        cache_store: Arc<CountingStore>,
    ) -> (Arc<CountingStore>, Arc<CacheManager>) {
        manager_with_shared_stores(
            max_bytes,
            Some(max_bytes),
            cache_store,
            Arc::new(CountingStore::new()),
            "controlled-cache",
            QuotaOptions::default(),
        )
        .await
    }

    async fn manager_with_shared_stores(
        max_bytes: u64,
        aggregate_max_bytes: Option<u64>,
        cache_store: Arc<CountingStore>,
        policy_store: Arc<CountingStore>,
        database_identity: &str,
        quota_options: QuotaOptions,
    ) -> (Arc<CountingStore>, Arc<CacheManager>) {
        let origin = Arc::new(CountingStore::new());
        let namespace = cache_namespace("repository", database_identity);
        let (cancel, _) = watch::channel(false);
        let tiers = vec![Arc::new(CacheTier {
            id: "controlled".to_owned(),
            store: cache_store,
            confidentiality: CacheConfidentiality::Encrypted,
            metrics: CacheMetrics::default(),
            circuit: StdMutex::new(CircuitState::default()),
            reconciliation_lag: AtomicU64::new(0),
            inventory_reconciliation_failed: AtomicBool::new(false),
        })];
        let initial_policies = vec![policy(max_bytes)];
        let initial_canonical = canonical_policy(0, &tiers, &initial_policies).unwrap();
        let manager = CacheManager {
            origin: origin.clone(),
            confidentiality: CacheConfidentiality::Encrypted,
            policy_store,
            policy_path: policy_path(&namespace),
            namespace: namespace.clone(),
            tiers,
            capacity: Arc::new(StdMutex::new(CapacityState::default())),
            inflight: Arc::new(StdMutex::new(HashMap::new())),
            policy_update: Arc::new(Mutex::new(())),
            policy_snapshot: Arc::new(StdRwLock::new(Arc::new(PolicySnapshot {
                revision: 0,
                canonical: initial_canonical,
                generation: 0,
                policies: initial_policies.clone(),
            }))),
            quota: Arc::new(QuotaCoordination {
                manager_id: sha256_hex(rand::random::<[u8; 32]>()),
                ledger_path: quota_path(&namespace),
                healthy: AtomicBool::new(true),
                ledger_revision: AtomicU64::new(0),
                lease_expiry_ms: AtomicU64::new(u64::MAX),
                verified_bytes: AtomicU64::new(0),
                unverified_bytes: AtomicU64::new(0),
                reconciliation_lag: AtomicU64::new(0),
                reconciliation_pending: AtomicBool::new(false),
                reconciliation_retry_at_ms: AtomicU64::new(0),
                policy_sync_lag: AtomicU64::new(0),
                policy_sync_error: StdMutex::new(String::new()),
                latest_ledger: StdRwLock::new(None),
                operation: Mutex::new(()),
                cancel,
                task: Mutex::new(None),
                options: quota_options,
            }),
            aggregate_max_bytes,
            max_inflight_bytes: max_bytes,
            part_size_bytes: 4096,
            background_budget: Arc::new(Semaphore::new(max_bytes as usize)),
            background_task_budget: Arc::new(Semaphore::new(1)),
            background_tasks: Arc::new(StdMutex::new(Vec::new())),
            pending_deletions: Arc::new(StdMutex::new(HashMap::new())),
            deletion_wake: Arc::new(Notify::new()),
            deletion_task: Arc::new(Mutex::new(None)),
            closing: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(CacheMetrics::default()),
        };
        manager.initialize_policy().await.unwrap();
        manager.initialize_quota().await;
        manager.enqueue_latest_pending_deletions();
        manager.start_quota_heartbeat().await;
        manager.start_deletion_worker().await;
        (origin, Arc::new(manager))
    }

    async fn two_tier_manager(database_identity: &str) -> Arc<CacheManager> {
        Arc::new(
            CacheManager::new_with_quota_options(
                Arc::new(InMemory::new()),
                Arc::new(CountingStore::new()),
                CacheConfig {
                    tiers: vec![
                        CacheTierConfig {
                            id: "first".to_owned(),
                            store: ReplicaStoreConfig::Memory,
                            confidentiality: CacheConfidentiality::Encrypted,
                            policy: policy(1_000),
                        },
                        CacheTierConfig {
                            id: "second".to_owned(),
                            store: ReplicaStoreConfig::Memory,
                            confidentiality: CacheConfidentiality::Encrypted,
                            policy: policy(2_000),
                        },
                    ],
                    aggregate_max_bytes: None,
                    part_size_bytes: 4096,
                    max_inflight_bytes: 4096,
                    max_background_tasks: 1,
                },
                "repository",
                database_identity,
                QuotaOptions {
                    lease_duration: Duration::from_secs(1),
                    renew_interval: Duration::from_secs(60),
                },
            )
            .await
            .unwrap(),
        )
    }

    async fn cached_get(cache: &dyn ObjectStore, path: &ObjectPath, options: GetOptions) -> Bytes {
        cache
            .get_opts(path, options)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    }

    async fn wait_for_admissions(cache: &CacheManager, count: u64) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.status().metrics.admissions < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background cache admission");
    }

    async fn wait_for_tier_admissions(cache: &CacheManager, tier_ids: &[&str]) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let status = cache.status();
                if tier_ids.iter().all(|id| {
                    status
                        .tiers
                        .iter()
                        .any(|tier| tier.id == *id && tier.metrics.admissions > 0)
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background cache tier admissions");
    }

    #[test]
    fn only_foreground_compacted_sst_reads_are_cacheable() {
        assert!(cacheable_read(&options(
            TableStoreKind::Main,
            SstType::Compacted,
            false
        )));
        assert!(cacheable_read(&options(
            TableStoreKind::Reader,
            SstType::Compacted,
            false
        )));
        assert!(!cacheable_read(&GetOptions::default()));
        assert!(!cacheable_read(&options(
            TableStoreKind::Main,
            SstType::Wal,
            false
        )));
        assert!(!cacheable_read(&options(
            TableStoreKind::Compactor,
            SstType::Compacted,
            false
        )));
        assert!(!cacheable_read(&options(
            TableStoreKind::Main,
            SstType::Compacted,
            true
        )));
        for conditional in 0..4 {
            let mut request = options(TableStoreKind::Main, SstType::Compacted, false);
            match conditional {
                0 => request.if_match = Some("etag".to_owned()),
                1 => request.if_none_match = Some("etag".to_owned()),
                2 => request.if_modified_since = Some("2026-01-01T00:00:00Z".parse().unwrap()),
                _ => request.if_unmodified_since = Some("2026-01-01T00:00:00Z".parse().unwrap()),
            }
            assert!(!cacheable_read(&request));
        }
    }

    #[test]
    fn cache_configuration_rejects_unsafe_bounds() {
        let mut config = CacheConfig {
            part_size_bytes: 4097,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        config.part_size_bytes = DEFAULT_PART_SIZE_BYTES;
        config.max_inflight_bytes = 1;
        assert!(config.validate().is_err());
        config.max_inflight_bytes = DEFAULT_PART_SIZE_BYTES;
        config.aggregate_max_bytes = Some(0);
        assert!(config.validate().is_err());

        config.aggregate_max_bytes = None;
        config.tiers = vec![CacheTierConfig {
            id: "slatedb".to_owned(),
            store: ReplicaStoreConfig::Memory,
            confidentiality: CacheConfidentiality::Encrypted,
            policy: policy(4096),
        }];
        assert!(config.validate().is_err());
        config.tiers[0].id = "a".repeat(129);
        assert!(config.validate().is_err());

        config.tiers = (0..=MAX_CACHE_TIERS)
            .map(|index| CacheTierConfig {
                id: format!("tier-{index}"),
                store: ReplicaStoreConfig::Memory,
                confidentiality: CacheConfidentiality::Encrypted,
                policy: policy(4096),
            })
            .collect();
        assert!(config.validate().is_err());
        config.tiers.pop();
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn maximum_cardinality_status_does_not_block_cache_reads() {
        let origin = Arc::new(InMemory::new());
        let cache = Arc::new(
            CacheManager::new(
                origin.clone(),
                Arc::new(InMemory::new()),
                CacheConfig {
                    tiers: (0..MAX_CACHE_TIERS)
                        .map(|index| CacheTierConfig {
                            id: format!("tier-{index}"),
                            store: ReplicaStoreConfig::Memory,
                            confidentiality: CacheConfidentiality::Encrypted,
                            policy: policy(4096),
                        })
                        .collect(),
                    aggregate_max_bytes: None,
                    part_size_bytes: 4096,
                    max_inflight_bytes: 4096,
                    max_background_tasks: 1,
                },
                "repository",
                "maximum-status-cardinality",
            )
            .await
            .unwrap(),
        );
        let path = ObjectPath::from("sst/status-concurrency");
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        let store = cache.store(origin, CacheConfidentiality::Encrypted);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let status_task = {
            let cache = cache.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..64 {
                    assert_eq!(cache.status().tiers.len(), MAX_CACHE_TIERS);
                    tokio::task::yield_now().await;
                }
            })
        };
        let read_task = {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                cached_get(
                    &*store,
                    &path,
                    options(TableStoreKind::Main, SstType::Compacted, false),
                )
                .await
            })
        };
        barrier.wait().await;
        let (status_result, read_result) = tokio::time::timeout(
            Duration::from_secs(2),
            futures_util::future::join(status_task, read_task),
        )
        .await
        .expect("status and read remain responsive at maximum cardinality");
        status_result.unwrap();
        assert_eq!(read_result.unwrap(), b"value"[..]);
    }

    #[tokio::test]
    async fn aggregate_status_reports_the_binding_reclaim_limit() {
        let origin: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = CacheManager::new(
            origin,
            Arc::new(InMemory::new()),
            CacheConfig {
                tiers: vec![CacheTierConfig {
                    id: "memory".to_owned(),
                    store: ReplicaStoreConfig::Memory,
                    confidentiality: CacheConfidentiality::Encrypted,
                    policy: policy(100),
                }],
                aggregate_max_bytes: Some(75),
                part_size_bytes: 4096,
                max_inflight_bytes: 4096,
                max_background_tasks: 1,
            },
            "repository",
            "aggregate-status",
        )
        .await
        .unwrap();
        cache.commit_entry(0, "entry", 100, 1, 1);
        let status = cache.status();
        assert_eq!(status.used_bytes, 0);
        assert_eq!(status.local_used_bytes, 100);
        assert_eq!(status.tiers[0].local_used_bytes, 100);
    }

    #[tokio::test]
    async fn policy_persistence_uses_stable_authority() {
        let origin: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let policy_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut disabled = policy(4096);
        disabled.enabled = false;
        let cache = CacheManager::new(
            origin,
            policy_store.clone(),
            CacheConfig {
                tiers: vec![
                    CacheTierConfig {
                        id: "disabled".to_owned(),
                        store: ReplicaStoreConfig::Memory,
                        confidentiality: CacheConfidentiality::Encrypted,
                        policy: disabled.clone(),
                    },
                    CacheTierConfig {
                        id: "enabled".to_owned(),
                        store: ReplicaStoreConfig::Memory,
                        confidentiality: CacheConfidentiality::Encrypted,
                        policy: policy(4096),
                    },
                ],
                aggregate_max_bytes: None,
                part_size_bytes: 4096,
                max_inflight_bytes: 4096,
                max_background_tasks: 1,
            },
            "repository",
            "policy-tier",
        )
        .await
        .unwrap();
        cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "disabled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        assert!(cache.tiers[0].store.get(&cache.policy_path).await.is_err());
        assert!(cache.tiers[1].store.get(&cache.policy_path).await.is_err());
        assert!(policy_store.get(&cache.policy_path).await.is_ok());
    }

    #[tokio::test]
    async fn policy_restart_survives_when_every_tier_is_disabled() {
        let policy_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut disabled = policy(4096);
        disabled.enabled = false;
        let config = CacheConfig {
            tiers: vec![CacheTierConfig {
                id: "disabled".to_owned(),
                store: ReplicaStoreConfig::Memory,
                confidentiality: CacheConfidentiality::Encrypted,
                policy: policy(4096),
            }],
            aggregate_max_bytes: None,
            part_size_bytes: 4096,
            max_inflight_bytes: 4096,
            max_background_tasks: 1,
        };
        let first = CacheManager::new(
            Arc::new(InMemory::new()),
            policy_store.clone(),
            config.clone(),
            "repository",
            "policy-restart",
        )
        .await
        .unwrap();
        first
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "disabled".to_owned(),
                    policy: disabled.clone(),
                }],
            )
            .await
            .unwrap();

        let reopened = CacheManager::new(
            Arc::new(InMemory::new()),
            policy_store,
            config,
            "repository",
            "policy-restart",
        )
        .await
        .unwrap();
        assert_eq!(reopened.status().revision, 1);
        assert_eq!(reopened.status().tiers[0].policy, disabled);
    }

    #[tokio::test]
    async fn bypass_and_cold_warm_reads_preserve_bytes() {
        let (origin, cache) = manager(64 * 1024).await;
        let path = ObjectPath::from("sst/one");
        origin
            .put(&path, Bytes::from_static(b"ciphertext").into())
            .await
            .unwrap();
        assert_eq!(
            cache.get(&path).await.unwrap().bytes().await.unwrap(),
            b"ciphertext"[..]
        );
        assert_eq!(cache.status().metrics.admissions, 0);

        let eligible = options(TableStoreKind::Main, SstType::Compacted, false);
        let cold = cached_get(&cache, &path, eligible.clone()).await;
        wait_for_admissions(&cache, 1).await;
        let warm = cached_get(&cache, &path, eligible).await;
        assert_eq!(cold, warm);
        assert_eq!(origin.reads.load(Ordering::Acquire), 2);
        assert_eq!(cache.status().metrics.hits, 1);
    }

    #[tokio::test]
    async fn corrupt_cache_falls_back_to_origin() {
        let (origin, cache) = manager(64 * 1024).await;
        let path = ObjectPath::from("sst/corrupt");
        origin
            .put(&path, Bytes::from_static(b"good").into())
            .await
            .unwrap();
        let eligible = options(TableStoreKind::Main, SstType::Compacted, false);
        cached_get(&cache, &path, eligible.clone()).await;
        wait_for_admissions(&cache, 1).await;
        let key = cache_key(
            &cache_domain_namespace(&cache.namespace, CacheConfidentiality::Encrypted),
            &path,
            &eligible,
        );
        cache.tiers[0]
            .store
            .put(&entry_path(&key, "data"), Bytes::from_static(b"bad").into())
            .await
            .unwrap();
        assert_eq!(cached_get(&cache, &path, eligible).await, b"good"[..]);
        assert_eq!(origin.reads.load(Ordering::Acquire), 2);
        assert_eq!(cache.status().metrics.corruptions, 1);
    }

    #[tokio::test]
    async fn retry_evicts_a_reused_path_before_reading_origin() {
        let (origin, cache) = manager(64 * 1024).await;
        let path = ObjectPath::from("sst/reused-generation");
        let eligible = options(TableStoreKind::Main, SstType::Compacted, false);
        origin
            .put(&path, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        assert_eq!(
            cached_get(&cache, &path, eligible.clone()).await,
            b"old"[..]
        );
        wait_for_admissions(&cache, 1).await;
        origin
            .put(&path, Bytes::from_static(b"new").into())
            .await
            .unwrap();
        assert_eq!(
            cached_get(&cache, &path, eligible.clone()).await,
            b"old"[..]
        );
        assert_eq!(
            cached_get(
                &cache,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, true),
            )
            .await,
            b"new"[..]
        );
        assert_eq!(cached_get(&cache, &path, eligible).await, b"new"[..]);
    }

    #[tokio::test]
    async fn encryption_wrapper_keeps_plaintext_out_of_raw_cache() {
        use vaulticdb::encryption::envelope::wrap_brokered_object_store;

        let origin: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let policy_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = Arc::new(
            CacheManager::new(
                origin,
                policy_store,
                CacheConfig {
                    tiers: vec![CacheTierConfig {
                        id: "encrypted".to_owned(),
                        store: ReplicaStoreConfig::Memory,
                        confidentiality: CacheConfidentiality::Encrypted,
                        policy: policy(64 * 1024),
                    }],
                    aggregate_max_bytes: None,
                    part_size_bytes: 4096,
                    max_inflight_bytes: 64 * 1024,
                    max_background_tasks: 16,
                },
                "repository",
                "encrypted-cache",
            )
            .await
            .unwrap(),
        );
        let encrypted =
            wrap_brokered_object_store("repository", cache.clone(), &[7; 32], 1).unwrap();
        let path = ObjectPath::from("sst/encrypted");
        encrypted
            .put(&path, Bytes::from_static(b"plaintext-secret").into())
            .await
            .unwrap();
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        assert_eq!(
            cached_get(&encrypted, &path, request).await,
            b"plaintext-secret"[..]
        );
        wait_for_admissions(&cache, 1).await;
        let mut objects = cache.tiers[0]
            .store
            .list(Some(&ObjectPath::from("entries")));
        while let Some(object) = objects.next().await {
            let object = object.unwrap();
            if object.location.as_ref().ends_with(".data") {
                let raw = cache.tiers[0]
                    .store
                    .get(&object.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert!(!raw
                    .windows(b"plaintext-secret".len())
                    .any(|part| part == b"plaintext-secret"));
            }
        }
        assert_eq!(
            cached_get(
                &encrypted,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false)
            )
            .await,
            b"plaintext-secret"[..]
        );
    }

    #[tokio::test]
    async fn mixed_confidentiality_tiers_cache_their_declared_byte_domains() {
        use vaulticdb::encryption::envelope::wrap_brokered_object_store;

        let origin: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = Arc::new(
            CacheManager::new(
                origin,
                Arc::new(InMemory::new()),
                CacheConfig {
                    tiers: vec![
                        CacheTierConfig {
                            id: "raw".to_owned(),
                            store: ReplicaStoreConfig::Memory,
                            confidentiality: CacheConfidentiality::Encrypted,
                            policy: policy(64 * 1024),
                        },
                        CacheTierConfig {
                            id: "trusted".to_owned(),
                            store: ReplicaStoreConfig::Memory,
                            confidentiality: CacheConfidentiality::DecryptedHighlyTrusted,
                            policy: policy(64 * 1024),
                        },
                    ],
                    aggregate_max_bytes: Some(128 * 1024),
                    part_size_bytes: 4096,
                    max_inflight_bytes: 64 * 1024,
                    max_background_tasks: 16,
                },
                "repository",
                "mixed-cache",
            )
            .await
            .unwrap(),
        );
        let encrypted =
            wrap_brokered_object_store("repository", cache.clone(), &[9; 32], 1).unwrap();
        let decrypted = cache.store(
            encrypted.clone(),
            CacheConfidentiality::DecryptedHighlyTrusted,
        );
        let path = ObjectPath::from("sst/mixed");
        decrypted
            .put(&path, Bytes::from_static(b"mixed-plaintext").into())
            .await
            .unwrap();
        assert_eq!(
            cached_get(
                &decrypted,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false),
            )
            .await,
            b"mixed-plaintext"[..]
        );
        wait_for_tier_admissions(&cache, &["raw", "trusted"]).await;

        let mut cached_payloads = Vec::new();
        for tier in &cache.tiers {
            let mut objects = tier.store.list(Some(&ObjectPath::from("entries")));
            while let Some(object) = objects.next().await {
                let object = object.unwrap();
                if object.location.as_ref().ends_with(".data") {
                    let bytes = tier
                        .store
                        .get(&object.location)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap();
                    cached_payloads.push((tier.confidentiality, bytes));
                }
            }
        }
        assert!(cached_payloads.iter().any(|(mode, bytes)| {
            *mode == CacheConfidentiality::Encrypted
                && !bytes
                    .windows(b"mixed-plaintext".len())
                    .any(|part| part == b"mixed-plaintext")
        }));
        assert!(cached_payloads.iter().any(|(mode, bytes)| {
            *mode == CacheConfidentiality::DecryptedHighlyTrusted
                && bytes.as_ref() == b"mixed-plaintext"
        }));
        let status = cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "trusted".to_owned(),
                    policy: policy(32 * 1024),
                }],
            )
            .await
            .unwrap();
        assert_eq!(status.tiers.len(), 2);
        assert_eq!(
            status.tiers[0].confidentiality,
            CacheConfidentiality::Encrypted
        );
        assert_eq!(
            status.tiers[1].confidentiality,
            CacheConfidentiality::DecryptedHighlyTrusted
        );
        assert_eq!(status.revision, 1);
        assert_eq!(status.tiers[1].policy.max_bytes, 32 * 1024);
        assert!(status.used_bytes <= status.aggregate_max_bytes.unwrap());
    }

    #[tokio::test]
    async fn confidentiality_change_on_restart_does_not_reinterpret_cached_bytes() {
        let root = std::env::temp_dir().join(format!(
            "vaulticdb-cache-confidentiality-{}",
            rand::random::<u64>()
        ));
        let path = ObjectPath::from("sst/restart");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        let first_origin = Arc::new(CountingStore::new());
        first_origin
            .put(&path, Bytes::from_static(b"encrypted-domain-value").into())
            .await
            .unwrap();
        let first = Arc::new(
            CacheManager::new(
                first_origin,
                Arc::new(InMemory::new()),
                CacheConfig {
                    tiers: vec![CacheTierConfig {
                        id: "local".to_owned(),
                        store: ReplicaStoreConfig::Local { root: root.clone() },
                        confidentiality: CacheConfidentiality::Encrypted,
                        policy: policy(64 * 1024),
                    }],
                    aggregate_max_bytes: None,
                    part_size_bytes: 4096,
                    max_inflight_bytes: 64 * 1024,
                    max_background_tasks: 16,
                },
                "repository",
                "restart-cache",
            )
            .await
            .unwrap(),
        );
        cached_get(&first, &path, request.clone()).await;
        wait_for_admissions(&first, 1).await;
        drop(first);

        let second_origin = Arc::new(CountingStore::new());
        second_origin
            .put(&path, Bytes::from_static(b"decrypted-domain-value").into())
            .await
            .unwrap();
        let second = Arc::new(
            CacheManager::new(
                second_origin.clone(),
                Arc::new(InMemory::new()),
                CacheConfig {
                    tiers: vec![CacheTierConfig {
                        id: "local".to_owned(),
                        store: ReplicaStoreConfig::Local { root: root.clone() },
                        confidentiality: CacheConfidentiality::DecryptedHighlyTrusted,
                        policy: policy(64 * 1024),
                    }],
                    aggregate_max_bytes: None,
                    part_size_bytes: 4096,
                    max_inflight_bytes: 64 * 1024,
                    max_background_tasks: 16,
                },
                "repository",
                "restart-cache",
            )
            .await
            .unwrap(),
        );
        let decrypted = second.store(
            second_origin.clone(),
            CacheConfidentiality::DecryptedHighlyTrusted,
        );
        assert_eq!(second.status().used_bytes, 0);
        assert_eq!(
            cached_get(&decrypted, &path, request).await,
            b"decrypted-domain-value"[..]
        );
        assert_eq!(second_origin.reads.load(Ordering::Acquire), 1);
        wait_for_admissions(&second, 1).await;
        drop(decrypted);
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn capacity_is_bounded_and_oversized_entries_are_rejected() {
        let (origin, cache) = manager(1_200).await;
        for name in ["one", "two", "three"] {
            let path = ObjectPath::from(format!("sst/{name}"));
            origin
                .put(&path, Bytes::from(vec![1_u8; 64]).into())
                .await
                .unwrap();
            cached_get(
                &cache,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false),
            )
            .await;
            wait_for_admissions(&cache, 1).await;
            assert!(cache.status().used_bytes <= 1_200);
        }
        let (origin, cache) = manager(128).await;
        let path = ObjectPath::from("sst/large");
        origin
            .put(&path, Bytes::from(vec![1_u8; 256]).into())
            .await
            .unwrap();
        cached_get(
            &cache,
            &path,
            options(TableStoreKind::Main, SstType::Compacted, false),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.status().metrics.admission_rejections == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background oversized admission rejection");
        assert_eq!(cache.status().used_bytes, 0);
        assert_eq!(cache.status().metrics.admission_rejections, 1);
        assert_eq!(cache.status().metrics.admission_rejections_reservation, 1);
    }

    #[tokio::test]
    async fn background_admission_rejections_report_budget_reason() {
        for task_budget in [false, true] {
            let (origin, cache) = manager(4096).await;
            let path = ObjectPath::from(if task_budget {
                "sst/task-budget"
            } else {
                "sst/byte-budget"
            });
            origin
                .put(&path, Bytes::from_static(b"value").into())
                .await
                .unwrap();
            let _permit = if task_budget {
                let permits = cache.background_task_budget.available_permits() as u32;
                cache
                    .background_task_budget
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .unwrap()
            } else {
                let permits = cache.background_budget.available_permits() as u32;
                cache
                    .background_budget
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .unwrap()
            };
            cached_get(
                &cache,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false),
            )
            .await;
            let metrics = cache.status().metrics;
            assert_eq!(metrics.admission_rejections, 1);
            assert_eq!(
                metrics.admission_rejections_background_task,
                u64::from(task_budget)
            );
            assert_eq!(
                metrics.admission_rejections_background_budget,
                u64::from(!task_budget)
            );
            assert_eq!(metrics.admission_rejections_reservation, 0);
        }
    }

    #[tokio::test]
    async fn idle_and_absolute_expiry_fall_back_to_origin() {
        for absolute in [false, true] {
            let (origin, cache) = manager(64 * 1024).await;
            let path = ObjectPath::from(if absolute { "sst/absolute" } else { "sst/idle" });
            origin
                .put(&path, Bytes::from_static(b"value").into())
                .await
                .unwrap();
            let eligible = options(TableStoreKind::Reader, SstType::Compacted, false);
            cached_get(&cache, &path, eligible.clone()).await;
            let mut expiry_policy = policy(64 * 1024);
            if absolute {
                expiry_policy.idle_age_ms = 0;
                expiry_policy.absolute_age_ms = Some(0);
            } else {
                expiry_policy.idle_age_ms = 1;
                expiry_policy.absolute_age_ms = None;
            }
            cache
                .update_policy(
                    0,
                    vec![CacheTierPolicyUpdate {
                        id: "memory".to_owned(),
                        policy: expiry_policy,
                    }],
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            cached_get(&cache, &path, eligible).await;
            assert_eq!(
                origin.reads.load(Ordering::Acquire),
                2,
                "absolute expiry: {absolute}"
            );
        }
    }

    #[tokio::test]
    async fn policy_cas_disable_and_shrink_reclaim_entries() {
        let (origin, cache) = manager(64 * 1024).await;
        let path = ObjectPath::from("sst/policy");
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(
            &cache,
            &path,
            options(TableStoreKind::Main, SstType::Compacted, false),
        )
        .await;
        let mut disabled = policy(1);
        disabled.enabled = false;
        let status = cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "memory".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        assert_eq!(status.revision, 1);
        assert_eq!(status.used_bytes, 0);
        assert!(cache.update_policy(0, Vec::new()).await.is_err());
    }

    #[tokio::test]
    async fn ranges_have_distinct_cache_identity() {
        let (origin, cache) = manager(64 * 1024).await;
        let path = ObjectPath::from("sst/ranges");
        origin
            .put(&path, Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();
        let mut first = options(TableStoreKind::Reader, SstType::Compacted, false);
        first.range = Some(GetRange::Bounded(0..4));
        let mut second = options(TableStoreKind::Reader, SstType::Compacted, false);
        second.range = Some(GetRange::Bounded(4..8));
        assert_eq!(cached_get(&cache, &path, first.clone()).await, b"0123"[..]);
        wait_for_admissions(&cache, 1).await;
        assert_eq!(cached_get(&cache, &path, second.clone()).await, b"4567"[..]);
        wait_for_admissions(&cache, 2).await;
        assert_eq!(cached_get(&cache, &path, first).await, b"0123"[..]);
        assert_eq!(cached_get(&cache, &path, second).await, b"4567"[..]);
        assert_eq!(origin.reads.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn concurrent_misses_coalesce_origin_reads() {
        let (origin, cache) = manager(64 * 1024).await;
        let path = ObjectPath::from("sst/concurrent");
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let path = path.clone();
            tasks.push(tokio::spawn(async move {
                cached_get(
                    &cache,
                    &path,
                    options(TableStoreKind::Main, SstType::Compacted, false),
                )
                .await
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap(), b"value"[..]);
        }
        assert_eq!(origin.reads.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn delayed_cache_fill_does_not_delay_origin_response() {
        let cache_store = Arc::new(CountingStore::new());
        cache_store.write_delay_ms.store(200, Ordering::Release);
        let (origin, cache) = manager_with_cache_store(64 * 1024, cache_store).await;
        let path = ObjectPath::from("sst/nonblocking");
        origin
            .put(&path, Bytes::from_static(b"origin-value").into())
            .await
            .unwrap();
        let started = Instant::now();
        assert_eq!(
            cached_get(
                &*cache,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false)
            )
            .await,
            b"origin-value"[..]
        );
        assert!(started.elapsed() < Duration::from_millis(100));
        wait_for_admissions(&cache, 1).await;
    }

    #[tokio::test]
    async fn cache_credential_denial_opens_circuit_and_origin_reads_continue() {
        let cache_store = Arc::new(CountingStore::new());
        cache_store.deny.store(true, Ordering::Release);
        let (origin, cache) = manager_with_cache_store(64 * 1024, cache_store).await;
        for index in 0..CIRCUIT_FAILURES {
            let path = ObjectPath::from(format!("sst/denied-{index}"));
            origin
                .put(&path, Bytes::from_static(b"origin").into())
                .await
                .unwrap();
            assert_eq!(
                cached_get(
                    &*cache,
                    &path,
                    options(TableStoreKind::Main, SstType::Compacted, false)
                )
                .await,
                b"origin"[..]
            );
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cache.status().tiers[0].circuit_open {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(origin.reads.load(Ordering::Acquire), CIRCUIT_FAILURES);
    }

    #[tokio::test]
    async fn disable_defers_physical_deletion_until_blocked_read_unpins() {
        let cache_store = Arc::new(CountingStore::new());
        let (origin, cache) = manager_with_cache_store(64 * 1024, cache_store.clone()).await;
        let path = ObjectPath::from("sst/pinned");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        let key = cache_key(
            &cache_domain_namespace(&cache.namespace, CacheConfidentiality::Encrypted),
            &path,
            &request,
        );
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(&*cache, &path, request.clone()).await;
        wait_for_admissions(&cache, 1).await;
        cache_store.data_read_delay_ms.store(200, Ordering::Release);
        let reader = {
            let cache = cache.clone();
            let path = path.clone();
            tokio::spawn(async move { cached_get(&*cache, &path, request).await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.status().pinned_bytes == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut disabled = policy(64 * 1024);
        disabled.enabled = false;
        let status = cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        assert!(status.used_bytes > 0);
        assert!(status.pending_reclaim_bytes > 0);
        assert_eq!(reader.await.unwrap(), b"value"[..]);
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.status().used_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(matches!(
            cache_store.head(&entry_path(&key, "meta")).await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn policy_generation_rejects_an_inflight_reservation() {
        let (_, cache) = manager(4096).await;
        let current = policy(4096);
        let reservation = cache.reserve(0, "stale", 128, 128, &current).await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let updater = {
            let cache = cache.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                let mut disabled = policy(4096);
                disabled.enabled = false;
                cache
                    .update_policy(
                        0,
                        vec![CacheTierPolicyUpdate {
                            id: "memory".to_owned(),
                            policy: disabled,
                        }],
                    )
                    .await
                    .unwrap();
                barrier.wait().await;
            })
        };
        barrier.wait().await;
        assert_eq!(
            cache
                .commit_reservation(&reservation, "stale", 128, 1, 1)
                .await,
            AdmissionCommit::Rejected
        );
        cache.release_reservation(reservation).await;
        updater.await.unwrap();
        assert_eq!(cache.status().reserved_bytes, 0);
        assert_eq!(cache.status().used_bytes, 0);
    }

    #[tokio::test]
    async fn metadata_write_failure_stays_charged_until_denied_deletion_recovers() {
        let cache_store = Arc::new(CountingStore::new());
        cache_store
            .fail_metadata_put_once
            .store(true, Ordering::Release);
        cache_store.deny_delete.store(true, Ordering::Release);
        let (origin, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        let path = ObjectPath::from("sst/metadata-write-failure");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(&*cache, &path, request).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.pending_deletions.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        let entry = ledger.entries.values().next().unwrap();
        assert_eq!(entry.state, QuotaEntryState::Deleting);
        assert!(entry.bytes > 0);
        assert!(cache.status().used_bytes > 0);
        assert_eq!(cache.status().reserved_bytes, 0);

        cache_store.deny_delete.store(false, Ordering::Release);
        cache.deletion_wake.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cache.status().used_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn policy_rejection_after_write_stays_charged_when_deletion_is_denied() {
        let cache_store = Arc::new(CountingStore::new());
        cache_store.write_delay_ms.store(100, Ordering::Release);
        let (origin, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        let path = ObjectPath::from("sst/post-write-policy-rejection");
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(
            &*cache,
            &path,
            options(TableStoreKind::Main, SstType::Compacted, false),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache_store.writes.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cache_store.deny_delete.store(true, Ordering::Release);
        let mut disabled = policy(4096);
        disabled.enabled = false;
        cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.pending_deletions.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        assert!(ledger
            .entries
            .values()
            .any(|entry| entry.state == QuotaEntryState::Deleting));
        assert!(cache.status().used_bytes > 0);
        assert_eq!(cache.status().reserved_bytes, 0);
    }

    #[tokio::test]
    async fn concurrent_tiers_cannot_double_allocate_aggregate_capacity() {
        let origin: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = Arc::new(
            CacheManager::new(
                origin,
                Arc::new(InMemory::new()),
                CacheConfig {
                    tiers: vec![
                        CacheTierConfig {
                            id: "first".to_owned(),
                            store: ReplicaStoreConfig::Memory,
                            confidentiality: CacheConfidentiality::Encrypted,
                            policy: policy(128),
                        },
                        CacheTierConfig {
                            id: "second".to_owned(),
                            store: ReplicaStoreConfig::Memory,
                            confidentiality: CacheConfidentiality::Encrypted,
                            policy: policy(128),
                        },
                    ],
                    aggregate_max_bytes: Some(128),
                    part_size_bytes: 4096,
                    max_inflight_bytes: 4096,
                    max_background_tasks: 1,
                },
                "repository",
                "aggregate-race",
            )
            .await
            .unwrap(),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for index in 0..2 {
            let cache = cache.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .reserve(index, &format!("entry-{index}"), 128, 128, &policy(128))
                    .await
            }));
        }
        barrier.wait().await;
        let reservations = futures_util::future::join_all(tasks)
            .await
            .into_iter()
            .filter_map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(reservations.len(), 1);
        assert_eq!(cache.status().reserved_bytes, 128);
        cache.release_reservation(reservations[0].clone()).await;
        assert_eq!(cache.status().reserved_bytes, 0);
    }

    #[tokio::test]
    async fn shared_managers_cannot_overreserve_tier_or_aggregate_capacity() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let quota_options = QuotaOptions {
            lease_duration: Duration::from_secs(1),
            renew_interval: Duration::from_secs(60),
        };
        let (_, first) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store.clone(),
            policy_store.clone(),
            "shared-race",
            quota_options,
        )
        .await;
        let (_, second) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store,
            policy_store,
            "shared-race",
            quota_options,
        )
        .await;
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for (manager, key) in [(first.clone(), "first"), (second.clone(), "second")] {
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                manager.reserve(0, key, 128, 128, &policy(128)).await
            }));
        }
        barrier.wait().await;
        let reservations = futures_util::future::join_all(tasks)
            .await
            .into_iter()
            .filter_map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(reservations.len(), 1);
        let (ledger, _) = first.read_quota_ledger().await.unwrap();
        assert_eq!(
            ledger
                .managers
                .values()
                .flat_map(|grant| grant.reserved_by_tier.values())
                .sum::<u64>(),
            128
        );
    }

    #[tokio::test]
    async fn crashed_manager_reservation_stays_charged_until_lease_expiry() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let quota_options = QuotaOptions {
            lease_duration: Duration::from_millis(80),
            renew_interval: Duration::from_secs(60),
        };
        let (_, crashed) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store.clone(),
            policy_store.clone(),
            "crash-lease",
            quota_options,
        )
        .await;
        assert!(crashed
            .reserve(0, "crashed", 128, 128, &policy(128))
            .await
            .is_some());
        drop(crashed);
        let (_, before_expiry) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store.clone(),
            policy_store.clone(),
            "crash-lease",
            quota_options,
        )
        .await;
        assert!(before_expiry
            .reserve(0, "blocked", 128, 128, &policy(128))
            .await
            .is_none());
        drop(before_expiry);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, after_expiry) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store,
            policy_store,
            "crash-lease",
            quota_options,
        )
        .await;
        assert!(after_expiry
            .reserve(0, "reclaimed", 128, 128, &policy(128))
            .await
            .is_some());
    }

    #[tokio::test]
    async fn healthy_manager_reconciles_expired_crashed_manager_without_restart() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let quota_options = QuotaOptions {
            lease_duration: Duration::from_millis(80),
            renew_interval: Duration::from_millis(20),
        };
        let (_, first) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store.clone(),
            policy_store.clone(),
            "healthy-reclaims-crash",
            quota_options,
        )
        .await;
        let (_, second) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store,
            policy_store,
            "healthy-reclaims-crash",
            quota_options,
        )
        .await;
        assert!(first
            .reserve(0, "crashed", 128, 128, &policy(128))
            .await
            .is_some());
        assert!(second
            .reserve(0, "blocked", 128, 128, &policy(128))
            .await
            .is_none());

        let _ = first.quota.cancel.send(true);
        if let Some(task) = first.quota.task.lock().await.take() {
            task.await.unwrap();
        }

        let reservation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(reservation) =
                    second.reserve(0, "reclaimed", 128, 128, &policy(128)).await
                {
                    break reservation;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("healthy manager should reconcile expired foreign reservation");
        let (ledger, _) = second.read_quota_ledger().await.unwrap();
        assert!(!ledger.managers.contains_key(&first.quota.manager_id));
        assert_eq!(reservation.bytes, 128);
    }

    #[tokio::test]
    async fn heartbeat_and_rpc_policy_installations_are_serialized() {
        let cache = two_tier_manager("policy-install-race").await;
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let heartbeat = {
            let cache = cache.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                cache.load_policy().await
            })
        };
        let rpc = {
            let cache = cache.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .update_policy(
                        0,
                        vec![
                            CacheTierPolicyUpdate {
                                id: "first".to_owned(),
                                policy: policy(1_001),
                            },
                            CacheTierPolicyUpdate {
                                id: "second".to_owned(),
                                policy: policy(2_001),
                            },
                        ],
                    )
                    .await
            })
        };
        barrier.wait().await;
        heartbeat.await.unwrap().unwrap();
        rpc.await.unwrap().unwrap();
        let status = cache.status();
        assert_eq!(status.revision, 1);
        assert_eq!(status.tiers[0].policy.max_bytes, 1_001);
        assert_eq!(status.tiers[1].policy.max_bytes, 2_001);
    }

    #[tokio::test]
    async fn observed_policy_revision_always_has_its_complete_vector() {
        let cache = two_tier_manager("policy-vector-invariant").await;
        let updater = {
            let cache = cache.clone();
            tokio::spawn(async move {
                for revision in 1..=32 {
                    cache
                        .update_policy(
                            revision - 1,
                            vec![
                                CacheTierPolicyUpdate {
                                    id: "first".to_owned(),
                                    policy: policy(1_000 + revision),
                                },
                                CacheTierPolicyUpdate {
                                    id: "second".to_owned(),
                                    policy: policy(2_000 + revision),
                                },
                            ],
                        )
                        .await
                        .unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };
        while !updater.is_finished() {
            let status = cache.status();
            assert_eq!(status.tiers[0].policy.max_bytes, 1_000 + status.revision);
            assert_eq!(status.tiers[1].policy.max_bytes, 2_000 + status.revision);
            tokio::task::yield_now().await;
        }
        updater.await.unwrap();
        let status = cache.status();
        assert_eq!(status.revision, 32);
        assert_eq!(status.tiers[0].policy.max_bytes, 1_032);
        assert_eq!(status.tiers[1].policy.max_bytes, 2_032);
    }

    #[tokio::test]
    async fn clean_close_releases_shared_reservations() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let quota_options = QuotaOptions {
            lease_duration: Duration::from_secs(1),
            renew_interval: Duration::from_secs(60),
        };
        let (_, first) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store.clone(),
            policy_store.clone(),
            "clean-release",
            quota_options,
        )
        .await;
        assert!(first
            .reserve(0, "first", 128, 128, &policy(128))
            .await
            .is_some());
        first.close().await.unwrap();
        let (_, second) = manager_with_shared_stores(
            128,
            Some(128),
            cache_store,
            policy_store,
            "clean-release",
            quota_options,
        )
        .await;
        assert!(second
            .reserve(0, "second", 128, 128, &policy(128))
            .await
            .is_some());
    }

    #[tokio::test]
    async fn coordination_outage_blocks_fill_but_origin_read_succeeds() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (origin, manager) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store.clone(),
            "coordination-outage",
            QuotaOptions::default(),
        )
        .await;
        let seed_path = ObjectPath::from("sst/seed");
        origin
            .put(&seed_path, Bytes::from_static(b"cached-value").into())
            .await
            .unwrap();
        cached_get(
            &*manager,
            &seed_path,
            options(TableStoreKind::Main, SstType::Compacted, false),
        )
        .await;
        wait_for_admissions(&manager, 1).await;
        let path = ObjectPath::from("sst/outage");
        origin
            .put(&path, Bytes::from_static(b"origin-value").into())
            .await
            .unwrap();
        policy_store.deny.store(true, Ordering::Release);
        assert_eq!(
            cached_get(
                &*manager,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false)
            )
            .await,
            b"origin-value"[..]
        );
        renew_quota_lease(
            &manager.policy_store,
            &manager.namespace,
            manager.aggregate_max_bytes,
            Some(manager.policy().revision),
            &manager.quota,
        )
        .await;
        assert_eq!(manager.status().metrics.admissions, 1);
        assert!(!manager.status().quota_coordination_healthy);
        assert!(manager.status().unverified_stale_bytes > 0);
    }

    #[tokio::test]
    async fn policy_shrink_fences_admission_across_managers() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, first) = manager_with_shared_stores(
            256,
            Some(256),
            cache_store.clone(),
            policy_store.clone(),
            "shared-shrink",
            QuotaOptions::default(),
        )
        .await;
        let (_, second) = manager_with_shared_stores(
            256,
            Some(256),
            cache_store,
            policy_store,
            "shared-shrink",
            QuotaOptions::default(),
        )
        .await;
        first
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: policy(64),
                }],
            )
            .await
            .unwrap();
        assert!(second
            .reserve(0, "too-large", 128, 128, &policy(256))
            .await
            .is_none());
        assert_eq!(second.status().revision, 1);
    }

    #[tokio::test]
    async fn restart_reconciliation_reuses_shared_cached_entries() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (origin, first) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "restart-reuse",
            QuotaOptions::default(),
        )
        .await;
        let path = ObjectPath::from("sst/reused");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        origin
            .put(&path, Bytes::from_static(b"persisted-cache-value").into())
            .await
            .unwrap();
        assert_eq!(
            cached_get(&*first, &path, request.clone()).await,
            b"persisted-cache-value"[..]
        );
        wait_for_admissions(&first, 1).await;
        first.close().await.unwrap();

        let (reopened_origin, reopened) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store,
            "restart-reuse",
            QuotaOptions::default(),
        )
        .await;
        assert_eq!(
            cached_get(&*reopened, &path, request).await,
            b"persisted-cache-value"[..]
        );
        assert_eq!(reopened_origin.reads.load(Ordering::Acquire), 0);
        assert!(reopened.status().quota_coordination_healthy);
        assert_eq!(reopened.status().unverified_stale_bytes, 0);
    }

    #[tokio::test]
    async fn local_backend_reuses_entries_without_crossing_confidentiality_namespaces() {
        let root = std::env::temp_dir().join(format!(
            "vaulticdb-read-cache-{}",
            sha256_hex(rand::random::<[u8; 32]>())
        ));
        let policy_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let origin = Arc::new(CountingStore::new());
        let config = |confidentiality| CacheConfig {
            tiers: vec![CacheTierConfig {
                id: "local".to_owned(),
                store: ReplicaStoreConfig::Local { root: root.clone() },
                confidentiality,
                policy: policy(4096),
            }],
            aggregate_max_bytes: Some(4096),
            part_size_bytes: 4096,
            max_inflight_bytes: 4096,
            max_background_tasks: 1,
        };
        let path = ObjectPath::from("sst/local-restart");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        origin
            .put(&path, Bytes::from_static(b"local-cache-value").into())
            .await
            .unwrap();

        let first = CacheManager::new(
            origin.clone(),
            policy_store.clone(),
            config(CacheConfidentiality::Encrypted),
            "repository",
            "local-adapter",
        )
        .await
        .unwrap();
        assert_eq!(
            cached_get(&first, &path, request.clone()).await,
            b"local-cache-value"[..]
        );
        wait_for_admissions(&first, 1).await;
        first.close().await.unwrap();

        let reopened = CacheManager::new(
            origin.clone(),
            policy_store.clone(),
            config(CacheConfidentiality::Encrypted),
            "repository",
            "local-adapter",
        )
        .await
        .unwrap();
        assert_eq!(
            cached_get(&reopened, &path, request.clone()).await,
            b"local-cache-value"[..]
        );
        assert_eq!(origin.reads.load(Ordering::Acquire), 1);
        reopened.close().await.unwrap();

        let decrypted = Arc::new(
            CacheManager::new(
                origin.clone(),
                policy_store,
                config(CacheConfidentiality::DecryptedHighlyTrusted),
                "repository",
                "local-adapter",
            )
            .await
            .unwrap(),
        );
        let decrypted_store =
            decrypted.store(origin.clone(), CacheConfidentiality::DecryptedHighlyTrusted);
        assert_eq!(
            cached_get(&*decrypted_store, &path, request).await,
            b"local-cache-value"[..]
        );
        assert_eq!(origin.reads.load(Ordering::Acquire), 2);
        wait_for_admissions(&decrypted, 1).await;
        decrypted.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn deleting_generation_blocks_other_manager_admission_until_exact_removal() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (origin, first) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "delete-admit-race",
            QuotaOptions::default(),
        )
        .await;
        let (_, second) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store,
            "delete-admit-race",
            QuotaOptions::default(),
        )
        .await;
        let path = ObjectPath::from("sst/delete-admit");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(&*first, &path, request.clone()).await;
        wait_for_admissions(&first, 1).await;
        second.reconcile().await;
        let key = cache_key(
            &cache_domain_namespace(&first.namespace, CacheConfidentiality::Encrypted),
            &path,
            &request,
        );
        let generation = first
            .mark_shared_entry_deleting(0, &key)
            .await
            .unwrap()
            .unwrap();
        let status = first.status();
        assert!(status.deletion_pending_bytes > 0);
        assert!(status.tiers[0].deletion_pending_bytes > 0);
        assert!(second
            .reserve(0, &key, 128, 128, &policy(4096))
            .await
            .is_none());
        assert!(first.delete_objects(0, &key).await);
        assert!(first.finalize_delete(0, &key, generation).await);
        assert!(second
            .reserve(0, &key, 128, 128, &policy(4096))
            .await
            .is_some());
        let status = second.status();
        assert_eq!(status.used_bytes, 0);
        assert_eq!(status.reserved_bytes, 128);
    }

    #[tokio::test]
    async fn active_reconciler_fences_entry_mutations() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, first) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "reconcile-mutation-race",
            QuotaOptions::default(),
        )
        .await;
        let (_, second) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store,
            "reconcile-mutation-race",
            QuotaOptions::default(),
        )
        .await;
        let (mut ledger, version) = first.read_quota_ledger().await.unwrap();
        ledger.reconciler = Some(QuotaReconciler {
            manager_id: first.quota.manager_id.clone(),
            lease_expires_ms: now_ms().saturating_add(10_000),
        });
        ledger.revision = ledger.revision.saturating_add(1);
        first.write_quota_ledger(&ledger, version).await.unwrap();
        assert!(second
            .reserve(0, "fenced", 128, 128, &policy(4096))
            .await
            .is_none());
        assert!(second
            .mark_shared_entry_deleting(0, "fenced")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn inventory_aborts_when_ledger_revision_changes() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store,
            "inventory-revision-race",
            QuotaOptions::default(),
        )
        .await;
        cache_store
            .put(
                &ObjectPath::from("entries/race.data"),
                Bytes::from_static(b"value").into(),
            )
            .await
            .unwrap();
        cache_store.list_delay_ms.store(100, Ordering::Release);
        let recovery = {
            let cache = cache.clone();
            tokio::spawn(async move {
                recover_quota_coordination(
                    &cache.policy_store,
                    &cache.namespace,
                    cache.aggregate_max_bytes,
                    &cache.tiers,
                    &cache.policy(),
                    &cache.capacity,
                    &cache.quota,
                    &cache.pending_deletions,
                    &cache.deletion_wake,
                )
                .await
            })
        };
        let (mut ledger, version) = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let current = cache.read_quota_ledger().await.unwrap();
                if current.0.reconciler.is_some() {
                    break current;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        ledger.revision = ledger.revision.saturating_add(1);
        cache.write_quota_ledger(&ledger, version).await.unwrap();
        assert!(!recovery.await.unwrap());
    }

    #[tokio::test]
    async fn deletion_outage_retries_until_disabled_tier_is_reclaimed() {
        let cache_store = Arc::new(CountingStore::new());
        let (origin, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        let path = ObjectPath::from("sst/delete-retry");
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(
            &*cache,
            &path,
            options(TableStoreKind::Main, SstType::Compacted, false),
        )
        .await;
        wait_for_admissions(&cache, 1).await;
        cache_store.deny_delete.store(true, Ordering::Release);
        let mut disabled = policy(4096);
        disabled.enabled = false;
        cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        assert!(cache.status().used_bytes > 0);
        cache_store.deny_delete.store(false, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), async {
            while cache.status().used_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn deletion_burst_exceeding_task_budget_is_not_dropped() {
        let cache_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        cache_store.deny_delete.store(true, Ordering::Release);
        let (mut ledger, version) = cache.read_quota_ledger().await.unwrap();
        let burst = 32_u64;
        for generation in 1..=burst {
            let key = format!("burst-{generation}");
            cache_store
                .put(&entry_path(&key, "data"), Bytes::from_static(b"d").into())
                .await
                .unwrap();
            cache_store
                .put(&entry_path(&key, "meta"), Bytes::from_static(b"m").into())
                .await
                .unwrap();
            {
                let mut state = cache.capacity.lock().unwrap();
                cache.commit_entry_locked(&mut state, 0, &key, 2, 1, 1, generation);
            }
            let entry_id = quota_entry_id(&cache.tiers[0], &key);
            ledger.entries.insert(
                entry_id,
                QuotaEntry {
                    key: key.clone(),
                    tier_id: "controlled".to_owned(),
                    owner_manager_id: cache.quota.manager_id.clone(),
                    bytes: 2,
                    generation,
                    policy_revision: 0,
                    expires_ms: 0,
                    state: QuotaEntryState::Active,
                },
            );
            cache.evict(0, &key, EvictionReason::Capacity).await;
        }
        ledger.next_generation = burst;
        *ledger
            .managers
            .get_mut(&cache.quota.manager_id)
            .unwrap()
            .committed_by_tier
            .entry("controlled".to_owned())
            .or_default() = burst * 2;
        ledger.revision = ledger.revision.saturating_add(1);
        cache.write_quota_ledger(&ledger, version).await.unwrap();
        assert_eq!(
            cache.pending_deletions.lock().unwrap().len(),
            burst as usize
        );
        cache_store.deny_delete.store(false, Ordering::Release);
        cache.deletion_wake.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !cache.pending_deletions.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(cache.status().used_bytes, 0);
    }

    #[tokio::test]
    async fn deletion_worker_is_idle_between_heartbeats_and_wakes_promptly() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "idle-deletion-worker",
            QuotaOptions {
                lease_duration: Duration::from_secs(2),
                renew_interval: Duration::from_millis(500),
            },
        )
        .await;
        let idle_reads = policy_store.reads.load(Ordering::Acquire);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(policy_store.reads.load(Ordering::Acquire), idle_reads);

        let key = "prompt-local-deletion";
        cache_store
            .put(&entry_path(key, "data"), Bytes::from_static(b"x").into())
            .await
            .unwrap();
        cache_store
            .put(&entry_path(key, "meta"), Bytes::from_static(b"m").into())
            .await
            .unwrap();
        let (mut ledger, version) = cache.read_quota_ledger().await.unwrap();
        let generation = 1;
        ledger.next_generation = generation;
        ledger.entries.insert(
            quota_entry_id(&cache.tiers[0], key),
            QuotaEntry {
                key: key.to_owned(),
                tier_id: "controlled".to_owned(),
                owner_manager_id: cache.quota.manager_id.clone(),
                bytes: 2,
                generation,
                policy_revision: 0,
                expires_ms: 0,
                state: QuotaEntryState::Active,
            },
        );
        *ledger
            .managers
            .get_mut(&cache.quota.manager_id)
            .unwrap()
            .committed_by_tier
            .entry("controlled".to_owned())
            .or_default() = 2;
        ledger.revision = ledger.revision.saturating_add(1);
        cache.write_quota_ledger(&ledger, version).await.unwrap();
        cache.commit_entry(0, key, 2, 1, 1);

        cache.evict(0, key, EvictionReason::Capacity).await;
        tokio::time::timeout(Duration::from_millis(150), async {
            while !cache.pending_deletions.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            cache_store.head(&entry_path(key, "data")).await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn hit_only_manager_observes_remote_disable() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let quota_options = QuotaOptions {
            lease_duration: Duration::from_secs(1),
            renew_interval: Duration::from_millis(20),
        };
        let (first_origin, first) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "hit-only-disable",
            quota_options,
        )
        .await;
        let (second_origin, second) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store,
            "hit-only-disable",
            quota_options,
        )
        .await;
        let path = ObjectPath::from("sst/hit-only");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        for origin in [&first_origin, &second_origin] {
            origin
                .put(&path, Bytes::from_static(b"value").into())
                .await
                .unwrap();
        }
        cached_get(&*first, &path, request.clone()).await;
        wait_for_admissions(&first, 1).await;
        second.reconcile().await;
        assert_eq!(
            cached_get(&*second, &path, request.clone()).await,
            b"value"[..]
        );
        assert_eq!(second_origin.reads.load(Ordering::Acquire), 0);
        cache_store.deny_delete.store(true, Ordering::Release);
        let mut disabled = policy(4096);
        disabled.enabled = false;
        first
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while second.status().revision != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(cached_get(&*second, &path, request).await, b"value"[..]);
        assert_eq!(second_origin.reads.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn idle_manager_enforces_remote_disable() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let quota_options = QuotaOptions {
            lease_duration: Duration::from_secs(1),
            renew_interval: Duration::from_millis(20),
        };
        let (_, first) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "idle-remote-disable",
            quota_options,
        )
        .await;
        let (second_origin, second) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store,
            "idle-remote-disable",
            quota_options,
        )
        .await;
        let path = ObjectPath::from("sst/idle-remote-disable");
        second_origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(
            &*second,
            &path,
            options(TableStoreKind::Main, SstType::Compacted, false),
        )
        .await;
        wait_for_admissions(&second, 1).await;
        assert!(second.status().local_used_bytes > 0);

        let mut disabled = policy(4096);
        disabled.enabled = false;
        first
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let status = second.status();
                if status.revision == 1 && status.local_used_bytes == 0 && status.used_bytes == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            cache_store
                .head(&entry_path(
                    &cache_key(
                        &second.namespace,
                        &path,
                        &options(TableStoreKind::Main, SstType::Compacted, false),
                    ),
                    "data"
                ))
                .await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn reconciler_retires_only_expired_foreign_ownership() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let quota_options = QuotaOptions {
            lease_duration: Duration::from_secs(1),
            renew_interval: Duration::from_secs(20),
        };
        let (_, first) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "expired-foreign-owner",
            quota_options,
        )
        .await;
        let (second_origin, second) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store,
            "expired-foreign-owner",
            quota_options,
        )
        .await;
        let path = ObjectPath::from("sst/expired-foreign-owner");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        let key = cache_key(&second.namespace, &path, &request);
        second_origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(&*second, &path, request).await;
        wait_for_admissions(&second, 1).await;
        assert!(first.reconcile().await);
        assert!(first.status().local_used_bytes > 0);
        assert!(first
            .mark_shared_entry_deleting(0, &key)
            .await
            .unwrap()
            .is_none());

        let _ = second.quota.cancel.send(true);
        if let Some(task) = second.quota.task.lock().await.take() {
            task.await.unwrap();
        }
        let mut disabled = policy(4096);
        disabled.enabled = false;
        first
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        let (mut ledger, version) = first.read_quota_ledger().await.unwrap();
        ledger
            .managers
            .get_mut(&second.quota.manager_id)
            .unwrap()
            .lease_expires_ms = 0;
        ledger.revision = ledger.revision.saturating_add(1);
        first.write_quota_ledger(&ledger, version).await.unwrap();
        first.deletion_wake.notify_one();

        tokio::time::timeout(Duration::from_secs(1), async {
            while first.status().used_bytes != 0 || first.status().local_used_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn committed_policy_survives_lost_cas_response() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store.clone(),
            "policy-response-lost",
            QuotaOptions::default(),
        )
        .await;
        policy_store
            .fail_put_after_commit_once
            .store(true, Ordering::Release);
        let mut disabled = policy(4096);
        disabled.enabled = false;
        let status = cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: disabled,
                }],
            )
            .await
            .unwrap();
        assert_eq!(status.revision, 1);
        assert!(!status.tiers[0].policy.enabled);
    }

    #[tokio::test]
    async fn quota_deletion_survives_lost_cas_response() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (origin, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store.clone(),
            "delete-response-lost",
            QuotaOptions::default(),
        )
        .await;
        let path = ObjectPath::from("sst/delete-response-lost");
        let request = options(TableStoreKind::Main, SstType::Compacted, false);
        let key = cache_key(
            &cache_domain_namespace(&cache.namespace, CacheConfidentiality::Encrypted),
            &path,
            &request,
        );
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        cached_get(&*cache, &path, request).await;
        wait_for_admissions(&cache, 1).await;
        policy_store
            .fail_put_after_commit_once
            .store(true, Ordering::Release);
        cache.enqueue_pending_deletion(0, &key, None);
        cache.process_pending_deletions(false).await;
        assert!(!cache.pending_deletions.lock().unwrap().is_empty());
        for pending in cache.pending_deletions.lock().unwrap().values_mut() {
            pending.retry_at_ms = 0;
        }
        cache.process_pending_deletions(false).await;
        assert!(cache.pending_deletions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn startup_policy_outage_fails_closed_then_recovers() {
        let origin = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        policy_store.deny.store(true, Ordering::Release);
        let cache = CacheManager::new_with_quota_options(
            origin.clone(),
            policy_store.clone(),
            CacheConfig {
                tiers: vec![CacheTierConfig {
                    id: "memory".to_owned(),
                    store: ReplicaStoreConfig::Memory,
                    confidentiality: CacheConfidentiality::Encrypted,
                    policy: policy(4096),
                }],
                aggregate_max_bytes: Some(4096),
                part_size_bytes: 4096,
                max_inflight_bytes: 4096,
                max_background_tasks: 1,
            },
            "repository",
            "startup-policy-outage",
            QuotaOptions {
                lease_duration: Duration::from_millis(100),
                renew_interval: Duration::from_millis(20),
            },
        )
        .await
        .unwrap();
        assert!(!cache.status().tiers[0].policy.enabled);
        policy_store.deny.store(false, Ordering::Release);
        let document = PersistedPolicy {
            format: CACHE_FORMAT,
            namespace: cache.namespace.clone(),
            revision: 0,
            tiers: vec![CacheTierPolicyUpdate {
                id: "memory".to_owned(),
                policy: policy(4096),
            }],
        };
        policy_store
            .put(
                &cache.policy_path,
                serde_json::to_vec(&document).unwrap().into(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cache.status().tiers[0].policy.enabled
                || !cache.status().quota_coordination_healthy
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn heartbeat_reacquires_an_expired_manager_lease() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store,
            "lease-recovery",
            QuotaOptions {
                lease_duration: Duration::from_millis(80),
                renew_interval: Duration::from_millis(20),
            },
        )
        .await;
        let (mut ledger, version) = cache.read_quota_ledger().await.unwrap();
        ledger
            .managers
            .get_mut(&cache.quota.manager_id)
            .unwrap()
            .lease_expires_ms = 0;
        ledger.revision = ledger.revision.saturating_add(1);
        cache.write_quota_ledger(&ledger, version).await.unwrap();
        cache.quota.healthy.store(false, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cache.status().quota_coordination_healthy
                || cache.status().quota_lease_expiry_ms <= now_ms()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(cache
            .reserve(0, "after-recovery", 128, 128, &policy(4096))
            .await
            .is_some());
    }

    #[tokio::test]
    async fn missing_or_rolled_back_policy_after_revision_one_stays_fail_closed() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store.clone(),
            "policy-rollback",
            QuotaOptions {
                lease_duration: Duration::from_secs(1),
                renew_interval: Duration::from_millis(20),
            },
        )
        .await;
        cache
            .update_policy(
                0,
                vec![CacheTierPolicyUpdate {
                    id: "controlled".to_owned(),
                    policy: policy(4096),
                }],
            )
            .await
            .unwrap();
        policy_store.inner.delete(&cache.policy_path).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.status().tiers[0].policy.enabled
                || cache.status().policy_sync_error.is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(cache.status().revision, 1);
        assert!(cache
            .reserve(0, "blocked", 64, 64, &policy(4096))
            .await
            .is_none());
        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        assert_eq!(ledger.policy_revision, 1);

        let rollback = PersistedPolicy {
            format: CACHE_FORMAT,
            namespace: cache.namespace.clone(),
            revision: 0,
            tiers: vec![CacheTierPolicyUpdate {
                id: "controlled".to_owned(),
                policy: policy(4096),
            }],
        };
        policy_store
            .inner
            .put(
                &cache.policy_path,
                serde_json::to_vec(&rollback).unwrap().into(),
            )
            .await
            .unwrap();
        assert!(cache.load_policy().await.is_err());
        assert_eq!(cache.status().revision, 1);
        let mut equivocated_policy = policy(4096);
        equivocated_policy.enabled = false;
        let equivocated = PersistedPolicy {
            format: CACHE_FORMAT,
            namespace: cache.namespace.clone(),
            revision: 1,
            tiers: vec![CacheTierPolicyUpdate {
                id: "controlled".to_owned(),
                policy: equivocated_policy,
            }],
        };
        policy_store
            .inner
            .put(
                &cache.policy_path,
                serde_json::to_vec(&equivocated).unwrap().into(),
            )
            .await
            .unwrap();
        assert!(cache.load_policy().await.is_err());
        policy_store
            .inner
            .put(&cache.policy_path, Bytes::from_static(b"not-json").into())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cache.status().policy_sync_error.contains("decode") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!cache.status().tiers[0].policy.enabled);
        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        assert_eq!(ledger.policy_revision, 1);
    }

    #[tokio::test]
    async fn update_policy_at_revision_zero_does_not_recreate_a_deleted_document() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store.clone(),
            "policy-revision-zero-delete",
            QuotaOptions::default(),
        )
        .await;
        policy_store.inner.delete(&cache.policy_path).await.unwrap();

        assert!(cache.update_policy(0, Vec::new()).await.is_err());
        assert!(matches!(
            policy_store.inner.head(&cache.policy_path).await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn update_policy_rejects_same_revision_altered_persisted_contents() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store.clone(),
            "policy-update-equivocation",
            QuotaOptions::default(),
        )
        .await;
        let mut altered = policy(4096);
        altered.enabled = false;
        let document = PersistedPolicy {
            format: CACHE_FORMAT,
            namespace: cache.namespace.clone(),
            revision: 0,
            tiers: vec![CacheTierPolicyUpdate {
                id: "controlled".to_owned(),
                policy: altered,
            }],
        };
        policy_store
            .inner
            .put(
                &cache.policy_path,
                serde_json::to_vec(&document).unwrap().into(),
            )
            .await
            .unwrap();

        assert!(cache.update_policy(0, Vec::new()).await.is_err());
        assert_eq!(cache.status().revision, 0);
        assert!(cache.status().tiers[0].policy.enabled);
    }

    #[tokio::test]
    async fn reconciliation_preserves_admitting_generation_before_physical_write() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store,
            "admit-reconcile-race",
            QuotaOptions::default(),
        )
        .await;
        let reservation = cache
            .reserve(0, "pending", 128, 128, &policy(4096))
            .await
            .unwrap();
        assert!(
            recover_quota_coordination(
                &cache.policy_store,
                &cache.namespace,
                cache.aggregate_max_bytes,
                &cache.tiers,
                &cache.policy(),
                &cache.capacity,
                &cache.quota,
                &cache.pending_deletions,
                &cache.deletion_wake,
            )
            .await
        );
        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        let entry = ledger.entries.get(&reservation.quota_entry_id).unwrap();
        assert_eq!(entry.state, QuotaEntryState::Admitting);
        assert_eq!(entry.generation, reservation.generation);
        assert_eq!(entry.bytes, reservation.bytes);
        cache.release_reservation(reservation).await;
    }

    #[tokio::test]
    async fn restart_discovers_charged_orphan_and_removes_it_after_recovery() {
        let cache_store = Arc::new(CountingStore::new());
        cache_store
            .put(
                &entry_path("restart-orphan", "data"),
                Bytes::from_static(b"orphaned-bytes").into(),
            )
            .await
            .unwrap();
        cache_store.deny_delete.store(true, Ordering::Release);
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store,
            "restart-orphan",
            QuotaOptions::default(),
        )
        .await;

        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        let entry = ledger.entries.values().next().unwrap();
        assert_eq!(entry.state, QuotaEntryState::Deleting);
        assert_eq!(entry.owner_manager_id, cache.quota.manager_id);
        assert_eq!(entry.bytes, b"orphaned-bytes".len() as u64);
        assert_eq!(cache.status().used_bytes, b"orphaned-bytes".len() as u64);

        cache_store.deny_delete.store(false, Ordering::Release);
        cache.deletion_wake.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cache.status().used_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            cache_store
                .head(&entry_path("restart-orphan", "data"))
                .await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn startup_partial_inventory_blocks_admission_until_heartbeat_recovery() {
        let cache_store = Arc::new(CountingStore::new());
        let orphan_path = entry_path("startup-partial-orphan", "data");
        cache_store
            .put(&orphan_path, Bytes::from_static(b"orphaned-bytes").into())
            .await
            .unwrap();
        cache_store.list_error_after.store(1, Ordering::Release);
        let origin = Arc::new(CountingStore::new());
        let first_path = ObjectPath::from("sst/before-recovery");
        origin
            .put(&first_path, Bytes::from_static(b"origin-before").into())
            .await
            .unwrap();
        let cache = Arc::new(
            CacheManager::new_with_quota_options(
                origin.clone(),
                Arc::new(CountingStore::new()),
                CacheConfig {
                    tiers: vec![CacheTierConfig {
                        id: "controlled".to_owned(),
                        store: ReplicaStoreConfig::Test(cache_store.clone()),
                        confidentiality: CacheConfidentiality::Encrypted,
                        policy: policy(4096),
                    }],
                    aggregate_max_bytes: Some(4096),
                    part_size_bytes: 4096,
                    max_inflight_bytes: 4096,
                    max_background_tasks: 1,
                },
                "repository",
                "startup-partial-inventory",
                QuotaOptions {
                    lease_duration: Duration::from_millis(200),
                    renew_interval: Duration::from_millis(20),
                },
            )
            .await
            .unwrap(),
        );

        let status = cache.status();
        assert!(status.tiers[0].policy.enabled);
        assert_eq!(status.policy_sync_lag, 0);
        assert!(!status.quota_coordination_healthy);
        assert!(status.quota_reconciliation_lag > 0);
        assert!(status.tiers[0].reconciliation_lag > 0);
        assert!(cache.quota.reconciliation_pending.load(Ordering::Acquire));
        assert_eq!(
            cache
                .quota
                .reconciliation_retry_at_ms
                .load(Ordering::Acquire),
            0
        );
        let startup_revision = status.quota_ledger_revision;
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.status().quota_ledger_revision <= startup_revision {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let status = cache.status();
        assert!(status.tiers[0].policy.enabled);
        assert_eq!(status.policy_sync_lag, 0);
        assert!(!status.quota_coordination_healthy);
        assert!(cache.quota.reconciliation_pending.load(Ordering::Acquire));
        assert_eq!(
            cached_get(
                &cache,
                &first_path,
                options(TableStoreKind::Main, SstType::Compacted, false),
            )
            .await,
            b"origin-before"[..]
        );
        tokio::task::yield_now().await;
        assert_eq!(cache.status().metrics.origin_reads, 1);
        assert_eq!(cache.status().metrics.admissions, 0);

        cache_store
            .list_error_after
            .store(u64::MAX, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let status = cache.status();
                let orphan_removed = matches!(
                    cache_store.head(&orphan_path).await,
                    Err(slatedb::object_store::Error::NotFound { .. })
                );
                if status.quota_coordination_healthy && orphan_removed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let second_path = ObjectPath::from("sst/after-recovery");
        origin
            .put(&second_path, Bytes::from_static(b"origin-after").into())
            .await
            .unwrap();
        assert_eq!(
            cached_get(
                &cache,
                &second_path,
                options(TableStoreKind::Main, SstType::Compacted, false),
            )
            .await,
            b"origin-after"[..]
        );
        wait_for_admissions(&cache, 1).await;
        let status = cache.status();
        assert!(status.quota_coordination_healthy);
        assert!(!cache.quota.reconciliation_pending.load(Ordering::Acquire));
        assert_eq!(status.quota_reconciliation_lag, 0);
        assert_eq!(status.tiers[0].reconciliation_lag, 0);
        assert!(status.used_bytes <= status.aggregate_max_bytes.unwrap());
        assert!(status.tiers[0].used_bytes <= status.tiers[0].policy.max_bytes);
    }

    #[tokio::test]
    async fn reconciliation_never_reactivates_a_deleting_generation() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store,
            "deleting-restart",
            QuotaOptions::default(),
        )
        .await;
        let key = "deleting-restart";
        cache_store
            .put(&entry_path(key, "data"), Bytes::from_static(b"data").into())
            .await
            .unwrap();
        let sidecar = CacheSidecar {
            format: CACHE_FORMAT,
            namespace: cache_domain_namespace(&cache.namespace, CacheConfidentiality::Encrypted),
            origin_path: "sst/deleting-restart".to_owned(),
            request_identity: "request".to_owned(),
            location: "sst/deleting-restart".to_owned(),
            last_modified: "1970-01-01T00:00:00Z".to_owned(),
            size: 4,
            e_tag: None,
            version: None,
            response_start: 0,
            response_end: 4,
            attributes: Vec::new(),
            length: 4,
            sha256: sha256_hex(b"data"),
            created_ms: 1,
            accessed_ms: 1,
        };
        cache_store
            .put(
                &entry_path(key, "meta"),
                serde_json::to_vec(&sidecar).unwrap().into(),
            )
            .await
            .unwrap();
        let (mut ledger, version) = cache.read_quota_ledger().await.unwrap();
        let generation = 7;
        ledger.entries.insert(
            quota_entry_id(&cache.tiers[0], key),
            QuotaEntry {
                key: key.to_owned(),
                tier_id: "controlled".to_owned(),
                owner_manager_id: cache.quota.manager_id.clone(),
                bytes: 4,
                generation,
                policy_revision: 0,
                expires_ms: 0,
                state: QuotaEntryState::Deleting,
            },
        );
        *ledger
            .managers
            .get_mut(&cache.quota.manager_id)
            .unwrap()
            .committed_by_tier
            .entry("controlled".to_owned())
            .or_default() = 4;
        ledger.revision = ledger.revision.saturating_add(1);
        cache.write_quota_ledger(&ledger, version).await.unwrap();
        cache_store.deny_delete.store(true, Ordering::Release);

        assert!(
            recover_quota_coordination(
                &cache.policy_store,
                &cache.namespace,
                cache.aggregate_max_bytes,
                &cache.tiers,
                &cache.policy(),
                &cache.capacity,
                &cache.quota,
                &cache.pending_deletions,
                &cache.deletion_wake,
            )
            .await
        );
        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        let entry = ledger
            .entries
            .get(&quota_entry_id(&cache.tiers[0], key))
            .unwrap();
        assert_eq!(entry.state, QuotaEntryState::Deleting);
        assert_eq!(entry.generation, generation);
        assert_eq!(cache.status().used_bytes, 4);
    }

    #[tokio::test]
    async fn policy_change_after_active_commit_retires_exact_generation() {
        let cache_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        let key = "post-commit-policy-race";
        let reservation = cache
            .reserve(0, key, 128, 128, &policy(4096))
            .await
            .unwrap();
        cache_store
            .put(&entry_path(key, "data"), Bytes::from_static(b"data").into())
            .await
            .unwrap();
        cache_store
            .put(&entry_path(key, "meta"), Bytes::from_static(b"meta").into())
            .await
            .unwrap();
        let generation = cache
            .commit_shared_reservation(&reservation, 128)
            .await
            .unwrap();
        let current = cache.policy();
        let mut policies = current.policies.clone();
        policies[0].enabled = false;
        *cache
            .policy_snapshot
            .write()
            .unwrap_or_else(|lock| lock.into_inner()) = Arc::new(PolicySnapshot {
            revision: current.revision,
            canonical: current.canonical.clone(),
            generation: current.generation.saturating_add(1),
            policies,
        });
        assert_eq!(
            cache
                .finish_committed_reservation(&reservation, key, 128, 1, 1, generation)
                .await,
            AdmissionCommit::Retiring
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let (ledger, _) = cache.read_quota_ledger().await.unwrap();
                if !ledger.entries.contains_key(&reservation.quota_entry_id) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn hanging_coordination_does_not_delay_origin_response() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (origin, cache) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store.clone(),
            "hanging-coordination",
            QuotaOptions::default(),
        )
        .await;
        let path = ObjectPath::from("sst/hanging-coordination");
        origin
            .put(&path, Bytes::from_static(b"origin").into())
            .await
            .unwrap();
        policy_store
            .coordination_delay_ms
            .store(1_000, Ordering::Release);
        let started = Instant::now();
        assert_eq!(
            cached_get(
                &*cache,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false),
            )
            .await,
            b"origin"[..]
        );
        assert!(started.elapsed() < Duration::from_millis(150));
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.status().quota_coordination_healthy {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn delayed_fill_releases_all_waiters_and_bounds_flight_state() {
        let cache_store = Arc::new(CountingStore::new());
        cache_store.write_delay_ms.store(200, Ordering::Release);
        let (origin, cache) = manager_with_cache_store(64 * 1024, cache_store).await;
        let path = ObjectPath::from("sst/delayed-concurrent");
        origin
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        let started = Instant::now();
        let readers = (0..16).map(|_| {
            let cache = cache.clone();
            let path = path.clone();
            tokio::spawn(async move {
                cached_get(
                    &*cache,
                    &path,
                    options(TableStoreKind::Main, SstType::Compacted, false),
                )
                .await
            })
        });
        for result in futures_util::future::join_all(readers).await {
            assert_eq!(result.unwrap(), b"value"[..]);
        }
        assert!(started.elapsed() < Duration::from_millis(150));
        assert_eq!(origin.reads.load(Ordering::Acquire), 1);
        wait_for_admissions(&cache, 1).await;
        assert!(cache.inflight.lock().unwrap().is_empty());
        assert!(cache.background_tasks.lock().unwrap().len() <= 1);
    }

    #[tokio::test]
    async fn status_uses_shared_ledger_and_keeps_local_counters_separate() {
        let cache_store = Arc::new(CountingStore::new());
        let policy_store = Arc::new(CountingStore::new());
        let (_, first) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store.clone(),
            policy_store.clone(),
            "global-status",
            QuotaOptions::default(),
        )
        .await;
        let (_, second) = manager_with_shared_stores(
            4096,
            Some(4096),
            cache_store,
            policy_store,
            "global-status",
            QuotaOptions::default(),
        )
        .await;
        let reservation = first
            .reserve(0, "reserved", 128, 128, &policy(4096))
            .await
            .unwrap();
        second.read_quota_ledger().await.unwrap();
        let status = second.status();
        assert_eq!(status.reserved_bytes, 128);
        assert_eq!(status.local_reserved_bytes, 0);
        assert_eq!(status.tiers[0].reserved_bytes, 128);
        assert_eq!(status.tiers[0].local_reserved_bytes, 0);
        first.release_reservation(reservation).await;
    }

    #[tokio::test]
    async fn status_reports_zero_shared_accounting_for_unlisted_tiers() {
        let cache = two_tier_manager("unlisted-tier-status").await;
        {
            let mut capacity = cache.capacity.lock().unwrap();
            capacity.used_by_tier.insert(1, 256);
            capacity.reserved_by_tier.insert(1, 128);
        }
        let mut ledger = QuotaLedger {
            format: QUOTA_FORMAT,
            namespace: cache.namespace.clone(),
            revision: 0,
            next_generation: 0,
            policy_revision: 0,
            aggregate_max_bytes: None,
            managers: HashMap::new(),
            entries: HashMap::new(),
            reconciler: None,
        };
        ledger.entries.insert(
            "first-entry".to_owned(),
            QuotaEntry {
                key: "first-entry".to_owned(),
                tier_id: "first".to_owned(),
                owner_manager_id: cache.quota.manager_id.clone(),
                bytes: 64,
                generation: 0,
                policy_revision: 0,
                expires_ms: u64::MAX,
                state: QuotaEntryState::Active,
            },
        );
        *cache.quota.latest_ledger.write().unwrap() = Some(ledger);

        let status = cache.status();
        assert_eq!(status.tiers[0].used_bytes, 64);
        assert_eq!(status.tiers[1].used_bytes, 0);
        assert_eq!(status.tiers[1].reserved_bytes, 0);
        assert_eq!(status.tiers[1].local_used_bytes, 256);
        assert_eq!(status.tiers[1].local_reserved_bytes, 128);
    }

    #[tokio::test]
    async fn close_stops_new_cache_writes_and_releases_the_ledger() {
        let cache_store = Arc::new(CountingStore::new());
        let (origin, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        cache.close().await.unwrap();
        let writes_before = cache_store.writes.load(Ordering::Acquire);
        let path = ObjectPath::from("sst/after-close");
        origin
            .put(&path, Bytes::from_static(b"origin").into())
            .await
            .unwrap();
        assert_eq!(
            cached_get(
                &*cache,
                &path,
                options(TableStoreKind::Main, SstType::Compacted, false),
            )
            .await,
            b"origin"[..]
        );
        assert_eq!(cache_store.writes.load(Ordering::Acquire), writes_before);
        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        let grant = ledger.managers.get(&cache.quota.manager_id).unwrap();
        assert!(grant.reserved_by_tier.is_empty());
        assert!(grant.lease_expires_ms <= now_ms());
    }

    #[tokio::test]
    async fn concurrent_spawn_and_close_has_no_writes_after_close_returns() {
        let cache_store = Arc::new(CountingStore::new());
        cache_store.write_delay_ms.store(10, Ordering::Release);
        let (_, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        let attempts = (0..64).map(|index| {
            let cache = cache.clone();
            let cache_store = cache_store.clone();
            tokio::spawn(async move {
                cache
                    .spawn_background(async move {
                        let _ = cache_store
                            .put(
                                &ObjectPath::from(format!("spawn-close-{index}")),
                                Bytes::from_static(b"x").into(),
                            )
                            .await;
                    })
                    .await
            })
        });
        let close = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.close().await })
        };
        for attempt in futures_util::future::join_all(attempts).await {
            let _ = attempt.unwrap();
        }
        close.await.unwrap().unwrap();
        let writes_after_close = cache_store.writes.load(Ordering::Acquire);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            cache_store.writes.load(Ordering::Acquire),
            writes_after_close
        );
    }

    #[tokio::test]
    async fn blocked_large_deletion_backlog_has_bounded_shutdown() {
        let cache_store = Arc::new(CountingStore::new());
        let (_, cache) = manager_with_cache_store(4096, cache_store.clone()).await;
        cache_store.deny_delete.store(true, Ordering::Release);
        let (mut ledger, version) = cache.read_quota_ledger().await.unwrap();
        for generation in 1..=128_u64 {
            let key = format!("blocked-{generation}");
            cache_store
                .put(&entry_path(&key, "data"), Bytes::from_static(b"x").into())
                .await
                .unwrap();
            ledger.entries.insert(
                quota_entry_id(&cache.tiers[0], &key),
                QuotaEntry {
                    key,
                    tier_id: "controlled".to_owned(),
                    owner_manager_id: cache.quota.manager_id.clone(),
                    bytes: 1,
                    generation,
                    policy_revision: 0,
                    expires_ms: 0,
                    state: QuotaEntryState::Deleting,
                },
            );
        }
        ledger.next_generation = 128;
        *ledger
            .managers
            .get_mut(&cache.quota.manager_id)
            .unwrap()
            .committed_by_tier
            .entry("controlled".to_owned())
            .or_default() = 128;
        ledger.revision = ledger.revision.saturating_add(1);
        cache.write_quota_ledger(&ledger, version).await.unwrap();

        let started = Instant::now();
        let error = cache.close().await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(error.to_string().contains("pending deletions"));
        let (ledger, _) = cache.read_quota_ledger().await.unwrap();
        assert_eq!(ledger.entries.len(), 128);
        assert_eq!(cache.status().used_bytes, 128);
    }
}
