//! Chunked authenticated encryption for metadata object stores.

use std::{
    collections::{HashMap, VecDeque},
    env,
    fmt::{Debug, Display, Formatter},
    hash::{DefaultHasher, Hash, Hasher},
    num::NonZeroUsize,
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use aws_lc_rs::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use bytes::{Bytes, BytesMut};
use futures_util::{stream, stream::BoxStream, StreamExt};
use rand::RngCore;
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};
use slatedb::object_store::{
    path::Path, CopyMode, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use slatedb::object_store_tag::{ObjectStoreCallTag, SstType};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::ids::RepositoryId;

const MAGIC: &[u8; 8] = b"VLTDBENC";
const FORMAT_VERSION: u8 = 1;
const ALGORITHM_AES_256_GCM: u8 = 1;
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const HEADER_SIZE: usize = 8 + 1 + 1 + 4 + 4 + 8 + NONCE_SIZE;
const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;
const DEFAULT_MAX_CRYPTO_THREADS: usize = 8;
const MAX_CRYPTO_THREADS: usize = 64;
const PARALLEL_CRYPTO_CHUNKS: usize = 4;
const HEADER_CACHE_ENTRIES: usize = 16 * 1024;
const IMMUTABLE_SST_FRESHNESS: std::time::Duration = std::time::Duration::from_secs(1);
const DEFAULT_CHUNK_CACHE_BYTES: usize = 16 * 1024 * 1024 * 1024;
const MAX_CHUNK_CACHE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const MAX_CHUNK_CACHE_ENTRIES: usize = 262144;
const MAX_CHUNK_FLIGHTS: usize = 256;
const MAX_CHUNK_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
const MAX_CACHED_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_CACHED_RANGE_CHUNKS: usize = 16;
static NEXT_CHUNK_NAMESPACE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum SstFreshness {
    Strict,
    #[default]
    Bounded,
    Immutable,
}

impl SstFreshness {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "strict" => Ok(Self::Strict),
            "bounded" => Ok(Self::Bounded),
            "immutable" => Ok(Self::Immutable),
            _ => anyhow::bail!("VAULTICDB_SST_FRESHNESS must be strict, bounded or immutable"),
        }
    }

    fn configured() -> anyhow::Result<Self> {
        match env::var_os("VAULTICDB_SST_FRESHNESS") {
            None => Ok(Self::default()),
            Some(value) => {
                Self::parse(value.to_str().ok_or_else(|| {
                    anyhow::anyhow!("VAULTICDB_SST_FRESHNESS must be valid UTF-8")
                })?)
            }
        }
    }
}

pub mod envelope;
pub mod recovery_capsule;

#[derive(Debug, thiserror::Error)]
pub(crate) enum EncryptionError {
    #[error("encrypted object header is malformed or unsupported")]
    Header,
    #[error("encrypted object length is inconsistent with its header")]
    Length,
    #[error("encrypted object authentication failed")]
    Authentication,
    #[error("plaintext range is invalid")]
    Range,
}

pub fn is_integrity_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source.downcast_ref::<EncryptionError>().is_some() {
            return true;
        }
        current = source.source();
    }
    false
}

#[derive(Clone)]
pub(crate) struct EncryptedObjectStore {
    read_metrics: Arc<EncryptionReadMetrics>,
    inner: Arc<dyn ObjectStore>,
    repository_id: RepositoryId,
    keyring: Arc<RwLock<Keyring>>,
    crypto: Arc<CryptoExecutor>,
    headers: Arc<Mutex<HeaderCache>>,
    header_flights: Arc<Mutex<HashMap<Path, Arc<HeaderValidationFlight>>>>,
    chunks: Arc<ChunkCache>,
    chunk_namespace: u64,
    chunk_size: usize,
    sst_freshness: SstFreshness,
}

#[derive(Default)]
struct EncryptionReadMetrics {
    reads: AtomicU64,
    requested_plaintext_bytes: AtomicU64,
    rounded_ciphertext_bytes: AtomicU64,
    ciphertext_origin_calls: AtomicU64,
    ciphertext_origin_bytes: AtomicU64,
    ciphertext_origin_ns: AtomicU64,
    ciphertext_origin_latency: ReadLatencyBuckets,
    chunk_wait_ns: AtomicU64,
    chunk_wait_latency: ReadLatencyBuckets,
    chunk_admission_wait_ns: AtomicU64,
    chunk_admission_latency: ReadLatencyBuckets,
    header_decode_ns: AtomicU64,
    header_cache_hits: AtomicU64,
    header_cache_misses: AtomicU64,
    header_cache_evictions: AtomicU64,
    crypto_jobs: AtomicU64,
    crypto_queue_ns: AtomicU64,
    crypto_service_ns: AtomicU64,
    head_reads: AtomicU64,
    full_reads: AtomicU64,
    range_reads: AtomicU64,
    header_validation_calls: AtomicU64,
    header_validation_ns: AtomicU64,
    header_validation_coalesced_reads: AtomicU64,
    chunk_cache_hits: AtomicU64,
    chunk_cache_misses: AtomicU64,
    chunk_cache_evictions: AtomicU64,
    chunk_cache_bytes: AtomicU64,
    chunk_coalesced_reads: AtomicU64,
    chunk_inflight: AtomicU64,
    chunk_sampled_first_fetches: AtomicU64,
    chunk_sampled_refetches: AtomicU64,
    chunk_sampled_history_evictions: AtomicU64,
    chunk_sampled_reuse_ns: AtomicU64,
    chunk_sampled_reuse_bytes: AtomicU64,
    chunk_sampled_refetch_within_64m: AtomicU64,
    chunk_sampled_refetch_within_256m: AtomicU64,
    chunk_sampled_refetch_within_1g: AtomicU64,
    read_windows: Mutex<VecDeque<EncryptionReadWindow>>,
    read_windows_dropped: AtomicU64,
}

#[derive(Default, serde::Serialize)]
struct EncryptionReadWindow {
    unix_seconds: u64,
    requested_plaintext_bytes: u64,
    ciphertext_origin_bytes: u64,
}

#[derive(Default)]
struct ReadLatencyBuckets {
    counts: [AtomicU64; 8],
}

impl ReadLatencyBuckets {
    fn record(&self, nanoseconds: u64) {
        let upper = [
            100_000,
            1_000_000,
            5_000_000,
            20_000_000,
            100_000_000,
            500_000_000,
            2_000_000_000,
        ];
        let bucket = upper.partition_point(|limit| nanoseconds > *limit);
        self.counts[bucket].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> [u64; 8] {
        std::array::from_fn(|index| self.counts[index].load(Ordering::Relaxed))
    }
}

struct ReadTiming<'counter> {
    counter: &'counter AtomicU64,
    histogram: Option<&'counter ReadLatencyBuckets>,
    started: Instant,
}

impl<'counter> ReadTiming<'counter> {
    fn new(counter: &'counter AtomicU64) -> Self {
        Self {
            counter,
            histogram: None,
            started: Instant::now(),
        }
    }

    fn with_histogram(mut self, histogram: &'counter ReadLatencyBuckets) -> Self {
        self.histogram = Some(histogram);
        self
    }
}

impl Drop for ReadTiming<'_> {
    fn drop(&mut self) {
        let elapsed = self
            .started
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX);
        self.counter.fetch_add(elapsed, Ordering::Relaxed);
        if let Some(histogram) = self.histogram {
            histogram.record(elapsed);
        }
    }
}

impl EncryptionReadMetrics {
    fn record_window(&self, requested: u64, origin: u64) {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.record_window_at(seconds, requested, origin);
    }

    fn record_window_at(&self, seconds: u64, requested: u64, origin: u64) {
        let mut windows = self
            .read_windows
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let bucket =
            (seconds / 10 * 10).max(windows.back().map_or(0, |window| window.unix_seconds));
        if windows
            .back()
            .is_none_or(|window| window.unix_seconds != bucket)
        {
            if windows.len() == 360 {
                windows.pop_front();
                self.read_windows_dropped.fetch_add(1, Ordering::Relaxed);
            }
            windows.push_back(EncryptionReadWindow {
                unix_seconds: bucket,
                ..Default::default()
            });
        }
        let window = windows.back_mut().unwrap();
        window.requested_plaintext_bytes =
            window.requested_plaintext_bytes.saturating_add(requested);
        window.ciphertext_origin_bytes = window.ciphertext_origin_bytes.saturating_add(origin);
    }

    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "reads": self.reads.load(Ordering::Relaxed),
            "requested_plaintext_bytes": self.requested_plaintext_bytes.load(Ordering::Relaxed),
            "rounded_ciphertext_bytes": self.rounded_ciphertext_bytes.load(Ordering::Relaxed),
            "ciphertext_origin_calls": self.ciphertext_origin_calls.load(Ordering::Relaxed),
            "ciphertext_origin_bytes": self.ciphertext_origin_bytes.load(Ordering::Relaxed),
            "ciphertext_origin_ns": self.ciphertext_origin_ns.load(Ordering::Relaxed),
            "ciphertext_origin_latency_buckets": self.ciphertext_origin_latency.snapshot(),
            "chunk_wait_ns": self.chunk_wait_ns.load(Ordering::Relaxed),
            "chunk_wait_latency_buckets": self.chunk_wait_latency.snapshot(),
            "chunk_admission_wait_ns": self.chunk_admission_wait_ns.load(Ordering::Relaxed),
            "chunk_admission_latency_buckets": self.chunk_admission_latency.snapshot(),
            "header_decode_ns": self.header_decode_ns.load(Ordering::Relaxed),
            "header_cache_hits": self.header_cache_hits.load(Ordering::Relaxed),
            "header_cache_misses": self.header_cache_misses.load(Ordering::Relaxed),
            "header_cache_evictions": self.header_cache_evictions.load(Ordering::Relaxed),
            "crypto_jobs": self.crypto_jobs.load(Ordering::Relaxed),
            "crypto_queue_ns": self.crypto_queue_ns.load(Ordering::Relaxed),
            "crypto_service_ns": self.crypto_service_ns.load(Ordering::Relaxed),
            "head_reads": self.head_reads.load(Ordering::Relaxed),
            "full_reads": self.full_reads.load(Ordering::Relaxed),
            "range_reads": self.range_reads.load(Ordering::Relaxed),
            "header_validation_calls": self.header_validation_calls.load(Ordering::Relaxed),
            "header_validation_ns": self.header_validation_ns.load(Ordering::Relaxed),
            "header_validation_coalesced_reads": self.header_validation_coalesced_reads.load(Ordering::Relaxed),
            "chunk_cache_hits": self.chunk_cache_hits.load(Ordering::Relaxed),
            "chunk_cache_misses": self.chunk_cache_misses.load(Ordering::Relaxed),
            "chunk_cache_evictions": self.chunk_cache_evictions.load(Ordering::Relaxed),
            "chunk_cache_bytes": self.chunk_cache_bytes.load(Ordering::Relaxed),
            "chunk_coalesced_reads": self.chunk_coalesced_reads.load(Ordering::Relaxed),
            "chunk_inflight": self.chunk_inflight.load(Ordering::Relaxed),
            "chunk_sampled_first_fetches": self.chunk_sampled_first_fetches.load(Ordering::Relaxed),
            "chunk_sampled_refetches": self.chunk_sampled_refetches.load(Ordering::Relaxed),
            "chunk_sampled_history_evictions": self.chunk_sampled_history_evictions.load(Ordering::Relaxed),
            "chunk_sampled_reuse_ns": self.chunk_sampled_reuse_ns.load(Ordering::Relaxed),
            "chunk_sampled_reuse_bytes": self.chunk_sampled_reuse_bytes.load(Ordering::Relaxed),
            "chunk_sampled_refetch_within_64m": self.chunk_sampled_refetch_within_64m.load(Ordering::Relaxed),
            "chunk_sampled_refetch_within_256m": self.chunk_sampled_refetch_within_256m.load(Ordering::Relaxed),
            "chunk_sampled_refetch_within_1g": self.chunk_sampled_refetch_within_1g.load(Ordering::Relaxed),
            "read_windows": *self.read_windows.lock().unwrap_or_else(|error| error.into_inner()),
            "read_windows_dropped": self.read_windows_dropped.load(Ordering::Relaxed),
        })
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ChunkKey {
    namespace: u64,
    location: Path,
    header: Header,
    e_tag: Option<String>,
    version: Option<String>,
    modified_seconds: i64,
    modified_nanos: u32,
    index: usize,
}

struct AuthenticatedChunk {
    plaintext: Zeroizing<Vec<u8>>,
}

struct ValidatedHeader {
    meta: ObjectMeta,
    attributes: slatedb::object_store::Attributes,
    extensions: slatedb::object_store::Extensions,
}

type SharedHeaderResult =
    std::result::Result<Arc<ValidatedHeader>, Arc<slatedb::object_store::Error>>;

struct HeaderValidationFlight {
    tag: Option<ObjectStoreCallTag>,
    cached: CachedHeader,
    result: tokio::sync::watch::Receiver<Option<SharedHeaderResult>>,
}

struct HeaderValidationGuard {
    flights: Arc<Mutex<HashMap<Path, Arc<HeaderValidationFlight>>>>,
    location: Path,
    flight: Arc<HeaderValidationFlight>,
}

impl Drop for HeaderValidationGuard {
    fn drop(&mut self) {
        let mut flights = self
            .flights
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if flights
            .get(&self.location)
            .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
        {
            flights.remove(&self.location);
        }
    }
}

type SharedChunkResult =
    std::result::Result<Arc<AuthenticatedChunk>, Arc<slatedb::object_store::Error>>;

struct ChunkFlight {
    result: tokio::sync::watch::Receiver<Option<SharedChunkResult>>,
}

struct ChunkCacheState {
    entries: lru::LruCache<ChunkKey, Arc<AuthenticatedChunk>>,
    bytes: usize,
    flights: HashMap<ChunkKey, Arc<ChunkFlight>>,
    flight_bytes: usize,
    sampled_fetches: lru::LruCache<u64, (Instant, u64)>,
    fetched_bytes: u64,
}

struct ChunkCache {
    state: Mutex<ChunkCacheState>,
    changed: tokio::sync::Notify,
    capacity: usize,
    limits: ChunkCacheLimits,
    metrics: Arc<EncryptionReadMetrics>,
}

#[derive(Clone, Copy, Debug)]
struct ChunkCacheLimits {
    entries: usize,
    flights: usize,
    flight_bytes: usize,
    chunk_bytes: usize,
    range_chunks: usize,
}

impl Default for ChunkCacheLimits {
    fn default() -> Self {
        Self {
            entries: 65536,
            flights: 64,
            flight_bytes: 16 * 1024 * 1024,
            chunk_bytes: 1024 * 1024,
            range_chunks: 4,
        }
    }
}

impl ChunkCacheLimits {
    fn configured() -> anyhow::Result<Self> {
        let defaults = Self::default();
        Self {
            entries: configured_chunk_limit(
                "VAULTICDB_CHUNK_CACHE_ENTRIES",
                defaults.entries,
                MAX_CHUNK_CACHE_ENTRIES,
            )?,
            flights: configured_chunk_limit(
                "VAULTICDB_CHUNK_FETCH_LIMIT",
                defaults.flights,
                MAX_CHUNK_FLIGHTS,
            )?,
            flight_bytes: configured_chunk_limit(
                "VAULTICDB_CHUNK_FETCH_BYTES",
                defaults.flight_bytes,
                MAX_CHUNK_FLIGHT_BYTES,
            )?,
            chunk_bytes: configured_chunk_limit(
                "VAULTICDB_CHUNK_MAX_BYTES",
                defaults.chunk_bytes,
                MAX_CACHED_CHUNK_BYTES,
            )?,
            range_chunks: configured_chunk_limit(
                "VAULTICDB_CHUNK_RANGE_LIMIT",
                defaults.range_chunks,
                MAX_CACHED_RANGE_CHUNKS,
            )?,
        }
        .validate()
    }

    fn validate(self) -> anyhow::Result<Self> {
        if self.flight_bytes < self.chunk_bytes {
            anyhow::bail!("VAULTICDB_CHUNK_FETCH_BYTES must be at least VAULTICDB_CHUNK_MAX_BYTES");
        }
        Ok(self)
    }
}

impl ChunkCache {
    #[cfg(test)]
    fn new(capacity: usize, metrics: Arc<EncryptionReadMetrics>) -> Self {
        Self::with_limits(capacity, ChunkCacheLimits::default(), metrics)
    }

    fn with_limits(
        capacity: usize,
        limits: ChunkCacheLimits,
        metrics: Arc<EncryptionReadMetrics>,
    ) -> Self {
        Self {
            state: Mutex::new(ChunkCacheState {
                entries: lru::LruCache::new(NonZeroUsize::new(limits.entries).unwrap()),
                bytes: 0,
                flights: HashMap::new(),
                flight_bytes: 0,
                sampled_fetches: lru::LruCache::new(NonZeroUsize::new(16384).unwrap()),
                fetched_bytes: 0,
            }),
            changed: tokio::sync::Notify::new(),
            capacity,
            limits,
            metrics,
        }
    }

    fn record_fetch(&self, key: &ChunkKey, length: usize) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        self.record_fetch_fingerprint(hasher.finish(), length, Instant::now());
    }

    fn record_fetch_fingerprint(&self, fingerprint: u64, length: usize, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.fetched_bytes = state.fetched_bytes.saturating_add(length as u64);
        if fingerprint & 63 != 0 {
            return;
        }
        let fetched_bytes = state.fetched_bytes;
        if let Some((previous, previous_bytes)) = state.sampled_fetches.get(&fingerprint).copied() {
            let distance = fetched_bytes.saturating_sub(previous_bytes);
            self.metrics
                .chunk_sampled_refetches
                .fetch_add(1, Ordering::Relaxed);
            self.metrics.chunk_sampled_reuse_ns.fetch_add(
                now.saturating_duration_since(previous)
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
            self.metrics
                .chunk_sampled_reuse_bytes
                .fetch_add(distance, Ordering::Relaxed);
            for (bound, counter) in [
                (64 << 20, &self.metrics.chunk_sampled_refetch_within_64m),
                (256 << 20, &self.metrics.chunk_sampled_refetch_within_256m),
                (1 << 30, &self.metrics.chunk_sampled_refetch_within_1g),
            ] {
                if distance <= bound {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            self.metrics
                .chunk_sampled_first_fetches
                .fetch_add(1, Ordering::Relaxed);
            if state.sampled_fetches.len() == state.sampled_fetches.cap().get() {
                self.metrics
                    .chunk_sampled_history_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        state.sampled_fetches.put(fingerprint, (now, fetched_bytes));
    }

    fn insert(&self, key: ChunkKey, chunk: Arc<AuthenticatedChunk>) {
        let length = chunk.plaintext.len();
        if length > self.capacity {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.bytes + length > self.capacity || state.entries.len() == self.limits.entries {
            if let Some((_, expired)) = state.entries.pop_lru() {
                state.bytes -= expired.plaintext.len();
                self.metrics
                    .chunk_cache_evictions
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
        if let Some(previous) = state.entries.put(key, chunk) {
            state.bytes -= previous.plaintext.len();
        }
        state.bytes += length;
        self.metrics
            .chunk_cache_bytes
            .store(state.bytes as u64, Ordering::Relaxed);
    }

    fn invalidate(&self, namespace: u64, location: &Path) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let keys = state
            .entries
            .iter()
            .filter(|(key, _)| key.namespace == namespace && key.location == *location)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(expired) = state.entries.pop(&key) {
                state.bytes -= expired.plaintext.len();
            }
        }
        self.metrics
            .chunk_cache_bytes
            .store(state.bytes as u64, Ordering::Relaxed);
    }

    fn retire_keys_before(&self, version: u32) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let keys = state
            .entries
            .iter()
            .filter(|(key, _)| key.header.key_version < version)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(expired) = state.entries.pop(&key) {
                state.bytes -= expired.plaintext.len();
            }
        }
        self.metrics
            .chunk_cache_bytes
            .store(state.bytes as u64, Ordering::Relaxed);
    }
}

struct ChunkFlightGuard {
    cache: Arc<ChunkCache>,
    key: ChunkKey,
    bytes: usize,
}

impl Drop for ChunkFlightGuard {
    fn drop(&mut self) {
        let mut state = self
            .cache
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.flights.remove(&self.key);
        state.flight_bytes -= self.bytes;
        self.cache
            .metrics
            .chunk_inflight
            .fetch_sub(1, Ordering::Relaxed);
        drop(state);
        self.cache.changed.notify_waiters();
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct SharedChunkError(#[source] Arc<slatedb::object_store::Error>);

fn shared_chunk_error(error: Arc<slatedb::object_store::Error>) -> slatedb::object_store::Error {
    let source = Box::new(SharedChunkError(error.clone()));
    match error.as_ref() {
        slatedb::object_store::Error::NotFound { path, .. } => {
            slatedb::object_store::Error::NotFound {
                path: path.clone(),
                source,
            }
        }
        slatedb::object_store::Error::Precondition { path, .. } => {
            slatedb::object_store::Error::Precondition {
                path: path.clone(),
                source,
            }
        }
        slatedb::object_store::Error::NotModified { path, .. } => {
            slatedb::object_store::Error::NotModified {
                path: path.clone(),
                source,
            }
        }
        _ => slatedb::object_store::Error::Generic {
            store: "vaulticdb encrypted chunk cache",
            source,
        },
    }
}

fn configured_chunk_cache_bytes() -> anyhow::Result<usize> {
    match env::var("VAULTICDB_CHUNK_CACHE_BYTES") {
        Ok(value) => parse_chunk_cache_bytes(&value),
        Err(env::VarError::NotPresent) => Ok(DEFAULT_CHUNK_CACHE_BYTES),
        Err(error) => Err(anyhow::anyhow!(
            "invalid VAULTICDB_CHUNK_CACHE_BYTES: {error}"
        )),
    }
}

fn parse_chunk_cache_bytes(value: &str) -> anyhow::Result<usize> {
    let bytes = value.parse::<u64>()?;
    if bytes > MAX_CHUNK_CACHE_BYTES
        || value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        anyhow::bail!(
            "VAULTICDB_CHUNK_CACHE_BYTES must be an integer from 0 to {MAX_CHUNK_CACHE_BYTES}"
        );
    }
    Ok(usize::try_from(bytes)?)
}

fn configured_chunk_limit(name: &str, default: usize, maximum: usize) -> anyhow::Result<usize> {
    match env::var(name) {
        Ok(value) => parse_chunk_limit(name, &value, maximum),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(anyhow::anyhow!("invalid {name}: {error}")),
    }
}

fn parse_chunk_limit(name: &str, value: &str, maximum: usize) -> anyhow::Result<usize> {
    let limit = value.parse::<usize>()?;
    if limit == 0 || limit > maximum || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        anyhow::bail!("{name} must be an integer from 1 to {maximum}");
    }
    Ok(limit)
}

#[derive(Default)]
struct HeaderCache {
    entries: HashMap<Path, CachedHeader>,
    order: VecDeque<Path>,
    generation: u64,
}

#[derive(Clone)]
struct CachedHeader {
    header: Header,
    meta: ObjectMeta,
    attributes: slatedb::object_store::Attributes,
    extensions: slatedb::object_store::Extensions,
    tag: Option<ObjectStoreCallTag>,
    validated_at: Instant,
}

impl HeaderCache {
    fn get(&self, location: &Path) -> Option<CachedHeader> {
        self.entries.get(location).cloned()
    }

    fn insert(
        &mut self,
        location: Path,
        header: Header,
        meta: ObjectMeta,
        attributes: slatedb::object_store::Attributes,
        extensions: slatedb::object_store::Extensions,
        tag: Option<ObjectStoreCallTag>,
    ) {
        if self
            .entries
            .insert(
                location.clone(),
                CachedHeader {
                    header,
                    meta,
                    attributes,
                    extensions,
                    tag,
                    validated_at: Instant::now(),
                },
            )
            .is_some()
        {
            return;
        }
        self.order.push_back(location);
        while self.entries.len() > HEADER_CACHE_ENTRIES {
            if let Some(expired) = self.order.pop_front() {
                self.entries.remove(&expired);
            }
        }
    }

    fn remove(&mut self, location: &Path) {
        self.generation = self.generation.wrapping_add(1);
        self.entries.remove(location);
        self.order.retain(|entry| entry != location);
    }
}

struct CryptoExecutor {
    pool: ThreadPool,
    permits: Arc<tokio::sync::Semaphore>,
}

impl CryptoExecutor {
    fn new(threads: usize) -> anyhow::Result<Self> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("vaulticdb-crypto-{index}"))
            .build()
            .map_err(|error| anyhow::anyhow!("initialize metadata crypto pool: {error}"))?;
        Ok(Self {
            pool,
            permits: Arc::new(tokio::sync::Semaphore::new(threads)),
        })
    }

    async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        self.run_measured(operation, None).await
    }

    async fn run_measured<T, F>(
        &self,
        operation: F,
        metrics: Option<Arc<EncryptionReadMetrics>>,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let queued = Instant::now();
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| encryption_error(EncryptionError::Authentication))?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let _permit = permit;
            let result = if let Some(metrics) = metrics {
                metrics.crypto_jobs.fetch_add(1, Ordering::Relaxed);
                metrics.crypto_queue_ns.fetch_add(
                    queued.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                let _service = ReadTiming::new(&metrics.crypto_service_ns);
                operation()
            } else {
                operation()
            };
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| encryption_error(EncryptionError::Authentication))?
    }
}

fn configured_crypto_threads() -> anyhow::Result<usize> {
    let default = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(DEFAULT_MAX_CRYPTO_THREADS);
    let Some(value) = env::var_os("VAULTICDB_CRYPTO_THREADS") else {
        return Ok(default);
    };
    let value = value
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("VAULTICDB_CRYPTO_THREADS must be valid UTF-8"))?;
    parse_crypto_threads(value)
}

fn parse_crypto_threads(value: &str) -> anyhow::Result<usize> {
    let threads = value.parse::<usize>().map_err(|_| {
        anyhow::anyhow!("VAULTICDB_CRYPTO_THREADS must be an integer between 1 and 64")
    })?;
    if !(1..=MAX_CRYPTO_THREADS).contains(&threads) {
        anyhow::bail!("VAULTICDB_CRYPTO_THREADS must be between 1 and 64");
    }
    Ok(threads)
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub(crate) struct EncryptionKey {
    pub(crate) version: u32,
    key: Box<[u8; 32]>,
}

impl EncryptionKey {
    pub(crate) fn new(version: u32, key: [u8; 32]) -> Self {
        let key = Box::new(key);
        #[cfg(unix)]
        unsafe {
            libc::mlock(key.as_ptr().cast(), key.len());
        }
        Self { version, key }
    }

    pub(crate) fn secret(&self) -> &[u8; 32] {
        &self.key
    }
}

impl Clone for EncryptionKey {
    fn clone(&self) -> Self {
        Self::new(self.version, *self.key)
    }
}

struct Keyring {
    keys: Vec<CipherKey>,
    write_version: u32,
}

struct CipherKey {
    key: EncryptionKey,
    cipher: Arc<LessSafeKey>,
}

impl CipherKey {
    fn new(key: EncryptionKey) -> anyhow::Result<Self> {
        let cipher = UnboundKey::new(&AES_256_GCM, key.secret())
            .map(LessSafeKey::new)
            .map(Arc::new)
            .map_err(|_| anyhow::anyhow!("initialize metadata AES-256-GCM key"))?;
        Ok(Self { key, cipher })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Header {
    key_version: u32,
    chunk_size: usize,
    plaintext_len: usize,
    nonce: [u8; NONCE_SIZE],
}

impl Debug for EncryptedObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EncryptedObjectStore")
            .field("inner", &self.inner)
            .field("repository_id", &self.repository_id)
            .field(
                "write_version",
                &self.keyring.read().map(|keyring| keyring.write_version),
            )
            .field("crypto_threads", &self.crypto.pool.current_num_threads())
            .field(
                "cached_headers",
                &self.headers.lock().map(|headers| headers.entries.len()),
            )
            .field("chunk_size", &self.chunk_size)
            .field("chunk_cache_capacity_bytes", &self.chunks.capacity)
            .field("chunk_cache_limits", &self.chunks.limits)
            .field("sst_freshness", &self.sst_freshness)
            .finish_non_exhaustive()
    }
}

impl Display for EncryptedObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "encrypted({})", self.inner)
    }
}

impl EncryptedObjectStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        repository_id: impl Into<RepositoryId>,
        keys: Vec<EncryptionKey>,
        write_version: u32,
    ) -> anyhow::Result<Self> {
        if keys.is_empty()
            || write_version == 0
            || !keys.iter().any(|key| key.version == write_version)
        {
            anyhow::bail!("active metadata encryption key is missing");
        }
        let mut versions = keys.iter().map(|key| key.version).collect::<Vec<_>>();
        versions.sort_unstable();
        versions.dedup();
        if versions.len() != keys.len() || versions.first() == Some(&0) {
            anyhow::bail!("metadata encryption key versions must be unique and non-zero");
        }
        let keys = keys
            .into_iter()
            .map(CipherKey::new)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let sst_freshness = SstFreshness::configured()?;
        eprintln!(
            "{}",
            serde_json::json!({
                "category": "lifecycle", "component": "vaulticdb", "event": "sst_freshness_configured",
                "fields": {"mode": match sst_freshness { SstFreshness::Strict => "strict", SstFreshness::Bounded => "bounded", SstFreshness::Immutable => "immutable" }}
            })
        );
        let crypto = Arc::new(CryptoExecutor::new(configured_crypto_threads()?)?);
        let read_metrics = Arc::new(EncryptionReadMetrics::default());
        let chunks = Arc::new(ChunkCache::with_limits(
            configured_chunk_cache_bytes()?,
            ChunkCacheLimits::configured()?,
            read_metrics.clone(),
        ));
        Ok(Self {
            inner,
            read_metrics,
            repository_id: repository_id.into(),
            keyring: Arc::new(RwLock::new(Keyring {
                keys,
                write_version,
            })),
            crypto,
            headers: Arc::new(Mutex::new(HeaderCache::default())),
            header_flights: Arc::new(Mutex::new(HashMap::new())),
            chunks,
            chunk_namespace: NEXT_CHUNK_NAMESPACE.fetch_add(1, Ordering::Relaxed),
            chunk_size: DEFAULT_CHUNK_SIZE,
            sst_freshness,
        })
    }

    pub(crate) fn with_inner(&self, inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            read_metrics: self.read_metrics.clone(),
            repository_id: self.repository_id.clone(),
            keyring: self.keyring.clone(),
            crypto: self.crypto.clone(),
            headers: Arc::new(Mutex::new(HeaderCache::default())),
            header_flights: Arc::new(Mutex::new(HashMap::new())),
            chunks: self.chunks.clone(),
            chunk_namespace: NEXT_CHUNK_NAMESPACE.fetch_add(1, Ordering::Relaxed),
            chunk_size: self.chunk_size,
            sst_freshness: self.sst_freshness,
        }
    }

    #[cfg(test)]
    fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    fn cipher(&self, version: u32) -> Result<Arc<LessSafeKey>> {
        self.keyring
            .read()
            .map_err(|_| encryption_error(EncryptionError::Header))?
            .keys
            .iter()
            .find(|key| key.key.version == version)
            .map(|key| Arc::clone(&key.cipher))
            .ok_or_else(|| encryption_error(EncryptionError::Header))
    }

    fn write_version(&self) -> Result<u32> {
        self.keyring
            .read()
            .map(|keyring| keyring.write_version)
            .map_err(|_| encryption_error(EncryptionError::Header))
    }

    fn cached_header(&self, location: &Path) -> Option<CachedHeader> {
        let cached = self
            .headers
            .lock()
            .ok()
            .and_then(|headers| headers.get(location));
        if cached.is_some() {
            self.read_metrics
                .header_cache_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.read_metrics
                .header_cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        cached
    }

    fn cache_header(
        &self,
        location: &Path,
        header: Header,
        meta: &ObjectMeta,
        attributes: &slatedb::object_store::Attributes,
        extensions: &slatedb::object_store::Extensions,
        tag: Option<ObjectStoreCallTag>,
        generation: u64,
    ) {
        if let Ok(mut headers) = self.headers.lock() {
            if headers.generation != generation {
                return;
            }
            let evicts = headers.entries.len() == HEADER_CACHE_ENTRIES
                && !headers.entries.contains_key(location);
            headers.insert(
                location.clone(),
                header,
                meta.clone(),
                attributes.clone(),
                extensions.clone(),
                tag,
            );
            if evicts {
                self.read_metrics
                    .header_cache_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn invalidate_header(&self, location: &Path) {
        self.chunks.invalidate(self.chunk_namespace, location);
        if let Ok(mut headers) = self.headers.lock() {
            headers.remove(location);
        }
    }

    pub(crate) fn install_write_key(&self, key: EncryptionKey) -> anyhow::Result<()> {
        if key.version == 0 {
            anyhow::bail!("metadata encryption key version must be non-zero");
        }
        let mut keyring = self
            .keyring
            .write()
            .map_err(|_| anyhow::anyhow!("metadata encryption keyring lock poisoned"))?;
        if keyring
            .keys
            .iter()
            .any(|existing| existing.key.version == key.version)
        {
            anyhow::bail!("metadata encryption key version already exists");
        }
        keyring.write_version = key.version;
        keyring.keys.push(CipherKey::new(key)?);
        Ok(())
    }

    pub(crate) fn retire_read_keys_before(&self, version: u32) -> anyhow::Result<()> {
        let mut keyring = self
            .keyring
            .write()
            .map_err(|_| anyhow::anyhow!("metadata encryption keyring lock poisoned"))?;
        if version != keyring.write_version {
            anyhow::bail!("only metadata DEKs older than the active write key can be retired");
        }
        keyring.keys.retain(|key| key.key.version >= version);
        self.chunks.retire_keys_before(version);
        Ok(())
    }

    async fn encrypt(&self, location: &Path, plaintext: Bytes) -> Result<Bytes> {
        let store = self.clone();
        let location = location.clone();
        self.crypto
            .run(move || store.encrypt_sync(&location, &plaintext))
            .await
    }

    fn encrypt_sync(&self, location: &Path, plaintext: &[u8]) -> Result<Bytes> {
        let mut nonce = [0u8; NONCE_SIZE];
        rand::rng().fill_bytes(&mut nonce);
        let header = Header {
            key_version: self.write_version()?,
            chunk_size: self.chunk_size,
            plaintext_len: plaintext.len(),
            nonce,
        };
        let mut result = BytesMut::with_capacity(ciphertext_len(header)?);
        encode_header(header, &mut result);
        let cipher = self.cipher(header.key_version)?;
        let chunks = chunk_count(header.plaintext_len, header.chunk_size);
        if chunks >= PARALLEL_CRYPTO_CHUNKS {
            result.resize(ciphertext_len(header)?, 0);
            result[HEADER_SIZE..]
                .par_chunks_mut(header.chunk_size + TAG_SIZE)
                .enumerate()
                .try_for_each(|(index, output)| {
                    let start = index * header.chunk_size;
                    let end = (start + header.chunk_size).min(plaintext.len());
                    let (ciphertext, tag) = output.split_at_mut(end - start);
                    let nonce = Nonce::assume_unique_for_key(chunk_nonce(header.nonce, index)?);
                    let aad =
                        associated_data(&self.repository_id, location, header, index, end - start);
                    cipher
                        .seal_out_of_place_scatter(
                            nonce,
                            Aad::from(aad),
                            &plaintext[start..end],
                            ciphertext,
                            &[],
                            tag,
                        )
                        .map_err(|_| encryption_error(EncryptionError::Authentication))
                })?;
            return Ok(result.freeze());
        }
        for index in 0..chunks {
            let start = index * header.chunk_size;
            let end = (start + header.chunk_size).min(plaintext.len());
            let chunk = &plaintext[start..end];
            let nonce = Nonce::assume_unique_for_key(chunk_nonce(header.nonce, index)?);
            let aad = associated_data(&self.repository_id, location, header, index, chunk.len());
            let encrypted_start = result.len();
            result.extend_from_slice(chunk);
            let tag = cipher
                .seal_in_place_separate_tag(nonce, Aad::from(aad), &mut result[encrypted_start..])
                .map_err(|_| encryption_error(EncryptionError::Authentication))?;
            result.extend_from_slice(tag.as_ref());
        }
        Ok(result.freeze())
    }

    async fn plaintext_meta(&self, meta: ObjectMeta) -> Result<ObjectMeta> {
        let header_bytes = self
            .inner
            .get_range(&meta.location, 0..HEADER_SIZE as u64)
            .await?;
        let header = decode_header(&header_bytes)?;
        plaintext_meta_from_header(meta, header)
    }

    async fn decrypt_chunks(
        &self,
        location: &Path,
        header: Header,
        first_chunk: usize,
        ciphertext: Bytes,
    ) -> Result<Bytes> {
        let store = self.clone();
        let location = location.clone();
        self.crypto
            .run_measured(
                move || store.decrypt_chunks_sync(&location, header, first_chunk, ciphertext),
                Some(self.read_metrics.clone()),
            )
            .await
    }

    fn decrypt_chunks_sync(
        &self,
        location: &Path,
        header: Header,
        first_chunk: usize,
        ciphertext: Bytes,
    ) -> Result<Bytes> {
        let cipher = self.cipher(header.key_version)?;
        let chunk_stride = header.chunk_size + TAG_SIZE;
        let selected_chunks = ciphertext.len().div_ceil(chunk_stride);
        let total_chunks = chunk_count(header.plaintext_len, header.chunk_size);
        if selected_chunks == 0 || first_chunk + selected_chunks > total_chunks {
            return Err(encryption_error(EncryptionError::Length));
        }
        let plaintext_len = (first_chunk..first_chunk + selected_chunks)
            .map(|index| plaintext_chunk_len(header, index))
            .sum::<usize>();
        if plaintext_len + selected_chunks * TAG_SIZE != ciphertext.len() {
            return Err(encryption_error(EncryptionError::Length));
        }
        let ciphertext = match ciphertext.try_into_mut() {
            Ok(mut result) => {
                let decrypt = |offset: usize, encrypted: &mut [u8]| {
                    let index = first_chunk + offset;
                    let plaintext_chunk_len = plaintext_chunk_len(header, index);
                    let (plaintext, tag) = encrypted.split_at_mut(plaintext_chunk_len);
                    let nonce = Nonce::assume_unique_for_key(chunk_nonce(header.nonce, index)?);
                    let aad = associated_data(
                        &self.repository_id,
                        location,
                        header,
                        index,
                        plaintext_chunk_len,
                    );
                    cipher
                        .open_in_place_separate_tag(nonce, Aad::from(aad), tag, plaintext)
                        .map(|_| ())
                        .map_err(|_| encryption_error(EncryptionError::Authentication))
                };
                if selected_chunks >= PARALLEL_CRYPTO_CHUNKS {
                    result
                        .par_chunks_mut(chunk_stride)
                        .enumerate()
                        .try_for_each(|(offset, encrypted)| decrypt(offset, encrypted))?;
                } else {
                    result
                        .chunks_mut(chunk_stride)
                        .enumerate()
                        .try_for_each(|(offset, encrypted)| decrypt(offset, encrypted))?;
                }
                let mut plaintext_offset = 0;
                for offset in 0..selected_chunks {
                    let index = first_chunk + offset;
                    let chunk_len = plaintext_chunk_len(header, index);
                    let ciphertext_offset = offset * chunk_stride;
                    result.copy_within(
                        ciphertext_offset..ciphertext_offset + chunk_len,
                        plaintext_offset,
                    );
                    plaintext_offset += chunk_len;
                }
                result.truncate(plaintext_len);
                return Ok(result.freeze());
            }
            Err(ciphertext) => ciphertext,
        };
        if selected_chunks >= PARALLEL_CRYPTO_CHUNKS {
            let mut result = BytesMut::zeroed(plaintext_len);
            ciphertext
                .par_chunks(chunk_stride)
                .zip(result.par_chunks_mut(header.chunk_size))
                .enumerate()
                .try_for_each(|(offset, (encrypted, plaintext))| {
                    let index = first_chunk + offset;
                    let plaintext_chunk_len = plaintext_chunk_len(header, index);
                    let nonce = Nonce::assume_unique_for_key(chunk_nonce(header.nonce, index)?);
                    let aad = associated_data(
                        &self.repository_id,
                        location,
                        header,
                        index,
                        plaintext_chunk_len,
                    );
                    cipher
                        .open_separate_gather(
                            nonce,
                            Aad::from(aad),
                            &encrypted[..plaintext_chunk_len],
                            &encrypted[plaintext_chunk_len..],
                            plaintext,
                        )
                        .map_err(|_| encryption_error(EncryptionError::Authentication))
                })?;
            return Ok(result.freeze());
        }
        let mut result = BytesMut::with_capacity(ciphertext.len());
        let mut offset = 0;
        for index in first_chunk..total_chunks {
            let plaintext_chunk_len = plaintext_chunk_len(header, index);
            let encrypted_len = plaintext_chunk_len + TAG_SIZE;
            if offset + encrypted_len > ciphertext.len() {
                break;
            }
            let nonce = Nonce::assume_unique_for_key(chunk_nonce(header.nonce, index)?);
            let aad = associated_data(
                &self.repository_id,
                location,
                header,
                index,
                plaintext_chunk_len,
            );
            let plaintext_start = result.len();
            let tag_start = offset + plaintext_chunk_len;
            result.extend_from_slice(&ciphertext[offset..tag_start]);
            cipher
                .open_in_place_separate_tag(
                    nonce,
                    Aad::from(aad),
                    &ciphertext[tag_start..offset + encrypted_len],
                    &mut result[plaintext_start..],
                )
                .map_err(|_| encryption_error(EncryptionError::Authentication))?;
            offset += encrypted_len;
            if offset == ciphertext.len() {
                return Ok(result.freeze());
            }
        }
        Err(encryption_error(EncryptionError::Length))
    }

    async fn validated_cached_header(
        &self,
        location: &Path,
        options: &GetOptions,
    ) -> Result<
        Option<(
            CachedHeader,
            slatedb::object_store::Attributes,
            slatedb::object_store::Extensions,
        )>,
    > {
        let Some(cached) = self.cached_header(location) else {
            return Ok(None);
        };
        let tag = ObjectStoreCallTag::from_extensions(&options.extensions);
        let immutable_sst = Self::immutable_sst_tag(options).is_some();
        if immutable_sst
            && cached.tag == tag
            && (cached.meta.e_tag.is_some() || cached.meta.version.is_some())
            && match self.sst_freshness {
                SstFreshness::Strict => false,
                SstFreshness::Bounded => cached.validated_at.elapsed() < IMMUTABLE_SST_FRESHNESS,
                SstFreshness::Immutable => true,
            }
        {
            return Ok(Some((cached.clone(), cached.attributes, cached.extensions)));
        }
        let mut validation = options.clone();
        validation.head = true;
        validation.range = None;
        let shareable = validation.if_match.is_none()
            && validation.if_none_match.is_none()
            && validation.if_modified_since.is_none()
            && validation.if_unmodified_since.is_none()
            && validation.version.is_none()
            && validation.extensions.len() == usize::from(tag.is_some())
            && tag.as_ref().is_none_or(|tag| tag.retry.is_none());
        let observed = if shareable {
            let flight = {
                let mut flights = self
                    .header_flights
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(flight) = flights.get(location) {
                    if flight.cached.header == cached.header
                        && flight.cached.meta == cached.meta
                        && flight.tag == tag
                    {
                        self.read_metrics
                            .header_validation_coalesced_reads
                            .fetch_add(1, Ordering::Relaxed);
                        Some(flight.clone())
                    } else {
                        None
                    }
                } else if flights.len() < self.chunks.limits.flights {
                    let (sender, receiver) = tokio::sync::watch::channel(None);
                    let flight = Arc::new(HeaderValidationFlight {
                        tag: tag.clone(),
                        cached: cached.clone(),
                        result: receiver,
                    });
                    flights.insert(location.clone(), flight.clone());
                    let guard = HeaderValidationGuard {
                        flights: self.header_flights.clone(),
                        location: location.clone(),
                        flight: flight.clone(),
                    };
                    let store = self.clone();
                    let location = location.clone();
                    let request = validation.clone();
                    tokio::spawn(async move {
                        let result = store
                            .validation_head(&location, request)
                            .await
                            .map_err(Arc::new);
                        drop(guard);
                        sender.send_replace(Some(result));
                    });
                    Some(flight)
                } else {
                    None
                }
            };
            if let Some(flight) = flight {
                let mut result = flight.result.clone();
                loop {
                    let completed = result.borrow_and_update().clone();
                    if let Some(completed) = completed {
                        break completed.map_err(shared_chunk_error)?;
                    }
                    result
                        .changed()
                        .await
                        .map_err(|_| slatedb::object_store::Error::Generic {
                            store: "vaulticdb encrypted header validation",
                            source: Box::new(std::io::Error::other(
                                "header validation worker terminated",
                            )),
                        })?;
                }
            } else {
                self.validation_head(location, validation).await?
            }
        } else {
            self.validation_head(location, validation).await?
        };
        if observed.meta == cached.meta
            && (observed.meta.e_tag.is_some() || observed.meta.version.is_some())
        {
            if immutable_sst {
                if let Ok(mut headers) = self.headers.lock() {
                    if let Some(current) = headers.entries.get_mut(location) {
                        if current.header == cached.header && current.meta == cached.meta {
                            current.validated_at = Instant::now();
                            current.attributes = observed.attributes.clone();
                            current.extensions = observed.extensions.clone();
                            current.tag = tag;
                        }
                    }
                }
            }
            Ok(Some((
                cached,
                observed.attributes.clone(),
                observed.extensions.clone(),
            )))
        } else {
            self.invalidate_header(location);
            Ok(None)
        }
    }

    async fn validation_head(
        &self,
        location: &Path,
        validation: GetOptions,
    ) -> Result<Arc<ValidatedHeader>> {
        self.read_metrics
            .header_validation_calls
            .fetch_add(1, Ordering::Relaxed);
        let _validation = ReadTiming::new(&self.read_metrics.header_validation_ns);
        let observed = self.inner.get_opts(location, validation).await?;
        Ok(Arc::new(ValidatedHeader {
            meta: observed.meta,
            attributes: observed.attributes,
            extensions: observed.extensions,
        }))
    }

    fn immutable_sst_tag(options: &GetOptions) -> Option<ObjectStoreCallTag> {
        ObjectStoreCallTag::from_extensions(&options.extensions).filter(|tag| {
            tag.sst_type == SstType::Compacted
                && tag.retry.is_none()
                && options.extensions.len() == 1
                && options.if_match.is_none()
                && options.if_none_match.is_none()
                && options.if_modified_since.is_none()
                && options.if_unmodified_since.is_none()
                && options.version.is_none()
        })
    }

    async fn fetch_chunk(
        &self,
        location: &Path,
        header: Header,
        index: usize,
        expected: ObjectMeta,
        mut options: GetOptions,
    ) -> Result<Arc<AuthenticatedChunk>> {
        let start = encrypted_chunk_offset(header, index)?;
        let end = start
            .checked_add(plaintext_chunk_len(header, index) + TAG_SIZE)
            .ok_or_else(|| encryption_error(EncryptionError::Length))?;
        options.range = Some(GetRange::Bounded(start as u64..end as u64));
        options.head = false;
        self.read_metrics
            .rounded_ciphertext_bytes
            .fetch_add((end - start) as u64, Ordering::Relaxed);
        self.read_metrics
            .ciphertext_origin_calls
            .fetch_add(1, Ordering::Relaxed);
        let timing = ReadTiming::new(&self.read_metrics.ciphertext_origin_ns)
            .with_histogram(&self.read_metrics.ciphertext_origin_latency);
        let encrypted = self.inner.get_opts(location, options).await?;
        if encrypted.meta != expected {
            self.invalidate_header(location);
            return Err(encryption_error(EncryptionError::Authentication));
        }
        let ciphertext = encrypted.bytes().await?;
        self.read_metrics
            .ciphertext_origin_bytes
            .fetch_add(ciphertext.len() as u64, Ordering::Relaxed);
        self.read_metrics.record_window(0, ciphertext.len() as u64);
        drop(timing);
        let plaintext = self
            .decrypt_chunks(location, header, index, ciphertext)
            .await?;
        if plaintext.len() != plaintext_chunk_len(header, index) {
            return Err(encryption_error(EncryptionError::Length));
        }
        Ok(Arc::new(AuthenticatedChunk {
            plaintext: Zeroizing::new(plaintext.to_vec()),
        }))
    }

    async fn cached_chunk(
        &self,
        location: &Path,
        header: Header,
        index: usize,
        expected: ObjectMeta,
        options: GetOptions,
    ) -> Result<Arc<AuthenticatedChunk>> {
        let key = ChunkKey {
            namespace: self.chunk_namespace,
            location: location.clone(),
            header,
            e_tag: expected.e_tag.clone(),
            version: expected.version.clone(),
            modified_seconds: expected.last_modified.timestamp(),
            modified_nanos: expected.last_modified.timestamp_subsec_nanos(),
            index,
        };
        let length = plaintext_chunk_len(header, index);
        loop {
            let notified = self.chunks.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut launch = None;
            let flight = {
                let mut state = self
                    .chunks
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(chunk) = state.entries.get(&key) {
                    self.read_metrics
                        .chunk_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(chunk.clone());
                }
                if let Some(flight) = state.flights.get(&key) {
                    self.read_metrics
                        .chunk_coalesced_reads
                        .fetch_add(1, Ordering::Relaxed);
                    Some(flight.clone())
                } else if state.flights.len() < self.chunks.limits.flights
                    && state.flight_bytes + length <= self.chunks.limits.flight_bytes
                {
                    let (sender, receiver) = tokio::sync::watch::channel(None);
                    let flight = Arc::new(ChunkFlight { result: receiver });
                    state.flights.insert(key.clone(), flight.clone());
                    state.flight_bytes += length;
                    self.read_metrics
                        .chunk_cache_misses
                        .fetch_add(1, Ordering::Relaxed);
                    self.read_metrics
                        .chunk_inflight
                        .fetch_add(1, Ordering::Relaxed);
                    launch = Some((
                        sender,
                        ChunkFlightGuard {
                            cache: self.chunks.clone(),
                            key: key.clone(),
                            bytes: length,
                        },
                    ));
                    Some(flight)
                } else {
                    None
                }
            };
            let Some(flight) = flight else {
                let _admission = ReadTiming::new(&self.read_metrics.chunk_admission_wait_ns)
                    .with_histogram(&self.read_metrics.chunk_admission_latency);
                notified.await;
                continue;
            };
            if let Some((sender, guard)) = launch {
                let store = self.clone();
                let location = location.clone();
                let expected = expected.clone();
                let options = options.clone();
                tokio::spawn(async move {
                    let result = store
                        .fetch_chunk(&location, header, index, expected, options)
                        .await
                        .map_err(Arc::new);
                    if let Ok(chunk) = &result {
                        guard.cache.record_fetch(&guard.key, chunk.plaintext.len());
                        if let Ok(keyring) = store.keyring.read() {
                            if keyring
                                .keys
                                .iter()
                                .any(|key| key.key.version == header.key_version)
                            {
                                guard.cache.insert(guard.key.clone(), chunk.clone());
                            }
                        }
                    }
                    sender.send_replace(Some(result));
                    drop(guard);
                });
            }
            let _wait = ReadTiming::new(&self.read_metrics.chunk_wait_ns)
                .with_histogram(&self.read_metrics.chunk_wait_latency);
            let mut result = flight.result.clone();
            loop {
                let completed = result.borrow_and_update().clone();
                if let Some(completed) = completed {
                    return completed.map_err(shared_chunk_error);
                }
                result
                    .changed()
                    .await
                    .map_err(|_| slatedb::object_store::Error::Generic {
                        store: "vaulticdb encrypted chunk cache",
                        source: Box::new(std::io::Error::other(
                            "encrypted chunk worker terminated",
                        )),
                    })?;
            }
        }
    }

    async fn header(
        &self,
        location: &Path,
        mut options: GetOptions,
    ) -> Result<(
        Header,
        ObjectMeta,
        slatedb::object_store::Attributes,
        slatedb::object_store::Extensions,
    )> {
        options.range = Some(GetRange::Bounded(0..HEADER_SIZE as u64));
        options.head = false;
        self.read_metrics
            .ciphertext_origin_calls
            .fetch_add(1, Ordering::Relaxed);
        let fetch = ReadTiming::new(&self.read_metrics.ciphertext_origin_ns)
            .with_histogram(&self.read_metrics.ciphertext_origin_latency);
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let bytes = result.bytes().await?;
        self.read_metrics
            .ciphertext_origin_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.read_metrics.record_window(0, bytes.len() as u64);
        drop(fetch);
        let _decode = ReadTiming::new(&self.read_metrics.header_decode_ns);
        let header = decode_header(&bytes)?;
        Ok((header, meta, attributes, extensions))
    }
}

fn plaintext_meta_from_header(meta: ObjectMeta, header: Header) -> Result<ObjectMeta> {
    if meta.size != ciphertext_len(header)? as u64 {
        return Err(encryption_error(EncryptionError::Length));
    }
    Ok(ObjectMeta {
        size: header.plaintext_len as u64,
        ..meta
    })
}

#[async_trait]
impl ObjectStore for EncryptedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.invalidate_header(location);
        let plaintext = collect_payload(payload);
        let encrypted = self.encrypt(location, plaintext).await?;
        let result = self
            .inner
            .put_opts(location, encrypted.into(), options)
            .await;
        self.invalidate_header(location);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = self
            .inner
            .put_multipart_opts(location, options.clone())
            .await?;
        Ok(Box::new(EncryptedMultipartUpload {
            store: self.clone(),
            location: location.clone(),
            inner,
            options,
            parts: Vec::new(),
            finished: false,
        }))
    }

    async fn get_opts(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        self.read_metrics.reads.fetch_add(1, Ordering::Relaxed);
        let tag = ObjectStoreCallTag::from_extensions(&options.extensions);
        let retry = tag.as_ref().is_some_and(|tag| tag.retry.is_some());
        if retry {
            self.invalidate_header(location);
        }
        let generation = self
            .headers
            .lock()
            .map_err(|_| encryption_error(EncryptionError::Header))?
            .generation;
        let requested_range = options.range.take();
        let cache_range =
            requested_range.is_some() && options.extensions.len() == usize::from(tag.is_some());
        let head = options.head;
        if head {
            self.read_metrics.head_reads.fetch_add(1, Ordering::Relaxed);
        } else if requested_range.is_some() {
            self.read_metrics
                .range_reads
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.read_metrics.full_reads.fetch_add(1, Ordering::Relaxed);
        }
        let mut header_response;
        let mut cached_meta = None;
        let mut header = if head {
            let response = self.header(location, options.clone()).await?;
            self.cache_header(
                location,
                response.0,
                &response.1,
                &response.2,
                &response.3,
                Self::immutable_sst_tag(&options),
                generation,
            );
            header_response = Some((response.1, response.2, response.3));
            response.0
        } else if let Some((cached, attributes, extensions)) =
            self.validated_cached_header(location, &options).await?
        {
            header_response = Some((cached.meta.clone(), attributes, extensions));
            cached_meta = Some(cached.meta);
            cached.header
        } else {
            let response = self.header(location, options.clone()).await?;
            self.cache_header(
                location,
                response.0,
                &response.1,
                &response.2,
                &response.3,
                Self::immutable_sst_tag(&options),
                generation,
            );
            header_response = Some((response.1, response.2, response.3));
            response.0
        };
        let mut range = resolve_range(requested_range.clone(), header.plaintext_len)?;
        if !head && range.is_empty() && cached_meta.is_some() {
            let response = self.header(location, options.clone()).await?;
            header = response.0;
            range = resolve_range(requested_range, header.plaintext_len)?;
            self.cache_header(
                location,
                header,
                &response.1,
                &response.2,
                &response.3,
                Self::immutable_sst_tag(&options),
                generation,
            );
            header_response = Some((response.1, response.2, response.3));
            cached_meta = None;
        }
        let (payload, encrypted_meta, attributes, extensions) = if head || range.is_empty() {
            let (meta, attributes, extensions) = match header_response {
                Some(response) => response,
                None => {
                    let response = self.header(location, options.clone()).await?;
                    (response.1, response.2, response.3)
                }
            };
            (Bytes::new(), meta, attributes, extensions)
        } else {
            self.read_metrics
                .requested_plaintext_bytes
                .fetch_add(range.len() as u64, Ordering::Relaxed);
            self.read_metrics.record_window(range.len() as u64, 0);
            let first_chunk = range.start / header.chunk_size;
            let last_chunk = (range.end - 1) / header.chunk_size;
            if cache_range
                && !retry
                && last_chunk - first_chunk < self.chunks.limits.range_chunks
                && header.chunk_size <= self.chunks.limits.chunk_bytes
            {
                let (meta, attributes, extensions) = header_response.as_ref().unwrap();
                if meta.e_tag.is_some() || meta.version.is_some() {
                    self.cipher(header.key_version)?;
                    let chunks = futures_util::future::try_join_all(
                        (first_chunk..=last_chunk).map(|index| {
                            self.cached_chunk(
                                location,
                                header,
                                index,
                                meta.clone(),
                                options.clone(),
                            )
                        }),
                    )
                    .await?;
                    let mut payload = BytesMut::with_capacity(range.len());
                    for (offset, chunk) in chunks.iter().enumerate() {
                        let chunk_start = (first_chunk + offset) * header.chunk_size;
                        let start = range.start.saturating_sub(chunk_start);
                        let end = (range.end - chunk_start).min(chunk.plaintext.len());
                        payload.extend_from_slice(&chunk.plaintext[start..end]);
                    }
                    let payload = payload.freeze();
                    return Ok(GetResult {
                        payload: GetResultPayload::Stream(
                            stream::once(async { Ok(payload) }).boxed(),
                        ),
                        meta: plaintext_meta_from_header(meta.clone(), header)?,
                        range: range.start as u64..range.end as u64,
                        attributes: attributes.clone(),
                        extensions: extensions.clone(),
                    });
                }
            }
            let ciphertext_start = encrypted_chunk_offset(header, first_chunk)?;
            let ciphertext_end = encrypted_chunk_offset(header, last_chunk)?
                .checked_add(plaintext_chunk_len(header, last_chunk) + TAG_SIZE)
                .ok_or_else(|| encryption_error(EncryptionError::Length))?;
            options.range = Some(GetRange::Bounded(
                ciphertext_start as u64..ciphertext_end as u64,
            ));
            options.head = false;
            self.read_metrics.rounded_ciphertext_bytes.fetch_add(
                (ciphertext_end - ciphertext_start) as u64,
                Ordering::Relaxed,
            );
            self.read_metrics
                .ciphertext_origin_calls
                .fetch_add(1, Ordering::Relaxed);
            let fetch = ReadTiming::new(&self.read_metrics.ciphertext_origin_ns)
                .with_histogram(&self.read_metrics.ciphertext_origin_latency);
            let encrypted = self.inner.get_opts(location, options).await?;
            if cached_meta
                .as_ref()
                .is_some_and(|cached| cached != &encrypted.meta)
            {
                self.invalidate_header(location);
                return Err(encryption_error(EncryptionError::Authentication));
            }
            let encrypted_meta = encrypted.meta.clone();
            let attributes = encrypted.attributes.clone();
            let extensions = encrypted.extensions.clone();
            let ciphertext = encrypted.bytes().await?;
            self.read_metrics
                .ciphertext_origin_bytes
                .fetch_add(ciphertext.len() as u64, Ordering::Relaxed);
            self.read_metrics.record_window(0, ciphertext.len() as u64);
            drop(fetch);
            let chunks = self
                .decrypt_chunks(location, header, first_chunk, ciphertext)
                .await?;
            let relative_start = range.start - first_chunk * header.chunk_size;
            let payload = if range.len() == chunks.len() {
                chunks
            } else {
                Bytes::copy_from_slice(&chunks[relative_start..relative_start + range.len()])
            };
            (payload, encrypted_meta, attributes, extensions)
        };
        let meta = plaintext_meta_from_header(encrypted_meta, header)?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async { Ok(payload) }).boxed()),
            meta,
            range: range.start as u64..range.end as u64,
            attributes,
            extensions,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        let locations = locations
            .map(move |location| {
                if let Ok(location) = &location {
                    store.invalidate_header(location);
                }
                location
            })
            .boxed();
        let store = self.clone();
        self.inner
            .delete_stream(locations)
            .map(move |result| {
                if let Ok(location) = &result {
                    store.invalidate_header(location);
                }
                result
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        self.inner
            .list(prefix)
            .then(move |meta| {
                let this = this.clone();
                async move { this.plaintext_meta(meta?).await }
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        self.inner
            .list_with_offset(prefix, offset)
            .then(move |meta| {
                let this = this.clone();
                async move { this.plaintext_meta(meta?).await }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        let mut objects = Vec::with_capacity(result.objects.len());
        for meta in result.objects {
            objects.push(self.plaintext_meta(meta).await?);
        }
        result.objects = objects;
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let plaintext = self.get(from).await?.bytes().await?;
        let put_options = PutOptions {
            mode: match options.mode {
                CopyMode::Overwrite => PutMode::Overwrite,
                CopyMode::Create => PutMode::Create,
            },
            extensions: options.extensions,
            ..Default::default()
        };
        self.put_opts(to, plaintext.into(), put_options).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct EncryptedMultipartUpload {
    store: EncryptedObjectStore,
    location: Path,
    inner: Box<dyn MultipartUpload>,
    options: PutMultipartOptions,
    parts: Vec<Bytes>,
    finished: bool,
}

#[async_trait]
impl MultipartUpload for EncryptedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.parts.push(collect_payload(data));
        Box::pin(async { Ok(()) })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.finished {
            return Err(encryption_error(EncryptionError::Length));
        }
        self.finished = true;
        let plaintext = collect_parts(std::mem::take(&mut self.parts));
        self.inner.abort().await?;
        let options = PutOptions {
            tags: self.options.tags.clone(),
            attributes: self.options.attributes.clone(),
            extensions: self.options.extensions.clone(),
            ..Default::default()
        };
        self.store
            .put_opts(&self.location, plaintext.into(), options)
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.finished = true;
        self.parts.clear();
        self.inner.abort().await
    }
}

fn encode_header(header: Header, target: &mut BytesMut) {
    target.extend_from_slice(MAGIC);
    target.extend_from_slice(&[FORMAT_VERSION, ALGORITHM_AES_256_GCM]);
    target.extend_from_slice(&header.key_version.to_be_bytes());
    target.extend_from_slice(&(header.chunk_size as u32).to_be_bytes());
    target.extend_from_slice(&(header.plaintext_len as u64).to_be_bytes());
    target.extend_from_slice(&header.nonce);
}

fn collect_payload(payload: PutPayload) -> Bytes {
    collect_parts(payload.into_iter().collect())
}

fn collect_parts(parts: Vec<Bytes>) -> Bytes {
    let length = parts.iter().map(Bytes::len).sum();
    let mut result = BytesMut::with_capacity(length);
    for part in parts {
        result.extend_from_slice(&part);
    }
    result.freeze()
}

fn decode_header(data: &[u8]) -> Result<Header> {
    if data.len() < HEADER_SIZE
        || &data[..8] != MAGIC
        || data[8] != FORMAT_VERSION
        || data[9] != ALGORITHM_AES_256_GCM
    {
        return Err(encryption_error(EncryptionError::Header));
    }
    let mut key_version_bytes = [0u8; 4];
    key_version_bytes.copy_from_slice(&data[10..14]);
    let key_version = u32::from_be_bytes(key_version_bytes);
    let mut chunk_size_bytes = [0u8; 4];
    chunk_size_bytes.copy_from_slice(&data[14..18]);
    let chunk_size = u32::from_be_bytes(chunk_size_bytes) as usize;
    let mut plaintext_len_bytes = [0u8; 8];
    plaintext_len_bytes.copy_from_slice(&data[18..26]);
    let plaintext_len = u64::from_be_bytes(plaintext_len_bytes) as usize;
    let mut nonce = [0u8; NONCE_SIZE];
    nonce.copy_from_slice(&data[26..38]);
    if key_version == 0 || chunk_size == 0 {
        return Err(encryption_error(EncryptionError::Header));
    }
    Ok(Header {
        key_version,
        chunk_size,
        plaintext_len,
        nonce,
    })
}

fn chunk_count(length: usize, chunk_size: usize) -> usize {
    (length.saturating_add(chunk_size - 1) / chunk_size).max(1)
}

fn ciphertext_len(header: Header) -> Result<usize> {
    HEADER_SIZE
        .checked_add(header.plaintext_len)
        .and_then(|length| {
            length.checked_add(chunk_count(header.plaintext_len, header.chunk_size) * TAG_SIZE)
        })
        .ok_or_else(|| encryption_error(EncryptionError::Length))
}

fn plaintext_chunk_len(header: Header, index: usize) -> usize {
    header
        .plaintext_len
        .saturating_sub(index * header.chunk_size)
        .min(header.chunk_size)
}

fn encrypted_chunk_offset(header: Header, index: usize) -> Result<usize> {
    HEADER_SIZE
        .checked_add(
            index
                .checked_mul(header.chunk_size + TAG_SIZE)
                .ok_or_else(|| encryption_error(EncryptionError::Length))?,
        )
        .ok_or_else(|| encryption_error(EncryptionError::Length))
}

fn chunk_nonce(base: [u8; NONCE_SIZE], index: usize) -> Result<[u8; NONCE_SIZE]> {
    let index = u32::try_from(index).map_err(|_| encryption_error(EncryptionError::Length))?;
    let mut nonce = base;
    let mut counter_bytes = [0u8; 4];
    counter_bytes.copy_from_slice(&nonce[8..12]);
    let counter = u32::from_be_bytes(counter_bytes) ^ index;
    nonce[8..12].copy_from_slice(&counter.to_be_bytes());
    Ok(nonce)
}

fn associated_data(
    repository_id: &str,
    location: &Path,
    header: Header,
    index: usize,
    chunk_len: usize,
) -> Vec<u8> {
    let mut aad = Vec::new();
    aad.extend_from_slice(MAGIC);
    aad.extend_from_slice(&[FORMAT_VERSION, ALGORITHM_AES_256_GCM]);
    aad.extend_from_slice(&(repository_id.len() as u64).to_be_bytes());
    aad.extend_from_slice(repository_id.as_bytes());
    aad.extend_from_slice(&(location.as_ref().len() as u64).to_be_bytes());
    aad.extend_from_slice(location.as_ref().as_bytes());
    aad.extend_from_slice(&header.key_version.to_be_bytes());
    aad.extend_from_slice(&(header.chunk_size as u32).to_be_bytes());
    aad.extend_from_slice(&(header.plaintext_len as u64).to_be_bytes());
    aad.extend_from_slice(&header.nonce);
    aad.extend_from_slice(&(index as u64).to_be_bytes());
    aad.extend_from_slice(&(chunk_len as u64).to_be_bytes());
    aad
}

fn resolve_range(range: Option<GetRange>, length: usize) -> Result<Range<usize>> {
    match range {
        None => Ok(0..length),
        Some(GetRange::Bounded(range))
            if range.start <= range.end && range.end <= length as u64 =>
        {
            Ok(range.start as usize..range.end as usize)
        }
        Some(GetRange::Offset(offset)) if offset <= length as u64 => Ok(offset as usize..length),
        Some(GetRange::Suffix(count)) => Ok(length.saturating_sub(count as usize)..length),
        _ => Err(encryption_error(EncryptionError::Range)),
    }
}

fn encryption_error(error: EncryptionError) -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "vaulticdb encrypted object store",
        source: Box::new(error),
    }
}

include!("encryption/tests.rs");
