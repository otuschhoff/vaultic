use std::{
    collections::VecDeque,
    ops::Range,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use bytes::Bytes;
use futures_util::Stream;
use slatedb::object_store::GetResultPayload;

use crate::attribution::{OwnedTimingGuard, TimingMetric, TimingSnapshot};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ObjectStoreRole {
    Main,
    Wal,
    Coordination,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestObjectDelayProfile {
    #[cfg_attr(not(any(test, feature = "test-failpoints")), allow(dead_code))]
    version: u32,
    #[cfg_attr(not(any(test, feature = "test-failpoints")), allow(dead_code))]
    target: String,
    role: ObjectStoreRole,
    operation: String,
    delay_ms: u64,
}

impl TestObjectDelayProfile {
    #[cfg(any(test, feature = "test-failpoints"))]
    fn parse(encoded: &str) -> Result<Self> {
        let profile: Self =
            serde_json::from_str(encoded).context("decode test object delay profile")?;
        if profile.version != 1 {
            bail!("test object delay profile version must be 1");
        }
        if profile.target != "isolated" {
            bail!("test object delay profile target must be isolated");
        }
        if profile.operation != "put" {
            bail!("test object delay profile operation must be put");
        }
        if profile.delay_ms > 1_000 {
            bail!("test object delay profile delay must not exceed 1000 ms");
        }
        Ok(profile)
    }

    async fn delay(&self, role: ObjectStoreRole, operation: &str) {
        if self.role == role && self.operation == operation && self.delay_ms != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        }
    }
}

#[cfg(feature = "test-failpoints")]
fn test_object_delay_profile_from_env() -> Result<Option<Arc<TestObjectDelayProfile>>> {
    let Some(encoded) = std::env::var_os("VAULTICDB_TEST_OBJECT_DELAY_PROFILE") else {
        return Ok(None);
    };
    if std::env::var("VAULTICDB_TEST_CAPABILITY").as_deref()
        != Ok("vaulticdb-process-tests-v1")
    {
        bail!("test object delay profiles require the process-test capability");
    }
    let encoded = encoded
        .into_string()
        .map_err(|_| anyhow::anyhow!("test object delay profile is not UTF-8"))?;
    Ok(Some(Arc::new(TestObjectDelayProfile::parse(&encoded)?)))
}

#[cfg(not(feature = "test-failpoints"))]
fn test_object_delay_profile_from_env() -> Result<Option<Arc<TestObjectDelayProfile>>> {
    Ok(None)
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ObjectOperationSnapshot {
    pub(crate) timing: TimingSnapshot,
    pub(crate) transferred_bytes: u64,
    pub(crate) transferred_bytes_available: bool,
    pub(crate) timeout_outcomes_available: bool,
}

#[derive(Debug)]
struct ObjectOperationMetric {
    timing: Arc<TimingMetric>,
    transferred_bytes: AtomicU64,
    transferred_bytes_available: bool,
}

impl ObjectOperationMetric {
    fn new(enabled: bool, transferred_bytes_available: bool) -> Self {
        Self {
            timing: Arc::new(if enabled {
                TimingMetric::default()
            } else {
                TimingMetric::disabled(false)
            }),
            transferred_bytes: AtomicU64::new(0),
            transferred_bytes_available: enabled && transferred_bytes_available,
        }
    }

    fn snapshot(&self) -> ObjectOperationSnapshot {
        ObjectOperationSnapshot {
            timing: self.timing.snapshot(),
            transferred_bytes: self.transferred_bytes.load(Ordering::Relaxed),
            transferred_bytes_available: self.transferred_bytes_available,
            timeout_outcomes_available: false,
        }
    }

    fn timer(&self) -> OwnedTimingGuard {
        self.timing.timer_owned()
    }

    fn add_bytes(&self, bytes: u64) {
        let _ =
            self.transferred_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_add(bytes))
                });
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ObjectStoreRoleSnapshot {
    pub(crate) put: ObjectOperationSnapshot,
    pub(crate) multipart_init: ObjectOperationSnapshot,
    pub(crate) multipart_part: ObjectOperationSnapshot,
    pub(crate) multipart_complete: ObjectOperationSnapshot,
    pub(crate) multipart_abort: ObjectOperationSnapshot,
    pub(crate) get: ObjectOperationSnapshot,
    pub(crate) head: ObjectOperationSnapshot,
    pub(crate) get_body: ObjectOperationSnapshot,
    pub(crate) get_ranges: ObjectOperationSnapshot,
    pub(crate) delete: ObjectOperationSnapshot,
    pub(crate) list: ObjectOperationSnapshot,
    pub(crate) list_with_offset: ObjectOperationSnapshot,
    pub(crate) list_with_delimiter: ObjectOperationSnapshot,
    pub(crate) copy: ObjectOperationSnapshot,
    pub(crate) rename: ObjectOperationSnapshot,
    pub(crate) retry_delay_available: bool,
    pub(crate) background_pressure_available: bool,
}

#[derive(Debug)]
pub(crate) struct ObjectStoreRoleMetrics {
    put: Arc<ObjectOperationMetric>,
    multipart_init: Arc<ObjectOperationMetric>,
    multipart_part: Arc<ObjectOperationMetric>,
    multipart_complete: Arc<ObjectOperationMetric>,
    multipart_abort: Arc<ObjectOperationMetric>,
    get: Arc<ObjectOperationMetric>,
    head: Arc<ObjectOperationMetric>,
    get_body: Arc<ObjectOperationMetric>,
    get_ranges: Arc<ObjectOperationMetric>,
    delete: Arc<ObjectOperationMetric>,
    list: Arc<ObjectOperationMetric>,
    list_with_offset: Arc<ObjectOperationMetric>,
    list_with_delimiter: Arc<ObjectOperationMetric>,
    copy: Arc<ObjectOperationMetric>,
    rename: Arc<ObjectOperationMetric>,
}

impl Default for ObjectStoreRoleMetrics {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ObjectStoreRoleMetrics {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            put: Arc::new(ObjectOperationMetric::new(enabled, true)),
            multipart_init: Arc::new(ObjectOperationMetric::new(enabled, false)),
            multipart_part: Arc::new(ObjectOperationMetric::new(enabled, true)),
            multipart_complete: Arc::new(ObjectOperationMetric::new(enabled, false)),
            multipart_abort: Arc::new(ObjectOperationMetric::new(enabled, false)),
            get: Arc::new(ObjectOperationMetric::new(enabled, false)),
            head: Arc::new(ObjectOperationMetric::new(enabled, false)),
            get_body: Arc::new(ObjectOperationMetric::new(enabled, true)),
            get_ranges: Arc::new(ObjectOperationMetric::new(enabled, true)),
            delete: Arc::new(ObjectOperationMetric::new(enabled, false)),
            list: Arc::new(ObjectOperationMetric::new(enabled, false)),
            list_with_offset: Arc::new(ObjectOperationMetric::new(enabled, false)),
            list_with_delimiter: Arc::new(ObjectOperationMetric::new(enabled, false)),
            copy: Arc::new(ObjectOperationMetric::new(enabled, false)),
            rename: Arc::new(ObjectOperationMetric::new(enabled, false)),
        }
    }
}

impl ObjectStoreRoleMetrics {
    pub(crate) fn snapshot(&self) -> ObjectStoreRoleSnapshot {
        ObjectStoreRoleSnapshot {
            put: self.put.snapshot(),
            multipart_init: self.multipart_init.snapshot(),
            multipart_part: self.multipart_part.snapshot(),
            multipart_complete: self.multipart_complete.snapshot(),
            multipart_abort: self.multipart_abort.snapshot(),
            get: self.get.snapshot(),
            head: self.head.snapshot(),
            get_body: self.get_body.snapshot(),
            get_ranges: self.get_ranges.snapshot(),
            delete: self.delete.snapshot(),
            list: self.list.snapshot(),
            list_with_offset: self.list_with_offset.snapshot(),
            list_with_delimiter: self.list_with_delimiter.snapshot(),
            copy: self.copy.snapshot(),
            rename: self.rename.snapshot(),
            retry_delay_available: false,
            background_pressure_available: false,
        }
    }
}

#[derive(Debug)]
struct RoleAwareObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<ObjectStoreRoleMetrics>,
    tagged_wal_metrics: Option<Arc<ObjectStoreRoleMetrics>>,
    role: ObjectStoreRole,
    test_delay_profile: Option<Arc<TestObjectDelayProfile>>,
}

#[derive(Debug)]
struct RoleAwareMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    metrics: Arc<ObjectStoreRoleMetrics>,
}

struct MonitoredObjectStream<T> {
    inner: BoxStream<'static, slatedb::object_store::Result<T>>,
    guard: Option<OwnedTimingGuard>,
}

#[derive(Clone, Copy)]
enum DeleteRole {
    Main,
    Wal,
}

struct DeleteRoleGuard {
    guard: OwnedTimingGuard,
    failed: bool,
}

#[derive(Default)]
struct DeleteStreamState {
    pending: VecDeque<DeleteRole>,
    main: Option<DeleteRoleGuard>,
    wal: Option<DeleteRoleGuard>,
}

struct MonitoredDeleteStream {
    inner: BoxStream<'static, slatedb::object_store::Result<ObjectPath>>,
    state: Arc<std::sync::Mutex<DeleteStreamState>>,
}

impl<T> std::fmt::Debug for MonitoredObjectStream<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MonitoredObjectStream")
    }
}

impl<T> Stream for MonitoredObjectStream<T> {
    type Item = slatedb::object_store::Result<T>;

    fn poll_next(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(error))) => {
                if let Some(mut guard) = this.guard.take() {
                    guard.failed();
                }
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                if let Some(mut guard) = this.guard.take() {
                    guard.succeeded();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Stream for MonitoredDeleteStream {
    type Item = slatedb::object_store::Result<ObjectPath>;

    fn poll_next(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Err(error))) => {
                let mut state = this
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let role = state.pending.pop_front().unwrap_or(DeleteRole::Main);
                let role_guard = match role {
                    DeleteRole::Main => &mut state.main,
                    DeleteRole::Wal => &mut state.wal,
                };
                if let Some(role_guard) = role_guard {
                    role_guard.guard.failed();
                    role_guard.failed = true;
                }
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(path))) => {
                this.state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .pending
                    .pop_front();
                Poll::Ready(Some(Ok(path)))
            }
            Poll::Ready(None) => {
                settle_delete_guards(&this.state);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn settle_delete_guards(state: &std::sync::Mutex<DeleteStreamState>) {
    let (main, wal) = {
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (state.main.take(), state.wal.take())
    };
    for mut role_guard in [main, wal].into_iter().flatten() {
        if !role_guard.failed {
            role_guard.guard.succeeded();
        }
    }
}

fn monitored_stream<T: Send + 'static>(
    inner: BoxStream<'static, slatedb::object_store::Result<T>>,
    metric: Arc<ObjectOperationMetric>,
) -> BoxStream<'static, slatedb::object_store::Result<T>> {
    let guard = metric.timing.timer_owned();
    Box::pin(MonitoredObjectStream {
        inner,
        guard: Some(guard),
    })
}

fn settle<T>(guard: &mut OwnedTimingGuard, result: &slatedb::object_store::Result<T>) {
    if result.is_ok() {
        guard.succeeded();
    } else {
        guard.failed();
    }
}

#[async_trait]
impl MultipartUpload for RoleAwareMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let bytes = payload.content_length().try_into().unwrap_or(u64::MAX);
        let future = self.inner.put_part(payload);
        let metrics = Arc::clone(&self.metrics);
        let mut guard = metrics.multipart_part.timer();
        Box::pin(async move {
            let result = future.await;
            settle(&mut guard, &result);
            if result.is_ok() {
                metrics.multipart_part.add_bytes(bytes);
            }
            result
        })
    }

    async fn complete(&mut self) -> slatedb::object_store::Result<PutResult> {
        let mut guard = self.metrics.multipart_complete.timer();
        let result = self.inner.complete().await;
        settle(&mut guard, &result);
        result
    }

    async fn abort(&mut self) -> slatedb::object_store::Result<()> {
        let mut guard = self.metrics.multipart_abort.timer();
        let result = self.inner.abort().await;
        settle(&mut guard, &result);
        result
    }
}

impl std::fmt::Display for RoleAwareObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "role-aware ({})", self.inner)
    }
}

impl RoleAwareObjectStore {
    fn is_wal_path(path: &ObjectPath) -> bool {
        let mut parts = path.as_ref().split('/').rev();
        let last = parts.next();
        last == Some("wal") || parts.next() == Some("wal")
    }

    fn metrics_for_path(&self, path: &ObjectPath) -> Arc<ObjectStoreRoleMetrics> {
        if Self::is_wal_path(path) {
            self.tagged_wal_metrics
                .as_ref()
                .map_or_else(|| Arc::clone(&self.metrics), Arc::clone)
        } else {
            Arc::clone(&self.metrics)
        }
    }

    fn metrics_for_paths(&self, paths: &[&ObjectPath]) -> Arc<ObjectStoreRoleMetrics> {
        paths
            .iter()
            .find_map(|path| {
                Self::is_wal_path(path)
                    .then(|| self.tagged_wal_metrics.as_ref())
                    .flatten()
            })
            .map_or_else(|| Arc::clone(&self.metrics), Arc::clone)
    }

    fn metrics_for_extensions(
        &self,
        extensions: &slatedb::object_store::Extensions,
    ) -> Arc<ObjectStoreRoleMetrics> {
        let is_wal = slatedb::object_store_tag::ObjectStoreCallTag::from_extensions(extensions)
            .is_some_and(|tag| tag.sst_type == slatedb::object_store_tag::SstType::Wal);
        if is_wal {
            self.tagged_wal_metrics
                .as_ref()
                .map_or_else(|| Arc::clone(&self.metrics), Arc::clone)
        } else {
            Arc::clone(&self.metrics)
        }
    }

    fn role_for_extensions(
        &self,
        extensions: &slatedb::object_store::Extensions,
    ) -> ObjectStoreRole {
        let is_wal = slatedb::object_store_tag::ObjectStoreCallTag::from_extensions(extensions)
            .is_some_and(|tag| tag.sst_type == slatedb::object_store_tag::SstType::Wal);
        if is_wal && self.tagged_wal_metrics.is_some() {
            ObjectStoreRole::Wal
        } else {
            self.role
        }
    }

    async fn test_delay(&self, role: ObjectStoreRole, operation: &str) {
        if let Some(profile) = &self.test_delay_profile {
            profile.delay(role, operation).await;
        }
    }
}

#[async_trait]
impl ObjectStore for RoleAwareObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        let bytes = payload.content_length().try_into().unwrap_or(u64::MAX);
        let metrics = self.metrics_for_extensions(&options.extensions);
        let role = self.role_for_extensions(&options.extensions);
        let mut guard = metrics.put.timer();
        self.test_delay(role, "put").await;
        let result = self.inner.put_opts(location, payload, options).await;
        settle(&mut guard, &result);
        if result.is_ok() {
            metrics.put.add_bytes(bytes);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        let metrics = self.metrics_for_extensions(&options.extensions);
        let mut guard = metrics.multipart_init.timer();
        let result = self.inner.put_multipart_opts(location, options).await;
        settle(&mut guard, &result);
        result.map(|inner| {
            Box::new(RoleAwareMultipartUpload { inner, metrics }) as Box<dyn MultipartUpload>
        })
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        let metrics = self.metrics_for_extensions(&options.extensions);
        let head = options.head;
        let metric = if head {
            Arc::clone(&metrics.head)
        } else {
            Arc::clone(&metrics.get)
        };
        let mut guard = metric.timer();
        let result = self.inner.get_opts(location, options).await;
        settle(&mut guard, &result);
        result.map(|mut result| {
            if head {
                return result;
            }
            result.payload = match result.payload {
                GetResultPayload::Stream(stream) => {
                    let body_metrics = Arc::clone(&metrics.get_body);
                    let counted = stream
                        .map(move |result| {
                            if let Ok(bytes) = &result {
                                body_metrics.add_bytes(bytes.len().try_into().unwrap_or(u64::MAX));
                            }
                            result
                        })
                        .boxed();
                    GetResultPayload::Stream(monitored_stream(
                        counted,
                        Arc::clone(&metrics.get_body),
                    ))
                }
                #[allow(unreachable_patterns)]
                payload => payload,
            };
            result
        })
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> slatedb::object_store::Result<Vec<Bytes>> {
        let metrics = self.metrics_for_path(location);
        let mut guard = metrics.get_ranges.timer();
        let result = self.inner.get_ranges(location, ranges).await;
        settle(&mut guard, &result);
        if let Ok(parts) = &result {
            metrics.get_ranges.add_bytes(
                parts
                    .iter()
                    .map(|part| part.len() as u64)
                    .fold(0, u64::saturating_add),
            );
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectPath>> {
        let main_metrics = Arc::clone(&self.metrics);
        let wal_metrics = self.tagged_wal_metrics.clone();
        let state = Arc::new(std::sync::Mutex::new(DeleteStreamState::default()));
        let input_state = Arc::clone(&state);
        let locations = locations
            .map(move |location| {
                let role = if location
                    .as_ref()
                    .is_ok_and(RoleAwareObjectStore::is_wal_path)
                    && wal_metrics.is_some()
                {
                    DeleteRole::Wal
                } else {
                    DeleteRole::Main
                };
                let mut state = input_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state.pending.push_back(role);
                let role_guard = match role {
                    DeleteRole::Main => &mut state.main,
                    DeleteRole::Wal => &mut state.wal,
                };
                if role_guard.is_none() {
                    let metrics = match role {
                        DeleteRole::Main => &main_metrics,
                        DeleteRole::Wal => wal_metrics.as_ref().unwrap_or(&main_metrics),
                    };
                    *role_guard = Some(DeleteRoleGuard {
                        guard: metrics.delete.timer(),
                        failed: false,
                    });
                }
                location
            })
            .boxed();
        Box::pin(MonitoredDeleteStream {
            inner: self.inner.delete_stream(locations),
            state,
        })
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        let metrics = prefix.map_or_else(
            || Arc::clone(&self.metrics),
            |prefix| self.metrics_for_path(prefix),
        );
        monitored_stream(self.inner.list(prefix), Arc::clone(&metrics.list))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        let metrics = prefix.map_or_else(
            || self.metrics_for_path(offset),
            |prefix| self.metrics_for_paths(&[prefix, offset]),
        );
        monitored_stream(
            self.inner.list_with_offset(prefix, offset),
            Arc::clone(&metrics.list_with_offset),
        )
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> slatedb::object_store::Result<ListResult> {
        let metrics = prefix.map_or_else(
            || Arc::clone(&self.metrics),
            |prefix| self.metrics_for_path(prefix),
        );
        let mut guard = metrics.list_with_delimiter.timer();
        let result = self.inner.list_with_delimiter(prefix).await;
        settle(&mut guard, &result);
        result
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        let metrics = self.metrics_for_paths(&[from, to]);
        let mut guard = metrics.copy.timer();
        let result = self.inner.copy_opts(from, to, options).await;
        settle(&mut guard, &result);
        result
    }

    async fn rename_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: slatedb::object_store::RenameOptions,
    ) -> slatedb::object_store::Result<()> {
        let metrics = self.metrics_for_paths(&[from, to]);
        let mut guard = metrics.rename.timer();
        let result = self.inner.rename_opts(from, to, options).await;
        settle(&mut guard, &result);
        result
    }
}

fn role_aware_object_store(
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<ObjectStoreRoleMetrics>,
    tagged_wal_metrics: Option<Arc<ObjectStoreRoleMetrics>>,
    role: ObjectStoreRole,
    test_delay_profile: Option<Arc<TestObjectDelayProfile>>,
) -> Arc<dyn ObjectStore> {
    Arc::new(RoleAwareObjectStore {
        inner,
        metrics,
        tagged_wal_metrics,
        role,
        test_delay_profile,
    })
}

#[derive(Clone, Debug)]
pub(crate) enum ObjectStoreConfig {
    Local {
        root: PathBuf,
    },
    Memory,
    S3 {
        bucket: String,
        prefix: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        provider: Option<String>,
        bucket_lookup: Option<String>,
    },
    Rados {
        monitors: String,
        cluster_fsid: String,
        pool: String,
        namespace: String,
        prefix: String,
        client: String,
        key: Zeroizing<String>,
    },
    Replicated {
        replicas: Vec<ReplicaConfig>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ReplicaConfig {
    pub(crate) id: String,
    pub(crate) store: ReplicaStoreConfig,
}

#[derive(Clone, Debug)]
pub(crate) enum ReplicaStoreConfig {
    Local {
        root: PathBuf,
    },
    Memory,
    #[cfg(test)]
    Test(Arc<dyn ObjectStore>),
    S3 {
        bucket: String,
        prefix: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        access_key_id: Option<Zeroizing<String>>,
        secret_access_key: Option<Zeroizing<String>>,
        session_token: Option<Zeroizing<String>>,
        provider: Option<String>,
        bucket_lookup: Option<String>,
    },
    Rados {
        monitors: String,
        cluster_fsid: String,
        pool: String,
        namespace: String,
        prefix: String,
        client: String,
        key: Zeroizing<String>,
    },
    Azure {
        account: String,
        container: String,
        prefix: Option<String>,
        endpoint: String,
        access_key: Option<Zeroizing<String>>,
        bearer_token: Option<Zeroizing<String>>,
        sas_token: Option<Zeroizing<String>>,
    },
    Gcs {
        bucket: String,
        prefix: Option<String>,
        bearer_token: Option<Zeroizing<String>>,
        service_account_key: Option<Zeroizing<String>>,
    },
}

#[derive(Debug, Default)]
struct WalMetrics {
    uploaded_bytes: AtomicU64,
    outstanding_flushes: AtomicU64,
    durability_failures: AtomicU64,
    last_flush_latency_ms: AtomicU64,
    cleanup_failures: AtomicU64,
    retained: std::sync::Mutex<HashMap<String, (u64, u64)>>,
}

impl WalMetrics {
    fn snapshot(&self) -> WalStatus {
        let retained = self.retained.lock().ok();
        let retained_bytes = retained
            .as_ref()
            .map(|objects| objects.values().map(|(size, _)| size).sum())
            .unwrap_or(0);
        let retained_segments = retained.as_ref().map_or(0, |objects| objects.len() as u64);
        let oldest_segment_unix_ms = retained
            .as_ref()
            .and_then(|objects| objects.values().map(|(_, timestamp)| *timestamp).min())
            .unwrap_or(0);
        WalStatus {
            uploaded_bytes: self.uploaded_bytes.load(Ordering::Acquire),
            outstanding_flushes: self.outstanding_flushes.load(Ordering::Acquire),
            durability_failures: self.durability_failures.load(Ordering::Acquire),
            last_flush_latency_ms: self.last_flush_latency_ms.load(Ordering::Acquire),
            retained_bytes,
            retained_segments,
            oldest_segment_unix_ms,
            cleanup_failures: self.cleanup_failures.load(Ordering::Acquire),
        }
    }

    fn retain(&self, location: &ObjectPath, size: u64, timestamp_ms: u64) {
        if let Ok(mut retained) = self.retained.lock() {
            retained.insert(location.to_string(), (size, timestamp_ms));
        }
    }

    fn remove(&self, location: &ObjectPath) {
        if let Ok(mut retained) = self.retained.lock() {
            retained.remove(location.as_ref());
        }
    }
}

#[derive(Debug)]
struct MonitoredWalStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<WalMetrics>,
}

#[derive(Debug)]
struct MonitoredWalUpload {
    inner: Box<dyn MultipartUpload>,
    location: ObjectPath,
    metrics: Arc<WalMetrics>,
    uploaded_bytes: Arc<AtomicU64>,
}

#[async_trait]
impl MultipartUpload for MonitoredWalUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let size = payload.content_length() as u64;
        let upload = self.inner.put_part(payload);
        let metrics = Arc::clone(&self.metrics);
        let uploaded_bytes = Arc::clone(&self.uploaded_bytes);
        metrics.outstanding_flushes.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            let started = Instant::now();
            let result = upload.await;
            metrics.outstanding_flushes.fetch_sub(1, Ordering::AcqRel);
            metrics.last_flush_latency_ms.store(
                started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                Ordering::Release,
            );
            if result.is_ok() {
                metrics.uploaded_bytes.fetch_add(size, Ordering::AcqRel);
                uploaded_bytes.fetch_add(size, Ordering::AcqRel);
            } else {
                metrics.durability_failures.fetch_add(1, Ordering::AcqRel);
            }
            result
        })
    }

    async fn complete(&mut self) -> slatedb::object_store::Result<PutResult> {
        let started = Instant::now();
        self.metrics
            .outstanding_flushes
            .fetch_add(1, Ordering::AcqRel);
        let result = self.inner.complete().await;
        self.metrics
            .outstanding_flushes
            .fetch_sub(1, Ordering::AcqRel);
        self.metrics.last_flush_latency_ms.store(
            started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            Ordering::Release,
        );
        if result.is_ok() {
            self.metrics.retain(
                &self.location,
                self.uploaded_bytes.load(Ordering::Acquire),
                current_unix_ms(),
            );
        } else {
            self.metrics
                .durability_failures
                .fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    async fn abort(&mut self) -> slatedb::object_store::Result<()> {
        self.inner.abort().await
    }
}

impl std::fmt::Display for MonitoredWalStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "monitored WAL ({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MonitoredWalStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        let size = payload.content_length() as u64;
        let started = Instant::now();
        self.metrics
            .outstanding_flushes
            .fetch_add(1, Ordering::AcqRel);
        let result = self.inner.put_opts(location, payload, options).await;
        self.metrics
            .outstanding_flushes
            .fetch_sub(1, Ordering::AcqRel);
        self.metrics.last_flush_latency_ms.store(
            started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            Ordering::Release,
        );
        if result.is_ok() {
            self.metrics
                .uploaded_bytes
                .fetch_add(size, Ordering::AcqRel);
            self.metrics.retain(location, size, current_unix_ms());
        } else {
            self.metrics
                .durability_failures
                .fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(MonitoredWalUpload {
            inner,
            location: location.clone(),
            metrics: Arc::clone(&self.metrics),
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectPath>> {
        let metrics = Arc::clone(&self.metrics);
        self.inner
            .delete_stream(locations)
            .map(move |result| {
                match &result {
                    Ok(location) => metrics.remove(location),
                    Err(_) => {
                        metrics.cleanup_failures.fetch_add(1, Ordering::AcqRel);
                    }
                }
                result
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
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
        let size = self
            .inner
            .head(from)
            .await
            .map_or(0, |metadata| metadata.size);
        let started = Instant::now();
        self.metrics
            .outstanding_flushes
            .fetch_add(1, Ordering::AcqRel);
        let result = self.inner.copy_opts(from, to, options).await;
        self.metrics
            .outstanding_flushes
            .fetch_sub(1, Ordering::AcqRel);
        self.metrics.last_flush_latency_ms.store(
            started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            Ordering::Release,
        );
        if result.is_ok() {
            self.metrics
                .uploaded_bytes
                .fetch_add(size, Ordering::AcqRel);
            self.metrics.retain(to, size, current_unix_ms());
        } else {
            self.metrics
                .durability_failures
                .fetch_add(1, Ordering::AcqRel);
        }
        result
    }
}

async fn monitored_wal_store(
    store: Arc<dyn ObjectStore>,
) -> Result<(Arc<dyn ObjectStore>, Arc<WalMetrics>)> {
    #[cfg(any(test, feature = "test-failpoints"))]
    check_storage_failpoint(StorageFailpoint::InventoryWal)?;
    let metrics = Arc::new(WalMetrics::default());
    let mut objects = store.list(None);
    while let Some(object) = objects.next().await {
        let object = object.context("inventory WAL object store")?;
        metrics.retain(
            &object.location,
            object.size,
            object
                .last_modified
                .timestamp_millis()
                .try_into()
                .unwrap_or(0),
        );
    }
    Ok((
        Arc::new(MonitoredWalStore {
            inner: store,
            metrics: Arc::clone(&metrics),
        }),
        metrics,
    ))
}

#[derive(Debug)]
struct RenewableObjectStore {
    current: std::sync::RwLock<Arc<dyn ObjectStore>>,
    valid_until_ms: AtomicU64,
    write_until_ms: AtomicU64,
}

impl std::fmt::Display for RenewableObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("brokered renewable object store")
    }
}

#[derive(Debug)]
struct ConditionalLocalFileSystem {
    inner: LocalFileSystem,
    lock_path: PathBuf,
}

impl ConditionalLocalFileSystem {
    fn new(root: &std::path::Path) -> Result<Self> {
        Ok(Self {
            inner: LocalFileSystem::new_with_prefix(root)
                .with_context(|| format!("open SlateDB data directory {}", root.display()))?,
            lock_path: root.with_extension("coordination.lock"),
        })
    }

    async fn lock(&self) -> slatedb::object_store::Result<File> {
        let lock_path = self.lock_path.clone();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)?;
            file.lock_exclusive()?;
            Ok::<_, std::io::Error>(file)
        })
        .await
        .map_err(|source| slatedb::object_store::Error::Generic {
            store: "ConditionalLocalFileSystem",
            source: Box::new(source),
        })?
        .map_err(|source| slatedb::object_store::Error::Generic {
            store: "ConditionalLocalFileSystem",
            source: Box::new(source),
        })
    }
}

impl std::fmt::Display for ConditionalLocalFileSystem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "conditional {}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ConditionalLocalFileSystem {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        mut options: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        let PutMode::Update(version) = &options.mode else {
            return self.inner.put_opts(location, payload, options).await;
        };
        let expected =
            version
                .e_tag
                .clone()
                .ok_or_else(|| slatedb::object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "local conditional update requires an ETag".into(),
                })?;
        let _lock = self.lock().await?;
        self.inner
            .get_opts(location, GetOptions::new().with_if_match(Some(expected)))
            .await?;
        options.mode = PutMode::Overwrite;
        self.inner.put_opts(location, payload, options).await
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
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
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

impl RenewableObjectStore {
    fn new(current: Arc<dyn ObjectStore>, valid_until_ms: u64, ttl: std::time::Duration) -> Self {
        Self {
            current: std::sync::RwLock::new(current),
            valid_until_ms: AtomicU64::new(valid_until_ms),
            write_until_ms: AtomicU64::new(
                valid_until_ms.saturating_sub(storage_write_safety_margin(ttl).as_millis() as u64),
            ),
        }
    }

    fn current(&self, write: bool) -> slatedb::object_store::Result<Arc<dyn ObjectStore>> {
        let deadline = if write {
            self.write_until_ms.load(Ordering::Acquire)
        } else {
            self.valid_until_ms.load(Ordering::Acquire)
        };
        if unix_time_ms().unwrap_or(deadline) >= deadline {
            return Err(slatedb::object_store::Error::Generic {
                store: "brokered",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "storage credential expired",
                )),
            });
        }
        self.current
            .read()
            .map(|current| Arc::clone(&current))
            .map_err(|_| slatedb::object_store::Error::Generic {
                store: "brokered",
                source: Box::new(std::io::Error::other("storage credential lock poisoned")),
            })
    }

    fn replace(
        &self,
        next: Arc<dyn ObjectStore>,
        valid_until_ms: u64,
        ttl: std::time::Duration,
    ) -> Result<()> {
        *self
            .current
            .write()
            .map_err(|_| anyhow::anyhow!("storage credential lock poisoned"))? = next;
        self.valid_until_ms.store(valid_until_ms, Ordering::Release);
        self.write_until_ms.store(
            valid_until_ms.saturating_sub(storage_write_safety_margin(ttl).as_millis() as u64),
            Ordering::Release,
        );
        Ok(())
    }
}

fn storage_write_safety_margin(ttl: std::time::Duration) -> std::time::Duration {
    std::cmp::min(std::time::Duration::from_secs(300), ttl / 10)
}

#[async_trait]
impl ObjectStore for RenewableObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        self.current(true)?
            .put_opts(location, payload, options)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        self.current(true)?
            .put_multipart_opts(location, options)
            .await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        self.current(false)?.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectPath>> {
        match self.current(true) {
            Ok(current) => current.delete_stream(locations),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        match self.current(false) {
            Ok(current) => current.list(prefix),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> slatedb::object_store::Result<ListResult> {
        self.current(false)?.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.current(true)?.copy_opts(from, to, options).await
    }
}

pub(crate) fn object_store(
    repository_id: &str,
    config: &ObjectStoreConfig,
) -> Result<(String, Arc<dyn ObjectStore>)> {
    let repository_key = crate::repository_key(repository_id);
    match config {
        ObjectStoreConfig::Local { root } => {
            let root = root.join(&repository_key);
            std::fs::create_dir_all(&root)
                .with_context(|| format!("create SlateDB data directory {}", root.display()))?;
            let store = ConditionalLocalFileSystem::new(&root)?;
            Ok(("db".to_owned(), Arc::new(store)))
        }
        ObjectStoreConfig::Memory => Ok((repository_key, Arc::new(InMemory::new()))),
        ObjectStoreConfig::S3 {
            bucket,
            prefix,
            endpoint,
            region,
            provider,
            bucket_lookup,
        } => {
            let builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
            let store = configure_s3_builder(
                builder,
                bucket,
                endpoint.as_deref(),
                region.as_deref(),
                provider.as_deref(),
                bucket_lookup.as_deref(),
            )?
            .build()
            .context("configure S3-compatible object store")?;
            let path = match prefix {
                Some(prefix) => {
                    format!("{}/{repository_key}", prefix.trim_matches('/'))
                }
                None => repository_key,
            };
            Ok((path, Arc::new(store)))
        }
        ObjectStoreConfig::Rados {
            monitors,
            cluster_fsid,
            pool,
            namespace,
            prefix,
            client,
            key,
        } => Ok((
            "db".to_owned(),
            rados::open(rados::Config {
                monitors,
                cluster_fsid,
                pool,
                namespace,
                prefix: &format!("{}/{repository_key}", prefix.trim_matches('/')),
                client,
                key,
            })
            .context("configure native RADOS object store")?,
        )),
        ObjectStoreConfig::Replicated { replicas } => {
            replicated_object_store(replicas, &repository_key)
        }
    }
}

fn replicated_object_store(
    replicas: &[ReplicaConfig],
    repository_key: &str,
) -> Result<(String, Arc<dyn ObjectStore>)> {
    let mut stores = Vec::new();
    for replica in replicas {
        stores.push((
            replica.id.clone(),
            replica_store(&replica.store, repository_key, &replica.id)?,
        ));
    }
    Ok((
        "db".to_owned(),
        Arc::new(ReplicatedObjectStore::new(stores)?),
    ))
}

fn replicated_replica_store(
    config: &ObjectStoreConfig,
    id: &str,
    repository_key: &str,
) -> Result<Arc<dyn ObjectStore>> {
    let ObjectStoreConfig::Replicated { replicas } = config else {
        bail!("fencing replica requires replicated object storage");
    };
    let replica = replicas
        .iter()
        .find(|replica| replica.id == id)
        .with_context(|| format!("fencing replica {id:?} is not configured"))?;
    replica_store(&replica.store, repository_key, id)
}

fn wal_object_store(
    repository_id: &str,
    config: &ReplicaStoreConfig,
) -> Result<Arc<dyn ObjectStore>> {
    replica_store(config, &crate::repository_key(repository_id), "wal")
}

fn repository_control_store(
    config: &ObjectStoreConfig,
    path: &str,
    store: Arc<dyn ObjectStore>,
) -> Arc<dyn ObjectStore> {
    match config {
        ObjectStoreConfig::S3 { .. } => Arc::new(PrefixStore::new(store, ObjectPath::from(path))),
        _ => store,
    }
}

fn wal_store_identity(config: &WalStoreConfig) -> String {
    match config {
        WalStoreConfig::Inherit => "inherit".to_owned(),
        WalStoreConfig::Store(ReplicaStoreConfig::Local { root }) => {
            format!("local:{}", root.display())
        }
        WalStoreConfig::Store(ReplicaStoreConfig::Memory) => "memory".to_owned(),
        #[cfg(test)]
        WalStoreConfig::Store(ReplicaStoreConfig::Test(_)) => "test".to_owned(),
        WalStoreConfig::Store(ReplicaStoreConfig::S3 {
            bucket,
            prefix,
            endpoint,
            region,
            provider,
            bucket_lookup,
            ..
        }) => format!(
            "s3:{bucket}:{}:{}:{}:{}:{}",
            prefix.as_deref().unwrap_or_default(),
            endpoint.as_deref().unwrap_or_default(),
            region.as_deref().unwrap_or_default(),
            provider.as_deref().unwrap_or_default(),
            bucket_lookup.as_deref().unwrap_or_default()
        ),
        WalStoreConfig::Store(ReplicaStoreConfig::Rados {
            monitors,
            cluster_fsid,
            pool,
            namespace,
            prefix,
            client,
            ..
        }) => format!("rados:{cluster_fsid}:{monitors}:{pool}:{namespace}:{prefix}:{client}"),
        WalStoreConfig::Store(ReplicaStoreConfig::Azure { .. }) => "unsupported:azure".to_owned(),
        WalStoreConfig::Store(ReplicaStoreConfig::Gcs { .. }) => "unsupported:gcs".to_owned(),
    }
}

fn wal_store_kind(config: &WalStoreConfig) -> &'static str {
    match config {
        WalStoreConfig::Inherit => "inherited",
        WalStoreConfig::Store(ReplicaStoreConfig::Local { .. }) => "local",
        WalStoreConfig::Store(ReplicaStoreConfig::Memory) => "memory",
        #[cfg(test)]
        WalStoreConfig::Store(ReplicaStoreConfig::Test(_)) => "test",
        WalStoreConfig::Store(ReplicaStoreConfig::S3 { .. }) => "s3",
        WalStoreConfig::Store(ReplicaStoreConfig::Rados { .. }) => "rados",
        WalStoreConfig::Store(ReplicaStoreConfig::Azure { .. }) => "unsupported-azure",
        WalStoreConfig::Store(ReplicaStoreConfig::Gcs { .. }) => "unsupported-gcs",
    }
}

fn wal_store_durability(config: &WalStoreConfig) -> &'static str {
    match config {
        WalStoreConfig::Inherit => "inherited",
        WalStoreConfig::Store(ReplicaStoreConfig::Local { .. } | ReplicaStoreConfig::Memory) => {
            "local-process"
        }
        #[cfg(test)]
        WalStoreConfig::Store(ReplicaStoreConfig::Test(_)) => "test",
        WalStoreConfig::Store(ReplicaStoreConfig::S3 { .. } | ReplicaStoreConfig::Rados { .. }) => {
            "shared-remote"
        }
        WalStoreConfig::Store(
            ReplicaStoreConfig::Azure { .. } | ReplicaStoreConfig::Gcs { .. },
        ) => "unsupported",
    }
}

pub(super) fn replica_store(
    config: &ReplicaStoreConfig,
    repository_key: &str,
    id: &str,
) -> Result<Arc<dyn ObjectStore>> {
    match config {
        ReplicaStoreConfig::Local { root } => {
            let root = root.join(repository_key);
            std::fs::create_dir_all(&root).with_context(|| {
                format!(
                    "create replicated SlateDB data directory {}",
                    root.display()
                )
            })?;
            Ok(Arc::new(ConditionalLocalFileSystem::new(&root)?))
        }
        ReplicaStoreConfig::Memory => Ok(Arc::new(InMemory::new())),
        #[cfg(test)]
        ReplicaStoreConfig::Test(store) => Ok(store.clone()),
        ReplicaStoreConfig::S3 {
            bucket,
            prefix,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            provider,
            bucket_lookup,
        } => {
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
            builder = configure_s3_builder(
                builder,
                bucket,
                endpoint.as_deref(),
                region.as_deref(),
                provider.as_deref(),
                bucket_lookup.as_deref(),
            )?;
            if let Some(access_key_id) = access_key_id {
                builder = builder.with_access_key_id(access_key_id.as_str());
            }
            if let Some(secret_access_key) = secret_access_key {
                builder = builder.with_secret_access_key(secret_access_key.as_str());
            }
            if let Some(session_token) = session_token {
                builder = builder.with_token(session_token.as_str());
            }
            let store = builder
                .build()
                .with_context(|| format!("configure S3-compatible object store replica {id}"))?;
            let path = match prefix {
                Some(value) => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                None => repository_key.to_owned(),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
        ReplicaStoreConfig::Rados {
            monitors,
            cluster_fsid,
            pool,
            namespace,
            prefix,
            client,
            key,
        } => rados::open(rados::Config {
            monitors,
            cluster_fsid,
            pool,
            namespace,
            prefix: &format!("{}/{repository_key}", prefix.trim_matches('/')),
            client,
            key,
        })
        .with_context(|| format!("configure native RADOS object store replica {id}")),
        ReplicaStoreConfig::Azure {
            account,
            container,
            prefix,
            endpoint,
            access_key,
            bearer_token,
            sas_token,
        } => {
            let mut builder = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container)
                .with_endpoint(endpoint.clone());
            if let Some(access_key) = access_key {
                builder = builder.with_access_key(access_key.as_str());
            }
            if let Some(token) = bearer_token {
                builder = builder.with_bearer_token_authorization(token.as_str());
            }
            if let Some(token) = sas_token {
                builder = builder.with_sas_authorization(
                    split_sas(token.as_str()).context("parse Azure SAS credential")?,
                );
            }
            let store = builder
                .build()
                .with_context(|| format!("configure Azure Blob object store replica {id}"))?;
            let path = match prefix {
                Some(value) => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                None => repository_key.to_owned(),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
        ReplicaStoreConfig::Gcs {
            bucket,
            prefix,
            bearer_token,
            service_account_key,
        } => {
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
            if let Some(token) = bearer_token {
                builder = builder.with_bearer_token(token.as_str());
            }
            if let Some(key) = service_account_key {
                builder = builder.with_service_account_key(key.as_str());
            }
            let store = builder
                .build()
                .with_context(|| format!("configure GCS object store replica {id}"))?;
            let path = match prefix {
                Some(value) => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                None => repository_key.to_owned(),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
    }
}

fn configure_s3_builder(
    mut builder: AmazonS3Builder,
    bucket: &str,
    endpoint: Option<&str>,
    region: Option<&str>,
    provider: Option<&str>,
    bucket_lookup: Option<&str>,
) -> Result<AmazonS3Builder> {
    if provider.is_some_and(|value| !value.is_empty() && value != "generic") && endpoint.is_none() {
        bail!("VaulticDB S3 provider profile requires an explicit endpoint");
    }
    let mut values = std::collections::BTreeMap::new();
    values.insert(
        "bucket".to_owned(),
        serde_json::Value::String(bucket.to_owned()),
    );
    values.insert(
        "url".to_owned(),
        serde_json::Value::String(endpoint.unwrap_or("https://s3.amazonaws.com").to_owned()),
    );
    values.insert(
        "region".to_owned(),
        serde_json::Value::String(region.unwrap_or_default().to_owned()),
    );
    if let Some(provider) = provider {
        values.insert(
            "provider".to_owned(),
            serde_json::Value::String(provider.to_owned()),
        );
    }
    if let Some(bucket_lookup) = bucket_lookup {
        values.insert(
            "bucket_lookup".to_owned(),
            serde_json::Value::String(bucket_lookup.to_owned()),
        );
    }
    let profile = vaulticdb::topology::normalize_s3_endpoint(&values)?;
    if endpoint.is_some() {
        builder = builder.with_endpoint(&profile.endpoint);
    }
    if !profile.region.is_empty() {
        builder = builder.with_region(&profile.region);
    }
    builder = builder.with_virtual_hosted_style_request(profile.bucket_lookup == "dns");
    if profile.endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    Ok(builder)
}

#[cfg(test)]
mod object_store_tests {
    use super::*;

    #[tokio::test]
    async fn local_store_conditionally_replaces_stale_writer_claim() {
        let root = std::env::temp_dir().join(format!(
            "vaulticdb-local-takeover-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let (_, store) = object_store(
            "repository",
            &ObjectStoreConfig::Local { root: root.clone() },
        )
        .unwrap();

        assert_eq!(
            claim_writer_epoch(store.as_ref(), None).await.unwrap(),
            Some(1)
        );
        assert_eq!(
            claim_writer_epoch(store.as_ref(), Some(1)).await.unwrap(),
            Some(2)
        );
        assert!(claim_writer_epoch(store.as_ref(), Some(1)).await.is_err());
        assert_eq!(active_writer_epoch(store.as_ref()).await.unwrap(), Some(2));

        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn named_s3_provider_requires_an_explicit_endpoint() {
        let error = configure_s3_builder(
            AmazonS3Builder::new(),
            "bucket",
            None,
            Some("eu-central-2"),
            Some("wasabi"),
            None,
        )
        .expect_err("named provider without endpoint must fail");
        assert!(error
            .to_string()
            .contains("provider profile requires an explicit endpoint"));
    }

    #[tokio::test]
    async fn renewable_store_stops_writes_before_reads_and_recovers_after_swap() {
        let initial: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let existing = ObjectPath::from("existing");
        initial.put(&existing, "old".into()).await.unwrap();
        let store = RenewableObjectStore::new(
            initial,
            current_unix_ms() + 1_000,
            std::time::Duration::from_secs(100),
        );

        assert!(store.get(&existing).await.is_ok());
        let error = store
            .put(&ObjectPath::from("blocked"), "data".into())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("storage credential expired"));

        let replacement: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .replace(
                replacement,
                current_unix_ms() + 60_000,
                std::time::Duration::from_secs(100),
            )
            .unwrap();
        store
            .put(&ObjectPath::from("renewed"), "data".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn renewable_store_fails_reads_after_hard_expiry() {
        let store = RenewableObjectStore::new(
            Arc::new(InMemory::new()),
            current_unix_ms().saturating_sub(1),
            std::time::Duration::from_secs(100),
        );
        let error = store.get(&ObjectPath::from("expired")).await.unwrap_err();
        assert!(error.to_string().contains("storage credential expired"));
    }
}
