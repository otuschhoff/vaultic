#[derive(Clone, Debug)]
pub(crate) enum ObjectStoreConfig {
    Local { root: PathBuf },
    Memory,
    S3 {
        bucket: String,
        prefix: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        provider: Option<String>,
        bucket_lookup: Option<String>,
    },
    Replicated { replicas: Vec<ReplicaConfig> },
}

#[derive(Clone, Debug)]
pub(crate) struct ReplicaConfig {
    pub(crate) id: String,
    pub(crate) store: ReplicaStoreConfig,
}

#[derive(Clone, Debug)]
pub(crate) enum ReplicaStoreConfig {
    Local { root: PathBuf },
    Memory,
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
        let retained_segments = retained
            .as_ref()
            .map_or(0, |objects| objects.len() as u64);
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
            metrics
                .outstanding_flushes
                .fetch_sub(1, Ordering::AcqRel);
            metrics.last_flush_latency_ms.store(
                started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                Ordering::Release,
            );
            if result.is_ok() {
                metrics.uploaded_bytes.fetch_add(size, Ordering::AcqRel);
                uploaded_bytes.fetch_add(size, Ordering::AcqRel);
            } else {
                metrics
                    .durability_failures
                    .fetch_add(1, Ordering::AcqRel);
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
            self.metrics.uploaded_bytes.fetch_add(size, Ordering::AcqRel);
            self.metrics
                .retain(location, size, current_unix_ms());
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
        let size = self.inner.head(from).await.map_or(0, |metadata| metadata.size);
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
            self.metrics.uploaded_bytes.fetch_add(size, Ordering::AcqRel);
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
    let metrics = Arc::new(WalMetrics::default());
    let mut objects = store.list(None);
    while let Some(object) = objects.next().await {
        let object = object.context("inventory WAL object store")?;
        metrics.retain(
            &object.location,
            object.size,
            object.last_modified.timestamp_millis().try_into().unwrap_or(0),
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
        let expected = version.e_tag.clone().ok_or_else(|| {
            slatedb::object_store::Error::Precondition {
                path: location.to_string(),
                source: "local conditional update requires an ETag".into(),
            }
        })?;
        let _lock = self.lock().await?;
        self.inner
            .get_opts(
                location,
                GetOptions::new().with_if_match(Some(expected)),
            )
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
        self.current(true)?.put_opts(location, payload, options).await
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
        }) => format!(
            "rados:{cluster_fsid}:{monitors}:{pool}:{namespace}:{prefix}:{client}"
        ),
        WalStoreConfig::Store(ReplicaStoreConfig::Azure { .. }) => "unsupported:azure".to_owned(),
        WalStoreConfig::Store(ReplicaStoreConfig::Gcs { .. }) => "unsupported:gcs".to_owned(),
    }
}

fn wal_store_kind(config: &WalStoreConfig) -> &'static str {
    match config {
        WalStoreConfig::Inherit => "inherited",
        WalStoreConfig::Store(ReplicaStoreConfig::Local { .. }) => "local",
        WalStoreConfig::Store(ReplicaStoreConfig::Memory) => "memory",
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
        WalStoreConfig::Store(
            ReplicaStoreConfig::S3 { .. } | ReplicaStoreConfig::Rados { .. },
        ) => "shared-remote",
        WalStoreConfig::Store(ReplicaStoreConfig::Azure { .. } | ReplicaStoreConfig::Gcs { .. }) => {
            "unsupported"
        }
    }
}

fn replica_store(
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
    values.insert("bucket".to_owned(), serde_json::Value::String(bucket.to_owned()));
    values.insert(
        "url".to_owned(),
        serde_json::Value::String(endpoint.unwrap_or("https://s3.amazonaws.com").to_owned()),
    );
    values.insert(
        "region".to_owned(),
        serde_json::Value::String(region.unwrap_or_default().to_owned()),
    );
    if let Some(provider) = provider {
        values.insert("provider".to_owned(), serde_json::Value::String(provider.to_owned()));
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

        assert_eq!(claim_writer_epoch(store.as_ref(), None).await.unwrap(), Some(1));
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
