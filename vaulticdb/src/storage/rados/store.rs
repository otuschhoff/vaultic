use std::{fmt, ops::Range, sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, Mutex}, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, stream::BoxStream, StreamExt};
use slatedb::object_store::{
    path::Path, Attributes, CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result, UploadPart,
};
use tokio::sync::Notify;

const STALE_MULTIPART_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub(super) struct StoredObject {
    pub(super) bytes: Bytes,
    pub(super) size: u64,
    pub(super) version: u64,
    pub(super) modified: SystemTime,
}

#[derive(Clone, Debug)]
pub(super) struct StoredMeta {
    pub(super) name: String,
    pub(super) size: u64,
    pub(super) version: u64,
    pub(super) modified: SystemTime,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum DriverError {
    #[error("object not found")]
    NotFound,
    #[error("object already exists")]
    Exists,
    #[error("object version precondition failed")]
    Precondition,
    #[error("invalid object range")]
    Range,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

pub(super) trait Driver: fmt::Debug + Send + Sync {
    fn put(&self, name: &str, bytes: Bytes, mode: WriteMode) -> std::result::Result<u64, DriverError>;
    fn get(&self, name: &str, range: Option<Range<u64>>) -> std::result::Result<StoredObject, DriverError>;
    fn delete(&self, name: &str) -> std::result::Result<(), DriverError>;
    fn list(&self, prefix: &str) -> std::result::Result<Vec<StoredMeta>, DriverError>;
}

#[derive(Clone, Copy, Debug)]
pub(super) enum WriteMode { Overwrite, Create, Update(u64) }

#[derive(Clone, Debug)]
pub(super) struct RadosStore { driver: Arc<dyn Driver>, prefix: String }

impl RadosStore {
    pub(super) fn new(driver: Arc<dyn Driver>, prefix: &str) -> Self {
        Self { driver, prefix: prefix.trim_matches('/').to_owned() }
    }
    fn name(&self, location: &Path) -> String { format!("{}/{}", self.prefix, location) }
    fn staging_prefix(&self) -> String {
        use sha2::{Digest, Sha256};
        let repository = Sha256::digest(self.prefix.as_bytes());
        format!(".vaultic-rados/multipart/{repository:x}/")
    }
    fn staging_name(&self, upload: u128, ordinal: usize) -> String {
        format!("{}{upload:032x}/{ordinal:016x}", self.staging_prefix())
    }
    async fn sweep_stale_multipart(&self) -> Result<()> {
        let prefix = self.staging_prefix(); let driver = Arc::clone(&self.driver);
        let objects = blocking(move || driver.list(&prefix)).await
            .map_err(|error| map_error(Path::from(".vaultic-rados/multipart"), error))?;
        let stale = objects.into_iter().filter(|value| {
            SystemTime::now().duration_since(value.modified).is_ok_and(|age| age >= STALE_MULTIPART_AGE)
        }).map(|value| StagedPart { ordinal: 0, name: value.name }).collect::<Vec<_>>();
        let _ = cleanup_staged(&self.driver, &stale).await;
        Ok(())
    }
    fn relative(&self, name: &str) -> Result<Path> {
        let value = name.strip_prefix(&self.prefix).and_then(|value| value.strip_prefix('/'))
            .ok_or_else(|| generic("object outside repository prefix"))?;
        Path::parse(value).map_err(Into::into)
    }
    async fn listed(&self, prefix: Option<&Path>) -> Result<Vec<ObjectMeta>> {
        let requested = prefix.map_or_else(|| self.prefix.clone(), |value| self.name(value));
        let driver = Arc::clone(&self.driver);
        let request = requested.clone();
        let values = blocking(move || driver.list(&request)).await.map_err(|error| map_error(Path::from(requested), error))?;
        let mut result = values.into_iter().map(|value| Ok(ObjectMeta {
            location: self.relative(&value.name)?, last_modified: value.modified.into(), size: value.size,
            e_tag: Some(value.version.to_string()), version: Some(value.version.to_string()),
        })).collect::<Result<Vec<_>>>()?;
        result.sort_by(|left, right| left.location.cmp(&right.location));
        Ok(result)
    }
}

impl fmt::Display for RadosStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("VaulticDB native RADOS object store") }
}

#[async_trait]
impl ObjectStore for RadosStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, options: PutOptions) -> Result<PutResult> {
        if !options.attributes.is_empty() { return Err(slatedb::object_store::Error::NotSupported { source: "RADOS attributes unsupported".into() }); }
        let mode = match options.mode {
            PutMode::Overwrite => WriteMode::Overwrite,
            PutMode::Create => WriteMode::Create,
            PutMode::Update(value) => WriteMode::Update(value.version.or(value.e_tag).ok_or_else(|| precondition(location, "missing RADOS version"))?.parse().map_err(|_| precondition(location, "invalid RADOS version"))?),
        };
        let name = self.name(location); let driver = Arc::clone(&self.driver); let bytes = Bytes::from(payload);
        let version = blocking(move || driver.put(&name, bytes, mode)).await.map_err(|error| map_error(location.clone(), error))?;
        Ok(PutResult { e_tag: Some(version.to_string()), version: Some(version.to_string()), extensions: Default::default() })
    }
    async fn put_multipart_opts(&self, location: &Path, options: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.sweep_stale_multipart().await?;
        Ok(Box::new(Multipart {
            store: self.clone(), location: location.clone(), options,
            upload: rand::random(), next_part: Arc::new(AtomicUsize::new(0)),
            parts: Arc::new(Mutex::new(Vec::new())), cancelled: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicUsize::new(0)), settled: Arc::new(Notify::new()),
            cleanup_errors: Arc::new(Mutex::new(Vec::new())), accepting_parts: true, finished: false,
        }))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let name = self.name(location); let driver = Arc::clone(&self.driver);
        let head = blocking(move || driver.get(&name, None)).await.map_err(|error| map_error(location.clone(), error))?;
        let meta = ObjectMeta { location: location.clone(), last_modified: head.modified.into(), size: head.size, e_tag: Some(head.version.to_string()), version: Some(head.version.to_string()) };
        options.check_preconditions(&meta)?;
        if options.version.as_ref().is_some_and(|value| value != &head.version.to_string()) { return Err(precondition(location, "RADOS version is not current")); }
        let range = options.range.as_ref().map_or(Ok(0..head.size), |value| value.as_range(head.size).map_err(|error| generic(error.to_string())))?;
        let bytes = if options.head || range.is_empty() { Bytes::new() } else if range == (0..head.size) { head.bytes } else {
            let name = self.name(location); let driver = Arc::clone(&self.driver); let read_range = range.clone();
            blocking(move || driver.get(&name, Some(read_range))).await.map_err(|error| map_error(location.clone(), error))?.bytes
        };
        Ok(GetResult { payload: GetResultPayload::Stream(stream::once(async { Ok(bytes) }).boxed()), meta, range, attributes: Attributes::new(), extensions: options.extensions })
    }
    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let store = self.clone(); locations.then(move |location| { let store = store.clone(); async move {
            let location = location?; let name = store.name(&location); let driver = Arc::clone(&store.driver);
            match blocking(move || driver.delete(&name)).await { Ok(()) | Err(DriverError::NotFound) => Ok(location), Err(error) => Err(map_error(location, error)) }
        }}).boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone(); let prefix = prefix.cloned(); stream::once(async move { store.listed(prefix.as_ref()).await }).map(|result| stream::iter(match result { Ok(values) => values.into_iter().map(Ok).collect(), Err(error) => vec![Err(error)] })).flatten().boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let values = self.listed(prefix).await?; let base = prefix.map(ToString::to_string).unwrap_or_default(); let mut objects = Vec::new(); let mut common = std::collections::BTreeSet::new();
        for value in values { let location = value.location.to_string(); let remainder = location.strip_prefix(&base).unwrap_or(&location).trim_start_matches('/'); if let Some((first, _)) = remainder.split_once('/') { common.insert(Path::from(if base.is_empty() { first.to_owned() } else { format!("{base}/{first}") })); } else { objects.push(value); } }
        Ok(ListResult { common_prefixes: common.into_iter().collect(), objects, extensions: Default::default() })
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let bytes = self.get_opts(from, GetOptions::default()).await?.bytes().await?; let mode = match options.mode { CopyMode::Overwrite => PutMode::Overwrite, CopyMode::Create => PutMode::Create };
        self.put_opts(to, bytes.into(), PutOptions { mode, extensions: options.extensions, ..Default::default() }).await?; Ok(())
    }
}

#[derive(Debug)]
struct Multipart {
    store: RadosStore,
    location: Path,
    options: PutMultipartOptions,
    upload: u128,
    next_part: Arc<AtomicUsize>,
    parts: Arc<Mutex<Vec<StagedPart>>>,
    cancelled: Arc<AtomicBool>,
    in_flight: Arc<AtomicUsize>,
    settled: Arc<Notify>,
    cleanup_errors: Arc<Mutex<Vec<String>>>,
    accepting_parts: bool,
    finished: bool,
}

#[derive(Clone, Debug)]
struct StagedPart { ordinal: usize, name: String }

struct InFlightPart {
    count: Arc<AtomicUsize>,
    settled: Arc<Notify>,
}

#[derive(Debug, thiserror::Error)]
enum MultipartCleanupError {
    #[error("failed to delete staged multipart objects: {failures:?}")]
    Deletions { failures: Vec<String> },
    #[error("RADOS cleanup worker failed: {message}")]
    Worker { message: String },
    #[error("multipart final object was published but staging cleanup failed")]
    Published {
        #[source]
        source: Box<slatedb::object_store::Error>,
    },
}

fn multipart_cleanup_error(error: MultipartCleanupError) -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "native RADOS",
        source: Box::new(error),
    }
}

impl Drop for InFlightPart {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.settled.notify_waiters();
    }
}

#[async_trait]
impl MultipartUpload for Multipart {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        if !self.accepting_parts {
            return Box::pin(async { Err(generic("multipart already finished")) });
        }
        let ordinal = self.next_part.fetch_add(1, Ordering::SeqCst);
        let name = self.store.staging_name(self.upload, ordinal);
        let driver = Arc::clone(&self.store.driver);
        let parts = Arc::clone(&self.parts);
        let cancelled = Arc::clone(&self.cancelled);
        let in_flight = Arc::clone(&self.in_flight);
        let settled = Arc::clone(&self.settled);
        let cleanup_errors = Arc::clone(&self.cleanup_errors);
        in_flight.fetch_add(1, Ordering::AcqRel);
        let guard = InFlightPart { count: in_flight, settled };
        Box::pin(async move {
            let staged_name = name.clone();
            let cleanup_name = staged_name.clone();
            blocking(move || {
            let _guard = guard;
                let result = match driver.put(&staged_name, Bytes::from(payload), WriteMode::Create) {
                    Ok(version) => match parts.lock() {
                        Ok(mut registered) => {
                            if cancelled.load(Ordering::Acquire) {
                                if let Err(error) = driver.delete(&cleanup_name) {
                                    if let Ok(mut errors) = cleanup_errors.lock() {
                                        errors.push(error.to_string());
                                    }
                                }
                            } else {
                                registered.push(StagedPart { ordinal, name: cleanup_name });
                            }
                            Ok(version)
                        }
                        Err(_) => Err(DriverError::Other("multipart staging lock poisoned".to_owned())),
                    },
                    Err(error) => Err(error),
                };
                result
            })
                .await
                .map_err(|error| map_error(Path::from(name.clone()), error))?;
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.finished { return Err(generic("multipart already completed")); }
        self.accepting_parts = false;
        loop {
            let settled = self.settled.notified();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            settled.await;
        }
        let mut parts = self.parts.lock().map_err(|_| generic("multipart staging lock poisoned"))?.clone();
        parts.sort_by_key(|part| part.ordinal);
        let mut bytes = Vec::new();
        for part in &parts {
            let name = part.name.clone();
            let driver = Arc::clone(&self.store.driver);
            let staged = blocking(move || driver.get(&name, None)).await
                .map_err(|error| map_error(Path::from(part.name.clone()), error))?;
            bytes.extend_from_slice(&staged.bytes);
        }
        let result = self.store.put_opts(&self.location, bytes.into(), PutOptions {
            tags: self.options.tags.clone(), attributes: self.options.attributes.clone(),
            extensions: self.options.extensions.clone(), ..Default::default()
        }).await?;
        self.finished = true;
        cleanup_staged(&self.store.driver, &parts).await.map_err(|error| {
            multipart_cleanup_error(MultipartCleanupError::Published {
                source: Box::new(error),
            })
        })?;
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        if self.finished { return Ok(()); }
        self.accepting_parts = false;
        let parts = {
            let parts = self.parts.lock().map_err(|_| generic("multipart staging lock poisoned"))?;
            self.cancelled.store(true, Ordering::Release);
            parts.clone()
        };
        loop {
            let settled = self.settled.notified();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            settled.await;
        }
        self.finished = true;
        cleanup_staged(&self.store.driver, &parts).await?;
        let errors = self.cleanup_errors.lock().map_err(|_| generic("multipart cleanup error lock poisoned"))?;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(multipart_cleanup_error(MultipartCleanupError::Deletions {
                failures: errors.clone(),
            }))
        }
    }
}

impl Drop for Multipart {
    fn drop(&mut self) {
        if self.finished { return; }
        let Ok(parts) = self.parts.lock().map(|parts| {
            self.cancelled.store(true, Ordering::Release);
            parts.clone()
        }) else { return; };
        let driver = Arc::clone(&self.store.driver);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return; };
        std::mem::drop(runtime.spawn_blocking(move || {
            for part in parts { let _ = driver.delete(&part.name); }
        }));
    }
}

async fn cleanup_staged(driver: &Arc<dyn Driver>, parts: &[StagedPart]) -> Result<()> {
    let driver = Arc::clone(driver); let parts = parts.to_vec();
    let failures = tokio::task::spawn_blocking(move || {
        parts
            .into_iter()
            .filter_map(|part| {
                driver
                    .delete(&part.name)
                    .err()
                    .map(|error| format!("{}: {error}", part.name))
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|error| {
        multipart_cleanup_error(MultipartCleanupError::Worker {
            message: error.to_string(),
        })
    })?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(multipart_cleanup_error(MultipartCleanupError::Deletions {
            failures,
        }))
    }
}

async fn blocking<T: Send + 'static>(operation: impl FnOnce() -> std::result::Result<T, DriverError> + Send + 'static) -> std::result::Result<T, DriverError> { tokio::task::spawn_blocking(operation).await.map_err(|error| DriverError::Other(format!("RADOS worker failed: {error}")))? }
fn map_error(path: Path, error: DriverError) -> slatedb::object_store::Error { match error { DriverError::NotFound => slatedb::object_store::Error::NotFound { path: path.to_string(), source: Box::new(error) }, DriverError::Exists => slatedb::object_store::Error::AlreadyExists { path: path.to_string(), source: Box::new(error) }, DriverError::Precondition => precondition(&path, error.to_string()), DriverError::Range | DriverError::Other(_) => generic(error.to_string()), DriverError::Io(_) => slatedb::object_store::Error::Generic { store: "native RADOS", source: Box::new(error) } } }
fn precondition(path: &Path, source: impl Into<String>) -> slatedb::object_store::Error { slatedb::object_store::Error::Precondition { path: path.to_string(), source: source.into().into() } }
fn generic(source: impl Into<String>) -> slatedb::object_store::Error { slatedb::object_store::Error::Generic { store: "native RADOS", source: source.into().into() } }

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::{ObjectStoreExt, UpdateVersion};
    use std::{collections::BTreeMap, sync::RwLock};

    #[derive(Debug, Default)]
    struct MemoryDriver {
        objects: RwLock<BTreeMap<String, StoredObject>>,
        next: std::sync::atomic::AtomicU64,
        fail_deletes: std::sync::atomic::AtomicBool,
        panic_deletes: std::sync::atomic::AtomicBool,
    }

    impl Driver for MemoryDriver {
        fn put(&self, name: &str, bytes: Bytes, mode: WriteMode) -> std::result::Result<u64, DriverError> {
            let mut objects = self.objects.write().map_err(|_| DriverError::Other("lock poisoned".to_owned()))?;
            match (mode, objects.get(name)) {
                (WriteMode::Create, Some(_)) => return Err(DriverError::Exists),
                (WriteMode::Update(expected), Some(current)) if expected != current.version => return Err(DriverError::Precondition),
                (WriteMode::Update(_), None) => return Err(DriverError::Precondition),
                _ => {}
            }
            let version = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            objects.insert(name.to_owned(), StoredObject { size: bytes.len() as u64, bytes, version, modified: SystemTime::now() });
            Ok(version)
        }
        fn get(&self, name: &str, range: Option<Range<u64>>) -> std::result::Result<StoredObject, DriverError> {
            let object = self.objects.read().map_err(|_| DriverError::Other("lock poisoned".to_owned()))?.get(name).cloned().ok_or(DriverError::NotFound)?;
            let Some(range) = range else { return Ok(object); };
            if range.start > range.end || range.end > object.size { return Err(DriverError::Range); }
            Ok(StoredObject { bytes: object.bytes.slice(range.start as usize..range.end as usize), ..object })
        }
        fn delete(&self, name: &str) -> std::result::Result<(), DriverError> {
            assert!(
                !self.panic_deletes.load(std::sync::atomic::Ordering::SeqCst),
                "injected delete panic"
            );
            if self.fail_deletes.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(DriverError::Other("injected delete failure".to_owned()));
            }
            self.objects.write().map_err(|_| DriverError::Other("lock poisoned".to_owned()))?.remove(name).map(|_| ()).ok_or(DriverError::NotFound)
        }
        fn list(&self, prefix: &str) -> std::result::Result<Vec<StoredMeta>, DriverError> { Ok(self.objects.read().map_err(|_| DriverError::Other("lock poisoned".to_owned()))?.iter().filter(|(name, _)| name.starts_with(prefix)).map(|(name, value)| StoredMeta { name: name.clone(), size: value.size, version: value.version, modified: value.modified }).collect()) }
    }

    #[tokio::test]
    async fn object_store_conformance() {
        let driver = Arc::new(MemoryDriver::default());
        let store = RadosStore::new(driver.clone(), "repo/db");
        let current = Path::from("manifest/current");
        let created = store.put_opts(&current, Bytes::from_static(b"0123456789").into(), PutOptions::from(PutMode::Create)).await.unwrap();
        assert!(matches!(store.put_opts(&current, Bytes::from_static(b"conflict").into(), PutOptions::from(PutMode::Create)).await, Err(slatedb::object_store::Error::AlreadyExists { .. })));
        assert_eq!(&store.get_range(&current, 3..7).await.unwrap()[..], b"3456");
        store.put_opts(&current, Bytes::from_static(b"next").into(), PutOptions::from(PutMode::Update(created.into()))).await.unwrap();
        let stale = store
            .put_opts(
                &current,
                Bytes::from_static(b"stale").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: Some("1".to_owned()),
                    version: Some("1".to_owned()),
                })),
            )
            .await;
        assert!(matches!(stale, Err(slatedb::object_store::Error::Precondition { .. })));
        let mut upload = store.put_multipart(&Path::from("sst/one")).await.unwrap();
        upload.put_part(Bytes::from_static(b"part-a").into()).await.unwrap();
        upload.put_part(Bytes::from_static(b"part-b").into()).await.unwrap();
        assert_eq!(driver.objects.read().unwrap().keys().filter(|name| name.starts_with(".vaultic-rados/multipart/")).count(), 2);
        upload.complete().await.unwrap();
        assert!(!driver.objects.read().unwrap().keys().any(|name| name.starts_with(".vaultic-rados/multipart/")));
        assert!(upload.put_part(Bytes::from_static(b"late").into()).await.is_err());
        assert!(!driver.objects.read().unwrap().keys().any(|name| name.starts_with(".vaultic-rados/multipart/")));
        assert_eq!(&store.get(&Path::from("sst/one")).await.unwrap().bytes().await.unwrap()[..], b"part-apart-b");

        let mut raced = store.put_multipart(&Path::from("sst/raced")).await.unwrap();
        let issued = raced.put_part(Bytes::from_static(b"issued-before-complete").into());
        let writer = tokio::spawn(issued);
        raced.complete().await.unwrap();
        writer.await.unwrap().unwrap();
        assert_eq!(&store.get(&Path::from("sst/raced")).await.unwrap().bytes().await.unwrap()[..], b"issued-before-complete");
        assert!(!driver.objects.read().unwrap().keys().any(|name| name.starts_with(".vaultic-rados/multipart/")));
        assert_eq!(store.list(None).collect::<Vec<_>>().await.len(), 3);

        let mut abandoned = store.put_multipart(&Path::from("sst/aborted")).await.unwrap();
        abandoned.put_part(Bytes::from_static(b"temporary").into()).await.unwrap();
        abandoned.abort().await.unwrap();
        assert!(!driver.objects.read().unwrap().keys().any(|name| name.starts_with(".vaultic-rados/multipart/")));
        assert!(abandoned.put_part(Bytes::from_static(b"late").into()).await.is_err());
        assert!(!driver.objects.read().unwrap().keys().any(|name| name.starts_with(".vaultic-rados/multipart/")));
        assert!(matches!(store.get(&Path::from("sst/aborted")).await, Err(slatedb::object_store::Error::NotFound { .. })));

        let stale_name = store.staging_name(7, 0);
        driver.objects.write().unwrap().insert(stale_name.clone(), StoredObject {
            bytes: Bytes::from_static(b"stale"), size: 5, version: 50,
            modified: SystemTime::now() - STALE_MULTIPART_AGE - std::time::Duration::from_secs(1),
        });
        let fresh_name = store.staging_name(8, 0);
        driver.objects.write().unwrap().insert(fresh_name.clone(), StoredObject {
            bytes: Bytes::from_static(b"fresh"), size: 5, version: 51, modified: SystemTime::now(),
        });
        let mut upload = store.put_multipart(&Path::from("sst/sweep")).await.unwrap();
        assert!(!driver.objects.read().unwrap().contains_key(&stale_name));
        assert!(driver.objects.read().unwrap().contains_key(&fresh_name));
        upload.abort().await.unwrap();
    }

    #[tokio::test]
    async fn abort_reports_cleanup_failure_for_in_flight_part() {
        let driver = Arc::new(MemoryDriver::default());
        let store = RadosStore::new(driver.clone(), "repo/db");
        let mut upload = store
            .put_multipart(&Path::from("sst/cancelled"))
            .await
            .unwrap();
        let part = upload.put_part(Bytes::from_static(b"in-flight").into());
        driver.fail_deletes.store(true, Ordering::Release);
        let writer = tokio::spawn(part);

        let error = upload.abort().await.unwrap_err();
        let slatedb::object_store::Error::Generic { source, .. } = &error else {
            panic!("unexpected in-flight cleanup error: {error}");
        };
        assert!(matches!(source.downcast_ref(), Some(MultipartCleanupError::Deletions { failures }) if failures.len() == 1));
        writer.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn explicit_multipart_cleanup_failures_are_reported() {
        let driver = Arc::new(MemoryDriver::default());
        let store = RadosStore::new(driver.clone(), "repo/db");
        let mut aborted = store.put_multipart(&Path::from("sst/aborted-failure")).await.unwrap();
        aborted.put_part(Bytes::from_static(b"temporary").into()).await.unwrap();
        aborted.put_part(Bytes::from_static(b"second").into()).await.unwrap();
        let staged = driver.objects.read().unwrap().keys().filter(|name| name.starts_with(".vaultic-rados/multipart/")).cloned().collect::<Vec<_>>();
        driver.fail_deletes.store(true, std::sync::atomic::Ordering::SeqCst);
        let error = aborted.abort().await.unwrap_err();
        let slatedb::object_store::Error::Generic { source, .. } = &error else {
            panic!("unexpected cleanup error: {error}");
        };
        let Some(MultipartCleanupError::Deletions { failures }) = source.downcast_ref() else {
            panic!("unexpected cleanup source: {source}");
        };
        assert_eq!(failures.len(), 2);
        assert!(staged.iter().all(|name| failures.iter().any(|failure| failure.starts_with(name))));
        assert!(aborted.abort().await.is_ok());

        driver.fail_deletes.store(false, std::sync::atomic::Ordering::SeqCst);
        let location = Path::from("sst/completed-cleanup-failure");
        let mut completed = store.put_multipart(&location).await.unwrap();
        completed.put_part(Bytes::from_static(b"published").into()).await.unwrap();
        driver.fail_deletes.store(true, std::sync::atomic::Ordering::SeqCst);
        let error = completed.complete().await.unwrap_err();
        let slatedb::object_store::Error::Generic { source, .. } = &error else {
            panic!("unexpected completion error: {error}");
        };
        assert!(matches!(source.downcast_ref(), Some(MultipartCleanupError::Published { .. })));
        driver.fail_deletes.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(&store.get(&location).await.unwrap().bytes().await.unwrap()[..], b"published");
    }

    #[tokio::test]
    async fn abort_reports_cleanup_worker_panic() {
        let driver = Arc::new(MemoryDriver::default());
        let store = RadosStore::new(driver.clone(), "repo/db");
        let mut upload = store.put_multipart(&Path::from("sst/worker-panic")).await.unwrap();
        upload.put_part(Bytes::from_static(b"temporary").into()).await.unwrap();
        driver.panic_deletes.store(true, Ordering::Release);

        let error = upload.abort().await.unwrap_err();
        let slatedb::object_store::Error::Generic { source, .. } = &error else {
            panic!("unexpected worker error: {error}");
        };
        assert!(matches!(source.downcast_ref(), Some(MultipartCleanupError::Worker { .. })));
        assert!(upload.put_part(Bytes::from_static(b"late").into()).await.is_err());
    }
}