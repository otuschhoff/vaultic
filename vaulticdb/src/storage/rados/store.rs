use std::{fmt, ops::Range, sync::{atomic::{AtomicUsize, Ordering}, Arc, Mutex}, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, stream::BoxStream, StreamExt};
use slatedb::object_store::{
    path::Path, Attributes, CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result, UploadPart,
};

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
        cleanup_staged(&self.driver, &stale).await;
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
            parts: Arc::new(Mutex::new(Vec::new())), finished: false,
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
    finished: bool,
}

#[derive(Clone, Debug)]
struct StagedPart { ordinal: usize, name: String }

#[async_trait]
impl MultipartUpload for Multipart {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let ordinal = self.next_part.fetch_add(1, Ordering::SeqCst);
        let name = self.store.staging_name(self.upload, ordinal);
        let driver = Arc::clone(&self.store.driver);
        let parts = Arc::clone(&self.parts);
        Box::pin(async move {
            let staged_name = name.clone();
            blocking(move || driver.put(&staged_name, Bytes::from(payload), WriteMode::Create))
                .await
                .map_err(|error| map_error(Path::from(name.clone()), error))?;
            parts.lock().map_err(|_| generic("multipart staging lock poisoned"))?
                .push(StagedPart { ordinal, name });
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.finished { return Err(generic("multipart already completed")); }
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
        cleanup_staged(&self.store.driver, &parts).await;
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        if self.finished { return Ok(()); }
        let parts = self.parts.lock().map_err(|_| generic("multipart staging lock poisoned"))?.clone();
        cleanup_staged(&self.store.driver, &parts).await;
        self.finished = true;
        Ok(())
    }
}

impl Drop for Multipart {
    fn drop(&mut self) {
        if self.finished { return; }
        let Ok(parts) = self.parts.lock().map(|parts| parts.clone()) else { return; };
        let driver = Arc::clone(&self.store.driver);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return; };
        std::mem::drop(runtime.spawn_blocking(move || {
            for part in parts { let _ = driver.delete(&part.name); }
        }));
    }
}

async fn cleanup_staged(driver: &Arc<dyn Driver>, parts: &[StagedPart]) {
    let driver = Arc::clone(driver); let parts = parts.to_vec();
    let _ = tokio::task::spawn_blocking(move || {
        for part in parts { let _ = driver.delete(&part.name); }
    }).await;
}

async fn blocking<T: Send + 'static>(operation: impl FnOnce() -> std::result::Result<T, DriverError> + Send + 'static) -> std::result::Result<T, DriverError> { tokio::task::spawn_blocking(operation).await.map_err(|error| DriverError::Other(format!("RADOS worker failed: {error}")))? }
fn map_error(path: Path, error: DriverError) -> slatedb::object_store::Error { match error { DriverError::NotFound => slatedb::object_store::Error::NotFound { path: path.to_string(), source: Box::new(error) }, DriverError::Exists => slatedb::object_store::Error::AlreadyExists { path: path.to_string(), source: Box::new(error) }, DriverError::Precondition => precondition(&path, error.to_string()), DriverError::Range | DriverError::Other(_) => generic(error.to_string()) } }
fn precondition(path: &Path, source: impl Into<String>) -> slatedb::object_store::Error { slatedb::object_store::Error::Precondition { path: path.to_string(), source: source.into().into() } }
fn generic(source: impl Into<String>) -> slatedb::object_store::Error { slatedb::object_store::Error::Generic { store: "native RADOS", source: source.into().into() } }

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::{ObjectStoreExt, UpdateVersion};
    use std::{collections::BTreeMap, sync::RwLock};

    #[derive(Debug, Default)]
    struct MemoryDriver { objects: RwLock<BTreeMap<String, StoredObject>>, next: std::sync::atomic::AtomicU64 }

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
        fn delete(&self, name: &str) -> std::result::Result<(), DriverError> { self.objects.write().map_err(|_| DriverError::Other("lock poisoned".to_owned()))?.remove(name).map(|_| ()).ok_or(DriverError::NotFound) }
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
        assert_eq!(&store.get(&Path::from("sst/one")).await.unwrap().bytes().await.unwrap()[..], b"part-apart-b");
        assert_eq!(store.list(None).collect::<Vec<_>>().await.len(), 2);

        let mut abandoned = store.put_multipart(&Path::from("sst/aborted")).await.unwrap();
        abandoned.put_part(Bytes::from_static(b"temporary").into()).await.unwrap();
        abandoned.abort().await.unwrap();
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
}