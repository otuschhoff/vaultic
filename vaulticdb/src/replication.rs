use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::try_join_all;
use futures_util::{StreamExt, TryStreamExt};
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result, UploadPart,
};
use std::{fmt, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct ReplicatedObjectStore {
    stores: Arc<Vec<Replica>>,
}

#[derive(Clone)]
struct Replica {
    id: String,
    store: Arc<dyn ObjectStore>,
}

impl ReplicatedObjectStore {
    pub fn new(stores: Vec<(String, Arc<dyn ObjectStore>)>) -> Result<Self> {
        if stores.len() < 2 {
            return Err(generic_error(
                "replicated object store requires at least two replicas",
            ));
        }
        let replicas = stores
            .into_iter()
            .map(|(id, store)| Replica { id, store })
            .collect();
        Ok(Self {
            stores: Arc::new(replicas),
        })
    }

    async fn get_from_any(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut last_error = None;
        for replica in self.stores.iter() {
            match replica.store.get_opts(location, options.clone()).await {
                Ok(result) => return Ok(result),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| generic_error("replicated object store has no replicas")))
    }

    async fn put_replica(
        replica: &Replica,
        location: &Path,
        bytes: &Bytes,
        options: &PutOptions,
    ) -> Result<PutResult> {
        match replica
            .store
            .put_opts(location, bytes.clone().into(), options.clone())
            .await
        {
            Ok(result) => Ok(result),
            Err(slatedb::object_store::Error::AlreadyExists { .. })
                if matches!(options.mode, PutMode::Create) =>
            {
                let existing =
                    replica.store.get(location).await.map_err(|error| {
                        replica_error("idempotent create read", &replica.id, error)
                    })?;
                let meta = existing.meta.clone();
                let existing_bytes = existing
                    .bytes()
                    .await
                    .map_err(|error| replica_error("idempotent create read", &replica.id, error))?;
                if existing_bytes == *bytes {
                    Ok(PutResult {
                        e_tag: meta.e_tag,
                        version: meta.version,
                        extensions: Default::default(),
                    })
                } else {
                    Err(replica_error(
                        "idempotent create",
                        &replica.id,
                        generic_error("existing object content differs"),
                    ))
                }
            }
            Err(error) => Err(replica_error("put", &replica.id, error)),
        }
    }

    async fn put_create(
        &self,
        location: &Path,
        bytes: Bytes,
        options: PutOptions,
    ) -> Result<PutResult> {
        let mut missing = Vec::new();
        let mut existing_result = None;
        for replica in self.stores.iter() {
            match replica.store.get(location).await {
                Ok(existing) => {
                    let meta = existing.meta.clone();
                    let existing_bytes = existing
                        .bytes()
                        .await
                        .map_err(|error| replica_error("create preflight", &replica.id, error))?;
                    if existing_bytes != bytes {
                        return Err(replica_error(
                            "create preflight",
                            &replica.id,
                            generic_error("existing object content differs"),
                        ));
                    }
                    if existing_result.is_none() {
                        existing_result = Some(PutResult {
                            e_tag: meta.e_tag,
                            version: meta.version,
                            extensions: Default::default(),
                        });
                    }
                }
                Err(slatedb::object_store::Error::NotFound { .. }) => missing.push(replica),
                Err(error) => return Err(replica_error("create preflight", &replica.id, error)),
            }
        }
        if missing.is_empty() {
            return Ok(existing_result.expect("replicated object store has at least two replicas"));
        }
        let writes = missing
            .into_iter()
            .map(|replica| async { Self::put_replica(replica, location, &bytes, &options).await });
        let mut results = try_join_all(writes).await?;
        Ok(results
            .pop()
            .or(existing_result)
            .expect("replicated object store has at least two replicas"))
    }

    async fn list_from_any(
        &self,
        prefix: Option<Path>,
        offset: Option<Path>,
    ) -> Result<Vec<ObjectMeta>> {
        let mut last_error = None;
        for replica in self.stores.iter() {
            let listed = match &offset {
                Some(offset) => replica.store.list_with_offset(prefix.as_ref(), offset),
                None => replica.store.list(prefix.as_ref()),
            }
            .try_collect::<Vec<_>>()
            .await;
            match listed {
                Ok(objects) => return Ok(objects),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| generic_error("replicated object store has no replicas")))
    }
}

impl fmt::Debug for ReplicatedObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicatedObjectStore")
            .field(
                "replicas",
                &self
                    .stores
                    .iter()
                    .map(|replica| &replica.id)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl fmt::Display for ReplicatedObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "vaulticdb replicated object store")
    }
}

#[async_trait]
impl ObjectStore for ReplicatedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let bytes: Bytes = payload.into();
        if matches!(options.mode, PutMode::Create) {
            return self.put_create(location, bytes, options).await;
        }
        let writes = self
            .stores
            .iter()
            .map(|replica| async { Self::put_replica(replica, location, &bytes, &options).await });
        let mut results = try_join_all(writes).await?;
        Ok(results.remove(0))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(ReplicatedMultipartUpload {
            store: self.clone(),
            location: location.clone(),
            options,
            parts: Arc::new(Mutex::new(Vec::new())),
            finished: false,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.get_from_any(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, Result<Path>> {
        let this = self.clone();
        locations
            .then(move |location| {
                let this = this.clone();
                async move {
                    let location = location?;
                    for replica in this.stores.iter() {
                        match replica.store.delete(&location).await {
                            Ok(()) => {}
                            Err(slatedb::object_store::Error::NotFound { .. }) => {}
                            Err(error) => {
                                return Err(replica_error("delete", &replica.id, error));
                            }
                        }
                    }
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        let prefix = prefix.cloned();
        stream_list(async move { this.list_from_any(prefix, None).await })
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures_util::stream::BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        let prefix = prefix.cloned();
        let offset = offset.clone();
        stream_list(async move { this.list_from_any(prefix, Some(offset)).await })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut last_error = None;
        for replica in self.stores.iter() {
            match replica.store.list_with_delimiter(prefix).await {
                Ok(result) => return Ok(result),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| generic_error("replicated object store has no replicas")))
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let bytes = self.get(from).await?.bytes().await?;
        let put_options = PutOptions {
            mode: match options.mode {
                slatedb::object_store::CopyMode::Overwrite => {
                    slatedb::object_store::PutMode::Overwrite
                }
                slatedb::object_store::CopyMode::Create => slatedb::object_store::PutMode::Create,
            },
            extensions: options.extensions,
            ..Default::default()
        };
        self.put_opts(to, bytes.into(), put_options).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct ReplicatedMultipartUpload {
    store: ReplicatedObjectStore,
    location: Path,
    options: PutMultipartOptions,
    parts: Arc<Mutex<Vec<Bytes>>>,
    finished: bool,
}

#[async_trait]
impl MultipartUpload for ReplicatedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let parts = self.parts.clone();
        let bytes = Bytes::from(data);
        Box::pin(async move {
            parts.lock().await.push(bytes);
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.finished {
            return Err(generic_error("multipart upload already completed"));
        }
        let parts = self.parts.lock().await;
        let total = parts.iter().map(Bytes::len).sum();
        let mut merged = Vec::with_capacity(total);
        for part in parts.iter() {
            merged.extend_from_slice(part);
        }
        drop(parts);
        self.finished = true;
        let options = PutOptions {
            tags: self.options.tags.clone(),
            attributes: self.options.attributes.clone(),
            extensions: self.options.extensions.clone(),
            ..Default::default()
        };
        self.store
            .put_opts(&self.location, merged.into(), options)
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }
}

fn replica_error(
    operation: &'static str,
    replica: &str,
    source: slatedb::object_store::Error,
) -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "vaulticdb replicated object store",
        source: format!("{operation} failed on replica {replica}: {source}").into(),
    }
}

fn generic_error(message: impl Into<String>) -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "vaulticdb replicated object store",
        source: message.into().into(),
    }
}

fn stream_list(
    listed: impl std::future::Future<Output = Result<Vec<ObjectMeta>>> + Send + 'static,
) -> futures_util::stream::BoxStream<'static, Result<ObjectMeta>> {
    futures_util::stream::once(listed)
        .flat_map(|result| match result {
            Ok(objects) => futures_util::stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => futures_util::stream::iter(vec![Err(error)]).boxed(),
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::{memory::InMemory, ObjectStoreExt, PutMode};

    #[tokio::test]
    async fn writes_delete_and_copy_all_replicas() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let source = Path::from("source");
        let target = Path::from("target");
        store
            .put(&source, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        assert_eq!(
            primary.get(&source).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        assert_eq!(
            secondary.get(&source).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        store.copy(&source, &target).await.unwrap();
        assert_eq!(
            primary.get(&target).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        assert_eq!(
            secondary.get(&target).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        store.delete(&source).await.unwrap();
        assert!(primary.get(&source).await.is_err());
        assert!(secondary.get(&source).await.is_err());
    }

    #[tokio::test]
    async fn reads_fail_over_from_primary_to_secondary() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("object");
        secondary
            .put(&path, Bytes::from_static(b"secondary").into())
            .await
            .unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            b"secondary"[..]
        );
    }

    #[tokio::test]
    async fn multipart_upload_is_replicated() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("multipart");
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"hello ").into())
            .await
            .unwrap();
        upload
            .put_part(Bytes::from_static(b"world").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();
        assert_eq!(
            primary.get(&path).await.unwrap().bytes().await.unwrap(),
            b"hello world"[..]
        );
        assert_eq!(
            secondary.get(&path).await.unwrap().bytes().await.unwrap(),
            b"hello world"[..]
        );
    }

    #[tokio::test]
    async fn create_retry_accepts_identical_existing_replica() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("create");
        primary
            .put(&path, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                Bytes::from_static(b"payload").into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        assert_eq!(
            secondary.get(&path).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
    }

    #[tokio::test]
    async fn create_retry_rejects_mismatched_existing_replica_before_writing_missing() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("mismatch");
        primary
            .put(&path, Bytes::from_static(b"different").into())
            .await
            .unwrap();
        assert!(store
            .put_opts(
                &path,
                Bytes::from_static(b"payload").into(),
                PutMode::Create.into(),
            )
            .await
            .is_err());
        assert!(secondary.get(&path).await.is_err());
    }
}
