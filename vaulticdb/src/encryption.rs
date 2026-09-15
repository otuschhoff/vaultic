//! Chunked authenticated encryption for metadata object stores.

use std::{
    collections::{HashMap, VecDeque},
    env,
    fmt::{Debug, Display, Formatter},
    ops::Range,
    sync::{Arc, Mutex, RwLock},
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
use zeroize::{Zeroize, ZeroizeOnDrop};

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
    inner: Arc<dyn ObjectStore>,
    repository_id: RepositoryId,
    keyring: Arc<RwLock<Keyring>>,
    crypto: Arc<CryptoExecutor>,
    headers: Arc<Mutex<HeaderCache>>,
    chunk_size: usize,
}

#[derive(Default)]
struct HeaderCache {
    entries: HashMap<Path, CachedHeader>,
    order: VecDeque<Path>,
}

#[derive(Clone)]
struct CachedHeader {
    header: Header,
    meta: ObjectMeta,
}

impl HeaderCache {
    fn get(&self, location: &Path) -> Option<CachedHeader> {
        self.entries.get(location).cloned()
    }

    fn insert(&mut self, location: Path, header: Header, meta: ObjectMeta) {
        if self
            .entries
            .insert(location.clone(), CachedHeader { header, meta })
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
        self.entries.remove(location);
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
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| encryption_error(EncryptionError::Authentication))?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let _permit = permit;
            let _ = sender.send(operation());
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

#[derive(Clone, Copy)]
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
        let crypto = Arc::new(CryptoExecutor::new(configured_crypto_threads()?)?);
        Ok(Self {
            inner,
            repository_id: repository_id.into(),
            keyring: Arc::new(RwLock::new(Keyring {
                keys,
                write_version,
            })),
            crypto,
            headers: Arc::new(Mutex::new(HeaderCache::default())),
            chunk_size: DEFAULT_CHUNK_SIZE,
        })
    }

    pub(crate) fn with_inner(&self, inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            repository_id: self.repository_id.clone(),
            keyring: self.keyring.clone(),
            crypto: self.crypto.clone(),
            headers: self.headers.clone(),
            chunk_size: self.chunk_size,
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
        self.headers
            .lock()
            .ok()
            .and_then(|headers| headers.get(location))
    }

    fn cache_header(&self, location: &Path, header: Header, meta: &ObjectMeta) {
        if let Ok(mut headers) = self.headers.lock() {
            headers.insert(location.clone(), header, meta.clone());
        }
    }

    fn invalidate_header(&self, location: &Path) {
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
            .run(move || store.decrypt_chunks_sync(&location, header, first_chunk, &ciphertext))
            .await
    }

    fn decrypt_chunks_sync(
        &self,
        location: &Path,
        header: Header,
        first_chunk: usize,
        ciphertext: &[u8],
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
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let header = decode_header(&result.bytes().await?)?;
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
        self.inner
            .put_opts(location, encrypted.into(), options)
            .await
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
        let requested_range = options.range.take();
        let head = options.head;
        let mut header_response = None;
        let mut cached_meta = None;
        let mut header = if head {
            let response = self.header(location, options.clone()).await?;
            self.cache_header(location, response.0, &response.1);
            header_response = Some((response.1, response.2, response.3));
            response.0
        } else if let Some(cached) = self.cached_header(location) {
            cached_meta = Some(cached.meta);
            cached.header
        } else {
            let response = self.header(location, options.clone()).await?;
            self.cache_header(location, response.0, &response.1);
            header_response = Some((response.1, response.2, response.3));
            response.0
        };
        let mut range = resolve_range(requested_range.clone(), header.plaintext_len)?;
        if !head && range.is_empty() && cached_meta.is_some() {
            let response = self.header(location, options.clone()).await?;
            header = response.0;
            range = resolve_range(requested_range, header.plaintext_len)?;
            self.cache_header(location, header, &response.1);
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
            let first_chunk = range.start / header.chunk_size;
            let last_chunk = (range.end - 1) / header.chunk_size;
            let ciphertext_start = encrypted_chunk_offset(header, first_chunk)?;
            let ciphertext_end = encrypted_chunk_offset(header, last_chunk)?
                .checked_add(plaintext_chunk_len(header, last_chunk) + TAG_SIZE)
                .ok_or_else(|| encryption_error(EncryptionError::Length))?;
            options.range = Some(GetRange::Bounded(
                ciphertext_start as u64..ciphertext_end as u64,
            ));
            options.head = false;
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
            let chunks = self
                .decrypt_chunks(location, header, first_chunk, encrypted.bytes().await?)
                .await?;
            let relative_start = range.start - first_chunk * header.chunk_size;
            (
                chunks.slice(relative_start..relative_start + range.len()),
                encrypted_meta,
                attributes,
                extensions,
            )
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
        self.inner.delete_stream(locations)
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
