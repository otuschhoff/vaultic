use std::{
    fmt::{Debug, Display, Formatter},
    ops::Range,
    sync::{Arc, Mutex, RwLock},
};

use aes_gcm::{
    aead::{Aead, Payload},
    Aes256Gcm, KeyInit, Nonce,
};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{stream, stream::BoxStream, StreamExt};
use rand::RngCore;
use slatedb::object_store::{
    path::Path, CopyMode, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

const MAGIC: &[u8; 8] = b"VLTDBENC";
const FORMAT_VERSION: u8 = 1;
const ALGORITHM_AES_256_GCM: u8 = 1;
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const HEADER_SIZE: usize = 8 + 1 + 1 + 4 + 4 + 8 + NONCE_SIZE;
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

pub mod envelope;

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

pub(crate) fn is_integrity_error(error: &(dyn std::error::Error + 'static)) -> bool {
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
pub struct EncryptedObjectStore {
    inner: Arc<dyn ObjectStore>,
    repository_id: Arc<str>,
    keyring: Arc<RwLock<Keyring>>,
    chunk_size: usize,
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct EncryptionKey {
    pub version: u32,
    key: Box<[u8; 32]>,
}

impl EncryptionKey {
    pub fn new(version: u32, key: [u8; 32]) -> Self {
        let key = Box::new(key);
        #[cfg(unix)]
        unsafe {
            libc::mlock(key.as_ptr().cast(), key.len());
        }
        Self { version, key }
    }
}

impl Clone for EncryptionKey {
    fn clone(&self) -> Self {
        Self::new(self.version, *self.key)
    }
}

struct Keyring {
    keys: Vec<EncryptionKey>,
    write_version: u32,
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
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        repository_id: impl Into<Arc<str>>,
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
        Ok(Self {
            inner,
            repository_id: repository_id.into(),
            keyring: Arc::new(RwLock::new(Keyring {
                keys,
                write_version,
            })),
            chunk_size: DEFAULT_CHUNK_SIZE,
        })
    }

    #[cfg(test)]
    fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    fn key(&self, version: u32) -> Result<Zeroizing<[u8; 32]>> {
        self.keyring
            .read()
            .map_err(|_| encryption_error(EncryptionError::Header))?
            .keys
            .iter()
            .find(|key| key.version == version)
            .map(|key| Zeroizing::new(*key.key))
            .ok_or_else(|| encryption_error(EncryptionError::Header))
    }

    fn write_version(&self) -> Result<u32> {
        self.keyring
            .read()
            .map(|keyring| keyring.write_version)
            .map_err(|_| encryption_error(EncryptionError::Header))
    }

    pub fn install_write_key(&self, key: EncryptionKey) -> anyhow::Result<()> {
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
            .any(|existing| existing.version == key.version)
        {
            anyhow::bail!("metadata encryption key version already exists");
        }
        keyring.write_version = key.version;
        keyring.keys.push(key);
        Ok(())
    }

    pub fn retire_read_keys_before(&self, version: u32) -> anyhow::Result<()> {
        let mut keyring = self
            .keyring
            .write()
            .map_err(|_| anyhow::anyhow!("metadata encryption keyring lock poisoned"))?;
        if version != keyring.write_version {
            anyhow::bail!("only metadata DEKs older than the active write key can be retired");
        }
        keyring.keys.retain(|key| key.version >= version);
        Ok(())
    }

    fn encrypt(&self, location: &Path, plaintext: &[u8]) -> Result<Bytes> {
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
        let key = self.key(header.key_version)?;
        let cipher = Aes256Gcm::new_from_slice(key.as_ref())
            .map_err(|_| encryption_error(EncryptionError::Header))?;
        for index in 0..chunk_count(header.plaintext_len, header.chunk_size) {
            let start = index * header.chunk_size;
            let end = (start + header.chunk_size).min(plaintext.len());
            let chunk = &plaintext[start..end];
            let nonce = chunk_nonce(header.nonce, index)?;
            let aad = associated_data(&self.repository_id, location, header, index, chunk.len());
            let encrypted = cipher
                .encrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: chunk,
                        aad: &aad,
                    },
                )
                .map_err(|_| encryption_error(EncryptionError::Authentication))?;
            result.extend_from_slice(&encrypted);
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

    fn decrypt_chunks(
        &self,
        location: &Path,
        header: Header,
        first_chunk: usize,
        ciphertext: &[u8],
    ) -> Result<Bytes> {
        let key = self.key(header.key_version)?;
        let cipher = Aes256Gcm::new_from_slice(key.as_ref())
            .map_err(|_| encryption_error(EncryptionError::Header))?;
        let mut result = BytesMut::new();
        let mut offset = 0;
        let total_chunks = chunk_count(header.plaintext_len, header.chunk_size);
        for index in first_chunk..total_chunks {
            let plaintext_chunk_len = plaintext_chunk_len(header, index);
            let encrypted_len = plaintext_chunk_len + TAG_SIZE;
            if offset + encrypted_len > ciphertext.len() {
                break;
            }
            let nonce = chunk_nonce(header.nonce, index)?;
            let aad = associated_data(
                &self.repository_id,
                location,
                header,
                index,
                plaintext_chunk_len,
            );
            let plaintext = cipher
                .decrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: &ciphertext[offset..offset + encrypted_len],
                        aad: &aad,
                    },
                )
                .map_err(|_| encryption_error(EncryptionError::Authentication))?;
            result.extend_from_slice(&plaintext);
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
        let plaintext = collect_payload(payload);
        self.inner
            .put_opts(
                location,
                self.encrypt(location, &plaintext)?.into(),
                options,
            )
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(EncryptedMultipartUpload {
            store: self.clone(),
            location: location.clone(),
            options,
            parts: Arc::new(Mutex::new(Vec::new())),
            finished: false,
        }))
    }

    async fn get_opts(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        let requested_range = options.range.take();
        let head = options.head;
        let (header, encrypted_meta, attributes, extensions) =
            self.header(location, options.clone()).await?;
        let meta = plaintext_meta_from_header(encrypted_meta, header)?;
        let range = resolve_range(requested_range, header.plaintext_len)?;
        let payload = if head || range.is_empty() {
            Bytes::new()
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
            let chunks =
                self.decrypt_chunks(location, header, first_chunk, &encrypted.bytes().await?)?;
            let relative_start = range.start - first_chunk * header.chunk_size;
            chunks.slice(relative_start..relative_start + range.len())
        };
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
    options: PutMultipartOptions,
    parts: Arc<Mutex<Vec<Bytes>>>,
    finished: bool,
}

#[async_trait]
impl MultipartUpload for EncryptedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let parts = Arc::clone(&self.parts);
        Box::pin(async move {
            let part = collect_payload(data);
            parts
                .lock()
                .map_err(|_| encryption_error(EncryptionError::Authentication))?
                .push(part);
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.finished {
            return Err(encryption_error(EncryptionError::Length));
        }
        let parts = self
            .parts
            .lock()
            .map_err(|_| encryption_error(EncryptionError::Authentication))?
            .clone();
        self.finished = true;
        let plaintext = collect_parts(parts);
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
        self.parts
            .lock()
            .map_err(|_| encryption_error(EncryptionError::Authentication))?
            .clear();
        Ok(())
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
    let key_version = u32::from_be_bytes(data[10..14].try_into().unwrap());
    let chunk_size = u32::from_be_bytes(data[14..18].try_into().unwrap()) as usize;
    let plaintext_len = u64::from_be_bytes(data[18..26].try_into().unwrap()) as usize;
    let nonce = data[26..38].try_into().unwrap();
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
    let counter = u32::from_be_bytes(nonce[8..12].try_into().unwrap()) ^ index;
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use slatedb::object_store::{memory::InMemory, ObjectStoreExt};

    fn store(inner: Arc<dyn ObjectStore>, repository: &str) -> EncryptedObjectStore {
        EncryptedObjectStore::new(inner, repository, vec![EncryptionKey::new(1, [7; 32])], 1)
            .unwrap()
            .with_chunk_size(16)
    }

    #[tokio::test]
    async fn round_trip_and_ranges_hide_plaintext() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("wal/0001");
        let plaintext = Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz");
        encrypted
            .put(&path, plaintext.clone().into())
            .await
            .unwrap();

        let raw = inner.get(&path).await.unwrap().bytes().await.unwrap();
        assert!(!raw
            .windows(plaintext.len())
            .any(|window| window == plaintext));
        assert_eq!(
            encrypted.get(&path).await.unwrap().bytes().await.unwrap(),
            plaintext
        );
        assert_eq!(
            encrypted.get_range(&path, 14..20).await.unwrap(),
            Bytes::from_static(b"efghij")
        );
        assert_eq!(
            encrypted
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Suffix(4)))
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"wxyz")
        );
        assert_eq!(encrypted.head(&path).await.unwrap().size, 36);
    }

    #[tokio::test]
    async fn keyring_rotation_switches_writes_and_preserves_old_reads() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let old_path = Path::from("wal/old");
        let new_path = Path::from("wal/new");
        encrypted
            .put(&old_path, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        encrypted
            .install_write_key(EncryptionKey::new(2, [9; 32]))
            .unwrap();
        encrypted
            .put(&new_path, Bytes::from_static(b"new").into())
            .await
            .unwrap();
        let old_raw = inner.get(&old_path).await.unwrap().bytes().await.unwrap();
        let new_raw = inner.get(&new_path).await.unwrap().bytes().await.unwrap();
        assert_eq!(decode_header(&old_raw).unwrap().key_version, 1);
        assert_eq!(decode_header(&new_raw).unwrap().key_version, 2);
        assert_eq!(
            encrypted
                .get(&old_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "old"
        );
        assert_eq!(
            encrypted
                .get(&new_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "new"
        );
    }

    #[tokio::test]
    async fn empty_object_is_authenticated() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("manifest/empty");
        encrypted.put(&path, Bytes::new().into()).await.unwrap();
        assert!(inner.head(&path).await.unwrap().size > HEADER_SIZE as u64);
        assert!(encrypted
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .is_empty());

        let mut raw = inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        raw[18] ^= 1;
        inner.put(&path, raw.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn repeated_object_writes_use_fresh_nonces() {
        let inner = Arc::new(InMemory::new());
        let store = EncryptedObjectStore::new(
            inner.clone(),
            "repo-a",
            vec![EncryptionKey::new(1, [7; 32])],
            1,
        )
        .unwrap();
        let location = Path::from("manifest/nonce-test");
        store
            .put(&location, Bytes::from_static(b"same plaintext").into())
            .await
            .unwrap();
        let first = inner.get(&location).await.unwrap().bytes().await.unwrap();
        store
            .put(&location, Bytes::from_static(b"same plaintext").into())
            .await
            .unwrap();
        let second = inner.get(&location).await.unwrap().bytes().await.unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn tamper_and_cross_repository_copy_fail_authentication() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = store(inner.clone(), "repo-a");
        let other = store(inner.clone(), "repo-b");
        let path = Path::from("compacted/1.sst");
        source
            .put(&path, Bytes::from_static(b"secret metadata").into())
            .await
            .unwrap();
        assert!(other.get(&path).await.is_err());

        let mut raw = inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        raw[HEADER_SIZE] ^= 1;
        inner.put(&path, raw.into()).await.unwrap();
        assert!(source.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn malformed_truncated_reordered_and_relocated_objects_fail_closed() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("sst/source");
        let relocated = Path::from("sst/relocated");
        encrypted
            .put(
                &path,
                Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz").into(),
            )
            .await
            .unwrap();
        let original = inner.get(&path).await.unwrap().bytes().await.unwrap();

        inner
            .put(&relocated, original.clone().into())
            .await
            .unwrap();
        assert!(encrypted.get(&relocated).await.is_err());

        let mut corrupted = original.to_vec();
        corrupted[8] = FORMAT_VERSION + 1;
        inner.put(&path, corrupted.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());

        let mut corrupted = original.to_vec();
        corrupted[10..14].copy_from_slice(&99u32.to_be_bytes());
        inner.put(&path, corrupted.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());

        inner
            .put(&path, original.slice(..original.len() - 1).into())
            .await
            .unwrap();
        assert!(encrypted.get(&path).await.is_err());

        let mut reordered = original.to_vec();
        let encrypted_chunk_len = 16 + TAG_SIZE;
        let first = reordered[HEADER_SIZE..HEADER_SIZE + encrypted_chunk_len].to_vec();
        let second = reordered
            [HEADER_SIZE + encrypted_chunk_len..HEADER_SIZE + 2 * encrypted_chunk_len]
            .to_vec();
        reordered[HEADER_SIZE..HEADER_SIZE + encrypted_chunk_len].copy_from_slice(&second);
        reordered[HEADER_SIZE + encrypted_chunk_len..HEADER_SIZE + 2 * encrypted_chunk_len]
            .copy_from_slice(&first);
        inner.put(&path, reordered.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn ranges_multipart_conditions_copy_and_list_preserve_object_store_semantics() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("wal/multipart");
        let mut upload = encrypted.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        upload
            .put_part(Bytes::from_static(b"ghijklmnopqrstuvwxyz").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();

        assert_eq!(
            encrypted.get_range(&path, 0..16).await.unwrap(),
            "0123456789abcdef"
        );
        assert_eq!(
            encrypted.get_range(&path, 16..32).await.unwrap(),
            "ghijklmnopqrstuv"
        );
        assert_eq!(
            encrypted
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Offset(32)))
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "wxyz"
        );
        assert_eq!(
            encrypted
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Suffix(999)))
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "0123456789abcdefghijklmnopqrstuvwxyz"
        );
        assert!(encrypted
            .put_opts(
                &path,
                Bytes::from_static(b"replacement").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .is_err());

        let copy = Path::from("wal/copied");
        encrypted.copy(&path, &copy).await.unwrap();
        assert_eq!(
            encrypted.get(&copy).await.unwrap().bytes().await.unwrap(),
            "0123456789abcdefghijklmnopqrstuvwxyz"
        );
        assert_ne!(
            inner.get(&path).await.unwrap().bytes().await.unwrap(),
            inner.get(&copy).await.unwrap().bytes().await.unwrap()
        );
        let mut listed = encrypted
            .list(Some(&Path::from("wal")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        listed.sort_by(|left, right| left.location.cmp(&right.location));
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().all(|meta| meta.size == 36));
    }

    #[test]
    fn integrity_errors_are_recognized_through_object_store_source_chain() {
        let error = encryption_error(EncryptionError::Authentication);
        assert!(is_integrity_error(&error));
    }
}
