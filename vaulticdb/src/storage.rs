use std::{
    collections::HashMap,
    env,
    ops::Bound::{Excluded, Unbounded},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use prost::Message;
use serde::{Deserialize, Serialize};
use slatedb::{
    object_store::{aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory, ObjectStore},
    Db, DbIterator, DbTransaction, ErrorKind, IsolationLevel, WriteBatch,
};
use tokio::sync::{Mutex, RwLock};
use tonic::Status;

use crate::{
    encryption::envelope::{self, EncryptionStatus, KeyManager},
    proto::{GetResponse, KeyValue, ScanResponse, WriteBatchRequest},
};

const MAX_ACTIVE_TRANSACTIONS: usize = 1_024;
const DONE_FIELD_ENCODED_LEN: usize = 2;
const DEFAULT_TRANSACTION_IDLE_TIMEOUT_SECS: u64 = 300;
const MASTER_KEY_RECORD: &[u8] = b"meta:master-key";
const ENCRYPTION_POLICY_RECORD: &[u8] = b"meta:encryption";
const MAX_MASTER_KEY_BYTES: usize = 4096;

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
    repository_id: String,
}

pub struct Storage {
    db: Db,
    encryption: EncryptionStatus,
    key_manager: Option<Arc<KeyManager>>,
    transactions: RwLock<HashMap<String, Arc<TransactionSlot>>>,
    next_transaction: AtomicU64,
    transaction_idle_timeout_ms: u64,
}

impl Storage {
    pub async fn open(repository_id: &str) -> Result<Self> {
        let (path, object_store) = object_store(repository_id)?;
        let (object_store, encryption, key_manager) =
            envelope::configure(repository_id, object_store).await?;
        let db = Db::open(path, object_store)
            .await
            .context("open SlateDB database")?;
        let storage = Self {
            db,
            encryption,
            key_manager,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            transaction_idle_timeout_ms: transaction_idle_timeout_ms()?,
        };
        storage.ensure_encryption_policy(repository_id).await?;
        Ok(storage)
    }

    pub fn encryption_status(&self) -> &EncryptionStatus {
        &self.encryption
    }

    pub fn key_manager(&self) -> Result<&Arc<KeyManager>, Status> {
        self.key_manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("key management requires metadata encryption")
        })
    }

    async fn ensure_encryption_policy(&self, repository_id: &str) -> Result<()> {
        let existing = self
            .db
            .get(ENCRYPTION_POLICY_RECORD)
            .await
            .context("read metadata encryption policy")?;
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
            repository_id: repository_id.to_owned(),
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
        if !self.encryption.initializing {
            bail!("metadata encryption policy is missing while encryption is required");
        }
        self.db
            .put(
                ENCRYPTION_POLICY_RECORD,
                serde_json::to_vec(&expected).context("encode metadata encryption policy")?,
            )
            .await
            .context("write metadata encryption policy")?
            .await_durable()
            .await
            .context("persist metadata encryption policy")
    }

    pub async fn get_master_key(&self) -> Result<Option<Vec<u8>>, Status> {
        if !self.encryption.enabled {
            return Err(Status::failed_precondition(
                "master-key-in-DB requires metadata encryption",
            ));
        }
        self.db
            .get(MASTER_KEY_RECORD)
            .await
            .map_err(storage_error)
            .map(|value| value.map(|bytes| bytes.to_vec()))
    }

    pub async fn store_master_key(&self, master_key: &[u8]) -> Result<(), Status> {
        if !self.encryption.enabled {
            return Err(Status::failed_precondition(
                "master-key-in-DB requires metadata encryption",
            ));
        }
        if master_key.is_empty() || master_key.len() > MAX_MASTER_KEY_BYTES {
            return Err(Status::invalid_argument("invalid repository master key"));
        }
        if let Some(existing) = self
            .db
            .get(MASTER_KEY_RECORD)
            .await
            .map_err(storage_error)?
        {
            if existing.as_ref() == master_key {
                return Ok(());
            }
            return Err(Status::already_exists(
                "a different repository master key is already stored",
            ));
        }
        self.db
            .put(MASTER_KEY_RECORD, master_key)
            .await
            .map_err(storage_error)?
            .await_durable()
            .await
            .map_err(storage_error)
    }

    pub async fn close(&self) -> Result<()> {
        self.transactions.write().await.clear();
        self.db.close().await.context("close SlateDB database")
    }

    pub async fn get(&self, key: &[u8], transaction_id: &str) -> Result<GetResponse, Status> {
        validate_key(key)?;
        let value = if transaction_id.is_empty() {
            self.db.get(key).await.map_err(storage_error)?
        } else {
            let transaction = self.transaction(transaction_id).await?;
            let transaction = transaction.transaction.lock().await;
            transaction
                .as_ref()
                .ok_or_else(|| Status::not_found("transaction was closed"))?
                .get(key)
                .await
                .map_err(storage_error)?
        };
        Ok(match value {
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
        })
    }

    pub async fn scan(
        &self,
        prefix: &[u8],
        after_key: &[u8],
        page_size: usize,
        transaction_id: &str,
    ) -> Result<ScanResponse, Status> {
        if !after_key.is_empty() && !after_key.starts_with(prefix) {
            return Err(Status::invalid_argument(
                "scan cursor is outside the prefix",
            ));
        }
        let suffix = after_key.strip_prefix(prefix).unwrap_or_default();
        let mut iterator = if transaction_id.is_empty() {
            scan_prefix_db(&self.db, prefix, suffix).await?
        } else {
            let transaction = self.transaction(transaction_id).await?;
            let transaction = transaction.transaction.lock().await;
            scan_prefix_transaction(
                transaction
                    .as_ref()
                    .ok_or_else(|| Status::not_found("transaction was closed"))?,
                prefix,
                suffix,
            )
            .await?
        };
        collect_page(&mut iterator, page_size).await
    }

    pub async fn write_batch(&self, request: &WriteBatchRequest) -> Result<bool, Status> {
        validate_mutations(request)?;
        if request.transaction_id.is_empty() {
            let mut batch = WriteBatch::new();
            for put in &request.puts {
                batch.put(&put.key, &put.value);
            }
            for key in &request.deletes {
                batch.delete(key);
            }
            let handle = self.db.write(batch).await.map_err(storage_error)?;
            if request.await_durable {
                handle.await_durable().await.map_err(storage_error)?;
            }
            return Ok(request.await_durable);
        }

        if request.await_durable {
            return Err(Status::invalid_argument(
                "transaction mutations become durable only at commit",
            ));
        }
        let transaction = self.transaction(&request.transaction_id).await?;
        let transaction = transaction.transaction.lock().await;
        let transaction = transaction
            .as_ref()
            .ok_or_else(|| Status::not_found("transaction was closed"))?;
        for put in &request.puts {
            transaction
                .put(&put.key, &put.value)
                .map_err(storage_error)?;
        }
        for key in &request.deletes {
            transaction.delete(key).map_err(storage_error)?;
        }
        Ok(false)
    }

    pub async fn begin(&self) -> Result<String, Status> {
        let mut transactions = self.transactions.write().await;
        let now = unix_time_ms().map_err(storage_status)?;
        transactions.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || !transaction_expired(
                    slot.last_touched_ms.load(Ordering::Relaxed),
                    now,
                    self.transaction_idle_timeout_ms,
                )
        });
        if transactions.len() >= MAX_ACTIVE_TRANSACTIONS {
            return Err(Status::resource_exhausted(
                "active transaction limit exceeded",
            ));
        }
        let transaction = self
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .map_err(storage_error)?;
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
        Ok(id)
    }

    pub async fn commit(&self, transaction_id: &str) -> Result<(), Status> {
        let transaction = self.remove_transaction(transaction_id).await?;
        let transaction = transaction
            .transaction
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::not_found("transaction was closed"))?;
        if let Some(handle) = transaction.commit().await.map_err(storage_error)? {
            handle.await_durable().await.map_err(storage_error)?;
        }
        Ok(())
    }

    pub async fn rollback(&self, transaction_id: &str) -> Result<(), Status> {
        let transaction = self.remove_transaction(transaction_id).await?;
        let transaction = transaction
            .transaction
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::not_found("transaction was closed"))?;
        transaction.rollback();
        Ok(())
    }

    async fn transaction(&self, transaction_id: &str) -> Result<Arc<TransactionSlot>, Status> {
        if transaction_id.is_empty() {
            return Err(Status::invalid_argument("transaction ID is required"));
        }
        let transaction = self
            .transactions
            .read()
            .await
            .get(transaction_id)
            .cloned()
            .ok_or_else(|| Status::not_found("transaction was not found"))?;
        transaction
            .last_touched_ms
            .store(unix_time_ms().map_err(storage_status)?, Ordering::Relaxed);
        Ok(transaction)
    }

    async fn remove_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Arc<TransactionSlot>, Status> {
        if transaction_id.is_empty() {
            return Err(Status::invalid_argument("transaction ID is required"));
        }
        self.transactions
            .write()
            .await
            .remove(transaction_id)
            .ok_or_else(|| Status::not_found("transaction was not found"))
    }
}

fn transaction_idle_timeout_ms() -> Result<u64> {
    let seconds = match env::var("VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS") {
        Ok(value) => value
            .parse::<u64>()
            .context("parse VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS")?,
        Err(env::VarError::NotPresent) => DEFAULT_TRANSACTION_IDLE_TIMEOUT_SECS,
        Err(error) => return Err(error.into()),
    };
    if seconds < 10 {
        bail!("VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS must be at least 10")
    }
    seconds
        .checked_mul(1_000)
        .context("VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS is too large")
}

fn unix_time_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("system time exceeds u64 milliseconds")
}

fn storage_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

fn transaction_expired(last_touched_ms: u64, now_ms: u64, timeout_ms: u64) -> bool {
    now_ms.saturating_sub(last_touched_ms) >= timeout_ms
}

async fn scan_prefix_db(db: &Db, prefix: &[u8], suffix: &[u8]) -> Result<DbIterator, Status> {
    if suffix.is_empty() {
        db.scan_prefix(prefix, ..).await.map_err(storage_error)
    } else {
        db.scan_prefix(prefix, (Excluded(suffix), Unbounded))
            .await
            .map_err(storage_error)
    }
}

async fn scan_prefix_transaction(
    transaction: &DbTransaction,
    prefix: &[u8],
    suffix: &[u8],
) -> Result<DbIterator, Status> {
    if suffix.is_empty() {
        transaction
            .scan_prefix(prefix, ..)
            .await
            .map_err(storage_error)
    } else {
        transaction
            .scan_prefix(prefix, (Excluded(suffix), Unbounded))
            .await
            .map_err(storage_error)
    }
}

async fn collect_page(iterator: &mut DbIterator, page_size: usize) -> Result<ScanResponse, Status> {
    let mut entries = Vec::with_capacity(page_size);
    let mut response_bytes = 0usize;
    while entries.len() < page_size {
        let Some(item) = iterator.next().await.map_err(storage_error)? else {
            return Ok(ScanResponse {
                entries,
                done: true,
            });
        };
        let entry = KeyValue {
            key: item.key.to_vec(),
            value: item.value.to_vec(),
        };
        let next_size = response_bytes
            .checked_add(repeated_message_encoded_len(entry.encoded_len()))
            .ok_or_else(|| Status::resource_exhausted("scan response size overflow"))?;
        if next_size > crate::MAX_MESSAGE_BYTES as usize - DONE_FIELD_ENCODED_LEN {
            if entries.is_empty() {
                return Err(Status::resource_exhausted(
                    "scan entry exceeds response byte limit",
                ));
            }
            return Ok(ScanResponse {
                entries,
                done: false,
            });
        }
        response_bytes = next_size;
        entries.push(entry);
    }
    let done = iterator.next().await.map_err(storage_error)?.is_none();
    Ok(ScanResponse { entries, done })
}

pub(crate) fn repeated_message_encoded_len(message_len: usize) -> usize {
    1 + encoded_varint_len(message_len as u64) + message_len
}

fn encoded_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn validate_key(key: &[u8]) -> Result<(), Status> {
    if key.is_empty() {
        return Err(Status::invalid_argument("key must not be empty"));
    }
    Ok(())
}

fn validate_mutations(request: &WriteBatchRequest) -> Result<(), Status> {
    for put in &request.puts {
        validate_key(&put.key)?;
    }
    for key in &request.deletes {
        validate_key(key)?;
    }
    Ok(())
}

fn storage_error(error: slatedb::Error) -> Status {
    let message = format!("SlateDB operation failed: {error}");
    if crate::encryption::is_integrity_error(&error) {
        return Status::data_loss(message);
    }
    match error.kind() {
        ErrorKind::Transaction => Status::aborted(message),
        ErrorKind::Unavailable | ErrorKind::Closed(_) => Status::unavailable(message),
        ErrorKind::Invalid => Status::invalid_argument(message),
        ErrorKind::Data => Status::data_loss(message),
        _ => Status::internal(message),
    }
}

fn object_store(repository_id: &str) -> Result<(String, Arc<dyn ObjectStore>)> {
    let repository_key = crate::repository_key(repository_id);
    match env::var("VAULTICDB_OBJECT_STORE")
        .unwrap_or_else(|_| "local".to_owned())
        .as_str()
    {
        "local" => {
            let root = env::var("VAULTICDB_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| env::temp_dir().join("vaulticdb").join("data"))
                .join(&repository_key);
            std::fs::create_dir_all(&root)
                .with_context(|| format!("create SlateDB data directory {}", root.display()))?;
            let store = LocalFileSystem::new_with_prefix(&root)
                .with_context(|| format!("open SlateDB data directory {}", root.display()))?;
            Ok(("db".to_owned(), Arc::new(store)))
        }
        "memory" => Ok((repository_key, Arc::new(InMemory::new()))),
        "s3" => {
            let bucket = env::var("VAULTICDB_S3_BUCKET")
                .context("VAULTICDB_S3_BUCKET is required for S3 storage")?;
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .context("configure S3-compatible object store")?;
            let path = match env::var("VAULTICDB_S3_PREFIX") {
                Ok(prefix) if !prefix.trim_matches('/').is_empty() => {
                    format!("{}/{repository_key}", prefix.trim_matches('/'))
                }
                Ok(_) => bail!("VAULTICDB_S3_PREFIX must not be empty"),
                Err(env::VarError::NotPresent) => repository_key,
                Err(error) => return Err(error.into()),
            };
            Ok((path, Arc::new(store)))
        }
        value => {
            bail!("unsupported VAULTICDB_OBJECT_STORE {value:?}; expected local, memory, or s3")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{repeated_message_encoded_len, transaction_expired};

    #[test]
    fn repeated_message_size_includes_tag_and_varint_length() {
        assert_eq!(repeated_message_encoded_len(0), 2);
        assert_eq!(repeated_message_encoded_len(127), 129);
        assert_eq!(repeated_message_encoded_len(128), 131);
        assert_eq!(
            repeated_message_encoded_len(16 * 1024 * 1024),
            16 * 1024 * 1024 + 5
        );
    }

    #[test]
    fn transaction_expiry_is_idle_and_clock_safe() {
        assert!(!transaction_expired(1_000, 1_999, 1_000));
        assert!(transaction_expired(1_000, 2_000, 1_000));
        assert!(!transaction_expired(2_000, 1_000, 1_000));
    }
}
