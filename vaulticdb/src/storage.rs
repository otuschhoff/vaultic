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
use futures_util::StreamExt;
use prost::Message;
use serde::{Deserialize, Serialize};
use slatedb::{
    object_store::{
        aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, local::LocalFileSystem,
        memory::InMemory, prefix::PrefixStore, ObjectStore,
    },
    Db, DbIterator, DbTransaction, ErrorKind, IsolationLevel, WriteBatch,
};
use tokio::sync::{Mutex, RwLock};
use tonic::Status;

use crate::{
    proto::{GetResponse, KeyValue, ScanResponse, WriteBatchRequest},
    replication::ReplicatedObjectStore,
};
use vaulticdb::broker::{acquire_metadata_lease, BrokerLeaseConnection};
use vaulticdb::encryption::{
    self,
    envelope::{self, EncryptionStatus, KeyManager},
};

const MAX_ACTIVE_TRANSACTIONS: usize = 1_024;
const DONE_FIELD_ENCODED_LEN: usize = 2;
const DEFAULT_TRANSACTION_IDLE_TIMEOUT_SECS: u64 = 300;
const MASTER_KEY_RECORD: &[u8] = b"meta:master-key";
const CAPSULE_MIGRATION_RECORD: &[u8] = b"meta:capsule-migration";
const CAPSULE_MIGRATION_FINALIZED_RECORD: &[u8] = b"meta:capsule-migration-finalized";
const ENCRYPTION_POLICY_RECORD: &[u8] = b"meta:encryption";
const METADATA_REBUILD_RECORD: &[u8] = b"meta:recovery-rebuild";
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
    broker_lease: Option<BrokerLeaseConnection>,
}

impl Storage {
    pub async fn open(repository_id: &str) -> Result<Self> {
        let (path, object_store) = object_store(repository_id)?;
        let recovery_initialize =
            env::var("VAULTICDB_METADATA_REBUILD_INITIALIZE").as_deref() == Ok("true");
        if recovery_initialize && env::var_os("VAULTICDB_BROKER_SOCKET").is_none() {
            bail!("metadata rebuild initialization requires a broker metadata-DEK lease");
        }
        if recovery_initialize && metadata_store_has_database_objects(object_store.as_ref()).await?
        {
            bail!("metadata rebuild initialization requires an empty candidate metadata store");
        }
        let mut broker_lease = None;
        let (object_store, encryption, key_manager) = if let Ok(socket) =
            env::var("VAULTICDB_BROKER_SOCKET")
        {
            let manifest = env::var("VAULTICDB_RELEASE_MANIFEST")
                .context("VAULTICDB_RELEASE_MANIFEST is required with VAULTICDB_BROKER_SOCKET")?;
            let ttl = env::var("VAULTICDB_BROKER_LEASE_SECONDS")
                .unwrap_or_else(|_| "3600".to_owned())
                .parse::<u64>()
                .context("invalid VAULTICDB_BROKER_LEASE_SECONDS")?;
            let (lease, dek) = acquire_metadata_lease(
                &socket,
                std::path::Path::new(&manifest),
                std::time::Duration::from_secs(ttl),
            )
            .await?;
            let configured = envelope::configure_brokered(
                repository_id,
                object_store,
                &dek,
                lease.key_version,
                lease.capsule_generation,
                recovery_initialize,
            )?;
            broker_lease = Some(lease);
            configured
        } else {
            envelope::configure(repository_id, object_store).await?
        };
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
            broker_lease,
        };
        storage.ensure_encryption_policy(repository_id).await?;
        if recovery_initialize {
            storage
                .record_metadata_rebuild_handoff(repository_id)
                .await?;
        }
        Ok(storage)
    }

    pub fn broker_lease_monitor(&self) -> Option<(tokio::sync::watch::Receiver<bool>, u64)> {
        self.broker_lease
            .as_ref()
            .map(|lease| (lease.disconnected(), lease.expires_unix_ms))
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

    async fn record_metadata_rebuild_handoff(&self, repository_id: &str) -> Result<()> {
        let lease = self
            .broker_lease
            .as_ref()
            .context("metadata rebuild handoff requires a broker lease")?;
        let value = serde_json::to_vec(&serde_json::json!({
            "format": 1,
            "repository_id": repository_id,
            "capsule_generation": lease.capsule_generation,
            "metadata_dek_version": lease.key_version,
            "broker_epoch_id": lease.epoch_id,
        }))?;
        self.db
            .put(METADATA_REBUILD_RECORD, value)
            .await?
            .await_durable()
            .await
            .context("persist metadata rebuild handoff")
    }

    pub async fn get_master_key(&self) -> Result<Option<Vec<u8>>, Status> {
        if self.broker_lease.is_some() {
            return Err(Status::failed_precondition(
                "repository master key is authoritative only in the recovery capsule",
            ));
        }
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
        if self.broker_lease.is_some() {
            return Err(Status::failed_precondition(
                "master-key-in-DB is prohibited in brokered mode",
            ));
        }
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

    pub async fn record_capsule_migration(&self, capsule_sha256: &str) -> Result<(), Status> {
        if capsule_sha256.len() != 64
            || !capsule_sha256
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
        {
            return Err(Status::invalid_argument("invalid capsule digest"));
        }
        let (pending, finalized) = self.capsule_migration_status().await?;
        if finalized.is_some() {
            return Err(Status::failed_precondition(
                "capsule migration is already finalized",
            ));
        }
        if let Some(pending) = pending {
            if pending == capsule_sha256 {
                return Ok(());
            }
            return Err(Status::already_exists(
                "a different capsule migration is already pending",
            ));
        }
        self.db
            .put(CAPSULE_MIGRATION_RECORD, capsule_sha256.as_bytes())
            .await
            .map_err(storage_error)?
            .await_durable()
            .await
            .map_err(storage_error)
    }

    pub async fn capsule_migration_status(
        &self,
    ) -> Result<(Option<String>, Option<String>), Status> {
        let pending = self
            .db
            .get(CAPSULE_MIGRATION_RECORD)
            .await
            .map_err(storage_error)?
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose()
            .map_err(|_| Status::data_loss("pending capsule migration digest is invalid"))?;
        let finalized = self
            .db
            .get(CAPSULE_MIGRATION_FINALIZED_RECORD)
            .await
            .map_err(storage_error)?
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose()
            .map_err(|_| Status::data_loss("finalized capsule migration digest is invalid"))?;
        Ok((pending, finalized))
    }

    pub async fn finalize_capsule_migration(&self, capsule_sha256: &str) -> Result<(), Status> {
        let pending = self
            .db
            .get(CAPSULE_MIGRATION_RECORD)
            .await
            .map_err(storage_error)?;
        let Some(pending) = pending else {
            let finalized = self
                .db
                .get(CAPSULE_MIGRATION_FINALIZED_RECORD)
                .await
                .map_err(storage_error)?;
            if finalized.as_deref() == Some(capsule_sha256.as_bytes()) {
                return Ok(());
            }
            return Err(Status::failed_precondition(
                "no matching prepared or finalized capsule migration",
            ));
        };
        if pending.as_ref() != capsule_sha256.as_bytes() {
            return Err(Status::failed_precondition(
                "prepared capsule digest mismatch",
            ));
        }
        let mut batch = slatedb::WriteBatch::new();
        batch.delete(MASTER_KEY_RECORD);
        batch.delete(CAPSULE_MIGRATION_RECORD);
        batch.put(
            CAPSULE_MIGRATION_FINALIZED_RECORD,
            capsule_sha256.as_bytes(),
        );
        self.db.write(batch).await.map_err(storage_error)?;
        self.db.flush().await.map_err(storage_error)
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

async fn metadata_store_has_database_objects(store: &dyn ObjectStore) -> Result<bool> {
    let mut objects = store.list(None);
    while let Some(object) = objects.next().await {
        let object = object.context("inspect candidate metadata store")?;
        if !object.location.as_ref().starts_with("_vaultic/") {
            return Ok(true);
        }
    }
    Ok(false)
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
    if encryption::is_integrity_error(&error) {
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

pub(crate) fn object_store(repository_id: &str) -> Result<(String, Arc<dyn ObjectStore>)> {
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
        "replicated" => replicated_object_store(&repository_key),
        value => {
            bail!("unsupported VAULTICDB_OBJECT_STORE {value:?}; expected local, memory, s3, or replicated")
        }
    }
}

fn replicated_object_store(repository_key: &str) -> Result<(String, Arc<dyn ObjectStore>)> {
    let replicas = env::var("VAULTICDB_REPLICATED_REPLICAS")
        .context("VAULTICDB_REPLICATED_REPLICAS is required for replicated storage")?;
    let mut stores = Vec::new();
    for raw_id in replicas.split(',') {
        let id = raw_id.trim();
        if id.is_empty() {
            bail!("VAULTICDB_REPLICATED_REPLICAS contains an empty replica ID");
        }
        stores.push((id.to_owned(), replicated_replica_store(id, repository_key)?));
    }
    Ok((
        "db".to_owned(),
        Arc::new(ReplicatedObjectStore::new(stores)?),
    ))
}

fn replicated_replica_store(id: &str, repository_key: &str) -> Result<Arc<dyn ObjectStore>> {
    let prefix = format!("VAULTICDB_REPLICATED_{}", env_id(id));
    match env::var(format!("{prefix}_OBJECT_STORE"))
        .with_context(|| format!("{prefix}_OBJECT_STORE is required"))?
        .as_str()
    {
        "local" => {
            let root = env::var(format!("{prefix}_DATA_DIR"))
                .map(PathBuf::from)
                .with_context(|| format!("{prefix}_DATA_DIR is required for local replica {id}"))?
                .join(repository_key);
            std::fs::create_dir_all(&root).with_context(|| {
                format!(
                    "create replicated SlateDB data directory {}",
                    root.display()
                )
            })?;
            Ok(Arc::new(
                LocalFileSystem::new_with_prefix(&root).with_context(|| {
                    format!("open replicated SlateDB data directory {}", root.display())
                })?,
            ))
        }
        "memory" => Ok(Arc::new(InMemory::new())),
        "s3" => {
            let bucket = env::var(format!("{prefix}_S3_BUCKET"))
                .with_context(|| format!("{prefix}_S3_BUCKET is required for S3 replica {id}"))?;
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .with_context(|| format!("configure S3-compatible object store replica {id}"))?;
            let path = match env::var(format!("{prefix}_S3_PREFIX")) {
                Ok(value) if !value.trim_matches('/').is_empty() => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                Ok(_) => bail!("{prefix}_S3_PREFIX must not be empty"),
                Err(env::VarError::NotPresent) => repository_key.to_owned(),
                Err(error) => return Err(error.into()),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
        "azure" => {
            let account = env::var(format!("{prefix}_AZURE_ACCOUNT")).with_context(|| {
                format!("{prefix}_AZURE_ACCOUNT is required for Azure replica {id}")
            })?;
            let container = env::var(format!("{prefix}_AZURE_CONTAINER")).with_context(|| {
                format!("{prefix}_AZURE_CONTAINER is required for Azure replica {id}")
            })?;
            let mut builder = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container);
            if let Ok(access_key) = env::var(format!("{prefix}_AZURE_ACCESS_KEY")) {
                builder = builder.with_access_key(access_key);
            }
            if let Ok(token) = env::var(format!("{prefix}_AZURE_BEARER_TOKEN")) {
                builder = builder.with_bearer_token_authorization(token);
            }
            let store = builder
                .build()
                .with_context(|| format!("configure Azure Blob object store replica {id}"))?;
            let path = match env::var(format!("{prefix}_AZURE_PREFIX")) {
                Ok(value) if !value.trim_matches('/').is_empty() => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                Ok(_) => bail!("{prefix}_AZURE_PREFIX must not be empty"),
                Err(env::VarError::NotPresent) => repository_key.to_owned(),
                Err(error) => return Err(error.into()),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
        value => {
            bail!(
                "unsupported {prefix}_OBJECT_STORE {value:?}; expected local, memory, s3, or azure"
            )
        }
    }
}

fn env_id(id: &str) -> String {
    id.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::{path::Path, ObjectStoreExt};

    #[tokio::test]
    async fn s3_metadata_rebuild_destroys_rebuilds_and_reopens_encrypted_candidate() {
        if env::var_os("VAULTICDB_TEST_S3_ENDPOINT").is_none() {
            return;
        }
        let bucket =
            env::var("VAULTICDB_TEST_S3_BUCKET").unwrap_or_else(|_| "vaulticdb-ci".to_owned());
        let prefix = format!(
            "phase20/rebuild-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let raw: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .unwrap(),
            prefix,
        ));

        let stale = Path::from("manifest/stale");
        raw.put(&stale, b"stale-authoritative-metadata".to_vec().into())
            .await
            .unwrap();
        let stale_objects = raw
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(stale_objects.len(), 1);
        for object in stale_objects {
            raw.delete(&object.location).await.unwrap();
        }
        assert!(!metadata_store_has_database_objects(raw.as_ref())
            .await
            .unwrap());

        let repository_id = "phase20-s3-rebuild";
        let dek = [0x5a; 32];
        let plaintext = b"phase20-known-plaintext-metadata";
        let (encrypted, _, _) =
            envelope::configure_brokered(repository_id, raw.clone(), &dek, 3, 9, true).unwrap();
        let db = Db::open("db", encrypted).await.unwrap();
        db.put(b"p:rebuilt-pack", plaintext.to_vec())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.put(
            METADATA_REBUILD_RECORD,
            serde_json::to_vec(&serde_json::json!({
                "format": 1,
                "repository_id": repository_id,
                "capsule_generation": 9,
                "metadata_dek_version": 3,
                "broker_epoch_id": "test-epoch",
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        db.close().await.unwrap();

        let objects = raw
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(!objects.is_empty());
        for object in &objects {
            let bytes = raw
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert!(!bytes
                .windows(plaintext.len())
                .any(|value| value == plaintext));
            assert!(!bytes.windows(dek.len()).any(|value| value == dek));
        }

        let (encrypted, _, _) =
            envelope::configure_brokered(repository_id, raw.clone(), &dek, 3, 9, false).unwrap();
        let reopened = Db::open("db", encrypted).await.unwrap();
        assert_eq!(
            reopened.get(b"p:rebuilt-pack").await.unwrap().unwrap(),
            plaintext.as_slice()
        );
        assert!(reopened
            .get(METADATA_REBUILD_RECORD)
            .await
            .unwrap()
            .is_some());
        reopened.close().await.unwrap();

        for object in objects {
            raw.delete(&object.location).await.unwrap();
        }
    }

    #[tokio::test]
    async fn metadata_rebuild_candidate_ignores_capsules_but_rejects_database_objects() {
        let store = InMemory::new();
        store
            .put(
                &slatedb::object_store::path::Path::from("_vaultic/recovery-capsules/one.json"),
                vec![1_u8].into(),
            )
            .await
            .unwrap();
        assert!(!metadata_store_has_database_objects(&store).await.unwrap());
        store
            .put(
                &slatedb::object_store::path::Path::from("manifest/0001"),
                vec![2_u8].into(),
            )
            .await
            .unwrap();
        assert!(metadata_store_has_database_objects(&store).await.unwrap());
    }
    use slatedb::object_store::memory::InMemory;

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

    #[tokio::test]
    async fn capsule_migration_keeps_master_key_until_matching_finalize() {
        let db = Db::open("migration-test", Arc::new(InMemory::new()))
            .await
            .unwrap();
        let storage = Storage {
            db,
            encryption: EncryptionStatus {
                enabled: true,
                algorithm: "AES-256-GCM",
                active_dek_version: 1,
                envelope_generation: 1,
                unlock_slot: Some("test".to_owned()),
                recovery_unlock: false,
                initializing: false,
            },
            key_manager: None,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            transaction_idle_timeout_ms: 1_000,
            broker_lease: None,
        };
        storage.store_master_key(b"repository-key").await.unwrap();
        let digest = "ab".repeat(32);
        storage.record_capsule_migration(&digest).await.unwrap();
        storage.record_capsule_migration(&digest).await.unwrap();
        assert!(storage
            .record_capsule_migration(&"cd".repeat(32))
            .await
            .is_err());
        assert_eq!(
            storage.capsule_migration_status().await.unwrap(),
            (Some(digest.clone()), None)
        );
        assert!(storage
            .finalize_capsule_migration(&"cd".repeat(32))
            .await
            .is_err());
        assert_eq!(
            storage.get_master_key().await.unwrap().unwrap(),
            b"repository-key"
        );
        storage.finalize_capsule_migration(&digest).await.unwrap();
        storage.finalize_capsule_migration(&digest).await.unwrap();
        assert!(storage
            .finalize_capsule_migration(&"cd".repeat(32))
            .await
            .is_err());
        assert!(storage.get_master_key().await.unwrap().is_none());
        assert_eq!(
            storage.capsule_migration_status().await.unwrap(),
            (None, Some(digest))
        );
        storage.close().await.unwrap();
    }
}
