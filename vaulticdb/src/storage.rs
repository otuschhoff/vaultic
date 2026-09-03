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
use sha2::{Digest, Sha256};
use slatedb::{
    config::DbReaderOptions,
    object_store::{
        aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, local::LocalFileSystem,
        memory::InMemory, path::Path as ObjectPath, prefix::PrefixStore, ObjectStore,
        ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
    },
    Db, DbIterator, DbReader, DbReaderMode, DbTransaction, ErrorKind, IsolationLevel, WriteBatch,
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
const IDEMPOTENCY_PREFIX: &[u8] = b"meta:idempotency:";
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const WRITER_EPOCH_PREFIX: &str = "_vaultic/writer-epochs";
const ACTIVE_WRITER_PATH: &str = "_vaultic/active-writer";

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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdempotencyRecord {
    format: u32,
    operation: String,
    request_sha256: String,
    durable: bool,
}

pub struct Storage {
    database: RwLock<Database>,
    database_path: String,
    object_store: Arc<dyn ObjectStore>,
    coordination_store: Arc<dyn ObjectStore>,
    encryption: EncryptionStatus,
    key_manager: Option<Arc<KeyManager>>,
    transactions: RwLock<HashMap<String, Arc<TransactionSlot>>>,
    next_transaction: AtomicU64,
    last_durable_sequence: AtomicU64,
    transaction_idle_timeout_ms: u64,
    broker_lease: Option<BrokerLeaseConnection>,
    writer_epoch: AtomicU64,
}

enum Database {
    Writer(Db),
    Reader(DbReader),
    Unavailable,
}

impl Database {
    fn as_writer(&self) -> Option<&Db> {
        match self {
            Self::Writer(db) => Some(db),
            Self::Reader(_) | Self::Unavailable => None,
        }
    }
}

impl Storage {
    pub async fn open(repository_id: &str) -> Result<Self> {
        let (path, object_store) = object_store(repository_id)?;
        let coordination_store =
            if env::var("VAULTICDB_OBJECT_STORE").as_deref() == Ok("replicated") {
                let replica = env::var("VAULTICDB_FENCING_REPLICA")
                    .context("replicated metadata requires VAULTICDB_FENCING_REPLICA")?;
                replicated_replica_store(&replica, &crate::repository_key(repository_id))?
            } else {
                object_store.clone()
            };
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
        let (database, writer_epoch) =
            match claim_writer_epoch(coordination_store.as_ref(), None).await? {
                Some(epoch) => {
                    let db = match Db::open(path.clone(), object_store.clone()).await {
                        Ok(db) => db,
                        Err(error) => {
                            release_writer_claim(coordination_store.as_ref(), epoch)
                                .await
                                .context("release writer claim after database open failure")?;
                            return Err(error).context("open SlateDB database");
                        }
                    };
                    (Database::Writer(db), epoch)
                }
                None => (
                    Database::Reader(
                        DbReader::open(
                            path.as_str(),
                            object_store.clone(),
                            DbReaderMode::FollowLatest,
                            DbReaderOptions {
                                skip_wal_replay: false,
                                ..Default::default()
                            },
                        )
                        .await
                        .context("open SlateDB database as non-fencing reader")?,
                    ),
                    latest_writer_epoch(coordination_store.as_ref()).await?,
                ),
            };
        let storage = Self {
            database: RwLock::new(database),
            database_path: path,
            object_store,
            coordination_store,
            encryption,
            key_manager,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: transaction_idle_timeout_ms()?,
            broker_lease,
            writer_epoch: AtomicU64::new(writer_epoch),
        };
        let initialize = async {
            storage.ensure_encryption_policy(repository_id).await?;
            if recovery_initialize {
                storage
                    .record_metadata_rebuild_handoff(repository_id)
                    .await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = initialize {
            storage
                .close()
                .await
                .context("close storage after initialization failure")?;
            return Err(error).context("initialize VaulticDB storage");
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

    pub async fn writer_status_epoch(&self) -> (bool, u64) {
        (
            matches!(&*self.database.read().await, Database::Writer(_)),
            self.writer_epoch.load(Ordering::Acquire),
        )
    }

    pub fn last_durable_sequence(&self) -> u64 {
        self.last_durable_sequence.load(Ordering::Acquire)
    }

    pub fn key_manager(&self) -> Result<&Arc<KeyManager>, Status> {
        self.key_manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("key management requires metadata encryption")
        })
    }

    async fn ensure_encryption_policy(&self, repository_id: &str) -> Result<()> {
        let database = self.database.read().await;
        let Database::Writer(db) = &*database else {
            bail!("metadata encryption policy requires a writer")
        };
        let existing = self
            .writer()
            .await?
            .as_writer()
            .expect("writer was validated")
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
        db.put(
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
        let database = self.database.read().await;
        let Database::Writer(db) = &*database else {
            bail!("metadata rebuild handoff requires a writer")
        };
        db.put(METADATA_REBUILD_RECORD, value)
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
        self.read_value(MASTER_KEY_RECORD)
            .await
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
        if let Some(existing) = self.read_value(MASTER_KEY_RECORD).await? {
            if existing.as_ref() == master_key {
                return Ok(());
            }
            return Err(Status::already_exists(
                "a different repository master key is already stored",
            ));
        }
        let database = self.writer().await?;
        let Database::Writer(db) = &*database else {
            unreachable!()
        };
        db.put(MASTER_KEY_RECORD, master_key)
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
        let database = self.writer().await?;
        let Database::Writer(db) = &*database else {
            unreachable!()
        };
        db.put(CAPSULE_MIGRATION_RECORD, capsule_sha256.as_bytes())
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
            .read_value(CAPSULE_MIGRATION_RECORD)
            .await?
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose()
            .map_err(|_| Status::data_loss("pending capsule migration digest is invalid"))?;
        let finalized = self
            .read_value(CAPSULE_MIGRATION_FINALIZED_RECORD)
            .await?
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose()
            .map_err(|_| Status::data_loss("finalized capsule migration digest is invalid"))?;
        Ok((pending, finalized))
    }

    pub async fn finalize_capsule_migration(&self, capsule_sha256: &str) -> Result<(), Status> {
        let pending = self.read_value(CAPSULE_MIGRATION_RECORD).await?;
        let Some(pending) = pending else {
            let finalized = self.read_value(CAPSULE_MIGRATION_FINALIZED_RECORD).await?;
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
        let database = self.writer().await?;
        let Database::Writer(db) = &*database else {
            unreachable!()
        };
        db.write(batch).await.map_err(storage_error)?;
        db.flush().await.map_err(storage_error)
    }

    pub async fn close(&self) -> Result<()> {
        self.transactions.write().await.clear();
        let database = self.database.write().await;
        let was_writer = matches!(&*database, Database::Writer(_));
        match &*database {
            Database::Writer(db) => db.close().await.context("close SlateDB writer"),
            Database::Reader(reader) => reader.close().await.context("close SlateDB reader"),
            Database::Unavailable => Ok(()),
        }?;
        if was_writer {
            release_writer_claim(
                self.coordination_store.as_ref(),
                self.writer_epoch.load(Ordering::Acquire),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn demote(&self) -> Result<()> {
        if !self.transactions.read().await.is_empty() {
            bail!("active transactions prevent writer demotion")
        }
        let mut database = self.database.write().await;
        if !matches!(&*database, Database::Writer(_)) {
            bail!("VaulticDB is not the metadata writer")
        }
        let Database::Writer(db) = std::mem::replace(&mut *database, Database::Unavailable) else {
            unreachable!()
        };
        db.flush()
            .await
            .context("flush SlateDB writer before demotion")?;
        self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
        db.close()
            .await
            .context("close SlateDB writer before demotion")?;
        let reader = DbReader::open(
            self.database_path.as_str(),
            self.object_store.clone(),
            DbReaderMode::FollowLatest,
            DbReaderOptions {
                skip_wal_replay: false,
                ..Default::default()
            },
        )
        .await
        .context("open non-fencing SlateDB reader")?;
        *database = Database::Reader(reader);
        release_writer_claim(
            self.coordination_store.as_ref(),
            self.writer_epoch.load(Ordering::Acquire),
        )
        .await?;
        Ok(())
    }

    pub async fn promote(&self, takeover_epoch: Option<u64>) -> Result<u64> {
        let epoch = claim_writer_epoch(self.coordination_store.as_ref(), takeover_epoch)
            .await?
            .context("another VaulticDB instance acquired the next writer epoch")?;
        let mut database = self.database.write().await;
        if !matches!(&*database, Database::Reader(_)) {
            bail!("VaulticDB is not read-only")
        }
        let Database::Reader(reader) = std::mem::replace(&mut *database, Database::Unavailable)
        else {
            unreachable!()
        };
        reader
            .close()
            .await
            .context("close SlateDB reader before promotion")?;
        let db = Db::open(self.database_path.as_str(), self.object_store.clone())
            .await
            .context("open freshly fenced SlateDB writer")?;
        *database = Database::Writer(db);
        self.writer_epoch.store(epoch, Ordering::Release);
        Ok(epoch)
    }

    pub async fn active_transactions(&self) -> usize {
        self.transactions.read().await.len()
    }

    async fn writer(&self) -> Result<tokio::sync::RwLockReadGuard<'_, Database>, Status> {
        let database = self.database.read().await;
        if !matches!(&*database, Database::Writer(_)) {
            return Err(Status::failed_precondition(
                "vaulticdb is not the metadata writer",
            ));
        }
        Ok(database)
    }

    async fn read_value(&self, key: &[u8]) -> Result<Option<bytes::Bytes>, Status> {
        let database = self.database.read().await;
        match &*database {
            Database::Writer(db) => db.get(key).await.map_err(storage_error),
            Database::Reader(reader) => reader.get(key).await.map_err(storage_error),
            Database::Unavailable => Err(Status::unavailable("vaulticdb storage is transitioning")),
        }
    }

    pub async fn get(&self, key: &[u8], transaction_id: &str) -> Result<GetResponse, Status> {
        validate_key(key)?;
        let value = if transaction_id.is_empty() {
            self.read_value(key).await?
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
        let database = self.database.read().await;
        let mut iterator = if transaction_id.is_empty() {
            match &*database {
                Database::Writer(db) => scan_prefix_db(db, prefix, suffix).await?,
                Database::Reader(reader) => scan_prefix_reader(reader, prefix, suffix).await?,
                Database::Unavailable => {
                    return Err(Status::unavailable("vaulticdb storage is transitioning"));
                }
            }
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
        self.assert_current_writer_epoch().await?;
        if request.transaction_id.is_empty() {
            let request_digest = write_batch_digest(request);
            let idempotency_key = idempotency_record_key(&request.idempotency_key)?;
            if let Some(key) = idempotency_key.as_ref() {
                if let Some(existing) = self.read_idempotency(key).await? {
                    if existing.operation != "write-batch"
                        || existing.request_sha256 != request_digest
                    {
                        return Err(Status::already_exists(
                            "idempotency key is bound to a different operation",
                        ));
                    }
                    return Ok(existing.durable);
                }
            }
            let mut batch = WriteBatch::new();
            for put in &request.puts {
                batch.put(&put.key, &put.value);
            }
            for key in &request.deletes {
                batch.delete(key);
            }
            if let Some(key) = idempotency_key {
                let record = IdempotencyRecord {
                    format: 1,
                    operation: "write-batch".to_owned(),
                    request_sha256: request_digest,
                    durable: true,
                };
                batch.put(
                    key,
                    serde_json::to_vec(&record).map_err(|error| {
                        Status::internal(format!("encode idempotency record: {error}"))
                    })?,
                );
            }
            let database = self.writer().await?;
            let Database::Writer(db) = &*database else {
                unreachable!()
            };
            let handle = db.write(batch).await.map_err(storage_error)?;
            if request.await_durable || !request.idempotency_key.is_empty() {
                handle.await_durable().await.map_err(storage_error)?;
                self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
            }
            return Ok(request.await_durable || !request.idempotency_key.is_empty());
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
        self.assert_current_writer_epoch().await?;
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
            .writer()
            .await?
            .as_writer()
            .expect("writer was validated")
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

    pub async fn commit(&self, transaction_id: &str, idempotency_key: &str) -> Result<(), Status> {
        self.assert_current_writer_epoch().await?;
        let request_digest = transaction_digest(transaction_id);
        let record_key = idempotency_record_key(idempotency_key)?;
        if let Some(key) = record_key.as_ref() {
            if let Some(existing) = self.read_idempotency(key).await? {
                if existing.operation != "transaction-commit"
                    || existing.request_sha256 != request_digest
                {
                    return Err(Status::already_exists(
                        "idempotency key is bound to a different operation",
                    ));
                }
                return Ok(());
            }
        }
        let transaction = self.remove_transaction(transaction_id).await?;
        let transaction = transaction
            .transaction
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::not_found("transaction was closed"))?;
        if let Some(key) = record_key {
            let record = IdempotencyRecord {
                format: 1,
                operation: "transaction-commit".to_owned(),
                request_sha256: request_digest,
                durable: true,
            };
            transaction
                .put(
                    key,
                    serde_json::to_vec(&record).map_err(|error| {
                        Status::internal(format!("encode idempotency record: {error}"))
                    })?,
                )
                .map_err(storage_error)?;
        }
        if let Some(handle) = transaction.commit().await.map_err(storage_error)? {
            handle.await_durable().await.map_err(storage_error)?;
        }
        self.last_durable_sequence.fetch_add(1, Ordering::AcqRel);
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

    async fn assert_current_writer_epoch(&self) -> Result<(), Status> {
        let claimed = self.writer_epoch.load(Ordering::Acquire);
        let observed = latest_writer_epoch(self.coordination_store.as_ref())
            .await
            .map_err(storage_status)?;
        let active = active_writer_epoch(self.coordination_store.as_ref())
            .await
            .map_err(storage_status)?;
        if claimed == 0 || observed != claimed || active != Some(claimed) {
            return Err(Status::failed_precondition(format!(
                "writer epoch is stale: claimed {claimed}, authoritative {observed}, active {active:?}"
            )));
        }
        Ok(())
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

    async fn read_idempotency(&self, key: &[u8]) -> Result<Option<IdempotencyRecord>, Status> {
        self.read_value(key)
            .await?
            .map(|value| {
                serde_json::from_slice(&value)
                    .map_err(|_| Status::data_loss("invalid durable idempotency record"))
            })
            .transpose()
    }
}

fn idempotency_record_key(value: &str) -> Result<Option<Vec<u8>>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_IDEMPOTENCY_KEY_BYTES || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Status::invalid_argument("invalid idempotency key"));
    }
    let mut key = IDEMPOTENCY_PREFIX.to_vec();
    key.extend_from_slice(value.as_bytes());
    Ok(Some(key))
}

fn write_batch_digest(request: &WriteBatchRequest) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vaulticdb-write-batch-v1\0");
    for put in &request.puts {
        digest.update((put.key.len() as u64).to_be_bytes());
        digest.update(&put.key);
        digest.update((put.value.len() as u64).to_be_bytes());
        digest.update(&put.value);
    }
    digest.update([0xff]);
    for delete in &request.deletes {
        digest.update((delete.len() as u64).to_be_bytes());
        digest.update(delete);
    }
    format!("{:x}", digest.finalize())
}

fn transaction_digest(transaction_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vaulticdb-transaction-commit-v1\0");
    digest.update(transaction_id.as_bytes());
    format!("{:x}", digest.finalize())
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

async fn latest_writer_epoch(store: &dyn ObjectStore) -> Result<u64> {
    let prefix = ObjectPath::from(WRITER_EPOCH_PREFIX);
    let mut objects = store.list(Some(&prefix));
    let mut latest = 0u64;
    while let Some(object) = objects.next().await {
        let object = object.context("list writer epoch coordination objects")?;
        let Some(name) = object.location.as_ref().rsplit('/').next() else {
            continue;
        };
        if let Ok(epoch) = name.parse::<u64>() {
            latest = latest.max(epoch);
        }
    }
    Ok(latest)
}

async fn claim_writer_epoch(
    store: &dyn ObjectStore,
    takeover_epoch: Option<u64>,
) -> Result<Option<u64>> {
    let epoch = latest_writer_epoch(store)
        .await?
        .checked_add(1)
        .context("writer epoch overflow")?;
    let active_path = ObjectPath::from(ACTIVE_WRITER_PATH);
    let mode = if let Some(expected_epoch) = takeover_epoch {
        let current = store
            .get(&active_path)
            .await
            .context("read active writer claim for takeover")?;
        let version = UpdateVersion {
            e_tag: current.meta.e_tag.clone(),
            version: current.meta.version.clone(),
        };
        let bytes = current
            .bytes()
            .await
            .context("read active writer takeover claim")?;
        let observed: u64 = std::str::from_utf8(&bytes)
            .context("decode active writer takeover claim")?
            .parse()
            .context("parse active writer takeover epoch")?;
        if observed != expected_epoch || observed != latest_writer_epoch(store).await? {
            bail!("active writer changed since takeover was authorized")
        }
        PutMode::Update(version)
    } else {
        PutMode::Create
    };
    match store
        .put_opts(
            &active_path,
            epoch.to_string().into_bytes().into(),
            PutOptions::from(mode),
        )
        .await
    {
        Ok(_) => {}
        Err(
            slatedb::object_store::Error::AlreadyExists { .. }
            | slatedb::object_store::Error::Precondition { .. },
        ) => return Ok(None),
        Err(error) => return Err(error).context("claim active writer ownership"),
    }
    let path = ObjectPath::from(format!("{WRITER_EPOCH_PREFIX}/{epoch:020}"));
    let value = format!("pid={} time_ms={}\n", std::process::id(), unix_time_ms()?).into_bytes();
    if let Err(error) = store
        .put_opts(&path, value.into(), PutOptions::from(PutMode::Create))
        .await
    {
        let _ = store.delete(&active_path).await;
        return Err(error).context("publish writer epoch history");
    }
    Ok(Some(epoch))
}

async fn active_writer_epoch(store: &dyn ObjectStore) -> Result<Option<u64>> {
    match store.get(&ObjectPath::from(ACTIVE_WRITER_PATH)).await {
        Ok(result) => {
            let bytes = result.bytes().await.context("read active writer claim")?;
            let value = std::str::from_utf8(&bytes).context("decode active writer claim")?;
            Ok(Some(value.parse().context("parse active writer epoch")?))
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error).context("read active writer claim"),
    }
}

async fn release_writer_claim(store: &dyn ObjectStore, epoch: u64) -> Result<()> {
    if active_writer_epoch(store).await? != Some(epoch) {
        bail!("refusing to release a writer claim owned by another epoch")
    }
    store
        .delete(&ObjectPath::from(ACTIVE_WRITER_PATH))
        .await
        .context("release active writer claim")
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

async fn scan_prefix_reader(
    reader: &DbReader,
    prefix: &[u8],
    suffix: &[u8],
) -> Result<DbIterator, Status> {
    if suffix.is_empty() {
        reader.scan_prefix(prefix, ..).await.map_err(storage_error)
    } else {
        reader
            .scan_prefix(prefix, (Excluded(suffix), Unbounded))
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

#[cfg(test)]
mod role_tests {
    use super::*;
    use crate::proto::{KeyValue, WriteBatchRequest};

    #[tokio::test]
    async fn demote_reads_and_promote_writes_without_restart() {
        let suffix = rand::random::<u64>();
        unsafe {
            env::set_var("VAULTICDB_OBJECT_STORE", "memory");
            env::set_var("VAULTICDB_DATABASE_PATH", format!("role-test-{suffix}"));
        }
        let storage = Storage::open(&format!("role-repo-{suffix}")).await.unwrap();
        let request = WriteBatchRequest {
            puts: vec![KeyValue {
                key: b"key".to_vec(),
                value: b"before".to_vec(),
            }],
            await_durable: true,
            ..Default::default()
        };
        storage.write_batch(&request).await.unwrap();
        storage.demote().await.unwrap();
        assert_eq!(storage.get(b"key", "").await.unwrap().value, b"before");
        assert!(storage.write_batch(&request).await.is_err());

        storage.promote(None).await.unwrap();
        let request = WriteBatchRequest {
            puts: vec![KeyValue {
                key: b"key".to_vec(),
                value: b"after".to_vec(),
            }],
            await_durable: true,
            ..Default::default()
        };
        storage.write_batch(&request).await.unwrap();
        assert_eq!(storage.get(b"key", "").await.unwrap().value, b"after");
        storage.close().await.unwrap();
        unsafe {
            env::remove_var("VAULTICDB_DATABASE_PATH");
        }
    }

    #[tokio::test]
    async fn durable_idempotency_recovers_batches_and_transaction_commits() {
        let suffix = rand::random::<u64>();
        unsafe {
            env::set_var("VAULTICDB_OBJECT_STORE", "memory");
            env::set_var(
                "VAULTICDB_DATABASE_PATH",
                format!("idempotency-test-{suffix}"),
            );
        }
        let storage = Storage::open(&format!("idempotency-repo-{suffix}"))
            .await
            .unwrap();
        let request = WriteBatchRequest {
            puts: vec![KeyValue {
                key: b"direct".to_vec(),
                value: b"one".to_vec(),
            }],
            idempotency_key: "batch-one".to_owned(),
            ..Default::default()
        };
        assert!(storage.write_batch(&request).await.unwrap());
        assert!(storage.write_batch(&request).await.unwrap());
        let mut conflict = request.clone();
        conflict.puts[0].value = b"two".to_vec();
        assert_eq!(
            storage.write_batch(&conflict).await.unwrap_err().code(),
            tonic::Code::AlreadyExists
        );

        let transaction_id = storage.begin().await.unwrap();
        storage
            .write_batch(&WriteBatchRequest {
                puts: vec![KeyValue {
                    key: b"transaction".to_vec(),
                    value: b"committed".to_vec(),
                }],
                transaction_id: transaction_id.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        storage.commit(&transaction_id, "commit-one").await.unwrap();
        storage.commit(&transaction_id, "commit-one").await.unwrap();
        assert_eq!(
            storage.get(b"transaction", "").await.unwrap().value,
            b"committed"
        );
        storage.close().await.unwrap();
        unsafe {
            env::remove_var("VAULTICDB_DATABASE_PATH");
        }
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
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let db = Db::open("migration-test", object_store.clone())
            .await
            .unwrap();
        let storage = Storage {
            database: RwLock::new(Database::Writer(db)),
            database_path: "migration-test".to_owned(),
            coordination_store: object_store.clone(),
            object_store,
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
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: 1_000,
            broker_lease: None,
            writer_epoch: AtomicU64::new(1),
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

    #[tokio::test]
    async fn writer_epoch_claim_is_exclusive_and_fences_stale_claims() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), Some(1))
                .await
                .unwrap(),
            Some(2)
        );
        assert!(claim_writer_epoch(object_store.as_ref(), Some(1))
            .await
            .is_err());

        let db = Db::open("epoch-test", object_store.clone()).await.unwrap();
        let storage = Storage {
            database: RwLock::new(Database::Writer(db)),
            database_path: "epoch-test".to_owned(),
            coordination_store: object_store.clone(),
            object_store,
            encryption: EncryptionStatus {
                enabled: false,
                algorithm: "none",
                active_dek_version: 0,
                envelope_generation: 0,
                unlock_slot: None,
                recovery_unlock: false,
                initializing: false,
            },
            key_manager: None,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: 1_000,
            broker_lease: None,
            writer_epoch: AtomicU64::new(1),
        };
        let error = storage.assert_current_writer_epoch().await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
}
