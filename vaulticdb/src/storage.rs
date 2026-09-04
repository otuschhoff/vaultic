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
    error::VaulticDbError,
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
const ACTIVE_GENERATION_PATH: &str = "_vaultic/metadata-authority";
const GENERATION_DECISION_PREFIX: &str = "_vaultic/metadata-authority-decisions";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationAuthority {
    pub format: u32,
    pub repository_id: String,
    pub decision: u64,
    pub active_generation: u64,
    pub namespace: String,
    pub previous_generation: u64,
    pub previous_namespace: String,
    pub state: String,
    pub report_sha256: String,
    pub decided_at_ms: u64,
    pub observation_until_ms: u64,
    pub retired_generation: u64,
}

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
        let existing = db
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
        let db = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
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
        let db = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
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
        let db = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
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
        let previous = std::mem::replace(&mut *database, Database::Unavailable);
        let Database::Writer(db) = previous else {
            *database = previous;
            return Err(VaulticDbError::WriterDemoted.into());
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
        let previous = std::mem::replace(&mut *database, Database::Unavailable);
        let Database::Reader(reader) = previous else {
            *database = previous;
            return Err(VaulticDbError::WriterTransitioning.into());
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

    pub async fn refresh_writer_fence(&self) -> Result<u64> {
        let current = self.writer_epoch.load(Ordering::Acquire);
        let epoch = claim_writer_epoch(self.coordination_store.as_ref(), Some(current))
            .await?
            .context("writer changed before metadata generation fencing")?;
        self.writer_epoch.store(epoch, Ordering::Release);
        Ok(epoch)
    }

    pub async fn ensure_writer_fence(&self) -> Result<()> {
        let current = self.writer_epoch.load(Ordering::Acquire);
        if current == 0
            || active_writer_epoch(self.coordination_store.as_ref()).await? != Some(current)
        {
            bail!("writer epoch is stale")
        }
        Ok(())
    }

    pub async fn mutations_allowed(&self, repository_id: &str) -> Result<bool> {
        Ok(self.generation_authority(repository_id).await?.state == "healthy")
    }

    pub async fn generation_authority(&self, repository_id: &str) -> Result<GenerationAuthority> {
        Ok(
            read_generation_authority(self.coordination_store.as_ref(), repository_id)
                .await?
                .0,
        )
    }

    pub async fn quarantine_generation(
        &self,
        repository_id: &str,
        expected_generation: u64,
        diagnostic_sha256: String,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&diagnostic_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.active_generation != expected_generation {
            bail!("metadata generation changed since quarantine was authorized")
        }
        if current.state == "healing-required" && current.report_sha256 == diagnostic_sha256 {
            return Ok(current);
        }
        if current.state != "healthy" {
            bail!("metadata generation is already under a recovery interlock")
        }
        let mut authority = current;
        authority.decision = authority
            .decision
            .checked_add(1)
            .context("generation decision overflow")?;
        authority.state = "healing-required".to_owned();
        authority.report_sha256 = diagnostic_sha256;
        authority.decided_at_ms = unix_time_ms()?;
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub async fn activate_generation(
        &self,
        repository_id: &str,
        expected_generation: u64,
        candidate_generation: u64,
        namespace: String,
        report_sha256: String,
        observation_window_ms: u64,
    ) -> Result<GenerationAuthority> {
        validate_generation_input(candidate_generation, &namespace, &report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.active_generation != expected_generation
            || candidate_generation <= current.active_generation
            || current.state != "healing-required"
        {
            bail!("metadata generation changed since activation was authorized")
        }
        let decided_at_ms = unix_time_ms()?;
        let authority = GenerationAuthority {
            format: 1,
            repository_id: repository_id.to_owned(),
            decision: current
                .decision
                .checked_add(1)
                .context("generation decision overflow")?,
            active_generation: candidate_generation,
            namespace,
            previous_generation: current.active_generation,
            previous_namespace: current.namespace,
            state: "post-activation".to_owned(),
            report_sha256,
            decided_at_ms,
            observation_until_ms: decided_at_ms
                .checked_add(observation_window_ms)
                .context("generation observation deadline overflow")?,
            retired_generation: current.retired_generation,
        };
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub async fn verify_generation(
        &self,
        repository_id: &str,
        expected_decision: u64,
        report_sha256: String,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.decision != expected_decision || current.state != "post-activation" {
            bail!("metadata generation is not awaiting the authorized post-activation check")
        }
        if unix_time_ms()? < current.observation_until_ms {
            bail!("metadata generation observation window has not elapsed")
        }
        let mut authority = current;
        authority.decision = authority
            .decision
            .checked_add(1)
            .context("generation decision overflow")?;
        authority.state = "healthy".to_owned();
        authority.report_sha256 = report_sha256;
        authority.decided_at_ms = unix_time_ms()?;
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub async fn rollback_generation(
        &self,
        repository_id: &str,
        expected_decision: u64,
        report_sha256: String,
        observation_window_ms: u64,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.decision != expected_decision
            || current.state != "post-activation"
            || current.previous_generation == 0
            || current.previous_namespace.is_empty()
        {
            bail!("metadata generation rollback is no longer permitted")
        }
        let decided_at_ms = unix_time_ms()?;
        let authority = GenerationAuthority {
            format: 1,
            repository_id: repository_id.to_owned(),
            decision: current
                .decision
                .checked_add(1)
                .context("generation decision overflow")?,
            active_generation: current.previous_generation,
            namespace: current.previous_namespace,
            previous_generation: current.active_generation,
            previous_namespace: current.namespace,
            state: "post-activation".to_owned(),
            report_sha256,
            decided_at_ms,
            observation_until_ms: decided_at_ms
                .checked_add(observation_window_ms)
                .context("generation observation deadline overflow")?,
            retired_generation: current.retired_generation,
        };
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    pub async fn retire_generation(
        &self,
        repository_id: &str,
        expected_decision: u64,
        generation: u64,
        report_sha256: String,
    ) -> Result<GenerationAuthority> {
        validate_report_sha256(&report_sha256)?;
        let (current, version) =
            read_generation_authority(self.coordination_store.as_ref(), repository_id).await?;
        if current.decision != expected_decision
            || current.state != "healthy"
            || generation == current.active_generation
            || generation != current.previous_generation
        {
            bail!("metadata generation is not eligible for retirement")
        }
        let mut authority = current;
        authority.decision = authority
            .decision
            .checked_add(1)
            .context("generation decision overflow")?;
        authority.previous_generation = 0;
        authority.previous_namespace.clear();
        authority.retired_generation = generation;
        authority.report_sha256 = report_sha256;
        authority.decided_at_ms = unix_time_ms()?;
        publish_generation_authority(self.coordination_store.as_ref(), &authority, version).await?;
        Ok(authority)
    }

    async fn writer(&self) -> Result<tokio::sync::RwLockReadGuard<'_, Database>, Status> {
        let database = self.database.read().await;
        if !matches!(&*database, Database::Writer(_)) {
            return Err(VaulticDbError::WriterDemoted.into());
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
            let db = database
                .as_writer()
                .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
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
        let database = self.writer().await?;
        let writer = database
            .as_writer()
            .ok_or_else(|| Status::from(VaulticDbError::WriterDemoted))?;
        let transaction = writer
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

include!("storage/operations.rs");

include!("storage/role_tests.rs");

include!("storage/object_store.rs");

include!("storage/tests.rs");
