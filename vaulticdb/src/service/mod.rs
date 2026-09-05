//! gRPC service implementation and request coordination.

use std::{
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::{sync::watch, sync::Mutex};
use tonic::{Request, Response, Status};
use vaulticdb::encryption;
use vaulticdb::ids::RepositoryId;

use crate::{
    config::Config,
    error::VaulticDbError,
    proto::{
        self, vaultic_db_server::VaulticDb, ActivateGenerationRequest, AddCloudKeySlotRequest,
        AddLocalKeySlotRequest, BeginResponse, CapabilitiesRequest, CapabilitiesResponse,
        CommitResponse, DemoteWriterRequest, Empty, EncryptionAuditResponse,
        EscrowMasterKeyRequest, EscrowMasterKeyResponse, ExportKeyEnvelopeResponse,
        FinalizeCapsuleMigrationRequest, GenerationStatusRequest, GenerationStatusResponse,
        GetRequest, GetResponse, HealthRequest, HealthResponse, KeySlotInfo, KeyStatusRequest,
        KeyStatusResponse, MasterKeyRequest, MasterKeyResponse, MultiGetRequest, MultiGetResponse,
        PrepareCapsuleMigrationRequest, PrepareCapsuleMigrationResponse, PromoteWriterRequest,
        PublishCapsuleMutationRequest, PublishCapsuleMutationResponse, QuarantineGenerationRequest,
        RecoverEscrowRequest, RemoveKeySlotRequest, RetireGenerationRequest, RewriteDekRequest,
        RewriteDekResponse, RollbackGenerationRequest, RotateDekRequest, RotateLocalKeySlotRequest,
        ScanRequest, ScanResponse, StoreMasterKeyRequest, TransactionRequest,
        VerifyGenerationRequest, WriteBatchRequest, WriteBatchResponse, WriterStatusRequest,
        WriterStatusResponse,
    },
    storage::{self, Storage},
    MAX_BATCH_ITEMS, MAX_CONCURRENT_REQUESTS, MAX_MESSAGE_BYTES, MAX_PAGE_ITEMS, PROTOCOL_VERSION,
    SCHEMA_VERSION,
};
use vaulticdb::writer_role::{WriterRole as CoreWriterRole, WriterRoleState};
use zeroize::Zeroizing;

#[path = "encryption.rs"]
mod encryption_helpers;
mod generation;
mod kv;
mod transactions;
mod writer_role;

use kv::{check_context, check_request, check_storage_request};
use writer_role::role_error;

pub(crate) use writer_role::unix_time_ms_i64;

#[derive(Clone)]
pub(crate) struct DaemonState {
    pub(crate) daemon_id: Arc<str>,
    pub(crate) repository_id: RepositoryId,
    pub(crate) auth_token: Option<Arc<Zeroizing<String>>>,
    pub(crate) unix_socket: bool,
    pub(crate) tcp_enabled: bool,
    pub(crate) draining: Arc<AtomicBool>,
    pub(crate) writer_role: Arc<Mutex<WriterRoleState>>,
    pub(crate) writer_transition: Arc<Mutex<()>>,
    pub(crate) last_writer_activity: Arc<Mutex<Instant>>,
    pub(crate) writer_idle_grace: Option<Duration>,
    pub(crate) writer_transition_timeout: Duration,
    pub(crate) clock_started: Instant,
    pub(crate) clock_started_unix_ms: i64,
}

#[derive(Clone)]
pub(crate) struct Service {
    pub(crate) state: DaemonState,
    pub(crate) shutdown: watch::Sender<bool>,
    pub(crate) storage: Arc<Storage>,
}

struct WriteIntentGuard {
    writer_role: Arc<Mutex<WriterRoleState>>,
    last_writer_activity: Arc<Mutex<Instant>>,
}

impl Drop for WriteIntentGuard {
    fn drop(&mut self) {
        let writer_role = self.writer_role.clone();
        let last_writer_activity = self.last_writer_activity.clone();
        tokio::spawn(async move {
            writer_role.lock().await.finish_write();
            *last_writer_activity.lock().await = Instant::now();
        });
    }
}

#[tonic::async_trait]
impl VaulticDb for Service {
    async fn generation_status(
        &self,
        request: Request<GenerationStatusRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        self.handle_generation_status(request).await
    }

    async fn activate_generation(
        &self,
        request: Request<ActivateGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        self.handle_activate_generation(request).await
    }

    async fn quarantine_generation(
        &self,
        request: Request<QuarantineGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        self.handle_quarantine_generation(request).await
    }

    async fn verify_generation(
        &self,
        request: Request<VerifyGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        self.handle_verify_generation(request).await
    }

    async fn rollback_generation(
        &self,
        request: Request<RollbackGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        self.handle_rollback_generation(request).await
    }

    async fn retire_generation(
        &self,
        request: Request<RetireGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        self.handle_retire_generation(request).await
    }

    async fn writer_status(
        &self,
        request: Request<WriterStatusRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        self.handle_writer_status(request).await
    }

    async fn demote_writer(
        &self,
        request: Request<DemoteWriterRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        self.handle_demote_writer(request).await
    }

    async fn promote_writer(
        &self,
        request: Request<PromoteWriterRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        self.handle_promote_writer(request).await
    }

    async fn publish_capsule_mutation(
        &self,
        request: Request<PublishCapsuleMutationRequest>,
    ) -> Result<Response<PublishCapsuleMutationResponse>, Status> {
        self.handle_publish_capsule_mutation(request).await
    }

    async fn prepare_capsule_migration(
        &self,
        request: Request<PrepareCapsuleMigrationRequest>,
    ) -> Result<Response<PrepareCapsuleMigrationResponse>, Status> {
        self.handle_prepare_capsule_migration(request).await
    }

    async fn finalize_capsule_migration(
        &self,
        request: Request<FinalizeCapsuleMigrationRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.handle_finalize_capsule_migration(request).await
    }
    async fn check_encryption(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<EncryptionAuditResponse>, Status> {
        self.handle_check_encryption(request).await
    }

    async fn export_key_envelope(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<ExportKeyEnvelopeResponse>, Status> {
        self.handle_export_key_envelope(request).await
    }

    async fn escrow_master_key(
        &self,
        request: Request<EscrowMasterKeyRequest>,
    ) -> Result<Response<EscrowMasterKeyResponse>, Status> {
        self.handle_escrow_master_key(request).await
    }

    async fn recover_escrow(
        &self,
        request: Request<RecoverEscrowRequest>,
    ) -> Result<Response<MasterKeyResponse>, Status> {
        self.handle_recover_escrow(request).await
    }

    async fn rewrite_dek(
        &self,
        request: Request<RewriteDekRequest>,
    ) -> Result<Response<RewriteDekResponse>, Status> {
        self.handle_rewrite_dek(request).await
    }

    async fn rotate_dek(
        &self,
        request: Request<RotateDekRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.handle_rotate_dek(request).await
    }

    async fn add_cloud_key_slot(
        &self,
        request: Request<AddCloudKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.handle_add_cloud_key_slot(request).await
    }

    async fn key_status(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.handle_key_status(request).await
    }

    async fn add_local_key_slot(
        &self,
        request: Request<AddLocalKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.handle_add_local_key_slot(request).await
    }

    async fn remove_key_slot(
        &self,
        request: Request<RemoveKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.handle_remove_key_slot(request).await
    }

    async fn rotate_local_key_slot(
        &self,
        request: Request<RotateLocalKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.handle_rotate_local_key_slot(request).await
    }

    async fn get_master_key(
        &self,
        request: Request<MasterKeyRequest>,
    ) -> Result<Response<MasterKeyResponse>, Status> {
        self.handle_get_master_key(request).await
    }

    async fn store_master_key(
        &self,
        request: Request<StoreMasterKeyRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.handle_store_master_key(request).await
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        self.handle_health(request).await
    }

    async fn capabilities(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        self.handle_capabilities(request).await
    }

    async fn drain(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.handle_drain(request).await
    }

    async fn shutdown(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.handle_shutdown(request).await
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.handle_get(request).await
    }

    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        self.handle_multi_get(request).await
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        self.handle_scan(request).await
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        self.handle_write_batch(request).await
    }

    async fn begin(&self, request: Request<Empty>) -> Result<Response<BeginResponse>, Status> {
        self.handle_begin(request).await
    }

    async fn commit(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        self.handle_commit(request).await
    }

    async fn rollback(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.handle_rollback(request).await
    }
}

include!("operations.rs");
