use std::{
    env,
    fs::File,
    future::Future,
    io::{self, Read},
    net::SocketAddr,
    os::fd::{FromRawFd, RawFd},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use hmac::{Hmac, Mac};
use ipnet::IpNet;
use prost::Message;
use sha2::{Digest, Sha256};
use slatedb::config::DbReaderOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbReader, DbReaderMode, WriteBatch};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::watch,
    sync::{mpsc, Mutex},
};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{transport::Server, Request, Response, Status};
use vaulticdb::encryption;

pub mod proto {
    tonic::include_proto!("vaulticdb.v1");
}

use proto::{
    vaultic_db_server::{VaulticDb, VaulticDbServer},
    ActivateGenerationRequest, AddCloudKeySlotRequest, AddLocalKeySlotRequest, BeginResponse,
    CapabilitiesRequest, CapabilitiesResponse, CommitResponse, DemoteWriterRequest, Empty,
    EncryptionAuditResponse, EscrowMasterKeyRequest, EscrowMasterKeyResponse,
    ExportKeyEnvelopeResponse, FinalizeCapsuleMigrationRequest, GenerationStatusRequest,
    GenerationStatusResponse, GetRequest, GetResponse, HealthRequest, HealthResponse, KeySlotInfo,
    KeyStatusRequest, KeyStatusResponse, MasterKeyRequest, MasterKeyResponse, MultiGetRequest,
    MultiGetResponse, PrepareCapsuleMigrationRequest, PrepareCapsuleMigrationResponse,
    PromoteWriterRequest, PublishCapsuleMutationRequest, PublishCapsuleMutationResponse,
    QuarantineGenerationRequest, RecoverEscrowRequest, RemoveKeySlotRequest, RequestContext,
    RetireGenerationRequest, RewriteDekRequest, RewriteDekResponse, RollbackGenerationRequest,
    RotateDekRequest, RotateLocalKeySlotRequest, ScanRequest, ScanResponse, StoreMasterKeyRequest,
    TransactionRequest, VerifyGenerationRequest, WriteBatchRequest, WriteBatchResponse,
    WriterStatusRequest, WriterStatusResponse,
};
use vaulticdb::writer_role::{RoleError, WriterRole as CoreWriterRole, WriterRoleState};
use zeroize::Zeroizing;

mod error;
mod replication;
mod storage;

use error::VaulticDbError;
use storage::{repeated_message_encoded_len, GenerationAuthority, Storage};

const PROTOCOL_VERSION: &str = "vaulticdb.v1";
const SCHEMA_VERSION: &str = "0";
const MAX_BATCH_ITEMS: u32 = 10_000;
const MAX_PAGE_ITEMS: u32 = 1_000;
const MAX_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 128;

fn generation_status_response(authority: GenerationAuthority) -> GenerationStatusResponse {
    GenerationStatusResponse {
        repository_id: authority.repository_id,
        decision: authority.decision,
        active_generation: authority.active_generation,
        namespace: authority.namespace,
        previous_generation: authority.previous_generation,
        previous_namespace: authority.previous_namespace,
        state: authority.state.clone(),
        report_sha256: authority.report_sha256,
        decided_at_unix_ms: authority.decided_at_ms.min(i64::MAX as u64) as i64,
        observation_until_unix_ms: authority.observation_until_ms.min(i64::MAX as u64) as i64,
        retired_generation: authority.retired_generation,
        destructive_maintenance_allowed: authority.state == "healthy",
    }
}

#[derive(Clone)]
struct DaemonState {
    daemon_id: Arc<str>,
    repository_id: Arc<str>,
    auth_token: Option<Arc<Zeroizing<String>>>,
    unix_socket: bool,
    tcp_enabled: bool,
    draining: Arc<AtomicBool>,
    writer_role: Arc<Mutex<WriterRoleState>>,
    writer_transition: Arc<Mutex<()>>,
    last_writer_activity: Arc<Mutex<Instant>>,
    writer_idle_grace: Option<Duration>,
    writer_transition_timeout: Duration,
    clock_started: Instant,
    clock_started_unix_ms: i64,
}

#[derive(Clone)]
struct Service {
    state: DaemonState,
    shutdown: watch::Sender<bool>,
    storage: Arc<Storage>,
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
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let authority = self
            .storage
            .generation_authority(&request.get_ref().repository_id)
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(generation_status_response(authority)))
    }

    async fn activate_generation(
        &self,
        request: Request<ActivateGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.approve {
            return Err(Status::failed_precondition(
                "metadata generation activation requires explicit approval",
            ));
        }
        let authority = self
            .storage
            .activate_generation(
                &request.repository_id,
                request.expected_active_generation,
                request.candidate_generation,
                request.candidate_namespace,
                request.report_sha256,
                request.observation_window_ms,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(generation_status_response(authority)))
    }

    async fn quarantine_generation(
        &self,
        request: Request<QuarantineGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.healing_required {
            return Err(Status::invalid_argument(
                "quarantine requires a proven healing-required classification",
            ));
        }
        let authority = self
            .storage
            .quarantine_generation(
                &request.repository_id,
                request.expected_active_generation,
                request.diagnostic_sha256,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(generation_status_response(authority)))
    }

    async fn verify_generation(
        &self,
        request: Request<VerifyGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.post_activation_check_clean {
            return Err(Status::failed_precondition(
                "post-activation index check did not pass",
            ));
        }
        let authority = self
            .storage
            .verify_generation(
                &request.repository_id,
                request.expected_decision,
                request.report_sha256,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(generation_status_response(authority)))
    }

    async fn rollback_generation(
        &self,
        request: Request<RollbackGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.acknowledge {
            return Err(Status::failed_precondition(
                "metadata generation rollback requires separate acknowledgement",
            ));
        }
        let authority = self
            .storage
            .rollback_generation(
                &request.repository_id,
                request.expected_decision,
                request.report_sha256,
                request.observation_window_ms,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        self.storage
            .refresh_writer_fence()
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(generation_status_response(authority)))
    }

    async fn retire_generation(
        &self,
        request: Request<RetireGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.acknowledge {
            return Err(Status::failed_precondition(
                "metadata generation retirement requires separate acknowledgement",
            ));
        }
        let authority = self
            .storage
            .retire_generation(
                &request.repository_id,
                request.expected_decision,
                request.generation,
                request.report_sha256,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(generation_status_response(authority)))
    }

    async fn writer_status(
        &self,
        request: Request<WriterStatusRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        Ok(Response::new(self.writer_status_response().await))
    }

    async fn demote_writer(
        &self,
        request: Request<DemoteWriterRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let request = request.into_inner();
        let timeout = if request.timeout_ms == 0 {
            self.state.writer_transition_timeout
        } else {
            Duration::from_millis(request.timeout_ms.clamp(1, 300_000))
        };
        let status = self
            .transition_to_reader(timeout, request.reason, request.force)
            .await?;
        Ok(Response::new(status))
    }

    async fn promote_writer(
        &self,
        request: Request<PromoteWriterRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let _transition = self.state.writer_transition.lock().await;
        let request = request.into_inner();
        let reason = request.reason;
        {
            let mut role = self.state.writer_role.lock().await;
            role.begin_promotion(Instant::now(), reason)
                .map_err(role_error)?;
        }
        let next_epoch = match self
            .storage
            .promote(if request.force_takeover {
                Some(request.expected_active_epoch)
            } else {
                None
            })
            .await
        {
            Ok(epoch) => epoch,
            Err(error) => {
                let observed_epoch = self.storage.writer_status_epoch().await.1;
                self.state.writer_role.lock().await.fence(
                    observed_epoch,
                    Instant::now(),
                    "writer promotion failed",
                );
                return Err(Status::failed_precondition(format!(
                    "writer promotion failed: {error:#}"
                )));
            }
        };
        self.state
            .writer_role
            .lock()
            .await
            .complete_promotion(next_epoch, Instant::now())
            .map_err(role_error)?;
        *self.state.last_writer_activity.lock().await = Instant::now();
        Ok(Response::new(self.writer_status_response().await))
    }

    async fn publish_capsule_mutation(
        &self,
        request: Request<PublishCapsuleMutationRequest>,
    ) -> Result<Response<PublishCapsuleMutationResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let request = request.into_inner();
        let capsule = validate_capsule_mutation(
            std::path::Path::new(&request.capsule_directory),
            &request.repository_id,
            &request.capsule,
            &request.capsule_sha256,
            request.identity_recovery,
        )
        .map_err(key_management_error)?;
        let manager = self.storage.key_manager()?;
        let mirror_path = manager
            .publish_capsule_mirror(&capsule)
            .await
            .map_err(key_management_error)?;
        let local_path = encryption::recovery_capsule::publish_local(
            std::path::Path::new(&request.capsule_directory),
            &capsule,
        )
        .map_err(key_management_error)?;
        Ok(Response::new(PublishCapsuleMutationResponse {
            generation: capsule.header.generation,
            local_path: local_path.display().to_string(),
            mirror_path,
            capsule_sha256: request.capsule_sha256,
        }))
    }

    async fn prepare_capsule_migration(
        &self,
        request: Request<PrepareCapsuleMigrationRequest>,
    ) -> Result<Response<PrepareCapsuleMigrationResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let mut request = request.into_inner();
        if request.threshold == 0 || request.threshold > u32::from(u8::MAX) {
            return Err(Status::invalid_argument("invalid capsule threshold"));
        }
        let manager = self.storage.key_manager()?;
        let audit = manager
            .audit_objects()
            .await
            .map_err(key_management_error)?;
        if audit.invalid_objects != 0
            || audit.plaintext_objects != 0
            || audit.old_version_objects != 0
        {
            return Err(Status::failed_precondition(
                "all metadata objects must authenticate under the active DEK before migration",
            ));
        }
        let metadata_dek = manager
            .active_dek_for_migration()
            .await
            .map_err(key_management_error)?;
        let repository_master_key =
            Zeroizing::new(self.storage.get_master_key().await?.ok_or_else(|| {
                Status::failed_precondition("repository master key is not stored")
            })?);
        let protected_credentials = request
            .members
            .iter_mut()
            .map(|member| {
                Ok((
                    member.member_id.clone(),
                    member.provider.clone(),
                    Zeroizing::new(std::mem::take(&mut member.credential)),
                ))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let credentials = protected_credentials
            .iter()
            .map(|(member_id, provider, credential)| {
                let credential = match provider.as_str() {
                    "offline-argon2id" => {
                        encryption::recovery_capsule::MemberCredential::Passphrase(
                            credential.as_slice(),
                        )
                    }
                    "offline-keyfile" => encryption::recovery_capsule::MemberCredential::Keyfile(
                        credential.as_slice(),
                    ),
                    _ => return Err(Status::invalid_argument(
                        "migration currently accepts offline-argon2id and offline-keyfile members",
                    )),
                };
                Ok((member_id.as_str(), credential))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let capsule = encryption::recovery_capsule::CapsuleBuilder::new(
            request.repository_id.clone(),
            request.generation,
        )
        .broker_identity_public_key(&request.broker_identity_public_key)
        .create_offline_threshold(
            &request.group_id,
            request.threshold as u8,
            &credentials,
            &metadata_dek,
            &repository_master_key,
        )
        .map_err(key_management_error)?;
        let verification = credentials
            .iter()
            .map(|(member, credential)| ((*member).to_owned(), *credential))
            .collect();
        capsule
            .recover_offline(&verification)
            .map_err(key_management_error)?;
        let local_path = encryption::recovery_capsule::publish_local(
            std::path::Path::new(&request.capsule_directory),
            &capsule,
        )
        .map_err(key_management_error)?;
        let mirror_path = manager
            .publish_capsule_mirror(&capsule)
            .await
            .map_err(key_management_error)?;
        let mut encoded = serde_json::to_vec_pretty(&capsule)
            .map_err(|error| key_management_error(error.into()))?;
        encoded.push(b'\n');
        let capsule_sha256 = format!("{:x}", Sha256::digest(&encoded));
        self.storage
            .record_capsule_migration(&capsule_sha256)
            .await?;
        Ok(Response::new(PrepareCapsuleMigrationResponse {
            generation: capsule.header.generation,
            local_path: local_path.display().to_string(),
            mirror_path,
            capsule_sha256,
            capsule: encoded,
        }))
    }

    async fn finalize_capsule_migration(
        &self,
        request: Request<FinalizeCapsuleMigrationRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let repository_id = request.get_ref().repository_id.as_str();
        let capsule_sha256 = request.get_ref().capsule_sha256.as_str();
        match self.storage.get_master_key().await? {
            Some(master_key) => {
                let master_key = Zeroizing::new(master_key);
                verify_capsule_migration_proof(
                    &master_key,
                    repository_id,
                    capsule_sha256,
                    &request.get_ref().broker_key_proof,
                )?;
                self.storage
                    .finalize_capsule_migration(capsule_sha256)
                    .await?;
            }
            None => {
                self.storage
                    .finalize_capsule_migration(capsule_sha256)
                    .await?;
            }
        }
        Ok(Response::new(Empty::default()))
    }
    async fn check_encryption(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<EncryptionAuditResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        if !self.storage.encryption_status().enabled {
            return Ok(Response::new(EncryptionAuditResponse {
                enabled: false,
                ..Default::default()
            }));
        }
        let manager = self.storage.key_manager()?;
        let (envelope_generation, active_dek_version, _) = manager.status().await;
        let audit = manager
            .audit_objects()
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(EncryptionAuditResponse {
            objects: audit.objects,
            invalid_objects: audit.invalid_objects,
            plaintext_objects: audit.plaintext_objects,
            old_version_objects: audit.old_version_objects,
            envelope_generation,
            active_dek_version,
            algorithm: "AES-256-GCM".to_string(),
            enabled: true,
        }))
    }

    async fn export_key_envelope(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<ExportKeyEnvelopeResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let manager = self.storage.key_manager()?;
        let (generation, _, _) = manager.status().await;
        Ok(Response::new(ExportKeyEnvelopeResponse {
            envelope: manager
                .export_envelope()
                .await
                .map_err(key_management_error)?,
            generation,
        }))
    }

    async fn escrow_master_key(
        &self,
        request: Request<EscrowMasterKeyRequest>,
    ) -> Result<Response<EscrowMasterKeyResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let request = request.into_inner();
        let token = cloud_token(request.bearer_token)?;
        let provider = encryption::envelope::providers::for_management(&request.provider, token)
            .await
            .map_err(key_management_error)?;
        let master_key =
            Zeroizing::new(self.storage.get_master_key().await?.ok_or_else(|| {
                Status::failed_precondition("repository master key is not stored")
            })?);
        let record = encryption::envelope::create_escrow_record(
            &request.repository_id,
            &request.escrow_id,
            &request.key_reference,
            &master_key,
            provider.as_ref(),
        )
        .await
        .map_err(key_management_error)?;
        Ok(Response::new(EscrowMasterKeyResponse {
            record: serde_json::to_vec_pretty(&record)
                .map_err(|error| key_management_error(error.into()))?,
        }))
    }

    async fn recover_escrow(
        &self,
        request: Request<RecoverEscrowRequest>,
    ) -> Result<Response<MasterKeyResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let request = request.into_inner();
        let record: encryption::envelope::EscrowRecord = serde_json::from_slice(&request.record)
            .map_err(|error| Status::invalid_argument(format!("decode escrow record: {error}")))?;
        let token = cloud_token(request.bearer_token)?;
        let provider = encryption::envelope::providers::for_management(&record.provider, token)
            .await
            .map_err(key_management_error)?;
        let master_key = encryption::envelope::recover_escrow_record(
            &record,
            &request.repository_id,
            provider.as_ref(),
        )
        .await
        .map_err(key_management_error)?;
        Ok(Response::new(MasterKeyResponse {
            found: true,
            master_key: master_key.to_vec(),
        }))
    }

    async fn rewrite_dek(
        &self,
        request: Request<RewriteDekRequest>,
    ) -> Result<Response<RewriteDekResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let (rewritten, remaining) = self
            .storage
            .key_manager()?
            .rewrite_old_deks(request.get_ref().max_objects as usize)
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(RewriteDekResponse {
            rewritten: rewritten as u64,
            remaining: remaining as u64,
        }))
    }

    async fn rotate_dek(
        &self,
        request: Request<RotateDekRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        self.storage
            .key_manager()?
            .rotate_dek()
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    async fn add_cloud_key_slot(
        &self,
        request: Request<AddCloudKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let request = request.into_inner();
        let token = Zeroizing::new(request.bearer_token);
        let token =
            if token.is_empty() {
                None
            } else {
                Some(String::from_utf8(token.to_vec()).map_err(|_| {
                    Status::invalid_argument("cloud bearer token is not valid UTF-8")
                })?)
            };
        let provider = encryption::envelope::providers::for_management(&request.provider, token)
            .await
            .map_err(key_management_error)?;
        self.storage
            .key_manager()?
            .add_cloud_slot(
                &request.slot_id,
                &request.key_reference,
                request.priority,
                provider.as_ref(),
            )
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    async fn key_status(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        Ok(Response::new(self.key_status_response().await?))
    }

    async fn add_local_key_slot(
        &self,
        request: Request<AddLocalKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let request = request.into_inner();
        let passphrase = Zeroizing::new(request.passphrase);
        self.storage
            .key_manager()?
            .add_local_slot(
                &request.slot_id,
                &passphrase,
                request.priority,
                request.recovery,
            )
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    async fn remove_key_slot(
        &self,
        request: Request<RemoveKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let slot_id = request.into_inner().slot_id;
        self.storage
            .key_manager()?
            .remove_slot(&slot_id)
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    async fn rotate_local_key_slot(
        &self,
        request: Request<RotateLocalKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        let _intent = self.write_intent().await?;
        let request = request.into_inner();
        let passphrase = Zeroizing::new(request.passphrase);
        self.storage
            .key_manager()?
            .rotate_local_slot(&request.slot_id, &passphrase)
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    async fn get_master_key(
        &self,
        request: Request<MasterKeyRequest>,
    ) -> Result<Response<MasterKeyResponse>, Status> {
        check_request(
            &self.state,
            &request,
            request.get_ref().repository_id.as_str(),
        )?;
        check_context(request.get_ref().context.as_ref())?;
        if !self.state.unix_socket {
            return Err(Status::failed_precondition(
                "master-key-in-DB is available only over a private Unix socket",
            ));
        }
        let value = self.storage.get_master_key().await?;
        Ok(Response::new(MasterKeyResponse {
            found: value.is_some(),
            master_key: value.unwrap_or_default(),
        }))
    }

    async fn store_master_key(
        &self,
        request: Request<StoreMasterKeyRequest>,
    ) -> Result<Response<Empty>, Status> {
        check_request(
            &self.state,
            &request,
            request.get_ref().repository_id.as_str(),
        )?;
        check_context(request.get_ref().context.as_ref())?;
        if !self.state.unix_socket {
            return Err(Status::failed_precondition(
                "master-key-in-DB is available only over a private Unix socket",
            ));
        }
        let _intent = self.write_intent().await?;
        let master_key = Zeroizing::new(request.get_ref().master_key.clone());
        self.storage.store_master_key(&master_key).await?;
        Ok(Response::new(Empty { context: None }))
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        check_request(
            &self.state,
            &request,
            request.get_ref().repository_id.as_str(),
        )?;
        check_context(request.get_ref().context.as_ref())?;
        Ok(Response::new(HealthResponse {
            daemon_id: self.state.daemon_id.to_string(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            schema_version: SCHEMA_VERSION.to_owned(),
            repository_id: self.state.repository_id.to_string(),
            slate_db_revision: String::new(),
            ready: !self.state.draining.load(Ordering::Acquire),
        }))
    }

    async fn capabilities(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        check_request(
            &self.state,
            &request,
            request.get_ref().repository_id.as_str(),
        )?;
        check_context(request.get_ref().context.as_ref())?;
        let encryption = self.storage.encryption_status();
        Ok(Response::new(CapabilitiesResponse {
            daemon_id: self.state.daemon_id.to_string(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            schema_version: SCHEMA_VERSION.to_owned(),
            repository_id: self.state.repository_id.to_string(),
            unix_socket: self.state.unix_socket,
            tcp_enabled: self.state.tcp_enabled,
            max_batch_items: MAX_BATCH_ITEMS,
            max_message_bytes: MAX_MESSAGE_BYTES,
            max_page_items: MAX_PAGE_ITEMS,
            max_concurrent_requests: MAX_CONCURRENT_REQUESTS as u32,
            encryption_enabled: encryption.enabled,
            encryption_algorithm: encryption.algorithm.to_owned(),
            active_dek_version: encryption.active_dek_version,
            envelope_generation: encryption.envelope_generation,
            unlock_slot: encryption.unlock_slot.clone().unwrap_or_default(),
            recovery_unlock: encryption.recovery_unlock,
            writer_roles: true,
            durable_idempotency: true,
        }))
    }

    async fn drain(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        check_request(&self.state, &request, "")?;
        check_context(request.get_ref().context.as_ref())?;
        self.state.draining.store(true, Ordering::Release);
        Ok(Response::new(Empty { context: None }))
    }

    async fn shutdown(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        check_request(&self.state, &request, "")?;
        check_context(request.get_ref().context.as_ref())?;
        self.state.draining.store(true, Ordering::Release);
        let _ = self.shutdown.send(true);
        Ok(Response::new(Empty { context: None }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let request = request.into_inner();
        Ok(Response::new(
            self.storage
                .get(&request.key, &request.transaction_id)
                .await?,
        ))
    }

    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        if request.get_ref().keys.len() > MAX_BATCH_ITEMS as usize {
            return Err(Status::resource_exhausted("multi-get item limit exceeded"));
        }
        let request = request.into_inner();
        let mut results = Vec::with_capacity(request.keys.len());
        let mut response_bytes = 0usize;
        for key in request.keys {
            let result = self.storage.get(&key, &request.transaction_id).await?;
            response_bytes = response_bytes
                .checked_add(repeated_message_encoded_len(result.encoded_len()))
                .ok_or_else(|| Status::resource_exhausted("multi-get response size overflow"))?;
            if response_bytes > MAX_MESSAGE_BYTES as usize {
                return Err(Status::resource_exhausted(
                    "multi-get response byte limit exceeded",
                ));
            }
            results.push(result);
        }
        Ok(Response::new(MultiGetResponse { results }))
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        validate_scan(request.get_ref())?;
        let request = request.into_inner();
        Ok(Response::new(
            self.storage
                .scan(
                    &request.prefix,
                    &request.after_key,
                    request.page_size as usize,
                    &request.transaction_id,
                )
                .await?,
        ))
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        validate_write_batch(request.get_ref())?;
        let durable = self
            .with_write_intent(self.storage.write_batch(request.get_ref()))
            .await?;
        Ok(Response::new(WriteBatchResponse { durable }))
    }

    async fn begin(&self, request: Request<Empty>) -> Result<Response<BeginResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        self.state
            .writer_role
            .lock()
            .await
            .transaction_opened()
            .map_err(role_error)?;
        let transaction_id = match self.storage.begin().await {
            Ok(transaction_id) => transaction_id,
            Err(error) => {
                self.state.writer_role.lock().await.transaction_closed();
                return Err(error);
            }
        };
        *self.state.last_writer_activity.lock().await = Instant::now();
        Ok(Response::new(BeginResponse { transaction_id }))
    }

    async fn commit(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let result = self
            .storage
            .commit(
                &request.get_ref().transaction_id,
                &request.get_ref().idempotency_key,
            )
            .await;
        if result.is_ok() {
            self.state.writer_role.lock().await.transaction_closed();
            *self.state.last_writer_activity.lock().await = Instant::now();
        }
        result?;
        Ok(Response::new(CommitResponse { durable: true }))
    }

    async fn rollback(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<Empty>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let result = self
            .storage
            .rollback(&request.get_ref().transaction_id)
            .await;
        if result.is_ok() {
            self.state.writer_role.lock().await.transaction_closed();
            *self.state.last_writer_activity.lock().await = Instant::now();
        }
        result?;
        Ok(Response::new(Empty { context: None }))
    }
}

fn verify_capsule_migration_proof(
    master_key: &[u8],
    repository_id: &str,
    capsule_sha256: &str,
    proof: &[u8],
) -> Result<(), Status> {
    let mut verifier = Hmac::<Sha256>::new_from_slice(master_key)
        .map_err(|_| Status::internal("initialize capsule migration proof verification"))?;
    verifier.update(b"vaultic-capsule-migration-finalize-v1\0");
    verifier.update(repository_id.as_bytes());
    verifier.update(b"\0");
    verifier.update(capsule_sha256.as_bytes());
    verifier
        .verify_slice(proof)
        .map_err(|_| Status::permission_denied("capsule-recovered repository key proof failed"))
}

fn validate_capsule_mutation(
    capsule_directory: &std::path::Path,
    repository_id: &str,
    encoded: &[u8],
    expected_digest: &str,
    identity_recovery: bool,
) -> Result<encryption::recovery_capsule::RecoveryCapsule> {
    if capsule_directory.as_os_str().is_empty() || expected_digest.len() != 64 {
        bail!("capsule directory and SHA-256 digest are required");
    }
    let capsule: encryption::recovery_capsule::RecoveryCapsule =
        serde_json::from_slice(encoded).context("decode recovery capsule")?;
    capsule.validate()?;
    let canonical = serde_json::to_vec(&capsule)?;
    if canonical != encoded {
        bail!("recovery capsule must use canonical JSON encoding");
    }
    if capsule.header.repository_id != repository_id {
        bail!("recovery capsule repository identity mismatch");
    }
    if format!("{:x}", Sha256::digest(&canonical)) != expected_digest {
        bail!("recovery capsule digest mismatch");
    }
    let (_, current) =
        encryption::recovery_capsule::discover_latest(capsule_directory, repository_id)?
            .context("current recovery capsule is missing")?;
    if capsule.header.generation == current.header.generation {
        if capsule != current {
            bail!("recovery capsule generation conflicts with the current capsule");
        }
        return Ok(capsule);
    }
    let expected_generation = current
        .header
        .generation
        .checked_add(1)
        .context("capsule generation overflow")?;
    if capsule.header.generation != expected_generation {
        bail!("capsule mutation generation is not sequential");
    }
    let identity_changed =
        capsule.header.broker_identity_public_key != current.header.broker_identity_public_key;
    if identity_changed != identity_recovery {
        bail!("broker identity pin change does not match identity-recovery declaration");
    }
    Ok(capsule)
}

async fn publish_capsule_without_database(arguments: &[String]) -> Result<()> {
    if arguments.len() != 5 {
        bail!("usage: vaulticdb publish-capsule CAPSULE_DIRECTORY CAPSULE_FILE SHA256 IDENTITY_RECOVERY");
    }
    let repository_id =
        env::var("VAULTICDB_REPOSITORY_ID").context("VAULTICDB_REPOSITORY_ID is required")?;
    let capsule_directory = PathBuf::from(&arguments[1]);
    let encoded = std::fs::read(&arguments[2])
        .with_context(|| format!("read recovery capsule {}", arguments[2]))?;
    let identity_recovery = arguments[4]
        .parse::<bool>()
        .context("IDENTITY_RECOVERY must be true or false")?;
    let capsule = validate_capsule_mutation(
        &capsule_directory,
        &repository_id,
        &encoded,
        &arguments[3],
        identity_recovery,
    )?;
    let (_, object_store) = storage::object_store(&repository_id)?;
    let mirror_path =
        encryption::recovery_capsule::publish_mirror(object_store.as_ref(), &capsule).await?;
    let local_path = encryption::recovery_capsule::publish_local(&capsule_directory, &capsule)?;
    println!(
        "{}",
        serde_json::json!({
            "generation": capsule.header.generation,
            "local_path": local_path,
            "mirror_path": mirror_path,
            "capsule_sha256": arguments[3],
        })
    );
    Ok(())
}

impl Service {
    async fn transition_to_reader(
        &self,
        timeout: Duration,
        reason: String,
        force: bool,
    ) -> Result<WriterStatusResponse, Status> {
        let _transition = self.state.writer_transition.lock().await;
        {
            let mut role = self.state.writer_role.lock().await;
            role.begin_demotion(Instant::now(), reason, force)
                .map_err(role_error)?;
        }
        let drain = async {
            while self.storage.active_transactions().await != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(timeout, drain).await.is_err() {
            self.state
                .writer_role
                .lock()
                .await
                .fail_demotion(Instant::now());
            return Err(Status::deadline_exceeded(
                "writer demotion quiescence timed out",
            ));
        }
        match self.storage.demote().await {
            Ok(()) => self
                .state
                .writer_role
                .lock()
                .await
                .complete_demotion(Instant::now())
                .map_err(role_error)?,
            Err(error) => {
                self.state
                    .writer_role
                    .lock()
                    .await
                    .fail_demotion(Instant::now());
                return Err(Status::failed_precondition(format!(
                    "writer demotion failed: {error:#}"
                )));
            }
        }
        Ok(self.writer_status_response().await)
    }

    async fn write_intent(&self) -> Result<WriteIntentGuard, Status> {
        if !self
            .storage
            .mutations_allowed(&self.state.repository_id)
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?
        {
            return Err(Status::failed_precondition(
                "metadata generation mutation interlock is active",
            ));
        }
        self.authority_intent().await
    }

    async fn authority_intent(&self) -> Result<WriteIntentGuard, Status> {
        self.storage
            .ensure_writer_fence()
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        self.state
            .writer_role
            .lock()
            .await
            .admit_write()
            .map_err(role_error)?;
        Ok(WriteIntentGuard {
            writer_role: self.state.writer_role.clone(),
            last_writer_activity: self.state.last_writer_activity.clone(),
        })
    }

    async fn with_write_intent<T, F>(&self, operation: F) -> Result<T, Status>
    where
        F: Future<Output = Result<T, Status>>,
    {
        self.state
            .writer_role
            .lock()
            .await
            .admit_write()
            .map_err(role_error)?;
        let result = operation.await;
        self.state.writer_role.lock().await.finish_write();
        *self.state.last_writer_activity.lock().await = Instant::now();
        result
    }

    async fn writer_status_response(&self) -> WriterStatusResponse {
        let status = self.state.writer_role.lock().await.status();
        let transition_unix_ms = self.state.clock_started_unix_ms.saturating_add(
            status
                .transition_started
                .saturating_duration_since(self.state.clock_started)
                .as_millis()
                .min(i64::MAX as u128) as i64,
        );
        let idle_deadline_unix_ms = match self.state.writer_idle_grace {
            Some(grace) => {
                let activity = *self.state.last_writer_activity.lock().await;
                self.state.clock_started_unix_ms.saturating_add(
                    activity
                        .saturating_duration_since(self.state.clock_started)
                        .saturating_add(grace)
                        .as_millis()
                        .min(i64::MAX as u128) as i64,
                )
            }
            None => 0,
        };
        WriterStatusResponse {
            instance_id: self.state.daemon_id.to_string(),
            role: match status.role {
                CoreWriterRole::ReadOnly => proto::WriterRole::ReadOnly as i32,
                CoreWriterRole::Promoting => proto::WriterRole::Promoting as i32,
                CoreWriterRole::ReadWrite => proto::WriterRole::ReadWrite as i32,
                CoreWriterRole::Demoting => proto::WriterRole::Demoting as i32,
                CoreWriterRole::Fenced => proto::WriterRole::Fenced as i32,
            },
            current_epoch: status.current_epoch,
            observed_epoch: status.observed_epoch,
            transition_reason: status.transition_reason,
            transition_unix_ms,
            active_write_intents: status.active_write_intents,
            active_transactions: self.storage.active_transactions().await as u64,
            last_durable_sequence: self.storage.last_durable_sequence(),
            idle_deadline_unix_ms,
            promotion_safe: status.promotion_safe,
        }
    }

    fn check_key_request<T>(
        &self,
        request: &Request<T>,
        repository_id: &str,
    ) -> Result<(), Status> {
        check_request(&self.state, request, repository_id)?;
        if !self.state.unix_socket {
            return Err(Status::failed_precondition(
                "key management is available only over a private Unix socket",
            ));
        }
        Ok(())
    }

    async fn key_status_response(&self) -> Result<KeyStatusResponse, Status> {
        let (envelope_generation, active_dek_version, slots) =
            self.storage.key_manager()?.status().await;
        let (pending_capsule_migration_sha256, finalized_capsule_migration_sha256) =
            self.storage.capsule_migration_status().await?;
        Ok(KeyStatusResponse {
            envelope_generation,
            active_dek_version,
            slots: slots
                .into_iter()
                .map(|slot| KeySlotInfo {
                    id: slot.id,
                    provider: slot.provider,
                    priority: slot.priority,
                    recovery: slot.recovery,
                    key_reference: slot.key_reference,
                    dek_version: slot.dek_version,
                })
                .collect(),
            pending_capsule_migration_sha256: pending_capsule_migration_sha256.unwrap_or_default(),
            finalized_capsule_migration_sha256: finalized_capsule_migration_sha256
                .unwrap_or_default(),
        })
    }
}

fn key_management_error(error: anyhow::Error) -> Status {
    VaulticDbError::key_management(error).into()
}

fn role_error(error: RoleError) -> Status {
    VaulticDbError::from(error).into()
}

fn unix_time_ms_i64() -> Result<i64, Status> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time is before Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| Status::internal("system time exceeds signed milliseconds"))
}

fn configured_duration(
    name: &str,
    default: Duration,
    allow_disabled: bool,
) -> Result<Option<Duration>> {
    let Ok(value) = env::var(name) else {
        return Ok((!allow_disabled || !default.is_zero()).then_some(default));
    };
    let value = value.trim();
    if allow_disabled && (value.is_empty() || value == "0" || value.eq_ignore_ascii_case("off")) {
        return Ok(None);
    }
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        bail!("{name} must use an ms, s, m, or h suffix")
    };
    let milliseconds = number
        .parse::<u64>()
        .with_context(|| format!("parse {name}"))?
        .checked_mul(multiplier)
        .with_context(|| format!("{name} is too large"))?;
    if milliseconds == 0 {
        bail!("{name} must be positive or explicitly disabled")
    }
    Ok(Some(Duration::from_millis(milliseconds)))
}

fn cloud_token(value: Vec<u8>) -> Result<Option<String>, Status> {
    let value = Zeroizing::new(value);
    if value.is_empty() {
        return Ok(None);
    }
    String::from_utf8(value.to_vec())
        .map(Some)
        .map_err(|_| Status::invalid_argument("cloud bearer token is not valid UTF-8"))
}

fn check_storage_request<T>(
    state: &DaemonState,
    request: &Request<T>,
    context: Option<&RequestContext>,
) -> Result<(), Status> {
    check_request(state, request, "")?;
    check_context(context)?;
    if state.draining.load(Ordering::Acquire) {
        return Err(Status::unavailable("vaulticdb is draining"));
    }
    Ok(())
}

fn check_context(context: Option<&RequestContext>) -> Result<(), Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("request context is required"))?;
    if context.request_id.is_empty() {
        return Err(Status::invalid_argument("request ID is required"));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time is before Unix epoch"))?
        .as_millis() as i64;
    if context.deadline_unix_ms > 0 && context.deadline_unix_ms <= now {
        return Err(Status::deadline_exceeded("request deadline has expired"));
    }
    Ok(())
}

pub fn validate_write_batch(request: &WriteBatchRequest) -> Result<(), Status> {
    let item_count = request
        .puts
        .len()
        .checked_add(request.deletes.len())
        .ok_or_else(|| Status::resource_exhausted("batch item count overflow"))?;
    if item_count > MAX_BATCH_ITEMS as usize {
        return Err(Status::resource_exhausted("batch item limit exceeded"));
    }
    if request.encoded_len() > MAX_MESSAGE_BYTES as usize {
        return Err(Status::resource_exhausted("batch byte limit exceeded"));
    }
    Ok(())
}

pub fn validate_scan(request: &ScanRequest) -> Result<(), Status> {
    if request.page_size == 0 || request.page_size > MAX_PAGE_ITEMS {
        return Err(Status::invalid_argument(
            "scan page size is outside the supported range",
        ));
    }
    Ok(())
}

fn check_repository(state: &DaemonState, requested: &str) -> Result<(), Status> {
    if requested.is_empty() || requested == state.repository_id.as_ref() {
        return Ok(());
    }
    Err(Status::failed_precondition("repository identity mismatch"))
}

fn check_request<T>(
    state: &DaemonState,
    request: &Request<T>,
    repository_id: &str,
) -> Result<(), Status> {
    if let Some(token) = &state.auth_token {
        let expected = format!("Bearer {}", token.as_str());
        if request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
        {
            return Err(Status::unauthenticated("invalid vaulticdb authorization"));
        }
    }
    check_repository(state, repository_id)
}

#[derive(Debug)]
enum Transport {
    Unix(PathBuf),
    Tcp(SocketAddr, Vec<IpNet>),
}

fn parse_transport(repository_id: &str, has_auth_token: bool) -> Result<Transport> {
    let transport = env::var("VAULTICDB_TRANSPORT").unwrap_or_else(|_| "unix".to_owned());
    match transport.as_str() {
        "unix" => Ok(Transport::Unix(PathBuf::from(
            env::var("VAULTICDB_SOCKET").unwrap_or_else(|_| default_socket_path(repository_id)),
        ))),
        "tcp" => {
            if env::var("VAULTICDB_TCP_ALLOWLIST")
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                bail!("VAULTICDB_TCP_ALLOWLIST is required when TCP transport is enabled")
            }
            if !has_auth_token {
                bail!("a TCP authentication token is required when TCP transport is enabled")
            }
            let addr =
                env::var("VAULTICDB_TCP_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_owned());
            let allowlist = env::var("VAULTICDB_TCP_ALLOWLIST")?
                .split(',')
                .map(|value| value.trim().parse().context("invalid IP allowlist entry"))
                .collect::<Result<Vec<IpNet>>>()?;
            Ok(Transport::Tcp(
                addr.parse().context("invalid VAULTICDB_TCP_ADDR")?,
                allowlist,
            ))
        }
        other => bail!("unsupported VAULTICDB_TRANSPORT {other:?}; expected unix or tcp"),
    }
}

fn default_socket_path(repository_id: &str) -> String {
    let runtime_dir =
        env::var("VAULTICDB_RUNTIME_DIR").unwrap_or_else(|_| "/tmp/vaulticdb".to_owned());
    let digest = Sha256::digest(if repository_id.is_empty() {
        b"default"
    } else {
        repository_id.as_bytes()
    });
    format!("{runtime_dir}/{digest:x}.sock")
}

fn repository_key(repository_id: &str) -> String {
    let digest = Sha256::digest(if repository_id.is_empty() {
        b"default"
    } else {
        repository_id.as_bytes()
    });
    format!("{digest:x}")
}

fn read_auth_token() -> Result<Option<Zeroizing<String>>> {
    let Some(descriptor) = env::var_os("VAULTICDB_TCP_AUTH_TOKEN_FD") else {
        return Ok(None);
    };
    unsafe { env::remove_var("VAULTICDB_TCP_AUTH_TOKEN_FD") };
    let descriptor: RawFd = descriptor
        .to_string_lossy()
        .parse()
        .context("invalid TCP authentication-token descriptor")?;
    if descriptor < 3 {
        bail!("TCP authentication-token descriptor must not be a standard stream")
    }
    let mut input = unsafe { File::from_raw_fd(descriptor) }.take(64 * 1024 + 1);
    let mut token = Zeroizing::new(String::new());
    input
        .read_to_string(&mut token)
        .context("read TCP authentication token")?;
    if token.is_empty() || token.len() > 64 * 1024 {
        bail!("TCP authentication token must contain between 1 and 65536 bytes")
    }
    Ok(Some(token))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    disable_core_dumps();
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "publish-capsule")
    {
        return publish_capsule_without_database(&arguments).await;
    }
    if env::var_os("VAULTICDB_NATIVE_SMOKE").is_some() {
        return native_smoke().await;
    }

    let repository_id = env::var("VAULTICDB_REPOSITORY_ID").unwrap_or_default();
    let auth_token = read_auth_token()?;
    let transport = parse_transport(&repository_id, auth_token.is_some())?;
    let clock_started = Instant::now();
    let minimum_writer_tenure = configured_duration(
        "VAULTICDB_WRITER_MINIMUM_TENURE",
        Duration::from_secs(30),
        false,
    )?
    .expect("minimum writer tenure has a default");
    let writer_idle_grace =
        configured_duration("VAULTICDB_WRITER_IDLE_GRACE", Duration::ZERO, true)?;
    let writer_transition_timeout = configured_duration(
        "VAULTICDB_WRITER_TRANSITION_TIMEOUT",
        Duration::from_secs(30),
        false,
    )?
    .expect("writer transition timeout has a default");
    let mut state = DaemonState {
        daemon_id: Arc::from(
            env::var("VAULTICDB_DAEMON_ID").unwrap_or_else(|_| "vaulticdb-dev".to_owned()),
        ),
        repository_id: Arc::from(repository_id),
        auth_token: auth_token.map(Arc::new),
        unix_socket: matches!(&transport, Transport::Unix(_)),
        tcp_enabled: matches!(&transport, Transport::Tcp(_, _)),
        draining: Arc::new(AtomicBool::new(false)),
        writer_role: Arc::new(Mutex::new(WriterRoleState::read_write(
            1,
            clock_started,
            minimum_writer_tenure,
        ))),
        writer_transition: Arc::new(Mutex::new(())),
        last_writer_activity: Arc::new(Mutex::new(clock_started)),
        writer_idle_grace,
        writer_transition_timeout,
        clock_started,
        clock_started_unix_ms: unix_time_ms_i64()?,
    };
    let tcp_enabled = matches!(transport, Transport::Tcp(_, _));
    let (shutdown, shutdown_rx) = watch::channel(false);

    match transport {
        Transport::Unix(path) => {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
                set_private_directory_permissions(parent)?;
            }
            let lock_path = path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            remove_stale_socket(&path).await?;
            write_runtime_metadata(&path, false)?;
            let listener = match UnixListener::bind(&path) {
                Ok(listener) => listener,
                Err(error) => {
                    remove_runtime_metadata(&path);
                    return Err(error)
                        .with_context(|| format!("bind Unix socket {}", path.display()));
                }
            };
            if let Err(error) = set_private_socket_permissions(&path) {
                let _ = tokio::fs::remove_file(&path).await;
                remove_runtime_metadata(&path);
                return Err(error);
            }
            let storage = Arc::new(Storage::open(state.repository_id.as_ref()).await?);
            let (is_writer, epoch) = storage.writer_status_epoch().await;
            state.writer_role = Arc::new(Mutex::new(if is_writer {
                WriterRoleState::read_write(epoch, clock_started, minimum_writer_tenure)
            } else {
                WriterRoleState::read_only(epoch, clock_started, minimum_writer_tenure)
            }));
            monitor_broker_lease(storage.as_ref(), shutdown.clone());
            let service = storage_service(state.clone(), shutdown.clone(), storage.clone());
            let stream = UnixListenerStream::new(listener);
            let result = Server::builder()
                .concurrency_limit_per_connection(MAX_CONCURRENT_REQUESTS)
                .add_service(service)
                .serve_with_incoming_shutdown(stream, shutdown_signal(shutdown_rx))
                .await;
            let close_result = storage.close().await;
            drop(_lock);
            let _ = tokio::fs::remove_file(&path).await;
            remove_runtime_metadata(&path);
            result?;
            close_result?;
        }
        Transport::Tcp(addr, allowlist) => {
            let metadata_path = env::var("VAULTICDB_TCP_METADATA")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    PathBuf::from(
                        env::var("VAULTICDB_RUNTIME_DIR")
                            .unwrap_or_else(|_| "/tmp/vaulticdb".to_owned()),
                    )
                    .join("vaulticdb-tcp")
                });
            let listener = TcpListener::bind(addr).await.context("bind TCP listener")?;
            if let Some(parent) = metadata_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
                set_private_directory_permissions(parent)?;
            }
            let lock_path = metadata_path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            write_runtime_metadata(&metadata_path, tcp_enabled)?;
            let storage = Arc::new(Storage::open(state.repository_id.as_ref()).await?);
            let (is_writer, epoch) = storage.writer_status_epoch().await;
            state.writer_role = Arc::new(Mutex::new(if is_writer {
                WriterRoleState::read_write(epoch, clock_started, minimum_writer_tenure)
            } else {
                WriterRoleState::read_only(epoch, clock_started, minimum_writer_tenure)
            }));
            monitor_broker_lease(storage.as_ref(), shutdown.clone());
            let service = storage_service(state, shutdown, storage.clone());
            let (sender, receiver) = mpsc::channel(64);
            tokio::spawn(accept_allowed_tcp(listener, allowlist, sender));
            let result = Server::builder()
                .concurrency_limit_per_connection(MAX_CONCURRENT_REQUESTS)
                .add_service(service)
                .serve_with_incoming_shutdown(
                    ReceiverStream::new(receiver),
                    shutdown_signal(shutdown_rx),
                )
                .await;
            let close_result = storage.close().await;
            remove_runtime_metadata(&metadata_path);
            result?;
            close_result?;
        }
    }
    Ok(())
}

fn monitor_broker_lease(storage: &Storage, shutdown: watch::Sender<bool>) {
    let Some((mut disconnected, expires_unix_ms)) = storage.broker_lease_monitor() else {
        return;
    };
    tokio::spawn(async move {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(expires_unix_ms);
        let until_expiry = std::time::Duration::from_millis(expires_unix_ms.saturating_sub(now));
        tokio::select! {
            _ = disconnected.changed() => {}
            _ = tokio::time::sleep(until_expiry) => {}
        }
        let _ = shutdown.send(true);
    });
}

fn disable_core_dumps() {
    #[cfg(unix)]
    unsafe {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        libc::setrlimit(libc::RLIMIT_CORE, &limit);
    }
}

fn storage_service(
    state: DaemonState,
    shutdown: watch::Sender<bool>,
    storage: Arc<Storage>,
) -> VaulticDbServer<Service> {
    let service = Service {
        state,
        shutdown,
        storage,
    };
    if let Some(grace) = service.state.writer_idle_grace {
        let idle_service = service.clone();
        tokio::spawn(async move {
            let poll = grace
                .min(Duration::from_secs(1))
                .max(Duration::from_millis(100));
            let mut interval = tokio::time::interval(poll);
            loop {
                interval.tick().await;
                let status = idle_service.state.writer_role.lock().await.status();
                let last_activity = *idle_service.state.last_writer_activity.lock().await;
                if status.role == CoreWriterRole::ReadWrite
                    && status.active_write_intents == 0
                    && status.active_transactions == 0
                    && Instant::now().saturating_duration_since(last_activity) >= grace
                {
                    let _ = idle_service
                        .transition_to_reader(
                            idle_service.state.writer_transition_timeout,
                            "configured idle grace elapsed".to_owned(),
                            false,
                        )
                        .await;
                }
            }
        });
    }
    VaulticDbServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES as usize)
        .max_encoding_message_size(MAX_MESSAGE_BYTES as usize)
}

fn write_runtime_metadata(socket: &Path, tcp_enabled: bool) -> Result<()> {
    let pid_path = socket.with_extension("pid");
    let cap_path = socket.with_extension("cap");
    std::fs::write(pid_path, format!("{}\n", std::process::id()))?;
    std::fs::write(
        cap_path,
        format!(
            "protocol={PROTOCOL_VERSION}\nschema={SCHEMA_VERSION}\ntcp_enabled={tcp_enabled}\n"
        ),
    )?;
    Ok(())
}

fn remove_runtime_metadata(socket: &Path) {
    let _ = std::fs::remove_file(socket.with_extension("pid"));
    let _ = std::fs::remove_file(socket.with_extension("cap"));
}

async fn remove_stale_socket(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        bail!("refusing to replace non-socket endpoint {}", path.display());
    }
    match tokio::net::UnixStream::connect(path).await {
        Ok(_) => bail!("vaulticdb endpoint {} is already active", path.display()),
        Err(_) => {
            tokio::fs::remove_file(path).await?;
            Ok(())
        }
    }
}

async fn accept_allowed_tcp(
    listener: TcpListener,
    allowlist: Vec<IpNet>,
    sender: mpsc::Sender<Result<tokio::net::TcpStream, io::Error>>,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = sender.send(Err(error)).await;
                return;
            }
        };
        if allowlist.iter().any(|network| network.contains(&peer.ip()))
            && sender.send(Ok(stream)).await.is_err()
        {
            return;
        }
    }
}

async fn native_smoke() -> Result<()> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("vaulticdb-phase0-smoke", object_store.clone()).await?;

    let mut batch = WriteBatch::new();
    batch.put(b"phase0/key", b"phase0/value");
    let write = db.write(batch).await?;
    write.await_durable().await?;
    db.close().await?;

    let reader_options = DbReaderOptions {
        skip_wal_replay: true,
        ..Default::default()
    };
    let reader = DbReader::open(
        "vaulticdb-phase0-smoke",
        object_store,
        DbReaderMode::FollowLatest,
        reader_options,
    )
    .await?;
    let value = reader.get(b"phase0/key").await?;
    if value.as_deref() != Some(b"phase0/value".as_slice()) {
        bail!("native SlateDB smoke read returned an unexpected value")
    }
    reader.close().await?;
    println!("vaulticdb native SlateDB smoke ok");
    Ok(())
}

async fn shutdown_signal(mut requested: watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = requested.changed() => {}
    }
}

fn acquire_singleton_lock(path: &Path) -> Result<File> {
    let lock = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .or_else(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                std::fs::OpenOptions::new().write(true).open(path)
            } else {
                Err(error)
            }
        })
        .with_context(|| format!("open vaulticdb singleton lock {}", path.display()))?;
    lock.try_lock_exclusive()
        .with_context(|| format!("acquire vaulticdb singleton lock {}", path.display()))?;
    Ok(lock)
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_socket_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o600);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_socket_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::IntoRawFd;
    use std::os::unix::net::UnixStream as StdUnixStream;
    use std::sync::{Mutex, OnceLock};
    use vaulticdb::encryption::recovery_capsule::{
        publish_local, CapsuleBuilder, MemberCredential,
    };

    #[test]
    fn core_dumps_are_disabled() {
        disable_core_dumps();
        let mut limit = libc::rlimit {
            rlim_cur: libc::RLIM_INFINITY,
            rlim_max: libc::RLIM_INFINITY,
        };
        assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) }, 0);
        assert_eq!(limit.rlim_cur, 0);
        assert_eq!(limit.rlim_max, 0);
    }

    fn transport_environment_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn unix_is_the_default_transport() {
        let _guard = transport_environment_lock().lock().unwrap();
        for key in [
            "VAULTICDB_TRANSPORT",
            "VAULTICDB_SOCKET",
            "VAULTICDB_TCP_ALLOWLIST",
            "VAULTICDB_TCP_AUTH_TOKEN_FD",
        ] {
            unsafe { env::remove_var(key) };
        }
        assert!(
            matches!(parse_transport("", false).unwrap(), Transport::Unix(path) if path == PathBuf::from(default_socket_path("")).as_path())
        );
    }

    #[test]
    fn capsule_migration_proof_binds_key_repository_and_digest() {
        let mut proof = Hmac::<Sha256>::new_from_slice(b"repository-key").unwrap();
        proof.update(b"vaultic-capsule-migration-finalize-v1\0repository-a\0");
        proof.update("ab".repeat(32).as_bytes());
        let proof = proof.finalize().into_bytes();
        assert!(verify_capsule_migration_proof(
            b"repository-key",
            "repository-a",
            &"ab".repeat(32),
            &proof
        )
        .is_ok());
        assert!(verify_capsule_migration_proof(
            b"wrong-key",
            "repository-a",
            &"ab".repeat(32),
            &proof
        )
        .is_err());
        assert!(verify_capsule_migration_proof(
            b"repository-key",
            "repository-b",
            &"ab".repeat(32),
            &proof
        )
        .is_err());
        assert!(verify_capsule_migration_proof(
            b"repository-key",
            "repository-a",
            &"cd".repeat(32),
            &proof
        )
        .is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn identity_recovery_publishes_capsule_without_opening_database() {
        let _guard = transport_environment_lock().lock().unwrap();
        let root = env::temp_dir().join(format!(
            "vaultic-capsule-publisher-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&root).unwrap();
        let capsule_directory = root.join("capsules");
        let credentials = [
            ("alice", MemberCredential::Passphrase(b"alice passphrase")),
            ("bob", MemberCredential::Passphrase(b"bob passphrase")),
        ];
        let current = CapsuleBuilder::new("repo-a", 1)
            .broker_identity_public_key(&[1; 32])
            .create_offline_threshold("operators", 2, &credentials, &[7; 32], b"master-key")
            .unwrap();
        publish_local(&capsule_directory, &current).unwrap();
        let candidate = CapsuleBuilder::new("repo-a", 2)
            .broker_identity_public_key(&[2; 32])
            .create_offline_threshold("operators", 2, &credentials, &[7; 32], b"master-key")
            .unwrap();
        let encoded = serde_json::to_vec(&candidate).unwrap();
        let digest = format!("{:x}", Sha256::digest(&encoded));
        let candidate_path = root.join("candidate.json");
        std::fs::write(&candidate_path, encoded).unwrap();
        unsafe {
            env::set_var("VAULTICDB_REPOSITORY_ID", "repo-a");
            env::set_var("VAULTICDB_OBJECT_STORE", "memory");
        }
        let arguments = vec![
            "publish-capsule".to_owned(),
            capsule_directory.display().to_string(),
            candidate_path.display().to_string(),
            digest,
            "true".to_owned(),
        ];
        publish_capsule_without_database(&arguments).await.unwrap();
        publish_capsule_without_database(&arguments).await.unwrap();
        let (_, published) =
            encryption::recovery_capsule::discover_latest(&capsule_directory, "repo-a")
                .unwrap()
                .unwrap();
        assert_eq!(published, candidate);
        unsafe {
            env::remove_var("VAULTICDB_REPOSITORY_ID");
            env::remove_var("VAULTICDB_OBJECT_STORE");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tcp_requires_authentication_and_allowlist() {
        let _guard = transport_environment_lock().lock().unwrap();
        unsafe { env::set_var("VAULTICDB_TRANSPORT", "tcp") };
        unsafe { env::remove_var("VAULTICDB_TCP_ALLOWLIST") };
        unsafe { env::remove_var("VAULTICDB_TCP_AUTH_TOKEN_FD") };
        assert!(parse_transport("", false).is_err());
        unsafe { env::set_var("VAULTICDB_TCP_ALLOWLIST", "127.0.0.1/32,::1/128") };
        assert!(parse_transport("", false).is_err());
        assert!(
            matches!(parse_transport("", true).unwrap(), Transport::Tcp(_, networks) if networks.len() == 2)
        );
        for key in [
            "VAULTICDB_TRANSPORT",
            "VAULTICDB_TCP_ALLOWLIST",
            "VAULTICDB_TCP_AUTH_TOKEN_FD",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    fn tcp_authentication_descriptor_is_consumed_and_closed() {
        let _guard = transport_environment_lock().lock().unwrap();
        let (mut writer, reader) = StdUnixStream::pair().unwrap();
        writer.write_all(b"test-token").unwrap();
        drop(writer);
        let descriptor = reader.into_raw_fd();
        unsafe { env::set_var("VAULTICDB_TCP_AUTH_TOKEN_FD", descriptor.to_string()) };
        let token = read_auth_token().unwrap().unwrap();
        assert_eq!(token.as_str(), "test-token");
        assert!(env::var_os("VAULTICDB_TCP_AUTH_TOKEN_FD").is_none());
        assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EBADF)
        );
    }

    #[test]
    fn singleton_lock_recovers_after_previous_process_exit() {
        let directory = env::temp_dir().join(format!("vaulticdb-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("vaulticdb.lock");
        let first = acquire_singleton_lock(&path).unwrap();
        assert!(acquire_singleton_lock(&path).is_err());
        drop(first);
        assert!(acquire_singleton_lock(&path).is_ok());
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn future_storage_envelopes_enforce_advertised_limits() {
        let mut batch = WriteBatchRequest {
            deletes: vec![Vec::new(); MAX_BATCH_ITEMS as usize],
            ..Default::default()
        };
        assert!(validate_write_batch(&batch).is_ok());
        batch.deletes.push(Vec::new());
        assert_eq!(
            validate_write_batch(&batch).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );

        let oversized = WriteBatchRequest {
            deletes: vec![vec![0; MAX_MESSAGE_BYTES as usize]],
            ..Default::default()
        };
        assert_eq!(
            validate_write_batch(&oversized).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert!(validate_scan(&ScanRequest {
            page_size: MAX_PAGE_ITEMS,
            ..Default::default()
        })
        .is_ok());
        assert!(validate_scan(&ScanRequest::default()).is_err());
        assert!(validate_scan(&ScanRequest {
            page_size: MAX_PAGE_ITEMS + 1,
            ..Default::default()
        })
        .is_err());
    }
}
