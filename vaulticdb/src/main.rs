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

include!("service/operations.rs");

include!("transport.rs");

include!("service/tests.rs");
