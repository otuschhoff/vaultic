//! gRPC handlers for encryption keys, envelopes, escrow, and capsules.

use prost::Message;
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};
use zeroize::Zeroizing;

use crate::{
    error::VaulticDbError,
    proto::{
        AddCloudKeySlotRequest, AddLocalKeySlotRequest, Empty, EncryptionAuditResponse,
        EscrowMasterKeyRequest, EscrowMasterKeyResponse, ExportKeyEnvelopeResponse,
        FinalizeCapsuleMigrationRequest, KeyStatusRequest, KeyStatusResponse, MasterKeyRequest,
        MasterKeyResponse, PrepareCapsuleMigrationRequest, PrepareCapsuleMigrationResponse,
        PublishCapsuleMutationRequest, PublishCapsuleMutationResponse, RecoverEscrowRequest,
        RemoveKeySlotRequest, RewriteDekRequest, RewriteDekResponse, RotateDekRequest,
        RotateLocalKeySlotRequest, StoreMasterKeyRequest,
    },
    storage::{CapsuleMigrationIntent, Storage},
};
use vaulticdb::{
    encryption::{self, envelope::KeyManager},
    topology::TopologyDocument,
};

use super::{
    check_context, check_request, validate_capsule_mutation, verify_capsule_migration_proof,
    Service,
};

#[cfg(feature = "test-failpoints")]
fn crash_at_capsule_boundary(name: &str) {
    if std::env::var("VAULTICDB_TEST_CAPABILITY").as_deref() == Ok("vaulticdb-process-tests-v1")
        && std::env::var("VAULTICDB_TEST_CRASHPOINTS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .any(|configured| configured == name)
    {
        eprintln!("injected process crash at capsule boundary {name}");
        std::process::abort();
    }
}

pub(super) fn key_management_error(error: anyhow::Error) -> Status {
    VaulticDbError::key_management(error).into()
}

pub(super) fn cloud_token(value: Vec<u8>) -> Result<Option<String>, Status> {
    let value = Zeroizing::new(value);
    if value.is_empty() {
        return Ok(None);
    }
    String::from_utf8(value.to_vec()).map(Some).map_err(|_| {
        Status::from(VaulticDbError::InvalidRequest {
            field: "bearer_token".to_owned(),
            message: "cloud bearer token is not valid UTF-8".to_owned(),
        })
    })
}

fn capsule_migration_request_sha256(request: &PrepareCapsuleMigrationRequest) -> String {
    let mut identity_request = request.clone();
    identity_request.context = None;
    format!("{:x}", Sha256::digest(identity_request.encode_to_vec()))
}

async fn publish_capsule_migration(
    storage: &Storage,
    manager: &KeyManager,
    mut intent: CapsuleMigrationIntent,
) -> Result<PrepareCapsuleMigrationResponse, Status> {
    let capsule: encryption::recovery_capsule::RecoveryCapsule =
        serde_json::from_slice(&intent.capsule).map_err(|_| {
            Status::from(VaulticDbError::StorageDataLoss {
                message: "persisted capsule migration is invalid".to_owned(),
            })
        })?;
    let path = encryption::recovery_capsule::publish_local(
        std::path::Path::new(&intent.capsule_directory),
        &capsule,
    )
    .map_err(key_management_error)?;
    if let Some(recorded) = &intent.local_path {
        if recorded != &path.display().to_string() {
            return Err(VaulticDbError::StorageConflict {
                message: "recorded local capsule path does not match publication path".to_owned(),
                retryable: false,
            }
            .into());
        }
    } else {
        #[cfg(feature = "test-failpoints")]
        crash_at_capsule_boundary("capsule-local-visible");
        intent.local_path = Some(path.display().to_string());
        storage.write_capsule_migration_intent(&intent).await?;
        #[cfg(feature = "test-failpoints")]
        crash_at_capsule_boundary("capsule-local-recorded");
    }
    let mirror_path = manager
        .publish_capsule_mirror(&capsule)
        .await
        .map_err(key_management_error)?;
    if let Some(recorded) = &intent.mirror_path {
        if recorded != &mirror_path {
            return Err(VaulticDbError::StorageConflict {
                message: "recorded capsule mirror path does not match publication path".to_owned(),
                retryable: false,
            }
            .into());
        }
    } else {
        #[cfg(feature = "test-failpoints")]
        crash_at_capsule_boundary("capsule-mirror-visible");
        intent.mirror_path = Some(mirror_path);
        storage.write_capsule_migration_intent(&intent).await?;
        #[cfg(feature = "test-failpoints")]
        crash_at_capsule_boundary("capsule-mirror-recorded");
    }
    Ok(PrepareCapsuleMigrationResponse {
        generation: intent.generation,
        local_path: intent.local_path.unwrap_or_default(),
        mirror_path: intent.mirror_path.unwrap_or_default(),
        capsule_sha256: intent.capsule_sha256,
        capsule: intent.capsule,
    })
}

impl Service {
    pub(super) async fn handle_publish_capsule_mutation(
        &self,
        request: Request<PublishCapsuleMutationRequest>,
    ) -> Result<Response<PublishCapsuleMutationResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
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
        let storage = self.storage().await?;
        let manager = storage.key_manager()?;
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

    pub(super) async fn handle_prepare_capsule_migration(
        &self,
        request: Request<PrepareCapsuleMigrationRequest>,
    ) -> Result<Response<PrepareCapsuleMigrationResponse>, Status> {
        let _transition = self.state.writer_transition.lock().await;
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let mut request = request.into_inner();
        let request_sha256 = capsule_migration_request_sha256(&request);
        if request.threshold == 0 || request.threshold > u32::from(u8::MAX) {
            return Err(VaulticDbError::InvalidRequest {
                field: "threshold".to_owned(),
                message: "invalid capsule threshold".to_owned(),
            }
            .into());
        }
        let sealed_topology = Zeroizing::new(std::mem::take(&mut request.sealed_topology));
        let topology = TopologyDocument::decode(sealed_topology.as_slice()).map_err(|error| {
            Status::from(VaulticDbError::InvalidRequest {
                field: "sealed_topology".to_owned(),
                message: format!("invalid sealed topology: {error}"),
            })
        })?;
        if topology.repository_id != request.repository_id
            || topology.topology_generation != request.generation
        {
            return Err(VaulticDbError::InvalidRequest {
                field: "sealed_topology".to_owned(),
                message: "sealed topology identity must match the capsule migration".to_owned(),
            }
            .into());
        }
        let storage = self.storage().await?;
        let manager = storage.key_manager()?;
        if let Some(intent) = storage.capsule_migration_intent().await? {
            if intent.repository_id != request.repository_id
                || intent.generation != request.generation
                || intent.capsule_directory != request.capsule_directory
                || intent.request_sha256 != request_sha256
            {
                return Err(VaulticDbError::StorageConflict {
                    message: "a different capsule migration is already pending".to_owned(),
                    retryable: false,
                }
                .into());
            }
            return Ok(Response::new(
                publish_capsule_migration(storage.as_ref(), manager.as_ref(), intent).await?,
            ));
        }
        let audit = manager
            .audit_objects()
            .await
            .map_err(key_management_error)?;
        if audit.invalid_objects != 0
            || audit.plaintext_objects != 0
            || audit.old_version_objects != 0
        {
            return Err(VaulticDbError::Precondition {
                field: "encryption_audit".to_owned(),
                message:
                    "all metadata objects must authenticate under the active DEK before migration"
                        .to_owned(),
            }
            .into());
        }
        let (active_dek_version, metadata_dek) = manager
            .active_dek_for_migration()
            .await
            .map_err(key_management_error)?;
        let repository_master_key =
            Zeroizing::new(storage.get_master_key().await?.ok_or_else(|| {
                Status::from(VaulticDbError::Precondition {
                    field: "master_key".to_owned(),
                    message: "repository master key is not stored".to_owned(),
                })
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
                    _ => {
                        return Err(VaulticDbError::InvalidRequest {
                            field: "members.provider".to_owned(),
                            message: "migration currently accepts offline-argon2id and offline-keyfile members".to_owned(),
                        }
                        .into())
                    }
                };
                Ok((member_id.as_str(), credential))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let capsule = encryption::recovery_capsule::CapsuleBuilder::new(
            request.repository_id.clone(),
            request.generation,
            sealed_topology.as_slice(),
        )
        .broker_identity_public_key(&request.broker_identity_public_key)
        .key_versions(1, active_dek_version, 1)
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
        let mut encoded = serde_json::to_vec_pretty(&capsule)
            .map_err(|error| key_management_error(error.into()))?;
        encoded.push(b'\n');
        let capsule_sha256 = format!("{:x}", Sha256::digest(&encoded));
        let intent = storage
            .store_capsule_migration_intent(&CapsuleMigrationIntent {
                format: 1,
                repository_id: request.repository_id,
                generation: capsule.header.generation,
                capsule_directory: request.capsule_directory,
                request_sha256,
                capsule_sha256,
                capsule: encoded,
                local_path: None,
                mirror_path: None,
            })
            .await?;
        #[cfg(feature = "test-failpoints")]
        crash_at_capsule_boundary("capsule-intent-persisted");
        Ok(Response::new(
            publish_capsule_migration(storage.as_ref(), manager.as_ref(), intent).await?,
        ))
    }

    pub(super) async fn handle_finalize_capsule_migration(
        &self,
        request: Request<FinalizeCapsuleMigrationRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let storage = self.storage().await?;
        let repository_id = request.get_ref().repository_id.as_str();
        let capsule_sha256 = request.get_ref().capsule_sha256.as_str();
        match storage.get_master_key().await? {
            Some(master_key) => {
                let master_key = Zeroizing::new(master_key);
                verify_capsule_migration_proof(
                    &master_key,
                    repository_id,
                    capsule_sha256,
                    &request.get_ref().broker_key_proof,
                )?;
                let intent = storage.capsule_migration_intent().await?.ok_or_else(|| {
                    Status::from(VaulticDbError::Precondition {
                        field: "capsule_migration".to_owned(),
                        message: "capsule migration intention is not pending".to_owned(),
                    })
                })?;
                if intent.capsule_sha256 != capsule_sha256 {
                    return Err(VaulticDbError::Precondition {
                        field: "capsule_sha256".to_owned(),
                        message: "capsule migration digest does not match pending intention"
                            .to_owned(),
                    }
                    .into());
                }
                let manager = storage.key_manager()?;
                publish_capsule_migration(storage.as_ref(), manager.as_ref(), intent).await?;
                storage.finalize_capsule_migration(capsule_sha256).await?;
            }
            None => {
                storage.finalize_capsule_migration(capsule_sha256).await?;
            }
        }
        Ok(Response::new(Empty::default()))
    }

    pub(super) async fn handle_check_encryption(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<EncryptionAuditResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let storage = self.storage().await?;
        if !storage.encryption_status().enabled {
            return Ok(Response::new(EncryptionAuditResponse {
                enabled: false,
                ..Default::default()
            }));
        }
        let manager = storage.key_manager()?;
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

    pub(super) async fn handle_export_key_envelope(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<ExportKeyEnvelopeResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let storage = self.storage().await?;
        let manager = storage.key_manager()?;
        let (generation, _, _) = manager.status().await;
        Ok(Response::new(ExportKeyEnvelopeResponse {
            envelope: manager
                .export_envelope()
                .await
                .map_err(key_management_error)?,
            generation,
        }))
    }

    pub(super) async fn handle_escrow_master_key(
        &self,
        request: Request<EscrowMasterKeyRequest>,
    ) -> Result<Response<EscrowMasterKeyResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let request = request.into_inner();
        let token = cloud_token(request.bearer_token)?;
        let provider = encryption::envelope::providers::for_management(&request.provider, token)
            .await
            .map_err(key_management_error)?;
        let storage = self.storage().await?;
        let master_key = Zeroizing::new(storage.get_master_key().await?.ok_or_else(|| {
            Status::from(VaulticDbError::Precondition {
                field: "master_key".to_owned(),
                message: "repository master key is not stored".to_owned(),
            })
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

    pub(super) async fn handle_recover_escrow(
        &self,
        request: Request<RecoverEscrowRequest>,
    ) -> Result<Response<MasterKeyResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let request = request.into_inner();
        let record: encryption::envelope::EscrowRecord = serde_json::from_slice(&request.record)
            .map_err(|error| {
                Status::from(VaulticDbError::InvalidRequest {
                    field: "record".to_owned(),
                    message: format!("decode escrow record: {error}"),
                })
            })?;
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

    pub(super) async fn handle_rewrite_dek(
        &self,
        request: Request<RewriteDekRequest>,
    ) -> Result<Response<RewriteDekResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let storage = self.storage().await?;
        let (rewritten, remaining) = storage
            .key_manager()?
            .rewrite_old_deks(request.get_ref().max_objects as usize)
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(RewriteDekResponse {
            rewritten: rewritten as u64,
            remaining: remaining as u64,
        }))
    }

    pub(super) async fn handle_rotate_dek(
        &self,
        request: Request<RotateDekRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        let _transition = self.state.writer_transition.lock().await;
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let storage = self.storage().await?;
        storage
            .key_manager()?
            .rotate_dek()
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    pub(super) async fn handle_add_cloud_key_slot(
        &self,
        request: Request<AddCloudKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let request = request.into_inner();
        let token = Zeroizing::new(request.bearer_token);
        let token = if token.is_empty() {
            None
        } else {
            Some(String::from_utf8(token.to_vec()).map_err(|_| {
                Status::from(VaulticDbError::InvalidRequest {
                    field: "bearer_token".to_owned(),
                    message: "cloud bearer token is not valid UTF-8".to_owned(),
                })
            })?)
        };
        let provider = encryption::envelope::providers::for_management(&request.provider, token)
            .await
            .map_err(key_management_error)?;
        let storage = self.storage().await?;
        storage
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

    pub(super) async fn handle_key_status(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        Ok(Response::new(self.key_status_response().await?))
    }

    pub(super) async fn handle_add_local_key_slot(
        &self,
        request: Request<AddLocalKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let request = request.into_inner();
        let passphrase = Zeroizing::new(request.passphrase);
        let storage = self.storage().await?;
        storage
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

    pub(super) async fn handle_remove_key_slot(
        &self,
        request: Request<RemoveKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let slot_id = request.into_inner().slot_id;
        let storage = self.storage().await?;
        storage
            .key_manager()?
            .remove_slot(&slot_id)
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    pub(super) async fn handle_rotate_local_key_slot(
        &self,
        request: Request<RotateLocalKeySlotRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let request = request.into_inner();
        let passphrase = Zeroizing::new(request.passphrase);
        let storage = self.storage().await?;
        storage
            .key_manager()?
            .rotate_local_slot(&request.slot_id, &passphrase)
            .await
            .map_err(key_management_error)?;
        Ok(Response::new(self.key_status_response().await?))
    }

    pub(super) async fn handle_get_master_key(
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
            return Err(VaulticDbError::Precondition {
                field: "transport".to_owned(),
                message: "master-key-in-DB is available only over a private Unix socket".to_owned(),
            }
            .into());
        }
        let storage = self.storage().await?;
        let value = storage.get_master_key().await?;
        Ok(Response::new(MasterKeyResponse {
            found: value.is_some(),
            master_key: value.unwrap_or_default(),
        }))
    }

    pub(super) async fn handle_store_master_key(
        &self,
        request: Request<StoreMasterKeyRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.check_key_request(
            &request,
            request.get_ref().repository_id.as_str(),
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.write_intent().await?;
        let master_key = Zeroizing::new(request.get_ref().master_key.clone());
        let storage = self.storage().await?;
        storage.store_master_key(&master_key).await?;
        Ok(Response::new(Empty { context: None }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capsule_migration_identity_covers_threshold_and_ignores_context() {
        let mut request = PrepareCapsuleMigrationRequest {
            repository_id: "repository".to_owned(),
            generation: 4,
            threshold: 2,
            context: Some(crate::proto::RequestContext {
                request_id: "first".to_owned(),
                deadline_unix_ms: 10,
            }),
            ..Default::default()
        };
        let digest = capsule_migration_request_sha256(&request);
        request.context = Some(crate::proto::RequestContext {
            request_id: "retry".to_owned(),
            deadline_unix_ms: 20,
        });
        assert_eq!(capsule_migration_request_sha256(&request), digest);
        request.threshold = 3;
        assert_ne!(capsule_migration_request_sha256(&request), digest);
        request.threshold = 2;
        request.sealed_topology = b"different topology".to_vec();
        assert_ne!(capsule_migration_request_sha256(&request), digest);
    }
}
