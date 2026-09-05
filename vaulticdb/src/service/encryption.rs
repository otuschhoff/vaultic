//! gRPC handlers for encryption keys, envelopes, escrow, and capsules.

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
};
use vaulticdb::encryption;

use super::{
    check_context, check_request, validate_capsule_mutation, verify_capsule_migration_proof,
    Service,
};

pub(super) fn key_management_error(error: anyhow::Error) -> Status {
    VaulticDbError::key_management(error).into()
}

pub(super) fn cloud_token(value: Vec<u8>) -> Result<Option<String>, Status> {
    let value = Zeroizing::new(value);
    if value.is_empty() {
        return Ok(None);
    }
    String::from_utf8(value.to_vec())
        .map(Some)
        .map_err(|_| Status::invalid_argument("cloud bearer token is not valid UTF-8"))
}

impl Service {
    pub(super) async fn handle_publish_capsule_mutation(
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

    pub(super) async fn handle_prepare_capsule_migration(
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

    pub(super) async fn handle_finalize_capsule_migration(
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

    pub(super) async fn handle_check_encryption(
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

    pub(super) async fn handle_export_key_envelope(
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

    pub(super) async fn handle_escrow_master_key(
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

    pub(super) async fn handle_recover_escrow(
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

    pub(super) async fn handle_rewrite_dek(
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

    pub(super) async fn handle_rotate_dek(
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

    pub(super) async fn handle_add_cloud_key_slot(
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

    pub(super) async fn handle_key_status(
        &self,
        request: Request<KeyStatusRequest>,
    ) -> Result<Response<KeyStatusResponse>, Status> {
        self.check_key_request(&request, request.get_ref().repository_id.as_str())?;
        Ok(Response::new(self.key_status_response().await?))
    }

    pub(super) async fn handle_add_local_key_slot(
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

    pub(super) async fn handle_remove_key_slot(
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

    pub(super) async fn handle_rotate_local_key_slot(
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

    pub(super) async fn handle_store_master_key(
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
}
