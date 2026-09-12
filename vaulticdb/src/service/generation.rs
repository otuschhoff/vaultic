//! gRPC handlers for metadata generation authority transitions.

use std::sync::atomic::Ordering;

use tonic::{Request, Response, Status};

use crate::{
    error::VaulticDbError,
    proto::{
        ActivateGenerationRequest, GenerationStatusRequest, GenerationStatusResponse,
        QuarantineGenerationRequest, RetireGenerationRequest, RollbackGenerationRequest,
        VerifyGenerationRequest,
    },
    storage::GenerationAuthority,
};

use super::{check_context, check_request, Service};

pub(super) fn status_response(authority: GenerationAuthority) -> GenerationStatusResponse {
    GenerationStatusResponse {
        repository_id: authority.repository_id.into_string(),
        decision: authority.decision,
        active_generation: authority.active_generation,
        namespace: authority.namespace.into_string(),
        previous_generation: authority.previous_generation,
        previous_namespace: authority.previous_namespace.into_string(),
        state: authority.state.clone(),
        report_sha256: authority.report_sha256,
        decided_at_unix_ms: authority.decided_at_ms.min(i64::MAX as u64) as i64,
        observation_until_unix_ms: authority.observation_until_ms.min(i64::MAX as u64) as i64,
        retired_generation: authority.retired_generation,
        destructive_maintenance_allowed: authority.state == "healthy",
    }
}

impl Service {
    pub(super) fn check_generation_mutation<T>(
        &self,
        request: &Request<T>,
        repository_id: &str,
        context: Option<&crate::proto::RequestContext>,
    ) -> Result<(), Status> {
        check_request(&self.state, request, repository_id)?;
        check_context(context)?;
        if self.state.draining.load(Ordering::Acquire) {
            return Err(VaulticDbError::StorageUnavailable {
                message: "vaulticdb is draining".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    pub(super) async fn handle_generation_status(
        &self,
        request: Request<GenerationStatusRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let storage = self.storage().await?;
        let authority = storage
            .generation_authority(&request.get_ref().repository_id)
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_activate_generation(
        &self,
        request: Request<ActivateGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        let _transition = self.state.writer_transition.lock().await;
        self.check_generation_mutation(
            &request,
            &request.get_ref().repository_id,
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.approve {
            return Err(VaulticDbError::Precondition {
                field: "approve".to_owned(),
                message: "metadata generation activation requires explicit approval".to_owned(),
            }
            .into());
        }
        let storage = self.storage().await?;
        let authority = storage
            .activate_generation(
                &request.repository_id,
                request.expected_active_generation,
                request.candidate_generation,
                request.candidate_namespace.into(),
                request.report_sha256,
                request.observation_window_ms,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_quarantine_generation(
        &self,
        request: Request<QuarantineGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        let _transition = self.state.writer_transition.lock().await;
        self.check_generation_mutation(
            &request,
            &request.get_ref().repository_id,
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.healing_required {
            return Err(VaulticDbError::InvalidRequest {
                field: "healing_required".to_owned(),
                message: "quarantine requires a proven healing-required classification".to_owned(),
            }
            .into());
        }
        let storage = self.storage().await?;
        let authority = storage
            .quarantine_generation(
                &request.repository_id,
                request.expected_active_generation,
                request.diagnostic_sha256,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_verify_generation(
        &self,
        request: Request<VerifyGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        let _transition = self.state.writer_transition.lock().await;
        self.check_generation_mutation(
            &request,
            &request.get_ref().repository_id,
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.post_activation_check_clean {
            return Err(VaulticDbError::Precondition {
                field: "post_activation_check_clean".to_owned(),
                message: "post-activation index check did not pass".to_owned(),
            }
            .into());
        }
        let storage = self.storage().await?;
        let authority = storage
            .verify_generation(
                &request.repository_id,
                request.expected_decision,
                request.report_sha256,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_rollback_generation(
        &self,
        request: Request<RollbackGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        let service = self.clone();
        tokio::spawn(async move { service.rollback_generation_inner(request).await })
            .await
            .map_err(|error| Status::internal(format!("join generation rollback: {error}")))?
    }

    async fn rollback_generation_inner(
        &self,
        request: Request<RollbackGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        let _transition = self.state.writer_transition.lock().await;
        self.check_generation_mutation(
            &request,
            &request.get_ref().repository_id,
            request.get_ref().context.as_ref(),
        )?;
        let request = request.into_inner();
        if !request.acknowledge {
            return Err(VaulticDbError::Precondition {
                field: "acknowledge".to_owned(),
                message: "metadata generation rollback requires separate acknowledgement"
                    .to_owned(),
            }
            .into());
        }
        let admission = self.mutation_admission().await?;
        let storage = self.storage().await?;
        if let Some(authority) = storage
            .committed_generation_rollback(
                &request.repository_id,
                request.expected_decision,
                &request.report_sha256,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?
        {
            match storage.database_state().await {
                crate::storage::DatabaseState::Reader => {
                    return Ok(Response::new(status_response(authority)));
                }
                crate::storage::DatabaseState::Unavailable => {
                    return Err(VaulticDbError::StorageUnavailable {
                        message: "generation rollback is committed but storage is unavailable"
                            .to_owned(),
                    }
                    .into());
                }
                crate::storage::DatabaseState::Writer => {}
            }
            self.state.writer_role.lock().await.fence(
                storage.writer_status_epoch().await.1,
                std::time::Instant::now(),
                "generation rollback fence reconciliation",
            );
            self.transition_lifecycle(
                crate::lifecycle::DaemonPhase::Fenced,
                "generation rollback fence reconciliation",
            )
            .await?;
            let epoch = storage.refresh_writer_fence().await.map_err(|error| {
                Status::from(VaulticDbError::GenerationReconciliationPending {
                    message: format!("{error:#}"),
                    generation: authority.active_generation,
                })
            })?;
            self.state
                .writer_role
                .lock()
                .await
                .recover_fenced_writer(epoch, std::time::Instant::now())
                .map_err(super::role_error)?;
            self.transition_lifecycle(
                crate::lifecycle::DaemonPhase::ReadWrite,
                "generation rollback fence reconciled",
            )
            .await?;
            return Ok(Response::new(status_response(authority)));
        }
        let _intent = self.authority_intent_with_admission(admission).await?;
        let authority = storage
            .rollback_generation(
                &request.repository_id,
                request.expected_decision,
                request.report_sha256,
                request.observation_window_ms,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        let epoch = match storage.refresh_writer_fence().await {
            Ok(epoch) => epoch,
            Err(error) => {
                self.state.writer_role.lock().await.fence(
                    storage.writer_status_epoch().await.1,
                    std::time::Instant::now(),
                    "generation rollback committed; writer fence refresh failed",
                );
                self.transition_lifecycle(
                    crate::lifecycle::DaemonPhase::Fenced,
                    "generation rollback committed; writer fence refresh failed",
                )
                .await?;
                return Err(Status::from(
                    VaulticDbError::GenerationReconciliationPending {
                        message: format!("{error:#}"),
                        generation: authority.active_generation,
                    },
                ));
            }
        };
        self.state
            .writer_role
            .lock()
            .await
            .refresh_writer_epoch(epoch, std::time::Instant::now())
            .map_err(super::role_error)?;
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_retire_generation(
        &self,
        request: Request<RetireGenerationRequest>,
    ) -> Result<Response<GenerationStatusResponse>, Status> {
        let _transition = self.state.writer_transition.lock().await;
        self.check_generation_mutation(
            &request,
            &request.get_ref().repository_id,
            request.get_ref().context.as_ref(),
        )?;
        let _intent = self.authority_intent().await?;
        let request = request.into_inner();
        if !request.acknowledge {
            return Err(VaulticDbError::Precondition {
                field: "acknowledge".to_owned(),
                message: "metadata generation retirement requires separate acknowledgement"
                    .to_owned(),
            }
            .into());
        }
        let storage = self.storage().await?;
        let authority = storage
            .retire_generation(
                &request.repository_id,
                request.expected_decision,
                request.generation,
                request.report_sha256,
            )
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        Ok(Response::new(status_response(authority)))
    }
}
