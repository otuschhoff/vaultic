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

impl Service {
    pub(super) async fn handle_generation_status(
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
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_activate_generation(
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
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_quarantine_generation(
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
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_verify_generation(
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
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_rollback_generation(
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
        Ok(Response::new(status_response(authority)))
    }

    pub(super) async fn handle_retire_generation(
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
        Ok(Response::new(status_response(authority)))
    }
}
