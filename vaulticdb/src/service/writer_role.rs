//! Writer-role status, promotion, demotion, and idle-yield handlers.

use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};

use crate::{
    error::VaulticDbError,
    proto::{DemoteWriterRequest, PromoteWriterRequest, WriterStatusRequest, WriterStatusResponse},
};
use vaulticdb::writer_role::RoleError;

use super::{check_context, check_request, Service};

pub(super) fn role_error(error: RoleError) -> Status {
    VaulticDbError::from(error).into()
}

pub(crate) fn unix_time_ms_i64() -> Result<i64, Status> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time is before Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| Status::internal("system time exceeds signed milliseconds"))
}

impl Service {
    pub(super) async fn handle_writer_status(
        &self,
        request: Request<WriterStatusRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        Ok(Response::new(self.writer_status_response().await))
    }

    pub(super) async fn handle_demote_writer(
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

    pub(super) async fn handle_promote_writer(
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
}
