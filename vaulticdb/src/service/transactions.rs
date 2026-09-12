//! Transaction and write-batch gRPC handlers.

use prost::Message;
use std::time::Instant;
use tonic::{Request, Response, Status};

use crate::{
    proto::{
        BeginResponse, CommitResponse, Empty, TransactionRequest, WriteBatchRequest,
        WriteBatchResponse,
    },
    MAX_BATCH_ITEMS, MAX_MESSAGE_BYTES,
};

use super::{check_storage_request, role_error, Service};

pub(crate) fn validate_write_batch(request: &WriteBatchRequest) -> Result<(), Status> {
    let item_count = request
        .puts
        .len()
        .checked_add(request.deletes.len())
        .ok_or_else(|| {
            Status::from(crate::error::VaulticDbError::ResourceExhausted {
                message: "batch item count overflow".to_owned(),
                retryable: false,
            })
        })?;
    if item_count > MAX_BATCH_ITEMS as usize {
        return Err(crate::error::VaulticDbError::ResourceExhausted {
            message: "batch item limit exceeded".to_owned(),
            retryable: false,
        }
        .into());
    }
    if request.encoded_len() > MAX_MESSAGE_BYTES as usize {
        return Err(crate::error::VaulticDbError::ResourceExhausted {
            message: "batch byte limit exceeded".to_owned(),
            retryable: false,
        }
        .into());
    }
    Ok(())
}

impl Service {
    pub(super) async fn handle_write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        validate_write_batch(request.get_ref())?;
        let storage = self.storage().await?;
        let durable = self
            .with_write_intent(storage.write_batch(request.get_ref()))
            .await?;
        Ok(Response::new(WriteBatchResponse { durable }))
    }

    pub(super) async fn handle_begin(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<BeginResponse>, Status> {
        let service = self.clone();
        tokio::spawn(async move { service.begin_inner(request).await })
            .await
            .map_err(|error| Status::internal(format!("join begin transaction: {error}")))?
    }

    async fn begin_inner(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<BeginResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let _admission = self.mutation_admission().await?;
        let storage = self.storage().await?;
        self.ensure_writer_authority().await?;
        self.state
            .writer_role
            .lock()
            .await
            .transaction_opened()
            .map_err(role_error)?;
        let outcome = match storage.begin().await {
            Ok(outcome) => outcome,
            Err(failure) => {
                let mut role = self.state.writer_role.lock().await;
                role.transaction_closed();
                for _ in 0..failure.expired {
                    role.transaction_closed();
                }
                drop(role);
                self.ensure_writer_authority().await?;
                return Err(failure.status);
            }
        };
        for _ in 0..outcome.expired {
            self.state.writer_role.lock().await.transaction_closed();
        }
        *self.state.last_writer_activity.lock().await = Instant::now();
        Ok(Response::new(BeginResponse {
            transaction_id: outcome.transaction_id,
        }))
    }

    pub(super) async fn handle_commit(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let service = self.clone();
        tokio::spawn(async move { service.commit_inner(request).await })
            .await
            .map_err(|error| Status::internal(format!("join commit transaction: {error}")))?
    }

    async fn commit_inner(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let _admission = self.mutation_admission().await?;
        let storage = self.storage().await?;
        self.ensure_writer_authority().await?;
        let result = storage
            .commit(
                &request.get_ref().transaction_id,
                &request.get_ref().idempotency_key,
            )
            .await;
        let consumed = match &result {
            Ok(outcome) => outcome.consumed,
            Err(failure) => failure.consumed,
        };
        if consumed {
            self.state.writer_role.lock().await.transaction_closed();
            *self.state.last_writer_activity.lock().await = Instant::now();
        }
        if result.is_err() && !consumed {
            self.ensure_writer_authority().await?;
        }
        result.map_err(|failure| failure.status)?;
        Ok(Response::new(CommitResponse { durable: true }))
    }

    pub(super) async fn handle_rollback(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<Empty>, Status> {
        let service = self.clone();
        tokio::spawn(async move { service.rollback_inner(request).await })
            .await
            .map_err(|error| Status::internal(format!("join rollback transaction: {error}")))?
    }

    async fn rollback_inner(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<Empty>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let _admission = self.mutation_admission().await?;
        let storage = self.storage().await?;
        self.ensure_writer_authority().await?;
        let result = storage.rollback(&request.get_ref().transaction_id).await;
        let consumed = match &result {
            Ok(outcome) => outcome.consumed,
            Err(failure) => failure.consumed,
        };
        if consumed {
            self.state.writer_role.lock().await.transaction_closed();
            *self.state.last_writer_activity.lock().await = Instant::now();
        }
        if result.is_err() && !consumed {
            self.ensure_writer_authority().await?;
        }
        result.map_err(|failure| failure.status)?;
        Ok(Response::new(Empty { context: None }))
    }
}
