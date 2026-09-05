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
        .ok_or_else(|| Status::resource_exhausted("batch item count overflow"))?;
    if item_count > MAX_BATCH_ITEMS as usize {
        return Err(Status::resource_exhausted("batch item limit exceeded"));
    }
    if request.encoded_len() > MAX_MESSAGE_BYTES as usize {
        return Err(Status::resource_exhausted("batch byte limit exceeded"));
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
        let durable = self
            .with_write_intent(self.storage.write_batch(request.get_ref()))
            .await?;
        Ok(Response::new(WriteBatchResponse { durable }))
    }

    pub(super) async fn handle_begin(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<BeginResponse>, Status> {
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

    pub(super) async fn handle_commit(
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

    pub(super) async fn handle_rollback(
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
