//! Transaction and write-batch gRPC handlers.

use prost::Message;
use std::time::Instant;
use tonic::{Request, Response, Status};

use crate::{
    proto::{
        AwaitDurableThroughRequest, AwaitDurableThroughResponse, BeginResponse, CommitResponse,
        DurabilityToken, Empty, TransactionRequest, WriteBatchRequest, WriteBatchResponse,
    },
    MAX_BATCH_ITEMS, MAX_MESSAGE_BYTES,
};

use super::{check_storage_request, process_test_barrier, role_error, Service};

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
        let mut timer = self.state.attribution.write_batch_request.timer();
        let result = async {
            check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
            validate_write_batch(request.get_ref())?;
            let storage = self.storage().await?;
            let _intent = self.write_intent().await?;
            let authority = self.durability_authority(storage.as_ref()).await?;
            let outcome = storage.write_batch(request.get_ref()).await;
            process_test_barrier("VAULTICDB_TEST_MUTATION_COMPLETE_BARRIER").await?;
            if outcome.is_err() {
                self.ensure_writer_authority().await?;
            }
            let outcome = outcome?;
            let durability_token = self.durability_token(authority, outcome.applied_sequence);
            Ok(Response::new(WriteBatchResponse {
                durable: outcome.durable,
                durability_token,
            }))
        }
        .await;
        timer.record_result(&result);
        result
    }

    pub(super) async fn handle_await_durable_through(
        &self,
        request: Request<AwaitDurableThroughRequest>,
    ) -> Result<Response<AwaitDurableThroughResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let token = request
            .get_ref()
            .token
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("durability token is required"))?;
        let storage = self.storage().await?;
        let durable_sequence = storage
            .await_durable_through(
                self.state.repository_id.as_str(),
                token.repository_generation,
                token.writer_epoch,
                token.applied_sequence,
            )
            .await?;
        Ok(Response::new(AwaitDurableThroughResponse {
            durable_through: Some(DurabilityToken {
                repository_generation: token.repository_generation,
                writer_epoch: token.writer_epoch,
                applied_sequence: durable_sequence,
            }),
        }))
    }

    async fn durability_authority(
        &self,
        storage: &crate::storage::Storage,
    ) -> Result<Option<(u64, u64)>, Status> {
        if storage.supports_durability_tokens() {
            Ok(Some((
                storage
                    .active_generation(self.state.repository_id.as_str())
                    .await?,
                storage.writer_epoch(),
            )))
        } else {
            Ok(None)
        }
    }

    fn durability_token(
        &self,
        authority: Option<(u64, u64)>,
        applied_sequence: Option<u64>,
    ) -> Option<DurabilityToken> {
        authority.zip(applied_sequence).map(
            |((repository_generation, writer_epoch), applied_sequence)| DurabilityToken {
                repository_generation,
                writer_epoch,
                applied_sequence,
            },
        )
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
        let mut timer = self.state.attribution.begin_request.timer();
        let result = async {
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
        .await;
        timer.record_result(&result);
        result
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
        let mut timer = self.state.attribution.commit_request.timer();
        let result = async {
            check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
            let _admission = self.mutation_admission().await?;
            let storage = self.storage().await?;
            self.ensure_writer_authority().await?;
            let authority = self.durability_authority(storage.as_ref()).await?;
            let result = storage
                .commit(
                    &request.get_ref().transaction_id,
                    &request.get_ref().idempotency_key,
                    request.get_ref().defer_durability,
                    request.get_ref().require_durability_token,
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
            let outcome = result.map_err(|failure| failure.status)?;
            Ok(Response::new(CommitResponse {
                durable: !request.get_ref().defer_durability,
                durability_token: self.durability_token(authority, outcome.applied_sequence),
            }))
        }
        .await;
        timer.record_result(&result);
        result
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
        let mut timer = self.state.attribution.rollback_request.timer();
        let result = async {
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
        .await;
        timer.record_result(&result);
        result
    }
}
