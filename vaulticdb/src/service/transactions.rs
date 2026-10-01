//! Transaction and write-batch gRPC handlers.

use prost::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tonic::{Request, Response, Status};

use crate::{
    attribution::CommitStage,
    error::VaulticDbError,
    proto::{
        AwaitDurableThroughRequest, AwaitDurableThroughResponse, BeginOwnedRequest,
        BeginPublicationRequest, BeginResponse, CancelBeginRequest, CommitResponse,
        DurabilityToken, Empty, TransactionRequest, WriteBatchRequest, WriteBatchResponse,
    },
    MAX_BATCH_ITEMS, MAX_MESSAGE_BYTES,
};

use super::{check_storage_request, process_test_barrier, role_error, Service};

#[derive(Default)]
pub(crate) struct FinalizationLocks {
    locks: Mutex<HashMap<String, Weak<Mutex<()>>>>,
}

impl FinalizationLocks {
    async fn lock(&self, transaction_id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.locks.lock().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(transaction_id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(transaction_id.to_owned(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }
}

#[derive(Default)]
pub(crate) struct BeginRecord {
    pub(crate) canceled: bool,
    pub(crate) transaction_id: Option<String>,
    pub(crate) authority: Option<(u64, u64)>,
}

#[derive(Default)]
pub(crate) struct BeginReconciliations {
    entries: Mutex<HashMap<String, (i64, Arc<Mutex<BeginRecord>>)>>,
}

impl BeginReconciliations {
    pub(crate) async fn lock(
        &self,
        request_id: &str,
        deadline_ms: i64,
        now_ms: i64,
    ) -> Result<OwnedMutexGuard<BeginRecord>, Status> {
        if request_id.len() != 32
            || !request_id.bytes().all(|byte| byte.is_ascii_hexdigit())
            || deadline_ms <= 0
            || deadline_ms > now_ms.saturating_add(11_000)
        {
            return Err(Status::invalid_argument(
                "invalid Begin reconciliation identity or deadline",
            ));
        }
        let entry = {
            let mut entries = self.entries.lock().await;
            entries.retain(|_, (deadline, entry)| {
                Arc::strong_count(entry) > 1 || deadline.saturating_add(60_000) >= now_ms
            });
            if let Some((deadline, entry)) = entries.get(request_id) {
                if *deadline != deadline_ms {
                    return Err(Status::invalid_argument(
                        "Begin request identity reused with another deadline",
                    ));
                }
                entry.clone()
            } else {
                if deadline_ms.saturating_add(60_000) < now_ms {
                    return Err(Status::failed_precondition(
                        "Begin reconciliation history expired",
                    ));
                }
                if entries.len() >= 4096 {
                    return Err(Status::resource_exhausted(
                        "Begin reconciliation ledger is full",
                    ));
                }
                let entry = Arc::new(Mutex::new(BeginRecord::default()));
                entries.insert(request_id.to_owned(), (deadline_ms, entry.clone()));
                entry
            }
        };
        Ok(entry.lock_owned().await)
    }
}

#[cfg(test)]
mod finalization_tests {
    use super::{BeginReconciliations, FinalizationLocks};
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn begin_reconciliation_serializes_creation_and_cancellation() {
        let ledger = Arc::new(BeginReconciliations::default());
        let request_id = "a".repeat(32);
        let mut canceled = ledger.lock(&request_id, 10_000, 0).await.unwrap();
        canceled.canceled = true;
        let waiting = ledger.clone();
        let waiting_id = request_id.clone();
        let begin =
            tokio::spawn(async move { waiting.lock(&waiting_id, 10_000, 0).await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!begin.is_finished());
        drop(canceled);
        let begun = begin.await.unwrap();
        assert!(begun.canceled);
        assert!(begun.transaction_id.is_none());
        drop(begun);
        assert!(ledger.lock(&request_id, 9_999, 0).await.is_err());
        let mut created = ledger.lock(&"b".repeat(32), 10_000, 0).await.unwrap();
        created.transaction_id = Some("transaction".to_owned());
        drop(created);
        let mut cancellation = ledger.lock(&"b".repeat(32), 10_000, 0).await.unwrap();
        assert_eq!(cancellation.transaction_id.as_deref(), Some("transaction"));
        cancellation.canceled = true;
        drop(cancellation);
        let _expired = ledger.lock(&"c".repeat(32), 80_000, 70_001).await.unwrap();
        assert_eq!(ledger.entries.lock().await.len(), 1);
        assert!(ledger.lock(&request_id, 10_000, 70_001).await.is_err());
    }

    #[tokio::test]
    async fn begin_reconciliation_bounds_and_retired_history() {
        let ledger = BeginReconciliations::default();
        for index in 0..4096 {
            drop(
                ledger
                    .lock(&format!("{index:032x}"), 10_000, 0)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(
            ledger
                .lock(&"f".repeat(32), 10_000, 0)
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::ResourceExhausted
        );
        let active = ledger
            .lock(&format!("{:032x}", 0), 10_000, 0)
            .await
            .unwrap();
        let new = ledger.lock(&"f".repeat(32), 80_000, 70_001).await.unwrap();
        assert_eq!(ledger.entries.lock().await.len(), 2);
        drop(new);
        drop(active);
        assert_eq!(
            ledger
                .lock(&format!("{:032x}", 0), 10_000, 70_001)
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::FailedPrecondition
        );
        for (identity, deadline) in [
            ("bad".to_owned(), 80_000),
            ("g".repeat(32), 80_000),
            ("a".repeat(32), 0),
            ("b".repeat(32), 82_002),
        ] {
            assert_eq!(
                ledger
                    .lock(&identity, deadline, 70_001)
                    .await
                    .err()
                    .unwrap()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[tokio::test]
    async fn finalization_locks_isolate_transactions_and_prune_cancelled_waiters() {
        let locks = Arc::new(FinalizationLocks::default());
        let first = locks.lock("first").await;
        let independent = tokio::time::timeout(Duration::from_secs(1), locks.lock("second"))
            .await
            .expect("unrelated transaction must not wait");
        let waiting_locks = locks.clone();
        let waiter = tokio::spawn(async move { waiting_locks.lock("first").await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if locks.locks.lock().await["first"].strong_count() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waiter must reach the existing lock");
        assert!(!waiter.is_finished());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        drop(first);
        drop(independent);
        let _next = locks.lock("next").await;
        let entries = locks.locks.lock().await;
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key("next"));
    }
}

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
    fn begin_clock_ms() -> Result<i64, Status> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("invalid Begin reconciliation clock"))?
            .as_millis()
            .try_into()
            .map_err(|_| Status::internal("Begin reconciliation clock overflow"))
    }

    pub(super) async fn handle_begin_owned(
        &self,
        request: Request<BeginOwnedRequest>,
    ) -> Result<Response<BeginResponse>, Status> {
        let service = self.clone();
        tokio::spawn(async move { service.begin_owned_inner(request).await })
            .await
            .map_err(|error| Status::internal(format!("join owned Begin: {error}")))?
    }

    async fn begin_owned_inner(
        &self,
        request: Request<BeginOwnedRequest>,
    ) -> Result<Response<BeginResponse>, Status> {
        let mut timer = self.state.attribution.begin_request.timer();
        let result = async {
            check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
            let input = request.get_ref();
            let mut record = self
                .begin_reconciliations
                .lock(
                    &input.begin_request_id,
                    input.begin_deadline_unix_ms,
                    Self::begin_clock_ms()?,
                )
                .await?;
            if record.canceled {
                return Err(Status::cancelled("Begin request was canceled"));
            }
            if record.transaction_id.is_some() {
                return Err(Status::already_exists(
                    "Begin request already allocated a transaction",
                ));
            }
            if input.begin_deadline_unix_ms <= Self::begin_clock_ms()? {
                record.canceled = true;
                return Err(Status::deadline_exceeded(
                    "Begin admission deadline expired",
                ));
            }
            let storage = self.storage().await?;
            self.ensure_writer_authority().await?;
            let permits = loop {
                if input.begin_deadline_unix_ms <= Self::begin_clock_ms()? {
                    record.canceled = true;
                    return Err(Status::deadline_exceeded(
                        "Begin admission deadline expired",
                    ));
                }
                let remaining = std::time::Duration::from_millis(
                    (input.begin_deadline_unix_ms - Self::begin_clock_ms()?).max(0) as u64,
                );
                let acquired = tokio::time::timeout(
                    remaining,
                    storage.acquire_publication_permits(&input.content_ids),
                )
                .await;
                let acquired = match acquired {
                    Ok(result) => result?,
                    Err(_) => {
                        record.canceled = true;
                        return Err(Status::deadline_exceeded(
                            "Begin admission deadline expired",
                        ));
                    }
                };
                if let Some(permits) = acquired {
                    break permits;
                }
                let (_, expired) = storage.prune_expired_transactions().await;
                for _ in 0..expired {
                    self.state.writer_role.lock().await.transaction_closed();
                }
            };
            if input.begin_deadline_unix_ms <= Self::begin_clock_ms()? {
                record.canceled = true;
                return Err(Status::deadline_exceeded(
                    "Begin admission deadline expired",
                ));
            }
            let _admission = self.mutation_admission().await?;
            self.ensure_writer_authority().await?;
            let authority = Some(
                self.durability_authority(storage.as_ref())
                    .await?
                    .unwrap_or((0, storage.writer_epoch())),
            );
            if input.begin_deadline_unix_ms <= Self::begin_clock_ms()? {
                record.canceled = true;
                return Err(Status::deadline_exceeded(
                    "Begin admission deadline expired",
                ));
            }
            let response = self
                .begin_with_publication_permits(storage, permits)
                .await?;
            record.transaction_id = Some(response.get_ref().transaction_id.clone());
            record.authority = authority;
            Ok(response)
        }
        .await;
        timer.record_result(&result);
        result
    }

    pub(super) async fn handle_cancel_begin(
        &self,
        request: Request<CancelBeginRequest>,
    ) -> Result<Response<Empty>, Status> {
        let service = self.clone();
        tokio::spawn(async move { service.cancel_begin_inner(request).await })
            .await
            .map_err(|error| Status::internal(format!("join cancel Begin: {error}")))?
    }

    async fn cancel_begin_inner(
        &self,
        request: Request<CancelBeginRequest>,
    ) -> Result<Response<Empty>, Status> {
        let mut timer = self.state.attribution.rollback_request.timer();
        let result = async {
            check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
            let input = request.get_ref();
            let mut record = self
                .begin_reconciliations
                .lock(
                    &input.begin_request_id,
                    input.begin_deadline_unix_ms,
                    Self::begin_clock_ms()?,
                )
                .await?;
            record.canceled = true;
            let _finalization = if let Some(transaction_id) = record.transaction_id.as_ref() {
                Some(self.finalization_locks.lock(transaction_id).await)
            } else {
                None
            };
            let _admission = self.mutation_admission().await?;
            let storage = self.storage().await?;
            self.ensure_writer_authority().await?;
            if let Some(transaction_id) = record.transaction_id.as_ref() {
                let authority = Some(
                    self.durability_authority(storage.as_ref())
                        .await?
                        .unwrap_or((0, storage.writer_epoch())),
                );
                if record.authority != authority {
                    return Err(Status::failed_precondition(
                        "Begin reconciliation authority changed",
                    ));
                }
                let result = storage.rollback(transaction_id).await;
                let consumed = match &result {
                    Ok(outcome) => outcome.consumed,
                    Err(failure) => failure.consumed,
                };
                if consumed {
                    self.state.writer_role.lock().await.transaction_closed();
                    *self.state.last_writer_activity.lock().await = Instant::now();
                }
                if let Err(failure) = result {
                    self.ensure_writer_authority().await?;
                    if failure.status.code() != tonic::Code::NotFound {
                        return Err(failure.status);
                    }
                }
            }
            Ok(Response::new(Empty { context: None }))
        }
        .await;
        timer.record_result(&result);
        result
    }

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
            self.begin_with_publication_permits(storage, Vec::new())
                .await
        }
        .await;
        timer.record_result(&result);
        result
    }

    pub(super) async fn begin_publication_inner(
        &self,
        request: Request<BeginPublicationRequest>,
    ) -> Result<Response<BeginResponse>, Status> {
        let mut timer = self.state.attribution.begin_request.timer();
        let result = async {
            check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
            let storage = self.storage().await?;
            let permits = loop {
                if let Some(permits) = storage
                    .acquire_publication_permits(&request.get_ref().content_ids)
                    .await?
                {
                    break permits;
                }
                let (_, expired) = storage.prune_expired_transactions().await;
                let mut role = self.state.writer_role.lock().await;
                for _ in 0..expired {
                    role.transaction_closed();
                }
            };
            let _admission = self.mutation_admission().await?;
            self.ensure_writer_authority().await?;
            self.begin_with_publication_permits(storage, permits).await
        }
        .await;
        timer.record_result(&result);
        result
    }

    async fn begin_with_publication_permits(
        &self,
        storage: std::sync::Arc<crate::storage::Storage>,
        permits: Vec<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<Response<BeginResponse>, Status> {
        self.state
            .writer_role
            .lock()
            .await
            .transaction_opened()
            .map_err(role_error)?;
        let begin = if permits.is_empty() {
            storage.begin().await
        } else {
            storage.begin_with_publication_permits(permits).await
        };
        let outcome = match begin {
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
            idle_timeout_ms: storage.transaction_idle_timeout_ms(),
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
        let mut timer = self.state.attribution.commit_request.timer();
        let mut stage = CommitStage::RequestValidation;
        let mut transaction_consumed = None;
        let result = async {
            check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
            let _finalization = self
                .finalization_locks
                .lock(&request.get_ref().transaction_id)
                .await;
            let fence = request.get_ref().publication_fence.as_ref();
            let _transition = if fence.is_some() {
                Some(self.state.writer_transition.lock().await)
            } else {
                None
            };
            stage = CommitStage::Admission;
            let _admission = self.mutation_admission().await?;
            stage = CommitStage::Storage;
            let storage = self.storage().await?;
            stage = CommitStage::WriterAuthority;
            self.ensure_writer_authority().await?;
            if let Some(fence) = fence {
                stage = CommitStage::PublicationFence;
                if fence.read_session_id.is_empty()
                    || fence.read_session_id == request.get_ref().transaction_id
                    || request.get_ref().defer_durability
                {
                    return Err(Status::invalid_argument("invalid publication fence"));
                }
                let generation = storage
                    .generation_authority(&self.state.repository_id)
                    .await
                    .map_err(VaulticDbError::generation)
                    .map_err(Status::from)?;
                if generation.active_generation != fence.generation
                    || generation.decision != fence.decision
                    || generation.state != "healthy"
                {
                    return Err(Status::failed_precondition(
                        "publication generation changed",
                    ));
                }
                storage
                    .validate_read_session(&fence.read_session_id)
                    .await?;
            }
            stage = CommitStage::DurabilityAuthority;
            let authority = self.durability_authority(storage.as_ref()).await?;
            stage = CommitStage::StorageCommit;
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
            transaction_consumed = Some(consumed);
            if consumed {
                self.state.writer_role.lock().await.transaction_closed();
                *self.state.last_writer_activity.lock().await = Instant::now();
            }
            if result.is_err() && !consumed {
                stage = CommitStage::PostCommitAuthority;
                self.ensure_writer_authority().await?;
                stage = CommitStage::StorageCommit;
            }
            let outcome = result.map_err(|failure| failure.status)?;
            Ok(Response::new(CommitResponse {
                durable: !request.get_ref().defer_durability,
                durability_token: self.durability_token(authority, outcome.applied_sequence),
            }))
        }
        .await;
        timer.record_status_result(&result);
        if let Err(status) = &result {
            if let Some(event) = self.state.attribution.commit_failures.record(
                stage,
                status.code(),
                transaction_consumed,
            ) {
                use std::io::Write;
                let _ = writeln!(std::io::stderr().lock(), "{event}");
            }
        }
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
            let _finalization = self
                .finalization_locks
                .lock(&request.get_ref().transaction_id)
                .await;
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
