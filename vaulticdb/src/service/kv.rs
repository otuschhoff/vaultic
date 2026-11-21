//! Request validation and key-value read handlers.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};

use crate::{
    error::VaulticDbError,
    proto::{
        GetRequest, GetResponse, MultiGetRequest, MultiGetResponse, RequestContext, ScanRequest,
        ScanResponse,
    },
    MAX_BATCH_ITEMS, MAX_MESSAGE_BYTES, MAX_PAGE_ITEMS,
};

use super::{DaemonState, Service};

fn timed_read_response<T>(payload: T, started: Instant, storage_elapsed: Duration) -> Response<T> {
    let mut response = Response::new(payload);
    response.metadata_mut().insert(
        "x-vaulticdb-read-storage-ns",
        storage_elapsed.as_nanos().to_string().parse().unwrap(),
    );
    response.metadata_mut().insert(
        "x-vaulticdb-read-handler-ns",
        started.elapsed().as_nanos().to_string().parse().unwrap(),
    );
    response
}

fn set_database_read_timing<T>(response: &mut Response<T>, timing: Option<(Duration, Duration)>) {
    if let Some((lock, engine)) = timing {
        for (header, elapsed) in [
            ("x-vaulticdb-read-db-lock-ns", lock),
            ("x-vaulticdb-read-engine-ns", engine),
        ] {
            response.metadata_mut().insert(header, elapsed.as_nanos().to_string().parse().unwrap());
        }
    }
}

pub(crate) fn validate_scan(request: &ScanRequest) -> Result<(), Status> {
    if request.page_size == 0 || request.page_size > MAX_PAGE_ITEMS {
        return Err(VaulticDbError::InvalidRequest {
            field: "page_size".to_owned(),
            message: "scan page size is outside the supported range".to_owned(),
        }
        .into());
    }
    Ok(())
}

pub(super) fn check_storage_request<T>(
    state: &DaemonState,
    request: &Request<T>,
    context: Option<&RequestContext>,
) -> Result<(), Status> {
    check_request(state, request, "")?;
    check_context(context)?;
    if state.draining.load(Ordering::Acquire) {
        return Err(VaulticDbError::StorageUnavailable {
            message: "vaulticdb is draining".to_owned(),
        }
        .into());
    }
    Ok(())
}

pub(super) fn check_context(context: Option<&RequestContext>) -> Result<(), Status> {
    let context = context.ok_or_else(|| {
        Status::from(VaulticDbError::InvalidRequest {
            field: "context".to_owned(),
            message: "request context is required".to_owned(),
        })
    })?;
    if context.request_id.is_empty() {
        return Err(VaulticDbError::InvalidRequest {
            field: "request_id".to_owned(),
            message: "request ID is required".to_owned(),
        }
        .into());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time is before Unix epoch"))?
        .as_millis() as i64;
    if context.deadline_unix_ms > 0 && context.deadline_unix_ms <= now {
        return Err(VaulticDbError::DeadlineExceeded {
            message: "request deadline has expired".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn check_repository(state: &DaemonState, requested: &str) -> Result<(), Status> {
    if requested.is_empty() || requested == state.repository_id.as_ref() {
        return Ok(());
    }
    Err(VaulticDbError::Namespace {
        message: "repository identity mismatch".to_owned(),
    }
    .into())
}

pub(super) fn check_request<T>(
    state: &DaemonState,
    request: &Request<T>,
    repository_id: &str,
) -> Result<(), Status> {
    if let Some(token) = &state.auth_token {
        let expected = format!("Bearer {}", token.as_str());
        if request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
        {
            return Err(VaulticDbError::Authentication {
                message: "invalid vaulticdb authorization".to_owned(),
            }
            .into());
        }
    }
    check_repository(state, repository_id)
}

impl Service {
    pub(super) async fn handle_get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let started = Instant::now();
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let request = request.into_inner();
        let storage = self.storage().await?;
        let storage_started = Instant::now();
        let result = storage.get(&request.key, &request.transaction_id).await?;
        Ok(timed_read_response(
            result,
            started,
            storage_started.elapsed(),
        ))
    }

    pub(super) async fn handle_multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        let started = Instant::now();
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        if request.get_ref().keys.len() > MAX_BATCH_ITEMS as usize {
            return Err(VaulticDbError::ResourceExhausted {
                message: "multi-get item limit exceeded".to_owned(),
                retryable: false,
            }
            .into());
        }
        let request = request.into_inner();
        let storage = self.storage().await?;
        let storage_started = Instant::now();
        let mut database_timing = None;
        let results = storage
            .multi_get_with_timing(
                &request.keys,
                &request.transaction_id,
                MAX_MESSAGE_BYTES as usize,
                &mut database_timing,
            )
            .await?;
        let mut response = timed_read_response(
            MultiGetResponse { results },
            started,
            storage_started.elapsed(),
        );
        set_database_read_timing(&mut response, database_timing);
        Ok(response)
    }

    pub(super) async fn handle_scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<ScanResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        validate_scan(request.get_ref())?;
        let request = request.into_inner();
        let storage = self.storage().await?;
        Ok(Response::new(
            storage
                .scan(
                    &request.prefix,
                    &request.after_key,
                    request.page_size as usize,
                    &request.transaction_id,
                )
                .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_timing_metadata_preserves_payload() {
        let mut response = timed_read_response(
            42,
            Instant::now() - Duration::from_millis(1),
            Duration::from_nanos(250),
        );
        assert_eq!(*response.get_ref(), 42);
        let handler: u64 = response
            .metadata()
            .get("x-vaulticdb-read-handler-ns")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let storage: u64 = response
            .metadata()
            .get("x-vaulticdb-read-storage-ns")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(handler >= 1_000_000);
        assert_eq!(storage, 250);
        assert!(response.metadata().get("x-vaulticdb-read-db-lock-ns").is_none());
        set_database_read_timing(&mut response, Some((Duration::from_nanos(10), Duration::from_nanos(200))));
        assert_eq!(response.metadata().get("x-vaulticdb-read-db-lock-ns").unwrap(), "10");
        assert_eq!(response.metadata().get("x-vaulticdb-read-engine-ns").unwrap(), "200");
    }
}
