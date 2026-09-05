//! Request validation and key-value read handlers.

use std::sync::atomic::Ordering;

use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    proto::{
        GetRequest, GetResponse, MultiGetRequest, MultiGetResponse, RequestContext, ScanRequest,
        ScanResponse,
    },
    storage::repeated_message_encoded_len,
    MAX_BATCH_ITEMS, MAX_MESSAGE_BYTES, MAX_PAGE_ITEMS,
};

use super::{DaemonState, Service};

pub(crate) fn validate_scan(request: &ScanRequest) -> Result<(), Status> {
    if request.page_size == 0 || request.page_size > MAX_PAGE_ITEMS {
        return Err(Status::invalid_argument(
            "scan page size is outside the supported range",
        ));
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
        return Err(Status::unavailable("vaulticdb is draining"));
    }
    Ok(())
}

pub(super) fn check_context(context: Option<&RequestContext>) -> Result<(), Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("request context is required"))?;
    if context.request_id.is_empty() {
        return Err(Status::invalid_argument("request ID is required"));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time is before Unix epoch"))?
        .as_millis() as i64;
    if context.deadline_unix_ms > 0 && context.deadline_unix_ms <= now {
        return Err(Status::deadline_exceeded("request deadline has expired"));
    }
    Ok(())
}

fn check_repository(state: &DaemonState, requested: &str) -> Result<(), Status> {
    if requested.is_empty() || requested == state.repository_id.as_ref() {
        return Ok(());
    }
    Err(Status::failed_precondition("repository identity mismatch"))
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
            return Err(Status::unauthenticated("invalid vaulticdb authorization"));
        }
    }
    check_repository(state, repository_id)
}

impl Service {
    pub(super) async fn handle_get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let request = request.into_inner();
        Ok(Response::new(
            self.storage
                .get(&request.key, &request.transaction_id)
                .await?,
        ))
    }

    pub(super) async fn handle_multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        if request.get_ref().keys.len() > MAX_BATCH_ITEMS as usize {
            return Err(Status::resource_exhausted("multi-get item limit exceeded"));
        }
        let request = request.into_inner();
        let mut results = Vec::with_capacity(request.keys.len());
        let mut response_bytes = 0usize;
        for key in request.keys {
            let result = self.storage.get(&key, &request.transaction_id).await?;
            response_bytes = response_bytes
                .checked_add(repeated_message_encoded_len(result.encoded_len()))
                .ok_or_else(|| Status::resource_exhausted("multi-get response size overflow"))?;
            if response_bytes > MAX_MESSAGE_BYTES as usize {
                return Err(Status::resource_exhausted(
                    "multi-get response byte limit exceeded",
                ));
            }
            results.push(result);
        }
        Ok(Response::new(MultiGetResponse { results }))
    }

    pub(super) async fn handle_scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<ScanResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        validate_scan(request.get_ref())?;
        let request = request.into_inner();
        Ok(Response::new(
            self.storage
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
