use anyhow::Error as AnyError;
use bytes::Bytes;
use prost::Message;
use thiserror::Error;
use tonic::{Code, Status};

use crate::proto::ErrorDetail;
use vaulticdb::writer_role::RoleError;

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<RpcAny>,
}

#[derive(Clone, PartialEq, Message)]
struct RpcAny {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum VaulticDbError {
    #[error("writer is fenced by epoch {generation}")]
    WriterFenced { generation: u64 },
    #[error("writer role changed during the operation")]
    WriterDemoted,
    #[error("writer role is transitioning")]
    WriterTransitioning,
    #[error("metadata generation authority: {message}")]
    Generation { message: String },
    #[error("metadata namespace: {message}")]
    Namespace { message: String },
    #[error("metadata encryption: {message}")]
    Encryption { message: String },
    #[error("idempotency: {message}")]
    Idempotency { message: String },
    #[error("storage unavailable: {message}")]
    StorageUnavailable { message: String },
    #[error("invalid {field}: {message}")]
    InvalidRequest { field: String, message: String },
    #[error("key management: {message}")]
    KeyManagement { message: String },
    #[error("writer role: {message}")]
    WriterRole { message: String },
}

impl VaulticDbError {
    pub fn generation(error: AnyError) -> Self {
        Self::Generation {
            message: format!("{error:#}"),
        }
    }

    pub fn key_management(error: impl Into<AnyError>) -> Self {
        let error = error.into();
        Self::KeyManagement {
            message: format!("{error:#}"),
        }
    }

    fn properties(&self) -> (Code, &'static str, bool, &str, u64) {
        match self {
            Self::WriterFenced { generation } => {
                (Code::Aborted, "writer_fenced", false, "", *generation)
            }
            Self::WriterDemoted => (Code::FailedPrecondition, "writer_demoted", true, "", 0),
            Self::WriterTransitioning => (Code::Unavailable, "writer_transitioning", true, "", 0),
            Self::Generation { .. } => (
                Code::FailedPrecondition,
                "generation_changed",
                false,
                "generation",
                0,
            ),
            Self::Namespace { .. } => (
                Code::FailedPrecondition,
                "namespace_mismatch",
                false,
                "namespace",
                0,
            ),
            Self::Encryption { .. } => (
                Code::DataLoss,
                "encryption_integrity",
                false,
                "encryption",
                0,
            ),
            Self::Idempotency { .. } => (
                Code::Aborted,
                "idempotency_conflict",
                false,
                "idempotency_key",
                0,
            ),
            Self::StorageUnavailable { .. } => {
                (Code::Unavailable, "storage_unavailable", true, "", 0)
            }
            Self::InvalidRequest { field, .. } => {
                (Code::InvalidArgument, "invalid_request", false, field, 0)
            }
            Self::KeyManagement { .. } => {
                (Code::FailedPrecondition, "key_management", false, "", 0)
            }
            Self::WriterRole { .. } => (Code::FailedPrecondition, "writer_role", false, "", 0),
        }
    }
}

impl From<RoleError> for VaulticDbError {
    fn from(error: RoleError) -> Self {
        match error {
            RoleError::Fenced { observed_epoch } => Self::WriterFenced {
                generation: observed_epoch,
            },
            RoleError::Transitioning => Self::WriterTransitioning,
            RoleError::NotWriter => Self::WriterDemoted,
            other => Self::WriterRole {
                message: other.to_string(),
            },
        }
    }
}

impl From<VaulticDbError> for Status {
    fn from(error: VaulticDbError) -> Self {
        let (status_code, detail_code, retryable, field, generation) = error.properties();
        let message = error.to_string();
        let detail = ErrorDetail {
            code: detail_code.to_owned(),
            message: message.clone(),
            retryable,
            field: field.to_owned(),
            generation,
        };
        let status = RpcStatus {
            code: status_code as i32,
            message: message.clone(),
            details: vec![RpcAny {
                type_url: "type.googleapis.com/vaulticdb.v1.ErrorDetail".to_owned(),
                value: detail.encode_to_vec(),
            }],
        };
        Status::with_details(status_code, message, Bytes::from(status.encode_to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let envelope = RpcStatus::decode(status.details()).unwrap();
        assert_eq!(envelope.code, status.code() as i32);
        assert_eq!(envelope.message, status.message());
        ErrorDetail::decode(envelope.details[0].value.as_slice()).unwrap()
    }

    #[test]
    fn writer_fence_status_has_machine_readable_details() {
        let status: Status = VaulticDbError::WriterFenced { generation: 42 }.into();
        assert_eq!(status.code(), Code::Aborted);
        let detail = decode_detail(&status);
        assert_eq!(detail.code, "writer_fenced");
        assert_eq!(detail.generation, 42);
        assert!(!detail.retryable);
    }

    #[test]
    fn transitioning_role_is_retryable() {
        let status: Status = VaulticDbError::from(RoleError::Transitioning).into();
        assert_eq!(status.code(), Code::Unavailable);
        let detail = decode_detail(&status);
        assert_eq!(detail.code, "writer_transitioning");
        assert!(detail.retryable);
    }

    #[test]
    fn every_domain_variant_has_stable_status_properties() {
        let cases = [
            (
                VaulticDbError::WriterDemoted,
                Code::FailedPrecondition,
                "writer_demoted",
                true,
                "",
            ),
            (
                VaulticDbError::Generation {
                    message: "changed".into(),
                },
                Code::FailedPrecondition,
                "generation_changed",
                false,
                "generation",
            ),
            (
                VaulticDbError::Namespace {
                    message: "wrong".into(),
                },
                Code::FailedPrecondition,
                "namespace_mismatch",
                false,
                "namespace",
            ),
            (
                VaulticDbError::Encryption {
                    message: "tag".into(),
                },
                Code::DataLoss,
                "encryption_integrity",
                false,
                "encryption",
            ),
            (
                VaulticDbError::Idempotency {
                    message: "reused".into(),
                },
                Code::Aborted,
                "idempotency_conflict",
                false,
                "idempotency_key",
            ),
            (
                VaulticDbError::StorageUnavailable {
                    message: "offline".into(),
                },
                Code::Unavailable,
                "storage_unavailable",
                true,
                "",
            ),
            (
                VaulticDbError::InvalidRequest {
                    field: "key".into(),
                    message: "empty".into(),
                },
                Code::InvalidArgument,
                "invalid_request",
                false,
                "key",
            ),
            (
                VaulticDbError::KeyManagement {
                    message: "slot".into(),
                },
                Code::FailedPrecondition,
                "key_management",
                false,
                "",
            ),
            (
                VaulticDbError::WriterRole {
                    message: "tenure".into(),
                },
                Code::FailedPrecondition,
                "writer_role",
                false,
                "",
            ),
        ];
        for (error, code, detail_code, retryable, field) in cases {
            let status: Status = error.into();
            assert_eq!(status.code(), code);
            let detail = decode_detail(&status);
            assert_eq!(detail.code, detail_code);
            assert_eq!(detail.retryable, retryable);
            assert_eq!(detail.field, field);
        }
    }
}
