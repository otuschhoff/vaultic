#[cfg(test)]
mod tests {
    //! Error-to-status mapping tests.

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
    fn deterministic_resource_limit_is_not_retryable() {
        let status: Status = VaulticDbError::ResourceExhausted {
            message: "batch byte limit exceeded".to_owned(),
            retryable: false,
        }
        .into();
        assert_eq!(status.code(), Code::ResourceExhausted);
        let detail = decode_detail(&status);
        assert_eq!(detail.code, "resource_exhausted");
        assert!(!detail.retryable);
    }

    #[test]
    fn provider_io_is_retryable_storage_unavailability() {
        let status: Status = VaulticDbError::generation(anyhow::Error::new(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "provider timed out"),
        ))
        .into();
        assert_eq!(status.code(), Code::Unavailable);
        let detail = decode_detail(&status);
        assert_eq!(detail.code, "storage_unavailable");
        assert!(detail.retryable);
    }

    #[test]
    fn permanent_provider_io_is_not_retryable() {
        for kind in [
            std::io::ErrorKind::InvalidInput,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::Unsupported,
        ] {
            let status: Status = VaulticDbError::generation(anyhow::Error::new(
                std::io::Error::new(kind, "permanent provider failure"),
            ))
            .into();
            assert_eq!(status.code(), Code::FailedPrecondition);
            let detail = decode_detail(&status);
            assert_eq!(detail.code, "generation_changed");
            assert!(!detail.retryable);
        }
    }

    async fn http_status_error(status: u16) -> reqwest::Error {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                )
                .await
                .unwrap();
        });
        let error = reqwest::get(format!("http://{address}"))
            .await
            .unwrap()
            .error_for_status()
            .unwrap_err();
        server.await.unwrap();
        error
    }

    #[tokio::test]
    async fn provider_http_retryability_matches_status_class() {
        for (status_code, retryable) in [(429, true), (503, true), (403, false)] {
            let error = anyhow::Error::new(http_status_error(status_code).await);
            assert_eq!(provider_unavailable(&error), retryable, "HTTP {status_code}");
        }
    }

    #[test]
    fn object_store_provider_retryability_matches_error_class() {
        let transient = anyhow::Error::new(slatedb::object_store::Error::Generic {
            store: "S3",
            source: "retry budget exhausted after HTTP 503".into(),
        });
        assert!(provider_unavailable(&transient));

        let permanent = anyhow::Error::new(slatedb::object_store::Error::PermissionDenied {
            path: "coordination/writer".to_owned(),
            source: "HTTP 403".into(),
        });
        assert!(!provider_unavailable(&permanent));

        for store in ["vaulticdb replicated object store", "native RADOS", "Config"] {
            let permanent = anyhow::Error::new(slatedb::object_store::Error::Generic {
                store,
                source: "permanent store failure".into(),
            });
            assert!(!provider_unavailable(&permanent), "store {store}");
        }

        let rados_timeout = anyhow::Error::new(slatedb::object_store::Error::Generic {
            store: "native RADOS",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "RADOS operation timed out",
            )),
        });
        assert!(provider_unavailable(&rados_timeout));
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
                VaulticDbError::GenerationReconciliationPending {
                    message: "refresh failed".into(),
                    generation: 9,
                },
                Code::Unavailable,
                "generation_reconciliation_pending",
                true,
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
                VaulticDbError::StorageConflict {
                    message: "transaction".into(),
                    retryable: true,
                },
                Code::Aborted,
                "storage_conflict",
                true,
                "",
            ),
            (
                VaulticDbError::StorageDataLoss {
                    message: "corrupt".into(),
                },
                Code::DataLoss,
                "storage_data_loss",
                false,
                "storage",
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
                VaulticDbError::Precondition {
                    field: "approval".into(),
                    message: "required".into(),
                },
                Code::FailedPrecondition,
                "precondition_failed",
                false,
                "approval",
            ),
            (
                VaulticDbError::DeadlineExceeded {
                    message: "expired".into(),
                },
                Code::DeadlineExceeded,
                "deadline_exceeded",
                true,
                "",
            ),
            (
                VaulticDbError::ResourceExhausted {
                    message: "limit".into(),
                    retryable: true,
                },
                Code::ResourceExhausted,
                "resource_exhausted",
                true,
                "",
            ),
            (
                VaulticDbError::NotFound {
                    field: "transaction_id".into(),
                    message: "missing".into(),
                },
                Code::NotFound,
                "not_found",
                false,
                "transaction_id",
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
            (
                VaulticDbError::Authentication {
                    message: "token".into(),
                },
                Code::Unauthenticated,
                "authentication_failed",
                false,
                "",
            ),
            (
                VaulticDbError::Authorization {
                    message: "policy".into(),
                },
                Code::PermissionDenied,
                "authorization_failed",
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
