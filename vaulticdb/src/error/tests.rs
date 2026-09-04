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
