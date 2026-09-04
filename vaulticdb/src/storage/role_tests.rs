#[cfg(test)]
mod role_tests {
    use super::*;
    use crate::proto::{KeyValue, WriteBatchRequest};

    #[tokio::test]
    async fn demote_reads_and_promote_writes_without_restart() {
        let suffix = rand::random::<u64>();
        unsafe {
            env::set_var("VAULTICDB_OBJECT_STORE", "memory");
            env::set_var("VAULTICDB_DATABASE_PATH", format!("role-test-{suffix}"));
        }
        let storage = Storage::open(&format!("role-repo-{suffix}")).await.unwrap();
        let request = WriteBatchRequest {
            puts: vec![KeyValue {
                key: b"key".to_vec(),
                value: b"before".to_vec(),
            }],
            await_durable: true,
            ..Default::default()
        };
        storage.write_batch(&request).await.unwrap();
        storage.demote().await.unwrap();
        assert_eq!(storage.get(b"key", "").await.unwrap().value, b"before");
        assert!(storage.write_batch(&request).await.is_err());

        storage.promote(None).await.unwrap();
        let request = WriteBatchRequest {
            puts: vec![KeyValue {
                key: b"key".to_vec(),
                value: b"after".to_vec(),
            }],
            await_durable: true,
            ..Default::default()
        };
        storage.write_batch(&request).await.unwrap();
        assert_eq!(storage.get(b"key", "").await.unwrap().value, b"after");
        storage.close().await.unwrap();
        unsafe {
            env::remove_var("VAULTICDB_DATABASE_PATH");
        }
    }

    #[tokio::test]
    async fn durable_idempotency_recovers_batches_and_transaction_commits() {
        let suffix = rand::random::<u64>();
        unsafe {
            env::set_var("VAULTICDB_OBJECT_STORE", "memory");
            env::set_var(
                "VAULTICDB_DATABASE_PATH",
                format!("idempotency-test-{suffix}"),
            );
        }
        let storage = Storage::open(&format!("idempotency-repo-{suffix}"))
            .await
            .unwrap();
        let request = WriteBatchRequest {
            puts: vec![KeyValue {
                key: b"direct".to_vec(),
                value: b"one".to_vec(),
            }],
            idempotency_key: "batch-one".to_owned(),
            ..Default::default()
        };
        assert!(storage.write_batch(&request).await.unwrap());
        assert!(storage.write_batch(&request).await.unwrap());
        let mut conflict = request.clone();
        conflict.puts[0].value = b"two".to_vec();
        assert_eq!(
            storage.write_batch(&conflict).await.unwrap_err().code(),
            tonic::Code::AlreadyExists
        );

        let transaction_id = storage.begin().await.unwrap();
        storage
            .write_batch(&WriteBatchRequest {
                puts: vec![KeyValue {
                    key: b"transaction".to_vec(),
                    value: b"committed".to_vec(),
                }],
                transaction_id: transaction_id.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        storage.commit(&transaction_id, "commit-one").await.unwrap();
        storage.commit(&transaction_id, "commit-one").await.unwrap();
        assert_eq!(
            storage.get(b"transaction", "").await.unwrap().value,
            b"committed"
        );
        storage.close().await.unwrap();
        unsafe {
            env::remove_var("VAULTICDB_DATABASE_PATH");
        }
    }
}
