#[cfg(test)]
mod tests {
    //! Storage persistence and generation authority tests.

    use super::*;
    use std::env;
    use slatedb::object_store::{path::Path, ObjectStoreExt};

    async fn listed_paths(store: &dyn ObjectStore) -> Vec<String> {
        let mut paths = Vec::new();
        let mut objects = store.list(None);
        while let Some(object) = objects.next().await {
            paths.push(object.unwrap().location.to_string());
        }
        paths
    }

    #[tokio::test]
    async fn direct_s3_control_objects_are_repository_scoped() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        bucket
            .put(
                &Path::from("other-repository/_vaultic/wal-target-v1"),
                "other".into(),
            )
            .await
            .unwrap();
        let config = ObjectStoreConfig::S3 {
            bucket: "bucket".to_owned(),
            prefix: None,
            endpoint: None,
            region: None,
            provider: None,
            bucket_lookup: None,
        };
        let scoped = repository_control_store(&config, "repository", bucket.clone());

        ensure_wal_target_identity(scoped.as_ref(), "s3:bucket:wal", false)
            .await
            .unwrap();

        assert_eq!(listed_paths(scoped.as_ref()).await, vec![WAL_TARGET_PATH]);
        assert!(listed_paths(bucket.as_ref())
            .await
            .contains(&"repository/_vaultic/wal-target-v1".to_owned()));
    }

    #[tokio::test]
    async fn wal_metrics_track_multipart_uploads() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wal, metrics) = monitored_wal_store(raw).await.unwrap();
        let location = Path::from("wal/0001.sst");
        let mut upload = wal
            .put_multipart_opts(&location, PutMultipartOptions::default())
            .await
            .unwrap();
        upload.put_part("part-a".into()).await.unwrap();
        upload.put_part("part-b".into()).await.unwrap();
        upload.complete().await.unwrap();

        let status = metrics.snapshot();
        assert_eq!(status.uploaded_bytes, 12);
        assert_eq!(status.retained_bytes, 12);
        assert_eq!(status.retained_segments, 1);
        assert_eq!(status.outstanding_flushes, 0);
        assert_eq!(status.durability_failures, 0);
    }

    #[tokio::test]
    async fn wal_metrics_track_finalizing_copies() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        raw.put(&Path::from("staging/0001"), "wal-data".into())
            .await
            .unwrap();
        let (wal, metrics) = monitored_wal_store(raw).await.unwrap();

        wal.copy(
            &Path::from("staging/0001"),
            &Path::from("wal/0001.sst"),
        )
        .await
        .unwrap();

        let status = metrics.snapshot();
        assert_eq!(status.uploaded_bytes, 8);
        assert_eq!(status.retained_bytes, 16);
        assert_eq!(status.retained_segments, 2);
        assert_eq!(status.outstanding_flushes, 0);
        assert_eq!(status.durability_failures, 0);
    }

    #[tokio::test]
    async fn separate_wal_store_routes_durable_writes_and_reader_replay() {
        let main: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let raw_wal: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wal, wal_metrics) = monitored_wal_store(raw_wal).await.unwrap();
        let path = format!("separate-wal-{}", rand::random::<u64>());
        let db = open_writer(&path, main.clone(), Some(wal.clone()))
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"wal-key", b"wal-value");
        db.write(batch).await.unwrap().await_durable().await.unwrap();

        let status = wal_metrics.snapshot();
        assert!(status.uploaded_bytes > 0);
        assert!(status.retained_bytes > 0);
        assert!(status.retained_segments > 0);
        assert_eq!(status.outstanding_flushes, 0);
        assert_eq!(status.durability_failures, 0);

        let wal_paths = listed_paths(wal.as_ref()).await;
        assert!(!wal_paths.is_empty());
        assert!(wal_paths.iter().all(|item| item.contains("wal")));
        assert!(listed_paths(main.as_ref())
            .await
            .iter()
            .all(|item| !item.contains("/wal/")));

        let reader = open_reader(&path, main.clone(), Some(wal.clone()))
            .await
            .unwrap();
        assert_eq!(reader.get(b"wal-key").await.unwrap().as_deref(), Some(&b"wal-value"[..]));
        reader.close().await.unwrap();
        db.close().await.unwrap();

        ensure_wal_target_identity(main.as_ref(), "s3:bucket-a:wal", false)
            .await
            .unwrap();
        assert!(ensure_wal_target_identity(main.as_ref(), "s3:bucket-b:wal", true)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn opening_writer_publishes_replayed_wal_for_readers() {
        let main: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = format!("publish-replayed-wal-{}", rand::random::<u64>());
        let db = Db::builder(path.as_str(), main.clone())
            .with_wal_object_store(wal.clone())
            .build()
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"replayed-key", b"replayed-value");
        db.write(batch).await.unwrap().await_durable().await.unwrap();
        db.close_with_options(slatedb::config::CloseOptions { flush_type: None })
            .await
            .unwrap();

        let writer = open_writer(path.as_str(), main.clone(), Some(wal.clone()))
            .await
            .unwrap();
        let reader = DbReader::builder(path.as_str(), main)
            .with_wal_object_store(wal)
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_options(DbReaderOptions {
                skip_wal_replay: true,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        assert_eq!(
            reader.get(b"replayed-key").await.unwrap().as_deref(),
            Some(&b"replayed-value"[..])
        );
        reader.close().await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn wal_metrics_report_expired_credential_failures() {
        let renewable = Arc::new(RenewableObjectStore::new(
            Arc::new(InMemory::new()),
            current_unix_ms() + 60_000,
            std::time::Duration::from_secs(60),
        ));
        let (wal, metrics) = monitored_wal_store(renewable.clone()).await.unwrap();
        renewable.write_until_ms.store(0, Ordering::Release);

        assert!(wal
            .put(&Path::from("wal/expired.sst"), b"payload".to_vec().into())
            .await
            .is_err());
        let status = metrics.snapshot();
        assert_eq!(status.outstanding_flushes, 0);
        assert_eq!(status.durability_failures, 1);
        assert_eq!(status.uploaded_bytes, 0);
        assert_eq!(status.retained_segments, 0);
    }

    #[tokio::test]
    async fn s3_metadata_rebuild_destroys_rebuilds_and_reopens_encrypted_candidate() {
        if env::var_os("VAULTICDB_TEST_S3_ENDPOINT").is_none() {
            return;
        }
        let bucket =
            env::var("VAULTICDB_TEST_S3_BUCKET").unwrap_or_else(|_| "vaulticdb-ci".to_owned());
        let prefix = format!(
            "phase20/rebuild-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let raw: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .unwrap(),
            prefix,
        ));

        let stale = Path::from("manifest/stale");
        raw.put(&stale, b"stale-authoritative-metadata".to_vec().into())
            .await
            .unwrap();
        let stale_objects = raw
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(stale_objects.len(), 1);
        for object in stale_objects {
            raw.delete(&object.location).await.unwrap();
        }
        assert!(!metadata_store_has_database_objects(raw.as_ref())
            .await
            .unwrap());

        let repository_id = "phase20-s3-rebuild";
        let dek = [0x5a; 32];
        let plaintext = b"phase20-known-plaintext-metadata";
        let (encrypted, _, _) =
            envelope::configure_brokered(repository_id, raw.clone(), &dek, 3, 9, true).unwrap();
        let db = Db::open("db", encrypted).await.unwrap();
        db.put(b"p:rebuilt-pack", plaintext.to_vec())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.put(
            METADATA_REBUILD_RECORD,
            serde_json::to_vec(&serde_json::json!({
                "format": 1,
                "repository_id": repository_id,
                "capsule_generation": 9,
                "metadata_dek_version": 3,
                "broker_epoch_id": "test-epoch",
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        db.close().await.unwrap();

        let objects = raw
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(!objects.is_empty());
        for object in &objects {
            let bytes = raw
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert!(!bytes
                .windows(plaintext.len())
                .any(|value| value == plaintext));
            assert!(!bytes.windows(dek.len()).any(|value| value == dek));
        }

        let (encrypted, _, _) =
            envelope::configure_brokered(repository_id, raw.clone(), &dek, 3, 9, false).unwrap();
        let reopened = Db::open("db", encrypted).await.unwrap();
        assert_eq!(
            reopened.get(b"p:rebuilt-pack").await.unwrap().unwrap(),
            plaintext.as_slice()
        );
        assert!(reopened
            .get(METADATA_REBUILD_RECORD)
            .await
            .unwrap()
            .is_some());
        reopened.close().await.unwrap();

        for object in objects {
            raw.delete(&object.location).await.unwrap();
        }
    }

    #[tokio::test]
    async fn metadata_rebuild_candidate_ignores_capsules_but_rejects_database_objects() {
        let store = InMemory::new();
        store
            .put(
                &slatedb::object_store::path::Path::from("_vaultic/recovery-capsules/one.json"),
                vec![1_u8].into(),
            )
            .await
            .unwrap();
        assert!(!metadata_store_has_database_objects(&store).await.unwrap());
        store
            .put(
                &slatedb::object_store::path::Path::from("manifest/0001"),
                vec![2_u8].into(),
            )
            .await
            .unwrap();
        assert!(metadata_store_has_database_objects(&store).await.unwrap());
    }
    use slatedb::object_store::memory::InMemory;

    #[test]
    fn repeated_message_size_includes_tag_and_varint_length() {
        assert_eq!(repeated_message_encoded_len(0), 2);
        assert_eq!(repeated_message_encoded_len(127), 129);
        assert_eq!(repeated_message_encoded_len(128), 131);
        assert_eq!(
            repeated_message_encoded_len(16 * 1024 * 1024),
            16 * 1024 * 1024 + 5
        );
    }

    #[test]
    fn transaction_expiry_is_idle_and_clock_safe() {
        assert!(!transaction_expired(1_000, 1_999, 1_000));
        assert!(transaction_expired(1_000, 2_000, 1_000));
        assert!(!transaction_expired(2_000, 1_000, 1_000));
    }

    #[tokio::test]
    async fn capsule_migration_keeps_master_key_until_matching_finalize() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let db = Db::open("migration-test", object_store.clone())
            .await
            .unwrap();
        let storage = Storage {
            database: RwLock::new(Database::Writer(db)),
            database_path: "migration-test".to_owned(),
            coordination_store: object_store.clone(),
            object_store,
            wal_object_store: None,
            wal_metrics: None,
            encryption: EncryptionStatus {
                enabled: true,
                algorithm: "AES-256-GCM",
                active_dek_version: 1,
                envelope_generation: 1,
                unlock_slot: Some("test".to_owned()),
                recovery_unlock: false,
                initializing: false,
            },
            key_manager: None,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: 1_000,
            credential_manager: None,
            broker_lease_metadata: None,
            writer_epoch: AtomicU64::new(1),
            wal_target: "inherited",
            wal_durability: "inherited",
        };
        storage.store_master_key(b"repository-key").await.unwrap();
        let digest = "ab".repeat(32);
        storage.record_capsule_migration(&digest).await.unwrap();
        storage.record_capsule_migration(&digest).await.unwrap();
        assert!(storage
            .record_capsule_migration(&"cd".repeat(32))
            .await
            .is_err());
        assert_eq!(
            storage.capsule_migration_status().await.unwrap(),
            (Some(digest.clone()), None)
        );
        assert!(storage
            .finalize_capsule_migration(&"cd".repeat(32))
            .await
            .is_err());
        assert_eq!(
            storage.get_master_key().await.unwrap().unwrap(),
            b"repository-key"
        );
        storage.finalize_capsule_migration(&digest).await.unwrap();
        storage.finalize_capsule_migration(&digest).await.unwrap();
        assert!(storage
            .finalize_capsule_migration(&"cd".repeat(32))
            .await
            .is_err());
        assert!(storage.get_master_key().await.unwrap().is_none());
        assert_eq!(
            storage.capsule_migration_status().await.unwrap(),
            (None, Some(digest))
        );
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn writer_epoch_claim_is_exclusive_and_fences_stale_claims() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), Some(1))
                .await
                .unwrap(),
            Some(2)
        );
        assert!(claim_writer_epoch(object_store.as_ref(), Some(1))
            .await
            .is_err());

        let wal_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("epoch-test", object_store.clone())
            .with_wal_object_store(wal_object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = Storage {
            database: RwLock::new(Database::Writer(db)),
            database_path: "epoch-test".to_owned(),
            coordination_store: object_store.clone(),
            object_store,
            wal_object_store: Some(wal_object_store),
            wal_metrics: None,
            encryption: EncryptionStatus {
                enabled: false,
                algorithm: "none",
                active_dek_version: 0,
                envelope_generation: 0,
                unlock_slot: None,
                recovery_unlock: false,
                initializing: false,
            },
            key_manager: None,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: 1_000,
            credential_manager: None,
            broker_lease_metadata: None,
            writer_epoch: AtomicU64::new(1),
            wal_target: "memory",
            wal_durability: "local-process",
        };
        let error = storage.assert_current_writer_epoch().await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn reader_validates_existing_encryption_policy_for_writer_takeover() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = format!("encrypted-reader-{}", rand::random::<u64>());
        let db = Db::open(path.as_str(), object_store.clone())
            .await
            .unwrap();
        let policy = EncryptionPolicy {
            format: 1,
            required: true,
            algorithm: "AES-256-GCM".to_owned(),
            object_format: 1,
            repository_id: "repo".into(),
        };
        db.put(
            ENCRYPTION_POLICY_RECORD,
            serde_json::to_vec(&policy).unwrap(),
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        db.close().await.unwrap();
        let reader = open_reader(&path, object_store.clone(), None)
            .await
            .unwrap();
        let storage = Storage {
            database: RwLock::new(Database::Reader(reader)),
            database_path: path,
            coordination_store: object_store.clone(),
            object_store,
            wal_object_store: None,
            wal_metrics: None,
            encryption: EncryptionStatus {
                enabled: true,
                algorithm: "AES-256-GCM",
                active_dek_version: 1,
                envelope_generation: 1,
                unlock_slot: Some("test".to_owned()),
                recovery_unlock: false,
                initializing: false,
            },
            key_manager: None,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: 1_000,
            credential_manager: None,
            broker_lease_metadata: None,
            writer_epoch: AtomicU64::new(1),
            wal_target: "inherited",
            wal_durability: "inherited",
        };

        storage.ensure_encryption_policy("repo").await.unwrap();
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn metadata_generation_activation_rejects_stale_compare_and_swap() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (current, version) = read_generation_authority(object_store.as_ref(), "repo")
            .await
            .unwrap();
        let first = GenerationAuthority {
            format: 1,
            repository_id: "repo".into(),
            decision: current.decision + 1,
            active_generation: 2,
            namespace: "candidate-a".into(),
            previous_generation: current.active_generation,
            previous_namespace: current.namespace.clone(),
            state: "post-activation".to_owned(),
            report_sha256: "ab".repeat(32),
            decided_at_ms: 1,
            observation_until_ms: 2,
            retired_generation: 0,
        };
        let mut second = first.clone();
        second.namespace = "candidate-b".into();
        publish_generation_authority(object_store.as_ref(), &first, version.clone())
            .await
            .unwrap();
        assert!(
            publish_generation_authority(object_store.as_ref(), &second, version)
                .await
                .is_err()
        );
        assert_eq!(
            read_generation_authority(object_store.as_ref(), "repo")
                .await
                .unwrap()
                .0,
            first
        );
    }

    #[tokio::test]
    async fn metadata_generation_lifecycle_gates_mutation_rollback_and_retirement() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open("generation-lifecycle", object_store.clone())
            .await
            .unwrap();
        let storage = Storage {
            database: RwLock::new(Database::Writer(db)),
            database_path: "generation-lifecycle".to_owned(),
            coordination_store: object_store.clone(),
            object_store,
            wal_object_store: None,
            wal_metrics: None,
            encryption: EncryptionStatus {
                enabled: false,
                algorithm: "none",
                active_dek_version: 0,
                envelope_generation: 0,
                unlock_slot: None,
                recovery_unlock: false,
                initializing: false,
            },
            key_manager: None,
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            transaction_idle_timeout_ms: 1_000,
            credential_manager: None,
            broker_lease_metadata: None,
            writer_epoch: AtomicU64::new(0),
            wal_target: "inherited",
            wal_durability: "inherited",
        };
        let diagnostic = "aa".repeat(32);
        let quarantined = storage
            .quarantine_generation("repo", 1, diagnostic)
            .await
            .unwrap();
        assert_eq!(quarantined.state, "healing-required");
        assert!(!storage.mutations_allowed("repo").await.unwrap());

        let activated = storage
            .activate_generation(
                "repo",
                1,
                2,
                "candidate-2".into(),
                "bb".repeat(32),
                60_000,
            )
            .await
            .unwrap();
        assert_eq!(activated.state, "post-activation");
        assert!(storage
            .verify_generation("repo", activated.decision, "cc".repeat(32))
            .await
            .is_err());

        let rolled_back = storage
            .rollback_generation("repo", activated.decision, "dd".repeat(32), 0)
            .await
            .unwrap();
        assert_eq!(rolled_back.active_generation, 1);
        assert!(rolled_back.decision > activated.decision);
        let verified = storage
            .verify_generation("repo", rolled_back.decision, "ee".repeat(32))
            .await
            .unwrap();
        assert!(storage.mutations_allowed("repo").await.unwrap());
        let retired = storage
            .retire_generation("repo", verified.decision, 2, "ff".repeat(32))
            .await
            .unwrap();
        assert_eq!(retired.retired_generation, 2);
        assert_eq!(retired.previous_generation, 0);
    }
}
