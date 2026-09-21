#[cfg(test)]
mod tests {
    //! Storage persistence and generation authority tests.

    use super::*;
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    use slatedb_common::metrics::{MetricsRecorder, LATENCY_BOUNDARIES};
    use std::{collections::HashMap, env};
    use vaulticdb::encryption::envelope::{EncryptionConfig, EncryptionMode, ProviderCredentials};

    static STORAGE_FAILPOINT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[derive(Debug, Default)]
    struct ControlledObjectStore {
        inner: InMemory,
        delay_put: std::sync::atomic::AtomicBool,
        fail_put: std::sync::atomic::AtomicBool,
        fail_main_delete: Arc<std::sync::atomic::AtomicBool>,
        put_started: tokio::sync::Notify,
        put_release: tokio::sync::Notify,
    }

    impl std::fmt::Display for ControlledObjectStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("controlled object store")
        }
    }

    #[async_trait]
    impl ObjectStore for ControlledObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> slatedb::object_store::Result<PutResult> {
            if self.delay_put.load(Ordering::Acquire) {
                self.put_started.notify_waiters();
                self.put_release.notified().await;
            }
            if self.fail_put.swap(false, Ordering::AcqRel) {
                return Err(slatedb::object_store::Error::Generic {
                    store: "controlled",
                    source: "injected put failure".into(),
                });
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> slatedb::object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
        ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
            let fail_main_delete = Arc::clone(&self.fail_main_delete);
            self.inner
                .delete_stream(locations)
                .map(move |result| match result {
                    Ok(path)
                        if path.as_ref() == "main-delete"
                            && fail_main_delete.swap(false, Ordering::AcqRel) =>
                    {
                        Err(slatedb::object_store::Error::Generic {
                            store: "controlled",
                            source: "injected delete failure".into(),
                        })
                    }
                    result => result,
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> slatedb::object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> slatedb::object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn slatedb_tuning_preserves_unset_defaults() {
        let defaults = Settings::default();
        let settings = SlateDbTuning {
            flush_interval: Some(std::time::Duration::from_millis(500)),
            max_unflushed_bytes: Some(4 * 1024 * 1024 * 1024),
            l0_sst_size_bytes: None,
        }
        .settings();

        assert_eq!(
            settings.flush_interval,
            Some(std::time::Duration::from_millis(500))
        );
        assert_eq!(settings.max_unflushed_bytes, 4 * 1024 * 1024 * 1024);
        assert_eq!(settings.l0_sst_size_bytes, defaults.l0_sst_size_bytes);
    }

    async fn listed_paths(store: &dyn ObjectStore) -> Vec<String> {
        let mut paths = Vec::new();
        let mut objects = store.list(None);
        while let Some(object) = objects.next().await {
            paths.push(object.unwrap().location.to_string());
        }
        paths
    }

    fn compacted_ulid(path: &str) -> Option<&str> {
        let name = path.split_once("compacted/")?.1.strip_suffix(".sst")?;
        (name.len() == 26
            && name.bytes().all(|byte| {
                byte.is_ascii_digit()
                    || matches!(byte, b'A'..=b'H' | b'J'..=b'N' | b'P'..=b'T' | b'V'..=b'Z')
            }))
        .then_some(name)
    }

    fn cache_storage_config(confidentiality: cache::CacheConfidentiality) -> StorageConfig {
        StorageConfig {
            object_store: ObjectStoreConfig::Memory,
            wal_store: WalStoreConfig::Inherit,
            slatedb_tuning: SlateDbTuning::default(),
            cache: cache::CacheConfig {
                tiers: vec![cache::CacheTierConfig {
                    id: "memory".to_owned(),
                    store: ReplicaStoreConfig::Memory,
                    confidentiality,
                    policy: cache::CacheTierPolicy {
                        enabled: true,
                        max_bytes: 64 * 1024,
                        idle_age_ms: 0,
                        absolute_age_ms: None,
                        read_priority: 1,
                        admission_priority: 1,
                        timeout_ms: 250,
                    },
                }],
                aggregate_max_bytes: Some(64 * 1024),
                part_size_bytes: 4096,
                max_inflight_bytes: 8192,
                max_background_tasks: 2,
            },
            fencing_replica: None,
            metadata_rebuild_initialize: false,
            metadata_rebuild_reset: false,
            bulk_import_local_wal_data_dir: None,
            broker: None,
            encryption: EncryptionConfig {
                mode: EncryptionMode::Off,
                passphrase_file: None,
                recovery_acknowledged: false,
                provider_credentials: ProviderCredentials::new(HashMap::new()),
            },
            transaction_idle_timeout_ms: 1_000,
            slatedb_multiget: false,
            attribution_disabled: false,
            topology_source: TopologySource::External,
            topology_override_local: None,
        }
    }

    #[tokio::test]
    async fn disabled_attribution_preserves_operations_without_wrappers_or_metrics() {
        let repository_id = format!("disabled-attribution-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.cache = cache::CacheConfig::default();
        config.attribution_disabled = true;
        let storage = Storage::open(&repository_id, &config).await.unwrap();

        assert!(!storage.object_store.to_string().contains("role-aware"));
        assert!(!storage
            .coordination_store
            .to_string()
            .contains("role-aware"));
        assert!(
            storage
                .write_batch(&WriteBatchRequest {
                    puts: vec![KeyValue {
                        key: b"key".to_vec(),
                        value: b"value".to_vec(),
                    }],
                    await_durable: true,
                    ..Default::default()
                })
                .await
                .unwrap()
                .durable
        );
        assert_eq!(storage.get(b"key", "").await.unwrap().value, b"value");

        for snapshot in [
            storage.attribution.transaction_begin.snapshot(),
            storage.attribution.engine_submit.snapshot(),
            storage.attribution.durable_wait.snapshot(),
            storage.attribution.finalization.snapshot(),
            storage.attribution.object_store_main.snapshot().put.timing,
            storage.attribution.object_store_main.snapshot().get.timing,
            storage
                .attribution
                .object_store_coordination
                .snapshot()
                .put
                .timing,
        ] {
            assert_eq!(snapshot.attempts, 0);
            assert_eq!(snapshot.completed, 0);
            assert_eq!(snapshot.active, 0);
        }
        let engine = storage.engine_metrics_snapshot();
        assert_eq!(engine.write_batches, 0);
        assert_eq!(engine.write_ops, 0);
        assert_eq!(engine.batch_write_queue.completed, 0);
        assert_eq!(engine.batch_write_service.completed, 0);
        assert!(
            !storage
                .attribution
                .object_store_main
                .snapshot()
                .put
                .transferred_bytes_available
        );
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn encryption_off_rejects_encrypted_cache_tiers() {
        let repository_id = format!("cache-encryption-off-{}", rand::random::<u64>());
        let error = match Storage::open(
            &repository_id,
            &cache_storage_config(cache::CacheConfidentiality::Encrypted),
        )
        .await
        {
            Ok(storage) => {
                storage.close().await.unwrap();
                panic!("encrypted cache tier unexpectedly opened without metadata encryption")
            }
            Err(error) => error.to_string(),
        };
        assert!(error.contains("encrypted read-cache tiers require metadata encryption"));
    }

    #[tokio::test]
    async fn metadata_rebuild_reset_allows_volatile_memory_read_cache() {
        let repository_id = format!("reset-cache-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        let root = env::temp_dir().join(format!(
            "vaulticdb-reset-memory-cache-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        config.object_store = ObjectStoreConfig::Local { root: root.clone() };

        let initial = Storage::open(&repository_id, &config).await.unwrap();
        let database = initial.database.read().await;
        let Database::Writer(db) = &*database else {
            panic!("initial cache candidate did not open as writer")
        };
        db.put(b"p:stale", b"old".to_vec())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        drop(database);
        initial.close().await.unwrap();

        config.metadata_rebuild_reset = true;
        let storage = Storage::open(&repository_id, &config).await.unwrap();
        assert!(storage.read_value(b"p:stale").await.unwrap().is_none());
        let status = storage.cache_status().unwrap();
        assert_eq!(status.tiers.len(), 1);
        assert_eq!(
            status.tiers[0].confidentiality,
            cache::CacheConfidentiality::DecryptedHighlyTrusted
        );
        storage.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metadata_rebuild_reset_rejects_persistent_read_cache() {
        let repository_id = format!("reset-persistent-cache-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.metadata_rebuild_reset = true;
        config.cache.tiers[0].store = ReplicaStoreConfig::Local {
            root: env::temp_dir().join(format!("vaulticdb-reset-cache-{}", rand::random::<u64>())),
        };
        let error = match Storage::open(&repository_id, &config).await {
            Ok(storage) => {
                storage.close().await.unwrap();
                panic!("metadata reset unexpectedly accepted a persistent read cache")
            }
            Err(error) => error.to_string(),
        };
        assert!(error.contains("permits only volatile memory read-cache tiers"));
    }

    #[tokio::test]
    async fn encryption_off_allows_decrypted_highly_trusted_cache_tiers() {
        let repository_id = format!("cache-decrypted-off-{}", rand::random::<u64>());
        let storage = Storage::open(
            &repository_id,
            &cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted),
        )
        .await
        .unwrap();
        assert_eq!(
            storage.cache_status().unwrap().tiers[0].confidentiality,
            cache::CacheConfidentiality::DecryptedHighlyTrusted
        );
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn dedicated_wal_inventory_failure_precedes_cache_startup() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let repository_id = format!("failed-wal-inventory-cache-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.wal_store = WalStoreConfig::Store(ReplicaStoreConfig::Memory);
        arm_storage_failpoint(StorageFailpoint::InventoryWal);

        let error = match Storage::open(&repository_id, &config).await {
            Ok(storage) => {
                storage.close().await.unwrap();
                panic!("WAL inventory unexpectedly succeeded")
            }
            Err(error) => format!("{error:#}"),
        };

        assert!(error.contains("InventoryWal"));
        assert!(!cache::has_test_inspection(&repository_id));
    }

    #[tokio::test]
    async fn encryption_failure_preserves_primary_when_cache_close_fails() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let repository_id = format!("failed-encryption-cache-close-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::Encrypted);
        config.encryption.mode = EncryptionMode::Required;
        arm_storage_failpoint(StorageFailpoint::CloseCache);

        let error = match Storage::open(&repository_id, &config).await {
            Ok(storage) => {
                storage.close().await.unwrap();
                panic!("metadata encryption unexpectedly configured")
            }
            Err(error) => format!("{error:#}"),
        };

        assert!(error.starts_with("configure metadata encryption"));
        assert!(error.contains("key envelope is missing"));
        assert!(error.contains("cleanup failures: close read-cache quota coordinator"));
        assert!(error.contains("CloseCache"));
        assert_failed_open_cache_is_fenced(&repository_id).await;
    }

    #[test]
    fn storage_failpoints_fire_once() {
        let failpoint = StorageFailpoint::OpenWriter("fail-once".to_owned());
        arm_storage_failpoint(failpoint.clone());
        assert!(check_storage_failpoint(failpoint.clone()).is_err());
        assert!(check_storage_failpoint(failpoint).is_ok());
    }

    async fn assert_failed_open_cache_is_fenced(repository_id: &str) {
        let inspection = cache::test_inspection(repository_id);
        let returned = inspection.state().await;
        assert!(returned.closing);
        assert_eq!(returned.background_task_count, 0);
        assert!(returned.ledger_lease_expired || returned.conservatively_fenced);
        tokio::task::yield_now().await;
        let settled = inspection.state().await;
        assert_eq!(settled.background_task_count, 0);
        assert_eq!(settled.cache_write_count, returned.cache_write_count);
    }

    #[tokio::test]
    async fn failed_writer_open_preserves_release_failure_and_closes_cache() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let repository_id = format!("failed-writer-open-cache-{}", rand::random::<u64>());
        let config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        let (path, _) = object_store(&repository_id, &config.object_store).unwrap();
        arm_storage_failpoint(StorageFailpoint::OpenWriter(path.clone()));
        arm_storage_failpoint(StorageFailpoint::ReleaseWriterClaimAfterFailedOpen(path));

        let error = match Storage::open(&repository_id, &config).await {
            Ok(storage) => {
                storage.close().await.unwrap();
                panic!("writer open unexpectedly succeeded")
            }
            Err(error) => format!("{error:#}"),
        };

        assert!(error.starts_with("open SlateDB database: injected storage failure"));
        assert!(error.contains("cleanup failures: release writer claim"));
        assert!(error.contains("ReleaseWriterClaimAfterFailedOpen"));
        assert_failed_open_cache_is_fenced(&repository_id).await;
    }

    #[tokio::test]
    async fn failed_latest_epoch_preserves_reader_close_failure_and_closes_cache() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let repository_id = format!("failed-reader-epoch-cache-{}", rand::random::<u64>());
        let root = std::env::temp_dir().join(&repository_id);
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.object_store = ObjectStoreConfig::Local { root: root.clone() };
        Storage::open(&repository_id, &config)
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        let (path, _) = object_store(&repository_id, &config.object_store).unwrap();
        arm_storage_failpoint(StorageFailpoint::WriterClaimUnavailable(path.clone()));
        arm_storage_failpoint(StorageFailpoint::ObserveLatestWriterEpoch(path.clone()));
        arm_storage_failpoint(StorageFailpoint::CloseReader(path));

        let error = match Storage::open(&repository_id, &config).await {
            Ok(storage) => {
                storage.close().await.unwrap();
                panic!("reader epoch observation unexpectedly succeeded")
            }
            Err(error) => format!("{error:#}"),
        };

        assert!(error.starts_with("observe latest SlateDB writer epoch: injected storage failure"));
        assert!(error.contains("cleanup failures: close SlateDB reader"));
        assert!(error.contains("CloseReader"));
        assert_failed_open_cache_is_fenced(&repository_id).await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn slatedb_compacted_sst_paths_are_unique_ulid_generations() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = format!("sst-path-contract-{}", rand::random::<u64>());
        let db = open_writer(
            &database,
            object_store.clone(),
            None,
            &SlateDbTuning::default(),
        )
        .await
        .unwrap();
        let mut compacted = std::collections::HashSet::new();
        for (key, value) in [
            (b"first".as_slice(), b"one".as_slice()),
            (b"second", b"two"),
        ] {
            let mut batch = WriteBatch::new();
            batch.put(key, value);
            db.write(batch)
                .await
                .unwrap()
                .await_durable()
                .await
                .unwrap();
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
            let paths = listed_paths(object_store.as_ref()).await;
            let observed = paths
                .iter()
                .filter_map(|path| compacted_ulid(path).map(str::to_owned))
                .collect::<Vec<_>>();
            assert!(
                !observed.is_empty(),
                "flush did not publish a compacted ULID SST: {paths:?}"
            );
            compacted.extend(observed);
        }
        db.close().await.unwrap();
        assert!(
            compacted.len() >= 2,
            "each flush must publish a fresh compacted SST identity"
        );
    }

    #[tokio::test]
    async fn engine_metrics_snapshot_reports_real_writes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = format!("engine-metrics-{}", rand::random::<u64>());
        let engine_metrics = Arc::new(DefaultMetricsRecorder::new());
        let db = open_writer_with_metrics(
            &database,
            object_store.clone(),
            None,
            &SlateDbTuning::default(),
            engine_metrics.clone(),
        )
        .await
        .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"key", b"value");
        db.write(batch)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.close().await.unwrap();

        let counter = |name, outcome| {
            engine_metrics.register_counter(
                name,
                "",
                &[(slatedb::db_stats::OUTCOME_LABEL, outcome)],
            )
        };
        counter(
            slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_SUCCESS,
        )
        .increment(2);
        counter(
            slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_FAILURE,
        )
        .increment(3);
        counter(
            slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_CANCELLATION,
        )
        .increment(4);
        counter(
            slatedb::db_stats::BACKPRESSURE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_TIMEOUT,
        )
        .increment(5);
        engine_metrics
            .register_up_down_counter(slatedb::db_stats::BACKPRESSURE_WAITERS, "", &[])
            .increment(2);
        let now_unix_ms: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();
        engine_metrics
            .register_gauge(
                slatedb::db_stats::BACKPRESSURE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS,
                "",
                &[],
            )
            .set(now_unix_ms.saturating_sub(5));
        let backpressure_histogram = engine_metrics.register_histogram(
            slatedb::db_stats::BACKPRESSURE_WAIT_SECONDS,
            "",
            &[],
            LATENCY_BOUNDARIES,
        );
        backpressure_histogram.record(0.002);
        backpressure_histogram.record(20.0);

        counter(
            slatedb::db_stats::BATCH_WRITE_QUEUE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_FAILURE,
        )
        .increment(2);
        counter(
            slatedb::db_stats::BATCH_WRITE_QUEUE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_CANCELLATION,
        )
        .increment(3);
        counter(
            slatedb::db_stats::BATCH_WRITE_SERVICE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_FAILURE,
        )
        .increment(4);
        counter(
            slatedb::db_stats::BATCH_WRITE_SERVICE_OUTCOME_COUNT,
            slatedb::db_stats::OUTCOME_CANCELLATION,
        )
        .increment(5);
        for (name, value) in [
            (slatedb::db_stats::MULTI_GET_CALLS, 6),
            (slatedb::db_stats::MULTI_GET_INPUT_KEYS, 7),
            (slatedb::db_stats::MULTI_GET_UNIQUE_KEYS, 8),
            (slatedb::db_stats::MULTI_GET_SST_VISITS, 9),
            (slatedb::db_stats::MULTI_GET_CANDIDATE_KEYS, 10),
            (slatedb::db_stats::MULTI_GET_NEEDED_BLOCKS, 11),
            (slatedb::db_stats::MULTI_GET_COALESCED_READS, 12),
            (slatedb::db_stats::MULTI_GET_NEEDED_BLOCK_BYTES, 13),
            (slatedb::db_stats::MULTI_GET_COALESCED_READ_BYTES, 14),
            (slatedb::db_stats::MULTI_GET_PROJECTED_READS_GAP_8, 15),
            (slatedb::db_stats::MULTI_GET_PROJECTED_READ_BYTES_GAP_8, 16),
            (slatedb::db_stats::MULTI_GET_PROJECTED_READS_GAP_32, 17),
            (slatedb::db_stats::MULTI_GET_PROJECTED_READ_BYTES_GAP_32, 18),
            (slatedb::db_stats::MULTI_GET_PROJECTED_READS_GAP_128, 19),
            (
                slatedb::db_stats::MULTI_GET_PROJECTED_READ_BYTES_GAP_128,
                20,
            ),
        ] {
            engine_metrics
                .register_counter(name, "", &[])
                .increment(value);
        }
        engine_metrics
            .register_up_down_counter(slatedb::db_stats::BATCH_WRITE_SERVICE_ACTIVE, "", &[])
            .increment(1);
        engine_metrics
            .register_gauge(
                slatedb::db_stats::BATCH_WRITE_SERVICE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS,
                "",
                &[],
            )
            .set(now_unix_ms.saturating_add(60_000));

        let mut storage = transition_storage(Database::Writer(db), database, object_store, 1);
        storage.engine_metrics = Some(engine_metrics);
        let engine = storage.engine_metrics_snapshot();
        assert_eq!(engine.write_batches, 1);
        assert_eq!(engine.write_ops, 1);
        assert!(engine.memtable_write_bytes > 0);
        assert_eq!(engine.multi_get_calls, 6);
        assert_eq!(engine.multi_get_input_keys, 7);
        assert_eq!(engine.multi_get_unique_keys, 8);
        assert_eq!(engine.multi_get_sst_visits, 9);
        assert_eq!(engine.multi_get_candidate_keys, 10);
        assert_eq!(engine.multi_get_needed_blocks, 11);
        assert_eq!(engine.multi_get_coalesced_reads, 12);
        assert_eq!(engine.multi_get_needed_block_bytes, 13);
        assert_eq!(engine.multi_get_coalesced_read_bytes, 14);
        assert_eq!(engine.multi_get_projected_reads_gap_8, 15);
        assert_eq!(engine.multi_get_projected_read_bytes_gap_8, 16);
        assert_eq!(engine.multi_get_projected_reads_gap_32, 17);
        assert_eq!(engine.multi_get_projected_read_bytes_gap_32, 18);
        assert_eq!(engine.multi_get_projected_reads_gap_128, 19);
        assert_eq!(engine.multi_get_projected_read_bytes_gap_128, 20);
        assert_eq!(engine.backpressure.active, 2);
        assert!((4_000..=20_000).contains(&engine.backpressure.oldest_active_us));
        assert_eq!(engine.backpressure.completed, 2);
        assert_eq!(engine.backpressure.successes, 2);
        assert_eq!(engine.backpressure.failures, 3);
        assert_eq!(engine.backpressure.cancellations, 4);
        assert_eq!(engine.backpressure.timeouts, 5);
        assert_eq!(engine.backpressure.total_us, 20_002_000);
        assert_eq!(engine.backpressure.max_us, 20_000_000);
        assert_eq!(engine.backpressure.latency_bucket_upper_us.len(), 13);
        assert_eq!(engine.backpressure.latency_bucket_upper_us[0], 1_000);
        assert_eq!(engine.backpressure.latency_bucket_upper_us[12], u64::MAX);
        assert_eq!(engine.backpressure.latency_bucket_counts[1], 1);
        assert_eq!(engine.backpressure.latency_bucket_counts[12], 1);
        assert_eq!(engine.batch_write_queue_depth, 0);
        assert_eq!(engine.batch_write_queue.successes, 1);
        assert_eq!(engine.batch_write_queue.failures, 2);
        assert_eq!(engine.batch_write_queue.cancellations, 3);
        assert_eq!(engine.batch_write_queue.completed, 1);
        assert_eq!(engine.batch_write_queue.latency_bucket_upper_us.len(), 13);
        assert_eq!(
            engine
                .batch_write_queue
                .latency_bucket_counts
                .iter()
                .sum::<u64>(),
            1
        );
        assert_eq!(engine.batch_write_service.successes, 1);
        assert_eq!(engine.batch_write_service.failures, 4);
        assert_eq!(engine.batch_write_service.cancellations, 5);
        assert_eq!(engine.batch_write_service.active, 1);
        assert_eq!(engine.batch_write_service.oldest_active_us, 0);
        assert_eq!(engine.batch_write_service.completed, 1);
        assert_eq!(engine.batch_write_service.latency_bucket_upper_us.len(), 13);
        assert_eq!(
            engine
                .batch_write_service
                .latency_bucket_counts
                .iter()
                .sum::<u64>(),
            1
        );
        assert_eq!(oldest_active_age_us(0, 100), 0);
        assert_eq!(oldest_active_age_us(101, 100), 0);
    }

    fn transition_storage(
        database: Database,
        path: String,
        object_store: Arc<dyn ObjectStore>,
        epoch: u64,
    ) -> Storage {
        Storage {
            database: RwLock::new(database),
            database_path: path,
            coordination_store: object_store.clone(),
            object_store,
            cache_manager: None,
            wal_object_store: None,
            wal_metrics: None,
            attribution: Arc::new(StorageAttribution::default()),
            engine_metrics: Some(Arc::new(DefaultMetricsRecorder::new())),
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
            capsule_migration: Mutex::new(()),
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            last_applied_engine_sequence: AtomicU64::new(0),
            durable_engine_sequence: AtomicU64::new(0),
            latest_write_handle: Mutex::new(None),
            transaction_idle_timeout_ms: 1_000,
            slatedb_multiget: false,
            slatedb_tuning: SlateDbTuning::default(),
            metadata_rebuild_reset: false,
            bulk_import_local_wal_data_dir: None,
            credential_manager: None,
            broker_lease_metadata: None,
            writer_epoch: AtomicU64::new(epoch),
            wal_target: "inherited",
            wal_durability: "inherited",
        }
    }

    #[tokio::test]
    async fn failed_promotion_open_releases_claim_and_recovers_reader() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("failed-promotion-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        writer.close().await.unwrap();
        let reader = open_reader(&path, object_store.clone(), None)
            .await
            .unwrap();
        let storage = transition_storage(
            Database::Reader(reader),
            path.clone(),
            object_store.clone(),
            1,
        );

        arm_storage_failpoint(StorageFailpoint::OpenWriter(path.clone()));
        let failure = storage.promote(Some(1)).await.unwrap_err();

        assert_eq!(failure.database, DatabaseState::Reader);
        assert!(!failure.claim_held);
        assert_eq!(failure.epoch, 2);
        assert_eq!(
            active_writer_epoch(object_store.as_ref()).await.unwrap(),
            None
        );
        assert!(matches!(
            &*storage.database.read().await,
            Database::Reader(_)
        ));
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_demotion_release_keeps_reader_and_reports_claim() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("failed-demotion-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = transition_storage(Database::Writer(writer), path, object_store.clone(), 1);

        arm_storage_failpoint(StorageFailpoint::ReleaseWriterClaim(object_store.as_ref()
            as *const dyn ObjectStore
            as *const ()
            as usize));
        let failure = storage.demote().await.unwrap_err();

        assert_eq!(failure.database, DatabaseState::Reader);
        assert!(failure.claim_held);
        assert_eq!(failure.epoch, 1);
        assert_eq!(
            active_writer_epoch(object_store.as_ref()).await.unwrap(),
            Some(1)
        );
        assert!(matches!(
            &*storage.database.read().await,
            Database::Reader(_)
        ));
        storage.close().await.unwrap();
        release_writer_claim(object_store.as_ref(), 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failed_commit_reports_consumed_transaction() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("failed-commit-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = transition_storage(Database::Writer(writer), path.clone(), object_store, 1);
        let transaction_id = storage.begin().await.unwrap().transaction_id;
        assert_eq!(storage.transactions.read().await.len(), 1);

        arm_storage_failpoint(StorageFailpoint::BeforeTransactionCommit(path));
        let failure = storage
            .commit(&transaction_id, "", false, false)
            .await
            .unwrap_err();

        assert!(failure.consumed);
        assert_eq!(storage.transactions.read().await.len(), 0);
        assert_eq!(storage.attribution.engine_submit.snapshot().attempts, 1);
        assert_eq!(storage.attribution.engine_submit.snapshot().failures, 1);
        let missing = storage
            .commit("unknown", "", false, false)
            .await
            .unwrap_err();
        assert!(!missing.consumed);
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn deferred_commit_requires_rebuild_reset_and_skips_durability_wait() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("deferred-commit-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let mut storage =
            transition_storage(Database::Writer(writer), path.clone(), object_store, 1);
        let transaction_id = storage.begin().await.unwrap().transaction_id;
        storage
            .write_batch(&WriteBatchRequest {
                transaction_id: transaction_id.clone(),
                puts: vec![KeyValue {
                    key: b"deferred".to_vec(),
                    value: b"value".to_vec(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let rejected = storage
            .commit(&transaction_id, "", true, false)
            .await
            .unwrap_err();
        assert!(!rejected.consumed);
        assert_eq!(rejected.status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(storage.transactions.read().await.len(), 1);
        assert_eq!(storage.attribution.durable_wait.snapshot().attempts, 0);

        storage.metadata_rebuild_reset = true;
        arm_storage_failpoint(StorageFailpoint::BeforeTransactionDurability(path.clone()));
        assert!(
            storage
                .commit(&transaction_id, "", true, false)
                .await
                .unwrap()
                .consumed
        );
        assert_eq!(storage.last_durable_sequence.load(Ordering::Acquire), 0);
        assert_eq!(storage.attribution.durable_wait.snapshot().attempts, 0);

        let durable_id = storage.begin().await.unwrap().transaction_id;
        storage
            .write_batch(&WriteBatchRequest {
                transaction_id: durable_id.clone(),
                puts: vec![KeyValue {
                    key: b"durable".to_vec(),
                    value: b"value".to_vec(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        let failure = storage
            .commit(&durable_id, "", false, false)
            .await
            .unwrap_err();
        assert!(failure.consumed);
        assert_eq!(storage.attribution.engine_submit.snapshot().attempts, 2);
        assert_eq!(storage.attribution.engine_submit.snapshot().failures, 0);
        assert_eq!(storage.attribution.durable_wait.snapshot().attempts, 1);
        assert_eq!(storage.attribution.durable_wait.snapshot().failures, 1);
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn token_required_commit_rejects_memory_wal_before_consumption() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("memory-token-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let mut storage = transition_storage(Database::Writer(writer), path, object_store, 1);
        storage.metadata_rebuild_reset = true;
        storage.wal_target = "memory";
        let transaction_id = storage.begin().await.unwrap().transaction_id;
        storage
            .write_batch(&WriteBatchRequest {
                transaction_id: transaction_id.clone(),
                puts: vec![KeyValue {
                    key: b"memory-token".to_vec(),
                    value: b"value".to_vec(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let rejected = storage
            .commit(&transaction_id, "", true, true)
            .await
            .unwrap_err();
        assert!(!rejected.consumed);
        assert_eq!(rejected.status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(storage.transactions.read().await.len(), 1);
        assert_eq!(storage.read_value(b"memory-token").await.unwrap(), None);

        assert!(
            storage
                .commit(&transaction_id, "", true, false)
                .await
                .unwrap()
                .consumed
        );
        assert_eq!(
            storage
                .read_value(b"memory-token")
                .await
                .unwrap()
                .as_deref(),
            Some(&b"value"[..])
        );
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn writer_fence_check_distinguishes_a_stale_claim() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("stale-fence-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = transition_storage(Database::Writer(writer), path, object_store.clone(), 1);
        release_writer_claim(object_store.as_ref(), 1)
            .await
            .unwrap();
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(2)
        );

        assert!(matches!(
            storage.ensure_writer_fence().await,
            Err(WriterFenceFailure::Stale { observed_epoch: 2 })
        ));
        assert!(storage.close().await.is_err());
        assert_eq!(storage.attribution.finalization.snapshot().failures, 1);
        release_writer_claim(object_store.as_ref(), 2)
            .await
            .unwrap();
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
    async fn object_store_metrics_keep_stalled_put_active_and_cancel_on_drop() {
        let controlled = Arc::new(ControlledObjectStore::default());
        controlled.delay_put.store(true, Ordering::Release);
        let metrics = Arc::new(ObjectStoreRoleMetrics::default());
        let store = role_aware_object_store(
            controlled.clone(),
            metrics.clone(),
            None,
            ObjectStoreRole::Main,
            None,
        );
        let pending_store = store.clone();
        let pending = tokio::spawn(async move {
            pending_store
                .put(
                    &Path::from("stalled"),
                    Bytes::from_static(b"payload").into(),
                )
                .await
        });
        controlled.put_started.notified().await;
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        let active = metrics.snapshot().put.timing;
        assert_eq!(active.attempts, 1);
        assert_eq!(active.active, 1);
        assert!(active.oldest_active_us > 0);

        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        let cancelled = metrics.snapshot().put.timing;
        assert_eq!(cancelled.active, 0);
        assert_eq!(cancelled.cancellations, 1);
        assert_eq!(cancelled.completed, 1);
    }

    #[test]
    fn object_delay_profile_accepts_shared_schema_only_for_exact_target() {
        let encoded = r#"{
            "schema_version":1,
            "profile_id":"m2-rust-shared",
            "enabled":true,
            "test_only":true,
            "scenario":"local",
            "backend":"local",
            "mode":"service",
            "operation":"import",
            "role":"wal",
            "method":"put",
            "access_pattern":"sequential",
            "target_id":"test-repository",
            "resource_id":"local-fixture-device",
            "placement":"inside_service",
            "latency_semantics":"service_completion",
            "interpretation":"additive",
            "endpoint":"dependency",
            "acknowledgement":"unchanged",
            "delay_us":25000,
            "jitter_us":0,
            "tail_delay_us":0,
            "tail_every":0,
            "correlated_for":0,
            "bandwidth_bytes_per_second":0,
            "concurrency":1,
            "deadline_ms":0,
            "max_retries":0,
            "retry_error":"none",
            "seed":34,
            "holds":["local-fixture-device"],
            "unknowns":[]
        }"#;

        let profile = TestObjectDelayProfile::parse(encoded, "test-repository").unwrap();
        assert_eq!(profile.role, ObjectStoreRole::Wal);
        assert_eq!(profile.operation, "put");
        assert_eq!(
            profile.delay_duration(ObjectStoreRole::Wal, "put"),
            Some(std::time::Duration::from_millis(25))
        );
        assert!(TestObjectDelayProfile::parse(encoded, "other-repository").is_err());
        assert!(TestObjectDelayProfile::parse(
            &encoded.replace("\"endpoint\":\"dependency\"", "\"endpoint\":\"caller\""),
            "test-repository",
        )
        .is_err());
    }

    #[tokio::test]
    async fn object_delay_profile_targets_only_wal_puts() {
        let profile = Arc::new(
            TestObjectDelayProfile::parse(
                r#"{"version":1,"target":"isolated","role":"wal","operation":"put","delay_ms":25}"#,
                "test-repository",
            )
            .unwrap(),
        );
        let main_metrics = Arc::new(ObjectStoreRoleMetrics::default());
        let wal_metrics = Arc::new(ObjectStoreRoleMetrics::default());
        let main = role_aware_object_store(
            Arc::new(InMemory::new()),
            main_metrics.clone(),
            Some(wal_metrics.clone()),
            ObjectStoreRole::Main,
            Some(profile),
        );

        main.put(&Path::from("main"), Bytes::from_static(b"main").into())
            .await
            .unwrap();

        let mut wal_options = PutOptions::default();
        wal_options
            .extensions
            .insert(slatedb::object_store_tag::ObjectStoreCallTag::new(
                slatedb::object_store_tag::TableStoreKind::Main,
                slatedb::object_store_tag::SstType::Wal,
            ));
        let wal_started = Instant::now();
        main.put_opts(
            &Path::from("database/wal/0001.sst"),
            Bytes::from_static(b"wal").into(),
            wal_options,
        )
        .await
        .unwrap();
        assert!(wal_started.elapsed() >= std::time::Duration::from_millis(25));
        assert_eq!(main_metrics.snapshot().put.timing.attempts, 1);
        assert_eq!(wal_metrics.snapshot().put.timing.attempts, 1);
    }

    #[tokio::test]
    async fn object_delay_profile_delays_get_body_consumption_and_listing() {
        let inner = Arc::new(InMemory::new());
        inner
            .put(&Path::from("object"), Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        let body_profile = Arc::new(
            TestObjectDelayProfile::parse(
                r#"{"version":1,"target":"isolated","role":"main","operation":"get_body","delay_ms":25}"#,
                "test-repository",
            )
            .unwrap(),
        );
        let store = role_aware_object_store(
            inner.clone(),
            Arc::new(ObjectStoreRoleMetrics::default()),
            None,
            ObjectStoreRole::Main,
            Some(body_profile),
        );
        let response_started = Instant::now();
        let result = store.get(&Path::from("object")).await.unwrap();
        assert!(response_started.elapsed() < std::time::Duration::from_millis(20));
        let body_started = Instant::now();
        assert_eq!(
            result.bytes().await.unwrap(),
            Bytes::from_static(b"payload")
        );
        assert!(body_started.elapsed() >= std::time::Duration::from_millis(25));

        let list_profile = Arc::new(
            TestObjectDelayProfile::parse(
                r#"{"version":1,"target":"isolated","role":"main","operation":"list","delay_ms":25}"#,
                "test-repository",
            )
            .unwrap(),
        );
        let list_store = role_aware_object_store(
            inner,
            Arc::new(ObjectStoreRoleMetrics::default()),
            None,
            ObjectStoreRole::Main,
            Some(list_profile),
        );
        let mut objects = list_store.list(None);
        let list_started = Instant::now();
        assert!(objects.next().await.unwrap().is_ok());
        assert!(list_started.elapsed() >= std::time::Duration::from_millis(25));
    }

    #[tokio::test]
    async fn object_delay_profile_releases_capacity_when_body_consumption_is_cancelled() {
        let profile = Arc::new(
            TestObjectDelayProfile::parse(
                r#"{"version":1,"target":"isolated","role":"main","operation":"get_body","delay_ms":250,"concurrency":1}"#,
                "test-repository",
            )
            .unwrap(),
        );
        let store = role_aware_object_store(
            Arc::new(InMemory::new()),
            Arc::new(ObjectStoreRoleMetrics::default()),
            None,
            ObjectStoreRole::Main,
            Some(profile),
        );
        store
            .put(
                &Path::from("cancelled"),
                Bytes::from_static(b"cancelled").into(),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from("after-cancel"),
                Bytes::from_static(b"available").into(),
            )
            .await
            .unwrap();
        let result = store.get(&Path::from("cancelled")).await.unwrap();
        let pending = tokio::spawn(async move { result.bytes().await });
        tokio::task::yield_now().await;
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());

        let result = store.get(&Path::from("after-cancel")).await.unwrap();
        let bytes = tokio::time::timeout(std::time::Duration::from_millis(500), result.bytes())
            .await
            .expect("cancelled body released shared capacity")
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"available"));
    }

    #[tokio::test]
    async fn object_delay_profile_targets_multipart_completion() {
        let profile = Arc::new(
            TestObjectDelayProfile::parse(
                r#"{"version":1,"target":"isolated","role":"main","operation":"multipart_complete","delay_ms":25}"#,
                "test-repository",
            )
            .unwrap(),
        );
        let store = role_aware_object_store(
            Arc::new(InMemory::new()),
            Arc::new(ObjectStoreRoleMetrics::default()),
            None,
            ObjectStoreRole::Main,
            Some(profile),
        );
        let mut upload = store.put_multipart(&Path::from("multipart")).await.unwrap();
        let part_started = Instant::now();
        upload
            .put_part(Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        assert!(part_started.elapsed() < std::time::Duration::from_millis(20));
        let complete_started = Instant::now();
        upload.complete().await.unwrap();
        assert!(complete_started.elapsed() >= std::time::Duration::from_millis(25));
    }

    #[tokio::test]
    async fn object_delay_profile_preserves_conditional_puts() {
        let profile = Arc::new(
            TestObjectDelayProfile::parse(
                r#"{"version":1,"target":"isolated","role":"main","operation":"conditional_put","delay_ms":1}"#,
                "test-repository",
            )
            .unwrap(),
        );
        let store = role_aware_object_store(
            Arc::new(InMemory::new()),
            Arc::new(ObjectStoreRoleMetrics::default()),
            None,
            ObjectStoreRole::Main,
            Some(profile),
        );
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        store
            .put_opts(
                &Path::from("conditional"),
                Bytes::from_static(b"first").into(),
                options.clone(),
            )
            .await
            .unwrap();
        assert!(store
            .put_opts(
                &Path::from("conditional"),
                Bytes::from_static(b"second").into(),
                options,
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn object_delay_profile_shares_bounded_capacity() {
        let profile = Arc::new(
            TestObjectDelayProfile::parse(
                r#"{"version":1,"target":"isolated","role":"main","operation":"put","delay_ms":25,"concurrency":1}"#,
                "test-repository",
            )
            .unwrap(),
        );
        let store = role_aware_object_store(
            Arc::new(InMemory::new()),
            Arc::new(ObjectStoreRoleMetrics::default()),
            None,
            ObjectStoreRole::Main,
            Some(profile),
        );
        let first_store = store.clone();
        let first = tokio::spawn(async move {
            first_store
                .put(&Path::from("first"), Bytes::from_static(b"first").into())
                .await
        });
        let second_store = store.clone();
        let second = tokio::spawn(async move {
            second_store
                .put(&Path::from("second"), Bytes::from_static(b"second").into())
                .await
        });
        let started = Instant::now();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
    }

    #[tokio::test]
    async fn durability_fence_coalesces_waiters_and_covers_prefix() {
        let entered = Arc::new(AtomicU64::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let handle = WriteHandle::new(10, 0, {
            let entered = entered.clone();
            let release = release.clone();
            move || {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.fetch_add(1, Ordering::AcqRel);
                    release.notified().await;
                    Ok(())
                }
            }
        });
        let latest = Mutex::new(Some(handle));
        let durable = AtomicU64::new(0);
        let release_waiters = async {
            while entered.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
            release.notify_waiters();
        };
        let (first, second, ()) = tokio::join!(
            await_engine_durable_through(&latest, &durable, 10, 5),
            await_engine_durable_through(&latest, &durable, 10, 10),
            release_waiters,
        );
        assert_eq!(first.unwrap(), 10);
        assert_eq!(second.unwrap(), 10);
        assert_eq!(durable.load(Ordering::Acquire), 10);
        assert_eq!(
            await_engine_durable_through(&latest, &durable, 10, 5)
                .await
                .unwrap(),
            10
        );
        assert_eq!(entered.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn durability_fence_rejects_future_sequence_and_propagates_wal_failure() {
        let failed = WriteHandle::new(7, 0, || async {
            Err(slatedb::Error::unavailable(
                "injected WAL failure".to_owned(),
            ))
        });
        let latest = Mutex::new(Some(failed));
        let durable = AtomicU64::new(0);
        let future = await_engine_durable_through(&latest, &durable, 7, 8)
            .await
            .unwrap_err();
        assert_eq!(future.code(), tonic::Code::FailedPrecondition);
        let wal = await_engine_durable_through(&latest, &durable, 7, 7)
            .await
            .unwrap_err();
        assert_eq!(wal.code(), tonic::Code::Unavailable);
        assert_eq!(durable.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn durability_fence_retains_the_highest_concurrent_write_handle() {
        let latest = Mutex::new(None);
        let newer = WriteHandle::new(10, 0, || async { Ok(()) });
        let older = WriteHandle::new(5, 0, || async { Ok(()) });

        retain_latest_write_handle(&latest, &newer).await;
        retain_latest_write_handle(&latest, &older).await;

        assert_eq!(latest.lock().await.as_ref().unwrap().seqnum(), 10);
    }

    #[tokio::test]
    async fn durability_fence_rejects_generation_change_during_wait() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("generation-fence-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = Arc::new(transition_storage(
            Database::Writer(writer),
            path,
            object_store.clone(),
            1,
        ));
        storage
            .last_applied_engine_sequence
            .store(10, Ordering::Release);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let handle = WriteHandle::new(10, 0, {
            let entered = entered.clone();
            let release = release.clone();
            move || {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                }
            }
        });
        *storage.latest_write_handle.lock().await = Some(handle);
        let waiting = {
            let storage = storage.clone();
            tokio::spawn(async move { storage.await_durable_through("repo", 1, 1, 10).await })
        };
        entered.notified().await;
        let (current, version) = read_generation_authority(object_store.as_ref(), "repo")
            .await
            .unwrap();
        let changed = GenerationAuthority {
            format: 1,
            repository_id: "repo".into(),
            decision: current.decision + 1,
            active_generation: 2,
            namespace: "generation-2".into(),
            previous_generation: current.active_generation,
            previous_namespace: current.namespace,
            state: "post-activation".to_owned(),
            report_sha256: "ab".repeat(32),
            decided_at_ms: 1,
            observation_until_ms: 2,
            retired_generation: 0,
        };
        publish_generation_authority(object_store.as_ref(), &changed, version)
            .await
            .unwrap();
        release.notify_waiters();

        let error = waiting.await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("became stale while waiting"));
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn object_store_metrics_settle_bytes_streams_and_fixed_roles() {
        let controlled = Arc::new(ControlledObjectStore::default());
        let main_metrics = Arc::new(ObjectStoreRoleMetrics::default());
        let wal_metrics = Arc::new(ObjectStoreRoleMetrics::default());
        let coordination_metrics = Arc::new(ObjectStoreRoleMetrics::default());
        let main = role_aware_object_store(
            controlled.clone(),
            main_metrics.clone(),
            Some(wal_metrics.clone()),
            ObjectStoreRole::Main,
            None,
        );
        let coordination = role_aware_object_store(
            Arc::new(InMemory::new()),
            coordination_metrics.clone(),
            None,
            ObjectStoreRole::Coordination,
            None,
        );

        main.put(&Path::from("main"), Bytes::from_static(b"main-data").into())
            .await
            .unwrap();
        controlled.fail_put.store(true, Ordering::Release);
        assert!(main
            .put(&Path::from("failed"), Bytes::from_static(b"nope").into())
            .await
            .is_err());

        let mut wal_options = PutOptions::default();
        wal_options
            .extensions
            .insert(slatedb::object_store_tag::ObjectStoreCallTag::new(
                slatedb::object_store_tag::TableStoreKind::Main,
                slatedb::object_store_tag::SstType::Wal,
            ));
        main.put_opts(
            &Path::from("database/wal/0001.sst"),
            Bytes::from_static(b"wal-data").into(),
            wal_options,
        )
        .await
        .unwrap();
        main.list(Some(&Path::from("database/wal")))
            .collect::<Vec<_>>()
            .await;
        main.delete(&Path::from("database/wal/0001.sst"))
            .await
            .unwrap();
        controlled
            .inner
            .put(
                &Path::from("main-delete"),
                Bytes::from_static(b"main").into(),
            )
            .await
            .unwrap();
        controlled
            .inner
            .put(
                &Path::from("database/wal/mixed.sst"),
                Bytes::from_static(b"wal").into(),
            )
            .await
            .unwrap();
        controlled.fail_main_delete.store(true, Ordering::Release);
        let mixed_delete = main
            .delete_stream(
                futures_util::stream::iter([
                    Ok(Path::from("main-delete")),
                    Ok(Path::from("database/wal/mixed.sst")),
                ])
                .boxed(),
            )
            .collect::<Vec<_>>()
            .await;
        assert!(mixed_delete[0].is_err());
        assert!(mixed_delete[1].is_ok());
        let mut cancelled_delete = main.delete_stream(
            futures_util::stream::once(async { Ok(Path::from("database/wal/cancelled.sst")) })
                .chain(futures_util::stream::pending())
                .boxed(),
        );
        assert!(futures_util::poll!(&mut cancelled_delete.next()).is_ready());
        assert_eq!(wal_metrics.snapshot().delete.timing.active, 1);
        assert!(futures_util::poll!(&mut cancelled_delete.next()).is_pending());
        drop(cancelled_delete);

        let body = main
            .get(&Path::from("main"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(body, Bytes::from_static(b"main-data"));

        let mut upload = main.put_multipart(&Path::from("multipart")).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"part-one").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();

        let abandoned_list = main.list(None);
        assert_eq!(main_metrics.snapshot().list.timing.active, 1);
        drop(abandoned_list);
        let list_cancelled = main_metrics.snapshot().list.timing;
        assert_eq!(list_cancelled.active, 0);
        assert_eq!(list_cancelled.cancellations, 1);
        main.list(None).collect::<Vec<_>>().await;

        coordination
            .put(&Path::from("claim"), Bytes::from_static(b"epoch").into())
            .await
            .unwrap();
        coordination.head(&Path::from("claim")).await.unwrap();

        let main_snapshot = main_metrics.snapshot();
        assert_eq!(main_snapshot.put.timing.successes, 1);
        assert_eq!(main_snapshot.put.timing.failures, 1);
        assert_eq!(main_snapshot.put.transferred_bytes, 9);
        assert_eq!(main_snapshot.get_body.transferred_bytes, 9);
        assert_eq!(main_snapshot.get_body.timing.successes, 1);
        assert_eq!(main_snapshot.multipart_part.transferred_bytes, 8);
        assert_eq!(main_snapshot.multipart_complete.timing.successes, 1);
        assert_eq!(main_snapshot.list.timing.successes, 1);
        assert!(!main_snapshot.put.timeout_outcomes_available);
        assert!(!main_snapshot.retry_delay_available);
        assert!(!main_snapshot.background_pressure_available);

        let wal_snapshot = wal_metrics.snapshot();
        assert_eq!(wal_snapshot.put.timing.successes, 1);
        assert_eq!(wal_snapshot.put.transferred_bytes, 8);
        assert_eq!(wal_snapshot.list.timing.successes, 1);
        assert_eq!(wal_snapshot.delete.timing.successes, 2);
        assert_eq!(wal_snapshot.delete.timing.cancellations, 1);
        assert_eq!(wal_snapshot.delete.timing.active, 0);
        assert_eq!(wal_snapshot.get.timing.attempts, 0);
        assert_eq!(main_snapshot.delete.timing.failures, 1);

        let coordination_snapshot = coordination_metrics.snapshot();
        assert_eq!(coordination_snapshot.put.timing.successes, 1);
        assert_eq!(coordination_snapshot.head.timing.successes, 1);
        assert_eq!(main_snapshot.head.timing.attempts, 0);
    }

    #[tokio::test]
    async fn wal_metrics_track_finalizing_copies() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        raw.put(&Path::from("staging/0001"), "wal-data".into())
            .await
            .unwrap();
        let (wal, metrics) = monitored_wal_store(raw).await.unwrap();

        wal.copy(&Path::from("staging/0001"), &Path::from("wal/0001.sst"))
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
        let db = open_writer(
            &path,
            main.clone(),
            Some(wal.clone()),
            &SlateDbTuning::default(),
        )
        .await
        .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"wal-key", b"wal-value");
        db.write(batch)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();

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
        assert_eq!(
            reader.get(b"wal-key").await.unwrap().as_deref(),
            Some(&b"wal-value"[..])
        );
        reader.close().await.unwrap();
        db.close().await.unwrap();

        ensure_wal_target_identity(main.as_ref(), "s3:bucket-a:wal", false)
            .await
            .unwrap();
        assert!(
            ensure_wal_target_identity(main.as_ref(), "s3:bucket-b:wal", true)
                .await
                .is_err()
        );
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
        db.write(batch)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.close_with_options(slatedb::config::CloseOptions { flush_type: None })
            .await
            .unwrap();

        let writer = open_writer(
            path.as_str(),
            main.clone(),
            Some(wal.clone()),
            &SlateDbTuning::default(),
        )
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

    #[tokio::test]
    async fn metadata_rebuild_reset_removes_database_and_wal_binding_but_preserves_control() {
        let store = InMemory::new();
        let capsule = Path::from("_vaultic/recovery-capsules/one.json");
        let wal_target = Path::from(WAL_TARGET_PATH);
        let manifest = Path::from("manifest/0001");
        for path in [&capsule, &wal_target, &manifest] {
            store.put(path, vec![1_u8].into()).await.unwrap();
        }

        reset_metadata_store(&store, false).await.unwrap();

        assert!(store.head(&capsule).await.is_ok());
        assert!(matches!(
            store.head(&wal_target).await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            store.head(&manifest).await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
        assert!(!metadata_store_has_database_objects(&store).await.unwrap());
    }

    #[tokio::test]
    async fn clean_memory_wal_rebuild_reopens_with_persisted_local_wal() {
        let root = env::temp_dir().join(format!(
            "vaulticdb-memory-local-handoff-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let wal_root = root.join("wal");
        let repository_id = format!("memory-local-handoff-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.cache = cache::CacheConfig::default();
        config.object_store = ObjectStoreConfig::Local { root: root.clone() };
        config.wal_store = WalStoreConfig::Store(ReplicaStoreConfig::Memory);
        config.metadata_rebuild_reset = true;
        config.bulk_import_local_wal_data_dir = Some(wal_root.clone());

        let memory = Storage::open(&repository_id, &config).await.unwrap();
        let database = memory.database.read().await;
        let Database::Writer(db) = &*database else {
            panic!("bulk import did not open a writer")
        };
        db.put(b"p:imported", b"value".to_vec())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.put(BULK_IMPORT_COMPLETE_RECORD, b"complete".to_vec())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        drop(database);
        memory.close().await.unwrap();

        config.wal_store = WalStoreConfig::Inherit;
        config.metadata_rebuild_reset = false;
        config.bulk_import_local_wal_data_dir = None;
        let local = Storage::open(&repository_id, &config).await.unwrap();
        assert_eq!(local.wal_target, "local");
        assert_eq!(
            local.read_value(b"p:imported").await.unwrap(),
            Some(bytes::Bytes::from_static(b"value"))
        );
        local.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn flushed_memory_wal_rebuild_without_handoff_cannot_reopen() {
        let _failpoint_guard = STORAGE_FAILPOINT_TEST_LOCK.lock().await;
        let root = env::temp_dir().join(format!(
            "vaulticdb-torn-memory-wal-handoff-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let repository_id = format!("torn-memory-wal-handoff-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.cache = cache::CacheConfig::default();
        config.object_store = ObjectStoreConfig::Local { root: root.clone() };
        config.wal_store = WalStoreConfig::Store(ReplicaStoreConfig::Memory);
        config.metadata_rebuild_reset = true;
        config.bulk_import_local_wal_data_dir = Some(root.join("wal"));

        let memory = Storage::open(&repository_id, &config).await.unwrap();
        let database = memory.database.read().await;
        let Database::Writer(db) = &*database else {
            panic!("bulk import did not open a writer")
        };
        db.put(b"p:imported", b"value".to_vec()).await.unwrap();
        db.put(BULK_IMPORT_COMPLETE_RECORD, b"complete".to_vec())
            .await
            .unwrap();
        drop(database);

        arm_storage_failpoint(StorageFailpoint::AfterFlushBeforeHandoff(
            memory.database_path.clone(),
        ));
        assert!(memory.close().await.is_err());
        assert!(local_wal_handoff_target(memory.coordination_store.as_ref())
            .await
            .unwrap()
            .is_none());
        drop(memory);

        config.wal_store = WalStoreConfig::Inherit;
        config.metadata_rebuild_reset = false;
        config.bulk_import_local_wal_data_dir = None;
        assert!(Storage::open(&repository_id, &config).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn active_memory_wal_binding_rejects_local_target() {
        let store = InMemory::new();
        ensure_wal_target_identity(&store, "memory", false)
            .await
            .unwrap();
        assert!(ensure_wal_target_identity(&store, "local:/tmp/wal", true)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn clean_incomplete_memory_wal_rebuild_does_not_handoff() {
        let root = env::temp_dir().join(format!(
            "vaulticdb-incomplete-memory-wal-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let repository_id = format!("incomplete-memory-wal-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.cache = cache::CacheConfig::default();
        config.object_store = ObjectStoreConfig::Local { root: root.clone() };
        config.wal_store = WalStoreConfig::Store(ReplicaStoreConfig::Memory);
        config.metadata_rebuild_reset = true;
        config.bulk_import_local_wal_data_dir = Some(root.join("wal"));

        Storage::open(&repository_id, &config)
            .await
            .unwrap()
            .close()
            .await
            .unwrap();

        config.wal_store = WalStoreConfig::Inherit;
        config.metadata_rebuild_reset = false;
        config.bulk_import_local_wal_data_dir = None;
        assert!(Storage::open(&repository_id, &config).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metadata_rebuild_reset_reopens_empty_writable_local_candidate() {
        let root = env::temp_dir().join(format!(
            "vaulticdb-reset-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let repository_id = format!("reset-repository-{}", rand::random::<u64>());
        let mut config = cache_storage_config(cache::CacheConfidentiality::DecryptedHighlyTrusted);
        config.cache = cache::CacheConfig::default();
        config.object_store = ObjectStoreConfig::Local { root: root.clone() };

        let storage = Storage::open(&repository_id, &config).await.unwrap();
        let database = storage.database.read().await;
        let Database::Writer(db) = &*database else {
            panic!("new reset candidate did not open as writer")
        };
        db.put(b"p:stale", b"old".to_vec())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        drop(database);
        storage.close().await.unwrap();

        let (_, coordination_store) = object_store(&repository_id, &config.object_store).unwrap();
        let stale_epoch = claim_writer_epoch(coordination_store.as_ref(), None)
            .await
            .unwrap()
            .unwrap();

        config.metadata_rebuild_reset = true;
        let reset = Storage::open(&repository_id, &config).await.unwrap();
        assert!(reset.read_value(b"p:stale").await.unwrap().is_none());
        assert!(reset.writer_epoch.load(Ordering::Acquire) > stale_epoch);
        let database = reset.database.read().await;
        let Database::Writer(db) = &*database else {
            panic!("reset candidate did not reopen as writer")
        };
        db.put(b"p:new", b"new".to_vec())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        drop(database);
        reset.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
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
    async fn multi_get_preserves_transaction_order_and_response_limit() {
        for slatedb_multiget in [false, true] {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            assert_eq!(
                claim_writer_epoch(object_store.as_ref(), None)
                    .await
                    .unwrap(),
                Some(1)
            );
            let path = format!("multi-get-{}", rand::random::<u64>());
            let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
                .await
                .unwrap();
            let mut storage = transition_storage(Database::Writer(writer), path, object_store, 1);
            storage.slatedb_multiget = slatedb_multiget;
            assert!(storage
                .multi_get(&[], "unknown", usize::MAX)
                .await
                .unwrap()
                .is_empty());
            storage
                .write_batch(&WriteBatchRequest {
                    puts: vec![KeyValue {
                        key: b"committed".to_vec(),
                        value: b"stored".to_vec(),
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            let committed = storage
                .multi_get(
                    &[b"committed".to_vec(), b"missing".to_vec()],
                    "",
                    usize::MAX,
                )
                .await
                .unwrap();
            assert_eq!(committed[0].value, b"stored");
            assert!(!committed[1].found);

            let transaction_id = storage.begin().await.unwrap().transaction_id;
            storage
                .write_batch(&WriteBatchRequest {
                    puts: vec![KeyValue {
                        key: b"present".to_vec(),
                        value: b"value".to_vec(),
                    }],
                    transaction_id: transaction_id.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();

            let keys = vec![
                b"present".to_vec(),
                b"missing".to_vec(),
                b"present".to_vec(),
            ];
            let results = storage
                .multi_get(&keys, &transaction_id, usize::MAX)
                .await
                .unwrap();
            assert_eq!(
                results
                    .iter()
                    .map(|result| result.key.as_slice())
                    .collect::<Vec<_>>(),
                keys
            );
            assert_eq!(results[0].value, b"value");
            assert!(!results[1].found);
            assert_eq!(results[2].value, b"value");
            assert!(
                storage
                    .get(b"present", &transaction_id)
                    .await
                    .unwrap()
                    .found
            );
            assert!(!storage
                .scan(b"present", b"", 1, &transaction_id)
                .await
                .unwrap()
                .entries
                .is_empty());

            let error = storage
                .multi_get(&keys, &transaction_id, 0)
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::ResourceExhausted);
            assert_eq!(
                storage
                    .attribution
                    .transaction_slot_lock_wait
                    .snapshot()
                    .completed,
                5
            );
            storage.rollback(&transaction_id).await.unwrap();
            storage.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn slatedb_multi_get_reports_read_amplification() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("multi-get-metrics-{}", rand::random::<u64>());
        let engine_metrics = Arc::new(DefaultMetricsRecorder::new());
        let writer = open_writer_with_metrics(
            &path,
            object_store.clone(),
            None,
            &SlateDbTuning::default(),
            engine_metrics.clone(),
        )
        .await
        .unwrap();
        writer
            .put(b"present", b"value")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        writer
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();

        let mut storage = transition_storage(Database::Writer(writer), path, object_store, 1);
        storage.engine_metrics = Some(engine_metrics);
        storage.slatedb_multiget = true;
        let results = storage
            .multi_get(
                &[
                    b"present".to_vec(),
                    b"missing".to_vec(),
                    b"present".to_vec(),
                ],
                "",
                usize::MAX,
            )
            .await
            .unwrap();
        assert_eq!(results[0].value, b"value");
        assert!(!results[1].found);
        assert_eq!(results[2].value, b"value");

        let engine = storage.engine_metrics_snapshot();
        assert_eq!(engine.multi_get_calls, 1);
        assert_eq!(engine.multi_get_input_keys, 3);
        assert_eq!(engine.multi_get_unique_keys, 2);
        assert!(engine.multi_get_sst_visits > 0);
        assert!(engine.multi_get_candidate_keys > 0);
        assert!(engine.multi_get_needed_blocks > 0);
        assert!(engine.multi_get_coalesced_reads > 0);
        assert!(engine.multi_get_needed_block_bytes > 0);
        assert!(engine.multi_get_coalesced_read_bytes >= engine.multi_get_needed_block_bytes);
        for (reads, bytes) in [
            (
                engine.multi_get_projected_reads_gap_8,
                engine.multi_get_projected_read_bytes_gap_8,
            ),
            (
                engine.multi_get_projected_reads_gap_32,
                engine.multi_get_projected_read_bytes_gap_32,
            ),
            (
                engine.multi_get_projected_reads_gap_128,
                engine.multi_get_projected_read_bytes_gap_128,
            ),
        ] {
            assert!(reads > 0);
            assert!(reads <= engine.multi_get_coalesced_reads);
            assert!(bytes >= engine.multi_get_coalesced_read_bytes);
        }
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn slatedb_multi_get_reads_non_fencing_reader() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = format!("reader-multi-get-{}", rand::random::<u64>());
        let db = Db::open(path.as_str(), object_store.clone()).await.unwrap();
        db.put(b"present", b"value")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.close().await.unwrap();
        let reader = open_reader(&path, object_store.clone(), None)
            .await
            .unwrap();
        let mut storage = transition_storage(Database::Reader(reader), path, object_store, 0);
        storage.slatedb_multiget = true;

        let results = storage
            .multi_get(&[b"present".to_vec(), b"missing".to_vec()], "", usize::MAX)
            .await
            .unwrap();
        assert_eq!(results[0].value, b"value");
        assert!(!results[1].found);
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn expired_transactions_report_counter_reconciliation() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("expired-transaction-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = transition_storage(Database::Writer(writer), path, object_store, 1);
        let transaction_id = storage.begin().await.unwrap().transaction_id;
        storage
            .transactions
            .read()
            .await
            .get(&transaction_id)
            .unwrap()
            .last_touched_ms
            .store(0, Ordering::Relaxed);

        assert_eq!(storage.prune_expired_transactions().await, (0, 1));
        assert_eq!(storage.prune_expired_transactions().await, (0, 0));
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_begin_reports_transactions_pruned_before_failure() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("failed-begin-expiry-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = transition_storage(Database::Writer(writer), path, object_store.clone(), 1);
        let transaction_id = storage.begin().await.unwrap().transaction_id;
        storage
            .transactions
            .read()
            .await
            .get(&transaction_id)
            .unwrap()
            .last_touched_ms
            .store(0, Ordering::Relaxed);
        let writer = {
            let mut database = storage.database.write().await;
            match std::mem::replace(&mut *database, Database::Unavailable) {
                Database::Writer(writer) => writer,
                _ => panic!("expected writer database"),
            }
        };
        writer.close().await.unwrap();

        let failure = storage.begin().await.unwrap_err();
        assert_eq!(failure.expired, 1);
        assert_eq!(storage.transactions.read().await.len(), 0);
        assert_eq!(storage.attribution.transaction_begin.snapshot().failures, 1);
        release_writer_claim(object_store.as_ref(), 1)
            .await
            .unwrap();
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
            cache_manager: None,
            wal_object_store: None,
            wal_metrics: None,
            attribution: Arc::new(StorageAttribution::default()),
            engine_metrics: Some(Arc::new(DefaultMetricsRecorder::new())),
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
            capsule_migration: Mutex::new(()),
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            last_applied_engine_sequence: AtomicU64::new(0),
            durable_engine_sequence: AtomicU64::new(0),
            latest_write_handle: Mutex::new(None),
            transaction_idle_timeout_ms: 1_000,
            slatedb_multiget: false,
            slatedb_tuning: SlateDbTuning::default(),
            metadata_rebuild_reset: false,
            bulk_import_local_wal_data_dir: None,
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
    async fn capsule_migration_intention_preserves_bytes_and_progress() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("migration-intent-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = transition_storage(Database::Writer(writer), path, object_store, 1);
        storage
            .store_master_key(b"repository-key")
            .await
            .unwrap_err();
        let capsule = b"exact capsule bytes\n".to_vec();
        let digest = format!("{:x}", Sha256::digest(&capsule));
        let intent = CapsuleMigrationIntent {
            format: 1,
            repository_id: "repo".to_owned(),
            generation: 3,
            capsule_directory: "/capsules".to_owned(),
            request_sha256: "request-digest".to_owned(),
            capsule_sha256: digest.clone(),
            capsule: capsule.clone(),
            local_path: None,
            mirror_path: None,
        };
        assert_eq!(
            storage
                .store_capsule_migration_intent(&intent)
                .await
                .unwrap(),
            intent
        );
        let mut progressed = storage.capsule_migration_intent().await.unwrap().unwrap();
        assert_eq!(progressed.capsule, capsule);
        progressed.local_path = Some("/capsules/recovery.json".to_owned());
        storage
            .write_capsule_migration_intent(&progressed)
            .await
            .unwrap();
        assert_eq!(
            storage
                .finalize_capsule_migration(&digest)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        progressed.mirror_path = Some("meta:capsule-mirror".to_owned());
        storage
            .write_capsule_migration_intent(&progressed)
            .await
            .unwrap();
        assert_eq!(
            storage.capsule_migration_status().await.unwrap(),
            (Some(digest.clone()), None)
        );
        storage.finalize_capsule_migration(&digest).await.unwrap();
        assert_eq!(
            storage.capsule_migration_status().await.unwrap(),
            (None, Some(digest))
        );
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_capsule_migration_intentions_have_one_winner() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            claim_writer_epoch(object_store.as_ref(), None)
                .await
                .unwrap(),
            Some(1)
        );
        let path = format!("migration-race-{}", rand::random::<u64>());
        let writer = open_writer(&path, object_store.clone(), None, &SlateDbTuning::default())
            .await
            .unwrap();
        let storage = Arc::new(transition_storage(
            Database::Writer(writer),
            path,
            object_store,
            1,
        ));
        let intent = |suffix: u8| {
            let capsule = vec![suffix; 32];
            CapsuleMigrationIntent {
                format: 1,
                repository_id: "repo".to_owned(),
                generation: u64::from(suffix),
                capsule_directory: format!("/capsules/{suffix}"),
                request_sha256: format!("request-{suffix}"),
                capsule_sha256: format!("{:x}", Sha256::digest(&capsule)),
                capsule,
                local_path: None,
                mirror_path: None,
            }
        };
        let first = intent(1);
        let second = intent(2);
        let (first_result, second_result) = tokio::join!(
            storage.store_capsule_migration_intent(&first),
            storage.store_capsule_migration_intent(&second),
        );
        assert_ne!(first_result.is_ok(), second_result.is_ok());
        let stored = storage.capsule_migration_intent().await.unwrap().unwrap();
        assert!(stored == first || stored == second);
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
        assert!(release_writer_claim(object_store.as_ref(), 1)
            .await
            .is_err());
        assert_eq!(
            active_writer_epoch(object_store.as_ref()).await.unwrap(),
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
            cache_manager: None,
            wal_object_store: Some(wal_object_store),
            wal_metrics: None,
            attribution: Arc::new(StorageAttribution::default()),
            engine_metrics: Some(Arc::new(DefaultMetricsRecorder::new())),
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
            capsule_migration: Mutex::new(()),
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            last_applied_engine_sequence: AtomicU64::new(0),
            durable_engine_sequence: AtomicU64::new(0),
            latest_write_handle: Mutex::new(None),
            transaction_idle_timeout_ms: 1_000,
            slatedb_multiget: false,
            slatedb_tuning: SlateDbTuning::default(),
            metadata_rebuild_reset: false,
            bulk_import_local_wal_data_dir: None,
            credential_manager: None,
            broker_lease_metadata: None,
            writer_epoch: AtomicU64::new(1),
            wal_target: "memory",
            wal_durability: "local-process",
        };
        let error = storage.assert_current_writer_epoch().await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Aborted);
        assert!(!error.details().is_empty());
    }

    #[tokio::test]
    async fn reader_validates_existing_encryption_policy_for_writer_takeover() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = format!("encrypted-reader-{}", rand::random::<u64>());
        let db = Db::open(path.as_str(), object_store.clone()).await.unwrap();
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
            cache_manager: None,
            wal_object_store: None,
            wal_metrics: None,
            attribution: Arc::new(StorageAttribution::default()),
            engine_metrics: Some(Arc::new(DefaultMetricsRecorder::new())),
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
            capsule_migration: Mutex::new(()),
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            last_applied_engine_sequence: AtomicU64::new(0),
            durable_engine_sequence: AtomicU64::new(0),
            latest_write_handle: Mutex::new(None),
            transaction_idle_timeout_ms: 1_000,
            slatedb_multiget: false,
            slatedb_tuning: SlateDbTuning::default(),
            metadata_rebuild_reset: false,
            bulk_import_local_wal_data_dir: None,
            credential_manager: None,
            broker_lease_metadata: None,
            writer_epoch: AtomicU64::new(1),
            wal_target: "inherited",
            wal_durability: "inherited",
        };

        storage
            .ensure_encryption_policy("repo", false)
            .await
            .unwrap();
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
            cache_manager: None,
            wal_object_store: None,
            wal_metrics: None,
            attribution: Arc::new(StorageAttribution::default()),
            engine_metrics: Some(Arc::new(DefaultMetricsRecorder::new())),
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
            capsule_migration: Mutex::new(()),
            transactions: RwLock::new(HashMap::new()),
            next_transaction: AtomicU64::new(1),
            last_durable_sequence: AtomicU64::new(0),
            last_applied_engine_sequence: AtomicU64::new(0),
            durable_engine_sequence: AtomicU64::new(0),
            latest_write_handle: Mutex::new(None),
            transaction_idle_timeout_ms: 1_000,
            slatedb_multiget: false,
            slatedb_tuning: SlateDbTuning::default(),
            metadata_rebuild_reset: false,
            bulk_import_local_wal_data_dir: None,
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
            .activate_generation("repo", 1, 2, "candidate-2".into(), "bb".repeat(32), 60_000)
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
        assert_eq!(rolled_back.state, "rollback-observation");
        assert!(rolled_back.decision > activated.decision);
        assert_eq!(
            storage
                .rollback_generation("repo", activated.decision, "dd".repeat(32), 0)
                .await
                .unwrap(),
            rolled_back
        );
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
