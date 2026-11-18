#[cfg(test)]
mod tests {
    //! Metadata object encryption tests.

    use super::*;
    use aes_gcm::{
        aead::{Aead, Payload},
        Aes256Gcm, KeyInit,
    };
    use futures_util::TryStreamExt;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{
        local::LocalFileSystem, Attribute, AttributeValue, Attributes, ObjectStoreExt,
    };
    use slatedb::{
        config::{FlushOptions, FlushType},
        object_store_tag::{ObjectStoreCallTag, RetryReason, SstType, TableStoreKind},
        Db, WriteBatch,
    };

    #[test]
    fn sampled_chunk_refetch_history_is_bounded_and_counts_distance() {
        let metrics = Arc::new(EncryptionReadMetrics::default());
        let cache = ChunkCache::new(0, metrics.clone());
        let now = Instant::now();
        cache.record_fetch_fingerprint(0, 256 << 10, now);
        cache.record_fetch_fingerprint(1, 128 << 20, now);
        cache.record_fetch_fingerprint(0, 256 << 10, now + std::time::Duration::from_secs(2));
        assert_eq!(metrics.chunk_sampled_first_fetches.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.chunk_sampled_refetches.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.chunk_sampled_reuse_ns.load(Ordering::Relaxed), 2_000_000_000);
        assert_eq!(metrics.chunk_sampled_refetch_within_64m.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.chunk_sampled_refetch_within_256m.load(Ordering::Relaxed), 1);
        for index in 1..=16384 {
            cache.record_fetch_fingerprint(index * 64, 1, now);
        }
        assert_eq!(cache.state.lock().unwrap().sampled_fetches.len(), 16384);
        assert_eq!(metrics.chunk_sampled_history_evictions.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn read_windows_are_bounded_and_keep_distinct_byte_counts() {
        let metrics = EncryptionReadMetrics::default();
        metrics.record_window_at(21, 100, 0);
        metrics.record_window_at(29, 0, 256);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot["read_windows"][0]["unix_seconds"], 20);
        assert_eq!(snapshot["read_windows"][0]["requested_plaintext_bytes"], 100);
        assert_eq!(snapshot["read_windows"][0]["ciphertext_origin_bytes"], 256);
        for seconds in 3..=363 {
            metrics.record_window_at(seconds * 10, 1, 2);
        }
        assert_eq!(metrics.read_windows.lock().unwrap().len(), 360);
        assert_eq!(metrics.read_windows_dropped.load(Ordering::Relaxed), 2);
    }

    #[derive(Debug, Default)]
    struct CountingStore {
        inner: InMemory,
        reads: Mutex<Vec<(String, u64, u64)>>,
        delay: Option<std::time::Duration>,
        body_delay: Option<std::time::Duration>,
        fail_request: bool,
        request_not_modified: bool,
        fail_body: bool,
        payload_gate: Option<Arc<tokio::sync::Semaphore>>,
        head_gate: Option<Arc<tokio::sync::Semaphore>>,
        panic_payload: std::sync::atomic::AtomicBool,
        omit_identity: bool,
    }

    #[tokio::test]
    async fn origin_body_payloads_preserve_file_and_stream_reads() {
        let encrypted = store(Arc::new(InMemory::new()), "origin-body-payloads");
        assert!(encrypted.read_metrics.snapshot().get("ciphertext_origin_file_read").is_none());
        assert!(encrypted.read_metrics.snapshot().get("ciphertext_origin_file_range_read").is_none());
        let location = Path::from("payload");
        let memory = InMemory::new();
        memory.put(&location, Bytes::from_static(b"stream-body").into()).await.unwrap();
        let streamed = memory.get(&location).await.unwrap();
        assert!(matches!(&streamed.payload, GetResultPayload::Stream(..)));
        assert_eq!(encrypted.origin_body(streamed).await.unwrap(), Bytes::from_static(b"stream-body"));
        let directory = std::env::temp_dir().join(format!("vaulticdb-body-payload-{}", rand::random::<u64>()));
        std::fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let local = LocalFileSystem::new_with_prefix(&directory).unwrap();
        local.put(&location, Bytes::from_static(b"file-body").into()).await.unwrap();
        let file = local.get_opts(&location, GetOptions { range: Some(GetRange::Bounded(2..7)), ..Default::default() }).await.unwrap();
        assert!(matches!(&file.payload, GetResultPayload::File(..)));
        assert_eq!(encrypted.origin_body(file).await.unwrap(), Bytes::from_static(b"le-bo"));
        let metrics = encrypted.read_metrics.snapshot();
        let payloads = &metrics["ciphertext_origin_body_payloads"];
        assert_eq!(payloads["file_calls"], 1);
        assert_eq!(payloads["stream_calls"], 1);
        assert_eq!(metrics["ciphertext_origin_body_calls"], 2);
        assert_eq!(metrics["ciphertext_origin_body_successes"], 2);
        assert_eq!(metrics["ciphertext_origin_body_active"], 0);
        let elapsed = payloads["file_ns"].as_u64().unwrap() + payloads["stream_ns"].as_u64().unwrap();
        assert!(elapsed > 0 && elapsed <= metrics["ciphertext_origin_body_ns"].as_u64().unwrap());
        if let Ok(expected) = std::env::var("VAULTIC_BODY_TIMING_EXPECT_HOOK") {
            assert!(matches!(expected.as_str(), "0" | "1"));
            assert_eq!(metrics.get("ciphertext_origin_file_read").is_some(), expected == "1");
            assert_eq!(metrics.get("ciphertext_origin_file_range_read").is_some(), expected == "1");
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[tokio::test]
    async fn origin_body_payloads_count_errors_and_cancellation() {
        let encrypted = store(Arc::new(InMemory::new()), "origin-body-outcomes");
        let memory = InMemory::new();
        let location = Path::from("payload");
        memory.put(&location, Bytes::from_static(b"body").into()).await.unwrap();
        let mut failed = memory.get(&location).await.unwrap();
        failed.payload = GetResultPayload::Stream(stream::once(async {
            Err(encryption_error(EncryptionError::Length))
        }).boxed());
        assert!(encrypted.origin_body(failed).await.is_err());
        let mut cancelled = memory.get(&location).await.unwrap();
        cancelled.payload = GetResultPayload::Stream(stream::once(std::future::pending()).boxed());
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), encrypted.origin_body(cancelled)).await.is_err());
        let metrics = encrypted.read_metrics.snapshot();
        let payloads = &metrics["ciphertext_origin_body_payloads"];
        assert_eq!(payloads["file_calls"], 0);
        assert_eq!(payloads["file_ns"], 0);
        assert_eq!(payloads["stream_calls"], 2);
        assert!(payloads["stream_ns"].as_u64().unwrap() > 0);
        assert_eq!(metrics["ciphertext_origin_body_calls"], 2);
        assert_eq!(metrics["ciphertext_origin_body_errors"], 1);
        assert_eq!(metrics["ciphertext_origin_body_cancellations"], 1);
        assert_eq!(metrics["ciphertext_origin_body_active"], 0);
        assert!(payloads["stream_ns"].as_u64().unwrap() <= metrics["ciphertext_origin_body_ns"].as_u64().unwrap());
    }

    #[cfg(feature = "diagnostic-body-timing")]
    async fn diagnostic_file_fixture() -> (EncryptedObjectStore, GetResult, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!("vaulticdb-file-hook-{}", rand::random::<u64>()));
        std::fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let local = LocalFileSystem::new_with_prefix(&directory).unwrap();
        let location = Path::from("payload");
        local.put(&location, Bytes::from_static(b"diagnostic-body").into()).await.unwrap();
        let result = local.get_opts(&location, GetOptions { range: Some(GetRange::Bounded(2..7)), ..Default::default() }).await.unwrap();
        assert!(matches!(result.payload, GetResultPayload::File(..)));
        (store(Arc::new(InMemory::new()), "diagnostic-file"), result, directory)
    }

    #[cfg(feature = "diagnostic-body-timing")]
    struct BlockingRelease(Option<std::sync::mpsc::Sender<()>>);

    #[cfg(feature = "diagnostic-body-timing")]
    impl Drop for BlockingRelease {
        fn drop(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    #[cfg(feature = "diagnostic-body-timing")]
    async fn occupy_blocking_worker() -> (BlockingRelease, tokio::task::JoinHandle<()>) {
        let (ready, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _ = ready.send(());
            let _ = released.recv();
        });
        started.await.unwrap();
        (BlockingRelease(Some(release)), worker)
    }

    #[cfg(feature = "diagnostic-body-timing")]
    #[test]
    #[ignore = "requires the private pinned object_store body-timing patch"]
    fn origin_file_read_diagnostic_queue() {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
        runtime.block_on(async {
            let (encrypted, result, directory) = diagnostic_file_fixture().await;
            let (release, occupied) = occupy_blocking_worker().await;
            let mut body = Box::pin(encrypted.origin_body(result));
            assert!(futures_util::poll!(body.as_mut()).is_pending());
            let queued = encrypted.read_metrics.file_read_metrics.snapshot();
            assert_eq!((queued.calls, queued.started, queued.active), (1, 0, 1));
            drop(release);
            occupied.await.unwrap();
            assert_eq!(body.await.unwrap(), Bytes::from_static(b"agnos"));
            #[cfg(target_os = "linux")]
            if let Ok(expected) = std::env::var("VAULTIC_DIAGNOSTIC_SCHED_COMM") {
                let worker = tokio::task::spawn_blocking(|| {
                    std::fs::read_to_string("/proc/thread-self/comm").unwrap()
                }).await.unwrap();
                assert_eq!(worker.trim_end(), expected);
                let caller = std::fs::read_to_string("/proc/thread-self/comm").unwrap();
                assert_ne!(caller.trim_end(), expected);
            }
            let file = encrypted.read_metrics.file_read_metrics.snapshot();
            assert_eq!((file.calls, file.started, file.finished, file.resumed), (1, 1, 1, 1));
            assert_eq!((file.errors, file.panics, file.abandoned, file.active, file.cancellations, file.direct_calls), (0, 0, 0, 0, 0, 0));
            assert!(file.queue_ns > 0 && file.service_ns > 0);
            let metrics = encrypted.read_metrics.snapshot();
            let payload = metrics["ciphertext_origin_body_payloads"]["file_ns"].as_u64().unwrap();
            assert!(file.queue_ns + file.service_ns + file.completion_ns <= payload);
            assert_eq!(metrics["ciphertext_origin_file_read"]["calls"], 1);
            let range = &metrics["ciphertext_origin_file_range_read"];
            assert_eq!(range["calls"], 1);
            assert_eq!(range["finished"], 1);
            assert_eq!(range["allocation_calls"], 1);
            assert_eq!(range["zero_fill_calls"], 1);
            assert_eq!(range["conversion_calls"], 1);
            assert_eq!(range["read_calls"], 1);
            assert_eq!(range["requested_bytes"], 5);
            assert_eq!(range["read_bytes"], 5);
            for name in ["active", "errors", "panics", "read_errors", "read_interrupted", "read_eof", "empty_calls", "unsupported_calls"] {
                assert_eq!(range[name], 0, "{name}");
            }
            let stages: u64 = ["allocation_ns", "zero_fill_ns", "read_ns", "conversion_ns"].iter().map(|name| range[name].as_u64().unwrap()).sum();
            assert!(stages <= range["total_ns"].as_u64().unwrap());
            assert!(range["total_ns"].as_u64().unwrap() <= file.service_ns);
            std::fs::remove_dir_all(directory).unwrap();
        });
    }

    #[cfg(feature = "diagnostic-body-timing")]
    #[test]
    #[ignore = "requires the private pinned object_store body-timing patch"]
    fn origin_file_read_diagnostic_cancellation() {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
        let (metrics, directory) = runtime.block_on(async {
            let (encrypted, result, directory) = diagnostic_file_fixture().await;
            let (release, occupied) = occupy_blocking_worker().await;
            let mut body = Box::pin(encrypted.origin_body(result));
            assert!(futures_util::poll!(body.as_mut()).is_pending());
            drop(body);
            let file = encrypted.read_metrics.file_read_metrics.snapshot();
            assert_eq!((file.calls, file.started, file.active, file.cancellations), (1, 0, 1, 1));
            assert_eq!(encrypted.read_metrics.snapshot()["ciphertext_origin_body_cancellations"], 1);
            drop(release);
            occupied.await.unwrap();
            (encrypted.read_metrics.clone(), directory)
        });
        drop(runtime);
        let file = metrics.file_read_metrics.snapshot();
        assert_eq!((file.started, file.finished, file.active, file.resumed, file.cancellations), (1, 1, 0, 0, 1));
        assert_eq!((file.range.calls, file.range.finished, file.range.active, file.range.read_bytes), (1, 1, 0, 5));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(feature = "diagnostic-body-timing")]
    #[test]
    #[ignore = "requires the private pinned object_store body-timing patch"]
    fn origin_file_read_diagnostic_error() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (encrypted, mut result, directory) = diagnostic_file_fixture().await;
            let path = directory.join("payload");
            let mut reference = LocalFileSystem::new_with_prefix(&directory).unwrap().get(&Path::from("payload")).await.unwrap();
            reference.range = result.range.clone();
            reference.payload = GetResultPayload::File(std::fs::OpenOptions::new().write(true).open(&path).unwrap(), path.clone());
            let expected = reference.bytes().await.unwrap_err();
            result.payload = GetResultPayload::File(std::fs::OpenOptions::new().write(true).open(&path).unwrap(), path);
            let error = encrypted.origin_body(result).await.unwrap_err();
            assert_eq!(error.to_string(), expected.to_string());
            let file = encrypted.read_metrics.file_read_metrics.snapshot();
            assert_eq!((file.calls, file.started, file.finished, file.errors, file.resumed, file.active), (1, 1, 1, 1, 1, 0));
            assert_eq!(encrypted.read_metrics.snapshot()["ciphertext_origin_body_errors"], 1);
            let range = encrypted.read_metrics.file_read_metrics.snapshot().range;
            assert_eq!((range.calls, range.finished, range.active, range.errors, range.read_errors), (1, 1, 0, 1, 1));
            assert_eq!((range.read.calls, range.read_bytes, range.conversion.calls), (1, 0, 0));
            std::fs::remove_dir_all(directory).unwrap();
        });
    }

    #[cfg(feature = "diagnostic-body-timing")]
    #[test]
    #[ignore = "requires the private pinned object_store range-timing patch"]
    fn origin_file_read_diagnostic_empty() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (encrypted, mut result, directory) = diagnostic_file_fixture().await;
            result.range = 2..2;
            assert_eq!(encrypted.origin_body(result).await.unwrap(), Bytes::new());
            let range = encrypted.read_metrics.file_read_metrics.snapshot().range;
            assert_eq!((range.calls, range.finished, range.active, range.errors, range.empty_calls), (1, 1, 0, 0, 1));
            assert_eq!((range.allocation.calls, range.zero_fill.calls, range.conversion.calls), (1, 1, 1));
            assert_eq!((range.read.calls, range.requested_bytes, range.read_bytes), (0, 0, 0));
            std::fs::remove_dir_all(directory).unwrap();
        });
    }

    #[cfg(feature = "diagnostic-body-timing")]
    #[test]
    #[ignore = "requires the private pinned object_store range-timing patch"]
    fn origin_file_read_diagnostic_eof() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (encrypted, mut result, directory) = diagnostic_file_fixture().await;
            result.range = 9..20;
            let mut reference = LocalFileSystem::new_with_prefix(&directory).unwrap().get(&Path::from("payload")).await.unwrap();
            reference.range = result.range.clone();
            let expected = reference.bytes().await.unwrap_err();
            let actual = encrypted.origin_body(result).await.unwrap_err();
            assert_eq!(actual.to_string(), expected.to_string());
            let range = encrypted.read_metrics.file_read_metrics.snapshot().range;
            assert_eq!((range.calls, range.finished, range.active, range.errors), (1, 1, 0, 1));
            assert_eq!((range.requested_bytes, range.read_bytes, range.read.calls, range.read_eof), (11, 6, 2, 1));
            assert_eq!((range.read_errors, range.conversion.calls, range.panics), (0, 0, 0));
            std::fs::remove_dir_all(directory).unwrap();
        });
    }

    impl Display for CountingStore {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("counting memory object store")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            if options.head {
                if let Some(gate) = &self.head_gate {
                    gate.acquire().await.unwrap().forget();
                }
            }
            if !options.head
                && matches!(&options.range, Some(GetRange::Bounded(range)) if range.start >= HEADER_SIZE as u64)
            {
                if let Some(gate) = &self.payload_gate {
                    gate.acquire().await.unwrap().forget();
                }
                assert!(
                    !self.panic_payload.swap(false, Ordering::Relaxed),
                    "injected chunk worker panic"
                );
            }
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            if self.fail_request {
                return Err(encryption_error(EncryptionError::Length));
            }
            if self.request_not_modified {
                return Err(slatedb::object_store::Error::NotModified {
                    path: location.to_string(),
                    source: Box::new(EncryptionError::Length),
                });
            }
            let head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            let mut meta = result.meta.clone();
            if self.omit_identity {
                meta.e_tag = None;
                meta.version = None;
            }
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = result.extensions.clone();
            let bytes = if head {
                Bytes::new()
            } else {
                result.bytes().await?
            };
            self.reads.lock().unwrap().push((
                location.to_string(),
                range.start,
                bytes.len() as u64,
            ));
            let body_delay = self.body_delay;
            let fail_body = self.fail_body;
            Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::once(async move {
                        if let Some(delay) = body_delay {
                            tokio::time::sleep(delay).await;
                        }
                        if fail_body {
                            return Err(encryption_error(EncryptionError::Length));
                        }
                        Ok(bytes)
                    })
                    .boxed(),
                ),
                meta,
                range,
                attributes,
                extensions,
            })
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn store(inner: Arc<dyn ObjectStore>, repository: &str) -> EncryptedObjectStore {
        EncryptedObjectStore::new(inner, repository, vec![EncryptionKey::new(1, [7; 32])], 1)
            .unwrap()
            .with_chunk_size(16)
    }

    #[tokio::test]
    async fn encrypted_read_work_attribution() {
        for body_delay in [false, true] {
            let inner = Arc::new(CountingStore {
                delay: (!body_delay).then_some(std::time::Duration::from_millis(5)),
                body_delay: body_delay.then_some(std::time::Duration::from_millis(5)),
                ..Default::default()
            });
            let encrypted = store(inner.clone(), "attribution");
            let path = Path::from("metadata");
            encrypted
                .put(
                    &path,
                    Bytes::from_static(b"0123456789abcdef0123456789abcdef").into(),
                )
                .await
                .unwrap();
            assert_eq!(
                encrypted.get_range(&path, 3..7).await.unwrap(),
                Bytes::from_static(b"3456")
            );
            let first = encrypted.read_metrics.snapshot();
            assert_eq!(
                encrypted.get_range(&path, 3..7).await.unwrap(),
                Bytes::from_static(b"3456")
            );
            let metrics = encrypted.read_metrics.snapshot();
            assert_eq!(metrics["reads"], 2);
            assert_eq!(metrics["range_reads"], 2);
            assert_eq!(metrics["requested_plaintext_bytes"], 8);
            assert_eq!(metrics["rounded_ciphertext_bytes"], 32);
            assert_eq!(metrics["ciphertext_origin_calls"], 2);
            assert_eq!(
                metrics["ciphertext_origin_bytes"],
                (HEADER_SIZE + 32) as u64
            );
            assert_eq!(metrics["header_cache_misses"], 1);
            assert_eq!(metrics["header_cache_hits"], 1);
            assert_eq!(metrics["crypto_jobs"], 1);
            assert_eq!(metrics["chunk_cache_hits"], 1);
            assert_eq!(metrics["chunk_cache_misses"], 1);
            assert_eq!(metrics["chunk_cache_bytes"], 16);
            assert_eq!(metrics["header_validation_calls"], 1);
            if !body_delay {
                assert!(metrics["header_validation_ns"].as_u64().unwrap() >= 5_000_000);
            }
            assert!(metrics["ciphertext_origin_ns"].as_u64().unwrap() >= 10_000_000);
            assert_eq!(metrics["ciphertext_origin_body_calls"], 2);
            assert_eq!(metrics["ciphertext_origin_request_successes"], 2);
            assert_eq!(metrics["ciphertext_origin_body_successes"], 2);
            assert_eq!(metrics["ciphertext_origin_request_active"], 0);
            assert_eq!(metrics["ciphertext_origin_body_active"], 0);
            let delayed_stage = if body_delay {
                "ciphertext_origin_body_ns"
            } else {
                "ciphertext_origin_request_ns"
            };
            assert!(metrics[delayed_stage].as_u64().unwrap() >= 10_000_000);
            let request_ns = metrics["ciphertext_origin_request_ns"].as_u64().unwrap();
            let body_ns = metrics["ciphertext_origin_body_ns"].as_u64().unwrap();
            assert!(metrics["ciphertext_origin_ns"].as_u64().unwrap() >= request_ns + body_ns);
            for key in [
                "ciphertext_origin_request_ns",
                "ciphertext_origin_body_ns",
                "ciphertext_origin_body_calls",
            ] {
                assert_eq!(metrics[key], first[key], "cached read changed {key}");
            }
            assert!(metrics["crypto_service_ns"].as_u64().unwrap() > 0);
        }
    }

    #[tokio::test]
    async fn missing_object_origin_request_has_no_body() {
        let inner = Arc::new(CountingStore {
            delay: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        });
        let encrypted = store(inner, "missing-origin");
        let error = encrypted
            .get_range(&Path::from("missing.sst"), 0..1)
            .await
            .unwrap_err();
        assert!(matches!(error, object_store::Error::NotFound { .. }));
        let metrics = encrypted.read_metrics.snapshot();
        assert_eq!(metrics["ciphertext_origin_calls"], 1);
        assert_eq!(metrics["ciphertext_origin_body_calls"], 0);
        assert_eq!(metrics["ciphertext_origin_request_not_found"], 1);
        for name in [
            "ciphertext_origin_request_successes",
            "ciphertext_origin_request_errors",
            "ciphertext_origin_request_cancellations",
            "ciphertext_origin_request_active",
            "ciphertext_origin_body_successes",
            "ciphertext_origin_body_errors",
            "ciphertext_origin_body_cancellations",
            "ciphertext_origin_body_active",
            "ciphertext_origin_identity_rejections",
        ] {
            assert_eq!(metrics[name], 0, "unexpected {name}");
        }
        assert_eq!(metrics["ciphertext_origin_body_ns"], 0);
        assert_eq!(metrics["ciphertext_origin_bytes"], 0);
        let request_ns = metrics["ciphertext_origin_request_ns"].as_u64().unwrap();
        assert!(request_ns >= 5_000_000);
        assert!(metrics["ciphertext_origin_ns"].as_u64().unwrap() >= request_ns);
    }

    #[test]
    fn origin_request_error_details_are_bounded() {
        use slatedb::object_store::Error;
        use std::io::ErrorKind;
        let metrics = EncryptionReadMetrics::default();
        for _repeat in 0..2 {
            for site in [OriginReadSite::Header, OriginReadSite::Chunk, OriginReadSite::Range] {
                for variant in ["generic", "not_found", "precondition"] {
                    for conditional in [false, true] {
                        for cause in [
                            None, Some(ErrorKind::NotFound), Some(ErrorKind::PermissionDenied),
                            Some(ErrorKind::InvalidInput), Some(ErrorKind::UnexpectedEof),
                            Some(ErrorKind::TimedOut), Some(ErrorKind::Interrupted),
                            Some(ErrorKind::WouldBlock), Some(ErrorKind::Other),
                        ] {
                            let source: Box<dyn std::error::Error + Send + Sync> = match cause {
                                Some(kind) => Box::new(std::io::Error::new(kind, "private-error-text")),
                                None => Box::new(EncryptionError::Length),
                            };
                            let error = match variant {
                                "generic" => Error::Generic { store: "fixture", source },
                                "not_found" => Error::NotFound { path: "private-path".to_owned(), source },
                                _ => Error::Precondition { path: "private-path".to_owned(), source },
                            };
                            metrics.record_origin_request_error(site, conditional, &error);
                        }
                    }
                }
            }
        }
        let snapshot = metrics.snapshot();
        let counts = snapshot["ciphertext_origin_request_error_counts"].as_object().unwrap();
        assert_eq!(counts.len(), ORIGIN_ERROR_DETAIL_LIMIT + 1);
        assert_eq!(counts["dropped"], 68);
        assert_eq!(counts.iter().filter(|(key, _)| key.as_str() != "dropped")
            .map(|(_, value)| value.as_u64().unwrap()).sum::<u64>(), 256);
        assert!(!snapshot["ciphertext_origin_request_error_counts"].to_string().contains("private-"));
    }

    #[tokio::test]
    async fn origin_conditional_request_error_details() {
        let inner = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "conditional-origin");
        let location = Path::from("private-object-name");
        encrypted.put(&location, Bytes::from_static(b"payload").into()).await.unwrap();
        let tag = inner.head(&location).await.unwrap().e_tag.unwrap();
        for (options, expected) in [
            (GetOptions { head: true, if_none_match: Some(tag), ..Default::default() }, "not_modified"),
            (GetOptions { head: true, if_match: Some("wrong".to_owned()), ..Default::default() }, "precondition"),
        ] {
            let error = encrypted.get_opts(&location, options).await.unwrap_err();
            let expected_key = format!("header/{expected}/conditional/io_unclassified");
            assert_eq!(origin_request_error_key(OriginReadSite::Header, true, &error), expected_key);
            assert_eq!(encrypted.read_metrics.snapshot()["ciphertext_origin_request_error_counts"][expected_key], 1);
        }
        let metrics = encrypted.read_metrics.snapshot();
        assert_eq!(metrics["ciphertext_origin_request_errors"], 2);
        assert_eq!(metrics["ciphertext_origin_request_not_modified"], 1);
        assert_eq!(metrics["ciphertext_origin_body_calls"], 0);
        assert_eq!(metrics["ciphertext_origin_request_error_counts"]["dropped"], 0);
        assert!(!metrics["ciphertext_origin_request_error_counts"].to_string().contains("private-object-name"));
        let error = slatedb::object_store::Error::Generic {
            store: "fixture",
            source: Box::new(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "private-error-text")),
        };
        assert_eq!(origin_request_error_key(OriginReadSite::Range, false, &error), "range/generic/plain/io_permission_denied");
    }

    #[tokio::test]
    async fn origin_not_modified_requires_matching_request_condition() {
        for (options, expected) in [
            (GetOptions::default(), 0),
            (GetOptions { if_match: Some("tag".to_owned()), ..Default::default() }, 0),
            (GetOptions { if_unmodified_since: Some("2026-01-01T00:00:00Z".parse().unwrap()), ..Default::default() }, 0),
            (GetOptions { if_none_match: Some("tag".to_owned()), ..Default::default() }, 1),
            (GetOptions { if_modified_since: Some("2026-01-01T00:00:00Z".parse().unwrap()), ..Default::default() }, 1),
        ] {
            let encrypted = store(Arc::new(CountingStore { request_not_modified: true, ..Default::default() }), "conditional-origin");
            assert!(matches!(encrypted.get_opts(&Path::from("metadata"), options).await,
                Err(slatedb::object_store::Error::NotModified { .. })));
            let metrics = encrypted.read_metrics.snapshot();
            assert_eq!(metrics["ciphertext_origin_request_errors"], 1);
            assert_eq!(metrics["ciphertext_origin_request_not_modified"], expected);
            assert_eq!(metrics["ciphertext_origin_body_calls"], 0);
            assert_eq!(metrics["ciphertext_origin_request_active"], 0);
        }
    }

    #[tokio::test]
    async fn origin_boundary_consumer_reuses_conditional_header() {
        use slatedb_txn_obj::{object_store::ObjectStoreBoundaryObject, BoundaryObject, MonotonicId, TransactionalObjectError};
        let directory = std::env::temp_dir().join(format!("vaulticdb-origin-boundary-{}", rand::random::<u64>()));
        std::fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let encrypted = Arc::new(EncryptedObjectStore::new(
            Arc::new(LocalFileSystem::new_with_prefix(&directory).unwrap()),
            "boundary-origin", vec![EncryptionKey::new(1, [7; 32])], 1,
        ).unwrap());
        let root = Path::from("root");
        let location = root.clone().join("gc").join("manifest.boundary");
        let boundary = ObjectStoreBoundaryObject::new(&root, encrypted.clone(), "manifest");
        boundary.check(MonotonicId::new(3)).await.unwrap();
        encrypted.put(&location, Bytes::from_static(b"2").into()).await.unwrap();
        assert!(encrypted.get(&location).await.unwrap().meta.e_tag.is_some(), "encrypted boundary must expose an ETag");
        boundary.check(MonotonicId::new(3)).await.unwrap();
        let before = encrypted.read_metrics.snapshot();
        encrypted.invalidate_header(&location);
        boundary.check(MonotonicId::new(3)).await.unwrap();
        let after = encrypted.read_metrics.snapshot();
        assert_eq!(after["ciphertext_origin_request_not_modified"], 1, "before={before}, after={after}");
        assert_eq!(after["ciphertext_origin_request_errors"], 1);
        assert_eq!(after["ciphertext_origin_body_calls"], before["ciphertext_origin_body_calls"]);
        assert_eq!(after["ciphertext_origin_request_error_counts"]["header/not_modified/conditional/io_unclassified"], 1);
        encrypted.put(&location, Bytes::from_static(b"40").into()).await.unwrap();
        assert!(matches!(boundary.check(MonotonicId::new(3)).await,
            Err(TransactionalObjectError::ObjectVersionExists)));
        boundary.check(MonotonicId::new(41)).await.unwrap();
        encrypted.delete(&location).await.unwrap();
        boundary.check(MonotonicId::new(41)).await.unwrap();
        let metrics = encrypted.read_metrics.snapshot();
        assert_eq!(metrics["ciphertext_origin_request_not_found"], 2);
        assert_eq!(metrics["ciphertext_origin_request_errors"], metrics["ciphertext_origin_request_not_modified"]);
        assert_eq!(metrics["ciphertext_origin_request_active"], 0);
        assert_eq!(metrics["ciphertext_origin_body_active"], 0);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[tokio::test]
    async fn origin_failures_and_cancellations_are_classified() {
        for stage in [
            "request_error",
            "body_error",
            "request_cancel",
            "body_cancel",
        ] {
            let inner = Arc::new(CountingStore {
                fail_request: stage == "request_error",
                fail_body: stage == "body_error",
                delay: (stage == "request_cancel").then_some(std::time::Duration::from_secs(60)),
                body_delay: (stage == "body_cancel").then_some(std::time::Duration::from_secs(60)),
                ..Default::default()
            });
            let encrypted = store(inner, "origin-outcomes");
            let location = Path::from("metadata");
            encrypted
                .put(&location, Bytes::from_static(b"payload").into())
                .await
                .unwrap();
            {
                let reader = encrypted.get_range(&location, 0..1);
                tokio::pin!(reader);
                if stage.ends_with("cancel") {
                    assert!(futures_util::poll!(reader.as_mut()).is_pending());
                    let active = if stage == "request_cancel" {
                        "ciphertext_origin_request_active"
                    } else {
                        "ciphertext_origin_body_active"
                    };
                    assert_eq!(encrypted.read_metrics.snapshot()[active], 1);
                } else {
                    assert!(reader.await.is_err());
                }
            }
            let metrics = encrypted.read_metrics.snapshot();
            let body_started = stage.starts_with("body");
            let details = metrics["ciphertext_origin_request_error_counts"].as_object().unwrap();
            assert_eq!(details["dropped"], 0, "{stage}");
            assert_eq!(details.len(), if stage == "request_error" { 2 } else { 1 }, "{stage}");
            if stage == "request_error" {
                assert_eq!(details["header/generic/plain/io_unclassified"], 1);
            }
            assert_eq!(metrics["ciphertext_origin_calls"], 1);
            assert_eq!(
                metrics["ciphertext_origin_request_successes"],
                u64::from(body_started)
            );
            assert_eq!(metrics["ciphertext_origin_body_calls"], u64::from(body_started));
            for (name, expected) in [
                (
                    "ciphertext_origin_request_errors",
                    stage == "request_error",
                ),
                ("ciphertext_origin_body_errors", stage == "body_error"),
                (
                    "ciphertext_origin_request_cancellations",
                    stage == "request_cancel",
                ),
                (
                    "ciphertext_origin_body_cancellations",
                    stage == "body_cancel",
                ),
            ] {
                assert_eq!(metrics[name], u64::from(expected), "{stage}: {name}");
            }
            for name in [
                "ciphertext_origin_request_active",
                "ciphertext_origin_body_active",
                "ciphertext_origin_request_not_found",
                "ciphertext_origin_identity_rejections",
                "ciphertext_origin_body_successes",
            ] {
                assert_eq!(metrics[name], 0, "{stage}: {name}");
            }
        }
    }

    #[tokio::test]
    async fn concurrent_ranges_share_and_cache_authenticated_chunks() {
        let inner = Arc::new(CountingStore {
            delay: Some(std::time::Duration::from_millis(20)),
            ..Default::default()
        });
        let encrypted = store(inner.clone(), "coalesced");
        let location = Path::from("compacted/shared.sst");
        let plaintext = Bytes::from_static(b"0123456789abcdef0123456789abcdef");
        encrypted
            .put(&location, plaintext.clone().into())
            .await
            .unwrap();
        encrypted.head(&location).await.unwrap();
        inner.reads.lock().unwrap().clear();
        let barrier = Arc::new(tokio::sync::Barrier::new(32));
        let readers = (0..32)
            .map(|index| {
                let encrypted = encrypted.clone();
                let location = location.clone();
                let barrier = barrier.clone();
                let plaintext = plaintext.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    let start = index % 8;
                    assert_eq!(
                        encrypted
                            .get_range(&location, start..start + 4)
                            .await
                            .unwrap(),
                        plaintext.slice(start as usize..start as usize + 4)
                    );
                })
            })
            .collect::<Vec<_>>();
        for reader in readers {
            reader.await.unwrap();
        }
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            encrypted.get_range(&location, 10..14).await.unwrap(),
            plaintext.slice(10..14)
        );
        let reads = inner.reads.lock().unwrap();
        assert_eq!(
            reads
                .iter()
                .filter(|(_, start, _)| *start >= HEADER_SIZE as u64)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn cancelled_validation_leader_preserves_followers() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let inner = Arc::new(CountingStore {
            head_gate: Some(gate.clone()),
            ..Default::default()
        });
        let encrypted = store(inner, "validation-cancellation");
        let location = Path::from("compacted/validation.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        encrypted.head(&location).await.unwrap();
        let leader = {
            let encrypted = encrypted.clone();
            let location = location.clone();
            tokio::spawn(async move { encrypted.get_range(&location, 0..4).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        leader.abort();
        leader.await.unwrap_err();
        let follower = {
            let encrypted = encrypted.clone();
            let location = location.clone();
            tokio::spawn(async move { encrypted.get_range(&location, 4..8).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if encrypted
                    .read_metrics
                    .header_validation_coalesced_reads
                    .load(Ordering::Relaxed)
                    > 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.add_permits(1);
        assert_eq!(
            follower.await.unwrap().unwrap(),
            Bytes::from_static(b"4567")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn failed_shared_validation_is_retried() {
        let inner = Arc::new(CountingStore {
            delay: Some(std::time::Duration::from_millis(20)),
            ..Default::default()
        });
        let encrypted = store(inner.clone(), "validation-retry");
        let location = Path::from("compacted/deleted.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        encrypted.head(&location).await.unwrap();
        let original = inner
            .inner
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        inner.inner.delete(&location).await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let readers = (0..8)
            .map(|_| {
                let encrypted = encrypted.clone();
                let location = location.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    assert!(matches!(
                        encrypted.get_range(&location, 0..4).await.unwrap_err(),
                        slatedb::object_store::Error::NotFound { .. }
                    ));
                })
            })
            .collect::<Vec<_>>();
        for reader in readers {
            reader.await.unwrap();
        }
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            1
        );
        inner.inner.put(&location, original.into()).await.unwrap();
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn cancelling_chunk_leader_does_not_cancel_followers() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let inner = Arc::new(CountingStore {
            payload_gate: Some(gate.clone()),
            ..Default::default()
        });
        let encrypted = store(inner.clone(), "cancelled-leader");
        let location = Path::from("compacted/cancel.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        encrypted.head(&location).await.unwrap();
        let leader = {
            let encrypted = encrypted.clone();
            let location = location.clone();
            tokio::spawn(async move { encrypted.get_range(&location, 0..4).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while encrypted
                .read_metrics
                .chunk_inflight
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let follower = {
            let encrypted = encrypted.clone();
            let location = location.clone();
            tokio::spawn(async move { encrypted.get_range(&location, 4..8).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while encrypted
                .read_metrics
                .chunk_coalesced_reads
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        leader.abort();
        gate.add_permits(1);
        assert_eq!(
            follower.await.unwrap().unwrap(),
            Bytes::from_static(b"4567")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_inflight
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn chunk_authentication_failure_is_shared_and_not_cached() {
        let inner = Arc::new(CountingStore {
            delay: Some(std::time::Duration::from_millis(30)),
            ..Default::default()
        });
        let encrypted = store(inner.clone(), "shared-error");
        let location = Path::from("compacted/error.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        let original = inner
            .inner
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut corrupt = original.to_vec();
        corrupt[HEADER_SIZE] ^= 1;
        inner.inner.put(&location, corrupt.into()).await.unwrap();
        encrypted.head(&location).await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(16));
        let mut readers = Vec::new();
        for _ in 0..16 {
            let encrypted = encrypted.clone();
            let location = location.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                let error = encrypted.get_range(&location, 0..4).await.unwrap_err();
                assert!(is_integrity_error(&error));
            }));
        }
        for reader in readers {
            reader.await.unwrap();
        }
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_coalesced_reads
                .load(Ordering::Relaxed),
            15
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_bytes
                .load(Ordering::Relaxed),
            0
        );
        inner.inner.put(&location, original.into()).await.unwrap();
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_inflight
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn chunk_cache_validates_overwrites_deletes_and_conditions() {
        let inner = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "versions");
        let location = Path::from("compacted/versions.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"0123")
        );
        let previous = inner.head(&location).await.unwrap();
        let replacement = encrypted
            .encrypt(&location, Bytes::from_static(b"fedcba9876543210"))
            .await
            .unwrap();
        inner.put(&location, replacement.into()).await.unwrap();
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"fedc")
        );
        let options = GetOptions {
            range: Some(GetRange::Bounded(0..4)),
            if_match: previous.e_tag,
            ..Default::default()
        };
        assert!(matches!(
            encrypted.get_opts(&location, options).await.unwrap_err(),
            slatedb::object_store::Error::Precondition { .. }
        ));
        let current = inner.head(&location).await.unwrap();
        let options = GetOptions {
            range: Some(GetRange::Bounded(0..4)),
            if_none_match: current.e_tag,
            ..Default::default()
        };
        assert!(matches!(
            encrypted.get_opts(&location, options).await.unwrap_err(),
            slatedb::object_store::Error::NotModified { .. }
        ));
        inner.delete(&location).await.unwrap();
        assert!(matches!(
            encrypted.get_range(&location, 0..4).await.unwrap_err(),
            slatedb::object_store::Error::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn compacted_sst_reuses_fresh_header_but_revalidates_after_expiry() {
        let inner = Arc::new(CountingStore::default());
        let mut encrypted = store(inner.clone(), "immutable-sst");
        encrypted.sst_freshness = SstFreshness::Bounded;
        let location = Path::from("compacted/immutable.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        let options = || GetOptions {
            range: Some(GetRange::Bounded(0..4)),
            extensions: ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Compacted).into(),
            ..Default::default()
        };
        assert_eq!(
            encrypted
                .get_opts(&location, options())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            encrypted
                .get_opts(&location, options())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            0
        );
        let mut other_tag = options();
        other_tag.extensions = ObjectStoreCallTag::new_with_segment(
            TableStoreKind::Main,
            SstType::Compacted,
            Some(Bytes::from_static(b"other")),
        )
        .into();
        encrypted.get_opts(&location, other_tag).await.unwrap();
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            1
        );
        let mut conditional = options();
        conditional.if_match = inner.inner.head(&location).await.unwrap().e_tag;
        encrypted.get_opts(&location, conditional).await.unwrap();
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            2
        );
        let mut retry = options();
        let mut tag = ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Compacted);
        tag.retry = Some(RetryReason::CrcMismatch);
        retry.extensions = tag.into();
        encrypted.get_opts(&location, retry).await.unwrap();
        assert_eq!(
            encrypted
                .read_metrics
                .header_cache_misses
                .load(Ordering::Relaxed),
            2
        );
        encrypted
            .headers
            .lock()
            .unwrap()
            .entries
            .get_mut(&location)
            .unwrap()
            .validated_at -= IMMUTABLE_SST_FRESHNESS;
        assert_eq!(
            encrypted
                .get_opts(&location, options())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            3
        );
        encrypted
            .headers
            .lock()
            .unwrap()
            .entries
            .get_mut(&location)
            .unwrap()
            .validated_at -= IMMUTABLE_SST_FRESHNESS;
        inner.inner.delete(&location).await.unwrap();
        assert!(matches!(
            encrypted.get_opts(&location, options()).await.unwrap_err(),
            slatedb::object_store::Error::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn compacted_sst_freshness_policy_is_explicit_and_bounded_by_identity() {
        assert!(SstFreshness::parse("dedicated").is_err());
        assert_eq!(SstFreshness::parse("strict").unwrap(), SstFreshness::Strict);
        assert_eq!(
            SstFreshness::parse("bounded").unwrap(),
            SstFreshness::Bounded
        );
        assert_eq!(
            SstFreshness::parse("immutable").unwrap(),
            SstFreshness::Immutable
        );
        for policy in [SstFreshness::Strict, SstFreshness::Immutable] {
            let inner = Arc::new(CountingStore::default());
            let mut encrypted = store(inner.clone(), "sst-policy");
            encrypted.sst_freshness = policy;
            let location = Path::from("compacted/policy.sst");
            encrypted
                .put(&location, Bytes::from_static(b"0123456789abcdef").into())
                .await
                .unwrap();
            let options = || GetOptions {
                range: Some(GetRange::Bounded(0..4)),
                extensions: ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Compacted)
                    .into(),
                ..Default::default()
            };
            encrypted.get_opts(&location, options()).await.unwrap();
            encrypted
                .headers
                .lock()
                .unwrap()
                .entries
                .get_mut(&location)
                .unwrap()
                .validated_at -= IMMUTABLE_SST_FRESHNESS;
            assert_eq!(
                encrypted
                    .get_opts(&location, options())
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                Bytes::from_static(b"0123")
            );
            assert_eq!(
                encrypted
                    .read_metrics
                    .header_validation_calls
                    .load(Ordering::Relaxed),
                u64::from(policy == SstFreshness::Strict)
            );
            encrypted.delete(&location).await.unwrap();
            assert!(matches!(
                encrypted.get_opts(&location, options()).await.unwrap_err(),
                slatedb::object_store::Error::NotFound { .. }
            ));
        }
    }

    #[tokio::test]
    async fn compacted_sst_validation_coalesces_only_identical_tags() {
        for different_segment in [false, true] {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let inner = Arc::new(CountingStore {
                head_gate: Some(gate.clone()),
                ..Default::default()
            });
            let mut encrypted = store(inner, "tagged-validation");
            encrypted.sst_freshness = SstFreshness::Strict;
            let location = Path::from("compacted/tagged-validation.sst");
            encrypted
                .put(&location, Bytes::from_static(b"0123456789abcdef").into())
                .await
                .unwrap();
            encrypted.head(&location).await.unwrap();
            let request = |segment: bool| {
                let mut tag = ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Compacted);
                if segment {
                    tag.segment = Some(Bytes::from_static(b"other"));
                }
                GetOptions {
                    range: Some(GetRange::Bounded(0..4)),
                    extensions: tag.into(),
                    ..Default::default()
                }
            };
            let first = {
                let encrypted = encrypted.clone();
                let location = location.clone();
                let options = request(false);
                tokio::spawn(async move {
                    encrypted
                        .get_opts(&location, options)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap()
                })
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while encrypted
                    .read_metrics
                    .header_validation_calls
                    .load(Ordering::Relaxed)
                    == 0
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let second = {
                let encrypted = encrypted.clone();
                let location = location.clone();
                let options = request(different_segment);
                tokio::spawn(async move {
                    encrypted
                        .get_opts(&location, options)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap()
                })
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while if different_segment {
                    encrypted
                        .read_metrics
                        .header_validation_calls
                        .load(Ordering::Relaxed)
                        < 2
                } else {
                    encrypted
                        .read_metrics
                        .header_validation_coalesced_reads
                        .load(Ordering::Relaxed)
                        == 0
                } {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            gate.add_permits(if different_segment { 2 } else { 1 });
            assert_eq!(first.await.unwrap(), Bytes::from_static(b"0123"));
            assert_eq!(second.await.unwrap(), Bytes::from_static(b"0123"));
            assert_eq!(
                encrypted
                    .read_metrics
                    .header_validation_calls
                    .load(Ordering::Relaxed),
                if different_segment { 2 } else { 1 }
            );
        }
    }

    #[tokio::test]
    async fn immutable_sst_policy_excludes_unknown_extensions_and_mutable_requests() {
        let inner = Arc::new(CountingStore::default());
        let mut encrypted = store(inner.clone(), "sst-exclusions");
        encrypted.sst_freshness = SstFreshness::Immutable;
        let location = Path::from("compacted/exclusions.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        let options = || GetOptions {
            range: Some(GetRange::Bounded(0..4)),
            extensions: ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Compacted).into(),
            ..Default::default()
        };
        encrypted.get_opts(&location, options()).await.unwrap();
        let mut unknown = options();
        unknown.extensions.insert("unknown semantics");
        assert!(EncryptedObjectStore::immutable_sst_tag(&unknown).is_none());
        let mut historical = options();
        historical.version = Some("old-version".to_owned());
        assert!(EncryptedObjectStore::immutable_sst_tag(&historical).is_none());
        let mut conditional_seed = options();
        conditional_seed.if_match = Some("etag".to_owned());
        assert!(EncryptedObjectStore::immutable_sst_tag(&conditional_seed).is_none());
        let mut wal = options();
        wal.extensions = ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Wal).into();
        for excluded in [
            unknown.clone(),
            wal,
            GetOptions {
                range: Some(GetRange::Bounded(0..4)),
                ..Default::default()
            },
        ] {
            assert!(encrypted
                .validated_cached_header(&location, &excluded)
                .await
                .unwrap()
                .is_some());
        }
        assert_eq!(
            encrypted
                .read_metrics
                .header_validation_calls
                .load(Ordering::Relaxed),
            3
        );
        let origin_calls = encrypted
            .read_metrics
            .ciphertext_origin_calls
            .load(Ordering::Relaxed);
        assert_eq!(
            encrypted
                .get_opts(&location, unknown)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .ciphertext_origin_calls
                .load(Ordering::Relaxed),
            origin_calls + 1
        );
        let mut conditional = options();
        conditional.if_match = Some("wrong-etag".to_owned());
        assert!(encrypted.get_opts(&location, conditional).await.is_err());
        inner.inner.delete(&location).await.unwrap();
        assert_eq!(
            encrypted
                .get_opts(&location, options())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"0123")
        );
        let mut versioned = options();
        versioned.version = Some("unsupported-version".to_owned());
        assert!(encrypted.get_opts(&location, versioned).await.is_err());
        let mut retry = options();
        let mut tag = ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Compacted);
        tag.retry = Some(slatedb::object_store_tag::RetryReason::CrcMismatch);
        retry.extensions = tag.into();
        assert!(matches!(
            encrypted.get_opts(&location, retry).await.unwrap_err(),
            slatedb::object_store::Error::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn chunk_cache_bypasses_objects_without_stable_identity() {
        let inner = Arc::new(CountingStore {
            omit_identity: true,
            ..Default::default()
        });
        let encrypted = store(inner, "identityless");
        let location = Path::from("compacted/no-identity.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                encrypted.get_range(&location, 0..4).await.unwrap(),
                Bytes::from_static(b"0123")
            );
        }
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_bytes
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            encrypted.read_metrics.crypto_jobs.load(Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn chunk_cache_rejects_replacement_during_fetch() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let inner = Arc::new(CountingStore {
            payload_gate: Some(gate.clone()),
            ..Default::default()
        });
        let encrypted = store(inner.clone(), "replacement-race");
        let location = Path::from("compacted/race.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        let reader = {
            let encrypted = encrypted.clone();
            let location = location.clone();
            tokio::spawn(async move { encrypted.get_range(&location, 0..4).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while encrypted
                .read_metrics
                .chunk_inflight
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let replacement = encrypted
            .encrypt(&location, Bytes::from_static(b"fedcba9876543210"))
            .await
            .unwrap();
        inner
            .inner
            .put(&location, replacement.into())
            .await
            .unwrap();
        gate.add_permits(1);
        assert!(is_integrity_error(&reader.await.unwrap().unwrap_err()));
        let metrics = encrypted.read_metrics.snapshot();
        assert_eq!(metrics["ciphertext_origin_identity_rejections"], 1);
        assert_eq!(metrics["ciphertext_origin_request_successes"], 2);
        assert_eq!(metrics["ciphertext_origin_body_calls"], 1);
        assert_eq!(metrics["ciphertext_origin_request_active"], 0);
        assert_eq!(metrics["ciphertext_origin_body_active"], 0);
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_bytes
                .load(Ordering::Relaxed),
            0
        );
        gate.add_permits(1);
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"fedc")
        );
    }

    #[tokio::test]
    async fn chunk_cache_is_byte_bounded_and_lru() {
        let inner = Arc::new(CountingStore::default());
        let mut encrypted = store(inner, "bounded");
        encrypted.chunks = Arc::new(ChunkCache::new(32, encrypted.read_metrics.clone()));
        let location = Path::from("compacted/lru.sst");
        encrypted
            .put(&location, Bytes::from(vec![9; 64]).into())
            .await
            .unwrap();
        for start in [0, 16, 0, 32, 0] {
            assert_eq!(
                encrypted
                    .get_range(&location, start..start + 1)
                    .await
                    .unwrap(),
                Bytes::from_static(&[9])
            );
            let state = encrypted.chunks.state.lock().unwrap();
            assert!(state.bytes <= 32);
            assert!(state.entries.len() <= 2);
        }
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_hits
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_evictions
                .load(Ordering::Relaxed),
            1
        );
        encrypted.get_range(&location, 16..17).await.unwrap();
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            4
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_bytes
                .load(Ordering::Relaxed),
            32
        );
    }

    #[tokio::test]
    async fn zero_chunk_retention_still_coalesces() {
        let inner = Arc::new(CountingStore {
            delay: Some(std::time::Duration::from_millis(20)),
            ..Default::default()
        });
        let mut encrypted = store(inner, "no-retention");
        encrypted.chunks = Arc::new(ChunkCache::new(0, encrypted.read_metrics.clone()));
        let location = Path::from("compacted/zero.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        encrypted.head(&location).await.unwrap();
        let (first, second) = tokio::join!(
            encrypted.get_range(&location, 0..4),
            encrypted.get_range(&location, 4..8)
        );
        assert_eq!(first.unwrap(), Bytes::from_static(b"0123"));
        assert_eq!(second.unwrap(), Bytes::from_static(b"4567"));
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_coalesced_reads
                .load(Ordering::Relaxed),
            1
        );
        encrypted.get_range(&location, 0..4).await.unwrap();
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_bytes
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn chunk_cache_budget_is_shared_but_backing_stores_are_isolated() {
        let first = Arc::new(InMemory::new());
        let second = Arc::new(InMemory::new());
        let encrypted = store(first.clone(), "separate-stores");
        let other = encrypted.with_inner(second.clone());
        assert!(Arc::ptr_eq(&encrypted.chunks, &other.chunks));
        let location = Path::from("same-path.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        let bytes = first.get(&location).await.unwrap().bytes().await.unwrap();
        second.put(&location, bytes.clone().into()).await.unwrap();
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            other.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_misses
                .load(Ordering::Relaxed),
            2
        );
        let mut corrupt = bytes.to_vec();
        corrupt[HEADER_SIZE] ^= 1;
        second.put(&location, corrupt.into()).await.unwrap();
        assert!(is_integrity_error(
            &other.get_range(&location, 0..4).await.unwrap_err()
        ));
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"0123")
        );
    }

    #[test]
    fn invalidated_headers_do_not_accumulate_order_entries() {
        let mut headers = HeaderCache::default();
        let location = Path::from("mutable");
        for _ in 0..100 {
            headers.insert(
                location.clone(),
                Header {
                    key_version: 1,
                    chunk_size: 16,
                    plaintext_len: 16,
                    nonce: [0; NONCE_SIZE],
                },
                ObjectMeta {
                    location: location.clone(),
                    last_modified: Default::default(),
                    size: (HEADER_SIZE + 32) as u64,
                    e_tag: None,
                    version: None,
                },
                Attributes::new(),
                slatedb::object_store::Extensions::new(),
                None,
            );
            headers.remove(&location);
        }
        assert!(headers.order.is_empty());
        assert!(headers.entries.is_empty());
    }

    #[tokio::test]
    async fn stale_header_observations_cannot_repopulate_after_mutation() {
        let inner = Arc::new(InMemory::new());
        let encrypted = store(inner, "header-generation");
        let location = Path::from("compacted/generation.sst");
        encrypted
            .put(&location, Bytes::from_static(b"original").into())
            .await
            .unwrap();
        let generation = encrypted.headers.lock().unwrap().generation;
        let observed = encrypted
            .header(&location, GetOptions::default())
            .await
            .unwrap();
        encrypted
            .put(&location, Bytes::from_static(b"replaced").into())
            .await
            .unwrap();
        encrypted.cache_header(
            &location,
            observed.0,
            &observed.1,
            &observed.2,
            &observed.3,
            None,
            generation,
        );
        assert!(encrypted.headers.lock().unwrap().entries.is_empty());
        let generation = encrypted.headers.lock().unwrap().generation;
        let observed = encrypted
            .header(&location, GetOptions::default())
            .await
            .unwrap();
        encrypted.delete(&location).await.unwrap();
        encrypted.cache_header(
            &location,
            observed.0,
            &observed.1,
            &observed.2,
            &observed.3,
            None,
            generation,
        );
        assert!(encrypted.headers.lock().unwrap().entries.is_empty());
    }

    #[tokio::test]
    async fn chunk_flights_bound_unique_loads_and_bytes() {
        let limits = ChunkCacheLimits::default();
        let maximums = ChunkCacheLimits {
            entries: MAX_CHUNK_CACHE_ENTRIES,
            flights: MAX_CHUNK_FLIGHTS,
            flight_bytes: MAX_CHUNK_FLIGHT_BYTES,
            chunk_bytes: MAX_CACHED_CHUNK_BYTES,
            range_chunks: MAX_CACHED_RANGE_CHUNKS,
        }
        .validate()
        .unwrap();
        for (limits, chunk_size, readers, admitted) in [
            (limits, 16, limits.flights + 1, limits.flights),
            (limits, limits.chunk_bytes, 17, 16),
            (maximums, 16, maximums.flights + 1, maximums.flights),
            (maximums, maximums.chunk_bytes, 17, 16),
        ] {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let inner = Arc::new(CountingStore {
                payload_gate: Some(gate.clone()),
                ..Default::default()
            });
            let mut encrypted = store(inner, "admission").with_chunk_size(chunk_size);
            encrypted.chunks = Arc::new(ChunkCache::with_limits(
                0,
                limits,
                encrypted.read_metrics.clone(),
            ));
            let location = Path::from("compacted/admission.sst");
            encrypted
                .put(&location, Bytes::from(vec![7; readers * chunk_size]).into())
                .await
                .unwrap();
            encrypted.head(&location).await.unwrap();
            let mut tasks = Vec::new();
            for index in 0..readers {
                let encrypted = encrypted.clone();
                let location = location.clone();
                tasks.push(tokio::spawn(async move {
                    let start = (index * chunk_size) as u64;
                    encrypted
                        .get_range(&location, start..start + 1)
                        .await
                        .unwrap()
                }));
            }
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    {
                        let state = encrypted.chunks.state.lock().unwrap();
                        assert!(state.flights.len() <= limits.flights);
                        assert!(state.flight_bytes <= limits.flight_bytes);
                        if state.flights.len() == admitted {
                            break;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            gate.add_permits(readers);
            for task in tasks {
                assert_eq!(task.await.unwrap(), Bytes::from_static(&[7]));
            }
            let state = encrypted.chunks.state.lock().unwrap();
            assert!(state.flights.is_empty());
            assert_eq!(state.flight_bytes, 0);
        }
    }

    #[tokio::test]
    async fn experimental_chunk_cache_entry_limit_is_enforced() {
        let mut encrypted = store(Arc::new(InMemory::new()), "entries");
        let limits = ChunkCacheLimits {
            entries: 2,
            ..Default::default()
        };
        encrypted.chunks = Arc::new(ChunkCache::with_limits(
            4096,
            limits,
            encrypted.read_metrics.clone(),
        ));
        let location = Path::from("compacted/entries.sst");
        encrypted
            .put(&location, Bytes::from(vec![7; 48]).into())
            .await
            .unwrap();
        for start in [0, 16, 32] {
            encrypted
                .get_range(&location, start..start + 1)
                .await
                .unwrap();
        }
        let state = encrypted.chunks.state.lock().unwrap();
        assert_eq!(state.entries.len(), 2);
        assert_eq!(state.bytes, 32);
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_cache_evictions
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn chunk_worker_panic_releases_admission_and_allows_retry() {
        let inner = Arc::new(CountingStore::default());
        let encrypted = store(inner.clone(), "panic");
        let location = Path::from("compacted/panic.sst");
        encrypted
            .put(&location, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        inner.panic_payload.store(true, Ordering::Relaxed);
        assert!(tokio::time::timeout(
            std::time::Duration::from_secs(2),
            encrypted.get_range(&location, 0..4)
        )
        .await
        .unwrap()
        .is_err());
        assert_eq!(
            encrypted
                .read_metrics
                .chunk_inflight
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            encrypted.get_range(&location, 0..4).await.unwrap(),
            Bytes::from_static(b"0123")
        );
    }

    #[test]
    fn chunk_cache_retirement_removes_old_plaintext() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let encrypted = store(Arc::new(InMemory::new()), "retirement");
            let location = Path::from("compacted/retired.sst");
            encrypted
                .put(&location, Bytes::from_static(b"0123456789abcdef").into())
                .await
                .unwrap();
            encrypted.get_range(&location, 0..4).await.unwrap();
            assert_eq!(
                encrypted
                    .read_metrics
                    .chunk_cache_bytes
                    .load(Ordering::Relaxed),
                16
            );
            encrypted
                .install_write_key(EncryptionKey::new(2, [8; 32]))
                .unwrap();
            assert_eq!(
                encrypted.get_range(&location, 0..4).await.unwrap(),
                Bytes::from_static(b"0123")
            );
            encrypted.retire_read_keys_before(2).unwrap();
            assert_eq!(
                encrypted
                    .read_metrics
                    .chunk_cache_bytes
                    .load(Ordering::Relaxed),
                0
            );
            assert!(is_integrity_error(
                &encrypted.get_range(&location, 0..4).await.unwrap_err()
            ));
        });
    }

    #[test]
    fn read_latency_buckets_cover_boundaries_and_overflow() {
        let histogram = ReadLatencyBuckets::default();
        for value in [0, 100_000, 100_001, 1_000_001, 5_000_001, 20_000_001, 100_000_001, 500_000_001, 2_000_000_001, u64::MAX] {
            histogram.record(value);
        }
        assert_eq!(histogram.snapshot(), [2, 1, 1, 1, 1, 1, 1, 2]);
    }

    #[tokio::test]
    async fn authenticated_reads_export_fetch_and_wait_latency() {
        let encrypted = store(Arc::new(InMemory::new()), "repo-a");
        let location = Path::from("compacted/latency.sst");
        encrypted.put(&location, Bytes::from_static(b"latency fixture").into()).await.unwrap();
        assert_eq!(encrypted.get_range(&location, 0..7).await.unwrap(), Bytes::from_static(b"latency"));
        let metrics = &encrypted.read_metrics;
        assert_eq!(metrics.ciphertext_origin_latency.snapshot().iter().sum::<u64>(), metrics.ciphertext_origin_calls.load(Ordering::Relaxed));
        assert!(metrics.chunk_wait_latency.snapshot().iter().sum::<u64>() > 0);
        assert!(metrics.chunk_wait_ns.load(Ordering::Relaxed) > 0);
        let snapshot = metrics.snapshot();
        for name in ["ciphertext_origin_latency_buckets", "chunk_wait_latency_buckets", "chunk_admission_latency_buckets"] {
            assert_eq!(snapshot[name].as_array().unwrap().len(), 8);
        }
    }

    #[test]
    fn chunk_cache_configuration_is_bounded() {
        assert_eq!(parse_chunk_cache_bytes("0").unwrap(), 0);
        assert_eq!(
            parse_chunk_cache_bytes("67108864").unwrap(),
            64 * 1024 * 1024
        );
        assert_eq!(
            parse_chunk_cache_bytes("4294967296").unwrap(),
            4 * 1024 * 1024 * 1024
        );
        assert_eq!(
            parse_chunk_cache_bytes("8589934592").unwrap(),
            8 * 1024 * 1024 * 1024
        );
        assert_eq!(
            parse_chunk_cache_bytes("17179869184").unwrap(),
            DEFAULT_CHUNK_CACHE_BYTES
        );
        assert_eq!(
            parse_chunk_cache_bytes("34359738368").unwrap(),
            32 * 1024 * 1024 * 1024
        );
        assert_eq!(
            parse_chunk_cache_bytes("68719476736").unwrap() as u64,
            MAX_CHUNK_CACHE_BYTES
        );
        for value in [
            "68719476737",
            "-1",
            "+1",
            " 1",
            "64MiB",
            "",
            "18446744073709551616",
        ] {
            assert!(
                parse_chunk_cache_bytes(value).is_err(),
                "accepted {value:?}"
            );
        }
        let defaults = ChunkCacheLimits::default();
        assert_eq!(
            (
                defaults.entries,
                defaults.flights,
                defaults.flight_bytes,
                defaults.chunk_bytes,
                defaults.range_chunks
            ),
            (65536, 64, 16 * 1024 * 1024, 1024 * 1024, 4)
        );
        for (maximum, expected) in [
            (MAX_CHUNK_CACHE_ENTRIES, 262144),
            (MAX_CHUNK_FLIGHTS, 256),
            (MAX_CHUNK_FLIGHT_BYTES, 64 * 1024 * 1024),
            (MAX_CACHED_CHUNK_BYTES, 4 * 1024 * 1024),
            (MAX_CACHED_RANGE_CHUNKS, 16),
        ] {
            assert_eq!(maximum, expected);
            assert_eq!(
                parse_chunk_limit("limit", &maximum.to_string(), maximum).unwrap(),
                maximum
            );
            assert!(parse_chunk_limit("limit", &(maximum + 1).to_string(), maximum).is_err());
            for invalid in ["0", "-1", "+1", " 1", "1MiB", "", "18446744073709551616"] {
                assert!(parse_chunk_limit("limit", invalid, maximum).is_err());
            }
        }
        assert!(ChunkCacheLimits {
            flight_bytes: 1,
            ..defaults
        }
        .validate()
        .is_err());
        assert!(defaults.validate().is_ok());
    }

    #[tokio::test]
    async fn crypto_queue_and_service_attribution() {
        let executor = CryptoExecutor::new(1).unwrap();
        let metrics = Arc::new(EncryptionReadMetrics::default());
        let held = executor.permits.acquire().await.unwrap();
        let operation = executor.run_measured(
            || {
                std::thread::sleep(std::time::Duration::from_millis(5));
                Ok(17)
            },
            Some(metrics.clone()),
        );
        tokio::pin!(operation);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), &mut operation)
                .await
                .is_err()
        );
        drop(held);
        assert_eq!(operation.await.unwrap(), 17);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot["crypto_jobs"], 1);
        assert!(snapshot["crypto_queue_ns"].as_u64().unwrap() >= 5_000_000);
        assert!(snapshot["crypto_service_ns"].as_u64().unwrap() >= 5_000_000);
    }

    #[test]
    fn crypto_thread_count_accepts_only_configured_bounds() {
        assert_eq!(parse_crypto_threads("1").unwrap(), 1);
        assert_eq!(parse_crypto_threads("12").unwrap(), 12);
        assert_eq!(parse_crypto_threads("64").unwrap(), 64);
        for invalid in ["0", "65", "many", "", " 8"] {
            assert!(
                parse_crypto_threads(invalid).is_err(),
                "accepted invalid crypto thread count {invalid:?}"
            );
        }
    }

    #[test]
    fn decryption_reuses_unique_ciphertext_allocation() {
        let encrypted = store(Arc::new(InMemory::new()), "repo-a");
        let location = Path::from("compacted/reused.sst");
        for plaintext_len in [7, 16 * PARALLEL_CRYPTO_CHUNKS + 7] {
            let plaintext = Bytes::from(vec![0x5a; plaintext_len]);
            let object = encrypted.encrypt_sync(&location, &plaintext).unwrap();
            let header = decode_header(&object).unwrap();
            let ciphertext = BytesMut::from(&object[HEADER_SIZE..]).freeze();
            let allocation = ciphertext.as_ptr();

            let decrypted = encrypted
                .decrypt_chunks_sync(&location, header, 0, ciphertext)
                .unwrap();

            assert_eq!(decrypted, plaintext);
            assert_eq!(decrypted.as_ptr(), allocation);
        }
    }

    #[tokio::test]
    async fn partial_range_does_not_retain_decrypted_chunk_allocation() {
        let encrypted = store(Arc::new(InMemory::new()), "repo-a").with_chunk_size(64 * 1024);
        let location = Path::from("compacted/small-range.sst");
        let plaintext = Bytes::from(
            (0..128 * 1024)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
        );
        encrypted
            .put(&location, plaintext.clone().into())
            .await
            .unwrap();
        for range in [128..192, 65504..65568, 131008..131072] {
            let mut options = GetOptions::default();
            options.range = Some(GetRange::Bounded(range.start as u64..range.end as u64));
            let result = encrypted.get_opts(&location, options).await.unwrap();
            assert_eq!(result.range, range.start as u64..range.end as u64);
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes, plaintext.slice(range.clone()));
            let owned = bytes.try_into_mut().expect("range must own its allocation");
            assert_eq!(owned.capacity(), range.len());
        }
    }

    #[tokio::test]
    #[ignore = "offline repeated encrypted point-read measurement"]
    async fn nearby_ranges_reuse_one_authenticated_chunk_offline() {
        let encrypted = EncryptedObjectStore::new(
            Arc::new(InMemory::new()),
            "repo-a",
            vec![EncryptionKey::new(1, [7; 32])],
            1,
        )
        .unwrap();
        let location = Path::from("compacted/nearby-ranges.sst");
        let plaintext = Bytes::from(
            (0..DEFAULT_CHUNK_SIZE)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
        );
        encrypted
            .put(&location, plaintext.clone().into())
            .await
            .unwrap();
        encrypted.get_range(&location, 0..64).await.unwrap();
        let ranges = (0..16)
            .map(|index| index * 8192..index * 8192 + 64)
            .collect::<Vec<_>>();
        let groups = 100;
        for modes in [
            ["individual", "manual", "get_ranges"],
            ["get_ranges", "manual", "individual"],
        ] {
            for mode in modes {
                let before = encrypted.read_metrics.snapshot();
                let started = std::time::Instant::now();
                for _ in 0..groups {
                    match mode {
                        "manual" => {
                            let chunk = encrypted
                                .get_range(&location, 0..DEFAULT_CHUNK_SIZE as u64)
                                .await
                                .unwrap();
                            for range in &ranges {
                                assert_eq!(&chunk[range.clone()], &plaintext[range.clone()]);
                            }
                        }
                        "get_ranges" => {
                            let requested = ranges
                                .iter()
                                .map(|range| range.start as u64..range.end as u64)
                                .collect::<Vec<_>>();
                            let actual = encrypted.get_ranges(&location, &requested).await.unwrap();
                            for (result, range) in actual.iter().zip(&ranges) {
                                assert_eq!(result, &plaintext.slice(range.clone()));
                            }
                        }
                        _ => {
                            for range in &ranges {
                                let actual = encrypted
                                    .get_range(&location, range.start as u64..range.end as u64)
                                    .await
                                    .unwrap();
                                assert_eq!(actual, plaintext.slice(range.clone()));
                            }
                        }
                    }
                }
                let requests = groups
                    * if mode == "individual" {
                        ranges.len()
                    } else {
                        1
                    };
                let after = encrypted.read_metrics.snapshot();
                let origin_bytes = after["ciphertext_origin_bytes"].as_u64().unwrap()
                    - before["ciphertext_origin_bytes"].as_u64().unwrap();
                assert_eq!(
                    origin_bytes, 0,
                    "warm nearby ranges must reuse authenticated chunks"
                );
                eprintln!("mode={mode} groups={groups} requests={requests} ciphertext_bytes={origin_bytes} elapsed_ms={}",
                    started.elapsed().as_millis());
            }
        }
    }

    #[tokio::test]
    #[ignore = "offline persisted encrypted SST read attribution"]
    async fn encrypted_sst_nearby_gets_vs_multi_get_offline() {
        let raw = Arc::new(CountingStore::default());
        let mut template = EncryptedObjectStore::new(
            raw.clone(),
            "repo-a",
            vec![EncryptionKey::new(1, [7; 32])],
            1,
        )
        .unwrap();
        template.chunks = Arc::new(ChunkCache::new(0, template.read_metrics.clone()));
        let encrypted = Arc::new(template);
        let path = "encrypted-nearby-sst";
        let writer = Db::builder(path, encrypted.clone())
            .with_db_cache_disabled()
            .build()
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        for ordinal in 0..4096u32 {
            batch.put(
                format!("key:{ordinal:05}"),
                vec![(ordinal % 251) as u8; 1024],
            );
        }
        writer.write(batch).await.unwrap();
        writer
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        let keys = (1024..1040)
            .map(|ordinal| format!("key:{ordinal:05}"))
            .collect::<Vec<_>>();
        let groups = 32;
        for modes in [
            ["individual", "batch", "cached", "chunks"],
            ["chunks", "cached", "batch", "individual"],
        ] {
            let mut individual = None;
            let mut batched = None;
            for mode in modes {
                let reader_store: Arc<dyn ObjectStore> = if mode == "chunks" {
                    let mut store = encrypted.with_inner(raw.clone());
                    store.chunks = Arc::new(ChunkCache::new(
                        DEFAULT_CHUNK_CACHE_BYTES,
                        store.read_metrics.clone(),
                    ));
                    Arc::new(store)
                } else {
                    encrypted.clone()
                };
                let builder = Db::builder(path, reader_store);
                let reader = if mode == "cached" {
                    builder.build().await.unwrap()
                } else {
                    builder.with_db_cache_disabled().build().await.unwrap()
                };
                if mode == "cached" || mode == "chunks" {
                    for key in &keys {
                        reader.get(key).await.unwrap();
                    }
                }
                raw.reads.lock().unwrap().clear();
                let started = std::time::Instant::now();
                let mut results = Vec::new();
                for _ in 0..groups {
                    results = if mode == "batch" {
                        reader.multi_get(&keys).await.unwrap()
                    } else {
                        let mut results = Vec::new();
                        for key in &keys {
                            results.push(reader.get(key).await.unwrap());
                        }
                        results
                    };
                }
                let elapsed = started.elapsed();
                assert_eq!(results.len(), keys.len());
                for (offset, value) in results.into_iter().enumerate() {
                    assert_eq!(
                        value.unwrap(),
                        Bytes::from(vec![((1024 + offset) % 251) as u8; 1024])
                    );
                }
                let reads = std::mem::take(&mut *raw.reads.lock().unwrap());
                let mut by_class = std::collections::BTreeMap::new();
                let mut by_size = std::collections::BTreeMap::new();
                for (location, start, bytes) in reads {
                    let class = if location.contains("/wal/") {
                        "wal"
                    } else if location.contains(".sst") {
                        "sst"
                    } else if location.contains("manifest") {
                        "manifest"
                    } else {
                        "other"
                    };
                    let totals = by_class.entry(class).or_insert((0usize, 0u64, 0u64));
                    totals.0 += 1;
                    totals.1 += bytes;
                    totals.2 = totals.2.max(start);
                    *by_size.entry((class, bytes)).or_insert(0usize) += 1;
                }
                let sst = by_class.get("sst").copied().unwrap_or_default();
                if mode == "batch" {
                    batched = Some(sst);
                } else if mode == "cached" {
                    assert_eq!(
                        sst.0, 0,
                        "warm cached point reads must not reach encrypted SST storage"
                    );
                } else if mode == "chunks" {
                    assert_eq!(sst.1, 0, "warm authenticated chunks must eliminate SST payload reads even without the DB cache");
                } else {
                    individual = Some(sst);
                }
                eprintln!("mode={mode} operations={} elapsed_us={} reads_by_class={by_class:?} reads_by_size={by_size:?}",
                    groups * keys.len(), elapsed.as_micros());
                reader.close().await.unwrap();
            }
            let individual = individual.unwrap();
            let batched = batched.unwrap();
            assert!(individual.0 > batched.0 * 4 && individual.1 > batched.1 * 4);
        }
    }

    fn rustcrypto_encrypt(location: &Path, plaintext: &[u8]) -> Bytes {
        let header = Header {
            key_version: 1,
            chunk_size: 16,
            plaintext_len: plaintext.len(),
            nonce: [11; NONCE_SIZE],
        };
        let cipher = Aes256Gcm::new_from_slice(&[7; 32]).unwrap();
        let mut result = BytesMut::with_capacity(ciphertext_len(header).unwrap());
        encode_header(header, &mut result);
        for index in 0..chunk_count(header.plaintext_len, header.chunk_size) {
            let start = index * header.chunk_size;
            let end = (start + header.chunk_size).min(plaintext.len());
            let nonce = chunk_nonce(header.nonce, index).unwrap();
            let aad = associated_data("repo-a", location, header, index, end - start);
            let encrypted = cipher
                .encrypt(
                    aes_gcm::Nonce::from_slice(&nonce),
                    Payload {
                        msg: &plaintext[start..end],
                        aad: &aad,
                    },
                )
                .unwrap();
            result.extend_from_slice(&encrypted);
        }
        result.freeze()
    }

    fn rustcrypto_decrypt(location: &Path, ciphertext: &[u8]) -> Bytes {
        let header = decode_header(ciphertext).unwrap();
        let cipher = Aes256Gcm::new_from_slice(&[7; 32]).unwrap();
        let mut result = BytesMut::with_capacity(header.plaintext_len);
        let mut offset = HEADER_SIZE;
        for index in 0..chunk_count(header.plaintext_len, header.chunk_size) {
            let plaintext_len = plaintext_chunk_len(header, index);
            let encrypted_len = plaintext_len + TAG_SIZE;
            let nonce = chunk_nonce(header.nonce, index).unwrap();
            let aad = associated_data("repo-a", location, header, index, plaintext_len);
            let plaintext = cipher
                .decrypt(
                    aes_gcm::Nonce::from_slice(&nonce),
                    Payload {
                        msg: &ciphertext[offset..offset + encrypted_len],
                        aad: &aad,
                    },
                )
                .unwrap();
            result.extend_from_slice(&plaintext);
            offset += encrypted_len;
        }
        result.freeze()
    }

    #[tokio::test]
    async fn multipart_surfaces_unsupported_attributes_when_upload_starts() {
        let root = std::env::temp_dir().join(format!(
            "vaulticdb-encryption-multipart-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let inner: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&root).unwrap());
        let encrypted =
            EncryptedObjectStore::new(inner, "repo-a", vec![EncryptionKey::new(1, [7; 32])], 1)
                .unwrap();
        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::Metadata("slatedb-put-id".into()),
            AttributeValue::from("test-put-id"),
        );

        let result = encrypted
            .put_multipart_opts(
                &Path::from("compacted/test.sst"),
                PutMultipartOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await;

        assert!(matches!(
            result,
            Err(slatedb::object_store::Error::NotSupported { .. }
                | slatedb::object_store::Error::NotImplemented { .. })
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn multipart_preserves_submission_order_when_parts_complete_out_of_order() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner, "repo-a");
        let path = Path::from("compacted/ordered.sst");
        let mut upload = encrypted.put_multipart(&path).await.unwrap();
        let first = upload.put_part(Bytes::from_static(b"first-").into());
        let second = upload.put_part(Bytes::from_static(b"second-").into());
        let third = upload.put_part(Bytes::from_static(b"third").into());

        third.await.unwrap();
        first.await.unwrap();
        second.await.unwrap();
        upload.complete().await.unwrap();

        assert_eq!(
            encrypted.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"first-second-third")
        );
    }

    #[tokio::test]
    async fn separate_slatedb_wal_is_encrypted_at_rest() {
        let main: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let raw_wal: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let template = store(main.clone(), "repo-a");
        let encrypted_main: Arc<dyn ObjectStore> = Arc::new(template.clone());
        let encrypted_wal: Arc<dyn ObjectStore> = Arc::new(template.with_inner(raw_wal.clone()));
        let db = Db::builder("encrypted-wal", encrypted_main)
            .with_wal_object_store(encrypted_wal)
            .build()
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"plaintext-key", b"plaintext-value");
        db.write(batch)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();

        let objects = raw_wal.list(None).try_collect::<Vec<_>>().await.unwrap();
        assert!(!objects.is_empty());
        for object in objects {
            let raw = raw_wal
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert!(!raw
                .windows(b"plaintext-key".len())
                .any(|window| window == b"plaintext-key"));
            assert!(!raw
                .windows(b"plaintext-value".len())
                .any(|window| window == b"plaintext-value"));
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn round_trip_and_ranges_hide_plaintext() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("wal/0001");
        let plaintext = Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz");
        encrypted
            .put(&path, plaintext.clone().into())
            .await
            .unwrap();

        let raw = inner.get(&path).await.unwrap().bytes().await.unwrap();
        assert!(!raw
            .windows(plaintext.len())
            .any(|window| window == plaintext));
        assert_eq!(
            encrypted.get(&path).await.unwrap().bytes().await.unwrap(),
            plaintext
        );
        assert_eq!(
            encrypted.get_range(&path, 14..20).await.unwrap(),
            Bytes::from_static(b"efghij")
        );
        assert_eq!(
            encrypted
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Suffix(4)))
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"wxyz")
        );
        assert_eq!(encrypted.head(&path).await.unwrap().size, 36);
    }

    #[tokio::test]
    async fn aws_lc_and_rustcrypto_ciphertexts_are_compatible() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("sst/cross-backend");
        let plaintext = Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz");

        inner
            .put(&path, rustcrypto_encrypt(&path, &plaintext).into())
            .await
            .unwrap();
        assert_eq!(
            encrypted.get(&path).await.unwrap().bytes().await.unwrap(),
            plaintext
        );

        encrypted
            .put(&path, plaintext.clone().into())
            .await
            .unwrap();
        let raw = inner.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(rustcrypto_decrypt(&path, &raw), plaintext);
    }

    #[tokio::test]
    async fn parallel_chunks_round_trip_and_reject_tampering() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("compacted/parallel.sst");
        let plaintext = Bytes::from((0..=255).cycle().take(16 * 9 + 3).collect::<Vec<_>>());

        encrypted
            .put(&path, plaintext.clone().into())
            .await
            .unwrap();
        assert_eq!(
            encrypted.get(&path).await.unwrap().bytes().await.unwrap(),
            plaintext
        );
        assert_eq!(
            encrypted.get_range(&path, 7..137).await.unwrap(),
            plaintext.slice(7..137)
        );

        let mut raw = inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        raw[HEADER_SIZE + 5 * (16 + TAG_SIZE)] ^= 1;
        inner.put(&path, raw.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn keyring_rotation_switches_writes_and_preserves_old_reads() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let old_path = Path::from("wal/old");
        let new_path = Path::from("wal/new");
        encrypted
            .put(&old_path, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        encrypted
            .install_write_key(EncryptionKey::new(2, [9; 32]))
            .unwrap();
        encrypted
            .put(&new_path, Bytes::from_static(b"new").into())
            .await
            .unwrap();
        let old_raw = inner.get(&old_path).await.unwrap().bytes().await.unwrap();
        let new_raw = inner.get(&new_path).await.unwrap().bytes().await.unwrap();
        assert_eq!(decode_header(&old_raw).unwrap().key_version, 1);
        assert_eq!(decode_header(&new_raw).unwrap().key_version, 2);
        assert_eq!(
            encrypted
                .get(&old_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "old"
        );
        assert_eq!(
            encrypted
                .get(&new_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "new"
        );
    }

    #[tokio::test]
    async fn empty_object_is_authenticated() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("manifest/empty");
        encrypted.put(&path, Bytes::new().into()).await.unwrap();
        assert!(inner.head(&path).await.unwrap().size > HEADER_SIZE as u64);
        assert!(encrypted
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .is_empty());

        let mut raw = inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        raw[18] ^= 1;
        inner.put(&path, raw.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn repeated_object_writes_use_fresh_nonces() {
        let inner = Arc::new(InMemory::new());
        let store = EncryptedObjectStore::new(
            inner.clone(),
            "repo-a",
            vec![EncryptionKey::new(1, [7; 32])],
            1,
        )
        .unwrap();
        let location = Path::from("manifest/nonce-test");
        store
            .put(&location, Bytes::from_static(b"same plaintext").into())
            .await
            .unwrap();
        let first = inner.get(&location).await.unwrap().bytes().await.unwrap();
        store
            .put(&location, Bytes::from_static(b"same plaintext").into())
            .await
            .unwrap();
        let second = inner.get(&location).await.unwrap().bytes().await.unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn tamper_and_cross_repository_copy_fail_authentication() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = store(inner.clone(), "repo-a");
        let other = store(inner.clone(), "repo-b");
        let path = Path::from("compacted/1.sst");
        source
            .put(&path, Bytes::from_static(b"secret metadata").into())
            .await
            .unwrap();
        assert!(other.get(&path).await.is_err());

        let mut raw = inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        raw[HEADER_SIZE] ^= 1;
        inner.put(&path, raw.into()).await.unwrap();
        assert!(source.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn malformed_truncated_reordered_and_relocated_objects_fail_closed() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("sst/source");
        let relocated = Path::from("sst/relocated");
        encrypted
            .put(
                &path,
                Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz").into(),
            )
            .await
            .unwrap();
        let original = inner.get(&path).await.unwrap().bytes().await.unwrap();

        inner
            .put(&relocated, original.clone().into())
            .await
            .unwrap();
        assert!(encrypted.get(&relocated).await.is_err());

        let mut corrupted = original.to_vec();
        corrupted[8] = FORMAT_VERSION + 1;
        inner.put(&path, corrupted.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());

        let mut corrupted = original.to_vec();
        corrupted[10..14].copy_from_slice(&99u32.to_be_bytes());
        inner.put(&path, corrupted.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());

        inner
            .put(&path, original.slice(..original.len() - 1).into())
            .await
            .unwrap();
        assert!(encrypted.get(&path).await.is_err());

        let mut reordered = original.to_vec();
        let encrypted_chunk_len = 16 + TAG_SIZE;
        let first = reordered[HEADER_SIZE..HEADER_SIZE + encrypted_chunk_len].to_vec();
        let second = reordered
            [HEADER_SIZE + encrypted_chunk_len..HEADER_SIZE + 2 * encrypted_chunk_len]
            .to_vec();
        reordered[HEADER_SIZE..HEADER_SIZE + encrypted_chunk_len].copy_from_slice(&second);
        reordered[HEADER_SIZE + encrypted_chunk_len..HEADER_SIZE + 2 * encrypted_chunk_len]
            .copy_from_slice(&first);
        inner.put(&path, reordered.into()).await.unwrap();
        assert!(encrypted.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn ranges_multipart_conditions_copy_and_list_preserve_object_store_semantics() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let encrypted = store(inner.clone(), "repo-a");
        let path = Path::from("wal/multipart");
        let mut upload = encrypted.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        upload
            .put_part(Bytes::from_static(b"ghijklmnopqrstuvwxyz").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();

        assert_eq!(
            encrypted.get_range(&path, 0..16).await.unwrap(),
            "0123456789abcdef"
        );
        assert_eq!(
            encrypted.get_range(&path, 16..32).await.unwrap(),
            "ghijklmnopqrstuv"
        );
        assert_eq!(
            encrypted
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Offset(32)))
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "wxyz"
        );
        assert_eq!(
            encrypted
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Suffix(999)))
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "0123456789abcdefghijklmnopqrstuvwxyz"
        );
        assert!(encrypted
            .put_opts(
                &path,
                Bytes::from_static(b"replacement").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .is_err());

        let copy = Path::from("wal/copied");
        encrypted.copy(&path, &copy).await.unwrap();
        assert_eq!(
            encrypted.get(&copy).await.unwrap().bytes().await.unwrap(),
            "0123456789abcdefghijklmnopqrstuvwxyz"
        );
        assert_ne!(
            inner.get(&path).await.unwrap().bytes().await.unwrap(),
            inner.get(&copy).await.unwrap().bytes().await.unwrap()
        );
        let mut listed = encrypted
            .list(Some(&Path::from("wal")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        listed.sort_by(|left, right| left.location.cmp(&right.location));
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().all(|meta| meta.size == 36));
    }

    #[test]
    fn integrity_errors_are_recognized_through_object_store_source_chain() {
        let error = encryption_error(EncryptionError::Authentication);
        assert!(is_integrity_error(&error));
    }
}
