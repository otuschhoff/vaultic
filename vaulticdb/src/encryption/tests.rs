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
        payload_gate: Option<Arc<tokio::sync::Semaphore>>,
        head_gate: Option<Arc<tokio::sync::Semaphore>>,
        panic_payload: std::sync::atomic::AtomicBool,
        omit_identity: bool,
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
            Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async { Ok(bytes) }).boxed()),
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
        let inner = Arc::new(CountingStore {
            delay: Some(std::time::Duration::from_millis(5)),
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
        assert!(metrics["header_validation_ns"].as_u64().unwrap() >= 5_000_000);
        assert!(metrics["ciphertext_origin_ns"].as_u64().unwrap() >= 10_000_000);
        assert!(metrics["crypto_service_ns"].as_u64().unwrap() > 0);
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
    fn chunk_cache_configuration_is_bounded() {
        assert_eq!(parse_chunk_cache_bytes("0").unwrap(), 0);
        assert_eq!(
            parse_chunk_cache_bytes("67108864").unwrap(),
            DEFAULT_CHUNK_CACHE_BYTES
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
            parse_chunk_cache_bytes("17179869184").unwrap() as u64,
            MAX_CHUNK_CACHE_BYTES
        );
        for value in [
            "17179869185",
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
            (4096, 64, 16 * 1024 * 1024, 1024 * 1024, 4)
        );
        for (maximum, expected) in [
            (MAX_CHUNK_CACHE_ENTRIES, 65536),
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
