#[cfg(test)]
mod tests {
    //! Key envelope lifecycle tests.

    use super::*;
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct AuditStore {
        inner: InMemory,
        listed_size: AtomicU64,
        started: AtomicUsize,
        active: Semaphore,
        release: Semaphore,
        fail_get: AtomicBool,
    }

    impl std::fmt::Display for AuditStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("audit test store")
        }
    }

    #[async_trait]
    impl ObjectStore for AuditStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> slatedb::object_store::Result<PutResult> {
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
            let _active = self.active.acquire().await.unwrap();
            self.started.fetch_add(1, Ordering::SeqCst);
            self.release.acquire().await.unwrap().forget();
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(slatedb::object_store::Error::Generic {
                    store: "audit-test",
                    source: "injected read failure".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
        ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            let size = self.listed_size.load(Ordering::SeqCst);
            self.inner
                .list(prefix)
                .map(move |object| {
                    object.map(|mut meta| {
                        meta.size = size;
                        meta
                    })
                })
                .boxed()
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

    #[tokio::test]
    async fn local_slot_round_trip_and_binding() {
        let passphrase = b"correct horse battery staple";
        let (envelope, expected) = new_local_envelope("repo-a", passphrase).unwrap();
        validate_envelope(&envelope, "repo-a").unwrap();
        let credentials = ProviderCredentials::default();
        let (_, actual) = unlock_envelope(&envelope, Some(passphrase), &credentials)
            .await
            .unwrap();
        assert_eq!(actual.as_slice(), expected.as_slice());
        assert!(unlock_envelope(&envelope, Some(b"wrong"), &credentials)
            .await
            .is_err());
        assert!(validate_envelope(&envelope, "repo-b").is_err());
    }

    struct MockProvider(&'static str);

    fn mock_binding(context: &KeyContext<'_>) -> [u8; 32] {
        Sha256::digest(format!(
            "{}\0{}\0{}\0{}\0{}",
            context.repository_id,
            context.slot_id,
            context.key_reference,
            context.dek_version,
            context.purpose
        ))
        .into()
    }

    #[async_trait]
    impl KeyProvider for MockProvider {
        fn name(&self) -> &'static str {
            self.0
        }

        async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
            let mut result = mock_binding(context).to_vec();
            result.extend_from_slice(plaintext);
            Ok(result)
        }

        async fn unwrap(
            &self,
            context: &KeyContext<'_>,
            ciphertext: &[u8],
        ) -> Result<Zeroizing<Vec<u8>>> {
            if ciphertext.len() < 32 || ciphertext[..32] != mock_binding(context) {
                bail!("mock provider context mismatch");
            }
            Ok(Zeroizing::new(ciphertext[32..].to_vec()))
        }
    }

    #[tokio::test]
    async fn cloud_slots_wrap_independently_with_bound_context() {
        for provider_name in ["aws-kms", "azure-key-vault", "gcp-kms"] {
            let provider = MockProvider(provider_name);
            let (mut envelope, dek) = new_local_envelope("repo-a", b"local").unwrap();
            add_cloud_slot(&mut envelope, &dek, "cloud", "versioned-key", 1, &provider)
                .await
                .unwrap();
            let slot = envelope
                .slots
                .iter()
                .find(|slot| slot.id == "cloud")
                .unwrap();
            let ciphertext = BASE64.decode(&slot.wrapped_dek).unwrap();
            let context = KeyContext {
                repository_id: &envelope.repository_id,
                slot_id: &slot.id,
                key_reference: &slot.key_reference,
                dek_version: slot.dek_version,
                purpose: "metadata-dek",
            };
            let payload = provider.unwrap(&context, &ciphertext).await.unwrap();
            assert_eq!(
                decode_cloud_payload(&context, &payload).unwrap().as_slice(),
                dek.as_slice()
            );
            let foreign = KeyContext {
                repository_id: "repo-b",
                ..context
            };
            let payload = provider.unwrap(&foreign, &ciphertext).await;
            assert!(payload.is_err() || decode_cloud_payload(&foreign, &payload.unwrap()).is_err());
        }
    }

    #[tokio::test]
    async fn escrow_recovers_without_metadata_and_rejects_foreign_repository() {
        let provider = MockProvider("aws-kms");
        let master_key = b"base64-direct-open-master-key";
        let record = create_escrow_record(
            "repo-a",
            "primary",
            "arn:aws:kms:region:account:key/version",
            master_key,
            &provider,
        )
        .await
        .unwrap();
        let serialized = serde_json::to_vec(&record).unwrap();
        let standalone: EscrowRecord = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(
            recover_escrow_record(&standalone, "repo-a", &provider)
                .await
                .unwrap()
                .as_slice(),
            master_key
        );
        assert!(recover_escrow_record(&standalone, "repo-b", &provider)
            .await
            .is_err());
    }

    #[test]
    fn weak_argon2_parameters_are_rejected() {
        let config = Argon2Config {
            salt: BASE64.encode([1; SALT_BYTES]),
            memory_kib: 1024,
            iterations: 1,
            parallelism: 1,
        };
        assert!(derive_kek(b"passphrase", &config).is_err());
    }

    #[tokio::test]
    async fn recovery_unlock_with_cloud_slot_requires_explicit_acknowledgement() {
        let (mut envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let provider = MockProvider("azure-key-vault");
        add_cloud_slot(
            &mut envelope,
            &dek,
            "cloud-primary",
            "https://example.vault.azure.net/keys/key/version",
            1,
            &provider,
        )
        .await
        .unwrap();
        assert!(enforce_recovery_acknowledgement(&envelope, true, false).is_err());
        enforce_recovery_acknowledgement(&envelope, true, true).unwrap();
        enforce_recovery_acknowledgement(&envelope, false, false).unwrap();
    }

    #[test]
    fn local_slot_lifecycle_preserves_dek_and_generations() {
        let (mut envelope, dek) = new_local_envelope("repo-a", b"first").unwrap();
        add_local_slot(&mut envelope, &dek, "second", b"second", 10, false).unwrap();
        assert_eq!(envelope.generation, 2);
        let second = envelope
            .slots
            .iter()
            .find(|slot| slot.id == "second")
            .unwrap();
        assert_eq!(
            unwrap_local(&envelope, second, b"second")
                .unwrap()
                .as_slice(),
            dek.as_slice()
        );
        rotate_local_slot(&mut envelope, &dek, "second", b"rotated").unwrap();
        assert_eq!(envelope.generation, 3);
        let second = envelope
            .slots
            .iter()
            .find(|slot| slot.id == "second")
            .unwrap();
        assert!(unwrap_local(&envelope, second, b"second").is_err());
        assert_eq!(
            unwrap_local(&envelope, second, b"rotated")
                .unwrap()
                .as_slice(),
            dek.as_slice()
        );
        remove_slot(&mut envelope, "local-recovery").unwrap();
        assert_eq!(envelope.generation, 4);
        assert!(remove_slot(&mut envelope, "second").is_err());
        assert!(add_local_slot(&mut envelope, &dek, "second", b"x", 1, true).is_err());
    }

    #[tokio::test]
    async fn immutable_envelope_generations_select_latest() {
        let store = InMemory::new();
        let (mut first, dek) = new_local_envelope("repo-a", b"first").unwrap();
        publish_envelope(&store, &first).await.unwrap();
        add_local_slot(&mut first, &dek, "second", b"second", 10, false).unwrap();
        publish_envelope(&store, &first).await.unwrap();
        assert!(publish_envelope(&store, &first).await.is_err());
        let loaded = load_envelope(&store, "repo-a").await.unwrap().unwrap();
        assert_eq!(loaded.generation, 2);
        assert_eq!(loaded.slots.len(), 2);
        assert!(load_envelope(&store, "repo-b").await.is_err());
    }

    #[tokio::test]
    async fn plaintext_migration_is_encrypted_and_idempotent() {
        let raw = Arc::new(InMemory::new());
        let location = Path::from("db/manifest/0001");
        let plaintext = b"sensitive SlateDB manifest";
        raw.put(&location, plaintext.as_slice().into())
            .await
            .unwrap();
        let encrypted: Arc<dyn ObjectStore> = Arc::new(
            EncryptedObjectStore::new(
                raw.clone(),
                "repo-a",
                vec![EncryptionKey::new(1, [7; DEK_BYTES])],
                1,
            )
            .unwrap(),
        );
        migrate_plaintext_objects(raw.as_ref(), encrypted.as_ref())
            .await
            .unwrap();
        migrate_plaintext_objects(raw.as_ref(), encrypted.as_ref())
            .await
            .unwrap();
        let persisted = raw.get(&location).await.unwrap().bytes().await.unwrap();
        assert!(persisted.starts_with(crate::encryption::MAGIC));
        assert!(!persisted
            .windows(plaintext.len())
            .any(|window| window == plaintext));
        assert_eq!(
            encrypted
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            plaintext.as_slice()
        );
    }

    #[tokio::test]
    async fn rewrite_authenticates_then_retires_old_deks_across_restart() {
        let raw = Arc::new(InMemory::new());
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        publish_envelope(raw.as_ref(), &envelope).await.unwrap();
        let (store, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let manager = manager.unwrap();
        let old_location = Path::from("db/old-object");
        store
            .put(&old_location, b"old version".as_slice().into())
            .await
            .unwrap();
        let retired_ciphertext = raw.get(&old_location).await.unwrap().bytes().await.unwrap();
        manager.rotate_dek().await.unwrap();
        assert_eq!(
            manager.audit_objects().await.unwrap().old_version_objects,
            1
        );
        assert_eq!(manager.rewrite_old_deks(10).await.unwrap(), (1, 0));
        let (generation, active_version, _) = manager.status().await;
        assert_eq!((generation, active_version), (3, 2));

        raw.put(&old_location, retired_ciphertext.into())
            .await
            .unwrap();
        assert!(store.get(&old_location).await.is_err());

        let restarted_envelope = load_envelope(raw.as_ref(), "repo-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restarted_envelope.retired_through_dek_version, 1);
        let root = unwrap_local(
            &restarted_envelope,
            &restarted_envelope.slots[0],
            b"recovery",
        )
        .unwrap();
        let (restarted, _, _) =
            encrypted_store(raw, "repo-a", restarted_envelope, root, "local-recovery").unwrap();
        assert!(restarted.get(&old_location).await.is_err());
    }

    #[tokio::test]
    async fn plaintext_object_substitution_is_reported_and_rejected() {
        let raw = Arc::new(InMemory::new());
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let (encrypted, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let location = Path::from("db/substituted-manifest");
        raw.put(
            &location,
            b"plaintext attacker replacement".as_slice().into(),
        )
        .await
        .unwrap();

        let audit = manager.unwrap().audit_objects().await.unwrap();
        assert_eq!(audit.objects, 1);
        assert_eq!(audit.plaintext_objects, 1);
        let error = encrypted.get(&location).await.unwrap_err();
        assert!(crate::encryption::is_integrity_error(&error));
    }

    #[tokio::test]
    async fn parallel_audit_matches_serial_and_authenticates_payloads() {
        let raw = Arc::new(InMemory::new());
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let (encrypted, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let manager = manager.unwrap();
        encrypted
            .put(&Path::from("db/old"), b"old".as_slice().into())
            .await
            .unwrap();
        manager.rotate_dek().await.unwrap();
        for ordinal in 0..8 {
            encrypted
                .put(
                    &Path::from(format!("db/current-{ordinal}")),
                    vec![ordinal as u8; 300_000].into(),
                )
                .await
                .unwrap();
        }
        raw.put(&Path::from("db/plain"), b"plain".as_slice().into())
            .await
            .unwrap();
        raw.put(
            &Path::from("db/bad-header"),
            crate::encryption::MAGIC.to_vec().into(),
        )
        .await
        .unwrap();
        raw.put(
            &Path::from("_vaultic/ignored"),
            b"internal".as_slice().into(),
        )
        .await
        .unwrap();
        let serial = manager.audit_objects_with_workers(1).await.unwrap();
        let parallel = manager.audit_objects().await.unwrap();
        assert_eq!(
            (
                serial.objects,
                serial.invalid_objects,
                serial.plaintext_objects,
                serial.old_version_objects
            ),
            (11, 1, 1, 1)
        );
        assert_eq!(
            (
                parallel.objects,
                parallel.invalid_objects,
                parallel.plaintext_objects,
                parallel.old_version_objects
            ),
            (
                serial.objects,
                serial.invalid_objects,
                serial.plaintext_objects,
                serial.old_version_objects
            )
        );
        let location = Path::from("db/current-7");
        let mut bytes = raw
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        raw.put(&location, bytes.into()).await.unwrap();
        for workers in [1, AUDIT_WORKERS] {
            let error = manager
                .audit_objects_with_workers(workers)
                .await
                .unwrap_err();
            assert!(crate::encryption::is_integrity_error(error.as_ref()));
        }
        let error = manager.audit_objects_for_check().await.unwrap_err();
        assert!(crate::encryption::is_integrity_error(error.as_ref()));
    }

    #[tokio::test]
    async fn audit_overlaps_reads_with_count_and_byte_limits() {
        let raw = Arc::new(AuditStore {
            inner: InMemory::new(),
            listed_size: AtomicU64::new(5),
            started: AtomicUsize::new(0),
            active: Semaphore::new(100),
            release: Semaphore::new(0),
            fail_get: AtomicBool::new(false),
        });
        for ordinal in 0..12 {
            raw.put(
                &Path::from(format!("db/{ordinal:02}")),
                b"plain".as_slice().into(),
            )
            .await
            .unwrap();
        }
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let (_, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let manager = manager.unwrap();
        let half_budget = u64::from(AUDIT_MEMORY_UNITS / 2) * AUDIT_MEMORY_UNIT_BYTES;
        for (size, expected) in [(5, 4), (half_budget, 2), (u64::MAX, 1)] {
            raw.listed_size.store(size, Ordering::SeqCst);
            raw.started.store(0, Ordering::SeqCst);
            {
                let audit = manager.audit_objects();
                tokio::pin!(audit);
                assert!(futures_util::poll!(&mut audit).is_pending());
                assert_eq!(raw.started.load(Ordering::SeqCst), expected);
                assert_eq!(raw.active.available_permits(), 100 - expected);
            }
            assert_eq!(raw.active.available_permits(), 100);
        }
        raw.listed_size.store(5, Ordering::SeqCst);
        raw.started.store(0, Ordering::SeqCst);
        raw.release.add_permits(12);
        let audit = manager.audit_objects().await.unwrap();
        assert_eq!((audit.objects, audit.plaintext_objects), (12, 12));
        assert_eq!(raw.started.load(Ordering::SeqCst), 12);
        assert_eq!(raw.active.available_permits(), 100);
        raw.fail_get.store(true, Ordering::SeqCst);
        {
            let audit = manager.audit_objects();
            tokio::pin!(audit);
            assert!(futures_util::poll!(&mut audit).is_pending());
            assert_eq!(raw.active.available_permits(), 96);
            raw.release.add_permits(1);
            let error = audit.await.unwrap_err();
            assert!(error.to_string().contains("injected read failure"));
        }
        assert_eq!(raw.active.available_permits(), 100);
    }

    #[tokio::test]
    async fn audit_byte_admission_is_exclusive_and_cancellation_safe() {
        assert_eq!(audit_memory_units(0), 1);
        assert_eq!(audit_memory_units(AUDIT_MEMORY_UNIT_BYTES), 1);
        assert_eq!(audit_memory_units(AUDIT_MEMORY_UNIT_BYTES + 1), 2);
        assert_eq!(audit_memory_units(u64::MAX), AUDIT_MEMORY_UNITS);
        let memory = Semaphore::new(AUDIT_MEMORY_UNITS as usize);
        let small = memory.acquire_many(1).await.unwrap();
        {
            let large = memory.acquire_many(audit_memory_units(u64::MAX));
            tokio::pin!(large);
            assert!(futures_util::poll!(&mut large).is_pending());
        }
        drop(small);
        assert_eq!(memory.available_permits(), AUDIT_MEMORY_UNITS as usize);
        let large = memory
            .acquire_many(audit_memory_units(u64::MAX))
            .await
            .unwrap();
        assert!(memory.try_acquire().is_err());
        drop(large);
        let permits = futures_util::future::join_all(
            (0..AUDIT_WORKERS).map(|_| memory.acquire_many(AUDIT_MEMORY_UNITS / AUDIT_WORKERS as u32)),
        )
        .await;
        assert!(permits.iter().all(Result::is_ok));
        assert_eq!(memory.available_permits(), 0);
        drop(permits);
        assert_eq!(memory.available_permits(), AUDIT_MEMORY_UNITS as usize);
    }

    #[tokio::test]
    async fn audit_fails_closed_when_listed_metadata_disappears() {
        let raw = Arc::new(AuditStore {
            inner: InMemory::new(),
            listed_size: AtomicU64::new(5),
            started: AtomicUsize::new(0),
            active: Semaphore::new(100),
            release: Semaphore::new(0),
            fail_get: AtomicBool::new(false),
        });
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let (_, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let manager = manager.unwrap();
        let location = Path::from("db/compactions/00000000000000000377.compactions");
        for workers in [1, AUDIT_WORKERS] {
            raw.put(&location, b"plain".as_slice().into()).await.unwrap();
            {
                let audit = manager.audit_objects_with_workers(workers);
                tokio::pin!(audit);
                assert!(futures_util::poll!(&mut audit).is_pending());
                assert_eq!(raw.active.available_permits(), 99);
                raw.inner.delete(&location).await.unwrap();
                raw.release.add_permits(1);
                let error = audit.await.unwrap_err();
                assert!(matches!(
                    error.downcast_ref::<slatedb::object_store::Error>(),
                    Some(slatedb::object_store::Error::NotFound { .. })
                ));
            }
            assert_eq!(raw.active.available_permits(), 100);
            assert_eq!(manager.audit_objects().await.unwrap().objects, 0);
        }
    }

    #[tokio::test]
    async fn check_audit_relists_once_only_for_missing_objects() {
        let raw = Arc::new(AuditStore {
            inner: InMemory::new(),
            listed_size: AtomicU64::new(5),
            started: AtomicUsize::new(0),
            active: Semaphore::new(100),
            release: Semaphore::new(0),
            fail_get: AtomicBool::new(false),
        });
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let (_, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let manager = manager.unwrap();
        let location = Path::from("db/compactions/first");
        let replacement = Path::from("db/compactions/replacement");
        for missing_again in [false, true] {
            raw.started.store(0, Ordering::SeqCst);
            raw.put(&location, b"plain".as_slice().into()).await.unwrap();
            {
                let audit = manager.audit_objects_for_check();
                tokio::pin!(audit);
                assert!(futures_util::poll!(&mut audit).is_pending());
                assert_eq!(raw.started.load(Ordering::SeqCst), 1);
                raw.inner.delete(&location).await.unwrap();
                raw.put(&replacement, b"plain".as_slice().into()).await.unwrap();
                raw.release.add_permits(1);
                assert!(futures_util::poll!(&mut audit).is_pending());
                assert_eq!(raw.started.load(Ordering::SeqCst), 2);
                if missing_again {
                    raw.inner.delete(&replacement).await.unwrap();
                }
                raw.release.add_permits(1);
                let result = audit.await;
                if missing_again {
                    let error = result.unwrap_err();
                    assert!(matches!(
                        error.downcast_ref::<slatedb::object_store::Error>(),
                        Some(slatedb::object_store::Error::NotFound { .. })
                    ));
                } else {
                    let audit = result.unwrap();
                    assert_eq!((audit.objects, audit.plaintext_objects), (1, 1));
                    raw.inner.delete(&replacement).await.unwrap();
                }
                assert_eq!(raw.started.load(Ordering::SeqCst), 2);
            }
            assert_eq!(raw.active.available_permits(), 100);
        }
        raw.put(&location, b"plain".as_slice().into()).await.unwrap();
        raw.started.store(0, Ordering::SeqCst);
        raw.fail_get.store(true, Ordering::SeqCst);
        raw.release.add_permits(2);
        let error = manager.audit_objects_for_check().await.unwrap_err();
        assert!(error.to_string().contains("injected read failure"));
        assert_eq!(raw.started.load(Ordering::SeqCst), 1);
        assert_eq!(raw.active.available_permits(), 100);
    }
}
