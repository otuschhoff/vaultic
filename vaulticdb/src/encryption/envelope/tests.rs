#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn local_slot_round_trip_and_binding() {
        let passphrase = b"correct horse battery staple";
        let (envelope, expected) = new_local_envelope("repo-a", passphrase).unwrap();
        validate_envelope(&envelope, "repo-a").unwrap();
        let (_, actual) = unlock_envelope(&envelope, Some(passphrase)).await.unwrap();
        assert_eq!(actual.as_slice(), expected.as_slice());
        assert!(unlock_envelope(&envelope, Some(b"wrong")).await.is_err());
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
}
