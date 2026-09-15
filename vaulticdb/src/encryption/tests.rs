#[cfg(test)]
mod tests {
    //! Metadata object encryption tests.

    use super::*;
    use aes_gcm::{
        aead::{Aead, Payload},
        Aes256Gcm, KeyInit,
    };
    use futures_util::TryStreamExt;
    use slatedb::object_store::{
        local::LocalFileSystem, Attribute, AttributeValue, Attributes, ObjectStoreExt,
    };
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, WriteBatch};

    fn store(inner: Arc<dyn ObjectStore>, repository: &str) -> EncryptedObjectStore {
        EncryptedObjectStore::new(inner, repository, vec![EncryptionKey::new(1, [7; 32])], 1)
            .unwrap()
            .with_chunk_size(16)
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
        let encrypted = EncryptedObjectStore::new(
            inner,
            "repo-a",
            vec![EncryptionKey::new(1, [7; 32])],
            1,
        )
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
        db.write(batch).await.unwrap().await_durable().await.unwrap();

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
            assert!(!raw.windows(b"plaintext-key".len()).any(|window| window == b"plaintext-key"));
            assert!(!raw.windows(b"plaintext-value".len()).any(|window| window == b"plaintext-value"));
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

        let mut raw = inner.get(&path).await.unwrap().bytes().await.unwrap().to_vec();
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
