#[cfg(test)]
mod tests {
    //! Metadata object encryption tests.

    use super::*;
    use futures_util::TryStreamExt;
    use slatedb::object_store::{memory::InMemory, ObjectStoreExt};

    fn store(inner: Arc<dyn ObjectStore>, repository: &str) -> EncryptedObjectStore {
        EncryptedObjectStore::new(inner, repository, vec![EncryptionKey::new(1, [7; 32])], 1)
            .unwrap()
            .with_chunk_size(16)
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
