#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::{memory::InMemory, ObjectStoreExt, PutMode};

    #[tokio::test]
    async fn writes_delete_and_copy_all_replicas() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let source = Path::from("source");
        let target = Path::from("target");
        store
            .put(&source, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        assert_eq!(
            primary.get(&source).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        assert_eq!(
            secondary.get(&source).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        store.copy(&source, &target).await.unwrap();
        assert_eq!(
            primary.get(&target).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        assert_eq!(
            secondary.get(&target).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
        store.delete(&source).await.unwrap();
        assert!(primary.get(&source).await.is_err());
        assert!(secondary.get(&source).await.is_err());
    }

    #[tokio::test]
    async fn reads_fail_over_from_primary_to_secondary() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("object");
        secondary
            .put(&path, Bytes::from_static(b"secondary").into())
            .await
            .unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            b"secondary"[..]
        );
    }

    #[tokio::test]
    async fn multipart_upload_is_replicated() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("multipart");
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"hello ").into())
            .await
            .unwrap();
        upload
            .put_part(Bytes::from_static(b"world").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();
        assert_eq!(
            primary.get(&path).await.unwrap().bytes().await.unwrap(),
            b"hello world"[..]
        );
        assert_eq!(
            secondary.get(&path).await.unwrap().bytes().await.unwrap(),
            b"hello world"[..]
        );
    }

    #[tokio::test]
    async fn create_retry_accepts_identical_existing_replica() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("create");
        primary
            .put(&path, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                Bytes::from_static(b"payload").into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        assert_eq!(
            secondary.get(&path).await.unwrap().bytes().await.unwrap(),
            b"payload"[..]
        );
    }

    #[tokio::test]
    async fn create_retry_rejects_mismatched_existing_replica_before_writing_missing() {
        let primary = Arc::new(InMemory::new());
        let secondary = Arc::new(InMemory::new());
        let store = ReplicatedObjectStore::new(vec![
            ("primary".to_owned(), primary.clone()),
            ("secondary".to_owned(), secondary.clone()),
        ])
        .unwrap();
        let path = Path::from("mismatch");
        primary
            .put(&path, Bytes::from_static(b"different").into())
            .await
            .unwrap();
        assert!(store
            .put_opts(
                &path,
                Bytes::from_static(b"payload").into(),
                PutMode::Create.into(),
            )
            .await
            .is_err());
        assert!(secondary.get(&path).await.is_err());
    }
}
