#[cfg(test)]
mod tests {
    //! Daemon service integration tests.

    use super::*;
    use std::io::Write;
    use std::os::fd::IntoRawFd;
    use std::os::unix::net::UnixStream as StdUnixStream;
    use std::sync::{Mutex, OnceLock};
    use vaulticdb::encryption::recovery_capsule::{
        publish_local, CapsuleBuilder, MemberCredential,
    };

    #[test]
    fn core_dumps_are_disabled() {
        disable_core_dumps();
        let mut limit = libc::rlimit {
            rlim_cur: libc::RLIM_INFINITY,
            rlim_max: libc::RLIM_INFINITY,
        };
        assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) }, 0);
        assert_eq!(limit.rlim_cur, 0);
        assert_eq!(limit.rlim_max, 0);
    }

    fn transport_environment_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn unix_is_the_default_transport() {
        let _guard = transport_environment_lock().lock().unwrap();
        for key in [
            "VAULTICDB_TRANSPORT",
            "VAULTICDB_SOCKET",
            "VAULTICDB_TCP_ALLOWLIST",
            "VAULTICDB_TCP_AUTH_TOKEN_FD",
        ] {
            unsafe { env::remove_var(key) };
        }
        assert!(
            matches!(config::transport_from_env("", "/tmp/vaulticdb", false).unwrap(), TransportConfig::Unix(path) if path == PathBuf::from(config::default_socket_path("/tmp/vaulticdb", "")))
        );
    }

    #[test]
    fn capsule_migration_proof_binds_key_repository_and_digest() {
        let mut proof = Hmac::<Sha256>::new_from_slice(b"repository-key").unwrap();
        proof.update(b"vaultic-capsule-migration-finalize-v1\0repository-a\0");
        proof.update("ab".repeat(32).as_bytes());
        let proof = proof.finalize().into_bytes();
        assert!(verify_capsule_migration_proof(
            b"repository-key",
            "repository-a",
            &"ab".repeat(32),
            &proof
        )
        .is_ok());
        assert!(verify_capsule_migration_proof(
            b"wrong-key",
            "repository-a",
            &"ab".repeat(32),
            &proof
        )
        .is_err());
        assert!(verify_capsule_migration_proof(
            b"repository-key",
            "repository-b",
            &"ab".repeat(32),
            &proof
        )
        .is_err());
        assert!(verify_capsule_migration_proof(
            b"repository-key",
            "repository-a",
            &"cd".repeat(32),
            &proof
        )
        .is_err());
    }

    #[allow(
        clippy::await_holding_lock,
        reason = "serializes process-wide environment mutation for the full async test"
    )]
    #[tokio::test(flavor = "current_thread")]
    async fn identity_recovery_publishes_capsule_without_opening_database() {
        let _guard = transport_environment_lock().lock().unwrap();
        let root = env::temp_dir().join(format!(
            "vaultic-capsule-publisher-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&root).unwrap();
        let capsule_directory = root.join("capsules");
        let credentials = [
            ("alice", MemberCredential::Passphrase(b"alice passphrase")),
            ("bob", MemberCredential::Passphrase(b"bob passphrase")),
        ];
        let current = CapsuleBuilder::new("repo-a", 1)
            .broker_identity_public_key(&[1; 32])
            .create_offline_threshold("operators", 2, &credentials, &[7; 32], b"master-key")
            .unwrap();
        publish_local(&capsule_directory, &current).unwrap();
        let candidate = CapsuleBuilder::new("repo-a", 2)
            .broker_identity_public_key(&[2; 32])
            .create_offline_threshold("operators", 2, &credentials, &[7; 32], b"master-key")
            .unwrap();
        let encoded = serde_json::to_vec(&candidate).unwrap();
        let digest = format!("{:x}", Sha256::digest(&encoded));
        let candidate_path = root.join("candidate.json");
        std::fs::write(&candidate_path, encoded).unwrap();
        unsafe {
            env::set_var("VAULTICDB_REPOSITORY_ID", "repo-a");
            env::set_var("VAULTICDB_OBJECT_STORE", "memory");
        }
        let arguments = vec![
            "publish-capsule".to_owned(),
            capsule_directory.display().to_string(),
            candidate_path.display().to_string(),
            digest,
            "true".to_owned(),
        ];
        publish_capsule_without_database(&arguments).await.unwrap();
        publish_capsule_without_database(&arguments).await.unwrap();
        let (_, published) =
            encryption::recovery_capsule::discover_latest(&capsule_directory, "repo-a")
                .unwrap()
                .unwrap();
        assert_eq!(published, candidate);
        unsafe {
            env::remove_var("VAULTICDB_REPOSITORY_ID");
            env::remove_var("VAULTICDB_OBJECT_STORE");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tcp_requires_authentication_and_allowlist() {
        let _guard = transport_environment_lock().lock().unwrap();
        unsafe { env::set_var("VAULTICDB_TRANSPORT", "tcp") };
        unsafe { env::remove_var("VAULTICDB_TCP_ALLOWLIST") };
        unsafe { env::remove_var("VAULTICDB_TCP_AUTH_TOKEN_FD") };
        assert!(config::transport_from_env("", "/tmp/vaulticdb", false).is_err());
        unsafe { env::set_var("VAULTICDB_TCP_ALLOWLIST", "127.0.0.1/32,::1/128") };
        assert!(config::transport_from_env("", "/tmp/vaulticdb", false).is_err());
        assert!(
            matches!(config::transport_from_env("", "/tmp/vaulticdb", true).unwrap(), TransportConfig::Tcp { allowlist, .. } if allowlist.len() == 2)
        );
        for key in [
            "VAULTICDB_TRANSPORT",
            "VAULTICDB_TCP_ALLOWLIST",
            "VAULTICDB_TCP_AUTH_TOKEN_FD",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    fn tcp_authentication_descriptor_is_consumed_and_closed() {
        let _guard = transport_environment_lock().lock().unwrap();
        let (mut writer, reader) = StdUnixStream::pair().unwrap();
        writer.write_all(b"test-token").unwrap();
        drop(writer);
        let descriptor = reader.into_raw_fd();
        unsafe { env::set_var("VAULTICDB_TCP_AUTH_TOKEN_FD", descriptor.to_string()) };
        let token = config::read_auth_token().unwrap().unwrap();
        assert_eq!(token.as_str(), "test-token");
        assert!(env::var_os("VAULTICDB_TCP_AUTH_TOKEN_FD").is_none());
        assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EBADF)
        );
    }

    #[test]
    fn singleton_lock_recovers_after_previous_process_exit() {
        let directory = env::temp_dir().join(format!("vaulticdb-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("vaulticdb.lock");
        let first = acquire_singleton_lock(&path).unwrap();
        assert!(acquire_singleton_lock(&path).is_err());
        drop(first);
        assert!(acquire_singleton_lock(&path).is_ok());
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn future_storage_envelopes_enforce_advertised_limits() {
        let mut batch = WriteBatchRequest {
            deletes: vec![Vec::new(); MAX_BATCH_ITEMS as usize],
            ..Default::default()
        };
        assert!(validate_write_batch(&batch).is_ok());
        batch.deletes.push(Vec::new());
        assert_eq!(
            validate_write_batch(&batch).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );

        let oversized = WriteBatchRequest {
            deletes: vec![vec![0; MAX_MESSAGE_BYTES as usize]],
            ..Default::default()
        };
        assert_eq!(
            validate_write_batch(&oversized).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert!(validate_scan(&ScanRequest {
            page_size: MAX_PAGE_ITEMS,
            ..Default::default()
        })
        .is_ok());
        assert!(validate_scan(&ScanRequest::default()).is_err());
        assert!(validate_scan(&ScanRequest {
            page_size: MAX_PAGE_ITEMS + 1,
            ..Default::default()
        })
        .is_err());
    }
}
