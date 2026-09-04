#[cfg(test)]
mod tests {
    use super::*;

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
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use ed25519_dalek::{Signature, Verifier};
    use ed25519_dalek::{Signer, SigningKey};
    use rand08::rngs::OsRng as LegacyOsRng;
    use sha2::{Digest, Sha256};
    use std::{
        collections::BTreeSet, env, ffi::OsString, fs, os::unix::fs::MetadataExt, path::Path,
        time::Duration,
    };
    use vaulticdb::{
        broker::{encrypt_offline_contribution, ClientAuthorization},
        encryption::recovery_capsule::{CapsuleBuilder, MemberCredential},
    };

    async fn exchange(stream: &mut UnixStream, request: serde_json::Value) -> serde_json::Value {
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        stream.write_all(&encoded).await.unwrap();
        let mut response = Vec::new();
        BufReader::new(stream)
            .read_until(b'\n', &mut response)
            .await
            .unwrap();
        serde_json::from_slice(&response).unwrap()
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn peer_inspection_uses_kernel_identity_and_running_executable() {
        let (peer, _other) = UnixStream::pair().unwrap();
        let inspected = inspect_peer(&peer).unwrap();
        let executable = env::current_exe().unwrap();
        assert_eq!(inspected.uid, unsafe { libc::geteuid() });
        assert_eq!(
            inspected.executable_sha256,
            format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()))
        );
        assert_eq!(
            inspected.owned_by_root,
            fs::metadata(&executable).unwrap().uid() == 0
        );
        assert_eq!(
            inspected.installation_path_read_only,
            trusted_installation_path(&executable).unwrap()
        );
    }

    #[tokio::test]
    async fn stale_socket_cleanup_rejects_active_and_non_socket_paths() {
        let root = PathBuf::from("/tmp").join(format!(
            "vbsc-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let socket = root.join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        assert!(remove_stale_socket(&socket).await.is_err());
        drop(listener);
        remove_stale_socket(&socket).await.unwrap();
        assert!(!socket.exists());

        fs::write(&socket, b"do not replace").unwrap();
        assert!(remove_stale_socket(&socket).await.is_err());
        assert_eq!(fs::read(&socket).unwrap(), b"do not replace");
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn installation_path_rejects_mutable_ancestors_and_accepts_system_binary() {
        let root = env::temp_dir().join(format!(
            "vaultic-broker-untrusted-path-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let executable = root.join("vaulticdb");
        fs::write(&executable, b"not an executable").unwrap();
        assert!(!trusted_installation_path(&executable).unwrap());
        fs::remove_dir_all(root).unwrap();

        assert!(trusted_installation_path(Path::new("/bin/sh")).unwrap());
    }

    #[test]
    fn provisioning_creates_pinned_identity_and_verifiable_release() {
        let root = env::temp_dir().join(format!(
            "vaultic-broker-provisioning-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let identity_private = root.join("identity.key");
        let identity_public = root.join("identity.pub");
        identity_init(&[
            identity_private.clone().into_os_string(),
            identity_public.clone().into_os_string(),
        ])
        .unwrap();
        assert_eq!(fs::read(&identity_private).unwrap().len(), 32);
        assert_eq!(
            BASE64
                .decode(
                    String::from_utf8(fs::read(&identity_public).unwrap())
                        .unwrap()
                        .trim()
                )
                .unwrap()
                .len(),
            32
        );

        let release_private = root.join("release.key");
        let release_key = SigningKey::from_bytes(&[9; 32]);
        write_new_file(&release_private, release_key.as_bytes(), 0o600).unwrap();
        let executable = env::current_exe().unwrap();
        let manifest_path = root.join("release.json");
        release_sign(&[
            release_private.into_os_string(),
            executable.clone().into_os_string(),
            OsString::from("vaulticdb"),
            OsString::from("7"),
            OsString::from("release-a"),
            manifest_path.clone().into_os_string(),
        ])
        .unwrap();
        let manifest: vaulticdb::broker::ReleaseManifest =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
        assert_eq!(
            manifest.executable_sha256,
            format!("{:x}", Sha256::digest(fs::read(executable).unwrap()))
        );
        let message = serde_json::to_vec(&(
            "vaultic-client-release-v1",
            &manifest.component,
            manifest.version,
            &manifest.executable_sha256,
            &manifest.release_identity,
        ))
        .unwrap();
        let signature = Signature::from_slice(&BASE64.decode(manifest.signature).unwrap()).unwrap();
        release_key
            .verifying_key()
            .verify(&message, &signature)
            .unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lease_challenge_is_protocol_and_executable_bound() {
        let challenge = "challenge-a";
        let digest = "ab".repeat(32);
        let response = lease_challenge_response(challenge, &digest);
        assert_eq!(response.len(), 64);
        assert_eq!(response, lease_challenge_response(challenge, &digest));
        assert_ne!(response, lease_challenge_response("challenge-b", &digest));
        assert_ne!(
            response,
            lease_challenge_response(challenge, &"cd".repeat(32))
        );
    }

    #[test]
    fn connection_protocol_consumes_lease_challenge_once() {
        let mut protocol = ConnectionProtocol {
            negotiated: true,
            lease_challenge: Some("challenge-a".to_owned()),
        };
        assert_eq!(
            protocol.lease_challenge.take().as_deref(),
            Some("challenge-a")
        );
        assert!(protocol.lease_challenge.take().is_none());
    }

    #[test]
    fn security_events_reject_secret_bearing_fields() {
        assert!(security_event_json(
            "notice",
            "auth",
            "lease_granted",
            &[
                ("component", "vaultic".to_owned()),
                ("release_identity", "release-a".to_owned())
            ],
        )
        .unwrap()
        .contains("\"component\":\"vaultic-key-broker\""));
        assert!(security_event_json(
            "warning",
            "auth",
            "request_rejected",
            &[("bearer_token", "secret-token".to_owned())],
        )
        .is_err());
    }

    #[test]
    fn contribution_rejections_have_stable_secret_free_events() {
        let authentication = anyhow::Error::new(ContributionRejection::PayloadAuthentication);
        let (severity, category, event, fields) = rejection_event(&authentication, "connection-a");
        assert_eq!(
            (severity, category, event),
            (
                "warning",
                "integrity",
                "contribution_payload_authentication_failed"
            )
        );
        let encoded = security_event_json(severity, category, event, &fields).unwrap();
        assert!(encoded.contains("\"connection_id\":\"connection-a\""));
        assert!(!encoded.contains("ciphertext"));

        let invalid = anyhow::Error::new(ContributionRejection::PayloadInvalid);
        let (severity, category, event, fields) = rejection_event(&invalid, "connection-b");
        assert_eq!(
            (severity, category, event),
            ("warning", "integrity", "contribution_payload_invalid")
        );
        let encoded = security_event_json(severity, category, event, &fields).unwrap();
        assert!(encoded.contains("\"connection_id\":\"connection-b\""));
        assert!(!encoded.contains("ciphertext"));

        let rollback = anyhow::Error::new(ContributionRejection::Rollback {
            last_seen_generation: 8,
            current_generation: 7,
        });
        let (severity, category, event, fields) = rejection_event(&rollback, "connection-b");
        assert_eq!(
            (severity, category, event),
            ("critical", "integrity", "capsule_rollback_rejected")
        );
        let encoded = security_event_json(severity, category, event, &fields).unwrap();
        assert!(encoded.contains("\"last_seen_generation\":\"8\""));
        assert!(encoded.contains("\"current_generation\":\"7\""));
        assert!(!encoded.contains("share"));
    }

    #[tokio::test]
    async fn unix_service_negotiates_connections_independently_and_broadcasts_lock() {
        let identity = SigningKey::generate(&mut LegacyOsRng);
        let capsule = CapsuleBuilder::new("repo-a", 1)
            .broker_identity_public_key(identity.verifying_key().as_bytes())
            .create_offline_threshold(
                "operators",
                1,
                &[("alice", MemberCredential::Passphrase(b"alice passphrase"))],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap();
        let broker = Arc::new(Mutex::new(
            KeyBroker::new(capsule, identity, Vec::new(), None).unwrap(),
        ));
        let notification = Arc::new(Notify::new());
        let (mut client_a, server_a) = UnixStream::pair().unwrap();
        let (mut client_b, server_b) = UnixStream::pair().unwrap();
        let task_a = tokio::spawn(serve_connection(
            server_a,
            broker.clone(),
            "unix:/test/broker.sock".to_owned(),
            notification.clone(),
        ));
        let task_b = tokio::spawn(serve_connection(
            server_b,
            broker.clone(),
            "unix:/test/broker.sock".to_owned(),
            notification,
        ));

        for stream in [&mut client_a, &mut client_b] {
            let response = exchange(
                stream,
                serde_json::json!({"operation":"negotiate","protocols":[PROTOCOL_VERSION]}),
            )
            .await;
            assert_eq!(response["result"], "negotiated");
            assert_eq!(response["protocol"], PROTOCOL_VERSION);
            assert!(response["challenge"]
                .as_str()
                .is_some_and(|value| !value.is_empty()));
        }
        let status = exchange(&mut client_b, serde_json::json!({"operation":"status"})).await;
        assert_eq!(status["result"], "status");
        assert_eq!(status["repository_id"], "repo-a");

        let locked = exchange(&mut client_a, serde_json::json!({"operation":"lock"})).await;
        assert_eq!(locked["result"], "ok");
        let mut closed = Vec::new();
        assert_eq!(
            client_b
                .readable()
                .await
                .and_then(|_| client_b.try_read_buf(&mut closed))
                .unwrap(),
            0
        );

        drop(client_a);
        task_a.await.unwrap().unwrap();
        task_b.await.unwrap().unwrap();
        assert!(broker.lock().await.status(unix_time_ms().unwrap()).locked);
    }

    #[tokio::test]
    async fn protocol_policy_mutation_retains_candidate_until_exact_activation() {
        let identity = SigningKey::generate(&mut LegacyOsRng);
        let release_key = SigningKey::from_bytes(&[6; 32]);
        let capsule = CapsuleBuilder::new("repo-a", 1)
            .broker_identity_public_key(identity.verifying_key().as_bytes())
            .create_offline_threshold(
                "operators",
                1,
                &[("alice", MemberCredential::Passphrase(b"alice passphrase"))],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap();
        let authorizations = vec![ClientAuthorization {
            component: "vaultic".to_owned(),
            minimum_version: 20,
            maximum_version: 20,
            release_identity: "release-a".to_owned(),
            release_public_key: release_key.verifying_key().to_bytes(),
            peer_uid: 42,
            capabilities: BTreeSet::from([Capability::PolicyMutation]),
        }];
        let mut state = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let session = state
            .create_session("unix:/test/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        let contribution = encrypt_offline_contribution(
            &capsule,
            &session,
            "unix:/test/broker.sock",
            "alice",
            &MemberCredential::Passphrase(b"alice passphrase"),
            1,
            None,
            1_001,
        )
        .unwrap();
        assert!(state.submit_contribution(contribution, 1_002).unwrap());
        let broker = Mutex::new(state);
        let peer = PeerProcess {
            uid: 42,
            executable_sha256: "ab".repeat(32),
            owned_by_root: true,
            installation_path_read_only: true,
        };
        let authorize = |challenge: &str| {
            let manifest = serde_json::to_vec(&(
                "vaultic-client-release-v1",
                "vaultic",
                20_u64,
                &peer.executable_sha256,
                "release-a",
            ))
            .unwrap();
            AuthorizedOperation {
                component: "vaultic".to_owned(),
                version: 20,
                release_identity: "release-a".to_owned(),
                release_signature: BASE64.encode(release_key.sign(&manifest).to_bytes()),
                challenge_response: lease_challenge_response(challenge, &peer.executable_sha256),
            }
        };
        let mut protocol = ConnectionProtocol {
            negotiated: true,
            lease_challenge: Some("prepare-challenge".to_owned()),
        };
        let prepared = handle_request(
            BrokerRequest::PreparePolicyMutation {
                authorization: authorize("prepare-challenge"),
                policy: UnlockPolicy::Threshold {
                    group_id: "operators".to_owned(),
                    required: 1,
                    members: vec!["bob".to_owned()],
                },
                members: vec![OfflinePolicyMember {
                    member_id: "bob".to_owned(),
                    provider: MemberProvider::OfflineArgon2id,
                    credential: BASE64.encode(b"bob passphrase"),
                }],
                external_members: Vec::new(),
                acknowledge_downgrade: false,
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .unwrap();
        let digest = match prepared {
            BrokerResponse::PolicyMutationPrepared {
                capsule,
                capsule_sha256,
            } => {
                assert_eq!(capsule.header.generation, 2);
                capsule_sha256
            }
            _ => panic!("unexpected mutation response"),
        };
        protocol.lease_challenge = Some("pending-challenge".to_owned());
        let pending = handle_request(
            BrokerRequest::PendingPolicyMutation {
                authorization: authorize("pending-challenge"),
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .unwrap();
        match pending {
            BrokerResponse::PolicyMutationPrepared {
                capsule,
                capsule_sha256,
            } => {
                assert_eq!(capsule.header.generation, 2);
                assert_eq!(capsule_sha256, digest);
            }
            _ => panic!("unexpected pending mutation response"),
        }
        protocol.lease_challenge = Some("wrong-challenge".to_owned());
        assert!(handle_request(
            BrokerRequest::ActivatePolicyMutation {
                authorization: authorize("wrong-challenge"),
                capsule_sha256: "00".repeat(32),
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .is_err());
        assert!(broker.lock().await.status(1_003).policy_mutation_pending);
        protocol.lease_challenge = Some("activate-challenge".to_owned());
        handle_request(
            BrokerRequest::ActivatePolicyMutation {
                authorization: authorize("activate-challenge"),
                capsule_sha256: digest,
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .unwrap();
        let status = broker.lock().await.status(1_004);
        assert!(status.locked);
        assert_eq!(status.capsule_generation, 2);
        assert!(!status.policy_mutation_pending);
    }
}
