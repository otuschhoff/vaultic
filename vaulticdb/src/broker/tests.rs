#[cfg(test)]
mod tests {
    //! Key broker behavior tests.

    use super::*;
    use crate::encryption::{
        envelope::providers::{KeyContext, KeyProvider},
        recovery_capsule::{
            CapsuleBuilder, ExternalMemberProtection, MemberProvider, PrincipalBinding,
        },
    };
    use crate::topology::{
        AzureUserDelegationPolicy, Credential, CredentialKind, Provider, S3StsPolicy,
        StaticCredentialPolicy,
    };
    use async_trait::async_trait;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    struct ContextProvider;

    #[async_trait]
    impl KeyProvider for ContextProvider {
        fn name(&self) -> &'static str {
            "azure-key-vault"
        }

        async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
            let binding = serde_json::to_vec(&(
                context.repository_id,
                context.slot_id,
                context.key_reference,
                context.dek_version,
                context.purpose,
            ))?;
            let mut wrapped = Sha256::digest(binding).to_vec();
            wrapped.extend_from_slice(plaintext);
            Ok(wrapped)
        }

        async fn unwrap(
            &self,
            context: &KeyContext<'_>,
            ciphertext: &[u8],
        ) -> Result<Zeroizing<Vec<u8>>> {
            let binding = serde_json::to_vec(&(
                context.repository_id,
                context.slot_id,
                context.key_reference,
                context.dek_version,
                context.purpose,
            ))?;
            let expected = Sha256::digest(binding);
            if ciphertext.len() < expected.len() || ciphertext[..expected.len()] != expected[..] {
                bail!("external member context mismatch");
            }
            Ok(Zeroizing::new(ciphertext[expected.len()..].to_vec()))
        }
    }

    fn setup() -> (RecoveryCapsule, SigningKey, Vec<ClientAuthorization>) {
        setup_with_topology(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v2.json"
        )))
    }

    fn setup_with_topology(
        topology: &[u8],
    ) -> (RecoveryCapsule, SigningKey, Vec<ClientAuthorization>) {
        let identity = SigningKey::generate(&mut LegacyOsRng);
        let capsule = CapsuleBuilder::new(
            "repo-a",
            4,
            topology,
        )
            .broker_identity_public_key(identity.verifying_key().as_bytes())
            .create_offline_threshold(
                "operators",
                2,
                &[
                    ("alice", MemberCredential::Passphrase(b"alice passphrase")),
                    ("bob", MemberCredential::Passphrase(b"bob passphrase")),
                    ("carol", MemberCredential::Keyfile(&[3; 32])),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap();
        let authorizations = vec![ClientAuthorization {
            component: "vaulticdb".to_owned(),
            minimum_version: 20,
            maximum_version: 21,
            release_identity: "release-key-a".to_owned(),
            release_public_key: release_signing_key().verifying_key().to_bytes(),
            peer_uid: 42,
            capabilities: BTreeSet::from([
                Capability::MetadataDek,
                Capability::TopologyRead,
                Capability::CredentialLease,
                Capability::PolicyMutation,
            ]),
            credential_refs: BTreeSet::from(["cred:drive".to_owned()]),
            read_only: false,
        }];
        (capsule, identity, authorizations)
    }

    fn unlock_broker(mut broker: KeyBroker, capsule: &RecoveryCapsule) -> KeyBroker {
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        for (member, passphrase) in [
            ("alice", b"alice passphrase".as_slice()),
            ("bob", b"bob passphrase".as_slice()),
        ] {
            let contribution = encrypt_offline_contribution(
                capsule,
                &session,
                "unix:/broker.sock",
                member,
                &MemberCredential::Passphrase(passphrase),
                4,
                None,
                1_001,
            )
            .unwrap();
            broker.submit_contribution(contribution, 1_002).unwrap();
        }
        broker
    }

    fn client() -> ClientIdentity {
        let mut client = ClientIdentity {
            connection_id: "connection-a".to_owned(),
            component: "vaulticdb".to_owned(),
            version: 20,
            release_identity: "release-key-a".to_owned(),
            executable_sha256: "ab".repeat(32),
            release_signature: String::new(),
            peer_uid: 42,
            executable_owned_by_root: true,
            installation_path_read_only: true,
        };
        client.release_signature = BASE64.encode(
            release_signing_key()
                .sign(&release_manifest(&client).unwrap())
                .to_bytes(),
        );
        client
    }

    fn release_signing_key() -> SigningKey {
        SigningKey::from_bytes(&[6; 32])
    }

    fn signed_client(
        signing_key: &SigningKey,
        release_identity: &str,
        version: u64,
    ) -> ClientIdentity {
        let mut client = ClientIdentity {
            connection_id: "connection-a".to_owned(),
            component: "vaulticdb".to_owned(),
            version,
            release_identity: release_identity.to_owned(),
            executable_sha256: "ab".repeat(32),
            release_signature: String::new(),
            peer_uid: 42,
            executable_owned_by_root: true,
            installation_path_read_only: true,
        };
        client.release_signature = BASE64.encode(
            signing_key
                .sign(&release_manifest(&client).unwrap())
                .to_bytes(),
        );
        client
    }

    #[test]
    fn active_session_capacity_is_bounded_and_reclaimed_after_expiry() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule, identity, authorizations, None).unwrap();
        for _ in 0..MAX_ACTIVE_SESSIONS {
            broker
                .create_session(
                    "unix:/run/vaultic/broker.sock",
                    Duration::from_secs(1),
                    1_000,
                )
                .unwrap();
        }
        assert!(broker
            .create_session(
                "unix:/run/vaultic/broker.sock",
                Duration::from_secs(1),
                1_000
            )
            .is_err());
        assert!(broker
            .create_session(
                "unix:/run/vaultic/broker.sock",
                Duration::from_secs(1),
                2_000
            )
            .is_ok());
    }

    #[test]
    fn signed_hpke_quorum_unlocks_and_leases_are_scoped() {
        let (capsule, identity, authorizations) = setup();
        let restart_identity = identity.clone();
        let restart_authorizations = authorizations.clone();
        let mut broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        assert!(broker.status(1_000).unwrap().locked);
        let session = broker
            .create_session(
                "unix:/run/vaultic/broker.sock",
                Duration::from_secs(60),
                1_000,
            )
            .unwrap();
        for (member, credential, unlocked) in [
            (
                "alice",
                MemberCredential::Passphrase(b"alice passphrase"),
                false,
            ),
            ("bob", MemberCredential::Passphrase(b"bob passphrase"), true),
        ] {
            let contribution = encrypt_offline_contribution(
                &capsule,
                &session,
                "unix:/run/vaultic/broker.sock",
                member,
                &credential,
                4,
                None,
                1_001,
            )
            .unwrap();
            assert_eq!(
                broker.submit_contribution(contribution, 1_002).unwrap(),
                unlocked
            );
        }
        assert!(!broker.status(1_003).unwrap().locked);
        let lease = broker
            .acquire_lease(
                &client(),
                Capability::MetadataDek,
                Duration::from_secs(30),
                1_004,
            )
            .unwrap();
        assert_eq!(lease.key.as_slice(), &[7; 32]);
        assert!(broker
            .acquire_lease(
                &client(),
                Capability::RepositoryMasterKey,
                Duration::from_secs(30),
                1_004,
            )
            .is_err());
        let mut forged = client();
        forged.version = 21;
        assert!(broker
            .acquire_lease(
                &forged,
                Capability::MetadataDek,
                Duration::from_secs(30),
                1_004,
            )
            .is_err());
        broker.disconnect("connection-a");
        assert_eq!(broker.status(1_005).unwrap().active_leases, 0);
        let mut reconnected = client();
        reconnected.connection_id = "connection-b".to_owned();
        let reacquired = broker
            .acquire_lease(
                &reconnected,
                Capability::MetadataDek,
                Duration::from_secs(30),
                1_005,
            )
            .unwrap();
        assert_eq!(reacquired.key.as_slice(), &[7; 32]);

        let mut restarted =
            KeyBroker::new(capsule, restart_identity, restart_authorizations, None).unwrap();
        assert!(restarted.status(1_006).unwrap().locked);
        assert!(restarted
            .release_lease(&reacquired.lease_id, "connection-b")
            .is_err());
        assert!(restarted
            .acquire_lease(
                &reconnected,
                Capability::MetadataDek,
                Duration::from_secs(30),
                1_006,
            )
            .is_err());
        broker.lock();
        assert!(broker.status(1_006).unwrap().locked);
    }

    #[tokio::test]
    async fn topology_and_credentials_are_released_without_broad_secret_disclosure() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        for (member, passphrase) in [
            ("alice", b"alice passphrase".as_slice()),
            ("bob", b"bob passphrase".as_slice()),
        ] {
            let contribution = encrypt_offline_contribution(
                &capsule,
                &session,
                "unix:/broker.sock",
                member,
                &MemberCredential::Passphrase(passphrase),
                4,
                None,
                1_001,
            )
            .unwrap();
            broker.submit_contribution(contribution, 1_002).unwrap();
        }

        let topology = broker
            .acquire_lease(
                &client(),
                Capability::TopologyRead,
                Duration::from_secs(30),
                1_003,
            )
            .unwrap();
        let topology = String::from_utf8(topology.key.to_vec()).unwrap();
        assert!(topology.contains("cred:drive"));
        assert!(!topology.contains("refresh-secret"));

        let credential = broker
            .acquire_credential_lease(
                &client(),
                "cred:drive",
                Duration::from_secs(30),
                1_003,
            )
            .unwrap();
        assert!(String::from_utf8(credential.key.to_vec())
            .unwrap()
            .contains("refresh-secret"));
        assert!(broker
            .acquire_credential_lease(
                &client(),
                "cred:archive",
                Duration::from_secs(30),
                1_003,
            )
            .is_err());

        broker.authorizations[0].read_only = true;
        assert!(broker
            .acquire_credential_lease(
                &client(),
                "cred:drive",
                Duration::from_secs(30),
                1_003,
            )
            .is_err());
        let read_credential = broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:drive",
                "storage-read",
                Duration::from_secs(30),
                1_003,
            )
            .await
            .unwrap();
        assert!(String::from_utf8(read_credential.key.to_vec())
            .unwrap()
            .contains("refresh-secret"));
        assert!(broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:drive",
                "storage-append",
                Duration::from_secs(30),
                1_003,
            )
            .await
            .is_err());
        for tier in ["storage-maintain", "storage-lock"] {
            assert!(broker
                .acquire_storage_credential_lease(
                    &client(),
                    "pack:drive",
                    tier,
                    Duration::from_secs(30),
                    1_003,
                )
                .await
                .is_err());
        }
        broker.lock();
        assert_eq!(broker.status(1_004).unwrap().active_leases, 0);
    }

    #[tokio::test]
    async fn sts_recovery_retires_checked_out_static_fallback_generation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for status in [503, 200, 503] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 16 * 1024];
                let _ = stream.read(&mut request).await.unwrap();
                let (reason, body) = if status == 200 {
                    (
                        "OK",
                        r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>temporary-access</AccessKeyId><SecretAccessKey>temporary-secret</SecretAccessKey><SessionToken>temporary-token</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>request-a</RequestId></ResponseMetadata></AssumeRoleResponse>"#,
                    )
                } else {
                    (
                        "Service Unavailable",
                        r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Receiver</Type><Code>ServiceUnavailable</Code><Message>unavailable</Message></Error><RequestId>request-a</RequestId></ErrorResponse>"#,
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let mut topology = TopologyDocument::decode(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v2.json"
        )))
        .unwrap();
        topology.credentials.insert(
            "cred:issuer".to_owned(),
            topology.credentials["cred:archive"].clone(),
        );
        let backend = &mut topology.pack_backends[0];
        backend.endpoint.insert(
            "url".to_owned(),
            serde_json::Value::String(format!("http://{address}")),
        );
        backend.credential_policy = Some(CredentialPolicy {
            sts: Some(S3StsPolicy {
                issuer_ref: "cred:issuer".to_owned(),
                endpoint: format!("http://{address}"),
                region: "us-east-1".to_owned(),
                session_name: "vaultic-test".to_owned(),
                external_id: None,
                roles: crate::topology::CredentialBindings {
                    storage_read: Some("arn:aws:iam::123456789012:role/read".to_owned()),
                    storage_append: None,
                    storage_maintain: None,
                    storage_lock: None,
                },
                fallback: StsFallback::StaticOnUnavailable,
            }),
            azure_user_delegation: None,
            gcp_downscope: None,
            r#static: Some(StaticCredentialPolicy {
                generation: 1,
                revoked_generations: Vec::new(),
                bindings: crate::topology::CredentialBindings {
                    storage_read: Some("cred:archive".to_owned()),
                    storage_append: None,
                    storage_maintain: None,
                    storage_lock: None,
                },
            }),
        });
        let encoded = topology.canonical_json().unwrap();
        let (capsule, identity, mut authorizations) = setup_with_topology(&encoded);
        authorizations[0]
            .credential_refs
            .insert("cred:issuer".to_owned());
        let broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let mut broker = unlock_broker(broker, &capsule);

        assert!(broker
            .acquire_credential_lease(
                &client(),
                "cred:issuer",
                Duration::from_secs(30),
                1_003,
            )
            .unwrap_err()
            .to_string()
            .contains("broker-only"));

        let fallback = broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:archive",
                "storage-read",
                Duration::from_secs(30),
                1_003,
            )
            .await
            .unwrap();
        assert_eq!(fallback.credential_source, Some("static-fallback"));
        assert_eq!(fallback.static_generation, Some(1));
        let fallback_status = broker.status(1_003).unwrap();
        assert!(!fallback_status.compliant);
        assert!(fallback_status.findings.iter().any(|finding| {
            finding.contains("static-fallback-active-provider-authority")
        }));

        let issued = broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:archive",
                "storage-read",
                Duration::from_secs(30),
                1_004,
            )
            .await
            .unwrap();
        assert_eq!(issued.credential_source, Some("sts"));
        assert!(String::from_utf8(issued.key.to_vec())
            .unwrap()
            .contains("temporary-token"));

        assert!(broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:archive",
                "storage-read",
                Duration::from_secs(30),
                1_005,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("retired"));
        let status = broker.status(1_006).unwrap();
        assert!(!status.compliant);
        assert!(status.findings.iter().any(|finding| {
            finding.contains("static-fallback-provider-revocation-required")
        }));
    }

    #[tokio::test]
    async fn azure_recovery_retires_fallback_without_releasing_entra_issuer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for (index, status) in [200, 503, 200, 200, 200, 503].into_iter().enumerate() {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 16 * 1024];
                let read = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                let token_request = index % 2 == 0;
                let (reason, content_type, body) = if token_request {
                    assert!(request.contains("/tenant-a/oauth2/v2.0/token"));
                    ("OK", "application/json", r#"{"access_token":"entra-token"}"#.to_owned())
                } else if status == 200 {
                    assert!(request
                        .to_ascii_lowercase()
                        .contains("authorization: bearer entra-token"));
                    (
                        "OK",
                        "application/xml",
                        format!(
                            "<UserDelegationKey><SignedOid>issuer-oid</SignedOid><SignedTid>tenant-a</SignedTid><SignedStart>2020-01-01T00:00:00Z</SignedStart><SignedExpiry>2099-01-01T00:00:00Z</SignedExpiry><SignedService>b</SignedService><SignedVersion>2026-04-06</SignedVersion><Value>{}</Value></UserDelegationKey>",
                            BASE64.encode(b"delegation-key")
                        ),
                    )
                } else {
                    ("Service Unavailable", "application/xml", "<Error/>".to_owned())
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let mut topology = TopologyDocument::decode(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v2.json"
        )))
        .unwrap();
        topology.credentials.insert(
            "cred:archive".to_owned(),
            Credential {
                kind: CredentialKind::AzureSas,
                access_key_id: None,
                secret_access_key: None,
                session_token: None,
                expires_at: None,
                account_name: Some("account-a".to_owned()),
                account_key: None,
                sas_token: Some("sp=rl&sig=static".to_owned()),
                service_account_json: None,
                access_token: None,
                subject: None,
                tenant_id: None,
                client_id: None,
                client_secret: None,
                refresh_token: None,
                scopes: Vec::new(),
                token_uri: None,
                issued_at: None,
                rotation_due: None,
            },
        );
        topology.credentials.insert(
            "cred:azure-issuer".to_owned(),
            Credential {
                kind: CredentialKind::AzureEntraClientSecret,
                access_key_id: None,
                secret_access_key: None,
                session_token: None,
                expires_at: None,
                account_name: None,
                account_key: None,
                sas_token: None,
                service_account_json: None,
                access_token: None,
                subject: None,
                tenant_id: Some("tenant-a".to_owned()),
                client_id: Some("client-a".to_owned()),
                client_secret: Some("issuer-secret".to_owned()),
                refresh_token: None,
                scopes: vec!["https://storage.azure.com/.default".to_owned()],
                token_uri: Some(format!(
                    "http://{address}/tenant-a/oauth2/v2.0/token"
                )),
                issued_at: None,
                rotation_due: None,
            },
        );
        let backend = &mut topology.pack_backends[0];
        backend.provider = Provider::Azure;
        backend.endpoint = BTreeMap::from([
            ("url".to_owned(), serde_json::Value::String(format!("http://{address}"))),
            ("account".to_owned(), serde_json::Value::String("account-a".to_owned())),
            ("container".to_owned(), serde_json::Value::String("repo-a".to_owned())),
            ("prefix".to_owned(), serde_json::Value::String(String::new())),
        ]);
        backend.credential_policy = Some(CredentialPolicy {
            sts: None,
            azure_user_delegation: Some(AzureUserDelegationPolicy {
                issuer_ref: "cred:azure-issuer".to_owned(),
                service_version: "2026-04-06".to_owned(),
                signed_ip: None,
                hierarchical_namespace: false,
                tiers: vec![StorageCredentialTier::Read],
                fallback: StsFallback::StaticOnUnavailable,
            }),
            gcp_downscope: None,
            r#static: Some(StaticCredentialPolicy {
                generation: 1,
                revoked_generations: Vec::new(),
                bindings: crate::topology::CredentialBindings {
                    storage_read: Some("cred:archive".to_owned()),
                    storage_append: None,
                    storage_maintain: None,
                    storage_lock: None,
                },
            }),
        });
        let encoded = topology.canonical_json().unwrap();
        let (capsule, identity, mut authorizations) = setup_with_topology(&encoded);
        authorizations[0]
            .credential_refs
            .insert("cred:azure-issuer".to_owned());
        let broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let mut broker = unlock_broker(broker, &capsule);

        assert!(broker
            .acquire_credential_lease(
                &client(),
                "cred:azure-issuer",
                Duration::from_secs(30),
                1_003,
            )
            .unwrap_err()
            .to_string()
            .contains("broker-only"));
        let fallback = broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:archive",
                "storage-read",
                Duration::from_secs(30),
                1_003,
            )
            .await
            .unwrap();
        assert_eq!(fallback.credential_source, Some("static-fallback"));
        assert_eq!(fallback.static_generation, Some(1));

        let issued = broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:archive",
                "storage-read",
                Duration::from_secs(30),
                1_004,
            )
            .await
            .unwrap();
        assert_eq!(issued.credential_source, Some("azure-user-delegation"));
        assert!(issued.provider_expires_at.is_some());
        let credential: Credential = serde_json::from_slice(issued.key.as_slice()).unwrap();
        assert_eq!(credential.kind, CredentialKind::AzureSas);
        assert!(credential.sas_token.is_some());
        assert!(credential.client_secret.is_none());

        assert!(broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:archive",
                "storage-read",
                Duration::from_secs(30),
                1_005,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("retired"));
    }

    #[tokio::test]
    async fn static_fallback_retirement_survives_broker_restarts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for status in [503, 200, 503] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 16 * 1024];
                let _ = stream.read(&mut request).await.unwrap();
                let (reason, body) = if status == 200 {
                    (
                        "OK",
                        r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>temporary-access</AccessKeyId><SecretAccessKey>temporary-secret</SecretAccessKey><SessionToken>temporary-token</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>request-a</RequestId></ResponseMetadata></AssumeRoleResponse>"#,
                    )
                } else {
                    (
                        "Service Unavailable",
                        r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Receiver</Type><Code>ServiceUnavailable</Code><Message>unavailable</Message></Error><RequestId>request-a</RequestId></ErrorResponse>"#,
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let mut topology = TopologyDocument::decode(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v2.json"
        )))
        .unwrap();
        topology.credentials.insert(
            "cred:issuer".to_owned(),
            topology.credentials["cred:archive"].clone(),
        );
        let backend = &mut topology.pack_backends[0];
        backend.endpoint.insert(
            "url".to_owned(),
            serde_json::Value::String(format!("http://{address}")),
        );
        backend.credential_policy = Some(CredentialPolicy {
            sts: Some(S3StsPolicy {
                issuer_ref: "cred:issuer".to_owned(),
                endpoint: format!("http://{address}"),
                region: "us-east-1".to_owned(),
                session_name: "vaultic-test".to_owned(),
                external_id: None,
                roles: crate::topology::CredentialBindings {
                    storage_read: Some("arn:aws:iam::123456789012:role/read".to_owned()),
                    storage_append: None,
                    storage_maintain: None,
                    storage_lock: None,
                },
                fallback: StsFallback::StaticOnUnavailable,
            }),
            azure_user_delegation: None,
            gcp_downscope: None,
            r#static: Some(StaticCredentialPolicy {
                generation: 1,
                revoked_generations: Vec::new(),
                bindings: crate::topology::CredentialBindings {
                    storage_read: Some("cred:archive".to_owned()),
                    storage_append: None,
                    storage_maintain: None,
                    storage_lock: None,
                },
            }),
        });
        let encoded = topology.canonical_json().unwrap();
        let (capsule, identity, authorizations) = setup_with_topology(&encoded);
        let state_path = std::env::temp_dir().join(format!(
            "vaultic-fallback-state-{}-{}.json",
            std::process::id(),
            rand::random::<u64>()
        ));

        let new_broker = || {
            KeyBroker::new_with_fallback_state(
                capsule.clone(),
                identity.clone(),
                authorizations.clone(),
                None,
                state_path.clone(),
                false,
            )
            .unwrap()
        };
        let mut broker = unlock_broker(new_broker(), &capsule);
        assert_eq!(
            broker
                .acquire_storage_credential_lease(
                    &client(),
                    "pack:archive",
                    "storage-read",
                    Duration::from_secs(30),
                    1_003,
                )
                .await
                .unwrap()
                .credential_source,
            Some("static-fallback")
        );

        let mut broker = unlock_broker(new_broker(), &capsule);
        assert_eq!(
            broker
                .acquire_storage_credential_lease(
                    &client(),
                    "pack:archive",
                    "storage-read",
                    Duration::from_secs(30),
                    1_004,
                )
                .await
                .unwrap()
                .credential_source,
            Some("sts")
        );

        let mut broker = unlock_broker(new_broker(), &capsule);
        assert!(broker
            .acquire_storage_credential_lease(
                &client(),
                "pack:archive",
                "storage-read",
                Duration::from_secs(30),
                1_005,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("retired"));
        let _ = std::fs::remove_file(state_path);
    }

    #[test]
    fn release_key_rotation_preserves_strict_client_authorization() {
        let (capsule, identity, _) = setup();
        let old_key = SigningKey::from_bytes(&[6; 32]);
        let new_key = SigningKey::from_bytes(&[9; 32]);
        let authorizations = vec![
            ClientAuthorization {
                component: "vaulticdb".to_owned(),
                minimum_version: 20,
                maximum_version: 21,
                release_identity: "release-key-a".to_owned(),
                release_public_key: old_key.verifying_key().to_bytes(),
                peer_uid: 42,
                capabilities: BTreeSet::from([Capability::MetadataDek]),
                credential_refs: BTreeSet::new(),
                read_only: false,
            },
            ClientAuthorization {
                component: "vaulticdb".to_owned(),
                minimum_version: 21,
                maximum_version: 22,
                release_identity: "release-key-b".to_owned(),
                release_public_key: new_key.verifying_key().to_bytes(),
                peer_uid: 42,
                capabilities: BTreeSet::from([Capability::MetadataDek]),
                credential_refs: BTreeSet::new(),
                read_only: false,
            },
        ];
        let broker = KeyBroker::new(capsule, identity, authorizations, None).unwrap();

        let old_release = signed_client(&old_key, "release-key-a", 21);
        let new_release = signed_client(&new_key, "release-key-b", 21);
        assert!(broker
            .authorize(&old_release, Capability::MetadataDek)
            .is_ok());
        assert!(broker
            .authorize(&new_release, Capability::MetadataDek)
            .is_ok());

        let rejected = [
            signed_client(&old_key, "release-key-a", 19),
            signed_client(&new_key, "release-key-b", 20),
            {
                let mut value = signed_client(&old_key, "release-key-a", 21);
                value.peer_uid = 7;
                value
            },
            {
                let mut value = signed_client(&old_key, "release-key-a", 21);
                value.component = "vaultic".to_owned();
                value
            },
            {
                let mut value = signed_client(&old_key, "release-key-a", 21);
                value.installation_path_read_only = false;
                value
            },
            {
                let mut value = signed_client(&old_key, "release-key-a", 21);
                value.executable_owned_by_root = false;
                value
            },
            {
                let mut value = signed_client(&old_key, "release-key-a", 21);
                value.executable_sha256 = "cd".repeat(32);
                value
            },
            signed_client(&new_key, "release-key-a", 21),
        ];
        for client in rejected {
            assert!(broker.authorize(&client, Capability::MetadataDek).is_err());
        }
        assert!(broker
            .authorize(&old_release, Capability::RepositoryMasterKey)
            .is_err());
    }

    #[test]
    fn policy_mutation_preserves_keys_refreshes_shares_and_relocks() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        for (member, passphrase) in [
            ("alice", b"alice passphrase".as_slice()),
            ("bob", b"bob passphrase".as_slice()),
        ] {
            let contribution = encrypt_offline_contribution(
                &capsule,
                &session,
                "unix:/broker.sock",
                member,
                &MemberCredential::Passphrase(passphrase),
                4,
                None,
                1_001,
            )
            .unwrap();
            broker.submit_contribution(contribution, 1_002).unwrap();
        }
        broker
            .acquire_lease(
                &client(),
                Capability::MetadataDek,
                Duration::from_secs(30),
                1_003,
            )
            .unwrap();

        let policy = UnlockPolicy::Threshold {
            group_id: "new-operators".to_owned(),
            required: 2,
            members: vec!["dana".into(), "erin".into(), "frank".into()],
        };
        let protections = [
            ("dana", MemberCredential::Passphrase(b"dana passphrase")),
            ("erin", MemberCredential::Passphrase(b"erin passphrase")),
            ("frank", MemberCredential::Keyfile(&[8; 32])),
        ];
        let (candidate, digest) = broker
            .prepare_offline_policy_mutation(&client(), policy, &protections, false, 1_004)
            .unwrap();

        assert_eq!(candidate.header.generation, 5);
        assert_eq!(
            candidate.header.metadata_dek_version,
            capsule.header.metadata_dek_version
        );
        assert_eq!(
            candidate.header.repository_key_version,
            capsule.header.repository_key_version
        );
        assert_ne!(candidate.header.logical_id, capsule.header.logical_id);
        assert_ne!(
            candidate.metadata_dek.ciphertext,
            capsule.metadata_dek.ciphertext
        );
        assert_ne!(candidate.members, capsule.members);
        let recovered = candidate
            .recover_offline(&BTreeMap::from([
                (
                    "dana".to_owned(),
                    MemberCredential::Passphrase(b"dana passphrase"),
                ),
                (
                    "erin".to_owned(),
                    MemberCredential::Passphrase(b"erin passphrase"),
                ),
            ]))
            .unwrap();
        assert_eq!(recovered.metadata_dek.as_slice(), &[7; 32]);
        assert_eq!(
            recovered.repository_master_key.as_slice(),
            b"repository-master-key"
        );
        assert_eq!(
            recovered.sealed_topology.as_slice(),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../testdata/topology-v2.json"
            ))
            .as_slice()
        );
        assert_eq!(broker.status(1_005).unwrap().capsule_generation, 4);
        assert_eq!(broker.status(1_005).unwrap().active_leases, 0);
        assert_eq!(
            broker.status(1_005).unwrap().pending_capsule_generation,
            Some(5)
        );
        assert_eq!(
            broker
                .status(1_005)
                .unwrap()
                .pending_capsule_sha256
                .as_deref(),
            Some(digest.as_str())
        );
        assert!(broker
            .acquire_lease(
                &client(),
                Capability::MetadataDek,
                Duration::from_secs(30),
                1_005,
            )
            .is_err());
        assert!(broker
            .activate_policy_mutation(&client(), "wrong-digest")
            .is_err());
        broker.activate_policy_mutation(&client(), &digest).unwrap();
        let status = broker.status(1_006).unwrap();
        assert!(status.locked);
        assert_eq!(status.capsule_generation, 5);
        assert_eq!(status.active_leases, 0);
        assert!(!status.policy_mutation_pending);
    }

    #[tokio::test]
    async fn topology_mutation_rewraps_validated_secret_as_next_generation() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        for (member, passphrase) in [
            ("alice", b"alice passphrase".as_slice()),
            ("bob", b"bob passphrase".as_slice()),
        ] {
            let contribution = encrypt_offline_contribution(
                &capsule,
                &session,
                "unix:/broker.sock",
                member,
                &MemberCredential::Passphrase(passphrase),
                4,
                None,
                1_001,
            )
            .unwrap();
            broker.submit_contribution(contribution, 1_002).unwrap();
        }
        let mut credential: crate::topology::Credential = serde_json::from_value(serde_json::json!({
            "kind": "oauth2-refresh-token",
            "client_id": "client-id",
            "client_secret": "rotated-client-secret",
            "refresh_token": "rotated-refresh-secret",
            "scopes": ["https://www.googleapis.com/auth/drive.file"],
            "token_uri": "https://oauth2.googleapis.com/token"
        }))
        .unwrap();
        let protections = [
            ("alice", MemberProtection::Offline(MemberCredential::Passphrase(b"alice passphrase"))),
            ("bob", MemberProtection::Offline(MemberCredential::Passphrase(b"bob passphrase"))),
            ("carol", MemberProtection::Offline(MemberCredential::Keyfile(&[3; 32]))),
        ];
        let (candidate, _) = broker
            .prepare_topology_mutation(
                &client(),
                crate::topology::TopologyMutation::RotateCredential {
                    reference: "cred:drive".to_owned(),
                    credential: credential.clone(),
                },
                &protections,
                1_003,
            )
            .await
            .unwrap();
        credential.refresh_token.take();
        let recovered = candidate
            .recover_offline(&BTreeMap::from([
                ("alice".to_owned(), MemberCredential::Passphrase(b"alice passphrase")),
                ("bob".to_owned(), MemberCredential::Passphrase(b"bob passphrase")),
            ]))
            .unwrap();
        let topology = crate::topology::TopologyDocument::decode(
            recovered.sealed_topology.as_slice(),
        )
        .unwrap();
        assert_eq!(candidate.header.generation, 5);
        assert_eq!(topology.topology_generation, 5);
        assert_eq!(
            topology.credentials["cred:drive"].refresh_token.as_deref(),
            Some("rotated-refresh-secret")
        );
    }

    #[test]
    fn identity_recovery_requires_acknowledgement_and_repin_before_leases() {
        let (capsule, _, authorizations) = setup();
        let replacement_identity = SigningKey::from_bytes(&[11; 32]);
        let replacement_public_key = replacement_identity.verifying_key().to_bytes();
        let mut broker = KeyBroker::new_identity_recovery(
            capsule.clone(),
            replacement_identity,
            authorizations,
            None,
        )
        .unwrap();
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        assert!(session.transcript.identity_recovery);
        assert!(encrypt_offline_contribution(
            &capsule,
            &session,
            "unix:/broker.sock",
            "alice",
            &MemberCredential::Passphrase(b"alice passphrase"),
            4,
            None,
            1_001,
        )
        .is_err());
        for (member, passphrase) in [
            ("alice", b"alice passphrase".as_slice()),
            ("bob", b"bob passphrase".as_slice()),
        ] {
            let contribution = encrypt_offline_contribution_unverified(
                &capsule,
                &session,
                "unix:/broker.sock",
                member,
                &MemberCredential::Passphrase(passphrase),
                4,
                None,
                1_001,
            )
            .unwrap();
            broker.submit_contribution(contribution, 1_002).unwrap();
        }
        assert!(broker.status(1_003).unwrap().identity_recovery);
        assert!(broker
            .acquire_lease(
                &client(),
                Capability::MetadataDek,
                Duration::from_secs(30),
                1_003,
            )
            .is_err());

        let policy = capsule.policy.clone();
        let protections = [
            (
                "alice",
                MemberCredential::Passphrase(b"new alice passphrase"),
            ),
            ("bob", MemberCredential::Passphrase(b"new bob passphrase")),
            ("carol", MemberCredential::Keyfile(&[12; 32])),
        ];
        let (candidate, digest) = broker
            .prepare_offline_policy_mutation(&client(), policy, &protections, false, 1_004)
            .unwrap();
        assert_eq!(
            BASE64
                .decode(&candidate.header.broker_identity_public_key)
                .unwrap(),
            replacement_public_key
        );
        broker.activate_policy_mutation(&client(), &digest).unwrap();
        let status = broker.status(1_005).unwrap();
        assert!(status.locked);
        assert!(!status.identity_recovery);
        assert_eq!(status.capsule_generation, 5);
    }

    #[tokio::test]
    async fn mixed_cloud_policy_mutation_preserves_keys_and_context_binding() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        for (member, passphrase) in [
            ("alice", b"alice passphrase".as_slice()),
            ("bob", b"bob passphrase".as_slice()),
        ] {
            let contribution = encrypt_offline_contribution(
                &capsule,
                &session,
                "unix:/broker.sock",
                member,
                &MemberCredential::Passphrase(passphrase),
                4,
                None,
                1_001,
            )
            .unwrap();
            broker.submit_contribution(contribution, 1_002).unwrap();
        }
        let provider = ContextProvider;
        let policy = UnlockPolicy::Threshold {
            group_id: "operators".to_owned(),
            required: 2,
            members: vec!["alice".into(), "cloud".into()],
        };
        let protections = [
            (
                "alice",
                MemberProtection::Offline(MemberCredential::Passphrase(b"new alice passphrase")),
            ),
            (
                "cloud",
                MemberProtection::External(ExternalMemberProtection {
                    provider: MemberProvider::AzureKeyVault,
                    key_reference: "https://example.vault.azure.net/keys/cloud/version",
                    principal: Some(PrincipalBinding {
                        authority: "entra".to_owned(),
                        tenant_account_or_project: "tenant-a".to_owned(),
                        immutable_principal_id: "object-cloud".to_owned(),
                    }),
                    hardware: None,
                    key_provider: &provider,
                }),
            ),
        ];
        let (candidate, _) = broker
            .prepare_policy_mutation(&client(), policy, &protections, false, 1_003)
            .await
            .unwrap();
        let offline = candidate
            .unwrap_offline_member(
                "alice",
                &MemberCredential::Passphrase(b"new alice passphrase"),
            )
            .unwrap();
        let cloud = candidate
            .unwrap_external_member("cloud", &provider)
            .await
            .unwrap();
        let recovered = candidate.recover_from_shares(&[offline, cloud]).unwrap();
        assert_eq!(recovered.metadata_dek.as_slice(), &[7; 32]);
        assert_eq!(
            recovered.repository_master_key.as_slice(),
            b"repository-master-key"
        );
        let mut tampered = candidate;
        tampered.members[1].key_reference.push_str("-other");
        assert!(tampered
            .unwrap_external_member("cloud", &provider)
            .await
            .is_err());
    }

    #[test]
    fn session_tampering_replay_duplicates_and_rollback_fail_closed() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 2_000)
            .unwrap();
        let mut tampered = session.clone();
        tampered.transcript.endpoint_binding = "unix:/fake.sock".to_owned();
        assert!(verify_session(&capsule, &tampered, "unix:/fake.sock", 2_001).is_err());

        let rollback = encrypt_offline_contribution(
            &capsule,
            &session,
            "unix:/broker.sock",
            "alice",
            &MemberCredential::Passphrase(b"alice passphrase"),
            5,
            None,
            2_001,
        )
        .unwrap();
        let error = broker.submit_contribution(rollback, 2_002).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ContributionRejection>(),
            Some(ContributionRejection::Rollback {
                last_seen_generation: 5,
                current_generation: 4,
            })
        ));

        let contribution = encrypt_offline_contribution(
            &capsule,
            &session,
            "unix:/broker.sock",
            "alice",
            &MemberCredential::Passphrase(b"alice passphrase"),
            4,
            Some("principal-a".to_owned()),
            2_001,
        )
        .unwrap();
        let mut malformed = contribution.clone();
        malformed.ciphertext = "not-base64".to_owned();
        let error = broker.submit_contribution(malformed, 2_002).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ContributionRejection>(),
            Some(ContributionRejection::PayloadInvalid)
        ));
        let malformed_share = encrypt_contribution_payload(
            &session,
            &ContributionPayload {
                member_id: "alice".into(),
                share_index: capsule.members[0].share_index,
                share: vec![0],
                last_seen_generation: 4,
                principal_id: Some("principal-a".to_owned()),
                unverified_session_acknowledged: false,
            },
        )
        .unwrap();
        let error = broker
            .submit_contribution(malformed_share, 2_002)
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ContributionRejection>(),
            Some(ContributionRejection::PayloadInvalid)
        ));
        let invalid_principal = encrypt_offline_contribution(
            &capsule,
            &session,
            "unix:/broker.sock",
            "alice",
            &MemberCredential::Passphrase(b"alice passphrase"),
            4,
            Some(String::new()),
            2_001,
        )
        .unwrap();
        assert!(broker
            .submit_contribution(invalid_principal, 2_002)
            .is_err());
        assert!(!broker
            .submit_contribution(contribution.clone(), 2_002)
            .unwrap());
        assert!(broker.submit_contribution(contribution, 2_003).is_err());

        let poisoned_session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 3_000)
            .unwrap();
        let mut wrong_share = capsule
            .unwrap_offline_member("alice", &MemberCredential::Passphrase(b"alice passphrase"))
            .unwrap()
            .plaintext
            .to_vec();
        *wrong_share.last_mut().unwrap() ^= 1;
        let poisoned = encrypt_contribution_payload(
            &poisoned_session,
            &ContributionPayload {
                member_id: "alice".into(),
                share_index: capsule
                    .members
                    .iter()
                    .find(|member| member.member_id == "alice")
                    .unwrap()
                    .share_index,
                share: wrong_share,
                last_seen_generation: 4,
                principal_id: None,
                unverified_session_acknowledged: false,
            },
        )
        .unwrap();
        assert!(!broker.submit_contribution(poisoned, 3_001).unwrap());
        let bob = encrypt_offline_contribution(
            &capsule,
            &poisoned_session,
            "unix:/broker.sock",
            "bob",
            &MemberCredential::Passphrase(b"bob passphrase"),
            4,
            None,
            3_001,
        )
        .unwrap();
        let error = broker.submit_contribution(bob.clone(), 3_002).unwrap_err();
        assert!(error.to_string().contains("session closed"));
        let error = broker.submit_contribution(bob, 3_003).unwrap_err();
        assert!(error
            .to_string()
            .contains("unknown or expired unlock session"));
    }

    #[test]
    fn expiry_locks_epoch_and_revokes_leases() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(
            capsule.clone(),
            identity,
            authorizations,
            Some(Duration::from_secs(10)),
        )
        .unwrap();
        let session = broker
            .create_session("unix:/broker.sock", Duration::from_secs(60), 10_000)
            .unwrap();
        for (member, passphrase) in [
            ("alice", b"alice passphrase".as_slice()),
            ("bob", b"bob passphrase".as_slice()),
        ] {
            let contribution = encrypt_offline_contribution(
                &capsule,
                &session,
                "unix:/broker.sock",
                member,
                &MemberCredential::Passphrase(passphrase),
                4,
                None,
                10_001,
            )
            .unwrap();
            broker.submit_contribution(contribution, 10_002).unwrap();
        }
        broker
            .acquire_lease(
                &client(),
                Capability::MetadataDek,
                Duration::from_secs(30),
                10_003,
            )
            .unwrap();
        let status = broker.status(20_003).unwrap();
        assert!(status.locked);
        assert_eq!(status.active_leases, 0);
    }
}
