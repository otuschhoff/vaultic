#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::{
        envelope::providers::{KeyContext, KeyProvider},
        recovery_capsule::{
            CapsuleBuilder, ExternalMemberProtection, MemberProvider, PrincipalBinding,
        },
    };
    use async_trait::async_trait;

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
        let identity = SigningKey::generate(&mut LegacyOsRng);
        let capsule = CapsuleBuilder::new("repo-a", 4)
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
            capabilities: BTreeSet::from([Capability::MetadataDek, Capability::PolicyMutation]),
        }];
        (capsule, identity, authorizations)
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
        assert!(broker.status(1_000).locked);
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
        assert!(!broker.status(1_003).locked);
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
        assert_eq!(broker.status(1_005).active_leases, 0);
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
        assert!(restarted.status(1_006).locked);
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
        assert!(broker.status(1_006).locked);
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
            },
            ClientAuthorization {
                component: "vaulticdb".to_owned(),
                minimum_version: 21,
                maximum_version: 22,
                release_identity: "release-key-b".to_owned(),
                release_public_key: new_key.verifying_key().to_bytes(),
                peer_uid: 42,
                capabilities: BTreeSet::from([Capability::MetadataDek]),
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
            members: vec!["dana".to_owned(), "erin".to_owned(), "frank".to_owned()],
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
        assert_eq!(broker.status(1_005).capsule_generation, 4);
        assert_eq!(broker.status(1_005).active_leases, 0);
        assert_eq!(broker.status(1_005).pending_capsule_generation, Some(5));
        assert_eq!(
            broker.status(1_005).pending_capsule_sha256.as_deref(),
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
        let status = broker.status(1_006);
        assert!(status.locked);
        assert_eq!(status.capsule_generation, 5);
        assert_eq!(status.active_leases, 0);
        assert!(!status.policy_mutation_pending);
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
        assert!(broker.status(1_003).identity_recovery);
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
        let status = broker.status(1_005);
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
            members: vec!["alice".to_owned(), "cloud".to_owned()],
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
                member_id: "alice".to_owned(),
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
                member_id: "alice".to_owned(),
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
        let status = broker.status(20_003);
        assert!(status.locked);
        assert_eq!(status.active_leases, 0);
    }
}
