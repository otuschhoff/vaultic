#[cfg(test)]
mod tests {
    //! Recovery capsule policy and reconstruction tests.

    use super::*;
    use async_trait::async_trait;

    struct ContextProvider {
        name: &'static str,
    }

    #[async_trait]
    impl KeyProvider for ContextProvider {
        fn name(&self) -> &'static str {
            self.name
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

    fn capsule(required: u8) -> RecoveryCapsule {
        CapsuleBuilder::new("repo-a", 7)
            .broker_identity_public_key(&[9; 32])
            .create_offline_threshold(
                "operators",
                required,
                &[
                    ("alice", MemberCredential::Passphrase(b"alice passphrase")),
                    ("bob", MemberCredential::Passphrase(b"bob passphrase")),
                    ("carol", MemberCredential::Keyfile(&[3; 32])),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap()
    }

    #[test]
    fn every_threshold_subset_recovers_and_fewer_fails() {
        let capsule = capsule(2);
        for credentials in [
            BTreeMap::from([
                (
                    "alice".to_owned(),
                    MemberCredential::Passphrase(b"alice passphrase"),
                ),
                (
                    "bob".to_owned(),
                    MemberCredential::Passphrase(b"bob passphrase"),
                ),
            ]),
            BTreeMap::from([
                (
                    "alice".to_owned(),
                    MemberCredential::Passphrase(b"alice passphrase"),
                ),
                ("carol".to_owned(), MemberCredential::Keyfile(&[3; 32])),
            ]),
            BTreeMap::from([
                (
                    "bob".to_owned(),
                    MemberCredential::Passphrase(b"bob passphrase"),
                ),
                ("carol".to_owned(), MemberCredential::Keyfile(&[3; 32])),
            ]),
        ] {
            let recovered = capsule.recover_offline(&credentials).unwrap();
            assert_eq!(recovered.metadata_dek.as_slice(), &[7; 32]);
            assert_eq!(
                recovered.repository_master_key.as_slice(),
                b"repository-master-key"
            );
        }
        assert!(capsule
            .recover_offline(&BTreeMap::from([(
                "alice".to_owned(),
                MemberCredential::Passphrase(b"alice passphrase"),
            )]))
            .is_err());
    }

    #[tokio::test]
    async fn external_member_wrap_is_context_bound_and_recovers_with_offline_member() {
        let azure = ContextProvider {
            name: "azure-key-vault",
        };
        let aws = ContextProvider { name: "aws-kms" };
        let policy = UnlockPolicy::Threshold {
            group_id: "operators".to_owned(),
            required: 2,
            members: vec!["alice".into(), "bob".into()],
        };
        let capsule = CapsuleBuilder::new("repo-a", 8)
            .broker_identity_public_key(&[9; 32])
            .create_policy(
                policy,
                &[
                    (
                        "alice",
                        MemberProtection::External(ExternalMemberProtection {
                            provider: MemberProvider::AzureKeyVault,
                            key_reference: "https://example.vault.azure.net/keys/alice/version",
                            principal: Some(PrincipalBinding {
                                authority: "entra".to_owned(),
                                tenant_account_or_project: "tenant-a".to_owned(),
                                immutable_principal_id: "object-alice".to_owned(),
                            }),
                            hardware: None,
                            key_provider: &azure,
                        }),
                    ),
                    (
                        "bob",
                        MemberProtection::Offline(MemberCredential::Keyfile(&[4; 32])),
                    ),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .await
            .unwrap();

        let external = capsule
            .unwrap_external_member("alice", &azure)
            .await
            .unwrap();
        assert!(capsule.unwrap_external_member("alice", &aws).await.is_err());
        let mut substituted_key = capsule.clone();
        substituted_key.members[0].key_reference =
            "https://example.vault.azure.net/keys/other/version".to_owned();
        assert!(substituted_key
            .unwrap_external_member("alice", &azure)
            .await
            .is_err());
        let mut substituted_principal = capsule.clone();
        substituted_principal.members[0]
            .principal
            .as_mut()
            .unwrap()
            .immutable_principal_id = "object-mallory".to_owned();
        assert!(substituted_principal
            .unwrap_external_member("alice", &azure)
            .await
            .is_err());
        let offline = capsule
            .unwrap_offline_member("bob", &MemberCredential::Keyfile(&[4; 32]))
            .unwrap();
        let recovered = capsule.recover_from_shares(&[external, offline]).unwrap();
        assert_eq!(recovered.metadata_dek.as_slice(), &[7; 32]);
        assert_eq!(
            recovered.repository_master_key.as_slice(),
            b"repository-master-key"
        );
    }

    #[tokio::test]
    async fn mixed_two_of_four_accepts_secure_enclave_yubikey_gcp_and_offline() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use p256::elliptic_curve::sec1::ToEncodedPoint;

        let yubikey = ContextProvider {
            name: "yubikey-piv",
        };
        let enclave = ContextProvider {
            name: "macos-secure-enclave",
        };
        let gcp = ContextProvider { name: "gcp-kms" };
        let enclave_public_key = p256::SecretKey::random(&mut rand08::rngs::OsRng)
            .public_key()
            .to_encoded_point(false);
        let enclave_tag = URL_SAFE_NO_PAD.encode([8u8; 32]);
        let enclave_reference = format!(
            "secure-enclave:application-tag={enclave_tag};public-key={};access-control=biometry-current-set",
            URL_SAFE_NO_PAD.encode(enclave_public_key.as_bytes())
        );
        let enclave_fingerprint =
            format!("sha256:{:x}", Sha256::digest(enclave_public_key.as_bytes()));
        let yubikey_reference = "pkcs11:module-path=/usr/lib/libykcs11.so;slot-id=1;id=9a;public-key-sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;type=rsa-key-pair";
        let capsule = CapsuleBuilder::new("repo-a", 9)
            .broker_identity_public_key(&[9; 32])
            .create_policy(
                UnlockPolicy::Threshold {
                    group_id: "operators".to_owned(),
                    required: 2,
                    members: vec![
                        "enclave".into(),
                        "gcp".into(),
                        "offline".into(),
                        "yubikey".into(),
                    ],
                },
                &[
                    (
                        "enclave",
                        MemberProtection::External(ExternalMemberProtection {
                            provider: MemberProvider::MacosSecureEnclave,
                            key_reference: &enclave_reference,
                            principal: None,
                            hardware: Some(HardwareBinding {
                                credential_id: enclave_tag.clone(),
                                public_key: enclave_fingerprint.clone(),
                                serial_number: None,
                                attestation_fingerprint: None,
                                user_presence_required: true,
                            }),
                            key_provider: &enclave,
                        }),
                    ),
                    (
                        "gcp",
                        MemberProtection::External(ExternalMemberProtection {
                            provider: MemberProvider::GcpKms,
                            key_reference: "projects/project-a/locations/global/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1",
                            principal: Some(PrincipalBinding {
                                authority: "gcp-iam".to_owned(),
                                tenant_account_or_project: "project-a".to_owned(),
                                immutable_principal_id: "user:operator@example.com".to_owned(),
                            }),
                            hardware: None,
                            key_provider: &gcp,
                        }),
                    ),
                    (
                        "offline",
                        MemberProtection::Offline(MemberCredential::Keyfile(&[4; 32])),
                    ),
                    (
                        "yubikey",
                        MemberProtection::External(ExternalMemberProtection {
                            provider: MemberProvider::YubikeyPiv,
                            key_reference: yubikey_reference,
                            principal: None,
                            hardware: Some(HardwareBinding {
                                credential_id: "piv-9a".to_owned(),
                                public_key: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                                serial_number: Some("12345678".to_owned()),
                                attestation_fingerprint: None,
                                user_presence_required: true,
                            }),
                            key_provider: &yubikey,
                        }),
                    ),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .await
            .unwrap();

        let status = capsule.effective_policy_status().unwrap();
        assert_eq!(status.minimum_custodians, 2);
        assert!(status.compliant);
        assert!(status.principal_verified);
        assert!(status.hardware_verified);
        assert!(status.custody_assumed);

        let mut substituted_binding = capsule.clone();
        substituted_binding
            .members
            .iter_mut()
            .find(|member| member.member_id == "enclave")
            .unwrap()
            .hardware
            .as_mut()
            .unwrap()
            .public_key = "sha256:substituted".to_owned();
        assert!(substituted_binding.validate().is_err());

        let enclave_share = capsule
            .unwrap_external_member("enclave", &enclave)
            .await
            .unwrap();
        let offline_share = capsule
            .unwrap_offline_member("offline", &MemberCredential::Keyfile(&[4; 32]))
            .unwrap();
        let recovered = capsule
            .recover_from_shares(&[enclave_share, offline_share])
            .unwrap();
        assert_eq!(recovered.metadata_dek.as_slice(), &[7; 32]);
        assert_eq!(
            recovered.repository_master_key.as_slice(),
            b"repository-master-key"
        );

        let mut duplicate = capsule;
        let enclave_member = duplicate
            .members
            .iter()
            .find(|member| member.member_id == "enclave")
            .unwrap()
            .clone();
        let yubikey_member = duplicate
            .members
            .iter_mut()
            .find(|member| member.member_id == "yubikey")
            .unwrap();
        yubikey_member.provider = MemberProvider::MacosSecureEnclave;
        yubikey_member.key_reference = enclave_member.key_reference;
        yubikey_member.hardware = enclave_member.hardware;
        let duplicate_status = duplicate.effective_policy_status().unwrap();
        assert!(!duplicate_status.compliant);
        assert!(duplicate_status
            .findings
            .iter()
            .any(|finding| finding.contains("duplicate hardware credential")));
        assert!(duplicate_status
            .findings
            .contains(&"duplicate hardware public key".to_owned()));
    }

    #[test]
    fn external_share_binding_has_stable_cross_language_fixture() {
        let header = CapsuleHeader {
            format: 2,
            logical_id: "unused".to_owned(),
            repository_id: "repo-a".into(),
            generation: 8,
            root_key_version: 1,
            metadata_dek_version: 1,
            repository_key_version: 1,
            algorithm: "unused".to_owned(),
            policy_hash: "policy-hash".to_owned(),
            broker_identity_public_key: "unused".to_owned(),
            policy_intent: PolicyIntent::Quorum,
        };
        let member = MemberShare {
            member_id: "alice".into(),
            group_id: "operators".to_owned(),
            share_index: 1,
            threshold: 2,
            share_count: 2,
            provider: MemberProvider::AzureKeyVault,
            key_reference: "https://example.vault.azure.net/keys/alice/version".to_owned(),
            wrapped_share: String::new(),
            nonce: None,
            argon2: None,
            principal: Some(PrincipalBinding {
                authority: "entra".to_owned(),
                tenant_account_or_project: "tenant-a".to_owned(),
                immutable_principal_id: "object-alice".to_owned(),
            }),
            hardware: None,
        };
        assert_eq!(
            external_share_purpose(&header, &member).unwrap(),
            "recovery-capsule-share:98436d46b6026a26669db00967c0c1c744f1095700a3c5b73abeddcbf8302306"
        );
    }

    #[test]
    fn both_payloads_must_authenticate() {
        let mut capsule = capsule(1);
        std::mem::swap(
            &mut capsule.metadata_dek.ciphertext,
            &mut capsule.repository_master_key.ciphertext,
        );
        let credentials = BTreeMap::from([(
            "alice".to_owned(),
            MemberCredential::Passphrase(b"alice passphrase"),
        )]);
        assert!(capsule.recover_offline(&credentials).is_err());
    }

    #[test]
    fn capsule_authentication_is_location_independent_and_repository_bound() {
        let capsule = capsule(1);
        let exported = serde_json::to_vec(&capsule).unwrap();
        let mirror: RecoveryCapsule = serde_json::from_slice(&exported).unwrap();
        mirror.validate().unwrap();

        let mut foreign = mirror;
        foreign.header.repository_id = "repo-b".into();
        assert!(foreign.validate().is_err());
    }

    #[test]
    fn corrupt_share_binding_fails_closed() {
        let mut capsule = capsule(2);
        capsule.members[0].share_index = capsule.members[1].share_index;
        assert!(capsule.validate().is_err());
    }

    #[test]
    fn composed_policy_requires_password_and_either_hardware_seat() {
        let policy = UnlockPolicy::AllOf {
            policies: vec![
                UnlockPolicy::AnyOf {
                    policies: vec![
                        UnlockPolicy::Member {
                            member_id: "yubikey-primary".into(),
                        },
                        UnlockPolicy::Member {
                            member_id: "yubikey-backup".into(),
                        },
                    ],
                },
                UnlockPolicy::Member {
                    member_id: "offline-password".into(),
                },
            ],
        };
        let capsule = CapsuleBuilder::new("repo-a", 8)
            .broker_identity_public_key(&[9; 32])
            .create_offline_policy(
                policy,
                &[
                    ("yubikey-primary", MemberCredential::Keyfile(&[1; 32])),
                    ("yubikey-backup", MemberCredential::Keyfile(&[2; 32])),
                    (
                        "offline-password",
                        MemberCredential::Passphrase(b"offline passphrase"),
                    ),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap();
        for hardware in [("yubikey-primary", [1; 32]), ("yubikey-backup", [2; 32])] {
            let credentials = BTreeMap::from([
                (
                    hardware.0.to_owned(),
                    MemberCredential::Keyfile(&hardware.1),
                ),
                (
                    "offline-password".to_owned(),
                    MemberCredential::Passphrase(b"offline passphrase"),
                ),
            ]);
            assert!(capsule.recover_offline(&credentials).is_ok());
        }
        assert!(capsule
            .recover_offline(&BTreeMap::from([(
                "offline-password".to_owned(),
                MemberCredential::Passphrase(b"offline passphrase"),
            )]))
            .is_err());
        assert!(capsule
            .recover_offline(&BTreeMap::from([(
                "yubikey-primary".to_owned(),
                MemberCredential::Keyfile(&[1; 32]),
            )]))
            .is_err());
    }

    #[test]
    fn immutable_local_publication_discovers_latest_generation() {
        let directory = std::env::temp_dir().join(format!(
            "vaultic-capsule-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let first = capsule(1);
        let first_path = publish_local(&directory, &first).unwrap();
        assert_eq!(publish_local(&directory, &first).unwrap(), first_path);
        let latest = discover_latest(&directory, "repo-a").unwrap().unwrap();
        assert_eq!(latest.1.header.generation, 7);

        let mut conflict = first;
        conflict.metadata_dek.ciphertext.push('A');
        assert!(publish_local(&directory, &conflict).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn effective_policy_labels_bootstrap_and_offline_quorum_truthfully() {
        let bootstrap = capsule(1).effective_policy_status().unwrap();
        assert_eq!(bootstrap.minimum_custodians, 1);
        assert!(!bootstrap.compliant);
        assert!(bootstrap.custody_assumed);
        assert!(bootstrap
            .findings
            .iter()
            .any(|finding| finding.contains("bootstrap")));

        let quorum = capsule(2).effective_policy_status().unwrap();
        assert_eq!(quorum.minimum_custodians, 2);
        assert!(quorum.compliant);
        assert!(quorum.custody_assumed);
        assert!(!quorum.principal_verified);
    }

    #[test]
    fn assurance_bindings_reject_mismatched_authority_and_duplicate_hardware_key() {
        let mut aws_capsule = capsule(2);
        aws_capsule.members[0].provider = MemberProvider::AwsKms;
        aws_capsule.members[0].nonce = None;
        aws_capsule.members[0].argon2 = None;
        aws_capsule.members[0].key_reference =
            "arn:aws:kms:us-east-1:123456789012:key/a".to_owned();
        aws_capsule.members[0].principal = Some(PrincipalBinding {
            authority: "entra".to_owned(),
            tenant_account_or_project: "123456789012".to_owned(),
            immutable_principal_id: "arn:aws:iam::123456789012:role/custodian-a".to_owned(),
        });
        assert!(aws_capsule.validate().is_err());

        let mut hardware_capsule = capsule(2);
        for (index, member) in hardware_capsule.members.iter_mut().enumerate() {
            member.provider = MemberProvider::YubikeyPiv;
            member.key_reference = "pkcs11:module-path=/usr/lib/libykcs11.so;slot-id=1;id=9a;public-key-sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;type=rsa-key-pair".to_owned();
            member.nonce = None;
            member.argon2 = None;
            member.hardware = Some(HardwareBinding {
                credential_id: format!("credential-{index}"),
                public_key:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                serial_number: None,
                attestation_fingerprint: None,
                user_presence_required: true,
            });
        }
        let status = hardware_capsule.effective_policy_status().unwrap();
        assert!(!status.compliant);
        assert!(status.hardware_verified);
        assert!(status
            .findings
            .contains(&"duplicate hardware public key".to_owned()));
    }

    #[test]
    fn fido2_capsule_binds_credential_and_public_key() {
        let mut hardware_capsule = capsule(2);
        for (index, member) in hardware_capsule.members.iter_mut().enumerate() {
            let (credential_id, public_key_der, public_key) = match index {
                0 => (
                    "AQID",
                    "BAUG",
                    "sha256:787c798e39a5bc1910355bae6d0cd87a36b2e10fd0202a83e3bb6b005da83472",
                ),
                1 => (
                    "AQIE",
                    "BAUH",
                    "sha256:8c7fdb659a9365d10a5499b5bf9f8ca06b17d9d85943e59c04b731f6698a5e6d",
                ),
                _ => (
                    "AQIF",
                    "BAUI",
                    "sha256:840783f8ac59b5d855485caf270137690f02686134765e03d0c0a15e065e6b76",
                ),
            };
            member.provider = MemberProvider::Fido2HmacSecret;
            member.key_reference = format!(
                "fido2:rp-id=vaultic.example;credential-id={credential_id};public-key-der={public_key_der}"
            );
            member.nonce = None;
            member.argon2 = None;
            member.hardware = Some(HardwareBinding {
                credential_id: credential_id.to_owned(),
                public_key: public_key.to_owned(),
                serial_number: None,
                attestation_fingerprint: None,
                user_presence_required: true,
            });
        }
        let status = hardware_capsule.effective_policy_status().unwrap();
        assert!(status.hardware_verified);
        assert!(status.compliant);
        hardware_capsule.members[0]
            .hardware
            .as_mut()
            .unwrap()
            .public_key = "sha256:wrong".to_owned();
        assert!(hardware_capsule.validate().is_err());
    }
}
