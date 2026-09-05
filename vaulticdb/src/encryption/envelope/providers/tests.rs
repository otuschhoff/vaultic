#[cfg(test)]
mod tests {
    //! Key provider binding and authentication tests.

    use super::*;

    #[test]
    fn provider_context_is_bound_to_repository_purpose_and_version() {
        let context = KeyContext {
            repository_id: "repo",
            slot_id: "slot",
            key_reference: "key",
            dek_version: 7,
            purpose: "metadata-dek",
        };
        let binding = context.binding();
        assert!(binding.contains(concat!("repo\0slot\0", "7\0metadata-dek")));
        let aws = context.aws_context();
        assert_eq!(aws["vaultic:repository"], "repo");
        assert_eq!(aws["vaultic:purpose"], "metadata-dek");
    }

    #[test]
    fn azure_references_require_an_explicit_key_version() {
        assert!(
            validate_azure_key_reference("https://example.vault.azure.net/keys/key/version")
                .is_ok()
        );
        assert!(validate_azure_key_reference(
            "https://example.managedhsm.azure.net/keys/key/version"
        )
        .is_ok());
        assert!(validate_azure_key_reference("https://example.vault.azure.net/keys/key").is_err());
        assert!(
            validate_azure_key_reference("http://example.vault.azure.net/keys/key/version")
                .is_err()
        );
        assert!(validate_azure_key_reference("https://attacker.example/keys/key/version").is_err());
    }

    #[test]
    fn vault_transit_references_are_https_key_urls() {
        assert_eq!(
            vault_transit_endpoint(
                "https://vault.example/v1/team-transit/keys/metadata",
                "encrypt"
            )
            .unwrap()
            .as_str(),
            "https://vault.example/v1/team-transit/encrypt/metadata"
        );
        assert!(
            vault_transit_endpoint("http://vault.example/v1/transit/keys/metadata", "encrypt")
                .is_err()
        );
        assert!(vault_transit_endpoint(
            "https://token@vault.example/v1/transit/keys/metadata",
            "encrypt"
        )
        .is_err());
        assert!(
            vault_transit_endpoint("https://vault.example/v1/transit/keys", "encrypt").is_err()
        );
    }

    #[test]
    fn pkcs11_references_pin_module_slot_and_secret_key() {
        let reference = parse_pkcs11_reference(
            "pkcs11:module-path=/usr/local/lib/pkcs11.so;slot-id=7;object=vaultic-metadata;type=secret-key",
        )
        .unwrap();
        assert_eq!(reference.slot_id, 7);
        assert_eq!(reference.object, "vaultic-metadata");
        assert!(parse_pkcs11_reference(
            "pkcs11:module-path=relative.so;slot-id=7;object=key;type=secret-key"
        )
        .is_err());
        assert!(parse_pkcs11_reference(
            "pkcs11:module-path=/module.so;slot-id=7;object=key;type=private"
        )
        .is_err());
    }

    #[test]
    fn yubikey_piv_references_pin_module_slot_and_rsa_key_id() {
        let reference = parse_yubikey_piv_reference(
            "pkcs11:module-path=/usr/local/lib/libykcs11.dylib;slot-id=2;id=9a;public-key-sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;type=rsa-key-pair",
        )
        .unwrap();
        assert_eq!(reference.module_path, "/usr/local/lib/libykcs11.dylib");
        assert_eq!(reference.slot_id, 2);
        assert_eq!(reference.key_id, vec![0x9a]);
        assert_eq!(reference.public_key_fingerprint, "a".repeat(64));
        assert!(parse_yubikey_piv_reference(
            "pkcs11:module-path=libykcs11.so;slot-id=2;id=9a;type=rsa-key-pair"
        )
        .is_err());
        assert!(parse_yubikey_piv_reference(
            "pkcs11:module-path=/libykcs11.so;slot-id=2;id=zz;type=rsa-key-pair"
        )
        .is_err());
        assert!(parse_yubikey_piv_reference(
            "pkcs11:module-path=/libykcs11.so;slot-id=2;id=9a;type=private"
        )
        .is_err());
    }

    #[tokio::test]
    async fn fido2_secret_wrap_is_authenticated_and_context_bound() {
        let provider = Fido2HmacSecretProvider::from_base64(BASE64.encode([7u8; 32])).unwrap();
        let context = KeyContext {
            repository_id: "repo-a",
            slot_id: "fido-a",
            key_reference: "fido2:reference",
            dek_version: 3,
            purpose: "capsule-share",
        };
        let ciphertext = provider.wrap(&context, b"share").await.unwrap();
        assert_eq!(
            provider
                .unwrap(&context, &ciphertext)
                .await
                .unwrap()
                .as_slice(),
            b"share"
        );
        let other = KeyContext {
            repository_id: "repo-b",
            ..context
        };
        assert!(provider.unwrap(&other, &ciphertext).await.is_err());
        assert!(Fido2HmacSecretProvider::from_base64(BASE64.encode([1u8; 31])).is_err());
    }

    #[test]
    fn fido2_reference_binds_credential_and_public_key() {
        let reference = "fido2:rp-id=vaultic.example;credential-id=AQID;public-key-der=BAUG";
        let (credential, public_key) = fido2_hardware_bindings(reference).unwrap();
        assert_eq!(credential, "AQID");
        assert_eq!(
            public_key,
            format!("sha256:{:x}", Sha256::digest([4, 5, 6]))
        );
        assert!(fido2_hardware_bindings(
            "fido2:rp-id=bad/id;credential-id=AQID;public-key-der=BAUG"
        )
        .is_err());
        assert!(fido2_hardware_bindings(
            "fido2:rp-id=vaultic.example;credential-id=;public-key-der=BAUG"
        )
        .is_err());
    }

    #[tokio::test]
    async fn macos_secure_enclave_wrap_is_authenticated_and_context_bound() {
        let private_key = p256::SecretKey::random(&mut OsRng);
        let public_key = private_key.public_key().to_encoded_point(false);
        let application_tag = URL_SAFE_NO_PAD.encode(b"vaultic/repo-a/enclave-a");
        let reference = format!(
            "secure-enclave:application-tag={application_tag};public-key={};access-control=biometry-current-set",
            URL_SAFE_NO_PAD.encode(public_key.as_bytes())
        );
        let provider = MacosSecureEnclaveProvider::from_key_reference(&reference).unwrap();
        let context = KeyContext {
            repository_id: "repo-a",
            slot_id: "enclave-a",
            key_reference: &reference,
            dek_version: 3,
            purpose: "capsule-share",
        };
        let ciphertext = provider.wrap(&context, b"share").await.unwrap();
        let ephemeral_public = PublicKey::from_sec1_bytes(
            macos_secure_enclave_ephemeral_public_key(&ciphertext).unwrap(),
        )
        .unwrap();
        let shared_secret = p256::ecdh::diffie_hellman(
            private_key.to_nonzero_scalar(),
            ephemeral_public.as_affine(),
        );
        assert_eq!(
            macos_secure_enclave_unwrap_with_shared_secret(
                &context,
                &ciphertext,
                shared_secret.raw_secret_bytes().as_slice(),
            )
            .unwrap()
            .as_slice(),
            b"share"
        );

        let other_context = KeyContext {
            repository_id: "repo-b",
            ..context
        };
        assert!(macos_secure_enclave_unwrap_with_shared_secret(
            &other_context,
            &ciphertext,
            shared_secret.raw_secret_bytes().as_slice(),
        )
        .is_err());
        for other_context in [
            KeyContext {
                slot_id: "enclave-b",
                ..context
            },
            KeyContext {
                dek_version: 4,
                ..context
            },
            KeyContext {
                purpose: "metadata-dek",
                ..context
            },
        ] {
            assert!(macos_secure_enclave_unwrap_with_shared_secret(
                &other_context,
                &ciphertext,
                shared_secret.raw_secret_bytes().as_slice(),
            )
            .is_err());
        }
        let mut tampered = ciphertext.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(macos_secure_enclave_unwrap_with_shared_secret(
            &context,
            &tampered,
            shared_secret.raw_secret_bytes().as_slice(),
        )
        .is_err());

        let (credential_id, fingerprint) =
            macos_secure_enclave_hardware_bindings(&reference).unwrap();
        assert_eq!(credential_id, application_tag);
        assert_eq!(
            fingerprint,
            format!("sha256:{:x}", Sha256::digest(public_key.as_bytes()))
        );
        assert!(MacosSecureEnclaveProvider::from_key_reference(
            "secure-enclave:application-tag=AQ;public-key=AQ;access-control=user-presence"
        )
        .is_err());
        assert!(MacosSecureEnclaveProvider::from_key_reference(
            "secure-enclave:application-tag=AQ;access-control=biometry-current-set"
        )
        .is_err());
        assert!(MacosSecureEnclaveProvider::from_key_reference(
            "secure-enclave:application-tag=AQ;public-key=AQ;access-control=biometry-current-set;extra=AQ"
        )
        .is_err());
    }
}
