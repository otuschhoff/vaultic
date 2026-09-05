//! Offline and hardware-backed key custodian command-line client.
#![warn(unreachable_pub)]

use std::{env, fs, io::Read, os::unix::fs::PermissionsExt};

use anyhow::{bail, Context, Result};
use base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD},
    Engine,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use vaulticdb::encryption::envelope::providers::{
    Fido2HmacSecretProvider, KeyContext, KeyProvider, YubikeyPivProvider,
};
use zeroize::{Zeroize, Zeroizing};

#[cfg(target_os = "macos")]
use vaulticdb::encryption::envelope::providers::{
    macos_secure_enclave_ephemeral_public_key, macos_secure_enclave_reference_parts,
    macos_secure_enclave_unwrap_with_shared_secret,
};

#[cfg(not(target_env = "musl"))]
use ctap_hid_fido2::{
    fidokey::{
        get_assertion::{Extension as GetExtension, GetAssertionArgsBuilder},
        make_credential::{Extension as MakeExtension, MakeCredentialArgsBuilder},
    },
    public_key::{PublicKey, PublicKeyType},
    verifier, Cfg, FidoKeyHidFactory,
};

#[cfg(not(target_env = "musl"))]
fn fido2_enroll(arguments: &[String]) -> Result<()> {
    let pin = read_pin(&arguments[1])?;
    validate_rp_id(&arguments[2])?;
    let device = FidoKeyHidFactory::create(&Cfg::init().with_keep_alive_msg_to_stderr(true))?;
    let challenge = verifier::create_challenge();
    let extension = MakeExtension::HmacSecret(Some(true));
    let request = MakeCredentialArgsBuilder::new(&arguments[2], &challenge)
        .pin(pin.as_str())
        .extensions(&[extension])
        .build();
    let attestation = device.make_credential_with_args(&request)?;
    if !attestation.flags.user_present_result || !attestation.flags.user_verified_result {
        bail!("FIDO2 enrollment did not prove user presence and verification");
    }
    if !attestation
        .extensions
        .iter()
        .any(|extension| matches!(extension, MakeExtension::HmacSecret(Some(true))))
    {
        bail!("authenticator did not enable hmac-secret for the credential");
    }
    let public_key = &attestation.credential_publickey.der;
    let public_key_hash = Sha256::digest(public_key);
    let attestation_fingerprint = if let Some(certificate) = attestation.attstmt_x5c.first() {
        let verification = std::panic::catch_unwind(|| {
            verifier::verify_attestation(&arguments[2], &challenge, &attestation)
        })
        .map_err(|_| anyhow::anyhow!("FIDO2 attestation verification failed"))?;
        if !verification.is_success
            || verification.credential_id != attestation.credential_descriptor.id
            || verification.credential_public_key.der != *public_key
        {
            bail!("FIDO2 attestation verification failed");
        }
        Some(format!("sha256:{:x}", Sha256::digest(certificate)))
    } else {
        None
    };
    println!(
        "{}",
        json!({
            "credential_id": URL_SAFE_NO_PAD.encode(&attestation.credential_descriptor.id),
            "public_key": format!("sha256:{public_key_hash:x}"),
            "public_key_der": URL_SAFE_NO_PAD.encode(public_key),
            "relying_party_id": arguments[2],
            "attestation_fingerprint": attestation_fingerprint,
            "user_presence_required": true
        })
    );
    Ok(())
}

#[cfg(target_env = "musl")]
fn fido2_enroll(_: &[String]) -> Result<()> {
    bail!("FIDO2 operations require the dynamically linked vaultic-key-custodian build")
}

async fn fido2_unwrap(arguments: &[String]) -> Result<()> {
    let mut secret = fido2_secret(&arguments[1], &arguments[2], &arguments[3], &arguments[4])?;
    let provider = Fido2HmacSecretProvider::from_base64(BASE64.encode(secret))?;
    secret.zeroize();
    let ciphertext = read_wrapped_share("FIDO2")?;
    let plaintext = provider
        .unwrap(
            &KeyContext {
                repository_id: &arguments[2],
                slot_id: &arguments[3],
                key_reference: &arguments[4],
                dek_version: arguments[5].parse().context("invalid root key version")?,
                purpose: &arguments[6],
            },
            &ciphertext,
        )
        .await?;
    println!("{}", BASE64.encode(plaintext.as_slice()));
    Ok(())
}

#[cfg(not(target_env = "musl"))]
fn fido2_secret(
    pin_file: &str,
    repository_id: &str,
    member_id: &str,
    reference: &str,
) -> Result<[u8; 32]> {
    let pin = read_pin(pin_file)?;
    let (rp_id, credential_id, public_key_der) = parse_fido2_reference(reference)?;
    let mut salt_hash = Sha256::new();
    salt_hash.update(b"vaultic-fido2-hmac-secret-v1\0");
    salt_hash.update(repository_id.as_bytes());
    salt_hash.update([0]);
    salt_hash.update(member_id.as_bytes());
    let salt: [u8; 32] = salt_hash.finalize().into();
    let config = Cfg::init().with_keep_alive_msg_to_stderr(true);
    let device = FidoKeyHidFactory::create(&config)?;
    let challenge = verifier::create_challenge();
    let extension = GetExtension::HmacSecret(Some(salt));
    let request = GetAssertionArgsBuilder::new(&rp_id, &challenge)
        .pin(pin.as_str())
        .credential_id(&credential_id)
        .extensions(&[extension])
        .build();
    let assertions = device.get_assertion_with_args(&request)?;
    let assertion = assertions
        .first()
        .context("authenticator returned no FIDO2 assertion")?;
    if assertion.credential_id != credential_id
        || !assertion.flags.user_present_result
        || !assertion.flags.user_verified_result
    {
        bail!("FIDO2 assertion did not match the credential or prove UP and UV");
    }
    let public_key = PublicKey::with_der(&public_key_der, PublicKeyType::Ecdsa256);
    if !verifier::verify_assertion(&rp_id, &public_key, &challenge, assertion) {
        bail!("FIDO2 assertion signature verification failed");
    }
    assertion
        .extensions
        .iter()
        .find_map(|extension| match extension {
            GetExtension::HmacSecret(Some(secret)) => Some(*secret),
            _ => None,
        })
        .context("authenticator returned no hmac-secret output")
}

#[cfg(target_env = "musl")]
fn fido2_secret(_: &str, _: &str, _: &str, _: &str) -> Result<[u8; 32]> {
    bail!("FIDO2 operations require the dynamically linked vaultic-key-custodian build")
}

#[cfg(target_os = "macos")]
fn macos_secure_enclave_enroll(arguments: &[String]) -> Result<()> {
    use std::io::Write;

    use security_framework::{
        access_control::{ProtectionMode, SecAccessControl},
        item::Location,
        key::{GenerateKeyOptions, KeyType, SecKey, Token},
        passwords_options::AccessControlOptions,
    };

    let application_tag = URL_SAFE_NO_PAD
        .decode(&arguments[1])
        .context("decode macOS Secure Enclave application tag")?;
    if application_tag.len() != 32 {
        bail!("macOS Secure Enclave application tag must be 32 random bytes");
    }
    let access_control = SecAccessControl::create_with_protection(
        Some(ProtectionMode::AccessibleWhenUnlockedThisDeviceOnly),
        (AccessControlOptions::BIOMETRY_CURRENT_SET | AccessControlOptions::PRIVATE_KEY_USAGE)
            .bits(),
    )
    .context("create macOS Secure Enclave biometric access control")?;
    let mut options = GenerateKeyOptions::default();
    options
        .set_key_type(KeyType::ec())
        .set_size_in_bits(256)
        .set_token(Token::SecureEnclave)
        .set_location(Location::DataProtectionKeychain)
        .set_label(macos_secure_enclave_label(&arguments[1]))
        .set_access_control(access_control);
    let private_key = SecKey::new(&options).map_err(|error| {
        anyhow::anyhow!(
            "create Secure Enclave key; the custodian must be code-signed for Data Protection Keychain access: {error}"
        )
    })?;
    let enrollment = (|| -> Result<String> {
        let public_key = private_key
            .public_key()
            .context("Secure Enclave returned no public key")?
            .external_representation()
            .context("export Secure Enclave public key")?
            .to_vec();
        if public_key.len() != 65 || public_key.first() != Some(&4) {
            bail!("Secure Enclave returned an invalid P-256 public key");
        }
        Ok(serde_json::to_string(&json!({
            "application_tag": arguments[1],
            "public_key": format!("sha256:{:x}", Sha256::digest(&public_key)),
            "public_key_data": URL_SAFE_NO_PAD.encode(public_key),
            "access_control": "biometry-current-set",
            "user_presence_required": true
        }))?)
    })();
    let output = match enrollment {
        Ok(output) => output,
        Err(error) => {
            private_key.delete().ok();
            return Err(error);
        }
    };
    if let Err(error) = writeln!(std::io::stdout().lock(), "{output}") {
        private_key.delete().ok();
        return Err(error).context("write Secure Enclave enrollment result");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn macos_secure_enclave_enroll(_: &[String]) -> Result<()> {
    bail!("macOS Secure Enclave operations require macOS")
}

#[cfg(target_os = "macos")]
fn macos_secure_enclave_delete(arguments: &[String]) -> Result<()> {
    use security_framework::item::{ItemSearchOptions, KeyClass, Reference, SearchResult};

    let (application_tag, expected_public_key) =
        macos_secure_enclave_reference_parts(&arguments[1])?;
    let mut search = ItemSearchOptions::new();
    search
        .ignore_legacy_keychains()
        .key_class(KeyClass::private())
        .label(&macos_secure_enclave_label(
            &URL_SAFE_NO_PAD.encode(application_tag),
        ))
        .load_refs(true);
    let mut results = search.search().context(
        "find Secure Enclave key for enrollment rollback; the custodian must be code-signed for Data Protection Keychain access",
    )?;
    if results.len() != 1 {
        bail!("Secure Enclave key for enrollment rollback is missing or ambiguous");
    }
    let private_key = match results.remove(0) {
        SearchResult::Ref(Reference::Key(key)) => key,
        _ => bail!("Secure Enclave rollback lookup did not return a private key"),
    };
    let actual_public_key = private_key
        .public_key()
        .context("Secure Enclave returned no public key during enrollment rollback")?
        .external_representation()
        .context("export Secure Enclave public key during enrollment rollback")?
        .to_vec();
    if actual_public_key != expected_public_key {
        bail!("Secure Enclave rollback public key does not match the enrollment binding");
    }
    private_key
        .delete()
        .context("delete Secure Enclave key after failed enrollment")
}

#[cfg(not(target_os = "macos"))]
fn macos_secure_enclave_delete(_: &[String]) -> Result<()> {
    bail!("macOS Secure Enclave operations require macOS")
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn macos_secure_enclave_unwrap(arguments: &[String]) -> Result<()> {
    use core_foundation::data::CFData;
    use security_framework::{
        item::{ItemSearchOptions, KeyClass, Reference, SearchResult},
        key::{Algorithm, KeyType, SecKey},
        os::macos::key::SecKeyExt,
    };

    let ciphertext = read_wrapped_share("macOS Secure Enclave")?;
    let (application_tag, expected_public_key) =
        macos_secure_enclave_reference_parts(&arguments[3])?;
    let encoded_tag = URL_SAFE_NO_PAD.encode(application_tag);
    let mut search = ItemSearchOptions::new();
    search
        .ignore_legacy_keychains()
        .key_class(KeyClass::private())
        .label(&macos_secure_enclave_label(&encoded_tag))
        .load_refs(true);
    let mut results = search.search().context(
        "find Secure Enclave key; the custodian must be code-signed for Data Protection Keychain access",
    )?;
    if results.len() != 1 {
        bail!("macOS Secure Enclave key is missing or ambiguous");
    }
    let private_key = match results.remove(0) {
        SearchResult::Ref(Reference::Key(key)) => key,
        _ => bail!("macOS Secure Enclave lookup did not return a private key"),
    };
    let actual_public_key = private_key
        .public_key()
        .context("Secure Enclave returned no public key")?
        .external_representation()
        .context("export Secure Enclave public key")?
        .to_vec();
    if actual_public_key != expected_public_key {
        bail!("macOS Secure Enclave public key does not match the capsule binding");
    }
    let ephemeral = macos_secure_enclave_ephemeral_public_key(&ciphertext)?;
    let ephemeral_key = SecKey::from_data(KeyType::ec(), &CFData::from_buffer(ephemeral))
        .map_err(|error| anyhow::anyhow!("import ephemeral P-256 public key: {error}"))?;
    let mut shared_secret = Zeroizing::new(
        private_key
            .key_exchange(Algorithm::ECDHKeyExchangeStandard, &ephemeral_key, 32, None)
            .map_err(|error| {
                anyhow::anyhow!(
                    "Secure Enclave ECDH; Touch ID authorization was denied or unavailable: {error}"
                )
            })?,
    );
    if shared_secret.len() != 32 {
        shared_secret.zeroize();
        bail!("Secure Enclave returned an invalid ECDH result");
    }
    let plaintext = macos_secure_enclave_unwrap_with_shared_secret(
        &KeyContext {
            repository_id: &arguments[1],
            slot_id: &arguments[2],
            key_reference: &arguments[3],
            dek_version: arguments[4].parse().context("invalid root key version")?,
            purpose: &arguments[5],
        },
        &ciphertext,
        shared_secret.as_slice(),
    )?;
    shared_secret.zeroize();
    println!("{}", BASE64.encode(plaintext.as_slice()));
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn macos_secure_enclave_unwrap(_: &[String]) -> Result<()> {
    bail!("macOS Secure Enclave operations require macOS")
}

#[cfg(target_os = "macos")]
fn macos_secure_enclave_label(application_tag: &str) -> String {
    format!("com.vaultic.secure-enclave.{application_tag}")
}

fn read_pin(path: &str) -> Result<Zeroizing<String>> {
    let metadata = fs::metadata(path).context("inspect hardware PIN file")?;
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!("hardware PIN file must not be accessible by group or others");
    }
    let mut pin = Zeroizing::new(fs::read_to_string(path).context("read hardware PIN file")?);
    while pin.ends_with(char::is_whitespace) {
        pin.pop();
    }
    if pin.is_empty() {
        bail!("hardware PIN file is empty");
    }
    Ok(pin)
}

fn read_wrapped_share(provider: &str) -> Result<Zeroizing<Vec<u8>>> {
    const MAX_ENCODED_SHARE_BYTES: usize = 1024 * 1024;
    let mut encoded = Zeroizing::new(Vec::new());
    std::io::stdin()
        .take((MAX_ENCODED_SHARE_BYTES + 1) as u64)
        .read_to_end(&mut encoded)
        .context("read wrapped share from stdin")?;
    decode_wrapped_share(encoded.as_slice(), provider)
}

fn decode_wrapped_share(encoded: &[u8], provider: &str) -> Result<Zeroizing<Vec<u8>>> {
    const MAX_ENCODED_SHARE_BYTES: usize = 1024 * 1024;
    if encoded.len() > MAX_ENCODED_SHARE_BYTES {
        bail!("wrapped share exceeds input limit");
    }
    let encoded = encoded.trim_ascii_end();
    if encoded.is_empty() {
        bail!("wrapped share stdin is empty");
    }
    Ok(Zeroizing::new(BASE64.decode(encoded).with_context(
        || format!("decode wrapped {provider} share"),
    )?))
}

#[cfg(not(target_env = "musl"))]
fn parse_fido2_reference(reference: &str) -> Result<(String, Vec<u8>, Vec<u8>)> {
    let fields = reference
        .strip_prefix("fido2:")
        .context("FIDO2 key reference must start with fido2:")?
        .split(';')
        .map(|field| {
            field
                .split_once('=')
                .context("invalid FIDO2 key reference field")
        })
        .collect::<Result<std::collections::HashMap<_, _>>>()?;
    if fields.len() != 3 {
        bail!("FIDO2 key reference requires rp-id, credential-id, and public-key-der");
    }
    let rp_id = fields.get("rp-id").context("FIDO2 rp-id is missing")?;
    validate_rp_id(rp_id)?;
    Ok((
        (*rp_id).to_owned(),
        URL_SAFE_NO_PAD.decode(
            fields
                .get("credential-id")
                .context("FIDO2 credential-id is missing")?,
        )?,
        URL_SAFE_NO_PAD.decode(
            fields
                .get("public-key-der")
                .context("FIDO2 public-key-der is missing")?,
        )?,
    ))
}

#[cfg(not(target_env = "musl"))]
fn validate_rp_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 253
        || value.starts_with('.')
        || value.ends_with('.')
        || value
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'))
    {
        bail!("FIDO2 relying-party ID is invalid");
    }
    Ok(())
}

include!("vaultic-key-custodian/tests.rs");
