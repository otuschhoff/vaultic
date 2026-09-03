use std::{env, fs, os::unix::fs::PermissionsExt};

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
    let ciphertext = Zeroizing::new(
        BASE64
            .decode(&arguments[7])
            .context("decode wrapped FIDO2 share")?,
    );
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

#[cfg(all(test, not(target_env = "musl")))]
mod tests {
    use super::*;

    #[test]
    fn fido2_reference_requires_safe_rp_and_decodable_pins() {
        let (rp_id, credential, public_key) = parse_fido2_reference(
            "fido2:rp-id=vaultic.example;credential-id=AQID;public-key-der=BAUG",
        )
        .unwrap();
        assert_eq!(rp_id, "vaultic.example");
        assert_eq!(credential, [1, 2, 3]);
        assert_eq!(public_key, [4, 5, 6]);
        assert!(parse_fido2_reference(
            "fido2:rp-id=https://vaultic.example;credential-id=AQID;public-key-der=BAUG"
        )
        .is_err());
        assert!(parse_fido2_reference(
            "fido2:rp-id=vaultic.example;credential-id=***;public-key-der=BAUG"
        )
        .is_err());
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.first().map(String::as_str) {
        Some("yubikey-piv-unwrap") if arguments.len() == 8 => yubikey_piv_unwrap(&arguments).await,
        Some("fido2-enroll") if arguments.len() == 3 => fido2_enroll(&arguments),
        Some("fido2-hmac-secret-derive") if arguments.len() == 5 => {
            let mut secret =
                fido2_secret(&arguments[1], &arguments[2], &arguments[3], &arguments[4])?;
            println!("{}", BASE64.encode(secret));
            secret.zeroize();
            Ok(())
        }
        Some("fido2-hmac-secret-unwrap") if arguments.len() == 8 => fido2_unwrap(&arguments).await,
        _ => bail!("invalid vaultic-key-custodian operation or argument count"),
    }
}

async fn yubikey_piv_unwrap(arguments: &[String]) -> Result<()> {
    let pin = read_pin(&arguments[1])?;
    let root_key_version = arguments[5]
        .parse::<u32>()
        .context("invalid root key version")?;
    let ciphertext = Zeroizing::new(
        BASE64
            .decode(&arguments[7])
            .context("decode wrapped PIV share")?,
    );
    let provider = YubikeyPivProvider::new(pin.as_str().to_owned());
    let plaintext = provider
        .unwrap(
            &KeyContext {
                repository_id: &arguments[2],
                slot_id: &arguments[3],
                key_reference: &arguments[4],
                dek_version: root_key_version,
                purpose: &arguments[6],
            },
            &ciphertext,
        )
        .await?;
    println!("{}", BASE64.encode(plaintext.as_slice()));
    Ok(())
}
