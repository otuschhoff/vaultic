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

    #[test]
    fn wrapped_share_stdin_is_bounded_and_strictly_decoded() {
        assert_eq!(
            decode_wrapped_share(b"Y2lwaGVydGV4dA==\n", "test")
                .unwrap()
                .as_slice(),
            b"ciphertext"
        );
        assert!(decode_wrapped_share(b"", "test").is_err());
        assert!(decode_wrapped_share(b"not-base64", "test").is_err());
        assert!(decode_wrapped_share(&vec![b'A'; 1024 * 1024 + 1], "test").is_err());
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.first().map(String::as_str) {
        Some("yubikey-piv-unwrap") if arguments.len() == 7 => yubikey_piv_unwrap(&arguments).await,
        Some("fido2-enroll") if arguments.len() == 3 => fido2_enroll(&arguments),
        Some("fido2-hmac-secret-derive") if arguments.len() == 5 => {
            let mut secret =
                fido2_secret(&arguments[1], &arguments[2], &arguments[3], &arguments[4])?;
            println!("{}", BASE64.encode(secret));
            secret.zeroize();
            Ok(())
        }
        Some("fido2-hmac-secret-unwrap") if arguments.len() == 7 => fido2_unwrap(&arguments).await,
        Some("macos-secure-enclave-enroll") if arguments.len() == 2 => {
            macos_secure_enclave_enroll(&arguments)
        }
        Some("macos-secure-enclave-delete") if arguments.len() == 2 => {
            macos_secure_enclave_delete(&arguments)
        }
        Some("macos-secure-enclave-unwrap") if arguments.len() == 6 => {
            macos_secure_enclave_unwrap(&arguments)
        }
        _ => bail!("invalid vaultic-key-custodian operation or argument count"),
    }
}

async fn yubikey_piv_unwrap(arguments: &[String]) -> Result<()> {
    let pin = read_pin(&arguments[1])?;
    let root_key_version = arguments[5]
        .parse::<u32>()
        .context("invalid root key version")?;
    let ciphertext = read_wrapped_share("PIV")?;
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
