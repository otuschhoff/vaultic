use crate::ids::RepositoryId;

pub fn macos_secure_enclave_hardware_bindings(reference: &str) -> Result<(String, String)> {
    let reference = parse_macos_secure_enclave_reference(reference)?;
    Ok((
        reference.application_tag,
        format!("sha256:{:x}", Sha256::digest(reference.public_key)),
    ))
}

pub fn macos_secure_enclave_reference_parts(reference: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let reference = parse_macos_secure_enclave_reference(reference)?;
    Ok((
        URL_SAFE_NO_PAD
            .decode(reference.application_tag)
            .expect("validated application tag"),
        reference.public_key,
    ))
}

pub fn macos_secure_enclave_ephemeral_public_key(ciphertext: &[u8]) -> Result<&[u8]> {
    if ciphertext.len()
        < 1 + MACOS_SECURE_ENCLAVE_PUBLIC_KEY_BYTES + MACOS_SECURE_ENCLAVE_NONCE_BYTES + 16
        || ciphertext[0] != MACOS_SECURE_ENCLAVE_VERSION
    {
        bail!("macOS Secure Enclave ciphertext is malformed or truncated");
    }
    let public_key = &ciphertext[1..1 + MACOS_SECURE_ENCLAVE_PUBLIC_KEY_BYTES];
    PublicKey::from_sec1_bytes(public_key)
        .context("decode ephemeral macOS Secure Enclave public key")?;
    Ok(public_key)
}

pub fn macos_secure_enclave_unwrap_with_shared_secret(
    context: &KeyContext<'_>,
    ciphertext: &[u8],
    shared_secret: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    macos_secure_enclave_ephemeral_public_key(ciphertext)?;
    if shared_secret.len() != 32 {
        bail!("macOS Secure Enclave ECDH output must be 32 bytes");
    }
    let nonce_offset = 1 + MACOS_SECURE_ENCLAVE_PUBLIC_KEY_BYTES;
    let payload_offset = nonce_offset + MACOS_SECURE_ENCLAVE_NONCE_BYTES;
    let key = macos_secure_enclave_key(shared_secret, context)?;
    let cipher = Aes256Gcm::new_from_slice(key.as_slice()).expect("32-byte AES key");
    cipher
        .decrypt(
            Nonce::from_slice(&ciphertext[nonce_offset..payload_offset]),
            Payload {
                msg: &ciphertext[payload_offset..],
                aad: context.binding().as_bytes(),
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| anyhow::anyhow!("macOS Secure Enclave share authentication failed"))
}

fn macos_secure_enclave_key(
    shared_secret: &[u8],
    context: &KeyContext<'_>,
) -> Result<Zeroizing<[u8; 32]>> {
    let mut key = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(
        Some(b"vaultic-macos-secure-enclave-share-v1"),
        shared_secret,
    )
    .expand(context.binding().as_bytes(), key.as_mut())
    .map_err(|_| anyhow::anyhow!("derive macOS Secure Enclave share key"))?;
    Ok(key)
}

fn parse_macos_secure_enclave_reference(reference: &str) -> Result<MacosSecureEnclaveReference> {
    let fields = reference
        .strip_prefix("secure-enclave:")
        .context("macOS Secure Enclave key reference must start with secure-enclave:")?
        .split(';')
        .map(|field| {
            field
                .split_once('=')
                .context("invalid macOS Secure Enclave key reference field")
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if fields.len() != 3 || fields.get("access-control") != Some(&"biometry-current-set") {
        bail!("macOS Secure Enclave key reference requires application-tag, public-key, and access-control=biometry-current-set");
    }
    let application_tag = fields
        .get("application-tag")
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .context("macOS Secure Enclave application tag is invalid")?;
    let application_tag_bytes = URL_SAFE_NO_PAD
        .decode(application_tag)
        .context("decode macOS Secure Enclave application tag")?;
    if application_tag_bytes.is_empty() || application_tag_bytes.len() > 128 {
        bail!("macOS Secure Enclave application tag is invalid");
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(
            fields
                .get("public-key")
                .context("macOS Secure Enclave public key is missing")?,
        )
        .context("decode macOS Secure Enclave public key")?;
    PublicKey::from_sec1_bytes(&public_key).context("decode macOS Secure Enclave public key")?;
    Ok(MacosSecureEnclaveReference {
        application_tag: (*application_tag).to_owned(),
        public_key,
    })
}

struct OwnedKeyContext {
    repository_id: RepositoryId,
    slot_id: String,
    key_reference: String,
    dek_version: u32,
    purpose: String,
}

impl From<&KeyContext<'_>> for OwnedKeyContext {
    fn from(context: &KeyContext<'_>) -> Self {
        Self {
            repository_id: context.repository_id.into(),
            slot_id: context.slot_id.to_owned(),
            key_reference: context.key_reference.to_owned(),
            dek_version: context.dek_version,
            purpose: context.purpose.to_owned(),
        }
    }
}

impl OwnedKeyContext {
    fn as_borrowed(&self) -> KeyContext<'_> {
        KeyContext {
            repository_id: self.repository_id.as_str(),
            slot_id: &self.slot_id,
            key_reference: &self.key_reference,
            dek_version: self.dek_version,
            purpose: &self.purpose,
        }
    }
}

struct Pkcs11Reference {
    module_path: String,
    slot_id: u64,
    object: String,
}

struct YubikeyPivReference {
    module_path: String,
    slot_id: u64,
    key_id: Vec<u8>,
    public_key_fingerprint: String,
}

fn parse_yubikey_piv_reference(reference: &str) -> Result<YubikeyPivReference> {
    let fields = reference
        .strip_prefix("pkcs11:")
        .context("YubiKey PIV key reference must start with pkcs11:")?
        .split(';')
        .map(|field| {
            field
                .split_once('=')
                .context("invalid YubiKey PIV key reference field")
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if fields.len() != 5 || fields.get("type") != Some(&"rsa-key-pair") {
        bail!("YubiKey PIV key reference requires module-path, slot-id, id, public-key-sha256, and type=rsa-key-pair");
    }
    let module_path = fields
        .get("module-path")
        .filter(|value| value.starts_with('/') && !value.contains('\0'))
        .context("YKCS11 module-path must be absolute")?;
    let encoded_id = fields
        .get("id")
        .filter(|value| !value.is_empty() && value.len() % 2 == 0)
        .context("YubiKey PIV key id must be non-empty hexadecimal bytes")?;
    let key_id = encoded_id
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hexadecimal key id is ASCII");
            u8::from_str_radix(text, 16).context("YubiKey PIV key id is not hexadecimal")
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(YubikeyPivReference {
        module_path: (*module_path).to_owned(),
        slot_id: fields
            .get("slot-id")
            .context("YubiKey PIV slot-id is missing")?
            .parse()
            .context("YubiKey PIV slot-id is invalid")?,
        key_id,
        public_key_fingerprint: fields
            .get("public-key-sha256")
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .context("YubiKey PIV public-key-sha256 must contain 64 hexadecimal characters")?
            .to_ascii_lowercase(),
    })
}

pub fn yubikey_piv_public_key_binding(reference: &str) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        parse_yubikey_piv_reference(reference)?.public_key_fingerprint
    ))
}

pub fn fido2_hardware_bindings(reference: &str) -> Result<(String, String)> {
    let fields = reference
        .strip_prefix("fido2:")
        .context("FIDO2 key reference must start with fido2:")?
        .split(';')
        .map(|field| {
            field
                .split_once('=')
                .context("invalid FIDO2 key reference field")
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if fields.len() != 3 {
        bail!("FIDO2 key reference requires rp-id, credential-id, and public-key-der");
    }
    let rp_id = fields.get("rp-id").context("FIDO2 rp-id is missing")?;
    if rp_id.is_empty()
        || rp_id.len() > 253
        || rp_id.starts_with('.')
        || rp_id.ends_with('.')
        || rp_id
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'))
    {
        bail!("FIDO2 relying-party ID is invalid");
    }
    let credential_id = fields
        .get("credential-id")
        .context("FIDO2 credential-id is missing")?;
    let credential = URL_SAFE_NO_PAD
        .decode(credential_id)
        .context("decode FIDO2 credential-id")?;
    if credential.is_empty() {
        bail!("FIDO2 credential-id is empty");
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(
            fields
                .get("public-key-der")
                .context("FIDO2 public-key-der is missing")?,
        )
        .context("decode FIDO2 public-key-der")?;
    if public_key.is_empty() {
        bail!("FIDO2 public-key-der is empty");
    }
    Ok((
        (*credential_id).to_owned(),
        format!("sha256:{:x}", Sha256::digest(public_key)),
    ))
}

fn rsa_public_key_fingerprint(modulus: &[u8], exponent: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update((modulus.len() as u64).to_be_bytes());
    digest.update(modulus);
    digest.update((exponent.len() as u64).to_be_bytes());
    digest.update(exponent);
    format!("{:x}", digest.finalize())
}

fn parse_pkcs11_reference(reference: &str) -> Result<Pkcs11Reference> {
    let fields = reference
        .strip_prefix("pkcs11:")
        .context("PKCS#11 key reference must start with pkcs11:")?
        .split(';')
        .map(|field| {
            field
                .split_once('=')
                .context("invalid PKCS#11 key reference field")
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if fields.len() != 4 || fields.get("type") != Some(&"secret-key") {
        bail!("PKCS#11 key reference requires module-path, slot-id, object, and type=secret-key");
    }
    let module_path = fields
        .get("module-path")
        .filter(|value| value.starts_with('/') && !value.contains('\0'))
        .context("PKCS#11 module-path must be absolute")?;
    let object = fields
        .get("object")
        .filter(|value| !value.is_empty() && !value.contains('\0'))
        .context("PKCS#11 object label is empty")?;
    Ok(Pkcs11Reference {
        module_path: (*module_path).to_owned(),
        slot_id: fields
            .get("slot-id")
            .context("PKCS#11 slot-id is missing")?
            .parse()
            .context("PKCS#11 slot-id is invalid")?,
        object: (*object).to_owned(),
    })
}
