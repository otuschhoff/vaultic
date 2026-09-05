pub async fn create_escrow_record(
    repository_id: &str,
    escrow_id: &str,
    key_reference: &str,
    master_key: &[u8],
    provider: &dyn KeyProvider,
) -> Result<EscrowRecord> {
    if escrow_id.is_empty() || key_reference.is_empty() || master_key.is_empty() {
        bail!("escrow ID, key reference, and master key are required");
    }
    let context = KeyContext {
        repository_id,
        slot_id: escrow_id,
        key_reference,
        dek_version: 1,
        purpose: "repository-master-key-escrow",
    };
    let payload = encode_cloud_payload(&context, master_key)?;
    let wrapped = provider.wrap(&context, &payload).await?;
    Ok(EscrowRecord {
        format: 1,
        repository_id: repository_id.into(),
        escrow_id: escrow_id.to_owned(),
        provider: provider.name().to_owned(),
        key_reference: key_reference.to_owned(),
        wrapped_master_key: BASE64.encode(wrapped),
    })
}

pub async fn recover_escrow_record(
    record: &EscrowRecord,
    repository_id: &str,
    provider: &dyn KeyProvider,
) -> Result<Zeroizing<Vec<u8>>> {
    if record.format != 1
        || record.repository_id != repository_id
        || record.provider != provider.name()
    {
        bail!("escrow record identity or provider mismatch");
    }
    let context = KeyContext {
        repository_id,
        slot_id: &record.escrow_id,
        key_reference: &record.key_reference,
        dek_version: 1,
        purpose: "repository-master-key-escrow",
    };
    let wrapped = BASE64
        .decode(&record.wrapped_master_key)
        .context("decode escrow ciphertext")?;
    let payload = provider.unwrap(&context, &wrapped).await?;
    decode_cloud_payload(&context, &payload)
}

fn remove_slot(envelope: &mut KeyEnvelope, slot_id: &str) -> Result<()> {
    let position = envelope
        .slots
        .iter()
        .position(|slot| slot.id == slot_id)
        .context("metadata key slot does not exist")?;
    if envelope.slots.len() == 1 {
        bail!("cannot remove the final metadata key slot");
    }
    envelope.slots.remove(position);
    envelope.generation = envelope
        .generation
        .checked_add(1)
        .context("metadata key envelope generation overflow")?;
    validate_envelope(envelope, &envelope.repository_id)
}

fn rotate_local_slot(
    envelope: &mut KeyEnvelope,
    dek: &[u8],
    slot_id: &str,
    passphrase: &[u8],
) -> Result<()> {
    let old = envelope
        .slots
        .iter()
        .find(|slot| slot.id == slot_id)
        .cloned()
        .context("metadata key slot does not exist")?;
    if old.provider != "local-argon2id" {
        bail!("metadata key slot is not an Argon2id slot");
    }
    envelope.slots.retain(|slot| slot.id != slot_id);
    add_local_slot(
        envelope,
        dek,
        slot_id,
        passphrase,
        old.priority,
        old.recovery,
    )?;
    envelope.generation -= 1;
    envelope.generation = envelope
        .generation
        .checked_add(1)
        .context("metadata key envelope generation overflow")?;
    Ok(())
}

fn parse_envelope(data: &[u8], repository_id: &str) -> Result<KeyEnvelope> {
    let envelope: KeyEnvelope =
        serde_json::from_slice(data).context("decode metadata key envelope")?;
    validate_envelope(&envelope, repository_id)?;
    Ok(envelope)
}

fn validate_envelope(envelope: &KeyEnvelope, repository_id: &str) -> Result<()> {
    if envelope.format != ENVELOPE_FORMAT
        || envelope.repository_id != repository_id
        || envelope.generation == 0
        || envelope.active_dek_version == 0
        || envelope.slots.is_empty()
    {
        bail!("metadata key envelope is malformed or belongs to another repository");
    }
    let mut ids = std::collections::HashSet::new();
    for slot in &envelope.slots {
        if slot.id.is_empty()
            || !ids.insert(&slot.id)
            || slot.dek_version == 0
            || slot.wrapped_dek.is_empty()
            || slot.nonce.is_empty()
        {
            bail!("metadata key envelope contains an invalid slot");
        }
        if slot.provider == "local-argon2id" && slot.argon2.is_none() {
            bail!("local recovery slot is missing Argon2id parameters");
        }
    }
    let root_version = envelope.slots[0].dek_version;
    if envelope
        .slots
        .iter()
        .any(|slot| slot.dek_version != root_version)
    {
        bail!("metadata key envelope slots do not share one root DEK version");
    }
    let mut expected = root_version;
    for rotation in &envelope.rotations {
        expected = expected
            .checked_add(1)
            .context("metadata DEK version overflow")?;
        if rotation.version != expected
            || rotation.wrapped_dek.is_empty()
            || rotation.nonce.is_empty()
        {
            bail!("metadata key envelope contains an invalid DEK rotation chain");
        }
    }
    if expected != envelope.active_dek_version {
        bail!("metadata key envelope active DEK is not reachable from its slots");
    }
    if envelope.retired_through_dek_version >= envelope.active_dek_version {
        bail!("metadata key envelope retires its active DEK");
    }
    Ok(())
}

fn readable_deks(envelope: &KeyEnvelope, root: &[u8]) -> Result<Vec<EncryptionKey>> {
    Ok(expand_deks(envelope, root)?
        .into_iter()
        .filter(|key| key.version > envelope.retired_through_dek_version)
        .collect())
}

fn wrap_local(
    repository_id: &str,
    slot_id: &str,
    dek_version: u32,
    passphrase: &[u8],
    config: &Argon2Config,
    nonce: [u8; NONCE_BYTES],
    dek: &[u8],
) -> Result<Vec<u8>> {
    let mut kek = derive_kek(passphrase, config)?;
    let cipher = Aes256Gcm::new_from_slice(&kek)
        .map_err(|_| anyhow::anyhow!("initialize metadata key wrapping cipher"))?;
    let result = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: dek,
                aad: &slot_aad(repository_id, slot_id, dek_version),
            },
        )
        .map_err(|_| anyhow::anyhow!("wrap metadata key"));
    kek.zeroize();
    result
}

fn unwrap_local(
    envelope: &KeyEnvelope,
    slot: &KeySlot,
    passphrase: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let config = slot
        .argon2
        .as_ref()
        .context("missing Argon2id parameters")?;
    let nonce = BASE64.decode(&slot.nonce).context("decode slot nonce")?;
    let wrapped = BASE64
        .decode(&slot.wrapped_dek)
        .context("decode wrapped metadata key")?;
    if nonce.len() != NONCE_BYTES {
        bail!("invalid metadata key slot nonce");
    }
    let mut kek = derive_kek(passphrase, config)?;
    let cipher = Aes256Gcm::new_from_slice(&kek)
        .map_err(|_| anyhow::anyhow!("initialize metadata key unwrapping cipher"))?;
    let result = cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &wrapped,
                aad: &slot_aad(&envelope.repository_id, &slot.id, slot.dek_version),
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| anyhow::anyhow!("metadata key slot authentication failed"));
    kek.zeroize();
    result
}

fn derive_kek(passphrase: &[u8], config: &Argon2Config) -> Result<[u8; DEK_BYTES]> {
    if config.memory_kib < 64 * 1024
        || config.iterations < 3
        || config.parallelism == 0
        || config.parallelism > 16
    {
        bail!("Argon2id parameters do not meet the minimum policy");
    }
    let salt = BASE64
        .decode(&config.salt)
        .context("decode Argon2id salt")?;
    if salt.len() < SALT_BYTES {
        bail!("Argon2id salt is too short");
    }
    let params = Params::new(
        config.memory_kib,
        config.iterations,
        config.parallelism,
        Some(DEK_BYTES),
    )
    .map_err(|error| anyhow::anyhow!("invalid Argon2id parameters: {error}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0u8; DEK_BYTES];
    argon2
        .hash_password_into(passphrase, &salt, &mut key)
        .map_err(|error| anyhow::anyhow!("derive recovery key: {error}"))?;
    Ok(key)
}

fn read_recovery_passphrase(path: Option<&std::path::Path>) -> Result<Option<Zeroizing<Vec<u8>>>> {
    let Some(path) = path else {
        return Ok(None);
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("inspect recovery passphrase file {}", path.display()))?;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("recovery passphrase file must not be accessible by group or others");
        }
    }
    let mut value = std::fs::read(path)
        .with_context(|| format!("read recovery passphrase file {}", path.display()))?;
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value.pop();
    }
    if value.is_empty() {
        bail!("recovery passphrase file is empty");
    }
    Ok(Some(Zeroizing::new(value)))
}

fn slot_aad(repository_id: &str, slot_id: &str, dek_version: u32) -> Vec<u8> {
    format!("vaulticdb-key-slot\0{repository_id}\0{slot_id}\0{dek_version}").into_bytes()
}

fn disabled_status() -> EncryptionStatus {
    EncryptionStatus {
        enabled: false,
        algorithm: "",
        active_dek_version: 0,
        envelope_generation: 0,
        unlock_slot: None,
        recovery_unlock: false,
        initializing: false,
    }
}
