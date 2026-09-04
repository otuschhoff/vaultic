pub async fn acquire_metadata_lease(
    socket: &str,
    manifest_path: &std::path::Path,
    ttl: Duration,
) -> Result<(BrokerLeaseConnection, Zeroizing<Vec<u8>>)> {
    if socket.is_empty() || ttl.is_zero() || ttl > MAX_LEASE_TTL {
        bail!("invalid broker metadata lease configuration");
    }
    let manifest_bytes = std::fs::read(manifest_path)
        .with_context(|| format!("read release manifest {}", manifest_path.display()))?;
    let manifest: ReleaseManifest =
        serde_json::from_slice(&manifest_bytes).context("decode release manifest")?;
    if manifest.component != "vaulticdb" {
        bail!("release manifest is not for vaulticdb");
    }
    let executable = std::env::current_exe()?.canonicalize()?;
    let actual_digest = format!("{:x}", Sha256::digest(std::fs::read(&executable)?));
    if manifest.executable_sha256.to_ascii_lowercase() != actual_digest {
        bail!("release manifest executable digest does not match running vaulticdb");
    }
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect key broker {socket}"))?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let negotiation = serde_json::json!({
        "operation": "negotiate",
        "protocols": ["vaultic-key-broker.v1"],
    });
    let mut negotiation = serde_json::to_vec(&negotiation)?;
    negotiation.push(b'\n');
    writer.write_all(&negotiation).await?;
    writer.flush().await?;
    let mut negotiation_response = Vec::new();
    reader.read_until(b'\n', &mut negotiation_response).await?;
    #[derive(Deserialize)]
    struct NegotiationResponse {
        result: String,
        #[serde(default)]
        protocol: String,
        #[serde(default)]
        challenge: String,
        #[serde(default)]
        message: String,
    }
    let negotiation: NegotiationResponse = serde_json::from_slice(&negotiation_response)?;
    if negotiation.result != "negotiated"
        || negotiation.protocol != "vaultic-key-broker.v1"
        || negotiation.challenge.is_empty()
    {
        bail!(
            "key broker protocol negotiation failed: {}",
            negotiation.message
        );
    }
    let challenge_response = format!(
        "{:x}",
        Sha256::digest(format!(
            "vaultic-broker-lease-challenge-v1\0vaultic-key-broker.v1\0{}\0{}",
            negotiation.challenge, actual_digest
        ))
    );
    let request = serde_json::json!({
        "operation": "acquire_lease",
        "component": manifest.component,
        "version": manifest.version,
        "release_identity": manifest.release_identity,
        "release_signature": manifest.signature,
        "capability": "metadata-dek",
        "ttl_seconds": ttl.as_secs(),
        "challenge_response": challenge_response,
    });
    let mut request = serde_json::to_vec(&request)?;
    request.push(b'\n');
    writer.write_all(&request).await?;
    writer.flush().await?;
    let mut response = Vec::new();
    reader.read_until(b'\n', &mut response).await?;
    if response.len() > 1024 * 1024 {
        bail!("broker lease response exceeds size limit");
    }
    #[derive(Deserialize)]
    struct LeaseResponse {
        result: String,
        #[serde(default)]
        code: String,
        #[serde(default)]
        message: String,
        #[serde(default)]
        lease_id: String,
        #[serde(default)]
        epoch_id: String,
        #[serde(default)]
        expires_unix_ms: u64,
        #[serde(default)]
        key_version: u32,
        #[serde(default)]
        capsule_generation: u64,
        #[serde(default)]
        key: String,
    }
    let response: LeaseResponse = serde_json::from_slice(&response)?;
    if response.result == "error" {
        bail!(
            "key broker rejected metadata lease ({}): {}",
            response.code,
            response.message
        );
    }
    if response.result != "lease"
        || response.lease_id.is_empty()
        || response.epoch_id.is_empty()
        || response.key_version == 0
        || response.capsule_generation == 0
    {
        bail!("invalid key broker metadata lease response");
    }
    let key = Zeroizing::new(
        BASE64
            .decode(&response.key)
            .context("decode leased metadata DEK")?,
    );
    if key.len() != 32 {
        bail!("broker metadata DEK has an invalid length");
    }
    let (disconnected_sender, disconnected) = watch::channel(false);
    tokio::spawn(async move {
        let mut byte = [0_u8; 1];
        let _ = tokio::io::AsyncReadExt::read(&mut reader, &mut byte).await;
        let _ = disconnected_sender.send(true);
    });
    Ok((
        BrokerLeaseConnection {
            _connection: writer,
            disconnected,
            lease_id: response.lease_id,
            epoch_id: response.epoch_id,
            expires_unix_ms: response.expires_unix_ms,
            key_version: response.key_version,
            capsule_generation: response.capsule_generation,
        },
        key,
    ))
}

impl Drop for KeyBroker {
    fn drop(&mut self) {
        self.lock();
        if self.identity_locked {
            unlock_memory(self.identity.as_bytes());
            self.identity_locked = false;
        }
    }
}

fn protect_recovered_keys(keys: &RecoveredKeys) -> Result<()> {
    lock_memory(keys.metadata_dek.as_slice())?;
    if let Err(error) = lock_memory(keys.repository_master_key.as_slice()) {
        unlock_memory(keys.metadata_dek.as_slice());
        return Err(error);
    }
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(
            keys.metadata_dek.as_ptr().cast_mut().cast(),
            keys.metadata_dek.len(),
            libc::MADV_DONTDUMP,
        );
        libc::madvise(
            keys.repository_master_key.as_ptr().cast_mut().cast(),
            keys.repository_master_key.len(),
            libc::MADV_DONTDUMP,
        );
    }
    Ok(())
}

fn lock_memory(secret: &[u8]) -> Result<()> {
    if secret.is_empty() {
        bail!("refusing to protect empty secret memory");
    }
    let result = unsafe { libc::mlock(secret.as_ptr().cast(), secret.len()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("lock broker secret memory");
    }
    Ok(())
}

fn unlock_memory(secret: &[u8]) {
    if !secret.is_empty() {
        unsafe {
            libc::munlock(secret.as_ptr().cast(), secret.len());
        }
    }
}

pub fn encrypt_offline_contribution(
    capsule: &RecoveryCapsule,
    session: &SignedSession,
    endpoint_binding: &str,
    member_id: &str,
    credential: &MemberCredential<'_>,
    last_seen_generation: u64,
    principal_id: Option<String>,
    now_unix_ms: u64,
) -> Result<EncryptedContribution> {
    verify_session(capsule, session, endpoint_binding, now_unix_ms)?;
    encrypt_offline_contribution_inner(
        capsule,
        session,
        member_id,
        credential,
        last_seen_generation,
        principal_id,
        false,
    )
}

pub fn encrypt_offline_contribution_unverified(
    capsule: &RecoveryCapsule,
    session: &SignedSession,
    endpoint_binding: &str,
    member_id: &str,
    credential: &MemberCredential<'_>,
    last_seen_generation: u64,
    principal_id: Option<String>,
    now_unix_ms: u64,
) -> Result<EncryptedContribution> {
    verify_unverified_session(capsule, session, endpoint_binding, now_unix_ms)?;
    encrypt_offline_contribution_inner(
        capsule,
        session,
        member_id,
        credential,
        last_seen_generation,
        principal_id,
        true,
    )
}

fn encrypt_offline_contribution_inner(
    capsule: &RecoveryCapsule,
    session: &SignedSession,
    member_id: &str,
    credential: &MemberCredential<'_>,
    last_seen_generation: u64,
    principal_id: Option<String>,
    unverified_session_acknowledged: bool,
) -> Result<EncryptedContribution> {
    let share = capsule.unwrap_offline_member(member_id, credential)?;
    let payload = ContributionPayload {
        member_id: share.member_id,
        share_index: share.share_index,
        share: share.plaintext.to_vec(),
        last_seen_generation,
        principal_id,
        unverified_session_acknowledged,
    };
    encrypt_contribution_payload(session, &payload)
}

fn encrypt_contribution_payload(
    session: &SignedSession,
    payload: &ContributionPayload,
) -> Result<EncryptedContribution> {
    let mut plaintext = Zeroizing::new(serde_json::to_vec(&payload)?);
    let transcript = encode_transcript(&session.transcript)?;
    let public_key_bytes = BASE64.decode(&session.transcript.hpke_public_key)?;
    let public_key = <SessionKem as KemTrait>::PublicKey::from_bytes(&public_key_bytes)
        .map_err(|_| anyhow::anyhow!("invalid session HPKE public key"))?;
    let mut rng = StdRng::from_os_rng();
    let (encapped_key, mut sender) = hpke::setup_sender::<SessionAead, SessionKdf, SessionKem, _>(
        &OpModeS::Base,
        &public_key,
        SESSION_INFO,
        &mut rng,
    )
    .map_err(|_| anyhow::anyhow!("initialize contribution HPKE sender"))?;
    let tag = sender
        .seal_in_place_detached(plaintext.as_mut(), &transcript)
        .map_err(|_| anyhow::anyhow!("encrypt contribution"))?;
    Ok(EncryptedContribution {
        session_id: session.transcript.session_id.clone(),
        encapped_key: BASE64.encode(encapped_key.to_bytes()),
        ciphertext: BASE64.encode(plaintext.as_slice()),
        tag: BASE64.encode(tag.to_bytes()),
    })
}

pub fn verify_session(
    capsule: &RecoveryCapsule,
    session: &SignedSession,
    endpoint_binding: &str,
    now_unix_ms: u64,
) -> Result<()> {
    verify_session_transcript(capsule, session, endpoint_binding, now_unix_ms)?;
    if session.transcript.identity_recovery {
        bail!("identity-recovery session requires explicit unverified-session acknowledgement");
    }
    let public_key_bytes = BASE64.decode(&capsule.header.broker_identity_public_key)?;
    let public_key = VerifyingKey::from_bytes(
        &public_key_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid broker identity public key length"))?,
    )?;
    let signature_bytes = BASE64.decode(&session.signature)?;
    let signature = Signature::from_slice(&signature_bytes)?;
    public_key.verify(&encode_transcript(&session.transcript)?, &signature)?;
    Ok(())
}

pub fn verify_unverified_session(
    capsule: &RecoveryCapsule,
    session: &SignedSession,
    endpoint_binding: &str,
    now_unix_ms: u64,
) -> Result<()> {
    verify_session_transcript(capsule, session, endpoint_binding, now_unix_ms)?;
    if !session.transcript.identity_recovery {
        bail!("normal signed session cannot use unverified-session acknowledgement");
    }
    Ok(())
}

fn verify_session_transcript(
    capsule: &RecoveryCapsule,
    session: &SignedSession,
    endpoint_binding: &str,
    now_unix_ms: u64,
) -> Result<()> {
    capsule.validate()?;
    let transcript = &session.transcript;
    if transcript.protocol != "vaultic-key-broker.v1"
        || transcript.repository_id != capsule.header.repository_id
        || transcript.capsule_generation != capsule.header.generation
        || transcript.endpoint_binding != endpoint_binding
        || transcript.expires_unix_ms <= now_unix_ms
        || transcript.session_id.is_empty()
    {
        bail!("unlock session transcript does not match capsule or endpoint");
    }
    let encoded = encode_transcript(transcript)?;
    if session.fingerprint != session_fingerprint(&encoded) {
        bail!("unlock session fingerprint mismatch");
    }
    Ok(())
}

fn decrypt_contribution(
    session: &SessionState,
    contribution: &EncryptedContribution,
) -> Result<ContributionPayload> {
    let encapped_bytes = BASE64
        .decode(&contribution.encapped_key)
        .map_err(|_| ContributionRejection::PayloadInvalid)?;
    if encapped_bytes.len() != 32 {
        return Err(ContributionRejection::PayloadInvalid.into());
    }
    let encapped_key = <SessionKem as KemTrait>::EncappedKey::from_bytes(&encapped_bytes)
        .map_err(|_| ContributionRejection::PayloadInvalid)?;
    let tag_bytes = BASE64
        .decode(&contribution.tag)
        .map_err(|_| ContributionRejection::PayloadInvalid)?;
    if tag_bytes.len() != 16 {
        return Err(ContributionRejection::PayloadInvalid.into());
    }
    let tag = AeadTag::<SessionAead>::from_bytes(&tag_bytes)
        .map_err(|_| ContributionRejection::PayloadInvalid)?;
    let mut ciphertext = Zeroizing::new(
        BASE64
            .decode(&contribution.ciphertext)
            .map_err(|_| ContributionRejection::PayloadInvalid)?,
    );
    let transcript = encode_transcript(&session.signed.transcript)?;
    let mut receiver = hpke::setup_receiver::<SessionAead, SessionKdf, SessionKem>(
        &OpModeR::Base,
        &session.private_key,
        &encapped_key,
        SESSION_INFO,
    )
    .map_err(|_| ContributionRejection::PayloadInvalid)?;
    receiver
        .open_in_place_detached(ciphertext.as_mut(), &transcript, &tag)
        .map_err(|_| ContributionRejection::PayloadAuthentication)?;
    serde_json::from_slice(&ciphertext).map_err(|_| ContributionRejection::PayloadInvalid.into())
}

mod base64_bytes {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        BASE64
            .decode(String::deserialize(deserializer)?)
            .map_err(D::Error::custom)
    }
}

fn encode_transcript(transcript: &SessionTranscript) -> Result<Vec<u8>> {
    serde_json::to_vec(transcript).context("encode unlock session transcript")
}

fn release_manifest(client: &ClientIdentity) -> Result<Vec<u8>> {
    if client.component.is_empty()
        || client.release_identity.is_empty()
        || client.executable_sha256.len() != 64
        || !client
            .executable_sha256
            .bytes()
            .all(|value| value.is_ascii_hexdigit())
    {
        bail!("invalid client release manifest");
    }
    serde_json::to_vec(&(
        "vaultic-client-release-v1",
        &client.component,
        client.version,
        &client.executable_sha256,
        &client.release_identity,
    ))
    .context("encode client release manifest")
}

fn session_fingerprint(transcript: &[u8]) -> String {
    let digest = Sha256::digest(transcript);
    digest[..16]
        .chunks(2)
        .map(|chunk| format!("{:02X}{:02X}", chunk[0], chunk[1]))
        .collect::<Vec<_>>()
        .join("-")
}

fn random_id(rng: &mut impl RngCore) -> String {
    let mut bytes = [0_u8; 16];
    rng.fill_bytes(&mut bytes);
    BASE64.encode(bytes)
}

pub fn unix_time_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
