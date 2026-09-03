use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hpke::{
    aead::{AeadTag, ChaCha20Poly1305},
    kdf::HkdfSha256,
    kem::X25519HkdfSha256,
    Deserializable, Kem as KemTrait, OpModeR, OpModeS, Serializable,
};
use rand::{rngs::StdRng, RngCore, SeedableRng};
#[cfg(test)]
use rand08::rngs::OsRng as LegacyOsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::watch,
};

use crate::encryption::recovery_capsule::{
    MemberCredential, RecoveredKeys, RecoveryCapsule, UnwrappedMemberShare,
};

type SessionKem = X25519HkdfSha256;
type SessionKdf = HkdfSha256;
type SessionAead = ChaCha20Poly1305;

const SESSION_INFO: &[u8] = b"vaultic-key-broker-contribution-v1";
const MAX_SESSION_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_ACTIVE_SESSIONS: usize = 64;
const MAX_LEASE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum Capability {
    MetadataDek,
    RepositoryMasterKey,
    MetadataLossRecovery,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionTranscript {
    pub protocol: String,
    pub session_id: String,
    pub repository_id: String,
    pub capsule_generation: u64,
    pub endpoint_binding: String,
    pub nonce: String,
    pub expires_unix_ms: u64,
    pub hpke_public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedSession {
    pub transcript: SessionTranscript,
    pub signature: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EncryptedContribution {
    pub session_id: String,
    pub encapped_key: String,
    pub ciphertext: String,
    pub tag: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContributionPayload {
    member_id: String,
    share_index: u8,
    share: Vec<u8>,
    last_seen_generation: u64,
    principal_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlockStatus {
    pub locked: bool,
    pub repository_id: String,
    pub capsule_generation: u64,
    pub epoch_id: Option<String>,
    pub active_sessions: usize,
    pub active_leases: usize,
    pub minimum_custodians: usize,
    pub principal_verified: bool,
    pub hardware_verified: bool,
    pub custody_assumed: bool,
    pub compliant: bool,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    pub connection_id: String,
    pub component: String,
    pub version: u64,
    pub release_identity: String,
    pub executable_sha256: String,
    pub release_signature: String,
    pub peer_uid: u32,
    pub executable_owned_by_root: bool,
    pub installation_path_read_only: bool,
}

#[derive(Debug, Clone)]
pub struct ClientAuthorization {
    pub component: String,
    pub minimum_version: u64,
    pub maximum_version: u64,
    pub release_identity: String,
    pub release_public_key: [u8; 32],
    pub peer_uid: u32,
    pub capabilities: BTreeSet<Capability>,
}

#[derive(Debug)]
pub struct KeyLease {
    pub lease_id: String,
    pub epoch_id: String,
    pub capability: Capability,
    pub expires_unix_ms: u64,
    pub key_version: u32,
    pub capsule_generation: u64,
    pub key: Zeroizing<Vec<u8>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub component: String,
    pub version: u64,
    pub release_identity: String,
    pub executable_sha256: String,
    pub signature: String,
}

pub struct BrokerLeaseConnection {
    _connection: tokio::net::unix::OwnedWriteHalf,
    disconnected: watch::Receiver<bool>,
    pub lease_id: String,
    pub epoch_id: String,
    pub expires_unix_ms: u64,
    pub key_version: u32,
    pub capsule_generation: u64,
}

impl BrokerLeaseConnection {
    pub fn disconnected(&self) -> watch::Receiver<bool> {
        self.disconnected.clone()
    }
}

struct SessionState {
    signed: SignedSession,
    private_key: <SessionKem as KemTrait>::PrivateKey,
    contributions: Vec<UnwrappedMemberShare>,
    member_ids: BTreeSet<String>,
    share_indexes: BTreeSet<(String, u8)>,
    principal_ids: BTreeSet<String>,
}

struct UnlockEpoch {
    id: String,
    expires_unix_ms: Option<u64>,
    keys: RecoveredKeys,
}

struct LeaseState {
    epoch_id: String,
    connection_id: String,
    expires_unix_ms: u64,
}

pub struct KeyBroker {
    capsule: RecoveryCapsule,
    identity: SigningKey,
    sessions: BTreeMap<String, SessionState>,
    epoch: Option<UnlockEpoch>,
    leases: BTreeMap<String, LeaseState>,
    authorizations: Vec<ClientAuthorization>,
    maximum_unlocked_lifetime: Option<Duration>,
    identity_locked: bool,
}

impl KeyBroker {
    pub fn new(
        capsule: RecoveryCapsule,
        identity: SigningKey,
        authorizations: Vec<ClientAuthorization>,
        maximum_unlocked_lifetime: Option<Duration>,
    ) -> Result<Self> {
        capsule.validate()?;
        let pinned = BASE64
            .decode(&capsule.header.broker_identity_public_key)
            .context("decode pinned broker identity")?;
        if pinned.as_slice() != identity.verifying_key().as_bytes() {
            bail!("broker identity does not match capsule pin");
        }
        if maximum_unlocked_lifetime == Some(Duration::ZERO) {
            bail!("maximum unlocked lifetime must not be zero");
        }
        lock_memory(identity.as_bytes())?;
        Ok(Self {
            capsule,
            identity,
            sessions: BTreeMap::new(),
            epoch: None,
            leases: BTreeMap::new(),
            authorizations,
            maximum_unlocked_lifetime,
            identity_locked: true,
        })
    }

    pub fn status(&mut self, now_unix_ms: u64) -> UnlockStatus {
        self.expire(now_unix_ms);
        let policy = self
            .capsule
            .effective_policy_status()
            .expect("validated capsule policy");
        UnlockStatus {
            locked: self.epoch.is_none(),
            repository_id: self.capsule.header.repository_id.clone(),
            capsule_generation: self.capsule.header.generation,
            epoch_id: self.epoch.as_ref().map(|epoch| epoch.id.clone()),
            active_sessions: self.sessions.len(),
            active_leases: self.leases.len(),
            minimum_custodians: policy.minimum_custodians,
            principal_verified: policy.principal_verified,
            hardware_verified: policy.hardware_verified,
            custody_assumed: policy.custody_assumed,
            compliant: policy.compliant,
            findings: policy.findings,
        }
    }

    pub fn create_session(
        &mut self,
        endpoint_binding: &str,
        ttl: Duration,
        now_unix_ms: u64,
    ) -> Result<SignedSession> {
        self.expire(now_unix_ms);
        if self.epoch.is_some() {
            bail!("broker is already unlocked");
        }
        if endpoint_binding.is_empty() || ttl.is_zero() || ttl > MAX_SESSION_TTL {
            bail!("invalid unlock session endpoint or lifetime");
        }
        if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
            bail!("too many active unlock sessions");
        }
        let mut rng = StdRng::from_os_rng();
        let (private_key, public_key) = SessionKem::gen_keypair(&mut rng);
        let session_id = random_id(&mut rng);
        let transcript = SessionTranscript {
            protocol: "vaultic-key-broker.v1".to_owned(),
            session_id: session_id.clone(),
            repository_id: self.capsule.header.repository_id.clone(),
            capsule_generation: self.capsule.header.generation,
            endpoint_binding: endpoint_binding.to_owned(),
            nonce: random_id(&mut rng),
            expires_unix_ms: now_unix_ms
                .checked_add(u64::try_from(ttl.as_millis())?)
                .context("session expiry overflow")?,
            hpke_public_key: BASE64.encode(public_key.to_bytes()),
        };
        let transcript_bytes = encode_transcript(&transcript)?;
        let signature = self.identity.sign(&transcript_bytes);
        let signed = SignedSession {
            fingerprint: session_fingerprint(&transcript_bytes),
            transcript,
            signature: BASE64.encode(signature.to_bytes()),
        };
        self.sessions.insert(
            session_id,
            SessionState {
                signed: signed.clone(),
                private_key,
                contributions: Vec::new(),
                member_ids: BTreeSet::new(),
                share_indexes: BTreeSet::new(),
                principal_ids: BTreeSet::new(),
            },
        );
        Ok(signed)
    }

    pub fn submit_contribution(
        &mut self,
        contribution: EncryptedContribution,
        now_unix_ms: u64,
    ) -> Result<bool> {
        self.expire(now_unix_ms);
        let session = self
            .sessions
            .get_mut(&contribution.session_id)
            .context("unknown or expired unlock session")?;
        if now_unix_ms >= session.signed.transcript.expires_unix_ms {
            bail!("unlock session has expired");
        }
        let payload = decrypt_contribution(session, &contribution)?;
        if payload.last_seen_generation > self.capsule.header.generation {
            bail!("custodian generation attestation rejects capsule rollback");
        }
        let member = self
            .capsule
            .members
            .iter()
            .find(|member| member.member_id == payload.member_id)
            .context("contribution references unknown capsule member")?;
        if member.share_index != payload.share_index
            || !session.member_ids.insert(payload.member_id.clone())
            || !session
                .share_indexes
                .insert((member.group_id.clone(), payload.share_index))
        {
            bail!("duplicate or re-indexed contribution");
        }
        if let Some(principal_id) = payload.principal_id {
            if principal_id.is_empty() || !session.principal_ids.insert(principal_id) {
                bail!("duplicate or invalid contributing principal");
            }
        }
        session.contributions.push(UnwrappedMemberShare {
            member_id: payload.member_id,
            share_index: payload.share_index,
            plaintext: Zeroizing::new(payload.share),
        });

        let Ok(keys) = self.capsule.recover_from_shares(&session.contributions) else {
            return Ok(false);
        };
        protect_recovered_keys(&keys)?;
        let epoch_id = random_id(&mut rand::rng());
        let expires_unix_ms = self
            .maximum_unlocked_lifetime
            .map(|ttl| now_unix_ms.saturating_add(ttl.as_millis() as u64));
        self.epoch = Some(UnlockEpoch {
            id: epoch_id,
            expires_unix_ms,
            keys,
        });
        self.close_all_sessions();
        Ok(true)
    }

    pub fn acquire_lease(
        &mut self,
        client: &ClientIdentity,
        capability: Capability,
        ttl: Duration,
        now_unix_ms: u64,
    ) -> Result<KeyLease> {
        self.expire(now_unix_ms);
        if ttl.is_zero() || ttl > MAX_LEASE_TTL || client.connection_id.is_empty() {
            bail!("invalid lease request");
        }
        self.authorize(client, capability)?;
        let epoch = self.epoch.as_ref().context("broker is locked")?;
        let expires_unix_ms = now_unix_ms
            .checked_add(u64::try_from(ttl.as_millis())?)
            .context("lease expiry overflow")?;
        let key = match capability {
            Capability::MetadataDek => epoch.keys.metadata_dek.as_slice(),
            Capability::RepositoryMasterKey | Capability::MetadataLossRecovery => {
                epoch.keys.repository_master_key.as_slice()
            }
        };
        let lease_id = random_id(&mut rand::rng());
        self.leases.insert(
            lease_id.clone(),
            LeaseState {
                epoch_id: epoch.id.clone(),
                connection_id: client.connection_id.clone(),
                expires_unix_ms,
            },
        );
        Ok(KeyLease {
            lease_id,
            epoch_id: epoch.id.clone(),
            capability,
            expires_unix_ms,
            key_version: match capability {
                Capability::MetadataDek => self.capsule.header.metadata_dek_version,
                Capability::RepositoryMasterKey | Capability::MetadataLossRecovery => {
                    self.capsule.header.repository_key_version
                }
            },
            capsule_generation: self.capsule.header.generation,
            key: Zeroizing::new(key.to_vec()),
        })
    }

    pub fn release_lease(&mut self, lease_id: &str, connection_id: &str) -> Result<()> {
        let lease = self.leases.get(lease_id).context("unknown lease")?;
        if lease.connection_id != connection_id {
            bail!("lease is bound to another connection");
        }
        self.leases.remove(lease_id);
        Ok(())
    }

    pub fn disconnect(&mut self, connection_id: &str) {
        self.leases
            .retain(|_, lease| lease.connection_id != connection_id);
    }

    pub fn lock(&mut self) {
        self.close_all_sessions();
        self.leases.clear();
        if let Some(epoch) = self.epoch.take() {
            unlock_memory(epoch.keys.metadata_dek.as_slice());
            unlock_memory(epoch.keys.repository_master_key.as_slice());
        }
    }

    fn authorize(&self, client: &ClientIdentity, capability: Capability) -> Result<()> {
        if !client.executable_owned_by_root || !client.installation_path_read_only {
            bail!("client executable ownership or installation path is not trusted");
        }
        let authorization = self.authorizations.iter().find(|authorization| {
            authorization.component == client.component
                && authorization.release_identity == client.release_identity
                && authorization.peer_uid == client.peer_uid
                && client.version >= authorization.minimum_version
                && client.version <= authorization.maximum_version
                && authorization.capabilities.contains(&capability)
        });
        let Some(authorization) = authorization else {
            bail!("client identity, version, or capability is not authorized");
        };
        let manifest = release_manifest(client)?;
        let signature = Signature::from_slice(
            &BASE64
                .decode(&client.release_signature)
                .context("decode client release signature")?,
        )?;
        VerifyingKey::from_bytes(&authorization.release_public_key)?
            .verify(&manifest, &signature)
            .context("client release signature verification failed")?;
        Ok(())
    }

    fn expire(&mut self, now_unix_ms: u64) {
        let expired_sessions = self
            .sessions
            .iter()
            .filter(|(_, session)| session.signed.transcript.expires_unix_ms <= now_unix_ms)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for session_id in expired_sessions {
            self.sessions.remove(&session_id);
        }
        if self
            .epoch
            .as_ref()
            .and_then(|epoch| epoch.expires_unix_ms)
            .is_some_and(|expiry| expiry <= now_unix_ms)
        {
            self.lock();
            return;
        }
        self.leases.retain(|_, lease| {
            lease.expires_unix_ms > now_unix_ms
                && self
                    .epoch
                    .as_ref()
                    .is_some_and(|epoch| epoch.id == lease.epoch_id)
        });
    }

    fn close_all_sessions(&mut self) {
        self.sessions.clear();
    }
}

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
    let request = serde_json::json!({
        "operation": "acquire_lease",
        "component": manifest.component,
        "version": manifest.version,
        "release_identity": manifest.release_identity,
        "release_signature": manifest.signature,
        "capability": "metadata-dek",
        "ttl_seconds": ttl.as_secs(),
    });
    let mut request = serde_json::to_vec(&request)?;
    request.push(b'\n');
    writer.write_all(&request).await?;
    writer.flush().await?;
    let mut reader = BufReader::new(reader);
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
        bail!("key broker rejected metadata lease ({}): {}", response.code, response.message);
    }
    if response.result != "lease"
        || response.lease_id.is_empty()
        || response.epoch_id.is_empty()
        || response.key_version == 0
        || response.capsule_generation == 0
    {
        bail!("invalid key broker metadata lease response");
    }
    let key = Zeroizing::new(BASE64.decode(&response.key).context("decode leased metadata DEK")?);
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
    let share = capsule.unwrap_offline_member(member_id, credential)?;
    let payload = ContributionPayload {
        member_id: share.member_id,
        share_index: share.share_index,
        share: share.plaintext.to_vec(),
        last_seen_generation,
        principal_id,
    };
    let mut plaintext = Zeroizing::new(serde_json::to_vec(&payload)?);
    let transcript = encode_transcript(&session.transcript)?;
    let public_key_bytes = BASE64.decode(&session.transcript.hpke_public_key)?;
    let public_key = <SessionKem as KemTrait>::PublicKey::from_bytes(&public_key_bytes)
        .map_err(|_| anyhow::anyhow!("invalid session HPKE public key"))?;
    let mut rng = StdRng::from_os_rng();
    let (encapped_key, mut sender) = hpke::setup_sender::<
        SessionAead,
        SessionKdf,
        SessionKem,
        _,
    >(&OpModeS::Base, &public_key, SESSION_INFO, &mut rng)
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
    let public_key_bytes = BASE64.decode(&capsule.header.broker_identity_public_key)?;
    let public_key = VerifyingKey::from_bytes(
        &public_key_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid broker identity public key length"))?,
    )?;
    let signature_bytes = BASE64.decode(&session.signature)?;
    let signature = Signature::from_slice(&signature_bytes)?;
    let encoded = encode_transcript(transcript)?;
    public_key.verify(&encoded, &signature)?;
    if session.fingerprint != session_fingerprint(&encoded) {
        bail!("unlock session fingerprint mismatch");
    }
    Ok(())
}

fn decrypt_contribution(
    session: &SessionState,
    contribution: &EncryptedContribution,
) -> Result<ContributionPayload> {
    let encapped_bytes = BASE64.decode(&contribution.encapped_key)?;
    if encapped_bytes.len() != 32 {
        bail!("invalid HPKE encapsulated key length");
    }
    let encapped_key = <SessionKem as KemTrait>::EncappedKey::from_bytes(&encapped_bytes)
        .map_err(|_| anyhow::anyhow!("invalid HPKE encapsulated key"))?;
    let tag_bytes = BASE64.decode(&contribution.tag)?;
    if tag_bytes.len() != 16 {
        bail!("invalid HPKE authentication tag length");
    }
    let tag = AeadTag::<SessionAead>::from_bytes(&tag_bytes)
        .map_err(|_| anyhow::anyhow!("invalid HPKE authentication tag"))?;
    let mut ciphertext = Zeroizing::new(BASE64.decode(&contribution.ciphertext)?);
    let transcript = encode_transcript(&session.signed.transcript)?;
    let mut receiver = hpke::setup_receiver::<SessionAead, SessionKdf, SessionKem>(
        &OpModeR::Base,
        &session.private_key,
        &encapped_key,
        SESSION_INFO,
    )
    .map_err(|_| anyhow::anyhow!("initialize contribution HPKE receiver"))?;
    receiver
        .open_in_place_detached(ciphertext.as_mut(), &transcript, &tag)
        .map_err(|_| anyhow::anyhow!("contribution authentication failed"))?;
    serde_json::from_slice(&ciphertext).context("decode contribution payload")
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
    digest[..10]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::recovery_capsule::CapsuleBuilder;

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
            capabilities: BTreeSet::from([Capability::MetadataDek]),
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

    #[test]
    fn active_session_capacity_is_bounded_and_reclaimed_after_expiry() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule, identity, authorizations, None).unwrap();
        for _ in 0..MAX_ACTIVE_SESSIONS {
            broker
                .create_session("unix:/run/vaultic/broker.sock", Duration::from_secs(1), 1_000)
                .unwrap();
        }
        assert!(broker
            .create_session("unix:/run/vaultic/broker.sock", Duration::from_secs(1), 1_000)
            .is_err());
        assert!(broker
            .create_session("unix:/run/vaultic/broker.sock", Duration::from_secs(1), 2_000)
            .is_ok());
    }

    #[test]
    fn signed_hpke_quorum_unlocks_and_leases_are_scoped() {
        let (capsule, identity, authorizations) = setup();
        let mut broker = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        assert!(broker.status(1_000).locked);
        let session = broker
            .create_session("unix:/run/vaultic/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        for (member, credential, unlocked) in [
            ("alice", MemberCredential::Passphrase(b"alice passphrase"), false),
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
            assert_eq!(broker.submit_contribution(contribution, 1_002).unwrap(), unlocked);
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
        broker.lock();
        assert!(broker.status(1_006).locked);
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
        assert!(broker.submit_contribution(rollback, 2_002).is_err());

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
        assert!(!broker.submit_contribution(contribution.clone(), 2_002).unwrap());
        assert!(broker.submit_contribution(contribution, 2_003).is_err());
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