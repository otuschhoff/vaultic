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
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::watch,
};
use zeroize::Zeroizing;

use crate::encryption::recovery_capsule::{
    validate_shamir_share, CapsuleBuilder, MemberCredential, MemberProtection, RecoveredKeys,
    RecoveryCapsule, UnlockPolicy, UnwrappedMemberShare,
};

type SessionKem = X25519HkdfSha256;
type SessionKdf = HkdfSha256;
type SessionAead = ChaCha20Poly1305;

const SESSION_INFO: &[u8] = b"vaultic-key-broker-contribution-v1";
const MAX_SESSION_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_ACTIVE_SESSIONS: usize = 64;
const MAX_LEASE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, thiserror::Error)]
pub enum ContributionRejection {
    #[error("contribution payload authentication failed")]
    PayloadAuthentication,
    #[error("contribution payload is malformed")]
    PayloadInvalid,
    #[error("custodian generation attestation rejects capsule rollback")]
    Rollback {
        last_seen_generation: u64,
        current_generation: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum Capability {
    MetadataDek,
    RepositoryMasterKey,
    MetadataLossRecovery,
    PolicyMutation,
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
    pub identity_recovery: bool,
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
    #[serde(with = "base64_bytes")]
    share: Vec<u8>,
    last_seen_generation: u64,
    principal_id: Option<String>,
    #[serde(default)]
    unverified_session_acknowledged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlockStatus {
    pub locked: bool,
    pub repository_id: String,
    pub capsule_generation: u64,
    pub capsule_logical_id: String,
    pub policy_hash: String,
    pub epoch_id: Option<String>,
    pub active_sessions: usize,
    pub active_leases: usize,
    pub minimum_custodians: usize,
    pub principal_verified: bool,
    pub hardware_verified: bool,
    pub custody_assumed: bool,
    pub compliant: bool,
    pub findings: Vec<String>,
    pub policy_mutation_pending: bool,
    pub pending_capsule_generation: Option<u64>,
    pub pending_capsule_sha256: Option<String>,
    pub identity_recovery: bool,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct ExpirationSummary {
    pub expired_sessions: usize,
    pub expired_leases: usize,
    pub automatic_lock: bool,
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

struct PendingPolicyMutation {
    capsule: RecoveryCapsule,
    digest: String,
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
    pending_policy_mutation: Option<PendingPolicyMutation>,
    identity_recovery: bool,
}

impl KeyBroker {
    pub fn new(
        capsule: RecoveryCapsule,
        identity: SigningKey,
        authorizations: Vec<ClientAuthorization>,
        maximum_unlocked_lifetime: Option<Duration>,
    ) -> Result<Self> {
        Self::new_with_identity_mode(
            capsule,
            identity,
            authorizations,
            maximum_unlocked_lifetime,
            false,
        )
    }

    pub fn new_identity_recovery(
        capsule: RecoveryCapsule,
        identity: SigningKey,
        authorizations: Vec<ClientAuthorization>,
        maximum_unlocked_lifetime: Option<Duration>,
    ) -> Result<Self> {
        Self::new_with_identity_mode(
            capsule,
            identity,
            authorizations,
            maximum_unlocked_lifetime,
            true,
        )
    }

    fn new_with_identity_mode(
        capsule: RecoveryCapsule,
        identity: SigningKey,
        authorizations: Vec<ClientAuthorization>,
        maximum_unlocked_lifetime: Option<Duration>,
        identity_recovery: bool,
    ) -> Result<Self> {
        capsule.validate()?;
        let pinned = BASE64
            .decode(&capsule.header.broker_identity_public_key)
            .context("decode pinned broker identity")?;
        let identity_matches = pinned.as_slice() == identity.verifying_key().as_bytes();
        if !identity_recovery && !identity_matches {
            bail!("broker identity does not match capsule pin");
        }
        if identity_recovery && identity_matches {
            bail!("identity recovery requires a fresh broker identity");
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
            pending_policy_mutation: None,
            identity_recovery,
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
            capsule_logical_id: self.capsule.header.logical_id.clone(),
            policy_hash: self.capsule.header.policy_hash.clone(),
            epoch_id: self.epoch.as_ref().map(|epoch| epoch.id.clone()),
            active_sessions: self.sessions.len(),
            active_leases: self.leases.len(),
            minimum_custodians: policy.minimum_custodians,
            principal_verified: policy.principal_verified,
            hardware_verified: policy.hardware_verified,
            custody_assumed: policy.custody_assumed,
            compliant: policy.compliant,
            findings: policy.findings,
            policy_mutation_pending: self.pending_policy_mutation.is_some(),
            pending_capsule_generation: self
                .pending_policy_mutation
                .as_ref()
                .map(|pending| pending.capsule.header.generation),
            pending_capsule_sha256: self
                .pending_policy_mutation
                .as_ref()
                .map(|pending| pending.digest.clone()),
            identity_recovery: self.identity_recovery,
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
            identity_recovery: self.identity_recovery,
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
        if payload.unverified_session_acknowledged != self.identity_recovery {
            bail!("identity-recovery contribution acknowledgement does not match broker mode");
        }
        if payload.last_seen_generation > self.capsule.header.generation {
            return Err(ContributionRejection::Rollback {
                last_seen_generation: payload.last_seen_generation,
                current_generation: self.capsule.header.generation,
            }
            .into());
        }
        let member = self
            .capsule
            .members
            .iter()
            .find(|member| member.member_id == payload.member_id)
            .context("contribution references unknown capsule member")?;
        let share_identity = (member.group_id.clone(), payload.share_index);
        if member.share_index != payload.share_index
            || session.member_ids.contains(&payload.member_id)
            || session.share_indexes.contains(&share_identity)
        {
            bail!("duplicate or re-indexed contribution");
        }
        if let Some(principal_id) = payload.principal_id.as_ref() {
            if principal_id.is_empty() || session.principal_ids.contains(principal_id) {
                bail!("duplicate or invalid contributing principal");
            }
        }
        validate_shamir_share(&payload.share).map_err(|_| ContributionRejection::PayloadInvalid)?;
        let member_id = payload.member_id;
        let principal_id = payload.principal_id;
        let accepted_contribution = UnwrappedMemberShare {
            member_id: member_id.clone(),
            share_index: payload.share_index,
            plaintext: Zeroizing::new(payload.share),
        };
        let mut candidate_contributions = session.contributions.clone();
        candidate_contributions.push(accepted_contribution);

        if !self.capsule.policy_satisfied_by(&candidate_contributions) {
            session.member_ids.insert(member_id);
            session.share_indexes.insert(share_identity);
            if let Some(principal_id) = principal_id {
                session.principal_ids.insert(principal_id);
            }
            session.contributions = candidate_contributions;
            return Ok(false);
        }
        let keys = match self.capsule.recover_from_shares(&candidate_contributions) {
            Ok(keys) => keys,
            Err(error) => {
                self.sessions.remove(&contribution.session_id);
                return Err(error).context(
                    "satisfied unlock policy failed share reconstruction or payload authentication; session closed",
                );
            }
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
        if self.pending_policy_mutation.is_some() {
            bail!("policy mutation is pending publication");
        }
        if self.identity_recovery {
            bail!("key leases are disabled until broker identity is repinned");
        }
        if capability == Capability::PolicyMutation {
            bail!("policy mutation capability cannot issue a key lease");
        }
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
            Capability::PolicyMutation => unreachable!("rejected above"),
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
                Capability::PolicyMutation => unreachable!("rejected above"),
            },
            capsule_generation: self.capsule.header.generation,
            key: Zeroizing::new(key.to_vec()),
        })
    }

    pub fn prepare_offline_policy_mutation(
        &mut self,
        client: &ClientIdentity,
        policy: UnlockPolicy,
        credentials: &[(&str, MemberCredential<'_>)],
        acknowledge_downgrade: bool,
        now_unix_ms: u64,
    ) -> Result<(RecoveryCapsule, String)> {
        self.expire(now_unix_ms);
        self.authorize(client, Capability::PolicyMutation)?;
        if self.pending_policy_mutation.is_some() {
            bail!("a policy mutation is already pending publication");
        }
        let epoch = self.epoch.as_ref().context("broker is locked")?;
        let generation = self
            .capsule
            .header
            .generation
            .checked_add(1)
            .context("capsule generation overflow")?;
        let candidate = CapsuleBuilder::new(&self.capsule.header.repository_id, generation)
            .broker_identity_public_key(self.identity.verifying_key().as_bytes())
            .key_versions(
                self.capsule.header.root_key_version,
                self.capsule.header.metadata_dek_version,
                self.capsule.header.repository_key_version,
            )
            .create_offline_policy(
                policy,
                credentials,
                epoch.keys.metadata_dek.as_slice(),
                epoch.keys.repository_master_key.as_slice(),
            )?;
        self.accept_policy_mutation(candidate, acknowledge_downgrade)
    }

    pub async fn prepare_policy_mutation(
        &mut self,
        client: &ClientIdentity,
        policy: UnlockPolicy,
        protections: &[(&str, MemberProtection<'_>)],
        acknowledge_downgrade: bool,
        now_unix_ms: u64,
    ) -> Result<(RecoveryCapsule, String)> {
        self.expire(now_unix_ms);
        self.authorize(client, Capability::PolicyMutation)?;
        if self.pending_policy_mutation.is_some() {
            bail!("a policy mutation is already pending publication");
        }
        let epoch = self.epoch.as_ref().context("broker is locked")?;
        let generation = self
            .capsule
            .header
            .generation
            .checked_add(1)
            .context("capsule generation overflow")?;
        let candidate = CapsuleBuilder::new(&self.capsule.header.repository_id, generation)
            .broker_identity_public_key(self.identity.verifying_key().as_bytes())
            .key_versions(
                self.capsule.header.root_key_version,
                self.capsule.header.metadata_dek_version,
                self.capsule.header.repository_key_version,
            )
            .create_policy(
                policy,
                protections,
                epoch.keys.metadata_dek.as_slice(),
                epoch.keys.repository_master_key.as_slice(),
            )
            .await?;
        self.accept_policy_mutation(candidate, acknowledge_downgrade)
    }

    fn accept_policy_mutation(
        &mut self,
        candidate: RecoveryCapsule,
        acknowledge_downgrade: bool,
    ) -> Result<(RecoveryCapsule, String)> {
        let current_status = self.capsule.effective_policy_status()?;
        let candidate_status = candidate.effective_policy_status()?;
        let is_downgrade = candidate_status.minimum_custodians < current_status.minimum_custodians
            || (current_status.compliant && !candidate_status.compliant);
        if is_downgrade && !acknowledge_downgrade {
            bail!("policy downgrade requires explicit acknowledgement");
        }
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&candidate)?));
        self.close_all_sessions();
        self.leases.clear();
        self.pending_policy_mutation = Some(PendingPolicyMutation {
            capsule: candidate.clone(),
            digest: digest.clone(),
        });
        Ok((candidate, digest))
    }

    pub fn activate_policy_mutation(
        &mut self,
        client: &ClientIdentity,
        digest: &str,
    ) -> Result<()> {
        self.authorize(client, Capability::PolicyMutation)?;
        let pending = self
            .pending_policy_mutation
            .as_ref()
            .context("no policy mutation is pending publication")?;
        if pending.digest != digest {
            bail!("published capsule digest does not match pending policy mutation");
        }
        let pending = self.pending_policy_mutation.take().expect("checked above");
        self.capsule = pending.capsule;
        self.identity_recovery = false;
        self.lock();
        Ok(())
    }

    pub fn pending_policy_mutation(
        &self,
        client: &ClientIdentity,
    ) -> Result<(RecoveryCapsule, String)> {
        self.authorize(client, Capability::PolicyMutation)?;
        let pending = self
            .pending_policy_mutation
            .as_ref()
            .context("no policy mutation is pending publication")?;
        Ok((pending.capsule.clone(), pending.digest.clone()))
    }

    pub fn cancel_policy_mutation(&mut self) -> Result<()> {
        self.pending_policy_mutation
            .take()
            .context("no policy mutation is pending publication")?;
        Ok(())
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

    pub fn authorize_client(&self, client: &ClientIdentity, capability: Capability) -> Result<()> {
        self.authorize(client, capability)
    }

    pub fn expire_state(&mut self, now_unix_ms: u64) -> ExpirationSummary {
        let leases_before = self.leases.len();
        let expired_sessions = self
            .sessions
            .iter()
            .filter(|(_, session)| session.signed.transcript.expires_unix_ms <= now_unix_ms)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let expired_session_count = expired_sessions.len();
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
            return ExpirationSummary {
                expired_sessions: expired_session_count,
                expired_leases: leases_before,
                automatic_lock: true,
            };
        }
        self.leases.retain(|_, lease| {
            lease.expires_unix_ms > now_unix_ms
                && self
                    .epoch
                    .as_ref()
                    .is_some_and(|epoch| epoch.id == lease.epoch_id)
        });
        ExpirationSummary {
            expired_sessions: expired_session_count,
            expired_leases: leases_before - self.leases.len(),
            automatic_lock: false,
        }
    }

    fn expire(&mut self, now_unix_ms: u64) {
        let _ = self.expire_state(now_unix_ms);
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
