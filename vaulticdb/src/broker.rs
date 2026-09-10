//! Key broker sessions, authorization, contribution handling, and leases.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hkdf::Hkdf;
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
use crate::ids::{MemberId, RepositoryId, SessionId};
use crate::topology::{
    select_static_credential_reference, CredentialPolicy, StorageCredentialTier, StsFallback,
    TopologyDocument, TopologyMutation,
};

pub mod audit;
mod azure_sas;
mod fallback_state;
mod gcp_downscope;
pub mod peer;
pub mod protocol;
pub mod startup;
mod sts;

type SessionKem = X25519HkdfSha256;
type SessionKdf = HkdfSha256;
type SessionAead = ChaCha20Poly1305;

const SESSION_INFO: &[u8] = b"vaultic-key-broker-contribution-v1";
const MAX_SESSION_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_ACTIVE_SESSIONS: usize = 64;
const MAX_LEASE_TTL: Duration = Duration::from_secs(60 * 60);
const STS_ISSUE_TIMEOUT: Duration = Duration::from_secs(15);

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
    TopologyDiscovery,
    TopologyRead,
    CredentialLease,
    MetadataLossRecovery,
    PolicyMutation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionTranscript {
    pub protocol: String,
    pub session_id: SessionId,
    pub repository_id: RepositoryId,
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
    pub session_id: SessionId,
    pub encapped_key: String,
    pub ciphertext: String,
    pub tag: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContributionPayload {
    member_id: MemberId,
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
    pub repository_id: RepositoryId,
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
    pub credential_refs: BTreeSet<String>,
    pub read_only: bool,
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
    pub credential_source: Option<&'static str>,
    pub storage_target: Option<String>,
    pub storage_tier: Option<String>,
    pub provider_expires_at: Option<String>,
    pub static_generation: Option<u64>,
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
    pub credential_source: Option<String>,
    pub storage_target: Option<String>,
    pub storage_tier: Option<String>,
    pub provider_expires_at: Option<String>,
    pub static_generation: Option<u64>,
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
    member_ids: BTreeSet<MemberId>,
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
    sessions: BTreeMap<SessionId, SessionState>,
    epoch: Option<UnlockEpoch>,
    leases: BTreeMap<String, LeaseState>,
    authorizations: Vec<ClientAuthorization>,
    maximum_unlocked_lifetime: Option<Duration>,
    identity_locked: bool,
    pending_policy_mutation: Option<PendingPolicyMutation>,
    identity_recovery: bool,
    issued_static_fallbacks: BTreeSet<(String, u64)>,
    retired_static_fallbacks: BTreeSet<(String, u64)>,
    fallback_state_store: Option<fallback_state::FallbackStateStore>,
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
            None,
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
            None,
        )
    }

    pub fn new_with_fallback_state(
        capsule: RecoveryCapsule,
        identity: SigningKey,
        authorizations: Vec<ClientAuthorization>,
        maximum_unlocked_lifetime: Option<Duration>,
        fallback_state_path: PathBuf,
        identity_recovery: bool,
    ) -> Result<Self> {
        Self::new_with_identity_mode(
            capsule,
            identity,
            authorizations,
            maximum_unlocked_lifetime,
            identity_recovery,
            Some(fallback_state_path),
        )
    }

    fn new_with_identity_mode(
        capsule: RecoveryCapsule,
        identity: SigningKey,
        authorizations: Vec<ClientAuthorization>,
        maximum_unlocked_lifetime: Option<Duration>,
        identity_recovery: bool,
        fallback_state_path: Option<PathBuf>,
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
        let pinned_key: [u8; 32] = pinned
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("pinned broker identity must be 32 bytes"))?;
        let pinned_key = VerifyingKey::from_bytes(&pinned_key)?;
        let (fallback_state_store, issued_static_fallbacks, retired_static_fallbacks) =
            if let Some(path) = fallback_state_path {
                let (store, issued, retired) = fallback_state::FallbackStateStore::load(
                    path,
                    capsule.header.repository_id.as_str(),
                    &capsule.header.logical_id,
                    &pinned_key,
                )?;
                (Some(store), issued, retired)
            } else {
                (None, BTreeSet::new(), BTreeSet::new())
            };
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
            issued_static_fallbacks,
            retired_static_fallbacks,
            fallback_state_store,
        })
    }

    pub fn status(&mut self, now_unix_ms: u64) -> Result<UnlockStatus> {
        self.expire(now_unix_ms);
        let policy = self.capsule.effective_policy_status()?;
        let mut findings = policy.findings;
        if let Some(epoch) = self.epoch.as_ref() {
            let encoded = &epoch.keys.sealed_topology;
            let topology = TopologyDocument::decode(encoded.as_slice())?;
            let now = time::OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(now_unix_ms) * 1_000_000,
            )?;
            for reference in topology
                .credentials
                .iter()
                .filter(|(_, credential)| credential.rotation_overdue(now))
                .map(|(reference, _)| reference)
            {
                findings.push(format!("credential-rotation-overdue: {reference}"));
            }
            for (target, generation) in &self.issued_static_fallbacks {
                let acknowledged = storage_credential_policy(&topology, target)
                    .ok()
                    .and_then(|(configured, _)| configured.r#static.as_ref())
                    .is_some_and(|configured| configured.revoked_generations.contains(generation));
                if !acknowledged {
                    findings.push(format!(
                        "static-fallback-active-provider-authority: {target} generation {generation}"
                    ));
                }
            }
            for (target, generation) in &self.retired_static_fallbacks {
                let acknowledged = storage_credential_policy(&topology, target)
                    .ok()
                    .and_then(|(configured, _)| configured.r#static.as_ref())
                    .is_some_and(|configured| configured.revoked_generations.contains(generation));
                if !acknowledged {
                    findings.push(format!(
                        "static-fallback-provider-revocation-required: {target} generation {generation}"
                    ));
                }
            }
        }
        Ok(UnlockStatus {
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
            compliant: policy.compliant
                && !findings.iter().any(|finding| {
                    finding.starts_with("credential-rotation-overdue:")
                        || finding.starts_with("static-fallback-active-provider-authority:")
                        || finding.starts_with("static-fallback-provider-revocation-required:")
                }),
            findings,
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
        })
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
        let session_id = SessionId::from(random_id(&mut rng));
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
        if capability == Capability::CredentialLease {
            bail!("credential leases require an exact credential reference");
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
            Capability::MetadataDek => Zeroizing::new(epoch.keys.metadata_dek.to_vec()),
            Capability::RepositoryMasterKey | Capability::MetadataLossRecovery => {
                Zeroizing::new(epoch.keys.repository_master_key.to_vec())
            }
            Capability::TopologyDiscovery => {
                let mut key = Zeroizing::new(vec![0_u8; 32]);
                Hkdf::<Sha256>::new(
                    Some(self.capsule.header.repository_id.as_bytes()),
                    epoch.keys.repository_master_key.as_slice(),
                )
                .expand(b"vaultic/bootstrap-topology-v1", key.as_mut_slice())
                .map_err(|_| anyhow::anyhow!("derive topology discovery key"))?;
                key
            }
            Capability::TopologyRead => {
                let encoded = &epoch.keys.sealed_topology;
                Zeroizing::new(TopologyDocument::decode(encoded.as_slice())?.redacted_json()?)
            }
            Capability::CredentialLease | Capability::PolicyMutation => {
                unreachable!("rejected above")
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
                Capability::RepositoryMasterKey
                | Capability::TopologyDiscovery
                | Capability::MetadataLossRecovery
                | Capability::TopologyRead
                | Capability::CredentialLease => self.capsule.header.repository_key_version,
                Capability::PolicyMutation => unreachable!("rejected above"),
            },
            capsule_generation: self.capsule.header.generation,
            key,
            credential_source: None,
            storage_target: None,
            storage_tier: None,
            provider_expires_at: None,
            static_generation: None,
        })
    }

    pub fn acquire_credential_lease(
        &mut self,
        client: &ClientIdentity,
        credential_ref: &str,
        ttl: Duration,
        now_unix_ms: u64,
    ) -> Result<KeyLease> {
        self.issue_credential_lease(client, credential_ref, ttl, now_unix_ms, false)
    }

    pub async fn acquire_storage_credential_lease(
        &mut self,
        client: &ClientIdentity,
        storage_target: &str,
        storage_tier: &str,
        ttl: Duration,
        now_unix_ms: u64,
    ) -> Result<KeyLease> {
        self.expire(now_unix_ms);
        if self.pending_policy_mutation.is_some() || self.identity_recovery {
            bail!("credential leases are unavailable in the current broker state");
        }
        if ttl.is_zero() || ttl > MAX_LEASE_TTL || client.connection_id.is_empty() {
            bail!("invalid lease request");
        }
        self.authorize(client, Capability::CredentialLease)?;
        let authorization = self
            .authorization(client, Capability::CredentialLease)
            .context("credential lease is not authorized")?;
        let tier = match storage_tier {
            "storage-read" => StorageCredentialTier::Read,
            "storage-append" => StorageCredentialTier::Append,
            "storage-maintain" => StorageCredentialTier::Maintain,
            "storage-lock" => StorageCredentialTier::Lock,
            _ => bail!("invalid storage credential tier"),
        };
        if authorization.read_only && tier != StorageCredentialTier::Read {
            bail!("read-only client cannot lease a write-capable storage credential");
        }
        let epoch = self.epoch.as_ref().context("broker is locked")?;
        let topology = TopologyDocument::decode(epoch.keys.sealed_topology.as_slice())?;
        let (policy, endpoint) = storage_credential_policy(&topology, storage_target)?;
        let policy = policy.clone();
        let endpoint = endpoint.clone();
        let mut fell_back = false;
        if let Some(sts_policy) = policy
            .sts
            .as_ref()
            .filter(|configured| configured.roles.reference(tier).is_some())
        {
            let issuer = topology
                .credentials
                .get(&sts_policy.issuer_ref)
                .context("topology has no STS issuer credential")?
                .clone();
            let issued = tokio::time::timeout(
                STS_ISSUE_TIMEOUT,
                sts::issue(&issuer, sts_policy, &endpoint, tier, ttl),
            )
            .await
            .unwrap_or_else(|_| {
                Err(sts::StsIssueError::Unavailable(anyhow::anyhow!(
                    "STS request exceeded {} seconds",
                    STS_ISSUE_TIMEOUT.as_secs()
                )))
            });
            match issued {
                Ok(credential) => {
                    let recovered = self
                        .issued_static_fallbacks
                        .iter()
                        .filter(|(target, _)| target == storage_target)
                        .cloned()
                        .collect::<Vec<_>>();
                    for fallback in &recovered {
                        self.issued_static_fallbacks.remove(fallback);
                        self.retired_static_fallbacks.insert(fallback.clone());
                    }
                    if !recovered.is_empty() {
                        if let Err(error) = self.persist_fallback_state() {
                            for fallback in recovered {
                                self.retired_static_fallbacks.remove(&fallback);
                                self.issued_static_fallbacks.insert(fallback);
                            }
                            return Err(error.context(
                                "record static fallback retirement before issuing STS credential",
                            ));
                        }
                    }
                    let provider_expires_at = credential.expires_at.clone();
                    let key = Zeroizing::new(
                        serde_json::to_vec(&credential).context("encode issued STS credential")?,
                    );
                    return self.issue_credential_payload(
                        client,
                        key,
                        ttl,
                        now_unix_ms,
                        Some("sts"),
                        Some(storage_target.to_owned()),
                        Some(storage_tier.to_owned()),
                        provider_expires_at,
                        None,
                    );
                }
                Err(sts::StsIssueError::Terminal(error)) => return Err(error),
                Err(sts::StsIssueError::Unavailable(error)) => {
                    if sts_policy.fallback != StsFallback::StaticOnUnavailable {
                        return Err(
                            error.context("STS unavailable and static fallback is disabled")
                        );
                    }
                    fell_back = true;
                }
            }
        }
        if let Some(azure_policy) = policy
            .azure_user_delegation
            .as_ref()
            .filter(|configured| configured.tiers.contains(&tier))
        {
            let issuer = topology
                .credentials
                .get(&azure_policy.issuer_ref)
                .context("topology has no Azure user delegation issuer credential")?
                .clone();
            let issued = tokio::time::timeout(
                STS_ISSUE_TIMEOUT,
                azure_sas::issue(&issuer, azure_policy, &endpoint, tier, ttl),
            )
            .await
            .unwrap_or_else(|_| {
                Err(azure_sas::AzureSasIssueError::Unavailable(anyhow::anyhow!(
                    "Azure user delegation request exceeded {} seconds",
                    STS_ISSUE_TIMEOUT.as_secs()
                )))
            });
            match issued {
                Ok(credential) => {
                    let recovered = self
                        .issued_static_fallbacks
                        .iter()
                        .filter(|(target, _)| target == storage_target)
                        .cloned()
                        .collect::<Vec<_>>();
                    for fallback in &recovered {
                        self.issued_static_fallbacks.remove(fallback);
                        self.retired_static_fallbacks.insert(fallback.clone());
                    }
                    if !recovered.is_empty() {
                        if let Err(error) = self.persist_fallback_state() {
                            for fallback in recovered {
                                self.retired_static_fallbacks.remove(&fallback);
                                self.issued_static_fallbacks.insert(fallback);
                            }
                            return Err(error.context(
                                "record static fallback retirement before issuing Azure SAS",
                            ));
                        }
                    }
                    let provider_expires_at = credential.expires_at.clone();
                    let key = Zeroizing::new(
                        serde_json::to_vec(&credential)
                            .context("encode issued Azure SAS credential")?,
                    );
                    return self.issue_credential_payload(
                        client,
                        key,
                        ttl,
                        now_unix_ms,
                        Some("azure-user-delegation"),
                        Some(storage_target.to_owned()),
                        Some(storage_tier.to_owned()),
                        provider_expires_at,
                        None,
                    );
                }
                Err(azure_sas::AzureSasIssueError::Terminal(error)) => return Err(error),
                Err(azure_sas::AzureSasIssueError::Unavailable(error)) => {
                    if azure_policy.fallback != StsFallback::StaticOnUnavailable {
                        return Err(error.context(
                            "Azure user delegation unavailable and static fallback is disabled",
                        ));
                    }
                    fell_back = true;
                }
            }
        }
        if let Some(gcp_policy) = policy
            .gcp_downscope
            .as_ref()
            .filter(|configured| configured.tiers.contains(&tier))
        {
            let issuer = topology
                .credentials
                .get(&gcp_policy.issuer_ref)
                .context("topology has no GCP downscope issuer credential")?
                .clone();
            let issued = tokio::time::timeout(
                STS_ISSUE_TIMEOUT,
                gcp_downscope::issue(&issuer, gcp_policy, &endpoint, tier, ttl),
            )
            .await
            .unwrap_or_else(|_| {
                Err(gcp_downscope::GcpDownscopeIssueError::Unavailable(
                    anyhow::anyhow!(
                        "GCP downscope request exceeded {} seconds",
                        STS_ISSUE_TIMEOUT.as_secs()
                    ),
                ))
            });
            match issued {
                Ok(credential) => {
                    let recovered = self
                        .issued_static_fallbacks
                        .iter()
                        .filter(|(target, _)| target == storage_target)
                        .cloned()
                        .collect::<Vec<_>>();
                    for fallback in &recovered {
                        self.issued_static_fallbacks.remove(fallback);
                        self.retired_static_fallbacks.insert(fallback.clone());
                    }
                    if !recovered.is_empty() {
                        if let Err(error) = self.persist_fallback_state() {
                            for fallback in recovered {
                                self.retired_static_fallbacks.remove(&fallback);
                                self.issued_static_fallbacks.insert(fallback);
                            }
                            return Err(error.context(
                                "record static fallback retirement before issuing GCP access token",
                            ));
                        }
                    }
                    let provider_expires_at = credential.expires_at.clone();
                    let key = Zeroizing::new(
                        serde_json::to_vec(&credential)
                            .context("encode issued GCP access token credential")?,
                    );
                    return self.issue_credential_payload(
                        client,
                        key,
                        ttl,
                        now_unix_ms,
                        Some("gcp-downscope"),
                        Some(storage_target.to_owned()),
                        Some(storage_tier.to_owned()),
                        provider_expires_at,
                        None,
                    );
                }
                Err(gcp_downscope::GcpDownscopeIssueError::Terminal(error)) => return Err(error),
                Err(gcp_downscope::GcpDownscopeIssueError::Unavailable(error)) => {
                    if gcp_policy.fallback != StsFallback::StaticOnUnavailable {
                        return Err(error.context(
                            "GCP downscoping unavailable and static fallback is disabled",
                        ));
                    }
                    fell_back = true;
                }
            }
        }
        let credential_ref = select_static_credential_reference(Some(&policy), tier)?.to_owned();
        let static_generation = policy
            .r#static
            .as_ref()
            .map(|configured| configured.generation);
        if fell_back {
            let fallback = (
                storage_target.to_owned(),
                static_generation.unwrap_or_default(),
            );
            if self.retired_static_fallbacks.contains(&fallback) {
                bail!("static fallback generation is retired and must be rotated after provider revocation");
            }
            if self.issued_static_fallbacks.insert(fallback.clone()) {
                if let Err(error) = self.persist_fallback_state() {
                    self.issued_static_fallbacks.remove(&fallback);
                    return Err(error
                        .context("record static fallback checkout before releasing credential"));
                }
            }
        }
        let mut lease =
            self.issue_credential_lease(client, &credential_ref, ttl, now_unix_ms, true)?;
        lease.credential_source = Some(if fell_back {
            "static-fallback"
        } else {
            "static"
        });
        lease.storage_target = Some(storage_target.to_owned());
        lease.storage_tier = Some(storage_tier.to_owned());
        lease.static_generation = static_generation;
        Ok(lease)
    }

    fn persist_fallback_state(&self) -> Result<()> {
        if let Some(store) = &self.fallback_state_store {
            store.persist(
                &self.issued_static_fallbacks,
                &self.retired_static_fallbacks,
                &self.identity,
            )?;
        }
        Ok(())
    }

    fn issue_credential_lease(
        &mut self,
        client: &ClientIdentity,
        credential_ref: &str,
        ttl: Duration,
        now_unix_ms: u64,
        tier_authorized: bool,
    ) -> Result<KeyLease> {
        self.expire(now_unix_ms);
        if self.pending_policy_mutation.is_some() || self.identity_recovery {
            bail!("credential leases are unavailable in the current broker state");
        }
        if ttl.is_zero() || ttl > MAX_LEASE_TTL || client.connection_id.is_empty() {
            bail!("invalid lease request");
        }
        self.authorize(client, Capability::CredentialLease)?;
        let authorization = self
            .authorization(client, Capability::CredentialLease)
            .context("credential lease is not authorized")?;
        if !tier_authorized
            && !authorization.credential_refs.contains("*")
            && !authorization.credential_refs.contains(credential_ref)
        {
            bail!("credential reference is not authorized for this client");
        }
        let epoch = self.epoch.as_ref().context("broker is locked")?;
        let encoded = &epoch.keys.sealed_topology;
        let topology = TopologyDocument::decode(encoded.as_slice())?;
        if topology
            .pack_backends
            .iter()
            .any(|backend| policy_uses_issuer(backend.credential_policy.as_ref(), credential_ref))
            || topology.metadata_replicas.replicas.values().any(|replica| {
                policy_uses_issuer(replica.credential_policy.as_ref(), credential_ref)
            })
        {
            bail!("dynamic issuer credentials are broker-only");
        }
        let credential = topology
            .credentials
            .get(credential_ref)
            .context("topology has no such credential")?;
        if authorization.read_only && credential.has_secret() && !tier_authorized {
            bail!("read-only client cannot lease a delete-capable credential");
        }
        let key = Zeroizing::new(topology.credential_json(credential_ref)?);
        self.issue_credential_payload(client, key, ttl, now_unix_ms, None, None, None, None, None)
    }

    fn issue_credential_payload(
        &mut self,
        client: &ClientIdentity,
        key: Zeroizing<Vec<u8>>,
        ttl: Duration,
        now_unix_ms: u64,
        credential_source: Option<&'static str>,
        storage_target: Option<String>,
        storage_tier: Option<String>,
        provider_expires_at: Option<String>,
        static_generation: Option<u64>,
    ) -> Result<KeyLease> {
        let epoch = self.epoch.as_ref().context("broker is locked")?;
        let expires_unix_ms = now_unix_ms
            .checked_add(u64::try_from(ttl.as_millis())?)
            .context("lease expiry overflow")?;
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
            capability: Capability::CredentialLease,
            expires_unix_ms,
            key_version: self.capsule.header.repository_key_version,
            capsule_generation: self.capsule.header.generation,
            key,
            credential_source,
            storage_target,
            storage_tier,
            provider_expires_at,
            static_generation,
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
        let builder = CapsuleBuilder::new(
            &self.capsule.header.repository_id,
            generation,
            epoch.keys.sealed_topology.as_slice(),
        )
        .broker_identity_public_key(self.identity.verifying_key().as_bytes())
        .key_versions(
            self.capsule.header.root_key_version,
            self.capsule.header.metadata_dek_version,
            self.capsule.header.repository_key_version,
        );
        let candidate = builder.create_offline_policy(
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
        let builder = CapsuleBuilder::new(
            &self.capsule.header.repository_id,
            generation,
            epoch.keys.sealed_topology.as_slice(),
        )
        .broker_identity_public_key(self.identity.verifying_key().as_bytes())
        .key_versions(
            self.capsule.header.root_key_version,
            self.capsule.header.metadata_dek_version,
            self.capsule.header.repository_key_version,
        );
        let candidate = builder
            .create_policy(
                policy,
                protections,
                epoch.keys.metadata_dek.as_slice(),
                epoch.keys.repository_master_key.as_slice(),
            )
            .await?;
        self.accept_policy_mutation(candidate, acknowledge_downgrade)
    }

    pub async fn prepare_topology_mutation(
        &mut self,
        client: &ClientIdentity,
        mutation: TopologyMutation,
        protections: &[(&str, MemberProtection<'_>)],
        now_unix_ms: u64,
    ) -> Result<(RecoveryCapsule, String)> {
        self.expire(now_unix_ms);
        self.authorize(client, Capability::PolicyMutation)?;
        if self.pending_policy_mutation.is_some() {
            bail!("a policy mutation is already pending publication");
        }
        let generation = self
            .capsule
            .header
            .generation
            .checked_add(1)
            .context("capsule generation overflow")?;
        let epoch = self.epoch.as_ref().context("broker is locked")?;
        let current = &epoch.keys.sealed_topology;
        let mut document = TopologyDocument::decode(current.as_slice())?;
        document.apply_mutation(mutation, generation)?;
        let topology = Zeroizing::new(document.canonical_json()?);
        let candidate = CapsuleBuilder::new(
            &self.capsule.header.repository_id,
            generation,
            topology.as_slice(),
        )
        .broker_identity_public_key(self.identity.verifying_key().as_bytes())
        .key_versions(
            self.capsule.header.root_key_version,
            self.capsule.header.metadata_dek_version,
            self.capsule.header.repository_key_version,
        )
        .create_policy(
            self.capsule.policy.clone(),
            protections,
            epoch.keys.metadata_dek.as_slice(),
            epoch.keys.repository_master_key.as_slice(),
        )
        .await?;
        self.accept_policy_mutation(candidate, false)
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
        self.persist_fallback_state()
            .context("re-sign static fallback lifecycle state before policy activation")?;
        let pending = self
            .pending_policy_mutation
            .take()
            .context("no policy mutation is pending publication")?;
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
            unlock_memory(epoch.keys.sealed_topology.as_slice());
        }
    }

    fn authorization(
        &self,
        client: &ClientIdentity,
        capability: Capability,
    ) -> Option<&ClientAuthorization> {
        self.authorizations.iter().find(|authorization| {
            authorization.component == client.component
                && authorization.release_identity == client.release_identity
                && authorization.peer_uid == client.peer_uid
                && client.version >= authorization.minimum_version
                && client.version <= authorization.maximum_version
                && authorization.capabilities.contains(&capability)
        })
    }

    fn authorize(&self, client: &ClientIdentity, capability: Capability) -> Result<()> {
        if !client.executable_owned_by_root || !client.installation_path_read_only {
            bail!("client executable ownership or installation path is not trusted");
        }
        let authorization = self.authorization(client, capability);
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

fn storage_credential_policy<'a>(
    topology: &'a TopologyDocument,
    storage_target: &str,
) -> Result<(
    &'a CredentialPolicy,
    &'a BTreeMap<String, serde_json::Value>,
)> {
    if let Some(id) = storage_target.strip_prefix("pack:") {
        let backend = topology
            .pack_backends
            .iter()
            .find(|backend| backend.id == id)
            .context("topology has no such pack backend")?;
        return Ok((
            backend
                .credential_policy
                .as_ref()
                .context("pack backend has no credential policy")?,
            &backend.endpoint,
        ));
    }
    if let Some(id) = storage_target.strip_prefix("metadata:") {
        let replica = topology
            .metadata_replicas
            .replicas
            .get(id)
            .context("topology has no such metadata replica")?;
        return Ok((
            replica
                .credential_policy
                .as_ref()
                .context("metadata replica has no credential policy")?,
            &replica.endpoint,
        ));
    }
    if storage_target == "wal" {
        let target = topology
            .wal_target
            .as_ref()
            .context("topology has no separate WAL target")?;
        return Ok((
            target
                .credential_policy
                .as_ref()
                .context("WAL target has no credential policy")?,
            &target.endpoint,
        ));
    }
    bail!("invalid storage credential target")
}

fn policy_uses_issuer(policy: Option<&CredentialPolicy>, credential_ref: &str) -> bool {
    policy.is_some_and(|policy| {
        policy
            .sts
            .as_ref()
            .is_some_and(|issuer| issuer.issuer_ref == credential_ref)
            || policy
                .azure_user_delegation
                .as_ref()
                .is_some_and(|issuer| issuer.issuer_ref == credential_ref)
            || policy
                .gcp_downscope
                .as_ref()
                .is_some_and(|issuer| issuer.issuer_ref == credential_ref)
    })
}

include!("broker/lease.rs");

include!("broker/tests.rs");
