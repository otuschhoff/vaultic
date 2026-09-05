//! Recovery capsule policies, shares, persistence, and key reconstruction.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use aes_gcm::{
    aead::{Aead, Payload},
    Aes256Gcm, KeyInit, Nonce,
};
use anyhow::{bail, Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hkdf::Hkdf;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sharks::{Share, Sharks};
use slatedb::object_store::{
    path::Path as ObjectPath, Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode,
    PutOptions,
};
use zeroize::Zeroizing;

use super::envelope::providers::{KeyContext, KeyProvider};
use crate::ids::{MemberId, RepositoryId};

pub const CAPSULE_FORMAT: u32 = 2;
pub const ROOT_KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 12;
const SALT_BYTES: usize = 16;
const DEFAULT_MEMORY_KIB: u32 = 64 * 1024;
const DEFAULT_ITERATIONS: u32 = 3;
const DEFAULT_PARALLELISM: u32 = 1;
const EXTERNAL_SHARE_MAGIC: &[u8] = b"VLTCAPSH1";
pub const CAPSULE_DIRECTORY: &str = "_vaultic/recovery-capsules";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryCapsule {
    pub header: CapsuleHeader,
    pub policy: UnlockPolicy,
    pub members: Vec<MemberShare>,
    pub metadata_dek: WrappedPayload,
    pub repository_master_key: WrappedPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapsuleHeader {
    pub format: u32,
    pub logical_id: String,
    pub repository_id: RepositoryId,
    pub generation: u64,
    pub root_key_version: u32,
    pub metadata_dek_version: u32,
    pub repository_key_version: u32,
    pub algorithm: String,
    pub policy_hash: String,
    pub broker_identity_public_key: String,
    pub policy_intent: PolicyIntent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyIntent {
    Bootstrap,
    Quorum,
    BreakGlass,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum UnlockPolicy {
    Member {
        member_id: MemberId,
    },
    AnyOf {
        policies: Vec<UnlockPolicy>,
    },
    AllOf {
        policies: Vec<UnlockPolicy>,
    },
    Threshold {
        group_id: String,
        required: u8,
        members: Vec<MemberId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemberShare {
    pub member_id: MemberId,
    pub group_id: String,
    pub share_index: u8,
    pub threshold: u8,
    pub share_count: u8,
    pub provider: MemberProvider,
    pub key_reference: String,
    pub wrapped_share: String,
    pub nonce: Option<String>,
    pub argon2: Option<Argon2Config>,
    pub principal: Option<PrincipalBinding>,
    pub hardware: Option<HardwareBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PrincipalBinding {
    pub authority: String,
    pub tenant_account_or_project: String,
    pub immutable_principal_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HardwareBinding {
    pub credential_id: String,
    pub public_key: String,
    pub serial_number: Option<String>,
    pub attestation_fingerprint: Option<String>,
    pub user_presence_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePolicyStatus {
    pub minimum_custodians: usize,
    pub principal_verified: bool,
    pub hardware_verified: bool,
    pub custody_assumed: bool,
    pub compliant: bool,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum MemberProvider {
    OfflineArgon2id,
    OfflineKeyfile,
    YubikeyPiv,
    Fido2HmacSecret,
    MacosSecureEnclave,
    AzureKeyVault,
    AwsKms,
    AwsCloudhsm,
    GcpKms,
    GcpCloudHsm,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Argon2Config {
    pub salt: String,
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WrappedPayload {
    pub purpose: String,
    pub nonce: String,
    pub ciphertext: String,
}

#[derive(Debug)]
pub struct RecoveredKeys {
    pub metadata_dek: Zeroizing<Vec<u8>>,
    pub repository_master_key: Zeroizing<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct UnwrappedMemberShare {
    pub member_id: MemberId,
    pub share_index: u8,
    pub plaintext: Zeroizing<Vec<u8>>,
}

pub(crate) fn validate_shamir_share(share: &[u8]) -> Result<()> {
    Share::try_from(share)
        .map(|_| ())
        .map_err(|error| anyhow::anyhow!("decode Shamir share: {error}"))
}

pub struct ExternalMemberProtection<'a> {
    pub provider: MemberProvider,
    pub key_reference: &'a str,
    pub principal: Option<PrincipalBinding>,
    pub hardware: Option<HardwareBinding>,
    pub key_provider: &'a dyn KeyProvider,
}

pub enum MemberProtection<'a> {
    Offline(MemberCredential<'a>),
    External(ExternalMemberProtection<'a>),
}

pub struct CapsuleBuilder {
    repository_id: RepositoryId,
    generation: u64,
    root_key_version: u32,
    metadata_dek_version: u32,
    repository_key_version: u32,
    broker_identity_public_key: Vec<u8>,
}

impl CapsuleBuilder {
    pub fn new(repository_id: impl Into<RepositoryId>, generation: u64) -> Self {
        Self {
            repository_id: repository_id.into(),
            generation,
            root_key_version: 1,
            metadata_dek_version: 1,
            repository_key_version: 1,
            broker_identity_public_key: Vec::new(),
        }
    }

    pub fn broker_identity_public_key(mut self, public_key: &[u8]) -> Self {
        self.broker_identity_public_key = public_key.to_vec();
        self
    }

    pub fn key_versions(
        mut self,
        root_key_version: u32,
        metadata_dek_version: u32,
        repository_key_version: u32,
    ) -> Self {
        self.root_key_version = root_key_version;
        self.metadata_dek_version = metadata_dek_version;
        self.repository_key_version = repository_key_version;
        self
    }

    pub fn create_offline_threshold(
        self,
        group_id: &str,
        required: u8,
        credentials: &[(&str, MemberCredential<'_>)],
        metadata_dek: &[u8],
        repository_master_key: &[u8],
    ) -> Result<RecoveryCapsule> {
        let share_count = u8::try_from(credentials.len()).context("too many members")?;
        if required == 0 || required > share_count || credentials.is_empty() {
            bail!("invalid threshold {required}-of-{share_count}");
        }
        let mut member_ids = BTreeSet::new();
        for (member_id, _) in credentials {
            if member_id.is_empty() || !member_ids.insert((*member_id).to_owned()) {
                bail!("member IDs must be non-empty and unique");
            }
        }

        let policy = UnlockPolicy::Threshold {
            group_id: group_id.to_owned(),
            required,
            members: member_ids.into_iter().map(MemberId::from).collect(),
        };
        self.create_offline_policy(policy, credentials, metadata_dek, repository_master_key)
    }

    pub fn create_offline_policy(
        self,
        policy: UnlockPolicy,
        credentials: &[(&str, MemberCredential<'_>)],
        metadata_dek: &[u8],
        repository_master_key: &[u8],
    ) -> Result<RecoveryCapsule> {
        if self.repository_id.is_empty() || self.generation == 0 {
            bail!("repository ID and non-zero generation are required");
        }
        if metadata_dek.len() != 32 || repository_master_key.is_empty() {
            bail!("metadata DEK must be 32 bytes and repository master key must not be empty");
        }
        let policy_member_ids = policy.member_ids()?;
        let credential_ids = credentials
            .iter()
            .map(|(member_id, _)| MemberId::from(*member_id))
            .collect::<BTreeSet<_>>();
        if credential_ids.len() != credentials.len() || credential_ids != policy_member_ids {
            bail!("credentials must match policy members exactly");
        }
        let policy_hash = policy_hash(&policy)?;
        let mut header = CapsuleHeader {
            format: CAPSULE_FORMAT,
            logical_id: String::new(),
            repository_id: self.repository_id,
            generation: self.generation,
            root_key_version: self.root_key_version,
            metadata_dek_version: self.metadata_dek_version,
            repository_key_version: self.repository_key_version,
            algorithm: "HKDF-SHA256/AES-256-GCM/Shamir-GF256".to_owned(),
            policy_hash,
            broker_identity_public_key: BASE64.encode(self.broker_identity_public_key),
            policy_intent: if policy.minimum_custodians() >= 2 {
                PolicyIntent::Quorum
            } else {
                PolicyIntent::Bootstrap
            },
        };
        header.logical_id = logical_id(&header);

        let mut root_secret = Zeroizing::new([0_u8; ROOT_KEY_BYTES]);
        rand::rng().fill_bytes(root_secret.as_mut());
        let mut distributed = Vec::new();
        distribute_policy_secret(&policy, root_secret.as_ref(), "root", &mut distributed)?;
        let credential_map = credentials.iter().copied().collect::<BTreeMap<_, _>>();
        let mut members = Vec::with_capacity(distributed.len());
        for share in distributed {
            let credential = credential_map
                .get(share.member_id.as_str())
                .context("missing member credential")?;
            members.push(wrap_offline_share(
                &header,
                &share.group_id,
                &share.member_id,
                share.share_index,
                share.threshold,
                share.share_count,
                credential,
                &share.plaintext,
            )?);
        }
        let metadata_dek =
            wrap_payload(&header, metadata_dek, "metadata-dek", root_secret.as_ref())?;
        let repository_master_key = wrap_payload(
            &header,
            repository_master_key,
            "repository-master-key",
            root_secret.as_ref(),
        )?;
        let capsule = RecoveryCapsule {
            header,
            policy,
            members,
            metadata_dek,
            repository_master_key,
        };
        capsule.validate()?;
        Ok(capsule)
    }

    pub async fn create_policy(
        self,
        policy: UnlockPolicy,
        protections: &[(&str, MemberProtection<'_>)],
        metadata_dek: &[u8],
        repository_master_key: &[u8],
    ) -> Result<RecoveryCapsule> {
        if self.repository_id.is_empty() || self.generation == 0 {
            bail!("repository ID and non-zero generation are required");
        }
        if metadata_dek.len() != 32 || repository_master_key.is_empty() {
            bail!("metadata DEK must be 32 bytes and repository master key must not be empty");
        }
        let policy_member_ids = policy.member_ids()?;
        let protection_ids = protections
            .iter()
            .map(|(member_id, _)| MemberId::from(*member_id))
            .collect::<BTreeSet<_>>();
        if protection_ids.len() != protections.len() || protection_ids != policy_member_ids {
            bail!("member protections must match policy members exactly");
        }
        let policy_hash = policy_hash(&policy)?;
        let mut header = CapsuleHeader {
            format: CAPSULE_FORMAT,
            logical_id: String::new(),
            repository_id: self.repository_id,
            generation: self.generation,
            root_key_version: self.root_key_version,
            metadata_dek_version: self.metadata_dek_version,
            repository_key_version: self.repository_key_version,
            algorithm: "HKDF-SHA256/AES-256-GCM/Shamir-GF256".to_owned(),
            policy_hash,
            broker_identity_public_key: BASE64.encode(self.broker_identity_public_key),
            policy_intent: if policy.minimum_custodians() >= 2 {
                PolicyIntent::Quorum
            } else {
                PolicyIntent::Bootstrap
            },
        };
        header.logical_id = logical_id(&header);

        let mut root_secret = Zeroizing::new([0_u8; ROOT_KEY_BYTES]);
        rand::rng().fill_bytes(root_secret.as_mut());
        let mut distributed = Vec::new();
        distribute_policy_secret(&policy, root_secret.as_ref(), "root", &mut distributed)?;
        let protection_map = protections
            .iter()
            .map(|(member_id, protection)| (*member_id, protection))
            .collect::<BTreeMap<_, _>>();
        let mut members = Vec::with_capacity(distributed.len());
        for share in distributed {
            let protection = protection_map
                .get(share.member_id.as_str())
                .context("missing member protection")?;
            let member = match protection {
                MemberProtection::Offline(credential) => wrap_offline_share(
                    &header,
                    &share.group_id,
                    &share.member_id,
                    share.share_index,
                    share.threshold,
                    share.share_count,
                    credential,
                    &share.plaintext,
                )?,
                MemberProtection::External(external) => {
                    wrap_external_share(&header, &share, external).await?
                }
            };
            members.push(member);
        }
        let metadata_dek =
            wrap_payload(&header, metadata_dek, "metadata-dek", root_secret.as_ref())?;
        let repository_master_key = wrap_payload(
            &header,
            repository_master_key,
            "repository-master-key",
            root_secret.as_ref(),
        )?;
        let capsule = RecoveryCapsule {
            header,
            policy,
            members,
            metadata_dek,
            repository_master_key,
        };
        capsule.validate()?;
        Ok(capsule)
    }
}

#[derive(Clone, Copy)]
pub enum MemberCredential<'a> {
    Passphrase(&'a [u8]),
    Keyfile(&'a [u8]),
}

impl RecoveryCapsule {
    pub fn validate(&self) -> Result<()> {
        if self.header.format != CAPSULE_FORMAT
            || self.header.generation == 0
            || self.header.repository_id.is_empty()
            || self.header.logical_id != logical_id(&self.header)
            || self.header.algorithm != "HKDF-SHA256/AES-256-GCM/Shamir-GF256"
        {
            bail!("invalid recovery capsule header");
        }
        if !matches!(
            BASE64.decode(&self.header.broker_identity_public_key),
            Ok(key) if key.len() == 32
        ) {
            bail!("invalid broker identity public key");
        }
        if self.header.policy_hash != policy_hash(&self.policy)? {
            bail!("recovery capsule policy hash mismatch");
        }
        if self.metadata_dek.purpose != "metadata-dek"
            || self.repository_master_key.purpose != "repository-master-key"
        {
            bail!("recovery capsule payload purpose mismatch");
        }
        let referenced = self.policy.member_ids()?;
        let mut seen_members = BTreeSet::new();
        let mut seen_shares = BTreeSet::new();
        for member in &self.members {
            if member.member_id.is_empty()
                || !referenced.contains(&member.member_id)
                || !seen_members.insert(member.member_id.clone())
                || member.share_index == 0
                || member.threshold == 0
                || member.threshold > member.share_count
                || member.share_index > member.share_count
                || !seen_shares.insert((member.group_id.clone(), member.share_index))
            {
                bail!("invalid or duplicate recovery capsule member share");
            }
            match member.provider {
                MemberProvider::OfflineArgon2id
                    if member.argon2.is_none() || member.nonce.is_none() =>
                {
                    bail!("offline Argon2id member is missing wrapping parameters")
                }
                MemberProvider::OfflineKeyfile if member.nonce.is_none() => {
                    bail!("offline keyfile member is missing a nonce")
                }
                _ => {}
            }
        }
        if seen_members != referenced {
            bail!("recovery capsule members do not match policy");
        }
        for member in &self.members {
            match member.provider {
                MemberProvider::OfflineArgon2id | MemberProvider::OfflineKeyfile => {
                    if member.principal.is_some() || member.hardware.is_some() {
                        bail!("offline member must not claim principal or hardware verification");
                    }
                }
                MemberProvider::YubikeyPiv
                | MemberProvider::Fido2HmacSecret
                | MemberProvider::MacosSecureEnclave => {
                    let hardware = member
                        .hardware
                        .as_ref()
                        .context("hardware member lacks credential binding")?;
                    if hardware.credential_id.is_empty()
                        || hardware.public_key.is_empty()
                        || !hardware.user_presence_required
                    {
                        bail!("hardware member binding is incomplete");
                    }
                    if member.provider == MemberProvider::YubikeyPiv
                        && hardware.public_key
                            != crate::encryption::envelope::providers::yubikey_piv_public_key_binding(
                                &member.key_reference,
                            )?
                    {
                        bail!("YubiKey PIV hardware binding does not match key reference");
                    }
                    if member.provider == MemberProvider::Fido2HmacSecret {
                        let (credential_id, public_key) =
                            crate::encryption::envelope::providers::fido2_hardware_bindings(
                                &member.key_reference,
                            )?;
                        if hardware.credential_id != credential_id
                            || hardware.public_key != public_key
                        {
                            bail!("FIDO2 hardware binding does not match key reference");
                        }
                    }
                    if member.provider == MemberProvider::MacosSecureEnclave {
                        let (credential_id, public_key) = crate::encryption::envelope::providers::macos_secure_enclave_hardware_bindings(&member.key_reference)?;
                        if hardware.credential_id != credential_id
                            || hardware.public_key != public_key
                            || hardware.serial_number.is_some()
                            || hardware.attestation_fingerprint.is_some()
                        {
                            bail!("macOS Secure Enclave hardware binding does not match key reference");
                        }
                    }
                }
                _ => {
                    let principal = member
                        .principal
                        .as_ref()
                        .context("cloud member lacks principal binding")?;
                    if member.key_reference.is_empty()
                        || principal.authority.is_empty()
                        || principal.tenant_account_or_project.is_empty()
                        || principal.immutable_principal_id.is_empty()
                    {
                        bail!("cloud member binding is incomplete");
                    }
                    let expected_authority = match member.provider {
                        MemberProvider::AzureKeyVault => "entra",
                        MemberProvider::AwsKms | MemberProvider::AwsCloudhsm => "aws-iam",
                        MemberProvider::GcpKms | MemberProvider::GcpCloudHsm => "gcp-iam",
                        _ => unreachable!(),
                    };
                    if principal.authority != expected_authority {
                        bail!("cloud member principal authority does not match provider");
                    }
                }
            }
        }
        validate_policy(&self.policy, &self.members, "root")
    }

    pub fn effective_policy_status(&self) -> Result<EffectivePolicyStatus> {
        self.validate()?;
        let minimum_custodians = self.policy.minimum_custodians();
        let mut findings = Vec::new();
        if self.header.policy_intent == PolicyIntent::Quorum && minimum_custodians < 2 {
            findings.push("quorum policy has an effective single-custodian path".to_owned());
        }
        if self.header.policy_intent == PolicyIntent::Bootstrap {
            findings.push("single-member bootstrap path remains active".to_owned());
        }
        let mut cloud_keys = BTreeSet::new();
        let mut principals = BTreeSet::new();
        let mut hardware_credentials = BTreeSet::new();
        let mut hardware_public_keys = BTreeSet::new();
        let mut principal_verified = false;
        let mut hardware_verified = false;
        let mut custody_assumed = false;
        for member in &self.members {
            match member.provider {
                MemberProvider::OfflineArgon2id | MemberProvider::OfflineKeyfile => {
                    custody_assumed = true;
                }
                MemberProvider::YubikeyPiv
                | MemberProvider::Fido2HmacSecret
                | MemberProvider::MacosSecureEnclave => {
                    hardware_verified = true;
                    let hardware = member
                        .hardware
                        .as_ref()
                        .context("hardware member is missing hardware identity")?;
                    let credential = &hardware.credential_id;
                    if !hardware_credentials.insert(credential.clone()) {
                        findings.push(format!("duplicate hardware credential {credential}"));
                    }
                    let public_key = &hardware.public_key;
                    if !hardware_public_keys.insert(public_key.clone()) {
                        findings.push("duplicate hardware public key".to_owned());
                    }
                }
                _ => {
                    principal_verified = true;
                    if !cloud_keys.insert((member.provider.clone(), member.key_reference.clone())) {
                        findings.push(format!(
                            "duplicate cloud key reference {}",
                            member.key_reference
                        ));
                    }
                    let principal = member
                        .principal
                        .as_ref()
                        .context("cloud member is missing principal identity")?;
                    let identity = (
                        principal.authority.clone(),
                        principal.tenant_account_or_project.clone(),
                        principal.immutable_principal_id.clone(),
                    );
                    if !principals.insert(identity) {
                        findings.push(format!(
                            "overlapping cloud principal {}",
                            principal.immutable_principal_id
                        ));
                    }
                }
            }
        }
        Ok(EffectivePolicyStatus {
            minimum_custodians,
            principal_verified,
            hardware_verified,
            custody_assumed,
            compliant: self.header.policy_intent == PolicyIntent::Quorum
                && minimum_custodians >= 2
                && findings.is_empty(),
            findings,
        })
    }

    pub fn recover_offline(
        &self,
        credentials: &BTreeMap<String, MemberCredential<'_>>,
    ) -> Result<RecoveredKeys> {
        self.validate()?;
        let mut shares = Vec::new();
        for member in &self.members {
            let Some(credential) = credentials.get(member.member_id.as_str()) else {
                continue;
            };
            if let Ok(share) = self.unwrap_offline_member(&member.member_id, credential) {
                shares.push(share);
            }
        }
        self.recover_from_shares(&shares)
    }

    pub fn unwrap_offline_member(
        &self,
        member_id: &str,
        credential: &MemberCredential<'_>,
    ) -> Result<UnwrappedMemberShare> {
        self.validate()?;
        let member = self
            .members
            .iter()
            .find(|member| member.member_id == member_id)
            .context("capsule has no such member")?;
        let plaintext = unwrap_offline_share(&self.header, member, credential)?;
        validate_shamir_share(plaintext.as_slice())?;
        Ok(UnwrappedMemberShare {
            member_id: member_id.into(),
            share_index: member.share_index,
            plaintext,
        })
    }

    pub async fn unwrap_external_member(
        &self,
        member_id: &str,
        provider: &dyn KeyProvider,
    ) -> Result<UnwrappedMemberShare> {
        self.validate()?;
        let member = self
            .members
            .iter()
            .find(|member| member.member_id == member_id)
            .context("capsule has no such member")?;
        validate_external_provider(&member.provider, provider.name())?;
        let ciphertext = BASE64
            .decode(&member.wrapped_share)
            .context("decode externally wrapped member share")?;
        let purpose = external_share_purpose(&self.header, member)?;
        let plaintext = provider
            .unwrap(
                &KeyContext {
                    repository_id: self.header.repository_id.as_str(),
                    slot_id: member.member_id.as_str(),
                    key_reference: &member.key_reference,
                    dek_version: self.header.root_key_version,
                    purpose: &purpose,
                },
                &ciphertext,
            )
            .await
            .context("unwrap externally protected member share")?;
        let plaintext = decode_external_share(&purpose, plaintext.as_slice())?;
        validate_shamir_share(plaintext.as_slice())?;
        Ok(UnwrappedMemberShare {
            member_id: member_id.into(),
            share_index: member.share_index,
            plaintext,
        })
    }

    pub fn recover_from_shares(
        &self,
        contributions: &[UnwrappedMemberShare],
    ) -> Result<RecoveredKeys> {
        self.validate()?;
        let mut unlocked = BTreeSet::new();
        let mut indexes = BTreeSet::new();
        for contribution in contributions {
            let member = self
                .members
                .iter()
                .find(|member| member.member_id == contribution.member_id)
                .context("contribution references an unknown member")?;
            if member.share_index != contribution.share_index
                || !unlocked.insert(contribution.member_id.clone())
                || !indexes.insert((member.group_id.clone(), contribution.share_index))
            {
                bail!("duplicate or re-indexed member contribution");
            }
            validate_shamir_share(contribution.plaintext.as_slice())?;
        }
        if !self.policy.satisfied_by(&unlocked) {
            bail!("unlock policy is not satisfied");
        }
        let root_secret = recover_policy_secret(&self.policy, contributions, "root")?;
        if root_secret.len() != ROOT_KEY_BYTES {
            bail!("reconstructed root key has an invalid length");
        }
        let metadata_dek = unwrap_payload(
            &self.header,
            &self.metadata_dek,
            "metadata-dek",
            root_secret.as_slice(),
        )?;
        let repository_master_key = unwrap_payload(
            &self.header,
            &self.repository_master_key,
            "repository-master-key",
            root_secret.as_slice(),
        )?;
        Ok(RecoveredKeys {
            metadata_dek,
            repository_master_key,
        })
    }

    pub(crate) fn policy_satisfied_by(&self, contributions: &[UnwrappedMemberShare]) -> bool {
        let members = contributions
            .iter()
            .map(|contribution| contribution.member_id.clone())
            .collect::<BTreeSet<_>>();
        self.policy.satisfied_by(&members)
    }
}

include!("recovery_capsule/publish.rs");

include!("recovery_capsule/shares.rs");

include!("recovery_capsule/tests.rs");
