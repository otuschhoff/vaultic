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
    pub repository_id: String,
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
    Member { member_id: String },
    AnyOf { policies: Vec<UnlockPolicy> },
    AllOf { policies: Vec<UnlockPolicy> },
    Threshold {
        group_id: String,
        required: u8,
        members: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemberShare {
    pub member_id: String,
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

#[derive(Debug)]
pub struct UnwrappedMemberShare {
    pub member_id: String,
    pub share_index: u8,
    pub plaintext: Zeroizing<Vec<u8>>,
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
    repository_id: String,
    generation: u64,
    root_key_version: u32,
    metadata_dek_version: u32,
    repository_key_version: u32,
    broker_identity_public_key: Vec<u8>,
}

impl CapsuleBuilder {
    pub fn new(repository_id: impl Into<String>, generation: u64) -> Self {
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
            members: member_ids.into_iter().collect(),
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
            .map(|(member_id, _)| (*member_id).to_owned())
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
        let metadata_dek = wrap_payload(
            &header,
            metadata_dek,
            "metadata-dek",
            root_secret.as_ref(),
        )?;
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
            .map(|(member_id, _)| (*member_id).to_owned())
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
        let metadata_dek = wrap_payload(
            &header,
            metadata_dek,
            "metadata-dek",
            root_secret.as_ref(),
        )?;
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
                MemberProvider::YubikeyPiv | MemberProvider::Fido2HmacSecret => {
                    let hardware = member.hardware.as_ref().context("hardware member lacks credential binding")?;
                    if hardware.credential_id.is_empty()
                        || hardware.public_key.is_empty()
                        || !hardware.user_presence_required
                    {
                        bail!("hardware member binding is incomplete");
                    }
                }
                _ => {
                    let principal = member.principal.as_ref().context("cloud member lacks principal binding")?;
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
                MemberProvider::YubikeyPiv | MemberProvider::Fido2HmacSecret => {
                    hardware_verified = true;
                    let credential = &member.hardware.as_ref().unwrap().credential_id;
                    if !hardware_credentials.insert(credential.clone()) {
                        findings.push(format!("duplicate hardware credential {credential}"));
                    }
                    let public_key = &member.hardware.as_ref().unwrap().public_key;
                    if !hardware_public_keys.insert(public_key.clone()) {
                        findings.push("duplicate hardware public key".to_owned());
                    }
                }
                _ => {
                    principal_verified = true;
                    if !cloud_keys.insert((member.provider.clone(), member.key_reference.clone())) {
                        findings.push(format!("duplicate cloud key reference {}", member.key_reference));
                    }
                    let principal = member.principal.as_ref().unwrap();
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
            let Some(credential) = credentials.get(&member.member_id) else {
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
        Share::try_from(plaintext.as_slice())
            .map_err(|error| anyhow::anyhow!("decode Shamir share: {error}"))?;
        Ok(UnwrappedMemberShare {
            member_id: member_id.to_owned(),
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
                    repository_id: &self.header.repository_id,
                    slot_id: &member.member_id,
                    key_reference: &member.key_reference,
                    dek_version: self.header.root_key_version,
                    purpose: &purpose,
                },
                &ciphertext,
            )
            .await
            .context("unwrap externally protected member share")?;
        let plaintext = decode_external_share(&purpose, plaintext.as_slice())?;
        Share::try_from(plaintext.as_slice())
            .map_err(|error| anyhow::anyhow!("decode Shamir share: {error}"))?;
        Ok(UnwrappedMemberShare {
            member_id: member_id.to_owned(),
            share_index: member.share_index,
            plaintext,
        })
    }

    pub fn recover_from_shares(&self, contributions: &[UnwrappedMemberShare]) -> Result<RecoveredKeys> {
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
            Share::try_from(contribution.plaintext.as_slice())
                .map_err(|error| anyhow::anyhow!("decode Shamir share: {error}"))?;
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
}

pub fn publish_local(directory: &Path, capsule: &RecoveryCapsule) -> Result<PathBuf> {
    capsule.validate()?;
    fs::create_dir_all(directory)
        .with_context(|| format!("create capsule directory {}", directory.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    let path = directory.join(format!("{:020}.json", capsule.header.generation));
    let mut encoded = serde_json::to_vec_pretty(capsule)?;
    encoded.push(b'\n');
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(&path) {
        Ok(mut file) => {
            if let Err(error) = file.write_all(&encoded).and_then(|_| file.sync_all()) {
                let _ = fs::remove_file(&path);
                return Err(error).context("persist immutable recovery capsule");
            }
            FileSync::sync_directory(directory)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read(&path)?;
            if existing != encoded {
                bail!("immutable recovery capsule generation already exists with different bytes");
            }
        }
        Err(error) => return Err(error).context("create immutable recovery capsule"),
    }
    Ok(path)
}

pub fn discover_latest(directory: &Path, repository_id: &str) -> Result<Option<(PathBuf, RecoveryCapsule)>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("read recovery capsule directory"),
    };
    let mut latest: Option<(PathBuf, RecoveryCapsule)> = None;
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".json") {
            continue;
        }
        if !file_type.is_file() || file_type.is_symlink() {
            bail!("recovery capsule generation must be a non-symlink regular file");
        }
        let capsule: RecoveryCapsule = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("decode recovery capsule {}", entry.path().display()))?;
        capsule.validate()?;
        if capsule.header.repository_id != repository_id {
            bail!("recovery capsule repository identity mismatch");
        }
        let expected_name = format!("{:020}.json", capsule.header.generation);
        if name != expected_name {
            bail!("recovery capsule filename does not match authenticated generation");
        }
        if latest
            .as_ref()
            .is_none_or(|(_, current)| capsule.header.generation > current.header.generation)
        {
            latest = Some((entry.path(), capsule));
        }
    }
    Ok(latest)
}

pub async fn publish_mirror(store: &dyn ObjectStore, capsule: &RecoveryCapsule) -> Result<String> {
    capsule.validate()?;
    let location = format!(
        "{}/{:020}.json",
        CAPSULE_DIRECTORY, capsule.header.generation
    );
    let mut encoded = serde_json::to_vec_pretty(capsule)?;
    encoded.push(b'\n');
    let path = ObjectPath::from(location.as_str());
    match store
        .put_opts(
            &path,
            encoded.clone().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => {}
        Err(error @ ObjectStoreError::AlreadyExists { .. }) => {
            let existing = store.get(&path).await?.bytes().await?;
            if existing.as_ref() != encoded.as_slice() {
                return Err(error).context("immutable capsule mirror conflicts with existing generation");
            }
        }
        Err(error) => return Err(error).context("publish immutable capsule mirror"),
    }
    Ok(location)
}

struct FileSync;

impl FileSync {
    fn sync_directory(directory: &Path) -> Result<()> {
        let file = fs::File::open(directory)?;
        file.sync_all()?;
        Ok(())
    }
}

impl UnlockPolicy {
    fn member_ids(&self) -> Result<BTreeSet<String>> {
        let mut ids = BTreeSet::new();
        self.collect_member_ids(&mut ids)?;
        Ok(ids)
    }

    fn collect_member_ids(&self, ids: &mut BTreeSet<String>) -> Result<()> {
        match self {
            Self::Member { member_id } => {
                if member_id.is_empty() || !ids.insert(member_id.clone()) {
                    bail!("policy member IDs must be non-empty and unique");
                }
            }
            Self::AnyOf { policies } | Self::AllOf { policies } => {
                if policies.is_empty() {
                    bail!("composed policy must not be empty");
                }
                for policy in policies {
                    policy.collect_member_ids(ids)?;
                }
            }
            Self::Threshold {
                group_id,
                required,
                members,
            } => {
                if group_id.is_empty()
                    || *required == 0
                    || usize::from(*required) > members.len()
                    || members.is_empty()
                {
                    bail!("invalid threshold policy");
                }
                for member in members {
                    if member.is_empty() || !ids.insert(member.clone()) {
                        bail!("policy member IDs must be non-empty and unique");
                    }
                }
            }
        }
        Ok(())
    }

    fn satisfied_by(&self, members: &BTreeSet<String>) -> bool {
        match self {
            Self::Member { member_id } => members.contains(member_id),
            Self::AnyOf { policies } => policies.iter().any(|policy| policy.satisfied_by(members)),
            Self::AllOf { policies } => policies.iter().all(|policy| policy.satisfied_by(members)),
            Self::Threshold {
                required,
                members: required_members,
                ..
            } => {
                required_members
                    .iter()
                    .filter(|member| members.contains(*member))
                    .count()
                    >= usize::from(*required)
            }
        }
    }

    pub fn minimum_custodians(&self) -> usize {
        match self {
            Self::Member { .. } => 1,
            Self::AnyOf { policies } => policies
                .iter()
                .map(Self::minimum_custodians)
                .min()
                .unwrap_or(0),
            Self::AllOf { policies } => policies.iter().map(Self::minimum_custodians).sum(),
            Self::Threshold { required, .. } => usize::from(*required),
        }
    }
}

fn validate_policy(policy: &UnlockPolicy, members: &[MemberShare], path: &str) -> Result<()> {
    match policy {
        UnlockPolicy::Member { member_id } => {
            let member = members
                .iter()
                .find(|member| &member.member_id == member_id)
                .context("policy references an unknown member")?;
            if member.group_id != format!("policy:{path}")
                || member.share_index != 1
                || member.threshold != 1
                || member.share_count != 1
            {
                bail!("member share binding does not match policy");
            }
        }
        UnlockPolicy::AnyOf { policies } => {
            for (index, policy) in policies.iter().enumerate() {
                validate_policy(policy, members, &format!("{path}/any/{index}"))?;
            }
        }
        UnlockPolicy::AllOf { policies } => {
            for (index, policy) in policies.iter().enumerate() {
                validate_policy(policy, members, &format!("{path}/all/{index}"))?;
            }
        }
        UnlockPolicy::Threshold {
            group_id,
            required,
            members: policy_members,
        } => {
            for member_id in policy_members {
                let member = members
                    .iter()
                    .find(|member| &member.member_id == member_id)
                    .context("policy references an unknown member")?;
                if &member.group_id != group_id
                    || member.threshold != *required
                    || usize::from(member.share_count) != policy_members.len()
                {
                    bail!("threshold member binding does not match policy");
                }
            }
        }
    }
    Ok(())
}

struct DistributedShare {
    member_id: String,
    group_id: String,
    share_index: u8,
    threshold: u8,
    share_count: u8,
    plaintext: Zeroizing<Vec<u8>>,
}

fn distribute_policy_secret(
    policy: &UnlockPolicy,
    secret: &[u8],
    path: &str,
    output: &mut Vec<DistributedShare>,
) -> Result<()> {
    match policy {
        UnlockPolicy::Member { member_id } => {
            let share = Sharks(1)
                .dealer(secret)
                .next()
                .context("create single-member share")?;
            output.push(DistributedShare {
                member_id: member_id.clone(),
                group_id: format!("policy:{path}"),
                share_index: 1,
                threshold: 1,
                share_count: 1,
                plaintext: Zeroizing::new(Vec::from(&share)),
            });
        }
        UnlockPolicy::AnyOf { policies } => {
            for (index, child) in policies.iter().enumerate() {
                distribute_policy_secret(child, secret, &format!("{path}/any/{index}"), output)?;
            }
        }
        UnlockPolicy::AllOf { policies } => {
            let mut combined = Zeroizing::new(vec![0_u8; secret.len()]);
            for (index, child) in policies.iter().enumerate() {
                let fragment = if index + 1 == policies.len() {
                    secret
                        .iter()
                        .zip(combined.iter())
                        .map(|(secret, combined)| secret ^ combined)
                        .collect::<Vec<_>>()
                } else {
                    let mut fragment = vec![0_u8; secret.len()];
                    rand::rng().fill_bytes(&mut fragment);
                    for (combined, value) in combined.iter_mut().zip(&fragment) {
                        *combined ^= value;
                    }
                    fragment
                };
                let fragment = Zeroizing::new(fragment);
                distribute_policy_secret(
                    child,
                    &fragment,
                    &format!("{path}/all/{index}"),
                    output,
                )?;
            }
        }
        UnlockPolicy::Threshold {
            group_id,
            required,
            members,
        } => {
            let share_count = u8::try_from(members.len()).context("too many threshold members")?;
            for ((member_id, share), share_index) in members
                .iter()
                .zip(Sharks(*required).dealer(secret))
                .zip(1_u8..)
            {
                output.push(DistributedShare {
                    member_id: member_id.clone(),
                    group_id: group_id.clone(),
                    share_index,
                    threshold: *required,
                    share_count,
                    plaintext: Zeroizing::new(Vec::from(&share)),
                });
            }
        }
    }
    Ok(())
}

fn recover_policy_secret(
    policy: &UnlockPolicy,
    contributions: &[UnwrappedMemberShare],
    path: &str,
) -> Result<Zeroizing<Vec<u8>>> {
    match policy {
        UnlockPolicy::Member { member_id } => {
            let contribution = contributions
                .iter()
                .find(|contribution| &contribution.member_id == member_id)
                .context("missing member contribution")?;
            recover_shamir(std::slice::from_ref(contribution), 1)
        }
        UnlockPolicy::AnyOf { policies } => {
            for (index, child) in policies.iter().enumerate() {
                if let Ok(secret) = recover_policy_secret(
                    child,
                    contributions,
                    &format!("{path}/any/{index}"),
                ) {
                    return Ok(secret);
                }
            }
            bail!("no any_of alternative is satisfied")
        }
        UnlockPolicy::AllOf { policies } => {
            let mut secret: Option<Zeroizing<Vec<u8>>> = None;
            for (index, child) in policies.iter().enumerate() {
                let fragment = recover_policy_secret(
                    child,
                    contributions,
                    &format!("{path}/all/{index}"),
                )?;
                if let Some(secret) = secret.as_mut() {
                    if secret.len() != fragment.len() {
                        bail!("all_of fragments have inconsistent lengths");
                    }
                    for (value, fragment) in secret.iter_mut().zip(fragment.iter()) {
                        *value ^= fragment;
                    }
                } else {
                    secret = Some(fragment);
                }
            }
            secret.context("empty all_of policy")
        }
        UnlockPolicy::Threshold {
            required, members, ..
        } => {
            let shares = contributions
                .iter()
                .filter(|contribution| members.contains(&contribution.member_id))
                .take(usize::from(*required))
                .collect::<Vec<_>>();
            recover_shamir_refs(&shares, *required)
        }
    }
}

fn recover_shamir(contributions: &[UnwrappedMemberShare], required: u8) -> Result<Zeroizing<Vec<u8>>> {
    recover_shamir_refs(&contributions.iter().collect::<Vec<_>>(), required)
}

fn recover_shamir_refs(
    contributions: &[&UnwrappedMemberShare],
    required: u8,
) -> Result<Zeroizing<Vec<u8>>> {
    if contributions.len() < usize::from(required) {
        bail!("insufficient Shamir shares");
    }
    let shares = contributions
        .iter()
        .map(|contribution| {
            Share::try_from(contribution.plaintext.as_slice())
                .map_err(|error| anyhow::anyhow!("decode Shamir share: {error}"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Zeroizing::new(
        Sharks(required)
            .recover(&shares)
            .map_err(|error| anyhow::anyhow!("reconstruct policy secret: {error}"))?,
    ))
}

fn wrap_offline_share(
    header: &CapsuleHeader,
    group_id: &str,
    member_id: &str,
    share_index: u8,
    threshold: u8,
    share_count: u8,
    credential: &MemberCredential<'_>,
    share: &[u8],
) -> Result<MemberShare> {
    let (provider, argon2, kek) = match credential {
        MemberCredential::Passphrase(passphrase) => {
            let mut salt = [0_u8; SALT_BYTES];
            rand::rng().fill_bytes(&mut salt);
            let config = Argon2Config {
                salt: BASE64.encode(salt),
                memory_kib: DEFAULT_MEMORY_KIB,
                iterations: DEFAULT_ITERATIONS,
                parallelism: DEFAULT_PARALLELISM,
            };
            (
                MemberProvider::OfflineArgon2id,
                Some(config.clone()),
                derive_passphrase_kek(passphrase, &config)?,
            )
        }
        MemberCredential::Keyfile(keyfile) => (
            MemberProvider::OfflineKeyfile,
            None,
            derive_keyfile_kek(header, member_id, keyfile)?,
        ),
    };
    let mut nonce = [0_u8; NONCE_BYTES];
    rand::rng().fill_bytes(&mut nonce);
    let aad = share_aad(
        header,
        group_id,
        member_id,
        share_index,
        threshold,
        share_count,
        &provider,
    )?;
    let ciphertext = Aes256Gcm::new_from_slice(kek.as_ref())?
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: share, aad: &aad })
        .map_err(|_| anyhow::anyhow!("wrap member share"))?;
    Ok(MemberShare {
        member_id: member_id.to_owned(),
        group_id: group_id.to_owned(),
        share_index,
        threshold,
        share_count,
        provider,
        key_reference: String::new(),
        wrapped_share: BASE64.encode(ciphertext),
        nonce: Some(BASE64.encode(nonce)),
        argon2,
        principal: None,
        hardware: None,
    })
}

async fn wrap_external_share(
    header: &CapsuleHeader,
    share: &DistributedShare,
    protection: &ExternalMemberProtection<'_>,
) -> Result<MemberShare> {
    validate_external_provider(&protection.provider, protection.key_provider.name())?;
    if protection.key_reference.is_empty() {
        bail!("external member key reference must not be empty");
    }
    let mut member = MemberShare {
        member_id: share.member_id.clone(),
        group_id: share.group_id.clone(),
        share_index: share.share_index,
        threshold: share.threshold,
        share_count: share.share_count,
        provider: protection.provider.clone(),
        key_reference: protection.key_reference.to_owned(),
        wrapped_share: String::new(),
        nonce: None,
        argon2: None,
        principal: protection.principal.clone(),
        hardware: protection.hardware.clone(),
    };
    let purpose = external_share_purpose(header, &member)?;
    let payload = encode_external_share(&purpose, &share.plaintext);
    let ciphertext = protection
        .key_provider
        .wrap(
            &KeyContext {
                repository_id: &header.repository_id,
                slot_id: &member.member_id,
                key_reference: &member.key_reference,
                dek_version: header.root_key_version,
                purpose: &purpose,
            },
            &payload,
        )
        .await
        .context("wrap externally protected member share")?;
    member.wrapped_share = BASE64.encode(ciphertext);
    Ok(member)
}

fn validate_external_provider(provider: &MemberProvider, key_provider: &str) -> Result<()> {
    let valid = matches!(
        (provider, key_provider),
        (MemberProvider::AzureKeyVault, "azure-key-vault")
            | (MemberProvider::AwsKms, "aws-kms")
            | (MemberProvider::AwsCloudhsm, "aws-kms")
            | (MemberProvider::GcpKms | MemberProvider::GcpCloudHsm, "gcp-kms")
            | (MemberProvider::YubikeyPiv, "pkcs11")
            | (MemberProvider::Fido2HmacSecret, "fido2-hmac-secret")
    );
    if !valid {
        bail!("member provider does not match external key provider");
    }
    Ok(())
}

fn external_share_purpose(header: &CapsuleHeader, member: &MemberShare) -> Result<String> {
    let binding = serde_json::to_vec(&(
        "vaultic-recovery-capsule-external-share",
        &header.repository_id,
        header.generation,
        header.root_key_version,
        &header.policy_hash,
        &member.group_id,
        &member.member_id,
        member.share_index,
        member.threshold,
        member.share_count,
        &member.provider,
        &member.key_reference,
        &member.principal,
        &member.hardware,
    ))?;
    Ok(format!(
        "recovery-capsule-share:{}",
        Sha256::digest(binding)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

fn encode_external_share(purpose: &str, share: &[u8]) -> Zeroizing<Vec<u8>> {
    let mut payload = Zeroizing::new(Vec::with_capacity(
        EXTERNAL_SHARE_MAGIC.len() + Sha256::output_size() + share.len(),
    ));
    payload.extend_from_slice(EXTERNAL_SHARE_MAGIC);
    payload.extend_from_slice(&Sha256::digest(purpose.as_bytes()));
    payload.extend_from_slice(share);
    payload
}

fn decode_external_share(purpose: &str, payload: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let prefix_len = EXTERNAL_SHARE_MAGIC.len() + Sha256::output_size();
    if payload.len() <= prefix_len
        || !payload.starts_with(EXTERNAL_SHARE_MAGIC)
        || payload[EXTERNAL_SHARE_MAGIC.len()..prefix_len] != Sha256::digest(purpose.as_bytes())[..]
    {
        bail!("externally wrapped member share context mismatch");
    }
    Ok(Zeroizing::new(payload[prefix_len..].to_vec()))
}

fn unwrap_offline_share(
    header: &CapsuleHeader,
    member: &MemberShare,
    credential: &MemberCredential<'_>,
) -> Result<Zeroizing<Vec<u8>>> {
    let kek = match (&member.provider, credential) {
        (MemberProvider::OfflineArgon2id, MemberCredential::Passphrase(passphrase)) => {
            derive_passphrase_kek(
                passphrase,
                member.argon2.as_ref().context("missing Argon2 parameters")?,
            )?
        }
        (MemberProvider::OfflineKeyfile, MemberCredential::Keyfile(keyfile)) => {
            derive_keyfile_kek(header, &member.member_id, keyfile)?
        }
        _ => bail!("member credential type does not match provider"),
    };
    let nonce = decode_fixed::<NONCE_BYTES>(member.nonce.as_deref().unwrap_or_default(), "share nonce")?;
    let ciphertext = BASE64
        .decode(&member.wrapped_share)
        .context("decode wrapped member share")?;
    let aad = share_aad(
        header,
        &member.group_id,
        &member.member_id,
        member.share_index,
        member.threshold,
        member.share_count,
        &member.provider,
    )?;
    let plaintext = Aes256Gcm::new_from_slice(kek.as_ref())?
        .decrypt(Nonce::from_slice(&nonce), Payload { msg: &ciphertext, aad: &aad })
        .map_err(|_| anyhow::anyhow!("member share authentication failed"))?;
    Ok(Zeroizing::new(plaintext))
}

fn derive_passphrase_kek(passphrase: &[u8], config: &Argon2Config) -> Result<Zeroizing<[u8; 32]>> {
    if config.memory_kib < DEFAULT_MEMORY_KIB
        || config.iterations < DEFAULT_ITERATIONS
        || config.parallelism == 0
    {
        bail!("Argon2id parameters are below the minimum");
    }
    let salt = decode_fixed::<SALT_BYTES>(&config.salt, "Argon2id salt")?;
    let params = Params::new(
        config.memory_kib,
        config.iterations,
        config.parallelism,
        Some(32),
    )
    .map_err(|error| anyhow::anyhow!("invalid Argon2id parameters: {error}"))?;
    let mut key = Zeroizing::new([0_u8; 32]);
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(passphrase, &salt, key.as_mut())
        .map_err(|error| anyhow::anyhow!("derive Argon2id wrapping key: {error}"))?;
    Ok(key)
}

fn derive_keyfile_kek(
    header: &CapsuleHeader,
    member_id: &str,
    keyfile: &[u8],
) -> Result<Zeroizing<[u8; 32]>> {
    if keyfile.len() < 32 {
        bail!("offline keyfile must contain at least 32 bytes");
    }
    let hkdf = Hkdf::<Sha256>::new(Some(header.repository_id.as_bytes()), keyfile);
    let mut key = Zeroizing::new([0_u8; 32]);
    hkdf.expand(
        format!("vaultic-capsule-keyfile\0{}\0{}", header.generation, member_id).as_bytes(),
        key.as_mut(),
    )
    .map_err(|_| anyhow::anyhow!("derive keyfile wrapping key"))?;
    Ok(key)
}

fn wrap_payload(
    header: &CapsuleHeader,
    plaintext: &[u8],
    purpose: &str,
    root_secret: &[u8],
) -> Result<WrappedPayload> {
    let key = derive_payload_key(header, purpose, root_secret)?;
    let mut nonce = [0_u8; NONCE_BYTES];
    rand::rng().fill_bytes(&mut nonce);
    let aad = payload_aad(header, purpose)?;
    let ciphertext = Aes256Gcm::new_from_slice(key.as_ref())?
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: plaintext, aad: &aad })
        .map_err(|_| anyhow::anyhow!("wrap {purpose}"))?;
    Ok(WrappedPayload {
        purpose: purpose.to_owned(),
        nonce: BASE64.encode(nonce),
        ciphertext: BASE64.encode(ciphertext),
    })
}

fn unwrap_payload(
    header: &CapsuleHeader,
    payload: &WrappedPayload,
    purpose: &str,
    root_secret: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    if payload.purpose != purpose {
        bail!("wrapped payload purpose mismatch");
    }
    let key = derive_payload_key(header, purpose, root_secret)?;
    let nonce = decode_fixed::<NONCE_BYTES>(&payload.nonce, "payload nonce")?;
    let ciphertext = BASE64.decode(&payload.ciphertext).context("decode wrapped payload")?;
    let aad = payload_aad(header, purpose)?;
    let plaintext = Aes256Gcm::new_from_slice(key.as_ref())?
        .decrypt(Nonce::from_slice(&nonce), Payload { msg: &ciphertext, aad: &aad })
        .map_err(|_| anyhow::anyhow!("{purpose} authentication failed"))?;
    Ok(Zeroizing::new(plaintext))
}

fn derive_payload_key(
    header: &CapsuleHeader,
    purpose: &str,
    root_secret: &[u8],
) -> Result<Zeroizing<[u8; 32]>> {
    let hkdf = Hkdf::<Sha256>::new(Some(header.repository_id.as_bytes()), root_secret);
    let mut key = Zeroizing::new([0_u8; 32]);
    hkdf.expand(
        format!(
            "vaultic-capsule\0{}\0{}\0{}\0{}",
            header.format, header.generation, header.root_key_version, purpose
        )
        .as_bytes(),
        key.as_mut(),
    )
    .map_err(|_| anyhow::anyhow!("derive {purpose} wrapping key"))?;
    Ok(key)
}

fn payload_aad(header: &CapsuleHeader, purpose: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(&(
        "vaultic-recovery-capsule-payload",
        header,
        purpose,
        if purpose == "metadata-dek" {
            header.metadata_dek_version
        } else {
            header.repository_key_version
        },
    ))
    .context("encode payload binding")
}

fn share_aad(
    header: &CapsuleHeader,
    group_id: &str,
    member_id: &str,
    share_index: u8,
    threshold: u8,
    share_count: u8,
    provider: &MemberProvider,
) -> Result<Vec<u8>> {
    serde_json::to_vec(&(
        "vaultic-recovery-capsule-share",
        &header.repository_id,
        header.generation,
        header.root_key_version,
        &header.policy_hash,
        group_id,
        member_id,
        share_index,
        threshold,
        share_count,
        provider,
    ))
    .context("encode share binding")
}

fn policy_hash(policy: &UnlockPolicy) -> Result<String> {
    Ok(BASE64.encode(Sha256::digest(
        serde_json::to_vec(policy).context("encode unlock policy")?,
    )))
}

fn logical_id(header: &CapsuleHeader) -> String {
    format!(
        "vaultic-capsule/{}/{:020}",
        header.repository_id, header.generation
    )
}

fn decode_fixed<const N: usize>(encoded: &str, name: &str) -> Result<[u8; N]> {
    let decoded = BASE64.decode(encoded).with_context(|| format!("decode {name}"))?;
    decoded.try_into().map_err(|_| anyhow::anyhow!("invalid {name} length"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct ContextProvider {
        name: &'static str,
    }

    #[async_trait]
    impl KeyProvider for ContextProvider {
        fn name(&self) -> &'static str {
            self.name
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

    fn capsule(required: u8) -> RecoveryCapsule {
        CapsuleBuilder::new("repo-a", 7)
            .broker_identity_public_key(&[9; 32])
            .create_offline_threshold(
                "operators",
                required,
                &[
                    ("alice", MemberCredential::Passphrase(b"alice passphrase")),
                    ("bob", MemberCredential::Passphrase(b"bob passphrase")),
                    ("carol", MemberCredential::Keyfile(&[3; 32])),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap()
    }

    #[test]
    fn every_threshold_subset_recovers_and_fewer_fails() {
        let capsule = capsule(2);
        for credentials in [
            BTreeMap::from([
                ("alice".to_owned(), MemberCredential::Passphrase(b"alice passphrase")),
                ("bob".to_owned(), MemberCredential::Passphrase(b"bob passphrase")),
            ]),
            BTreeMap::from([
                ("alice".to_owned(), MemberCredential::Passphrase(b"alice passphrase")),
                ("carol".to_owned(), MemberCredential::Keyfile(&[3; 32])),
            ]),
            BTreeMap::from([
                ("bob".to_owned(), MemberCredential::Passphrase(b"bob passphrase")),
                ("carol".to_owned(), MemberCredential::Keyfile(&[3; 32])),
            ]),
        ] {
            let recovered = capsule.recover_offline(&credentials).unwrap();
            assert_eq!(recovered.metadata_dek.as_slice(), &[7; 32]);
            assert_eq!(recovered.repository_master_key.as_slice(), b"repository-master-key");
        }
        assert!(capsule
            .recover_offline(&BTreeMap::from([(
                "alice".to_owned(),
                MemberCredential::Passphrase(b"alice passphrase"),
            )]))
            .is_err());
    }

    #[tokio::test]
    async fn external_member_wrap_is_context_bound_and_recovers_with_offline_member() {
        let azure = ContextProvider {
            name: "azure-key-vault",
        };
        let aws = ContextProvider { name: "aws-kms" };
        let policy = UnlockPolicy::Threshold {
            group_id: "operators".to_owned(),
            required: 2,
            members: vec!["alice".to_owned(), "bob".to_owned()],
        };
        let capsule = CapsuleBuilder::new("repo-a", 8)
            .broker_identity_public_key(&[9; 32])
            .create_policy(
                policy,
                &[
                    (
                        "alice",
                        MemberProtection::External(ExternalMemberProtection {
                            provider: MemberProvider::AzureKeyVault,
                            key_reference: "https://example.vault.azure.net/keys/alice/version",
                            principal: Some(PrincipalBinding {
                                authority: "entra".to_owned(),
                                tenant_account_or_project: "tenant-a".to_owned(),
                                immutable_principal_id: "object-alice".to_owned(),
                            }),
                            hardware: None,
                            key_provider: &azure,
                        }),
                    ),
                    (
                        "bob",
                        MemberProtection::Offline(MemberCredential::Keyfile(&[4; 32])),
                    ),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .await
            .unwrap();

        let external = capsule.unwrap_external_member("alice", &azure).await.unwrap();
        assert!(capsule.unwrap_external_member("alice", &aws).await.is_err());
        let mut substituted_key = capsule.clone();
        substituted_key.members[0].key_reference =
            "https://example.vault.azure.net/keys/other/version".to_owned();
        assert!(substituted_key
            .unwrap_external_member("alice", &azure)
            .await
            .is_err());
        let mut substituted_principal = capsule.clone();
        substituted_principal.members[0]
            .principal
            .as_mut()
            .unwrap()
            .immutable_principal_id = "object-mallory".to_owned();
        assert!(substituted_principal
            .unwrap_external_member("alice", &azure)
            .await
            .is_err());
        let offline = capsule
            .unwrap_offline_member("bob", &MemberCredential::Keyfile(&[4; 32]))
            .unwrap();
        let recovered = capsule.recover_from_shares(&[external, offline]).unwrap();
        assert_eq!(recovered.metadata_dek.as_slice(), &[7; 32]);
        assert_eq!(
            recovered.repository_master_key.as_slice(),
            b"repository-master-key"
        );
    }

    #[test]
    fn external_share_binding_has_stable_cross_language_fixture() {
        let header = CapsuleHeader {
            format: 2,
            logical_id: "unused".to_owned(),
            repository_id: "repo-a".to_owned(),
            generation: 8,
            root_key_version: 1,
            metadata_dek_version: 1,
            repository_key_version: 1,
            algorithm: "unused".to_owned(),
            policy_hash: "policy-hash".to_owned(),
            broker_identity_public_key: "unused".to_owned(),
            policy_intent: PolicyIntent::Quorum,
        };
        let member = MemberShare {
            member_id: "alice".to_owned(),
            group_id: "operators".to_owned(),
            share_index: 1,
            threshold: 2,
            share_count: 2,
            provider: MemberProvider::AzureKeyVault,
            key_reference: "https://example.vault.azure.net/keys/alice/version".to_owned(),
            wrapped_share: String::new(),
            nonce: None,
            argon2: None,
            principal: Some(PrincipalBinding {
                authority: "entra".to_owned(),
                tenant_account_or_project: "tenant-a".to_owned(),
                immutable_principal_id: "object-alice".to_owned(),
            }),
            hardware: None,
        };
        assert_eq!(
            external_share_purpose(&header, &member).unwrap(),
            "recovery-capsule-share:98436d46b6026a26669db00967c0c1c744f1095700a3c5b73abeddcbf8302306"
        );
    }

    #[test]
    fn both_payloads_must_authenticate() {
        let mut capsule = capsule(1);
        std::mem::swap(&mut capsule.metadata_dek.ciphertext, &mut capsule.repository_master_key.ciphertext);
        let credentials = BTreeMap::from([(
            "alice".to_owned(),
            MemberCredential::Passphrase(b"alice passphrase"),
        )]);
        assert!(capsule.recover_offline(&credentials).is_err());
    }

    #[test]
    fn capsule_authentication_is_location_independent_and_repository_bound() {
        let capsule = capsule(1);
        let exported = serde_json::to_vec(&capsule).unwrap();
        let mirror: RecoveryCapsule = serde_json::from_slice(&exported).unwrap();
        mirror.validate().unwrap();

        let mut foreign = mirror;
        foreign.header.repository_id = "repo-b".to_owned();
        assert!(foreign.validate().is_err());
    }

    #[test]
    fn corrupt_share_binding_fails_closed() {
        let mut capsule = capsule(2);
        capsule.members[0].share_index = capsule.members[1].share_index;
        assert!(capsule.validate().is_err());
    }

    #[test]
    fn composed_policy_requires_password_and_either_hardware_seat() {
        let policy = UnlockPolicy::AllOf {
            policies: vec![
                UnlockPolicy::AnyOf {
                    policies: vec![
                        UnlockPolicy::Member {
                            member_id: "yubikey-primary".to_owned(),
                        },
                        UnlockPolicy::Member {
                            member_id: "yubikey-backup".to_owned(),
                        },
                    ],
                },
                UnlockPolicy::Member {
                    member_id: "offline-password".to_owned(),
                },
            ],
        };
        let capsule = CapsuleBuilder::new("repo-a", 8)
            .broker_identity_public_key(&[9; 32])
            .create_offline_policy(
                policy,
                &[
                    ("yubikey-primary", MemberCredential::Keyfile(&[1; 32])),
                    ("yubikey-backup", MemberCredential::Keyfile(&[2; 32])),
                    (
                        "offline-password",
                        MemberCredential::Passphrase(b"offline passphrase"),
                    ),
                ],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap();
        for hardware in [
            ("yubikey-primary", [1; 32]),
            ("yubikey-backup", [2; 32]),
        ] {
            let credentials = BTreeMap::from([
                (hardware.0.to_owned(), MemberCredential::Keyfile(&hardware.1)),
                (
                    "offline-password".to_owned(),
                    MemberCredential::Passphrase(b"offline passphrase"),
                ),
            ]);
            assert!(capsule.recover_offline(&credentials).is_ok());
        }
        assert!(capsule
            .recover_offline(&BTreeMap::from([(
                "offline-password".to_owned(),
                MemberCredential::Passphrase(b"offline passphrase"),
            )]))
            .is_err());
        assert!(capsule
            .recover_offline(&BTreeMap::from([(
                "yubikey-primary".to_owned(),
                MemberCredential::Keyfile(&[1; 32]),
            )]))
            .is_err());
    }

    #[test]
    fn immutable_local_publication_discovers_latest_generation() {
        let directory = std::env::temp_dir().join(format!(
            "vaultic-capsule-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let first = capsule(1);
        let first_path = publish_local(&directory, &first).unwrap();
        assert_eq!(publish_local(&directory, &first).unwrap(), first_path);
        let latest = discover_latest(&directory, "repo-a").unwrap().unwrap();
        assert_eq!(latest.1.header.generation, 7);

        let mut conflict = first;
        conflict.metadata_dek.ciphertext.push('A');
        assert!(publish_local(&directory, &conflict).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn effective_policy_labels_bootstrap_and_offline_quorum_truthfully() {
        let bootstrap = capsule(1).effective_policy_status().unwrap();
        assert_eq!(bootstrap.minimum_custodians, 1);
        assert!(!bootstrap.compliant);
        assert!(bootstrap.custody_assumed);
        assert!(bootstrap.findings.iter().any(|finding| finding.contains("bootstrap")));

        let quorum = capsule(2).effective_policy_status().unwrap();
        assert_eq!(quorum.minimum_custodians, 2);
        assert!(quorum.compliant);
        assert!(quorum.custody_assumed);
        assert!(!quorum.principal_verified);
    }

    #[test]
    fn assurance_bindings_reject_mismatched_authority_and_duplicate_hardware_key() {
        let mut aws_capsule = capsule(2);
        aws_capsule.members[0].provider = MemberProvider::AwsKms;
        aws_capsule.members[0].nonce = None;
        aws_capsule.members[0].argon2 = None;
        aws_capsule.members[0].key_reference = "arn:aws:kms:us-east-1:123456789012:key/a".to_owned();
        aws_capsule.members[0].principal = Some(PrincipalBinding {
            authority: "entra".to_owned(),
            tenant_account_or_project: "123456789012".to_owned(),
            immutable_principal_id: "arn:aws:iam::123456789012:role/custodian-a".to_owned(),
        });
        assert!(aws_capsule.validate().is_err());

        let mut hardware_capsule = capsule(2);
        for (index, member) in hardware_capsule.members.iter_mut().enumerate() {
            member.provider = MemberProvider::YubikeyPiv;
            member.nonce = None;
            member.argon2 = None;
            member.hardware = Some(HardwareBinding {
                credential_id: format!("credential-{index}"),
                public_key: "same-pinned-public-key".to_owned(),
                serial_number: None,
                attestation_fingerprint: None,
                user_presence_required: true,
            });
        }
        let status = hardware_capsule.effective_policy_status().unwrap();
        assert!(!status.compliant);
        assert!(status.findings.contains(&"duplicate hardware public key".to_owned()));
    }
}