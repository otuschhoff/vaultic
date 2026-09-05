//! Metadata key envelopes, slots, rotation, escrow, and provider integration.

use std::{path::PathBuf, sync::Arc};

use aes_gcm::{
    aead::{Aead, Payload},
    Aes256Gcm, KeyInit, Nonce,
};
use anyhow::{bail, Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures_util::{StreamExt, TryStreamExt};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use tokio::sync::Mutex;
use zeroize::{Zeroize, Zeroizing};

use super::{EncryptedObjectStore, EncryptionKey};
use crate::ids::RepositoryId;
pub use providers::ProviderCredentials;
use providers::{KeyContext, KeyProvider};

pub mod providers;

const ENVELOPE_PATH: &str = "_vaultic/key-envelope.json";
const ENVELOPE_PREFIX: &str = "_vaultic/key-envelopes";
const ENVELOPE_FORMAT: u32 = 1;
const DEK_BYTES: usize = 32;
const SALT_BYTES: usize = 16;
const NONCE_BYTES: usize = 12;
const DEFAULT_MEMORY_KIB: u32 = 64 * 1024;
const DEFAULT_ITERATIONS: u32 = 3;
const DEFAULT_PARALLELISM: u32 = 1;
const CLOUD_BINDING_MAGIC: &[u8] = b"VLTDBKMS1";
const ROTATION_AAD_PREFIX: &str = "vaulticdb-dek-rotation";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyEnvelope {
    pub(crate) format: u32,
    pub(crate) repository_id: RepositoryId,
    pub(crate) generation: u64,
    pub(crate) active_dek_version: u32,
    pub(crate) slots: Vec<KeySlot>,
    #[serde(default)]
    pub(crate) rotations: Vec<DekRotation>,
    #[serde(default)]
    pub(crate) retired_through_dek_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DekRotation {
    pub(crate) version: u32,
    pub(crate) wrapped_dek: String,
    pub(crate) nonce: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscrowRecord {
    pub format: u32,
    pub repository_id: RepositoryId,
    pub escrow_id: String,
    pub provider: String,
    pub key_reference: String,
    pub wrapped_master_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeySlot {
    pub(crate) id: String,
    pub(crate) provider: String,
    pub(crate) priority: u32,
    pub(crate) recovery: bool,
    pub(crate) key_reference: String,
    pub(crate) dek_version: u32,
    pub(crate) wrapped_dek: String,
    pub(crate) nonce: String,
    pub(crate) argon2: Option<Argon2Config>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Argon2Config {
    pub(crate) salt: String,
    pub(crate) memory_kib: u32,
    pub(crate) iterations: u32,
    pub(crate) parallelism: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    Off,
    Required,
    Initialize,
}

#[derive(Debug)]
pub struct EncryptionConfig {
    pub mode: EncryptionMode,
    pub passphrase_file: Option<PathBuf>,
    pub recovery_acknowledged: bool,
    pub provider_credentials: ProviderCredentials,
}

#[derive(Debug, Clone)]
pub struct EncryptionStatus {
    pub enabled: bool,
    pub algorithm: &'static str,
    pub active_dek_version: u32,
    pub envelope_generation: u64,
    pub unlock_slot: Option<String>,
    pub recovery_unlock: bool,
    pub initializing: bool,
}

#[derive(Debug, Clone)]
pub struct SlotStatus {
    pub id: String,
    pub provider: String,
    pub priority: u32,
    pub recovery: bool,
    pub key_reference: String,
    pub dek_version: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct EncryptionAudit {
    pub objects: u64,
    pub invalid_objects: u64,
    pub plaintext_objects: u64,
    pub old_version_objects: u64,
}

struct KeyState {
    envelope: KeyEnvelope,
    dek: Zeroizing<Vec<u8>>,
}

pub struct KeyManager {
    inner: Arc<dyn ObjectStore>,
    encrypted: EncryptedObjectStore,
    state: Mutex<KeyState>,
    rewrite: Mutex<()>,
}

impl KeyManager {
    fn new(
        inner: Arc<dyn ObjectStore>,
        encrypted: EncryptedObjectStore,
        envelope: KeyEnvelope,
        dek: Zeroizing<Vec<u8>>,
    ) -> Self {
        Self {
            inner,
            encrypted,
            state: Mutex::new(KeyState { envelope, dek }),
            rewrite: Mutex::new(()),
        }
    }

    pub async fn status(&self) -> (u64, u32, Vec<SlotStatus>) {
        let state = self.state.lock().await;
        let slots = state
            .envelope
            .slots
            .iter()
            .map(|slot| SlotStatus {
                id: slot.id.clone(),
                provider: slot.provider.clone(),
                priority: slot.priority,
                recovery: slot.recovery,
                key_reference: slot.key_reference.clone(),
                dek_version: slot.dek_version,
            })
            .collect();
        (
            state.envelope.generation,
            state.envelope.active_dek_version,
            slots,
        )
    }

    pub async fn export_envelope(&self) -> Result<Vec<u8>> {
        let state = self.state.lock().await;
        serde_json::to_vec_pretty(&state.envelope).context("encode metadata key envelope")
    }

    pub async fn active_dek_for_migration(&self) -> Result<Zeroizing<Vec<u8>>> {
        let state = self.state.lock().await;
        let keys = expand_deks(&state.envelope, &state.dek)?;
        let active = keys
            .iter()
            .find(|key| key.version == state.envelope.active_dek_version)
            .context("active metadata DEK is unavailable")?;
        Ok(Zeroizing::new(active.secret().to_vec()))
    }

    pub async fn publish_capsule_mirror(
        &self,
        capsule: &super::recovery_capsule::RecoveryCapsule,
    ) -> Result<String> {
        super::recovery_capsule::publish_mirror(self.inner.as_ref(), capsule).await
    }

    pub async fn add_local_slot(
        &self,
        slot_id: &str,
        passphrase: &[u8],
        priority: u32,
        recovery: bool,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.envelope.clone();
        add_local_slot(
            &mut next, &state.dek, slot_id, passphrase, priority, recovery,
        )?;
        publish_envelope(self.inner.as_ref(), &next).await?;
        state.envelope = next;
        Ok(())
    }

    pub async fn add_cloud_slot(
        &self,
        slot_id: &str,
        key_reference: &str,
        priority: u32,
        provider: &dyn KeyProvider,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.envelope.clone();
        add_cloud_slot(
            &mut next,
            &state.dek,
            slot_id,
            key_reference,
            priority,
            provider,
        )
        .await?;
        publish_envelope(self.inner.as_ref(), &next).await?;
        state.envelope = next;
        Ok(())
    }

    pub async fn remove_slot(&self, slot_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.envelope.clone();
        remove_slot(&mut next, slot_id)?;
        publish_envelope(self.inner.as_ref(), &next).await?;
        state.envelope = next;
        Ok(())
    }

    pub async fn rotate_local_slot(&self, slot_id: &str, passphrase: &[u8]) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.envelope.clone();
        rotate_local_slot(&mut next, &state.dek, slot_id, passphrase)?;
        publish_envelope(self.inner.as_ref(), &next).await?;
        state.envelope = next;
        Ok(())
    }

    pub async fn rotate_dek(&self) -> Result<(u64, u32)> {
        let mut state = self.state.lock().await;
        let keys = expand_deks(&state.envelope, &state.dek)?;
        let previous = keys.last().context("metadata DEK chain is empty")?;
        let version = previous
            .version
            .checked_add(1)
            .context("metadata DEK version overflow")?;
        let mut key = [0u8; DEK_BYTES];
        rand::rng().fill_bytes(&mut key);
        let mut nonce = [0u8; NONCE_BYTES];
        rand::rng().fill_bytes(&mut nonce);
        let cipher = Aes256Gcm::new_from_slice(previous.key.as_ref())
            .map_err(|_| anyhow::anyhow!("initialize metadata DEK rotation cipher"))?;
        let wrapped = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &key,
                    aad: &rotation_aad(&state.envelope.repository_id, version),
                },
            )
            .map_err(|_| anyhow::anyhow!("wrap rotated metadata DEK"))?;
        let mut next = state.envelope.clone();
        next.active_dek_version = version;
        next.generation = next
            .generation
            .checked_add(1)
            .context("metadata key envelope generation overflow")?;
        next.rotations.push(DekRotation {
            version,
            wrapped_dek: BASE64.encode(wrapped),
            nonce: BASE64.encode(nonce),
        });
        validate_envelope(&next, &next.repository_id)?;
        publish_envelope(self.inner.as_ref(), &next).await?;
        self.encrypted
            .install_write_key(EncryptionKey::new(version, key))?;
        state.envelope = next;
        Ok((state.envelope.generation, version))
    }

    pub async fn rewrite_old_deks(&self, max_objects: usize) -> Result<(usize, usize)> {
        if max_objects == 0 || max_objects > 10_000 {
            bail!("DEK rewrite batch size must be between 1 and 10000");
        }
        let _rewrite = self.rewrite.lock().await;
        let active_version = self.state.lock().await.envelope.active_dek_version;
        let mut objects = self.inner.list(None);
        let mut rewritten = 0;
        let mut remaining = 0;
        while let Some(object) = objects.next().await {
            let object = object.context("list object for DEK rewrite")?;
            if object.location.as_ref().starts_with("_vaultic/") {
                continue;
            }
            let raw = self
                .inner
                .get(&object.location)
                .await
                .with_context(|| format!("read DEK rewrite object {}", object.location))?
                .bytes()
                .await?;
            let header = super::decode_header(&raw)?;
            if header.key_version == active_version {
                continue;
            }
            if rewritten >= max_objects {
                remaining += 1;
                continue;
            }
            let plaintext = self.encrypted.get(&object.location).await?.bytes().await?;
            self.encrypted
                .put(&object.location, plaintext.into())
                .await?;
            rewritten += 1;
        }
        if remaining == 0 {
            let audit = self.audit_objects().await?;
            if audit.old_version_objects != 0
                || audit.invalid_objects != 0
                || audit.plaintext_objects != 0
            {
                bail!(
                    "metadata DEKs cannot be retired: {} old-version, {} invalid, and {} plaintext objects remain",
                    audit.old_version_objects,
                    audit.invalid_objects,
                    audit.plaintext_objects
                );
            }
            let mut state = self.state.lock().await;
            let retire_through = state.envelope.active_dek_version.saturating_sub(1);
            if retire_through > state.envelope.retired_through_dek_version {
                let mut next = state.envelope.clone();
                next.retired_through_dek_version = retire_through;
                next.generation = next
                    .generation
                    .checked_add(1)
                    .context("metadata key envelope generation overflow")?;
                validate_envelope(&next, &next.repository_id)?;
                publish_envelope(self.inner.as_ref(), &next).await?;
                self.encrypted
                    .retire_read_keys_before(next.active_dek_version)?;
                state.envelope = next;
            }
        }
        Ok((rewritten, remaining))
    }

    pub async fn audit_objects(&self) -> Result<EncryptionAudit> {
        let state = self.state.lock().await;
        let known_versions = readable_deks(&state.envelope, &state.dek)?
            .into_iter()
            .map(|key| key.version)
            .collect::<std::collections::HashSet<_>>();
        let active_version = state.envelope.active_dek_version;
        drop(state);
        let mut audit = EncryptionAudit {
            objects: 0,
            invalid_objects: 0,
            plaintext_objects: 0,
            old_version_objects: 0,
        };
        let mut objects = self.inner.list(None);
        while let Some(object) = objects.next().await {
            let object = object.context("list object for encryption audit")?;
            if object.location.as_ref().starts_with("_vaultic/") {
                continue;
            }
            audit.objects += 1;
            let raw = self.inner.get(&object.location).await?.bytes().await?;
            if !raw.starts_with(super::MAGIC) {
                audit.plaintext_objects += 1;
                continue;
            }
            match super::decode_header(&raw) {
                Ok(header) if known_versions.contains(&header.key_version) => {
                    if header.key_version != active_version {
                        audit.old_version_objects += 1;
                    }
                    if let Err(error) = self.encrypted.get(&object.location).await?.bytes().await {
                        if super::is_integrity_error(&error) {
                            audit.invalid_objects += 1;
                        } else {
                            return Err(error).context("authenticate object for encryption audit");
                        }
                    }
                }
                _ => audit.invalid_objects += 1,
            }
        }
        Ok(audit)
    }
}

pub async fn configure(
    repository_id: &str,
    inner: Arc<dyn ObjectStore>,
    config: &EncryptionConfig,
) -> Result<(
    Arc<dyn ObjectStore>,
    EncryptionStatus,
    Option<Arc<KeyManager>>,
)> {
    let mode = config.mode;
    let envelope = load_envelope(inner.as_ref(), repository_id).await?;

    if mode == EncryptionMode::Off {
        if envelope.is_some() {
            bail!("metadata encryption envelope exists but encryption is disabled");
        }
        return Ok((inner, disabled_status(), None));
    }

    let passphrase = read_recovery_passphrase(config.passphrase_file.as_deref())?;
    let (envelope, dek, slot_id, recovery_unlock) = match envelope {
        Some(envelope) => {
            let (slot, dek) = unlock_envelope(
                &envelope,
                passphrase.as_deref().map(Vec::as_slice),
                &config.provider_credentials,
            )
            .await?;
            let slot_id = slot.id.clone();
            let recovery = slot.recovery;
            (envelope, dek, slot_id, recovery)
        }
        None if mode == EncryptionMode::Initialize => {
            let passphrase = passphrase
                .as_deref()
                .context("initializing metadata encryption requires a recovery passphrase file")?;
            let (envelope, dek) = new_local_envelope(repository_id, passphrase)?;
            publish_envelope(inner.as_ref(), &envelope).await?;
            (envelope, dek, "local-recovery".to_owned(), true)
        }
        None => bail!("metadata encryption is required but the key envelope is missing"),
    };
    enforce_recovery_acknowledgement(&envelope, recovery_unlock, config.recovery_acknowledged)?;
    let mut configured = encrypted_store(inner.clone(), repository_id, envelope, dek, &slot_id)?;
    configured.1.initializing = mode == EncryptionMode::Initialize;
    if mode == EncryptionMode::Initialize {
        migrate_plaintext_objects(inner.as_ref(), configured.0.as_ref()).await?;
    }
    Ok(configured)
}

pub fn configure_brokered(
    repository_id: &str,
    inner: Arc<dyn ObjectStore>,
    dek: &[u8],
    dek_version: u32,
    capsule_generation: u64,
    initializing: bool,
) -> Result<(
    Arc<dyn ObjectStore>,
    EncryptionStatus,
    Option<Arc<KeyManager>>,
)> {
    if repository_id.is_empty()
        || dek.len() != DEK_BYTES
        || dek_version == 0
        || capsule_generation == 0
    {
        bail!("invalid brokered metadata encryption configuration");
    }
    let key: [u8; DEK_BYTES] = dek
        .try_into()
        .map_err(|_| anyhow::anyhow!("brokered metadata DEK has an invalid length"))?;
    let encrypted: Arc<dyn ObjectStore> = Arc::new(EncryptedObjectStore::new(
        inner,
        repository_id,
        vec![EncryptionKey::new(dek_version, key)],
        dek_version,
    )?);
    Ok((
        encrypted,
        EncryptionStatus {
            enabled: true,
            algorithm: "AES-256-GCM",
            active_dek_version: dek_version,
            envelope_generation: capsule_generation,
            unlock_slot: Some("broker-lease".to_owned()),
            recovery_unlock: false,
            initializing,
        },
        None,
    ))
}

fn enforce_recovery_acknowledgement(
    envelope: &KeyEnvelope,
    recovery_unlock: bool,
    acknowledged: bool,
) -> Result<()> {
    if recovery_unlock
        && envelope.slots.iter().any(|candidate| !candidate.recovery)
        && !acknowledged
    {
        bail!("recovery slot selected while cloud slots exist; explicit recovery acknowledgement is required");
    }
    Ok(())
}

async fn migrate_plaintext_objects(
    inner: &dyn ObjectStore,
    encrypted: &dyn ObjectStore,
) -> Result<()> {
    let objects = inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .context("list objects for metadata encryption migration")?;
    for object in objects {
        if object.location.as_ref().starts_with("_vaultic/") {
            continue;
        }
        let value = inner
            .get(&object.location)
            .await
            .with_context(|| format!("read plaintext migration object {}", object.location))?
            .bytes()
            .await?;
        if value.starts_with(super::MAGIC) {
            continue;
        }
        encrypted
            .put(&object.location, value.into())
            .await
            .with_context(|| format!("encrypt migration object {}", object.location))?;
    }
    Ok(())
}

async fn unlock_envelope<'a>(
    envelope: &'a KeyEnvelope,
    passphrase: Option<&[u8]>,
    provider_credentials: &ProviderCredentials,
) -> Result<(&'a KeySlot, Zeroizing<Vec<u8>>)> {
    let mut slots = envelope.slots.iter().collect::<Vec<_>>();
    slots.sort_by_key(|slot| slot.priority);
    for slot in slots {
        if slot.provider == "local-argon2id" {
            if let Some(passphrase) = passphrase {
                if let Ok(dek) = unwrap_local(envelope, slot, passphrase) {
                    return Ok((slot, dek));
                }
            }
            continue;
        }
        let Some(provider) = providers::from_config(&slot.provider, provider_credentials).await?
        else {
            continue;
        };
        let wrapped = BASE64
            .decode(&slot.wrapped_dek)
            .context("decode cloud-wrapped metadata key")?;
        let context = KeyContext {
            repository_id: &envelope.repository_id,
            slot_id: &slot.id,
            key_reference: &slot.key_reference,
            dek_version: slot.dek_version,
            purpose: "metadata-dek",
        };
        if let Ok(payload) = provider.unwrap(&context, &wrapped).await {
            if let Ok(dek) = decode_cloud_payload(&context, &payload) {
                return Ok((slot, dek));
            }
        }
    }
    bail!("no metadata key slot could be unwrapped")
}

async fn load_envelope(
    inner: &dyn ObjectStore,
    repository_id: &str,
) -> Result<Option<KeyEnvelope>> {
    let prefix = Path::from(ENVELOPE_PREFIX);
    let objects = inner
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .context("list metadata key envelope generations")?;
    let mut latest: Option<KeyEnvelope> = None;
    for object in objects {
        let data = inner
            .get(&object.location)
            .await
            .context("read metadata key envelope generation")?
            .bytes()
            .await?;
        let candidate = parse_envelope(&data, repository_id)?;
        if latest
            .as_ref()
            .is_none_or(|current| candidate.generation > current.generation)
        {
            latest = Some(candidate);
        }
    }
    if latest.is_some() {
        return Ok(latest);
    }
    match inner.get(&Path::from(ENVELOPE_PATH)).await {
        Ok(result) => Ok(Some(parse_envelope(&result.bytes().await?, repository_id)?)),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error).context("read legacy metadata key envelope"),
    }
}

async fn publish_envelope(inner: &dyn ObjectStore, envelope: &KeyEnvelope) -> Result<()> {
    validate_envelope(envelope, &envelope.repository_id)?;
    let path = Path::from(format!(
        "{ENVELOPE_PREFIX}/{:020}.json",
        envelope.generation
    ));
    let encoded = serde_json::to_vec_pretty(envelope)?;
    inner
        .put_opts(&path, encoded.into(), PutOptions::from(PutMode::Create))
        .await
        .with_context(|| {
            format!(
                "publish metadata key envelope generation {}",
                envelope.generation
            )
        })?;
    Ok(())
}

fn encrypted_store(
    inner: Arc<dyn ObjectStore>,
    repository_id: &str,
    envelope: KeyEnvelope,
    dek: Zeroizing<Vec<u8>>,
    slot_id: &str,
) -> Result<(
    Arc<dyn ObjectStore>,
    EncryptionStatus,
    Option<Arc<KeyManager>>,
)> {
    best_effort_lock_memory(&dek);
    let keys = expand_deks(&envelope, &dek)?;
    let keys = keys
        .into_iter()
        .filter(|key| key.version > envelope.retired_through_dek_version)
        .collect();
    let status = EncryptionStatus {
        enabled: true,
        algorithm: "AES-256-GCM",
        active_dek_version: envelope.active_dek_version,
        envelope_generation: envelope.generation,
        unlock_slot: Some(slot_id.to_owned()),
        recovery_unlock: envelope
            .slots
            .iter()
            .any(|slot| slot.id == slot_id && slot.recovery),
        initializing: false,
    };
    let store = EncryptedObjectStore::new(inner, repository_id, keys, envelope.active_dek_version)?;
    let manager = Arc::new(KeyManager::new(
        store.inner.clone(),
        store.clone(),
        envelope,
        dek,
    ));
    Ok((Arc::new(store), status, Some(manager)))
}

fn best_effort_lock_memory(value: &[u8]) {
    #[cfg(unix)]
    unsafe {
        libc::mlock(value.as_ptr().cast(), value.len());
    }
}

fn new_local_envelope(
    repository_id: &str,
    passphrase: &[u8],
) -> Result<(KeyEnvelope, Zeroizing<Vec<u8>>)> {
    let mut dek = Zeroizing::new(vec![0u8; DEK_BYTES]);
    rand::rng().fill_bytes(&mut dek);
    let mut salt = [0u8; SALT_BYTES];
    rand::rng().fill_bytes(&mut salt);
    let config = Argon2Config {
        salt: BASE64.encode(salt),
        memory_kib: DEFAULT_MEMORY_KIB,
        iterations: DEFAULT_ITERATIONS,
        parallelism: DEFAULT_PARALLELISM,
    };
    let mut nonce = [0u8; NONCE_BYTES];
    rand::rng().fill_bytes(&mut nonce);
    let wrapped = wrap_local(
        repository_id,
        "local-recovery",
        1,
        passphrase,
        &config,
        nonce,
        &dek,
    )?;
    Ok((
        KeyEnvelope {
            format: ENVELOPE_FORMAT,
            repository_id: repository_id.into(),
            generation: 1,
            active_dek_version: 1,
            slots: vec![KeySlot {
                id: "local-recovery".to_owned(),
                provider: "local-argon2id".to_owned(),
                priority: 1000,
                recovery: true,
                key_reference: "operator-passphrase".to_owned(),
                dek_version: 1,
                wrapped_dek: BASE64.encode(wrapped),
                nonce: BASE64.encode(nonce),
                argon2: Some(config),
            }],
            rotations: Vec::new(),
            retired_through_dek_version: 0,
        },
        dek,
    ))
}

fn add_local_slot(
    envelope: &mut KeyEnvelope,
    dek: &[u8],
    slot_id: &str,
    passphrase: &[u8],
    priority: u32,
    recovery: bool,
) -> Result<()> {
    if slot_id.is_empty() || envelope.slots.iter().any(|slot| slot.id == slot_id) {
        bail!("metadata key slot ID is empty or already exists");
    }
    if dek.len() != DEK_BYTES {
        bail!("metadata DEK has invalid length");
    }
    let mut salt = [0u8; SALT_BYTES];
    rand::rng().fill_bytes(&mut salt);
    let config = Argon2Config {
        salt: BASE64.encode(salt),
        memory_kib: DEFAULT_MEMORY_KIB,
        iterations: DEFAULT_ITERATIONS,
        parallelism: DEFAULT_PARALLELISM,
    };
    let mut nonce = [0u8; NONCE_BYTES];
    rand::rng().fill_bytes(&mut nonce);
    let wrapped = wrap_local(
        &envelope.repository_id,
        slot_id,
        root_dek_version(envelope)?,
        passphrase,
        &config,
        nonce,
        dek,
    )?;
    envelope.slots.push(KeySlot {
        id: slot_id.to_owned(),
        provider: "local-argon2id".to_owned(),
        priority,
        recovery,
        key_reference: "operator-passphrase".to_owned(),
        dek_version: root_dek_version(envelope)?,
        wrapped_dek: BASE64.encode(wrapped),
        nonce: BASE64.encode(nonce),
        argon2: Some(config),
    });
    envelope.generation = envelope
        .generation
        .checked_add(1)
        .context("metadata key envelope generation overflow")?;
    validate_envelope(envelope, &envelope.repository_id)
}

async fn add_cloud_slot(
    envelope: &mut KeyEnvelope,
    dek: &[u8],
    slot_id: &str,
    key_reference: &str,
    priority: u32,
    provider: &dyn KeyProvider,
) -> Result<()> {
    if slot_id.is_empty() || envelope.slots.iter().any(|slot| slot.id == slot_id) {
        bail!("metadata key slot ID is empty or already exists");
    }
    if key_reference.is_empty() || dek.len() != DEK_BYTES {
        bail!("cloud key reference or metadata DEK is invalid");
    }
    let context = KeyContext {
        repository_id: &envelope.repository_id,
        slot_id,
        key_reference,
        dek_version: root_dek_version(envelope)?,
        purpose: "metadata-dek",
    };
    let payload = encode_cloud_payload(&context, dek)?;
    let wrapped = provider.wrap(&context, &payload).await?;
    envelope.slots.push(KeySlot {
        id: slot_id.to_owned(),
        provider: provider.name().to_owned(),
        priority,
        recovery: false,
        key_reference: key_reference.to_owned(),
        dek_version: root_dek_version(envelope)?,
        wrapped_dek: BASE64.encode(wrapped),
        nonce: "provider-managed".to_owned(),
        argon2: None,
    });
    envelope.generation = envelope
        .generation
        .checked_add(1)
        .context("metadata key envelope generation overflow")?;
    validate_envelope(envelope, &envelope.repository_id)
}

fn root_dek_version(envelope: &KeyEnvelope) -> Result<u32> {
    envelope
        .slots
        .first()
        .map(|slot| slot.dek_version)
        .context("metadata key envelope has no root slot")
}

fn expand_deks(envelope: &KeyEnvelope, root: &[u8]) -> Result<Vec<EncryptionKey>> {
    let root_key: [u8; DEK_BYTES] = root
        .try_into()
        .map_err(|_| anyhow::anyhow!("unwrapped metadata root key has invalid length"))?;
    let mut keys = vec![EncryptionKey::new(root_dek_version(envelope)?, root_key)];
    for rotation in &envelope.rotations {
        let previous = keys.last().context("metadata DEK chain has no root")?;
        let nonce = BASE64
            .decode(&rotation.nonce)
            .context("decode DEK rotation nonce")?;
        let wrapped = BASE64
            .decode(&rotation.wrapped_dek)
            .context("decode rotated metadata DEK")?;
        if nonce.len() != NONCE_BYTES {
            bail!("invalid DEK rotation nonce");
        }
        let cipher = Aes256Gcm::new_from_slice(previous.key.as_ref())
            .map_err(|_| anyhow::anyhow!("initialize metadata DEK rotation cipher"))?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &wrapped,
                    aad: &rotation_aad(&envelope.repository_id, rotation.version),
                },
            )
            .map_err(|_| anyhow::anyhow!("metadata DEK rotation authentication failed"))?;
        let key: [u8; DEK_BYTES] = plaintext
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("rotated metadata DEK has invalid length"))?;
        keys.push(EncryptionKey::new(rotation.version, key));
    }
    Ok(keys)
}

fn rotation_aad(repository_id: &str, version: u32) -> Vec<u8> {
    format!("{ROTATION_AAD_PREFIX}\0{repository_id}\0{version}").into_bytes()
}

fn cloud_binding(context: &KeyContext<'_>) -> [u8; 32] {
    Sha256::digest(format!(
        "vaulticdb-cloud-slot\0{}\0{}\0{}\0{}\0{}",
        context.repository_id,
        context.slot_id,
        context.key_reference,
        context.dek_version,
        context.purpose
    ))
    .into()
}

fn encode_cloud_payload(context: &KeyContext<'_>, dek: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if dek.is_empty() || dek.len() > u32::MAX as usize {
        bail!("cloud-wrapped secret has invalid length");
    }
    let mut payload = Zeroizing::new(Vec::with_capacity(
        CLOUD_BINDING_MAGIC.len() + 32 + 4 + dek.len(),
    ));
    payload.extend_from_slice(CLOUD_BINDING_MAGIC);
    payload.extend_from_slice(&cloud_binding(context));
    payload.extend_from_slice(&(dek.len() as u32).to_be_bytes());
    payload.extend_from_slice(dek);
    Ok(payload)
}

fn decode_cloud_payload(context: &KeyContext<'_>, payload: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let prefix = CLOUD_BINDING_MAGIC.len();
    if payload.len() < prefix + 32 + 4
        || payload[..prefix] != *CLOUD_BINDING_MAGIC
        || payload[prefix..prefix + 32] != cloud_binding(context)
    {
        bail!("cloud-wrapped metadata key binding mismatch");
    }
    let mut length_bytes = [0u8; 4];
    length_bytes.copy_from_slice(&payload[prefix + 32..prefix + 36]);
    let length = u32::from_be_bytes(length_bytes) as usize;
    if payload.len() != prefix + 36 + length {
        bail!("cloud-wrapped secret length mismatch");
    }
    Ok(Zeroizing::new(payload[prefix + 36..].to_vec()))
}

include!("envelope/operations.rs");

include!("envelope/tests.rs");
