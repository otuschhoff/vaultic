use std::{env, path::PathBuf, sync::Arc};

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
pub struct KeyEnvelope {
    pub format: u32,
    pub repository_id: String,
    pub generation: u64,
    pub active_dek_version: u32,
    pub slots: Vec<KeySlot>,
    #[serde(default)]
    pub rotations: Vec<DekRotation>,
    #[serde(default)]
    pub retired_through_dek_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DekRotation {
    pub version: u32,
    pub wrapped_dek: String,
    pub nonce: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscrowRecord {
    pub format: u32,
    pub repository_id: String,
    pub escrow_id: String,
    pub provider: String,
    pub key_reference: String,
    pub wrapped_master_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeySlot {
    pub id: String,
    pub provider: String,
    pub priority: u32,
    pub recovery: bool,
    pub key_reference: String,
    pub dek_version: u32,
    pub wrapped_dek: String,
    pub nonce: String,
    pub argon2: Option<Argon2Config>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Argon2Config {
    pub salt: String,
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    Off,
    Required,
    Initialize,
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
        let cipher = Aes256Gcm::new_from_slice(previous.key.as_ref()).expect("fixed-size DEK");
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
) -> Result<(
    Arc<dyn ObjectStore>,
    EncryptionStatus,
    Option<Arc<KeyManager>>,
)> {
    let mode = encryption_mode()?;
    let envelope = load_envelope(inner.as_ref(), repository_id).await?;

    if mode == EncryptionMode::Off {
        if envelope.is_some() {
            bail!("metadata encryption envelope exists but encryption is disabled");
        }
        return Ok((inner, disabled_status(), None));
    }

    let passphrase = read_recovery_passphrase()?;
    let (envelope, dek, slot_id, recovery_unlock) = match envelope {
        Some(envelope) => {
            let (slot, dek) = unlock_envelope(
                &envelope,
                passphrase.as_deref().map(|value| value.as_slice()),
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
    enforce_recovery_acknowledgement(
        &envelope,
        recovery_unlock,
        env::var("VAULTICDB_ENCRYPTION_RECOVERY_ACK").as_deref() == Ok("true"),
    )?;
    let mut configured = encrypted_store(inner.clone(), repository_id, envelope, dek, &slot_id)?;
    configured.1.initializing = mode == EncryptionMode::Initialize;
    if mode == EncryptionMode::Initialize {
        migrate_plaintext_objects(inner.as_ref(), configured.0.as_ref()).await?;
    }
    Ok(configured)
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
        let Some(provider) = providers::from_environment(&slot.provider).await? else {
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
            repository_id: repository_id.to_owned(),
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
        let cipher = Aes256Gcm::new_from_slice(previous.key.as_ref()).expect("fixed-size DEK");
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
    let length = u32::from_be_bytes(
        payload[prefix + 32..prefix + 36]
            .try_into()
            .expect("fixed-size length"),
    ) as usize;
    if payload.len() != prefix + 36 + length {
        bail!("cloud-wrapped secret length mismatch");
    }
    Ok(Zeroizing::new(payload[prefix + 36..].to_vec()))
}

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
        repository_id: repository_id.to_owned(),
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
    let cipher = Aes256Gcm::new_from_slice(&kek).expect("fixed-size KEK");
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
    let cipher = Aes256Gcm::new_from_slice(&kek).expect("fixed-size KEK");
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

fn read_recovery_passphrase() -> Result<Option<Zeroizing<Vec<u8>>>> {
    let Some(path) = env::var_os("VAULTICDB_ENCRYPTION_PASSPHRASE_FILE") else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("inspect recovery passphrase file {}", path.display()))?;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("recovery passphrase file must not be accessible by group or others");
        }
    }
    let mut value = std::fs::read(&path)
        .with_context(|| format!("read recovery passphrase file {}", path.display()))?;
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value.pop();
    }
    if value.is_empty() {
        bail!("recovery passphrase file is empty");
    }
    Ok(Some(Zeroizing::new(value)))
}

fn encryption_mode() -> Result<EncryptionMode> {
    match env::var("VAULTICDB_ENCRYPTION")
        .unwrap_or_else(|_| "off".to_owned())
        .as_str()
    {
        "off" => Ok(EncryptionMode::Off),
        "required" => Ok(EncryptionMode::Required),
        "initialize" => Ok(EncryptionMode::Initialize),
        value => bail!(
            "unsupported VAULTICDB_ENCRYPTION {value:?}; expected off, required, or initialize"
        ),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn local_slot_round_trip_and_binding() {
        let passphrase = b"correct horse battery staple";
        let (envelope, expected) = new_local_envelope("repo-a", passphrase).unwrap();
        validate_envelope(&envelope, "repo-a").unwrap();
        let (_, actual) = unlock_envelope(&envelope, Some(passphrase)).await.unwrap();
        assert_eq!(actual.as_slice(), expected.as_slice());
        assert!(unlock_envelope(&envelope, Some(b"wrong")).await.is_err());
        assert!(validate_envelope(&envelope, "repo-b").is_err());
    }

    struct MockProvider(&'static str);

    fn mock_binding(context: &KeyContext<'_>) -> [u8; 32] {
        Sha256::digest(format!(
            "{}\0{}\0{}\0{}\0{}",
            context.repository_id,
            context.slot_id,
            context.key_reference,
            context.dek_version,
            context.purpose
        ))
        .into()
    }

    #[async_trait]
    impl KeyProvider for MockProvider {
        fn name(&self) -> &'static str {
            self.0
        }

        async fn wrap(&self, context: &KeyContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>> {
            let mut result = mock_binding(context).to_vec();
            result.extend_from_slice(plaintext);
            Ok(result)
        }

        async fn unwrap(
            &self,
            context: &KeyContext<'_>,
            ciphertext: &[u8],
        ) -> Result<Zeroizing<Vec<u8>>> {
            if ciphertext.len() < 32 || ciphertext[..32] != mock_binding(context) {
                bail!("mock provider context mismatch");
            }
            Ok(Zeroizing::new(ciphertext[32..].to_vec()))
        }
    }

    #[tokio::test]
    async fn cloud_slots_wrap_independently_with_bound_context() {
        for provider_name in ["aws-kms", "azure-key-vault", "gcp-kms"] {
            let provider = MockProvider(provider_name);
            let (mut envelope, dek) = new_local_envelope("repo-a", b"local").unwrap();
            add_cloud_slot(&mut envelope, &dek, "cloud", "versioned-key", 1, &provider)
                .await
                .unwrap();
            let slot = envelope
                .slots
                .iter()
                .find(|slot| slot.id == "cloud")
                .unwrap();
            let ciphertext = BASE64.decode(&slot.wrapped_dek).unwrap();
            let context = KeyContext {
                repository_id: &envelope.repository_id,
                slot_id: &slot.id,
                key_reference: &slot.key_reference,
                dek_version: slot.dek_version,
                purpose: "metadata-dek",
            };
            let payload = provider.unwrap(&context, &ciphertext).await.unwrap();
            assert_eq!(
                decode_cloud_payload(&context, &payload).unwrap().as_slice(),
                dek.as_slice()
            );
            let foreign = KeyContext {
                repository_id: "repo-b",
                ..context
            };
            let payload = provider.unwrap(&foreign, &ciphertext).await;
            assert!(payload.is_err() || decode_cloud_payload(&foreign, &payload.unwrap()).is_err());
        }
    }

    #[tokio::test]
    async fn escrow_recovers_without_metadata_and_rejects_foreign_repository() {
        let provider = MockProvider("aws-kms");
        let master_key = b"base64-direct-open-master-key";
        let record = create_escrow_record(
            "repo-a",
            "primary",
            "arn:aws:kms:region:account:key/version",
            master_key,
            &provider,
        )
        .await
        .unwrap();
        let serialized = serde_json::to_vec(&record).unwrap();
        let standalone: EscrowRecord = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(
            recover_escrow_record(&standalone, "repo-a", &provider)
                .await
                .unwrap()
                .as_slice(),
            master_key
        );
        assert!(recover_escrow_record(&standalone, "repo-b", &provider)
            .await
            .is_err());
    }

    #[test]
    fn weak_argon2_parameters_are_rejected() {
        let config = Argon2Config {
            salt: BASE64.encode([1; SALT_BYTES]),
            memory_kib: 1024,
            iterations: 1,
            parallelism: 1,
        };
        assert!(derive_kek(b"passphrase", &config).is_err());
    }

    #[tokio::test]
    async fn recovery_unlock_with_cloud_slot_requires_explicit_acknowledgement() {
        let (mut envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let provider = MockProvider("azure-key-vault");
        add_cloud_slot(
            &mut envelope,
            &dek,
            "cloud-primary",
            "https://example.vault.azure.net/keys/key/version",
            1,
            &provider,
        )
        .await
        .unwrap();
        assert!(enforce_recovery_acknowledgement(&envelope, true, false).is_err());
        enforce_recovery_acknowledgement(&envelope, true, true).unwrap();
        enforce_recovery_acknowledgement(&envelope, false, false).unwrap();
    }

    #[test]
    fn local_slot_lifecycle_preserves_dek_and_generations() {
        let (mut envelope, dek) = new_local_envelope("repo-a", b"first").unwrap();
        add_local_slot(&mut envelope, &dek, "second", b"second", 10, false).unwrap();
        assert_eq!(envelope.generation, 2);
        let second = envelope
            .slots
            .iter()
            .find(|slot| slot.id == "second")
            .unwrap();
        assert_eq!(
            unwrap_local(&envelope, second, b"second")
                .unwrap()
                .as_slice(),
            dek.as_slice()
        );
        rotate_local_slot(&mut envelope, &dek, "second", b"rotated").unwrap();
        assert_eq!(envelope.generation, 3);
        let second = envelope
            .slots
            .iter()
            .find(|slot| slot.id == "second")
            .unwrap();
        assert!(unwrap_local(&envelope, second, b"second").is_err());
        assert_eq!(
            unwrap_local(&envelope, second, b"rotated")
                .unwrap()
                .as_slice(),
            dek.as_slice()
        );
        remove_slot(&mut envelope, "local-recovery").unwrap();
        assert_eq!(envelope.generation, 4);
        assert!(remove_slot(&mut envelope, "second").is_err());
        assert!(add_local_slot(&mut envelope, &dek, "second", b"x", 1, true).is_err());
    }

    #[tokio::test]
    async fn immutable_envelope_generations_select_latest() {
        let store = InMemory::new();
        let (mut first, dek) = new_local_envelope("repo-a", b"first").unwrap();
        publish_envelope(&store, &first).await.unwrap();
        add_local_slot(&mut first, &dek, "second", b"second", 10, false).unwrap();
        publish_envelope(&store, &first).await.unwrap();
        assert!(publish_envelope(&store, &first).await.is_err());
        let loaded = load_envelope(&store, "repo-a").await.unwrap().unwrap();
        assert_eq!(loaded.generation, 2);
        assert_eq!(loaded.slots.len(), 2);
        assert!(load_envelope(&store, "repo-b").await.is_err());
    }

    #[tokio::test]
    async fn plaintext_migration_is_encrypted_and_idempotent() {
        let raw = Arc::new(InMemory::new());
        let location = Path::from("db/manifest/0001");
        let plaintext = b"sensitive SlateDB manifest";
        raw.put(&location, plaintext.as_slice().into())
            .await
            .unwrap();
        let encrypted: Arc<dyn ObjectStore> = Arc::new(
            EncryptedObjectStore::new(
                raw.clone(),
                "repo-a",
                vec![EncryptionKey::new(1, [7; DEK_BYTES])],
                1,
            )
            .unwrap(),
        );
        migrate_plaintext_objects(raw.as_ref(), encrypted.as_ref())
            .await
            .unwrap();
        migrate_plaintext_objects(raw.as_ref(), encrypted.as_ref())
            .await
            .unwrap();
        let persisted = raw.get(&location).await.unwrap().bytes().await.unwrap();
        assert!(persisted.starts_with(crate::encryption::MAGIC));
        assert!(!persisted
            .windows(plaintext.len())
            .any(|window| window == plaintext));
        assert_eq!(
            encrypted
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            plaintext.as_slice()
        );
    }

    #[tokio::test]
    async fn rewrite_authenticates_then_retires_old_deks_across_restart() {
        let raw = Arc::new(InMemory::new());
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        publish_envelope(raw.as_ref(), &envelope).await.unwrap();
        let (store, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let manager = manager.unwrap();
        let old_location = Path::from("db/old-object");
        store
            .put(&old_location, b"old version".as_slice().into())
            .await
            .unwrap();
        let retired_ciphertext = raw.get(&old_location).await.unwrap().bytes().await.unwrap();
        manager.rotate_dek().await.unwrap();
        assert_eq!(
            manager.audit_objects().await.unwrap().old_version_objects,
            1
        );
        assert_eq!(manager.rewrite_old_deks(10).await.unwrap(), (1, 0));
        let (generation, active_version, _) = manager.status().await;
        assert_eq!((generation, active_version), (3, 2));

        raw.put(&old_location, retired_ciphertext.into())
            .await
            .unwrap();
        assert!(store.get(&old_location).await.is_err());

        let restarted_envelope = load_envelope(raw.as_ref(), "repo-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restarted_envelope.retired_through_dek_version, 1);
        let root = unwrap_local(
            &restarted_envelope,
            &restarted_envelope.slots[0],
            b"recovery",
        )
        .unwrap();
        let (restarted, _, _) =
            encrypted_store(raw, "repo-a", restarted_envelope, root, "local-recovery").unwrap();
        assert!(restarted.get(&old_location).await.is_err());
    }

    #[tokio::test]
    async fn plaintext_object_substitution_is_reported_and_rejected() {
        let raw = Arc::new(InMemory::new());
        let (envelope, dek) = new_local_envelope("repo-a", b"recovery").unwrap();
        let (encrypted, _, manager) =
            encrypted_store(raw.clone(), "repo-a", envelope, dek, "local-recovery").unwrap();
        let location = Path::from("db/substituted-manifest");
        raw.put(
            &location,
            b"plaintext attacker replacement".as_slice().into(),
        )
        .await
        .unwrap();

        let audit = manager.unwrap().audit_objects().await.unwrap();
        assert_eq!(audit.objects, 1);
        assert_eq!(audit.plaintext_objects, 1);
        let error = encrypted.get(&location).await.unwrap_err();
        assert!(crate::encryption::is_integrity_error(&error));
    }
}
