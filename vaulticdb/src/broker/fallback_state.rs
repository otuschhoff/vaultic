use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

const FORMAT: u32 = 1;
const SIGNING_CONTEXT: &str = "vaultic-static-fallback-state-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateFile {
    format: u32,
    repository_id: String,
    capsule_logical_id: String,
    issued: BTreeSet<(String, u64)>,
    retired: BTreeSet<(String, u64)>,
    signature: String,
}

pub(super) struct FallbackStateStore {
    path: PathBuf,
    repository_id: String,
    capsule_logical_id: String,
}

impl FallbackStateStore {
    pub(super) fn load(
        path: PathBuf,
        repository_id: &str,
        capsule_logical_id: &str,
        verifying_key: &VerifyingKey,
    ) -> Result<(Self, BTreeSet<(String, u64)>, BTreeSet<(String, u64)>)> {
        let store = Self {
            path,
            repository_id: repository_id.to_owned(),
            capsule_logical_id: capsule_logical_id.to_owned(),
        };
        if !store.path.exists() {
            return Ok((store, BTreeSet::new(), BTreeSet::new()));
        }
        require_private_regular_file(&store.path)?;
        let state: StateFile = serde_json::from_slice(&fs::read(&store.path)?)
            .context("decode static fallback lifecycle state")?;
        if state.format != FORMAT
            || state.repository_id != store.repository_id
            || state.capsule_logical_id != store.capsule_logical_id
        {
            bail!("static fallback lifecycle state does not match the recovery capsule");
        }
        if !state.issued.is_disjoint(&state.retired) {
            bail!("static fallback lifecycle state contains conflicting entries");
        }
        let signature = Signature::from_slice(
            &BASE64
                .decode(&state.signature)
                .context("decode static fallback lifecycle signature")?,
        )?;
        verifying_key
            .verify(&signing_bytes(&state)?, &signature)
            .context("verify static fallback lifecycle signature")?;
        Ok((store, state.issued, state.retired))
    }

    pub(super) fn persist(
        &self,
        issued: &BTreeSet<(String, u64)>,
        retired: &BTreeSet<(String, u64)>,
        signing_key: &SigningKey,
    ) -> Result<()> {
        if !issued.is_disjoint(retired) {
            bail!("static fallback lifecycle state contains conflicting entries");
        }
        let mut state = StateFile {
            format: FORMAT,
            repository_id: self.repository_id.clone(),
            capsule_logical_id: self.capsule_logical_id.clone(),
            issued: issued.clone(),
            retired: retired.clone(),
            signature: String::new(),
        };
        state.signature = BASE64.encode(signing_key.sign(&signing_bytes(&state)?).to_bytes());
        let mut encoded = serde_json::to_vec(&state)?;
        encoded.push(b'\n');
        atomic_write(&self.path, &encoded)
    }
}

fn signing_bytes(state: &StateFile) -> Result<Vec<u8>> {
    serde_json::to_vec(&(
        SIGNING_CONTEXT,
        state.format,
        &state.repository_id,
        &state.capsule_logical_id,
        &state.issued,
        &state.retired,
    ))
    .context("encode static fallback lifecycle signature payload")
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("static fallback lifecycle path has no parent")?;
    if !parent.exists() {
        fs::create_dir_all(parent)?;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    if path.exists() {
        require_private_regular_file(path)?;
    }
    let file_name = path
        .file_name()
        .context("static fallback lifecycle path has no file name")?
        .to_string_lossy();
    let temporary = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        rand::random::<u64>()
    ));
    let write_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .with_context(|| format!("create {}", temporary.display()))?;
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result.with_context(|| format!("persist {}", path.display()))
}

fn require_private_regular_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect static fallback lifecycle file {}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        bail!(
            "{} must be a non-symlink regular file with mode 0600 or stricter",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand08::rngs::OsRng;

    #[test]
    fn state_is_signed_and_bound_to_repository() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let state_path = std::env::temp_dir().join(format!(
            "vaultic-fallback-state-{}-{}.state",
            std::process::id(),
            rand::random::<u64>()
        ));
        let (store, mut issued, retired) = FallbackStateStore::load(
            state_path.clone(),
            "repo-a",
            "capsule-a",
            &signing_key.verifying_key(),
        )
        .unwrap();
        issued.insert(("pack:archive".to_owned(), 3));
        store.persist(&issued, &retired, &signing_key).unwrap();

        let (_, loaded, _) = FallbackStateStore::load(
            state_path.clone(),
            "repo-a",
            "capsule-a",
            &signing_key.verifying_key(),
        )
        .unwrap();
        assert_eq!(loaded, issued);
        assert!(FallbackStateStore::load(
            state_path.clone(),
            "repo-b",
            "capsule-a",
            &signing_key.verifying_key(),
        )
        .is_err());

        let mut encoded = std::fs::read(&state_path).unwrap();
        let offset = encoded.iter().position(|byte| *byte == b'3').unwrap();
        encoded[offset] = b'4';
        std::fs::write(&state_path, encoded).unwrap();
        assert!(FallbackStateStore::load(
            state_path.clone(),
            "repo-a",
            "capsule-a",
            &signing_key.verifying_key(),
        )
        .is_err());
        let _ = std::fs::remove_file(state_path);
    }
}
