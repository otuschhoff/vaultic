use std::{env, fs, os::unix::fs::PermissionsExt};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use vaulticdb::encryption::envelope::providers::{KeyContext, KeyProvider, YubikeyPivProvider};
use zeroize::{Zeroize, Zeroizing};

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 8 || arguments[0] != "yubikey-piv-unwrap" {
        bail!("usage: vaultic-key-custodian yubikey-piv-unwrap PIN_FILE REPOSITORY_ID MEMBER_ID KEY_REFERENCE ROOT_KEY_VERSION PURPOSE CIPHERTEXT_BASE64");
    }
    let metadata = fs::metadata(&arguments[1]).context("inspect YubiKey PIV PIN file")?;
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!("YubiKey PIV PIN file must not be accessible by group or others");
    }
    let mut pin = fs::read_to_string(&arguments[1]).context("read YubiKey PIV PIN file")?;
    while pin.ends_with(char::is_whitespace) {
        pin.pop();
    }
    if pin.is_empty() {
        bail!("YubiKey PIV PIN file is empty");
    }
    let root_key_version = arguments[5]
        .parse::<u32>()
        .context("invalid root key version")?;
    let ciphertext = Zeroizing::new(
        BASE64
            .decode(&arguments[7])
            .context("decode wrapped PIV share")?,
    );
    let provider = YubikeyPivProvider::new(pin.clone());
    pin.zeroize();
    let plaintext = provider
        .unwrap(
            &KeyContext {
                repository_id: &arguments[2],
                slot_id: &arguments[3],
                key_reference: &arguments[4],
                dek_version: root_key_version,
                purpose: &arguments[6],
            },
            &ciphertext,
        )
        .await?;
    println!("{}", BASE64.encode(plaintext.as_slice()));
    Ok(())
}
