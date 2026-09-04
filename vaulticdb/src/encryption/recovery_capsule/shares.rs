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
                distribute_policy_secret(child, &fragment, &format!("{path}/all/{index}"), output)?;
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
                if let Ok(secret) =
                    recover_policy_secret(child, contributions, &format!("{path}/any/{index}"))
                {
                    return Ok(secret);
                }
            }
            bail!("no any_of alternative is satisfied")
        }
        UnlockPolicy::AllOf { policies } => {
            let mut secret: Option<Zeroizing<Vec<u8>>> = None;
            for (index, child) in policies.iter().enumerate() {
                let fragment =
                    recover_policy_secret(child, contributions, &format!("{path}/all/{index}"))?;
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

fn recover_shamir(
    contributions: &[UnwrappedMemberShare],
    required: u8,
) -> Result<Zeroizing<Vec<u8>>> {
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
    Ok(Zeroizing::new(Sharks(required).recover(&shares).map_err(
        |error| anyhow::anyhow!("reconstruct policy secret: {error}"),
    )?))
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
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: share,
                aad: &aad,
            },
        )
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
            | (
                MemberProvider::GcpKms | MemberProvider::GcpCloudHsm,
                "gcp-kms"
            )
            | (MemberProvider::YubikeyPiv, "yubikey-piv")
            | (MemberProvider::Fido2HmacSecret, "fido2-hmac-secret")
            | (MemberProvider::MacosSecureEnclave, "macos-secure-enclave")
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
                member
                    .argon2
                    .as_ref()
                    .context("missing Argon2 parameters")?,
            )?
        }
        (MemberProvider::OfflineKeyfile, MemberCredential::Keyfile(keyfile)) => {
            derive_keyfile_kek(header, &member.member_id, keyfile)?
        }
        _ => bail!("member credential type does not match provider"),
    };
    let nonce =
        decode_fixed::<NONCE_BYTES>(member.nonce.as_deref().unwrap_or_default(), "share nonce")?;
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
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad: &aad,
            },
        )
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
        format!(
            "vaultic-capsule-keyfile\0{}\0{}",
            header.generation, member_id
        )
        .as_bytes(),
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
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
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
    let ciphertext = BASE64
        .decode(&payload.ciphertext)
        .context("decode wrapped payload")?;
    let aad = payload_aad(header, purpose)?;
    let plaintext = Aes256Gcm::new_from_slice(key.as_ref())?
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad: &aad,
            },
        )
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
    let decoded = BASE64
        .decode(encoded)
        .with_context(|| format!("decode {name}"))?;
    decoded
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid {name} length"))
}
