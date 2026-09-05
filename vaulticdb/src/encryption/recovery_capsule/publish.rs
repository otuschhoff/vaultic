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

pub fn discover_latest(
    directory: &Path,
    repository_id: &str,
) -> Result<Option<(PathBuf, RecoveryCapsule)>> {
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
                return Err(error)
                    .context("immutable capsule mirror conflicts with existing generation");
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
    fn member_ids(&self) -> Result<BTreeSet<MemberId>> {
        let mut ids = BTreeSet::new();
        self.collect_member_ids(&mut ids)?;
        Ok(ids)
    }

    fn collect_member_ids(&self, ids: &mut BTreeSet<MemberId>) -> Result<()> {
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

    fn satisfied_by(&self, members: &BTreeSet<MemberId>) -> bool {
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
    member_id: MemberId,
    group_id: String,
    share_index: u8,
    threshold: u8,
    share_count: u8,
    plaintext: Zeroizing<Vec<u8>>,
}
