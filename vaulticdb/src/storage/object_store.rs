pub(crate) fn object_store(repository_id: &str) -> Result<(String, Arc<dyn ObjectStore>)> {
    let repository_key = crate::repository_key(repository_id);
    match env::var("VAULTICDB_OBJECT_STORE")
        .unwrap_or_else(|_| "local".to_owned())
        .as_str()
    {
        "local" => {
            let root = env::var("VAULTICDB_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| env::temp_dir().join("vaulticdb").join("data"))
                .join(&repository_key);
            std::fs::create_dir_all(&root)
                .with_context(|| format!("create SlateDB data directory {}", root.display()))?;
            let store = LocalFileSystem::new_with_prefix(&root)
                .with_context(|| format!("open SlateDB data directory {}", root.display()))?;
            Ok(("db".to_owned(), Arc::new(store)))
        }
        "memory" => Ok((repository_key, Arc::new(InMemory::new()))),
        "s3" => {
            let bucket = env::var("VAULTICDB_S3_BUCKET")
                .context("VAULTICDB_S3_BUCKET is required for S3 storage")?;
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .context("configure S3-compatible object store")?;
            let path = match env::var("VAULTICDB_S3_PREFIX") {
                Ok(prefix) if !prefix.trim_matches('/').is_empty() => {
                    format!("{}/{repository_key}", prefix.trim_matches('/'))
                }
                Ok(_) => bail!("VAULTICDB_S3_PREFIX must not be empty"),
                Err(env::VarError::NotPresent) => repository_key,
                Err(error) => return Err(error.into()),
            };
            Ok((path, Arc::new(store)))
        }
        "replicated" => replicated_object_store(&repository_key),
        value => {
            bail!("unsupported VAULTICDB_OBJECT_STORE {value:?}; expected local, memory, s3, or replicated")
        }
    }
}

fn replicated_object_store(repository_key: &str) -> Result<(String, Arc<dyn ObjectStore>)> {
    let replicas = env::var("VAULTICDB_REPLICATED_REPLICAS")
        .context("VAULTICDB_REPLICATED_REPLICAS is required for replicated storage")?;
    let mut stores = Vec::new();
    for raw_id in replicas.split(',') {
        let id = raw_id.trim();
        if id.is_empty() {
            bail!("VAULTICDB_REPLICATED_REPLICAS contains an empty replica ID");
        }
        stores.push((id.to_owned(), replicated_replica_store(id, repository_key)?));
    }
    Ok((
        "db".to_owned(),
        Arc::new(ReplicatedObjectStore::new(stores)?),
    ))
}

fn replicated_replica_store(id: &str, repository_key: &str) -> Result<Arc<dyn ObjectStore>> {
    let prefix = format!("VAULTICDB_REPLICATED_{}", env_id(id));
    match env::var(format!("{prefix}_OBJECT_STORE"))
        .with_context(|| format!("{prefix}_OBJECT_STORE is required"))?
        .as_str()
    {
        "local" => {
            let root = env::var(format!("{prefix}_DATA_DIR"))
                .map(PathBuf::from)
                .with_context(|| format!("{prefix}_DATA_DIR is required for local replica {id}"))?
                .join(repository_key);
            std::fs::create_dir_all(&root).with_context(|| {
                format!(
                    "create replicated SlateDB data directory {}",
                    root.display()
                )
            })?;
            Ok(Arc::new(
                LocalFileSystem::new_with_prefix(&root).with_context(|| {
                    format!("open replicated SlateDB data directory {}", root.display())
                })?,
            ))
        }
        "memory" => Ok(Arc::new(InMemory::new())),
        "s3" => {
            let bucket = env::var(format!("{prefix}_S3_BUCKET"))
                .with_context(|| format!("{prefix}_S3_BUCKET is required for S3 replica {id}"))?;
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .with_context(|| format!("configure S3-compatible object store replica {id}"))?;
            let path = match env::var(format!("{prefix}_S3_PREFIX")) {
                Ok(value) if !value.trim_matches('/').is_empty() => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                Ok(_) => bail!("{prefix}_S3_PREFIX must not be empty"),
                Err(env::VarError::NotPresent) => repository_key.to_owned(),
                Err(error) => return Err(error.into()),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
        "azure" => {
            let account = env::var(format!("{prefix}_AZURE_ACCOUNT")).with_context(|| {
                format!("{prefix}_AZURE_ACCOUNT is required for Azure replica {id}")
            })?;
            let container = env::var(format!("{prefix}_AZURE_CONTAINER")).with_context(|| {
                format!("{prefix}_AZURE_CONTAINER is required for Azure replica {id}")
            })?;
            let mut builder = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container);
            if let Ok(access_key) = env::var(format!("{prefix}_AZURE_ACCESS_KEY")) {
                builder = builder.with_access_key(access_key);
            }
            if let Ok(token) = env::var(format!("{prefix}_AZURE_BEARER_TOKEN")) {
                builder = builder.with_bearer_token_authorization(token);
            }
            let store = builder
                .build()
                .with_context(|| format!("configure Azure Blob object store replica {id}"))?;
            let path = match env::var(format!("{prefix}_AZURE_PREFIX")) {
                Ok(value) if !value.trim_matches('/').is_empty() => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                Ok(_) => bail!("{prefix}_AZURE_PREFIX must not be empty"),
                Err(env::VarError::NotPresent) => repository_key.to_owned(),
                Err(error) => return Err(error.into()),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
        value => {
            bail!(
                "unsupported {prefix}_OBJECT_STORE {value:?}; expected local, memory, s3, or azure"
            )
        }
    }
}

fn env_id(id: &str) -> String {
    id.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}
