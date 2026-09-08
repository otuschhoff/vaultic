#[derive(Clone, Debug)]
pub(crate) enum ObjectStoreConfig {
    Local { root: PathBuf },
    Memory,
    S3 { bucket: String, prefix: Option<String> },
    Replicated { replicas: Vec<ReplicaConfig> },
}

#[derive(Clone, Debug)]
pub(crate) struct ReplicaConfig {
    pub(crate) id: String,
    pub(crate) store: ReplicaStoreConfig,
}

#[derive(Clone, Debug)]
pub(crate) enum ReplicaStoreConfig {
    Local { root: PathBuf },
    Memory,
    S3 {
        bucket: String,
        prefix: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        access_key_id: Option<Zeroizing<String>>,
        secret_access_key: Option<Zeroizing<String>>,
    },
    Azure {
        account: String,
        container: String,
        prefix: Option<String>,
        access_key: Option<Zeroizing<String>>,
        bearer_token: Option<Zeroizing<String>>,
    },
}

pub(crate) fn object_store(
    repository_id: &str,
    config: &ObjectStoreConfig,
) -> Result<(String, Arc<dyn ObjectStore>)> {
    let repository_key = crate::repository_key(repository_id);
    match config {
        ObjectStoreConfig::Local { root } => {
            let root = root.join(&repository_key);
            std::fs::create_dir_all(&root)
                .with_context(|| format!("create SlateDB data directory {}", root.display()))?;
            let store = LocalFileSystem::new_with_prefix(&root)
                .with_context(|| format!("open SlateDB data directory {}", root.display()))?;
            Ok(("db".to_owned(), Arc::new(store)))
        }
        ObjectStoreConfig::Memory => Ok((repository_key, Arc::new(InMemory::new()))),
        ObjectStoreConfig::S3 { bucket, prefix } => {
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .context("configure S3-compatible object store")?;
            let path = match prefix {
                Some(prefix) => {
                    format!("{}/{repository_key}", prefix.trim_matches('/'))
                }
                None => repository_key,
            };
            Ok((path, Arc::new(store)))
        }
        ObjectStoreConfig::Replicated { replicas } => {
            replicated_object_store(replicas, &repository_key)
        }
    }
}

fn replicated_object_store(
    replicas: &[ReplicaConfig],
    repository_key: &str,
) -> Result<(String, Arc<dyn ObjectStore>)> {
    let mut stores = Vec::new();
    for replica in replicas {
        stores.push((
            replica.id.clone(),
            replica_store(&replica.store, repository_key, &replica.id)?,
        ));
    }
    Ok((
        "db".to_owned(),
        Arc::new(ReplicatedObjectStore::new(stores)?),
    ))
}

fn replicated_replica_store(
    config: &ObjectStoreConfig,
    id: &str,
    repository_key: &str,
) -> Result<Arc<dyn ObjectStore>> {
    let ObjectStoreConfig::Replicated { replicas } = config else {
        bail!("fencing replica requires replicated object storage");
    };
    let replica = replicas
        .iter()
        .find(|replica| replica.id == id)
        .with_context(|| format!("fencing replica {id:?} is not configured"))?;
    replica_store(&replica.store, repository_key, id)
}

fn replica_store(
    config: &ReplicaStoreConfig,
    repository_key: &str,
    id: &str,
) -> Result<Arc<dyn ObjectStore>> {
    match config {
        ReplicaStoreConfig::Local { root } => {
            let root = root.join(repository_key);
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
        ReplicaStoreConfig::Memory => Ok(Arc::new(InMemory::new())),
        ReplicaStoreConfig::S3 {
            bucket,
            prefix,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
        } => {
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if let Some(region) = region {
                builder = builder.with_region(region);
            }
            if let Some(access_key_id) = access_key_id {
                builder = builder.with_access_key_id(access_key_id.as_str());
            }
            if let Some(secret_access_key) = secret_access_key {
                builder = builder.with_secret_access_key(secret_access_key.as_str());
            }
            let store = builder
                .build()
                .with_context(|| format!("configure S3-compatible object store replica {id}"))?;
            let path = match prefix {
                Some(value) => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                None => repository_key.to_owned(),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
        ReplicaStoreConfig::Azure {
            account,
            container,
            prefix,
            access_key,
            bearer_token,
        } => {
            let mut builder = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container);
            if let Some(access_key) = access_key {
                builder = builder.with_access_key(access_key.as_str());
            }
            if let Some(token) = bearer_token {
                builder = builder.with_bearer_token_authorization(token.as_str());
            }
            let store = builder
                .build()
                .with_context(|| format!("configure Azure Blob object store replica {id}"))?;
            let path = match prefix {
                Some(value) => {
                    format!("{}/{repository_key}", value.trim_matches('/'))
                }
                None => repository_key.to_owned(),
            };
            Ok(Arc::new(PrefixStore::new(store, path)))
        }
    }
}
