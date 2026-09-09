#[derive(Clone, Debug)]
pub(crate) enum ObjectStoreConfig {
    Local { root: PathBuf },
    Memory,
    S3 {
        bucket: String,
        prefix: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        provider: Option<String>,
        bucket_lookup: Option<String>,
    },
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
        session_token: Option<Zeroizing<String>>,
        provider: Option<String>,
        bucket_lookup: Option<String>,
    },
    Azure {
        account: String,
        container: String,
        prefix: Option<String>,
        endpoint: String,
        access_key: Option<Zeroizing<String>>,
        bearer_token: Option<Zeroizing<String>>,
        sas_token: Option<Zeroizing<String>>,
    },
    Gcs {
        bucket: String,
        prefix: Option<String>,
        bearer_token: Option<Zeroizing<String>>,
        service_account_key: Option<Zeroizing<String>>,
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
        ObjectStoreConfig::S3 {
            bucket,
            prefix,
            endpoint,
            region,
            provider,
            bucket_lookup,
        } => {
            let builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
            let store = configure_s3_builder(
                builder,
                bucket,
                endpoint.as_deref(),
                region.as_deref(),
                provider.as_deref(),
                bucket_lookup.as_deref(),
            )?
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
            session_token,
            provider,
            bucket_lookup,
        } => {
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
            builder = configure_s3_builder(
                builder,
                bucket,
                endpoint.as_deref(),
                region.as_deref(),
                provider.as_deref(),
                bucket_lookup.as_deref(),
            )?;
            if let Some(access_key_id) = access_key_id {
                builder = builder.with_access_key_id(access_key_id.as_str());
            }
            if let Some(secret_access_key) = secret_access_key {
                builder = builder.with_secret_access_key(secret_access_key.as_str());
            }
            if let Some(session_token) = session_token {
                builder = builder.with_token(session_token.as_str());
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
            endpoint,
            access_key,
            bearer_token,
            sas_token,
        } => {
            let mut builder = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container)
                .with_endpoint(endpoint.clone());
            if let Some(access_key) = access_key {
                builder = builder.with_access_key(access_key.as_str());
            }
            if let Some(token) = bearer_token {
                builder = builder.with_bearer_token_authorization(token.as_str());
            }
            if let Some(token) = sas_token {
                builder = builder.with_sas_authorization(
                    split_sas(token.as_str()).context("parse Azure SAS credential")?,
                );
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
        ReplicaStoreConfig::Gcs {
            bucket,
            prefix,
            bearer_token,
            service_account_key,
        } => {
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
            if let Some(token) = bearer_token {
                builder = builder.with_bearer_token(token.as_str());
            }
            if let Some(key) = service_account_key {
                builder = builder.with_service_account_key(key.as_str());
            }
            let store = builder
                .build()
                .with_context(|| format!("configure GCS object store replica {id}"))?;
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

fn configure_s3_builder(
    mut builder: AmazonS3Builder,
    bucket: &str,
    endpoint: Option<&str>,
    region: Option<&str>,
    provider: Option<&str>,
    bucket_lookup: Option<&str>,
) -> Result<AmazonS3Builder> {
    if provider.is_some_and(|value| !value.is_empty() && value != "generic") && endpoint.is_none() {
        bail!("VaulticDB S3 provider profile requires an explicit endpoint");
    }
    let mut values = std::collections::BTreeMap::new();
    values.insert("bucket".to_owned(), serde_json::Value::String(bucket.to_owned()));
    values.insert(
        "url".to_owned(),
        serde_json::Value::String(endpoint.unwrap_or("https://s3.amazonaws.com").to_owned()),
    );
    values.insert(
        "region".to_owned(),
        serde_json::Value::String(region.unwrap_or_default().to_owned()),
    );
    if let Some(provider) = provider {
        values.insert("provider".to_owned(), serde_json::Value::String(provider.to_owned()));
    }
    if let Some(bucket_lookup) = bucket_lookup {
        values.insert(
            "bucket_lookup".to_owned(),
            serde_json::Value::String(bucket_lookup.to_owned()),
        );
    }
    let profile = vaulticdb::topology::normalize_s3_endpoint(&values)?;
    if endpoint.is_some() {
        builder = builder.with_endpoint(&profile.endpoint);
    }
    if !profile.region.is_empty() {
        builder = builder.with_region(&profile.region);
    }
    builder = builder.with_virtual_hosted_style_request(profile.bucket_lookup == "dns");
    if profile.endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    Ok(builder)
}

#[cfg(test)]
mod object_store_tests {
    use super::*;

    #[test]
    fn named_s3_provider_requires_an_explicit_endpoint() {
        let error = configure_s3_builder(
            AmazonS3Builder::new(),
            "bucket",
            None,
            Some("eu-central-2"),
            Some("wasabi"),
            None,
        )
        .expect_err("named provider without endpoint must fail");
        assert!(error
            .to_string()
            .contains("provider profile requires an explicit endpoint"));
    }
}
