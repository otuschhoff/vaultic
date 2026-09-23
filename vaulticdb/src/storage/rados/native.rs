use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use rados_client::{
    Client, ErrorKind, OperationOptions, Pool, ReadOp, SecretKey, SubOperationFlags, WriteOp,
};
use tokio::runtime::Runtime;

use super::{
    store::{
        Driver, DriverError, StoredMeta, StoredObject, WriteMode, ATTRIBUTES_XATTR,
        MAX_ATTRIBUTES_SIZE,
    },
    Config,
};

#[derive(Debug)]
struct Native {
    client: Client,
    pool: Pool,
    runtime: Option<Runtime>,
}

impl Drop for Native {
    fn drop(&mut self) {
        self.client.close();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn client_config(config: &Config<'_>) -> Result<rados_client::Config> {
    let timeout = Duration::from_secs(30);
    Ok(rados_client::Config::default()
        .with_option("mon_host", config.monitors)?
        .with_entity(config.client)?
        .with_cluster_fsid(config.cluster_fsid.to_ascii_lowercase())?
        .with_key(SecretKey::new(config.key.as_bytes())?)
        .with_timeouts(timeout, timeout, timeout)?)
}

pub(super) fn open(config: Config<'_>) -> Result<Arc<dyn Driver>> {
    let settings = client_config(&config)?;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?;
                let client = Client::new(settings).context("create RADOS client")?;
                let result = runtime.block_on(async {
                    client
                        .connect(OperationOptions::default())
                        .await
                        .context("connect RADOS")?;
                    if !client
                        .fsid()
                        .is_some_and(|actual| actual.eq_ignore_ascii_case(config.cluster_fsid))
                    {
                        bail!("RADOS cluster identity does not match sealed identity");
                    }
                    client
                        .open_pool(config.pool, OperationOptions::default())
                        .await?
                        .with_namespace(config.namespace)
                        .context("open RADOS namespace")
                });
                match result {
                    Ok(pool) => Ok(Arc::new(Native {
                        client,
                        pool,
                        runtime: Some(runtime),
                    }) as Arc<dyn Driver>),
                    Err(error) => {
                        client.close();
                        Err(error)
                    }
                }
            })
            .join()
            .map_err(|_| anyhow::anyhow!("RADOS connection thread panicked"))?
    })
}

impl Native {
    fn run<F: Future + Send>(&self, future: F) -> F::Output
    where
        F::Output: Send,
    {
        let runtime = self
            .runtime
            .as_ref()
            .expect("RADOS runtime exists until drop");
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| runtime.block_on(future))
                    .join()
                    .expect("RADOS operation thread panicked")
            })
        } else {
            runtime.block_on(future)
        }
    }
}

impl Driver for Native {
    fn put(
        &self,
        name: &str,
        bytes: Bytes,
        attributes: Bytes,
        mode: WriteMode,
    ) -> std::result::Result<u64, DriverError> {
        self.run(async {
            let object = self.pool.object(name)?;
            let operation = match mode {
                WriteMode::Create => WriteOp::new().create(true)?,
                WriteMode::Update(version) => WriteOp::new().assert_version(version)?,
                WriteMode::Overwrite => WriteOp::new(),
            }
            .write_full(&bytes)?
            .set_xattr(ATTRIBUTES_XATTR, &attributes)?;
            object
                .execute_write(operation, OperationOptions::default())
                .await
                .map(|result| result.version)
        })
        .map_err(|error| map_error(error, mode))
    }

    fn get(
        &self,
        name: &str,
        range: Option<std::ops::Range<u64>>,
    ) -> std::result::Result<StoredObject, DriverError> {
        self.run(async {
            let object = self.pool.object(name).map_err(read_error)?;
            'attempt: for _ in 0..3 {
                let info = object
                    .stat(OperationOptions::default())
                    .await
                    .map_err(read_error)?;
                let selected = range.clone().unwrap_or(0..info.size);
                if selected.start > selected.end || selected.end > info.size {
                    return Err(DriverError::Range);
                }
                let length = usize::try_from(selected.end - selected.start)
                    .map_err(|_| DriverError::Range)?;
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(length)
                    .map_err(|error| DriverError::Other(error.to_string()))?;
                let mut offset = selected.start;
                loop {
                    let chunk = (selected.end - offset).min(4 * 1024 * 1024);
                    let operation = ReadOp::new()
                        .assert_version(info.version)
                        .and_then(|operation| {
                            if chunk == 0 {
                                Ok(operation)
                            } else {
                                operation.read(offset, chunk)
                            }
                        })
                        .and_then(|operation| operation.get_xattr(ATTRIBUTES_XATTR))
                        .and_then(|operation| {
                            operation.set_flags(
                                if chunk == 0 { 1 } else { 2 },
                                SubOperationFlags::FAIL_OK,
                            )
                        })
                        .map_err(read_error)?;
                    let result = match object
                        .execute_read(operation, OperationOptions::default())
                        .await
                    {
                        Ok(result) => result,
                        Err(error) if version_conflict(&error) => continue 'attempt,
                        Err(error) => return Err(read_error(error)),
                    };
                    if result.results.len() != if chunk == 0 { 2 } else { 3 } {
                        return Err(DriverError::Other(
                            "invalid RADOS read result count".to_owned(),
                        ));
                    }
                    let mut results = result.results.into_iter();
                    let assertion = results.next().expect("checked result count");
                    if let Some(error) = assertion.error {
                        if version_conflict(&error) {
                            continue 'attempt;
                        }
                        return Err(read_error(error));
                    }
                    if chunk != 0 {
                        let data = results.next().expect("checked result count");
                        if let Some(error) = data.error {
                            return Err(read_error(error));
                        }
                        if data.data.len() as u64 != chunk {
                            return Err(DriverError::Range);
                        }
                        bytes.extend_from_slice(&data.data);
                    }
                    let attribute = results.next().expect("checked result count");
                    let attributes = match attribute.error {
                        Some(error) if error.wire_errno() == Some(-61) => Vec::new(),
                        Some(error) => return Err(read_error(error)),
                        None => attribute.data,
                    };
                    if attributes.len() > MAX_ATTRIBUTES_SIZE {
                        return Err(DriverError::Other(
                            "RADOS attributes exceed size limit".to_owned(),
                        ));
                    }
                    offset += chunk;
                    if offset == selected.end {
                        return Ok(StoredObject {
                            bytes: bytes.into(),
                            attributes: attributes.into(),
                            size: info.size,
                            version: info.version,
                            modified: info.modified_at,
                        });
                    }
                }
            }
            Err(DriverError::Precondition)
        })
    }

    fn delete(&self, name: &str) -> std::result::Result<(), DriverError> {
        self.run(async {
            self.pool
                .object(name)?
                .remove(OperationOptions::default())
                .await
                .map(|_| ())
        })
        .map_err(read_error)
    }

    fn list(&self, prefix: &str) -> std::result::Result<Vec<StoredMeta>, DriverError> {
        self.run(async {
            let mut cursor = self.pool.begin_object_cursor().map_err(read_error)?;
            let mut result = Vec::new();
            loop {
                let page = self
                    .pool
                    .list_objects(&cursor, 1024, OperationOptions::default())
                    .await
                    .map_err(read_error)?;
                for entry in page.values {
                    let name = String::from_utf8(entry.name).map_err(|_| {
                        DriverError::Other("RADOS object name is not UTF-8".to_owned())
                    })?;
                    if !name.starts_with(prefix) {
                        continue;
                    }
                    let info = self
                        .pool
                        .object(&name)
                        .map_err(read_error)?
                        .stat(OperationOptions::default())
                        .await
                        .map_err(read_error)?;
                    result.push(StoredMeta {
                        name,
                        size: info.size,
                        version: info.version,
                        modified: info.modified_at,
                    });
                }
                if !page.more {
                    break;
                }
                if page.next == cursor {
                    return Err(DriverError::Other(
                        "RADOS listing made no progress".to_owned(),
                    ));
                }
                cursor = page.next;
            }
            Ok(result)
        })
    }
}

fn version_conflict(error: &rados_client::Error) -> bool {
    error.kind() == ErrorKind::Conflict || matches!(error.wire_errno(), Some(-34 | -75))
}
fn read_error(error: rados_client::Error) -> DriverError {
    map_error(error, WriteMode::Overwrite)
}
fn map_error(error: rados_client::Error, mode: WriteMode) -> DriverError {
    match error.kind() {
        ErrorKind::NotFound => DriverError::NotFound,
        ErrorKind::AlreadyExists if matches!(mode, WriteMode::Create) => DriverError::Exists,
        ErrorKind::OutcomeUnknown => DriverError::Other(error.to_string()),
        _ if matches!(mode, WriteMode::Update(_)) && version_conflict(&error) => {
            DriverError::Precondition
        }
        _ if error.wire_errno() == Some(-34) => DriverError::Range,
        _ => DriverError::Other(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rados::store::RadosStore;
    use slatedb::{config::DbReaderOptions, Db, DbReader, DbReaderMode, WriteBatch};
    use zeroize::Zeroizing;

    #[test]
    fn pure_rust_config_preserves_sealed_identity() {
        let key = Zeroizing::new("not-logged-secret".to_owned());
        let config = client_config(&Config {
            monitors: "mon-a:3300,mon-b:3300",
            cluster_fsid: "2F525D6A-8F31-4F79-B731-82A6ACB235F5",
            pool: "pool",
            namespace: "namespace",
            prefix: "prefix",
            client: "client.test",
            key: &key,
        })
        .expect("client configuration");
        assert_eq!(config.option("entity").as_deref(), Some("client.test"));
        assert_eq!(
            config.option("fsid").as_deref(),
            Some("2f525d6a-8f31-4f79-b731-82a6acb235f5")
        );
        assert_eq!(
            config.option("mon_host").as_deref(),
            Some("mon-a:3300,mon-b:3300")
        );
        assert!(!format!("{config:?}").contains(key.as_str()));
    }

    #[tokio::test]
    async fn pure_rust_runtime_can_run_and_drop_inside_tokio() {
        let settings = rados_client::Config::default()
            .with_monitors(["127.0.0.1:3300"])
            .expect("settings");
        let client = Client::new(settings).expect("client");
        let pool = client.pool("pool").expect("unresolved pool");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let driver = Native {
            client,
            pool,
            runtime: Some(runtime),
        };
        assert_eq!(
            driver.run(async {
                tokio::task::yield_now().await;
                42
            }),
            42
        );
        drop(driver);
    }

    #[test]
    fn pure_rust_unknown_write_outcome_is_not_a_conflict() {
        let error = rados_client::Error::outcome_unknown(ErrorKind::Timeout);
        assert!(matches!(
            map_error(error, WriteMode::Update(1)),
            DriverError::Other(_)
        ));
    }

    #[test]
    fn live_rados_attributes_round_trip() {
        let required = |name| std::env::var(name).ok();
        let (
            Some(monitors),
            Some(cluster_fsid),
            Some(pool),
            Some(namespace),
            Some(client),
            Some(secret),
        ) = (
            required("VAULTIC_RADOS_TEST_MONITORS"),
            required("VAULTIC_RADOS_TEST_CLUSTER_FSID"),
            required("VAULTIC_RADOS_TEST_POOL"),
            required("VAULTIC_RADOS_TEST_NAMESPACE"),
            required("VAULTIC_RADOS_TEST_CLIENT"),
            required("VAULTIC_RADOS_TEST_KEY"),
        )
        else {
            return;
        };
        let key = Zeroizing::new(secret);
        let driver = open(Config {
            monitors: &monitors,
            cluster_fsid: &cluster_fsid,
            pool: &pool,
            namespace: &namespace,
            prefix: "live-attributes",
            client: &client,
            key: &key,
        })
        .expect("open live RADOS");
        let name = format!("live-attributes/{}", rand::random::<u128>());
        let first = Bytes::from_static(
            br#"{"version":1,"values":[{"kind":"metadata","key":"put-id","value":"attempt-1"}]}"#,
        );
        let created = driver
            .put(
                &name,
                Bytes::from_static(b"first"),
                first.clone(),
                WriteMode::Create,
            )
            .expect("create attributed object");
        assert_eq!(
            driver
                .get(&name, None)
                .expect("read attributed object")
                .attributes,
            first
        );
        let empty = Bytes::from_static(br#"{"version":1,"values":[]}"#);
        driver
            .put(
                &name,
                Bytes::from_static(b"second"),
                empty.clone(),
                WriteMode::Update(created),
            )
            .expect("replace attributed object");
        assert_eq!(
            driver
                .get(&name, None)
                .expect("read replaced object")
                .attributes,
            empty
        );
        driver.delete(&name).expect("delete attributed object");
    }

    #[tokio::test]
    async fn live_rados_atomicity_reopen_and_isolation() {
        let Ok(monitors) = std::env::var("VAULTIC_RADOS_TEST_MONITORS") else {
            return;
        };
        let Ok(secret) = std::env::var("VAULTIC_RADOS_TEST_KEY") else {
            return;
        };
        let key = Zeroizing::new(secret);
        let config = || Config {
            monitors: &monitors,
            cluster_fsid: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
            pool: "vaultic",
            namespace: "repo",
            prefix: "live-rust",
            client: "client.vaultic",
            key: &key,
        };
        let driver = open(config()).expect("open live RADOS");
        let name = "live-rust/manifest/current";
        let attributes = Bytes::from_static(br#"{"version":1,"values":[]}"#);
        let created = driver
            .put(
                name,
                Bytes::from_static(b"0123456789"),
                attributes.clone(),
                WriteMode::Create,
            )
            .expect("create object");
        assert!(matches!(
            driver.put(
                name,
                Bytes::from_static(b"conflict"),
                attributes.clone(),
                WriteMode::Create
            ),
            Err(DriverError::Exists)
        ));
        assert_eq!(
            &driver.get(name, Some(3..7)).expect("range read").bytes[..],
            b"3456"
        );
        let head = driver.get(name, Some(0..0)).expect("metadata-only read");
        assert!(head.bytes.is_empty());
        assert_eq!(head.size, 10);
        assert_eq!(
            driver.get(name, None).expect("read attributes").attributes,
            attributes
        );
        let updated = driver
            .put(
                name,
                Bytes::from_static(b"updated"),
                attributes.clone(),
                WriteMode::Update(created),
            )
            .expect("conditional update");
        assert!(updated > created);
        assert!(matches!(
            driver.put(
                name,
                Bytes::from_static(b"stale"),
                attributes.clone(),
                WriteMode::Update(created)
            ),
            Err(DriverError::Precondition)
        ));
        assert_eq!(driver.list("live-rust/").expect("list objects").len(), 1);
        drop(driver);
        let reopened = open(config()).expect("reopen live RADOS");
        assert_eq!(
            &reopened.get(name, None).expect("read after reopen").bytes[..],
            b"updated"
        );
        reopened.delete(name).expect("delete live object");

        let denied = Config {
            namespace: "forbidden",
            ..config()
        };
        let denied = open(denied).expect("open forbidden namespace handle");
        assert!(matches!(
            denied.put(
                "live-rust/denied",
                Bytes::from_static(b"denied"),
                attributes,
                WriteMode::Create
            ),
            Err(DriverError::Other(_))
        ));

        let store = std::sync::Arc::new(RadosStore::new(
            open(config()).expect("open SlateDB RADOS store"),
            "live-rust-db",
        ));
        let wal_store = std::sync::Arc::new(RadosStore::new(
            open(config()).expect("open SlateDB RADOS WAL store"),
            "live-rust-wal",
        ));
        let database_path = format!("db-{}", rand::random::<u64>());
        let database = Db::builder(database_path.as_str(), store.clone())
            .with_wal_object_store(wal_store.clone())
            .build()
            .await
            .expect("open SlateDB on RADOS with separate WAL");
        let mut batch = WriteBatch::new();
        batch.put(b"phase27/key", b"phase27/value");
        database
            .write(batch)
            .await
            .expect("write SlateDB value")
            .await_durable()
            .await
            .expect("durable SlateDB write");
        database.close().await.expect("close SlateDB writer");
        let reader = DbReader::builder(database_path.as_str(), store)
            .with_wal_object_store(wal_store)
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_options(DbReaderOptions::default())
            .build()
            .await
            .expect("reopen SlateDB reader with separate RADOS WAL");
        assert_eq!(
            reader
                .get(b"phase27/key")
                .await
                .expect("read reopened SlateDB value")
                .as_deref(),
            Some(b"phase27/value".as_slice())
        );
        reader.close().await.expect("close SlateDB reader");
    }
}
