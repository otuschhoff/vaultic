use std::sync::Arc;

#[cfg(not(feature = "rados"))]
use anyhow::bail;
use anyhow::Result;
use slatedb::object_store::ObjectStore;
use zeroize::Zeroizing;

#[path = "rados/store.rs"]
#[cfg(any(test, feature = "rados"))]
mod store;

#[cfg(feature = "rados")]
#[path = "rados/native.rs"]
mod native;

pub(crate) struct Config<'a> {
    pub(crate) monitors: &'a str,
    pub(crate) cluster_fsid: &'a str,
    pub(crate) pool: &'a str,
    pub(crate) namespace: &'a str,
    pub(crate) prefix: &'a str,
    pub(crate) client: &'a str,
    pub(crate) key: &'a Zeroizing<String>,
}

#[cfg(not(feature = "rados"))]
pub(crate) fn open(config: Config<'_>) -> Result<Arc<dyn ObjectStore>> {
    let _ = (
        config.monitors,
        config.cluster_fsid,
        config.pool,
        config.namespace,
        config.prefix,
        config.client,
        config.key,
    );
    bail!("native RADOS support is unavailable; rebuild vaulticdb with the rados feature")
}

#[cfg(feature = "rados")]
pub(crate) fn open(config: Config<'_>) -> Result<Arc<dyn ObjectStore>> {
    let prefix = config.prefix.to_owned();
    Ok(Arc::new(store::RadosStore::new(native::open(config)?, &prefix)))
}

#[cfg(all(test, not(feature = "rados")))]
mod tests {
    use super::*;

    #[test]
    fn disabled_build_fails_clearly() {
        let key = Zeroizing::new("secret".to_owned());
        let error = open(Config {
            monitors: "mon:3300",
            cluster_fsid: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
            pool: "pool",
            namespace: "namespace",
            prefix: "prefix",
            client: "client.test",
            key: &key,
        })
        .expect_err("disabled RADOS build must fail");
        assert!(error.to_string().contains("rebuild vaulticdb with the rados feature"));
    }
}