use std::{ffi::{CStr, CString}, os::raw::{c_char, c_int, c_void}, ptr, sync::Mutex, time::{Duration, UNIX_EPOCH}};

use anyhow::{bail, Context, Result};
use bytes::Bytes;

use super::{Config, store::{Driver, DriverError, StoredMeta, StoredObject, WriteMode}};

type Rados = *mut c_void;
type IoCtx = *mut c_void;
type WriteOp = *mut c_void;
type ListCtx = *mut c_void;

#[link(name = "rados")]
unsafe extern "C" {
    fn rados_create2(cluster: *mut Rados, cluster_name: *const c_char, name: *const c_char, flags: u64) -> c_int;
    fn rados_conf_set(cluster: Rados, option: *const c_char, value: *const c_char) -> c_int;
    fn rados_connect(cluster: Rados) -> c_int;
    fn rados_shutdown(cluster: Rados);
    fn rados_cluster_fsid(cluster: Rados, buffer: *mut c_char, length: usize) -> c_int;
    fn rados_ioctx_create(cluster: Rados, pool: *const c_char, ioctx: *mut IoCtx) -> c_int;
    fn rados_ioctx_destroy(ioctx: IoCtx);
    fn rados_ioctx_set_namespace(ioctx: IoCtx, namespace: *const c_char);
    fn rados_stat(ioctx: IoCtx, object: *const c_char, size: *mut u64, mtime: *mut i64) -> c_int;
    fn rados_read(ioctx: IoCtx, object: *const c_char, buffer: *mut c_char, length: usize, offset: u64) -> isize;
    fn rados_remove(ioctx: IoCtx, object: *const c_char) -> c_int;
    fn rados_get_last_version(ioctx: IoCtx) -> u64;
    fn rados_create_write_op() -> WriteOp;
    fn rados_release_write_op(operation: WriteOp);
    fn rados_write_op_create(operation: WriteOp, exclusive: c_int, category: *const c_char);
    fn rados_write_op_assert_version(operation: WriteOp, version: u64);
    fn rados_write_op_write_full(operation: WriteOp, buffer: *const c_char, length: usize);
    fn rados_write_op_operate(operation: WriteOp, ioctx: IoCtx, object: *const c_char, mtime: *mut c_void, flags: c_int) -> c_int;
    fn rados_nobjects_list_open(ioctx: IoCtx, context: *mut ListCtx) -> c_int;
    fn rados_nobjects_list_next(context: ListCtx, entry: *mut *const c_char, key: *mut *const c_char, namespace: *mut *const c_char) -> c_int;
    fn rados_nobjects_list_close(context: ListCtx);
}

#[derive(Debug)]
struct Handles { cluster: Rados, ioctx: IoCtx }
unsafe impl Send for Handles {}

#[derive(Debug)]
struct Native { handles: Mutex<Handles> }

impl Drop for Native {
    fn drop(&mut self) {
        if let Ok(handles) = self.handles.get_mut() { unsafe { rados_ioctx_destroy(handles.ioctx); rados_shutdown(handles.cluster); } }
    }
}

pub(super) fn open(config: Config<'_>) -> Result<std::sync::Arc<dyn Driver>> {
    let cluster_name = cstring("ceph")?;
    let client = cstring(config.client)?;
    let mut cluster = ptr::null_mut();
    check(unsafe { rados_create2(&mut cluster, cluster_name.as_ptr(), client.as_ptr(), 0) }).context("create RADOS connection")?;
    let result = (|| {
        for (option, value) in [("mon_host", config.monitors), ("key", config.key.as_str()), ("rados_osd_op_timeout", "30"), ("rados_mon_op_timeout", "30"), ("client_mount_timeout", "30")] {
            check(unsafe { rados_conf_set(cluster, cstring(option)?.as_ptr(), cstring(value)?.as_ptr()) }).with_context(|| format!("configure RADOS {option}"))?;
        }
        check(unsafe { rados_connect(cluster) }).context("connect RADOS")?;
        let mut fsid = [0 as c_char; 37];
        check(unsafe { rados_cluster_fsid(cluster, fsid.as_mut_ptr(), fsid.len()) }).context("read RADOS cluster FSID")?;
        let actual = unsafe { CStr::from_ptr(fsid.as_ptr()) }.to_str().context("RADOS FSID is not UTF-8")?;
        if !actual.eq_ignore_ascii_case(config.cluster_fsid) { bail!("RADOS cluster identity {actual:?} does not match sealed identity {:?}", config.cluster_fsid); }
        let mut ioctx = ptr::null_mut();
        check(unsafe { rados_ioctx_create(cluster, cstring(config.pool)?.as_ptr(), &mut ioctx) }).context("open RADOS pool")?;
        unsafe { rados_ioctx_set_namespace(ioctx, cstring(config.namespace)?.as_ptr()); }
        Ok(std::sync::Arc::new(Native { handles: Mutex::new(Handles { cluster, ioctx }) }) as std::sync::Arc<dyn Driver>)
    })();
    if result.is_err() { unsafe { rados_shutdown(cluster); } }
    result
}

impl Driver for Native {
    fn put(&self, name: &str, bytes: Bytes, mode: WriteMode) -> std::result::Result<u64, DriverError> {
        let handles = self.handles.lock().map_err(|_| other("RADOS lock poisoned"))?;
        let operation = unsafe { rados_create_write_op() };
        if operation.is_null() { return Err(other("create RADOS write operation failed")); }
        let guard = WriteGuard(operation);
        match mode { WriteMode::Create => unsafe { rados_write_op_create(operation, 1, ptr::null()) }, WriteMode::Update(version) => unsafe { rados_write_op_assert_version(operation, version) }, WriteMode::Overwrite => {} }
        unsafe { rados_write_op_write_full(operation, bytes.as_ptr().cast(), bytes.len()); }
        let object = cstring_driver(name)?;
        let result = unsafe { rados_write_op_operate(operation, handles.ioctx, object.as_ptr(), ptr::null_mut(), 0) };
        drop(guard);
        map_status(result, mode)?;
        Ok(unsafe { rados_get_last_version(handles.ioctx) })
    }

    fn get(&self, name: &str, range: Option<std::ops::Range<u64>>) -> std::result::Result<StoredObject, DriverError> {
        let handles = self.handles.lock().map_err(|_| other("RADOS lock poisoned"))?;
        let object = cstring_driver(name)?;
        for _ in 0..3 {
            let mut size = 0; let mut mtime = 0;
            map_status(unsafe { rados_stat(handles.ioctx, object.as_ptr(), &mut size, &mut mtime) }, WriteMode::Overwrite)?;
            let version = unsafe { rados_get_last_version(handles.ioctx) };
            let selected = range.clone().unwrap_or(0..size);
            if selected.start > selected.end || selected.end > size { return Err(DriverError::Range); }
            let mut bytes = vec![0_u8; (selected.end - selected.start) as usize];
            let read = unsafe { rados_read(handles.ioctx, object.as_ptr(), bytes.as_mut_ptr().cast(), bytes.len(), selected.start) };
            if read < 0 { map_status(read as c_int, WriteMode::Overwrite)?; }
            if read as usize != bytes.len() { return Err(DriverError::Range); }
            let after = unsafe { rados_get_last_version(handles.ioctx) };
            if after == version { return Ok(StoredObject { bytes: Bytes::from(bytes), size, version, modified: UNIX_EPOCH + Duration::from_secs(mtime.max(0) as u64) }); }
        }
        Err(DriverError::Precondition)
    }

    fn delete(&self, name: &str) -> std::result::Result<(), DriverError> {
        let handles = self.handles.lock().map_err(|_| other("RADOS lock poisoned"))?;
        map_status(unsafe { rados_remove(handles.ioctx, cstring_driver(name)?.as_ptr()) }, WriteMode::Overwrite)
    }

    fn list(&self, prefix: &str) -> std::result::Result<Vec<StoredMeta>, DriverError> {
        let handles = self.handles.lock().map_err(|_| other("RADOS lock poisoned"))?;
        let mut context = ptr::null_mut();
        map_status(unsafe { rados_nobjects_list_open(handles.ioctx, &mut context) }, WriteMode::Overwrite)?;
        let guard = ListGuard(context); let mut result = Vec::new();
        loop {
            let mut entry = ptr::null(); let mut key = ptr::null(); let mut namespace = ptr::null();
            let status = unsafe { rados_nobjects_list_next(context, &mut entry, &mut key, &mut namespace) };
            if status == -2 { break; }
            map_status(status, WriteMode::Overwrite)?;
            let name = unsafe { CStr::from_ptr(entry) }.to_string_lossy().into_owned();
            if name.starts_with(prefix) { let mut size = 0; let mut mtime = 0; map_status(unsafe { rados_stat(handles.ioctx, entry, &mut size, &mut mtime) }, WriteMode::Overwrite)?; let version = unsafe { rados_get_last_version(handles.ioctx) }; result.push(StoredMeta { name, size, version, modified: UNIX_EPOCH + Duration::from_secs(mtime.max(0) as u64) }); }
        }
        drop(guard); Ok(result)
    }
}

struct WriteGuard(WriteOp); impl Drop for WriteGuard { fn drop(&mut self) { unsafe { rados_release_write_op(self.0); } } }
struct ListGuard(ListCtx); impl Drop for ListGuard { fn drop(&mut self) { unsafe { rados_nobjects_list_close(self.0); } } }
fn cstring(value: &str) -> Result<CString> { CString::new(value).context("RADOS value contains NUL") }
fn cstring_driver(value: &str) -> std::result::Result<CString, DriverError> { CString::new(value).map_err(|_| other("RADOS object name contains NUL")) }
fn check(status: c_int) -> Result<()> { if status < 0 { Err(std::io::Error::from_raw_os_error(-status).into()) } else { Ok(()) } }
fn other(message: impl Into<String>) -> DriverError { DriverError::Other(message.into()) }
fn map_status(status: c_int, mode: WriteMode) -> std::result::Result<(), DriverError> { if status >= 0 { return Ok(()); } match -status { 2 => Err(DriverError::NotFound), 17 if matches!(mode, WriteMode::Create) => Err(DriverError::Exists), 34 | 75 if matches!(mode, WriteMode::Update(_)) => Err(DriverError::Precondition), 34 => Err(DriverError::Range), errno => Err(other(std::io::Error::from_raw_os_error(errno).to_string())) } }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rados::store::RadosStore;
    use slatedb::{config::DbReaderOptions, Db, DbReader, DbReaderMode, WriteBatch};
    use zeroize::Zeroizing;

    #[tokio::test]
    async fn live_rados_atomicity_reopen_and_isolation() {
        let Ok(monitors) = std::env::var("VAULTIC_RADOS_TEST_MONITORS") else { return; };
        let Ok(secret) = std::env::var("VAULTIC_RADOS_TEST_KEY") else { return; };
        let key = Zeroizing::new(secret);
        let config = || Config {
            monitors: &monitors, cluster_fsid: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
            pool: "vaultic", namespace: "repo", prefix: "live-rust", client: "client.vaultic", key: &key,
        };
        let driver = open(config()).expect("open live RADOS");
        let name = "live-rust/manifest/current";
        let created = driver.put(name, Bytes::from_static(b"0123456789"), WriteMode::Create).expect("create object");
        assert!(matches!(driver.put(name, Bytes::from_static(b"conflict"), WriteMode::Create), Err(DriverError::Exists)));
        assert_eq!(&driver.get(name, Some(3..7)).expect("range read").bytes[..], b"3456");
        let updated = driver.put(name, Bytes::from_static(b"updated"), WriteMode::Update(created)).expect("conditional update");
        assert!(updated > created);
        assert!(matches!(driver.put(name, Bytes::from_static(b"stale"), WriteMode::Update(created)), Err(DriverError::Precondition)));
        assert_eq!(driver.list("live-rust/").expect("list objects").len(), 1);
        drop(driver);
        let reopened = open(config()).expect("reopen live RADOS");
        assert_eq!(&reopened.get(name, None).expect("read after reopen").bytes[..], b"updated");
        reopened.delete(name).expect("delete live object");

        let denied = Config { namespace: "forbidden", ..config() };
        let denied = open(denied).expect("open forbidden namespace handle");
        assert!(matches!(denied.put("live-rust/denied", Bytes::from_static(b"denied"), WriteMode::Create), Err(DriverError::Other(_))));

        let store = std::sync::Arc::new(RadosStore::new(open(config()).expect("open SlateDB RADOS store"), "live-rust-db"));
        let database_path = format!("db-{}", rand::random::<u64>());
        let database = Db::open(database_path.as_str(), store.clone()).await.expect("open SlateDB on RADOS");
        let mut batch = WriteBatch::new();
        batch.put(b"phase27/key", b"phase27/value");
        database.write(batch).await.expect("write SlateDB value").await_durable().await.expect("durable SlateDB write");
        database.close().await.expect("close SlateDB writer");
        let reader = DbReader::open(database_path.as_str(), store, DbReaderMode::FollowLatest, DbReaderOptions::default()).await.expect("reopen SlateDB reader");
        assert_eq!(reader.get(b"phase27/key").await.expect("read reopened SlateDB value").as_deref(), Some(b"phase27/value".as_slice()));
        reader.close().await.expect("close SlateDB reader");
    }
}