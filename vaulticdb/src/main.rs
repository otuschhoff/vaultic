#![allow(clippy::result_large_err)]

use std::{
    env,
    fs::File,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use ipnet::IpNet;
use prost::Message;
use sha2::{Digest, Sha256};
use slatedb::config::DbReaderOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbReader, DbReaderMode, WriteBatch};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::mpsc,
    sync::watch,
};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{transport::Server, Request, Response, Status};

pub mod proto {
    tonic::include_proto!("vaulticdb.v1");
}

use proto::{
    vaultic_db_server::{VaulticDb, VaulticDbServer},
    BeginResponse, CapabilitiesRequest, CapabilitiesResponse, CommitResponse, Empty, GetRequest,
    GetResponse, HealthRequest, HealthResponse, MultiGetRequest, MultiGetResponse, RequestContext,
    ScanRequest, ScanResponse, TransactionRequest, WriteBatchRequest, WriteBatchResponse,
};

mod storage;

use storage::{repeated_message_encoded_len, Storage};

const PROTOCOL_VERSION: &str = "vaulticdb.v1";
const SCHEMA_VERSION: &str = "0";
const MAX_BATCH_ITEMS: u32 = 10_000;
const MAX_PAGE_ITEMS: u32 = 1_000;
const MAX_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 128;

#[derive(Clone)]
struct DaemonState {
    daemon_id: Arc<str>,
    repository_id: Arc<str>,
    auth_token: Option<Arc<str>>,
    unix_socket: bool,
    tcp_enabled: bool,
    draining: Arc<AtomicBool>,
}

#[derive(Clone)]
struct Service {
    state: DaemonState,
    shutdown: watch::Sender<bool>,
    storage: Arc<Storage>,
}

#[tonic::async_trait]
impl VaulticDb for Service {
    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        check_request(
            &self.state,
            &request,
            request.get_ref().repository_id.as_str(),
        )?;
        check_context(request.get_ref().context.as_ref())?;
        Ok(Response::new(HealthResponse {
            daemon_id: self.state.daemon_id.to_string(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            schema_version: SCHEMA_VERSION.to_owned(),
            repository_id: self.state.repository_id.to_string(),
            slate_db_revision: String::new(),
            ready: !self.state.draining.load(Ordering::Acquire),
        }))
    }

    async fn capabilities(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        check_request(
            &self.state,
            &request,
            request.get_ref().repository_id.as_str(),
        )?;
        check_context(request.get_ref().context.as_ref())?;
        Ok(Response::new(CapabilitiesResponse {
            daemon_id: self.state.daemon_id.to_string(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            schema_version: SCHEMA_VERSION.to_owned(),
            repository_id: self.state.repository_id.to_string(),
            unix_socket: self.state.unix_socket,
            tcp_enabled: self.state.tcp_enabled,
            max_batch_items: MAX_BATCH_ITEMS,
            max_message_bytes: MAX_MESSAGE_BYTES,
            max_page_items: MAX_PAGE_ITEMS,
            max_concurrent_requests: MAX_CONCURRENT_REQUESTS as u32,
        }))
    }

    async fn drain(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        check_request(&self.state, &request, "")?;
        check_context(request.get_ref().context.as_ref())?;
        self.state.draining.store(true, Ordering::Release);
        Ok(Response::new(Empty { context: None }))
    }

    async fn shutdown(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        check_request(&self.state, &request, "")?;
        check_context(request.get_ref().context.as_ref())?;
        self.state.draining.store(true, Ordering::Release);
        let _ = self.shutdown.send(true);
        Ok(Response::new(Empty { context: None }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        let request = request.into_inner();
        Ok(Response::new(
            self.storage
                .get(&request.key, &request.transaction_id)
                .await?,
        ))
    }

    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        if request.get_ref().keys.len() > MAX_BATCH_ITEMS as usize {
            return Err(Status::resource_exhausted("multi-get item limit exceeded"));
        }
        let request = request.into_inner();
        let mut results = Vec::with_capacity(request.keys.len());
        let mut response_bytes = 0usize;
        for key in request.keys {
            let result = self.storage.get(&key, &request.transaction_id).await?;
            response_bytes = response_bytes
                .checked_add(repeated_message_encoded_len(result.encoded_len()))
                .ok_or_else(|| Status::resource_exhausted("multi-get response size overflow"))?;
            if response_bytes > MAX_MESSAGE_BYTES as usize {
                return Err(Status::resource_exhausted(
                    "multi-get response byte limit exceeded",
                ));
            }
            results.push(result);
        }
        Ok(Response::new(MultiGetResponse { results }))
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        validate_scan(request.get_ref())?;
        let request = request.into_inner();
        Ok(Response::new(
            self.storage
                .scan(
                    &request.prefix,
                    &request.after_key,
                    request.page_size as usize,
                    &request.transaction_id,
                )
                .await?,
        ))
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        validate_write_batch(request.get_ref())?;
        let durable = self.storage.write_batch(request.get_ref()).await?;
        Ok(Response::new(WriteBatchResponse { durable }))
    }

    async fn begin(&self, request: Request<Empty>) -> Result<Response<BeginResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        Ok(Response::new(BeginResponse {
            transaction_id: self.storage.begin().await?,
        }))
    }

    async fn commit(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        self.storage
            .commit(&request.get_ref().transaction_id)
            .await?;
        Ok(Response::new(CommitResponse { durable: true }))
    }

    async fn rollback(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<Empty>, Status> {
        check_storage_request(&self.state, &request, request.get_ref().context.as_ref())?;
        self.storage
            .rollback(&request.get_ref().transaction_id)
            .await?;
        Ok(Response::new(Empty { context: None }))
    }
}

fn check_storage_request<T>(
    state: &DaemonState,
    request: &Request<T>,
    context: Option<&RequestContext>,
) -> Result<(), Status> {
    check_request(state, request, "")?;
    check_context(context)?;
    if state.draining.load(Ordering::Acquire) {
        return Err(Status::unavailable("vaulticdb is draining"));
    }
    Ok(())
}

fn check_context(context: Option<&RequestContext>) -> Result<(), Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("request context is required"))?;
    if context.request_id.is_empty() {
        return Err(Status::invalid_argument("request ID is required"));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time is before Unix epoch"))?
        .as_millis() as i64;
    if context.deadline_unix_ms > 0 && context.deadline_unix_ms <= now {
        return Err(Status::deadline_exceeded("request deadline has expired"));
    }
    Ok(())
}

pub fn validate_write_batch(request: &WriteBatchRequest) -> Result<(), Status> {
    let item_count = request
        .puts
        .len()
        .checked_add(request.deletes.len())
        .ok_or_else(|| Status::resource_exhausted("batch item count overflow"))?;
    if item_count > MAX_BATCH_ITEMS as usize {
        return Err(Status::resource_exhausted("batch item limit exceeded"));
    }
    if request.encoded_len() > MAX_MESSAGE_BYTES as usize {
        return Err(Status::resource_exhausted("batch byte limit exceeded"));
    }
    Ok(())
}

pub fn validate_scan(request: &ScanRequest) -> Result<(), Status> {
    if request.page_size == 0 || request.page_size > MAX_PAGE_ITEMS {
        return Err(Status::invalid_argument(
            "scan page size is outside the supported range",
        ));
    }
    Ok(())
}

fn check_repository(state: &DaemonState, requested: &str) -> Result<(), Status> {
    if requested.is_empty() || requested == state.repository_id.as_ref() {
        return Ok(());
    }
    Err(Status::failed_precondition("repository identity mismatch"))
}

fn check_request<T>(
    state: &DaemonState,
    request: &Request<T>,
    repository_id: &str,
) -> Result<(), Status> {
    if let Some(token) = &state.auth_token {
        let expected = format!("Bearer {token}");
        if request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
        {
            return Err(Status::unauthenticated("invalid vaulticdb authorization"));
        }
    }
    check_repository(state, repository_id)
}

#[derive(Debug)]
enum Transport {
    Unix(PathBuf),
    Tcp(SocketAddr, Vec<IpNet>),
}

fn parse_transport(repository_id: &str) -> Result<Transport> {
    let transport = env::var("VAULTICDB_TRANSPORT").unwrap_or_else(|_| "unix".to_owned());
    match transport.as_str() {
        "unix" => Ok(Transport::Unix(PathBuf::from(
            env::var("VAULTICDB_SOCKET").unwrap_or_else(|_| default_socket_path(repository_id)),
        ))),
        "tcp" => {
            if env::var("VAULTICDB_TCP_ALLOWLIST")
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                bail!("VAULTICDB_TCP_ALLOWLIST is required when TCP transport is enabled")
            }
            if env::var("VAULTICDB_TCP_AUTH_TOKEN")
                .unwrap_or_default()
                .is_empty()
            {
                bail!("VAULTICDB_TCP_AUTH_TOKEN is required when TCP transport is enabled")
            }
            let addr =
                env::var("VAULTICDB_TCP_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_owned());
            let allowlist = env::var("VAULTICDB_TCP_ALLOWLIST")?
                .split(',')
                .map(|value| value.trim().parse().context("invalid IP allowlist entry"))
                .collect::<Result<Vec<IpNet>>>()?;
            Ok(Transport::Tcp(
                addr.parse().context("invalid VAULTICDB_TCP_ADDR")?,
                allowlist,
            ))
        }
        other => bail!("unsupported VAULTICDB_TRANSPORT {other:?}; expected unix or tcp"),
    }
}

fn default_socket_path(repository_id: &str) -> String {
    let runtime_dir =
        env::var("VAULTICDB_RUNTIME_DIR").unwrap_or_else(|_| "/tmp/vaulticdb".to_owned());
    let digest = Sha256::digest(if repository_id.is_empty() {
        b"default"
    } else {
        repository_id.as_bytes()
    });
    format!("{runtime_dir}/{digest:x}.sock")
}

fn repository_key(repository_id: &str) -> String {
    let digest = Sha256::digest(if repository_id.is_empty() {
        b"default"
    } else {
        repository_id.as_bytes()
    });
    format!("{digest:x}")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if env::var_os("VAULTICDB_NATIVE_SMOKE").is_some() {
        return native_smoke().await;
    }

    let repository_id = env::var("VAULTICDB_REPOSITORY_ID").unwrap_or_default();
    let transport = parse_transport(&repository_id)?;
    let auth_token = env::var("VAULTICDB_TCP_AUTH_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let state = DaemonState {
        daemon_id: Arc::from(
            env::var("VAULTICDB_DAEMON_ID").unwrap_or_else(|_| "vaulticdb-dev".to_owned()),
        ),
        repository_id: Arc::from(repository_id),
        auth_token: auth_token.map(Arc::from),
        unix_socket: matches!(&transport, Transport::Unix(_)),
        tcp_enabled: matches!(&transport, Transport::Tcp(_, _)),
        draining: Arc::new(AtomicBool::new(false)),
    };
    let tcp_enabled = matches!(transport, Transport::Tcp(_, _));
    let (shutdown, shutdown_rx) = watch::channel(false);

    match transport {
        Transport::Unix(path) => {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
                set_private_directory_permissions(parent)?;
            }
            let lock_path = path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            remove_stale_socket(&path).await?;
            write_runtime_metadata(&path, false)?;
            let listener = match UnixListener::bind(&path) {
                Ok(listener) => listener,
                Err(error) => {
                    remove_runtime_metadata(&path);
                    return Err(error)
                        .with_context(|| format!("bind Unix socket {}", path.display()));
                }
            };
            if let Err(error) = set_private_socket_permissions(&path) {
                let _ = tokio::fs::remove_file(&path).await;
                remove_runtime_metadata(&path);
                return Err(error);
            }
            let storage = Arc::new(Storage::open(state.repository_id.as_ref()).await?);
            let service = storage_service(state.clone(), shutdown.clone(), storage.clone());
            let stream = UnixListenerStream::new(listener);
            let result = Server::builder()
                .concurrency_limit_per_connection(MAX_CONCURRENT_REQUESTS)
                .add_service(service)
                .serve_with_incoming_shutdown(stream, shutdown_signal(shutdown_rx))
                .await;
            let close_result = storage.close().await;
            drop(_lock);
            let _ = tokio::fs::remove_file(&path).await;
            remove_runtime_metadata(&path);
            result?;
            close_result?;
        }
        Transport::Tcp(addr, allowlist) => {
            let metadata_path = env::var("VAULTICDB_TCP_METADATA")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    PathBuf::from(
                        env::var("VAULTICDB_RUNTIME_DIR")
                            .unwrap_or_else(|_| "/tmp/vaulticdb".to_owned()),
                    )
                    .join("vaulticdb-tcp")
                });
            let listener = TcpListener::bind(addr).await.context("bind TCP listener")?;
            if let Some(parent) = metadata_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
                set_private_directory_permissions(parent)?;
            }
            let lock_path = metadata_path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            write_runtime_metadata(&metadata_path, tcp_enabled)?;
            let storage = Arc::new(Storage::open(state.repository_id.as_ref()).await?);
            let service = storage_service(state, shutdown, storage.clone());
            let (sender, receiver) = mpsc::channel(64);
            tokio::spawn(accept_allowed_tcp(listener, allowlist, sender));
            let result = Server::builder()
                .concurrency_limit_per_connection(MAX_CONCURRENT_REQUESTS)
                .add_service(service)
                .serve_with_incoming_shutdown(
                    ReceiverStream::new(receiver),
                    shutdown_signal(shutdown_rx),
                )
                .await;
            let close_result = storage.close().await;
            remove_runtime_metadata(&metadata_path);
            result?;
            close_result?;
        }
    }
    Ok(())
}

fn storage_service(
    state: DaemonState,
    shutdown: watch::Sender<bool>,
    storage: Arc<Storage>,
) -> VaulticDbServer<Service> {
    VaulticDbServer::new(Service {
        state,
        shutdown,
        storage,
    })
    .max_decoding_message_size(MAX_MESSAGE_BYTES as usize)
    .max_encoding_message_size(MAX_MESSAGE_BYTES as usize)
}

fn write_runtime_metadata(socket: &Path, tcp_enabled: bool) -> Result<()> {
    let pid_path = socket.with_extension("pid");
    let cap_path = socket.with_extension("cap");
    std::fs::write(pid_path, format!("{}\n", std::process::id()))?;
    std::fs::write(
        cap_path,
        format!(
            "protocol={PROTOCOL_VERSION}\nschema={SCHEMA_VERSION}\ntcp_enabled={tcp_enabled}\n"
        ),
    )?;
    Ok(())
}

fn remove_runtime_metadata(socket: &Path) {
    let _ = std::fs::remove_file(socket.with_extension("pid"));
    let _ = std::fs::remove_file(socket.with_extension("cap"));
}

async fn remove_stale_socket(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        bail!("refusing to replace non-socket endpoint {}", path.display());
    }
    match tokio::net::UnixStream::connect(path).await {
        Ok(_) => bail!("vaulticdb endpoint {} is already active", path.display()),
        Err(_) => {
            tokio::fs::remove_file(path).await?;
            Ok(())
        }
    }
}

async fn accept_allowed_tcp(
    listener: TcpListener,
    allowlist: Vec<IpNet>,
    sender: mpsc::Sender<Result<tokio::net::TcpStream, io::Error>>,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = sender.send(Err(error)).await;
                return;
            }
        };
        if allowlist.iter().any(|network| network.contains(&peer.ip()))
            && sender.send(Ok(stream)).await.is_err()
        {
            return;
        }
    }
}

async fn native_smoke() -> Result<()> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("vaulticdb-phase0-smoke", object_store.clone()).await?;

    let mut batch = WriteBatch::new();
    batch.put(b"phase0/key", b"phase0/value");
    let write = db.write(batch).await?;
    write.await_durable().await?;
    db.close().await?;

    let reader_options = DbReaderOptions {
        skip_wal_replay: true,
        ..Default::default()
    };
    let reader = DbReader::open(
        "vaulticdb-phase0-smoke",
        object_store,
        DbReaderMode::FollowLatest,
        reader_options,
    )
    .await?;
    let value = reader.get(b"phase0/key").await?;
    if value.as_deref() != Some(b"phase0/value".as_slice()) {
        bail!("native SlateDB smoke read returned an unexpected value")
    }
    reader.close().await?;
    println!("vaulticdb native SlateDB smoke ok");
    Ok(())
}

async fn shutdown_signal(mut requested: watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = requested.changed() => {}
    }
}

fn acquire_singleton_lock(path: &Path) -> Result<File> {
    let lock = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .or_else(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                std::fs::OpenOptions::new().write(true).open(path)
            } else {
                Err(error)
            }
        })
        .with_context(|| format!("open vaulticdb singleton lock {}", path.display()))?;
    lock.try_lock_exclusive()
        .with_context(|| format!("acquire vaulticdb singleton lock {}", path.display()))?;
    Ok(lock)
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_socket_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o600);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_socket_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn transport_environment_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn unix_is_the_default_transport() {
        let _guard = transport_environment_lock().lock().unwrap();
        for key in [
            "VAULTICDB_TRANSPORT",
            "VAULTICDB_SOCKET",
            "VAULTICDB_TCP_ALLOWLIST",
            "VAULTICDB_TCP_AUTH_TOKEN",
        ] {
            unsafe { env::remove_var(key) };
        }
        assert!(
            matches!(parse_transport("").unwrap(), Transport::Unix(path) if path == PathBuf::from(default_socket_path("")).as_path())
        );
    }

    #[test]
    fn tcp_requires_authentication_and_allowlist() {
        let _guard = transport_environment_lock().lock().unwrap();
        unsafe { env::set_var("VAULTICDB_TRANSPORT", "tcp") };
        unsafe { env::remove_var("VAULTICDB_TCP_ALLOWLIST") };
        unsafe { env::remove_var("VAULTICDB_TCP_AUTH_TOKEN") };
        assert!(parse_transport("").is_err());
        unsafe { env::set_var("VAULTICDB_TCP_ALLOWLIST", "127.0.0.1/32,::1/128") };
        assert!(parse_transport("").is_err());
        unsafe { env::set_var("VAULTICDB_TCP_AUTH_TOKEN", "test-token") };
        assert!(
            matches!(parse_transport("").unwrap(), Transport::Tcp(_, networks) if networks.len() == 2)
        );
        for key in [
            "VAULTICDB_TRANSPORT",
            "VAULTICDB_TCP_ALLOWLIST",
            "VAULTICDB_TCP_AUTH_TOKEN",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    fn singleton_lock_recovers_after_previous_process_exit() {
        let directory = env::temp_dir().join(format!("vaulticdb-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("vaulticdb.lock");
        let first = acquire_singleton_lock(&path).unwrap();
        assert!(acquire_singleton_lock(&path).is_err());
        drop(first);
        assert!(acquire_singleton_lock(&path).is_ok());
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn future_storage_envelopes_enforce_advertised_limits() {
        let mut batch = WriteBatchRequest {
            deletes: vec![Vec::new(); MAX_BATCH_ITEMS as usize],
            ..Default::default()
        };
        assert!(validate_write_batch(&batch).is_ok());
        batch.deletes.push(Vec::new());
        assert_eq!(
            validate_write_batch(&batch).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );

        let oversized = WriteBatchRequest {
            deletes: vec![vec![0; MAX_MESSAGE_BYTES as usize]],
            ..Default::default()
        };
        assert_eq!(
            validate_write_batch(&oversized).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert!(validate_scan(&ScanRequest {
            page_size: MAX_PAGE_ITEMS,
            ..Default::default()
        })
        .is_ok());
        assert!(validate_scan(&ScanRequest::default()).is_err());
        assert!(validate_scan(&ScanRequest {
            page_size: MAX_PAGE_ITEMS + 1,
            ..Default::default()
        })
        .is_err());
    }
}
