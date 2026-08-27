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
    tonic::include_proto!("vaulticd.v1");
}

use proto::{
    vaultic_daemon_server::{VaulticDaemon, VaulticDaemonServer},
    CapabilitiesRequest, CapabilitiesResponse, Empty, HealthRequest, HealthResponse,
    RequestContext,
};

const PROTOCOL_VERSION: &str = "vaulticd.v1";
const SCHEMA_VERSION: &str = "0";
const MAX_BATCH_ITEMS: u32 = 10_000;
const MAX_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;

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
}

#[tonic::async_trait]
impl VaulticDaemon for Service {
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
            return Err(Status::unauthenticated("invalid vaulticd authorization"));
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
    let transport = env::var("VAULTICD_TRANSPORT").unwrap_or_else(|_| "unix".to_owned());
    match transport.as_str() {
        "unix" => Ok(Transport::Unix(PathBuf::from(
            env::var("VAULTICD_SOCKET").unwrap_or_else(|_| default_socket_path(repository_id)),
        ))),
        "tcp" => {
            if env::var("VAULTICD_TCP_ALLOWLIST")
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                bail!("VAULTICD_TCP_ALLOWLIST is required when TCP transport is enabled")
            }
            if env::var("VAULTICD_TCP_AUTH_TOKEN")
                .unwrap_or_default()
                .is_empty()
            {
                bail!("VAULTICD_TCP_AUTH_TOKEN is required when TCP transport is enabled")
            }
            let addr =
                env::var("VAULTICD_TCP_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_owned());
            let allowlist = env::var("VAULTICD_TCP_ALLOWLIST")?
                .split(',')
                .map(|value| value.trim().parse().context("invalid IP allowlist entry"))
                .collect::<Result<Vec<IpNet>>>()?;
            Ok(Transport::Tcp(
                addr.parse().context("invalid VAULTICD_TCP_ADDR")?,
                allowlist,
            ))
        }
        other => bail!("unsupported VAULTICD_TRANSPORT {other:?}; expected unix or tcp"),
    }
}

fn default_socket_path(repository_id: &str) -> String {
    let runtime_dir =
        env::var("VAULTICD_RUNTIME_DIR").unwrap_or_else(|_| "/tmp/vaulticd".to_owned());
    let digest = Sha256::digest(if repository_id.is_empty() {
        b"default"
    } else {
        repository_id.as_bytes()
    });
    format!("{runtime_dir}/{digest:x}.sock")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if env::var_os("VAULTICD_NATIVE_SMOKE").is_some() {
        return native_smoke().await;
    }

    let repository_id = env::var("VAULTICD_REPOSITORY_ID").unwrap_or_default();
    let transport = parse_transport(&repository_id)?;
    let auth_token = env::var("VAULTICD_TCP_AUTH_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let state = DaemonState {
        daemon_id: Arc::from(
            env::var("VAULTICD_DAEMON_ID").unwrap_or_else(|_| "vaulticd-dev".to_owned()),
        ),
        repository_id: Arc::from(repository_id),
        auth_token: auth_token.map(Arc::from),
        unix_socket: matches!(&transport, Transport::Unix(_)),
        tcp_enabled: matches!(&transport, Transport::Tcp(_, _)),
        draining: Arc::new(AtomicBool::new(false)),
    };
    let tcp_enabled = matches!(transport, Transport::Tcp(_, _));
    let (shutdown, shutdown_rx) = watch::channel(false);
    let service = VaulticDaemonServer::new(Service { state, shutdown })
        .max_decoding_message_size(MAX_MESSAGE_BYTES as usize)
        .max_encoding_message_size(MAX_MESSAGE_BYTES as usize);

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
            let stream = UnixListenerStream::new(listener);
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(stream, shutdown_signal(shutdown_rx))
                .await?;
            drop(_lock);
            let _ = tokio::fs::remove_file(&path).await;
            remove_runtime_metadata(&path);
        }
        Transport::Tcp(addr, allowlist) => {
            let metadata_path = PathBuf::from(
                env::var("VAULTICD_RUNTIME_DIR").unwrap_or_else(|_| "/tmp/vaulticd".to_owned()),
            )
            .join("vaulticd-tcp");
            let listener = TcpListener::bind(addr).await.context("bind TCP listener")?;
            if let Some(parent) = metadata_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
                set_private_directory_permissions(parent)?;
            }
            write_runtime_metadata(&metadata_path, tcp_enabled)?;
            let (sender, receiver) = mpsc::channel(64);
            tokio::spawn(accept_allowed_tcp(listener, allowlist, sender));
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    ReceiverStream::new(receiver),
                    shutdown_signal(shutdown_rx),
                )
                .await?;
            remove_runtime_metadata(&metadata_path);
        }
    }
    Ok(())
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
        Ok(_) => bail!("vaulticd endpoint {} is already active", path.display()),
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
    let db = Db::open("vaulticd-phase0-smoke", object_store.clone()).await?;

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
        "vaulticd-phase0-smoke",
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
    println!("vaulticd native SlateDB smoke ok");
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
        .with_context(|| format!("open vaulticd singleton lock {}", path.display()))?;
    lock.try_lock_exclusive()
        .with_context(|| format!("acquire vaulticd singleton lock {}", path.display()))?;
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
            "VAULTICD_TRANSPORT",
            "VAULTICD_SOCKET",
            "VAULTICD_TCP_ALLOWLIST",
            "VAULTICD_TCP_AUTH_TOKEN",
        ] {
            unsafe { env::remove_var(key) };
        }
        assert!(
            matches!(parse_transport("").unwrap(), Transport::Unix(path) if path == PathBuf::from(default_socket_path("")))
        );
    }

    #[test]
    fn tcp_requires_authentication_and_allowlist() {
        let _guard = transport_environment_lock().lock().unwrap();
        unsafe { env::set_var("VAULTICD_TRANSPORT", "tcp") };
        unsafe { env::remove_var("VAULTICD_TCP_ALLOWLIST") };
        unsafe { env::remove_var("VAULTICD_TCP_AUTH_TOKEN") };
        assert!(parse_transport("").is_err());
        unsafe { env::set_var("VAULTICD_TCP_ALLOWLIST", "127.0.0.1/32,::1/128") };
        assert!(parse_transport("").is_err());
        unsafe { env::set_var("VAULTICD_TCP_AUTH_TOKEN", "test-token") };
        assert!(
            matches!(parse_transport("").unwrap(), Transport::Tcp(_, networks) if networks.len() == 2)
        );
        for key in [
            "VAULTICD_TRANSPORT",
            "VAULTICD_TCP_ALLOWLIST",
            "VAULTICD_TCP_AUTH_TOKEN",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    fn singleton_lock_recovers_after_previous_process_exit() {
        let directory = env::temp_dir().join(format!("vaulticd-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("vaulticd.lock");
        let first = acquire_singleton_lock(&path).unwrap();
        assert!(acquire_singleton_lock(&path).is_err());
        drop(first);
        assert!(acquire_singleton_lock(&path).is_ok());
        let _ = std::fs::remove_dir_all(directory);
    }
}
