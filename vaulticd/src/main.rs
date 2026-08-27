use std::{
    env,
    fs::File,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use slatedb::config::DbReaderOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbReader, DbReaderMode, WriteBatch};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::watch,
};
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::{transport::Server, Request, Response, Status};

pub mod proto {
    tonic::include_proto!("vaulticd.v1");
}

use proto::{
    vaultic_daemon_server::{VaulticDaemon, VaulticDaemonServer},
    CapabilitiesRequest, CapabilitiesResponse, Empty, HealthRequest, HealthResponse,
};

const PROTOCOL_VERSION: &str = "vaulticd.v1";
const SCHEMA_VERSION: &str = "0";
const MAX_BATCH_ITEMS: u32 = 10_000;
const MAX_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;

#[derive(Clone)]
struct DaemonState {
    daemon_id: Arc<str>,
    repository_id: Arc<str>,
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
        check_repository(&self.state, request.get_ref().repository_id.as_str())?;
        Ok(Response::new(HealthResponse {
            daemon_id: self.state.daemon_id.to_string(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            schema_version: SCHEMA_VERSION.to_owned(),
            repository_id: self.state.repository_id.to_string(),
            slate_db_revision: String::new(),
            ready: true,
        }))
    }

    async fn capabilities(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        check_repository(&self.state, request.get_ref().repository_id.as_str())?;
        Ok(Response::new(CapabilitiesResponse {
            daemon_id: self.state.daemon_id.to_string(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            schema_version: SCHEMA_VERSION.to_owned(),
            repository_id: self.state.repository_id.to_string(),
            unix_socket: true,
            tcp_enabled: false,
            max_batch_items: MAX_BATCH_ITEMS,
            max_message_bytes: MAX_MESSAGE_BYTES,
        }))
    }

    async fn drain(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn shutdown(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        let _ = self.shutdown.send(true);
        Ok(Response::new(Empty {}))
    }
}

fn check_repository(state: &DaemonState, requested: &str) -> Result<(), Status> {
    if requested.is_empty() || requested == state.repository_id.as_ref() {
        return Ok(());
    }
    Err(Status::failed_precondition("repository identity mismatch"))
}

#[derive(Debug)]
enum Transport {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

fn parse_transport() -> Result<Transport> {
    let transport = env::var("VAULTICD_TRANSPORT").unwrap_or_else(|_| "unix".to_owned());
    match transport.as_str() {
        "unix" => Ok(Transport::Unix(PathBuf::from(
            env::var("VAULTICD_SOCKET")
                .unwrap_or_else(|_| "/tmp/vaulticd/vaulticd.sock".to_owned()),
        ))),
        "tcp" => {
            if env::var("VAULTICD_TCP_ALLOWLIST")
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                bail!("VAULTICD_TCP_ALLOWLIST is required when TCP transport is enabled")
            }
            let addr =
                env::var("VAULTICD_TCP_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_owned());
            Ok(Transport::Tcp(
                addr.parse().context("invalid VAULTICD_TCP_ADDR")?,
            ))
        }
        other => bail!("unsupported VAULTICD_TRANSPORT {other:?}; expected unix or tcp"),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if env::var_os("VAULTICD_NATIVE_SMOKE").is_some() {
        return native_smoke().await;
    }

    let transport = parse_transport()?;
    let repository_id = env::var("VAULTICD_REPOSITORY_ID").unwrap_or_default();
    let state = DaemonState {
        daemon_id: Arc::from(
            env::var("VAULTICD_DAEMON_ID").unwrap_or_else(|_| "vaulticd-dev".to_owned()),
        ),
        repository_id: Arc::from(repository_id),
    };
    let (shutdown, shutdown_rx) = watch::channel(false);
    let service = VaulticDaemonServer::new(Service { state, shutdown });

    match transport {
        Transport::Unix(path) => {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
                set_private_directory_permissions(parent)?;
            }
            let lock_path = path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            let _ = tokio::fs::remove_file(&path).await;
            let listener = UnixListener::bind(&path)
                .with_context(|| format!("bind Unix socket {}", path.display()))?;
            set_private_socket_permissions(&path)?;
            let stream = UnixListenerStream::new(listener);
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(stream, shutdown_signal(shutdown_rx))
                .await?;
            drop(_lock);
            let _ = tokio::fs::remove_file(&lock_path).await;
            let _ = tokio::fs::remove_file(&path).await;
        }
        Transport::Tcp(addr) => {
            let listener = TcpListener::bind(addr).await.context("bind TCP listener")?;
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown_signal(shutdown_rx),
                )
                .await?;
        }
    }
    Ok(())
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
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("acquire vaulticd singleton lock {}", path.display()))
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
