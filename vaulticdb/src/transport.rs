fn repository_key(repository_id: &str) -> String {
    let digest = Sha256::digest(if repository_id.is_empty() {
        b"default"
    } else {
        repository_id.as_bytes()
    });
    format!("{digest:x}")
}

fn injected_transport_failure(name: &str) -> Option<anyhow::Error> {
    #[cfg(feature = "test-failpoints")]
    if std::env::var("VAULTICDB_TEST_CAPABILITY").as_deref()
        == Ok("vaulticdb-process-tests-v1")
        && std::env::var("VAULTICDB_TEST_TRANSPORT_FAILURE").as_deref() == Ok(name)
    {
        return Some(anyhow::anyhow!("injected transport {name} failure"));
    }
    let _ = name;
    None
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    disable_core_dumps();
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.as_slice() == ["--version"] {
        vaulticdb::build_info::print_version("vaulticdb");
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "publish-capsule")
    {
        return publish_capsule_without_database(&arguments).await;
    }
    if Config::native_smoke_requested() {
        return native_smoke().await;
    }

    let Config {
        repository_id,
        daemon_id,
        auth_token,
        transport,
        minimum_writer_tenure,
        writer_idle_grace,
        writer_transition_timeout,
        storage: storage_config,
    } = Config::from_env()?;
    let clock_started = Instant::now();
    let state = DaemonState {
        daemon_id: Arc::from(daemon_id),
        repository_id: repository_id.clone(),
        auth_token: auth_token.map(Arc::new),
        unix_socket: matches!(&transport, TransportConfig::Unix(_)),
        tcp_enabled: matches!(&transport, TransportConfig::Tcp { .. }),
        draining: Arc::new(AtomicBool::new(false)),
        lifecycle: Arc::new(Mutex::new(DaemonLifecycle::loading(
            unix_time_ms_i64()?,
        ))),
        writer_role: Arc::new(Mutex::new(WriterRoleState::read_write(
            1,
            clock_started,
            minimum_writer_tenure,
        ))),
        writer_transition: Arc::new(Mutex::new(())),
        mutation_admission: Arc::new(RwLock::new(())),
        last_writer_activity: Arc::new(Mutex::new(clock_started)),
        minimum_writer_tenure,
        writer_idle_grace,
        writer_transition_timeout,
        clock_started,
        clock_started_unix_ms: unix_time_ms_i64()?,
    };
    let tcp_enabled = matches!(transport, TransportConfig::Tcp { .. });
    let (shutdown, shutdown_rx) = watch::channel(false);

    match transport {
        TransportConfig::Unix(path) => {
            if let Some(parent) = path.parent() {
                prepare_private_runtime_directory(parent)?;
            }
            let lock_path = path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            remove_stale_socket(&path).await?;
            write_runtime_metadata(&path, false).await?;
            let listener = match UnixListener::bind(&path) {
                Ok(listener) => listener,
                Err(error) => {
                    return combine_results(vec![
                        (
                            "bind Unix socket",
                            Err(error)
                                .with_context(|| format!("bind Unix socket {}", path.display())),
                        ),
                        ("cleanup runtime metadata", remove_runtime_metadata(&path)),
                    ]);
                }
            };
            if let Err(error) = set_private_socket_permissions(&path) {
                let socket_cleanup = remove_file_if_exists(&path).await;
                let metadata_cleanup = remove_runtime_metadata(&path);
                return combine_results(vec![
                    ("set private socket permissions", Err(error)),
                    ("cleanup Unix socket", socket_cleanup),
                    ("cleanup runtime metadata", metadata_cleanup),
                ]);
            }
            let mut artifacts = RuntimeArtifacts::unix(path.clone());
            let (service, runtime) = storage_service(state.clone(), shutdown.clone());
            let loader = spawn_storage_loader(runtime.clone(), storage_config, shutdown.clone());
            let stream = UnixListenerStream::new(listener);
            let result = Server::builder()
                .concurrency_limit_per_connection(MAX_CONCURRENT_REQUESTS)
                .add_service(service)
                .serve_with_incoming_shutdown(stream, shutdown_signal(shutdown_rx))
                .await;
            let result = injected_transport_failure("server")
                .map_or_else(|| result.map_err(Into::into), Err);
            let loaded = loader
                .await
                .context("receive SlateDB loading result")
                .and_then(|result| result);
            let transition_result = match injected_transport_failure("stopping-transition") {
                Some(error) => Err(error),
                None => runtime
                    .transition_lifecycle(DaemonPhase::Stopping, "gRPC server stopped")
                    .await
                    .map_err(|status| anyhow::anyhow!(status.message().to_owned())),
            };
            let close_result = match loaded {
                Ok(storage) => storage.close().await,
                Err(error) => Err(error),
            };
            let close_result = injected_transport_failure("storage-close")
                .map_or(close_result, Err);
            let artifact_cleanup = artifacts.cleanup();
            drop(_lock);
            combine_results(vec![
                ("serve gRPC", result),
                ("transition lifecycle", transition_result),
                ("close storage", close_result),
                ("cleanup runtime artifacts", artifact_cleanup),
            ])?;
        }
        TransportConfig::Tcp { address, allowlist, metadata_path } => {
            let listener = TcpListener::bind(address).await.context("bind TCP listener")?;
            if let Some(parent) = metadata_path.parent() {
                prepare_private_runtime_directory(parent)?;
            }
            let lock_path = metadata_path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            write_runtime_metadata(&metadata_path, tcp_enabled).await?;
            let mut artifacts = RuntimeArtifacts::tcp(metadata_path.clone());
            let (service, runtime) = storage_service(state, shutdown.clone());
            let loader = spawn_storage_loader(runtime.clone(), storage_config, shutdown.clone());
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
            let result = injected_transport_failure("server")
                .map_or_else(|| result.map_err(Into::into), Err);
            let loaded = loader
                .await
                .context("receive SlateDB loading result")
                .and_then(|result| result);
            let transition_result = match injected_transport_failure("stopping-transition") {
                Some(error) => Err(error),
                None => runtime
                    .transition_lifecycle(DaemonPhase::Stopping, "gRPC server stopped")
                    .await
                    .map_err(|status| anyhow::anyhow!(status.message().to_owned())),
            };
            let close_result = match loaded {
                Ok(storage) => storage.close().await,
                Err(error) => Err(error),
            };
            let close_result = injected_transport_failure("storage-close")
                .map_or(close_result, Err);
            let artifact_cleanup = artifacts.cleanup();
            combine_results(vec![
                ("serve gRPC", result),
                ("transition lifecycle", transition_result),
                ("close storage", close_result),
                ("cleanup runtime artifacts", artifact_cleanup),
            ])?;
        }
    }
    Ok(())
}

struct RuntimeArtifacts {
    socket_path: Option<PathBuf>,
    metadata_path: PathBuf,
    armed: bool,
}

impl RuntimeArtifacts {
    fn unix(path: PathBuf) -> Self {
        Self {
            socket_path: Some(path.clone()),
            metadata_path: path,
            armed: true,
        }
    }

    fn tcp(metadata_path: PathBuf) -> Self {
        Self {
            socket_path: None,
            metadata_path,
            armed: true,
        }
    }

    fn cleanup(&mut self) -> Result<()> {
        self.armed = false;
        let mut results = Vec::new();
        if let Some(path) = &self.socket_path {
            results.push(("remove Unix socket", remove_file_if_exists_sync(path)));
        }
        results.push((
            "remove runtime metadata",
            remove_runtime_metadata(&self.metadata_path),
        ));
        combine_results(results)
    }
}

impl Drop for RuntimeArtifacts {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cleanup();
        }
    }
}

fn combine_results(results: Vec<(&str, Result<()>)>) -> Result<()> {
    let errors = results
        .into_iter()
        .filter_map(|(operation, result)| {
            result.err().map(|error| OperationFailure {
                operation: operation.to_owned(),
                error,
            })
        })
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow::Error::new(CombinedOperationErrors(errors)))
    }
}

#[derive(Debug)]
struct OperationFailure {
    operation: String,
    error: anyhow::Error,
}

struct CombinedOperationErrors(Vec<OperationFailure>);

impl std::fmt::Debug for CombinedOperationErrors {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

impl std::fmt::Display for CombinedOperationErrors {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            &self
                .0
                .iter()
                .map(|failure| format!("{}: {:#}", failure.operation, failure.error))
                .collect::<Vec<_>>()
                .join("; "),
        )
    }
}

impl std::error::Error for CombinedOperationErrors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|failure| failure.error.as_ref())
    }
}

fn spawn_storage_loader(
    service: Service,
    storage_config: crate::storage::StorageConfig,
    shutdown: watch::Sender<bool>,
) -> oneshot::Receiver<Result<Arc<Storage>>> {
    let (result_sender, result_receiver) = oneshot::channel();
    tokio::spawn(async move {
        let result = tokio::spawn(load_storage(service, storage_config))
            .await
            .context("join SlateDB loading task")
            .and_then(|result| result);
        if result.is_err() {
            let _ = shutdown.send(true);
        }
        let _ = result_sender.send(result);
    });
    result_receiver
}

async fn load_storage(service: Service, storage_config: crate::storage::StorageConfig) -> Result<Arc<Storage>> {
    #[cfg(feature = "test-failpoints")]
    if std::env::var("VAULTICDB_TEST_CAPABILITY").as_deref()
        == Ok("vaulticdb-process-tests-v1")
        && std::env::var_os("VAULTICDB_TEST_PANIC_LOADER").is_some()
    {
        panic!("injected storage loader panic");
    }
    let started = Instant::now();
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"storage_load_started\"}}"
    );
    let storage = match Storage::open(service.state.repository_id.as_ref(), &storage_config).await {
        Ok(storage) => Arc::new(storage),
        Err(error) => {
            eprintln!(
                "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"storage_load_failed\",\"fields\":{{\"elapsed_ms\":{}}}}}",
                started.elapsed().as_millis()
            );
            service
                .transition_lifecycle(DaemonPhase::Failed, format!("open SlateDB: {error:#}"))
                .await
                .map_err(|status| anyhow::anyhow!(status.message().to_owned()))?;
            return Err(error).context("open SlateDB database");
        }
    };
    let (is_writer, mut epoch) = storage.writer_status_epoch().await;
    let reconciliation = async {
        if is_writer
            && storage
                .generation_authority(service.state.repository_id.as_ref())
                .await?
                .state
                == "rollback-observation"
        {
            epoch = storage
                .refresh_writer_fence()
                .await
                .context("reconcile writer fence after committed generation rollback")?;
        }
        anyhow::Ok(())
    }
    .await;
    if let Err(error) = reconciliation {
        let transition_result = service
            .transition_lifecycle(
                DaemonPhase::Failed,
                format!("reconcile generation writer fence: {error:#}"),
            )
            .await
            .map_err(|status| anyhow::anyhow!(status.message().to_owned()));
        let close_result = storage.close().await;
        return match combine_results(vec![
            ("reconcile generation writer fence", Err(error)),
            ("transition lifecycle", transition_result),
            ("close storage", close_result),
        ]) {
            Err(error) => Err(error),
            Ok(()) => unreachable!("reconciliation failure must be preserved"),
        };
    }
    *service.state.writer_role.lock().await = if is_writer {
        WriterRoleState::read_write(
            epoch,
            service.state.clock_started,
            service.state.minimum_writer_tenure,
        )
    } else {
        WriterRoleState::read_only(
            epoch,
            service.state.clock_started,
            service.state.minimum_writer_tenure,
        )
    };
    *service.storage.write().await = Some(storage.clone());
    let became_ready = service
        .state
        .lifecycle
        .lock()
        .await
        .finish_loading(
                if is_writer {
                    DaemonPhase::ReadWrite
                } else {
                    DaemonPhase::ReadOnly
                },
                if is_writer {
                    "SlateDB writer ready"
                } else {
                    "SlateDB reader ready"
                },
                unix_time_ms_i64().map_err(|status| anyhow::anyhow!(status.message().to_owned()))?,
            );
    let became_ready = match became_ready {
        Ok(became_ready) => became_ready,
        Err(error) => {
            *service.storage.write().await = None;
            return match storage.close().await {
                Ok(()) => Err(anyhow::anyhow!(error)).context("finish loading lifecycle"),
                Err(close_error) => Err(anyhow::anyhow!(
                    "{error}; additionally failed to close SlateDB: {close_error:#}"
                )),
            };
        }
    };
    monitor_broker_lease(storage.as_ref(), service.shutdown.clone());
    if became_ready {
        monitor_writer_idle(service.clone());
    }
    eprintln!(
        "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"storage_ready\",\"fields\":{{\"role\":\"{}\",\"epoch\":{},\"elapsed_ms\":{}}}}}",
        if is_writer { "writer" } else { "reader" },
        epoch,
        started.elapsed().as_millis()
    );
    Ok(storage)
}

fn monitor_broker_lease(storage: &Storage, shutdown: watch::Sender<bool>) {
    let Some(mut valid_until) = storage.broker_lease_monitor() else {
        return;
    };
    tokio::spawn(async move {
        loop {
            let expires_unix_ms = *valid_until.borrow();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(expires_unix_ms);
            let until_expiry =
                std::time::Duration::from_millis(expires_unix_ms.saturating_sub(now));
            tokio::select! {
                changed = valid_until.changed() => {
                    if changed.is_ok() {
                        continue;
                    }
                }
                _ = tokio::time::sleep(until_expiry) => {}
            }
            eprintln!(
                "{{\"category\":\"lifecycle\",\"component\":\"vaulticdb\",\"event\":\"credential_expired\"}}"
            );
            let _ = shutdown.send(true);
            return;
        }
    });
}

fn disable_core_dumps() {
    #[cfg(unix)]
    unsafe {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        libc::setrlimit(libc::RLIMIT_CORE, &limit);
    }
}

fn storage_service(
    state: DaemonState,
    shutdown: watch::Sender<bool>,
) -> (VaulticDbServer<Service>, Service) {
    let service = Service {
        state,
        shutdown,
        storage: Arc::new(RwLock::new(None)),
    };
    let server = VaulticDbServer::new(service.clone())
        .max_decoding_message_size(MAX_MESSAGE_BYTES as usize)
        .max_encoding_message_size(MAX_MESSAGE_BYTES as usize);
    (server, service)
}

fn monitor_writer_idle(service: Service) {
    if let Some(grace) = service.state.writer_idle_grace {
        let idle_service = service.clone();
        tokio::spawn(async move {
            let poll = grace
                .min(Duration::from_secs(1))
                .max(Duration::from_millis(100));
            let mut interval = tokio::time::interval(poll);
            loop {
                interval.tick().await;
                let status = idle_service.state.writer_role.lock().await.status();
                let last_activity = *idle_service.state.last_writer_activity.lock().await;
                if status.role == CoreWriterRole::ReadWrite
                    && status.active_write_intents == 0
                    && status.active_transactions == 0
                    && Instant::now().saturating_duration_since(last_activity) >= grace
                {
                    let _ = idle_service
                        .transition_to_reader(
                            idle_service.state.writer_transition_timeout,
                            "configured idle grace elapsed".to_owned(),
                            false,
                        )
                        .await;
                }
            }
        });
    }
}

async fn write_runtime_metadata(socket: &Path, tcp_enabled: bool) -> Result<()> {
    let pid_path = socket.with_extension("pid");
    let cap_path = socket.with_extension("cap");
    std::fs::write(&pid_path, format!("{}\n", std::process::id()))?;
    crate::service::process_test_barrier("VAULTICDB_TEST_RUNTIME_METADATA_BARRIER")
        .await
        .map_err(|status| anyhow::anyhow!(status.message().to_owned()))?;
    if let Err(error) = std::fs::write(
        &cap_path,
        format!(
            "protocol={PROTOCOL_VERSION}\nschema={SCHEMA_VERSION}\ntcp_enabled={tcp_enabled}\n"
        ),
    ) {
        remove_file_if_exists_sync(&pid_path)
            .with_context(|| format!("rollback PID metadata after capability write failed: {error}"))?;
        return Err(error).with_context(|| format!("write {}", cap_path.display()));
    }
    Ok(())
}

fn remove_runtime_metadata(socket: &Path) -> Result<()> {
    let pid_result = remove_file_if_exists_sync(&socket.with_extension("pid"));
    let cap_result = remove_file_if_exists_sync(&socket.with_extension("cap"));
    match (pid_result, cap_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(pid), Ok(())) => Err(pid),
        (Ok(()), Err(cap)) => Err(cap),
        (Err(pid), Err(cap)) => Err(anyhow::anyhow!("{pid:#}; {cap:#}")),
    }
}

fn remove_file_if_exists_sync(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

async fn remove_file_if_exists(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
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
fn prepare_private_runtime_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true).mode(0o700).create(path)?;
        }
        Err(error) => return Err(error.into()),
    }

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        bail!("vaulticdb runtime path {} is not a directory", path.display());
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!("vaulticdb runtime directory {} has an unsafe owner", path.display());
    }
    if metadata.permissions().mode() & 0o777 != 0o700 {
        bail!("vaulticdb runtime directory {} must have mode 0700", path.display());
    }
    Ok(())
}

#[cfg(not(unix))]
fn prepare_private_runtime_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(Into::into)
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

#[cfg(all(test, unix))]
mod runtime_directory_tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    #[test]
    fn creates_private_directory_and_rejects_unsafe_paths() {
        let root = std::env::temp_dir().join(format!(
            "vaulticdb-runtime-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let runtime = root.join("runtime");
        prepare_private_runtime_directory(&runtime).unwrap();
        assert_eq!(
            std::fs::metadata(&runtime).unwrap().permissions().mode() & 0o777,
            0o700
        );

        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(prepare_private_runtime_directory(&runtime).is_err());
        std::fs::remove_dir(&runtime).unwrap();

        let target = root.join("target");
        std::fs::create_dir(&target).unwrap();
        symlink(&target, &runtime).unwrap();
        assert!(prepare_private_runtime_directory(&runtime).is_err());

        std::fs::remove_file(&runtime).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn runtime_artifact_cleanup_removes_files_and_reports_wrong_types() {
        let root = std::env::temp_dir().join(format!(
            "vaulticdb-cleanup-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&root).unwrap();
        let endpoint = root.join("vaulticdb.sock");
        std::fs::write(endpoint.with_extension("pid"), b"pid").unwrap();
        std::fs::write(endpoint.with_extension("cap"), b"cap").unwrap();
        remove_runtime_metadata(&endpoint).unwrap();
        assert!(!endpoint.with_extension("pid").exists());
        assert!(!endpoint.with_extension("cap").exists());

        std::fs::create_dir(&endpoint).unwrap();
        assert!(remove_file_if_exists(&endpoint).await.is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn runtime_artifact_guards_cleanup_unix_and_tcp_metadata() {
        for unix in [true, false] {
            let root = std::path::PathBuf::from("/tmp").join(format!(
                "vaulticdb-artifact-guard-test-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir(&root).unwrap();
            let endpoint = root.join("vaulticdb.sock");
            std::fs::write(endpoint.with_extension("pid"), b"pid").unwrap();
            std::fs::write(endpoint.with_extension("cap"), b"cap").unwrap();
            if unix {
                let listener = std::os::unix::net::UnixListener::bind(&endpoint).unwrap();
                drop(listener);
                drop(RuntimeArtifacts::unix(endpoint.clone()));
                assert!(!endpoint.exists());
            } else {
                drop(RuntimeArtifacts::tcp(endpoint.clone()));
            }
            assert!(!endpoint.with_extension("pid").exists());
            assert!(!endpoint.with_extension("cap").exists());
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn result_aggregation_preserves_primary_and_cleanup_failures() {
        let error = combine_results(vec![
            ("serve gRPC", Err(anyhow::anyhow!("primary failure"))),
            ("cleanup runtime artifacts", Err(anyhow::anyhow!("cleanup failure"))),
        ])
        .unwrap_err();
        let combined = error.downcast_ref::<CombinedOperationErrors>().unwrap();
        assert_eq!(combined.0.len(), 2);
        assert_eq!(combined.0[0].operation, "serve gRPC");
        assert_eq!(combined.0[0].error.to_string(), "primary failure");
        assert_eq!(combined.0[1].operation, "cleanup runtime artifacts");
        assert_eq!(combined.0[1].error.to_string(), "cleanup failure");
    }
}
