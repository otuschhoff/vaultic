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
    disable_core_dumps();
    let arguments = env::args().skip(1).collect::<Vec<_>>();
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
    let mut state = DaemonState {
        daemon_id: Arc::from(daemon_id),
        repository_id: Arc::from(repository_id),
        auth_token: auth_token.map(Arc::new),
        unix_socket: matches!(&transport, TransportConfig::Unix(_)),
        tcp_enabled: matches!(&transport, TransportConfig::Tcp { .. }),
        draining: Arc::new(AtomicBool::new(false)),
        writer_role: Arc::new(Mutex::new(WriterRoleState::read_write(
            1,
            clock_started,
            minimum_writer_tenure,
        ))),
        writer_transition: Arc::new(Mutex::new(())),
        last_writer_activity: Arc::new(Mutex::new(clock_started)),
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
            let storage = Arc::new(
                Storage::open(state.repository_id.as_ref(), &storage_config).await?,
            );
            let (is_writer, epoch) = storage.writer_status_epoch().await;
            state.writer_role = Arc::new(Mutex::new(if is_writer {
                WriterRoleState::read_write(epoch, clock_started, minimum_writer_tenure)
            } else {
                WriterRoleState::read_only(epoch, clock_started, minimum_writer_tenure)
            }));
            monitor_broker_lease(storage.as_ref(), shutdown.clone());
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
        TransportConfig::Tcp { address, allowlist, metadata_path } => {
            let listener = TcpListener::bind(address).await.context("bind TCP listener")?;
            if let Some(parent) = metadata_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
                set_private_directory_permissions(parent)?;
            }
            let lock_path = metadata_path.with_extension("lock");
            let _lock = acquire_singleton_lock(&lock_path)?;
            write_runtime_metadata(&metadata_path, tcp_enabled)?;
            let storage = Arc::new(
                Storage::open(state.repository_id.as_ref(), &storage_config).await?,
            );
            let (is_writer, epoch) = storage.writer_status_epoch().await;
            state.writer_role = Arc::new(Mutex::new(if is_writer {
                WriterRoleState::read_write(epoch, clock_started, minimum_writer_tenure)
            } else {
                WriterRoleState::read_only(epoch, clock_started, minimum_writer_tenure)
            }));
            monitor_broker_lease(storage.as_ref(), shutdown.clone());
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

fn monitor_broker_lease(storage: &Storage, shutdown: watch::Sender<bool>) {
    let Some((mut disconnected, expires_unix_ms)) = storage.broker_lease_monitor() else {
        return;
    };
    tokio::spawn(async move {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(expires_unix_ms);
        let until_expiry = std::time::Duration::from_millis(expires_unix_ms.saturating_sub(now));
        tokio::select! {
            _ = disconnected.changed() => {}
            _ = tokio::time::sleep(until_expiry) => {}
        }
        let _ = shutdown.send(true);
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
    storage: Arc<Storage>,
) -> VaulticDbServer<Service> {
    let service = Service {
        state,
        shutdown,
        storage,
    };
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
    VaulticDbServer::new(service)
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
