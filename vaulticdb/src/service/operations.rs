fn verify_capsule_migration_proof(
    master_key: &[u8],
    repository_id: &str,
    capsule_sha256: &str,
    proof: &[u8],
) -> Result<(), Status> {
    let mut verifier = Hmac::<Sha256>::new_from_slice(master_key)
        .map_err(|_| Status::internal("initialize capsule migration proof verification"))?;
    verifier.update(b"vaultic-capsule-migration-finalize-v1\0");
    verifier.update(repository_id.as_bytes());
    verifier.update(b"\0");
    verifier.update(capsule_sha256.as_bytes());
    verifier
        .verify_slice(proof)
        .map_err(|_| Status::permission_denied("capsule-recovered repository key proof failed"))
}

fn validate_capsule_mutation(
    capsule_directory: &std::path::Path,
    repository_id: &str,
    encoded: &[u8],
    expected_digest: &str,
    identity_recovery: bool,
) -> Result<encryption::recovery_capsule::RecoveryCapsule> {
    if capsule_directory.as_os_str().is_empty() || expected_digest.len() != 64 {
        bail!("capsule directory and SHA-256 digest are required");
    }
    let capsule: encryption::recovery_capsule::RecoveryCapsule =
        serde_json::from_slice(encoded).context("decode recovery capsule")?;
    capsule.validate()?;
    let canonical = serde_json::to_vec(&capsule)?;
    if canonical != encoded {
        bail!("recovery capsule must use canonical JSON encoding");
    }
    if capsule.header.repository_id != repository_id {
        bail!("recovery capsule repository identity mismatch");
    }
    if format!("{:x}", Sha256::digest(&canonical)) != expected_digest {
        bail!("recovery capsule digest mismatch");
    }
    let (_, current) =
        encryption::recovery_capsule::discover_latest(capsule_directory, repository_id)?
            .context("current recovery capsule is missing")?;
    if capsule.header.generation == current.header.generation {
        if capsule != current {
            bail!("recovery capsule generation conflicts with the current capsule");
        }
        return Ok(capsule);
    }
    let expected_generation = current
        .header
        .generation
        .checked_add(1)
        .context("capsule generation overflow")?;
    if capsule.header.generation != expected_generation {
        bail!("capsule mutation generation is not sequential");
    }
    let identity_changed =
        capsule.header.broker_identity_public_key != current.header.broker_identity_public_key;
    if identity_changed != identity_recovery {
        bail!("broker identity pin change does not match identity-recovery declaration");
    }
    Ok(capsule)
}

async fn publish_capsule_without_database(arguments: &[String]) -> Result<()> {
    if arguments.len() != 5 {
        bail!("usage: vaulticdb publish-capsule CAPSULE_DIRECTORY CAPSULE_FILE SHA256 IDENTITY_RECOVERY");
    }
    let repository_id =
        env::var("VAULTICDB_REPOSITORY_ID").context("VAULTICDB_REPOSITORY_ID is required")?;
    let capsule_directory = PathBuf::from(&arguments[1]);
    let encoded = std::fs::read(&arguments[2])
        .with_context(|| format!("read recovery capsule {}", arguments[2]))?;
    let identity_recovery = arguments[4]
        .parse::<bool>()
        .context("IDENTITY_RECOVERY must be true or false")?;
    let capsule = validate_capsule_mutation(
        &capsule_directory,
        &repository_id,
        &encoded,
        &arguments[3],
        identity_recovery,
    )?;
    let (_, object_store) = storage::object_store(&repository_id)?;
    let mirror_path =
        encryption::recovery_capsule::publish_mirror(object_store.as_ref(), &capsule).await?;
    let local_path = encryption::recovery_capsule::publish_local(&capsule_directory, &capsule)?;
    println!(
        "{}",
        serde_json::json!({
            "generation": capsule.header.generation,
            "local_path": local_path,
            "mirror_path": mirror_path,
            "capsule_sha256": arguments[3],
        })
    );
    Ok(())
}

impl Service {
    async fn transition_to_reader(
        &self,
        timeout: Duration,
        reason: String,
        force: bool,
    ) -> Result<WriterStatusResponse, Status> {
        let _transition = self.state.writer_transition.lock().await;
        {
            let mut role = self.state.writer_role.lock().await;
            role.begin_demotion(Instant::now(), reason, force)
                .map_err(role_error)?;
        }
        let drain = async {
            while self.storage.active_transactions().await != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(timeout, drain).await.is_err() {
            self.state
                .writer_role
                .lock()
                .await
                .fail_demotion(Instant::now());
            return Err(Status::deadline_exceeded(
                "writer demotion quiescence timed out",
            ));
        }
        match self.storage.demote().await {
            Ok(()) => self
                .state
                .writer_role
                .lock()
                .await
                .complete_demotion(Instant::now())
                .map_err(role_error)?,
            Err(error) => {
                self.state
                    .writer_role
                    .lock()
                    .await
                    .fail_demotion(Instant::now());
                return Err(Status::failed_precondition(format!(
                    "writer demotion failed: {error:#}"
                )));
            }
        }
        Ok(self.writer_status_response().await)
    }

    async fn write_intent(&self) -> Result<WriteIntentGuard, Status> {
        if !self
            .storage
            .mutations_allowed(&self.state.repository_id)
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?
        {
            return Err(Status::failed_precondition(
                "metadata generation mutation interlock is active",
            ));
        }
        self.authority_intent().await
    }

    async fn authority_intent(&self) -> Result<WriteIntentGuard, Status> {
        self.storage
            .ensure_writer_fence()
            .await
            .map_err(VaulticDbError::generation)
            .map_err(Status::from)?;
        self.state
            .writer_role
            .lock()
            .await
            .admit_write()
            .map_err(role_error)?;
        Ok(WriteIntentGuard {
            writer_role: self.state.writer_role.clone(),
            last_writer_activity: self.state.last_writer_activity.clone(),
        })
    }

    async fn with_write_intent<T, F>(&self, operation: F) -> Result<T, Status>
    where
        F: Future<Output = Result<T, Status>>,
    {
        self.state
            .writer_role
            .lock()
            .await
            .admit_write()
            .map_err(role_error)?;
        let result = operation.await;
        self.state.writer_role.lock().await.finish_write();
        *self.state.last_writer_activity.lock().await = Instant::now();
        result
    }

    async fn writer_status_response(&self) -> WriterStatusResponse {
        let status = self.state.writer_role.lock().await.status();
        let transition_unix_ms = self.state.clock_started_unix_ms.saturating_add(
            status
                .transition_started
                .saturating_duration_since(self.state.clock_started)
                .as_millis()
                .min(i64::MAX as u128) as i64,
        );
        let idle_deadline_unix_ms = match self.state.writer_idle_grace {
            Some(grace) => {
                let activity = *self.state.last_writer_activity.lock().await;
                self.state.clock_started_unix_ms.saturating_add(
                    activity
                        .saturating_duration_since(self.state.clock_started)
                        .saturating_add(grace)
                        .as_millis()
                        .min(i64::MAX as u128) as i64,
                )
            }
            None => 0,
        };
        WriterStatusResponse {
            instance_id: self.state.daemon_id.to_string(),
            role: match status.role {
                CoreWriterRole::ReadOnly => proto::WriterRole::ReadOnly as i32,
                CoreWriterRole::Promoting => proto::WriterRole::Promoting as i32,
                CoreWriterRole::ReadWrite => proto::WriterRole::ReadWrite as i32,
                CoreWriterRole::Demoting => proto::WriterRole::Demoting as i32,
                CoreWriterRole::Fenced => proto::WriterRole::Fenced as i32,
            },
            current_epoch: status.current_epoch,
            observed_epoch: status.observed_epoch,
            transition_reason: status.transition_reason,
            transition_unix_ms,
            active_write_intents: status.active_write_intents,
            active_transactions: self.storage.active_transactions().await as u64,
            last_durable_sequence: self.storage.last_durable_sequence(),
            idle_deadline_unix_ms,
            promotion_safe: status.promotion_safe,
        }
    }

    fn check_key_request<T>(
        &self,
        request: &Request<T>,
        repository_id: &str,
    ) -> Result<(), Status> {
        check_request(&self.state, request, repository_id)?;
        if !self.state.unix_socket {
            return Err(Status::failed_precondition(
                "key management is available only over a private Unix socket",
            ));
        }
        Ok(())
    }

    async fn key_status_response(&self) -> Result<KeyStatusResponse, Status> {
        let (envelope_generation, active_dek_version, slots) =
            self.storage.key_manager()?.status().await;
        let (pending_capsule_migration_sha256, finalized_capsule_migration_sha256) =
            self.storage.capsule_migration_status().await?;
        Ok(KeyStatusResponse {
            envelope_generation,
            active_dek_version,
            slots: slots
                .into_iter()
                .map(|slot| KeySlotInfo {
                    id: slot.id,
                    provider: slot.provider,
                    priority: slot.priority,
                    recovery: slot.recovery,
                    key_reference: slot.key_reference,
                    dek_version: slot.dek_version,
                })
                .collect(),
            pending_capsule_migration_sha256: pending_capsule_migration_sha256.unwrap_or_default(),
            finalized_capsule_migration_sha256: finalized_capsule_migration_sha256
                .unwrap_or_default(),
        })
    }
}

fn key_management_error(error: anyhow::Error) -> Status {
    VaulticDbError::key_management(error).into()
}

fn role_error(error: RoleError) -> Status {
    VaulticDbError::from(error).into()
}

fn unix_time_ms_i64() -> Result<i64, Status> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system time is before Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| Status::internal("system time exceeds signed milliseconds"))
}

fn configured_duration(
    name: &str,
    default: Duration,
    allow_disabled: bool,
) -> Result<Option<Duration>> {
    let Ok(value) = env::var(name) else {
        return Ok((!allow_disabled || !default.is_zero()).then_some(default));
    };
    let value = value.trim();
    if allow_disabled && (value.is_empty() || value == "0" || value.eq_ignore_ascii_case("off")) {
        return Ok(None);
    }
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        bail!("{name} must use an ms, s, m, or h suffix")
    };
    let milliseconds = number
        .parse::<u64>()
        .with_context(|| format!("parse {name}"))?
        .checked_mul(multiplier)
        .with_context(|| format!("{name} is too large"))?;
    if milliseconds == 0 {
        bail!("{name} must be positive or explicitly disabled")
    }
    Ok(Some(Duration::from_millis(milliseconds)))
}

fn cloud_token(value: Vec<u8>) -> Result<Option<String>, Status> {
    let value = Zeroizing::new(value);
    if value.is_empty() {
        return Ok(None);
    }
    String::from_utf8(value.to_vec())
        .map(Some)
        .map_err(|_| Status::invalid_argument("cloud bearer token is not valid UTF-8"))
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
        let expected = format!("Bearer {}", token.as_str());
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
