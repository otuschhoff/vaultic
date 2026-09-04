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

pub(crate) async fn publish_capsule_without_database(arguments: &[String]) -> Result<()> {
    if arguments.len() != 5 {
        bail!("usage: vaulticdb publish-capsule CAPSULE_DIRECTORY CAPSULE_FILE SHA256 IDENTITY_RECOVERY");
    }
    let config = Config::from_env()?;
    let repository_id = config.repository_id;
    if repository_id.is_empty() {
        bail!("VAULTICDB_REPOSITORY_ID is required");
    }
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
    let (_, object_store) = storage::object_store(&repository_id, &config.storage.object_store)?;
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
    async fn handle_health(
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

    async fn handle_capabilities(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        check_request(
            &self.state,
            &request,
            request.get_ref().repository_id.as_str(),
        )?;
        check_context(request.get_ref().context.as_ref())?;
        let encryption = self.storage.encryption_status();
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
            encryption_enabled: encryption.enabled,
            encryption_algorithm: encryption.algorithm.to_owned(),
            active_dek_version: encryption.active_dek_version,
            envelope_generation: encryption.envelope_generation,
            unlock_slot: encryption.unlock_slot.clone().unwrap_or_default(),
            recovery_unlock: encryption.recovery_unlock,
            writer_roles: true,
            durable_idempotency: true,
        }))
    }

    async fn handle_drain(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        check_request(&self.state, &request, "")?;
        check_context(request.get_ref().context.as_ref())?;
        self.state.draining.store(true, Ordering::Release);
        Ok(Response::new(Empty { context: None }))
    }

    async fn handle_shutdown(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        check_request(&self.state, &request, "")?;
        check_context(request.get_ref().context.as_ref())?;
        self.state.draining.store(true, Ordering::Release);
        let _ = self.shutdown.send(true);
        Ok(Response::new(Empty { context: None }))
    }

    pub(crate) async fn transition_to_reader(
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
