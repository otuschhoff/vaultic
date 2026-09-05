fn idempotency_record_key(value: &str) -> Result<Option<Vec<u8>>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_IDEMPOTENCY_KEY_BYTES || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Status::invalid_argument("invalid idempotency key"));
    }
    let mut key = IDEMPOTENCY_PREFIX.to_vec();
    key.extend_from_slice(value.as_bytes());
    Ok(Some(key))
}

fn write_batch_digest(request: &WriteBatchRequest) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vaulticdb-write-batch-v1\0");
    for put in &request.puts {
        digest.update((put.key.len() as u64).to_be_bytes());
        digest.update(&put.key);
        digest.update((put.value.len() as u64).to_be_bytes());
        digest.update(&put.value);
    }
    digest.update([0xff]);
    for delete in &request.deletes {
        digest.update((delete.len() as u64).to_be_bytes());
        digest.update(delete);
    }
    format!("{:x}", digest.finalize())
}

fn transaction_digest(transaction_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vaulticdb-transaction-commit-v1\0");
    digest.update(transaction_id.as_bytes());
    format!("{:x}", digest.finalize())
}

async fn metadata_store_has_database_objects(store: &dyn ObjectStore) -> Result<bool> {
    let mut objects = store.list(None);
    while let Some(object) = objects.next().await {
        let object = object.context("inspect candidate metadata store")?;
        if !object.location.as_ref().starts_with("_vaultic/") {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn latest_writer_epoch(store: &dyn ObjectStore) -> Result<u64> {
    let prefix = ObjectPath::from(WRITER_EPOCH_PREFIX);
    let mut objects = store.list(Some(&prefix));
    let mut latest = 0u64;
    while let Some(object) = objects.next().await {
        let object = object.context("list writer epoch coordination objects")?;
        let Some(name) = object.location.as_ref().rsplit('/').next() else {
            continue;
        };
        if let Ok(epoch) = name.parse::<u64>() {
            latest = latest.max(epoch);
        }
    }
    Ok(latest)
}

async fn claim_writer_epoch(
    store: &dyn ObjectStore,
    takeover_epoch: Option<u64>,
) -> Result<Option<u64>> {
    let epoch = latest_writer_epoch(store)
        .await?
        .checked_add(1)
        .context("writer epoch overflow")?;
    let active_path = ObjectPath::from(ACTIVE_WRITER_PATH);
    let mode = if let Some(expected_epoch) = takeover_epoch {
        let current = store
            .get(&active_path)
            .await
            .context("read active writer claim for takeover")?;
        let version = UpdateVersion {
            e_tag: current.meta.e_tag.clone(),
            version: current.meta.version.clone(),
        };
        let bytes = current
            .bytes()
            .await
            .context("read active writer takeover claim")?;
        let observed: u64 = std::str::from_utf8(&bytes)
            .context("decode active writer takeover claim")?
            .parse()
            .context("parse active writer takeover epoch")?;
        if observed != expected_epoch || observed != latest_writer_epoch(store).await? {
            bail!("active writer changed since takeover was authorized")
        }
        PutMode::Update(version)
    } else {
        PutMode::Create
    };
    match store
        .put_opts(
            &active_path,
            epoch.to_string().into_bytes().into(),
            PutOptions::from(mode),
        )
        .await
    {
        Ok(_) => {}
        Err(
            slatedb::object_store::Error::AlreadyExists { .. }
            | slatedb::object_store::Error::Precondition { .. },
        ) => return Ok(None),
        Err(error) => return Err(error).context("claim active writer ownership"),
    }
    let path = ObjectPath::from(format!("{WRITER_EPOCH_PREFIX}/{epoch:020}"));
    let value = format!("pid={} time_ms={}\n", std::process::id(), unix_time_ms()?).into_bytes();
    if let Err(error) = store
        .put_opts(&path, value.into(), PutOptions::from(PutMode::Create))
        .await
    {
        let _ = store.delete(&active_path).await;
        return Err(error).context("publish writer epoch history");
    }
    Ok(Some(epoch))
}

async fn active_writer_epoch(store: &dyn ObjectStore) -> Result<Option<u64>> {
    match store.get(&ObjectPath::from(ACTIVE_WRITER_PATH)).await {
        Ok(result) => {
            let bytes = result.bytes().await.context("read active writer claim")?;
            let value = std::str::from_utf8(&bytes).context("decode active writer claim")?;
            Ok(Some(value.parse().context("parse active writer epoch")?))
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error).context("read active writer claim"),
    }
}

async fn read_generation_authority(
    store: &dyn ObjectStore,
    repository_id: &str,
) -> Result<(GenerationAuthority, Option<UpdateVersion>)> {
    let path = ObjectPath::from(ACTIVE_GENERATION_PATH);
    match store.get(&path).await {
        Ok(result) => {
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let bytes = result
                .bytes()
                .await
                .context("read metadata generation authority")?;
            let authority: GenerationAuthority =
                serde_json::from_slice(&bytes).context("decode metadata generation authority")?;
            if authority.format != 1 || authority.repository_id != repository_id {
                bail!("metadata generation authority identity mismatch")
            }
            Ok((authority, Some(version)))
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok((
            GenerationAuthority {
                format: 1,
                repository_id: repository_id.into(),
                decision: 0,
                active_generation: 1,
                namespace: "default".into(),
                previous_generation: 0,
                previous_namespace: Namespace::default(),
                state: "healthy".to_owned(),
                report_sha256: String::new(),
                decided_at_ms: 0,
                observation_until_ms: 0,
                retired_generation: 0,
            },
            None,
        )),
        Err(error) => Err(error).context("read metadata generation authority"),
    }
}

async fn publish_generation_authority(
    store: &dyn ObjectStore,
    authority: &GenerationAuthority,
    version: Option<UpdateVersion>,
) -> Result<()> {
    let encoded = serde_json::to_vec(authority).context("encode metadata generation authority")?;
    let digest = Sha256::digest(&encoded);
    let decision_path = ObjectPath::from(format!(
        "{GENERATION_DECISION_PREFIX}/{:020}-{}",
        authority.decision,
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ));
    store
        .put_opts(
            &decision_path,
            encoded.clone().into(),
            PutOptions::from(PutMode::Create),
        )
        .await
        .context("publish immutable metadata generation decision")?;
    let mode = version.map_or(PutMode::Create, PutMode::Update);
    if let Err(error) = store
        .put_opts(
            &ObjectPath::from(ACTIVE_GENERATION_PATH),
            encoded.into(),
            PutOptions::from(mode),
        )
        .await
    {
        if matches!(
            error,
            slatedb::object_store::Error::AlreadyExists { .. }
                | slatedb::object_store::Error::Precondition { .. }
        ) {
            bail!("metadata generation authority changed concurrently")
        }
        return Err(error).context("activate metadata generation authority");
    }
    Ok(())
}

async fn release_writer_claim(store: &dyn ObjectStore, epoch: u64) -> Result<()> {
    if active_writer_epoch(store).await? != Some(epoch) {
        bail!("refusing to release a writer claim owned by another epoch")
    }
    store
        .delete(&ObjectPath::from(ACTIVE_WRITER_PATH))
        .await
        .context("release active writer claim")
}

fn unix_time_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("system time exceeds u64 milliseconds")
}

fn storage_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

fn transaction_expired(last_touched_ms: u64, now_ms: u64, timeout_ms: u64) -> bool {
    now_ms.saturating_sub(last_touched_ms) >= timeout_ms
}

async fn scan_prefix_db(db: &Db, prefix: &[u8], suffix: &[u8]) -> Result<DbIterator, Status> {
    if suffix.is_empty() {
        db.scan_prefix(prefix, ..).await.map_err(storage_error)
    } else {
        db.scan_prefix(prefix, (Excluded(suffix), Unbounded))
            .await
            .map_err(storage_error)
    }
}

async fn scan_prefix_reader(
    reader: &DbReader,
    prefix: &[u8],
    suffix: &[u8],
) -> Result<DbIterator, Status> {
    if suffix.is_empty() {
        reader.scan_prefix(prefix, ..).await.map_err(storage_error)
    } else {
        reader
            .scan_prefix(prefix, (Excluded(suffix), Unbounded))
            .await
            .map_err(storage_error)
    }
}

async fn scan_prefix_transaction(
    transaction: &DbTransaction,
    prefix: &[u8],
    suffix: &[u8],
) -> Result<DbIterator, Status> {
    if suffix.is_empty() {
        transaction
            .scan_prefix(prefix, ..)
            .await
            .map_err(storage_error)
    } else {
        transaction
            .scan_prefix(prefix, (Excluded(suffix), Unbounded))
            .await
            .map_err(storage_error)
    }
}

async fn collect_page(iterator: &mut DbIterator, page_size: usize) -> Result<ScanResponse, Status> {
    let mut entries = Vec::with_capacity(page_size);
    let mut response_bytes = 0usize;
    while entries.len() < page_size {
        let Some(item) = iterator.next().await.map_err(storage_error)? else {
            return Ok(ScanResponse {
                entries,
                done: true,
            });
        };
        let entry = KeyValue {
            key: item.key.to_vec(),
            value: item.value.to_vec(),
        };
        let next_size = response_bytes
            .checked_add(repeated_message_encoded_len(entry.encoded_len()))
            .ok_or_else(|| Status::resource_exhausted("scan response size overflow"))?;
        if next_size > crate::MAX_MESSAGE_BYTES as usize - DONE_FIELD_ENCODED_LEN {
            if entries.is_empty() {
                return Err(Status::resource_exhausted(
                    "scan entry exceeds response byte limit",
                ));
            }
            return Ok(ScanResponse {
                entries,
                done: false,
            });
        }
        response_bytes = next_size;
        entries.push(entry);
    }
    let done = iterator.next().await.map_err(storage_error)?.is_none();
    Ok(ScanResponse { entries, done })
}

pub(crate) fn repeated_message_encoded_len(message_len: usize) -> usize {
    1 + encoded_varint_len(message_len as u64) + message_len
}

fn encoded_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn validate_key(key: &[u8]) -> Result<(), Status> {
    if key.is_empty() {
        return Err(Status::invalid_argument("key must not be empty"));
    }
    Ok(())
}

fn validate_generation_input(generation: u64, namespace: &str, report_sha256: &str) -> Result<()> {
    if generation == 0 || namespace.trim().is_empty() || namespace.len() > 1024 {
        bail!("invalid candidate metadata generation")
    }
    validate_report_sha256(report_sha256)
}

fn validate_report_sha256(report_sha256: &str) -> Result<()> {
    if report_sha256.len() != 64 || !report_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("healing report SHA-256 must contain 64 hexadecimal characters")
    }
    Ok(())
}

fn validate_mutations(request: &WriteBatchRequest) -> Result<(), Status> {
    for put in &request.puts {
        validate_key(&put.key)?;
    }
    for key in &request.deletes {
        validate_key(key)?;
    }
    Ok(())
}

fn storage_error(error: slatedb::Error) -> Status {
    let message = format!("SlateDB operation failed: {error}");
    if encryption::is_integrity_error(&error) {
        return Status::data_loss(message);
    }
    match error.kind() {
        ErrorKind::Transaction => Status::aborted(message),
        ErrorKind::Unavailable | ErrorKind::Closed(_) => Status::unavailable(message),
        ErrorKind::Invalid => Status::invalid_argument(message),
        ErrorKind::Data => Status::data_loss(message),
        _ => Status::internal(message),
    }
}
