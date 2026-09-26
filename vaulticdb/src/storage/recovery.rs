use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt};
use std::{sync::Arc, time::Duration};

const ORDERING_ERROR: &str = "WAL replay saw out-of-order seqs across WAL files.";

#[derive(Clone, Copy)]
struct Limits {
    files: usize,
    bytes: u64,
    rows: u64,
    timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            files: 20_000,
            bytes: 256 * 1024 * 1024,
            rows: 2_000_000,
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Manifest {
    id: u64,
    writer_epoch: u64,
    last_l0_seq: u64,
    replay_after_wal_id: u64,
}

#[derive(Serialize)]
struct Report {
    category: &'static str,
    event: &'static str,
    version: u32,
    complete: bool,
    manifest_stable: bool,
    reason: &'static str,
    manifest: Option<Manifest>,
    summary: Summary,
    operator_approval_required: bool,
    automatic_repair_allowed: bool,
    exclusive_ownership_verified: bool,
    steps: [&'static str; 6],
}

impl Default for Report {
    fn default() -> Self {
        Self {
            category: "recovery",
            event: "wal_recovery_plan",
            version: 1,
            complete: false,
            manifest_stable: false,
            reason: "inspection_failed",
            manifest: None,
            summary: Summary::default(),
            operator_approval_required: true,
            automatic_repair_allowed: false,
            exclusive_ownership_verified: false,
            steps: [
                "Stop retrying startup; coordinate all writers and verify exclusive ownership.",
                "Preserve metadata, every WAL namespace and coordination state; checksum-verify an independent copy.",
                "Confine all copied storage paths, including persisted WAL bindings, to an isolated working copy.",
                "Inspect authenticated WAL identity and sequence ranges; lower sequences alone do not authorize removal.",
                "Test reversible quarantine only on the copy; validate replay, latest durable data, snapshots, integrity and reopen.",
                "Obtain explicit operator approval with rollback evidence before any live repair or activation.",
            ],
        }
    }
}

pub(super) async fn diagnose_if_needed(
    error: &anyhow::Error,
    path: &str,
    store: Arc<dyn ObjectStore>,
    wal_store: Option<Arc<dyn ObjectStore>>,
) {
    if !is_ordering_failure(error) {
        return;
    }
    let report = inspect(
        path,
        store.clone(),
        wal_store.unwrap_or(store),
        Limits::default(),
    )
    .await;
    if let Ok(encoded) = serde_json::to_string(&report) {
        eprintln!("{encoded}");
    }
}

async fn read_manifest(admin: &slatedb::admin::Admin) -> Result<(Manifest, serde_json::Value)> {
    let value = serde_json::to_value(
        admin
            .read_manifest(None)
            .await?
            .context("manifest missing")?,
    )?;
    Ok((serde_json::from_value(value.clone())?, value))
}

async fn inspect(
    path: &str,
    store: Arc<dyn ObjectStore>,
    wal_store: Arc<dyn ObjectStore>,
    limits: Limits,
) -> Report {
    let mut report = Report::default();
    let outcome = tokio::time::timeout(
        limits.timeout,
        inspect_inner(path, store, wal_store, limits, &mut report),
    )
    .await;
    match outcome {
        Err(_) => report.reason = "time_limit",
        Ok(Err(_)) => report.reason = "inspection_failed",
        Ok(Ok(())) => {}
    }
    report
}

async fn inspect_inner(
    path: &str,
    store: Arc<dyn ObjectStore>,
    wal_store: Arc<dyn ObjectStore>,
    limits: Limits,
    report: &mut Report,
) -> Result<()> {
    let admin = slatedb::admin::Admin::builder(path, store).build();
    let (manifest, original) = read_manifest(&admin).await?;
    let cutoff = manifest.replay_after_wal_id;
    let flushed = manifest.last_l0_seq;
    report.manifest = Some(manifest);
    let prefix = Path::from(format!("{path}/wal"));
    let mut listing = wal_store.list(Some(&prefix));
    let mut files = Vec::new();
    let mut listed = 0usize;
    let mut bytes = 0u64;
    while let Some(object) = listing.next().await {
        let object = object?;
        listed += 1;
        if listed > limits.files {
            report.reason = "file_limit";
            return Ok(());
        }
        let name = object
            .location
            .as_ref()
            .strip_prefix(&format!("{path}/wal/"))
            .context("WAL prefix mismatch")?;
        let Some(number) = name.strip_suffix(".sst") else {
            anyhow::bail!("unexpected WAL object");
        };
        let wal_id = number.parse::<u64>()?;
        if wal_id <= cutoff {
            continue;
        }
        bytes = bytes
            .checked_add(object.size)
            .context("WAL size overflow")?;
        if object.size > 16 * 1024 * 1024 || bytes > limits.bytes {
            report.reason = "byte_limit";
            return Ok(());
        }
        files.push((wal_id, object));
    }
    files.sort_unstable_by_key(|(wal_id, _)| *wal_id);
    anyhow::ensure!(
        files.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "duplicate WAL ID"
    );
    let reader = slatedb::WalReader::new(path, wal_store.clone());
    report.summary.previous_id = Some(cutoff);
    for (wal_id, expected) in files {
        if wal_store.head(&expected.location).await? != expected {
            report.reason = "wal_changed";
            return Ok(());
        }
        let mut iterator = reader.get(wal_id).iterator().await?;
        let mut rows = 0u64;
        let mut minimum = u64::MAX;
        let mut maximum = 0;
        while let Some(row) = iterator.next().await? {
            rows += 1;
            if report.summary.inspected_rows + rows > limits.rows {
                report.reason = "row_limit";
                return Ok(());
            }
            minimum = minimum.min(row.seq);
            maximum = maximum.max(row.seq);
        }
        if wal_store.head(&expected.location).await? != expected {
            report.reason = "wal_changed";
            return Ok(());
        }
        report.summary.observe(
            wal_id,
            rows,
            (rows > 0).then_some((minimum, maximum)),
            flushed,
        );
    }
    let (_, current) = read_manifest(&admin).await?;
    report.manifest_stable = original == current;
    report.complete = report.manifest_stable;
    report.reason = if !report.manifest_stable {
        "manifest_changed"
    } else if report.summary.first_violation.is_some() {
        "ordering_violation_confirmed"
    } else {
        "ordering_violation_not_reproduced"
    };
    Ok(())
}

pub(crate) fn is_ordering_failure(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains(ORDERING_ERROR))
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct OrderingViolation {
    wal_id: u64,
    min_seq: u64,
    preceding_max_seq: u64,
}

#[derive(Default, Debug, Serialize)]
struct Summary {
    inspected_files: u64,
    inspected_rows: u64,
    gaps: u64,
    first_violation: Option<OrderingViolation>,
    lower_sequence_tail_files: u64,
    tail_above_flushed_watermark: bool,
    #[serde(skip)]
    previous_id: Option<u64>,
    #[serde(skip)]
    previous_max: Option<u64>,
}

impl Summary {
    fn observe(&mut self, wal_id: u64, rows: u64, bounds: Option<(u64, u64)>, flushed: u64) {
        self.inspected_files += 1;
        self.inspected_rows += rows;
        if self
            .previous_id
            .is_some_and(|previous| previous.checked_add(1) != Some(wal_id))
        {
            self.gaps += 1;
        }
        self.previous_id = Some(wal_id);
        if let Some((minimum, maximum)) = bounds {
            if let Some(previous) = self.previous_max {
                if minimum <= previous && self.first_violation.is_none() {
                    self.first_violation = Some(OrderingViolation {
                        wal_id,
                        min_seq: minimum,
                        preceding_max_seq: previous,
                    });
                }
            }
            if self.first_violation.is_some() {
                if maximum <= flushed {
                    self.lower_sequence_tail_files += 1;
                } else {
                    self.tail_above_flushed_watermark = true;
                }
            }
            self.previous_max = Some(maximum);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::ObjectStoreExt;

    async fn contents(store: &dyn ObjectStore) -> Vec<(String, bytes::Bytes)> {
        let mut listing = store.list(None);
        let mut result = Vec::new();
        while let Some(object) = listing.next().await {
            let object = object.unwrap();
            result.push((
                object.location.to_string(),
                store
                    .get(&object.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
            ));
        }
        result.sort_by(|left, right| left.0.cmp(&right.0));
        result
    }

    #[tokio::test]
    async fn inspection_is_read_only_and_limits_fail_closed() {
        check_inspection(false).await;
    }

    #[tokio::test]
    async fn encrypted_inspection_is_read_only_and_limits_fail_closed() {
        check_inspection(true).await;
    }

    async fn check_inspection(encrypted: bool) {
        let mut main: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let mut wal: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        if encrypted {
            main = vaulticdb::encryption::envelope::wrap_brokered_object_store(
                "recovery-test",
                main,
                &[7; 32],
                1,
            )
            .unwrap();
            wal = vaulticdb::encryption::envelope::wrap_brokered_object_store(
                "recovery-test",
                wal,
                &[7; 32],
                1,
            )
            .unwrap();
        }
        let db = slatedb::Db::builder("db", main.clone())
            .with_wal_object_store(wal.clone())
            .build()
            .await
            .unwrap();
        let mut batch = slatedb::WriteBatch::new();
        batch.put(b"secret-key-not-in-report", b"secret-value-not-in-report");
        db.write(batch)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let files = slatedb::WalReader::new("db", wal.clone())
            .list(..)
            .await
            .unwrap();
        let source = files
            .last()
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .metadata
            .location;
        let next = files.last().unwrap().id + 10;
        db.close().await.unwrap();
        let payload = wal.get(&source).await.unwrap().bytes().await.unwrap();
        wal.put(
            &Path::from(format!("db/wal/{next:020}.sst")),
            payload.clone().into(),
        )
        .await
        .unwrap();
        wal.put(
            &Path::from(format!("db/wal/{:020}.sst", next + 1)),
            payload.into(),
        )
        .await
        .unwrap();
        let original_main = contents(main.as_ref()).await;
        let original_wal = contents(wal.as_ref()).await;
        let report = inspect("db", main.clone(), wal.clone(), Limits::default()).await;
        assert!(report.complete);
        assert!(report.manifest_stable);
        assert_eq!(report.reason, "ordering_violation_confirmed");
        assert!(report.operator_approval_required);
        assert!(!report.automatic_repair_allowed);
        assert!(!report.exclusive_ownership_verified);
        let encoded = serde_json::to_string(&report).unwrap();
        assert!(!encoded.contains("secret-key"));
        assert!(!encoded.contains("secret-value"));
        for (limits, reason) in [
            (
                Limits {
                    files: 0,
                    ..Limits::default()
                },
                "file_limit",
            ),
            (
                Limits {
                    bytes: 0,
                    ..Limits::default()
                },
                "byte_limit",
            ),
            (
                Limits {
                    rows: 0,
                    ..Limits::default()
                },
                "row_limit",
            ),
        ] {
            let report = inspect("db", main.clone(), wal.clone(), limits).await;
            assert!(!report.complete);
            assert!(!report.automatic_repair_allowed);
            assert_eq!(report.reason, reason);
        }
        assert_eq!(contents(main.as_ref()).await, original_main);
        assert_eq!(contents(wal.as_ref()).await, original_wal);
    }

    #[tokio::test]
    async fn stalled_store_hits_deadline_without_authorizing_repair() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::limit::LimitStore::new(
            slatedb::object_store::memory::InMemory::new(),
            0,
        ));
        let report = inspect(
            "db",
            store.clone(),
            store,
            Limits {
                timeout: Duration::from_millis(5),
                ..Limits::default()
            },
        )
        .await;
        assert_eq!(report.reason, "time_limit");
        assert!(!report.complete);
        assert!(!report.automatic_repair_allowed);
    }

    #[tokio::test]
    async fn missing_manifest_never_produces_complete_plan() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let report = inspect("missing", store.clone(), store, Limits::default()).await;
        assert!(!report.complete);
        assert_eq!(report.reason, "inspection_failed");
    }

    #[test]
    fn classifies_only_ordering_failures() {
        assert!(is_ordering_failure(
            &anyhow::anyhow!(ORDERING_ERROR).context("open reader")
        ));
        assert!(!is_ordering_failure(&anyhow::anyhow!(
            "detected newer DB client"
        )));
        assert!(!is_ordering_failure(&anyhow::anyhow!("storage timeout")));
    }

    #[test]
    fn summarizes_overlap_gaps_and_unsafe_tail() {
        let mut summary = Summary::default();
        summary.observe(83950, 1, Some((279193, 279193)), 272534);
        summary.observe(83951, 256, Some((245877, 245877)), 272534);
        summary.observe(87199, 256, Some((249125, 249125)), 272534);
        assert_eq!(
            summary.first_violation,
            Some(OrderingViolation {
                wal_id: 83951,
                min_seq: 245877,
                preceding_max_seq: 279193,
            })
        );
        assert_eq!(summary.lower_sequence_tail_files, 2);
        assert_eq!(summary.gaps, 1);
        assert!(!summary.tail_above_flushed_watermark);
        summary.observe(87200, 1, Some((280000, 280000)), 272534);
        assert!(summary.tail_above_flushed_watermark);
    }

    #[test]
    fn empty_fencing_markers_do_not_reset_ordering() {
        let mut summary = Summary::default();
        summary.observe(1, 1, Some((10, 10)), 10);
        summary.observe(2, 0, None, 10);
        summary.observe(3, 1, Some((10, 10)), 10);
        assert!(summary.first_violation.is_some());
        assert_eq!(summary.inspected_rows, 2);
    }
}
