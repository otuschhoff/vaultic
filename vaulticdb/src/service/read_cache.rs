//! gRPC handlers for disposable SlateDB read-cache policy and status.

use tonic::{Request, Response, Status};

use crate::{
    error::VaulticDbError,
    proto::{
        ReadCacheConfidentiality, ReadCacheMetrics, ReadCacheStatusRequest,
        ReadCacheStatusResponse, ReadCacheTierPolicy, ReadCacheTierStatus,
        UpdateReadCachePolicyRequest,
    },
    storage::cache::{
        CacheConfidentiality, CacheMetricsSnapshot, CacheStatus, CacheTierPolicy,
        CacheTierPolicyUpdate,
    },
};

use super::{check_context, check_request, Service};

fn metrics_response(metrics: CacheMetricsSnapshot) -> ReadCacheMetrics {
    ReadCacheMetrics {
        hits: metrics.hits,
        misses: metrics.misses,
        origin_reads: metrics.origin_reads,
        origin_reads_avoided: metrics.origin_reads_avoided,
        corruptions: metrics.corruptions,
        timeouts: metrics.timeouts,
        failures: metrics.failures,
        bypasses: metrics.bypasses,
        admissions: metrics.admissions,
        admission_rejections: metrics.admission_rejections,
        admission_rejections_reservation: metrics.admission_rejections_reservation,
        admission_rejections_background_budget: metrics.admission_rejections_background_budget,
        admission_rejections_background_task: metrics.admission_rejections_background_task,
        admission_rejection_reasons_available: true,
        capacity_evictions: metrics.capacity_evictions,
        idle_evictions: metrics.idle_evictions,
        absolute_evictions: metrics.absolute_evictions,
        corruption_evictions: metrics.corruption_evictions,
        read_latency_total_us: metrics.read_latency_total_us,
        read_latency_count: metrics.read_latency_count,
        write_latency_total_us: metrics.write_latency_total_us,
        write_latency_count: metrics.write_latency_count,
    }
}

fn policy_response(id: String, policy: CacheTierPolicy) -> ReadCacheTierPolicy {
    ReadCacheTierPolicy {
        tier_id: id,
        enabled: policy.enabled,
        max_bytes: policy.max_bytes,
        idle_age_ms: policy.idle_age_ms,
        absolute_age_ms: policy.absolute_age_ms,
        read_priority: policy.read_priority,
        admission_priority: policy.admission_priority,
        timeout_ms: policy.timeout_ms,
    }
}

fn status_response(status: Option<CacheStatus>) -> ReadCacheStatusResponse {
    let Some(status) = status else {
        return ReadCacheStatusResponse::default();
    };
    ReadCacheStatusResponse {
        revision: status.revision,
        namespace: status.namespace,
        aggregate_max_bytes: status.aggregate_max_bytes,
        used_bytes: status.used_bytes,
        reserved_bytes: status.reserved_bytes,
        pinned_bytes: status.pinned_bytes,
        inflight_bytes: status.inflight_bytes,
        max_inflight_bytes: status.max_inflight_bytes,
        deletion_pending_bytes: status
            .deletion_pending_known
            .then_some(status.deletion_pending_bytes),
        pending_reclaim_bytes: status.pending_reclaim_bytes,
        quota_coordination_healthy: status.quota_coordination_healthy,
        quota_ledger_revision: status.quota_ledger_revision,
        quota_lease_expiry_unix_ms: status.quota_lease_expiry_ms,
        unverified_stale_bytes: status.unverified_stale_bytes,
        quota_reconciliation_lag: status.quota_reconciliation_lag,
        local_used_bytes: status.local_used_bytes,
        local_reserved_bytes: status.local_reserved_bytes,
        policy_sync_lag: status.policy_sync_lag,
        policy_sync_error: status.policy_sync_error,
        metrics: Some(metrics_response(status.metrics)),
        tiers: status
            .tiers
            .into_iter()
            .map(|tier| ReadCacheTierStatus {
                policy: Some(policy_response(tier.id, tier.policy)),
                used_bytes: tier.used_bytes,
                reserved_bytes: tier.reserved_bytes,
                pinned_bytes: tier.pinned_bytes,
                requested_max_bytes: tier.requested_max_bytes,
                deletion_pending_bytes: tier
                    .deletion_pending_known
                    .then_some(tier.deletion_pending_bytes),
                pending_reclaim_bytes: tier.pending_reclaim_bytes,
                reconciliation_lag: tier.reconciliation_lag,
                circuit_open: tier.circuit_open,
                metrics: Some(metrics_response(tier.metrics)),
                confidentiality: match tier.confidentiality {
                    CacheConfidentiality::Encrypted => ReadCacheConfidentiality::Encrypted as i32,
                    CacheConfidentiality::DecryptedHighlyTrusted => {
                        ReadCacheConfidentiality::DecryptedHighlyTrusted as i32
                    }
                },
                local_used_bytes: tier.local_used_bytes,
                local_reserved_bytes: tier.local_reserved_bytes,
            })
            .collect(),
    }
}

fn policy_update(policy: ReadCacheTierPolicy) -> Result<CacheTierPolicyUpdate, Status> {
    if policy.tier_id.is_empty() {
        return Err(VaulticDbError::InvalidRequest {
            field: "tiers.tier_id".to_owned(),
            message: "cache tier ID is required".to_owned(),
        }
        .into());
    }
    Ok(CacheTierPolicyUpdate {
        id: policy.tier_id,
        policy: CacheTierPolicy {
            enabled: policy.enabled,
            max_bytes: policy.max_bytes,
            idle_age_ms: policy.idle_age_ms,
            absolute_age_ms: policy.absolute_age_ms,
            read_priority: policy.read_priority,
            admission_priority: policy.admission_priority,
            timeout_ms: policy.timeout_ms,
        },
    })
}

impl Service {
    pub(super) async fn handle_cache_status(
        &self,
        request: Request<ReadCacheStatusRequest>,
    ) -> Result<Response<ReadCacheStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let storage = self.storage().await?;
        Ok(Response::new(status_response(storage.cache_status())))
    }

    pub(super) async fn handle_update_cache_policy(
        &self,
        request: Request<UpdateReadCachePolicyRequest>,
    ) -> Result<Response<ReadCacheStatusResponse>, Status> {
        check_request(&self.state, &request, &request.get_ref().repository_id)?;
        check_context(request.get_ref().context.as_ref())?;
        let _admission = self.mutation_admission().await?;
        let request = request.into_inner();
        let updates = request
            .tiers
            .into_iter()
            .map(policy_update)
            .collect::<Result<Vec<_>, _>>()?;
        let storage = self.storage().await?;
        let status = storage
            .update_cache_policy(request.expected_revision, updates)
            .await
            .map_err(|error| {
                let message = error.to_string();
                if message.contains("revision mismatch") {
                    VaulticDbError::Precondition {
                        field: "expected_revision".to_owned(),
                        message: "read-cache policy revision does not match".to_owned(),
                    }
                } else if message.contains("not configured") {
                    VaulticDbError::Precondition {
                        field: "tiers".to_owned(),
                        message: "read cache is not configured".to_owned(),
                    }
                } else if message.contains("unknown read-cache tier") {
                    VaulticDbError::InvalidRequest {
                        field: "tiers.tier_id".to_owned(),
                        message,
                    }
                } else if message.contains("read-cache tier") {
                    VaulticDbError::InvalidRequest {
                        field: "tiers".to_owned(),
                        message,
                    }
                } else {
                    VaulticDbError::StorageUnavailable {
                        message: "read-cache policy persistence failed".to_owned(),
                    }
                }
            })
            .map_err(Status::from)?;
        Ok(Response::new(status_response(Some(status))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::atomic::AtomicBool, sync::Arc, time::Duration};
    use tokio::sync::{watch, Mutex, RwLock};

    use crate::{
        lifecycle::DaemonLifecycle,
        proto::RequestContext,
        service::DaemonState,
        storage::{
            cache::{CacheConfig, CacheTierConfig},
            ObjectStoreConfig, ReplicaStoreConfig, StorageConfig, TopologySource, WalStoreConfig,
        },
    };
    use vaulticdb::{
        encryption::envelope::{EncryptionConfig, EncryptionMode, ProviderCredentials},
        writer_role::WriterRoleState,
    };

    fn context() -> Option<RequestContext> {
        Some(RequestContext {
            request_id: "cache-test".to_owned(),
            deadline_unix_ms: 0,
        })
    }

    fn storage_config(cache: CacheConfig) -> StorageConfig {
        StorageConfig {
            object_store: ObjectStoreConfig::Memory,
            wal_store: WalStoreConfig::Inherit,
            slatedb_tuning: crate::storage::SlateDbTuning::default(),
            cache,
            fencing_replica: None,
            metadata_rebuild_initialize: false,
            metadata_rebuild_reset: false,
            bulk_import_local_wal_data_dir: None,
            broker: None,
            encryption: EncryptionConfig {
                mode: EncryptionMode::Off,
                passphrase_file: None,
                recovery_acknowledged: false,
                provider_credentials: ProviderCredentials::new(HashMap::new()),
            },
            transaction_idle_timeout_ms: 1_000,
            slatedb_multiget: false,
            attribution_disabled: false,
            topology_source: TopologySource::External,
            topology_override_local: None,
        }
    }

    fn cache_config() -> CacheConfig {
        CacheConfig {
            tiers: vec![CacheTierConfig {
                id: "ram".to_owned(),
                store: ReplicaStoreConfig::Memory,
                confidentiality:
                    crate::storage::cache::CacheConfidentiality::DecryptedHighlyTrusted,
                policy: CacheTierPolicy {
                    enabled: true,
                    max_bytes: 1024 * 1024,
                    idle_age_ms: 0,
                    absolute_age_ms: None,
                    read_priority: 1,
                    admission_priority: 1,
                    timeout_ms: 250,
                },
            }],
            aggregate_max_bytes: Some(1024 * 1024),
            part_size_bytes: 4096,
            max_inflight_bytes: 8192,
            max_background_tasks: 2,
        }
    }

    async fn service(cache: CacheConfig) -> (Service, Arc<crate::storage::Storage>) {
        let repository_id = format!("cache-service-{}", rand::random::<u64>());
        let storage = Arc::new(
            crate::storage::Storage::open(&repository_id, &storage_config(cache))
                .await
                .unwrap(),
        );
        let now = std::time::Instant::now();
        let (shutdown, _) = watch::channel(false);
        let service = Service {
            state: DaemonState {
                daemon_id: Arc::from("cache-test-daemon"),
                repository_id: repository_id.into(),
                auth_token: None,
                unix_socket: true,
                tcp_enabled: false,
                draining: Arc::new(AtomicBool::new(false)),
                lifecycle: Arc::new(Mutex::new(DaemonLifecycle::loading(0))),
                writer_role: Arc::new(Mutex::new(WriterRoleState::read_only(
                    0,
                    now,
                    Duration::ZERO,
                ))),
                writer_transition: Arc::new(Mutex::new(())),
                mutation_admission: Arc::new(RwLock::new(())),
                attribution: Arc::new(crate::attribution::ServiceAttribution::default()),
                last_writer_activity: Arc::new(Mutex::new(now)),
                minimum_writer_tenure: Duration::ZERO,
                writer_idle_grace: None,
                writer_transition_timeout: Duration::from_secs(30),
                clock_started: now,
                clock_started_unix_ms: 0,
            },
            shutdown,
            storage: Arc::new(RwLock::new(Some(storage.clone()))),
        };
        (service, storage)
    }

    fn status_request(service: &Service) -> Request<ReadCacheStatusRequest> {
        Request::new(ReadCacheStatusRequest {
            repository_id: service.state.repository_id.to_string(),
            context: context(),
        })
    }

    fn update_request(
        service: &Service,
        expected_revision: u64,
        tier_id: &str,
    ) -> Request<UpdateReadCachePolicyRequest> {
        Request::new(UpdateReadCachePolicyRequest {
            repository_id: service.state.repository_id.to_string(),
            context: context(),
            expected_revision,
            tiers: vec![ReadCacheTierPolicy {
                tier_id: tier_id.to_owned(),
                enabled: true,
                max_bytes: 512 * 1024,
                idle_age_ms: 1_000,
                absolute_age_ms: Some(2_000),
                read_priority: 2,
                admission_priority: 3,
                timeout_ms: 100,
            }],
        })
    }

    #[tokio::test]
    async fn cache_status_reports_policy_and_no_cache_is_empty() {
        let (configured, configured_storage) = service(cache_config()).await;
        let response = configured
            .handle_cache_status(status_request(&configured))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.revision, 0);
        assert!(!response.namespace.is_empty());
        assert!(response.quota_coordination_healthy);
        assert!(response.quota_ledger_revision > 0);
        assert!(response.quota_lease_expiry_unix_ms > 0);
        assert_eq!(response.tiers.len(), 1);
        assert_eq!(response.tiers[0].policy.as_ref().unwrap().tier_id, "ram");
        assert_eq!(
            response.tiers[0].confidentiality,
            ReadCacheConfidentiality::DecryptedHighlyTrusted as i32
        );
        configured_storage.close().await.unwrap();

        let (disabled, disabled_storage) = service(CacheConfig::default()).await;
        let response = disabled
            .handle_cache_status(status_request(&disabled))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response, ReadCacheStatusResponse::default());
        disabled_storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn update_cache_policy_reports_cas_and_unknown_tier_errors() {
        let (service, storage) = service(cache_config()).await;
        let response = service
            .handle_update_cache_policy(update_request(&service, 0, "ram"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.revision, 1);
        assert_eq!(response.tiers[0].requested_max_bytes, 512 * 1024);

        let cas = service
            .handle_update_cache_policy(update_request(&service, 0, "ram"))
            .await
            .unwrap_err();
        assert_eq!(cas.code(), tonic::Code::FailedPrecondition);
        assert!(!cas.details().is_empty());

        let unknown = service
            .handle_update_cache_policy(update_request(&service, 1, "missing"))
            .await
            .unwrap_err();
        assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
        assert!(!unknown.details().is_empty());
        storage.close().await.unwrap();
    }
}
