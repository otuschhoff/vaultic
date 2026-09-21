package telemetry

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	MonitorSchemaVersion   = 2
	MaxMonitorComponents   = 16
	MaxMonitorMetrics      = 256
	MaxMonitorOperations   = 128
	MaxMonitorQueues       = 64
	MaxMonitorStorage      = 128
	MaxMonitorCaches       = 64
	MaxMonitorLabels       = 8
	MaxHistogramBuckets    = 64
	MaxMonitorStringLength = 128
)

var monitorNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type metricSpec struct {
	kind     MetricKind
	unit     string
	labels   []string
	required []string
	bounds   [][]uint64
}

func vaulticLatencyBounds() []uint64 {
	return []uint64{10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, math.MaxUint64}
}

func slateDBLatencyBounds() []uint64 {
	return []uint64{1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, math.MaxUint64}
}

var monitorValueSets = map[string]map[string]struct{}{
	"component":      values("vaultic", "vaulticdb", "key_broker", "cache_coordinator"),
	"queue":          values("batch_write", "legacy_import_ingest", "legacy_import_reduce"),
	"operation":      values("backup", "restore", "check", "legacy_import", "forget", "prune", "replicate", "cache_fill", "cache_evict", "placement", "export", "analytics", "maintenance", "gdpr", "staging_reconcile", "key_management", "compaction", "recovery"),
	"outcome":        values("success", "failure", "cancellation", "timeout"),
	"role":           values("repository", "database", "wal", "coordination", "source", "scratch", "cache", "rpc", "broker"),
	"representation": values("encrypted_pack", "encrypted_range", "compressed_container", "decoded_extent", "whole_file", "sst", "block", "metadata"),
	"throttle":       values("none", "concurrency", "bandwidth", "capacity", "backend_retry", "credential_renewal", "writer_fencing", "wal_flush", "wal_retention", "compaction", "durability", "shutdown"),
	"phase":          values("queued", "admission", "planning", "source", "read", "write", "upload", "publish", "reconcile", "ingest", "reduce", "cleanup", "finalize", "verify", "delete", "retry", "wait", "complete"),
	"stage": values(
		"prepare", "hash", "hints", "ingest_begin", "receipt_read", "pack_read", "blob_read", "plan_build",
		"mutation_rpc", "commit", "post_commit", "ingest_retry_backoff", "ingest_recovery_read",
		"reduce_begin", "reduce_receipt_read", "reduce_prefetch", "reduce_aggregate_plan", "reduce_history_plan", "reduce_encode",
		"reduce_mutation_rpc", "reduce_commit", "reduce_retry_backoff", "reduce_recovery_read", "reduce_checkpoint_read",
		"planning_total", "reduction_total", "gate_wait", "transaction_total", "cleanup_total", "cleanup_scan",
		"cleanup_begin", "cleanup_write", "cleanup_commit",
	),
	"statistic": values(
		"batches", "ingested_batches", "reduced_batches", "attempts", "ingest_attempts", "reduce_attempts",
		"ingest_failures", "reduce_failures", "commits", "retries", "conflicts", "packs_committed", "blobs_committed",
		"mutations_committed", "mutation_rpcs", "mutation_rpc_attempts", "reduction_mutation_rpcs",
		"reduction_mutation_rpc_attempts", "reduction_mutations", "receipt_reads", "reduction_receipt_reads",
		"recovery_reads", "reduce_checkpoint_reads", "catalog_read_rpcs", "catalog_read_keys", "reduction_plan_read_rpcs",
		"reduction_plan_read_keys", "planning_reads", "source_indexes", "definitely_absent", "possibly_present", "found",
		"false_positive_equivalent", "cleanup_calls", "cleanup_pages", "cleanup_receipts", "cleanup_deferred_commits",
		"filter_inserts", "encoded_committed", "replanned",
	),
	"blocking":        values("prerequisite", "lock", "concurrency", "byte_budget", "source_io", "backend_io", "rpc_response", "retry_backoff", "credential_renewal", "writer_fencing", "durability", "wal_flush", "compaction", "human_confirmation", "shutdown"),
	"storage_role":    values("repository", "database", "wal", "coordination", "source", "scratch", "cache"),
	"storage_class":   values("pack", "index", "snapshot", "sst", "manifest", "wal_segment", "coordination", "scratch", "unknown"),
	"placement_state": values("primary", "replica", "pending", "deleting", "reconciled", "unknown"),
	"cache_family":    values("aggregate", "memory", "disk", "remote", "slatedb", "unknown"),
	"acknowledgement": values("unknown", "inherited", "memory", "persistent", "local-process", "shared-remote", "durable-object-store", "test", "unsupported"),
	"wal_target":      values("inherited", "local", "memory", "s3", "rados", "test", "unsupported-azure", "unsupported-gcs"),
}

func monitorValues(kind string) map[string]struct{} { return monitorValueSets[kind] }

type Availability string

const (
	AvailabilityExact         Availability = "exact"
	AvailabilityEstimated     Availability = "estimated"
	AvailabilityStale         Availability = "stale"
	AvailabilityUnavailable   Availability = "unavailable"
	AvailabilityNotApplicable Availability = "not_applicable"
)

type MetricKind string

const (
	MetricGauge     MetricKind = "gauge"
	MetricCounter   MetricKind = "counter"
	MetricHistogram MetricKind = "histogram"
)

type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metric struct {
	Name         string       `json:"name"`
	Kind         MetricKind   `json:"kind"`
	Unit         string       `json:"unit"`
	Availability Availability `json:"availability"`
	Labels       []Label      `json:"labels,omitempty"`
	Value        uint64       `json:"value,omitempty"`
	Count        uint64       `json:"count,omitempty"`
	Sum          uint64       `json:"sum,omitempty"`
	Maximum      uint64       `json:"maximum,omitempty"`
	BucketUpper  []uint64     `json:"bucket_upper,omitempty"`
	BucketCounts []uint64     `json:"bucket_counts,omitempty"`
}

type QueueSnapshot struct {
	Name                 string       `json:"name"`
	Availability         Availability `json:"availability"`
	CapacityAvailability Availability `json:"capacity_availability"`
	Depth                uint64       `json:"depth"`
	Capacity             uint64       `json:"capacity"`
	ActiveWorkers        uint64       `json:"active_workers"`
	Admitted             uint64       `json:"admitted"`
	Rejected             uint64       `json:"rejected"`
	OldestItemAgeUS      uint64       `json:"oldest_item_age_us"`
	Backpressure         string       `json:"backpressure,omitempty"`
	BackpressureTime     uint64       `json:"backpressure_time_us,omitempty"`
}

type ActiveOperation struct {
	ID             string `json:"id"`
	ParentID       string `json:"parent_id,omitempty"`
	Class          string `json:"class"`
	Phase          string `json:"phase"`
	BlockingReason string `json:"blocking_reason,omitempty"`
	StartedUnixMS  int64  `json:"started_unix_ms"`
	UpdatedUnixMS  int64  `json:"updated_unix_ms"`
	CompletedUnits uint64 `json:"completed_units"`
	ExpectedUnits  uint64 `json:"expected_units,omitempty"`
}

type StorageSnapshot struct {
	BackendID                  string       `json:"backend_id"`
	Role                       string       `json:"role"`
	Availability               Availability `json:"availability"`
	ObjectCount                uint64       `json:"object_count"`
	PayloadBytes               uint64       `json:"payload_bytes"`
	PhysicalBytes              uint64       `json:"physical_bytes,omitempty"`
	ReconciledAtMS             int64        `json:"reconciled_at_ms,omitempty"`
	Acknowledgement            string       `json:"acknowledgement,omitempty"`
	ObjectClass                string       `json:"object_class,omitempty"`
	PlacementState             string       `json:"placement_state,omitempty"`
	Representation             string       `json:"representation,omitempty"`
	ObjectCountAvailability    Availability `json:"object_count_availability"`
	PayloadAvailability        Availability `json:"payload_availability"`
	PhysicalAvailability       Availability `json:"physical_availability"`
	ReconciliationAvailability Availability `json:"reconciliation_availability"`
}

type CacheSnapshot struct {
	ID                            string       `json:"id"`
	Availability                  Availability `json:"availability"`
	RequestedBytes                uint64       `json:"requested_bytes"`
	EffectiveBytes                uint64       `json:"effective_bytes"`
	UsedBytes                     uint64       `json:"used_bytes"`
	ReservedBytes                 uint64       `json:"reserved_bytes"`
	PinnedBytes                   uint64       `json:"pinned_bytes"`
	StagingBytes                  uint64       `json:"staging_bytes"`
	DeletionPendingBytes          uint64       `json:"deletion_pending_bytes"`
	ReclaimPendingBytes           uint64       `json:"reclaim_pending_bytes"`
	AvailableBytes                uint64       `json:"available_bytes"`
	Hits                          uint64       `json:"hits"`
	Misses                        uint64       `json:"misses"`
	OriginBytes                   uint64       `json:"origin_bytes"`
	CacheBytes                    uint64       `json:"cache_bytes"`
	OriginReadsAvoided            uint64       `json:"origin_reads_avoided"`
	InflightFills                 uint64       `json:"inflight_fills"`
	ReconciliationAgeMS           uint64       `json:"reconciliation_age_ms,omitempty"`
	ReconciliationLag             uint64       `json:"reconciliation_lag"`
	ControllerState               string       `json:"controller_state,omitempty"`
	CircuitState                  string       `json:"circuit_state,omitempty"`
	Enabled                       bool         `json:"enabled"`
	Family                        string       `json:"family,omitempty"`
	Representation                string       `json:"representation,omitempty"`
	ObjectCount                   uint64       `json:"object_count"`
	TrafficAvailability           Availability `json:"traffic_availability,omitempty"`
	TrafficBytesAvailability      Availability `json:"traffic_bytes_availability,omitempty"`
	FillAvailability              Availability `json:"fill_availability,omitempty"`
	InventoryAvailability         Availability `json:"inventory_availability,omitempty"`
	DeletionAvailability          Availability `json:"deletion_availability,omitempty"`
	ReconciliationAgeAvailability Availability `json:"reconciliation_age_availability,omitempty"`
	ReconciliationLagAvailability Availability `json:"reconciliation_lag_availability,omitempty"`
}

type OperationOverflowSnapshot struct {
	Class string `json:"class"`
	Count uint64 `json:"count"`
}

type WALSnapshot struct {
	Target                 string       `json:"target"`
	Durability             string       `json:"durability"`
	Availability           Availability `json:"availability"`
	UploadedBytes          uint64       `json:"uploaded_bytes"`
	OutstandingFlushes     uint64       `json:"outstanding_flushes"`
	RetainedBytes          uint64       `json:"retained_bytes"`
	RetainedSegments       uint64       `json:"retained_segments"`
	OldestUncheckpointedMS uint64       `json:"oldest_uncheckpointed_ms"`
	DurabilityFailures     uint64       `json:"durability_failures"`
	CleanupFailures        uint64       `json:"cleanup_failures"`
	ThrottleReason         string       `json:"throttle_reason,omitempty"`
}

type ComponentSnapshot struct {
	Component          string                      `json:"component"`
	ProcessStartID     string                      `json:"process_start_id"`
	CapturedUnixMS     int64                       `json:"captured_unix_ms"`
	Availability       Availability                `json:"availability"`
	Stale              bool                        `json:"stale"`
	Metrics            []Metric                    `json:"metrics,omitempty"`
	Queues             []QueueSnapshot             `json:"queues,omitempty"`
	Operations         []ActiveOperation           `json:"operations,omitempty"`
	OperationOverflow  []OperationOverflowSnapshot `json:"operation_overflow,omitempty"`
	CardinalityDropped uint64                      `json:"cardinality_dropped"`
	Storage            []StorageSnapshot           `json:"storage,omitempty"`
	Caches             []CacheSnapshot             `json:"caches,omitempty"`
	WAL                *WALSnapshot                `json:"wal,omitempty"`
}

type MonitorSnapshot struct {
	SchemaVersion  int                 `json:"schema_version"`
	CapturedUnixMS int64               `json:"captured_unix_ms"`
	Components     []ComponentSnapshot `json:"components"`
}

func NewMonitorSnapshot(now time.Time, components ...ComponentSnapshot) MonitorSnapshot {
	return MonitorSnapshot{
		SchemaVersion: MonitorSchemaVersion, CapturedUnixMS: now.UnixMilli(), Components: components,
	}
}

func (snapshot MonitorSnapshot) Validate() error {
	if snapshot.SchemaVersion != MonitorSchemaVersion {
		return fmt.Errorf("unsupported monitor schema version %d", snapshot.SchemaVersion)
	}
	if snapshot.CapturedUnixMS <= 0 {
		return errors.New("monitor capture timestamp is required")
	}
	if len(snapshot.Components) > MaxMonitorComponents {
		return fmt.Errorf("monitor components exceed limit %d", MaxMonitorComponents)
	}
	seenComponents := make(map[string]struct{}, len(snapshot.Components))
	for index := range snapshot.Components {
		component := &snapshot.Components[index]
		if err := validateMonitorName("component", component.Component); err != nil {
			return err
		}
		if _, known := monitorValues("component")[component.Component]; !known {
			return fmt.Errorf("unknown monitor component %q", component.Component)
		}
		if _, exists := seenComponents[component.Component]; exists {
			return fmt.Errorf("duplicate monitor component %q", component.Component)
		}
		seenComponents[component.Component] = struct{}{}
		if !validConfiguredID(component.ProcessStartID) {
			return fmt.Errorf("component %q has invalid process start ID", component.Component)
		}
		if component.CapturedUnixMS <= 0 {
			return fmt.Errorf("component %q capture timestamp is required", component.Component)
		}
		if !validAvailability(component.Availability) {
			return fmt.Errorf("component %q has invalid availability %q", component.Component, component.Availability)
		}
		if len(component.Metrics) > MaxMonitorMetrics {
			return fmt.Errorf("component %q metrics exceed limit %d", component.Component, MaxMonitorMetrics)
		}
		if len(component.Operations) > MaxMonitorOperations {
			return fmt.Errorf("component %q operations exceed limit %d", component.Component, MaxMonitorOperations)
		}
		if len(component.OperationOverflow) > len(monitorValues("operation")) {
			return fmt.Errorf("component %q operation overflow records exceed bounded classes", component.Component)
		}
		if len(component.Queues) > MaxMonitorQueues {
			return fmt.Errorf("component %q queues exceed limit %d", component.Component, MaxMonitorQueues)
		}
		if len(component.Storage) > MaxMonitorStorage {
			return fmt.Errorf("component %q storage records exceed limit %d", component.Component, MaxMonitorStorage)
		}
		if len(component.Caches) > MaxMonitorCaches {
			return fmt.Errorf("component %q caches exceed limit %d", component.Component, MaxMonitorCaches)
		}
		if err := validateMetrics(component.Metrics); err != nil {
			return fmt.Errorf("component %q: %w", component.Component, err)
		}
		seenQueues := make(map[string]struct{}, len(component.Queues))
		for _, queue := range component.Queues {
			if err := validateMonitorName("queue", queue.Name); err != nil {
				return err
			}
			if _, known := monitorValues("queue")[queue.Name]; !known {
				return fmt.Errorf("queue %q is not in monitor schema", queue.Name)
			}
			if _, exists := seenQueues[queue.Name]; exists {
				return fmt.Errorf("component %q has duplicate queue %q", component.Component, queue.Name)
			}
			seenQueues[queue.Name] = struct{}{}
			if !validAvailability(queue.Availability) {
				return fmt.Errorf("queue %q has invalid availability %q", queue.Name, queue.Availability)
			}
			if !validAvailability(queue.CapacityAvailability) {
				return fmt.Errorf("queue %q has invalid capacity availability %q", queue.Name, queue.CapacityAvailability)
			}
			if queue.CapacityAvailability == AvailabilityExact && queue.Capacity != 0 && queue.Depth > queue.Capacity {
				return fmt.Errorf("queue %q depth exceeds capacity", queue.Name)
			}
			if queue.Backpressure != "" {
				if err := validateMonitorName("backpressure", queue.Backpressure); err != nil {
					return err
				}
				if _, known := monitorValues("throttle")[queue.Backpressure]; !known {
					return fmt.Errorf("queue %q has unbounded backpressure %q", queue.Name, queue.Backpressure)
				}
			}
		}
		seenOperations := make(map[string]struct{}, len(component.Operations))
		for _, operation := range component.Operations {
			if !validOpaqueID(operation.ID) {
				return errors.New("active operation has invalid ID")
			}
			if _, exists := seenOperations[operation.ID]; exists {
				return fmt.Errorf("component %q has duplicate active operation %q", component.Component, operation.ID)
			}
			seenOperations[operation.ID] = struct{}{}
			if operation.ParentID != "" && !validOpaqueID(operation.ParentID) {
				return fmt.Errorf("active operation %q has invalid parent ID", operation.ID)
			}
			if err := validateMonitorName("operation class", operation.Class); err != nil {
				return err
			}
			if _, known := monitorValues("operation")[operation.Class]; !known {
				return fmt.Errorf("active operation %q has unbounded class %q", operation.ID, operation.Class)
			}
			if err := validateMonitorName("operation phase", operation.Phase); err != nil {
				return err
			}
			if _, known := monitorValues("phase")[operation.Phase]; !known {
				return fmt.Errorf("active operation %q has unbounded phase %q", operation.ID, operation.Phase)
			}
			if operation.BlockingReason != "" {
				if err := validateMonitorName("blocking reason", operation.BlockingReason); err != nil {
					return err
				}
				if _, known := monitorValues("blocking")[operation.BlockingReason]; !known {
					return fmt.Errorf("active operation %q has unbounded blocking reason %q", operation.ID, operation.BlockingReason)
				}
			}
			if operation.StartedUnixMS <= 0 || operation.UpdatedUnixMS < operation.StartedUnixMS || operation.StartedUnixMS > component.CapturedUnixMS || operation.UpdatedUnixMS > component.CapturedUnixMS {
				return fmt.Errorf("active operation %q has invalid timestamps", operation.ID)
			}
		}
		seenOverflow := make(map[string]struct{}, len(component.OperationOverflow))
		for _, overflow := range component.OperationOverflow {
			if _, known := monitorValues("operation")[overflow.Class]; !known {
				return fmt.Errorf("operation overflow has unbounded class %q", overflow.Class)
			}
			if _, exists := seenOverflow[overflow.Class]; exists {
				return fmt.Errorf("component %q has duplicate operation overflow class %q", component.Component, overflow.Class)
			}
			seenOverflow[overflow.Class] = struct{}{}
		}
		seenStorage := make(map[string]struct{}, len(component.Storage))
		for _, storage := range component.Storage {
			if !validConfiguredID(storage.BackendID) {
				return fmt.Errorf("storage backend ID %q is invalid", storage.BackendID)
			}
			if err := validateMonitorName("storage role", storage.Role); err != nil {
				return err
			}
			if _, known := monitorValues("storage_role")[storage.Role]; !known {
				return fmt.Errorf("storage backend %q has unbounded role %q", storage.BackendID, storage.Role)
			}
			storageIdentity := strings.Join([]string{storage.BackendID, storage.Role, storage.ObjectClass, storage.PlacementState, storage.Representation}, "\x00")
			if _, exists := seenStorage[storageIdentity]; exists {
				return fmt.Errorf("component %q has duplicate storage identity", component.Component)
			}
			seenStorage[storageIdentity] = struct{}{}
			if !validAvailability(storage.Availability) {
				return fmt.Errorf("storage backend %q has invalid availability %q", storage.BackendID, storage.Availability)
			}
			if storage.Acknowledgement != "" {
				if _, known := monitorValues("acknowledgement")[storage.Acknowledgement]; !known {
					return fmt.Errorf("storage backend %q has invalid acknowledgement", storage.BackendID)
				}
			}
			if storage.ObjectClass != "" {
				if _, known := monitorValues("storage_class")[storage.ObjectClass]; !known {
					return fmt.Errorf("storage backend %q has invalid object class", storage.BackendID)
				}
			}
			if storage.PlacementState != "" {
				if _, known := monitorValues("placement_state")[storage.PlacementState]; !known {
					return fmt.Errorf("storage backend %q has invalid placement state", storage.BackendID)
				}
			}
			if storage.Representation != "" {
				if _, known := monitorValues("representation")[storage.Representation]; !known {
					return fmt.Errorf("storage backend %q has invalid representation", storage.BackendID)
				}
			}
			if !validAvailability(storage.ObjectCountAvailability) || !validAvailability(storage.PayloadAvailability) || !validAvailability(storage.PhysicalAvailability) || !validAvailability(storage.ReconciliationAvailability) {
				return fmt.Errorf("storage backend %q has invalid field availability", storage.BackendID)
			}
			if storage.ReconciledAtMS < 0 || storage.ReconciledAtMS > component.CapturedUnixMS || storage.ReconciliationAvailability == AvailabilityExact && storage.ReconciledAtMS == 0 {
				return fmt.Errorf("storage backend %q has invalid reconciliation timestamp", storage.BackendID)
			}
		}
		seenCaches := make(map[string]struct{}, len(component.Caches))
		for _, cache := range component.Caches {
			if !validConfiguredID(cache.ID) {
				return fmt.Errorf("cache ID %q is invalid", cache.ID)
			}
			cacheIdentity := strings.Join([]string{cache.ID, cache.Family, cache.Representation}, "\x00")
			if _, exists := seenCaches[cacheIdentity]; exists {
				return fmt.Errorf("component %q has duplicate cache %q", component.Component, cache.ID)
			}
			seenCaches[cacheIdentity] = struct{}{}
			if !validAvailability(cache.Availability) {
				return fmt.Errorf("cache %q has invalid availability %q", cache.ID, cache.Availability)
			}
			if cache.ControllerState != "" && !slices.Contains([]string{"healthy", "degraded", "not_applicable"}, cache.ControllerState) {
				return fmt.Errorf("cache %q has invalid controller state %q", cache.ID, cache.ControllerState)
			}
			if !slices.Contains([]string{"closed", "open", "not_applicable", "unknown"}, cache.CircuitState) {
				return fmt.Errorf("cache %q has invalid circuit state %q", cache.ID, cache.CircuitState)
			}
			if cache.Family != "" {
				if _, known := monitorValues("cache_family")[cache.Family]; !known {
					return fmt.Errorf("cache %q has invalid family", cache.ID)
				}
			}
			if cache.Representation != "" {
				if _, known := monitorValues("representation")[cache.Representation]; !known {
					return fmt.Errorf("cache %q has invalid representation", cache.ID)
				}
			}
			if !validAvailability(cache.TrafficAvailability) || !validAvailability(cache.TrafficBytesAvailability) || !validAvailability(cache.FillAvailability) || !validAvailability(cache.InventoryAvailability) || !validAvailability(cache.DeletionAvailability) || !validAvailability(cache.ReconciliationAgeAvailability) || !validAvailability(cache.ReconciliationLagAvailability) {
				return fmt.Errorf("cache %q has invalid field availability", cache.ID)
			}
			if cache.DeletionPendingBytes > cache.UsedBytes || cache.PinnedBytes > cache.UsedBytes || cache.ReclaimPendingBytes > cache.UsedBytes {
				return fmt.Errorf("cache %q has inconsistent capacity accounting", cache.ID)
			}
			if cache.Availability == AvailabilityExact && cache.Enabled {
				expectedAvailable := uint64(0)
				if cache.UsedBytes <= cache.EffectiveBytes && cache.ReservedBytes < cache.EffectiveBytes-cache.UsedBytes {
					expectedAvailable = cache.EffectiveBytes - cache.UsedBytes - cache.ReservedBytes
				}
				if cache.AvailableBytes != expectedAvailable {
					return fmt.Errorf("cache %q has inconsistent available capacity", cache.ID)
				}
			} else if cache.AvailableBytes > cache.EffectiveBytes {
				return fmt.Errorf("cache %q has inconsistent capacity accounting", cache.ID)
			}
		}
		if component.WAL != nil {
			if !validAvailability(component.WAL.Availability) {
				return fmt.Errorf("component %q WAL has invalid availability %q", component.Component, component.WAL.Availability)
			}
			if component.WAL.Availability != AvailabilityUnavailable && (component.WAL.Target == "" || component.WAL.Durability == "") {
				return fmt.Errorf("component %q available WAL requires target and durability", component.Component)
			}
			if component.WAL.Target != "" {
				if _, known := monitorValues("wal_target")[component.WAL.Target]; !known {
					return fmt.Errorf("component %q WAL target %q is invalid", component.Component, component.WAL.Target)
				}
			}
			if component.WAL.Durability != "" {
				if _, known := monitorValues("acknowledgement")[component.WAL.Durability]; !known {
					return fmt.Errorf("component %q WAL durability %q is invalid", component.Component, component.WAL.Durability)
				}
			}
			walDurability := map[string]string{
				"inherited": "inherited", "local": "local-process", "memory": "local-process",
				"s3": "shared-remote", "rados": "shared-remote", "test": "test",
				"unsupported-azure": "unsupported", "unsupported-gcs": "unsupported",
			}
			if component.WAL.Target != "" && walDurability[component.WAL.Target] != component.WAL.Durability {
				return fmt.Errorf("component %q WAL target and durability are incompatible", component.Component)
			}
			if component.WAL.ThrottleReason != "" {
				if _, known := monitorValues("throttle")[component.WAL.ThrottleReason]; !known {
					return fmt.Errorf("component %q WAL throttle reason %q is invalid", component.Component, component.WAL.ThrottleReason)
				}
			}
		}
	}
	return nil
}

func validateMetrics(metrics []Metric) error {
	seen := make(map[string]struct{}, len(metrics))
	for _, metric := range metrics {
		if err := validateMonitorName("metric", metric.Name); err != nil {
			return err
		}
		if err := validateMonitorName("unit", metric.Unit); err != nil {
			return err
		}
		spec, known := newMonitorMetricSpecs()[metric.Name]
		if !known {
			return fmt.Errorf("metric %q is not in monitor schema v%d", metric.Name, MonitorSchemaVersion)
		}
		if metric.Kind != spec.kind || metric.Unit != spec.unit {
			return fmt.Errorf("metric %q has kind/unit %q/%q, want %q/%q", metric.Name, metric.Kind, metric.Unit, spec.kind, spec.unit)
		}
		if !validAvailability(metric.Availability) {
			return fmt.Errorf("metric %q has invalid availability %q", metric.Name, metric.Availability)
		}
		if len(metric.Labels) > MaxMonitorLabels {
			return fmt.Errorf("metric %q labels exceed limit %d", metric.Name, MaxMonitorLabels)
		}
		labels := append([]Label(nil), metric.Labels...)
		sort.Slice(labels, func(left, right int) bool { return labels[left].Name < labels[right].Name })
		identity := metric.Name
		for index, label := range labels {
			if err := validateMonitorName("label", label.Name); err != nil {
				return err
			}
			if index > 0 && labels[index-1].Name == label.Name {
				return fmt.Errorf("metric %q has duplicate label %q", metric.Name, label.Name)
			}
			if label.Value == "" || len(label.Value) > MaxMonitorStringLength || strings.ContainsAny(label.Value, "\r\n\x00") {
				return fmt.Errorf("metric %q has invalid label %q", metric.Name, label.Name)
			}
			allowed := monitorValues(label.Name)
			known := allowed != nil
			if !known {
				return fmt.Errorf("metric %q uses unbounded label %q", metric.Name, label.Name)
			}
			if _, known = allowed[label.Value]; !known {
				return fmt.Errorf("metric %q label %q has unbounded value %q", metric.Name, label.Name, label.Value)
			}
			if !slices.Contains(spec.labels, label.Name) {
				return fmt.Errorf("metric %q does not allow label %q", metric.Name, label.Name)
			}
			identity += "\x00" + label.Name + "=" + label.Value
		}
		for _, required := range spec.required {
			if !slices.ContainsFunc(labels, func(label Label) bool { return label.Name == required }) {
				return fmt.Errorf("metric %q requires label %q", metric.Name, required)
			}
		}
		operation := labelValue(labels, "operation")
		role := labelValue(labels, "role")
		if operation != "" && role != "" && !validOperationRole(operation, role) {
			return fmt.Errorf("metric %q has unsupported operation/role pair %q/%q", metric.Name, operation, role)
		}
		if _, exists := seen[identity]; exists {
			return fmt.Errorf("duplicate metric identity %q", metric.Name)
		}
		seen[identity] = struct{}{}
		switch metric.Kind {
		case MetricGauge, MetricCounter:
			if len(metric.BucketUpper) != 0 || len(metric.BucketCounts) != 0 || metric.Count != 0 || metric.Sum != 0 || metric.Maximum != 0 {
				return fmt.Errorf("metric %q has histogram fields for kind %q", metric.Name, metric.Kind)
			}
		case MetricHistogram:
			if metric.Value != 0 {
				return fmt.Errorf("metric %q has scalar value for histogram kind", metric.Name)
			}
			if metric.Count == 0 && (metric.Sum != 0 || metric.Maximum != 0) || metric.Maximum > metric.Sum {
				return fmt.Errorf("metric %q histogram aggregates are inconsistent", metric.Name)
			}
			if len(metric.BucketUpper) == 0 || len(metric.BucketUpper) != len(metric.BucketCounts) {
				return fmt.Errorf("metric %q has invalid histogram buckets", metric.Name)
			}
			if len(metric.BucketUpper) > MaxHistogramBuckets {
				return fmt.Errorf("metric %q histogram buckets exceed limit %d", metric.Name, MaxHistogramBuckets)
			}
			var previousUpper uint64
			var previousCount uint64
			for index := range metric.BucketUpper {
				if index > 0 && metric.BucketUpper[index] <= previousUpper {
					return fmt.Errorf("metric %q histogram bounds are not increasing", metric.Name)
				}
				if index > 0 && metric.BucketCounts[index] < previousCount {
					return fmt.Errorf("metric %q histogram counts are not cumulative", metric.Name)
				}
				previousUpper = metric.BucketUpper[index]
				previousCount = metric.BucketCounts[index]
			}
			if metric.BucketCounts[len(metric.BucketCounts)-1] != metric.Count {
				return fmt.Errorf("metric %q histogram terminal bucket does not match count", metric.Name)
			}
			if metric.Availability == AvailabilityUnavailable && (!slices.Equal(metric.BucketUpper, []uint64{math.MaxUint64}) || metric.Count != 0 || metric.Sum != 0 || metric.Maximum != 0 || !slices.Equal(metric.BucketCounts, []uint64{0})) {
				return fmt.Errorf("metric %q unavailable histogram must use an empty sentinel", metric.Name)
			}
			validBounds := metric.Availability == AvailabilityUnavailable && slices.Equal(metric.BucketUpper, []uint64{math.MaxUint64})
			for _, bounds := range spec.bounds {
				validBounds = validBounds || slices.Equal(metric.BucketUpper, bounds)
			}
			if !validBounds {
				return fmt.Errorf("metric %q histogram bounds are not defined by monitor schema v%d", metric.Name, MonitorSchemaVersion)
			}
		default:
			return fmt.Errorf("metric %q has invalid kind %q", metric.Name, metric.Kind)
		}
	}
	return nil
}

func labelValue(labels []Label, name string) string {
	for _, label := range labels {
		if label.Name == name {
			return label.Value
		}
	}
	return ""
}

func validOperationRole(operation, role string) bool {
	roles := map[string][]string{
		"backup":            {"source", "repository", "database"},
		"restore":           {"repository", "source"},
		"check":             {"repository", "database", "scratch"},
		"legacy_import":     {"source", "database", "wal", "coordination", "rpc"},
		"forget":            {"database"},
		"prune":             {"repository", "database"},
		"replicate":         {"source", "repository"},
		"cache_fill":        {"cache", "coordination"},
		"cache_evict":       {"cache", "coordination"},
		"placement":         {"repository", "database"},
		"export":            {"repository", "source", "database"},
		"analytics":         {"repository", "database"},
		"maintenance":       {"repository", "database", "scratch"},
		"gdpr":              {"repository", "database"},
		"staging_reconcile": {"repository", "database", "coordination"},
		"key_management":    {"broker", "coordination", "repository"},
		"compaction":        {"database"},
		"recovery":          {"database", "wal", "coordination"},
	}
	return slices.Contains(roles[operation], role)
}

func newMonitorMetricSpecs() map[string]metricSpec {
	specs := make(map[string]metricSpec)
	add := func(name string, kind MetricKind, unit string, labels, required []string, bounds ...[]uint64) {
		specs[name] = metricSpec{kind: kind, unit: unit, labels: labels, required: required, bounds: bounds}
	}
	for _, name := range []string{"engine_write_batches", "engine_write_operations", "engine_backpressure_events", "engine_immutable_memtable_flushes", "engine_l0_stalls_sst_count", "engine_l0_stalls_ssts_per_key", "engine_get_keys", "engine_filter_point_positives", "engine_filter_point_negatives", "engine_filter_point_false_positives", "engine_multi_get_calls", "engine_multi_get_input_keys", "engine_multi_get_unique_keys", "engine_multi_get_sst_visits", "engine_multi_get_candidate_keys", "engine_multi_get_needed_blocks", "engine_multi_get_coalesced_reads", "engine_multi_get_projected_reads_gap_8", "engine_multi_get_projected_reads_gap_32", "engine_multi_get_projected_reads_gap_128", "cache_hits", "cache_misses", "cache_origin_reads", "cache_origin_reads_avoided", "cache_corruptions", "cache_timeouts", "cache_failures", "cache_bypasses", "cache_admissions", "cache_admission_rejections", "cache_admission_rejections_reservation", "cache_admission_rejections_background_budget", "cache_admission_rejections_background_task", "cache_capacity_evictions", "cache_idle_evictions", "cache_absolute_evictions", "cache_corruption_evictions", "cache_read_latency_count", "cache_write_latency_count", "monitor_export_failures", "monitor_export_dropped", "runtime_gc_cycles"} {
		add(name, MetricCounter, "operations", nil, nil)
	}
	for _, name := range []string{"runtime_gc_pause_cpu", "cache_read_latency_total", "cache_write_latency_total", "process_cpu_user", "process_cpu_system"} {
		add(name, MetricCounter, "microseconds", nil, nil)
	}
	for _, name := range []string{"engine_memtable_write_bytes", "engine_wal_flush_bytes", "engine_l0_flush_bytes", "engine_compacted_bytes", "engine_multi_get_needed_block_bytes", "engine_multi_get_coalesced_read_bytes", "engine_multi_get_projected_read_bytes_gap_8", "engine_multi_get_projected_read_bytes_gap_32", "engine_multi_get_projected_read_bytes_gap_128", "process_read_bytes", "process_write_bytes"} {
		add(name, MetricCounter, "bytes", nil, nil)
	}
	add("engine_compacted_ssts", MetricCounter, "objects", nil, nil)
	for _, name := range []string{"engine_memtable_bytes", "engine_max_unflushed_bytes", "engine_l0_sst_size_bytes", "engine_block_cache_bytes", "engine_meta_cache_bytes", "runtime_heap_alloc_bytes", "runtime_heap_inuse_bytes", "runtime_heap_sys_bytes", "process_rss_bytes"} {
		add(name, MetricGauge, "bytes", nil, nil)
	}
	for _, name := range []string{"engine_l0_sst_objects", "engine_sst_objects", "engine_sorted_runs"} {
		add(name, MetricGauge, "objects", nil, nil)
	}
	for _, name := range []string{"engine_running_compactions", "broker_active_sessions", "broker_active_leases", "runtime_goroutines", "process_threads"} {
		add(name, MetricGauge, "operations", nil, nil)
	}
	add("monitor_export_pending", MetricGauge, "operations", nil, nil)
	add("monitor_export_capacity", MetricGauge, "operations", nil, nil)
	add("monitor_export_in_flight", MetricGauge, "operations", nil, nil)
	add("monitor_export_oldest_age", MetricGauge, "microseconds", nil, nil)
	add("engine_flush_interval", MetricGauge, "milliseconds", nil, nil)
	add("broker_locked", MetricGauge, "state", nil, nil)
	for _, name := range []string{"admission_wait_latency", "admission_lock_hold_latency", "fence_check_latency", "write_batch_request_latency", "begin_request_latency", "commit_request_latency", "rollback_request_latency", "transaction_begin_latency", "transaction_map_lock_wait_latency", "transaction_slot_lock_wait_latency", "engine_submit_latency", "durable_wait_latency", "finalization_latency"} {
		add(name, MetricHistogram, "microseconds", []string{"outcome"}, nil, vaulticLatencyBounds())
	}
	for _, name := range []string{"engine_backpressure_latency", "engine_batch_queue_latency", "engine_batch_service_latency"} {
		add(name, MetricHistogram, "microseconds", []string{"outcome"}, nil, slateDBLatencyBounds())
	}
	for _, operation := range []string{"put", "multipart_init", "multipart_part", "multipart_complete", "multipart_abort", "get", "head", "get_body", "get_ranges", "delete", "list", "list_with_offset", "list_with_delimiter", "copy", "rename"} {
		add("object_"+operation+"_latency", MetricHistogram, "microseconds", []string{"role", "outcome"}, []string{"role"}, vaulticLatencyBounds())
		add("object_"+operation+"_bytes", MetricCounter, "bytes", []string{"role"}, []string{"role"})
	}
	add("operation_started", MetricCounter, "operations", []string{"operation"}, []string{"operation"})
	add("operation_completed", MetricCounter, "operations", []string{"operation", "outcome"}, []string{"operation", "outcome"})
	add("operation_active", MetricGauge, "operations", []string{"operation"}, []string{"operation"})
	add("operation_processed_bytes", MetricCounter, "bytes", []string{"operation", "role"}, []string{"operation", "role"})
	add("wait_attempts", MetricCounter, "operations", []string{"operation", "role", "throttle"}, []string{"operation", "role", "throttle"})
	add("wait_contentions", MetricCounter, "operations", []string{"operation", "role", "throttle"}, []string{"operation", "role", "throttle"})
	add("wait_completed", MetricCounter, "operations", []string{"operation", "role", "throttle", "outcome"}, []string{"operation", "role", "throttle", "outcome"})
	add("wait_active", MetricGauge, "operations", []string{"operation", "role", "throttle"}, []string{"operation", "role", "throttle"})
	add("wait_oldest_age", MetricGauge, "microseconds", []string{"operation", "role", "throttle"}, []string{"operation", "role", "throttle"})
	add("wait_duration", MetricHistogram, "microseconds", []string{"operation", "role", "throttle", "outcome"}, []string{"operation", "role", "throttle", "outcome"}, vaulticLatencyBounds())
	add("legacy_import_stage_count", MetricCounter, "operations", []string{"stage"}, []string{"stage"})
	add("legacy_import_stage_time", MetricCounter, "microseconds", []string{"stage"}, []string{"stage"})
	for _, name := range []string{"legacy_import_stage_p50", "legacy_import_stage_p95", "legacy_import_stage_p99"} {
		add(name, MetricGauge, "microseconds", []string{"stage"}, []string{"stage"})
	}
	add("legacy_import_events", MetricCounter, "operations", []string{"statistic"}, []string{"statistic"})
	add("legacy_import_processed_bytes", MetricCounter, "bytes", []string{"statistic"}, []string{"statistic"})
	add("legacy_import_filter_bytes", MetricGauge, "bytes", nil, nil)
	add("legacy_import_filter_layers", MetricGauge, "objects", nil, nil)
	add("legacy_import_filter_fallback", MetricGauge, "state", nil, nil)
	add("dependency_requests", MetricCounter, "operations", []string{"operation", "role", "outcome"}, []string{"operation", "role", "outcome"})
	add("dependency_bytes", MetricCounter, "bytes", []string{"operation", "role", "outcome"}, []string{"operation", "role", "outcome"})
	add("dependency_latency", MetricHistogram, "microseconds", []string{"operation", "role", "outcome"}, []string{"operation", "role", "outcome"}, vaulticLatencyBounds())
	return specs
}

func validateMonitorName(kind, value string) error {
	if !monitorNamePattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a bounded schema name", kind, value)
	}
	return nil
}

func validConfiguredID(value string) bool {
	if value == "" || len(value) > MaxMonitorStringLength || strings.ContainsAny(value, "\r\n\x00") {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') && !(character >= '0' && character <= '9') && !strings.ContainsRune("._-", character) {
			return false
		}
	}
	return true
}

func validOpaqueID(value string) bool {
	return validConfiguredID(value)
}

func validAvailability(value Availability) bool {
	switch value {
	case AvailabilityExact, AvailabilityEstimated, AvailabilityStale, AvailabilityUnavailable, AvailabilityNotApplicable:
		return true
	default:
		return false
	}
}

func values(entries ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		result[entry] = struct{}{}
	}
	return result
}
