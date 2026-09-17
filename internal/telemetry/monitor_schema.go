package telemetry

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	MonitorSchemaVersion   = 1
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

var monitorLabelValues = map[string]map[string]struct{}{
	"component":      values("vaultic", "vaulticdb", "key_broker", "cache_coordinator"),
	"operation":      values("backup", "restore", "check", "legacy_import", "forget", "prune", "replicate", "cache_fill", "compaction", "recovery"),
	"outcome":        values("success", "failure", "cancellation", "timeout"),
	"role":           values("repository", "database", "wal", "coordination", "source", "scratch", "cache"),
	"representation": values("encrypted_pack", "encrypted_range", "compressed_container", "decoded_extent", "whole_file", "sst", "block", "metadata"),
	"throttle":       values("none", "concurrency", "bandwidth", "capacity", "backend_retry", "credential_renewal", "writer_fencing", "wal_flush", "wal_retention", "compaction", "durability", "shutdown"),
}

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
	Name             string       `json:"name"`
	Availability     Availability `json:"availability"`
	Depth            uint64       `json:"depth"`
	Capacity         uint64       `json:"capacity"`
	ActiveWorkers    uint64       `json:"active_workers"`
	Admitted         uint64       `json:"admitted"`
	Rejected         uint64       `json:"rejected"`
	OldestItemAgeUS  uint64       `json:"oldest_item_age_us"`
	Backpressure     string       `json:"backpressure,omitempty"`
	BackpressureTime uint64       `json:"backpressure_time_us,omitempty"`
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
	BackendID       string       `json:"backend_id"`
	Role            string       `json:"role"`
	Availability    Availability `json:"availability"`
	ObjectCount     uint64       `json:"object_count"`
	PayloadBytes    uint64       `json:"payload_bytes"`
	PhysicalBytes   uint64       `json:"physical_bytes,omitempty"`
	ReconciledAtMS  int64        `json:"reconciled_at_ms,omitempty"`
	Acknowledgement string       `json:"acknowledgement,omitempty"`
}

type CacheSnapshot struct {
	ID                  string       `json:"id"`
	Availability        Availability `json:"availability"`
	RequestedBytes      uint64       `json:"requested_bytes"`
	EffectiveBytes      uint64       `json:"effective_bytes"`
	UsedBytes           uint64       `json:"used_bytes"`
	ReservedBytes       uint64       `json:"reserved_bytes"`
	PinnedBytes         uint64       `json:"pinned_bytes"`
	StagingBytes        uint64       `json:"staging_bytes"`
	ReclaimPendingBytes uint64       `json:"reclaim_pending_bytes"`
	AvailableBytes      uint64       `json:"available_bytes"`
	Hits                uint64       `json:"hits"`
	Misses              uint64       `json:"misses"`
	OriginBytes         uint64       `json:"origin_bytes"`
	CacheBytes          uint64       `json:"cache_bytes"`
	OriginReadsAvoided  uint64       `json:"origin_reads_avoided"`
	InflightFills       uint64       `json:"inflight_fills"`
	ReconciliationAgeMS uint64       `json:"reconciliation_age_ms,omitempty"`
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
	Component          string            `json:"component"`
	ProcessStartID     string            `json:"process_start_id"`
	CapturedUnixMS     int64             `json:"captured_unix_ms"`
	Availability       Availability      `json:"availability"`
	Stale              bool              `json:"stale"`
	Metrics            []Metric          `json:"metrics,omitempty"`
	Queues             []QueueSnapshot   `json:"queues,omitempty"`
	Operations         []ActiveOperation `json:"operations,omitempty"`
	OperationOverflow  uint64            `json:"operation_overflow"`
	CardinalityDropped uint64            `json:"cardinality_dropped"`
	Storage            []StorageSnapshot `json:"storage,omitempty"`
	Caches             []CacheSnapshot   `json:"caches,omitempty"`
	WAL                *WALSnapshot      `json:"wal,omitempty"`
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
			if _, exists := seenQueues[queue.Name]; exists {
				return fmt.Errorf("component %q has duplicate queue %q", component.Component, queue.Name)
			}
			seenQueues[queue.Name] = struct{}{}
			if !validAvailability(queue.Availability) {
				return fmt.Errorf("queue %q has invalid availability %q", queue.Name, queue.Availability)
			}
			if queue.Capacity != 0 && queue.Depth > queue.Capacity {
				return fmt.Errorf("queue %q depth exceeds capacity", queue.Name)
			}
			if queue.Backpressure != "" {
				if err := validateMonitorName("backpressure", queue.Backpressure); err != nil {
					return err
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
			if _, known := monitorLabelValues["operation"][operation.Class]; !known {
				return fmt.Errorf("active operation %q has unbounded class %q", operation.ID, operation.Class)
			}
			if err := validateMonitorName("operation phase", operation.Phase); err != nil {
				return err
			}
			if operation.BlockingReason != "" {
				if err := validateMonitorName("blocking reason", operation.BlockingReason); err != nil {
					return err
				}
			}
			if operation.StartedUnixMS <= 0 || operation.UpdatedUnixMS < operation.StartedUnixMS {
				return fmt.Errorf("active operation %q has invalid timestamps", operation.ID)
			}
		}
		seenStorage := make(map[string]struct{}, len(component.Storage))
		for _, storage := range component.Storage {
			if !validConfiguredID(storage.BackendID) {
				return fmt.Errorf("storage backend ID %q is invalid", storage.BackendID)
			}
			if err := validateMonitorName("storage role", storage.Role); err != nil {
				return err
			}
			storageIdentity := storage.BackendID + "\x00" + storage.Role
			if _, exists := seenStorage[storageIdentity]; exists {
				return fmt.Errorf("component %q has duplicate storage identity", component.Component)
			}
			seenStorage[storageIdentity] = struct{}{}
			if !validAvailability(storage.Availability) {
				return fmt.Errorf("storage backend %q has invalid availability %q", storage.BackendID, storage.Availability)
			}
			if storage.Acknowledgement != "" && !monitorNamePattern.MatchString(storage.Acknowledgement) {
				return fmt.Errorf("storage backend %q has invalid acknowledgement", storage.BackendID)
			}
		}
		seenCaches := make(map[string]struct{}, len(component.Caches))
		for _, cache := range component.Caches {
			if !validConfiguredID(cache.ID) {
				return fmt.Errorf("cache ID %q is invalid", cache.ID)
			}
			if _, exists := seenCaches[cache.ID]; exists {
				return fmt.Errorf("component %q has duplicate cache %q", component.Component, cache.ID)
			}
			seenCaches[cache.ID] = struct{}{}
			if !validAvailability(cache.Availability) {
				return fmt.Errorf("cache %q has invalid availability %q", cache.ID, cache.Availability)
			}
		}
		if component.WAL != nil {
			if !validAvailability(component.WAL.Availability) {
				return fmt.Errorf("component %q WAL has invalid availability %q", component.Component, component.WAL.Availability)
			}
			for name, value := range map[string]string{"target": component.WAL.Target, "durability": component.WAL.Durability, "throttle reason": component.WAL.ThrottleReason} {
				if value != "" && !monitorNamePattern.MatchString(value) {
					return fmt.Errorf("component %q WAL %s %q is invalid", component.Component, name, value)
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
			allowed, known := monitorLabelValues[label.Name]
			if !known {
				return fmt.Errorf("metric %q uses unbounded label %q", metric.Name, label.Name)
			}
			if _, known = allowed[label.Value]; !known {
				return fmt.Errorf("metric %q label %q has unbounded value %q", metric.Name, label.Name, label.Value)
			}
			identity += "\x00" + label.Name + "=" + label.Value
		}
		if _, exists := seen[identity]; exists {
			return fmt.Errorf("duplicate metric identity %q", metric.Name)
		}
		seen[identity] = struct{}{}
		switch metric.Kind {
		case MetricGauge, MetricCounter:
			if len(metric.BucketUpper) != 0 || len(metric.BucketCounts) != 0 || metric.Count != 0 || metric.Sum != 0 {
				return fmt.Errorf("metric %q has histogram fields for kind %q", metric.Name, metric.Kind)
			}
		case MetricHistogram:
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
		default:
			return fmt.Errorf("metric %q has invalid kind %q", metric.Name, metric.Kind)
		}
	}
	return nil
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
