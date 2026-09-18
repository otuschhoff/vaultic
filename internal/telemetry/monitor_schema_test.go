package telemetry

import (
	"strings"
	"testing"
	"time"
)

func validMonitorSnapshot() MonitorSnapshot {
	return NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: "process-1", CapturedUnixMS: 1000,
		Availability: AvailabilityExact,
		Metrics: []Metric{{
			Name: "durable_wait_latency", Kind: MetricHistogram, Unit: "microseconds",
			Availability: AvailabilityExact, Labels: []Label{{Name: "outcome", Value: "success"}},
			Count: 2, Sum: 12, Maximum: 8, BucketUpper: vaulticLatencyBounds(), BucketCounts: []uint64{2, 2, 2, 2, 2, 2, 2, 2},
		}},
		Queues: []QueueSnapshot{{Name: "batch_write", Availability: AvailabilityExact, CapacityAvailability: AvailabilityExact, Depth: 1, Capacity: 8}},
		Operations: []ActiveOperation{{
			ID: "generated-1", Class: "legacy_import", Phase: "reduce", StartedUnixMS: 1, UpdatedUnixMS: 2,
		}},
	})
}

func TestMonitorSnapshotValidationAcceptsBoundedSchema(t *testing.T) {
	if err := validMonitorSnapshot().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationRejectsUnboundedAndMalformedValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*MonitorSnapshot)
		want string
	}{
		{name: "version", edit: func(snapshot *MonitorSnapshot) { snapshot.SchemaVersion++ }, want: "unsupported"},
		{name: "unsafe-process-start-id", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].ProcessStartID = "process\\tag"
		}, want: "invalid process start ID"},
		{name: "secret-label-name", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Name = "access-token"
		}, want: "bounded schema name"},
		{name: "label-newline", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Value = "secret\nvalue"
		}, want: "invalid label"},
		{name: "unknown-label", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Name = "snapshot"
		}, want: "unbounded label"},
		{name: "unbounded-label-value", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Value = "snapshot-1234"
		}, want: "unbounded value"},
		{name: "histogram-shape", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].BucketCounts = []uint64{1}
		}, want: "invalid histogram"},
		{name: "histogram-terminal", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Count = 3
		}, want: "terminal bucket"},
		{name: "histogram-bucket-limit", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].BucketUpper = make([]uint64, MaxHistogramBuckets+1)
			snapshot.Components[0].Metrics[0].BucketCounts = make([]uint64, MaxHistogramBuckets+1)
		}, want: "buckets exceed"},
		{name: "unknown-metric", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Name = "custom_metric"
		}, want: "not in monitor schema"},
		{name: "changed-buckets", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].BucketUpper[0]++
		}, want: "not defined by monitor schema"},
		{name: "queue-capacity", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Queues[0].Depth = 9
		}, want: "depth exceeds"},
		{name: "queue-backpressure", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Queues[0].Backpressure = "custom_reason"
		}, want: "unbounded backpressure"},
		{name: "unknown-queue", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Queues[0].Name = "per_repository_value"
		}, want: "not in monitor schema"},
		{name: "operation-limit", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations = make([]ActiveOperation, MaxMonitorOperations+1)
		}, want: "operations exceed"},
		{name: "unsafe-operation-id", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations[0].ID = "/repository/private/path"
		}, want: "invalid ID"},
		{name: "future-operation", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations[0].UpdatedUnixMS = 1001
		}, want: "invalid timestamps"},
		{name: "unbounded-operation-class", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations[0].Class = "custom_user_value"
		}, want: "unbounded class"},
		{name: "unbounded-operation-phase", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations[0].Phase = "custom_phase"
		}, want: "unbounded phase"},
		{name: "cache-limit", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Caches = make([]CacheSnapshot, MaxMonitorCaches+1)
		}, want: "caches exceed"},
		{name: "duplicate-cache", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Caches = []CacheSnapshot{
				{ID: "slatedb", Availability: AvailabilityExact, CircuitState: "closed", TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable, InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable},
				{ID: "slatedb", Availability: AvailabilityExact, CircuitState: "closed", TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable, InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable},
			}
		}, want: "duplicate cache"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := validMonitorSnapshot()
			test.edit(&snapshot)
			if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestMonitorSnapshotValidationRejectsUnsupportedOperationRole(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics = []Metric{{
		Name: "operation_processed_bytes", Kind: MetricCounter, Unit: "bytes", Availability: AvailabilityExact,
		Labels: []Label{{Name: "operation", Value: "backup"}, {Name: "role", Value: "broker"}},
	}}
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported operation/role") {
		t.Fatalf("operation/role error = %v", err)
	}
}

func TestMonitorSnapshotValidationAcceptsCheckerScratchRole(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics = []Metric{{
		Name: "operation_processed_bytes", Kind: MetricCounter, Unit: "bytes", Availability: AvailabilityExact,
		Labels: []Label{{Name: "operation", Value: "check"}, {Name: "role", Value: "scratch"}},
	}}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationAcceptsExportDatabaseRole(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics = []Metric{{
		Name: "operation_processed_bytes", Kind: MetricCounter, Unit: "bytes", Availability: AvailabilityExact,
		Labels: []Label{{Name: "operation", Value: "export"}, {Name: "role", Value: "database"}},
	}}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationAcceptsImportRPCAndQueues(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Queues = []QueueSnapshot{
		{Name: "legacy_import_ingest", Availability: AvailabilityExact, CapacityAvailability: AvailabilityUnavailable},
		{Name: "legacy_import_reduce", Availability: AvailabilityExact, CapacityAvailability: AvailabilityUnavailable},
	}
	snapshot.Components[0].Metrics = []Metric{{
		Name: "dependency_requests", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact,
		Labels: []Label{{Name: "operation", Value: "legacy_import"}, {Name: "role", Value: "rpc"}, {Name: "outcome", Value: "success"}},
	}}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationIgnoresNonExactQueueCapacity(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Queues[0].Depth = 10
	snapshot.Components[0].Queues[0].Capacity = 1
	snapshot.Components[0].Queues[0].CapacityAvailability = AvailabilityUnavailable
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationRejectsInvalidReconciliationTimestamp(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Storage = []StorageSnapshot{{
		BackendID: "primary", Role: "repository", Availability: AvailabilityExact,
		ObjectCountAvailability: AvailabilityUnavailable, PayloadAvailability: AvailabilityUnavailable,
		PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityExact,
	}}
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "reconciliation timestamp") {
		t.Fatalf("reconciliation timestamp error = %v", err)
	}
}

func TestMonitorSnapshotValidationAcceptsClassedOperationOverflow(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].OperationOverflow = []OperationOverflowSnapshot{{Class: "restore", Count: 2}}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationRejectsDuplicateMetricIdentity(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics = append(snapshot.Components[0].Metrics, snapshot.Components[0].Metrics[0])
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate metric identity") {
		t.Fatalf("validation error = %v", err)
	}
}

func TestMonitorSnapshotValidationRequiresMetricIdentityLabels(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics = []Metric{{
		Name: "object_put_bytes", Kind: MetricCounter, Unit: "bytes", Availability: AvailabilityExact,
	}}
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "requires label") {
		t.Fatalf("validation error = %v", err)
	}
}

func TestMonitorSnapshotValidationRejectsInvalidKindFieldsAndUnavailableHistogram(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics[0].Value = 1
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "scalar value") {
		t.Fatalf("histogram scalar error = %v", err)
	}
	snapshot = validMonitorSnapshot()
	metric := &snapshot.Components[0].Metrics[0]
	metric.Availability = AvailabilityUnavailable
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "empty sentinel") {
		t.Fatalf("unavailable histogram error = %v", err)
	}
}

func TestMonitorSnapshotValidationRequiresAvailableWALIdentity(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].WAL = &WALSnapshot{Availability: AvailabilityExact}
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "requires target and durability") {
		t.Fatalf("WAL identity error = %v", err)
	}
}

func TestMonitorSnapshotValidationRejectsContradictoryWALGuarantee(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].WAL = &WALSnapshot{
		Target: "memory", Durability: "shared-remote", Availability: AvailabilityExact,
	}
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("WAL guarantee error = %v", err)
	}
}

func TestMonitorSnapshotValidationRejectsInconsistentCacheAccounting(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Caches = []CacheSnapshot{{
		ID: "slatedb", Availability: AvailabilityExact, Enabled: true, EffectiveBytes: 10, AvailableBytes: 11,
		ControllerState: "healthy", CircuitState: "closed", Family: "aggregate", Representation: "block",
		TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable,
		InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable,
	}}
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "available capacity") {
		t.Fatalf("cache accounting error = %v", err)
	}
	snapshot.Components[0].Caches[0].AvailableBytes = 0
	snapshot.Components[0].Caches[0].UsedBytes = 10
	snapshot.Components[0].Caches[0].DeletionPendingBytes = 11
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "inconsistent capacity") {
		t.Fatalf("cache deletion accounting error = %v", err)
	}
}

func TestMonitorSnapshotValidationUsesFrozenInventoryDimensionsAsIdentity(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Storage = []StorageSnapshot{
		{BackendID: "primary", Role: "repository", ObjectClass: "pack", PlacementState: "primary", Representation: "encrypted_pack", Availability: AvailabilityExact, ObjectCountAvailability: AvailabilityUnavailable, PayloadAvailability: AvailabilityUnavailable, PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityUnavailable},
		{BackendID: "primary", Role: "repository", ObjectClass: "index", PlacementState: "primary", Representation: "metadata", Availability: AvailabilityExact, ObjectCountAvailability: AvailabilityUnavailable, PayloadAvailability: AvailabilityUnavailable, PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityUnavailable},
	}
	snapshot.Components[0].Caches = []CacheSnapshot{
		{ID: "shared", Family: "memory", Representation: "block", Availability: AvailabilityExact, CircuitState: "closed", TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable, InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable},
		{ID: "shared", Family: "disk", Representation: "block", Availability: AvailabilityExact, CircuitState: "closed", TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable, InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable},
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationRequiresExactAvailableCapacity(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Caches = []CacheSnapshot{{
		ID: "slatedb", Availability: AvailabilityExact, Enabled: true, CircuitState: "closed",
		EffectiveBytes: 100, UsedBytes: 40, ReservedBytes: 10, AvailableBytes: 49,
		TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable,
		InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable,
	}}
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "available capacity") {
		t.Fatalf("cache available capacity error = %v", err)
	}
}

func TestMonitorSnapshotValidationAcceptsFrozenInventoryDimensions(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Storage = []StorageSnapshot{{
		BackendID: "primary", Role: "repository", Availability: AvailabilityExact,
		ObjectClass: "pack", PlacementState: "primary", Representation: "encrypted_pack",
		ObjectCountAvailability: AvailabilityUnavailable, PayloadAvailability: AvailabilityUnavailable,
		PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityUnavailable,
	}}
	snapshot.Components[0].Caches = []CacheSnapshot{{
		ID: "slatedb", Availability: AvailabilityExact, Enabled: true,
		Family: "slatedb", Representation: "block", ControllerState: "healthy", CircuitState: "closed",
		TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable,
		InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable,
	}}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}
