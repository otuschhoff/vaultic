package indexcmd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/legacyimport"
	"github.com/otuschhoff/vaultic/internal/telemetry"
)

type recordingMonitorExporter struct {
	mu        sync.Mutex
	snapshots []telemetry.MonitorSnapshot
	exported  chan struct{}
}

type monitorStatusClient struct {
	writer    daemon.WriterStatus
	writerErr error
	cacheErr  error
}

type fixedImportStatsProvider struct{ stats daemon.LegacyImportStats }

func (provider fixedImportStatsProvider) LegacyImportStats() daemon.LegacyImportStats {
	return provider.stats
}

func (client monitorStatusClient) WriterStatus(context.Context) (daemon.WriterStatus, error) {
	return client.writer, client.writerErr
}

func (client monitorStatusClient) ReadCacheStatus(context.Context) (daemon.ReadCacheStatus, error) {
	return daemon.ReadCacheStatus{}, client.cacheErr
}

func (monitorStatusClient) WALInfo() daemon.WALInfo { return daemon.WALInfo{} }

type cancelBlockingMonitorExporter struct {
	started chan struct{}
}

func (exporter *cancelBlockingMonitorExporter) Export(ctx context.Context, _ telemetry.MonitorSnapshot) error {
	select {
	case exporter.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func (exporter *recordingMonitorExporter) Export(_ context.Context, snapshot telemetry.MonitorSnapshot) error {
	exporter.mu.Lock()
	exporter.snapshots = append(exporter.snapshots, snapshot)
	exporter.mu.Unlock()
	select {
	case exporter.exported <- struct{}{}:
	default:
	}
	return nil
}

func (exporter *recordingMonitorExporter) Snapshots() []telemetry.MonitorSnapshot {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return append([]telemetry.MonitorSnapshot(nil), exporter.snapshots...)
}

func TestLegacyImportMonitorExportsCompletedLifecycle(t *testing.T) {
	exporter := &recordingMonitorExporter{exported: make(chan struct{}, 2)}
	worker := telemetry.NewAsyncExporter(exporter, 2)
	scheduler := legacyimport.NewSchedulerTelemetry()
	scheduler.StartAction()
	monitor := startLegacyImportMonitorLoop(context.Background(), time.Hour, time.Second, worker,
		func(_ context.Context, now time.Time) (telemetry.MonitorSnapshot, error) {
			return telemetry.NewMonitorSnapshot(now, scheduler.Component(now)), nil
		}, func(err error) { t.Error(err) })
	select {
	case <-exporter.exported:
	case <-time.After(5 * time.Second):
		t.Fatal("initial import monitor snapshot was not exported")
	}
	scheduler.BeginHandoff()
	scheduler.FinishAction(legacyimport.Result{}, nil)
	monitor.Close()
	snapshots := exporter.Snapshots()
	if len(snapshots) < 2 {
		t.Fatalf("exported snapshots = %d, want at least initial and final", len(snapshots))
	}
	final := snapshots[len(snapshots)-1].Components[0]
	if len(final.Operations) != 0 {
		t.Fatalf("final snapshot retained active operation: %+v", final.Operations)
	}
	if value := monitorMetricValue(final.Metrics, "operation_completed"); value != 1 {
		t.Fatalf("completed operations = %d, want 1", value)
	}
}

func TestLegacyImportMonitorExportsDegradedSnapshot(t *testing.T) {
	exporter := &recordingMonitorExporter{exported: make(chan struct{}, 2)}
	worker := telemetry.NewAsyncExporter(exporter, 2)
	reported := make(chan error, 2)
	monitor := startLegacyImportMonitorLoop(context.Background(), time.Hour, time.Second, worker,
		func(_ context.Context, now time.Time) (telemetry.MonitorSnapshot, error) {
			component := legacyimport.NewSchedulerTelemetry().Component(now)
			return telemetry.NewMonitorSnapshot(now, component), errors.New("daemon status unavailable")
		}, func(err error) { reported <- err })
	monitor.Close()
	if len(exporter.Snapshots()) == 0 {
		t.Fatal("valid degraded snapshot was not exported")
	}
	select {
	case err := <-reported:
		if err == nil || err.Error() != "daemon status unavailable" {
			t.Fatalf("reported error = %v", err)
		}
	default:
		t.Fatal("collection degradation was not reported")
	}
}

func TestLegacyImportMonitorSourceReportsUnavailableAndStaleDaemon(t *testing.T) {
	scheduler := legacyimport.NewSchedulerTelemetry()
	now := time.Now()
	source := &legacyImportMonitorSource{}
	snapshot, err := source.Snapshot(context.Background(), scheduler, now)
	if err == nil || snapshot.Components[1].Availability != telemetry.AvailabilityUnavailable {
		t.Fatalf("unavailable daemon snapshot = %+v, error = %v", snapshot.Components, err)
	}
	if validationErr := snapshot.Validate(); validationErr != nil {
		t.Fatal(validationErr)
	}
	source.SetClient(monitorStatusClient{writer: daemon.WriterStatus{ProcessStartedUnixMS: 1, CapturedUnixMS: now.Add(-time.Second).UnixMilli()}})
	snapshot, err = source.Snapshot(context.Background(), scheduler, now)
	if err != nil || snapshot.Components[1].Availability != telemetry.AvailabilityStale || !snapshot.Components[1].Stale {
		t.Fatalf("stale daemon snapshot = %+v, error = %v", snapshot.Components[1], err)
	}
	source.SetClient(nil)
	snapshot, err = source.Snapshot(context.Background(), scheduler, now.Add(time.Second))
	if err == nil || snapshot.Components[1].Availability != telemetry.AvailabilityStale {
		t.Fatalf("cached daemon snapshot = %+v, error = %v", snapshot.Components[1], err)
	}
}

func TestLegacyImportMonitorSourceReportsUnavailableCache(t *testing.T) {
	now := time.Now()
	source := &legacyImportMonitorSource{client: monitorStatusClient{
		writer:   daemon.WriterStatus{ProcessStartedUnixMS: 1, CapturedUnixMS: now.UnixMilli()},
		cacheErr: errors.New("cache unavailable"),
	}}
	snapshot, err := source.Snapshot(context.Background(), legacyimport.NewSchedulerTelemetry(), now)
	if err == nil || len(snapshot.Components[1].Caches) != 1 || snapshot.Components[1].Caches[0].Availability != telemetry.AvailabilityUnavailable {
		t.Fatalf("cache failure snapshot = %+v, error = %v", snapshot.Components[1], err)
	}
	if validationErr := snapshot.Validate(); validationErr != nil {
		t.Fatal(validationErr)
	}
}

func TestLegacyImportMonitorSourceExportsBoundedImportStats(t *testing.T) {
	now := time.Now()
	source := &legacyImportMonitorSource{client: monitorStatusClient{
		writer: daemon.WriterStatus{ProcessStartedUnixMS: 1, CapturedUnixMS: now.UnixMilli()},
	}}
	source.SetStatsProvider(fixedImportStatsProvider{stats: daemon.LegacyImportStats{
		Operations: map[string]daemon.DurationDistribution{
			"reduce_prefetch": {Count: 2, Sum: 30 * time.Millisecond, P50: 10 * time.Millisecond, P95: 20 * time.Millisecond, P99: 20 * time.Millisecond},
		},
		ReducedBatches: 2, ReductionPlanReadRPCs: 2, ReductionPlanReadKeys: 24,
		PlanningTime: time.Second, FilterBytes: 1024, FilterLayers: 1, FilterInserts: 9,
	}})
	snapshot, err := source.Snapshot(context.Background(), legacyimport.NewSchedulerTelemetry(), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
	metrics := snapshot.Components[0].Metrics
	if len(metrics) > telemetry.MaxMonitorMetrics {
		t.Fatalf("metric count = %d, limit = %d", len(metrics), telemetry.MaxMonitorMetrics)
	}
	if value := labeledMonitorMetricValue(metrics, "legacy_import_stage_count", "stage", "reduce_prefetch"); value != 2 {
		t.Fatalf("reduce prefetch count = %d, want 2", value)
	}
	if value := labeledMonitorMetricValue(metrics, "legacy_import_events", "statistic", "reduction_plan_read_rpcs"); value != 2 {
		t.Fatalf("reduction plan RPCs = %d, want 2", value)
	}
}

func TestLegacyImportMonitorCloseIsBoundedByOneTimeout(t *testing.T) {
	exporter := &cancelBlockingMonitorExporter{started: make(chan struct{}, 1)}
	worker := telemetry.NewAsyncExporter(exporter, 1)
	monitor := startLegacyImportMonitorLoop(context.Background(), time.Hour, 50*time.Millisecond, worker,
		func(_ context.Context, now time.Time) (telemetry.MonitorSnapshot, error) {
			return telemetry.NewMonitorSnapshot(now, legacyimport.NewSchedulerTelemetry().Component(now)), nil
		}, func(error) {})
	select {
	case <-exporter.started:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked exporter did not start")
	}
	started := time.Now()
	monitor.Close()
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded close took %s", elapsed)
	}
}

func monitorMetricValue(metrics []telemetry.Metric, name string) uint64 {
	for _, metric := range metrics {
		if metric.Name == name {
			return metric.Value
		}
	}
	return 0
}

func labeledMonitorMetricValue(metrics []telemetry.Metric, name, labelName, labelValue string) uint64 {
	for _, metric := range metrics {
		if metric.Name != name {
			continue
		}
		for _, label := range metric.Labels {
			if label.Name == labelName && label.Value == labelValue {
				return metric.Value
			}
		}
	}
	return 0
}
