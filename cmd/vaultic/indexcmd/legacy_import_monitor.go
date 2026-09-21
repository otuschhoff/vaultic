package indexcmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/legacyimport"
	"github.com/otuschhoff/vaultic/internal/telemetry"
)

type legacyImportMonitorSource struct {
	mu       sync.RWMutex
	client   legacyImportMonitorClient
	last     telemetry.ComponentSnapshot
	haveLast bool
}

type legacyImportMonitorClient interface {
	WriterStatus(context.Context) (daemon.WriterStatus, error)
	ReadCacheStatus(context.Context) (daemon.ReadCacheStatus, error)
	WALInfo() daemon.WALInfo
}

func (source *legacyImportMonitorSource) SetClient(client legacyImportMonitorClient) {
	source.mu.Lock()
	source.client = client
	source.mu.Unlock()
}

func (source *legacyImportMonitorSource) Snapshot(ctx context.Context, scheduler *legacyimport.SchedulerTelemetry, now time.Time) (telemetry.MonitorSnapshot, error) {
	source.mu.RLock()
	client := source.client
	last := source.last
	haveLast := source.haveLast
	source.mu.RUnlock()
	components := []telemetry.ComponentSnapshot{scheduler.Component(now)}
	if client == nil {
		if haveLast {
			last.Availability = telemetry.AvailabilityStale
			last.Stale = true
			components = append(components, last)
		} else {
			components = append(components, unavailableLegacyImportMonitorComponent("vaulticdb", now))
		}
		return telemetry.NewMonitorSnapshot(now, components...), errors.New("VaulticDB client is unavailable")
	}
	writer, writerErr := client.WriterStatus(ctx)
	if writerErr != nil {
		components = append(components, unavailableLegacyImportMonitorComponent("vaulticdb", now))
		return telemetry.NewMonitorSnapshot(now, components...), fmt.Errorf("collect VaulticDB writer status: %w", writerErr)
	}
	cache, cacheErr := client.ReadCacheStatus(ctx)
	component := telemetry.VaulticDBComponent(writer, cache, client.WALInfo())
	if component.CapturedUnixMS < now.UnixMilli() && component.Availability != telemetry.AvailabilityUnavailable {
		component.Availability = telemetry.AvailabilityStale
		component.Stale = true
	}
	if cacheErr != nil {
		component.Caches = []telemetry.CacheSnapshot{{
			ID: "slatedb", Availability: telemetry.AvailabilityUnavailable, CircuitState: "unknown",
			TrafficAvailability: telemetry.AvailabilityUnavailable, TrafficBytesAvailability: telemetry.AvailabilityUnavailable,
			FillAvailability: telemetry.AvailabilityUnavailable, InventoryAvailability: telemetry.AvailabilityUnavailable,
			DeletionAvailability: telemetry.AvailabilityUnavailable, ReconciliationAgeAvailability: telemetry.AvailabilityUnavailable,
			ReconciliationLagAvailability: telemetry.AvailabilityUnavailable,
		}}
	}
	source.mu.Lock()
	source.last = component
	source.haveLast = true
	source.mu.Unlock()
	components = append(components, component)
	if cacheErr != nil {
		return telemetry.NewMonitorSnapshot(now, components...), fmt.Errorf("collect VaulticDB cache status: %w", cacheErr)
	}
	return telemetry.NewMonitorSnapshot(now, components...), nil
}

func unavailableLegacyImportMonitorComponent(name string, now time.Time) telemetry.ComponentSnapshot {
	return telemetry.ComponentSnapshot{
		Component: name, ProcessStartID: "unavailable", CapturedUnixMS: now.UnixMilli(),
		Availability: telemetry.AvailabilityUnavailable,
	}
}

type legacyImportMonitorExport struct {
	cancel  context.CancelFunc
	done    chan struct{}
	stop    sync.Once
	worker  *telemetry.AsyncExporter
	close   func() error
	timeout time.Duration
	collect func(context.Context, time.Time) (telemetry.MonitorSnapshot, error)
	onError func(error)
	now     func() time.Time
}

func startLegacyImportMonitorExport(ctx context.Context, options importMonitorExportOptions, scheduler *legacyimport.SchedulerTelemetry, source *legacyImportMonitorSource) (*legacyImportMonitorExport, error) {
	if options.URL == "" && options.JSONLPath == "" {
		return nil, nil
	}
	var exporter telemetry.SnapshotExporter
	var closeExporter func() error
	if options.JSONLPath != "" {
		localExporter, err := telemetry.NewJSONLExporter(options.JSONLPath)
		if err != nil {
			return nil, fmt.Errorf("configure legacy import monitor export: %w", err)
		}
		exporter = localExporter
		closeExporter = localExporter.Close
	} else {
		influxExporter, err := telemetry.NewInfluxExporter(telemetry.InfluxConfig{
			URL: options.URL, Org: options.Org, Bucket: options.Bucket, DeploymentID: options.DeploymentID,
			TokenFile: options.TokenFile, TokenEnv: options.TokenEnv, Timeout: options.Timeout, BatchLimit: options.BatchLimit,
		})
		if err != nil {
			return nil, fmt.Errorf("configure legacy import monitor export: %w", err)
		}
		exporter = influxExporter
	}
	worker := telemetry.NewAsyncExporterWithConfig(exporter, telemetry.AsyncExporterConfig{
		Capacity: options.Queue, RetryLimit: options.RetryLimit, RetryBackoff: options.RetryBackoff, Timeout: options.Timeout,
	})
	monitor := startLegacyImportMonitorLoop(ctx, options.Interval, options.Timeout, worker, func(collectCtx context.Context, now time.Time) (telemetry.MonitorSnapshot, error) {
		return source.Snapshot(collectCtx, scheduler, now)
	}, func(err error) {
		log.Printf("legacy import monitor export: %v", err)
	})
	monitor.close = closeExporter
	return monitor, nil
}

func startLegacyImportMonitorLoop(
	ctx context.Context,
	interval, timeout time.Duration,
	worker *telemetry.AsyncExporter,
	collect func(context.Context, time.Time) (telemetry.MonitorSnapshot, error),
	onError func(error),
) *legacyImportMonitorExport {
	loopCtx, cancel := context.WithCancel(ctx)
	monitor := &legacyImportMonitorExport{
		cancel: cancel, done: make(chan struct{}), worker: worker, timeout: timeout,
		collect: collect, onError: onError, now: time.Now,
	}
	go func() {
		defer close(monitor.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		monitor.submit(loopCtx)
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				monitor.submit(loopCtx)
			}
		}
	}()
	return monitor
}

func (monitor *legacyImportMonitorExport) submit(ctx context.Context) bool {
	collectCtx, cancel := context.WithTimeout(ctx, monitor.timeout)
	snapshot, err := monitor.collect(collectCtx, monitor.now())
	cancel()
	if err != nil && monitor.onError != nil {
		monitor.onError(err)
	}
	addLegacyImportExporterHealth(&snapshot, monitor.worker.Stats())
	if validationErr := snapshot.Validate(); validationErr != nil {
		if monitor.onError != nil {
			monitor.onError(fmt.Errorf("validate legacy import monitor snapshot: %w", validationErr))
		}
		return false
	}
	return monitor.worker.Submit(snapshot)
}

func addLegacyImportExporterHealth(snapshot *telemetry.MonitorSnapshot, stats telemetry.ExporterStats) {
	for index := range snapshot.Components {
		component := &snapshot.Components[index]
		if component.Component != "vaultic" {
			continue
		}
		component.Metrics = append(component.Metrics,
			telemetry.Metric{Name: "monitor_export_failures", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: stats.Failures},
			telemetry.Metric{Name: "monitor_export_dropped", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: stats.Dropped},
			telemetry.Metric{Name: "monitor_export_pending", Kind: telemetry.MetricGauge, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: uint64(max(stats.Pending, 0))},
			telemetry.Metric{Name: "monitor_export_capacity", Kind: telemetry.MetricGauge, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: uint64(max(stats.Capacity, 0))},
			telemetry.Metric{Name: "monitor_export_in_flight", Kind: telemetry.MetricGauge, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: boolUint64(stats.InFlight)},
			telemetry.Metric{Name: "monitor_export_oldest_age", Kind: telemetry.MetricGauge, Unit: "microseconds", Availability: telemetry.AvailabilityExact, Value: uint64(max(stats.OldestAge.Microseconds(), 0))},
		)
		return
	}
}

func boolUint64(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func (monitor *legacyImportMonitorExport) Close() {
	if monitor == nil {
		return
	}
	monitor.StopCollection()
	started := time.Now()
	finalSubmitted := monitor.submit(context.Background())
	remaining := max(monitor.timeout-time.Since(started), 0)
	drained := monitor.worker.CloseWithin(remaining)
	if monitor.close != nil {
		closeExporter := func() {
			if err := monitor.close(); err != nil {
				log.Printf("legacy import monitor export close: %v", err)
			}
		}
		if drained {
			closeExporter()
		} else {
			go func() {
				<-monitor.worker.Done()
				closeExporter()
			}()
		}
	}
	stats := monitor.worker.Stats()
	if !finalSubmitted || !drained || stats.Failures != 0 || stats.Dropped != 0 {
		log.Printf("legacy import monitor export completed: final_submitted=%t drained=%t failures=%d dropped=%d pending=%d", finalSubmitted, drained, stats.Failures, stats.Dropped, stats.Pending)
	}
}

func (monitor *legacyImportMonitorExport) StopCollection() {
	if monitor == nil {
		return
	}
	monitor.stop.Do(func() {
		monitor.cancel()
		<-monitor.done
	})
}
