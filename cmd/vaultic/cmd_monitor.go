package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
)

var monitorProcessStarted = time.Now()

type monitorOptions struct {
	JSON      bool
	Component string
	Backend   string
	Cache     string
	Interval  time.Duration
	View      string
	Active    bool
	Reconcile bool
	Timeout   time.Duration
}

type monitorExportOptions struct {
	URL          string
	Org          string
	Bucket       string
	DeploymentID string
	TokenFile    string
	TokenEnv     string
	Interval     time.Duration
	Timeout      time.Duration
	Queue        int
	BatchLimit   int
	RetryLimit   int
	RetryBackoff time.Duration
}

func newMonitorCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{
		Use:               "monitor",
		Short:             "Inspect bounded operational monitoring snapshots",
		Args:              cobra.NoArgs,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
	}
	command.AddCommand(
		newMonitorSnapshotCommand("status", "Show aggregate component status", globalOptions),
		newMonitorSnapshotCommand("storage", "Show repository and database storage", globalOptions),
		newMonitorSnapshotCommand("operations", "Show active operations and queues", globalOptions),
		newMonitorSnapshotCommand("caches", "Show repository and database read caches", globalOptions),
		newMonitorWatchCommand(globalOptions),
		newMonitorExportCommand(globalOptions),
	)
	return command
}

func newMonitorSnapshotCommand(view, description string, globalOptions *global.Options) *cobra.Command {
	var options monitorOptions
	command := &cobra.Command{
		Use:               view,
		Short:             description,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			for _, filter := range []string{options.Component, options.Backend, options.Cache} {
				if err := validateMonitorFilter(filter); err != nil {
					return err
				}
			}
			snapshot, err := collectMonitorSnapshotWithTimeout(command.Context(), globalOptions, options.Reconcile, options.Timeout)
			if err != nil {
				return err
			}
			filterMonitorSnapshot(&snapshot, view, options)
			if err := snapshot.Validate(); err != nil {
				return err
			}
			if options.JSON || globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(snapshot))
				return nil
			}
			for _, line := range monitorLines(snapshot, view) {
				globalOptions.Term.Print(line)
			}
			return nil
		},
	}
	command.Flags().BoolVar(&options.JSON, "json", false, "emit the versioned monitoring snapshot as JSON")
	command.Flags().DurationVar(&options.Timeout, "collector-timeout", 10*time.Second, "maximum time for one snapshot collection")
	command.Flags().StringVar(&options.Component, "component", "", "show only a configured component")
	if view == "status" || view == "storage" {
		command.Flags().StringVar(&options.Backend, "backend", "", "show only a configured backend")
	}
	if view == "storage" {
		command.Flags().BoolVar(&options.Reconcile, "reconcile", false, "recompute logical repository storage from placement records")
	}
	if view == "operations" {
		command.Flags().BoolVar(&options.Active, "active", false, "show only active operations")
	}
	if view == "caches" {
		command.Flags().StringVar(&options.Cache, "cache", "", "show only a configured cache")
	}
	return command
}

func newMonitorWatchCommand(globalOptions *global.Options) *cobra.Command {
	options := monitorOptions{Interval: time.Second, View: "status", Timeout: 10 * time.Second}
	command := &cobra.Command{
		Use:               "watch",
		Short:             "Watch bounded operational monitoring snapshots",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			if !globalOptions.Term.OutputIsTerminal() || !globalOptions.Term.CanUpdateStatus() {
				return fmt.Errorf("monitor watch requires an interactive terminal")
			}
			return watchMonitor(command.Context(), globalOptions, options)
		},
	}
	command.Flags().DurationVar(&options.Interval, "interval", time.Second, "snapshot refresh interval")
	command.Flags().DurationVar(&options.Timeout, "collector-timeout", 10*time.Second, "maximum time for one snapshot collection")
	command.Flags().StringVar(&options.View, "view", "status", "view: overview, storage, operations, wal, caches, or latency")
	return command
}

func watchMonitor(ctx context.Context, globalOptions *global.Options, options monitorOptions) error {
	if options.Interval < 100*time.Millisecond || options.Interval > time.Minute {
		return fmt.Errorf("monitor interval must be between 100ms and 1m")
	}
	view := options.View
	if view == "overview" {
		view = "status"
	}
	switch view {
	case "status", "storage", "operations", "wal", "caches", "latency":
	default:
		return fmt.Errorf("unknown monitor view %q", options.View)
	}
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	defer globalOptions.Term.SetStatus(nil)
	var previous *telemetry.MonitorSnapshot
	for {
		snapshot, err := collectMonitorSnapshotWithTimeout(ctx, globalOptions, false, options.Timeout)
		if err != nil {
			return err
		}
		derived := monitorDerivedLines(previous, snapshot, view)
		unfiltered := snapshot
		previous = &unfiltered
		filterMonitorSnapshot(&snapshot, view, options)
		globalOptions.Term.SetStatus(append(monitorLines(snapshot, view), derived...))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func monitorDerivedLines(previous *telemetry.MonitorSnapshot, current telemetry.MonitorSnapshot, view string) []string {
	previousComponents := make(map[string]telemetry.ComponentSnapshot, len(current.Components))
	if previous != nil {
		for _, component := range previous.Components {
			previousComponents[component.Component] = component
		}
	}
	var lines []string
	for _, component := range current.Components {
		prior, sameProcess := previousComponents[component.Component]
		sameProcess = sameProcess && prior.ProcessStartID == component.ProcessStartID && component.ProcessStartID != "legacy" && component.ProcessStartID != "unavailable" && component.CapturedUnixMS > prior.CapturedUnixMS && !prior.Stale && !component.Stale && prior.Availability == telemetry.AvailabilityExact && component.Availability == telemetry.AvailabilityExact
		priorMetrics := make(map[string]telemetry.Metric, len(prior.Metrics))
		if sameProcess {
			for _, metric := range prior.Metrics {
				priorMetrics[monitorMetricIdentity(metric)] = metric
			}
		}
		for _, metric := range component.Metrics {
			identity := monitorMetricIdentity(metric)
			priorMetric, found := priorMetrics[identity]
			reset := !sameProcess || !found || metric.Availability != telemetry.AvailabilityExact || priorMetric.Availability != telemetry.AvailabilityExact
			switch {
			case view == "status" && metric.Kind == telemetry.MetricCounter:
				if !reset && metric.Value >= priorMetric.Value {
					seconds := float64(component.CapturedUnixMS-prior.CapturedUnixMS) / 1000
					lines = append(lines, fmt.Sprintf("  rate %s.%s=%.2f %s/s reset=false", component.Component, identity, float64(metric.Value-priorMetric.Value)/seconds, metric.Unit))
				} else {
					lines = append(lines, fmt.Sprintf("  rate %s.%s=unavailable reset=true", component.Component, identity))
				}
			case view == "latency" && metric.Kind == telemetry.MetricHistogram:
				counts, validDelta := histogramDelta(priorMetric, metric, !reset)
				if validDelta {
					lines = append(lines, fmt.Sprintf("  latency %s.%s p50<=%d p95<=%d p99<=%d %s reset=false", component.Component, identity, histogramPercentile(metric.BucketUpper, counts, 50), histogramPercentile(metric.BucketUpper, counts, 95), histogramPercentile(metric.BucketUpper, counts, 99), metric.Unit))
				} else {
					lines = append(lines, fmt.Sprintf("  latency %s.%s warming reset=true", component.Component, identity))
				}
			}
		}
	}
	return lines
}

func monitorMetricIdentity(metric telemetry.Metric) string {
	labels := append([]telemetry.Label(nil), metric.Labels...)
	sort.Slice(labels, func(left, right int) bool { return labels[left].Name < labels[right].Name })
	identity := metric.Name
	for _, label := range labels {
		identity += "." + label.Name + "=" + label.Value
	}
	return identity
}

func histogramDelta(previous, current telemetry.Metric, comparable bool) ([]uint64, bool) {
	if !comparable || previous.Kind != telemetry.MetricHistogram || len(previous.BucketUpper) != len(current.BucketUpper) || len(current.BucketUpper) == 0 {
		return nil, false
	}
	counts := make([]uint64, len(current.BucketCounts))
	for index := range counts {
		if previous.BucketUpper[index] != current.BucketUpper[index] || current.BucketCounts[index] < previous.BucketCounts[index] {
			return nil, false
		}
		counts[index] = current.BucketCounts[index] - previous.BucketCounts[index]
	}
	return counts, counts[len(counts)-1] != 0
}

func histogramPercentile(bounds, cumulative []uint64, percentile uint64) uint64 {
	count := cumulative[len(cumulative)-1]
	rank := (count/100)*percentile + (count%100*percentile+99)/100
	for index, value := range cumulative {
		if value >= rank {
			return bounds[index]
		}
	}
	return bounds[len(bounds)-1]
}

func newMonitorExportCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "export", Short: "Export bounded monitoring snapshots", Args: cobra.NoArgs, DisableAutoGenTag: true}
	options := monitorExportOptions{Interval: 15 * time.Second, Timeout: 5 * time.Second, Queue: 4, BatchLimit: 1000, RetryLimit: 3, RetryBackoff: time.Second}
	influx := &cobra.Command{
		Use: "influxdb", Short: "Continuously export snapshots to InfluxDB v2", Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			return runMonitorInfluxExport(command.Context(), globalOptions, options)
		},
	}
	influx.Flags().StringVar(&options.URL, "url", "", "InfluxDB v2 server URL")
	influx.Flags().StringVar(&options.Org, "org", "", "InfluxDB organization")
	influx.Flags().StringVar(&options.Bucket, "bucket", "", "InfluxDB bucket")
	influx.Flags().StringVar(&options.DeploymentID, "deployment-id", "default", "bounded deployment identity")
	influx.Flags().StringVar(&options.TokenFile, "token-file", "", "protected Unix file containing the InfluxDB token")
	influx.Flags().StringVar(&options.TokenEnv, "token-env", "", "environment variable containing the InfluxDB token")
	influx.Flags().DurationVar(&options.Interval, "interval", options.Interval, "snapshot export interval")
	influx.Flags().DurationVar(&options.Timeout, "timeout", options.Timeout, "collection and request timeout")
	influx.Flags().IntVar(&options.Queue, "queue-capacity", options.Queue, "maximum queued snapshots")
	influx.Flags().IntVar(&options.BatchLimit, "batch-limit", options.BatchLimit, "maximum points per HTTP request")
	influx.Flags().IntVar(&options.RetryLimit, "retry-limit", options.RetryLimit, "maximum retries per snapshot")
	influx.Flags().DurationVar(&options.RetryBackoff, "retry-backoff", options.RetryBackoff, "initial bounded retry backoff")
	command.AddCommand(influx)
	return command
}

func runMonitorInfluxExport(ctx context.Context, globalOptions *global.Options, options monitorExportOptions) error {
	if err := validateMonitorExportOptions(options); err != nil {
		return err
	}
	exporter, err := telemetry.NewInfluxExporter(telemetry.InfluxConfig{
		URL: options.URL, Org: options.Org, Bucket: options.Bucket, DeploymentID: options.DeploymentID,
		TokenFile: options.TokenFile, TokenEnv: options.TokenEnv, Timeout: options.Timeout, BatchLimit: options.BatchLimit,
	})
	if err != nil {
		return err
	}
	worker := telemetry.NewAsyncExporterWithConfig(exporter, telemetry.AsyncExporterConfig{
		Capacity: options.Queue, RetryLimit: options.RetryLimit, RetryBackoff: options.RetryBackoff, Timeout: options.Timeout,
	})
	defer worker.CloseWithin(options.Timeout)
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	for {
		snapshot, collectErr := collectMonitorSnapshotWithTimeout(ctx, globalOptions, false, options.Timeout)
		if collectErr != nil {
			return collectErr
		}
		addMonitorExportHealth(&snapshot, worker)
		if validateErr := snapshot.Validate(); validateErr != nil {
			return fmt.Errorf("validate export monitoring snapshot: %w", validateErr)
		}
		worker.Submit(snapshot)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func validateMonitorExportOptions(options monitorExportOptions) error {
	if options.Interval < time.Second || options.Interval > time.Hour {
		return fmt.Errorf("monitor export interval must be between 1s and 1h")
	}
	if options.Timeout < 100*time.Millisecond || options.Timeout > time.Minute {
		return fmt.Errorf("monitor export timeout must be between 100ms and 1m")
	}
	if options.Queue < 1 || options.Queue > 1024 || options.BatchLimit < 1 || options.BatchLimit > 10000 || options.RetryLimit < 0 || options.RetryLimit > 16 || options.RetryBackoff < 0 || options.RetryBackoff > time.Minute || (options.RetryLimit > 0 && options.RetryBackoff == 0) {
		return fmt.Errorf("monitor export bounds are invalid")
	}
	return nil
}

func addMonitorExportHealth(snapshot *telemetry.MonitorSnapshot, worker *telemetry.AsyncExporter) {
	addMonitorExportHealthValues(snapshot, worker.Failures(), worker.Dropped())
}

func addMonitorExportHealthValues(snapshot *telemetry.MonitorSnapshot, failures, dropped uint64) {
	for index := range snapshot.Components {
		if snapshot.Components[index].Component != "vaultic" {
			continue
		}
		health := []telemetry.Metric{
			{Name: "monitor_export_failures", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: failures},
			{Name: "monitor_export_dropped", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: dropped},
		}
		metrics := snapshot.Components[index].Metrics
		if displaced := max(len(metrics)+len(health)-telemetry.MaxMonitorMetrics, 0); displaced != 0 {
			metrics = metrics[:len(metrics)-displaced]
			snapshot.Components[index].CardinalityDropped += uint64(displaced)
		}
		snapshot.Components[index].Metrics = append(metrics, health...)
		return
	}
}

func collectMonitorSnapshotWithTimeout(ctx context.Context, globalOptions *global.Options, reconcile bool, timeout time.Duration) (telemetry.MonitorSnapshot, error) {
	if timeout < 100*time.Millisecond || timeout > time.Minute {
		return telemetry.MonitorSnapshot{}, fmt.Errorf("monitor collector timeout must be between 100ms and 1m")
	}
	collectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	snapshot, err := collectMonitorSnapshot(collectCtx, globalOptions, reconcile)
	if err != nil {
		return telemetry.MonitorSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return telemetry.MonitorSnapshot{}, fmt.Errorf("validate collected monitoring snapshot: %w", err)
	}
	return snapshot, nil
}

func collectMonitorSnapshot(ctx context.Context, globalOptions *global.Options, reconcile bool) (telemetry.MonitorSnapshot, error) {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	_, repo, unlock, err := openWithReadLock(ctx, *globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		now := time.Now()
		return telemetry.NewMonitorSnapshot(now,
			unavailableMonitorComponent("vaultic", now),
			collectBrokerMonitorComponent(ctx, globalOptions.KeyBrokerSocket, now),
			unavailableMonitorComponent("vaulticdb", now),
		), nil
	}
	defer unlock()
	now := time.Now()
	components := []telemetry.ComponentSnapshot{repositoryMonitorComponent(repo, now)}
	components = append(components, collectBrokerMonitorComponent(ctx, globalOptions.KeyBrokerSocket, now))
	daemonEngine, ok := repo.Engine().(*metadataindex.DaemonEngine)
	if !ok {
		components = append(components, unavailableMonitorComponent("vaulticdb", now))
		return telemetry.NewMonitorSnapshot(now, components...), nil
	}
	stats, statsErr := maintenance.Stats(ctx, daemonEngine.SchemaStore(), maintenance.StatsOptions{})
	if statsErr == nil {
		components[0].Storage = repositoryAggregateStorage(stats)
	}
	if reconcile {
		counts, countErr := maintenance.BackendPlacementCounts(ctx, daemonEngine.SchemaStore(), maintenance.PlacementModel{})
		if countErr != nil {
			return telemetry.MonitorSnapshot{}, fmt.Errorf("reconcile repository placement storage: %w", countErr)
		}
		components[0].Storage = placementStorageSnapshots(counts)
	}
	writer, writerErr := daemonEngine.Client().WriterStatus(ctx)
	if writerErr != nil {
		components = append(components, unavailableMonitorComponent("vaulticdb", now))
		return telemetry.NewMonitorSnapshot(now, components...), nil
	}
	cache, cacheErr := daemonEngine.Client().ReadCacheStatus(ctx)
	component := telemetry.VaulticDBComponent(writer, cache, daemonEngine.Client().WALInfo())
	if cacheErr != nil {
		component.Caches = []telemetry.CacheSnapshot{{
			ID: "slatedb", Availability: telemetry.AvailabilityUnavailable,
			CircuitState:        "unknown",
			TrafficAvailability: telemetry.AvailabilityUnavailable, TrafficBytesAvailability: telemetry.AvailabilityUnavailable, FillAvailability: telemetry.AvailabilityUnavailable,
			InventoryAvailability: telemetry.AvailabilityUnavailable, DeletionAvailability: telemetry.AvailabilityUnavailable,
			ReconciliationAgeAvailability: telemetry.AvailabilityUnavailable,
			ReconciliationLagAvailability: telemetry.AvailabilityUnavailable,
		}}
	}
	components = append(components, component)
	return telemetry.NewMonitorSnapshot(now, components...), nil
}

func repositoryAggregateStorage(stats maintenance.StatsResult) []telemetry.StorageSnapshot {
	physicalAvailability := telemetry.AvailabilityExact
	if stats.PhysicalSizeUnknownPacks != 0 || stats.UsageUnaccountedPacks != 0 {
		physicalAvailability = telemetry.AvailabilityEstimated
	}
	return []telemetry.StorageSnapshot{{
		BackendID: "repository", Role: "repository", Availability: telemetry.AvailabilityExact,
		ObjectCount: stats.Totals.PackCount, PayloadBytes: stats.Totals.PayloadSize,
		PhysicalBytes: stats.StoredPhysicalSize, ObjectClass: "pack",
		PlacementState: "unknown", Representation: "encrypted_pack",
		ObjectCountAvailability: telemetry.AvailabilityExact, PayloadAvailability: telemetry.AvailabilityExact,
		PhysicalAvailability: physicalAvailability, ReconciliationAvailability: telemetry.AvailabilityUnavailable,
	}}
}

func placementStorageSnapshots(counts map[string]maintenance.BackendPlacementCount) []telemetry.StorageSnapshot {
	storage := make([]telemetry.StorageSnapshot, 0, len(counts))
	for backendID, count := range counts {
		storage = append(storage, telemetry.StorageSnapshot{
			BackendID: backendID, Role: "repository", Availability: telemetry.AvailabilityEstimated,
			ObjectCount: count.Objects, PhysicalBytes: count.Bytes,
			ObjectClass: "pack", PlacementState: "unknown", Representation: "encrypted_pack",
			ObjectCountAvailability: telemetry.AvailabilityEstimated, PayloadAvailability: telemetry.AvailabilityUnavailable,
			PhysicalAvailability: telemetry.AvailabilityEstimated, ReconciliationAvailability: telemetry.AvailabilityUnavailable,
		})
	}
	sort.Slice(storage, func(left, right int) bool { return storage[left].BackendID < storage[right].BackendID })
	return storage
}

func collectBrokerMonitorComponent(ctx context.Context, socket string, now time.Time) telemetry.ComponentSnapshot {
	if socket == "" {
		return unavailableMonitorComponent("key_broker", now)
	}
	client, err := broker.Dial(ctx, socket)
	if err != nil {
		return unavailableMonitorComponent("key_broker", now)
	}
	defer client.Close()
	status, err := client.Status(ctx)
	if err != nil {
		return unavailableMonitorComponent("key_broker", now)
	}
	locked := uint64(0)
	if status.Locked {
		locked = 1
	}
	processStartID := "legacy"
	capturedUnixMS := now.UnixMilli()
	availability := telemetry.AvailabilityEstimated
	if status.ProcessStartedUnixMS > 0 {
		processStartID = strconv.FormatInt(status.ProcessStartedUnixMS, 10)
		capturedUnixMS = status.CapturedUnixMS
		availability = telemetry.AvailabilityExact
	}
	return telemetry.ComponentSnapshot{
		Component: "key_broker", ProcessStartID: processStartID, CapturedUnixMS: capturedUnixMS,
		Availability: availability,
		Metrics: []telemetry.Metric{
			{Name: "broker_locked", Kind: telemetry.MetricGauge, Unit: "state", Availability: telemetry.AvailabilityExact, Value: locked},
			{Name: "broker_active_sessions", Kind: telemetry.MetricGauge, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: uint64(status.ActiveSessions)},
			{Name: "broker_active_leases", Kind: telemetry.MetricGauge, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: uint64(status.ActiveLeases)},
		},
	}
}

func repositoryMonitorComponent(repo *repository.Repository, now time.Time) telemetry.ComponentSnapshot {
	status := repo.ReadCacheStatus()
	accountingMetrics, accountingOperations, accountingOverflow, accountingDropped := telemetry.DefaultProductionAccounting().Snapshot(telemetry.MaxMonitorMetrics)
	component := telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(monitorProcessStarted.UnixMilli(), 10),
		CapturedUnixMS: now.UnixMilli(), Availability: telemetry.AvailabilityExact,
		Metrics: accountingMetrics, Operations: accountingOperations, OperationOverflow: accountingOverflow,
		CardinalityDropped: accountingDropped,
		Storage: []telemetry.StorageSnapshot{{
			BackendID: "repository", Role: "repository", Availability: telemetry.AvailabilityUnavailable,
			ObjectClass: "unknown", PlacementState: "unknown", Representation: "encrypted_pack",
			ObjectCountAvailability: telemetry.AvailabilityUnavailable, PayloadAvailability: telemetry.AvailabilityUnavailable,
			PhysicalAvailability: telemetry.AvailabilityUnavailable, ReconciliationAvailability: telemetry.AvailabilityUnavailable,
		}},
	}
	if status.Enabled {
		availability, reconciliationAgeAvailability := repositoryCacheAvailability(status.TelemetryState)
		controllerState := "healthy"
		if availability != telemetry.AvailabilityExact || status.CapacityHealth != "" && status.CapacityHealth != "healthy" {
			controllerState = "degraded"
		}
		available := uint64(0)
		if status.UsedBytes <= status.AggregateMaxBytes && status.ReservedBytes < status.AggregateMaxBytes-status.UsedBytes {
			available = status.AggregateMaxBytes - status.UsedBytes - status.ReservedBytes
		}
		component.Caches = append(component.Caches, telemetry.CacheSnapshot{
			ID: "repository", Availability: availability,
			RequestedBytes: status.RequestedMaxBytes, EffectiveBytes: status.AggregateMaxBytes,
			UsedBytes: status.UsedBytes, ReservedBytes: status.ReservedBytes,
			PinnedBytes: status.PinnedBytes, StagingBytes: status.InflightBytes,
			DeletionPendingBytes: status.PendingDeleteBytes, AvailableBytes: available,
			Enabled: true, Family: "aggregate", Representation: "encrypted_pack",
			ControllerState: controllerState, ReconciliationAgeMS: status.TelemetryAgeMS,
			CircuitState:        "not_applicable",
			TrafficAvailability: telemetry.AvailabilityUnavailable, TrafficBytesAvailability: telemetry.AvailabilityUnavailable, FillAvailability: telemetry.AvailabilityUnavailable,
			InventoryAvailability: telemetry.AvailabilityUnavailable, DeletionAvailability: availability,
			ReconciliationAgeAvailability: reconciliationAgeAvailability,
			ReconciliationLagAvailability: telemetry.AvailabilityUnavailable,
		})
	}
	return component
}

func repositoryCacheAvailability(state string) (telemetry.Availability, telemetry.Availability) {
	switch state {
	case "fixed", "fresh":
		return telemetry.AvailabilityExact, telemetry.AvailabilityExact
	case "stale":
		return telemetry.AvailabilityStale, telemetry.AvailabilityExact
	case "denied", "inconsistent", "unavailable":
		return telemetry.AvailabilityUnavailable, telemetry.AvailabilityUnavailable
	default:
		return telemetry.AvailabilityEstimated, telemetry.AvailabilityUnavailable
	}
}

func unavailableMonitorComponent(name string, now time.Time) telemetry.ComponentSnapshot {
	return telemetry.ComponentSnapshot{
		Component: name, ProcessStartID: "unavailable", CapturedUnixMS: now.UnixMilli(),
		Availability: telemetry.AvailabilityUnavailable, Stale: true,
	}
}

func filterMonitorSnapshot(snapshot *telemetry.MonitorSnapshot, view string, options monitorOptions) {
	components := snapshot.Components[:0]
	for _, component := range snapshot.Components {
		if options.Component != "" && component.Component != options.Component {
			continue
		}
		if options.Backend != "" {
			storage := component.Storage[:0]
			for _, item := range component.Storage {
				if item.BackendID == options.Backend {
					storage = append(storage, item)
				}
			}
			component.Storage = storage
		}
		switch view {
		case "storage":
			component.Metrics, component.Queues, component.Operations, component.Caches, component.WAL = nil, nil, nil, nil, nil
		case "operations":
			component.Metrics, component.Storage, component.Caches, component.WAL = nil, nil, nil, nil
			if options.Active {
				component.Queues = nil
			}
		case "caches":
			component.Metrics, component.Queues, component.Operations, component.Storage, component.WAL = nil, nil, nil, nil, nil
			if options.Cache != "" {
				caches := component.Caches[:0]
				for _, cache := range component.Caches {
					if cache.ID == options.Cache {
						caches = append(caches, cache)
					}
				}
				component.Caches = caches
			}
		case "wal":
			component.Metrics, component.Queues, component.Operations, component.Storage, component.Caches = nil, nil, nil, nil, nil
		case "latency":
			metrics := component.Metrics[:0]
			for _, metric := range component.Metrics {
				if metric.Kind == telemetry.MetricHistogram {
					metrics = append(metrics, metric)
				}
			}
			component.Metrics, component.Queues, component.Operations, component.Storage, component.Caches, component.WAL = metrics, nil, nil, nil, nil, nil
		}
		components = append(components, component)
	}
	snapshot.Components = components
}

func monitorLines(snapshot telemetry.MonitorSnapshot, view string) []string {
	lines := []string{fmt.Sprintf("monitor schema=%d captured=%s", snapshot.SchemaVersion, time.UnixMilli(snapshot.CapturedUnixMS).Format(time.RFC3339))}
	for _, component := range snapshot.Components {
		lines = append(lines, fmt.Sprintf("%-12s availability=%s stale=%v metrics=%d queues=%d operations=%d storage=%d caches=%d", component.Component, component.Availability, component.Stale, len(component.Metrics), len(component.Queues), len(component.Operations), len(component.Storage), len(component.Caches)))
		if view == "operations" {
			for _, queue := range component.Queues {
				availability := monitorInheritedAvailability(component.Availability, queue.Availability)
				capacityAvailability := monitorInheritedAvailability(component.Availability, queue.CapacityAvailability)
				lines = append(lines, fmt.Sprintf("  queue %-20s availability=%s depth=%s capacity=%s active=%s oldest=%s throttle=%s", queue.Name, availability, monitorAvailableUint(queue.Depth, availability), monitorAvailableUint(queue.Capacity, capacityAvailability), monitorAvailableUint(queue.ActiveWorkers, availability), monitorAvailableDuration(queue.OldestItemAgeUS, availability), queue.Backpressure))
			}
			for _, operation := range component.Operations {
				age := time.Duration(component.CapturedUnixMS-operation.StartedUnixMS) * time.Millisecond
				if age < 0 {
					age = 0
				}
				lines = append(lines, fmt.Sprintf("  operation %-16s availability=%s class=%s phase=%s progress=%s/%s age=%s blocked=%s", operation.ID, component.Availability, operation.Class, operation.Phase, monitorAvailableUint(operation.CompletedUnits, component.Availability), monitorAvailableUint(operation.ExpectedUnits, component.Availability), monitorAvailableDuration(uint64(age/time.Microsecond), component.Availability), operation.BlockingReason))
			}
		}
		if view == "storage" {
			for _, storage := range component.Storage {
				availability := monitorInheritedAvailability(component.Availability, storage.Availability)
				lines = append(lines, fmt.Sprintf("  storage %-18s role=%s availability=%s objects=%s payload=%s physical=%s acknowledgement=%s", storage.BackendID, storage.Role, availability, monitorAvailableUint(storage.ObjectCount, monitorInheritedAvailability(component.Availability, storage.ObjectCountAvailability)), monitorAvailableUint(storage.PayloadBytes, monitorInheritedAvailability(component.Availability, storage.PayloadAvailability)), monitorAvailableUint(storage.PhysicalBytes, monitorInheritedAvailability(component.Availability, storage.PhysicalAvailability)), storage.Acknowledgement))
			}
		}
		if view == "caches" || view == "status" {
			for _, cache := range component.Caches {
				availability := monitorInheritedAvailability(component.Availability, cache.Availability)
				trafficAvailability := monitorInheritedAvailability(component.Availability, cache.TrafficAvailability)
				lines = append(lines, fmt.Sprintf("  cache %-20s availability=%s used=%s reserved=%s available=%s hits=%s misses=%s", cache.ID, availability, monitorAvailableUint(cache.UsedBytes, availability), monitorAvailableUint(cache.ReservedBytes, availability), monitorAvailableUint(cache.AvailableBytes, availability), monitorAvailableUint(cache.Hits, trafficAvailability), monitorAvailableUint(cache.Misses, trafficAvailability)))
			}
		}
		if (view == "wal" || view == "status") && component.WAL != nil {
			availability := monitorInheritedAvailability(component.Availability, component.WAL.Availability)
			lines = append(lines, fmt.Sprintf("  wal target=%s durability=%s availability=%s retained=%s flushes=%s throttle=%s", component.WAL.Target, component.WAL.Durability, availability, monitorAvailableUint(component.WAL.RetainedBytes, availability), monitorAvailableUint(component.WAL.OutstandingFlushes, availability), component.WAL.ThrottleReason))
		}
	}
	if len(snapshot.Components) == 0 {
		lines = append(lines, "no matching components")
	}
	return lines
}

func monitorAvailableUint(value uint64, availability telemetry.Availability) string {
	switch availability {
	case telemetry.AvailabilityExact:
		return strconv.FormatUint(value, 10)
	case telemetry.AvailabilityEstimated, telemetry.AvailabilityStale:
		return fmt.Sprintf("%d(%s)", value, availability)
	default:
		return string(availability)
	}
}

func monitorAvailableDuration(value uint64, availability telemetry.Availability) string {
	formatted := (time.Duration(value) * time.Microsecond).String()
	if availability == telemetry.AvailabilityExact {
		return formatted
	}
	return fmt.Sprintf("%s(%s)", formatted, availability)
}

func monitorInheritedAvailability(parent, child telemetry.Availability) telemetry.Availability {
	if child == telemetry.AvailabilityNotApplicable {
		return child
	}
	order := map[telemetry.Availability]int{
		telemetry.AvailabilityExact: 0, telemetry.AvailabilityEstimated: 1,
		telemetry.AvailabilityStale: 2, telemetry.AvailabilityUnavailable: 3,
	}
	if order[parent] > order[child] {
		return parent
	}
	return child
}

func validateMonitorFilter(value string) error {
	if value != "" && (len(value) > telemetry.MaxMonitorStringLength || strings.ContainsAny(value, "\r\n\x00")) {
		return fmt.Errorf("invalid monitor filter")
	}
	return nil
}
