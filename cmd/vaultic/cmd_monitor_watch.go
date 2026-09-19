package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/ui"
)

type monitorTicker interface {
	C() <-chan time.Time
	Stop()
}

type monitorRealTicker struct{ ticker *time.Ticker }

func (ticker monitorRealTicker) C() <-chan time.Time { return ticker.ticker.C }
func (ticker monitorRealTicker) Stop()               { ticker.ticker.Stop() }

type monitorWatchDeps struct {
	now         func() time.Time
	width       func() int
	collect     func(context.Context) (telemetry.MonitorSnapshot, error)
	setStatus   func([]string)
	pollKeys    func() ([]byte, error)
	collectTick <-chan time.Time
	renderTick  <-chan time.Time
	inputTick   <-chan time.Time
	stop        func()
}

type monitorReconcileDeps struct {
	now       func() time.Time
	collect   func(context.Context) (telemetry.MonitorSnapshot, error)
	setStatus func([]string)
	progress  <-chan time.Time
	stop      func()
}

type monitorWatchState struct {
	view             string
	sortKey          string
	sortDesc         bool
	paused           bool
	lastSnapshot     *telemetry.MonitorSnapshot
	lastCapturedAt   time.Time
	windowStore      monitorWindowStore
	windowResetLabel string
	windowResetAt    time.Time
}

type monitorWindowStore struct {
	samples []monitorWindowSample
}

const (
	monitorWindowResolution = 15 * time.Second
	maxMonitorWindowSamples = 62
)

type monitorWindowSample struct {
	capturedAt time.Time
	snapshot   telemetry.MonitorSnapshot
}

type monitorMetricWindow struct {
	rate  float64
	p50   string
	p95   string
	p99   string
	state string
}

type monitorWatchCollectionResult struct {
	snapshot telemetry.MonitorSnapshot
	err      error
}

func collectMonitorSnapshotForCommand(ctx context.Context, globalOptions *global.Options, view string, options monitorOptions) (telemetry.MonitorSnapshot, error) {
	if view == monitorViewStorage && options.Reconcile {
		return collectMonitorSnapshotReconcileAsync(ctx, globalOptions, options, monitorReconcileDeps{})
	}
	return collectMonitorSnapshotWithTimeout(ctx, globalOptions, options.Reconcile, options.Timeout)
}

func collectMonitorSnapshotReconcileAsync(ctx context.Context, globalOptions *global.Options, options monitorOptions, deps monitorReconcileDeps) (telemetry.MonitorSnapshot, error) {
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.collect == nil {
		deps.collect = func(collectCtx context.Context) (telemetry.MonitorSnapshot, error) {
			return collectMonitorSnapshotWithTimeout(collectCtx, globalOptions, true, options.Timeout)
		}
	}
	showProgress := deps.setStatus != nil || globalOptions.Term.OutputIsTerminal() && globalOptions.Term.CanUpdateStatus()
	if deps.setStatus == nil && showProgress {
		deps.setStatus = globalOptions.Term.SetStatus
	}
	if deps.progress == nil && showProgress {
		ticker := time.NewTicker(250 * time.Millisecond)
		deps.progress = ticker.C
		deps.stop = ticker.Stop
	}
	if deps.stop != nil {
		defer deps.stop()
	}
	if showProgress {
		defer deps.setStatus(nil)
	}

	type result struct {
		snapshot telemetry.MonitorSnapshot
		err      error
	}
	resultCh := make(chan result, 1)
	started := deps.now()
	go func() {
		snapshot, err := deps.collect(ctx)
		resultCh <- result{snapshot: snapshot, err: err}
	}()
	for {
		select {
		case <-ctx.Done():
			return telemetry.MonitorSnapshot{}, ctx.Err()
		case result := <-resultCh:
			return result.snapshot, result.err
		case now := <-deps.progress:
			if !showProgress {
				continue
			}
			elapsed := now.Sub(started).Truncate(100 * time.Millisecond)
			line := fmt.Sprintf("monitor storage reconcile running elapsed=%s cancel=ctrl-c", elapsed)
			deps.setStatus([]string{line})
		}
	}
}

func runMonitorWatchLoop(ctx context.Context, globalOptions *global.Options, options monitorOptions, deps monitorWatchDeps) (resultErr error) {
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	view, err := normalizeMonitorView(options.View)
	if err != nil {
		return err
	}
	if options.Interval < 100*time.Millisecond || options.Interval > time.Minute {
		return fmt.Errorf("monitor interval must be between 100ms and 1m")
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.collect == nil {
		deps.collect = func(collectCtx context.Context) (telemetry.MonitorSnapshot, error) {
			return collectMonitorSnapshotWithTimeout(collectCtx, globalOptions, false, options.Timeout)
		}
	}
	if deps.setStatus == nil {
		deps.setStatus = globalOptions.Term.SetStatus
	}
	if deps.width == nil {
		deps.width = func() int { return monitorTerminalWidth(globalOptions) }
	}
	poller, pollerClose, err := monitorWatchKeyPoller(globalOptions, deps.pollKeys)
	if err != nil {
		return err
	}
	defer func() {
		cancel()
		resultErr = errors.Join(resultErr, pollerClose())
	}()
	if deps.collectTick == nil || deps.renderTick == nil || deps.inputTick == nil {
		collectTicker := monitorRealTicker{ticker: time.NewTicker(options.Interval)}
		renderTicker := monitorRealTicker{ticker: time.NewTicker(250 * time.Millisecond)}
		inputTicker := monitorRealTicker{ticker: time.NewTicker(100 * time.Millisecond)}
		deps.collectTick, deps.renderTick, deps.inputTick = collectTicker.C(), renderTicker.C(), inputTicker.C()
		deps.stop = func() {
			collectTicker.Stop()
			renderTicker.Stop()
			inputTicker.Stop()
		}
	}
	if deps.stop != nil {
		defer deps.stop()
	}
	defer deps.setStatus(nil)

	state := monitorWatchState{view: view, sortKey: monitorSortKeys(view)[0]}
	collecting := true
	collection := startMonitorWatchCollection(watchCtx, deps.collect)
	deps.setStatus(state.render(deps.width(), deps.now()))
	for {
		select {
		case <-watchCtx.Done():
			return watchCtx.Err()
		case result := <-collection:
			collecting = false
			if result.err != nil {
				return result.err
			}
			if !state.paused {
				monitorRecordSnapshot(deps.now(), result.snapshot, &state)
			}
			deps.setStatus(state.render(deps.width(), deps.now()))
		case <-deps.collectTick:
			if state.paused || collecting {
				continue
			}
			collecting = true
			collection = startMonitorWatchCollection(watchCtx, deps.collect)
		case <-deps.renderTick:
			deps.setStatus(state.render(deps.width(), deps.now()))
		case <-deps.inputTick:
			keys, pollErr := poller()
			if pollErr != nil {
				return pollErr
			}
			wasPaused := state.paused
			if monitorApplyKeys(&state, keys, deps.now()) {
				return nil
			}
			if wasPaused && !state.paused && !collecting {
				collecting = true
				collection = startMonitorWatchCollection(watchCtx, deps.collect)
			}
			if len(keys) != 0 {
				deps.setStatus(state.render(deps.width(), deps.now()))
			}
		}
	}
}

func startMonitorWatchCollection(ctx context.Context, collect func(context.Context) (telemetry.MonitorSnapshot, error)) <-chan monitorWatchCollectionResult {
	result := make(chan monitorWatchCollectionResult, 1)
	go func() {
		snapshot, err := collect(ctx)
		result <- monitorWatchCollectionResult{snapshot: snapshot, err: err}
	}()
	return result
}

func monitorWatchKeyPoller(globalOptions *global.Options, injected func() ([]byte, error)) (func() ([]byte, error), func() error, error) {
	if injected != nil {
		return injected, func() error { return nil }, nil
	}
	if !globalOptions.Term.InputIsTerminal() {
		return nil, nil, fmt.Errorf("monitor watch requires terminal input")
	}
	poller, err := newMonitorKeyPoller(globalOptions.Term.InputRaw())
	if err != nil {
		return nil, nil, err
	}
	return poller.Poll, poller.Close, nil
}

func monitorTerminalWidth(globalOptions *global.Options) int {
	return globalOptions.Term.Width()
}

func monitorRecordSnapshot(now time.Time, snapshot telemetry.MonitorSnapshot, state *monitorWatchState) {
	state.lastSnapshot = &snapshot
	state.lastCapturedAt = time.UnixMilli(snapshot.CapturedUnixMS)
	state.windowStore.Add(now, snapshot)
}

func normalizeMonitorView(view string) (string, error) {
	if view == "" || view == monitorViewOverview {
		return monitorViewOverview, nil
	}
	switch view {
	case monitorViewStatus, monitorViewStorage, monitorViewOperations, monitorViewWAL, monitorViewCaches, monitorViewLatency:
		return view, nil
	default:
		return "", fmt.Errorf("unknown monitor view %q", view)
	}
}

func (store *monitorWindowStore) Add(now time.Time, snapshot telemetry.MonitorSnapshot) {
	compact := compactMonitorWindowSnapshot(snapshot)
	if count := len(store.samples); count != 0 && now.Sub(store.samples[count-1].capturedAt) < monitorWindowResolution {
		if monitorWindowAvailabilityEqual(store.samples[count-1].snapshot, compact) {
			return
		}
	}
	store.samples = append(store.samples, monitorWindowSample{capturedAt: now, snapshot: compact})
	cutoff := now.Add(-15 * time.Minute)
	keep := store.samples[:0]
	for index, sample := range store.samples {
		if !sample.capturedAt.Before(cutoff) {
			if index > 0 && len(keep) == 0 {
				keep = append(keep, store.samples[index-1])
			}
			keep = append(keep, sample)
		}
	}
	store.samples = keep
	if len(store.samples) > maxMonitorWindowSamples {
		store.samples = store.samples[len(store.samples)-maxMonitorWindowSamples:]
	}
}

func monitorWindowAvailabilityEqual(left, right telemetry.MonitorSnapshot) bool {
	if len(left.Components) != len(right.Components) {
		return false
	}
	for _, leftComponent := range left.Components {
		rightComponent, found := monitorFindComponent(right, leftComponent.Component)
		if !found || leftComponent.ProcessStartID != rightComponent.ProcessStartID || leftComponent.Availability != rightComponent.Availability || leftComponent.Stale != rightComponent.Stale || len(leftComponent.Metrics) != len(rightComponent.Metrics) {
			return false
		}
		for _, leftMetric := range leftComponent.Metrics {
			rightMetric, metricFound := monitorFindMetric(rightComponent, leftMetric)
			if !metricFound || leftMetric.Availability != rightMetric.Availability || monitorMetricRegressed(leftMetric, rightMetric) {
				return false
			}
		}
	}
	return true
}

func compactMonitorWindowSnapshot(snapshot telemetry.MonitorSnapshot) telemetry.MonitorSnapshot {
	compact := telemetry.MonitorSnapshot{SchemaVersion: snapshot.SchemaVersion, CapturedUnixMS: snapshot.CapturedUnixMS}
	compact.Components = make([]telemetry.ComponentSnapshot, 0, len(snapshot.Components))
	for _, component := range snapshot.Components {
		entry := telemetry.ComponentSnapshot{
			Component: component.Component, ProcessStartID: component.ProcessStartID,
			CapturedUnixMS: component.CapturedUnixMS, Availability: component.Availability, Stale: component.Stale,
		}
		for _, metric := range component.Metrics {
			if metric.Kind != telemetry.MetricCounter && metric.Kind != telemetry.MetricHistogram {
				continue
			}
			metric.Labels = append([]telemetry.Label(nil), metric.Labels...)
			metric.BucketUpper = append([]uint64(nil), metric.BucketUpper...)
			metric.BucketCounts = append([]uint64(nil), metric.BucketCounts...)
			entry.Metrics = append(entry.Metrics, metric)
		}
		compact.Components = append(compact.Components, entry)
	}
	return compact
}

func (store *monitorWindowStore) Reset() {
	store.samples = nil
}

func monitorApplyKeys(state *monitorWatchState, keys []byte, now time.Time) bool {
	for _, key := range keys {
		switch key {
		case 0x03, 'q', 'Q':
			return true
		case '1', 'o', 'O':
			state.view = monitorViewOverview
			state.sortKey = monitorSortKeys(state.view)[0]
		case '2':
			state.view = monitorViewStorage
			state.sortKey = monitorSortKeys(state.view)[0]
		case '3', 'p', 'P':
			state.view = monitorViewOperations
			state.sortKey = monitorSortKeys(state.view)[0]
		case '4', 'w', 'W':
			state.view = monitorViewWAL
			state.sortKey = monitorSortKeys(state.view)[0]
		case '5', 'c', 'C':
			state.view = monitorViewCaches
			state.sortKey = monitorSortKeys(state.view)[0]
		case '6', 'l', 'L':
			state.view = monitorViewLatency
			state.sortKey = monitorSortKeys(state.view)[0]
		case 's':
			state.sortKey = monitorNextSortKey(state.view, state.sortKey)
		case 'S':
			state.sortDesc = !state.sortDesc
		case ' ':
			state.paused = !state.paused
		case 'r', 'R':
			state.windowStore.Reset()
			state.windowResetAt = now
			state.windowResetLabel = "manual"
		}
	}
	return false
}

func monitorSortKeys(view string) []string {
	switch view {
	case monitorViewStorage:
		return []string{"backend", "objects", "physical"}
	case monitorViewOperations:
		return []string{"age", "progress", "class"}
	case monitorViewCaches:
		return []string{"id", "used", "hit_ratio"}
	case monitorViewLatency:
		return []string{"metric", "p95"}
	case monitorViewWAL:
		return []string{"component", "retained"}
	default:
		return []string{"component", "stale", "metrics"}
	}
}

func monitorNextSortKey(view, current string) string {
	keys := monitorSortKeys(view)
	for index, key := range keys {
		if key == current {
			return keys[(index+1)%len(keys)]
		}
	}
	return keys[0]
}

func (state monitorWatchState) render(width int, now time.Time) []string {
	if state.lastSnapshot == nil {
		return []string{"monitor watch waiting for first sample"}
	}
	if width > 0 && width < 96 {
		return state.renderNarrow(width, now)
	}
	snapshot := cloneMonitorWatchSnapshot(*state.lastSnapshot)
	renderView := state.view
	if renderView == monitorViewOverview {
		renderView = monitorViewStatus
	}
	filterMonitorSnapshot(&snapshot, renderView, monitorOptions{})
	state.sortSnapshot(&snapshot)
	lines := []string{state.headerLine(width, now), "keys: 1..6 view, s sort, S reverse, space pause, r reset-window, q quit"}
	if state.view == monitorViewOverview {
		lines = append(lines, state.overviewLines(snapshot)...)
	} else {
		lines = append(lines, monitorLines(snapshot, state.view)...)
	}
	lines = append(lines, state.windowLines(snapshot)...)
	if width > 0 {
		for index := range lines {
			lines[index] = ui.Truncate(lines[index], width)
		}
	}
	return lines
}

func cloneMonitorWatchSnapshot(snapshot telemetry.MonitorSnapshot) telemetry.MonitorSnapshot {
	clone := snapshot
	clone.Components = append([]telemetry.ComponentSnapshot(nil), snapshot.Components...)
	for componentIndex := range clone.Components {
		component := &clone.Components[componentIndex]
		component.Metrics = append([]telemetry.Metric(nil), component.Metrics...)
		for metricIndex := range component.Metrics {
			metric := &component.Metrics[metricIndex]
			metric.Labels = append([]telemetry.Label(nil), metric.Labels...)
			metric.BucketUpper = append([]uint64(nil), metric.BucketUpper...)
			metric.BucketCounts = append([]uint64(nil), metric.BucketCounts...)
		}
		component.Queues = append([]telemetry.QueueSnapshot(nil), component.Queues...)
		component.Operations = append([]telemetry.ActiveOperation(nil), component.Operations...)
		component.OperationOverflow = append([]telemetry.OperationOverflowSnapshot(nil), component.OperationOverflow...)
		component.Storage = append([]telemetry.StorageSnapshot(nil), component.Storage...)
		component.Caches = append([]telemetry.CacheSnapshot(nil), component.Caches...)
		if component.WAL != nil {
			wal := *component.WAL
			component.WAL = &wal
		}
	}
	return clone
}

func (state monitorWatchState) overviewLines(snapshot telemetry.MonitorSnapshot) []string {
	stale := make([]string, 0, len(snapshot.Components))
	var queueComponent, queueName, queueThrottle string
	var queueDepth, queueCapacity uint64
	var queuePressure float64
	var walRetained, walFlushes uint64
	var failures, retries uint64
	failureMetricUnavailable := false
	queueSeen, queueUnavailable := false, false
	walSeen, walUnavailable := false, false
	collectorsIncomplete := false
	var writebackRate float64
	writebackRateAvailable := false
	lines := []string{fmt.Sprintf("monitor schema=%d captured=%s", snapshot.SchemaVersion, time.UnixMilli(snapshot.CapturedUnixMS).Format(time.RFC3339))}
	for _, component := range snapshot.Components {
		if component.Stale || component.Availability != telemetry.AvailabilityExact {
			stale = append(stale, component.Component+"="+string(component.Availability))
			collectorsIncomplete = true
		}
		for _, queue := range component.Queues {
			queueSeen = true
			availability := monitorInheritedAvailability(component.Availability, queue.Availability)
			capacityAvailability := monitorInheritedAvailability(component.Availability, queue.CapacityAvailability)
			if availability != telemetry.AvailabilityExact || capacityAvailability != telemetry.AvailabilityExact {
				queueUnavailable = true
				continue
			}
			pressure := float64(queue.Depth)
			if queue.Capacity != 0 {
				pressure /= float64(queue.Capacity)
			}
			if queueComponent == "" || pressure > queuePressure {
				queueComponent, queueName, queueThrottle = component.Component, queue.Name, queue.Backpressure
				queueDepth, queueCapacity, queuePressure = queue.Depth, queue.Capacity, pressure
			}
		}
		if component.WAL != nil {
			walSeen = true
			if monitorInheritedAvailability(component.Availability, component.WAL.Availability) == telemetry.AvailabilityExact {
				walRetained = walRetained + component.WAL.RetainedBytes
				walFlushes = walFlushes + component.WAL.OutstandingFlushes
			} else {
				walUnavailable = true
			}
		}
		if component.Availability == telemetry.AvailabilityExact && !component.Stale {
			for _, operation := range component.Operations {
				if operation.Phase == "retry" {
					retries++
				}
			}
		}
		for _, metric := range component.Metrics {
			if metric.Name == "engine_memtable_write_bytes" && metric.Kind == telemetry.MetricCounter {
				window := state.counterWindow(component.Component, metric, time.Minute)
				if window.state == "" {
					writebackRate += window.rate
					writebackRateAvailable = true
				}
			}
			if metric.Name != "operation_completed" || metric.Kind != telemetry.MetricCounter || metric.Unit != "operations" {
				continue
			}
			if metric.Availability != telemetry.AvailabilityExact || component.Availability != telemetry.AvailabilityExact || component.Stale {
				failureMetricUnavailable = true
				continue
			}
			for _, label := range metric.Labels {
				if label.Name == "outcome" && label.Value == "failure" {
					failures += metric.Value
				}
			}
		}
		for _, cache := range component.Caches {
			availability := monitorInheritedAvailability(component.Availability, cache.Availability)
			trafficAvailability := monitorInheritedAvailability(component.Availability, cache.TrafficBytesAvailability)
			occupancy := monitorAvailableRatio(cache.UsedBytes+cache.ReservedBytes, cache.EffectiveBytes, availability)
			byteHit := monitorAvailableRatio(cache.CacheBytes, cache.CacheBytes+cache.OriginBytes, trafficAvailability)
			lines = append(lines, fmt.Sprintf("cache %s.%s occupancy=%s byte_hit_ratio=%s origin_bytes=%s availability=%s", component.Component, cache.ID, occupancy, byteHit, monitorAvailableUint(cache.OriginBytes, monitorInheritedAvailability(component.Availability, cache.TrafficBytesAvailability)), monitorInheritedAvailability(component.Availability, cache.Availability)))
		}
	}
	if queueComponent == "" {
		if queueSeen && queueUnavailable || collectorsIncomplete {
			lines = append(lines, "writeback backlog=unavailable")
		} else {
			lines = append(lines, "writeback backlog=not_applicable")
		}
	} else {
		completeness := "exact"
		if queueUnavailable || collectorsIncomplete {
			completeness = "partial"
		}
		lines = append(lines, fmt.Sprintf("writeback queue=%s.%s depth=%d capacity=%d pressure=%.2f throttle=%s availability=%s", queueComponent, queueName, queueDepth, queueCapacity, queuePressure, queueThrottle, completeness))
	}
	if writebackRateAvailable {
		completeness := ""
		if collectorsIncomplete {
			completeness = "(partial)"
		}
		lines = append(lines, fmt.Sprintf("writeback rate_1m=%.2f%s bytes/s", writebackRate, completeness))
	} else {
		lines = append(lines, "writeback rate_1m=warming_or_unavailable")
	}
	if walSeen && !walUnavailable && !collectorsIncomplete {
		lines = append(lines, fmt.Sprintf("wal retained_bytes=%d outstanding_flushes=%d availability=exact", walRetained, walFlushes))
	} else if walSeen && (walRetained != 0 || walFlushes != 0) {
		lines = append(lines, fmt.Sprintf("wal retained_bytes=%d(partial) outstanding_flushes=%d(partial) availability=partial", walRetained, walFlushes))
	} else if walSeen {
		lines = append(lines, "wal retained_bytes=unavailable outstanding_flushes=unavailable")
	} else if collectorsIncomplete {
		lines = append(lines, "wal retained_bytes=unavailable outstanding_flushes=unavailable")
	} else {
		lines = append(lines, "wal=not_applicable")
	}
	completeness := ""
	if collectorsIncomplete {
		completeness = "(partial)"
	}
	failureCompleteness := completeness
	if failureMetricUnavailable {
		failureCompleteness = "(partial)"
	}
	lines = append(lines, fmt.Sprintf("active retries=%d%s cumulative_operation_failures=%d%s", retries, completeness, failures, failureCompleteness), "slowest_backend=unavailable(no bounded per-backend latency)")
	if len(stale) == 0 {
		lines = append(lines, "collectors stale=none")
	} else {
		lines = append(lines, "collectors stale="+strings.Join(stale, ","))
	}
	return lines
}

func monitorRatio(numerator, denominator uint64) string {
	if denominator == 0 {
		return "unavailable"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(numerator)/float64(denominator))
}

func monitorAvailableRatio(numerator, denominator uint64, availability telemetry.Availability) string {
	if availability != telemetry.AvailabilityExact {
		return string(availability)
	}
	return monitorRatio(numerator, denominator)
}

func (state monitorWatchState) renderNarrow(width int, now time.Time) []string {
	age := now.Sub(state.lastCapturedAt).Round(time.Millisecond)
	if age < 0 {
		age = 0
	}
	snapshot := *state.lastSnapshot
	exact, stale, unavailable := 0, 0, 0
	for _, component := range snapshot.Components {
		switch component.Availability {
		case telemetry.AvailabilityExact:
			exact++
		case telemetry.AvailabilityUnavailable:
			unavailable++
		default:
			stale++
		}
	}
	lines := []string{
		fmt.Sprintf("monitor narrow width=%d view=%s pause=%v age=%s", width, state.view, state.paused, age),
		fmt.Sprintf("components exact=%d stale_or_estimated=%d unavailable=%d", exact, stale, unavailable),
		"keys: 1..6 s S space r q",
	}
	if width > 0 {
		for index := range lines {
			lines[index] = ui.Truncate(lines[index], width)
		}
	}
	return lines
}

func (state monitorWatchState) headerLine(width int, now time.Time) string {
	age := now.Sub(state.lastCapturedAt).Round(100 * time.Millisecond)
	if age < 0 {
		age = 0
	}
	reset := "none"
	if state.windowResetLabel != "" {
		reset = state.windowResetLabel + "@" + state.windowResetAt.Format(time.RFC3339)
	}
	return fmt.Sprintf("monitor watch view=%s sort=%s desc=%v paused=%v sample_age=%s retention=15m windows=1m,5m,15m reset=%s width=%d", state.view, state.sortKey, state.sortDesc, state.paused, age, reset, width)
}

func (state monitorWatchState) windowLines(snapshot telemetry.MonitorSnapshot) []string {
	windows := []struct {
		name string
		dur  time.Duration
	}{{name: "1m", dur: time.Minute}, {name: "5m", dur: 5 * time.Minute}, {name: "15m", dur: 15 * time.Minute}}
	windowByName := map[string]time.Duration{}
	for _, window := range windows {
		windowByName[window.name] = window.dur
	}
	lines := []string{"window summaries:"}
	switch state.view {
	case monitorViewOverview, monitorViewStatus:
		for _, component := range snapshot.Components {
			for _, metric := range component.Metrics {
				if metric.Kind != telemetry.MetricCounter {
					continue
				}
				identity := component.Component + "." + monitorMetricIdentity(metric)
				entries := make([]string, 0, len(windows))
				for _, window := range windows {
					stats := state.counterWindow(component.Component, metric, windowByName[window.name])
					if stats.state != "" {
						entries = append(entries, window.name+":unavailable("+stats.state+")")
						continue
					}
					entries = append(entries, fmt.Sprintf("%s:%.2f", window.name, stats.rate))
				}
				lines = append(lines, fmt.Sprintf("  rate %s %s %s/s", identity, strings.Join(entries, " "), metric.Unit))
			}
		}
	case monitorViewLatency:
		for _, component := range snapshot.Components {
			for _, metric := range component.Metrics {
				if metric.Kind != telemetry.MetricHistogram {
					continue
				}
				identity := component.Component + "." + monitorMetricIdentity(metric)
				entries := make([]string, 0, len(windows))
				for _, window := range windows {
					stats := state.histogramWindow(component.Component, metric, windowByName[window.name])
					if stats.state != "" {
						entries = append(entries, window.name+":"+stats.state)
						continue
					}
					entries = append(entries, fmt.Sprintf("%s:p50<=%s,p95<=%s,p99<=%s", window.name, stats.p50, stats.p95, stats.p99))
				}
				lines = append(lines, fmt.Sprintf("  latency %s %s %s", identity, strings.Join(entries, " "), metric.Unit))
			}
		}
	default:
		lines = append(lines, "  rates and percentiles are available in overview and latency views")
	}
	return lines
}

func (state monitorWatchState) counterWindow(component string, metric telemetry.Metric, window time.Duration) monitorMetricWindow {
	if metric.Availability != telemetry.AvailabilityExact {
		return monitorMetricWindow{state: monitorWindowAvailabilityState(metric.Availability)}
	}
	baseline, seconds, status := state.windowBaseline(component, metric, window)
	if status != "" || seconds <= 0 {
		return monitorMetricWindow{state: status}
	}
	if metric.Value < baseline.Value {
		return monitorMetricWindow{state: "reset"}
	}
	rate := float64(metric.Value-baseline.Value) / seconds
	return monitorMetricWindow{rate: rate}
}

func (state monitorWatchState) histogramWindow(component string, metric telemetry.Metric, window time.Duration) monitorMetricWindow {
	if metric.Availability != telemetry.AvailabilityExact {
		return monitorMetricWindow{state: monitorWindowAvailabilityState(metric.Availability)}
	}
	baseline, _, status := state.windowBaseline(component, metric, window)
	if status != "" {
		return monitorMetricWindow{state: status}
	}
	delta, valid := histogramDelta(baseline, metric, true)
	if !valid {
		return monitorMetricWindow{state: "reset"}
	}
	return monitorMetricWindow{
		p50: strconv.FormatUint(histogramPercentile(metric.BucketUpper, delta, 50), 10),
		p95: strconv.FormatUint(histogramPercentile(metric.BucketUpper, delta, 95), 10),
		p99: strconv.FormatUint(histogramPercentile(metric.BucketUpper, delta, 99), 10),
	}
}

func (state monitorWatchState) windowBaseline(component string, current telemetry.Metric, window time.Duration) (telemetry.Metric, float64, string) {
	if state.lastSnapshot == nil {
		return telemetry.Metric{}, 0, "warming"
	}
	latestComponent, latestFound := monitorFindComponent(*state.lastSnapshot, component)
	if !latestFound {
		return telemetry.Metric{}, 0, "unavailable"
	}
	if latestComponent.Availability != telemetry.AvailabilityExact || latestComponent.Stale {
		return telemetry.Metric{}, 0, monitorWindowComponentState(latestComponent)
	}
	if latestComponent.ProcessStartID == "unavailable" || latestComponent.ProcessStartID == "legacy" {
		return telemetry.Metric{}, 0, "reset"
	}
	latestMS := latestComponent.CapturedUnixMS
	windowStartMS := latestMS - window.Milliseconds()
	localWindowStartMS := state.lastSnapshot.CapturedUnixMS - window.Milliseconds()
	var before *telemetry.Metric
	var beforeCaptured int64
	var within *telemetry.Metric
	var withinCaptured int64
	var prior *telemetry.Metric
	for _, sample := range state.windowStore.samples {
		componentSample, found := monitorFindComponent(sample.snapshot, component)
		if !found {
			if sample.snapshot.CapturedUnixMS > localWindowStartMS {
				return telemetry.Metric{}, 0, "unavailable"
			}
			continue
		}
		if latestComponent.ProcessStartID != componentSample.ProcessStartID {
			if sample.snapshot.CapturedUnixMS > localWindowStartMS {
				return telemetry.Metric{}, 0, "reset"
			}
			continue
		}
		if componentSample.CapturedUnixMS >= latestMS {
			continue
		}
		if componentSample.Availability != telemetry.AvailabilityExact || componentSample.Stale {
			if sample.snapshot.CapturedUnixMS > localWindowStartMS {
				return telemetry.Metric{}, 0, monitorWindowComponentState(componentSample)
			}
			continue
		}
		metricSample, metricFound := monitorFindMetric(componentSample, current)
		if !metricFound {
			if sample.snapshot.CapturedUnixMS > localWindowStartMS {
				return telemetry.Metric{}, 0, "unavailable"
			}
			continue
		}
		if metricSample.Availability != telemetry.AvailabilityExact {
			if sample.snapshot.CapturedUnixMS > localWindowStartMS {
				return telemetry.Metric{}, 0, monitorWindowAvailabilityState(metricSample.Availability)
			}
			continue
		}
		candidate := metricSample
		if componentSample.CapturedUnixMS <= windowStartMS {
			before = &candidate
			beforeCaptured = componentSample.CapturedUnixMS
		}
		if componentSample.CapturedUnixMS > windowStartMS && within == nil {
			within = &candidate
			withinCaptured = componentSample.CapturedUnixMS
		}
		if sample.snapshot.CapturedUnixMS < localWindowStartMS-monitorWindowResolution.Milliseconds() {
			continue
		}
		if prior != nil && monitorMetricRegressed(*prior, metricSample) {
			return telemetry.Metric{}, 0, "reset"
		}
		prior = &candidate
	}
	if before != nil && windowStartMS-beforeCaptured <= monitorWindowResolution.Milliseconds() {
		seconds := float64(latestMS-beforeCaptured) / 1000
		return *before, seconds, ""
	}
	if within != nil {
		seconds := float64(latestMS-withinCaptured) / 1000
		return *within, seconds, ""
	}
	return telemetry.Metric{}, 0, "warming"
}

func monitorWindowComponentState(component telemetry.ComponentSnapshot) string {
	if component.Stale {
		return string(telemetry.AvailabilityStale)
	}
	return monitorWindowAvailabilityState(component.Availability)
}

func monitorWindowAvailabilityState(availability telemetry.Availability) string {
	if availability == "" || availability == telemetry.AvailabilityExact {
		return string(telemetry.AvailabilityUnavailable)
	}
	return string(availability)
}

func monitorMetricRegressed(previous, current telemetry.Metric) bool {
	if current.Kind == telemetry.MetricCounter {
		return current.Value < previous.Value
	}
	if current.Kind != telemetry.MetricHistogram || len(current.BucketCounts) != len(previous.BucketCounts) {
		return true
	}
	for index := range current.BucketCounts {
		if current.BucketCounts[index] < previous.BucketCounts[index] {
			return true
		}
	}
	return false
}

func monitorFindComponent(snapshot telemetry.MonitorSnapshot, name string) (telemetry.ComponentSnapshot, bool) {
	for _, component := range snapshot.Components {
		if component.Component == name {
			return component, true
		}
	}
	return telemetry.ComponentSnapshot{}, false
}

func monitorFindMetric(component telemetry.ComponentSnapshot, target telemetry.Metric) (telemetry.Metric, bool) {
	targetID := monitorMetricIdentity(target)
	for _, metric := range component.Metrics {
		if metric.Kind == target.Kind && monitorMetricIdentity(metric) == targetID {
			return metric, true
		}
	}
	return telemetry.Metric{}, false
}

func monitorSortSnapshot(snapshot *telemetry.MonitorSnapshot, view, sortKey string, desc bool) {
	for index := range snapshot.Components {
		component := &snapshot.Components[index]
		switch view {
		case monitorViewStorage:
			sort.Slice(component.Storage, func(left, right int) bool {
				leftItem, rightItem := component.Storage[left], component.Storage[right]
				cmp := 0
				switch sortKey {
				case "objects":
					cmp = monitorCompareAvailableUint(leftItem.ObjectCount, monitorInheritedAvailability(component.Availability, leftItem.ObjectCountAvailability), rightItem.ObjectCount, monitorInheritedAvailability(component.Availability, rightItem.ObjectCountAvailability), desc)
				case "physical":
					cmp = monitorCompareAvailableUint(leftItem.PhysicalBytes, monitorInheritedAvailability(component.Availability, leftItem.PhysicalAvailability), rightItem.PhysicalBytes, monitorInheritedAvailability(component.Availability, rightItem.PhysicalAvailability), desc)
				default:
					cmp = strings.Compare(leftItem.BackendID, rightItem.BackendID)
				}
				if cmp == 0 {
					cmp = strings.Compare(leftItem.BackendID, rightItem.BackendID)
				}
				if desc && sortKey == "backend" {
					cmp = -cmp
				}
				return cmp < 0
			})
		case monitorViewOperations:
			sort.Slice(component.Operations, func(left, right int) bool {
				leftItem, rightItem := component.Operations[left], component.Operations[right]
				cmp := 0
				switch sortKey {
				case "progress":
					cmp = monitorCompareAvailableFloat(monitorOperationProgress(leftItem), component.Availability, monitorOperationProgress(rightItem), component.Availability, desc)
				case "class":
					cmp = strings.Compare(leftItem.Class, rightItem.Class)
				default:
					cmp = cmpInt64(rightItem.StartedUnixMS, leftItem.StartedUnixMS)
				}
				if cmp == 0 {
					cmp = strings.Compare(leftItem.ID, rightItem.ID)
				}
				if desc && sortKey != "progress" {
					cmp = -cmp
				}
				return cmp < 0
			})
		case monitorViewCaches:
			sort.Slice(component.Caches, func(left, right int) bool {
				leftItem, rightItem := component.Caches[left], component.Caches[right]
				cmp := 0
				switch sortKey {
				case "used":
					cmp = monitorCompareAvailableUint(leftItem.UsedBytes, monitorInheritedAvailability(component.Availability, leftItem.Availability), rightItem.UsedBytes, monitorInheritedAvailability(component.Availability, rightItem.Availability), desc)
				case "hit_ratio":
					cmp = monitorCompareAvailableFloat(monitorHitRatio(leftItem), monitorCacheHitRatioAvailability(component.Availability, leftItem), monitorHitRatio(rightItem), monitorCacheHitRatioAvailability(component.Availability, rightItem), desc)
				default:
					cmp = strings.Compare(leftItem.ID, rightItem.ID)
				}
				if cmp == 0 {
					cmp = strings.Compare(leftItem.ID, rightItem.ID)
				}
				if desc && sortKey == "id" {
					cmp = -cmp
				}
				return cmp < 0
			})
		case monitorViewLatency:
			sort.Slice(component.Metrics, func(left, right int) bool {
				leftMetric, rightMetric := component.Metrics[left], component.Metrics[right]
				cmp := 0
				switch sortKey {
				default:
					cmp = strings.Compare(monitorMetricIdentity(leftMetric), monitorMetricIdentity(rightMetric))
				}
				if desc {
					cmp = -cmp
				}
				return cmp < 0
			})
		}
	}
	sort.Slice(snapshot.Components, func(left, right int) bool {
		leftComponent, rightComponent := snapshot.Components[left], snapshot.Components[right]
		cmp := 0
		switch view {
		case monitorViewWAL:
			if sortKey == "retained" {
				leftRetained, rightRetained := uint64(0), uint64(0)
				leftAvailability, rightAvailability := telemetry.AvailabilityNotApplicable, telemetry.AvailabilityNotApplicable
				if leftComponent.WAL != nil {
					leftRetained = leftComponent.WAL.RetainedBytes
					leftAvailability = monitorInheritedAvailability(leftComponent.Availability, leftComponent.WAL.Availability)
				}
				if rightComponent.WAL != nil {
					rightRetained = rightComponent.WAL.RetainedBytes
					rightAvailability = monitorInheritedAvailability(rightComponent.Availability, rightComponent.WAL.Availability)
				}
				cmp = monitorCompareAvailableUint(leftRetained, leftAvailability, rightRetained, rightAvailability, desc)
			} else {
				cmp = strings.Compare(leftComponent.Component, rightComponent.Component)
			}
		case monitorViewOverview, monitorViewStatus:
			switch sortKey {
			case "stale":
				cmp = cmpBool(leftComponent.Stale || leftComponent.Availability != telemetry.AvailabilityExact, rightComponent.Stale || rightComponent.Availability != telemetry.AvailabilityExact)
			case "metrics":
				cmp = cmpInt64(int64(len(leftComponent.Metrics)), int64(len(rightComponent.Metrics)))
			default:
				cmp = strings.Compare(leftComponent.Component, rightComponent.Component)
			}
		default:
			cmp = strings.Compare(leftComponent.Component, rightComponent.Component)
		}
		if cmp == 0 {
			cmp = strings.Compare(leftComponent.ProcessStartID, rightComponent.ProcessStartID)
		}
		if desc && !(view == monitorViewWAL && sortKey == "retained") {
			cmp = -cmp
		}
		return cmp < 0
	})
}

func monitorOperationProgress(operation telemetry.ActiveOperation) float64 {
	if operation.ExpectedUnits == 0 {
		return 0
	}
	return float64(operation.CompletedUnits) / float64(operation.ExpectedUnits)
}

func monitorCacheHitRatioAvailability(componentAvailability telemetry.Availability, cache telemetry.CacheSnapshot) telemetry.Availability {
	availability := monitorInheritedAvailability(componentAvailability, cache.TrafficAvailability)
	if availability == telemetry.AvailabilityExact && cache.Hits+cache.Misses == 0 {
		return telemetry.AvailabilityUnavailable
	}
	return availability
}

func monitorCompareAvailableUint(left uint64, leftAvailability telemetry.Availability, right uint64, rightAvailability telemetry.Availability, desc bool) int {
	return monitorCompareAvailableFloat(float64(left), leftAvailability, float64(right), rightAvailability, desc)
}

func monitorCompareAvailableFloat(left float64, leftAvailability telemetry.Availability, right float64, rightAvailability telemetry.Availability, desc bool) int {
	leftRank, rightRank := monitorAvailabilityRank(leftAvailability), monitorAvailabilityRank(rightAvailability)
	if leftRank != rightRank {
		return cmpInt64(int64(leftRank), int64(rightRank))
	}
	cmp := cmpFloat64(left, right)
	if desc {
		cmp = -cmp
	}
	return cmp
}

func monitorAvailabilityRank(availability telemetry.Availability) int {
	switch availability {
	case telemetry.AvailabilityExact:
		return 0
	case telemetry.AvailabilityEstimated:
		return 1
	case telemetry.AvailabilityStale:
		return 2
	default:
		return 3
	}
}

func (state monitorWatchState) sortSnapshot(snapshot *telemetry.MonitorSnapshot) {
	monitorSortSnapshot(snapshot, state.view, state.sortKey, state.sortDesc)
	if state.view != monitorViewLatency || state.sortKey != "p95" {
		return
	}
	for componentIndex := range snapshot.Components {
		component := &snapshot.Components[componentIndex]
		sort.Slice(component.Metrics, func(left, right int) bool {
			leftP95, leftAvailable := state.monitorWindowP95(component.Component, component.Metrics[left])
			rightP95, rightAvailable := state.monitorWindowP95(component.Component, component.Metrics[right])
			cmp := 0
			if leftAvailable != rightAvailable {
				if leftAvailable {
					cmp = -1
				} else {
					cmp = 1
				}
			} else if leftAvailable {
				cmp = cmpUint64(leftP95, rightP95)
				if state.sortDesc {
					cmp = -cmp
				}
			}
			if cmp == 0 {
				cmp = strings.Compare(monitorMetricIdentity(component.Metrics[left]), monitorMetricIdentity(component.Metrics[right]))
				if state.sortDesc {
					cmp = -cmp
				}
			}
			return cmp < 0
		})
	}
}

func (state monitorWatchState) monitorWindowP95(component string, metric telemetry.Metric) (uint64, bool) {
	stats := state.histogramWindow(component, metric, time.Minute)
	if stats.state != "" {
		return 0, false
	}
	value, err := strconv.ParseUint(stats.p95, 10, 64)
	return value, err == nil
}

func monitorHitRatio(cache telemetry.CacheSnapshot) float64 {
	total := cache.Hits + cache.Misses
	if total == 0 {
		return 0
	}
	return float64(cache.Hits) / float64(total)
}

func cmpUint64(left, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func cmpInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func cmpFloat64(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func cmpBool(left, right bool) int {
	switch {
	case !left && right:
		return -1
	case left && !right:
		return 1
	default:
		return 0
	}
}

type monitorKeyPoller interface {
	Poll() ([]byte, error)
	Close() error
}

func newMonitorKeyPoller(reader io.ReadCloser) (monitorKeyPoller, error) {
	poller, err := newPlatformMonitorKeyPoller(reader)
	if err != nil {
		return nil, err
	}
	if poller == nil {
		return nil, fmt.Errorf("monitor watch keyboard input is unsupported on this platform")
	}
	return poller, nil
}
