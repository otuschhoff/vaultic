package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/ui"
)

func TestMonitorWindowRatesAndPercentiles(t *testing.T) {
	state := monitorWatchState{}
	start := time.Unix(1, 0)
	samples := []struct {
		offset  time.Duration
		counter uint64
		counts  []uint64
	}{
		{offset: 0, counter: 100, counts: []uint64{1, 5, 10}},
		{offset: 10 * time.Minute, counter: 700, counts: []uint64{2, 10, 20}},
		{offset: 15 * time.Minute, counter: 1000, counts: []uint64{4, 20, 40}},
	}
	for _, sample := range samples {
		snapshot := monitorTestSnapshot(start.Add(sample.offset), sample.counter, sample.counts)
		state.windowStore.Add(start.Add(sample.offset), snapshot)
		state.lastSnapshot = &snapshot
		state.lastCapturedAt = start.Add(sample.offset)
	}
	latest := state.lastSnapshot.Components[0].Metrics[0]
	rate1 := state.counterWindow("vaultic", latest, time.Minute)
	if rate1.state != "warming" {
		t.Fatalf("1m window should warm when no baseline is available: %+v", rate1)
	}
	rate5 := state.counterWindow("vaultic", latest, 5*time.Minute)
	if rate5.state != "" {
		t.Fatalf("5m window unexpectedly reset: %+v", rate5)
	}
	if rate5.rate != 1.0 {
		t.Fatalf("5m rate = %.2f", rate5.rate)
	}
	rate15 := state.counterWindow("vaultic", latest, 15*time.Minute)
	if rate15.state != "" {
		t.Fatalf("15m window unexpectedly reset: %+v", rate15)
	}
	histMetric := state.lastSnapshot.Components[0].Metrics[1]
	hist := state.histogramWindow("vaultic", histMetric, 5*time.Minute)
	if hist.state != "" || hist.p95 != "1000" {
		t.Fatalf("histogram window = %+v", hist)
	}
}

func TestMonitorWatchDefaultsToOverview(t *testing.T) {
	command := newMonitorWatchCommand(&global.Options{})
	if got := command.Flags().Lookup("view").DefValue; got != monitorViewOverview {
		t.Fatalf("default view = %q", got)
	}
}

func TestMonitorWindowMarksProcessAndCounterResets(t *testing.T) {
	start := time.Unix(1, 0)
	state := monitorWatchState{}
	first := monitorTestSnapshot(start, 100, []uint64{1, 5, 10})
	state.windowStore.Add(start, first)
	restarted := monitorTestSnapshot(start.Add(time.Minute), 2, []uint64{0, 1, 1})
	restarted.Components[0].ProcessStartID = "proc-two"
	state.windowStore.Add(start.Add(time.Minute), restarted)
	state.lastSnapshot = &restarted
	if got := state.counterWindow("vaultic", restarted.Components[0].Metrics[0], 5*time.Minute); got.state != "reset" {
		t.Fatalf("process reset window = %+v", got)
	}

	state.windowStore.Reset()
	state.windowStore.Add(start, first)
	regressed := monitorTestSnapshot(start.Add(time.Minute), 2, []uint64{0, 1, 1})
	state.windowStore.Add(start.Add(time.Minute), regressed)
	state.lastSnapshot = &regressed
	if got := state.counterWindow("vaultic", regressed.Components[0].Metrics[0], 5*time.Minute); got.state != "reset" {
		t.Fatalf("counter reset window = %+v", got)
	}
}

func TestMonitorWindowDoesNotBridgeUnavailableSample(t *testing.T) {
	start := time.Unix(1, 0)
	state := monitorWatchState{}
	first := monitorTestSnapshot(start, 100, []uint64{1, 5, 10})
	state.windowStore.Add(start, first)
	unavailable := monitorTestSnapshot(start.Add(5*time.Second), 100, []uint64{1, 5, 10})
	unavailable.Components[0].Availability = telemetry.AvailabilityUnavailable
	unavailable.Components[0].Stale = true
	state.windowStore.Add(start.Add(5*time.Second), unavailable)
	latest := monitorTestSnapshot(start.Add(time.Minute), 160, []uint64{2, 10, 20})
	state.windowStore.Add(start.Add(time.Minute), latest)
	state.lastSnapshot = &latest
	if got := state.counterWindow("vaultic", latest.Components[0].Metrics[0], time.Minute); got.state != "stale" {
		t.Fatalf("window bridged unavailable sample: %+v", got)
	}
}

func TestMonitorWindowRetainsMetricResetInsideAnchorResolution(t *testing.T) {
	start := time.Unix(1, 0)
	state := monitorWatchState{}
	first := monitorTestSnapshot(start, 100, []uint64{1, 5, 10})
	state.windowStore.Add(start, first)
	reset := monitorTestSnapshot(start.Add(5*time.Second), 1, []uint64{0, 0, 0})
	state.windowStore.Add(start.Add(5*time.Second), reset)
	recovered := monitorTestSnapshot(start.Add(time.Minute), 60, []uint64{1, 2, 3})
	state.windowStore.Add(start.Add(time.Minute), recovered)
	state.lastSnapshot = &recovered
	if got := state.counterWindow("vaultic", recovered.Components[0].Metrics[0], time.Minute); got.state != "reset" {
		t.Fatalf("window lost intra-anchor reset: %+v", got)
	}
}

func TestMonitorWindowResetExpiresOutsideRequestedWindow(t *testing.T) {
	start := time.Unix(1, 0)
	state := monitorWatchState{}
	for _, sample := range []struct {
		offset  time.Duration
		counter uint64
	}{{0, 100}, {time.Minute, 1}, {10 * time.Minute, 541}, {11 * time.Minute, 601}} {
		snapshot := monitorTestSnapshot(start.Add(sample.offset), sample.counter, []uint64{1, 5, 10})
		state.windowStore.Add(start.Add(sample.offset), snapshot)
		state.lastSnapshot = &snapshot
	}
	if got := state.counterWindow("vaultic", state.lastSnapshot.Components[0].Metrics[0], time.Minute); got.state != "" || got.rate != 1 {
		t.Fatalf("expired reset affected 1m window: %+v", got)
	}
	if got := state.counterWindow("vaultic", state.lastSnapshot.Components[0].Metrics[0], 15*time.Minute); got.state != "reset" {
		t.Fatalf("15m window missed retained reset: %+v", got)
	}
}

func TestMonitorWindowUsesComponentCaptureTimes(t *testing.T) {
	start := time.Unix(1, 0)
	state := monitorWatchState{}
	first := monitorTestSnapshot(start, 100, []uint64{1, 5, 10})
	first.CapturedUnixMS = start.Add(-30 * time.Second).UnixMilli()
	state.windowStore.Add(start, first)
	latest := monitorTestSnapshot(start.Add(time.Minute), 160, []uint64{2, 10, 20})
	latest.CapturedUnixMS = start.Add(30 * time.Second).UnixMilli()
	state.windowStore.Add(start.Add(time.Minute), latest)
	state.lastSnapshot = &latest
	got := state.counterWindow("vaultic", latest.Components[0].Metrics[0], time.Minute)
	if got.state != "" || got.rate != 1 {
		t.Fatalf("component-clock rate = %+v", got)
	}
}

func TestMonitorWindowScopesUnavailableTransitionByLocalCaptureTime(t *testing.T) {
	start := time.Unix(1, 0)
	state := monitorWatchState{}
	first := monitorTestSnapshot(start, 100, []uint64{1, 5, 10})
	first.Components[0].CapturedUnixMS = start.Add(-time.Hour).UnixMilli()
	state.windowStore.Add(start, first)
	unavailable := monitorTestSnapshot(start.Add(30*time.Second), 100, []uint64{1, 5, 10})
	unavailable.Components[0].CapturedUnixMS = start.Add(-time.Hour + 30*time.Second).UnixMilli()
	unavailable.Components[0].Availability = telemetry.AvailabilityUnavailable
	unavailable.Components[0].Stale = true
	state.windowStore.Add(start.Add(30*time.Second), unavailable)
	latest := monitorTestSnapshot(start.Add(time.Minute), 160, []uint64{2, 10, 20})
	latest.Components[0].CapturedUnixMS = start.Add(-time.Hour + time.Minute).UnixMilli()
	state.windowStore.Add(start.Add(time.Minute), latest)
	state.lastSnapshot = &latest
	if got := state.counterWindow("vaultic", latest.Components[0].Metrics[0], time.Minute); got.state != "stale" {
		t.Fatalf("clock skew hid unavailable transition: %+v", got)
	}
}

func TestMonitorWindowRetentionBoundedToFifteenMinutes(t *testing.T) {
	store := monitorWindowStore{}
	start := time.Unix(1, 0)
	for minute := 0; minute <= 16; minute++ {
		at := start.Add(time.Duration(minute) * time.Minute)
		store.Add(at, monitorTestSnapshot(at, uint64(minute), []uint64{1, 1, 1}))
	}
	if len(store.samples) != 17 {
		t.Fatalf("retained samples = %d", len(store.samples))
	}
	if got := store.samples[0].capturedAt; got != start {
		t.Fatalf("oldest retained sample = %s", got)
	}
	for sample := 0; sample < 20_000; sample++ {
		at := start.Add(17*time.Minute + time.Duration(sample)*100*time.Millisecond)
		snapshot := monitorTestSnapshot(at, uint64(sample), []uint64{1, 1, 1})
		snapshot.Components[0].Storage = []telemetry.StorageSnapshot{{BackendID: "must-not-be-retained"}}
		store.Add(at, snapshot)
	}
	if len(store.samples) > maxMonitorWindowSamples {
		t.Fatalf("retained samples = %d, limit = %d", len(store.samples), maxMonitorWindowSamples)
	}
	for _, sample := range store.samples {
		if len(sample.snapshot.Components[0].Storage) != 0 {
			t.Fatal("window history retained non-metric snapshot payload")
		}
	}
}

func TestMonitorApplyKeysCoversViewsPauseResetAndSort(t *testing.T) {
	state := monitorWatchState{view: monitorViewStatus, sortKey: "component"}
	state.windowStore.samples = []monitorWindowSample{{capturedAt: time.Unix(1, 0)}}
	now := time.Unix(10, 0)
	quit := monitorApplyKeys(&state, []byte{'2', 's', 'S', ' ', 'r', '6', 'q'}, now)
	if !quit {
		t.Fatal("q key did not exit")
	}
	if state.view != monitorViewLatency {
		t.Fatalf("view = %s", state.view)
	}
	if !state.sortDesc {
		t.Fatalf("sortDesc = %v", state.sortDesc)
	}
	if !state.paused {
		t.Fatalf("paused = %v", state.paused)
	}
	if state.windowResetLabel != "manual" || state.windowResetAt != now || len(state.windowStore.samples) != 0 {
		t.Fatalf("reset state = %+v", state)
	}
}

func TestMonitorApplyKeysTreatsControlCAsQuit(t *testing.T) {
	if !monitorApplyKeys(&monitorWatchState{}, []byte{0x03}, time.Time{}) {
		t.Fatal("control-c did not quit raw-mode watch")
	}
}

func TestMonitorSortSnapshotAppliesRequestedOrdering(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 2_000, Availability: telemetry.AvailabilityExact,
		Storage: []telemetry.StorageSnapshot{{BackendID: "a", ObjectCount: 1}, {BackendID: "z", ObjectCount: 9}},
		Caches:  []telemetry.CacheSnapshot{{ID: "first", Hits: 1, Misses: 9}, {ID: "second", Hits: 9, Misses: 1}},
	})
	monitorSortSnapshot(&snapshot, monitorViewStorage, "objects", true)
	if snapshot.Components[0].Storage[0].BackendID != "z" {
		t.Fatalf("storage sort = %+v", snapshot.Components[0].Storage)
	}
	monitorSortSnapshot(&snapshot, monitorViewCaches, "hit_ratio", true)
	if snapshot.Components[0].Caches[0].ID != "second" {
		t.Fatalf("cache sort = %+v", snapshot.Components[0].Caches)
	}
}

func TestMonitorSortUsesDisplayedAgeAndAvailability(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(20, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 20_000, Availability: telemetry.AvailabilityExact,
		Operations: []telemetry.ActiveOperation{{ID: "new", StartedUnixMS: 15_000}, {ID: "old", StartedUnixMS: 5_000}},
		Storage: []telemetry.StorageSnapshot{
			{BackendID: "unavailable", ObjectCount: 100, ObjectCountAvailability: telemetry.AvailabilityUnavailable},
			{BackendID: "exact", ObjectCount: 1, ObjectCountAvailability: telemetry.AvailabilityExact},
		},
	})
	monitorSortSnapshot(&snapshot, monitorViewOperations, "age", false)
	if snapshot.Components[0].Operations[0].ID != "new" {
		t.Fatalf("age sort = %+v", snapshot.Components[0].Operations)
	}
	monitorSortSnapshot(&snapshot, monitorViewStorage, "objects", true)
	if snapshot.Components[0].Storage[0].BackendID != "exact" {
		t.Fatalf("availability sort = %+v", snapshot.Components[0].Storage)
	}
}

func TestMonitorCacheSortRanksEmptyRatioAfterMeasuredZero(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact,
		Caches: []telemetry.CacheSnapshot{
			{ID: "empty", TrafficAvailability: telemetry.AvailabilityExact},
			{ID: "measured-zero", Misses: 1, TrafficAvailability: telemetry.AvailabilityExact},
		},
	})
	monitorSortSnapshot(&snapshot, monitorViewCaches, "hit_ratio", false)
	if snapshot.Components[0].Caches[0].ID != "measured-zero" {
		t.Fatalf("cache ratio availability sort = %+v", snapshot.Components[0].Caches)
	}
}

func TestMonitorCacheViewDisplaysSortedHitRatio(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact,
		Caches: []telemetry.CacheSnapshot{{ID: "objects", Hits: 3, Misses: 1, Availability: telemetry.AvailabilityExact, TrafficAvailability: telemetry.AvailabilityExact}},
	})
	state := monitorWatchState{view: monitorViewCaches, sortKey: "hit_ratio", lastSnapshot: &snapshot, lastCapturedAt: time.Unix(1, 0)}
	output := strings.Join(state.render(160, time.Unix(2, 0)), "\n")
	if !strings.Contains(output, "hit_ratio=75.0%") {
		t.Fatalf("cache view omitted hit ratio: %s", output)
	}
}

func TestMonitorSortSnapshotAppliesOverviewOrdering(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0),
		telemetry.ComponentSnapshot{Component: "vaultic", ProcessStartID: "a", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact, Metrics: []telemetry.Metric{{Name: "requests"}}},
		telemetry.ComponentSnapshot{Component: "vaulticdb", ProcessStartID: "b", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityStale, Stale: true},
		telemetry.ComponentSnapshot{Component: "key_broker", ProcessStartID: "c", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact, Metrics: []telemetry.Metric{{Name: "one"}, {Name: "two"}}},
	)
	monitorSortSnapshot(&snapshot, monitorViewOverview, "metrics", true)
	if snapshot.Components[0].Component != "key_broker" {
		t.Fatalf("metrics sort = %+v", snapshot.Components)
	}
	monitorSortSnapshot(&snapshot, monitorViewOverview, "stale", true)
	if snapshot.Components[0].Component != "vaulticdb" {
		t.Fatalf("stale sort = %+v", snapshot.Components)
	}
}

func TestMonitorLatencySortUsesDisplayedOneMinuteP95(t *testing.T) {
	start := time.Unix(1, 0)
	baseline := monitorTestSnapshot(start, 0, []uint64{0, 0, 0})
	baseline.Components[0].Metrics = []telemetry.Metric{
		{Name: "fast", Kind: telemetry.MetricHistogram, Unit: "microseconds", Availability: telemetry.AvailabilityExact, BucketUpper: []uint64{10, 100, 1000}, BucketCounts: []uint64{0, 0, 0}},
		{Name: "slow", Kind: telemetry.MetricHistogram, Unit: "microseconds", Availability: telemetry.AvailabilityExact, BucketUpper: []uint64{10, 100, 1000}, BucketCounts: []uint64{0, 0, 0}},
	}
	current := monitorTestSnapshot(start.Add(time.Minute), 0, []uint64{0, 0, 0})
	current.Components[0].Metrics = []telemetry.Metric{
		{Name: "fast", Kind: telemetry.MetricHistogram, Unit: "microseconds", Availability: telemetry.AvailabilityExact, BucketUpper: []uint64{10, 100, 1000}, BucketCounts: []uint64{9, 10, 10}},
		{Name: "slow", Kind: telemetry.MetricHistogram, Unit: "microseconds", Availability: telemetry.AvailabilityExact, BucketUpper: []uint64{10, 100, 1000}, BucketCounts: []uint64{0, 1, 10}},
	}
	state := monitorWatchState{view: monitorViewLatency, sortKey: "p95", sortDesc: true, lastSnapshot: &current}
	state.windowStore.Add(start, baseline)
	state.windowStore.Add(start.Add(time.Minute), current)
	state.sortSnapshot(&current)
	if current.Components[0].Metrics[0].Name != "slow" {
		t.Fatalf("latency sort = %+v", current.Components[0].Metrics)
	}
}

func TestWatchMonitorLoopPauseResumeAndCancel(t *testing.T) {
	collectTick := make(chan time.Time, 4)
	renderTick := make(chan time.Time, 4)
	inputTick := make(chan time.Time, 4)
	keys := make(chan []byte, 4)
	statuses := make(chan []string, 8)
	collectCalls := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := monitorWatchDeps{
		now:   func() time.Time { return time.Unix(20, 0) },
		width: func() int { return 140 },
		collect: func(context.Context) (telemetry.MonitorSnapshot, error) {
			collectCalls++
			return monitorTestSnapshot(time.Unix(20, 0), uint64(collectCalls), []uint64{1, 1, 1}), nil
		},
		setStatus: func(lines []string) { statuses <- append([]string(nil), lines...) },
		pollKeys: func() ([]byte, error) {
			select {
			case k := <-keys:
				return k, nil
			default:
				return nil, nil
			}
		},
		collectTick: collectTick,
		renderTick:  renderTick,
		inputTick:   inputTick,
	}
	done := make(chan error, 1)
	go func() {
		done <- runMonitorWatchLoop(ctx, &global.Options{}, monitorOptions{Interval: time.Second, View: "overview"}, deps)
	}()
	<-statuses
	<-statuses
	if collectCalls != 1 {
		t.Fatalf("initial collect calls = %d", collectCalls)
	}
	keys <- []byte{' '}
	inputTick <- time.Now()
	<-statuses
	keys <- []byte{' '}
	inputTick <- time.Now()
	<-statuses
	collectTick <- time.Now()
	<-statuses
	keys <- []byte{'r'}
	inputTick <- time.Now()
	for {
		lines := <-statuses
		if strings.Contains(strings.Join(lines, "\n"), "reset=manual") {
			break
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("watch cancel error = %v", err)
	}
}

func TestWatchMonitorLoopRemainsInteractiveWhileCollectionIsBlocked(t *testing.T) {
	collectTick := make(chan time.Time)
	renderTick := make(chan time.Time, 1)
	inputTick := make(chan time.Time, 1)
	keys := make(chan []byte, 1)
	statuses := make(chan []string, 4)
	canceled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runMonitorWatchLoop(ctx, &global.Options{}, monitorOptions{Interval: time.Second, View: "overview"}, monitorWatchDeps{
			now: func() time.Time { return time.Unix(20, 0) }, width: func() int { return 140 },
			collect: func(ctx context.Context) (telemetry.MonitorSnapshot, error) {
				<-ctx.Done()
				close(canceled)
				return telemetry.MonitorSnapshot{}, ctx.Err()
			},
			setStatus: func(lines []string) { statuses <- append([]string(nil), lines...) },
			pollKeys: func() ([]byte, error) {
				select {
				case key := <-keys:
					return key, nil
				default:
					return nil, nil
				}
			},
			collectTick: collectTick, renderTick: renderTick, inputTick: inputTick,
		})
	}()
	if lines := <-statuses; !strings.Contains(strings.Join(lines, "\n"), "waiting for first sample") {
		t.Fatalf("initial status = %q", lines)
	}
	keys <- []byte{'q'}
	inputTick <- time.Now()
	if err := <-done; err != nil {
		t.Fatalf("quit while collecting: %v", err)
	}
	<-canceled
}

func TestWatchRenderNarrowAndResizeResponse(t *testing.T) {
	snapshot := monitorTestSnapshot(time.Unix(30, 0), 10, []uint64{1, 1, 1})
	state := monitorWatchState{view: monitorViewStatus, sortKey: "component", lastSnapshot: &snapshot, lastCapturedAt: time.Unix(30, 0)}
	narrow := strings.Join(state.render(80, time.Unix(31, 0)), "\n")
	if !strings.Contains(narrow, "monitor narrow width=80") {
		t.Fatalf("narrow output = %q", narrow)
	}
	for _, line := range state.render(24, time.Unix(31, 0)) {
		if ui.DisplayWidth(line) > 24 {
			t.Fatalf("narrow line width = %d: %q", ui.DisplayWidth(line), line)
		}
	}
	wide := strings.Join(state.render(140, time.Unix(31, 0)), "\n")
	if strings.Contains(wide, "monitor narrow") || !strings.Contains(wide, "width=140") {
		t.Fatalf("wide output = %q", wide)
	}
	for _, line := range state.render(100, time.Unix(31, 0)) {
		if ui.DisplayWidth(line) > 100 {
			t.Fatalf("wide line width = %d: %q", ui.DisplayWidth(line), line)
		}
	}
}

func TestWatchViewRenderingDoesNotMutateRetainedSnapshot(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact,
		Storage:    []telemetry.StorageSnapshot{{BackendID: "repository", Availability: telemetry.AvailabilityExact}},
		Operations: []telemetry.ActiveOperation{{ID: "backup", Class: "backup"}},
	})
	state := monitorWatchState{view: monitorViewStorage, sortKey: "backend", lastSnapshot: &snapshot, lastCapturedAt: time.Unix(1, 0)}
	state.render(140, time.Unix(2, 0))
	state.view, state.sortKey = monitorViewOperations, "age"
	output := strings.Join(state.render(140, time.Unix(2, 0)), "\n")
	if !strings.Contains(output, "operation backup") {
		t.Fatalf("view switch lost retained operation: %s", output)
	}
}

func TestWatchPauseDiscardsInflightCollectionAndResumeCollectsImmediately(t *testing.T) {
	collectTick := make(chan time.Time)
	renderTick := make(chan time.Time)
	inputTick := make(chan time.Time, 4)
	keys := make(chan []byte, 4)
	statuses := make(chan []string, 8)
	firstRelease := make(chan struct{})
	collectCalls := make(chan int, 4)
	call := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runMonitorWatchLoop(ctx, &global.Options{}, monitorOptions{Interval: time.Second, View: "overview"}, monitorWatchDeps{
			now: func() time.Time { return time.Unix(20, 0) }, width: func() int { return 140 },
			collect: func(context.Context) (telemetry.MonitorSnapshot, error) {
				call++
				current := call
				collectCalls <- current
				if current == 1 {
					<-firstRelease
				}
				return monitorTestSnapshot(time.Unix(20+int64(current), 0), uint64(current), []uint64{1, 1, 1}), nil
			},
			setStatus: func(lines []string) { statuses <- append([]string(nil), lines...) },
			pollKeys: func() ([]byte, error) {
				select {
				case key := <-keys:
					return key, nil
				default:
					return nil, nil
				}
			},
			collectTick: collectTick, renderTick: renderTick, inputTick: inputTick,
		})
	}()
	<-statuses
	<-collectCalls
	keys <- []byte{' '}
	inputTick <- time.Now()
	<-statuses
	close(firstRelease)
	paused := <-statuses
	if !strings.Contains(strings.Join(paused, "\n"), "waiting for first sample") {
		t.Fatalf("paused collection changed display: %q", paused)
	}
	keys <- []byte{' '}
	inputTick <- time.Now()
	<-statuses
	if resumedCall := <-collectCalls; resumedCall != 2 {
		t.Fatalf("resume collection call = %d", resumedCall)
	}
	<-statuses
	keys <- []byte{'q'}
	inputTick <- time.Now()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWatchRenderShowsUnavailableComponents(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0),
		telemetry.ComponentSnapshot{Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact},
		telemetry.ComponentSnapshot{Component: "vaulticdb", ProcessStartID: "unavailable", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityUnavailable, Stale: true},
	)
	state := monitorWatchState{view: monitorViewStatus, sortKey: "component", lastSnapshot: &snapshot, lastCapturedAt: time.Unix(1, 0)}
	output := strings.Join(state.render(140, time.Unix(2, 0)), "\n")
	if !strings.Contains(output, "vaulticdb") || !strings.Contains(output, "availability=unavailable") {
		t.Fatalf("render output = %q", output)
	}
}

func TestCollectMonitorSnapshotReconcileAsyncProgressAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	progress := make(chan time.Time, 2)
	status := make(chan []string, 4)
	started := make(chan struct{}, 1)
	deps := monitorReconcileDeps{
		now:      func() time.Time { return time.Unix(1, 0) },
		progress: progress,
		setStatus: func(lines []string) {
			status <- append([]string(nil), lines...)
		},
		collect: func(ctx context.Context) (telemetry.MonitorSnapshot, error) {
			started <- struct{}{}
			<-ctx.Done()
			return telemetry.MonitorSnapshot{}, ctx.Err()
		},
	}
	done := make(chan error, 1)
	go func() {
		_, err := collectMonitorSnapshotReconcileAsync(ctx, &global.Options{}, monitorOptions{Timeout: time.Second}, deps)
		done <- err
	}()
	<-started
	progress <- time.Unix(1, int64(500*time.Millisecond))
	lines := <-status
	if len(lines) == 0 || !strings.Contains(lines[0], "reconcile running") {
		t.Fatalf("progress lines = %+v", lines)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcile cancel error = %v", err)
	}
}

func TestCollectMonitorSnapshotReconcileAsyncSuppressesNonInteractiveProgress(t *testing.T) {
	progress := make(chan time.Time, 1)
	completed := make(chan struct{})
	term := &nonInteractiveMonitorTerminal{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := collectMonitorSnapshotReconcileAsync(ctx, &global.Options{Term: term}, monitorOptions{Timeout: time.Second}, monitorReconcileDeps{
			now: func() time.Time { return time.Unix(1, 0) }, progress: progress,
			collect: func(ctx context.Context) (telemetry.MonitorSnapshot, error) {
				<-completed
				return telemetry.MonitorSnapshot{}, ctx.Err()
			},
		})
		done <- err
	}()
	progress <- time.Unix(2, 0)
	close(completed)
	cancel()
	<-done
	if len(term.Output) != 0 {
		t.Fatalf("non-interactive progress output = %q", term.Output)
	}
}

type nonInteractiveMonitorTerminal struct{ ui.MockTerminal }

func (*nonInteractiveMonitorTerminal) OutputIsTerminal() bool { return false }

func (*nonInteractiveMonitorTerminal) CanUpdateStatus() bool { return false }

func TestMonitorOverviewHighlightsPressureEffectivenessAndStaleness(t *testing.T) {
	baseline := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact,
		Metrics: []telemetry.Metric{{Name: "engine_memtable_write_bytes", Kind: telemetry.MetricCounter, Unit: "bytes", Availability: telemetry.AvailabilityExact, Value: 100}},
	})
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(61, 0),
		telemetry.ComponentSnapshot{
			Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 61000, Availability: telemetry.AvailabilityExact,
			Metrics:    []telemetry.Metric{{Name: "engine_memtable_write_bytes", Kind: telemetry.MetricCounter, Unit: "bytes", Availability: telemetry.AvailabilityExact, Value: 160}},
			Queues:     []telemetry.QueueSnapshot{{Name: "batch_write", Availability: telemetry.AvailabilityExact, CapacityAvailability: telemetry.AvailabilityExact, Depth: 8, Capacity: 10, Backpressure: "capacity"}},
			Operations: []telemetry.ActiveOperation{{ID: "retry-one", Class: "backup", Phase: "retry", StartedUnixMS: 1}},
			Caches:     []telemetry.CacheSnapshot{{ID: "repository", Availability: telemetry.AvailabilityExact, EffectiveBytes: 100, UsedBytes: 60, ReservedBytes: 10, CacheBytes: 75, OriginBytes: 25, TrafficBytesAvailability: telemetry.AvailabilityExact}},
		},
		telemetry.ComponentSnapshot{Component: "vaulticdb", ProcessStartID: "unavailable", CapturedUnixMS: 61000, Availability: telemetry.AvailabilityUnavailable, Stale: true},
	)
	state := monitorWatchState{view: monitorViewOverview, sortKey: "component", lastSnapshot: &snapshot, lastCapturedAt: time.Unix(61, 0)}
	state.windowStore.Add(time.Unix(1, 0), baseline)
	state.windowStore.Add(time.Unix(61, 0), snapshot)
	output := strings.Join(state.render(140, time.Unix(62, 0)), "\n")
	for _, expected := range []string{"occupancy=70.0%", "byte_hit_ratio=75.0%", "pressure=0.80", "writeback rate_1m=1.00(partial) bytes/s", "active retries=1(partial)", "cumulative_operation_failures=0(partial)", "vaulticdb=unavailable", "slowest_backend=unavailable"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("overview missing %q: %s", expected, output)
		}
	}
}

func TestMonitorOverviewDoesNotPresentUnavailableValuesAsZero(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: "unavailable", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityUnavailable, Stale: true,
		Queues: []telemetry.QueueSnapshot{{Name: "writeback", Availability: telemetry.AvailabilityUnavailable, CapacityAvailability: telemetry.AvailabilityUnavailable}},
		WAL:    &telemetry.WALSnapshot{Availability: telemetry.AvailabilityUnavailable},
		Caches: []telemetry.CacheSnapshot{{ID: "objects", Availability: telemetry.AvailabilityUnavailable, TrafficBytesAvailability: telemetry.AvailabilityUnavailable}},
	})
	state := monitorWatchState{view: monitorViewOverview, sortKey: "component", lastSnapshot: &snapshot, lastCapturedAt: time.Unix(1, 0)}
	output := strings.Join(state.render(160, time.Unix(2, 0)), "\n")
	for _, expected := range []string{"occupancy=unavailable", "byte_hit_ratio=unavailable", "backlog=unavailable", "retained_bytes=unavailable"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("overview missing %q: %s", expected, output)
		}
	}
}

func TestMonitorUnavailableCurrentMetricDoesNotProduceWindowValue(t *testing.T) {
	start := time.Unix(1, 0)
	baseline := monitorTestSnapshot(start, 100, []uint64{1, 5, 10})
	current := monitorTestSnapshot(start.Add(time.Minute), 160, []uint64{2, 10, 20})
	current.Components[0].Metrics[0].Availability = telemetry.AvailabilityUnavailable
	state := monitorWatchState{lastSnapshot: &current}
	state.windowStore.Add(start, baseline)
	state.windowStore.Add(start.Add(time.Minute), current)
	if got := state.counterWindow("vaultic", current.Components[0].Metrics[0], time.Minute); got.state != "unavailable" {
		t.Fatalf("unavailable current metric produced window: %+v", got)
	}
}

func TestMonitorOverviewMarksUnavailableFailureMetricPartial(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact,
		Metrics: []telemetry.Metric{{Name: "operation_completed", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityUnavailable, Labels: []telemetry.Label{{Name: "operation", Value: "backup"}, {Name: "outcome", Value: "failure"}}}},
	})
	state := monitorWatchState{view: monitorViewOverview, sortKey: "component", lastSnapshot: &snapshot, lastCapturedAt: time.Unix(1, 0)}
	output := strings.Join(state.render(160, time.Unix(2, 0)), "\n")
	if !strings.Contains(output, "cumulative_operation_failures=0(partial)") {
		t.Fatalf("failure aggregate did not retain unavailability: %s", output)
	}
}

func monitorTestSnapshot(captured time.Time, counter uint64, histogramCounts []uint64) telemetry.MonitorSnapshot {
	return telemetry.NewMonitorSnapshot(captured, telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "proc-one", CapturedUnixMS: captured.UnixMilli(), Availability: telemetry.AvailabilityExact,
		Metrics: []telemetry.Metric{
			{Name: "requests", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: counter},
			{Name: "latency", Kind: telemetry.MetricHistogram, Unit: "microseconds", Availability: telemetry.AvailabilityExact, BucketUpper: []uint64{10, 100, 1000}, BucketCounts: append([]uint64(nil), histogramCounts...)},
		},
	})
}
