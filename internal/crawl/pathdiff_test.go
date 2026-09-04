package crawl

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	uppathdiff "github.com/otuschhoff/pathdiff"
)

type fakePathdiffClient struct {
	status     uppathdiff.Status
	statusErr  error
	engines    [][]uppathdiff.EngineInfo
	enginesErr error
	events     []uppathdiff.Event
	eventsErr  error
	retention  time.Duration
	retainErr  error
	engineCall int
	queryPath  string
	queryStart time.Time
	queryEnd   time.Time
}

type fakeChangeService struct {
	window ChangeWindow
	err    error
}

func (service fakeChangeService) Changes(context.Context, ChangeQuery) (ChangeWindow, error) {
	return service.window, service.err
}

func TestLoadTopologyStrict(t *testing.T) {
	tests := map[string]string{
		"unknown-field":       `{"version":1,"unknown":true}`,
		"trailing-value":      `{"version":1} {"version":1}`,
		"unsupported-version": `{"version":2}`,
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "map.json")
			if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadTopology(filename); err == nil {
				t.Fatal("invalid topology was accepted")
			}
		})
	}
}

func (client *fakePathdiffClient) Status(context.Context) (uppathdiff.Status, error) {
	return client.status, client.statusErr
}

func (client *fakePathdiffClient) Engines(context.Context) ([]uppathdiff.EngineInfo, error) {
	if client.enginesErr != nil {
		return nil, client.enginesErr
	}
	index := min(client.engineCall, len(client.engines)-1)
	client.engineCall++
	return client.engines[index], nil
}

func (client *fakePathdiffClient) EventsByPath(_ context.Context, path string, start, end time.Time) ([]uppathdiff.Event, error) {
	client.queryPath, client.queryStart, client.queryEnd = path, start, end
	return client.events, client.eventsErr
}

func (client *fakePathdiffClient) Retention(context.Context) (time.Duration, error) {
	return client.retention, client.retainErr
}

func TestPathdiffServiceReportsObservationWindow(t *testing.T) {
	since := time.Now().UTC().Add(-2 * time.Hour)
	source := testTopology(t.TempDir()).Sources[0]
	client := testPathdiffClient(since)
	window, err := NewPathdiffService(client).Changes(t.Context(), ChangeQuery{Source: source, Start: since, End: since.Add(time.Hour)})
	if err != nil || !window.Continuous || !window.ObservedSince.Equal(since.Add(-time.Hour)) || window.ObservedUntil.Before(since.Add(time.Hour)) {
		t.Fatalf("change window = %#v, %v", window, err)
	}
	if client.queryPath != source.RemotePath || !client.queryStart.Equal(since) || !client.queryEnd.Equal(since.Add(time.Hour)) {
		t.Fatalf("service query = %q [%s, %s]", client.queryPath, client.queryStart, client.queryEnd)
	}

	reconnected := testPathdiffClient(since)
	reconnected.engines[1] = []uppathdiff.EngineInfo{{
		LIFIPv4: "192.0.2.10", SVMID: "svm-7", SVMName: "svm-data", Since: since.Add(time.Minute),
	}}
	window, err = NewPathdiffService(reconnected).Changes(t.Context(), ChangeQuery{Source: source, Start: since, End: since.Add(time.Hour)})
	if err != nil || window.Continuous || window.Reason == "" {
		t.Fatalf("reconnected change window = %#v, %v", window, err)
	}
}

func TestPathdiffServiceFailsClosed(t *testing.T) {
	end := time.Now().UTC().Add(-time.Minute)
	start := end.Add(-time.Hour)
	source := testTopology(t.TempDir()).Sources[0]
	query := ChangeQuery{Source: source, Start: start, End: end}
	tests := map[string]func(*fakePathdiffClient, *ChangeQuery){
		"not-running":       func(client *fakePathdiffClient, _ *ChangeQuery) { client.status.Running = false },
		"retention-horizon": func(client *fakePathdiffClient, _ *ChangeQuery) { client.retention = 30 * time.Minute },
		"missing-engine":    func(client *fakePathdiffClient, _ *ChangeQuery) { client.engines[0] = nil },
		"future-window":     func(_ *fakePathdiffClient, query *ChangeQuery) { query.End = time.Now().UTC().Add(time.Hour) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			client := testPathdiffClient(start)
			candidate := query
			mutate(client, &candidate)
			window, err := NewPathdiffService(client).Changes(t.Context(), candidate)
			if err != nil || window.Continuous || window.Reason == "" {
				t.Fatalf("change window = %#v, %v", window, err)
			}
		})
	}

	client := testPathdiffClient(start)
	client.eventsErr = errors.New("query failed")
	if _, err := NewPathdiffService(client).Changes(t.Context(), query); err == nil {
		t.Fatal("event query error was ignored")
	}
	if _, err := NewPathdiffService(client).Changes(t.Context(), ChangeQuery{Source: source, Start: end, End: start}); err == nil {
		t.Fatal("invalid query window was accepted")
	}
}

func TestBuildPathdiffPlanResolvesTopology(t *testing.T) {
	target := t.TempDir()
	since := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	topology := testTopology(target)
	service := testChangeService(since, until, uppathdiff.Event{
		Path: "/vol/home/projects/report.txt", Timestamp: since.Add(time.Minute),
		LIFIPv4: "192.0.2.10", SVMID: "svm-7", SVMName: "svm-data", VolumeName: "home", VolumeMSID: "42",
	})

	plan, err := BuildPathdiffPlan(t.Context(), service, topology, []string{target}, since, until)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Selective || plan.Reason != "" {
		t.Fatalf("expected selective plan, got %#v", plan)
	}
	want := []string{filepath.Join(target, "projects")}
	if len(plan.ChangedDirs) != 1 || plan.ChangedDirs[0] != want[0] {
		t.Fatalf("changed directories = %q, want %q", plan.ChangedDirs, want)
	}
	if plan.ReuseSubtree(filepath.Join(target, "projects")) {
		t.Fatal("changed subtree was reused")
	}
	if !plan.ReuseSubtree(filepath.Join(target, "archive")) {
		t.Fatal("unchanged subtree was not reused")
	}
}

func TestBuildPathdiffPlanHandlesRootAndZeroEventIntervals(t *testing.T) {
	target := t.TempDir()
	since := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)

	zeroTopology := testTopology(target)
	zeroPlan, err := BuildPathdiffPlan(t.Context(), testChangeService(since, until), zeroTopology, []string{target}, since, until)
	if err != nil || !zeroPlan.Selective || len(zeroPlan.ChangedDirs) != 0 {
		t.Fatalf("zero-event plan = %#v, %v", zeroPlan, err)
	}

	rootEvent := uppathdiff.Event{
		Path: "/vol/home", Timestamp: since.Add(time.Minute), LIFIPv4: "192.0.2.10",
		SVMID: "svm-7", SVMName: "svm-data", VolumeName: "home", VolumeMSID: "42",
	}
	rootPlan, err := BuildPathdiffPlan(t.Context(), testChangeService(since, until, rootEvent), testTopology(target), []string{target}, since, until)
	if err != nil || !rootPlan.Selective || len(rootPlan.ChangedDirs) != 1 || rootPlan.ChangedDirs[0] != filepath.Clean(target) {
		t.Fatalf("root-event plan = %#v, %v", rootPlan, err)
	}
	if rootPlan.ReuseSubtree(filepath.Join(target, "child")) {
		t.Fatal("remote-root event allowed a child subtree to be reused")
	}
}

func TestBuildPathdiffPlanCrawlsTargetForRename(t *testing.T) {
	target := t.TempDir()
	since := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	event := uppathdiff.Event{
		Path: "/vol/home/old/place", Operation: "NFS_RENAME", Timestamp: since.Add(time.Minute),
		LIFIPv4: "192.0.2.10", SVMID: "svm-7", SVMName: "svm-data", VolumeName: "home", VolumeMSID: "42",
	}
	plan, err := BuildPathdiffPlan(t.Context(), testChangeService(since, until, event), testTopology(target), []string{target}, since, until)
	if err != nil || !plan.Selective || len(plan.ChangedDirs) != 1 || plan.ChangedDirs[0] != filepath.Clean(target) {
		t.Fatalf("rename plan = %#v, %v", plan, err)
	}
}

func TestBuildPathdiffPlanFallsBackOnUnverifiedCoverage(t *testing.T) {
	target := t.TempDir()
	since := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	event := uppathdiff.Event{
		Path: "/vol/home/file", Timestamp: since.Add(time.Minute), LIFIPv4: "192.0.2.10",
		SVMID: "svm-7", SVMName: "svm-data", VolumeName: "home", VolumeMSID: "42",
	}
	tests := map[string]func(*Topology, *ChangeWindow){
		"late-observation":  func(_ *Topology, window *ChangeWindow) { window.ObservedSince = since.Add(time.Second) },
		"observation-ended": func(_ *Topology, window *ChangeWindow) { window.ObservedUntil = until.Add(-time.Second) },
		"discontinuous": func(_ *Topology, window *ChangeWindow) {
			window.Continuous = false
			window.Reason = "service reported a gap"
		},
		"topology":  func(topology *Topology, _ *ChangeWindow) { topology.Sources[0].VolumeMSID = "99" },
		"event-lif": func(_ *Topology, window *ChangeWindow) { window.Events[0].LIFIPv4 = "192.0.2.99" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			topology := testTopology(target)
			window := testChangeService(since, until, event).window
			mutate(&topology, &window)
			plan, err := BuildPathdiffPlan(t.Context(), fakeChangeService{window: window}, topology, []string{target}, since, until)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Selective || plan.Reason == "" {
				t.Fatalf("expected explained full-crawl fallback, got %#v", plan)
			}
		})
	}
}

func TestBuildPathdiffPlanRejectsAmbiguousTopology(t *testing.T) {
	target := t.TempDir()
	since := time.Now().UTC().Add(-time.Hour)
	until := since.Add(time.Minute)
	tests := map[string]func(*Topology){
		"relative-remote-path": func(topology *Topology) { topology.Sources[0].RemotePath = "vol/home" },
		"duplicate-target": func(topology *Topology) {
			topology.Sources = append(topology.Sources, topology.Sources[0])
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			topology := testTopology(target)
			mutate(&topology)
			plan, err := BuildPathdiffPlan(t.Context(), testChangeService(since, until), topology, []string{target}, since, until)
			if err != nil || plan.Selective || plan.Reason == "" {
				t.Fatalf("expected explained full-crawl fallback, got %#v, %v", plan, err)
			}
		})
	}
}

func TestRemotePathWithinUsesSlashBoundaries(t *testing.T) {
	tests := map[string]bool{
		"/vol/home":               true,
		"/vol/home/projects/a":    true,
		"/vol/home-other/file":    false,
		"/vol/home/../other/file": false,
	}
	for candidate, want := range tests {
		if got := remotePathWithin("/vol/home", candidate); got != want {
			t.Errorf("remotePathWithin(%q) = %t, want %t", candidate, got, want)
		}
	}
}

func testTopology(target string) Topology {
	return Topology{
		Version: 1,
		Sources: []SourceMapping{{
			Target: target, RemotePath: "/vol/home", LIF: "192.0.2.10",
			SVMID: "svm-7", SVM: "svm-data", Volume: "home", VolumeMSID: "42",
		}},
	}
}

func testPathdiffClient(since time.Time, events ...uppathdiff.Event) *fakePathdiffClient {
	engines := []uppathdiff.EngineInfo{{
		LIFIPv4: "192.0.2.10", SVMID: "svm-7", SVMName: "svm-data", Since: since.Add(-time.Hour),
	}}
	return &fakePathdiffClient{
		status:  uppathdiff.Status{Running: true},
		engines: []([]uppathdiff.EngineInfo){engines, engines},
		events:  events, retention: 24 * time.Hour,
	}
}

func testChangeService(since, until time.Time, events ...uppathdiff.Event) fakeChangeService {
	return fakeChangeService{window: ChangeWindow{
		Events: events, ObservedSince: since.Add(-time.Hour), ObservedUntil: until.Add(time.Second), Continuous: true,
	}}
}

func BenchmarkPathdiffSparseSubtreeSelection(b *testing.B) {
	root := b.TempDir()
	plan := Plan{Selective: true, ChangedDirs: []string{
		filepath.Join(root, "100", "changed"),
		filepath.Join(root, "50000", "changed"),
		filepath.Join(root, "99999", "changed"),
	}}
	plan.changedSet = make(map[string]struct{}, len(plan.ChangedDirs))
	for _, directory := range plan.ChangedDirs {
		plan.changedSet[directory] = struct{}{}
	}
	paths := make([]string, 100_000)
	for index := range paths {
		paths[index] = filepath.Join(root, strconv.Itoa(index))
	}
	b.ResetTimer()
	for b.Loop() {
		reused := 0
		for _, candidate := range paths {
			if plan.ReuseSubtree(candidate) {
				reused++
			}
		}
		if reused < len(paths)-3 {
			b.Fatalf("only %d of %d sparse subtrees were reusable", reused, len(paths))
		}
	}
}
