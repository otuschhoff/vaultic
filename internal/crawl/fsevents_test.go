package crawl

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fsevents"
)

type fakeFSEventsSource struct {
	current     uint64
	journalUUID string
	result      fsevents.ReplayResult
	err         error
}

func (source fakeFSEventsSource) CurrentEventID(uint64) (uint64, error) {
	return source.current, source.err
}

func (source fakeFSEventsSource) JournalUUID(uint64) (string, error) {
	return source.journalUUID, source.err
}

func (source fakeFSEventsSource) Replay(context.Context, uint64, []string, uint64, time.Duration) (fsevents.ReplayResult, error) {
	return source.result, source.err
}

type fakeVolumeResolver map[string]FSEventsVolume

func (resolver fakeVolumeResolver) ResolveVolume(target string) (FSEventsVolume, error) {
	volume, ok := resolver[target]
	if !ok {
		return FSEventsVolume{}, errors.New("unknown target")
	}
	return volume, nil
}

func TestBuildFSEventsPlanSelectsChangedDirectories(t *testing.T) {
	root := t.TempDir()
	plan := BuildFSEventsPlan(t.Context(), fakeFSEventsSource{
		current: 12, journalUUID: "journal",
		result: fsevents.ReplayResult{
			Flags: fsevents.FlagHistoryDone, HistoryDone: true,
			Events: []fsevents.Event{
				{Path: filepath.Join(root, "docs", "report"), ID: 11},
				{Path: filepath.Join(root, "tree"), ID: 12, Recursive: true},
				{Path: filepath.Join(filepath.Dir(root), "outside"), ID: 12},
			},
		},
	}, fakeVolumeResolver{root: {Root: root, Device: 7, VolumeUUID: "volume"}}, []data.FSEventsAnchor{{
		VolumeUUID: "volume", JournalUUID: "journal", EventID: 10, SourceRoots: []string{root},
	}}, []string{root}, time.Second)
	if len(plan.Roots) != 1 || !plan.Roots[0].Selective || plan.Roots[0].EventCount != 2 {
		t.Fatalf("root plans = %#v", plan.Roots)
	}
	want := []string{filepath.Join(root, "docs"), filepath.Join(root, "tree")}
	if len(plan.Plan.ChangedDirs) != len(want) {
		t.Fatalf("changed dirs = %q, want %q", plan.Plan.ChangedDirs, want)
	}
	for index := range want {
		if plan.Plan.ChangedDirs[index] != want[index] {
			t.Fatalf("changed dirs = %q, want %q", plan.Plan.ChangedDirs, want)
		}
	}
}

func TestBuildFSEventsPlanFallsBackPerRoot(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	resolver := fakeVolumeResolver{
		first:  {Root: first, Device: 7, VolumeUUID: "first"},
		second: {Root: second, Device: 8, VolumeUUID: "second"},
	}
	anchors := []data.FSEventsAnchor{{
		VolumeUUID: "first", JournalUUID: "journal", EventID: 10, SourceRoots: []string{first},
	}}
	plan := BuildFSEventsPlan(t.Context(), fakeFSEventsSource{
		current: 10, journalUUID: "journal",
		result: fsevents.ReplayResult{Flags: fsevents.FlagHistoryDone, HistoryDone: true},
	}, resolver, anchors, []string{first, second}, time.Second)
	if !plan.Roots[0].Selective || plan.Roots[1].Selective || !strings.Contains(plan.Roots[1].Reason, "no FSEvents anchor") {
		t.Fatalf("root plans = %#v", plan.Roots)
	}
	if !plan.Plan.ReuseSubtree(filepath.Join(first, "unchanged")) || plan.Plan.ReuseSubtree(second) {
		t.Fatalf("per-root reuse was not preserved: %#v", plan.Plan)
	}
}

func TestBuildFSEventsPlanFallsBackWhenRootIsNotFrozen(t *testing.T) {
	root := t.TempDir()
	plan := BuildFSEventsPlan(t.Context(), fakeFSEventsSource{}, fakeVolumeResolver{}, nil, []string{root}, time.Second)
	if len(plan.Roots) != 1 || plan.Roots[0].Selective || !strings.Contains(plan.Roots[0].Reason, "resolve source volume") {
		t.Fatalf("root plans = %#v", plan.Roots)
	}
	if plan.Plan.ReuseSubtree(root) || plan.Plan.ReuseSubtree(filepath.Join(root, "child")) {
		t.Fatalf("unfrozen root was eligible for reuse: %#v", plan.Plan)
	}
}

func TestBuildFSEventsPlanFailsClosed(t *testing.T) {
	root := t.TempDir()
	resolver := fakeVolumeResolver{root: {Root: root, Device: 7, VolumeUUID: "volume"}}
	anchor := data.FSEventsAnchor{VolumeUUID: "volume", JournalUUID: "journal", EventID: 10, SourceRoots: []string{root}}
	tests := map[string]fakeFSEventsSource{
		"uuid":     {current: 12, journalUUID: "other"},
		"backward": {current: 9, journalUUID: "journal"},
		"timeout":  {current: 12, journalUUID: "journal", err: context.DeadlineExceeded},
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			plan := BuildFSEventsPlan(t.Context(), source, resolver, []data.FSEventsAnchor{anchor}, []string{root}, time.Second)
			if plan.Roots[0].Selective || plan.Roots[0].Reason == "" || plan.Plan.ReuseSubtree(root) {
				t.Fatalf("plan = %#v", plan)
			}
		})
	}
	for _, flag := range []fsevents.Flags{
		fsevents.FlagKernelDropped, fsevents.FlagUserDropped, fsevents.FlagEventIDsWrapped,
		fsevents.FlagRootChanged, fsevents.FlagMount, fsevents.FlagUnmount,
	} {
		plan := BuildFSEventsPlan(t.Context(), fakeFSEventsSource{
			current: 12, journalUUID: "journal",
			result: fsevents.ReplayResult{Flags: flag | fsevents.FlagHistoryDone, HistoryDone: true},
		}, resolver, []data.FSEventsAnchor{anchor}, []string{root}, time.Second)
		if plan.Roots[0].Selective || plan.Roots[0].Reason == "" {
			t.Fatalf("flag %d plan = %#v", flag, plan)
		}
	}
}
