package crawl

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fsevents"
)

type FSEventsVolume struct {
	Root        string
	JournalRoot string
	Device      uint64
	VolumeUUID  string
}

type FSEventsVolumeResolver interface {
	ResolveVolume(string) (FSEventsVolume, error)
}

type FSEventsRootPlan struct {
	Root       string
	VolumeUUID string
	Selective  bool
	Reason     string
	EventCount int
}

type FSEventsPlan struct {
	Plan  Plan
	Roots []FSEventsRootPlan
}

func BuildFSEventsPlan(
	ctx context.Context,
	source fsevents.Source,
	resolver FSEventsVolumeResolver,
	anchors []data.FSEventsAnchor,
	targets []string,
	timeout time.Duration,
) FSEventsPlan {
	changed := make([]string, 0)
	roots := make([]FSEventsRootPlan, 0, len(targets))
	for _, target := range targets {
		root := FSEventsRootPlan{Root: filepath.Clean(target)}
		volume, err := resolver.ResolveVolume(target)
		if err != nil {
			root.Reason = fmt.Sprintf("resolve source volume: %v", err)
		} else {
			root.Root = filepath.Clean(volume.Root)
			root, changed = planFSEventsRoot(ctx, source, anchors, volume, timeout, changed)
		}
		if !root.Selective {
			changed = append(changed, root.Root)
		}
		roots = append(roots, root)
	}
	return FSEventsPlan{Plan: NewSelectivePlan(changed), Roots: roots}
}

func planFSEventsRoot(
	ctx context.Context,
	source fsevents.Source,
	anchors []data.FSEventsAnchor,
	volume FSEventsVolume,
	timeout time.Duration,
	changed []string,
) (FSEventsRootPlan, []string) {
	root := FSEventsRootPlan{Root: filepath.Clean(volume.Root), VolumeUUID: volume.VolumeUUID}
	anchor, ok := findFSEventsAnchor(anchors, volume.VolumeUUID, root.Root)
	if !ok {
		root.Reason = fmt.Sprintf("no FSEvents anchor for volume %q in parent snapshot", volume.VolumeUUID)
		return root, changed
	}
	journalUUID, err := source.JournalUUID(volume.Device)
	if err != nil {
		root.Reason = fmt.Sprintf("read FSEvents journal UUID: %v", err)
		return root, changed
	}
	if !strings.EqualFold(journalUUID, anchor.JournalUUID) {
		root.Reason = "FSEvents journal was reset since parent (UUID changed)"
		return root, changed
	}
	current, err := source.CurrentEventID(volume.Device)
	if err != nil {
		root.Reason = fmt.Sprintf("read current FSEvents event ID: %v", err)
		return root, changed
	}
	if current < anchor.EventID {
		root.Reason = "FSEvents event ID moved backwards"
		return root, changed
	}
	journalRoot := volume.JournalRoot
	if journalRoot == "" {
		journalRoot = root.Root
	}
	result, err := source.Replay(ctx, volume.Device, []string{journalRoot}, anchor.EventID, timeout)
	if err != nil {
		root.Reason = fmt.Sprintf("FSEvents replay did not complete: %v", err)
		return root, changed
	}
	if reason := fsevents.CoverageFailure(result.Flags, result.HistoryDone); reason != "" {
		root.Reason = reason
		return root, changed
	}
	for _, event := range result.Events {
		eventPath := filepath.Clean(event.Path)
		if volume.JournalRoot != "" {
			relative, relErr := filepath.Rel(filepath.Clean(volume.JournalRoot), eventPath)
			if relErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				continue
			}
			eventPath = filepath.Join(root.Root, relative)
		}
		if event.ID <= anchor.EventID || event.ID > current || !pathWithin(root.Root, eventPath) {
			continue
		}
		if event.Recursive || eventPath == root.Root {
			changed = append(changed, eventPath)
		} else {
			changed = append(changed, filepath.Dir(eventPath))
		}
		root.EventCount++
	}
	root.Selective = true
	return root, changed
}

func findFSEventsAnchor(anchors []data.FSEventsAnchor, volumeUUID, root string) (data.FSEventsAnchor, bool) {
	root = filepath.Clean(root)
	for _, anchor := range anchors {
		if !strings.EqualFold(anchor.VolumeUUID, volumeUUID) {
			continue
		}
		for _, sourceRoot := range anchor.SourceRoots {
			if filepath.Clean(sourceRoot) == root {
				return anchor, true
			}
		}
	}
	return data.FSEventsAnchor{}, false
}

func FSEventsFallbackReason(roots []FSEventsRootPlan) string {
	reasons := make([]string, 0)
	for _, root := range roots {
		if root.Reason != "" {
			reasons = append(reasons, fmt.Sprintf("%s: %s", root.Root, root.Reason))
		}
	}
	sort.Strings(reasons)
	return strings.Join(reasons, "; ")
}
