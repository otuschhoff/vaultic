package crawl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	uppathdiff "github.com/otuschhoff/pathdiff"
)

type PathdiffClient interface {
	Status(context.Context) (uppathdiff.Status, error)
	Engines(context.Context) ([]uppathdiff.EngineInfo, error)
	EventsByPath(context.Context, string, time.Time, time.Time) ([]uppathdiff.Event, error)
	Retention(context.Context) (time.Duration, error)
}

type Topology struct {
	Version int             `json:"version"`
	Sources []SourceMapping `json:"sources"`
}

type SourceMapping struct {
	Target     string `json:"target"`
	RemotePath string `json:"remote_path"`
	LIF        string `json:"lif"`
	SVMID      string `json:"svm_id"`
	SVM        string `json:"svm"`
	Volume     string `json:"volume"`
	VolumeMSID string `json:"volume_msid"`
}

type ChangeQuery struct {
	Source SourceMapping
	Start  time.Time
	End    time.Time
}

type ChangeWindow struct {
	Events        []uppathdiff.Event
	ObservedSince time.Time
	ObservedUntil time.Time
	Continuous    bool
	Reason        string
}

type ChangeService interface {
	Changes(context.Context, ChangeQuery) (ChangeWindow, error)
}

type PathdiffService struct {
	client PathdiffClient
}

func NewPathdiffService(client PathdiffClient) *PathdiffService {
	return &PathdiffService{client: client}
}

func (service *PathdiffService) Changes(ctx context.Context, query ChangeQuery) (ChangeWindow, error) {
	if query.Start.IsZero() || !query.Start.Before(query.End) {
		return ChangeWindow{}, fmt.Errorf("invalid pathdiff change window")
	}
	now := time.Now().UTC()
	if query.End.After(now) {
		return ChangeWindow{Reason: "pathdiff cannot attest a window ending in the future"}, nil
	}
	status, err := service.client.Status(ctx)
	if err != nil {
		return ChangeWindow{}, fmt.Errorf("query pathdiff status: %w", err)
	}
	if !status.Running {
		return ChangeWindow{Reason: "pathdiff service is not running"}, nil
	}
	retention, err := service.client.Retention(ctx)
	if err != nil {
		return ChangeWindow{}, fmt.Errorf("query pathdiff retention: %w", err)
	}
	if retention <= 0 || query.Start.Before(now.Add(-retention)) {
		return ChangeWindow{Reason: "pathdiff retention does not cover the requested window"}, nil
	}
	before, err := service.client.Engines(ctx)
	if err != nil {
		return ChangeWindow{}, fmt.Errorf("query pathdiff observation state: %w", err)
	}
	beforeEngine, ok := matchingEngine(before, query.Source)
	if !ok {
		return ChangeWindow{Reason: "pathdiff is not observing the requested LIF/SVM"}, nil
	}
	events, err := service.client.EventsByPath(ctx, query.Source.RemotePath, query.Start, query.End)
	if err != nil {
		return ChangeWindow{}, fmt.Errorf("query pathdiff changes: %w", err)
	}
	after, err := service.client.Engines(ctx)
	if err != nil {
		return ChangeWindow{}, fmt.Errorf("confirm pathdiff observation state: %w", err)
	}
	afterEngine, ok := matchingEngine(after, query.Source)
	if !ok || !afterEngine.Since.Equal(beforeEngine.Since) {
		return ChangeWindow{Reason: "pathdiff observation session changed during the query"}, nil
	}
	return ChangeWindow{
		Events: events, ObservedSince: beforeEngine.Since,
		ObservedUntil: query.End, Continuous: true,
	}, nil
}

type Plan struct {
	Selective   bool
	Reason      string
	ChangedDirs []string
	changedSet  map[string]struct{}
}

func LoadTopology(filename string) (Topology, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Topology{}, fmt.Errorf("open pathdiff SVM map: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var topology Topology
	if err := decoder.Decode(&topology); err != nil {
		return Topology{}, fmt.Errorf("decode pathdiff SVM map: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Topology{}, fmt.Errorf("decode pathdiff SVM map: trailing JSON value")
	}
	if topology.Version != 1 {
		return Topology{}, fmt.Errorf("unsupported pathdiff SVM map version %d", topology.Version)
	}
	return topology, nil
}

func BuildPathdiffPlan(ctx context.Context, service ChangeService, topology Topology, targets []string, since, until time.Time) (Plan, error) {
	full := func(format string, args ...any) (Plan, error) {
		return Plan{Reason: fmt.Sprintf(format, args...)}, nil
	}
	if since.IsZero() || !since.Before(until) {
		return full("parent snapshot time is unavailable or not before backup time")
	}
	mappings, err := matchMappings(topology.Sources, targets)
	if err != nil {
		return full("topology mismatch: %v", err)
	}
	changed := make(map[string]struct{})
	// The parent snapshot timestamp is its backup start, not an atomic
	// filesystem fence. Including equal timestamps prevents false negatives.
	for _, mapping := range mappings {
		window, err := service.Changes(ctx, ChangeQuery{Source: mapping, Start: since, End: until})
		if err != nil {
			return full("query pathdiff changes for %q: %v", mapping.RemotePath, err)
		}
		if !window.Continuous || window.ObservedSince.IsZero() || window.ObservedSince.After(since) || window.ObservedUntil.Before(until) {
			reason := window.Reason
			if reason == "" {
				reason = "the service did not observe the complete requested window"
			}
			return full("coverage for %q is incomplete: %s", mapping.RemotePath, reason)
		}
		for _, event := range window.Events {
			if event.Timestamp.Before(since) || event.Timestamp.After(until) || event.LIFIPv4 != mapping.LIF || event.SVMID != mapping.SVMID || event.SVMName != mapping.SVM || event.VolumeMSID != mapping.VolumeMSID || event.VolumeName != mapping.Volume || !remotePathWithin(mapping.RemotePath, event.Path) {
				return full("pathdiff returned an event outside the requested path, window, or topology for %q", mapping.RemotePath)
			}
			if strings.Contains(strings.ToLower(event.Operation), "rename") {
				changed[filepath.Clean(mapping.Target)] = struct{}{}
				continue
			}
			relative := strings.TrimPrefix(path.Clean(event.Path), path.Clean(mapping.RemotePath))
			relative = strings.TrimPrefix(relative, "/")
			local := filepath.Join(mapping.Target, filepath.FromSlash(relative))
			changedDir := filepath.Clean(filepath.Dir(local))
			if relative == "" {
				changedDir = filepath.Clean(mapping.Target)
			}
			changed[changedDir] = struct{}{}
		}
	}
	dirs := make([]string, 0, len(changed))
	for dir := range changed {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	dirs = collapsePaths(dirs)
	changedSet := make(map[string]struct{}, len(dirs))
	for _, directory := range dirs {
		changedSet[directory] = struct{}{}
	}
	return Plan{Selective: true, ChangedDirs: dirs, changedSet: changedSet}, nil
}

func (plan Plan) ReuseSubtree(sourcePath string) bool {
	if !plan.Selective {
		return false
	}
	sourcePath = filepath.Clean(sourcePath)
	changedSet := plan.changedSet
	if changedSet == nil {
		changedSet = make(map[string]struct{}, len(plan.ChangedDirs))
		for _, directory := range plan.ChangedDirs {
			changedSet[directory] = struct{}{}
		}
	}
	for ancestor := sourcePath; ; ancestor = filepath.Dir(ancestor) {
		if _, changed := changedSet[ancestor]; changed {
			return false
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			break
		}
	}
	prefix := sourcePath
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	index := sort.SearchStrings(plan.ChangedDirs, prefix)
	if index < len(plan.ChangedDirs) && strings.HasPrefix(plan.ChangedDirs[index], prefix) {
		return false
	}
	return true
}

func matchMappings(mappings []SourceMapping, targets []string) ([]SourceMapping, error) {
	matched := make([]SourceMapping, 0, len(targets))
	for _, target := range targets {
		absolute, err := filepath.Abs(target)
		if err != nil {
			return nil, err
		}
		var found *SourceMapping
		for _, mapping := range mappings {
			mappedTarget, err := filepath.Abs(mapping.Target)
			if err != nil {
				continue
			}
			if filepath.Clean(absolute) == filepath.Clean(mappedTarget) && mapping.LIF != "" && mapping.SVMID != "" && mapping.SVM != "" && mapping.Volume != "" && mapping.VolumeMSID != "" && path.IsAbs(mapping.RemotePath) {
				if found != nil {
					return nil, fmt.Errorf("target %q has multiple complete LIF/SVM/volume mappings", target)
				}
				mapping.Target = mappedTarget
				mapping.RemotePath = path.Clean(mapping.RemotePath)
				found = &mapping
			}
		}
		if found == nil {
			return nil, fmt.Errorf("target %q has no complete LIF/SVM/volume mapping", target)
		}
		matched = append(matched, *found)
	}
	return matched, nil
}

func matchingEngine(engines []uppathdiff.EngineInfo, mapping SourceMapping) (uppathdiff.EngineInfo, bool) {
	for _, engine := range engines {
		if engine.LIFIPv4 == mapping.LIF && engine.SVMID == mapping.SVMID && engine.SVMName == mapping.SVM {
			return engine, true
		}
	}
	return uppathdiff.EngineInfo{}, false
}

// collapsePaths expects lexicographically sorted paths.
func collapsePaths(paths []string) []string {
	result := make([]string, 0, len(paths))
	for _, candidate := range paths {
		if len(result) == 0 || !pathWithin(result[len(result)-1], candidate) {
			result = append(result, candidate)
		}
	}
	return result
}

func pathWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func remotePathWithin(parent, child string) bool {
	parent = path.Clean(parent)
	child = path.Clean(child)
	return child == parent || parent == "/" && strings.HasPrefix(child, "/") || strings.HasPrefix(child, parent+"/")
}
