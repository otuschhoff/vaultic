package data

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/itchyny/gojq"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// ErrNoSnapshotFound is returned when no snapshot for the given criteria could be found.
var ErrNoSnapshotFound = errors.New("no snapshot found")

// A SnapshotFilter denotes a set of snapshots based on hosts, tags and paths.
type SnapshotFilter struct {
	_ struct{} // Force naming fields in literals.

	Hosts []string
	Tags  TagLists
	Paths []string
	// Match snapshots from before this timestamp. Zero for no limit.
	TimestampLimit time.Time

	// Extended filters (rustic --filter-* equivalents). All are optional.

	// Labels matches snapshots whose Label is in the list.
	Labels      []string
	FilterHosts []string
	FilterPaths [][]string
	FilterTags  TagLists
	// PathsExact matches snapshots whose Paths equal exactly one of the given
	// path lists (no subset matching).
	PathsExact [][]string
	// TagsExact matches snapshots whose Tags equal exactly one of the given
	// tag lists (no subset matching).
	TagsExact [][]string
	// After matches snapshots taken at or after this time. Zero for no limit.
	After time.Time
	// SizeMin/SizeMax bound the snapshot's TotalBytesProcessed. Zero disables.
	SizeMin, SizeMax uint64
	// SizeAddedMin/SizeAddedMax bound the snapshot's DataAdded. Zero disables.
	SizeAddedMin, SizeAddedMax uint64
	// FilterLast, when > 0, keeps only the newest FilterLast snapshots of the
	// otherwise-matching set (per group when used with grouping).
	FilterLast int
	// FilterJQ is a jq expression that must return a boolean for each snapshot.
	FilterJQ string
}

func (f *SnapshotFilter) Empty() bool {
	selectionCount := len(f.Hosts) + len(f.Tags) + len(f.Paths) + len(f.Labels)
	filterCount := len(f.FilterHosts) + len(f.FilterPaths) + len(f.FilterTags) + len(f.PathsExact) + len(f.TagsExact)
	return selectionCount+filterCount == 0 &&
		f.TimestampLimit.IsZero() && f.After.IsZero() &&
		f.SizeMin == 0 && f.SizeMax == 0 &&
		f.SizeAddedMin == 0 &&
		f.SizeAddedMax == 0 &&
		f.FilterLast == 0 &&
		f.FilterJQ == ""
}

// matches reports whether sn satisfies all configured filter criteria.
func (f *SnapshotFilter) matches(sn *Snapshot) bool {
	return sn.HasHostname(f.Hosts) &&
		sn.HasTagList(f.Tags) &&
		sn.HasPaths(f.Paths) &&
		(sn.HasHostname(f.FilterHosts) || len(f.FilterHosts) == 0) &&
		(matchesStringLists(sn.Paths, f.FilterPaths)) &&
		(sn.HasTagList(f.FilterTags) || len(f.FilterTags) == 0) &&
		f.matchesLabel(sn) &&
		f.matchesPathsExact(sn) &&
		f.matchesTagsExact(sn) &&
		f.matchesTime(sn) &&
		f.matchesSize(sn) &&
		f.matchesJQ(sn)
}

func matchesStringLists(values []string, filters [][]string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		if snHasAll(values, filter) {
			return true
		}
	}
	return false
}

func snHasAll(values, wanted []string) bool {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	for _, value := range wanted {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func (f *SnapshotFilter) matchesLabel(sn *Snapshot) bool {
	if len(f.Labels) == 0 {
		return true
	}
	return slices.Contains(f.Labels, sn.Label)
}

func (f *SnapshotFilter) matchesPathsExact(sn *Snapshot) bool {
	if len(f.PathsExact) == 0 {
		return true
	}
	for _, pl := range f.PathsExact {
		if stringSlicesEqualUnordered(sn.Paths, pl) {
			return true
		}
	}
	return false
}

func (f *SnapshotFilter) matchesTagsExact(sn *Snapshot) bool {
	if len(f.TagsExact) == 0 {
		return true
	}
	for _, tl := range f.TagsExact {
		if stringSlicesEqualUnordered(sn.Tags, tl) {
			return true
		}
	}
	return false
}

func (f *SnapshotFilter) matchesTime(sn *Snapshot) bool {
	if !f.After.IsZero() && !sn.Time.After(f.After) {
		return false
	}
	if !f.TimestampLimit.IsZero() && !sn.Time.Before(f.TimestampLimit) {
		return false
	}
	return true
}

func (f *SnapshotFilter) matchesSize(sn *Snapshot) bool {
	if f.SizeMin == 0 && f.SizeMax == 0 && f.SizeAddedMin == 0 && f.SizeAddedMax == 0 {
		return true
	}
	if sn.Summary == nil {
		// without a summary the snapshot cannot satisfy a size filter
		return false
	}
	total := sn.Summary.TotalBytesProcessed
	if f.SizeMin != 0 && total < f.SizeMin {
		return false
	}
	if f.SizeMax != 0 && total > f.SizeMax {
		return false
	}
	added := sn.Summary.DataAdded
	if f.SizeAddedMin != 0 && added < f.SizeAddedMin {
		return false
	}
	if f.SizeAddedMax != 0 && added > f.SizeAddedMax {
		return false
	}
	return true
}

func (f *SnapshotFilter) matchesJQ(sn *Snapshot) bool {
	if f.FilterJQ == "" {
		return true
	}
	code, err := snapshotJQCode(f.FilterJQ)
	if err != nil {
		return false
	}
	encoded, err := json.Marshal(sn)
	if err != nil {
		return false
	}
	var input any
	if err := json.Unmarshal(encoded, &input); err != nil {
		return false
	}
	result, ok := code.Run(input).Next()
	if !ok {
		return false
	}
	matched, ok := result.(bool)
	return ok && matched
}

var snapshotJQCache = struct {
	sync.RWMutex
	codes map[string]*gojq.Code
}{codes: make(map[string]*gojq.Code)}

func snapshotJQCode(expression string) (*gojq.Code, error) {
	snapshotJQCache.RLock()
	code := snapshotJQCache.codes[expression]
	snapshotJQCache.RUnlock()
	if code != nil {
		return code, nil
	}
	query, err := gojq.Parse(expression)
	if err != nil {
		return nil, err
	}
	code, err = gojq.Compile(query)
	if err != nil {
		return nil, err
	}
	snapshotJQCache.Lock()
	snapshotJQCache.codes[expression] = code
	snapshotJQCache.Unlock()
	return code, nil
}

// stringSlicesEqualUnordered reports whether a and b contain the same set of
// strings, ignoring order.
func stringSlicesEqualUnordered(a, b []string) bool {
	setA := make(map[string]struct{}, len(a))
	for _, s := range a {
		setA[s] = struct{}{}
	}
	setB := make(map[string]struct{}, len(b))
	for _, s := range b {
		setB[s] = struct{}{}
	}
	if len(setA) != len(setB) {
		return false
	}
	for s := range setA {
		if _, ok := setB[s]; !ok {
			return false
		}
	}
	return true
}

// findLatestN finds the N-th latest snapshot matching the filter (0 = latest).
// This implements rustic's "latest~N" syntax.
func (f *SnapshotFilter) findLatestN(
	ctx context.Context,
	be vaultic.Lister,
	loader vaultic.LoaderUnpacked,
	n int,
) (*Snapshot, error) {
	matches, err := f.findSorted(ctx, be, loader)
	if err != nil {
		return nil, err
	}
	if len(matches) <= n {
		if len(matches) == 0 {
			return nil, ErrNoSnapshotFound
		}
		return nil, errors.Errorf("no snapshot found for latest~%d (only %d snapshots match)", n, len(matches))
	}
	// matches is sorted oldest first; the n-th latest is at len-1-n
	return matches[len(matches)-1-n], nil
}

// findSorted returns all snapshots matching the filter, sorted oldest first.
func (f *SnapshotFilter) findSorted(
	ctx context.Context,
	be vaultic.Lister,
	loader vaultic.LoaderUnpacked,
) (Snapshots, error) {
	var err error
	absTargets := make([]string, 0, len(f.Paths))
	for _, target := range f.Paths {
		if !filepath.IsAbs(target) {
			target, err = filepath.Abs(target)
			if err != nil {
				return nil, errors.Wrap(err, "Abs")
			}
		}
		absTargets = append(absTargets, filepath.Clean(target))
	}
	f.Paths = absTargets

	var matches Snapshots
	err = ForAllSnapshots(ctx, be, loader, nil, func(id vaultic.ID, snapshot *Snapshot, err error) error {
		if err != nil {
			return errors.Errorf("Error loading snapshot %v: %w", id.Str(), err)
		}
		if !f.matches(snapshot) {
			return nil
		}
		matches = append(matches, snapshot)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// sort oldest first
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].Time.Before(matches[j].Time) })

	// apply --filter-last (keep only the newest FilterLast matches)
	if f.FilterLast > 0 && len(matches) > f.FilterLast {
		matches = matches[len(matches)-f.FilterLast:]
	}
	return matches, nil
}

func splitSnapshotID(s string) (id, subfolder string) {
	id, subfolder, _ = strings.Cut(s, ":")
	return
}

// FindSnapshot takes a string and tries to find a snapshot whose ID matches
// the string as closely as possible.
func FindSnapshot(
	ctx context.Context,
	be vaultic.Lister,
	loader vaultic.LoaderUnpacked,
	s string,
) (*Snapshot, string, error) {
	s, subfolder := splitSnapshotID(s)

	// no need to list snapshots if `s` is already a full id
	id, err := vaultic.ParseID(s)
	if err != nil {
		// find snapshot id with prefix
		id, err = vaultic.Find(ctx, be, vaultic.SnapshotFile, s)
		if err != nil {
			return nil, "", err
		}
	}
	sn, err := LoadSnapshot(ctx, loader, id)
	return sn, subfolder, err
}

// FindLatest returns either the latest of a filtered list of all snapshots
// or a snapshot specified by `snapshotID`. It also resolves "latest~N".
func (f *SnapshotFilter) FindLatest(
	ctx context.Context,
	be vaultic.Lister,
	loader vaultic.LoaderUnpacked,
	snapshotID string,
) (*Snapshot, string, error) {
	id, subfolder := splitSnapshotID(snapshotID)
	if n, ok := parseLatestN(id); ok {
		sn, err := f.findLatestN(ctx, be, loader, n)
		if errors.Is(err, ErrNoSnapshotFound) {
			err = fmt.Errorf("snapshot filter (Paths:%v Tags:%v Hosts:%v): %w",
				f.Paths, f.Tags, f.Hosts, err)
		}
		return sn, subfolder, err
	}
	return FindSnapshot(ctx, be, loader, snapshotID)
}

// parseLatestN parses "latest" and "latest~N" (rustic syntax). It reports
// whether the id used the latest syntax and the value of N.
func parseLatestN(id string) (n int, ok bool) {
	if id == "latest" {
		return 0, true
	}
	if strings.HasPrefix(id, "latest~") {
		v, err := strconv.Atoi(strings.TrimPrefix(id, "latest~"))
		if err != nil || v < 0 {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

type SnapshotFindCb func(string, *Snapshot, error) error

var ErrInvalidSnapshotSyntax = errors.New("<snapshot>:<subfolder> syntax not allowed")

// FindAll yields Snapshots, either given explicitly by `snapshotIDs` or filtered from the list of all snapshots.
func (f *SnapshotFilter) FindAll(
	ctx context.Context,
	be vaultic.Lister,
	loader vaultic.LoaderUnpacked,
	snapshotIDs []string,
	fn SnapshotFindCb,
) error {
	if len(snapshotIDs) != 0 {
		be, err := vaultic.MemorizeList(ctx, be, vaultic.SnapshotFile)
		if err != nil {
			return err
		}
		return f.findExplicit(ctx, be, loader, snapshotIDs, fn)
	}

	// collect the matching snapshots so that --filter-last can be applied
	var filtered Snapshots
	err := ForAllSnapshots(ctx, be, loader, nil, func(_ vaultic.ID, sn *Snapshot, err error) error {
		if err == nil && !f.matches(sn) {
			return nil
		}

		filtered = append(filtered, sn)
		return nil
	})
	if err != nil {
		return err
	}

	// apply --filter-last: keep only the newest FilterLast matching snapshots
	if f.FilterLast > 0 && len(filtered) > f.FilterLast {
		sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].Time.Before(filtered[j].Time) })
		filtered = filtered[len(filtered)-f.FilterLast:]
	}

	for _, sn := range filtered {
		if err := fn(sn.ID().String(), sn, nil); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (f *SnapshotFilter) findExplicit(
	ctx context.Context,
	be vaultic.Lister,
	loader vaultic.LoaderUnpacked,
	snapshotIDs []string,
	fn SnapshotFindCb,
) error {
	usedFilter := false
	ids := vaultic.NewIDSet()
	for _, requested := range snapshotIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		name, snapshot, used, skip, err := f.resolveExplicit(ctx, be, loader, requested, usedFilter, ids)
		usedFilter = usedFilter || used
		if skip {
			continue
		}
		if err := fn(name, snapshot, err); err != nil {
			return err
		}
	}
	if !usedFilter && !f.Empty() {
		return fn("filters", nil, errors.Errorf("explicit snapshot ids are given"))
	}
	return ctx.Err()
}

func (f *SnapshotFilter) resolveExplicit(
	ctx context.Context,
	be vaultic.Lister,
	loader vaultic.LoaderUnpacked,
	requested string,
	usedFilter bool,
	ids vaultic.IDSet,
) (string, *Snapshot, bool, bool, error) {
	if n, ok := parseLatestN(requested); ok {
		if usedFilter {
			return requested, nil, false, true, nil
		}
		snapshot, err := f.findLatestN(ctx, be, loader, n)
		if errors.Is(err, ErrNoSnapshotFound) {
			err = errors.Errorf("no snapshot matched given filter (Paths:%v Tags:%v Hosts:%v)", f.Paths, f.Tags, f.Hosts)
		}
		if snapshot != nil {
			ids.Insert(*snapshot.ID())
		}
		return requested, snapshot, true, false, err
	}
	if strings.HasPrefix(requested, "latest:") {
		return requested, nil, false, false, ErrInvalidSnapshotSyntax
	}
	snapshot, subfolder, err := FindSnapshot(ctx, be, loader, requested)
	if err == nil && subfolder != "" {
		return requested, snapshot, false, false, ErrInvalidSnapshotSyntax
	}
	if err != nil {
		return requested, snapshot, false, false, err
	}
	if ids.Has(*snapshot.ID()) {
		return requested, snapshot, false, true, nil
	}
	ids.Insert(*snapshot.ID())
	return snapshot.ID().String(), snapshot, false, false, nil
}
