// Package analytics implements optional, rebuildable creation analytics.
package analytics

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const pageSize = 10_000

// Store is the durable index surface needed by analytics.
type Store interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error)
	WriteMutableBatch(context.Context, []daemon.Mutation, [][]byte, bool) error
}

// Config controls deterministic path classification and cache promotion.
type Config struct {
	SVMDepth          int      `json:"svm_depth,omitempty"`
	VolumeDepth       int      `json:"volume_depth,omitempty"`
	PathGroupDepth    int      `json:"path_group_depth,omitempty"`
	PathGroupPrefixes []string `json:"path_group_prefixes,omitempty"`
	CacheAfter        uint64   `json:"cache_after,omitempty"`
	CacheTTLSeconds   int64    `json:"cache_ttl_seconds,omitempty"`
}

func (config Config) normalized() Config {
	if config.SVMDepth == 0 {
		config.SVMDepth = 1
	}
	if config.VolumeDepth == 0 {
		config.VolumeDepth = 2
	}
	if config.PathGroupDepth == 0 {
		config.PathGroupDepth = 3
	}
	if config.CacheAfter == 0 {
		config.CacheAfter = 3
	}
	if config.CacheTTLSeconds == 0 {
		config.CacheTTLSeconds = 86400
	}
	return config
}

func (config Config) Validate() error {
	config = config.normalized()
	if config.SVMDepth < 1 || config.VolumeDepth < 1 || config.PathGroupDepth < 1 {
		return fmt.Errorf("analytics path depths must be positive")
	}
	if config.CacheTTLSeconds < 0 {
		return fmt.Errorf("analytics cache TTL must not be negative")
	}
	seenPrefixes := map[string]struct{}{}
	for _, prefix := range config.PathGroupPrefixes {
		clean := path.Clean(prefix)
		if !strings.HasPrefix(prefix, "/") || clean != prefix {
			return fmt.Errorf("analytics path-group prefix %q must be absolute", prefix)
		}
		if _, exists := seenPrefixes[clean]; exists {
			return fmt.Errorf("duplicate analytics path-group prefix %q", prefix)
		}
		seenPrefixes[clean] = struct{}{}
	}
	return nil
}

// LifecycleResult describes an enable, rebuild, disable, or purge operation.
type LifecycleResult struct {
	Enabled    bool   `json:"enabled"`
	Generation uint64 `json:"generation"`
	Facts      uint64 `json:"facts"`
	Removed    uint64 `json:"removed,omitempty"`
	BuiltAt    int64  `json:"built_at,omitempty"`
}

func Status(ctx context.Context, store Store) (schema.AnalyticsMetadataRecord, error) {
	value, found, err := store.Get(ctx, schema.AnalyticsMetadataKey())
	if err != nil || !found {
		return schema.AnalyticsMetadataRecord{}, err
	}
	return schema.UnmarshalAnalyticsMetadataRecord(value)
}

func Enable(ctx context.Context, store Store, config Config, dryRun bool) (LifecycleResult, error) {
	return Rebuild(ctx, store, config, dryRun)
}

func Rebuild(ctx context.Context, store Store, config Config, dryRun bool) (LifecycleResult, error) {
	if err := config.Validate(); err != nil {
		return LifecycleResult{}, err
	}
	config = config.normalized()
	old, err := Status(ctx, store)
	if err != nil {
		return LifecycleResult{}, err
	}
	generation := old.Generation + 1
	if generation == 0 {
		generation = 1
	}
	if !dryRun {
		building := old
		building.Enabled = false
		building.Facts = 0
		building.CacheEntries = 0
		value, err := building.MarshalBinary()
		if err != nil {
			return LifecycleResult{}, err
		}
		if err := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsMetadataKey(), Value: value}}, nil, false); err != nil {
			return LifecycleResult{}, err
		}
	}
	removed, err := purgePrefixes(ctx, store, dryRun, schema.AnalyticsFactPrefix(), schema.AnalyticsCachePrefix())
	if err != nil {
		return LifecycleResult{}, err
	}

	var facts uint64
	var previous schema.ParsedKey
	havePrevious := false
	var mutations []daemon.Mutation
	err = scan(ctx, store, []byte("iv:"), func(kv daemon.KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil || parsed.Kind != schema.KeyInodeRevision {
			return fmt.Errorf("invalid inode revision key %x", kv.Key)
		}
		if havePrevious && parsed.FSID == previous.FSID && parsed.Inode == previous.Inode {
			return nil
		}
		havePrevious, previous = true, parsed
		revision, err := schema.UnmarshalInodeRevision(kv.Value)
		if err != nil {
			return err
		}
		fact := makeFact(parsed, revision, config)
		if current, found, err := store.Get(ctx, schema.CurrentInodeKey(parsed.FSID, parsed.Inode)); err != nil {
			return err
		} else if found {
			pointer, err := schema.UnmarshalCurrentPointer(current)
			if err != nil {
				return err
			}
			fact.Residency = schema.AnalyticsLive
			_ = pointer
		}
		value, err := fact.MarshalBinary()
		if err != nil {
			return err
		}
		mutations = append(mutations, daemon.Mutation{Key: schema.AnalyticsFactKey(parsed.FSID, parsed.Inode), Value: value})
		facts++
		if len(mutations) == pageSize && !dryRun {
			if err := store.WriteMutableBatch(ctx, mutations, nil, false); err != nil {
				return err
			}
			mutations = mutations[:0]
		}
		return nil
	})
	if err != nil {
		return LifecycleResult{}, err
	}
	builtAt := time.Now().UnixNano()
	configJSON, err := json.Marshal(config)
	if err != nil {
		return LifecycleResult{}, err
	}
	metadata := schema.AnalyticsMetadataRecord{Enabled: true, Generation: generation, Facts: facts, BuiltAt: builtAt, ConfigJSON: string(configJSON)}
	value, err := metadata.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	if !dryRun {
		mutations = append(mutations, daemon.Mutation{Key: schema.AnalyticsMetadataKey(), Value: value})
		if err := store.WriteMutableBatch(ctx, mutations, nil, false); err != nil {
			return LifecycleResult{}, err
		}
	}
	return LifecycleResult{Enabled: true, Generation: generation, Facts: facts, Removed: removed, BuiltAt: builtAt}, nil
}

func Disable(ctx context.Context, store Store, purge, dryRun bool) (LifecycleResult, error) {
	metadata, err := Status(ctx, store)
	if err != nil {
		return LifecycleResult{}, err
	}
	var removed uint64
	if purge {
		removed, err = purgePrefixes(ctx, store, dryRun, schema.AnalyticsFactPrefix(), schema.AnalyticsCachePrefix())
		if err != nil {
			return LifecycleResult{}, err
		}
	}
	metadata.Enabled, metadata.Facts, metadata.CacheEntries = false, 0, 0
	value, err := metadata.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	if !dryRun {
		if err := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsMetadataKey(), Value: value}}, nil, false); err != nil {
			return LifecycleResult{}, err
		}
	}
	return LifecycleResult{Enabled: false, Generation: metadata.Generation, Removed: removed}, nil
}

func makeFact(key schema.ParsedKey, revision schema.InodeRevision, config Config) schema.AnalyticsFactRecord {
	fact := schema.AnalyticsFactRecord{Revision: key.Revision, UID: revision.UID, GID: revision.GID, LogicalSize: revision.Size, SourcePath: revision.SourcePath, Residency: schema.AnalyticsArchiveOnly, CreationBasis: schema.AnalyticsTimeUnknown}
	fact.Known = revision.Known
	if revision.Known&schema.KnownCTime != 0 {
		fact.CreatedAt, fact.CreationBasis = revision.CTime, schema.AnalyticsCTime
	} else if revision.Known&schema.KnownMTime != 0 {
		fact.CreatedAt, fact.CreationBasis = revision.MTime, schema.AnalyticsMTime
	}
	if fact.CreationBasis != schema.AnalyticsTimeUnknown {
		created := time.Unix(0, fact.CreatedAt).UTC()
		fact.CalendarYear, fact.CalendarMonth = int32(created.Year()), uint8(created.Month())
		isoYear, week := created.ISOWeek()
		fact.ISOYear, fact.Workweek = int32(isoYear), uint8(week)
	}
	if revision.Known&schema.KnownSize != 0 {
		fact.SizeLog10 = uint8(sizeLog10(revision.Size))
	}
	parts := splitPath(revision.SourcePath)
	fact.SVM = atDepth(parts, config.SVMDepth)
	fact.Volume = atDepth(parts, config.VolumeDepth)
	fact.PathGroup = classifyGroup(revision.SourcePath, parts, config)
	return fact
}

func splitPath(value string) []string {
	return strings.FieldsFunc(path.Clean(value), func(r rune) bool { return r == '/' })
}
func atDepth(parts []string, depth int) string {
	if depth > 0 && len(parts) >= depth {
		return parts[depth-1]
	}
	return "unknown"
}
func classifyGroup(source string, parts []string, config Config) string {
	best := ""
	for _, prefix := range config.PathGroupPrefixes {
		clean := path.Clean(prefix)
		if (source == clean || strings.HasPrefix(source, clean+"/")) && len(clean) > len(best) {
			best = clean
		}
	}
	if best != "" {
		return best
	}
	return atDepth(parts, config.PathGroupDepth)
}

// Query supports arbitrary conjunctions and arbitrary grouping subsets.
type Query struct {
	UIDs        []uint32 `json:"uids,omitempty"`
	GIDs        []uint32 `json:"gids,omitempty"`
	Years       []int    `json:"years,omitempty"`
	Months      []int    `json:"months,omitempty"`
	ISOYears    []int    `json:"iso_years,omitempty"`
	Workweeks   []int    `json:"workweeks,omitempty"`
	SVMs        []string `json:"svms,omitempty"`
	Volumes     []string `json:"volumes,omitempty"`
	PathGroups  []string `json:"path_groups,omitempty"`
	SizeMin     *uint64  `json:"size_min,omitempty"`
	SizeMax     *uint64  `json:"size_max,omitempty"`
	SizeLog10   []int    `json:"size_log10,omitempty"`
	Residencies []string `json:"residencies,omitempty"`
	GroupBy     []string `json:"group_by,omitempty"`
}

type Group struct {
	Dimensions   map[string]string `json:"dimensions"`
	Files        uint64            `json:"files"`
	LogicalBytes uint64            `json:"logical_bytes"`
}
type Result struct {
	SchemaVersion       int     `json:"schema_version"`
	Generation          uint64  `json:"generation"`
	Cached              bool    `json:"cached"`
	Files               uint64  `json:"files"`
	LogicalBytes        uint64  `json:"logical_bytes"`
	UnknownCreationTime uint64  `json:"unknown_creation_time"`
	Groups              []Group `json:"groups,omitempty"`
}
type cacheEnvelope struct {
	Generation uint64  `json:"generation"`
	Hits       uint64  `json:"hits"`
	ExpiresAt  int64   `json:"expires_at"`
	Result     *Result `json:"result,omitempty"`
}

func Execute(ctx context.Context, store Store, query Query) (Result, error) {
	if err := query.Validate(); err != nil {
		return Result{}, err
	}
	metadata, err := Status(ctx, store)
	if err != nil {
		return Result{}, err
	}
	if !metadata.Enabled {
		return Result{}, fmt.Errorf("analytics is disabled; enable or rebuild it first")
	}
	var config Config
	if metadata.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(metadata.ConfigJSON), &config); err != nil {
			return Result{}, err
		}
	}
	config = config.normalized()
	canonical, err := canonicalQuery(query)
	if err != nil {
		return Result{}, err
	}
	hash := sha256.Sum256(canonical)
	cacheKey := schema.AnalyticsCacheKey(schema.ID(hash))
	now := time.Now().Unix()
	var cached cacheEnvelope
	if value, found, err := store.Get(ctx, cacheKey); err != nil {
		return Result{}, err
	} else if found && json.Unmarshal(value, &cached) == nil && cached.Generation == metadata.Generation && cached.Result != nil && cached.ExpiresAt >= now {
		result := *cached.Result
		result.Cached = true
		return result, nil
	}
	result := Result{SchemaVersion: 1, Generation: metadata.Generation}
	groups := map[string]*Group{}
	err = scan(ctx, store, schema.AnalyticsFactPrefix(), func(kv daemon.KeyValue) error {
		fact, err := schema.UnmarshalAnalyticsFactRecord(kv.Value)
		if err != nil {
			return err
		}
		if !matches(fact, query) {
			return nil
		}
		result.Files++
		result.LogicalBytes += fact.LogicalSize
		if fact.CreationBasis == schema.AnalyticsTimeUnknown {
			result.UnknownCreationTime++
		}
		dimensions := dimensions(fact, query.GroupBy)
		if len(dimensions) != 0 {
			key, _ := json.Marshal(dimensions)
			group := groups[string(key)]
			if group == nil {
				group = &Group{Dimensions: dimensions}
				groups[string(key)] = group
			}
			group.Files++
			group.LogicalBytes += fact.LogicalSize
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Groups = append(result.Groups, *groups[key])
	}
	cached.Hits++
	promoted := false
	if cached.Hits >= config.CacheAfter {
		promoted = cached.Result == nil
		copyResult := result
		cached.Result = &copyResult
		cached.ExpiresAt = now + config.CacheTTLSeconds
	}
	cached.Generation = metadata.Generation
	value, err := json.Marshal(cached)
	if err != nil {
		return Result{}, err
	}
	mutations := []daemon.Mutation{{Key: cacheKey, Value: value}}
	if promoted {
		metadata.CacheEntries++
		metadataValue, err := metadata.MarshalBinary()
		if err != nil {
			return Result{}, err
		}
		mutations = append(mutations, daemon.Mutation{Key: schema.AnalyticsMetadataKey(), Value: metadataValue})
	}
	if err := store.WriteMutableBatch(ctx, mutations, nil, false); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (query Query) Validate() error {
	if query.SizeMin != nil && query.SizeMax != nil && *query.SizeMin >= *query.SizeMax {
		return fmt.Errorf("size-min must be less than exclusive size-max")
	}
	for _, month := range query.Months {
		if month < 1 || month > 12 {
			return fmt.Errorf("month must be in 1..12, got %d", month)
		}
	}
	for _, week := range query.Workweeks {
		if week < 1 || week > 53 {
			return fmt.Errorf("workweek must be in 1..53, got %d", week)
		}
	}
	for _, magnitude := range query.SizeLog10 {
		if magnitude < 0 || magnitude > 19 {
			return fmt.Errorf("size-log10 must be in 0..19, got %d", magnitude)
		}
	}
	allowed := map[string]bool{"uid": true, "gid": true, "year": true, "month": true, "iso-year": true, "workweek": true, "svm": true, "volume": true, "path-group": true, "size-log10": true, "residency": true}
	for _, name := range query.GroupBy {
		if !allowed[name] {
			return fmt.Errorf("unknown analytics group-by dimension %q", name)
		}
	}
	for _, residency := range query.Residencies {
		if residency != "live" && residency != "archive-only" && residency != "unknown" {
			return fmt.Errorf("unknown analytics residency %q", residency)
		}
	}
	return nil
}

func canonicalQuery(query Query) ([]byte, error) {
	query.UIDs = append([]uint32(nil), query.UIDs...)
	query.GIDs = append([]uint32(nil), query.GIDs...)
	query.Years = append([]int(nil), query.Years...)
	query.Months = append([]int(nil), query.Months...)
	query.ISOYears = append([]int(nil), query.ISOYears...)
	query.Workweeks = append([]int(nil), query.Workweeks...)
	query.SizeLog10 = append([]int(nil), query.SizeLog10...)
	query.SVMs = append([]string(nil), query.SVMs...)
	query.Volumes = append([]string(nil), query.Volumes...)
	query.PathGroups = append([]string(nil), query.PathGroups...)
	query.Residencies = append([]string(nil), query.Residencies...)
	query.GroupBy = append([]string(nil), query.GroupBy...)
	sort.Slice(query.UIDs, func(i, j int) bool { return query.UIDs[i] < query.UIDs[j] })
	sort.Slice(query.GIDs, func(i, j int) bool { return query.GIDs[i] < query.GIDs[j] })
	sort.Ints(query.Years)
	sort.Ints(query.Months)
	sort.Ints(query.ISOYears)
	sort.Ints(query.Workweeks)
	sort.Ints(query.SizeLog10)
	sort.Strings(query.SVMs)
	sort.Strings(query.Volumes)
	sort.Strings(query.PathGroups)
	sort.Strings(query.Residencies)
	sort.Strings(query.GroupBy)
	return json.Marshal(query)
}

func matches(f schema.AnalyticsFactRecord, q Query) bool {
	if (len(q.UIDs) != 0 && (f.Known&schema.KnownUID == 0 || !hasUint32(q.UIDs, f.UID))) || (len(q.GIDs) != 0 && (f.Known&schema.KnownGID == 0 || !hasUint32(q.GIDs, f.GID))) || (q.SizeMin != nil && (f.Known&schema.KnownSize == 0 || f.LogicalSize < *q.SizeMin)) || (q.SizeMax != nil && (f.Known&schema.KnownSize == 0 || f.LogicalSize >= *q.SizeMax)) {
		return false
	}
	if !hasString(q.SVMs, f.SVM) || !hasString(q.Volumes, f.Volume) || !hasString(q.PathGroups, f.PathGroup) || !hasString(q.Residencies, residencyName(f.Residency)) {
		return false
	}
	if len(q.SizeLog10) != 0 && (f.Known&schema.KnownSize == 0 || !hasInt(q.SizeLog10, int(f.SizeLog10))) {
		return false
	}
	if f.CreationBasis == schema.AnalyticsTimeUnknown {
		return len(q.Years) == 0 && len(q.Months) == 0 && len(q.ISOYears) == 0 && len(q.Workweeks) == 0
	}
	return hasInt(q.Years, int(f.CalendarYear)) && hasInt(q.Months, int(f.CalendarMonth)) && hasInt(q.ISOYears, int(f.ISOYear)) && hasInt(q.Workweeks, int(f.Workweek))
}
func hasUint32(values []uint32, value uint32) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func hasInt(values []int, value int) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func hasString(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func sizeLog10(size uint64) int {
	if size == 0 {
		return 0
	}
	return int(math.Floor(math.Log10(float64(size))))
}
func residencyName(value schema.AnalyticsResidency) string {
	switch value {
	case schema.AnalyticsLive:
		return "live"
	case schema.AnalyticsArchiveOnly:
		return "archive-only"
	default:
		return "unknown"
	}
}
func dimensions(f schema.AnalyticsFactRecord, names []string) map[string]string {
	if len(names) == 0 {
		return nil
	}
	values := map[string]string{}
	for _, name := range names {
		switch name {
		case "uid":
			if f.Known&schema.KnownUID != 0 {
				values[name] = strconv.FormatUint(uint64(f.UID), 10)
			} else {
				values[name] = "unknown"
			}
		case "gid":
			if f.Known&schema.KnownGID != 0 {
				values[name] = strconv.FormatUint(uint64(f.GID), 10)
			} else {
				values[name] = "unknown"
			}
		case "year":
			if f.CreationBasis != schema.AnalyticsTimeUnknown {
				values[name] = strconv.Itoa(int(f.CalendarYear))
			} else {
				values[name] = "unknown"
			}
		case "month":
			if f.CreationBasis != schema.AnalyticsTimeUnknown {
				values[name] = strconv.Itoa(int(f.CalendarMonth))
			} else {
				values[name] = "unknown"
			}
		case "iso-year":
			if f.CreationBasis != schema.AnalyticsTimeUnknown {
				values[name] = strconv.Itoa(int(f.ISOYear))
			} else {
				values[name] = "unknown"
			}
		case "workweek":
			if f.CreationBasis != schema.AnalyticsTimeUnknown {
				values[name] = strconv.Itoa(int(f.Workweek))
			} else {
				values[name] = "unknown"
			}
		case "svm":
			values[name] = f.SVM
		case "volume":
			values[name] = f.Volume
		case "path-group":
			values[name] = f.PathGroup
		case "size-log10":
			if f.Known&schema.KnownSize != 0 {
				values[name] = strconv.Itoa(int(f.SizeLog10))
			} else {
				values[name] = "unknown"
			}
		case "residency":
			values[name] = residencyName(f.Residency)
		}
	}
	return values
}

func Purge(ctx context.Context, store Store, dryRun bool) (LifecycleResult, error) {
	return Disable(ctx, store, true, dryRun)
}
func purgePrefixes(ctx context.Context, store Store, dryRun bool, prefixes ...[]byte) (uint64, error) {
	var removed uint64
	for _, prefix := range prefixes {
		var deletes [][]byte
		err := scan(ctx, store, prefix, func(kv daemon.KeyValue) error {
			removed++
			deletes = append(deletes, append([]byte(nil), kv.Key...))
			if len(deletes) == pageSize && !dryRun {
				if err := store.WriteMutableBatch(ctx, nil, deletes, false); err != nil {
					return err
				}
				deletes = deletes[:0]
			}
			return nil
		})
		if err != nil {
			return removed, err
		}
		if len(deletes) > 0 && !dryRun {
			if err := store.WriteMutableBatch(ctx, nil, deletes, false); err != nil {
				return removed, err
			}
		}
	}
	return removed, nil
}
func scan(ctx context.Context, store Store, prefix []byte, visit func(daemon.KeyValue) error) error {
	var cursor []byte
	for {
		entries, more, err := store.ScanPrefix(ctx, prefix, cursor, pageSize)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := visit(entry); err != nil {
				return err
			}
		}
		if !more {
			return nil
		}
		if len(entries) == 0 {
			return fmt.Errorf("analytics scan returned an empty continuation page")
		}
		cursor = append(cursor[:0], entries[len(entries)-1].Key...)
		cursor = append(cursor, 0)
		if !bytes.HasPrefix(cursor, prefix) {
			return fmt.Errorf("analytics scan escaped prefix")
		}
	}
}
