package maintenance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// IntrospectSchemaVersion versions the JSON contract of every introspection
// result. The human-readable output may change freely; this number must be
// incremented whenever the JSON shape or the meaning of a field changes, so a
// consumer can detect the change instead of silently misreading it.
//
// Version 1 reports the Phase 9 tier dimension. When the placement model of
// Phase 12 replaces tiers with backends, this becomes version 2.
const IntrospectSchemaVersion = 1

// ErrLegacyRepository is returned by introspection commands that have no
// meaningful answer without a SlateDB pack catalog. Reporting a partial answer
// as though it were complete would be worse than refusing.
var ErrLegacyRepository = errors.New("this command requires a SlateDB-authoritative repository; a legacy JSON repository has no pack catalog to report on")

// StatsSource records where a composition answer came from, because the
// aggregate records are constant-time while some groupings require a catalog
// scan. A reader should not have to guess which it got.
type StatsSource string

const (
	SourceAggregates StatsSource = "aggregates"
	SourceCatalog    StatsSource = "catalog"
)

// StatsOptions selects and groups repository composition.
type StatsOptions struct {
	// GroupBy names the dimensions to break totals down by: tier, type, or
	// state. Grouping by state requires a catalog scan because no aggregate
	// carries the lifecycle dimension.
	GroupBy []string
	Tier    string
	Type    string
	State   string
	Verify  bool
	Rebuild bool
	DryRun  bool
}

// StatsGroup is one row of composition totals.
type StatsGroup struct {
	Dimension          string  `json:"dimension"`
	Key                string  `json:"key"`
	PackCount          uint64  `json:"pack_count"`
	PhysicalSize       uint64  `json:"physical_size"`
	PayloadSize        uint64  `json:"payload_size"`
	HeaderSize         uint64  `json:"header_size"`
	BlobCount          uint64  `json:"blob_count"`
	UsedPayloadBytes   uint64  `json:"used_payload_bytes"`
	UnusedPayloadBytes uint64  `json:"unused_payload_bytes"`
	AccountedPackCount uint64  `json:"accounted_pack_count"`
	UnusedRatio        float64 `json:"unused_ratio"`
}

// StatsResult is the versioned JSON contract of `index stats`.
type StatsResult struct {
	SchemaVersion int         `json:"schema_version"`
	Source        StatsSource `json:"source"`
	Totals        StatsGroup  `json:"totals"`
	// StoredPhysicalSize is the number of bytes actually stored across every
	// location. It equals the logical physical size until a pack can have
	// several placements, and is reported separately so the difference is
	// never mistaken for drift.
	StoredPhysicalSize uint64       `json:"stored_physical_size"`
	Groups             []StatsGroup `json:"groups,omitempty"`

	UnknownTierPacks      uint64 `json:"unknown_tier_packs"`
	UnknownTypePacks      uint64 `json:"unknown_type_packs"`
	MixedTypePacks        uint64 `json:"mixed_type_packs"`
	RetentionUnknownPacks uint64 `json:"retention_unknown_packs"`
	// CreationTimeUnknownPacks and PhysicalSizeUnknownPacks are the coverage
	// gaps of the two facts that the size and history answers rest on. A pack
	// with no creation time is invisible to every time filter, and a pack with
	// no physical size understates every byte total, so both are stated rather
	// than folded into a total that would then look complete.
	CreationTimeUnknownPacks uint64 `json:"creation_time_unknown_packs"`
	PhysicalSizeUnknownPacks uint64 `json:"physical_size_unknown_packs"`
	// RetentionCounted records whether RetentionUnknownPacks was actually
	// measured. No aggregate carries the retention dimension, so the constant-
	// time path cannot know it; reporting zero there would present "not
	// measured" as "none found".
	RetentionCounted      bool   `json:"retention_counted"`
	UsageUnaccountedPacks uint64 `json:"usage_unaccounted_packs"`
	TierAggregatesUnbuilt bool   `json:"tier_aggregates_unbuilt,omitempty"`

	Drift   []AggregateDelta `json:"drift,omitempty"`
	Rebuilt *RebuildResult   `json:"rebuilt,omitempty"`
}

func (result StatsResult) HasDrift() bool { return len(result.Drift) != 0 }

// Stats reports repository composition. It reads the aggregate records, which
// is constant time, unless a grouping or verification requires the catalog.
func Stats(ctx context.Context, store Store, options StatsOptions) (StatsResult, error) {
	result := StatsResult{SchemaVersion: IntrospectSchemaVersion, Source: SourceAggregates}
	if err := validateStatsOptions(options); err != nil {
		return result, err
	}
	if options.Rebuild {
		rebuilt, err := RebuildPackAggregates(ctx, store, options.DryRun)
		if err != nil {
			return result, err
		}
		result.Rebuilt = &rebuilt
	}

	// A filter, a state grouping, or verification all need per-pack facts that
	// the aggregates cannot answer.
	needsCatalog := options.Verify || options.Tier != "" || options.Type != "" || options.State != "" ||
		containsDimension(options.GroupBy, "state")
	if !needsCatalog {
		if err := statsFromAggregates(ctx, store, options, &result); err != nil {
			return result, err
		}
		return result, nil
	}

	result.Source = SourceCatalog
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return result, err
	}
	if err := statsFromCatalog(packs, options, &result); err != nil {
		return result, err
	}
	if options.Verify {
		check := CheckResult{}
		if err := checkAggregates(ctx, store, packs, &check, 0); err != nil {
			return result, err
		}
		result.TierAggregatesUnbuilt = check.TierAggregatesUnbuilt
		if check.AggregateMismatch != 0 {
			// Reuse the rebuild path's delta computation so drift is reported
			// in exactly the form the repair would apply.
			deltas, deltaErr := RebuildPackAggregates(ctx, store, true)
			if deltaErr != nil {
				return result, deltaErr
			}
			result.Drift = deltas.Deltas
		}
	}
	return result, nil
}

func validateStatsOptions(options StatsOptions) error {
	for _, dimension := range options.GroupBy {
		switch dimension {
		case "tier", "type", "state":
		default:
			return fmt.Errorf("unsupported grouping %q; supported: tier, type, state", dimension)
		}
	}
	if options.Tier != "" && parseTierName(options.Tier) == 0 {
		return fmt.Errorf("unknown tier %q; supported: unknown, hot, cold, mirrored, single", options.Tier)
	}
	if options.Type != "" && parsePackTypeName(options.Type) == 0 {
		return fmt.Errorf("unknown pack type %q; supported: data, tree, mixed, unknown", options.Type)
	}
	if options.State != "" && parseLifecycleName(options.State) == 0 {
		return fmt.Errorf("unknown pack state %q", options.State)
	}
	return nil
}

func statsFromAggregates(ctx context.Context, store Store, options StatsOptions, result *StatsResult) error {
	all, found, err := readAggregate(ctx, store, schema.PackAggregateKey(schema.AggregateAll))
	if err != nil {
		return err
	}
	if found {
		result.Totals = aggregateToGroup("all", "all", all)
		result.StoredPhysicalSize = all.PhysicalSize
		result.UsageUnaccountedPacks = all.PackCount - min(all.AccountedPackCount, all.PackCount)
	}
	for _, dimension := range options.GroupBy {
		switch dimension {
		case "type":
			for kind := schema.AggregateData; kind < schema.AggregateAll; kind++ {
				aggregate, ok, readErr := readAggregate(ctx, store, schema.PackAggregateKey(kind))
				if readErr != nil {
					return readErr
				}
				if !ok || aggregate.PackCount == 0 {
					continue
				}
				result.Groups = append(result.Groups, aggregateToGroup("type", aggregateKindName(kind), aggregate))
			}
		case "tier":
			anyTier := false
			for _, tier := range schema.TierAggregateKinds() {
				aggregate, ok, readErr := readAggregate(ctx, store, schema.TierAggregateKey(tier))
				if readErr != nil {
					return readErr
				}
				if !ok {
					continue
				}
				anyTier = true
				if aggregate.PackCount == 0 {
					continue
				}
				result.Groups = append(result.Groups, aggregateToGroup("tier", tier.String(), aggregate))
			}
			if !anyTier && result.Totals.PackCount != 0 {
				result.TierAggregatesUnbuilt = true
			}
		}
	}
	// The type dimension is the only one that can report mixed and unknown
	// counts without a scan.
	for kind, target := range map[schema.AggregateKind]*uint64{
		schema.AggregateMixed: &result.MixedTypePacks, schema.AggregateUnknown: &result.UnknownTypePacks,
	} {
		aggregate, ok, readErr := readAggregate(ctx, store, schema.PackAggregateKey(kind))
		if readErr != nil {
			return readErr
		}
		if ok {
			*target = aggregate.PackCount
		}
	}
	unknownTier, ok, err := readAggregate(ctx, store, schema.TierAggregateKey(schema.TierUnknown))
	if err != nil {
		return err
	}
	if ok {
		result.UnknownTierPacks = unknownTier.PackCount
	}
	return nil
}

func statsFromCatalog(packs map[vaultic.ID]schema.PackRecord, options StatsOptions, result *StatsResult) error {
	groups := map[string]map[string]*StatsGroup{}
	for _, dimension := range options.GroupBy {
		groups[dimension] = map[string]*StatsGroup{}
	}
	for _, record := range packs {
		if !packMatchesStatsFilter(record, options) {
			continue
		}
		accumulateStatsGroup(&result.Totals, record)
		result.StoredPhysicalSize += record.PhysicalSize
		countPackFacts(record, result)
		for dimension, buckets := range groups {
			key := statsDimensionKey(dimension, record)
			bucket, ok := buckets[key]
			if !ok {
				bucket = &StatsGroup{Dimension: dimension, Key: key}
				buckets[key] = bucket
			}
			accumulateStatsGroup(bucket, record)
		}
	}
	result.Totals.Dimension, result.Totals.Key = "all", "all"
	// Only the catalog path visits every pack's retention source.
	result.RetentionCounted = true
	finishRatio(&result.Totals)
	for _, dimension := range options.GroupBy {
		keys := make([]string, 0, len(groups[dimension]))
		for key := range groups[dimension] {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			bucket := groups[dimension][key]
			finishRatio(bucket)
			result.Groups = append(result.Groups, *bucket)
		}
	}
	return nil
}

func packMatchesStatsFilter(record schema.PackRecord, options StatsOptions) bool {
	if options.Tier != "" && normalizedTier(record) != parseTierName(options.Tier) {
		return false
	}
	if options.Type != "" && record.Type != parsePackTypeName(options.Type) {
		return false
	}
	if options.State != "" && record.Lifecycle != parseLifecycleName(options.State) {
		return false
	}
	return true
}

func statsDimensionKey(dimension string, record schema.PackRecord) string {
	switch dimension {
	case "tier":
		return normalizedTier(record).String()
	case "type":
		return packTypeName(record.Type)
	case "state":
		return lifecycleName(record.Lifecycle)
	}
	return "unknown"
}

func accumulateStatsGroup(group *StatsGroup, record schema.PackRecord) {
	group.PackCount++
	group.PhysicalSize += record.PhysicalSize
	group.PayloadSize += record.PayloadSize
	group.HeaderSize += record.HeaderSize
	group.BlobCount += record.BlobCount
	if record.UsageKnown {
		group.AccountedPackCount++
		group.UsedPayloadBytes += record.UsedPayloadBytes
		group.UnusedPayloadBytes += record.UnusedPayloadBytes
	}
}

func countPackFacts(record schema.PackRecord, result *StatsResult) {
	if normalizedTier(record) == schema.TierUnknown {
		result.UnknownTierPacks++
	}
	switch record.Type {
	case schema.PackUnknown:
		result.UnknownTypePacks++
	case schema.PackMixed:
		result.MixedTypePacks++
	}
	if record.RetentionSource == 0 || record.RetentionSource == schema.RetentionUnknown {
		result.RetentionUnknownPacks++
	}
	if !record.UsageKnown {
		result.UsageUnaccountedPacks++
	}
	if !record.CreationTimeKnown {
		result.CreationTimeUnknownPacks++
	}
	if !record.PhysicalSizeKnown {
		result.PhysicalSizeUnknownPacks++
	}
}

func finishRatio(group *StatsGroup) {
	// The ratio is only meaningful over packs whose usage was actually
	// computed, so it is derived from the accounted payload rather than from
	// the whole payload.
	accounted := group.UsedPayloadBytes + group.UnusedPayloadBytes
	if accounted == 0 {
		group.UnusedRatio = 0
		return
	}
	group.UnusedRatio = float64(group.UnusedPayloadBytes) / float64(accounted)
}

func aggregateToGroup(dimension, key string, aggregate schema.PackAggregate) StatsGroup {
	group := StatsGroup{
		Dimension: dimension, Key: key,
		PackCount: aggregate.PackCount, PhysicalSize: aggregate.PhysicalSize,
		PayloadSize: aggregate.PayloadSize, HeaderSize: aggregate.HeaderSize,
		BlobCount: aggregate.BlobCount, UsedPayloadBytes: aggregate.UsedPayloadBytes,
		UnusedPayloadBytes: aggregate.UnusedPayloadBytes, AccountedPackCount: aggregate.AccountedPackCount,
	}
	finishRatio(&group)
	return group
}

func readAggregate(ctx context.Context, store Store, key []byte) (schema.PackAggregate, bool, error) {
	value, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return schema.PackAggregate{}, false, err
	}
	aggregate, decodeErr := schema.UnmarshalPackAggregate(value)
	if decodeErr != nil {
		if errors.Is(decodeErr, schema.ErrMalformed) {
			return schema.PackAggregate{}, false, nil
		}
		return schema.PackAggregate{}, false, decodeErr
	}
	return aggregate, true, nil
}

// PackFilter selects and orders pack catalog entries.
type PackFilter struct {
	Tier             string
	Type             string
	State            string
	CreatedBefore    time.Time
	CreatedAfter     time.Time
	MinSize          uint64
	MaxSize          uint64
	UnusedRatioAbove float64
	RetentionExpired bool
	RetentionUnknown bool
	DeletePending    bool
	Sort             string
	Limit            uint
	CountOnly        bool
	// Now anchors retention-expiry evaluation and is overridable for tests.
	Now time.Time
}

// PackEntry describes one catalog pack in the JSON contract.
type PackEntry struct {
	ID                 string  `json:"id"`
	Type               string  `json:"type"`
	Tier               string  `json:"tier"`
	State              string  `json:"state"`
	PhysicalSize       uint64  `json:"physical_size"`
	PhysicalSizeKnown  bool    `json:"physical_size_known"`
	PayloadSize        uint64  `json:"payload_size"`
	HeaderSize         uint64  `json:"header_size"`
	BlobCount          uint64  `json:"blob_count"`
	UsageKnown         bool    `json:"usage_known"`
	UsedPayloadBytes   uint64  `json:"used_payload_bytes"`
	UnusedPayloadBytes uint64  `json:"unused_payload_bytes"`
	UnusedRatio        float64 `json:"unused_ratio"`
	CreatedAt          int64   `json:"created_at,omitempty"`
	CreationTimeKnown  bool    `json:"creation_time_known"`
	MinRetentionUntil  int64   `json:"min_retention_until,omitempty"`
	RetentionSource    string  `json:"retention_source"`
	DeleteAfter        int64   `json:"delete_after,omitempty"`
	StorageClass       string  `json:"storage_class,omitempty"`
}

// PacksResult is the versioned JSON contract of `index packs`.
type PacksResult struct {
	SchemaVersion int         `json:"schema_version"`
	Scanned       uint64      `json:"scanned"`
	Matched       uint64      `json:"matched"`
	Returned      uint64      `json:"returned"`
	Packs         []PackEntry `json:"packs,omitempty"`

	UnknownTierPacks      uint64 `json:"unknown_tier_packs"`
	UnknownTypePacks      uint64 `json:"unknown_type_packs"`
	RetentionUnknownPacks uint64 `json:"retention_unknown_packs"`
	UsageUnaccountedPacks uint64 `json:"usage_unaccounted_packs"`

	// Undecidable counts packs that a filter could neither include nor exclude
	// on the evidence available, because the fact the filter asks about was
	// never recorded. Without this an operator would read "matched 0" as "no
	// such packs" when the truth is "cannot tell".
	Undecidable            uint64 `json:"undecidable"`
	UndecidableCreatedTime uint64 `json:"undecidable_creation_time"`
	UndecidableRetention   uint64 `json:"undecidable_retention"`
	UndecidableUsage       uint64 `json:"undecidable_usage"`
}

// QueryPacks filters, orders, and limits the pack catalog.
func QueryPacks(ctx context.Context, store Store, filter PackFilter) (PacksResult, error) {
	result := PacksResult{SchemaVersion: IntrospectSchemaVersion}
	if err := validatePackFilter(filter); err != nil {
		return result, err
	}
	now := filter.Now
	if now.IsZero() {
		now = time.Now()
	}
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return result, err
	}
	result.Scanned = uint64(len(packs))

	entries := make([]PackEntry, 0, len(packs))
	for id, record := range packs {
		if !packMatchesFilter(id, record, filter, now) {
			countUndecidable(record, filter, &result)
			continue
		}
		result.Matched++
		countPackFactsForPacks(record, &result)
		if filter.CountOnly {
			continue
		}
		entries = append(entries, packEntry(id, record))
	}
	if filter.CountOnly {
		return result, nil
	}
	sortPackEntries(entries, filter.Sort)
	if filter.Limit > 0 && uint(len(entries)) > filter.Limit {
		entries = entries[:filter.Limit]
	}
	result.Packs = entries
	result.Returned = uint64(len(entries))
	return result, nil
}

func validatePackFilter(filter PackFilter) error {
	switch filter.Sort {
	case "", "size", "created", "unused", "unused-ratio", "delete-after", "id":
	default:
		return fmt.Errorf("unsupported sort %q; supported: id, size, created, unused, unused-ratio, delete-after", filter.Sort)
	}
	if filter.Tier != "" && parseTierName(filter.Tier) == 0 {
		return fmt.Errorf("unknown tier %q", filter.Tier)
	}
	if filter.Type != "" && parsePackTypeName(filter.Type) == 0 {
		return fmt.Errorf("unknown pack type %q", filter.Type)
	}
	if filter.State != "" && parseLifecycleName(filter.State) == 0 {
		return fmt.Errorf("unknown pack state %q", filter.State)
	}
	if filter.MaxSize != 0 && filter.MinSize > filter.MaxSize {
		return fmt.Errorf("--min-size exceeds --max-size")
	}
	return nil
}

func packMatchesFilter(id vaultic.ID, record schema.PackRecord, filter PackFilter, now time.Time) bool {
	_ = id
	if filter.Tier != "" && normalizedTier(record) != parseTierName(filter.Tier) {
		return false
	}
	if filter.Type != "" && record.Type != parsePackTypeName(filter.Type) {
		return false
	}
	if filter.State != "" && record.Lifecycle != parseLifecycleName(filter.State) {
		return false
	}
	if filter.DeletePending && record.Lifecycle != schema.PackDeletePending {
		return false
	}
	if filter.MinSize != 0 && record.PhysicalSize < filter.MinSize {
		return false
	}
	if filter.MaxSize != 0 && record.PhysicalSize > filter.MaxSize {
		return false
	}
	// A pack whose creation time is unknown must never satisfy a time filter:
	// an unknown timestamp is not evidence of being inside or outside a range.
	if !filter.CreatedAfter.IsZero() {
		if !record.CreationTimeKnown || record.CreationTime <= filter.CreatedAfter.UnixNano() {
			return false
		}
	}
	if !filter.CreatedBefore.IsZero() {
		if !record.CreationTimeKnown || record.CreationTime >= filter.CreatedBefore.UnixNano() {
			return false
		}
	}
	retentionUnknown := record.RetentionSource == 0 || record.RetentionSource == schema.RetentionUnknown
	if filter.RetentionUnknown && !retentionUnknown {
		return false
	}
	if filter.RetentionExpired {
		// Retention can only be expired when it was known in the first place.
		if retentionUnknown || record.MinRetentionUntil > now.UnixNano() {
			return false
		}
	}
	if filter.UnusedRatioAbove > 0 {
		if !record.UsageKnown || entryUnusedRatio(record) <= filter.UnusedRatioAbove {
			return false
		}
	}
	return true
}

func countPackFactsForPacks(record schema.PackRecord, result *PacksResult) {
	if normalizedTier(record) == schema.TierUnknown {
		result.UnknownTierPacks++
	}
	if record.Type == schema.PackUnknown {
		result.UnknownTypePacks++
	}
	if record.RetentionSource == 0 || record.RetentionSource == schema.RetentionUnknown {
		result.RetentionUnknownPacks++
	}
	if !record.UsageKnown {
		result.UsageUnaccountedPacks++
	}
}

// countUndecidable records why a filter had to exclude a pack for lack of
// evidence rather than on the merits. Only the filters that consult a fact
// which may be missing can produce this; a tier or type filter always has an
// answer, because "unknown" is itself a value there.
func countUndecidable(record schema.PackRecord, filter PackFilter, result *PacksResult) {
	undecidable := false
	if (!filter.CreatedAfter.IsZero() || !filter.CreatedBefore.IsZero()) && !record.CreationTimeKnown {
		result.UndecidableCreatedTime++
		undecidable = true
	}
	if filter.RetentionExpired && (record.RetentionSource == 0 || record.RetentionSource == schema.RetentionUnknown) {
		result.UndecidableRetention++
		undecidable = true
	}
	if filter.UnusedRatioAbove > 0 && !record.UsageKnown {
		result.UndecidableUsage++
		undecidable = true
	}
	if undecidable {
		result.Undecidable++
	}
}

func entryUnusedRatio(record schema.PackRecord) float64 {
	accounted := record.UsedPayloadBytes + record.UnusedPayloadBytes
	if !record.UsageKnown || accounted == 0 {
		return 0
	}
	return float64(record.UnusedPayloadBytes) / float64(accounted)
}

func packEntry(id vaultic.ID, record schema.PackRecord) PackEntry {
	return PackEntry{
		ID: id.String(), Type: packTypeName(record.Type), Tier: normalizedTier(record).String(),
		State: lifecycleName(record.Lifecycle), PhysicalSize: record.PhysicalSize,
		PhysicalSizeKnown: record.PhysicalSizeKnown, PayloadSize: record.PayloadSize,
		HeaderSize: record.HeaderSize, BlobCount: record.BlobCount,
		UsageKnown: record.UsageKnown, UsedPayloadBytes: record.UsedPayloadBytes,
		UnusedPayloadBytes: record.UnusedPayloadBytes, UnusedRatio: entryUnusedRatio(record),
		CreatedAt: record.CreationTime, CreationTimeKnown: record.CreationTimeKnown,
		MinRetentionUntil: record.MinRetentionUntil, RetentionSource: retentionSourceName(record.RetentionSource),
		DeleteAfter: record.DeleteAfter, StorageClass: record.StorageClass,
	}
}

func sortPackEntries(entries []PackEntry, order string) {
	// Every ordering falls back to the pack ID so output is deterministic and
	// golden-testable rather than dependent on map iteration.
	less := func(left, right PackEntry) bool { return left.ID < right.ID }
	switch order {
	case "size":
		less = func(left, right PackEntry) bool {
			if left.PhysicalSize != right.PhysicalSize {
				return left.PhysicalSize > right.PhysicalSize
			}
			return left.ID < right.ID
		}
	case "created":
		less = func(left, right PackEntry) bool {
			if left.CreatedAt != right.CreatedAt {
				return left.CreatedAt > right.CreatedAt
			}
			return left.ID < right.ID
		}
	case "unused":
		less = func(left, right PackEntry) bool {
			if left.UnusedPayloadBytes != right.UnusedPayloadBytes {
				return left.UnusedPayloadBytes > right.UnusedPayloadBytes
			}
			return left.ID < right.ID
		}
	case "unused-ratio":
		less = func(left, right PackEntry) bool {
			if left.UnusedRatio != right.UnusedRatio {
				return left.UnusedRatio > right.UnusedRatio
			}
			return left.ID < right.ID
		}
	case "delete-after":
		less = func(left, right PackEntry) bool {
			if left.DeleteAfter != right.DeleteAfter {
				return left.DeleteAfter < right.DeleteAfter
			}
			return left.ID < right.ID
		}
	}
	sort.Slice(entries, func(i, j int) bool { return less(entries[i], entries[j]) })
}

func normalizedTier(record schema.PackRecord) schema.PackTier {
	if record.Tier == 0 {
		return schema.TierUnknown
	}
	return record.Tier
}

func packTypeName(value schema.PackType) string {
	switch value {
	case schema.PackData:
		return "data"
	case schema.PackTree:
		return "tree"
	case schema.PackMixed:
		return "mixed"
	}
	return "unknown"
}

func parsePackTypeName(name string) schema.PackType {
	switch strings.ToLower(name) {
	case "data":
		return schema.PackData
	case "tree":
		return schema.PackTree
	case "mixed":
		return schema.PackMixed
	case "unknown":
		return schema.PackUnknown
	}
	return 0
}

func parseTierName(name string) schema.PackTier {
	for _, tier := range schema.TierAggregateKinds() {
		if strings.EqualFold(name, tier.String()) {
			return tier
		}
	}
	return 0
}

var lifecycleNames = map[schema.PackLifecycle]string{
	schema.PackImported: "imported", schema.PackPublished: "published",
	schema.PackExportPending: "export-pending", schema.PackDeletePending: "delete-pending",
	schema.PackDeleted: "deleted", schema.PackOrphaned: "orphaned", schema.PackStateUnknown: "unknown",
}

func lifecycleName(value schema.PackLifecycle) string {
	if name, ok := lifecycleNames[value]; ok {
		return name
	}
	return "unknown"
}

func parseLifecycleName(name string) schema.PackLifecycle {
	for value, candidate := range lifecycleNames {
		if strings.EqualFold(name, candidate) {
			return value
		}
	}
	return 0
}

func retentionSourceName(value schema.RetentionSource) string {
	switch value {
	case schema.RetentionConfig:
		return "config"
	case schema.RetentionBackend:
		return "backend"
	}
	return "unknown"
}

func aggregateKindName(kind schema.AggregateKind) string {
	switch kind {
	case schema.AggregateData:
		return "data"
	case schema.AggregateTree:
		return "tree"
	case schema.AggregateMixed:
		return "mixed"
	case schema.AggregateUnknown:
		return "unknown"
	}
	return "all"
}

func containsDimension(dimensions []string, target string) bool {
	for _, dimension := range dimensions {
		if dimension == target {
			return true
		}
	}
	return false
}
