// Package maintenance implements operator-controlled SlateDB index workflows.
package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const scanPageSize = 10_000

type Store interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error)
	MarkIndexPublished(context.Context, schema.ID, []schema.ID) (uint64, error)
	WriteMutableBatch(context.Context, []daemon.Mutation, [][]byte, bool) error
}

type LegacySource interface {
	vaultic.ListerLoaderUnpacked
}

type LegacyDestination interface {
	SaveLegacyIndex(context.Context, *legacyindex.Index) (vaultic.ID, error)
}

type legacyExportVerifier interface {
	vaultic.LoaderUnpacked
}

type ExportOptions struct {
	Full          bool
	DryRun        bool
	Verify        bool
	Since         uint64
	PacksPerIndex uint
}

type ExportResult struct {
	PacksSelected  uint64       `json:"packs_selected"`
	BlobsSelected  uint64       `json:"blobs_selected"`
	IndexesWritten uint64       `json:"indexes_written"`
	ExportSequence uint64       `json:"export_sequence,omitempty"`
	IndexIDs       []vaultic.ID `json:"index_ids,omitempty"`
}

type CheckResult struct {
	LegacyIndexes             uint64 `json:"legacy_indexes"`
	LegacySnapshots           uint64 `json:"legacy_snapshots"`
	SlateDBSnapshots          uint64 `json:"slatedb_snapshots"`
	LegacyLocations           uint64 `json:"legacy_locations"`
	SlateDBLocations          uint64 `json:"slatedb_locations"`
	MissingInSlateDB          uint64 `json:"missing_in_slatedb"`
	MissingInLegacy           uint64 `json:"missing_in_legacy"`
	MissingPacks              uint64 `json:"missing_packs"`
	InvalidPacks              uint64 `json:"invalid_packs"`
	AggregateMismatch         uint64 `json:"aggregate_mismatches"`
	ReverseEdgeMismatch       uint64 `json:"reverse_edge_mismatches"`
	UnresolvedReferences      uint64 `json:"unresolved_references"`
	SnapshotMismatch          uint64 `json:"snapshot_mismatches"`
	UnresolvedSnapshots       uint64 `json:"unresolved_snapshots"`
	PendingCrawlDebt          uint64 `json:"pending_crawl_debt"`
	PendingExports            uint64 `json:"pending_exports"`
	FailedExports             uint64 `json:"failed_exports"`
	ExportCheckpoints         uint64 `json:"export_checkpoints"`
	MixedPacks                uint64 `json:"mixed_packs"`
	UnknownPacks              uint64 `json:"unknown_packs"`
	UnknownTierPacks          uint64 `json:"unknown_tier_packs"`
	RetentionUnknownPacks     uint64 `json:"retention_unknown_packs"`
	UsageUnaccountedPacks     uint64 `json:"usage_unaccounted_packs"`
	PlacementRecordsMalformed uint64 `json:"placement_records_malformed"`
	MissingPlacementRecords   uint64 `json:"missing_placement_records"`
	BackendPackMismatch       uint64 `json:"backend_pack_mismatches"`
	DerivedTierMismatch       uint64 `json:"derived_tier_mismatches"`
	PacksBelowDurability      uint64 `json:"packs_below_durability"`
	UnknownPlacementBackends  uint64 `json:"unknown_placement_backends"`
	// TierAggregatesUnbuilt marks a repository written before the tier
	// dimension existed. It is a pending rebuild, not drift.
	TierAggregatesUnbuilt bool `json:"tier_aggregates_unbuilt,omitempty"`
	// HistoryEventsMalformed counts unreadable pack history records. History
	// is advisory and derived, so this is reported but never makes the check
	// dirty.
	HistoryEventsMalformed uint64    `json:"history_events_malformed"`
	GCCandidates           uint64    `json:"gc_candidates"`
	Warnings               uint64    `json:"warnings"`
	Findings               []Finding `json:"findings,omitempty"`
}

func (result CheckResult) Clean() bool {
	return result.MissingInSlateDB == 0 && result.MissingInLegacy == 0 && result.MissingPacks == 0 && result.InvalidPacks == 0 && result.AggregateMismatch == 0 && result.ReverseEdgeMismatch == 0 && result.SnapshotMismatch == 0 && result.FailedExports == 0 && result.MissingPlacementRecords == 0 && result.BackendPackMismatch == 0 && result.DerivedTierMismatch == 0 && result.PacksBelowDurability == 0
}

func (result CheckResult) HasWarnings() bool { return result.Warnings != 0 }

type CheckOptions struct {
	LegacyOnly       bool
	SlateDBOnly      bool
	IncludeCrawlDebt bool
	MaxFindings      uint
	PlacementModel   PlacementModel
}

type Finding struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
	Want string `json:"want,omitempty"`
	Got  string `json:"got,omitempty"`
}

type RebuildResult struct {
	PacksScanned              uint64           `json:"packs_scanned"`
	AggregatesChanged         uint64           `json:"aggregates_changed"`
	PlacementRecordsChanged   uint64           `json:"placement_records_changed"`
	BackendPackRecordsChanged uint64           `json:"backend_pack_records_changed"`
	TierSummaryChanged        uint64           `json:"tier_summary_changed"`
	UpdateSequence            uint64           `json:"update_sequence"`
	Deltas                    []AggregateDelta `json:"deltas,omitempty"`
}

type AggregateDelta struct {
	Kind   schema.AggregateKind  `json:"kind,omitempty"`
	Tier   schema.PackTier       `json:"tier,omitempty"`
	Key    string                `json:"key"`
	Before *schema.PackAggregate `json:"before,omitempty"`
	After  schema.PackAggregate  `json:"after"`
}

type location struct {
	BlobID             vaultic.ID
	PackID             vaultic.ID
	Type               vaultic.BlobType
	Offset             uint
	Length             uint
	UncompressedLength uint
}

type packLocationStats struct {
	types    map[vaultic.ID][]schema.BlobType
	counts   map[vaultic.ID]uint64
	payloads map[vaultic.ID]uint64
}

func Export(ctx context.Context, store Store, destination LegacyDestination, options ExportOptions) (ExportResult, error) {
	var result ExportResult
	if options.PacksPerIndex == 0 {
		options.PacksPerIndex = 1_000
	}
	if options.PacksPerIndex > uint(^uint(0)>>1) {
		return result, fmt.Errorf("packs per index exceeds platform limit")
	}
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return result, err
	}
	exported, err := loadExportProvenance(ctx, store)
	if err != nil {
		return result, err
	}
	selected := make(map[vaultic.ID]schema.PackRecord)
	for id, record := range packs {
		if record.Lifecycle == schema.PackDeleted {
			continue
		}
		sequence, hasCheckpoint := exported[id]
		if options.Full || (options.Since > 0 && (!hasCheckpoint || sequence > options.Since)) || (options.Since == 0 && record.Lifecycle != schema.PackPublished) {
			selected[id] = record
		}
	}
	byPack, err := loadBlobLocations(ctx, store, selected)
	if err != nil {
		return result, err
	}
	ids := sortedPackIDs(selected)
	result.PacksSelected = uint64(len(ids))
	for _, blobs := range byPack {
		result.BlobsSelected += uint64(len(blobs))
	}
	for start := 0; start < len(ids); start += int(options.PacksPerIndex) {
		end := min(start+int(options.PacksPerIndex), len(ids))
		index := legacyindex.NewIndex()
		for _, id := range ids[start:end] {
			index.StorePack(id, byPack[id])
		}
		index.Finalize()
		if options.DryRun {
			continue
		}
		indexID, saveErr := destination.SaveLegacyIndex(ctx, index)
		if saveErr != nil {
			return result, fmt.Errorf("save legacy index: %w", saveErr)
		}
		if options.Verify {
			verifier, ok := destination.(legacyExportVerifier)
			if !ok {
				return result, fmt.Errorf("export destination does not support verification")
			}
			encoded, loadErr := verifier.LoadUnpacked(ctx, vaultic.IndexFile, indexID)
			if loadErr != nil {
				return result, fmt.Errorf("verify exported index %s: %w", indexID.Str(), loadErr)
			}
			if _, decodeErr := legacyindex.DecodeIndex(encoded, indexID); decodeErr != nil {
				return result, fmt.Errorf("verify exported index %s: %w", indexID.Str(), decodeErr)
			}
		}
		result.IndexesWritten++
		result.IndexIDs = append(result.IndexIDs, indexID)
		packIDs := make([]schema.ID, end-start)
		for index, id := range ids[start:end] {
			packIDs[index] = schema.ID(id)
		}
		sequence, putErr := store.MarkIndexPublished(ctx, schema.ID(indexID), packIDs)
		if putErr != nil {
			return result, fmt.Errorf("checkpoint exported index %s: %w", indexID.Str(), putErr)
		}
		result.ExportSequence = max(result.ExportSequence, sequence)
	}
	return result, nil
}

func Check(ctx context.Context, source LegacySource, store Store, maxFindings uint) (CheckResult, error) {
	return CheckWithOptions(ctx, source, store, CheckOptions{MaxFindings: maxFindings})
}

func CheckWithOptions(ctx context.Context, source LegacySource, store Store, options CheckOptions) (CheckResult, error) {
	if options.LegacyOnly && options.SlateDBOnly {
		return CheckResult{}, fmt.Errorf("legacy-only and SlateDB-only checks are mutually exclusive")
	}
	var result CheckResult
	legacy := make(map[string]struct{})
	legacyPacks := make(map[vaultic.ID]uint64)
	legacySnapshots := make(map[vaultic.ID]struct{})
	var err error
	if !options.SlateDBOnly {
		legacy, legacyPacks, result.LegacyIndexes, err = loadLegacyLocations(ctx, source)
		if err != nil {
			return result, err
		}
		result.LegacyLocations = uint64(len(legacy))
		legacySnapshots, err = loadLegacySnapshots(ctx, source)
		if err != nil {
			return result, err
		}
		result.LegacySnapshots = uint64(len(legacySnapshots))
	}
	if options.LegacyOnly {
		return result, nil
	}
	slatedb, packStats, err := loadSlateDBLocations(ctx, store)
	if err != nil {
		return result, err
	}
	result.SlateDBLocations = uint64(len(slatedb))
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return result, err
	}
	if !options.SlateDBOnly {
		for id, count := range legacyPacks {
			if _, found := packs[id]; !found {
				if _, exactFound, getErr := store.Get(ctx, schema.PackKey(schema.ID(id))); getErr != nil {
					return result, getErr
				} else if exactFound {
					return result, fmt.Errorf("pack scan omitted existing pack %s", id.String())
				}
				if count == 0 {
					result.Warnings++
					addFinding(&result, options.MaxFindings, Finding{Kind: "catalog_only_pack", Key: id.String(), Got: "zero blob locations"})
				} else {
					result.MissingPacks++
					addFinding(&result, options.MaxFindings, Finding{Kind: "missing_pack", Key: id.String(), Want: "slatedb", Got: fmt.Sprintf("legacy blobs=%d", count)})
				}
			}
		}
		for id := range packs {
			if _, found := legacyPacks[id]; !found {
				result.MissingPacks++
				addFinding(&result, options.MaxFindings, Finding{Kind: "missing_pack", Key: id.String(), Want: "legacy"})
			}
		}
		for key := range legacy {
			if _, found := slatedb[key]; !found {
				result.MissingInSlateDB++
				addFinding(&result, options.MaxFindings, Finding{Kind: "missing_blob", Key: key})
			}
		}
		for key := range slatedb {
			if _, found := legacy[key]; !found {
				result.MissingInLegacy++
				addFinding(&result, options.MaxFindings, Finding{Kind: "unexpected_blob", Key: key})
			}
		}
	}
	if err := checkPackCatalog(packs, packStats, &result, options.MaxFindings); err != nil {
		return result, err
	}
	if err := checkAggregates(ctx, store, packs, &result, options.MaxFindings); err != nil {
		return result, err
	}
	checkPackLifetime(packs, &result)
	if err := checkPlacementRecords(ctx, store, packs, options.PlacementModel, &result, options.MaxFindings); err != nil {
		return result, err
	}
	checkPackHistory(ctx, store, &result)
	if err := checkOperationalState(ctx, store, options, packs, &result); err != nil {
		return result, err
	}
	if !options.SlateDBOnly {
		if err := checkExportProvenance(ctx, source, store, packs, &result, options.MaxFindings); err != nil {
			return result, err
		}
	}
	if err := checkReferences(ctx, store, &result, options.MaxFindings); err != nil {
		return result, err
	}
	if err := checkSnapshots(ctx, store, legacySnapshots, options.SlateDBOnly, &result, options.MaxFindings); err != nil {
		return result, err
	}
	return result, nil
}

// aggregateTarget is one aggregate record to compare and rewrite. Type
// aggregates always exist once a repository has any pack; tier aggregates are
// only materialized for tiers that actually hold packs, so an absent record
// for an empty tier is correct rather than drift.
type aggregateTarget struct {
	key      []byte
	delta    AggregateDelta
	expected schema.PackAggregate
	optional bool
}

// typeAggregateCount is how many aggregate targets belong to the type
// dimension; the remainder are the tier dimension.
const typeAggregateCount = int(schema.AggregateAll - schema.AggregateData + 1)

func aggregateTargets(rebuilt map[schema.AggregateKind]schema.PackAggregate, tiers map[schema.PackTier]schema.PackAggregate) []aggregateTarget {
	targets := make([]aggregateTarget, 0, len(rebuilt)+len(tiers))
	for kind := schema.AggregateData; kind <= schema.AggregateAll; kind++ {
		key := schema.PackAggregateKey(kind)
		targets = append(targets, aggregateTarget{key: key, expected: rebuilt[kind], delta: AggregateDelta{Kind: kind, Key: string(key)}})
	}
	for _, tier := range schema.TierAggregateKinds() {
		key := schema.TierAggregateKey(tier)
		targets = append(targets, aggregateTarget{key: key, expected: tiers[tier], delta: AggregateDelta{Tier: tier, Key: string(key)}, optional: true})
	}
	return targets
}

func RebuildPackAggregates(ctx context.Context, store Store, dryRun bool) (RebuildResult, error) {
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return RebuildResult{}, err
	}
	sequence, err := nextAggregateSequence(ctx, store)
	if err != nil {
		return RebuildResult{}, err
	}
	records := make([]schema.PackRecord, 0, len(packs))
	for _, record := range packs {
		records = append(records, record)
	}
	rebuilt, err := schema.RebuildPackAggregates(records, sequence)
	if err != nil {
		return RebuildResult{}, err
	}
	rebuiltTiers, err := schema.RebuildTierAggregates(records, sequence)
	if err != nil {
		return RebuildResult{}, err
	}
	targets := aggregateTargets(rebuilt, rebuiltTiers)

	result := RebuildResult{PacksScanned: uint64(len(records)), UpdateSequence: sequence}
	needsRebuild := false
	write := make([]bool, len(targets))
	for index, target := range targets {
		current, found, getErr := store.Get(ctx, target.key)
		if getErr != nil {
			return result, getErr
		}
		var currentRecord schema.PackAggregate
		if found {
			currentRecord, getErr = schema.UnmarshalPackAggregate(current)
			if getErr != nil {
				if !errors.Is(getErr, schema.ErrMalformed) {
					return result, getErr
				}
				found = false
			}
		}
		write[index] = found || !target.optional || !emptyAggregate(target.expected)
		comparisonCurrent, comparisonExpected := currentRecord, target.expected
		comparisonCurrent.UpdateSequence, comparisonExpected.UpdateSequence = 0, 0
		if found && comparisonCurrent == comparisonExpected {
			continue
		}
		if !found && target.optional && emptyAggregate(target.expected) {
			continue
		}
		result.AggregatesChanged++
		needsRebuild = true
		delta := target.delta
		delta.After = target.expected
		if found {
			stored := currentRecord
			delta.Before = &stored
		}
		result.Deltas = append(result.Deltas, delta)
	}
	if needsRebuild && !dryRun {
		puts := make([]daemon.Mutation, 0, len(targets))
		for index, target := range targets {
			if !write[index] {
				continue
			}
			encoded, encodeErr := target.expected.MarshalBinary()
			if encodeErr != nil {
				return result, encodeErr
			}
			puts = append(puts, daemon.Mutation{Key: target.key, Value: encoded})
		}
		if err := store.WriteMutableBatch(ctx, puts, nil, true); err != nil {
			return result, fmt.Errorf("write pack aggregates: %w", err)
		}
	}
	return result, nil
}

func emptyAggregate(aggregate schema.PackAggregate) bool {
	aggregate.UpdateSequence = 0
	return aggregate == schema.PackAggregate{}
}

func loadPacks(ctx context.Context, store Store) (map[vaultic.ID]schema.PackRecord, error) {
	result := make(map[vaultic.ID]schema.PackRecord)
	err := scan(ctx, store, []byte("p:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPack {
			return fmt.Errorf("invalid pack key %q", entry.Key)
		}
		record, err := schema.UnmarshalPackRecord(entry.Value)
		if err != nil {
			return fmt.Errorf("decode pack %x: %w", parsed.ID, err)
		}
		result[vaultic.ID(parsed.ID)] = record
		return nil
	})
	return result, err
}

func loadBlobLocations(ctx context.Context, store Store, selected map[vaultic.ID]schema.PackRecord) (map[vaultic.ID]pack.Blobs, error) {
	result := make(map[vaultic.ID]pack.Blobs, len(selected))
	err := scan(ctx, store, []byte("b:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyBlob {
			return fmt.Errorf("invalid blob key %q", entry.Key)
		}
		record, err := schema.UnmarshalBlobRecord(entry.Value)
		if err != nil {
			return err
		}
		for _, item := range record.Locations {
			packID := vaultic.ID(item.PackID)
			if _, found := selected[packID]; !found {
				continue
			}
			result[packID] = append(result[packID], pack.Blob{BlobHandle: vaultic.BlobHandle{ID: vaultic.ID(parsed.ID), Type: vaultic.BlobType(item.Type)}, Offset: uint(item.Offset), Length: uint(item.Length), UncompressedLength: uint(item.UncompressedSize)})
		}
		return nil
	})
	return result, err
}

func loadLegacyLocations(ctx context.Context, source LegacySource) (map[string]struct{}, map[vaultic.ID]uint64, uint64, error) {
	result := make(map[string]struct{})
	packs := make(map[vaultic.ID]uint64)
	var indexes uint64
	err := legacyindex.ForAllIndexes(ctx, source, source, func(_ vaultic.ID, index *legacyindex.Index, loadErr error) error {
		indexes++
		if loadErr != nil {
			return loadErr
		}
		for item := range index.Values() {
			result[locationKey(location{BlobID: item.Blob.ID, PackID: item.Pack, Type: item.Blob.Type, Offset: item.Blob.Offset, Length: item.Blob.Length, UncompressedLength: item.Blob.UncompressedLength})] = struct{}{}
			packs[item.Pack]++
		}
		for id := range index.Packs() {
			if _, found := packs[id]; !found {
				packs[id] = 0
			}
		}
		return nil
	})
	return result, packs, indexes, err
}

func loadLegacySnapshots(ctx context.Context, source LegacySource) (map[vaultic.ID]struct{}, error) {
	result := make(map[vaultic.ID]struct{})
	err := source.List(ctx, vaultic.SnapshotFile, func(id vaultic.ID, _ int64) error {
		result[id] = struct{}{}
		return nil
	})
	return result, err
}

type referenceStats struct {
	inodes    map[[2]uint64]struct{}
	manifests map[schema.ID]struct{}
}

func checkReferences(ctx context.Context, store Store, result *CheckResult, maxFindings uint) error {
	stats := make(map[schema.ID]*referenceStats)
	getStats := func(id schema.ID) *referenceStats {
		value := stats[id]
		if value == nil {
			value = &referenceStats{inodes: make(map[[2]uint64]struct{}), manifests: make(map[schema.ID]struct{})}
			stats[id] = value
		}
		return value
	}
	if err := scan(ctx, store, []byte("ri:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalReverseInodeRecord(entry.Value)
		if err != nil {
			return err
		}
		if record.State == schema.ReferenceUnresolved {
			result.UnresolvedReferences++
			result.Warnings++
			return nil
		}
		getStats(parsed.ID).inodes[[2]uint64{uint64(parsed.FSID), parsed.Inode}] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	if err := scan(ctx, store, []byte("rm:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalReverseManifestRecord(entry.Value)
		if err != nil {
			return err
		}
		if record.State == schema.ReferenceUnresolved {
			result.UnresolvedReferences++
			result.Warnings++
			return nil
		}
		getStats(parsed.ID).manifests[parsed.SecondID] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	counts := make(map[schema.ID]schema.ReferenceCountRecord)
	if err := scan(ctx, store, []byte("rc:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalReferenceCountRecord(entry.Value)
		if err != nil {
			return err
		}
		counts[parsed.ID] = record
		return nil
	}); err != nil {
		return err
	}
	for id, expected := range stats {
		count, found := counts[id]
		minimum := uint64(len(expected.inodes) + len(expected.manifests))
		if !found || count.DistinctInodes != uint64(len(expected.inodes)) || count.DistinctManifests != uint64(len(expected.manifests)) || count.TotalReferences < minimum {
			result.ReverseEdgeMismatch++
			addFinding(result, maxFindings, Finding{Kind: "reference_count_drift", Key: vaultic.ID(id).String(), Want: fmt.Sprintf("inodes=%d manifests=%d total>=%d", len(expected.inodes), len(expected.manifests), minimum), Got: fmt.Sprintf("inodes=%d manifests=%d total=%d", count.DistinctInodes, count.DistinctManifests, count.TotalReferences)})
		}
	}
	for id, count := range counts {
		if _, found := stats[id]; !found && (count.DistinctInodes != 0 || count.DistinctManifests != 0 || count.TotalReferences != 0) {
			result.ReverseEdgeMismatch++
			addFinding(result, maxFindings, Finding{Kind: "missing_reverse_edge", Key: vaultic.ID(id).String(), Got: fmt.Sprintf("inodes=%d manifests=%d total=%d", count.DistinctInodes, count.DistinctManifests, count.TotalReferences)})
		}
	}
	return nil
}

func checkSnapshots(ctx context.Context, store Store, legacy map[vaultic.ID]struct{}, slatedbOnly bool, result *CheckResult, maxFindings uint) error {
	slatedb := make(map[vaultic.ID]struct{})
	err := scan(ctx, store, []byte("s:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalSnapshotRecord(entry.Value)
		if err != nil {
			return err
		}
		id := vaultic.ID(parsed.ID)
		slatedb[id] = struct{}{}
		rootKey := schema.DirectoryRevisionKey(record.RootFSID, record.RootInode, record.RootRevision)
		if _, found, getErr := store.Get(ctx, rootKey); getErr != nil {
			return getErr
		} else if !found {
			result.SnapshotMismatch++
			addFinding(result, maxFindings, Finding{Kind: "missing_snapshot_root", Key: id.String()})
		}
		return nil
	})
	if err != nil {
		return err
	}
	result.SlateDBSnapshots = uint64(len(slatedb))
	if slatedbOnly {
		return nil
	}
	for id := range legacy {
		if _, found := slatedb[id]; !found {
			checkpoint, checkpointFound, err := store.Get(ctx, schema.SnapshotImportCheckpointKey(schema.ID(id)))
			if err != nil {
				return err
			}
			if checkpointFound {
				if _, err := schema.UnmarshalSnapshotImportCheckpointRecord(checkpoint); err != nil {
					return err
				}
				result.UnresolvedSnapshots++
				result.Warnings++
				addFinding(result, maxFindings, Finding{Kind: "unresolved_snapshot", Key: id.String(), Got: "imported traversal has no normalized root identity"})
			} else {
				result.SnapshotMismatch++
				addFinding(result, maxFindings, Finding{Kind: "missing_snapshot", Key: id.String(), Want: "slatedb"})
			}
		}
	}
	for id := range slatedb {
		if _, found := legacy[id]; !found {
			result.SnapshotMismatch++
			addFinding(result, maxFindings, Finding{Kind: "missing_snapshot", Key: id.String(), Want: "legacy"})
		}
	}
	return nil
}

func checkOperationalState(ctx context.Context, store Store, options CheckOptions, packs map[vaultic.ID]schema.PackRecord, result *CheckResult) error {
	for id, record := range packs {
		switch record.Type {
		case schema.PackMixed:
			result.MixedPacks++
		case schema.PackUnknown:
			result.UnknownPacks++
			result.Warnings++
			addFinding(result, options.MaxFindings, Finding{Kind: "unknown_pack_type", Key: id.String()})
		}
		if record.Lifecycle == schema.PackImported || record.Lifecycle == schema.PackExportPending {
			result.PendingExports++
			result.Warnings++
		}
	}
	if err := scan(ctx, store, []byte("q:"), func(entry daemon.KeyValue) error {
		record, err := schema.UnmarshalCrawlDebtRecord(entry.Value)
		if err != nil {
			return err
		}
		if record.Status == schema.DebtPending || record.Status == schema.DebtFailed {
			result.PendingCrawlDebt++
			result.Warnings++
			if options.IncludeCrawlDebt {
				parsed, _ := schema.ParseKey(entry.Key)
				addFinding(result, options.MaxFindings, Finding{Kind: "crawl_debt", Key: vaultic.ID(parsed.SecondID).String(), Got: record.ErrorClass})
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, prefix := range [][]byte{[]byte("gc:b:"), []byte("gc:p:")} {
		if err := scan(ctx, store, prefix, func(entry daemon.KeyValue) error {
			record, err := schema.UnmarshalGarbageCollectionRecord(entry.Value)
			if err != nil {
				return err
			}
			if record.State == schema.GCCandidate || record.State == schema.GCPendingRevalidation {
				result.GCCandidates++
				addFinding(result, options.MaxFindings, Finding{Kind: "unreachable_blob_candidate", Key: fmt.Sprintf("%x", entry.Key)})
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return scan(ctx, store, []byte("meta:export-snapshot:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalExportCheckpointRecord(entry.Value)
		if err != nil {
			return err
		}
		switch record.State {
		case schema.ExportPending:
			result.PendingExports++
			result.Warnings++
		case schema.ExportFailed:
			result.FailedExports++
			addFinding(result, options.MaxFindings, Finding{Kind: "stale_export", Key: vaultic.ID(parsed.ID).String()})
		}
		return nil
	})
}

func checkPackCatalog(packs map[vaultic.ID]schema.PackRecord, stats packLocationStats, result *CheckResult, maxFindings uint) error {
	for id, record := range packs {
		if record.BlobCount == 0 && len(stats.types[id]) == 0 {
			continue
		}
		actualType := schema.ClassifyPack(stats.types[id])
		if record.BlobCount != stats.counts[id] || record.PayloadSize != stats.payloads[id] || record.Type != actualType {
			result.InvalidPacks++
			addFinding(result, maxFindings, Finding{Kind: "pack_metadata_mismatch", Key: id.String(), Want: fmt.Sprintf("type=%d blobs=%d payload=%d", actualType, stats.counts[id], stats.payloads[id]), Got: fmt.Sprintf("type=%d blobs=%d payload=%d", record.Type, record.BlobCount, record.PayloadSize)})
		}
	}
	return nil
}

func loadSlateDBLocations(ctx context.Context, store Store) (map[string]struct{}, packLocationStats, error) {
	result := make(map[string]struct{})
	stats := packLocationStats{types: make(map[vaultic.ID][]schema.BlobType), counts: make(map[vaultic.ID]uint64), payloads: make(map[vaultic.ID]uint64)}
	err := scan(ctx, store, []byte("b:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalBlobRecord(entry.Value)
		if err != nil {
			return err
		}
		for _, item := range record.Locations {
			packID := vaultic.ID(item.PackID)
			result[locationKey(location{BlobID: vaultic.ID(parsed.ID), PackID: vaultic.ID(item.PackID), Type: vaultic.BlobType(item.Type), Offset: uint(item.Offset), Length: uint(item.Length), UncompressedLength: uint(item.UncompressedSize)})] = struct{}{}
			stats.types[packID] = append(stats.types[packID], item.Type)
			stats.counts[packID]++
			if math.MaxUint64-stats.payloads[packID] < uint64(item.Length) {
				return fmt.Errorf("pack %s payload overflow", packID.Str())
			}
			stats.payloads[packID] += uint64(item.Length)
		}
		return nil
	})
	return result, stats, err
}

func checkAggregates(ctx context.Context, store Store, packs map[vaultic.ID]schema.PackRecord, result *CheckResult, maxFindings uint) error {
	records := make([]schema.PackRecord, 0, len(packs))
	for _, record := range packs {
		records = append(records, record)
	}
	want, err := schema.RebuildPackAggregates(records, 0)
	if err != nil {
		return err
	}
	wantTiers, err := schema.RebuildTierAggregates(records, 0)
	if err != nil {
		return err
	}
	targets := aggregateTargets(want, wantTiers)
	stored := make([]schema.PackAggregate, len(targets))
	found := make([]bool, len(targets))
	malformed := make([]error, len(targets))
	for index, target := range targets {
		value, ok, getErr := store.Get(ctx, target.key)
		if getErr != nil {
			return getErr
		}
		if ok {
			stored[index], getErr = schema.UnmarshalPackAggregate(value)
			if getErr != nil {
				if !errors.Is(getErr, schema.ErrMalformed) {
					return getErr
				}
				malformed[index] = getErr
			}
		}
		found[index] = ok
	}
	for index, target := range targets {
		if malformed[index] != nil {
			result.AggregateMismatch++
			addFinding(result, maxFindings, Finding{Kind: "aggregate_drift", Key: target.delta.Key, Got: malformed[index].Error()})
			continue
		}
		expected, got := target.expected, stored[index]
		got.UpdateSequence, expected.UpdateSequence = 0, 0
		if found[index] && got == expected {
			continue
		}
		// An absent tier record is a pending rebuild rather than drift: the
		// repository may predate the tier dimension, and the dimension is an
		// accelerator that index check can rebuild. A tier record that exists
		// but disagrees with the catalog is real drift and is reported below.
		if target.optional && !found[index] {
			if !emptyAggregate(expected) {
				result.TierAggregatesUnbuilt = true
			}
			continue
		}
		result.AggregateMismatch++
		addFinding(result, maxFindings, Finding{Kind: "aggregate_drift", Key: target.delta.Key, Want: fmt.Sprintf("%+v", expected), Got: fmt.Sprintf("%+v", got)})
	}
	return nil
}

// checkPackHistory reports unreadable history records. A corrupt or missing
// history record must never change the check's verdict: history is derived and
// advisory, and a gap in it is not repository damage.
func checkPackHistory(ctx context.Context, store Store, result *CheckResult) {
	scanned, err := ScanHistory(ctx, store, 0, 0)
	if err != nil {
		return
	}
	result.HistoryEventsMalformed = scanned.Malformed
}

// checkPackLifetime reports how much of the catalog carries trustworthy tier
// and lifetime facts. These are counts rather than findings: packs inherited
// from a legacy import are legitimately tier-unknown and retention-unknown
// forever, so they must neither fail an otherwise clean check nor crowd out
// real findings on a repository with millions of packs.
func checkPackLifetime(packs map[vaultic.ID]schema.PackRecord, result *CheckResult) {
	for _, record := range packs {
		if record.Tier == 0 || record.Tier == schema.TierUnknown {
			result.UnknownTierPacks++
		}
		if record.RetentionSource == 0 || record.RetentionSource == schema.RetentionUnknown {
			result.RetentionUnknownPacks++
		}
		if !record.UsageKnown {
			result.UsageUnaccountedPacks++
		}
	}
}

func nextAggregateSequence(ctx context.Context, store Store) (uint64, error) {
	var maximum uint64
	keys := make([][]byte, 0, typeAggregateCount+len(schema.TierAggregateKinds()))
	for kind := schema.AggregateData; kind <= schema.AggregateAll; kind++ {
		keys = append(keys, schema.PackAggregateKey(kind))
	}
	// The tier dimension is maintained incrementally too, so its sequence can
	// already exceed the type dimension's. Ignoring it would let a rebuild
	// write a sequence lower than one already published.
	for _, tier := range schema.TierAggregateKinds() {
		keys = append(keys, schema.TierAggregateKey(tier))
	}
	for _, key := range keys {
		value, found, err := store.Get(ctx, key)
		if err != nil {
			return 0, err
		}
		if found {
			record, decodeErr := schema.UnmarshalPackAggregate(value)
			if decodeErr != nil {
				if errors.Is(decodeErr, schema.ErrMalformed) {
					continue
				}
				return 0, decodeErr
			}
			maximum = max(maximum, record.UpdateSequence)
		}
	}
	if maximum == math.MaxUint64 {
		return 0, fmt.Errorf("pack aggregate update sequence overflow")
	}
	return maximum + 1, nil
}

func scan(ctx context.Context, store Store, prefix []byte, visit func(daemon.KeyValue) error) error {
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, after, scanPageSize)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := visit(entry); err != nil {
				return err
			}
			after = append(after[:0], entry.Key...)
		}
		if done {
			return nil
		}
		if len(entries) == 0 {
			return fmt.Errorf("scan %q made no progress", prefix)
		}
	}
}

func loadExportProvenance(ctx context.Context, store Store) (map[vaultic.ID]uint64, error) {
	byPack := make(map[vaultic.ID]uint64)
	err := scan(ctx, store, []byte("meta:export-index:"), func(entry daemon.KeyValue) error {
		record, err := schema.UnmarshalExportIndexCheckpointRecord(entry.Value)
		if err != nil {
			return err
		}
		for _, packID := range record.PackIDs {
			id := vaultic.ID(packID)
			byPack[id] = max(byPack[id], record.Sequence)
		}
		return nil
	})
	return byPack, err
}

func checkExportProvenance(ctx context.Context, source LegacySource, store Store, packs map[vaultic.ID]schema.PackRecord, result *CheckResult, maxFindings uint) error {
	return scan(ctx, store, []byte("meta:export-index:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalExportIndexCheckpointRecord(entry.Value)
		if err != nil {
			return err
		}
		result.ExportCheckpoints++
		indexID := vaultic.ID(parsed.ID)
		encoded, err := source.LoadUnpacked(ctx, vaultic.IndexFile, indexID)
		if err != nil {
			result.FailedExports++
			addFinding(result, maxFindings, Finding{Kind: "stale_export", Key: indexID.String(), Got: err.Error()})
			return nil
		}
		index, err := legacyindex.DecodeIndex(encoded, indexID)
		if err != nil {
			result.FailedExports++
			addFinding(result, maxFindings, Finding{Kind: "hash_mismatch", Key: indexID.String(), Got: err.Error()})
			return nil
		}
		actualPackIDs := make([]schema.ID, 0, len(record.PackIDs))
		for packID := range index.Packs() {
			actualPackIDs = append(actualPackIDs, schema.ID(packID))
		}
		sort.Slice(actualPackIDs, func(left, right int) bool { return bytes.Compare(actualPackIDs[left][:], actualPackIDs[right][:]) < 0 })
		if !slices.Equal(actualPackIDs, record.PackIDs) {
			result.FailedExports++
			addFinding(result, maxFindings, Finding{Kind: "stale_export", Key: indexID.String(), Want: fmt.Sprintf("packs=%x", record.PackIDs), Got: fmt.Sprintf("packs=%x", actualPackIDs)})
			return nil
		}
		for _, packID := range record.PackIDs {
			id := vaultic.ID(packID)
			if _, found := packs[id]; !found {
				result.FailedExports++
				addFinding(result, maxFindings, Finding{Kind: "stale_export", Key: indexID.String(), Got: "missing pack " + id.String()})
			}
		}
		return nil
	})
}

func sortedPackIDs(packs map[vaultic.ID]schema.PackRecord) []vaultic.ID {
	ids := make([]vaultic.ID, 0, len(packs))
	for id := range packs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return bytes.Compare(ids[left][:], ids[right][:]) < 0 })
	return ids
}

func locationKey(item location) string {
	return fmt.Sprintf("%s:%s:%d:%d:%d:%d", item.BlobID.String(), item.PackID.String(), item.Type, item.Offset, item.Length, item.UncompressedLength)
}

func addFinding(result *CheckResult, maximum uint, finding Finding) {
	if maximum == 0 || uint(len(result.Findings)) < maximum {
		result.Findings = append(result.Findings, finding)
	}
}
