package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// GCStore is the daemon capability required to discover, revalidate, and
// sweep garbage in a SlateDB-authoritative repository.
type GCStore interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error)
	WriteMutableBatch(context.Context, []daemon.Mutation, [][]byte, bool) error
	UpdatePackUsage(context.Context, map[schema.ID]daemon.PackUsage) (uint64, error)
	MarkPackDeletePending(context.Context, schema.ID) error
	MarkPackDeleted(context.Context, schema.ID, []schema.ID) error
}

const gcScanPageSize = 10_000

// GCOptions controls a single discover/revalidate/sweep pass.
type GCOptions struct {
	// DryRun reports the plan without mutating anything.
	DryRun bool
	// DiscoverOnly records candidate blobs from reverse references and the
	// pack catalog without the more expensive snapshot walk, and never
	// deletes or repacks anything. It is intended for cheap, frequent
	// scheduling ahead of an occasional full sweep.
	DiscoverOnly bool
	// MinCandidateAge delays sweeping a newly confirmed candidate until it
	// has been continuously observed as unreachable for at least this long,
	// guarding against races with in-flight or clock-skewed writers.
	MinCandidateAge time.Duration
	// IgnoreSnapshots excludes these snapshot IDs from the retained-root walk,
	// as if they had already been forgotten.
	IgnoreSnapshots vaultic.IDSet
}

// GCStats summarizes one discover/revalidate/sweep pass.
type GCStats struct {
	MessageType         string `json:"message_type"`
	BlobsScanned        uint64 `json:"blobs_scanned"`
	PacksScanned        uint64 `json:"packs_scanned"`
	BlobCandidates      uint64 `json:"blob_candidates"`
	WholePackCandidates uint64 `json:"whole_pack_candidates"`
	MixedPackCandidates uint64 `json:"mixed_pack_candidates"`
	PendingAge          uint64 `json:"pending_age"`
	// PendingRetries is how many packs are currently stuck delete-pending
	// from a prior interrupted sweep, queued for retry (whether or not this
	// run actually attempts one, e.g. DryRun).
	PendingRetries uint64 `json:"pending_retries"`
	// PacksDeleted is the total number of packs physically removed this run:
	// wholly unreachable packs, superseded originals freed by a repack (also
	// counted in PacksRepacked), and successful retries (also counted in
	// PacksRetried). It is a total, not additional to those other fields.
	PacksDeleted     uint64 `json:"packs_deleted"`
	PacksRepacked    uint64 `json:"packs_repacked"`
	PacksRetried     uint64 `json:"packs_retried"`
	PacksRetryFailed uint64 `json:"packs_retry_failed"`
	BlobsFreed       uint64 `json:"blobs_freed"`
	BytesFreed       uint64 `json:"bytes_freed"`
	// PacksAccounted is how many pack records had their used/unused payload
	// split refreshed from this run's reachability result.
	PacksAccounted uint64 `json:"packs_accounted"`
	// PacksUnaccountable is how many packs were left with unknown usage
	// because their blob index membership did not sum to the catalog payload
	// size. No split is invented for them.
	PacksUnaccountable uint64 `json:"packs_unaccountable"`
}

// GCPlan is the result of discovery and revalidation, ready for Execute.
type GCPlan struct {
	repo  *Repository
	store GCStore
	opts  GCOptions

	wholePacks       map[vaultic.ID][]vaultic.ID // pack -> every member blob, ready to sweep now
	mixedPacks       map[vaultic.ID]*gcBlobSet   // pack -> live blobs to keep, ready to repack now
	mixedPackMembers map[vaultic.ID][]vaultic.ID // pack -> every member blob (for post-repack deletion)
	retryPacks       map[vaultic.ID][]vaultic.ID // already delete-pending, retry physical deletion
	packBytes        map[vaultic.ID]uint64

	Stats GCStats
}

// PlanGC discovers GC candidates from reverse references and the pack
// catalog, then (unless DiscoverOnly) re-walks every retained snapshot root
// to confirm reachability before scheduling any deletion. Reachability is
// always re-verified this way immediately before Execute performs any
// destructive action; the reverse-reference scan only narrows the search.
func PlanGC(ctx context.Context, opts GCOptions, repo *Repository, printer vaultic.Printer) (*GCPlan, error) {
	if repo.Engine().Mode() != metadataindex.ModeSlateDB {
		return nil, fmt.Errorf("gc requires a SlateDB-authoritative repository; use prune for legacy repositories")
	}
	engine, ok := repo.Engine().(*metadataindex.DaemonEngine)
	if !ok {
		return nil, fmt.Errorf("gc requires the SlateDB daemon engine")
	}
	if repo.Connections() < 2 {
		return nil, fmt.Errorf("gc requires a backend connection limit of at least two")
	}
	store := engine.SchemaStore()

	plan := &GCPlan{
		repo: repo, store: store, opts: opts,
		wholePacks: make(map[vaultic.ID][]vaultic.ID), mixedPacks: make(map[vaultic.ID]*gcBlobSet),
		mixedPackMembers: make(map[vaultic.ID][]vaultic.ID), retryPacks: make(map[vaultic.ID][]vaultic.ID),
		packBytes: make(map[vaultic.ID]uint64), Stats: GCStats{MessageType: "gc_summary"},
	}

	printer.P("scanning reverse references...\n")
	referenced, err := scanReferencedDataBlobs(ctx, store)
	if err != nil {
		return nil, err
	}

	printer.P("scanning pack catalog...\n")
	packs, err := scanPacks(ctx, store)
	if err != nil {
		return nil, err
	}

	printer.P("scanning blob catalog...\n")
	blobTypes, packMembers, packMemberBytes, err := scanBlobCatalog(ctx, store)
	if err != nil {
		return nil, err
	}
	plan.Stats.BlobsScanned = uint64(len(blobTypes))

	for packID, record := range packs {
		if record.Lifecycle == schema.PackDeletePending {
			plan.retryPacks[packID] = packMembers[packID]
		}
	}
	plan.Stats.PendingRetries = uint64(len(plan.retryPacks))

	if opts.DiscoverOnly {
		if err := plan.discoverOnly(ctx, referenced, blobTypes); err != nil {
			return nil, err
		}
		return plan, nil
	}

	printer.P("re-walking retained snapshot roots...\n")
	keepSet := newGCBlobSet()
	if err := walkRetainedSnapshots(ctx, repo, opts.IgnoreSnapshots, keepSet, printer); err != nil {
		return nil, err
	}

	unreachable := make(map[vaultic.ID]struct{}, len(blobTypes))
	for id, blobType := range blobTypes {
		handle := vaultic.BlobHandle{ID: id, Type: legacyBlobType(blobType)}
		if keepSet.Has(handle) {
			continue
		}
		if blobType == schema.BlobData {
			if _, hasEdge := referenced[id]; hasEdge {
				continue
			}
		}
		unreachable[id] = struct{}{}
	}
	plan.Stats.BlobCandidates = uint64(len(unreachable))

	existing, err := loadExistingGCTimestamps(ctx, store)
	if err != nil {
		return nil, err
	}

	classification, err := classifyPacks(packs, packMembers, blobTypes, unreachable, existing, time.Now(), opts.MinCandidateAge)
	if err != nil {
		return nil, err
	}
	plan.wholePacks = classification.wholePacks
	plan.mixedPacks = classification.mixedPacks
	plan.mixedPackMembers = classification.mixedPackMembers
	plan.packBytes = classification.packBytes
	plan.Stats.PacksScanned = classification.packsScanned
	plan.Stats.WholePackCandidates = classification.wholePackCandidates
	plan.Stats.MixedPackCandidates = classification.mixedPackCandidates
	plan.Stats.PendingAge = classification.pendingAge

	if len(classification.gcPuts) != 0 && !opts.DryRun {
		if err := store.WriteMutableBatch(ctx, classification.gcPuts, nil, true); err != nil {
			return nil, fmt.Errorf("record GC bookkeeping: %w", err)
		}
	}

	// Reachability was just recomputed, so this is the point where usage
	// accounting can be refreshed without a second full sweep.
	usage, inconsistent := computePackUsage(packs, packMemberBytes, unreachable)
	plan.Stats.PacksUnaccountable = inconsistent
	if len(usage) != 0 && !opts.DryRun {
		applied, err := store.UpdatePackUsage(ctx, usage)
		if err != nil {
			return nil, fmt.Errorf("record pack usage accounting: %w", err)
		}
		plan.Stats.PacksAccounted = applied
	}
	return plan, nil
}

// gcClassification is the pure, side-effect-free result of deciding, for each
// published pack, whether every member blob is confirmed unreachable (whole),
// only some are (mixed, needs repack), or none are (skip).
type gcClassification struct {
	wholePacks       map[vaultic.ID][]vaultic.ID
	mixedPacks       map[vaultic.ID]*gcBlobSet
	mixedPackMembers map[vaultic.ID][]vaultic.ID
	packBytes        map[vaultic.ID]uint64
	gcPuts           []daemon.Mutation

	packsScanned, wholePackCandidates, mixedPackCandidates, pendingAge uint64
}

func classifyPacks(
	packs map[vaultic.ID]schema.PackRecord,
	packMembers map[vaultic.ID][]vaultic.ID,
	blobTypes map[vaultic.ID]schema.BlobType,
	unreachable map[vaultic.ID]struct{},
	existing map[string]int64,
	now time.Time,
	minAge time.Duration,
) (gcClassification, error) {
	result := gcClassification{
		wholePacks: make(map[vaultic.ID][]vaultic.ID), mixedPacks: make(map[vaultic.ID]*gcBlobSet),
		mixedPackMembers: make(map[vaultic.ID][]vaultic.ID), packBytes: make(map[vaultic.ID]uint64),
	}
	for packID, record := range packs {
		if record.Lifecycle != schema.PackPublished {
			continue
		}
		members := packMembers[packID]
		if len(members) == 0 {
			continue
		}
		result.packsScanned++
		keep := newGCBlobSet()
		allUnreachable := true
		for _, blobID := range members {
			if _, gone := unreachable[blobID]; gone {
				continue
			}
			allUnreachable = false
			keep.Insert(vaultic.BlobHandle{ID: blobID, Type: legacyBlobType(blobTypes[blobID])})
		}
		if !allUnreachable && keep.Len() == len(members) {
			continue // pack is fully live; not a GC candidate at all
		}

		key := schema.GarbageCollectionKey(schema.GCPack, schema.ID(packID))
		discovered := now
		if previous, found := existing[string(key)]; found {
			discovered = time.Unix(0, previous)
		}
		// A prior --discover-only pass only ever records blob-level
		// candidates; honor the earliest of those timestamps too, so
		// continuous unreachability is measured from when a blob was first
		// observed, not only from when a pack was first classified.
		for _, blobID := range members {
			if _, gone := unreachable[blobID]; !gone {
				continue
			}
			blobKey := schema.GarbageCollectionKey(schema.GCBlob, schema.ID(blobID))
			if previous, found := existing[string(blobKey)]; found {
				if candidate := time.Unix(0, previous); candidate.Before(discovered) {
					discovered = candidate
				}
			}
		}
		value, err := (schema.GarbageCollectionRecord{State: schema.GCRevalidated, DiscoveredUnixNano: discovered.UnixNano()}).MarshalBinary()
		if err != nil {
			return gcClassification{}, err
		}
		result.gcPuts = append(result.gcPuts, daemon.Mutation{Key: key, Value: value})

		ready := now.Sub(discovered) >= minAge
		if allUnreachable {
			result.wholePackCandidates++
			result.packBytes[packID] = record.PhysicalSize
			if ready {
				result.wholePacks[packID] = members
			} else {
				result.pendingAge++
			}
		} else {
			result.mixedPackCandidates++
			result.packBytes[packID] = record.PhysicalSize
			if ready {
				result.mixedPacks[packID] = keep
				result.mixedPackMembers[packID] = members
			} else {
				result.pendingAge++
			}
		}
	}
	return result, nil
}

// Execute performs the destructive part of a GC pass: retrying previously
// interrupted deletions, repacking mixed packs, and deleting packs confirmed
// wholly unreachable. It is a no-op for DiscoverOnly or DryRun plans.
func (plan *GCPlan) Execute(ctx context.Context, printer vaultic.Printer) error {
	if plan.opts.DiscoverOnly {
		return nil
	}
	if plan.opts.DryRun {
		return nil
	}
	if err := plan.retryPendingDeletions(ctx, printer); err != nil {
		return err
	}
	if err := plan.repackMixedPacks(ctx, printer); err != nil {
		return err
	}
	return plan.deleteWholePacks(ctx, printer)
}

func (plan *GCPlan) retryPendingDeletions(ctx context.Context, printer vaultic.Printer) error {
	for packID, members := range plan.retryPacks {
		if err := plan.deletePackObjectAndCatalog(ctx, packID, members); err != nil {
			plan.Stats.PacksRetryFailed++
			printer.E("retry deleting pack %s: %v\n", packID.Str(), err)
			continue
		}
		plan.Stats.PacksRetried++
	}
	return nil
}

func (plan *GCPlan) repackMixedPacks(ctx context.Context, printer vaultic.Printer) error {
	if len(plan.mixedPacks) == 0 {
		return nil
	}
	packSet := vaultic.NewIDSet()
	keep := newGCBlobSet()
	for packID, handles := range plan.mixedPacks {
		packSet.Insert(packID)
		for handle := range handles.handles {
			keep.Insert(handle)
		}
	}
	bar := printer.NewCounter("packs repacked")
	err := plan.repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		return CopyBlobs(ctx, plan.repo, plan.repo, uploader, packSet, keep, bar, printer.P)
	})
	if err != nil {
		return fmt.Errorf("repack mixed packs: %w", err)
	}
	if keep.Len() != 0 {
		return fmt.Errorf("internal error: %d live blobs were not repacked", keep.Len())
	}
	for packID := range plan.mixedPacks {
		if err := plan.store.MarkPackDeletePending(ctx, schema.ID(packID)); err != nil {
			printer.E("mark repacked pack %s delete-pending: %v\n", packID.Str(), err)
			continue
		}
		if err := plan.deletePackObjectAndCatalog(ctx, packID, plan.mixedPackMembers[packID]); err != nil {
			printer.E("deleting repacked pack %s: %v\n", packID.Str(), err)
			continue
		}
		plan.Stats.PacksRepacked++
	}
	return nil
}

func (plan *GCPlan) deleteWholePacks(ctx context.Context, printer vaultic.Printer) error {
	for packID, members := range plan.wholePacks {
		if err := plan.store.MarkPackDeletePending(ctx, schema.ID(packID)); err != nil {
			printer.E("mark pack %s delete-pending: %v\n", packID.Str(), err)
			continue
		}
		if err := plan.deletePackObjectAndCatalog(ctx, packID, members); err != nil {
			printer.E("deleting pack %s: %v\n", packID.Str(), err)
			continue
		}
	}
	return nil
}

// deletePackObjectAndCatalog physically removes the pack object and, only on
// success, purges its catalog record. A failed removal leaves the pack
// visible as delete-pending for a later retry pass. An object that is
// already gone (a prior run removed it but crashed before the catalog was
// purged) is treated as success, matching object-store DELETE semantics
// where deleting a missing key is a no-op rather than an error; otherwise a
// retry could get permanently stuck never reaching MarkPackDeleted again.
func (plan *GCPlan) deletePackObjectAndCatalog(ctx context.Context, packID vaultic.ID, members []vaultic.ID) error {
	if err := (&internalRepository{plan.repo}).RemoveUnpacked(ctx, vaultic.PackFile, packID); err != nil && !plan.repo.Backend().IsNotExist(err) {
		return err
	}
	if err := plan.store.MarkPackDeleted(ctx, schema.ID(packID), toSchemaIDs(members)); err != nil {
		return err
	}
	plan.Stats.PacksDeleted++
	plan.Stats.BlobsFreed += uint64(len(members))
	plan.Stats.BytesFreed += plan.packBytes[packID]
	return nil
}

func (plan *GCPlan) discoverOnly(ctx context.Context, referenced map[vaultic.ID]struct{}, blobTypes map[vaultic.ID]schema.BlobType) error {
	existing, err := loadExistingGCTimestamps(ctx, plan.store)
	if err != nil {
		return err
	}
	now := time.Now()
	var puts []daemon.Mutation
	for id, blobType := range blobTypes {
		if blobType != schema.BlobData {
			continue // tree blobs are only ever resolved by the snapshot walk
		}
		if _, hasEdge := referenced[id]; hasEdge {
			continue
		}
		key := schema.GarbageCollectionKey(schema.GCBlob, schema.ID(id))
		discovered := now
		if previous, found := existing[string(key)]; found {
			discovered = time.Unix(0, previous)
		}
		value, err := (schema.GarbageCollectionRecord{State: schema.GCCandidate, DiscoveredUnixNano: discovered.UnixNano()}).MarshalBinary()
		if err != nil {
			return err
		}
		puts = append(puts, daemon.Mutation{Key: key, Value: value})
		plan.Stats.BlobCandidates++
	}
	if plan.opts.DryRun || len(puts) == 0 {
		return nil
	}
	return plan.store.WriteMutableBatch(ctx, puts, nil, true)
}

// scanReferencedDataBlobs scans ri:/rm: directly rather than the rc:
// materialized counter: rc: is only ever incremented in the same
// transaction that writes an ri:/rm: edge, so it can never be nonzero
// without a corresponding edge already making the blob referenced here. A
// drifted/stale rc: counter (rc:>0 with no backing edge) is exactly what
// index check's reference_count_drift finding exists to surface; GC must
// not treat that drift as a reason to keep a blob it would otherwise judge
// unreachable from both the reverse-edge scan and the snapshot walk.
func scanReferencedDataBlobs(ctx context.Context, store GCStore) (map[vaultic.ID]struct{}, error) {
	referenced := make(map[vaultic.ID]struct{})
	for _, prefix := range [][]byte{[]byte("ri:"), []byte("rm:")} {
		if err := gcScan(ctx, store, prefix, func(entry daemon.KeyValue) error {
			parsed, err := schema.ParseKey(entry.Key)
			if err != nil {
				return err
			}
			referenced[vaultic.ID(parsed.ID)] = struct{}{}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return referenced, nil
}

func scanPacks(ctx context.Context, store GCStore) (map[vaultic.ID]schema.PackRecord, error) {
	packs := make(map[vaultic.ID]schema.PackRecord)
	err := gcScan(ctx, store, []byte("p:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPack {
			return fmt.Errorf("invalid pack key %q", entry.Key)
		}
		record, err := schema.UnmarshalPackRecord(entry.Value)
		if err != nil {
			return fmt.Errorf("decode pack %x: %w", parsed.ID, err)
		}
		packs[vaultic.ID(parsed.ID)] = record
		return nil
	})
	return packs, err
}

// PruneStaleLegacyIndexes deletes legacy JSON index files, and any export
// provenance checkpoint tracking them, that reference at least one pack no
// longer present in the SlateDB catalog. Physically deleting a pack
// necessarily makes any legacy index that referenced it stale; leaving the
// stale object in place would actively mislead legacy Restic/Rustic clients
// rather than merely lagging. Every legacy index file present on the backend
// is decoded and checked directly, not only ones vaultic itself exported,
// because indexes inherited from a pre-import legacy repository are never
// covered by an export checkpoint. Call this only after a full export has
// ensured every currently live pack has fresh coverage elsewhere, so a pack
// whose only prior legacy coverage came from a superseded index never
// becomes invisible to legacy clients.
func PruneStaleLegacyIndexes(ctx context.Context, repo *Repository) (uint64, error) {
	engine, ok := repo.Engine().(*metadataindex.DaemonEngine)
	if !ok {
		return 0, fmt.Errorf("pruning stale legacy indexes requires the SlateDB daemon engine")
	}
	store := engine.SchemaStore()
	packs, err := scanPacks(ctx, store)
	if err != nil {
		return 0, err
	}
	checkpoints, err := loadExportIndexCheckpointKeys(ctx, store)
	if err != nil {
		return 0, err
	}

	var indexIDs []vaultic.ID
	if err := repo.List(ctx, vaultic.IndexFile, func(id vaultic.ID, _ int64) error {
		indexIDs = append(indexIDs, id)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("list legacy indexes: %w", err)
	}

	var removed uint64
	for _, indexID := range indexIDs {
		encoded, err := repo.LoadUnpacked(ctx, vaultic.IndexFile, indexID)
		if err != nil {
			return removed, fmt.Errorf("load legacy index %s: %w", indexID.Str(), err)
		}
		decoded, err := legacyindex.DecodeIndex(encoded, indexID)
		if err != nil {
			return removed, fmt.Errorf("decode legacy index %s: %w", indexID.Str(), err)
		}
		stale := false
		for packID := range decoded.Packs() {
			if _, found := packs[packID]; !found {
				stale = true
				break
			}
		}
		if !stale {
			continue
		}
		// Delete the checkpoint before the physical object: if the physical
		// removal below fails or is interrupted, the file remains listed and
		// this loop retries it on the next run. The reverse order could
		// remove the file yet leave an un-retriable orphaned checkpoint,
		// since a removed file never reappears in the listing scanned above.
		if checkpointKey, tracked := checkpoints[indexID]; tracked {
			if err := store.WriteMutableBatch(ctx, nil, [][]byte{checkpointKey}, true); err != nil {
				return removed, fmt.Errorf("remove stale legacy index checkpoint %s: %w", indexID.Str(), err)
			}
		}
		if err := (&internalRepository{repo}).RemoveUnpacked(ctx, vaultic.IndexFile, indexID); err != nil {
			return removed, fmt.Errorf("remove stale legacy index %s: %w", indexID.Str(), err)
		}
		removed++
	}
	return removed, nil
}

func loadExportIndexCheckpointKeys(ctx context.Context, store GCStore) (map[vaultic.ID][]byte, error) {
	checkpoints := make(map[vaultic.ID][]byte)
	err := gcScan(ctx, store, []byte("meta:export-index:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		checkpoints[vaultic.ID(parsed.ID)] = append([]byte(nil), entry.Key...)
		return nil
	})
	return checkpoints, err
}

// scanBlobCatalog reads the blob index once and returns blob types, pack
// membership, and the payload bytes each blob contributes to each pack. The
// byte map is what makes pack usage rebuildable from the blob index alone.
func scanBlobCatalog(ctx context.Context, store GCStore) (map[vaultic.ID]schema.BlobType, map[vaultic.ID][]vaultic.ID, map[vaultic.ID]map[vaultic.ID]uint64, error) {
	blobTypes := make(map[vaultic.ID]schema.BlobType)
	packMembers := make(map[vaultic.ID][]vaultic.ID)
	packMemberBytes := make(map[vaultic.ID]map[vaultic.ID]uint64)
	seen := make(map[[2]vaultic.ID]struct{})
	err := gcScan(ctx, store, []byte("b:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyBlob {
			return fmt.Errorf("invalid blob key %q", entry.Key)
		}
		record, err := schema.UnmarshalBlobRecord(entry.Value)
		if err != nil {
			return fmt.Errorf("decode blob %x: %w", parsed.ID, err)
		}
		blobID := vaultic.ID(parsed.ID)
		for _, location := range record.Locations {
			blobTypes[blobID] = location.Type
			packID := vaultic.ID(location.PackID)
			membership := [2]vaultic.ID{packID, blobID}
			if _, dup := seen[membership]; dup {
				continue
			}
			seen[membership] = struct{}{}
			packMembers[packID] = append(packMembers[packID], blobID)
			if packMemberBytes[packID] == nil {
				packMemberBytes[packID] = make(map[vaultic.ID]uint64)
			}
			packMemberBytes[packID][blobID] = uint64(location.Length)
		}
		return nil
	})
	return blobTypes, packMembers, packMemberBytes, err
}

// computePackUsage splits each pack's payload into reachable and unreachable
// bytes from the blob index and the confirmed-unreachable set. It returns only
// the records whose accounting changed, plus the number of packs whose member
// bytes did not sum to the catalog payload size and were therefore left
// unaccounted rather than recorded with a fabricated split.
func computePackUsage(
	packs map[vaultic.ID]schema.PackRecord,
	packMemberBytes map[vaultic.ID]map[vaultic.ID]uint64,
	unreachable map[vaultic.ID]struct{},
) (map[schema.ID]daemon.PackUsage, uint64) {
	updates := make(map[schema.ID]daemon.PackUsage)
	var inconsistent uint64
	for packID, record := range packs {
		if record.Lifecycle == schema.PackDeleted {
			continue
		}
		members, ok := packMemberBytes[packID]
		if !ok {
			continue
		}
		var used, unused, total uint64
		for blobID, length := range members {
			total += length
			if _, gone := unreachable[blobID]; gone {
				unused += length
				continue
			}
			used += length
		}
		// The catalog payload size is authoritative. If the blob index does
		// not agree with it, the split cannot be trusted, so usage stays
		// unknown instead of being recorded as measured.
		if total != record.PayloadSize {
			inconsistent++
			continue
		}
		if record.UsageKnown && record.UsedPayloadBytes == used && record.UnusedPayloadBytes == unused {
			continue
		}
		updates[schema.ID(packID)] = daemon.PackUsage{Used: used, Unused: unused}
	}
	return updates, inconsistent
}

func loadExistingGCTimestamps(ctx context.Context, store GCStore) (map[string]int64, error) {
	timestamps := make(map[string]int64)
	for _, prefix := range [][]byte{[]byte("gc:b:"), []byte("gc:p:")} {
		if err := gcScan(ctx, store, prefix, func(entry daemon.KeyValue) error {
			record, err := schema.UnmarshalGarbageCollectionRecord(entry.Value)
			if err != nil {
				return err
			}
			timestamps[string(entry.Key)] = record.DiscoveredUnixNano
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return timestamps, nil
}

func walkRetainedSnapshots(ctx context.Context, repo *Repository, ignore vaultic.IDSet, keep *gcBlobSet, printer vaultic.Printer) error {
	var trees vaultic.IDs
	err := data.ForAllSnapshots(ctx, repo, repo, ignore, func(_ vaultic.ID, snapshot *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		trees = append(trees, *snapshot.Tree)
		return nil
	})
	if err != nil {
		return fmt.Errorf("load retained snapshots: %w", err)
	}
	bar := printer.NewCounter("snapshots")
	bar.SetMax(uint64(len(trees)))
	defer bar.Done()
	return data.FindUsedBlobs(ctx, repo, trees, keep, bar)
}

func gcScan(ctx context.Context, store GCStore, prefix []byte, visit func(daemon.KeyValue) error) error {
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, after, gcScanPageSize)
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

func legacyBlobType(blobType schema.BlobType) vaultic.BlobType {
	if blobType == schema.BlobTree {
		return vaultic.TreeBlob
	}
	return vaultic.DataBlob
}

func toSchemaIDs(ids []vaultic.ID) []schema.ID {
	result := make([]schema.ID, len(ids))
	for index, id := range ids {
		result[index] = schema.ID(id)
	}
	return result
}

// gcBlobSet is a simple mutable blob-handle set satisfying both
// vaultic.FindBlobSet (used while walking snapshots) and the unexported
// repackBlobSet interface (used by CopyBlobs while repacking).
type gcBlobSet struct {
	handles map[vaultic.BlobHandle]struct{}
}

func newGCBlobSet() *gcBlobSet                      { return &gcBlobSet{handles: make(map[vaultic.BlobHandle]struct{})} }
func (s *gcBlobSet) Has(bh vaultic.BlobHandle) bool { _, found := s.handles[bh]; return found }
func (s *gcBlobSet) Insert(bh vaultic.BlobHandle)   { s.handles[bh] = struct{}{} }
func (s *gcBlobSet) Delete(bh vaultic.BlobHandle)   { delete(s.handles, bh) }
func (s *gcBlobSet) Len() int                       { return len(s.handles) }
