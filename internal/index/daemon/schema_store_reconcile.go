package daemon

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UpdatePackUsage records refreshed usage accounting for packs whose
// reachability was just recomputed.
//
// It runs as a transaction per batch so the pack record and both aggregate
// dimensions move together, and so a concurrent publish is resolved by
// conflict retry rather than by clobbering its aggregate increment. A pack
// whose payload size no longer matches the caller's split is skipped: the
// caller's reachability view is stale for that pack and usage stays unknown
// rather than being recorded wrongly.
func (store *SchemaStore) UpdatePackUsage(ctx context.Context, usage map[schema.ID]PackUsage) (uint64, error) {
	return store.UpdatePackUsageForRun(ctx, usage, schema.ID{})
}

// UpdatePackUsageForRun records refreshed usage accounting and attributes the
// resulting coalesced history events to one run.
func (store *SchemaStore) UpdatePackUsageForRun(
	ctx context.Context,
	usage map[schema.ID]PackUsage,
	runID schema.ID,
) (uint64, error) {
	ids := make([]schema.ID, 0, len(usage))
	for id := range usage {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return bytes.Compare(ids[left][:], ids[right][:]) < 0 })
	var applied uint64
	for start := 0; start < len(ids); start += packUsageBatchSize {
		batch := ids[start:min(start+packUsageBatchSize, len(ids))]
		count, err := store.updatePackUsageBatch(ctx, batch, usage, runID)
		if err != nil {
			return applied, err
		}
		applied += count
	}
	return applied, nil
}

const packUsageBatchSize = 256

func (store *SchemaStore) updatePackUsageBatch(
	ctx context.Context,
	ids []schema.ID,
	usage map[schema.ID]PackUsage,
	runID schema.ID,
) (uint64, error) {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		count, err := store.updatePackUsageOnce(ctx, ids, usage, runID)
		if status.Code(err) != codes.Aborted {
			return count, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return 0, fmt.Errorf("update pack usage: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) updatePackUsageOnce(
	ctx context.Context,
	ids []schema.ID,
	usage map[schema.ID]PackUsage,
	runID schema.ID,
) (uint64, error) {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (uint64, error) {
		rollbackTransaction(ctx, transaction)
		return 0, err
	}
	puts := make([]Mutation, 0, len(ids)+len(aggregateKeys()))
	changes := make([]packChange, 0, len(ids))
	for _, id := range ids {
		key := schema.PackKey(id)
		value, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			return fail(getErr)
		}
		if !found {
			continue
		}
		current, decodeErr := schema.UnmarshalPackRecord(value)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		split := usage[id]
		if split.Used > current.PayloadSize || split.Used+split.Unused != current.PayloadSize {
			continue
		}
		if current.UsageKnown && current.UsedPayloadBytes == split.Used && current.UnusedPayloadBytes == split.Unused {
			continue
		}
		updated := current
		updated.UsageKnown, updated.UsedPayloadBytes, updated.UnusedPayloadBytes = true, split.Used, split.Unused
		encoded, encodeErr := updated.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: key, Value: encoded})
		changes = append(changes, packChange{packID: id, old: current, current: updated})
	}
	if len(changes) == 0 {
		rollbackTransaction(ctx, transaction)
		return 0, nil
	}
	aggregates, err := applyPackAggregateDeltas(ctx, transaction, changes)
	if err != nil {
		return fail(err)
	}
	puts = append(puts, aggregates...)
	// One coalesced usage event per pack per run, never one per blob.
	events := make([]PackEvent, 0, len(changes))
	for _, change := range changes {
		usedDelta, unusedDelta := usageDeltas(&change.old, change.current)
		events = append(events, PackEvent{
			PackID: change.packID,
			Record: schema.PackHistoryEvent{
				Type: schema.EventUsageChanged, PackType: change.current.Type,
				PhysicalSize: change.current.PhysicalSize, PayloadSize: change.current.PayloadSize,
				UsedDelta: usedDelta, UnusedDelta: unusedDelta, RunID: runID,
			},
		})
	}
	history, err := packHistoryMutations(ctx, transaction, events)
	if err != nil {
		return fail(err)
	}
	puts = append(puts, history...)
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fail(err)
	}
	return uint64(len(changes)), nil
}

type packChange struct {
	packID       schema.ID
	old, current schema.PackRecord
}

// applyPackAggregateDeltas folds several pack changes into one read-modify-write
// of each aggregate. Calling updatePackAggregates per pack inside a single
// transaction would be wrong: the reads would not observe the pending puts from
// earlier packs, so every delta but the last would be lost.
//
//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func applyPackAggregateDeltas(ctx context.Context, transaction *Transaction, changes []packChange) ([]Mutation, error) {
	keys := aggregateKeys()
	values, found, err := transaction.MultiGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	puts := make([]Mutation, 0, len(keys))
	for offset, key := range keys {
		aggregate := schema.PackAggregate{}
		if found[offset] {
			aggregate, err = schema.UnmarshalPackAggregate(values[offset].Value)
			if err != nil {
				return nil, err
			}
		}
		touched := false
		for _, change := range changes {
			if aggregateAppliesTo(offset, change.old) {
				if found[offset] || !isTierAggregateOffset(offset) {
					if err := subtractPackAggregate(&aggregate, change.old); err != nil {
						return nil, err
					}
					touched = true
				}
			}
			if aggregateAppliesTo(offset, change.current) {
				if err := addPackAggregate(&aggregate, change.current); err != nil {
					return nil, err
				}
				touched = true
			}
		}
		// An aggregate that gained and lost nothing is unchanged, so it is
		// left untouched rather than rewritten with a bumped sequence.
		if !touched {
			continue
		}
		aggregate.UpdateSequence++
		encoded, err := aggregate.MarshalBinary()
		if err != nil {
			return nil, err
		}
		puts = append(puts, Mutation{Key: key, Value: encoded})
	}
	return puts, nil
}

func updatePackAggregates(
	ctx context.Context,
	transaction *Transaction,
	old *schema.PackRecord,
	current schema.PackRecord,
) ([]Mutation, error) {
	keys := aggregateKeys()
	values, found, err := transaction.MultiGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	puts := make([]Mutation, 0, len(keys))
	for offset, key := range keys {
		aggregate := schema.PackAggregate{}
		if found[offset] {
			aggregate, err = schema.UnmarshalPackAggregate(values[offset].Value)
			if err != nil {
				return nil, err
			}
		}
		touched := false
		if old != nil && aggregateAppliesTo(offset, *old) {
			// A pack whose tier was unknown and is now known moves between tier
			// records. On a repository that predates the tier dimension the record
			// it is leaving does not exist, and there is nothing to subtract.
			// Failing here would block the publish outright; the pending rebuild
			// reconciles the dimension instead.
			if found[offset] || !isTierAggregateOffset(offset) {
				if err := subtractPackAggregate(&aggregate, *old); err != nil {
					return nil, err
				}
				touched = true
			}
		}
		if aggregateAppliesTo(offset, current) {
			if err := addPackAggregate(&aggregate, current); err != nil {
				return nil, err
			}
			touched = true
		}
		// Type aggregates always exist once anything has been published. Only
		// the tier dimension is sparse: a tier that neither gained nor lost a
		// pack is left alone so publishing one pack does not materialize a
		// record for every tier the repository does not use.
		if !touched && isTierAggregateOffset(offset) && !found[offset] {
			continue
		}
		aggregate.UpdateSequence++
		encoded, err := aggregate.MarshalBinary()
		if err != nil {
			return nil, err
		}
		puts = append(puts, Mutation{Key: key, Value: encoded})
	}
	return puts, nil
}

// aggregateKeys returns the type dimension followed by the tier dimension, in
// the fixed order aggregateAppliesTo decodes offsets against.
func aggregateKeys() [][]byte {
	keys := make([][]byte, 0, 5+len(schema.TierAggregateKinds()))
	for kind := schema.AggregateData; kind <= schema.AggregateAll; kind++ {
		keys = append(keys, schema.PackAggregateKey(kind))
	}
	for _, tier := range schema.TierAggregateKinds() {
		keys = append(keys, schema.TierAggregateKey(tier))
	}
	return keys
}

func aggregateAppliesTo(offset int, record schema.PackRecord) bool {
	if !isTierAggregateOffset(offset) {
		kind := schema.AggregateKind(offset) + schema.AggregateData
		return kind == schema.AggregateAll || aggregateKind(record.Type) == kind
	}
	tier := record.Tier
	if tier == 0 {
		tier = schema.TierUnknown
	}
	return schema.TierAggregateKinds()[offset-typeAggregateCount] == tier
}

// typeAggregateCount is how many entries of aggregateKeys() belong to the type
// dimension; the remainder are the tier dimension.
const typeAggregateCount = int(schema.AggregateAll - schema.AggregateData + 1)

func isTierAggregateOffset(offset int) bool { return offset >= typeAggregateCount }

// removePackAggregates subtracts a deleted pack's totals from the relevant
// per-type, all-packs, and per-tier aggregates without adding any replacement
// record.
func removePackAggregates(ctx context.Context, transaction *Transaction, record schema.PackRecord) ([]Mutation, error) {
	keys := aggregateKeys()
	values, found, err := transaction.MultiGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	puts := make([]Mutation, 0, len(keys))
	for offset, key := range keys {
		if !aggregateAppliesTo(offset, record) {
			continue
		}
		if !found[offset] {
			// A type aggregate must exist once anything was published, so
			// its absence is corruption. An absent tier aggregate only means
			// this repository predates the tier dimension; deleting a pack
			// must not fail for that reason. The pending rebuild reported by
			// index check materializes the dimension later.
			if isTierAggregateOffset(offset) {
				continue
			}
			return nil, fmt.Errorf("pack aggregate %q is missing", key)
		}
		aggregate, err := schema.UnmarshalPackAggregate(values[offset].Value)
		if err != nil {
			return nil, err
		}
		if err := subtractPackAggregate(&aggregate, record); err != nil {
			return nil, err
		}
		aggregate.UpdateSequence++
		encoded, err := aggregate.MarshalBinary()
		if err != nil {
			return nil, err
		}
		puts = append(puts, Mutation{Key: key, Value: encoded})
	}
	return puts, nil
}

func aggregateKind(packType schema.PackType) schema.AggregateKind {
	return schema.AggregateKind(packType)
}

func subtractPackAggregate(aggregate *schema.PackAggregate, record schema.PackRecord) error {
	if aggregate.PackCount == 0 || aggregate.PhysicalSize < record.PhysicalSize ||
		aggregate.PayloadSize < record.PayloadSize ||
		aggregate.HeaderSize < record.HeaderSize ||
		aggregate.BlobCount < record.BlobCount {
		return fmt.Errorf("pack aggregate underflow")
	}
	if record.UsageKnown &&
		(aggregate.AccountedPackCount == 0 ||
			aggregate.UsedPayloadBytes < record.UsedPayloadBytes ||
			aggregate.UnusedPayloadBytes < record.UnusedPayloadBytes) {
		return fmt.Errorf("pack aggregate usage underflow")
	}
	aggregate.PackCount--
	aggregate.PhysicalSize -= record.PhysicalSize
	aggregate.PayloadSize -= record.PayloadSize
	aggregate.HeaderSize -= record.HeaderSize
	aggregate.BlobCount -= record.BlobCount
	if record.UsageKnown {
		aggregate.AccountedPackCount--
		aggregate.UsedPayloadBytes -= record.UsedPayloadBytes
		aggregate.UnusedPayloadBytes -= record.UnusedPayloadBytes
	}
	return nil
}

func addPackAggregate(aggregate *schema.PackAggregate, record schema.PackRecord) error {
	if aggregate.PackCount == math.MaxUint64 || math.MaxUint64-aggregate.PhysicalSize < record.PhysicalSize ||
		math.MaxUint64-aggregate.PayloadSize < record.PayloadSize ||
		math.MaxUint64-aggregate.HeaderSize < record.HeaderSize ||
		math.MaxUint64-aggregate.BlobCount < record.BlobCount {
		return fmt.Errorf("pack aggregate overflow")
	}
	if record.UsageKnown &&
		(aggregate.AccountedPackCount == math.MaxUint64 ||
			math.MaxUint64-aggregate.UsedPayloadBytes < record.UsedPayloadBytes ||
			math.MaxUint64-aggregate.UnusedPayloadBytes < record.UnusedPayloadBytes) {
		return fmt.Errorf("pack aggregate usage overflow")
	}
	aggregate.PackCount++
	aggregate.PhysicalSize += record.PhysicalSize
	aggregate.PayloadSize += record.PayloadSize
	aggregate.HeaderSize += record.HeaderSize
	aggregate.BlobCount += record.BlobCount
	if record.UsageKnown {
		aggregate.AccountedPackCount++
		aggregate.UsedPayloadBytes += record.UsedPayloadBytes
		aggregate.UnusedPayloadBytes += record.UnusedPayloadBytes
	}
	return nil
}

func (store *SchemaStore) Put(ctx context.Context, key, value []byte, durable bool) error {
	return store.WriteMutableBatch(ctx, []Mutation{{Key: key, Value: value}}, nil, durable)
}

// WriteMutableBatch atomically updates independently mutable schema records.
// Immutable records, current pointers, and the revision counter require their
// dedicated transactional operations.
func (store *SchemaStore) WriteMutableBatch(
	ctx context.Context,
	puts []Mutation,
	deletes [][]byte,
	durable bool,
) error {
	if err := validateDistinctMutations(puts, deletes); err != nil {
		return err
	}
	for _, put := range puts {
		if err := validateMutableKey(put.Key); err != nil {
			return err
		}
		if err := schema.ValidateValue(put.Key, put.Value); err != nil {
			return err
		}
	}
	for _, key := range deletes {
		if err := validateMutableDeleteKey(key); err != nil {
			return err
		}
	}
	acknowledged, err := store.client.WriteBatch(ctx, puts, deletes, durable, "")
	if err != nil {
		return err
	}
	if durable && !acknowledged {
		return fmt.Errorf("vaulticdb did not acknowledge durable schema write")
	}
	return nil
}

// PublishSchemaBatch atomically creates immutable records and updates mutable
// catalog, aggregate, reverse-index, GC, and crawl-debt records. Existing
// immutable values must match byte-for-byte.
func (store *SchemaStore) PublishSchemaBatch(ctx context.Context, puts []Mutation, deletes [][]byte) error {
	return store.publishSchemaBatch(ctx, puts, deletes, "")
}

// PublishSchemaBatchWithIdempotency publishes one logical schema transaction and
// permits an ambiguous commit to be recovered with the same durable key.
func (store *SchemaStore) PublishSchemaBatchWithIdempotency(
	ctx context.Context,
	puts []Mutation,
	deletes [][]byte,
	idempotencyKey string,
) error {
	if idempotencyKey == "" {
		return fmt.Errorf("idempotency key is required")
	}
	return store.publishSchemaBatch(ctx, puts, deletes, idempotencyKey)
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func (store *SchemaStore) publishSchemaBatch(
	ctx context.Context,
	puts []Mutation,
	deletes [][]byte,
	idempotencyKey string,
) error {
	seen := make(map[string]struct{}, len(puts)+len(deletes))
	immutableKeys := make([][]byte, 0, len(puts))
	immutableIndexes := make([]int, 0, len(puts))
	for index, put := range puts {
		keyString := string(put.Key)
		if _, duplicate := seen[keyString]; duplicate {
			return fmt.Errorf("schema batch contains duplicate key")
		}
		seen[keyString] = struct{}{}
		immutable, err := validatePublishKey(put.Key)
		if err != nil {
			return err
		}
		if err := schema.ValidateValue(put.Key, put.Value); err != nil {
			return err
		}
		if immutable {
			immutableKeys = append(immutableKeys, put.Key)
			immutableIndexes = append(immutableIndexes, index)
		}
	}
	for _, key := range deletes {
		keyString := string(key)
		if _, duplicate := seen[keyString]; duplicate {
			return fmt.Errorf("schema batch contains duplicate key")
		}
		seen[keyString] = struct{}{}
		if err := validateMutableDeleteKey(key); err != nil {
			return err
		}
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	remainingPuts := append([]Mutation(nil), puts...)
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if len(immutableKeys) > 0 {
		values, found, err := transaction.MultiGet(ctx, immutableKeys)
		if err != nil {
			rollbackTransaction(ctx, transaction)
			return err
		}
		remove := make(map[int]struct{})
		for offset, exists := range found {
			if !exists {
				continue
			}
			index := immutableIndexes[offset]
			if !bytes.Equal(values[offset].Value, puts[index].Value) {
				rollbackTransaction(ctx, transaction)
				return fmt.Errorf("immutable schema record already exists with different data")
			}
			remove[index] = struct{}{}
		}
		if len(remove) > 0 {
			remainingPuts = remainingPuts[:0]
			for index, put := range puts {
				if _, skip := remove[index]; !skip {
					remainingPuts = append(remainingPuts, put)
				}
			}
		}
	}
	if len(remainingPuts) == 0 && len(deletes) == 0 {
		return transaction.Rollback(ctx)
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), remainingPuts, deletes); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.CommitWithIdempotency(ctx, idempotencyKey); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

func validateMutableKey(key []byte) error {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return err
	}
	switch parsed.Kind {
	case schema.KeyPack,
		schema.KeyPackAggregate,
		schema.KeyTierAggregate,
		schema.KeyReverseManifest,
		schema.KeyReverseInode,
		schema.KeyReferenceCount,
		schema.KeyGarbageCollection,
		schema.KeyCrawlDebt,
		schema.KeyImportCheckpoint,
		schema.KeySnapshotImportCheckpoint,
		schema.KeyExportCheckpoint,
		schema.KeyPackHistoryBucket,
		schema.KeyHistoryRawFloor,
		schema.KeyHistoryEnabledAt,
		schema.KeyPackPlacement,
		schema.KeyBackendPack,
		schema.KeyPlacementDeleteQueue,
		schema.KeyPlacementRequest,
		schema.KeyRepackLineage,
		schema.KeyPromotionEligibility,
		schema.KeyAnalyticsFact,
		schema.KeyAnalyticsCache,
		schema.KeyAnalyticsMetadata,
		schema.KeyAnalyticsBuildCheckpoint,
		schema.KeyAnalyticsDictionary,
		schema.KeyAnalyticsFactSegment,
		schema.KeyAnalyticsSegmentMetadata,
		schema.KeyAnalyticsDimensionIndex,
		schema.KeyAnalyticsResidency,
		schema.KeyAnalyticsWatermark,
		schema.KeyAnalyticsManifest,
		schema.KeyAnalyticsQueryResult,
		schema.KeyAnalyticsQueryHeat,
		schema.KeyAnalyticsQueryView,
		schema.KeyAnalyticsQueryJob,
		schema.KeyGrowthTime,
		schema.KeyGrowthPath,
		schema.KeyUserSummary,
		schema.KeyGroupSummary,
		schema.KeyUserStats,
		schema.KeyGroupStats,
		schema.KeyUserChurn,
		schema.KeyUserInode,
		schema.KeyUserBlob,
		schema.KeyUserBlobContribution,
		schema.KeyAnalyticsDerivedMarker,
		schema.KeyPathVersion,
		schema.KeyUIDExclusionPolicy:
		return nil
	case schema.KeyPackHistory:
		// The event log is append-only: entries are written by the catalog
		// transition that produced them, never rewritten through a batch.
		return fmt.Errorf("pack history events are append-only")
	case schema.KeyNextRevision:
		return fmt.Errorf("revision sequence requires AllocateRevision")
	default:
		return fmt.Errorf("schema key requires a dedicated transactional operation")
	}
}

func validateMutableDeleteKey(key []byte) error {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return err
	}
	switch parsed.Kind {
	case schema.KeyReverseManifest,
		schema.KeyReverseInode,
		schema.KeyReferenceCount,
		schema.KeyGarbageCollection,
		schema.KeyExportIndexCheckpoint,
		schema.KeyPackHistory,
		schema.KeyPackHistoryBucket,
		schema.KeyPackPlacement,
		schema.KeyBackendPack,
		schema.KeyPlacementDeleteQueue,
		schema.KeyPlacementRequest,
		schema.KeyRepackLineage,
		schema.KeyPromotionEligibility,
		schema.KeyAnalyticsFact,
		schema.KeyAnalyticsCache,
		schema.KeyAnalyticsMetadata,
		schema.KeyAnalyticsBuildCheckpoint,
		schema.KeyAnalyticsDictionary,
		schema.KeyAnalyticsFactSegment,
		schema.KeyAnalyticsSegmentMetadata,
		schema.KeyAnalyticsDimensionIndex,
		schema.KeyAnalyticsResidency,
		schema.KeyAnalyticsDelta,
		schema.KeyAnalyticsWatermark,
		schema.KeyAnalyticsManifest,
		schema.KeyAnalyticsQueryResult,
		schema.KeyAnalyticsQueryHeat,
		schema.KeyAnalyticsQueryView,
		schema.KeyAnalyticsQueryJob,
		schema.KeyGrowthTime,
		schema.KeyGrowthPath,
		schema.KeyUserSummary,
		schema.KeyGroupSummary,
		schema.KeyUserStats,
		schema.KeyGroupStats,
		schema.KeyUserChurn,
		schema.KeyUserInode,
		schema.KeyUserBlob,
		schema.KeyUserBlobContribution,
		schema.KeyAnalyticsDerivedMarker,
		schema.KeyPathVersion,
		schema.KeyUIDExclusionPolicy:
		// History is explicitly prunable: it is derived, advisory, and retained
		// on its own schedule.
		return nil
	default:
		return fmt.Errorf("schema key must remain visible and cannot be deleted")
	}
}

func validatePublishKey(key []byte) (bool, error) {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return false, err
	}
	switch parsed.Kind {
	case schema.KeyBlob, schema.KeyInodeRevision, schema.KeyDirectoryRevision, schema.KeySnapshot:
		return true, nil
	case schema.KeyPack,
		schema.KeyPackAggregate,
		schema.KeyTierAggregate,
		schema.KeyReverseManifest,
		schema.KeyReverseInode,
		schema.KeyReferenceCount,
		schema.KeyGarbageCollection,
		schema.KeyCrawlDebt,
		schema.KeyImportCheckpoint,
		schema.KeySnapshotImportCheckpoint,
		schema.KeyExportCheckpoint,
		schema.KeyPackHistoryBucket,
		schema.KeyHistoryRawFloor,
		schema.KeyHistoryEnabledAt,
		schema.KeyPackPlacement,
		schema.KeyBackendPack,
		schema.KeyPlacementDeleteQueue,
		schema.KeyPlacementRequest,
		schema.KeyRepackLineage,
		schema.KeyPromotionEligibility,
		schema.KeyAnalyticsFact,
		schema.KeyAnalyticsCache,
		schema.KeyAnalyticsMetadata,
		schema.KeyAnalyticsBuildCheckpoint,
		schema.KeyPathVersion:
		return false, nil
	case schema.KeyPackHistory:
		return true, nil
	case schema.KeyContentManifest:
		return false, fmt.Errorf("content manifests require CreateContentManifest")
	case schema.KeyNextRevision:
		return false, fmt.Errorf("revision sequence requires AllocateRevision")
	default:
		return false, fmt.Errorf("schema key requires a dedicated transactional operation")
	}
}

// MarkExportPending durably exposes a legacy snapshot whose authoritative
// scope has not yet been committed.
func (store *SchemaStore) MarkExportPending(ctx context.Context, snapshotID schema.ID, rootKey []byte) error {
	record := schema.ExportCheckpointRecord{State: schema.ExportPending, RootKey: append([]byte(nil), rootKey...)}
	encoded, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return store.Put(ctx, schema.ExportCheckpointKey(snapshotID), encoded, true)
}
