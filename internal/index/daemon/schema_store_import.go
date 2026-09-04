package daemon

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PublishPack atomically merges a newly-written pack's catalog entry, blob
// locations, and aggregates. Retries are idempotent and preserve existing
// duplicate blob locations from other packs.
func (store *SchemaStore) PublishPack(ctx context.Context, published PublishedPack) error {
	imported := LegacyPackImport{
		PackID: published.PackID, Record: published.Record, Blobs: published.Blobs,
		Placements:         published.Placements,
		PredecessorPackIDs: published.PredecessorPackIDs, LineageKind: published.LineageKind, RunID: published.RunID,
	}
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.importPackOnce(ctx, imported, false)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("publish pack: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) importPackOnce(ctx context.Context, imported LegacyPackImport, legacy bool) error {
	if imported.PackID == (schema.ID{}) || (legacy && imported.SourceIndex == (schema.ID{})) {
		return fmt.Errorf("pack publication requires its identity%s", map[bool]string{true: " and source index", false: ""}[legacy])
	}
	wantLifecycle := schema.PackExportPending
	if legacy {
		wantLifecycle = schema.PackImported
	}
	if imported.Record.Lifecycle != wantLifecycle {
		return fmt.Errorf("pack publication has invalid lifecycle")
	}
	locationTypes := make([]schema.BlobType, 0, imported.Record.BlobCount)
	var locationCount uint64
	for blobID, blob := range imported.Blobs {
		blob = canonicalBlobRecord(blob)
		imported.Blobs[blobID] = blob
		for _, location := range blob.Locations {
			if location.PackID != imported.PackID {
				return fmt.Errorf("legacy blob location belongs to a different pack")
			}
			locationTypes = append(locationTypes, location.Type)
			locationCount++
		}
	}
	if locationCount != imported.Record.BlobCount || schema.ClassifyPack(locationTypes) != imported.Record.Type {
		return fmt.Errorf("legacy pack record does not match its blob locations")
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		rollbackTransaction(ctx, transaction)
		return err
	}

	packKey := schema.PackKey(imported.PackID)
	packValue, packFound, err := transaction.Get(ctx, packKey)
	if err != nil {
		return fail(err)
	}
	var oldRecord *schema.PackRecord
	if packFound {
		record, decodeErr := schema.UnmarshalPackRecord(packValue)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		oldRecord = &record
		imported.Record = mergeImportedPackRecord(record, imported.Record, imported.SourceIndex, legacy)
	} else {
		if legacy {
			imported.Record.SourceIndexIDs = appendUniqueID(imported.Record.SourceIndexIDs, imported.SourceIndex)
		}
	}
	sort.Slice(imported.Record.SourceIndexIDs, func(left, right int) bool {
		return bytes.Compare(imported.Record.SourceIndexIDs[left][:], imported.Record.SourceIndexIDs[right][:]) < 0
	})

	puts := make([]Mutation, 0, len(imported.Blobs)+7)
	var newLocationCount, newPayloadSize uint64
	for blobID, incoming := range imported.Blobs {
		key := schema.BlobKey(blobID)
		value, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			return fail(getErr)
		}
		if found {
			existing, decodeErr := schema.UnmarshalBlobRecord(value)
			if decodeErr != nil {
				return fail(decodeErr)
			}
			for _, location := range incoming.Locations {
				if !containsPhysicalLocation(existing.Locations, location) {
					newLocationCount++
					if math.MaxUint64-newPayloadSize < uint64(location.Length) {
						return fail(fmt.Errorf("pack payload size overflow"))
					}
					newPayloadSize += uint64(location.Length)
				}
			}
			incoming = mergeBlobRecords(existing, incoming)
		} else {
			newLocationCount += uint64(len(incoming.Locations))
			for _, location := range incoming.Locations {
				if math.MaxUint64-newPayloadSize < uint64(location.Length) {
					return fail(fmt.Errorf("pack payload size overflow"))
				}
				newPayloadSize += uint64(location.Length)
			}
		}
		incoming = canonicalBlobRecord(incoming)
		encoded, encodeErr := incoming.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: key, Value: encoded})
	}
	if oldRecord != nil {
		if math.MaxUint64-oldRecord.BlobCount < newLocationCount || math.MaxUint64-oldRecord.PayloadSize < newPayloadSize {
			return fail(fmt.Errorf("pack catalog size overflow"))
		}
		imported.Record.BlobCount = oldRecord.BlobCount + newLocationCount
		imported.Record.PayloadSize = oldRecord.PayloadSize + newPayloadSize
	}
	if imported.Record.PhysicalSizeKnown {
		if imported.Record.PhysicalSize < imported.Record.PayloadSize {
			imported.Record.HeaderSize = 0
		} else {
			imported.Record.HeaderSize = imported.Record.PhysicalSize - imported.Record.PayloadSize
		}
	}
	clearStalePackUsage(&imported.Record)
	encodedPack, err := imported.Record.MarshalBinary()
	if err != nil {
		return fail(err)
	}
	puts = append(puts, Mutation{Key: packKey, Value: encodedPack})
	placementPuts, err := placementMutations(imported.PackID, imported.Placements)
	if err != nil {
		return fail(err)
	}
	puts = append(puts, placementPuts...)
	for _, predecessor := range imported.PredecessorPackIDs {
		kind := imported.LineageKind
		if kind == 0 {
			kind = schema.LineageRepack
		}
		lineage, encodeErr := (schema.RepackLineageRecord{RunID: imported.RunID, Kind: kind}).MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: schema.RepackLineageKey(predecessor, imported.PackID), Value: lineage})
	}

	aggregates, err := updatePackAggregates(ctx, transaction, oldRecord, imported.Record)
	if err != nil {
		return fail(err)
	}
	puts = append(puts, aggregates...)
	if imported.Debt != nil {
		if len(imported.DebtKey) == 0 {
			return fail(fmt.Errorf("legacy pack debt requires a key"))
		}
		debtValue, encodeErr := imported.Debt.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		if encodeErr = schema.ValidateValue(imported.DebtKey, debtValue); encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: imported.DebtKey, Value: debtValue})
	} else {
		debtKey := schema.CrawlDebtKey(schema.ID{}, imported.PackID)
		debtValue, found, getErr := transaction.Get(ctx, debtKey)
		if getErr != nil {
			return fail(getErr)
		}
		if found {
			debt, decodeErr := schema.UnmarshalCrawlDebtRecord(debtValue)
			if decodeErr != nil {
				return fail(decodeErr)
			}
			if debt.Reason == schema.DebtUnavailablePack && debt.Status != schema.DebtResolved {
				debt.Status = schema.DebtResolved
				debt.ErrorClass = ""
				debt.LastAttemptUnixNano = time.Now().UnixNano()
				encoded, encodeErr := debt.MarshalBinary()
				if encodeErr != nil {
					return fail(encodeErr)
				}
				puts = append(puts, Mutation{Key: debtKey, Value: encoded})
			}
		}
	}
	limits := store.client.Limits()
	if imported.BatchSize > 0 && imported.BatchSize < limits.MaxBatchItems {
		limits.MaxBatchItems = imported.BatchSize
	}
	eventType := schema.EventCreated
	if legacy {
		eventType = schema.EventImported
	}
	// A pack that supersedes others is a rewrite, not new data, so it is
	// recorded with its lineage rather than as a plain creation.
	if len(imported.PredecessorPackIDs) != 0 {
		eventType = schema.EventRepackedInto
		if imported.LineageKind == schema.LineagePromotion {
			eventType = schema.EventPromoted
		}
	}
	usedDelta, unusedDelta := usageDeltas(oldRecord, imported.Record)
	events := []PackEvent{{
		PackID: imported.PackID,
		Record: schema.PackHistoryEvent{
			Type: eventType, PackType: imported.Record.Type,
			PhysicalSize: imported.Record.PhysicalSize, PayloadSize: imported.Record.PayloadSize,
			UsedDelta: usedDelta, UnusedDelta: unusedDelta,
			PredecessorPackIDs: imported.PredecessorPackIDs, RunID: imported.RunID,
		},
	}}
	// A pack whose recorded tier became known, or moved, is a distinct
	// transition from its creation and is recorded separately.
	if oldRecord != nil && oldRecord.Tier != imported.Record.Tier {
		events = append(events, PackEvent{
			PackID: imported.PackID,
			Record: schema.PackHistoryEvent{
				Type: schema.EventTierChanged, PackType: imported.Record.Type,
				PhysicalSize: imported.Record.PhysicalSize, PayloadSize: imported.Record.PayloadSize,
				RunID: imported.RunID, ReasonCode: imported.Record.Tier.String(),
			},
		})
	}
	history, err := packHistoryMutations(ctx, transaction, events)
	if err != nil {
		return fail(err)
	}
	puts = append(puts, history...)
	if err := writeTransactionBatches(ctx, transaction, limits, puts, nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

func (store *SchemaStore) MarkPackPublished(ctx context.Context, packID schema.ID) error {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.markPackPublishedOnce(ctx, packID)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("mark pack published: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) MarkIndexPublished(ctx context.Context, indexID schema.ID, packIDs []schema.ID) (uint64, error) {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		sequence, err := store.markIndexPublishedOnce(ctx, indexID, packIDs)
		if status.Code(err) != codes.Aborted {
			return sequence, err
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
	return 0, fmt.Errorf("mark index published: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) markIndexPublishedOnce(ctx context.Context, indexID schema.ID, packIDs []schema.ID) (uint64, error) {
	if indexID == (schema.ID{}) || len(packIDs) == 0 {
		return 0, fmt.Errorf("export index and pack IDs are required")
	}
	packIDs = append([]schema.ID(nil), packIDs...)
	sort.Slice(packIDs, func(left, right int) bool { return bytes.Compare(packIDs[left][:], packIDs[right][:]) < 0 })
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (uint64, error) {
		rollbackTransaction(ctx, transaction)
		return 0, err
	}
	checkpointKey := schema.ExportIndexCheckpointKey(indexID)
	checkpointValue, found, err := transaction.Get(ctx, checkpointKey)
	if err != nil {
		return fail(err)
	}
	var checkpoint schema.ExportIndexCheckpointRecord
	puts := make([]Mutation, 0, len(packIDs)+2)
	if found {
		checkpoint, err = schema.UnmarshalExportIndexCheckpointRecord(checkpointValue)
		if err != nil || !slices.Equal(checkpoint.PackIDs, packIDs) {
			return fail(fmt.Errorf("export index checkpoint conflicts with pack provenance"))
		}
	} else {
		nextValue, nextFound, getErr := transaction.Get(ctx, schema.NextExportSequenceKey())
		if getErr != nil {
			return fail(getErr)
		}
		next := uint64(1)
		if nextFound {
			next, err = schema.UnmarshalNextExportSequence(nextValue)
			if err != nil {
				return fail(err)
			}
		}
		if next == math.MaxUint64 {
			return fail(fmt.Errorf("export sequence overflow"))
		}
		checkpoint = schema.ExportIndexCheckpointRecord{Sequence: next, PackIDs: packIDs}
		checkpointValue, err = checkpoint.MarshalBinary()
		if err != nil {
			return fail(err)
		}
		encodedNext, encodeErr := schema.MarshalNextExportSequence(next + 1)
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: checkpointKey, Value: checkpointValue}, Mutation{Key: schema.NextExportSequenceKey(), Value: encodedNext})
	}
	for _, packID := range packIDs {
		key := schema.PackKey(packID)
		value, packFound, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			return fail(getErr)
		}
		if !packFound {
			return fail(fmt.Errorf("published pack is missing"))
		}
		record, decodeErr := schema.UnmarshalPackRecord(value)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		if record.Lifecycle != schema.PackPublished {
			if record.Lifecycle != schema.PackExportPending && record.Lifecycle != schema.PackImported {
				return fail(fmt.Errorf("pack cannot transition from lifecycle %d to published", record.Lifecycle))
			}
			record.Lifecycle = schema.PackPublished
			encoded, encodeErr := record.MarshalBinary()
			if encodeErr != nil {
				return fail(encodeErr)
			}
			puts = append(puts, Mutation{Key: key, Value: encoded})
		}
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return 0, err
	}
	return checkpoint.Sequence, nil
}

func (store *SchemaStore) markPackPublishedOnce(ctx context.Context, packID schema.ID) error {
	key := schema.PackKey(packID)
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		rollbackTransaction(ctx, transaction)
		return err
	}
	value, found, err := transaction.Get(ctx, key)
	if err != nil {
		return fail(err)
	}
	if !found {
		return fail(fmt.Errorf("published pack is missing"))
	}
	record, err := schema.UnmarshalPackRecord(value)
	if err != nil {
		return fail(err)
	}
	if record.Lifecycle == schema.PackPublished {
		return transaction.Rollback(ctx)
	}
	if record.Lifecycle != schema.PackExportPending && record.Lifecycle != schema.PackImported {
		return fail(fmt.Errorf("pack cannot transition from lifecycle %d to published", record.Lifecycle))
	}
	record.Lifecycle = schema.PackPublished
	encoded, err := record.MarshalBinary()
	if err != nil {
		return fail(err)
	}
	puts := []Mutation{{Key: key, Value: encoded}}
	history, err := packHistoryMutations(ctx, transaction, []PackEvent{{
		PackID: packID,
		Record: schema.PackHistoryEvent{
			Type: schema.EventPublished, PackType: record.Type,
			PhysicalSize: record.PhysicalSize, PayloadSize: record.PayloadSize,
		},
	}})
	if err != nil {
		return fail(err)
	}
	if err := transaction.WriteBatch(ctx, append(puts, history...), nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

func placementMutations(packID schema.ID, placements map[uint64]schema.PlacementRecord) ([]Mutation, error) {
	if len(placements) == 0 {
		return nil, nil
	}
	puts := make([]Mutation, 0, 2*len(placements))
	for backend, placement := range placements {
		if backend == 0 {
			return nil, fmt.Errorf("pack placement requires a backend")
		}
		value, err := placement.MarshalBinary()
		if err != nil {
			return nil, err
		}
		puts = append(puts, Mutation{Key: schema.PackPlacementKey(packID, backend), Value: value})
		backendValue, err := (schema.BackendPackRecord{
			State: placement.State, Bytes: placement.Bytes, PlacedAt: placement.PlacedAt,
		}).MarshalBinary()
		if err != nil {
			return nil, err
		}
		puts = append(puts, Mutation{Key: schema.BackendPackKey(backend, packID), Value: backendValue})
	}
	return puts, nil
}

func (store *SchemaStore) packPlacementKeys(ctx context.Context, packID schema.ID) ([][]byte, error) {
	prefix := schema.PackPlacementPrefix(packID)
	keys := make([][]byte, 0)
	var after []byte
	for {
		entries, done, err := store.client.ScanPage(ctx, prefix, after, store.client.Limits().MaxPageItems, "")
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			keys = append(keys, append([]byte(nil), entry.Key...))
			after = append(after[:0], entry.Key...)
		}
		if done {
			return keys, nil
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("scan placement keys made no progress")
		}
	}
}

// MarkPackDeletePending begins the two-phase deletion of a pack that GC has
// confirmed is wholly unreachable. It is idempotent and retried on conflict.
func (store *SchemaStore) MarkPackDeletePending(ctx context.Context, packID schema.ID) error {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.markPackDeletePendingOnce(ctx, packID)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("mark pack delete-pending: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) markPackDeletePendingOnce(ctx context.Context, packID schema.ID) error {
	key := schema.PackKey(packID)
	placementKeys, err := store.packPlacementKeys(ctx, packID)
	if err != nil {
		return err
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		rollbackTransaction(ctx, transaction)
		return err
	}
	value, found, err := transaction.Get(ctx, key)
	if err != nil {
		return fail(err)
	}
	if !found {
		return fail(fmt.Errorf("pack to delete is missing"))
	}
	record, err := schema.UnmarshalPackRecord(value)
	if err != nil {
		return fail(err)
	}
	if record.Lifecycle == schema.PackDeletePending {
		return transaction.Rollback(ctx)
	}
	if record.Lifecycle != schema.PackPublished {
		return fail(fmt.Errorf("pack cannot transition from lifecycle %d to delete-pending", record.Lifecycle))
	}
	record.Lifecycle = schema.PackDeletePending
	encoded, err := record.MarshalBinary()
	if err != nil {
		return fail(err)
	}
	puts := []Mutation{{Key: key, Value: encoded}}
	now := time.Now().UnixNano()
	for _, placementKey := range placementKeys {
		value, found, getErr := transaction.Get(ctx, placementKey)
		if getErr != nil {
			return fail(getErr)
		}
		if !found {
			continue
		}
		parsed, parseErr := schema.ParseKey(placementKey)
		if parseErr != nil || parsed.Kind != schema.KeyPackPlacement {
			return fail(fmt.Errorf("invalid placement key %q", placementKey))
		}
		placement, decodeErr := schema.UnmarshalPlacementRecord(value)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		if placement.State != schema.PlacementLive && placement.State != schema.PlacementPending {
			continue
		}
		placement.State = schema.PlacementEvicting
		placement.DeleteAfter = now
		if placement.RetentionSource != schema.RetentionUnknown && placement.MinRetentionUntil > placement.DeleteAfter {
			placement.DeleteAfter = placement.MinRetentionUntil
		}
		encodedPlacement, encodeErr := placement.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: placementKey, Value: encodedPlacement})
		backendValue, encodeErr := (schema.BackendPackRecord{State: placement.State, Bytes: placement.Bytes, PlacedAt: placement.PlacedAt}).MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: schema.BackendPackKey(parsed.Backend, packID), Value: backendValue})
		queueValue, encodeErr := (schema.PlacementDeleteRecord{Backend: parsed.Backend, PhysicalSize: placement.Bytes, Reason: "gc", RunID: schema.ID{}}).MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts = append(puts, Mutation{Key: schema.PlacementDeleteQueueKey(placement.DeleteAfter, packID, parsed.Backend), Value: queueValue})
	}
	history, err := packHistoryMutations(ctx, transaction, []PackEvent{{
		PackID: packID,
		Record: schema.PackHistoryEvent{
			Type: schema.EventDeletePending, PackType: record.Type,
			PhysicalSize: record.PhysicalSize, PayloadSize: record.PayloadSize,
		},
	}})
	if err != nil {
		return fail(err)
	}
	if err := transaction.WriteBatch(ctx, append(puts, history...), nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

// MarkPackDeleted completes two-phase deletion after the backend object has
// been physically removed. It purges the pack catalog record, strips this
// pack's locations from every member blob (deleting blobs left with none),
// decrements aggregates, and removes stale GC bookkeeping. memberBlobIDs must
// be the exact set of blob IDs that had a location in this pack.
func (store *SchemaStore) MarkPackDeleted(ctx context.Context, packID schema.ID, memberBlobIDs []schema.ID) error {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.markPackDeletedOnce(ctx, packID, memberBlobIDs)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("mark pack deleted: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) markPackDeletedOnce(ctx context.Context, packID schema.ID, memberBlobIDs []schema.ID) error {
	packKey := schema.PackKey(packID)
	placementKeys, err := store.packPlacementKeys(ctx, packID)
	if err != nil {
		return err
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		rollbackTransaction(ctx, transaction)
		return err
	}
	packValue, found, err := transaction.Get(ctx, packKey)
	if err != nil {
		return fail(err)
	}
	if !found {
		return fail(fmt.Errorf("pack to delete is missing"))
	}
	record, err := schema.UnmarshalPackRecord(packValue)
	if err != nil {
		return fail(err)
	}
	if record.Lifecycle != schema.PackDeletePending {
		return fail(fmt.Errorf("pack cannot be deleted from lifecycle %d", record.Lifecycle))
	}

	deletes := make([][]byte, 0, len(memberBlobIDs)+2+3*len(placementKeys))
	puts := make([]Mutation, 0, len(memberBlobIDs)+5)
	deletes = append(deletes, packKey)
	for _, placementKey := range placementKeys {
		parsed, parseErr := schema.ParseKey(placementKey)
		if parseErr != nil || parsed.Kind != schema.KeyPackPlacement {
			return fail(fmt.Errorf("invalid placement key %q", placementKey))
		}
		deletes = append(deletes, placementKey, schema.BackendPackKey(parsed.Backend, packID))
		value, found, getErr := transaction.Get(ctx, placementKey)
		if getErr != nil {
			return fail(getErr)
		}
		if !found {
			continue
		}
		placement, decodeErr := schema.UnmarshalPlacementRecord(value)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		if placement.DeleteAfter != 0 {
			deletes = append(deletes, schema.PlacementDeleteQueueKey(placement.DeleteAfter, packID, parsed.Backend))
		}
	}

	for _, blobID := range memberBlobIDs {
		blobKey := schema.BlobKey(blobID)
		value, blobFound, getErr := transaction.Get(ctx, blobKey)
		if getErr != nil {
			return fail(getErr)
		}
		if !blobFound {
			continue
		}
		blob, decodeErr := schema.UnmarshalBlobRecord(value)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		remaining := make([]schema.BlobLocation, 0, len(blob.Locations))
		for _, location := range blob.Locations {
			if location.PackID != packID {
				remaining = append(remaining, location)
			}
		}
		switch {
		case len(remaining) == 0:
			deletes = append(deletes, blobKey)
		case len(remaining) != len(blob.Locations):
			blob.Locations = remaining
			encoded, encodeErr := blob.MarshalBinary()
			if encodeErr != nil {
				return fail(encodeErr)
			}
			puts = append(puts, Mutation{Key: blobKey, Value: encoded})
		}
	}

	aggregatePuts, err := removePackAggregates(ctx, transaction, record)
	if err != nil {
		return fail(err)
	}
	puts = append(puts, aggregatePuts...)

	// The deletion event carries the pack's sizes because the catalog record
	// is removed in this same transaction; history must stay readable for
	// packs that no longer exist.
	usedDelta, unusedDelta := usageDeltas(&record, schema.PackRecord{})
	history, err := packHistoryMutations(ctx, transaction, []PackEvent{{
		PackID: packID,
		Record: schema.PackHistoryEvent{
			Type: schema.EventDeleted, PackType: record.Type,
			PhysicalSize: record.PhysicalSize, PayloadSize: record.PayloadSize,
			UsedDelta: usedDelta, UnusedDelta: unusedDelta,
		},
	}})
	if err != nil {
		return fail(err)
	}
	puts = append(puts, history...)

	staleGCKeys := make([][]byte, 0, len(memberBlobIDs)+1)
	staleGCKeys = append(staleGCKeys, schema.GarbageCollectionKey(schema.GCPack, packID))
	for _, blobID := range memberBlobIDs {
		staleGCKeys = append(staleGCKeys, schema.GarbageCollectionKey(schema.GCBlob, blobID))
	}
	for _, key := range staleGCKeys {
		_, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			return fail(getErr)
		}
		if found {
			deletes = append(deletes, key)
		}
	}

	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, deletes); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

func mergeBlobRecords(existing, incoming schema.BlobRecord) schema.BlobRecord {
	result := schema.BlobRecord{Locations: append([]schema.BlobLocation(nil), existing.Locations...)}
	for _, candidate := range incoming.Locations {
		found := false
		for index, location := range result.Locations {
			if samePhysicalLocation(location, candidate) {
				if candidate.UncompressedSize > location.UncompressedSize {
					result.Locations[index].UncompressedSize = candidate.UncompressedSize
				}
				found = true
				break
			}
		}
		if !found {
			result.Locations = append(result.Locations, candidate)
		}
	}
	return canonicalBlobRecord(result)
}

func canonicalBlobRecord(record schema.BlobRecord) schema.BlobRecord {
	canonical := make([]schema.BlobLocation, 0, len(record.Locations))
	for _, candidate := range record.Locations {
		merged := false
		for index, location := range canonical {
			if samePhysicalLocation(location, candidate) {
				if candidate.UncompressedSize > location.UncompressedSize {
					canonical[index].UncompressedSize = candidate.UncompressedSize
				}
				merged = true
				break
			}
		}
		if !merged {
			canonical = append(canonical, candidate)
		}
	}
	record.Locations = canonical
	sort.Slice(record.Locations, func(left, right int) bool {
		leftLocation, rightLocation := record.Locations[left], record.Locations[right]
		if comparison := bytes.Compare(leftLocation.PackID[:], rightLocation.PackID[:]); comparison != 0 {
			return comparison < 0
		}
		if leftLocation.Type != rightLocation.Type {
			return leftLocation.Type < rightLocation.Type
		}
		if leftLocation.Offset != rightLocation.Offset {
			return leftLocation.Offset < rightLocation.Offset
		}
		if leftLocation.Length != rightLocation.Length {
			return leftLocation.Length < rightLocation.Length
		}
		return leftLocation.UncompressedSize < rightLocation.UncompressedSize
	})
	return record
}

func samePhysicalLocation(left, right schema.BlobLocation) bool {
	return left.PackID == right.PackID && left.Offset == right.Offset && left.Length == right.Length && left.Type == right.Type
}

func containsPhysicalLocation(locations []schema.BlobLocation, candidate schema.BlobLocation) bool {
	for _, location := range locations {
		if samePhysicalLocation(location, candidate) {
			return true
		}
	}
	return false
}

func appendUniqueID(ids []schema.ID, candidate schema.ID) []schema.ID {
	for _, id := range ids {
		if id == candidate {
			return ids
		}
	}
	return append(ids, candidate)
}

func mergeImportedPackRecord(existing, incoming schema.PackRecord, source schema.ID, includeSource bool) schema.PackRecord {
	result := existing
	if includeSource {
		result.SourceIndexIDs = appendUniqueID(result.SourceIndexIDs, source)
	}
	for _, sourceIndex := range incoming.SourceIndexIDs {
		result.SourceIndexIDs = appendUniqueID(result.SourceIndexIDs, sourceIndex)
	}
	if incoming.PhysicalSizeKnown {
		result.PhysicalSize = incoming.PhysicalSize
		result.HeaderSize = incoming.HeaderSize
		result.PhysicalSizeKnown = true
	}
	if result.Type != incoming.Type {
		result.Type = schema.PackMixed
	}
	if existing.Type == schema.PackUnknown {
		result.Type = incoming.Type
	}
	if incoming.Type == schema.PackUnknown {
		result.Type = existing.Type
	}
	applyPackLifetime(&result, existing, incoming)
	sort.Slice(result.SourceIndexIDs, func(left, right int) bool {
		return bytes.Compare(result.SourceIndexIDs[left][:], result.SourceIndexIDs[right][:]) < 0
	})
	return result
}

// applyPackLifetime resolves tier and lifetime facts when a pack is seen
// again, leaving every other field of result untouched. A known fact is never
// replaced by an unknown one, and an unknown fact is never synthesized: a
// second legacy index reporting the same pack cannot turn an unknown creation
// time into a known one.
func applyPackLifetime(result *schema.PackRecord, existing, incoming schema.PackRecord) {
	if !existing.CreationTimeKnown && incoming.CreationTimeKnown {
		result.CreationTime, result.CreationTimeKnown = incoming.CreationTime, true
	}
	if isUnknownTier(existing.Tier) && !isUnknownTier(incoming.Tier) {
		result.Tier = incoming.Tier
	}
	if existing.StorageClass == "" {
		result.StorageClass = incoming.StorageClass
	}
	if isUnknownRetention(existing.RetentionSource) && !isUnknownRetention(incoming.RetentionSource) && result.CreationTimeKnown {
		result.RetentionSource, result.MinRetentionUntil = incoming.RetentionSource, incoming.MinRetentionUntil
	}
	if incoming.DeleteAfter != 0 {
		result.DeleteAfter = incoming.DeleteAfter
	}
	if incoming.UsageKnown {
		result.UsageKnown, result.UsedPayloadBytes, result.UnusedPayloadBytes = true, incoming.UsedPayloadBytes, incoming.UnusedPayloadBytes
	}
}

func isUnknownTier(tier schema.PackTier) bool {
	return tier == 0 || tier == schema.TierUnknown
}

func isUnknownRetention(source schema.RetentionSource) bool {
	return source == 0 || source == schema.RetentionUnknown
}

// clearStalePackUsage drops usage accounting whose payload basis changed.
// Usage is recomputed by GC discovery; keeping a stale split would both fail
// the record invariant and misreport reclaimable bytes.
func clearStalePackUsage(record *schema.PackRecord) {
	if !record.UsageKnown {
		record.UsedPayloadBytes, record.UnusedPayloadBytes = 0, 0
		return
	}
	if record.UsedPayloadBytes > record.PayloadSize ||
		record.UsedPayloadBytes+record.UnusedPayloadBytes != record.PayloadSize {
		record.UsageKnown, record.UsedPayloadBytes, record.UnusedPayloadBytes = false, 0, 0
	}
}

// PackUsage is the reachable/unreachable payload split computed for one pack.
type PackUsage struct{ Used, Unused uint64 }
