package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrLegacyImportReceiptMissing = errors.New("legacy import receipt is missing")
var ErrLegacyImportReceiptUnresolved = errors.New("legacy import receipt has not been reduced")
var ErrLegacyImportFreshRequired = errors.New("stage 3 legacy import requires fresh import mode")

var legacyImportCleanupScanPageSize uint32 = 1_000
var legacyImportCleanupDeletePageSize uint32 = 256

//nolint:gocognit // Retry, idempotency recovery, and metric publication remain one transaction lifecycle.
func (store *SchemaStore) IngestLegacyPacks(
	ctx context.Context,
	session schema.ID,
	batch uint64,
	imports []LegacyPackImport,
) error {
	if session == (schema.ID{}) {
		return fmt.Errorf("legacy import session is required")
	}
	if !store.allowLegacySplitSession(session, true) {
		return fmt.Errorf("%w: rerun with --force-reset-old-idx to enable fresh import split sessions", ErrLegacyImportFreshRequired)
	}
	started := time.Now()
	defer func() { store.legacyMetrics.totalNanos.Add(uint64(time.Since(started))) }()
	store.legacyMetrics.batches.Add(1)
	prepared, err := prepareLegacyImportBatch(imports, nil)
	if err != nil {
		return err
	}
	if len(prepared) == 0 {
		return nil
	}
	receiptKey := schema.LegacyImportReceiptKey(session, batch)
	contentHash, err := legacyImportContentHash(session, batch, prepared)
	if err != nil {
		return err
	}
	hints := store.freshImportLookupHintsForBatch(prepared)
	deferDurability := store.freshImportSeen != nil
	backoff := 100 * time.Microsecond
	for attempt := range revisionAllocationAttempts {
		store.legacyMetrics.attempts.Add(1)
		attemptHints := hints
		if attempt > 0 {
			// Retried plans must reread all keys because concurrent ingest writers
			// may have committed IDs unknown to this process-local filter.
			attemptHints = legacyImportBatchHints{}
		}
		committed, err := store.ingestLegacyPacksOnce(
			ctx,
			receiptKey,
			prepared,
			contentHash,
			attemptHints,
			deferDurability,
			attempt > 0,
		)
		if status.Code(err) == codes.Aborted {
			store.legacyMetrics.conflicts.Add(1)
			store.legacyMetrics.retries.Add(1)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			backoff = min(backoff*2, 25*time.Millisecond)
			continue
		}
		if err != nil {
			resolved, resolveErr := store.lookupLegacyImportReceipt(ctx, receiptKey, contentHash)
			if resolveErr == nil {
				switch resolved {
				case receiptMatched:
					return nil
				case receiptConflict:
					return fmt.Errorf("%w: legacy import receipt content hash mismatch", ErrIdempotencyConflict)
				case receiptMissing:
				}
			}
			return err
		}
		if !committed {
			return nil
		}
		if store.freshImportSeen != nil {
			packIDs, blobIDs := uniqueLegacyImportIDs(prepared)
			for _, packID := range packIDs {
				store.freshImportSeen.insert(packID)
			}
			for _, blobID := range blobIDs {
				store.freshImportSeen.insert(blobID)
			}
		}
		store.legacyMetrics.commits.Add(1)
		store.legacyMetrics.ingestedBatches.Add(1)
		packIDs, blobIDs := uniqueLegacyImportIDs(prepared)
		store.legacyMetrics.packsCommitted.Add(uint64(len(packIDs)))
		store.legacyMetrics.blobsCommitted.Add(uint64(len(blobIDs)))
		store.legacyMetrics.sourceIndexes.Add(uniqueLegacySourceIndexCount(prepared))
		return nil
	}
	return fmt.Errorf("ingest legacy packs: %w", errors.New("transaction conflict retry limit exceeded"))
}

func (store *SchemaStore) ingestLegacyPacksOnce(
	ctx context.Context,
	receiptKey []byte,
	imports []LegacyPackImport,
	contentHash schema.ID,
	hints legacyImportBatchHints,
	deferDurability bool,
	replanned bool,
) (bool, error) {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return false, err
	}
	fail := func(err error) (bool, error) {
		rollbackTransaction(ctx, transaction)
		return false, err
	}
	value, found, err := transaction.Get(ctx, receiptKey)
	if err != nil {
		return fail(err)
	}
	if found {
		record, err := schema.UnmarshalLegacyImportReceiptRecord(value)
		if err != nil {
			return fail(err)
		}
		if record.ContentHash != contentHash {
			return fail(fmt.Errorf("%w: legacy import receipt content hash mismatch", ErrIdempotencyConflict))
		}
		if err := transaction.Rollback(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	planningStarted := time.Now()
	plan, limits, err := store.planLegacyIngestBatch(ctx, transaction, receiptKey, imports, contentHash, hints)
	store.legacyMetrics.planningNanos.Add(uint64(time.Since(planningStarted)))
	if err != nil {
		return fail(err)
	}
	if replanned {
		store.legacyMetrics.replannedBytes.Add(plan.encodedBytes)
	}
	mutationStarted := time.Now()
	mutationRPCs, err := writeTransactionBatchesMeasured(ctx, transaction, limits, plan.puts, nil)
	store.legacyMetrics.mutationRPCNanos.Add(uint64(time.Since(mutationStarted)))
	store.legacyMetrics.mutationRPCs.Add(mutationRPCs)
	if err != nil {
		return fail(err)
	}
	commit := transaction.Commit
	if deferDurability {
		commit = transaction.CommitDeferred
	}
	commitStarted := time.Now()
	if err := commit(ctx); err != nil {
		store.legacyMetrics.commitNanos.Add(uint64(time.Since(commitStarted)))
		rollbackTransaction(ctx, transaction)
		return false, err
	}
	store.legacyMetrics.commitNanos.Add(uint64(time.Since(commitStarted)))
	store.legacyMetrics.mutationsCommitted.Add(uint64(len(plan.puts)))
	store.legacyMetrics.encodedBytesCommitted.Add(plan.encodedBytes)
	return true, nil
}

func (store *SchemaStore) planLegacyIngestBatch(
	ctx context.Context,
	transaction *Transaction,
	receiptKey []byte,
	imports []LegacyPackImport,
	contentHash schema.ID,
	hints legacyImportBatchHints,
) (legacyImportBatchPlan, Limits, error) {
	packIDs, blobIDs := uniqueLegacyImportIDs(imports)
	if hints.filterUsed {
		definitelyAbsent := uint64(len(hints.packsAbsent) + len(hints.blobsAbsent))
		store.legacyMetrics.definitelyAbsent.Add(definitelyAbsent)
		store.legacyMetrics.possiblyPresent.Add(uint64(len(packIDs)+len(blobIDs)) - definitelyAbsent)
	}
	packOriginal, err := store.loadLegacyPackRecords(ctx, transaction, packIDs, hints.packsAbsent, hints.filterUsed)
	if err != nil {
		return legacyImportBatchPlan{}, Limits{}, err
	}
	blobCurrent, err := store.loadLegacyBlobRecords(ctx, transaction, blobIDs, hints.blobsAbsent, hints.filterUsed)
	if err != nil {
		return legacyImportBatchPlan{}, Limits{}, err
	}
	state := newLegacyImportBatchState(packOriginal, blobCurrent, len(packIDs)+len(blobIDs), len(imports))
	for index, imported := range imports {
		if err := state.applyImport(ctx, transaction, store, imported); err != nil {
			return legacyImportBatchPlan{}, Limits{}, &LegacyImportInputError{Index: index, Err: err}
		}
	}
	for _, blobID := range blobIDs {
		value, err := state.blobCurrent[blobID].MarshalBinary()
		if err != nil {
			return legacyImportBatchPlan{}, Limits{}, err
		}
		state.mutations[string(schema.BlobKey(blobID))] = Mutation{Key: schema.BlobKey(blobID), Value: value}
	}
	for key, value := range state.debtValues {
		if state.debtDirty[key] {
			state.mutations[key] = Mutation{Key: []byte(key), Value: value}
		}
	}
	receipt, err := buildLegacyImportReceipt(contentHash, imports, packIDs, packOriginal, state)
	if err != nil {
		return legacyImportBatchPlan{}, Limits{}, err
	}
	receiptValue, err := receipt.MarshalBinary()
	if err != nil {
		return legacyImportBatchPlan{}, Limits{}, err
	}
	state.mutations[string(receiptKey)] = Mutation{Key: receiptKey, Value: receiptValue}
	puts := make([]Mutation, 0, len(state.mutations))
	var encodedBytes uint64
	for _, mutation := range state.mutations {
		puts = append(puts, mutation)
		encodedBytes += uint64(len(mutation.Key)+len(mutation.Value)) + 32
	}
	transactionBytes := uint64(maxLegacyImportTransactionBytes)
	if len(imports) > 0 && imports[0].TransactionBytes > 0 {
		transactionBytes = imports[0].TransactionBytes
	}
	if len(imports) > 1 && (len(puts) > LegacyImportTransactionMutationLimit || encodedBytes > transactionBytes) {
		return legacyImportBatchPlan{}, Limits{}, fmt.Errorf(
			"%w: mutations=%d bytes=%d",
			ErrLegacyImportBatchTooLarge,
			len(puts),
			encodedBytes,
		)
	}
	sort.Slice(puts, func(left, right int) bool { return bytes.Compare(puts[left].Key, puts[right].Key) < 0 })
	if err := validateLegacyImportMutations(puts); err != nil {
		return legacyImportBatchPlan{}, Limits{}, err
	}
	limits := store.client.Limits()
	for _, imported := range imports {
		if imported.BatchSize > 0 && imported.BatchSize < limits.MaxBatchItems {
			limits.MaxBatchItems = imported.BatchSize
		}
	}
	return legacyImportBatchPlan{puts: puts, encodedBytes: encodedBytes}, limits, nil
}

func buildLegacyImportReceipt(
	contentHash schema.ID,
	imports []LegacyPackImport,
	packIDs []schema.ID,
	packOriginal map[schema.ID]*schema.PackRecord,
	state *legacyImportBatchState,
) (schema.LegacyImportReceiptRecord, error) {
	if len(imports) == 0 {
		return schema.LegacyImportReceiptRecord{}, fmt.Errorf("legacy import receipt requires at least one import")
	}
	changes := make([]schema.LegacyImportPackChange, 0, len(packIDs))
	for _, packID := range packIDs {
		current := state.packCurrent[packID]
		if current == nil {
			return schema.LegacyImportReceiptRecord{}, fmt.Errorf("legacy import receipt missing current pack state")
		}
		currentValue, err := current.MarshalBinary()
		if err != nil {
			return schema.LegacyImportReceiptRecord{}, err
		}
		var oldValue []byte
		if old := packOriginal[packID]; old != nil {
			oldValue, err = old.MarshalBinary()
			if err != nil {
				return schema.LegacyImportReceiptRecord{}, err
			}
		}
		changes = append(changes, schema.LegacyImportPackChange{PackID: packID, Old: oldValue, Current: currentValue})
	}
	events := make([]schema.LegacyImportEvent, 0, len(state.events))
	for _, event := range state.events {
		value, err := event.Record.MarshalBinary()
		if err != nil {
			continue
		}
		events = append(events, schema.LegacyImportEvent{PackID: event.PackID, Value: value})
	}
	packsImported, blobsImported, errorsSeen := legacyImportReceiptCounters(imports)
	return schema.LegacyImportReceiptRecord{
		ContentHash:   contentHash,
		SourceIndex:   imports[len(imports)-1].SourceIndex,
		PacksImported: packsImported,
		BlobsImported: blobsImported,
		ErrorsSeen:    errorsSeen,
		Changes:       changes,
		Events:        events,
	}, nil
}

func legacyImportReceiptCounters(imports []LegacyPackImport) (uint64, uint64, uint64) {
	packsImported := uint64(len(imports))
	var blobsImported uint64
	var errorsSeen uint64
	for _, imported := range imports {
		blobsImported += imported.Record.BlobCount
		if imported.Debt != nil {
			errorsSeen++
		}
	}
	return packsImported, blobsImported, errorsSeen
}

//nolint:gocognit // The hash deliberately frames every persisted import field in schema order.
func legacyImportContentHash(session schema.ID, batch uint64, imports []LegacyPackImport) (schema.ID, error) {
	hasher := sha256.New()
	if _, err := hasher.Write(session[:]); err != nil {
		return schema.ID{}, err
	}
	var batchValue [8]byte
	binary.BigEndian.PutUint64(batchValue[:], batch)
	if _, err := hasher.Write(batchValue[:]); err != nil {
		return schema.ID{}, err
	}
	for _, imported := range imports {
		if _, err := hasher.Write(imported.SourceIndex[:]); err != nil {
			return schema.ID{}, err
		}
		if _, err := hasher.Write(imported.PackID[:]); err != nil {
			return schema.ID{}, err
		}
		recordValue, err := imported.Record.MarshalBinary()
		if err != nil {
			return schema.ID{}, err
		}
		if err := hashFramedBytes(hasher, recordValue); err != nil {
			return schema.ID{}, err
		}
		blobIDs := sortedBlobIDs(imported.Blobs)
		if err := hashFramedU64(hasher, uint64(len(blobIDs))); err != nil {
			return schema.ID{}, err
		}
		for _, blobID := range blobIDs {
			if _, err := hasher.Write(blobID[:]); err != nil {
				return schema.ID{}, err
			}
			blobValue, err := imported.Blobs[blobID].MarshalBinary()
			if err != nil {
				return schema.ID{}, err
			}
			if err := hashFramedBytes(hasher, blobValue); err != nil {
				return schema.ID{}, err
			}
		}
		if err := hashFramedBytes(hasher, imported.DebtKey); err != nil {
			return schema.ID{}, err
		}
		if imported.Debt == nil {
			if err := hashFramedBytes(hasher, nil); err != nil {
				return schema.ID{}, err
			}
		} else {
			debtValue, err := imported.Debt.MarshalBinary()
			if err != nil {
				return schema.ID{}, err
			}
			if err := hashFramedBytes(hasher, debtValue); err != nil {
				return schema.ID{}, err
			}
		}
		placementBackends := make([]uint64, 0, len(imported.Placements))
		for backend := range imported.Placements {
			placementBackends = append(placementBackends, backend)
		}
		sort.Slice(placementBackends, func(left, right int) bool { return placementBackends[left] < placementBackends[right] })
		if err := hashFramedU64(hasher, uint64(len(placementBackends))); err != nil {
			return schema.ID{}, err
		}
		for _, backend := range placementBackends {
			if err := hashFramedU64(hasher, backend); err != nil {
				return schema.ID{}, err
			}
			placementValue, err := imported.Placements[backend].MarshalBinary()
			if err != nil {
				return schema.ID{}, err
			}
			if err := hashFramedBytes(hasher, placementValue); err != nil {
				return schema.ID{}, err
			}
		}
		if err := hashFramedU64(hasher, uint64(len(imported.PredecessorPackIDs))); err != nil {
			return schema.ID{}, err
		}
		for _, predecessor := range imported.PredecessorPackIDs {
			if _, err := hasher.Write(predecessor[:]); err != nil {
				return schema.ID{}, err
			}
		}
		if err := hashFramedU64(hasher, uint64(imported.LineageKind)); err != nil {
			return schema.ID{}, err
		}
		if _, err := hasher.Write(imported.RunID[:]); err != nil {
			return schema.ID{}, err
		}
	}
	var contentHash schema.ID
	copy(contentHash[:], hasher.Sum(nil))
	return contentHash, nil
}

func hashFramedBytes(hasher interface{ Write([]byte) (int, error) }, value []byte) error {
	if err := hashFramedU64(hasher, uint64(len(value))); err != nil {
		return err
	}
	if len(value) == 0 {
		return nil
	}
	_, err := hasher.Write(value)
	return err
}

func hashFramedU64(hasher interface{ Write([]byte) (int, error) }, value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, err := hasher.Write(encoded[:])
	return err
}

type legacyImportReceiptStatus byte

const (
	receiptMissing legacyImportReceiptStatus = iota + 1
	receiptMatched
	receiptConflict
)

func (store *SchemaStore) lookupLegacyImportReceipt(
	ctx context.Context,
	receiptKey []byte,
	contentHash schema.ID,
) (legacyImportReceiptStatus, error) {
	value, found, err := store.Get(ctx, receiptKey)
	if err != nil {
		return receiptMissing, err
	}
	if !found {
		return receiptMissing, nil
	}
	record, err := schema.UnmarshalLegacyImportReceiptRecord(value)
	if err != nil {
		return receiptMissing, err
	}
	if record.ContentHash != contentHash {
		return receiptConflict, nil
	}
	return receiptMatched, nil
}

func (store *SchemaStore) ReduceLegacyImportBatch(
	ctx context.Context,
	session schema.ID,
	batch uint64,
	finalCheckpoint *Mutation,
) error {
	if session == (schema.ID{}) {
		return fmt.Errorf("legacy import session is required")
	}
	if !store.allowLegacySplitSession(session, false) {
		return fmt.Errorf("%w: rerun with --force-reset-old-idx to enable fresh import split sessions", ErrLegacyImportFreshRequired)
	}
	if err := validateLegacyImportCheckpoint(finalCheckpoint); err != nil {
		return err
	}
	started := time.Now()
	defer func() { store.legacyMetrics.totalNanos.Add(uint64(time.Since(started))) }()
	store.legacyMetrics.batches.Add(1)
	deferDurability := store.freshImportSeen != nil
	backoff := 100 * time.Microsecond
	receiptKey := schema.LegacyImportReceiptKey(session, batch)
	for range revisionAllocationAttempts {
		reduceStarted := time.Now()
		committed, err := store.reduceLegacyImportBatchOnce(ctx, receiptKey, finalCheckpoint, deferDurability)
		store.legacyMetrics.reductionNanos.Add(uint64(time.Since(reduceStarted)))
		if status.Code(err) == codes.Aborted {
			store.legacyMetrics.conflicts.Add(1)
			store.legacyMetrics.retries.Add(1)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			backoff = min(backoff*2, 25*time.Millisecond)
			continue
		}
		if err != nil {
			state, lookupErr := store.lookupReducedLegacyImportReceipt(ctx, receiptKey)
			if lookupErr == nil && state {
				if finalCheckpoint == nil {
					return nil
				}
				return store.ensureCheckpointMatches(ctx, *finalCheckpoint)
			}
			return err
		}
		if !committed {
			return nil
		}
		store.legacyMetrics.commits.Add(1)
		store.legacyMetrics.reducedBatches.Add(1)
		return nil
	}
	return fmt.Errorf("reduce legacy import batch: %w", errors.New("transaction conflict retry limit exceeded"))
}

//nolint:gocognit,nestif // Recovery-state validation and atomic reduction share one transaction boundary.
func (store *SchemaStore) reduceLegacyImportBatchOnce(
	ctx context.Context,
	receiptKey []byte,
	finalCheckpoint *Mutation,
	deferDurability bool,
) (bool, error) {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return false, err
	}
	fail := func(err error) (bool, error) {
		rollbackTransaction(ctx, transaction)
		return false, err
	}
	value, found, err := transaction.Get(ctx, receiptKey)
	if err != nil {
		return fail(err)
	}
	if !found {
		if finalCheckpoint != nil {
			applied, err := checkpointMatchesInTransaction(ctx, transaction, *finalCheckpoint)
			if err != nil {
				return fail(err)
			}
			if applied {
				if err := transaction.Rollback(ctx); err != nil {
					return false, err
				}
				return false, nil
			}
		}
		return fail(fmt.Errorf("%w", ErrLegacyImportReceiptMissing))
	}
	receipt, err := schema.UnmarshalLegacyImportReceiptRecord(value)
	if err != nil {
		return fail(err)
	}
	if finalCheckpoint != nil {
		if err := validateCheckpointForLegacyReceipt(*finalCheckpoint, receipt); err != nil {
			return fail(err)
		}
	}
	if receipt.Reduced {
		if finalCheckpoint != nil {
			applied, err := checkpointMatchesInTransaction(ctx, transaction, *finalCheckpoint)
			if err != nil {
				return fail(err)
			}
			if !applied {
				return fail(fmt.Errorf("legacy checkpoint for reduced batch is missing"))
			}
		}
		if err := transaction.Rollback(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	changes, err := receiptPackChanges(receipt)
	if err != nil {
		return fail(err)
	}
	mutations := make([]Mutation, 0, len(changes)+len(receipt.Events)+2+len(aggregateKeys()))
	if len(changes) > 0 {
		store.legacyMetrics.planningReads.Add(uint64(len(aggregateKeys())))
		aggregates, err := applyPackAggregateDeltas(ctx, transaction, changes, true)
		if err != nil {
			return fail(err)
		}
		mutations = append(mutations, aggregates...)
	}
	events, err := receiptPackEvents(receipt)
	if err != nil {
		return fail(err)
	}
	if len(events) > 0 {
		store.legacyMetrics.planningReads.Add(2)
	}
	history, err := packHistoryMutations(ctx, transaction, events)
	if err != nil {
		return fail(err)
	}
	mutations = append(mutations, history...)
	if finalCheckpoint != nil {
		mutations = append(mutations, *finalCheckpoint)
	}
	receipt.Reduced = true
	encodedReceipt, err := receipt.MarshalBinary()
	if err != nil {
		return fail(err)
	}
	mutations = append(mutations, Mutation{Key: receiptKey, Value: encodedReceipt})
	sort.Slice(mutations, func(left, right int) bool { return bytes.Compare(mutations[left].Key, mutations[right].Key) < 0 })
	if err := validateLegacyImportMutations(mutations); err != nil {
		return fail(err)
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), mutations, nil); err != nil {
		return fail(err)
	}
	commit := transaction.Commit
	if deferDurability {
		commit = transaction.CommitDeferred
	}
	if err := commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return false, err
	}
	return true, nil
}

func validateCheckpointForLegacyReceipt(checkpoint Mutation, receipt schema.LegacyImportReceiptRecord) error {
	parsed, err := schema.ParseKey(checkpoint.Key)
	if err != nil || parsed.Kind != schema.KeyImportCheckpoint {
		return fmt.Errorf("legacy import checkpoint has an invalid key")
	}
	if parsed.ID != receipt.SourceIndex {
		return fmt.Errorf("legacy import checkpoint source index mismatch")
	}
	record, err := schema.UnmarshalImportCheckpointRecord(checkpoint.Value)
	if err != nil {
		return fmt.Errorf("legacy import checkpoint has an invalid value: %w", err)
	}
	if record.PacksImported < receipt.PacksImported ||
		record.BlobsImported < receipt.BlobsImported ||
		record.ErrorsSeen < receipt.ErrorsSeen {
		return fmt.Errorf("legacy import checkpoint counters mismatch")
	}
	return nil
}

func receiptPackChanges(receipt schema.LegacyImportReceiptRecord) ([]packChange, error) {
	changes := make([]packChange, 0, len(receipt.Changes))
	for _, change := range receipt.Changes {
		current, err := schema.UnmarshalPackRecord(change.Current)
		if err != nil {
			return nil, err
		}
		var old *schema.PackRecord
		if len(change.Old) > 0 {
			decoded, err := schema.UnmarshalPackRecord(change.Old)
			if err != nil {
				return nil, err
			}
			old = &decoded
		}
		changes = append(changes, packChange{packID: change.PackID, old: old, current: current})
	}
	return changes, nil
}

func receiptPackEvents(receipt schema.LegacyImportReceiptRecord) ([]PackEvent, error) {
	events := make([]PackEvent, 0, len(receipt.Events))
	for _, event := range receipt.Events {
		record, err := schema.UnmarshalPackHistoryEvent(event.Value)
		if err != nil {
			return nil, err
		}
		events = append(events, PackEvent{PackID: event.PackID, Record: record})
	}
	return events, nil
}

func checkpointMatchesInTransaction(ctx context.Context, transaction *Transaction, checkpoint Mutation) (bool, error) {
	value, found, err := transaction.Get(ctx, checkpoint.Key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if !bytes.Equal(value, checkpoint.Value) {
		return false, fmt.Errorf("legacy import checkpoint conflicts with existing value")
	}
	return true, nil
}

func (store *SchemaStore) ensureCheckpointMatches(ctx context.Context, checkpoint Mutation) error {
	value, found, err := store.Get(ctx, checkpoint.Key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("legacy import checkpoint is missing")
	}
	if !bytes.Equal(value, checkpoint.Value) {
		return fmt.Errorf("legacy import checkpoint conflicts with existing value")
	}
	return nil
}

func (store *SchemaStore) lookupReducedLegacyImportReceipt(ctx context.Context, receiptKey []byte) (bool, error) {
	value, found, err := store.Get(ctx, receiptKey)
	if err != nil || !found {
		return false, err
	}
	receipt, err := schema.UnmarshalLegacyImportReceiptRecord(value)
	if err != nil {
		return false, err
	}
	return receipt.Reduced, nil
}

func (store *SchemaStore) CompleteLegacyImportSession(ctx context.Context, session schema.ID) error {
	if session == (schema.ID{}) {
		return fmt.Errorf("legacy import session is required")
	}
	if !store.allowLegacySplitSession(session, false) {
		return fmt.Errorf("%w: rerun with --force-reset-old-idx to enable fresh import split sessions", ErrLegacyImportFreshRequired)
	}
	prefix := schema.LegacyImportReceiptPrefix(session)
	if err := store.ensureLegacyImportSessionReduced(ctx, prefix); err != nil {
		return err
	}
	if err := store.deleteLegacyImportReceiptsBounded(ctx, prefix); err != nil {
		return err
	}
	store.clearLegacySplitSession(session)
	return nil
}

func (store *SchemaStore) allowLegacySplitSession(session schema.ID, mark bool) bool {
	store.legacySplitMu.Lock()
	defer store.legacySplitMu.Unlock()
	if store.freshImportSeen != nil {
		if mark {
			store.legacySplitAuth[session] = struct{}{}
		}
		return true
	}
	_, ok := store.legacySplitAuth[session]
	return ok
}

func (store *SchemaStore) clearLegacySplitSession(session schema.ID) {
	store.legacySplitMu.Lock()
	delete(store.legacySplitAuth, session)
	store.legacySplitMu.Unlock()
}

func (store *SchemaStore) ensureLegacyImportSessionReduced(ctx context.Context, prefix []byte) error {
	var cursor []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, cursor, legacyImportCleanupScanPageSize)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			receipt, err := schema.UnmarshalLegacyImportReceiptRecord(entry.Value)
			if err != nil {
				return err
			}
			if !receipt.Reduced {
				return fmt.Errorf("%w", ErrLegacyImportReceiptUnresolved)
			}
			cursor = entry.Key
		}
		if done {
			return nil
		}
	}
}

func (store *SchemaStore) deleteLegacyImportReceiptsBounded(ctx context.Context, prefix []byte) error {
	var cursor []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, cursor, legacyImportCleanupDeletePageSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			if done {
				return nil
			}
			continue
		}
		deletes := make([][]byte, 0, len(entries))
		for _, entry := range entries {
			deletes = append(deletes, append([]byte(nil), entry.Key...))
		}
		if err := store.deleteLegacyImportReceiptChunkWithRetry(ctx, deletes); err != nil {
			return err
		}
		cursor = entries[len(entries)-1].Key
		if done {
			return nil
		}
	}
}

func (store *SchemaStore) deleteLegacyImportReceiptChunkWithRetry(ctx context.Context, deletes [][]byte) error {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		transaction, err := store.client.Begin(ctx)
		if err != nil {
			return err
		}
		if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), nil, deletes); err != nil {
			rollbackTransaction(ctx, transaction)
			if status.Code(err) != codes.Aborted {
				return err
			}
		} else if err := transaction.Commit(ctx); err != nil {
			rollbackTransaction(ctx, transaction)
			if status.Code(err) != codes.Aborted {
				return err
			}
		} else {
			return nil
		}
		store.legacyMetrics.conflicts.Add(1)
		store.legacyMetrics.retries.Add(1)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("cleanup legacy import receipts: %w", errors.New("transaction conflict retry limit exceeded"))
}
