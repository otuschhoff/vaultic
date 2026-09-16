package daemon

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const (
	LegacyImportTransactionMutationLimit = 8_000
	maxLegacyImportTransactionBytes      = 8 << 20
)

var ErrLegacyImportBatchTooLarge = fmt.Errorf("legacy import transaction exceeds configured bounds")

type LegacyImportInputError struct {
	Index int
	Err   error
}

func (err *LegacyImportInputError) Error() string {
	return fmt.Sprintf("legacy import input %d: %v", err.Index, err.Err)
}

func (err *LegacyImportInputError) Unwrap() error { return err.Err }

type legacyImportBatchHints struct {
	packsAbsent map[schema.ID]struct{}
	blobsAbsent map[schema.ID]struct{}
	filterUsed  bool
}

type legacyImportBatchPlan struct {
	puts         []Mutation
	encodedBytes uint64
}

type legacyImportBatchState struct {
	packCurrent    map[schema.ID]*schema.PackRecord
	blobCurrent    map[schema.ID]schema.BlobRecord
	mutations      map[string]Mutation
	events         []PackEvent
	debtValues     map[string][]byte
	debtLoaded     map[string]bool
	debtExists     map[string]bool
	debtDirty      map[string]bool
	batchDebtInput map[string][]byte
}

func (store *SchemaStore) freshImportLookupHintsForBatch(imports []LegacyPackImport) legacyImportBatchHints {
	hints := legacyImportBatchHints{}
	if store.freshImportSeen == nil {
		return hints
	}
	hints.filterUsed = true
	hints.packsAbsent = make(map[schema.ID]struct{}, len(imports))
	hints.blobsAbsent = make(map[schema.ID]struct{})
	for _, imported := range imports {
		if !store.freshImportSeen.possiblyContains(imported.PackID) {
			hints.packsAbsent[imported.PackID] = struct{}{}
		}
		for blobID := range imported.Blobs {
			if !store.freshImportSeen.possiblyContains(blobID) {
				hints.blobsAbsent[blobID] = struct{}{}
			}
		}
	}
	return hints
}

func validateLegacyImportCheckpoint(checkpoint *Mutation) error {
	if checkpoint == nil {
		return nil
	}
	parsed, err := schema.ParseKey(checkpoint.Key)
	if err != nil || parsed.Kind != schema.KeyImportCheckpoint {
		return fmt.Errorf("legacy import checkpoint has an invalid key")
	}
	if _, err := schema.UnmarshalImportCheckpointRecord(checkpoint.Value); err != nil {
		return fmt.Errorf("legacy import checkpoint has an invalid value: %w", err)
	}
	return nil
}

func (store *SchemaStore) importLegacyPacksOnce(
	ctx context.Context,
	imports []LegacyPackImport,
	checkpoint *Mutation,
	hints legacyImportBatchHints,
	deferDurability bool,
	replanned bool,
) error {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		rollbackTransaction(ctx, transaction)
		return err
	}
	planningStarted := time.Now()
	plan, limits, err := store.planLegacyImportBatch(ctx, transaction, imports, checkpoint, hints)
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
	store.legacyMetrics.mutationRPCAttempts.Add(mutationRPCs.attempted)
	store.legacyMetrics.mutationRPCs.Add(mutationRPCs.succeeded)
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
		return err
	}
	store.legacyMetrics.commitNanos.Add(uint64(time.Since(commitStarted)))
	store.legacyMetrics.mutationsCommitted.Add(uint64(len(plan.puts)))
	store.legacyMetrics.encodedBytesCommitted.Add(plan.encodedBytes)
	return nil
}

func (store *SchemaStore) planLegacyImportBatch(
	ctx context.Context,
	transaction *Transaction,
	imports []LegacyPackImport,
	checkpoint *Mutation,
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
	return store.finalizeLegacyImportBatchPlan(ctx, transaction, imports, checkpoint, packIDs, blobIDs, packOriginal, state)
}

func (store *SchemaStore) finalizeLegacyImportBatchPlan(
	ctx context.Context,
	transaction *Transaction,
	imports []LegacyPackImport,
	checkpoint *Mutation,
	packIDs, blobIDs []schema.ID,
	packOriginal map[schema.ID]*schema.PackRecord,
	state *legacyImportBatchState,
) (legacyImportBatchPlan, Limits, error) {
	for _, blobID := range blobIDs {
		value, err := state.blobCurrent[blobID].MarshalBinary()
		if err != nil {
			return legacyImportBatchPlan{}, Limits{}, err
		}
		state.mutations[string(schema.BlobKey(blobID))] = Mutation{Key: schema.BlobKey(blobID), Value: value}
	}
	changes := make([]packChange, 0, len(packIDs))
	for _, packID := range packIDs {
		changes = append(changes, packChange{packID: packID, old: packOriginal[packID], current: *state.packCurrent[packID]})
	}
	if len(changes) > 0 {
		store.legacyMetrics.planningReads.Add(uint64(len(aggregateKeys())))
		aggregates, err := applyPackAggregateDeltas(ctx, transaction, changes, true)
		if err != nil {
			return legacyImportBatchPlan{}, Limits{}, err
		}
		for _, mutation := range aggregates {
			state.mutations[string(mutation.Key)] = mutation
		}
	}
	if len(state.events) > 0 {
		store.legacyMetrics.planningReads.Add(2)
	}
	history, err := packHistoryMutations(ctx, transaction, state.events)
	if err != nil {
		return legacyImportBatchPlan{}, Limits{}, err
	}
	for _, mutation := range history {
		state.mutations[string(mutation.Key)] = mutation
	}
	for key, value := range state.debtValues {
		if state.debtDirty[key] {
			state.mutations[key] = Mutation{Key: []byte(key), Value: value}
		}
	}
	if checkpoint != nil {
		if err := addCompatibleLegacyMutation(state.mutations, *checkpoint); err != nil {
			return legacyImportBatchPlan{}, Limits{}, err
		}
	}
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
			"%w: mutations=%d bytes=%d", ErrLegacyImportBatchTooLarge, len(puts), encodedBytes,
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

func newLegacyImportBatchState(
	packOriginal map[schema.ID]*schema.PackRecord,
	blobCurrent map[schema.ID]schema.BlobRecord,
	keyCount int,
	importCount int,
) *legacyImportBatchState {
	return &legacyImportBatchState{
		packCurrent: clonePackRecordMap(packOriginal), blobCurrent: blobCurrent,
		mutations: make(map[string]Mutation, keyCount+len(aggregateKeys())),
		events:    make([]PackEvent, 0, importCount*2), debtValues: make(map[string][]byte),
		debtLoaded: make(map[string]bool), debtExists: make(map[string]bool), debtDirty: make(map[string]bool),
		batchDebtInput: make(map[string][]byte),
	}
}

func (state *legacyImportBatchState) applyImport(
	ctx context.Context,
	transaction *Transaction,
	store *SchemaStore,
	imported LegacyPackImport,
) error {
	oldRecord := state.packCurrent[imported.PackID]
	if oldRecord == nil {
		imported.Record.SourceIndexIDs = appendUniqueID(imported.Record.SourceIndexIDs, imported.SourceIndex)
	} else {
		imported.Record = mergeImportedPackRecord(*oldRecord, imported.Record, imported.SourceIndex, true)
	}
	sort.Slice(imported.Record.SourceIndexIDs, func(left, right int) bool {
		return bytes.Compare(imported.Record.SourceIndexIDs[left][:], imported.Record.SourceIndexIDs[right][:]) < 0
	})
	plan := packImportPlan{}
	for _, blobID := range sortedBlobIDs(imported.Blobs) {
		incoming := canonicalBlobRecord(imported.Blobs[blobID])
		existing, found := state.blobCurrent[blobID]
		if found {
			if err := accumulateNewLocations(&plan, existing, incoming); err != nil {
				return err
			}
			incoming = mergeBlobRecords(existing, incoming)
		} else if err := accumulateAllLocations(&plan, incoming); err != nil {
			return err
		}
		state.blobCurrent[blobID] = incoming
	}
	if err := planPackRecord(&imported, oldRecord, &plan); err != nil {
		return err
	}
	packRecord := imported.Record
	state.packCurrent[imported.PackID] = &packRecord
	state.events = append(state.events, packImportEvents(imported, oldRecord, true)...)
	for _, mutation := range plan.puts {
		if bytes.Equal(mutation.Key, schema.PackKey(imported.PackID)) {
			state.mutations[string(mutation.Key)] = mutation
			continue
		}
		if err := addCompatibleLegacyMutation(state.mutations, mutation); err != nil {
			return err
		}
	}
	return planLegacyBatchDebt(
		ctx, transaction, imported, state.debtValues, state.debtLoaded, state.debtExists,
		state.debtDirty, state.batchDebtInput, &store.legacyMetrics,
	)
}

func validateLegacyImportMutations(puts []Mutation) error {
	if err := validateDistinctMutations(puts, nil); err != nil {
		return err
	}
	for _, mutation := range puts {
		if err := schema.ValidateValue(mutation.Key, mutation.Value); err != nil {
			return err
		}
	}
	return nil
}

func uniqueLegacyImportIDs(imports []LegacyPackImport) ([]schema.ID, []schema.ID) {
	packs := make(map[schema.ID]struct{}, len(imports))
	blobs := make(map[schema.ID]struct{})
	for _, imported := range imports {
		packs[imported.PackID] = struct{}{}
		for blobID := range imported.Blobs {
			blobs[blobID] = struct{}{}
		}
	}
	packIDs := sortedIDSet(packs)
	blobIDs := sortedIDSet(blobs)
	return packIDs, blobIDs
}

func sortedIDSet(values map[schema.ID]struct{}) []schema.ID {
	result := make([]schema.ID, 0, len(values))
	for id := range values {
		result = append(result, id)
	}
	sort.Slice(result, func(left, right int) bool { return bytes.Compare(result[left][:], result[right][:]) < 0 })
	return result
}

func sortedBlobIDs(values map[schema.ID]schema.BlobRecord) []schema.ID {
	ids := make([]schema.ID, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return bytes.Compare(ids[left][:], ids[right][:]) < 0 })
	return ids
}

func (store *SchemaStore) loadLegacyPackRecords(
	ctx context.Context,
	transaction *Transaction,
	ids []schema.ID,
	absent map[schema.ID]struct{},
	countFilterHits bool,
) (map[schema.ID]*schema.PackRecord, error) {
	result := make(map[schema.ID]*schema.PackRecord, len(ids))
	keys, selected := legacyLookupKeys(ids, absent, schema.PackKey)
	store.legacyMetrics.planningReads.Add(uint64(len(keys)))
	values, found, readRPCs, readKeys, err := legacyMultiGet(ctx, transaction, keys)
	store.legacyMetrics.catalogReadRPCs.Add(readRPCs)
	store.legacyMetrics.catalogReadKeys.Add(readKeys)
	if err != nil {
		return nil, err
	}
	var foundCount uint64
	for offset, id := range selected {
		if !found[offset] {
			continue
		}
		foundCount++
		record, err := schema.UnmarshalPackRecord(values[offset].Value)
		if err != nil {
			return nil, err
		}
		result[id] = &record
	}
	if countFilterHits {
		store.legacyMetrics.found.Add(foundCount)
	}
	return result, nil
}

func (store *SchemaStore) loadLegacyBlobRecords(
	ctx context.Context,
	transaction *Transaction,
	ids []schema.ID,
	absent map[schema.ID]struct{},
	countFilterHits bool,
) (map[schema.ID]schema.BlobRecord, error) {
	result := make(map[schema.ID]schema.BlobRecord, len(ids))
	keys, selected := legacyLookupKeys(ids, absent, schema.BlobKey)
	store.legacyMetrics.planningReads.Add(uint64(len(keys)))
	values, found, readRPCs, readKeys, err := legacyMultiGet(ctx, transaction, keys)
	store.legacyMetrics.catalogReadRPCs.Add(readRPCs)
	store.legacyMetrics.catalogReadKeys.Add(readKeys)
	if err != nil {
		return nil, err
	}
	var foundCount uint64
	for offset, id := range selected {
		if !found[offset] {
			continue
		}
		foundCount++
		record, err := schema.UnmarshalBlobRecord(values[offset].Value)
		if err != nil {
			return nil, err
		}
		result[id] = record
	}
	if countFilterHits {
		store.legacyMetrics.found.Add(foundCount)
	}
	return result, nil
}

func legacyLookupKeys(
	ids []schema.ID,
	absent map[schema.ID]struct{},
	key func(schema.ID) []byte,
) ([][]byte, []schema.ID) {
	keys := make([][]byte, 0, len(ids))
	selected := make([]schema.ID, 0, len(ids))
	for _, id := range ids {
		if _, knownAbsent := absent[id]; knownAbsent {
			continue
		}
		keys = append(keys, key(id))
		selected = append(selected, id)
	}
	return keys, selected
}

func legacyMultiGet(ctx context.Context, transaction *Transaction, keys [][]byte) ([]KeyValue, []bool, uint64, uint64, error) {
	values := make([]KeyValue, len(keys))
	found := make([]bool, len(keys))
	var readRPCs, readKeys uint64
	for start := 0; start < len(keys); {
		end, err := blobLookupBatchEnd(transaction.client.Limits(), keys, start)
		if err != nil {
			return nil, nil, readRPCs, readKeys, err
		}
		readRPCs++
		readKeys += uint64(end - start)
		batchValues, batchFound, err := transaction.MultiGet(ctx, keys[start:end])
		if err != nil {
			return nil, nil, readRPCs, readKeys, err
		}
		copy(values[start:end], batchValues)
		copy(found[start:end], batchFound)
		start = end
	}
	return values, found, readRPCs, readKeys, nil
}

func clonePackRecordMap(records map[schema.ID]*schema.PackRecord) map[schema.ID]*schema.PackRecord {
	result := make(map[schema.ID]*schema.PackRecord, len(records))
	for id, record := range records {
		copyRecord := *record
		copyRecord.SourceIndexIDs = append([]schema.ID(nil), record.SourceIndexIDs...)
		result[id] = &copyRecord
	}
	return result
}

func addCompatibleLegacyMutation(mutations map[string]Mutation, mutation Mutation) error {
	key := string(mutation.Key)
	if existing, found := mutations[key]; found && !bytes.Equal(existing.Value, mutation.Value) {
		return fmt.Errorf("legacy import batch contains conflicting mutation for %q", mutation.Key)
	}
	mutations[key] = mutation
	return nil
}

func planLegacyBatchDebt(
	ctx context.Context,
	transaction *Transaction,
	imported LegacyPackImport,
	values map[string][]byte,
	loaded map[string]bool,
	exists map[string]bool,
	dirty map[string]bool,
	batchInputs map[string][]byte,
	metrics *legacyImportMetrics,
) error {
	key := imported.DebtKey
	if imported.Debt == nil {
		key = schema.CrawlDebtKey(schema.ID{}, imported.PackID)
	}
	keyString := string(key)
	if imported.Debt == nil && !loaded[keyString] {
		metrics.planningReads.Add(1)
		metrics.catalogReadRPCs.Add(1)
		metrics.catalogReadKeys.Add(1)
		value, valueExists, err := transaction.Get(ctx, key)
		if err != nil {
			return err
		}
		values[keyString], exists[keyString], loaded[keyString] = value, valueExists, true
	}
	if imported.Debt != nil {
		value, err := imported.Debt.MarshalBinary()
		if err != nil {
			return err
		}
		if err := schema.ValidateValue(imported.DebtKey, value); err != nil {
			return err
		}
		if existing, duplicate := batchInputs[keyString]; duplicate && !bytes.Equal(existing, value) {
			return fmt.Errorf("legacy import batch contains conflicting debt for %q", key)
		}
		batchInputs[keyString] = value
		values[keyString], exists[keyString], loaded[keyString], dirty[keyString] = value, true, true, true
		return nil
	}
	if !exists[keyString] {
		return nil
	}
	debt, err := schema.UnmarshalCrawlDebtRecord(values[keyString])
	if err != nil {
		return err
	}
	if debt.Reason != schema.DebtUnavailablePack || debt.Status == schema.DebtResolved {
		return nil
	}
	debt.Status = schema.DebtResolved
	debt.ErrorClass = ""
	debt.LastAttemptUnixNano = time.Now().UnixNano()
	values[keyString], err = debt.MarshalBinary()
	dirty[keyString] = err == nil
	return err
}
