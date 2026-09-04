package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type reconciledRevisionInput struct {
	reconciled     ReconciledRevision
	currentValue   []byte
	currentParsed  schema.ParsedKey
	revisionParsed schema.ParsedKey
}

type reconciledRevisionState struct {
	revisionFound bool
	currentFound  bool
	oldContent    []schema.ID
	oldManifestID schema.ID
	noop          bool
}

type reconciledRevisionPlan struct {
	puts       map[string]Mutation
	manifestID schema.ID
	segments   []schema.ContentManifest
	oldUnique  []schema.ID
	newUnique  []schema.ID
}

func (store *SchemaStore) publishReconciledRevisionOnce(ctx context.Context, reconciled ReconciledRevision) error {
	input, err := prepareReconciledRevision(reconciled)
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
	plan, noop, err := store.planReconciledRevision(ctx, transaction, input)
	if err != nil {
		return fail(err)
	}
	if noop {
		return transaction.Rollback(ctx)
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), sortedReconciledMutations(plan), nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

func (store *SchemaStore) planReconciledRevision(ctx context.Context, transaction *Transaction, input reconciledRevisionInput) (reconciledRevisionPlan, bool, error) {
	state, err := loadReconciledRevisionState(ctx, transaction, input)
	if err != nil || state.noop {
		return reconciledRevisionPlan{}, state.noop, err
	}
	plan, err := newReconciledRevisionPlan(ctx, transaction, input, state)
	if err != nil {
		return reconciledRevisionPlan{}, false, err
	}
	phases := []func() error{
		func() error { return planHistoricalReferences(ctx, transaction, input, state, plan) },
		func() error { return planCurrentInodeReferences(ctx, transaction, input, plan) },
		func() error { return planCurrentManifestReferences(ctx, transaction, input, plan) },
		func() error { return planReconciledDebt(ctx, transaction, input.reconciled, plan) },
		func() error { return planReconciledAnalytics(ctx, transaction, input, state, plan) },
		func() error { return planReconciledHardlinks(input, plan) },
	}
	for _, phase := range phases {
		if err := phase(); err != nil {
			return reconciledRevisionPlan{}, false, err
		}
	}
	return plan, false, nil
}

func sortedReconciledMutations(plan reconciledRevisionPlan) []Mutation {
	mutations := make([]Mutation, 0, len(plan.puts))
	for _, mutation := range plan.puts {
		mutations = append(mutations, mutation)
	}
	sort.Slice(mutations, func(left, right int) bool { return bytes.Compare(mutations[left].Key, mutations[right].Key) < 0 })
	return mutations
}

func newReconciledRevisionPlan(ctx context.Context, transaction *Transaction, input reconciledRevisionInput, state reconciledRevisionState) (reconciledRevisionPlan, error) {
	reconciled := input.reconciled
	plan := reconciledRevisionPlan{
		puts:      map[string]Mutation{string(reconciled.CurrentKey): {Key: reconciled.CurrentKey, Value: input.currentValue}},
		oldUnique: uniqueSchemaIDs(state.oldContent), newUnique: uniqueSchemaIDs(reconciled.ContentIDs),
	}
	if !state.revisionFound {
		plan.puts[string(reconciled.RevisionKey)] = Mutation{Key: reconciled.RevisionKey, Value: reconciled.RevisionValue}
	}
	var err error
	plan.manifestID, plan.segments, err = reconciledManifest(reconciled.ContentIDs)
	if err != nil {
		return reconciledRevisionPlan{}, err
	}
	for index, segment := range plan.segments {
		key := schema.ContentManifestKey(plan.manifestID, uint32(index))
		encoded, err := segment.MarshalBinary()
		if err != nil {
			return reconciledRevisionPlan{}, err
		}
		existing, found, err := transaction.Get(ctx, key)
		if err != nil {
			return reconciledRevisionPlan{}, err
		}
		if found && !bytes.Equal(existing, encoded) {
			return reconciledRevisionPlan{}, fmt.Errorf("immutable content manifest segment already exists with different data")
		}
		if !found {
			plan.puts[string(key)] = Mutation{Key: key, Value: encoded}
		}
	}
	return plan, nil
}

func planHistoricalReferences(ctx context.Context, transaction *Transaction, input reconciledRevisionInput, state reconciledRevisionState, plan reconciledRevisionPlan) error {
	newSet := make(map[schema.ID]struct{}, len(plan.newUnique))
	for _, id := range plan.newUnique {
		newSet[id] = struct{}{}
	}
	for _, id := range plan.oldUnique {
		if _, remains := newSet[id]; remains {
			continue
		}
		key := schema.ReverseInodeKey(id, input.currentParsed.FSID, input.currentParsed.Inode)
		value, err := (schema.ReverseInodeRecord{LatestRevision: input.reconciled.Revision, State: schema.ReferenceHistorical}).MarshalBinary()
		if err != nil {
			return err
		}
		plan.puts[string(key)] = Mutation{Key: key, Value: value}
		countKey := schema.ReferenceCountKey(id)
		count, err := transactionReferenceCount(ctx, transaction, plan.puts, countKey)
		if err != nil {
			return err
		}
		count.UpdateSequence = input.reconciled.Revision
		encoded, err := count.MarshalBinary()
		if err != nil {
			return err
		}
		plan.puts[string(countKey)] = Mutation{Key: countKey, Value: encoded}
	}
	if state.oldManifestID == (schema.ID{}) || state.oldManifestID == plan.manifestID {
		return nil
	}
	for _, id := range plan.oldUnique {
		key := schema.ReverseManifestKey(id, state.oldManifestID)
		value, found, err := transaction.Get(ctx, key)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		reverse, err := schema.UnmarshalReverseManifestRecord(value)
		if err != nil {
			return err
		}
		reverse.State = schema.ReferenceHistorical
		encoded, err := reverse.MarshalBinary()
		if err != nil {
			return err
		}
		plan.puts[string(key)] = Mutation{Key: key, Value: encoded}
	}
	return nil
}

func planCurrentInodeReferences(ctx context.Context, transaction *Transaction, input reconciledRevisionInput, plan reconciledRevisionPlan) error {
	for _, id := range plan.newUnique {
		key := schema.ReverseInodeKey(id, input.currentParsed.FSID, input.currentParsed.Inode)
		_, found, err := transaction.Get(ctx, key)
		if err != nil {
			return err
		}
		value, err := (schema.ReverseInodeRecord{LatestRevision: input.reconciled.Revision, State: schema.ReferenceCurrent}).MarshalBinary()
		if err != nil {
			return err
		}
		plan.puts[string(key)] = Mutation{Key: key, Value: value}
		countKey := schema.ReferenceCountKey(id)
		count, err := transactionReferenceCount(ctx, transaction, plan.puts, countKey)
		if err != nil {
			return err
		}
		occurrences := countID(input.reconciled.ContentIDs, id)
		if math.MaxUint64-count.TotalReferences < occurrences || count.DistinctRevisions == math.MaxUint64 ||
			(!found && count.DistinctInodes == math.MaxUint64) {
			return fmt.Errorf("reference count overflow")
		}
		count.TotalReferences += occurrences
		count.DistinctRevisions++
		if !found {
			count.DistinctInodes++
		}
		count.UpdateSequence = input.reconciled.Revision
		encoded, err := count.MarshalBinary()
		if err != nil {
			return err
		}
		plan.puts[string(countKey)] = Mutation{Key: countKey, Value: encoded}
	}
	return nil
}

func planCurrentManifestReferences(ctx context.Context, transaction *Transaction, input reconciledRevisionInput, plan reconciledRevisionPlan) error {
	if len(plan.segments) == 0 {
		return nil
	}
	segmentByID := make(map[schema.ID]uint32, len(plan.newUnique))
	for index, id := range input.reconciled.ContentIDs {
		if _, found := segmentByID[id]; !found {
			segmentByID[id] = uint32(index / schema.DefaultContentSegmentIDs)
		}
	}
	for id, segment := range segmentByID {
		if err := planCurrentManifestReference(ctx, transaction, input.reconciled.Revision, plan, id, segment); err != nil {
			return err
		}
	}
	return nil
}

func planCurrentManifestReference(ctx context.Context, transaction *Transaction, revision uint64, plan reconciledRevisionPlan, id schema.ID, segment uint32) error {
	key := schema.ReverseManifestKey(id, plan.manifestID)
	_, found, err := transaction.Get(ctx, key)
	if err != nil {
		return err
	}
	value, err := (schema.ReverseManifestRecord{Segment: segment, State: schema.ReferenceCurrent}).MarshalBinary()
	if err != nil {
		return err
	}
	plan.puts[string(key)] = Mutation{Key: key, Value: value}
	if found {
		return nil
	}
	countKey := schema.ReferenceCountKey(id)
	count, err := transactionReferenceCount(ctx, transaction, plan.puts, countKey)
	if err != nil {
		return err
	}
	if count.TotalReferences == math.MaxUint64 || count.DistinctManifests == math.MaxUint64 {
		return fmt.Errorf("reference count overflow")
	}
	count.TotalReferences++
	count.DistinctManifests++
	count.UpdateSequence = revision
	encoded, err := count.MarshalBinary()
	if err != nil {
		return err
	}
	plan.puts[string(countKey)] = Mutation{Key: countKey, Value: encoded}
	return nil
}

func planReconciledDebt(ctx context.Context, transaction *Transaction, reconciled ReconciledRevision, plan reconciledRevisionPlan) error {
	for _, key := range reconciled.DebtKeys {
		value, found, err := transaction.Get(ctx, key)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		debt, err := schema.UnmarshalCrawlDebtRecord(value)
		if err != nil {
			return err
		}
		debt.Status = schema.DebtResolved
		debt.ErrorClass = ""
		debt.LastAttemptUnixNano = time.Now().UnixNano()
		encoded, err := debt.MarshalBinary()
		if err != nil {
			return err
		}
		plan.puts[string(key)] = Mutation{Key: key, Value: encoded}
	}
	for _, mutation := range reconciled.RelatedPuts {
		plan.puts[string(mutation.Key)] = mutation
	}
	return nil
}

func planReconciledAnalytics(ctx context.Context, transaction *Transaction, input reconciledRevisionInput, state reconciledRevisionState, plan reconciledRevisionPlan) error {
	if input.currentParsed.Kind != schema.KeyCurrentInode || state.currentFound {
		return nil
	}
	metadataValue, found, err := transaction.Get(ctx, schema.AnalyticsMetadataKey())
	if err != nil || !found {
		return err
	}
	metadata, err := schema.UnmarshalAnalyticsMetadataRecord(metadataValue)
	if err != nil || !metadata.Enabled {
		return nil
	}
	revision, err := schema.UnmarshalInodeRevision(input.reconciled.RevisionValue)
	if err != nil {
		return err
	}
	delta := schema.AnalyticsDeltaRecord{
		Kind: schema.AnalyticsDeltaCreation, FSID: input.revisionParsed.FSID, Inode: input.revisionParsed.Inode,
		IdentityGeneration: input.reconciled.Revision, Revision: input.reconciled.Revision,
		UID: revision.UID, GID: revision.GID, Known: revision.Known, LogicalSize: revision.Size,
		CreatedAt: time.Now().UnixNano(), CreationBasis: schema.AnalyticsFirstSeen,
		IdentityContinuity: schema.AnalyticsContinuityUnknown, State: schema.AnalyticsLive,
		ClassificationEpoch: metadata.Generation,
	}
	encoded, err := delta.MarshalBinary()
	if err != nil {
		return fmt.Errorf("encode analytics outbox delta: %w", err)
	}
	key := schema.AnalyticsDeltaKey(input.reconciled.Revision, 0)
	plan.puts[string(key)] = Mutation{Key: key, Value: encoded}
	return nil
}

func planReconciledHardlinks(input reconciledRevisionInput, plan reconciledRevisionPlan) error {
	reconciled := input.reconciled
	if !reconciled.HasMultipleParents || len(reconciled.HardlinkParents) == 0 {
		return nil
	}
	record := schema.HardlinkRefsRecord{
		FSID: input.revisionParsed.FSID, Inode: input.revisionParsed.Inode, Revision: reconciled.Revision,
		Parents: reconciled.HardlinkParents, Freshness: schema.FreshnessVerified,
	}
	encoded, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	key := schema.HardlinkRefsKey(input.revisionParsed.FSID, input.revisionParsed.Inode, reconciled.Revision)
	plan.puts[string(key)] = Mutation{Key: key, Value: encoded}
	return nil
}

func prepareReconciledRevision(reconciled ReconciledRevision) (reconciledRevisionInput, error) {
	currentValue, err := (schema.CurrentPointer{Revision: reconciled.Revision, RecordKey: reconciled.RevisionKey}).MarshalBinary()
	if err != nil {
		return reconciledRevisionInput{}, err
	}
	currentParsed, err := schema.ParseKey(reconciled.CurrentKey)
	if err != nil {
		return reconciledRevisionInput{}, err
	}
	revisionParsed, err := schema.ParseKey(reconciled.RevisionKey)
	matchingKinds := currentParsed.Kind == schema.KeyCurrentInode && revisionParsed.Kind == schema.KeyInodeRevision ||
		currentParsed.Kind == schema.KeyCurrentDirectory && revisionParsed.Kind == schema.KeyDirectoryRevision
	if err != nil || reconciled.Revision == 0 || revisionParsed.Revision != reconciled.Revision ||
		currentParsed.FSID != revisionParsed.FSID || currentParsed.Inode != revisionParsed.Inode || !matchingKinds {
		return reconciledRevisionInput{}, fmt.Errorf("reconciled revision key mismatch")
	}
	if err := schema.ValidateValue(reconciled.RevisionKey, reconciled.RevisionValue); err != nil {
		return reconciledRevisionInput{}, err
	}
	if currentParsed.Kind == schema.KeyCurrentDirectory && len(reconciled.ContentIDs) != 0 {
		return reconciledRevisionInput{}, fmt.Errorf("directory reconciliation cannot contain file content")
	}
	if err := validateReconciledRevisionExtras(reconciled); err != nil {
		return reconciledRevisionInput{}, err
	}
	return reconciledRevisionInput{reconciled, currentValue, currentParsed, revisionParsed}, nil
}

func validateReconciledRevisionExtras(reconciled ReconciledRevision) error {
	for _, key := range reconciled.DebtKeys {
		parsed, err := schema.ParseKey(key)
		if err != nil || parsed.Kind != schema.KeyCrawlDebt {
			return fmt.Errorf("reconciliation debt key is invalid")
		}
	}
	for _, mutation := range reconciled.RelatedPuts {
		if err := validateMutableKey(mutation.Key); err != nil {
			return err
		}
		if err := schema.ValidateValue(mutation.Key, mutation.Value); err != nil {
			return err
		}
	}
	return nil
}

func loadReconciledRevisionState(ctx context.Context, transaction *Transaction, input reconciledRevisionInput) (reconciledRevisionState, error) {
	reconciled := input.reconciled
	existing, revisionFound, err := transaction.Get(ctx, reconciled.RevisionKey)
	if err != nil {
		return reconciledRevisionState{}, err
	}
	if revisionFound && !bytes.Equal(existing, reconciled.RevisionValue) {
		return reconciledRevisionState{}, fmt.Errorf("immutable revision already exists with different data")
	}
	state := reconciledRevisionState{revisionFound: revisionFound}
	oldCurrent, currentFound, err := transaction.Get(ctx, reconciled.CurrentKey)
	state.currentFound = currentFound
	if err != nil || !state.currentFound {
		return state, err
	}
	pointer, err := schema.UnmarshalCurrentPointer(oldCurrent)
	if err != nil {
		return state, err
	}
	if revisionFound && pointer.Revision == reconciled.Revision && bytes.Equal(pointer.RecordKey, reconciled.RevisionKey) {
		state.noop = true
		return state, nil
	}
	if pointer.Revision > reconciled.Revision {
		return state, fmt.Errorf("current revision %d is newer than %d", pointer.Revision, reconciled.Revision)
	}
	if input.currentParsed.Kind != schema.KeyCurrentInode {
		return state, nil
	}
	return loadPriorInodeContent(ctx, transaction, pointer.RecordKey, state)
}

func loadPriorInodeContent(ctx context.Context, transaction *Transaction, key []byte, state reconciledRevisionState) (reconciledRevisionState, error) {
	value, found, err := transaction.Get(ctx, key)
	if err != nil {
		return state, err
	}
	if !found {
		return state, fmt.Errorf("current inode revision is missing")
	}
	record, err := schema.UnmarshalInodeRevision(value)
	if err != nil {
		return state, err
	}
	state.oldContent, err = transactionContentIDs(ctx, transaction, record)
	if record.ContentMode == schema.ContentManifestRef {
		state.oldManifestID = record.ContentManifestID
	}
	return state, err
}

func reconciledManifest(ids []schema.ID) (schema.ID, []schema.ContentManifest, error) {
	if len(ids) <= schema.MaxInlineContentIDs {
		return schema.ID{}, nil, nil
	}
	return schema.SegmentContent(ids, schema.DefaultContentSegmentIDs)
}

func transactionContentIDs(ctx context.Context, transaction *Transaction, record schema.InodeRevision) ([]schema.ID, error) {
	switch record.ContentMode {
	case schema.ContentNone:
		return nil, nil
	case schema.ContentInline:
		return append([]schema.ID(nil), record.ContentIDs...), nil
	case schema.ContentManifestRef:
		segmentCount := (uint64(record.ContentCount) + schema.DefaultContentSegmentIDs - 1) / schema.DefaultContentSegmentIDs
		segments := make([]schema.ContentManifest, segmentCount)
		for index := range segments {
			value, found, err := transaction.Get(ctx, schema.ContentManifestKey(record.ContentManifestID, uint32(index)))
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fmt.Errorf("content manifest segment is missing")
			}
			segments[index], err = schema.UnmarshalContentManifest(value)
			if err != nil {
				return nil, err
			}
		}
		return schema.AssembleContent(record.ContentManifestID, segments)
	default:
		return nil, fmt.Errorf("invalid content mode")
	}
}

func transactionReferenceCount(ctx context.Context, transaction *Transaction, puts map[string]Mutation, key []byte) (schema.ReferenceCountRecord, error) {
	if mutation, found := puts[string(key)]; found {
		return schema.UnmarshalReferenceCountRecord(mutation.Value)
	}
	value, found, err := transaction.Get(ctx, key)
	if err != nil || !found {
		return schema.ReferenceCountRecord{}, err
	}
	return schema.UnmarshalReferenceCountRecord(value)
}

func uniqueSchemaIDs(ids []schema.ID) []schema.ID {
	seen := make(map[schema.ID]struct{}, len(ids))
	result := make([]schema.ID, 0, len(ids))
	for _, id := range ids {
		if _, found := seen[id]; found {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func countID(ids []schema.ID, target schema.ID) uint64 {
	var count uint64
	for _, id := range ids {
		if id == target {
			count++
		}
	}
	return count
}

func validateRelatedMutations(puts []Mutation, deletes [][]byte) error {
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
	return nil
}

func writeTransactionBatches(ctx context.Context, transaction *Transaction, limits Limits, puts []Mutation, deletes [][]byte) error {
	if limits.MaxBatchItems == 0 || limits.MaxMessageBytes < 1024 {
		return fmt.Errorf("vaulticdb advertised insufficient transaction batch limits")
	}
	maxBytes := uint64(limits.MaxMessageBytes) - 512
	for putStart := 0; putStart < len(puts); {
		used := uint64(128)
		putEnd := putStart
		for putEnd < len(puts) && uint64(putEnd-putStart) < uint64(limits.MaxBatchItems) {
			size := uint64(len(puts[putEnd].Key)+len(puts[putEnd].Value)) + 32
			if size > maxBytes {
				return fmt.Errorf("schema mutation exceeds daemon message limit")
			}
			if used+size > maxBytes {
				break
			}
			used += size
			putEnd++
		}
		if putEnd == putStart {
			return fmt.Errorf("schema mutation batch made no progress")
		}
		if err := transaction.WriteBatch(ctx, puts[putStart:putEnd], nil); err != nil {
			return err
		}
		putStart = putEnd
	}
	for deleteStart := 0; deleteStart < len(deletes); {
		used := uint64(128)
		deleteEnd := deleteStart
		for deleteEnd < len(deletes) && uint64(deleteEnd-deleteStart) < uint64(limits.MaxBatchItems) {
			size := uint64(len(deletes[deleteEnd])) + 16
			if size > maxBytes {
				return fmt.Errorf("schema delete exceeds daemon message limit")
			}
			if used+size > maxBytes {
				break
			}
			used += size
			deleteEnd++
		}
		if deleteEnd == deleteStart {
			return fmt.Errorf("schema delete batch made no progress")
		}
		if err := transaction.WriteBatch(ctx, nil, deletes[deleteStart:deleteEnd]); err != nil {
			return err
		}
		deleteStart = deleteEnd
	}
	return nil
}

func validateDistinctMutations(puts []Mutation, deletes [][]byte) error {
	seen := make(map[string]struct{}, len(puts)+len(deletes))
	for _, put := range puts {
		key := string(put.Key)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("schema batch contains duplicate key")
		}
		seen[key] = struct{}{}
	}
	for _, deleteKey := range deletes {
		key := string(deleteKey)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("schema batch contains duplicate key")
		}
		seen[key] = struct{}{}
	}
	return nil
}

// AllocateRevision atomically returns one durable, monotonically increasing
// repository revision sequence. Serializable conflicts are retried.
func (store *SchemaStore) AllocateRevision(ctx context.Context) (uint64, error) {
	key := schema.NextRevisionKey()
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		transaction, err := store.client.Begin(ctx)
		if err != nil {
			return 0, err
		}
		encoded, found, err := transaction.Get(ctx, key)
		if err != nil {
			rollbackTransaction(ctx, transaction)
			return 0, err
		}
		next := uint64(1)
		if found {
			next, err = schema.UnmarshalNextRevision(encoded)
			if err != nil {
				rollbackTransaction(ctx, transaction)
				return 0, err
			}
		}
		if next == math.MaxUint64 {
			rollbackTransaction(ctx, transaction)
			return 0, fmt.Errorf("repository revision sequence exhausted")
		}
		encodedNext, err := schema.MarshalNextRevision(next + 1)
		if err != nil {
			rollbackTransaction(ctx, transaction)
			return 0, err
		}
		if err := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: encodedNext}}, nil); err != nil {
			rollbackTransaction(ctx, transaction)
			return 0, err
		}
		if err := transaction.Commit(ctx); err != nil {
			rollbackTransaction(ctx, transaction)
			if status.Code(err) == codes.Aborted {
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return 0, ctx.Err()
				case <-timer.C:
				}
				backoff = min(backoff*2, 25*time.Millisecond)
				continue
			}
			return 0, err
		}
		return next, nil
	}
	return 0, fmt.Errorf("allocate repository revision: %w", errors.New("transaction conflict retry limit exceeded"))
}
