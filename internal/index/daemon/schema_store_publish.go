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

func (store *SchemaStore) publishReconciledRevisionOnce(ctx context.Context, reconciled ReconciledRevision) error {
	currentValue, err := (schema.CurrentPointer{Revision: reconciled.Revision, RecordKey: reconciled.RevisionKey}).MarshalBinary()
	if err != nil {
		return err
	}
	currentParsed, err := schema.ParseKey(reconciled.CurrentKey)
	if err != nil {
		return err
	}
	revisionParsed, err := schema.ParseKey(reconciled.RevisionKey)
	if err != nil || reconciled.Revision == 0 || revisionParsed.Revision != reconciled.Revision ||
		currentParsed.FSID != revisionParsed.FSID || currentParsed.Inode != revisionParsed.Inode ||
		!((currentParsed.Kind == schema.KeyCurrentInode && revisionParsed.Kind == schema.KeyInodeRevision) ||
			(currentParsed.Kind == schema.KeyCurrentDirectory && revisionParsed.Kind == schema.KeyDirectoryRevision)) {
		return fmt.Errorf("reconciled revision key mismatch")
	}
	if err := schema.ValidateValue(reconciled.RevisionKey, reconciled.RevisionValue); err != nil {
		return err
	}
	if currentParsed.Kind == schema.KeyCurrentDirectory && len(reconciled.ContentIDs) != 0 {
		return fmt.Errorf("directory reconciliation cannot contain file content")
	}
	for _, key := range reconciled.DebtKeys {
		parsed, parseErr := schema.ParseKey(key)
		if parseErr != nil || parsed.Kind != schema.KeyCrawlDebt {
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

	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		rollbackTransaction(ctx, transaction)
		return err
	}
	existingRevision, revisionFound, err := transaction.Get(ctx, reconciled.RevisionKey)
	if err != nil {
		return fail(err)
	}
	if revisionFound && !bytes.Equal(existingRevision, reconciled.RevisionValue) {
		return fail(fmt.Errorf("immutable revision already exists with different data"))
	}

	var oldContent []schema.ID
	var oldManifestID schema.ID
	oldCurrent, currentFound, err := transaction.Get(ctx, reconciled.CurrentKey)
	if err != nil {
		return fail(err)
	}
	if currentFound {
		pointer, decodeErr := schema.UnmarshalCurrentPointer(oldCurrent)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		if revisionFound && pointer.Revision == reconciled.Revision && bytes.Equal(pointer.RecordKey, reconciled.RevisionKey) {
			return transaction.Rollback(ctx)
		}
		if pointer.Revision > reconciled.Revision {
			return fail(fmt.Errorf("current revision %d is newer than %d", pointer.Revision, reconciled.Revision))
		}
		if currentParsed.Kind == schema.KeyCurrentInode {
			value, found, getErr := transaction.Get(ctx, pointer.RecordKey)
			if getErr != nil {
				return fail(getErr)
			}
			if !found {
				return fail(fmt.Errorf("current inode revision is missing"))
			}
			record, decodeErr := schema.UnmarshalInodeRevision(value)
			if decodeErr != nil {
				return fail(decodeErr)
			}
			oldContent, err = transactionContentIDs(ctx, transaction, record)
			if err != nil {
				return fail(err)
			}
			if record.ContentMode == schema.ContentManifestRef {
				oldManifestID = record.ContentManifestID
			}
		}
	}

	puts := map[string]Mutation{string(reconciled.CurrentKey): {Key: reconciled.CurrentKey, Value: currentValue}}
	if !revisionFound {
		puts[string(reconciled.RevisionKey)] = Mutation{Key: reconciled.RevisionKey, Value: reconciled.RevisionValue}
	}
	manifestID, segments, err := reconciledManifest(reconciled.ContentIDs)
	if err != nil {
		return fail(err)
	}
	for index, segment := range segments {
		key := schema.ContentManifestKey(manifestID, uint32(index))
		encoded, encodeErr := segment.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		existing, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			return fail(getErr)
		}
		if found && !bytes.Equal(existing, encoded) {
			return fail(fmt.Errorf("immutable content manifest segment already exists with different data"))
		}
		if !found {
			puts[string(key)] = Mutation{Key: key, Value: encoded}
		}
	}

	oldUnique := uniqueSchemaIDs(oldContent)
	newUnique := uniqueSchemaIDs(reconciled.ContentIDs)
	newSet := make(map[schema.ID]struct{}, len(newUnique))
	for _, id := range newUnique {
		newSet[id] = struct{}{}
	}
	for _, id := range oldUnique {
		if _, remains := newSet[id]; remains {
			continue
		}
		key := schema.ReverseInodeKey(id, currentParsed.FSID, currentParsed.Inode)
		value, encodeErr := (schema.ReverseInodeRecord{LatestRevision: reconciled.Revision, State: schema.ReferenceHistorical}).MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts[string(key)] = Mutation{Key: key, Value: value}
		countKey := schema.ReferenceCountKey(id)
		count, countErr := transactionReferenceCount(ctx, transaction, puts, countKey)
		if countErr != nil {
			return fail(countErr)
		}
		count.UpdateSequence = reconciled.Revision
		encoded, encodeErr := count.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts[string(countKey)] = Mutation{Key: countKey, Value: encoded}
	}
	if oldManifestID != (schema.ID{}) && oldManifestID != manifestID {
		for _, id := range oldUnique {
			key := schema.ReverseManifestKey(id, oldManifestID)
			value, found, getErr := transaction.Get(ctx, key)
			if getErr != nil {
				return fail(getErr)
			}
			if !found {
				continue
			}
			reverse, decodeErr := schema.UnmarshalReverseManifestRecord(value)
			if decodeErr != nil {
				return fail(decodeErr)
			}
			reverse.State = schema.ReferenceHistorical
			encoded, encodeErr := reverse.MarshalBinary()
			if encodeErr != nil {
				return fail(encodeErr)
			}
			puts[string(key)] = Mutation{Key: key, Value: encoded}
		}
	}
	for _, id := range newUnique {
		key := schema.ReverseInodeKey(id, currentParsed.FSID, currentParsed.Inode)
		_, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			return fail(getErr)
		}
		value, encodeErr := (schema.ReverseInodeRecord{LatestRevision: reconciled.Revision, State: schema.ReferenceCurrent}).MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts[string(key)] = Mutation{Key: key, Value: value}
		countKey := schema.ReferenceCountKey(id)
		count, countErr := transactionReferenceCount(ctx, transaction, puts, countKey)
		if countErr != nil {
			return fail(countErr)
		}
		occurrences := countID(reconciled.ContentIDs, id)
		if math.MaxUint64-count.TotalReferences < occurrences || count.DistinctRevisions == math.MaxUint64 || (!found && count.DistinctInodes == math.MaxUint64) {
			return fail(fmt.Errorf("reference count overflow"))
		}
		count.TotalReferences += occurrences
		count.DistinctRevisions++
		if !found {
			count.DistinctInodes++
		}
		count.UpdateSequence = reconciled.Revision
		encoded, encodeErr := count.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts[string(countKey)] = Mutation{Key: countKey, Value: encoded}
	}

	if len(segments) > 0 {
		segmentByID := make(map[schema.ID]uint32, len(newUnique))
		for index, id := range reconciled.ContentIDs {
			if _, found := segmentByID[id]; !found {
				segmentByID[id] = uint32(index / schema.DefaultContentSegmentIDs)
			}
		}
		for id, segment := range segmentByID {
			key := schema.ReverseManifestKey(id, manifestID)
			_, found, getErr := transaction.Get(ctx, key)
			if getErr != nil {
				return fail(getErr)
			}
			value, encodeErr := (schema.ReverseManifestRecord{Segment: segment, State: schema.ReferenceCurrent}).MarshalBinary()
			if encodeErr != nil {
				return fail(encodeErr)
			}
			puts[string(key)] = Mutation{Key: key, Value: value}
			if !found {
				countKey := schema.ReferenceCountKey(id)
				count, countErr := transactionReferenceCount(ctx, transaction, puts, countKey)
				if countErr != nil {
					return fail(countErr)
				}
				if count.TotalReferences == math.MaxUint64 || count.DistinctManifests == math.MaxUint64 {
					return fail(fmt.Errorf("reference count overflow"))
				}
				count.TotalReferences++
				count.DistinctManifests++
				count.UpdateSequence = reconciled.Revision
				encoded, encodeErr := count.MarshalBinary()
				if encodeErr != nil {
					return fail(encodeErr)
				}
				puts[string(countKey)] = Mutation{Key: countKey, Value: encoded}
			}
		}
	}

	for _, key := range reconciled.DebtKeys {
		value, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			return fail(getErr)
		}
		if !found {
			continue
		}
		debt, decodeErr := schema.UnmarshalCrawlDebtRecord(value)
		if decodeErr != nil {
			return fail(decodeErr)
		}
		debt.Status = schema.DebtResolved
		debt.ErrorClass = ""
		debt.LastAttemptUnixNano = time.Now().UnixNano()
		encoded, encodeErr := debt.MarshalBinary()
		if encodeErr != nil {
			return fail(encodeErr)
		}
		puts[string(key)] = Mutation{Key: key, Value: encoded}
	}
	for _, mutation := range reconciled.RelatedPuts {
		puts[string(mutation.Key)] = mutation
	}
	if currentParsed.Kind == schema.KeyCurrentInode && !currentFound {
		metadataValue, enabled, getErr := transaction.Get(ctx, schema.AnalyticsMetadataKey())
		if getErr != nil {
			return fail(getErr)
		}
		if enabled {
			metadata, decodeErr := schema.UnmarshalAnalyticsMetadataRecord(metadataValue)
			if decodeErr == nil && metadata.Enabled {
				revision, decodeErr := schema.UnmarshalInodeRevision(reconciled.RevisionValue)
				if decodeErr != nil {
					return fail(decodeErr)
				}
				delta := schema.AnalyticsDeltaRecord{
					Kind: schema.AnalyticsDeltaCreation, FSID: revisionParsed.FSID, Inode: revisionParsed.Inode,
					IdentityGeneration: reconciled.Revision, Revision: reconciled.Revision,
					UID: revision.UID, GID: revision.GID, Known: revision.Known, LogicalSize: revision.Size,
					CreatedAt: time.Now().UnixNano(), CreationBasis: schema.AnalyticsFirstSeen,
					IdentityContinuity: schema.AnalyticsContinuityUnknown, State: schema.AnalyticsLive,
					ClassificationEpoch: metadata.Generation,
				}
				encoded, encodeErr := delta.MarshalBinary()
				if encodeErr != nil {
					return fail(fmt.Errorf("encode analytics outbox delta: %w", encodeErr))
				}
				key := schema.AnalyticsDeltaKey(reconciled.Revision, 0)
				puts[string(key)] = Mutation{Key: key, Value: encoded}
			}
		}
	}

	// Publish hardlink reference records for multi-parent inodes.
	if reconciled.HasMultipleParents && len(reconciled.HardlinkParents) > 0 {
		hrRec := schema.HardlinkRefsRecord{
			FSID: revisionParsed.FSID, Inode: revisionParsed.Inode, Revision: reconciled.Revision,
			Parents: reconciled.HardlinkParents, Freshness: schema.FreshnessVerified,
		}
		hrEncoded, hrErr := hrRec.MarshalBinary()
		if hrErr != nil {
			return fail(hrErr)
		}
		hrKey := schema.HardlinkRefsKey(revisionParsed.FSID, revisionParsed.Inode, reconciled.Revision)
		puts[string(hrKey)] = Mutation{Key: hrKey, Value: hrEncoded}
	}

	mutations := make([]Mutation, 0, len(puts))
	for _, mutation := range puts {
		mutations = append(mutations, mutation)
	}
	sort.Slice(mutations, func(left, right int) bool { return bytes.Compare(mutations[left].Key, mutations[right].Key) < 0 })
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), mutations, nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
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
