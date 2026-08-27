package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const revisionAllocationAttempts = 128

// SchemaStore applies the Vaultic schema's immutability and revision rules over
// the bounded daemon client.
type SchemaStore struct{ client *Client }

func NewSchemaStore(client *Client) *SchemaStore { return &SchemaStore{client: client} }

func (store *SchemaStore) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if _, err := schema.ParseKey(key); err != nil {
		return nil, false, err
	}
	return store.client.Get(ctx, key, "")
}

func (store *SchemaStore) Put(ctx context.Context, key, value []byte, durable bool) error {
	return store.WriteMutableBatch(ctx, []Mutation{{Key: key, Value: value}}, nil, durable)
}

// WriteMutableBatch atomically updates independently mutable schema records.
// Immutable records, current pointers, and the revision counter require their
// dedicated transactional operations.
func (store *SchemaStore) WriteMutableBatch(ctx context.Context, puts []Mutation, deletes [][]byte, durable bool) error {
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
	if err := transaction.WriteBatch(ctx, remainingPuts, deletes); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
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
	case schema.KeyPack, schema.KeyPackAggregate, schema.KeyReverseManifest, schema.KeyReverseInode,
		schema.KeyReferenceCount, schema.KeyGarbageCollection, schema.KeyCrawlDebt:
		return nil
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
	case schema.KeyReverseManifest, schema.KeyReverseInode, schema.KeyReferenceCount:
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
	case schema.KeyPack, schema.KeyPackAggregate, schema.KeyReverseManifest, schema.KeyReverseInode,
		schema.KeyReferenceCount, schema.KeyGarbageCollection, schema.KeyCrawlDebt:
		return false, nil
	case schema.KeyContentManifest:
		return false, fmt.Errorf("content manifests require CreateContentManifest")
	case schema.KeyNextRevision:
		return false, fmt.Errorf("revision sequence requires AllocateRevision")
	default:
		return false, fmt.Errorf("schema key requires a dedicated transactional operation")
	}
}

// CreateImmutable creates a historical record or verifies an identical retry.
// It never overwrites an existing key with different bytes.
func (store *SchemaStore) CreateImmutable(ctx context.Context, key, value []byte) error {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return err
	}
	switch parsed.Kind {
	case schema.KeyBlob, schema.KeyInodeRevision, schema.KeyDirectoryRevision, schema.KeySnapshot:
	case schema.KeyContentManifest:
		return fmt.Errorf("content manifests require CreateContentManifest")
	default:
		return fmt.Errorf("schema key is mutable and cannot use CreateImmutable")
	}
	if err := schema.ValidateValue(key, value); err != nil {
		return err
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	existing, found, err := transaction.Get(ctx, key)
	if err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if found {
		if rollbackErr := rollbackTransaction(ctx, transaction); rollbackErr != nil {
			return rollbackErr
		}
		if !bytes.Equal(existing, value) {
			return fmt.Errorf("immutable schema record already exists with different data")
		}
		return nil
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: value}}, nil); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

// CreateContentManifest atomically creates every immutable segment for one
// content-addressed ordered blob sequence, or verifies an identical retry.
func (store *SchemaStore) CreateContentManifest(ctx context.Context, ids []schema.ID) (schema.ID, error) {
	return store.PublishContentManifest(ctx, ids, nil, nil)
}

// PublishContentManifest atomically creates canonical immutable segments and
// applies their mutable reverse-reference updates.
func (store *SchemaStore) PublishContentManifest(ctx context.Context, ids []schema.ID, relatedPuts []Mutation, relatedDeletes [][]byte) (schema.ID, error) {
	if err := validateRelatedMutations(relatedPuts, relatedDeletes); err != nil {
		return schema.ID{}, err
	}
	manifestID, segments, err := schema.SegmentContent(ids, schema.DefaultContentSegmentIDs)
	if err != nil {
		return schema.ID{}, err
	}
	if _, err := schema.AssembleContent(manifestID, append([]schema.ContentManifest(nil), segments...)); err != nil {
		return schema.ID{}, err
	}
	keys := make([][]byte, len(segments))
	encoded := make([][]byte, len(segments))
	for index, segment := range segments {
		keys[index] = schema.ContentManifestKey(manifestID, uint32(index))
		encoded[index], err = segment.MarshalBinary()
		if err != nil {
			return schema.ID{}, err
		}
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return schema.ID{}, err
	}
	for start := 0; start < len(keys); {
		end, err := manifestBatchEnd(store.client.Limits(), keys, encoded, start)
		if err != nil {
			rollbackTransaction(ctx, transaction)
			return schema.ID{}, err
		}
		values, found, err := transaction.MultiGet(ctx, keys[start:end])
		if err != nil {
			rollbackTransaction(ctx, transaction)
			return schema.ID{}, err
		}
		puts := make([]Mutation, 0, end-start)
		for offset := range keys[start:end] {
			index := start + offset
			if found[offset] {
				if !bytes.Equal(values[offset].Value, encoded[index]) {
					rollbackTransaction(ctx, transaction)
					return schema.ID{}, fmt.Errorf("immutable content manifest segment already exists with different data")
				}
				continue
			}
			puts = append(puts, Mutation{Key: keys[index], Value: encoded[index]})
		}
		if len(puts) > 0 {
			if err := transaction.WriteBatch(ctx, puts, nil); err != nil {
				rollbackTransaction(ctx, transaction)
				return schema.ID{}, err
			}
		}
		start = end
	}
	if len(relatedPuts) > 0 || len(relatedDeletes) > 0 {
		if err := transaction.WriteBatch(ctx, relatedPuts, relatedDeletes); err != nil {
			rollbackTransaction(ctx, transaction)
			return schema.ID{}, err
		}
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return schema.ID{}, err
	}
	return manifestID, nil
}

func manifestBatchEnd(limits Limits, keys, values [][]byte, start int) (int, error) {
	if limits.MaxBatchItems == 0 || limits.MaxMessageBytes < 128 {
		return 0, fmt.Errorf("vaulticdb advertised insufficient manifest batch limits")
	}
	maxBytes := uint64(limits.MaxMessageBytes) / 2
	used := uint64(0)
	end := start
	for end < len(keys) && uint64(end-start) < uint64(limits.MaxBatchItems) {
		size := uint64(len(keys[end])) + uint64(len(values[end])) + 32
		if size > maxBytes {
			return 0, fmt.Errorf("content manifest segment exceeds daemon message limit")
		}
		if used+size > maxBytes {
			break
		}
		used += size
		end++
	}
	if end == start {
		return 0, fmt.Errorf("content manifest batch made no progress")
	}
	return end, nil
}

// PublishRevision atomically creates an immutable revision and advances its
// current pointer. A retry with identical revision bytes is idempotent.
func (store *SchemaStore) PublishRevision(ctx context.Context, currentKey, revisionKey, revisionValue []byte, revision uint64) error {
	return store.PublishRevisionBatch(ctx, currentKey, revisionKey, revisionValue, revision, nil, nil)
}

// PublishRevisionBatch atomically creates an immutable revision, advances its
// current pointer, and applies corresponding mutable reverse-reference data.
func (store *SchemaStore) PublishRevisionBatch(ctx context.Context, currentKey, revisionKey, revisionValue []byte, revision uint64, relatedPuts []Mutation, relatedDeletes [][]byte) error {
	if err := validateRelatedMutations(relatedPuts, relatedDeletes); err != nil {
		return err
	}
	current, err := schema.CurrentPointer{Revision: revision, RecordKey: revisionKey}.MarshalBinary()
	if err != nil {
		return err
	}
	currentParsed, err := schema.ParseKey(currentKey)
	if err != nil {
		return err
	}
	parsed, err := schema.ParseKey(revisionKey)
	validPair := currentParsed.FSID == parsed.FSID && currentParsed.Inode == parsed.Inode &&
		((currentParsed.Kind == schema.KeyCurrentInode && parsed.Kind == schema.KeyInodeRevision) ||
			(currentParsed.Kind == schema.KeyCurrentDirectory && parsed.Kind == schema.KeyDirectoryRevision))
	if err != nil || parsed.Revision != revision || !validPair {
		return fmt.Errorf("revision key does not match revision %d", revision)
	}
	if err := schema.ValidateValue(revisionKey, revisionValue); err != nil {
		return err
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	existing, found, err := transaction.Get(ctx, revisionKey)
	if err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if found && !bytes.Equal(existing, revisionValue) {
		rollbackTransaction(ctx, transaction)
		return fmt.Errorf("immutable revision already exists with different data")
	}
	currentValue, currentFound, err := transaction.Get(ctx, currentKey)
	if err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if currentFound {
		pointer, decodeErr := schema.UnmarshalCurrentPointer(currentValue)
		if decodeErr != nil {
			rollbackTransaction(ctx, transaction)
			return decodeErr
		}
		if pointer.Revision > revision {
			rollbackTransaction(ctx, transaction)
			return fmt.Errorf("current revision %d is newer than %d", pointer.Revision, revision)
		}
	}
	puts := []Mutation{{Key: currentKey, Value: current}}
	if !found {
		puts = append(puts, Mutation{Key: revisionKey, Value: revisionValue})
	}
	puts = append(puts, relatedPuts...)
	if err := transaction.WriteBatch(ctx, puts, relatedDeletes); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
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

func rollbackTransaction(ctx context.Context, transaction *Transaction) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultRPCDeadline)
	defer cancel()
	return transaction.Rollback(cleanupCtx)
}
