package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func loadCompatibleBuildCheckpoint(
	ctx context.Context,
	store Store,
	generation uint64,
	configJSON string,
) (schema.AnalyticsBuildCheckpointRecord, bool, error) {
	value, found, err := store.Get(ctx, schema.AnalyticsBuildCheckpointKey())
	if err != nil || !found {
		return schema.AnalyticsBuildCheckpointRecord{}, false, err
	}
	checkpoint, err := schema.UnmarshalAnalyticsBuildCheckpointRecord(value)
	if err != nil {
		return schema.AnalyticsBuildCheckpointRecord{}, false, fmt.Errorf("decode analytics build checkpoint: %w", err)
	}
	if checkpoint.FormatVersion == 1 && checkpoint.Generation == generation && checkpoint.ConfigJSON == configJSON {
		return checkpoint, true, nil
	}
	if err := cleanupCandidateGeneration(ctx, store, checkpoint.Generation); err != nil {
		return schema.AnalyticsBuildCheckpointRecord{}, false, fmt.Errorf("clean abandoned analytics build: %w", err)
	}
	if err := store.WriteMutableBatch(ctx, nil, [][]byte{schema.AnalyticsBuildCheckpointKey()}, true); err != nil {
		return schema.AnalyticsBuildCheckpointRecord{}, false, err
	}
	return schema.AnalyticsBuildCheckpointRecord{}, false, nil
}

func saveBuildCheckpoint(ctx context.Context, store Store, checkpoint schema.AnalyticsBuildCheckpointRecord) error {
	value, err := checkpoint.MarshalBinary()
	if err != nil {
		return err
	}
	return store.WriteMutableBatch(
		ctx,
		[]daemon.Mutation{{Key: schema.AnalyticsBuildCheckpointKey(), Value: value}},
		nil,
		true,
	)
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func validateBuildCheckpoint(
	ctx context.Context,
	store Store,
	checkpoint schema.AnalyticsBuildCheckpointRecord,
) (bool, error) {
	if len(checkpoint.CandidateSegments) == 0 && len(checkpoint.SourceKeyCursor) != 0 {
		return false, nil
	}
	if len(checkpoint.SourceKeyCursor) != 0 {
		key, err := schema.ParseKey(checkpoint.SourceKeyCursor)
		if err != nil || key.Kind != schema.KeyInodeRevision && key.Kind != schema.KeyAuthoritativeSourceBinding {
			return false, nil
		}
	}
	for index, segment := range checkpoint.CandidateSegments {
		expectedSegment := checkpoint.Generation<<32 | uint64(index+1)
		if segment != expectedSegment {
			return false, nil
		}
		segmentValue, found, err := store.Get(ctx, schema.AnalyticsFactSegmentKey(segment))
		if err != nil || !found {
			return false, err
		}
		rows, err := decodeSegment(segmentValue)
		if err != nil {
			return false, nil
		}
		metadataValue, found, err := store.Get(ctx, schema.AnalyticsSegmentMetadataKey(segment))
		if err != nil || !found {
			return false, err
		}
		metadata, err := schema.UnmarshalAnalyticsSegmentMetadataRecord(metadataValue)
		if err != nil || metadata.RowCount != uint32(len(rows.Identity)) {
			return false, nil
		}
		for dimension, values := range indexValues(rows) {
			for value := range values {
				if _, found, err := store.Get(ctx, schema.AnalyticsDimensionIndexKey(dimension, value, segment)); err != nil ||
					!found {
					return false, err
				}
			}
		}
	}
	return true, nil
}

func cleanupCandidateGeneration(ctx context.Context, store Store, generation uint64) error {
	if generation == 0 {
		return nil
	}
	minimum, maximum := generation<<32, (generation+1)<<32
	var deletes [][]byte
	appendDelete := func(key []byte) error {
		deletes = append(deletes, append([]byte(nil), key...))
		if len(deletes) < pageSize {
			return nil
		}
		if err := store.WriteMutableBatch(ctx, nil, deletes, false); err != nil {
			return err
		}
		deletes = deletes[:0]
		return nil
	}
	for _, prefix := range [][]byte{schema.AnalyticsFactSegmentPrefix(), schema.AnalyticsSegmentMetadataPrefix(), []byte("ai:")} {
		if err := scan(ctx, store, prefix, func(kv daemon.KeyValue) error {
			key, err := schema.ParseKey(kv.Key)
			if err != nil {
				return err
			}
			if key.Generation > minimum && (maximum == 0 || key.Generation < maximum) {
				return appendDelete(kv.Key)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if err := scan(ctx, store, schema.AnalyticsDerivedGenerationPrefix(generation), func(kv daemon.KeyValue) error {
		return appendDelete(kv.Key)
	}); err != nil {
		return err
	}
	if len(deletes) == 0 {
		return nil
	}
	return store.WriteMutableBatch(ctx, nil, deletes, false)
}

func streamAuthoritativeFacts(
	ctx context.Context,
	store Store,
	config Config,
	afterKey []byte,
	batchSize int,
	consume func([]buildFact, []byte) error,
) (uint64, uint64, error) {
	bindings, _, err := store.ScanPrefix(ctx, []byte("asb:"), nil, 1)
	if err != nil {
		return 0, 0, err
	}
	if len(bindings) != 0 {
		return streamSourceBindings(ctx, store, config, afterKey, batchSize, consume)
	}
	return streamLegacyRevisions(ctx, store, config, afterKey, batchSize, consume)
}

func streamSourceBindings(
	ctx context.Context,
	store Store,
	config Config,
	afterKey []byte,
	batchSize int,
	consume func([]buildFact, []byte) error,
) (uint64, uint64, error) {
	var batch []buildFact
	var cursor, lastKey []byte
	var facts, applied uint64
	if len(afterKey) != 0 {
		cursor = append(append([]byte(nil), afterKey...), 0)
	}
	for {
		items, done, err := store.ScanPrefix(ctx, []byte("asb:"), cursor, uint32(batchSize))
		if err != nil {
			return facts, applied, err
		}
		for _, item := range items {
			build, observedCommit, err := sourceBindingFact(ctx, store, config, item.Key, item.Value)
			if err != nil {
				return facts, applied, err
			}
			batch = append(batch, build)
			facts++
			if observedCommit > applied {
				applied = observedCommit
			}
			lastKey = append(lastKey[:0], item.Key...)
			if len(batch) == batchSize {
				if err := consume(batch, lastKey); err != nil {
					return facts, applied, err
				}
				batch = batch[:0]
			}
		}
		if done {
			break
		}
		if len(items) == 0 {
			return facts, applied, fmt.Errorf("authoritative source scan returned an empty continuation page")
		}
		cursor = append(append(cursor[:0], items[len(items)-1].Key...), 0)
	}
	if len(batch) != 0 {
		if err := consume(batch, lastKey); err != nil {
			return facts, applied, err
		}
	}
	if head, available, err := authoritativeHead(ctx, store); err != nil {
		return facts, applied, err
	} else if available && head > applied {
		applied = head
	}
	return facts, applied, nil
}

func sourceBindingFact(ctx context.Context, store Store, config Config, keyBytes, bindingBytes []byte) (buildFact, uint64, error) {
	key, err := schema.ParseKey(keyBytes)
	if err != nil || key.Kind != schema.KeyAuthoritativeSourceBinding {
		return buildFact{}, 0, fmt.Errorf("invalid authoritative source binding key %x", keyBytes)
	}
	binding, err := schema.UnmarshalAuthoritativeSourceBindingRecord(bindingBytes)
	if err != nil {
		return buildFact{}, 0, err
	}
	value, found, err := store.Get(ctx, schema.InodeRevisionKey(key.FSID, key.Inode, binding.Revision))
	if err != nil || !found {
		return buildFact{}, 0, errors.Join(
			err,
			fmt.Errorf("source binding points to missing revision %d:%d:%d", key.FSID, key.Inode, binding.Revision),
		)
	}
	revision, err := schema.UnmarshalInodeRevision(value)
	if err != nil {
		return buildFact{}, 0, err
	}
	fact := makeFact(schema.ParsedKey{FSID: key.FSID, Inode: key.Inode, Revision: binding.Revision}, revision, config)
	fact.IdentityGeneration, fact.IdentityContinuity = binding.Generation, binding.Continuity
	retained, lastComplete, err := sourceBindingResidency(ctx, store, key, binding, &fact)
	if err != nil {
		return buildFact{}, 0, err
	}
	identity := segmentIdentity{
		FSID: key.FSID, Inode: key.Inode, Generation: binding.Generation, Revision: binding.Revision, Known: fact.Known,
	}
	return buildFact{identity: identity, fact: fact, retainedRefs: retained, lastComplete: lastComplete}, binding.LastObservedCommit, nil
}

func sourceBindingResidency(
	ctx context.Context,
	store Store,
	key schema.ParsedKey,
	binding schema.AuthoritativeSourceBindingRecord,
	fact *schema.AnalyticsFactRecord,
) (uint64, int64, error) {
	switch binding.State {
	case schema.AuthoritativeSourceLive:
		fact.Residency = schema.AnalyticsLive
		return 0, 0, nil
	case schema.AuthoritativeSourceDeleted:
		proofValue, found, err := store.Get(ctx, schema.AuthoritativeCrawlProofKey(key.ID, binding.LastObservedCommit))
		if err != nil || !found {
			return 0, 0, errors.Join(err, fmt.Errorf("deleted source binding has no crawl proof"))
		}
		proof, err := schema.UnmarshalAuthoritativeCrawlProofRecord(proofValue)
		if err != nil || !proof.Complete || !proof.DebtFree {
			return 0, 0, errors.Join(err, fmt.Errorf("deleted source binding has invalid crawl proof"))
		}
		retained, err := retainedReferencesForIdentity(ctx, store, key.FSID, key.Inode, binding.Generation)
		if err != nil {
			return 0, 0, err
		}
		fact.Residency = schema.AnalyticsExpired
		if retained != 0 {
			fact.Residency = schema.AnalyticsArchiveOnly
		}
		return retained, proof.CompletedAt, nil
	default:
		fact.Residency = schema.AnalyticsUnknown
		return 0, 0, nil
	}
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func streamLegacyRevisions(
	ctx context.Context,
	store Store,
	config Config,
	afterKey []byte,
	batchSize int,
	consume func([]buildFact, []byte) error,
) (uint64, uint64, error) {
	var batch []buildFact
	var cursor, lastKey []byte
	var previousFSID uint32
	var previousInode, facts, applied uint64
	if len(afterKey) != 0 {
		cursor = append(append([]byte(nil), afterKey...), 0)
		if parsed, err := schema.ParseKey(afterKey); err == nil {
			previousFSID, previousInode = parsed.FSID, parsed.Inode
		}
	}
	for {
		items, done, err := store.ScanPrefix(ctx, []byte("iv:"), cursor, uint32(batchSize+1))
		if err != nil {
			return facts, applied, err
		}
		for _, item := range items {
			key, err := schema.ParseKey(item.Key)
			if err != nil || key.Kind != schema.KeyInodeRevision {
				return facts, applied, fmt.Errorf("invalid inode revision key %x", item.Key)
			}
			if key.Revision > applied {
				applied = key.Revision
			}
			if key.FSID == previousFSID && key.Inode == previousInode {
				lastKey = append(lastKey[:0], item.Key...)
				continue
			}
			if len(batch) == batchSize {
				if err := consume(batch, lastKey); err != nil {
					return facts, applied, err
				}
				batch = batch[:0]
			}
			previousFSID, previousInode = key.FSID, key.Inode
			lastKey = append(lastKey[:0], item.Key...)
			revision, err := schema.UnmarshalInodeRevision(item.Value)
			if err != nil {
				return facts, applied, err
			}
			fact := makeFact(key, revision, config)
			fact.IdentityGeneration, fact.IdentityContinuity = key.Revision, schema.AnalyticsContinuityUnknown
			if _, found, err := store.Get(ctx, schema.CurrentInodeKey(key.FSID, key.Inode)); err != nil {
				return facts, applied, err
			} else if found {
				fact.Residency = schema.AnalyticsLive
			} else {
				fact.Residency = schema.AnalyticsUnknown
			}
			batch = append(
				batch,
				buildFact{
					identity: segmentIdentity{
						FSID:       key.FSID,
						Inode:      key.Inode,
						Generation: key.Revision,
						Revision:   key.Revision,
						Known:      fact.Known,
					},
					fact: fact,
				},
			)
			facts++
		}
		if done {
			break
		}
		if len(items) == 0 {
			return facts, applied, fmt.Errorf("inode revision scan returned an empty continuation page")
		}
		cursor = append(append(cursor[:0], items[len(items)-1].Key...), 0)
	}
	if len(batch) != 0 {
		if err := consume(batch, lastKey); err != nil {
			return facts, applied, err
		}
	}
	if head, available, err := authoritativeHead(ctx, store); err != nil {
		return facts, applied, err
	} else if available && head > applied {
		applied = head
	}
	return facts, applied, nil
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func retainedReferencesForIdentity(
	ctx context.Context,
	store Store,
	fsid uint32,
	inode, generation uint64,
) (uint64, error) {
	nextGeneration := uint64(0)
	if err := scan(ctx, store, []byte("asb:"), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		if key.FSID != fsid || key.Inode != inode {
			return nil
		}
		binding, err := schema.UnmarshalAuthoritativeSourceBindingRecord(kv.Value)
		if err != nil {
			return err
		}
		if binding.Generation > generation && (nextGeneration == 0 || binding.Generation < nextGeneration) {
			nextGeneration = binding.Generation
		}
		return nil
	}); err != nil {
		return 0, err
	}
	var references uint64
	err := scan(ctx, store, []byte("s:"), func(kv daemon.KeyValue) error {
		snapshot, err := schema.UnmarshalSnapshotRecord(kv.Value)
		if err != nil {
			return err
		}
		var visit func([]byte, int) (bool, error)
		visit = func(key []byte, depth int) (bool, error) {
			if depth > 1024 {
				return false, fmt.Errorf("snapshot directory depth exceeds analytics limit")
			}
			value, found, err := store.Get(ctx, key)
			if err != nil || !found {
				return false, errors.Join(err, fmt.Errorf("snapshot directory %x is missing", key))
			}
			directory, err := schema.UnmarshalDirectoryRevision(value)
			if err != nil {
				return false, err
			}
			for _, child := range directory.Children {
				parsed, err := schema.ParseKey(child.MetadataKey)
				if err != nil {
					return false, err
				}
				if parsed.Kind == schema.KeyDirectoryRevision {
					if found, err := visit(child.MetadataKey, depth+1); err != nil || found {
						return found, err
					}
				} else if parsed.Kind == schema.KeyInodeRevision &&
					parsed.FSID == fsid &&
					parsed.Inode == inode &&
					parsed.Revision >= generation &&
					(nextGeneration == 0 ||
						parsed.Revision < nextGeneration) {
					return true, nil
				}
			}
			return false, nil
		}
		found, err := visit(
			schema.DirectoryRevisionKey(snapshot.RootFSID, snapshot.RootInode, snapshot.RootRevision),
			0,
		)
		if err != nil {
			return err
		}
		if found {
			references++
		}
		return nil
	})
	return references, err
}

func segmentDictionaries(ctx context.Context, store Store, facts []buildFact) (dictionaries, []daemon.Mutation, error) {
	result := dictionaries{ids: map[schema.AnalyticsDictionaryKind]map[string]uint32{}}
	var puts []daemon.Mutation
	for _, item := range facts {
		for _, entry := range []struct {
			kind  schema.AnalyticsDictionaryKind
			value string
		}{{schema.AnalyticsDictionarySVM,
			item.fact.SVM},
			{schema.AnalyticsDictionaryVolume,
				item.fact.Volume},
			{schema.AnalyticsDictionaryPathGroup,
				item.fact.PathGroup}} {
			if entry.value == "" || entry.value == "unknown" {
				continue
			}
			if result.ids[entry.kind] == nil {
				result.ids[entry.kind] = map[string]uint32{}
			}
			if result.ids[entry.kind][entry.value] != 0 {
				continue
			}
			digest := sha256.Sum256([]byte(entry.value))
			id := binary.BigEndian.Uint32(digest[:4])
			if id == 0 {
				id = 1
			}
			key := schema.AnalyticsDictionaryKey(entry.kind, id)
			if value, found, err := store.Get(ctx, key); err != nil {
				return dictionaries{}, nil, err
			} else if found {
				record, err := schema.UnmarshalAnalyticsDictionaryRecord(value)
				if err != nil || record.Value != entry.value {
					return dictionaries{}, nil, errors.Join(err, fmt.Errorf("analytics dictionary hash collision for %q", entry.value))
				}
			} else {
				encoded, err := (schema.AnalyticsDictionaryRecord{Value: entry.value}).MarshalBinary()
				if err != nil {
					return dictionaries{}, nil, err
				}
				puts = append(puts, daemon.Mutation{Key: key, Value: encoded})
			}
			result.ids[entry.kind][entry.value] = id
		}
	}
	return result, puts, nil
}

func writeSegmentDerived(
	ctx context.Context,
	store Store,
	generation, parentGeneration, segment uint64,
	facts []buildFact,
) error {
	puts := make([]daemon.Mutation, 0, len(facts))
	for row, item := range facts {
		overlay := schema.AnalyticsResidencyRecord{
			State:                item.fact.Residency,
			LastCompleteCrawl:    item.lastComplete,
			RetainedSnapshotRefs: item.retainedRefs,
			ClassificationEpoch:  generation,
			FactSegment:          segment,
			Row:                  uint32(row),
		}
		encoded, err := overlay.MarshalBinary()
		if err != nil {
			return err
		}
		key := schema.AnalyticsResidencyKey(item.identity.FSID, item.identity.Inode, item.identity.Generation)
		puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, key), Value: encoded})
	}
	if err := writeBatches(ctx, store, puts); err != nil {
		return fmt.Errorf("write analytics candidate overlays: %w", err)
	}
	return writeCandidateViews(ctx, store, generation, parentGeneration, facts)
}

func visitInodeContent(
	ctx context.Context,
	store Store,
	record schema.InodeRevision,
	visit func(uint32, schema.ID) error,
) error {
	switch record.ContentMode {
	case schema.ContentNone:
		return nil
	case schema.ContentInline:
		for ordinal, id := range record.ContentIDs {
			if err := visit(uint32(ordinal), id); err != nil {
				return err
			}
		}
		return nil
	case schema.ContentManifestRef:
		var ordinal uint32
		for segment := uint32(0); ; segment++ {
			value, found, err := store.Get(ctx, schema.ContentManifestKey(record.ContentManifestID, segment))
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("missing content manifest %x segment %d", record.ContentManifestID, segment)
			}
			manifest, err := schema.UnmarshalContentManifest(value)
			if err != nil {
				return err
			}
			for _, id := range manifest.ContentIDs {
				if err := visit(ordinal, id); err != nil {
					return err
				}
				ordinal++
			}
			if segment+1 == manifest.SegmentCount {
				return nil
			}
		}
	default:
		return fmt.Errorf("unknown inode content mode %d", record.ContentMode)
	}
}
