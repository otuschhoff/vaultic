package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

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

func collectAuthoritativeFacts(ctx context.Context, store Store, config Config) ([]buildFact, uint64, error) {
	sourceBindings, bindingCommit, err := collectSourceEvidence(ctx, store)
	if err != nil {
		return nil, 0, err
	}
	retainedReferences, snapshotCommit, err := collectRetainedReferences(ctx, store, sourceBindings)
	if err != nil {
		return nil, 0, err
	}
	var facts []buildFact
	var currentKey struct {
		fsid  uint32
		inode uint64
	}
	var revisions []struct {
		key    schema.ParsedKey
		record schema.InodeRevision
	}
	var maxRevision uint64
	flush := func() error {
		if len(revisions) == 0 {
			return nil
		}
		currentRevision := uint64(0)
		if value, found, err := store.Get(ctx, schema.CurrentInodeKey(currentKey.fsid, currentKey.inode)); err != nil {
			return err
		} else if found {
			pointer, err := schema.UnmarshalCurrentPointer(value)
			if err != nil {
				return fmt.Errorf("current inode %d:%d: %w", currentKey.fsid, currentKey.inode, err)
			}
			currentRevision = pointer.Revision
			matched := false
			for _, revision := range revisions {
				if revision.key.Revision == currentRevision {
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("current inode %d:%d points to missing revision %d", currentKey.fsid, currentKey.inode, currentRevision)
			}
		}
		evidence := sourceBindings[currentKey]
		if len(evidence) == 0 {
			first := revisions[0]
			evidence = []sourceEvidence{
				{
					generation: first.key.Revision,
					revision:   first.key.Revision,
					state:      schema.AuthoritativeSourceUnknown,
					continuity: schema.AnalyticsContinuityUnknown,
				},
			}
			if currentRevision != 0 {
				evidence[0].state = schema.AuthoritativeSourceLive
			}
		}
		for _, source := range evidence {
			var selected *struct {
				key    schema.ParsedKey
				record schema.InodeRevision
			}
			for index := range revisions {
				if revisions[index].key.Revision == source.revision {
					selected = &revisions[index]
					break
				}
			}
			if selected == nil {
				return fmt.Errorf(
					"source binding %d:%d generation %d points to missing revision %d",
					currentKey.fsid,
					currentKey.inode,
					source.generation,
					source.revision,
				)
			}
			fact := makeFact(selected.key, selected.record, config)
			fact.IdentityGeneration = source.generation
			fact.IdentityContinuity = source.continuity
			identity := segmentIdentity{
				FSID:       currentKey.fsid,
				Inode:      currentKey.inode,
				Generation: source.generation,
				Revision:   source.revision,
				Known:      fact.Known,
			}
			membershipIdentity := segmentIdentity{
				FSID:       identity.FSID,
				Inode:      identity.Inode,
				Generation: identity.Generation,
				Revision:   identity.Generation,
			}
			retained := retainedReferences[membershipIdentity]
			switch source.state {
			case schema.AuthoritativeSourceLive:
				fact.Residency = schema.AnalyticsLive
			case schema.AuthoritativeSourceDeleted:
				if retained > 0 {
					fact.Residency = schema.AnalyticsArchiveOnly
				} else {
					fact.Residency = schema.AnalyticsExpired
				}
			default:
				fact.Residency = schema.AnalyticsUnknown
			}
			facts = append(
				facts,
				buildFact{identity: identity, fact: fact, retainedRefs: retained, lastComplete: source.lastComplete},
			)
		}
		revisions = revisions[:0]
		return nil
	}
	err = scan(ctx, store, []byte("iv:"), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil || key.Kind != schema.KeyInodeRevision {
			return fmt.Errorf("invalid inode revision key %x", kv.Key)
		}
		if len(revisions) != 0 && (key.FSID != currentKey.fsid || key.Inode != currentKey.inode) {
			if err := flush(); err != nil {
				return err
			}
		}
		currentKey.fsid, currentKey.inode = key.FSID, key.Inode
		record, err := schema.UnmarshalInodeRevision(kv.Value)
		if err != nil {
			return fmt.Errorf("inode revision %d:%d:%d: %w", key.FSID, key.Inode, key.Revision, err)
		}
		revisions = append(revisions, struct {
			key    schema.ParsedKey
			record schema.InodeRevision
		}{key, record})
		if key.Revision > maxRevision {
			maxRevision = key.Revision
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if err := flush(); err != nil {
		return nil, 0, err
	}
	if snapshotCommit > maxRevision {
		maxRevision = snapshotCommit
	}
	if bindingCommit > maxRevision {
		maxRevision = bindingCommit
	}
	if head, available, err := authoritativeHead(ctx, store); err != nil {
		return nil, 0, err
	} else if available && head > maxRevision {
		maxRevision = head
	}
	return facts, maxRevision, nil
}

func collectSourceEvidence(ctx context.Context, store Store) (map[struct {
	fsid  uint32
	inode uint64
}][]sourceEvidence, uint64, error) {
	type inodeIdentity struct {
		fsid  uint32
		inode uint64
	}
	byGeneration := map[inodeIdentity]map[uint64]sourceEvidence{}
	var maxCommit uint64
	err := scan(ctx, store, []byte("asb:"), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil || key.Kind != schema.KeyAuthoritativeSourceBinding {
			return fmt.Errorf("invalid authoritative source binding key %x", kv.Key)
		}
		binding, err := schema.UnmarshalAuthoritativeSourceBindingRecord(kv.Value)
		if err != nil {
			return err
		}
		evidence := sourceEvidence{
			generation: binding.Generation,
			revision:   binding.Revision,
			state:      binding.State,
			continuity: binding.Continuity,
			commit:     binding.LastObservedCommit,
		}
		if binding.State == schema.AuthoritativeSourceDeleted {
			value, found, err := store.Get(ctx, schema.AuthoritativeCrawlProofKey(key.ID, binding.LastObservedCommit))
			if err != nil || !found {
				return errors.Join(err, fmt.Errorf("deleted source binding has no crawl proof"))
			}
			proof, err := schema.UnmarshalAuthoritativeCrawlProofRecord(value)
			if err != nil || !proof.Complete || !proof.DebtFree {
				return errors.Join(err, fmt.Errorf("deleted source binding has invalid crawl proof"))
			}
			evidence.lastComplete = proof.CompletedAt
		}
		identity := inodeIdentity{key.FSID, key.Inode}
		if byGeneration[identity] == nil {
			byGeneration[identity] = map[uint64]sourceEvidence{}
		}
		prior, found := byGeneration[identity][binding.Generation]
		if !found || sourceStatePriority(evidence.state) > sourceStatePriority(prior.state) ||
			sourceStatePriority(evidence.state) == sourceStatePriority(prior.state) && evidence.commit > prior.commit {
			byGeneration[identity][binding.Generation] = evidence
		}
		if binding.LastObservedCommit > maxCommit {
			maxCommit = binding.LastObservedCommit
		}
		return nil
	})
	result := map[struct {
		fsid  uint32
		inode uint64
	}][]sourceEvidence{}
	for identity, generations := range byGeneration {
		key := struct {
			fsid  uint32
			inode uint64
		}{identity.fsid, identity.inode}
		for _, evidence := range generations {
			result[key] = append(result[key], evidence)
		}
		sort.Slice(result[key], func(i, j int) bool { return result[key][i].generation < result[key][j].generation })
	}
	return result, maxCommit, err
}

func sourceStatePriority(state schema.AuthoritativeSourceState) int {
	switch state {
	case schema.AuthoritativeSourceLive:
		return 3
	case schema.AuthoritativeSourceUnknown:
		return 2
	default:
		return 1
	}
}

func collectRetainedReferences(ctx context.Context, store Store, sourceBindings map[struct {
	fsid  uint32
	inode uint64
}][]sourceEvidence) (map[segmentIdentity]uint64, uint64, error) {
	result := map[segmentIdentity]uint64{}
	var maxCommit uint64
	err := scan(ctx, store, []byte("s:"), func(kv daemon.KeyValue) error {
		snapshot, err := schema.UnmarshalSnapshotRecord(kv.Value)
		if err != nil {
			return err
		}
		if snapshot.CommitSequence > maxCommit {
			maxCommit = snapshot.CommitSequence
		}
		seenDirectories := map[string]struct{}{}
		seenIdentities := map[segmentIdentity]struct{}{}
		var visit func([]byte) error
		visit = func(key []byte) error {
			if _, seen := seenDirectories[string(key)]; seen {
				return nil
			}
			seenDirectories[string(key)] = struct{}{}
			value, found, err := store.Get(ctx, key)
			if err != nil || !found {
				return errors.Join(err, fmt.Errorf("snapshot directory %x is missing", key))
			}
			directory, err := schema.UnmarshalDirectoryRevision(value)
			if err != nil {
				return err
			}
			for _, child := range directory.Children {
				parsed, err := schema.ParseKey(child.MetadataKey)
				if err != nil {
					return err
				}
				if parsed.Kind == schema.KeyDirectoryRevision {
					if err := visit(child.MetadataKey); err != nil {
						return err
					}
					continue
				}
				if parsed.Kind != schema.KeyInodeRevision {
					continue
				}
				generationRevision := uint64(0)
				for _, evidence := range sourceBindings[struct {
					fsid  uint32
					inode uint64
				}{parsed.FSID, parsed.Inode}] {
					if evidence.generation <= parsed.Revision && evidence.generation > generationRevision {
						generationRevision = evidence.generation
					}
				}
				if generationRevision == 0 {
					items, _, err := store.ScanPrefix(
						ctx,
						schema.InodeRevisionPrefix(parsed.FSID, parsed.Inode),
						nil,
						1,
					)
					if err != nil || len(items) == 0 {
						return errors.Join(
							err,
							fmt.Errorf("snapshot inode %d:%d has no revision", parsed.FSID, parsed.Inode),
						)
					}
					generation, err := schema.ParseKey(items[0].Key)
					if err != nil {
						return err
					}
					generationRevision = generation.Revision
				}
				identity := segmentIdentity{
					FSID: parsed.FSID, Inode: parsed.Inode,
					Generation: generationRevision, Revision: generationRevision,
				}
				seenIdentities[identity] = struct{}{}
			}
			return nil
		}
		root := schema.DirectoryRevisionKey(snapshot.RootFSID, snapshot.RootInode, snapshot.RootRevision)
		if err := visit(root); err != nil {
			return err
		}
		for identity := range seenIdentities {
			result[identity]++
		}
		return nil
	})
	return result, maxCommit, err
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
