package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

var (
	analyticsZstdEncoder = mustAnalyticsZstdEncoder()
	analyticsZstdDecoder = mustAnalyticsZstdDecoder()
)

func mustAnalyticsZstdEncoder() *zstd.Encoder {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
	if err != nil {
		// Fixed options must initialize before package use.
		panic(err) //nolint:forbidigo // Fixed encoder options must initialize before package use.
	}
	return encoder
}

func mustAnalyticsZstdDecoder() *zstd.Decoder {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		// Fixed options must initialize before package use.
		panic(err) //nolint:forbidigo // Fixed decoder options must initialize before package use.
	}
	return decoder
}

type segmentIdentity struct {
	FSID       uint32 `json:"f"`
	Inode      uint64 `json:"i"`
	Generation uint64 `json:"g"`
	Revision   uint64 `json:"r"`
	Known      uint16 `json:"k"`
}

type analyticsIdentityKey struct {
	fsid       uint32
	inode      uint64
	generation uint64
}

type segmentRows struct {
	Identity   []segmentIdentity
	UID        []uint32
	GID        []uint32
	CreatedAt  []int64
	Basis      []schema.AnalyticsCreationBasis
	Continuity []schema.AnalyticsIdentityContinuity
	Year       []int32
	Month      []uint8
	ISOYear    []int32
	Workweek   []uint8
	SVM        []uint32
	Volume     []uint32
	PathGroup  []uint32
	Size       []uint64
	SizeLog10  []uint8
}

type buildFact struct {
	identity     segmentIdentity
	fact         schema.AnalyticsFactRecord
	retainedRefs uint64
	lastComplete int64
}

type pinnedGeneration struct {
	epoch     uint64
	manifest  schema.AnalyticsManifestRecord
	watermark schema.AnalyticsWatermarkRecord
}

type CatchUpOptions struct {
	MaxDeltas uint32
}

type CatchUpResult struct {
	Processed           uint32 `json:"processed"`
	PeakDeltasBuffered  uint32 `json:"peak_deltas_buffered,omitempty"`
	PeakWorkingSetBytes uint64 `json:"peak_working_set_bytes,omitempty"`
	AppliedCommit       uint64 `json:"applied_commit"`
	AuthoritativeHead   uint64 `json:"authoritative_head,omitempty"`
	LagCommits          uint64 `json:"lag_commits,omitempty"`
	Current             bool   `json:"current"`
}

const maxManifestLayerDepth = 8

var analyticsDerivedTombstone = []byte{0xff, 'A', 'T'}

func CatchUpStatus(ctx context.Context, store Store) (CatchUpResult, error) {
	return catchUpStatus(ctx, store, 0)
}

// CatchUp consumes a bounded ordered prefix of the analytics outbox. Derived
// state is rebuilt and published before any covered delta is reclaimed.
func CatchUp(ctx context.Context, store Store, options CatchUpOptions) (CatchUpResult, error) {
	unlock, err := lockAnalyticsPublication(store)
	if err != nil {
		return CatchUpResult{}, err
	}
	defer unlock()
	return catchUp(ctx, store, options)
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func catchUp(ctx context.Context, store Store, options CatchUpOptions) (CatchUpResult, error) {
	metadata, err := Status(ctx, store)
	if err != nil {
		return CatchUpResult{}, err
	}
	if !metadata.Enabled {
		return catchUpStatus(ctx, store, 0)
	}
	var config Config
	if metadata.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(metadata.ConfigJSON), &config); err != nil {
			return CatchUpResult{}, fmt.Errorf("decode analytics config: %w", err)
		}
	}
	config = config.normalized()
	limit := options.MaxDeltas
	if limit == 0 || limit > uint32(config.SegmentRows) {
		limit = uint32(config.SegmentRows)
	}
	items, _, err := store.ScanPrefix(ctx, schema.AnalyticsDeltaPrefix(), nil, limit)
	if err != nil {
		return CatchUpResult{}, err
	}
	if len(items) == 0 {
		return catchUpStatus(ctx, store, 0)
	}
	deletes := make([][]byte, 0, len(items))
	deltas := make([]schema.AnalyticsDeltaRecord, 0, len(items))
	var lastCommit uint64
	for _, item := range items {
		parsed, parseErr := schema.ParseKey(item.Key)
		if parseErr != nil || parsed.Kind != schema.KeyAnalyticsDelta {
			return CatchUpResult{}, fmt.Errorf("invalid analytics outbox key %x", item.Key)
		}
		delta, decodeErr := schema.UnmarshalAnalyticsDeltaRecord(item.Value)
		if decodeErr != nil {
			return CatchUpResult{}, fmt.Errorf("analytics outbox %d:%d: %w", parsed.Commit, parsed.Ordinal, decodeErr)
		}
		deltas = append(deltas, delta)
		if parsed.Commit < lastCommit {
			return CatchUpResult{}, fmt.Errorf("analytics outbox is not in commit order")
		}
		lastCommit = parsed.Commit
		deletes = append(deletes, append([]byte(nil), item.Key...))
	}

	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if pinned, found, pinErr := pinLatest(ctx, store); pinErr != nil {
		return CatchUpResult{}, pinErr
	} else if !found || pinned.watermark.AppliedCommit < lastCommit {
		metadata, statusErr := Status(ctx, store)
		if statusErr != nil {
			return CatchUpResult{}, statusErr
		}
		if found && pinned.manifest.LayerDepth < maxManifestLayerDepth {
			err = publishDeltaGeneration(ctx, store, metadata, config, pinned, deltas, lastCommit)
		} else {
			_, err = rebuild(ctx, store, config, false)
		}
		if err != nil {
			return CatchUpResult{}, fmt.Errorf("catch up analytics: %w", err)
		}
	}
	pinned, found, err := pinLatest(ctx, store)
	if err != nil || !found {
		return CatchUpResult{}, errors.Join(err, fmt.Errorf("analytics catch-up published no durable watermark"))
	}
	for index, item := range items {
		parsed, _ := schema.ParseKey(item.Key)
		if parsed.Commit > pinned.watermark.AppliedCommit {
			deletes = deletes[:index]
			break
		}
	}
	if len(deletes) != 0 {
		if err := store.WriteMutableBatch(ctx, nil, deletes, true); err != nil {
			return CatchUpResult{}, fmt.Errorf("reclaim applied analytics outbox: %w", err)
		}
	}
	result, err := catchUpStatus(ctx, store, uint32(len(deletes)))
	result.PeakDeltasBuffered = uint32(len(items))
	result.PeakWorkingSetBytes = uint64(len(items)) * uint64(160)
	return result, err
}

func publishDeltaGeneration(
	ctx context.Context,
	store Store,
	metadata schema.AnalyticsMetadataRecord,
	config Config,
	parent pinnedGeneration,
	deltas []schema.AnalyticsDeltaRecord,
	appliedCommit uint64,
) error {
	generation := metadata.Generation + 1
	if generation == 0 {
		return fmt.Errorf("analytics generation overflow")
	}
	if err := cleanupCandidateGeneration(ctx, store, generation); err != nil {
		return fmt.Errorf("clean orphaned analytics delta generation: %w", err)
	}
	facts, previous, affected, err := collectDeltaFacts(ctx, store, metadata.Generation, config, parent, deltas)
	if err != nil {
		return err
	}
	segment := generation<<32 | 1
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if len(facts) != 0 {
		dictionaries, dictionaryPuts, err := segmentDictionaries(ctx, store, facts)
		if err != nil {
			return err
		}
		if err := writeBatches(ctx, store, dictionaryPuts); err != nil {
			return err
		}
		puts, err := buildSegment(segment, generation, facts, dictionaries)
		if err != nil {
			return err
		}
		if err := writeBatches(ctx, store, puts); err != nil {
			return err
		}
		if err := writeSegmentDerived(ctx, store, generation, parent.epoch, segment, facts); err != nil {
			return err
		}
		oldFacts := make([]buildFact, 0, len(previous))
		for _, old := range previous {
			oldFacts = append(oldFacts, old)
		}
		if err := subtractCandidateViews(ctx, store, generation, parent.epoch, oldFacts); err != nil {
			return err
		}
		if err := writeDerivedTombstones(ctx, store, generation, previous, affected); err != nil {
			return err
		}
	}
	segments := []uint64(nil)
	if len(facts) != 0 {
		segments = []uint64{segment}
	}
	manifestValue,
		err := (schema.AnalyticsManifestRecord{Generation: generation,
		ParentGeneration: parent.epoch,
		LayerDepth:       parent.manifest.LayerDepth + 1,
		Segments:         segments}).MarshalBinary()
	if err != nil {
		return err
	}
	builtAt := time.Now().UnixNano()
	watermarkValue,
		err := (schema.AnalyticsWatermarkRecord{RepositoryGeneration: generation,
		AppliedCommit:      appliedCommit,
		ManifestGeneration: generation,
		AppliedAt:          builtAt}).MarshalBinary()
	if err != nil {
		return err
	}
	metadata.Generation, metadata.BuiltAt = generation, builtAt
	metadataValue, err := metadata.MarshalBinary()
	if err != nil {
		return err
	}
	publication := []daemon.Mutation{
		{Key: schema.AnalyticsDerivedGenerationMarkerKey(generation), Value: []byte{schema.Version}},
		{Key: schema.AnalyticsManifestKey(generation), Value: manifestValue},
		{Key: schema.AnalyticsWatermarkKey(generation), Value: watermarkValue},
		{Key: schema.AnalyticsMetadataKey(), Value: metadataValue},
	}
	return store.WriteMutableBatch(ctx, publication, nil, true)
}

func collectDeltaFacts(
	ctx context.Context,
	store Store,
	classificationEpoch uint64,
	config Config,
	parent pinnedGeneration,
	deltas []schema.AnalyticsDeltaRecord,
) ([]buildFact, map[analyticsIdentityKey]buildFact, map[analyticsIdentityKey]buildFact, error) {
	order := make([]analyticsIdentityKey, 0, len(deltas))
	affected := make(map[analyticsIdentityKey]buildFact, len(deltas))
	previous := make(map[analyticsIdentityKey]buildFact, len(deltas))
	for _, delta := range deltas {
		if delta.ClassificationEpoch != classificationEpoch {
			return nil, nil, nil, fmt.Errorf(
				"analytics classification epoch changed from %d to %d; streaming rebuild required",
				delta.ClassificationEpoch,
				classificationEpoch,
			)
		}
		key := analyticsIdentityKey{delta.FSID, delta.Inode, delta.IdentityGeneration}
		current, present := affected[key]
		item, first, old, err := applyAnalyticsDelta(ctx, store, config, parent, current, delta, !present)
		if err != nil {
			return nil, nil, nil, err
		}
		if first {
			order = append(order, key)
			if old != nil {
				previous[key] = *old
			}
		}
		affected[key] = item
	}
	facts := make([]buildFact, 0, len(order))
	for _, key := range order {
		facts = append(facts, affected[key])
	}
	return facts, previous, affected, nil
}

func applyAnalyticsDelta(
	ctx context.Context,
	store Store,
	config Config,
	parent pinnedGeneration,
	item buildFact,
	delta schema.AnalyticsDeltaRecord,
	first bool,
) (buildFact, bool, *buildFact, error) {
	var old *buildFact
	if first {
		loaded, found, err := loadActiveBuildFact(ctx, store, parent, delta.FSID, delta.Inode, delta.IdentityGeneration)
		if err != nil {
			return item, first, nil, err
		}
		if found {
			item, old = loaded, &loaded
		} else {
			item.identity = segmentIdentity{
				FSID: delta.FSID, Inode: delta.Inode, Generation: delta.IdentityGeneration,
				Revision: delta.Revision, Known: delta.Known,
			}
		}
	}
	if delta.Kind == schema.AnalyticsDeltaCreation || delta.Kind == schema.AnalyticsDeltaClassification {
		value, found, err := store.Get(ctx, schema.InodeRevisionKey(delta.FSID, delta.Inode, delta.Revision))
		if err != nil || !found {
			return item, first, old, errors.Join(
				err,
				fmt.Errorf("analytics delta points to missing inode revision %d:%d:%d", delta.FSID, delta.Inode, delta.Revision),
			)
		}
		revision, err := schema.UnmarshalInodeRevision(value)
		if err != nil {
			return item, first, old, err
		}
		item.identity.Revision, item.identity.Known = delta.Revision, delta.Known
		item.fact = makeFact(schema.ParsedKey{FSID: delta.FSID, Inode: delta.Inode, Revision: delta.Revision}, revision, config)
		item.fact.Revision, item.fact.UID, item.fact.GID, item.fact.Known = delta.Revision, delta.UID, delta.GID, delta.Known
		item.fact.CreatedAt, item.fact.LogicalSize = delta.CreatedAt, delta.LogicalSize
		item.fact.Residency, item.fact.CreationBasis = delta.State, delta.CreationBasis
		item.fact.IdentityGeneration, item.fact.IdentityContinuity = delta.IdentityGeneration, delta.IdentityContinuity
		populateFactCalendar(&item.fact)
		item.retainedRefs = delta.RetainedSnapshotRefs
		return item, first, old, nil
	}
	if delta.Kind == schema.AnalyticsDeltaSourceState {
		item.fact.Residency = delta.State
		if delta.IdentityContinuity != schema.AnalyticsContinuityUnknown {
			item.fact.IdentityContinuity = delta.IdentityContinuity
		}
		return item, first, old, nil
	}
	if err := applyReferenceDelta(&item, delta); err != nil {
		return item, first, old, err
	}
	return item, first, old, nil
}

func applyReferenceDelta(item *buildFact, delta schema.AnalyticsDeltaRecord) error {
	switch delta.ReferenceOperation {
	case schema.AnalyticsReferencesIncrement:
		item.retainedRefs++
	case schema.AnalyticsReferencesDecrement:
		if item.retainedRefs == 0 {
			return fmt.Errorf("analytics retained-reference underflow for %d:%d:%d", delta.FSID, delta.Inode, delta.IdentityGeneration)
		}
		item.retainedRefs--
	case schema.AnalyticsReferencesSet:
		item.retainedRefs = delta.RetainedSnapshotRefs
	default:
		if delta.RetainedSnapshotRefs == 0 {
			if item.retainedRefs == 0 {
				return fmt.Errorf("analytics retained-reference underflow for legacy delta %d:%d:%d", delta.FSID, delta.Inode, delta.IdentityGeneration)
			}
			item.retainedRefs--
		} else {
			item.retainedRefs += delta.RetainedSnapshotRefs
		}
	}
	if item.fact.Residency == schema.AnalyticsExpired && item.retainedRefs > 0 {
		item.fact.Residency = schema.AnalyticsArchiveOnly
	} else if item.fact.Residency == schema.AnalyticsArchiveOnly && item.retainedRefs == 0 {
		item.fact.Residency = schema.AnalyticsExpired
	}
	return nil
}

func subtractCandidateViews(
	ctx context.Context,
	store Store,
	generation, parentGeneration uint64,
	facts []buildFact,
) error {
	aggregates := map[string]schema.AnalyticsAggregateRecord{}
	summaries := map[string]schema.AnalyticsSummaryRecord{}
	addAggregate := func(key []byte, size uint64, deletion bool) {
		value := aggregates[string(key)]
		if deletion {
			value.FilesDeleted++
			value.BytesDeleted += size
		} else {
			value.FilesAdded++
			value.BytesAdded += size
		}
		aggregates[string(key)] = value
	}
	for _, item := range facts {
		addFactToCandidateViews(item, addAggregate, summaries)
	}
	puts, err := subtractAggregateMutations(ctx, store, generation, parentGeneration, aggregates)
	if err != nil {
		return err
	}
	summaryPuts, err := subtractSummaryMutations(ctx, store, generation, parentGeneration, summaries)
	if err != nil {
		return err
	}
	puts = append(puts, summaryPuts...)
	return writeBatches(ctx, store, puts)
}

func subtractAggregateMutations(
	ctx context.Context,
	store Store,
	generation, parentGeneration uint64,
	aggregates map[string]schema.AnalyticsAggregateRecord,
) ([]daemon.Mutation, error) {
	puts := make([]daemon.Mutation, 0, len(aggregates))
	for key, subtract := range aggregates {
		logicalKey := []byte(key)
		current, found, err := store.Get(ctx, schema.AnalyticsDerivedKey(generation, logicalKey))
		if err != nil {
			return nil, err
		}
		if !found {
			current, found, err = getActiveDerived(ctx, store, parentGeneration, logicalKey)
			if err != nil {
				return nil, err
			}
		}
		if !found {
			return nil, fmt.Errorf("analytics parent aggregate %x is missing", logicalKey)
		}
		record, err := schema.UnmarshalAnalyticsAggregateRecord(current)
		if err != nil || record.BytesAdded < subtract.BytesAdded || record.BytesDeleted < subtract.BytesDeleted ||
			record.FilesAdded < subtract.FilesAdded || record.FilesDeleted < subtract.FilesDeleted {
			return nil, errors.Join(err, fmt.Errorf("analytics aggregate subtraction underflow"))
		}
		record.BytesAdded -= subtract.BytesAdded
		record.BytesDeleted -= subtract.BytesDeleted
		record.FilesAdded -= subtract.FilesAdded
		record.FilesDeleted -= subtract.FilesDeleted
		value, err := record.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if record == (schema.AnalyticsAggregateRecord{}) {
			value = analyticsDerivedTombstone
		}
		puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, logicalKey), Value: value})
	}
	return puts, nil
}

func subtractSummaryMutations(
	ctx context.Context,
	store Store,
	generation, parentGeneration uint64,
	summaries map[string]schema.AnalyticsSummaryRecord,
) ([]daemon.Mutation, error) {
	puts := make([]daemon.Mutation, 0, len(summaries))
	for key, subtract := range summaries {
		logicalKey := []byte(key)
		current, found, err := store.Get(ctx, schema.AnalyticsDerivedKey(generation, logicalKey))
		if err != nil {
			return nil, err
		}
		if !found {
			current, found, err = getActiveDerived(ctx, store, parentGeneration, logicalKey)
			if err != nil {
				return nil, err
			}
		}
		if !found {
			return nil, fmt.Errorf("analytics parent summary %x is missing", logicalKey)
		}
		record, err := schema.UnmarshalAnalyticsSummaryRecord(current)
		if err != nil || record.ActiveBytes < subtract.ActiveBytes || record.ActiveFiles < subtract.ActiveFiles ||
			record.UniqueBlobCount < subtract.UniqueBlobCount || record.UniqueBlobBytes < subtract.UniqueBlobBytes {
			return nil, errors.Join(err, fmt.Errorf("analytics summary subtraction underflow"))
		}
		record.ActiveBytes -= subtract.ActiveBytes
		record.ActiveFiles -= subtract.ActiveFiles
		record.UniqueBlobCount -= subtract.UniqueBlobCount
		record.UniqueBlobBytes -= subtract.UniqueBlobBytes
		value, err := record.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if record == (schema.AnalyticsSummaryRecord{}) {
			value = analyticsDerivedTombstone
		}
		puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, logicalKey), Value: value})
	}
	return puts, nil
}

func addFactToCandidateViews(
	item buildFact,
	addAggregate func([]byte, uint64, bool),
	summaries map[string]schema.AnalyticsSummaryRecord,
) {
	fact := item.fact
	size := uint64(0)
	if fact.Known&schema.KnownSize != 0 {
		size = fact.LogicalSize
	}
	addBuckets := func(instant time.Time, deletion bool) {
		year := time.Date(instant.Year(), 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
		month := time.Date(instant.Year(), instant.Month(), 1, 0, 0, 0, 0, time.UTC).UnixNano()
		weekday := (int(instant.Weekday()) + 6) % 7
		week := time.Date(instant.Year(), instant.Month(), instant.Day()-weekday, 0, 0, 0, 0, time.UTC).UnixNano()
		for _, bucket := range []struct {
			granularity schema.AnalyticsGranularity
			timestamp   int64
		}{{schema.AnalyticsGranularityYear, year}, {schema.AnalyticsGranularityMonth, month}, {schema.AnalyticsGranularityWeek, week}} {
			addAggregate(schema.GrowthTimeKey(bucket.granularity, bucket.timestamp, schema.TierUnknown), size, deletion)
			if fact.PathGroup != "unknown" {
				addAggregate(schema.GrowthPathKey(fact.PathGroup, bucket.granularity, bucket.timestamp), size, deletion)
			}
			if fact.Known&schema.KnownUID != 0 {
				addAggregate(schema.UserChurnKey(fact.UID, bucket.granularity, bucket.timestamp), size, deletion)
			}
		}
	}
	if fact.CreationBasis != schema.AnalyticsTimeUnknown {
		addBuckets(time.Unix(0, fact.CreatedAt).UTC(), false)
	}
	if item.lastComplete != 0 && (fact.Residency == schema.AnalyticsArchiveOnly || fact.Residency == schema.AnalyticsExpired) {
		addBuckets(time.Unix(0, item.lastComplete).UTC(), true)
	}
	addFactSummary(
		summaries, fact.Known&schema.KnownUID != 0,
		schema.UserStatsKey(fact.UID, fact.Residency), schema.UserSummaryKey(fact.UID), fact.Residency, size,
	)
	addFactSummary(
		summaries, fact.Known&schema.KnownGID != 0,
		schema.GroupStatsKey(fact.GID, fact.Residency), schema.GroupSummaryKey(fact.GID), fact.Residency, size,
	)
}

func addFactSummary(
	summaries map[string]schema.AnalyticsSummaryRecord,
	known bool,
	statsKey, summaryKey []byte,
	residency schema.AnalyticsResidency,
	size uint64,
) {
	if !known {
		return
	}
	stats := summaries[string(statsKey)]
	stats.ActiveFiles++
	stats.ActiveBytes += size
	summaries[string(statsKey)] = stats
	if residency == schema.AnalyticsLive {
		summary := summaries[string(summaryKey)]
		summary.ActiveFiles++
		summary.ActiveBytes += size
		summaries[string(summaryKey)] = summary
	}
}

func writeDerivedTombstones[K comparable](
	ctx context.Context,
	store Store,
	generation uint64,
	previous, current map[K]buildFact,
) error {
	var puts []daemon.Mutation
	for key, old := range previous {
		updated := current[key]
		if old.fact.Known&schema.KnownUID == 0 ||
			updated.fact.Known&schema.KnownUID != 0 && old.fact.UID == updated.fact.UID {
			continue
		}
		appendTombstone := func(key []byte) {
			puts = append(
				puts,
				daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, key), Value: analyticsDerivedTombstone},
			)
		}
		appendTombstone(schema.UserInodeKey(old.fact.UID, old.identity.FSID, old.identity.Inode))
		revisionValue, found, err := store.Get(
			ctx,
			schema.InodeRevisionKey(old.identity.FSID, old.identity.Inode, old.identity.Revision),
		)
		if err != nil || !found {
			return errors.Join(
				err,
				fmt.Errorf(
					"analytics fact revision %d:%d:%d is missing",
					old.identity.FSID,
					old.identity.Inode,
					old.identity.Revision,
				),
			)
		}
		revision, err := schema.UnmarshalInodeRevision(revisionValue)
		if err != nil {
			return err
		}
		if err := visitInodeContent(ctx, store, revision, func(ordinal uint32, blob schema.ID) error {
			appendTombstone(schema.UserBlobContributionKey(old.fact.UID, blob, old.identity.FSID, old.identity.Inode, old.identity.Generation, ordinal))
			return nil
		}); err != nil {
			return err
		}
	}
	return writeBatches(ctx, store, puts)
}

func populateFactCalendar(fact *schema.AnalyticsFactRecord) {
	if fact.CreationBasis != schema.AnalyticsTimeUnknown {
		created := time.Unix(0, fact.CreatedAt).UTC()
		fact.CalendarYear, fact.CalendarMonth = int32(created.Year()), uint8(created.Month())
		isoYear, week := created.ISOWeek()
		fact.ISOYear, fact.Workweek = int32(isoYear), uint8(week)
	}
	if fact.Known&schema.KnownSize != 0 {
		fact.SizeLog10 = uint8(sizeLog10(fact.LogicalSize))
	}
}

func loadActiveBuildFact(
	ctx context.Context,
	store Store,
	pinned pinnedGeneration,
	fsid uint32,
	inode, generation uint64,
) (buildFact, bool, error) {
	value, found, err := getActiveDerived(
		ctx,
		store,
		pinned.epoch,
		schema.AnalyticsResidencyKey(fsid, inode, generation),
	)
	if err != nil || !found {
		return buildFact{}, false, err
	}
	overlay, err := schema.UnmarshalAnalyticsResidencyRecord(value)
	if err != nil {
		return buildFact{}, false, err
	}
	segmentValue, found, err := store.Get(ctx, schema.AnalyticsFactSegmentKey(overlay.FactSegment))
	if err != nil || !found {
		return buildFact{}, false, errors.Join(
			err,
			fmt.Errorf("analytics overlay points to missing segment %d", overlay.FactSegment),
		)
	}
	rows, err := decodeSegment(segmentValue)
	if err != nil || int(overlay.Row) >= len(rows.Identity) {
		return buildFact{}, false, errors.Join(err, fmt.Errorf("analytics overlay row is out of bounds"))
	}
	dict, err := loadDictionaries(ctx, store)
	if err != nil {
		return buildFact{}, false, err
	}
	fact := rowFact(rows, int(overlay.Row), dict)
	fact.Residency = overlay.State
	if revisionValue, found, err := store.Get(ctx, schema.InodeRevisionKey(fsid, inode, rows.Identity[overlay.Row].Revision)); err != nil {
		return buildFact{}, false, err
	} else if found {
		revision, err := schema.UnmarshalInodeRevision(revisionValue)
		if err != nil {
			return buildFact{}, false, err
		}
		fact.SourcePath = revision.SourcePath
	}
	return buildFact{
		identity:     rows.Identity[overlay.Row],
		fact:         fact,
		retainedRefs: overlay.RetainedSnapshotRefs,
		lastComplete: overlay.LastCompleteCrawl,
	}, true, nil
}

func catchUpStatus(ctx context.Context, store Store, processed uint32) (CatchUpResult, error) {
	result := CatchUpResult{Processed: processed}
	if pinned, found, err := pinLatest(ctx, store); err != nil {
		return result, err
	} else if found {
		result.AppliedCommit = pinned.watermark.AppliedCommit
	}
	head, available, err := authoritativeHead(ctx, store)
	if err != nil {
		return result, err
	}
	if available {
		result.AuthoritativeHead = head
		if head > result.AppliedCommit {
			result.LagCommits = head - result.AppliedCommit
		}
		result.Current = result.AppliedCommit >= head
	}
	return result, nil
}

func Rebuild(ctx context.Context, store Store, config Config, dryRun bool) (LifecycleResult, error) {
	unlock, err := lockAnalyticsPublication(store)
	if err != nil {
		return LifecycleResult{}, err
	}
	defer unlock()
	return rebuild(ctx, store, config, dryRun)
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func rebuild(ctx context.Context, store Store, config Config, dryRun bool) (LifecycleResult, error) {
	if err := config.Validate(); err != nil {
		return LifecycleResult{}, err
	}
	config = config.normalized()
	old, err := Status(ctx, store)
	if err != nil {
		return LifecycleResult{}, err
	}
	generation := old.Generation + 1
	if generation == 0 {
		generation = 1
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		return LifecycleResult{}, err
	}
	var peakFacts, peakBytes uint64
	observeBatch := func(facts []buildFact) {
		if uint64(len(facts)) > peakFacts {
			peakFacts = uint64(len(facts))
		}
		var bytes uint64
		for _, item := range facts {
			bytes += 256 + uint64(
				len(item.fact.SourcePath)+len(item.fact.SVM)+len(item.fact.Volume)+len(item.fact.PathGroup),
			)
		}
		if bytes > peakBytes {
			peakBytes = bytes
		}
	}
	if dryRun {
		return rebuildDryRun(ctx, store, config, generation, observeBatch, &peakFacts, &peakBytes)
	}

	checkpoint, resumed, err := loadCompatibleBuildCheckpoint(ctx, store, generation, string(configJSON))
	if err != nil {
		return LifecycleResult{}, err
	}
	if resumed {
		valid, err := validateBuildCheckpoint(ctx, store, checkpoint)
		if err != nil {
			return LifecycleResult{}, err
		}
		if !valid {
			if err := cleanupCandidateGeneration(ctx, store, checkpoint.Generation); err != nil {
				return LifecycleResult{}, fmt.Errorf("clean inconsistent analytics build: %w", err)
			}
			resumed = false
		}
	}
	if !resumed {
		now := time.Now().UnixNano()
		checkpoint = schema.AnalyticsBuildCheckpointRecord{
			BuildID: schema.ID(
				sha256.Sum256(append(append([]byte(nil), configJSON...), uint64Bytes(uint64(now))...)),
			),
			FormatVersion: 1,
			Generation:    generation,
			ConfigJSON:    string(configJSON),
			StartedAt:     now,
			UpdatedAt:     now,
		}
		if err := saveBuildCheckpoint(ctx, store, checkpoint); err != nil {
			return LifecycleResult{}, err
		}
	}

	segments := append([]uint64(nil), checkpoint.CandidateSegments...)
	ordinal := uint64(len(segments) + 1)
	_, appliedCommit, err := streamAuthoritativeFacts(
		ctx,
		store,
		config,
		checkpoint.SourceKeyCursor,
		config.SegmentRows,
		func(facts []buildFact, sourceKey []byte) error {
			observeBatch(facts)
			segment := generation<<32 | ordinal
			if segment == 0 {
				return fmt.Errorf("analytics segment identifier overflow")
			}
			dictionaries, dictionaryPuts, err := segmentDictionaries(ctx, store, facts)
			if err != nil {
				return err
			}
			if err := writeBatches(ctx, store, dictionaryPuts); err != nil {
				return err
			}
			puts, err := buildSegment(segment, generation, facts, dictionaries)
			if err != nil {
				return err
			}
			if err := writeBatches(ctx, store, puts); err != nil {
				return err
			}
			if err := writeSegmentDerived(ctx, store, generation, 0, segment, facts); err != nil {
				return err
			}
			segments = append(segments, segment)
			checkpoint.SourceKeyCursor = append(checkpoint.SourceKeyCursor[:0], sourceKey...)
			checkpoint.Facts += uint64(len(facts))
			checkpoint.CandidateSegments = append(checkpoint.CandidateSegments, segment)
			checkpoint.UpdatedAt = time.Now().UnixNano()
			if err := saveBuildCheckpoint(ctx, store, checkpoint); err != nil {
				return err
			}
			ordinal++
			return nil
		},
	)
	if err != nil {
		return LifecycleResult{}, err
	}
	checkpoint.AppliedCommit = appliedCommit

	return publishRebuild(ctx, store, checkpoint, configJSON, generation, segments, resumed, peakFacts, peakBytes)
}

func rebuildDryRun(
	ctx context.Context,
	store Store,
	config Config,
	generation uint64,
	observeBatch func([]buildFact),
	peakFacts, peakBytes *uint64,
) (LifecycleResult, error) {
	var facts uint64
	_, _, err := streamAuthoritativeFacts(ctx, store, config, nil, config.SegmentRows, func(batch []buildFact, _ []byte) error {
		observeBatch(batch)
		facts += uint64(len(batch))
		return nil
	})
	return LifecycleResult{
		Enabled:             true,
		Generation:          generation,
		Facts:               facts,
		PeakFactsBuffered:   *peakFacts,
		PeakWorkingSetBytes: *peakBytes,
	}, err
}

func publishRebuild(
	ctx context.Context,
	store Store,
	checkpoint schema.AnalyticsBuildCheckpointRecord,
	configJSON []byte,
	generation uint64,
	segments []uint64,
	resumed bool,
	peakFacts, peakBytes uint64,
) (LifecycleResult, error) {
	builtAt := time.Now().UnixNano()
	manifest := schema.AnalyticsManifestRecord{Generation: generation, Segments: segments}
	manifestValue, err := manifest.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	watermark := schema.AnalyticsWatermarkRecord{
		RepositoryGeneration: generation,
		AppliedCommit:        checkpoint.AppliedCommit,
		ManifestGeneration:   generation,
		AppliedAt:            builtAt,
	}
	watermarkValue, err := watermark.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	metadata := schema.AnalyticsMetadataRecord{
		Enabled:    true,
		Generation: generation,
		Facts:      checkpoint.Facts,
		BuiltAt:    builtAt,
		ConfigJSON: string(configJSON),
	}
	metadataValue, err := metadata.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	if err := store.WriteMutableBatch(ctx,
		[]daemon.Mutation{{Key: schema.AnalyticsDerivedGenerationMarkerKey(generation),
			Value: []byte{schema.Version}}},
		nil,
		true); err != nil {
		return LifecycleResult{}, fmt.Errorf("complete analytics candidate views: %w", err)
	}
	publication := []daemon.Mutation{
		{Key: schema.AnalyticsManifestKey(generation), Value: manifestValue},
		{Key: schema.AnalyticsWatermarkKey(generation), Value: watermarkValue},
		{Key: schema.AnalyticsMetadataKey(), Value: metadataValue},
	}
	if err := store.WriteMutableBatch(ctx, publication, [][]byte{schema.AnalyticsBuildCheckpointKey()}, true); err != nil {
		return LifecycleResult{}, fmt.Errorf("publish analytics generation: %w", err)
	}
	removed, err := cleanupOldDerived(ctx, store, generation)
	if err != nil {
		return LifecycleResult{}, err
	}
	return LifecycleResult{
		Enabled:             true,
		Generation:          generation,
		Facts:               checkpoint.Facts,
		Removed:             removed,
		BuiltAt:             builtAt,
		BuildID:             fmt.Sprintf("%x", checkpoint.BuildID),
		Resumed:             resumed,
		PeakFactsBuffered:   peakFacts,
		PeakWorkingSetBytes: peakBytes,
	}, nil
}
