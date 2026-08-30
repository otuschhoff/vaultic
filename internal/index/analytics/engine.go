package analytics

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

var (
	analyticsZstdEncoder = mustAnalyticsZstdEncoder()
	analyticsZstdDecoder = mustAnalyticsZstdDecoder()
	analyticsPublishMu   sync.Mutex
)

func mustAnalyticsZstdEncoder() *zstd.Encoder {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
	if err != nil {
		panic(err)
	}
	return encoder
}

func mustAnalyticsZstdDecoder() *zstd.Decoder {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		panic(err)
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

type sourceEvidence struct {
	generation   uint64
	revision     uint64
	state        schema.AuthoritativeSourceState
	continuity   schema.AnalyticsIdentityContinuity
	lastComplete int64
	commit       uint64
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
	analyticsPublishMu.Lock()
	defer analyticsPublishMu.Unlock()
	return catchUp(ctx, store, options)
}

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

func publishDeltaGeneration(ctx context.Context, store Store, metadata schema.AnalyticsMetadataRecord, config Config, parent pinnedGeneration, deltas []schema.AnalyticsDeltaRecord, appliedCommit uint64) error {
	generation := metadata.Generation + 1
	if generation == 0 {
		return fmt.Errorf("analytics generation overflow")
	}
	if err := cleanupCandidateGeneration(ctx, store, generation); err != nil {
		return fmt.Errorf("clean orphaned analytics delta generation: %w", err)
	}
	type identityKey struct {
		fsid       uint32
		inode      uint64
		generation uint64
	}
	order := make([]identityKey, 0, len(deltas))
	affected := make(map[identityKey]buildFact, len(deltas))
	previous := make(map[identityKey]buildFact, len(deltas))
	for _, delta := range deltas {
		if delta.ClassificationEpoch != metadata.Generation {
			return fmt.Errorf("analytics classification epoch changed from %d to %d; streaming rebuild required", delta.ClassificationEpoch, metadata.Generation)
		}
		key := identityKey{delta.FSID, delta.Inode, delta.IdentityGeneration}
		item, present := affected[key]
		if !present {
			loaded, found, err := loadActiveBuildFact(ctx, store, parent, delta.FSID, delta.Inode, delta.IdentityGeneration)
			if err != nil {
				return err
			}
			if found {
				item = loaded
				previous[key] = loaded
			} else {
				item = buildFact{identity: segmentIdentity{FSID: delta.FSID, Inode: delta.Inode, Generation: delta.IdentityGeneration, Revision: delta.Revision, Known: delta.Known}}
			}
			order = append(order, key)
		}
		if delta.Kind == schema.AnalyticsDeltaCreation || delta.Kind == schema.AnalyticsDeltaClassification {
			revisionValue, found, err := store.Get(ctx, schema.InodeRevisionKey(delta.FSID, delta.Inode, delta.Revision))
			if err != nil || !found {
				return errors.Join(err, fmt.Errorf("analytics delta points to missing inode revision %d:%d:%d", delta.FSID, delta.Inode, delta.Revision))
			}
			revision, err := schema.UnmarshalInodeRevision(revisionValue)
			if err != nil {
				return err
			}
			item.identity.Revision, item.identity.Known = delta.Revision, delta.Known
			item.fact = makeFact(schema.ParsedKey{FSID: delta.FSID, Inode: delta.Inode, Revision: delta.Revision}, revision, config)
			item.fact.Revision, item.fact.UID, item.fact.GID, item.fact.Known = delta.Revision, delta.UID, delta.GID, delta.Known
			item.fact.CreatedAt, item.fact.LogicalSize = delta.CreatedAt, delta.LogicalSize
			item.fact.Residency, item.fact.CreationBasis = delta.State, delta.CreationBasis
			item.fact.IdentityGeneration, item.fact.IdentityContinuity = delta.IdentityGeneration, delta.IdentityContinuity
			populateFactCalendar(&item.fact)
			item.retainedRefs = delta.RetainedSnapshotRefs
		} else if delta.Kind == schema.AnalyticsDeltaSourceState {
			item.fact.Residency = delta.State
			if delta.IdentityContinuity != schema.AnalyticsContinuityUnknown {
				item.fact.IdentityContinuity = delta.IdentityContinuity
			}
		} else {
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
				// Legacy deltas encoded publish as one and forget as zero.
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
		}
		affected[key] = item
	}
	facts := make([]buildFact, 0, len(order))
	for _, key := range order {
		facts = append(facts, affected[key])
	}
	segment := generation<<32 | 1
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
	manifestValue, err := (schema.AnalyticsManifestRecord{Generation: generation, ParentGeneration: parent.epoch, LayerDepth: parent.manifest.LayerDepth + 1, Segments: segments}).MarshalBinary()
	if err != nil {
		return err
	}
	builtAt := time.Now().UnixNano()
	watermarkValue, err := (schema.AnalyticsWatermarkRecord{RepositoryGeneration: generation, AppliedCommit: appliedCommit, ManifestGeneration: generation, AppliedAt: builtAt}).MarshalBinary()
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

func subtractCandidateViews(ctx context.Context, store Store, generation, parentGeneration uint64, facts []buildFact) error {
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
		if fact.Known&schema.KnownUID != 0 {
			statsKey := string(schema.UserStatsKey(fact.UID, fact.Residency))
			stats := summaries[statsKey]
			stats.ActiveFiles++
			stats.ActiveBytes += size
			summaries[statsKey] = stats
			if fact.Residency == schema.AnalyticsLive {
				key := string(schema.UserSummaryKey(fact.UID))
				summary := summaries[key]
				summary.ActiveFiles++
				summary.ActiveBytes += size
				summaries[key] = summary
			}
		}
		if fact.Known&schema.KnownGID != 0 {
			statsKey := string(schema.GroupStatsKey(fact.GID, fact.Residency))
			stats := summaries[statsKey]
			stats.ActiveFiles++
			stats.ActiveBytes += size
			summaries[statsKey] = stats
			if fact.Residency == schema.AnalyticsLive {
				key := string(schema.GroupSummaryKey(fact.GID))
				summary := summaries[key]
				summary.ActiveFiles++
				summary.ActiveBytes += size
				summaries[key] = summary
			}
		}
	}
	var puts []daemon.Mutation
	for key, subtract := range aggregates {
		logicalKey := []byte(key)
		current, found, err := store.Get(ctx, schema.AnalyticsDerivedKey(generation, logicalKey))
		if err != nil {
			return err
		}
		if !found {
			current, found, err = getActiveDerived(ctx, store, parentGeneration, logicalKey)
			if err != nil {
				return err
			}
		}
		if !found {
			return fmt.Errorf("analytics parent aggregate %x is missing", logicalKey)
		}
		record, err := schema.UnmarshalAnalyticsAggregateRecord(current)
		if err != nil || record.BytesAdded < subtract.BytesAdded || record.BytesDeleted < subtract.BytesDeleted || record.FilesAdded < subtract.FilesAdded || record.FilesDeleted < subtract.FilesDeleted {
			return errors.Join(err, fmt.Errorf("analytics aggregate subtraction underflow"))
		}
		record.BytesAdded -= subtract.BytesAdded
		record.BytesDeleted -= subtract.BytesDeleted
		record.FilesAdded -= subtract.FilesAdded
		record.FilesDeleted -= subtract.FilesDeleted
		value, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		if record == (schema.AnalyticsAggregateRecord{}) {
			value = analyticsDerivedTombstone
		}
		puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, logicalKey), Value: value})
	}
	for key, subtract := range summaries {
		logicalKey := []byte(key)
		current, found, err := store.Get(ctx, schema.AnalyticsDerivedKey(generation, logicalKey))
		if err != nil {
			return err
		}
		if !found {
			current, found, err = getActiveDerived(ctx, store, parentGeneration, logicalKey)
			if err != nil {
				return err
			}
		}
		if !found {
			return fmt.Errorf("analytics parent summary %x is missing", logicalKey)
		}
		record, err := schema.UnmarshalAnalyticsSummaryRecord(current)
		if err != nil || record.ActiveBytes < subtract.ActiveBytes || record.ActiveFiles < subtract.ActiveFiles || record.UniqueBlobCount < subtract.UniqueBlobCount || record.UniqueBlobBytes < subtract.UniqueBlobBytes {
			return errors.Join(err, fmt.Errorf("analytics summary subtraction underflow"))
		}
		record.ActiveBytes -= subtract.ActiveBytes
		record.ActiveFiles -= subtract.ActiveFiles
		record.UniqueBlobCount -= subtract.UniqueBlobCount
		record.UniqueBlobBytes -= subtract.UniqueBlobBytes
		value, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		if record == (schema.AnalyticsSummaryRecord{}) {
			value = analyticsDerivedTombstone
		}
		puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, logicalKey), Value: value})
	}
	return writeBatches(ctx, store, puts)
}

func writeDerivedTombstones[K comparable](ctx context.Context, store Store, generation uint64, previous, current map[K]buildFact) error {
	var puts []daemon.Mutation
	for key, old := range previous {
		updated := current[key]
		if old.fact.Known&schema.KnownUID == 0 || updated.fact.Known&schema.KnownUID != 0 && old.fact.UID == updated.fact.UID {
			continue
		}
		appendTombstone := func(key []byte) {
			puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, key), Value: analyticsDerivedTombstone})
		}
		appendTombstone(schema.UserInodeKey(old.fact.UID, old.identity.FSID, old.identity.Inode))
		revisionValue, found, err := store.Get(ctx, schema.InodeRevisionKey(old.identity.FSID, old.identity.Inode, old.identity.Revision))
		if err != nil || !found {
			return errors.Join(err, fmt.Errorf("analytics fact revision %d:%d:%d is missing", old.identity.FSID, old.identity.Inode, old.identity.Revision))
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

func loadActiveBuildFact(ctx context.Context, store Store, pinned pinnedGeneration, fsid uint32, inode, generation uint64) (buildFact, bool, error) {
	value, found, err := getActiveDerived(ctx, store, pinned.epoch, schema.AnalyticsResidencyKey(fsid, inode, generation))
	if err != nil || !found {
		return buildFact{}, false, err
	}
	overlay, err := schema.UnmarshalAnalyticsResidencyRecord(value)
	if err != nil {
		return buildFact{}, false, err
	}
	segmentValue, found, err := store.Get(ctx, schema.AnalyticsFactSegmentKey(overlay.FactSegment))
	if err != nil || !found {
		return buildFact{}, false, errors.Join(err, fmt.Errorf("analytics overlay points to missing segment %d", overlay.FactSegment))
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
	return buildFact{identity: rows.Identity[overlay.Row], fact: fact, retainedRefs: overlay.RetainedSnapshotRefs, lastComplete: overlay.LastCompleteCrawl}, true, nil
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
	analyticsPublishMu.Lock()
	defer analyticsPublishMu.Unlock()
	return rebuild(ctx, store, config, dryRun)
}

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
			bytes += 256 + uint64(len(item.fact.SourcePath)+len(item.fact.SVM)+len(item.fact.Volume)+len(item.fact.PathGroup))
		}
		if bytes > peakBytes {
			peakBytes = bytes
		}
	}
	if dryRun {
		var facts uint64
		_, _, err := streamAuthoritativeFacts(ctx, store, config, nil, config.SegmentRows, func(batch []buildFact, _ []byte) error {
			observeBatch(batch)
			facts += uint64(len(batch))
			return nil
		})
		return LifecycleResult{Enabled: true, Generation: generation, Facts: facts, PeakFactsBuffered: peakFacts, PeakWorkingSetBytes: peakBytes}, err
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
			BuildID:       schema.ID(sha256.Sum256(append(append([]byte(nil), configJSON...), uint64Bytes(uint64(now))...))),
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
	_, appliedCommit, err := streamAuthoritativeFacts(ctx, store, config, checkpoint.SourceKeyCursor, config.SegmentRows, func(facts []buildFact, sourceKey []byte) error {
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
	})
	if err != nil {
		return LifecycleResult{}, err
	}
	checkpoint.AppliedCommit = appliedCommit

	builtAt := time.Now().UnixNano()
	manifest := schema.AnalyticsManifestRecord{Generation: generation, Segments: segments}
	manifestValue, err := manifest.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	watermark := schema.AnalyticsWatermarkRecord{RepositoryGeneration: generation, AppliedCommit: appliedCommit, ManifestGeneration: generation, AppliedAt: builtAt}
	watermarkValue, err := watermark.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	metadata := schema.AnalyticsMetadataRecord{Enabled: true, Generation: generation, Facts: checkpoint.Facts, BuiltAt: builtAt, ConfigJSON: string(configJSON)}
	metadataValue, err := metadata.MarshalBinary()
	if err != nil {
		return LifecycleResult{}, err
	}
	if err := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsDerivedGenerationMarkerKey(generation), Value: []byte{schema.Version}}}, nil, true); err != nil {
		return LifecycleResult{}, fmt.Errorf("complete analytics candidate views: %w", err)
	}
	publication := []daemon.Mutation{
		daemon.Mutation{Key: schema.AnalyticsManifestKey(generation), Value: manifestValue},
		daemon.Mutation{Key: schema.AnalyticsWatermarkKey(generation), Value: watermarkValue},
		daemon.Mutation{Key: schema.AnalyticsMetadataKey(), Value: metadataValue},
	}
	if err := store.WriteMutableBatch(ctx, publication, [][]byte{schema.AnalyticsBuildCheckpointKey()}, true); err != nil {
		return LifecycleResult{}, fmt.Errorf("publish analytics generation: %w", err)
	}
	removed, err := cleanupOldDerived(ctx, store, generation)
	if err != nil {
		return LifecycleResult{}, err
	}
	return LifecycleResult{Enabled: true, Generation: generation, Facts: checkpoint.Facts, Removed: removed, BuiltAt: builtAt, BuildID: fmt.Sprintf("%x", checkpoint.BuildID), Resumed: resumed, PeakFactsBuffered: peakFacts, PeakWorkingSetBytes: peakBytes}, nil
}

func loadCompatibleBuildCheckpoint(ctx context.Context, store Store, generation uint64, configJSON string) (schema.AnalyticsBuildCheckpointRecord, bool, error) {
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
	return store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsBuildCheckpointKey(), Value: value}}, nil, true)
}

func validateBuildCheckpoint(ctx context.Context, store Store, checkpoint schema.AnalyticsBuildCheckpointRecord) (bool, error) {
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
				if _, found, err := store.Get(ctx, schema.AnalyticsDimensionIndexKey(dimension, value, segment)); err != nil || !found {
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

func streamAuthoritativeFacts(ctx context.Context, store Store, config Config, afterKey []byte, batchSize int, consume func([]buildFact, []byte) error) (uint64, uint64, error) {
	bindings, _, err := store.ScanPrefix(ctx, []byte("asb:"), nil, 1)
	if err != nil {
		return 0, 0, err
	}
	if len(bindings) != 0 {
		return streamSourceBindings(ctx, store, config, afterKey, batchSize, consume)
	}
	return streamLegacyRevisions(ctx, store, config, afterKey, batchSize, consume)
}

func streamSourceBindings(ctx context.Context, store Store, config Config, afterKey []byte, batchSize int, consume func([]buildFact, []byte) error) (uint64, uint64, error) {
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
			key, err := schema.ParseKey(item.Key)
			if err != nil || key.Kind != schema.KeyAuthoritativeSourceBinding {
				return facts, applied, fmt.Errorf("invalid authoritative source binding key %x", item.Key)
			}
			binding, err := schema.UnmarshalAuthoritativeSourceBindingRecord(item.Value)
			if err != nil {
				return facts, applied, err
			}
			revisionKey := schema.InodeRevisionKey(key.FSID, key.Inode, binding.Revision)
			value, found, err := store.Get(ctx, revisionKey)
			if err != nil || !found {
				return facts, applied, errors.Join(err, fmt.Errorf("source binding points to missing revision %d:%d:%d", key.FSID, key.Inode, binding.Revision))
			}
			revision, err := schema.UnmarshalInodeRevision(value)
			if err != nil {
				return facts, applied, err
			}
			fact := makeFact(schema.ParsedKey{FSID: key.FSID, Inode: key.Inode, Revision: binding.Revision}, revision, config)
			fact.IdentityGeneration, fact.IdentityContinuity = binding.Generation, binding.Continuity
			lastComplete := int64(0)
			retained := uint64(0)
			switch binding.State {
			case schema.AuthoritativeSourceLive:
				fact.Residency = schema.AnalyticsLive
			case schema.AuthoritativeSourceDeleted:
				proofValue, found, err := store.Get(ctx, schema.AuthoritativeCrawlProofKey(key.ID, binding.LastObservedCommit))
				if err != nil || !found {
					return facts, applied, errors.Join(err, fmt.Errorf("deleted source binding has no crawl proof"))
				}
				proof, err := schema.UnmarshalAuthoritativeCrawlProofRecord(proofValue)
				if err != nil || !proof.Complete || !proof.DebtFree {
					return facts, applied, errors.Join(err, fmt.Errorf("deleted source binding has invalid crawl proof"))
				}
				retained, err = retainedReferencesForIdentity(ctx, store, key.FSID, key.Inode, binding.Generation)
				if err != nil {
					return facts, applied, err
				}
				lastComplete = proof.CompletedAt
				fact.Residency = schema.AnalyticsExpired
				if retained != 0 {
					fact.Residency = schema.AnalyticsArchiveOnly
				}
			default:
				fact.Residency = schema.AnalyticsUnknown
			}
			identity := segmentIdentity{FSID: key.FSID, Inode: key.Inode, Generation: binding.Generation, Revision: binding.Revision, Known: fact.Known}
			batch = append(batch, buildFact{identity: identity, fact: fact, retainedRefs: retained, lastComplete: lastComplete})
			facts++
			if binding.LastObservedCommit > applied {
				applied = binding.LastObservedCommit
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

func streamLegacyRevisions(ctx context.Context, store Store, config Config, afterKey []byte, batchSize int, consume func([]buildFact, []byte) error) (uint64, uint64, error) {
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
			batch = append(batch, buildFact{identity: segmentIdentity{FSID: key.FSID, Inode: key.Inode, Generation: key.Revision, Revision: key.Revision, Known: fact.Known}, fact: fact})
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

func retainedReferencesForIdentity(ctx context.Context, store Store, fsid uint32, inode, generation uint64) (uint64, error) {
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
				} else if parsed.Kind == schema.KeyInodeRevision && parsed.FSID == fsid && parsed.Inode == inode && parsed.Revision >= generation && (nextGeneration == 0 || parsed.Revision < nextGeneration) {
					return true, nil
				}
			}
			return false, nil
		}
		found, err := visit(schema.DirectoryRevisionKey(snapshot.RootFSID, snapshot.RootInode, snapshot.RootRevision), 0)
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
		}{{schema.AnalyticsDictionarySVM, item.fact.SVM}, {schema.AnalyticsDictionaryVolume, item.fact.Volume}, {schema.AnalyticsDictionaryPathGroup, item.fact.PathGroup}} {
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

func writeSegmentDerived(ctx context.Context, store Store, generation, parentGeneration, segment uint64, facts []buildFact) error {
	puts := make([]daemon.Mutation, 0, len(facts))
	for row, item := range facts {
		overlay := schema.AnalyticsResidencyRecord{State: item.fact.Residency, LastCompleteCrawl: item.lastComplete, RetainedSnapshotRefs: item.retainedRefs, ClassificationEpoch: generation, FactSegment: segment, Row: uint32(row)}
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
			evidence = []sourceEvidence{{generation: first.key.Revision, revision: first.key.Revision, state: schema.AuthoritativeSourceUnknown, continuity: schema.AnalyticsContinuityUnknown}}
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
				return fmt.Errorf("source binding %d:%d generation %d points to missing revision %d", currentKey.fsid, currentKey.inode, source.generation, source.revision)
			}
			fact := makeFact(selected.key, selected.record, config)
			fact.IdentityGeneration = source.generation
			fact.IdentityContinuity = source.continuity
			identity := segmentIdentity{FSID: currentKey.fsid, Inode: currentKey.inode, Generation: source.generation, Revision: source.revision, Known: fact.Known}
			membershipIdentity := segmentIdentity{FSID: identity.FSID, Inode: identity.Inode, Generation: identity.Generation, Revision: identity.Generation}
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
			facts = append(facts, buildFact{identity: identity, fact: fact, retainedRefs: retained, lastComplete: source.lastComplete})
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
		evidence := sourceEvidence{generation: binding.Generation, revision: binding.Revision, state: binding.State, continuity: binding.Continuity, commit: binding.LastObservedCommit}
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
		if !found || sourceStatePriority(evidence.state) > sourceStatePriority(prior.state) || sourceStatePriority(evidence.state) == sourceStatePriority(prior.state) && evidence.commit > prior.commit {
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
					items, _, err := store.ScanPrefix(ctx, schema.InodeRevisionPrefix(parsed.FSID, parsed.Inode), nil, 1)
					if err != nil || len(items) == 0 {
						return errors.Join(err, fmt.Errorf("snapshot inode %d:%d has no revision", parsed.FSID, parsed.Inode))
					}
					generation, err := schema.ParseKey(items[0].Key)
					if err != nil {
						return err
					}
					generationRevision = generation.Revision
				}
				seenIdentities[segmentIdentity{FSID: parsed.FSID, Inode: parsed.Inode, Generation: generationRevision, Revision: generationRevision}] = struct{}{}
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

func visitInodeContent(ctx context.Context, store Store, record schema.InodeRevision, visit func(uint32, schema.ID) error) error {
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

type dictionaries struct {
	ids    map[schema.AnalyticsDictionaryKind]map[string]uint32
	values map[schema.AnalyticsDictionaryKind][]string
}

func makeDictionaries(facts []buildFact, existing map[schema.AnalyticsDictionaryKind]map[uint32]string) dictionaries {
	result := dictionaries{ids: map[schema.AnalyticsDictionaryKind]map[string]uint32{}, values: map[schema.AnalyticsDictionaryKind][]string{}}
	for _, kind := range []schema.AnalyticsDictionaryKind{schema.AnalyticsDictionarySVM, schema.AnalyticsDictionaryVolume, schema.AnalyticsDictionaryPathGroup} {
		set := map[string]struct{}{}
		for id, value := range existing[kind] {
			for len(result.values[kind]) < int(id) {
				result.values[kind] = append(result.values[kind], "")
			}
			result.values[kind][id-1] = value
			set[value] = struct{}{}
		}
		for _, fact := range facts {
			value := fact.fact.SVM
			if kind == schema.AnalyticsDictionaryVolume {
				value = fact.fact.Volume
			}
			if kind == schema.AnalyticsDictionaryPathGroup {
				value = fact.fact.PathGroup
			}
			if value != "" && value != "unknown" {
				set[value] = struct{}{}
			}
		}
		var additions []string
		for value := range set {
			found := false
			for _, existingValue := range result.values[kind] {
				if value == existingValue {
					found = true
					break
				}
			}
			if !found {
				additions = append(additions, value)
			}
		}
		sort.Strings(additions)
		result.values[kind] = append(result.values[kind], additions...)
		result.ids[kind] = map[string]uint32{}
		for index, value := range result.values[kind] {
			result.ids[kind][value] = uint32(index + 1)
		}
	}
	return result
}

func marshalDictionaries(dict dictionaries) ([]daemon.Mutation, error) {
	var puts []daemon.Mutation
	for _, kind := range []schema.AnalyticsDictionaryKind{schema.AnalyticsDictionarySVM, schema.AnalyticsDictionaryVolume, schema.AnalyticsDictionaryPathGroup} {
		for index, value := range dict.values[kind] {
			if value == "" {
				continue
			}
			encoded, err := (schema.AnalyticsDictionaryRecord{Value: value}).MarshalBinary()
			if err != nil {
				return nil, err
			}
			puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDictionaryKey(kind, uint32(index+1)), Value: encoded})
		}
	}
	return puts, nil
}

func buildSegment(segment, generation uint64, facts []buildFact, dict dictionaries) ([]daemon.Mutation, error) {
	rows := segmentRows{}
	for _, item := range facts {
		rows.Identity = append(rows.Identity, item.identity)
		rows.UID = append(rows.UID, item.fact.UID)
		rows.GID = append(rows.GID, item.fact.GID)
		rows.CreatedAt = append(rows.CreatedAt, item.fact.CreatedAt)
		rows.Basis = append(rows.Basis, item.fact.CreationBasis)
		rows.Continuity = append(rows.Continuity, item.fact.IdentityContinuity)
		rows.Year = append(rows.Year, item.fact.CalendarYear)
		rows.Month = append(rows.Month, item.fact.CalendarMonth)
		rows.ISOYear = append(rows.ISOYear, item.fact.ISOYear)
		rows.Workweek = append(rows.Workweek, item.fact.Workweek)
		rows.SVM = append(rows.SVM, dict.ids[schema.AnalyticsDictionarySVM][item.fact.SVM])
		rows.Volume = append(rows.Volume, dict.ids[schema.AnalyticsDictionaryVolume][item.fact.Volume])
		rows.PathGroup = append(rows.PathGroup, dict.ids[schema.AnalyticsDictionaryPathGroup][item.fact.PathGroup])
		rows.Size = append(rows.Size, item.fact.LogicalSize)
		rows.SizeLog10 = append(rows.SizeLog10, item.fact.SizeLog10)
	}
	columnValues := []any{rows.Identity, rows.UID, rows.GID, rows.CreatedAt, rows.Basis, rows.Continuity, rows.Year, rows.Month, rows.ISOYear, rows.Workweek, rows.SVM, rows.Volume, rows.PathGroup, rows.Size, rows.SizeLog10}
	record := schema.AnalyticsFactSegmentRecord{RowCount: uint32(len(facts))}
	for index, values := range columnValues {
		data, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		record.Columns = append(record.Columns, schema.AnalyticsColumn{Kind: schema.AnalyticsColumnKind(index + 1), Codec: schema.AnalyticsCodecZstd, Data: analyticsZstdEncoder.EncodeAll(data, nil)})
	}
	value, err := record.MarshalBinary()
	if err != nil {
		return nil, err
	}
	metadata := segmentMetadata(generation, facts)
	metadataValue, err := metadata.MarshalBinary()
	if err != nil {
		return nil, err
	}
	puts := []daemon.Mutation{{Key: schema.AnalyticsFactSegmentKey(segment), Value: value}, {Key: schema.AnalyticsSegmentMetadataKey(segment), Value: metadataValue}}
	for dimension, values := range indexValues(rows) {
		for present, bitmap := range values {
			matches := countBits(bitmap)
			index := schema.AnalyticsDimensionIndexRecord{Codec: schema.AnalyticsCodecZstd, RowCount: uint32(len(facts)), MatchCount: matches, Bitmap: analyticsZstdEncoder.EncodeAll(bitmap, nil)}
			for row := range facts {
				if bitSet(bitmap, row) && facts[row].fact.Known&schema.KnownSize != 0 {
					index.LogicalBytes += facts[row].fact.LogicalSize
				}
			}
			encoded, err := index.MarshalBinary()
			if err != nil {
				return nil, err
			}
			puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDimensionIndexKey(dimension, present, segment), Value: encoded})
		}
	}
	return puts, nil
}

func segmentMetadata(generation uint64, facts []buildFact) schema.AnalyticsSegmentMetadataRecord {
	metadata := schema.AnalyticsSegmentMetadataRecord{RowCount: uint32(len(facts)), MinCreatedAt: facts[0].fact.CreatedAt, MaxCreatedAt: facts[0].fact.CreatedAt, MinLogicalSize: facts[0].fact.LogicalSize, MaxLogicalSize: facts[0].fact.LogicalSize, MinRevision: facts[0].identity.Revision, MaxRevision: facts[0].identity.Revision, FirstCommit: facts[0].identity.Revision, LastCommit: facts[0].identity.Revision, ClassificationEpoch: generation, CodecParameters: "json-columns-v1;zstd=3"}
	for _, item := range facts[1:] {
		if item.fact.CreatedAt < metadata.MinCreatedAt {
			metadata.MinCreatedAt = item.fact.CreatedAt
		}
		if item.fact.CreatedAt > metadata.MaxCreatedAt {
			metadata.MaxCreatedAt = item.fact.CreatedAt
		}
		if item.fact.LogicalSize < metadata.MinLogicalSize {
			metadata.MinLogicalSize = item.fact.LogicalSize
		}
		if item.fact.LogicalSize > metadata.MaxLogicalSize {
			metadata.MaxLogicalSize = item.fact.LogicalSize
		}
		if item.identity.Revision < metadata.MinRevision {
			metadata.MinRevision = item.identity.Revision
		}
		if item.identity.Revision > metadata.MaxRevision {
			metadata.MaxRevision = item.identity.Revision
		}
		if item.identity.Revision < metadata.FirstCommit {
			metadata.FirstCommit = item.identity.Revision
		}
		if item.identity.Revision > metadata.LastCommit {
			metadata.LastCommit = item.identity.Revision
		}
	}
	return metadata
}

func indexValues(rows segmentRows) map[schema.AnalyticsDimension]map[uint64][]byte {
	result := map[schema.AnalyticsDimension]map[uint64][]byte{}
	add := func(dimension schema.AnalyticsDimension, row int, present bool, value uint64) {
		if !present {
			return
		}
		if result[dimension] == nil {
			result[dimension] = map[uint64][]byte{}
		}
		bitmap := result[dimension][value]
		if bitmap == nil {
			bitmap = make([]byte, (len(rows.Identity)+7)/8)
		}
		bitmap[row/8] |= 1 << uint(row%8)
		result[dimension][value] = bitmap
	}
	for row, identity := range rows.Identity {
		add(schema.AnalyticsDimensionUID, row, identity.Known&schema.KnownUID != 0, uint64(rows.UID[row]))
		add(schema.AnalyticsDimensionGID, row, identity.Known&schema.KnownGID != 0, uint64(rows.GID[row]))
		knownTime := rows.Basis[row] != schema.AnalyticsTimeUnknown
		add(schema.AnalyticsDimensionCalendarYear, row, knownTime, uint64(uint32(rows.Year[row])))
		add(schema.AnalyticsDimensionCalendarMonth, row, knownTime, uint64(rows.Month[row]))
		add(schema.AnalyticsDimensionISOYear, row, knownTime, uint64(uint32(rows.ISOYear[row])))
		add(schema.AnalyticsDimensionWorkweek, row, knownTime, uint64(rows.Workweek[row]))
		add(schema.AnalyticsDimensionSVM, row, rows.SVM[row] != 0, uint64(rows.SVM[row]))
		add(schema.AnalyticsDimensionVolume, row, rows.Volume[row] != 0, uint64(rows.Volume[row]))
		add(schema.AnalyticsDimensionPathGroup, row, rows.PathGroup[row] != 0, uint64(rows.PathGroup[row]))
		add(schema.AnalyticsDimensionSizeLog10, row, identity.Known&schema.KnownSize != 0, uint64(rows.SizeLog10[row]))
		add(schema.AnalyticsDimensionCreationBasis, row, true, uint64(rows.Basis[row]))
		add(schema.AnalyticsDimensionIdentityContinuity, row, true, uint64(rows.Continuity[row]))
	}
	return result
}

func Execute(ctx context.Context, store Store, query Query) (Result, error) {
	if err := query.Validate(); err != nil {
		return Result{}, err
	}
	metadata, err := Status(ctx, store)
	if err != nil {
		return Result{}, err
	}
	if !metadata.Enabled {
		return Result{}, fmt.Errorf("analytics is disabled; enable or rebuild it first")
	}
	pinned, found, err := pinLatest(ctx, store)
	if err != nil {
		return Result{}, err
	}
	if !found {
		hasManifest := false
		if err := scan(ctx, store, schema.AnalyticsManifestPrefix(), func(daemon.KeyValue) error { hasManifest = true; return nil }); err != nil {
			return Result{}, err
		}
		if hasManifest {
			return Result{}, fmt.Errorf("analytics manifest exists without a published watermark")
		}
		result, err := executeLegacy(ctx, store, query)
		result.Explain.LegacyFallback = err == nil
		result.Explain.Source = "legacy-facts"
		return result, err
	}
	head, headAvailable, err := authoritativeHead(ctx, store)
	if err != nil {
		return Result{}, err
	}
	if query.RequireCurrent && !headAvailable {
		return Result{}, fmt.Errorf("require-current is unavailable: analytics Store does not expose an authoritative metadata head; pinned applied commit is %d", pinned.watermark.AppliedCommit)
	}
	if query.RequireCurrent && pinned.watermark.AppliedCommit < head {
		return Result{}, fmt.Errorf("analytics is not current: applied commit %d, authoritative metadata head %d", pinned.watermark.AppliedCommit, head)
	}
	if headAvailable && pinned.watermark.AppliedCommit < head && !query.AllowStale {
		return Result{}, fmt.Errorf("analytics is stale: applied commit %d, authoritative metadata head %d; use allow-stale or catch up", pinned.watermark.AppliedCommit, head)
	}
	canonical, err := canonicalQuery(query)
	if err != nil {
		return Result{}, err
	}
	hash := sha256.Sum256(canonical)
	cacheKey := schema.AnalyticsQueryResultKey(schema.ID(hash), pinned.watermark.RepositoryGeneration, pinned.epoch, pinned.watermark.AppliedCommit)
	if result, ok, err := readResultCache(ctx, store, cacheKey); err != nil {
		return Result{}, err
	} else if ok {
		result.Cached = true
		return result, nil
	}
	result := Result{SchemaVersion: 2, Generation: pinned.manifest.Generation, Watermark: WatermarkInfo{RepositoryGeneration: pinned.watermark.RepositoryGeneration, ClassificationEpoch: pinned.epoch, AppliedCommit: pinned.watermark.AppliedCommit, AppliedAt: pinned.watermark.AppliedAt, AuthoritativeHead: head, AuthoritativeHeadAvailable: headAvailable}, Explain: Explain{Source: "raw-segments"}}
	if head > pinned.watermark.AppliedCommit {
		result.Watermark.LagCommits = head - pinned.watermark.AppliedCommit
	}
	if viewResult, ok, fallbacks, err := readCompatibleView(ctx, store, query, pinned); err != nil {
		return Result{}, err
	} else if ok {
		viewResult.Watermark.AuthoritativeHead = head
		viewResult.Watermark.AuthoritativeHeadAvailable = headAvailable
		viewResult.Watermark.LagCommits = result.Watermark.LagCommits
		return viewResult, nil
	} else {
		result.Explain.ViewFallbacks = fallbacks
	}
	dict, err := loadDictionaries(ctx, store)
	if err != nil {
		return Result{}, err
	}
	groups := map[string]*Group{}
	for _, segment := range pinned.manifest.Segments {
		result.Explain.SegmentsConsidered++
		metadataValue, present, err := store.Get(ctx, schema.AnalyticsSegmentMetadataKey(segment))
		if err != nil {
			return Result{}, err
		}
		if !present {
			return Result{}, fmt.Errorf("published analytics segment %d has no metadata", segment)
		}
		metadata, err := schema.UnmarshalAnalyticsSegmentMetadataRecord(metadataValue)
		if err != nil {
			return Result{}, err
		}
		if pruneSegment(metadata, query) {
			result.Explain.SegmentsPruned++
			continue
		}
		segmentValue, present, err := store.Get(ctx, schema.AnalyticsFactSegmentKey(segment))
		if err != nil {
			return Result{}, err
		}
		if !present {
			return Result{}, fmt.Errorf("published analytics segment %d is missing", segment)
		}
		rows, err := decodeSegment(segmentValue)
		if err != nil {
			return Result{}, err
		}
		candidates, used, fallbacks, err := indexedCandidates(ctx, store, segment, rows, query, dict)
		if err != nil {
			return Result{}, err
		}
		result.Explain.IndexesUsed = append(result.Explain.IndexesUsed, used...)
		result.Explain.IndexFallbacks = append(result.Explain.IndexFallbacks, fallbacks...)
		result.Explain.SegmentsScanned++
		for row := range rows.Identity {
			if candidates != nil && !bitSet(candidates, row) {
				continue
			}
			result.Explain.RowsScanned++
			fact := rowFact(rows, row, dict)
			residencyKey := schema.AnalyticsResidencyKey(rows.Identity[row].FSID, rows.Identity[row].Inode, rows.Identity[row].Generation)
			residencyValue, present, err := getActiveDerived(ctx, store, pinned.epoch, residencyKey)
			if err != nil {
				return Result{}, err
			}
			if present {
				overlay, err := schema.UnmarshalAnalyticsResidencyRecord(residencyValue)
				if err != nil {
					return Result{}, err
				}
				if overlay.FactSegment != segment || overlay.Row != uint32(row) {
					continue
				}
				if overlay.ClassificationEpoch <= pinned.epoch {
					fact.Residency = overlay.State
				}
			}
			if !matchesComplete(fact, query) {
				continue
			}
			result.Files++
			if fact.Known&schema.KnownSize != 0 {
				result.LogicalBytes += fact.LogicalSize
			}
			if fact.CreationBasis == schema.AnalyticsTimeUnknown {
				result.UnknownCreationTime++
			}
			dims := dimensions(fact, query.GroupBy)
			if len(dims) != 0 {
				key, _ := json.Marshal(dims)
				group := groups[string(key)]
				if group == nil {
					group = &Group{Dimensions: dims}
					groups[string(key)] = group
				}
				group.Files++
				if fact.Known&schema.KnownSize != 0 {
					group.LogicalBytes += fact.LogicalSize
				}
			}
		}
	}
	for key := range groups {
		_ = key
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Groups = append(result.Groups, *groups[key])
	}
	if err := updateCache(ctx, store, query, schema.ID(hash), cacheKey, result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func authoritativeHead(ctx context.Context, store Store) (uint64, bool, error) {
	headStore, ok := store.(metadataHeadStore)
	if !ok {
		return 0, false, nil
	}
	head, err := headStore.MetadataHead(ctx)
	return head, true, err
}

func pinLatest(ctx context.Context, store Store) (pinnedGeneration, bool, error) {
	var result pinnedGeneration
	err := scan(ctx, store, schema.AnalyticsWatermarkPrefix(), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		watermark, err := schema.UnmarshalAnalyticsWatermarkRecord(kv.Value)
		if err != nil {
			return err
		}
		if key.Epoch > result.epoch {
			result.epoch, result.watermark = key.Epoch, watermark
		}
		return nil
	})
	if err != nil || result.epoch == 0 {
		return result, false, err
	}
	value, found, err := store.Get(ctx, schema.AnalyticsManifestKey(result.epoch))
	if err != nil {
		return result, false, err
	}
	if !found {
		return result, false, fmt.Errorf("analytics watermark %d has no manifest", result.epoch)
	}
	result.manifest, err = schema.UnmarshalAnalyticsManifestRecord(value)
	if err != nil {
		return result, false, err
	}
	if result.manifest.Generation != result.watermark.ManifestGeneration {
		return result, false, fmt.Errorf("analytics manifest/watermark generation mismatch")
	}
	segments, err := resolveManifestSegments(ctx, store, result.manifest)
	if err != nil {
		return result, false, err
	}
	result.manifest.Segments = segments
	return result, true, nil
}

func resolveManifestSegments(ctx context.Context, store Store, manifest schema.AnalyticsManifestRecord) ([]uint64, error) {
	segments := append([]uint64(nil), manifest.Segments...)
	current := manifest
	for depth := 0; current.ParentGeneration != 0; depth++ {
		if depth >= maxManifestLayerDepth || current.LayerDepth == 0 {
			return nil, fmt.Errorf("analytics manifest parent chain exceeds %d layers", maxManifestLayerDepth)
		}
		value, found, err := store.Get(ctx, schema.AnalyticsManifestKey(current.ParentGeneration))
		if err != nil || !found {
			return nil, errors.Join(err, fmt.Errorf("analytics manifest parent %d is missing", current.ParentGeneration))
		}
		parent, err := schema.UnmarshalAnalyticsManifestRecord(value)
		if err != nil || parent.LayerDepth+1 != current.LayerDepth {
			return nil, errors.Join(err, fmt.Errorf("analytics manifest parent depth is invalid"))
		}
		segments = append(append([]uint64(nil), parent.Segments...), segments...)
		current = parent
	}
	return segments, nil
}

func decodeSegment(value []byte) (segmentRows, error) {
	record, err := schema.UnmarshalAnalyticsFactSegmentRecord(value)
	if err != nil {
		return segmentRows{}, err
	}
	var rows segmentRows
	targets := []any{&rows.Identity, &rows.UID, &rows.GID, &rows.CreatedAt, &rows.Basis, &rows.Continuity, &rows.Year, &rows.Month, &rows.ISOYear, &rows.Workweek, &rows.SVM, &rows.Volume, &rows.PathGroup, &rows.Size, &rows.SizeLog10}
	if len(record.Columns) != len(targets) {
		return rows, fmt.Errorf("analytics segment has %d columns, want %d", len(record.Columns), len(targets))
	}
	for index, column := range record.Columns {
		if column.Kind != schema.AnalyticsColumnKind(index+1) {
			return rows, fmt.Errorf("unsupported analytics column %d", column.Kind)
		}
		data := column.Data
		if column.Codec == schema.AnalyticsCodecZstd {
			data, err = analyticsZstdDecoder.DecodeAll(data, nil)
			if err != nil {
				return rows, fmt.Errorf("decompress analytics column %d: %w", column.Kind, err)
			}
		} else if column.Codec != schema.AnalyticsCodecRaw {
			return rows, fmt.Errorf("unsupported analytics column codec %d", column.Codec)
		}
		if err := json.Unmarshal(data, targets[index]); err != nil {
			return rows, err
		}
	}
	for _, length := range []int{len(rows.Identity), len(rows.UID), len(rows.GID), len(rows.CreatedAt), len(rows.Basis), len(rows.Continuity), len(rows.Year), len(rows.Month), len(rows.ISOYear), len(rows.Workweek), len(rows.SVM), len(rows.Volume), len(rows.PathGroup), len(rows.Size), len(rows.SizeLog10)} {
		if length != int(record.RowCount) {
			return rows, fmt.Errorf("analytics column row-count mismatch")
		}
	}
	return rows, nil
}

func rowFact(rows segmentRows, row int, dict map[schema.AnalyticsDictionaryKind]map[uint32]string) schema.AnalyticsFactRecord {
	identity := rows.Identity[row]
	return schema.AnalyticsFactRecord{Revision: identity.Revision, UID: rows.UID[row], GID: rows.GID[row], Known: identity.Known, CreatedAt: rows.CreatedAt[row], LogicalSize: rows.Size[row], CalendarYear: rows.Year[row], CalendarMonth: rows.Month[row], ISOYear: rows.ISOYear[row], Workweek: rows.Workweek[row], SizeLog10: rows.SizeLog10[row], SVM: dictionaryValue(dict, schema.AnalyticsDictionarySVM, rows.SVM[row]), Volume: dictionaryValue(dict, schema.AnalyticsDictionaryVolume, rows.Volume[row]), PathGroup: dictionaryValue(dict, schema.AnalyticsDictionaryPathGroup, rows.PathGroup[row]), Residency: schema.AnalyticsUnknown, CreationBasis: rows.Basis[row], IdentityGeneration: identity.Generation, IdentityContinuity: rows.Continuity[row]}
}

func loadDictionaries(ctx context.Context, store Store) (map[schema.AnalyticsDictionaryKind]map[uint32]string, error) {
	result := map[schema.AnalyticsDictionaryKind]map[uint32]string{}
	for _, kind := range []schema.AnalyticsDictionaryKind{schema.AnalyticsDictionarySVM, schema.AnalyticsDictionaryVolume, schema.AnalyticsDictionaryPathGroup} {
		result[kind] = map[uint32]string{}
		if err := scan(ctx, store, schema.AnalyticsDictionaryPrefix(kind), func(kv daemon.KeyValue) error {
			key, err := schema.ParseKey(kv.Key)
			if err != nil {
				return err
			}
			value, err := schema.UnmarshalAnalyticsDictionaryRecord(kv.Value)
			if err != nil {
				return err
			}
			result[kind][key.Ordinal] = value.Value
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func dictionaryValue(dict map[schema.AnalyticsDictionaryKind]map[uint32]string, kind schema.AnalyticsDictionaryKind, id uint32) string {
	if id == 0 {
		return "unknown"
	}
	if value := dict[kind][id]; value != "" {
		return value
	}
	return "unknown"
}

func pruneSegment(metadata schema.AnalyticsSegmentMetadataRecord, query Query) bool {
	if query.SizeMin != nil && metadata.MaxLogicalSize < *query.SizeMin {
		return true
	}
	if query.SizeMax != nil && metadata.MinLogicalSize >= *query.SizeMax {
		return true
	}
	return false
}

type indexRequest struct {
	dimension schema.AnalyticsDimension
	values    []uint64
	name      string
}

func indexedCandidates(ctx context.Context, store Store, segment uint64, rows segmentRows, query Query, dict map[schema.AnalyticsDictionaryKind]map[uint32]string) ([]byte, []string, []string, error) {
	requests := queryIndexes(query, dict)
	var candidates []byte
	var used, fallbacks []string
	for _, request := range requests {
		union := make([]byte, (len(rows.Identity)+7)/8)
		usable := true
		for _, value := range request.values {
			encoded, found, err := store.Get(ctx, schema.AnalyticsDimensionIndexKey(request.dimension, value, segment))
			if err != nil {
				return nil, nil, nil, err
			}
			if !found {
				usable = false
				break
			}
			index, err := schema.UnmarshalAnalyticsDimensionIndexRecord(encoded)
			if err != nil || index.RowCount != uint32(len(rows.Identity)) {
				usable = false
				break
			}
			bitmap := index.Bitmap
			if index.Codec == schema.AnalyticsCodecZstd {
				bitmap, err = analyticsZstdDecoder.DecodeAll(bitmap, nil)
			} else if index.Codec != schema.AnalyticsCodecRaw {
				err = fmt.Errorf("unsupported analytics bitmap codec %d", index.Codec)
			}
			if err != nil || len(bitmap) != len(union) || countBits(bitmap) != index.MatchCount {
				usable = false
				break
			}
			for offset := range union {
				union[offset] |= bitmap[offset]
			}
		}
		if !usable {
			fallbacks = append(fallbacks, request.name+":"+strconv.FormatUint(segment, 10))
			continue
		}
		used = append(used, request.name+":"+strconv.FormatUint(segment, 10))
		if candidates == nil {
			candidates = union
		} else {
			for offset := range candidates {
				candidates[offset] &= union[offset]
			}
		}
	}
	return candidates, used, fallbacks, nil
}

func queryIndexes(query Query, dict map[schema.AnalyticsDictionaryKind]map[uint32]string) []indexRequest {
	var result []indexRequest
	addInts := func(d schema.AnalyticsDimension, name string, values []int) {
		if len(values) == 0 {
			return
		}
		converted := make([]uint64, len(values))
		for index, value := range values {
			converted[index] = uint64(value)
		}
		result = append(result, indexRequest{d, converted, name})
	}
	if len(query.UIDs) != 0 {
		values := make([]uint64, len(query.UIDs))
		for index, value := range query.UIDs {
			values[index] = uint64(value)
		}
		result = append(result, indexRequest{schema.AnalyticsDimensionUID, values, "uid"})
	}
	if len(query.GIDs) != 0 {
		values := make([]uint64, len(query.GIDs))
		for index, value := range query.GIDs {
			values[index] = uint64(value)
		}
		result = append(result, indexRequest{schema.AnalyticsDimensionGID, values, "gid"})
	}
	addInts(schema.AnalyticsDimensionCalendarYear, "year", query.Years)
	addInts(schema.AnalyticsDimensionCalendarMonth, "month", query.Months)
	addInts(schema.AnalyticsDimensionISOYear, "iso-year", query.ISOYears)
	addInts(schema.AnalyticsDimensionWorkweek, "workweek", query.Workweeks)
	addInts(schema.AnalyticsDimensionSizeLog10, "size-log10", query.SizeLog10)
	for _, item := range []struct {
		dimension schema.AnalyticsDimension
		kind      schema.AnalyticsDictionaryKind
		name      string
		values    []string
	}{{schema.AnalyticsDimensionSVM, schema.AnalyticsDictionarySVM, "svm", query.SVMs}, {schema.AnalyticsDimensionVolume, schema.AnalyticsDictionaryVolume, "volume", query.Volumes}, {schema.AnalyticsDimensionPathGroup, schema.AnalyticsDictionaryPathGroup, "path-group", query.PathGroups}} {
		if len(item.values) == 0 {
			continue
		}
		var ids []uint64
		for id, value := range dict[item.kind] {
			if hasString(item.values, value) {
				ids = append(ids, uint64(id))
			}
		}
		if len(ids) != 0 {
			result = append(result, indexRequest{item.dimension, ids, item.name})
		}
	}
	return result
}

func matchesComplete(fact schema.AnalyticsFactRecord, query Query) bool {
	if !query.IncludeIncomplete && fact.IdentityContinuity == schema.AnalyticsContinuityUnknown {
		return false
	}
	if !matches(fact, query) {
		return false
	}
	if len(query.CreationBases) != 0 && !hasString(query.CreationBases, creationBasisName(fact.CreationBasis)) {
		return false
	}
	return len(query.IdentityContinuities) == 0 || hasString(query.IdentityContinuities, continuityName(fact.IdentityContinuity))
}

func creationBasisName(value schema.AnalyticsCreationBasis) string {
	switch value {
	case schema.AnalyticsCTime:
		return "ctime"
	case schema.AnalyticsMTime:
		return "mtime"
	case schema.AnalyticsBirthTime:
		return "birth-time"
	case schema.AnalyticsFirstSeen:
		return "first-seen"
	default:
		return "unknown"
	}
}
func continuityName(value schema.AnalyticsIdentityContinuity) string {
	switch value {
	case schema.AnalyticsContinuityProven:
		return "proven"
	case schema.AnalyticsContinuitySourceGeneration:
		return "source-generation"
	default:
		return "unknown"
	}
}

type cacheRecord struct {
	ExpiresAt int64  `json:"expires_at"`
	Result    Result `json:"result"`
}
type heatRecord struct {
	Hits      uint64 `json:"hits"`
	ScanCost  uint64 `json:"scan_cost"`
	UpdatedAt int64  `json:"updated_at"`
}

type viewRecord struct {
	Predicates           []byte   `json:"predicates"`
	Shape                []byte   `json:"shape"`
	GroupBy              []string `json:"group_by"`
	RepositoryGeneration uint64   `json:"repository_generation"`
	ClassificationEpoch  uint64   `json:"classification_epoch"`
	AppliedCommit        uint64   `json:"applied_commit"`
	ExpiresAt            int64    `json:"expires_at"`
	LastUsed             int64    `json:"last_used"`
	Result               Result   `json:"result"`
}

func readCompatibleView(ctx context.Context, store Store, query Query, pinned pinnedGeneration) (Result, bool, []string, error) {
	predicates, err := canonicalPredicates(query)
	if err != nil {
		return Result{}, false, nil, err
	}
	now := time.Now().Unix()
	var selected *viewRecord
	var selectedKey []byte
	fallbacks := make([]string, 0)
	err = scan(ctx, store, []byte("aq:view:"), func(kv daemon.KeyValue) error {
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(kv.Value)
		if decodeErr != nil {
			fallbacks = append(fallbacks, "malformed")
			return nil
		}
		var view viewRecord
		if json.Unmarshal(record.Payload, &view) != nil || len(view.Predicates) == 0 || len(view.GroupBy) == 0 {
			fallbacks = append(fallbacks, "malformed")
			return nil
		}
		if view.RepositoryGeneration != pinned.watermark.RepositoryGeneration || view.ClassificationEpoch != pinned.epoch || view.AppliedCommit != pinned.watermark.AppliedCommit {
			fallbacks = append(fallbacks, "stale-generation")
			return nil
		}
		if view.ExpiresAt < now {
			fallbacks = append(fallbacks, "expired")
			return nil
		}
		if !bytes.Equal(view.Predicates, predicates) || !groupSubset(query.GroupBy, view.GroupBy) {
			fallbacks = append(fallbacks, "incompatible")
			return nil
		}
		if selected == nil || len(view.GroupBy) < len(selected.GroupBy) {
			copy := view
			selected = &copy
			selectedKey = append([]byte(nil), kv.Key...)
		}
		return nil
	})
	if err != nil || selected == nil {
		return Result{}, false, fallbacks, err
	}
	result := rollupView(selected.Result, query.GroupBy)
	result.Cached = false
	result.Explain = Explain{Source: "adaptive-view"}
	selected.LastUsed = now
	payload, _ := json.Marshal(selected)
	if value, marshalErr := (schema.AnalyticsQueryRecord{Payload: payload}).MarshalBinary(); marshalErr == nil {
		if writeErr := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: selectedKey, Value: value}}, nil, false); writeErr != nil {
			return Result{}, false, fallbacks, writeErr
		}
	} else {
		return Result{}, false, fallbacks, marshalErr
	}
	return result, true, fallbacks, nil
}

func groupSubset(requested, available []string) bool {
	for _, name := range requested {
		if !slices.Contains(available, name) {
			return false
		}
	}
	return true
}

func rollupView(source Result, groupBy []string) Result {
	result := source
	result.Groups = nil
	if len(groupBy) == 0 {
		return result
	}
	groups := map[string]*Group{}
	for _, sourceGroup := range source.Groups {
		dimensions := make(map[string]string, len(groupBy))
		for _, name := range groupBy {
			dimensions[name] = sourceGroup.Dimensions[name]
		}
		key, _ := json.Marshal(dimensions)
		group := groups[string(key)]
		if group == nil {
			group = &Group{Dimensions: dimensions}
			groups[string(key)] = group
		}
		group.Files += sourceGroup.Files
		group.LogicalBytes += sourceGroup.LogicalBytes
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Groups = append(result.Groups, *groups[key])
	}
	return result
}

func readResultCache(ctx context.Context, store Store, key []byte) (Result, bool, error) {
	value, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return Result{}, false, err
	}
	record, err := schema.UnmarshalAnalyticsQueryRecord(value)
	if err != nil {
		return Result{}, false, nil
	}
	var cached cacheRecord
	if json.Unmarshal(record.Payload, &cached) != nil || cached.ExpiresAt < time.Now().Unix() {
		return Result{}, false, nil
	}
	return cached.Result, true, nil
}

func updateCache(ctx context.Context, store Store, query Query, hash schema.ID, resultKey []byte, result Result) error {
	analyticsPublishMu.Lock()
	defer analyticsPublishMu.Unlock()
	metadata, err := Status(ctx, store)
	if err != nil {
		return err
	}
	var config Config
	if metadata.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(metadata.ConfigJSON), &config); err != nil {
			return err
		}
	}
	config = config.normalized()
	heatKey := schema.AnalyticsQueryHeatKey(hash)
	heat := heatRecord{}
	if value, found, err := store.Get(ctx, heatKey); err != nil {
		return err
	} else if found {
		record, err := schema.UnmarshalAnalyticsQueryRecord(value)
		if err == nil {
			_ = json.Unmarshal(record.Payload, &heat)
		}
	}
	heat.Hits++
	heat.ScanCost += result.Explain.RowsScanned
	heat.UpdatedAt = time.Now().Unix()
	heatPayload, _ := json.Marshal(heat)
	heatValue, err := (schema.AnalyticsQueryRecord{Payload: heatPayload}).MarshalBinary()
	if err != nil {
		return err
	}
	puts := []daemon.Mutation{{Key: heatKey, Value: heatValue}}
	if heat.Hits >= config.CacheAfter || heat.ScanCost >= uint64(config.SegmentRows) {
		payload, _ := json.Marshal(cacheRecord{ExpiresAt: time.Now().Unix() + config.CacheTTLSeconds, Result: result})
		value, err := (schema.AnalyticsQueryRecord{Payload: payload}).MarshalBinary()
		if err != nil {
			return err
		}
		puts = append(puts, daemon.Mutation{Key: resultKey, Value: value})
		if len(query.GroupBy) != 0 && len(result.Groups) != 0 && len(result.Groups) <= config.CacheMaxEntries {
			predicates, predicateErr := canonicalPredicates(query)
			shape, shapeErr := canonicalShape(query)
			if predicateErr != nil || shapeErr != nil {
				return errors.Join(predicateErr, shapeErr)
			}
			viewID := schema.ID(sha256.Sum256(append(append([]byte(nil), predicates...), shape...)))
			now := time.Now().Unix()
			view := viewRecord{Predicates: predicates, Shape: shape, GroupBy: append([]string(nil), query.GroupBy...), RepositoryGeneration: result.Watermark.RepositoryGeneration, ClassificationEpoch: result.Watermark.ClassificationEpoch, AppliedCommit: result.Watermark.AppliedCommit, ExpiresAt: now + config.CacheTTLSeconds, LastUsed: now, Result: result}
			viewPayload, _ := json.Marshal(view)
			viewValue, marshalErr := (schema.AnalyticsQueryRecord{Payload: viewPayload}).MarshalBinary()
			if marshalErr != nil {
				return marshalErr
			}
			puts = append(puts, daemon.Mutation{Key: schema.AnalyticsQueryViewKey(viewID, result.Generation), Value: viewValue})
		}
		if heat.Hits == config.CacheAfter && metadata.Generation == result.Generation {
			metadata.CacheEntries++
			metadataValue, err := metadata.MarshalBinary()
			if err != nil {
				return err
			}
			puts = append(puts, daemon.Mutation{Key: schema.AnalyticsMetadataKey(), Value: metadataValue})
		}
	}
	if err := store.WriteMutableBatch(ctx, puts, nil, false); err != nil {
		return fmt.Errorf("write analytics cache: %w", err)
	}
	return cleanupCache(ctx, store, config.CacheMaxEntries)
}

func cleanupCache(ctx context.Context, store Store, maximum int) error {
	type entry struct {
		key     []byte
		expires int64
		rank    int64
	}
	var entries []entry
	if err := scan(ctx, store, []byte("aq:result:"), func(kv daemon.KeyValue) error {
		expires := int64(0)
		if record, err := schema.UnmarshalAnalyticsQueryRecord(kv.Value); err == nil {
			var cached cacheRecord
			if json.Unmarshal(record.Payload, &cached) == nil {
				expires = cached.ExpiresAt
			}
		}
		entries = append(entries, entry{key: append([]byte(nil), kv.Key...), expires: expires})
		return nil
	}); err != nil {
		return err
	}
	if len(entries) <= maximum {
		entries = nil
	} else {
		sort.Slice(entries, func(i, j int) bool { return entries[i].expires < entries[j].expires })
	}
	deleteCount := len(entries) - maximum
	if deleteCount < 0 {
		deleteCount = 0
	}
	deletes := make([][]byte, deleteCount)
	for index := range deletes {
		deletes[index] = entries[index].key
	}
	type heatEntry struct {
		key     []byte
		updated int64
	}
	var heatEntries []heatEntry
	if err := scan(ctx, store, []byte("aq:heat:"), func(kv daemon.KeyValue) error {
		updated := int64(0)
		if record, err := schema.UnmarshalAnalyticsQueryRecord(kv.Value); err == nil {
			var heat heatRecord
			if json.Unmarshal(record.Payload, &heat) == nil {
				updated = heat.UpdatedAt
			}
		}
		heatEntries = append(heatEntries, heatEntry{append([]byte(nil), kv.Key...), updated})
		return nil
	}); err != nil {
		return err
	}
	if len(heatEntries) > maximum {
		sort.Slice(heatEntries, func(i, j int) bool { return heatEntries[i].updated < heatEntries[j].updated })
		for _, entry := range heatEntries[:len(heatEntries)-maximum] {
			deletes = append(deletes, entry.key)
		}
	}
	var viewEntries []entry
	if err := scan(ctx, store, []byte("aq:view:"), func(kv daemon.KeyValue) error {
		expires := int64(0)
		lastUsed := int64(0)
		if record, err := schema.UnmarshalAnalyticsQueryRecord(kv.Value); err == nil {
			var view viewRecord
			if json.Unmarshal(record.Payload, &view) == nil {
				expires = view.ExpiresAt
				lastUsed = view.LastUsed
			}
		}
		viewEntries = append(viewEntries, entry{key: append([]byte(nil), kv.Key...), expires: expires, rank: lastUsed})
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(viewEntries, func(i, j int) bool {
		if viewEntries[i].rank != viewEntries[j].rank {
			return viewEntries[i].rank < viewEntries[j].rank
		}
		return viewEntries[i].expires < viewEntries[j].expires
	})
	for len(viewEntries) > maximum {
		deletes = append(deletes, viewEntries[0].key)
		viewEntries = viewEntries[1:]
	}
	for len(viewEntries) > 0 && viewEntries[0].expires < time.Now().Unix() {
		deletes = append(deletes, viewEntries[0].key)
		viewEntries = viewEntries[1:]
	}
	if len(deletes) == 0 {
		return nil
	}
	return store.WriteMutableBatch(ctx, nil, deletes, false)
}

func writeCandidateViews(ctx context.Context, store Store, generation, parentGeneration uint64, facts []buildFact) error {
	aggregates := map[string]schema.AnalyticsAggregateRecord{}
	summaries := map[string]schema.AnalyticsSummaryRecord{}
	addAggregate := func(key []byte, size uint64) {
		value := aggregates[string(key)]
		value.FilesAdded++
		value.BytesAdded += size
		aggregates[string(key)] = value
	}
	addDeletion := func(key []byte, size uint64) {
		value := aggregates[string(key)]
		value.FilesDeleted++
		value.BytesDeleted += size
		aggregates[string(key)] = value
	}
	for _, item := range facts {
		fact := item.fact
		size := uint64(0)
		if fact.Known&schema.KnownSize != 0 {
			size = fact.LogicalSize
		}
		if fact.CreationBasis != schema.AnalyticsTimeUnknown {
			instant := time.Unix(0, fact.CreatedAt).UTC()
			year := time.Date(instant.Year(), 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
			month := time.Date(instant.Year(), instant.Month(), 1, 0, 0, 0, 0, time.UTC).UnixNano()
			weekday := (int(instant.Weekday()) + 6) % 7
			week := time.Date(instant.Year(), instant.Month(), instant.Day()-weekday, 0, 0, 0, 0, time.UTC).UnixNano()
			addAggregate(schema.GrowthTimeKey(schema.AnalyticsGranularityYear, year, schema.TierUnknown), size)
			addAggregate(schema.GrowthTimeKey(schema.AnalyticsGranularityMonth, month, schema.TierUnknown), size)
			addAggregate(schema.GrowthTimeKey(schema.AnalyticsGranularityWeek, week, schema.TierUnknown), size)
			if fact.PathGroup != "unknown" {
				addAggregate(schema.GrowthPathKey(fact.PathGroup, schema.AnalyticsGranularityYear, year), size)
				addAggregate(schema.GrowthPathKey(fact.PathGroup, schema.AnalyticsGranularityMonth, month), size)
				addAggregate(schema.GrowthPathKey(fact.PathGroup, schema.AnalyticsGranularityWeek, week), size)
			}
			if fact.Known&schema.KnownUID != 0 {
				addAggregate(schema.UserChurnKey(fact.UID, schema.AnalyticsGranularityYear, year), size)
				addAggregate(schema.UserChurnKey(fact.UID, schema.AnalyticsGranularityMonth, month), size)
				addAggregate(schema.UserChurnKey(fact.UID, schema.AnalyticsGranularityWeek, week), size)
			}
		}
		if item.lastComplete != 0 && (fact.Residency == schema.AnalyticsArchiveOnly || fact.Residency == schema.AnalyticsExpired) {
			instant := time.Unix(0, item.lastComplete).UTC()
			year := time.Date(instant.Year(), 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
			month := time.Date(instant.Year(), instant.Month(), 1, 0, 0, 0, 0, time.UTC).UnixNano()
			weekday := (int(instant.Weekday()) + 6) % 7
			week := time.Date(instant.Year(), instant.Month(), instant.Day()-weekday, 0, 0, 0, 0, time.UTC).UnixNano()
			addDeletion(schema.GrowthTimeKey(schema.AnalyticsGranularityYear, year, schema.TierUnknown), size)
			addDeletion(schema.GrowthTimeKey(schema.AnalyticsGranularityMonth, month, schema.TierUnknown), size)
			addDeletion(schema.GrowthTimeKey(schema.AnalyticsGranularityWeek, week, schema.TierUnknown), size)
			if fact.PathGroup != "unknown" {
				addDeletion(schema.GrowthPathKey(fact.PathGroup, schema.AnalyticsGranularityYear, year), size)
				addDeletion(schema.GrowthPathKey(fact.PathGroup, schema.AnalyticsGranularityMonth, month), size)
				addDeletion(schema.GrowthPathKey(fact.PathGroup, schema.AnalyticsGranularityWeek, week), size)
			}
			if fact.Known&schema.KnownUID != 0 {
				addDeletion(schema.UserChurnKey(fact.UID, schema.AnalyticsGranularityYear, year), size)
				addDeletion(schema.UserChurnKey(fact.UID, schema.AnalyticsGranularityMonth, month), size)
				addDeletion(schema.UserChurnKey(fact.UID, schema.AnalyticsGranularityWeek, week), size)
			}
		}
		if fact.Known&schema.KnownUID != 0 {
			stats := summaries[string(schema.UserStatsKey(fact.UID, fact.Residency))]
			stats.ActiveFiles++
			stats.ActiveBytes += size
			summaries[string(schema.UserStatsKey(fact.UID, fact.Residency))] = stats
			if fact.Residency == schema.AnalyticsLive {
				summary := summaries[string(schema.UserSummaryKey(fact.UID))]
				summary.ActiveFiles++
				summary.ActiveBytes += size
				summaries[string(schema.UserSummaryKey(fact.UID))] = summary
			}
		}
		if fact.Known&schema.KnownGID != 0 {
			stats := summaries[string(schema.GroupStatsKey(fact.GID, fact.Residency))]
			stats.ActiveFiles++
			stats.ActiveBytes += size
			summaries[string(schema.GroupStatsKey(fact.GID, fact.Residency))] = stats
			if fact.Residency == schema.AnalyticsLive {
				summary := summaries[string(schema.GroupSummaryKey(fact.GID))]
				summary.ActiveFiles++
				summary.ActiveBytes += size
				summaries[string(schema.GroupSummaryKey(fact.GID))] = summary
			}
		}
	}
	puts := make([]daemon.Mutation, 0, pageSize)
	appendPut := func(key []byte, value []byte) error {
		puts = append(puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(generation, key), Value: value})
		if len(puts) < pageSize {
			return nil
		}
		if err := store.WriteMutableBatch(ctx, puts, nil, false); err != nil {
			return err
		}
		puts = puts[:0]
		return nil
	}
	for key, record := range aggregates {
		derivedKey := schema.AnalyticsDerivedKey(generation, []byte(key))
		if current, found, err := store.Get(ctx, derivedKey); err != nil {
			return err
		} else if found {
			prior, err := schema.UnmarshalAnalyticsAggregateRecord(current)
			if err != nil {
				return err
			}
			record.BytesAdded += prior.BytesAdded
			record.BytesDeleted += prior.BytesDeleted
			record.FilesAdded += prior.FilesAdded
			record.FilesDeleted += prior.FilesDeleted
		} else if parentGeneration != 0 {
			current, found, err := getActiveDerived(ctx, store, parentGeneration, []byte(key))
			if err != nil {
				return err
			}
			if found {
				prior, err := schema.UnmarshalAnalyticsAggregateRecord(current)
				if err != nil {
					return err
				}
				record.BytesAdded += prior.BytesAdded
				record.BytesDeleted += prior.BytesDeleted
				record.FilesAdded += prior.FilesAdded
				record.FilesDeleted += prior.FilesDeleted
			}
		}
		value, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		if err := appendPut([]byte(key), value); err != nil {
			return err
		}
	}
	for key, record := range summaries {
		derivedKey := schema.AnalyticsDerivedKey(generation, []byte(key))
		if current, found, err := store.Get(ctx, derivedKey); err != nil {
			return err
		} else if found {
			prior, err := schema.UnmarshalAnalyticsSummaryRecord(current)
			if err != nil {
				return err
			}
			record.ActiveBytes += prior.ActiveBytes
			record.ActiveFiles += prior.ActiveFiles
			record.UniqueBlobCount += prior.UniqueBlobCount
			record.UniqueBlobBytes += prior.UniqueBlobBytes
		} else if parentGeneration != 0 {
			current, found, err := getActiveDerived(ctx, store, parentGeneration, []byte(key))
			if err != nil {
				return err
			}
			if found {
				prior, err := schema.UnmarshalAnalyticsSummaryRecord(current)
				if err != nil {
					return err
				}
				record.ActiveBytes += prior.ActiveBytes
				record.ActiveFiles += prior.ActiveFiles
				record.UniqueBlobCount += prior.UniqueBlobCount
				record.UniqueBlobBytes += prior.UniqueBlobBytes
			}
		}
		value, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		if err := appendPut([]byte(key), value); err != nil {
			return err
		}
	}
	for _, item := range facts {
		if item.fact.Known&schema.KnownUID == 0 {
			continue
		}
		inodeValue, err := (schema.AnalyticsUserInodeRecord{LatestRevision: item.identity.Revision, PathSample: item.fact.SourcePath}).MarshalBinary()
		if err != nil {
			return err
		}
		if err := appendPut(schema.UserInodeKey(item.fact.UID, item.identity.FSID, item.identity.Inode), inodeValue); err != nil {
			return err
		}
		if item.fact.CreatedAt == 0 {
			continue
		}
		revisionValue, found, err := store.Get(ctx, schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision))
		if err != nil || !found {
			return errors.Join(err, fmt.Errorf("analytics fact revision %d:%d:%d is missing", item.identity.FSID, item.identity.Inode, item.identity.Revision))
		}
		revision, err := schema.UnmarshalInodeRevision(revisionValue)
		if err != nil {
			return err
		}
		if err := visitInodeContent(ctx, store, revision, func(ordinal uint32, blob schema.ID) error {
			blobValue, err := (schema.AnalyticsUserBlobRecord{ReferenceCount: 1, FirstSeen: item.fact.CreatedAt}).MarshalBinary()
			if err != nil {
				return err
			}
			key := schema.UserBlobContributionKey(item.fact.UID, blob, item.identity.FSID, item.identity.Inode, item.identity.Generation, ordinal)
			if err := appendPut(key, blobValue); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return writeBatches(ctx, store, puts)
}

func cleanupOldDerived(ctx context.Context, store Store, generation uint64) (uint64, error) {
	pinnedGenerations, err := pinnedJobGenerations(ctx, store)
	if err != nil {
		return 0, err
	}
	var deletes [][]byte
	for _, prefix := range [][]byte{schema.AnalyticsFactPrefix(), schema.AnalyticsCachePrefix()} {
		if err := scan(ctx, store, prefix, func(kv daemon.KeyValue) error {
			key, err := schema.ParseKey(kv.Key)
			if err != nil {
				return err
			}
			if key.Kind == schema.KeyAnalyticsFact || key.Kind == schema.KeyAnalyticsCache {
				deletes = append(deletes, append([]byte(nil), kv.Key...))
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	if generation > 2 {
		minimum := (generation - 1) << 32
		for _, prefix := range [][]byte{schema.AnalyticsFactSegmentPrefix(), schema.AnalyticsSegmentMetadataPrefix()} {
			if err := scan(ctx, store, prefix, func(kv daemon.KeyValue) error {
				key, err := schema.ParseKey(kv.Key)
				if err != nil {
					return err
				}
				if key.Generation < minimum {
					if _, pinned := pinnedGenerations[key.Generation>>32]; pinned {
						return nil
					}
					deletes = append(deletes, append([]byte(nil), kv.Key...))
				}
				return nil
			}); err != nil {
				return 0, err
			}
		}
		if err := scan(ctx, store, []byte("ai:"), func(kv daemon.KeyValue) error {
			key, err := schema.ParseKey(kv.Key)
			if err != nil {
				return err
			}
			if key.Generation < minimum {
				if _, pinned := pinnedGenerations[key.Generation>>32]; pinned {
					return nil
				}
				deletes = append(deletes, append([]byte(nil), kv.Key...))
			}
			return nil
		}); err != nil {
			return 0, err
		}
		for _, prefix := range [][]byte{schema.AnalyticsManifestPrefix(), schema.AnalyticsWatermarkPrefix()} {
			if err := scan(ctx, store, prefix, func(kv daemon.KeyValue) error {
				key, err := schema.ParseKey(kv.Key)
				if err != nil {
					return err
				}
				if key.Epoch+1 < generation {
					if _, pinned := pinnedGenerations[key.Epoch]; pinned {
						return nil
					}
					deletes = append(deletes, append([]byte(nil), kv.Key...))
				}
				return nil
			}); err != nil {
				return 0, err
			}
		}
		if err := scan(ctx, store, []byte("av1:"), func(kv daemon.KeyValue) error {
			key, err := schema.ParseKey(kv.Key)
			if err != nil {
				return err
			}
			if key.ViewGeneration+1 < generation {
				if _, pinned := pinnedGenerations[key.ViewGeneration]; pinned {
					return nil
				}
				deletes = append(deletes, append([]byte(nil), kv.Key...))
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	if len(deletes) != 0 {
		if err := store.WriteMutableBatch(ctx, nil, deletes, false); err != nil {
			return 0, err
		}
	}
	return uint64(len(deletes)), nil
}

func pinnedJobGenerations(ctx context.Context, store Store) (map[uint64]struct{}, error) {
	result := map[uint64]struct{}{}
	err := scan(ctx, store, schema.AnalyticsQueryJobPrefix(), func(kv daemon.KeyValue) error {
		job, err := schema.UnmarshalAnalyticsQueryJobRecord(kv.Value)
		if err != nil {
			return err
		}
		if job.State == schema.AnalyticsQueryComplete || job.State == schema.AnalyticsQueryCancelled {
			return nil
		}
		result[job.ClassificationEpoch] = struct{}{}
		manifestValue, found, err := store.Get(ctx, schema.AnalyticsManifestKey(job.ClassificationEpoch))
		if err != nil || !found {
			return err
		}
		manifest, err := schema.UnmarshalAnalyticsManifestRecord(manifestValue)
		if err != nil {
			return err
		}
		for manifest.ParentGeneration != 0 {
			result[manifest.ParentGeneration] = struct{}{}
			manifestValue, found, err = store.Get(ctx, schema.AnalyticsManifestKey(manifest.ParentGeneration))
			if err != nil || !found {
				return err
			}
			manifest, err = schema.UnmarshalAnalyticsManifestRecord(manifestValue)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

func writeBatches(ctx context.Context, store Store, puts []daemon.Mutation) error {
	for len(puts) > 0 {
		count := pageSize
		if count > len(puts) {
			count = len(puts)
		}
		if err := store.WriteMutableBatch(ctx, puts[:count], nil, false); err != nil {
			return err
		}
		puts = puts[count:]
	}
	return nil
}
func bitSet(bitmap []byte, row int) bool {
	return row >= 0 && row/8 < len(bitmap) && bitmap[row/8]&(1<<uint(row%8)) != 0
}
func countBits(bitmap []byte) uint32 {
	var result uint32
	for _, value := range bitmap {
		for value != 0 {
			result += uint32(value & 1)
			value >>= 1
		}
	}
	return result
}

func Start(ctx context.Context, store Store, query Query) (schema.ID, error) {
	if err := query.Validate(); err != nil {
		return schema.ID{}, err
	}
	pinned, found, err := pinLatest(ctx, store)
	if err != nil {
		return schema.ID{}, err
	}
	if !found {
		return schema.ID{}, fmt.Errorf("async analytics requires a published manifest")
	}
	canonical, err := canonicalQuery(query)
	if err != nil {
		return schema.ID{}, err
	}
	id := schema.ID(sha256.Sum256(append(canonical, uint64Bytes(uint64(time.Now().UnixNano()))...)))
	record := schema.AnalyticsQueryJobRecord{State: schema.AnalyticsQueryPending, CanonicalQuery: canonical, RepositoryGeneration: pinned.watermark.RepositoryGeneration, ClassificationEpoch: pinned.epoch, AppliedCommit: pinned.watermark.AppliedCommit, UpdatedAt: time.Now().UnixNano()}
	value, err := record.MarshalBinary()
	if err != nil {
		return schema.ID{}, err
	}
	if err := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsQueryJobKey(id), Value: value}}, nil, true); err != nil {
		return schema.ID{}, err
	}
	return id, nil
}

func Resume(ctx context.Context, store Store, id schema.ID) (Result, error) {
	record, err := loadJob(ctx, store, id)
	if err != nil {
		return Result{}, err
	}
	if record.State == schema.AnalyticsQueryCancelled {
		return Result{}, context.Canceled
	}
	if record.State == schema.AnalyticsQueryComplete {
		var result Result
		if err := json.Unmarshal(record.Result, &result); err != nil {
			return Result{}, err
		}
		return result, nil
	}
	if record.State == schema.AnalyticsQueryFailed {
		record.Error = ""
	}
	var query Query
	if err := json.Unmarshal(record.CanonicalQuery, &query); err != nil {
		return Result{}, err
	}
	pinned, err := pinJobGeneration(ctx, store, record)
	if err != nil {
		record.State = schema.AnalyticsQueryFailed
		record.Error = err.Error()
		record.UpdatedAt = time.Now().UnixNano()
		if saveErr := saveJob(context.WithoutCancel(ctx), store, id, record); saveErr != nil {
			return Result{}, errors.Join(err, saveErr)
		}
		return Result{}, err
	}
	result := Result{SchemaVersion: 2, Generation: pinned.manifest.Generation, Watermark: WatermarkInfo{RepositoryGeneration: record.RepositoryGeneration, ClassificationEpoch: record.ClassificationEpoch, AppliedCommit: record.AppliedCommit, AppliedAt: pinned.watermark.AppliedAt}}
	if len(record.Result) != 0 {
		if err := json.Unmarshal(record.Result, &result); err != nil {
			return Result{}, fmt.Errorf("decode analytics job checkpoint: %w", err)
		}
	}
	completed := make(map[uint64]struct{}, len(record.CompletedSegments))
	for _, segment := range record.CompletedSegments {
		completed[segment] = struct{}{}
	}
	record.State = schema.AnalyticsQueryRunning
	record.UpdatedAt = time.Now().UnixNano()
	if err := saveJob(ctx, store, id, record); err != nil {
		return Result{}, err
	}
	for _, segment := range pinned.manifest.Segments {
		if _, done := completed[segment]; done {
			continue
		}
		if err := ctx.Err(); err != nil {
			record.State = schema.AnalyticsQueryPending
			record.UpdatedAt = time.Now().UnixNano()
			if saveErr := saveJob(context.WithoutCancel(ctx), store, id, record); saveErr != nil {
				return Result{}, errors.Join(err, saveErr)
			}
			return Result{}, err
		}
		if err := executeJobSegment(ctx, store, query, pinned, segment, &result); err != nil {
			record.UpdatedAt = time.Now().UnixNano()
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				record.State = schema.AnalyticsQueryPending
			} else {
				record.State = schema.AnalyticsQueryFailed
				record.Error = err.Error()
			}
			if saveErr := saveJob(context.WithoutCancel(ctx), store, id, record); saveErr != nil {
				return Result{}, errors.Join(err, saveErr)
			}
			return Result{}, err
		}
		record.CompletedSegments = append(record.CompletedSegments, segment)
		record.RowsScanned = result.Explain.RowsScanned
		record.Result, _ = json.Marshal(result)
		record.UpdatedAt = time.Now().UnixNano()
		if err := saveJob(ctx, store, id, record); err != nil {
			return Result{}, err
		}
	}
	hash := sha256.Sum256(record.CanonicalQuery)
	cacheKey := schema.AnalyticsQueryResultKey(schema.ID(hash), record.RepositoryGeneration, record.ClassificationEpoch, record.AppliedCommit)
	if err := updateCache(ctx, store, query, schema.ID(hash), cacheKey, result); err != nil {
		record.State = schema.AnalyticsQueryFailed
		record.Error = err.Error()
		record.UpdatedAt = time.Now().UnixNano()
		if saveErr := saveJob(context.WithoutCancel(ctx), store, id, record); saveErr != nil {
			return Result{}, errors.Join(err, saveErr)
		}
		return Result{}, err
	}
	record.State = schema.AnalyticsQueryComplete
	record.Result, _ = json.Marshal(result)
	record.UpdatedAt = time.Now().UnixNano()
	if err := saveJob(ctx, store, id, record); err != nil {
		return Result{}, err
	}
	return result, nil
}

func pinJobGeneration(ctx context.Context, store Store, job schema.AnalyticsQueryJobRecord) (pinnedGeneration, error) {
	var pinned pinnedGeneration
	watermarkValue, found, err := store.Get(ctx, schema.AnalyticsWatermarkKey(job.ClassificationEpoch))
	if err != nil {
		return pinned, err
	}
	if !found {
		return pinned, fmt.Errorf("pinned analytics generation %d is unavailable: watermark was reclaimed", job.ClassificationEpoch)
	}
	pinned.watermark, err = schema.UnmarshalAnalyticsWatermarkRecord(watermarkValue)
	if err != nil {
		return pinned, err
	}
	if pinned.watermark.RepositoryGeneration != job.RepositoryGeneration || pinned.watermark.AppliedCommit != job.AppliedCommit {
		return pinned, fmt.Errorf("pinned analytics generation %d changed", job.ClassificationEpoch)
	}
	manifestValue, found, err := store.Get(ctx, schema.AnalyticsManifestKey(job.ClassificationEpoch))
	if err != nil {
		return pinned, err
	}
	if !found {
		return pinned, fmt.Errorf("pinned analytics generation %d is unavailable: manifest was reclaimed", job.ClassificationEpoch)
	}
	pinned.manifest, err = schema.UnmarshalAnalyticsManifestRecord(manifestValue)
	pinned.epoch = job.ClassificationEpoch
	if err != nil {
		return pinned, err
	}
	if pinned.manifest.Generation != pinned.watermark.ManifestGeneration {
		return pinned, fmt.Errorf("pinned analytics generation %d is inconsistent", job.ClassificationEpoch)
	}
	segments, err := resolveManifestSegments(ctx, store, pinned.manifest)
	if err != nil {
		return pinned, err
	}
	pinned.manifest.Segments = segments
	return pinned, nil
}

func getActiveDerived(ctx context.Context, store Store, generation uint64, legacyKey []byte) ([]byte, bool, error) {
	for depth := 0; generation != 0; depth++ {
		if depth > maxManifestLayerDepth {
			return nil, false, fmt.Errorf("analytics derived parent chain exceeds %d layers", maxManifestLayerDepth)
		}
		value, found, err := store.Get(ctx, schema.AnalyticsDerivedKey(generation, legacyKey))
		if err != nil || found {
			if found && bytes.Equal(value, analyticsDerivedTombstone) {
				return nil, false, nil
			}
			return value, found, err
		}
		manifestValue, found, err := store.Get(ctx, schema.AnalyticsManifestKey(generation))
		if err != nil {
			return nil, false, err
		}
		if !found {
			break
		}
		manifest, err := schema.UnmarshalAnalyticsManifestRecord(manifestValue)
		if err != nil {
			return nil, false, err
		}
		generation = manifest.ParentGeneration
	}
	return store.Get(ctx, legacyKey)
}

type derivedScanIterator struct {
	generation uint64
	prefix     []byte
	cursor     []byte
	items      []daemon.KeyValue
	index      int
	done       bool
}

func (iterator *derivedScanIterator) head(ctx context.Context, store Store) (daemon.KeyValue, bool, error) {
	for iterator.index >= len(iterator.items) && !iterator.done {
		items, done, err := store.ScanPrefix(ctx, schema.AnalyticsDerivedPrefix(iterator.generation, iterator.prefix), iterator.cursor, 256)
		if err != nil {
			return daemon.KeyValue{}, false, err
		}
		iterator.items, iterator.index, iterator.done = items, 0, done
		if len(items) == 0 && !done {
			return daemon.KeyValue{}, false, fmt.Errorf("analytics derived scan returned an empty continuation page")
		}
		if len(items) != 0 {
			iterator.cursor = append(append(iterator.cursor[:0], items[len(items)-1].Key...), 0)
		}
	}
	if iterator.index >= len(iterator.items) {
		return daemon.KeyValue{}, false, nil
	}
	return iterator.items[iterator.index], true, nil
}

func scanActiveDerivedPrefix(ctx context.Context, store Store, generation uint64, prefix []byte, visit func(daemon.KeyValue) error) error {
	var iterators []*derivedScanIterator
	for depth := 0; generation != 0; depth++ {
		if depth > maxManifestLayerDepth {
			return fmt.Errorf("analytics derived parent chain exceeds %d layers", maxManifestLayerDepth)
		}
		iterators = append(iterators, &derivedScanIterator{generation: generation, prefix: prefix})
		value, found, err := store.Get(ctx, schema.AnalyticsManifestKey(generation))
		if err != nil {
			return err
		}
		if !found {
			break
		}
		manifest, err := schema.UnmarshalAnalyticsManifestRecord(value)
		if err != nil {
			return err
		}
		generation = manifest.ParentGeneration
	}
	for {
		selected := -1
		var selectedKey []byte
		heads := make([]daemon.KeyValue, len(iterators))
		present := make([]bool, len(iterators))
		for index, iterator := range iterators {
			item, found, err := iterator.head(ctx, store)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			heads[index], present[index] = item, true
			logicalKey := item.Key[13:]
			if selected == -1 || bytes.Compare(logicalKey, selectedKey) < 0 {
				selected, selectedKey = index, logicalKey
			}
		}
		if selected == -1 {
			return nil
		}
		chosen := heads[selected]
		for index := range iterators {
			if present[index] && bytes.Equal(heads[index].Key[13:], selectedKey) {
				iterators[index].index++
			}
		}
		if bytes.Equal(chosen.Value, analyticsDerivedTombstone) {
			continue
		}
		if err := visit(chosen); err != nil {
			return err
		}
	}
}

func executeJobSegment(ctx context.Context, store Store, query Query, pinned pinnedGeneration, segment uint64, result *Result) error {
	dict, err := loadDictionaries(ctx, store)
	if err != nil {
		return err
	}
	result.Explain.SegmentsConsidered++
	metadataValue, found, err := store.Get(ctx, schema.AnalyticsSegmentMetadataKey(segment))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("pinned analytics segment %d metadata is unavailable", segment)
	}
	metadata, err := schema.UnmarshalAnalyticsSegmentMetadataRecord(metadataValue)
	if err != nil {
		return err
	}
	if metadata.ClassificationEpoch != pinned.epoch {
		return fmt.Errorf("pinned analytics segment %d belongs to epoch %d", segment, metadata.ClassificationEpoch)
	}
	if pruneSegment(metadata, query) {
		result.Explain.SegmentsPruned++
		return nil
	}
	segmentValue, found, err := store.Get(ctx, schema.AnalyticsFactSegmentKey(segment))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("pinned analytics segment %d is unavailable", segment)
	}
	rows, err := decodeSegment(segmentValue)
	if err != nil {
		return err
	}
	candidates, used, fallbacks, err := indexedCandidates(ctx, store, segment, rows, query, dict)
	if err != nil {
		return err
	}
	result.Explain.IndexesUsed = append(result.Explain.IndexesUsed, used...)
	result.Explain.IndexFallbacks = append(result.Explain.IndexFallbacks, fallbacks...)
	result.Explain.SegmentsScanned++
	groups := make(map[string]*Group, len(result.Groups))
	for index := range result.Groups {
		key, _ := json.Marshal(result.Groups[index].Dimensions)
		group := result.Groups[index]
		groups[string(key)] = &group
	}
	for row := range rows.Identity {
		if err := ctx.Err(); err != nil {
			return err
		}
		if candidates != nil && !bitSet(candidates, row) {
			continue
		}
		result.Explain.RowsScanned++
		fact := rowFact(rows, row, dict)
		residencyKey := schema.AnalyticsResidencyKey(rows.Identity[row].FSID, rows.Identity[row].Inode, rows.Identity[row].Generation)
		residencyValue, present, err := getActiveDerived(ctx, store, pinned.epoch, residencyKey)
		if err != nil {
			return err
		}
		if present {
			overlay, err := schema.UnmarshalAnalyticsResidencyRecord(residencyValue)
			if err != nil {
				return err
			}
			if overlay.ClassificationEpoch == pinned.epoch && overlay.FactSegment == segment && overlay.Row == uint32(row) {
				fact.Residency = overlay.State
			}
		}
		if !matchesComplete(fact, query) {
			continue
		}
		result.Files++
		if fact.Known&schema.KnownSize != 0 {
			result.LogicalBytes += fact.LogicalSize
		}
		if fact.CreationBasis == schema.AnalyticsTimeUnknown {
			result.UnknownCreationTime++
		}
		dims := dimensions(fact, query.GroupBy)
		if len(dims) != 0 {
			key, _ := json.Marshal(dims)
			group := groups[string(key)]
			if group == nil {
				group = &Group{Dimensions: dims}
				groups[string(key)] = group
			}
			group.Files++
			if fact.Known&schema.KnownSize != 0 {
				group.LogicalBytes += fact.LogicalSize
			}
		}
	}
	result.Groups = result.Groups[:0]
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Groups = append(result.Groups, *groups[key])
	}
	return nil
}

func Cancel(ctx context.Context, store Store, id schema.ID) error {
	record, err := loadJob(ctx, store, id)
	if err != nil {
		return err
	}
	if record.State == schema.AnalyticsQueryComplete || record.State == schema.AnalyticsQueryFailed {
		return fmt.Errorf("analytics job is already terminal")
	}
	record.State = schema.AnalyticsQueryCancelled
	record.UpdatedAt = time.Now().UnixNano()
	return saveJob(ctx, store, id, record)
}
func Wait(ctx context.Context, store Store, id schema.ID) (Result, error) {
	return Resume(ctx, store, id)
}
func loadJob(ctx context.Context, store Store, id schema.ID) (schema.AnalyticsQueryJobRecord, error) {
	value, found, err := store.Get(ctx, schema.AnalyticsQueryJobKey(id))
	if err != nil {
		return schema.AnalyticsQueryJobRecord{}, err
	}
	if !found {
		return schema.AnalyticsQueryJobRecord{}, fmt.Errorf("analytics job not found")
	}
	return schema.UnmarshalAnalyticsQueryJobRecord(value)
}
func saveJob(ctx context.Context, store Store, id schema.ID, record schema.AnalyticsQueryJobRecord) error {
	value, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsQueryJobKey(id), Value: value}}, nil, true)
}

func uint64Bytes(value uint64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, value)
	return result
}
