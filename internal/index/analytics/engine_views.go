package analytics

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func writeCandidateViews(ctx context.Context, store Store, generation, parentGeneration uint64, facts []buildFact) error {
	aggregates, summaries := planCandidateViews(facts)
	writer := candidateViewWriter{ctx: ctx, store: store, generation: generation, puts: make([]daemon.Mutation, 0, pageSize)}
	for key, record := range aggregates {
		if err := writer.writeAggregate(parentGeneration, []byte(key), record); err != nil {
			return err
		}
	}
	for key, record := range summaries {
		if err := writer.writeSummary(parentGeneration, []byte(key), record); err != nil {
			return err
		}
	}
	for _, item := range facts {
		if err := writer.writeFactContributions(item); err != nil {
			return err
		}
	}
	return writer.flush()
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func planCandidateViews(facts []buildFact) (map[string]schema.AnalyticsAggregateRecord, map[string]schema.AnalyticsSummaryRecord) {
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
	return aggregates, summaries
}

type candidateViewWriter struct {
	ctx        context.Context
	store      Store
	generation uint64
	puts       []daemon.Mutation
}

func (writer *candidateViewWriter) append(key, value []byte) error {
	writer.puts = append(writer.puts, daemon.Mutation{Key: schema.AnalyticsDerivedKey(writer.generation, key), Value: value})
	if len(writer.puts) < pageSize {
		return nil
	}
	return writer.flush()
}

func (writer *candidateViewWriter) flush() error {
	if err := writeBatches(writer.ctx, writer.store, writer.puts); err != nil {
		return err
	}
	writer.puts = writer.puts[:0]
	return nil
}

func (writer *candidateViewWriter) priorValue(parentGeneration uint64, key []byte) ([]byte, bool, error) {
	current, found, err := writer.store.Get(writer.ctx, schema.AnalyticsDerivedKey(writer.generation, key))
	if err != nil || found || parentGeneration == 0 {
		return current, found, err
	}
	return getActiveDerived(writer.ctx, writer.store, parentGeneration, key)
}

func (writer *candidateViewWriter) writeAggregate(parentGeneration uint64, key []byte, record schema.AnalyticsAggregateRecord) error {
	current, found, err := writer.priorValue(parentGeneration, key)
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
	value, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return writer.append(key, value)
}

func (writer *candidateViewWriter) writeSummary(parentGeneration uint64, key []byte, record schema.AnalyticsSummaryRecord) error {
	current, found, err := writer.priorValue(parentGeneration, key)
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
	value, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return writer.append(key, value)
}

func (writer *candidateViewWriter) writeFactContributions(item buildFact) error {
	if item.fact.Known&schema.KnownUID == 0 {
		return nil
	}
	inodeValue, err := (schema.AnalyticsUserInodeRecord{LatestRevision: item.identity.Revision, PathSample: item.fact.SourcePath}).MarshalBinary()
	if err != nil {
		return err
	}
	if err := writer.append(schema.UserInodeKey(item.fact.UID, item.identity.FSID, item.identity.Inode), inodeValue); err != nil || item.fact.CreatedAt == 0 {
		return err
	}
	revisionValue, found, err := writer.store.Get(writer.ctx, schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision))
	if err != nil || !found {
		return errors.Join(err, fmt.Errorf("analytics fact revision %d:%d:%d is missing", item.identity.FSID, item.identity.Inode, item.identity.Revision))
	}
	revision, err := schema.UnmarshalInodeRevision(revisionValue)
	if err != nil {
		return err
	}
	return visitInodeContent(writer.ctx, writer.store, revision, func(ordinal uint32, blob schema.ID) error {
		blobValue, err := (schema.AnalyticsUserBlobRecord{ReferenceCount: 1, FirstSeen: item.fact.CreatedAt}).MarshalBinary()
		if err != nil {
			return err
		}
		key := schema.UserBlobContributionKey(item.fact.UID, blob, item.identity.FSID, item.identity.Inode, item.identity.Generation, ordinal)
		return writer.append(key, blobValue)
	})
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
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
	record := schema.AnalyticsQueryJobRecord{
		State:                schema.AnalyticsQueryPending,
		CanonicalQuery:       canonical,
		RepositoryGeneration: pinned.watermark.RepositoryGeneration,
		ClassificationEpoch:  pinned.epoch,
		AppliedCommit:        pinned.watermark.AppliedCommit,
		UpdatedAt:            time.Now().UnixNano(),
	}
	value, err := record.MarshalBinary()
	if err != nil {
		return schema.ID{}, err
	}
	if err := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsQueryJobKey(id), Value: value}}, nil, true); err != nil {
		return schema.ID{}, err
	}
	return id, nil
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
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
	result := Result{
		SchemaVersion: 2,
		Generation:    pinned.manifest.Generation,
		Watermark: WatermarkInfo{
			RepositoryGeneration: record.RepositoryGeneration,
			ClassificationEpoch:  record.ClassificationEpoch,
			AppliedCommit:        record.AppliedCommit,
			AppliedAt:            pinned.watermark.AppliedAt,
		},
	}
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
		record.Result, err = json.Marshal(result)
		if err != nil {
			return Result{}, fmt.Errorf("marshal partial analytics result: %w", err)
		}
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
	record.Result, err = json.Marshal(result)
	if err != nil {
		return Result{}, fmt.Errorf("marshal analytics result: %w", err)
	}
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

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
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

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
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
