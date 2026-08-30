package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type ConsistencyFinding struct {
	Kind string
	Key  string
	Want string
	Got  string
}

func CheckConsistency(ctx context.Context, store Store) ([]ConsistencyFinding, error) {
	metadata, err := Status(ctx, store)
	if err != nil || !metadata.Enabled {
		return nil, err
	}
	findings := make([]ConsistencyFinding, 0)
	add := func(kind string, key []byte, want, got string) {
		findings = append(findings, ConsistencyFinding{Kind: kind, Key: string(key), Want: want, Got: got})
	}

	watermarkKey := schema.AnalyticsWatermarkKey(metadata.Generation)
	watermarkValue, found, err := store.Get(ctx, watermarkKey)
	if err != nil {
		return nil, err
	}
	if !found {
		add("analytics_watermark_missing", watermarkKey, "active watermark", "missing")
		return findings, nil
	}
	watermark, err := schema.UnmarshalAnalyticsWatermarkRecord(watermarkValue)
	if err != nil {
		add("analytics_watermark_malformed", watermarkKey, "decodable watermark", err.Error())
		return findings, nil
	}
	manifestKey := schema.AnalyticsManifestKey(metadata.Generation)
	manifestValue, found, err := store.Get(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if !found {
		add("analytics_manifest_missing", manifestKey, "active manifest", "missing")
		return findings, nil
	}
	manifest, err := schema.UnmarshalAnalyticsManifestRecord(manifestValue)
	if err != nil {
		add("analytics_manifest_malformed", manifestKey, "decodable manifest", err.Error())
		return findings, nil
	}
	if watermark.ManifestGeneration != manifest.Generation || manifest.Generation != metadata.Generation || watermark.RepositoryGeneration != metadata.Generation {
		add("analytics_generation_mismatch", manifestKey, fmt.Sprintf("metadata=watermark=manifest=%d", metadata.Generation), fmt.Sprintf("repository=%d watermark=%d manifest=%d", watermark.RepositoryGeneration, watermark.ManifestGeneration, manifest.Generation))
	}
	segments, err := resolveManifestSegments(ctx, store, manifest)
	if err != nil {
		add("analytics_manifest_chain_invalid", manifestKey, "valid bounded acyclic parent chain", err.Error())
		return findings, nil
	}
	for current := manifest; ; {
		layerMarker := schema.AnalyticsDerivedGenerationMarkerKey(current.Generation)
		value, markerFound, getErr := store.Get(ctx, layerMarker)
		if getErr != nil {
			return nil, getErr
		}
		if !markerFound || len(value) != 1 || value[0] != schema.Version {
			add("analytics_completion_marker_invalid", layerMarker, "generation completion marker", "missing or malformed")
		}
		if current.ParentGeneration == 0 {
			break
		}
		parentValue, _, _ := store.Get(ctx, schema.AnalyticsManifestKey(current.ParentGeneration))
		current, _ = schema.UnmarshalAnalyticsManifestRecord(parentValue)
	}
	activeSegments := make(map[uint64]struct{}, len(segments))
	for _, segment := range segments {
		activeSegments[segment] = struct{}{}
	}
	dictionaries := map[schema.AnalyticsDictionaryKind]map[uint32]string{}
	for _, kind := range []schema.AnalyticsDictionaryKind{schema.AnalyticsDictionarySVM, schema.AnalyticsDictionaryVolume, schema.AnalyticsDictionaryPathGroup} {
		dictionaries[kind] = map[uint32]string{}
		values := map[string]uint32{}
		if scanErr := scan(ctx, store, schema.AnalyticsDictionaryPrefix(kind), func(kv daemon.KeyValue) error {
			key, parseErr := schema.ParseKey(kv.Key)
			record, decodeErr := schema.UnmarshalAnalyticsDictionaryRecord(kv.Value)
			if parseErr != nil || decodeErr != nil {
				add("analytics_dictionary_malformed", kv.Key, "decodable dictionary key and value", fmt.Sprint(parseErr, decodeErr))
				return nil
			}
			if previous, duplicate := values[record.Value]; duplicate && previous != key.Ordinal {
				add("analytics_dictionary_duplicate", kv.Key, "one ID per value", fmt.Sprintf("also ID %d", previous))
			}
			values[record.Value], dictionaries[kind][key.Ordinal] = key.Ordinal, record.Value
			return nil
		}); scanErr != nil {
			return nil, scanErr
		}
	}
	var facts uint64
	type activeFact struct {
		fact         schema.AnalyticsFactRecord
		identity     segmentIdentity
		lastComplete int64
	}
	activeFacts := make([]activeFact, 0, metadata.Facts)
	expectedIndexKeys := map[string]struct{}{}
	for _, segment := range segments {
		segmentKey := schema.AnalyticsFactSegmentKey(segment)
		segmentValue, segmentFound, getErr := store.Get(ctx, segmentKey)
		if getErr != nil {
			return nil, getErr
		}
		metadataKey := schema.AnalyticsSegmentMetadataKey(segment)
		metadataValue, metadataFound, getErr := store.Get(ctx, metadataKey)
		if getErr != nil {
			return nil, getErr
		}
		if !segmentFound || !metadataFound {
			add("analytics_segment_pair_missing", segmentKey, "fact segment and metadata", fmt.Sprintf("segment=%t metadata=%t", segmentFound, metadataFound))
			continue
		}
		rows, decodeErr := decodeSegment(segmentValue)
		segmentMetadata, metadataErr := schema.UnmarshalAnalyticsSegmentMetadataRecord(metadataValue)
		if decodeErr != nil || metadataErr != nil {
			add("analytics_segment_malformed", segmentKey, "decodable segment and metadata", fmt.Sprint(decodeErr, metadataErr))
			continue
		}
		if segmentMetadata.RowCount != uint32(len(rows.Identity)) || segmentMetadata.ClassificationEpoch > metadata.Generation {
			add("analytics_segment_metadata_mismatch", metadataKey, fmt.Sprintf("rows=%d epoch<=%d", len(rows.Identity), metadata.Generation), fmt.Sprintf("rows=%d epoch=%d", segmentMetadata.RowCount, segmentMetadata.ClassificationEpoch))
		}
		for row := range rows.Identity {
			for _, reference := range []struct {
				kind schema.AnalyticsDictionaryKind
				id   uint32
			}{{schema.AnalyticsDictionarySVM, rows.SVM[row]}, {schema.AnalyticsDictionaryVolume, rows.Volume[row]}, {schema.AnalyticsDictionaryPathGroup, rows.PathGroup[row]}} {
				if reference.id != 0 && dictionaries[reference.kind][reference.id] == "" {
					add("analytics_dictionary_reference_missing", segmentKey, "referenced dictionary ID", fmt.Sprintf("kind=%d id=%d row=%d", reference.kind, reference.id, row))
				}
			}
			overlayKey := schema.AnalyticsResidencyKey(rows.Identity[row].FSID, rows.Identity[row].Inode, rows.Identity[row].Generation)
			overlayValue, overlayFound, overlayErr := getActiveDerived(ctx, store, metadata.Generation, overlayKey)
			if overlayErr != nil {
				return nil, overlayErr
			}
			if !overlayFound {
				add("analytics_overlay_missing", overlayKey, fmt.Sprintf("segment=%d row=%d", segment, row), "missing")
			} else if overlay, decodeErr := schema.UnmarshalAnalyticsResidencyRecord(overlayValue); decodeErr != nil || overlay.FactSegment != segment || overlay.Row != uint32(row) || overlay.ClassificationEpoch > metadata.Generation {
				add("analytics_overlay_mismatch", overlayKey, fmt.Sprintf("segment=%d row=%d epoch<=%d", segment, row, metadata.Generation), fmt.Sprint(decodeErr))
			} else {
				fact := rowFact(rows, row, dictionaries)
				fact.Residency = overlay.State
				activeFacts = append(activeFacts, activeFact{fact: fact, identity: rows.Identity[row], lastComplete: overlay.LastCompleteCrawl})
			}
		}
		for dimension, values := range indexValues(rows) {
			for value, expectedBitmap := range values {
				indexKey := schema.AnalyticsDimensionIndexKey(dimension, value, segment)
				expectedIndexKeys[string(indexKey)] = struct{}{}
				encoded, indexFound, getErr := store.Get(ctx, indexKey)
				if getErr != nil {
					return nil, getErr
				}
				if !indexFound {
					add("analytics_index_missing", indexKey, "dimension index", "missing")
					continue
				}
				index, decodeErr := schema.UnmarshalAnalyticsDimensionIndexRecord(encoded)
				bitmap := index.Bitmap
				if decodeErr == nil && index.Codec == schema.AnalyticsCodecZstd {
					bitmap, decodeErr = analyticsZstdDecoder.DecodeAll(bitmap, nil)
				} else if decodeErr == nil && index.Codec != schema.AnalyticsCodecRaw {
					decodeErr = fmt.Errorf("unsupported bitmap codec %d", index.Codec)
				}
				var logicalBytes uint64
				for row := range rows.Identity {
					if bitSet(expectedBitmap, row) && rows.Identity[row].Known&schema.KnownSize != 0 {
						logicalBytes += rows.Size[row]
					}
				}
				if decodeErr != nil || index.RowCount != uint32(len(rows.Identity)) || index.MatchCount != countBits(expectedBitmap) || index.LogicalBytes != logicalBytes || !bytes.Equal(bitmap, expectedBitmap) {
					add("analytics_index_mismatch", indexKey, fmt.Sprintf("rows=%d matches=%d bytes=%d bitmap=%x", len(rows.Identity), countBits(expectedBitmap), logicalBytes, expectedBitmap), fmt.Sprintf("rows=%d matches=%d bytes=%d bitmap=%x error=%v", index.RowCount, index.MatchCount, index.LogicalBytes, bitmap, decodeErr))
				}
			}
		}
		facts += uint64(len(rows.Identity))
	}
	if facts != metadata.Facts {
		add("analytics_fact_count_mismatch", schema.AnalyticsMetadataKey(), fmt.Sprint(metadata.Facts), fmt.Sprint(facts))
	}
	expectedAggregates := map[string]schema.AnalyticsAggregateRecord{}
	expectedSummaries := map[string]schema.AnalyticsSummaryRecord{}
	addAggregate := func(key []byte, size uint64, deleted bool) {
		record := expectedAggregates[string(key)]
		if deleted {
			record.FilesDeleted++
			record.BytesDeleted += size
		} else {
			record.FilesAdded++
			record.BytesAdded += size
		}
		expectedAggregates[string(key)] = record
	}
	for _, item := range activeFacts {
		fact, size := item.fact, uint64(0)
		if fact.Known&schema.KnownSize != 0 {
			size = fact.LogicalSize
		}
		addBuckets := func(instant time.Time, deleted bool) {
			year := time.Date(instant.Year(), 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
			month := time.Date(instant.Year(), instant.Month(), 1, 0, 0, 0, 0, time.UTC).UnixNano()
			weekday := (int(instant.Weekday()) + 6) % 7
			week := time.Date(instant.Year(), instant.Month(), instant.Day()-weekday, 0, 0, 0, 0, time.UTC).UnixNano()
			for _, bucket := range []struct {
				granularity schema.AnalyticsGranularity
				timestamp   int64
			}{{schema.AnalyticsGranularityYear, year}, {schema.AnalyticsGranularityMonth, month}, {schema.AnalyticsGranularityWeek, week}} {
				addAggregate(schema.GrowthTimeKey(bucket.granularity, bucket.timestamp, schema.TierUnknown), size, deleted)
				if fact.PathGroup != "unknown" {
					addAggregate(schema.GrowthPathKey(fact.PathGroup, bucket.granularity, bucket.timestamp), size, deleted)
				}
				if fact.Known&schema.KnownUID != 0 {
					addAggregate(schema.UserChurnKey(fact.UID, bucket.granularity, bucket.timestamp), size, deleted)
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
			stats := expectedSummaries[statsKey]
			stats.ActiveFiles++
			stats.ActiveBytes += size
			expectedSummaries[statsKey] = stats
			if fact.Residency == schema.AnalyticsLive {
				key := string(schema.UserSummaryKey(fact.UID))
				summary := expectedSummaries[key]
				summary.ActiveFiles++
				summary.ActiveBytes += size
				expectedSummaries[key] = summary
			}
		}
		if fact.Known&schema.KnownGID != 0 {
			statsKey := string(schema.GroupStatsKey(fact.GID, fact.Residency))
			stats := expectedSummaries[statsKey]
			stats.ActiveFiles++
			stats.ActiveBytes += size
			expectedSummaries[statsKey] = stats
			if fact.Residency == schema.AnalyticsLive {
				key := string(schema.GroupSummaryKey(fact.GID))
				summary := expectedSummaries[key]
				summary.ActiveFiles++
				summary.ActiveBytes += size
				expectedSummaries[key] = summary
			}
		}
	}
	for key, expected := range expectedAggregates {
		value, found, getErr := getActiveDerived(ctx, store, metadata.Generation, []byte(key))
		if getErr != nil {
			return nil, getErr
		}
		actual, decodeErr := schema.UnmarshalAnalyticsAggregateRecord(value)
		if !found || decodeErr != nil || actual != expected {
			add("analytics_materialized_aggregate_mismatch", []byte(key), fmt.Sprintf("%+v", expected), fmt.Sprintf("%+v error=%v", actual, decodeErr))
		}
	}
	for key, expected := range expectedSummaries {
		value, found, getErr := getActiveDerived(ctx, store, metadata.Generation, []byte(key))
		if getErr != nil {
			return nil, getErr
		}
		actual, decodeErr := schema.UnmarshalAnalyticsSummaryRecord(value)
		if !found || decodeErr != nil || actual != expected {
			add("analytics_materialized_summary_mismatch", []byte(key), fmt.Sprintf("%+v", expected), fmt.Sprintf("%+v error=%v", actual, decodeErr))
		}
	}
	expectedGDPR := map[string][]byte{}
	for _, item := range activeFacts {
		if item.fact.Known&schema.KnownUID == 0 {
			continue
		}
		revisionValue, found, getErr := store.Get(ctx, schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision))
		if getErr != nil {
			return nil, getErr
		}
		if !found {
			add("analytics_gdpr_source_missing", schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision), "authoritative revision", "missing")
			continue
		}
		revision, decodeErr := schema.UnmarshalInodeRevision(revisionValue)
		if decodeErr != nil {
			add("analytics_gdpr_source_malformed", schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision), "decodable authoritative revision", decodeErr.Error())
			continue
		}
		inodeKey := schema.UserInodeKey(item.fact.UID, item.identity.FSID, item.identity.Inode)
		inodeExpected, _ := (schema.AnalyticsUserInodeRecord{LatestRevision: item.identity.Revision, PathSample: revision.SourcePath}).MarshalBinary()
		expectedGDPR[string(inodeKey)] = inodeExpected
		if item.fact.CreatedAt == 0 {
			continue
		}
		if visitErr := visitInodeContent(ctx, store, revision, func(ordinal uint32, blob schema.ID) error {
			key := schema.UserBlobContributionKey(item.fact.UID, blob, item.identity.FSID, item.identity.Inode, item.identity.Generation, ordinal)
			value, marshalErr := (schema.AnalyticsUserBlobRecord{ReferenceCount: 1, FirstSeen: item.fact.CreatedAt}).MarshalBinary()
			if marshalErr == nil {
				expectedGDPR[string(key)] = value
			}
			return marshalErr
		}); visitErr != nil {
			add("analytics_gdpr_source_malformed", schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision), "decodable content references", visitErr.Error())
		}
	}
	for key, expected := range expectedGDPR {
		actual, found, getErr := getActiveDerived(ctx, store, metadata.Generation, []byte(key))
		if getErr != nil {
			return nil, getErr
		}
		if !found || !bytes.Equal(actual, expected) {
			add("analytics_gdpr_view_mismatch", []byte(key), fmt.Sprintf("%x", expected), fmt.Sprintf("%x", actual))
		}
	}
	for _, prefix := range [][]byte{schema.AnalyticsDerivedPrefix(metadata.Generation, []byte("u:inodes:")), schema.AnalyticsDerivedPrefix(metadata.Generation, []byte("u:blobs:")), schema.AnalyticsDerivedPrefix(metadata.Generation, []byte("u:blobv1:"))} {
		if scanErr := scan(ctx, store, prefix, func(kv daemon.KeyValue) error {
			logicalKey := kv.Key[13:]
			parsed, parseErr := schema.ParseKey(logicalKey)
			var decodeErr error
			if parseErr == nil && parsed.Kind == schema.KeyUserInode {
				_, decodeErr = schema.UnmarshalAnalyticsUserInodeRecord(kv.Value)
			} else if parseErr == nil && (parsed.Kind == schema.KeyUserBlob || parsed.Kind == schema.KeyUserBlobContribution) {
				_, decodeErr = schema.UnmarshalAnalyticsUserBlobRecord(kv.Value)
			}
			if parseErr != nil || decodeErr != nil {
				add("analytics_gdpr_view_malformed", logicalKey, "decodable GDPR materialization", fmt.Sprint(parseErr, decodeErr))
			}
			return nil
		}); scanErr != nil {
			return nil, scanErr
		}
	}
	if scanErr := scan(ctx, store, []byte("ai:"), func(kv daemon.KeyValue) error {
		key, parseErr := schema.ParseKey(kv.Key)
		if parseErr != nil {
			add("analytics_index_malformed", kv.Key, "valid index key", parseErr.Error())
			return nil
		}
		if _, active := activeSegments[key.Generation]; !active {
			add("analytics_index_inactive_segment", kv.Key, "active segment", fmt.Sprint(key.Generation))
		}
		if _, expected := expectedIndexKeys[string(kv.Key)]; !expected && key.Generation != 0 {
			add("analytics_index_unexpected", kv.Key, "index derived from active rows", "no matching dimension value")
		}
		if _, decodeErr := schema.UnmarshalAnalyticsDimensionIndexRecord(kv.Value); decodeErr != nil {
			add("analytics_index_malformed", kv.Key, "decodable index", decodeErr.Error())
		}
		return nil
	}); scanErr != nil {
		return nil, scanErr
	}
	if scanErr := scan(ctx, store, schema.AnalyticsDerivedPrefix(metadata.Generation, []byte("ar:")), func(kv daemon.KeyValue) error {
		logicalKey := kv.Key[13:]
		key, parseErr := schema.ParseKey(logicalKey)
		overlay, decodeErr := schema.UnmarshalAnalyticsResidencyRecord(kv.Value)
		if parseErr != nil || decodeErr != nil {
			add("analytics_overlay_mismatch", logicalKey, "decodable overlay", fmt.Sprint(parseErr, decodeErr))
			return nil
		}
		if _, active := activeSegments[overlay.FactSegment]; !active {
			add("analytics_overlay_inactive_segment", logicalKey, "active fact segment", fmt.Sprint(overlay.FactSegment))
			return nil
		}
		segmentValue, found, getErr := store.Get(ctx, schema.AnalyticsFactSegmentKey(overlay.FactSegment))
		if getErr != nil {
			return getErr
		}
		rows, rowsErr := decodeSegment(segmentValue)
		if !found || rowsErr != nil || int(overlay.Row) >= len(rows.Identity) || rows.Identity[overlay.Row].FSID != key.FSID || rows.Identity[overlay.Row].Inode != key.Inode || rows.Identity[overlay.Row].Generation != key.Generation {
			add("analytics_overlay_identity_mismatch", logicalKey, "overlay identity matches active row", fmt.Sprint(rowsErr))
		}
		return nil
	}); scanErr != nil {
		return nil, scanErr
	}
	lastCommit, lastOrdinal := uint64(0), uint32(0)
	if scanErr := scan(ctx, store, schema.AnalyticsDeltaPrefix(), func(kv daemon.KeyValue) error {
		key, parseErr := schema.ParseKey(kv.Key)
		_, decodeErr := schema.UnmarshalAnalyticsDeltaRecord(kv.Value)
		if parseErr != nil || decodeErr != nil {
			add("analytics_outbox_malformed", kv.Key, "ordered decodable delta", fmt.Sprint(parseErr, decodeErr))
			return nil
		}
		if key.Commit < lastCommit || key.Commit == lastCommit && key.Ordinal <= lastOrdinal {
			add("analytics_outbox_order", kv.Key, "strict commit/ordinal order", fmt.Sprintf("after %d:%d", lastCommit, lastOrdinal))
		}
		if key.Commit <= watermark.AppliedCommit {
			add("analytics_outbox_watermark_order", kv.Key, fmt.Sprintf("commit>%d", watermark.AppliedCommit), fmt.Sprint(key.Commit))
		}
		lastCommit, lastOrdinal = key.Commit, key.Ordinal
		return nil
	}); scanErr != nil {
		return nil, scanErr
	}
	if scanErr := scan(ctx, store, schema.AnalyticsQueryJobPrefix(), func(kv daemon.KeyValue) error {
		job, decodeErr := schema.UnmarshalAnalyticsQueryJobRecord(kv.Value)
		if decodeErr != nil {
			add("analytics_job_malformed", kv.Key, "decodable pinned job", decodeErr.Error())
			return nil
		}
		if job.ClassificationEpoch > metadata.Generation || job.RepositoryGeneration > watermark.RepositoryGeneration {
			add("analytics_job_future_generation", kv.Key, "published generation", fmt.Sprintf("repository=%d epoch=%d", job.RepositoryGeneration, job.ClassificationEpoch))
		}
		for _, segment := range job.CompletedSegments {
			if _, active := activeSegments[segment]; !active && job.ClassificationEpoch == metadata.Generation {
				add("analytics_job_segment_inactive", kv.Key, "completed active segment", fmt.Sprint(segment))
			}
		}
		return nil
	}); scanErr != nil {
		return nil, scanErr
	}
	if scanErr := scan(ctx, store, []byte("aq:view:"), func(kv daemon.KeyValue) error {
		key, parseErr := schema.ParseKey(kv.Key)
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(kv.Value)
		var view viewRecord
		if decodeErr == nil {
			decodeErr = json.Unmarshal(record.Payload, &view)
		}
		if parseErr != nil || decodeErr != nil || len(view.Predicates) == 0 || len(view.Shape) == 0 || len(view.GroupBy) == 0 {
			add("analytics_view_malformed", kv.Key, "decodable bounded cuboid", fmt.Sprint(parseErr, decodeErr))
			return nil
		}
		if key.Value != view.Result.Generation || view.RepositoryGeneration != view.Result.Watermark.RepositoryGeneration || view.ClassificationEpoch != view.Result.Watermark.ClassificationEpoch || view.AppliedCommit != view.Result.Watermark.AppliedCommit {
			add("analytics_view_scope_mismatch", kv.Key, "key and result scope agree", "scope mismatch")
		}
		if view.ExpiresAt < time.Now().Unix() {
			add("analytics_view_expired", kv.Key, "unexpired adaptive view", fmt.Sprint(view.ExpiresAt))
		}
		return nil
	}); scanErr != nil {
		return nil, scanErr
	}
	if scanErr := scan(ctx, store, []byte("aq:result:"), func(kv daemon.KeyValue) error {
		key, parseErr := schema.ParseKey(kv.Key)
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(kv.Value)
		var cached cacheRecord
		if decodeErr == nil {
			decodeErr = json.Unmarshal(record.Payload, &cached)
		}
		if parseErr != nil || decodeErr != nil {
			add("analytics_cache_malformed", kv.Key, "decodable scoped result", fmt.Sprint(parseErr, decodeErr))
			return nil
		}
		if cached.Result.Watermark.RepositoryGeneration != key.Generation || cached.Result.Watermark.ClassificationEpoch != key.Epoch || cached.Result.Watermark.AppliedCommit != key.Commit {
			add("analytics_cache_scope_mismatch", kv.Key, "cache key matches result watermark", "scope mismatch")
		}
		return nil
	}); scanErr != nil {
		return nil, scanErr
	}
	return findings, nil
}
