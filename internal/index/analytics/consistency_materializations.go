package analytics

import (
	"bytes"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func (checker *consistencyChecker) checkMaterializations() error {
	if checker.workspace != nil {
		if err := checker.spoolExpectedMaterializations(); err != nil {
			return err
		}
		if err := checker.workspace.ForEachAggregate(func(key []byte, expected schema.AnalyticsAggregateRecord) error {
			checker.checkAggregate(key, expected)
			return nil
		}); err != nil {
			return err
		}
		return checker.workspace.ForEachSummary(func(key []byte, expected schema.AnalyticsSummaryRecord) error {
			checker.checkSummary(key, expected)
			return nil
		})
	}
	aggregates, summaries, err := checker.expectedMaterializations()
	if err != nil {
		return err
	}
	for key, expected := range aggregates {
		checker.checkAggregate([]byte(key), expected)
	}
	for key, expected := range summaries {
		checker.checkSummary([]byte(key), expected)
	}
	return nil
}

func (checker *consistencyChecker) checkAggregate(key []byte, expected schema.AnalyticsAggregateRecord) {
	value, found := checker.getDerived(key, "readable materialized aggregate")
	if !found {
		checker.add("analytics_materialized_aggregate_mismatch", key, fmt.Sprintf("%+v", expected), "missing")
		return
	}
	actual, err := schema.UnmarshalAnalyticsAggregateRecord(value)
	if err != nil {
		checker.unreadable("analytics_materialized_aggregate_mismatch", key, "decodable materialized aggregate", err)
	} else if actual != expected {
		checker.add("analytics_materialized_aggregate_mismatch", key, fmt.Sprintf("%+v", expected), fmt.Sprintf("%+v", actual))
	}
}

func (checker *consistencyChecker) spoolExpectedMaterializations() error {
	return checker.forEachActiveFact(func(fact schema.AnalyticsFactRecord, _ segmentIdentity, lastComplete int64) error {
		size := uint64(0)
		if fact.Known&schema.KnownSize != 0 {
			size = fact.LogicalSize
		}
		if fact.CreationBasis != schema.AnalyticsTimeUnknown {
			if err := emitConsistencyBuckets(checker.workspace.AddAggregate, fact, time.Unix(0, fact.CreatedAt).UTC(), size, false); err != nil {
				return err
			}
		}
		if lastComplete != 0 && (fact.Residency == schema.AnalyticsArchiveOnly || fact.Residency == schema.AnalyticsExpired) {
			if err := emitConsistencyBuckets(checker.workspace.AddAggregate, fact, time.Unix(0, lastComplete).UTC(), size, true); err != nil {
				return err
			}
		}
		return emitConsistencySummaries(checker.workspace.AddSummary, fact, size)
	})
}

func emitConsistencyBuckets(
	add func([]byte, schema.AnalyticsAggregateRecord) error,
	fact schema.AnalyticsFactRecord,
	instant time.Time,
	size uint64,
	deleted bool,
) error {
	weekday := (int(instant.Weekday()) + 6) % 7
	buckets := []struct {
		granularity schema.AnalyticsGranularity
		timestamp   int64
	}{
		{schema.AnalyticsGranularityYear, time.Date(instant.Year(), 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
		{schema.AnalyticsGranularityMonth, time.Date(instant.Year(), instant.Month(), 1, 0, 0, 0, 0, time.UTC).UnixNano()},
		{schema.AnalyticsGranularityWeek, time.Date(instant.Year(), instant.Month(), instant.Day()-weekday, 0, 0, 0, 0, time.UTC).UnixNano()},
	}
	for _, bucket := range buckets {
		record := schema.AnalyticsAggregateRecord{FilesAdded: 1, BytesAdded: size}
		if deleted {
			record = schema.AnalyticsAggregateRecord{FilesDeleted: 1, BytesDeleted: size}
		}
		if err := add(schema.GrowthTimeKey(bucket.granularity, bucket.timestamp, schema.TierUnknown), record); err != nil {
			return err
		}
		if fact.PathGroup != "unknown" {
			if err := add(schema.GrowthPathKey(fact.PathGroup, bucket.granularity, bucket.timestamp), record); err != nil {
				return err
			}
		}
		if fact.Known&schema.KnownUID != 0 {
			if err := add(schema.UserChurnKey(fact.UID, bucket.granularity, bucket.timestamp), record); err != nil {
				return err
			}
		}
	}
	return nil
}

func emitConsistencySummaries(
	add func([]byte, schema.AnalyticsSummaryRecord) error,
	fact schema.AnalyticsFactRecord,
	size uint64,
) error {
	record := schema.AnalyticsSummaryRecord{ActiveFiles: 1, ActiveBytes: size}
	var keys [][]byte
	if fact.Known&schema.KnownUID != 0 {
		keys = append(keys, schema.UserStatsKey(fact.UID, fact.Residency))
		if fact.Residency == schema.AnalyticsLive {
			keys = append(keys, schema.UserSummaryKey(fact.UID))
		}
	}
	if fact.Known&schema.KnownGID != 0 {
		keys = append(keys, schema.GroupStatsKey(fact.GID, fact.Residency))
		if fact.Residency == schema.AnalyticsLive {
			keys = append(keys, schema.GroupSummaryKey(fact.GID))
		}
	}
	for _, key := range keys {
		if err := add(key, record); err != nil {
			return err
		}
	}
	return nil
}

func (checker *consistencyChecker) checkSummary(key []byte, expected schema.AnalyticsSummaryRecord) {
	value, found := checker.getDerived(key, "readable materialized summary")
	if !found {
		checker.add("analytics_materialized_summary_mismatch", key, fmt.Sprintf("%+v", expected), "missing")
		return
	}
	actual, err := schema.UnmarshalAnalyticsSummaryRecord(value)
	if err != nil {
		checker.unreadable("analytics_materialized_summary_mismatch", key, "decodable materialized summary", err)
	} else if actual != expected {
		checker.add("analytics_materialized_summary_mismatch", key, fmt.Sprintf("%+v", expected), fmt.Sprintf("%+v", actual))
	}
}

func (checker *consistencyChecker) expectedMaterializations() (
	map[string]schema.AnalyticsAggregateRecord,
	map[string]schema.AnalyticsSummaryRecord,
	error,
) {
	aggregates := map[string]schema.AnalyticsAggregateRecord{}
	summaries := map[string]schema.AnalyticsSummaryRecord{}
	err := checker.forEachActiveFact(func(fact schema.AnalyticsFactRecord, _ segmentIdentity, lastComplete int64) error {
		size := uint64(0)
		if fact.Known&schema.KnownSize != 0 {
			size = fact.LogicalSize
		}
		if fact.CreationBasis != schema.AnalyticsTimeUnknown {
			addConsistencyBuckets(aggregates, fact, time.Unix(0, fact.CreatedAt).UTC(), size, false)
		}
		if lastComplete != 0 && (fact.Residency == schema.AnalyticsArchiveOnly || fact.Residency == schema.AnalyticsExpired) {
			addConsistencyBuckets(aggregates, fact, time.Unix(0, lastComplete).UTC(), size, true)
		}
		addConsistencySummaries(summaries, fact, size)
		return nil
	})
	return aggregates, summaries, err
}

func addConsistencyBuckets(records map[string]schema.AnalyticsAggregateRecord, fact schema.AnalyticsFactRecord, instant time.Time, size uint64, deleted bool) {
	weekday := (int(instant.Weekday()) + 6) % 7
	buckets := []struct {
		granularity schema.AnalyticsGranularity
		timestamp   int64
	}{
		{schema.AnalyticsGranularityYear, time.Date(instant.Year(), 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
		{schema.AnalyticsGranularityMonth, time.Date(instant.Year(), instant.Month(), 1, 0, 0, 0, 0, time.UTC).UnixNano()},
		{schema.AnalyticsGranularityWeek, time.Date(instant.Year(), instant.Month(), instant.Day()-weekday, 0, 0, 0, 0, time.UTC).UnixNano()},
	}
	for _, bucket := range buckets {
		addConsistencyAggregate(records, schema.GrowthTimeKey(bucket.granularity, bucket.timestamp, schema.TierUnknown), size, deleted)
		if fact.PathGroup != "unknown" {
			addConsistencyAggregate(records, schema.GrowthPathKey(fact.PathGroup, bucket.granularity, bucket.timestamp), size, deleted)
		}
		if fact.Known&schema.KnownUID != 0 {
			addConsistencyAggregate(records, schema.UserChurnKey(fact.UID, bucket.granularity, bucket.timestamp), size, deleted)
		}
	}
}

func addConsistencyAggregate(records map[string]schema.AnalyticsAggregateRecord, key []byte, size uint64, deleted bool) {
	record := records[string(key)]
	if deleted {
		record.FilesDeleted++
		record.BytesDeleted += size
	} else {
		record.FilesAdded++
		record.BytesAdded += size
	}
	records[string(key)] = record
}

func addConsistencySummaries(records map[string]schema.AnalyticsSummaryRecord, fact schema.AnalyticsFactRecord, size uint64) {
	if fact.Known&schema.KnownUID != 0 {
		addConsistencySummary(records, schema.UserStatsKey(fact.UID, fact.Residency), size)
		if fact.Residency == schema.AnalyticsLive {
			addConsistencySummary(records, schema.UserSummaryKey(fact.UID), size)
		}
	}
	if fact.Known&schema.KnownGID != 0 {
		addConsistencySummary(records, schema.GroupStatsKey(fact.GID, fact.Residency), size)
		if fact.Residency == schema.AnalyticsLive {
			addConsistencySummary(records, schema.GroupSummaryKey(fact.GID), size)
		}
	}
}

func addConsistencySummary(records map[string]schema.AnalyticsSummaryRecord, key []byte, size uint64) {
	record := records[string(key)]
	record.ActiveFiles++
	record.ActiveBytes += size
	records[string(key)] = record
}

func (checker *consistencyChecker) checkGDPR() error {
	if checker.workspace != nil {
		if err := checker.spoolExpectedGDPR(); err != nil {
			return err
		}
		if err := checker.workspace.ForEachGDPR(func(key, want []byte) error {
			checker.checkGDPRValue(key, want)
			return nil
		}); err != nil {
			return err
		}
		return checker.checkGDPRPrefixes()
	}
	expected, err := checker.expectedGDPR()
	if err != nil {
		return err
	}
	for key, want := range expected {
		checker.checkGDPRValue([]byte(key), want)
	}
	return checker.checkGDPRPrefixes()
}

func (checker *consistencyChecker) checkGDPRValue(key, want []byte) {
	actual, found := checker.getDerived(key, "readable GDPR materialization")
	if !found || !bytes.Equal(actual, want) {
		checker.add("analytics_gdpr_view_mismatch", key, fmt.Sprintf("%x", want), fmt.Sprintf("%x", actual))
	}
}

func (checker *consistencyChecker) checkGDPRPrefixes() error {
	for _, prefix := range [][]byte{
		schema.AnalyticsDerivedPrefix(checker.metadata.Generation, []byte("u:inodes:")),
		schema.AnalyticsDerivedPrefix(checker.metadata.Generation, []byte("u:blobs:")),
		schema.AnalyticsDerivedPrefix(checker.metadata.Generation, []byte("u:blobv1:")),
	} {
		if err := checker.checkGDPRPrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

func (checker *consistencyChecker) spoolExpectedGDPR() error {
	return checker.forEachActiveFact(func(fact schema.AnalyticsFactRecord, identity segmentIdentity, _ int64) error {
		if fact.Known&schema.KnownUID == 0 {
			return nil
		}
		key := schema.InodeRevisionKey(identity.FSID, identity.Inode, identity.Revision)
		value, found := checker.get(key, "readable authoritative revision")
		if !found {
			checker.add("analytics_gdpr_source_missing", key, "authoritative revision", "missing")
			return nil
		}
		revision, err := schema.UnmarshalInodeRevision(value)
		if err != nil {
			checker.unreadable("analytics_gdpr_source_malformed", key, "decodable authoritative revision", err)
			return nil
		}
		return checker.addExpectedGDPRWorkspace(fact, identity, revision)
	})
}

func (checker *consistencyChecker) addExpectedGDPRWorkspace(
	fact schema.AnalyticsFactRecord,
	identity segmentIdentity,
	revision schema.InodeRevision,
) error {
	inodeKey := schema.UserInodeKey(fact.UID, identity.FSID, identity.Inode)
	inodeValue, err := (schema.AnalyticsUserInodeRecord{LatestRevision: identity.Revision, PathSample: revision.SourcePath}).MarshalBinary()
	if err != nil {
		checker.unreadable("analytics_gdpr_source_malformed", inodeKey, "encodable GDPR inode materialization", err)
		return nil
	}
	if err := checker.workspace.AddGDPR(inodeKey, inodeValue); err != nil {
		return err
	}
	if fact.CreatedAt == 0 {
		return nil
	}
	return visitInodeContent(checker.ctx, checker.store, revision, func(ordinal uint32, blob schema.ID) error {
		key := schema.UserBlobContributionKey(fact.UID, blob, identity.FSID, identity.Inode, identity.Generation, ordinal)
		value, err := (schema.AnalyticsUserBlobRecord{ReferenceCount: 1, FirstSeen: fact.CreatedAt}).MarshalBinary()
		if err != nil {
			return err
		}
		return checker.workspace.AddGDPR(key, value)
	})
}

func (checker *consistencyChecker) expectedGDPR() (map[string][]byte, error) {
	expected := map[string][]byte{}
	err := checker.forEachActiveFact(func(fact schema.AnalyticsFactRecord, identity segmentIdentity, _ int64) error {
		if fact.Known&schema.KnownUID == 0 {
			return nil
		}
		key := schema.InodeRevisionKey(identity.FSID, identity.Inode, identity.Revision)
		value, found := checker.get(key, "readable authoritative revision")
		if !found {
			checker.add("analytics_gdpr_source_missing", key, "authoritative revision", "missing")
			return nil
		}
		revision, err := schema.UnmarshalInodeRevision(value)
		if err != nil {
			checker.unreadable("analytics_gdpr_source_malformed", key, "decodable authoritative revision", err)
			return nil
		}
		checker.addExpectedGDPR(expected, fact, identity, revision)
		return nil
	})
	return expected, err
}

func (checker *consistencyChecker) addExpectedGDPR(
	expected map[string][]byte,
	fact schema.AnalyticsFactRecord,
	identity segmentIdentity,
	revision schema.InodeRevision,
) {
	inodeKey := schema.UserInodeKey(fact.UID, identity.FSID, identity.Inode)
	inodeValue, err := (schema.AnalyticsUserInodeRecord{LatestRevision: identity.Revision, PathSample: revision.SourcePath}).MarshalBinary()
	if err != nil {
		checker.unreadable("analytics_gdpr_source_malformed", inodeKey, "encodable GDPR inode materialization", err)
		return
	}
	expected[string(inodeKey)] = inodeValue
	if fact.CreatedAt == 0 {
		return
	}
	err = visitInodeContent(checker.ctx, checker.store, revision, func(ordinal uint32, blob schema.ID) error {
		key := schema.UserBlobContributionKey(fact.UID, blob, identity.FSID, identity.Inode, identity.Generation, ordinal)
		value, marshalErr := (schema.AnalyticsUserBlobRecord{ReferenceCount: 1, FirstSeen: fact.CreatedAt}).MarshalBinary()
		if marshalErr == nil {
			expected[string(key)] = value
		}
		return marshalErr
	})
	if err != nil {
		key := schema.InodeRevisionKey(identity.FSID, identity.Inode, identity.Revision)
		checker.unreadable("analytics_gdpr_source_malformed", key, "decodable content references", err)
	}
}

func (checker *consistencyChecker) checkGDPRPrefix(prefix []byte) error {
	return scan(checker.ctx, checker.store, prefix, func(kv daemon.KeyValue) error {
		logicalKey := kv.Key[13:]
		parsed, parseErr := schema.ParseKey(logicalKey)
		var decodeErr error
		if parseErr == nil && parsed.Kind == schema.KeyUserInode {
			_, decodeErr = schema.UnmarshalAnalyticsUserInodeRecord(kv.Value)
		} else if parseErr == nil && (parsed.Kind == schema.KeyUserBlob || parsed.Kind == schema.KeyUserBlobContribution) {
			_, decodeErr = schema.UnmarshalAnalyticsUserBlobRecord(kv.Value)
		}
		if parseErr != nil || decodeErr != nil {
			checker.unreadable("analytics_gdpr_view_malformed", logicalKey, "decodable GDPR materialization",
				firstConsistencyError(parseErr, decodeErr))
		}
		return nil
	})
}
