package analytics

import (
	"bytes"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func (checker *consistencyChecker) checkMaterializations() error {
	aggregates, summaries := checker.expectedMaterializations()
	for key, expected := range aggregates {
		value, found := checker.getDerived([]byte(key), "readable materialized aggregate")
		if !found {
			checker.add("analytics_materialized_aggregate_mismatch", []byte(key), fmt.Sprintf("%+v", expected), "missing")
			continue
		}
		actual, err := schema.UnmarshalAnalyticsAggregateRecord(value)
		if err != nil {
			checker.unreadable("analytics_materialized_aggregate_mismatch", []byte(key), "decodable materialized aggregate", err)
		} else if actual != expected {
			checker.add("analytics_materialized_aggregate_mismatch", []byte(key), fmt.Sprintf("%+v", expected), fmt.Sprintf("%+v", actual))
		}
	}
	for key, expected := range summaries {
		checker.checkSummary([]byte(key), expected)
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

func (checker *consistencyChecker) expectedMaterializations() (map[string]schema.AnalyticsAggregateRecord, map[string]schema.AnalyticsSummaryRecord) {
	aggregates := map[string]schema.AnalyticsAggregateRecord{}
	summaries := map[string]schema.AnalyticsSummaryRecord{}
	for _, item := range checker.activeFacts {
		size := uint64(0)
		if item.fact.Known&schema.KnownSize != 0 {
			size = item.fact.LogicalSize
		}
		if item.fact.CreationBasis != schema.AnalyticsTimeUnknown {
			addConsistencyBuckets(aggregates, item.fact, time.Unix(0, item.fact.CreatedAt).UTC(), size, false)
		}
		if item.lastComplete != 0 &&
			(item.fact.Residency == schema.AnalyticsArchiveOnly || item.fact.Residency == schema.AnalyticsExpired) {
			addConsistencyBuckets(aggregates, item.fact, time.Unix(0, item.lastComplete).UTC(), size, true)
		}
		addConsistencySummaries(summaries, item.fact, size)
	}
	return aggregates, summaries
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
	expected := checker.expectedGDPR()
	for key, want := range expected {
		actual, found := checker.getDerived([]byte(key), "readable GDPR materialization")
		if !found || !bytes.Equal(actual, want) {
			checker.add("analytics_gdpr_view_mismatch", []byte(key), fmt.Sprintf("%x", want), fmt.Sprintf("%x", actual))
		}
	}
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

func (checker *consistencyChecker) expectedGDPR() map[string][]byte {
	expected := map[string][]byte{}
	for _, item := range checker.activeFacts {
		if item.fact.Known&schema.KnownUID == 0 {
			continue
		}
		key := schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision)
		value, found := checker.get(key, "readable authoritative revision")
		if !found {
			checker.add("analytics_gdpr_source_missing", key, "authoritative revision", "missing")
			continue
		}
		revision, err := schema.UnmarshalInodeRevision(value)
		if err != nil {
			checker.unreadable("analytics_gdpr_source_malformed", key, "decodable authoritative revision", err)
			continue
		}
		checker.addExpectedGDPR(expected, item, revision)
	}
	return expected
}

func (checker *consistencyChecker) addExpectedGDPR(expected map[string][]byte, item consistencyActiveFact, revision schema.InodeRevision) {
	inodeKey := schema.UserInodeKey(item.fact.UID, item.identity.FSID, item.identity.Inode)
	inodeValue, err := (schema.AnalyticsUserInodeRecord{LatestRevision: item.identity.Revision, PathSample: revision.SourcePath}).MarshalBinary()
	if err != nil {
		checker.unreadable("analytics_gdpr_source_malformed", inodeKey, "encodable GDPR inode materialization", err)
		return
	}
	expected[string(inodeKey)] = inodeValue
	if item.fact.CreatedAt == 0 {
		return
	}
	err = visitInodeContent(checker.ctx, checker.store, revision, func(ordinal uint32, blob schema.ID) error {
		key := schema.UserBlobContributionKey(item.fact.UID, blob, item.identity.FSID, item.identity.Inode,
			item.identity.Generation, ordinal)
		value, marshalErr := (schema.AnalyticsUserBlobRecord{ReferenceCount: 1, FirstSeen: item.fact.CreatedAt}).MarshalBinary()
		if marshalErr == nil {
			expected[string(key)] = value
		}
		return marshalErr
	})
	if err != nil {
		key := schema.InodeRevisionKey(item.identity.FSID, item.identity.Inode, item.identity.Revision)
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
