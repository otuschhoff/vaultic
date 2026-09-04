package analytics

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func (checker *consistencyChecker) checkIndexCatalog() error {
	return scan(checker.ctx, checker.store, []byte("ai:"), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			checker.unreadable("analytics_index_malformed", kv.Key, "valid index key", err)
			return nil
		}
		if _, active := checker.activeSegments[key.Generation]; !active {
			checker.add("analytics_index_inactive_segment", kv.Key, "active segment", fmt.Sprint(key.Generation))
		}
		if _, expected := checker.expectedIndexKeys[string(kv.Key)]; !expected && key.Generation != 0 {
			checker.add("analytics_index_unexpected", kv.Key, "index derived from active rows", "no matching dimension value")
		}
		if _, err := schema.UnmarshalAnalyticsDimensionIndexRecord(kv.Value); err != nil {
			checker.unreadable("analytics_index_malformed", kv.Key, "decodable index", err)
		}
		return nil
	})
}

func (checker *consistencyChecker) checkOverlayCatalog() error {
	prefix := schema.AnalyticsDerivedPrefix(checker.metadata.Generation, []byte("ar:"))
	return scan(checker.ctx, checker.store, prefix, func(kv daemon.KeyValue) error {
		logicalKey := kv.Key[13:]
		key, parseErr := schema.ParseKey(logicalKey)
		overlay, decodeErr := schema.UnmarshalAnalyticsResidencyRecord(kv.Value)
		if parseErr != nil || decodeErr != nil {
			checker.unreadable("analytics_overlay_mismatch", logicalKey, "decodable overlay",
				firstConsistencyError(parseErr, decodeErr))
			return nil
		}
		if _, active := checker.activeSegments[overlay.FactSegment]; !active {
			checker.add("analytics_overlay_inactive_segment", logicalKey, "active fact segment", fmt.Sprint(overlay.FactSegment))
			return nil
		}
		segmentKey := schema.AnalyticsFactSegmentKey(overlay.FactSegment)
		segmentValue, found := checker.get(segmentKey, "readable overlay fact segment")
		if !found {
			checker.add("analytics_overlay_identity_mismatch", logicalKey, "overlay identity matches active row", "segment missing")
			return nil
		}
		rows, err := decodeSegment(segmentValue)
		if err != nil {
			checker.unreadable("analytics_overlay_identity_mismatch", segmentKey, "decodable overlay fact segment", err)
			return nil
		}
		if int(overlay.Row) >= len(rows.Identity) || rows.Identity[overlay.Row].FSID != key.FSID ||
			rows.Identity[overlay.Row].Inode != key.Inode || rows.Identity[overlay.Row].Generation != key.Generation {
			checker.add("analytics_overlay_identity_mismatch", logicalKey, "overlay identity matches active row", "identity mismatch")
		}
		return nil
	})
}

func (checker *consistencyChecker) checkOutbox() error {
	if checker.watermark == nil {
		return nil
	}
	lastCommit, lastOrdinal := uint64(0), uint32(0)
	return scan(checker.ctx, checker.store, schema.AnalyticsDeltaPrefix(), func(kv daemon.KeyValue) error {
		key, parseErr := schema.ParseKey(kv.Key)
		_, decodeErr := schema.UnmarshalAnalyticsDeltaRecord(kv.Value)
		if parseErr != nil || decodeErr != nil {
			checker.unreadable("analytics_outbox_malformed", kv.Key, "ordered decodable delta",
				firstConsistencyError(parseErr, decodeErr))
			return nil
		}
		if key.Commit < lastCommit || key.Commit == lastCommit && key.Ordinal <= lastOrdinal {
			checker.add("analytics_outbox_order", kv.Key, "strict commit/ordinal order",
				fmt.Sprintf("after %d:%d", lastCommit, lastOrdinal))
		}
		if key.Commit <= checker.watermark.AppliedCommit {
			checker.add("analytics_outbox_watermark_order", kv.Key,
				fmt.Sprintf("commit>%d", checker.watermark.AppliedCommit), fmt.Sprint(key.Commit))
		}
		lastCommit, lastOrdinal = key.Commit, key.Ordinal
		return nil
	})
}

func (checker *consistencyChecker) checkJobs() error {
	return scan(checker.ctx, checker.store, schema.AnalyticsQueryJobPrefix(), func(kv daemon.KeyValue) error {
		job, err := schema.UnmarshalAnalyticsQueryJobRecord(kv.Value)
		if err != nil {
			checker.unreadable("analytics_job_malformed", kv.Key, "decodable pinned job", err)
			return nil
		}
		futureRepository := checker.watermark != nil && job.RepositoryGeneration > checker.watermark.RepositoryGeneration
		if job.ClassificationEpoch > checker.metadata.Generation || futureRepository {
			checker.add("analytics_job_future_generation", kv.Key, "published generation",
				fmt.Sprintf("repository=%d epoch=%d", job.RepositoryGeneration, job.ClassificationEpoch))
		}
		for _, segment := range job.CompletedSegments {
			if _, active := checker.activeSegments[segment]; !active && job.ClassificationEpoch == checker.metadata.Generation {
				checker.add("analytics_job_segment_inactive", kv.Key, "completed active segment", fmt.Sprint(segment))
			}
		}
		return nil
	})
}

func (checker *consistencyChecker) checkViews() error {
	return scan(checker.ctx, checker.store, []byte("aq:view:"), func(kv daemon.KeyValue) error {
		key, parseErr := schema.ParseKey(kv.Key)
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(kv.Value)
		var view viewRecord
		if decodeErr == nil {
			decodeErr = json.Unmarshal(record.Payload, &view)
		}
		if parseErr != nil || decodeErr != nil {
			checker.unreadable("analytics_view_malformed", kv.Key, "decodable bounded cuboid",
				firstConsistencyError(parseErr, decodeErr))
			return nil
		}
		if len(view.Predicates) == 0 || len(view.Shape) == 0 || len(view.GroupBy) == 0 {
			checker.add("analytics_view_malformed", kv.Key, "bounded cuboid dimensions", "empty view dimensions")
			return nil
		}
		if key.Value != view.Result.Generation || view.RepositoryGeneration != view.Result.Watermark.RepositoryGeneration ||
			view.ClassificationEpoch != view.Result.Watermark.ClassificationEpoch ||
			view.AppliedCommit != view.Result.Watermark.AppliedCommit {
			checker.add("analytics_view_scope_mismatch", kv.Key, "key and result scope agree", "scope mismatch")
		}
		if view.ExpiresAt < time.Now().Unix() {
			checker.add("analytics_view_expired", kv.Key, "unexpired adaptive view", fmt.Sprint(view.ExpiresAt))
		}
		return nil
	})
}

func (checker *consistencyChecker) checkCache() error {
	return scan(checker.ctx, checker.store, []byte("aq:result:"), func(kv daemon.KeyValue) error {
		key, parseErr := schema.ParseKey(kv.Key)
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(kv.Value)
		var cached cacheRecord
		if decodeErr == nil {
			decodeErr = json.Unmarshal(record.Payload, &cached)
		}
		if parseErr != nil || decodeErr != nil {
			checker.unreadable("analytics_cache_malformed", kv.Key, "decodable scoped result",
				firstConsistencyError(parseErr, decodeErr))
			return nil
		}
		if cached.Result.Watermark.RepositoryGeneration != key.Generation ||
			cached.Result.Watermark.ClassificationEpoch != key.Epoch ||
			cached.Result.Watermark.AppliedCommit != key.Commit {
			checker.add("analytics_cache_scope_mismatch", kv.Key, "cache key matches result watermark", "scope mismatch")
		}
		return nil
	})
}
