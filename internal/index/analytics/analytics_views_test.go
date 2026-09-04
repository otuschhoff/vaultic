package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestSegmentsDictionariesAndMalformedIndexFallback(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	for index := 0; index < 7; index++ {
		putRevision(
			t,
			store,
			1,
			uint64(index+1),
			uint64(index+1),
			uint32(10+index%2),
			20,
			uint64(100+index),
			time.Date(2024, time.January, index+1, 0, 0, 0, 0, time.UTC),
			fmt.Sprintf("/svm-%d/vol/group/file", index%2),
			true,
		)
	}
	if _, err := Enable(ctx, store, Config{SegmentRows: 3, CacheAfter: 1000}, false); err != nil {
		t.Fatal(err)
	}
	if got := prefixCount(store, schema.AnalyticsFactSegmentPrefix()); got != 3 {
		t.Fatalf("fact segments = %d, want 3", got)
	}
	if got := prefixCount(store, schema.AnalyticsDictionaryPrefix(schema.AnalyticsDictionarySVM)); got != 2 {
		t.Fatalf("SVM dictionary entries = %d, want 2", got)
	}
	query := Query{UIDs: []uint32{10}, IncludeIncomplete: true}
	want, err := Execute(ctx, store, query)
	if err != nil || want.Files != 4 || len(want.Explain.IndexesUsed) != 3 {
		t.Fatalf("indexed query failed: %+v, %v", want, err)
	}
	pinned, _, err := pinLatest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	key := schema.AnalyticsDimensionIndexKey(schema.AnalyticsDimensionUID, 10, pinned.manifest.Segments[0])
	store.values[string(key)] = []byte("malformed")
	got, err := Execute(ctx, store, Query{UIDs: []uint32{10}, GroupBy: []string{"uid"}, IncludeIncomplete: true})
	if err != nil || got.Files != want.Files || len(got.Explain.IndexFallbacks) != 1 {
		t.Fatalf("malformed index did not scan-fallback: %+v, %v", got, err)
	}
}

func TestRandomQueriesMatchBruteForce(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	random := rand.New(rand.NewSource(160016))
	type expectedFact struct {
		uid, gid                   uint32
		year, month, isoYear, week int
		size                       uint64
		live                       bool
	}
	var facts []expectedFact
	for index := 0; index < 120; index++ {
		instant := time.Date(
			2018+random.Intn(8),
			time.Month(1+random.Intn(12)),
			1+random.Intn(27),
			0,
			0,
			0,
			0,
			time.UTC,
		)
		uid, gid, size := uint32(random.Intn(5)), uint32(random.Intn(4)), uint64(random.Intn(100000))
		live := random.Intn(2) == 0
		putRevision(
			t,
			store,
			3,
			uint64(index+1),
			uint64(index+1),
			uid,
			gid,
			size,
			instant,
			fmt.Sprintf("/svm-%d/vol-%d/group/file-%d", uid%2, gid%2, index),
			live,
		)
		isoYear, week := instant.ISOWeek()
		facts = append(facts, expectedFact{uid, gid, instant.Year(), int(instant.Month()), isoYear, week, size, live})
	}
	if _, err := Enable(ctx, store, Config{SegmentRows: 17, CacheAfter: 100000, CacheMaxEntries: 2000}, false); err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 500; iteration++ {
		query := Query{IncludeIncomplete: true}
		if random.Intn(2) == 0 {
			query.UIDs = []uint32{uint32(random.Intn(5))}
		}
		if random.Intn(3) == 0 {
			query.GIDs = []uint32{uint32(random.Intn(4))}
		}
		if random.Intn(3) == 0 {
			query.Years = []int{2018 + random.Intn(8)}
		}
		if random.Intn(4) == 0 {
			query.Months = []int{1 + random.Intn(12)}
		}
		if random.Intn(3) == 0 {
			minimum := uint64(random.Intn(80000))
			maximum := minimum + uint64(1+random.Intn(30000))
			query.SizeMin, query.SizeMax = &minimum, &maximum
		}
		if random.Intn(3) == 0 {
			if random.Intn(2) == 0 {
				query.Residencies = []string{"live"}
			} else {
				query.Residencies = []string{"unknown"}
			}
		}
		var wantFiles, wantBytes uint64
		for _, fact := range facts {
			if len(query.UIDs) != 0 && fact.uid != query.UIDs[0] || len(query.GIDs) != 0 && fact.gid != query.GIDs[0] ||
				len(query.Years) != 0 && fact.year != query.Years[0] ||
				len(query.Months) != 0 && fact.month != query.Months[0] {
				continue
			}
			if query.SizeMin != nil && fact.size < *query.SizeMin ||
				query.SizeMax != nil && fact.size >= *query.SizeMax {
				continue
			}
			if len(query.Residencies) != 0 && (query.Residencies[0] == "live") != fact.live {
				continue
			}
			wantFiles++
			wantBytes += fact.size
		}
		got, err := Execute(ctx, store, query)
		if err != nil || got.Files != wantFiles || got.LogicalBytes != wantBytes {
			t.Fatalf(
				"iteration %d query %+v: got files=%d bytes=%d, want files=%d bytes=%d, err=%v",
				iteration,
				query,
				got.Files,
				got.LogicalBytes,
				wantFiles,
				wantBytes,
				err,
			)
		}
	}
}

func TestIdentityContinuityDefaultAndIncludeIncomplete(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 1, 1, 1, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	pinned, _, _ := pinLatest(ctx, store)
	segmentKey := schema.AnalyticsFactSegmentKey(pinned.manifest.Segments[0])
	record, err := schema.UnmarshalAnalyticsFactSegmentRecord(store.values[string(segmentKey)])
	if err != nil {
		t.Fatal(err)
	}
	continuityColumn := &record.Columns[int(schema.AnalyticsColumnIdentityContinuity)-1]
	continuityColumn.Codec = schema.AnalyticsCodecRaw
	continuityColumn.Data, _ = jsonMarshal([]schema.AnalyticsIdentityContinuity{schema.AnalyticsContinuityUnknown})
	store.values[string(segmentKey)], err = record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(ctx, store, Query{})
	if err != nil || result.Files != 0 {
		t.Fatalf("unknown continuity included by default: %+v, %v", result, err)
	}
	result, err = Execute(ctx, store, Query{IncludeIncomplete: true, GroupBy: []string{"identity-continuity"}})
	if err != nil || result.Files != 1 || result.Groups[0].Dimensions["identity-continuity"] != "unknown" {
		t.Fatalf("include-incomplete failed: %+v, %v", result, err)
	}
}

func TestMaterializedViewsAndAsyncLifecycle(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	created := time.Date(2020, 12, 31, 12, 0, 0, 0, time.UTC)
	revision := schema.InodeRevision{
		CTime:        created.UnixNano(),
		Size:         2048,
		UID:          42,
		GID:          7,
		Known:        schema.KnownCTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath,
		SourcePath:   "/svm/vol/group/file",
		Freshness:    schema.FreshnessVerified,
		ContentMode:  schema.ContentInline,
		ContentCount: 1,
		ContentIDs:   []schema.ID{{1}},
	}
	putRecord(t, store, schema.InodeRevisionKey(1, 1, 1), revision)
	putRecord(
		t,
		store,
		schema.CurrentInodeKey(1, 1),
		schema.CurrentPointer{Revision: 1, RecordKey: schema.InodeRevisionKey(1, 1, 1)},
	)
	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	for prefix, name := range map[string]string{"g:time:": "time",
		"g:path:":    "path",
		"u:summary:": "user summary",
		"g:summary:": "group summary",
		"u:churn:":   "user churn",
		"u:inodes:":  "user inode",
		"u:blobv1:":  "user blob"} {
		if prefixCount(store, schema.AnalyticsDerivedPrefix(1, []byte(prefix))) == 0 {
			t.Fatalf("missing %s materialized view", name)
		}
	}
	id, err := Start(ctx, store, Query{UIDs: []uint32{42}, IncludeIncomplete: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Wait(ctx, store, id)
	if err != nil || result.Files != 1 {
		t.Fatalf("async wait failed: %+v, %v", result, err)
	}
	job, err := loadJob(ctx, store, id)
	if err != nil || job.State != schema.AnalyticsQueryComplete || len(job.Result) == 0 {
		t.Fatalf("job was not persisted complete: %+v, %v", job, err)
	}
	cancelID, err := Start(ctx, store, Query{})
	if err != nil {
		t.Fatal(err)
	}
	if err := Cancel(ctx, store, cancelID); err != nil {
		t.Fatal(err)
	}
	if _, err := Resume(ctx, store, cancelID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled job resumed: %v", err)
	}
}

func TestQueryJobResumesFromSegmentCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	for index := 0; index < 5; index++ {
		putRevision(
			t,
			store,
			1,
			uint64(index+1),
			uint64(index+1),
			42,
			7,
			uint64(index+1),
			time.Date(2024, 1, index+1, 0, 0, 0, 0, time.UTC),
			fmt.Sprintf("/svm/vol/group/%d", index),
			true,
		)
	}
	if _, err := Enable(ctx, store, Config{SegmentRows: 2, CacheAfter: 1000}, false); err != nil {
		t.Fatal(err)
	}
	id, err := Start(ctx, store, Query{UIDs: []uint32{42}, IncludeIncomplete: true})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	store.cancelJobAfterSegments, store.cancelJob = 1, cancel
	if _, err := Resume(cancelled, store, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted job returned %v", err)
	}
	checkpoint, err := loadJob(ctx, store, id)
	if err != nil || checkpoint.State != schema.AnalyticsQueryPending || len(checkpoint.CompletedSegments) != 1 ||
		checkpoint.RowsScanned != 2 {
		t.Fatalf("query checkpoint = %#v, %v", checkpoint, err)
	}
	result, err := Wait(ctx, store, id)
	if err != nil || result.Files != 5 || result.LogicalBytes != 15 || result.Explain.SegmentsScanned != 3 {
		t.Fatalf("resumed query = %+v, %v", result, err)
	}
}

func TestQueryJobFailsWhenPinnedGenerationIsUnavailable(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 42, 7, 1, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	id, err := Start(ctx, store, Query{IncludeIncomplete: true})
	if err != nil {
		t.Fatal(err)
	}
	delete(store.values, string(schema.AnalyticsManifestKey(1)))
	if _, err := Resume(ctx, store, id); err == nil ||
		!strings.Contains(err.Error(), "pinned analytics generation 1 is unavailable") {
		t.Fatalf("pinned invalidation returned %v", err)
	}
	job, err := loadJob(ctx, store, id)
	if err != nil || job.State != schema.AnalyticsQueryFailed || job.Error == "" {
		t.Fatalf("invalidated job = %#v, %v", job, err)
	}
}

func TestPendingQueryJobPinsGenerationAcrossCleanup(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 42, 7, 1, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{SegmentRows: 1}, false); err != nil {
		t.Fatal(err)
	}
	id, err := Start(ctx, store, Query{IncludeIncomplete: true})
	if err != nil {
		t.Fatal(err)
	}
	putRevision(t, store, 1, 2, 2, 43, 7, 2, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/group/b", true)
	if _, err := Rebuild(ctx, store, Config{SegmentRows: 1}, false); err != nil {
		t.Fatal(err)
	}
	putRevision(t, store, 1, 3, 3, 44, 7, 3, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), "/svm/vol/group/c", true)
	if _, err := Rebuild(ctx, store, Config{SegmentRows: 1}, false); err != nil {
		t.Fatal(err)
	}
	result, err := Resume(ctx, store, id)
	if err != nil || result.Generation != 1 || result.Files != 1 {
		t.Fatalf("pinned query after cleanup = %+v, %v", result, err)
	}
}

func TestFreshnessCancellationCanonicalizationAndLegacyFallback(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 5, 6, 7, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	query := Query{UIDs: []uint32{5, 4}, GroupBy: []string{"uid", "year"}, IncludeIncomplete: true, AllowStale: true}
	originalUIDs := append([]uint32(nil), query.UIDs...)
	originalGroups := append([]string(nil), query.GroupBy...)
	result, err := Execute(ctx, store, query)
	if err != nil || result.Files != 1 || result.Watermark.AppliedCommit != 1 ||
		result.Watermark.AuthoritativeHeadAvailable {
		t.Fatalf("allow-stale result lacked exact watermark: %+v, %v", result, err)
	}
	if !equalUint32s(query.UIDs, originalUIDs) || !equalStrings(query.GroupBy, originalGroups) {
		t.Fatalf(
			"Execute mutated caller query: before=%v/%v after=%v/%v",
			originalUIDs,
			originalGroups,
			query.UIDs,
			query.GroupBy,
		)
	}
	query.RequireCurrent = true
	if _, err := Execute(ctx, store, query); err == nil {
		t.Fatal("require-current succeeded without an authoritative head API")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Execute(cancelled, store, Query{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled query returned %v", err)
	}

	legacy := newMemoryStore()
	metadata := schema.AnalyticsMetadataRecord{Enabled: true, Generation: 9, Facts: 1, BuiltAt: 1, ConfigJSON: `{}`}
	putRecord(t, legacy, schema.AnalyticsMetadataKey(), metadata)
	fact := schema.AnalyticsFactRecord{
		Revision:      1,
		UID:           5,
		GID:           6,
		Known:         schema.KnownUID | schema.KnownGID | schema.KnownSize,
		LogicalSize:   7,
		Residency:     schema.AnalyticsLive,
		CreationBasis: schema.AnalyticsTimeUnknown,
	}
	putRecord(t, legacy, schema.AnalyticsFactKey(1, 1), fact)
	legacyResult, err := Execute(ctx, legacy, Query{UIDs: []uint32{5}})
	if err != nil || legacyResult.Files != 1 || !legacyResult.Explain.LegacyFallback {
		t.Fatalf("legacy manifest-free fallback failed: %+v, %v", legacyResult, err)
	}
}

func TestAuthoritativeHeadReportsLagAndRequiresCurrent(t *testing.T) {
	ctx := context.Background()
	base := newMemoryStore()
	putRevision(t, base, 1, 1, 1, 5, 6, 7, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, base, Config{CacheAfter: 1000}, false); err != nil {
		t.Fatal(err)
	}
	store := &memoryStoreWithHead{memoryStore: base, head: 2}
	result, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
	if err != nil || !result.Watermark.AuthoritativeHeadAvailable || result.Watermark.AuthoritativeHead != 2 ||
		result.Watermark.LagCommits != 1 {
		t.Fatalf("lag was not reported: %+v, %v", result.Watermark, err)
	}
	if _, err := Execute(ctx, store, Query{IncludeIncomplete: true, RequireCurrent: true}); err == nil {
		t.Fatal("require-current accepted a lagging watermark")
	}
	store.head = 1
	if _, err := Execute(ctx, store, Query{IncludeIncomplete: true, RequireCurrent: true}); err != nil {
		t.Fatalf("require-current rejected a current watermark: %v", err)
	}
	store.head = 3
	if _, err := Execute(ctx, store, Query{IncludeIncomplete: true}); err == nil {
		t.Fatal("stale analytics result was accepted without AllowStale")
	}
	stale, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
	if err != nil || stale.Watermark.LagCommits == 0 {
		t.Fatalf("explicit stale analytics result failed: %+v, %v", stale.Watermark, err)
	}
	if _, err := Rebuild(ctx, store, Config{CacheAfter: 1000}, false); err != nil {
		t.Fatal(err)
	}
	result, err = Execute(ctx, store, Query{IncludeIncomplete: true, RequireCurrent: true})
	if err != nil || result.Watermark.AppliedCommit != 3 || result.Watermark.LagCommits != 0 {
		t.Fatalf("complete rebuild did not cover authoritative head: %+v, %v", result.Watermark, err)
	}
}

func equalUint32s(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestGrowthAndUserStatsReports(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 600, 700, 100, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/team/a", true)
	putRevision(t, store, 1, 2, 2, 601, 700, 300, time.Date(2024, 2, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/team/b", true)
	if _, err := Enable(ctx, store, Config{CacheAfter: 1000}, false); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	until := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	growth, err := Growth(
		ctx,
		store,
		GrowthOptions{Granularity: "month", Since: &since, Until: &until, SVMs: []string{"svm"}},
	)
	if err != nil || len(growth.Buckets) != 2 || growth.Buckets[0].Files != 1 || growth.Buckets[1].LogicalBytes != 300 {
		t.Fatalf("unexpected growth report: %+v, %v", growth, err)
	}
	if growth.Explain.Source != "raw-segments" {
		t.Fatalf("unsupported growth filters did not fall back to raw segments: %+v", growth.Explain)
	}
	stats, err := UserStats(ctx, store, UserStatsOptions{GroupBy: "user", Limit: 1})
	if err != nil || len(stats.Rows) != 1 || stats.Rows[0].ID != 601 || stats.Rows[0].LogicalBytes != 300 ||
		stats.Rows[0].Residency != "live" {
		t.Fatalf("unexpected user stats: %+v, %v", stats, err)
	}
}

func TestLegacyUnscopedViewsRemainReadable(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRecord(
		t,
		store,
		schema.AnalyticsMetadataKey(),
		schema.AnalyticsMetadataRecord{Enabled: true, Generation: 1, BuiltAt: 1, ConfigJSON: "{}"},
	)
	putRecord(t, store, schema.AnalyticsManifestKey(1), schema.AnalyticsManifestRecord{Generation: 1})
	putRecord(
		t,
		store,
		schema.AnalyticsWatermarkKey(1),
		schema.AnalyticsWatermarkRecord{RepositoryGeneration: 1, ManifestGeneration: 1, AppliedAt: 1},
	)
	month := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	putRecord(
		t,
		store,
		schema.GrowthTimeKey(schema.AnalyticsGranularityMonth, month, schema.TierUnknown),
		schema.AnalyticsAggregateRecord{FilesAdded: 2, BytesAdded: 400},
	)
	growth, err := Growth(ctx, store, GrowthOptions{Granularity: "month"})
	if err != nil || growth.Explain.Source != "legacy-unscoped-view" || len(growth.Buckets) != 1 ||
		growth.Buckets[0].Files != 2 ||
		growth.Buckets[0].LogicalBytes != 400 {
		t.Fatalf("legacy growth view = %+v, %v", growth, err)
	}
}

func TestMaterializedViewsMatchRawAndHaveScopedPhysicalKeys(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	created := time.Date(2024, 2, 2, 0, 0, 0, 0, time.UTC)
	for index, size := range []uint64{100, 300} {
		record := schema.InodeRevision{
			CTime:        created.AddDate(0, index, 0).UnixNano(),
			Size:         size,
			UID:          uint32(600 + index),
			GID:          700,
			Known:        schema.KnownCTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath,
			SourcePath:   fmt.Sprintf("/svm/vol/team/%d", index),
			Freshness:    schema.FreshnessVerified,
			ContentMode:  schema.ContentInline,
			ContentCount: 1,
			ContentIDs:   []schema.ID{{1}},
		}
		putRecord(t, store, schema.InodeRevisionKey(1, uint64(index+1), uint64(index+1)), record)
		putRecord(
			t,
			store,
			schema.CurrentInodeKey(1, uint64(index+1)),
			schema.CurrentPointer{
				Revision:  uint64(index + 1),
				RecordKey: schema.InodeRevisionKey(1, uint64(index+1), uint64(index+1)),
			},
		)
	}
	if _, err := Enable(ctx, store, Config{CacheAfter: 1000}, false); err != nil {
		t.Fatal(err)
	}
	growth, err := Growth(ctx, store, GrowthOptions{Granularity: "month"})
	if err != nil || growth.Explain.Source != "materialized-view" {
		t.Fatalf("growth did not use materialized view: %+v, %v", growth, err)
	}
	rawGrowth, err := Execute(ctx, store, Query{GroupBy: []string{"year", "month"}, IncludeIncomplete: true})
	if err != nil || len(growth.Buckets) != len(rawGrowth.Groups) {
		t.Fatalf("growth/raw shape mismatch: %+v %+v, %v", growth, rawGrowth, err)
	}
	for index := range growth.Buckets {
		if growth.Buckets[index].Files != rawGrowth.Groups[index].Files ||
			growth.Buckets[index].LogicalBytes != rawGrowth.Groups[index].LogicalBytes {
			t.Fatalf("growth/raw mismatch at %d: %+v %+v", index, growth.Buckets[index], rawGrowth.Groups[index])
		}
	}
	stats, err := UserStats(ctx, store, UserStatsOptions{GroupBy: "user"})
	if err != nil || stats.Explain.Source != "materialized-view" {
		t.Fatalf("user stats did not use materialized view: %+v, %v", stats, err)
	}
	rawStats, err := Execute(
		ctx,
		store,
		Query{Residencies: []string{"live"}, GroupBy: []string{"uid", "residency"}, IncludeIncomplete: true},
	)
	if err != nil || len(stats.Rows) != len(rawStats.Groups) {
		t.Fatalf("user stats/raw shape mismatch: %+v %+v, %v", stats, rawStats, err)
	}
	rawRows := map[string]Group{}
	for _, group := range rawStats.Groups {
		rawRows[group.Dimensions["uid"]+":"+group.Dimensions["residency"]] = group
	}
	for _, row := range stats.Rows {
		group, found := rawRows[strconv.FormatUint(uint64(row.ID), 10)+":"+row.Residency]
		if !found || row.Files != group.Files || row.LogicalBytes != group.LogicalBytes {
			t.Fatalf("user stats/raw mismatch: %+v %+v", row, group)
		}
	}
	if store.publicationPuts != 3 {
		t.Fatalf("atomic publication contains %d puts, want only manifest/watermark/metadata", store.publicationPuts)
	}
	for _, key := range [][]byte{
		schema.AnalyticsDerivedGenerationMarkerKey(1),
		schema.AnalyticsDerivedKey(1,
			schema.GrowthTimeKey(schema.AnalyticsGranularityMonth,
				time.Date(2024,
					2,
					1,
					0,
					0,
					0,
					0,
					time.UTC).UnixNano(),
				schema.TierUnknown)),

		schema.AnalyticsDerivedKey(1, schema.UserSummaryKey(600)),
		schema.AnalyticsDerivedKey(1, schema.UserStatsKey(600, schema.AnalyticsLive)),
		schema.AnalyticsDerivedKey(1, schema.UserInodeKey(600, 1, 1)),
		schema.AnalyticsDerivedKey(1, schema.UserBlobContributionKey(600, schema.ID{1}, 1, 1, 1, 0)),
		schema.AnalyticsDerivedKey(1, schema.AnalyticsResidencyKey(1, 1, 1)),
	} {
		if _, found, err := store.Get(ctx, key); err != nil || !found {
			t.Fatalf("scoped physical key %x: found=%t err=%v", key, found, err)
		}
	}
	audit, err := GDPRAudit(ctx, store, 600)
	if err != nil || audit.Explain.Source != "materialized-view" || len(audit.Inodes) != 1 || len(audit.Blobs) != 1 {
		t.Fatalf("GDPR did not use scoped views: %+v, %v", audit, err)
	}
	explained, err := GDPRAuditWithOptions(
		ctx,
		store,
		600,
		GDPRAuditOptions{ExplainSurvivingChunks: true, ExternalSourceLimit: 1},
	)
	if err != nil || len(explained.Blobs) != 1 || explained.Blobs[0].SurvivingExplanation == nil {
		t.Fatalf("GDPR survival explanation missing: %+v, %v", explained, err)
	}
	survival := explained.Blobs[0].SurvivingExplanation
	if survival.ScopedReferences != 1 || survival.ExternalReferences != 1 || !survival.WouldSurvive ||
		survival.SourcesTruncated ||
		len(survival.ExternalSources) != 1 ||
		survival.ExternalSources[0].UID != 601 ||
		survival.ExternalSources[0].Path != "/svm/vol/team/1" {
		t.Fatalf("unexpected GDPR survival explanation: %+v", survival)
	}
}

func TestGDPRAuditReportsMappingsLocationsAndRetention(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	uid := uint32(600)
	blobID := schema.ID{1}
	packID := schema.ID{2}
	putRecord(
		t,
		store,
		schema.UserInodeKey(uid, 3, 4),
		schema.AnalyticsUserInodeRecord{LatestRevision: 5, PathSample: "/home/alice/file"},
	)
	putRecord(
		t,
		store,
		schema.UserBlobKey(uid, blobID),
		schema.AnalyticsUserBlobRecord{ReferenceCount: 2, FirstSeen: 3},
	)
	putRecord(
		t,
		store,
		schema.InodeRevisionKey(3, 4, 5),
		schema.InodeRevision{
			UID:          uid,
			Known:        schema.KnownUID | schema.KnownPath,
			ContentMode:  schema.ContentInline,
			ContentCount: 1,
			ContentIDs:   []schema.ID{blobID},
			SourcePath:   "/home/alice/file",
			Freshness:    schema.FreshnessVerified,
		},
	)
	putRecord(
		t,
		store,
		schema.InodeRevisionKey(3, 5, 6),
		schema.InodeRevision{
			UID:          601,
			Known:        schema.KnownUID | schema.KnownPath,
			ContentMode:  schema.ContentInline,
			ContentCount: 1,
			ContentIDs:   []schema.ID{blobID},
			SourcePath:   "/home/bob/shared",
			Freshness:    schema.FreshnessVerified,
		},
	)
	putRecord(
		t,
		store,
		schema.BlobKey(blobID),
		schema.BlobRecord{
			Locations: []schema.BlobLocation{{PackID: packID, Length: 10, UncompressedSize: 10, Type: schema.BlobData}},
		},
	)
	putRecord(
		t,
		store,
		schema.PackKey(packID),
		schema.PackRecord{
			Type:              schema.PackData,
			PhysicalSize:      10,
			PayloadSize:       10,
			BlobCount:         1,
			PhysicalSizeKnown: true,
			CreationTime:      1,
			CreationTimeKnown: true,
			Lifecycle:         schema.PackImported,
			Tier:              schema.TierCold,
			RetentionSource:   schema.RetentionConfig,
			MinRetentionUntil: 99,
		},
	)
	putRecord(
		t,
		store,
		schema.PackPlacementKey(packID, 42),
		schema.PlacementRecord{
			State:             schema.PlacementLive,
			Bytes:             10,
			RetentionSource:   schema.RetentionBackend,
			MinRetentionUntil: 100,
		},
	)
	audit, err := GDPRAudit(ctx, store, uid)
	if err != nil || len(audit.Inodes) != 1 || audit.Inodes[0].Residency != "unknown" || len(audit.Blobs) != 1 ||
		len(audit.Blobs[0].Packs) != 1 {
		t.Fatalf("unexpected GDPR audit: %+v, %v", audit, err)
	}
	pack := audit.Blobs[0].Packs[0]
	if pack.Tier != "cold" || !pack.RetentionAvailable || pack.RetentionUntil != 100 || len(pack.Backends) != 1 ||
		pack.Backends[0] != 42 ||
		len(pack.Placements) != 1 ||
		pack.Placements[0].State != "live" {
		t.Fatalf("unexpected GDPR pack report: %+v", pack)
	}
	explained, err := GDPRAuditWithOptions(ctx, store, uid, GDPRAuditOptions{ExplainSurvivingChunks: true})
	if err != nil || explained.Blobs[0].SurvivingExplanation == nil ||
		explained.Blobs[0].SurvivingExplanation.ScopedReferences != 1 ||
		explained.Blobs[0].SurvivingExplanation.ExternalReferences != 1 ||
		explained.Blobs[0].SurvivingExplanation.ExternalSources[0].Path != "/home/bob/shared" {
		t.Fatalf("unexpected authoritative survival explanation: %+v, %v", explained, err)
	}
}

func TestQueryJobStatusAndCachePurge(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 1, 2, 10, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/team/a", true)
	if _, err := Enable(ctx, store, Config{CacheAfter: 1}, false); err != nil {
		t.Fatal(err)
	}
	id, err := Start(ctx, store, Query{IncludeIncomplete: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := QueryJobStatus(ctx, store, id)
	if err != nil || status.State != "pending" {
		t.Fatalf("unexpected pending status: %+v, %v", status, err)
	}
	if _, err := Resume(ctx, store, id); err != nil {
		t.Fatal(err)
	}
	status, err = QueryJobStatus(ctx, store, id)
	if err != nil || status.State != "complete" || status.RowsScanned == 0 {
		t.Fatalf("unexpected complete status: %+v, %v", status, err)
	}
	viewID := schema.ID{9}
	putRecord(
		t,
		store,
		schema.AnalyticsQueryViewKey(viewID, 1),
		schema.AnalyticsQueryRecord{Payload: []byte(`{"files":1}`)},
	)
	cache, err := InspectCache(ctx, store)
	if err != nil || cache.Results == 0 || cache.Heat == 0 || cache.Views != 1 || cache.Jobs != 1 {
		t.Fatalf("unexpected cache status: %+v, %v", cache, err)
	}
	status, err = QueryJobStatus(ctx, store, id)
	operational, statusErr := InspectStatus(ctx, store)
	if err != nil || statusErr != nil || operational.SchemaVersion != 1 || operational.Lifecycle.Facts != 1 ||
		operational.Cache.Jobs != 1 ||
		status.State != "complete" {
		t.Fatalf("unexpected operational status: %+v, %v, %v", operational, err, statusErr)
	}
	if result, err := PurgeCache(ctx, store, true, true, false); err != nil || result.Removed < 4 {
		t.Fatalf("unexpected cache purge: %+v, %v", result, err)
	}
	cache, err = InspectCache(ctx, store)
	if err != nil || cache != (CacheStatus{}) {
		t.Fatalf("cache survived purge: %+v, %v", cache, err)
	}
}

func TestAdaptiveViewsPromoteReuseFallbackAndEvict(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 2, 10, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/a/one", true)
	putRevision(t, store, 1, 2, 1, 7, 3, 20, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/a/two", true)
	putRevision(t, store, 1, 3, 1, 8, 3, 30, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), "/svm/vol/b/three", true)
	if _, err := Enable(ctx, store, Config{CacheAfter: 2, CacheMaxEntries: 3, CacheTTLSeconds: 60}, false); err != nil {
		t.Fatal(err)
	}
	broad := Query{GroupBy: []string{"uid", "gid"}, IncludeIncomplete: true}
	oracle, err := Execute(ctx, store, broad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(ctx, store, broad); err != nil {
		t.Fatal(err)
	}
	cache, err := InspectCache(ctx, store)
	if err != nil || cache.Views != 1 {
		t.Fatalf("adaptive view was not promoted: %+v, %v", cache, err)
	}
	reused, err := Execute(ctx, store, Query{GroupBy: []string{"uid"}, IncludeIncomplete: true})
	if err != nil || reused.Explain.Source != "adaptive-view" || reused.Files != oracle.Files ||
		reused.LogicalBytes != oracle.LogicalBytes ||
		len(reused.Groups) != 2 {
		t.Fatalf("compatible view reuse was not exact: oracle=%+v reused=%+v err=%v", oracle, reused, err)
	}
	incompatible, err := Execute(
		ctx,
		store,
		Query{UIDs: []uint32{7}, GroupBy: []string{"uid"}, IncludeIncomplete: true},
	)
	if err != nil || incompatible.Explain.Source != "raw-segments" ||
		!slices.Contains(incompatible.Explain.ViewFallbacks, "incompatible") ||
		incompatible.Files != 2 {
		t.Fatalf("incompatible query did not fall back exactly: %+v, %v", incompatible, err)
	}
	malformedID := schema.ID{0xff}
	store.values[string(schema.AnalyticsQueryViewKey(malformedID, 1))] = []byte("broken")
	malformed, err := Execute(ctx, store, Query{GIDs: []uint32{2}, IncludeIncomplete: true})
	if err != nil || !slices.Contains(malformed.Explain.ViewFallbacks, "malformed") || malformed.Files != 1 {
		t.Fatalf("malformed view fallback failed: %+v, %v", malformed, err)
	}

	for key, value := range store.values {
		if !strings.HasPrefix(key, "aq:view:") || key == string(schema.AnalyticsQueryViewKey(malformedID, 1)) {
			continue
		}
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(value)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		var view viewRecord
		if err := json.Unmarshal(record.Payload, &view); err != nil {
			t.Fatal(err)
		}
		view.RepositoryGeneration++
		payload, _ := json.Marshal(view)
		putRecord(t, store, []byte(key), schema.AnalyticsQueryRecord{Payload: payload})
		break
	}
	stale, err := Execute(ctx, store, Query{Months: []int{1}, IncludeIncomplete: true})
	if err != nil || !slices.Contains(stale.Explain.ViewFallbacks, "stale-generation") || stale.Files != 3 {
		t.Fatalf("stale view fallback failed: %+v, %v", stale, err)
	}
	if err := cleanupCache(ctx, store, 1); err != nil {
		t.Fatal(err)
	}
	cache, err = InspectCache(ctx, store)
	if err != nil || cache.Views > 1 {
		t.Fatalf("adaptive view eviction is unbounded: %+v, %v", cache, err)
	}
}

func TestCheckConsistencyCorruptionFamilies(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 2, 10, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/team/one", true)
	if _, err := Enable(ctx, store, Config{CacheAfter: 1}, false); err != nil {
		t.Fatal(err)
	}
	assertKind := func(want string) {
		t.Helper()
		findings, err := CheckConsistency(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		for _, finding := range findings {
			if finding.Kind == want {
				return
			}
		}
		t.Fatalf("missing %q in findings: %+v", want, findings)
	}
	if findings, err := CheckConsistency(ctx, store); err != nil || len(findings) != 0 {
		t.Fatalf("clean analytics failed consistency check: %+v, %v", findings, err)
	}

	metadata, _ := Status(ctx, store)
	markerKey := schema.AnalyticsDerivedGenerationMarkerKey(metadata.Generation)
	marker := append([]byte(nil), store.values[string(markerKey)]...)
	delete(store.values, string(markerKey))
	assertKind("analytics_completion_marker_invalid")
	store.values[string(markerKey)] = marker

	manifestKey := schema.AnalyticsManifestKey(metadata.Generation)
	manifestValue := append([]byte(nil), store.values[string(manifestKey)]...)
	store.values[string(manifestKey)] = []byte("broken")
	assertKind("analytics_manifest_malformed")
	store.values[string(manifestKey)] = manifestValue

	manifest, _ := schema.UnmarshalAnalyticsManifestRecord(manifestValue)
	segment := manifest.Segments[0]
	segmentMetadataKey := schema.AnalyticsSegmentMetadataKey(segment)
	segmentMetadata := append([]byte(nil), store.values[string(segmentMetadataKey)]...)
	delete(store.values, string(segmentMetadataKey))
	assertKind("analytics_segment_pair_missing")
	store.values[string(segmentMetadataKey)] = segmentMetadata

	var dictionaryKey, indexKey, overlayKey []byte
	for key := range store.values {
		switch {
		case dictionaryKey == nil && strings.HasPrefix(key, "ad:"):
			dictionaryKey = []byte(key)
		case indexKey == nil && strings.HasPrefix(key, "ai:"):
			indexKey = []byte(key)
		case overlayKey == nil && strings.HasPrefix(key, "av1:") && strings.Contains(key, "ar:"):
			overlayKey = []byte(key)
		}
	}
	dictionaryValue := append([]byte(nil), store.values[string(dictionaryKey)]...)
	store.values[string(dictionaryKey)] = []byte("broken")
	assertKind("analytics_dictionary_malformed")
	store.values[string(dictionaryKey)] = dictionaryValue

	indexValue := append([]byte(nil), store.values[string(indexKey)]...)
	store.values[string(indexKey)] = []byte("broken")
	assertKind("analytics_index_mismatch")
	store.values[string(indexKey)] = indexValue

	overlayValue := append([]byte(nil), store.values[string(overlayKey)]...)
	store.values[string(overlayKey)] = []byte("broken")
	assertKind("analytics_overlay_mismatch")
	store.values[string(overlayKey)] = overlayValue

	var aggregateKey, summaryKey, gdprKey []byte
	for key := range store.values {
		if !strings.HasPrefix(key, "av1:") {
			continue
		}
		switch {
		case aggregateKey == nil && (strings.Contains(key, "g:time:") || strings.Contains(key, "g:path:")):
			aggregateKey = []byte(key)
		case summaryKey == nil && strings.Contains(key, "u:summary:"):
			summaryKey = []byte(key)
		case gdprKey == nil && strings.Contains(key, "u:inodes:"):
			gdprKey = []byte(key)
		}
	}
	aggregateValue := append([]byte(nil), store.values[string(aggregateKey)]...)
	store.values[string(aggregateKey)] = []byte("broken")
	assertKind("analytics_materialized_aggregate_mismatch")
	store.values[string(aggregateKey)] = aggregateValue

	summaryValue := append([]byte(nil), store.values[string(summaryKey)]...)
	store.values[string(summaryKey)] = []byte("broken")
	assertKind("analytics_materialized_summary_mismatch")
	store.values[string(summaryKey)] = summaryValue

	gdprValue := append([]byte(nil), store.values[string(gdprKey)]...)
	store.values[string(gdprKey)] = []byte("broken")
	assertKind("analytics_gdpr_view_malformed")
	store.values[string(gdprKey)] = gdprValue

	outboxKey := schema.AnalyticsDeltaKey(metadata.Generation, 1)
	store.values[string(outboxKey)] = []byte("broken")
	assertKind("analytics_outbox_malformed")
	delete(store.values, string(outboxKey))

	jobKey := schema.AnalyticsQueryJobKey(schema.ID{1})
	store.values[string(jobKey)] = []byte("broken")
	assertKind("analytics_job_malformed")
	delete(store.values, string(jobKey))

	viewKey := schema.AnalyticsQueryViewKey(schema.ID{1}, metadata.Generation)
	store.values[string(viewKey)] = []byte("broken")
	assertKind("analytics_view_malformed")
}

func TestAnalyticsStatusJSONShape(t *testing.T) {
	encoded, err := json.Marshal(
		OperationalStatus{SchemaVersion: 1, Cache: CacheStatus{Results: 2, Heat: 3, Views: 4, Jobs: 5}},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := ("{\"schema_version\":1,\"lifecycle\":{\"enabled\":false,\"generation\":0,\"facts\":0,\"c" +
		"ache_entries\":0,\"built_at\":0},\"catch_up\":{\"processed\":0,\"applied_commit\":0,\"" +
		"current\":false},\"cache\":{\"results\":2,\"heat\":3,\"views\":4,\"jobs\":5}}")
	if string(encoded) != want {
		t.Fatalf("status JSON = %s, want %s", encoded, want)
	}
}

func putRevision(
	t *testing.T,
	store *memoryStore,
	fsid uint32,
	inode, revision uint64,
	uid, gid uint32,
	size uint64,
	created time.Time,
	source string,
	live bool,
) {
	t.Helper()
	record := schema.InodeRevision{
		CTime:      created.UnixNano(),
		Size:       size,
		UID:        uid,
		GID:        gid,
		Known:      schema.KnownCTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath,
		SourcePath: source,
		Freshness:  schema.FreshnessVerified,
	}
	putRecord(t, store, schema.InodeRevisionKey(fsid, inode, revision), record)
	if live {
		putRecord(
			t,
			store,
			schema.CurrentInodeKey(fsid, inode),
			schema.CurrentPointer{Revision: revision, RecordKey: schema.InodeRevisionKey(fsid, inode, revision)},
		)
	}
}

func prefixCount(store *memoryStore, prefix []byte) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	count := 0
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), prefix) {
			count++
		}
	}
	return count
}

func jsonMarshal(value any) ([]byte, error) {
	return json.Marshal(value)
}
