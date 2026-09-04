package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func seriesFor(t *testing.T, store *memoryStore, options SeriesOptions) SeriesResult {
	t.Helper()
	result, err := HistorySeries(context.Background(), store, options)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// writeBucket stores one rollup bucket directly, which lets a series test pin
// coverage and values without replaying an event stream.
func writeBucket(
	t *testing.T,
	store *memoryStore,
	granularity schema.HistoryGranularity,
	start uint64,
	packType schema.PackType,
	bucket schema.PackHistoryBucket,
) {
	t.Helper()
	store.set(t, schema.PackHistoryBucketKey(granularity, start, 0, packType), bucket)
}

// TestHistoryGoldenOutput pins the `index history --json` contract.
func TestHistoryGoldenOutput(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	base := uint64(1_699_920_000) // 2023-11-14T00:00:00Z, a day boundary
	for index := range uint64(4) {
		writeBucket(t, store, schema.GranularityDaily, base+index*86400, schema.PackData, schema.PackHistoryBucket{
			PacksCreated: 2 + index, BytesAdded: 1000 * (index + 1), BytesDeleted: 100 * index,
			BytesRepacked: 50, PacksRepacked: 1,
			Coverage: schema.CoverageComplete, EventsObserved: 3 + index,
		})
	}
	goldenJSON(t, "history_bytes_daily", seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "day"}))
	goldenJSON(t, "history_forecast", seriesFor(t, store, SeriesOptions{
		Metric: "bytes", Bucket: "day", Histogram: true, Forecast: true,
	}))
	goldenJSON(t, "history_histogram", seriesFor(t, store, SeriesOptions{
		Metric: "created", Bucket: "day", Histogram: true,
	}))
	goldenJSON(t, "history_by_type", seriesFor(t, store, SeriesOptions{
		Metric: "created", Bucket: "day", GroupBy: "type",
	}))
	goldenJSON(t, "history_range", seriesFor(t, store, SeriesOptions{
		Metric: "bytes", Bucket: "day",
		Since: time.Unix(int64(base+86400), 0).UTC(),
		Until: time.Unix(int64(base+3*86400), 0).UTC(),
	}))
	goldenJSON(t, "history_weekly", seriesFor(t, store, SeriesOptions{Metric: "created", Bucket: "week"}))
	goldenJSON(t, "history_net_growth", seriesFor(t, store, SeriesOptions{Metric: "net-growth", Bucket: "day"}))
}

// TestHistoryPruneGoldenOutput pins the `index history prune --json` contract.
func TestHistoryPruneGoldenOutput(t *testing.T) {
	store := newHistoryStore(t)
	writeEvent(t, store, baseHour, 1, deterministicID(1), creationEvent(400))
	writeEvent(t, store, baseHour+testHour, 2, deterministicID(2), creationEvent(600))
	result, err := PruneHistory(context.Background(), store, HistoryRetentionOptions{
		DryRun: true, Now: time.Unix(int64(baseHour+10*testHour), 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	goldenJSON(t, "history_prune_dry_run", result)
}

// TestRepackChurnIsNeverCountedAsGrowth: rewriting a pack moves bytes, it does
// not create them, so net-growth must ignore it while the churn totals still
// report it.
func TestRepackChurnIsNeverCountedAsGrowth(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	writeBucket(t, store, schema.GranularityDaily, 1_699_920_000, schema.PackData, schema.PackHistoryBucket{
		BytesAdded: 500, BytesDeleted: 500, BytesRepacked: 5000, PacksRepacked: 4,
		BytesPromoted: 2000, PacksPromoted: 2, Coverage: schema.CoverageComplete,
	})
	result := seriesFor(t, store, SeriesOptions{Metric: "net-growth", Bucket: "day"})
	if len(result.Points) != 1 || result.Points[0].Value != 0 {
		t.Fatalf("repack and promotion churn leaked into net growth: %#v", result.Points)
	}
	if result.BytesRepacked != 5000 || result.PacksRepacked != 4 {
		t.Fatalf("repack churn was not reported separately: %#v", result)
	}
	if result.BytesPromoted != 2000 || result.PacksPromoted != 2 {
		t.Fatalf("promotion churn was not reported separately: %#v", result)
	}
}

// TestWeeklyBucketsAreStableAcrossTimezonesAndDST is the bucketing guarantee:
// weeks are derived in UTC, so a reader in a zone that observes DST gets the
// same boundaries and never a 23- or 25-hour week.
func TestWeeklyBucketsAreStableAcrossTimezonesAndDST(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	// The US DST transitions of 2023: spring forward 2023-03-12 and fall back
	// 2023-11-05, both inside the days written here.
	days := []string{
		"2023-03-10T12:00:00Z", "2023-03-12T12:00:00Z", "2023-03-14T12:00:00Z",
		"2023-11-03T12:00:00Z", "2023-11-05T12:00:00Z", "2023-11-07T12:00:00Z",
	}
	for _, day := range days {
		moment, err := time.Parse(time.RFC3339, day)
		if err != nil {
			t.Fatal(err)
		}
		start := BucketStart(schema.GranularityDaily, uint64(moment.Unix()))
		writeBucket(t, store, schema.GranularityDaily, start, schema.PackData, schema.PackHistoryBucket{
			PacksCreated: 1, Coverage: schema.CoverageComplete, EventsObserved: 1,
		})
	}

	expected := seriesFor(t, store, SeriesOptions{Metric: "created", Bucket: "week"})
	for _, zone := range []string{"UTC", "America/New_York", "Australia/Lord_Howe", "Asia/Kathmandu"} {
		location, err := time.LoadLocation(zone)
		if err != nil {
			t.Skipf("timezone database unavailable: %v", err)
		}
		previous := time.Local
		time.Local = location
		got := seriesFor(t, store, SeriesOptions{Metric: "created", Bucket: "week"})
		time.Local = previous

		if len(got.Points) != len(expected.Points) {
			t.Fatalf("zone %s changed the number of weekly buckets: %d vs %d", zone, len(got.Points), len(expected.Points))
		}
		for index := range got.Points {
			if got.Points[index] != expected.Points[index] {
				t.Fatalf("zone %s changed weekly bucket %d: %#v vs %#v", zone, index, got.Points[index], expected.Points[index])
			}
		}
	}

	// Every derived week must start on a Monday at midnight UTC and the days
	// must fold into exactly the weeks that contain them.
	for _, point := range expected.Points {
		start := time.Unix(point.BucketStart, 0).UTC()
		if start.Weekday() != time.Monday || start.Hour() != 0 || start.Minute() != 0 || start.Second() != 0 {
			t.Fatalf("weekly bucket does not start on a UTC Monday midnight: %s", start)
		}
	}
	var total int64
	for _, point := range expected.Points {
		total += point.Value
	}
	if total != int64(len(days)) {
		t.Fatalf("weekly folding lost days: total %d, want %d", total, len(days))
	}
}

// TestDailyBucketsAreUTCAlignedRegardlessOfLocalTime keeps the stored
// granularity independent of the process timezone.
func TestDailyBucketsAreUTCAlignedRegardlessOfLocalTime(t *testing.T) {
	location, err := time.LoadLocation("Pacific/Chatham") // a 45-minute DST offset
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}
	previous := time.Local
	time.Local = location
	defer func() { time.Local = previous }()

	moment, err := time.Parse(time.RFC3339, "2023-09-24T13:37:00Z")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(int64(BucketStart(schema.GranularityDaily, uint64(moment.Unix()))), 0).UTC()
	if start.Format(time.RFC3339) != "2023-09-24T00:00:00Z" {
		t.Fatalf("daily bucket start = %s, want 2023-09-24T00:00:00Z", start.Format(time.RFC3339))
	}
}

// TestForecastRefusesIncompleteSeriesUnlessAllowed: projecting growth from
// buckets that were never fully observed would produce a confidently wrong
// number, so it is refused by default and only permitted explicitly.
func TestForecastRefusesIncompleteSeriesUnlessAllowed(t *testing.T) {
	base := uint64(1_699_920_000)
	for name, coverage := range map[string]schema.HistoryCoverage{
		"partial":       schema.CoveragePartial,
		"reconstructed": schema.CoverageReconstructed,
	} {
		store := &memoryStore{values: make(map[string][]byte)}
		for index := range uint64(4) {
			bucketCoverage := schema.CoverageComplete
			if index == 1 {
				bucketCoverage = coverage
			}
			writeBucket(t, store, schema.GranularityDaily, base+index*86400, schema.PackData, schema.PackHistoryBucket{
				BytesAdded: 1000 * (index + 1), Coverage: bucketCoverage, EventsObserved: 2,
			})
		}

		refused := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "day", Forecast: true})
		if refused.Forecast == nil || refused.Forecast.RefusedReason == "" {
			t.Fatalf("%s coverage was forecast anyway: %#v", name, refused.Forecast)
		}
		if !refused.HasIncompleteCoverage() {
			t.Fatalf("%s coverage was not surfaced on the result", name)
		}

		allowed := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "day", Forecast: true, AllowIncomplete: true})
		if allowed.Forecast == nil || allowed.Forecast.RefusedReason != "" {
			t.Fatalf("--allow-incomplete did not permit the %s forecast: %#v", name, allowed.Forecast)
		}
		if !allowed.Forecast.IncompleteInput {
			t.Fatalf("%s forecast did not admit its input was incomplete", name)
		}
	}
}

// TestForecastRefusesWhenThereIsNoTrendToFit: one point, or a flat series, is
// not evidence of a trend.
func TestForecastRefusesWhenThereIsNoTrendToFit(t *testing.T) {
	base := uint64(1_699_920_000)
	single := &memoryStore{values: make(map[string][]byte)}
	writeBucket(t, single, schema.GranularityDaily, base, schema.PackData, schema.PackHistoryBucket{
		BytesAdded: 100, Coverage: schema.CoverageComplete,
	})
	result := seriesFor(t, single, SeriesOptions{Metric: "bytes", Bucket: "day", Forecast: true})
	if result.Forecast == nil || result.Forecast.RefusedReason == "" {
		t.Fatalf("a single point was forecast: %#v", result.Forecast)
	}
}

// TestForecastProjectsAKnownSlope pins the arithmetic on a series whose trend
// is exactly linear.
func TestForecastProjectsAKnownSlope(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	base := uint64(1_699_920_000)
	for index := range uint64(5) {
		writeBucket(t, store, schema.GranularityDaily, base+index*86400, schema.PackData, schema.PackHistoryBucket{
			BytesAdded: 1000 * (index + 1), Coverage: schema.CoverageComplete, EventsObserved: 1,
		})
	}
	forecast := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "day", Forecast: true}).Forecast
	if forecast == nil || forecast.RefusedReason != "" {
		t.Fatalf("a clean linear series was refused: %#v", forecast)
	}
	if forecast.PerBucket < 999.9 || forecast.PerBucket > 1000.1 {
		t.Fatalf("slope = %f, want 1000", forecast.PerBucket)
	}
	if forecast.NextBucketValue < 5999.9 || forecast.NextBucketValue > 6000.1 {
		t.Fatalf("next bucket = %f, want 6000", forecast.NextBucketValue)
	}
	if forecast.BucketsUsed != 5 {
		t.Fatalf("buckets used = %d, want 5", forecast.BucketsUsed)
	}
}

// TestHistogramKeepsTheLargestValue: a half-open final bin would silently drop
// the maximum, which is the one value an operator is most likely looking for.
func TestHistogramKeepsTheLargestValue(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	base := uint64(1_699_920_000)
	values := []uint64{10, 20, 30, 40, 5000}
	for index, value := range values {
		writeBucket(t, store, schema.GranularityDaily, base+uint64(index)*86400, schema.PackData, schema.PackHistoryBucket{
			BytesAdded: value, Coverage: schema.CoverageComplete,
		})
	}
	result := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "day", Histogram: true})
	var counted uint64
	for _, bin := range result.Histogram {
		counted += bin.Count
	}
	if counted != uint64(len(values)) {
		t.Fatalf("histogram counted %d of %d points", counted, len(values))
	}
}

// TestSeriesFoldsToTheWeakestCoverage: a point built from a complete and a
// reconstructed bucket is only as trustworthy as the reconstructed one.
func TestSeriesFoldsToTheWeakestCoverage(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	start := uint64(1_699_920_000)
	writeBucket(t, store, schema.GranularityDaily, start, schema.PackData, schema.PackHistoryBucket{
		PacksCreated: 1, Coverage: schema.CoverageComplete,
	})
	writeBucket(t, store, schema.GranularityDaily, start, schema.PackTree, schema.PackHistoryBucket{
		PacksCreated: 1, Coverage: schema.CoverageReconstructed,
	})
	result := seriesFor(t, store, SeriesOptions{Metric: "created", Bucket: "day"})
	if len(result.Points) != 1 {
		t.Fatalf("buckets of different pack types did not fold: %#v", result.Points)
	}
	if result.Points[0].Coverage != schema.CoverageReconstructed.String() {
		t.Fatalf("folded coverage = %s, want reconstructed", result.Points[0].Coverage)
	}
	if result.ReconstructedBuckets != 1 || result.CompleteBuckets != 0 {
		t.Fatalf("coverage counts do not reflect the fold: %#v", result)
	}
}

// TestSeriesGroupingSplitsByPackType keeps the type dimension available.
func TestSeriesGroupingSplitsByPackType(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	start := uint64(1_699_920_000)
	writeBucket(t, store, schema.GranularityDaily, start, schema.PackData, schema.PackHistoryBucket{
		PacksCreated: 3, Coverage: schema.CoverageComplete,
	})
	writeBucket(t, store, schema.GranularityDaily, start, schema.PackTree, schema.PackHistoryBucket{
		PacksCreated: 7, Coverage: schema.CoverageComplete,
	})
	result := seriesFor(t, store, SeriesOptions{Metric: "created", Bucket: "day", GroupBy: "type"})
	got := map[string]int64{}
	for _, point := range result.Points {
		got[point.Group] = point.Value
	}
	if got["data"] != 3 || got["tree"] != 7 {
		t.Fatalf("grouping by type did not split the series: %#v", got)
	}
}

// TestSeriesRangeFiltersOnBucketStart covers --since and --until.
func TestSeriesRangeFiltersOnBucketStart(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	base := uint64(1_699_920_000)
	for index := range uint64(5) {
		writeBucket(t, store, schema.GranularityDaily, base+index*86400, schema.PackData, schema.PackHistoryBucket{
			PacksCreated: 1, Coverage: schema.CoverageComplete,
		})
	}
	since := time.Unix(int64(base+86400), 0).UTC()
	until := time.Unix(int64(base+3*86400), 0).UTC()
	result := seriesFor(t, store, SeriesOptions{Metric: "created", Bucket: "day", Since: since, Until: until})
	if len(result.Points) != 2 {
		t.Fatalf("range filter returned %d buckets, want 2: %#v", len(result.Points), result.Points)
	}
	if result.Points[0].BucketStart != since.Unix() {
		t.Fatalf("--since is not inclusive: %d vs %d", result.Points[0].BucketStart, since.Unix())
	}
	for _, point := range result.Points {
		if point.BucketStart >= until.Unix() {
			t.Fatalf("--until is not exclusive: %d", point.BucketStart)
		}
	}
}

// TestSeriesRejectsUnknownMetricsAndBuckets: an unrecognised request must fail
// rather than return an empty series that looks like "no activity".
func TestSeriesRejectsUnknownMetricsAndBuckets(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	for name, options := range map[string]SeriesOptions{
		"metric":   {Metric: "gigabytes", Bucket: "day"},
		"bucket":   {Metric: "bytes", Bucket: "fortnight"},
		"grouping": {Metric: "bytes", Bucket: "day", GroupBy: "backend"},
	} {
		if _, err := HistorySeries(context.Background(), store, options); err == nil {
			t.Errorf("unknown %s was accepted", name)
		}
	}
}

// TestCorruptBucketsDoNotFailTheSeries: history is advisory, so a malformed
// rollup must not deny the operator every other answer.
func TestCorruptBucketsDoNotFailTheSeries(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	start := uint64(1_699_920_000)
	writeBucket(t, store, schema.GranularityDaily, start, schema.PackData, schema.PackHistoryBucket{
		PacksCreated: 2, Coverage: schema.CoverageComplete,
	})
	store.values[string(schema.PackHistoryBucketKey(schema.GranularityDaily, start+86400, 0, schema.PackTree))] = []byte{0x01, 0x02}
	result := seriesFor(t, store, SeriesOptions{Metric: "created", Bucket: "day"})
	if len(result.Points) != 1 || result.Points[0].Value != 2 {
		t.Fatalf("a corrupt bucket disturbed the series: %#v", result.Points)
	}
}

// TestHistoryAnswersFromRawEventsBeforeAnyRollup: rollup buckets are only
// written by `index history prune`. Asking a question must not require first
// mutating the repository, so raw events that no bucket covers yet are folded
// in on the fly.
func TestHistoryAnswersFromRawEventsBeforeAnyRollup(t *testing.T) {
	store := newHistoryStore(t)
	writeEvent(t, store, baseHour, 1, vaultic.NewRandomID(), creationEvent(400))
	writeEvent(t, store, baseHour+60, 2, vaultic.NewRandomID(), creationEvent(600))

	result := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "hour"})
	if len(result.Points) != 1 {
		t.Fatalf("raw events produced %d buckets, want 1: %#v", len(result.Points), result.Points)
	}
	if result.Points[0].Value != 1000 {
		t.Fatalf("raw-event fold value = %d, want 1000", result.Points[0].Value)
	}

	// After a rollup the answer must not change, and must certainly not double.
	if _, err := RollupHistory(context.Background(), store, false); err != nil {
		t.Fatal(err)
	}
	rolled := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "hour"})
	if len(rolled.Points) != 1 || rolled.Points[0].Value != 1000 {
		t.Fatalf("rolling up changed or double-counted the series: %#v", rolled.Points)
	}
}

// TestStoredBucketsWinOverRawEvents: once retention has discarded raw events,
// the stored bucket is the only record of that period. Folding the surviving
// raw events on top of it would under- or double-count.
func TestStoredBucketsWinOverRawEvents(t *testing.T) {
	store := newHistoryStore(t)
	writeEvent(t, store, baseHour, 1, vaultic.NewRandomID(), creationEvent(400))
	if _, err := RollupHistory(context.Background(), store, false); err != nil {
		t.Fatal(err)
	}
	before := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "hour"})
	if len(before.Points) != 1 || before.Points[0].Value != 400 {
		t.Fatalf("rolled-up series = %#v, want a single 400 point", before.Points)
	}
	// Running the series again must be idempotent rather than additive.
	after := seriesFor(t, store, SeriesOptions{Metric: "bytes", Bucket: "hour"})
	if after.Points[0].Value != before.Points[0].Value {
		t.Fatalf("repeating the query changed the answer: %d then %d",
			before.Points[0].Value, after.Points[0].Value)
	}
}
