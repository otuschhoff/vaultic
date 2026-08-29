package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	testHour  = 3600
	testDay   = 24 * testHour
	baseHour  = uint64(1700000000) / testHour * testHour
	baseDay   = uint64(1700000000) / testDay * testDay
	oneMonth  = 31 * testDay
	oneMinute = 60
)

// writeEvent stores a raw history event directly, which is how a test builds a
// log without driving a real daemon through every transition.
func writeEvent(t *testing.T, store *memoryStore, seconds, sequence uint64, pack vaultic.ID, record schema.PackHistoryEvent) {
	t.Helper()
	store.set(t, schema.PackHistoryKey(seconds, sequence, schema.ID(pack)), record)
}

func creationEvent(size uint64) schema.PackHistoryEvent {
	return schema.PackHistoryEvent{Type: schema.EventCreated, PackType: schema.PackData, PhysicalSize: size, PayloadSize: size}
}

func deletionEvent(size uint64) schema.PackHistoryEvent {
	return schema.PackHistoryEvent{Type: schema.EventDeleted, PackType: schema.PackData, PhysicalSize: size, PayloadSize: size}
}

func repackEvent(size uint64, predecessor vaultic.ID) schema.PackHistoryEvent {
	return schema.PackHistoryEvent{
		Type: schema.EventRepackedInto, PackType: schema.PackData,
		PhysicalSize: size, PayloadSize: size,
		PredecessorPackIDs: []schema.ID{schema.ID(predecessor)},
	}
}

func newHistoryStore(t *testing.T) *memoryStore {
	t.Helper()
	return &memoryStore{values: make(map[string][]byte)}
}

// TestRollupIsIdempotentAndMatchesRawEvents is the core rollup guarantee: a
// bucket is a pure function of its raw range, so recomputing writes nothing
// and the totals equal a direct scan.
func TestRollupIsIdempotentAndMatchesRawEvents(t *testing.T) {
	store := newHistoryStore(t)
	packA, packB := vaultic.NewRandomID(), vaultic.NewRandomID()
	writeEvent(t, store, baseHour+10, 1, packA, creationEvent(100))
	writeEvent(t, store, baseHour+20, 2, packB, creationEvent(200))
	writeEvent(t, store, baseHour+30, 3, packA, deletionEvent(100))

	first, err := RollupHistory(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if first.EventsScanned != 3 || first.BucketsWritten == 0 {
		t.Fatalf("first rollup = %#v", first)
	}

	// Recomputing over an unchanged raw range must be a no-op.
	writes := store.batchWrites
	second, err := RollupHistory(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if second.BucketsWritten != 0 {
		t.Fatalf("rollup was not idempotent: %#v", second)
	}
	if store.batchWrites != writes {
		t.Fatal("idempotent rollup still wrote to the store")
	}

	// The stored hourly bucket must equal an independent fold of the raw log.
	scanned, err := ScanHistory(context.Background(), store, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := AccumulateBuckets(scanned.Events, []schema.HistoryGranularity{schema.GranularityHourly}, 0, 0)
	key := schema.PackHistoryBucketKey(schema.GranularityHourly, baseHour, 0, schema.PackData)
	value, found, err := store.Get(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("hourly bucket missing: %v", err)
	}
	got, err := schema.UnmarshalPackHistoryBucket(value)
	if err != nil {
		t.Fatal(err)
	}
	for bucketKey, expected := range want {
		if bucketKey.start != baseHour {
			continue
		}
		if got != expected {
			t.Fatalf("bucket = %+v, want %+v", got, expected)
		}
	}
	if got.PacksCreated != 2 || got.PacksDeleted != 1 || got.BytesAdded != 300 || got.BytesDeleted != 100 {
		t.Fatalf("bucket totals = %+v", got)
	}
	if got.EventsObserved != 3 {
		t.Fatalf("observed events = %d, want 3", got.EventsObserved)
	}
}

// TestRepackChurnIsSeparateFromGrowth checks that a rewrite is never counted
// as new data, which is what makes a growth series meaningful.
func TestRepackChurnIsSeparateFromGrowth(t *testing.T) {
	store := newHistoryStore(t)
	original, replacement := vaultic.NewRandomID(), vaultic.NewRandomID()
	writeEvent(t, store, baseHour+10, 1, original, creationEvent(100))
	writeEvent(t, store, baseHour+20, 2, replacement, repackEvent(90, original))
	writeEvent(t, store, baseHour+30, 3, original, deletionEvent(100))

	if _, err := RollupHistory(context.Background(), store, false); err != nil {
		t.Fatal(err)
	}
	value, _, err := store.Get(context.Background(), schema.PackHistoryBucketKey(schema.GranularityHourly, baseHour, 0, schema.PackData))
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := schema.UnmarshalPackHistoryBucket(value)
	if err != nil {
		t.Fatal(err)
	}
	if bucket.PacksCreated != 1 || bucket.BytesAdded != 100 {
		t.Fatalf("repack was counted as growth: %+v", bucket)
	}
	if bucket.PacksRepacked != 1 || bucket.BytesRepacked != 90 {
		t.Fatalf("repack churn not recorded: %+v", bucket)
	}
}

// TestCoverageFlagsReflectCollectionAndTruncation covers step 7 and the
// truncation marker: a bucket predating collection is reconstructed, one
// predating the retained raw floor is partial, and a fully observed one is
// complete.
func TestCoverageFlagsReflectCollectionAndTruncation(t *testing.T) {
	store := newHistoryStore(t)
	pack := vaultic.NewRandomID()
	beforeCollection := baseDay - 2*testDay
	truncated := baseDay - testDay
	observed := baseDay

	writeEvent(t, store, beforeCollection+oneMinute, 1, pack, creationEvent(10))
	writeEvent(t, store, truncated+oneMinute, 2, pack, creationEvent(20))
	writeEvent(t, store, observed+oneMinute, 3, pack, creationEvent(30))

	store.set(t, schema.HistoryEnabledAtKey(), schema.HistoryMarker{UnixSeconds: truncated})
	store.set(t, schema.HistoryRawFloorKey(), schema.HistoryMarker{UnixSeconds: observed})

	result, err := RollupHistory(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Reconstructed == 0 || result.Partial == 0 {
		t.Fatalf("coverage not reported: %#v", result)
	}
	for _, testCase := range []struct {
		name  string
		start uint64
		want  schema.HistoryCoverage
	}{
		{"before collection", beforeCollection, schema.CoverageReconstructed},
		{"before raw floor", truncated, schema.CoveragePartial},
		{"fully observed", observed, schema.CoverageComplete},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			value, found, err := store.Get(context.Background(), schema.PackHistoryBucketKey(schema.GranularityDaily, testCase.start, 0, schema.PackData))
			if err != nil || !found {
				t.Fatalf("daily bucket missing: %v", err)
			}
			bucket, err := schema.UnmarshalPackHistoryBucket(value)
			if err != nil {
				t.Fatal(err)
			}
			if bucket.Coverage != testCase.want {
				t.Fatalf("coverage = %v, want %v", bucket.Coverage, testCase.want)
			}
		})
	}
}

// TestRetentionRollsUpBeforeTruncating is the ordering guarantee of step 5:
// raw events may only be discarded after the buckets describing them exist.
func TestRetentionRollsUpBeforeTruncating(t *testing.T) {
	store := newHistoryStore(t)
	pack := vaultic.NewRandomID()
	now := time.Unix(int64(baseDay)+testDay, 0).UTC()
	old := baseDay - 10*testDay
	recent := uint64(now.Unix()) - oneMinute

	writeEvent(t, store, old, 1, pack, creationEvent(50))
	writeEvent(t, store, recent, 2, pack, creationEvent(70))

	result, err := PruneHistory(context.Background(), store, HistoryRetentionOptions{
		KeepRaw: 2 * 24 * time.Hour, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RawEventsRemoved != 1 {
		t.Fatalf("removed = %d, want 1", result.RawEventsRemoved)
	}
	if result.NewRawFloor == 0 {
		t.Fatal("raw floor was not recorded")
	}

	// The truncated event's bucket must survive, carrying its totals.
	value, found, err := store.Get(context.Background(), schema.PackHistoryBucketKey(schema.GranularityDaily, old/testDay*testDay, 0, schema.PackData))
	if err != nil || !found {
		t.Fatalf("bucket for truncated range missing: %v", err)
	}
	bucket, err := schema.UnmarshalPackHistoryBucket(value)
	if err != nil {
		t.Fatal(err)
	}
	if bucket.PacksCreated != 1 || bucket.BytesAdded != 50 {
		t.Fatalf("truncated range lost its totals: %+v", bucket)
	}

	// Only the recent raw event remains.
	scanned, err := ScanHistory(context.Background(), store, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(scanned.Events) != 1 || scanned.Events[0].UnixSeconds != recent {
		t.Fatalf("remaining raw events = %+v", scanned.Events)
	}

	// A later rollup now reports the truncated range as partial rather than
	// silently complete.
	if _, err := RollupHistory(context.Background(), store, false); err != nil {
		t.Fatal(err)
	}
	floor, err := readHistoryMarker(context.Background(), store, schema.HistoryRawFloorKey())
	if err != nil || floor == 0 {
		t.Fatalf("floor = %d, %v", floor, err)
	}
}

// TestRetentionNeverLowersTheRawFloor guards against a second pass claiming
// coverage the repository no longer has.
func TestRetentionNeverLowersTheRawFloor(t *testing.T) {
	store := newHistoryStore(t)
	store.set(t, schema.HistoryRawFloorKey(), schema.HistoryMarker{UnixSeconds: baseDay + 100*testDay})
	now := time.Unix(int64(baseDay)+testDay, 0).UTC()

	result, err := PruneHistory(context.Background(), store, HistoryRetentionOptions{KeepRaw: time.Hour, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.NewRawFloor != baseDay+100*testDay {
		t.Fatalf("raw floor moved backwards to %d", result.NewRawFloor)
	}
}

// TestRetentionPrunesBucketsPerGranularity checks that each tier is retained on
// its own schedule.
func TestRetentionPrunesBucketsPerGranularity(t *testing.T) {
	store := newHistoryStore(t)
	now := time.Unix(int64(baseDay)+oneMonth, 0).UTC()
	stale := baseDay - 5*testDay

	for _, granularity := range schema.HistoryGranularities() {
		store.set(t, schema.PackHistoryBucketKey(granularity, stale, 0, schema.PackData),
			schema.PackHistoryBucket{PacksCreated: 1, Coverage: schema.CoverageComplete})
	}

	result, err := PruneHistory(context.Background(), store, HistoryRetentionOptions{
		KeepHourly: 24 * time.Hour, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.BucketsRemoved != 1 {
		t.Fatalf("removed = %d, want only the hourly bucket", result.BucketsRemoved)
	}
	if _, found, _ := store.Get(context.Background(), schema.PackHistoryBucketKey(schema.GranularityHourly, stale, 0, schema.PackData)); found {
		t.Fatal("hourly bucket outlived its retention")
	}
	for _, granularity := range []schema.HistoryGranularity{schema.GranularityDaily, schema.GranularityMonthly} {
		if _, found, _ := store.Get(context.Background(), schema.PackHistoryBucketKey(granularity, stale, 0, schema.PackData)); !found {
			t.Fatalf("%v bucket pruned without a retention setting", granularity)
		}
	}
}

// TestCorruptHistoryLeavesDataPathsGreen is the step 6 fault injection: a
// corrupt or truncated history record must never fail or alter a data path.
func TestCorruptHistoryLeavesDataPathsGreen(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackImported)
	destination := &memoryDestination{}
	if _, err := Export(context.Background(), store, destination, ExportOptions{Full: true}); err != nil {
		t.Fatal(err)
	}
	clean, err := Check(context.Background(), destination, store, 10)
	if err != nil || !clean.Clean() {
		t.Fatalf("baseline check = %#v, %v", clean, err)
	}

	// Corrupt every kind of history record at once.
	store.values[string(schema.PackHistoryKey(baseHour, 1, schema.ID(vaultic.NewRandomID())))] = []byte{0}
	store.values[string(schema.PackHistoryBucketKey(schema.GranularityHourly, baseHour, 0, schema.PackData))] = []byte{0}
	store.values[string(schema.HistoryRawFloorKey())] = []byte{0}
	store.values[string(schema.HistoryEnabledAtKey())] = []byte{0, 1, 2}

	// Every data path must still succeed and reach the same verdict.
	if _, err := Export(context.Background(), store, &memoryDestination{}, ExportOptions{Full: true}); err != nil {
		t.Fatalf("export failed with corrupt history: %v", err)
	}
	after, err := Check(context.Background(), destination, store, 10)
	if err != nil {
		t.Fatalf("check failed with corrupt history: %v", err)
	}
	if !after.Clean() {
		t.Fatalf("corrupt history changed the check verdict: %#v", after)
	}
	if _, err := RebuildPackAggregates(context.Background(), store, false); err != nil {
		t.Fatalf("aggregate rebuild failed with corrupt history: %v", err)
	}

	// History's own readers degrade instead of failing: the corrupt raw event
	// is counted, not fatal.
	scanned, err := ScanHistory(context.Background(), store, 0, 0)
	if err != nil {
		t.Fatalf("history scan failed on corrupt input: %v", err)
	}
	if scanned.Malformed != 1 {
		t.Fatalf("malformed events = %d, want 1", scanned.Malformed)
	}
	rollup, err := RollupHistory(context.Background(), store, false)
	if err != nil {
		t.Fatalf("rollup failed on corrupt input: %v", err)
	}
	if rollup.MalformedEvents != 1 {
		t.Fatalf("rollup malformed = %d, want 1", rollup.MalformedEvents)
	}

	// Check reports the corruption as a count without changing its verdict.
	if after.HistoryEventsMalformed != 1 {
		t.Fatalf("check reported %d malformed history events, want 1", after.HistoryEventsMalformed)
	}
}

// TestImportedBucketsAreReconstructed covers the second clause of step 7: an
// import records packs that existed before the import ran, so the bucket
// describes inferred rather than observed activity.
func TestImportedBucketsAreReconstructed(t *testing.T) {
	store := newHistoryStore(t)
	pack := vaultic.NewRandomID()
	writeEvent(t, store, baseHour+10, 1, pack, schema.PackHistoryEvent{
		Type: schema.EventImported, PackType: schema.PackData, PhysicalSize: 40, PayloadSize: 40,
	})
	writeEvent(t, store, baseHour+2*testHour+10, 2, vaultic.NewRandomID(), creationEvent(60))

	if _, err := RollupHistory(context.Background(), store, false); err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name  string
		start uint64
		want  schema.HistoryCoverage
	}{
		{"import bucket", baseHour, schema.CoverageReconstructed},
		{"ordinary bucket", baseHour + 2*testHour, schema.CoverageComplete},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			value, found, err := store.Get(context.Background(), schema.PackHistoryBucketKey(schema.GranularityHourly, testCase.start, 0, schema.PackData))
			if err != nil || !found {
				t.Fatalf("bucket missing: %v", err)
			}
			bucket, err := schema.UnmarshalPackHistoryBucket(value)
			if err != nil {
				t.Fatal(err)
			}
			if bucket.Coverage != testCase.want {
				t.Fatalf("coverage = %v, want %v", bucket.Coverage, testCase.want)
			}
		})
	}
}

// TestBucketStartAlignsOnUTCBoundaries pins bucketing to UTC so a boundary does
// not move with the reader's timezone.
func TestBucketStartAlignsOnUTCBoundaries(t *testing.T) {
	moment := time.Date(2026, 3, 15, 13, 47, 22, 0, time.UTC)
	seconds := uint64(moment.Unix())
	for _, testCase := range []struct {
		granularity schema.HistoryGranularity
		want        time.Time
	}{
		{schema.GranularityHourly, time.Date(2026, 3, 15, 13, 0, 0, 0, time.UTC)},
		{schema.GranularityDaily, time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)},
		{schema.GranularityMonthly, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
	} {
		if got := BucketStart(testCase.granularity, seconds); got != uint64(testCase.want.Unix()) {
			t.Fatalf("%v start = %d, want %d", testCase.granularity, got, testCase.want.Unix())
		}
	}
}
