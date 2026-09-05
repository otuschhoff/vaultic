package analytics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type memoryStore struct {
	mu                     sync.Mutex
	publicationMu          sync.Mutex
	values                 map[string][]byte
	failPublication        bool
	failDeltaDelete        bool
	failSegment            uint64
	failCheckpointCursor   uint64
	writeCounts            map[string]int
	publicationPuts        int
	cancelJobAfterSegments int
	cancelJob              context.CancelFunc
	failScanPrefix         []byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{values: map[string][]byte{}, writeCounts: map[string]int{}}
}

func (store *memoryStore) LockAnalyticsPublication() {
	store.publicationMu.Lock()
}

func (store *memoryStore) UnlockAnalyticsPublication() {
	store.publicationMu.Unlock()
}

func (store *memoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.values[string(key)]
	return append([]byte(nil), value...), ok, nil
}

func (store *memoryStore) ScanPrefix(
	_ context.Context,
	prefix, cursor []byte,
	limit uint32,
) ([]daemon.KeyValue, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.failScanPrefix) != 0 && bytes.Equal(prefix, store.failScanPrefix) {
		return nil, false, fmt.Errorf("injected forbidden scan of %q", prefix)
	}
	keys := make([]string, 0)
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), prefix) && (len(cursor) == 0 || bytes.Compare([]byte(key), cursor) >= 0) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	done := len(keys) <= int(limit)
	if !done {
		keys = keys[:limit]
	}
	result := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		result[index] = daemon.KeyValue{Key: []byte(key), Value: append([]byte(nil), store.values[key]...)}
	}
	return result, done, nil
}

func (store *memoryStore) WriteMutableBatch(_ context.Context, puts []daemon.Mutation, deletes [][]byte, _ bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, put := range puts {
		if store.failSegment != 0 && bytes.Equal(put.Key, schema.AnalyticsFactSegmentKey(store.failSegment)) {
			store.failSegment = 0
			return errors.New("injected candidate segment failure")
		}
		if bytes.Equal(put.Key, schema.AnalyticsBuildCheckpointKey()) && store.failCheckpointCursor != 0 {
			checkpoint, err := schema.UnmarshalAnalyticsBuildCheckpointRecord(put.Value)
			if err == nil && checkpoint.Facts == store.failCheckpointCursor {
				store.failCheckpointCursor = 0
				return errors.New("injected build checkpoint failure")
			}
		}
	}
	if store.failPublication {
		for _, put := range puts {
			if bytes.HasPrefix(put.Key, schema.AnalyticsManifestPrefix()) {
				return errors.New("injected publication failure")
			}
		}
	}
	for _, put := range puts {
		if bytes.HasPrefix(put.Key, schema.AnalyticsManifestPrefix()) {
			store.publicationPuts = len(puts)
			break
		}
	}
	if store.failDeltaDelete {
		for _, key := range deletes {
			if bytes.HasPrefix(key, schema.AnalyticsDeltaPrefix()) {
				return errors.New("injected outbox reclamation failure")
			}
		}
	}
	for _, put := range puts {
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
		store.writeCounts[string(put.Key)]++
		if store.cancelJobAfterSegments != 0 && bytes.HasPrefix(put.Key, schema.AnalyticsQueryJobPrefix()) {
			job, err := schema.UnmarshalAnalyticsQueryJobRecord(put.Value)
			if err == nil && len(job.CompletedSegments) == store.cancelJobAfterSegments && store.cancelJob != nil {
				store.cancelJob()
				store.cancelJob = nil
			}
		}
	}
	for _, key := range deletes {
		delete(store.values, string(key))
	}
	return nil
}

type memoryStoreWithHead struct {
	*memoryStore
	head uint64
}

func (store *memoryStoreWithHead) MetadataHead(context.Context) (uint64, error) {
	return store.head, nil
}

func putRecord(t *testing.T, store *memoryStore, key []byte, record interface{ MarshalBinary() ([]byte, error) }) {
	t.Helper()
	value, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(key)] = value
}

func TestRebuildQueryCacheDisableAndReenable(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	created := time.Date(2019, time.December, 30, 12, 0, 0, 0, time.UTC).UnixNano()
	known := schema.KnownCTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath
	first := schema.InodeRevision{
		CTime:      created,
		Size:       1000,
		UID:        10,
		GID:        20,
		Known:      known,
		SourcePath: "/svm-a/volume-a/qtree/file",
		Freshness:  schema.FreshnessVerified,
	}
	later := first
	later.Size = 100000
	later.SourcePath = "/changed/path/file"
	archive := schema.InodeRevision{
		MTime:      created,
		Size:       10_000,
		UID:        11,
		GID:        21,
		Known:      schema.KnownMTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath,
		SourcePath: "/svm-b/volume-b/other/file",
		Freshness:  schema.FreshnessImported,
	}
	putRecord(t, store, schema.InodeRevisionKey(1, 100, 1), first)
	putRecord(t, store, schema.InodeRevisionKey(1, 100, 2), later)
	putRecord(t, store, schema.InodeRevisionKey(1, 200, 3), archive)
	putRecord(
		t,
		store,
		schema.CurrentInodeKey(1, 100),
		schema.CurrentPointer{Revision: 2, RecordKey: schema.InodeRevisionKey(1, 100, 2)},
	)
	config := Config{PathGroupPrefixes: []string{"/svm-a/volume-a/qtree"}, CacheAfter: 2, CacheTTLSeconds: 60}
	built, err := Enable(ctx, store, config, false)
	if err != nil {
		t.Fatal(err)
	}
	if built.Facts != 2 || !built.Enabled {
		t.Fatalf("unexpected build: %+v", built)
	}

	query := Query{
		Years:             []int{2019},
		Months:            []int{12},
		ISOYears:          []int{2020},
		Workweeks:         []int{1},
		SizeLog10:         []int{3},
		SVMs:              []string{"svm-a"},
		Volumes:           []string{"volume-a"},
		PathGroups:        []string{"/svm-a/volume-a/qtree"},
		Residencies:       []string{"live"},
		GroupBy:           []string{"uid", "month", "workweek", "size-log10", "residency"},
		IncludeIncomplete: true,
	}
	result, err := Execute(ctx, store, query)
	if err != nil {
		t.Fatal(err)
	}
	if result.Files != 1 || result.LogicalBytes != 1000 || len(result.Groups) != 1 || result.Cached {
		t.Fatalf("unexpected result: %+v", result)
	}
	archiveResult, err := Execute(ctx, store, Query{Residencies: []string{"unknown"}, IncludeIncomplete: true})
	if err != nil || archiveResult.Files != 1 || archiveResult.LogicalBytes != 10_000 {
		t.Fatalf("unknown historical residency classification failed: %+v, %v", archiveResult, err)
	}
	result, err = Execute(ctx, store, query)
	if err != nil {
		t.Fatal(err)
	}
	if result.Cached {
		t.Fatal("second query should promote, not hit, the cache")
	}
	result, err = Execute(ctx, store, query)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Cached {
		t.Fatal("third identical query should hit the cache")
	}
	status, err := Status(ctx, store)
	if err != nil || status.CacheEntries != 1 {
		t.Fatalf("cache promotion missing from status: %+v, %v", status, err)
	}
	maximum := uint64(1000)
	result, err = Execute(ctx, store, Query{SizeMax: &maximum, IncludeIncomplete: true})
	if err != nil || result.Files != 0 {
		t.Fatalf("exclusive size maximum matched its boundary: %+v, %v", result, err)
	}

	disabled, err := Disable(ctx, store, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.Enabled || disabled.Removed < 3 {
		t.Fatalf("unexpected disable result: %+v", disabled)
	}
	if _, err := Execute(ctx, store, Query{}); err == nil {
		t.Fatal("query unexpectedly succeeded while disabled")
	}
	rebuilt, err := Enable(ctx, store, config, false)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.Facts != 2 || rebuilt.Generation != 2 {
		t.Fatalf("unexpected re-enable result: %+v", rebuilt)
	}
	result, err = Execute(ctx, store, query)
	if err != nil || result.Cached || result.Generation != 2 {
		t.Fatalf("old-generation cache survived rebuild: %+v, %v", result, err)
	}
}

func TestUnknownFieldsAndValidation(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRecord(t, store, schema.InodeRevisionKey(2, 1, 1), schema.InodeRevision{Freshness: schema.FreshnessUnknown})
	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	result, err := Execute(
		ctx,
		store,
		Query{GroupBy: []string{"uid", "gid", "year", "size-log10"}, IncludeIncomplete: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Files != 1 || result.UnknownCreationTime != 1 || result.Groups[0].Dimensions["uid"] != "unknown" ||
		result.Groups[0].Dimensions["size-log10"] != "unknown" {
		t.Fatalf("unknown provenance was lost: %+v", result)
	}
	if result, err = Execute(ctx, store, Query{UIDs: []uint32{0}, IncludeIncomplete: true}); err != nil ||
		result.Files != 0 {
		t.Fatalf("unknown UID matched UID zero: %+v, %v", result, err)
	}
	for _, query := range []Query{{Months: []int{13}},
		{Workweeks: []int{0}},
		{GroupBy: []string{"bogus"}},
		{Residencies: []string{"missing"}},
		{SizeMin: uint64Pointer(10),
			SizeMax: uint64Pointer(10)}} {
		if _, err := Execute(ctx, store, query); err == nil {
			t.Fatalf("invalid query accepted: %+v", query)
		}
	}
	if err := (Config{PathGroupPrefixes: []string{"/a", "/a"}}).Validate(); err == nil {
		t.Fatal("duplicate path-group prefix accepted")
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }

func TestManifestPublicationIsWholeAndEmptyGenerationIsValid(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	if result, err := Enable(ctx, store, Config{}, false); err != nil || result.Facts != 0 {
		t.Fatalf("empty rebuild failed: %+v, %v", result, err)
	}
	result, err := Execute(ctx, store, Query{})
	if err != nil || result.Files != 0 || result.Generation != 1 {
		t.Fatalf("empty published generation failed: %+v, %v", result, err)
	}
	if _, found, err := store.Get(ctx, schema.AnalyticsDerivedGenerationMarkerKey(1)); err != nil || !found {
		t.Fatalf("empty generation marker: found=%t err=%v", found, err)
	}
	putRevision(t, store, 1, 1, 1, 7, 8, 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	store.failPublication = true
	if _, err := Rebuild(ctx, store, Config{}, false); err == nil {
		t.Fatal("injected publication failure was ignored")
	}
	store.failPublication = false
	result, err = Execute(ctx, store, Query{})
	if err != nil || result.Generation != 1 || result.Files != 0 {
		t.Fatalf("failed generation became visible: %+v, %v", result, err)
	}
}

func TestRebuildResumesCandidateCheckpoints(t *testing.T) {
	for _, test := range []struct {
		name                 string
		configureFailure     func(*memoryStore)
		wantCheckpointCursor uint64
		wantSourceKey        []byte
		wantFirstWrites      int
	}{
		{
			name:                 "before candidate write",
			configureFailure:     func(store *memoryStore) { store.failSegment = 1<<32 | 1 },
			wantCheckpointCursor: 0,
			wantFirstWrites:      1,
		},
		{name: ("after candidate write before checkpoint"),
			configureFailure:     func(store *memoryStore) { store.failCheckpointCursor = 2 },
			wantCheckpointCursor: 0,
			wantFirstWrites:      2},

		{name: ("after candidate checkpoint"),
			configureFailure:     func(store *memoryStore) { store.failSegment = 1<<32 | 2 },
			wantCheckpointCursor: 2,
			wantSourceKey: schema.InodeRevisionKey(1,
				2,
				2),
			wantFirstWrites: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryStore()
			for index := 0; index < 4; index++ {
				putRevision(
					t,
					store,
					1,
					uint64(index+1),
					uint64(index+1),
					10,
					20,
					100,
					time.Date(2024, 1, index+1, 0, 0, 0, 0, time.UTC),
					fmt.Sprintf("/svm/vol/group/%d", index),
					true,
				)
			}
			test.configureFailure(store)
			if _, err := Rebuild(ctx, store, Config{SegmentRows: 2}, false); err == nil {
				t.Fatal("injected rebuild failure was ignored")
			}
			value, found, err := store.Get(ctx, schema.AnalyticsBuildCheckpointKey())
			if err != nil || !found {
				t.Fatalf("build checkpoint: found=%t err=%v", found, err)
			}
			checkpoint, err := schema.UnmarshalAnalyticsBuildCheckpointRecord(value)
			if err != nil || checkpoint.Facts != test.wantCheckpointCursor {
				t.Fatalf("checkpoint = %#v, err=%v", checkpoint, err)
			}
			if !bytes.Equal(checkpoint.SourceKeyCursor, test.wantSourceKey) {
				t.Fatalf("source cursor = %x, want %x", checkpoint.SourceKeyCursor, test.wantSourceKey)
			}
			result, err := Rebuild(ctx, store, Config{SegmentRows: 2}, false)
			if err != nil || !result.Resumed || result.Facts != 4 {
				t.Fatalf("resumed rebuild = %+v, %v", result, err)
			}
			firstSegment := schema.AnalyticsFactSegmentKey(1<<32 | 1)
			if writes := store.writeCounts[string(firstSegment)]; writes != test.wantFirstWrites {
				t.Fatalf("first candidate writes = %d, want %d", writes, test.wantFirstWrites)
			}
			if _, found, _ := store.Get(ctx, schema.AnalyticsBuildCheckpointKey()); found {
				t.Fatal("published build checkpoint survived pointer flip")
			}
		})
	}
}

func TestRebuildBoundsFactBufferAndKeepsViewsExactAcrossBatches(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	for index := 0; index < 7; index++ {
		putRevision(
			t,
			store,
			1,
			uint64(index+1),
			uint64(index+1),
			42,
			7,
			10,
			time.Date(2024, 1, index+1, 0, 0, 0, 0, time.UTC),
			fmt.Sprintf("/svm/vol/group/%d", index),
			true,
		)
	}
	result, err := Rebuild(ctx, store, Config{SegmentRows: 3}, false)
	if err != nil || result.Facts != 7 || result.PeakFactsBuffered != 3 || result.PeakWorkingSetBytes == 0 {
		t.Fatalf("bounded rebuild = %+v, %v", result, err)
	}
	growth, err := Growth(ctx, store, GrowthOptions{Granularity: "year"})
	if err != nil || len(growth.Buckets) != 1 || growth.Buckets[0].Files != 7 || growth.Buckets[0].LogicalBytes != 70 {
		t.Fatalf("multi-batch growth view = %+v, %v", growth, err)
	}
}

func TestCatchUpCompactsAtManifestChainBound(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 1, 1, 1, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/1", true)
	if _, err := Enable(ctx, store, Config{SegmentRows: 2}, false); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < maxManifestLayerDepth+1; index++ {
		status, err := Status(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		revision := uint64(index + 2)
		putRevision(
			t,
			store,
			1,
			revision,
			revision,
			uint32(revision),
			1,
			1,
			time.Date(2024, 1, index+2, 0, 0, 0, 0, time.UTC),
			fmt.Sprintf("/svm/vol/group/%d", revision),
			true,
		)
		delta := schema.AnalyticsDeltaRecord{
			Kind:                schema.AnalyticsDeltaCreation,
			FSID:                1,
			Inode:               revision,
			IdentityGeneration:  revision,
			Revision:            revision,
			UID:                 uint32(revision),
			GID:                 1,
			Known:               schema.KnownUID | schema.KnownGID | schema.KnownSize,
			CreatedAt:           int64(revision),
			LogicalSize:         1,
			CreationBasis:       schema.AnalyticsFirstSeen,
			IdentityContinuity:  schema.AnalyticsContinuitySourceGeneration,
			State:               schema.AnalyticsLive,
			ClassificationEpoch: status.Generation,
		}
		putRecord(t, store, schema.AnalyticsDeltaKey(revision, 0), delta)
		if _, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1}); err != nil {
			t.Fatal(err)
		}
	}
	pinned, found, err := pinLatest(ctx, store)
	if err != nil || !found || pinned.manifest.LayerDepth != 0 {
		t.Fatalf("compacted manifest = %+v, found=%t err=%v", pinned.manifest, found, err)
	}
	query, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
	if err != nil || query.Files != uint64(maxManifestLayerDepth+2) {
		t.Fatalf("compacted query = %+v, %v", query, err)
	}
}

func TestConcurrentRebuildQueryAndCatchUp(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 1, 1, 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/1", true)
	if _, err := Enable(ctx, store, Config{SegmentRows: 1}, false); err != nil {
		t.Fatal(err)
	}
	putRevision(t, store, 1, 2, 2, 2, 1, 200, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/group/2", true)
	delta := schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaCreation,
		FSID:                1,
		Inode:               2,
		IdentityGeneration:  2,
		Revision:            2,
		UID:                 2,
		GID:                 1,
		Known:               schema.KnownUID | schema.KnownGID | schema.KnownSize,
		CreatedAt:           2,
		LogicalSize:         200,
		CreationBasis:       schema.AnalyticsFirstSeen,
		IdentityContinuity:  schema.AnalyticsContinuitySourceGeneration,
		State:               schema.AnalyticsLive,
		ClassificationEpoch: 1,
	}
	putRecord(t, store, schema.AnalyticsDeltaKey(2, 0), delta)
	errors := make(chan error, 3)
	go func() {
		_, err := Rebuild(ctx, store, Config{SegmentRows: 1}, false)
		errors <- err
	}()
	go func() {
		_, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1})
		errors <- err
	}()
	go func() {
		for range 20 {
			result, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
			if err != nil {
				errors <- err
				return
			}
			if result.Files != 1 && result.Files != 2 {
				errors <- fmt.Errorf("query observed partial generation: %+v", result)
				return
			}
		}
		errors <- nil
	}()
	for range 3 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	result, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
	if err != nil || result.Files != 2 || result.LogicalBytes != 300 {
		t.Fatalf("final concurrent result = %+v, %v", result, err)
	}
}

func TestRebuildFailureBeforePointerFlipKeepsOldEpochAndResumes(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 8, 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{SegmentRows: 1}, false); err != nil {
		t.Fatal(err)
	}
	putRevision(t, store, 1, 2, 2, 9, 10, 200, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/group/b", true)
	store.failPublication = true
	if _, err := Rebuild(ctx, store, Config{SegmentRows: 1}, false); err == nil {
		t.Fatal("injected pointer-flip failure was ignored")
	}
	store.failPublication = false
	old, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
	if err != nil || old.Generation != 1 || old.Files != 1 {
		t.Fatalf("old epoch visibility = %+v, %v", old, err)
	}
	growth, err := Growth(ctx, store, GrowthOptions{Granularity: "month"})
	if err != nil || growth.Explain.Source != "materialized-view" || len(growth.Buckets) != 1 ||
		growth.Buckets[0].Files != 1 {
		t.Fatalf("failed candidate views became visible: %+v, %v", growth, err)
	}
	resumed, err := Rebuild(ctx, store, Config{SegmentRows: 1}, false)
	if err != nil || !resumed.Resumed || resumed.Generation != 2 {
		t.Fatalf("pointer-flip resume = %+v, %v", resumed, err)
	}
}

func TestCorruptBuildCheckpointRebuildsWithoutSkippingFacts(t *testing.T) {
	for _, test := range []struct {
		name    string
		corrupt func(*testing.T, *memoryStore, schema.AnalyticsBuildCheckpointRecord) schema.AnalyticsBuildCheckpointRecord
	}{
		{
			name: "source cursor",
			corrupt: func(
				_ *testing.T, _ *memoryStore, checkpoint schema.AnalyticsBuildCheckpointRecord,
			) schema.AnalyticsBuildCheckpointRecord {
				checkpoint.SourceKeyCursor = []byte("iv:corrupt")
				return checkpoint
			},
		},
		{name: "segment ordinal",
			corrupt: func(_ *testing.T,
				_ *memoryStore,
				checkpoint schema.AnalyticsBuildCheckpointRecord) schema.AnalyticsBuildCheckpointRecord {
				checkpoint.CandidateSegments[0]++
				return checkpoint
			}},
		{name: "segment metadata",
			corrupt: func(_ *testing.T,
				store *memoryStore,
				checkpoint schema.AnalyticsBuildCheckpointRecord) schema.AnalyticsBuildCheckpointRecord {
				store.values[string(schema.AnalyticsSegmentMetadataKey(checkpoint.CandidateSegments[0]))] = []byte("corrupt")
				return checkpoint
			}},
		{name: "segment rows",
			corrupt: func(_ *testing.T,
				store *memoryStore,
				checkpoint schema.AnalyticsBuildCheckpointRecord) schema.AnalyticsBuildCheckpointRecord {
				store.values[string(schema.AnalyticsFactSegmentKey(checkpoint.CandidateSegments[0]))] = []byte("corrupt")
				return checkpoint
			}},
		{name: "segment index",
			corrupt: func(t *testing.T,
				store *memoryStore,
				checkpoint schema.AnalyticsBuildCheckpointRecord) schema.AnalyticsBuildCheckpointRecord {
				for key := range store.values {
					parsed, err := schema.ParseKey([]byte(key))
					if err == nil && parsed.Kind == schema.KeyAnalyticsDimensionIndex && parsed.Generation == checkpoint.CandidateSegments[0] {
						delete(store.values, key)
						return checkpoint
					}
				}
				t.Fatal("candidate segment had no index")
				return checkpoint
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryStore()
			for index := 0; index < 3; index++ {
				putRevision(
					t,
					store,
					1,
					uint64(index+1),
					uint64(index+1),
					uint32(10+index),
					20,
					100,
					time.Date(2024, 1, index+1, 0, 0, 0, 0, time.UTC),
					fmt.Sprintf("/svm/vol/group/%d", index),
					true,
				)
			}
			store.failSegment = 1<<32 | 2
			if _, err := Rebuild(ctx, store, Config{SegmentRows: 2}, false); err == nil {
				t.Fatal("injected candidate failure was ignored")
			}
			value, _, _ := store.Get(ctx, schema.AnalyticsBuildCheckpointKey())
			checkpoint, err := schema.UnmarshalAnalyticsBuildCheckpointRecord(value)
			if err != nil {
				t.Fatal(err)
			}
			checkpoint = test.corrupt(t, store, checkpoint)
			putRecord(t, store, schema.AnalyticsBuildCheckpointKey(), checkpoint)
			result, err := Rebuild(ctx, store, Config{SegmentRows: 2}, false)
			if err != nil || result.Resumed || result.Facts != 3 {
				t.Fatalf("corrupt checkpoint rebuild = %+v, %v", result, err)
			}
			query, err := Execute(ctx, store, Query{IncludeIncomplete: true})
			if err != nil || query.Files != 3 {
				t.Fatalf("corrupt checkpoint skipped facts: %+v, %v", query, err)
			}
		})
	}
}

func TestCatchUpCrashBeforeAndAfterPublication(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 8, 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	putRevision(t, store, 1, 2, 2, 9, 10, 200, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/group/b", true)
	delta := schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaCreation,
		FSID:                1,
		Inode:               2,
		IdentityGeneration:  2,
		Revision:            2,
		UID:                 9,
		GID:                 10,
		Known:               schema.KnownUID | schema.KnownGID | schema.KnownSize,
		CreatedAt:           2,
		LogicalSize:         200,
		CreationBasis:       schema.AnalyticsFirstSeen,
		State:               schema.AnalyticsLive,
		ClassificationEpoch: 1,
	}
	putRecord(t, store, schema.AnalyticsDeltaKey(2, 0), delta)

	store.failPublication = true
	if _, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1}); err == nil {
		t.Fatal("catch-up ignored publication failure")
	}
	store.failPublication = false
	if _, found, _ := store.Get(ctx, schema.AnalyticsDeltaKey(2, 0)); !found {
		t.Fatal("delta was deleted before durable publication")
	}
	pinned, _, err := pinLatest(ctx, store)
	if err != nil || pinned.watermark.AppliedCommit != 1 {
		t.Fatalf("failed publication advanced watermark: %+v, %v", pinned.watermark, err)
	}

	store.failDeltaDelete = true
	if _, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1}); err == nil {
		t.Fatal("catch-up ignored post-publication reclamation failure")
	}
	store.failDeltaDelete = false
	pinned, _, err = pinLatest(ctx, store)
	if err != nil || pinned.watermark.AppliedCommit != 2 {
		t.Fatalf("durable publication was not visible: %+v, %v", pinned.watermark, err)
	}
	generation := pinned.manifest.Generation
	result, err := CatchUp(ctx, &memoryStoreWithHead{memoryStore: store, head: 2}, CatchUpOptions{MaxDeltas: 1})
	if err != nil || result.Processed != 1 || !result.Current || result.LagCommits != 0 {
		t.Fatalf("catch-up replay failed: %+v, %v", result, err)
	}
	pinned, _, _ = pinLatest(ctx, store)
	if pinned.manifest.Generation != generation {
		t.Fatalf("reclamation replay rebuilt generation %d as %d", generation, pinned.manifest.Generation)
	}
	if _, found, _ := store.Get(ctx, schema.AnalyticsDeltaKey(2, 0)); found {
		t.Fatal("covered delta survived successful replay")
	}
	query, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
	if err != nil || query.Files != 2 || query.LogicalBytes != 300 {
		t.Fatalf("catch-up replay changed totals: %+v, %v", query, err)
	}
}

func TestCatchUpDoesNotScanAuthoritativeRevisions(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 8, 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{SegmentRows: 1}, false); err != nil {
		t.Fatal(err)
	}
	putRevision(t, store, 1, 2, 2, 9, 10, 200, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/group/b", true)
	delta := schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaCreation,
		FSID:                1,
		Inode:               2,
		IdentityGeneration:  2,
		Revision:            2,
		UID:                 9,
		GID:                 10,
		Known:               schema.KnownUID | schema.KnownGID | schema.KnownSize,
		CreatedAt:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
		LogicalSize:         200,
		CreationBasis:       schema.AnalyticsFirstSeen,
		IdentityContinuity:  schema.AnalyticsContinuitySourceGeneration,
		State:               schema.AnalyticsLive,
		ClassificationEpoch: 1,
	}
	putRecord(t, store, schema.AnalyticsDeltaKey(2, 0), delta)
	store.failScanPrefix = []byte("iv:")
	if result, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1}); err != nil || result.Processed != 1 ||
		result.AppliedCommit != 2 {
		t.Fatalf("incremental catch-up = %+v, %v", result, err)
	}
	query, err := Execute(ctx, store, Query{IncludeIncomplete: true, AllowStale: true})
	if err != nil || query.Files != 2 || query.LogicalBytes != 300 || query.Generation != 2 {
		t.Fatalf("layered query = %+v, %v", query, err)
	}
}

func TestCatchUpTombstonesParentGDPRMappingOnUIDChange(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	created := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	revision := schema.InodeRevision{
		CTime:        created.UnixNano(),
		Size:         100,
		UID:          7,
		GID:          8,
		Known:        schema.KnownCTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath,
		SourcePath:   "/svm/vol/group/a",
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
	revision.UID = 9
	putRecord(t, store, schema.InodeRevisionKey(1, 1, 2), revision)
	delta := schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaCreation,
		FSID:                1,
		Inode:               1,
		IdentityGeneration:  1,
		Revision:            2,
		UID:                 9,
		GID:                 8,
		Known:               revision.Known,
		CreatedAt:           revision.CTime,
		LogicalSize:         revision.Size,
		CreationBasis:       schema.AnalyticsCTime,
		IdentityContinuity:  schema.AnalyticsContinuityProven,
		State:               schema.AnalyticsLive,
		ClassificationEpoch: 1,
	}
	putRecord(t, store, schema.AnalyticsDeltaKey(2, 0), delta)
	if _, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1}); err != nil {
		t.Fatal(err)
	}
	oldAudit, err := GDPRAudit(ctx, store, 7)
	if err != nil || len(oldAudit.Inodes) != 0 || len(oldAudit.Blobs) != 0 {
		t.Fatalf("old UID mappings survived tombstone: %+v, %v", oldAudit, err)
	}
	newAudit, err := GDPRAudit(ctx, store, 9)
	if err != nil || len(newAudit.Inodes) != 1 || len(newAudit.Blobs) != 1 {
		t.Fatalf("new UID mappings missing: %+v, %v", newAudit, err)
	}
}

func TestCatchUpStateUpdateAdjustsLayeredViewsExactly(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 8, 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	delta := schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaSourceState,
		FSID:                1,
		Inode:               1,
		IdentityGeneration:  1,
		Revision:            1,
		State:               schema.AnalyticsExpired,
		ClassificationEpoch: 1,
	}
	putRecord(t, store, schema.AnalyticsDeltaKey(2, 0), delta)
	if _, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1}); err != nil {
		t.Fatal(err)
	}
	stats, err := UserStats(
		ctx,
		store,
		UserStatsOptions{UIDs: []uint32{7}, Residencies: []string{"live", "expired"}, GroupBy: "user"},
	)
	if err != nil || len(stats.Rows) != 1 || stats.Rows[0].Residency != "expired" || stats.Rows[0].Files != 1 ||
		stats.Rows[0].LogicalBytes != 100 {
		t.Fatalf("layered state views = %+v, %v", stats, err)
	}
	query, err := Execute(
		ctx,
		store,
		Query{UIDs: []uint32{7}, Residencies: []string{"expired"}, IncludeIncomplete: true, AllowStale: true},
	)
	if err != nil || query.Files != 1 || query.LogicalBytes != 100 {
		t.Fatalf("layered state query = %+v, %v", query, err)
	}
}

func TestDisabledCatchUpDoesNoWorkAndPurgeRemovesLifecycleState(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 8, 100, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/group/a", true)
	if _, err := Enable(ctx, store, Config{SegmentRows: 1}, false); err != nil {
		t.Fatal(err)
	}
	jobID, err := Start(ctx, store, Query{IncludeIncomplete: true})
	if err != nil {
		t.Fatal(err)
	}
	putRevision(t, store, 1, 2, 2, 9, 10, 200, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), "/svm/vol/group/b", true)
	store.failSegment = 2<<32 | 2
	if _, err := Rebuild(ctx, store, Config{SegmentRows: 1}, false); err == nil {
		t.Fatal("injected candidate failure was ignored")
	}
	if _, err := Disable(ctx, store, false, false); err != nil {
		t.Fatal(err)
	}
	delta := schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaCreation,
		FSID:                1,
		Inode:               2,
		IdentityGeneration:  2,
		Revision:            2,
		UID:                 9,
		GID:                 10,
		Known:               schema.KnownUID | schema.KnownGID | schema.KnownSize,
		CreatedAt:           2,
		LogicalSize:         200,
		CreationBasis:       schema.AnalyticsFirstSeen,
		State:               schema.AnalyticsLive,
		ClassificationEpoch: 1,
	}
	putRecord(t, store, schema.AnalyticsDeltaKey(2, 0), delta)
	if result, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1}); err != nil || result.Processed != 0 {
		t.Fatalf("disabled catch-up = %+v, %v", result, err)
	}
	if _, found, _ := store.Get(ctx, schema.AnalyticsDeltaKey(2, 0)); !found {
		t.Fatal("disabled catch-up consumed a delta")
	}
	if _, err := Purge(ctx, store, false); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{schema.AnalyticsBuildCheckpointKey(), schema.AnalyticsQueryJobKey(jobID)} {
		if _, found, _ := store.Get(ctx, key); found {
			t.Fatalf("purge retained lifecycle key %x", key)
		}
	}
	if prefixCount(store, schema.AnalyticsFactSegmentPrefix()) != 0 || prefixCount(store, []byte("ai:")) != 0 {
		t.Fatal("purge retained analytics candidates")
	}
}

func TestProvenDeletionArchiveOnlyForgetExpiresAndUpdatesChurn(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	created := time.Date(2025, time.January, 4, 12, 0, 0, 0, time.UTC)
	deleted := time.Date(2025, time.February, 5, 12, 0, 0, 0, time.UTC)
	revision := schema.InodeRevision{
		CTime:      created.UnixNano(),
		Size:       4096,
		UID:        42,
		GID:        7,
		Known:      schema.KnownCTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath,
		SourcePath: "/svm/vol/group/file",
		Freshness:  schema.FreshnessVerified,
	}
	putRecord(t, store, schema.InodeRevisionKey(1, 2, 1), revision)
	root := schema.DirectoryRevision{
		Known:      schema.KnownPath,
		SourcePath: "/",
		Freshness:  schema.FreshnessVerified,
		Children: []schema.DirectoryChild{
			{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: schema.InodeRevisionKey(1, 2, 1)},
		},
	}
	putRecord(t, store, schema.DirectoryRevisionKey(1, 99, 2), root)
	snapshotID := schema.ID{1}
	putRecord(
		t,
		store,
		schema.SnapshotKey(snapshotID),
		schema.SnapshotRecord{CommitSequence: 9, RootFSID: 1, RootInode: 99, RootRevision: 2},
	)
	scope := schema.ID{2}
	putRecord(
		t,
		store,
		schema.AuthoritativeCrawlProofKey(scope, 10),
		schema.AuthoritativeCrawlProofRecord{
			ScopeID:     scope,
			RootFSID:    1,
			RootInode:   99,
			StartFence:  1,
			EndCommit:   10,
			CompletedAt: deleted.UnixNano(),
			Complete:    true,
			DebtFree:    true,
		},
	)
	putRecord(
		t,
		store,
		schema.AuthoritativeSourceBindingKey(scope, 1, 2, 1),
		schema.AuthoritativeSourceBindingRecord{
			Generation:         1,
			Revision:           1,
			State:              schema.AuthoritativeSourceDeleted,
			Continuity:         schema.AnalyticsContinuityProven,
			LastObservedCommit: 10,
		},
	)

	if _, err := Enable(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	result, err := Execute(ctx, store, Query{Residencies: []string{"archive-only"}})
	if err != nil || result.Files != 1 {
		t.Fatalf("retained deleted generation = %+v, %v", result, err)
	}
	stats, err := UserStats(ctx, store, UserStatsOptions{GroupBy: "user"})
	if err != nil || stats.Explain.Source != "materialized-view" || len(stats.Rows) != 1 ||
		stats.Rows[0].Residency != "archive-only" {
		t.Fatalf("archive-only user stats = %+v, %v", stats, err)
	}
	churnKey := schema.UserChurnKey(
		42,
		schema.AnalyticsGranularityMonth,
		time.Date(2025, time.February, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
	)
	churnValue, found, err := store.Get(ctx, schema.AnalyticsDerivedKey(1, churnKey))
	if err != nil || !found {
		t.Fatalf("deletion churn: found=%t err=%v", found, err)
	}
	churn, err := schema.UnmarshalAnalyticsAggregateRecord(churnValue)
	if err != nil || churn.FilesDeleted != 1 || churn.BytesDeleted != 4096 {
		t.Fatalf("deletion churn = %#v, %v", churn, err)
	}

	delete(store.values, string(schema.SnapshotKey(snapshotID)))
	if _, err := Rebuild(ctx, store, Config{}, false); err != nil {
		t.Fatal(err)
	}
	result, err = Execute(ctx, store, Query{Residencies: []string{"expired"}})
	if err != nil || result.Files != 1 {
		t.Fatalf("forgotten deleted generation = %+v, %v", result, err)
	}
	if _, found, err := store.Get(ctx, schema.InodeRevisionKey(1, 2, 1)); err != nil || !found {
		t.Fatalf("logical expiry removed physical metadata: found=%t err=%v", found, err)
	}
}
