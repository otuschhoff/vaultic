package analytics

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type memoryStore struct {
	mu     sync.Mutex
	values map[string][]byte
}

func newMemoryStore() *memoryStore { return &memoryStore{values: map[string][]byte{}} }
func (store *memoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.values[string(key)]
	return append([]byte(nil), value...), ok, nil
}
func (store *memoryStore) ScanPrefix(_ context.Context, prefix, cursor []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0)
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), prefix) && (len(cursor) == 0 || bytes.Compare([]byte(key), cursor) >= 0) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	more := len(keys) > int(limit)
	if more {
		keys = keys[:limit]
	}
	result := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		result[index] = daemon.KeyValue{Key: []byte(key), Value: append([]byte(nil), store.values[key]...)}
	}
	return result, more, nil
}
func (store *memoryStore) WriteMutableBatch(_ context.Context, puts []daemon.Mutation, deletes [][]byte, _ bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, put := range puts {
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
	}
	for _, key := range deletes {
		delete(store.values, string(key))
	}
	return nil
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
	first := schema.InodeRevision{CTime: created, Size: 1000, UID: 10, GID: 20, Known: known, SourcePath: "/svm-a/volume-a/qtree/file", Freshness: schema.FreshnessVerified}
	later := first
	later.Size = 100000
	later.SourcePath = "/changed/path/file"
	archive := schema.InodeRevision{MTime: created, Size: 10_000, UID: 11, GID: 21, Known: schema.KnownMTime | schema.KnownSize | schema.KnownUID | schema.KnownGID | schema.KnownPath, SourcePath: "/svm-b/volume-b/other/file", Freshness: schema.FreshnessImported}
	putRecord(t, store, schema.InodeRevisionKey(1, 100, 1), first)
	putRecord(t, store, schema.InodeRevisionKey(1, 100, 2), later)
	putRecord(t, store, schema.InodeRevisionKey(1, 200, 3), archive)
	putRecord(t, store, schema.CurrentInodeKey(1, 100), schema.CurrentPointer{Revision: 2, RecordKey: schema.InodeRevisionKey(1, 100, 2)})
	config := Config{PathGroupPrefixes: []string{"/svm-a/volume-a/qtree"}, CacheAfter: 2, CacheTTLSeconds: 60}
	built, err := Enable(ctx, store, config, false)
	if err != nil {
		t.Fatal(err)
	}
	if built.Facts != 2 || !built.Enabled {
		t.Fatalf("unexpected build: %+v", built)
	}

	query := Query{Years: []int{2019}, Months: []int{12}, ISOYears: []int{2020}, Workweeks: []int{1}, SizeLog10: []int{3}, SVMs: []string{"svm-a"}, Volumes: []string{"volume-a"}, PathGroups: []string{"/svm-a/volume-a/qtree"}, Residencies: []string{"live"}, GroupBy: []string{"uid", "month", "workweek", "size-log10", "residency"}}
	result, err := Execute(ctx, store, query)
	if err != nil {
		t.Fatal(err)
	}
	if result.Files != 1 || result.LogicalBytes != 1000 || len(result.Groups) != 1 || result.Cached {
		t.Fatalf("unexpected result: %+v", result)
	}
	archiveResult, err := Execute(ctx, store, Query{Residencies: []string{"archive-only"}})
	if err != nil || archiveResult.Files != 1 || archiveResult.LogicalBytes != 10_000 {
		t.Fatalf("archive-only classification failed: %+v, %v", archiveResult, err)
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
	result, err = Execute(ctx, store, Query{SizeMax: &maximum})
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
	result, err := Execute(ctx, store, Query{GroupBy: []string{"uid", "gid", "year", "size-log10"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Files != 1 || result.UnknownCreationTime != 1 || result.Groups[0].Dimensions["uid"] != "unknown" || result.Groups[0].Dimensions["size-log10"] != "unknown" {
		t.Fatalf("unknown provenance was lost: %+v", result)
	}
	if result, err = Execute(ctx, store, Query{UIDs: []uint32{0}}); err != nil || result.Files != 0 {
		t.Fatalf("unknown UID matched UID zero: %+v, %v", result, err)
	}
	for _, query := range []Query{{Months: []int{13}}, {Workweeks: []int{0}}, {GroupBy: []string{"bogus"}}, {Residencies: []string{"deleted"}}, {SizeMin: uint64Pointer(10), SizeMax: uint64Pointer(10)}} {
		if _, err := Execute(ctx, store, query); err == nil {
			t.Fatalf("invalid query accepted: %+v", query)
		}
	}
	if err := (Config{PathGroupPrefixes: []string{"/a", "/a"}}).Validate(); err == nil {
		t.Fatal("duplicate path-group prefix accepted")
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }
