package history

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func goldenJSON(t *testing.T, name string, value any) {
	t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join("testdata", name+".json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (set UPDATE_GOLDEN=1 to create it): %v", path, err)
	}
	if string(expected) != string(encoded) {
		t.Fatalf("golden %s mismatch:\nwant:\n%s\ngot:\n%s", path, expected, encoded)
	}
}

type memoryStore struct {
	values       map[string][]byte
	currentReads int
	multiGets    int
}

func newMemoryStore() *memoryStore { return &memoryStore{values: make(map[string][]byte)} }

func (store *memoryStore) set(t *testing.T, key []byte, record interface{ MarshalBinary() ([]byte, error) }) {
	t.Helper()
	value, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(key)] = value
}

func (store *memoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	if len(key) >= 2 && (string(key[:2]) == "i:" || string(key[:2]) == "d:") {
		store.currentReads++
	}
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *memoryStore) MultiGet(ctx context.Context, keys [][]byte) ([]daemon.KeyValue, []bool, error) {
	store.multiGets++
	values := make([]daemon.KeyValue, len(keys))
	found := make([]bool, len(keys))
	for index, key := range keys {
		value, ok, err := store.Get(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		values[index], found[index] = daemon.KeyValue{Key: key, Value: value}, ok
	}
	return values, found, nil
}

func (store *memoryStore) ScanPrefix(_ context.Context, prefix, after []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	keys := make([]string, 0)
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), prefix) && (len(after) == 0 || key > string(after)) {
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

func id(seed byte) schema.ID {
	var value schema.ID
	for index := range value {
		value[index] = seed
	}
	return value
}

func snapshotJSON(paths ...string) []byte {
	encoded, _ := json.Marshal(struct {
		Time  string   `json:"time"`
		Paths []string `json:"paths"`
	}{Time: "2026-08-29T12:00:00Z", Paths: paths})
	return encoded
}

func addSnapshot(t *testing.T, store *memoryStore, commit uint64, seed byte, rootKey []byte, paths ...string) string {
	t.Helper()
	snapshotID := id(seed)
	root, err := schema.ParseKey(rootKey)
	if err != nil {
		t.Fatal(err)
	}
	store.set(
		t,
		schema.SnapshotKey(snapshotID),
		schema.SnapshotRecord{
			CommitSequence: commit,
			RootFSID:       root.FSID,
			RootInode:      root.Inode,
			RootRevision:   root.Revision,
			OriginalJSON:   snapshotJSON(paths...),
		},
	)
	store.set(t, schema.SnapshotCommitKey(commit, snapshotID), schema.SnapshotCommitRecord{SnapshotTimeUnixNano: int64(commit), RootKey: rootKey})
	return vaultic.ID(snapshotID).String()
}

func inode(size uint64, source string) schema.InodeRevision {
	return schema.InodeRevision{
		Size:       size,
		MTime:      int64(size),
		CTime:      int64(size + 1),
		Known:      schema.KnownSize | schema.KnownMTime | schema.KnownCTime | schema.KnownPath,
		SourcePath: source,
		Freshness:  schema.FreshnessVerified,
	}
}

func dir(children ...schema.DirectoryChild) schema.DirectoryRevision {
	return schema.DirectoryRevision{Children: children, SourcePath: "/", Known: schema.KnownPath, Freshness: schema.FreshnessVerified}
}

func TestFileHistoryClassifiesRenameDeleteRecreateAndDirectoryReplacement(t *testing.T) {
	store := newMemoryStore()
	file1v1 := schema.InodeRevisionKey(1, 10, 1)
	file1v2 := schema.InodeRevisionKey(1, 10, 2)
	file2v1 := schema.InodeRevisionKey(1, 20, 1)
	dirReplacement := schema.DirectoryRevisionKey(1, 30, 1)
	store.set(t, file1v1, inode(10, "a.txt"))
	store.set(t, file1v2, inode(20, "b.txt"))
	store.set(t, file2v1, inode(30, "a.txt"))
	store.set(t, dirReplacement, dir(schema.DirectoryChild{Name: "child", Inode: 40, Type: schema.NodeFile, MetadataKey: schema.InodeRevisionKey(1, 40, 1)}))
	store.set(t, schema.InodeRevisionKey(1, 40, 1), inode(1, "a.txt/child"))

	roots := [][]byte{
		schema.DirectoryRevisionKey(0, 0, 1), schema.DirectoryRevisionKey(0, 0, 2), schema.DirectoryRevisionKey(0, 0, 3),
		schema.DirectoryRevisionKey(0, 0, 4), schema.DirectoryRevisionKey(0, 0, 5), schema.DirectoryRevisionKey(0, 0, 6),
	}
	store.set(t, roots[0], dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file1v1}))
	store.set(t, roots[1], dir(schema.DirectoryChild{Name: "b.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file1v2}))
	store.set(t, roots[2], dir())
	store.set(t, roots[3], dir(schema.DirectoryChild{Name: "a.txt", Inode: 20, Type: schema.NodeFile, MetadataKey: file2v1}))
	store.set(t, roots[4], dir(schema.DirectoryChild{Name: "a.txt", Inode: 30, Type: schema.NodeDirectory, MetadataKey: dirReplacement}))
	store.set(t, roots[5], dir())
	for index, root := range roots {
		addSnapshot(t, store, uint64(index+1), byte(index+1), root, "/")
	}

	result, err := FileHistory(context.Background(), store, "a.txt", Options{Snapshots: true})
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, len(result.Changes))
	for index, change := range result.Changes {
		kinds[index] = change.Kind
	}
	want := []string{"created", "deleted", "created", "rebound", "deleted"}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("changes = %#v, want %v", result.Changes, want)
	}
	if result.Changes[0].Inode != 10 || result.Changes[2].Inode != 20 || result.Changes[3].NodeType != "directory" {
		t.Fatalf("unexpected bindings: %#v", result.Changes)
	}
	if store.currentReads != 0 {
		t.Fatalf("resolver read current pointers %d times", store.currentReads)
	}
	if result.Metrics.BindingChanges != uint64(len(want)) || result.Metrics.AveragePathComponents == 0 {
		t.Fatalf("metrics not recorded: %#v", result.Metrics)
	}
}

func TestPathOutsideSnapshotScopeIsNotDeletion(t *testing.T) {
	store := newMemoryStore()
	root1 := schema.DirectoryRevisionKey(0, 0, 1)
	root2 := schema.DirectoryRevisionKey(0, 0, 2)
	file := schema.InodeRevisionKey(1, 10, 1)
	store.set(t, file, inode(10, "a.txt"))
	store.set(t, root1, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file}))
	store.set(t, root2, dir())
	addSnapshot(t, store, 1, 11, root1, "/a.txt")
	addSnapshot(t, store, 2, 12, root2, "/elsewhere")

	result, err := FileHistory(context.Background(), store, "a.txt", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Changes) != 2 || result.Changes[1].Kind != "not-covered" || result.Changes[1].Covered {
		t.Fatalf("out-of-scope path was not reported as not-covered: %#v", result.Changes)
	}
}

func TestPathAtUsesImmutableSnapshotNotCurrentPointer(t *testing.T) {
	store := newMemoryStore()
	oldRoot := schema.DirectoryRevisionKey(0, 0, 1)
	newRoot := schema.DirectoryRevisionKey(0, 0, 2)
	oldFile := schema.InodeRevisionKey(1, 10, 1)
	newFile := schema.InodeRevisionKey(1, 20, 1)
	store.set(t, oldFile, inode(10, "a.txt"))
	store.set(t, newFile, inode(20, "a.txt"))
	store.set(t, oldRoot, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: oldFile}))
	store.set(t, newRoot, dir(schema.DirectoryChild{Name: "a.txt", Inode: 20, Type: schema.NodeFile, MetadataKey: newFile}))
	snapshot := addSnapshot(t, store, 1, 21, oldRoot, "/")
	addSnapshot(t, store, 2, 22, newRoot, "/")
	store.set(t, schema.CurrentDirectoryKey(0, 0), schema.CurrentPointer{Revision: 2, RecordKey: newRoot})
	store.set(t, schema.CurrentInodeKey(1, 10), schema.CurrentPointer{Revision: 1, RecordKey: oldFile})

	result, err := PathAt(context.Background(), store, "a.txt", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Present || result.Inode != 10 || result.Revision != 1 {
		t.Fatalf("path-at resolved through current state: %#v", result)
	}
	if store.currentReads != 0 {
		t.Fatalf("resolver read current pointers %d times", store.currentReads)
	}
}

func TestPathAtReportsEveryHardlinkParent(t *testing.T) {
	store := newMemoryStore()
	root := schema.DirectoryRevisionKey(0, 0, 1)
	file := schema.InodeRevisionKey(1, 10, 1)
	store.set(t, file, schema.InodeRevision{HasMultipleParents: true, Size: 10, Known: schema.KnownSize, Freshness: schema.FreshnessVerified})
	store.set(t, schema.HardlinkRefsKey(1, 10, 1), schema.HardlinkRefsRecord{
		FSID: 1, Inode: 10, Revision: 1, Freshness: schema.FreshnessVerified,
		Parents: []schema.HardlinkParentRef{{ParentInode: 2, Name: "a.txt"}, {ParentInode: 3, Name: "also-a.txt"}},
	})
	store.set(t, root, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file}))
	snapshot := addSnapshot(t, store, 1, 51, root, "/")

	result, err := PathAt(context.Background(), store, "a.txt", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"2/a.txt", "3/also-a.txt"}
	if fmt.Sprint(result.Hardlinks) != fmt.Sprint(want) {
		t.Fatalf("hardlinks = %#v, want %#v", result.Hardlinks, want)
	}
}

func TestMemoizationDoesNotChangeResults(t *testing.T) {
	store := newMemoryStore()
	sharedDir := schema.DirectoryRevisionKey(1, 5, 1)
	file := schema.InodeRevisionKey(1, 10, 1)
	store.set(t, file, inode(10, "dir/a.txt"))
	store.set(t, sharedDir, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file}))
	for commit := uint64(1); commit <= 3; commit++ {
		root := schema.DirectoryRevisionKey(0, 0, commit)
		store.set(t, root, dir(schema.DirectoryChild{Name: "dir", Inode: 5, Type: schema.NodeDirectory, MetadataKey: sharedDir}))
		addSnapshot(t, store, commit, byte(30+commit), root, "/")
	}
	result, err := FileHistory(context.Background(), store, "dir/a.txt", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Changes) != 1 || result.Changes[0].Kind != "created" {
		t.Fatalf("memoized result changed semantics: %#v", result.Changes)
	}
	if result.Metrics.DirectoryCacheHits == 0 || store.multiGets == 0 {
		t.Fatalf("memoization/batching not exercised: metrics=%#v multiGets=%d", result.Metrics, store.multiGets)
	}
}

func TestMissingChildRevisionFailsClosed(t *testing.T) {
	store := newMemoryStore()
	root := schema.DirectoryRevisionKey(0, 0, 1)
	missing := schema.DirectoryRevisionKey(1, 10, 1)
	store.set(t, root, dir(schema.DirectoryChild{Name: "dir", Inode: 10, Type: schema.NodeDirectory, MetadataKey: missing}))
	addSnapshot(t, store, 1, 41, root, "/")
	if _, err := FileHistory(context.Background(), store, "dir/file.txt", Options{}); err == nil {
		t.Fatal("missing child revision was treated as absent instead of failing closed")
	}
}

func TestInodeHistoryScansRevisionPrefixInOrder(t *testing.T) {
	store := newMemoryStore()
	store.set(t, schema.InodeRevisionKey(7, 9, 1), inode(10, "a"))
	store.set(t, schema.InodeRevisionKey(7, 9, 2), inode(20, "a"))
	store.set(t, schema.InodeRevisionKey(7, 10, 1), inode(30, "other"))
	result, err := InodeHistory(context.Background(), store, 7, 9, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Revisions) != 1 || result.Revisions[0].Revision != 2 || result.Revisions[0].Size != 20 {
		t.Fatalf("inode history = %#v", result)
	}
}

func TestFileHistoryDistinguishesContentAndMetadataChanges(t *testing.T) {
	store := newMemoryStore()
	root1 := schema.DirectoryRevisionKey(0, 0, 1)
	root2 := schema.DirectoryRevisionKey(0, 0, 2)
	root3 := schema.DirectoryRevisionKey(0, 0, 3)
	contentA, contentB := id(91), id(92)
	file1 := schema.InodeRevisionKey(1, 10, 1)
	file2 := schema.InodeRevisionKey(1, 10, 2)
	file3 := schema.InodeRevisionKey(1, 10, 3)
	store.set(
		t,
		file1,
		schema.InodeRevision{
			Size:         10,
			Known:        schema.KnownSize,
			ContentMode:  schema.ContentInline,
			ContentIDs:   []schema.ID{contentA},
			ContentCount: 1,
			Freshness:    schema.FreshnessVerified,
		},
	)
	store.set(
		t,
		file2,
		schema.InodeRevision{
			Size:         20,
			Known:        schema.KnownSize,
			ContentMode:  schema.ContentInline,
			ContentIDs:   []schema.ID{contentA},
			ContentCount: 1,
			Freshness:    schema.FreshnessVerified,
		},
	)
	store.set(
		t,
		file3,
		schema.InodeRevision{
			Size:         30,
			Known:        schema.KnownSize,
			ContentMode:  schema.ContentInline,
			ContentIDs:   []schema.ID{contentB},
			ContentCount: 1,
			Freshness:    schema.FreshnessVerified,
		},
	)
	store.set(t, root1, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file1}))
	store.set(t, root2, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file2}))
	store.set(t, root3, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file3}))
	for commit, root := range [][]byte{root1, root2, root3} {
		addSnapshot(t, store, uint64(commit+1), byte(70+commit), root, "/")
	}
	result, err := FileHistory(context.Background(), store, "a.txt", Options{Content: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Changes) != 3 {
		t.Fatalf("changes = %#v", result.Changes)
	}
	if !result.Changes[1].MetadataOnly || result.Changes[1].ContentChanged {
		t.Fatalf("metadata-only change misclassified: %#v", result.Changes[1])
	}
	if !result.Changes[2].ContentChanged || result.Changes[2].MetadataOnly {
		t.Fatalf("content change misclassified: %#v", result.Changes[2])
	}
}

func TestGoldenJSONOutputs(t *testing.T) {
	store := newMemoryStore()
	root := schema.DirectoryRevisionKey(0, 0, 1)
	file := schema.InodeRevisionKey(1, 10, 1)
	store.set(t, file, inode(10, "a.txt"))
	store.set(t, root, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file}))
	snapshot := addSnapshot(t, store, 1, 61, root, "/")

	fileHistory, err := FileHistory(context.Background(), store, "a.txt", Options{Snapshots: true})
	if err != nil {
		t.Fatal(err)
	}
	goldenJSON(t, "file_history", fileHistory)

	pathAt, err := PathAt(context.Background(), store, "a.txt", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	goldenJSON(t, "path_at", pathAt)

	inodeHistory, err := InodeHistory(context.Background(), store, 1, 10, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	goldenJSON(t, "inode_history", inodeHistory)
}

func TestFileHistoryFromPathIndexFallsBackAndVerifiesMembership(t *testing.T) {
	store := newMemoryStore()
	root := schema.DirectoryRevisionKey(0, 0, 1)
	file := schema.InodeRevisionKey(1, 10, 1)
	store.set(t, file, inode(10, "a.txt"))
	store.set(t, root, dir(schema.DirectoryChild{Name: "a.txt", Inode: 10, Type: schema.NodeFile, MetadataKey: file}))
	addSnapshot(t, store, 1, 91, root, "/")

	if _, ok, err := FileHistoryFromPathIndex(context.Background(), store, "a.txt", Options{}); err != nil || ok {
		t.Fatalf("empty pv index returned ok=%v err=%v", ok, err)
	}
	value, err := (schema.PathVersionRecord{State: schema.PathBound, NodeType: schema.NodeFile, Inode: 10, Revision: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(schema.PathVersionKey(0, "a.txt", 1))] = value
	indexed, ok, err := FileHistoryFromPathIndex(context.Background(), store, "a.txt", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || indexed.Source != "path-index" || len(indexed.Changes) != 1 || !indexed.Changes[0].Present {
		t.Fatalf("indexed file history = %#v ok=%v", indexed, ok)
	}

	badValue, err := (schema.PathVersionRecord{State: schema.PathBound, NodeType: schema.NodeFile, Inode: 99, Revision: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(schema.PathVersionKey(0, "a.txt", 1))] = badValue
	indexed, ok, err = FileHistoryFromPathIndex(context.Background(), store, "a.txt", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(indexed.Changes) != 1 || indexed.Changes[0].Present {
		t.Fatalf("pv membership verification failed: %#v ok=%v", indexed, ok)
	}
}
