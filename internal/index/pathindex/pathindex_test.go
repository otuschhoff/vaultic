package pathindex

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type memoryStore struct{ values map[string][]byte }

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
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *memoryStore) MultiGet(ctx context.Context, keys [][]byte) ([]daemon.KeyValue, []bool, error) {
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
	entries := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		entries[index] = daemon.KeyValue{Key: []byte(key), Value: append([]byte(nil), store.values[key]...)}
	}
	return entries, done, nil
}

func (store *memoryStore) WriteMutableBatch(_ context.Context, puts []daemon.Mutation, deletes [][]byte, _ bool) error {
	for _, put := range puts {
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
	}
	for _, key := range deletes {
		delete(store.values, string(key))
	}
	return nil
}

func id(seed byte) schema.ID {
	var value schema.ID
	for index := range value {
		value[index] = seed
	}
	return value
}

func inode(size uint64) schema.InodeRevision {
	return schema.InodeRevision{Size: size, Known: schema.KnownSize, Freshness: schema.FreshnessVerified}
}

func dir(children ...schema.DirectoryChild) schema.DirectoryRevision {
	return schema.DirectoryRevision{Children: children, SourcePath: "/", Known: schema.KnownPath, Freshness: schema.FreshnessVerified}
}

func addSnapshot(t *testing.T, store *memoryStore, commit uint64, seed byte, rootKey []byte) {
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
			OriginalJSON:   []byte(`{"paths":["/"]}`),
		},
	)
	store.set(t, schema.SnapshotCommitKey(commit, snapshotID), schema.SnapshotCommitRecord{SnapshotTimeUnixNano: int64(commit), RootKey: rootKey})
}

func rootWithFile(t *testing.T, store *memoryStore, commit uint64, pathName string, inodeNumber, revision, size uint64) []byte {
	t.Helper()
	root := schema.DirectoryRevisionKey(0, 0, commit)
	file := schema.InodeRevisionKey(1, inodeNumber, revision)
	store.set(t, file, inode(size))
	store.set(t, root, dir(schema.DirectoryChild{Name: pathName, Inode: inodeNumber, Type: schema.NodeFile, MetadataKey: file}))
	return root
}

func TestPathIndexRebuildWritesOnlyBindingChanges(t *testing.T) {
	store := newMemoryStore()
	root1 := rootWithFile(t, store, 1, "a.txt", 10, 1, 10)
	root2 := rootWithFile(t, store, 2, "a.txt", 10, 1, 10)
	root3 := rootWithFile(t, store, 3, "a.txt", 10, 2, 20)
	addSnapshot(t, store, 1, 1, root1)
	addSnapshot(t, store, 2, 2, root2)
	addSnapshot(t, store, 3, 3, root3)

	result, err := Rebuild(context.Background(), store, []string{"a.txt"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.BindingsChanged != 2 {
		t.Fatalf("bindings changed = %d, want 2", result.BindingsChanged)
	}
	if _, found, _ := store.Get(context.Background(), schema.PathVersionKey(0, "a.txt", 2)); found {
		t.Fatal("unchanged binding wrote pv entry at commit 2")
	}
	if _, found, _ := store.Get(context.Background(), schema.PathVersionKey(0, "a.txt", 1)); !found {
		t.Fatal("missing initial binding")
	}
	if _, found, _ := store.Get(context.Background(), schema.PathVersionKey(0, "a.txt", 3)); !found {
		t.Fatal("missing changed binding")
	}
}

func TestPathIndexKeyOrderingAndBoundary(t *testing.T) {
	keys := [][]byte{
		schema.PathVersionKey(0, "a/b", 1),
		schema.PathVersionKey(0, "a/b", 2),
		schema.PathVersionKey(0, "a/b/c", 1),
		schema.PathVersionKey(0, "a/bc", 1),
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }) {
		t.Fatalf("test keys are not in expected lexical order")
	}
	prefix := schema.PathVersionPrefix(0, "a/b")
	if !bytes.HasPrefix(keys[0], prefix) || bytes.HasPrefix(keys[3], prefix) {
		t.Fatal("path terminator boundary collides with sibling path")
	}
}

func TestPathIndexOverflowPathIsMeasuredNotTruncated(t *testing.T) {
	store := newMemoryStore()
	root := rootWithFile(t, store, 1, "a.txt", 10, 1, 10)
	addSnapshot(t, store, 1, 1, root)
	long := strings.Repeat("x", schema.MaxPathIndexPathBytes+1)
	result, err := Rebuild(context.Background(), store, []string{long}, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.OverflowPaths != 1 || result.BindingsChanged != 1 {
		t.Fatalf("overflow result = %#v", result)
	}
	value, found, err := store.Get(context.Background(), schema.PathOverflowKey(0, long, 1))
	if err != nil || !found {
		t.Fatalf("overflow marker missing: found=%v err=%v", found, err)
	}
	record, err := schema.UnmarshalPathVersionRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != schema.PathOverflow || record.Path != long {
		t.Fatalf("overflow marker = %#v", record)
	}
}

func TestPathIndexRebuildDoesNotDeleteOtherPaths(t *testing.T) {
	store := newMemoryStore()
	root := rootWithFile(t, store, 1, "a.txt", 10, 1, 10)
	addSnapshot(t, store, 1, 1, root)
	otherValue, err := (schema.PathVersionRecord{State: schema.PathBound, NodeType: schema.NodeFile, Inode: 20, Revision: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	otherKey := schema.PathVersionKey(0, "other.txt", 1)
	store.values[string(otherKey)] = otherValue
	if _, err := Rebuild(context.Background(), store, []string{"a.txt"}, false); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.Get(context.Background(), otherKey); !found {
		t.Fatal("rebuilding a.txt deleted other.txt pv binding")
	}
}

func TestPathIndexPrunesForgottenCommits(t *testing.T) {
	store := newMemoryStore()
	for commit := uint64(1); commit <= 3; commit++ {
		value, err := (schema.PathVersionRecord{State: schema.PathBound, NodeType: schema.NodeFile, Inode: 10, Revision: commit}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		store.values[string(schema.PathVersionKey(0, "a.txt", commit))] = value
	}
	changed, err := PruneBefore(context.Background(), store, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Fatalf("dry-run prune = %d, want 2", changed)
	}
	if _, found, _ := store.Get(context.Background(), schema.PathVersionKey(0, "a.txt", 1)); !found {
		t.Fatal("dry-run deleted a pv row")
	}
	changed, err = PruneBefore(context.Background(), store, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Fatalf("prune = %d, want 2", changed)
	}
	if _, found, _ := store.Get(context.Background(), schema.PathVersionKey(0, "a.txt", 1)); found {
		t.Fatal("old pv row survived prune")
	}
	if _, found, _ := store.Get(context.Background(), schema.PathVersionKey(0, "a.txt", 3)); !found {
		t.Fatal("retained pv row was pruned")
	}
}
