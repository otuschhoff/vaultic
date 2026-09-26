package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestDecodeDeferredObservationsStrict(t *testing.T) {
	want := DeferredObservation{SnapshotPath: "/snapshot/file", SourcePath: "/source/file", ParentPath: "/source"}
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	observations, err := DecodeDeferredObservations([]json.RawMessage{payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].SnapshotPath != want.SnapshotPath ||
		observations[0].SourcePath != want.SourcePath {
		t.Fatalf("unexpected decoded observations: %#v", observations)
	}

	for _, invalid := range []json.RawMessage{
		append(append(json.RawMessage(nil), payload[:len(payload)-1]...), []byte(`,"unknown":true}`)...),
		append(append(json.RawMessage(nil), payload...), []byte(` {}`)...),
	} {
		if _, err := DecodeDeferredObservations([]json.RawMessage{invalid}); err == nil {
			t.Fatalf("accepted invalid deferred observation %q", invalid)
		}
	}
}

func TestSnapshotRootIncludesMissingAncestors(t *testing.T) {
	store := newFakeStore()
	reconciler := &Reconciler{ctx: t.Context(), store: store}
	fileKey := schema.InodeRevisionKey(7, 11, 1)
	published := map[string]publishedItem{
		"/actual/file": {snapshotPath: "/volume/source/file", identity: identity{fsid: 7, inode: 11}, typeID: schema.NodeFile, key: fileKey},
	}
	if err := reconciler.publishSnapshotRoot(published); err != nil {
		t.Fatal(err)
	}
	key := reconciler.RootKey()
	for _, name := range []string{"volume", "source", "file"} {
		value, found, err := store.Get(t.Context(), key)
		if err != nil || !found {
			t.Fatalf("missing directory %q: %v", name, err)
		}
		record, err := schema.UnmarshalDirectoryRevision(value)
		if err != nil || len(record.Children) != 1 || record.Children[0].Name != name {
			t.Fatalf("directory %q children=%+v err=%v", name, record.Children, err)
		}
		key = record.Children[0].MetadataKey
	}
	if !bytes.Equal(key, fileKey) {
		t.Fatalf("synthetic ancestors changed leaf identity: %x", key)
	}
	if len(published) != 1 {
		t.Fatalf("synthetic ancestors changed source observations: %+v", published)
	}
}

func TestSnapshotAncestorsPreserveObservedDirectories(t *testing.T) {
	store := newFakeStore()
	reconciler := &Reconciler{ctx: t.Context(), store: store}
	directoryKey := schema.DirectoryRevisionKey(7, 10, 12)
	published := map[string]publishedItem{
		"/actual/source": {snapshotPath: "/volume/source", identity: identity{fsid: 7, inode: 10}, typeID: schema.NodeDirectory, key: directoryKey},
		"/actual/source/file": {
			snapshotPath: "/volume/source/file", identity: identity{fsid: 7, inode: 11}, typeID: schema.NodeFile,
			key: schema.InodeRevisionKey(7, 11, 13),
		},
		"/other": {
			snapshotPath: "/volume/other", identity: identity{fsid: 8, inode: 20}, typeID: schema.NodeFile,
			key: schema.InodeRevisionKey(8, 20, 14),
		},
	}
	roots, err := reconciler.publishSnapshotAncestors(published)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0].Name != "volume" || store.next != 1 {
		t.Fatalf("roots=%+v revisions=%d", roots, store.next)
	}
	value, _, err := store.Get(t.Context(), roots[0].MetadataKey)
	if err != nil {
		t.Fatal(err)
	}
	record, err := schema.UnmarshalDirectoryRevision(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Children) != 2 || record.Children[0].Name != "other" || record.Children[1].Name != "source" ||
		!bytes.Equal(record.Children[1].MetadataKey, directoryKey) {
		t.Fatalf("observed directory was lost or replaced: %+v", record.Children)
	}
	if len(published) != 3 {
		t.Fatal("source observations mutated")
	}
}

func TestSnapshotAncestorsRejectIncompleteObservedSubtree(t *testing.T) {
	store := newFakeStore()
	reconciler := &Reconciler{ctx: t.Context(), store: store}
	published := map[string]publishedItem{
		"/actual": {
			snapshotPath: "/source", identity: identity{fsid: 7, inode: 10}, typeID: schema.NodeDirectory,
			key: schema.DirectoryRevisionKey(7, 10, 12),
		},
		"/actual/gap/file": {
			snapshotPath: "/source/gap/file", identity: identity{fsid: 7, inode: 11}, typeID: schema.NodeFile,
			key: schema.InodeRevisionKey(7, 11, 13),
		},
	}
	if _, err := reconciler.publishSnapshotAncestors(published); err == nil || store.next != 0 {
		t.Fatalf("incomplete observed subtree accepted: revisions=%d err=%v", store.next, err)
	}
	for _, name := range []string{"..", "../file", "../../nested/file"} {
		if _, err := reconciler.publishSnapshotAncestors(map[string]publishedItem{
			"/actual": {snapshotPath: name},
		}); err == nil || store.next != 0 {
			t.Fatalf("escaping path %q accepted: revisions=%d err=%v", name, store.next, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reconciler.ctx = ctx
	if _, err := reconciler.publishSnapshotAncestors(published); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publication returned %v", err)
	}
}

type fakeFS struct {
	mu      sync.Mutex
	entries map[string]fs.ExtendedFileInfo
	parents map[string]string
	gate    <-chan struct{}
}

func (filesystem *fakeFS) Lstat(path string) (*fs.ExtendedFileInfo, error) {
	if filesystem.gate != nil {
		<-filesystem.gate
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	info, found := filesystem.entries[filepath.ToSlash(path)]
	if !found {
		return nil, os.ErrNotExist
	}
	return &info, nil
}

func (filesystem *fakeFS) Dir(path string) string {
	if parent, found := filesystem.parents[filepath.ToSlash(path)]; found {
		return parent
	}
	return pathpkg.Dir(filepath.ToSlash(path))
}

func TestAuthoritativeCrawlClaimIsExplicitAndFailsClosed(t *testing.T) {
	if claim := (&Reconciler{}).AuthoritativeCrawlClaim(); claim != nil {
		t.Fatalf("default reconciler produced authoritative claim: %#v", claim)
	}
	scope := AuthoritativeCrawlScope{
		ScopeID:    testSchemaID(240),
		RootFSID:   1,
		RootInode:  2,
		StartFence: 3,
		Complete:   true,
	}
	debtKey := schema.CrawlDebtKey(schema.ID{}, testSchemaID(241))
	reconciler := &Reconciler{
		options:   Options{Authoritative: &scope},
		crawlDebt: map[string][]byte{string(debtKey): debtKey},
	}
	claim := reconciler.AuthoritativeCrawlClaim()
	if claim == nil || !claim.Complete || len(claim.DebtKeys) != 1 || !bytes.Equal(claim.DebtKeys[0], debtKey) {
		t.Fatalf("complete authoritative claim = %#v", claim)
	}
	reconciler.deferred.Store(1)
	if claim := reconciler.AuthoritativeCrawlClaim(); claim == nil || claim.Complete {
		t.Fatalf("deferred reconciliation overclaimed completeness: %#v", claim)
	}
}

func TestReplayDeferredUsesFreshAuthoritativeRevisions(t *testing.T) {
	store, observations := deferredReplayFixture()
	root, err := ReplayDeferred(context.Background(), store, observations, Options{})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := schema.ParseKey(root)
	if err != nil || parsed.Kind != schema.KeyDirectoryRevision || len(store.published) != 1 || store.next != 2 {
		t.Fatalf(
			"root=%x parsed=%#v published=%d revisions=%d err=%v",
			root,
			parsed,
			len(store.published),
			store.next,
			err,
		)
	}
}

func deferredReplayFixture() (*fakeStore, []DeferredObservation) {
	store := newFakeStore()
	now := time.Now().UTC()
	rootStat := DeferredStat{
		Name:       "source",
		Mode:       os.ModeDir | 0o755,
		DeviceID:   7,
		Inode:      10,
		Links:      1,
		ModTime:    now,
		ChangeTime: now,
	}
	fileStat := DeferredStat{
		Name:       "file",
		Mode:       0o644,
		DeviceID:   7,
		Inode:      11,
		Links:      1,
		Size:       4,
		ModTime:    now,
		ChangeTime: now,
	}
	return store, []DeferredObservation{{
		SnapshotPath: "/file", SourcePath: "/source/file", ParentPath: "/source",
		Node: data.Node{Name: "file", Type: data.NodeTypeFile, Size: 4}, Stat: fileStat, ParentStat: rootStat,
	}}
}

func TestReplayDeferredRetryReusesSyntheticRoot(t *testing.T) {
	store, observations := deferredReplayFixture()
	first, err := ReplayDeferred(context.Background(), store, observations, Options{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ReplayDeferred(context.Background(), store, observations, Options{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("replay root changed from %x to %x", first, second)
	}
}

type fakeStore struct {
	mu        sync.Mutex
	values    map[string][]byte
	next      uint64
	published []daemon.ReconciledRevision
}

func newFakeStore() *fakeStore { return &fakeStore{values: make(map[string][]byte)} }

func (store *fakeStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *fakeStore) Put(_ context.Context, key, value []byte, _ bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.values[string(key)] = append([]byte(nil), value...)
	return nil
}

func (store *fakeStore) MultiGet(ctx context.Context, keys [][]byte) ([]daemon.KeyValue, []bool, error) {
	values := make([]daemon.KeyValue, len(keys))
	found := make([]bool, len(keys))
	for index, key := range keys {
		value, exists, err := store.Get(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		values[index] = daemon.KeyValue{Key: append([]byte(nil), key...), Value: value}
		found[index] = exists
	}
	return values, found, nil
}

func (store *fakeStore) ScanPrefix(
	_ context.Context,
	prefix, after []byte,
	pageSize uint32,
) ([]daemon.KeyValue, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0)
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), prefix) && bytes.Compare([]byte(key), after) > 0 {
			keys = append(keys, key)
		}
	}
	sortStrings(keys)
	done := len(keys) <= int(pageSize)
	if !done {
		keys = keys[:pageSize]
	}
	entries := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		entries[index] = daemon.KeyValue{Key: []byte(key), Value: append([]byte(nil), store.values[key]...)}
	}
	return entries, done, nil
}

func (store *fakeStore) AllocateRevision(context.Context) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.next++
	return store.next, nil
}

func (store *fakeStore) PublishRevision(
	_ context.Context,
	currentKey, revisionKey, revisionValue []byte,
	revision uint64,
) error {
	return store.PublishRevisionBatch(context.Background(), currentKey, revisionKey, revisionValue, revision, nil, nil)
}

func (store *fakeStore) PublishRevisionBatch(
	_ context.Context,
	currentKey, revisionKey, revisionValue []byte,
	revision uint64,
	relatedPuts []daemon.Mutation,
	relatedDeletes [][]byte,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	pointer, err := (schema.CurrentPointer{Revision: revision, RecordKey: revisionKey}).MarshalBinary()
	if err != nil {
		return err
	}
	store.values[string(currentKey)] = pointer
	store.values[string(revisionKey)] = append([]byte(nil), revisionValue...)
	for _, put := range relatedPuts {
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
	}
	for _, key := range relatedDeletes {
		delete(store.values, string(key))
	}
	return nil
}

func (store *fakeStore) PublishReconciledRevision(_ context.Context, request daemon.ReconciledRevision) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	pointer, err := (schema.CurrentPointer{Revision: request.Revision, RecordKey: request.RevisionKey}).MarshalBinary()
	if err != nil {
		return err
	}
	store.values[string(request.CurrentKey)] = pointer
	store.values[string(request.RevisionKey)] = append([]byte(nil), request.RevisionValue...)
	if len(request.ContentIDs) > schema.MaxInlineContentIDs {
		manifestID, segments, segmentErr := schema.SegmentContent(request.ContentIDs, schema.DefaultContentSegmentIDs)
		if segmentErr != nil {
			return segmentErr
		}
		for index, segment := range segments {
			encoded, encodeErr := segment.MarshalBinary()
			if encodeErr != nil {
				return encodeErr
			}
			store.values[string(schema.ContentManifestKey(manifestID, uint32(index)))] = encoded
		}
	}
	for _, key := range request.DebtKeys {
		value, found := store.values[string(key)]
		if !found {
			continue
		}
		debt, decodeErr := schema.UnmarshalCrawlDebtRecord(value)
		if decodeErr != nil {
			return decodeErr
		}
		debt.Status = schema.DebtResolved
		value, decodeErr = debt.MarshalBinary()
		if decodeErr != nil {
			return decodeErr
		}
		store.values[string(key)] = value
	}
	for _, put := range request.RelatedPuts {
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
	}
	store.published = append(store.published, request)
	return nil
}

func (store *fakeStore) RecordCrawlDebtFailure(_ context.Context, keys [][]byte, errorClass string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, key := range keys {
		value, found := store.values[string(key)]
		if !found {
			continue
		}
		debt, err := schema.UnmarshalCrawlDebtRecord(value)
		if err != nil {
			return err
		}
		debt.RetryCount++
		debt.ErrorClass = errorClass
		debt.LastAttemptUnixNano = time.Now().UnixNano()
		value, err = debt.MarshalBinary()
		if err != nil {
			return err
		}
		store.values[string(key)] = value
	}
	return nil
}

func (store *fakeStore) ResolveCrawlDebt(_ context.Context, keys [][]byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, key := range keys {
		value, found := store.values[string(key)]
		if !found {
			continue
		}
		debt, err := schema.UnmarshalCrawlDebtRecord(value)
		if err != nil {
			return err
		}
		debt.Status = schema.DebtResolved
		debt.ErrorClass = ""
		value, err = debt.MarshalBinary()
		if err != nil {
			return err
		}
		store.values[string(key)] = value
	}
	return nil
}

func TestImportedFileBecomesVerifiedAndThenReuses(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	filesystem := testFilesystem()
	content := []vaultic.ID{testVaulticID(1), testVaulticID(2)}
	imported := schema.InodeRevision{ParentInode: 10, Known: schema.KnownParent, Freshness: schema.FreshnessImported}
	seedCurrent(t, store, 1, 11, imported)
	debtKey := schema.CrawlDebtKey(testSchemaID(90), testSchemaID(91))
	store.values[string(debtKey)] = encode(
		t,
		schema.CrawlDebtRecord{
			PathOrTree: []byte("file"),
			Reason:     schema.DebtUnknownFreshness,
			Status:     schema.DebtPending,
		},
	)

	reconciler, err := New(ctx, filesystem, store, Options{Workers: 2, QueueDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	previous := &data.Node{Name: "file", Type: data.NodeTypeFile, Content: content}
	fileStat := filesystem.entries["/root/file"]
	if reconciler.CanReuse("/file", "/root/file", &fileStat, previous) {
		t.Fatal("imported inode incorrectly permitted content reuse")
	}
	reconciler.Observe("/file", "/root/file", &data.Node{Name: "file", Type: data.NodeTypeFile, Content: content})
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	if metrics := reconciler.Metrics(); metrics.Scanned != 1 || metrics.Reconciled != 1 || metrics.Reused != 0 ||
		metrics.Failed != 0 {
		t.Fatalf("first metrics = %#v", metrics)
	}
	record := currentInode(t, store, 1, 11)
	wantKnown := schema.KnownMTime | schema.KnownCTime | schema.KnownSize | schema.KnownMode
	wantKnown |= schema.KnownUID | schema.KnownGID | schema.KnownParent | schema.KnownPath
	if record.Freshness != schema.FreshnessVerified ||
		record.Known != wantKnown ||
		len(record.ContentIDs) != 2 {
		t.Fatalf("verified record = %#v", record)
	}
	debt, err := schema.UnmarshalCrawlDebtRecord(store.values[string(debtKey)])
	if err != nil || debt.Status != schema.DebtResolved {
		t.Fatalf("debt = %#v, err=%v", debt, err)
	}

	reconciler, err = New(ctx, filesystem, store, Options{Workers: 1, QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !reconciler.CanReuse("/file", "/root/file", &fileStat, previous) {
		t.Fatal("verified matching inode did not permit content reuse")
	}
	mismatchedStat := fileStat
	mismatchedStat.Size++
	if reconciler.CanReuse("/file", "/root/file", &mismatchedStat, previous) {
		t.Fatal("mismatched metadata incorrectly permitted content reuse")
	}
	reconciler.Observe("/file", "/root/file", &data.Node{Name: "file", Type: data.NodeTypeFile, Content: content})
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	if metrics := reconciler.Metrics(); metrics.Reused != 1 || metrics.Changed != 0 {
		t.Fatalf("reuse metrics = %#v", metrics)
	}
}

func TestReconcileWritesPathVersionForOptedInBindingChanges(t *testing.T) {
	store := newFakeStore()
	filesystem := testFilesystem()
	reconciler, err := New(
		context.Background(),
		filesystem,
		store,
		Options{Workers: 1, QueueDepth: 4, PathIndexPaths: []string{"/file"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/file", "/root/file", fileNode("file", 1))
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	pointer, err := schema.UnmarshalCurrentPointer(store.values[string(schema.CurrentInodeKey(1, 11))])
	if err != nil {
		t.Fatal(err)
	}
	key := schema.PathVersionKey(0, "file", pointer.Revision)
	value, found := store.values[string(key)]
	if !found {
		t.Fatalf("path-version key %x was not written", key)
	}
	binding, err := schema.UnmarshalPathVersionRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if binding.State != schema.PathBound || binding.Inode != 11 || binding.Revision != pointer.Revision {
		t.Fatalf("binding = %#v, pointer revision %d", binding, pointer.Revision)
	}

	filesystem.entries["/root/file"] = fileInfo(1, 11, 8, 2)
	reconciler, err = New(
		context.Background(),
		filesystem,
		store,
		Options{Workers: 1, QueueDepth: 4, PathIndexPaths: []string{"/file"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/file", "/root/file", fileNode("file", 2))
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	var pvCount int
	for key := range store.values {
		if strings.HasPrefix(key, "pv:") {
			pvCount++
		}
	}
	if pvCount != 1 {
		t.Fatalf("unchanged file wrote %d path-version records, want 1", pvCount)
	}
}

func TestReconcileWritesPathVersionTombstoneForDeletedIndexedPath(t *testing.T) {
	store := newFakeStore()
	filesystem := testFilesystem()
	reconciler, err := New(
		context.Background(),
		filesystem,
		store,
		Options{Workers: 1, QueueDepth: 4, PathIndexPaths: []string{"/file"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/file", "/root/file", fileNode("file", 1))
	reconciler.Observe("/", "/root", &data.Node{Name: "root", Type: data.NodeTypeDir})
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}

	delete(filesystem.entries, "/root/file")
	reconciler, err = New(
		context.Background(),
		filesystem,
		store,
		Options{Workers: 1, QueueDepth: 4, PathIndexPaths: []string{"/file"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/", "/root", &data.Node{Name: "root", Type: data.NodeTypeDir})
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	var tombstones int
	for key, value := range store.values {
		if !strings.HasPrefix(key, "pv:") {
			continue
		}
		record, err := schema.UnmarshalPathVersionRecord(value)
		if err != nil {
			t.Fatal(err)
		}
		if record.State == schema.PathTombstone {
			tombstones++
		}
	}
	if tombstones != 1 {
		t.Fatalf("tombstones = %d, want 1", tombstones)
	}
}

func TestMetadataAndContentChangesPreserveRevisions(t *testing.T) {
	store := newFakeStore()
	filesystem := testFilesystem()
	runFile(t, filesystem, store, testVaulticID(1))
	firstPointer, err := schema.UnmarshalCurrentPointer(store.values[string(schema.CurrentInodeKey(1, 11))])
	if err != nil {
		t.Fatal(err)
	}
	first := firstPointer.Revision
	filesystem.entries["/root/file"] = fileInfo(1, 11, 8, 2)
	runFile(t, filesystem, store, testVaulticID(1))
	metadataPointer, err := schema.UnmarshalCurrentPointer(store.values[string(schema.CurrentInodeKey(1, 11))])
	if err != nil {
		t.Fatal(err)
	}
	metadataRevision := metadataPointer.Revision
	if metadataRevision <= first {
		t.Fatal("metadata-only change did not allocate a revision")
	}
	runFile(t, filesystem, store, testVaulticID(2))
	contentPointer, err := schema.UnmarshalCurrentPointer(store.values[string(schema.CurrentInodeKey(1, 11))])
	if err != nil {
		t.Fatal(err)
	}
	if contentPointer.Revision <= metadataRevision {
		t.Fatal("content change did not allocate a revision")
	}
	if _, found := store.values[string(schema.InodeRevisionKey(1, 11, first))]; !found {
		t.Fatal("historical inode revision was removed")
	}
}

type countedParentFS struct {
	statFS
	calls  int
	cancel context.CancelFunc
}

type gatedPublicationStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
	failure error
	active  atomic.Int32
	peak    atomic.Int32
}

type blockAllocationStore struct {
	*fakeStore
	requests []uint64
	failure  error
}

func (store *blockAllocationStore) AllocateRevisionBlock(ctx context.Context, count uint64) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	store.requests = append(store.requests, count)
	if store.failure != nil {
		return 0, store.failure
	}
	start := store.next + 1
	store.next += count
	return start, nil
}

func publicationTestBatch(first, count int) []preparedItem {
	batch := make([]preparedItem, count)
	for index := range batch {
		name := fmt.Sprintf("file%d", first+index)
		inode := uint64(100 + first + index)
		batch[index] = preparedItem{
			workItem: workItem{sourcePath: "/root/" + name, snapshotPath: "/" + name, node: *fileNode(name, 1)},
			identity: identity{fsid: 1, inode: inode}, parent: identity{fsid: 1, inode: 10}, stat: fileInfo(1, inode, 1, 1),
		}
	}
	return batch
}

func TestPublicationRevisionBlocksAreLazyAndGroupScoped(t *testing.T) {
	store := &blockAllocationStore{fakeStore: newFakeStore()}
	reconciler := &Reconciler{ctx: context.Background(), filesystem: testFilesystem(), store: store}
	published := make(map[string]publishedItem)
	first := publicationTestBatch(0, publicationConcurrency)
	reconciler.publishInodes(first, published)
	reconciler.publishInodes(first, published)
	if len(store.requests) != 1 || store.next != publicationConcurrency || len(store.published) != publicationConcurrency {
		t.Fatalf("reuse allocated revisions: requests=%v next=%d published=%d", store.requests, store.next, len(store.published))
	}
	mixed := append(append([]preparedItem(nil), first[:3]...), publicationTestBatch(4, 1)...)
	reconciler.publishInodes(mixed, published)
	if store.next != 8 || store.published[4].Revision != 5 {
		t.Fatalf("partial group: next=%d revision=%d", store.next, store.published[4].Revision)
	}
	reconciler.publishInodes(publicationTestBatch(5, publicationConcurrency), published)
	if len(store.requests) != 3 || store.next != 12 || len(published) != 9 || len(reconciler.errors) != 0 {
		t.Fatalf("groups: requests=%v next=%d published=%d errors=%v", store.requests, store.next, len(published), reconciler.errors)
	}
	seen := make(map[uint64]bool)
	for _, item := range store.published {
		if seen[item.Revision] || item.Revision == 0 || (item.Revision >= 6 && item.Revision <= 8) {
			t.Fatalf("duplicate or carried-over revision %d", item.Revision)
		}
		seen[item.Revision] = true
	}
	for _, count := range store.requests {
		if count != publicationConcurrency {
			t.Fatalf("unbounded reservation: %d", count)
		}
	}
	metrics := reconciler.Metrics()
	if metrics.PublicationGroups != [publicationConcurrency]uint64{0, 0, 0, 4} ||
		metrics.RevisionAllocationCalls != 3 || metrics.RevisionAllocationFailures != 0 ||
		metrics.RevisionsReserved != 12 || metrics.InodeRevisionsAssigned != 9 ||
		metrics.InodePublicationCalls != 9 || metrics.InodePublicationFailures != 0 ||
		metrics.Reused != 7 || metrics.PublicationGroupNS == 0 || metrics.RevisionAllocationNS == 0 || metrics.InodePublicationNS == 0 {
		t.Fatalf("publication attribution: %+v", metrics)
	}
}

func TestPublicationMetricsEmptyPartialAndScalarGroups(t *testing.T) {
	for _, blocks := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocks=%t", blocks), func(t *testing.T) {
			var store Store = newFakeStore()
			if blocks {
				store = &blockAllocationStore{fakeStore: newFakeStore()}
			}
			reconciler := &Reconciler{ctx: context.Background(), filesystem: testFilesystem(), store: store}
			published := make(map[string]publishedItem)
			reconciler.publishInodes(nil, published)
			if metrics := reconciler.Metrics(); metrics != (Metrics{}) {
				t.Fatalf("empty group recorded work: %+v", metrics)
			}
			for count := 1; count <= publicationConcurrency; count++ {
				reconciler.publishInodes(publicationTestBatch(count*10, count), published)
			}
			calls := uint64(10)
			if blocks {
				calls = 4
			}
			metrics := reconciler.Metrics()
			if metrics.PublicationGroups != [publicationConcurrency]uint64{1, 1, 1, 1} ||
				metrics.RevisionAllocationCalls != calls || metrics.RevisionsReserved != 10 || metrics.InodeRevisionsAssigned != 10 ||
				metrics.InodePublicationCalls != 10 || metrics.InodePublicationFailures != 0 || metrics.RevisionAllocationFailures != 0 {
				t.Fatalf("publication attribution: %+v", metrics)
			}
		})
	}
}

func TestPublicationRevisionBlockFailureAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure := errors.New("reservation failed")
			store := &blockAllocationStore{fakeStore: newFakeStore(), failure: failure}
			if canceled {
				cancel()
				failure = context.Canceled
			}
			reconciler := &Reconciler{ctx: ctx, filesystem: testFilesystem(), store: store}
			published := make(map[string]publishedItem)
			reconciler.publishInodes(publicationTestBatch(0, publicationConcurrency), published)
			if len(published) != 0 || len(store.published) != 0 || store.next != 0 || len(reconciler.errors) != publicationConcurrency {
				t.Fatalf("failed reservation published work: published=%d next=%d errors=%v", len(published), store.next, reconciler.errors)
			}
			for _, err := range reconciler.errors {
				if !errors.Is(err, failure) {
					t.Fatalf("expected %v, got %v", failure, err)
				}
			}
			metrics := reconciler.Metrics()
			calls := uint64(publicationConcurrency)
			if canceled {
				calls = 0
			}
			if metrics.RevisionAllocationCalls != calls || metrics.RevisionAllocationFailures != calls ||
				metrics.RevisionsReserved != 0 || metrics.InodeRevisionsAssigned != 0 || metrics.InodePublicationCalls != 0 {
				t.Fatalf("failed allocation attribution: %+v", metrics)
			}
		})
	}
}

func (store *gatedPublicationStore) PublishReconciledRevision(ctx context.Context, revision daemon.ReconciledRevision) error {
	active := store.active.Add(1)
	defer store.active.Add(-1)
	for peak := store.peak.Load(); active > peak; peak = store.peak.Load() {
		if store.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	store.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-store.release:
		if store.failure != nil {
			return store.failure
		}
		return store.fakeStore.PublishReconciledRevision(ctx, revision)
	}
}

func TestIndependentPublicationsOverlapWithinBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &gatedPublicationStore{fakeStore: newFakeStore(), entered: make(chan struct{}, 8), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(store.release) }) }
	defer unblock()
	reconciler := &Reconciler{ctx: ctx, filesystem: testFilesystem(), store: store}
	batch := make([]preparedItem, 8)
	for index := range batch {
		name := fmt.Sprintf("file%d", index)
		inode := uint64(100 + index)
		batch[index] = preparedItem{
			workItem: workItem{sourcePath: "/root/" + name, snapshotPath: "/" + name, node: *fileNode(name, 1)},
			identity: identity{fsid: 1, inode: inode}, parent: identity{fsid: 1, inode: 10}, stat: fileInfo(1, inode, 1, 1),
		}
	}
	published := make(map[string]publishedItem)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var directories []preparedItem
		reconciler.processBatch(batch, &directories, make(map[identity][]preparedItem), published)
	}()
	for range publicationConcurrency {
		select {
		case <-store.entered:
		case <-ctx.Done():
			t.Fatal("independent publications did not overlap")
		}
	}
	unblock()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("publication did not finish")
	}
	if store.peak.Load() != publicationConcurrency || len(published) != len(batch) || len(reconciler.errors) != 0 {
		t.Fatalf("peak=%d published=%d errors=%v", store.peak.Load(), len(published), reconciler.errors)
	}
}

func TestPublicationsJoinOnCancellationAndFailure(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			failure := errors.New("publication failed")
			store := &gatedPublicationStore{
				fakeStore: newFakeStore(), entered: make(chan struct{}, publicationConcurrency),
				release: make(chan struct{}), failure: failure,
			}
			reconciler := &Reconciler{ctx: ctx, filesystem: testFilesystem(), store: store}
			batch := make([]preparedItem, publicationConcurrency)
			for index := range batch {
				name := fmt.Sprintf("file%d", index)
				inode := uint64(100 + index)
				batch[index] = preparedItem{
					workItem: workItem{sourcePath: "/root/" + name, snapshotPath: "/" + name, node: *fileNode(name, 1)},
					identity: identity{fsid: 1, inode: inode}, parent: identity{fsid: 1, inode: 10}, stat: fileInfo(1, inode, 1, 1),
				}
			}
			published := make(map[string]publishedItem)
			done := make(chan struct{})
			go func() {
				defer close(done)
				reconciler.publishInodes(batch, published)
			}()
			for range publicationConcurrency {
				select {
				case <-store.entered:
				case <-ctx.Done():
					t.Fatal("publications did not enter store")
				}
			}
			if canceled {
				cancel()
				failure = context.Canceled
			} else {
				close(store.release)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("publication workers did not finish")
			}
			if store.active.Load() != 0 || len(published) != 0 || len(reconciler.errors) != len(batch) {
				t.Fatalf("active=%d published=%d errors=%v", store.active.Load(), len(published), reconciler.errors)
			}
			metrics := reconciler.Metrics()
			if metrics.InodePublicationCalls != publicationConcurrency || metrics.InodePublicationFailures != publicationConcurrency ||
				metrics.InodePublicationNS == 0 || metrics.InodeRevisionsAssigned != publicationConcurrency {
				t.Fatalf("failed publication attribution: %+v", metrics)
			}
			for _, err := range reconciler.errors {
				if !errors.Is(err, failure) {
					t.Fatalf("expected %v, got %v", failure, err)
				}
			}
		})
	}
}

func TestPublicationConflictsPreserveIdentityAndPathOrder(t *testing.T) {
	first := preparedItem{identity: identity{fsid: 1, inode: 1}, workItem: workItem{sourcePath: "/source/a", snapshotPath: "/snapshot/a"}}
	independent := preparedItem{identity: identity{fsid: 1, inode: 2}, workItem: workItem{sourcePath: "/source/b", snapshotPath: "/snapshot/b"}}
	if publicationConflicts([]preparedItem{first}, independent) {
		t.Fatal("independent inode conflicts")
	}
	for _, conflict := range []preparedItem{
		{identity: first.identity, workItem: independent.workItem},
		{identity: independent.identity, workItem: workItem{sourcePath: first.sourcePath, snapshotPath: independent.snapshotPath}},
		{identity: independent.identity, workItem: workItem{sourcePath: independent.sourcePath, snapshotPath: "snapshot/a"}},
	} {
		if !publicationConflicts([]preparedItem{first}, conflict) {
			t.Fatalf("conflicting publication accepted: %+v", conflict)
		}
	}
}

func (filesystem *countedParentFS) Dir(sourcePath string) string {
	filesystem.calls++
	if filesystem.cancel != nil {
		filesystem.cancel()
	}
	return filesystem.statFS.Dir(sourcePath)
}

func TestPublishDirectoriesIndexesChildren(t *testing.T) {
	const directoryCount = 16
	const fileCount = 128
	store := newFakeStore()
	filesystem := &countedParentFS{statFS: testFilesystem()}
	reconciler := &Reconciler{ctx: context.Background(), filesystem: filesystem, store: store}
	published := make(map[string]publishedItem)
	directories := make([]preparedItem, 0, directoryCount+1)
	for index := 0; index < directoryCount; index++ {
		inode := uint64(index + 10)
		directories = append(directories, preparedItem{
			workItem: workItem{sourcePath: fmt.Sprintf("/source/d%02d", index), snapshotPath: fmt.Sprintf("/snapshot/renamed%02d", index)},
			identity: identity{fsid: 1, inode: inode}, parent: identity{fsid: 1, inode: 1}, stat: dirInfo(1, inode),
		})
	}
	directories = append(directories, preparedItem{
		workItem: workItem{sourcePath: "/source", snapshotPath: "/snapshot"},
		identity: identity{fsid: 1, inode: 1}, parent: identity{fsid: 1, inode: 2}, stat: dirInfo(1, 1),
	})
	for index := 0; index < fileCount; index++ {
		inode := uint64(index + 1000)
		published[fmt.Sprintf("/source/d%02d/f%03d", index%directoryCount, index)] = publishedItem{
			identity: identity{fsid: 1, inode: inode}, key: schema.InodeRevisionKey(1, inode, 1),
			typeID: nodeType(data.NodeTypeFile), snapshotPath: fmt.Sprintf("/snapshot/renamed%02d/file%03d", index%directoryCount, index),
		}
	}
	reconciler.publishDirectories(directories, published)
	if len(reconciler.errors) != 0 {
		t.Fatal(reconciler.errors)
	}
	if filesystem.calls > 2*(fileCount+len(directories)) {
		t.Fatalf("parent lookups are not linear: %d", filesystem.calls)
	}
	for index := 0; index < directoryCount; index++ {
		record, err := schema.UnmarshalDirectoryRevision(currentValue(t, store, schema.CurrentDirectoryKey(1, uint64(index+10))))
		if err != nil || len(record.Children) != fileCount/directoryCount {
			t.Fatalf("directory %d: children=%d err=%v", index, len(record.Children), err)
		}
		for _, child := range record.Children {
			if !strings.HasPrefix(child.Name, "file") {
				t.Fatalf("source basename leaked into snapshot: %q", child.Name)
			}
		}
	}
	root, err := schema.UnmarshalDirectoryRevision(currentValue(t, store, schema.CurrentDirectoryKey(1, 1)))
	if err != nil || len(root.Children) != directoryCount {
		t.Fatalf("root children=%d err=%v", len(root.Children), err)
	}
}

func TestPublishDirectoriesCancelsDuringParentIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	filesystem := &countedParentFS{statFS: testFilesystem(), cancel: cancel}
	reconciler := &Reconciler{ctx: ctx, filesystem: filesystem, store: newFakeStore()}
	published := map[string]publishedItem{"/root/one": {}, "/root/two": {}}
	reconciler.publishDirectories(nil, published)
	if filesystem.calls != 1 || len(reconciler.errors) != 1 || !errors.Is(reconciler.errors[0], context.Canceled) {
		t.Fatalf("calls=%d errors=%v", filesystem.calls, reconciler.errors)
	}
}

func TestDirectoryMoveDeletionAndHardlink(t *testing.T) {
	store := newFakeStore()
	filesystem := testFilesystem()
	filesystem.entries["/root/old"] = fileInfo(1, 12, 3, 1)
	first := runTree(t, filesystem, store, []observed{{"/old", "/root/old", fileNode("old", 3)}})
	if len(first.Children) != 1 || first.Children[0].Name != "old" {
		t.Fatalf("first directory = %#v", first)
	}
	delete(filesystem.entries, "/root/old")
	filesystem.entries["/root/new"] = fileInfo(1, 12, 3, 1)
	second := runTree(t, filesystem, store, []observed{{"/new", "/root/new", fileNode("new", 3)}})
	if len(second.Children) != 1 || second.Children[0].Name != "new" {
		t.Fatalf("moved directory = %#v", second)
	}

	filesystem.entries["/root/link-a"] = fileInfo(1, 20, 3, 2)
	filesystem.entries["/root/link-b"] = fileInfo(1, 20, 3, 2)
	before := len(store.published)
	runTree(t, filesystem, store, []observed{
		{"/link-a", "/root/link-a", fileNode("link-a", 4)},
		{"/link-b", "/root/link-b", fileNode("link-b", 4)},
	})
	inodeWrites := 0
	for _, request := range store.published[before:] {
		parsed, _ := schema.ParseKey(request.RevisionKey)
		if parsed.Kind == schema.KeyInodeRevision && parsed.Inode == 20 {
			inodeWrites++
			record, err := schema.UnmarshalInodeRevision(request.RevisionValue)
			if err != nil || record.Known&(schema.KnownParent|schema.KnownPath) != 0 {
				t.Fatalf("hardlink record = %#v, err=%v", record, err)
			}
		}
	}
	if inodeWrites != 1 {
		t.Fatalf("hardlink inode writes = %d", inodeWrites)
	}
}

func TestParentCycleIsRejected(t *testing.T) {
	store := newFakeStore()
	filesystem := testFilesystem()
	filesystem.entries["/root/a"] = dirInfo(1, 30)
	filesystem.entries["/root/b"] = dirInfo(1, 31)
	filesystem.parents = map[string]string{"/root/a": "/root/b", "/root/b": "/root/a"}
	reconciler, err := New(context.Background(), filesystem, store, Options{Workers: 2, QueueDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/a", "/root/a", &data.Node{Name: "a", Type: data.NodeTypeDir})
	reconciler.Observe("/b", "/root/b", &data.Node{Name: "b", Type: data.NodeTypeDir})
	if err := reconciler.Close(); err == nil {
		t.Fatal("parent cycle was accepted")
	}
	if len(store.published) != 0 {
		t.Fatal("cycle published directory revisions")
	}
}

func TestUnavailableIdentityIsDeferredAndFailedDebtIsRetried(t *testing.T) {
	store := newFakeStore()
	filesystem := testFilesystem()
	filesystem.entries["/root/file"] = fileInfo(0, 11, 8, 1)
	reconciler, err := New(context.Background(), filesystem, store, Options{Workers: 1, QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/file", "/root/file", fileNode("file", 1))
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	if metrics := reconciler.Metrics(); metrics.Deferred != 1 || metrics.Failed != 0 || metrics.Reconciled != 0 {
		t.Fatalf("deferred metrics = %#v", metrics)
	}
	if pendingDebtCount(store) != 1 {
		t.Fatalf("deferred identity debt count = %d", pendingDebtCount(store))
	}
	reconciler, err = New(context.Background(), filesystem, store, Options{Workers: 1, QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/file", "/root/file", fileNode("file", 1))
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	if debt := onlyPendingDebt(t, store); debt.RetryCount != 1 || debt.LastAttemptUnixNano == 0 {
		t.Fatalf("repeated deferred debt = %#v", debt)
	}

	filesystem = testFilesystem()
	delete(filesystem.entries, "/root/file")
	debtKey := schema.CrawlDebtKey(testSchemaID(92), testSchemaID(93))
	store.values[string(debtKey)] = encode(
		t,
		schema.CrawlDebtRecord{
			PathOrTree: []byte("file"),
			Reason:     schema.DebtUnknownFreshness,
			Status:     schema.DebtPending,
		},
	)
	reconciler, err = New(context.Background(), filesystem, store, Options{Workers: 1, QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/file", "/root/file", fileNode("file", 1))
	if err := reconciler.Close(); err == nil {
		t.Fatal("missing live file did not fail reconciliation")
	}
	debt, err := schema.UnmarshalCrawlDebtRecord(store.values[string(debtKey)])
	if err != nil || debt.Status != schema.DebtPending || debt.RetryCount != 1 || debt.ErrorClass != "not-found" ||
		debt.LastAttemptUnixNano == 0 {
		t.Fatalf("retried debt = %#v, err=%v", debt, err)
	}
}

func TestLargeManifestIsReused(t *testing.T) {
	store := newFakeStore()
	filesystem := testFilesystem()
	content := make(vaultic.IDs, schema.MaxInlineContentIDs+1)
	for index := range content {
		content[index] = testVaulticID(byte(index + 1))
	}
	runFile(t, filesystem, store, content...)
	manifestID := schema.ContentManifestID(toSchemaIDs(content))
	if _, found := store.values[string(schema.ContentManifestKey(manifestID, 0))]; !found {
		t.Fatal("large content manifest was not written")
	}
	delete(store.values, string(schema.ContentManifestKey(manifestID, 0)))
	reconciler, err := New(context.Background(), filesystem, store, Options{Workers: 1, QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	fileStat := filesystem.entries["/root/file"]
	if reconciler.CanReuse("/file", "/root/file", &fileStat, &data.Node{Type: data.NodeTypeFile, Content: content}) {
		t.Fatal("missing manifest segment incorrectly permitted content reuse")
	}
	reconciler.Observe("/file", "/root/file", &data.Node{Type: data.NodeTypeFile, Content: content})
	if err := reconciler.Close(); err != nil {
		t.Fatalf("manifest recovery failed: %v", err)
	}
	if _, found := store.values[string(schema.ContentManifestKey(manifestID, 0))]; !found {
		t.Fatal("missing manifest segment was not republished")
	}
	before := len(store.published)
	runFile(t, filesystem, store, content...)
	if len(store.published) != before {
		t.Fatal("verified manifest-backed content was republished")
	}
}

func TestQueueBackpressureAndDefaults(t *testing.T) {
	gate := make(chan struct{})
	filesystem := testFilesystem()
	filesystem.gate = gate
	store := newFakeStore()
	reconciler, err := New(context.Background(), filesystem, store, Options{Workers: 1, QueueDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if cap(reconciler.input) != 2 {
		t.Fatalf("queue capacity = %d", cap(reconciler.input))
	}
	done := make(chan struct{})
	go func() {
		for range 4 {
			reconciler.Observe("/file", "/root/file", fileNode("file", 1))
		}
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("producer bypassed bounded queue backpressure")
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("producer did not resume after queue drained")
	}
	_ = reconciler.Close()
	defaults, err := Options{}.withDefaults()
	if err != nil || defaults.Workers != 128 || defaults.QueueDepth != 50_000 || defaults.BatchSize != 5_000 {
		t.Fatalf("defaults = %#v, err=%v", defaults, err)
	}
}

func TestDefaultWorkerCountAppliesBackpressure(t *testing.T) {
	gate := make(chan struct{})
	filesystem := testFilesystem()
	filesystem.gate = gate
	reconciler, err := New(
		context.Background(),
		filesystem,
		newFakeStore(),
		Options{Workers: DefaultWorkers, QueueDepth: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for range DefaultWorkers + 2 {
			reconciler.Observe("/file", "/root/file", fileNode("file", 1))
		}
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("128-worker producer bypassed queue backpressure")
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("128-worker producer did not resume")
	}
	_ = reconciler.Close()
}

func TestDaemonBackedPublicationAttribution(t *testing.T) {
	for _, shared := range []bool{false, true} {
		for _, groupSize := range []int{1, publicationConcurrency} {
			t.Run(fmt.Sprintf("shared=%t/group=%d", shared, groupSize), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				dataDirectory := t.TempDir()
				if root := os.Getenv("VAULTICDB_TEST_DATA_ROOT"); root != "" {
					var err error
					dataDirectory, err = os.MkdirTemp(root, "publication-")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.RemoveAll(dataDirectory) })
				}
				socketDirectory, err := os.MkdirTemp("", "vaultic-pub-")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
				options := daemon.Options{
					Socket: filepath.Join(socketDirectory, "daemon.sock"), RepositoryID: "publication-attribution",
					DaemonPath: reconciliationDaemonBinary(t), DataDir: dataDirectory, WALFlushInterval: 100 * time.Millisecond,
				}
				client, err := daemon.Ensure(ctx, options)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close(context.Background())
				store := daemon.NewSchemaStore(client)
				reconciler := &Reconciler{ctx: ctx, filesystem: testFilesystem(), store: store}
				before, err := client.WriterStatus(ctx)
				if err != nil || before.EngineFlushIntervalMS != 100 {
					t.Fatalf("initial writer status=%+v err=%v", before, err)
				}
				const count = 32
				batch := publicationTestBatch(0, count)
				if !shared {
					for index := range batch {
						batch[index].node.Content = []vaultic.ID{testVaulticID(byte(index + 1))}
					}
				}
				published := make(map[string]publishedItem)
				started := time.Now()
				for offset := 0; offset < count; offset += groupSize {
					reconciler.publishInodes(batch[offset:offset+groupSize], published)
				}
				elapsed := time.Since(started)
				metrics := reconciler.Metrics()
				if len(reconciler.errors) != 0 || len(published) != count || metrics.RevisionAllocationCalls != uint64(count/groupSize) ||
					metrics.RevisionsReserved != count || metrics.InodeRevisionsAssigned != count || metrics.InodePublicationCalls != count ||
					metrics.InodePublicationFailures != 0 || metrics.RevisionAllocationFailures != 0 ||
					metrics.PublicationGroups[groupSize-1] != uint64(count/groupSize) {
					t.Fatalf("publication: metrics=%+v published=%d errors=%v", metrics, len(published), reconciler.errors)
				}
				after, err := client.WriterStatus(ctx)
				if err != nil || after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
					t.Fatalf("publication cleanup: writer=%+v err=%v", after, err)
				}
				attempts := after.Attribution.CommitRequest.Attempts - before.Attribution.CommitRequest.Attempts
				failures := after.Attribution.CommitRequest.Failures - before.Attribution.CommitRequest.Failures
				if attempts-failures != uint64(count+count/groupSize) {
					t.Fatalf("commit accounting: attempts=%d failures=%d", attempts, failures)
				}
				t.Logf("inodes=%d seconds=%.6f groups=%v allocation_calls=%d allocation_ns=%d publication_calls=%d publication_ns=%d "+
					"group_ns=%d commit_attempts=%d commit_failures=%d durable_us=%d wal_put_attempts=%d",
					count, elapsed.Seconds(), metrics.PublicationGroups, metrics.RevisionAllocationCalls, metrics.RevisionAllocationNS,
					metrics.InodePublicationCalls, metrics.InodePublicationNS, metrics.PublicationGroupNS, attempts, failures,
					after.Attribution.DurableWait.TotalUS-before.Attribution.DurableWait.TotalUS,
					after.Attribution.ObjectStoreWAL.Put.Timing.Attempts-before.Attribution.ObjectStoreWAL.Put.Timing.Attempts)
				for offset := 0; offset < count; offset += groupSize {
					reconciler.publishInodes(batch[offset:offset+groupSize], published)
				}
				if reused := reconciler.Metrics(); len(reconciler.errors) != 0 || reused.Reused != count ||
					reused.InodePublicationCalls != metrics.InodePublicationCalls || reused.RevisionAllocationCalls != metrics.RevisionAllocationCalls {
					t.Fatalf("reuse attribution: %+v errors=%v", reused, reconciler.errors)
				}
				if err := client.Close(ctx); err != nil {
					t.Fatal(err)
				}
				reopened, err := daemon.Ensure(ctx, options)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close(context.Background())
				store = daemon.NewSchemaStore(reopened)
				for _, item := range batch {
					value, found, err := store.Get(ctx, schema.CurrentInodeKey(item.identity.fsid, item.identity.inode))
					if err != nil || !found {
						t.Fatalf("reopened current inode: found=%t err=%v", found, err)
					}
					pointer, err := schema.UnmarshalCurrentPointer(value)
					if err != nil || !bytes.Equal(pointer.RecordKey, published[item.sourcePath].key) {
						t.Fatalf("reopened pointer=%+v err=%v", pointer, err)
					}
					value, found, err = store.Get(ctx, pointer.RecordKey)
					if err != nil || !found {
						t.Fatalf("reopened revision: found=%t err=%v", found, err)
					}
					record, err := schema.UnmarshalInodeRevision(value)
					if err != nil || len(record.ContentIDs) != 1 || record.ContentIDs[0] != schema.ID(item.node.Content[0]) {
						t.Fatalf("reopened content=%+v err=%v", record, err)
					}
				}
				if next, err := store.AllocateRevision(ctx); err != nil || next != count+1 {
					t.Fatalf("reopened next revision=%d err=%v", next, err)
				}
			})
		}
	}
}

func TestDaemonBackedPublicationRevisionBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	socketDirectory, err := os.MkdirTemp("", "vaultic-reconcile-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	client, err := daemon.Ensure(ctx, daemon.Options{
		Socket: filepath.Join(socketDirectory, "daemon.sock"), RepositoryID: "reconcile-blocks",
		DaemonPath: reconciliationDaemonBinary(t), DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := daemon.NewSchemaStore(client)
	reconciler := &Reconciler{ctx: ctx, filesystem: testFilesystem(), store: store}
	published := make(map[string]publishedItem)
	batch := publicationTestBatch(0, publicationConcurrency)
	reconciler.publishInodes(batch, published)
	reconciler.publishInodes(batch, published)
	if len(published) != publicationConcurrency || len(reconciler.errors) != 0 {
		t.Fatalf("published=%d errors=%v", len(published), reconciler.errors)
	}
	seen := make(map[uint64]bool)
	for _, item := range batch {
		value, found, err := store.Get(ctx, schema.CurrentInodeKey(item.identity.fsid, item.identity.inode))
		if err != nil || !found {
			t.Fatalf("current inode: found=%t err=%v", found, err)
		}
		pointer, err := schema.UnmarshalCurrentPointer(value)
		if err != nil || pointer.Revision == 0 || pointer.Revision > publicationConcurrency || seen[pointer.Revision] {
			t.Fatalf("invalid revision: pointer=%+v err=%v", pointer, err)
		}
		seen[pointer.Revision] = true
	}
	if next, err := store.AllocateRevision(ctx); err != nil || next != publicationConcurrency+1 {
		t.Fatalf("post-group revision=%d err=%v", next, err)
	}
	writer, err := client.WriterStatus(ctx)
	if err != nil || writer.ActiveTransactions != 0 || writer.ActiveWriteIntents != 0 {
		t.Fatalf("publication cleanup: writer=%+v err=%v", writer, err)
	}
}

func TestDaemonBackedPostImportReconciliation(t *testing.T) {
	binary := reconciliationDaemonBinary(t)
	socketDirectory, err := os.MkdirTemp("", "vaultic-reconcile-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: filepath.Join(socketDirectory, "daemon.sock"), RepositoryID: "phase5-post-import",
		DaemonPath: binary, DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := daemon.NewSchemaStore(client)
	root := t.TempDir()
	filename := filepath.Join(root, "file")
	if err := os.WriteFile(filename, []byte("phase5"), 0o640); err != nil {
		t.Fatal(err)
	}
	local := fs.NewLocal()
	fileInfo, err := local.Lstat(filename)
	if err != nil {
		t.Fatal(err)
	}
	parentInfo, err := local.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.DeviceID > math.MaxUint32 || parentInfo.DeviceID > math.MaxUint32 {
		t.Skip("filesystem identity does not fit the schema")
	}
	fsid, inode := uint32(fileInfo.DeviceID), fileInfo.Inode
	importRevision, err := store.AllocateRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	importKey := schema.InodeRevisionKey(fsid, inode, importRevision)
	importValue := encode(
		t,
		schema.InodeRevision{
			ParentInode: parentInfo.Inode,
			Known:       schema.KnownParent,
			Freshness:   schema.FreshnessImported,
		},
	)
	if err := store.PublishRevision(context.Background(), schema.CurrentInodeKey(fsid, inode), importKey, importValue, importRevision); err != nil {
		t.Fatal(err)
	}
	debtKey := schema.CrawlDebtKey(testSchemaID(94), testSchemaID(95))
	if err := store.Put(context.Background(), debtKey, encode(t, schema.CrawlDebtRecord{
		PathOrTree: []byte("file"), Reason: schema.DebtUnknownFreshness, Status: schema.DebtPending,
	}), true); err != nil {
		t.Fatal(err)
	}
	reconciler, err := New(context.Background(), local, store, Options{Workers: 2, QueueDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	content := []vaultic.ID{testVaulticID(7)}
	previous := &data.Node{Type: data.NodeTypeFile, Content: content}
	target := &archiver.Archiver{
		ReuseNode:     func(string, string, *fs.ExtendedFileInfo, *data.Node) bool { return true },
		ReconcileNode: func(string, string, *data.Node) {},
	}
	drain := Attach(target, reconciler)
	if target.ReuseNode("/file", filename, fileInfo, previous) {
		t.Fatal("daemon-backed imported inode permitted reuse")
	}
	target.ReconcileNode("/file", filename, &data.Node{Name: "file", Type: data.NodeTypeFile, Content: content})
	if err := drain(); err != nil {
		t.Fatal(err)
	}
	pointerValue, found, err := store.Get(context.Background(), schema.CurrentInodeKey(fsid, inode))
	if err != nil || !found {
		t.Fatalf("current inode: found=%t err=%v", found, err)
	}
	pointer, err := schema.UnmarshalCurrentPointer(pointerValue)
	if err != nil || pointer.Revision <= importRevision {
		t.Fatalf("reconciled pointer = %#v, err=%v", pointer, err)
	}
	verifiedValue, found, err := store.Get(context.Background(), pointer.RecordKey)
	if err != nil || !found {
		t.Fatalf("verified revision: found=%t err=%v", found, err)
	}
	verified, err := schema.UnmarshalInodeRevision(verifiedValue)
	if err != nil || verified.Freshness != schema.FreshnessVerified || verified.ParentInode != parentInfo.Inode ||
		len(verified.ContentIDs) != 1 {
		t.Fatalf("verified revision = %#v, err=%v", verified, err)
	}
	debtValue, found, err := store.Get(context.Background(), debtKey)
	if err != nil || !found {
		t.Fatalf("resolved debt: found=%t err=%v", found, err)
	}
	debt, err := schema.UnmarshalCrawlDebtRecord(debtValue)
	if err != nil || debt.Status != schema.DebtResolved {
		t.Fatalf("resolved debt = %#v, err=%v", debt, err)
	}
}

func reconciliationDaemonBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("VAULTICDB_TEST_BINARY"); binary != "" {
		return binary
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate reconciliation test source")
	}
	binary := filepath.Join(filepath.Dir(source), "..", "..", "..", "vaulticdb", "target", "debug", "vaulticdb")
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	return binary
}

type observed struct {
	snapshotPath string
	sourcePath   string
	node         *data.Node
}

func runTree(t *testing.T, filesystem *fakeFS, store *fakeStore, children []observed) schema.DirectoryRevision {
	t.Helper()
	reconciler, err := New(context.Background(), filesystem, store, Options{Workers: 4, QueueDepth: 8})
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		reconciler.Observe(child.snapshotPath, child.sourcePath, child.node)
	}
	reconciler.Observe("/", "/root", &data.Node{Name: "root", Type: data.NodeTypeDir})
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
	value := currentValue(t, store, schema.CurrentDirectoryKey(1, 10))
	record, err := schema.UnmarshalDirectoryRevision(value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func runFile(t *testing.T, filesystem *fakeFS, store *fakeStore, content ...vaultic.ID) {
	t.Helper()
	reconciler, err := New(context.Background(), filesystem, store, Options{Workers: 2, QueueDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Observe("/file", "/root/file", &data.Node{Name: "file", Type: data.NodeTypeFile, Content: content})
	if err := reconciler.Close(); err != nil {
		t.Fatal(err)
	}
}

func testFilesystem() *fakeFS {
	return &fakeFS{entries: map[string]fs.ExtendedFileInfo{
		"/":          dirInfo(1, 1),
		"/root":      dirInfo(1, 10),
		"/root/file": fileInfo(1, 11, 8, 1),
	}}
}

func fileInfo(device, inode uint64, size int64, links uint64) fs.ExtendedFileInfo {
	return fs.ExtendedFileInfo{
		DeviceID:   device,
		Inode:      inode,
		Size:       size,
		Links:      links,
		Mode:       0o644,
		UID:        3,
		GID:        4,
		ModTime:    time.Unix(5, 6),
		ChangeTime: time.Unix(7, 8),
	}
}

func dirInfo(device, inode uint64) fs.ExtendedFileInfo {
	return fs.ExtendedFileInfo{
		DeviceID:   device,
		Inode:      inode,
		Links:      1,
		Mode:       os.ModeDir | 0o755,
		ModTime:    time.Unix(5, 6),
		ChangeTime: time.Unix(7, 8),
	}
}

func fileNode(name string, id byte) *data.Node {
	return &data.Node{Name: name, Type: data.NodeTypeFile, Content: []vaultic.ID{testVaulticID(id)}}
}

func seedCurrent(t *testing.T, store *fakeStore, fsid uint32, inode uint64, record schema.InodeRevision) {
	t.Helper()
	store.next++
	key := schema.InodeRevisionKey(fsid, inode, store.next)
	store.values[string(key)] = encode(t, record)
	pointer, err := (schema.CurrentPointer{Revision: store.next, RecordKey: key}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(schema.CurrentInodeKey(fsid, inode))] = pointer
}

func currentInode(t *testing.T, store *fakeStore, fsid uint32, inode uint64) schema.InodeRevision {
	t.Helper()
	value := currentValue(t, store, schema.CurrentInodeKey(fsid, inode))
	record, err := schema.UnmarshalInodeRevision(value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func currentValue(t *testing.T, store *fakeStore, currentKey []byte) []byte {
	t.Helper()
	pointer, err := schema.UnmarshalCurrentPointer(store.values[string(currentKey)])
	if err != nil {
		t.Fatal(err)
	}
	value, found := store.values[string(pointer.RecordKey)]
	if !found {
		t.Fatalf("missing revision %q", pointer.RecordKey)
	}
	return value
}

func encode(t *testing.T, value interface{ MarshalBinary() ([]byte, error) }) []byte {
	t.Helper()
	encoded, err := value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testSchemaID(value byte) (id schema.ID)   { id[0] = value; return id }
func testVaulticID(value byte) (id vaultic.ID) { id[0] = value; return id }

func toSchemaIDs(ids []vaultic.ID) []schema.ID {
	result := make([]schema.ID, len(ids))
	for index, id := range ids {
		result[index] = schema.ID(id)
	}
	return result
}

func sortStrings(values []string) {
	for left := range values {
		for right := left + 1; right < len(values); right++ {
			if values[right] < values[left] {
				values[left], values[right] = values[right], values[left]
			}
		}
	}
}

func pendingDebtCount(store *fakeStore) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	count := 0
	for key, value := range store.values {
		if !bytes.HasPrefix([]byte(key), []byte("q:")) {
			continue
		}
		debt, err := schema.UnmarshalCrawlDebtRecord(value)
		if err == nil && debt.Status == schema.DebtPending {
			count++
		}
	}
	return count
}

func onlyPendingDebt(t *testing.T, store *fakeStore) schema.CrawlDebtRecord {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	var result schema.CrawlDebtRecord
	count := 0
	for key, value := range store.values {
		if !bytes.HasPrefix([]byte(key), []byte("q:")) {
			continue
		}
		debt, err := schema.UnmarshalCrawlDebtRecord(value)
		if err != nil {
			t.Fatal(err)
		}
		if debt.Status == schema.DebtPending {
			result = debt
			count++
		}
	}
	if count != 1 {
		t.Fatalf("pending debt count = %d", count)
	}
	return result
}

var _ Store = (*fakeStore)(nil)
var _ statFS = (*fakeFS)(nil)
var _ = fmt.Sprintf
