package legacyimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type memorySource struct {
	indexes   map[vaultic.ID][]byte
	snapshots map[vaultic.ID][]byte
	blobs     map[vaultic.ID][]byte
}

func (*memorySource) Connections() uint { return 1 }
func (source *memorySource) List(_ context.Context, fileType vaultic.FileType, fn func(vaultic.ID, int64) error) error {
	values := source.indexes
	if fileType == vaultic.SnapshotFile {
		values = source.snapshots
	} else if fileType != vaultic.IndexFile {
		return nil
	}
	for id, value := range values {
		if err := fn(id, int64(len(value))); err != nil {
			return err
		}
	}
	return nil
}
func (source *memorySource) LoadUnpacked(_ context.Context, fileType vaultic.FileType, id vaultic.ID) ([]byte, error) {
	values := source.indexes
	if fileType == vaultic.SnapshotFile {
		values = source.snapshots
	}
	value, found := values[id]
	if !found {
		return nil, errors.New("not found")
	}
	return append([]byte(nil), value...), nil
}
func (source *memorySource) LoadBlob(_ context.Context, handle vaultic.BlobHandle, _ []byte) ([]byte, error) {
	value, found := source.blobs[handle.ID]
	if !found {
		return nil, errors.New("blob not found")
	}
	return append([]byte(nil), value...), nil
}

type fixedStatter struct {
	size int64
	err  error
}

func (statter fixedStatter) Stat(context.Context, backend.Handle) (backend.FileInfo, error) {
	return backend.FileInfo{Size: statter.size}, statter.err
}

type memoryStore struct {
	values           map[string][]byte
	imports          []daemon.LegacyPackImport
	revisions        uint64
	revisionsWritten uint64
}

func newMemoryStore() *memoryStore { return &memoryStore{values: make(map[string][]byte)} }
func (store *memoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}
func (store *memoryStore) ImportLegacyPack(_ context.Context, imported daemon.LegacyPackImport) error {
	store.imports = append(store.imports, imported)
	return nil
}
func (store *memoryStore) Put(_ context.Context, key, value []byte, _ bool) error {
	store.values[string(key)] = append([]byte(nil), value...)
	return nil
}
func (store *memoryStore) AllocateRevision(context.Context) (uint64, error) {
	store.revisions++
	return store.revisions, nil
}
func (store *memoryStore) PublishRevisionBatch(_ context.Context, currentKey, revisionKey, value []byte, revision uint64, related []daemon.Mutation, _ [][]byte) error {
	pointer, err := (schema.CurrentPointer{Revision: revision, RecordKey: revisionKey}).MarshalBinary()
	if err != nil {
		return err
	}
	store.values[string(currentKey)] = pointer
	store.values[string(revisionKey)] = append([]byte(nil), value...)
	for _, mutation := range related {
		store.values[string(mutation.Key)] = append([]byte(nil), mutation.Value...)
	}
	store.revisionsWritten++
	return nil
}
func (store *memoryStore) PublishContentManifest(_ context.Context, ids []schema.ID, related []daemon.Mutation, _ [][]byte) (schema.ID, error) {
	for _, mutation := range related {
		store.values[string(mutation.Key)] = append([]byte(nil), mutation.Value...)
	}
	return schema.ContentManifestID(ids), nil
}

func encodedIndex(t *testing.T, packID, blobID vaultic.ID) []byte {
	t.Helper()
	idx := index.NewIndex()
	idx.StorePack(packID, pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, Offset: 3, Length: 10, UncompressedLength: 8}})
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestImportContinuesMalformedIndexesAndResumes(t *testing.T) {
	indexID1, indexID2 := vaultic.NewRandomID(), vaultic.NewRandomID()
	packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID()
	source := &memorySource{indexes: map[vaultic.ID][]byte{
		indexID1: encodedIndex(t, packID, blobID),
		indexID2: []byte(`{"packs":`),
	}}
	store := newMemoryStore()

	result, err := Import(context.Background(), source, fixedStatter{size: 16}, store, Options{Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.IndexesImported != 1 || result.ErrorsSeen != 1 || len(store.imports) != 1 {
		t.Fatalf("unexpected first import result: %#v, imports=%d", result, len(store.imports))
	}
	result, err = Import(context.Background(), source, fixedStatter{size: 16}, store, Options{Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.IndexesResumed != 1 || len(store.imports) != 1 {
		t.Fatalf("resume did not skip completed index: %#v, imports=%d", result, len(store.imports))
	}
}

func TestImportDryRunAndWorkBudget(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}}
	store := newMemoryStore()
	result, err := Import(context.Background(), source, fixedStatter{size: 16}, store, Options{DryRun: true})
	if err != nil || result.BlobsImported != 1 || len(store.imports) != 0 || len(store.values) != 0 {
		t.Fatalf("unexpected dry-run result: %#v, err=%v", result, err)
	}
	_, err = Import(context.Background(), source, fixedStatter{size: 16}, store, Options{WorkBudget: 0})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Import(context.Background(), source, fixedStatter{size: 16}, store, Options{WorkBudget: 1})
	if err != nil {
		t.Fatal(err)
	}
}

func TestImportRecordsUnavailablePackDebtAndHonorsMaxErrors(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}}
	store := newMemoryStore()
	result, err := Import(context.Background(), source, fixedStatter{err: errors.New("offline")}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if result.CrawlDebtCreated != 1 || result.ErrorsSeen != 1 || store.imports[0].Debt == nil {
		t.Fatalf("missing pack debt: %#v", result)
	}
	_, err = Import(context.Background(), source, fixedStatter{err: errors.New("offline")}, newMemoryStore(), Options{MaxErrors: 1})
	if !errors.Is(err, ErrLimitReached) {
		t.Fatalf("max errors returned %v", err)
	}
}

func TestImportRecordsKnownPackSmallerThanIndexedPayload(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}}
	store := newMemoryStore()
	result, err := Import(context.Background(), source, fixedStatter{size: 5}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if result.CrawlDebtCreated != 1 || result.ErrorsSeen != 1 || len(store.imports) != 1 {
		t.Fatalf("missing inconsistent-size debt: %#v", result)
	}
	record := store.imports[0].Record
	if !record.PhysicalSizeKnown || record.PhysicalSize != 5 || record.PayloadSize != 10 || record.HeaderSize != 0 || store.imports[0].Debt == nil {
		t.Fatalf("inconsistent pack metadata = %#v", record)
	}
	if _, err := record.MarshalBinary(); err != nil {
		t.Fatalf("known inconsistent pack was not representable: %v", err)
	}
}

func TestImportSnapshotsPreservesUnknownFactsAndResumes(t *testing.T) {
	contentID := vaultic.NewRandomID()
	childTree := treeJSON(t, &data.Node{
		Name: "file", Type: data.NodeTypeFile, DeviceID: 7, Inode: 11,
		Content: vaultic.IDs{contentID},
	})
	childTreeID := vaultic.Hash(childTree)
	rootTree := treeJSON(t, &data.Node{
		Name: "top", Type: data.NodeTypeDir, DeviceID: 7, Inode: 10, Subtree: &childTreeID,
	})
	rootTreeID := vaultic.Hash(rootTree)
	snapshotID := vaultic.NewRandomID()
	snapshotJSON, err := json.Marshal(data.Snapshot{Tree: &rootTreeID, Paths: []string{"/source"}})
	if err != nil {
		t.Fatal(err)
	}
	source := &memorySource{
		indexes: map[vaultic.ID][]byte{}, snapshots: map[vaultic.ID][]byte{snapshotID: snapshotJSON},
		blobs: map[vaultic.ID][]byte{rootTreeID: rootTree, childTreeID: childTree},
	}
	store := newMemoryStore()
	options := Options{Resume: true, SnapshotDepth: 1}
	result, err := Import(context.Background(), source, fixedStatter{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	if result.SnapshotsImported != 1 || result.NodesImported != 1 || result.CrawlDebtCreated < 3 || store.revisionsWritten != 1 {
		t.Fatalf("unexpected tree import result: %#v, revisions=%d", result, store.revisionsWritten)
	}
	current := schema.CurrentInodeKey(7, 11)
	pointerValue, found := store.values[string(current)]
	if !found {
		t.Fatal("known nested file was not imported")
	}
	pointer, err := schema.UnmarshalCurrentPointer(pointerValue)
	if err != nil {
		t.Fatal(err)
	}
	record, err := schema.UnmarshalInodeRevision(store.values[string(pointer.RecordKey)])
	if err != nil {
		t.Fatal(err)
	}
	if record.Freshness != schema.FreshnessImported || record.Known != schema.KnownParent|schema.KnownPath || record.ParentInode != 10 || len(record.ContentIDs) != 1 || record.ContentIDs[0] != schema.ID(contentID) {
		t.Fatalf("imported inode invented or lost facts: %#v", record)
	}
	if _, found := store.values[string(schema.ReverseInodeKey(schema.ID(contentID), 7, 11))]; !found {
		t.Fatal("reverse inode reference was not imported")
	}
	result, err = Import(context.Background(), source, fixedStatter{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	if result.SnapshotsResumed != 1 || store.revisionsWritten != 1 {
		t.Fatalf("snapshot resume replayed revisions: %#v, revisions=%d", result, store.revisionsWritten)
	}
}

func TestImportRealLegacyRepository(t *testing.T) {
	repo, unpacked, be := repository.TestRepositoryWithVersion(t, vaultic.StableRepoVersion)
	packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID()
	packData := []byte("0123456789abcdef")
	if err := be.Save(context.Background(), backend.Handle{Type: backend.PackFile, Name: packID.String()}, backend.NewByteReader(packData, be.Hasher())); err != nil {
		t.Fatal(err)
	}
	idx := index.NewIndex()
	idx.StorePack(packID, pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, Offset: 0, Length: 10, UncompressedLength: 8}})
	idx.Finalize()
	if _, err := idx.SaveIndex(context.Background(), unpacked); err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	result, err := Import(context.Background(), repo, be, store, Options{Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.IndexesImported != 1 || result.PacksImported != 1 || result.BlobsImported != 1 || result.CrawlDebtCreated != 0 || len(store.imports) != 1 {
		t.Fatalf("unexpected real repository import: %#v", result)
	}
	imported := store.imports[0]
	if imported.Record.PhysicalSize != uint64(len(packData)) || imported.Record.PayloadSize != 10 || imported.Record.HeaderSize != 6 || imported.Debt != nil {
		t.Fatalf("incorrect imported pack metadata: %#v", imported.Record)
	}
}

func TestImportClassifiesEmptyPackAsUnknown(t *testing.T) {
	indexID, packID := vaultic.NewRandomID(), vaultic.NewRandomID()
	source := &memorySource{indexes: map[vaultic.ID][]byte{
		indexID: fmt.Appendf(nil, `{"packs":[{"id":"%s","blobs":[]}]}`, packID.String()),
	}}
	store := newMemoryStore()
	result, err := Import(context.Background(), source, fixedStatter{size: 9}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if result.PacksImported != 1 || result.BlobsImported != 0 || len(store.imports) != 1 || store.imports[0].Record.Type != schema.PackUnknown || store.imports[0].Record.HeaderSize != 9 {
		t.Fatalf("empty pack import = %#v, result=%#v", store.imports, result)
	}
}

func TestImportClassifiesMixedPack(t *testing.T) {
	indexID, packID := vaultic.NewRandomID(), vaultic.NewRandomID()
	dataID, treeID := vaultic.NewRandomID(), vaultic.NewRandomID()
	idx := index.NewIndex()
	idx.StorePack(packID, pack.Blobs{
		{BlobHandle: vaultic.BlobHandle{ID: dataID, Type: vaultic.DataBlob}, Offset: 0, Length: 8},
		{BlobHandle: vaultic.BlobHandle{ID: treeID, Type: vaultic.TreeBlob}, Offset: 8, Length: 8},
	})
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}}
	store := newMemoryStore()
	result, err := Import(context.Background(), source, fixedStatter{size: 20}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if result.PacksImported != 1 || len(store.imports) != 1 || store.imports[0].Record.Type != schema.PackMixed || store.imports[0].Record.BlobCount != 2 {
		t.Fatalf("mixed pack import = %#v, result=%#v", store.imports, result)
	}
}

func TestSnapshotTraversalLimitsLeaveNoCheckpoint(t *testing.T) {
	fileTree := treeJSON(t, &data.Node{Name: "file", Type: data.NodeTypeFile, DeviceID: 7, Inode: 12})
	fileTreeID := vaultic.Hash(fileTree)
	nestedTree := treeJSON(t, &data.Node{Name: "nested", Type: data.NodeTypeDir, DeviceID: 7, Inode: 11, Subtree: &fileTreeID})
	nestedTreeID := vaultic.Hash(nestedTree)
	rootTree := treeJSON(t, &data.Node{Name: "top", Type: data.NodeTypeDir, DeviceID: 7, Inode: 10, Subtree: &nestedTreeID})
	rootTreeID := vaultic.Hash(rootTree)
	snapshotID := vaultic.NewRandomID()
	snapshotJSON, err := json.Marshal(data.Snapshot{Tree: &rootTreeID})
	if err != nil {
		t.Fatal(err)
	}
	source := &memorySource{
		indexes: map[vaultic.ID][]byte{}, snapshots: map[vaultic.ID][]byte{snapshotID: snapshotJSON},
		blobs: map[vaultic.ID][]byte{rootTreeID: rootTree, nestedTreeID: nestedTree, fileTreeID: fileTree},
	}
	depthStore := newMemoryStore()
	result, err := Import(context.Background(), source, fixedStatter{}, depthStore, Options{SnapshotDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.TreesVisited != 2 || result.NodesImported != 0 || result.CrawlDebtCreated == 0 {
		t.Fatalf("depth bound was not explicit: %#v", result)
	}
	workStore := newMemoryStore()
	_, err = Import(context.Background(), source, fixedStatter{}, workStore, Options{SnapshotWorkBudget: 1})
	if !errors.Is(err, ErrLimitReached) {
		t.Fatalf("work budget returned %v", err)
	}
	if _, found := workStore.values[string(schema.SnapshotImportCheckpointKey(schema.ID(snapshotID)))]; found {
		t.Fatal("partial traversal published a snapshot checkpoint")
	}
}

func TestLargeContentManifestUsesCanonicalReverseSegments(t *testing.T) {
	content := make(vaultic.IDs, schema.DefaultContentSegmentIDs+1)
	for index := range content {
		binary.BigEndian.PutUint32(content[index][28:], uint32(index+1))
	}
	childTree := treeJSON(t, &data.Node{Name: "large", Type: data.NodeTypeFile, DeviceID: 7, Inode: 11, Content: content})
	childTreeID := vaultic.Hash(childTree)
	rootTree := treeJSON(t, &data.Node{Name: "top", Type: data.NodeTypeDir, DeviceID: 7, Inode: 10, Subtree: &childTreeID})
	rootTreeID := vaultic.Hash(rootTree)
	snapshotID := vaultic.NewRandomID()
	snapshotJSON, err := json.Marshal(data.Snapshot{Tree: &rootTreeID})
	if err != nil {
		t.Fatal(err)
	}
	source := &memorySource{
		indexes: map[vaultic.ID][]byte{}, snapshots: map[vaultic.ID][]byte{snapshotID: snapshotJSON},
		blobs: map[vaultic.ID][]byte{rootTreeID: rootTree, childTreeID: childTree},
	}
	store := newMemoryStore()
	if _, err := Import(context.Background(), source, fixedStatter{}, store, Options{SnapshotDepth: 1}); err != nil {
		t.Fatal(err)
	}
	manifestID := schema.ContentManifestID(schemaIDs(content))
	reverseValue, found := store.values[string(schema.ReverseManifestKey(schema.ID(content[len(content)-1]), manifestID))]
	if !found {
		t.Fatal("last manifest reverse reference is missing")
	}
	reverse, err := schema.UnmarshalReverseManifestRecord(reverseValue)
	if err != nil || reverse.Segment != 1 || reverse.State != schema.ReferenceUnresolved {
		t.Fatalf("last manifest reverse reference = %#v, err=%v", reverse, err)
	}
	if _, found := store.values[string(schema.ReverseInodeKey(schema.ID(content[len(content)-1]), 7, 11))]; !found {
		t.Fatal("large manifest content is missing its reverse inode reference")
	}
}

func schemaIDs(ids vaultic.IDs) []schema.ID {
	result := make([]schema.ID, len(ids))
	for index, id := range ids {
		result[index] = schema.ID(id)
	}
	return result
}

func treeJSON(t *testing.T, nodes ...*data.Node) []byte {
	t.Helper()
	builder := data.NewTreeJSONBuilder()
	for _, node := range nodes {
		if err := builder.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := builder.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
