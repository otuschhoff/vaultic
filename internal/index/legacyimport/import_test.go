package legacyimport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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

type blockingStatter struct {
	entered chan time.Duration
	release chan struct{}
}

func (statter *blockingStatter) Stat(ctx context.Context, _ backend.Handle) (backend.FileInfo, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return backend.FileInfo{}, errors.New("pack preparation context has no deadline")
	}
	select {
	case statter.entered <- time.Until(deadline):
	case <-ctx.Done():
		return backend.FileInfo{}, ctx.Err()
	}
	select {
	case <-statter.release:
		return backend.FileInfo{Size: 2}, nil
	case <-ctx.Done():
		return backend.FileInfo{}, ctx.Err()
	}
}

func (statter fixedStatter) Stat(context.Context, backend.Handle) (backend.FileInfo, error) {
	return backend.FileInfo{Size: statter.size}, statter.err
}

type memoryStore struct {
	mu                sync.Mutex
	values            map[string][]byte
	imports           []daemon.LegacyPackImport
	batchSizes        []int
	checkpointBatches []int
	revisions         uint64
	revisionsWritten  uint64
}

func newMemoryStore() *memoryStore { return &memoryStore{values: make(map[string][]byte)} }
func (store *memoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}
func (store *memoryStore) ImportLegacyPacks(
	_ context.Context,
	imports []daemon.LegacyPackImport,
	checkpoint *daemon.Mutation,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.imports = append(store.imports, imports...)
	store.batchSizes = append(store.batchSizes, len(imports))
	if checkpoint != nil {
		store.checkpointBatches = append(store.checkpointBatches, len(store.batchSizes)-1)
		store.values[string(checkpoint.Key)] = append([]byte(nil), checkpoint.Value...)
	}
	return nil
}
func (store *memoryStore) Put(_ context.Context, key, value []byte, _ bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.values[string(key)] = append([]byte(nil), value...)
	return nil
}
func (store *memoryStore) AllocateRevision(context.Context) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.revisions++
	return store.revisions, nil
}

func (store *memoryStore) PublishRevisionBatch(
	_ context.Context,
	currentKey, revisionKey, value []byte,
	revision uint64,
	related []daemon.Mutation,
	_ [][]byte,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
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

func (store *memoryStore) PublishContentManifest(
	_ context.Context,
	ids []schema.ID,
	related []daemon.Mutation,
	_ [][]byte,
) (schema.ID, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, mutation := range related {
		store.values[string(mutation.Key)] = append([]byte(nil), mutation.Value...)
	}
	return schema.ContentManifestID(ids), nil
}

type blockingImportStore struct {
	*memoryStore
	entered chan struct{}
	release chan struct{}
}

func (store *blockingImportStore) ImportLegacyPacks(
	ctx context.Context,
	imports []daemon.LegacyPackImport,
	checkpoint *daemon.Mutation,
) error {
	select {
	case store.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-store.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return store.memoryStore.ImportLegacyPacks(ctx, imports, checkpoint)
}

type failingImportStore struct {
	*memoryStore
	calls int
}

type inputFailingImportStore struct{ *memoryStore }

func (store *inputFailingImportStore) ImportLegacyPacks(
	context.Context,
	[]daemon.LegacyPackImport,
	*daemon.Mutation,
) error {
	return &daemon.LegacyImportInputError{Index: 1, Err: errors.New("injected second-input failure")}
}

type sizeLimitedImportStore struct {
	*memoryStore
	maxPacks int
}

type failBatchStore struct {
	*memoryStore
	calls  int
	failAt int
}

func (store *failBatchStore) ImportLegacyPacks(
	ctx context.Context,
	imports []daemon.LegacyPackImport,
	checkpoint *daemon.Mutation,
) error {
	store.calls++
	if store.calls == store.failAt {
		return errors.New("injected batch failure")
	}
	return store.memoryStore.ImportLegacyPacks(ctx, imports, checkpoint)
}

func (store *sizeLimitedImportStore) ImportLegacyPacks(
	ctx context.Context,
	imports []daemon.LegacyPackImport,
	checkpoint *daemon.Mutation,
) error {
	if len(imports) > store.maxPacks {
		return fmt.Errorf("injected actual-size undercount: %w", daemon.ErrLegacyImportBatchTooLarge)
	}
	return store.memoryStore.ImportLegacyPacks(ctx, imports, checkpoint)
}

func (store *failingImportStore) ImportLegacyPacks(
	context.Context,
	[]daemon.LegacyPackImport,
	*daemon.Mutation,
) error {
	store.calls++
	return errors.New("injected pack import failure")
}

type deadlineImportStore struct {
	*memoryStore
	remaining chan time.Duration
}

type splitStore struct {
	*memoryStore
	mu sync.Mutex

	entered            chan uint64
	blockByEnter       map[uint64]chan struct{}
	blockByBatch       map[uint64]chan struct{}
	ingestCalls        uint64
	failByEnter        map[uint64]error
	failIngest         map[uint64]error
	failAnyIngest      error
	failReduce         map[uint64]error
	maxIngestPacks     int
	batchFingerprints  map[uint64][32]byte
	failedPacksByBatch map[uint64]schema.ID

	ingestedImports  map[uint64][]daemon.LegacyPackImport
	ingestedOrder    []uint64
	reducedOrder     []uint64
	reduceCheckpoint []bool

	activeIngest uint64
	peakIngest   uint64

	completeCalls uint64
}

func newSplitStore() *splitStore {
	return &splitStore{
		memoryStore:        newMemoryStore(),
		entered:            make(chan uint64, 64),
		blockByEnter:       make(map[uint64]chan struct{}),
		blockByBatch:       make(map[uint64]chan struct{}),
		failByEnter:        make(map[uint64]error),
		failIngest:         make(map[uint64]error),
		failReduce:         make(map[uint64]error),
		ingestedImports:    make(map[uint64][]daemon.LegacyPackImport),
		batchFingerprints:  make(map[uint64][32]byte),
		failedPacksByBatch: make(map[uint64]schema.ID),
	}
}

func (store *splitStore) IngestLegacyPacks(
	ctx context.Context,
	_ schema.ID,
	batch uint64,
	imports []daemon.LegacyPackImport,
) error {
	store.mu.Lock()
	store.activeIngest++
	store.ingestCalls++
	if store.activeIngest > store.peakIngest {
		store.peakIngest = store.activeIngest
	}
	blockEnter := store.blockByEnter[store.ingestCalls]
	block := store.blockByBatch[batch]
	fail := store.failIngest[batch]
	if fail == nil {
		fail = store.failByEnter[store.ingestCalls]
	}
	if store.failAnyIngest != nil {
		fail = store.failAnyIngest
	}
	store.ingestedOrder = append(store.ingestedOrder, batch)
	store.entered <- batch
	store.mu.Unlock()
	defer func() {
		store.mu.Lock()
		store.activeIngest--
		store.mu.Unlock()
	}()
	if blockEnter != nil {
		select {
		case <-blockEnter:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail != nil {
		store.mu.Lock()
		if len(imports) > 0 {
			store.failedPacksByBatch[batch] = imports[0].PackID
		}
		store.mu.Unlock()
		return fail
	}
	if store.maxIngestPacks > 0 && len(imports) > store.maxIngestPacks {
		return daemon.ErrLegacyImportBatchTooLarge
	}
	fingerprint := stage3TestBatchFingerprint(imports)
	store.mu.Lock()
	if existing, found := store.batchFingerprints[batch]; found && existing != fingerprint {
		store.mu.Unlock()
		return daemon.ErrIdempotencyConflict
	}
	store.batchFingerprints[batch] = fingerprint
	store.ingestedImports[batch] = append([]daemon.LegacyPackImport(nil), imports...)
	store.mu.Unlock()
	return nil
}

func (store *splitStore) ReduceLegacyImportBatch(
	ctx context.Context,
	_ schema.ID,
	batch uint64,
	checkpoint *daemon.Mutation,
) error {
	store.mu.Lock()
	fail := store.failReduce[batch]
	imports := store.ingestedImports[batch]
	store.reducedOrder = append(store.reducedOrder, batch)
	store.reduceCheckpoint = append(store.reduceCheckpoint, checkpoint != nil)
	store.mu.Unlock()
	if fail != nil {
		return fail
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(imports) == 0 {
		return daemon.ErrLegacyImportReceiptMissing
	}
	return store.memoryStore.ImportLegacyPacks(ctx, imports, checkpoint)
}

func (store *splitStore) CompleteLegacyImportSession(_ context.Context, _ schema.ID) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.completeCalls++
	return nil
}

func stage3TestBatchFingerprint(imports []daemon.LegacyPackImport) [32]byte {
	hasher := sha256.New()
	var framed [8]byte
	binary.BigEndian.PutUint64(framed[:], uint64(len(imports)))
	_, _ = hasher.Write(framed[:])
	for _, imported := range imports {
		_, _ = hasher.Write(imported.SourceIndex[:])
		_, _ = hasher.Write(imported.PackID[:])
		recordValue, _ := imported.Record.MarshalBinary()
		binary.BigEndian.PutUint64(framed[:], uint64(len(recordValue)))
		_, _ = hasher.Write(framed[:])
		_, _ = hasher.Write(recordValue)
		blobIDs := make([]schema.ID, 0, len(imported.Blobs))
		for blobID := range imported.Blobs {
			blobIDs = append(blobIDs, blobID)
		}
		sort.Slice(blobIDs, func(left, right int) bool { return bytes.Compare(blobIDs[left][:], blobIDs[right][:]) < 0 })
		binary.BigEndian.PutUint64(framed[:], uint64(len(blobIDs)))
		_, _ = hasher.Write(framed[:])
		for _, blobID := range blobIDs {
			_, _ = hasher.Write(blobID[:])
			blobValue, _ := imported.Blobs[blobID].MarshalBinary()
			binary.BigEndian.PutUint64(framed[:], uint64(len(blobValue)))
			_, _ = hasher.Write(framed[:])
			_, _ = hasher.Write(blobValue)
		}
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

func (store *deadlineImportStore) ImportLegacyPacks(
	ctx context.Context,
	imports []daemon.LegacyPackImport,
	checkpoint *daemon.Mutation,
) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("pack import context has no deadline")
	}
	store.remaining <- time.Until(deadline)
	return store.memoryStore.ImportLegacyPacks(ctx, imports, checkpoint)
}

func encodedIndex(t *testing.T, packID, blobID vaultic.ID) []byte {
	t.Helper()
	idx := index.NewIndex()
	idx.StorePack(
		packID,
		pack.Blobs{
			{
				BlobHandle:         vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob},
				Offset:             3,
				Length:             10,
				UncompressedLength: 8,
			},
		},
	)
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func encodedIndexWithPacks(t *testing.T, packIDs []vaultic.ID) []byte {
	t.Helper()
	idx := index.NewIndex()
	for _, packID := range packIDs {
		idx.StorePack(
			packID,
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestImportStage3IndependentIngestsOverlapAndReduceInOrder(t *testing.T) {
	indexID := vaultic.NewRandomID()
	packIDs := []vaultic.ID{vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()}
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndexWithPacks(t, packIDs)}}
	store := newSplitStore()
	gate := make(chan struct{})
	store.blockByEnter[1] = gate
	store.blockByEnter[2] = gate

	done := make(chan struct{})
	go func() {
		_, err := Import(
			context.Background(),
			source,
			fixedStatter{size: 16},
			store,
			Options{PublicationLanes: 2, PacksPerTransaction: 1},
		)
		if err != nil {
			t.Errorf("stage 3 import failed: %v", err)
		}
		close(done)
	}()

	<-store.entered
	<-store.entered
	close(gate)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stage 3 import did not complete")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.peakIngest < 2 {
		t.Fatalf("peak concurrent stage 3 ingests = %d, want >=2", store.peakIngest)
	}
	if len(store.reducedOrder) != 4 {
		t.Fatalf("reduced batches = %d, want 4", len(store.reducedOrder))
	}
	reducedSeen := make(map[uint64]struct{}, len(store.reducedOrder))
	for _, batch := range store.reducedOrder {
		reducedSeen[batch] = struct{}{}
	}
	if len(reducedSeen) != 4 {
		t.Fatalf("reduced order has duplicates: %v", store.reducedOrder)
	}
	if !slices.Equal(store.reduceCheckpoint, []bool{false, false, false, true}) {
		t.Fatalf("checkpoint flags = %v", store.reduceCheckpoint)
	}
}

func TestImportStage3OverlappingDependenciesDoNotOverlap(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	sharedBlob := vaultic.NewRandomID()
	for range 2 {
		idx.StorePack(
			vaultic.NewRandomID(),
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: sharedBlob, Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	store := newSplitStore()
	firstBlock := make(chan struct{})
	store.blockByEnter[1] = firstBlock

	done := make(chan error, 1)
	go func() {
		_, err := Import(
			context.Background(),
			&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
			fixedStatter{size: 16},
			store,
			Options{PublicationLanes: 2, PacksPerTransaction: 1},
		)
		done <- err
	}()

	<-store.entered
	select {
	case <-store.entered:
		t.Fatal("overlapping stage 3 batches were ingested concurrently")
	case <-time.After(100 * time.Millisecond):
	}
	close(firstBlock)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.peakIngest != 1 {
		t.Fatalf("peak overlapping stage 3 ingests = %d, want 1", store.peakIngest)
	}
}

func TestImportStage3BlockedBatchPreventsDependencyInversion(t *testing.T) {
	indexID := vaultic.NewRandomID()
	leftBlob := vaultic.NewRandomID()
	rightBlob := vaultic.NewRandomID()
	idx := index.NewIndex()
	idx.StorePack(vaultic.ID{1}, pack.Blobs{
		{BlobHandle: vaultic.BlobHandle{ID: leftBlob, Type: vaultic.DataBlob}, Length: 1},
	})
	idx.StorePack(vaultic.ID{2}, pack.Blobs{
		{BlobHandle: vaultic.BlobHandle{ID: leftBlob, Type: vaultic.DataBlob}, Length: 1},
		{BlobHandle: vaultic.BlobHandle{ID: rightBlob, Type: vaultic.DataBlob}, Length: 1},
	})
	idx.StorePack(vaultic.ID{3}, pack.Blobs{
		{BlobHandle: vaultic.BlobHandle{ID: rightBlob, Type: vaultic.DataBlob}, Length: 1},
	})
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	store := newSplitStore()
	firstBlock := make(chan struct{})
	store.blockByEnter[1] = firstBlock
	done := make(chan error, 1)
	go func() {
		_, err := Import(
			context.Background(),
			&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
			fixedStatter{size: 16},
			store,
			Options{PublicationLanes: 2, PacksPerTransaction: 1},
		)
		done <- err
	}()

	<-store.entered
	select {
	case <-store.entered:
		t.Fatal("later batch bypassed an earlier dependency waiter")
	case <-time.After(100 * time.Millisecond):
	}
	close(firstBlock)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stage 3 import deadlocked after dependency inversion")
	}
}

func TestImportStage3IngestFailureCancelsAndStopsReduction(t *testing.T) {
	indexID := vaultic.NewRandomID()
	packIDs := []vaultic.ID{vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()}
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndexWithPacks(t, packIDs)}}
	store := newSplitStore()
	store.failAnyIngest = errors.New("injected stage 3 ingest failure")

	_, err := Import(
		context.Background(),
		source,
		fixedStatter{size: 16},
		store,
		Options{PublicationLanes: 2, PacksPerTransaction: 1},
	)
	if err == nil || !strings.Contains(err.Error(), "injected stage 3 ingest failure") {
		t.Fatalf("stage 3 ingest failure error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.reducedOrder) > 0 {
		t.Fatalf("reduction continued after stage 3 ingest failure: %v", store.reducedOrder)
	}
}

func TestImportStage3AdaptiveSplitUsesDeterministicChildrenAndFinalCheckpoint(t *testing.T) {
	indexID := vaultic.NewRandomID()
	packIDs := []vaultic.ID{vaultic.NewRandomID(), vaultic.NewRandomID()}
	store := newSplitStore()
	store.maxIngestPacks = 1
	result, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndexWithPacks(t, packIDs)}},
		fixedStatter{size: 16},
		store,
		Options{PublicationLanes: 2, PacksPerTransaction: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ingestedOrder) != 3 {
		t.Fatalf("ingested batch count = %d, want 3", len(store.ingestedOrder))
	}
	root := store.ingestedOrder[0]
	left, right, err := stage3SplitBatchIDs(root)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(store.ingestedOrder, []uint64{root, left, right}) {
		t.Fatalf("ingested batch IDs = %v", store.ingestedOrder)
	}
	if !slices.Equal(store.reducedOrder, []uint64{left, right}) {
		t.Fatalf("reduced batch IDs = %v", store.reducedOrder)
	}
	if !slices.Equal(store.reduceCheckpoint, []bool{false, true}) {
		t.Fatalf("checkpoint flags = %v", store.reduceCheckpoint)
	}
	if result.AdaptiveSplits != 1 || result.BatchesCommitted != 1 || result.BatchesReduced != 2 {
		t.Fatalf("unexpected stage 3 split result: %#v", result)
	}
}

func TestImportStage3PartialWorkBudgetSkipsCleanupButFinalCompletesSession(t *testing.T) {
	indexID := vaultic.NewRandomID()
	packIDs := []vaultic.ID{vaultic.NewRandomID(), vaultic.NewRandomID()}
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndexWithPacks(t, packIDs)}}

	partialStore := newSplitStore()
	_, err := Import(
		context.Background(), source, fixedStatter{size: 16}, partialStore,
		Options{PublicationLanes: 2, PacksPerTransaction: 1, WorkBudget: 1},
	)
	if !errors.Is(err, ErrLimitReached) {
		t.Fatalf("partial stage 3 error = %v", err)
	}
	partialStore.mu.Lock()
	if partialStore.completeCalls != 0 {
		partialStore.mu.Unlock()
		t.Fatalf("partial import completed session %d times", partialStore.completeCalls)
	}
	partialStore.mu.Unlock()

	finalStore := newSplitStore()
	_, err = Import(
		context.Background(), source, fixedStatter{size: 16}, finalStore,
		Options{PublicationLanes: 2, PacksPerTransaction: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	finalStore.mu.Lock()
	defer finalStore.mu.Unlock()
	if finalStore.completeCalls != 1 {
		t.Fatalf("final import completed session %d times, want 1", finalStore.completeCalls)
	}
}

func TestImportStage3ResumeCheckpointRunsCleanupBeforeSkip(t *testing.T) {
	indexID := vaultic.NewRandomID()
	store := newSplitStore()
	checkpoint, err := (schema.ImportCheckpointRecord{PacksImported: 1, BlobsImported: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(schema.ImportCheckpointKey(schema.ID(indexID)))] = checkpoint
	result, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndexWithPacks(t, []vaultic.ID{vaultic.NewRandomID()})}},
		fixedStatter{size: 16},
		store,
		Options{Resume: true, PublicationLanes: 2, PacksPerTransaction: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if result.IndexesResumed != 1 || result.IndexesImported != 0 {
		t.Fatalf("resume result = %#v", result)
	}
	if store.completeCalls != 1 {
		t.Fatalf("cleanup calls on resume skip = %d, want 1", store.completeCalls)
	}
}

func TestImportStage3PartialThenFullRunAvoidsReceiptConflictAndCompletes(t *testing.T) {
	indexID := vaultic.NewRandomID()
	packIDs := []vaultic.ID{vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()}
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndexWithPacks(t, packIDs)}}
	store := newSplitStore()

	partialResult, err := Import(
		context.Background(), source, fixedStatter{size: 16}, store,
		Options{Resume: true, PublicationLanes: 2, PacksPerTransaction: 2, WorkBudget: 1},
	)
	if !errors.Is(err, ErrLimitReached) {
		t.Fatalf("partial stage 3 run error = %v", err)
	}
	if partialResult.PacksImported != 1 || partialResult.BatchesCommitted != 1 {
		t.Fatalf("partial stage 3 result = %#v", partialResult)
	}

	fullResult, err := Import(
		context.Background(), source, fixedStatter{size: 16}, store,
		Options{Resume: true, PublicationLanes: 2, PacksPerTransaction: 2},
	)
	if err != nil {
		t.Fatalf("full stage 3 run after partial failed: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if fullResult.PacksImported != 3 || fullResult.BlobsImported != 3 || fullResult.BatchesCommitted != 2 {
		t.Fatalf("full stage 3 result = %#v", fullResult)
	}
	if store.completeCalls != 1 {
		t.Fatalf("complete session calls = %d, want 1", store.completeCalls)
	}
	checkpointKey := schema.ImportCheckpointKey(schema.ID(indexID))
	if _, found := store.values[string(checkpointKey)]; !found {
		t.Fatal("full stage 3 run did not publish final checkpoint")
	}
}

func TestImportStage3PrefersRealLaneFailureOverEarlierCanceledLane(t *testing.T) {
	indexID := vaultic.NewRandomID()
	var firstPack vaultic.ID
	var secondPack vaultic.ID
	firstPack[31] = 1
	secondPack[31] = 2
	packIDs := []vaultic.ID{firstPack, secondPack}
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndexWithPacks(t, packIDs)}}
	store := newSplitStore()
	blockFirst := make(chan struct{})
	store.blockByEnter[1] = blockFirst
	store.failByEnter[2] = errors.New("later-lane-sentinel")

	sentinel := store.failByEnter[2]
	resultErr := make(chan error, 1)
	go func() {
		_, err := Import(
			context.Background(), source, fixedStatter{size: 16}, store,
			Options{PublicationLanes: 2, PacksPerTransaction: 1},
		)
		resultErr <- err
	}()

	firstBatch := <-store.entered
	secondBatch := <-store.entered
	close(blockFirst)

	err := <-resultErr
	if err == nil || !strings.Contains(err.Error(), sentinel.Error()) {
		t.Fatalf("preferred lane failure error = %v", err)
	}
	if !strings.Contains(err.Error(), secondPack.Str()) {
		t.Fatalf("preferred lane failure pack mismatch: err=%v first=%d second=%d", err, firstBatch, secondBatch)
	}
}

func TestImportStage3FallbackAndLaneValidation(t *testing.T) {
	_, err := Import(context.Background(), &memorySource{}, fixedStatter{}, newMemoryStore(), Options{PublicationLanes: 9})
	if err == nil || !strings.Contains(err.Error(), "publication lanes must not exceed 8") {
		t.Fatalf("publication lane validation error = %v", err)
	}

	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	store := newMemoryStore()
	result, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
		fixedStatter{size: 16},
		store,
		Options{PublicationLanes: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.imports) != 1 || result.BatchesCommitted != 1 || result.BatchesIngested != 0 || result.BatchesReduced != 0 {
		t.Fatalf("stage 2 fallback result=%#v imports=%d", result, len(store.imports))
	}
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
	if err != nil || result.BlobsImported != 1 || result.BatchesCommitted != 0 || len(store.imports) != 0 || len(store.values) != 0 {
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

func TestImportPublishesOrderedBoundedBatchesAndFinalCheckpoint(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	expected := make([]vaultic.ID, 5)
	for offset := range expected {
		expected[offset] = vaultic.NewRandomID()
		idx.StorePack(
			expected[offset],
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	sort.Slice(expected, func(left, right int) bool { return string(expected[left][:]) < string(expected[right][:]) })
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	result, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
		fixedStatter{size: 2},
		store,
		Options{PackWorkers: 3, PacksPerTransaction: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(store.batchSizes, []int{2, 2, 1}) ||
		!slices.Equal(store.checkpointBatches, []int{2}) || result.BatchesCommitted != 3 {
		t.Fatalf("batch boundaries=%v checkpoints=%v result=%#v", store.batchSizes, store.checkpointBatches, result)
	}
	for offset, imported := range store.imports {
		actual := vaultic.ID(imported.PackID)
		if actual != expected[offset] {
			t.Fatalf("import order[%d] = %s, want %s", offset, actual.Str(), expected[offset].Str())
		}
	}
}

func TestImportPreparedByteBudgetCannotDeadlockBeforeBatchFlush(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	for range 5 {
		idx.StorePack(
			vaultic.NewRandomID(),
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := Import(
			context.Background(),
			&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
			fixedStatter{size: 2}, newMemoryStore(),
			Options{PacksPerTransaction: 8, PreparedImportBytes: 2_000},
		)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("import deadlocked below the prepared-byte transaction floor")
	}
}

func TestImportResumeReplaysCommittedBatchesWithoutCheckpoint(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	expected := make([]vaultic.ID, 3)
	for offset := range expected {
		expected[offset] = vaultic.NewRandomID()
		idx.StorePack(
			expected[offset],
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	sort.Slice(expected, func(left, right int) bool { return bytes.Compare(expected[left][:], expected[right][:]) < 0 })
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	for failAt := 2; failAt <= len(expected); failAt++ {
		t.Run(fmt.Sprintf("boundary-%d", failAt-1), func(t *testing.T) {
			source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}}
			store := &failBatchStore{memoryStore: newMemoryStore(), failAt: failAt}
			_, err := Import(
				context.Background(), source, fixedStatter{size: 2}, store,
				Options{Resume: true, PacksPerTransaction: 1},
			)
			committed := failAt - 1
			if err == nil || len(store.imports) != committed {
				t.Fatalf("interrupted import: imports=%d want=%d err=%v", len(store.imports), committed, err)
			}
			checkpointKey := schema.ImportCheckpointKey(schema.ID(indexID))
			if _, found := store.values[string(checkpointKey)]; found {
				t.Fatal("partial import published a checkpoint")
			}
			result, err := Import(
				context.Background(), source, fixedStatter{size: 2}, store,
				Options{Resume: true, PacksPerTransaction: 1},
			)
			if err != nil || result.IndexesImported != 1 || len(store.imports) != committed+len(expected) {
				t.Fatalf("resumed import: result=%#v imports=%d err=%v", result, len(store.imports), err)
			}
			for index := range committed {
				if vaultic.ID(store.imports[index].PackID) != expected[index] ||
					vaultic.ID(store.imports[committed+index].PackID) != expected[index] {
					t.Fatalf("committed prefix was not replayed: %#v", store.imports)
				}
			}
			if _, found := store.values[string(checkpointKey)]; !found {
				t.Fatal("resumed import did not publish final checkpoint")
			}
		})
	}
}

func TestImportAdaptivelySplitsUnderestimatedBatch(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	for range 5 {
		idx.StorePack(
			vaultic.NewRandomID(),
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	store := &sizeLimitedImportStore{memoryStore: newMemoryStore(), maxPacks: 2}
	result, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
		fixedStatter{size: 2},
		store,
		Options{PacksPerTransaction: 8},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(store.batchSizes, []int{2, 1, 2}) ||
		!slices.Equal(store.checkpointBatches, []int{2}) || result.BatchesCommitted != 3 ||
		result.AdaptiveSplits != 2 || result.PeakPreparedPacks == 0 || result.PeakPreparedBytes == 0 {
		t.Fatalf("adaptive batches=%v checkpoints=%v result=%#v", store.batchSizes, store.checkpointBatches, result)
	}
}

func TestImportReportsLiveBatchAndCheckpointProgress(t *testing.T) {
	indexID, secondIndexID := vaultic.NewRandomID(), vaultic.NewRandomID()
	idx := index.NewIndex()
	for range 5 {
		idx.StorePack(
			vaultic.NewRandomID(),
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	var progress []Progress
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes(), secondIndexID: encoded.Bytes()}},
		fixedStatter{size: 2},
		newMemoryStore(),
		Options{PacksPerTransaction: 2, Progress: func(update Progress) { progress = append(progress, update) }},
	)
	if err != nil {
		t.Fatal(err)
	}
	var sawLive, sawPending bool
	var previous Progress
	for index, update := range progress {
		if update.IndexesCompleted < 2 && update.PacksImported > previous.PacksImported {
			sawLive = true
		}
		if update.CheckpointPending {
			sawPending = true
		}
		if update.PacksPrepared < update.PacksImported || update.PeakPreparedBytes < update.QueuedPreparedBytes {
			t.Fatalf("invalid live progress = %#v", update)
		}
		if index > 0 && (update.PreparationTime < previous.PreparationTime ||
			update.PublicationTime < previous.PublicationTime ||
			update.CheckpointBatchTime < previous.CheckpointBatchTime ||
			update.PeakPreparedPacks < previous.PeakPreparedPacks ||
			update.PeakPreparedBytes < previous.PeakPreparedBytes ||
			update.AdaptiveSplits < previous.AdaptiveSplits) {
			t.Fatalf("non-monotonic cumulative progress: previous=%#v current=%#v", previous, update)
		}
		previous = update
	}
	if !sawLive || !sawPending || progress[len(progress)-1].CheckpointPending {
		t.Fatalf("live progress snapshots = %#v", progress)
	}
}

func TestImportPackTimeoutOnlyBoundsPreparation(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	statter := &blockingStatter{entered: make(chan time.Duration, 1), release: make(chan struct{})}
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
		statter,
		newMemoryStore(),
		Options{PackTimeout: 10 * time.Millisecond},
	)
	if err == nil || !strings.Contains(err.Error(), "pack import exceeded 10ms; increase --pack-timeout") {
		t.Fatalf("preparation timeout error = %v", err)
	}
}

func TestImportReportsCheckpointOnlyFailure(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
		fixedStatter{},
		&failingImportStore{memoryStore: newMemoryStore()},
		Options{},
	)
	if err == nil || !strings.Contains(err.Error(), "publish import checkpoint") {
		t.Fatalf("checkpoint-only failure = %v", err)
	}
}

func TestImportReportsFinalCheckpointBatchFailure(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
		fixedStatter{size: 16},
		&failingImportStore{memoryStore: newMemoryStore()},
		Options{},
	)
	if err == nil || !strings.Contains(err.Error(), "publish final import batch and checkpoint") {
		t.Fatalf("final checkpoint batch failure = %v", err)
	}
}

func TestImportRunsPackWorkersConcurrentlyBeforeCheckpoint(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	for range 3 {
		idx.StorePack(
			vaultic.NewRandomID(),
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	statter := &blockingStatter{entered: make(chan time.Duration, 3), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := Import(
			context.Background(),
			&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
			statter,
			store,
			Options{Resume: true, PackWorkers: 2},
		)
		done <- err
	}()
	for range 2 {
		select {
		case <-statter.entered:
		case <-time.After(time.Second):
			t.Fatal("pack imports did not overlap")
		}
	}
	select {
	case <-statter.entered:
		t.Fatal("pack preparations exceeded the configured worker limit")
	case <-time.After(50 * time.Millisecond):
	}
	if _, found := store.values[string(schema.ImportCheckpointKey(schema.ID(indexID)))]; found {
		t.Fatal("checkpoint published before pack workers completed")
	}
	close(statter.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, found := store.values[string(schema.ImportCheckpointKey(schema.ID(indexID)))]; !found {
		t.Fatal("checkpoint missing after pack workers completed")
	}
}

func TestImportAppliesConfiguredBatchTimeout(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	store := &deadlineImportStore{memoryStore: newMemoryStore(), remaining: make(chan time.Duration, 1)}
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
		fixedStatter{size: 16},
		store,
		Options{ImportBatchTimeout: 2 * time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	remaining := <-store.remaining
	if remaining <= time.Minute || remaining > 2*time.Minute {
		t.Fatalf("batch deadline remaining = %v, want within (1m, 2m]", remaining)
	}
}

func TestImportPreservesShorterCallerDeadline(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	store := &deadlineImportStore{memoryStore: newMemoryStore(), remaining: make(chan time.Duration, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := Import(
		ctx,
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
		fixedStatter{size: 16},
		store,
		Options{ImportBatchTimeout: 2 * time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	remaining := <-store.remaining
	if remaining <= 20*time.Second || remaining > 30*time.Second {
		t.Fatalf("pack import deadline remaining = %v, want within (20s, 30s]", remaining)
	}
}

func TestImportRejectsNegativePackTimeout(t *testing.T) {
	_, err := Import(context.Background(), &memorySource{}, fixedStatter{}, newMemoryStore(), Options{PackTimeout: -time.Second})
	if err == nil || !strings.Contains(err.Error(), "pack timeout must not be negative") {
		t.Fatalf("negative pack timeout error = %v", err)
	}
}

func TestImportRejectsInvalidBatchTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{-time.Second, maxImportBatchTimeout + time.Second} {
		_, err := Import(
			context.Background(), &memorySource{}, fixedStatter{}, newMemoryStore(), Options{ImportBatchTimeout: timeout},
		)
		if err == nil || !strings.Contains(err.Error(), "import batch timeout must be between") {
			t.Fatalf("batch timeout %v error = %v", timeout, err)
		}
	}
}

func TestImportRejectsUnboundedTransactionPackFloor(t *testing.T) {
	_, err := Import(
		context.Background(), &memorySource{}, fixedStatter{}, newMemoryStore(),
		Options{PacksPerTransaction: MaxPacksPerTransaction + 1},
	)
	if err == nil || !strings.Contains(err.Error(), "packs per transaction must not exceed 256") {
		t.Fatalf("unbounded transaction floor error = %v", err)
	}
}

func TestImportExplainsBatchTimeout(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	store := &blockingImportStore{
		memoryStore: newMemoryStore(),
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
		fixedStatter{size: 16},
		store,
		Options{ImportBatchTimeout: 10 * time.Millisecond},
	)
	if err == nil || !strings.Contains(err.Error(), "legacy import batch exceeded 10ms") {
		t.Fatalf("batch timeout error = %v", err)
	}
}

func TestImportDoesNotMislabelCallerDeadlineAsPackTimeout(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	store := &blockingImportStore{
		memoryStore: newMemoryStore(),
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := Import(
		ctx,
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
		fixedStatter{size: 16},
		store,
		Options{ImportBatchTimeout: time.Minute},
	)
	if err == nil || strings.Contains(err.Error(), "legacy import batch exceeded") {
		t.Fatalf("caller deadline error = %v", err)
	}
}

func TestImportPreservesCancellationWhileAwaitingPreparedPack(t *testing.T) {
	for range 20 {
		indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
		statter := &blockingStatter{entered: make(chan time.Duration, 1), release: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := Import(
				ctx,
				&memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}},
				statter,
				newMemoryStore(),
				Options{PackWorkers: 1},
			)
			done <- err
		}()
		<-statter.entered
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled import error = %v", err)
		}
	}
}

func TestImportStopsDispatchAfterPackFailure(t *testing.T) {
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	for range 3 {
		idx.StorePack(
			vaultic.NewRandomID(),
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	store := &failingImportStore{memoryStore: newMemoryStore()}
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
		fixedStatter{size: 2},
		store,
		Options{Resume: true, PackWorkers: 1},
	)
	if err == nil || !strings.Contains(err.Error(), "injected pack import failure") {
		t.Fatalf("pack failure = %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("pack imports after failure = %d, want 1", store.calls)
	}
	if _, found := store.values[string(schema.ImportCheckpointKey(schema.ID(indexID)))]; found {
		t.Fatal("failed index published a checkpoint")
	}
}

func TestImportAttributesBatchPlanningFailureToResponsiblePack(t *testing.T) {
	indexID := vaultic.NewRandomID()
	firstPack, secondPack := vaultic.NewRandomID(), vaultic.NewRandomID()
	if bytes.Compare(firstPack[:], secondPack[:]) > 0 {
		firstPack, secondPack = secondPack, firstPack
	}
	idx := index.NewIndex()
	for _, packID := range []vaultic.ID{firstPack, secondPack} {
		idx.StorePack(
			packID,
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	_, err := Import(
		context.Background(),
		&memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}},
		fixedStatter{size: 2},
		&inputFailingImportStore{memoryStore: newMemoryStore()},
		Options{PacksPerTransaction: 2},
	)
	if err == nil || !strings.Contains(err.Error(), secondPack.Str()) || strings.Contains(err.Error(), firstPack.Str()) {
		t.Fatalf("batch planning failure attribution = %v", err)
	}
}

func TestImportRecordsUnavailablePackDebtAsWarning(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}}
	store := newMemoryStore()
	result, err := Import(context.Background(), source, fixedStatter{err: errors.New("offline")}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if result.CrawlDebtCreated != 1 || result.WarningsSeen != 1 || result.ErrorsSeen != 0 ||
		store.imports[0].Debt == nil {
		t.Fatalf("missing pack debt: %#v", result)
	}
	_, err = Import(
		context.Background(),
		source,
		fixedStatter{err: errors.New("offline")},
		newMemoryStore(),
		Options{MaxErrors: 1},
	)
	if err != nil {
		t.Fatalf("warning counted against max errors: %v", err)
	}
}

func BenchmarkImportPackTransactionSizes(b *testing.B) {
	const packCount = 64
	indexID := vaultic.NewRandomID()
	idx := index.NewIndex()
	for range packCount {
		idx.StorePack(
			vaultic.NewRandomID(),
			pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 1}},
		)
	}
	var encoded bytes.Buffer
	if err := idx.Encode(&encoded); err != nil {
		b.Fatal(err)
	}
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encoded.Bytes()}}
	for _, packsPerTransaction := range []uint{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("packs-%d", packsPerTransaction), func(b *testing.B) {
			var commits uint64
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := Import(
					context.Background(), source, fixedStatter{size: 2}, newMemoryStore(),
					Options{PackWorkers: 8, PacksPerTransaction: packsPerTransaction},
				)
				if err != nil {
					b.Fatal(err)
				}
				commits += result.BatchesCommitted
			}
			b.ReportMetric(float64(commits)/float64(b.N), "commits/op")
			b.ReportMetric(float64(packCount), "packs/op")
		})
	}
}

// TestImportLeavesEveryPackTierAndRetentionUnknown pins the Phase 9 rule for
// inherited packs: a legacy index carries no routing or creation time, so no
// tier, timestamp, or retention deadline may be synthesized for it. A pack
// imported this way stays retention-unknown permanently, which is what keeps
// it out of any early-deletion savings claim.
func TestImportLeavesEveryPackTierAndRetentionUnknown(t *testing.T) {
	indexID, packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	source := &memorySource{indexes: map[vaultic.ID][]byte{indexID: encodedIndex(t, packID, blobID)}}
	store := newMemoryStore()
	if _, err := Import(context.Background(), source, fixedStatter{size: 10}, store, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(store.imports) != 1 {
		t.Fatalf("imports = %d", len(store.imports))
	}
	record := store.imports[0].Record
	if record.Tier != 0 && record.Tier != schema.TierUnknown {
		t.Fatalf("imported pack was assigned tier %v", record.Tier)
	}
	if record.RetentionSource != 0 && record.RetentionSource != schema.RetentionUnknown {
		t.Fatalf("imported pack was assigned retention source %v", record.RetentionSource)
	}
	if record.CreationTimeKnown || record.CreationTime != 0 {
		t.Fatalf("imported pack invented a creation time: %d/%t", record.CreationTime, record.CreationTimeKnown)
	}
	if record.MinRetentionUntil != 0 || record.DeleteAfter != 0 {
		t.Fatalf("imported pack invented a deadline: %#v", record)
	}
	if record.UsageKnown {
		t.Fatal("imported pack claimed usage accounting")
	}
	// The persisted form must also read back as explicitly unknown, rather
	// than as a zero value that a later reader could misinterpret.
	encoded, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := schema.UnmarshalPackRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Tier != schema.TierUnknown || decoded.RetentionSource != schema.RetentionUnknown {
		t.Fatalf("persisted imported pack = %#v", decoded)
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
	if result.CrawlDebtCreated != 1 || result.WarningsSeen != 1 || result.ErrorsSeen != 0 || len(store.imports) != 1 {
		t.Fatalf("missing inconsistent-size debt: %#v", result)
	}
	record := store.imports[0].Record
	if !record.PhysicalSizeKnown || record.PhysicalSize != 5 || record.PayloadSize != 10 || record.HeaderSize != 0 ||
		store.imports[0].Debt == nil {
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
	var updates []Progress
	options := Options{
		Resume: true, SnapshotDepth: 1,
		Progress: func(progress Progress) { updates = append(updates, progress) },
	}
	result, err := Import(context.Background(), source, fixedStatter{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	if result.SnapshotsImported != 1 || result.NodesImported != 1 || result.CrawlDebtCreated < 3 ||
		store.revisionsWritten != 1 {
		t.Fatalf("unexpected tree import result: %#v, revisions=%d", result, store.revisionsWritten)
	}
	finalProgress := updates[len(updates)-1]
	if finalProgress.SnapshotsCompleted != 1 || finalProgress.SnapshotsTotal != 1 ||
		finalProgress.SnapshotsImported != 1 {
		t.Fatalf("final snapshot progress = %#v", finalProgress)
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
	if record.Freshness != schema.FreshnessImported || record.Known != schema.KnownParent|schema.KnownPath ||
		record.ParentInode != 10 ||
		len(record.ContentIDs) != 1 ||
		record.ContentIDs[0] != schema.ID(contentID) {
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
	if err := be.Save(context.Background(),
		backend.Handle{Type: backend.PackFile,
			Name: packID.String()},
		backend.NewByteReader(packData,
			be.Hasher())); err != nil {
		t.Fatal(err)
	}
	idx := index.NewIndex()
	idx.StorePack(
		packID,
		pack.Blobs{
			{
				BlobHandle:         vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob},
				Offset:             0,
				Length:             10,
				UncompressedLength: 8,
			},
		},
	)
	idx.Finalize()
	if _, err := idx.SaveIndex(context.Background(), unpacked); err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	result, err := Import(context.Background(), repo, be, store, Options{Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.IndexesImported != 1 || result.PacksImported != 1 || result.BlobsImported != 1 ||
		result.CrawlDebtCreated != 0 ||
		len(store.imports) != 1 {
		t.Fatalf("unexpected real repository import: %#v", result)
	}
	imported := store.imports[0]
	if imported.Record.PhysicalSize != uint64(len(packData)) || imported.Record.PayloadSize != 10 ||
		imported.Record.HeaderSize != 6 ||
		imported.Debt != nil {
		t.Fatalf("incorrect imported pack metadata: %#v", imported.Record)
	}
}

func TestImportReportsIndexProgressAndTotals(t *testing.T) {
	source := &memorySource{indexes: map[vaultic.ID][]byte{
		vaultic.NewRandomID(): []byte(`{"packs":[]}`),
		vaultic.NewRandomID(): []byte(`{"packs":[]}`),
	}}
	var updates []Progress
	result, err := Import(context.Background(), source, fixedStatter{}, newMemoryStore(), Options{
		Progress: func(progress Progress) { updates = append(updates, progress) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IndexesTotal != 2 || result.IndexesImported != 2 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if len(updates) != 3 {
		t.Fatalf("got %d progress updates, want initial plus two completed indexes: %#v", len(updates), updates)
	}
	if initial := updates[0]; initial.IndexesCompleted != 0 || initial.IndexesTotal != 2 {
		t.Fatalf("initial progress = %#v", initial)
	}
	if final := updates[len(updates)-1]; final.IndexesCompleted != 2 || final.IndexesTotal != 2 || final.IndexesImported != 2 {
		t.Fatalf("final progress = %#v", final)
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
	if result.PacksImported != 1 || result.BlobsImported != 0 || len(store.imports) != 1 ||
		store.imports[0].Record.Type != schema.PackUnknown ||
		store.imports[0].Record.HeaderSize != 9 {
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
	if result.PacksImported != 1 || len(store.imports) != 1 || store.imports[0].Record.Type != schema.PackMixed ||
		store.imports[0].Record.BlobCount != 2 {
		t.Fatalf("mixed pack import = %#v, result=%#v", store.imports, result)
	}
}

func TestSnapshotTraversalLimitsLeaveNoCheckpoint(t *testing.T) {
	fileTree := treeJSON(t, &data.Node{Name: "file", Type: data.NodeTypeFile, DeviceID: 7, Inode: 12})
	fileTreeID := vaultic.Hash(fileTree)
	nestedTree := treeJSON(
		t,
		&data.Node{Name: "nested", Type: data.NodeTypeDir, DeviceID: 7, Inode: 11, Subtree: &fileTreeID},
	)
	nestedTreeID := vaultic.Hash(nestedTree)
	rootTree := treeJSON(
		t,
		&data.Node{Name: "top", Type: data.NodeTypeDir, DeviceID: 7, Inode: 10, Subtree: &nestedTreeID},
	)
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
	childTree := treeJSON(
		t,
		&data.Node{Name: "large", Type: data.NodeTypeFile, DeviceID: 7, Inode: 11, Content: content},
	)
	childTreeID := vaultic.Hash(childTree)
	rootTree := treeJSON(
		t,
		&data.Node{Name: "top", Type: data.NodeTypeDir, DeviceID: 7, Inode: 10, Subtree: &childTreeID},
	)
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
