package daemon

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestSchemaStoreLegacyIngestReduceMatchesStage2(t *testing.T) {
	originalClock := historyClock
	historyClock = func() time.Time { return time.Unix(1_700_000_000, 0) }
	t.Cleanup(func() { historyClock = originalClock })

	ctx := context.Background()
	newStore := func(repositoryID string) (*SchemaStore, *Client) {
		client, err := Ensure(ctx, Options{
			Socket: testSocket(t), RepositoryID: repositoryID, DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
			RebuildReset: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return NewSchemaStore(client), client
	}
	stage2, stage2Client := newStore("phase32-stage3-stage2")
	defer stage2Client.Close(ctx)
	stage3, stage3Client := newStore("phase32-stage3-stage3")
	defer stage3Client.Close(ctx)
	stage3.EnableFreshLegacyImport()

	source := daemonTestID(41)
	session := daemonTestID(200)
	pack1, pack2 := daemonTestID(42), daemonTestID(43)
	blob1, blob2 := daemonTestID(44), daemonTestID(45)

	batch1 := []LegacyPackImport{legacyPackImport(source, pack1, map[schema.ID]schema.BlobRecord{
		blob1: {Locations: []schema.BlobLocation{{PackID: pack1, Offset: 1, Length: 5, Type: schema.BlobData}}},
	})}
	batch2 := []LegacyPackImport{legacyPackImport(source, pack2, map[schema.ID]schema.BlobRecord{
		blob2: {Locations: []schema.BlobLocation{{PackID: pack2, Offset: 0, Length: 4, Type: schema.BlobData}}},
	})}
	checkpointRecord := schema.ImportCheckpointRecord{PacksImported: 1, BlobsImported: 1}
	checkpoint := Mutation{Key: schema.ImportCheckpointKey(source), Value: encodeSchemaRecord(t, checkpointRecord)}

	if err := stage2.ImportLegacyPacks(ctx, batch1, nil); err != nil {
		t.Fatal(err)
	}
	if err := stage2.ImportLegacyPacks(ctx, batch2, &checkpoint); err != nil {
		t.Fatal(err)
	}

	if err := stage3.IngestLegacyPacks(ctx, session, 2, batch2); err != nil {
		t.Fatal(err)
	}
	if err := stage3.IngestLegacyPacks(ctx, session, 1, batch1); err != nil {
		t.Fatal(err)
	}
	if err := stage3.ReduceLegacyImportBatch(ctx, session, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := stage3.ReduceLegacyImportBatch(ctx, session, 2, &checkpoint); err != nil {
		t.Fatal(err)
	}

	prefixes := [][]byte{[]byte("b:"), []byte("p:"), []byte("pl:"), []byte("q:"), []byte("a:"), []byte("ph:")}
	stage2Records := readSchemaPrefixes(t, stage2, prefixes)
	stage3Records := readSchemaPrefixes(t, stage3, prefixes)
	normalizeAggregateSequences(t, stage2Records)
	normalizeAggregateSequences(t, stage3Records)

	stage2Checkpoint, found, err := stage2.Get(ctx, checkpoint.Key)
	if err != nil || !found {
		t.Fatalf("read stage2 checkpoint: found=%t err=%v", found, err)
	}
	stage3Checkpoint, found, err := stage3.Get(ctx, checkpoint.Key)
	if err != nil || !found {
		t.Fatalf("read stage3 checkpoint: found=%t err=%v", found, err)
	}
	stage2Records[string(checkpoint.Key)] = stage2Checkpoint
	stage3Records[string(checkpoint.Key)] = stage3Checkpoint

	if !reflect.DeepEqual(stage3Records, stage2Records) {
		t.Fatalf("stage3 reduction metadata differs from stage2:\nstage2=%#v\nstage3=%#v", stage2Records, stage3Records)
	}
}

func TestSchemaStoreLegacyIngestReduceMatchesStage2WithDuplicatePackBlob(t *testing.T) {
	ctx := context.Background()
	newStore := func(repositoryID string) (*SchemaStore, *Client) {
		client, err := Ensure(ctx, Options{
			Socket: testSocket(t), RepositoryID: repositoryID, DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
			RebuildReset: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return NewSchemaStore(client), client
	}
	stage2, stage2Client := newStore("phase32-stage3-duplicate-stage2")
	defer stage2Client.Close(ctx)
	stage3, stage3Client := newStore("phase32-stage3-duplicate-stage3")
	defer stage3Client.Close(ctx)
	stage3.EnableFreshLegacyImport()

	source := daemonTestID(240)
	session := daemonTestID(241)
	packID := daemonTestID(242)
	blobID := daemonTestID(243)
	imports := []LegacyPackImport{
		legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
			blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: 0, Length: 3, Type: schema.BlobData}}},
		}),
		legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
			blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: 3, Length: 2, Type: schema.BlobData}}},
		}),
	}
	checkpoint := Mutation{
		Key:   schema.ImportCheckpointKey(source),
		Value: encodeSchemaRecord(t, schema.ImportCheckpointRecord{PacksImported: 2, BlobsImported: 2}),
	}

	if err := stage2.ImportLegacyPacks(ctx, imports, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := stage3.IngestLegacyPacks(ctx, session, 1, imports); err != nil {
		t.Fatal(err)
	}
	if err := stage3.ReduceLegacyImportBatch(ctx, session, 1, &checkpoint); err != nil {
		t.Fatal(err)
	}

	prefixes := [][]byte{[]byte("b:"), []byte("p:"), []byte("pl:"), []byte("q:"), []byte("a:"), []byte("ph:")}
	stage2Records := readSchemaPrefixes(t, stage2, prefixes)
	stage3Records := readSchemaPrefixes(t, stage3, prefixes)
	normalizeAggregateSequences(t, stage2Records)
	normalizeAggregateSequences(t, stage3Records)

	stage2Checkpoint, found, err := stage2.Get(ctx, checkpoint.Key)
	if err != nil || !found {
		t.Fatalf("read stage2 checkpoint: found=%t err=%v", found, err)
	}
	stage3Checkpoint, found, err := stage3.Get(ctx, checkpoint.Key)
	if err != nil || !found {
		t.Fatalf("read stage3 checkpoint: found=%t err=%v", found, err)
	}
	stage2Records[string(checkpoint.Key)] = stage2Checkpoint
	stage3Records[string(checkpoint.Key)] = stage3Checkpoint

	if !reflect.DeepEqual(stage3Records, stage2Records) {
		t.Fatalf("stage3 duplicate-pack/blob metadata differs from stage2:\nstage2=%#v\nstage3=%#v", stage2Records, stage3Records)
	}
}

func TestSchemaStoreLegacyIngestIdempotencyConflict(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-idempotency", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session := daemonTestID(201)
	source, packID, blobID := daemonTestID(47), daemonTestID(48), daemonTestID(49)
	imports := []LegacyPackImport{legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
		blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: 1, Length: 3, Type: schema.BlobData}}},
	})}

	if err := store.IngestLegacyPacks(ctx, session, 1, imports); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestLegacyPacks(ctx, session, 1, imports); err != nil {
		t.Fatalf("idempotent ingest failed: %v", err)
	}

	conflicting := append([]LegacyPackImport(nil), imports...)
	conflicting[0].Blobs = map[schema.ID]schema.BlobRecord{
		blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: 2, Length: 3, Type: schema.BlobData}}},
	}
	err = store.IngestLegacyPacks(ctx, session, 1, conflicting)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting ingest error = %v", err)
	}
}

func TestSchemaStoreLegacyReducerReplayIsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-reduce-replay", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session := daemonTestID(202)
	source, packID, blobID := daemonTestID(50), daemonTestID(51), daemonTestID(52)
	imports := []LegacyPackImport{legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
		blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: 0, Length: 4, Type: schema.BlobData}}},
	})}
	if err := store.IngestLegacyPacks(ctx, session, 1, imports); err != nil {
		t.Fatal(err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 1, nil); err != nil {
		t.Fatal(err)
	}
	first := readSchemaPrefixes(t, store, [][]byte{[]byte("a:"), []byte("ph:"), []byte("meta:next-event-seq")})
	if err := store.ReduceLegacyImportBatch(ctx, session, 1, nil); err != nil {
		t.Fatal(err)
	}
	second := readSchemaPrefixes(t, store, [][]byte{[]byte("a:"), []byte("ph:"), []byte("meta:next-event-seq")})
	normalizeAggregateSequences(t, first)
	normalizeAggregateSequences(t, second)
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("replay reduction changed state:\nfirst=%#v\nsecond=%#v", first, second)
	}
	receiptValue, found, err := store.Get(ctx, schema.LegacyImportReceiptKey(session, 1))
	if err != nil || !found {
		t.Fatalf("read reduced receipt: found=%t err=%v", found, err)
	}
	receipt, err := schema.UnmarshalLegacyImportReceiptRecord(receiptValue)
	if err != nil || !receipt.Reduced {
		t.Fatalf("reduced receipt = %#v, err=%v", receipt, err)
	}
}

func TestSchemaStoreLegacyCheckpointOnlyAtFinalReduction(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-final-checkpoint", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session := daemonTestID(203)
	source := daemonTestID(53)
	batch1 := []LegacyPackImport{legacyPackImport(source, daemonTestID(54), map[schema.ID]schema.BlobRecord{
		daemonTestID(55): {Locations: []schema.BlobLocation{{PackID: daemonTestID(54), Offset: 1, Length: 2, Type: schema.BlobData}}},
	})}
	batch2 := []LegacyPackImport{legacyPackImport(source, daemonTestID(56), map[schema.ID]schema.BlobRecord{
		daemonTestID(57): {Locations: []schema.BlobLocation{{PackID: daemonTestID(56), Offset: 0, Length: 3, Type: schema.BlobData}}},
	})}
	checkpoint := Mutation{
		Key: schema.ImportCheckpointKey(source),
		Value: encodeSchemaRecord(t, schema.ImportCheckpointRecord{
			PacksImported: 2,
			BlobsImported: 2,
		}),
	}
	if err := store.IngestLegacyPacks(ctx, session, 2, batch2); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestLegacyPacks(ctx, session, 1, batch1); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, checkpoint.Key); err != nil || found {
		t.Fatalf("checkpoint before reduction: found=%t err=%v", found, err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, checkpoint.Key); err != nil || found {
		t.Fatalf("checkpoint before final reduction: found=%t err=%v", found, err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 2, &checkpoint); err != nil {
		t.Fatal(err)
	}
	value, found, err := store.Get(ctx, checkpoint.Key)
	if err != nil || !found || !bytes.Equal(value, checkpoint.Value) {
		t.Fatalf("final checkpoint: found=%t err=%v value=%x", found, err, value)
	}
}

func TestSchemaStoreCompleteLegacyImportSessionRejectsUnresolved(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-cleanup", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session := daemonTestID(204)
	source := daemonTestID(58)
	batch1 := []LegacyPackImport{legacyPackImport(source, daemonTestID(59), map[schema.ID]schema.BlobRecord{
		daemonTestID(60): {Locations: []schema.BlobLocation{{PackID: daemonTestID(59), Offset: 0, Length: 2, Type: schema.BlobData}}},
	})}
	batch2 := []LegacyPackImport{legacyPackImport(source, daemonTestID(61), map[schema.ID]schema.BlobRecord{
		daemonTestID(62): {Locations: []schema.BlobLocation{{PackID: daemonTestID(61), Offset: 1, Length: 2, Type: schema.BlobData}}},
	})}
	if err := store.IngestLegacyPacks(ctx, session, 1, batch1); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestLegacyPacks(ctx, session, 2, batch2); err != nil {
		t.Fatal(err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 1, nil); err != nil {
		t.Fatal(err)
	}
	err = store.CompleteLegacyImportSession(ctx, session)
	if !errors.Is(err, ErrLegacyImportReceiptUnresolved) {
		t.Fatalf("cleanup unresolved error = %v", err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 2, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteLegacyImportSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	entries, _, err := store.ScanPrefix(ctx, schema.LegacyImportReceiptPrefix(session), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cleanup left receipts behind: %d", len(entries))
	}
}

func TestSchemaStoreReduceLegacyImportMissingReceiptAllowedWhenCheckpointExists(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-missing-receipt", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session := daemonTestID(205)
	source := daemonTestID(63)
	checkpoint := Mutation{
		Key:   schema.ImportCheckpointKey(source),
		Value: encodeSchemaRecord(t, schema.ImportCheckpointRecord{PacksImported: 1, BlobsImported: 1}),
	}
	if err := store.Put(ctx, checkpoint.Key, checkpoint.Value, true); err != nil {
		t.Fatal(err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 7, &checkpoint); err != nil {
		t.Fatalf("reduce with existing checkpoint failed: %v", err)
	}
	err = store.ReduceLegacyImportBatch(ctx, session, 7, nil)
	if !errors.Is(err, ErrLegacyImportReceiptMissing) {
		t.Fatalf("missing receipt error = %v", err)
	}
}

func TestSchemaStoreLegacySplitRequiresFreshImportMode(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-fresh-required", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	session := daemonTestID(206)
	source := daemonTestID(64)
	imports := []LegacyPackImport{legacyPackImport(source, daemonTestID(65), map[schema.ID]schema.BlobRecord{
		daemonTestID(66): {Locations: []schema.BlobLocation{{PackID: daemonTestID(65), Offset: 0, Length: 1, Type: schema.BlobData}}},
	})}
	err = store.IngestLegacyPacks(ctx, session, 1, imports)
	if !errors.Is(err, ErrLegacyImportFreshRequired) {
		t.Fatalf("fresh guard ingest error = %v", err)
	}
}

func TestSchemaStoreLegacyCompleteSessionDeletesInBoundedPages(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-cleanup-bounded", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session := daemonTestID(207)
	source := daemonTestID(67)

	originalScanPage := legacyImportCleanupScanPageSize
	originalDeletePage := legacyImportCleanupDeletePageSize
	legacyImportCleanupScanPageSize = 3
	legacyImportCleanupDeletePageSize = 2
	t.Cleanup(func() {
		legacyImportCleanupScanPageSize = originalScanPage
		legacyImportCleanupDeletePageSize = originalDeletePage
	})

	for i := range 9 {
		packID := daemonTestID(byte(70 + i*2))
		blobID := daemonTestID(byte(71 + i*2))
		imports := []LegacyPackImport{legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
			blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: uint64(i), Length: 1, Type: schema.BlobData}}},
		})}
		batch := uint64(i + 1)
		if err := store.IngestLegacyPacks(ctx, session, batch, imports); err != nil {
			t.Fatal(err)
		}
		if err := store.ReduceLegacyImportBatch(ctx, session, batch, nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.CompleteLegacyImportSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	entries, done, err := store.ScanPrefix(ctx, schema.LegacyImportReceiptPrefix(session), nil, 32)
	if err != nil {
		t.Fatal(err)
	}
	if !done || len(entries) != 0 {
		t.Fatalf("cleanup left receipts: done=%t entries=%d", done, len(entries))
	}
}

func TestSchemaStoreLegacyIngestSinglePackExceedsPlanningBoundsCommits(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-single-pack-too-large", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session := daemonTestID(208)
	source, packID, blobID := daemonTestID(80), daemonTestID(81), daemonTestID(82)
	imports := []LegacyPackImport{legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
		blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: 0, Length: 1, Type: schema.BlobData}}},
	})}
	imports[0].TransactionBytes = 1

	if err := store.IngestLegacyPacks(ctx, session, 1, imports); err != nil {
		t.Fatalf("single-pack oversized ingest: %v", err)
	}
	if _, found, getErr := store.Get(ctx, schema.PackKey(packID)); getErr != nil || !found {
		t.Fatalf("oversized single-pack missing pack: found=%t err=%v", found, getErr)
	}
	if _, found, getErr := store.Get(ctx, schema.LegacyImportReceiptKey(session, 1)); getErr != nil || !found {
		t.Fatalf("oversized single-pack missing receipt: found=%t err=%v", found, getErr)
	}
	stats := store.LegacyImportStats()
	if stats.Commits != 1 || stats.IngestedBatches != 1 {
		t.Fatalf("oversized single-pack commit stats: %#v", stats)
	}
}

func legacyPackImport(source, packID schema.ID, blobs map[schema.ID]schema.BlobRecord) LegacyPackImport {
	blobCount := uint64(0)
	payload := uint64(0)
	for _, blob := range blobs {
		for _, location := range blob.Locations {
			blobCount++
			payload += uint64(location.Length)
		}
	}
	return LegacyPackImport{
		SourceIndex: source,
		PackID:      packID,
		Record: schema.PackRecord{
			Type:              schema.PackData,
			PhysicalSize:      payload + 2,
			PhysicalSizeKnown: true,
			PayloadSize:       payload,
			HeaderSize:        2,
			BlobCount:         blobCount,
			Lifecycle:         schema.PackImported,
		},
		Blobs: blobs,
	}
}
