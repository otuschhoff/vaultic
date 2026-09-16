package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestLegacyImportOperationStatsConcurrentSnapshots(t *testing.T) {
	store := &SchemaStore{}
	const workers = 8
	const observations = 1000
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for range observations {
				store.legacyMetrics.operations.ingestBegin.observe(8 * time.Nanosecond)
				_ = store.LegacyImportStats()
			}
		}()
	}
	group.Wait()
	stats := store.LegacyImportStats()
	distribution := stats.Operations["ingest_begin"]
	if distribution.Count != workers*observations || distribution.Sum != workers*observations*8*time.Nanosecond ||
		distribution.P50 != 15*time.Nanosecond || distribution.P95 != 15*time.Nanosecond || distribution.P99 != 15*time.Nanosecond {
		t.Fatalf("begin distribution = %+v", distribution)
	}
	stats.Operations["ingest_begin"] = DurationDistribution{}
	if store.LegacyImportStats().Operations["ingest_begin"].Count != workers*observations {
		t.Fatal("operation snapshot aliases metric state")
	}
}

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
	stats := stage3.LegacyImportStats()
	if stats.Attempts != 2 || stats.IngestAttempts != 2 || stats.ReduceAttempts != 2 || stats.IngestFailures != 0 || stats.ReduceFailures != 0 {
		t.Fatalf("stage 3 phase counters = %#v", stats)
	}
	if stats.ReceiptReads != 2 || stats.ReductionReceiptReads != 2 || stats.ReductionMutationRPCs == 0 || stats.ReductionMutations == 0 {
		t.Fatalf("stage 3 read/write counters = %#v", stats)
	}
	if stats.MutationRPCAttempts != stats.MutationRPCs || stats.ReductionMutationRPCAttempts != stats.ReductionMutationRPCs {
		t.Fatalf("successful stage 3 RPC attempt counters = %#v", stats)
	}
	if stats.CatalogReadRPCs == 0 || stats.CatalogReadKeys == 0 ||
		stats.ReductionPlanReadRPCs == 0 || stats.ReductionPlanReadKeys == 0 {
		t.Fatalf("stage 3 actual read counters = %#v", stats)
	}
	for _, operation := range []string{
		"prepare", "hash", "hints", "ingest_begin", "receipt_read", "pack_read", "blob_read", "plan_build",
		"mutation_rpc", "commit", "post_commit", "reduce_receipt_read", "reduce_aggregate_plan",
		"reduce_history_plan", "reduce_begin", "reduce_encode", "reduce_mutation_rpc", "reduce_commit",
	} {
		if stats.Operations[operation].Count == 0 {
			t.Errorf("operation %q was not observed: %#v", operation, stats.Operations)
		}
	}
}

func TestSchemaStoreLegacySplitValidationFailuresAreCounted(t *testing.T) {
	store := &SchemaStore{}
	if err := store.IngestLegacyPacks(context.Background(), schema.ID{}, 1, nil); err == nil {
		t.Fatal("zero-session ingest succeeded")
	}
	if err := store.ReduceLegacyImportBatch(context.Background(), schema.ID{}, 1, nil); err == nil {
		t.Fatal("zero-session reduction succeeded")
	}
	stats := store.LegacyImportStats()
	if stats.Batches != 2 || stats.IngestFailures != 1 || stats.ReduceFailures != 1 ||
		stats.IngestAttempts != 0 || stats.ReduceAttempts != 0 {
		t.Fatalf("validation failure counters = %#v", stats)
	}
}

func TestSchemaStoreLegacySplitCancellationAndFailedRPCAttemptAreCounted(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-stage3-cancellation", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	calls, err := writeTransactionBatchesMeasured(canceled, transaction, client.Limits(), []Mutation{{
		Key: schema.ImportCheckpointKey(daemonTestID(91)), Value: []byte("value"),
	}}, nil)
	rollbackTransaction(ctx, transaction)
	if err == nil || calls.attempted != 1 || calls.succeeded != 0 {
		t.Fatalf("canceled mutation calls = %#v, error = %v", calls, err)
	}

	transaction, err = client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([][]byte, int(client.Limits().MaxBatchItems)+1)
	for index := range keys {
		keys[index] = schema.PackKey(daemonTestID(byte(index + 1)))
	}
	_, _, readRPCs, readKeys, err := legacyMultiGet(canceled, transaction, keys)
	rollbackTransaction(ctx, transaction)
	if err == nil || readRPCs != 1 || readKeys == 0 || readKeys >= uint64(len(keys)) {
		t.Fatalf("canceled catalog reads: rpcs=%d keys=%d planned=%d error=%v", readRPCs, readKeys, len(keys), err)
	}

	transaction, err = client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var historyReads uint64
	_, err = packHistoryMutationsMeasured(canceled, transaction, []PackEvent{{PackID: daemonTestID(90)}}, func() {
		historyReads++
	})
	rollbackTransaction(ctx, transaction)
	if err == nil || historyReads != 1 {
		t.Fatalf("canceled history reads = %d, error = %v", historyReads, err)
	}

	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	session, source, packID := daemonTestID(92), daemonTestID(93), daemonTestID(94)
	err = store.IngestLegacyPacks(canceled, session, 1, []LegacyPackImport{legacyPackImport(source, packID, nil)})
	if err == nil {
		t.Fatal("canceled ingest succeeded")
	}
	stats := store.LegacyImportStats()
	if stats.IngestFailures != 1 || stats.IngestAttempts != 0 || stats.Operations["prepare"].Count != 1 {
		t.Fatalf("canceled ingest counters = %#v", stats)
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
	if err := store.IngestLegacyPacks(ctx, session, 2, imports); err != nil {
		t.Fatalf("new batch catalog replay failed: %v", err)
	}

	conflicting := append([]LegacyPackImport(nil), imports...)
	conflicting[0].Blobs = map[schema.ID]schema.BlobRecord{
		blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: 2, Length: 3, Type: schema.BlobData}}},
	}
	err = store.IngestLegacyPacks(ctx, session, 1, conflicting)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting ingest error = %v", err)
	}
	if stats := store.LegacyImportStats(); stats.IngestFailures != 1 || stats.IngestAttempts != 4 ||
		stats.CatalogReadRPCs == 0 || stats.CatalogReadKeys == 0 {
		t.Fatalf("conflicting ingest counters = %#v", stats)
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
	if stats := store.LegacyImportStats(); stats.ReduceFailures != 1 || stats.ReduceAttempts != 2 ||
		stats.ReduceCheckpointReads != 1 || stats.Operations["reduce_checkpoint_read"].Count != 1 {
		t.Fatalf("missing receipt counters = %#v", stats)
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
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprintf("deferred=%t", deferred), func(t *testing.T) {
			testLegacyCompleteSessionDeletesInBoundedPages(t, deferred)
		})
	}
}

func testLegacyCompleteSessionDeletesInBoundedPages(t *testing.T, deferred bool) {
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
	if deferred {
		if err := store.EnableDeferredLegacyImportCleanup(); err != nil {
			t.Fatal(err)
		}
	}
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
	stats := store.LegacyImportStats()
	if stats.CleanupCalls != 1 || stats.CleanupPages != 5 || stats.CleanupReceipts != 9 {
		t.Fatalf("cleanup counters: %+v", stats)
	}
	if stats.CleanupTime <= 0 || stats.CleanupScanTime <= 0 || stats.CleanupCommitTime <= 0 {
		t.Fatalf("missing cleanup timings: %+v", stats)
	}
	if (deferred && stats.CleanupDeferredCommits != 5) || (!deferred && stats.CleanupDeferredCommits != 0) {
		t.Fatalf("cleanup durability counters: %+v", stats)
	}
	if err := store.CompleteLegacyImportSession(ctx, session); err != nil {
		t.Fatalf("cleanup replay: %v", err)
	}
	if _, found, err := client.Get(ctx, []byte(bulkImportCompleteKey), ""); err != nil || found {
		t.Fatalf("cleanup authorized incomplete import: found=%t err=%v", found, err)
	}
}

func TestSchemaStoreDeferredCleanupRequiresFreshImport(t *testing.T) {
	store := &SchemaStore{}
	if err := store.EnableDeferredLegacyImportCleanup(); !errors.Is(err, ErrLegacyImportFreshRequired) {
		t.Fatalf("deferred cleanup fresh guard: %v", err)
	}
}

func TestSchemaStoreDeferredCleanupSurvivesBulkImportHandoff(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "phase32-deferred-cleanup-handoff",
		DaemonPath: daemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "memory", WALDataDir: directory + "/wal", WALFlushInterval: 500 * time.Millisecond,
		MaxUnflushedBytes: 16 << 30, L0SSTSizeBytes: 256 << 20,
		RebuildReset: true, FreshBulkImport: true,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	if err := store.EnableDeferredLegacyImportCleanup(); err != nil {
		t.Fatal(err)
	}
	source, session, packID, blobID := daemonTestID(220), daemonTestID(221), daemonTestID(222), daemonTestID(223)
	imports := []LegacyPackImport{legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
		blobID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 1, Type: schema.BlobData}}},
	})}
	checkpoint := Mutation{Key: schema.ImportCheckpointKey(source), Value: encodeSchemaRecord(t, schema.ImportCheckpointRecord{PacksImported: 1, BlobsImported: 1})}
	if err := store.IngestLegacyPacks(ctx, session, 1, imports); err != nil {
		t.Fatal(err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 1, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteLegacyImportSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	if stats := store.LegacyImportStats(); stats.CleanupDeferredCommits != 1 {
		t.Fatalf("cleanup durability stats: %+v", stats)
	}
	if err := store.MarkBulkImportComplete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.WALStore = "local"
	options.RebuildReset = false
	options.FreshBulkImport = false
	reopened, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(ctx)
	if _, found, err := NewSchemaStore(reopened).Get(ctx, checkpoint.Key); err != nil || !found {
		t.Fatalf("reopen checkpoint: found=%t err=%v", found, err)
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
