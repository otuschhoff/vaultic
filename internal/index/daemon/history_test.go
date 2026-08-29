package daemon

import (
	"context"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func historyTestStore(t *testing.T, repositoryID string) (*SchemaStore, context.Context) {
	t.Helper()
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: repositoryID, DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return NewSchemaStore(client), context.Background()
}

// readHistory returns every raw event in key order, which is time order then
// sequence order.
func readHistory(t *testing.T, store *SchemaStore, ctx context.Context) []schema.PackHistoryEvent {
	t.Helper()
	events, _ := readHistoryWithKeys(t, store, ctx)
	return events
}

func readHistoryWithKeys(t *testing.T, store *SchemaStore, ctx context.Context) ([]schema.PackHistoryEvent, []schema.ParsedKey) {
	t.Helper()
	var (
		events []schema.PackHistoryEvent
		keys   []schema.ParsedKey
		after  []byte
	)
	for {
		entries, done, err := store.ScanPrefix(ctx, []byte("ph:"), after, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			parsed, parseErr := schema.ParseKey(entry.Key)
			if parseErr != nil || parsed.Kind != schema.KeyPackHistory {
				t.Fatalf("unexpected history key %q: %v", entry.Key, parseErr)
			}
			record, decodeErr := schema.UnmarshalPackHistoryEvent(entry.Value)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			events = append(events, record)
			keys = append(keys, parsed)
			after = entry.Key
		}
		if done {
			return events, keys
		}
	}
}

func historyPack(id schema.ID, blob schema.ID, payload uint64, lifecycle schema.PackLifecycle) PublishedPack {
	return PublishedPack{
		PackID: id,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: payload, BlobCount: 1, Lifecycle: lifecycle},
		Blobs: map[schema.ID]schema.BlobRecord{
			blob: {Locations: []schema.BlobLocation{{PackID: id, Length: uint32(payload), Type: schema.BlobData}}},
		},
	}
}

// TestPackTransitionsRecordHistory asserts that every catalog transition
// leaves an event, and that the events describe the pack well enough to remain
// meaningful after its catalog record is gone.
func TestPackTransitionsRecordHistory(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-transitions")
	packID, blobID := daemonTestID(10), daemonTestID(11)

	if err := store.PublishPack(ctx, historyPack(packID, blobID, 12, schema.PackExportPending)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackPublished(ctx, packID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdatePackUsage(ctx, map[schema.ID]PackUsage{packID: {Used: 5, Unused: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeletePending(ctx, packID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeleted(ctx, packID, []schema.ID{blobID}); err != nil {
		t.Fatal(err)
	}

	events := readHistory(t, store, ctx)
	want := []schema.PackEventType{
		schema.EventCreated, schema.EventPublished, schema.EventUsageChanged,
		schema.EventDeletePending, schema.EventDeleted,
	}
	if len(events) != len(want) {
		t.Fatalf("events = %d, want %d: %+v", len(events), len(want), events)
	}
	for index, expected := range want {
		if events[index].Type != expected {
			t.Fatalf("event %d = %v, want %v", index, events[index].Type, expected)
		}
	}
	// The usage event carries the signed delta from unknown to the new split.
	if events[2].UsedDelta != 5 || events[2].UnusedDelta != 7 {
		t.Fatalf("usage deltas = %+v", events[2])
	}
	// The deletion event releases the accounted bytes and still describes the
	// pack, because its catalog record no longer exists.
	if events[4].PayloadSize != 12 || events[4].UsedDelta != -5 || events[4].UnusedDelta != -7 {
		t.Fatalf("deletion event = %+v", events[4])
	}
	if _, found, err := store.Get(ctx, schema.PackKey(packID)); err != nil || found {
		t.Fatalf("pack record still present: found=%t err=%v", found, err)
	}
}

// TestHistoryEventsSurviveTheirPack is the exit criterion's core requirement:
// history for a deleted pack must still be readable.
func TestHistoryEventsSurviveTheirPack(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-survives")
	packID, blobID := daemonTestID(20), daemonTestID(21)
	if err := store.PublishPack(ctx, historyPack(packID, blobID, 9, schema.PackExportPending)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackPublished(ctx, packID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeletePending(ctx, packID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeleted(ctx, packID, []schema.ID{blobID}); err != nil {
		t.Fatal(err)
	}

	_, keys := readHistoryWithKeys(t, store, ctx)
	var forPack int
	for _, key := range keys {
		if key.ID == packID {
			forPack++
		}
	}
	if forPack != 4 {
		t.Fatalf("events for deleted pack = %d, want 4", forPack)
	}
}

// TestRepackLineageIsRecordedAcrossGenerations proves that a rewrite is
// distinguishable from growth, and that lineage can be followed back through
// several repacks.
func TestRepackLineageIsRecordedAcrossGenerations(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-lineage")
	runID := daemonTestID(99)

	first, firstBlob := daemonTestID(30), daemonTestID(31)
	if err := store.PublishPack(ctx, historyPack(first, firstBlob, 10, schema.PackExportPending)); err != nil {
		t.Fatal(err)
	}
	second, secondBlob := daemonTestID(32), daemonTestID(33)
	generationTwo := historyPack(second, secondBlob, 10, schema.PackExportPending)
	generationTwo.PredecessorPackIDs, generationTwo.RunID = []schema.ID{first}, runID
	if err := store.PublishPack(ctx, generationTwo); err != nil {
		t.Fatal(err)
	}
	third, thirdBlob := daemonTestID(34), daemonTestID(35)
	generationThree := historyPack(third, thirdBlob, 10, schema.PackExportPending)
	generationThree.PredecessorPackIDs, generationThree.RunID = []schema.ID{second}, runID
	if err := store.PublishPack(ctx, generationThree); err != nil {
		t.Fatal(err)
	}

	events, keys := readHistoryWithKeys(t, store, ctx)
	lineage := make(map[schema.ID]schema.ID, 2)
	var created, repacked int
	for index, event := range events {
		switch event.Type {
		case schema.EventCreated:
			created++
		case schema.EventRepackedInto:
			repacked++
			if len(event.PredecessorPackIDs) != 1 {
				t.Fatalf("repack event missing lineage: %+v", event)
			}
			if event.RunID != runID {
				t.Fatalf("repack event lost its run ID: %+v", event)
			}
			lineage[keys[index].ID] = event.PredecessorPackIDs[0]
		}
	}
	if created != 1 || repacked != 2 {
		t.Fatalf("created=%d repacked=%d, want 1 and 2", created, repacked)
	}
	// Walking predecessors must reach the original pack.
	if lineage[third] != second || lineage[second] != first {
		t.Fatalf("lineage not reconstructable: %+v", lineage)
	}
}

// TestHistoryKeysAreUniqueAndOrderedUnderConcurrentWriters exercises the
// global sequence: without it, two writers recording events for different
// packs in the same second could produce colliding or unordered keys.
func TestHistoryKeysAreUniqueAndOrderedUnderConcurrentWriters(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-concurrent")
	const writers = 8

	var wait sync.WaitGroup
	errs := make(chan error, writers)
	for index := range writers {
		wait.Add(1)
		go func(offset int) {
			defer wait.Done()
			packID, blobID := daemonTestID(byte(40+offset)), daemonTestID(byte(80+offset))
			if err := store.PublishPack(ctx, historyPack(packID, blobID, 8, schema.PackExportPending)); err != nil {
				errs <- err
				return
			}
			errs <- store.MarkPackPublished(ctx, packID)
		}(index)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	_, keys := readHistoryWithKeys(t, store, ctx)
	if len(keys) != writers*2 {
		t.Fatalf("events = %d, want %d", len(keys), writers*2)
	}
	seen := make(map[uint64]struct{}, len(keys))
	var previous uint64
	for index, key := range keys {
		if _, duplicate := seen[key.Revision]; duplicate {
			t.Fatalf("duplicate event sequence %d", key.Revision)
		}
		seen[key.Revision] = struct{}{}
		// Scan order is key order, so sequences must increase monotonically.
		if index > 0 && key.Revision <= previous {
			t.Fatalf("event sequence went backwards: %d after %d", key.Revision, previous)
		}
		previous = key.Revision
	}
}

// TestHistoryRecordsCollectionEnabledMarker checks that the first event also
// records when collection began, which is what later marks earlier buckets
// reconstructed rather than complete.
func TestHistoryRecordsCollectionEnabledMarker(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-enabled-marker")
	if _, found, err := store.Get(ctx, schema.HistoryEnabledAtKey()); err != nil || found {
		t.Fatalf("marker present before any event: found=%t err=%v", found, err)
	}
	packID, blobID := daemonTestID(60), daemonTestID(61)
	if err := store.PublishPack(ctx, historyPack(packID, blobID, 4, schema.PackExportPending)); err != nil {
		t.Fatal(err)
	}
	value, found, err := store.Get(ctx, schema.HistoryEnabledAtKey())
	if err != nil || !found {
		t.Fatalf("marker missing after first event: found=%t err=%v", found, err)
	}
	marker, err := schema.UnmarshalHistoryMarker(value)
	if err != nil || marker.UnixSeconds == 0 {
		t.Fatalf("marker = %+v, %v", marker, err)
	}
}

// TestPackHistoryIsAppendOnlyThroughBatches guards the invariant that raw
// events may only be written by the transition that produced them.
func TestPackHistoryIsAppendOnlyThroughBatches(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-append-only")
	event := schema.PackHistoryEvent{Type: schema.EventCreated, PackType: schema.PackData}
	encoded, err := event.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	key := schema.PackHistoryKey(1700000000, 1, daemonTestID(70))
	if err := store.WriteMutableBatch(ctx, []Mutation{{Key: key, Value: encoded}}, nil, true); err == nil {
		t.Fatal("history event was rewritable through a mutable batch")
	}
	// Retention must still be able to remove them.
	if err := store.WriteMutableBatch(ctx, nil, [][]byte{key}, true); err != nil {
		t.Fatalf("history event could not be pruned: %v", err)
	}
}

// TestUsageEventsAreCoalescedPerPack asserts one usage event per pack per
// update, regardless of how many blobs changed reachability. Emitting one per
// blob would make the log grow with the blob index rather than with events.
func TestUsageEventsAreCoalescedPerPack(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-coalesce")
	packID := daemonTestID(120)
	blobs := map[schema.ID]schema.BlobRecord{}
	var payload uint64
	for index := range 6 {
		blobID := daemonTestID(byte(130 + index))
		blobs[blobID] = schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: packID, Offset: payload, Length: 10, Type: schema.BlobData}}}
		payload += 10
	}
	published := PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: payload, BlobCount: uint64(len(blobs)), Lifecycle: schema.PackExportPending},
		Blobs:  blobs,
	}
	if err := store.PublishPack(ctx, published); err != nil {
		t.Fatal(err)
	}
	before := len(readHistory(t, store, ctx))
	if _, err := store.UpdatePackUsage(ctx, map[schema.ID]PackUsage{packID: {Used: 20, Unused: payload - 20}}); err != nil {
		t.Fatal(err)
	}
	events := readHistory(t, store, ctx)
	var usage int
	for _, event := range events[before:] {
		if event.Type == schema.EventUsageChanged {
			usage++
		}
	}
	if usage != 1 {
		t.Fatalf("usage events = %d for %d blobs, want exactly 1", usage, len(blobs))
	}
}

// TestTierChangeIsRecorded covers the transition where an imported pack's tier
// becomes known once vaultic publishes it.
func TestTierChangeIsRecorded(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-tier-change")
	packID, blobID, sourceIndex := daemonTestID(140), daemonTestID(141), daemonTestID(142)
	imported := LegacyPackImport{
		PackID: packID, SourceIndex: sourceIndex,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 6, BlobCount: 1, Lifecycle: schema.PackImported},
		Blobs:  map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 6, Type: schema.BlobData}}}},
	}
	if err := store.ImportLegacyPack(ctx, imported); err != nil {
		t.Fatal(err)
	}
	routed := PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 6, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
		Blobs:  imported.Blobs,
	}
	if err := store.PublishPack(ctx, routed); err != nil {
		t.Fatal(err)
	}

	var tierChanges int
	for _, event := range readHistory(t, store, ctx) {
		if event.Type == schema.EventTierChanged {
			tierChanges++
			if event.ReasonCode != "cold" {
				t.Fatalf("tier change reason = %q, want the new tier", event.ReasonCode)
			}
		}
	}
	if tierChanges != 1 {
		t.Fatalf("tier change events = %d, want 1", tierChanges)
	}
}

// TestRecordPackEventsAppendsObservations covers the standalone path used for
// repack sources, failed deletions, and orphans, which are observations rather
// than catalog transitions.
func TestRecordPackEventsAppendsObservations(t *testing.T) {
	store, ctx := historyTestStore(t, "phase10-observations")
	packID := daemonTestID(90)
	events := []PackEvent{
		{PackID: packID, Record: schema.PackHistoryEvent{Type: schema.EventRepackedFrom, PackType: schema.PackData, ReasonCode: "mixed_pack_repack"}},
		{PackID: packID, Record: schema.PackHistoryEvent{Type: schema.EventDeleteFailed, PackType: schema.PackData, ReasonCode: "backend_error"}},
		{PackID: packID, Record: schema.PackHistoryEvent{Type: schema.EventOrphanDetected, PackType: schema.PackUnknown}},
	}
	if err := store.RecordPackEvents(ctx, events); err != nil {
		t.Fatal(err)
	}
	recorded := readHistory(t, store, ctx)
	if len(recorded) != 3 {
		t.Fatalf("recorded = %d, want 3", len(recorded))
	}
	if recorded[0].Type != schema.EventRepackedFrom || recorded[1].Type != schema.EventDeleteFailed || recorded[2].Type != schema.EventOrphanDetected {
		t.Fatalf("observations = %+v", recorded)
	}
	if recorded[1].ReasonCode != "backend_error" {
		t.Fatalf("reason code lost: %+v", recorded[1])
	}
}
