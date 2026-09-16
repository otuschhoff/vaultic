package daemon

import (
	"bytes"
	"math"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestFreshImportLookupHintsNeverForgetCommittedIDs(t *testing.T) {
	packID, firstBlob, secondBlob := daemonTestID(1), daemonTestID(2), daemonTestID(3)
	store := &SchemaStore{freshImportSeen: newIDSeenFilter(100, 1024, 0.001)}
	imported := LegacyPackImport{PackID: packID, Blobs: map[schema.ID]schema.BlobRecord{
		firstBlob: {}, secondBlob: {},
	}}
	hints := store.freshImportLookupHintsForBatch([]LegacyPackImport{imported})
	if _, absent := hints.packsAbsent[packID]; !absent || len(hints.blobsAbsent) != 2 {
		t.Fatalf("initial fresh import hints = %#v", hints)
	}
	store.freshImportSeen.insert(packID)
	store.freshImportSeen.insert(firstBlob)
	hints = store.freshImportLookupHintsForBatch([]LegacyPackImport{imported})
	if _, absent := hints.packsAbsent[packID]; absent {
		t.Fatal("committed pack was reported absent")
	}
	if _, absent := hints.blobsAbsent[firstBlob]; absent {
		t.Fatal("committed blob was reported absent")
	}
	if _, absent := hints.blobsAbsent[secondBlob]; !absent {
		t.Fatal("new blob did not receive an absence hint")
	}
}

func TestIDSeenFilterRollsOverWithoutFalseNegatives(t *testing.T) {
	filter := newIDSeenFilter(2, 1024, 0.001)
	ids := []schema.ID{daemonTestID(1), daemonTestID(2), daemonTestID(3), daemonTestID(4), daemonTestID(5)}
	for _, id := range ids {
		filter.insert(id)
	}
	for _, id := range ids {
		if !filter.possiblyContains(id) {
			t.Fatalf("committed ID %x was reported absent", id)
		}
	}
	stats := filter.stats()
	if stats.Layers < 2 || stats.Inserts == 0 || stats.Inserts > uint64(len(ids)) || stats.Bytes == 0 || stats.FallbackToDatabase {
		t.Fatalf("unexpected scalable filter stats: %#v", stats)
	}
	if stats.EstimatedFalsePositive > 0.001 {
		t.Fatalf("estimated false-positive probability = %g", stats.EstimatedFalsePositive)
	}
}

func TestIDSeenFilterFallsBackSafelyAtMemoryLimit(t *testing.T) {
	filter := newIDSeenFilter(1, 2, 0.5)
	filter.insert(daemonTestID(1))
	filter.insert(daemonTestID(2))
	if stats := filter.stats(); !stats.FallbackToDatabase {
		t.Fatalf("filter did not enter database fallback: %#v", stats)
	}
	if !filter.possiblyContains(daemonTestID(99)) {
		t.Fatal("fallback reported an unknown ID definitely absent")
	}
}

func TestIDSeenFilterFallsBackWhenInitialLayerDoesNotFit(t *testing.T) {
	filter := newIDSeenFilter(1_000, 1, 0.001)
	if !filter.possiblyContains(daemonTestID(1)) {
		t.Fatal("memory-limited filter reported definitely absent")
	}
	filter.insert(daemonTestID(1))
	stats := filter.stats()
	if !stats.FallbackToDatabase || stats.Layers != 0 || stats.Bytes != 0 || stats.Inserts != 0 {
		t.Fatalf("initial fallback stats = %#v", stats)
	}
}

func TestIDSeenFilterDoesNotCountDuplicateInserts(t *testing.T) {
	filter := newIDSeenFilter(2, 1024, 0.001)
	id := daemonTestID(7)
	filter.insert(id)
	filter.insert(id)
	if stats := filter.stats(); stats.Inserts != 1 || stats.Layers != 1 {
		t.Fatalf("duplicate insert stats = %#v", stats)
	}
}

func TestIDSeenFilterInvalidParametersFallBackSafely(t *testing.T) {
	for _, falsePositive := range []float64{-1, 0, 1, 2, math.NaN(), math.Inf(1)} {
		filter := newIDSeenFilter(10, 1024, falsePositive)
		if !filter.possiblyContains(daemonTestID(1)) {
			t.Fatalf("false-positive target %g reported an unknown ID absent", falsePositive)
		}
		if stats := filter.stats(); !stats.FallbackToDatabase || stats.Layers != 0 || stats.Bytes != 0 {
			t.Fatalf("false-positive target %g stats = %#v", falsePositive, stats)
		}
	}
	if bytes, probes := seenFilterLayerParameters(0, 0.001); bytes != 0 || probes != 0 {
		t.Fatalf("zero-capacity parameters = bytes %d probes %d", bytes, probes)
	}
}

func TestIDSeenFilterExtremeParametersDoNotWrapProbeCount(t *testing.T) {
	bytes, probes := seenFilterLayerParameters(1, math.SmallestNonzeroFloat64)
	if bytes == 0 || probes != math.MaxUint8 {
		t.Fatalf("extreme parameters = bytes %d probes %d", bytes, probes)
	}
	bytes, probes = seenFilterLayerParameters(math.MaxUint64, math.SmallestNonzeroFloat64)
	if bytes != 0 || probes != 0 {
		t.Fatalf("overflowing parameters = bytes %d probes %d", bytes, probes)
	}
}

func TestIDSeenFilterProjectionSupportsFourHundredMillionIDs(t *testing.T) {
	const supportedIDs = uint64(400_000_000)
	var capacity, allocated uint64
	probabilityAbsent := 1.0
	for layer, layerCapacity := 0, uint64(freshImportInitialCapacity); capacity < supportedIDs; layer++ {
		falsePositive := freshImportFalsePositive / math.Pow(2, float64(layer+1))
		bytes, probes := seenFilterLayerParameters(layerCapacity, falsePositive)
		if bytes == 0 || probes == 0 {
			t.Fatalf("layer %d parameters = bytes %d probes %d", layer, bytes, probes)
		}
		allocated += bytes
		capacity += layerCapacity
		probabilityAbsent *= 1 - falsePositive
		layerCapacity *= seenFilterGrowth
	}
	if capacity < supportedIDs || allocated > freshImportSeenMaxBytes {
		t.Fatalf("projected capacity=%d bytes=%d", capacity, allocated)
	}
	if aggregate := 1 - probabilityAbsent; aggregate > freshImportFalsePositive {
		t.Fatalf("projected aggregate false-positive probability = %g", aggregate)
	}
}

func TestPrepareReconciledRevisionPlanInput(t *testing.T) {
	record := schema.InodeRevision{Known: schema.KnownUID, UID: 42, Freshness: schema.FreshnessVerified}
	value, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	reconciled := ReconciledRevision{
		CurrentKey: schema.CurrentInodeKey(3, 9), RevisionKey: schema.InodeRevisionKey(3, 9, 7),
		RevisionValue: value, Revision: 7,
	}
	input, err := prepareReconciledRevision(reconciled)
	if err != nil {
		t.Fatal(err)
	}
	if input.currentParsed.FSID != 3 || input.revisionParsed.Inode != 9 || input.revisionParsed.Revision != 7 {
		t.Fatalf("parsed reconciliation identity = %#v / %#v", input.currentParsed, input.revisionParsed)
	}
	pointer, err := schema.UnmarshalCurrentPointer(input.currentValue)
	if err != nil || pointer.Revision != 7 || !bytes.Equal(pointer.RecordKey, reconciled.RevisionKey) {
		t.Fatalf("current pointer = %#v, err=%v", pointer, err)
	}
	reconciled.DebtKeys = [][]byte{schema.BlobKey(daemonTestID(1))}
	if _, err := prepareReconciledRevision(reconciled); err == nil {
		t.Fatal("non-debt key accepted as reconciliation debt")
	}
}

func TestPreparePackImportCanonicalizesAndValidatesLocations(t *testing.T) {
	packID := daemonTestID(10)
	blobID := daemonTestID(11)
	imported := LegacyPackImport{
		PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, BlobCount: 2, Lifecycle: schema.PackExportPending},
		Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{
			{PackID: packID, Offset: 8, Length: 2, Type: schema.BlobData},
			{PackID: packID, Offset: 1, Length: 3, Type: schema.BlobData},
		}}},
	}
	if err := preparePackImport(&imported, false); err != nil {
		t.Fatal(err)
	}
	locations := imported.Blobs[blobID].Locations
	if locations[0].Offset != 1 || locations[1].Offset != 8 {
		t.Fatalf("locations were not canonicalized: %#v", locations)
	}
	imported.Blobs[blobID] = schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: daemonTestID(12), Length: 1, Type: schema.BlobData}}}
	if err := preparePackImport(&imported, false); err == nil {
		t.Fatal("foreign pack location was accepted")
	}
}

func TestPlanGDPRRevisionRedactionsSeparatesTargetAndRemaining(t *testing.T) {
	targetKey := schema.InodeRevisionKey(2, 4, 6)
	remainingKey := schema.InodeRevisionKey(2, 5, 7)
	contentID := daemonTestID(20)
	target := schema.InodeRevision{
		Known: schema.KnownUID | schema.KnownSize | schema.KnownPath, UID: 42, Size: 9, SourcePath: "/secret",
		ContentMode: schema.ContentInline, ContentCount: 1, ContentIDs: []schema.ID{contentID},
	}
	targetValue, err := target.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	remaining := schema.InodeRevision{Known: schema.KnownUID, UID: 7}
	remainingValue, err := remaining.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	inventory := gdprInventory{revisions: []gdprRevision{
		{key: targetKey, parsed: mustParseDaemonKey(t, targetKey), record: target, value: targetValue},
		{key: remainingKey, parsed: mustParseDaemonKey(t, remainingKey), record: remaining, value: remainingValue},
	}, manifests: map[schema.ID][]gdprManifestSegment{}}
	plan, err := planGDPRRevisionRedactions(inventory, 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.puts) != 1 || len(plan.deletes) != 1 || len(plan.remaining) != 1 || len(plan.purgedHashes) != 1 {
		t.Fatalf("redaction plan sizes: puts=%d deletes=%d remaining=%d hashes=%d",
			len(plan.puts), len(plan.deletes), len(plan.remaining), len(plan.purgedHashes))
	}
	redacted, err := schema.UnmarshalInodeRevision(plan.puts[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	redactedKnown := redacted.Known & (schema.KnownUID | schema.KnownSize | schema.KnownPath)
	if redactedKnown != 0 || redacted.ContentMode != schema.ContentNone || len(redacted.ContentIDs) != 0 {
		t.Fatalf("revision was not fully redacted: %#v", redacted)
	}
	if _, found := plan.affectedBlobs[contentID]; !found {
		t.Fatal("target content was not included in affected blobs")
	}
}

func TestPlanGDPRDirectoryRedactionsPrunesTargetChildren(t *testing.T) {
	targetKey := schema.InodeRevisionKey(1, 8, 2)
	directoryKey := schema.DirectoryRevisionKey(1, 3, 4)
	record := schema.DirectoryRevision{Children: []schema.DirectoryChild{
		{Name: "drop", Inode: 8, Type: schema.NodeFile, MetadataKey: targetKey},
		{Name: "keep", Inode: 9, Type: schema.NodeFile, MetadataKey: schema.InodeRevisionKey(1, 9, 2)},
	}}
	value, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	targets := gdprForgetPlan{
		targetKeys: map[string]struct{}{string(targetKey): {}}, targetRevisions: map[[3]uint64]struct{}{},
		targetPaths: map[string]struct{}{},
	}
	directories := []gdprDirectory{{
		key: directoryKey, parsed: mustParseDaemonKey(t, directoryKey), record: record, value: value,
	}}
	plan, err := planGDPRDirectoryRedactions(directories, 42, targets)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.puts) != 1 || len(plan.purgedHashes) != 1 {
		t.Fatalf("directory plan sizes: puts=%d hashes=%d", len(plan.puts), len(plan.purgedHashes))
	}
	updated, err := schema.UnmarshalDirectoryRevision(plan.puts[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.Children) != 1 || updated.Children[0].Name != "keep" {
		t.Fatalf("directory children = %#v", updated.Children)
	}
}

func mustParseDaemonKey(t *testing.T, key []byte) schema.ParsedKey {
	t.Helper()
	parsed, err := schema.ParseKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
