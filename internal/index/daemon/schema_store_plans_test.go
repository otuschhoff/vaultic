package daemon

import (
	"bytes"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

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
