package schema

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func testID(value byte) ID { var id ID; id[0] = value; id[31] = value; return id }

type binaryRecord interface{ MarshalBinary() ([]byte, error) }

func roundTrip[T any](t *testing.T, input T, decode func([]byte) (T, error)) {
	t.Helper()
	encoded, err := any(input).(binaryRecord).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	output, err := decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(output, input) {
		t.Fatalf("round trip mismatch:\n got %#v\nwant %#v", output, input)
	}
	for size := range len(encoded) {
		if _, err := decode(encoded[:size]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("truncation at %d returned %v", size, err)
		}
	}
	if _, err := decode(append(encoded, 0)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("trailing data returned %v", err)
	}
}

func TestEveryKeyNamespaceRoundTrips(t *testing.T) {
	id, second := testID(1), testID(2)
	keys := []struct {
		key  []byte
		kind KeyKind
	}{
		{BlobKey(id), KeyBlob}, {PackKey(id), KeyPack}, {PackAggregateKey(AggregateData), KeyPackAggregate},
		{PackAggregateKey(AggregateTree), KeyPackAggregate}, {PackAggregateKey(AggregateMixed), KeyPackAggregate}, {PackAggregateKey(AggregateUnknown), KeyPackAggregate}, {PackAggregateKey(AggregateAll), KeyPackAggregate},
		{CurrentInodeKey(3, 4), KeyCurrentInode}, {InodeRevisionKey(3, 4, 5), KeyInodeRevision},
		{CurrentDirectoryKey(3, 4), KeyCurrentDirectory}, {DirectoryRevisionKey(3, 4, 5), KeyDirectoryRevision},
		{SnapshotKey(id), KeySnapshot}, {ContentManifestKey(id, 7), KeyContentManifest}, {ReverseManifestKey(id, second), KeyReverseManifest},
		{ReverseInodeKey(id, 3, 4), KeyReverseInode}, {ReferenceCountKey(id), KeyReferenceCount},
		{GarbageCollectionKey(GCBlob, id), KeyGarbageCollection}, {GarbageCollectionKey(GCPack, id), KeyGarbageCollection},
		{CrawlDebtKey(id, second), KeyCrawlDebt}, {ImportCheckpointKey(id), KeyImportCheckpoint},
		{SnapshotImportCheckpointKey(id), KeySnapshotImportCheckpoint}, {NextRevisionKey(), KeyNextRevision},
	}
	for _, test := range keys {
		parsed, err := ParseKey(test.key)
		if err != nil {
			t.Fatalf("parse %x: %v", test.key, err)
		}
		if parsed.Kind != test.kind {
			t.Fatalf("parse %x kind %v, want %v", test.key, parsed.Kind, test.kind)
		}
	}
	for _, malformed := range [][]byte{nil, {}, []byte("b:"), append(BlobKey(id), 0), []byte("a:pack:bogus"), []byte("gc:x:")} {
		if _, err := ParseKey(malformed); !errors.Is(err, ErrMalformed) {
			t.Fatalf("ParseKey(%x) = %v", malformed, err)
		}
	}
}

func TestSchemaRejectsSemanticKeyAndStateMismatches(t *testing.T) {
	id := testID(1)
	for _, pointer := range []CurrentPointer{
		{Revision: 1, RecordKey: BlobKey(id)},
		{Revision: 1, RecordKey: InodeRevisionKey(1, 2, 3)},
	} {
		if _, err := pointer.MarshalBinary(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid current pointer returned %v", err)
		}
	}

	for _, child := range []DirectoryChild{
		{Name: "file", Inode: 2, Type: NodeFile, MetadataKey: DirectoryRevisionKey(1, 2, 1)},
		{Name: "directory", Inode: 2, Type: NodeDirectory, MetadataKey: InodeRevisionKey(1, 2, 1)},
	} {
		if _, err := (DirectoryRevision{Children: []DirectoryChild{child}}).MarshalBinary(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("mismatched directory child returned %v", err)
		}
	}

	validDirectory := DirectoryRevision{Children: []DirectoryChild{{Name: "x", Inode: 2, Type: NodeFile, MetadataKey: InodeRevisionKey(1, 2, 1)}}}
	encodedDirectory, err := validDirectory.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	encodedDirectory[26] = byte(NodeDirectory)
	if _, err := UnmarshalDirectoryRevision(encodedDirectory); !errors.Is(err, ErrMalformed) {
		t.Fatalf("mismatched decoded directory child returned %v", err)
	}

	if key := GarbageCollectionKey(GCTarget(99), id); key != nil {
		t.Fatalf("invalid garbage-collection target produced key %x", key)
	}

	invalidKnown := InodeRevision{Known: 1 << 8, ContentMode: ContentNone, Freshness: FreshnessUnknown}
	if _, err := invalidKnown.MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("undefined known-field bit returned %v", err)
	}
	encodedInode, err := (InodeRevision{ContentMode: ContentNone}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(encodedInode[45:49], 1<<8)
	if _, err := UnmarshalInodeRevision(encodedInode); !errors.Is(err, ErrMalformed) {
		t.Fatalf("undefined decoded known-field bit returned %v", err)
	}

	inlineIDs := make([]ID, MaxInlineContentIDs+1)
	for index := range inlineIDs {
		inlineIDs[index] = testID(byte(index))
	}
	invalidContent := []InodeRevision{
		{ContentMode: ContentNone, ContentCount: 1},
		{ContentMode: ContentInline, ContentCount: uint32(len(inlineIDs)), ContentIDs: inlineIDs},
		{ContentMode: ContentInline, ContentCount: 1, ContentIDs: []ID{id}, ContentManifestID: id},
		{ContentMode: ContentManifestRef, ContentCount: MaxInlineContentIDs, ContentManifestID: id},
		{ContentMode: ContentManifestRef, ContentCount: MaxInlineContentIDs + 1},
		{ContentMode: ContentManifestRef, ContentCount: MaxContentIDs + 1, ContentManifestID: id},
	}
	for _, inode := range invalidContent {
		if _, err := inode.MarshalBinary(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid content state %#v returned %v", inode, err)
		}
	}
}

func TestValidateValueMatchesKeyFamily(t *testing.T) {
	id := testID(1)
	pack, _ := (PackRecord{Type: PackData, Lifecycle: PackImported}).MarshalBinary()
	if err := ValidateValue(PackKey(id), pack); err != nil {
		t.Fatal(err)
	}
	if err := ValidateValue(SnapshotKey(id), pack); !errors.Is(err, ErrMalformed) {
		t.Fatalf("mismatched record family returned %v", err)
	}
	pointer, _ := (CurrentPointer{Revision: 1, RecordKey: InodeRevisionKey(2, 3, 1)}).MarshalBinary()
	if err := ValidateValue(CurrentInodeKey(2, 4), pointer); !errors.Is(err, ErrMalformed) {
		t.Fatalf("mismatched current pointer returned %v", err)
	}
	segment, _ := (ContentManifest{TotalCount: 1, Segment: 0, SegmentCount: 1, ContentIDs: []ID{id}}).MarshalBinary()
	if err := ValidateValue(ContentManifestKey(id, 1), segment); !errors.Is(err, ErrMalformed) {
		t.Fatalf("mismatched content segment returned %v", err)
	}
	directory, _ := (DirectoryRevision{Children: []DirectoryChild{{Name: "foreign", Inode: 2, Type: NodeFile, MetadataKey: InodeRevisionKey(2, 2, 1)}}}).MarshalBinary()
	if err := ValidateValue(DirectoryRevisionKey(1, 1, 1), directory); !errors.Is(err, ErrMalformed) {
		t.Fatalf("cross-filesystem file child returned %v", err)
	}
}

func TestEncodedValuesStayWithinTransportLimit(t *testing.T) {
	oversizedField := make([]byte, MaxEncodedValueBytes)
	if _, err := (CrawlDebtRecord{PathOrTree: oversizedField, Reason: DebtMissingInode, Status: DebtPending}).MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized encoded record returned %v", err)
	}
	if _, err := UnmarshalPackAggregate(make([]byte, MaxEncodedValueBytes+1)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized decoded record returned %v", err)
	}
}

func TestSchemaRecordRoundTripsAndMalformedInput(t *testing.T) {
	id1, id2, id3 := testID(1), testID(2), testID(3)
	roundTrip(t, BlobRecord{Locations: []BlobLocation{{PackID: id1, Offset: 9, Length: 10, UncompressedSize: 11, Type: BlobData}, {PackID: id2, Offset: 12, Length: 13, UncompressedSize: 14, Type: BlobTree}}}, UnmarshalBlobRecord)
	packRecord := PackRecord{Type: PackMixed, PhysicalSize: 100, PhysicalSizeKnown: true, PayloadSize: 80, HeaderSize: 20, BlobCount: 2, CreationTime: 123, CreationTimeKnown: true, Lifecycle: PackPublished, SourceIndexIDs: []ID{id1, id2}}
	encodedPack, err := packRecord.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decodedPack, err := UnmarshalPackRecord(encodedPack)
	if err != nil || !reflect.DeepEqual(decodedPack, packRecord) {
		t.Fatalf("pack round trip = %#v, %v", decodedPack, err)
	}
	legacyPack, err := UnmarshalPackRecord(encodedPack[:len(encodedPack)-1])
	if err != nil || !legacyPack.PhysicalSizeKnown || !reflect.DeepEqual(legacyPack, packRecord) {
		t.Fatalf("legacy pack decode = %#v, %v", legacyPack, err)
	}
	for size := range len(encodedPack) - 1 {
		if _, err := UnmarshalPackRecord(encodedPack[:size]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("pack truncation at %d returned %v", size, err)
		}
	}
	if _, err := UnmarshalPackRecord(append(encodedPack, 0)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("pack trailing data returned %v", err)
	}
	for _, invalid := range []PackRecord{
		{Type: PackData, PhysicalSize: 10, PayloadSize: 8, HeaderSize: 2, Lifecycle: PackImported},
		{Type: PackData, PhysicalSize: 7, PhysicalSizeKnown: true, PayloadSize: 8, HeaderSize: 1, Lifecycle: PackImported},
		{Type: PackData, PhysicalSize: 10, PhysicalSizeKnown: true, PayloadSize: 8, HeaderSize: 1, Lifecycle: PackImported},
	} {
		if _, err := invalid.MarshalBinary(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid pack size state %#v returned %v", invalid, err)
		}
	}
	roundTrip(t, PackAggregate{PackCount: 1, PhysicalSize: 2, PayloadSize: 3, HeaderSize: 4, BlobCount: 5, UpdateSequence: 6}, UnmarshalPackAggregate)
	roundTrip(t, CurrentPointer{Revision: 7, RecordKey: InodeRevisionKey(2, 3, 7)}, UnmarshalCurrentPointer)
	roundTrip(t, InodeRevision{ParentInode: 1, HasMultipleParents: true, MTime: -2, CTime: 3, Size: 4, Mode: 0o644, UID: 5, GID: 6, Known: KnownMTime | KnownParent | KnownPath, ContentMode: ContentInline, ContentIDs: []ID{id1, id2}, ContentCount: 2, FileContentHash: id3, HashKnown: true, SourcePath: "dir/file", Freshness: FreshnessVerified}, UnmarshalInodeRevision)
	directory := DirectoryRevision{
		ParentInode: 1, Children: []DirectoryChild{{Name: "a", Inode: 8, Type: NodeFile, MetadataKey: InodeRevisionKey(2, 8, 9)}, {Name: "b", Inode: 9, Type: NodeDirectory, MetadataKey: DirectoryRevisionKey(2, 9, 10)}},
		MTime: -2, CTime: 3, Mode: 0o755, UID: 5, GID: 6, Known: KnownMTime | KnownCTime | KnownMode | KnownUID | KnownGID | KnownParent | KnownPath,
		SourcePath: "dir", Freshness: FreshnessVerified,
	}
	encodedDirectory, err := directory.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decodedDirectory, err := UnmarshalDirectoryRevision(encodedDirectory)
	if err != nil || !reflect.DeepEqual(decodedDirectory, directory) {
		t.Fatalf("directory round trip = %#v, %v", decodedDirectory, err)
	}
	legacyDirectory := legacyDirectoryEncoding(t, directory)
	decodedLegacyDirectory, err := UnmarshalDirectoryRevision(legacyDirectory)
	if err != nil || decodedLegacyDirectory.Freshness != FreshnessUnknown || decodedLegacyDirectory.Known != 0 || !reflect.DeepEqual(decodedLegacyDirectory.Children, directory.Children) {
		t.Fatalf("legacy directory decode = %#v, %v", decodedLegacyDirectory, err)
	}
	for size := range len(encodedDirectory) {
		if size == len(legacyDirectory) {
			continue
		}
		if _, err := UnmarshalDirectoryRevision(encodedDirectory[:size]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("directory truncation at %d returned %v", size, err)
		}
	}
	if _, err := UnmarshalDirectoryRevision(append(encodedDirectory, 0)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("directory trailing data returned %v", err)
	}
	json := []byte(`{"time":"now"}`)
	roundTrip(t, SnapshotRecord{CommitSequence: 11, RootFSID: 2, RootInode: 9, RootRevision: 10, OriginalJSON: json, JSONHash: ID(sha256.Sum256(json))}, UnmarshalSnapshotRecord)
	roundTrip(t, ContentManifest{TotalCount: 2, Segment: 0, SegmentCount: 1, ContentIDs: []ID{id1, id2}}, UnmarshalContentManifest)
	roundTrip(t, ReverseManifestRecord{Segment: 2, State: ReferenceHistorical}, UnmarshalReverseManifestRecord)
	roundTrip(t, ReverseInodeRecord{LatestRevision: 12, State: ReferenceCurrent}, UnmarshalReverseInodeRecord)
	roundTrip(t, ReferenceCountRecord{TotalReferences: 1, DistinctInodes: 2, DistinctRevisions: 3, DistinctManifests: 4, ReachableSnapshots: 5, UpdateSequence: 6}, UnmarshalReferenceCountRecord)
	roundTrip(t, GarbageCollectionRecord{State: GCPendingRevalidation, ObservedCommit: 3, DiscoveredUnixNano: -4, RevalidatedCommit: 5}, UnmarshalGarbageCollectionRecord)
	roundTrip(t, CrawlDebtRecord{SourceIndexOrPack: id1, SourceKnown: true, PathOrTree: []byte("path"), Reason: DebtUnknownFreshness, RetryCount: 2, LastAttemptUnixNano: 3, Status: DebtPending, ErrorClass: "temporary"}, UnmarshalCrawlDebtRecord)
	roundTrip(t, ImportCheckpointRecord{PacksImported: 2, BlobsImported: 3, ErrorsSeen: 4}, UnmarshalImportCheckpointRecord)
	roundTrip(t, SnapshotImportCheckpointRecord{TreesVisited: 2, NodesImported: 3, DebtsCreated: 4}, UnmarshalSnapshotImportCheckpointRecord)
	encoded, err := MarshalNextRevision(42)
	if err != nil {
		t.Fatal(err)
	}
	next, err := UnmarshalNextRevision(encoded)
	if err != nil || next != 42 {
		t.Fatalf("next revision = %d, %v", next, err)
	}
}

func legacyDirectoryEncoding(t *testing.T, record DirectoryRevision) []byte {
	t.Helper()
	encoder := newEncoder()
	encoder.u64(record.ParentInode)
	encoder.u32(uint32(len(record.Children)))
	for _, child := range record.Children {
		if err := encoder.string(child.Name); err != nil {
			t.Fatal(err)
		}
		encoder.u64(child.Inode)
		encoder.u8(byte(child.Type))
		if err := encoder.bytes(child.MetadataKey); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := encoder.finish()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestContentSegmentationIsOrderedImmutableAndDeterministic(t *testing.T) {
	ids := []ID{testID(1), testID(2), testID(3), testID(4), testID(5)}
	manifestID, segments, err := SegmentContent(ids, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 3 {
		t.Fatalf("got %d segments", len(segments))
	}
	assembled, err := AssembleContent(manifestID, append([]ContentManifest(nil), segments...))
	if err != nil {
		t.Fatal(err)
	}
	if !EqualIDs(assembled, ids) {
		t.Fatalf("assembled content order changed")
	}
	otherID, _, _ := SegmentContent(ids, 3)
	if otherID != manifestID {
		t.Fatal("segment size changed content identity")
	}
	segments[1].ContentIDs[0] = testID(9)
	if _, err := AssembleContent(manifestID, segments); !errors.Is(err, ErrMalformed) {
		t.Fatalf("modified manifest returned %v", err)
	}
	oversized := ContentManifest{TotalCount: MaxContentIDs + 1, SegmentCount: 1, ContentIDs: []ID{testID(1)}}
	if _, err := oversized.MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized manifest returned %v", err)
	}
	if _, err := AssembleContent(testID(1), []ContentManifest{oversized}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized assembly returned %v", err)
	}
}

func TestDirectoryValidationRejectsCyclesConflictsAndUnknownParents(t *testing.T) {
	root, child := InodeRef{1, 1}, InodeRef{1, 2}
	valid := map[InodeRef]DirectoryNode{
		root:  {Revision: 1, Record: DirectoryRevision{Children: []DirectoryChild{{Name: "child", Inode: 2, Type: NodeDirectory, MetadataKey: DirectoryRevisionKey(1, 2, 2)}}}},
		child: {Revision: 2, Record: DirectoryRevision{ParentInode: 1}},
	}
	if err := ValidateDirectoryGraph(root, valid); err != nil {
		t.Fatal(err)
	}
	cycle := map[InodeRef]DirectoryNode{root: valid[root], child: {Revision: 2, Record: DirectoryRevision{ParentInode: 1, Children: []DirectoryChild{{Name: "root", Inode: 1, Type: NodeDirectory, MetadataKey: DirectoryRevisionKey(1, 1, 1)}}}}}
	if err := ValidateDirectoryGraph(root, cycle); !errors.Is(err, ErrMalformed) {
		t.Fatalf("cycle returned %v", err)
	}
	orphan := map[InodeRef]DirectoryNode{root: valid[root], child: valid[child], {FSID: 1, Inode: 3}: {Revision: 3, Record: DirectoryRevision{ParentInode: 99}}}
	if err := ValidateDirectoryGraph(root, orphan); !errors.Is(err, ErrMalformed) {
		t.Fatalf("orphan returned %v", err)
	}
	secondParent := InodeRef{1, 3}
	conflict := map[InodeRef]DirectoryNode{
		root: valid[root], child: valid[child],
		secondParent: {Revision: 3, Record: DirectoryRevision{ParentInode: 1, Children: []DirectoryChild{{Name: "same-child", Inode: 2, Type: NodeDirectory, MetadataKey: DirectoryRevisionKey(1, 2, 2)}}}},
	}
	conflict[root] = DirectoryNode{Revision: 1, Record: DirectoryRevision{Children: []DirectoryChild{
		{Name: "child", Inode: 2, Type: NodeDirectory, MetadataKey: DirectoryRevisionKey(1, 2, 2)},
		{Name: "other", Inode: 3, Type: NodeDirectory, MetadataKey: DirectoryRevisionKey(1, 3, 3)},
	}}}
	if err := ValidateDirectoryGraph(root, conflict); !errors.Is(err, ErrMalformed) {
		t.Fatalf("conflicting parent returned %v", err)
	}
	crossFS := map[InodeRef]DirectoryNode{
		root:                {Revision: 1, Record: DirectoryRevision{Children: []DirectoryChild{{Name: "foreign", Inode: 2, Type: NodeDirectory, MetadataKey: DirectoryRevisionKey(2, 2, 1)}}}},
		{FSID: 2, Inode: 2}: {Revision: 1, Record: DirectoryRevision{ParentInode: 1}},
	}
	if err := ValidateDirectoryGraph(root, crossFS); !errors.Is(err, ErrMalformed) {
		t.Fatalf("cross-filesystem child returned %v", err)
	}
}

func TestSnapshotResolutionKeepsHistoricalDirectoryRevision(t *testing.T) {
	old := DirectoryRevision{Children: []DirectoryChild{{Name: "old", Inode: 2, Type: NodeFile, MetadataKey: InodeRevisionKey(1, 2, 1)}}}
	current := DirectoryRevision{Children: []DirectoryChild{{Name: "new", Inode: 3, Type: NodeFile, MetadataKey: InodeRevisionKey(1, 3, 2)}}}
	oldBytes, _ := old.MarshalBinary()
	currentBytes, _ := current.MarshalBinary()
	revisions := map[string][]byte{string(DirectoryRevisionKey(1, 1, 1)): oldBytes, string(DirectoryRevisionKey(1, 1, 2)): currentBytes}
	resolved, err := ResolveSnapshotRoot(SnapshotRecord{CommitSequence: 1, RootFSID: 1, RootInode: 1, RootRevision: 1}, revisions)
	if err != nil || resolved.Children[0].Name != "old" {
		t.Fatalf("snapshot root = %#v, %v", resolved, err)
	}
}

func TestPackClassificationAggregatesAndReverseCounts(t *testing.T) {
	if ClassifyPack([]BlobType{BlobData, BlobTree}) != PackMixed || ClassifyPack(nil) != PackUnknown || ClassifyPack([]BlobType{99}) != PackUnknown {
		t.Fatal("pack classification mismatch")
	}
	if _, err := (PackRecord{Type: 99, Lifecycle: PackImported}).MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("invalid pack type returned %v", err)
	}
	if _, err := (PackRecord{Type: PackData, Lifecycle: 99}).MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("invalid lifecycle returned %v", err)
	}
	records := []PackRecord{{Type: PackData, PhysicalSize: 10, PhysicalSizeKnown: true, PayloadSize: 8, HeaderSize: 2, BlobCount: 1, Lifecycle: PackImported}, {Type: PackMixed, PhysicalSize: 20, PhysicalSizeKnown: true, PayloadSize: 15, HeaderSize: 5, BlobCount: 2, Lifecycle: PackPublished}}
	aggregates, err := RebuildPackAggregates(records, 7)
	if err != nil {
		t.Fatal(err)
	}
	if aggregates[AggregateAll].PackCount != 2 || aggregates[AggregateMixed].PhysicalSize != 20 || aggregates[AggregateUnknown].UpdateSequence != 7 {
		t.Fatalf("unexpected aggregates: %#v", aggregates)
	}
	blob, manifest := testID(1), testID(2)
	counts := RebuildReferenceCounts([]ManifestEdge{{blob, manifest}, {blob, manifest}}, []InodeEdge{{blob, 1, 2, 3}, {blob, 1, 2, 4}}, 9)[blob]
	if counts.TotalReferences != 4 || counts.DistinctManifests != 1 || counts.DistinctInodes != 1 || counts.DistinctRevisions != 2 {
		t.Fatalf("unexpected reference counts: %#v", counts)
	}
}
