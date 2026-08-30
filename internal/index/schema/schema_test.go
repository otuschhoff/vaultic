package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func testID(value byte) ID { var id ID; id[0] = value; id[31] = value; return id }

type binaryRecord interface{ MarshalBinary() ([]byte, error) }

// roundTrip asserts encode/decode symmetry and that every truncation is
// rejected. legacyLengths lists prefix lengths that are valid on purpose,
// because a record whose tail was appended in a later phase must still decode
// from its earlier layout.
func roundTrip[T any](t *testing.T, input T, decode func([]byte) (T, error), legacyLengths ...int) {
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
	valid := make(map[int]bool, len(legacyLengths))
	for _, length := range legacyLengths {
		valid[length] = true
		if _, err := decode(encoded[:length]); err != nil {
			t.Fatalf("legacy length %d failed to decode: %v", length, err)
		}
	}
	for size := range len(encoded) {
		if valid[size] {
			continue
		}
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
		{BlobKey(id), KeyBlob}, {PackKey(id), KeyPack}, {SnapshotCommitKey(9, id), KeySnapshotCommit}, {PathVersionKey(1, "a/b", 9), KeyPathVersion}, {PackAggregateKey(AggregateData), KeyPackAggregate},
		{PackAggregateKey(AggregateTree), KeyPackAggregate}, {PackAggregateKey(AggregateMixed), KeyPackAggregate}, {PackAggregateKey(AggregateUnknown), KeyPackAggregate}, {PackAggregateKey(AggregateAll), KeyPackAggregate},
		{TierAggregateKey(TierUnknown), KeyTierAggregate}, {TierAggregateKey(TierHot), KeyTierAggregate},
		{TierAggregateKey(TierCold), KeyTierAggregate}, {TierAggregateKey(TierMirrored), KeyTierAggregate},
		{TierAggregateKey(TierSingle), KeyTierAggregate},
		{PackHistoryKey(1700000000, 7, id), KeyPackHistory},
		{PackHistoryBucketKey(GranularityHourly, 1700000000, 0, PackData), KeyPackHistoryBucket},
		{PackHistoryBucketKey(GranularityDaily, 1700000000, 3, PackTree), KeyPackHistoryBucket},
		{PackHistoryBucketKey(GranularityMonthly, 1700000000, 0, PackUnknown), KeyPackHistoryBucket},
		{NextEventSequenceKey(), KeyNextEventSequence},
		{HistoryRawFloorKey(), KeyHistoryRawFloor},
		{HistoryEnabledAtKey(), KeyHistoryEnabledAt},
		{PackPlacementKey(id, 42), KeyPackPlacement}, {BackendPackKey(42, id), KeyBackendPack},
		{PlacementDeleteQueueKey(1700000000, id, 42), KeyPlacementDeleteQueue},
		{PlacementRequestKey(1700000000, id), KeyPlacementRequest},
		{RepackLineageKey(id, second), KeyRepackLineage},
		{PromotionEligibilityKey(id), KeyPromotionEligibility},
		{AnalyticsFactKey(3, 4), KeyAnalyticsFact}, {AnalyticsCacheKey(id), KeyAnalyticsCache},
		{AnalyticsMetadataKey(), KeyAnalyticsMetadata}, {AnalyticsBuildCheckpointKey(), KeyAnalyticsBuildCheckpoint},
		{AnalyticsDictionaryKey(AnalyticsDictionarySVM, 7), KeyAnalyticsDictionary},
		{AnalyticsFactSegmentKey(8), KeyAnalyticsFactSegment}, {AnalyticsSegmentMetadataKey(8), KeyAnalyticsSegmentMetadata},
		{AnalyticsDimensionIndexKey(AnalyticsDimensionUID, 600, 8), KeyAnalyticsDimensionIndex},
		{AnalyticsResidencyKey(3, 4, 5), KeyAnalyticsResidency}, {AnalyticsDeltaKey(9, 2), KeyAnalyticsDelta},
		{AnalyticsDerivedKey(6, AnalyticsResidencyKey(3, 4, 5)), KeyAnalyticsResidency}, {AnalyticsDerivedGenerationMarkerKey(6), KeyAnalyticsDerivedMarker},
		{AnalyticsWatermarkKey(6), KeyAnalyticsWatermark}, {AnalyticsManifestKey(6), KeyAnalyticsManifest},
		{AnalyticsQueryResultKey(id, 2, 6, 9), KeyAnalyticsQueryResult}, {AnalyticsQueryHeatKey(id), KeyAnalyticsQueryHeat},
		{AnalyticsQueryViewKey(id, 3), KeyAnalyticsQueryView}, {AnalyticsQueryJobKey(id), KeyAnalyticsQueryJob},
		{GrowthTimeKey(AnalyticsGranularityMonth, -10, TierCold), KeyGrowthTime},
		{GrowthPathKey("/home/alice", AnalyticsGranularityYear, 10), KeyGrowthPath},
		{UserSummaryKey(600), KeyUserSummary}, {GroupSummaryKey(700), KeyGroupSummary},
		{UserStatsKey(600, AnalyticsArchiveOnly), KeyUserStats}, {GroupStatsKey(700, AnalyticsUnknown), KeyGroupStats},
		{UserChurnKey(600, AnalyticsGranularityWeek, 10), KeyUserChurn},
		{UserBlobContributionKey(600, id, 2, 3, 4, 5), KeyUserBlobContribution},
		{UserInodeKey(600, 3, 4), KeyUserInode}, {UserBlobKey(600, id), KeyUserBlob},
		{AuthoritativeCrawlProofKey(id, 9), KeyAuthoritativeCrawlProof}, {AuthoritativeSourceBindingKey(id, 3, 4, 5), KeyAuthoritativeSourceBinding},
		{CurrentInodeKey(3, 4), KeyCurrentInode}, {InodeRevisionKey(3, 4, 5), KeyInodeRevision},
		{CurrentDirectoryKey(3, 4), KeyCurrentDirectory}, {DirectoryRevisionKey(3, 4, 5), KeyDirectoryRevision},
		{SnapshotKey(id), KeySnapshot}, {ContentManifestKey(id, 7), KeyContentManifest}, {ReverseManifestKey(id, second), KeyReverseManifest},
		{ReverseInodeKey(id, 3, 4), KeyReverseInode}, {ReferenceCountKey(id), KeyReferenceCount},
		{GarbageCollectionKey(GCBlob, id), KeyGarbageCollection}, {GarbageCollectionKey(GCPack, id), KeyGarbageCollection},
		{CrawlDebtKey(id, second), KeyCrawlDebt}, {ImportCheckpointKey(id), KeyImportCheckpoint},
		{SnapshotImportCheckpointKey(id), KeySnapshotImportCheckpoint}, {ExportCheckpointKey(id), KeyExportCheckpoint}, {ExportIndexCheckpointKey(id), KeyExportIndexCheckpoint},
		{NextRevisionKey(), KeyNextRevision}, {NextExportSequenceKey(), KeyNextExportSequence},
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

func TestPhase16AnalyticsKeysPreserveComponentsAndOrdering(t *testing.T) {
	id := testID(7)
	tests := []struct {
		key   []byte
		check func(ParsedKey) bool
	}{
		{AnalyticsDictionaryKey(AnalyticsDictionaryVolume, 12), func(key ParsedKey) bool { return key.Dictionary == AnalyticsDictionaryVolume && key.Ordinal == 12 }},
		{AnalyticsFactSegmentKey(13), func(key ParsedKey) bool { return key.Generation == 13 }},
		{AnalyticsDimensionIndexKey(AnalyticsDimensionCalendarYear, 2024, 13), func(key ParsedKey) bool {
			return key.Dimension == AnalyticsDimensionCalendarYear && key.Value == 2024 && key.Generation == 13
		}},
		{AnalyticsResidencyKey(2, 3, 4), func(key ParsedKey) bool { return key.FSID == 2 && key.Inode == 3 && key.Generation == 4 }},
		{AnalyticsDerivedKey(15, UserInodeKey(600, 2, 3)), func(key ParsedKey) bool {
			return key.ViewGeneration == 15 && key.UID == 600 && key.FSID == 2 && key.Inode == 3
		}},
		{AnalyticsDerivedGenerationMarkerKey(15), func(key ParsedKey) bool { return key.ViewGeneration == 15 && key.Kind == KeyAnalyticsDerivedMarker }},
		{AnalyticsDerivedKey(15, UserBlobContributionKey(600, id, 2, 3, 4, 5)), func(key ParsedKey) bool {
			return key.ViewGeneration == 15 && key.UID == 600 && key.ID == id && key.FSID == 2 && key.Inode == 3 && key.Generation == 4 && key.Ordinal == 5
		}},
		{AnalyticsDeltaKey(14, 15), func(key ParsedKey) bool { return key.Commit == 14 && key.Ordinal == 15 }},
		{AuthoritativeCrawlProofKey(id, 23), func(key ParsedKey) bool { return key.ID == id && key.Commit == 23 }},
		{AuthoritativeSourceBindingKey(id, 2, 3, 4), func(key ParsedKey) bool {
			return key.ID == id && key.FSID == 2 && key.Inode == 3 && key.Generation == 4
		}},
		{AnalyticsWatermarkKey(16), func(key ParsedKey) bool { return key.Epoch == 16 }},
		{AnalyticsQueryResultKey(id, 17, 18, 19), func(key ParsedKey) bool {
			return key.ID == id && key.Generation == 17 && key.Epoch == 18 && key.Commit == 19
		}},
		{GrowthTimeKey(AnalyticsGranularityMonth, -20, TierCold), func(key ParsedKey) bool {
			return key.Granularity == HistoryGranularity(AnalyticsGranularityMonth) && key.Timestamp == -20 && key.Tier == TierCold
		}},
		{GrowthPathKey("home/alice/", AnalyticsGranularityYear, 21), func(key ParsedKey) bool { return key.Path == "/home/alice" && key.Timestamp == 21 }},
		{UserChurnKey(600, AnalyticsGranularityWeek, 22), func(key ParsedKey) bool { return key.UID == 600 && key.Timestamp == 22 }},
		{UserInodeKey(600, 2, 3), func(key ParsedKey) bool { return key.UID == 600 && key.FSID == 2 && key.Inode == 3 }},
		{UserBlobKey(600, id), func(key ParsedKey) bool { return key.UID == 600 && key.ID == id }},
	}
	for _, test := range tests {
		parsed, err := ParseKey(test.key)
		if err != nil || !test.check(parsed) {
			t.Fatalf("analytics key %x parsed as %#v, %v", test.key, parsed, err)
		}
		for size := range len(test.key) {
			if _, err := ParseKey(test.key[:size]); !errors.Is(err, ErrMalformed) {
				t.Fatalf("analytics key %x truncation at %d returned %v", test.key, size, err)
			}
		}
	}
	if bytes.Compare(AnalyticsDeltaKey(8, 99), AnalyticsDeltaKey(9, 0)) >= 0 || bytes.Compare(AnalyticsDeltaKey(9, 0), AnalyticsDeltaKey(9, 1)) >= 0 {
		t.Fatal("analytics delta keys do not preserve commit/ordinal order")
	}
	if bytes.Compare(GrowthTimeKey(AnalyticsGranularityWeek, -1, TierHot), GrowthTimeKey(AnalyticsGranularityWeek, 0, TierHot)) >= 0 {
		t.Fatal("growth time keys do not preserve signed timestamp order")
	}
	if prefix := GrowthPathPrefix("/home/alice"); !bytes.HasPrefix(GrowthPathKey("/home/alice", AnalyticsGranularityWeek, 1), prefix) || bytes.HasPrefix(GrowthPathKey("/home/alice2", AnalyticsGranularityWeek, 1), prefix) {
		t.Fatalf("growth path prefix is ambiguous: %x", prefix)
	}
	for _, invalid := range [][]byte{
		AnalyticsDictionaryKey(0, 1), AnalyticsFactSegmentKey(0), AnalyticsDimensionIndexKey(0, 1, 1),
		AnalyticsDimensionIndexKey(AnalyticsDimensionUID, 1, 0), AnalyticsResidencyKey(1, 2, 0), AnalyticsDeltaKey(0, 1),
		AuthoritativeCrawlProofKey(ID{}, 1), AuthoritativeCrawlProofKey(id, 0), AuthoritativeSourceBindingKey(ID{}, 1, 1, 1), AuthoritativeSourceBindingKey(id, 0, 1, 1), AuthoritativeSourceBindingKey(id, 1, 1, 0),
		AnalyticsWatermarkKey(0), AnalyticsManifestKey(0), AnalyticsQueryResultKey(id, 0, 1, 1),
		GrowthTimeKey(0, 1, TierHot), GrowthPathKey("/", AnalyticsGranularityWeek, 1),
	} {
		if invalid != nil {
			t.Fatalf("invalid analytics key constructor returned %x", invalid)
		}
	}
}

func TestPlacementKeysPreservePackBackendAndDeadline(t *testing.T) {
	id := testID(7)
	placement, err := ParseKey(PackPlacementKey(id, 0x0102030405060708))
	if err != nil {
		t.Fatal(err)
	}
	if placement.Kind != KeyPackPlacement || placement.ID != id || placement.Backend != 0x0102030405060708 {
		t.Fatalf("placement key parsed as %#v", placement)
	}
	if prefix := PackPlacementPrefix(id); len(prefix) != 36 || string(prefix[:3]) != "pl:" || prefix[35] != ':' {
		t.Fatalf("bad placement prefix %x", prefix)
	}

	reverse, err := ParseKey(BackendPackKey(0x0102030405060708, id))
	if err != nil {
		t.Fatal(err)
	}
	if reverse.Kind != KeyBackendPack || reverse.ID != id || reverse.Backend != 0x0102030405060708 {
		t.Fatalf("backend-pack key parsed as %#v", reverse)
	}
	if prefix := BackendPackPrefix(0x0102030405060708); len(prefix) != 12 || string(prefix[:3]) != "bp:" || prefix[11] != ':' {
		t.Fatalf("bad backend-pack prefix %x", prefix)
	}

	deadline := int64(1_700_000_000)
	queued, err := ParseKey(PlacementDeleteQueueKey(deadline, id, 0x0102030405060708))
	if err != nil {
		t.Fatal(err)
	}
	if queued.Kind != KeyPlacementDeleteQueue || queued.ID != id || queued.Backend != 0x0102030405060708 || queued.DeleteAfter != deadline {
		t.Fatalf("delete queue key parsed as %#v", queued)
	}
	if prefix := PlacementDeleteQueuePrefix(deadline); len(prefix) != 11 || string(prefix[:3]) != "dq:" {
		t.Fatalf("bad delete queue prefix %x", prefix)
	}
}

func TestSnapshotCommitKeyPreservesCommitAndSnapshot(t *testing.T) {
	id := testID(8)
	parsed, err := ParseKey(SnapshotCommitKey(99, id))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Kind != KeySnapshotCommit || parsed.Revision != 99 || parsed.ID != id {
		t.Fatalf("snapshot commit key parsed as %#v", parsed)
	}
	if string(SnapshotCommitPrefix()) != "sc:" {
		t.Fatalf("snapshot commit prefix = %q", SnapshotCommitPrefix())
	}
}

func TestPathVersionKeyOrderingAndTerminators(t *testing.T) {
	self1 := PathVersionKey(1, "a/b", 1)
	self2 := PathVersionKey(1, "/a/b", 2)
	child := PathVersionKey(1, "a/b/c", 1)
	sibling := PathVersionKey(1, "a/bc", 1)
	if self1 == nil || self2 == nil || child == nil || sibling == nil {
		t.Fatal("valid path-version key returned nil")
	}
	parsed, err := ParseKey(self1)
	if err != nil || parsed.Kind != KeyPathVersion || parsed.FSID != 1 || parsed.Path != "a/b" || parsed.Revision != 1 {
		t.Fatalf("path-version key parsed as %#v err=%v", parsed, err)
	}
	ordered := [][]byte{self1, self2, child, sibling}
	if !(bytes.Compare(ordered[0], ordered[1]) < 0 && bytes.Compare(ordered[1], ordered[2]) < 0 && bytes.Compare(ordered[2], ordered[3]) < 0) {
		t.Fatalf("path-version ordering is wrong: %x %x %x %x", self1, self2, child, sibling)
	}
	if prefix := PathVersionPrefix(1, "a/b"); !bytes.HasPrefix(self1, prefix) || bytes.HasPrefix(sibling, prefix) {
		t.Fatalf("path prefix collides with sibling: prefix=%x sibling=%x", prefix, sibling)
	}
	if subtree := PathVersionSubtreePrefix(1, "a/b"); !bytes.HasPrefix(child, subtree) || bytes.HasPrefix(sibling, subtree) {
		t.Fatalf("subtree prefix collides with sibling: subtree=%x sibling=%x", subtree, sibling)
	}
	if key := PathVersionKey(1, strings.Repeat("x", MaxPathIndexPathBytes+1), 1); key != nil {
		t.Fatalf("overlong path produced key %x", key)
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
	binary.BigEndian.PutUint32(encodedInode[46:50], 1<<8)
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
	analyticsValues := []struct{ key, value []byte }{}
	appendValue := func(key []byte, record binaryRecord) {
		value, err := record.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		analyticsValues = append(analyticsValues, struct{ key, value []byte }{key, value})
	}
	appendValue(AnalyticsDictionaryKey(AnalyticsDictionarySVM, 1), AnalyticsDictionaryRecord{Value: "svm"})
	appendValue(AnalyticsFactSegmentKey(1), AnalyticsFactSegmentRecord{RowCount: 1, Columns: []AnalyticsColumn{{Kind: AnalyticsColumnUID, Codec: AnalyticsCodecRaw, Data: []byte{1}}}})
	appendValue(AnalyticsSegmentMetadataKey(1), AnalyticsSegmentMetadataRecord{RowCount: 1, MinCreatedAt: 1, MaxCreatedAt: 1, MinLogicalSize: 1, MaxLogicalSize: 1, MinRevision: 1, MaxRevision: 1, FirstCommit: 1, LastCommit: 1, ClassificationEpoch: 1})
	appendValue(AnalyticsDimensionIndexKey(AnalyticsDimensionUID, 600, 1), AnalyticsDimensionIndexRecord{Codec: AnalyticsCodecRaw, RowCount: 1, MatchCount: 1, Bitmap: []byte{1}})
	appendValue(AnalyticsResidencyKey(1, 2, 3), AnalyticsResidencyRecord{State: AnalyticsLive, ClassificationEpoch: 1, FactSegment: 1})
	appendValue(AnalyticsDeltaKey(1, 0), AnalyticsDeltaRecord{Kind: AnalyticsDeltaSourceState, IdentityGeneration: 1, Revision: 1, State: AnalyticsLive, ClassificationEpoch: 1})
	appendValue(AnalyticsWatermarkKey(1), AnalyticsWatermarkRecord{RepositoryGeneration: 1, ManifestGeneration: 1, AppliedAt: 1})
	appendValue(AnalyticsManifestKey(1), AnalyticsManifestRecord{Generation: 1, Segments: []uint64{1}})
	appendValue(AnalyticsBuildCheckpointKey(), AnalyticsBuildCheckpointRecord{BuildID: id, FormatVersion: 1, Generation: 2, ConfigJSON: "{}", SourceKeyCursor: AuthoritativeSourceBindingKey(id, 1, 2, 3), Facts: 1, AppliedCommit: 1, CandidateSegments: []uint64{2<<32 | 1}, StartedAt: 1, UpdatedAt: 1})
	appendValue(AnalyticsQueryResultKey(id, 1, 1, 1), AnalyticsQueryRecord{Payload: []byte{1}})
	appendValue(AnalyticsQueryHeatKey(id), AnalyticsQueryRecord{Payload: []byte{1}})
	appendValue(AnalyticsQueryViewKey(id, 1), AnalyticsQueryRecord{Payload: []byte{1}})
	appendValue(AnalyticsQueryJobKey(id), AnalyticsQueryJobRecord{State: AnalyticsQueryPending, CanonicalQuery: []byte{1}, RepositoryGeneration: 1, ClassificationEpoch: 1, UpdatedAt: 1})
	appendValue(GrowthTimeKey(AnalyticsGranularityWeek, 1, TierHot), AnalyticsAggregateRecord{})
	appendValue(GrowthPathKey("/home", AnalyticsGranularityWeek, 1), AnalyticsAggregateRecord{})
	appendValue(UserSummaryKey(600), AnalyticsSummaryRecord{})
	appendValue(GroupSummaryKey(700), AnalyticsSummaryRecord{})
	appendValue(UserChurnKey(600, AnalyticsGranularityWeek, 1), AnalyticsAggregateRecord{})
	appendValue(UserInodeKey(600, 1, 2), AnalyticsUserInodeRecord{LatestRevision: 1})
	appendValue(UserBlobKey(600, id), AnalyticsUserBlobRecord{ReferenceCount: 1, FirstSeen: 1})
	for _, test := range analyticsValues {
		if err := ValidateValue(test.key, test.value); err != nil {
			t.Fatalf("ValidateValue(%x): %v", test.key, err)
		}
	}
	if err := ValidateValue(AnalyticsDictionaryKey(AnalyticsDictionarySVM, 1), analyticsValues[1].value); !errors.Is(err, ErrMalformed) {
		t.Fatalf("mismatched analytics record family returned %v", err)
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
	if _, err := (AnalyticsFactSegmentRecord{RowCount: 1, Columns: []AnalyticsColumn{{Kind: AnalyticsColumnUID, Codec: AnalyticsCodecRaw, Data: oversizedField}}}).MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized analytics segment returned %v", err)
	}
}

func TestPhase16AnalyticsRecordsRejectInvalidStates(t *testing.T) {
	invalid := []binaryRecord{
		AnalyticsDictionaryRecord{},
		AnalyticsFactSegmentRecord{RowCount: 1, Columns: []AnalyticsColumn{{Kind: AnalyticsColumnUID, Codec: AnalyticsCodecRaw, Data: []byte{1}}, {Kind: AnalyticsColumnUID, Codec: AnalyticsCodecRaw, Data: []byte{2}}}},
		AnalyticsSegmentMetadataRecord{RowCount: 1, MinCreatedAt: 2, MaxCreatedAt: 1, MinRevision: 1, MaxRevision: 1, FirstCommit: 1, LastCommit: 1, ClassificationEpoch: 1},
		AnalyticsDimensionIndexRecord{Codec: AnalyticsCodecRoaring, RowCount: 1, MatchCount: 2, Bitmap: []byte{1}},
		AnalyticsResidencyRecord{State: AnalyticsArchiveOnly, ClassificationEpoch: 1, FactSegment: 1},
		AnalyticsResidencyRecord{State: AnalyticsExpired, RetainedSnapshotRefs: 1, ClassificationEpoch: 1, FactSegment: 1},
		AuthoritativeCrawlProofRecord{ScopeID: testID(1), RootFSID: 1, RootInode: 1, StartFence: 2, EndCommit: 1, CompletedAt: 1, Complete: true, DebtFree: true},
		AuthoritativeSourceBindingRecord{Revision: 1, State: AuthoritativeSourceLive, LastObservedCommit: 1},
		AnalyticsDeltaRecord{Kind: AnalyticsDeltaCreation, IdentityGeneration: 1, Revision: 1, Known: 1 << 15, CreationBasis: AnalyticsCTime, ClassificationEpoch: 1},
		AnalyticsWatermarkRecord{RepositoryGeneration: 1, ManifestGeneration: 1},
		AnalyticsManifestRecord{Generation: 1, Segments: []uint64{2, 1}},
		AnalyticsUserInodeRecord{}, AnalyticsUserBlobRecord{},
		AnalyticsQueryJobRecord{State: AnalyticsQueryComplete, CanonicalQuery: []byte{1}, RepositoryGeneration: 1, ClassificationEpoch: 1, UpdatedAt: 1},
		AnalyticsQueryJobRecord{State: AnalyticsQueryFailed, CanonicalQuery: []byte{1}, RepositoryGeneration: 1, ClassificationEpoch: 1, UpdatedAt: 1},
	}
	for _, record := range invalid {
		if _, err := record.MarshalBinary(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid analytics record %#v returned %v", record, err)
		}
	}
}

// legacyPackEncodings reproduces the two historical pack record formats so the
// decoder's backward compatibility is tested against the real byte layouts
// rather than against an arbitrary truncation of the current one.
func legacyPackEncodings(t *testing.T, record PackRecord) (phase3, prePhysicalSize []byte) {
	t.Helper()
	e := newEncoder()
	e.u8(byte(record.Type))
	e.u64(record.PhysicalSize)
	e.u64(record.PayloadSize)
	e.u64(record.HeaderSize)
	e.u64(record.BlobCount)
	e.i64(record.CreationTime)
	e.bool(record.CreationTimeKnown)
	e.u8(byte(record.Lifecycle))
	e.u32(uint32(len(record.SourceIndexIDs)))
	for _, id := range record.SourceIndexIDs {
		e.id(id)
	}
	withoutFlag, err := e.finish()
	if err != nil {
		t.Fatal(err)
	}
	prePhysicalSize = append([]byte(nil), withoutFlag...)
	e.bool(record.PhysicalSizeKnown)
	phase3, err = e.finish()
	if err != nil {
		t.Fatal(err)
	}
	return phase3, prePhysicalSize
}

func TestSchemaRecordRoundTripsAndMalformedInput(t *testing.T) {
	id1, id2, id3 := testID(1), testID(2), testID(3)
	roundTrip(t, BlobRecord{Locations: []BlobLocation{{PackID: id1, Offset: 9, Length: 10, UncompressedSize: 11, Type: BlobData}, {PackID: id2, Offset: 12, Length: 13, UncompressedSize: 14, Type: BlobTree}}}, UnmarshalBlobRecord)
	packRecord := PackRecord{
		Type: PackMixed, PhysicalSize: 100, PhysicalSizeKnown: true, PayloadSize: 80, HeaderSize: 20, BlobCount: 2,
		CreationTime: 123, CreationTimeKnown: true, Lifecycle: PackPublished, SourceIndexIDs: []ID{id1, id2},
		Tier: TierCold, StorageClass: "GLACIER", MinRetentionUntil: 456, RetentionSource: RetentionConfig,
		UsageKnown: true, UsedPayloadBytes: 50, UnusedPayloadBytes: 30, DeleteAfter: 789,
	}
	encodedPack, err := packRecord.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decodedPack, err := UnmarshalPackRecord(encodedPack)
	if err != nil || !reflect.DeepEqual(decodedPack, packRecord) {
		t.Fatalf("pack round trip = %#v, %v", decodedPack, err)
	}
	// A record that never specified a tier must decode as explicitly unknown
	// rather than as tier zero, so an unconsidered pack is never mistaken for
	// a routed one.
	unspecified, err := (PackRecord{Type: PackData, Lifecycle: PackImported}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decodedUnspecified, err := UnmarshalPackRecord(unspecified)
	if err != nil || decodedUnspecified.Tier != TierUnknown || decodedUnspecified.RetentionSource != RetentionUnknown {
		t.Fatalf("unspecified tier decode = %#v, %v", decodedUnspecified, err)
	}
	// Phase 3 and pre-PhysicalSizeKnown records must still decode, and must
	// decode as tier-unknown and retention-unknown.
	phase3, prePhysical := legacyPackEncodings(t, packRecord)
	for name, encoded := range map[string][]byte{"phase3": phase3, "pre-physical-size": prePhysical} {
		legacyPack, decodeErr := UnmarshalPackRecord(encoded)
		if decodeErr != nil {
			t.Fatalf("%s pack decode failed: %v", name, decodeErr)
		}
		if legacyPack.Tier != TierUnknown || legacyPack.RetentionSource != RetentionUnknown {
			t.Fatalf("%s pack decoded tier %v/%v", name, legacyPack.Tier, legacyPack.RetentionSource)
		}
		if legacyPack.UsageKnown || legacyPack.UsedPayloadBytes != 0 || legacyPack.UnusedPayloadBytes != 0 || legacyPack.DeleteAfter != 0 || legacyPack.StorageClass != "" {
			t.Fatalf("%s pack invented lifetime facts: %#v", name, legacyPack)
		}
		if !legacyPack.PhysicalSizeKnown {
			t.Fatalf("%s pack lost the physical size state", name)
		}
		want := packRecord
		want.Tier, want.RetentionSource = TierUnknown, RetentionUnknown
		want.StorageClass, want.MinRetentionUntil = "", 0
		want.UsageKnown, want.UsedPayloadBytes, want.UnusedPayloadBytes, want.DeleteAfter = false, 0, 0, 0
		if !reflect.DeepEqual(legacyPack, want) {
			t.Fatalf("%s pack decode = %#v", name, legacyPack)
		}
	}
	// Every truncation is malformed except the two legacy record boundaries,
	// which are valid by design.
	valid := map[int]bool{len(phase3): true, len(prePhysical): true}
	for size := range len(encodedPack) - 1 {
		if valid[size] {
			continue
		}
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
	// The Phase 3 aggregate ended after UpdateSequence; that prefix length is
	// still a valid record.
	const phase3AggregateLength = 1 + 6*8
	roundTrip(t, PackAggregate{PackCount: 1, PhysicalSize: 2, PayloadSize: 3, HeaderSize: 4, BlobCount: 5, UpdateSequence: 6, UsedPayloadBytes: 7, UnusedPayloadBytes: 8, AccountedPackCount: 1}, UnmarshalPackAggregate, phase3AggregateLength)
	roundTrip(t, CurrentPointer{Revision: 7, RecordKey: InodeRevisionKey(2, 3, 7)}, UnmarshalCurrentPointer)
	roundTrip(t, PackHistoryEvent{
		Type: EventRepackedInto, PackType: PackData, Backend: 4,
		PhysicalSize: 100, PayloadSize: 80, UsedDelta: -5, UnusedDelta: 7,
		PredecessorPackIDs: []ID{id1, id2}, RunID: id3, ReasonCode: "mixed_pack_repack",
	}, UnmarshalPackHistoryEvent)
	roundTrip(t, PackHistoryBucket{
		PacksCreated: 1, PacksDeleted: 2, PacksRepacked: 3, PacksPromoted: 4,
		BytesAdded: 5, BytesDeleted: 6, BytesRepacked: 7, BytesPromoted: 8,
		EndPackCount: 9, EndPhysicalSize: 10, EndPayloadSize: 11,
		Coverage: CoveragePartial, EventsObserved: 12,
	}, UnmarshalPackHistoryBucket)
	roundTrip(t, PlacementRecord{
		State: PlacementEvicting, StorageClass: "GLACIER", PlacedAt: 123,
		PlacementTimeKnown: true, Bytes: 456, MinRetentionUntil: 789,
		RetentionSource: RetentionBackend, DeleteAfter: 999, LastVerifiedAt: 1001,
	}, UnmarshalPlacementRecord)
	roundTrip(t, BackendPackRecord{State: PlacementLive, Bytes: 456, PlacedAt: 123}, UnmarshalBackendPackRecord)
	roundTrip(t, PlacementDeleteRecord{Backend: 42, PhysicalSize: 456, Reason: "retention", RunID: id3}, UnmarshalPlacementDeleteRecord)
	roundTrip(t, PlacementRequestRecord{
		Classes: []string{"recent-data", "archival-data"}, Operation: PlacementRequestPlace,
		TargetBackend: 42, Attempts: 2, LastAttempt: 100, NotBefore: 200, LastError: "temporary",
	}, UnmarshalPlacementRequestRecord)
	roundTrip(t, RepackLineageRecord{RunID: id3, Kind: LineagePromotion}, UnmarshalRepackLineageRecord)
	roundTrip(t, PromotionEligibilityRecord{SurvivalUntil: 500, EvaluatedAt: 100, Indefinite: false}, UnmarshalPromotionEligibilityRecord)
	analyticsFact := AnalyticsFactRecord{Revision: 1, UID: 2, GID: 3, Known: KnownCTime, CreatedAt: 4, LogicalSize: 5, CalendarYear: 2024, CalendarMonth: 2, ISOYear: 2024, Workweek: 8, SourcePath: "/svm/vol/q/file", SVM: "svm", Volume: "vol", PathGroup: "q", Residency: AnalyticsLive, CreationBasis: AnalyticsCTime, IdentityGeneration: 42, IdentityContinuity: AnalyticsContinuitySourceGeneration}
	encodedAnalyticsFact, err := analyticsFact.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, analyticsFact, UnmarshalAnalyticsFactRecord, len(encodedAnalyticsFact)-9)
	roundTrip(t, AnalyticsMetadataRecord{Enabled: true, Generation: 1, Facts: 2, BuiltAt: 3}, UnmarshalAnalyticsMetadataRecord)
	roundTrip(t, AnalyticsDictionaryRecord{Value: "svm-finance"}, UnmarshalAnalyticsDictionaryRecord)
	roundTrip(t, AnalyticsFactSegmentRecord{RowCount: 2, Columns: []AnalyticsColumn{{Kind: AnalyticsColumnIdentity, Codec: AnalyticsCodecRaw, Data: []byte{1, 2}}, {Kind: AnalyticsColumnUID, Codec: AnalyticsCodecDelta, Data: []byte{3, 4}}}}, UnmarshalAnalyticsFactSegmentRecord)
	roundTrip(t, AnalyticsSegmentMetadataRecord{RowCount: 2, MinCreatedAt: -4, MaxCreatedAt: 5, MinLogicalSize: 6, MaxLogicalSize: 7, MinRevision: 8, MaxRevision: 9, FirstCommit: 10, LastCommit: 11, ClassificationEpoch: 12, Bloom: []byte{13}, CodecParameters: "raw-v1"}, UnmarshalAnalyticsSegmentMetadataRecord)
	roundTrip(t, AnalyticsDimensionIndexRecord{Codec: AnalyticsCodecRoaring, RowCount: 10, MatchCount: 2, LogicalBytes: 99, Bitmap: []byte{1, 2, 3}}, UnmarshalAnalyticsDimensionIndexRecord)
	roundTrip(t, AnalyticsResidencyRecord{State: AnalyticsArchiveOnly, LastCompleteCrawl: 2, RetainedSnapshotRefs: 3, ClassificationEpoch: 4, FactSegment: 5, Row: 6}, UnmarshalAnalyticsResidencyRecord)
	creationDelta := AnalyticsDeltaRecord{Kind: AnalyticsDeltaCreation, FSID: 1, Inode: 2, IdentityGeneration: 3, Revision: 4, UID: 5, GID: 6, Known: KnownSize, CreatedAt: 7, LogicalSize: 8, CreationBasis: AnalyticsBirthTime, IdentityContinuity: AnalyticsContinuitySourceGeneration, ClassificationEpoch: 9, SVM: 10, Volume: 11, PathGroup: 12}
	encodedCreationDelta, err := creationDelta.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, creationDelta, UnmarshalAnalyticsDeltaRecord, len(encodedCreationDelta)-1)
	referenceDelta := AnalyticsDeltaRecord{Kind: AnalyticsDeltaRetainedReferences, FSID: 1, Inode: 2, IdentityGeneration: 3, Revision: 4, State: AnalyticsUnknown, RetainedSnapshotRefs: 9, ClassificationEpoch: 10, ReferenceOperation: AnalyticsReferencesSet}
	encodedReferenceDelta, err := referenceDelta.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, referenceDelta, UnmarshalAnalyticsDeltaRecord, len(encodedReferenceDelta)-1)
	scope := testID(44)
	roundTrip(t, AuthoritativeCrawlProofRecord{ScopeID: scope, RootFSID: 1, RootInode: 2, StartFence: 3, EndCommit: 4, CompletedAt: 5, Complete: true, DebtFree: true}, UnmarshalAuthoritativeCrawlProofRecord)
	roundTrip(t, AuthoritativeSourceBindingRecord{Generation: 1, Revision: 2, State: AuthoritativeSourceDeleted, Continuity: AnalyticsContinuityProven, LastObservedCommit: 3}, UnmarshalAuthoritativeSourceBindingRecord)
	roundTrip(t, AnalyticsWatermarkRecord{RepositoryGeneration: 1, AppliedCommit: 2, ManifestGeneration: 3, AppliedAt: 4}, UnmarshalAnalyticsWatermarkRecord)
	roundTrip(t, AnalyticsManifestRecord{Generation: 1, Segments: []uint64{2, 3, 5}}, UnmarshalAnalyticsManifestRecord)
	roundTrip(t, AnalyticsAggregateRecord{BytesAdded: 1, BytesDeleted: 2, FilesAdded: 3, FilesDeleted: 4}, UnmarshalAnalyticsAggregateRecord)
	roundTrip(t, AnalyticsSummaryRecord{ActiveBytes: 1, ActiveFiles: 2, UniqueBlobCount: 3, UniqueBlobBytes: 4}, UnmarshalAnalyticsSummaryRecord)
	roundTrip(t, AnalyticsUserInodeRecord{LatestRevision: 1, PathSample: "/home/alice/file"}, UnmarshalAnalyticsUserInodeRecord)
	roundTrip(t, AnalyticsUserBlobRecord{ReferenceCount: 2, FirstSeen: 3}, UnmarshalAnalyticsUserBlobRecord)
	roundTrip(t, AnalyticsQueryRecord{Payload: []byte(`{"count":2}`)}, UnmarshalAnalyticsQueryRecord)
	roundTrip(t, AnalyticsQueryJobRecord{State: AnalyticsQueryComplete, CanonicalQuery: []byte(`{"uid":600}`), RepositoryGeneration: 1, ClassificationEpoch: 2, AppliedCommit: 3, CompletedSegments: []uint64{4, 5}, RowsScanned: 6, UpdatedAt: 7, Result: []byte(`{"count":2}`)}, UnmarshalAnalyticsQueryJobRecord)
	for _, invalid := range []PlacementRecord{
		{State: PlacementState(99)},
		{State: PlacementLive, PlacedAt: 1},
		{State: PlacementLive, RetentionSource: RetentionUnknown, MinRetentionUntil: 1},
		{State: PlacementLive, DeleteAfter: 1},
	} {
		if _, err := invalid.MarshalBinary(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid placement record %#v returned %v", invalid, err)
		}
	}
	if _, err := (BackendPackRecord{State: PlacementState(99)}).MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("invalid backend-pack record returned %v", err)
	}
	if _, err := (PlacementDeleteRecord{}).MarshalBinary(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("invalid placement delete record returned %v", err)
	}
	roundTrip(t, HistoryMarker{UnixSeconds: 1700000000}, UnmarshalHistoryMarker)
	roundTrip(t, ExportCheckpointRecord{State: ExportComplete, CommitSequence: 8, Attempts: 1, RootKey: DirectoryRevisionKey(0, 0, 7)}, UnmarshalExportCheckpointRecord)
	roundTrip(t, SnapshotCommitRecord{SnapshotTimeUnixNano: 123, RootKey: DirectoryRevisionKey(0, 0, 7)}, UnmarshalSnapshotCommitRecord)
	roundTrip(t, PathVersionRecord{State: PathBound, NodeType: NodeFile, Inode: 2, Revision: 3}, UnmarshalPathVersionRecord)
	roundTrip(t, PathVersionRecord{State: PathTombstone}, UnmarshalPathVersionRecord)
	roundTrip(t, PathVersionRecord{State: PathOverflow}, UnmarshalPathVersionRecord)
	roundTrip(t, ExportIndexCheckpointRecord{Sequence: 9, PackIDs: []ID{id1, id2}}, UnmarshalExportIndexCheckpointRecord)
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
