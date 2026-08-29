package index

import (
	"reflect"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var testPublishTime = time.Unix(1_700_000_000, 0)

// TestSchemaPackReportsAccumulatedPayloadSize guards against a regression
// where the returned record's PayloadSize/Type were snapshotted before the
// blob-accumulation loop ran, silently publishing every pack with a zero
// payload size regardless of its actual contents.
func TestSchemaPackReportsAccumulatedPayloadSize(t *testing.T) {
	packID := vaultic.NewRandomID()
	blobs := pack.Blobs{
		{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Offset: 0, Length: 53, UncompressedLength: 12},
		{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.TreeBlob}, Offset: 53, Length: 30, UncompressedLength: 20},
	}
	published, err := schemaPack(packID, blobs, 0, false, TierPolicy{}, testPublishTime)
	if err != nil {
		t.Fatal(err)
	}
	if published.Record.BlobCount != 2 || published.Record.PayloadSize != 83 || published.Record.Type != schema.PackMixed {
		t.Fatalf("published record = %#v", published.Record)
	}
	if len(published.Blobs) != 2 {
		t.Fatalf("published blobs = %#v", published.Blobs)
	}

	sized, err := schemaPack(packID, blobs, 100, true, TierPolicy{}, testPublishTime)
	if err != nil {
		t.Fatal(err)
	}
	if !sized.Record.PhysicalSizeKnown || sized.Record.PhysicalSize != 100 || sized.Record.PayloadSize != 83 || sized.Record.HeaderSize != 17 {
		t.Fatalf("sized published record = %#v", sized.Record)
	}

	if _, err := schemaPack(packID, blobs, 10, true, TierPolicy{}, testPublishTime); err == nil {
		t.Fatal("physical size smaller than payload was accepted")
	}
	if _, err := schemaPack(packID, nil, 0, false, TierPolicy{}, testPublishTime); err == nil {
		t.Fatal("empty pack was accepted")
	}
}

func blobsOfType(t *testing.T, types ...vaultic.BlobType) pack.Blobs {
	t.Helper()
	blobs := make(pack.Blobs, 0, len(types))
	var offset uint
	for _, blobType := range types {
		blobs = append(blobs, pack.Blob{
			BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: blobType},
			Offset:     offset, Length: 10, UncompressedLength: 8,
		})
		offset += 10
	}
	return blobs
}

func TestSchemaPackDerivesPlacementRecordsFromTier(t *testing.T) {
	policy := TierPolicy{Resolved: true, HotCold: true, Backends: []PlacementBackendPolicy{
		{ID: "hot", Hash: 1, Role: "primary"},
		{ID: "cold", Hash: 2, Role: "archival", StorageClass: "GLACIER", MinRetention: 180 * 24 * time.Hour},
	}}

	data, err := schemaPack(vaultic.NewRandomID(), blobsOfType(t, vaultic.DataBlob), 100, true, policy, testPublishTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Placements) != 1 {
		t.Fatalf("data pack placements = %#v, want cold only", data.Placements)
	}
	cold := data.Placements[2]
	if cold.State != schema.PlacementLive || cold.Bytes != 100 || cold.StorageClass != "GLACIER" {
		t.Fatalf("cold placement = %#v", cold)
	}
	if cold.RetentionSource != schema.RetentionConfig || cold.MinRetentionUntil != testPublishTime.Add(180*24*time.Hour).UnixNano() {
		t.Fatalf("cold retention = %#v", cold)
	}
	if data.Record.RetentionSource != schema.RetentionUnknown || data.Record.MinRetentionUntil != 0 {
		t.Fatalf("pack-level retention was not moved to placement: %#v", data.Record)
	}

	tree, err := schemaPack(vaultic.NewRandomID(), blobsOfType(t, vaultic.TreeBlob), 100, true, policy, testPublishTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(tree.Placements) != 2 {
		t.Fatalf("tree pack placements = %#v, want hot+cold", tree.Placements)
	}
	if _, ok := tree.Placements[1]; !ok {
		t.Fatalf("tree pack lacks hot placement: %#v", tree.Placements)
	}
	if _, ok := tree.Placements[2]; !ok {
		t.Fatalf("tree pack lacks cold placement: %#v", tree.Placements)
	}
}

func TestSchemaPackDoesNotInventUnknownPlacements(t *testing.T) {
	policy := TierPolicy{Backends: []PlacementBackendPolicy{{ID: "single", Hash: 1, Role: "primary"}}}
	published, err := schemaPack(vaultic.NewRandomID(), blobsOfType(t, vaultic.DataBlob), 100, true, policy, testPublishTime)
	if err != nil {
		t.Fatal(err)
	}
	if published.Record.Tier != schema.TierUnknown {
		t.Fatalf("unresolved policy recorded tier %#v", published.Record.Tier)
	}
	if len(published.Placements) != 0 {
		t.Fatalf("unresolved policy invented placements: %#v", published.Placements)
	}
}

// TestTierIsRecordedFromActualRouting pins the tier assigned at publish time
// for every pack type, in both repository layouts.
//
// A tree pack in a hot/cold repository is mirrored rather than hot, because
// hotcold.Save writes hot files to the hot backend and then mirrors them to
// the cold backend. Recording such a pack as hot-only would claim a placement
// that never happens.
func TestTierIsRecordedFromActualRouting(t *testing.T) {
	dataBlobs := blobsOfType(t, vaultic.DataBlob)
	treeBlobs := blobsOfType(t, vaultic.TreeBlob)
	mixedBlobs := blobsOfType(t, vaultic.DataBlob, vaultic.TreeBlob)

	for _, testCase := range []struct {
		name     string
		policy   TierPolicy
		blobs    pack.Blobs
		packType schema.PackType
		tier     schema.PackTier
	}{
		{"hotcold data", TierPolicy{Resolved: true, HotCold: true}, dataBlobs, schema.PackData, schema.TierCold},
		{"hotcold tree", TierPolicy{Resolved: true, HotCold: true}, treeBlobs, schema.PackTree, schema.TierMirrored},
		{"hotcold mixed", TierPolicy{Resolved: true, HotCold: true}, mixedBlobs, schema.PackMixed, schema.TierUnknown},
		{"single data", TierPolicy{Resolved: true}, dataBlobs, schema.PackData, schema.TierSingle},
		{"single tree", TierPolicy{Resolved: true}, treeBlobs, schema.PackTree, schema.TierSingle},
		{"single mixed", TierPolicy{Resolved: true}, mixedBlobs, schema.PackMixed, schema.TierSingle},
		// An engine whose routing was never established must not claim one.
		{"unresolved data", TierPolicy{}, dataBlobs, schema.PackData, schema.TierUnknown},
		{"unresolved tree", TierPolicy{}, treeBlobs, schema.PackTree, schema.TierUnknown},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			published, err := schemaPack(vaultic.NewRandomID(), testCase.blobs, 0, false, testCase.policy, testPublishTime)
			if err != nil {
				t.Fatal(err)
			}
			if published.Record.Type != testCase.packType {
				t.Fatalf("pack type = %v, want %v", published.Record.Type, testCase.packType)
			}
			if published.Record.Tier != testCase.tier {
				t.Fatalf("tier = %v, want %v", published.Record.Tier, testCase.tier)
			}
			// A pack vaultic writes itself always has a known creation time.
			if !published.Record.CreationTimeKnown || published.Record.CreationTime != testPublishTime.UnixNano() {
				t.Fatalf("creation time = %d/%t", published.Record.CreationTime, published.Record.CreationTimeKnown)
			}
			// Phase 9 records facts only; with no configured retention the
			// deadline must stay unknown rather than defaulting to zero-as-now.
			if published.Record.RetentionSource != schema.RetentionUnknown || published.Record.MinRetentionUntil != 0 {
				t.Fatalf("unconfigured retention = %v/%d", published.Record.RetentionSource, published.Record.MinRetentionUntil)
			}
			// Usage is only established by reachability analysis, never at
			// publish time.
			if published.Record.UsageKnown {
				t.Fatal("publish claimed usage accounting")
			}
			if _, err := published.Record.MarshalBinary(); err != nil {
				t.Fatalf("published record does not satisfy the schema invariants: %v", err)
			}
		})
	}
}

// TestConfiguredRetentionAppliesOnlyToColdBytes checks that a retention
// deadline is recorded exactly where cold bytes exist, and never without a
// known creation time to anchor it.
func TestConfiguredRetentionAppliesOnlyToColdBytes(t *testing.T) {
	policy := TierPolicy{Resolved: true, HotCold: true, MinRetention: 180 * 24 * time.Hour, StorageClass: "GLACIER"}
	want := testPublishTime.Add(policy.MinRetention).UnixNano()

	for _, testCase := range []struct {
		name      string
		blobs     pack.Blobs
		retention schema.RetentionSource
		deadline  int64
	}{
		{"data is cold", blobsOfType(t, vaultic.DataBlob), schema.RetentionConfig, want},
		{"tree is mirrored and has a cold copy", blobsOfType(t, vaultic.TreeBlob), schema.RetentionConfig, want},
		{"unknown tier gets no deadline", blobsOfType(t, vaultic.DataBlob, vaultic.TreeBlob), schema.RetentionUnknown, 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			published, err := schemaPack(vaultic.NewRandomID(), testCase.blobs, 0, false, policy, testPublishTime)
			if err != nil {
				t.Fatal(err)
			}
			if published.Record.RetentionSource != testCase.retention || published.Record.MinRetentionUntil != testCase.deadline {
				t.Fatalf("retention = %v/%d, want %v/%d", published.Record.RetentionSource, published.Record.MinRetentionUntil, testCase.retention, testCase.deadline)
			}
			if published.Record.StorageClass != "GLACIER" {
				t.Fatalf("storage class = %q", published.Record.StorageClass)
			}
			encoded, err := published.Record.MarshalBinary()
			if err != nil {
				t.Fatalf("published record does not satisfy the schema invariants: %v", err)
			}
			decoded, err := schema.UnmarshalPackRecord(encoded)
			if err != nil {
				t.Fatal(err)
			}
			want := published.Record
			if want.SourceIndexIDs == nil {
				want.SourceIndexIDs = []schema.ID{}
			}
			if !reflect.DeepEqual(decoded, want) {
				t.Fatalf("round trip = %#v", decoded)
			}
		})
	}
}

// TestPacksPublishedWithoutATierPolicyStayUnknown guards the conservative
// default: an engine whose routing was never established must record
// tier-unknown rather than positively claiming a single-backend repository,
// which would be wrong for every hot/cold repository wired without a policy.
func TestPacksPublishedWithoutATierPolicyStayUnknown(t *testing.T) {
	engine := NewDaemonEngine(nil)
	policy := engine.tierPolicy()
	if policy.Resolved || policy.HotCold {
		t.Fatalf("default policy = %#v", policy)
	}
	published, err := schemaPack(vaultic.NewRandomID(), blobsOfType(t, vaultic.DataBlob), 0, false, policy, testPublishTime)
	if err != nil {
		t.Fatal(err)
	}
	if published.Record.Tier != schema.TierUnknown {
		t.Fatalf("unwired engine recorded tier %v", published.Record.Tier)
	}
}
