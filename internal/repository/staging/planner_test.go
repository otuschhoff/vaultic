package staging

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type plannerStore map[string][]byte

func (store plannerStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	value, found := store[string(key)]
	return value, found, nil
}

func TestBuildDaemonCommitPlanMergesBlobLocationsAndRejectsMutableFacts(t *testing.T) {
	blobID := schema.ID{1}
	firstPack := schema.ID{2}
	secondPack := schema.ID{3}
	snapshotID := schema.ID{4}
	firstBlob, err := (schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: firstPack, Offset: 1, Length: 2, UncompressedSize: 3, Type: schema.BlobData}}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	secondBlob, err := (schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: secondPack, Offset: 4, Length: 5, UncompressedSize: 6, Type: schema.BlobData}}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := (schema.SnapshotRecord{CommitSequence: 1, RootFSID: 1, RootInode: 2, RootRevision: 3, OriginalJSON: []byte(`{"tree":"root"}`)}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	blobFact, err := SchemaFactRecord(schema.BlobKey(blobID), secondBlob)
	if err != nil {
		t.Fatal(err)
	}
	snapshotFact, err := SchemaFactRecord(schema.SnapshotKey(snapshotID), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildDaemonCommitPlan(context.Background(), plannerStore{string(schema.BlobKey(blobID)): firstBlob}, []Segment{{Records: []Record{snapshotFact, blobFact}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Puts) != 2 || plan.SnapshotID != "0400000000000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("plan = %#v", plan)
	}
	var merged schema.BlobRecord
	for _, put := range plan.Puts {
		if string(put.Key) == string(schema.BlobKey(blobID)) {
			merged, err = schema.UnmarshalBlobRecord(put.Value)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(merged.Locations) != 2 {
		t.Fatalf("merged locations = %#v", merged.Locations)
	}
	if _, err := SchemaFactRecord(schema.ReferenceCountKey(blobID), []byte{0}); err == nil {
		t.Fatal("mutable derived reference count was accepted as a journal fact")
	}
}
