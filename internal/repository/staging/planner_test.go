package staging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	indexreconcile "github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type plannerStore map[string][]byte

func observationRecord(t *testing.T) Record {
	t.Helper()
	payload, err := json.Marshal(
		indexreconcile.DeferredObservation{
			SnapshotPath: "/",
			SourcePath:   "/",
			Node:         data.Node{Type: "dir"},
			Stat:         indexreconcile.DeferredStat{DeviceID: 1, Inode: 1},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return Record{Kind: indexreconcile.DeferredObservationKind, Payload: payload}
}

func (store plannerStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	value, found := store[string(key)]
	return value, found, nil
}

func TestBuildDaemonCommitPlanDerivesProducerFacts(t *testing.T) {
	packID := strings.Repeat("11", 32)
	blobID := strings.Repeat("22", 32)
	blobPayload, err := json.Marshal(BlobFact{ID: blobID, Type: "data", PackID: packID, Offset: 3, Length: 7})
	if err != nil {
		t.Fatal(err)
	}
	snapshotJSON := []byte(`{"tree":"0123456789abcdef"}`)
	segment := Segment{
		Header: Header{CreatedAt: time.Unix(100, 0)},
		Packs: []Pack{
			{
				ID:          packID,
				Type:        "data",
				Size:        100,
				PayloadSize: 7,
				HeaderSize:  93,
				BlobCount:   1,
				SHA256:      packID,
				Placements:  []Placement{{BackendID: "a", FailureDomain: "site-a", Size: 100, SHA256: packID}},
			},
		},
		Records: []Record{
			{Kind: "blob-fact-v1", Payload: blobPayload},
			{Kind: "prospective-snapshot-v1", Payload: snapshotJSON},
			observationRecord(t),
		},
	}
	plan, err := BuildDaemonCommitPlan(context.Background(), plannerStore{}, []Segment{segment})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(snapshotJSON)
	if plan.SnapshotID != hex.EncodeToString(digest[:]) || string(plan.SnapshotJSON) != string(snapshotJSON) ||
		len(plan.Puts) != 4 {
		t.Fatalf("derived plan = %#v", plan)
	}
}

func TestBuildDaemonCommitPlanMergesBlobLocationsAndRejectsMutableFacts(t *testing.T) {
	blobID := schema.ID{1}
	firstPack := schema.ID{2}
	secondPack := schema.ID{3}
	snapshotID := schema.ID{4}
	firstBlob,
		err := (schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: firstPack,
		Offset:           1,
		Length:           2,
		UncompressedSize: 3,
		Type:             schema.BlobData}}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	secondBlob,
		err := (schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: secondPack,
		Offset:           4,
		Length:           5,
		UncompressedSize: 6,
		Type:             schema.BlobData}}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	snapshot,
		err := (schema.SnapshotRecord{CommitSequence: 1,
		RootFSID:     1,
		RootInode:    2,
		RootRevision: 3,
		OriginalJSON: []byte(`{"tree":"root"}`)}).MarshalBinary()
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
	plan, err := BuildDaemonCommitPlan(
		context.Background(),
		plannerStore{string(schema.BlobKey(blobID)): firstBlob},
		[]Segment{{Records: []Record{snapshotFact, blobFact, observationRecord(t)}}},
	)
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
