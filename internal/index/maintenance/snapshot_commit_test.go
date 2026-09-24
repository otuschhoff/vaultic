package maintenance

import (
	"context"
	"fmt"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestHistoricalSnapshotRootValidation(t *testing.T) {
	for _, mode := range []string{"tree", "missing", "data"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := &memoryStore{values: make(map[string][]byte)}
			snapshotID, treeID := deterministicID(85), deterministicID(86)
			original := []byte(fmt.Sprintf(`{"tree":"%s"}`, treeID.String()))
			store.set(t, schema.SnapshotKey(schema.ID(snapshotID)), schema.SnapshotRecord{LegacyTree: schema.ID(treeID), OriginalJSON: original})
			if mode != "missing" {
				kind := schema.BlobTree
				if mode == "data" {
					kind = schema.BlobData
				}
				store.set(t, schema.BlobKey(schema.ID(treeID)), schema.BlobRecord{Locations: []schema.BlobLocation{{Type: kind, Length: 1}}})
			}
			scratch, err := newCheckScratch(t.TempDir(), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer scratch.close()
			source := &memoryDestination{snapshots: map[vaultic.ID][]byte{snapshotID: original}}
			var result CheckResult
			if err := checkSnapshots(ctx, source, store, scratch, 1<<16, false, &result, 10); err != nil {
				t.Fatal(err)
			}
			if result.LegacySnapshots != 1 || result.SlateDBSnapshots != 1 || result.UnresolvedSnapshots != 0 || result.SnapshotCommitMismatch != 0 {
				t.Fatalf("incorrect historical coverage: %#v", result)
			}
			if (result.SnapshotMismatch != 0) != (mode != "tree") {
				t.Fatalf("unexpected snapshot mismatches: %#v", result)
			}
		})
	}
}

func TestSnapshotCommitIndexRebuildAndDriftDetection(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	snapshotID := deterministicID(81)
	rootKey := schema.DirectoryRevisionKey(0, 0, 7)
	store.set(t, rootKey, schema.DirectoryRevision{Children: nil, SourcePath: "/", Known: schema.KnownPath, Freshness: schema.FreshnessVerified})
	store.set(t, schema.SnapshotKey(schema.ID(snapshotID)), schema.SnapshotRecord{
		CommitSequence: 11, RootFSID: 0, RootInode: 0, RootRevision: 7,
		OriginalJSON: []byte(`{"time":"2026-08-29T12:34:56Z","tree":"x"}`),
	})
	legacyID, legacyTree := deterministicID(83), deterministicID(84)
	store.set(t, schema.SnapshotKey(schema.ID(legacyID)), schema.SnapshotRecord{
		LegacyTree: schema.ID(legacyTree), OriginalJSON: []byte(fmt.Sprintf(`{"tree":"%x"}`, legacyTree[:])),
	})

	result := CheckResult{}
	if err := checkSnapshotCommitIndex(context.Background(), store, &result, 10); err != nil {
		t.Fatal(err)
	}
	if result.SnapshotCommitMismatch != 1 {
		t.Fatalf("missing sc drift = %d, want 1", result.SnapshotCommitMismatch)
	}
	changed, err := RebuildSnapshotCommitIndex(context.Background(), store, true)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("dry-run rebuild changed = %d, want 1", changed)
	}
	if _, found, _ := store.Get(context.Background(), schema.SnapshotCommitKey(11, schema.ID(snapshotID))); found {
		t.Fatal("dry-run wrote sc record")
	}
	changed, err = RebuildSnapshotCommitIndex(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("rebuild changed = %d, want 1", changed)
	}
	value, found, err := store.Get(context.Background(), schema.SnapshotCommitKey(11, schema.ID(snapshotID)))
	if err != nil || !found {
		t.Fatalf("sc missing after rebuild: found=%v err=%v", found, err)
	}
	record, err := schema.UnmarshalSnapshotCommitRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if record.SnapshotTimeUnixNano == 0 || string(record.RootKey) != string(rootKey) {
		t.Fatalf("sc record = %#v", record)
	}

	staleSnapshot := deterministicID(82)
	store.set(t, schema.SnapshotCommitKey(99, schema.ID(staleSnapshot)), schema.SnapshotCommitRecord{RootKey: rootKey})
	changed, err = RebuildSnapshotCommitIndex(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("stale rebuild changed = %d, want 1", changed)
	}
	if _, found, err := store.Get(context.Background(), schema.SnapshotCommitKey(99, schema.ID(staleSnapshot))); err != nil || found {
		t.Fatalf("stale sc survived: found=%v err=%v", found, err)
	}
}
