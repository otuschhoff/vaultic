package maintenance

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestSnapshotCommitIndexRebuildAndDriftDetection(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	snapshotID := deterministicID(81)
	rootKey := schema.DirectoryRevisionKey(0, 0, 7)
	store.set(t, rootKey, schema.DirectoryRevision{Children: nil, SourcePath: "/", Known: schema.KnownPath, Freshness: schema.FreshnessVerified})
	store.set(t, schema.SnapshotKey(schema.ID(snapshotID)), schema.SnapshotRecord{
		CommitSequence: 11, RootFSID: 0, RootInode: 0, RootRevision: 7,
		OriginalJSON: []byte(`{"time":"2026-08-29T12:34:56Z","tree":"x"}`),
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
