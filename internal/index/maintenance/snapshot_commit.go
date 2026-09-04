package maintenance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func snapshotCommitMutations(ctx context.Context, store Store) ([]daemon.Mutation, error) {
	mutations := make([]daemon.Mutation, 0)
	err := scan(ctx, store, []byte("s:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeySnapshot {
			return fmt.Errorf("invalid snapshot key %q", entry.Key)
		}
		record, err := schema.UnmarshalSnapshotRecord(entry.Value)
		if err != nil {
			return err
		}
		rootKey := schema.DirectoryRevisionKey(record.RootFSID, record.RootInode, record.RootRevision)
		value, err := (schema.SnapshotCommitRecord{
			SnapshotTimeUnixNano: snapshotJSONTimeUnixNano(record.OriginalJSON),
			RootKey:              rootKey,
		}).MarshalBinary()
		if err != nil {
			return err
		}
		mutations = append(mutations, daemon.Mutation{Key: schema.SnapshotCommitKey(record.CommitSequence, parsed.ID), Value: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })
	return mutations, nil
}

func checkSnapshotCommitIndex(ctx context.Context, store Store, result *CheckResult, maxFindings uint) error {
	expected, err := snapshotCommitMutations(ctx, store)
	if err != nil {
		return err
	}
	expectedByKey := make(map[string]daemon.Mutation, len(expected))
	for _, mutation := range expected {
		expectedByKey[string(mutation.Key)] = mutation
		value, found, getErr := store.Get(ctx, mutation.Key)
		if getErr != nil {
			return getErr
		}
		if !found || !bytes.Equal(value, mutation.Value) {
			result.SnapshotCommitMismatch++
			parsed, _ := schema.ParseKey(mutation.Key)
			addFinding(
				result,
				maxFindings,
				Finding{Kind: "snapshot_commit_drift", Key: vaultic.ID(parsed.ID).String(), Want: fmt.Sprintf("commit=%d", parsed.Revision)},
			)
		}
	}
	return scan(ctx, store, schema.SnapshotCommitPrefix(), func(entry daemon.KeyValue) error {
		if _, ok := expectedByKey[string(entry.Key)]; ok {
			return nil
		}
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeySnapshotCommit {
			result.SnapshotCommitMismatch++
			addFinding(result, maxFindings, Finding{Kind: "snapshot_commit_malformed", Key: fmt.Sprintf("%x", entry.Key)})
			return nil
		}
		result.SnapshotCommitMismatch++
		addFinding(
			result,
			maxFindings,
			Finding{Kind: "stale_snapshot_commit", Key: vaultic.ID(parsed.ID).String(), Got: fmt.Sprintf("commit=%d", parsed.Revision)},
		)
		return nil
	})
}

func RebuildSnapshotCommitIndex(ctx context.Context, store Store, dryRun bool) (uint64, error) {
	expected, err := snapshotCommitMutations(ctx, store)
	if err != nil {
		return 0, err
	}
	expectedByKey := make(map[string]daemon.Mutation, len(expected))
	for _, mutation := range expected {
		expectedByKey[string(mutation.Key)] = mutation
	}
	deletes := make([][]byte, 0)
	if err := scan(ctx, store, schema.SnapshotCommitPrefix(), func(entry daemon.KeyValue) error {
		if _, ok := expectedByKey[string(entry.Key)]; !ok {
			deletes = append(deletes, append([]byte(nil), entry.Key...))
		}
		return nil
	}); err != nil {
		return 0, err
	}
	var changes uint64
	for _, mutation := range expected {
		value, found, getErr := store.Get(ctx, mutation.Key)
		if getErr != nil {
			return 0, getErr
		}
		if !found || !bytes.Equal(value, mutation.Value) {
			changes++
		}
	}
	changes += uint64(len(deletes))
	if dryRun || changes == 0 {
		return changes, nil
	}
	return changes, store.WriteMutableBatch(ctx, expected, deletes, false)
}

func snapshotJSONTimeUnixNano(original []byte) int64 {
	var decoded struct {
		Time time.Time `json:"time"`
	}
	if len(original) == 0 || json.Unmarshal(original, &decoded) != nil || decoded.Time.IsZero() {
		return 0
	}
	return decoded.Time.UnixNano()
}
