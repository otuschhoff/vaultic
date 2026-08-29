package maintenance

import (
	"bytes"
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/pathindex"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func checkPathVersionIndex(ctx context.Context, store Store, paths []string, result *CheckResult, maxFindings uint) error {
	if len(paths) == 0 {
		return nil
	}
	expectedStore := &pathIndexDryRunStore{Store: store, puts: map[string][]byte{}, deletes: map[string]struct{}{}}
	_, err := pathindex.Rebuild(ctx, expectedStore, paths, false)
	if err != nil {
		return err
	}
	for key, value := range expectedStore.puts {
		current, found, getErr := store.Get(ctx, []byte(key))
		if getErr != nil {
			return getErr
		}
		if !found || !bytes.Equal(current, value) {
			result.PathVersionMismatch++
			addFinding(result, maxFindings, Finding{Kind: "path_version_drift", Key: fmt.Sprintf("%x", []byte(key))})
		}
	}
	for key := range expectedStore.deletes {
		result.PathVersionMismatch++
		addFinding(result, maxFindings, Finding{Kind: "stale_path_version", Key: fmt.Sprintf("%x", []byte(key))})
	}
	return nil
}

func RebuildPathVersionIndex(ctx context.Context, store Store, paths []string, dryRun bool) (pathindex.BuildResult, error) {
	if len(paths) == 0 {
		return pathindex.BuildResult{}, nil
	}
	return pathindex.Rebuild(ctx, pathIndexStore{Store: store}, paths, dryRun)
}

func PrunePathVersionIndex(ctx context.Context, store Store, beforeCommit uint64, dryRun bool) (uint64, error) {
	return pathindex.PruneBefore(ctx, pathIndexStore{Store: store}, beforeCommit, dryRun)
}

type pathIndexStore struct{ Store }

type pathIndexDryRunStore struct {
	Store
	puts    map[string][]byte
	deletes map[string]struct{}
}

func (store *pathIndexDryRunStore) WriteMutableBatch(_ context.Context, puts []daemon.Mutation, deletes [][]byte, _ bool) error {
	for _, put := range puts {
		store.puts[string(put.Key)] = append([]byte(nil), put.Value...)
	}
	for _, key := range deletes {
		store.deletes[string(key)] = struct{}{}
	}
	return nil
}

func countPathVersionRecords(ctx context.Context, store Store) (uint64, error) {
	var count uint64
	err := scan(ctx, store, []byte("pv:"), func(entry daemon.KeyValue) error {
		if parsed, err := schema.ParseKey(entry.Key); err != nil || parsed.Kind != schema.KeyPathVersion {
			return fmt.Errorf("invalid path-version key %q", entry.Key)
		}
		count++
		return nil
	})
	return count, err
}
