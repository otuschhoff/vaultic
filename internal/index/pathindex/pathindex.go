package pathindex

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	indexhistory "github.com/otuschhoff/vaultic/internal/index/history"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type Store interface {
	indexhistory.Store
	WriteMutableBatch(context.Context, []daemon.Mutation, [][]byte, bool) error
}

type BuildResult struct {
	SnapshotsScanned uint64 `json:"snapshots_scanned"`
	BindingsChanged  uint64 `json:"bindings_changed"`
	OverflowPaths    uint64 `json:"overflow_paths"`
	BytesWritten     uint64 `json:"bytes_written"`
}

func Rebuild(ctx context.Context, store Store, paths []string, dryRun bool) (BuildResult, error) {
	var result BuildResult
	if len(paths) == 0 {
		return result, fmt.Errorf("path index rebuild requires at least one path")
	}
	mutations, err := expectedMutations(ctx, store, paths, &result)
	if err != nil {
		return result, err
	}
	deletes, err := staleKeys(ctx, store, paths, mutations)
	if err != nil {
		return result, err
	}
	result.BindingsChanged += uint64(len(deletes))
	if dryRun || (len(mutations) == 0 && len(deletes) == 0) {
		return result, nil
	}
	return result, store.WriteMutableBatch(ctx, mutations, deletes, false)
}

func PruneBefore(ctx context.Context, store Store, beforeCommit uint64, dryRun bool) (uint64, error) {
	if beforeCommit == 0 {
		return 0, nil
	}
	deletes := make([][]byte, 0)
	if err := scan(ctx, store, []byte("pv:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPathVersion {
			return fmt.Errorf("invalid path-version key %q", entry.Key)
		}
		if parsed.Revision < beforeCommit {
			deletes = append(deletes, append([]byte(nil), entry.Key...))
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if dryRun || len(deletes) == 0 {
		return uint64(len(deletes)), nil
	}
	return uint64(len(deletes)), store.WriteMutableBatch(ctx, nil, deletes, false)
}

func expectedMutations(ctx context.Context, store Store, paths []string, result *BuildResult) ([]daemon.Mutation, error) {
	commits, err := commits(ctx, store)
	if err != nil {
		return nil, err
	}
	result.SnapshotsScanned = uint64(len(commits))
	mutations := make([]daemon.Mutation, 0)
	for _, target := range paths {
		target = cleanPath(target)
		if len(target) > schema.MaxPathIndexPathBytes {
			result.OverflowPaths++
			for _, commit := range commits {
				key := schema.PathOverflowKey(0, target, commit)
				value, err := (schema.PathVersionRecord{State: schema.PathOverflow, Path: target}).MarshalBinary()
				if err != nil {
					return nil, err
				}
				if needsWrite(ctx, store, key, value) {
					mutations = append(mutations, daemon.Mutation{Key: key, Value: value})
					result.BindingsChanged++
					result.BytesWritten += uint64(len(key) + len(value))
				}
			}
			continue
		}
		var previous indexhistory.Binding
		for index, commit := range commits {
			binding, err := indexhistory.ResolvePathAtCommit(ctx, store, target, commit)
			if err != nil {
				return nil, err
			}
			if index > 0 && sameBinding(previous, binding) {
				continue
			}
			record := schema.PathVersionRecord{State: schema.PathTombstone}
			if !binding.Covered {
				record.State = schema.PathTombstone
			} else if binding.Present {
				record = schema.PathVersionRecord{State: schema.PathBound, NodeType: binding.NodeType, Inode: binding.Inode, Revision: binding.Revision}
			}
			key := schema.PathVersionKey(0, target, commit)
			if key == nil {
				result.OverflowPaths++
				continue
			}
			value, err := record.MarshalBinary()
			if err != nil {
				return nil, err
			}
			if needsWrite(ctx, store, key, value) {
				mutations = append(mutations, daemon.Mutation{Key: key, Value: value})
				result.BindingsChanged++
				result.BytesWritten += uint64(len(key) + len(value))
			}
			previous = binding
		}
	}
	sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })
	return mutations, nil
}

func needsWrite(ctx context.Context, store Store, key, value []byte) bool {
	current, found, err := store.Get(ctx, key)
	return err != nil || !found || !bytes.Equal(current, value)
}

func staleKeys(ctx context.Context, store Store, paths []string, expected []daemon.Mutation) ([][]byte, error) {
	expectedKeys := make(map[string]struct{}, len(expected))
	for _, mutation := range expected {
		expectedKeys[string(mutation.Key)] = struct{}{}
	}
	deletes := make([][]byte, 0)
	for _, target := range paths {
		prefix := schema.PathVersionPrefix(0, cleanPath(target))
		if prefix == nil {
			continue
		}
		if err := scan(ctx, store, prefix, func(entry daemon.KeyValue) error {
			if _, ok := expectedKeys[string(entry.Key)]; !ok {
				deletes = append(deletes, append([]byte(nil), entry.Key...))
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return deletes, nil
}

func commits(ctx context.Context, store Store) ([]uint64, error) {
	result := make([]uint64, 0)
	if err := scan(ctx, store, schema.SnapshotCommitPrefix(), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeySnapshotCommit {
			return fmt.Errorf("invalid snapshot commit key %q", entry.Key)
		}
		result = append(result, parsed.Revision)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func sameBinding(left, right indexhistory.Binding) bool {
	return left.Covered == right.Covered && left.Present == right.Present && left.Inode == right.Inode && left.Revision == right.Revision && left.NodeType == right.NodeType
}

func cleanPath(value string) string {
	value = strings.TrimPrefix(strings.TrimSpace(value), "/")
	if value == "" {
		return "."
	}
	return value
}

func scan(ctx context.Context, store Store, prefix []byte, visit func(daemon.KeyValue) error) error {
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, after, 10_000)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := visit(entry); err != nil {
				return err
			}
			after = append(after[:0], entry.Key...)
		}
		if done {
			return nil
		}
		if len(entries) == 0 {
			return fmt.Errorf("scan %q made no progress", prefix)
		}
	}
}
