package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// TestIntrospectionRefusesLegacyRepositoriesExplicitly is Phase 11 step 8: a
// repository with no SlateDB pack catalog has no composition, pack, or history
// answer to give. Refusing is required; returning empty results would present
// "cannot know" as "nothing there".
func TestIntrospectionRefusesLegacyRepositoriesExplicitly(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata)}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil

	for name, run := range map[string]func(ctx context.Context, globalOptions global.Options) error{
		"stats": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexStats(ctx, indexStatsOptions{}, globalOptions, globalOptions.Term)
			return err
		},
		"packs": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexPacks(ctx, indexPacksOptions{Sort: "id"}, globalOptions, globalOptions.Term)
			return err
		},
		"history": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexHistory(ctx, indexHistoryOptions{Metric: "bytes", Bucket: "day"}, globalOptions, globalOptions.Term)
			return err
		},
		"history prune": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexHistoryPrune(ctx, indexHistoryPruneOptions{DryRun: true}, globalOptions, globalOptions.Term)
			return err
		},
		"backends --compare": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexBackends(ctx, indexBackendsOptions{Compare: true}, globalOptions, globalOptions.Term)
			return err
		},
		"file-history": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexFileHistory(ctx, indexFileHistoryOptions{}, "a.txt", globalOptions, globalOptions.Term)
			return err
		},
		"path-at": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexPathAt(ctx, indexPathAtOptions{Snapshot: vaultic.NewRandomID().String()}, "a.txt", globalOptions, globalOptions.Term)
			return err
		},
		"path-index": func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexPathIndex(ctx, indexPathIndexOptions{Paths: []string{"a.txt"}}, globalOptions, globalOptions.Term)
			return err
		},
	} {
		err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return run(ctx, globalOptions)
		})
		if !errors.Is(err, maintenance.ErrLegacyRepository) {
			t.Errorf("index %s on a legacy repository returned %v, want ErrLegacyRepository", name, err)
		}
	}
}

// TestBackendsRunsReducedOnLegacyRepositories: unlike the catalog commands,
// `index backends` still has a truthful answer without SlateDB, because the
// backends exist regardless of the metadata engine. It must give that answer
// and mark it as reduced rather than pretend it is complete.
func TestBackendsRunsReducedOnLegacyRepositories(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata)}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil

	var result BackendsResult
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		result, runErr = runIndexBackends(ctx, indexBackendsOptions{}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil {
		t.Fatalf("index backends failed on a legacy repository: %v", err)
	}
	if !result.ReducedMode {
		t.Fatal("a legacy repository was not reported as reduced mode")
	}
	if result.Compared {
		t.Fatal("a legacy repository reported a catalog comparison it cannot make")
	}
	if len(result.Backends) == 0 {
		t.Fatal("reduced mode reported no backends at all")
	}
	var packs uint64
	for _, report := range result.Backends {
		if !report.Listed {
			t.Fatalf("%s backend was not listed without --no-list", report.Role)
		}
		for _, count := range report.FileTypes {
			if count.FileType == "pack" {
				packs += count.Objects
			}
		}
	}
	if packs == 0 {
		t.Fatal("reduced mode found no pack objects on a repository that holds a backup")
	}
}

// listCountingBackend wraps a real backend and records which file types were
// listed, so an end-to-end test can prove --no-list rather than infer it.
type listCountingBackend struct {
	backend.Backend
	mutex sync.Mutex
	calls map[backend.FileType]int
}

func (target *listCountingBackend) List(ctx context.Context, fileType backend.FileType, fn func(backend.FileInfo) error) error {
	target.mutex.Lock()
	target.calls[fileType]++
	target.mutex.Unlock()
	return target.Backend.List(ctx, fileType, fn)
}

// TestBackendsNoListIssuesNoDataListingsEndToEnd runs the real command against
// a real backend. Opening and locking a repository legitimately lists keys and
// locks, so the guarantee asserted here is the one that matters on an archival
// backend: not a single pack, index, or snapshot listing.
func TestBackendsNoListIssuesNoDataListingsEndToEnd(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata)}, backupOptions{}, env.globalOptions)

	counter := &listCountingBackend{calls: map[backend.FileType]int{}}
	env.globalOptions.BackendTestHook = func(inner backend.Backend) (backend.Backend, error) {
		counter.Backend = inner
		return counter, nil
	}

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexBackends(ctx, indexBackendsOptions{NoList: true}, globalOptions, globalOptions.Term)
		for _, report := range result.Backends {
			if report.Listed {
				t.Errorf("%s backend claimed to be listed under --no-list", report.Role)
			}
		}
		return runErr
	})
	if err != nil {
		t.Fatalf("index backends --no-list failed: %v", err)
	}

	for _, fileType := range []backend.FileType{backend.PackFile, backend.IndexFile, backend.SnapshotFile} {
		if calls := counter.calls[fileType]; calls != 0 {
			t.Errorf("--no-list issued %d listings of %v", calls, fileType)
		}
	}
}
