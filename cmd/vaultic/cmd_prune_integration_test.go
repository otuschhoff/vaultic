package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
)

func testRunPrune(t testing.TB, gopts global.Options, opts PruneOptions) {
	t.Helper()
	rtest.OK(t, testRunPruneOutput(t, gopts, opts))
}

func testRunPruneMustFail(t testing.TB, gopts global.Options, opts PruneOptions) {
	t.Helper()
	err := testRunPruneOutput(t, gopts, opts)
	rtest.Assert(t, err != nil, "expected non nil error")
}

func testRunPruneOutput(t testing.TB, gopts global.Options, opts PruneOptions) error {
	oldHook := gopts.BackendTestHook
	gopts.BackendTestHook = func(r backend.Backend) (backend.Backend, error) { return newListOnceBackend(r), nil }
	defer func() {
		gopts.BackendTestHook = oldHook
	}()
	return withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runPrune(context.TODO(), opts, gopts, gopts.Term)
	})
}

func TestPrune(t *testing.T) {
	testPruneVariants(t, false)
	testPruneVariants(t, true)
}

func testPruneVariants(t *testing.T, unsafeNoSpaceRecovery bool) {
	suffix := ""
	if unsafeNoSpaceRecovery {
		suffix = "-recovery"
	}
	t.Run("0"+suffix, func(t *testing.T) {
		opts := PruneOptions{MaxUnused: "0%", unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := CheckOptions{ReadData: true, CheckUnused: !unsafeNoSpaceRecovery}
		testPrune(t, opts, checkOpts)
	})

	t.Run("50"+suffix, func(t *testing.T) {
		opts := PruneOptions{MaxUnused: "50%", unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := CheckOptions{ReadData: true}
		testPrune(t, opts, checkOpts)
	})

	t.Run("unlimited"+suffix, func(t *testing.T) {
		opts := PruneOptions{MaxUnused: "unlimited", unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := CheckOptions{ReadData: true}
		testPrune(t, opts, checkOpts)
	})

	t.Run("CacheableOnly"+suffix, func(t *testing.T) {
		opts := PruneOptions{MaxUnused: "5%", RepackCacheableOnly: true, unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := CheckOptions{ReadData: true}
		testPrune(t, opts, checkOpts)
	})
}

func createPrunableRepo(t *testing.T, env *testEnvironment) {
	testSetupBackupData(t, env)
	opts := BackupOptions{}

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	firstSnapshot := testListSnapshots(t, env.gopts, 1)[0]

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)
	testListSnapshots(t, env.gopts, 3)

	testRunForgetJSON(t, env.gopts)
	testRunForget(t, env.gopts, ForgetOptions{}, firstSnapshot.String())
}

func testRunForgetJSON(t testing.TB, gopts global.Options, args ...string) {
	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.JSON = true
		opts := ForgetOptions{
			DryRun: true,
			Last:   1,
		}
		pruneOpts := PruneOptions{
			MaxUnused: "5%",
		}
		return runForget(context.TODO(), opts, pruneOpts, gopts, gopts.Term, args)
	})
	rtest.OK(t, err)

	var forgets []*ForgetGroup
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &forgets))

	rtest.Assert(t, len(forgets) == 1,
		"Expected 1 snapshot group, got %v", len(forgets))
	rtest.Assert(t, len(forgets[0].Keep) == 1,
		"Expected 1 snapshot to be kept, got %v", len(forgets[0].Keep))
	rtest.Assert(t, len(forgets[0].Remove) == 2,
		"Expected 2 snapshots to be removed, got %v", len(forgets[0].Remove))
}

func testPrune(t *testing.T, pruneOpts PruneOptions, checkOpts CheckOptions) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPrune(t, env.gopts, pruneOpts)
	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		_, err := runCheck(context.TODO(), checkOpts, gopts, nil, gopts.Term)
		return err
	}))
}

var pruneDefaultOptions = PruneOptions{MaxUnused: "5%"}

func TestPruneWithDamagedRepository(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	datafile := filepath.Join("testdata", "backup-data.tar.gz")
	testRunInit(t, env.gopts)

	rtest.SetupTarTestFixture(t, env.testdata, datafile)
	opts := BackupOptions{}

	// create and delete snapshot to create unused blobs
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	firstSnapshot := testListSnapshots(t, env.gopts, 1)[0]
	testRunForget(t, env.gopts, ForgetOptions{}, firstSnapshot.String())

	oldPacks := listPacks(env.gopts, t)

	// create new snapshot, but lose all data
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)
	testListSnapshots(t, env.gopts, 1)
	removePacksExcept(env.gopts, t, oldPacks, false)

	oldHook := env.gopts.BackendTestHook
	env.gopts.BackendTestHook = func(r backend.Backend) (backend.Backend, error) { return newListOnceBackend(r), nil }
	defer func() {
		env.gopts.BackendTestHook = oldHook
	}()
	// prune should fail
	rtest.Equals(t, repository.ErrPacksMissing, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		return runPrune(context.TODO(), pruneDefaultOptions, gopts, gopts.Term)
	}), "prune should have reported index not complete error")
}

// Test repos for edge cases
func TestEdgeCaseRepos(t *testing.T) {
	opts := CheckOptions{}

	// repo where index is completely missing
	// => check and prune should fail
	t.Run("no-index", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-index-missing.tar.gz", opts, pruneDefaultOptions, false, false)
	})

	// repo where an existing and used blob is missing from the index
	// => check and prune should fail
	t.Run("index-missing-blob", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-index-missing-blob.tar.gz", opts, pruneDefaultOptions, false, false)
	})

	// repo where a blob is missing
	// => check and prune should fail
	t.Run("missing-data", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-data-missing.tar.gz", opts, pruneDefaultOptions, false, false)
	})

	// repo where blobs which are not needed are missing or in invalid pack files
	// => check should fail and prune should repair this
	t.Run("missing-unused-data", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-unused-data-missing.tar.gz", opts, pruneDefaultOptions, false, true)
	})

	// repo where data exists that is not referenced
	// => check and prune should fully work
	t.Run("unreferenced-data", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-unreferenced-data.tar.gz", opts, pruneDefaultOptions, true, true)
	})

	// repo where an obsolete index still exists
	// => check and prune should fully work
	t.Run("obsolete-index", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-obsolete-index.tar.gz", opts, pruneDefaultOptions, true, true)
	})

	// repo which contains mixed (data/tree) packs
	// => check and prune should fully work
	t.Run("mixed-packs", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-mixed.tar.gz", opts, pruneDefaultOptions, true, true)
	})

	// repo which contains duplicate blobs
	// => checking for unused data should report an error and prune resolves the
	// situation
	opts = CheckOptions{
		ReadData:    true,
		CheckUnused: true,
	}
	t.Run("duplicates", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-duplicates.tar.gz", opts, pruneDefaultOptions, false, true)
	})
}

func testEdgeCaseRepo(t *testing.T, tarfile string, optionsCheck CheckOptions, optionsPrune PruneOptions, checkOK, pruneOK bool) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	datafile := filepath.Join("testdata", tarfile)
	rtest.SetupTarTestFixture(t, env.base, datafile)

	if checkOK {
		testRunCheck(t, env.gopts)
	} else {
		rtest.Assert(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			_, err := runCheck(context.TODO(), optionsCheck, gopts, nil, gopts.Term)
			return err
		}) != nil,
			"check should have reported an error")
	}

	if pruneOK {
		testRunPrune(t, env.gopts, optionsPrune)
		testRunCheck(t, env.gopts)
	} else {
		rtest.Assert(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			return runPrune(context.TODO(), optionsPrune, gopts, gopts.Term)
		}) != nil,
			"prune should have reported an error")
	}
}

func TestPruneRepackSmallerThanSmoke(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	// the implementation is already unit tested, so just check that
	// the setting reaches its goal
	createPrunableRepo(t, env)
	testRunPrune(t, env.gopts, PruneOptions{
		SmallPackSize: "4M",
		MaxUnused:     "5%",
	})
	testRunPruneMustFail(t, env.gopts, PruneOptions{
		SmallPackSize: "500M",
		MaxUnused:     "5%",
	})
}

func TestPruneJSON(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)

	buf, err := withCaptureStdout(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.JSON = true
		oldHook := gopts.BackendTestHook
		gopts.BackendTestHook = func(r backend.Backend) (backend.Backend, error) { return newListOnceBackend(r), nil }
		defer func() {
			gopts.BackendTestHook = oldHook
		}()
		return runPrune(ctx, pruneDefaultOptions, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	var stats repository.PruneStats
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &stats))

	rtest.Equals(t, "summary", stats.MessageType)
	rtest.Assert(t, stats.Blobs.Total > 0, "expected non-zero total blobs, got %v", stats.Blobs.Total)
	rtest.Assert(t, stats.Packs.Total > 0, "expected non-zero total packs, got %v", stats.Packs.Total)
}

// TestPruneRepackAll verifies --repack-all repacks packs and the repository
// stays consistent afterwards.
func TestPruneRepackAll(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPrune(t, env.gopts, PruneOptions{MaxUnused: "5%", RepackAll: true})
	testRunCheck(t, env.gopts)
}

// TestPruneMaxRepackPercent verifies --max-repack accepts a percentage of the
// repository size and produces a consistent repository.
func TestPruneMaxRepackPercent(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPrune(t, env.gopts, PruneOptions{MaxUnused: "0%", MaxRepack: "50%"})
	testRunCheck(t, env.gopts)
}

// TestPruneMaxRepackInvalid verifies invalid --max-repack values are rejected.
func TestPruneMaxRepackInvalid(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPruneMustFail(t, env.gopts, PruneOptions{MaxUnused: "5%", MaxRepack: "150%"})
	testRunPruneMustFail(t, env.gopts, PruneOptions{MaxUnused: "5%", MaxRepack: "12x"})
}

// TestPruneEarlyDeleteIndex verifies --early-delete-index prunes consistently.
func TestPruneEarlyDeleteIndex(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPrune(t, env.gopts, PruneOptions{MaxUnused: "0%", EarlyDeleteIndex: true})
	testRunCheck(t, env.gopts)
}

// TestPruneTwoPhase verifies the two-phase prune flow: --keep-delete performs
// only the repack+index phase, then a default (instant-delete) prune removes
// the deferred files. The repository must stay consistent throughout.
func TestPruneTwoPhase(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.TwoPhasePrune, true)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)

	// phase 1: repack + write new index, defer deletion
	testRunPrune(t, env.gopts, PruneOptions{MaxUnused: "0%", KeepDelete: true})
	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		if repo.Config().PrunePlan == nil {
			return fmt.Errorf("deferred prune marker was not persisted")
		}
		return nil
	}))
	// After phase 1 the superseded index files are kept alongside the new one,
	// so 'check' reports non-critical "pack contained in several indexes". This
	// is the expected, safe intermediate state of two-phase prune (no data is
	// lost or unreachable). Verify the repo is still fully readable instead of
	// requiring a strict-clean check here.
	testListSnapshots(t, env.gopts, 2)

	// phase 2: default instant-delete removes the deferred packs/indexes
	testRunPrune(t, env.gopts, PruneOptions{MaxUnused: "0%"})
	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		if repo.Config().PrunePlan != nil {
			return fmt.Errorf("deferred prune marker was not cleared")
		}
		return nil
	}))
	// now the duplicate index entries are gone and the repo must be clean
	testRunCheck(t, env.gopts)
}

// TestPruneTwoPhaseRequiresFeature verifies --keep-delete is gated behind the
// two-phase-prune feature flag.
func TestPruneTwoPhaseRequiresFeature(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.TwoPhasePrune, false)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPruneMustFail(t, env.gopts, PruneOptions{MaxUnused: "0%", KeepDelete: true})
}

// TestPruneConcurrencySoak is the Phase 4 chaos test: run a lock-free backup
// concurrently with a prune and a forget against the same repository, then
// verify 'check' is clean. Run with -race for full effect.
func TestPruneConcurrencySoak(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, true)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	// withTestEnvironment uses a zero lock double-check delay to speed normal
	// tests. This deliberate concurrent-exclusive scenario must exercise the
	// production visibility safeguard instead.
	repository.TestSetLockTimeout(t, 50*time.Millisecond)

	// seed a few snapshots so forget/prune have something to work with
	for i := 0; i < 3; i++ {
		testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	}
	env.gopts.BackendTestHook = nil

	var wg sync.WaitGroup
	backupResult := make(chan error, 1)

	// concurrent lock-free backup (append-only; must never corrupt the repo)
	wg.Add(1)
	go func() {
		defer wg.Done()
		backupResult <- withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			return runBackup(ctx, BackupOptions{Host: "lock-free-soak"}, gopts, gopts.Term, []string{filepath.Join(env.testdata, "0", "0", "9", "2")})
		})
	}()

	// concurrent prune (exclusive lock; may conflict with forget — tolerated)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			return runPrune(ctx, PruneOptions{MaxUnused: "50%"}, gopts, gopts.Term)
		})
	}()

	// Concurrent forget policy evaluation. Keep it dry-run: prune is the sole
	// destructive operation in this lock-contract soak, while forget still
	// exercises its exclusive acquisition and snapshot selection path.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = testRunForgetMayFail(t, env.gopts, ForgetOptions{Last: 1, DryRun: true})
	}()

	wg.Wait()
	backupErr := <-backupResult
	if backupErr != nil && !repository.IsAlreadyLocked(backupErr) {
		t.Fatalf("append backup failed unexpectedly: %v", backupErr)
	}

	// A concurrent prune/forget may legitimately conflict with the backup or
	// each other; the repository must remain consistent regardless. We only
	// require that a final check passes. The backup's separate host group must
	// also have a durable snapshot; this catches accidental dry-run behavior
	// without assuming how many seeded snapshots concurrent forget retains.
	var backupSnapshots int
	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		filter := data.SnapshotFilter{Hosts: []string{"lock-free-soak"}}
		return filter.FindAll(ctx, repo, repo, nil, func(_ string, _ *data.Snapshot, err error) error {
			if err == nil {
				backupSnapshots++
			}
			return err
		})
	}))
	if backupErr == nil {
		rtest.Equals(t, 1, backupSnapshots)
	} else {
		rtest.Equals(t, 0, backupSnapshots)
	}
	testRunCheck(t, env.gopts)
}

func TestLockFreeFeatureBackupRemainsWritable(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, true)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.gopts.BackendTestHook = nil

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 2)
}

func TestLockFreeAppendRetainsSharedLock(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, true)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.gopts.BackendTestHook = nil
	before, err := os.ReadDir(filepath.Join(env.repo, "locks"))
	rtest.OK(t, err)

	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, gopts.Term)
		_, _, unlock, err := openWithAppendLock(ctx, gopts, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		during, err := os.ReadDir(filepath.Join(env.repo, "locks"))
		if err != nil {
			return err
		}
		if len(during) != len(before)+1 {
			return fmt.Errorf("lock-free append lock count = %d, want %d", len(during), len(before)+1)
		}
		return nil
	}))
}

func TestLockFreeReadSkipsLockFile(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, true)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.gopts.BackendTestHook = nil
	before, err := os.ReadDir(filepath.Join(env.repo, "locks"))
	rtest.OK(t, err)

	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, gopts.Term)
		_, _, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		entries, err := os.ReadDir(filepath.Join(env.repo, "locks"))
		if err != nil {
			return err
		}
		if len(entries) != len(before) {
			return fmt.Errorf("lock-free read changed lock file count from %d to %d", len(before), len(entries))
		}
		return nil
	}))
}

func TestNoLockBackupWritesSnapshot(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, false)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.gopts.BackendTestHook = nil
	env.gopts.NoLock = true

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 1)
}
