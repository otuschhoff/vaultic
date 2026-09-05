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

func testRunPrune(t testing.TB, globalOptions global.Options, options pruneOptions) {
	t.Helper()
	rtest.OK(t, testRunPruneOutput(t, globalOptions, options))
}

func testRunPruneMustFail(t testing.TB, globalOptions global.Options, options pruneOptions) {
	t.Helper()
	err := testRunPruneOutput(t, globalOptions, options)
	rtest.Assert(t, err != nil, "expected non nil error")
}

func testRunPruneOutput(t testing.TB, globalOptions global.Options, options pruneOptions) error {
	oldHook := globalOptions.BackendTestHook
	globalOptions.BackendTestHook = func(r backend.Backend) (backend.Backend, error) { return newListOnceBackend(r), nil }
	defer func() {
		globalOptions.BackendTestHook = oldHook
	}()
	return withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runPrune(context.TODO(), options, globalOptions, globalOptions.Term)
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
		options := pruneOptions{MaxUnused: "0%", unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := checkOptions{ReadData: true, CheckUnused: !unsafeNoSpaceRecovery}
		testPrune(t, options, checkOpts)
	})

	t.Run("50"+suffix, func(t *testing.T) {
		options := pruneOptions{MaxUnused: "50%", unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := checkOptions{ReadData: true}
		testPrune(t, options, checkOpts)
	})

	t.Run("unlimited"+suffix, func(t *testing.T) {
		options := pruneOptions{MaxUnused: "unlimited", unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := checkOptions{ReadData: true}
		testPrune(t, options, checkOpts)
	})

	t.Run("CacheableOnly"+suffix, func(t *testing.T) {
		options := pruneOptions{MaxUnused: "5%", RepackCacheableOnly: true, unsafeRecovery: unsafeNoSpaceRecovery}
		checkOpts := checkOptions{ReadData: true}
		testPrune(t, options, checkOpts)
	})
}

func createPrunableRepo(t *testing.T, env *testEnvironment) {
	testSetupBackupData(t, env)
	options := backupOptions{}

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, options, env.globalOptions)
	firstSnapshot := testListSnapshots(t, env.globalOptions, 1)[0]

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, options, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, options, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 3)

	testRunForgetJSON(t, env.globalOptions)
	testRunForget(t, env.globalOptions, forgetOptions{}, firstSnapshot.String())
}

func testRunForgetJSON(t testing.TB, globalOptions global.Options, args ...string) {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = true
		options := forgetOptions{
			DryRun: true,
			Last:   1,
		}
		pruneOpts := pruneOptions{
			MaxUnused: "5%",
		}
		return runForget(context.TODO(), options, pruneOpts, globalOptions, globalOptions.Term, args)
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

func testPrune(t *testing.T, pruneOpts pruneOptions, checkOpts checkOptions) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPrune(t, env.globalOptions, pruneOpts)
	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, err := runCheck(context.TODO(), checkOpts, globalOptions, nil, globalOptions.Term)
		return err
	}))
}

var pruneDefaultOptions = pruneOptions{MaxUnused: "5%"}

func TestPruneWithDamagedRepository(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	datafile := filepath.Join("testdata", "backup-data.tar.gz")
	testRunInit(t, env.globalOptions)

	rtest.SetupTarTestFixture(t, env.testdata, datafile)
	options := backupOptions{}

	// create and delete snapshot to create unused blobs
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, options, env.globalOptions)
	firstSnapshot := testListSnapshots(t, env.globalOptions, 1)[0]
	testRunForget(t, env.globalOptions, forgetOptions{}, firstSnapshot.String())

	oldPacks := listPacks(env.globalOptions, t)

	// create new snapshot, but lose all data
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, options, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 1)
	removePacksExcept(env.globalOptions, t, oldPacks, false)

	oldHook := env.globalOptions.BackendTestHook
	env.globalOptions.BackendTestHook = func(r backend.Backend) (backend.Backend, error) { return newListOnceBackend(r), nil }
	defer func() {
		env.globalOptions.BackendTestHook = oldHook
	}()
	// prune should fail
	rtest.Equals(t, repository.ErrPacksMissing, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runPrune(context.TODO(), pruneDefaultOptions, globalOptions, globalOptions.Term)
	}), "prune should have reported index not complete error")
}

// Test repos for edge cases
func TestEdgeCaseRepos(t *testing.T) {
	options := checkOptions{}

	// repo where index is completely missing
	// => check and prune should fail
	t.Run("no-index", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-index-missing.tar.gz", options, pruneDefaultOptions, false, false)
	})

	// repo where an existing and used blob is missing from the index
	// => check and prune should fail
	t.Run("index-missing-blob", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-index-missing-blob.tar.gz", options, pruneDefaultOptions, false, false)
	})

	// repo where a blob is missing
	// => check and prune should fail
	t.Run("missing-data", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-data-missing.tar.gz", options, pruneDefaultOptions, false, false)
	})

	// repo where blobs which are not needed are missing or in invalid pack files
	// => check should fail and prune should repair this
	t.Run("missing-unused-data", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-unused-data-missing.tar.gz", options, pruneDefaultOptions, false, true)
	})

	// repo where data exists that is not referenced
	// => check and prune should fully work
	t.Run("unreferenced-data", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-unreferenced-data.tar.gz", options, pruneDefaultOptions, true, true)
	})

	// repo where an obsolete index still exists
	// => check and prune should fully work
	t.Run("obsolete-index", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-obsolete-index.tar.gz", options, pruneDefaultOptions, true, true)
	})

	// repo which contains mixed (data/tree) packs
	// => check and prune should fully work
	t.Run("mixed-packs", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-mixed.tar.gz", options, pruneDefaultOptions, true, true)
	})

	// repo which contains duplicate blobs
	// => checking for unused data should report an error and prune resolves the
	// situation
	options = checkOptions{
		ReadData:    true,
		CheckUnused: true,
	}
	t.Run("duplicates", func(t *testing.T) {
		testEdgeCaseRepo(t, "repo-duplicates.tar.gz", options, pruneDefaultOptions, false, true)
	})
}

func testEdgeCaseRepo(t *testing.T, tarfile string, optionsCheck checkOptions, optionsPrune pruneOptions, checkOK, pruneOK bool) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	datafile := filepath.Join("testdata", tarfile)
	rtest.SetupTarTestFixture(t, env.base, datafile)

	if checkOK {
		testRunCheck(t, env.globalOptions)
	} else {
		rtest.Assert(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			_, err := runCheck(context.TODO(), optionsCheck, globalOptions, nil, globalOptions.Term)
			return err
		}) != nil,
			"check should have reported an error")
	}

	if pruneOK {
		testRunPrune(t, env.globalOptions, optionsPrune)
		testRunCheck(t, env.globalOptions)
	} else {
		rtest.Assert(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runPrune(context.TODO(), optionsPrune, globalOptions, globalOptions.Term)
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
	testRunPrune(t, env.globalOptions, pruneOptions{
		SmallPackSize: "4M",
		MaxUnused:     "5%",
	})
	testRunPruneMustFail(t, env.globalOptions, pruneOptions{
		SmallPackSize: "500M",
		MaxUnused:     "5%",
	})
}

func TestPruneJSON(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)

	buf, err := withCaptureStdout(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = true
		oldHook := globalOptions.BackendTestHook
		globalOptions.BackendTestHook = func(r backend.Backend) (backend.Backend, error) { return newListOnceBackend(r), nil }
		defer func() {
			globalOptions.BackendTestHook = oldHook
		}()
		return runPrune(ctx, pruneDefaultOptions, globalOptions, globalOptions.Term)
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
	testRunPrune(t, env.globalOptions, pruneOptions{MaxUnused: "5%", RepackAll: true})
	testRunCheck(t, env.globalOptions)
}

// TestPruneMaxRepackPercent verifies --max-repack accepts a percentage of the
// repository size and produces a consistent repository.
func TestPruneMaxRepackPercent(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPrune(t, env.globalOptions, pruneOptions{MaxUnused: "0%", MaxRepack: "50%"})
	testRunCheck(t, env.globalOptions)
}

// TestPruneMaxRepackInvalid verifies invalid --max-repack values are rejected.
func TestPruneMaxRepackInvalid(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPruneMustFail(t, env.globalOptions, pruneOptions{MaxUnused: "5%", MaxRepack: "150%"})
	testRunPruneMustFail(t, env.globalOptions, pruneOptions{MaxUnused: "5%", MaxRepack: "12x"})
}

// TestPruneEarlyDeleteIndex verifies --early-delete-index prunes consistently.
func TestPruneEarlyDeleteIndex(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPrune(t, env.globalOptions, pruneOptions{MaxUnused: "0%", EarlyDeleteIndex: true})
	testRunCheck(t, env.globalOptions)
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
	testRunPrune(t, env.globalOptions, pruneOptions{MaxUnused: "0%", KeepDelete: true})
	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
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
	testListSnapshots(t, env.globalOptions, 2)

	// phase 2: default instant-delete removes the deferred packs/indexes
	testRunPrune(t, env.globalOptions, pruneOptions{MaxUnused: "0%"})
	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
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
	testRunCheck(t, env.globalOptions)
}

// TestPruneTwoPhaseRequiresFeature verifies --keep-delete is gated behind the
// two-phase-prune feature flag.
func TestPruneTwoPhaseRequiresFeature(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.TwoPhasePrune, false)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	createPrunableRepo(t, env)
	testRunPruneMustFail(t, env.globalOptions, pruneOptions{MaxUnused: "0%", KeepDelete: true})
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
		testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, backupOptions{}, env.globalOptions)
	}
	env.globalOptions.BackendTestHook = nil

	var wg sync.WaitGroup
	backupResult := make(chan error, 1)

	// concurrent lock-free backup (append-only; must never corrupt the repo)
	wg.Add(1)
	go func() {
		defer wg.Done()
		backupResult <- withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runBackup(
				ctx, backupOptions{Host: "lock-free-soak"}, globalOptions, globalOptions.Term,
				[]string{filepath.Join(env.testdata, "0", "0", "9", "2")},
			)
		})
	}()

	// concurrent prune (exclusive lock; may conflict with forget — tolerated)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runPrune(ctx, pruneOptions{MaxUnused: "50%"}, globalOptions, globalOptions.Term)
		})
	}()

	// Concurrent forget policy evaluation. Keep it dry-run: prune is the sole
	// destructive operation in this lock-contract soak, while forget still
	// exercises its exclusive acquisition and snapshot selection path.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = testRunForgetMayFail(t, env.globalOptions, forgetOptions{Last: 1, DryRun: true})
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
	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
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
	testRunCheck(t, env.globalOptions)
}

func TestLockFreeFeatureBackupRemainsWritable(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, true)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.globalOptions.BackendTestHook = nil

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, backupOptions{}, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, backupOptions{}, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 2)
}

func TestLockFreeAppendRetainsSharedLock(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, true)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.globalOptions.BackendTestHook = nil
	before, err := os.ReadDir(filepath.Join(env.repo, "locks"))
	rtest.OK(t, err)

	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, globalOptions.Term)
		_, _, unlock, err := openWithAppendLock(ctx, globalOptions, false, printer)
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
	env.globalOptions.BackendTestHook = nil
	before, err := os.ReadDir(filepath.Join(env.repo, "locks"))
	rtest.OK(t, err)

	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, globalOptions.Term)
		_, _, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
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

type blockFirstIndexSaveBackend struct {
	backend.Backend
	once    *sync.Once
	entered chan<- struct{}
	release <-chan struct{}
}

func (b *blockFirstIndexSaveBackend) Save(ctx context.Context, h backend.Handle, reader backend.RewindReader) error {
	if h.Type == backend.IndexFile {
		blocked := false
		b.once.Do(func() {
			blocked = true
			close(b.entered)
		})
		if blocked {
			select {
			case <-b.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return b.Backend.Save(ctx, h, reader)
}

// TestPrunePhaseAAllowsBackup proves the minimal-lock boundary: after the
// short exclusive claim is released, phase A holds only a shared lock while it
// uploads replacement indexes, so an append backup can complete before phase B
// takes its short exclusive deletion lock.
func TestPrunePhaseAAllowsBackup(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	createPrunableRepo(t, env)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	env.globalOptions.BackendTestHook = func(r backend.Backend) (backend.Backend, error) {
		return &blockFirstIndexSaveBackend{Backend: r, once: &once, entered: entered, release: release}, nil
	}

	pruneResult := make(chan error, 1)
	go func() {
		pruneResult <- withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runPrune(ctx, pruneOptions{MaxUnused: "0%"}, globalOptions, globalOptions.Term)
		})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("prune phase A did not begin index upload")
	}

	backupErr := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runBackup(
			ctx, backupOptions{Host: "phase-a-backup"}, globalOptions, globalOptions.Term,
			[]string{filepath.Join(env.testdata, "0", "0", "9", "2")},
		)
	})
	rtest.OK(t, backupErr)
	close(release)
	rtest.OK(t, <-pruneResult)
	testRunCheck(t, env.globalOptions)
}

func TestNoLockBackupWritesSnapshot(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, false)()

	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.globalOptions.BackendTestHook = nil
	env.globalOptions.NoLock = true

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, backupOptions{}, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 1)
}
