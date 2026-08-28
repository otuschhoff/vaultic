package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/repository"
)

// TestIndexGCDiscoversRevalidatesAndSweepsRealBackup exercises Phase 8 end to
// end against a real vaulticdb daemon: a snapshot uniquely covering a subtree
// is forgotten, the full history is imported and activated, and gc must
// discover the now-unreachable packs, re-walk the retained snapshot to
// confirm reachability, and delete them, while leaving the retained
// snapshot's data intact and restorable.
func TestIndexGCDiscoversRevalidatesAndSweepsRealBackup(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)

	doomedTarget := filepath.Join(env.testdata, "0", "0", "9")
	testRunBackup(t, "", []string{doomedTarget}, BackupOptions{}, env.gopts)
	doomedSnapshot := testListSnapshots(t, env.gopts, 1)[0]

	retainedTarget := filepath.Join(env.testdata, "0", "0", "9", "2")
	testRunBackup(t, "", []string{retainedTarget}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 2)

	testRunForget(t, env.gopts, ForgetOptions{}, doomedSnapshot.String())
	retained := testListSnapshots(t, env.gopts, 1)[0]

	env.gopts.BackendTestHook = nil

	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	// Using the repository-scoped default socket lets helpers that open the
	// repository without daemon options (listPacks, testRunCheck,
	// testRunRestore) find the same persistent daemon automatically.
	socket := daemon.DefaultSocket(repositoryID(t, env))
	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: socket, RepositoryID: repositoryID(t, env), DaemonPath: daemonPath,
		DataDir: filepath.Join(env.base, "vaulticdb"), ObjectStore: "local", PersistentDaemon: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	daemonOptions := indexDaemonOptions{Socket: socket}

	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexImport(ctx, indexImportOptions{
			Daemon: daemonOptions, FromLegacy: true, Resume: true, Activate: true, SnapshotDepth: ^uint(0),
		}, gopts, gopts.Term)
		if result.SnapshotsImported == 0 {
			t.Fatalf("full import did not import any snapshots: %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	// GC only ever considers published packs; export transitions freshly
	// imported/backed-up packs out of export-pending.
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Full: true}, gopts, gopts.Term)
		if result.PacksSelected == 0 {
			t.Fatalf("export did not publish any packs: %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	packsBefore := listPacks(env.gopts, t)

	// --discover-only must not delete or repack anything.
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		stats, runErr := runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions, DiscoverOnly: true}, gopts, gopts.Term)
		if stats.BlobCandidates == 0 {
			t.Fatalf("discover-only found no candidates: %#v", stats)
		}
		if stats.PacksDeleted != 0 || stats.PacksRepacked != 0 {
			t.Fatalf("discover-only mutated packs: %#v", stats)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if packsAfterDiscover := listPacks(env.gopts, t); len(packsAfterDiscover) != len(packsBefore) {
		t.Fatalf("discover-only changed pack count: before=%d after=%d", len(packsBefore), len(packsAfterDiscover))
	}

	// A high age requirement must postpone sweeping a freshly discovered candidate.
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		stats, runErr := runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions, MinCandidateAge: 24 * time.Hour}, gopts, gopts.Term)
		if stats.PendingAge == 0 {
			t.Fatalf("min-candidate-age did not postpone any candidate: %#v", stats)
		}
		if stats.PacksDeleted != 0 || stats.PacksRepacked != 0 {
			t.Fatalf("aged gate did not prevent sweeping: %#v", stats)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	// The real sweep must free at least one pack.
	var stats repository.GCStats
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		var runErr error
		stats, runErr = runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions}, gopts, gopts.Term)
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.PacksDeleted == 0 && stats.PacksRepacked == 0 {
		t.Fatalf("gc freed nothing: %#v", stats)
	}

	packsAfter := listPacks(env.gopts, t)
	if len(packsAfter) >= len(packsBefore) {
		t.Fatalf("gc did not reduce pack count: before=%d after=%d", len(packsBefore), len(packsAfter))
	}

	// A repeated run should find nothing new to reclaim.
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		converged, runErr := runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions}, gopts, gopts.Term)
		if converged.PacksDeleted != 0 || converged.PacksRepacked != 0 {
			t.Fatalf("converged gc unexpectedly freed more: %#v", converged)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	// index check must report a fully consistent, non-drifted catalog: gc
	// automatically re-exports and prunes stale legacy indexes internally.
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 10}, gopts, gopts.Term)
		return runErr
	})
	if err != nil {
		t.Fatalf("check after gc: %v", err)
	}

	// The retained snapshot's data must still restore byte-for-byte, and
	// restic-style CheckUnused must find nothing left dangling.
	restoreDir := filepath.Join(env.base, "restore-after-gc")
	testRunRestore(t, env.gopts, restoreDir, retained.String())
	testRunCheck(t, env.gopts)
}
