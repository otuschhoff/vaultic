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
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
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
	testRunBackup(t, "", []string{doomedTarget}, backupOptions{}, env.globalOptions)
	doomedSnapshot := testListSnapshots(t, env.globalOptions, 1)[0]

	retainedTarget := filepath.Join(env.testdata, "0", "0", "9", "2")
	testRunBackup(t, "", []string{retainedTarget}, backupOptions{}, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 2)

	testRunForget(t, env.globalOptions, forgetOptions{}, doomedSnapshot.String())
	retained := testListSnapshots(t, env.globalOptions, 1)[0]

	env.globalOptions.BackendTestHook = nil

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
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexImport(ctx, indexImportOptions{
			Daemon: daemonOptions, FromLegacy: true, Resume: true, Activate: true, SnapshotDepth: ^uint(0),
		}, globalOptions, globalOptions.Term)
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
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Full: true}, globalOptions, globalOptions.Term)
		if result.PacksSelected == 0 {
			t.Fatalf("export did not publish any packs: %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	packsBefore := listPacks(env.globalOptions, t)

	// --discover-only must not delete or repack anything.
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		stats, runErr := runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions, DiscoverOnly: true}, globalOptions, globalOptions.Term)
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
	if packsAfterDiscover := listPacks(env.globalOptions, t); len(packsAfterDiscover) != len(packsBefore) {
		t.Fatalf("discover-only changed pack count: before=%d after=%d", len(packsBefore), len(packsAfterDiscover))
	}

	// A high age requirement must postpone sweeping a freshly discovered candidate.
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		stats, runErr := runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions, MinCandidateAge: 24 * time.Hour}, globalOptions, globalOptions.Term)
		if stats.PendingAge == 0 {
			t.Fatalf("min-candidate-age did not postpone any candidate: %#v", stats)
		}
		if stats.PacksDeleted != 0 || stats.PacksRepacked != 0 {
			t.Fatalf("aged gate did not prevent sweeping: %#v", stats)
		}
		// This is the first run that actually computes reachability, so it is
		// where usage accounting is established. Postponing the sweep must not
		// postpone the accounting.
		if stats.PacksAccounted == 0 {
			t.Fatalf("reachability was computed but no usage was recorded: %#v", stats)
		}
		if stats.PacksUnaccountable != 0 {
			t.Fatalf("packs left unaccountable: %#v", stats)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	// The real sweep must free at least one pack.
	var stats repository.GCStats
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		stats, runErr = runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.PacksDeleted == 0 && stats.PacksRepacked == 0 {
		t.Fatalf("gc freed nothing: %#v", stats)
	}
	// Reachability is unchanged since the aged-gate run recorded it, so this
	// run must find nothing to re-account and still leave nothing unaccountable.
	if stats.PacksAccounted != 0 {
		t.Fatalf("usage accounting was rewritten without a reachability change: %#v", stats)
	}
	if stats.PacksUnaccountable != 0 {
		t.Fatalf("gc left %d packs unaccountable: %#v", stats.PacksUnaccountable, stats)
	}

	packsAfter := listPacks(env.globalOptions, t)
	if len(packsAfter) >= len(packsBefore) {
		t.Fatalf("gc did not reduce pack count: before=%d after=%d", len(packsBefore), len(packsAfter))
	}

	// A repeated run should find nothing new to reclaim.
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		converged, runErr := runIndexGC(ctx, indexGCOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
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
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 10}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil {
		t.Fatalf("check after gc: %v", err)
	}

	// The retained snapshot's data must still restore byte-for-byte, and
	// restic-style CheckUnused must find nothing left dangling.
	restoreDir := filepath.Join(env.base, "restore-after-gc")
	testRunRestore(t, env.globalOptions, restoreDir, retained.String())
	testRunCheck(t, env.globalOptions)

	assertPackHistoryAfterGC(t, env, daemonOptions, packsBefore)
}

// assertPackHistoryAfterGC is the Phase 10 exit criterion: a repository that
// has been backed up, repacked, and pruned must be able to report its full
// pack history, including for packs that no longer exist, with correct
// coverage flags.
func assertPackHistoryAfterGC(t *testing.T, env *testEnvironment, daemonOptions indexDaemonOptions, packsBefore vaultic.IDSet) {
	t.Helper()
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		config, err := daemonOptions.Config("")
		if err != nil {
			return err
		}
		ctx = repository.WithDaemonOptions(ctx, config)
		ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		store, _, closeStore, err := openIndexStore(ctx, repo, daemonOptions)
		if err != nil {
			return err
		}
		defer closeStore()

		scanned, err := maintenance.ScanHistory(ctx, store, 0, 0)
		if err != nil {
			return err
		}
		if len(scanned.Events) == 0 {
			t.Fatal("no pack history was recorded for a real backup and gc")
		}
		if scanned.Malformed != 0 {
			t.Fatalf("malformed history events = %d", scanned.Malformed)
		}

		seen := make(map[schema.PackEventType]int)
		historyPacks := make(map[vaultic.ID]struct{})
		lineage := 0
		for _, event := range scanned.Events {
			seen[event.Record.Type]++
			historyPacks[event.PackID] = struct{}{}
			if len(event.Record.PredecessorPackIDs) != 0 {
				lineage++
			}
		}
		// Packs in this repository originate from a legacy import rather than
		// from a fresh authoritative backup, so origin is recorded as
		// imported; either origin event satisfies the requirement.
		if seen[schema.EventCreated]+seen[schema.EventImported] == 0 {
			t.Fatalf("no pack origin events recorded: %v", seen)
		}
		for _, required := range []schema.PackEventType{
			schema.EventPublished, schema.EventDeletePending, schema.EventDeleted, schema.EventUsageChanged,
		} {
			if seen[required] == 0 {
				t.Fatalf("no %v events recorded: %v", required, seen)
			}
		}
		// A repack happened, so a destination pack must carry its lineage and
		// the superseded source must be recorded as repacked from.
		if lineage == 0 || seen[schema.EventRepackedFrom] == 0 || seen[schema.EventRepackedInto] == 0 {
			t.Fatalf("repack lineage missing: lineage=%d events=%v", lineage, seen)
		}

		// History must remain readable for packs that are gone from both the
		// backend and the catalog.
		packsNow := listPacks(env.globalOptions, t)
		var describedAndGone int
		for id := range packsBefore {
			if packsNow.Has(id) {
				continue
			}
			if _, described := historyPacks[id]; described {
				describedAndGone++
			}
		}
		if describedAndGone == 0 {
			t.Fatal("history describes no pack that has since been deleted")
		}

		// Rolling up must produce buckets, and a repeated rollup must be a
		// no-op over the same raw range.
		first, err := maintenance.RollupHistory(ctx, store, false)
		if err != nil {
			return err
		}
		if first.BucketsWritten == 0 {
			t.Fatalf("rollup produced no buckets: %#v", first)
		}
		second, err := maintenance.RollupHistory(ctx, store, false)
		if err != nil {
			return err
		}
		if second.BucketsWritten != 0 {
			t.Fatalf("rollup was not idempotent on a real repository: %#v", second)
		}
		// This repository imported a legacy history, so its buckets describe
		// inferred activity and must not claim to be complete.
		if second.Reconstructed == 0 {
			t.Fatalf("imported activity was reported as observed: %#v", second)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("pack history assertions: %v", err)
	}
}
