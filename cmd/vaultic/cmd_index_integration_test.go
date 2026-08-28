package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
)

func TestIndexWorkflowsImportResumeExportCheckAndRepair(t *testing.T) {
	testIndexWorkflows(t, false)
}

func TestIndexWorkflowsS3CompatibleMetadata(t *testing.T) {
	if os.Getenv("VAULTICDB_TEST_S3_ENDPOINT") == "" {
		t.Skip("VAULTICDB_TEST_S3_ENDPOINT is not configured")
	}
	testIndexWorkflows(t, true)
}

func testIndexWorkflows(t *testing.T, s3Metadata bool) {
	t.Helper()
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, BackupOptions{}, env.gopts)
	env.gopts.BackendTestHook = nil

	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socket := filepath.Join(env.base, "vaulticdb.sock")
	daemonConfig := daemon.Options{
		Socket: socket, RepositoryID: repositoryID(t, env), DaemonPath: daemonPath,
		DataDir: filepath.Join(env.base, "vaulticdb"), ObjectStore: "local", PersistentDaemon: true,
	}
	if s3Metadata {
		daemonConfig.DataDir = ""
		daemonConfig.ObjectStore = "s3"
		daemonConfig.S3Bucket = os.Getenv("VAULTICDB_TEST_S3_BUCKET")
		if daemonConfig.S3Bucket == "" {
			daemonConfig.S3Bucket = "vaulticdb-phase7"
		}
		daemonConfig.S3Prefix = "phase7/" + filepath.Base(env.base)
	}
	client, err := daemon.Ensure(context.Background(), daemonConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := daemon.NewSchemaStore(client)
	daemonOptions := indexDaemonOptions{Socket: socket}

	var dryResultIndexes uint64
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexImport(ctx, indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, DryRun: true, SnapshotDepth: ^uint(0)}, gopts, gopts.Term)
		dryResultIndexes = result.IndexesImported
		return runErr
	})
	if err != nil || dryResultIndexes == 0 {
		t.Fatalf("dry-run import indexes=%d err=%v", dryResultIndexes, err)
	}
	if entries, _, err := store.ScanPrefix(context.Background(), []byte("p:"), nil, 10); err != nil || len(entries) != 0 {
		t.Fatalf("dry-run wrote pack records: entries=%d err=%v", len(entries), err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, DryRun: true}, gopts, gopts.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("legacy-only dry-run export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 1}, gopts, gopts.Term)
		return runErr
	})
	if !errors.Is(err, errIndexDifferences) {
		t.Fatalf("legacy-only check error = %v", err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions, DryRun: true}, gopts, gopts.Term)
		if result.AggregatesChanged != 5 {
			t.Fatalf("legacy-only aggregate dry-run = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		_, runErr := runIndexImport(ctx, indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, SnapshotDepth: ^uint(0), SnapshotWorkBudget: 1}, gopts, gopts.Term)
		return runErr
	})
	if !errors.Is(err, errIndexIncomplete) {
		t.Fatalf("partial import error = %v", err)
	}

	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	var resumed uint64
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexImport(ctx, indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, Activate: true, SnapshotDepth: ^uint(0)}, gopts, gopts.Term)
		resumed = result.IndexesResumed
		return runErr
	})
	if err != nil || resumed == 0 {
		t.Fatalf("resumed activation indexes=%d err=%v", resumed, err)
	}
	var exported, selected, exportSequence uint64
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, PacksPerIndex: 2, Verify: true}, gopts, gopts.Term)
		exported, selected, exportSequence = result.IndexesWritten, result.PacksSelected, result.ExportSequence
		return runErr
	})
	if err != nil || exported == 0 || selected == 0 {
		t.Fatalf("checkpointed export indexes=%d packs=%d err=%v", exported, selected, err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions}, gopts, gopts.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("resumed export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Since: exportSequence}, gopts, gopts.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("since export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	var fullExportSequence uint64
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Full: true, Verify: true, PacksPerIndex: 1}, gopts, gopts.Term)
		if result.PacksSelected == 0 || result.IndexesWritten == 0 {
			t.Fatalf("full export = %#v", result)
		}
		fullExportSequence = result.ExportSequence
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Since: exportSequence, DryRun: true}, gopts, gopts.Term)
		if result.PacksSelected == 0 || result.IndexesWritten != 0 || fullExportSequence <= exportSequence {
			t.Fatalf("positive since export = %#v, full sequence=%d", result, fullExportSequence)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	check := func() error {
		return withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 10}, gopts, gopts.Term)
			return runErr
		})
	}
	if err := check(); err != nil {
		t.Fatalf("clean check: %v", err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, IncludeCrawlDebt: true, FailOnWarning: true, MaxFindings: 100}, gopts, gopts.Term)
		if result.PendingCrawlDebt == 0 || len(result.Findings) == 0 {
			t.Fatalf("warning check = %#v", result)
		}
		return runErr
	})
	if !errors.Is(err, errIndexDifferences) {
		t.Fatalf("warning check error = %v", err)
	}
	corrupt, err := (schema.PackAggregate{PackCount: 99, UpdateSequence: 99}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), schema.PackAggregateKey(schema.AggregateAll), corrupt, true); err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, errIndexDifferences) {
		t.Fatalf("corrupt check error = %v", err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		result, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions, DryRun: true}, gopts, gopts.Term)
		if result.AggregatesChanged == 0 {
			t.Fatal("dry-run did not report aggregate drift")
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, errIndexDifferences) {
		t.Fatalf("dry-run unexpectedly repaired aggregate: %v", err)
	}
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		_, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions}, gopts, gopts.Term)
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatalf("check after repair: %v", err)
	}
}

func repositoryID(t *testing.T, env *testEnvironment) string {
	t.Helper()
	var id string
	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(false, gopts.Verbosity, gopts.Term)
		ctx, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		id = repo.Config().ID
		return nil
	})
	test.OK(t, err)
	return id
}
