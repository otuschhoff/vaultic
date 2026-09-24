package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
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

func TestIndexFreshBulkImportHandoffAndActivation(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil

	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socket := filepath.Join(env.base, "fresh-vaulticdb.sock")
	dataDir := filepath.Join(env.base, "fresh-vaulticdb")
	repoID := repositoryID(t, env)
	daemonOptions := indexDaemonOptions{Socket: socket, DaemonPath: daemonPath, DataDir: dataDir, Start: true}
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()

	var imported uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexImport(ctx, indexImportOptions{
			Daemon: daemonOptions, FromLegacy: true, ForceResetOldIndex: true,
			Activate: true, SnapshotDepth: 0,
		}, globalOptions, globalOptions.Term)
		imported = result.PacksImported
		return runErr
	})
	if err != nil || imported == 0 {
		t.Fatalf("fresh bulk import packs=%d err=%v", imported, err)
	}
	var stats maintenance.StatsResult
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		stats, runErr = runIndexStats(ctx, indexStatsOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil || stats.Totals.PackCount != imported {
		t.Fatalf("authoritative stats packs=%d want=%d err=%v", stats.Totals.PackCount, imported, err)
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("non-persistent stats daemon socket remains: %v", err)
	}

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: socket, RepositoryID: repoID, DaemonPath: daemonPath,
		DataDir: dataDir, WALDataDir: filepath.Join(dataDir, "wal"), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := daemon.NewSchemaStore(client)
	marker, found, err := client.Get(context.Background(), []byte("_vaultic/bulk-import-complete-v1"), "")
	if err != nil || !found || string(marker) != "complete" {
		t.Fatalf("bulk import completion marker: found=%t value=%q err=%v", found, marker, err)
	}
	packs, _, err := store.ScanPrefix(context.Background(), []byte("p:"), nil, 100)
	if err != nil || uint64(len(packs)) != imported {
		t.Fatalf("persistent-WAL catalog packs=%d want=%d err=%v", len(packs), imported, err)
	}
}

func phase34M2IndexFixture(t *testing.T) (*testEnvironment, string, string, *daemon.Client) {
	t.Helper()
	env, cleanup := withTestEnvironment(t)
	t.Cleanup(cleanup)
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil
	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socket := filepath.Join(env.base, "phase34-m2-vaulticdb.sock")
	dataDir := filepath.Join(env.base, "phase34-m2-vaulticdb")
	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: socket, RepositoryID: repositoryID(t, env), DaemonPath: daemonPath,
		DataDir: dataDir, WALDataDir: filepath.Join(dataDir, "wal"), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return env, socket, dataDir, client
}

func phase34M2IndexController(t *testing.T, profile telemetry.ExperimentProfile, targetID string) *telemetry.ExperimentController {
	t.Helper()
	profile.TargetID = targetID
	controller, err := telemetry.NewScenarioHarness().Controller(profile, telemetry.ExperimentTarget{
		ID: targetID, Disposable: true, Confirmed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func TestPhase34M2ImportCommitResponseSweep(t *testing.T) {
	artifacts := t.TempDir()
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, _ int) {
		env, socket, dataDir, client := phase34M2IndexFixture(t)
		repositoryIdentity := repositoryID(t, env)
		if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
		if err != nil {
			t.Fatal(err)
		}
		profile := phase34M2Profile("legacy_import", "rpc", "commit", delay)
		profile.TargetID = repositoryIdentity
		controller := phase34M2IndexController(t, profile, repositoryIdentity)
		defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
		started := time.Now()
		var packsImported, indexesImported uint64
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			result, runErr := runIndexImport(ctx, indexImportOptions{
				Daemon: indexDaemonOptions{
					Socket: socket, DaemonPath: daemonPath, DataDir: dataDir, Start: true,
					ResponseDeliveryForTesting: controller.ResponseDelivery("commit"),
				},
				FromLegacy: true, ForceResetOldIndex: true, Activate: true, SnapshotDepth: ^uint(0),
			}, globalOptions, globalOptions.Term)
			packsImported, indexesImported = result.PacksImported, result.IndexesImported
			return runErr
		})
		if err != nil || packsImported == 0 || indexesImported == 0 {
			t.Fatalf("import packs=%d indexes=%d err=%v", packsImported, indexesImported, err)
		}
		client, err = daemon.Ensure(context.Background(), daemon.Options{
			Socket: socket, RepositoryID: repositoryIdentity, DaemonPath: daemonPath,
			DataDir: dataDir, WALDataDir: filepath.Join(dataDir, "wal"), ObjectStore: "local",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		marker, found, err := client.Get(context.Background(), []byte("_vaultic/bulk-import-complete-v1"), "")
		if err != nil || !found || string(marker) != "complete" {
			t.Fatalf("bulk import completion marker: found=%t value=%q err=%v", found, marker, err)
		}
		packs, _, err := daemon.NewSchemaStore(client).ScanPrefix(context.Background(), []byte("p:"), nil, 100)
		if err != nil || uint64(len(packs)) != packsImported {
			t.Fatalf("imported catalog packs=%d want=%d err=%v", len(packs), packsImported, err)
		}
		observation := controller.Observation()
		if observation.Started == 0 || observation.Active != 0 {
			t.Fatalf("commit response observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), repositoryIdentity, fmt.Sprintf("packs=%d indexes=%d marker=%s", packsImported, indexesImported, marker))
	})
}

func TestPhase34M2CheckScanResponseSweep(t *testing.T) {
	artifacts := t.TempDir()
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, _ int) {
		env, socket, _, _ := phase34M2IndexFixture(t)
		repositoryIdentity := repositoryID(t, env)
		defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
		err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			_, runErr := runIndexImport(ctx, indexImportOptions{
				Daemon: indexDaemonOptions{Socket: socket}, FromLegacy: true, Resume: true, Activate: true, SnapshotDepth: ^uint(0),
			}, globalOptions, globalOptions.Term)
			return runErr
		})
		if err != nil {
			t.Fatal(err)
		}
		var baseline maintenance.CheckResult
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			var runErr error
			baseline, runErr = runIndexCheck(ctx, indexCheckOptions{
				Daemon: indexDaemonOptions{Socket: socket}, MaxFindings: 10,
			}, globalOptions, globalOptions.Term)
			return runErr
		})
		if err != nil {
			t.Fatal(err)
		}
		profile := phase34M2Profile("check", "rpc", "scan", delay)
		profile.TargetID = repositoryIdentity
		controller := phase34M2IndexController(t, profile, repositoryIdentity)
		started := time.Now()
		var result maintenance.CheckResult
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			var runErr error
			result, runErr = runIndexCheck(ctx, indexCheckOptions{
				Daemon:      indexDaemonOptions{Socket: socket, ResponseDeliveryForTesting: controller.ResponseDelivery("scan")},
				MaxFindings: 10,
			}, globalOptions, globalOptions.Term)
			return runErr
		})
		if err != nil {
			t.Fatalf("check result=%+v err=%v", result, err)
		}
		baseline.Consistency.SessionID = ""
		result.Consistency.SessionID = ""
		baseline.Consistency.OptionsDigest = ""
		result.Consistency.OptionsDigest = ""
		baseline.Resources.MemoryLimitBytes = 0
		result.Resources.MemoryLimitBytes = 0
		if !reflect.DeepEqual(result, baseline) {
			t.Fatalf("injected check result differs from baseline:\nbaseline=%+v\ninjected=%+v", baseline, result)
		}
		observation := controller.Observation()
		if observation.Started == 0 || observation.Active != 0 {
			t.Fatalf("scan response observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), baseline.Consistency.LegacyInventoryDigest, fmt.Sprintf("snapshots=%d locations=%d findings=%d", result.SlateDBSnapshots, result.SlateDBLocations, len(result.Findings)))
	})
}

func testIndexWorkflows(t *testing.T, s3Metadata bool) {
	t.Helper()
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil

	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socket := daemon.DefaultSocket(repositoryID(t, env))
	daemonConfig := daemon.Options{
		Socket: socket, RepositoryID: repositoryID(t, env), DaemonPath: daemonPath,
		DataDir: filepath.Join(env.base, "vaulticdb"), ObjectStore: "local",
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
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexImport(
			ctx,
			indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, DryRun: true, SnapshotDepth: ^uint(0)},
			globalOptions,
			globalOptions.Term,
		)
		dryResultIndexes = result.IndexesImported
		return runErr
	})
	if err != nil || dryResultIndexes == 0 {
		t.Fatalf("dry-run import indexes=%d err=%v", dryResultIndexes, err)
	}
	if entries, _, err := store.ScanPrefix(context.Background(), []byte("p:"), nil, 10); err != nil || len(entries) != 0 {
		t.Fatalf("dry-run wrote pack records: entries=%d err=%v", len(entries), err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, DryRun: true}, globalOptions, globalOptions.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("legacy-only dry-run export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 1}, globalOptions, globalOptions.Term)
		return runErr
	})
	if !errors.Is(err, errIndexDifferences) {
		t.Fatalf("legacy-only check error = %v", err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions, DryRun: true}, globalOptions, globalOptions.Term)
		if result.AggregatesChanged != 5 {
			t.Fatalf("legacy-only aggregate dry-run = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, runErr := runIndexImport(
			ctx,
			indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, SnapshotDepth: ^uint(0), SnapshotWorkBudget: 1},
			globalOptions,
			globalOptions.Term,
		)
		return runErr
	})
	if !errors.Is(err, errIndexIncomplete) {
		t.Fatalf("partial import error = %v", err)
	}
	if entries, _, scanErr := store.ScanPrefix(context.Background(), []byte("p:"), nil, 10); scanErr != nil || len(entries) != 2 {
		t.Fatalf("partial import pack catalog: entries=%d err=%v", len(entries), scanErr)
	}

	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	var resumed uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexImport(
			ctx,
			indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, Activate: true, SnapshotDepth: ^uint(0)},
			globalOptions,
			globalOptions.Term,
		)
		resumed = result.IndexesResumed
		return runErr
	})
	if err != nil || resumed == 0 {
		t.Fatalf("resumed activation indexes=%d err=%v", resumed, err)
	}
	var exported, selected, exportSequence uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, PacksPerIndex: 2, Verify: true}, globalOptions, globalOptions.Term)
		exported, selected, exportSequence = result.IndexesWritten, result.PacksSelected, result.ExportSequence
		return runErr
	})
	if err != nil || exported == 0 || selected == 0 {
		t.Fatalf("checkpointed export indexes=%d packs=%d err=%v", exported, selected, err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("resumed export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Since: exportSequence}, globalOptions, globalOptions.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("since export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	var fullExportSequence uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(
			ctx, indexExportOptions{Daemon: daemonOptions, Full: true, Verify: true, PacksPerIndex: 1},
			globalOptions, globalOptions.Term,
		)
		if result.PacksSelected == 0 || result.IndexesWritten == 0 {
			t.Fatalf("full export = %#v", result)
		}
		fullExportSequence = result.ExportSequence
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Since: exportSequence, DryRun: true}, globalOptions, globalOptions.Term)
		if result.PacksSelected == 0 || result.IndexesWritten != 0 || fullExportSequence <= exportSequence {
			t.Fatalf("positive since export = %#v, full sequence=%d", result, fullExportSequence)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	check := func() error {
		return withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 10}, globalOptions, globalOptions.Term)
			return runErr
		})
	}
	if err := check(); err != nil {
		t.Fatalf("clean check: %v", err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexCheck(
			ctx,
			indexCheckOptions{Daemon: daemonOptions, IncludeCrawlDebt: true, FailOnWarning: true, MaxFindings: 100},
			globalOptions,
			globalOptions.Term,
		)
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
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions, DryRun: true}, globalOptions, globalOptions.Term)
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
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatalf("check after repair: %v", err)
	}

	assertIntrospectionAnswersWithoutListing(t, env, daemonOptions)
	snapshotIDs := testListSnapshots(t, env.globalOptions, 1)
	snapshotID := snapshotIDs[0]
	value, found, err := store.Get(context.Background(), schema.SnapshotKey(schema.ID(snapshotID)))
	if err != nil || !found {
		t.Fatalf("imported snapshot missing: %v", err)
	}
	snapshot, err := schema.UnmarshalSnapshotRecord(value)
	if err != nil || snapshot.LegacyTree == (schema.ID{}) || snapshot.CommitSequence != 0 {
		t.Fatalf("import did not preserve historical root: %#v %v", snapshot, err)
	}
	if err := os.Remove(filepath.Join(env.repo, "snapshots", snapshotID.String())); err != nil {
		t.Fatal(err)
	}
	if ids := testListSnapshots(t, env.globalOptions, 1); ids[0] != snapshotID {
		t.Fatalf("stored snapshot ID changed: %v", ids)
	}
	restored := filepath.Join(env.base, "historical-restore")
	testRunRestore(t, env.globalOptions, restored, snapshotID.String()+":"+toPathInSnapshot(filepath.Dir(env.testdata)))
	difference := directoriesContentsDiff(t, env.testdata, filepath.Join(restored, filepath.Base(env.testdata)))
	test.Assert(t, difference == "", "historical restore differs: %s", difference)
	if err := check(); !errors.Is(err, errIndexDifferences) {
		t.Fatalf("legacy inventory difference was hidden: %v", err)
	}
	if err := store.ForgetSnapshot(context.Background(), schema.ID(snapshotID)); err != nil {
		t.Fatal(err)
	}
	testListSnapshots(t, env.globalOptions, 0)
	assertCompareDetectsMissingAndExtraObjects(t, env, daemonOptions)
}

// assertCompareDetectsMissingAndExtraObjects is the Phase 11 `--compare` test:
// a backend with a deliberately removed object and a deliberately added one
// must produce both findings, reported separately, because a pack the catalog
// claims but the backend lacks is data loss while an object the backend holds
// that the catalog does not know is only waste.
func assertCompareDetectsMissingAndExtraObjects(t *testing.T, env *testEnvironment, daemonOptions indexDaemonOptions) {
	t.Helper()

	// A clean repository must compare clean, so the findings below are
	// attributable to the damage and not to a permanently noisy comparison.
	var clean BackendsResult
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		clean, runErr = runIndexBackends(ctx, indexBackendsOptions{Daemon: daemonOptions, Compare: true}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil {
		t.Fatalf("compare on an intact repository failed: %v", err)
	}
	if clean.MissingOnBackendNum != 0 || clean.UnknownToCatalogNum != 0 {
		t.Fatalf("compare on an intact repository reported findings: %#v", clean)
	}
	if clean.CatalogPacks == 0 {
		t.Fatal("compare found no catalog packs to compare")
	}

	// Remove one real pack from the backend and add one object the catalog has
	// never heard of.
	packDir := filepath.Join(env.repo, "data")
	var removed, extra string
	err = filepath.Walk(packDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() || removed != "" {
			return walkErr
		}
		// Only a file whose name is a pack ID is a pack; the backend layout
		// may also hold temporary files, and removing one of those would
		// damage nothing the catalog knows about.
		if _, parseErr := vaultic.ParseID(info.Name()); parseErr != nil {
			return walkErr
		}
		removed = info.Name()
		return os.Remove(path)
	})
	if err != nil || removed == "" {
		t.Fatalf("could not remove a pack object: removed=%q err=%v", removed, err)
	}
	// The extra object must be a syntactically valid ID the catalog has never
	// seen, so flipping the leading nibbles of a real one is enough.
	extra = "00" + removed[2:]
	if extra == removed {
		extra = "11" + removed[2:]
	}
	extraPath := filepath.Join(packDir, extra[:2], extra)
	if err := os.MkdirAll(filepath.Dir(extraPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(extraPath, []byte("not a pack this repository knows about"), 0o644); err != nil {
		t.Fatal(err)
	}

	var damaged BackendsResult
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		damaged, runErr = runIndexBackends(ctx, indexBackendsOptions{Daemon: daemonOptions, Compare: true}, globalOptions, globalOptions.Term)
		return runErr
	})
	// A pack missing on the backend is a difference, so a non-zero exit is
	// expected and is itself part of the contract.
	if !errors.Is(err, errIndexDifferences) {
		t.Fatalf("compare on a damaged repository returned %v, want errIndexDifferences; result=%#v", err, damaged)
	}
	if damaged.MissingOnBackendNum != 1 {
		t.Errorf("missing on backend = %d, want 1: %#v", damaged.MissingOnBackendNum, damaged.MissingOnBackend)
	}
	if len(damaged.MissingOnBackend) != 1 || damaged.MissingOnBackend[0] != removed {
		t.Errorf("missing object = %#v, want [%s]", damaged.MissingOnBackend, removed)
	}
	if damaged.UnknownToCatalogNum != 1 {
		t.Errorf("unknown to catalog = %d, want 1: %#v", damaged.UnknownToCatalogNum, damaged.UnknownToCatalog)
	}
	if len(damaged.UnknownToCatalog) != 1 || damaged.UnknownToCatalog[0] != extra {
		t.Errorf("extra object = %#v, want [%s]", damaged.UnknownToCatalog, extra)
	}

	// Restore the backend so later assertions in this test see a sane state.
	if err := os.Remove(extraPath); err != nil {
		t.Fatal(err)
	}
}

// assertIntrospectionAnswersWithoutListing is the Phase 11 exit criterion: an
// operator must be able to obtain pack counts, sizes, per-backend composition,
// and a creation histogram with a growth rate for a repository whose archival
// backend is never listed.
func assertIntrospectionAnswersWithoutListing(t *testing.T, env *testEnvironment, daemonOptions indexDaemonOptions) {
	t.Helper()

	// Every listing from here on would be a violation of the criterion, so the
	// backend is wrapped to record them.
	counter := &listCountingBackend{calls: map[backend.FileType]int{}}
	previousHook := env.globalOptions.BackendTestHook
	env.globalOptions.BackendTestHook = func(inner backend.Backend) (backend.Backend, error) {
		counter.Backend = inner
		return counter, nil
	}
	defer func() { env.globalOptions.BackendTestHook = previousHook }()

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		// Pack counts and sizes, without a filter, must come from the constant-
		// time aggregates.
		stats, runErr := runIndexStats(ctx, indexStatsOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if stats.Source != maintenance.SourceAggregates {
			t.Errorf("unfiltered stats scanned the catalog: %s", stats.Source)
		}
		if stats.Totals.PackCount == 0 || stats.Totals.PhysicalSize == 0 {
			t.Errorf("stats reported no packs or no bytes: %#v", stats.Totals)
		}
		if stats.SchemaVersion != maintenance.IntrospectSchemaVersion {
			t.Errorf("stats schema version = %d, want %d", stats.SchemaVersion, maintenance.IntrospectSchemaVersion)
		}

		// Composition by tier and type.
		grouped, runErr := runIndexStats(ctx, indexStatsOptions{
			Daemon: daemonOptions, GroupBy: []string{"tier", "type"},
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if len(grouped.Groups) == 0 {
			t.Errorf("grouped stats produced no composition rows: %#v", grouped)
		}

		// Individual packs, from the catalog.
		packs, runErr := runIndexPacks(ctx, indexPacksOptions{
			Daemon: daemonOptions, Sort: "size", Limit: 5,
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if packs.Matched == 0 {
			t.Errorf("pack query matched nothing: %#v", packs)
		}

		// Per-backend composition without touching the backend.
		backends, runErr := runIndexBackends(ctx, indexBackendsOptions{
			Daemon: daemonOptions, NoList: true,
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if backends.ReducedMode {
			t.Errorf("a SlateDB-authoritative repository reported reduced mode")
		}
		if len(backends.Backends) == 0 {
			t.Errorf("no backends were reported")
		}

		// A creation histogram with a growth rate. The repository is young, so
		// the series may legitimately be too short to fit a trend; what the
		// criterion requires is that the answer is produced and that a refusal
		// is explicit rather than a silently absent number.
		history, runErr := runIndexHistory(ctx, indexHistoryOptions{
			Daemon: daemonOptions, Metric: "created", Bucket: "hour",
			Histogram: true, Forecast: true, AllowIncomplete: true,
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if len(history.Points) == 0 {
			t.Errorf("history reported no buckets for a repository that was just written")
		}
		if history.Forecast == nil {
			t.Errorf("--forecast produced neither a projection nor a refusal")
		} else if history.Forecast.RefusedReason == "" && history.Forecast.BucketsUsed == 0 {
			t.Errorf("forecast claimed a projection from no buckets: %#v", history.Forecast)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("introspection commands failed: %v", err)
	}

	for _, fileType := range []backend.FileType{backend.PackFile, backend.IndexFile, backend.SnapshotFile} {
		if calls := counter.calls[fileType]; calls != 0 {
			t.Errorf("introspection listed %v %d times; the exit criterion forbids listing", fileType, calls)
		}
	}
}

func repositoryID(t *testing.T, env *testEnvironment) string {
	t.Helper()
	var id string
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
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
