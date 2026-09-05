package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/schema"
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
