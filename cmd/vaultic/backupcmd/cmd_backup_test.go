package backupcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/global"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type backupCloseEngine struct {
	*enginepkg.LegacyEngine
	close func() error
	stats *enginepkg.BlobLookupStats
}

func (engine *backupCloseEngine) Close() error { return engine.close() }

func (engine *backupCloseEngine) BlobLookupStats() (enginepkg.BlobLookupStats, bool) {
	if engine.stats == nil {
		return enginepkg.BlobLookupStats{}, false
	}
	return *engine.stats, true
}

func TestBackupCloseReportsLookupStatsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	term := &ui.MockTerminal{}
	repo := repository.TestRepository(t)
	stats := &enginepkg.BlobLookupStats{Capacity: 2}
	repo.SetEngine(&backupCloseEngine{LegacyEngine: enginepkg.NewLegacyEngine(), stats: stats, close: func() error {
		if len(term.Output) != 0 {
			t.Fatal("stats emitted before engine close")
		}
		stats.SizeRPCs = 7
		return nil
	}})
	run := &backupRun{ctx: ctx, repo: repo, term: term, globalOptions: global.Options{JSON: true}}
	run.close()
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		enginepkg.BlobLookupStats
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "metadata_lookup_stats" || record.Capacity != 2 || record.SizeRPCs != 7 {
		t.Fatalf("invalid final stats: %+v", record)
	}
}

type backupReconciliationStore struct {
	reconcile.Store
}

func (*backupReconciliationStore) ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error) {
	return nil, true, nil
}

func TestBackupCloseJoinsReconciliationAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reconciler, err := reconcile.New(ctx, fs.NewLocal(), &backupReconciliationStore{}, reconcile.Options{Workers: 1, QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	term := &ui.MockTerminal{}
	run := &backupRun{ctx: ctx, cancel: cancel, reconciler: reconciler, term: term, globalOptions: global.Options{JSON: true}}
	run.closeSource = func() {
		if ctx.Err() == nil || len(term.Output) != 1 {
			t.Fatal("source closed before cancellation and final reconciliation stats")
		}
	}
	run.close()
	var record struct {
		MessageType string `json:"message_type"`
		reconcile.Metrics
	}
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "reconciliation_stats" || record.Metrics != reconciler.Metrics() || record.Failed == 0 {
		t.Fatalf("incomplete final reconciliation stats: %+v", record)
	}
}

func TestBackupReportsReconciliationStats(t *testing.T) {
	term := &ui.MockTerminal{}
	run := &backupRun{term: term, globalOptions: global.Options{JSON: true}}
	stats := reconcile.Metrics{
		PublicationGroups: [4]uint64{1, 2, 3, 4}, RevisionAllocationCalls: 7, RevisionAllocationFailures: 1,
		RevisionsReserved: 20, InodeRevisionsAssigned: 18, RevisionAllocationNS: 100,
		InodePublicationCalls: 17, InodePublicationFailures: 2, InodePublicationNS: 200, PublicationGroupNS: 150,
	}
	run.reportReconciliationStats(stats)
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		reconcile.Metrics
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "reconciliation_stats" || record.Metrics != stats {
		t.Fatalf("invalid reconciliation stats: %+v", record)
	}
	run.globalOptions = global.Options{Quiet: true}
	run.reportReconciliationStats(stats)
	if len(term.Output) != 1 {
		t.Fatal("quiet mode emitted reconciliation stats")
	}
}

func TestBackupCloseReportsNFSStatsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	term := &ui.MockTerminal{}
	run := &backupRun{ctx: ctx, cancel: cancel, term: term, globalOptions: global.Options{JSON: true}}
	stats := fs.NFSStats{Lookups: 1, Getattrs: 2, ReadDirPlus: 3, Reads: 4, ReadBytes: 5, CacheHits: 6,
		ParentHits: 7, RetainedOpens: 8,
		Operations: map[string]fs.NFSOperationStats{"read": {
			Attempts: 4, Calls: 3, Errors: 1, Cancellations: 1, MaxActive: 2, QueueNanoseconds: 10, ServiceNanoseconds: 20}}}
	run.closeSource = func() {
		if ctx.Err() == nil {
			t.Fatal("source stats emitted before cancellation")
		}
		run.reportNFSStats(stats)
	}
	run.close()
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		fs.NFSStats
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "nfs_source_stats" || !reflect.DeepEqual(record.NFSStats, stats) {
		t.Fatalf("invalid NFS stats: %+v", record)
	}
	run.globalOptions = global.Options{Quiet: true}
	run.reportNFSStats(stats)
	if len(term.Output) != 1 {
		t.Fatal("quiet mode emitted NFS stats")
	}
}

func TestMetadataCacheBudgetFlag(t *testing.T) {
	command := NewCommand(&global.Options{})
	if budget, err := command.Flags().GetInt("metadata-cache-mib"); err != nil || budget != 64 {
		t.Fatalf("default budget=%d err=%v", budget, err)
	}
	for _, budget := range []int{0, -1, (int(^uint(0)>>1) >> 20) + 1} {
		if err := command.Flags().Set("metadata-cache-mib", strconv.Itoa(budget)); err != nil {
			t.Fatal(err)
		}
		options := backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch", MetadataCacheMiB: budget}
		if err := options.validateParent(); err == nil {
			t.Fatalf("invalid budget %d accepted", budget)
		}
	}
	if err := (backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch", MetadataCacheMiB: 1}).validateParent(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupCloseReleasesEngineAfterUnlock(t *testing.T) {
	repo := repository.TestRepository(t)
	unlocked, closes := false, 0
	repo.SetEngine(&backupCloseEngine{LegacyEngine: enginepkg.NewLegacyEngine(), close: func() error {
		if !unlocked {
			t.Error("engine closed before repository unlock")
		}
		closes++
		return nil
	}})
	run := &backupRun{repo: repo, closeRepo: func() { unlocked = true }}
	run.close()
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if closes != 1 {
		t.Fatalf("engine close calls=%d", closes)
	}
}

func TestAutomaticDeferredFallbackDistinguishesUnavailableFromCorrupt(t *testing.T) {
	options := backupOptions{AllowDeferredCommit: true, DeferredMode: "auto"}
	if !shouldUseDataPlaneFallback(fmt.Errorf("connect: %w", enginepkg.ErrUnavailable), options) {
		t.Fatal("operational metadata unavailability did not select deferred fallback")
	}
	if shouldUseDataPlaneFallback(fmt.Errorf("corrupt metadata manifest"), options) {
		t.Fatal("metadata corruption bypassed explicit acknowledgement")
	}
	options.AcknowledgeMetadataBypass = true
	if !shouldUseDataPlaneFallback(fmt.Errorf("corrupt metadata manifest"), options) {
		t.Fatal("acknowledged metadata corruption did not select deferred fallback")
	}
}

func TestMarkerPrefetchSharesSelectionCache(t *testing.T) {
	for _, caches := range []bool{false, true} {
		t.Run(fmt.Sprint(caches), func(t *testing.T) {
			root := t.TempDir()
			marker, content := ".nobackup", ""
			options := backupOptions{ExcludeIfPresent: []string{marker}}
			if caches {
				marker, content = "CACHEDIR.TAG", "Signature: 8a477f597d28d172789f06886806bc55"
				options = backupOptions{ExcludeCaches: true}
			}
			markerPath := filepath.Join(root, marker)
			if err := os.WriteFile(markerPath, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			filesystem := fs.NewLocal()
			rejects, prefetch, err := collectRejectFuncsWithPrefetch(options, []string{root}, filesystem, t.Logf)
			if err != nil {
				t.Fatal(err)
			}
			if len(prefetch) != 1 {
				t.Fatalf("expected one marker prefetch, got %d", len(prefetch))
			}
			name := filepath.Join(root, "child")
			if archiver.CombineRejects(prefetch)(name, nil, filesystem) {
				t.Fatal("prefetch did not observe marker")
			}
			if err := os.Remove(markerPath); err != nil {
				t.Fatal(err)
			}
			selectItem := archiver.CombineRejects(rejects)
			if selectItem(name, nil, filesystem) {
				t.Fatal("selection did not reuse prefetched marker result")
			}
			if !selectItem(markerPath, nil, filesystem) {
				t.Fatal("marker itself was excluded")
			}
		})
	}
}

func TestBackupCrawlFlags(t *testing.T) {
	command := NewCommand(&global.Options{})
	for _, name := range []string{
		"use-cwalk", "no-cwalk", "cwalk-concurrency", "use-pathdiff", "pathdiff-endpoint",
		"pathdiff-require-coverage", "pathdiff-svm-map", "use-fsevents", "fsevents-require-coverage",
		"fsevents-replay-timeout", "fsevents-full-crawl-every", "apfs-snapshot", "apfs-snapshot-require", "apfs-snapshot-keep",
	} {
		if command.Flags().Lookup(name) == nil {
			t.Errorf("backup flag --%s is not registered", name)
		}
	}
	if value, err := command.Flags().GetBool("use-cwalk"); err != nil || !value {
		t.Errorf("use-cwalk = %t, %v; want true", value, err)
	}
	if value, err := command.Flags().GetInt("cwalk-concurrency"); err != nil || value != 32 {
		t.Errorf("cwalk concurrency = %d, %v; want 32", value, err)
	}
	if err := command.Flags().Set("cwalk-concurrency", "7"); err != nil {
		t.Fatal(err)
	}
	if value, err := command.Flags().GetInt("cwalk-concurrency"); err != nil || value != 7 {
		t.Errorf("configured cwalk concurrency = %d, %v; want 7", value, err)
	}
}

func TestDirectNFSSourceValidation(t *testing.T) {
	command := NewCommand(&global.Options{})
	if value, err := command.Flags().GetBool("nfs-allow-missing-metadata"); err != nil || !value {
		t.Fatalf("missing metadata default = %t, %v; want true", value, err)
	}
	if err := command.Flags().Set("nfs-allow-missing-metadata", "false"); err != nil {
		t.Fatal(err)
	}
	if value, err := command.Flags().GetBool("nfs-allow-missing-metadata"); err != nil || value {
		t.Fatalf("strict metadata option = %t, %v; want false", value, err)
	}
	targets := []string{"nfs://nas:/export/source"}
	if err := validateNFSSources(backupOptions{}, targets); err == nil {
		t.Fatal("direct NFS silently accepted missing ACL/xattr metadata")
	}
	options := backupOptions{NFSAllowMissingMetadata: true, NFSConnections: 4}
	if err := validateNFSSources(options, targets); err != nil {
		t.Fatal(err)
	}
	if filtered, err := filterExisting(targets, func(string, ...any) {}); err != nil || len(filtered) != 1 || filtered[0] != targets[0] {
		t.Fatalf("URL was treated as a local path: %v %v", filtered, err)
	}
	options.UseFsSnapshot = true
	if err := validateNFSSources(options, targets); err == nil {
		t.Fatal("accepted local snapshot with direct NFS")
	}
	options.UseFsSnapshot = false
	options.NFSConnections = 17
	if err := validateNFSSources(options, targets); err == nil {
		t.Fatal("accepted unbounded NFS connections")
	}
	options.NFSConnections = 4
	options.NFSDirect = true
	if err := validateNFSSources(options, []string{"/mnt/nfs/source"}); err != nil {
		t.Fatal(err)
	}
}

func TestNoCWalkRestoresLegacyTraversal(t *testing.T) {
	command := NewCommand(&global.Options{})
	if err := command.Flags().Set("no-cwalk", "true"); err != nil {
		t.Fatal(err)
	}
	if err := command.PreRunE(command, nil); err != nil {
		t.Fatal(err)
	}
	if value, err := command.Flags().GetBool("use-cwalk"); err != nil || value {
		t.Errorf("use-cwalk = %t, %v; want false after --no-cwalk", value, err)
	}
}

func TestBackupCrawlOptionValidation(t *testing.T) {
	globalOptions := global.Options{InsecureNoPassword: true}
	tests := []struct {
		name    string
		options backupOptions
		want    string
	}{
		{"invalid-workers", backupOptions{UseCWalk: true}, "--cwalk-concurrency must be at least 1"},
		{"on-demand-needs-scratch", backupOptions{MetadataOnDemand: true}, "--metadata-on-demand requires --metadata-scratch"},
		{"scratch-needs-on-demand", backupOptions{MetadataScratch: "scratch"}, "--metadata-scratch requires --metadata-on-demand"},
		{"cache-needs-on-demand", backupOptions{MetadataCacheMiB: 128}, "--metadata-cache-mib requires --metadata-on-demand"},
		{"cache-needs-positive-budget", backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch"}, "--metadata-cache-mib must be positive"},
		{"on-demand-rejects-dry-run", backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch", DryRun: true}, "--metadata-on-demand cannot use"},
		{"pathdiff-needs-cwalk", backupOptions{UsePathdiff: true}, "--use-pathdiff requires --use-cwalk"},
		{"pathdiff-needs-endpoint", backupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true}, "--use-pathdiff requires --pathdiff-endpoint"},
		{
			"pathdiff-needs-map",
			backupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true, PathdiffEndpoint: "socket"},
			"--use-pathdiff requires --pathdiff-svm-map",
		},
		{"coverage-needs-pathdiff", backupOptions{PathdiffRequireCoverage: true}, "--pathdiff-require-coverage requires --use-pathdiff"},
		{
			"change-sources-exclusive",
			backupOptions{
				UseCWalk: true, CWalkConcurrency: 1,
				UsePathdiff: true, PathdiffEndpoint: "socket", PathdiffSVMMap: "map",
				UseFSEvents: true, FSEventsReplayTimeout: time.Minute,
			},
			"mutually exclusive",
		},
		{"fsevents-needs-cwalk", backupOptions{UseFSEvents: true, FSEventsReplayTimeout: time.Minute}, "--use-fsevents requires --use-cwalk"},
		{"fsevents-coverage-needs-source", backupOptions{FSEventsRequireCoverage: true}, "--fsevents-require-coverage requires --use-fsevents"},
		{"fsevents-timeout-positive", backupOptions{UseCWalk: true, CWalkConcurrency: 1, UseFSEvents: true}, "--fsevents-replay-timeout must be positive"},
		{"full-crawl-duration-nonnegative", backupOptions{FSEventsFullCrawlEvery: -time.Second}, "--fsevents-full-crawl-every cannot be negative"},
		{"snapshot-require-needs-snapshot", backupOptions{APFSSnapshotRequire: true}, "--apfs-snapshot-require requires --apfs-snapshot"},
		{"snapshot-keep-needs-snapshot", backupOptions{APFSSnapshotKeep: true}, "--apfs-snapshot-keep requires --apfs-snapshot"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.Check(globalOptions, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Check() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateDeferred(t *testing.T) {
	tests := []struct {
		name    string
		options backupOptions
		want    string
	}{
		{"mode-requires-opt-in", backupOptions{DeferredMode: "auto"}, "--deferred-mode requires"},
		{"mode-is-enumerated", backupOptions{AllowDeferredCommit: true, DeferredMode: "invalid"}, "requires --deferred-mode=auto"},
		{"bypass-is-acknowledged", backupOptions{AllowDeferredCommit: true, DeferredMode: "data-plane-only"}, "requires --acknowledge-metadata-bypass"},
		{"incompatible-parent", backupOptions{AllowDeferredCommit: true, DeferredMode: "auto", Parent: "latest"}, "deferred ingest cannot use"},
		{"positive-expiry", backupOptions{AllowDeferredCommit: true, DeferredMode: "auto"}, "--deferred-expiry must be positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.validateDeferred()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateDeferred() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateStdin(t *testing.T) {
	tests := []struct {
		name          string
		options       backupOptions
		globalOptions global.Options
		args          []string
		want          string
	}{
		{"password-and-data", backupOptions{Stdin: true}, global.Options{}, nil, "cannot read both password and data"},
		{"password-and-files-from", backupOptions{FilesFrom: []string{"-"}}, global.Options{}, nil, "unable to read password from stdin"},
		{
			"stdin-and-files-from", backupOptions{Stdin: true, FilesFrom: []string{"list"}},
			global.Options{InsecureNoPassword: true}, nil, "--stdin and --files-from cannot",
		},
		{"stdin-and-arguments", backupOptions{Stdin: true}, global.Options{InsecureNoPassword: true}, []string{"source"}, "files/dirs were listed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.validateStdin(test.globalOptions, test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateStdin() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateParent(t *testing.T) {
	options := backupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true, PathdiffEndpoint: "socket", PathdiffSVMMap: "map"}
	if err := options.validateParent(); err != nil {
		t.Fatalf("validateParent() error = %v", err)
	}
	options.PathdiffRequireCoverage = true
	if err := options.validateParent(); err != nil {
		t.Fatalf("validateParent() with coverage error = %v", err)
	}
}

func TestBackupHooksWireCallbacks(t *testing.T) {
	var reused, failed, reconciled, before, uploaded bool
	var completed, started, blobs, excluded int
	hooks := backupHooks{
		reuseSubtree:   func(string, string, *data.Node) bool { reused = true; return true },
		errorHandler:   func(string, error) error { failed = true; return nil },
		beforeSnapshot: func() error { before = true; return nil },
		reconcileNode:  func(string, string, *data.Node) { reconciled = true },
		deferredUploader: func(_ context.Context, _ func(context.Context, vaultic.BlobSaverWithAsync) error) error {
			uploaded = true
			return nil
		},
		progress: backupProgressHooks{
			completeItem: func(string, archiver.ItemAction, archiver.ItemStats, time.Duration) { completed++ },
			startFile:    func(string) { started++ },
			completeBlob: func(uint64) { blobs++ },
			excludedItem: func(string) { excluded++ },
		},
	}
	target := &archiver.Archiver{}
	hooks.wireReuseSubtree(target)
	hooks.wireError(target)
	hooks.wireProgress(target)
	hooks.wireReconciliation(target)
	var snapshotOpts archiver.SnapshotOptions
	hooks.wireDeferredUploader(&snapshotOpts)

	if !target.ReuseSubtree("", "", nil) || target.Error("", errors.New("read")) != nil {
		t.Fatal("wired reuse or error callback returned an unexpected result")
	}
	target.CompleteItem("", archiver.ItemAction(""), archiver.ItemStats{}, 0)
	target.StartFile("")
	target.CompleteBlob(0)
	target.ExcludedItem("")
	target.ReconcileNode("", "", nil)
	if err := target.BeforeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := snapshotOpts.DeferredUploader(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !reused || !failed || !reconciled || !before || !uploaded || completed != 1 || started != 1 || blobs != 1 || excluded != 1 {
		t.Fatalf("callbacks not all invoked: reuse=%v error=%v reconcile=%v before=%v upload=%v progress=%d/%d/%d/%d",
			reused, failed, reconciled, before, uploaded, completed, started, blobs, excluded)
	}
}

func TestBackupHooksDeferredCaptureChainsReconciliation(t *testing.T) {
	dir := rtest.TempDir(t)
	source := filepath.Join(dir, "source")
	rtest.OK(t, os.WriteFile(source, []byte("data"), 0600))
	capture := reconcile.NewDeferredCapture(fs.NewLocal())
	called := false
	target := &archiver.Archiver{ReconcileNode: func(string, string, *data.Node) { called = true }}
	hooks := backupHooks{deferredCapture: capture}
	hooks.wireDeferredCapture(target)
	target.ReconcileNode("/source", source, &data.Node{Name: "source"})
	observations, err := capture.Close()
	rtest.OK(t, err)
	if !called || len(observations) != 1 {
		t.Fatalf("deferred capture did not chain: prior=%v observations=%d", called, len(observations))
	}
}

func TestCollectTargets(t *testing.T) {
	dir := rtest.TempDir(t)

	fooSpace := "foo "
	barStar := "bar*"              // Must sort before the others, below.
	if runtime.GOOS == "windows" { // Doesn't allow "*" or trailing space.
		fooSpace = "foo"
		barStar = "bar"
	}

	var expect []string
	for _, filename := range []string{
		barStar, "baz", "cmdline arg", fooSpace,
		"fromfile", "fromfile-raw", "fromfile-verbatim", "quux",
	} {
		// All mentioned files must exist for collectTargets.
		f, err := os.Create(filepath.Join(dir, filename))
		rtest.OK(t, err)
		rtest.OK(t, f.Close())

		expect = append(expect, f.Name())
	}

	f1, err := os.Create(filepath.Join(dir, "fromfile"))
	rtest.OK(t, err)
	// Empty lines should be ignored. A line starting with '#' is a comment.
	_, err = fmt.Fprintf(f1, "\n%s*\n # here's a comment\n", f1.Name())
	rtest.OK(t, err)
	rtest.OK(t, f1.Close())

	f2, err := os.Create(filepath.Join(dir, "fromfile-verbatim"))
	rtest.OK(t, err)
	for _, filename := range []string{fooSpace, barStar} {
		// Empty lines should be ignored. CR+LF is allowed.
		_, err = fmt.Fprintf(f2, "%s\r\n\n", filepath.Join(dir, filename))
		rtest.OK(t, err)
	}
	rtest.OK(t, f2.Close())

	f3, err := os.Create(filepath.Join(dir, "fromfile-raw"))
	rtest.OK(t, err)
	for _, filename := range []string{"baz", "quux"} {
		_, err = fmt.Fprintf(f3, "%s\x00", filepath.Join(dir, filename))
		rtest.OK(t, err)
	}
	rtest.OK(t, err)
	rtest.OK(t, f3.Close())

	options := backupOptions{
		FilesFrom:         []string{f1.Name()},
		FilesFromVerbatim: []string{f2.Name()},
		FilesFromRaw:      []string{f3.Name()},
	}

	targets, err := collectTargets(options, []string{filepath.Join(dir, "cmdline arg")}, t.Logf, nil)
	rtest.OK(t, err)
	sort.Strings(targets)
	rtest.Equals(t, expect, targets)

	_, err = collectTargets(options, []string{filepath.Join(dir, "cmdline arg"), filepath.Join(dir, "non-existing-file")}, t.Logf, nil)
	rtest.Assert(t, errors.Is(err, ErrInvalidSourceData), "expected error when not all targets exist")
}

func TestFilterExistingUnreadable(t *testing.T) {
	dir := rtest.TempDir(t)

	existing := filepath.Join(dir, "existing")
	rtest.OK(t, os.Mkdir(existing, 0755))

	file := filepath.Join(dir, "file")
	rtest.OK(t, os.WriteFile(file, []byte("x"), 0600))

	// Regression test for #5667. A target whose Lstat fails with an error other
	// than ErrNotExist must be skipped (ENOTDIR on unix, NUL byte everywhere).
	for _, unreadable := range []string{filepath.Join(file, "child"), "invalid\x00path"} {
		result, err := filterExisting([]string{unreadable}, t.Logf)
		rtest.Assert(t, errors.Is(err, ErrNoSourceData), "input %q: expected ErrNoSourceData; got %v", unreadable, err)
		rtest.Assert(t, len(result) == 0, "input %q: expected no targets; got %v", unreadable, result)

		result, err = filterExisting([]string{existing, unreadable}, t.Logf)
		rtest.Assert(t, errors.Is(err, ErrInvalidSourceData), "input %q: expected ErrInvalidSourceData; got %v", unreadable, err)
		rtest.Equals(t, []string{existing}, result)
	}
}

func TestReadFilenamesRaw(t *testing.T) {
	// These should all be returned exactly as-is.
	expected := []string{
		"\xef\xbb\xbf/utf-8-bom",
		"/absolute",
		"../.././relative",
		"\t\t leading and trailing space   \t\t",
		"newline\nin filename",
		"not UTF-8: \x80\xff/simple",
		` / *[]* \ `,
	}

	var buf bytes.Buffer
	for _, name := range expected {
		buf.WriteString(name)
		buf.WriteByte(0)
	}

	got, err := readFilenamesRaw(&buf)
	rtest.OK(t, err)
	rtest.Equals(t, expected, got)

	// Empty input is ok.
	got, err = readFilenamesRaw(strings.NewReader(""))
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(got))

	// An empty filename is an error.
	_, err = readFilenamesRaw(strings.NewReader("foo\x00\x00"))
	rtest.Assert(t, err != nil, "no error for zero byte")
	rtest.Assert(t, strings.Contains(err.Error(), "empty filename"),
		"wrong error message: %v", err.Error())

	// No trailing NUL byte is an error, because it likely means we're
	// reading a line-oriented text file (someone forgot -print0).
	_, err = readFilenamesRaw(strings.NewReader("simple.txt"))
	rtest.Assert(t, err != nil, "no error for zero byte")
	rtest.Assert(t, strings.Contains(err.Error(), "zero byte"),
		"wrong error message: %v", err.Error())
}
