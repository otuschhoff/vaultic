package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"runtime"
	"strings"
	"time"

	uppathdiff "github.com/otuschhoff/pathdiff"
	"golang.org/x/sync/errgroup"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/crawl"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/global"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/analytics"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/staging" //nolint:depguard // backup publishes deferred staging journals
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/textfile"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/backup"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type backupRun struct {
	ctx       context.Context
	opts      BackupOptions
	gopts     global.Options
	term      ui.Terminal
	args      []string
	printer   backup.ProgressPrinter
	progress  *backup.Progress
	success   bool
	targets   []string
	timeStamp time.Time
	start     time.Time
	vssConfig fs.VSSConfig

	repo                *repository.Repository
	deferredActive      bool
	metadataBypassed    bool
	parent              *data.Snapshot
	targetFS            fs.FS
	pathdiffPlan        crawl.Plan
	authoritativeEngine *enginepkg.DaemonEngine
	reconciler          *reconcile.Reconciler

	group        *errgroup.Group
	cancel       context.CancelFunc
	archiver     *archiver.Archiver
	hooks        backupHooks
	snapshot     *data.Snapshot
	snapshotID   vaultic.ID
	summary      *archiver.Summary
	snapshotOpts archiver.SnapshotOptions

	deferredResult repository.DeferredUploadResult
	deferredStore  staging.Store
	deferredJobID  string
	deferredSeal   staging.Seal
	waitErr        error

	closeSource func()
	closeRepo   func()
}

type backupProgressHooks struct {
	completeItem func(string, archiver.ItemAction, archiver.ItemStats, time.Duration)
	startFile    func(string)
	completeBlob func(uint64)
	excludedItem func(string)
}

type backupHooks struct {
	reuseSubtree     func(string, string, *data.Node) bool
	errorHandler     archiver.ErrorFunc
	beforeSnapshot   func() error
	reconcileNode    func(string, string, *data.Node)
	deferredUploader func(context.Context, func(context.Context, vaultic.BlobSaverWithAsync) error) error
	progress         backupProgressHooks
	deferredCapture  *reconcile.DeferredCapture
}

func (hooks *backupHooks) wireReuseSubtree(target *archiver.Archiver) {
	if hooks.reuseSubtree != nil {
		target.ReuseSubtree = hooks.reuseSubtree
	}
}

func (hooks *backupHooks) wireError(target *archiver.Archiver) {
	if hooks.errorHandler != nil {
		target.Error = hooks.errorHandler
	}
}

func (hooks *backupHooks) wireProgress(target *archiver.Archiver) {
	target.CompleteItem = hooks.progress.completeItem
	target.StartFile = hooks.progress.startFile
	target.CompleteBlob = hooks.progress.completeBlob
	target.ExcludedItem = hooks.progress.excludedItem
}

func (hooks *backupHooks) wireReconciliation(target *archiver.Archiver) {
	if hooks.reconcileNode != nil {
		target.ReconcileNode = hooks.reconcileNode
	}
	if hooks.beforeSnapshot != nil {
		target.BeforeSnapshot = hooks.beforeSnapshot
	}
}

func (hooks *backupHooks) wireDeferredCapture(target *archiver.Archiver) {
	if hooks.deferredCapture == nil {
		return
	}
	previous := target.ReconcileNode
	target.ReconcileNode = func(snapshotPath, sourcePath string, node *data.Node) {
		previous(snapshotPath, sourcePath, node)
		hooks.deferredCapture.Observe(snapshotPath, sourcePath, node)
	}
}

func (hooks *backupHooks) wireDeferredUploader(opts *archiver.SnapshotOptions) {
	if hooks.deferredUploader != nil {
		opts.DeferredUploader = hooks.deferredUploader
	}
}

func runBackupPipeline(ctx context.Context, opts BackupOptions, gopts global.Options, term ui.Terminal, args []string) error {
	run := &backupRun{ctx: ctx, opts: opts, gopts: gopts, term: term, args: args, success: true}
	if err := prepareBackupTargets(run); err != nil {
		return err
	}
	defer run.close()
	var err error
	ctx, err = openBackupSources(ctx, run)
	if err != nil {
		return err
	}
	if err := configureArchiver(ctx, run); err != nil {
		return err
	}
	if err := executeBackup(run); err != nil {
		return err
	}
	return reportBackup(run)
}

func (run *backupRun) close() {
	if run.cancel != nil {
		run.cancel()
	}
	if run.closeSource != nil {
		run.closeSource()
	}
	if run.progress != nil {
		run.progress.Done()
	}
	if run.closeRepo != nil {
		run.closeRepo()
	}
}

func prepareBackupTargets(run *backupRun) error {
	if run.gopts.JSON {
		run.printer = backup.NewJSONProgress(run.term, run.gopts.Verbosity)
	} else {
		run.printer = backup.NewTextProgress(run.term, run.gopts.Verbosity)
	}
	if runtime.GOOS == "windows" {
		config, err := fs.ParseVSSConfig(run.gopts.Extended)
		if err != nil {
			return err
		}
		run.vssConfig = config
	}
	if err := run.opts.Check(run.gopts, run.args); err != nil {
		return err
	}
	targets, err := collectTargets(run.opts, run.args, run.printer.E, run.term.InputRaw())
	if err != nil && !errors.Is(err, ErrInvalidSourceData) {
		return err
	}
	if err != nil {
		run.success = false
	}
	run.targets = targets
	run.timeStamp = time.Now()
	run.start = run.timeStamp
	if run.opts.TimeStamp != "" {
		run.timeStamp, err = time.ParseInLocation(global.TimeFormat, run.opts.TimeStamp, time.Local)
		if err != nil {
			return errors.Fatalf("error in time option: %v", err)
		}
	}
	return initializeBackupRepository(run)
}

func initializeBackupRepository(run *backupRun) error {
	if run.gopts.Verbosity >= 2 && !run.gopts.JSON {
		run.printer.P("open repository")
	}
	if !run.opts.Init {
		return nil
	}
	_, err := global.OpenRepository(run.ctx, run.gopts, run.printer)
	if errors.Is(err, global.ErrNoRepository) {
		_, err = global.CreateRepository(run.ctx, run.gopts, vaultic.StableRepoVersion, nil, run.printer)
		if err != nil {
			return errors.Fatalf("initialize repository: %v", err)
		}
	}
	return err
}

func openBackupSources(ctx context.Context, run *backupRun) (context.Context, error) {
	ctx, err := openBackupRepository(ctx, run)
	if err != nil {
		return ctx, err
	}
	run.progress = backup.NewProgress(run.printer, run.gopts.Quiet, run.gopts.JSON, run.term.CanUpdateStatus())
	if err := loadBackupParent(run); err != nil {
		return ctx, err
	}
	return ctx, openBackupFilesystem(run)
}

func openBackupRepository(ctx context.Context, run *backupRun) (context.Context, error) {
	run.deferredActive = run.opts.AllowDeferredCommit && run.opts.DeferredMode != "auto"
	run.metadataBypassed = run.opts.DeferredMode == "data-plane-only"
	var err error
	if run.metadataBypassed {
		run.repo, err = global.OpenDataPlaneRepository(ctx, run.gopts, run.printer)
		if err == nil {
			run.closeRepo = func() { _ = run.repo.Close() }
		}
	} else {
		ctx, run.repo, run.closeRepo, err = openWithAppendLock(ctx, run.gopts, run.opts.DryRun, run.printer)
		run.ctx = ctx
		err = applyDeferredFallback(run, err)
	}
	if err != nil {
		return ctx, err
	}
	if run.deferredActive {
		emitDeferredMode(run)
	}
	return ctx, nil
}

func applyDeferredFallback(run *backupRun, openErr error) error {
	if shouldUseDataPlaneFallback(openErr, run.opts) {
		repo, err := global.OpenDataPlaneRepository(run.ctx, run.gopts, run.printer)
		if err != nil {
			return err
		}
		run.repo = repo
		run.deferredActive = true
		run.metadataBypassed = true
		run.closeRepo = func() { _ = repo.Close() }
		return nil
	}
	if openErr != nil || run.opts.DeferredMode != "auto" {
		return openErr
	}
	if engine, ok := run.repo.Engine().(*enginepkg.DaemonEngine); ok {
		status, err := engine.Client().WriterStatus(run.ctx)
		run.deferredActive = err != nil || status.Role != "read-write"
	}
	return nil
}

func emitDeferredMode(run *backupRun) {
	severity := observability.Notice
	message := "deferred backup mode selected"
	if run.metadataBypassed {
		severity = observability.Critical
		message = "metadata bypassed for data-plane-only deferred crawl"
	}
	_ = observability.Emit(run.ctx, observability.Event{
		Severity: severity, Category: observability.CategoryLifecycle,
		Component: "backup", Message: message, Fields: map[string]any{"mode": run.opts.DeferredMode},
	})
}

func loadBackupParent(run *backupRun) error {
	if !run.opts.Stdin && !run.deferredActive {
		parent, err := findParentSnapshot(run.ctx, run.repo, run.opts, run.targets, run.timeStamp)
		if err != nil {
			return err
		}
		run.parent = parent
		if !run.gopts.JSON && parent != nil {
			run.printer.P("using parent snapshot %v\n", parent.ID().Str())
		} else if !run.gopts.JSON {
			run.printer.P("no parent snapshot found, will read all files\n")
		}
	}
	if run.deferredActive {
		return nil
	}
	if !run.gopts.JSON {
		run.printer.V("load index files")
	}
	return run.repo.LoadIndex(run.ctx, run.printer)
}

func openBackupFilesystem(run *backupRun) error {
	run.targetFS = fs.NewLocal()
	if runtime.GOOS == "windows" && run.opts.UseFsSnapshot {
		if err := fs.HasSufficientPrivilegesForVSS(); err != nil {
			return err
		}
		errorHandler := func(item string, err error) { _ = run.progress.Error(item, err) }
		messageHandler := func(msg string, args ...any) {
			if !run.gopts.JSON {
				run.printer.P(msg, args...)
			}
		}
		localVSS := fs.NewLocalVss(errorHandler, messageHandler, run.vssConfig)
		run.targetFS = localVSS
		run.closeSource = localVSS.DeleteSnapshots
	}
	if run.opts.Stdin || run.opts.StdinCommand {
		return openBackupStdin(run)
	}
	if run.opts.fsTestHook != nil {
		run.targetFS = run.opts.fsTestHook(run.targetFS)
	}
	return nil
}

func openBackupStdin(run *backupRun) error {
	if !run.gopts.JSON {
		run.printer.V("read data from stdin")
	}
	filename := path.Join("/", run.opts.StdinFilename)
	source := run.term.InputRaw()
	var err error
	if run.opts.StdinCommand {
		source, err = fs.NewCommandReader(run.ctx, run.args, run.printer.E)
		if err != nil {
			return err
		}
	}
	run.targetFS, err = fs.NewReader(filename, source, fs.ReaderOptions{ModTime: run.timeStamp, Mode: 0644})
	if err != nil {
		return fmt.Errorf("failed to backup from stdin: %w", err)
	}
	run.targets = []string{filename}
	if run.opts.fsTestHook != nil {
		run.targetFS = run.opts.fsTestHook(run.targetFS)
	}
	return nil
}

func configureArchiver(ctx context.Context, run *backupRun) error {
	if err := configurePathdiff(run); err != nil {
		return err
	}
	selectByName, selectItem, mandatory, err := configureBackupSelection(run)
	if err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(ctx)
	cancelCtx, cancel := context.WithCancel(groupCtx)
	run.group, run.cancel = group, cancel
	configureBackupScanner(cancelCtx, run, selectByName, selectItem)
	options := archiver.Options{ReadConcurrency: run.opts.ReadConcurrency}
	if run.opts.UseCWalk && (!run.pathdiffPlan.Selective || len(run.pathdiffPlan.ChangedDirs) > 0) {
		options.CWalkConcurrency, options.CWalkQueue = run.opts.CWalkConcurrency, 4096
		if run.pathdiffPlan.Selective {
			options.CWalkRoots = run.pathdiffPlan.ChangedDirs
		}
	}
	repo := run.repo.AppendTransaction()
	if run.deferredActive {
		repo = run.repo.DeferredTransaction()
	}
	run.archiver = archiver.New(repo, run.targetFS, options)
	run.archiver.SelectByName, run.archiver.Select = selectByName, selectItem
	run.archiver.MandatorySelect = mandatory
	run.archiver.WithAtime = run.opts.WithAtime
	if err := configureBackupHooks(cancelCtx, run); err != nil {
		return err
	}
	configureBackupChangeFlags(run)
	return configureSnapshotOptions(run)
}

func configurePathdiff(run *backupRun) error {
	run.pathdiffPlan = crawl.Plan{Reason: "pathdiff is disabled"}
	if !run.opts.UsePathdiff {
		return nil
	}
	if !fs.IsLocal(run.targetFS) {
		run.pathdiffPlan.Reason = "source does not use the plain local filesystem"
	} else if run.parent == nil {
		run.pathdiffPlan.Reason = "no parent snapshot is available"
	} else if topology, err := crawl.LoadTopology(run.opts.PathdiffSVMMap); err != nil {
		run.pathdiffPlan.Reason = err.Error()
	} else {
		service := crawl.NewPathdiffService(uppathdiff.NewClient(run.opts.PathdiffEndpoint))
		plan, err := crawl.BuildPathdiffPlan(run.ctx, service, topology, run.targets, run.parent.Time, run.start)
		if err != nil {
			return fmt.Errorf("build pathdiff crawl plan: %w", err)
		}
		run.pathdiffPlan = plan
	}
	if !run.pathdiffPlan.Selective && run.opts.PathdiffRequireCoverage {
		return errors.Fatalf("pathdiff coverage is required: %s", run.pathdiffPlan.Reason)
	}
	if run.gopts.JSON {
		return nil
	}
	if run.pathdiffPlan.Selective {
		run.printer.V("pathdiff coverage verified; crawling %d changed subtrees", len(run.pathdiffPlan.ChangedDirs))
	} else {
		run.printer.V("pathdiff coverage unverified, using full cwalk traversal: %s", run.pathdiffPlan.Reason)
	}
	return nil
}

func configureBackupSelection(run *backupRun) (archiver.SelectByNameFunc, archiver.SelectFunc, archiver.SelectFunc, error) {
	byName, err := collectRejectByNameFuncs(run.opts, run.repo, run.printer.E)
	if err != nil {
		return nil, nil, nil, err
	}
	rejects, err := collectRejectFuncs(run.opts, run.targets, run.targetFS, run.printer.E)
	if err != nil {
		return nil, nil, nil, err
	}
	var mandatory archiver.SelectFunc = func(_ string, _ *fs.ExtendedFileInfo, _ fs.FS) bool { return true }
	if engine, ok := run.repo.Engine().(*enginepkg.DaemonEngine); ok && !run.deferredActive {
		run.authoritativeEngine = engine
		excludedUIDs, err := analytics.ExcludedUIDs(run.ctx, engine.SchemaStore())
		if err != nil {
			return nil, nil, nil, fmt.Errorf("load UID exclusion policy: %w", err)
		}
		if len(excludedUIDs) > 0 {
			uidPolicy := archiver.RejectUIDs(excludedUIDs)
			rejects = append(rejects, uidPolicy)
			mandatory = archiver.CombineRejects([]archiver.RejectFunc{uidPolicy})
		}
	}
	return archiver.CombineRejectByNames(byName), archiver.CombineRejects(rejects), mandatory, nil
}

func configureBackupScanner(ctx context.Context, run *backupRun, byName archiver.SelectByNameFunc, selectItem archiver.SelectFunc) {
	if run.opts.NoScan || run.pathdiffPlan.Selective || run.opts.UseCWalk {
		return
	}
	scanner := archiver.NewScanner(run.targetFS)
	scanner.SelectByName, scanner.Select = byName, selectItem
	scanner.Error, scanner.Result = run.printer.ScannerError, run.progress.ReportTotal
	if !run.gopts.JSON {
		run.printer.V("start scan on %v", run.targets)
	}
	run.group.Go(func() error { return scanner.Scan(ctx, run.targets) })
}

func configureBackupHooks(ctx context.Context, run *backupRun) error {
	run.hooks.errorHandler = func(item string, err error) error {
		run.success = false
		result := run.progress.Error(item, err)
		if result == nil && errors.IsFatal(err) {
			return err
		}
		return result
	}
	run.hooks.progress = backupProgressHooks{
		completeItem: run.progress.CompleteItem,
		startFile:    run.progress.StartFile,
		completeBlob: run.progress.CompleteBlob,
		excludedItem: run.progress.ExcludedItem,
	}
	if run.pathdiffPlan.Selective {
		run.hooks.reuseSubtree = func(_ string, sourcePath string, _ *data.Node) bool { return run.pathdiffPlan.ReuseSubtree(sourcePath) }
	}
	run.hooks.wireReuseSubtree(run.archiver)
	run.hooks.wireError(run.archiver)
	run.hooks.wireProgress(run.archiver)
	return configureReconciliationHooks(ctx, run)
}

func configureReconciliationHooks(ctx context.Context, run *backupRun) error {
	if run.authoritativeEngine != nil {
		reconciler, err := reconcile.New(ctx, run.targetFS, run.authoritativeEngine.SchemaStore(), reconcile.Options{
			PathIndexPaths: run.repo.Config().PathIndexPaths,
		})
		if err != nil {
			return fmt.Errorf("start authoritative metadata reconciliation: %w", err)
		}
		run.reconciler = reconciler
		reconcile.Attach(run.archiver, reconciler)
		run.hooks.beforeSnapshot = func() error {
			if err := reconciler.Close(); err != nil {
				return err
			}
			run.authoritativeEngine.SetNextSnapshotRoot(reconciler.RootKey())
			return nil
		}
		run.hooks.reconcileNode = run.archiver.ReconcileNode
	}
	if run.deferredActive {
		run.hooks.deferredCapture = reconcile.NewDeferredCapture(run.targetFS)
	}
	run.hooks.wireReconciliation(run.archiver)
	run.hooks.wireDeferredCapture(run.archiver)
	return nil
}

func configureBackupChangeFlags(run *backupRun) {
	if run.opts.IgnoreInode {
		run.archiver.ChangeIgnoreFlags |= archiver.ChangeIgnoreCtime | archiver.ChangeIgnoreInode
	}
	if run.opts.IgnoreCtime {
		run.archiver.ChangeIgnoreFlags |= archiver.ChangeIgnoreCtime
	}
}

func configureSnapshotOptions(run *backupRun) error {
	run.snapshotOpts = archiver.SnapshotOptions{
		Excludes: run.opts.Excludes, Tags: run.opts.Tags.Flatten(),
		BackupStart: run.start, Time: run.timeStamp, Hostname: run.opts.Host,
		Label: run.opts.Label, ParentSnapshot: run.parent,
		ProgramVersion: "vaultic " + global.Version, SkipIfUnchanged: run.opts.SkipIfUnchanged,
	}
	if run.deferredActive {
		if err := configureDeferredUploader(run); err != nil {
			return err
		}
	}
	if run.opts.DescriptionFrom != "" {
		value, err := textfile.Read(run.opts.DescriptionFrom)
		if err != nil {
			return errors.Fatalf("unable to read description from %q: %v", run.opts.DescriptionFrom, err)
		}
		run.snapshotOpts.Description = strings.TrimSpace(string(value))
	} else {
		run.snapshotOpts.Description = run.opts.Description
	}
	return configureDeleteProtection(run)
}

func configureDeferredUploader(run *backupRun) error {
	uploadOptions, store, err := run.repo.DeferredUploadPlan()
	if err != nil {
		return err
	}
	config := run.repo.Config().StagingQuota
	quota := staging.Quota{
		MaxBytes: config.MaxBytes, MaxJobs: config.MaxJobs,
		MaxAge: time.Duration(config.MaxAgeSeconds) * time.Second,
	}
	usage, err := store.ActiveUsage(run.ctx, run.repo.Config().ID)
	if err != nil {
		return fmt.Errorf("inspect deferred staging quota: %w", err)
	}
	if err := staging.CheckQuota(quota, usage.Jobs, 0, usage.Bytes, usage.OldestJobAt, 0, time.Now().UTC()); err != nil {
		_ = observability.Emit(run.ctx, observability.Event{
			Severity: observability.Error, Category: observability.CategoryLifecycle,
			Component: "backup", Message: "deferred staging quota refused upload",
		})
		return err
	}
	if quota.MaxBytes > 0 {
		uploadOptions.MaxAdditionalBytes = quota.MaxBytes - usage.Bytes
	}
	run.deferredStore = store
	run.hooks.deferredUploader = func(ctx context.Context, fn func(context.Context, vaultic.BlobSaverWithAsync) error) error {
		var err error
		run.deferredResult, err = run.repo.WithDeferredBlobUploader(ctx, uploadOptions, fn)
		return err
	}
	run.hooks.wireDeferredUploader(&run.snapshotOpts)
	return nil
}

func configureDeleteProtection(run *backupRun) error {
	if run.opts.DeleteNever {
		run.snapshotOpts.Delete = &data.DeleteOption{Never: true}
		return nil
	}
	if run.opts.DeleteAfter == "" {
		return nil
	}
	duration, err := data.ParseDuration(run.opts.DeleteAfter)
	if err != nil || duration.Zero() {
		return errors.Fatalf("invalid --delete-after duration %q: %v", run.opts.DeleteAfter, err)
	}
	until := run.timeStamp.AddDate(duration.Years, duration.Months, duration.Days).Add(time.Duration(duration.Hours) * time.Hour)
	run.snapshotOpts.Delete = &data.DeleteOption{After: &until}
	return nil
}

func executeBackup(run *backupRun) error {
	if !run.gopts.JSON {
		run.printer.V("start backup on %v", run.targets)
	}
	var err error
	run.snapshot, run.snapshotID, run.summary, err = run.archiver.Snapshot(run.ctx, run.targets, run.snapshotOpts)
	if err == nil && run.deferredActive {
		err = publishDeferredBackup(run)
	}
	if reconcileErr := publishReconciledBackup(run, err); reconcileErr != nil {
		err = reconcileErr
	}
	run.cancel()
	run.waitErr = run.group.Wait()
	if err != nil {
		return errors.Fatalf("unable to save snapshot: %v", err)
	}
	return nil
}

func publishDeferredBackup(run *backupRun) error {
	observations, err := run.hooks.deferredCapture.Close()
	if err != nil {
		return err
	}
	run.deferredJobID = vaultic.NewRandomID().String()
	snapshotPayload, err := json.Marshal(run.snapshot)
	if err != nil {
		return err
	}
	sourceDigest := sha256.Sum256([]byte(strings.Join(run.targets, "\x00")))
	now := time.Now().UTC()
	header := staging.Header{
		Format: 1, RepositoryID: run.repo.Config().ID,
		JobID: run.deferredJobID, IdempotencyKey: run.deferredJobID,
		CreatedAt: now, ExpiresAt: now.Add(run.opts.DeferredExpiry),
		CapsuleGeneration: 1, RepositoryKeyVersion: 1, ChunkerVersion: "rabin-v1",
		CompressionVersion:     fmt.Sprintf("repository-v%d", run.repo.Config().Version),
		PlacementPolicyVersion: 1, SourceIdentitySHA256: hex.EncodeToString(sourceDigest[:]),
		ConsistencyEvidence: "full-crawl",
	}
	records := append(run.deferredResult.Records, staging.Record{Kind: "prospective-snapshot-v1", Payload: snapshotPayload})
	for _, observation := range observations {
		payload, err := json.Marshal(observation)
		if err != nil {
			return err
		}
		records = append(records, staging.Record{Kind: reconcile.DeferredObservationKind, Payload: payload})
	}
	run.deferredSeal, _, _, err = run.deferredStore.PublishJob(run.ctx, header, run.deferredResult.Packs, records)
	if err == nil {
		_ = observability.Emit(run.ctx, observability.Event{
			Severity: observability.Info, Category: observability.CategoryIntegrity,
			Component: "backup", Message: "deferred pack durability verified",
			Fields: map[string]any{
				"job_id": run.deferredJobID, "pack_count": run.deferredSeal.PackCount,
				"protected_bytes": run.deferredSeal.ProtectedBytes,
			},
		})
		_ = observability.Emit(run.ctx, observability.Event{
			Severity: observability.Notice, Category: observability.CategoryLifecycle,
			Component: "staging", Message: "deferred ingest journal sealed",
			Fields: map[string]any{"job_id": run.deferredJobID, "expires_at": run.deferredSeal.Header.ExpiresAt},
		})
	}
	return err
}

func publishReconciledBackup(run *backupRun, snapshotErr error) error {
	if run.reconciler == nil {
		return snapshotErr
	}
	if err := run.reconciler.Close(); err != nil && snapshotErr == nil {
		return fmt.Errorf("reconcile authoritative snapshot metadata: %w", err)
	}
	if snapshotErr != nil || run.snapshotID.IsNull() {
		return snapshotErr
	}
	rootKey := run.reconciler.RootKey()
	if len(rootKey) == 0 {
		return fmt.Errorf("reconcile authoritative snapshot metadata: missing snapshot root")
	}
	if err := run.authoritativeEngine.PublishSnapshotScope(run.ctx, run.snapshotID, rootKey); err != nil {
		return fmt.Errorf("publish authoritative snapshot scope: %w", err)
	}
	return nil
}

func reportBackup(run *backupRun) error {
	if run.deferredActive {
		return reportDeferredBackup(run)
	}
	run.progress.Finish(run.snapshotID, run.summary, run.opts.DryRun)
	if !run.success {
		return ErrInvalidSourceData
	}
	if run.waitErr != nil {
		return run.waitErr
	}
	runPlacementScheduler(run)
	publishBackupTelemetry(run)
	if run.opts.List {
		if err := runLs(run.ctx, LsOptions{}, run.gopts, []string{run.snapshotID.String()}, run.term); err != nil {
			run.printer.E("listing created snapshot failed: %v\n", err)
		}
	}
	return nil
}

func reportDeferredBackup(run *backupRun) error {
	if run.waitErr != nil {
		return run.waitErr
	}
	if !run.success {
		return ErrInvalidSourceData
	}
	placements := make(map[string]uint64)
	for _, pack := range run.deferredResult.Packs {
		for _, placement := range pack.Placements {
			placements[placement.BackendID]++
		}
	}
	result := map[string]any{
		"state": "data_durable_metadata_pending", "job_id": run.deferredJobID,
		"packs": len(run.deferredResult.Packs), "protected_bytes": run.deferredSeal.ProtectedBytes,
		"placements": placements, "expires_at": run.deferredSeal.Header.ExpiresAt,
		"reason": run.opts.DeferredMode,
	}
	if run.gopts.JSON {
		encoded, _ := json.Marshal(result)
		run.term.Print(string(encoded))
	} else {
		run.printer.P("data durable; metadata pending (job %s, %d packs, expires in %s)\n",
			run.deferredJobID, len(run.deferredResult.Packs), run.opts.DeferredExpiry)
	}
	return nil
}

func runPlacementScheduler(run *backupRun) {
	if run.authoritativeEngine == nil || run.opts.DryRun {
		return
	}
	model, err := indexMaintenancePlacementModel(run.repo)
	if err == nil {
		_, err = maintenance.PlanPlacement(run.ctx, run.authoritativeEngine.SchemaStore(), maintenance.PlacementSchedulerOptions{Model: model, Now: time.Now()})
	}
	if err == nil {
		_, err = maintenance.ExecutePlacement(
			run.ctx, run.authoritativeEngine.SchemaStore(),
			repositoryPlacementActions{repo: run.repo, printer: run.printer},
			maintenance.PlacementWorkerOptions{Model: model, Now: time.Now(), MaxRequests: 1},
		)
	}
	if err != nil {
		run.printer.E("placement scheduler tick failed: %v\n", err)
	}
}

func publishBackupTelemetry(run *backupRun) {
	err := telemetry.Publish(run.ctx, telemetry.Config{
		PrometheusURL: run.gopts.PrometheusURL, PrometheusUser: run.gopts.PrometheusUser,
		PrometheusPass: run.gopts.PrometheusPass, InfluxURL: run.gopts.InfluxURL,
		InfluxToken: run.gopts.InfluxToken, InfluxOrg: run.gopts.InfluxOrg,
		InfluxBucket: run.gopts.InfluxBucket,
	}, telemetry.Backup{
		Repository: run.gopts.Repo, SnapshotID: run.snapshotID.String(),
		Label: run.snapshotOpts.Label, Summary: run.summary,
	})
	if err != nil {
		run.printer.E("telemetry publish failed: %v\n", err)
	}
}
