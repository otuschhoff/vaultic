package backupcmd

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

	"github.com/otuschhoff/vaultic/cmd/vaultic/indexcmd"
	"github.com/otuschhoff/vaultic/cmd/vaultic/querycmd"
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
	ctx           context.Context
	options       backupOptions
	globalOptions global.Options
	term          ui.Terminal
	args          []string
	printer       backup.ProgressPrinter
	progress      *backup.Progress
	success       bool
	targets       []string
	timeStamp     time.Time
	start         time.Time
	vssConfig     fs.VSSConfig

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

func (hooks *backupHooks) wireDeferredUploader(options *archiver.SnapshotOptions) {
	if hooks.deferredUploader != nil {
		options.DeferredUploader = hooks.deferredUploader
	}
}

func runBackupPipeline(ctx context.Context, options backupOptions, globalOptions global.Options, term ui.Terminal, args []string) error {
	run := &backupRun{ctx: ctx, options: options, globalOptions: globalOptions, term: term, args: args, success: true}
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
	if run.globalOptions.JSON {
		run.printer = backup.NewJSONProgress(run.term, run.globalOptions.Verbosity)
	} else {
		run.printer = backup.NewTextProgress(run.term, run.globalOptions.Verbosity)
	}
	if runtime.GOOS == "windows" {
		config, err := fs.ParseVSSConfig(run.globalOptions.Extended)
		if err != nil {
			return err
		}
		run.vssConfig = config
	}
	if err := run.options.Check(run.globalOptions, run.args); err != nil {
		return err
	}
	targets, err := collectTargets(run.options, run.args, run.printer.E, run.term.InputRaw())
	if err != nil && !errors.Is(err, ErrInvalidSourceData) {
		return err
	}
	if err != nil {
		run.success = false
	}
	run.targets = targets
	run.timeStamp = time.Now()
	run.start = run.timeStamp
	if run.options.TimeStamp != "" {
		run.timeStamp, err = time.ParseInLocation(global.TimeFormat, run.options.TimeStamp, time.Local)
		if err != nil {
			return errors.Fatalf("error in time option: %v", err)
		}
	}
	return initializeBackupRepository(run)
}

func initializeBackupRepository(run *backupRun) error {
	if run.globalOptions.Verbosity >= 2 && !run.globalOptions.JSON {
		run.printer.P("open repository")
	}
	if !run.options.Init {
		return nil
	}
	_, err := global.OpenRepository(run.ctx, run.globalOptions, run.printer)
	if errors.Is(err, global.ErrNoRepository) {
		_, err = global.CreateRepository(run.ctx, run.globalOptions, vaultic.StableRepoVersion, nil, run.printer)
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
	run.progress = backup.NewProgress(run.printer, run.globalOptions.Quiet, run.globalOptions.JSON, run.term.CanUpdateStatus())
	if err := loadBackupParent(run); err != nil {
		return ctx, err
	}
	return ctx, openBackupFilesystem(run)
}

func openBackupRepository(ctx context.Context, run *backupRun) (context.Context, error) {
	run.deferredActive = run.options.AllowDeferredCommit && run.options.DeferredMode != "auto"
	run.metadataBypassed = run.options.DeferredMode == "data-plane-only"
	var err error
	if run.metadataBypassed {
		run.repo, err = global.OpenDataPlaneRepository(ctx, run.globalOptions, run.printer)
		if err == nil {
			run.closeRepo = func() { errors.CloseQuietly(run.repo) }
		}
	} else {
		ctx, run.repo, run.closeRepo, err = openWithAppendLock(ctx, run.globalOptions, run.options.DryRun, run.printer)
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
	if shouldUseDataPlaneFallback(openErr, run.options) {
		repo, err := global.OpenDataPlaneRepository(run.ctx, run.globalOptions, run.printer)
		if err != nil {
			return err
		}
		run.repo = repo
		run.deferredActive = true
		run.metadataBypassed = true
		run.closeRepo = func() { errors.CloseQuietly(repo) }
		return nil
	}
	if openErr != nil || run.options.DeferredMode != "auto" {
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
	observability.EmitBestEffort(run.ctx, observability.Event{
		Severity: severity, Category: observability.CategoryLifecycle,
		Component: "backup", Message: message, Fields: map[string]any{"mode": run.options.DeferredMode},
	})
}

func loadBackupParent(run *backupRun) error {
	if !run.options.Stdin && !run.deferredActive {
		parent, err := findParentSnapshot(run.ctx, run.repo, run.options, run.targets, run.timeStamp)
		if err != nil {
			return err
		}
		run.parent = parent
		if !run.globalOptions.JSON && parent != nil {
			run.printer.P("using parent snapshot %v\n", parent.ID().Str())
		} else if !run.globalOptions.JSON {
			run.printer.P("no parent snapshot found, will read all files\n")
		}
	}
	if run.deferredActive {
		return nil
	}
	if !run.globalOptions.JSON {
		run.printer.V("load index files")
	}
	return run.repo.LoadIndex(run.ctx, run.printer)
}

func openBackupFilesystem(run *backupRun) error {
	run.targetFS = fs.NewLocal()
	if runtime.GOOS == "windows" && run.options.UseFsSnapshot {
		if err := fs.HasSufficientPrivilegesForVSS(); err != nil {
			return err
		}
		errorHandler := func(item string, err error) {
			_ = run.progress.Error(item, err) // Progress rendering cannot supersede the item error it is reporting.
		}
		messageHandler := func(msg string, args ...any) {
			if !run.globalOptions.JSON {
				run.printer.P(msg, args...)
			}
		}
		localVSS := fs.NewLocalVss(errorHandler, messageHandler, run.vssConfig)
		run.targetFS = localVSS
		run.closeSource = localVSS.DeleteSnapshots
	}
	if run.options.Stdin || run.options.StdinCommand {
		return openBackupStdin(run)
	}
	if run.options.FSTestHook != nil {
		run.targetFS = run.options.FSTestHook(run.targetFS)
	}
	return nil
}

func openBackupStdin(run *backupRun) error {
	if !run.globalOptions.JSON {
		run.printer.V("read data from stdin")
	}
	filename := path.Join("/", run.options.StdinFilename)
	source := run.term.InputRaw()
	var err error
	if run.options.StdinCommand {
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
	if run.options.FSTestHook != nil {
		run.targetFS = run.options.FSTestHook(run.targetFS)
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
	options := archiver.Options{ReadConcurrency: run.options.ReadConcurrency}
	if run.options.UseCWalk && (!run.pathdiffPlan.Selective || len(run.pathdiffPlan.ChangedDirs) > 0) {
		options.CWalkConcurrency, options.CWalkQueue = run.options.CWalkConcurrency, 4096
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
	run.archiver.WithAtime = run.options.WithAtime
	if err := configureBackupHooks(cancelCtx, run); err != nil {
		return err
	}
	configureBackupChangeFlags(run)
	return configureSnapshotOptions(run)
}

func configurePathdiff(run *backupRun) error {
	run.pathdiffPlan = crawl.Plan{Reason: "pathdiff is disabled"}
	if !run.options.UsePathdiff {
		return nil
	}
	if !fs.IsLocal(run.targetFS) {
		run.pathdiffPlan.Reason = "source does not use the plain local filesystem"
	} else if run.parent == nil {
		run.pathdiffPlan.Reason = "no parent snapshot is available"
	} else if topology, err := crawl.LoadTopology(run.options.PathdiffSVMMap); err != nil {
		run.pathdiffPlan.Reason = err.Error()
	} else {
		service := crawl.NewPathdiffService(uppathdiff.NewClient(run.options.PathdiffEndpoint))
		plan, err := crawl.BuildPathdiffPlan(run.ctx, service, topology, run.targets, run.parent.Time, run.start)
		if err != nil {
			return fmt.Errorf("build pathdiff crawl plan: %w", err)
		}
		run.pathdiffPlan = plan
	}
	if !run.pathdiffPlan.Selective && run.options.PathdiffRequireCoverage {
		return errors.Fatalf("pathdiff coverage is required: %s", run.pathdiffPlan.Reason)
	}
	if run.globalOptions.JSON {
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
	byName, err := collectRejectByNameFuncs(run.options, run.repo, run.printer.E)
	if err != nil {
		return nil, nil, nil, err
	}
	rejects, err := collectRejectFuncs(run.options, run.targets, run.targetFS, run.printer.E)
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
	if run.options.NoScan || run.pathdiffPlan.Selective || run.options.UseCWalk {
		return
	}
	scanner := archiver.NewScanner(run.targetFS)
	scanner.SelectByName, scanner.Select = byName, selectItem
	scanner.Error, scanner.Result = run.printer.ScannerError, run.progress.ReportTotal
	if !run.globalOptions.JSON {
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
	if run.options.IgnoreInode {
		run.archiver.ChangeIgnoreFlags |= archiver.ChangeIgnoreCtime | archiver.ChangeIgnoreInode
	}
	if run.options.IgnoreCtime {
		run.archiver.ChangeIgnoreFlags |= archiver.ChangeIgnoreCtime
	}
}

func configureSnapshotOptions(run *backupRun) error {
	run.snapshotOpts = archiver.SnapshotOptions{
		Excludes: run.options.Excludes, Tags: run.options.Tags.Flatten(),
		BackupStart: run.start, Time: run.timeStamp, Hostname: run.options.Host,
		Label: run.options.Label, ParentSnapshot: run.parent,
		ProgramVersion: "vaultic " + global.Version, SkipIfUnchanged: run.options.SkipIfUnchanged,
	}
	if run.deferredActive {
		if err := configureDeferredUploader(run); err != nil {
			return err
		}
	}
	if run.options.DescriptionFrom != "" {
		value, err := textfile.Read(run.options.DescriptionFrom)
		if err != nil {
			return errors.Fatalf("unable to read description from %q: %v", run.options.DescriptionFrom, err)
		}
		run.snapshotOpts.Description = strings.TrimSpace(string(value))
	} else {
		run.snapshotOpts.Description = run.options.Description
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
		observability.EmitBestEffort(run.ctx, observability.Event{
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
	if run.options.DeleteNever {
		run.snapshotOpts.Delete = &data.DeleteOption{Never: true}
		return nil
	}
	if run.options.DeleteAfter == "" {
		return nil
	}
	duration, err := data.ParseDuration(run.options.DeleteAfter)
	if err != nil || duration.Zero() {
		return errors.Fatalf("invalid --delete-after duration %q: %v", run.options.DeleteAfter, err)
	}
	until := run.timeStamp.AddDate(duration.Years, duration.Months, duration.Days).Add(time.Duration(duration.Hours) * time.Hour)
	run.snapshotOpts.Delete = &data.DeleteOption{After: &until}
	return nil
}

func executeBackup(run *backupRun) error {
	if !run.globalOptions.JSON {
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
		CreatedAt: now, ExpiresAt: now.Add(run.options.DeferredExpiry),
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
		observability.EmitBestEffort(run.ctx, observability.Event{
			Severity: observability.Info, Category: observability.CategoryIntegrity,
			Component: "backup", Message: "deferred pack durability verified",
			Fields: map[string]any{
				"job_id": run.deferredJobID, "pack_count": run.deferredSeal.PackCount,
				"protected_bytes": run.deferredSeal.ProtectedBytes,
			},
		})
		observability.EmitBestEffort(run.ctx, observability.Event{
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
	run.progress.Finish(run.snapshotID, run.summary, run.options.DryRun)
	if !run.success {
		return ErrInvalidSourceData
	}
	if run.waitErr != nil {
		return run.waitErr
	}
	runPlacementScheduler(run)
	publishBackupTelemetry(run)
	if run.options.List {
		if err := querycmd.RunLsDefault(run.ctx, run.globalOptions, []string{run.snapshotID.String()}, run.term); err != nil {
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
		"reason": run.options.DeferredMode,
	}
	if run.globalOptions.JSON {
		encoded, _ := json.Marshal(result)
		run.term.Print(string(encoded))
	} else {
		run.printer.P("data durable; metadata pending (job %s, %d packs, expires in %s)\n",
			run.deferredJobID, len(run.deferredResult.Packs), run.options.DeferredExpiry)
	}
	return nil
}

func runPlacementScheduler(run *backupRun) {
	if run.authoritativeEngine == nil || run.options.DryRun {
		return
	}
	model, err := indexcmd.MaintenancePlacementModel(run.repo)
	if err == nil {
		_, err = maintenance.PlanPlacement(run.ctx, run.authoritativeEngine.SchemaStore(), maintenance.PlacementSchedulerOptions{Model: model, Now: time.Now()})
	}
	if err == nil {
		_, err = maintenance.ExecutePlacement(
			run.ctx, run.authoritativeEngine.SchemaStore(),
			indexcmd.PlacementActions{Repository: run.repo, Printer: run.printer},
			maintenance.PlacementWorkerOptions{Model: model, Now: time.Now(), MaxRequests: 1},
		)
	}
	if err != nil {
		run.printer.E("placement scheduler tick failed: %v\n", err)
	}
}

func publishBackupTelemetry(run *backupRun) {
	err := telemetry.Publish(run.ctx, telemetry.Config{
		PrometheusURL: run.globalOptions.PrometheusURL, PrometheusUser: run.globalOptions.PrometheusUser,
		PrometheusPass: run.globalOptions.PrometheusPass, InfluxURL: run.globalOptions.InfluxURL,
		InfluxToken: run.globalOptions.InfluxToken, InfluxOrg: run.globalOptions.InfluxOrg,
		InfluxBucket: run.globalOptions.InfluxBucket,
	}, telemetry.Backup{
		Repository: run.globalOptions.Repo, SnapshotID: run.snapshotID.String(),
		Label: run.snapshotOpts.Label, Summary: run.summary,
	})
	if err != nil {
		run.printer.E("telemetry publish failed: %v\n", err)
	}
}
