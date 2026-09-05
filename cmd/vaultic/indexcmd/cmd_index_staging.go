package indexcmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/restorer"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	restoreui "github.com/otuschhoff/vaultic/internal/ui/restore"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
)

func newIndexStagingCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{
		Use:               "staging",
		Short:             "Inspect and control deferred ingest journals",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
	}
	command.AddCommand(
		newIndexStagingStatusCommand(globalOptions),
		newIndexStagingInspectCommand(globalOptions),
		newIndexStagingRestoreCommand(globalOptions),
		newIndexStagingReconcileCommand(globalOptions),
		newIndexStagingExtendCommand(globalOptions),
		newIndexStagingRejectCommand(globalOptions),
		newIndexStagingAbandonCommand(globalOptions),
	)
	return command
}

type stagingExtendOptions struct {
	Extension time.Duration
}

func (options stagingExtendOptions) Finalize() error {
	if options.Extension <= 0 {
		return fmt.Errorf("--by must be positive")
	}
	return nil
}

func newIndexStagingExtendCommand(globalOptions *global.Options) *cobra.Command {
	var options stagingExtendOptions
	command := &cobra.Command{
		Use:               "extend JOB_ID",
		Short:             "Extend a sealed journal expiry within repository policy",
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return options.Finalize()
		},
		RunE: func(command *cobra.Command, args []string) error {
			return withStagingSession(command.Context(), globalOptions, func(ctx context.Context, store repository.Store, repositoryID string) error {
				job, err := findStagingJob(ctx, store, repositoryID, args[0])
				if err != nil {
					return err
				}
				record, err := store.PublishExtension(ctx, job, job.EffectiveExpiresAt().Add(options.Extension))
				if err != nil {
					return err
				}
				observability.EmitBestEffort(
					ctx,
					observability.Event{
						Severity:  observability.Warning,
						Category:  observability.CategoryLifecycle,
						Component: "staging",
						Message:   "deferred ingest journal expiry extended",
					},
				)
				if globalOptions.JSON {
					globalOptions.Term.Print(ui.ToJSONString(record))
				} else {
					globalOptions.Term.Print(fmt.Sprintf("journal %s extended until %s\n", job.Header.JobID, record.ExpiresAt.Format(time.RFC3339)))
				}
				return nil
			})
		},
	}
	command.Flags().DurationVar(&options.Extension, "by", 0, "duration to extend the current journal expiry")
	return command
}

func newIndexStagingRestoreCommand(globalOptions *global.Options) *cobra.Command {
	var target string
	var dryRun, sparse, verify bool
	var overwrite restorer.OverwriteBehavior
	command := &cobra.Command{
		Use:               "restore JOB_ID",
		Short:             "Restore directly from a sealed deferred journal",
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, args []string) error {
			return runIndexStagingRestore(command.Context(), *globalOptions, args[0], target, dryRun, sparse, verify, overwrite)
		},
	}
	flags := command.Flags()
	flags.StringVarP(&target, "target", "t", "", "directory to extract data to")
	flags.BoolVar(&dryRun, "dry-run", false, "verify and read data without writing files")
	flags.BoolVar(&sparse, "sparse", false, "restore files as sparse")
	flags.BoolVar(&verify, "verify", false, "verify restored file content")
	flags.Var(&overwrite, "overwrite", "overwrite behavior, one of (always|if-changed|if-newer|never)")
	return command
}

func runIndexStagingRestore(
	ctx context.Context,
	globalOptions global.Options,
	jobID, target string,
	dryRun, sparse, verify bool,
	overwrite restorer.OverwriteBehavior,
) error {
	if target == "" {
		return fmt.Errorf("please specify a directory to restore to (--target)")
	}
	if dryRun && verify {
		return fmt.Errorf("--dry-run and --verify are mutually exclusive")
	}
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	repo, err := global.OpenDataPlaneRepository(ctx, globalOptions, printer)
	if err != nil {
		return err
	}
	defer func() { _ = repo.Close() }() // Read-only staging inspection has no buffered repository writes.
	store, err := newStagingStore(repo)
	if err != nil {
		return err
	}
	job, err := findStagingJob(ctx, store, repo.Config().ID, jobID)
	if err != nil {
		return err
	}
	segments, err := store.VerifyJob(ctx, job)
	if err != nil {
		return err
	}
	if err := verifyStagingPacks(ctx, store, segments); err != nil {
		return err
	}
	if err := repo.UseDeferredJournalIndex(segments); err != nil {
		return err
	}
	snapshot, err := stagingSnapshot(segments)
	if err != nil {
		return err
	}
	return restoreStagingSnapshot(ctx, globalOptions, repo, job, snapshot, target, dryRun, sparse, verify, overwrite)
}

func verifyStagingPacks(ctx context.Context, store repository.Store, segments []repository.Segment) error {
	verifier := repository.BackendPackVerifier{Backends: store.Mirrors, Policy: store.Policy}
	for _, segment := range segments {
		for _, pack := range segment.Packs {
			if err := verifier.VerifyPack(ctx, pack); err != nil {
				return fmt.Errorf("verify emergency restore pack: %w", err)
			}
		}
	}
	return nil
}

func stagingSnapshot(segments []repository.Segment) (*data.Snapshot, error) {
	var snapshot *data.Snapshot
	for _, segment := range segments {
		for _, record := range segment.Records {
			if record.Kind != "prospective-snapshot-v1" {
				continue
			}
			if snapshot != nil {
				return nil, fmt.Errorf("journal contains multiple prospective snapshots")
			}
			var candidate data.Snapshot
			if err := json.Unmarshal(record.Payload, &candidate); err != nil {
				return nil, fmt.Errorf("decode prospective snapshot: %w", err)
			}
			snapshot = &candidate
		}
	}
	if snapshot == nil || snapshot.Tree == nil {
		return nil, fmt.Errorf("journal has no prospective snapshot root")
	}
	return snapshot, nil
}

func restoreStagingSnapshot(
	ctx context.Context,
	globalOptions global.Options,
	repo *repository.Repository,
	job repository.Job,
	snapshot *data.Snapshot,
	target string,
	dryRun, sparse, verify bool,
	overwrite restorer.OverwriteBehavior,
) error {
	var restorePrinter restoreui.ProgressPrinter
	if globalOptions.JSON {
		restorePrinter = restoreui.NewJSONProgress(globalOptions.Term, globalOptions.Verbosity)
	} else {
		restorePrinter = restoreui.NewTextProgress(globalOptions.Term, globalOptions.Verbosity)
	}
	restoreProgress := restoreui.NewProgress(restorePrinter, globalOptions.Quiet, globalOptions.JSON, globalOptions.Term.CanUpdateStatus())
	restore := restorer.NewRestorer(repo, snapshot, restorer.Options{DryRun: dryRun, Sparse: sparse, Progress: restoreProgress, Overwrite: overwrite})
	restore.Error = func(location string, err error) error { return restoreProgress.Error(location, err) }
	restore.Warn = func(message string) { restorePrinter.E("Warning: %s\n", message) }
	restore.Info = func(message string) { restorePrinter.P("Info: %s\n", message) }
	count, err := restore.RestoreTo(ctx, filepath.Clean(target))
	if err != nil {
		return err
	}
	restoreProgress.Finish()
	if verify {
		bar := restorePrinter.NewCounterTerminalOnly("files verified")
		if _, err := restore.VerifyFiles(ctx, target, count, bar); err != nil {
			return err
		}
	}
	observability.EmitBestEffort(ctx, observability.Event{
		Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "staging",
		Message: "emergency deferred journal restore completed", Fields: map[string]any{"job_id": job.Header.JobID, "dry_run": dryRun},
	})
	return nil
}

type stagingRejectOptions struct {
	Reason string
}

func (options stagingRejectOptions) Finalize() error {
	if options.Reason == "" {
		return fmt.Errorf("rejection requires --reason")
	}
	return nil
}

func newIndexStagingRejectCommand(globalOptions *global.Options) *cobra.Command {
	var options stagingRejectOptions
	command := &cobra.Command{
		Use:               "reject JOB_ID",
		Short:             "Reject a journal that cannot be reconciled",
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return options.Finalize()
		},
		RunE: func(command *cobra.Command, args []string) error {
			return withStagingSession(command.Context(), globalOptions, func(ctx context.Context, store repository.Store, repositoryID string) error {
				job, err := findStagingJob(ctx, store, repositoryID, args[0])
				if err != nil {
					return err
				}
				rejection, err := store.PublishRejection(ctx, job, options.Reason)
				if err != nil {
					return err
				}
				observability.EmitBestEffort(
					ctx,
					observability.Event{
						Severity:  observability.Warning,
						Category:  observability.CategoryIntegrity,
						Component: "staging",
						Message:   "deferred ingest journal rejected",
					},
				)
				if globalOptions.JSON {
					globalOptions.Term.Print(ui.ToJSONString(rejection))
				} else {
					globalOptions.Term.Print(fmt.Sprintf("journal %s rejected: %s\n", job.Header.JobID, options.Reason))
				}
				return nil
			})
		},
	}
	command.Flags().StringVar(&options.Reason, "reason", "", "auditable reason the journal cannot be reconciled")
	return command
}

func newIndexStagingReconcileCommand(globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{
		Use:               "reconcile JOB_ID",
		Short:             "Commit one verified deferred ingest journal",
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, args []string) error {
			return runIndexStagingReconcile(command.Context(), globalOptions, args[0])
		},
	}
}

func runIndexStagingReconcile(ctx context.Context, globalOptions *global.Options, jobID string) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	ctx, repo, unlock, err := openWithAppendLock(ctx, *globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	store, err := newStagingStore(repo)
	if err != nil {
		return err
	}
	job, err := findStagingJob(ctx, store, repo.Config().ID, jobID)
	if err != nil {
		return err
	}
	engine, ok := repo.Engine().(*enginepkg.DaemonEngine)
	if !ok {
		return fmt.Errorf("staging reconciliation requires authoritative VaulticDB metadata")
	}
	authority := stagingAuthority(repo, engine)
	result := repository.Reconcile(ctx, store, authority, repository.BackendPackVerifier{Backends: store.Mirrors, Policy: store.Policy}, job)
	return handleStagingReconcileResult(ctx, globalOptions, engine, result)
}

func stagingAuthority(repo *repository.Repository, engine *enginepkg.DaemonEngine) *repository.DaemonAuthority {
	return &repository.DaemonAuthority{
		Client: engine.Client(), Store: engine.SchemaStore(),
		Preflight: func(ctx context.Context, header repository.Header) error {
			if header.RepositoryID != repo.Config().ID {
				return fmt.Errorf("journal repository identity mismatch")
			}
			_, err := engine.SchemaStore().MetadataHead(ctx)
			return err
		},
		SnapshotPublisher: func(ctx context.Context, expected string, snapshotJSON []byte) error {
			id, err := repo.SaveUnpacked(ctx, vaultic.WriteableSnapshotFile, snapshotJSON)
			if err != nil {
				return err
			}
			if id.String() != expected {
				return fmt.Errorf("published snapshot ID %s does not match journal %s", id, expected)
			}
			return nil
		},
		ReplayObservations: func(ctx context.Context, payloads []json.RawMessage) ([]byte, error) {
			observations, err := reconcile.DecodeDeferredObservations(payloads)
			if err != nil {
				return nil, err
			}
			return reconcile.ReplayDeferred(ctx, engine.SchemaStore(), observations, reconcile.Options{PathIndexPaths: repo.Config().PathIndexPaths})
		},
	}
}

func handleStagingReconcileResult(
	ctx context.Context,
	globalOptions *global.Options,
	engine *enginepkg.DaemonEngine,
	result repository.ReconcileResult,
) error {
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(result))
	} else {
		globalOptions.Term.Print(fmt.Sprintf("journal %s: %s; snapshot %s\n", result.JobID, result.Disposition, result.SnapshotID))
	}
	severity, category, err := stagingReconcileSeverity(ctx, engine, result)
	if err != nil {
		return err
	}
	observability.EmitBestEffort(ctx, observability.Event{
		Severity: severity, Category: category, Component: "staging", Message: "deferred ingest reconciliation finished",
		Fields: map[string]any{"job_id": result.JobID, "disposition": result.Disposition},
	})
	if result.Disposition != repository.ReconcileCommitted {
		return fmt.Errorf("journal reconciliation %s: %s", result.Disposition, result.Reason)
	}
	return nil
}

func stagingReconcileSeverity(
	ctx context.Context,
	engine *enginepkg.DaemonEngine,
	result repository.ReconcileResult,
) (observability.Severity, observability.Category, error) {
	switch result.Disposition {
	case repository.ReconcileCommitted:
		return observability.Info, observability.CategoryLifecycle, nil
	case repository.ReconcileRejected:
		return observability.Warning, observability.CategoryIntegrity, nil
	case repository.ReconcileRetryable:
		return observability.Warning, observability.CategoryLifecycle, nil
	case repository.ReconcileHealingRequired:
		diagnostic := sha256.Sum256([]byte(result.JobID + "\x00" + result.Reason))
		status, err := engine.Client().GenerationStatus(ctx)
		if err != nil {
			return 0, "", fmt.Errorf("query generation for healing quarantine: %w", err)
		}
		if _, err := engine.Client().QuarantineGeneration(ctx, status.ActiveGeneration, hex.EncodeToString(diagnostic[:])); err != nil {
			return 0, "", fmt.Errorf("quarantine healing-required generation: %w", err)
		}
		return observability.Critical, observability.CategoryIntegrity, nil
	}
	return observability.Warning, observability.CategoryIntegrity, fmt.Errorf("unknown reconciliation disposition %q", result.Disposition)
}

func newIndexStagingStatusCommand(globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{
		Use:               "status",
		Short:             "List authenticated deferred ingest journals",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			return withStagingSession(command.Context(), globalOptions, func(ctx context.Context, store repository.Store, repositoryID string) error {
				jobs, err := store.Discover(ctx, repositoryID)
				if err != nil {
					return err
				}
				printStagingJobs(globalOptions, jobs)
				return nil
			})
		},
	}
}

func newIndexStagingInspectCommand(globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{
		Use:               "inspect JOB_ID",
		Short:             "Verify and inspect one deferred ingest journal",
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, args []string) error {
			return withStagingSession(command.Context(), globalOptions, func(ctx context.Context, store repository.Store, repositoryID string) error {
				job, err := findStagingJob(ctx, store, repositoryID, args[0])
				if err != nil {
					return err
				}
				segments, err := store.VerifyJob(ctx, job)
				if err != nil {
					return err
				}
				result := struct {
					Job      repository.Job       `json:"job"`
					Segments []repository.Segment `json:"segments"`
				}{Job: job, Segments: segments}
				if globalOptions.JSON {
					globalOptions.Term.Print(ui.ToJSONString(result))
				} else {
					globalOptions.Term.Print(fmt.Sprintf(
						"journal %s: %s; %d segments; %d packs; %d protected bytes\n",
						job.Header.JobID,
						job.State,
						len(segments),
						job.Seal.PackCount,
						job.Seal.ProtectedBytes,
					))
				}
				return nil
			})
		},
	}
}

type stagingAbandonOptions struct {
	Reason      string
	Acknowledge bool
	SafetyDelay time.Duration
}

func (options stagingAbandonOptions) Finalize() error {
	if options.Reason == "" || !options.Acknowledge {
		return fmt.Errorf("abandonment requires --reason and --acknowledge-data-loss")
	}
	if options.SafetyDelay <= 0 {
		return fmt.Errorf("--safety-delay must be positive")
	}
	return nil
}

func newIndexStagingAbandonCommand(globalOptions *global.Options) *cobra.Command {
	options := stagingAbandonOptions{SafetyDelay: 24 * time.Hour}
	command := &cobra.Command{
		Use:               "abandon JOB_ID",
		Short:             "Publish an acknowledged journal abandonment",
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return options.Finalize()
		},
		RunE: func(command *cobra.Command, args []string) error {
			return withStagingSession(command.Context(), globalOptions, func(ctx context.Context, store repository.Store, repositoryID string) error {
				job, err := findStagingJob(ctx, store, repositoryID, args[0])
				if err != nil {
					return err
				}
				store.AbandonmentSafetyDelay = options.SafetyDelay
				abandonment, err := store.PublishAbandonment(ctx, job, options.Reason, "operator acknowledged staged data loss")
				if err != nil {
					return err
				}
				observability.EmitBestEffort(
					ctx,
					observability.Event{
						Severity:  observability.Critical,
						Category:  observability.CategoryLifecycle,
						Component: "staging",
						Message:   "sealed ingest journal abandoned",
					},
				)
				if globalOptions.JSON {
					globalOptions.Term.Print(ui.ToJSONString(abandonment))
				} else {
					globalOptions.Term.Print(fmt.Sprintf(
						"journal %s abandoned; packs remain protected until %s\n",
						job.Header.JobID,
						abandonment.DeleteAfter.Format(time.RFC3339),
					))
				}
				return nil
			})
		},
	}
	command.Flags().StringVar(&options.Reason, "reason", "", "auditable reason for abandoning staged data")
	command.Flags().BoolVar(&options.Acknowledge, "acknowledge-data-loss", false, "acknowledge that the staged backup may become unrecoverable")
	command.Flags().DurationVar(&options.SafetyDelay, "safety-delay", 24*time.Hour, "minimum delay before abandoned packs lose GC protection")
	return command
}

func findStagingJob(ctx context.Context, store repository.Store, repositoryID, jobID string) (repository.Job, error) {
	jobs, err := store.Discover(ctx, repositoryID)
	if err != nil {
		return repository.Job{}, err
	}
	for _, job := range jobs {
		if job.Header.JobID == jobID {
			return job, nil
		}
	}
	return repository.Job{}, fmt.Errorf("staging journal %q was not found", jobID)
}

func printStagingJobs(globalOptions *global.Options, jobs []repository.Job) {
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(jobs))
		return
	}
	for _, job := range jobs {
		globalOptions.Term.Print(
			fmt.Sprintf("%s\t%s\t%d bytes\texpires %s\n", job.Header.JobID, job.State, job.Seal.ProtectedBytes, job.EffectiveExpiresAt().Format(time.RFC3339)),
		)
	}
}
