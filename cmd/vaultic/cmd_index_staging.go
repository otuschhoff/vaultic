package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	"github.com/otuschhoff/vaultic/internal/restorer"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	restoreui "github.com/otuschhoff/vaultic/internal/ui/restore"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
)

func newIndexStagingCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "staging", Short: "Inspect and control deferred ingest journals", Args: cobra.NoArgs, DisableAutoGenTag: true}
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

func newIndexStagingExtendCommand(globalOptions *global.Options) *cobra.Command {
	var extension time.Duration
	command := &cobra.Command{Use: "extend JOB_ID", Short: "Extend a sealed journal expiry within repository policy", Args: cobra.ExactArgs(1), DisableAutoGenTag: true, RunE: func(command *cobra.Command, args []string) error {
		if extension <= 0 {
			return fmt.Errorf("--by must be positive")
		}
		return withStagingStore(command.Context(), globalOptions, func(ctx context.Context, store staging.Store, repositoryID string) error {
			job, err := findStagingJob(ctx, store, repositoryID, args[0])
			if err != nil {
				return err
			}
			record, err := store.PublishExtension(ctx, job, job.EffectiveExpiresAt().Add(extension))
			if err != nil {
				return err
			}
			_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "staging", Message: "deferred ingest journal expiry extended"})
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(record))
			} else {
				globalOptions.Term.Print(fmt.Sprintf("journal %s extended until %s\n", job.Header.JobID, record.ExpiresAt.Format(time.RFC3339)))
			}
			return nil
		})
	}}
	command.Flags().DurationVar(&extension, "by", 0, "duration to extend the current journal expiry")
	return command
}

func newIndexStagingRestoreCommand(globalOptions *global.Options) *cobra.Command {
	var target string
	var dryRun, sparse, verify bool
	var overwrite restorer.OverwriteBehavior
	command := &cobra.Command{Use: "restore JOB_ID", Short: "Restore directly from a sealed deferred journal", Args: cobra.ExactArgs(1), DisableAutoGenTag: true, RunE: func(command *cobra.Command, args []string) error {
		if target == "" {
			return fmt.Errorf("please specify a directory to restore to (--target)")
		}
		if dryRun && verify {
			return fmt.Errorf("--dry-run and --verify are mutually exclusive")
		}
		printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
		repo, err := global.OpenDataPlaneRepository(command.Context(), *globalOptions, printer)
		if err != nil {
			return err
		}
		defer repo.Close()
		store, err := stagingStore(repo)
		if err != nil {
			return err
		}
		job, err := findStagingJob(command.Context(), store, repo.Config().ID, args[0])
		if err != nil {
			return err
		}
		segments, err := store.VerifyJob(command.Context(), job)
		if err != nil {
			return err
		}
		verifier := staging.BackendPackVerifier{Backends: store.Mirrors, Policy: store.Policy}
		for _, segment := range segments {
			for _, pack := range segment.Packs {
				if err := verifier.VerifyPack(command.Context(), pack); err != nil {
					return fmt.Errorf("verify emergency restore pack: %w", err)
				}
			}
		}
		if err := repo.UseDeferredJournalIndex(segments); err != nil {
			return err
		}
		var snapshot *data.Snapshot
		for _, segment := range segments {
			for _, record := range segment.Records {
				if record.Kind != "prospective-snapshot-v1" {
					continue
				}
				if snapshot != nil {
					return fmt.Errorf("journal contains multiple prospective snapshots")
				}
				var candidate data.Snapshot
				if err := json.Unmarshal(record.Payload, &candidate); err != nil {
					return fmt.Errorf("decode prospective snapshot: %w", err)
				}
				snapshot = &candidate
			}
		}
		if snapshot == nil || snapshot.Tree == nil {
			return fmt.Errorf("journal has no prospective snapshot root")
		}
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
		count, err := restore.RestoreTo(command.Context(), filepath.Clean(target))
		if err != nil {
			return err
		}
		restoreProgress.Finish()
		if verify {
			bar := restorePrinter.NewCounterTerminalOnly("files verified")
			if _, err := restore.VerifyFiles(command.Context(), target, count, bar); err != nil {
				return err
			}
		}
		_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "staging", Message: "emergency deferred journal restore completed", Fields: map[string]any{"job_id": job.Header.JobID, "dry_run": dryRun}})
		return nil
	}}
	flags := command.Flags()
	flags.StringVarP(&target, "target", "t", "", "directory to extract data to")
	flags.BoolVar(&dryRun, "dry-run", false, "verify and read data without writing files")
	flags.BoolVar(&sparse, "sparse", false, "restore files as sparse")
	flags.BoolVar(&verify, "verify", false, "verify restored file content")
	flags.Var(&overwrite, "overwrite", "overwrite behavior, one of (always|if-changed|if-newer|never)")
	return command
}

func newIndexStagingRejectCommand(globalOptions *global.Options) *cobra.Command {
	var reason string
	command := &cobra.Command{Use: "reject JOB_ID", Short: "Reject a journal that cannot be reconciled", Args: cobra.ExactArgs(1), DisableAutoGenTag: true, RunE: func(command *cobra.Command, args []string) error {
		if reason == "" {
			return fmt.Errorf("rejection requires --reason")
		}
		return withStagingStore(command.Context(), globalOptions, func(ctx context.Context, store staging.Store, repositoryID string) error {
			job, err := findStagingJob(ctx, store, repositoryID, args[0])
			if err != nil {
				return err
			}
			rejection, err := store.PublishRejection(ctx, job, reason)
			if err != nil {
				return err
			}
			_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryIntegrity, Component: "staging", Message: "deferred ingest journal rejected"})
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(rejection))
			} else {
				globalOptions.Term.Print(fmt.Sprintf("journal %s rejected: %s\n", job.Header.JobID, reason))
			}
			return nil
		})
	}}
	command.Flags().StringVar(&reason, "reason", "", "auditable reason the journal cannot be reconciled")
	return command
}

func newIndexStagingReconcileCommand(globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{Use: "reconcile JOB_ID", Short: "Commit one verified deferred ingest journal", Args: cobra.ExactArgs(1), DisableAutoGenTag: true, RunE: func(command *cobra.Command, args []string) error {
		printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
		ctx, repo, unlock, err := openWithAppendLock(command.Context(), *globalOptions, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		store, err := stagingStore(repo)
		if err != nil {
			return err
		}
		job, err := findStagingJob(ctx, store, repo.Config().ID, args[0])
		if err != nil {
			return err
		}
		engine, ok := repo.Engine().(*enginepkg.DaemonEngine)
		if !ok {
			return fmt.Errorf("staging reconciliation requires authoritative VaulticDB metadata")
		}
		authority := &staging.DaemonAuthority{
			Client: engine.Client(), Store: engine.SchemaStore(),
			Preflight: func(ctx context.Context, header staging.Header) error {
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
		result := staging.Reconcile(ctx, store, authority, staging.BackendPackVerifier{Backends: store.Mirrors, Policy: store.Policy}, job)
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(result))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("journal %s: %s; snapshot %s\n", result.JobID, result.Disposition, result.SnapshotID))
		}
		severity := observability.Info
		category := observability.CategoryLifecycle
		if result.Disposition == staging.ReconcileRejected {
			severity, category = observability.Warning, observability.CategoryIntegrity
		} else if result.Disposition == staging.ReconcileHealingRequired {
			severity, category = observability.Critical, observability.CategoryIntegrity
			diagnostic := sha256.Sum256([]byte(result.JobID + "\x00" + result.Reason))
			status, statusErr := engine.Client().GenerationStatus(ctx)
			if statusErr != nil {
				return fmt.Errorf("query generation for healing quarantine: %w", statusErr)
			}
			if _, quarantineErr := engine.Client().QuarantineGeneration(ctx, status.ActiveGeneration, hex.EncodeToString(diagnostic[:])); quarantineErr != nil {
				return fmt.Errorf("quarantine healing-required generation: %w", quarantineErr)
			}
		} else if result.Disposition == staging.ReconcileRetryable {
			severity = observability.Warning
		}
		_ = observability.Emit(ctx, observability.Event{Severity: severity, Category: category, Component: "staging", Message: "deferred ingest reconciliation finished", Fields: map[string]any{"job_id": result.JobID, "disposition": result.Disposition}})
		if result.Disposition != staging.ReconcileCommitted {
			return fmt.Errorf("journal reconciliation %s: %s", result.Disposition, result.Reason)
		}
		return nil
	}}
}

func newIndexStagingStatusCommand(globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{Use: "status", Short: "List authenticated deferred ingest journals", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		return withStagingStore(command.Context(), globalOptions, func(ctx context.Context, store staging.Store, repositoryID string) error {
			jobs, err := store.Discover(ctx, repositoryID)
			if err != nil {
				return err
			}
			printStagingJobs(globalOptions, jobs)
			return nil
		})
	}}
}

func newIndexStagingInspectCommand(globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{Use: "inspect JOB_ID", Short: "Verify and inspect one deferred ingest journal", Args: cobra.ExactArgs(1), DisableAutoGenTag: true, RunE: func(command *cobra.Command, args []string) error {
		return withStagingStore(command.Context(), globalOptions, func(ctx context.Context, store staging.Store, repositoryID string) error {
			job, err := findStagingJob(ctx, store, repositoryID, args[0])
			if err != nil {
				return err
			}
			segments, err := store.VerifyJob(ctx, job)
			if err != nil {
				return err
			}
			result := struct {
				Job      staging.Job       `json:"job"`
				Segments []staging.Segment `json:"segments"`
			}{Job: job, Segments: segments}
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			} else {
				globalOptions.Term.Print(fmt.Sprintf("journal %s: %s; %d segments; %d packs; %d protected bytes\n", job.Header.JobID, job.State, len(segments), job.Seal.PackCount, job.Seal.ProtectedBytes))
			}
			return nil
		})
	}}
}

func newIndexStagingAbandonCommand(globalOptions *global.Options) *cobra.Command {
	var reason string
	var acknowledge bool
	var safetyDelay time.Duration
	command := &cobra.Command{Use: "abandon JOB_ID", Short: "Publish an acknowledged journal abandonment", Args: cobra.ExactArgs(1), DisableAutoGenTag: true, RunE: func(command *cobra.Command, args []string) error {
		if reason == "" || !acknowledge {
			return fmt.Errorf("abandonment requires --reason and --acknowledge-data-loss")
		}
		if safetyDelay <= 0 {
			return fmt.Errorf("--safety-delay must be positive")
		}
		return withStagingStore(command.Context(), globalOptions, func(ctx context.Context, store staging.Store, repositoryID string) error {
			job, err := findStagingJob(ctx, store, repositoryID, args[0])
			if err != nil {
				return err
			}
			store.AbandonmentSafetyDelay = safetyDelay
			abandonment, err := store.PublishAbandonment(ctx, job, reason, "operator acknowledged staged data loss")
			if err != nil {
				return err
			}
			_ = observability.Emit(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "staging", Message: "sealed ingest journal abandoned"})
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(abandonment))
			} else {
				globalOptions.Term.Print(fmt.Sprintf("journal %s abandoned; packs remain protected until %s\n", job.Header.JobID, abandonment.DeleteAfter.Format(time.RFC3339)))
			}
			return nil
		})
	}}
	command.Flags().StringVar(&reason, "reason", "", "auditable reason for abandoning staged data")
	command.Flags().BoolVar(&acknowledge, "acknowledge-data-loss", false, "acknowledge that the staged backup may become unrecoverable")
	command.Flags().DurationVar(&safetyDelay, "safety-delay", 24*time.Hour, "minimum delay before abandoned packs lose GC protection")
	return command
}

func withStagingStore(ctx context.Context, globalOptions *global.Options, action func(context.Context, staging.Store, string) error) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	ctx, repo, unlock, err := openWithReadLock(ctx, *globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()
	store, err := stagingStore(repo)
	if err != nil {
		return err
	}
	return action(ctx, store, repo.Config().ID)
}

func stagingStore(repo *repository.Repository) (staging.Store, error) {
	config := repo.Config()
	if len(config.StagingBackends) == 0 {
		return staging.Store{}, fmt.Errorf("repository has no configured staging backends")
	}
	mirrors := make(map[string]backend.Backend, len(config.StagingBackends))
	mirrorPlacements := make(map[string]staging.MirrorPlacement, len(config.StagingBackends))
	configured := make(map[string]vaultic.PlacementBackend, len(config.PlacementBackends))
	for _, placement := range config.PlacementBackends {
		configured[placement.ID] = placement
	}
	for _, id := range config.StagingBackends {
		placementBackend, ok := repo.PlacementBackend(repository.PlacementBackendHash(id))
		if !ok {
			return staging.Store{}, fmt.Errorf("staging backend %q is not open", id)
		}
		mirrors[id] = placementBackend
		placement := configured[id]
		mirrorPlacements[id] = staging.MirrorPlacement{FailureDomain: placement.FailureDomain, Offsite: placement.Offsite}
	}
	key, err := staging.DeriveJournalKey(repo.Key().EncryptionKey[:], config.ID)
	if err != nil {
		return staging.Store{}, err
	}
	policy := config.PlacementPolicy
	maxExtension := time.Duration(config.StagingQuota.MaxExtensionSeconds) * time.Second
	if maxExtension == 0 {
		maxExtension = 30 * 24 * time.Hour
	}
	return staging.Store{Mirrors: mirrors, MirrorPlacements: mirrorPlacements, Key: key, Policy: staging.Policy{MinCopies: policy.MinCopies, MinDomains: policy.MinDomains, MinOffsite: policy.MinOffsite}, MaxExtension: maxExtension}, nil
}

func findStagingJob(ctx context.Context, store staging.Store, repositoryID, jobID string) (staging.Job, error) {
	jobs, err := store.Discover(ctx, repositoryID)
	if err != nil {
		return staging.Job{}, err
	}
	for _, job := range jobs {
		if job.Header.JobID == jobID {
			return job, nil
		}
	}
	return staging.Job{}, fmt.Errorf("staging journal %q was not found", jobID)
}

func printStagingJobs(globalOptions *global.Options, jobs []staging.Job) {
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(jobs))
		return
	}
	for _, job := range jobs {
		globalOptions.Term.Print(fmt.Sprintf("%s\t%s\t%d bytes\texpires %s\n", job.Header.JobID, job.State, job.Seal.ProtectedBytes, job.EffectiveExpiresAt().Format(time.RFC3339)))
	}
}
