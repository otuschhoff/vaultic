package main

import (
	"context"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
)

func newIndexStagingCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "staging", Short: "Inspect and control deferred ingest journals", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(
		newIndexStagingStatusCommand(globalOptions),
		newIndexStagingInspectCommand(globalOptions),
		newIndexStagingAbandonCommand(globalOptions),
	)
	return command
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
	for _, id := range config.StagingBackends {
		placementBackend, ok := repo.PlacementBackend(repository.PlacementBackendHash(id))
		if !ok {
			return staging.Store{}, fmt.Errorf("staging backend %q is not open", id)
		}
		mirrors[id] = placementBackend
	}
	key, err := staging.DeriveJournalKey(repo.Key().EncryptionKey[:], config.ID)
	if err != nil {
		return staging.Store{}, err
	}
	policy := config.PlacementPolicy
	return staging.Store{Mirrors: mirrors, Key: key, Policy: staging.Policy{MinCopies: policy.MinCopies, MinDomains: policy.MinDomains, MinOffsite: policy.MinOffsite}}, nil
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
		globalOptions.Term.Print(fmt.Sprintf("%s\t%s\t%d bytes\texpires %s\n", job.Header.JobID, job.State, job.Seal.ProtectedBytes, job.Header.ExpiresAt.Format(time.RFC3339)))
	}
}
