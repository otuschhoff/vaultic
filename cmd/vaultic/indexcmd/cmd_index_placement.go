package indexcmd

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type indexPlacementOptions struct {
	Daemon           indexDaemonOptions
	Unsatisfied      bool
	Overdue          bool
	PendingPromotion bool
	Explain          string
	NoFail           bool
	DryRun           bool
	Execute          bool
	MaxRequests      uint64
	MaxBytes         uint64
}

type indexPlacementMigratePoolOptions struct {
	Daemon      indexDaemonOptions
	From        string
	To          string
	DryRun      bool
	Execute     bool
	MaxRequests uint64
	MaxBytes    uint64
}

func newIndexPlacementCommand(globalOptions *global.Options) *cobra.Command {
	var options indexPlacementOptions
	command := &cobra.Command{
		Use:   "placement",
		Short: "Report placement durability and scheduler queue state",
		Long: "Report pack placement state, unsatisfied durability, overdue offsite deadlines, and pending promotions. " +
			"The JSON output is intended for monitoring probes." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexPlacement(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().BoolVar(&options.Unsatisfied, "unsatisfied", false, "show only packs below the durability predicate")
	command.Flags().BoolVar(&options.Overdue, "overdue", false, "show only packs past their offsite deadline")
	command.Flags().BoolVar(&options.PendingPromotion, "pending-promotion", false, "show only packs that require archival promotion")
	command.Flags().StringVar(&options.Explain, "explain", "", "show placement reasoning for one pack ID")
	command.Flags().BoolVar(&options.NoFail, "no-fail", false, "return success even when packs are overdue")
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report scheduler requests without writing rq records")
	command.Flags().BoolVar(&options.Execute, "execute", false, "process queued placement work after planning")
	command.Flags().Uint64Var(&options.MaxRequests, "max-requests", 0, "process at most this many placement requests (0 is unlimited)")
	command.Flags().Uint64Var(&options.MaxBytes, "max-bytes", 0, "move at most this many pack bytes (0 is unlimited)")
	command.AddCommand(newIndexPlacementMigratePoolCommand(globalOptions))
	return command
}

func newIndexPlacementMigratePoolCommand(globalOptions *global.Options) *cobra.Command {
	var options indexPlacementMigratePoolOptions
	command := &cobra.Command{
		Use:               "migrate-pool --from BACKEND --to BACKEND",
		Short:             "Queue pack copies from a legacy placement backend to an active backend",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexPlacementMigratePool(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringVar(&options.From, "from", "", "source placement backend ID")
	command.Flags().StringVar(&options.To, "to", "", "target placement backend ID")
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report migration requests without writing rq records")
	command.Flags().BoolVar(&options.Execute, "execute", false, "process queued migration work after planning")
	command.Flags().Uint64Var(&options.MaxRequests, "max-requests", 0, "process at most this many placement requests (0 is unlimited)")
	command.Flags().Uint64Var(&options.MaxBytes, "max-bytes", 0, "move at most this many pack bytes (0 is unlimited)")
	_ = command.MarkFlagRequired("from")
	_ = command.MarkFlagRequired("to")
	return command
}

type PlacementActions struct {
	Repository *repository.Repository
	Printer    vaultic.Printer
}

func (actions PlacementActions) Place(ctx context.Context, packID vaultic.ID, backend maintenance.PlacementBackend) error {
	return actions.Repository.PlacePack(ctx, packID, backend.Hash)
}

func (actions PlacementActions) Promote(ctx context.Context, packID vaultic.ID, backend maintenance.PlacementBackend) error {
	_, err := repository.PromotePack(ctx, actions.Repository, packID, backend.Hash, actions.Printer)
	return err
}

func (actions PlacementActions) Evict(ctx context.Context, packID vaultic.ID, backend maintenance.PlacementBackend) error {
	return actions.Repository.EvictPack(ctx, packID, backend.Hash)
}

type repositoryPlacementActions = PlacementActions

func runIndexPlacement(
	ctx context.Context,
	options indexPlacementOptions,
	globalOptions global.Options,
	term ui.Terminal,
) (maintenance.PlacementSchedulerResult, error) {
	var result maintenance.PlacementSchedulerResult
	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	var repo *repository.Repository
	var unlock func()
	if options.DryRun {
		ctx, repo, unlock, err = openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	} else {
		ctx, repo, unlock, err = openWithExclusiveLock(ctx, globalOptions, false, printer)
	}
	if err != nil {
		return result, err
	}
	defer unlock()
	if err := requireSlateDBRepository(repo); err != nil {
		return result, err
	}
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return result, err
	}
	defer closeStore()
	model, err := indexMaintenancePlacementModel(repo)
	if err != nil {
		return result, err
	}
	result, err = maintenance.PlanPlacement(ctx, store, maintenance.PlacementSchedulerOptions{Model: model, Now: time.Now(), DryRun: options.DryRun})
	if err != nil {
		return result, err
	}
	if options.Execute && !options.DryRun {
		worker, workerErr := maintenance.ExecutePlacement(
			ctx,
			store,
			repositoryPlacementActions{Repository: repo, Printer: printer},
			maintenance.PlacementWorkerOptions{
				Model: model, Now: time.Now(), MaxRequests: options.MaxRequests, MaxBytes: options.MaxBytes,
			},
		)
		result.Worker = &worker
		if workerErr != nil {
			return result, workerErr
		}
		result, err = maintenance.PlanPlacement(ctx, store, maintenance.PlacementSchedulerOptions{Model: model, Now: time.Now()})
		result.Worker = &worker
		if err != nil {
			return result, err
		}
	}
	result.Statuses = filterPlacementStatuses(result.Statuses, options)
	if !globalOptions.JSON {
		printer.P(
			"packs %d; unsatisfied %d; overdue %d; pending promotion %d; scheduler requests %d\n",
			result.PacksScanned,
			result.Unsatisfied,
			result.Overdue,
			result.PendingPromotion,
			result.RequestsWritten,
		)
		if result.Worker != nil {
			printer.P(
				"worker attempted %d; placed %d; promoted %d; evicted %d; failed %d; deferred %d\n",
				result.Worker.Attempted,
				result.Worker.Placed,
				result.Worker.Promoted,
				result.Worker.Evicted,
				result.Worker.Failed,
				result.Worker.Deferred,
			)
		}
		for _, status := range result.Statuses {
			printer.P(
				"  %s class=%s durable=%v overdue=%v live=%v missing=%v\n",
				status.PackID,
				status.Class,
				status.Durable,
				status.Overdue,
				status.LiveBackends,
				status.MissingBackends,
			)
		}
	}
	if result.Overdue != 0 && !options.NoFail {
		return result, errIndexDifferences
	}
	return result, nil
}

func runIndexPlacementMigratePool(
	ctx context.Context,
	options indexPlacementMigratePoolOptions,
	globalOptions global.Options,
	term ui.Terminal,
) (maintenance.PlacementSchedulerResult, error) {
	var result maintenance.PlacementSchedulerResult
	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return result, err
	}
	defer unlock()
	if err := requireSlateDBRepository(repo); err != nil {
		return result, err
	}
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return result, err
	}
	defer closeStore()
	model, err := indexMaintenancePlacementModel(repo)
	if err != nil {
		return result, err
	}
	result, err = maintenance.PlanPoolMigration(
		ctx,
		store,
		maintenance.PlacementMigrationOptions{Model: model, From: options.From, To: options.To, Now: time.Now(), DryRun: options.DryRun},
	)
	if err != nil {
		return result, err
	}
	if options.Execute && !options.DryRun {
		worker, workerErr := maintenance.ExecutePlacement(
			ctx,
			store,
			repositoryPlacementActions{Repository: repo, Printer: printer},
			maintenance.PlacementWorkerOptions{
				Model: model, Now: time.Now(), MaxRequests: options.MaxRequests, MaxBytes: options.MaxBytes,
			},
		)
		result.Worker = &worker
		if workerErr != nil {
			return result, workerErr
		}
	}
	if !globalOptions.JSON {
		printer.P("queued %d migration requests from %s to %s\n", result.RequestsWritten, options.From, options.To)
		if result.Worker != nil {
			printer.P(
				"worker attempted %d; placed %d; promoted %d; evicted %d; failed %d; deferred %d\n",
				result.Worker.Attempted,
				result.Worker.Placed,
				result.Worker.Promoted,
				result.Worker.Evicted,
				result.Worker.Failed,
				result.Worker.Deferred,
			)
		}
	}
	return result, nil
}

func filterPlacementStatuses(input []maintenance.PlacementStatus, options indexPlacementOptions) []maintenance.PlacementStatus {
	filtered := input[:0]
	for _, status := range input {
		if options.Explain != "" && status.PackID != options.Explain {
			continue
		}
		if options.Unsatisfied && status.Durable {
			continue
		}
		if options.Overdue && !status.Overdue {
			continue
		}
		if options.PendingPromotion && !status.PendingPromotion {
			continue
		}
		filtered = append(filtered, status)
	}
	return filtered
}
