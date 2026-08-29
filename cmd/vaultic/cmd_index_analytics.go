package main

import (
	"context"
	"fmt"
	"math"

	"github.com/spf13/cobra"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/analytics"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
)

type indexAnalyticsOptions struct {
	Daemon      indexDaemonOptions
	DryRun      bool
	Purge       bool
	UIDs        []uint
	GIDs        []uint
	Years       []int
	Months      []int
	ISOYears    []int
	Workweeks   []int
	SVMs        []string
	Volumes     []string
	PathGroups  []string
	SizeMin     uint64
	SizeMax     uint64
	HasSizeMin  bool
	HasSizeMax  bool
	SizeLog10   []int
	Residencies []string
	GroupBy     []string
}

func newIndexAnalyticsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexAnalyticsOptions
	command := &cobra.Command{
		Use: "analytics", Short: "Query optional creation analytics", Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			options.HasSizeMin = command.Flags().Changed("size-min")
			options.HasSizeMax = command.Flags().Changed("size-max")
			result, err := runIndexAnalyticsQuery(command.Context(), options, *globalOptions)
			globalOptions.Term.Print(ui.ToJSONString(result))
			return err
		},
	}
	options.Daemon.AddFlags(command.PersistentFlags())
	addAnalyticsQueryFlags(command, &options)
	command.AddCommand(
		newIndexAnalyticsLifecycleCommand("enable", "Enable and fully rebuild creation analytics", &options, globalOptions),
		newIndexAnalyticsLifecycleCommand("rebuild", "Rebuild creation analytics from authoritative metadata", &options, globalOptions),
		newIndexAnalyticsLifecycleCommand("disable", "Disable creation analytics", &options, globalOptions),
		newIndexAnalyticsLifecycleCommand("purge", "Delete all derived analytics data", &options, globalOptions),
		newIndexAnalyticsStatusCommand(&options, globalOptions),
	)
	return command
}

func addAnalyticsQueryFlags(command *cobra.Command, options *indexAnalyticsOptions) {
	flags := command.Flags()
	flags.UintSliceVar(&options.UIDs, "uid", nil, "include UID; repeat or comma-separate")
	flags.UintSliceVar(&options.GIDs, "gid", nil, "include GID; repeat or comma-separate")
	flags.IntSliceVar(&options.Years, "year", nil, "include UTC calendar year")
	flags.IntSliceVar(&options.Months, "month", nil, "include calendar month (1-12)")
	flags.IntSliceVar(&options.ISOYears, "iso-year", nil, "include ISO week-numbering year")
	flags.IntSliceVar(&options.Workweeks, "workweek", nil, "include ISO workweek (1-53)")
	flags.StringSliceVar(&options.SVMs, "svm", nil, "include source SVM")
	flags.StringSliceVar(&options.Volumes, "volume", nil, "include source volume")
	flags.StringSliceVar(&options.PathGroups, "path-group", nil, "include classified source path group")
	flags.Uint64Var(&options.SizeMin, "size-min", 0, "minimum logical file size")
	flags.Uint64Var(&options.SizeMax, "size-max", 0, "maximum logical file size")
	flags.IntSliceVar(&options.SizeLog10, "size-log10", nil, "include decimal file-size magnitude")
	flags.StringSliceVar(&options.Residencies, "residency", nil, "include live, archive-only, or unknown")
	flags.StringSliceVar(&options.GroupBy, "group-by", nil, "group by any subset of uid,gid,year,month,iso-year,workweek,svm,volume,path-group,size-log10,residency")
}

func newIndexAnalyticsLifecycleCommand(action, short string, options *indexAnalyticsOptions, globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: action, Short: short, Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.RunE = func(command *cobra.Command, _ []string) error {
		result, err := runIndexAnalyticsLifecycle(command.Context(), action, *options, *globalOptions)
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}
	if action == "enable" || action == "rebuild" {
		command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report rebuild without writing analytics records")
	}
	if action == "disable" {
		command.Flags().BoolVar(&options.Purge, "purge", false, "also delete all derived facts and cache entries")
		command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report changes without writing")
	}
	if action == "purge" {
		command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report deletions without writing")
	}
	return command
}

func newIndexAnalyticsStatusCommand(options *indexAnalyticsOptions, globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{Use: "status", Short: "Show analytics lifecycle state", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		result, err := withAnalyticsStore(command.Context(), *options, *globalOptions, false, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
			return analytics.Status(ctx, store)
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
}

func runIndexAnalyticsLifecycle(ctx context.Context, action string, options indexAnalyticsOptions, globalOptions global.Options) (any, error) {
	return withAnalyticsStore(ctx, options, globalOptions, true, func(ctx context.Context, store analytics.Store, config analytics.Config) (any, error) {
		switch action {
		case "enable":
			return analytics.Enable(ctx, store, config, options.DryRun)
		case "rebuild":
			return analytics.Rebuild(ctx, store, config, options.DryRun)
		case "disable":
			return analytics.Disable(ctx, store, options.Purge, options.DryRun)
		case "purge":
			return analytics.Purge(ctx, store, options.DryRun)
		default:
			return nil, fmt.Errorf("unknown analytics lifecycle action %q", action)
		}
	})
}

func runIndexAnalyticsQuery(ctx context.Context, options indexAnalyticsOptions, globalOptions global.Options) (any, error) {
	return withAnalyticsStore(ctx, options, globalOptions, true, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
		uids, err := toUint32(options.UIDs)
		if err != nil {
			return nil, fmt.Errorf("invalid --uid: %w", err)
		}
		gids, err := toUint32(options.GIDs)
		if err != nil {
			return nil, fmt.Errorf("invalid --gid: %w", err)
		}
		query := analytics.Query{UIDs: uids, GIDs: gids, Years: options.Years, Months: options.Months, ISOYears: options.ISOYears, Workweeks: options.Workweeks, SVMs: options.SVMs, Volumes: options.Volumes, PathGroups: options.PathGroups, SizeLog10: options.SizeLog10, Residencies: options.Residencies, GroupBy: options.GroupBy}
		if options.HasSizeMin {
			query.SizeMin = &options.SizeMin
		}
		if options.HasSizeMax {
			query.SizeMax = &options.SizeMax
		}
		return analytics.Execute(ctx, store, query)
	})
}

func withAnalyticsStore(ctx context.Context, options indexAnalyticsOptions, globalOptions global.Options, exclusive bool, run func(context.Context, analytics.Store, analytics.Config) (any, error)) (any, error) {
	config, err := options.Daemon.config("")
	if err != nil {
		return nil, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	var repo *repository.Repository
	var unlock func()
	if exclusive {
		ctx, repo, unlock, err = openWithExclusiveLock(ctx, globalOptions, false, printer)
	} else {
		ctx, repo, unlock, err = openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	}
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := requireSlateDBRepository(repo); err != nil {
		return nil, err
	}
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return nil, err
	}
	defer closeStore()
	configured := repo.Config()
	analyticsConfig := analytics.Config{SVMDepth: configured.AnalyticsSVMDepth, VolumeDepth: configured.AnalyticsVolumeDepth, PathGroupDepth: configured.AnalyticsPathGroupDepth, PathGroupPrefixes: configured.AnalyticsPathGroupPrefixes, CacheAfter: configured.AnalyticsCacheAfter, CacheTTLSeconds: configured.AnalyticsCacheTTLSeconds}
	return run(ctx, store, analyticsConfig)
}

func toUint32(values []uint) ([]uint32, error) {
	result := make([]uint32, len(values))
	for index, value := range values {
		if uint64(value) > math.MaxUint32 {
			return nil, fmt.Errorf("value %d exceeds uint32", value)
		}
		result[index] = uint32(value)
	}
	return result, nil
}

func newIndexGrowthCommand(globalOptions *global.Options) *cobra.Command {
	var options indexAnalyticsOptions
	command := &cobra.Command{Use: "growth", Short: "Report creation growth by calendar period", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		options.GroupBy = []string{"year", "month"}
		result, err := runIndexAnalyticsQuery(command.Context(), options, *globalOptions)
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().IntSliceVar(&options.Years, "year", nil, "include UTC calendar year")
	command.Flags().IntSliceVar(&options.Months, "month", nil, "include calendar month (1-12)")
	return command
}

func newIndexUserStatsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexAnalyticsOptions
	command := &cobra.Command{Use: "user-stats", Short: "Report creation totals by UID and GID", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		options.GroupBy = []string{"uid", "gid"}
		result, err := runIndexAnalyticsQuery(command.Context(), options, *globalOptions)
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().UintSliceVar(&options.UIDs, "uid", nil, "include UID")
	command.Flags().UintSliceVar(&options.GIDs, "gid", nil, "include GID")
	return command
}

func newIndexGDPRCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "gdpr", Short: "Inspect retained data by identity", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(newIndexGDPRAuditCommand(globalOptions))
	return command
}

func newIndexGDPRAuditCommand(globalOptions *global.Options) *cobra.Command {
	var options indexAnalyticsOptions
	command := &cobra.Command{Use: "audit", Short: "Report retained creation data by identity and residency", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		options.GroupBy = []string{"uid", "gid", "residency", "svm", "volume"}
		result, err := runIndexAnalyticsQuery(command.Context(), options, *globalOptions)
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().UintSliceVar(&options.UIDs, "uid", nil, "include UID")
	command.Flags().UintSliceVar(&options.GIDs, "gid", nil, "include GID")
	command.Flags().StringSliceVar(&options.Residencies, "residency", nil, "include residency state")
	return command
}
