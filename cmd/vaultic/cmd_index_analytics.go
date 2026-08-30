package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/analytics"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
)

type indexAnalyticsOptions struct {
	Daemon            indexDaemonOptions
	DryRun            bool
	Purge             bool
	UIDs              []uint
	GIDs              []uint
	Years             []int
	Months            []int
	ISOYears          []int
	Workweeks         []int
	SVMs              []string
	Volumes           []string
	PathGroups        []string
	SizeMin           uint64
	SizeMax           uint64
	HasSizeMin        bool
	HasSizeMax        bool
	SizeLog10         []int
	Residencies       []string
	CreationBases     []string
	Continuities      []string
	GroupBy           []string
	IncludeIncomplete bool
	RequireCurrent    bool
	AllowStale        bool
	Explain           bool
	Async             bool
	QueryID           string
	Resume            bool
	Cancel            bool
	Wait              bool
}

func newIndexAnalyticsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexAnalyticsOptions
	command := &cobra.Command{
		Use: "analytics", Short: "Query optional creation analytics", Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			options.HasSizeMin = command.Flags().Changed("size-min")
			options.HasSizeMax = command.Flags().Changed("size-max")
			result, err := runIndexAnalytics(command.Context(), options, *globalOptions)
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
		newIndexAnalyticsCatchUpCommand(&options, globalOptions),
		newIndexAnalyticsCacheCommand(&options, globalOptions),
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
	flags.StringSliceVar(&options.Residencies, "residency", nil, "include live, archive-only, deleted, expired, or unknown")
	flags.StringSliceVar(&options.CreationBases, "creation-basis", nil, "include ctime, mtime, birth-time, first-seen, or unknown")
	flags.StringSliceVar(&options.Continuities, "identity-continuity", nil, "include proven, source-generation, or unknown")
	flags.StringSliceVar(&options.GroupBy, "group-by", nil, "group by any tracked analytics dimension")
	flags.BoolVar(&options.IncludeIncomplete, "include-incomplete", false, "include facts with incomplete identity continuity")
	flags.BoolVar(&options.RequireCurrent, "require-current", false, "fail unless the analytics watermark has reached metadata head")
	flags.BoolVar(&options.AllowStale, "allow-stale", false, "accept the latest completely published analytics watermark")
	flags.BoolVar(&options.Explain, "explain", false, "include query planning and scan details")
	flags.BoolVar(&options.Async, "async", false, "persist a query job and return its ID")
	flags.StringVar(&options.QueryID, "query-id", "", "inspect or operate on a persistent query job")
	flags.BoolVar(&options.Resume, "resume", false, "resume the job selected by --query-id")
	flags.BoolVar(&options.Cancel, "cancel", false, "cancel the job selected by --query-id")
	flags.BoolVar(&options.Wait, "wait", false, "run a new or existing persistent job to completion")
}

func runIndexAnalytics(ctx context.Context, options indexAnalyticsOptions, globalOptions global.Options) (any, error) {
	if err := validateAnalyticsJobOptions(options); err != nil {
		return nil, err
	}
	if !options.Async && options.QueryID == "" {
		return runIndexAnalyticsQuery(ctx, options, globalOptions)
	}
	return withAnalyticsStore(ctx, options, globalOptions, true, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
		if options.QueryID != "" {
			id, err := parseAnalyticsID(options.QueryID)
			if err != nil {
				return nil, err
			}
			if options.Cancel {
				if err := analytics.Cancel(ctx, store, id); err != nil {
					return nil, err
				}
				return analytics.QueryJobStatus(ctx, store, id)
			}
			if options.Resume || options.Wait {
				return analytics.Resume(ctx, store, id)
			}
			return analytics.QueryJobStatus(ctx, store, id)
		}
		query, err := analyticsQuery(options)
		if err != nil {
			return nil, err
		}
		id, err := analytics.Start(ctx, store, query)
		if err != nil {
			return nil, err
		}
		if options.Wait {
			return analytics.Wait(ctx, store, id)
		}
		return analytics.QueryJobStatus(ctx, store, id)
	})
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
	if action == "rebuild" {
		command.Flags().BoolVar(&options.Purge, "purge", false, "discard analytics checkpoints, jobs, candidates, and the visible generation before rebuilding")
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
			return analytics.InspectStatus(ctx, store)
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
}

func newIndexAnalyticsCatchUpCommand(options *indexAnalyticsOptions, globalOptions *global.Options) *cobra.Command {
	var maxDeltas uint32
	command := &cobra.Command{Use: "catch-up", Short: "Advance the analytics builder and report watermark lag", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		result, err := withAnalyticsStore(command.Context(), *options, *globalOptions, true, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
			if command.Flags().Changed("max-deltas") {
				return analytics.CatchUp(ctx, store, analytics.CatchUpOptions{MaxDeltas: maxDeltas})
			}
			return analytics.CatchUpStatus(ctx, store)
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	command.Flags().Uint32Var(&maxDeltas, "max-deltas", 0, "consume at most this many outbox deltas; omit for status only")
	return command
}

func newIndexAnalyticsCacheCommand(options *indexAnalyticsOptions, globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "cache", Short: "Inspect or purge analytics query cache and views", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		result, err := withAnalyticsStore(command.Context(), *options, *globalOptions, false, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
			return analytics.InspectCache(ctx, store)
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	var includeViews, includeJobs, dryRun bool
	purge := &cobra.Command{Use: "purge", Short: "Purge persistent query results and heat", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		result, err := withAnalyticsStore(command.Context(), *options, *globalOptions, true, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
			return analytics.PurgeCache(ctx, store, includeViews, includeJobs, dryRun)
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	purge.Flags().BoolVar(&includeViews, "views", false, "also purge adaptive view records")
	purge.Flags().BoolVar(&includeJobs, "jobs", false, "also purge persistent query jobs")
	purge.Flags().BoolVar(&dryRun, "dry-run", false, "report deletions without writing")
	command.AddCommand(purge)
	return command
}

func runIndexAnalyticsLifecycle(ctx context.Context, action string, options indexAnalyticsOptions, globalOptions global.Options) (any, error) {
	return withAnalyticsStore(ctx, options, globalOptions, true, func(ctx context.Context, store analytics.Store, config analytics.Config) (any, error) {
		switch action {
		case "enable":
			return analytics.Enable(ctx, store, config, options.DryRun)
		case "rebuild":
			if options.Purge {
				if _, err := analytics.Purge(ctx, store, options.DryRun); err != nil || options.DryRun {
					return nil, err
				}
			}
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
	if err := validateAnalyticsQueryOptions(options); err != nil {
		return nil, err
	}
	return withAnalyticsStore(ctx, options, globalOptions, true, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
		query, err := analyticsQuery(options)
		if err != nil {
			return nil, err
		}
		return analytics.Execute(ctx, store, query)
	})
}

func analyticsQuery(options indexAnalyticsOptions) (analytics.Query, error) {
	if err := validateAnalyticsQueryOptions(options); err != nil {
		return analytics.Query{}, err
	}
	uids, err := toUint32(options.UIDs)
	if err != nil {
		return analytics.Query{}, fmt.Errorf("invalid --uid: %w", err)
	}
	gids, err := toUint32(options.GIDs)
	if err != nil {
		return analytics.Query{}, fmt.Errorf("invalid --gid: %w", err)
	}
	query := analytics.Query{UIDs: uids, GIDs: gids, Years: append([]int(nil), options.Years...), Months: append([]int(nil), options.Months...), ISOYears: append([]int(nil), options.ISOYears...), Workweeks: append([]int(nil), options.Workweeks...), SVMs: append([]string(nil), options.SVMs...), Volumes: append([]string(nil), options.Volumes...), PathGroups: append([]string(nil), options.PathGroups...), SizeLog10: append([]int(nil), options.SizeLog10...), Residencies: append([]string(nil), options.Residencies...), CreationBases: append([]string(nil), options.CreationBases...), IdentityContinuities: append([]string(nil), options.Continuities...), GroupBy: append([]string(nil), options.GroupBy...), IncludeIncomplete: options.IncludeIncomplete, RequireCurrent: options.RequireCurrent, AllowStale: options.AllowStale}
	if options.HasSizeMin {
		query.SizeMin = &options.SizeMin
	}
	if options.HasSizeMax {
		query.SizeMax = &options.SizeMax
	}
	return query, query.Validate()
}

func validateAnalyticsQueryOptions(options indexAnalyticsOptions) error {
	if options.HasSizeMin && options.HasSizeMax && options.SizeMin >= options.SizeMax {
		return fmt.Errorf("--size-min must be less than exclusive --size-max")
	}
	if options.RequireCurrent && options.AllowStale {
		return fmt.Errorf("--require-current and --allow-stale are mutually exclusive")
	}
	return nil
}

func validateAnalyticsJobOptions(options indexAnalyticsOptions) error {
	if options.Async && options.QueryID != "" {
		return fmt.Errorf("--async and --query-id are mutually exclusive")
	}
	if (options.Resume || options.Cancel) && options.QueryID == "" {
		return fmt.Errorf("--resume and --cancel require --query-id")
	}
	if options.Resume && options.Cancel {
		return fmt.Errorf("--resume and --cancel are mutually exclusive")
	}
	if options.Wait && options.Cancel {
		return fmt.Errorf("--wait and --cancel are mutually exclusive")
	}
	if options.Wait && !options.Async && options.QueryID == "" {
		return fmt.Errorf("--wait requires --async or --query-id")
	}
	return validateAnalyticsQueryOptions(options)
}

func parseAnalyticsID(value string) (schema.ID, error) {
	var id schema.ID
	decoded, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil || len(decoded) != len(id) {
		return id, fmt.Errorf("invalid --query-id: expected 64 hexadecimal characters")
	}
	copy(id[:], decoded)
	return id, nil
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

type indexGrowthOptions struct {
	Daemon                    indexDaemonOptions
	Granularity, Since, Until string
	SVMs, Volumes, PathGroups []string
}

func newIndexGrowthCommand(globalOptions *global.Options) *cobra.Command {
	var options indexGrowthOptions
	command := &cobra.Command{Use: "growth", Short: "Report exact creation growth by calendar period", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		since, until, err := parseTimeRange(options.Since, options.Until)
		if err != nil {
			return err
		}
		base := indexAnalyticsOptions{Daemon: options.Daemon}
		result, err := withAnalyticsStore(command.Context(), base, *globalOptions, false, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
			return analytics.Growth(ctx, store, analytics.GrowthOptions{Granularity: options.Granularity, Since: since, Until: until, SVMs: options.SVMs, Volumes: options.Volumes, PathGroups: options.PathGroups})
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringVar(&options.Granularity, "granularity", "month", "bucket by year, month, or iso-week")
	command.Flags().StringVar(&options.Since, "since", "", "inclusive RFC3339 creation time")
	command.Flags().StringVar(&options.Until, "until", "", "exclusive RFC3339 creation time")
	command.Flags().StringSliceVar(&options.SVMs, "svm", nil, "include source SVM")
	command.Flags().StringSliceVar(&options.Volumes, "volume", nil, "include source volume")
	command.Flags().StringSliceVar(&options.PathGroups, "path", nil, "include classified source path group")
	command.Flags().StringSliceVar(&options.PathGroups, "path-group", nil, "alias for --path")
	return command
}

type indexUserStatsOptions struct {
	Daemon                indexDaemonOptions
	UIDs, GIDs            []uint
	Residencies           []string
	GroupBy, Since, Until string
	Limit                 int
}

func newIndexUserStatsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexUserStatsOptions
	command := &cobra.Command{Use: "user-stats", Short: "Rank exact creation totals by UID or GID", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		uids, err := toUint32(options.UIDs)
		if err != nil {
			return fmt.Errorf("invalid --uid: %w", err)
		}
		gids, err := toUint32(options.GIDs)
		if err != nil {
			return fmt.Errorf("invalid --gid: %w", err)
		}
		since, until, err := parseTimeRange(options.Since, options.Until)
		if err != nil {
			return err
		}
		base := indexAnalyticsOptions{Daemon: options.Daemon}
		result, err := withAnalyticsStore(command.Context(), base, *globalOptions, false, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
			return analytics.UserStats(ctx, store, analytics.UserStatsOptions{UIDs: uids, GIDs: gids, Residencies: options.Residencies, Since: since, Until: until, GroupBy: options.GroupBy, Limit: options.Limit})
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().UintSliceVar(&options.UIDs, "uid", nil, "include UID")
	command.Flags().UintSliceVar(&options.GIDs, "gid", nil, "include GID")
	command.Flags().StringSliceVar(&options.Residencies, "residency", nil, "include live, archive-only, deleted, expired, or unknown")
	command.Flags().StringVar(&options.GroupBy, "group-by", "user", "rank by user or group")
	command.Flags().StringVar(&options.Since, "since", "", "inclusive RFC3339 creation time")
	command.Flags().StringVar(&options.Until, "until", "", "exclusive RFC3339 creation time")
	command.Flags().IntVar(&options.Limit, "limit", 0, "maximum ranked rows; zero means all")
	return command
}

func newIndexGDPRCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "gdpr", Short: "Inspect retained data by identity", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(newIndexGDPRAuditCommand(globalOptions))
	return command
}

func newIndexGDPRAuditCommand(globalOptions *global.Options) *cobra.Command {
	var daemonOptions indexDaemonOptions
	var uid uint64
	command := &cobra.Command{Use: "audit", Short: "Report retained creation data by identity and residency", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !command.Flags().Changed("uid") {
			return fmt.Errorf("--uid is required")
		}
		if uid > math.MaxUint32 {
			return fmt.Errorf("invalid --uid: value %d exceeds uint32", uid)
		}
		base := indexAnalyticsOptions{Daemon: daemonOptions}
		result, err := withAnalyticsStore(command.Context(), base, *globalOptions, false, func(ctx context.Context, store analytics.Store, _ analytics.Config) (any, error) {
			return analytics.GDPRAudit(ctx, store, uint32(uid))
		})
		globalOptions.Term.Print(ui.ToJSONString(result))
		return err
	}}
	daemonOptions.AddFlags(command.Flags())
	command.Flags().Uint64Var(&uid, "uid", 0, "audit this UID")
	return command
}

func parseTimeRange(sinceValue, untilValue string) (*int64, *int64, error) {
	parse := func(name, value string) (*int64, error) {
		if value == "" {
			return nil, nil
		}
		instant, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("invalid --%s: expected RFC3339 timestamp: %w", name, err)
		}
		nanoseconds := instant.UnixNano()
		return &nanoseconds, nil
	}
	since, err := parse("since", sinceValue)
	if err != nil {
		return nil, nil, err
	}
	until, err := parse("until", untilValue)
	if err != nil {
		return nil, nil, err
	}
	if since != nil && until != nil && *since >= *until {
		return nil, nil, fmt.Errorf("--since must be less than exclusive --until")
	}
	return since, until, nil
}
