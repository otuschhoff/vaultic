package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// requireSlateDBRepository refuses introspection that has no meaningful answer
// without a pack catalog. Reporting a legacy repository's partial state as
// though it were complete would be worse than refusing outright.
func requireSlateDBRepository(repo *repository.Repository) error {
	if repo.Engine().Mode() != metadataindex.ModeSlateDB {
		return maintenance.ErrLegacyRepository
	}
	return nil
}

// --- index stats ---

type indexStatsOptions struct {
	Daemon  indexDaemonOptions
	GroupBy []string
	Tier    string
	Type    string
	State   string
	Verify  bool
	Rebuild bool
	DryRun  bool
}

func newIndexStatsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexStatsOptions
	command := &cobra.Command{
		Use:               "stats",
		Short:             "Report repository composition from the pack catalog",
		Long:              "Report pack counts and sizes for the repository, grouped by tier, type, or lifecycle state. Totals are read from the constant-time aggregate records unless a filter, grouping, or verification requires the catalog." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexStats(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringSliceVar(&options.GroupBy, "by", nil, "group totals by tier, type, or state")
	command.Flags().StringVar(&options.Tier, "tier", "", "only packs in this tier")
	command.Flags().StringVar(&options.Type, "type", "", "only packs of this type: data, tree, mixed, unknown")
	command.Flags().StringVar(&options.State, "state", "", "only packs in this lifecycle state")
	command.Flags().BoolVar(&options.Verify, "verify", false, "recompute totals from the pack catalog and report aggregate drift")
	command.Flags().BoolVar(&options.Rebuild, "rebuild", false, "rewrite aggregate records from the pack catalog")
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "with --rebuild, report the changes without writing them")
	return command
}

func runIndexStats(ctx context.Context, options indexStatsOptions, globalOptions global.Options, term ui.Terminal) (maintenance.StatsResult, error) {
	var result maintenance.StatsResult
	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	// Rebuilding mutates aggregates and therefore needs the exclusive view.
	open := openWithReadLock
	if options.Rebuild && !options.DryRun {
		open = func(ctx context.Context, gopts global.Options, noLock bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
			return openWithExclusiveLock(ctx, gopts, false, printer)
		}
	}
	ctx, repo, unlock, err := open(ctx, globalOptions, globalOptions.NoLock, printer)
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

	result, err = maintenance.Stats(ctx, store, maintenance.StatsOptions{
		GroupBy: options.GroupBy, Tier: options.Tier, Type: options.Type, State: options.State,
		Verify: options.Verify, Rebuild: options.Rebuild, DryRun: options.DryRun,
	})
	if err != nil {
		return result, err
	}
	if !globalOptions.JSON {
		printStats(printer, result)
	}
	if result.HasDrift() {
		return result, errIndexDifferences
	}
	return result, nil
}

func printStats(printer vaultic.Printer, result maintenance.StatsResult) {
	printer.P("source: %s\n", result.Source)
	printer.P("packs %d; physical %d; payload %d; header %d; blobs %d\n",
		result.Totals.PackCount, result.Totals.PhysicalSize, result.Totals.PayloadSize,
		result.Totals.HeaderSize, result.Totals.BlobCount)
	printer.P("stored physical %d (logical %d)\n", result.StoredPhysicalSize, result.Totals.PhysicalSize)
	printer.P("usage: used %d; unused %d; ratio %.4f (accounted packs %d)\n",
		result.Totals.UsedPayloadBytes, result.Totals.UnusedPayloadBytes,
		result.Totals.UnusedRatio, result.Totals.AccountedPackCount)
	// Unknowns are always stated rather than folded into the totals above.
	retention := "not measured (use --verify or a filter to scan the catalog)"
	if result.RetentionCounted {
		retention = fmt.Sprintf("%d", result.RetentionUnknownPacks)
	}
	printer.P("unknown tier %d; unknown type %d; mixed type %d; retention unknown %s; usage unaccounted %d\n",
		result.UnknownTierPacks, result.UnknownTypePacks, result.MixedTypePacks,
		retention, result.UsageUnaccountedPacks)
	if result.Source == maintenance.SourceCatalog {
		printer.P("coverage gaps: creation time unknown %d; physical size unknown %d\n",
			result.CreationTimeUnknownPacks, result.PhysicalSizeUnknownPacks)
	}
	if result.TierAggregatesUnbuilt {
		printer.P("per-tier aggregates have not been built yet; run 'vaultic index rebuild-pack-stats'\n")
	}
	for _, group := range result.Groups {
		printer.P("  %s=%s: packs %d; physical %d; payload %d; unused ratio %.4f\n",
			group.Dimension, group.Key, group.PackCount, group.PhysicalSize, group.PayloadSize, group.UnusedRatio)
	}
	if result.Rebuilt != nil {
		printer.P("rebuilt %d aggregate records from %d packs\n", result.Rebuilt.AggregatesChanged, result.Rebuilt.PacksScanned)
	}
	for _, delta := range result.Drift {
		printer.E("aggregate drift %s: packs %d, payload %d\n", delta.Key, delta.After.PackCount, delta.After.PayloadSize)
	}
}

// --- index packs ---

type indexPacksOptions struct {
	Daemon           indexDaemonOptions
	Tier             string
	Type             string
	State            string
	CreatedBefore    string
	CreatedAfter     string
	MinSize          uint64
	MaxSize          uint64
	UnusedRatioAbove float64
	RetentionExpired bool
	RetentionUnknown bool
	DeletePending    bool
	Sort             string
	Limit            uint
	CountOnly        bool
}

func newIndexPacksCommand(globalOptions *global.Options) *cobra.Command {
	var options indexPacksOptions
	command := &cobra.Command{
		Use:               "packs",
		Short:             "Query the SlateDB pack catalog",
		Long:              "Filter, sort, and list pack catalog entries without loading a full index or listing the backend." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexPacks(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringVar(&options.Tier, "tier", "", "only packs in this tier")
	command.Flags().StringVar(&options.Type, "type", "", "only packs of this type")
	command.Flags().StringVar(&options.State, "state", "", "only packs in this lifecycle state")
	command.Flags().StringVar(&options.CreatedBefore, "created-before", "", "only packs created before this RFC3339 time")
	command.Flags().StringVar(&options.CreatedAfter, "created-after", "", "only packs created after this RFC3339 time")
	command.Flags().Uint64Var(&options.MinSize, "min-size", 0, "only packs of at least this physical size")
	command.Flags().Uint64Var(&options.MaxSize, "max-size", 0, "only packs of at most this physical size")
	command.Flags().Float64Var(&options.UnusedRatioAbove, "unused-ratio-above", 0, "only packs whose accounted unused ratio exceeds this value")
	command.Flags().BoolVar(&options.RetentionExpired, "retention-expired", false, "only packs whose known minimum retention has passed")
	command.Flags().BoolVar(&options.RetentionUnknown, "retention-unknown", false, "only packs with no trustworthy retention deadline")
	command.Flags().BoolVar(&options.DeletePending, "delete-pending", false, "only packs awaiting physical deletion")
	command.Flags().StringVar(&options.Sort, "sort", "id", "order by id, size, created, unused, unused-ratio, or delete-after")
	command.Flags().UintVar(&options.Limit, "limit", 0, "return at most this many packs (zero is unlimited)")
	command.Flags().BoolVar(&options.CountOnly, "count-only", false, "report only how many packs matched")
	return command
}

func runIndexPacks(ctx context.Context, options indexPacksOptions, globalOptions global.Options, term ui.Terminal) (maintenance.PacksResult, error) {
	var result maintenance.PacksResult
	filter := maintenance.PackFilter{
		Tier: options.Tier, Type: options.Type, State: options.State,
		MinSize: options.MinSize, MaxSize: options.MaxSize,
		UnusedRatioAbove: options.UnusedRatioAbove, RetentionExpired: options.RetentionExpired,
		RetentionUnknown: options.RetentionUnknown, DeletePending: options.DeletePending,
		Sort: options.Sort, Limit: options.Limit, CountOnly: options.CountOnly,
	}
	var err error
	if filter.CreatedBefore, err = parseOptionalTime(options.CreatedBefore, "--created-before"); err != nil {
		return result, err
	}
	if filter.CreatedAfter, err = parseOptionalTime(options.CreatedAfter, "--created-after"); err != nil {
		return result, err
	}

	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
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

	result, err = maintenance.QueryPacks(ctx, store, filter)
	if err != nil {
		return result, err
	}
	if !globalOptions.JSON {
		printer.P("scanned %d packs; matched %d; returned %d\n", result.Scanned, result.Matched, result.Returned)
		printer.P("unknown tier %d; unknown type %d; retention unknown %d; usage unaccounted %d\n",
			result.UnknownTierPacks, result.UnknownTypePacks, result.RetentionUnknownPacks, result.UsageUnaccountedPacks)
		if result.Undecidable != 0 {
			// "matched 0" and "could not tell for 40" are different answers.
			printer.P("undecidable %d (creation time %d; retention %d; usage %d): these packs lack the fact the filter asks about and were excluded for lack of evidence\n",
				result.Undecidable, result.UndecidableCreatedTime,
				result.UndecidableRetention, result.UndecidableUsage)
		}
		for _, entry := range result.Packs {
			printer.P("  %s type=%s tier=%s state=%s physical=%d payload=%d unused-ratio=%.4f\n",
				entry.ID, entry.Type, entry.Tier, entry.State, entry.PhysicalSize, entry.PayloadSize, entry.UnusedRatio)
		}
	}
	return result, nil
}

func parseOptionalTime(value, flag string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC3339 timestamp: %w", flag, err)
	}
	return parsed, nil
}

// --- index history ---

type indexHistoryOptions struct {
	Daemon          indexDaemonOptions
	Metric          string
	Bucket          string
	Since           string
	Until           string
	GroupBy         string
	Histogram       bool
	Forecast        bool
	AllowIncomplete bool
}

func newIndexHistoryCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHistoryOptions
	command := &cobra.Command{
		Use:               "history",
		Short:             "Report pack lifecycle history over time",
		Long:              "Report a time series over the pack history rollups. Repack and promotion churn are reported separately from net growth, because rewriting or moving data is not growth." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexHistory(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringVar(&options.Metric, "metric", "bytes", "packs, bytes, created, deleted, repacked, promoted, net-growth, or unused")
	command.Flags().StringVar(&options.Bucket, "bucket", "day", "hour, day, week, or month")
	command.Flags().StringVar(&options.Since, "since", "", "only buckets at or after this RFC3339 time")
	command.Flags().StringVar(&options.Until, "until", "", "only buckets before this RFC3339 time")
	command.Flags().StringVar(&options.GroupBy, "by", "", "break the series down by type")
	command.Flags().BoolVar(&options.Histogram, "histogram", false, "render the distribution of bucket values")
	command.Flags().BoolVar(&options.Forecast, "forecast", false, "project the trend forward by one bucket")
	command.Flags().BoolVar(&options.AllowIncomplete, "allow-incomplete", false, "allow forecasting from partial or reconstructed buckets")
	command.AddCommand(newIndexHistoryPruneCommand(globalOptions))
	return command
}

func runIndexHistory(ctx context.Context, options indexHistoryOptions, globalOptions global.Options, term ui.Terminal) (maintenance.SeriesResult, error) {
	var result maintenance.SeriesResult
	seriesOptions := maintenance.SeriesOptions{
		Metric: options.Metric, Bucket: options.Bucket, GroupBy: options.GroupBy,
		Histogram: options.Histogram, Forecast: options.Forecast, AllowIncomplete: options.AllowIncomplete,
	}
	var err error
	if seriesOptions.Since, err = parseOptionalTime(options.Since, "--since"); err != nil {
		return result, err
	}
	if seriesOptions.Until, err = parseOptionalTime(options.Until, "--until"); err != nil {
		return result, err
	}

	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
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

	result, err = maintenance.HistorySeries(ctx, store, seriesOptions)
	if err != nil {
		return result, err
	}
	if !globalOptions.JSON {
		printHistory(printer, result)
	}
	return result, nil
}

func printHistory(printer vaultic.Printer, result maintenance.SeriesResult) {
	printer.P("metric %s by %s: %d buckets (complete %d; partial %d; reconstructed %d)\n",
		result.Metric, result.Bucket, len(result.Points),
		result.CompleteBuckets, result.PartialBuckets, result.ReconstructedBuckets)
	printer.P("churn: repacked %d bytes in %d packs; promoted %d bytes in %d packs\n",
		result.BytesRepacked, result.PacksRepacked, result.BytesPromoted, result.PacksPromoted)
	for _, point := range result.Points {
		label := time.Unix(point.BucketStart, 0).UTC().Format(time.RFC3339)
		if point.Group != "" {
			printer.P("  %s %s: %d (%s)\n", label, point.Group, point.Value, point.Coverage)
			continue
		}
		printer.P("  %s: %d (%s)\n", label, point.Value, point.Coverage)
	}
	for _, bin := range result.Histogram {
		printer.P("  [%d, %d): %d\n", bin.LowerBound, bin.UpperBound, bin.Count)
	}
	if result.Forecast != nil {
		if result.Forecast.RefusedReason != "" {
			printer.P("forecast unavailable: %s\n", result.Forecast.RefusedReason)
		} else {
			printer.P("forecast: %.2f per bucket; next bucket %.2f\n",
				result.Forecast.PerBucket, result.Forecast.NextBucketValue)
		}
	}
}

// --- index history prune ---

type indexHistoryPruneOptions struct {
	Daemon      indexDaemonOptions
	KeepRaw     time.Duration
	KeepHourly  time.Duration
	KeepDaily   time.Duration
	KeepMonthly time.Duration
	DryRun      bool
}

func newIndexHistoryPruneCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHistoryPruneOptions
	command := &cobra.Command{
		Use:               "prune",
		Short:             "Roll up and truncate pack history",
		Long:              "Roll up raw pack history events into buckets, then truncate raw events and buckets past their retention. Rolling up before truncating is what allows raw events to be discarded without losing the periods they describe." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexHistoryPrune(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().DurationVar(&options.KeepRaw, "keep-raw", 0, "retain raw events for this long (zero keeps them all)")
	command.Flags().DurationVar(&options.KeepHourly, "keep-hourly", 0, "retain hourly buckets for this long")
	command.Flags().DurationVar(&options.KeepDaily, "keep-daily", 0, "retain daily buckets for this long")
	command.Flags().DurationVar(&options.KeepMonthly, "keep-monthly", 0, "retain monthly buckets for this long")
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report what retention would remove without writing")
	return command
}

func runIndexHistoryPrune(ctx context.Context, options indexHistoryPruneOptions, globalOptions global.Options, term ui.Terminal) (maintenance.HistoryRetentionResult, error) {
	var result maintenance.HistoryRetentionResult
	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, options.DryRun, printer)
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

	result, err = maintenance.PruneHistory(ctx, store, maintenance.HistoryRetentionOptions{
		KeepRaw: options.KeepRaw, KeepHourly: options.KeepHourly,
		KeepDaily: options.KeepDaily, KeepMonthly: options.KeepMonthly, DryRun: options.DryRun,
	})
	if err != nil {
		return result, err
	}
	if !globalOptions.JSON {
		printer.P("rolled up %d events into %d buckets; removed %d raw events and %d buckets\n",
			result.Rollup.EventsScanned, result.Rollup.BucketsWritten, result.RawEventsRemoved, result.BucketsRemoved)
		if result.NewRawFloor != 0 {
			printer.P("retained raw events from %s onward\n", time.Unix(int64(result.NewRawFloor), 0).UTC().Format(time.RFC3339))
		}
	}
	return result, nil
}

// --- index backends ---

type indexBackendsOptions struct {
	Daemon  indexDaemonOptions
	Compare bool
	NoList  bool
}

// BackendFileTypeCount reports objects of one file type on one backend.
type BackendFileTypeCount struct {
	FileType string `json:"file_type"`
	Objects  uint64 `json:"objects"`
	Bytes    uint64 `json:"bytes"`
}

// BackendReport describes one configured backend.
type BackendReport struct {
	Role        string                 `json:"role"`
	Location    string                 `json:"location"`
	Connections uint                   `json:"connections"`
	Listed      bool                   `json:"listed"`
	FileTypes   []BackendFileTypeCount `json:"file_types,omitempty"`
	// MinRetention is unknown until the backend registry of Phase 12 declares
	// it, and is reported as unknown rather than as zero.
	MinRetentionKnown bool `json:"min_retention_known"`
}

// BackendsResult is the versioned JSON contract of `index backends`.
type BackendsResult struct {
	SchemaVersion int             `json:"schema_version"`
	HotCold       bool            `json:"hot_cold"`
	ReducedMode   bool            `json:"reduced_mode"`
	Backends      []BackendReport `json:"backends"`

	Compared            bool     `json:"compared"`
	CatalogPacks        uint64   `json:"catalog_packs"`
	BackendPacks        uint64   `json:"backend_packs"`
	MissingOnBackend    []string `json:"missing_on_backend,omitempty"`
	UnknownToCatalog    []string `json:"unknown_to_catalog,omitempty"`
	MissingOnBackendNum uint64   `json:"missing_on_backend_count"`
	UnknownToCatalogNum uint64   `json:"unknown_to_catalog_count"`
	// ExpectedAbsent counts catalog packs whose lifecycle means the backend
	// object is supposed to be gone: awaiting deletion, deleted, or orphaned.
	// Counting those as missing would report routine deletion as data loss.
	ExpectedAbsent uint64 `json:"expected_absent"`
}

func newIndexBackendsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexBackendsOptions
	command := &cobra.Command{
		Use:               "backends",
		Short:             "Report configured backends and cross-check them against the catalog",
		Long:              "Report each configured backend and, with --compare, cross-check a backend listing against the pack catalog. Listing is opt-in because on an archival backend it has a real cost." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexBackends(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().BoolVar(&options.Compare, "compare", false, "list the backend and cross-check it against the pack catalog")
	command.Flags().BoolVar(&options.NoList, "no-list", false, "answer from the catalog only, performing no backend listing")
	return command
}

func runIndexBackends(ctx context.Context, options indexBackendsOptions, globalOptions global.Options, term ui.Terminal) (BackendsResult, error) {
	result := BackendsResult{SchemaVersion: maintenance.IntrospectSchemaVersion}
	if options.Compare && options.NoList {
		return result, fmt.Errorf("--compare requires a backend listing and cannot be combined with --no-list")
	}
	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return result, err
	}
	defer unlock()

	// Unlike the catalog commands, this one still has a truthful answer for a
	// legacy repository: the backends exist regardless of the metadata engine.
	// It runs in a reduced mode that omits every catalog-derived field and
	// says so, rather than presenting a partial answer as complete.
	slateDB := requireSlateDBRepository(repo) == nil
	result.ReducedMode = !slateDB
	if !slateDB && options.Compare {
		return result, fmt.Errorf("--compare requires a SlateDB-authoritative repository: %w", maintenance.ErrLegacyRepository)
	}

	hot, cold, hotCold := repo.HotCold()
	result.HotCold = hotCold
	targets := []backendTarget{{role: "single", lister: repo.Backend()}}
	if hotCold {
		targets = []backendTarget{{role: "hot", lister: hot}, {role: "cold", lister: cold}}
	}
	reports, err := collectBackendReports(ctx, targets, options.NoList)
	if err != nil {
		return result, err
	}
	result.Backends = reports

	if options.Compare {
		if err := compareBackendToCatalog(ctx, repo, options, &result); err != nil {
			return result, err
		}
	}
	if !globalOptions.JSON {
		printBackends(printer, result)
	}
	if result.MissingOnBackendNum != 0 {
		return result, errIndexDifferences
	}
	return result, nil
}

var backendFileTypes = []struct {
	name     string
	fileType backend.FileType
}{
	{"pack", backend.PackFile}, {"index", backend.IndexFile},
	{"snapshot", backend.SnapshotFile}, {"key", backend.KeyFile}, {"lock", backend.LockFile},
}

// backendLister is the narrow slice of backend.Backend that this command uses.
// Naming it keeps the "--no-list issues zero requests" guarantee testable
// without standing up a full backend.
type backendLister interface {
	Properties() backend.Properties
	List(ctx context.Context, fileType backend.FileType, fn func(backend.FileInfo) error) error
}

type backendTarget struct {
	role   string
	lister backendLister
}

func collectBackendReports(ctx context.Context, targets []backendTarget, noList bool) ([]BackendReport, error) {
	reports := make([]BackendReport, 0, len(targets))
	for _, target := range targets {
		report := BackendReport{Role: target.role, Connections: target.lister.Properties().Connections}
		if locator, ok := target.lister.(interface{ Location() string }); ok {
			report.Location = locator.Location()
		}
		if !noList {
			counts, err := countBackendObjects(ctx, target.lister)
			if err != nil {
				return nil, err
			}
			report.FileTypes, report.Listed = counts, true
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func countBackendObjects(ctx context.Context, target backendLister) ([]BackendFileTypeCount, error) {
	counts := make([]BackendFileTypeCount, 0, len(backendFileTypes))
	for _, entry := range backendFileTypes {
		var count BackendFileTypeCount
		count.FileType = entry.name
		err := target.List(ctx, entry.fileType, func(info backend.FileInfo) error {
			count.Objects++
			if info.Size > 0 {
				count.Bytes += uint64(info.Size)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("list %s objects: %w", entry.name, err)
		}
		counts = append(counts, count)
	}
	return counts, nil
}

func compareBackendToCatalog(ctx context.Context, repo *repository.Repository, options indexBackendsOptions, result *BackendsResult) error {
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return err
	}
	defer closeStore()

	catalog, err := maintenance.QueryPacks(ctx, store, maintenance.PackFilter{})
	if err != nil {
		return err
	}
	known := make(map[string]struct{}, len(catalog.Packs))
	var expectedAbsent uint64
	for _, entry := range catalog.Packs {
		if !packShouldExistOnBackend(entry.State) {
			expectedAbsent++
			continue
		}
		known[entry.ID] = struct{}{}
	}
	result.CatalogPacks = uint64(len(known))
	result.ExpectedAbsent = expectedAbsent

	present := make(map[string]struct{}, len(known))
	err = repo.List(ctx, vaultic.PackFile, func(id vaultic.ID, _ int64) error {
		present[id.String()] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	result.BackendPacks = uint64(len(present))
	result.Compared = true

	result.MissingOnBackend, result.UnknownToCatalog = diffCatalogAgainstListing(known, present)
	result.MissingOnBackendNum = uint64(len(result.MissingOnBackend))
	result.UnknownToCatalogNum = uint64(len(result.UnknownToCatalog))
	return nil
}

// packShouldExistOnBackend reports whether a pack's lifecycle state implies a
// backend object. A pack awaiting deletion, already deleted, or recorded as
// orphaned is absent by design, and an unknown state is not evidence that an
// object should be there.
func packShouldExistOnBackend(state string) bool {
	switch state {
	case "imported", "published", "export-pending":
		return true
	}
	return false
}

// diffCatalogAgainstListing reports the two asymmetric findings separately: a
// pack the catalog claims but the backend does not hold is data loss, while a
// pack the backend holds but the catalog does not know is only waste. Folding
// them into one number would hide that difference.
func diffCatalogAgainstListing(known, present map[string]struct{}) (missing, unknown []string) {
	for id := range known {
		if _, found := present[id]; !found {
			missing = append(missing, id)
		}
	}
	for id := range present {
		if _, found := known[id]; !found {
			unknown = append(unknown, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(unknown)
	return missing, unknown
}

func printBackends(printer vaultic.Printer, result BackendsResult) {
	if result.ReducedMode {
		printer.P("reduced mode: this repository has no SlateDB pack catalog, so only backend facts are reported\n")
	}
	for _, report := range result.Backends {
		printer.P("%s backend: %s (connections %d)\n", report.Role, report.Location, report.Connections)
		if !report.Listed {
			printer.P("  not listed (--no-list)\n")
		}
		for _, count := range report.FileTypes {
			printer.P("  %s: %d objects, %d bytes\n", count.FileType, count.Objects, count.Bytes)
		}
		if !report.MinRetentionKnown {
			printer.P("  minimum retention: unknown\n")
		}
	}
	if result.Compared {
		printer.P("catalog packs %d; backend packs %d; missing on backend %d; unknown to catalog %d; expected absent %d\n",
			result.CatalogPacks, result.BackendPacks, result.MissingOnBackendNum,
			result.UnknownToCatalogNum, result.ExpectedAbsent)
		for _, id := range result.MissingOnBackend {
			printer.E("missing on backend: %s\n", id)
		}
		for _, id := range result.UnknownToCatalog {
			printer.P("unknown to catalog: %s\n", id)
		}
	}
}
