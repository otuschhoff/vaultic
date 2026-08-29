package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/legacyimport"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type indexDaemonOptions struct {
	Socket       string
	TCPAddress   string
	TCPAllowlist []string
	AuthToken    string
	DaemonPath   string
	DataDir      string
	ObjectStore  string
	S3Bucket     string
	S3Prefix     string
	Start        bool
	Persistent   bool
}

var errIndexDifferences = errors.New("metadata indexes differ")
var errIndexIncomplete = errors.New("metadata index workflow incomplete")

const indexExitStatus = `

EXIT STATUS
===========

Exit status is 0 if the command completed successfully.
Exit status is 1 for a fatal workflow or data error.
Exit status is 2 if import is partial or metadata differences are found.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`

func (options *indexDaemonOptions) AddFlags(flags *pflag.FlagSet) {
	flags.StringVar(&options.Socket, "daemon-socket", "", "vaulticdb Unix socket (repository-scoped default)")
	flags.StringVar(&options.TCPAddress, "daemon-tcp-address", "", "vaulticdb TCP address (opt-in)")
	flags.StringSliceVar(&options.TCPAllowlist, "daemon-tcp-allow", nil, "CIDR allowed to connect to a started TCP daemon")
	flags.StringVar(&options.AuthToken, "daemon-auth-token", "", "authentication token for an opt-in TCP daemon")
	flags.BoolVar(&options.Start, "start-daemon", false, "start vaulticdb when it is not already running")
	flags.BoolVar(&options.Persistent, "persistent-daemon", false, "leave a daemon started by this command running")
	flags.StringVar(&options.DaemonPath, "daemon-path", "vaulticdb", "path to vaulticdb when --start-daemon is set")
	flags.StringVar(&options.DataDir, "daemon-data-dir", "", "local vaulticdb data directory")
	flags.StringVar(&options.ObjectStore, "daemon-object-store", "", "vaulticdb object store: local, memory, or s3")
	flags.StringVar(&options.S3Bucket, "daemon-s3-bucket", "", "vaulticdb S3 bucket")
	flags.StringVar(&options.S3Prefix, "daemon-s3-prefix", "", "vaulticdb S3 key prefix")
}

func (options indexDaemonOptions) connect(ctx context.Context, repositoryID string) (*daemon.Client, error) {
	if options.TCPAddress == "" && (len(options.TCPAllowlist) != 0 || options.AuthToken != "") {
		return nil, fmt.Errorf("--daemon-tcp-allow and --daemon-auth-token require --daemon-tcp-address")
	}
	if options.TCPAddress != "" && options.Socket != "" {
		return nil, fmt.Errorf("--daemon-socket and --daemon-tcp-address are mutually exclusive")
	}
	config, err := options.config(repositoryID)
	if err != nil {
		return nil, err
	}
	if options.Start {
		return daemon.Ensure(ctx, config)
	}
	return daemon.Connect(ctx, config)
}

func (options indexDaemonOptions) config(repositoryID string) (daemon.Options, error) {
	if options.TCPAddress == "" && (len(options.TCPAllowlist) != 0 || options.AuthToken != "") {
		return daemon.Options{}, fmt.Errorf("--daemon-tcp-allow and --daemon-auth-token require --daemon-tcp-address")
	}
	if options.TCPAddress != "" && options.Socket != "" {
		return daemon.Options{}, fmt.Errorf("--daemon-socket and --daemon-tcp-address are mutually exclusive")
	}
	if options.Persistent && !options.Start {
		return daemon.Options{}, fmt.Errorf("--persistent-daemon requires --start-daemon")
	}
	config := daemon.Options{
		Socket: options.Socket, TCPAddress: options.TCPAddress, TCPAllowlist: options.TCPAllowlist,
		AuthToken: options.AuthToken, RepositoryID: repositoryID, DataDir: options.DataDir,
		ObjectStore: options.ObjectStore, S3Bucket: options.S3Bucket, S3Prefix: options.S3Prefix,
	}
	if options.Start {
		config.DaemonPath = options.DaemonPath
		config.PersistentDaemon = options.Persistent
	}
	return config, nil
}

func newIndexCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{
		Use:               "index",
		Short:             "Import, export, and verify metadata indexes",
		Long:              "The index command provides operator-controlled SlateDB migration, compatibility export, differential verification, and aggregate repair workflows.",
		GroupID:           cmdGroupAdvanced,
		DisableAutoGenTag: true,
	}
	command.AddCommand(
		newIndexImportCommand(globalOptions),
		newIndexExportCommand(globalOptions),
		newIndexCheckCommand(globalOptions),
		newIndexRebuildPackStatsCommand(globalOptions),
		newIndexGCCommand(globalOptions),
		newIndexStatsCommand(globalOptions),
		newIndexPacksCommand(globalOptions),
		newIndexHistoryCommand(globalOptions),
		newIndexBackendsCommand(globalOptions),
	)
	return command
}

type indexImportOptions struct {
	Daemon             indexDaemonOptions
	Resume             bool
	DryRun             bool
	Activate           bool
	FromLegacy         bool
	BatchSize          uint32
	MaxErrors          uint64
	WorkBudget         uint64
	SnapshotDepth      uint
	SnapshotWorkBudget uint64
}

func newIndexImportCommand(globalOptions *global.Options) *cobra.Command {
	var options indexImportOptions
	command := &cobra.Command{
		Use:               "import",
		Short:             "Import legacy JSON indexes into SlateDB",
		Long:              "Import legacy JSON indexes and snapshot metadata into SlateDB using durable per-source checkpoints. Findings are reported without abandoning successfully imported sources." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexImport(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	flags := command.Flags()
	options.Daemon.AddFlags(flags)
	flags.BoolVar(&options.Resume, "resume", true, "skip sources with durable import checkpoints")
	flags.BoolVar(&options.DryRun, "dry-run", false, "scan and validate without writing SlateDB")
	flags.BoolVar(&options.Activate, "activate", false, "make SlateDB authoritative after a complete import")
	flags.BoolVar(&options.FromLegacy, "from-legacy", true, "import from legacy JSON indexes")
	flags.Uint32Var(&options.BatchSize, "batch-size", 0, "maximum mutations per daemon transaction batch (zero uses daemon limit)")
	flags.Uint64Var(&options.MaxErrors, "max-errors", 0, "stop after this many source errors (zero is unlimited)")
	flags.Uint64Var(&options.WorkBudget, "work-budget", 0, "maximum blob records to examine (zero is unlimited)")
	flags.UintVar(&options.SnapshotDepth, "snapshot-depth", math.MaxUint, "maximum tree depth to import (zero disables snapshot import)")
	flags.Uint64Var(&options.SnapshotWorkBudget, "snapshot-work-budget", 0, "maximum snapshot nodes to examine (zero is unlimited)")
	return command
}

func runIndexImport(ctx context.Context, options indexImportOptions, globalOptions global.Options, term ui.Terminal) (legacyimport.Result, error) {
	var result legacyimport.Result
	if !options.FromLegacy {
		return result, fmt.Errorf("no import source selected; --from-legacy is currently required")
	}
	if options.DryRun && options.Activate {
		return result, fmt.Errorf("--activate cannot be combined with --dry-run")
	}
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
	store, client, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return result, fmt.Errorf("connect vaulticdb: %w", err)
	}
	keepStore := false
	defer func() {
		if !keepStore {
			closeStore()
		}
	}()
	if options.SnapshotDepth > 0 || options.SnapshotWorkBudget > 0 {
		if err := repo.LoadIndex(ctx, printer); err != nil {
			return result, fmt.Errorf("load source indexes for snapshot import: %w", err)
		}
	}
	result, err = legacyimport.Import(ctx, repo, repo.Backend(), store, legacyimport.Options{
		Resume: options.Resume, DryRun: options.DryRun, MaxErrors: options.MaxErrors,
		BatchSize:  options.BatchSize,
		WorkBudget: options.WorkBudget, SnapshotDepth: options.SnapshotDepth,
		SnapshotWorkBudget: options.SnapshotWorkBudget,
	})
	if err != nil && !errors.Is(err, legacyimport.ErrLimitReached) {
		return result, err
	}
	if options.Activate {
		if errors.Is(err, legacyimport.ErrLimitReached) || result.ErrorsSeen != 0 {
			return result, fmt.Errorf("cannot activate an incomplete import")
		}
		if client == nil {
			return result, fmt.Errorf("repository is already SlateDB-authoritative")
		}
		if activateErr := repo.EnableSlateDBAuthority(ctx, client); activateErr != nil {
			return result, fmt.Errorf("activate SlateDB authority: %w", activateErr)
		}
		keepStore = true
	}
	if !globalOptions.JSON {
		printer.P("imported %d indexes, %d packs, %d blobs, and %d snapshots\n", result.IndexesImported, result.PacksImported, result.BlobsImported, result.SnapshotsImported)
		for _, finding := range result.Findings {
			printer.E("%s %s: %s\n", finding.Stage, strings.TrimSpace(finding.SourceID.Str()), finding.Error)
		}
	}
	if errors.Is(err, legacyimport.ErrLimitReached) {
		return result, fmt.Errorf("%w: work or error limit reached", errIndexIncomplete)
	}
	if result.ErrorsSeen != 0 {
		return result, fmt.Errorf("%w: import completed with %d findings", errIndexIncomplete, result.ErrorsSeen)
	}
	return result, nil
}

func openIndexStore(ctx context.Context, repo *repository.Repository, options indexDaemonOptions) (*daemon.SchemaStore, *daemon.Client, func(), error) {
	if engine, ok := repo.Engine().(*metadataindex.DaemonEngine); ok {
		return engine.SchemaStore(), nil, func() {}, nil
	}
	client, err := options.connect(ctx, repo.Config().ID)
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("connect vaulticdb (use --start-daemon to start one): %w", err)
	}
	return daemon.NewSchemaStore(client), client, func() { _ = client.Close(context.Background()) }, nil
}

type indexExportOptions struct {
	Daemon        indexDaemonOptions
	Full          bool
	DryRun        bool
	Verify        bool
	Since         uint64
	PacksPerIndex uint
}

func newIndexExportCommand(globalOptions *global.Options) *cobra.Command {
	var options indexExportOptions
	command := &cobra.Command{
		Use: "export", Short: "Export SlateDB metadata as legacy JSON indexes", Long: "Export authoritative blob locations in canonical Restic JSON index format. By default only packs without completed export checkpoints are written; --full writes every live pack." + indexExitStatus, Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexExport(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().BoolVar(&options.Full, "full", false, "export all packs instead of only packs without export checkpoints")
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report export work without writing JSON indexes")
	command.Flags().BoolVar(&options.Verify, "verify", false, "read back and verify each exported JSON index")
	command.Flags().Uint64Var(&options.Since, "since", 0, "export packs recorded after this export sequence")
	command.Flags().UintVar(&options.PacksPerIndex, "packs-per-index", 1_000, "maximum packs per JSON index")
	return command
}

func runIndexExport(ctx context.Context, options indexExportOptions, globalOptions global.Options, term ui.Terminal) (maintenance.ExportResult, error) {
	var result maintenance.ExportResult
	if options.Full && options.Since != 0 {
		return result, fmt.Errorf("--full and --since are mutually exclusive")
	}
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
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return result, err
	}
	defer closeStore()
	result, err = maintenance.Export(ctx, store, repo, maintenance.ExportOptions{Full: options.Full, DryRun: options.DryRun, Verify: options.Verify, Since: options.Since, PacksPerIndex: options.PacksPerIndex})
	if err == nil && !globalOptions.JSON {
		printer.P("selected %d packs and %d blobs; wrote %d JSON indexes; export sequence %d\n", result.PacksSelected, result.BlobsSelected, result.IndexesWritten, result.ExportSequence)
	}
	return result, err
}

type indexCheckOptions struct {
	Daemon           indexDaemonOptions
	MaxFindings      uint
	LegacyOnly       bool
	SlateDBOnly      bool
	IncludeCrawlDebt bool
	FailOnWarning    bool
}

func newIndexCheckCommand(globalOptions *global.Options) *cobra.Command {
	var options indexCheckOptions
	command := &cobra.Command{
		Use: "check", Short: "Compare legacy JSON and SlateDB metadata", Long: "Compare deduplicated physical blob locations and pack catalogs between legacy JSON and SlateDB, and verify all pack aggregate records." + indexExitStatus, Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexCheck(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().UintVar(&options.MaxFindings, "max-findings", 100, "maximum detailed differences in the summary (zero is unlimited)")
	command.Flags().BoolVar(&options.LegacyOnly, "legacy-only", false, "validate only legacy JSON indexes")
	command.Flags().BoolVar(&options.SlateDBOnly, "slatedb-only", false, "validate only SlateDB metadata")
	command.Flags().BoolVar(&options.IncludeCrawlDebt, "include-crawl-debt", false, "include individual pending crawl-debt findings")
	command.Flags().BoolVar(&options.FailOnWarning, "fail-on-warning", false, "return exit status 2 for expected incompleteness warnings")
	return command
}

func runIndexCheck(ctx context.Context, options indexCheckOptions, globalOptions global.Options, term ui.Terminal) (maintenance.CheckResult, error) {
	var result maintenance.CheckResult
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
	var store *daemon.SchemaStore
	closeStore := func() {}
	if !options.LegacyOnly {
		store, _, closeStore, err = openIndexStore(ctx, repo, options.Daemon)
		if err != nil {
			return result, err
		}
	}
	defer closeStore()
	placementModel, err := indexMaintenancePlacementModel(repo)
	if err != nil {
		return result, err
	}
	result, err = maintenance.CheckWithOptions(ctx, repo, store, maintenance.CheckOptions{LegacyOnly: options.LegacyOnly, SlateDBOnly: options.SlateDBOnly, IncludeCrawlDebt: options.IncludeCrawlDebt, MaxFindings: options.MaxFindings, PlacementModel: placementModel})
	if err != nil {
		return result, err
	}
	if !globalOptions.JSON {
		printer.P("legacy locations: %d; SlateDB locations: %d; differences: %d; aggregate mismatches: %d\n", result.LegacyLocations, result.SlateDBLocations, result.MissingInSlateDB+result.MissingInLegacy, result.AggregateMismatch)
		printer.P("packs: unknown tier %d; retention unknown %d; usage unaccounted %d\n", result.UnknownTierPacks, result.RetentionUnknownPacks, result.UsageUnaccountedPacks)
		printer.P("placements: missing %d; reverse mismatches %d; tier mismatches %d; below durability %d; unknown backends %d\n",
			result.MissingPlacementRecords, result.BackendPackMismatch, result.DerivedTierMismatch,
			result.PacksBelowDurability, result.UnknownPlacementBackends)
		if result.TierAggregatesUnbuilt {
			printer.P("per-tier aggregates have not been built for this repository yet; run 'vaultic index rebuild-pack-stats'\n")
		}
		for _, finding := range result.Findings {
			printer.E("%s %s", finding.Kind, finding.Key)
			if finding.Want != "" || finding.Got != "" {
				printer.E(" (want %s, got %s)", finding.Want, finding.Got)
			}
			printer.E("\n")
		}
	}
	if !result.Clean() || (options.FailOnWarning && result.HasWarnings()) {
		return result, errIndexDifferences
	}
	return result, nil
}

func indexMaintenancePlacementModel(repo *repository.Repository) (maintenance.PlacementModel, error) {
	model, err := repo.PlacementModel()
	if err != nil {
		return maintenance.PlacementModel{}, err
	}
	converted := maintenance.PlacementModel{
		Policy: maintenance.DurabilityPolicy{
			MinCopies:  model.Policy.MinCopies,
			MinDomains: model.Policy.MinDomains,
			MinOffsite: model.Policy.MinOffsite,
		},
	}
	converted.Backends = make([]maintenance.PlacementBackend, 0, len(model.Backends))
	for _, backend := range model.Backends {
		converted.Backends = append(converted.Backends, maintenance.PlacementBackend{
			ID: backend.ID, Hash: backend.Hash, Role: backend.Role,
			Offsite: backend.Offsite, FailureDomain: backend.FailureDomain,
		})
	}
	return converted, nil
}

type indexRebuildPackStatsOptions struct {
	Daemon indexDaemonOptions
	DryRun bool
}

func newIndexRebuildPackStatsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexRebuildPackStatsOptions
	command := &cobra.Command{
		Use: "rebuild-pack-stats", Short: "Rebuild SlateDB pack aggregates", Long: "Recalculate every pack aggregate from the authoritative pack catalog and replace all aggregate records atomically when drift is present." + indexExitStatus, Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexRebuildPackStats(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "calculate aggregate changes without writing them")
	return command
}

func runIndexRebuildPackStats(ctx context.Context, options indexRebuildPackStatsOptions, globalOptions global.Options, term ui.Terminal) (maintenance.RebuildResult, error) {
	var result maintenance.RebuildResult
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
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return result, err
	}
	defer closeStore()
	placementModel, err := indexMaintenancePlacementModel(repo)
	if err != nil {
		return result, err
	}
	result.PlacementRecordsChanged, err = maintenance.RebuildPlacementRecords(ctx, store, placementModel, options.DryRun)
	if err != nil {
		return result, err
	}
	result.TierSummaryChanged, err = maintenance.RebuildDerivedTierSummary(ctx, store, placementModel, options.DryRun)
	if err != nil {
		return result, err
	}
	placementChanged := result.PlacementRecordsChanged
	tierSummaryChanged := result.TierSummaryChanged
	result, err = maintenance.RebuildPackAggregates(ctx, store, options.DryRun)
	if err != nil {
		return result, err
	}
	result.PlacementRecordsChanged = placementChanged
	result.TierSummaryChanged = tierSummaryChanged
	result.BackendPackRecordsChanged, err = maintenance.RebuildBackendPackIndex(ctx, store, options.DryRun)
	if err == nil && !globalOptions.JSON {
		printer.P("scanned %d packs; changed %d aggregate records, %d placement records, %d tier summaries, %d backend-pack records\n",
			result.PacksScanned, result.AggregatesChanged, result.PlacementRecordsChanged,
			result.TierSummaryChanged, result.BackendPackRecordsChanged)
		for _, delta := range result.Deltas {
			printer.P("  %s: packs %d, payload %d\n", delta.Key, delta.After.PackCount, delta.After.PayloadSize)
		}
	}
	return result, err
}

type indexGCOptions struct {
	Daemon          indexDaemonOptions
	DryRun          bool
	DiscoverOnly    bool
	MinCandidateAge time.Duration
}

func newIndexGCCommand(globalOptions *global.Options) *cobra.Command {
	var options indexGCOptions
	command := &cobra.Command{
		Use: "gc", Short: "Discover, revalidate, and sweep unreachable SlateDB packs",
		Long: "Discover GC candidates from reverse references and the pack catalog, re-walk every " +
			"retained snapshot root to confirm reachability, then delete wholly unreachable packs and " +
			"repack packs that mix live and unreachable blobs. A failed physical deletion leaves the " +
			"pack visible as delete-pending and is retried on the next run. Any freed or repacked pack " +
			"automatically triggers a full re-export and removes now-stale legacy JSON indexes, so " +
			"compatibility artifacts never reference a deleted pack. --discover-only records " +
			"candidates cheaply from reverse references without the snapshot walk or any deletion." + indexExitStatus,
		Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexGC(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report the GC plan without repacking or deleting anything")
	command.Flags().BoolVar(&options.DiscoverOnly, "discover-only", false, "record candidate blobs from reverse references without the snapshot walk or any deletion")
	command.Flags().DurationVar(&options.MinCandidateAge, "min-candidate-age", 0, "require a candidate to have been continuously unreachable for at least this long before sweeping it")
	return command
}

func runIndexGC(ctx context.Context, options indexGCOptions, globalOptions global.Options, term ui.Terminal) (repository.GCStats, error) {
	var result repository.GCStats
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
	if !options.DiscoverOnly {
		if err := repo.LoadIndex(ctx, printer); err != nil {
			return result, fmt.Errorf("load legacy index for reachability walk: %w", err)
		}
	}
	plan, err := repository.PlanGC(ctx, repository.GCOptions{
		DryRun: options.DryRun, DiscoverOnly: options.DiscoverOnly, MinCandidateAge: options.MinCandidateAge,
	}, repo, printer)
	if err != nil {
		return result, err
	}
	if err := plan.Execute(ctx, printer); err != nil {
		return plan.Stats, err
	}
	result = plan.Stats
	if !options.DryRun && (result.PacksDeleted != 0 || result.PacksRepacked != 0) {
		store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
		if err != nil {
			return result, fmt.Errorf("refresh legacy compatibility projection: %w", err)
		}
		defer closeStore()
		if _, err := maintenance.Export(ctx, store, repo, maintenance.ExportOptions{Full: true}); err != nil {
			return result, fmt.Errorf("refresh legacy compatibility projection: %w", err)
		}
		if _, err := repository.PruneStaleLegacyIndexes(ctx, repo); err != nil {
			return result, fmt.Errorf("prune stale legacy indexes: %w", err)
		}
	}
	if !globalOptions.JSON {
		printer.P("scanned %d packs and %d blobs; whole=%d mixed=%d pending-age=%d pending-retries=%d; deleted=%d (of which retried=%d) repacked=%d retry-failed=%d\n",
			result.PacksScanned, result.BlobsScanned, result.WholePackCandidates, result.MixedPackCandidates, result.PendingAge, result.PendingRetries,
			result.PacksDeleted, result.PacksRetried, result.PacksRepacked, result.PacksRetryFailed)
		if result.PacksAccounted != 0 || result.PacksUnaccountable != 0 {
			printer.P("refreshed usage accounting for %d packs; %d left unaccounted\n", result.PacksAccounted, result.PacksUnaccountable)
		}
	}
	if result.PacksRetryFailed != 0 {
		return result, fmt.Errorf("%w: %d packs remain delete-pending after a failed retry", errIndexIncomplete, result.PacksRetryFailed)
	}
	return result, nil
}
