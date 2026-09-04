package indexcmd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/legacyimport"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type indexDaemonOptions struct {
	Socket            string
	TCPAddress        string
	TCPAllowlist      []string
	AuthTokenFile     string
	DaemonPath        string
	DataDir           string
	ObjectStore       string
	S3Bucket          string
	S3Prefix          string
	EncryptionMode    string
	PassphraseFile    string
	AzureTokenFile    string
	GCPTokenFile      string
	VaultTokenFile    string
	PKCS11PINFile     string
	RecoveryUnlock    bool
	BrokerSocket      string
	BrokerManifest    string
	BrokerLease       time.Duration
	RebuildInitialize bool
	Start             bool
	Persistent        bool
}

var ErrDifferences = errors.New("metadata indexes differ")
var ErrIncomplete = errors.New("metadata index workflow incomplete")

var errIndexDifferences = ErrDifferences
var errIndexIncomplete = ErrIncomplete

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
	flags.StringVar(&options.AuthTokenFile, "daemon-auth-token-file", "", "protected authentication-token file for an opt-in TCP daemon")
	flags.BoolVar(&options.Start, "start-daemon", false, "start vaulticdb when it is not already running")
	flags.BoolVar(&options.Persistent, "persistent-daemon", false, "leave a daemon started by this command running")
	flags.StringVar(&options.DaemonPath, "daemon-path", "vaulticdb", "path to vaulticdb when --start-daemon is set")
	flags.StringVar(&options.DataDir, "daemon-data-dir", "", "local vaulticdb data directory")
	flags.StringVar(&options.ObjectStore, "daemon-object-store", "", "vaulticdb object store: local, memory, or s3")
	flags.StringVar(&options.S3Bucket, "daemon-s3-bucket", "", "vaulticdb S3 bucket")
	flags.StringVar(&options.S3Prefix, "daemon-s3-prefix", "", "vaulticdb S3 key prefix")
	flags.StringVar(&options.EncryptionMode, "metadata-encryption", "", "metadata encryption mode: off, required, or initialize")
	flags.StringVar(&options.PassphraseFile, "metadata-recovery-passphrase-file", "", "file containing the metadata recovery passphrase")
	flags.StringVar(&options.AzureTokenFile, "metadata-azure-token-file", "", "protected Azure Key Vault bearer-token file")
	flags.StringVar(&options.GCPTokenFile, "metadata-gcp-token-file", "", "protected Google Cloud KMS bearer-token file")
	flags.StringVar(&options.VaultTokenFile, "metadata-vault-token-file", "", "protected Vault Transit token file")
	flags.StringVar(&options.PKCS11PINFile, "metadata-pkcs11-pin-file", "", "protected PKCS#11 user PIN file")
	flags.BoolVar(&options.RecoveryUnlock, "metadata-recovery-unlock", false, "acknowledge use of a recovery slot while cloud slots exist")
	flags.StringVar(&options.BrokerSocket, "metadata-key-broker-socket", "", "local key-broker socket for the vaulticdb metadata-DEK lease")
	flags.StringVar(&options.BrokerManifest, "metadata-key-broker-release-manifest", "", "signed release manifest authorizing vaulticdb")
	flags.DurationVar(&options.BrokerLease, "metadata-key-broker-lease", time.Hour, "vaulticdb metadata-DEK lease lifetime")
	flags.BoolVar(&options.RebuildInitialize, "metadata-rebuild-initialize", false, "initialize an empty candidate metadata store under a broker-leased DEK")
}

func (options indexDaemonOptions) connect(ctx context.Context, repositoryID string) (*daemon.Client, error) {
	if options.TCPAddress == "" && (len(options.TCPAllowlist) != 0 || options.AuthTokenFile != "") {
		return nil, fmt.Errorf("--daemon-tcp-allow and --daemon-auth-token-file require --daemon-tcp-address")
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
	if options.TCPAddress == "" && (len(options.TCPAllowlist) != 0 || options.AuthTokenFile != "") {
		return daemon.Options{}, fmt.Errorf("--daemon-tcp-allow and --daemon-auth-token-file require --daemon-tcp-address")
	}
	if options.TCPAddress != "" && options.Socket != "" {
		return daemon.Options{}, fmt.Errorf("--daemon-socket and --daemon-tcp-address are mutually exclusive")
	}
	if options.Persistent && !options.Start {
		return daemon.Options{}, fmt.Errorf("--persistent-daemon requires --start-daemon")
	}
	var authToken string
	if options.AuthTokenFile != "" {
		value, err := readProtectedBinary(options.AuthTokenFile, "vaulticdb TCP authentication token", true)
		if err != nil {
			return daemon.Options{}, err
		}
		defer clear(value)
		if len(value) == 0 {
			return daemon.Options{}, errors.New("vaulticdb TCP authentication token is empty")
		}
		authToken = string(value)
	}
	config := daemon.Options{
		Socket: options.Socket, TCPAddress: options.TCPAddress, TCPAllowlist: options.TCPAllowlist,
		AuthToken: authToken, RepositoryID: repositoryID, DataDir: options.DataDir,
		DaemonPath:  options.DaemonPath,
		ObjectStore: options.ObjectStore, S3Bucket: options.S3Bucket, S3Prefix: options.S3Prefix,
		EncryptionMode: options.EncryptionMode, PassphraseFile: options.PassphraseFile,
		AzureTokenFile: options.AzureTokenFile,
		GCPTokenFile:   options.GCPTokenFile,
		VaultTokenFile: options.VaultTokenFile,
		PKCS11PINFile:  options.PKCS11PINFile,
		RecoveryUnlock: options.RecoveryUnlock,
		BrokerSocket:   options.BrokerSocket, BrokerManifest: options.BrokerManifest,
		BrokerLease: options.BrokerLease, RebuildInitialize: options.RebuildInitialize,
	}
	if options.Start {
		config.PersistentDaemon = options.Persistent
	}
	return config, nil
}

func (options indexDaemonOptions) Config(repositoryID string) (daemon.Options, error) {
	return options.config(repositoryID)
}

func (options indexDaemonOptions) Finalize() error {
	_, err := options.config("")
	return err
}

func NewCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{
		Use:   "index",
		Short: "Import, export, and verify metadata indexes",
		Long: "The index command provides operator-controlled SlateDB migration, compatibility export, " +
			"differential verification, and aggregate repair workflows.",
		GroupID:           "advanced",
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
		newIndexPlacementCommand(globalOptions),
		newIndexFileHistoryCommand(globalOptions),
		newIndexPathAtCommand(globalOptions),
		newIndexPathIndexCommand(globalOptions),
		newIndexAnalyticsCommand(globalOptions),
		newIndexGrowthCommand(globalOptions),
		newIndexUserStatsCommand(globalOptions),
		newIndexGDPRCommand(globalOptions),
		newIndexVerifyStorageCommand(globalOptions),
		newIndexKeysCommand(globalOptions),
		newIndexUnlockCommand(globalOptions),
		newIndexEncryptCommand(globalOptions),
		newIndexWriterCommand(globalOptions),
		newIndexStagingCommand(globalOptions),
		newIndexHealCommand(globalOptions),
	)
	return command
}

type indexImportOptions struct {
	Daemon                     indexDaemonOptions
	Resume                     bool
	DryRun                     bool
	Activate                   bool
	FromLegacy                 bool
	BatchSize                  uint32
	MaxErrors                  uint64
	WorkBudget                 uint64
	SnapshotDepth              uint
	SnapshotWorkBudget         uint64
	ConfirmMetadataLossRebuild bool
}

func newIndexImportCommand(globalOptions *global.Options) *cobra.Command {
	var options indexImportOptions
	command := &cobra.Command{
		Use:   "import",
		Short: "Import legacy JSON indexes into SlateDB",
		Long: "Import legacy JSON indexes and snapshot metadata into SlateDB using durable per-source checkpoints. " +
			"Findings are reported without abandoning successfully imported sources." + indexExitStatus,
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
	flags.BoolVar(
		&options.ConfirmMetadataLossRebuild,
		"confirm-metadata-loss-rebuild",
		false,
		"acknowledge replacement of lost or suspect authoritative metadata after candidate validation",
	)
	return command
}

func runIndexImport(
	ctx context.Context,
	options indexImportOptions,
	globalOptions global.Options,
	term ui.Terminal,
) (result legacyimport.Result, err error) {
	if !options.FromLegacy {
		return result, fmt.Errorf("no import source selected; --from-legacy is currently required")
	}
	if options.DryRun && options.Activate {
		return result, fmt.Errorf("--activate cannot be combined with --dry-run")
	}
	if err := prepareMetadataRebuild(ctx, options, globalOptions); err != nil {
		return result, err
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
	storeSession, err := openStoreSession(ctx, repo, options.Daemon)
	if err != nil {
		return result, fmt.Errorf("connect vaulticdb: %w", err)
	}
	store, client := storeSession.Store, storeSession.Client
	defer func() {
		err = errors.Join(err, storeSession.Close())
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
		if activateErr := activateImportedIndex(ctx, options, repo, store, client, result, err); activateErr != nil {
			return result, activateErr
		}
	}
	if !globalOptions.JSON {
		printIndexImportResult(printer, result)
	}
	if errors.Is(err, legacyimport.ErrLimitReached) {
		return result, fmt.Errorf("%w: work or error limit reached", errIndexIncomplete)
	}
	if result.ErrorsSeen != 0 {
		return result, fmt.Errorf("%w: import completed with %d findings", errIndexIncomplete, result.ErrorsSeen)
	}
	return result, nil
}

func prepareMetadataRebuild(ctx context.Context, options indexImportOptions, globalOptions global.Options) error {
	if !options.Daemon.RebuildInitialize {
		return nil
	}
	if !options.ConfirmMetadataLossRebuild || !options.Daemon.Start || !globalOptions.MetadataLossRecovery {
		return fmt.Errorf("metadata rebuild initialization requires --confirm-metadata-loss-rebuild, --start-daemon, and --metadata-loss-recovery")
	}
	candidate, err := validateMetadataRebuildTarget(options.Daemon)
	if err != nil {
		return err
	}
	if globalOptions.KeyBrokerSocket == "" || options.Daemon.BrokerSocket != globalOptions.KeyBrokerSocket {
		return fmt.Errorf("metadata rebuild requires the repository and candidate daemon to use the same key broker")
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryIntegrity, Component: "index",
		Message: "authenticated metadata rebuild started", Fields: map[string]any{"candidate": candidate}})
	return nil
}

func activateImportedIndex(ctx context.Context, options indexImportOptions, repo *repository.Repository, store *daemon.SchemaStore,
	client *daemon.Client, result legacyimport.Result, importErr error,
) error {
	if errors.Is(importErr, legacyimport.ErrLimitReached) || result.ErrorsSeen != 0 {
		return fmt.Errorf("cannot activate an incomplete import")
	}
	if client == nil {
		return fmt.Errorf("repository is already SlateDB-authoritative")
	}
	if options.Daemon.RebuildInitialize {
		if err := validateRebuiltIndex(ctx, options, repo, store, result); err != nil {
			return err
		}
	}
	if err := repo.EnableSlateDBAuthority(ctx, client); err != nil {
		return fmt.Errorf("activate SlateDB authority: %w", err)
	}
	if options.Daemon.RebuildInitialize {
		_ = observability.Emit(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index",
			Message: "authenticated metadata rebuild activated", Fields: map[string]any{"candidate": rebuildCandidateName(options.Daemon)}})
	}
	return nil
}

func validateRebuiltIndex(ctx context.Context, options indexImportOptions, repo *repository.Repository, store *daemon.SchemaStore,
	result legacyimport.Result,
) error {
	placementModel, err := indexMaintenancePlacementModel(repo)
	if err != nil {
		return err
	}
	validation, err := maintenance.CheckWithOptions(ctx, repo, store,
		maintenance.CheckOptions{MaxFindings: 100, PlacementModel: placementModel, PathIndexPaths: repo.Config().PathIndexPaths})
	if err != nil {
		return fmt.Errorf("validate rebuilt metadata candidate: %w", err)
	}
	if !validation.Clean() || validation.HasWarnings() {
		return fmt.Errorf("rebuilt metadata candidate failed validation: %d findings, %d warnings", len(validation.Findings), validation.Warnings)
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryIntegrity, Component: "index",
		Message: "authenticated metadata rebuild candidate validated",
		Fields:  map[string]any{"candidate": rebuildCandidateName(options.Daemon), "packs": result.PacksImported, "blobs": result.BlobsImported}})
	return nil
}

func printIndexImportResult(printer interface {
	P(msg string, args ...any)
	E(msg string, args ...any)
}, result legacyimport.Result) {
	printer.P("imported %d indexes, %d packs, %d blobs, and %d snapshots\n", result.IndexesImported, result.PacksImported,
		result.BlobsImported, result.SnapshotsImported)
	for _, finding := range result.Findings {
		printer.E("%s %s: %s\n", finding.Stage, strings.TrimSpace(finding.SourceID.Str()), finding.Error)
	}
}

func validateMetadataRebuildTarget(options indexDaemonOptions) (string, error) {
	if options.ObjectStore == "s3" {
		if options.DataDir != "" {
			return "", fmt.Errorf("S3 metadata rebuild candidate does not accept --daemon-data-dir")
		}
		prefix := strings.Trim(options.S3Prefix, "/")
		if options.S3Bucket == "" || prefix == "" {
			return "", fmt.Errorf("S3 metadata rebuild candidate requires --daemon-s3-bucket and a dedicated non-empty --daemon-s3-prefix")
		}
		return "s3://" + options.S3Bucket + "/" + prefix, nil
	}
	if options.ObjectStore != "" && options.ObjectStore != "local" {
		return "", fmt.Errorf("metadata rebuild candidate must use a persistent local or S3 object store")
	}
	if options.DataDir == "" {
		return "", fmt.Errorf("local metadata rebuild candidate requires a new --daemon-data-dir")
	}
	if _, err := os.Stat(options.DataDir); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return "", fmt.Errorf("inspect metadata rebuild candidate: %w", err)
		}
		return "", fmt.Errorf("metadata rebuild candidate directory already exists")
	}
	return options.DataDir, nil
}

func rebuildCandidateName(options indexDaemonOptions) string {
	if options.ObjectStore == "s3" {
		return "s3://" + options.S3Bucket + "/" + strings.Trim(options.S3Prefix, "/")
	}
	return options.DataDir
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
		Use:   "export",
		Short: "Export SlateDB metadata as legacy JSON indexes",
		Long: "Export authoritative blob locations in canonical Restic JSON index format. By default only packs without completed " +
			"export checkpoints are written; --full writes every live pack." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
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
	storeSession, err := openStoreSession(ctx, repo, options.Daemon)
	if err != nil {
		return result, err
	}
	defer storeSession.CloseAndLog()
	store := storeSession.Store
	result, err = maintenance.Export(
		ctx,
		store,
		repo,
		maintenance.ExportOptions{
			Full:          options.Full,
			DryRun:        options.DryRun,
			Verify:        options.Verify,
			Since:         options.Since,
			PacksPerIndex: options.PacksPerIndex,
		},
	)
	if err == nil && !globalOptions.JSON {
		printer.P(
			"selected %d packs and %d blobs; wrote %d JSON indexes; export sequence %d\n",
			result.PacksSelected,
			result.BlobsSelected,
			result.IndexesWritten,
			result.ExportSequence,
		)
	}
	return result, err
}

type indexCheckOptions struct {
	Daemon               indexDaemonOptions
	MaxFindings          uint
	LegacyOnly           bool
	SlateDBOnly          bool
	IncludeCrawlDebt     bool
	FailOnWarning        bool
	PathIndexPaths       []string
	QuorumCapsule        string
	BypassAttestation    string
	BypassAttestationKey string
}

func newIndexCheckCommand(globalOptions *global.Options) *cobra.Command {
	var options indexCheckOptions
	command := &cobra.Command{
		Use:   "check",
		Short: "Compare legacy JSON and SlateDB metadata",
		Long: "Compare deduplicated physical blob locations and pack catalogs between legacy JSON and SlateDB, " +
			"and verify all pack aggregate records." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
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
	command.Flags().StringSliceVar(&options.PathIndexPaths, "path-index", nil, "validate pv path-index entries for these paths")
	command.Flags().
		StringVar(
			&options.QuorumCapsule,
			"quorum-capsule",
			"",
			"verify quorum policy and access routes against this capsule and the configured metadata key broker",
		)
	command.Flags().StringVar(&options.BypassAttestation, "bypass-attestation", "", "mode-0600 signed non-discoverable bypass attestation")
	command.Flags().StringVar(&options.BypassAttestationKey, "bypass-attestation-key", "", "mode-0600 pinned Ed25519 attestation public key")
	return command
}

func runIndexCheck(ctx context.Context, options indexCheckOptions, globalOptions global.Options, term ui.Terminal) (maintenance.CheckResult, error) {
	var result maintenance.CheckResult
	if options.QuorumCapsule == "" && (options.BypassAttestation != "" || options.BypassAttestationKey != "") {
		return result, fmt.Errorf("--bypass-attestation and --bypass-attestation-key require --quorum-capsule")
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
	var store *daemon.SchemaStore
	var storeSession *Session
	if !options.LegacyOnly {
		storeSession, err = openStoreSession(ctx, repo, options.Daemon)
		if err != nil {
			return result, err
		}
		store = storeSession.Store
	}
	if storeSession != nil {
		defer storeSession.CloseAndLog()
	}
	placementModel, err := indexMaintenancePlacementModel(repo)
	if err != nil {
		return result, err
	}
	pathIndexPaths := append([]string(nil), repo.Config().PathIndexPaths...)
	pathIndexPaths = append(pathIndexPaths, options.PathIndexPaths...)
	result, err = maintenance.CheckWithOptions(
		ctx,
		repo,
		store,
		maintenance.CheckOptions{
			LegacyOnly:       options.LegacyOnly,
			SlateDBOnly:      options.SlateDBOnly,
			IncludeCrawlDebt: options.IncludeCrawlDebt,
			MaxFindings:      options.MaxFindings,
			PlacementModel:   placementModel,
			PathIndexPaths:   pathIndexPaths,
		},
	)
	if err != nil {
		return result, err
	}
	if options.QuorumCapsule != "" {
		if err := checkIndexQuorum(ctx, options, globalOptions, repo, &result); err != nil {
			return result, err
		}
	}
	if !globalOptions.JSON {
		printer.P(
			"legacy locations: %d; SlateDB locations: %d; differences: %d; aggregate mismatches: %d\n",
			result.LegacyLocations,
			result.SlateDBLocations,
			result.MissingInSlateDB+result.MissingInLegacy,
			result.AggregateMismatch,
		)
		printer.P("analytics consistency mismatches: %d\n", result.AnalyticsMismatch)
		printer.P(
			"packs: unknown tier %d; retention unknown %d; usage unaccounted %d\n",
			result.UnknownTierPacks,
			result.RetentionUnknownPacks,
			result.UsageUnaccountedPacks,
		)
		printer.P("placements: missing %d; reverse mismatches %d; tier mismatches %d; below durability %d; unknown backends %d\n",
			result.MissingPlacementRecords, result.BackendPackMismatch, result.DerivedTierMismatch,
			result.PacksBelowDurability, result.UnknownPlacementBackends)
		printer.P("verification state mismatches: %d\n", result.VerificationStateMismatch)
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

func checkIndexQuorum(ctx context.Context, options indexCheckOptions, globalOptions global.Options, repo *repository.Repository,
	result *maintenance.CheckResult,
) error {
	if options.Daemon.BrokerSocket == "" {
		return fmt.Errorf("--quorum-capsule requires --metadata-key-broker-socket")
	}
	capsule, err := indexbroker.LoadCapsule(options.QuorumCapsule)
	if err != nil {
		return err
	}
	brokerClient, err := indexbroker.Dial(ctx, options.Daemon.BrokerSocket)
	if err != nil {
		return err
	}
	quorum, err := brokerClient.Status(ctx)
	_ = brokerClient.Close()
	if err != nil {
		return err
	}
	if err := matchQuorumCapsule(capsule.RepositoryID(), capsule.Generation(), capsule.LogicalID(), capsule.PolicyHash(), quorum); err != nil {
		return err
	}
	metadataStatus, err := indexMetadataKeyStatus(ctx, options.Daemon, repo.Config().ID)
	if err != nil {
		return fmt.Errorf("read metadata key status for quorum check: %w", err)
	}
	findings := quorumAccessRouteFindings(globalOptions, metadataStatus)
	findings = append(findings, quorumAttestationFindings(capsule, options.BypassAttestation, options.BypassAttestationKey, time.Now())...)
	findings = append(findings, quorum.Findings...)
	result.QuorumChecked = true
	result.QuorumNonCompliant = !quorum.Compliant || len(findings) != 0
	result.MinimumCustodians = quorum.MinimumCustodians
	result.PrincipalVerified = quorum.PrincipalVerified
	result.HardwareVerified = quorum.HardwareVerified
	result.CustodyAssumed = quorum.CustodyAssumed
	for _, finding := range findings {
		if options.MaxFindings == 0 || uint(len(result.Findings)) < options.MaxFindings {
			result.Findings = append(result.Findings, maintenance.Finding{Kind: "quorum_bypass", Key: finding})
		}
	}
	return nil
}

func indexMetadataKeyStatus(ctx context.Context, options indexDaemonOptions, repositoryID string) (daemon.KeyStatus, error) {
	session, err := options.openDaemonSession(ctx, repositoryID)
	if err != nil {
		return daemon.KeyStatus{}, err
	}
	defer session.CloseAndLog()
	return session.Client.KeyStatus(ctx)
}

func MaintenancePlacementModel(repo *repository.Repository) (maintenance.PlacementModel, error) {
	model, err := repo.PlacementModel()
	if err != nil {
		return maintenance.PlacementModel{}, err
	}
	converted := maintenance.PlacementModel{
		Policy: maintenance.DurabilityPolicy{
			MinCopies: model.Policy.MinCopies, MinDomains: model.Policy.MinDomains,
			MinOffsite: model.Policy.MinOffsite, OffsiteDeadline: model.Policy.OffsiteDeadline,
			PromotionCrossoverSeconds: model.Policy.PromotionCrossoverSeconds,
		},
	}
	converted.Backends = make([]maintenance.PlacementBackend, 0, len(model.Backends))
	for _, backend := range model.Backends {
		converted.Backends = append(converted.Backends, maintenance.PlacementBackend{
			ID: backend.ID, Hash: backend.Hash, Role: backend.Role,
			Ingest: backend.Ingest, ReadEnabled: backend.ReadEnabled,
			Offsite: backend.Offsite, FailureDomain: backend.FailureDomain,
			RetrievalClass: backend.RetrievalClass, PricePerGBEgress: backend.PricePerGBEgress,
			MinRetentionSeconds: backend.MinRetentionSeconds,
			MaxBandwidthBytes:   backend.MaxBandwidthBytes, MaxRequestsPerSecond: backend.MaxRequestsPerSecond,
		})
	}
	return converted, nil
}

var indexMaintenancePlacementModel = MaintenancePlacementModel

type indexRebuildPackStatsOptions struct {
	Daemon         indexDaemonOptions
	DryRun         bool
	PathIndexPaths []string
}

func newIndexRebuildPackStatsCommand(globalOptions *global.Options) *cobra.Command {
	var options indexRebuildPackStatsOptions
	command := &cobra.Command{
		Use:   "rebuild-pack-stats",
		Short: "Rebuild SlateDB pack aggregates",
		Long: "Recalculate every pack aggregate from the authoritative pack catalog and replace all aggregate records atomically " +
			"when drift is present." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
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
	command.Flags().StringSliceVar(&options.PathIndexPaths, "path-index", nil, "rebuild pv path-index entries for these paths")
	return command
}

func runIndexRebuildPackStats(
	ctx context.Context,
	options indexRebuildPackStatsOptions,
	globalOptions global.Options,
	term ui.Terminal,
) (maintenance.RebuildResult, error) {
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
	if err != nil {
		return result, err
	}
	result.SnapshotCommitChanged, err = maintenance.RebuildSnapshotCommitIndex(ctx, store, options.DryRun)
	if err != nil {
		return result, err
	}
	pathIndexPaths := append([]string(nil), repo.Config().PathIndexPaths...)
	pathIndexPaths = append(pathIndexPaths, options.PathIndexPaths...)
	pathResult, err := maintenance.RebuildPathVersionIndex(ctx, store, pathIndexPaths, options.DryRun)
	result.PathVersionChanged, result.PathVersionOverflow = pathResult.BindingsChanged, pathResult.OverflowPaths
	if err == nil && !globalOptions.JSON {
		printer.P(
			"scanned %d packs; changed %d aggregate records, %d placement records, %d tier summaries, %d backend-pack records, %d snapshot-commit records\n",
			result.PacksScanned,
			result.AggregatesChanged,
			result.PlacementRecordsChanged,
			result.TierSummaryChanged,
			result.BackendPackRecordsChanged,
			result.SnapshotCommitChanged,
		)
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
		Use:   "gc",
		Short: "Discover, revalidate, and sweep unreachable SlateDB packs",
		Long: "Discover GC candidates from reverse references and the pack catalog, re-walk every " +
			"retained snapshot root to confirm reachability, then delete wholly unreachable packs and " +
			"repack packs that mix live and unreachable blobs. A failed physical deletion leaves the " +
			"pack visible as delete-pending and is retried on the next run. Any freed or repacked pack " +
			"automatically triggers a full re-export and removes now-stale legacy JSON indexes, so " +
			"compatibility artifacts never reference a deleted pack. --discover-only records " +
			"candidates cheaply from reverse references without the snapshot walk or any deletion." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
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
	command.Flags().
		BoolVar(&options.DiscoverOnly, "discover-only", false, "record candidate blobs from reverse references without the snapshot walk or any deletion")
	command.Flags().
		DurationVar(
			&options.MinCandidateAge,
			"min-candidate-age",
			0,
			"require a candidate to have been continuously unreachable for at least this long before sweeping it",
		)
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
		printer.P(
			"scanned %d packs and %d blobs; whole=%d mixed=%d pending-age=%d pending-retries=%d; "+
				"deleted=%d (of which retried=%d) repacked=%d retry-failed=%d\n",
			result.PacksScanned,
			result.BlobsScanned,
			result.WholePackCandidates,
			result.MixedPackCandidates,
			result.PendingAge,
			result.PendingRetries,
			result.PacksDeleted,
			result.PacksRetried,
			result.PacksRepacked,
			result.PacksRetryFailed,
		)
		if result.PacksAccounted != 0 || result.PacksUnaccountable != 0 {
			printer.P("refreshed usage accounting for %d packs; %d left unaccounted\n", result.PacksAccounted, result.PacksUnaccountable)
		}
	}
	if result.PacksRetryFailed != 0 {
		return result, fmt.Errorf("%w: %d packs remain delete-pending after a failed retry", errIndexIncomplete, result.PacksRetryFailed)
	}
	return result, nil
}
