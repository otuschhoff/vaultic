package indexcmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
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

func mustMarkFlagRequired(command *cobra.Command, name string) {
	if err := command.MarkFlagRequired(name); err != nil {
		panic(fmt.Sprintf("mark flag %q required: %v", name, err)) //nolint:forbidigo // Registered flags must exist during construction.
	}
}

func mustMarkPersistentFlagRequired(command *cobra.Command, name string) {
	if err := command.MarkPersistentFlagRequired(name); err != nil {
		panic(fmt.Sprintf("mark persistent flag %q required: %v", name, err)) //nolint:forbidigo // Registered flags must exist during construction.
	}
}

type indexDaemonOptions struct {
	Socket                        string
	TCPAddress                    string
	TCPAllowlist                  []string
	AuthTokenFile                 string
	DaemonPath                    string
	DataDir                       string
	ObjectStore                   string
	S3Bucket                      string
	S3Prefix                      string
	S3Endpoint                    string
	S3Region                      string
	S3Provider                    string
	S3BucketLookup                string
	WALStore                      string
	WALDataDir                    string
	WALFlushInterval              time.Duration
	MaxUnflushedBytes             uint64
	L0SSTSizeBytes                uint64
	WALS3Bucket                   string
	WALS3Prefix                   string
	WALS3Endpoint                 string
	WALS3Region                   string
	WALS3Provider                 string
	WALS3BucketLookup             string
	WALRadosMonitors              string
	WALRadosFSID                  string
	WALRadosPool                  string
	WALRadosNamespace             string
	WALRadosPrefix                string
	WALRadosClient                string
	WALRadosKeyFile               string
	EncryptionMode                string
	PassphraseFile                string
	AzureTokenFile                string
	GCPTokenFile                  string
	VaultTokenFile                string
	PKCS11PINFile                 string
	RecoveryUnlock                bool
	BrokerSocket                  string
	BrokerManifest                string
	BrokerLease                   time.Duration
	RebuildInitialize             bool
	RebuildReset                  bool
	FreshBulkImport               bool
	BulkImportPhysicalMemoryBytes uint64
	BulkImportReadCacheBytes      uint64
	Start                         bool
	Persistent                    bool
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
	flags.StringVar(&options.S3Endpoint, "daemon-s3-endpoint", "", "vaulticdb S3 endpoint URL")
	flags.StringVar(&options.S3Region, "daemon-s3-region", "", "vaulticdb S3 signing region")
	flags.StringVar(&options.S3Provider, "daemon-s3-provider", "", "vaulticdb S3 provider: generic, backblaze, or wasabi")
	flags.StringVar(&options.S3BucketLookup, "daemon-s3-bucket-lookup", "", "vaulticdb S3 bucket lookup: auto, dns, or path")
	flags.StringVar(&options.WALStore, "daemon-wal-store", "", "vaulticdb WAL store: inherit, local, memory (volatile), s3, or rados")
	flags.StringVar(&options.WALDataDir, "daemon-wal-data-dir", "", "local vaulticdb WAL directory")
	flags.DurationVar(&options.WALFlushInterval, "daemon-wal-flush-interval", 0, "SlateDB WAL flush interval (zero uses the default)")
	flags.Uint64Var(&options.MaxUnflushedBytes, "daemon-max-unflushed-bytes", 0, "SlateDB total unflushed byte limit (zero uses the default)")
	flags.Uint64Var(&options.L0SSTSizeBytes, "daemon-l0-sst-size-bytes", 0, "SlateDB L0 SST size threshold (zero uses the default)")
	flags.StringVar(&options.WALS3Bucket, "daemon-wal-s3-bucket", "", "vaulticdb WAL S3 bucket")
	flags.StringVar(&options.WALS3Prefix, "daemon-wal-s3-prefix", "", "vaulticdb WAL S3 key prefix")
	flags.StringVar(&options.WALS3Endpoint, "daemon-wal-s3-endpoint", "", "vaulticdb WAL S3 endpoint URL")
	flags.StringVar(&options.WALS3Region, "daemon-wal-s3-region", "", "vaulticdb WAL S3 signing region")
	flags.StringVar(&options.WALS3Provider, "daemon-wal-s3-provider", "", "vaulticdb WAL S3 provider profile")
	flags.StringVar(&options.WALS3BucketLookup, "daemon-wal-s3-bucket-lookup", "", "vaulticdb WAL S3 bucket lookup")
	flags.StringVar(&options.WALRadosMonitors, "daemon-wal-rados-monitors", "", "vaulticdb WAL Ceph monitor endpoints")
	flags.StringVar(&options.WALRadosFSID, "daemon-wal-rados-fsid", "", "vaulticdb WAL Ceph cluster FSID")
	flags.StringVar(&options.WALRadosPool, "daemon-wal-rados-pool", "", "vaulticdb WAL Ceph pool")
	flags.StringVar(&options.WALRadosNamespace, "daemon-wal-rados-namespace", "", "vaulticdb WAL Ceph namespace")
	flags.StringVar(&options.WALRadosPrefix, "daemon-wal-rados-prefix", "", "vaulticdb WAL RADOS object prefix")
	flags.StringVar(&options.WALRadosClient, "daemon-wal-rados-client", "", "vaulticdb WAL CephX client identity")
	flags.StringVar(&options.WALRadosKeyFile, "daemon-wal-rados-key-file", "", "protected file containing the vaulticdb WAL CephX key")
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
	var walRadosKey string
	if options.WALRadosKeyFile != "" {
		value, err := readProtectedBinary(options.WALRadosKeyFile, "vaulticdb WAL CephX key", true)
		if err != nil {
			return daemon.Options{}, err
		}
		defer clear(value)
		walRadosKey = strings.TrimSpace(string(value))
		if walRadosKey == "" {
			return daemon.Options{}, errors.New("vaulticdb WAL CephX key is empty")
		}
	}
	config := daemon.Options{
		Socket: options.Socket, TCPAddress: options.TCPAddress, TCPAllowlist: options.TCPAllowlist,
		AuthToken: authToken, RepositoryID: repositoryID, DataDir: options.DataDir,
		DaemonPath:  options.DaemonPath,
		ObjectStore: options.ObjectStore, S3Bucket: options.S3Bucket, S3Prefix: options.S3Prefix,
		S3Endpoint: options.S3Endpoint, S3Region: options.S3Region,
		S3Provider: options.S3Provider, S3BucketLookup: options.S3BucketLookup,
		WALStore: options.WALStore, WALDataDir: options.WALDataDir,
		WALFlushInterval: options.WALFlushInterval, MaxUnflushedBytes: options.MaxUnflushedBytes,
		L0SSTSizeBytes: options.L0SSTSizeBytes,
		WALS3Bucket:    options.WALS3Bucket, WALS3Prefix: options.WALS3Prefix,
		WALS3Endpoint: options.WALS3Endpoint, WALS3Region: options.WALS3Region,
		WALS3Provider: options.WALS3Provider, WALS3BucketLookup: options.WALS3BucketLookup,
		WALRadosMonitors: options.WALRadosMonitors, WALRadosFSID: options.WALRadosFSID,
		WALRadosPool: options.WALRadosPool, WALRadosNamespace: options.WALRadosNamespace,
		WALRadosPrefix: options.WALRadosPrefix, WALRadosClient: options.WALRadosClient,
		WALRadosKey:    walRadosKey,
		EncryptionMode: options.EncryptionMode, PassphraseFile: options.PassphraseFile,
		AzureTokenFile: options.AzureTokenFile,
		GCPTokenFile:   options.GCPTokenFile,
		VaultTokenFile: options.VaultTokenFile,
		PKCS11PINFile:  options.PKCS11PINFile,
		RecoveryUnlock: options.RecoveryUnlock,
		BrokerSocket:   options.BrokerSocket, BrokerManifest: options.BrokerManifest,
		BrokerLease: options.BrokerLease, RebuildInitialize: options.RebuildInitialize,
		RebuildReset:             options.RebuildReset,
		FreshBulkImport:          options.FreshBulkImport,
		BulkImportReadCacheBytes: options.BulkImportReadCacheBytes,
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
	ForceResetOldIndex         bool
	DryRun                     bool
	Activate                   bool
	FromLegacy                 bool
	BatchSize                  uint32
	PackWorkers                uint
	ImportPublicationLanes     uint
	PackTimeout                time.Duration
	PacksPerTransaction        uint
	ImportTransactionBytes     uint64
	PreparedImportBytes        uint64
	ImportBatchTimeout         time.Duration
	ImportDeferCleanup         bool
	MaxErrors                  uint64
	WorkBudget                 uint64
	SnapshotDepth              uint
	SnapshotWorkBudget         uint64
	ConfirmMetadataLossRebuild bool
}

const (
	bulkImportL0SSTSizeBytes = 256 * 1024 * 1024
	bulkImportCacheMaxBytes  = 64 * 1024 * 1024 * 1024
	bulkImportMaxUnflushed   = 16 * 1024 * 1024 * 1024
)

type bulkImportMemoryProfile struct {
	physicalBytes     uint64
	readCacheBytes    uint64
	maxUnflushedBytes uint64
}

func newBulkImportMemoryProfile(physicalBytes uint64) bulkImportMemoryProfile {
	const gib = uint64(1024 * 1024 * 1024)
	profile := bulkImportMemoryProfile{physicalBytes: physicalBytes, maxUnflushedBytes: 4 * gib}
	if physicalBytes == 0 {
		return profile
	}
	reserved := max(8*gib, physicalBytes/4)
	if reserved >= physicalBytes {
		profile.maxUnflushedBytes = gib
		return profile
	}
	usable := physicalBytes - reserved
	profile.readCacheBytes = min(bulkImportCacheMaxBytes, usable/2)
	profile.maxUnflushedBytes = min(bulkImportMaxUnflushed, max(gib, usable/8))
	return profile
}

func applyFreshBulkImportDefaults(options indexImportOptions) indexImportOptions {
	profile := newBulkImportMemoryProfile(physicalMemoryBytes())
	if options.Daemon.WALStore == "" {
		options.Daemon.WALStore = "memory"
	}
	if options.Daemon.WALDataDir == "" {
		if options.Daemon.DataDir != "" {
			options.Daemon.WALDataDir = filepath.Join(options.Daemon.DataDir, "wal")
		} else {
			options.Daemon.WALDataDir = filepath.Join(os.TempDir(), "vaulticdb", "wal")
		}
	}
	if options.Daemon.WALFlushInterval == 0 {
		options.Daemon.WALFlushInterval = 500 * time.Millisecond
	}
	if options.Daemon.MaxUnflushedBytes == 0 {
		options.Daemon.MaxUnflushedBytes = profile.maxUnflushedBytes
	}
	if options.Daemon.L0SSTSizeBytes == 0 {
		options.Daemon.L0SSTSizeBytes = bulkImportL0SSTSizeBytes
	}
	if options.PackWorkers == 0 {
		options.PackWorkers = uint(min(32, runtime.GOMAXPROCS(0)))
	}
	if options.ImportPublicationLanes == 0 {
		options.ImportPublicationLanes = 2
	}
	if options.PacksPerTransaction == 0 {
		options.PacksPerTransaction = 8
	}
	if options.ImportTransactionBytes == 0 {
		options.ImportTransactionBytes = 8 << 20
	}
	if options.PreparedImportBytes == 0 {
		options.PreparedImportBytes = 256 << 20
	}
	if options.ImportBatchTimeout == 0 {
		options.ImportBatchTimeout = 4 * time.Minute
	}
	options.Daemon.FreshBulkImport = true
	options.Daemon.BulkImportPhysicalMemoryBytes = profile.physicalBytes
	options.Daemon.BulkImportReadCacheBytes = profile.readCacheBytes
	return options
}

type importProgressReporter struct {
	started     time.Time
	lastPrinted time.Time
	lastPercent uint64
	lastPacks   uint64
	lastBlobs   uint64
	lastNodes   uint64
	printed     bool
	log         func(string)
}

func newImportProgressReporter(started time.Time) *importProgressReporter {
	return &importProgressReporter{started: started, log: func(message string) { log.Print(message) }}
}

func (reporter *importProgressReporter) Update(progress legacyimport.Progress) {
	reporter.update(time.Now(), progress)
}

func (reporter *importProgressReporter) update(now time.Time, progress legacyimport.Progress) {
	completed := progress.IndexesCompleted + progress.SnapshotsCompleted
	total := progress.IndexesTotal + progress.SnapshotsTotal
	percentBucket := uint64(100)
	percent := 100.0
	if total > 0 {
		percentBucket = completed * 100 / total
		percent = float64(completed) * 100 / float64(total)
	}
	if reporter.printed && completed != total &&
		percentBucket <= reporter.lastPercent && now.Sub(reporter.lastPrinted) < 10*time.Second {
		return
	}
	remaining, eta := importProgressEstimate(reporter.started, now, completed, total)
	intervalRate := "unknown"
	if reporter.printed {
		intervalRate = formatImportRate(
			progress.PacksImported-reporter.lastPacks,
			progress.BlobsImported-reporter.lastBlobs,
			progress.NodesImported-reporter.lastNodes,
			now.Sub(reporter.lastPrinted),
		)
	}
	overallRate := formatImportRate(
		progress.PacksImported, progress.BlobsImported, progress.NodesImported, now.Sub(reporter.started),
	)
	message := fmt.Sprintf(
		"legacy import progress: %.1f%%; indexes %d/%d (imported %d, resumed %d); "+
			"snapshots %d/%d (imported %d, resumed %d); packs prepared/committed %d/%d; blobs %d; "+
			"batches %d (adaptive splits %d); prepared bytes total %d; queue %d packs/%d bytes "+
			"(peak %d packs/%d bytes); "+
			"stage time prepare aggregate=%s publish aggregate-lane=%s checkpoint batch=%s; checkpoint pending %t; "+
			"speed last interval %s; speed since start %s; elapsed %s; est. remaining %s; ETA %s",
		percent,
		progress.IndexesCompleted,
		progress.IndexesTotal,
		progress.IndexesImported,
		progress.IndexesResumed,
		progress.SnapshotsCompleted,
		progress.SnapshotsTotal,
		progress.SnapshotsImported,
		progress.SnapshotsResumed,
		progress.PacksPrepared,
		progress.PacksImported,
		progress.BlobsImported,
		progress.BatchesCommitted,
		progress.AdaptiveSplits,
		progress.PreparedBytes,
		progress.QueuedPreparedPacks,
		progress.QueuedPreparedBytes,
		progress.PeakPreparedPacks,
		progress.PeakPreparedBytes,
		progress.PreparationTime.Round(time.Millisecond),
		progress.PublicationTime.Round(time.Millisecond),
		progress.CheckpointBatchTime.Round(time.Millisecond),
		progress.CheckpointPending,
		intervalRate,
		overallRate,
		now.Sub(reporter.started).Round(time.Second),
		remaining,
		eta,
	)
	reporter.log(message)
	reporter.printed = true
	reporter.lastPrinted = now
	reporter.lastPercent = percentBucket
	reporter.lastPacks = progress.PacksImported
	reporter.lastBlobs = progress.BlobsImported
	reporter.lastNodes = progress.NodesImported
}

func formatImportRate(packs, blobs, nodes uint64, elapsed time.Duration) string {
	if elapsed <= 0 {
		return "unknown"
	}
	seconds := elapsed.Seconds()
	return fmt.Sprintf(
		"%.1f packs/s, %.1f blobs/s, %.1f nodes/s",
		float64(packs)/seconds,
		float64(blobs)/seconds,
		float64(nodes)/seconds,
	)
}

func importProgressEstimate(started, now time.Time, completed, total uint64) (string, string) {
	if completed == 0 || total == 0 || completed > total {
		return "unknown", "unknown"
	}
	elapsed := now.Sub(started)
	remaining := time.Duration(float64(elapsed) * float64(total-completed) / float64(completed)).Round(time.Second)
	return remaining.String(), now.Add(remaining).Format(time.RFC3339)
}

func emitImportStatus(message string) {
	log.Print(message)
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
	flags.BoolVar(
		&options.ForceResetOldIndex,
		"force-reset-old-idx",
		false,
		"destroy an existing rebuild candidate and import it again from empty",
	)
	flags.BoolVar(&options.DryRun, "dry-run", false, "scan and validate without writing SlateDB")
	flags.BoolVar(&options.Activate, "activate", false, "make SlateDB authoritative after a complete import")
	flags.BoolVar(&options.FromLegacy, "from-legacy", true, "import from legacy JSON indexes")
	flags.BoolVar(&options.ImportDeferCleanup, "import-defer-cleanup", false, "defer receipt cleanup durability during a fresh memory-WAL import")
	flags.Uint32Var(&options.BatchSize, "batch-size", 0, "maximum mutations per daemon transaction batch (zero uses daemon limit)")
	flags.UintVar(&options.PackWorkers, "pack-workers", 0, "concurrent legacy pack preparations (zero uses up to eight available CPUs)")
	flags.UintVar(
		&options.ImportPublicationLanes,
		"import-publication-lanes",
		0,
		"concurrent stage 3 split-publication lanes (zero selects command defaults)",
	)
	flags.DurationVar(&options.PackTimeout, "pack-timeout", 5*time.Minute, "maximum time to prepare one legacy pack")
	flags.UintVar(
		&options.PacksPerTransaction, "packs-per-transaction", 0,
		"maximum packs per import transaction and minimum prepared-pack floor, up to 256 (zero uses eight)",
	)
	flags.Uint64Var(&options.ImportTransactionBytes, "import-transaction-bytes", 0, "estimated encoded bytes per import transaction (zero uses 8 MiB)")
	flags.Uint64Var(&options.PreparedImportBytes, "prepared-import-bytes", 0, "maximum queued prepared pack bytes (zero uses 256 MiB)")
	flags.DurationVar(
		&options.ImportBatchTimeout, "import-batch-timeout", 0,
		"maximum time for one import transaction, up to five minutes (zero uses four minutes)",
	)
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
	commandStarted := time.Now()
	defer func() {
		log.Printf("legacy import lifecycle: total=%s success=%t", time.Since(commandStarted), err == nil)
	}()
	options, err = validateIndexImportOptions(options)
	if err != nil {
		return result, err
	}
	if options.Daemon.FreshBulkImport {
		log.Printf(
			"fresh legacy bulk-import profile: physical_memory=%d read_cache=%d max_unflushed=%d l0_sst=%d wal=%s flush_interval=%s",
			options.Daemon.BulkImportPhysicalMemoryBytes, options.Daemon.BulkImportReadCacheBytes,
			options.Daemon.MaxUnflushedBytes,
			options.Daemon.L0SSTSizeBytes, options.Daemon.WALStore, options.Daemon.WALFlushInterval,
		)
	}
	if err := prepareMetadataRebuild(ctx, options, globalOptions); err != nil {
		return result, err
	}
	config, err := options.Daemon.config("")
	if err != nil {
		return result, err
	}
	repositoryConfig := config
	repositoryConfig.RebuildReset = false
	ctx = repository.WithDaemonOptions(ctx, repositoryConfig)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return result, err
	}
	defer unlock()
	if options.ForceResetOldIndex {
		if _, authoritative := repo.Engine().(*metadataindex.DaemonEngine); authoritative {
			return result, fmt.Errorf("--force-reset-old-idx refuses an authoritative SlateDB index")
		}
	}
	resetStarted := time.Time{}
	if options.ForceResetOldIndex {
		resetStarted = time.Now()
	}
	storeSession, err := openStoreSession(ctx, repo, options.Daemon)
	if err != nil {
		return result, fmt.Errorf("connect vaulticdb: %w", err)
	}
	var resetElapsed time.Duration
	if options.ForceResetOldIndex {
		resetElapsed = time.Since(resetStarted)
		emitImportStatus(fmt.Sprintf(
			"legacy import candidate reset and VaulticDB startup completed in %s",
			resetElapsed.Round(time.Millisecond),
		))
	}
	store := storeSession.Store
	if options.ForceResetOldIndex {
		store.EnableFreshLegacyImport()
	}
	defer func() {
		closeStarted := time.Now()
		closeErr := storeSession.Close()
		log.Printf("legacy import lifecycle: close=%s success=%t", time.Since(closeStarted), closeErr == nil)
		err = errors.Join(err, closeErr)
	}()
	if options.ImportDeferCleanup {
		if err := store.EnableDeferredLegacyImportCleanup(); err != nil {
			return result, err
		}
	}
	telemetry := legacyimport.NewSchedulerTelemetry()
	stopStats := startLegacyImportStats(ctx, store, 10*time.Second, func(stats daemon.LegacyImportStats) {
		logLegacyImportStats(stats)
		log.Printf("legacy import scheduler: %s", formatLegacySchedulerStats(telemetry.Snapshot()))
	})
	defer stopStats()
	if options.SnapshotDepth > 0 || options.SnapshotWorkBudget > 0 {
		if err := repo.LoadIndex(ctx, printer); err != nil {
			return result, fmt.Errorf("load source indexes for snapshot import: %w", err)
		}
	}
	started := time.Now()
	progressReporter := newImportProgressReporter(started)
	log.Printf(
		"legacy metadata import started: pack_workers=%d batch_size=%d packs_per_transaction=%d "+
			"transaction_bytes=%d prepared_bytes=%d publication_lanes=%d batch_timeout=%s snapshot_depth=%d resume=%t fresh=%t",
		options.PackWorkers, options.BatchSize, options.PacksPerTransaction, options.ImportTransactionBytes,
		options.PreparedImportBytes, options.ImportPublicationLanes, options.ImportBatchTimeout, options.SnapshotDepth, options.Resume,
		options.ForceResetOldIndex,
	)
	result, err = legacyimport.Import(ctx, repo, repo.Backend(), store, legacyimport.Options{
		Resume: options.Resume, DryRun: options.DryRun, MaxErrors: options.MaxErrors,
		BatchSize: options.BatchSize, PackWorkers: options.PackWorkers,
		PublicationLanes: options.ImportPublicationLanes, PackTimeout: options.PackTimeout,
		PacksPerTransaction: options.PacksPerTransaction, ImportTransactionBytes: options.ImportTransactionBytes,
		PreparedImportBytes: options.PreparedImportBytes, ImportBatchTimeout: options.ImportBatchTimeout,
		WorkBudget: options.WorkBudget, SnapshotDepth: options.SnapshotDepth,
		SnapshotWorkBudget: options.SnapshotWorkBudget, DeferSnapshotDurability: options.ForceResetOldIndex,
		Progress:  progressReporter.Update,
		Telemetry: telemetry,
	})
	stopStats()
	log.Printf("legacy import scheduler: %s", formatLegacySchedulerStats(telemetry.Snapshot()))
	logLegacyImportStats(store.LegacyImportStats())
	result.ResetElapsedMS = uint64(resetElapsed / time.Millisecond)
	logLegacyImportCompletion(started, result, err)
	if err != nil && !errors.Is(err, legacyimport.ErrLimitReached) {
		return result, err
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
	if options.Activate && options.Daemon.RebuildInitialize {
		if validationErr := validateRebuiltIndex(ctx, options, repo, store, result); validationErr != nil {
			return result, validationErr
		}
	}
	storeSession, client, err := completeFreshBulkImport(ctx, options, repo, storeSession)
	if err != nil {
		return result, err
	}
	if options.Activate {
		if activateErr := activateImportedIndex(ctx, options, repo, client); activateErr != nil {
			return result, activateErr
		}
	}
	return result, nil
}

func completeFreshBulkImport(
	ctx context.Context,
	options indexImportOptions,
	repo *repository.Repository,
	session *Session,
) (*Session, *daemon.Client, error) {
	if !options.Daemon.FreshBulkImport {
		return session, session.Client, nil
	}
	finalizationStarted := time.Now()
	defer func() {
		log.Printf("legacy import lifecycle: completion_handoff_reopen=%s", time.Since(finalizationStarted))
	}()
	if err := session.Store.MarkBulkImportComplete(ctx); err != nil {
		return session, nil, fmt.Errorf("mark successful bulk import complete: %w", err)
	}
	if !options.Activate {
		return session, session.Client, nil
	}
	if err := session.Close(); err != nil {
		return session, nil, fmt.Errorf("finalize bulk-import WAL handoff: %w", err)
	}
	reopened, err := openStoreSession(ctx, repo, completedBulkImportDaemonOptions(options.Daemon))
	if err != nil {
		return session, nil, fmt.Errorf("reopen completed bulk import on persistent WAL: %w", err)
	}
	return reopened, reopened.Client, nil
}

func completedBulkImportDaemonOptions(options indexDaemonOptions) indexDaemonOptions {
	options.RebuildInitialize = false
	options.RebuildReset = false
	options.FreshBulkImport = false
	options.WALStore = ""
	return options
}

func logLegacyImportCompletion(started time.Time, result legacyimport.Result, err error) {
	if err != nil {
		log.Printf(
			"legacy metadata import failed after %s: indexes=%d packs=%d blobs=%d snapshots=%d "+
				"batches_ingested=%d batches_reduced=%d batches_committed=%d peak_publication_lanes=%d: %v",
			time.Since(started).Round(time.Millisecond), result.IndexesImported, result.PacksImported,
			result.BlobsImported, result.SnapshotsImported, result.BatchesIngested, result.BatchesReduced,
			result.BatchesCommitted, result.PeakPublicationLanes, err,
		)
	} else {
		log.Printf(
			"legacy metadata import completed after %s: indexes=%d packs=%d blobs=%d snapshots=%d "+
				"batches_ingested=%d batches_reduced=%d batches_committed=%d peak_publication_lanes=%d",
			time.Since(started).Round(time.Millisecond), result.IndexesImported, result.PacksImported,
			result.BlobsImported, result.SnapshotsImported, result.BatchesIngested, result.BatchesReduced,
			result.BatchesCommitted, result.PeakPublicationLanes,
		)
	}
}

func logLegacyImportStats(importStats daemon.LegacyImportStats) {
	log.Printf(
		"legacy import cleanup: calls=%d pages=%d receipts=%d total=%s scan=%s begin=%s write=%s commit=%s deferred_commits=%d",
		importStats.CleanupCalls, importStats.CleanupPages, importStats.CleanupReceipts,
		importStats.CleanupTime, importStats.CleanupScanTime, importStats.CleanupBeginTime,
		importStats.CleanupWriteTime, importStats.CleanupCommitTime, importStats.CleanupDeferredCommits,
	)
	log.Printf(
		"legacy import transactions: batches=%d ingested=%d reduced=%d attempts=%d ingest_attempts=%d reduce_attempts=%d "+
			"ingest_failures=%d reduce_failures=%d commits=%d retries=%d conflicts=%d "+
			"packs=%d unique_blobs=%d source_indexes=%d mutations=%d bytes=%d replanned_bytes=%d "+
			"mutation_rpc_attempts=%d mutation_rpcs=%d reduction_mutation_rpc_attempts=%d reduction_mutation_rpcs=%d "+
			"reduction_mutations=%d receipt_reads=%d reduction_receipt_reads=%d recovery_reads=%d reduce_checkpoint_reads=%d "+
			"catalog_read_rpcs=%d catalog_read_keys=%d reduction_plan_read_rpcs=%d reduction_plan_read_keys=%d "+
			"planning_reads=%d gate_wait=%s planning=%s reduction=%s "+
			"mutation_rpc=%s commit=%s total_lane_time=%s lookups_absent=%d lookups_possible=%d lookups_found=%d "+
			"lookups_false_positive_equivalent=%d filter_layers=%d filter_bytes=%d filter_inserts=%d "+
			"filter_false_positive=%g filter_occupancy=%v filter_fallback=%t",
		importStats.Batches, importStats.IngestedBatches, importStats.ReducedBatches,
		importStats.Attempts, importStats.IngestAttempts, importStats.ReduceAttempts,
		importStats.IngestFailures, importStats.ReduceFailures, importStats.Commits, importStats.Retries, importStats.Conflicts,
		importStats.PacksCommitted, importStats.BlobsCommitted, importStats.SourceIndexesCommitted,
		importStats.MutationsCommitted, importStats.EncodedBytesCommitted, importStats.ReplannedBytes,
		importStats.MutationRPCAttempts, importStats.MutationRPCs,
		importStats.ReductionMutationRPCAttempts, importStats.ReductionMutationRPCs, importStats.ReductionMutations,
		importStats.ReceiptReads, importStats.ReductionReceiptReads, importStats.RecoveryReads, importStats.ReduceCheckpointReads,
		importStats.CatalogReadRPCs, importStats.CatalogReadKeys,
		importStats.ReductionPlanReadRPCs, importStats.ReductionPlanReadKeys,
		importStats.PlanningReads,
		importStats.GateWait, importStats.PlanningTime,
		importStats.ReductionTime,
		importStats.MutationRPCTime, importStats.CommitTime, importStats.TotalTime,
		importStats.DefinitelyAbsentLookups, importStats.PossiblyPresentLookups, importStats.FoundLookups,
		importStats.FalsePositiveEquivalentLookups, importStats.FilterLayers, importStats.FilterBytes,
		importStats.FilterInserts, importStats.FilterFalsePositive, importStats.FilterLayerOccupancy,
		importStats.FilterFallbackToDatabase,
	)
	log.Printf("legacy import operation latency: %s", formatLegacyOperationStats(importStats.Operations))
}

func formatLegacyOperationStats(operations map[string]daemon.DurationDistribution) string {
	names := make([]string, 0, len(operations))
	for name, distribution := range operations {
		if distribution.Count > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		distribution := operations[name]
		parts = append(parts, fmt.Sprintf(
			"%s=count:%d,sum:%s,p50<=%s,p95<=%s,p99<=%s",
			name, distribution.Count, distribution.Sum, distribution.P50, distribution.P95, distribution.P99,
		))
	}
	return strings.Join(parts, " ")
}

func formatLegacySchedulerStats(snapshot legacyimport.SchedulerSnapshot) string {
	phaseNames := make([]string, 0, len(snapshot.PhaseTime))
	for name := range snapshot.PhaseTime {
		phaseNames = append(phaseNames, name)
	}
	sort.Strings(phaseNames)
	phases := make([]string, 0, len(phaseNames))
	for _, name := range phaseNames {
		phases = append(phases, fmt.Sprintf("%s:%s", name, snapshot.PhaseTime[name]))
	}
	lanes := make([]string, 0, len(snapshot.LaneTime))
	for active, elapsed := range snapshot.LaneTime {
		if elapsed > 0 {
			lanes = append(lanes, fmt.Sprintf("%d:%s", active, elapsed))
		}
	}
	return fmt.Sprintf(
		"phase=%s phase_time=[%s] lane_time=[%s] active_lanes=%d ready=%d pending_reduction=%d "+
			"retained_bytes=%d unreduced_bytes=%d oldest_unreduced=%s operations=[%s]",
		snapshot.Phase, strings.Join(phases, ","), strings.Join(lanes, ","), snapshot.ActiveLanes,
		snapshot.ReadyBatches, snapshot.PendingReductionBatches, snapshot.RetainedPreparedBytes,
		snapshot.UnreducedPreparedBytes, snapshot.OldestUnreducedAge,
		formatLegacySchedulerOperations(snapshot.Operations),
	)
}

func formatLegacySchedulerOperations(operations map[string]legacyimport.DurationDistribution) string {
	names := make([]string, 0, len(operations))
	for name, distribution := range operations {
		if distribution.Count > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		distribution := operations[name]
		parts = append(parts, fmt.Sprintf(
			"%s=count:%d,sum:%s,p50<=%s,p95<=%s,p99<=%s",
			name, distribution.Count, distribution.Sum, distribution.P50, distribution.P95, distribution.P99,
		))
	}
	return strings.Join(parts, " ")
}

func validateIndexImportOptions(options indexImportOptions) (indexImportOptions, error) {
	if !options.FromLegacy {
		return options, fmt.Errorf("no import source selected; --from-legacy is currently required")
	}
	if options.ImportPublicationLanes > 8 {
		return options, fmt.Errorf("--import-publication-lanes must not exceed 8")
	}
	if options.ImportPublicationLanes > 1 && !options.ForceResetOldIndex {
		return options, fmt.Errorf("--import-publication-lanes >1 requires --force-reset-old-idx (Stage 3 split import is fresh-import-only)")
	}
	if options.PacksPerTransaction > legacyimport.MaxPacksPerTransaction {
		return options, fmt.Errorf("--packs-per-transaction must not exceed %d", legacyimport.MaxPacksPerTransaction)
	}
	if options.ForceResetOldIndex {
		if !options.Daemon.Start {
			return options, fmt.Errorf("--force-reset-old-idx requires --start-daemon")
		}
		if options.DryRun {
			return options, fmt.Errorf("--force-reset-old-idx cannot be combined with --dry-run")
		}
		if options.Daemon.Persistent {
			return options, fmt.Errorf("--force-reset-old-idx cannot be combined with --persistent-daemon because " +
				"the successful import must hand off from memory WAL to local WAL")
		}
		if _, err := validateMetadataRebuildTarget(options.Daemon, true); err != nil {
			return options, fmt.Errorf("validate reset target: %w", err)
		}
		options.Resume = false
		options.Daemon.RebuildReset = true
		options = applyFreshBulkImportDefaults(options)
	}
	if options.ImportPublicationLanes == 0 {
		options.ImportPublicationLanes = 1
	}
	if options.ImportDeferCleanup && (!options.ForceResetOldIndex || options.Daemon.WALStore != "memory" || options.ImportPublicationLanes < 2) {
		return options, fmt.Errorf("--import-defer-cleanup requires a fresh memory-WAL import with at least two publication lanes")
	}
	if options.DryRun && options.Activate {
		return options, fmt.Errorf("--activate cannot be combined with --dry-run")
	}
	return options, nil
}

func prepareMetadataRebuild(ctx context.Context, options indexImportOptions, globalOptions global.Options) error {
	if !options.Daemon.RebuildInitialize {
		return nil
	}
	if !options.ConfirmMetadataLossRebuild || !options.Daemon.Start || !globalOptions.MetadataLossRecovery {
		return fmt.Errorf("metadata rebuild initialization requires --confirm-metadata-loss-rebuild, --start-daemon, and --metadata-loss-recovery")
	}
	candidate, err := validateMetadataRebuildTarget(options.Daemon, options.ForceResetOldIndex)
	if err != nil {
		return err
	}
	if globalOptions.KeyBrokerSocket == "" || options.Daemon.BrokerSocket != globalOptions.KeyBrokerSocket {
		return fmt.Errorf("metadata rebuild requires the repository and candidate daemon to use the same key broker")
	}
	observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryIntegrity, Component: "index",
		Message: "authenticated metadata rebuild started", Fields: map[string]any{"candidate": candidate}})
	return nil
}

func activateImportedIndex(
	ctx context.Context,
	options indexImportOptions,
	repo *repository.Repository,
	client *daemon.Client,
) error {
	if client == nil {
		return fmt.Errorf("repository is already SlateDB-authoritative")
	}
	if err := repo.EnableSlateDBAuthority(ctx, client); err != nil {
		return fmt.Errorf("activate SlateDB authority: %w", err)
	}
	if options.Daemon.RebuildInitialize {
		observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index",
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
	observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryIntegrity, Component: "index",
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

func validateMetadataRebuildTarget(options indexDaemonOptions, allowExisting bool) (string, error) {
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
		if !allowExisting {
			return "", fmt.Errorf("metadata rebuild candidate directory already exists")
		}
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
	closeErr := brokerClient.Close()
	if err != nil {
		return errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return closeErr
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
