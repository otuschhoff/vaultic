package global

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/limiter"
	"github.com/otuschhoff/vaultic/internal/backend/location"
	"github.com/otuschhoff/vaultic/internal/configfile"
	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/otuschhoff/vaultic/internal/options"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/textfile"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/keyvault"
	"github.com/otuschhoff/vaultic/internal/observability"
)

// ErrNoRepository is used to report if opening a repository failed due
// to a missing backend storage location or config file
var ErrNoRepository = errors.New("repository does not exist")

const Version = "0.19.1-dev (compiled manually)"

// TimeFormat is the format used for all timestamps printed by vaultic.
const TimeFormat = "2006-01-02 15:04:05"

type BackendWrapper func(r backend.Backend) (backend.Backend, error)

// Options hold all global options for vaultic.
type Options struct {
	// UseProfiles selects local TOML profiles. Profiles are loaded by the root
	// command after Cobra has parsed flags and before command pre-runs execute.
	UseProfiles []string
	Profile     *configfile.Profile

	Repo                       string
	RepositoryFile             string
	BootstrapProfile           string
	PasswordFile               string
	PasswordCommand            string
	AzureKeyVaultURL           string
	AzureKeyVaultSecret        string
	AzureKeyVaultSecretVersion string
	AzureKeyVaultTimeout       time.Duration
	KeyHint                    string
	// MasterKey* open the repository directly with a master key (no password).
	MasterKey                 string
	MasterKeyFile             string
	MasterKeyCommand          string
	MetadataKeyInDB           bool
	MetadataDaemonSocket      string
	MetadataDaemonPath        string
	MetadataDaemonDataDir     string
	MetadataDaemonObjectStore string
	MetadataDaemonS3Bucket    string
	MetadataDaemonS3Prefix    string
	MetadataEncryptionMode    string
	MetadataPassphraseFile    string
	MetadataAzureTokenFile    string
	MetadataGCPTokenFile      string
	MetadataVaultTokenFile    string
	MetadataPKCS11PINFile     string
	MetadataRecoveryUnlock    bool
	MetadataLossRecovery      bool
	KeyBrokerSocket           string
	KeyBrokerReleaseManifest  string
	KeyBrokerLeaseDuration    time.Duration
	Quiet                     bool
	Verbose                   int
	LogFile                   string
	LogLevel                  string
	NoProgress                bool
	ProgressInterval          time.Duration
	NoLock                    bool
	RetryLock                 time.Duration
	JSON                      bool
	CacheDir                  string
	NoCache                   bool
	CleanupCache              bool
	Compression               repository.CompressionMode
	PackSize                  uint
	TreePackSize              uint
	DataPackSize              uint
	NoExtraVerify             bool
	InsecureNoPassword        bool

	// RepoHot is the location of the hot part of a hot/cold repository
	// (empty for a normal repository).
	RepoHot string

	// Warm-up (cold storage) options; see internal/warmup.
	WarmUpCommand     string
	WarmUpBatch       int
	WarmUpWait        time.Duration
	WarmUpWaitCommand string

	PrometheusURL  string
	PrometheusUser string
	PrometheusPass string
	InfluxURL      string
	InfluxToken    string
	InfluxOrg      string
	InfluxBucket   string
	OpenTelemetry  bool
	SyslogTargets  []string

	backend.TransportOptions
	limiter.Limits

	Password string
	Term     ui.Terminal

	Backends                              *location.Registry
	BackendTestHook, BackendInnerTestHook BackendWrapper

	// Verbosity is set as follows:
	//  0 means: don't print any messages except errors, this is used when --quiet is specified
	//  1 is the default: print essential messages
	//  2 means: print more messages, report minor things, this is used when --verbose is specified
	//  3 means: print very detailed debug messages, this is used when --verbose=2 is specified
	Verbosity uint

	Options []string

	Extended options.Options

	// packSizeFlag and compressionFlag detect if the corresponding CLI flag was set (CLI overrides env).
	// Lookup cannot return nil as the flags are added to the same FlagSet just above.
	packSizeFlag    *pflag.Flag
	compressionFlag *pflag.Flag
	// noExtraVerifyFlag detects if --no-extra-verify was set on the CLI.
	noExtraVerifyFlag *pflag.Flag
	// packSizeFromEnv / compressionFromEnv record whether the value came from
	// the environment (VAULTIC_/RESTIC_), which overrides the in-repo config.
	packSizeFromEnv    bool
	compressionFromEnv bool
}

func (opts *Options) AddFlags(f *pflag.FlagSet) {
	f.StringSliceVarP(&opts.UseProfiles, "use-profile", "P", nil, "load TOML `profile` (repeatable; default: vaultic.toml)")
	f.StringVarP(&opts.Repo, "repo", "r", "", "`repository` to backup to or restore from (default: $VAULTIC_REPOSITORY)")
	f.StringVarP(&opts.RepositoryFile, "repository-file", "", "", "`file` to read the repository location from (default: $VAULTIC_REPOSITORY_FILE)")
	f.StringVar(&opts.BootstrapProfile, "bootstrap-profile", "", "credential-free bootstrap topology profile (default: $VAULTIC_BOOTSTRAP_PROFILE)")
	f.StringVarP(&opts.PasswordFile, "password-file", "p", "", "`file` to read the repository password from (default: $VAULTIC_PASSWORD_FILE)")
	f.StringVarP(&opts.KeyHint, "key-hint", "", "", "`key` ID of key to try decrypting first (default: $VAULTIC_KEY_HINT)")
	f.StringVarP(
		&opts.PasswordCommand,
		"password-command",
		"",
		"",
		"shell `command` to obtain the repository password from (default: $VAULTIC_PASSWORD_COMMAND)",
	)
	f.StringVar(
		&opts.AzureKeyVaultURL,
		"azure-key-vault-url",
		"",
		"Azure Key Vault `URL` containing the repository passphrase (default: $VAULTIC_AZURE_KEY_VAULT_URL)",
	)
	f.StringVar(
		&opts.AzureKeyVaultSecret,
		"azure-key-vault-secret",
		"",
		"Azure Key Vault secret `name` containing the repository passphrase (default: $VAULTIC_AZURE_KEY_VAULT_SECRET)",
	)
	f.StringVar(
		&opts.AzureKeyVaultSecretVersion,
		"azure-key-vault-secret-version",
		"",
		"optional Azure Key Vault secret `version` (default: $VAULTIC_AZURE_KEY_VAULT_SECRET_VERSION)",
	)
	f.DurationVar(
		&opts.AzureKeyVaultTimeout,
		"azure-key-vault-timeout",
		30*time.Second,
		"startup SecretGet `timeout` (default: $VAULTIC_AZURE_KEY_VAULT_TIMEOUT or 30s)",
	)
	f.StringVar(
		&opts.MasterKey,
		"key",
		"",
		"master `key` (base64-encoded JSON) to open the repository directly, bypassing password keys (default: $VAULTIC_KEY)",
	)
	f.StringVar(
		&opts.MasterKeyFile,
		"key-file",
		"",
		"`file` containing the master key (base64-encoded JSON) to open the repository directly (default: $VAULTIC_KEY_FILE)",
	)
	f.StringVar(&opts.MasterKeyCommand, "key-command", "", "shell `command` to obtain the master key from (default: $VAULTIC_KEY_COMMAND)")
	f.BoolVar(&opts.MetadataKeyInDB, "metadata-key-in-db", false, "unlock the repository master key from encrypted SlateDB metadata")
	f.StringVar(&opts.MetadataDaemonSocket, "metadata-daemon-socket", "", "private vaulticdb Unix socket for key-in-DB unlock")
	f.StringVar(&opts.MetadataDaemonPath, "metadata-daemon-path", "", "start this vaulticdb binary for key-in-DB unlock")
	f.StringVar(&opts.MetadataDaemonDataDir, "metadata-daemon-data-dir", "", "local vaulticdb data directory for key-in-DB unlock")
	f.StringVar(&opts.MetadataDaemonObjectStore, "metadata-daemon-object-store", "", "vaulticdb object store for key-in-DB unlock")
	f.StringVar(&opts.MetadataDaemonS3Bucket, "metadata-daemon-s3-bucket", "", "vaulticdb S3 bucket for key-in-DB unlock")
	f.StringVar(&opts.MetadataDaemonS3Prefix, "metadata-daemon-s3-prefix", "", "vaulticdb S3 prefix for key-in-DB unlock")
	f.StringVar(&opts.MetadataEncryptionMode, "metadata-encryption-mode", "", "metadata encryption mode for key-in-DB unlock")
	f.StringVar(&opts.MetadataPassphraseFile, "metadata-passphrase-file", "", "protected metadata recovery passphrase file")
	f.StringVar(&opts.MetadataAzureTokenFile, "metadata-key-db-azure-token-file", "", "protected Azure KMS token file for key-in-DB unlock")
	f.StringVar(&opts.MetadataGCPTokenFile, "metadata-key-db-gcp-token-file", "", "protected Google KMS token file for key-in-DB unlock")
	f.StringVar(&opts.MetadataVaultTokenFile, "metadata-key-db-vault-token-file", "", "protected Vault Transit token file for key-in-DB unlock")
	f.StringVar(&opts.MetadataPKCS11PINFile, "metadata-key-db-pkcs11-pin-file", "", "protected PKCS#11 PIN file for key-in-DB unlock")
	f.BoolVar(&opts.MetadataRecoveryUnlock, "metadata-recovery-ack", false, "acknowledge metadata recovery-slot use")
	f.BoolVar(
		&opts.MetadataLossRecovery,
		"metadata-loss-recovery",
		false,
		"use the legacy JSON index after total SlateDB metadata loss (requires a direct master key)",
	)
	f.StringVar(&opts.KeyBrokerSocket, "key-broker-socket", "", "local vaultic-key-broker Unix socket")
	f.StringVar(&opts.KeyBrokerReleaseManifest, "key-broker-release-manifest", "", "signed release manifest authorizing this Vaultic executable")
	f.DurationVar(&opts.KeyBrokerLeaseDuration, "key-broker-lease", 15*time.Minute, "job-scoped repository-key lease lifetime")

	f.StringVar(&opts.RepoHot, "repo-hot", "", "hot part of a hot/cold `repository` (cold storage; default: $VAULTIC_REPO_HOT)")
	f.StringVar(
		&opts.WarmUpCommand,
		"warm-up-command",
		"",
		"warm-up `command` for cold storage, with %id/%path/%ids/%paths (default: $VAULTIC_WARM_UP_COMMAND)",
	)
	f.IntVar(
		&opts.WarmUpBatch,
		"warm-up-batch",
		1,
		"warm-up `batch` size: packs per %ids invocation or parallel %id invocations (default: $VAULTIC_WARM_UP_BATCH)",
	)
	f.DurationVar(&opts.WarmUpWait, "warm-up-wait", 0, "max `duration` to wait for warm-up to take effect (default: $VAULTIC_WARM_UP_WAIT)")
	f.StringVar(&opts.WarmUpWaitCommand, "warm-up-wait-command", "", "`command` to wait for warmed-up data (default: $VAULTIC_WARM_UP_WAIT_COMMAND)")
	f.BoolVarP(&opts.Quiet, "quiet", "q", false, "do not output comprehensive progress report")
	f.StringVar(&opts.LogFile, "log-file", env.Get("LOG_FILE"), "write library log messages to `file` (default: $VAULTIC_LOG_FILE)")
	f.StringVar(&opts.LogLevel, "log-level", env.Get("LOG_LEVEL"), "minimum log `level` (debug, info, warn, error; default: $VAULTIC_LOG_LEVEL)")
	f.BoolVar(&opts.NoProgress, "no-progress", false, "disable live progress output")
	f.DurationVar(&opts.ProgressInterval, "progress-interval", 0, "refresh live progress every `duration` (default: $VAULTIC_PROGRESS_INTERVAL)")
	// use empty parameter name as `-v, --verbose n` instead of the correct `--verbose=n` is confusing
	f.CountVarP(&opts.Verbose, "verbose", "v", "be verbose (specify multiple times or a level using --verbose=n``, max level/times is 2)")
	f.BoolVar(&opts.NoLock, "no-lock", false, "do not lock the repository, this allows some operations on read-only repositories")
	f.DurationVar(&opts.RetryLock, "retry-lock", 0, "retry to lock the repository if it is already locked, takes a value like 5m or 2h (default: no retries)")
	f.BoolVarP(&opts.JSON, "json", "", false, "set output mode to JSON for commands that support it")
	f.StringVar(&opts.CacheDir, "cache-dir", "", "set the cache `directory`. (default: use system default cache directory)")
	f.BoolVar(&opts.NoCache, "no-cache", false, "do not use a local cache")
	f.StringSliceVar(&opts.RootCertFilenames, "cacert", nil, "`file` to load root certificates from (default: use system certificates or $VAULTIC_CACERT)")
	f.StringVar(
		&opts.TLSClientCertKeyFilename,
		"tls-client-cert",
		"",
		"path to a `file` containing PEM encoded TLS client certificate and private key (default: $VAULTIC_TLS_CLIENT_CERT)",
	)
	f.BoolVar(
		&opts.InsecureNoPassword,
		"insecure-no-password",
		false,
		"use an empty password for the repository, must be passed to every vaultic command (insecure)",
	)
	f.BoolVar(&opts.InsecureTLS, "insecure-tls", false, "skip TLS certificate verification when connecting to the repository (insecure)")
	f.BoolVar(&opts.CleanupCache, "cleanup-cache", false, "auto remove old cache directories")
	const compressionFlag = "compression"
	f.Var(
		&opts.Compression,
		compressionFlag,
		"compression mode (only available for repository format version 2), one of (auto|off|fastest|better|max) (default: $VAULTIC_COMPRESSION)",
	)
	f.BoolVar(&opts.NoExtraVerify, "no-extra-verify", false, "skip additional verification of data before upload (see documentation)")
	f.StringVar(&opts.PrometheusURL, "prometheus", env.Get("PROMETHEUS"), "Pushgateway `URL` for successful backup metrics (default: $VAULTIC_PROMETHEUS)")
	f.StringVar(&opts.PrometheusUser, "prometheus-user", env.Get("PROMETHEUS_USER"), "Pushgateway username (default: $VAULTIC_PROMETHEUS_USER)")
	f.StringVar(&opts.PrometheusPass, "prometheus-pass", env.Get("PROMETHEUS_PASS"), "Pushgateway password (default: $VAULTIC_PROMETHEUS_PASS)")
	f.StringVar(
		&opts.InfluxURL,
		"influxdb-url",
		env.Get("INFLUXDB_URL"),
		"InfluxDB v2-compatible server `URL` for successful backup metrics (default: $VAULTIC_INFLUXDB_URL)",
	)
	f.StringVar(&opts.InfluxToken, "influxdb-token", env.Get("INFLUXDB_TOKEN"), "InfluxDB API token (default: $VAULTIC_INFLUXDB_TOKEN)")
	f.StringVar(&opts.InfluxOrg, "influxdb-org", env.Get("INFLUXDB_ORG"), "InfluxDB organization (default: $VAULTIC_INFLUXDB_ORG)")
	f.StringVar(&opts.InfluxBucket, "influxdb-bucket", env.Get("INFLUXDB_BUCKET"), "InfluxDB bucket (default: $VAULTIC_INFLUXDB_BUCKET)")
	f.BoolVar(&opts.OpenTelemetry, "opentelemetry", false, "emit OpenTelemetry spans through the configured global provider")
	f.StringSliceVar(&opts.SyslogTargets, "syslog-target", nil, "syslog target `URL` (repeatable; udp, tcp, tls, unix, or unixgram)")
	opts.noExtraVerifyFlag = f.Lookup("no-extra-verify")
	f.IntVar(&opts.Limits.UploadKb, "limit-upload", 0, "limits uploads to a maximum `rate` in KiB/s. (default: unlimited)")
	f.IntVar(&opts.Limits.DownloadKb, "limit-download", 0, "limits downloads to a maximum `rate` in KiB/s. (default: unlimited)")
	const packSizeFlag = "pack-size"
	f.UintVar(&opts.PackSize, packSizeFlag, 0, "set target pack `size` in MiB, created pack files may be larger (default: $VAULTIC_PACK_SIZE)")
	f.UintVar(&opts.TreePackSize, "tree-pack-size", 0, "set target tree pack size in MiB (profile/runtime override)")
	f.UintVar(&opts.DataPackSize, "data-pack-size", 0, "set target data pack size in MiB (profile/runtime override)")
	f.StringSliceVarP(&opts.Options, "option", "o", []string{}, "set extended option (`key=value`, can be specified multiple times)")
	f.StringVar(&opts.HTTPUserAgent, "http-user-agent", "", "set a http user agent for outgoing http requests")
	f.DurationVar(&opts.StuckRequestTimeout, "stuck-request-timeout", 5*time.Minute, "`duration` after which to retry stuck requests")

	opts.Repo = env.Get("REPOSITORY")
	opts.RepositoryFile = env.Get("REPOSITORY_FILE")
	opts.BootstrapProfile = env.Get("BOOTSTRAP_PROFILE")
	opts.PasswordFile = env.Get("PASSWORD_FILE")
	opts.KeyHint = env.Get("KEY_HINT")
	opts.PasswordCommand = env.Get("PASSWORD_COMMAND")
	opts.AzureKeyVaultURL = env.Get("AZURE_KEY_VAULT_URL")
	opts.AzureKeyVaultSecret = env.Get("AZURE_KEY_VAULT_SECRET")
	opts.AzureKeyVaultSecretVersion = env.Get("AZURE_KEY_VAULT_SECRET_VERSION")
	if value := env.Get("AZURE_KEY_VAULT_TIMEOUT"); value != "" {
		if timeout, err := time.ParseDuration(value); err == nil {
			opts.AzureKeyVaultTimeout = timeout
		}
	}
	opts.MasterKey = env.Get("KEY")
	opts.MasterKeyFile = env.Get("KEY_FILE")
	opts.MasterKeyCommand = env.Get("KEY_COMMAND")
	opts.MetadataKeyInDB = env.Get("METADATA_KEY_IN_DB") == "true"
	opts.MetadataDaemonSocket = env.Get("METADATA_DAEMON_SOCKET")
	opts.MetadataDaemonPath = env.Get("METADATA_DAEMON_PATH")
	opts.MetadataDaemonDataDir = env.Get("METADATA_DAEMON_DATA_DIR")
	opts.MetadataDaemonObjectStore = env.Get("METADATA_DAEMON_OBJECT_STORE")
	opts.MetadataDaemonS3Bucket = env.Get("METADATA_DAEMON_S3_BUCKET")
	opts.MetadataDaemonS3Prefix = env.Get("METADATA_DAEMON_S3_PREFIX")
	opts.MetadataEncryptionMode = env.Get("METADATA_ENCRYPTION_MODE")
	opts.MetadataPassphraseFile = env.Get("METADATA_PASSPHRASE_FILE")
	opts.MetadataAzureTokenFile = env.Get("METADATA_AZURE_TOKEN_FILE")
	opts.MetadataGCPTokenFile = env.Get("METADATA_GCP_TOKEN_FILE")
	opts.MetadataVaultTokenFile = env.Get("METADATA_VAULT_TOKEN_FILE")
	opts.MetadataPKCS11PINFile = env.Get("METADATA_PKCS11_PIN_FILE")
	opts.MetadataRecoveryUnlock = env.Get("METADATA_RECOVERY_ACK") == "true"
	opts.MetadataLossRecovery = env.Get("METADATA_LOSS_RECOVERY") == "true"
	opts.KeyBrokerSocket = env.Get("KEY_BROKER_SOCKET")
	opts.KeyBrokerReleaseManifest = env.Get("KEY_BROKER_RELEASE_MANIFEST")
	if value := env.Get("KEY_BROKER_LEASE"); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			opts.KeyBrokerLeaseDuration = duration
		}
	}
	opts.RepoHot = env.Get("REPO_HOT")
	opts.WarmUpCommand = env.Get("WARM_UP_COMMAND")
	if v := env.Get("WARM_UP_BATCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			opts.WarmUpBatch = n
		}
	}
	if v := env.Get("WARM_UP_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			opts.WarmUpWait = d
		}
	}
	opts.WarmUpWaitCommand = env.Get("WARM_UP_WAIT_COMMAND")
	if v := env.Get("CACERT"); v != "" {
		opts.RootCertFilenames = strings.Split(v, ",")
	}
	opts.TLSClientCertKeyFilename = env.Get("TLS_CLIENT_CERT")
	opts.packSizeFlag = f.Lookup(packSizeFlag)
	opts.compressionFlag = f.Lookup(compressionFlag)

	if v := env.Get("HTTP_USER_AGENT"); v != "" {
		opts.HTTPUserAgent = v
	}
}

func (opts *Options) PreRun(needsPassword bool) error {
	progress.ClearIntervalOverride()
	if opts.NoProgress && opts.ProgressInterval > 0 {
		return errors.Fatal("--no-progress and --progress-interval cannot be used together")
	}
	if opts.NoProgress {
		progress.SetIntervalOverride(0)
	} else if opts.ProgressInterval > 0 {
		progress.SetIntervalOverride(opts.ProgressInterval)
	}
	if opts.LogLevel != "" {
		switch opts.LogLevel {
		case "debug", "info", "warn", "error":
		default:
			return errors.Fatalf("invalid --log-level %q (expected debug, info, warn, or error)", opts.LogLevel)
		}
	}
	if envVal := env.Get("PACK_SIZE"); envVal != "" && !opts.packSizeFlag.Changed {
		targetPackSize, err := strconv.ParseUint(envVal, 10, 32)
		if err != nil {
			// Failing fast here keeps backups from running for a long time with the wrong pack size.
			return errors.Fatalf("invalid value for VAULTIC_PACK_SIZE (legacy: RESTIC_PACK_SIZE) %q: %v", envVal, err)
		}
		opts.PackSize = uint(targetPackSize)
		opts.packSizeFromEnv = true
	}
	if envVal := env.Get("COMPRESSION"); envVal != "" && !opts.compressionFlag.Changed {
		if err := opts.Compression.Set(envVal); err != nil {
			return errors.Fatalf("invalid value for VAULTIC_COMPRESSION (legacy: RESTIC_COMPRESSION) %q: %v", envVal, err)
		}
		opts.compressionFromEnv = true
	}

	// set verbosity, default is one
	opts.Verbosity = 1
	if opts.Quiet && opts.Verbose > 0 {
		return errors.Fatal("--quiet and --verbose cannot be specified at the same time")
	}

	switch {
	case opts.Verbose >= 2:
		opts.Verbosity = 3
	case opts.Verbose > 0:
		opts.Verbosity = 2
	case opts.Quiet:
		opts.Verbosity = 0
	}

	// parse extended options
	extendedOpts, err := options.Parse(opts.Options)
	if err != nil {
		return err
	}
	opts.Extended = extendedOpts
	if !needsPassword || opts.KeyBrokerSocket != "" {
		return nil
	}
	pwd, err := resolvePassword(opts, "VAULTIC_PASSWORD")
	if err != nil {
		return errors.Fatalf("Resolving password failed: %v", err)
	}
	opts.Password = pwd
	return nil
}

// resolvePassword determines the password to be used for opening the repository.
func resolvePassword(opts *Options, envStr string) (string, error) {
	keyVaultConfigured := opts.AzureKeyVaultURL != "" || opts.AzureKeyVaultSecret != "" || opts.AzureKeyVaultSecretVersion != ""
	if keyVaultConfigured {
		if opts.AzureKeyVaultURL == "" || opts.AzureKeyVaultSecret == "" {
			return "", errors.Fatalf("Azure Key Vault URL and secret name must be specified together")
		}
		if opts.PasswordFile != "" || opts.PasswordCommand != "" || resolvePasswordEnv(envStr) != "" {
			return "", errors.Fatalf("Azure Key Vault and other password sources are mutually exclusive")
		}
		if opts.AzureKeyVaultTimeout <= 0 {
			return "", errors.Fatalf("Azure Key Vault timeout must be positive")
		}
		ctx, cancel := context.WithTimeout(context.Background(), opts.AzureKeyVaultTimeout)
		defer cancel()
		secret, err := keyvault.FetchSecret(ctx, opts.AzureKeyVaultURL, opts.AzureKeyVaultSecret, opts.AzureKeyVaultSecretVersion)
		if err != nil {
			_ = observability.Emit(
				ctx,
				observability.Event{
					Severity:  observability.Error,
					Category:  observability.CategoryAuth,
					Component: "keyvault",
					Message:   "Azure Key Vault SecretGet failed",
					Fields: map[string]any{
						"vault_url":         opts.AzureKeyVaultURL,
						"secret_name":       opts.AzureKeyVaultSecret,
						"version_requested": opts.AzureKeyVaultSecretVersion != "",
					},
				},
			)
			return "", err
		}
		_ = observability.Emit(
			ctx,
			observability.Event{
				Severity:  observability.Notice,
				Category:  observability.CategoryAuth,
				Component: "keyvault",
				Message:   "Azure Key Vault SecretGet completed",
				Fields: map[string]any{
					"vault_url":         opts.AzureKeyVaultURL,
					"secret_name":       opts.AzureKeyVaultSecret,
					"version_requested": opts.AzureKeyVaultSecretVersion != "",
				},
			},
		)
		return secret, nil
	}
	if opts.PasswordFile != "" && opts.PasswordCommand != "" {
		return "", errors.Fatalf("Password file and command are mutually exclusive options")
	}
	if opts.PasswordCommand != "" {
		args, err := backend.SplitShellStrings(opts.PasswordCommand)
		if err != nil {
			return "", err
		}
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stderr = os.Stderr
		output, err := cmd.Output()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(output)), nil
	}
	if opts.PasswordFile != "" {
		return LoadPasswordFromFile(opts.PasswordFile)
	}

	if pwd := resolvePasswordEnv(envStr); pwd != "" {
		return pwd, nil
	}

	return "", nil
}

// resolvePasswordEnv reads the password from the environment variable named
// envStr (a VAULTIC_* name), accepting the legacy RESTIC_* name as fallback.
func resolvePasswordEnv(envStr string) string {
	return env.Get(strings.TrimPrefix(envStr, env.PrimaryPrefix))
}

// LoadPasswordFromFile loads a password from a file while stripping a BOM and
// converting the password to UTF-8.
func LoadPasswordFromFile(pwdFile string) (string, error) {
	s, err := textfile.Read(pwdFile)
	if errors.Is(err, os.ErrNotExist) {
		return "", errors.Fatalf("%s does not exist", pwdFile)
	}
	return strings.TrimSpace(string(s)), errors.Wrap(err, "Readfile")
}

// resolveMasterKey determines the master key (if any) from the --key,
// --key-file and --key-command options (or their environment equivalents).
// The options are mutually exclusive.
func resolveMasterKey(gopts Options) (string, error) {
	set := 0
	for _, v := range []string{gopts.MasterKey, gopts.MasterKeyFile, gopts.MasterKeyCommand} {
		if v != "" {
			set++
		}
	}
	if set == 0 {
		return "", nil
	}
	if set > 1 {
		return "", errors.Fatal("--key, --key-file and --key-command are mutually exclusive")
	}

	if gopts.MasterKey != "" {
		return gopts.MasterKey, nil
	}
	if gopts.MasterKeyFile != "" {
		return LoadPasswordFromFile(gopts.MasterKeyFile)
	}

	// --key-command
	args, err := backend.SplitShellStrings(gopts.MasterKeyCommand)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

// readPassword reads the password from a password file, the environment
// variable VAULTIC_PASSWORD or prompts the user. If the context is canceled,
// the function leaks the password reading goroutine.
func readPassword(ctx context.Context, gopts Options, prompt string) (string, error) {
	if gopts.InsecureNoPassword {
		if gopts.Password != "" {
			return "", errors.Fatal("--insecure-no-password must not be specified together with providing a password via a cli option or environment variable")
		}
		return "", nil
	}

	if gopts.Password != "" {
		return gopts.Password, nil
	}

	password, err := gopts.Term.ReadPassword(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("unable to read password: %w", err)
	}

	if len(password) == 0 {
		return "", errors.Fatal("an empty password is not allowed by default. Pass the flag `--insecure-no-password` to vaultic to disable this check")
	}

	return password, nil
}
