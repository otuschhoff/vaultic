package global

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/debug"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	"github.com/otuschhoff/vaultic/internal/textfile"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/observability"
)

// ReadPasswordTwice calls ReadPassword two times and returns an error when the
// passwords don't match. If the context is canceled, the function leaks the
// password reading goroutine.
func ReadPasswordTwice(ctx context.Context, globalOptions Options, prompt1, prompt2 string) (string, error) {
	pw1, err := readPassword(ctx, globalOptions, prompt1)
	if err != nil {
		return "", err
	}
	if globalOptions.Term.InputIsTerminal() {
		pw2, err := readPassword(ctx, globalOptions, prompt2)
		if err != nil {
			return "", err
		}

		if pw1 != pw2 {
			return "", errors.Fatal("passwords do not match")
		}
	}

	return pw1, nil
}

func readRepo(globalOptions Options) (string, error) {
	if globalOptions.Repo == "" && globalOptions.RepositoryFile == "" {
		return "", errors.Fatal("Please specify repository location (-r or --repository-file)")
	}

	repo := globalOptions.Repo
	if globalOptions.RepositoryFile != "" {
		if repo != "" {
			return "", errors.Fatal("Options -r and --repository-file are mutually exclusive, please specify only one")
		}

		s, err := textfile.Read(globalOptions.RepositoryFile)
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.Fatalf("%s does not exist", globalOptions.RepositoryFile)
		}
		if err != nil {
			return "", err
		}

		repo = strings.TrimSpace(string(s))
	}

	return repo, nil
}

const maxKeys = 20

type repositoryLocation struct {
	globalOptions      Options
	repository         string
	bootstrapMasterKey string
	bootstrapBroker    *indexbroker.Client
}

// OpenRepository reads the password and opens the repository.
func OpenRepository(ctx context.Context, globalOptions Options, printer vaultic.Printer) (*repository.Repository, error) {
	location, err := resolveRepositoryLocation(ctx, globalOptions, printer)
	if err != nil {
		return nil, err
	}
	defer func() {
		if location.bootstrapBroker != nil {
			errors.LogClose(location.bootstrapBroker, "close bootstrap broker", debug.Log)
		}
	}()

	s, openCtx, err := openAndAuthenticate(ctx, &location, printer)
	if err != nil {
		return nil, err
	}
	if err := attachEngine(openCtx, s, location.globalOptions, printer); err != nil {
		return nil, err
	}

	return s, nil
}

func resolveRepositoryLocation(ctx context.Context, globalOptions Options, printer vaultic.Printer) (repositoryLocation, error) {
	location := repositoryLocation{globalOptions: globalOptions}
	if globalOptions.BootstrapProfile != "" {
		if globalOptions.Repo != "" || globalOptions.RepositoryFile != "" {
			return repositoryLocation{}, errors.Fatal("--bootstrap-profile is mutually exclusive with --repo and --repository-file")
		}
		var err error
		location.globalOptions.Repo, location.bootstrapMasterKey, location.bootstrapBroker, err = resolveBootstrapRepository(ctx, globalOptions, printer)
		if err != nil {
			return repositoryLocation{}, err
		}
	}

	repo, err := readRepo(location.globalOptions)
	if err != nil {
		if location.bootstrapBroker != nil {
			errors.LogClose(location.bootstrapBroker, "close bootstrap broker", debug.Log)
		}
		return repositoryLocation{}, err
	}
	location.repository = repo

	return location, nil
}

func openAndAuthenticate(ctx context.Context, location *repositoryLocation, printer vaultic.Printer) (*repository.Repository, context.Context, error) {
	globalOptions := location.globalOptions
	be, err := innerOpenBackend(ctx, location.repository, globalOptions, globalOptions.Extended, false, printer)
	if err != nil {
		return nil, ctx, err
	}

	if err := hasRepositoryConfig(ctx, be, location.repository, globalOptions); err != nil {
		return nil, ctx, err
	}

	s, err := createRepositoryInstance(be, globalOptions)
	if err != nil {
		return nil, ctx, err
	}
	if err := authenticateRepository(ctx, s, location); err != nil {
		return nil, ctx, err
	}

	if globalOptions.MetadataLossRecovery {
		if err := validateMetadataLossRecoveryOptions(globalOptions); err != nil {
			return nil, ctx, err
		}
		ctx = repository.WithMetadataLossRecovery(ctx)
		observability.EmitBestEffort(
			ctx,
			observability.Event{
				Severity:  observability.Warning,
				Category:  observability.CategoryLifecycle,
				Component: "repository",
				Message:   "legacy metadata-loss recovery selected",
			},
		)
	}

	if globalOptions.KeyBrokerSocket == "" {
		if globalOptions.MetadataKeyInDB {
			ctx, err = authenticateWithMetadataKey(ctx, s, globalOptions)
		} else {
			err = decryptRepository(ctx, s, &globalOptions, printer)
		}
		if err != nil {
			return nil, ctx, err
		}
	}

	return s, ctx, nil
}

func authenticateRepository(ctx context.Context, s *repository.Repository, location *repositoryLocation) error {
	if location.bootstrapMasterKey != "" {
		err := s.UseMasterKey(ctx, location.bootstrapMasterKey)
		if err == nil && location.bootstrapBroker != nil {
			s.AddOwnedCloser(location.bootstrapBroker)
			location.bootstrapBroker = nil
		}
		return err
	}
	if location.globalOptions.KeyBrokerSocket != "" {
		return authenticateWithBroker(ctx, s, location.globalOptions)
	}

	return nil
}

func validateBrokeredUnlockOptions(globalOptions Options) error {
	if globalOptions.KeyBrokerReleaseManifest == "" {
		return errors.Fatal("--key-broker-release-manifest is required with --key-broker-socket")
	}
	if globalOptions.MetadataKeyInDB || globalOptions.MasterKey != "" || globalOptions.MasterKeyFile != "" ||
		globalOptions.MasterKeyCommand != "" || globalOptions.Password != "" ||
		globalOptions.PasswordFile != "" ||
		globalOptions.PasswordCommand != "" ||
		globalOptions.AzureKeyVaultURL != "" ||
		globalOptions.InsecureNoPassword {
		return errors.Fatal("brokered unlock is mutually exclusive with password, direct-key, Azure-secret, and key-in-DB routes")
	}
	return nil
}

func authenticateWithBroker(ctx context.Context, s *repository.Repository, globalOptions Options) error {
	if err := validateBrokeredUnlockOptions(globalOptions); err != nil {
		return err
	}
	client, err := indexbroker.Dial(ctx, globalOptions.KeyBrokerSocket)
	if err != nil {
		return err
	}
	capability := "repository-master-key"
	if globalOptions.MetadataLossRecovery {
		capability = "metadata-loss-recovery"
	}
	lease, err := client.AcquireLease(ctx, globalOptions.KeyBrokerReleaseManifest, capability, globalOptions.KeyBrokerLeaseDuration)
	if err != nil {
		errors.LogClose(client, "close key broker after lease failure", debug.Log)
		return err
	}
	if lease.ExpiresUnixMS <= uint64(time.Now().UnixMilli()) {
		clear(lease.Key)
		errors.LogClose(client, "close key broker after expired lease", debug.Log)
		return errors.Fatal("key broker returned an expired repository lease")
	}
	err = s.UseMasterKey(ctx, string(lease.Key))
	clear(lease.Key)
	if err != nil {
		errors.LogClose(client, "close key broker after repository unlock failure", debug.Log)
		return errors.Fatalf("open repository with broker lease: %v", err)
	}
	s.AddOwnedCloser(client)
	return nil
}

func validateMetadataLossRecoveryOptions(globalOptions Options) error {
	if globalOptions.KeyBrokerSocket == "" && (globalOptions.MetadataKeyInDB ||
		(globalOptions.MasterKey == "" && globalOptions.MasterKeyFile == "" && globalOptions.MasterKeyCommand == "")) {
		return errors.Fatal("metadata-loss recovery requires --key, --key-file, or --key-command and cannot use --metadata-key-in-db")
	}
	return nil
}

func authenticateWithMetadataKey(ctx context.Context, s *repository.Repository, globalOptions Options) (context.Context, error) {
	resolution, err := metadataindex.Resolve(ctx, s.Backend(), "")
	if err != nil || resolution.Mode != metadataindex.ModeSlateDB || resolution.Manifest == nil {
		return ctx, errors.Fatalf("discover repository identity for key-in-DB unlock: %v", err)
	}
	options := metadataDaemonOptions(globalOptions, resolution.Manifest.RepositoryID)
	ctx = repository.WithDaemonOptions(ctx, options)
	var client *daemon.Client
	if options.DaemonPath != "" {
		client, err = daemon.Ensure(ctx, options)
	} else {
		client, err = daemon.Connect(ctx, options)
	}
	if err != nil {
		return ctx, errors.Fatalf("unlock encrypted metadata for repository: %v", err)
	}
	masterKey, found, keyErr := client.GetMasterKey(ctx)
	errors.LogCleanup("close metadata daemon client", func() error { return client.Close(ctx) }, debug.Log)
	if keyErr != nil {
		return ctx, errors.Fatalf("read repository master key from encrypted metadata: %v", keyErr)
	}
	if !found {
		return ctx, errors.Fatalf("encrypted metadata does not contain a repository master key")
	}
	err = s.UseMasterKey(ctx, string(masterKey))
	clear(masterKey)
	if err == nil && s.Config().ID != resolution.Manifest.RepositoryID {
		err = errors.Fatalf("key-in-DB repository identity mismatch")
	}
	return ctx, err
}

func metadataDaemonOptions(globalOptions Options, repositoryID string) daemon.Options {
	return daemon.Options{
		Socket:           globalOptions.MetadataDaemonSocket,
		RepositoryID:     repositoryID,
		DaemonPath:       globalOptions.MetadataDaemonPath,
		PersistentDaemon: true,
		DataDir:          globalOptions.MetadataDaemonDataDir,
		ObjectStore:      globalOptions.MetadataDaemonObjectStore,
		S3Bucket:         globalOptions.MetadataDaemonS3Bucket,
		S3Prefix:         globalOptions.MetadataDaemonS3Prefix,
		EncryptionMode:   globalOptions.MetadataEncryptionMode,
		PassphraseFile:   globalOptions.MetadataPassphraseFile,
		AzureTokenFile:   globalOptions.MetadataAzureTokenFile,
		GCPTokenFile:     globalOptions.MetadataGCPTokenFile,
		VaultTokenFile:   globalOptions.MetadataVaultTokenFile,
		PKCS11PINFile:    globalOptions.MetadataPKCS11PINFile,
		RecoveryUnlock:   globalOptions.MetadataRecoveryUnlock,
	}
}

func attachEngine(ctx context.Context, s *repository.Repository, globalOptions Options, printer vaultic.Printer) error {
	if err := applyRepoConfig(s, globalOptions); err != nil {
		return err
	}
	placementFailures := attachPlacementBackends(ctx, s, globalOptions, printer)
	if len(s.Config().StagingBackends) > 0 {
		_, store, err := s.DeferredUploadPlan()
		if err != nil {
			errors.LogClose(s, "close repository after staging policy failure", debug.Log)
			return errors.Fatalf("reachable staging backends do not satisfy repository policy (%v): %v", placementFailures, err)
		}
		s.AttachStagedPackRoots(staging.PackRoots{Store: store, RepositoryID: s.Config().ID})
	}

	if _, err := s.ResolveEngineFromBackend(ctx); err != nil {
		errors.LogClose(s, "close repository after metadata engine failure", debug.Log)
		return errors.Fatalf("resolve repository metadata engine: %v", err)
	}

	printRepositoryInfo(s, globalOptions, printer)
	if globalOptions.NoCache {
		return nil
	}
	return setupCache(s, globalOptions, printer)
}

func attachPlacementBackends(ctx context.Context, s *repository.Repository, globalOptions Options, printer vaultic.Printer) map[string]error {
	placementFailures := make(map[string]error)
	for _, placement := range s.Config().PlacementBackends {
		if placement.Location == "" {
			continue
		}
		placementOptions := globalOptions
		placementOptions.RepoHot = ""
		placementBackend, err := innerOpenBackend(ctx, placement.Location, placementOptions, placementOptions.Extended, false, printer)
		if err != nil {
			placementFailures[placement.ID] = err
			printer.E("unable to open placement backend %q: %v\n", placement.ID, err)
			continue
		}
		s.AttachPlacementBackend(repository.PlacementBackendHash(placement.ID), placementBackend)
	}
	return placementFailures
}
