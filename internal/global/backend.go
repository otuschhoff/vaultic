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
func ReadPasswordTwice(ctx context.Context, gopts Options, prompt1, prompt2 string) (string, error) {
	pw1, err := readPassword(ctx, gopts, prompt1)
	if err != nil {
		return "", err
	}
	if gopts.Term.InputIsTerminal() {
		pw2, err := readPassword(ctx, gopts, prompt2)
		if err != nil {
			return "", err
		}

		if pw1 != pw2 {
			return "", errors.Fatal("passwords do not match")
		}
	}

	return pw1, nil
}

func readRepo(gopts Options) (string, error) {
	if gopts.Repo == "" && gopts.RepositoryFile == "" {
		return "", errors.Fatal("Please specify repository location (-r or --repository-file)")
	}

	repo := gopts.Repo
	if gopts.RepositoryFile != "" {
		if repo != "" {
			return "", errors.Fatal("Options -r and --repository-file are mutually exclusive, please specify only one")
		}

		s, err := textfile.Read(gopts.RepositoryFile)
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.Fatalf("%s does not exist", gopts.RepositoryFile)
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
	gopts              Options
	repository         string
	bootstrapMasterKey string
	bootstrapBroker    *indexbroker.Client
}

// OpenRepository reads the password and opens the repository.
func OpenRepository(ctx context.Context, gopts Options, printer vaultic.Printer) (*repository.Repository, error) {
	location, err := resolveRepositoryLocation(ctx, gopts, printer)
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
	if err := attachEngine(openCtx, s, location.gopts, printer); err != nil {
		return nil, err
	}

	return s, nil
}

func resolveRepositoryLocation(ctx context.Context, gopts Options, printer vaultic.Printer) (repositoryLocation, error) {
	location := repositoryLocation{gopts: gopts}
	if gopts.BootstrapProfile != "" {
		if gopts.Repo != "" || gopts.RepositoryFile != "" {
			return repositoryLocation{}, errors.Fatal("--bootstrap-profile is mutually exclusive with --repo and --repository-file")
		}
		var err error
		location.gopts.Repo, location.bootstrapMasterKey, location.bootstrapBroker, err = resolveBootstrapRepository(ctx, gopts, printer)
		if err != nil {
			return repositoryLocation{}, err
		}
	}

	repo, err := readRepo(location.gopts)
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
	gopts := location.gopts
	be, err := innerOpenBackend(ctx, location.repository, gopts, gopts.Extended, false, printer)
	if err != nil {
		return nil, ctx, err
	}

	if err := hasRepositoryConfig(ctx, be, location.repository, gopts); err != nil {
		return nil, ctx, err
	}

	s, err := createRepositoryInstance(be, gopts)
	if err != nil {
		return nil, ctx, err
	}
	if err := authenticateRepository(ctx, s, location); err != nil {
		return nil, ctx, err
	}

	if gopts.MetadataLossRecovery {
		if err := validateMetadataLossRecoveryOptions(gopts); err != nil {
			return nil, ctx, err
		}
		ctx = repository.WithMetadataLossRecovery(ctx)
		_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "repository", Message: "legacy metadata-loss recovery selected"})
	}

	if gopts.KeyBrokerSocket == "" {
		if gopts.MetadataKeyInDB {
			ctx, err = authenticateWithMetadataKey(ctx, s, gopts)
		} else {
			err = decryptRepository(ctx, s, &gopts, printer)
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
	if location.gopts.KeyBrokerSocket != "" {
		return authenticateWithBroker(ctx, s, location.gopts)
	}

	return nil
}

func validateBrokeredUnlockOptions(gopts Options) error {
	if gopts.KeyBrokerReleaseManifest == "" {
		return errors.Fatal("--key-broker-release-manifest is required with --key-broker-socket")
	}
	if gopts.MetadataKeyInDB || gopts.MasterKey != "" || gopts.MasterKeyFile != "" || gopts.MasterKeyCommand != "" || gopts.Password != "" || gopts.PasswordFile != "" || gopts.PasswordCommand != "" || gopts.AzureKeyVaultURL != "" || gopts.InsecureNoPassword {
		return errors.Fatal("brokered unlock is mutually exclusive with password, direct-key, Azure-secret, and key-in-DB routes")
	}
	return nil
}

func authenticateWithBroker(ctx context.Context, s *repository.Repository, gopts Options) error {
	if err := validateBrokeredUnlockOptions(gopts); err != nil {
		return err
	}
	client, err := indexbroker.Dial(ctx, gopts.KeyBrokerSocket)
	if err != nil {
		return err
	}
	capability := "repository-master-key"
	if gopts.MetadataLossRecovery {
		capability = "metadata-loss-recovery"
	}
	lease, err := client.AcquireLease(ctx, gopts.KeyBrokerReleaseManifest, capability, gopts.KeyBrokerLeaseDuration)
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

func validateMetadataLossRecoveryOptions(gopts Options) error {
	if gopts.KeyBrokerSocket == "" && (gopts.MetadataKeyInDB || (gopts.MasterKey == "" && gopts.MasterKeyFile == "" && gopts.MasterKeyCommand == "")) {
		return errors.Fatal("metadata-loss recovery requires --key, --key-file, or --key-command and cannot use --metadata-key-in-db")
	}
	return nil
}

func authenticateWithMetadataKey(ctx context.Context, s *repository.Repository, gopts Options) (context.Context, error) {
	resolution, err := metadataindex.Resolve(ctx, s.Backend(), "")
	if err != nil || resolution.Mode != metadataindex.ModeSlateDB || resolution.Manifest == nil {
		return ctx, errors.Fatalf("discover repository identity for key-in-DB unlock: %v", err)
	}
	options := metadataDaemonOptions(gopts, resolution.Manifest.RepositoryID)
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

func metadataDaemonOptions(gopts Options, repositoryID string) daemon.Options {
	return daemon.Options{
		Socket:           gopts.MetadataDaemonSocket,
		RepositoryID:     repositoryID,
		DaemonPath:       gopts.MetadataDaemonPath,
		PersistentDaemon: true,
		DataDir:          gopts.MetadataDaemonDataDir,
		ObjectStore:      gopts.MetadataDaemonObjectStore,
		S3Bucket:         gopts.MetadataDaemonS3Bucket,
		S3Prefix:         gopts.MetadataDaemonS3Prefix,
		EncryptionMode:   gopts.MetadataEncryptionMode,
		PassphraseFile:   gopts.MetadataPassphraseFile,
		AzureTokenFile:   gopts.MetadataAzureTokenFile,
		GCPTokenFile:     gopts.MetadataGCPTokenFile,
		VaultTokenFile:   gopts.MetadataVaultTokenFile,
		PKCS11PINFile:    gopts.MetadataPKCS11PINFile,
		RecoveryUnlock:   gopts.MetadataRecoveryUnlock,
	}
}

func attachEngine(ctx context.Context, s *repository.Repository, gopts Options, printer vaultic.Printer) error {
	if err := applyRepoConfig(s, gopts); err != nil {
		return err
	}
	placementFailures := attachPlacementBackends(ctx, s, gopts, printer)
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

	printRepositoryInfo(s, gopts, printer)
	if gopts.NoCache {
		return nil
	}
	return setupCache(s, gopts, printer)
}

func attachPlacementBackends(ctx context.Context, s *repository.Repository, gopts Options, printer vaultic.Printer) map[string]error {
	placementFailures := make(map[string]error)
	for _, placement := range s.Config().PlacementBackends {
		if placement.Location == "" {
			continue
		}
		placementOptions := gopts
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
