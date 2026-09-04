package global

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
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

// OpenRepository reads the password and opens the repository.
func OpenRepository(ctx context.Context, gopts Options, printer vaultic.Printer) (*repository.Repository, error) {
	var bootstrapMasterKey string
	var bootstrapBroker *indexbroker.Client
	if gopts.BootstrapProfile != "" {
		if gopts.Repo != "" || gopts.RepositoryFile != "" {
			return nil, errors.Fatal("--bootstrap-profile is mutually exclusive with --repo and --repository-file")
		}
		var err error
		gopts.Repo, bootstrapMasterKey, bootstrapBroker, err = resolveBootstrapRepository(ctx, gopts, printer)
		if err != nil {
			return nil, err
		}
		if bootstrapBroker != nil {
			defer func() {
				if bootstrapBroker != nil {
					errors.LogClose(bootstrapBroker, "close bootstrap broker", debug.Log)
				}
			}()
		}
	}
	repo, err := readRepo(gopts)
	if err != nil {
		return nil, err
	}

	be, err := innerOpenBackend(ctx, repo, gopts, gopts.Extended, false, printer)
	if err != nil {
		return nil, err
	}

	err = hasRepositoryConfig(ctx, be, repo, gopts)
	if err != nil {
		return nil, err
	}

	s, err := createRepositoryInstance(be, gopts)
	if err != nil {
		return nil, err
	}
	if bootstrapMasterKey != "" {
		err = s.UseMasterKey(ctx, bootstrapMasterKey)
		if err == nil && bootstrapBroker != nil {
			s.AddOwnedCloser(bootstrapBroker)
			bootstrapBroker = nil
		}
	} else if gopts.KeyBrokerSocket != "" {
		if gopts.KeyBrokerReleaseManifest == "" {
			return nil, errors.Fatal("--key-broker-release-manifest is required with --key-broker-socket")
		}
		if gopts.MetadataKeyInDB || gopts.MasterKey != "" || gopts.MasterKeyFile != "" || gopts.MasterKeyCommand != "" || gopts.Password != "" || gopts.PasswordFile != "" || gopts.PasswordCommand != "" || gopts.AzureKeyVaultURL != "" || gopts.InsecureNoPassword {
			return nil, errors.Fatal("brokered unlock is mutually exclusive with password, direct-key, Azure-secret, and key-in-DB routes")
		}
		client, connectErr := indexbroker.Dial(ctx, gopts.KeyBrokerSocket)
		if connectErr != nil {
			return nil, connectErr
		}
		capability := "repository-master-key"
		if gopts.MetadataLossRecovery {
			capability = "metadata-loss-recovery"
		}
		lease, leaseErr := client.AcquireLease(ctx, gopts.KeyBrokerReleaseManifest, capability, gopts.KeyBrokerLeaseDuration)
		if leaseErr != nil {
			errors.LogClose(client, "close key broker after lease failure", debug.Log)
			return nil, leaseErr
		}
		if lease.ExpiresUnixMS <= uint64(time.Now().UnixMilli()) {
			clear(lease.Key)
			errors.LogClose(client, "close key broker after expired lease", debug.Log)
			return nil, errors.Fatal("key broker returned an expired repository lease")
		}
		err = s.UseMasterKey(ctx, string(lease.Key))
		clear(lease.Key)
		if err != nil {
			errors.LogClose(client, "close key broker after repository unlock failure", debug.Log)
			return nil, errors.Fatalf("open repository with broker lease: %v", err)
		}
		s.AddOwnedCloser(client)
	}
	if gopts.MetadataLossRecovery {
		if gopts.KeyBrokerSocket == "" && (gopts.MetadataKeyInDB || (gopts.MasterKey == "" && gopts.MasterKeyFile == "" && gopts.MasterKeyCommand == "")) {
			return nil, errors.Fatal("metadata-loss recovery requires --key, --key-file, or --key-command and cannot use --metadata-key-in-db")
		}
		ctx = repository.WithMetadataLossRecovery(ctx)
		_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "repository", Message: "legacy metadata-loss recovery selected"})
	}

	if gopts.KeyBrokerSocket != "" {
		// The repository was already authenticated using the job-scoped broker lease.
	} else if gopts.MetadataKeyInDB {
		var resolution metadataindex.Resolution
		resolution, err = metadataindex.Resolve(ctx, be, "")
		if err != nil || resolution.Mode != metadataindex.ModeSlateDB || resolution.Manifest == nil {
			return nil, errors.Fatalf("discover repository identity for key-in-DB unlock: %v", err)
		}
		options := daemon.Options{
			Socket:           gopts.MetadataDaemonSocket,
			RepositoryID:     resolution.Manifest.RepositoryID,
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
		ctx = repository.WithDaemonOptions(ctx, options)
		var client *daemon.Client
		if options.DaemonPath != "" {
			client, err = daemon.Ensure(ctx, options)
		} else {
			client, err = daemon.Connect(ctx, options)
		}
		if err != nil {
			return nil, errors.Fatalf("unlock encrypted metadata for repository: %v", err)
		}
		masterKey, found, keyErr := client.GetMasterKey(ctx)
		errors.LogCleanup("close metadata daemon client", func() error { return client.Close(ctx) }, debug.Log)
		if keyErr != nil {
			return nil, errors.Fatalf("read repository master key from encrypted metadata: %v", keyErr)
		}
		if !found {
			return nil, errors.Fatalf("encrypted metadata does not contain a repository master key")
		}
		err = s.UseMasterKey(ctx, string(masterKey))
		clear(masterKey)
		if err == nil && s.Config().ID != resolution.Manifest.RepositoryID {
			err = errors.Fatalf("key-in-DB repository identity mismatch")
		}
	} else {
		err = decryptRepository(ctx, s, &gopts, printer)
	}
	if err != nil {
		return nil, err
	}

	// apply the in-repo config (compression, pack sizes, extra verify);
	// CLI flags and environment variables take precedence
	if err := applyRepoConfig(s, gopts); err != nil {
		return nil, err
	}
	openedPlacements := make(map[string]backend.Backend, len(s.Config().PlacementBackends))
	placementFailures := make(map[string]error)
	for _, placement := range s.Config().PlacementBackends {
		if placement.Location == "" {
			continue
		}
		placementOptions := gopts
		placementOptions.RepoHot = ""
		placementBackend, openErr := innerOpenBackend(ctx, placement.Location, placementOptions, placementOptions.Extended, false, printer)
		if openErr != nil {
			placementFailures[placement.ID] = openErr
			printer.E("unable to open placement backend %q: %v\n", placement.ID, openErr)
			continue
		}
		s.AttachPlacementBackend(repository.PlacementBackendHash(placement.ID), placementBackend)
		openedPlacements[placement.ID] = placementBackend
	}
	if len(s.Config().StagingBackends) > 0 {
		_, store, planErr := s.DeferredUploadPlan()
		if planErr != nil {
			errors.LogClose(s, "close repository after staging policy failure", debug.Log)
			return nil, errors.Fatalf("reachable staging backends do not satisfy repository policy (%v): %v", placementFailures, planErr)
		}
		s.AttachStagedPackRoots(staging.PackRoots{Store: store, RepositoryID: s.Config().ID})
	}

	if _, err := s.ResolveEngineFromBackend(ctx); err != nil {
		errors.LogClose(s, "close repository after metadata engine failure", debug.Log)
		return nil, errors.Fatalf("resolve repository metadata engine: %v", err)
	}

	printRepositoryInfo(s, gopts, printer)

	if gopts.NoCache {
		return s, nil
	}

	err = setupCache(s, gopts, printer)
	if err != nil {
		return nil, err
	}
	return s, nil
}
