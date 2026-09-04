package global

import (
	"context"
	"fmt"
	"os"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/location"
	"github.com/otuschhoff/vaultic/internal/debug"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/bootstrap"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/otuschhoff/vaultic/internal/errors"
)

func resolveBootstrapRepository(ctx context.Context, gopts Options, printer vaultic.Printer) (string, string, *indexbroker.Client, error) {
	profile, err := bootstrap.LoadProfile(gopts.BootstrapProfile)
	if err != nil {
		return "", "", nil, err
	}
	masterKey, err := resolveMasterKey(gopts)
	if err != nil {
		return "", "", nil, err
	}
	var brokerClient *indexbroker.Client
	var topologyKey []byte
	if gopts.KeyBrokerSocket != "" {
		if masterKey != "" || gopts.MetadataKeyInDB || gopts.Password != "" || gopts.PasswordFile != "" || gopts.PasswordCommand != "" {
			return "", "", nil, errors.Fatal("brokered bootstrap is mutually exclusive with password, direct-key, and key-in-DB routes")
		}
		if gopts.KeyBrokerReleaseManifest == "" {
			return "", "", nil, errors.Fatal("--key-broker-release-manifest is required with brokered bootstrap")
		}
		brokerClient, err = indexbroker.Dial(ctx, gopts.KeyBrokerSocket)
		if err != nil {
			return "", "", nil, err
		}
		lease, leaseErr := brokerClient.AcquireLease(ctx, gopts.KeyBrokerReleaseManifest, "topology-discovery", gopts.KeyBrokerLeaseDuration)
		if leaseErr != nil {
			errors.LogClose(brokerClient, "close bootstrap broker after lease failure", debug.Log)
			return "", "", nil, leaseErr
		}
		topologyKey = append([]byte(nil), lease.Key...)
		clear(lease.Key)
	}
	if masterKey == "" && len(topologyKey) == 0 {
		return "", "", brokerClient, errors.Fatal("bootstrap topology authentication requires a broker lease or direct repository key")
	}
	var rootKey []byte
	if masterKey != "" {
		key, decodeErr := repository.DecodeMasterKey(masterKey)
		if decodeErr != nil {
			if brokerClient != nil {
				errors.LogClose(brokerClient, "close bootstrap broker after key decode failure", debug.Log)
			}
			return "", "", nil, decodeErr
		}
		rootKey = key.EncryptionKey[:]
	}
	seeds := make(map[string]backend.Backend, len(profile.Seeds))
	locations := make(map[string]string, len(profile.Seeds))
	for _, seed := range profile.Seeds {
		seedBackend, openErr := innerOpenBackend(ctx, seed.Location, gopts, gopts.Extended, false, printer)
		if openErr != nil {
			continue
		}
		seeds[seed.ID] = seedBackend
		locations[seed.ID] = seed.Location
	}
	defer func() {
		for _, seed := range seeds {
			errors.LogClose(seed, "close bootstrap seed", debug.Log)
		}
	}()
	if len(seeds) == 0 {
		if brokerClient != nil {
			errors.LogClose(brokerClient, "close bootstrap broker without reachable seeds", debug.Log)
		}
		return "", "", nil, errors.Fatal("no bootstrap seed backend is reachable")
	}
	var copies []bootstrap.Copy
	var failures map[string]error
	if len(topologyKey) > 0 {
		copies, failures = bootstrap.DiscoverWithTopologyKey(ctx, seeds, topologyKey, profile.RepositoryID)
		clear(topologyKey)
	} else {
		copies, failures = bootstrap.Discover(ctx, seeds, rootKey, profile.RepositoryID)
	}
	if len(copies) == 0 {
		if brokerClient != nil {
			errors.LogClose(brokerClient, "close bootstrap broker without authenticated topology", debug.Log)
		}
		return "", "", nil, errors.Errorf("no authenticated bootstrap topology is reachable: %v", failures)
	}
	trusted := make([]bootstrap.Anchor, 0, 1)
	if profile.AnchorFile != "" {
		anchor, anchorErr := bootstrap.LoadAnchor(profile.AnchorFile)
		if anchorErr != nil && !os.IsNotExist(anchorErr) {
			if brokerClient != nil {
				errors.LogClose(brokerClient, "close bootstrap broker after anchor failure", debug.Log)
			}
			return "", "", nil, anchorErr
		}
		if anchorErr == nil {
			trusted = append(trusted, anchor)
		}
	}
	winner, err := bootstrap.Resolve(copies, trusted...)
	if err != nil {
		if brokerClient != nil {
			errors.LogClose(brokerClient, "close bootstrap broker after topology resolution failure", debug.Log)
		}
		return "", "", nil, err
	}
	reachable := make([]vaultic.PlacementBackend, 0, len(winner.Manifest.Backends))
	for _, declared := range winner.Manifest.Backends {
		candidate, openErr := innerOpenBackend(ctx, declared.Location, gopts, gopts.Extended, false, printer)
		if openErr != nil {
			continue
		}
		errors.LogClose(candidate, "close bootstrap policy candidate", debug.Log)
		reachable = append(reachable, declared)
	}
	if _, err := bootstrap.EvaluatePolicy(reachable, winner.Manifest.Policy); err != nil {
		if brokerClient != nil {
			errors.LogClose(brokerClient, "close bootstrap broker after policy failure", debug.Log)
		}
		return "", "", nil, err
	}
	location := locations[winner.Seed]
	if location == "" {
		return "", "", brokerClient, errors.Fatal("winning bootstrap topology has no configured seed locator")
	}
	if profile.AnchorFile != "" {
		if err := bootstrap.StoreAnchor(profile.AnchorFile, bootstrap.Anchor{RepositoryID: winner.Manifest.RepositoryID, Generation: winner.Manifest.Generation, SHA256: winner.SHA256}); err != nil {
			if brokerClient != nil {
				errors.LogClose(brokerClient, "close bootstrap broker after anchor update failure", debug.Log)
			}
			return "", "", nil, err
		}
	}
	if brokerClient != nil {
		lease, leaseErr := brokerClient.AcquireLease(ctx, gopts.KeyBrokerReleaseManifest, "repository-master-key", gopts.KeyBrokerLeaseDuration)
		if leaseErr != nil {
			errors.LogClose(brokerClient, "close bootstrap broker after repository lease failure", debug.Log)
			return "", "", nil, leaseErr
		}
		masterKey = string(lease.Key)
		clear(lease.Key)
	}
	return location, masterKey, brokerClient, nil
}

func OpenDataPlaneRepository(ctx context.Context, gopts Options, printer vaultic.Printer) (*repository.Repository, error) {
	if gopts.BootstrapProfile == "" {
		return nil, errors.Fatal("data-plane-only mode requires --bootstrap-profile")
	}
	location, masterKey, brokerClient, err := resolveBootstrapRepository(ctx, gopts, printer)
	if err != nil {
		return nil, err
	}
	if brokerClient != nil {
		defer func() {
			if brokerClient != nil {
				errors.LogClose(brokerClient, "close data-plane bootstrap broker", debug.Log)
			}
		}()
	}
	manifest, err := discoverBootstrapManifest(ctx, gopts, masterKey, printer)
	if err != nil {
		return nil, err
	}
	cfg, err := manifest.ConfigProjection()
	if err != nil {
		return nil, err
	}
	primary, err := innerOpenBackend(ctx, location, gopts, gopts.Extended, false, printer)
	if err != nil {
		return nil, err
	}
	repo, err := createRepositoryInstance(primary, gopts)
	if err != nil {
		errors.LogClose(primary, "close data-plane primary backend", debug.Log)
		return nil, err
	}
	if err := repo.UseDataPlaneMasterKey(masterKey, cfg); err != nil {
		errors.LogClose(repo, "close data-plane repository after key failure", debug.Log)
		return nil, err
	}
	for _, declared := range manifest.Backends {
		placementOptions := gopts
		placementOptions.RepoHot = ""
		destination, openErr := innerOpenBackend(ctx, declared.Location, placementOptions, placementOptions.Extended, false, printer)
		if openErr != nil {
			continue
		}
		repo.AttachPlacementBackend(repository.PlacementBackendHash(declared.ID), destination)
	}
	if _, _, err := repo.DeferredUploadPlan(); err != nil {
		errors.LogClose(repo, "close data-plane repository after staging failure", debug.Log)
		return nil, err
	}
	if brokerClient != nil {
		repo.AddOwnedCloser(brokerClient)
		brokerClient = nil
	}
	return repo, nil
}

func discoverBootstrapManifest(ctx context.Context, gopts Options, masterKey string, printer vaultic.Printer) (bootstrap.Manifest, error) {
	profile, err := bootstrap.LoadProfile(gopts.BootstrapProfile)
	if err != nil {
		return bootstrap.Manifest{}, err
	}
	key, err := repository.DecodeMasterKey(masterKey)
	if err != nil {
		return bootstrap.Manifest{}, err
	}
	seeds := make(map[string]backend.Backend, len(profile.Seeds))
	for _, seed := range profile.Seeds {
		destination, openErr := innerOpenBackend(ctx, seed.Location, gopts, gopts.Extended, false, printer)
		if openErr == nil {
			seeds[seed.ID] = destination
		}
	}
	defer func() {
		for _, seed := range seeds {
			errors.LogClose(seed, "close manifest discovery seed", debug.Log)
		}
	}()
	copies, failures := bootstrap.Discover(ctx, seeds, key.EncryptionKey[:], profile.RepositoryID)
	if len(copies) == 0 {
		return bootstrap.Manifest{}, errors.Errorf("no authenticated bootstrap topology is reachable: %v", failures)
	}
	trusted := make([]bootstrap.Anchor, 0, 1)
	if profile.AnchorFile != "" {
		anchor, anchorErr := bootstrap.LoadAnchor(profile.AnchorFile)
		if anchorErr != nil {
			return bootstrap.Manifest{}, anchorErr
		}
		trusted = append(trusted, anchor)
	}
	winner, err := bootstrap.Resolve(copies, trusted...)
	if err != nil {
		return bootstrap.Manifest{}, err
	}
	return winner.Manifest, nil
}

// hasRepositoryConfig checks if the repository config file exists and is not empty.
func hasRepositoryConfig(ctx context.Context, be backend.Backend, repo string, gopts Options) error {
	fi, err := be.Stat(ctx, backend.Handle{Type: backend.ConfigFile})
	if be.IsNotExist(err) {
		//nolint:staticcheck // capitalized error string is intentional
		return fmt.Errorf(
			"Fatal: %w: unable to open config file: %w\nIs there a repository at the following location?\n%v",
			ErrNoRepository, err, location.StripPassword(gopts.Backends, repo),
		)
	}
	if err != nil {
		return errors.Fatalf("unable to open config file: %v\n%v", err, location.StripPassword(gopts.Backends, repo))
	}

	if fi.Size == 0 {
		return errors.New("config file has zero size, invalid repository?")
	}

	return nil
}

// createRepositoryInstance creates a new repository instance with the given options.
func createRepositoryInstance(be backend.Backend, gopts Options) (*repository.Repository, error) {
	s, err := repository.New(be, repository.Options{
		Compression:   gopts.Compression,
		PackSize:      gopts.PackSize * 1024 * 1024,
		TreePackSize:  uint64(gopts.TreePackSize) * 1024 * 1024,
		DataPackSize:  uint64(gopts.DataPackSize) * 1024 * 1024,
		NoExtraVerify: gopts.NoExtraVerify,
	})
	if err != nil {
		return nil, errors.Fatalf("%s", err)
	}
	return s, nil
}
