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

func resolveBootstrapRepository(
	ctx context.Context,
	gopts Options,
	printer vaultic.Printer,
) (string, string, *indexbroker.Client, error) {
	profile, err := bootstrap.LoadProfile(gopts.BootstrapProfile)
	if err != nil {
		return "", "", nil, err
	}
	masterKey, rootKey, topologyKey, brokerClient, err := resolveBootstrapCredentials(ctx, gopts)
	if err != nil {
		return "", "", nil, err
	}
	seeds, locations := openBootstrapSeeds(ctx, profile, gopts, printer)
	defer func() {
		for _, seed := range seeds {
			errors.LogClose(seed, "close bootstrap seed", debug.Log)
		}
	}()
	if len(seeds) == 0 {
		closeBootstrapBroker(brokerClient, "close bootstrap broker without reachable seeds")
		return "", "", nil, errors.Fatal("no bootstrap seed backend is reachable")
	}
	copies, failures := discoverBootstrapCopies(ctx, seeds, profile.RepositoryID, rootKey, topologyKey)
	if len(copies) == 0 {
		closeBootstrapBroker(brokerClient, "close bootstrap broker without authenticated topology")
		return "", "", nil, errors.Errorf("no authenticated bootstrap topology is reachable: %v", failures)
	}
	winner, err := resolveBootstrapTopology(profile.AnchorFile, copies)
	if err != nil {
		closeBootstrapBroker(brokerClient, "close bootstrap broker after topology resolution failure")
		return "", "", nil, err
	}
	if err := validateBootstrapPolicy(ctx, winner.Manifest, gopts, printer); err != nil {
		closeBootstrapBroker(brokerClient, "close bootstrap broker after policy failure")
		return "", "", nil, err
	}
	location := locations[winner.Seed]
	if location == "" {
		return "", "", brokerClient, errors.Fatal("winning bootstrap topology has no configured seed locator")
	}
	if err := storeBootstrapAnchor(profile.AnchorFile, winner); err != nil {
		closeBootstrapBroker(brokerClient, "close bootstrap broker after anchor update failure")
		return "", "", nil, err
	}
	masterKey, err = acquireBootstrapMasterKey(ctx, gopts, brokerClient, masterKey)
	if err != nil {
		return "", "", nil, err
	}
	return location, masterKey, brokerClient, nil
}

func resolveBootstrapCredentials(
	ctx context.Context,
	gopts Options,
) (string, []byte, []byte, *indexbroker.Client, error) {
	masterKey, err := resolveMasterKey(gopts)
	if err != nil {
		return "", nil, nil, nil, err
	}
	if gopts.KeyBrokerSocket == "" {
		if masterKey == "" {
			return "", nil, nil, nil, errors.Fatal(
				"bootstrap topology authentication requires a broker lease or direct repository key",
			)
		}
		key, err := repository.DecodeMasterKey(masterKey)
		if err != nil {
			return "", nil, nil, nil, err
		}
		return masterKey, append([]byte(nil), key.EncryptionKey[:]...), nil, nil, nil
	}
	if masterKey != "" || gopts.MetadataKeyInDB || gopts.Password != "" || gopts.PasswordFile != "" || gopts.PasswordCommand != "" {
		return "", nil, nil, nil, errors.Fatal(
			"brokered bootstrap is mutually exclusive with password, direct-key, and key-in-DB routes",
		)
	}
	if gopts.KeyBrokerReleaseManifest == "" {
		return "", nil, nil, nil, errors.Fatal("--key-broker-release-manifest is required with brokered bootstrap")
	}
	client, err := indexbroker.Dial(ctx, gopts.KeyBrokerSocket)
	if err != nil {
		return "", nil, nil, nil, err
	}
	lease, err := client.AcquireLease(ctx, gopts.KeyBrokerReleaseManifest, "topology-discovery", gopts.KeyBrokerLeaseDuration)
	if err != nil {
		closeBootstrapBroker(client, "close bootstrap broker after lease failure")
		return "", nil, nil, nil, err
	}
	topologyKey := append([]byte(nil), lease.Key...)
	clear(lease.Key)
	return "", nil, topologyKey, client, nil
}

func openBootstrapSeeds(
	ctx context.Context,
	profile bootstrap.Profile,
	gopts Options,
	printer vaultic.Printer,
) (map[string]backend.Backend, map[string]string) {
	seeds := make(map[string]backend.Backend, len(profile.Seeds))
	locations := make(map[string]string, len(profile.Seeds))
	for _, seed := range profile.Seeds {
		seedBackend, err := innerOpenBackend(ctx, seed.Location, gopts, gopts.Extended, false, printer)
		if err == nil {
			seeds[seed.ID] = seedBackend
			locations[seed.ID] = seed.Location
		}
	}
	return seeds, locations
}

func discoverBootstrapCopies(
	ctx context.Context,
	seeds map[string]backend.Backend,
	repositoryID string,
	rootKey, topologyKey []byte,
) ([]bootstrap.Copy, map[string]error) {
	if len(topologyKey) == 0 {
		return bootstrap.Discover(ctx, seeds, rootKey, repositoryID)
	}
	copies, failures := bootstrap.DiscoverWithTopologyKey(ctx, seeds, topologyKey, repositoryID)
	clear(topologyKey)
	return copies, failures
}

func resolveBootstrapTopology(anchorFile string, copies []bootstrap.Copy) (bootstrap.Copy, error) {
	trusted := make([]bootstrap.Anchor, 0, 1)
	if anchorFile != "" {
		anchor, err := bootstrap.LoadAnchor(anchorFile)
		if err != nil && !os.IsNotExist(err) {
			return bootstrap.Copy{}, err
		}
		if err == nil {
			trusted = append(trusted, anchor)
		}
	}
	return bootstrap.Resolve(copies, trusted...)
}

func validateBootstrapPolicy(ctx context.Context, manifest bootstrap.Manifest, gopts Options, printer vaultic.Printer) error {
	reachable := make([]vaultic.PlacementBackend, 0, len(manifest.Backends))
	for _, declared := range manifest.Backends {
		candidate, err := innerOpenBackend(ctx, declared.Location, gopts, gopts.Extended, false, printer)
		if err != nil {
			continue
		}
		errors.LogClose(candidate, "close bootstrap policy candidate", debug.Log)
		reachable = append(reachable, declared)
	}
	_, err := bootstrap.EvaluatePolicy(reachable, manifest.Policy)
	return err
}

func storeBootstrapAnchor(anchorFile string, winner bootstrap.Copy) error {
	if anchorFile == "" {
		return nil
	}
	return bootstrap.StoreAnchor(anchorFile, bootstrap.Anchor{
		RepositoryID: winner.Manifest.RepositoryID,
		Generation:   winner.Manifest.Generation,
		SHA256:       winner.SHA256,
	})
}

func acquireBootstrapMasterKey(
	ctx context.Context,
	gopts Options,
	client *indexbroker.Client,
	masterKey string,
) (string, error) {
	if client == nil {
		return masterKey, nil
	}
	lease, err := client.AcquireLease(
		ctx, gopts.KeyBrokerReleaseManifest, "repository-master-key", gopts.KeyBrokerLeaseDuration,
	)
	if err != nil {
		closeBootstrapBroker(client, "close bootstrap broker after repository lease failure")
		return "", err
	}
	masterKey = string(lease.Key)
	clear(lease.Key)
	return masterKey, nil
}

func closeBootstrapBroker(client *indexbroker.Client, reason string) {
	if client != nil {
		errors.LogClose(client, reason, debug.Log)
	}
}

func OpenDataPlaneRepository(
	ctx context.Context,
	gopts Options,
	printer vaultic.Printer,
) (*repository.Repository, error) {
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
		destination, openErr := innerOpenBackend(
			ctx,
			declared.Location,
			placementOptions,
			placementOptions.Extended,
			false,
			printer,
		)
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

func discoverBootstrapManifest(
	ctx context.Context,
	gopts Options,
	masterKey string,
	printer vaultic.Printer,
) (bootstrap.Manifest, error) {
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
