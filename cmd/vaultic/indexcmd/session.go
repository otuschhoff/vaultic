package indexcmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	internalcli "github.com/otuschhoff/vaultic/internal/cli"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/healing"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// RepositoryOpener opens and locks a repository for an index operation.
type RepositoryOpener func(context.Context) (context.Context, *repository.Repository, func() error, error)

// DaemonConnector connects to the repository's metadata daemon.
type DaemonConnector func(context.Context, string) (*daemon.Client, error)

func openWithReadLock(
	ctx context.Context,
	options global.Options,
	noLock bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return internalcli.OpenRepository(ctx, options, internalcli.LockShared, internalcli.OpenOptions{
		AllowNoLock: noLock, LockFreeRead: true,
	}, printer)
}

func openWithExclusiveLock(
	ctx context.Context,
	options global.Options,
	dryRun bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return internalcli.OpenRepository(ctx, options, internalcli.LockExclusive, internalcli.OpenOptions{DryRun: dryRun}, printer)
}

func openWithAppendLock(
	ctx context.Context,
	options global.Options,
	dryRun bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return internalcli.OpenRepository(ctx, options, internalcli.LockShared, internalcli.OpenOptions{
		DryRun: dryRun, AllowNoLock: options.NoLock,
	}, printer)
}

// Session owns every resource used by one index command invocation.
type Session struct {
	Context       context.Context
	Repository    *repository.Repository
	Client        *daemon.Client
	Store         *daemon.SchemaStore
	ArtifactStore healing.Store
	StagingStore  repository.Store

	close []func() error
}

// CloseAndLog closes the session and reports failures for callback APIs that cannot return them.
func (session *Session) CloseAndLog() {
	if err := session.Close(); err != nil {
		debug.Log("close index command session: %v", err)
	}
}

// OpenRepositorySession opens a repository and owns its lock and close lifecycle.
func OpenRepositorySession(ctx context.Context, open RepositoryOpener) (_ *Session, err error) {
	session := &Session{Context: ctx}
	defer func() {
		if err != nil {
			err = errors.Join(err, session.Close())
		}
	}()

	var closeRepository func() error
	session.Context, session.Repository, closeRepository, err = open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open repository: %w", err)
	}
	session.close = append(session.close, session.Repository.Close)
	if closeRepository != nil {
		session.close = append(session.close, closeRepository)
	}
	return session, nil
}

// OpenDaemonSession connects to a repository-scoped metadata daemon.
func OpenDaemonSession(ctx context.Context, connect DaemonConnector, repositoryID string) (*Session, error) {
	session := &Session{Context: ctx}
	if err := session.ConnectDaemon(connect, repositoryID); err != nil {
		return nil, err
	}
	return session, nil
}

func (options indexDaemonOptions) openDaemonSession(ctx context.Context, repositoryID string) (*Session, error) {
	return OpenDaemonSession(ctx, options.connect, repositoryID)
}

func (options indexDaemonOptions) openSession(
	ctx context.Context,
	open RepositoryOpener,
	artifactDirectory string,
) (*Session, error) {
	return OpenSession(ctx, open, options.connect, artifactDirectory)
}

// ConnectDaemon attaches the repository daemon and transfers its lifecycle to the session.
func (session *Session) ConnectDaemon(connect DaemonConnector, repositoryID string) error {
	if session.Repository != nil {
		if engine, ok := session.Repository.Engine().(*metadataindex.DaemonEngine); ok {
			session.Client = engine.Client()
			session.Store = engine.SchemaStore()
			return nil
		}
	}
	client, err := connect(session.Context, repositoryID)
	if err != nil {
		return fmt.Errorf("connect vaulticdb: %w", err)
	}
	session.Client = client
	session.Store = daemon.NewSchemaStore(client)
	session.close = append(session.close, func() error {
		return client.Close(session.Context)
	})
	return nil
}

func (session *Session) connectDaemon(options indexDaemonOptions, repositoryID string) error {
	return session.ConnectDaemon(options.connect, repositoryID)
}

func withDaemonSession[T any](
	ctx context.Context,
	options indexDaemonOptions,
	repositoryID string,
	action func(*daemon.Client) (T, error),
) (T, error) {
	session, err := options.openDaemonSession(ctx, repositoryID)
	if err != nil {
		var zero T
		return zero, err
	}
	return runWithSession(session, func() (T, error) {
		return action(session.Client)
	})
}

func runWithSession[T any](session *Session, action func() (T, error)) (result T, err error) {
	defer func() {
		err = errors.Join(err, session.Close())
	}()
	return action()
}

// OpenSession opens the repository, resolves or connects its daemon, and
// derives the repository-scoped healing artifact store.
func OpenSession(ctx context.Context, open RepositoryOpener, connect DaemonConnector, artifactDirectory string) (_ *Session, err error) {
	session, err := OpenRepositorySession(ctx, open)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, session.Close())
		}
	}()

	if err = session.ConnectDaemon(connect, session.Repository.Config().ID); err != nil {
		return nil, fmt.Errorf("%w (use --start-daemon to start one)", err)
	}

	session.ArtifactStore = healing.Store{Directory: artifactDirectory}
	return session, nil
}

func openReadDaemonSession(
	ctx context.Context,
	globalOptions global.Options,
	daemonOptions indexDaemonOptions,
	repositoryID string,
	printer vaultic.Printer,
) (*Session, error) {
	configuredContext, err := indexDaemonContext(ctx, daemonOptions, repositoryID)
	if err != nil {
		return nil, err
	}
	return daemonOptions.openSession(configuredContext, func(ctx context.Context) (context.Context, *repository.Repository, func() error, error) {
		lockedContext, repo, unlock, openErr := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
		return lockedContext, repo, func() error { unlock(); return nil }, openErr
	}, "")
}

func openStoreSession(ctx context.Context, repo *repository.Repository, options indexDaemonOptions) (*Session, error) {
	session := &Session{Context: ctx, Repository: repo}
	if err := session.connectDaemon(options, repo.Config().ID); err != nil {
		return nil, fmt.Errorf("%w (use --start-daemon to start one)", err)
	}
	return session, nil
}

func openIndexStore(
	ctx context.Context,
	repo *repository.Repository,
	options indexDaemonOptions,
) (*daemon.SchemaStore, *daemon.Client, func(), error) {
	session, err := openStoreSession(ctx, repo, options)
	if err != nil {
		return nil, nil, func() {}, err
	}
	client := session.Client
	if _, repositoryOwned := repo.Engine().(*metadataindex.DaemonEngine); repositoryOwned {
		client = nil
	}
	return session.Store, client, func() {
		session.CloseAndLog()
	}, nil
}

// OpenStagingSession opens a repository and derives its authenticated staging store.
func OpenStagingSession(ctx context.Context, open RepositoryOpener) (_ *Session, err error) {
	session, err := OpenRepositorySession(ctx, open)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, session.Close())
		}
	}()
	session.StagingStore, err = newStagingStore(session.Repository)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func withStagingSession(
	ctx context.Context,
	globalOptions *global.Options,
	action func(context.Context, repository.Store, string) error,
) (err error) {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	session, err := OpenStagingSession(ctx, func(ctx context.Context) (context.Context, *repository.Repository, func() error, error) {
		lockedContext, repo, unlock, openErr := openWithReadLock(ctx, *globalOptions, globalOptions.NoLock, printer)
		return lockedContext, repo, func() error { unlock(); return nil }, openErr
	})
	if err != nil {
		return err
	}
	_, err = runWithSession(session, func() (struct{}, error) {
		return struct{}{}, action(ctx, session.StagingStore, session.Repository.Config().ID)
	})
	return err
}

func newStagingStore(repo *repository.Repository) (repository.Store, error) {
	config := repo.Config()
	if len(config.StagingBackends) == 0 {
		return repository.Store{}, fmt.Errorf("repository has no configured staging backends")
	}
	mirrors := make(map[string]backend.Backend, len(config.StagingBackends))
	mirrorPlacements := make(map[string]repository.MirrorPlacement, len(config.StagingBackends))
	configured := make(map[string]vaultic.PlacementBackend, len(config.PlacementBackends))
	for _, placement := range config.PlacementBackends {
		configured[placement.ID] = placement
	}
	for _, id := range config.StagingBackends {
		placementBackend, ok := repo.PlacementBackend(repository.PlacementBackendHash(id))
		if !ok {
			return repository.Store{}, fmt.Errorf("staging backend %q is not open", id)
		}
		mirrors[id] = placementBackend
		placement := configured[id]
		mirrorPlacements[id] = repository.MirrorPlacement{FailureDomain: placement.FailureDomain, Offsite: placement.Offsite}
	}
	key, err := repository.DeriveJournalKey(repo.Key().EncryptionKey[:], config.ID)
	if err != nil {
		return repository.Store{}, err
	}
	policy := config.PlacementPolicy
	maxExtension := time.Duration(config.StagingQuota.MaxExtensionSeconds) * time.Second
	if maxExtension == 0 {
		maxExtension = 30 * 24 * time.Hour
	}
	return repository.Store{
		Mirrors: mirrors, MirrorPlacements: mirrorPlacements, Key: key,
		Policy: repository.Policy{
			MinCopies: policy.MinCopies, MinDomains: policy.MinDomains, MinOffsite: policy.MinOffsite,
		},
		MaxExtension: maxExtension,
	}, nil
}

// Close releases all session-owned resources in reverse acquisition order.
func (session *Session) Close() error {
	var closeErr error
	for index := len(session.close) - 1; index >= 0; index-- {
		closeErr = errors.Join(closeErr, session.close[index]())
	}
	session.close = nil
	return closeErr
}
