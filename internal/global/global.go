package global

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/hotcold"
	"github.com/otuschhoff/vaultic/internal/backend/limiter"
	"github.com/otuschhoff/vaultic/internal/backend/location"
	"github.com/otuschhoff/vaultic/internal/backend/logger"
	"github.com/otuschhoff/vaultic/internal/backend/retry"
	"github.com/otuschhoff/vaultic/internal/backend/sema"
	"github.com/otuschhoff/vaultic/internal/backend/warmupcmd"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/options"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/warmup"

	"github.com/otuschhoff/vaultic/internal/errors"
)

// CreateRepositoryWithConfig initializes a repository at gopts.Repo with the
// given (already populated) config instead of a freshly generated one. Used to
// create the hot part of a hot/cold repository (shared repository ID). If
// masterKey is non-nil it is reused instead of generating a new one (shared
// master key between hot and cold parts).
func CreateRepositoryWithConfig(ctx context.Context, gopts Options, cfg vaultic.Config, masterKey *crypto.Key, printer vaultic.Printer) (*repository.Repository, error) {
	repo, err := readRepo(gopts)
	if err != nil {
		return nil, err
	}

	gopts.Password, err = ReadPasswordTwice(ctx, gopts,
		"enter password for new repository: ",
		"enter password again: ")
	if err != nil {
		return nil, err
	}

	be, err := innerOpenBackend(ctx, repo, gopts, gopts.Extended, true, printer)
	if err != nil {
		return nil, errors.Fatalf("create repository at %s failed: %v", location.StripPassword(gopts.Backends, repo), err)
	}

	s, err := createRepositoryInstance(be, gopts)
	if err != nil {
		return nil, err
	}

	if err := s.InitWithConfigAndKey(ctx, gopts.Password, cfg, masterKey); err != nil {
		return nil, errors.Fatalf("create key in repository at %s failed: %v", location.StripPassword(gopts.Backends, repo), err)
	}

	return s, nil
}

func innerOpenBackend(ctx context.Context, s string, gopts Options, opts options.Options, create bool, printer vaultic.Printer) (backend.Backend, error) {
	debug.Log("parsing location %v", location.StripPassword(gopts.Backends, s))

	scheme, cfg, err := parseConfig(gopts.Backends, s, opts)
	if err != nil {
		return nil, err
	}

	rt, lim, err := setupTransport(gopts)
	if err != nil {
		return nil, err
	}

	be, err := createOrOpenBackend(ctx, scheme, cfg, rt, lim, gopts, s, create, printer)
	if err != nil {
		return nil, err
	}

	be, err = wrapBackend(be, gopts, printer)
	if err != nil {
		return nil, err
	}

	// for a hot/cold repository, also open the hot part and combine both;
	// `s` is the cold (complete) repository, RepoHot the metadata cache
	if gopts.RepoHot != "" {
		hotGopts := gopts
		hotGopts.RepoHot = "" // open the hot part as a plain repository
		hotBe, err := innerOpenBackend(ctx, gopts.RepoHot, hotGopts, hotGopts.Extended, create, printer)
		if err != nil {
			return nil, errors.Errorf("unable to open hot repository %v: %w", location.StripPassword(gopts.Backends, gopts.RepoHot), err)
		}
		debug.Log("using hot/cold repository (hot: %v)", location.StripPassword(gopts.Backends, gopts.RepoHot))
		be = hotcold.New(hotBe, be)
	}

	return be, nil
}

// parseConfig parses the repository location and extended options and returns the scheme and configuration.
func parseConfig(backends *location.Registry, s string, opts options.Options) (string, any, error) {
	loc, err := location.Parse(backends, s)
	if err != nil {
		return "", nil, errors.Fatalf("parsing repository location failed: %v", err)
	}

	cfg := loc.Config
	if cfg, ok := cfg.(backend.ApplyEnvironmenter); ok {
		cfg.ApplyEnvironment("")
	}

	// only apply options for a particular backend here
	opts = opts.Extract(loc.Scheme)
	if err := opts.Apply(loc.Scheme, cfg); err != nil {
		return "", nil, err
	}

	debug.Log("opening %v repository at %#v", loc.Scheme, cfg)
	return loc.Scheme, cfg, nil
}

// setupTransport creates and configures the transport with rate limiting.
func setupTransport(gopts Options) (http.RoundTripper, limiter.Limiter, error) {
	rt, err := backend.Transport(gopts.TransportOptions)
	if err != nil {
		return nil, nil, errors.Fatalf("%s", err)
	}

	// wrap the transport so that the throughput via HTTP is limited
	lim := limiter.NewStaticLimiter(gopts.Limits)
	rt = lim.Transport(rt)

	return rt, lim, nil
}

// createOrOpenBackend creates or opens a backend using the appropriate factory method.
func createOrOpenBackend(ctx context.Context, scheme string, cfg any, rt http.RoundTripper, lim limiter.Limiter, gopts Options, s string, create bool, printer vaultic.Printer) (backend.Backend, error) {
	factory := gopts.Backends.Lookup(scheme)
	if factory == nil {
		return nil, errors.Fatalf("invalid backend: %q", scheme)
	}

	var be backend.Backend
	var err error
	if create {
		be, err = factory.Create(ctx, cfg, rt, lim, printer.E)
	} else {
		be, err = factory.Open(ctx, cfg, rt, lim, printer.E)
	}

	if errors.Is(err, backend.ErrNoRepository) {
		//nolint:staticcheck // capitalized error string is intentional
		return nil, fmt.Errorf("Fatal: %w at %v: %w", ErrNoRepository, location.StripPassword(gopts.Backends, s), err)
	}
	if err != nil {
		if create {
			// init already wraps the error message
			return nil, err
		}
		return nil, errors.Fatalf("unable to open repository at %v: %v", location.StripPassword(gopts.Backends, s), err)
	}

	return be, nil
}

// wrapBackend applies debug logging, test hooks, and retry wrapper to the backend.
func wrapBackend(be backend.Backend, gopts Options, printer vaultic.Printer) (backend.Backend, error) {
	// wrap with debug logging and connection limiting
	be = logger.New(sema.NewBackend(be))

	// route warm-up to the user-supplied warm-up command if configured
	if gopts.WarmUpCommand != "" {
		runner := warmup.New(warmup.Options{
			Command:     gopts.WarmUpCommand,
			Batch:       gopts.WarmUpBatch,
			Wait:        gopts.WarmUpWait,
			WaitCommand: gopts.WarmUpWaitCommand,
		}, nil, func(msg string) { printer.V("%s", msg) })
		be = warmupcmd.NewWarmupCommandBackend(be, runner, nil)
	}

	// wrap backend if a test specified an inner hook
	if gopts.BackendInnerTestHook != nil {
		var err error
		be, err = gopts.BackendInnerTestHook(be)
		if err != nil {
			return nil, err
		}
	}

	report := func(msg string, err error, d time.Duration) {
		if d >= 0 {
			printer.E("%v returned error, retrying after %v: %v", msg, d, err)
		} else {
			printer.E("%v failed: %v", msg, err)
		}
	}
	success := func(msg string, retries int) {
		printer.E("%v operation successful after %d retries", msg, retries)
	}
	be = retry.New(be, 15*time.Minute, report, success)

	// wrap backend if a test specified a hook
	if gopts.BackendTestHook != nil {
		var err error
		be, err = gopts.BackendTestHook(be)
		if err != nil {
			return nil, err
		}
	}

	return be, nil
}
