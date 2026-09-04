package global

import (
	"context"
	"os"
	"path/filepath"

	"github.com/otuschhoff/vaultic/internal/backend/cache"
	"github.com/otuschhoff/vaultic/internal/backend/location"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/restic/chunker"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/errors"
)

// flagChanged reports whether the given flag was explicitly set on the CLI.
// It tolerates a nil flag (e.g. when Options was built without AddFlags, as
// in tests), treating it as "not set".
func flagChanged(f *pflag.Flag) bool {
	return f != nil && f.Changed
}

// applyRepoConfig merges the in-repo config into the repository options,
// honoring the precedence CLI flag > environment > repo config > default.
func applyRepoConfig(s *repository.Repository, gopts Options) error {
	cfg := s.Config()

	// compression: repo config is a zstd level (-7..22); CLI/env use named modes
	if cfg.Compression != nil && !flagChanged(gopts.compressionFlag) && !gopts.compressionFromEnv {
		c, err := compressionFromLevel(*cfg.Compression)
		if err != nil {
			return err
		}
		s.SetCompression(c)
	}

	// extra verification (default on; config can disable it, CLI --no-extra-verify wins)
	if !cfg.ExtraVerifyEnabled() && !flagChanged(gopts.noExtraVerifyFlag) {
		s.SetNoExtraVerify(true)
	}

	// per-type pack sizes from the config (CLI --pack-size / env override the
	// generic target but not an explicit per-type config value)
	treeSize, treeLimit, treeGrow := cfg.TreePackSize()
	dataSize, dataLimit, dataGrow := cfg.DataPackSize()
	if cfg.TreePackSizeBytes != 0 || cfg.TreePackGrowFactor != nil || cfg.TreePackSizeLimitBytes != 0 {
		s.SetTreePackSizeConfig(treeSize, treeLimit, treeGrow)
	}
	if cfg.DataPackSizeBytes != 0 || cfg.DataPackGrowFactor != nil || cfg.DataPackSizeLimitBytes != 0 {
		s.SetDataPackSizeConfig(dataSize, dataLimit, dataGrow)
	}
	if gopts.TreePackSize != 0 {
		s.SetTreePackSize(uint64(gopts.TreePackSize)*1024*1024, 0)
	}
	if gopts.DataPackSize != 0 {
		s.SetDataPackSize(uint64(gopts.DataPackSize)*1024*1024, 0)
	}
	return nil
}

// compressionFromLevel maps a zstd compression level from the repo config
// onto the closest named CompressionMode.
func compressionFromLevel(level int) (repository.CompressionMode, error) {
	switch {
	case level == 0:
		return repository.CompressionOff, nil
	case level < 0:
		return repository.CompressionFastest, nil
	case level <= 3:
		// zstd default levels map to "auto"
		return repository.CompressionAuto, nil
	case level <= 9:
		return repository.CompressionBetter, nil
	case level <= 22:
		return repository.CompressionMax, nil
	default:
		return repository.CompressionInvalid, errors.Fatalf("invalid compression level %d in repository config", level)
	}
}

// decryptRepository handles password reading and decrypts the repository.
func decryptRepository(ctx context.Context, s *repository.Repository, gopts *Options, printer vaultic.Printer) error {
	// opening via a master key bypasses the password-based key files
	if mk, err := resolveMasterKey(*gopts); err != nil {
		return err
	} else if mk != "" {
		err := s.UseMasterKey(ctx, mk)
		if err != nil {
			return errors.Fatalf("%s", err)
		}
		return nil
	}

	passwordTriesLeft := 1
	if gopts.Term.InputIsTerminal() && gopts.Password == "" && !gopts.InsecureNoPassword {
		passwordTriesLeft = 3
	}

	var err error
	for ; passwordTriesLeft > 0; passwordTriesLeft-- {
		gopts.Password, err = readPassword(ctx, *gopts, "enter password for repository: ")
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && passwordTriesLeft > 1 {
			gopts.Password = ""
			printer.E("%s. Try again", err)
		}
		if err != nil {
			continue
		}

		err = s.SearchKey(ctx, gopts.Password, maxKeys, gopts.KeyHint)
		if err != nil && passwordTriesLeft > 1 {
			gopts.Password = ""
			printer.E("%s. Try again", err)
		}
	}
	if err != nil {
		if errors.IsFatal(err) || errors.Is(err, repository.ErrNoKeyFound) {
			return err
		}
		return errors.Fatalf("%s", err)
	}

	return nil
}

// printRepositoryInfo displays the repository ID, version and compression level.
func printRepositoryInfo(s *repository.Repository, gopts Options, printer vaultic.Printer) {
	id := s.Config().ID
	if len(id) > 8 {
		id = id[:8]
	}
	extra := ""
	if s.Config().Version >= 2 {
		extra = ", compression level " + gopts.Compression.String()
	}
	printer.PT("repository %v opened (version %v%s)", id, s.Config().Version, extra)
}

// setupCache creates a new cache and removes old cache directories if instructed to do so.
func setupCache(s *repository.Repository, gopts Options, printer vaultic.Printer) error {
	c, err := cache.New(s.Config().ID, gopts.CacheDir)
	if err != nil {
		printer.E("unable to open cache: %v", err)
		return err
	}

	if c.Created {
		printer.PT("created new cache in %v", c.Base)
	}

	// start using the cache
	s.UseCache(c, printer.E)

	oldCacheDirs, err := cache.Old(c.Base)
	if err != nil {
		printer.E("unable to find old cache directories: %v", err)
	}

	// nothing more to do if no old cache dirs could be found
	if len(oldCacheDirs) == 0 {
		return nil
	}

	// cleanup old cache dirs if instructed to do so
	if gopts.CleanupCache {
		printer.PT("removing %d old cache dirs from %v", len(oldCacheDirs), c.Base)
		for _, item := range oldCacheDirs {
			dir := filepath.Join(c.Base, item.Name())
			err = os.RemoveAll(dir)
			if err != nil {
				printer.E("unable to remove %v: %v", dir, err)
			}
		}
	} else {
		printer.PT("found %d old cache directories in %v, run `vaultic cache --cleanup` to remove them",
			len(oldCacheDirs), c.Base)
	}
	return nil
}

// CreateRepository a repository with the given version and chunker polynomial.
func CreateRepository(
	ctx context.Context,
	gopts Options,
	version uint,
	chunkerPolynomial *chunker.Pol,
	printer vaultic.Printer,
) (*repository.Repository, error) {
	if version < vaultic.MinRepoVersion || version > vaultic.MaxRepoVersion {
		return nil, errors.Fatalf("only repository versions between %v and %v are allowed", vaultic.MinRepoVersion, vaultic.MaxRepoVersion)
	}

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

	err = s.Init(ctx, version, gopts.Password, chunkerPolynomial)
	if err != nil {
		return nil, errors.Fatalf("create key in repository at %s failed: %v", location.StripPassword(gopts.Backends, repo), err)
	}

	return s, nil
}
