package main

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/restic/chunker"
	"github.com/vaultic/vaultic/internal/backend/location"
	"github.com/vaultic/vaultic/internal/errors"
	"github.com/vaultic/vaultic/internal/global"
	"github.com/vaultic/vaultic/internal/repository"
	"github.com/vaultic/vaultic/internal/ui"
	"github.com/vaultic/vaultic/internal/ui/progress"
	"github.com/vaultic/vaultic/internal/vaultic"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newInitCommand(globalOptions *global.Options) *cobra.Command {
	var opts InitOptions

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a new repository",
		Long: `
The "init" command initializes a new repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInit(cmd.Context(), opts, *globalOptions, args, globalOptions.Term)
		},
	}
	opts.AddFlags(cmd.Flags())
	return cmd
}

// InitOptions bundles all options for the init command.
type InitOptions struct {
	global.SecondaryRepoOptions
	CopyChunkerParameters bool
	RepositoryVersion     string

	// in-repo config applied right after init (subset of `config --set-*`)
	SetCompression  string
	SetAppendOnly   string
	SetExtraVerify  string
	SetChunker      string
	SetChunkSize    string
	SetChunkMinSize string
	SetChunkMaxSize string
	SetTreePackSize string
	SetDataPackSize string

	// HotOnly initializes only the hot part of a hot/cold repository
	// (requires --repo-hot): the cold repository must already exist; only
	// metadata is copied to the hot part.
	HotOnly bool
}

func (opts *InitOptions) AddFlags(f *pflag.FlagSet) {
	opts.SecondaryRepoOptions.AddFlags(f, "secondary", "to copy chunker parameters from")
	f.BoolVar(&opts.CopyChunkerParameters, "copy-chunker-params", false, "copy chunker parameters from the secondary repository (useful with the copy command)")
	f.StringVar(&opts.RepositoryVersion, "repository-version", "stable", "repository format version to use, allowed values are a format version, 'latest' and 'stable'")

	f.StringVar(&opts.SetCompression, "set-compression", "", "set initial compression `level` (-7..22, 0=off) in the repository config")
	f.StringVar(&opts.SetAppendOnly, "set-append-only", "", "set initial append-only `mode` (true|false)")
	f.StringVar(&opts.SetExtraVerify, "set-extra-verify", "", "verify data before upload (true|false; default true)")
	f.StringVar(&opts.SetChunker, "set-chunker", "", "set chunker `type` (rabin|fixed_size)")
	f.StringVar(&opts.SetChunkSize, "set-chunk-size", "", "set average/fixed chunk `size` in bytes")
	f.StringVar(&opts.SetChunkMinSize, "set-chunk-min-size", "", "set minimum chunk `size` in bytes")
	f.StringVar(&opts.SetChunkMaxSize, "set-chunk-max-size", "", "set maximum chunk `size` in bytes")
	f.StringVar(&opts.SetTreePackSize, "set-treepack-size", "", "set target tree pack `size` in bytes")
	f.StringVar(&opts.SetDataPackSize, "set-datapack-size", "", "set target data pack `size` in bytes")
	f.BoolVar(&opts.HotOnly, "hot-only", false, "only initialize the hot part of a hot/cold repository (requires --repo-hot; the cold repository must exist)")
}

func runInit(ctx context.Context, opts InitOptions, gopts global.Options, args []string, term ui.Terminal) error {
	if len(args) > 0 {
		return errors.Fatal("the init command expects no arguments, only options - please see `vaultic help init` for usage and flags")
	}

	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)

	if opts.HotOnly {
		return runInitHotOnly(ctx, opts, gopts, printer)
	}

	var version uint
	switch opts.RepositoryVersion {
	case "latest", "":
		version = vaultic.MaxRepoVersion
	case "stable":
		version = vaultic.StableRepoVersion
	default:
		v, err := strconv.ParseUint(opts.RepositoryVersion, 10, 32)
		if err != nil {
			return errors.Fatal("invalid repository version")
		}
		version = uint(v)
	}

	chunkerPolynomial, err := maybeReadChunkerPolynomial(ctx, opts, gopts, printer)
	if err != nil {
		return err
	}

	s, err := global.CreateRepository(ctx, gopts, version, chunkerPolynomial, printer)
	if err != nil {
		return errors.Fatalf("%s", err)
	}

	// apply initial in-repo config (from --set-* flags)
	if err := opts.applyConfig(ctx, s); err != nil {
		return err
	}

	if !gopts.JSON {
		printer.P("created vaultic repository %v at %s", s.Config().ID[:10], location.StripPassword(gopts.Backends, gopts.Repo))
		if opts.CopyChunkerParameters && chunkerPolynomial != nil {
			printer.P(" with chunker parameters copied from secondary repository")
		}
		printer.P("")
		printer.P("Please note that knowledge of your password is required to access")
		printer.P("the repository. Losing your password means that your data is")
		printer.P("irrecoverably lost.")

	} else {
		status := initSuccess{
			MessageType: "initialized",
			ID:          s.Config().ID,
			Repository:  location.StripPassword(gopts.Backends, gopts.Repo),
		}
		return json.NewEncoder(gopts.Term.OutputWriter()).Encode(status)
	}

	return nil
}

// runInitHotOnly initializes only the hot part of a hot/cold repository. The
// cold repository (gopts.Repo) must already exist; the hot part
// (gopts.RepoHot) is created and receives the metadata (config marked is_hot,
// keys, snapshots, indexes).
func runInitHotOnly(ctx context.Context, opts InitOptions, gopts global.Options, printer vaultic.Printer) error {
	if gopts.RepoHot == "" {
		return errors.Fatal("--hot-only requires --repo-hot to be set")
	}

	// open the existing cold repository to read its config (chunker params)
	coldGopts := gopts
	coldGopts.RepoHot = "" // open the cold repo as a normal repository
	cold, err := global.OpenRepository(ctx, coldGopts, printer)
	if err != nil {
		return errors.Fatalf("unable to open cold repository (must exist for --hot-only): %v", err)
	}

	// the hot part shares the cold repository's identity, chunker parameters and
	// master key; only metadata is mirrored. Mark it as is_hot.
	hotCfg := cold.Config()
	hotCfg.IsHot = true
	masterKey := cold.Key()
	defer func() { _ = cold.Close() }()

	// create the hot repository at RepoHot with the cold repo's config and key
	hotGopts := gopts
	hotGopts.Repo = gopts.RepoHot
	hotGopts.RepoHot = ""
	hot, err := global.CreateRepositoryWithConfig(ctx, hotGopts, hotCfg, masterKey, printer)
	if err != nil {
		return err
	}

	// mirror existing metadata (keys, snapshots, indexes) into the hot part
	if err := repository.CopyMetadata(ctx, cold, hot); err != nil {
		return err
	}

	printer.P("initialized hot repository %v at %s (cold repository: %s)",
		hot.Config().ID[:10], location.StripPassword(gopts.Backends, gopts.RepoHot),
		location.StripPassword(gopts.Backends, gopts.Repo))
	return nil
}

// applyConfig applies the --set-* init flags to the new repository config.
func (opts *InitOptions) applyConfig(ctx context.Context, s *repository.Repository) error {
	return s.UpdateConfig(ctx, func(cfg *vaultic.Config) error {
		if opts.SetCompression != "" {
			if err := setOptionalInt(opts.SetCompression, -7, 22, &cfg.Compression, "compression"); err != nil {
				return err
			}
		}
		if opts.SetAppendOnly != "" {
			if _, err := parseOptionalBool(opts.SetAppendOnly, &cfg.AppendOnlyFlag); err != nil {
				return err
			}
		}
		if opts.SetExtraVerify != "" {
			if err := setOptionalBoolPtr(opts.SetExtraVerify, &cfg.ExtraVerify); err != nil {
				return err
			}
		}
		if opts.SetChunker != "" {
			switch strings.ToLower(opts.SetChunker) {
			case "rabin":
				cfg.ChunkerType = vaultic.ChunkerRabin
			case "fixed_size", "fixedsize":
				cfg.ChunkerType = vaultic.ChunkerFixedSize
			default:
				return errors.Fatalf("invalid chunker %q, must be one of (rabin|fixed_size)", opts.SetChunker)
			}
		}
		if opts.SetChunkSize != "" {
			if err := setOptionalUint64(opts.SetChunkSize, &cfg.ChunkSizeBytes); err != nil {
				return err
			}
		}
		if opts.SetChunkMinSize != "" {
			if err := setOptionalUint64(opts.SetChunkMinSize, &cfg.ChunkMinSizeBytes); err != nil {
				return err
			}
		}
		if opts.SetChunkMaxSize != "" {
			if err := setOptionalUint64(opts.SetChunkMaxSize, &cfg.ChunkMaxSizeBytes); err != nil {
				return err
			}
		}
		if opts.SetTreePackSize != "" {
			if err := setOptionalUint64(opts.SetTreePackSize, &cfg.TreePackSizeBytes); err != nil {
				return err
			}
		}
		if opts.SetDataPackSize != "" {
			if err := setOptionalUint64(opts.SetDataPackSize, &cfg.DataPackSizeBytes); err != nil {
				return err
			}
		}
		return nil
	})
}

func maybeReadChunkerPolynomial(ctx context.Context, opts InitOptions, gopts global.Options, printer vaultic.Printer) (*chunker.Pol, error) {
	if opts.CopyChunkerParameters {
		otherGopts, _, err := opts.SecondaryRepoOptions.FillGlobalOpts(ctx, gopts, "secondary")
		if err != nil {
			return nil, err
		}

		otherRepo, err := global.OpenRepository(ctx, otherGopts, printer)
		if err != nil {
			return nil, err
		}

		pol := otherRepo.Config().ChunkerPolynomial
		return &pol, nil
	}

	if opts.Repo != "" || opts.RepositoryFile != "" || opts.LegacyRepo != "" || opts.LegacyRepositoryFile != "" {
		return nil, errors.Fatal("Secondary repository must only be specified when copying the chunker parameters")
	}
	return nil, nil
}

type initSuccess struct {
	MessageType string `json:"message_type"` // "initialized"
	ID          string `json:"id"`
	Repository  string `json:"repository"`
}
