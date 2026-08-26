package main

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vaultic/vaultic/internal/errors"
	"github.com/vaultic/vaultic/internal/global"
	"github.com/vaultic/vaultic/internal/ui/progress"
	"github.com/vaultic/vaultic/internal/vaultic"
)

func newConfigCommand(globalOptions *global.Options) *cobra.Command {
	var opts ConfigOptions

	cmd := &cobra.Command{
		Use:   "config [flags]",
		Short: "Read or modify the repository configuration",
		Long: `
The "config" command reads the repository configuration and optionally modifies
it using the --set-* options. Without any --set-* option it prints the current
configuration.

The configuration is stored inside the repository and applies to every client
that does not explicitly override it with a command line flag or environment
variable. Precedence is: CLI flag > environment > repository config > default.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			term := globalOptions.Term
			return runConfig(cmd.Context(), cmd.Flags(), opts, *globalOptions, args, progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term))
		},
	}
	opts.AddFlags(cmd.Flags())
	return cmd
}

// ConfigOptions bundles all options for the config command.
type ConfigOptions struct {
	setVersion      string
	setCompression  string
	setAppendOnly   string
	setExtraVerify  string
	setChunker      string
	setChunkSize    string
	setChunkMinSize string
	setChunkMaxSize string

	setTreePackSize       string
	setTreePackGrowfactor string
	setTreePackSizeLimit  string
	setDataPackSize       string
	setDataPackGrowfactor string
	setDataPackSizeLimit  string

	setMinPacksizeToleratePercent string
	setMaxPacksizeToleratePercent string
}

func (opts *ConfigOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&opts.setVersion, "set-version", "", "set the repository `version` (1 or 2; upgrade via 'migrate' instead when possible)")
	f.StringVar(&opts.setCompression, "set-compression", "", "set compression `level` (-7..22, 0=off) or 'unset'")
	f.StringVar(&opts.setAppendOnly, "set-append-only", "", "set append-only `mode` (true|false|unset)")
	f.StringVar(&opts.setExtraVerify, "set-extra-verify", "", "verify data before upload (true|false|unset; default true)")
	f.StringVar(&opts.setChunker, "set-chunker", "", "set chunker `type` (rabin|fixed_size|unset)")
	f.StringVar(&opts.setChunkSize, "set-chunk-size", "", "set average/fixed chunk `size` in bytes (or 'unset')")
	f.StringVar(&opts.setChunkMinSize, "set-chunk-min-size", "", "set minimum chunk `size` in bytes (or 'unset')")
	f.StringVar(&opts.setChunkMaxSize, "set-chunk-max-size", "", "set maximum chunk `size` in bytes (or 'unset')")
	f.StringVar(&opts.setTreePackSize, "set-treepack-size", "", "set target tree pack `size` in bytes (or 'unset')")
	f.StringVar(&opts.setTreePackGrowfactor, "set-treepack-growfactor", "", "set tree pack grow `factor` (or 'unset')")
	f.StringVar(&opts.setTreePackSizeLimit, "set-treepack-size-limit", "", "set tree pack size `limit` in bytes (or 'unset')")
	f.StringVar(&opts.setDataPackSize, "set-datapack-size", "", "set target data pack `size` in bytes (or 'unset')")
	f.StringVar(&opts.setDataPackGrowfactor, "set-datapack-growfactor", "", "set data pack grow `factor` (or 'unset')")
	f.StringVar(&opts.setDataPackSizeLimit, "set-datapack-size-limit", "", "set data pack size `limit` in bytes (or 'unset')")
	f.StringVar(&opts.setMinPacksizeToleratePercent, "set-min-packsize-tolerate-percent", "", "tolerated minimum pack size in `percent` of target (or 'unset')")
	f.StringVar(&opts.setMaxPacksizeToleratePercent, "set-max-packsize-tolerate-percent", "", "tolerated maximum pack size in `percent` of target, 0=unlimited (or 'unset')")
}

func runConfig(ctx context.Context, flags *pflag.FlagSet, opts ConfigOptions, gopts global.Options, args []string, printer vaultic.Printer) error {
	if len(args) > 0 {
		return errors.Fatal("the config command expects no arguments, only options - please see `vaultic help config` for usage and flags")
	}

	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	changed := false
	err = repo.UpdateConfig(ctx, func(cfg *vaultic.Config) error {
		var applyErr error
		changed, applyErr = opts.apply(flags, cfg)
		return applyErr
	})
	if err != nil {
		return err
	}

	if changed && !gopts.JSON {
		printer.S("saved new config")
	}

	return printConfig(repo.Config(), gopts, printer)
}

// apply applies the --set-* options to cfg. It reports whether anything changed.
func (opts *ConfigOptions) apply(flags *pflag.FlagSet, cfg *vaultic.Config) (bool, error) {
	changed := false
	set := func(name string) bool { return flags.Changed(name) }

	if set("set-version") {
		v, err := strconv.ParseUint(opts.setVersion, 10, 32)
		if err != nil || v < uint64(vaultic.MinRepoVersion) || v > uint64(vaultic.MaxRepoVersion) {
			return false, errors.Fatalf("invalid repository version %q", opts.setVersion)
		}
		cfg.Version = uint(v)
		changed = true
	}
	if set("set-compression") {
		if err := setOptionalInt(opts.setCompression, -7, 22, &cfg.Compression, "compression"); err != nil {
			return false, err
		}
		changed = true
	}
	if set("set-append-only") {
		if _, err := parseOptionalBool(opts.setAppendOnly, &cfg.AppendOnlyFlag); err != nil {
			return false, err
		}
		changed = true
	}
	if set("set-extra-verify") {
		if err := setOptionalBoolPtr(opts.setExtraVerify, &cfg.ExtraVerify); err != nil {
			return false, err
		}
		changed = true
	}

	chunkerChanged := false
	if set("set-chunker") {
		switch strings.ToLower(opts.setChunker) {
		case "unset":
			cfg.ChunkerCfg = nil
		default:
			ensureChunker(cfg)
			cfg.ChunkerCfg.Type = vaultic.ChunkerType(strings.ToLower(opts.setChunker))
		}
		chunkerChanged = true
	}
	if set("set-chunk-size") {
		ensureChunker(cfg)
		if err := setOptionalUint64(opts.setChunkSize, &cfg.ChunkerCfg.ChunkSize); err != nil {
			return false, err
		}
		chunkerChanged = true
	}
	if set("set-chunk-min-size") {
		ensureChunker(cfg)
		if err := setOptionalUint64(opts.setChunkMinSize, &cfg.ChunkerCfg.ChunkMinSize); err != nil {
			return false, err
		}
		chunkerChanged = true
	}
	if set("set-chunk-max-size") {
		ensureChunker(cfg)
		if err := setOptionalUint64(opts.setChunkMaxSize, &cfg.ChunkerCfg.ChunkMaxSize); err != nil {
			return false, err
		}
		chunkerChanged = true
	}
	// drop an empty chunker block entirely
	if chunkerChanged && cfg.ChunkerCfg != nil && *cfg.ChunkerCfg == (vaultic.ChunkerConfig{}) {
		cfg.ChunkerCfg = nil
	}
	changed = changed || chunkerChanged

	if err := applyPackConfig(flags, "set-treepack-", opts.setTreePackSize, opts.setTreePackGrowfactor, opts.setTreePackSizeLimit, &cfg.TreePack); err != nil {
		return false, err
	}
	if flags.Changed("set-treepack-size") || flags.Changed("set-treepack-growfactor") || flags.Changed("set-treepack-size-limit") {
		changed = true
	}
	if err := applyPackConfig(flags, "set-datapack-", opts.setDataPackSize, opts.setDataPackGrowfactor, opts.setDataPackSizeLimit, &cfg.DataPack); err != nil {
		return false, err
	}
	if flags.Changed("set-datapack-size") || flags.Changed("set-datapack-growfactor") || flags.Changed("set-datapack-size-limit") {
		changed = true
	}

	if set("set-min-packsize-tolerate-percent") {
		if err := setOptionalUint32(opts.setMinPacksizeToleratePercent, 100, &cfg.MinPacksizeToleratePercent, "min_packsize_tolerate_percent"); err != nil {
			return false, err
		}
		changed = true
	}
	if set("set-max-packsize-tolerate-percent") {
		if err := setOptionalUint32(opts.setMaxPacksizeToleratePercent, 100, &cfg.MaxPacksizeToleratePercent, "max_packsize_tolerate_percent"); err != nil {
			return false, err
		}
		changed = true
	}

	return changed, nil
}

func ensureChunker(cfg *vaultic.Config) {
	if cfg.ChunkerCfg == nil {
		cfg.ChunkerCfg = &vaultic.ChunkerConfig{}
	}
}

// applyPackConfig applies the three --set-{tree,data}pack-* options.
func applyPackConfig(flags *pflag.FlagSet, prefix, size, growfactor, sizeLimit string, p *vaultic.PackConfig) error {
	if flags.Changed(prefix + "size") {
		if err := setOptionalUint64(size, &p.Size); err != nil {
			return err
		}
	}
	if flags.Changed(prefix + "growfactor") {
		if isUnset(growfactor) {
			p.GrowFactor = 0
		} else {
			v, err := strconv.ParseUint(growfactor, 10, 32)
			if err != nil {
				return errors.Fatalf("invalid %s %q", prefix+"growfactor", growfactor)
			}
			p.GrowFactor = uint32(v)
		}
	}
	if flags.Changed(prefix + "size-limit") {
		if err := setOptionalUint64(sizeLimit, &p.SizeLimit); err != nil {
			return err
		}
	}
	return nil
}

// --- small typed helpers for the --set-* string options -------------------

func isUnset(s string) bool { return strings.EqualFold(s, "unset") }

func setOptionalInt(s string, min, max int, dst **int, name string) error {
	if isUnset(s) {
		*dst = nil
		return nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < min || v > max {
		return errors.Fatalf("invalid %s %q, must be %d..%d or 'unset'", name, s, min, max)
	}
	*dst = &v
	return nil
}

func setOptionalUint32(s string, max uint32, dst **uint32, name string) error {
	if isUnset(s) {
		*dst = nil
		return nil
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil || uint32(v) > max {
		return errors.Fatalf("invalid %s %q, must be 0..%d or 'unset'", name, s, max)
	}
	vv := uint32(v)
	*dst = &vv
	return nil
}

func setOptionalUint64(s string, dst *uint64) error {
	if isUnset(s) {
		*dst = 0
		return nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return errors.Fatalf("invalid size %q, expected bytes or 'unset'", s)
	}
	*dst = v
	return nil
}

func setOptionalBoolPtr(s string, dst **bool) error {
	if isUnset(s) {
		*dst = nil
		return nil
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		return errors.Fatalf("invalid boolean %q, use true|false|unset", s)
	}
	*dst = &v
	return nil
}

func parseOptionalBool(s string, dst *bool) (bool, error) {
	if isUnset(s) {
		*dst = false
		return true, nil
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		return false, errors.Fatalf("invalid boolean %q, use true|false|unset", s)
	}
	*dst = v
	return true, nil
}

// printConfig prints the config as JSON (for --json) or as key: value lines.
func printConfig(cfg vaultic.Config, gopts global.Options, printer vaultic.Printer) error {
	if gopts.JSON {
		data, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return err
		}
		printer.S("%s", data)
		return nil
	}

	printer.S("version: %d", cfg.Version)
	printer.S("id: %s", cfg.ID)
	printer.S("chunker_polynomial: %x", uint64(cfg.ChunkerPolynomial))
	if cfg.Compression != nil {
		printer.S("compression: %d", *cfg.Compression)
	}
	printer.S("append_only: %v", cfg.AppendOnly())
	printer.S("extra_verify: %v", cfg.ExtraVerifyEnabled())
	if cfg.ChunkerCfg != nil {
		printer.S("chunker: %s", cfg.ChunkerCfg.Type)
		if cfg.ChunkerCfg.ChunkSize != 0 {
			printer.S("chunk_size: %d", cfg.ChunkerCfg.ChunkSize)
		}
		if cfg.ChunkerCfg.ChunkMinSize != 0 {
			printer.S("chunk_min_size: %d", cfg.ChunkerCfg.ChunkMinSize)
		}
		if cfg.ChunkerCfg.ChunkMaxSize != 0 {
			printer.S("chunk_max_size: %d", cfg.ChunkerCfg.ChunkMaxSize)
		}
	}
	printPack := func(name string, p vaultic.PackConfig) {
		if p.Size != 0 {
			printer.S("%spack_size: %d", name, p.Size)
		}
		if p.GrowFactor != 0 {
			printer.S("%spack_growfactor: %d", name, p.GrowFactor)
		}
		if p.SizeLimit != 0 {
			printer.S("%spack_size_limit: %d", name, p.SizeLimit)
		}
	}
	printPack("tree", cfg.TreePack)
	printPack("data", cfg.DataPack)
	if cfg.MinPacksizeToleratePercent != nil {
		printer.S("min_packsize_tolerate_percent: %d", *cfg.MinPacksizeToleratePercent)
	}
	if cfg.MaxPacksizeToleratePercent != nil {
		printer.S("max_packsize_tolerate_percent: %d", *cfg.MaxPacksizeToleratePercent)
	}
	return nil
}
