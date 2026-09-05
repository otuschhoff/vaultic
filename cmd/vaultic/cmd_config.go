package main

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newConfigCommand(globalOptions *global.Options) *cobra.Command {
	var options configOptions

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
			return runConfig(
				cmd.Context(),
				cmd.Flags(),
				options,
				*globalOptions,
				args,
				progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term),
			)
		},
	}
	options.AddFlags(cmd.Flags())
	return cmd
}

// configOptions bundles all options for the config command.
type configOptions struct {
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

func (options *configOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&options.setVersion, "set-version", "", "set the repository `version` (1 or 2; upgrade via 'migrate' instead when possible)")
	f.StringVar(&options.setCompression, "set-compression", "", "set compression `level` (-7..22, 0=off) or 'unset'")
	f.StringVar(&options.setAppendOnly, "set-append-only", "", "set append-only `mode` (true|false|unset)")
	f.StringVar(&options.setExtraVerify, "set-extra-verify", "", "verify data before upload (true|false|unset; default true)")
	f.StringVar(&options.setChunker, "set-chunker", "", "set chunker `type` (rabin|fixed_size|unset)")
	f.StringVar(&options.setChunkSize, "set-chunk-size", "", "set average/fixed chunk `size` in bytes (or 'unset')")
	f.StringVar(&options.setChunkMinSize, "set-chunk-min-size", "", "set minimum chunk `size` in bytes (or 'unset')")
	f.StringVar(&options.setChunkMaxSize, "set-chunk-max-size", "", "set maximum chunk `size` in bytes (or 'unset')")
	f.StringVar(&options.setTreePackSize, "set-treepack-size", "", "set target tree pack `size` in bytes (or 'unset')")
	f.StringVar(&options.setTreePackGrowfactor, "set-treepack-growfactor", "", "set tree pack grow `factor` (or 'unset')")
	f.StringVar(&options.setTreePackSizeLimit, "set-treepack-size-limit", "", "set tree pack size `limit` in bytes (or 'unset')")
	f.StringVar(&options.setDataPackSize, "set-datapack-size", "", "set target data pack `size` in bytes (or 'unset')")
	f.StringVar(&options.setDataPackGrowfactor, "set-datapack-growfactor", "", "set data pack grow `factor` (or 'unset')")
	f.StringVar(&options.setDataPackSizeLimit, "set-datapack-size-limit", "", "set data pack size `limit` in bytes (or 'unset')")
	f.StringVar(
		&options.setMinPacksizeToleratePercent,
		"set-min-packsize-tolerate-percent",
		"",
		"tolerated minimum pack size in `percent` of target (or 'unset')",
	)
	f.StringVar(
		&options.setMaxPacksizeToleratePercent,
		"set-max-packsize-tolerate-percent",
		"",
		"tolerated maximum pack size in `percent` of target, 0=unlimited (or 'unset')",
	)
}

func runConfig(ctx context.Context, flags *pflag.FlagSet, options configOptions, globalOptions global.Options, args []string, printer vaultic.Printer) error {
	if len(args) > 0 {
		return errors.Fatal("the config command expects no arguments, only options - please see `vaultic help config` for usage and flags")
	}

	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	changed := false
	err = repo.UpdateConfig(ctx, func(cfg *vaultic.Config) error {
		var applyErr error
		changed, applyErr = options.apply(flags, cfg)
		return applyErr
	})
	if err != nil {
		return err
	}

	if changed && !globalOptions.JSON {
		printer.S("saved new config")
	}

	return printConfig(repo.Config(), globalOptions, printer)
}

// apply applies the --set-* options to cfg. It reports whether anything changed.
func (options *configOptions) apply(flags *pflag.FlagSet, cfg *vaultic.Config) (bool, error) {
	changed := false
	set := func(name string) bool { return flags.Changed(name) }

	if set("set-version") {
		v, err := strconv.ParseUint(options.setVersion, 10, 32)
		if err != nil || v < uint64(vaultic.MinRepoVersion) || v > uint64(vaultic.MaxRepoVersion) {
			return false, errors.Fatalf("invalid repository version %q", options.setVersion)
		}
		cfg.Version = uint(v)
		changed = true
	}
	if set("set-compression") {
		if err := setOptionalInt(options.setCompression, -7, 22, &cfg.Compression, "compression"); err != nil {
			return false, err
		}
		changed = true
	}
	if set("set-append-only") {
		if _, err := parseOptionalBool(options.setAppendOnly, &cfg.AppendOnlyFlag); err != nil {
			return false, err
		}
		changed = true
	}
	if set("set-extra-verify") {
		if err := setOptionalBoolPtr(options.setExtraVerify, &cfg.ExtraVerify); err != nil {
			return false, err
		}
		changed = true
	}

	chunkerChanged, err := options.applyChunkerConfig(flags, cfg)
	if err != nil {
		return false, err
	}
	changed = changed || chunkerChanged

	if err := applyPackConfig(flags, "set-treepack-", options.setTreePackSize, options.setTreePackGrowfactor, options.setTreePackSizeLimit,
		&cfg.TreePackSizeBytes, &cfg.TreePackGrowFactor, &cfg.TreePackSizeLimitBytes); err != nil {
		return false, err
	}
	if flags.Changed("set-treepack-size") || flags.Changed("set-treepack-growfactor") || flags.Changed("set-treepack-size-limit") {
		changed = true
	}
	if err := applyPackConfig(flags, "set-datapack-", options.setDataPackSize, options.setDataPackGrowfactor, options.setDataPackSizeLimit,
		&cfg.DataPackSizeBytes, &cfg.DataPackGrowFactor, &cfg.DataPackSizeLimitBytes); err != nil {
		return false, err
	}
	if flags.Changed("set-datapack-size") || flags.Changed("set-datapack-growfactor") || flags.Changed("set-datapack-size-limit") {
		changed = true
	}

	if set("set-min-packsize-tolerate-percent") {
		if err := setOptionalUint32(options.setMinPacksizeToleratePercent, 100, &cfg.MinPacksizeToleratePercent, "min_packsize_tolerate_percent"); err != nil {
			return false, err
		}
		changed = true
	}
	if set("set-max-packsize-tolerate-percent") {
		if err := setOptionalUint32(options.setMaxPacksizeToleratePercent, 100, &cfg.MaxPacksizeToleratePercent, "max_packsize_tolerate_percent"); err != nil {
			return false, err
		}
		changed = true
	}

	return changed, nil
}

func (options *configOptions) applyChunkerConfig(flags *pflag.FlagSet, cfg *vaultic.Config) (bool, error) {
	changed := false
	if flags.Changed("set-chunker") {
		switch strings.ToLower(options.setChunker) {
		case "unset", "":
			cfg.ChunkerType = ""
		case "rabin":
			cfg.ChunkerType = vaultic.ChunkerRabin
		case "fixed_size", "fixedsize":
			cfg.ChunkerType = vaultic.ChunkerFixedSize
		default:
			return false, errors.Fatalf("invalid chunker %q, must be one of (rabin|fixed_size|unset)", options.setChunker)
		}
		changed = true
	}
	values := []struct {
		flag  string
		value string
		dest  *uint64
	}{
		{"set-chunk-size", options.setChunkSize, &cfg.ChunkSizeBytes},
		{"set-chunk-min-size", options.setChunkMinSize, &cfg.ChunkMinSizeBytes},
		{"set-chunk-max-size", options.setChunkMaxSize, &cfg.ChunkMaxSizeBytes},
	}
	for _, value := range values {
		if !flags.Changed(value.flag) {
			continue
		}
		if err := setOptionalUint64(value.value, value.dest); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}

// applyPackConfig applies the three --set-{tree,data}pack-* options to the flat fields.
func applyPackConfig(flags *pflag.FlagSet, prefix, size, growfactor, sizeLimit string, sizeDst *uint64, gfDst **uint32, limitDst *uint64) error {
	if flags.Changed(prefix + "size") {
		if err := setOptionalUint64(size, sizeDst); err != nil {
			return err
		}
	}
	if flags.Changed(prefix + "growfactor") {
		if err := setOptionalUint32(growfactor, 1<<31-1, gfDst, prefix+"growfactor"); err != nil {
			return err
		}
	}
	if flags.Changed(prefix + "size-limit") {
		if err := setOptionalUint64(sizeLimit, limitDst); err != nil {
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
func printConfig(cfg vaultic.Config, globalOptions global.Options, printer vaultic.Printer) error {
	if globalOptions.JSON {
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
	if cfg.ChunkerType != "" {
		printer.S("chunker: %s", cfg.ChunkerType)
	}
	if cfg.ChunkSizeBytes != 0 {
		printer.S("chunk_size: %d", cfg.ChunkSizeBytes)
	}
	if cfg.ChunkMinSizeBytes != 0 {
		printer.S("chunk_min_size: %d", cfg.ChunkMinSizeBytes)
	}
	if cfg.ChunkMaxSizeBytes != 0 {
		printer.S("chunk_max_size: %d", cfg.ChunkMaxSizeBytes)
	}
	printPack := func(name string, size uint64, gf *uint32, limit uint64) {
		if size != 0 {
			printer.S("%spack_size: %d", name, size)
		}
		if gf != nil {
			printer.S("%spack_growfactor: %d", name, *gf)
		}
		if limit != 0 {
			printer.S("%spack_size_limit: %d", name, limit)
		}
	}
	printPack("tree", cfg.TreePackSizeBytes, cfg.TreePackGrowFactor, cfg.TreePackSizeLimitBytes)
	printPack("data", cfg.DataPackSizeBytes, cfg.DataPackGrowFactor, cfg.DataPackSizeLimitBytes)
	if cfg.MinPacksizeToleratePercent != nil {
		printer.S("min_packsize_tolerate_percent: %d", *cfg.MinPacksizeToleratePercent)
	}
	if cfg.MaxPacksizeToleratePercent != nil {
		printer.S("max_packsize_tolerate_percent: %d", *cfg.MaxPacksizeToleratePercent)
	}
	return nil
}
