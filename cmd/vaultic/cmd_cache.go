package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend/cache"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/ui/table"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newCacheCommand(globalOptions *global.Options) *cobra.Command {
	var options cacheOptions

	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Operate on local cache directories",
		Long: `
The "cache" command allows listing and cleaning local cache directories.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(_ *cobra.Command, args []string) error {
			return runCache(options, *globalOptions, args, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	cmd.AddCommand(newCacheStatusCommand(globalOptions))
	cmd.AddCommand(newCacheUpdateCommand(globalOptions))
	cmd.AddCommand(newCacheDrainCommand(globalOptions))
	cmd.AddCommand(newCacheClearCommand(globalOptions))
	return cmd
}

type cacheOperationOptions struct {
	JSON bool
}

func addCacheOperationFlags(flags *pflag.FlagSet, options *cacheOperationOptions) {
	flags.BoolVar(&options.JSON, "json", true, "emit JSON output")
}

func newCacheStatusCommand(globalOptions *global.Options) *cobra.Command {
	var options cacheOperationOptions
	command := &cobra.Command{
		Use:               "status",
		Short:             "Show read-cache status and metrics",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
			_, repo, unlock, err := openWithReadLock(command.Context(), *globalOptions, globalOptions.NoLock, printer)
			if err != nil {
				return err
			}
			defer unlock()
			status := repo.ReadCacheStatus()
			if options.JSON {
				globalOptions.Term.Print(ui.ToJSONString(status))
				return nil
			}
			globalOptions.Term.Print(fmt.Sprintf(
				"read-cache enabled=%v revision=%d admissions=%v used=%d reserved=%d",
				status.Enabled, status.PolicyRevision, status.AdmissionsEnabled, status.UsedBytes, status.ReservedBytes,
			))
			return nil
		},
	}
	addCacheOperationFlags(command.Flags(), &options)
	return command
}

type cacheUpdateOptions struct {
	cacheOperationOptions
	Tier              string
	Enabled           string
	MaxBytes          uint64
	ChunkBytes        uint64
	Trust             string
	TrustAck          string
	Codec             string
	IdleAge           time.Duration
	AbsoluteAge       time.Duration
	ReadPriority      uint32
	AdmissionPriority uint32
	ExpectedRevision  uint64
}

//nolint:gocognit // Flag-to-policy mapping remains explicit for validation and changed-value semantics.
func newCacheUpdateCommand(globalOptions *global.Options) *cobra.Command {
	var options cacheUpdateOptions
	command := &cobra.Command{
		Use:               "update",
		Short:             "Update read-cache policy for a tier",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			if options.Tier == "" {
				return errors.Fatal("--tier is required")
			}
			if err := validateCacheUpdateOptions(command.Flags(), options); err != nil {
				return err
			}
			printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
			_, repo, unlock, err := openWithCacheExclusiveLock(command.Context(), *globalOptions, printer)
			if err != nil {
				return err
			}
			defer unlock()
			update := repository.ReadCachePolicyUpdate{ID: options.Tier}
			if command.Flags().Changed("enabled") {
				value, parseErr := strconv.ParseBool(options.Enabled)
				if parseErr != nil {
					return errors.Fatalf("invalid --enabled value: %v", parseErr)
				}
				update.Enabled = &value
			}
			if command.Flags().Changed("max-bytes") {
				update.MaxBytes = &options.MaxBytes
			}
			if command.Flags().Changed("chunk-bytes") {
				update.ChunkBytes = &options.ChunkBytes
			}
			if command.Flags().Changed("trust") {
				trust := strings.TrimSpace(options.Trust)
				update.Trust = &trust
			}
			if command.Flags().Changed("trust-ack") {
				value, parseErr := strconv.ParseBool(options.TrustAck)
				if parseErr != nil {
					return errors.Fatalf("invalid --trust-ack value: %v", parseErr)
				}
				update.TrustAck = &value
			}
			if command.Flags().Changed("codec") {
				codec := strings.TrimSpace(options.Codec)
				update.Codec = &codec
			}
			if command.Flags().Changed("idle-age") {
				update.IdleAge = &options.IdleAge
			}
			if command.Flags().Changed("absolute-age") {
				update.AbsoluteAge = &options.AbsoluteAge
			}
			if command.Flags().Changed("read-priority") {
				update.ReadPriority = &options.ReadPriority
			}
			if command.Flags().Changed("admission-priority") {
				update.AdmissionPriority = &options.AdmissionPriority
			}
			if command.Flags().Changed("expected-revision") {
				update.ExpectedRev = &options.ExpectedRevision
			}
			if err := repo.UpdateReadCachePolicy(update); err != nil {
				return err
			}
			status := repo.ReadCacheStatus()
			if options.JSON {
				globalOptions.Term.Print(ui.ToJSONString(status))
				return nil
			}
			globalOptions.Term.Print(fmt.Sprintf("updated read-cache tier %s to revision %d", options.Tier, status.PolicyRevision))
			return nil
		},
	}
	addCacheOperationFlags(command.Flags(), &options.cacheOperationOptions)
	command.Flags().StringVar(&options.Tier, "tier", "", "read-cache tier ID")
	command.Flags().StringVar(&options.Enabled, "enabled", "", "set tier enabled state (true|false)")
	command.Flags().Uint64Var(&options.MaxBytes, "max-bytes", 0, "set tier byte budget")
	command.Flags().Uint64Var(&options.ChunkBytes, "chunk-bytes", 0, "set tier chunk size bytes")
	command.Flags().StringVar(&options.Trust, "trust", "", "set tier trust mode (encrypted-only|plaintext-allowed)")
	command.Flags().StringVar(&options.TrustAck, "trust-ack", "", "set plaintext trust acknowledgement (true|false)")
	command.Flags().StringVar(&options.Codec, "codec", "", "set representation codec label")
	command.Flags().DurationVar(&options.IdleAge, "idle-age", 0, "set maximum idle age (zero disables)")
	command.Flags().DurationVar(&options.AbsoluteAge, "absolute-age", 0, "set maximum absolute age (zero disables)")
	command.Flags().Uint32Var(&options.ReadPriority, "read-priority", 0, "set read traversal priority (lower is earlier)")
	command.Flags().Uint32Var(&options.AdmissionPriority, "admission-priority", 0, "set admission traversal priority (lower is earlier)")
	command.Flags().Uint64Var(&options.ExpectedRevision, "expected-revision", 0, "require policy CAS against this revision")
	return command
}

func validateCacheUpdateOptions(flags *pflag.FlagSet, options cacheUpdateOptions) error {
	const (
		cacheTrustEncrypted = "encrypted-only"
		cacheTrustPlaintext = "plaintext-allowed"
	)
	if flags.Changed("chunk-bytes") && options.ChunkBytes == 0 {
		return errors.Fatal("invalid --chunk-bytes value: must be greater than zero")
	}
	if flags.Changed("chunk-bytes") && options.ChunkBytes > uint64(math.MaxInt) {
		return errors.Fatalf("invalid --chunk-bytes value %d: exceeds this platform's integer limit", options.ChunkBytes)
	}
	if flags.Changed("chunk-bytes") && options.ChunkBytes > repository.MaxReadCacheChunkBytes {
		return errors.Fatalf(
			"invalid --chunk-bytes value %d: maximum is %d bytes (1 GiB)",
			options.ChunkBytes, repository.MaxReadCacheChunkBytes,
		)
	}
	if flags.Changed("idle-age") && options.IdleAge < 0 {
		return errors.Fatal("invalid --idle-age value: must not be negative")
	}
	if flags.Changed("absolute-age") && options.AbsoluteAge < 0 {
		return errors.Fatal("invalid --absolute-age value: must not be negative")
	}
	if flags.Changed("idle-age") && flags.Changed("absolute-age") && options.IdleAge > 0 && options.AbsoluteAge > 0 && options.AbsoluteAge < options.IdleAge {
		return errors.Fatal("invalid --absolute-age value: must not be shorter than --idle-age")
	}
	if flags.Changed("trust") {
		trust := strings.ToLower(strings.TrimSpace(options.Trust))
		switch trust {
		case cacheTrustEncrypted, cacheTrustPlaintext:
		default:
			return errors.Fatalf("invalid --trust value %q (use %q or %q)", options.Trust, cacheTrustEncrypted, cacheTrustPlaintext)
		}
	}
	if flags.Changed("trust") && strings.EqualFold(strings.TrimSpace(options.Trust), cacheTrustPlaintext) {
		ack := false
		if flags.Changed("trust-ack") {
			parsed, err := strconv.ParseBool(options.TrustAck)
			if err != nil {
				return errors.Fatalf("invalid --trust-ack value: %v", err)
			}
			ack = parsed
		}
		if !ack {
			return errors.Fatal("invalid --trust/--trust-ack: plaintext-allowed requires --trust-ack=true")
		}
	}
	return nil
}

func newCacheDrainCommand(globalOptions *global.Options) *cobra.Command {
	var options cacheOperationOptions
	command := &cobra.Command{
		Use:               "drain",
		Short:             "Drain all read-cache tiers",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
			ctx, repo, unlock, err := openWithCacheExclusiveLock(command.Context(), *globalOptions, printer)
			if err != nil {
				return err
			}
			defer unlock()
			if err := repo.DrainReadCache(ctx); err != nil {
				return err
			}
			status := repo.ReadCacheStatus()
			if options.JSON {
				globalOptions.Term.Print(ui.ToJSONString(status))
				return nil
			}
			globalOptions.Term.Print("drained read-cache tiers")
			return nil
		},
	}
	addCacheOperationFlags(command.Flags(), &options)
	return command
}

func newCacheClearCommand(globalOptions *global.Options) *cobra.Command {
	var options struct {
		cacheOperationOptions
		Tier string
	}
	command := &cobra.Command{
		Use:               "clear",
		Short:             "Clear read-cache entries (all tiers or one tier)",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
			ctx, repo, unlock, err := openWithCacheExclusiveLock(command.Context(), *globalOptions, printer)
			if err != nil {
				return err
			}
			defer unlock()
			if err := repo.ClearReadCache(ctx, options.Tier); err != nil {
				return err
			}
			status := repo.ReadCacheStatus()
			if options.JSON {
				globalOptions.Term.Print(ui.ToJSONString(status))
				return nil
			}
			if options.Tier == "" {
				globalOptions.Term.Print("cleared all read-cache tiers")
			} else {
				globalOptions.Term.Print(fmt.Sprintf("cleared read-cache tier %s", options.Tier))
			}
			return nil
		},
	}
	addCacheOperationFlags(command.Flags(), &options.cacheOperationOptions)
	command.Flags().StringVar(&options.Tier, "tier", "", "read-cache tier ID (empty clears all)")
	return command
}

// cacheOptions bundles all options for the snapshots command.
type cacheOptions struct {
	Cleanup bool
	MaxAge  uint
	NoSize  bool
}

func (options *cacheOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVar(&options.Cleanup, "cleanup", false, "remove old cache directories")
	f.UintVar(&options.MaxAge, "max-age", 30, "max age in `days` for cache directories to be considered old")
	f.BoolVar(&options.NoSize, "no-size", false, "do not output the size of the cache directories")
}

func runCache(options cacheOptions, globalOptions global.Options, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	if len(args) > 0 {
		return errors.Fatal("the cache command expects no arguments, only options - please see `vaultic help cache` for usage and flags")
	}

	if globalOptions.NoCache {
		return errors.Fatal("Refusing to do anything, the cache is disabled")
	}

	var (
		cachedir = globalOptions.CacheDir
		err      error
	)

	if cachedir == "" {
		cachedir, err = cache.DefaultDir()
		if err != nil {
			return err
		}
	}

	if options.Cleanup || globalOptions.CleanupCache {
		oldDirs, err := cache.OlderThan(cachedir, time.Duration(options.MaxAge)*24*time.Hour)
		if err != nil {
			return err
		}

		if len(oldDirs) == 0 {
			printer.P("no old cache dirs found")
			return nil
		}

		printer.P("remove %d old cache directories", len(oldDirs))

		for _, item := range oldDirs {
			dir := filepath.Join(cachedir, item.Name())
			err = os.RemoveAll(dir)
			if err != nil {
				printer.E("unable to remove %v: %v", dir, err)
			}
		}

		return nil
	}

	tab := table.New()

	type data struct {
		ID   string
		Last string
		Old  string
		Size string
	}

	tab.AddColumn("Repo ID", "{{ .ID }}")
	tab.AddColumn("Last Used", "{{ .Last }}")
	tab.AddColumn("Old", "{{ .Old }}")

	if !options.NoSize {
		tab.AddColumn("Size", "{{ .Size }}")
	}

	dirs, err := cache.All(cachedir)
	if err != nil {
		return err
	}

	if len(dirs) == 0 {
		printer.S("no cache dirs found, basedir is %v", cachedir)
		return nil
	}

	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].ModTime().Before(dirs[j].ModTime())
	})

	for _, entry := range dirs {
		var old string
		if cache.IsOld(entry.ModTime(), time.Duration(options.MaxAge)*24*time.Hour) {
			old = "yes"
		}

		var size string
		if !options.NoSize {
			bytes, err := dirSize(filepath.Join(cachedir, entry.Name()))
			if err != nil {
				return err
			}
			size = fmt.Sprintf("%11s", ui.FormatBytes(uint64(bytes)))
		}

		name := entry.Name()
		if !strings.HasPrefix(name, "vaultic-check-cache-") {
			name = name[:10]
		}

		tab.AddRow(data{
			name,
			fmt.Sprintf("%d days ago", uint(time.Since(entry.ModTime()).Hours()/24)),
			old,
			size,
		})
	}

	if err := tab.Write(globalOptions.Term.OutputWriter()); err != nil {
		return fmt.Errorf("write cache table: %w", err)
	}
	printer.S("%d cache dirs in %s", len(dirs), cachedir)

	return nil
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return err
		}

		if !info.IsDir() {
			size += info.Size()
		}

		return nil
	})
	return size, err
}
