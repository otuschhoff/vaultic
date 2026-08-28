package main

import (
	"context"
	"math"
	"runtime"
	"strconv"
	"strings"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newPruneCommand(globalOptions *global.Options) *cobra.Command {
	var opts PruneOptions

	cmd := &cobra.Command{
		Use:   "prune [flags]",
		Short: "Remove unneeded data from the repository",
		Long: `
The "prune" command checks the repository and removes data that is not
referenced and therefore not needed any more.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPrune(cmd.Context(), opts, *globalOptions, globalOptions.Term)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// PruneOptions collects all options for the cleanup command.
type PruneOptions struct {
	DryRun                bool
	UnsafeNoSpaceRecovery string

	unsafeRecovery bool

	MaxUnused      string
	maxUnusedBytes func(used uint64) (unused uint64) // calculates the number of unused bytes after repacking, according to MaxUnused

	MaxRepackSize  string
	MaxRepackBytes uint64
	// MaxRepack accepts a size (k/m/g/t), a percentage of the repository size
	// (e.g. "10%") or "unlimited"; superset of --max-repack-size.
	MaxRepack        string
	maxRepackPercent float64

	RepackCacheableOnly bool
	RepackUncompressed  bool
	RepackAll           bool
	// FastRepack trusts the index instead of re-reading pack contents.
	FastRepack bool
	// EarlyDeleteIndex removes old index files before deleting packs
	// (helps repositories running out of free space).
	EarlyDeleteIndex bool
	// KeepDelete (two-phase prune) runs only the repack+index phase and defers
	// deletion of superseded files to a later prune. InstantDelete is the
	// default (current single-phase behavior).
	KeepDelete    bool
	InstantDelete bool

	SmallPackSize  string
	SmallPackBytes uint64
}

func (opts *PruneOptions) AddFlags(f *pflag.FlagSet) {
	opts.AddLimitedFlags(f)
	f.BoolVarP(&opts.DryRun, "dry-run", "n", false, "do not modify the repository, just print what would be done")
	f.StringVarP(&opts.UnsafeNoSpaceRecovery, "unsafe-recover-no-free-space", "", "", "UNSAFE, READ THE DOCUMENTATION BEFORE USING! Try to recover a repository stuck with no free space. Do not use without trying out 'prune --max-repack-size 0' first.")
}

func (opts *PruneOptions) AddLimitedFlags(f *pflag.FlagSet) {
	var unused bool
	f.StringVar(&opts.MaxUnused, "max-unused", "5%", "tolerate given `limit` of unused data (absolute value in bytes with suffixes k/K, m/M, g/G, t/T, a value in % or the word 'unlimited')")
	f.StringVar(&opts.MaxRepackSize, "max-repack-size", "", "stop after repacking this much data in total (allowed suffixes for `size`: k/K, m/M, g/G, t/T)")
	f.StringVar(&opts.MaxRepack, "max-repack", "", "stop after repacking this much data: a `size` (k/m/g/t), a percentage of the repo size (e.g. '10%') or 'unlimited'")
	f.BoolVar(&opts.RepackCacheableOnly, "repack-cacheable-only", false, "only repack packs which are cacheable")
	f.BoolVar(&unused, "repack-small", false, "deprecated. Use --repack-smaller-than to specify a minimum size")
	f.BoolVar(&opts.RepackUncompressed, "repack-uncompressed", false, "repack all uncompressed data")
	f.BoolVar(&opts.RepackAll, "repack-all", false, "repack all packs (e.g. to change pack size or compression)")
	f.BoolVar(&opts.FastRepack, "fast-repack", false, "skip re-reading pack contents, trust the index (faster, needs an intact index)")
	f.BoolVar(&opts.EarlyDeleteIndex, "early-delete-index", false, "remove old index files before deleting packs (helps when the repository is out of free space)")
	f.BoolVar(&opts.KeepDelete, "keep-delete", false, "two-phase prune: only repack and write the new index now; defer deleting superseded packs/indexes to a later prune (requires the two-phase-prune feature)")
	f.BoolVar(&opts.InstantDelete, "instant-delete", true, "delete superseded packs/indexes in the same prune run (current behavior; the default)")
	f.StringVar(&opts.SmallPackSize, "repack-smaller-than", "", "pack `below-limit` packfiles (allowed suffixes: m/M)")

	err := f.MarkDeprecated("repack-small", "small files are automatically repacked. Use --repack-smaller-than to specify a minimum size")
	if err != nil {
		// MarkDeprecated only returns an error when the flag is not found
		panic(err)
	}
}

func verifyPruneOptions(opts *PruneOptions) error {
	opts.MaxRepackBytes = math.MaxUint64
	if len(opts.MaxRepackSize) > 0 {
		size, err := ui.ParseBytes(opts.MaxRepackSize)
		if err != nil {
			return err
		}
		opts.MaxRepackBytes = uint64(size)
	}
	// --max-repack supersedes --max-repack-size (size, %, or unlimited)
	if len(opts.MaxRepack) > 0 {
		bytes, pct, err := parseMaxRepack(opts.MaxRepack)
		if err != nil {
			return err
		}
		opts.MaxRepackBytes = bytes
		opts.maxRepackPercent = pct
	}
	if opts.UnsafeNoSpaceRecovery != "" {
		// prevent repacking data to make sure users cannot get stuck.
		opts.MaxRepackBytes = 0
	}

	maxUnused := strings.TrimSpace(opts.MaxUnused)
	if maxUnused == "" {
		return errors.Fatalf("invalid value for --max-unused: %q", opts.MaxUnused)
	}

	// parse MaxUnused either as unlimited, a percentage, or an absolute number of bytes
	switch {
	case maxUnused == "unlimited":
		opts.maxUnusedBytes = func(_ uint64) uint64 {
			return math.MaxUint64
		}

	case strings.HasSuffix(maxUnused, "%"):
		maxUnused = strings.TrimSuffix(maxUnused, "%")
		p, err := strconv.ParseFloat(maxUnused, 64)
		if err != nil {
			return errors.Fatalf("invalid percentage %q passed for --max-unused: %v", opts.MaxUnused, err)
		}

		if p < 0 {
			return errors.Fatal("percentage for --max-unused must be positive")
		}

		if p >= 100 {
			return errors.Fatal("percentage for --max-unused must be below 100%")
		}

		opts.maxUnusedBytes = func(used uint64) uint64 {
			return uint64(p / (100 - p) * float64(used))
		}

	default:
		size, err := ui.ParseBytes(maxUnused)
		if err != nil {
			return errors.Fatalf("invalid number of bytes %q for --max-unused: %v", opts.MaxUnused, err)
		}

		opts.maxUnusedBytes = func(_ uint64) uint64 {
			return uint64(size)
		}
	}

	if opts.SmallPackSize != "" {
		size, err := ui.ParseBytes(opts.SmallPackSize)
		if err != nil {
			return errors.Fatalf("invalid number of bytes %q for --repack-smaller-than: %v", opts.SmallPackSize, err)
		} else if size <= 0 {
			return errors.Fatalf("--repack-smaller-than must be larger than zero")
		}
		opts.SmallPackBytes = uint64(size)
	}

	if opts.KeepDelete {
		if !feature.Flag.Enabled(feature.TwoPhasePrune) {
			return errors.Fatalf("--keep-delete requires the two-phase-prune feature (set VAULTIC_FEATURES=two-phase-prune=true)")
		}
		if opts.EarlyDeleteIndex {
			return errors.Fatalf("--keep-delete and --early-delete-index are mutually exclusive")
		}
		if len(opts.UnsafeNoSpaceRecovery) > 0 {
			return errors.Fatalf("--keep-delete and --unsafe-recover-no-free-space are mutually exclusive")
		}
	}

	return nil
}

// parseMaxRepack parses --max-repack: "unlimited", a percentage of the
// repository size ("10%"), or an absolute size with k/m/g/t suffixes. It
// returns the absolute byte cap (MaxUint64 if not an absolute size) and the
// percentage (>0 if a percentage was given; resolved against the repo size in
// repository.PlanPrune).
func parseMaxRepack(s string) (bytes uint64, percent float64, err error) {
	s = strings.TrimSpace(s)
	if s == "unlimited" || s == "" {
		return math.MaxUint64, 0, nil
	}
	if strings.HasSuffix(s, "%") {
		p, err := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
		if err != nil || p < 0 || p > 100 {
			return 0, 0, errors.Fatalf("invalid percentage %q for --max-repack", s)
		}
		return math.MaxUint64, p, nil
	}
	size, err := ui.ParseBytes(s)
	if err != nil {
		return 0, 0, errors.Fatalf("invalid size %q for --max-repack: %v", s, err)
	}
	return uint64(size), 0, nil
}

func runPrune(ctx context.Context, opts PruneOptions, gopts global.Options, term ui.Terminal) error {
	err := verifyPruneOptions(&opts)
	if err != nil {
		return err
	}

	if opts.RepackUncompressed && gopts.Compression == repository.CompressionOff {
		return errors.Fatal("disabled compression and `--repack-uncompressed` are mutually exclusive")
	}

	if gopts.NoLock && !opts.DryRun {
		return errors.Fatal("--no-lock is only applicable in combination with --dry-run for prune command")
	}

	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)

	// Unsafe recovery, dry-run, and early-index deletion retain the established
	// all-exclusive execution path. Their semantics either intentionally break
	// normal ordering or need immediate deletion to free space.
	if opts.UnsafeNoSpaceRecovery != "" || opts.DryRun || opts.EarlyDeleteIndex {
		ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, opts.DryRun && gopts.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		if opts.UnsafeNoSpaceRecovery != "" {
			repoID := repo.Config().ID
			if opts.UnsafeNoSpaceRecovery != repoID {
				return errors.Fatalf("must pass id '%s' to --unsafe-recover-no-free-space", repoID)
			}
			opts.unsafeRecovery = true
		}
		return runPruneWithRepo(ctx, opts, gopts, repo, vaultic.NewIDSet(), printer)
	}

	// Claim phase A under a short exclusive lock. The pending durable marker
	// prevents another prune from starting its own shared phase A while ordinary
	// append writers may proceed after this lock is released.
	baseCtx := ctx
	ctx, repo, unlock, err := openWithExclusiveLock(baseCtx, gopts, false, printer)
	if err != nil {
		return err
	}
	if err := requireLegacyMetadataMutation(repo, "prune"); err != nil {
		unlock()
		return err
	}
	if repo.Config().PrunePlan != nil {
		if opts.KeepDelete {
			unlock()
			return errors.Fatal("repository already contains a deferred prune plan; run prune without --keep-delete to finalize it first")
		}
		err := repo.FinalizePrunePlan(ctx, printer)
		unlock()
		return err
	}
	marker, err := repo.BeginPrunePlan(ctx)
	unlock()
	if err != nil {
		return err
	}

	// Phase A is additive: plan, repack, upload replacement packs/indexes, and
	// atomically promote the claimed marker under a shared append lock.
	ctx, repo, unlock, err = openWithLockPolicy(baseCtx, gopts, LockShared, lockOpenOptions{}, printer)
	if err != nil {
		return err
	}
	if repo.Config().PrunePlan == nil || repo.Config().PrunePlan.ID != marker.ID {
		unlock()
		return errors.Fatal("prune phase A claim changed before additive work began")
	}
	phaseAOpts := opts
	phaseAOpts.KeepDelete = true
	err = runPrunePhaseAWithRepo(ctx, phaseAOpts, gopts, repo, vaultic.NewIDSet(), marker.ID, printer)
	unlock()
	if err != nil || opts.KeepDelete {
		return err
	}
	if repo.Config().PrunePlan == nil {
		// Phase A had no obsolete objects and cleared its initial claim.
		return nil
	}

	// Phase B is the only exclusive window: re-open, revalidate the durable
	// marker against current indexes, and delete only marker-listed candidates.
	return finalizePrunePhaseB(baseCtx, gopts, printer)
}

func finalizePrunePhaseB(ctx context.Context, gopts global.Options, printer vaultic.Printer) error {
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if err := requireLegacyMetadataMutation(repo, "prune"); err != nil {
		return err
	}
	if repo.Config().PrunePlan == nil {
		return errors.Fatal("prune phase B has no durable plan to finalize")
	}
	return repo.FinalizePrunePlan(ctx, printer)
}

func runPruneWithRepo(ctx context.Context, opts PruneOptions, gopts global.Options, repo *repository.Repository, ignoreSnapshots vaultic.IDSet, printer vaultic.Printer) error {
	if err := requireLegacyMetadataMutation(repo, "prune"); err != nil {
		return err
	}
	if repo.Cache() == nil && !gopts.JSON {
		printer.S("warning: running prune without a cache, this may be very slow!")
	}

	if repo.Config().PrunePlan != nil {
		if opts.KeepDelete {
			return errors.Fatal("repository already contains a deferred prune plan; run prune without --keep-delete to finalize it first")
		}
		// Deferred cleanup is intentionally a standalone invocation: it gets
		// one fresh index observation for revalidation, then deletes only the
		// marker's exact candidates. A later prune can create a new plan.
		return repo.FinalizePrunePlan(ctx, printer)
	}

	// Loading the index before snapshots is safe under this exclusive lock.
	err := repo.LoadIndex(ctx, printer)
	if err != nil {
		return err
	}

	popts := repository.PruneOptions{
		DryRun:         opts.DryRun,
		UnsafeRecovery: opts.unsafeRecovery,

		MaxUnusedBytes: opts.maxUnusedBytes,
		MaxRepackBytes: opts.MaxRepackBytes,
		SmallPackBytes: opts.SmallPackBytes,

		RepackCacheableOnly: opts.RepackCacheableOnly,
		RepackUncompressed:  opts.RepackUncompressed,
		RepackAll:           opts.RepackAll,
		FastRepack:          opts.FastRepack,
		EarlyDeleteIndex:    opts.EarlyDeleteIndex,
		MaxRepackPercent:    opts.maxRepackPercent,
		KeepDelete:          opts.KeepDelete,
	}

	plan, err := repository.PlanPrune(ctx, popts, repo, func(ctx context.Context, repo vaultic.Repository, usedBlobs vaultic.FindBlobSet) error {
		return getUsedBlobs(ctx, repo, usedBlobs, ignoreSnapshots, printer)
	}, printer)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if popts.DryRun {
		printer.P("\nWould have made the following changes:")
	}

	if !gopts.JSON {
		err = printPruneStats(printer, plan.Stats())
		if err != nil {
			return err
		}
	} else {
		gopts.Term.Print(ui.ToJSONString(plan.Stats()))
	}

	// Trigger GC to reset garbage collection threshold
	runtime.GC()

	return plan.Execute(ctx, printer)
}

// runPrunePhaseAWithRepo runs the additive phase of a minimal-lock prune. It
// is separate from runPruneWithRepo because forget --prune already owns an
// exclusive lock and intentionally retains the classic single-window flow.
func runPrunePhaseAWithRepo(ctx context.Context, opts PruneOptions, gopts global.Options, repo *repository.Repository, ignoreSnapshots vaultic.IDSet, markerID string, printer vaultic.Printer) error {
	if err := requireLegacyMetadataMutation(repo, "prune"); err != nil {
		return err
	}
	if repo.Cache() == nil && !gopts.JSON {
		printer.S("warning: running prune without a cache, this may be very slow!")
	}
	if err := repo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	popts := repository.PruneOptions{
		DryRun:              false,
		MaxUnusedBytes:      opts.maxUnusedBytes,
		MaxRepackBytes:      opts.MaxRepackBytes,
		SmallPackBytes:      opts.SmallPackBytes,
		RepackCacheableOnly: opts.RepackCacheableOnly,
		RepackUncompressed:  opts.RepackUncompressed,
		RepackAll:           opts.RepackAll,
		FastRepack:          opts.FastRepack,
		MaxRepackPercent:    opts.maxRepackPercent,
		KeepDelete:          true,
	}
	plan, err := repository.PlanPrune(ctx, popts, repo, func(ctx context.Context, repo vaultic.Repository, usedBlobs vaultic.FindBlobSet) error {
		return getUsedBlobs(ctx, repo, usedBlobs, ignoreSnapshots, printer)
	}, printer)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	plan.BindPrunePlan(markerID)
	if !gopts.JSON {
		if err := printPruneStats(printer, plan.Stats()); err != nil {
			return err
		}
	} else {
		gopts.Term.Print(ui.ToJSONString(plan.Stats()))
	}
	runtime.GC()
	if err := plan.Execute(ctx, printer); err != nil {
		return err
	}
	if marker := repo.Config().PrunePlan; marker != nil && marker.ID == markerID && marker.State == "phase_a" {
		// No index rewrite occurred, so there is nothing to defer. Clear the
		// initial claim now instead of making a later prune finalize a no-op.
		return repo.FinalizePrunePlan(ctx, printer)
	}
	return nil
}

// printPruneStats prints out the statistics
func printPruneStats(printer vaultic.Printer, stats repository.PruneStats) error {
	printer.V("\nused:         %10d blobs / %s", stats.Blobs.Used, ui.FormatBytes(stats.Size.Used))
	if stats.Blobs.Duplicate > 0 {
		printer.V("duplicates:   %10d blobs / %s", stats.Blobs.Duplicate, ui.FormatBytes(stats.Size.Duplicate))
	}
	printer.V("unused:       %10d blobs / %s", stats.Blobs.Unused, ui.FormatBytes(stats.Size.Unused))
	if stats.Size.Unref > 0 {
		printer.V("unreferenced:                    %s", ui.FormatBytes(stats.Size.Unref))
	}
	printer.V("total:        %10d blobs / %s", stats.Blobs.Total, ui.FormatBytes(stats.Size.Total))
	printer.V("unused size: %s of total size", ui.FormatPercent(stats.Size.Duplicate+stats.Size.Unused, stats.Size.Total))

	printer.P("\nto repack:    %10d blobs / %s", stats.Blobs.Repack, ui.FormatBytes(stats.Size.Repack))
	printer.P("this removes: %10d blobs / %s", stats.Blobs.Repackrm, ui.FormatBytes(stats.Size.Repackrm))
	printer.P("to delete:    %10d blobs / %s", stats.Blobs.Remove, ui.FormatBytes(stats.Size.Remove+stats.Size.Unref))
	printer.P("total prune:  %10d blobs / %s", stats.Blobs.RemoveTotal, ui.FormatBytes(stats.Size.RemoveTotal))
	if stats.Size.Uncompressed > 0 {
		printer.P("not yet compressed:              %s", ui.FormatBytes(stats.Size.Uncompressed))
	}
	printer.P("remaining:    %10d blobs / %s", stats.Blobs.Remain, ui.FormatBytes(stats.Size.Remain))
	printer.P("unused size after prune: %s (%s of remaining size)",
		ui.FormatBytes(stats.Size.RemainUnused), ui.FormatPercent(stats.Size.RemainUnused, stats.Size.Remain))
	printer.P("")
	printer.V("totally used packs: %10d", stats.Packs.Used)
	printer.V("partly used packs:  %10d", stats.Packs.PartlyUsed)
	printer.V("unused packs:       %10d\n\n", stats.Packs.Unused)

	printer.V("to keep:      %10d packs", stats.Packs.Keep)
	printer.V("to repack:    %10d packs", stats.Packs.Repack)
	printer.V("to delete:    %10d packs", stats.Packs.Remove)
	if stats.Packs.Unref > 0 {
		printer.V("to delete:    %10d unreferenced packs\n\n", stats.Packs.Unref)
	}
	return nil
}

func getUsedBlobs(ctx context.Context, repo vaultic.Repository, usedBlobs vaultic.FindBlobSet, ignoreSnapshots vaultic.IDSet, printer vaultic.Printer) error {
	var snapshotTrees vaultic.IDs
	printer.P("loading all snapshots...")
	err := data.ForAllSnapshots(ctx, repo, repo, ignoreSnapshots,
		func(id vaultic.ID, sn *data.Snapshot, err error) error {
			if err != nil {
				debug.Log("failed to load snapshot %v (error %v)", id, err)
				return err
			}
			debug.Log("add snapshot %v (tree %v)", id, *sn.Tree)
			snapshotTrees = append(snapshotTrees, *sn.Tree)
			return nil
		})
	if err != nil {
		return errors.Fatalf("failed loading snapshot: %v", err)
	}

	printer.P("finding data that is still in use for %d snapshots", len(snapshotTrees))

	bar := printer.NewCounter("snapshots")
	bar.SetMax(uint64(len(snapshotTrees)))
	defer bar.Done()

	err = data.FindUsedBlobs(ctx, repo, snapshotTrees, usedBlobs, bar)
	if err != nil {
		return errors.Fatalf("failed finding blobs: %v", err)
	}

	return nil
}
