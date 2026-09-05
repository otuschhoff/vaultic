package main

import (
	"context"
	"path/filepath"
	"runtime"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/filter"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/restorer"
	"github.com/otuschhoff/vaultic/internal/ui"
	restoreui "github.com/otuschhoff/vaultic/internal/ui/restore"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newRestoreCommand(globalOptions *global.Options) *cobra.Command {
	var options restoreOptions

	cmd := &cobra.Command{
		Use:   "restore [flags] snapshotID",
		Short: "Extract the data from a snapshot",
		Long: `
The "restore" command extracts the data from a snapshot from the repository to
a directory.

The special snapshotID "latest" can be used to restore the latest snapshot in the
repository.

To only restore a specific subfolder, you can use the "snapshotID:subfolder"
syntax, where "subfolder" is a path within the snapshot tree as shown by
"vaultic ls".

POSIX ACLs are always restored by their numeric value, while file ownership can optionally be restored by name instead of numeric value.

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
		PreRunE: func(_ *cobra.Command, _ []string) error {
			finalizeSnapshotFilter(&options.SnapshotFilter)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRestore(cmd.Context(), options, *globalOptions, globalOptions.Term, args)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

// restoreOptions collects all options for the restore command.
type restoreOptions struct {
	filter.ExcludePatternOptions
	filter.IncludePatternOptions
	Target string
	data.SnapshotFilter
	DryRun              bool
	Sparse              bool
	Verify              bool
	Overwrite           restorer.OverwriteBehavior
	Delete              bool
	ExcludeXattrPattern []string
	IncludeXattrPattern []string
	OwnershipByName     bool
}

func (options *restoreOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVarP(&options.Target, "target", "t", "", "directory to extract data to")

	options.ExcludePatternOptions.Add(f)
	options.IncludePatternOptions.Add(f)

	f.StringArrayVar(&options.ExcludeXattrPattern, "exclude-xattr", nil, "exclude xattr by `pattern` (can be specified multiple times)")
	f.StringArrayVar(&options.IncludeXattrPattern, "include-xattr", nil, "include xattr by `pattern` (can be specified multiple times)")

	initSingleSnapshotFilter(f, &options.SnapshotFilter)
	f.BoolVar(&options.DryRun, "dry-run", false, "do not write any data, just show what would be done")
	f.BoolVar(&options.Sparse, "sparse", false, "restore files as sparse")
	f.BoolVar(&options.Verify, "verify", false, "verify restored files content")
	f.Var(&options.Overwrite, "overwrite", "overwrite behavior, one of (always|if-changed|if-newer|never)")
	f.BoolVar(
		&options.Delete,
		"delete",
		false,
		"delete files from target directory if they do not exist in snapshot. Use '--dry-run -vv' to check what would be deleted",
	)
	if runtime.GOOS != "windows" {
		f.BoolVar(&options.OwnershipByName, "ownership-by-name", false, "restore file ownership by user name and group name (except POSIX ACLs)")
	}
}

func runRestore(ctx context.Context, options restoreOptions, globalOptions global.Options,
	term ui.Terminal, args []string) error {

	var printer restoreui.ProgressPrinter
	if globalOptions.JSON {
		printer = restoreui.NewJSONProgress(term, globalOptions.Verbosity)
	} else {
		printer = restoreui.NewTextProgress(term, globalOptions.Verbosity)
	}

	excludePatternFns, err := options.ExcludePatternOptions.CollectPatterns(printer.E)
	if err != nil {
		return err
	}

	includePatternFns, err := options.IncludePatternOptions.CollectPatterns(printer.E)
	if err != nil {
		return err
	}

	hasExcludes := len(excludePatternFns) > 0
	hasIncludes := len(includePatternFns) > 0
	if err := validateRestoreOptions(options, args, hasExcludes, hasIncludes); err != nil {
		return err
	}

	snapshotIDString := args[0]

	debug.Log("restore %v to %v", snapshotIDString, options.Target)

	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	sn, subfolder, err := options.SnapshotFilter.FindLatest(ctx, repo, repo, snapshotIDString)
	if err != nil {
		return errors.Fatalf("failed to find snapshot: %v", err)
	}

	err = repo.LoadIndex(ctx, printer)
	if err != nil {
		return err
	}

	sn.Tree, err = data.FindTreeDirectory(ctx, repo, sn.Tree, subfolder)
	if err != nil {
		return err
	}

	progress := restoreui.NewProgress(printer, globalOptions.Quiet, globalOptions.JSON, term.CanUpdateStatus())
	res := restorer.NewRestorer(repo, sn, restorer.Options{
		DryRun:          options.DryRun,
		Sparse:          options.Sparse,
		Progress:        progress,
		Overwrite:       options.Overwrite,
		Delete:          options.Delete,
		OwnershipByName: options.OwnershipByName,
	})

	totalErrors := 0
	res.Error = func(location string, err error) error {
		totalErrors++
		return progress.Error(location, err)
	}
	res.Warn = func(message string) {
		printer.E("Warning: %s\n", message)
	}
	res.Info = func(message string) {
		if globalOptions.JSON {
			return
		}
		printer.P("Info: %s\n", message)
	}

	configureRestoreFilter(res, excludePatternFns, includePatternFns, hasExcludes, hasIncludes)

	res.XattrSelectFilter, err = getXattrSelectFilter(options, printer)
	if err != nil {
		return err
	}

	if !globalOptions.JSON {
		printer.P("restoring %s to %s\n", res.Snapshot(), options.Target)
	}

	countRestoredFiles, err := res.RestoreTo(ctx, options.Target)
	if err != nil {
		return err
	}

	progress.Finish()

	if totalErrors > 0 {
		return errors.Fatalf("There were %d errors", totalErrors)
	}

	if options.Verify {
		return verifyRestoredFiles(ctx, res, printer, options.Target, countRestoredFiles, totalErrors, globalOptions.JSON)
	}

	return nil
}

func validateRestoreOptions(options restoreOptions, args []string, hasExcludes, hasIncludes bool) error {
	switch {
	case len(args) == 0:
		return errors.Fatal("no snapshot ID specified")
	case len(args) > 1:
		return errors.Fatalf("more than one snapshot ID specified: %v", args)
	case options.Target == "":
		return errors.Fatal("please specify a directory to restore to (--target)")
	case hasExcludes && hasIncludes:
		return errors.Fatal("exclude and include patterns are mutually exclusive")
	case options.DryRun && options.Verify:
		return errors.Fatal("--dry-run and --verify are mutually exclusive")
	case options.Delete && filepath.Clean(options.Target) == "/" && !hasExcludes && !hasIncludes:
		return errors.Fatal("'--target / --delete' must be combined with an include or exclude filter")
	default:
		return nil
	}
}

func verifyRestoredFiles(
	ctx context.Context,
	restore *restorer.Restorer,
	printer restoreui.ProgressPrinter,
	target string,
	countRestoredFiles uint64,
	totalErrors int,
	jsonOutput bool,
) error {
	if !jsonOutput {
		printer.P("verifying files in %s\n", target)
	}
	started := time.Now()
	bar := printer.NewCounterTerminalOnly("files verified")
	count, err := restore.VerifyFiles(ctx, target, countRestoredFiles, bar)
	if err != nil {
		return err
	}
	if totalErrors > 0 {
		return errors.Fatalf("There were %d errors", totalErrors)
	}
	if !jsonOutput {
		printer.P("finished verifying %d files in %s (took %s)\n", count, target,
			time.Since(started).Round(time.Millisecond))
	}
	return nil
}

func configureRestoreFilter(
	restorer *restorer.Restorer,
	excludePatternFns []filter.RejectByNameFunc,
	includePatternFns []filter.IncludeByNameFunc,
	hasExcludes bool,
	hasIncludes bool,
) {
	selectExcludeFilter := func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool) {
		matched := false
		for _, rejectFn := range excludePatternFns {
			matched = matched || rejectFn(item)

			// implementing a short-circuit here to improve the performance
			// to prevent additional pattern matching once the first pattern
			// matches.
			if matched {
				break
			}
		}
		// An exclude filter is basically a 'wildcard but foo',
		// so even if a childMayMatch, other children of a dir may not,
		// therefore childMayMatch does not matter, but we should not go down
		// unless the dir is selected for restore
		selectedForRestore = !matched
		childMayBeSelected = selectedForRestore && isDir

		return selectedForRestore, childMayBeSelected
	}

	selectIncludeFilter := func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool) {
		selectedForRestore = false
		childMayBeSelected = false
		for _, includeFn := range includePatternFns {
			matched, childMayMatch := includeFn(item)
			selectedForRestore = selectedForRestore || matched
			childMayBeSelected = childMayBeSelected || childMayMatch

			if selectedForRestore && childMayBeSelected {
				break
			}
		}
		childMayBeSelected = childMayBeSelected && isDir

		return selectedForRestore, childMayBeSelected
	}

	if hasExcludes {
		restorer.SelectFilter = selectExcludeFilter
	} else if hasIncludes {
		restorer.SelectFilter = selectIncludeFilter
	}
}

func getXattrSelectFilter(options restoreOptions, printer vaultic.Printer) (func(xattrName string) bool, error) {
	hasXattrExcludes := len(options.ExcludeXattrPattern) > 0
	hasXattrIncludes := len(options.IncludeXattrPattern) > 0

	if hasXattrExcludes && hasXattrIncludes {
		return nil, errors.Fatal("exclude and include xattr patterns are mutually exclusive")
	}

	if hasXattrExcludes {
		if err := filter.ValidatePatterns(options.ExcludeXattrPattern); err != nil {
			return nil, errors.Fatalf("--exclude-xattr: %s", err)
		}

		return func(xattrName string) bool {
			shouldReject := filter.RejectByPattern(options.ExcludeXattrPattern, printer.E)(xattrName)
			return !shouldReject
		}, nil
	}

	if hasXattrIncludes {
		// User has either input include xattr pattern(s) or we're using our default include pattern
		if err := filter.ValidatePatterns(options.IncludeXattrPattern); err != nil {
			return nil, errors.Fatalf("--include-xattr: %s", err)
		}

		return func(xattrName string) bool {
			shouldInclude, _ := filter.IncludeByPattern(options.IncludeXattrPattern, printer.E)(xattrName)
			return shouldInclude
		}, nil
	}

	// default to including all xattrs
	return func(_ string) bool { return true }, nil
}
