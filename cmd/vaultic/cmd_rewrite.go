package main

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/filter"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/walker"
)

func newRewriteCommand(globalOptions *global.Options) *cobra.Command {
	var options rewriteOptions

	cmd := &cobra.Command{
		Use:   "rewrite [flags] [snapshotID ...]",
		Short: "Rewrite snapshots to exclude files or change metadata",
		Long: `
The "rewrite" command creates new snapshots from existing ones. You can use
exclude or include filters to control which files are included in the new
snapshots. Unless --new-host or --new-time is specified, metadata (time, host,
tags) is preserved.

The snapshots to rewrite are specified using the --host, --tag and --path options,
or by providing a list of snapshot IDs. Please note that specifying neither any of
these options nor a snapshot ID will cause the command to rewrite all snapshots.

The special tag 'rewrite' will be added to the new snapshots to distinguish
them from the original ones, unless --forget is used. If the --forget option is
used, the original snapshots will instead be directly removed from the repository.

Please note that the --forget option only removes the snapshots and not the actual
data stored in the repository. In order to delete the no longer referenced data,
use the "prune" command.

When rewrite is used with the --snapshot-summary option, a new snapshot is
created containing statistics summary data. Only two fields in the summary will
be non-zero: TotalFilesProcessed and TotalBytesProcessed.

When rewrite is called with one of the --exclude or --include options,
TotalFilesProcessed and TotalBytesProcessed will be updated in the snapshot summary.

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
			return runRewrite(cmd.Context(), options, *globalOptions, args, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

type snapshotMetadata struct {
	Hostname string
	Time     *time.Time
}

type snapshotMetadataArgs struct {
	Hostname string
	Time     string
}

func (sma snapshotMetadataArgs) empty() bool {
	return sma.Hostname == "" && sma.Time == ""
}

func (sma snapshotMetadataArgs) convert() (*snapshotMetadata, error) {
	if sma.empty() {
		return nil, nil
	}

	var timeStamp *time.Time
	if sma.Time != "" {
		t, err := time.ParseInLocation(global.TimeFormat, sma.Time, time.Local)
		if err != nil {
			return nil, errors.Fatalf("error in time option: %v", err)
		}
		timeStamp = &t
	}
	return &snapshotMetadata{Hostname: sma.Hostname, Time: timeStamp}, nil
}

// rewriteOptions collects all options for the rewrite command.
type rewriteOptions struct {
	Forget          bool
	DryRun          bool
	SnapshotSummary bool

	Metadata snapshotMetadataArgs
	data.SnapshotFilter
	filter.ExcludePatternOptions
	filter.IncludePatternOptions
}

func (options *rewriteOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVarP(&options.Forget, "forget", "", false, "remove original snapshots after creating new ones")
	f.BoolVarP(&options.DryRun, "dry-run", "n", false, "do not do anything, just print what would be done")
	f.StringVar(&options.Metadata.Hostname, "new-host", "", "replace hostname")
	f.StringVar(&options.Metadata.Time, "new-time", "", "replace time of the backup")
	f.BoolVarP(&options.SnapshotSummary, "snapshot-summary", "s", false, "create snapshot summary record if it does not exist")

	initMultiSnapshotFilter(f, &options.SnapshotFilter, true)
	options.ExcludePatternOptions.Add(f)
	options.IncludePatternOptions.Add(f)
}

// rewriteFilterFunc returns the filtered tree ID or an error. If a snapshot summary is returned, the snapshot will
// be updated accordingly.
type rewriteFilterFunc func(ctx context.Context, sn *data.Snapshot, uploader vaultic.BlobSaver) (vaultic.ID, *data.SnapshotSummary, error)

func rewriteSnapshot(ctx context.Context, repo *repository.Repository, sn *data.Snapshot, options rewriteOptions, printer vaultic.Printer) (bool, error) {
	if sn.Tree == nil {
		return false, errors.Errorf("snapshot %v has nil tree", sn.ID().Str())
	}

	rejectByNameFuncs, err := options.ExcludePatternOptions.CollectPatterns(printer.E)
	if err != nil {
		return false, err
	}

	includeByNameFuncs, err := options.IncludePatternOptions.CollectPatterns(printer.E)
	if err != nil {
		return false, err
	}

	metadata, err := options.Metadata.convert()

	if err != nil {
		return false, err
	}

	filter := newRewriteFilter(repo, includeByNameFuncs, rejectByNameFuncs, options.SnapshotSummary, printer)

	return filterAndReplaceSnapshot(ctx, rewriteRequest{
		repo: repo, snapshot: sn, filter: filter, dryRun: options.DryRun, forget: options.Forget,
		newMetadata: metadata, addTag: "rewrite", printer: printer,
		keepEmptySnapshot: len(includeByNameFuncs) > 0,
	})
}

func newRewriteFilter(
	repo *repository.Repository,
	includeByNameFuncs []filter.IncludeByNameFunc,
	rejectByNameFuncs []filter.RejectByNameFunc,
	withSnapshotSummary bool,
	printer vaultic.Printer,
) rewriteFilterFunc {
	if len(includeByNameFuncs) == 0 && len(rejectByNameFuncs) == 0 && !withSnapshotSummary {
		return func(_ context.Context, snapshot *data.Snapshot, _ vaultic.BlobSaver) (vaultic.ID, *data.SnapshotSummary, error) {
			return *snapshot.Tree, nil, nil
		}
	}

	var rewriteNode walker.NodeRewriteFunc
	var keepEmptyDirectory walker.NodeKeepEmptyDirectoryFunc
	if len(includeByNameFuncs) > 0 {
		rewriteNode, keepEmptyDirectory = gatherIncludeFilters(includeByNameFuncs, printer)
	} else {
		rewriteNode = gatherExcludeFilters(rejectByNameFuncs, printer)
	}
	rewriter, querySize := walker.NewSnapshotSizeRewriter(rewriteNode, keepEmptyDirectory)
	return func(ctx context.Context, snapshot *data.Snapshot, uploader vaultic.BlobSaver) (vaultic.ID, *data.SnapshotSummary, error) {
		id, err := rewriter.RewriteTree(ctx, repo, uploader, "/", *snapshot.Tree)
		if err != nil {
			return vaultic.ID{}, nil, err
		}
		size := querySize()
		summary := &data.SnapshotSummary{}
		if snapshot.Summary != nil {
			*summary = *snapshot.Summary
		}
		summary.TotalFilesProcessed = size.FileCount
		summary.TotalBytesProcessed = size.FileSize
		return id, summary, nil
	}
}

type rewriteRequest struct {
	repo              vaultic.Repository
	snapshot          *data.Snapshot
	filter            rewriteFilterFunc
	dryRun            bool
	forget            bool
	newMetadata       *snapshotMetadata
	addTag            string
	printer           vaultic.Printer
	keepEmptySnapshot bool
}

func filterAndReplaceSnapshot(ctx context.Context, request rewriteRequest) (bool, error) {
	var filteredTree vaultic.ID
	var summary *data.SnapshotSummary
	err := request.repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var err error
		filteredTree, summary, err = request.filter(ctx, request.snapshot, uploader)
		return err
	})
	if err != nil {
		return false, err
	}

	if filteredTree.IsNull() {
		return handleEmptyRewriteSnapshot(ctx, request)
	}

	if rewriteSnapshotUnchanged(request, filteredTree, summary) {
		debug.Log("Snapshot %v not modified", request.snapshot)
		return false, nil
	}

	debug.Log("Snapshot %v modified", request.snapshot)
	if request.dryRun {
		printRewriteDryRun(request)
		return true, nil
	}

	return saveRewrittenSnapshot(ctx, request, filteredTree, summary)
}

func handleEmptyRewriteSnapshot(ctx context.Context, request rewriteRequest) (bool, error) {
	if request.keepEmptySnapshot {
		debug.Log("Snapshot %v not modified", request.snapshot)
		return false, nil
	}
	if request.dryRun {
		request.printer.P("would delete empty snapshot")
		return true, nil
	}
	if err := request.repo.RemoveUnpacked(ctx, vaultic.WriteableSnapshotFile, *request.snapshot.ID()); err != nil {
		return false, err
	}
	debug.Log("removed empty snapshot %v", request.snapshot.ID())
	request.printer.P("removed empty snapshot %v", request.snapshot.ID().Str())
	return true, nil
}

func rewriteSnapshotUnchanged(request rewriteRequest, filteredTree vaultic.ID, summary *data.SnapshotSummary) bool {
	matchingSummary := summary == nil || request.snapshot.Summary != nil && *summary == *request.snapshot.Summary
	return filteredTree == *request.snapshot.Tree && request.newMetadata == nil && matchingSummary
}

func printRewriteDryRun(request rewriteRequest) {
	request.printer.P("would save new snapshot")
	if request.forget {
		request.printer.P("would remove old snapshot")
	}
	if request.newMetadata != nil && request.newMetadata.Time != nil {
		request.printer.P("would set time to %s", request.newMetadata.Time)
	}
	if request.newMetadata != nil && request.newMetadata.Hostname != "" {
		request.printer.P("would set hostname to %s", request.newMetadata.Hostname)
	}
}

func saveRewrittenSnapshot(ctx context.Context, request rewriteRequest, filteredTree vaultic.ID, summary *data.SnapshotSummary) (bool, error) {
	// Always set the original snapshot id as this essentially a new snapshot.
	request.snapshot.Original = request.snapshot.ID()
	request.snapshot.Tree = &filteredTree
	if summary != nil {
		request.snapshot.Summary = summary
	}

	if !request.forget {
		request.snapshot.AddTags([]string{request.addTag})
	}

	if request.newMetadata != nil && request.newMetadata.Time != nil {
		request.printer.P("setting time to %s", *request.newMetadata.Time)
		request.snapshot.Time = *request.newMetadata.Time
	}

	if request.newMetadata != nil && request.newMetadata.Hostname != "" {
		request.printer.P("setting host to %s", request.newMetadata.Hostname)
		request.snapshot.Hostname = request.newMetadata.Hostname
	}

	// Save the new snapshot.
	id, err := data.SaveSnapshot(ctx, request.repo, request.snapshot)
	if err != nil {
		return false, err
	}
	request.printer.P("saved new snapshot %v", id.Str())

	if request.forget {
		if err = request.repo.RemoveUnpacked(ctx, vaultic.WriteableSnapshotFile, *request.snapshot.ID()); err != nil {
			return false, err
		}
		debug.Log("removed old snapshot %v", request.snapshot.ID())
		request.printer.P("removed old snapshot %v", request.snapshot.ID().Str())
	}
	return true, nil
}

func runRewrite(ctx context.Context, options rewriteOptions, globalOptions global.Options, args []string, term ui.Terminal) error {
	hasExcludes := !options.ExcludePatternOptions.Empty()
	hasIncludes := !options.IncludePatternOptions.Empty()
	if !options.SnapshotSummary && !hasExcludes && !hasIncludes && options.Metadata.empty() {
		return errors.Fatal("Nothing to do: no excludes/includes provided and no new metadata provided")
	} else if hasExcludes && hasIncludes {
		return errors.Fatal("exclude and include patterns are mutually exclusive")
	}

	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	var (
		repo   *repository.Repository
		unlock func()
		err    error
	)

	if options.Forget {
		printer.P("create exclusive lock for repository")
		ctx, repo, unlock, err = openWithExclusiveLock(ctx, globalOptions, options.DryRun, printer)
	} else {
		ctx, repo, unlock, err = openWithAppendLock(ctx, globalOptions, options.DryRun, printer)
	}
	if err != nil {
		return err
	}
	defer unlock()

	snapshotLister, err := vaultic.MemorizeList(ctx, repo, vaultic.SnapshotFile)
	if err != nil {
		return err
	}

	if err = repo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	changedCount := 0
	err = options.SnapshotFilter.FindAll(ctx, snapshotLister, repo, args, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		printer.P("\n%v", sn)
		changed, err := rewriteSnapshot(ctx, repo, sn, options, printer)
		if err != nil {
			return errors.Fatalf("unable to rewrite snapshot ID %q: %v", sn.ID().Str(), err)
		}
		if changed {
			changedCount++
		}
		return nil
	})
	if err != nil {
		return err
	}

	printer.P("")
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if changedCount == 0 {
		if !options.DryRun {
			printer.P("no snapshots were modified")
		} else {
			printer.P("no snapshots would be modified")
		}
	} else {
		if !options.DryRun {
			printer.P("modified %v snapshots", changedCount)
		} else {
			printer.P("would modify %v snapshots", changedCount)
		}
	}

	return nil
}

func gatherIncludeFilters(
	includeByNameFuncs []filter.IncludeByNameFunc,
	printer vaultic.Printer,
) (rewriteNode walker.NodeRewriteFunc, keepEmptyDirectory walker.NodeKeepEmptyDirectoryFunc) {
	inSelectByName := func(nodepath string, node *data.Node) bool {
		for _, include := range includeByNameFuncs {
			matched, childMayMatch := include(nodepath)
			if node.Type == data.NodeTypeDir {
				// include directories if they or some of their children may be included
				if matched || childMayMatch {
					return true
				}
			} else if matched {
				return true
			}
		}
		return false
	}

	rewriteNode = func(node *data.Node, path string) *data.Node {
		if inSelectByName(path, node) {
			if node.Type != data.NodeTypeDir {
				printer.VV("including %q\n", path)
			}
			return node
		}
		return nil
	}

	inSelectByNameDir := func(nodepath string) bool {
		for _, include := range includeByNameFuncs {
			matched, _ := include(nodepath)
			if matched {
				return matched
			}
		}
		return false
	}

	keepEmptyDirectory = func(path string) bool {
		keep := inSelectByNameDir(path)
		if keep {
			printer.VV("including directory %q\n", path)
		}
		return keep
	}

	return rewriteNode, keepEmptyDirectory
}

func gatherExcludeFilters(excludeByNameFuncs []filter.RejectByNameFunc, printer vaultic.Printer) (rewriteNode walker.NodeRewriteFunc) {
	exSelectByName := func(nodepath string) bool {
		for _, reject := range excludeByNameFuncs {
			if reject(nodepath) {
				return false
			}
		}
		return true
	}

	rewriteNode = func(node *data.Node, path string) *data.Node {
		if exSelectByName(path) {
			return node
		}

		printer.VV("excluding %q\n", path)
		return nil
	}

	return rewriteNode
}
