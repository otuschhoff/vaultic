package indexcmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
)

type indexPathIndexOptions struct {
	Daemon      indexDaemonOptions
	Paths       []string
	PruneBefore uint64
	DryRun      bool
}

func newIndexPathIndexCommand(globalOptions *global.Options) *cobra.Command {
	var options indexPathIndexOptions
	command := &cobra.Command{
		Use:   "path-index",
		Short: "Build the optional path history index",
		Long: "Build or refresh pv path-index entries for explicitly selected paths. The index is optional and derived; " +
			"path history falls back to the immutable walk when it is absent." + indexExitStatus,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := runIndexPathIndex(command.Context(), options, *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringSliceVar(&options.Paths, "path", nil, "path to index; may be supplied multiple times")
	command.Flags().Uint64Var(&options.PruneBefore, "prune-before", 0, "delete pv bindings before this commit sequence")
	command.Flags().BoolVar(&options.DryRun, "dry-run", false, "report changes without writing pv records")
	return command
}

func runIndexPathIndex(ctx context.Context, options indexPathIndexOptions, globalOptions global.Options, term ui.Terminal) (any, error) {
	config, err := options.Daemon.config("")
	if err != nil {
		return nil, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, options.DryRun, printer)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := requireSlateDBRepository(repo); err != nil {
		return nil, err
	}
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return nil, err
	}
	defer closeStore()
	if options.PruneBefore != 0 {
		removed, err := maintenance.PrunePathVersionIndex(ctx, store, options.PruneBefore, options.DryRun)
		if err == nil && !globalOptions.JSON {
			printer.P("pruned %d path-index bindings before commit %d\n", removed, options.PruneBefore)
		}
		return map[string]uint64{"pruned": removed, "before": options.PruneBefore}, err
	}
	paths := append([]string(nil), repo.Config().PathIndexPaths...)
	paths = append(paths, options.Paths...)
	if len(paths) == 0 {
		return nil, fmt.Errorf("index path-index requires at least one configured path_index_paths entry or --path")
	}
	result, err := maintenance.RebuildPathVersionIndex(ctx, store, paths, options.DryRun)
	if err == nil && !globalOptions.JSON {
		printer.P(
			"scanned %d snapshots; changed %d path bindings; overflow paths %d; estimated bytes %d\n",
			result.SnapshotsScanned,
			result.BindingsChanged,
			result.OverflowPaths,
			result.BytesWritten,
		)
	}
	return result, err
}
