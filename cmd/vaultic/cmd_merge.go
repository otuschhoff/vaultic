package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newMergeCommand(globalOptions *global.Options) *cobra.Command {
	var options mergeOptions
	cmd := &cobra.Command{
		Use:               "merge [flags] snapshotID [snapshotID ...]",
		Short:             "Merge snapshots into a new snapshot",
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			finalizeSnapshotFilter(&options.SnapshotFilter)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMerge(cmd.Context(), options, *globalOptions, args, globalOptions.Term)
		},
	}
	options.AddFlags(cmd.Flags())
	return cmd
}

type mergeOptions struct {
	data.SnapshotFilter
	Label string
}

func (options *mergeOptions) AddFlags(f *pflag.FlagSet) {
	// Kept separate from initMultiSnapshotFilter because merge requires explicit
	// source snapshots; filters constrain latest/N resolution only.
	// pflag.FlagSet satisfies this small interface.
	initMultiSnapshotFilter(f, &options.SnapshotFilter, true)
	f.StringVar(&options.Label, "label", "", "label for the merged snapshot (default: newest source)")
}

func runMerge(ctx context.Context, options mergeOptions, globalOptions global.Options, args []string, term ui.Terminal) error {
	if len(args) < 2 {
		return errors.Fatal("merge requires at least two snapshots")
	}
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithAppendLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if err := repo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	var snapshots data.Snapshots
	if err := options.SnapshotFilter.FindAll(ctx, repo, repo, args, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		snapshots = append(snapshots, sn)
		return nil
	}); err != nil {
		return err
	}
	if len(snapshots) < 2 {
		return errors.Fatal("merge requires at least two distinct snapshots")
	}
	sort.SliceStable(snapshots, func(i, j int) bool { return snapshots[i].Time.Before(snapshots[j].Time) })

	appendRepo := repo.AppendTransaction()
	var treeID vaultic.ID
	err = appendRepo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		trees := make([]vaultic.ID, 0, len(snapshots))
		for _, snapshot := range snapshots {
			if snapshot.Tree == nil {
				return fmt.Errorf("snapshot %s has no tree", snapshot.ID().Str())
			}
			trees = append(trees, *snapshot.Tree)
		}
		var err error
		treeID, err = mergeTrees(ctx, appendRepo, uploader, trees)
		return err
	})
	if err != nil {
		return err
	}

	newest := *snapshots[len(snapshots)-1]
	newest.Tree = &treeID
	newest.Time = time.Now()
	newest.Parent = nil
	newest.Original = nil
	newest.MergedSnapshots = make([]vaultic.ID, 0, len(snapshots))
	for _, snapshot := range snapshots {
		newest.MergedSnapshots = append(newest.MergedSnapshots, *snapshot.ID())
	}
	if options.Label != "" {
		newest.Label = options.Label
	}
	id, err := data.SaveSnapshot(ctx, appendRepo, &newest)
	if err != nil {
		return err
	}
	if globalOptions.JSON {
		return json.NewEncoder(term.OutputWriter()).Encode(newest)
	}
	printer.S("merged %d snapshots into %s", len(snapshots), id.Str())
	return nil
}

func mergeTrees(ctx context.Context, repo vaultic.AppendRepository, saver vaultic.BlobSaver, trees []vaultic.ID) (vaultic.ID, error) {
	nodes := make(map[string]*data.Node)
	for _, id := range trees {
		tree, err := data.LoadTree(ctx, repo, id)
		if err != nil {
			return vaultic.ID{}, err
		}
		for item := range tree {
			if item.Error != nil {
				return vaultic.ID{}, item.Error
			}
			node, err := cloneNode(item.Node)
			if err != nil {
				return vaultic.ID{}, err
			}
			if previous := nodes[node.Name]; previous != nil && previous.Type == data.NodeTypeDir && node.Type == data.NodeTypeDir && previous.Subtree != nil &&
				node.Subtree != nil {
				merged, err := mergeTrees(ctx, repo, saver, []vaultic.ID{*previous.Subtree, *node.Subtree})
				if err != nil {
					return vaultic.ID{}, err
				}
				node.Subtree = &merged
			}
			nodes[node.Name] = node // snapshots are oldest->newest; newest wins.
		}
	}
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	return data.SaveTree(ctx, saver, func(yield func(data.NodeOrError) bool) {
		for _, name := range names {
			if !yield(data.NodeOrError{Node: nodes[name]}) {
				return
			}
		}
	})
}

func cloneNode(node *data.Node) (*data.Node, error) {
	buf, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}
	var result data.Node
	if err := json.Unmarshal(buf, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
