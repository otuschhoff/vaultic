package main

import (
	"context"
	"os"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
)

func newRecoverCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recover [flags]",
		Short: "Recover data from the repository not referenced by snapshots",
		Long: `
The "recover" command builds a new snapshot from all directories it can find in
the raw data of the repository which are not referenced in an existing snapshot.
It can be used if, for example, a snapshot has been removed by accident with "forget".

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
			return runRecover(cmd.Context(), *globalOptions, globalOptions.Term)
		},
	}
	return cmd
}

func runRecover(ctx context.Context, globalOptions global.Options, term ui.Terminal) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	snapshotLister, err := vaultic.MemorizeList(ctx, repo, vaultic.SnapshotFile)
	if err != nil {
		return err
	}

	printer.P("ensuring index is complete\n")
	err = repository.RepairIndex(ctx, repo, repository.RepairIndexOptions{}, printer)
	if err != nil {
		return err
	}

	printer.P("load index files\n")
	if err = repo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	trees, err := findRecoverTrees(ctx, repo, printer)
	if err != nil {
		return err
	}

	printer.P("load snapshots\n")
	err = data.ForAllSnapshots(ctx, snapshotLister, repo, nil, func(_ vaultic.ID, sn *data.Snapshot, _ error) error {
		trees[*sn.Tree] = true
		return nil
	})
	if err != nil {
		return err
	}
	printer.P("done\n")

	roots := vaultic.NewIDSet()
	for id, seen := range trees {
		if !seen {
			printer.V("found root tree %v\n", id.Str())
			roots.Insert(id)
		}
	}
	printer.S("\nfound %d unreferenced roots\n", len(roots))

	if len(roots) == 0 {
		printer.P("no snapshot to write.\n")
		return nil
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	var treeID vaultic.ID
	err = repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var err error
		tw := data.NewTreeWriter(uploader)
		for id := range roots {
			var subtreeID = id
			node := data.Node{
				Type:       data.NodeTypeDir,
				Name:       id.Str(),
				Mode:       0755,
				Subtree:    &subtreeID,
				AccessTime: time.Now(),
				ModTime:    time.Now(),
				ChangeTime: time.Now(),
			}
			err := tw.AddNode(&node)
			if err != nil {
				return err
			}
		}

		treeID, err = tw.Finalize(ctx)
		if err != nil {
			return errors.Fatalf("unable to save new tree to the repository: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return createSnapshot(ctx, printer, "/recover", hostname, []string{"recovered"}, repo, &treeID)

}

func findRecoverTrees(ctx context.Context, repo *repository.Repository, printer vaultic.Printer) (map[vaultic.ID]bool, error) {
	trees := make(map[vaultic.ID]bool)
	if err := repo.ListBlobs(ctx, func(blob vaultic.PackBlob) {
		handle := blob.Handle()
		if handle.Type == vaultic.TreeBlob {
			trees[handle.ID] = false
		}
	}); err != nil {
		return nil, err
	}
	printer.P("load %d trees\n", len(trees))
	bar := printer.NewCounter("trees loaded")
	bar.SetMax(uint64(len(trees)))
	for id := range trees {
		tree, err := data.LoadTree(ctx, repo, id)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			printer.E("unable to load tree %v: %v\n", id.Str(), err)
			continue
		}
		for item := range tree {
			if item.Error != nil {
				return nil, item.Error
			}
			if item.Node.Type == data.NodeTypeDir && item.Node.Subtree != nil {
				trees[*item.Node.Subtree] = true
			}
		}
		bar.Add(1)
	}
	bar.Done()
	return trees, nil
}

func createSnapshot(
	ctx context.Context,
	printer vaultic.Printer,
	name, hostname string,
	tags []string,
	repo vaultic.SaverUnpacked[vaultic.WriteableFileType],
	tree *vaultic.ID,
) error {
	sn, err := data.NewSnapshot([]string{name}, tags, hostname, time.Now())
	if err != nil {
		return errors.Fatalf("unable to save snapshot: %v", err)
	}

	sn.Tree = tree

	id, err := data.SaveSnapshot(ctx, repo, sn)
	if err != nil {
		return errors.Fatalf("unable to save snapshot: %v", err)
	}

	printer.S("saved new snapshot %v\n", id.Str())
	return nil
}
