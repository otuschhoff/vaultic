package main

import (
	"context"
	"slices"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/walker"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newRepairSnapshotsCommand(globalOptions *global.Options) *cobra.Command {
	var options repairOptions

	cmd := &cobra.Command{
		Use:   "snapshots [flags] [snapshot ID] [...]",
		Short: "Repair snapshots",
		Long: `
The "repair snapshots" command repairs broken snapshots. It scans the given
snapshots and generates new ones with damaged directories and file contents
removed. If the broken snapshots are deleted, a prune run will be able to
clean up the repository.

The command depends on a correct index, thus make sure to run "repair index"
first!


WARNING
=======

Repairing and deleting broken snapshots causes data loss! It will remove broken
directories and modify broken files in the modified snapshots.

If the contents of directories and files are still available, the better option
is to run "backup" which in that case is able to heal existing snapshots. Only
use the "repair snapshots" command if you need to recover an old and broken
snapshot!

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			finalizeSnapshotFilter(&options.SnapshotFilter)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRepairSnapshots(cmd.Context(), *globalOptions, options, args, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

// repairOptions collects all options for the repair command.
type repairOptions struct {
	DryRun bool
	Forget bool

	data.SnapshotFilter
}

func (options *repairOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVarP(&options.DryRun, "dry-run", "n", false, "do not do anything, just print what would be done")
	f.BoolVarP(&options.Forget, "forget", "", false, "remove original snapshots after creating new ones")

	initMultiSnapshotFilter(f, &options.SnapshotFilter, true)
}

// handleUnreadableSnapshotFile is called when FindAll returns an error for ID 'id'
func handleUnreadableSnapshotFile(
	ctx context.Context,
	be vaultic.Lister,
	repo vaultic.Repository,
	options repairOptions,
	id string,
	args []string,
	printer vaultic.Printer,
) (bool, error) {
	brokenID, err := vaultic.Find(ctx, be, vaultic.SnapshotFile, id)
	if err != nil {
		return false, err
	}

	if options.Forget && slices.Index(args, id) >= 0 {
		if options.DryRun {
			printer.P("would remove unreadable snapshot %v", brokenID)
			return true, nil
		}

		if err := repo.RemoveUnpacked(ctx, vaultic.WriteableSnapshotFile, brokenID); err != nil {
			return false, errors.Wrapf(err, "unable to remove unreadable snapshot file %v", brokenID)
		}
		printer.P("removed unreadable snapshot %v", brokenID)
		return true, nil
	}

	return false, errors.Fatalf(
		"snapshot file %[1]s is unreadable, use `vaultic repair snapshots --forget %[1]s` to remove it. Original error: %v",
		brokenID,
		err,
	)
}

func runRepairSnapshots(ctx context.Context, globalOptions global.Options, options repairOptions, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, options.DryRun, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if err := requireLegacyMetadataMutation(repo, "repair snapshots"); err != nil {
		return err
	}

	snapshotLister, err := vaultic.MemorizeList(ctx, repo, vaultic.SnapshotFile)
	if err != nil {
		return err
	}

	if err := repo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	rewriter := newSnapshotRepairRewriter(repo, printer)

	changedCount := 0
	errOuter := options.SnapshotFilter.FindAll(ctx, snapshotLister, repo, args, func(id string, sn *data.Snapshot, err error) error {
		if err != nil {
			changed, err := handleUnreadableSnapshotFile(ctx, snapshotLister, repo, options, id, args, printer)
			if changed {
				changedCount++
			}
			return err
		}

		printer.P("\n%v", sn)
		changed, err := filterAndReplaceSnapshot(ctx, rewriteRequest{
			repo: repo, snapshot: sn,
			filter: func(ctx context.Context, sn *data.Snapshot, uploader vaultic.BlobSaver) (vaultic.ID, *data.SnapshotSummary, error) {
				id, err := rewriter.RewriteTree(ctx, repo, uploader, "/", *sn.Tree)
				return id, nil, err
			},
			dryRun: options.DryRun, forget: options.Forget, addTag: "repaired", printer: printer,
		})
		if err != nil {
			return errors.Fatalf("unable to rewrite snapshot ID %q: %v", sn.ID().Str(), err)
		}
		if changed {
			changedCount++
		}
		return nil
	})

	if errOuter != nil {
		return errOuter
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

func newSnapshotRepairRewriter(repo *repository.Repository, printer vaultic.Printer) *walker.TreeRewriter {
	return walker.NewTreeRewriter(walker.RewriteOpts{
		RewriteNode: func(node *data.Node, path string) *data.Node {
			if node.Type == data.NodeTypeIrregular || node.Type == data.NodeTypeInvalid {
				printer.P("  file %q: removed node with invalid type %q", path, node.Type)
				return nil
			}
			if node.Type != data.NodeTypeFile {
				return node
			}

			ok := true
			var newContent = vaultic.IDs{}
			var newSize uint64
			// check all contents and remove if not available
			for _, id := range node.Content {
				if size, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}); !found {
					ok = false
				} else {
					newContent = append(newContent, id)
					newSize += uint64(size)
				}
			}
			if !ok {
				printer.P("  file %q: removed missing content", path)
			} else if newSize != node.Size {
				printer.P("  file %q: fixed incorrect size", path)
			}
			// no-ops if already correct
			node.Content = newContent
			node.Size = newSize
			return node
		},
		RewriteFailedTree: func(_ vaultic.ID, path string, _ error) (data.TreeNodeIterator, error) {
			if path == "/" {
				printer.P("  dir %q: not readable", path)
				// remove snapshots with invalid root node
				return nil, nil
			}
			// If a subtree fails to load, remove it
			printer.P("  dir %q: replaced with empty directory", path)
			return slices.Values([]data.NodeOrError{}), nil
		},
		AllowUnstableSerialization: true,
	})
}
