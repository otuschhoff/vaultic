package indexcmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/otuschhoff/vaultic/internal/global"
	indexhistory "github.com/otuschhoff/vaultic/internal/index/history"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type indexFileHistoryOptions struct {
	Daemon    indexDaemonOptions
	FSID      uint32
	Since     uint64
	Until     uint64
	Follow    bool
	Inode     string
	Snapshots bool
	Content   bool
	Verify    bool
}

func newIndexFileHistoryCommand(globalOptions *global.Options) *cobra.Command {
	var options indexFileHistoryOptions
	command := &cobra.Command{
		Use:   "file-history <path>",
		Short: "Report historical path or inode revisions",
		Long: "Resolve path history through immutable directory revisions in commit order, or scan one inode's revision prefix. " +
			"Legacy repositories fail explicitly rather than answering from current state." + indexExitStatus,
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, args []string) error {
			result, err := runIndexFileHistory(command.Context(), options, args[0], *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().Uint32Var(&options.FSID, "fsid", 0, "filesystem id for inode queries")
	command.Flags().Uint64Var(&options.Since, "since", 0, "first commit sequence to include")
	command.Flags().Uint64Var(&options.Until, "until", 0, "first commit sequence to exclude")
	command.Flags().BoolVar(&options.Follow, "follow", false, "continue across renames (requires the Phase 14 path index)")
	command.Flags().StringVar(&options.Inode, "inode", "", "query an inode as <fsid>:<inode> instead of resolving a path")
	command.Flags().BoolVar(&options.Snapshots, "snapshots", false, "annotate each change with the snapshots that captured it")
	command.Flags().BoolVar(&options.Content, "content", false, "report content identity changes (requires the Phase 14 path index)")
	command.Flags().BoolVar(&options.Verify, "verify", false, "verify a path index result against the immutable walk (requires the Phase 14 path index)")
	return command
}

func runIndexFileHistory(ctx context.Context, options indexFileHistoryOptions, target string, globalOptions global.Options, term ui.Terminal) (any, error) {
	if options.Follow {
		return nil, fmt.Errorf("--follow requires the Phase 14 path index follow implementation")
	}
	store, printer, cleanup, err := openHistoryStore(ctx, options.Daemon, globalOptions, term)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	if options.Inode != "" {
		fsid, inode, err := parseInodeSelector(options.Inode, options.FSID)
		if err != nil {
			return nil, err
		}
		result, err := indexhistory.InodeHistory(ctx, store, fsid, inode, options.Since, options.Until)
		if err == nil && !globalOptions.JSON {
			printer.P("inode %d:%d revisions: %d\n", fsid, inode, len(result.Revisions))
			for _, revision := range result.Revisions {
				printer.P("  revision %d freshness=%s size=%d\n", revision.Revision, revision.Freshness, revision.Size)
			}
		}
		return result, err
	}
	queryOptions := indexhistory.Options{SinceCommit: options.Since, UntilCommit: options.Until, Snapshots: options.Snapshots, Content: options.Content}
	walk, err := indexhistory.FileHistory(ctx, store, target, queryOptions)
	if err != nil {
		return nil, err
	}
	result := walk
	if indexed, ok, indexErr := indexhistory.FileHistoryFromPathIndex(ctx, store, target, queryOptions); indexErr != nil {
		return nil, indexErr
	} else if ok {
		result = indexed
	}
	if options.Verify {
		if result.Source != "path-index" {
			return nil, fmt.Errorf("--verify requires pv path-index entries for %q", target)
		}
		for _, change := range result.Changes {
			if change.Kind == "bound" && !change.Present {
				return nil, fmt.Errorf("path-index disagreement at commit %d: binding is not reachable from the snapshot root", change.Commit)
			}
		}
		if len(result.Changes) != len(walk.Changes) {
			return nil, fmt.Errorf("path-index disagreement: %d indexed changes, %d walked changes", len(result.Changes), len(walk.Changes))
		}
	}
	if !globalOptions.JSON {
		printer.P("path %s changes: %d\n", result.Path, len(result.Changes))
		for _, change := range result.Changes {
			printer.P(
				"  commit %d %s inode=%d revision=%d present=%v covered=%v\n",
				change.Commit,
				change.Kind,
				change.Inode,
				change.Revision,
				change.Present,
				change.Covered,
			)
		}
	}
	return result, err
}

type indexPathAtOptions struct {
	Daemon   indexDaemonOptions
	Snapshot string
}

func newIndexPathAtCommand(globalOptions *global.Options) *cobra.Command {
	var options indexPathAtOptions
	command := &cobra.Command{
		Use:   "path-at <path>",
		Short: "Resolve one path in one historical snapshot",
		Long: "Resolve a path by walking immutable directory revisions from a snapshot root and report the directory revision chain used." +
			indexExitStatus,
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, args []string) error {
			result, err := runIndexPathAt(command.Context(), options, args[0], *globalOptions, globalOptions.Term)
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			}
			return err
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringVar(&options.Snapshot, "snapshot", "", "snapshot id to resolve within")
	_ = command.MarkFlagRequired("snapshot")
	return command
}

func runIndexPathAt(
	ctx context.Context,
	options indexPathAtOptions,
	target string,
	globalOptions global.Options,
	term ui.Terminal,
) (indexhistory.PathAtResult, error) {
	var result indexhistory.PathAtResult
	store, printer, cleanup, err := openHistoryStore(ctx, options.Daemon, globalOptions, term)
	if err != nil {
		return result, err
	}
	defer cleanup()
	result, err = indexhistory.PathAt(ctx, store, target, options.Snapshot)
	if err == nil && !globalOptions.JSON {
		printer.P(
			"%s at %s: present=%v covered=%v inode=%d revision=%d type=%s\n",
			result.Path,
			result.SnapshotID,
			result.Present,
			result.Covered,
			result.Inode,
			result.Revision,
			result.NodeType,
		)
		for _, key := range result.DirectoryChain {
			printer.P("  %s\n", key)
		}
	}
	return result, err
}

func openHistoryStore(
	ctx context.Context,
	daemonOptions indexDaemonOptions,
	globalOptions global.Options,
	term ui.Terminal,
) (indexhistory.Store, vaultic.Printer, func(), error) {
	config, err := daemonOptions.config("")
	if err != nil {
		return nil, nil, nil, err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := requireSlateDBRepository(repo); err != nil {
		unlock()
		return nil, nil, nil, err
	}
	store, _, closeStore, err := openIndexStore(ctx, repo, daemonOptions)
	if err != nil {
		unlock()
		return nil, nil, nil, err
	}
	cleanup := func() {
		closeStore()
		unlock()
	}
	return store, printer, cleanup, nil
}

func parseInodeSelector(selector string, fsidFlag uint32) (uint32, uint64, error) {
	parts := strings.Split(selector, ":")
	if len(parts) == 1 {
		inode, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || fsidFlag == 0 {
			return 0, 0, fmt.Errorf("--inode must be <fsid>:<inode> or paired with --fsid")
		}
		return fsidFlag, inode, err
	}
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("--inode must be <fsid>:<inode>")
	}
	fsid, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid inode fsid: %w", err)
	}
	inode, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid inode number: %w", err)
	}
	return uint32(fsid), inode, nil
}
