package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
)

func newRepoInfoCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "repoinfo",
		Short:             "Show repository object counts and sizes",
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRepoInfo(cmd.Context(), *globalOptions, globalOptions.Term)
		},
	}
	return cmd
}

type repoInfoType struct {
	Count uint64 `json:"count"`
	Size  uint64 `json:"size"`
}

type repoInfo struct {
	Types map[string]repoInfoType `json:"types"`
	Total repoInfoType            `json:"total"`
}

func runRepoInfo(ctx context.Context, globalOptions global.Options, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	info := repoInfo{Types: make(map[string]repoInfoType)}
	for _, tpe := range []vaultic.FileType{vaultic.PackFile, vaultic.KeyFile, vaultic.SnapshotFile, vaultic.IndexFile} {
		var entry repoInfoType
		if err := repo.List(ctx, tpe, func(_ vaultic.ID, size int64) error {
			entry.Count++
			if size > 0 {
				entry.Size += uint64(size)
			}
			return nil
		}); err != nil {
			return err
		}
		info.Types[tpe.String()] = entry
		info.Total.Count += entry.Count
		info.Total.Size += entry.Size
	}

	if globalOptions.JSON {
		return json.NewEncoder(term.OutputWriter()).Encode(info)
	}
	for _, name := range []string{"data", "key", "snapshot", "index"} {
		entry := info.Types[name]
		printer.S("%-10s %8d objects  %s", name+":", entry.Count, ui.FormatBytes(entry.Size))
	}
	printer.S("%-10s %8d objects  %s", "total:", info.Total.Count, ui.FormatBytes(info.Total.Size))
	return nil
}

func (i repoInfo) String() string { return fmt.Sprintf("%d objects", i.Total.Count) }
