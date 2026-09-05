//go:build debug

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func registerDebugCommand(cmd *cobra.Command, globalOptions *global.Options) {
	cmd.AddCommand(
		newDebugCommand(globalOptions),
	)
}

func newDebugCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "debug",
		Short:             "Debug commands",
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
	}
	cmd.AddCommand(newDebugDumpCommand(globalOptions))
	cmd.AddCommand(newDebugExamineCommand(globalOptions))
	return cmd
}

func newDebugDumpCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump [indexes|snapshots|all|packs]",
		Short: "Dump data structures",
		Long: `
The "dump" command dumps data structures from the repository as JSON objects. It
is used for debugging purposes only.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDebugDump(cmd.Context(), *globalOptions, args, globalOptions.Term)
		},
	}
	return cmd
}

func newDebugExamineCommand(globalOptions *global.Options) *cobra.Command {
	var options debugExamineOptions

	cmd := &cobra.Command{
		Use:               "examine pack-ID...",
		Short:             "Examine a pack file",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDebugExamine(cmd.Context(), *globalOptions, options, args, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

type debugExamineOptions struct {
	TryRepair     bool
	RepairByte    bool
	ExtractPack   bool
	ReuploadBlobs bool
}

func (options *debugExamineOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVar(&options.ExtractPack, "extract-pack", false, "write blobs to the current directory")
	f.BoolVar(&options.ReuploadBlobs, "reupload-blobs", false, "reupload blobs to the repository")
	f.BoolVar(&options.TryRepair, "try-repair", false, "try to repair broken blobs with single bit flips")
	f.BoolVar(&options.RepairByte, "repair-byte", false, "try to repair broken blobs by trying bytes")
}

func prettyPrintJSON(wr io.Writer, item interface{}) error {
	buf, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}

	_, err = wr.Write(append(buf, '\n'))
	return err
}

func debugPrintSnapshots(ctx context.Context, repo *repository.Repository, wr io.Writer) error {
	return data.ForAllSnapshots(ctx, repo, repo, nil, func(id vaultic.ID, snapshot *data.Snapshot, err error) error {
		if err != nil {
			return err
		}

		if _, err := fmt.Fprintf(wr, "snapshot_id: %v\n", id); err != nil {
			return err
		}

		return prettyPrintJSON(wr, snapshot)
	})
}

func runDebugDump(ctx context.Context, globalOptions global.Options, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	if len(args) != 1 {
		return errors.Fatal("type not specified")
	}

	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	tpe := args[0]

	switch tpe {
	case "indexes":
		return repository.DumpIndexes(ctx, repo, globalOptions.Term.OutputWriter(), printer)
	case "snapshots":
		return debugPrintSnapshots(ctx, repo, globalOptions.Term.OutputWriter())
	case "packs":
		return repository.DumpPacks(ctx, repo, globalOptions.Term.OutputWriter(), printer)
	case "all":
		printer.S("snapshots:")
		err := debugPrintSnapshots(ctx, repo, globalOptions.Term.OutputWriter())
		if err != nil {
			return err
		}

		printer.S("indexes:")
		err = repository.DumpIndexes(ctx, repo, globalOptions.Term.OutputWriter(), printer)
		if err != nil {
			return err
		}

		return nil
	default:
		return errors.Fatalf("no such type %q", tpe)
	}
}

func runDebugExamine(ctx context.Context, globalOptions global.Options, options debugExamineOptions, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	if options.ExtractPack && globalOptions.NoLock {
		return fmt.Errorf("--extract-pack and --no-lock are mutually exclusive")
	}

	ctx, repo, unlock, err := openWithAppendLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	ids := make([]vaultic.ID, 0)
	for _, name := range args {
		id, err := vaultic.ParseID(name)
		if err != nil {
			id, err = vaultic.Find(ctx, repo, vaultic.PackFile, name)
			if err != nil {
				printer.E("error: %v", err)
				continue
			}
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return errors.Fatal("no pack files to examine")
	}

	err = repo.LoadIndex(ctx, printer)
	if err != nil {
		return err
	}

	examineOpts := repository.ExaminePackOptions{
		TryRepair:     options.TryRepair,
		RepairByte:    options.RepairByte,
		ExtractPack:   options.ExtractPack,
		ReuploadBlobs: options.ReuploadBlobs,
	}
	for _, id := range ids {
		err := repository.ExaminePack(ctx, repo, id, examineOpts, printer)
		if err != nil {
			printer.E("error: %v", err)
		}
		if err == context.Canceled {
			break
		}
	}
	return nil
}
