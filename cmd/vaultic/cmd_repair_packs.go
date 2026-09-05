package main

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
)

func newRepairPacksCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "packs [packIDs...]",
		Short: "Salvage damaged pack files",
		Long: `
The "repair packs" command extracts intact blobs from the specified pack files, rebuilds
the index to remove the damaged pack files and removes the pack files from the repository.

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
			return runRepairPacks(cmd.Context(), *globalOptions, globalOptions.Term, args)
		},
	}
	return cmd
}

func runRepairPacks(ctx context.Context, globalOptions global.Options, term ui.Terminal, args []string) error {
	ids := vaultic.NewIDSet()
	for _, arg := range args {
		id, err := vaultic.ParseID(arg)
		if err != nil {
			return err
		}
		ids.Insert(id)
	}
	if len(ids) == 0 {
		return errors.Fatal("no ids specified")
	}

	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if err := requireLegacyMetadataMutation(repo, "repair packs"); err != nil {
		return err
	}

	err = repo.LoadIndex(ctx, printer)
	if err != nil {
		return errors.Fatalf("%s", err)
	}

	printer.P("saving backup copies of pack files to current folder")
	for id := range ids {
		buf, err := repo.LoadRaw(ctx, vaultic.PackFile, id)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// only skip creating a local copy if no data at all could be loaded
		if err != nil && buf == nil {
			printer.E("will remove packfile %v due to failed download: %v", id, err)
			continue
		}

		f, err := os.OpenFile("pack-"+id.String(), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, bytes.NewReader(buf)); err != nil {
			errors.CloseQuietly(f)
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}

	err = repository.RepairPacks(ctx, repo, ids, printer)
	if err != nil {
		return errors.Fatalf("%s", err)
	}

	printer.E("\nUse `vaultic repair snapshots --forget` to remove the corrupted data blobs from all snapshots")
	return nil
}
