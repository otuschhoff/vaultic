package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var catAllowedCmds = []string{"config", "index", "snapshot", "key", "masterkey", "lock", "pack", "blob", "tree"}

func newCatCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat [flags] [masterkey|config|pack ID|blob ID|snapshot ID|index ID|key ID|lock ID|tree snapshot:subfolder]",
		Short: "Print internal objects to stdout",
		Long: `
The "cat" command is used to print internal objects to stdout.

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
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCat(cmd.Context(), *globalOptions, args, globalOptions.Term)
		},
		ValidArgs: catAllowedCmds,
	}
	return cmd
}

func validateCatArgs(args []string) error {
	if len(args) < 1 {
		return errors.Fatal("type not specified")
	}

	validType := slices.Contains(catAllowedCmds, args[0])
	if !validType {
		return errors.Fatalf("invalid type %q, must be one of [%s]", args[0], strings.Join(catAllowedCmds, "|"))
	}

	if args[0] != "masterkey" && args[0] != "config" && len(args) != 2 {
		return errors.Fatal("ID not specified")
	}

	return nil
}

func runCat(ctx context.Context, globalOptions global.Options, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term)

	if err := validateCatArgs(args); err != nil {
		return err
	}

	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	tpe := args[0]

	var id vaultic.ID
	if tpe != "masterkey" && tpe != "config" && tpe != "snapshot" && tpe != "tree" {
		id, err = vaultic.ParseID(args[1])
		if err != nil {
			return errors.Fatalf("unable to parse ID: %v", err)
		}
	}

	return catObject(ctx, repo, tpe, args, id, printer, term)
}

func catObject(
	ctx context.Context,
	repo *repository.Repository,
	objectType string,
	args []string,
	id vaultic.ID,
	printer vaultic.Printer,
	term ui.Terminal,
) error {
	switch objectType {
	case "config":
		buf, err := json.MarshalIndent(repo.Config(), "", "  ")
		if err != nil {
			return err
		}

		printer.S(string(buf))
		return nil
	case "index":
		buf, err := repo.LoadUnpacked(ctx, vaultic.IndexFile, id)
		if err != nil {
			return err
		}

		printer.S(string(buf))
		return nil
	case "snapshot":
		sn, _, err := data.FindSnapshot(ctx, repo, repo, args[1])
		if err != nil {
			return errors.Fatalf("could not find snapshot: %v", err)
		}

		buf, err := json.MarshalIndent(sn, "", "  ")
		if err != nil {
			return err
		}

		printer.S(string(buf))
		return nil
	case "key":
		key, err := repository.LoadKey(ctx, repo, id)
		if err != nil {
			return err
		}

		buf, err := json.MarshalIndent(&key, "", "  ")
		if err != nil {
			return err
		}

		printer.S(string(buf))
		return nil
	case "masterkey":
		buf, err := json.MarshalIndent(repo.Key(), "", "  ")
		if err != nil {
			return err
		}

		printer.S(string(buf))
		return nil
	case "lock":
		lock, err := repository.LoadLock(ctx, repo, id)
		if err != nil {
			return err
		}

		buf, err := json.MarshalIndent(&lock, "", "  ")
		if err != nil {
			return err
		}

		printer.S(string(buf))
		return nil

	case "pack", "blob", "tree":
		return catRawObject(ctx, repo, objectType, args, id, printer, term)

	default:
		return errors.Fatal("invalid type")
	}
}

func catRawObject(
	ctx context.Context,
	repo *repository.Repository,
	objectType string,
	args []string,
	id vaultic.ID,
	printer vaultic.Printer,
	term ui.Terminal,
) error {
	switch objectType {
	case "pack":
		buf, err := repo.LoadRaw(ctx, vaultic.PackFile, id)
		if buf == nil {
			return err
		}
		hash := vaultic.Hash(buf)
		if !hash.Equal(id) {
			printer.E("Warning: hash of data does not match ID, want\n  %v\ngot:\n  %v", id.String(), hash.String())
		}
		_, err = term.OutputRaw().Write(buf)
		return err
	case "blob":
		if err := repo.LoadIndex(ctx, printer); err != nil {
			return err
		}
		for _, blobType := range []vaultic.BlobType{vaultic.DataBlob, vaultic.TreeBlob} {
			if _, ok := repo.LookupBlobSize(vaultic.BlobHandle{Type: blobType, ID: id}); !ok {
				continue
			}
			buf, err := repo.LoadBlob(ctx, vaultic.BlobHandle{Type: blobType, ID: id}, nil)
			if err != nil {
				return err
			}
			_, err = term.OutputRaw().Write(buf)
			return err
		}
		return errors.Fatal("blob not found")
	case "tree":
		snapshot, subfolder, err := data.FindSnapshot(ctx, repo, repo, args[1])
		if err != nil {
			return errors.Fatalf("could not find snapshot: %v", err)
		}
		if err := repo.LoadIndex(ctx, printer); err != nil {
			return err
		}
		snapshot.Tree, err = data.FindTreeDirectory(ctx, repo, snapshot.Tree, subfolder)
		if err != nil {
			return err
		}
		buf, err := repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: *snapshot.Tree}, nil)
		if err != nil {
			return err
		}
		_, err = term.OutputRaw().Write(buf)
		return err
	default:
		return errors.Fatal("invalid type")
	}
}
