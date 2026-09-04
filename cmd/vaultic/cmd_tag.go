package main

import (
	"context"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func newTagCommand(globalOptions *global.Options) *cobra.Command {
	var opts TagOptions

	cmd := &cobra.Command{
		Use:   "tag [flags] [snapshotID ...]",
		Short: "Modify tags on snapshots",
		Long: `
The "tag" command allows you to modify tags on existing snapshots.

You can either set/replace the entire set of tags on a snapshot, or
add tags to/remove tags from the existing set.

When no snapshotID is given, all snapshots matching the host, tag and path filter criteria are modified.

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
			finalizeSnapshotFilter(&opts.SnapshotFilter)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTag(cmd.Context(), opts, *globalOptions, globalOptions.Term, args)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// TagOptions bundles all options for the 'tag' command.
type TagOptions struct {
	data.SnapshotFilter
	SetTags    data.TagLists
	AddTags    data.TagLists
	RemoveTags data.TagLists

	// label/description/delete editing (vaultic/rustic snapshot metadata)
	SetLabel       string
	SetDescription string
	SetDelete      string // "", "never", "none", or a duration

	setLabelFlag, setDescriptionFlag, setDeleteFlag *pflag.Flag
}

func (opts *TagOptions) AddFlags(f *pflag.FlagSet) {
	f.Var(&opts.SetTags, "set", "`tags` which will replace the existing tags in the format `tag[,tag,...]` (can be given multiple times)")
	f.Var(&opts.AddTags, "add", "`tags` which will be added to the existing tags in the format `tag[,tag,...]` (can be given multiple times)")
	f.Var(&opts.RemoveTags, "remove", "`tags` which will be removed from the existing tags in the format `tag[,tag,...]` (can be given multiple times)")
	f.StringVar(&opts.SetLabel, "set-label", "", "set the snapshot `label` (use empty string to clear)")
	f.StringVar(&opts.SetDescription, "set-description", "", "set the snapshot `description` (use empty string to clear)")
	f.StringVar(&opts.SetDelete, "set-delete", "", "set delete protection: `never`, a `duration` (e.g. 10d) or 'none' to clear")
	opts.setLabelFlag = f.Lookup("set-label")
	opts.setDescriptionFlag = f.Lookup("set-description")
	opts.setDeleteFlag = f.Lookup("set-delete")
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, true)
}

type changedSnapshot struct {
	MessageType   string     `json:"message_type"` // changed
	OldSnapshotID vaultic.ID `json:"old_snapshot_id"`
	NewSnapshotID vaultic.ID `json:"new_snapshot_id"`
}

type changedSnapshotsSummary struct {
	MessageType      string `json:"message_type"` // summary
	ChangedSnapshots int    `json:"changed_snapshots"`
}

func changeSnapshotMeta(
	ctx context.Context,
	repo *repository.Repository,
	sn *data.Snapshot,
	opts TagOptions,
	now time.Time,
	printFunc func(changedSnapshot),
) (bool, error) {
	var changed bool

	setTags, addTags, removeTags := opts.SetTags.Flatten(), opts.AddTags.Flatten(), opts.RemoveTags.Flatten()
	if len(setTags) != 0 {
		// Setting the tag to an empty string really means no tags.
		if len(setTags) == 1 && setTags[0] == "" {
			setTags = nil
		}
		sn.Tags = setTags
		changed = true
	} else {
		changed = sn.AddTags(addTags)
		if sn.RemoveTags(removeTags) {
			changed = true
		}
	}

	if opts.setLabelFlag != nil && opts.setLabelFlag.Changed {
		sn.Label = opts.SetLabel
		changed = true
	}
	if opts.setDescriptionFlag != nil && opts.setDescriptionFlag.Changed {
		sn.Description = opts.SetDescription
		changed = true
	}
	if opts.setDeleteFlag != nil && opts.setDeleteFlag.Changed {
		switch strings.ToLower(opts.SetDelete) {
		case "none", "":
			sn.Delete = nil
		case "never":
			sn.Delete = &data.DeleteOption{Never: true}
		default:
			dur, err := data.ParseDuration(opts.SetDelete)
			if err != nil || dur.Zero() {
				return false, errors.Errorf("invalid --set-delete value %q (use 'never', 'none' or a duration)", opts.SetDelete)
			}
			until := now.AddDate(dur.Years, dur.Months, dur.Days).Add(time.Duration(dur.Hours) * time.Hour)
			sn.Delete = &data.DeleteOption{After: &until}
		}
		changed = true
	}

	if changed {
		// Retain the original snapshot id over all changes.
		if sn.Original == nil {
			sn.Original = sn.ID()
		}

		// Save the new snapshot.
		id, err := data.SaveSnapshot(ctx, repo, sn)
		if err != nil {
			return false, err
		}

		debug.Log("old snapshot %v saved as a new snapshot %v", sn.ID(), id)

		// Remove the old snapshot.
		if err = repo.RemoveUnpacked(ctx, vaultic.WriteableSnapshotFile, *sn.ID()); err != nil {
			return false, err
		}

		debug.Log("old snapshot %v removed", sn.ID())

		printFunc(changedSnapshot{MessageType: "changed", OldSnapshotID: *sn.ID(), NewSnapshotID: id})
	}
	return changed, nil
}

func runTag(ctx context.Context, opts TagOptions, gopts global.Options, term ui.Terminal, args []string) error {
	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)

	tagsChanged := len(opts.SetTags) != 0 || len(opts.AddTags) != 0 || len(opts.RemoveTags) != 0
	metaChanged := (opts.setLabelFlag != nil && opts.setLabelFlag.Changed) ||
		(opts.setDescriptionFlag != nil && opts.setDescriptionFlag.Changed) ||
		(opts.setDeleteFlag != nil && opts.setDeleteFlag.Changed)
	if !tagsChanged && !metaChanged {
		return errors.Fatal("nothing to do!")
	}
	if len(opts.SetTags) != 0 && (len(opts.AddTags) != 0 || len(opts.RemoveTags) != 0) {
		return errors.Fatal("--set and --add/--remove cannot be given at the same time")
	}

	printer.P("create exclusive lock for repository")
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	printFunc := func(c changedSnapshot) {
		printer.V("old snapshot ID: %v -> new snapshot ID: %v", c.OldSnapshotID, c.NewSnapshotID)
	}

	summary := changedSnapshotsSummary{MessageType: "summary", ChangedSnapshots: 0}
	printSummary := func(c changedSnapshotsSummary) {
		if c.ChangedSnapshots == 0 {
			printer.P("no snapshots were modified")
		} else {
			printer.P("modified %v snapshots", c.ChangedSnapshots)
		}
	}

	if gopts.JSON {
		printFunc = func(c changedSnapshot) {
			term.Print(ui.ToJSONString(c))
		}
		printSummary = func(c changedSnapshotsSummary) {
			term.Print(ui.ToJSONString(c))
		}
	}

	now := time.Now()
	err = opts.SnapshotFilter.FindAll(ctx, repo, repo, args, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		changed, err := changeSnapshotMeta(ctx, repo, sn, opts, now, printFunc)
		if err != nil {
			printer.E("unable to modify snapshot ID %q, ignoring: %v", sn.ID(), err)
			return nil
		}
		if changed {
			summary.ChangedSnapshots++
		}
		return nil
	})
	if err != nil {
		return err
	}

	printSummary(summary)

	return nil
}
