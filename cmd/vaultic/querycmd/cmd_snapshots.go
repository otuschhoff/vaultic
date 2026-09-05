package querycmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/ui/table"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func NewSnapshotsCommand(globalOptions *global.Options) *cobra.Command {
	var options snapshotOptions

	cmd := &cobra.Command{
		Use:   "snapshots [flags] [snapshotID ...]",
		Short: "List all snapshots",
		Long: `
The "snapshots" command lists all snapshots stored in the repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           "default",
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			finalizeSnapshotFilter(&options.SnapshotFilter)
			return options.Finalize()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSnapshots(cmd.Context(), options, *globalOptions, args, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

// snapshotOptions bundles all options for the snapshots command.
type snapshotOptions struct {
	data.SnapshotFilter
	Compact bool
	last    bool // Deprecated in favour of Latest.
	Latest  int
	GroupBy data.SnapshotGroupByOptions
}

func (options *snapshotOptions) AddFlags(f *pflag.FlagSet) {
	initMultiSnapshotFilter(f, &options.SnapshotFilter, true)
	f.BoolVarP(&options.Compact, "compact", "c", false, "use compact output format")
	f.BoolVar(&options.last, "last", false, "only show the last snapshot for each host and path")
	err := f.MarkDeprecated("last", "use --latest 1")
	if err != nil {
		// MarkDeprecated only returns an error when the flag is not found
		// Flag registration is a construction-time invariant.
		panic(err) //nolint:forbidigo // A missing flag here is a command-construction defect.
	}
	f.IntVar(&options.Latest, "latest", 0, "only show the last `n` snapshots for each host and path")
	f.VarP(&options.GroupBy, "group-by", "g", "`group` snapshots by host, paths and/or tags, separated by comma")
}

func (options *snapshotOptions) Finalize() error {
	if options.last && options.Latest == 0 {
		options.Latest = 1
	}
	return nil
}

func runSnapshots(ctx context.Context, options snapshotOptions, globalOptions global.Options, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	var snapshots data.Snapshots
	err = options.SnapshotFilter.FindAll(ctx, repo, repo, args, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		snapshots = append(snapshots, sn)
		return nil
	})
	if err != nil {
		return err
	}

	snapshotGroups, grouped, err := data.GroupSnapshots(snapshots, options.GroupBy)
	if err != nil {
		return err
	}

	for k, list := range snapshotGroups {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if options.Latest > 0 {
			if grouped {
				list = filterLatestSnapshotsInGroup(list, options.Latest)
			} else {
				list = filterLatestSnapshots(list, options.Latest)
			}
		}
		sort.Sort(sort.Reverse(list))
		snapshotGroups[k] = list
	}

	if globalOptions.JSON {
		err := printSnapshotGroupJSON(globalOptions.Term.OutputWriter(), snapshotGroups, grouped)
		if err != nil {
			printer.E("error printing snapshots: %v", err)
		}
		return nil
	}

	for k, list := range snapshotGroups {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if grouped {
			err := PrintSnapshotGroupHeader(globalOptions.Term.OutputWriter(), k)
			if err != nil {
				return err
			}
		}
		err := PrintSnapshots(globalOptions.Term.OutputWriter(), list, nil, options.Compact)
		if err != nil {
			return err
		}
	}

	return nil
}

// filterLastSnapshotsKey is used by FilterLastSnapshots.
type filterLastSnapshotsKey struct {
	Hostname    string
	JoinedPaths string
}

// newFilterLastSnapshotsKey initializes a filterLastSnapshotsKey from a Snapshot
func newFilterLastSnapshotsKey(sn *data.Snapshot) filterLastSnapshotsKey {
	// Shallow slice copy
	var paths = make([]string, len(sn.Paths))
	copy(paths, sn.Paths)
	sort.Strings(paths)
	return filterLastSnapshotsKey{sn.Hostname, strings.Join(paths, "|")}
}

// filterLatestSnapshots filters a list of snapshots to only return
// the limit last entries for each hostname and path. If the snapshot
// contains multiple paths, they will be joined and treated as one
// item.
func filterLatestSnapshots(list data.Snapshots, limit int) data.Snapshots {
	// Sort the snapshots so that the newer ones are listed first
	sort.SliceStable(list, func(i, j int) bool {
		return list[i].Time.After(list[j].Time)
	})

	var results data.Snapshots
	seen := make(map[filterLastSnapshotsKey]int)
	for _, sn := range list {
		key := newFilterLastSnapshotsKey(sn)
		if seen[key] < limit {
			seen[key]++
			results = append(results, sn)
		}
	}
	return results
}

// filterLatestSnapshotsInGroup filters a list of snapshots to only return
// the `limit` last entries. It is assumed that the snapshot list only contains
// one group of snapshots.
func filterLatestSnapshotsInGroup(list data.Snapshots, limit int) data.Snapshots {
	// Sort the snapshots so that the newer ones are listed first
	sort.SliceStable(list, func(i, j int) bool {
		return list[i].Time.After(list[j].Time)
	})

	return list[:min(limit, len(list))]
}

type snapshotTableRow struct {
	ID        string
	Timestamp string
	Hostname  string
	Label     string
	Tags      []string
	Reasons   []string
	Paths     []string
	Size      string
}

// PrintSnapshots prints a text table of the snapshots in list to stdout.
func PrintSnapshots(stdout io.Writer, list data.Snapshots, reasons []data.KeepReason, compact bool) error {
	// keep the reasons a snasphot is being kept in a map, so that it doesn't
	// get lost when the list of snapshots is sorted
	keepReasons := make(map[vaultic.ID]data.KeepReason, len(reasons))
	if len(reasons) > 0 {
		for i, sn := range list {
			id := sn.ID()
			keepReasons[*id] = reasons[i]
		}
	}
	hasSize, hasLabel := snapshotColumns(list)

	// always sort the snapshots so that the newer ones are listed last
	sort.SliceStable(list, func(i, j int) bool {
		return list[i].Time.Before(list[j].Time)
	})

	tab := table.New()
	addSnapshotColumns(tab, compact, hasSize, hasLabel, len(reasons) > 0)

	var multiline bool
	for _, sn := range list {
		row := snapshotTableRow{
			ID:        sn.ID().Str(),
			Timestamp: sn.Time.Local().Format(global.TimeFormat),
			Hostname:  sn.Hostname,
			Label:     sn.Label,
			Tags:      sn.Tags,
			Paths:     sn.Paths,
		}

		if len(reasons) > 0 {
			id := sn.ID()
			row.Reasons = keepReasons[*id].Matches
		}

		if len(sn.Paths) > 1 && !compact {
			multiline = true
		}

		if sn.Summary != nil {
			row.Size = ui.FormatBytes(sn.Summary.TotalBytesProcessed)
		}

		tab.AddRow(row)
	}

	// Add timezone information to prevent confusion:
	// Each snapshot can be registered in different timezones,
	// but we display them all in local timezone on this output.
	footer := fmt.Sprintf("%d snapshots", len(list))
	zoneName := time.Now().Local().Location().String()
	if zoneName == "Local" {
		zoneName = "local time"
	}
	tab.AddFooter(fmt.Sprintf("Timestamps shown in %s\n%s", zoneName, footer))

	if multiline {
		// print an additional blank line between snapshots

		var last int
		tab.PrintData = func(w io.Writer, idx int, s string) error {
			var err error
			if idx == last {
				_, err = fmt.Fprintf(w, "%s\n", s)
			} else {
				_, err = fmt.Fprintf(w, "\n%s\n", s)
			}
			last = idx
			return err
		}
	}

	return tab.Write(stdout)
}

func snapshotColumns(list data.Snapshots) (hasSize, hasLabel bool) {
	for _, snapshot := range list {
		hasSize = hasSize || snapshot.Summary != nil
		hasLabel = hasLabel || snapshot.Label != ""
	}
	return hasSize, hasLabel
}

func addSnapshotColumns(tab *table.Table, compact, hasSize, hasLabel, hasReasons bool) {
	tab.AddColumn("ID", "{{ .ID }}")
	tab.AddColumn("Time", "{{ .Timestamp }}")
	if compact {
		tab.AddColumn("Host", "{{ .Hostname }}")
		tab.AddColumn("Tags  ", `{{ join .Tags "\n" }}`)
		if hasSize {
			tab.AddColumn("Size", `{{ .Size }}`)
		}
		return
	}
	tab.AddColumn("Host      ", "{{ .Hostname }}")
	if hasLabel {
		tab.AddColumn("Label     ", "{{ .Label }}")
	}
	tab.AddColumn("Tags      ", `{{ join .Tags "," }}`)
	if hasReasons {
		tab.AddColumn("Reasons", `{{ join .Reasons "\n" }}`)
	}
	tab.AddColumn("Paths", `{{ join .Paths "\n" }}`)
	if hasSize {
		tab.AddColumn("Size", `{{ .Size }}`)
	}
}

// PrintSnapshotGroupHeader prints which group of the group-by option the
// following snapshots belong to.
// Prints nothing, if we did not group at all.
func PrintSnapshotGroupHeader(stdout io.Writer, groupKeyJSON string) error {
	var key data.SnapshotGroupKey

	err := json.Unmarshal([]byte(groupKeyJSON), &key)
	if err != nil {
		return err
	}

	if key.Hostname == "" && key.Tags == nil && key.Paths == nil {
		return nil
	}

	// Info
	header := "snapshots"
	var infoStrings []string
	if key.Hostname != "" {
		infoStrings = append(infoStrings, "host ["+key.Hostname+"]")
	}
	if key.Tags != nil {
		infoStrings = append(infoStrings, "tags ["+strings.Join(key.Tags, ", ")+"]")
	}
	if key.Paths != nil {
		infoStrings = append(infoStrings, "paths ["+strings.Join(key.Paths, ", ")+"]")
	}
	if infoStrings != nil {
		header += " for (" + strings.Join(infoStrings, ", ") + ")"
	}
	header += ":\n"
	_, err = stdout.Write([]byte(header))
	return err
}

// Snapshot helps to print Snapshots as JSON with their ID included.
type Snapshot struct {
	*data.Snapshot

	ID      *vaultic.ID `json:"id"`
	ShortID string      `json:"short_id"` // deprecated
}

// SnapshotGroup helps to print SnapshotGroups as JSON with their GroupReasons included.
type SnapshotGroup struct {
	GroupKey  data.SnapshotGroupKey `json:"group_key"`
	Snapshots []Snapshot            `json:"snapshots"`
}

// printSnapshotGroupJSON writes the JSON representation of list to stdout.
func printSnapshotGroupJSON(stdout io.Writer, snGroups map[string]data.Snapshots, grouped bool) error {
	if grouped {
		snapshotGroups := []SnapshotGroup{}

		for k, list := range snGroups {
			var key data.SnapshotGroupKey
			var err error
			var snapshots []Snapshot

			err = json.Unmarshal([]byte(k), &key)
			if err != nil {
				return err
			}

			for _, sn := range list {
				k := Snapshot{
					Snapshot: sn,
					ID:       sn.ID(),
					ShortID:  sn.ID().Str(),
				}
				snapshots = append(snapshots, k)
			}

			group := SnapshotGroup{
				GroupKey:  key,
				Snapshots: snapshots,
			}
			snapshotGroups = append(snapshotGroups, group)
		}

		return json.NewEncoder(stdout).Encode(snapshotGroups)
	}

	// Old behavior
	snapshots := []Snapshot{}

	for _, list := range snGroups {
		for _, sn := range list {
			k := Snapshot{
				Snapshot: sn,
				ID:       sn.ID(),
				ShortID:  sn.ID().Str(),
			}
			snapshots = append(snapshots, k)
		}
	}

	return json.NewEncoder(stdout).Encode(snapshots)
}
