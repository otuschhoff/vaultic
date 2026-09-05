package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/otuschhoff/vaultic/cmd/vaultic/querycmd"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type Snapshot = querycmd.Snapshot

func newForgetCommand(globalOptions *global.Options) *cobra.Command {
	var options forgetOptions
	var pruneOpts pruneOptions
	cmd := &cobra.Command{
		Use:   "forget [flags] [snapshot ID] [...]",
		Short: "Remove snapshots from the repository",
		Long: `
The "forget" command removes snapshots according to a policy. All snapshots are
first divided into groups according to "--group-by", and after that the policy
specified by the "--keep-*" options is applied to each group individually.
If there are not enough snapshots to keep one for each duration related
"--keep-{within-,}*" option, the oldest snapshot in the group is kept
additionally.
Please note that this command really only deletes the snapshot object in the
repository, which is a reference to data stored there. In order to remove the
unreferenced data after "forget" was run successfully, see the "prune" command.
Please also read the documentation for "forget" to learn about some important
security considerations.
EXIT STATUS
===========
Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 3 if there was an error removing one or more snapshots.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			finalizeSnapshotFilter(&options.SnapshotFilter)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runForget(cmd.Context(), options, pruneOpts, *globalOptions, globalOptions.Term, args)
		},
	}
	options.AddFlags(cmd.Flags())
	pruneOpts.AddLimitedFlags(cmd.Flags())
	return cmd
}

type ForgetPolicyCount int

var ErrNegativePolicyCount = errors.New("negative values not allowed, use 'unlimited' instead")
var ErrFailedToRemoveOneOrMoreSnapshots = errors.New("failed to remove one or more snapshots")

func (c *ForgetPolicyCount) Set(s string) error {
	switch s {
	case "unlimited":
		*c = -1
	default:
		val, err := strconv.ParseInt(s, 10, 0)
		if err != nil {
			return err
		}
		if val < 0 {
			return ErrNegativePolicyCount
		}
		*c = ForgetPolicyCount(val)
	}
	return nil
}

func (c *ForgetPolicyCount) String() string {
	switch *c {
	case -1:
		return "unlimited"
	default:
		return strconv.FormatInt(int64(*c), 10)
	}
}

func (c *ForgetPolicyCount) Type() string {
	return "n"
}

// forgetOptions collects all options for the forget command.
type forgetOptions struct {
	Last                ForgetPolicyCount
	Minutely            ForgetPolicyCount
	Hourly              ForgetPolicyCount
	Daily               ForgetPolicyCount
	Weekly              ForgetPolicyCount
	Monthly             ForgetPolicyCount
	QuarterYearly       ForgetPolicyCount
	HalfYearly          ForgetPolicyCount
	Yearly              ForgetPolicyCount
	Within              data.Duration
	WithinMinutely      data.Duration
	WithinHourly        data.Duration
	WithinDaily         data.Duration
	WithinWeekly        data.Duration
	WithinMonthly       data.Duration
	WithinQuarterYearly data.Duration
	WithinHalfYearly    data.Duration
	WithinYearly        data.Duration
	KeepTags            data.TagLists

	UnsafeAllowRemoveAll bool
	// OverrideDeleteProtection ignores delete protection (delete_never /
	// delete_after) markers on snapshots.
	OverrideDeleteProtection bool

	data.SnapshotFilter
	Compact bool

	// Grouping
	GroupBy data.SnapshotGroupByOptions
	DryRun  bool
	Prune   bool
}

type forgetPlan struct {
	remove    vaultic.IDSet
	groups    []*ForgetGroup
	protected int
	horizons  map[vaultic.ID]repository.SnapshotRetentionHorizon
}

func (options *forgetOptions) AddFlags(f *pflag.FlagSet) {
	f.VarP(&options.Last, "keep-last", "l", "keep the last `n` snapshots (use 'unlimited' to keep all snapshots)")
	f.Var(&options.Minutely, "keep-minutely", "keep the last `n` minutely snapshots")
	f.VarP(&options.Hourly, "keep-hourly", "H", "keep the last `n` hourly snapshots (use 'unlimited' to keep all hourly snapshots)")
	f.VarP(&options.Daily, "keep-daily", "d", "keep the last `n` daily snapshots (use 'unlimited' to keep all daily snapshots)")
	f.VarP(&options.Weekly, "keep-weekly", "w", "keep the last `n` weekly snapshots (use 'unlimited' to keep all weekly snapshots)")
	f.VarP(&options.Monthly, "keep-monthly", "m", "keep the last `n` monthly snapshots (use 'unlimited' to keep all monthly snapshots)")
	f.Var(&options.QuarterYearly, "keep-quarter-yearly", "keep the last `n` quarterly snapshots")
	f.Var(&options.HalfYearly, "keep-half-yearly", "keep the last `n` half-yearly snapshots")
	f.VarP(&options.Yearly, "keep-yearly", "y", "keep the last `n` yearly snapshots (use 'unlimited' to keep all yearly snapshots)")
	f.VarP(&options.Within, "keep-within", "", "keep snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.Var(&options.WithinMinutely, "keep-within-minutely", "keep minutely snapshots newer than `duration`")
	f.VarP(
		&options.WithinHourly, "keep-within-hourly", "",
		"keep hourly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot",
	)
	f.VarP(
		&options.WithinDaily, "keep-within-daily", "",
		"keep daily snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot",
	)
	f.VarP(
		&options.WithinWeekly, "keep-within-weekly", "",
		"keep weekly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot",
	)
	f.VarP(
		&options.WithinMonthly,
		"keep-within-monthly",
		"",
		"keep monthly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot",
	)
	f.Var(&options.WithinQuarterYearly, "keep-within-quarter-yearly", "keep quarterly snapshots newer than `duration`")
	f.Var(&options.WithinHalfYearly, "keep-within-half-yearly", "keep half-yearly snapshots newer than `duration`")
	f.VarP(
		&options.WithinYearly, "keep-within-yearly", "",
		"keep yearly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot",
	)
	f.Var(&options.KeepTags, "keep-tag", "keep snapshots with this `taglist` (can be specified multiple times)")
	f.BoolVar(&options.UnsafeAllowRemoveAll, "unsafe-allow-remove-all", false, "allow deleting all snapshots of a snapshot group")
	f.BoolVar(&options.UnsafeAllowRemoveAll, "keep-none", false, "allow deleting all snapshots of a snapshot group")
	f.BoolVar(
		&options.OverrideDeleteProtection,
		"override-delete-protection",
		false,
		"remove snapshots even if they are marked delete-protected (delete_never/delete_after)",
	)
	f.StringArrayVar(&options.Hosts, "hostname", nil, "only consider snapshots with the given `hostname` (can be specified multiple times)")
	err := f.MarkDeprecated("hostname", "use --host")
	if err != nil {
		// MarkDeprecated only returns an error when the flag is not found
		// Flag registration is a construction-time invariant.
		panic(err) //nolint:forbidigo // A missing flag here is a command-construction defect.
	}
	// must be defined after `--hostname` to not override the default value from the environment
	initMultiSnapshotFilter(f, &options.SnapshotFilter, false)
	f.BoolVarP(&options.Compact, "compact", "c", false, "use compact output format")
	options.GroupBy = data.SnapshotGroupByOptions{Host: true, Path: true}
	f.VarP(&options.GroupBy, "group-by", "g", "`group` snapshots by host, paths and/or tags, separated by comma (disable grouping with '')")
	f.BoolVarP(&options.DryRun, "dry-run", "n", false, "do not delete anything, just print what would be done")
	f.BoolVar(&options.Prune, "prune", false, "automatically run the 'prune' command if snapshots have been removed")
	f.SortFlags = false
}

func verifyForgetOptions(options *forgetOptions) error {
	if options.Last < -1 || options.Minutely < -1 || options.Hourly < -1 || options.Daily < -1 || options.Weekly < -1 ||
		options.Monthly < -1 || options.QuarterYearly < -1 || options.HalfYearly < -1 || options.Yearly < -1 {
		return errors.Fatal("negative values other than -1 are not allowed for --keep-*")
	}
	for _, d := range []data.Duration{options.Within, options.WithinMinutely, options.WithinHourly, options.WithinDaily,
		options.WithinMonthly, options.WithinQuarterYearly, options.WithinHalfYearly, options.WithinWeekly, options.WithinYearly} {
		if d.Hours < 0 || d.Days < 0 || d.Months < 0 || d.Years < 0 {
			return errors.Fatal("durations containing negative values are not allowed for --keep-within*")
		}
	}
	return nil
}

// buildForgetPlan evaluates the current snapshot set without modifying the
// repository. Phase A and the exclusive phase-B revalidation both use this
// same helper, so phase B can delete only IDs selected by both observations.
func buildForgetPlan(ctx context.Context, options forgetOptions, repo vaultic.Repository, args []string) (forgetPlan, error) {
	plan := forgetPlan{remove: vaultic.NewIDSet(), horizons: make(map[vaultic.ID]repository.SnapshotRetentionHorizon)}
	var snapshots data.Snapshots
	now := time.Now()
	isProtected := func(sn *data.Snapshot) bool {
		return !options.OverrideDeleteProtection && sn.Delete != nil && sn.Delete.MustKeep(now)
	}
	if err := options.SnapshotFilter.FindAll(ctx, repo, repo, args, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		if isProtected(sn) {
			plan.protected++
			return nil
		}
		snapshots = append(snapshots, sn)
		return nil
	}); err != nil {
		return forgetPlan{}, err
	}
	if len(args) > 0 {
		for _, sn := range snapshots {
			plan.remove.Insert(*sn.ID())
		}
		return plan, nil
	}

	snapshotGroups, _, err := data.GroupSnapshots(snapshots, options.GroupBy)
	if err != nil {
		return forgetPlan{}, err
	}
	policy := forgetPolicy(options)
	if err := validateForgetPolicy(options, policy); err != nil {
		return forgetPlan{}, err
	}
	for keyJSON, snapshotGroup := range snapshotGroups {
		if ctx.Err() != nil {
			return forgetPlan{}, ctx.Err()
		}
		var key data.SnapshotGroupKey
		if err := json.Unmarshal([]byte(keyJSON), &key); err != nil {
			return forgetPlan{}, err
		}
		keep, remove, reasons := data.ApplyPolicy(snapshotGroup, policy)
		if !policy.Empty() && len(keep) == 0 {
			return forgetPlan{}, fmt.Errorf("refusing to delete last snapshot of snapshot group %q", key.String())
		}
		group := &ForgetGroup{
			Tags:    key.Tags,
			Host:    key.Hostname,
			Paths:   key.Paths,
			Keep:    asJSONSnapshots(keep),
			Remove:  asJSONSnapshots(remove),
			Reasons: asJSONKeeps(reasons),
		}
		plan.groups = append(plan.groups, group)
		for _, reason := range reasons {
			horizon, known := data.PolicyRetentionHorizon(reason, policy)
			if !known {
				continue
			}
			plan.horizons[*reason.Snapshot.ID()] = repository.SnapshotRetentionHorizon{
				Until: horizon.Until, Indefinite: horizon.Indefinite,
			}
		}
		for _, sn := range remove {
			plan.remove.Insert(*sn.ID())
		}
	}
	return plan, nil
}

func forgetPolicy(options forgetOptions) data.ExpirePolicy {
	return data.ExpirePolicy{
		Last:                int(options.Last),
		Minutely:            int(options.Minutely),
		Hourly:              int(options.Hourly),
		Daily:               int(options.Daily),
		Weekly:              int(options.Weekly),
		Monthly:             int(options.Monthly),
		QuarterYearly:       int(options.QuarterYearly),
		HalfYearly:          int(options.HalfYearly),
		Yearly:              int(options.Yearly),
		Within:              options.Within,
		WithinMinutely:      options.WithinMinutely,
		WithinHourly:        options.WithinHourly,
		WithinDaily:         options.WithinDaily,
		WithinWeekly:        options.WithinWeekly,
		WithinMonthly:       options.WithinMonthly,
		WithinQuarterYearly: options.WithinQuarterYearly,
		WithinHalfYearly:    options.WithinHalfYearly,
		WithinYearly:        options.WithinYearly,
		Tags:                options.KeepTags,
	}
}

func validateForgetPolicy(options forgetOptions, policy data.ExpirePolicy) error {
	if !policy.Empty() {
		return nil
	}
	if !options.UnsafeAllowRemoveAll {
		return errors.Fatal("no policy was specified, no snapshots will be removed")
	}
	if options.SnapshotFilter.Empty() {
		return errors.Fatal("--unsafe-allow-remove-all is not allowed unless a snapshot filter option is specified")
	}
	return nil
}

func (plan forgetPlan) revalidatedAgainst(initial vaultic.IDSet) forgetPlan {
	plan.remove = plan.remove.Intersect(initial)
	for _, group := range plan.groups {
		filtered := group.Remove[:0]
		for _, sn := range group.Remove {
			if sn.ID != nil && plan.remove.Has(*sn.ID) {
				filtered = append(filtered, sn)
			}
		}
		group.Remove = filtered
	}
	return plan
}

// runForget evaluates retention under a shared/read lock, then takes an
// exclusive lock only to re-list, revalidate, and delete snapshots that remain
// selected by both views. A snapshot created, protected, or retained after
// phase A is never deleted by phase B.
func runForget(ctx context.Context, options forgetOptions, pruneOptions pruneOptions, globalOptions global.Options, term ui.Terminal, args []string) error {
	return runForgetWithPhaseACallback(ctx, options, pruneOptions, globalOptions, term, args, nil)
}

func runForgetWithPhaseACallback(
	ctx context.Context, options forgetOptions, pruneOptions pruneOptions,
	globalOptions global.Options, term ui.Terminal, args []string, afterPhaseA func(),
) error {
	if err := verifyForgetOptions(&options); err != nil {
		return err
	}
	if err := verifyPruneOptions(&pruneOptions); err != nil {
		return err
	}
	if globalOptions.NoLock && !options.DryRun {
		return errors.Fatal("--no-lock is only applicable in combination with --dry-run for forget command")
	}
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term)
	baseCtx := ctx
	// Dry runs are read-only and use a single shared/read observation.
	if options.DryRun {
		ctx, repo, unlock, err := openWithReadLock(baseCtx, globalOptions, globalOptions.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		plan, err := buildForgetPlan(ctx, options, repo, args)
		if err != nil {
			return err
		}
		return printForgetPlan(printer, globalOptions, options, args, plan)
	}
	// Phase A: policy evaluation while append writers may proceed.
	ctx, repo, unlock, err := openWithReadLock(baseCtx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	initial, err := buildForgetPlan(ctx, options, repo, args)
	unlock()
	if err != nil {
		return err
	}
	if afterPhaseA != nil {
		afterPhaseA()
	}
	// Phase B: a short exclusive window recomputes the policy and intersects
	// candidates with phase A before deleting. Existing snapshots that are now
	// protected or no longer selected are retained.
	ctx, repo, unlock, err = openWithExclusiveLock(baseCtx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := buildForgetPlan(ctx, options, repo, args)
	if err != nil {
		return err
	}
	plan := current.revalidatedAgainst(initial.remove)
	if err := printForgetPlan(printer, globalOptions, options, args, plan); err != nil {
		return err
	}
	failed, err := deleteForgetSnapshots(ctx, repo, plan.remove, printer)
	if err != nil {
		return err
	}
	if len(failed) != 0 {
		return ErrFailedToRemoveOneOrMoreSnapshots
	}
	if engine, ok := repo.Engine().(*metadataindex.DaemonEngine); ok {
		if err := repository.RecordPromotionEligibility(ctx, repo, engine.SchemaStore(), plan.horizons, time.Now(), printer); err != nil {
			return fmt.Errorf("record placement promotion eligibility: %w", err)
		}
	}
	if len(plan.remove) != 0 && options.Prune {
		printer.P("%d snapshots have been removed, running prune\n", len(plan.remove))
		return runPruneWithRepo(ctx, pruneOptions, globalOptions, repo, plan.remove, printer)
	}
	return nil
}

func printForgetPlan(printer vaultic.Printer, globalOptions global.Options, options forgetOptions, args []string, plan forgetPlan) error {
	if plan.protected > 0 && !globalOptions.JSON {
		printer.P("kept %d delete-protected snapshots\n", plan.protected)
	}
	if len(args) == 0 {
		printer.P("Applying Policy: %v\n", forgetPolicy(options))
	}
	if globalOptions.JSON {
		if len(plan.groups) != 0 {
			if err := printJSONForget(globalOptions.Term.OutputWriter(), plan.groups); err != nil {
				printer.E("error printing forget result: %v\n", err)
			}
		}
		return nil
	}
	if globalOptions.Quiet {
		return nil
	}
	for _, group := range plan.groups {
		if len(group.Keep) != 0 {
			printer.P("keep %d snapshots:\n", len(group.Keep))
			if err := querycmd.PrintSnapshots(globalOptions.Term.OutputWriter(), snapshotsFromJSON(group.Keep), nil, options.Compact); err != nil {
				return fmt.Errorf("print retained snapshots: %w", err)
			}
			printer.P("\n")
		}
		if len(group.Remove) != 0 {
			printer.P("remove %d snapshots:\n", len(group.Remove))
			if err := querycmd.PrintSnapshots(globalOptions.Term.OutputWriter(), snapshotsFromJSON(group.Remove), nil, options.Compact); err != nil {
				return fmt.Errorf("print removed snapshots: %w", err)
			}
			printer.P("\n")
		}
	}
	return nil
}

func snapshotsFromJSON(items []Snapshot) data.Snapshots {
	result := make(data.Snapshots, 0, len(items))
	for _, item := range items {
		result = append(result, item.Snapshot)
	}
	return result
}

func deleteForgetSnapshots(ctx context.Context, repo vaultic.Repository, ids vaultic.IDSet, printer vaultic.Printer) (vaultic.IDSet, error) {
	failed := vaultic.NewIDSet()
	if len(ids) == 0 {
		return failed, nil
	}
	bar := printer.NewCounter("files deleted")
	err := vaultic.ParallelRemove(ctx, repo, ids, vaultic.WriteableSnapshotFile, func(id vaultic.ID, err error) error {
		if err != nil {
			printer.E("unable to remove %v/%v from the repository\n", vaultic.SnapshotFile, id)
			failed.Insert(id)
		} else {
			printer.VV("removed %v/%v\n", vaultic.SnapshotFile, id)
		}
		return nil
	}, bar)
	bar.Done()
	return failed, err
}

// ForgetGroup helps to print what is forgotten in JSON.
type ForgetGroup struct {
	Tags    []string     `json:"tags"`
	Host    string       `json:"host"`
	Paths   []string     `json:"paths"`
	Keep    []Snapshot   `json:"keep"`
	Remove  []Snapshot   `json:"remove"`
	Reasons []KeepReason `json:"reasons"`
}

func asJSONSnapshots(list data.Snapshots) []Snapshot {
	var resultList []Snapshot
	for _, sn := range list {
		k := Snapshot{
			Snapshot: sn,
			ID:       sn.ID(),
			ShortID:  sn.ID().Str(),
		}
		resultList = append(resultList, k)
	}
	return resultList
}

// KeepReason helps to print KeepReasons as JSON with Snapshots with their ID included.
type KeepReason struct {
	Snapshot Snapshot `json:"snapshot"`
	Matches  []string `json:"matches"`
}

func asJSONKeeps(list []data.KeepReason) []KeepReason {
	var resultList []KeepReason
	for _, keep := range list {
		k := KeepReason{
			Snapshot: Snapshot{
				Snapshot: keep.Snapshot,
				ID:       keep.Snapshot.ID(),
				ShortID:  keep.Snapshot.ID().Str(),
			},
			Matches: keep.Matches,
		}
		resultList = append(resultList, k)
	}
	return resultList
}

func printJSONForget(stdout io.Writer, forgets []*ForgetGroup) error {
	return json.NewEncoder(stdout).Encode(forgets)
}
