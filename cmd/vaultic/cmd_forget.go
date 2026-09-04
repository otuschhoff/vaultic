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
	var opts ForgetOptions
	var pruneOpts PruneOptions
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
			finalizeSnapshotFilter(&opts.SnapshotFilter)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runForget(cmd.Context(), opts, pruneOpts, *globalOptions, globalOptions.Term, args)
		},
	}
	opts.AddFlags(cmd.Flags())
	pruneOpts.AddLimitedFlags(cmd.Flags())
	return cmd
}

type ForgetPolicyCount int

var ErrNegativePolicyCount = errors.New("negative values not allowed, use 'unlimited' instead")
var ErrFailedToRemoveOneOrMoreSnapshots = errors.New("failed to remove one or more snapshots")

// forgetPhaseATestHook pauses tests after shared policy evaluation and before
// exclusive revalidation. Production code leaves it nil.
var forgetPhaseATestHook func()

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

// ForgetOptions collects all options for the forget command.
type ForgetOptions struct {
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

func (opts *ForgetOptions) AddFlags(f *pflag.FlagSet) {
	f.VarP(&opts.Last, "keep-last", "l", "keep the last `n` snapshots (use 'unlimited' to keep all snapshots)")
	f.Var(&opts.Minutely, "keep-minutely", "keep the last `n` minutely snapshots")
	f.VarP(&opts.Hourly, "keep-hourly", "H", "keep the last `n` hourly snapshots (use 'unlimited' to keep all hourly snapshots)")
	f.VarP(&opts.Daily, "keep-daily", "d", "keep the last `n` daily snapshots (use 'unlimited' to keep all daily snapshots)")
	f.VarP(&opts.Weekly, "keep-weekly", "w", "keep the last `n` weekly snapshots (use 'unlimited' to keep all weekly snapshots)")
	f.VarP(&opts.Monthly, "keep-monthly", "m", "keep the last `n` monthly snapshots (use 'unlimited' to keep all monthly snapshots)")
	f.Var(&opts.QuarterYearly, "keep-quarter-yearly", "keep the last `n` quarterly snapshots")
	f.Var(&opts.HalfYearly, "keep-half-yearly", "keep the last `n` half-yearly snapshots")
	f.VarP(&opts.Yearly, "keep-yearly", "y", "keep the last `n` yearly snapshots (use 'unlimited' to keep all yearly snapshots)")
	f.VarP(&opts.Within, "keep-within", "", "keep snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.Var(&opts.WithinMinutely, "keep-within-minutely", "keep minutely snapshots newer than `duration`")
	f.VarP(&opts.WithinHourly, "keep-within-hourly", "", "keep hourly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(&opts.WithinDaily, "keep-within-daily", "", "keep daily snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(&opts.WithinWeekly, "keep-within-weekly", "", "keep weekly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(
		&opts.WithinMonthly,
		"keep-within-monthly",
		"",
		"keep monthly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot",
	)
	f.Var(&opts.WithinQuarterYearly, "keep-within-quarter-yearly", "keep quarterly snapshots newer than `duration`")
	f.Var(&opts.WithinHalfYearly, "keep-within-half-yearly", "keep half-yearly snapshots newer than `duration`")
	f.VarP(&opts.WithinYearly, "keep-within-yearly", "", "keep yearly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.Var(&opts.KeepTags, "keep-tag", "keep snapshots with this `taglist` (can be specified multiple times)")
	f.BoolVar(&opts.UnsafeAllowRemoveAll, "unsafe-allow-remove-all", false, "allow deleting all snapshots of a snapshot group")
	f.BoolVar(&opts.UnsafeAllowRemoveAll, "keep-none", false, "allow deleting all snapshots of a snapshot group")
	f.BoolVar(
		&opts.OverrideDeleteProtection,
		"override-delete-protection",
		false,
		"remove snapshots even if they are marked delete-protected (delete_never/delete_after)",
	)
	f.StringArrayVar(&opts.Hosts, "hostname", nil, "only consider snapshots with the given `hostname` (can be specified multiple times)")
	err := f.MarkDeprecated("hostname", "use --host")
	if err != nil {
		// MarkDeprecated only returns an error when the flag is not found
		panic(err) //nolint:forbidigo // flag registration is a construction-time invariant
	}
	// must be defined after `--hostname` to not override the default value from the environment
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, false)
	f.BoolVarP(&opts.Compact, "compact", "c", false, "use compact output format")
	opts.GroupBy = data.SnapshotGroupByOptions{Host: true, Path: true}
	f.VarP(&opts.GroupBy, "group-by", "g", "`group` snapshots by host, paths and/or tags, separated by comma (disable grouping with '')")
	f.BoolVarP(&opts.DryRun, "dry-run", "n", false, "do not delete anything, just print what would be done")
	f.BoolVar(&opts.Prune, "prune", false, "automatically run the 'prune' command if snapshots have been removed")
	f.SortFlags = false
}

func verifyForgetOptions(opts *ForgetOptions) error {
	if opts.Last < -1 || opts.Minutely < -1 || opts.Hourly < -1 || opts.Daily < -1 || opts.Weekly < -1 ||
		opts.Monthly < -1 || opts.QuarterYearly < -1 || opts.HalfYearly < -1 || opts.Yearly < -1 {
		return errors.Fatal("negative values other than -1 are not allowed for --keep-*")
	}
	for _, d := range []data.Duration{opts.Within, opts.WithinMinutely, opts.WithinHourly, opts.WithinDaily,
		opts.WithinMonthly, opts.WithinQuarterYearly, opts.WithinHalfYearly, opts.WithinWeekly, opts.WithinYearly} {
		if d.Hours < 0 || d.Days < 0 || d.Months < 0 || d.Years < 0 {
			return errors.Fatal("durations containing negative values are not allowed for --keep-within*")
		}
	}
	return nil
}

// buildForgetPlan evaluates the current snapshot set without modifying the
// repository. Phase A and the exclusive phase-B revalidation both use this
// same helper, so phase B can delete only IDs selected by both observations.
func buildForgetPlan(ctx context.Context, opts ForgetOptions, repo vaultic.Repository, args []string) (forgetPlan, error) {
	plan := forgetPlan{remove: vaultic.NewIDSet(), horizons: make(map[vaultic.ID]repository.SnapshotRetentionHorizon)}
	var snapshots data.Snapshots
	now := time.Now()
	isProtected := func(sn *data.Snapshot) bool {
		return !opts.OverrideDeleteProtection && sn.Delete != nil && sn.Delete.MustKeep(now)
	}
	if err := opts.SnapshotFilter.FindAll(ctx, repo, repo, args, func(_ string, sn *data.Snapshot, err error) error {
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

	snapshotGroups, _, err := data.GroupSnapshots(snapshots, opts.GroupBy)
	if err != nil {
		return forgetPlan{}, err
	}
	policy := forgetPolicy(opts)
	if err := validateForgetPolicy(opts, policy); err != nil {
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

func forgetPolicy(opts ForgetOptions) data.ExpirePolicy {
	return data.ExpirePolicy{
		Last:                int(opts.Last),
		Minutely:            int(opts.Minutely),
		Hourly:              int(opts.Hourly),
		Daily:               int(opts.Daily),
		Weekly:              int(opts.Weekly),
		Monthly:             int(opts.Monthly),
		QuarterYearly:       int(opts.QuarterYearly),
		HalfYearly:          int(opts.HalfYearly),
		Yearly:              int(opts.Yearly),
		Within:              opts.Within,
		WithinMinutely:      opts.WithinMinutely,
		WithinHourly:        opts.WithinHourly,
		WithinDaily:         opts.WithinDaily,
		WithinWeekly:        opts.WithinWeekly,
		WithinMonthly:       opts.WithinMonthly,
		WithinQuarterYearly: opts.WithinQuarterYearly,
		WithinHalfYearly:    opts.WithinHalfYearly,
		WithinYearly:        opts.WithinYearly,
		Tags:                opts.KeepTags,
	}
}

func validateForgetPolicy(opts ForgetOptions, policy data.ExpirePolicy) error {
	if !policy.Empty() {
		return nil
	}
	if !opts.UnsafeAllowRemoveAll {
		return errors.Fatal("no policy was specified, no snapshots will be removed")
	}
	if opts.SnapshotFilter.Empty() {
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
func runForget(ctx context.Context, opts ForgetOptions, pruneOptions PruneOptions, gopts global.Options, term ui.Terminal, args []string) error {
	if err := verifyForgetOptions(&opts); err != nil {
		return err
	}
	if err := verifyPruneOptions(&pruneOptions); err != nil {
		return err
	}
	if gopts.NoLock && !opts.DryRun {
		return errors.Fatal("--no-lock is only applicable in combination with --dry-run for forget command")
	}
	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)
	baseCtx := ctx
	// Dry runs are read-only and use a single shared/read observation.
	if opts.DryRun {
		ctx, repo, unlock, err := openWithReadLock(baseCtx, gopts, gopts.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		plan, err := buildForgetPlan(ctx, opts, repo, args)
		if err != nil {
			return err
		}
		printForgetPlan(printer, gopts, opts, args, plan)
		return nil
	}
	// Phase A: policy evaluation while append writers may proceed.
	ctx, repo, unlock, err := openWithReadLock(baseCtx, gopts, false, printer)
	if err != nil {
		return err
	}
	initial, err := buildForgetPlan(ctx, opts, repo, args)
	unlock()
	if err != nil {
		return err
	}
	if forgetPhaseATestHook != nil {
		forgetPhaseATestHook()
	}
	// Phase B: a short exclusive window recomputes the policy and intersects
	// candidates with phase A before deleting. Existing snapshots that are now
	// protected or no longer selected are retained.
	ctx, repo, unlock, err = openWithExclusiveLock(baseCtx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := buildForgetPlan(ctx, opts, repo, args)
	if err != nil {
		return err
	}
	plan := current.revalidatedAgainst(initial.remove)
	printForgetPlan(printer, gopts, opts, args, plan)
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
	if len(plan.remove) != 0 && opts.Prune {
		printer.P("%d snapshots have been removed, running prune\n", len(plan.remove))
		return runPruneWithRepo(ctx, pruneOptions, gopts, repo, plan.remove, printer)
	}
	return nil
}

func printForgetPlan(printer vaultic.Printer, gopts global.Options, opts ForgetOptions, args []string, plan forgetPlan) {
	if plan.protected > 0 && !gopts.JSON {
		printer.P("kept %d delete-protected snapshots\n", plan.protected)
	}
	if len(args) == 0 {
		printer.P("Applying Policy: %v\n", forgetPolicy(opts))
	}
	if gopts.JSON {
		if len(plan.groups) != 0 {
			if err := printJSONForget(gopts.Term.OutputWriter(), plan.groups); err != nil {
				printer.E("error printing forget result: %v\n", err)
			}
		}
		return
	}
	if gopts.Quiet {
		return
	}
	for _, group := range plan.groups {
		if len(group.Keep) != 0 {
			printer.P("keep %d snapshots:\n", len(group.Keep))
			_ = querycmd.PrintSnapshots(gopts.Term.OutputWriter(), snapshotsFromJSON(group.Keep), nil, opts.Compact)
			printer.P("\n")
		}
		if len(group.Remove) != 0 {
			printer.P("remove %d snapshots:\n", len(group.Remove))
			_ = querycmd.PrintSnapshots(gopts.Term.OutputWriter(), snapshotsFromJSON(group.Remove), nil, opts.Compact)
			printer.P("\n")
		}
	}
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

func runForgetLegacy(ctx context.Context, opts ForgetOptions, pruneOptions PruneOptions, gopts global.Options, term ui.Terminal, args []string) error {
	if err := verifyForgetOptions(&opts); err != nil {
		return err
	}
	if err := verifyPruneOptions(&pruneOptions); err != nil {
		return err
	}
	if gopts.NoLock && !opts.DryRun {
		return errors.Fatal("--no-lock is only applicable in combination with --dry-run for forget command")
	}
	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, opts.DryRun && gopts.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()
	snapshots, protected, err := findLegacyForgetSnapshots(ctx, opts, repo, args, time.Now())
	if err != nil {
		return err
	}
	if protected > 0 && !gopts.JSON {
		printer.P("kept %d delete-protected snapshots\n", protected)
	}
	removeSnIDs, jsonGroups, err := selectLegacyForgetSnapshots(ctx, opts, gopts, printer, snapshots, args)
	if err != nil {
		return err
	}
	failedSnIDs := vaultic.NewIDSet()
	if len(removeSnIDs) != 0 && !opts.DryRun {
		failedSnIDs, err = deleteForgetSnapshots(ctx, repo, removeSnIDs, printer)
		if err != nil {
			return err
		}
	} else if len(removeSnIDs) != 0 {
		printer.P("Would have removed the following snapshots:\n%v\n\n", removeSnIDs)
	}
	if gopts.JSON && len(jsonGroups) > 0 {
		err = printJSONForget(gopts.Term.OutputWriter(), jsonGroups)
		if err != nil {
			return err
		}
	}
	if len(failedSnIDs) > 0 {
		return ErrFailedToRemoveOneOrMoreSnapshots
	}
	if len(removeSnIDs) > 0 && opts.Prune {
		if opts.DryRun {
			printer.P("%d snapshots would be removed, running prune dry run\n", len(removeSnIDs))
		} else {
			printer.P("%d snapshots have been removed, running prune\n", len(removeSnIDs))
		}
		pruneOptions.DryRun = opts.DryRun
		return runPruneWithRepo(ctx, pruneOptions, gopts, repo, removeSnIDs, printer)
	}

	return nil
}

//nolint:unused // retained as part of the dormant legacy forget implementation
func findLegacyForgetSnapshots(ctx context.Context, opts ForgetOptions, repo vaultic.Repository, args []string, now time.Time) (data.Snapshots, int, error) {
	var snapshots data.Snapshots
	protected := 0
	err := opts.SnapshotFilter.FindAll(ctx, repo, repo, args, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		if !opts.OverrideDeleteProtection && sn.Delete != nil && sn.Delete.MustKeep(now) {
			protected++
			return nil
		}
		snapshots = append(snapshots, sn)
		return nil
	})
	return snapshots, protected, err
}

//nolint:unused // retained as part of the dormant legacy forget implementation
func selectLegacyForgetSnapshots(
	ctx context.Context, opts ForgetOptions, gopts global.Options, printer vaultic.Printer,
	snapshots data.Snapshots, args []string,
) (vaultic.IDSet, []*ForgetGroup, error) {
	removeIDs := vaultic.NewIDSet()
	if len(args) != 0 {
		for _, sn := range snapshots {
			removeIDs.Insert(*sn.ID())
		}
		return removeIDs, nil, nil
	}
	snapshotGroups, _, err := data.GroupSnapshots(snapshots, opts.GroupBy)
	if err != nil {
		return nil, nil, err
	}
	policy := forgetPolicy(opts)
	if err := validateForgetPolicy(opts, policy); err != nil {
		return nil, nil, err
	}
	printer.P("Applying Policy: %v\n", policy)
	jsonGroups := make([]*ForgetGroup, 0, len(snapshotGroups))
	for keyJSON, snapshotGroup := range snapshotGroups {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if gopts.Verbose >= 1 && !gopts.JSON {
			if err := querycmd.PrintSnapshotGroupHeader(gopts.Term.OutputWriter(), keyJSON); err != nil {
				return nil, nil, err
			}
		}
		group, remove, err := evaluateLegacyForgetGroup(opts, gopts, printer, keyJSON, snapshotGroup, policy)
		if err != nil {
			return nil, nil, err
		}
		jsonGroups = append(jsonGroups, group)
		for _, sn := range remove {
			removeIDs.Insert(*sn.ID())
		}
	}
	return removeIDs, jsonGroups, nil
}

//nolint:unused // retained as part of the dormant legacy forget implementation
func evaluateLegacyForgetGroup(
	opts ForgetOptions, gopts global.Options, printer vaultic.Printer,
	keyJSON string, snapshots data.Snapshots, policy data.ExpirePolicy,
) (*ForgetGroup, data.Snapshots, error) {
	var key data.SnapshotGroupKey
	if err := json.Unmarshal([]byte(keyJSON), &key); err != nil {
		return nil, nil, err
	}
	keep, remove, reasons := data.ApplyPolicy(snapshots, policy)
	if !policy.Empty() && len(keep) == 0 {
		return nil, nil, fmt.Errorf("refusing to delete last snapshot of snapshot group \"%v\"", key.String())
	}
	if len(keep) != 0 && !gopts.Quiet && !gopts.JSON {
		printer.P("keep %d snapshots:\n", len(keep))
		if err := querycmd.PrintSnapshots(gopts.Term.OutputWriter(), keep, reasons, opts.Compact); err != nil {
			return nil, nil, err
		}
		printer.P("\n")
	}
	if len(remove) != 0 && !gopts.Quiet && !gopts.JSON {
		printer.P("remove %d snapshots:\n", len(remove))
		if err := querycmd.PrintSnapshots(gopts.Term.OutputWriter(), remove, nil, opts.Compact); err != nil {
			return nil, nil, err
		}
		printer.P("\n")
	}
	group := &ForgetGroup{
		Tags: key.Tags, Host: key.Hostname, Paths: key.Paths,
		Keep: asJSONSnapshots(keep), Remove: asJSONSnapshots(remove), Reasons: asJSONKeeps(reasons),
	}
	return group, remove, nil
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
