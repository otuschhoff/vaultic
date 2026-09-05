package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/pflag"
)

// runSnapshotsFiltered runs the snapshots command with the given filter args
// and returns the snapshots. It uses the same default (ungrouped) JSON shape as
// testRunSnapshots.
func runSnapshotsFiltered(t testing.TB, globalOptions global.Options, args, ids []string) []Snapshot {
	t.Helper()
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = true
		var options snapshotOptions
		fs := pflag.NewFlagSet("snapshots", pflag.ContinueOnError)
		options.AddFlags(fs)
		rtest.OK(t, fs.Parse(args))
		finalizeSnapshotFilter(&options.SnapshotFilter)
		return runSnapshots(ctx, options, globalOptions, ids, globalOptions.Term)
	})
	rtest.OK(t, err)

	// the JSON output is grouped by GroupSnapshots even with default grouping;
	// try the flat list first, then the grouped shape
	var flat []Snapshot
	if err := json.Unmarshal(buf.Bytes(), &flat); err == nil && flat != nil {
		return flat
	}
	var grouped []SnapshotGroup
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &grouped))
	var out []Snapshot
	for _, g := range grouped {
		out = append(out, g.Snapshots...)
	}
	return out
}

func TestSnapshotLabelDescriptionAndFilter(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0")}, backupOptions{
		Host: "h1", Label: "daily", Description: "daily backup",
	}, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0")}, backupOptions{
		Host: "h1", Label: "weekly",
	}, env.globalOptions)

	// label is stored and shown
	all := runSnapshotsFiltered(t, env.globalOptions, nil, nil)
	rtest.Equals(t, 2, len(all))

	// --filter-label daily selects exactly one
	daily := runSnapshotsFiltered(t, env.globalOptions, []string{"--filter-label", "daily"}, nil)
	rtest.Equals(t, 1, len(daily))
	rtest.Equals(t, "daily", daily[0].Label)
	rtest.Equals(t, "daily backup", daily[0].Description)
}

func TestSnapshotFilterLastAndLatestN(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	for i := 0; i < 3; i++ {
		testRunBackup(t, "", []string{filepath.Join(env.testdata, "0")}, backupOptions{Host: "h1"}, env.globalOptions)
	}

	all := runSnapshotsFiltered(t, env.globalOptions, nil, nil)
	rtest.Equals(t, 3, len(all))

	// --filter-last 2 keeps the two newest
	latest := runSnapshotsFiltered(t, env.globalOptions, []string{"--filter-last", "2"}, nil)
	rtest.Equals(t, 2, len(latest))

	// latest~1 selects the second-newest snapshot (IDs are time-ordered here)
	ids := runSnapshotsFiltered(t, env.globalOptions, nil, []string{"latest~1"})
	rtest.Equals(t, 1, len(ids))
}

func TestForgetRespectsDeleteProtection(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	target := filepath.Join(env.testdata, "0")

	// one protected, two unprotected snapshots
	testRunBackup(t, "", []string{target}, backupOptions{Host: "h1"}, env.globalOptions)
	testRunBackup(t, "", []string{target}, backupOptions{Host: "h1", DeleteNever: true}, env.globalOptions)
	testRunBackup(t, "", []string{target}, backupOptions{Host: "h1"}, env.globalOptions)

	// forget everything in the group via a policy: only the protected snapshot
	// must survive. (--unsafe-allow-remove-all permits emptying a group; a
	// snapshot filter (here: the host) is required to use it)
	testRunForget(t, env.globalOptions, forgetOptions{
		Last:                 mustForgetPolicyCount(t, "0"),
		UnsafeAllowRemoveAll: true,
		SnapshotFilter:       data.SnapshotFilter{Hosts: []string{"h1"}},
	})

	// read back the surviving snapshots directly
	var remaining []*data.Snapshot
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, progress.NewTerminalPrinter(false, 0, globalOptions.Term))
		rtest.OK(t, err)
		defer unlock()
		return data.ForAllSnapshots(ctx, repo, repo, nil, func(_ vaultic.ID, sn *data.Snapshot, err error) error {
			if err == nil {
				remaining = append(remaining, sn)
			}
			return err
		})
	})
	rtest.OK(t, err)
	rtest.Equals(t, 1, len(remaining))
	rtest.Assert(t, remaining[0].Delete != nil && remaining[0].Delete.Never,
		"surviving snapshot is not the protected one")
}

func mustForgetPolicyCount(t testing.TB, s string) ForgetPolicyCount {
	t.Helper()
	var c ForgetPolicyCount
	rtest.OK(t, c.Set(s))
	return c
}
