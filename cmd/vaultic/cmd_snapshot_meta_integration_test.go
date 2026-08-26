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
func runSnapshotsFiltered(t testing.TB, gopts global.Options, args, ids []string) []Snapshot {
	t.Helper()
	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.JSON = true
		var opts SnapshotOptions
		fs := pflag.NewFlagSet("snapshots", pflag.ContinueOnError)
		opts.AddFlags(fs)
		rtest.OK(t, fs.Parse(args))
		finalizeSnapshotFilter(&opts.SnapshotFilter)
		return runSnapshots(ctx, opts, gopts, ids, gopts.Term)
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

	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0")}, BackupOptions{
		Host: "h1", Label: "daily", Description: "daily backup",
	}, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0")}, BackupOptions{
		Host: "h1", Label: "weekly",
	}, env.gopts)

	// label is stored and shown
	all := runSnapshotsFiltered(t, env.gopts, nil, nil)
	rtest.Equals(t, 2, len(all))

	// --filter-label daily selects exactly one
	daily := runSnapshotsFiltered(t, env.gopts, []string{"--filter-label", "daily"}, nil)
	rtest.Equals(t, 1, len(daily))
	rtest.Equals(t, "daily", daily[0].Label)
	rtest.Equals(t, "daily backup", daily[0].Description)
}

func TestSnapshotFilterLastAndLatestN(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	for i := 0; i < 3; i++ {
		testRunBackup(t, "", []string{filepath.Join(env.testdata, "0")}, BackupOptions{Host: "h1"}, env.gopts)
	}

	all := runSnapshotsFiltered(t, env.gopts, nil, nil)
	rtest.Equals(t, 3, len(all))

	// --filter-last 2 keeps the two newest
	latest := runSnapshotsFiltered(t, env.gopts, []string{"--filter-last", "2"}, nil)
	rtest.Equals(t, 2, len(latest))

	// latest~1 selects the second-newest snapshot (IDs are time-ordered here)
	ids := runSnapshotsFiltered(t, env.gopts, nil, []string{"latest~1"})
	rtest.Equals(t, 1, len(ids))
}

func TestForgetRespectsDeleteProtection(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	target := filepath.Join(env.testdata, "0")

	// one protected, two unprotected snapshots
	testRunBackup(t, "", []string{target}, BackupOptions{Host: "h1"}, env.gopts)
	testRunBackup(t, "", []string{target}, BackupOptions{Host: "h1", DeleteNever: true}, env.gopts)
	testRunBackup(t, "", []string{target}, BackupOptions{Host: "h1"}, env.gopts)

	// forget everything in the group via a policy: only the protected snapshot
	// must survive. (--unsafe-allow-remove-all permits emptying a group; a
	// snapshot filter (here: the host) is required to use it)
	testRunForget(t, env.gopts, ForgetOptions{
		Last:                 mustForgetPolicyCount(t, "0"),
		UnsafeAllowRemoveAll: true,
		SnapshotFilter:       data.SnapshotFilter{Hosts: []string{"h1"}},
	})

	// read back the surviving snapshots directly
	var remaining []*data.Snapshot
	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		ctx, repo, unlock, err := openWithReadLock(ctx, gopts, false, progress.NewTerminalPrinter(false, 0, gopts.Term))
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
