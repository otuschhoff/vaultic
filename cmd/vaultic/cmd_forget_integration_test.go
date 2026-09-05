package main

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func testRunForgetMayFail(t testing.TB, globalOptions global.Options, options forgetOptions, args ...string) error {
	pruneOpts := pruneOptions{
		MaxUnused: "5%",
	}
	return withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runForget(context.TODO(), options, pruneOpts, globalOptions, globalOptions.Term, args)
	})
}

func testRunForget(t testing.TB, globalOptions global.Options, options forgetOptions, args ...string) {
	rtest.OK(t, testRunForgetMayFail(t, globalOptions, options, args...))
}

func TestRunForgetSafetyNet(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)

	options := backupOptions{
		Host: "example",
	}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, options, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, options, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 2)

	// --keep-tags invalid
	err := testRunForgetMayFail(t, env.globalOptions, forgetOptions{
		KeepTags: data.TagLists{data.TagList{"invalid"}},
		GroupBy:  data.SnapshotGroupByOptions{Host: true, Path: true},
	})
	rtest.Assert(t, strings.Contains(err.Error(), `refusing to delete last snapshot of snapshot group "host example, path`), "wrong error message got %v", err)

	// disallow `forget --unsafe-allow-remove-all`
	err = testRunForgetMayFail(t, env.globalOptions, forgetOptions{
		UnsafeAllowRemoveAll: true,
	})
	rtest.Assert(
		t,
		strings.Contains(err.Error(), `--unsafe-allow-remove-all is not allowed unless a snapshot filter option is specified`),
		"wrong error message got %v",
		err,
	)

	// disallow `forget` without options
	err = testRunForgetMayFail(t, env.globalOptions, forgetOptions{})
	rtest.Assert(t, strings.Contains(err.Error(), `no policy was specified, no snapshots will be removed`), "wrong error message got %v", err)

	// `forget --host example --unsafe-allow-remove-all` should work
	testRunForget(t, env.globalOptions, forgetOptions{
		UnsafeAllowRemoveAll: true,
		GroupBy:              data.SnapshotGroupByOptions{Host: true, Path: true},
		SnapshotFilter: data.SnapshotFilter{
			Hosts: []string{options.Host},
		},
	})
	testListSnapshots(t, env.globalOptions, 0)
}

func TestForgetPhaseAAllowsBackupAndRevalidates(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.globalOptions.BackendTestHook = nil

	path := filepath.Join(env.testdata, "0", "0", "9")
	testRunBackup(t, "", []string{path}, backupOptions{Host: "forget-phase-a"}, env.globalOptions)
	testRunBackup(t, "", []string{path}, backupOptions{Host: "forget-phase-a"}, env.globalOptions)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	afterPhaseA := func() {
		once.Do(func() { close(entered) })
		<-release
	}

	forgetResult := make(chan error, 1)
	go func() {
		forgetResult <- withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runForgetWithPhaseACallback(ctx, forgetOptions{
				Last:    1,
				GroupBy: data.SnapshotGroupByOptions{Host: true, Path: true},
			}, pruneOptions{MaxUnused: "5%"}, globalOptions, globalOptions.Term, nil, afterPhaseA)
		})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("forget phase A did not complete")
	}

	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runBackup(ctx, backupOptions{Host: "forget-phase-a"}, globalOptions, globalOptions.Term, []string{path})
	}))
	close(release)
	rtest.OK(t, <-forgetResult)

	// The phase-A candidates were the first two snapshots. The snapshot created
	// during the unlocked policy phase is not in that candidate set and must
	// survive revalidation/deletion.
	testListSnapshots(t, env.globalOptions, 2)
	testRunPrune(t, env.globalOptions, pruneOptions{MaxUnused: "0%"})
	testRunCheck(t, env.globalOptions)
}
