package main

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func testRunRecover(t testing.TB, globalOptions global.Options) {
	rtest.OK(t, withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRecover(context.TODO(), globalOptions, globalOptions.Term)
	}))
}

func TestRecover(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	// must list index more than once
	env.globalOptions.BackendTestHook = nil
	defer cleanup()

	testSetupBackupData(t, env)

	// create backup and forget it afterwards
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	ids := testListSnapshots(t, env.globalOptions, 1)
	sn := testLoadSnapshot(t, env.globalOptions, ids[0])
	testRunForget(t, env.globalOptions, forgetOptions{}, ids[0].String())
	testListSnapshots(t, env.globalOptions, 0)

	testRunRecover(t, env.globalOptions)
	ids = testListSnapshots(t, env.globalOptions, 1)
	testRunCheck(t, env.globalOptions)
	// check that the root tree is included in the snapshot
	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runCat(context.TODO(), globalOptions, []string{"tree", ids[0].String() + ":" + sn.Tree.Str()}, globalOptions.Term)
	}))
}
