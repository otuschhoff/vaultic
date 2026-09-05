package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// testRunRepairPacks runs `vaultic repair packs` with capturing stdout and stderr
func testRunRepairPacks(t testing.TB, globalOptions global.Options, args []string) (string, string, error) {
	bufStdout, bufStderr, err := withCaptureStdoutStderr(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRepairPacks(ctx, globalOptions, globalOptions.Term, args)
	})

	return bufStdout.String(), bufStderr.String(), err
}

func TestRunRepairPackfiles(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	// backup of subtree 0/0/9/42
	testRunBackup(t, env.testdata, []string{filepath.Join(env.testdata, "0", "0", "9", "42")}, backupOptions{}, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 1)

	packfileID := vaultic.ID{}
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		rtest.OK(t, repo.LoadIndex(ctx, printer))
		// load packfiles from master index
		err = repo.ListBlobs(ctx, func(blob vaultic.PackBlob) {
			if blob.Handle().Type == vaultic.DataBlob {
				packfileID = blob.PackID()
				return
			}
		})
		rtest.OK(t, err)

		return nil
	})
	rtest.OK(t, err)

	rtest.Assert(t, !packfileID.IsNull(), "expected valid packfile ID")
	packIDString := packfileID.String()
	filename := filepath.Join(env.globalOptions.Repo, "data", packIDString[0:2], packIDString)
	rtest.OK(t, os.Remove(filename))

	_, outError, err := testRunCheckOutput(t, env.globalOptions, false)
	rtest.Assert(t, err != nil, "expected check errors, got none")
	rtest.Assert(t, strings.Contains(string(outError), packIDString), "expected mention of %q", packIDString)

	// change to temporary directory to not pollute the repository with backup files
	cleanupChdir := rtest.Chdir(t, env.base)
	defer cleanupChdir()
	// vaultic repair packs 'packIDString'
	_, _, err = testRunRepairPacks(t, env.globalOptions, []string{packIDString})
	rtest.OK(t, err)

	// run vaultic repair snapshots --forget
	testRunRepairSnapshot(t, env.globalOptions, true)
	_, _, err = testRunCheckOutput(t, env.globalOptions, false)
	rtest.OK(t, err)
}
