package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunCopy(t testing.TB, srcGopts global.Options, dstGopts global.Options) {
	globalOptions := srcGopts
	globalOptions.Repo = dstGopts.Repo
	globalOptions.Password = dstGopts.Password
	globalOptions.InsecureNoPassword = dstGopts.InsecureNoPassword
	copyOpts := copyOptions{
		SecondaryRepoOptions: global.SecondaryRepoOptions{
			Repo:               srcGopts.Repo,
			Password:           srcGopts.Password,
			InsecureNoPassword: srcGopts.InsecureNoPassword,
		},
	}

	rtest.OK(t, withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runCopy(context.TODO(), copyOpts, globalOptions, nil, globalOptions.Term)
	}))
}

func TestCopy(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	options := backupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, options, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, options, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, options, env.globalOptions)
	testRunCheck(t, env.globalOptions)

	testRunInit(t, env2.globalOptions)
	testRunCopy(t, env.globalOptions, env2.globalOptions)

	snapshotIDs := testListSnapshots(t, env.globalOptions, 3)
	copiedSnapshotIDs := testListSnapshots(t, env2.globalOptions, 3)

	// Check that the copies size seems reasonable
	stat := dirStats(t, env.repo)
	stat2 := dirStats(t, env2.repo)
	sizeDiff := int64(stat.size) - int64(stat2.size)
	if sizeDiff < 0 {
		sizeDiff = -sizeDiff
	}
	rtest.Assert(t, sizeDiff < int64(stat.size)/50, "expected less than 2%% size difference: %v vs. %v",
		stat.size, stat2.size)

	// Check integrity of the copy
	testRunCheck(t, env2.globalOptions)

	// Check that the copied snapshots have the same tree contents as the old ones (= identical tree hash)
	origRestores := make(map[string]struct{})
	for i, snapshotID := range snapshotIDs {
		restoredir := filepath.Join(env.base, fmt.Sprintf("restore%d", i))
		origRestores[restoredir] = struct{}{}
		testRunRestore(t, env.globalOptions, restoredir, snapshotID.String())
	}
	for i, snapshotID := range copiedSnapshotIDs {
		restoredir := filepath.Join(env2.base, fmt.Sprintf("restore%d", i))
		testRunRestore(t, env2.globalOptions, restoredir, snapshotID.String())
		foundMatch := false
		for cmpdir := range origRestores {
			diff := directoriesContentsDiff(t, restoredir, cmpdir)
			if diff == "" {
				delete(origRestores, cmpdir)
				foundMatch = true
			}
		}

		rtest.Assert(t, foundMatch, "found no counterpart for snapshot %v", snapshotID)
	}

	rtest.Assert(t, len(origRestores) == 0, "found not copied snapshots")

	// check that snapshots were properly batched while copying
	_, _, countBlobs := testPackAndBlobCounts(t, env.globalOptions)
	countTreePacksDst, countDataPacksDst, countBlobsDst := testPackAndBlobCounts(t, env2.globalOptions)

	rtest.Equals(t, countBlobs, countBlobsDst, "expected blob count in both repos to be equal")
	rtest.Equals(t, countTreePacksDst, 1, "expected 1 tree packfile")
	rtest.Equals(t, countDataPacksDst, 1, "expected 1 data packfile")
}

func testPackAndBlobCounts(t testing.TB, globalOptions global.Options) (countTreePacks int, countDataPacks int, countBlobs int) {
	rtest.OK(t, withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		rtest.OK(t, repo.List(context.TODO(), vaultic.PackFile, func(id vaultic.ID, size int64) error {
			handles, err := repo.ListPackHandles(context.TODO(), id, size)
			rtest.OK(t, err)
			rtest.Assert(t, len(handles) > 0, "a packfile should contain at least one blob")

			switch handles[0].Type {
			case vaultic.TreeBlob:
				countTreePacks++
			case vaultic.DataBlob:
				countDataPacks++
			}
			countBlobs += len(handles)
			return nil
		}))
		return nil
	}))

	return countTreePacks, countDataPacks, countBlobs
}

func TestCopyIncremental(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	options := backupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, options, env.globalOptions)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, options, env.globalOptions)
	testRunCheck(t, env.globalOptions)

	testRunInit(t, env2.globalOptions)
	testRunCopy(t, env.globalOptions, env2.globalOptions)

	testListSnapshots(t, env.globalOptions, 2)
	testListSnapshots(t, env2.globalOptions, 2)

	// Check that the copies size seems reasonable
	testRunCheck(t, env2.globalOptions)

	// check that no snapshots are copied, as there are no new ones
	testRunCopy(t, env.globalOptions, env2.globalOptions)
	testRunCheck(t, env2.globalOptions)
	testListSnapshots(t, env2.globalOptions, 2)

	// check that only new snapshots are copied
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, options, env.globalOptions)
	testRunCopy(t, env.globalOptions, env2.globalOptions)
	testRunCheck(t, env2.globalOptions)
	testListSnapshots(t, env.globalOptions, 3)
	testListSnapshots(t, env2.globalOptions, 3)

	// also test the reverse direction
	testRunCopy(t, env2.globalOptions, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 3)
}

func TestCopyUnstableJSON(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	// contains a symlink created using `ln -s '../i/'$'\355\246\361''d/samba' broken-symlink`
	datafile := filepath.Join("testdata", "copy-unstable-json.tar.gz")
	rtest.SetupTarTestFixture(t, env.base, datafile)

	testRunInit(t, env2.globalOptions)
	testRunCopy(t, env.globalOptions, env2.globalOptions)
	testRunCheck(t, env2.globalOptions)
	testListSnapshots(t, env2.globalOptions, 1)
}

func TestCopyToEmptyPassword(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()
	env2.globalOptions.Password = ""
	env2.globalOptions.InsecureNoPassword = true

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, backupOptions{}, env.globalOptions)

	testRunInit(t, env2.globalOptions)
	testRunCopy(t, env.globalOptions, env2.globalOptions)

	testListSnapshots(t, env.globalOptions, 1)
	testListSnapshots(t, env2.globalOptions, 1)
	testRunCheck(t, env2.globalOptions)
}
