package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/filter"
	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunRewriteExclude(t testing.TB, globalOptions global.Options, excludes []string, forget bool, metadata snapshotMetadataArgs) {
	options := rewriteOptions{
		ExcludePatternOptions: filter.ExcludePatternOptions{
			Excludes: excludes,
		},
		Forget:   forget,
		Metadata: metadata,
	}

	rtest.OK(t, withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRewrite(context.TODO(), options, globalOptions, nil, globalOptions.Term)
	}))
}

func testRunRewriteWithOpts(t testing.TB, options rewriteOptions, globalOptions global.Options, args []string) error {
	rtest.OK(t, withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRewrite(context.TODO(), options, globalOptions, args, globalOptions.Term)
	}))
	return nil
}

// testLsOutputContainsCount runs vaultic ls with the given options and asserts that
// exactly expectedCount lines of the output contain substring.
func testLsOutputContainsCount(t testing.TB, globalOptions global.Options, lsOpts lsOptions, lsArgs []string, substring string, expectedCount int) {
	t.Helper()
	out := testRunLsWithOpts(t, globalOptions, lsOpts, lsArgs)
	count := 0
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.Contains(line, substring) {
			count++
		}
	}
	rtest.Assert(t, count == expectedCount, "expected %d lines containing %q, but got %d", expectedCount, substring, count)
}

func createBasicRewriteRepo(t testing.TB, env *testEnvironment) vaultic.ID {
	testSetupBackupData(t, env)

	// create backup
	testRunBackup(t, filepath.Dir(env.testdata), []string{"testdata"}, backupOptions{}, env.globalOptions)
	snapshotIDs := testRunList(t, env.globalOptions, "snapshots")
	rtest.Assert(t, len(snapshotIDs) == 1, "expected one snapshot, got %v", snapshotIDs)
	testRunCheck(t, env.globalOptions)

	return snapshotIDs[0]
}

func createBasicRewriteRepoWithEmptyDirectory(t testing.TB, env *testEnvironment) vaultic.ID {
	testSetupBackupData(t, env)

	// make an empty directory named "empty-directory"
	rtest.OK(t, os.Mkdir(filepath.Join(env.testdata, "/0/tests", "empty-directory"), 0755))

	// create backup
	testRunBackup(t, filepath.Dir(env.testdata), []string{"testdata"}, backupOptions{}, env.globalOptions)
	snapshotIDs := testRunList(t, env.globalOptions, "snapshots")
	rtest.Assert(t, len(snapshotIDs) == 1, "expected one snapshot, got %v", snapshotIDs)

	return snapshotIDs[0]
}

func getSnapshot(t testing.TB, snapshotID vaultic.ID, env *testEnvironment) *data.Snapshot {
	t.Helper()

	var snapshots []*data.Snapshot
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		snapshots, err = data.TestLoadAllSnapshots(ctx, repo, nil)
		return err
	})
	rtest.OK(t, err)

	for _, s := range snapshots {
		if *s.ID() == snapshotID {
			return s
		}
	}
	return nil
}

func TestRewrite(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	createBasicRewriteRepo(t, env)

	// exclude some data
	testRunRewriteExclude(t, env.globalOptions, []string{"3"}, false, snapshotMetadataArgs{Hostname: "", Time: ""})
	snapshotIDs := testRunList(t, env.globalOptions, "snapshots")
	rtest.Assert(t, len(snapshotIDs) == 2, "expected two snapshots, got %v", snapshotIDs)
	testRunCheck(t, env.globalOptions)
}

func TestRewriteUnchanged(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	snapshotID := createBasicRewriteRepo(t, env)

	// use an exclude that will not exclude anything
	testRunRewriteExclude(t, env.globalOptions, []string{"3dflkhjgdflhkjetrlkhjgfdlhkj"}, false, snapshotMetadataArgs{Hostname: "", Time: ""})
	newSnapshotIDs := testRunList(t, env.globalOptions, "snapshots")
	rtest.Assert(t, len(newSnapshotIDs) == 1, "expected one snapshot, got %v", newSnapshotIDs)
	rtest.Assert(t, snapshotID == newSnapshotIDs[0], "snapshot id changed unexpectedly")
	testRunCheck(t, env.globalOptions)
}

func TestRewriteReplace(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	snapshotID := createBasicRewriteRepo(t, env)

	snapshot := getSnapshot(t, snapshotID, env)

	// exclude some data
	testRunRewriteExclude(t, env.globalOptions, []string{"3"}, true, snapshotMetadataArgs{Hostname: "", Time: ""})
	bytesExcluded, err := ui.ParseBytes("16K")
	rtest.OK(t, err)

	newSnapshotIDs := testListSnapshots(t, env.globalOptions, 1)
	rtest.Assert(t, snapshotID != newSnapshotIDs[0], "snapshot id should have changed")

	newSnapshot := getSnapshot(t, newSnapshotIDs[0], env)

	rtest.Equals(t, snapshot.Summary.TotalFilesProcessed-1, newSnapshot.Summary.TotalFilesProcessed, "snapshot file count should have changed")
	rtest.Equals(t, snapshot.Summary.TotalBytesProcessed-uint64(bytesExcluded), newSnapshot.Summary.TotalBytesProcessed, "snapshot size should have changed")

	// check forbids unused blobs, thus remove them first
	testRunPrune(t, env.globalOptions, pruneOptions{MaxUnused: "0"})
	testRunCheck(t, env.globalOptions)
}

func testRewriteMetadata(t *testing.T, metadata snapshotMetadataArgs) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	createBasicRewriteRepo(t, env)
	testRunRewriteExclude(t, env.globalOptions, []string{}, true, metadata)

	var snapshots []*data.Snapshot
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		snapshots, err = data.TestLoadAllSnapshots(ctx, repo, nil)
		return err
	})
	rtest.OK(t, err)
	rtest.Assert(t, len(snapshots) == 1, "expected one snapshot, got %v", len(snapshots))
	newSnapshot := snapshots[0]

	if metadata.Time != "" {
		rtest.Assert(t, newSnapshot.Time.Format(global.TimeFormat) == metadata.Time, "New snapshot should have time %s", metadata.Time)
	}

	if metadata.Hostname != "" {
		rtest.Assert(t, newSnapshot.Hostname == metadata.Hostname, "New snapshot should have host %s", metadata.Hostname)
	}
}

func TestRewriteMetadata(t *testing.T) {
	newHost := "new host"
	newTime := "1999-01-01 11:11:11"

	for _, metadata := range []snapshotMetadataArgs{
		{Hostname: "", Time: newTime},
		{Hostname: newHost, Time: ""},
		{Hostname: newHost, Time: newTime},
	} {
		testRewriteMetadata(t, metadata)
	}
}

func TestRewriteSnaphotSummary(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	createBasicRewriteRepo(t, env)

	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRewrite(ctx, rewriteOptions{SnapshotSummary: true}, globalOptions, []string{}, globalOptions.Term)
	}))
	// no new snapshot should be created as the snapshot already has a summary
	snapshots := testListSnapshots(t, env.globalOptions, 1)

	// replace snapshot by one without a summary
	var oldSummary *data.SnapshotSummary
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		sn, err := data.LoadSnapshot(ctx, repo, snapshots[0])
		rtest.OK(t, err)
		oldSummary = sn.Summary
		sn.Summary = nil
		rtest.OK(t, repo.RemoveUnpacked(ctx, vaultic.WriteableSnapshotFile, snapshots[0]))
		snapshots[0], err = data.SaveSnapshot(ctx, repo, sn)
		return err
	})
	rtest.OK(t, err)

	// rewrite snapshot and lookup ID of new snapshot
	rtest.OK(t, withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRewrite(ctx, rewriteOptions{SnapshotSummary: true}, globalOptions, []string{}, globalOptions.Term)
	}))
	newSnapshots := testListSnapshots(t, env.globalOptions, 2)
	newSnapshot := vaultic.NewIDSet(newSnapshots...).Sub(vaultic.NewIDSet(snapshots...)).List()[0]

	newSn := testLoadSnapshot(t, env.globalOptions, newSnapshot)
	rtest.Assert(t, newSn.Summary != nil, "snapshot should have summary attached")
	rtest.Equals(t, oldSummary.TotalBytesProcessed, newSn.Summary.TotalBytesProcessed, "unexpected TotalBytesProcessed value")
	rtest.Equals(t, oldSummary.TotalFilesProcessed, newSn.Summary.TotalFilesProcessed, "unexpected TotalFilesProcessed value")
}

func TestRewriteInclude(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		options              rewriteOptions
		lsSubstring          string
		lsExpectedCount      int
		summaryFilesExpected uint
	}{
		{"relative", rewriteOptions{
			Forget:                true,
			IncludePatternOptions: filter.IncludePatternOptions{Includes: []string{"*.txt"}},
		}, ".txt", 2, 2},
		{"absolute", rewriteOptions{
			Forget: true,
			// test that childMatches are working by only matching a subdirectory
			IncludePatternOptions: filter.IncludePatternOptions{Includes: []string{"/testdata/0/for_cmd_ls"}},
		}, "/testdata/0", 5, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, cleanup := withTestEnvironment(t)
			defer cleanup()
			createBasicRewriteRepo(t, env)
			snapshots := testListSnapshots(t, env.globalOptions, 1)

			rtest.OK(t, testRunRewriteWithOpts(t, tc.options, env.globalOptions, []string{"latest"}))

			newSnapshots := testListSnapshots(t, env.globalOptions, 1)
			rtest.Assert(t, snapshots[0] != newSnapshots[0], "snapshot id should have changed")

			testLsOutputContainsCount(t, env.globalOptions, lsOptions{}, []string{"latest"}, tc.lsSubstring, tc.lsExpectedCount)
			sn := testLoadSnapshot(t, env.globalOptions, newSnapshots[0])
			rtest.Assert(t, sn.Summary != nil, "snapshot should have a summary attached")
			rtest.Assert(t, sn.Summary.TotalFilesProcessed == tc.summaryFilesExpected,
				"there should be %d files in the snapshot, but there are %d files", tc.summaryFilesExpected, sn.Summary.TotalFilesProcessed)
		})
	}
}

func TestRewriteExcludeFiles(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	createBasicRewriteRepo(t, env)
	snapshots := testListSnapshots(t, env.globalOptions, 1)

	// exclude txt files
	err := testRunRewriteWithOpts(t,
		rewriteOptions{
			Forget:                true,
			ExcludePatternOptions: filter.ExcludePatternOptions{Excludes: []string{"*.txt"}},
		},
		env.globalOptions,
		[]string{"latest"})
	rtest.OK(t, err)
	newSnapshots := testListSnapshots(t, env.globalOptions, 1)
	rtest.Assert(t, snapshots[0] != newSnapshots[0], "snapshot id should have changed")

	testLsOutputContainsCount(t, env.globalOptions, lsOptions{}, []string{"latest"}, ".txt", 0)
}

func TestRewriteExcludeIncludeContradiction(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)

	// test contradiction
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRewrite(ctx,
			rewriteOptions{
				ExcludePatternOptions: filter.ExcludePatternOptions{Excludes: []string{"nonsense"}},
				IncludePatternOptions: filter.IncludePatternOptions{Includes: []string{"not allowed"}},
			},
			globalOptions, []string{"quack"}, env.globalOptions.Term)
	})
	rtest.Assert(
		t,
		err != nil && strings.Contains(err.Error(), "exclude and include patterns are mutually exclusive"),
		`expected to fail command with message "exclude and include patterns are mutually exclusive"`,
	)
}

func TestRewriteIncludeEmptyDirectory(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	snapIDEmpty := createBasicRewriteRepoWithEmptyDirectory(t, env)

	// vaultic rewrite <snapshots[0]> -i empty-directory --forget
	// exclude txt files
	err := testRunRewriteWithOpts(t,
		rewriteOptions{
			Forget:                true,
			IncludePatternOptions: filter.IncludePatternOptions{Includes: []string{"empty-directory"}},
		},
		env.globalOptions,
		[]string{"latest"})
	rtest.OK(t, err)
	newSnapshots := testListSnapshots(t, env.globalOptions, 1)
	rtest.Assert(t, snapIDEmpty != newSnapshots[0], "snapshot id should have changed")

	testLsOutputContainsCount(t, env.globalOptions, lsOptions{}, []string{"latest"}, "empty-directory", 1)
}

// TestRewriteIncludeNothing makes sure when nothing is included, the original snapshot stays untouched
func TestRewriteIncludeNothing(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	createBasicRewriteRepo(t, env)
	snapsBefore := testListSnapshots(t, env.globalOptions, 1)

	// vaultic rewrite latest -i nothing-whatsoever --forget
	err := testRunRewriteWithOpts(t,
		rewriteOptions{
			Forget:                true,
			IncludePatternOptions: filter.IncludePatternOptions{Includes: []string{"nothing-whatsoever"}},
		},
		env.globalOptions,
		[]string{"latest"})
	rtest.OK(t, err)

	snapsAfter := testListSnapshots(t, env.globalOptions, 1)
	rtest.Assert(t, snapsBefore[0] == snapsAfter[0], "snapshots should be identical but are %s and %s",
		snapsBefore[0].Str(), snapsAfter[0].Str())
}
