package main

import (
	"bufio"
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunList(t testing.TB, globalOptions global.Options, params ...string) vaultic.IDs {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runList(ctx, globalOptions, params, globalOptions.Term, "")
	})
	rtest.OK(t, err)
	return parseIDsFromReader(t, buf)
}

func parseIDsFromReader(t testing.TB, reader io.Reader) vaultic.IDs {
	t.Helper()
	IDs := vaultic.IDs{}
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		if len(scanner.Text()) == 64 {
			id, err := vaultic.ParseID(scanner.Text())
			if err != nil {
				t.Logf("parse id %v: %v", scanner.Text(), err)
				continue
			}
			IDs = append(IDs, id)
		} else {
			// 'list blobs' is different because it lists the blobs together with the blob type
			// e.g. "tree ac08ce34ba4f8123618661bef2425f7028ffb9ac740578a3ee88684d2523fee8"
			parts := strings.Split(scanner.Text(), " ")
			id, err := vaultic.ParseID(parts[len(parts)-1])
			if err != nil {
				t.Logf("parse id %v: %v", scanner.Text(), err)
				continue
			}
			IDs = append(IDs, id)
		}
	}

	return IDs
}

func testListSnapshots(t testing.TB, globalOptions global.Options, expected int) vaultic.IDs {
	t.Helper()
	snapshotIDs := testRunList(t, globalOptions, "snapshots")
	rtest.Assert(t, len(snapshotIDs) == expected, "expected %v snapshot, got %v", expected, snapshotIDs)
	return snapshotIDs
}

// extract blob set from repository index
func testListBlobs(t testing.TB, globalOptions global.Options) (blobSetFromIndex vaultic.IDSet) {
	err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		// make sure the index is loaded
		rtest.OK(t, repo.LoadIndex(ctx, vaultic.NoopTerminalCounterFactory))

		// get blobs from index
		blobSetFromIndex = vaultic.NewIDSet()
		rtest.OK(t, repo.ListBlobs(ctx, func(blob vaultic.PackBlob) {
			blobSetFromIndex.Insert(blob.Handle().ID)
		}))
		return nil
	})
	rtest.OK(t, err)

	return blobSetFromIndex
}

func TestListBlobs(t *testing.T) {

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	options := backupOptions{}

	// first backup
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, options, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 1)

	// run the `list blobs` command
	resticIDs := testRunList(t, env.globalOptions, "blobs")

	// convert to set
	testIDSet := vaultic.NewIDSet(resticIDs...)
	blobSetFromIndex := testListBlobs(t, env.globalOptions)

	rtest.Assert(t, blobSetFromIndex.Equals(testIDSet), "the set of vaultic.ID s should be equal")
}

func TestPackfileListWithSnapshot(t *testing.T) {
	// setup
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)

	// 3 backups, single file each
	options := backupOptions{}
	testRunBackup(t, env.testdata, []string{filepath.Join(env.testdata, "0", "0", "9", "40")}, options, env.globalOptions)
	testRunBackup(t, env.testdata, []string{filepath.Join(env.testdata, "0", "0", "9", "41")}, options, env.globalOptions)
	testRunBackup(t, env.testdata, []string{filepath.Join(env.testdata, "0", "0", "9", "42")}, options, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 3)

	// run packfilelist
	packfiles := testRunList(t, env.globalOptions, "packs")
	rtest.Assert(t, len(packfiles) == 6, "expected 6 packfiles in repository, got %d", len(packfiles))

	packfiles = testRunList(t, env.globalOptions, "packs", "latest")
	rtest.Assert(t, len(packfiles) == 2, "expected 2 packfiles in snapshot, got %d", len(packfiles))
}
