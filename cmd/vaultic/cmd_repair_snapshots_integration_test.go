package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunRepairSnapshot(t testing.TB, globalOptions global.Options, forget bool) {
	options := repairOptions{
		Forget: forget,
	}

	rtest.OK(t, withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRepairSnapshots(context.TODO(), globalOptions, options, nil, globalOptions.Term)
	}))
}

func createRandomFile(t testing.TB, env *testEnvironment, path string, size int) {
	fn := filepath.Join(env.testdata, path)
	rtest.OK(t, os.MkdirAll(filepath.Dir(fn), 0o755))

	h := fnv.New64()
	_, err := h.Write([]byte(path))
	rtest.OK(t, err)
	r := rand.New(rand.NewSource(int64(h.Sum64())))

	f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0o644)
	rtest.OK(t, err)
	_, err = io.Copy(f, io.LimitReader(r, int64(size)))
	rtest.OK(t, err)
	rtest.OK(t, f.Close())
}

func TestRepairSnapshotsWithLostData(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	createRandomFile(t, env, "foo/bar/file", 512*1024)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 1)
	// damage repository
	removePacksExcept(env.globalOptions, t, vaultic.NewIDSet(), false)

	createRandomFile(t, env, "foo/bar/file2", 256*1024)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	snapshotIDs := testListSnapshots(t, env.globalOptions, 2)
	testRunCheckMustFail(t, env.globalOptions)

	// repair but keep broken snapshots
	testRunRebuildIndex(t, env.globalOptions)
	testRunRepairSnapshot(t, env.globalOptions, false)
	testListSnapshots(t, env.globalOptions, 4)
	testRunCheckMustFail(t, env.globalOptions)

	// repository must be ok after removing the broken snapshots
	testRunForget(t, env.globalOptions, forgetOptions{}, snapshotIDs[0].String(), snapshotIDs[1].String())
	testListSnapshots(t, env.globalOptions, 2)
	_, _, err := testRunCheckOutput(t, env.globalOptions, false)
	rtest.OK(t, err)
}

func TestRepairSnapshotsWithLostTree(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	createRandomFile(t, env, "foo/bar/file", 12345)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	oldSnapshot := testListSnapshots(t, env.globalOptions, 1)
	oldPacks := testRunList(t, env.globalOptions, "packs")

	// keep foo/bar unchanged
	createRandomFile(t, env, "foo/bar2", 1024)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 2)

	// remove tree for foo/bar and the now completely broken first snapshot
	removePacks(env.globalOptions, t, vaultic.NewIDSet(oldPacks...))
	testRunForget(t, env.globalOptions, forgetOptions{}, oldSnapshot[0].String())
	testRunCheckMustFail(t, env.globalOptions)

	// repair
	testRunRebuildIndex(t, env.globalOptions)
	testRunRepairSnapshot(t, env.globalOptions, true)
	testListSnapshots(t, env.globalOptions, 1)
	_, _, err := testRunCheckOutput(t, env.globalOptions, false)
	rtest.OK(t, err)
}

func TestRepairSnapshotsWithLostRootTree(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	createRandomFile(t, env, "foo/bar/file", 12345)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	testListSnapshots(t, env.globalOptions, 1)
	oldPacks := testRunList(t, env.globalOptions, "packs")

	// remove all trees
	removePacks(env.globalOptions, t, vaultic.NewIDSet(oldPacks...))
	testRunCheckMustFail(t, env.globalOptions)

	// repair
	testRunRebuildIndex(t, env.globalOptions)
	testRunRepairSnapshot(t, env.globalOptions, true)
	testListSnapshots(t, env.globalOptions, 0)
	_, _, err := testRunCheckOutput(t, env.globalOptions, false)
	rtest.OK(t, err)
}

func TestRepairSnapshotsIntact(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	testRunBackup(t, filepath.Dir(env.testdata), []string{"testdata"}, backupOptions{}, env.globalOptions)
	oldSnapshotIDs := testListSnapshots(t, env.globalOptions, 1)

	// use an exclude that will not exclude anything
	testRunRepairSnapshot(t, env.globalOptions, false)
	snapshotIDs := testListSnapshots(t, env.globalOptions, 1)
	rtest.Assert(t, reflect.DeepEqual(oldSnapshotIDs, snapshotIDs), "unexpected snapshot id mismatch %v vs. %v", oldSnapshotIDs, snapshotIDs)
	testRunCheck(t, env.globalOptions)
}

func TestRepairSnapshotsBrokenSnapshots(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	// create backup
	testRunBackup(t, filepath.Dir(env.testdata), []string{"testdata"}, backupOptions{}, env.globalOptions)

	// create zero length file in snapshots/
	// will fail with
	// failed to load snapshot 1d204771: LoadRaw(<snapshot/1d20477115>): invalid data returned
	handle, err := os.Create(filepath.Join(env.repo, "snapshots", "1d20477115fb872069a28a80ffb95a82cb8b1b1920de046a68c0195da63f30cf"))
	rtest.OK(t, err)
	rtest.OK(t, handle.Close())

	// create some file with a correct sha256 name in snapshots/, will fail with
	// failed to load snapshot abcd1234: ciphertext verification failed
	contents := rtest.Random(1234567890123, 42)
	sha256Contents := sha256.Sum256(contents)
	target := hex.EncodeToString(sha256Contents[:])
	rtest.OK(t, os.WriteFile(filepath.Join(env.repo, "snapshots", target), contents, 0o600))

	// run repair snapshots
	repairOpts := repairOptions{Forget: true}
	env.globalOptions.BackendTestHook = nil
	_, err = withCaptureStdout(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runRepairSnapshots(ctx, globalOptions, repairOpts, []string{"1d204771", target[:8]}, globalOptions.Term)
	})
	rtest.OK(t, err)

	// verify that there are no snapshot errors
	testRunCheck(t, env.globalOptions)
}
