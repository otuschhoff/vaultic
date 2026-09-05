package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunSnapshots(t testing.TB, globalOptions global.Options) (newest *Snapshot, snapmap map[vaultic.ID]Snapshot) {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = true

		options := snapshotOptions{}
		return runSnapshots(ctx, options, globalOptions, []string{}, globalOptions.Term)
	})
	rtest.OK(t, err)

	snapshots := []Snapshot{}
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &snapshots))

	snapmap = make(map[vaultic.ID]Snapshot, len(snapshots))
	for _, sn := range snapshots {
		snapmap[*sn.ID] = sn
		if newest == nil || sn.Time.After(newest.Time) {
			newest = &sn
		}
	}
	return
}

func snapshotsGroupTestData(t *testing.T, env *testEnvironment, keepPath bool) string {
	testSetupBackupData(t, env)
	// two backups on the same host but with different paths
	options := backupOptions{Host: "testhost", TimeStamp: time.Now().Format(time.DateTime)}
	testRunBackup(t, filepath.Dir(env.testdata), []string{"testdata"}, options, env.globalOptions)
	// Use later timestamp for second backup
	options.TimeStamp = time.Now().Add(time.Second).Format(time.DateTime)
	snapshotsIDs := loadSnapshotMap(t, env.globalOptions)

	targets := []string{"testdata/0"}
	if keepPath {
		targets = []string{"testdata"}
	}
	testRunBackup(t, filepath.Dir(env.testdata), targets, options, env.globalOptions)
	_, secondSnapshotID := lastSnapshot(snapshotsIDs, loadSnapshotMap(t, env.globalOptions))

	return secondSnapshotID
}

func TestSnapshotsGroupByAndLatest(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	secondSnapshotID := snapshotsGroupTestData(t, env, false)
	buf, err := withCaptureStdout(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = true
		// only group by host but not path
		options := snapshotOptions{GroupBy: data.SnapshotGroupByOptions{Host: true}, Latest: 1}
		return runSnapshots(ctx, options, globalOptions, []string{}, globalOptions.Term)
	})
	rtest.OK(t, err)
	snapshots := []SnapshotGroup{}
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &snapshots))
	rtest.Assert(t, len(snapshots) == 1, "expected only one snapshot group, got %d", len(snapshots))
	rtest.Assert(t, snapshots[0].GroupKey.Hostname == "testhost", "expected group_key.hostname to be set to testhost, got %s", snapshots[0].GroupKey.Hostname)
	rtest.Assert(t, snapshots[0].GroupKey.Paths == nil, "expected group_key.paths to be set to nil, got %s", snapshots[0].GroupKey.Paths)
	rtest.Assert(t, snapshots[0].GroupKey.Tags == nil, "expected group_key.tags to be set to nil, got %s", snapshots[0].GroupKey.Tags)
	rtest.Assert(t, len(snapshots[0].Snapshots) == 1, "expected only one latest snapshot, got %d", len(snapshots[0].Snapshots))
	rtest.Equals(t, snapshots[0].Snapshots[0].ID.String(), secondSnapshotID, "unexpected snapshot ID")
}

func TestSnapshotsLatest(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	secondSnapshotID := snapshotsGroupTestData(t, env, true)

	buf, err := withCaptureStdout(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = true
		options := snapshotOptions{Latest: 1}
		return runSnapshots(ctx, options, globalOptions, []string{}, globalOptions.Term)
	})
	rtest.OK(t, err)
	snapshots := []Snapshot{}
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &snapshots))
	rtest.Assert(t, len(snapshots) == 1, "expected only one snapshot, got %d", len(snapshots))
	rtest.Equals(t, snapshots[0].ID.String(), secondSnapshotID, "unexpected snapshot ID")
}
