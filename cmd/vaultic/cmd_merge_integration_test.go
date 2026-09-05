package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
)

func TestMergeSnapshots(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)

	left := filepath.Join(env.testdata, "0", "0", "9", "2")
	right := filepath.Join(env.testdata, "0", "0", "9", "3")
	testRunBackup(t, "", []string{left}, backupOptions{}, env.globalOptions)
	testRunBackup(t, "", []string{right}, backupOptions{}, env.globalOptions)
	sources := testListSnapshots(t, env.globalOptions, 2)
	env.globalOptions.BackendTestHook = nil

	if err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runMerge(ctx, mergeOptions{Label: "merged"}, globalOptions, []string{sources[0].String(), sources[1].String()}, globalOptions.Term)
	}); err != nil {
		t.Fatal(err)
	}
	ids := testListSnapshots(t, env.globalOptions, 3)
	mergedID := ids[0]

	if err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, 0, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		for _, id := range ids {
			snapshot, err := data.LoadSnapshot(ctx, repo, id)
			if err != nil {
				return err
			}
			if snapshot.Label == "merged" && len(snapshot.MergedSnapshots) == 2 {
				mergedID = id
				return nil
			}
		}
		return fmt.Errorf("merged snapshot not found")
	}); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(env.base, "merged-restore")
	testRunRestore(t, env.globalOptions, target, mergedID.String())
	if _, err := filepath.Glob(filepath.Join(target, "**")); err != nil {
		t.Fatal(err)
	}
}
