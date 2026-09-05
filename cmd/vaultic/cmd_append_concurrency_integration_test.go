package main

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

// TestAppendWriterConcurrency verifies that the Shared policy permits additive
// writers to run together while still producing a repository that check can
// validate. The test intentionally leaves the lock-free feature disabled:
// Stage 2 proves additive writer behavior under shared locks; removing those
// locks requires the later prune-plan revalidation work.
func TestAppendWriterConcurrency(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.globalOptions.BackendTestHook = nil

	first := filepath.Join(env.testdata, "0", "0", "9", "2")
	second := filepath.Join(env.testdata, "0", "0", "9", "3")
	testRunBackup(t, "", []string{first}, backupOptions{Host: "append-source-a"}, env.globalOptions)
	testRunBackup(t, "", []string{second}, backupOptions{Host: "append-source-b"}, env.globalOptions)
	sources := testListSnapshots(t, env.globalOptions, 2)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for _, host := range []string{"append-backup-a", "append-backup-b"} {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			errs <- withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
				return runBackup(ctx, backupOptions{Host: host}, globalOptions, globalOptions.Term, []string{first})
			})
		}(host)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runMerge(
				ctx, mergeOptions{Label: "append-concurrent-merge"}, globalOptions,
				[]string{sources[0].String(), sources[1].String()}, globalOptions.Term,
			)
		})
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		rtest.OK(t, err)
	}

	testListSnapshots(t, env.globalOptions, 5)
	testRunCheck(t, env.globalOptions)
}

func TestAppendBackupAndCopyConcurrency(t *testing.T) {
	destination, cleanupDestination := withTestEnvironment(t)
	defer cleanupDestination()
	source, cleanupSource := withTestEnvironment(t)
	defer cleanupSource()
	destination.globalOptions.BackendTestHook = nil
	source.globalOptions.BackendTestHook = nil
	testSetupBackupData(t, destination)
	testSetupBackupData(t, source)

	sourcePath := filepath.Join(source.testdata, "0", "0", "9")
	destinationPath := filepath.Join(destination.testdata, "0", "0", "9", "2")
	testRunBackup(t, "", []string{sourcePath}, backupOptions{Host: "copy-source"}, source.globalOptions)
	sourceID := testListSnapshots(t, source.globalOptions, 1)[0]

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- withTermStatus(t, destination.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runBackup(ctx, backupOptions{Host: "copy-concurrent-backup"}, globalOptions, globalOptions.Term, []string{destinationPath})
		})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- withTermStatus(t, destination.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			options := copyOptions{SecondaryRepoOptions: global.SecondaryRepoOptions{
				Repo:     source.repo,
				Password: rtest.TestPassword,
			}}
			return runCopy(ctx, options, globalOptions, []string{sourceID.String()}, globalOptions.Term)
		})
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		rtest.OK(t, err)
	}

	testListSnapshots(t, destination.globalOptions, 2)
	testRunCheck(t, destination.globalOptions)
}
