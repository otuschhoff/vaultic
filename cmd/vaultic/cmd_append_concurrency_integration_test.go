package main

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/test"
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
	env.gopts.BackendTestHook = nil

	first := filepath.Join(env.testdata, "0", "0", "9", "2")
	second := filepath.Join(env.testdata, "0", "0", "9", "3")
	testRunBackup(t, "", []string{first}, BackupOptions{Host: "append-source-a"}, env.gopts)
	testRunBackup(t, "", []string{second}, BackupOptions{Host: "append-source-b"}, env.gopts)
	sources := testListSnapshots(t, env.gopts, 2)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for _, host := range []string{"append-backup-a", "append-backup-b"} {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			errs <- withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
				return runBackup(ctx, BackupOptions{Host: host}, gopts, gopts.Term, []string{first})
			})
		}(host)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			return runMerge(ctx, MergeOptions{Label: "append-concurrent-merge"}, gopts, []string{sources[0].String(), sources[1].String()}, gopts.Term)
		})
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		rtest.OK(t, err)
	}

	testListSnapshots(t, env.gopts, 5)
	testRunCheck(t, env.gopts)
}

func TestAppendBackupAndCopyConcurrency(t *testing.T) {
	destination, cleanupDestination := withTestEnvironment(t)
	defer cleanupDestination()
	source, cleanupSource := withTestEnvironment(t)
	defer cleanupSource()
	destination.gopts.BackendTestHook = nil
	source.gopts.BackendTestHook = nil
	testSetupBackupData(t, destination)
	testSetupBackupData(t, source)

	sourcePath := filepath.Join(source.testdata, "0", "0", "9")
	destinationPath := filepath.Join(destination.testdata, "0", "0", "9", "2")
	testRunBackup(t, "", []string{sourcePath}, BackupOptions{Host: "copy-source"}, source.gopts)
	sourceID := testListSnapshots(t, source.gopts, 1)[0]

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- withTermStatus(t, destination.gopts, func(ctx context.Context, gopts global.Options) error {
			return runBackup(ctx, BackupOptions{Host: "copy-concurrent-backup"}, gopts, gopts.Term, []string{destinationPath})
		})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- withTermStatus(t, destination.gopts, func(ctx context.Context, gopts global.Options) error {
			opts := CopyOptions{SecondaryRepoOptions: global.SecondaryRepoOptions{
				Repo:     source.repo,
				Password: test.TestPassword,
			}}
			return runCopy(ctx, opts, gopts, []string{sourceID.String()}, gopts.Term)
		})
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		rtest.OK(t, err)
	}

	testListSnapshots(t, destination.gopts, 2)
	testRunCheck(t, destination.gopts)
}
