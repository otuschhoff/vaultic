package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend/all"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/termstatus"
)

// TestLockingCLIHelper runs vaultic as a separate OS process. The helper is
// intentionally a test binary rather than an in-process goroutine so the
// backend lock protocol, not processLock, coordinates concurrent writers.
func TestLockingCLIHelper(t *testing.T) {
	if os.Getenv("VAULTIC_LOCKING_HELPER") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("VAULTIC_LOCKING_ARGS")), &args); err != nil {
		os.Exit(2)
	}
	gopts := global.Options{Backends: all.Backends()}
	term, cancel := termstatus.Setup(os.Stdin, os.Stdout, os.Stderr, false)
	defer cancel()
	gopts.Term = term
	root := newRootCommand(&gopts)
	root.SetArgs(args)
	if err := root.ExecuteContext(context.Background()); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func runLockingCLI(t *testing.T, env []string, args ...string) *exec.Cmd {
	t.Helper()
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockingCLIHelper$")
	cmd.Env = append(os.Environ(), env...)
	cmd.Env = append(cmd.Env,
		"VAULTIC_LOCKING_HELPER=1",
		"VAULTIC_LOCKING_ARGS="+string(encodedArgs),
	)
	return cmd
}

func TestCrossProcessAppendBackups(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	env.gopts.BackendTestHook = nil

	pathA := filepath.Join(env.testdata, "0", "0", "9", "2")
	pathB := filepath.Join(env.testdata, "0", "0", "9", "3")
	commonEnv := []string{
		"VAULTIC_REPOSITORY=" + env.repo,
		"VAULTIC_PASSWORD=" + test.TestPassword,
		"VAULTIC_CACHE_DIR=" + env.cache,
	}
	first := runLockingCLI(t, commonEnv, "backup", "--host", "subprocess-a", pathA)
	second := runLockingCLI(t, commonEnv, "backup", "--host", "subprocess-b", pathB)
	var firstOutput, secondOutput bytes.Buffer
	first.Stdout, first.Stderr = &firstOutput, &firstOutput
	second.Stdout, second.Stderr = &secondOutput, &secondOutput
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err != nil {
		t.Fatalf("first subprocess backup failed: %v\n%s", err, firstOutput.String())
	}
	if err := second.Wait(); err != nil {
		t.Fatalf("second subprocess backup failed: %v\n%s", err, secondOutput.String())
	}
	testListSnapshots(t, env.gopts, 2)
	testRunCheck(t, env.gopts)
}

func TestCrossProcessBackupAndPrune(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	createPrunableRepo(t, env)
	env.gopts.BackendTestHook = nil

	path := filepath.Join(env.testdata, "0", "0", "9", "2")
	commonEnv := []string{
		"VAULTIC_REPOSITORY=" + env.repo,
		"VAULTIC_PASSWORD=" + test.TestPassword,
		"VAULTIC_CACHE_DIR=" + env.cache,
	}
	prune := runLockingCLI(t, commonEnv, "prune", "--retry-lock", "10s", "--max-unused", "0%")
	backup := runLockingCLI(t, commonEnv, "backup", "--host", "subprocess-prune-backup", path)
	var pruneOutput, backupOutput bytes.Buffer
	prune.Stdout, prune.Stderr = &pruneOutput, &pruneOutput
	backup.Stdout, backup.Stderr = &backupOutput, &backupOutput
	if err := prune.Start(); err != nil {
		t.Fatal(err)
	}
	if err := backup.Start(); err != nil {
		t.Fatal(err)
	}
	if err := prune.Wait(); err != nil && !isExpectedLockConflict(pruneOutput.String()) {
		t.Fatalf("subprocess prune failed: %v\n%s", err, pruneOutput.String())
	}
	if err := backup.Wait(); err != nil && !isExpectedLockConflict(backupOutput.String()) {
		t.Fatalf("subprocess backup failed unexpectedly: %v\n%s", err, backupOutput.String())
	}

	// Either the append backup completed or it cleanly observed the short
	// exclusive phase-B window. In both cases prune must leave a clean repo.
	testRunCheck(t, env.gopts)
}

func isExpectedLockConflict(output string) bool {
	return strings.Contains(output, "repository is already locked") || strings.Contains(output, "repo already locked")
}
