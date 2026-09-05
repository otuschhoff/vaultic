package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
)

func TestRepoInfo(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil

	var result repoInfo
	buf, err := withCaptureStdout(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = true
		return runRepoInfo(ctx, globalOptions, globalOptions.Term)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Types["snapshot"].Count != 1 || result.Types["data"].Count == 0 || result.Total.Size == 0 {
		t.Fatalf("unexpected repository info: %#v", result)
	}
}
