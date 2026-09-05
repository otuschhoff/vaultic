package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
)

func TestProfileAppliesBackupAndGlobalFlags(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "nightly.toml")
	profile := "[global]\nquiet = true\n[backup]\nlabel = 'nightly'\nread-concurrency = 3\n"
	if err := os.WriteFile(profilePath, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}

	globalOptions := global.Options{}
	root := newRootCommand(&globalOptions)
	if err := root.PersistentFlags().Set("use-profile", profilePath); err != nil {
		t.Fatal(err)
	}
	backup, _, err := root.Find([]string{"backup"})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyProfile(backup, &globalOptions); err != nil {
		t.Fatal(err)
	}
	if !globalOptions.Quiet {
		t.Fatal("profile did not set global quiet flag")
	}
	if got := backup.Flags().Lookup("label").Value.String(); got != "nightly" {
		t.Fatalf("profile label = %q, want nightly", got)
	}
	if got := backup.Flags().Lookup("read-concurrency").Value.String(); got != "3" {
		t.Fatalf("profile read-concurrency = %q, want 3", got)
	}
}

func TestBackupPublishesInfluxV2Metrics(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)

	var line string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/write" || r.URL.Query().Get("org") != "vaultic" || r.URL.Query().Get("bucket") != "backups" {
			t.Errorf("unexpected Influx request %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Token secret" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(r.Body)
		line = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	env.globalOptions.InfluxURL = server.URL
	env.globalOptions.InfluxToken = "secret"
	env.globalOptions.InfluxOrg = "vaultic"
	env.globalOptions.InfluxBucket = "backups"
	if err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runBackup(ctx, backupOptions{Label: "nightly"}, globalOptions, globalOptions.Term, []string{filepath.Join(env.testdata, "0")})
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "vaultic_backup,") || !strings.Contains(line, "label=nightly") {
		t.Fatalf("unexpected Influx line protocol %q", line)
	}
}

func TestBackupInitCreatesMissingRepository(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)

	if err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runBackup(ctx, backupOptions{Init: true}, globalOptions, globalOptions.Term, []string{filepath.Join(env.testdata, "0")})
	}); err != nil {
		t.Fatal(err)
	}
	testListSnapshots(t, env.globalOptions, 1)
}
