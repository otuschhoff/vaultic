//go:build darwin || freebsd || linux || windows

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/nfs"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
	"github.com/spf13/cobra"
)

func TestServeNFSCommandDefaults(t *testing.T) {
	root := &cobra.Command{Use: "vaultic"}
	registerServeCommand(root, &global.Options{})
	command, _, err := root.Find([]string{"serve", "nfs"})
	if err != nil {
		t.Fatal(err)
	}
	if command.CommandPath() != "vaultic serve nfs" {
		t.Fatalf("command path = %q", command.CommandPath())
	}
	for name, want := range map[string]string{
		"listen": "127.0.0.1", "nfs-port": "20490", "mount-port": "20491",
		"export-name": "/snapshot", "owner": "preserved", "permissions": "preserved",
		"tree-cache-size": "16M", "blob-cache-size": "64M", "max-read-size": "1M",
		"idle-timeout": "0s", "graceful-drain-timeout": "5s",
	} {
		if got := command.Flags().Lookup(name).DefValue; got != want {
			t.Errorf("--%s default = %q, want %q", name, got, want)
		}
	}
}

func TestServeNFSOptionValidation(t *testing.T) {
	options := serveNFSOptions{
		Owner: "server", Permissions: "readable", TreeCacheSize: "16M", BlobCacheSize: "64M",
		MaxReadSize: "1M", MaxRequestSize: "20M", Listen: "127.0.0.1",
		NFSPort: nfs.DefaultNFSListenPort, MountPort: nfs.DefaultMountListenPort,
		ExportName: "/snapshot", GracefulDrainTimeout: time.Second,
		ConnectionIdleTimeout: time.Minute, MaxConnections: 1, MaxRequestsPerConnection: 1, HandleLimit: 1,
	}
	filesystem, err := nfsSnapshotConfig(options)
	if err != nil {
		t.Fatal(err)
	}
	if filesystem.Owner != snapshotfs.OwnerServer || filesystem.Permissions != snapshotfs.PermissionsReadable {
		t.Fatalf("snapshot config = %+v", filesystem)
	}
	if _, err := nfsServerConfig(options); err != nil {
		t.Fatal(err)
	}

	options.Owner = "unknown"
	if _, err := nfsSnapshotConfig(options); err == nil {
		t.Fatal("invalid owner accepted")
	}
	options.Owner = "preserved"
	options.Permissions = "unknown"
	if _, err := nfsSnapshotConfig(options); err == nil {
		t.Fatal("invalid permissions accepted")
	}
	if _, err := parseNFSSize("max-read-size", "not-a-size"); err == nil {
		t.Fatal("invalid size accepted")
	}
}

func TestWaitForNFSCleanup(t *testing.T) {
	done := make(chan struct{})
	close(done)
	if !waitForNFSCleanup(done, time.Second) {
		t.Fatal("completed cleanup timed out")
	}

	started := time.Now()
	if waitForNFSCleanup(make(chan struct{}), 10*time.Millisecond) {
		t.Fatal("incomplete cleanup reported completion")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cleanup timeout took %s", elapsed)
	}
}

func TestNFSReadinessFileOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready.json")
	started := time.Unix(1_700_000_000, 123).UTC()
	record := nfsReadinessRecord{
		Schema: nfsReadinessSchema, Version: nfsReadinessVersion, PID: os.Getpid(),
		RepositoryID: "repo", SnapshotID: "snapshot", Subfolder: "restore/data",
		NFSAddress: "127.0.0.1:20490", MountAddress: "127.0.0.1:20491",
		ExportName: "/snapshot", StartedAt: started,
	}
	if err := writeNFSReadinessFile(path, record); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("readiness mode = %o", info.Mode().Perm())
	}
	loaded, err := loadNFSReadinessFile(path)
	if err != nil || loaded != record {
		t.Fatalf("loaded readiness = %+v, %v", loaded, err)
	}
	if err := writeNFSReadinessFile(path, record); err == nil {
		t.Fatal("live readiness owner was overwritten")
	}
	liveOtherStart := record
	liveOtherStart.StartedAt = started.Add(-time.Hour)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(liveOtherStart)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeNFSReadinessFile(path, record); err == nil {
		t.Fatal("readiness record for live PID with another start time was overwritten")
	}

	stale := record
	stale.PID = -1
	stale.StartedAt = started.Add(-time.Hour)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := writeNFSReadinessFile(path, stale); err != nil {
		t.Fatal(err)
	}
	if err := writeNFSReadinessFile(path, record); err != nil {
		t.Fatalf("replace stale readiness: %v", err)
	}

	other := record
	other.StartedAt = started.Add(time.Second)
	if err := writeNFSReadinessFile(filepath.Join(path, "child"), other); err == nil {
		t.Fatal("missing readiness parent accepted")
	}
	removeNFSReadinessFile(path, other)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("non-owner removed readiness: %v", err)
	}
	removeNFSReadinessFile(path, record)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owner did not remove readiness: %v", err)
	}
}
