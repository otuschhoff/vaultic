//go:build darwin || freebsd || linux

package nfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestNativeNFSMountIntegration(t *testing.T) {
	if os.Getenv("VAULTIC_NFS_INTEGRATION_TESTS") == "" {
		t.Skip("set VAULTIC_NFS_INTEGRATION_TESTS=1 to enable privileged native NFS mount tests")
	}
	if os.Geteuid() != 0 {
		t.Skip("native NFS mount test requires root privileges")
	}

	mountTool := "mount_nfs"
	if runtime.GOOS == "linux" {
		mountTool = "mount"
	}
	mountPath, err := exec.LookPath(mountTool)
	if err != nil {
		t.Skipf("native NFS mount tool %q is unavailable: %v", mountTool, err)
	}
	unmountPath, err := exec.LookPath("umount")
	if err != nil {
		t.Skipf("native unmount tool is unavailable: %v", err)
	}

	fs, payload := testFilesystem(t, 64)
	server, err := New(fs.fs, Config{EphemeralPorts: true, MaxReadSize: 64, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() {
		cancel()
		_ = server.Close() // Preserve any test failure; shutdown cleanup is best effort.
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	}()
	addresses := waitReady(t, server)

	mountpoint := t.TempDir()
	mounted := false
	defer func() {
		if mounted {
			output, err := exec.Command(unmountPath, mountpoint).CombinedOutput()
			if err != nil {
				t.Errorf("unmount %s: %v: %s", mountpoint, err, output)
			}
		}
	}()
	host, nfsPort, _ := net.SplitHostPort(addresses.NFS)
	_, mountPort, _ := net.SplitHostPort(addresses.Mount)
	remote := host + ":/snapshot"
	if net.ParseIP(host).To4() == nil {
		remote = "[" + host + "]:/snapshot"
	}
	options := fmt.Sprintf("vers=3,proto=tcp,port=%s,mountport=%s,ro,nolock", nfsPort, mountPort)
	args := []string{"-o", options, remote, mountpoint}
	if runtime.GOOS == "linux" {
		args = append([]string{"-t", "nfs"}, args...)
	}
	output, err := exec.Command(mountPath, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("native NFS mount failed: %v: %s", err, output)
	}
	mounted = true

	entries, err := os.ReadDir(mountpoint)
	if err != nil || len(entries) == 0 {
		t.Fatalf("browse mounted export: entries=%d, err=%v", len(entries), err)
	}
	contents, err := os.ReadFile(filepath.Join(mountpoint, "a-file"))
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "a-file.copy")
	if err := os.WriteFile(copyPath, contents, 0600); err != nil {
		t.Fatal(err)
	}
	copied, err := os.ReadFile(copyPath)
	if err != nil || sha256.Sum256(copied) != sha256.Sum256(payload) {
		t.Fatalf("copied fixture hash mismatch: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mountpoint, "created"), []byte("mutation"), 0600); err == nil {
		t.Fatal("write to read-only NFS export succeeded")
	}
}
