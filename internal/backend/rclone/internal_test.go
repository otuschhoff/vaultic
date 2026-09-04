package rclone

import (
	"bytes"
	"context"
	"os/exec"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/errors"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

// vaultic should detect rclone exiting.
func TestRcloneExit(t *testing.T) {
	t.Parallel()
	requireRcloneStdio(t)
	dir := rtest.TempDir(t)
	cfg := NewConfig()
	cfg.Remote = dir
	be, err := Open(context.TODO(), cfg, nil, t.Logf)
	var e *exec.Error
	if errors.As(err, &e) && e.Err == exec.ErrNotFound {
		t.Skipf("program %q not found", e.Name)
		return
	}
	rtest.OK(t, err)
	defer func() {
		// ignore the error as the test will kill rclone (see below)
		_ = be.Close()
	}()

	err = be.(*rclone).cmd.Process.Kill()
	rtest.OK(t, err)
	t.Log("killed rclone")

	for range 10 {
		_, err = be.Stat(context.TODO(), backend.Handle{
			Name: "foo",
			Type: backend.PackFile,
		})
		rtest.Assert(t, err != nil, "expected an error")
	}
}

func requireRcloneStdio(t testing.TB) {
	t.Helper()
	path, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip(err)
	}
	command := exec.Command(path, "serve", "vaultic", "--help")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("--stdio")) {
		t.Skip("rclone does not support serve vaultic --stdio")
	}
}

// vaultic should detect rclone startup failures
func TestRcloneFailedStart(t *testing.T) {
	cfg := NewConfig()
	// exits with exit code 1
	cfg.Program = "false"
	_, err := Open(context.TODO(), cfg, nil, t.Logf)
	var e *exec.ExitError
	if !errors.As(err, &e) {
		// unexpected error
		rtest.OK(t, err)
	}
}
