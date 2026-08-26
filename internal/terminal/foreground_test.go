//go:build !windows

package terminal_test

import (
	"bufio"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/vaultic/vaultic/internal/terminal"
	rtest "github.com/vaultic/vaultic/internal/test"
)

func TestForeground(t *testing.T) {
	err := os.Setenv("VAULTIC_PASSWORD", "supersecret")
	rtest.OK(t, err)
	// legacy variable names must be filtered as well
	err = os.Setenv("RESTIC_PASSWORD", "supersecret")
	rtest.OK(t, err)

	cmd := exec.Command("env")
	stdout, err := cmd.StdoutPipe()
	rtest.OK(t, err)

	bg, err := terminal.StartForeground(cmd)
	rtest.OK(t, err)
	defer func() {
		rtest.OK(t, cmd.Wait())
	}()

	err = bg()
	rtest.OK(t, err)

	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "VAULTIC_PASSWORD=") ||
			strings.HasPrefix(sc.Text(), "RESTIC_PASSWORD=") {
			t.Error("subprocess got to see the password")
		}
	}
	rtest.OK(t, err)
}
