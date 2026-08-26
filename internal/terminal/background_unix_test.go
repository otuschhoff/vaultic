//go:build unix

package terminal

import (
	"os"
	"testing"

	rtest "github.com/vaultic/vaultic/internal/test"
)

func TestIsProcessBackground(t *testing.T) {
	tty, err := os.Open("/dev/tty")
	if err != nil {
		t.Skipf("can't open terminal: %v", err)
	}

	_, err = isProcessBackground(int(tty.Fd()))
	rtest.OK(t, err)

	_ = tty.Close()
}
