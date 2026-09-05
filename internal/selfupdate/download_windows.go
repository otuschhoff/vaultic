//go:build windows

package selfupdate

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/otuschhoff/vaultic/internal/errors"
)

// Rename (rather than remove) the running version. The running binary will be locked
// on Windows and cannot be removed while still executing.
func removeResticBinary(dir, target string) error {
	// nothing to do if the target does not exist
	if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	backup := filepath.Join(dir, filepath.Base(target)+".bak")
	if _, err := os.Stat(backup); err == nil {
		_ = os.Remove(backup) // A stale backup is harmless after the replacement executable is installed.
	}
	if err := os.Rename(target, backup); err != nil {
		return fmt.Errorf("unable to rename target file: %w", err)
	}
	return nil
}
