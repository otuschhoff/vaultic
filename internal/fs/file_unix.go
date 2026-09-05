//go:build !windows

package fs

import (
	"errors"
	"os"
	"syscall"
)

// fixpath returns an absolute path on windows, so vaultic can open long file
// names.
func fixpath(name string) string {
	return name
}

// isNotSupported returns true if the error is caused by an unsupported file system feature.
func isNotSupported(err error) bool {
	var pathError *os.PathError
	return errors.As(err, &pathError) && errors.Is(pathError.Err, syscall.ENOTSUP)
}

// chmod changes the mode of the named file to mode.
func chmod(name string, mode os.FileMode) error {
	err := os.Chmod(fixpath(name), mode)

	// ignore the error if the FS does not support setting this mode (e.g. CIFS with gvfs on Linux)
	if err != nil && isNotSupported(err) {
		return nil
	}

	return err
}
