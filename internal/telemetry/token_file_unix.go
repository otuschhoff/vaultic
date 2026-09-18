//go:build linux || darwin

package telemetry

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func readProtectedTokenFile(path string) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		if err := unix.Close(fd); err != nil {
			return nil, fmt.Errorf("close protected token descriptor: %w", err)
		}
		return nil, fmt.Errorf("open protected token file")
	}
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o077 != 0 || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("token file must be regular, owner-accessible only, and owned by the current user")
	}
	if stat.Size < 0 || stat.Size > maxTokenFileBytes {
		return nil, fmt.Errorf("token file exceeds %d bytes", maxTokenFileBytes)
	}
	contents, err := io.ReadAll(io.LimitReader(file, maxTokenFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(contents) > maxTokenFileBytes {
		return nil, fmt.Errorf("token file exceeds %d bytes", maxTokenFileBytes)
	}
	return contents, nil
}
