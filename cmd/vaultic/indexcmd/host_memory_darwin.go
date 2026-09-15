//go:build darwin

package indexcmd

import "golang.org/x/sys/unix"

func physicalMemoryBytes() uint64 {
	bytes, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return bytes
}
