//go:build linux

package indexcmd

import "golang.org/x/sys/unix"

func physicalMemoryBytes() uint64 {
	var info unix.Sysinfo_t
	if unix.Sysinfo(&info) != nil {
		return 0
	}
	return info.Totalram * uint64(info.Unit)
}
