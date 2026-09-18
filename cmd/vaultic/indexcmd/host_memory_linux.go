//go:build linux

package indexcmd

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func physicalMemoryBytes() uint64 {
	var info unix.Sysinfo_t
	if unix.Sysinfo(&info) != nil {
		return 0
	}
	return info.Totalram * uint64(info.Unit)
}

func availableMemoryBytes() (uint64, bool) {
	available := physicalMemoryBytes()
	availableOK := available != 0
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "MemAvailable:" {
				if kib, err := strconv.ParseUint(fields[1], 10, 64); err == nil && kib <= math.MaxUint64/1024 {
					available, availableOK = minAvailableMemory(available, availableOK, kib*1024)
				}
				break
			}
		}
	}
	for _, paths := range cgroupMemoryPaths() {
		limit, limitOK := readMemoryValue(paths[0])
		used, usedOK := readMemoryValue(paths[1])
		if limitOK && usedOK {
			reclaimable := cgroupReclaimableBytes(filepath.Dir(paths[0]))
			available, availableOK = minAvailableMemory(
				available, availableOK, cgroupMemoryHeadroom(limit, used, reclaimable),
			)
		}
	}
	return available, availableOK
}

func cgroupMemoryHeadroom(limit, used, reclaimable uint64) uint64 {
	committed := used - min(used, reclaimable)
	if committed >= limit {
		return 1
	}
	return limit - committed
}

func cgroupMemoryPaths() [][2]string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return defaultCgroupMemoryPaths()
	}
	paths := cgroupMemoryPathsFrom(data)
	if len(paths) == 0 {
		return defaultCgroupMemoryPaths()
	}
	return paths
}

func cgroupMemoryPathsFrom(data []byte) [][2]string {
	var paths [][2]string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			continue
		}
		if fields[0] == "0" && fields[1] == "" {
			paths = append(paths, cgroupAncestorMemoryPaths(
				"/sys/fs/cgroup", fields[2], "memory.max", "memory.current",
			)...)
			continue
		}
		for _, controller := range strings.Split(fields[1], ",") {
			if controller == "memory" {
				paths = append(paths, cgroupAncestorMemoryPaths(
					"/sys/fs/cgroup/memory", fields[2], "memory.limit_in_bytes", "memory.usage_in_bytes",
				)...)
				break
			}
		}
	}
	return paths
}

func cgroupAncestorMemoryPaths(root, relative, limitName, usedName string) [][2]string {
	relative = strings.TrimPrefix(filepath.Clean("/"+relative), "/")
	directory := filepath.Join(root, relative)
	var paths [][2]string
	for {
		paths = append(paths, [2]string{filepath.Join(directory, limitName), filepath.Join(directory, usedName)})
		if directory == root {
			return paths
		}
		directory = filepath.Dir(directory)
	}
}

func defaultCgroupMemoryPaths() [][2]string {
	return [][2]string{
		{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"},
		{"/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes"},
	}
}

func cgroupReclaimableBytes(directory string) uint64 {
	data, err := os.ReadFile(filepath.Join(directory, "memory.stat"))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || (fields[0] != "inactive_file" && fields[0] != "total_inactive_file") {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err == nil {
			return value
		}
	}
	return 0
}

func readMemoryValue(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	value := strings.TrimSpace(string(data))
	if value == "max" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil
}

func minAvailableMemory(current uint64, currentOK bool, candidate uint64) (uint64, bool) {
	if !currentOK || candidate < current {
		return candidate, true
	}
	return current, true
}
