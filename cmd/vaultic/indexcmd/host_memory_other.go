//go:build !darwin && !linux

package indexcmd

func physicalMemoryBytes() uint64 {
	return 0
}

func availableMemoryBytes() (uint64, bool) { return 0, false }
