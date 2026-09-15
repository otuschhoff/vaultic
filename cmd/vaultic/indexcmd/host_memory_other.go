//go:build !darwin && !linux

package indexcmd

func physicalMemoryBytes() uint64 {
	return 0
}
