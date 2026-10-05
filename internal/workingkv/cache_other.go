//go:build !linux

package workingkv

import (
	"os"
)

func cachedFileBytes(file *os.File) (uint64, error) {
	return 0, ErrWorkingPolicyConflict
}
