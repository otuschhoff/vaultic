package workingkv

import (
	"os"

	"golang.org/x/sys/unix"
)

func cachedFileBytes(file *os.File) (uint64, error) {
	var state unix.Cachestat_t
	err := unix.Cachestat(uint(file.Fd()), &unix.CachestatRange{Len: ^uint64(0)}, &state, 0)
	if err != nil {
		return 0, err
	}
	pageSize := uint64(os.Getpagesize())
	if state.Cache > ^uint64(0)/pageSize {
		return 0, ErrWorkingPolicyConflict
	}
	return state.Cache * pageSize, nil
}
