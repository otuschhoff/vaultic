package workingkv

import (
	"context"
	"errors"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

func readPortableResources() (Resources, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	physical, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return Resources{}, errors.Join(ErrWorkingPolicyConflict, errors.New("host resource sampling unavailable"))
	}
	self, err := process.NewProcessWithContext(ctx, int32(os.Getpid()))
	if err != nil {
		return Resources{}, err
	}
	resident, err := self.MemoryInfoWithContext(ctx)
	if err != nil {
		return Resources{}, errors.Join(ErrWorkingPolicyConflict, errors.New("process RSS sampling unavailable"))
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return Resources{HostLimit: physical.Total, HostAvailable: physical.Available, RSS: resident.RSS, RSSKnown: true, RuntimeBytes: memory.Sys - memory.HeapReleased}, nil
}
