package indexcmd

import (
	"context"
	"log"
	"runtime"
	runtimemetrics "runtime/metrics"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

func startLegacyImportStats(ctx context.Context, store *daemon.SchemaStore, interval time.Duration, emit func(daemon.LegacyImportStats)) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				emit(store.LegacyImportStats())
				samples := legacyImportRuntimeLogMetrics()
				heapInUse := samples[1].Value.Uint64() + samples[2].Value.Uint64()
				heapSystem := heapInUse + samples[3].Value.Uint64() + samples[4].Value.Uint64() + samples[5].Value.Uint64()
				gcPauseCPU := time.Duration(samples[7].Value.Float64() * float64(time.Second))
				log.Printf("legacy import runtime: gomaxprocs=%d goroutines=%d heap_alloc=%d heap_inuse=%d heap_sys=%d gc_cycles=%d gc_pause_cpu_total=%s",
					runtime.GOMAXPROCS(0), samples[0].Value.Uint64(), samples[1].Value.Uint64(), heapInUse, heapSystem,
					samples[6].Value.Uint64(), gcPauseCPU)
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); <-done }) }
}

func legacyImportRuntimeLogMetrics() []runtimemetrics.Sample {
	samples := []runtimemetrics.Sample{
		{Name: "/sched/goroutines:goroutines"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/stacks:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/cpu/classes/gc/pause:cpu-seconds"},
	}
	runtimemetrics.Read(samples)
	return samples
}
