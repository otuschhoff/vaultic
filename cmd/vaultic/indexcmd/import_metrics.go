package indexcmd

import (
	"context"
	"log"
	"runtime"
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
				var memory runtime.MemStats
				runtime.ReadMemStats(&memory)
				log.Printf("legacy import runtime: gomaxprocs=%d goroutines=%d heap_alloc=%d heap_inuse=%d heap_sys=%d gc_cycles=%d gc_pause_total=%s",
					runtime.GOMAXPROCS(0), runtime.NumGoroutine(), memory.HeapAlloc, memory.HeapInuse, memory.HeapSys,
					memory.NumGC, time.Duration(memory.PauseTotalNs))
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); <-done }) }
}
