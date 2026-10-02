package crawl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/cwalk"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/workingkv"
	"github.com/otuschhoff/vaultic/internal/workingstate"
)

type DirectoryManifest struct {
	database      *workingkv.SecureMap
	ctx           context.Context
	path          string
	mutex         sync.Mutex
	metrics       *telemetry.WorkingStateMetric
	closed        bool
	cleanupFailed bool
}

type directoryRecord struct {
	path    string
	names   []string
	release func()
}

type ManifestProgress struct {
	WorkingState    telemetry.WorkingStateSnapshot `json:"working_state"`
	RootsTotal      uint64                         `json:"roots_total"`
	RootsCompleted  uint64                         `json:"roots_completed"`
	DirectoriesRead uint64                         `json:"directories_read"`
	EntriesListed   uint64                         `json:"entries_listed"`
	SecondsElapsed  float64                        `json:"seconds_elapsed"`
	Finished        bool                           `json:"finished"`
	Complete        bool                           `json:"complete"`
}

type manifestCounters struct {
	roots       atomic.Uint64
	directories atomic.Uint64
	entries     atomic.Uint64
}

func BuildDirectoryManifest(
	ctx context.Context,
	roots []string,
	workers, queueCapacity int,
	ignore func(string, os.FileInfo) bool,
) (*DirectoryManifest, error) {
	return BuildDirectoryManifestWithProgress(ctx, roots, workers, queueCapacity, ignore, nil)
}

func BuildDirectoryManifestWithProgress(
	ctx context.Context,
	roots []string,
	workers, queueCapacity int,
	ignore func(string, os.FileInfo) bool,
	report func(ManifestProgress),
) (*DirectoryManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workers < 1 || queueCapacity < 1 {
		return nil, fmt.Errorf("cwalk workers and queue capacity must be positive")
	}
	database, err := workingkv.OpenSecureMap(ctx, "", "vaultic-cwalk-")
	if err != nil {
		return nil, fmt.Errorf("open cwalk manifest: %w", err)
	}
	manifest := &DirectoryManifest{database: database, path: database.Path(), ctx: ctx,
		metrics: telemetry.NewWorkingStateMetric(telemetry.WorkingDirectories, database.Backend())}
	manifest.metrics.Activate()
	started := time.Now()
	counters := &manifestCounters{}
	emitProgress := func(finished, complete bool) {
		if report != nil {
			report(ManifestProgress{WorkingState: manifest.WorkingState(), RootsTotal: uint64(len(roots)), RootsCompleted: counters.roots.Load(),
				DirectoriesRead: counters.directories.Load(), EntriesListed: counters.entries.Load(),
				SecondsElapsed: time.Since(started).Seconds(), Finished: finished, Complete: complete})
		}
	}
	emitProgress(false, false)
	walkCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	queueRelease, err := workingstate.Reserve(ctx, uint64(queueCapacity)*64)
	if err != nil {
		return nil, errors.Join(err, manifest.Close())
	}
	defer queueRelease()
	records := make(chan directoryRecord, queueCapacity)
	writeDone := make(chan error, 1)
	go writeDirectoryRecords(walkCtx, database, records, writeDone, func() { cancel(context.Canceled) }, manifest.metrics)

	var walkersMu sync.Mutex
	var walkers []*cwalk.Walker
	stopWalkers := func() {
		walkersMu.Lock()
		defer walkersMu.Unlock()
		for _, walker := range walkers {
			walker.Stop()
		}
	}
	monitorDone := make(chan struct{})
	monitorExited := make(chan struct{})
	go func() {
		defer close(monitorExited)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-walkCtx.Done():
				stopWalkers()
				return
			case <-monitorDone:
				return
			case <-ticker.C:
				emitProgress(false, false)
			}
		}
	}()

	walkErr := walkManifestRoots(walkCtx, cancel, roots, workers, ignore, records, &walkersMu, &walkers, counters)
	close(monitorDone)
	<-monitorExited
	close(records)
	writeErr := <-writeDone
	emitProgress(true, walkErr == nil && writeErr == nil && ctx.Err() == nil)
	if walkErr != nil || writeErr != nil || ctx.Err() != nil {
		_ = manifest.Close() // Preserve the walk/write failure; manifest cleanup cannot make it usable.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if cause := context.Cause(walkCtx); cause != nil && !errors.Is(cause, context.Canceled) {
			return nil, cause
		}
		if writeErr != nil && !errors.Is(writeErr, context.Canceled) {
			return nil, fmt.Errorf("write cwalk manifest: %w", writeErr)
		}
		if walkErr != nil {
			return nil, fmt.Errorf("build cwalk manifest: %w", walkErr)
		}
		return nil, fmt.Errorf("write cwalk manifest: %w", writeErr)
	}
	return manifest, nil
}

func writeDirectoryRecords(ctx context.Context, database *workingkv.SecureMap, records <-chan directoryRecord, done chan<- error, cancel context.CancelFunc, metrics *telemetry.WorkingStateMetric) {
	var resultErr error
	defer func() {
		metrics.ObserveBuffer(0)
		done <- resultErr
	}()
	for record := range records {
		if resultErr != nil {
			if record.release != nil {
				record.release()
			}
			continue
		}
		encoded, err := json.Marshal(record.names)
		if err == nil {
			metrics.ObserveBuffer(uint64(len(manifestKey(record.path)) + len(encoded)))
			err = database.PutValue(ctx, manifestKey(record.path), encoded)
		}
		if err == nil {
			metrics.Committed(1, uint64(len(manifestKey(record.path))+len(encoded)))
		}
		if record.release != nil {
			record.release()
		}
		if err != nil {
			cancel()
			resultErr = err
			continue
		}
	}
}

func walkManifestRoots(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	roots []string,
	workers int,
	ignore func(string, os.FileInfo) bool,
	records chan<- directoryRecord,
	walkersMu *sync.Mutex,
	walkers *[]*cwalk.Walker,
	counters *manifestCounters,
) error {
	lanes := min(4, workers, len(roots))
	if lanes <= 1 {
		return walkManifestRootBatch(ctx, cancel, roots, workers, ignore, records, walkersMu, walkers, counters)
	}
	var next atomic.Uint64
	var group sync.WaitGroup
	var failOnce sync.Once
	var firstErr error
	for lane := 0; lane < lanes; lane++ {
		budget := workers / lanes
		if lane < workers%lanes {
			budget++
		}
		group.Go(func() {
			for ctx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= len(roots) {
					return
				}
				if err := walkManifestRootBatch(ctx, cancel, roots[index:index+1], budget, ignore, records, walkersMu, walkers, counters); err != nil {
					failOnce.Do(func() {
						firstErr = err
						cancel(err)
					})
					return
				}
			}
		})
	}
	group.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func walkManifestRootBatch(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	roots []string,
	workers int,
	ignore func(string, os.FileInfo) bool,
	records chan<- directoryRecord,
	walkersMu *sync.Mutex,
	walkers *[]*cwalk.Walker,
	counters *manifestCounters,
) error {
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := os.Lstat(root)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			counters.roots.Add(1)
			continue
		}
		absoluteRoot, err := filepath.Abs(root)
		if err != nil {
			return err
		}
		walker := newManifestWalker(ctx, cancel, absoluteRoot, workers, ignore, records, counters)
		walkersMu.Lock()
		*walkers = append(*walkers, walker)
		walkersMu.Unlock()
		if err := ctx.Err(); err != nil {
			walker.Stop()
			return err
		}
		if err := walker.Run(); err != nil {
			return err
		}
		counters.roots.Add(1)
	}
	return nil
}

func newManifestWalker(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	root string,
	workers int,
	ignore func(string, os.FileInfo) bool,
	records chan<- directoryRecord,
	counters *manifestCounters,
) *cwalk.Walker {
	var walker *cwalk.Walker
	var resizeOnce sync.Once
	callbacks := cwalk.Callbacks{OnReadDir: func(relative string, entries []os.DirEntry, err error) {
		if ctx.Err() != nil {
			walker.Stop()
			return
		}
		if relative == "" {
			resizeOnce.Do(func() {
				if resizeErr := walker.ResizeWorkers(workers); resizeErr != nil {
					cancel(resizeErr)
				}
			})
		}
		if err != nil {
			return
		}
		counters.entries.Add(uint64(len(entries)))
		counters.directories.Add(1)
		path := filepath.Join(root, filepath.FromSlash(relative))
		charge := uint64(len(path))*8 + uint64(len(entries))*128 + 1024
		for _, entry := range entries {
			charge += uint64(len(entry.Name())) * 8
		}
		release, reserveErr := workingstate.Reserve(ctx, charge)
		if reserveErr != nil {
			cancel(reserveErr)
			walker.Stop()
			return
		}
		names := make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
		}
		select {
		case records <- directoryRecord{path: path, names: names, release: release}:
		case <-ctx.Done():
			release()
			walker.Stop()
		}
	}}
	walker = cwalk.NewWalker(root, 1, callbacks)
	walker.SetLogger(discardLogger{})
	if ignore != nil {
		walker.SetIgnoreFunc(func(_ string, relative string, info os.FileInfo) bool {
			return ignore(filepath.Join(root, filepath.FromSlash(relative)), info)
		})
	}
	return walker
}

func (manifest *DirectoryManifest) Names(directory string) ([]string, bool, error) {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, false, err
	}
	value, found, err := manifest.database.GetValue(manifest.ctx, manifestKey(absolute))
	if err != nil || !found {
		return nil, found, err
	}
	release, err := workingstate.Reserve(manifest.ctx, uint64(len(value))*3+1024)
	if err != nil {
		return nil, false, err
	}
	defer release()
	var names []string
	if err := json.Unmarshal(value, &names); err != nil {
		return nil, false, fmt.Errorf("decode cwalk directory %q: %w", directory, err)
	}
	return names, true, nil
}

func (manifest *DirectoryManifest) Close() error {
	manifest.mutex.Lock()
	defer manifest.mutex.Unlock()
	err := manifest.database.Close()
	removeErr := os.RemoveAll(manifest.path)
	manifest.cleanupFailed = removeErr != nil
	manifest.closed = true
	manifest.metrics.Close()
	if err != nil {
		return err
	}
	return removeErr
}

func (manifest *DirectoryManifest) WorkingState() telemetry.WorkingStateSnapshot {
	manifest.mutex.Lock()
	defer manifest.mutex.Unlock()
	manifest.metrics.SetBackend(manifest.database.Backend())
	if manifest.closed {
		return manifest.metrics.Snapshot(0, !manifest.cleanupFailed)
	}
	size, known := manifest.database.ScratchBytes()
	return manifest.metrics.Snapshot(size, known)
}

func manifestKey(path string) []byte {
	return append([]byte("d:"), []byte(filepath.Clean(path))...)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...interface{}) {}
