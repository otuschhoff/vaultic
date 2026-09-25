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

	"github.com/cockroachdb/pebble"
	"github.com/otuschhoff/cwalk"
)

type DirectoryManifest struct {
	database *pebble.DB
	path     string
}

type directoryRecord struct {
	path  string
	names []string
}

type ManifestProgress struct {
	RootsTotal      uint64  `json:"roots_total"`
	RootsCompleted  uint64  `json:"roots_completed"`
	DirectoriesRead uint64  `json:"directories_read"`
	EntriesListed   uint64  `json:"entries_listed"`
	SecondsElapsed  float64 `json:"seconds_elapsed"`
	Finished        bool    `json:"finished"`
	Complete        bool    `json:"complete"`
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
	directory, err := os.MkdirTemp("", "vaultic-cwalk-")
	if err != nil {
		return nil, fmt.Errorf("create cwalk manifest directory: %w", err)
	}
	database, err := pebble.Open(directory, &pebble.Options{})
	if err != nil {
		_ = os.RemoveAll(directory) // The database never opened, so this empty temporary directory is unreachable.
		return nil, fmt.Errorf("open cwalk manifest: %w", err)
	}
	manifest := &DirectoryManifest{database: database, path: directory}
	started := time.Now()
	counters := &manifestCounters{}
	emitProgress := func(finished, complete bool) {
		if report != nil {
			report(ManifestProgress{RootsTotal: uint64(len(roots)), RootsCompleted: counters.roots.Load(),
				DirectoriesRead: counters.directories.Load(), EntriesListed: counters.entries.Load(),
				SecondsElapsed: time.Since(started).Seconds(), Finished: finished, Complete: complete})
		}
	}
	emitProgress(false, false)
	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	records := make(chan directoryRecord, queueCapacity)
	writeDone := make(chan error, 1)
	go writeDirectoryRecords(database, records, writeDone, cancel)

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
		if walkErr != nil {
			return nil, fmt.Errorf("build cwalk manifest: %w", walkErr)
		}
		return nil, fmt.Errorf("write cwalk manifest: %w", writeErr)
	}
	return manifest, nil
}

func writeDirectoryRecords(database *pebble.DB, records <-chan directoryRecord, done chan<- error, cancel context.CancelFunc) {
	batch := database.NewBatch()
	defer func() { _ = batch.Close() }() // Commit errors are returned explicitly; batch close only releases memory.
	pending := 0
	for record := range records {
		encoded, err := json.Marshal(record.names)
		if err == nil {
			err = batch.Set(manifestKey(record.path), encoded, nil)
		}
		if err == nil {
			pending++
			if pending == 1024 {
				err = batch.Commit(pebble.NoSync)
				batch.Reset()
				pending = 0
			}
		}
		if err != nil {
			cancel()
			done <- err
			return
		}
	}
	if pending > 0 {
		done <- batch.Commit(pebble.NoSync)
		return
	}
	done <- nil
}

func walkManifestRoots(
	ctx context.Context,
	cancel context.CancelFunc,
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
	cancel context.CancelFunc,
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
					cancel()
				}
			})
		}
		if err != nil {
			return
		}
		counters.entries.Add(uint64(len(entries)))
		counters.directories.Add(1)
		names := make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
		}
		select {
		case records <- directoryRecord{path: filepath.Join(root, filepath.FromSlash(relative)), names: names}:
		case <-ctx.Done():
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
	value, closer, err := manifest.database.Get(manifestKey(absolute))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer closer.Close()
	var names []string
	if err := json.Unmarshal(value, &names); err != nil {
		return nil, false, fmt.Errorf("decode cwalk directory %q: %w", directory, err)
	}
	return names, true, nil
}

func (manifest *DirectoryManifest) Close() error {
	err := manifest.database.Close()
	removeErr := os.RemoveAll(manifest.path)
	if err != nil {
		return err
	}
	return removeErr
}

func manifestKey(path string) []byte {
	return append([]byte("d:"), []byte(filepath.Clean(path))...)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...interface{}) {}
