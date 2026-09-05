package archiver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/otuschhoff/cwalk"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/fs"
)

// Scanner  traverses the targets and calls the function Result with cumulated
// stats concerning the files and folders found. Select is used to decide which
// items should be included. Error is called when an error occurs.
type Scanner struct {
	FS           fs.FS
	SelectByName SelectByNameFunc
	Select       SelectFunc
	Error        ErrorFunc
	Result       func(item string, s ScanStats)
	CWalkWorkers int
	CWalkQueue   int
}

// NewScanner initializes a new Scanner.
func NewScanner(filesystem fs.FS) *Scanner {
	return &Scanner{
		FS:           filesystem,
		SelectByName: func(_ string) bool { return true },
		Select:       func(_ string, _ *fs.ExtendedFileInfo, _ fs.FS) bool { return true },
		Error:        func(_ string, err error) error { return err },
		Result:       func(_ string, _ ScanStats) {},
	}
}

// ScanStats collect statistics.
type ScanStats struct {
	Files, Dirs, Others uint
	Bytes               uint64
}

func (s *Scanner) scanTree(ctx context.Context, stats ScanStats, tree tree) (ScanStats, error) {
	// traverse the path in the file system for all leaf nodes
	if tree.Leaf() {
		abstarget, err := s.FS.Abs(tree.Path)
		if err != nil {
			return ScanStats{}, err
		}

		if s.CWalkWorkers > 0 && fs.IsLocal(s.FS) {
			stats, err = s.scanCWalk(ctx, stats, abstarget, tree.Explicit)
		} else {
			stats, err = s.scan(ctx, stats, abstarget, tree.Explicit)
		}
		if err != nil {
			return ScanStats{}, err
		}

		return stats, nil
	}

	// otherwise recurse into the nodes in a deterministic order
	for _, name := range tree.NodeNames() {
		var err error
		stats, err = s.scanTree(ctx, stats, tree.Nodes[name])
		if err != nil {
			return ScanStats{}, err
		}

		if ctx.Err() != nil {
			return stats, nil
		}
	}

	return stats, nil
}

//nolint:funlen,gocognit,gocyclo // Existing domain flow is an explicit complexity exception; new code remains gated.
func (s *Scanner) scanCWalk(ctx context.Context, stats ScanStats, target string, explicit bool) (ScanStats, error) {
	if ctx.Err() != nil {
		return stats, nil
	}
	if !explicit && !s.SelectByName(target) {
		return stats, nil
	}
	info, err := s.FS.Lstat(target)
	if err != nil {
		return stats, s.Error(target, err)
	}
	if !explicit && !s.Select(target, info, s.FS) {
		return stats, nil
	}
	if !info.Mode.IsDir() {
		return s.addScanResult(target, info, stats), nil
	}

	queueCapacity := s.CWalkQueue
	if queueCapacity <= 0 {
		queueCapacity = 4096
	}
	type scanEvent struct {
		item string
		info *fs.ExtendedFileInfo
	}
	events := make(chan scanEvent, queueCapacity)
	aggregated := make(chan ScanStats, 1)
	go func() {
		for event := range events {
			stats = s.addScanResult(event.item, event.info, stats)
		}
		aggregated <- stats
	}()

	var errorMu sync.Mutex
	var selectMu sync.Mutex
	var resizeOnce sync.Once
	var callbackErr error
	var walker *cwalk.Walker
	stopOnError := func(item string, err error) {
		errorMu.Lock()
		defer errorMu.Unlock()
		if callbackErr != nil {
			return
		}
		if handledErr := s.Error(item, err); handledErr != nil {
			callbackErr = handledErr
			walker.Stop()
		}
	}
	include := func(relPath string, osInfo os.FileInfo) bool {
		selectMu.Lock()
		defer selectMu.Unlock()
		item := s.FS.Join(target, relPath)
		if !s.SelectByName(item) {
			return false
		}
		fileInfo, err := s.FS.Lstat(item)
		if err != nil {
			stopOnError(item, err)
			return false
		}
		if !s.Select(item, fileInfo, s.FS) {
			return false
		}
		if osInfo.Mode() != fileInfo.Mode {
			stopOnError(item, fmt.Errorf("file type changed during scan"))
			return false
		}
		return true
	}
	emit := func(item string, info *fs.ExtendedFileInfo) {
		select {
		case events <- scanEvent{item: item, info: info}:
		case <-ctx.Done():
			walker.Stop()
		}
	}
	walkerCallbacks := cwalk.Callbacks{
		OnFileOrSymlink: func(relPath string, _ os.DirEntry) {
			item := s.FS.Join(target, relPath)
			fileInfo, err := s.FS.Lstat(item)
			if err != nil {
				stopOnError(item, err)
				return
			}
			emit(item, fileInfo)
		},
		OnReadDir: func(relPath string, _ []os.DirEntry, err error) {
			if relPath == "" {
				resizeOnce.Do(func() {
					if resizeErr := walker.ResizeWorkers(s.CWalkWorkers); resizeErr != nil {
						stopOnError(target, resizeErr)
					}
				})
			}
			item := s.FS.Join(target, relPath)
			if err != nil {
				stopOnError(item, err)
				return
			}
			if relPath == "" {
				return
			}
			fileInfo, statErr := s.FS.Lstat(item)
			if statErr != nil {
				stopOnError(item, statErr)
				return
			}
			emit(item, fileInfo)
		},
	}
	walker = cwalk.NewWalker(target, 1, walkerCallbacks)
	walker.SetIgnoreFunc(func(_ string, relPath string, info os.FileInfo) bool {
		return !include(relPath, info)
	})
	walker.SetLogger(discardCWalkLogger{})
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			walker.Stop()
		case <-done:
		}
	}()
	walkErr := walker.Run()
	close(done)
	close(events)
	stats = <-aggregated
	errorMu.Lock()
	err = callbackErr
	errorMu.Unlock()
	if err != nil {
		return stats, err
	}
	if walkErr != nil && ctx.Err() == nil && !errors.Is(walkErr, context.Canceled) {
		if err := s.Error(target, walkErr); err != nil {
			return stats, err
		}
		return s.scan(ctx, ScanStats{}, target, explicit)
	}
	stats.Dirs++
	s.Result(target, stats)
	return stats, nil
}

func (s *Scanner) addScanResult(target string, info *fs.ExtendedFileInfo, stats ScanStats) ScanStats {
	switch {
	case info.Mode.IsRegular():
		stats.Files++
		stats.Bytes += uint64(info.Size)
	case info.Mode.IsDir():
		stats.Dirs++
	default:
		stats.Others++
	}
	s.Result(target, stats)
	return stats
}

type discardCWalkLogger struct{}

func (discardCWalkLogger) Printf(string, ...interface{}) {}

// Scan traverses the targets. The function Result is called for each new item
// found, the complete result is also returned by Scan.
func (s *Scanner) Scan(ctx context.Context, targets []string) error {
	debug.Log("start scan for %v", targets)

	cleanTargets, err := resolveRelativeTargets(s.FS, targets)
	if err != nil {
		return err
	}

	debug.Log("clean targets %v", cleanTargets)

	// we're using the same tree representation as the archiver does
	tree, err := newTree(s.FS, cleanTargets)
	if err != nil {
		return err
	}

	stats, err := s.scanTree(ctx, ScanStats{}, *tree)
	if err != nil {
		return err
	}

	s.Result("", stats)
	debug.Log("result: %+v", stats)
	return nil
}

// explicit is true when this path was an explicit backup target (same meaning as tree.Explicit on a leaf).
func (s *Scanner) scan(ctx context.Context, stats ScanStats, target string, explicit bool) (ScanStats, error) {
	if ctx.Err() != nil {
		return stats, nil
	}

	// exclude files by path before running stat to reduce number of lstat calls
	if !explicit && !s.SelectByName(target) {
		return stats, nil
	}

	// get file information
	fi, err := s.FS.Lstat(target)
	if err != nil {
		return stats, s.Error(target, err)
	}

	// run remaining select functions that require file information
	if !explicit && !s.Select(target, fi, s.FS) {
		return stats, nil
	}

	switch {
	case fi.Mode.IsRegular():
		stats.Files++
		stats.Bytes += uint64(fi.Size)
	case fi.Mode.IsDir():
		names, err := fs.Readdirnames(s.FS, target, fs.O_NOFOLLOW)
		if err != nil {
			return stats, s.Error(target, err)
		}
		sort.Strings(names)

		for _, name := range names {
			stats, err = s.scan(ctx, stats, s.FS.Join(target, name), false)
			if err != nil {
				return stats, err
			}
		}
		stats.Dirs++
	default:
		stats.Others++
	}

	s.Result(target, stats)
	return stats, nil
}
