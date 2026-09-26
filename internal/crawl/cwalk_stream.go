package crawl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/otuschhoff/cwalk"
	"github.com/otuschhoff/vaultic/internal/fs"
)

type directoryResult struct {
	done        chan struct{}
	names       []string
	directories []string
	err         error
}

type DirectoryStream struct {
	filesystem    fs.FS
	ctx           context.Context
	cancel        context.CancelFunc
	jobs          chan string
	slots         chan struct{}
	group         sync.WaitGroup
	mutex         sync.Mutex
	pending       map[string]*directoryResult
	capacity      int
	metadata      *lru.Cache[string, fs.ReadDirEntry]
	metadataBytes int
}

func NewDirectoryStream(ctx context.Context, workers, capacity int) (*DirectoryStream, error) {
	return NewDirectoryStreamWithFS(ctx, workers, capacity, fs.NewLocal())
}

func NewDirectoryStreamWithFS(ctx context.Context, workers, capacity int, filesystem fs.FS) (*DirectoryStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workers < 1 || capacity < 1 {
		return nil, fmt.Errorf("cwalk workers and lookahead capacity must be positive")
	}
	metadata, err := lru.New[string, fs.ReadDirEntry](min(capacity, 64) * 128)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stream := &DirectoryStream{filesystem: filesystem, ctx: ctx, cancel: cancel, jobs: make(chan string, capacity),
		slots: make(chan struct{}, workers), pending: make(map[string]*directoryResult), capacity: capacity, metadata: metadata}
	for worker := 0; worker < workers; worker++ {
		stream.group.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case path := <-stream.jobs:
					stream.mutex.Lock()
					result := stream.pending[path]
					stream.mutex.Unlock()
					if result != nil {
						result.names, result.directories, result.err = stream.read(path)
						bytes := 0
						for _, name := range result.names {
							bytes += len(name) + 16
						}
						if bytes > 1<<20 {
							stream.mutex.Lock()
							delete(stream.pending, path)
							stream.mutex.Unlock()
						}
						close(result.done)
					}
				}
			}
		})
	}
	return stream, nil
}

func (stream *DirectoryStream) read(path string) ([]string, []string, error) {
	select {
	case stream.slots <- struct{}{}:
		defer func() { <-stream.slots }()
	case <-stream.ctx.Done():
		return nil, nil, stream.ctx.Err()
	}
	var names, directories []string
	var readErr error
	callbacks := cwalk.Callbacks{OnReadDir: func(_ string, entries []os.DirEntry, err error) {
		readErr = err
		if err != nil {
			return
		}
		names = make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
			if !fs.IsNFS(stream.filesystem) {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				readErr = err
				return
			}
			if retained, ok := info.(walkFileInfo); ok && retained.entry != nil {
				stream.rememberMetadata(stream.filesystem.Join(path, entry.Name()), *retained.entry)
			}
		}
	}}
	var walker *cwalk.Walker
	if fs.IsLocal(stream.filesystem) {
		walker = cwalk.NewWalker(path, 1, callbacks)
	} else {
		walker = cwalk.NewWalkerWithFS(".", 1, callbacks, walkFilesystem{filesystem: stream.filesystem, root: path})
	}
	walker.SetLogger(discardLogger{})
	walker.SetIgnoreFunc(func(_ string, relative string, info os.FileInfo) bool {
		if info.IsDir() && len(directories) < stream.capacity {
			directories = append(directories, stream.filesystem.Join(path, filepath.FromSlash(relative)))
		}
		return true
	})
	monitorDone, monitorExited := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(monitorExited)
		select {
		case <-stream.ctx.Done():
			walker.Stop()
		case <-monitorDone:
		}
	}()
	err := walker.Run()
	close(monitorDone)
	<-monitorExited
	if stream.ctx.Err() != nil {
		return nil, nil, stream.ctx.Err()
	}
	if err != nil {
		return nil, nil, err
	}
	if readErr != nil {
		return nil, nil, readErr
	}
	return names, directories, nil
}

func (stream *DirectoryStream) Names(path string) ([]string, bool, error) {
	stream.mutex.Lock()
	if err := stream.ctx.Err(); err != nil {
		stream.mutex.Unlock()
		return nil, false, err
	}
	stream.group.Add(1)
	stream.mutex.Unlock()
	defer stream.group.Done()
	path, err := stream.filesystem.Abs(path)
	if err != nil {
		return nil, false, err
	}
	stream.mutex.Lock()
	result := stream.pending[path]
	stream.mutex.Unlock()
	var names, directories []string
	if result != nil {
		select {
		case <-result.done:
			names, directories, err = result.names, result.directories, result.err
		case <-stream.ctx.Done():
			return nil, false, stream.ctx.Err()
		}
		stream.mutex.Lock()
		delete(stream.pending, path)
		stream.mutex.Unlock()
	} else {
		names, directories, err = stream.read(path)
	}
	if err != nil {
		return nil, false, err
	}
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	for _, directory := range directories {
		if stream.ctx.Err() != nil {
			return nil, false, stream.ctx.Err()
		}
		if stream.pending[directory] != nil {
			continue
		}
		if len(stream.pending) >= stream.capacity {
			for oldPath, oldResult := range stream.pending {
				select {
				case <-oldResult.done:
					delete(stream.pending, oldPath)
				default:
				}
				if len(stream.pending) < stream.capacity {
					break
				}
			}
		}
		if len(stream.pending) >= stream.capacity {
			break
		}
		select {
		case stream.jobs <- directory:
			stream.pending[directory] = &directoryResult{done: make(chan struct{})}
		default:
			return names, true, nil
		}
	}
	return names, true, nil
}

func retainedEntryBytes(name string, entry fs.ReadDirEntry) int {
	return 2*len(name) + len(entry.Name) + 1024
}

func (stream *DirectoryStream) rememberMetadata(name string, entry fs.ReadDirEntry) {
	size := retainedEntryBytes(name, entry)
	if size > 1<<20 {
		return
	}
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	if stream.ctx.Err() != nil {
		return
	}
	if previous, found := stream.metadata.Peek(name); found {
		stream.metadataBytes -= retainedEntryBytes(name, previous)
		stream.metadata.Remove(name)
	}
	for stream.metadata.Len() >= min(stream.capacity, 64)*128 || stream.metadataBytes+size > 8<<20 {
		oldName, previous, found := stream.metadata.RemoveOldest()
		if !found {
			break
		}
		stream.metadataBytes -= retainedEntryBytes(oldName, previous)
	}
	stream.metadata.Add(name, entry)
	stream.metadataBytes += size
}

func (stream *DirectoryStream) OpenMetadata(name string) (fs.File, bool, error) {
	name, err := stream.filesystem.Abs(name)
	if err != nil {
		return nil, false, err
	}
	stream.mutex.Lock()
	if err := stream.ctx.Err(); err != nil {
		stream.mutex.Unlock()
		return nil, false, err
	}
	entry, found := stream.metadata.Get(name)
	if found {
		stream.metadata.Remove(name)
		stream.metadataBytes -= retainedEntryBytes(name, entry)
		stream.group.Add(1)
	}
	stream.mutex.Unlock()
	if !found {
		return nil, false, nil
	}
	defer stream.group.Done()
	file, err := entry.OpenMetadata()
	return file, true, err
}

func (stream *DirectoryStream) Close() error {
	stream.mutex.Lock()
	stream.cancel()
	stream.mutex.Unlock()
	stream.group.Wait()
	stream.mutex.Lock()
	stream.metadata.Purge()
	stream.metadataBytes = 0
	stream.mutex.Unlock()
	return nil
}
