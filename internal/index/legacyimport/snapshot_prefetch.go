package legacyimport

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"sync"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingstate"
)

const snapshotPrefetchMaxBytes = 1 << 20

type snapshotBlobSizer interface {
	LookupBlobSize(vaultic.BlobHandle) (uint, bool)
}

type snapshotTreeRead struct {
	handle vaultic.BlobHandle
	done   chan struct{}
	value  []byte
	err    error
}

type snapshotTreePrefetch struct {
	ctx     context.Context
	cancel  context.CancelFunc
	source  SnapshotSource
	sizer   snapshotBlobSizer
	pending *snapshotTreeRead
	release func()
	once    sync.Once
}

func newSnapshotTreePrefetch(ctx context.Context, source SnapshotSource) (*snapshotTreePrefetch, error) {
	sizer, ok := source.(snapshotBlobSizer)
	if !ok {
		return nil, fmt.Errorf("snapshot prefetch requires size-aware source reads")
	}
	if workingstate.PolicyFrom(ctx) == nil {
		return nil, fmt.Errorf("snapshot prefetch requires managed memory admission")
	}
	release, err := workingstate.Reserve(ctx, 4*snapshotPrefetchMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("reserve snapshot prefetch: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	return &snapshotTreePrefetch{ctx: ctx, cancel: cancel, source: source, sizer: sizer, release: release}, nil
}

func (prefetch *snapshotTreePrefetch) schedule(handle vaultic.BlobHandle) {
	if prefetch.pending != nil || prefetch.ctx.Err() != nil {
		return
	}
	size, found := prefetch.sizer.LookupBlobSize(handle)
	if !found || size == 0 || size > snapshotPrefetchMaxBytes {
		return
	}
	read := &snapshotTreeRead{handle: handle, done: make(chan struct{})}
	prefetch.pending = read
	go func() {
		defer close(read.done)
		read.value, read.err = prefetch.source.LoadBlob(prefetch.ctx, handle, make([]byte, 0, int(size)))
	}()
}

func (prefetch *snapshotTreePrefetch) LoadBlob(ctx context.Context, handle vaultic.BlobHandle, buffer []byte) ([]byte, error) {
	read := prefetch.pending
	if read == nil || read.handle != handle {
		return prefetch.source.LoadBlob(ctx, handle, buffer)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-read.done:
	}
	prefetch.pending = nil
	if len(read.value) > snapshotPrefetchMaxBytes || cap(read.value) > 2*snapshotPrefetchMaxBytes {
		read.value = nil
		return prefetch.source.LoadBlob(ctx, handle, buffer)
	}
	return read.value, read.err
}

func (prefetch *snapshotTreePrefetch) close() {
	prefetch.once.Do(func() {
		prefetch.cancel()
		if prefetch.pending != nil {
			<-prefetch.pending.done
			prefetch.pending.value = nil
			prefetch.pending = nil
		}
		prefetch.release()
	})
}

func (importer *treeImporter) loadSnapshotTree(id vaultic.ID, depth uint) (data.TreeNodeIterator, error) {
	if importer.prefetch == nil {
		return data.LoadTree(importer.ctx, importer.source, id)
	}
	encoded, err := importer.prefetch.LoadBlob(importer.ctx, vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id}, nil)
	if err != nil {
		return nil, err
	}
	tree, err := data.NewTreeNodeIterator(bytes.NewReader(encoded))
	if err != nil || len(encoded) > snapshotPrefetchMaxBytes || importer.options.SnapshotDepth > 0 && depth >= importer.options.SnapshotDepth {
		return tree, err
	}
	release, err := workingstate.Reserve(importer.ctx, uint64(len(encoded))*8+4096)
	if err != nil {
		return tree, nil
	}
	return func(yield func(data.NodeOrError) bool) {
		defer release()
		next, stop := iter.Pull(tree)
		defer stop()
		current, more := next()
		for more {
			following, exists := next()
			if exists && following.Error == nil && following.Node != nil && following.Node.Type == data.NodeTypeDir && following.Node.Subtree != nil && !following.Node.Subtree.IsNull() {
				importer.prefetch.schedule(vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: *following.Node.Subtree})
			}
			if !yield(current) || current.Error != nil {
				return
			}
			current, more = following, exists
		}
	}, nil
}
