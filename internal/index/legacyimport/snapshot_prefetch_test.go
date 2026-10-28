package legacyimport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
)

type prefetchTestSource struct {
	*memorySource
	reads   atomic.Uint64
	started chan struct{}
	block   bool
	failure error
}

func (source *prefetchTestSource) LookupBlobSize(handle vaultic.BlobHandle) (uint, bool) {
	value, found := source.blobs[handle.ID]
	return uint(len(value)), found
}

func (source *prefetchTestSource) LoadBlob(ctx context.Context, handle vaultic.BlobHandle, buffer []byte) ([]byte, error) {
	source.reads.Add(1)
	if source.started != nil {
		source.started <- struct{}{}
	}
	if source.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if source.failure != nil {
		return nil, source.failure
	}
	return source.memorySource.LoadBlob(ctx, handle, buffer)
}

func TestSnapshotPrefetchDemandAndCleanup(t *testing.T) {
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 16<<20, "")
	if err != nil {
		t.Fatal(err)
	}
	encoded := treeJSON(t, &data.Node{Name: "file", Type: data.NodeTypeFile})
	id := vaultic.Hash(encoded)
	source := &prefetchTestSource{memorySource: &memorySource{blobs: map[vaultic.ID][]byte{id: encoded}}}
	prefetch, err := newSnapshotTreePrefetch(workingkv.WithPolicy(t.Context(), policy), source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prefetch.close)
	handle := vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id}
	prefetch.schedule(handle)
	prefetch.schedule(handle)
	value, err := prefetch.LoadBlob(t.Context(), handle, nil)
	if err != nil || string(value) != string(encoded) || source.reads.Load() != 1 {
		t.Fatal("prefetch did not reuse one exact read", err, source.reads.Load())
	}
	prefetch.close()
	prefetch.close()
	if policy.Budget().Used != 0 {
		t.Fatal("prefetch retained its reservation", policy.Budget())
	}
}

func TestSnapshotPrefetchCancellationJoinsWorker(t *testing.T) {
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 8<<20, "")
	if err != nil {
		t.Fatal(err)
	}
	id := vaultic.Hash([]byte("tree"))
	source := &prefetchTestSource{memorySource: &memorySource{blobs: map[vaultic.ID][]byte{id: []byte("tree")}}, started: make(chan struct{}, 1), block: true}
	prefetch, err := newSnapshotTreePrefetch(workingkv.WithPolicy(t.Context(), policy), source)
	if err != nil {
		t.Fatal(err)
	}
	prefetch.schedule(vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id})
	<-source.started
	prefetch.close()
	if policy.Budget().Used != 0 {
		t.Fatal("worker was not joined before release")
	}
}

func TestSnapshotPrefetchErrorIsDemandOnly(t *testing.T) {
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 8<<20, "")
	if err != nil {
		t.Fatal(err)
	}
	id := vaultic.Hash([]byte("tree"))
	injected := errors.New("prefetch failure")
	source := &prefetchTestSource{memorySource: &memorySource{blobs: map[vaultic.ID][]byte{id: []byte("tree")}}, failure: injected}
	prefetch, err := newSnapshotTreePrefetch(workingkv.WithPolicy(t.Context(), policy), source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prefetch.close)
	handle := vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id}
	prefetch.schedule(handle)
	if _, err := prefetch.LoadBlob(t.Context(), handle, nil); !errors.Is(err, injected) {
		t.Fatal("demand changed the source error", err)
	}
}

func TestSnapshotPrefetchRejectsUnaccountedSource(t *testing.T) {
	if _, err := newSnapshotTreePrefetch(t.Context(), &memorySource{}); err == nil {
		t.Fatal("unaccounted or unsized prefetch admitted")
	}
}
