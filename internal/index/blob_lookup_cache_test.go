package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type testBlobLookupSession struct {
	ctx       context.Context
	query     func(context.Context, []vaultic.BlobHandle) ([]vaultic.BlobSize, error)
	locations func(context.Context, vaultic.BlobHandle) ([]*pack.PackedBlob, error)
}

func (session *testBlobLookupSession) LookupContext(ctx context.Context, handle vaultic.BlobHandle) ([]*pack.PackedBlob, error) {
	return session.locations(ctx, handle)
}

func TestCachedBlobLocationLookup(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(fmt.Sprint(closing), func(t *testing.T) {
			started := make(chan struct{})
			session := &testBlobLookupSession{ctx: t.Context(), locations: func(ctx context.Context, _ vaultic.BlobHandle) ([]*pack.PackedBlob, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), blobLookupCacheEntryBytes, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer lookup.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := lookup.LookupContext(ctx, vaultic.NewRandomBlobHandle()); done <- err }()
			<-started
			if closing {
				lookup.Close()
			} else {
				cancel()
			}
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation=%v", err)
			}
			if !closing && lookup.ctx.Err() != nil {
				t.Fatal("individual cancellation poisoned lookup")
			}
		})
	}
	for _, failure := range []error{nil, errors.New("location RPC failed")} {
		handle := vaultic.NewRandomBlobHandle()
		blob := &pack.PackedBlob{Pack: vaultic.NewRandomID(), Blob: pack.Blob{BlobHandle: handle, Length: 100}}
		session := &testBlobLookupSession{ctx: t.Context(), locations: func(context.Context, vaultic.BlobHandle) ([]*pack.PackedBlob, error) {
			return []*pack.PackedBlob{blob}, failure
		}}
		local := NewLegacyEngine()
		lookup, err := NewCachedBlobLookup(session, local, blobLookupCacheEntryBytes, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer lookup.Close()
		blobs, err := lookup.LookupContext(t.Context(), handle)
		if !errors.Is(err, failure) {
			t.Fatalf("location err=%v", err)
		}
		if failure == nil && (len(blobs) != 1 || blobs[0] != blob) {
			t.Fatalf("locations=%v", blobs)
		}
		if failure != nil {
			if len(blobs) != 0 {
				t.Fatal("failure returned usable locations")
			}
			local.AddPending(handle, 99)
			if _, _, err := lookup.LookupSizeContext(t.Context(), handle); !errors.Is(err, failure) {
				t.Fatalf("local size hid location failure: %v", err)
			}
		}
	}
}

func (session *testBlobLookupSession) Context() context.Context { return session.ctx }

func (session *testBlobLookupSession) LookupBlobSizesContext(ctx context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
	return session.query(ctx, handles)
}

func TestCachedBlobLookupEvictionAndOverlay(t *testing.T) {
	var calls atomic.Int64
	session := &testBlobLookupSession{ctx: t.Context(), query: func(_ context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		calls.Add(1)
		return make([]vaultic.BlobSize, len(handles)), nil
	}}
	local := NewLegacyEngine()
	lookup, err := NewCachedBlobLookup(session, local, blobLookupCacheEntryBytes, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	handle := vaultic.NewRandomBlobHandle()
	for range 2 {
		results, err := lookup.LookupSizesContext(t.Context(), []vaultic.BlobHandle{handle, handle})
		if err != nil || len(results) != 2 || results[0].Found || results[1].Found {
			t.Fatalf("missing results=%v err=%v", results, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("miss was not cached: calls=%d", calls.Load())
	}
	var admitted atomic.Int64
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			added, err := lookup.AddPendingContext(t.Context(), handle, 123)
			if err != nil {
				t.Error(err)
			}
			if added {
				admitted.Add(1)
			}
		})
	}
	workers.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("admitted %d copies", admitted.Load())
	}
	for range 5 {
		if _, err := lookup.LookupSizesContext(t.Context(), []vaultic.BlobHandle{vaultic.NewRandomBlobHandle()}); err != nil {
			t.Fatal(err)
		}
		if lookup.cache.Len() > 1 {
			t.Fatal("cache exceeded entry budget")
		}
	}
	results, err := lookup.LookupSizesContext(t.Context(), []vaultic.BlobHandle{handle})
	if err != nil || results[0] != (vaultic.BlobSize{Size: 123, Found: true}) {
		t.Fatalf("eviction hid admitted blob: results=%v err=%v", results, err)
	}
}

func TestCachedBlobLookupCoalescesIndependentCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	session := &testBlobLookupSession{ctx: t.Context(), query: func(ctx context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return []vaultic.BlobSize{{Size: 99, Found: true}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), 2*blobLookupCacheEntryBytes, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handle := vaultic.NewRandomBlobHandle()
	done := make(chan error, 1)
	go func() { _, err := lookup.LookupSizesContext(ctx, []vaultic.BlobHandle{handle}); done <- err }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation=%v", err)
	}
	close(release)
	results, err := lookup.LookupSizesContext(t.Context(), []vaultic.BlobHandle{handle})
	if err != nil || results[0] != (vaultic.BlobSize{Size: 99, Found: true}) || calls.Load() != 1 {
		t.Fatalf("shared request lost: results=%v err=%v calls=%d", results, err, calls.Load())
	}
}

func TestCachedBlobLookupOverlayWinsInflightMiss(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	session := &testBlobLookupSession{ctx: t.Context(), query: func(ctx context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		close(started)
		select {
		case <-release:
			return make([]vaultic.BlobSize, len(handles)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	local := NewLegacyEngine()
	lookup, err := NewCachedBlobLookup(session, local, blobLookupCacheEntryBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	handle := vaultic.NewRandomBlobHandle()
	done := make(chan []vaultic.BlobSize, 1)
	go func() {
		result, err := lookup.LookupSizesContext(t.Context(), []vaultic.BlobHandle{handle})
		if err != nil {
			t.Error(err)
		}
		done <- result
	}()
	<-started
	local.AddPending(handle, 456)
	close(release)
	results := <-done
	if len(results) != 1 || results[0] != (vaultic.BlobSize{Size: 456, Found: true}) {
		t.Fatalf("in-flight miss hid new write: %v", results)
	}
}

func TestCachedBlobLookupFailureAndClose(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		failure := errors.New("session unavailable")
		session := &testBlobLookupSession{ctx: t.Context(), query: func(context.Context, []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
			if malformed {
				return nil, nil
			}
			return nil, failure
		}}
		lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), blobLookupCacheEntryBytes, 1)
		if err != nil {
			t.Fatal(err)
		}
		handles := []vaultic.BlobHandle{vaultic.NewRandomBlobHandle()}
		if _, err := lookup.LookupSizesContext(t.Context(), handles); err == nil {
			t.Fatal("failed lookup accepted")
		}
		if _, err := lookup.LookupSizesContext(t.Context(), handles); err == nil {
			t.Fatal("cache forgot sticky failure")
		}
		lookup.Close()
	}
	started := make(chan struct{})
	session := &testBlobLookupSession{ctx: t.Context(), query: func(ctx context.Context, _ []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), blobLookupCacheEntryBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := lookup.LookupSizesContext(t.Context(), []vaultic.BlobHandle{vaultic.NewRandomBlobHandle()})
		done <- err
	}()
	<-started
	lookup.Close()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("close error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close left a lookup running")
	}
	if len(lookup.pending) != 0 || lookup.cache.Len() != 0 {
		t.Fatal("close retained lookup state")
	}
}

func TestCachedBlobLookupHitsDoNotWaitForRPCSlots(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	session := &testBlobLookupSession{ctx: ctx, query: func(context.Context, []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		return []vaultic.BlobSize{{Size: 99, Found: true}}, nil
	}}
	lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), blobLookupCacheEntryBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	handles := []vaultic.BlobHandle{vaultic.NewRandomBlobHandle()}
	if _, err := lookup.LookupSizesContext(t.Context(), handles); err != nil {
		t.Fatal(err)
	}
	lookup.slots <- struct{}{}
	defer func() { <-lookup.slots }()
	caller, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if _, err := lookup.LookupSizesContext(caller, handles); err != nil {
		t.Fatalf("cache hit waited for RPC: %v", err)
	}
	failure := errors.New("session lost")
	cancel(failure)
	if _, err := lookup.LookupSizesContext(t.Context(), handles); !errors.Is(err, failure) {
		t.Fatalf("cached hit hid session loss: %v", err)
	}
}

func TestCachedBlobLookupBoundsInflightBatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, 2)
	var calls atomic.Int64
	session := &testBlobLookupSession{ctx: ctx, query: func(ctx context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		calls.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), blobLookupCacheEntryBytes, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	var callers sync.WaitGroup
	for range 32 {
		handles := make([]vaultic.BlobHandle, vaultic.BlobLookupBatchSize)
		for ordinal := range handles {
			handles[ordinal] = vaultic.NewRandomBlobHandle()
		}
		callers.Go(func() {
			_, err := lookup.LookupSizesContext(ctx, handles)
			if !errors.Is(err, context.Canceled) {
				t.Errorf("shutdown lookup error=%v", err)
			}
		})
	}
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("in-flight batches did not start")
		}
	}
	lookup.mutex.Lock()
	pending := len(lookup.pending)
	lookup.mutex.Unlock()
	if pending != 2*vaultic.BlobLookupBatchSize || calls.Load() != 2 {
		t.Fatalf("pending=%d calls=%d", pending, calls.Load())
	}
	lookup.Close()
	callers.Wait()
	if calls.Load() != 2 {
		t.Fatalf("queued RPC ran after close: %d", calls.Load())
	}
}

type overlayTestSaver struct{ err error }

func (saver *overlayTestSaver) Connections() uint { return 1 }
func (saver *overlayTestSaver) SaveUnpacked(_ context.Context, _ vaultic.FileType, value []byte) (vaultic.ID, error) {
	return vaultic.Hash(value), saver.err
}

func TestSpillingBlobLookup(t *testing.T) {
	session := &testBlobLookupSession{ctx: t.Context(), query: func(_ context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		return make([]vaultic.BlobSize, len(handles)), nil
	}}
	lookup, local, err := NewSpillingBlobLookup(session, t.TempDir(), blobLookupCacheEntryBytes, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	var expected []*pack.PackedBlob
	for cycle := range 32 {
		packID := vaultic.NewRandomID()
		var blobs pack.Blobs
		for ordinal := range 16 {
			handle := vaultic.NewRandomBlobHandle()
			if ordinal%2 == 0 {
				handle.Type = vaultic.TreeBlob
			} else {
				handle.Type = vaultic.DataBlob
			}
			if added, err := lookup.AddPendingContext(t.Context(), handle, 123); err != nil || !added {
				t.Fatalf("admission=%v err=%v", added, err)
			}
			blob := pack.Blob{BlobHandle: handle, Offset: uint(ordinal * 100), Length: 100, UncompressedLength: 123}
			blobs = append(blobs, blob)
			expected = append(expected, &pack.PackedBlob{Pack: packID, Blob: blob})
		}
		if err := local.StorePack(t.Context(), packID, blobs, &overlayTestSaver{}); err != nil {
			t.Fatal(err)
		}
		if err := local.Flush(t.Context(), &overlayTestSaver{}); err != nil {
			t.Fatal(err)
		}
		for range local.Values() {
			t.Fatalf("cycle %d retained exported locations", cycle)
		}
	}
	for _, want := range expected {
		got, err := lookup.LookupContext(t.Context(), want.Handle())
		if err != nil || len(got) != 1 || *got[0] != *want {
			t.Fatalf("spilled location mismatch: %v %v", got, err)
		}
		if size, found, err := lookup.LookupSizeContext(t.Context(), want.Handle()); err != nil || !found || size != 123 {
			t.Fatalf("spilled size=%d found=%v err=%v", size, found, err)
		}
		if added, err := lookup.AddPendingContext(t.Context(), want.Handle(), 123); err != nil || added {
			t.Fatalf("readmitted spilled blob: %v %v", added, err)
		}
	}
	path := lookup.written.path
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("overlay not removed: %v", err)
	}
}

func TestSpillingBlobLookupAutomaticExport(t *testing.T) {
	lookup, local, err := NewSpillingBlobLookup(&testBlobLookupSession{ctx: t.Context()}, t.TempDir(), blobLookupCacheEntryBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	blobs := make(pack.Blobs, 50001)
	for ordinal := range blobs {
		blobs[ordinal] = pack.Blob{BlobHandle: vaultic.NewRandomBlobHandle(), Offset: uint(ordinal * 100), Length: 100, UncompressedLength: 123}
	}
	if err := local.StorePack(t.Context(), vaultic.NewRandomID(), blobs, &overlayTestSaver{}); err != nil {
		t.Fatal(err)
	}
	for range local.Values() {
		t.Fatal("full exported index retained")
	}
	for _, ordinal := range []int{0, 25000, 50000} {
		if size, found, err := lookup.LookupSizeContext(t.Context(), blobs[ordinal].BlobHandle); err != nil || !found || size != 123 {
			t.Fatalf("automatic spill lookup: %d %v %v", size, found, err)
		}
	}
}

func TestSpillingBlobLookupConcurrentHandoff(t *testing.T) {
	lookup, local, err := NewSpillingBlobLookup(&testBlobLookupSession{ctx: t.Context()}, t.TempDir(), blobLookupCacheEntryBytes, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	handle := vaultic.NewRandomBlobHandle()
	if err := local.StorePack(t.Context(), vaultic.NewRandomID(), pack.Blobs{{BlobHandle: handle, Length: 100, UncompressedLength: 123}}, &overlayTestSaver{}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			<-start
			for range 32 {
				if size, found, err := lookup.LookupSizeContext(t.Context(), handle); err != nil || !found || size != 123 {
					t.Errorf("handoff lost blob: %d %v %v", size, found, err)
				}
				if admitted, err := lookup.AddPendingContext(t.Context(), handle, 123); err != nil || admitted {
					t.Errorf("handoff readmitted blob: %v %v", admitted, err)
				}
			}
		})
	}
	close(start)
	if err := local.Flush(t.Context(), &overlayTestSaver{}); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
}

func BenchmarkWrittenBlobLookup(b *testing.B) {
	for _, spilled := range []bool{false, true} {
		b.Run(fmt.Sprint(spilled), func(b *testing.B) {
			lookup, local, err := NewSpillingBlobLookup(&testBlobLookupSession{ctx: b.Context()}, b.TempDir(), 256*blobLookupCacheEntryBytes, 2)
			if err != nil {
				b.Fatal(err)
			}
			defer lookup.Close()
			handles := make([]vaultic.BlobHandle, 256)
			blobs := make(pack.Blobs, len(handles))
			for ordinal := range handles {
				handles[ordinal] = vaultic.NewRandomBlobHandle()
				blobs[ordinal] = pack.Blob{BlobHandle: handles[ordinal], Length: 100, UncompressedLength: 123}
			}
			if err := local.StorePack(b.Context(), vaultic.NewRandomID(), blobs, &overlayTestSaver{}); err != nil {
				b.Fatal(err)
			}
			if spilled {
				if err := local.Flush(b.Context(), &overlayTestSaver{}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := lookup.LookupSizesContext(b.Context(), handles); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestWrittenBlobStoreEncryptionAndFailure(t *testing.T) {
	store, err := newWrittenBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	handle := vaultic.NewRandomBlobHandle()
	packID := vaultic.NewRandomID()
	index := legacyindex.NewIndex()
	index.StorePack(packID, pack.Blobs{{BlobHandle: handle, Length: 100, UncompressedLength: 123}})
	if err := store.StoreIndex(t.Context(), index); err != nil {
		t.Fatal(err)
	}
	iterator, err := store.database.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !iterator.First() {
		t.Fatal("no encrypted entry")
	}
	key, value := bytes.Clone(iterator.Key()), bytes.Clone(iterator.Value())
	if bytes.Contains(key, handle.ID[:]) || bytes.Contains(key, packID[:]) || bytes.Contains(value, packID[:]) {
		t.Fatal("plaintext metadata in overlay")
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	value[len(value)-1] ^= 1
	if err := store.database.Set(key, value, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if blobs, err := store.lookup(t.Context(), handle, false); err == nil || len(blobs) != 0 {
		t.Fatal("tampered entry accepted")
	}
	if blobs, err := store.lookup(t.Context(), vaultic.NewRandomBlobHandle(), true); err == nil || len(blobs) != 0 {
		t.Fatal("tamper failure was not sticky")
	}
	if err := store.StoreIndex(t.Context(), index); err == nil {
		t.Fatal("write after corruption succeeded")
	}
}

func TestSpillingBlobLookupRetainsFailedExport(t *testing.T) {
	t.Run("failed spill after successful export", func(t *testing.T) {
		lookup, local, err := NewSpillingBlobLookup(&testBlobLookupSession{ctx: t.Context()}, t.TempDir(), blobLookupCacheEntryBytes, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer lookup.Close()
		handle := vaultic.NewRandomBlobHandle()
		if err := local.StorePack(t.Context(), vaultic.NewRandomID(), pack.Blobs{{BlobHandle: handle, Length: 100}}, &overlayTestSaver{}); err != nil {
			t.Fatal(err)
		}
		if err := lookup.written.Close(); err != nil {
			t.Fatal(err)
		}
		if err := local.Flush(t.Context(), &overlayTestSaver{}); err == nil {
			t.Fatal("closed spill store accepted handoff")
		}
		if len(local.Lookup(handle)) != 1 {
			t.Fatal("failed spill evicted entries")
		}
		if _, err := lookup.LookupContext(t.Context(), handle); err == nil {
			t.Fatal("local hit hid spill failure")
		}
	})
	session := &testBlobLookupSession{ctx: t.Context()}
	lookup, local, err := NewSpillingBlobLookup(session, t.TempDir(), blobLookupCacheEntryBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	handle := vaultic.NewRandomBlobHandle()
	if err := local.StorePack(t.Context(), vaultic.NewRandomID(), pack.Blobs{{BlobHandle: handle, Length: 100}}, &overlayTestSaver{}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("export failed")
	if err := local.Flush(t.Context(), &overlayTestSaver{err: failure}); !errors.Is(err, failure) {
		t.Fatalf("export err=%v", err)
	}
	if len(local.Lookup(handle)) != 1 {
		t.Fatal("failed export evicted entries")
	}
	if err := lookup.written.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := lookup.LookupSizeContext(t.Context(), handle); err == nil {
		t.Fatal("local hit hid closed overlay")
	}
}

func BenchmarkCachedBlobLookup(b *testing.B) {
	handles := make([]vaultic.BlobHandle, vaultic.BlobLookupBatchSize)
	for ordinal := range handles {
		handles[ordinal] = vaultic.NewRandomBlobHandle()
	}
	for _, cached := range []bool{false, true} {
		name := "cold"
		if cached {
			name = "warm"
		}
		b.Run(name, func(b *testing.B) {
			var calls atomic.Int64
			session := &testBlobLookupSession{ctx: b.Context(), query: func(_ context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
				calls.Add(1)
				return make([]vaultic.BlobSize, len(handles)), nil
			}}
			lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), len(handles)*blobLookupCacheEntryBytes, 4)
			if err != nil {
				b.Fatal(err)
			}
			defer lookup.Close()
			if _, err := lookup.LookupSizesContext(b.Context(), handles); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			before := calls.Load()
			for b.Loop() {
				if !cached {
					lookup.cache.Purge()
				}
				if _, err := lookup.LookupSizesContext(b.Context(), handles); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(calls.Load()-before)/float64(b.N), "batches/op")
		})
	}
}
