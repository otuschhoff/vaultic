package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
