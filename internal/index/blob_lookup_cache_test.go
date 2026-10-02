package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
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
		if stats := lookup.Stats(); stats.LocationRPCs != 1 || stats.LocationRPCNanoseconds == 0 {
			t.Fatalf("location counters=%+v", stats)
		}
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

func TestCachedBlobLookupRPCDeadlineAttribution(t *testing.T) {
	session := &testBlobLookupSession{ctx: t.Context(), query: func(context.Context, []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		return nil, context.DeadlineExceeded
	}}
	local := NewLegacyEngine()
	lookup, err := NewCachedBlobLookup(session, local, blobLookupCacheEntryBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	handle := vaultic.NewRandomBlobHandle()
	added, err := lookup.AddPendingContext(t.Context(), handle, 99)
	if added || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "blob size lookup RPC for 1 handles after ") {
		t.Fatalf("admission added=%v error=%v", added, err)
	}
	local.AddPending(handle, 99)
	if _, _, subsequent := lookup.LookupSizeContext(t.Context(), handle); subsequent != err {
		t.Fatalf("shared failure lost attribution: first=%v subsequent=%v", err, subsequent)
	}
	if stats := lookup.Stats(); stats.SizeRPCs != 1 || stats.NegativeHits != 0 {
		t.Fatalf("failed RPC retried or cached as missing: %+v", stats)
	}
}

func TestCachedBlobLookupStats(t *testing.T) {
	positive, negative, third := vaultic.NewRandomBlobHandle(), vaultic.NewRandomBlobHandle(), vaultic.NewRandomBlobHandle()
	session := &testBlobLookupSession{ctx: t.Context(), query: func(_ context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
		results := make([]vaultic.BlobSize, len(handles))
		for ordinal, handle := range handles {
			results[ordinal] = vaultic.BlobSize{Found: handle == positive, Size: 42}
		}
		return results, nil
	}}
	lookup, err := NewCachedBlobLookup(session, NewLegacyEngine(), 2*blobLookupCacheEntryBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	for _, handles := range [][]vaultic.BlobHandle{{positive, negative}, {positive, negative}, {third}} {
		if _, err := lookup.LookupSizesContext(t.Context(), handles); err != nil {
			t.Fatal(err)
		}
	}
	lookup.Close()
	stats := lookup.Stats()
	if stats.CacheHits != 2 || stats.NegativeHits != 1 || stats.CacheMisses != 5 || stats.Evictions != 1 ||
		stats.PeakEntries != 2 || stats.Capacity != 2 || stats.AccountedCapacityBytes != 2*blobLookupCacheEntryBytes ||
		stats.SizeRPCs != 2 || stats.SizeHandles != 3 || stats.SizeRPCNanoseconds == 0 || stats.LocationRPCs != 0 {
		t.Fatalf("unexpected closed cache stats: %+v", stats)
	}
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
	state := lookup.written.WorkingState()
	if state.CommittedEntries != 512 || state.CommittedEncodedBytes != 512*148 ||
		state.RetainedEntriesUpperBound != 512 || state.Backend != "pebble" || !state.ScratchBytesKnown {
		t.Fatalf("overlay working state: %+v", state)
	}
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
	if state := lookup.written.WorkingState(); !state.Closed || state.RetainedEntriesUpperBound != 0 {
		t.Fatalf("closed overlay working state: %+v", state)
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

func BenchmarkPhase35M0WrittenBlobs(b *testing.B) {
	before := telemetry.ReadWorkingRuntime()
	var final telemetry.WorkingStateSnapshot
	for range b.N {
		lookup, local, err := NewSpillingBlobLookup(&testBlobLookupSession{ctx: b.Context()}, b.TempDir(), 64<<20, 2)
		if err != nil {
			b.Fatal(err)
		}
		for packNumber := range 16 {
			blobs := make(pack.Blobs, 512)
			for ordinal := range blobs {
				blobs[ordinal] = pack.Blob{BlobHandle: vaultic.BlobHandle{Type: vaultic.DataBlob,
					ID: vaultic.Hash([]byte(fmt.Sprintf("phase35-blob-%d-%d", packNumber, ordinal)))},
					Offset: uint(ordinal * 128), Length: 128, UncompressedLength: 128}
			}
			packID := vaultic.Hash([]byte(fmt.Sprintf("phase35-pack-%d", packNumber)))
			if err := local.StorePack(b.Context(), packID, blobs, &overlayTestSaver{}); err != nil {
				b.Fatal(err)
			}
			if err := local.Flush(b.Context(), &overlayTestSaver{}); err != nil {
				b.Fatal(err)
			}
			got, err := lookup.LookupContext(b.Context(), blobs[0].BlobHandle)
			if err != nil || len(got) != 1 || got[0].Pack != packID {
				b.Fatalf("overlay parity: %v %v", got, err)
			}
		}
		final = lookup.written.WorkingState()
		if final.CommittedEntries != 8192 || final.CommittedEncodedBytes != 8192*148 {
			b.Fatalf("working-state parity: %+v", final)
		}
		path := lookup.written.path
		if err := lookup.Close(); err != nil {
			b.Fatal(err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			b.Fatalf("overlay cleanup: %v", err)
		}
	}
	b.StopTimer()
	encoded, err := json.Marshal(struct {
		Before telemetry.WorkingRuntimeSnapshot `json:"before"`
		After  telemetry.WorkingRuntimeSnapshot `json:"after"`
		State  telemetry.WorkingStateSnapshot   `json:"state"`
	}{before, telemetry.ReadWorkingRuntime(), final})
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("phase35_m0=%s", encoded)
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
	rows, err := store.database.Scan(t.Context(), nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatal("no encrypted entry")
	}
	key, value := rows[0].Key, rows[0].Value
	if bytes.Contains(key, handle.ID[:]) || bytes.Contains(key, packID[:]) || bytes.Contains(value, packID[:]) {
		t.Fatal("plaintext metadata in overlay")
	}
	value[len(value)-1] ^= 1
	if err := store.database.Put(t.Context(), []workingkv.Entry{{Key: key, Value: value}}); err != nil {
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

func TestM3OverlayParity(t *testing.T) {
	handle := vaultic.NewRandomBlobHandle()
	idx := legacyindex.NewIndex()
	for _, offset := range []uint{0, 64, 128} {
		idx.StorePack(vaultic.NewRandomID(), pack.Blobs{{BlobHandle: handle, Offset: offset, Length: 32, UncompressedLength: 64}})
	}
	for _, mode := range []workingkv.Mode{workingkv.ModeRAM, workingkv.ModeKV} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			policy, _ := workingkv.NewPolicy(mode, 4<<20, root)
			ctx := workingkv.WithPolicy(t.Context(), policy)
			store, err := newWrittenBlobStoreContext(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.StoreIndex(ctx, idx); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := store.StoreIndex(ctx, idx); err != nil {
					t.Fatal(err)
				}
			}
			blobs, err := store.lookup(ctx, handle, false)
			if err != nil || len(blobs) != 3 {
				t.Fatal("distinct/duplicate overlay locations", err, len(blobs))
			}
			for _, blob := range blobs {
				if blob.Blob.Length != 32 || blob.Blob.UncompressedLength != 64 {
					t.Fatal("location mismatch")
				}
			}
			if mode == workingkv.ModeRAM {
				entries, _ := os.ReadDir(root)
				if len(entries) != 0 {
					t.Fatal("RAM overlay used scratch")
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if policy.Budget().Used != 0 {
				t.Fatal("overlay reservation leaked")
			}
		})
	}
}

func TestM3LookupCapacity(t *testing.T) {
	for _, mode := range []workingkv.Mode{workingkv.ModeRAM, workingkv.ModeKV} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			policy, _ := workingkv.NewPolicy(mode, 4<<20, root)
			ctx := workingkv.WithPolicy(t.Context(), policy)
			lookup, _, err := NewSpillingBlobLookupContext(ctx, &testBlobLookupSession{ctx: ctx}, root, 64<<20, 2)
			if err != nil {
				t.Fatal(err)
			}
			if lookup.Stats().AccountedCapacityBytes > (4<<20)/8 || policy.Budget().Used == 0 {
				t.Fatal("LRU ignored shared envelope")
			}
			if err := lookup.Close(); err != nil {
				t.Fatal(err)
			}
			if err := lookup.Close(); err != nil {
				t.Fatal(err)
			}
			if policy.Budget().Used != 0 {
				t.Fatal("lookup reservation leaked")
			}
		})
	}
}

func TestWrittenBlobWorkingStateFailureAndOverwrite(t *testing.T) {
	store, err := newWrittenBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := store.path
	defer os.RemoveAll(path)
	idx := legacyindex.NewIndex()
	idx.StorePack(vaultic.NewRandomID(), pack.Blobs{{BlobHandle: vaultic.NewRandomBlobHandle(), Length: 1}})
	for range 2 {
		if err := store.StoreIndex(t.Context(), idx); err != nil {
			t.Fatal(err)
		}
	}
	if state := store.WorkingState(); state.CommittedEntries != 2 || state.RetainedEntriesUpperBound != 2 {
		t.Fatalf("overwrite upper bound: %+v", state)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.StoreIndex(ctx, idx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if state := store.WorkingState(); state.CommittedEntries != 2 || state.ObservedBufferBytes != 0 {
		t.Fatalf("canceled write counted: %+v", state)
	}
	store.path = path + "\x00"
	if err := store.Close(); err == nil {
		t.Fatal("cleanup failure missing")
	}
	if state := store.WorkingState(); !state.Closed || state.ScratchBytesKnown {
		t.Fatalf("failed cleanup claimed removed scratch: %+v", state)
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
