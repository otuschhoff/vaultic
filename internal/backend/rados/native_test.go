//go:build rados || radosfake

package rados

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/options"
)

type fakeRawStore struct {
	mu             sync.Mutex
	objects        map[string][]byte
	modTimes       map[string]time.Time
	failWrites     map[string]error
	failReads      map[string]error
	telemetry      backend.CapacityTelemetrySample
	casCalls       map[string]int
	casMutate      map[string][]byte
	ageKnown       map[string]bool
	listDelay      time.Duration
	listBlock      <-chan struct{}
	listCalls      int
	listPrefixes   []string
	beforeWrite    func(*fakeRawStore, string, []byte, bool) error
	beforeRemove   func(string)
	beforeStat     func(string)
	beforeRead     func(string)
	beforeCapacity func()
	afterRemove    func(string)
	closed         bool
}
type fakeNativeObjectIterator struct {
	values  []string
	index   int
	closed  bool
	started chan struct{}
	release <-chan struct{}
}

func (iterator *fakeNativeObjectIterator) Next() bool {
	if iterator.started != nil {
		close(iterator.started)
		iterator.started = nil
	}
	if iterator.release != nil {
		<-iterator.release
	}
	iterator.index++
	return iterator.index < len(iterator.values)
}

func (iterator *fakeNativeObjectIterator) Value() string {
	return iterator.values[iterator.index]
}

func (*fakeNativeObjectIterator) Err() error { return nil }

func (iterator *fakeNativeObjectIterator) Close() { iterator.closed = true }

func newFakeNative() (*nativeDriver, *fakeRawStore) {
	raw := &fakeRawStore{
		objects:    make(map[string][]byte),
		modTimes:   make(map[string]time.Time),
		failWrites: make(map[string]error),
		failReads:  make(map[string]error),
		casCalls:   make(map[string]int),
		casMutate:  make(map[string][]byte),
		ageKnown:   make(map[string]bool),
	}
	native := &nativeDriver{
		raw:                 raw,
		prefix:              "repository/",
		pool:                "pool",
		opTTL:               5 * time.Second,
		publicationLeaseKey: derivePublicationLeaseKey("test-cephx-secret", "cluster-a", "pool", "namespace-a", "repository/", "client.vaultic"),
		readSlots:           make(chan struct{}, maxDetachedReadOperations),
		writeSlots:          make(chan struct{}, maxDetachedWriteOperations),
	}
	native.resetOrphanGCState()
	return native, raw
}

func (store *fakeRawStore) stat(name string) (uint64, error) {
	if store.beforeStat != nil {
		store.beforeStat(name)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.failReads[name]; err != nil {
		return 0, err
	}
	data, ok := store.objects[name]
	if !ok {
		return 0, syscall.ENOENT
	}
	return uint64(len(data)), nil
}

func (store *fakeRawStore) read(name string, buffer []byte, offset uint64) (int, error) {
	if store.beforeRead != nil {
		store.beforeRead(name)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	data, ok := store.objects[name]
	if !ok {
		return 0, syscall.ENOENT
	}
	return copy(buffer, data[offset:]), nil
}

func (store *fakeRawStore) write(name string, data []byte, exclusive bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.beforeWrite != nil {
		if err := store.beforeWrite(store, name, data, exclusive); err != nil {
			return err
		}
	}
	if err := store.failWrites[name]; err != nil {
		return err
	}
	if _, ok := store.objects[name]; ok && exclusive {
		return syscall.EEXIST
	}
	store.objects[name] = bytes.Clone(data)
	store.modTimes[name] = time.Now()
	store.ageKnown[name] = true
	return nil
}

func (store *fakeRawStore) compareAndSwap(name string, expected []byte, replacement []byte, createOnly bool) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.casCalls[name]++
	current, exists := store.objects[name]
	if createOnly {
		if exists {
			return false, nil
		}
		store.objects[name] = bytes.Clone(replacement)
		store.modTimes[name] = time.Now()
		store.ageKnown[name] = true
		return true, nil
	}
	if replacementCurrent, ok := store.casMutate[name]; ok {
		delete(store.casMutate, name)
		store.objects[name] = bytes.Clone(replacementCurrent)
		store.modTimes[name] = time.Now()
		store.ageKnown[name] = true
		current = store.objects[name]
		exists = true
	}
	if !exists {
		return false, nil
	}
	if !bytes.Equal(current, expected) {
		return false, nil
	}
	store.objects[name] = bytes.Clone(replacement)
	store.modTimes[name] = time.Now()
	store.ageKnown[name] = true
	return true, nil
}

func (store *fakeRawStore) remove(name string) error {
	if store.beforeRemove != nil {
		store.beforeRemove(name)
	}
	store.mu.Lock()
	if _, ok := store.objects[name]; !ok {
		store.mu.Unlock()
		return syscall.ENOENT
	}
	delete(store.objects, name)
	delete(store.modTimes, name)
	delete(store.ageKnown, name)
	store.mu.Unlock()
	if store.afterRemove != nil {
		store.afterRemove(name)
	}
	return nil
}

func (store *fakeRawStore) listPage(ctx context.Context, prefix string, after string, limit int) ([]string, string, bool, error) {
	store.mu.Lock()
	store.listCalls++
	store.listPrefixes = append(store.listPrefixes, prefix)
	store.mu.Unlock()
	if store.listDelay > 0 {
		timer := time.NewTimer(store.listDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, "", false, ctx.Err()
		case <-timer.C:
		}
	}
	if store.listBlock != nil {
		select {
		case <-ctx.Done():
			return nil, "", false, ctx.Err()
		case <-store.listBlock:
		}
	}
	store.mu.Lock()
	names := make([]string, 0)
	for name := range store.objects {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	store.mu.Unlock()
	sort.Strings(names)
	if limit <= 0 {
		limit = len(names)
	}
	start := 0
	if after != "" {
		start = sort.SearchStrings(names, after)
		for start < len(names) && names[start] <= after {
			start++
		}
	}
	if start >= len(names) {
		return nil, "", true, nil
	}
	end := min(len(names), start+limit)
	page := append([]string{}, names[start:end]...)
	if end >= len(names) {
		return page, "", true, nil
	}
	return page, page[len(page)-1], false, nil
}

func (store *fakeRawStore) close() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
}

func (store *fakeRawStore) isClosed() bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.closed
}

func (store *fakeRawStore) statWithModTime(name string) (uint64, time.Time, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	data, ok := store.objects[name]
	if !ok {
		return 0, time.Time{}, false, syscall.ENOENT
	}
	modified, haveModified := store.modTimes[name]
	ageKnown := store.ageKnown[name]
	return uint64(len(data)), modified, haveModified && ageKnown, nil
}

func (store *fakeRawStore) setModified(name string, modified time.Time, known bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.objects[name]; !exists {
		return
	}
	store.modTimes[name] = modified
	store.ageKnown[name] = known
}

func (store *fakeRawStore) namesWithPrefix(prefix string) []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	names := make([]string, 0)
	for name := range store.objects {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (store *fakeRawStore) casCallCount(name string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.casCalls[name]
}

func (store *fakeRawStore) listPageCallCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.listCalls
}

func runSweepCycles(t *testing.T, native *nativeDriver, cycles int, grace time.Duration, deleteLimit int, pageLimit int) {
	t.Helper()
	for range cycles {
		if err := native.sweepOrphanChunks(t.Context(), time.Now(), grace, deleteLimit, pageLimit); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

func TestNativeListCursorRegistryPreservesExactIteratorPosition(t *testing.T) {
	values := make([]string, 0, 1500)
	for index := range 1500 {
		values = append(values, fmt.Sprintf("repository/chunks/%04d", index))
	}
	iterator := &fakeNativeObjectIterator{values: values, index: -1}
	registry := &nativeListCursorRegistry{}
	defer registry.close()
	createCalls := 0
	create := func() (nativeObjectIterator, error) {
		createCalls++
		return iterator, nil
	}
	cursor := ""
	collected := make([]string, 0, len(values))
	for {
		page, next, done, err := registry.page(t.Context(), "repository/chunks/", cursor, 17, create)
		if err != nil {
			t.Fatal(err)
		}
		collected = append(collected, page...)
		if done {
			break
		}
		if next == "" || next == cursor {
			t.Fatalf("cursor did not advance: current=%q next=%q", cursor, next)
		}
		cursor = next
	}
	if createCalls != 1 {
		t.Fatalf("iterator create calls = %d, want 1", createCalls)
	}
	if !slices.Equal(collected, values) {
		t.Fatal("paged iterator repeated, skipped, or reordered objects")
	}
	if !iterator.closed {
		t.Fatal("exhausted iterator was not closed")
	}
}

func TestNativeListCursorRegistryExpiresAbandonedIterator(t *testing.T) {
	iterator := &fakeNativeObjectIterator{values: []string{"repository/a", "repository/b"}, index: -1}
	registry := &nativeListCursorRegistry{}
	page, cursor, done, err := registry.page(t.Context(), "repository/", "", 1, func() (nativeObjectIterator, error) {
		return iterator, nil
	})
	if err != nil || done || len(page) != 1 || cursor == "" {
		t.Fatalf("first page = %v cursor=%q done=%v err=%v", page, cursor, done, err)
	}
	registry.mu.Lock()
	entry := registry.cursors[cursor]
	entry.expires = time.Now().Add(-time.Second)
	registry.cursors[cursor] = entry
	registry.mu.Unlock()
	_, _, _, err = registry.page(t.Context(), "repository/", cursor, 1, func() (nativeObjectIterator, error) {
		t.Fatal("expired cursor must not create a replacement iterator")
		return nil, nil
	})
	if !errors.Is(err, errNativeListCursorExpired) {
		t.Fatalf("expired cursor error = %v", err)
	}
	if !iterator.closed {
		t.Fatal("expired iterator was not closed")
	}
}

func TestNativeListCursorRegistryCloseWaitsForCheckedOutIterator(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	iterator := &fakeNativeObjectIterator{
		values: []string{"repository/a", "repository/b"}, index: -1,
		started: started, release: release,
	}
	registry := &nativeListCursorRegistry{}
	pageDone := make(chan error, 1)
	go func() {
		_, _, _, err := registry.page(t.Context(), "repository/", "", 1, func() (nativeObjectIterator, error) {
			return iterator, nil
		})
		pageDone <- err
	}()
	<-started
	closeDone := make(chan struct{})
	go func() {
		registry.close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("registry close returned while iterator was checked out")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-pageDone; !errors.Is(err, errNativeListCursorClosed) {
		t.Fatalf("page error during close = %v", err)
	}
	<-closeDone
	if !iterator.closed {
		t.Fatal("checked-out iterator was not closed after registry shutdown")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.cursors) != 0 {
		t.Fatal("iterator was reinserted after registry shutdown")
	}
}

type expiredCursorRawStore struct {
	*fakeRawStore
}

func (store *expiredCursorRawStore) listPage(context.Context, string, string, int) ([]string, string, bool, error) {
	return nil, "", false, errNativeListCursorExpired
}

func TestNativeOrphanSweepPersistsExpiredCursorReset(t *testing.T) {
	native, raw := newFakeNative()
	native.raw = &expiredCursorRawStore{fakeRawStore: raw}
	native.gcState = orphanGCState{
		phase: orphanGCCollectRefs, manifestCursor: "expired",
		referenced: map[string]struct{}{}, publicationEpoch: native.publicationEpoch.Load(),
	}
	state := native.gcState
	_, _, err := native.collectManifestReferencesPage(t.Context(), &state, 1)
	if !errors.Is(err, errNativeListCursorExpired) {
		t.Fatalf("expired cursor error = %v", err)
	}
	native.gcMu.Lock()
	persisted := native.gcState.manifestCursor
	native.gcMu.Unlock()
	if persisted != "" {
		t.Fatalf("persisted manifest cursor = %q, want reset", persisted)
	}
}

func (store *fakeRawStore) capacity(context.Context, string) (backend.CapacityTelemetrySample, error) {
	store.mu.Lock()
	sample := store.telemetry
	beforeCapacity := store.beforeCapacity
	store.mu.Unlock()
	if beforeCapacity != nil {
		beforeCapacity()
	}
	return sample, nil
}

func TestNativeCapacityTelemetryUsesRawProvider(t *testing.T) {
	native, raw := newFakeNative()
	raw.telemetry = backend.CapacityTelemetrySample{
		TotalRawBytes:        1024,
		FreeRawBytes:         768,
		PoolMaxAvailRawBytes: 500,
		RawAmplification:     2,
		Health:               "nearfull",
		PoolReplicaSizeKnown: true,
		PoolReplicaSize:      3,
		PoolMinSizeKnown:     true,
		PoolMinSize:          2,
	}
	sample, err := native.capacity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if sample.TotalRawBytes != 1024 || sample.FreeRawBytes != 768 || sample.PoolReplicaSize != 3 || !sample.PoolReplicaSizeKnown {
		t.Fatalf("unexpected sample: %#v", sample)
	}
}

func TestNativeCloseWaitsForDetachedCapacityTelemetry(t *testing.T) {
	native, raw := newFakeNative()
	started := make(chan struct{})
	release := make(chan struct{})
	raw.beforeCapacity = func() {
		close(started)
		<-release
	}

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	capacityResult := make(chan error, 1)
	go func() {
		_, err := native.capacity(ctx)
		capacityResult <- err
	}()
	<-started
	if err := <-capacityResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked capacity error = %v, want context deadline exceeded", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- native.close() }()
	select {
	case err := <-closed:
		t.Fatalf("close returned before capacity telemetry settled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if raw.isClosed() {
		t.Fatal("raw store closed while capacity telemetry was still in flight")
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not finish after capacity telemetry completed")
	}
}

func TestNativeChunkPublicationLifecycle(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/object"
	data := bytes.Repeat([]byte("a"), chunkBytes+17)

	if err := native.put(t.Context(), name, data, true); err != nil {
		t.Fatal(err)
	}
	if size, err := native.stat(t.Context(), name); err != nil || size != int64(len(data)) {
		t.Fatalf("stat = %d, %v", size, err)
	}
	ranged, err := native.read(t.Context(), name, chunkBytes-5, 12)
	if err != nil || !bytes.Equal(ranged, data[chunkBytes-5:chunkBytes+7]) {
		t.Fatalf("cross-chunk range = %d bytes, %v", len(ranged), err)
	}

	if err := native.put(t.Context(), name, data, true); !errors.Is(err, ErrExists) {
		t.Fatalf("identical native retry error = %v, want ErrExists for backend verification", err)
	}
	if _, err := native.read(t.Context(), name, 0, 0); err != nil {
		t.Fatalf("identical retry removed committed chunks: %v", err)
	}

	conflict := bytes.Repeat([]byte("b"), chunkBytes+17)
	conflictDigest := hashBytes(conflict)
	if err := native.put(t.Context(), name, conflict, true); !errors.Is(err, ErrExists) {
		t.Fatalf("conflicting publication error = %v, want ErrExists", err)
	}
	for ordinal := 0; ordinal < 2; ordinal++ {
		if _, ok := raw.objects[native.chunkName(name, conflictDigest, ordinal)]; !ok {
			t.Fatalf("conflicting publication removed deferred chunk %d", ordinal)
		}
	}

	oldDigest := hashBytes(data)
	replacement := bytes.Repeat([]byte("c"), chunkBytes+3)
	if err := native.put(t.Context(), name, replacement, false); err != nil {
		t.Fatal(err)
	}
	for ordinal := 0; ordinal < 2; ordinal++ {
		if _, ok := raw.objects[native.chunkName(name, oldDigest, ordinal)]; !ok {
			t.Fatalf("overwrite removed old chunk %d before orphan GC", ordinal)
		}
	}
	if err := native.remove(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	if pending, err := native.reclamationPending(t.Context(), name); err != nil || !pending {
		t.Fatalf("reclamation after logical remove = pending %v, err %v", pending, err)
	}
	chunks := raw.namesWithPrefix(path.Join(native.prefix, ".vaultic-rados", "chunks") + "/")
	if len(chunks) == 0 {
		t.Fatal("remove synchronously deleted published chunks")
	}
	for _, chunk := range chunks {
		raw.setModified(chunk, time.Now().Add(-2*time.Hour), true)
	}
	runSweepCycles(t, native, 12, time.Minute, 16, 32)
	if chunks = raw.namesWithPrefix(path.Join(native.prefix, ".vaultic-rados", "chunks") + "/"); len(chunks) != 0 {
		t.Fatalf("orphan GC left chunks: %v", chunks)
	}
	if pending, err := native.reclamationPending(t.Context(), name); err != nil || pending {
		t.Fatalf("reclamation after chunk GC = pending %v, err %v", pending, err)
	}
}

func TestNativeRemoveSameContentRepublishKeepsPublishedChunks(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/remove-republish"
	payload := bytes.Repeat([]byte("r"), chunkBytes+17)
	if err := native.put(t.Context(), name, payload, true); err != nil {
		t.Fatal(err)
	}

	republishDone := make(chan error, 1)
	var once sync.Once
	raw.afterRemove = func(removed string) {
		if removed != name {
			return
		}
		once.Do(func() {
			go func() { republishDone <- native.put(t.Context(), name, payload, true) }()
		})
	}
	if err := native.remove(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	if republishErr := <-republishDone; republishErr != nil {
		t.Fatalf("same-content republish failed: %v", republishErr)
	}
	got, err := native.read(t.Context(), name, 0, 0)
	if err != nil {
		t.Fatalf("republished object unreadable after remove: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("republished payload mismatch")
	}
}

func TestLogicalToRawHeadroomForRemainingPoolQuota(t *testing.T) {
	remainingLogical := saturatingSub(100, 80)
	if got := logicalToRawHeadroom(remainingLogical, 3); got != 60 {
		t.Fatalf("raw remaining quota = %d, want 60", got)
	}
	if got := logicalToRawHeadroom(saturatingSub(80, 100), 3); got != 0 {
		t.Fatalf("exhausted quota headroom = %d, want 0", got)
	}
	if got := logicalToRawHeadroom(math.MaxUint64, 3); got != math.MaxUint64 {
		t.Fatalf("overflowing raw headroom = %d, want saturation", got)
	}
}

func TestNativeReplacementKeepsChunksForReaderHoldingOldManifest(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/generation-reader"
	oldData := bytes.Repeat([]byte("o"), chunkBytes+17)
	newData := bytes.Repeat([]byte("n"), chunkBytes+23)
	if err := native.put(t.Context(), name, oldData, true); err != nil {
		t.Fatal(err)
	}

	oldChunk := native.chunkName(name, hashBytes(oldData), 0)
	readerStarted := make(chan struct{})
	releaseReader := make(chan struct{})
	var blocked atomic.Bool
	raw.beforeRead = func(readName string) {
		if readName != oldChunk || !blocked.CompareAndSwap(false, true) {
			return
		}
		close(readerStarted)
		<-releaseReader
	}

	readResult := make(chan []byte, 1)
	readError := make(chan error, 1)
	go func() {
		data, err := native.read(t.Context(), name, 0, 0)
		readResult <- data
		readError <- err
	}()
	<-readerStarted
	current, swapped, err := native.compareAndSwap(t.Context(), name, oldData, newData)
	if err != nil {
		t.Fatal(err)
	}
	if !swapped || !bytes.Equal(current, newData) {
		t.Fatalf("CAS replacement = swapped %v, current length %d", swapped, len(current))
	}
	close(releaseReader)
	if err := <-readError; err != nil {
		t.Fatalf("old-generation reader failed after replacement: %v", err)
	}
	if got := <-readResult; !bytes.Equal(got, oldData) {
		t.Fatal("old-generation reader returned replacement data")
	}
}

func TestNativeFailedPublicationDefersCreatedChunkCleanup(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/object"
	data := bytes.Repeat([]byte("x"), chunkBytes+1)
	raw.failWrites[name] = syscall.EIO

	if err := native.put(context.Background(), name, data, true); err == nil {
		t.Fatal("publication unexpectedly succeeded")
	}
	chunks := raw.namesWithPrefix(path.Join(native.prefix, ".vaultic-rados", "chunks") + "/")
	if len(chunks) != 2 {
		t.Fatalf("failed publication chunk count = %d, want 2", len(chunks))
	}
}

func TestNativeFailedPublicationConflictKeepsChunksReferencedByWinningManifest(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/same-payload"
	data := bytes.Repeat([]byte("k"), chunkBytes+1)
	_, winnerManifest, err := encodeManifestForBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	raw.beforeWrite = func(store *fakeRawStore, writeName string, _ []byte, exclusive bool) error {
		if writeName != name || !exclusive {
			return nil
		}
		if _, exists := store.objects[name]; exists {
			return nil
		}
		store.objects[name] = bytes.Clone(winnerManifest)
		return syscall.EEXIST
	}

	if err := native.put(t.Context(), name, data, true); !errors.Is(err, ErrExists) {
		t.Fatalf("simulated same-payload publication conflict error = %v, want ErrExists", err)
	}
	got, err := native.read(t.Context(), name, 0, 0)
	if err != nil {
		t.Fatalf("winner read failed after conflict cleanup: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("winner payload mismatch after conflict cleanup")
	}
}

func TestNativeFailedPublicationConflictRetainsLoserChunksForDeferredGC(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/different-payload"
	winner := bytes.Repeat([]byte("w"), chunkBytes+1)
	loser := bytes.Repeat([]byte("l"), chunkBytes+1)
	winnerDescriptor, winnerManifest, err := encodeManifestForBytes(winner)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := range winnerDescriptor.Chunks {
		start := ordinal * chunkBytes
		end := min(len(winner), start+chunkBytes)
		raw.objects[native.chunkName(name, winnerDescriptor.Digest, ordinal)] = bytes.Clone(winner[start:end])
	}
	raw.beforeWrite = func(store *fakeRawStore, writeName string, _ []byte, exclusive bool) error {
		if writeName != name || !exclusive {
			return nil
		}
		if _, exists := store.objects[name]; exists {
			return nil
		}
		store.objects[name] = bytes.Clone(winnerManifest)
		return syscall.EEXIST
	}

	if err := native.put(t.Context(), name, loser, true); !errors.Is(err, ErrExists) {
		t.Fatalf("simulated differing-payload publication conflict error = %v, want ErrExists", err)
	}
	for ordinal := 0; ordinal < 2; ordinal++ {
		if _, exists := raw.objects[native.chunkName(name, hashBytes(loser), ordinal)]; !exists {
			t.Fatalf("loser chunk %d missing before GC", ordinal)
		}
	}
	got, err := native.read(t.Context(), name, 0, 0)
	if err != nil {
		t.Fatalf("winner read failed after loser cleanup: %v", err)
	}
	if !bytes.Equal(got, winner) {
		t.Fatalf("winner payload mismatch after loser cleanup")
	}
}

func TestNativePublicationInterleavingLoserNeverDeletesReusedChunks(t *testing.T) {
	native, _ := newFakeNative()
	name := "repository/data/00/interleaving"
	payload := bytes.Repeat([]byte("q"), chunkBytes+1)

	_, loserManifest, releaseLease, err := native.preparePublication(t.Context(), name, payload)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseLease.release()

	if err := native.put(t.Context(), name, payload, true); err != nil {
		t.Fatalf("winner publication failed: %v", err)
	}
	if err := native.writeRaw(context.Background(), name, loserManifest, true); !errors.Is(err, ErrExists) {
		t.Fatalf("loser publication error = %v, want ErrExists", err)
	}

	got, err := native.read(t.Context(), name, 0, 0)
	if err != nil {
		t.Fatalf("winner read failed after loser conflict: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("winner payload mismatch after interleaving")
	}
}

func TestNativeOrphanChunkGCSkipsWithoutKnownAge(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/no-age"
	payload := bytes.Repeat([]byte("n"), chunkBytes+1)
	raw.failWrites[name] = syscall.EIO

	if err := native.put(context.Background(), name, payload, true); err == nil {
		t.Fatal("publication unexpectedly succeeded")
	}
	chunks := raw.namesWithPrefix(path.Join(native.prefix, ".vaultic-rados", "chunks") + "/")
	if len(chunks) == 0 {
		t.Fatal("expected orphan chunks")
	}
	for _, chunk := range chunks {
		raw.setModified(chunk, time.Now().Add(-2*time.Hour), false)
	}
	for range 6 {
		if err := native.sweepOrphanChunks(t.Context(), time.Now(), time.Minute, 16, 32); err != nil {
			t.Fatal(err)
		}
	}
	after := raw.namesWithPrefix(path.Join(native.prefix, ".vaultic-rados", "chunks") + "/")
	if len(after) != len(chunks) {
		t.Fatalf("unknown-age GC removed chunks: before=%d after=%d", len(chunks), len(after))
	}
}

func TestNativeOrphanChunkGCDeletesOnlyOldUnreferencedWithoutLease(t *testing.T) {
	native, raw := newFakeNative()
	orphanObject := "repository/data/00/orphan"
	orphanPayload := bytes.Repeat([]byte("o"), chunkBytes+1)
	raw.failWrites[orphanObject] = syscall.EIO
	if err := native.put(context.Background(), orphanObject, orphanPayload, true); err == nil {
		t.Fatal("orphan publication unexpectedly succeeded")
	}
	orphanChunks := []string{
		native.chunkName(orphanObject, hashBytes(orphanPayload), 0),
		native.chunkName(orphanObject, hashBytes(orphanPayload), 1),
	}
	for _, chunk := range orphanChunks {
		raw.setModified(chunk, time.Now().Add(-2*time.Hour), true)
	}

	referencedObject := "repository/data/00/referenced"
	referencedPayload := bytes.Repeat([]byte("r"), chunkBytes+1)
	if err := native.put(t.Context(), referencedObject, referencedPayload, true); err != nil {
		t.Fatal(err)
	}
	referencedChunk := native.chunkName(referencedObject, hashBytes(referencedPayload), 0)
	raw.setModified(referencedChunk, time.Now().Add(-2*time.Hour), true)

	leasedObject := "repository/data/00/leased"
	leasedPayload := bytes.Repeat([]byte("l"), chunkBytes+1)
	descriptor, _, leaseRelease, err := native.preparePublication(t.Context(), leasedObject, leasedPayload)
	if err != nil {
		t.Fatal(err)
	}
	defer leaseRelease.release()
	leasedChunk := native.chunkName(leasedObject, descriptor.Digest, 0)
	raw.setModified(leasedChunk, time.Now().Add(-2*time.Hour), true)

	for range 12 {
		if err := native.sweepOrphanChunks(t.Context(), time.Now(), time.Minute, 16, 32); err != nil {
			t.Fatal(err)
		}
	}
	if _, exists := raw.objects[orphanChunks[0]]; exists {
		t.Fatal("orphan chunk 0 was not collected")
	}
	if _, exists := raw.objects[orphanChunks[1]]; exists {
		t.Fatal("orphan chunk 1 was not collected")
	}
	if _, exists := raw.objects[referencedChunk]; !exists {
		t.Fatal("referenced chunk was incorrectly collected")
	}
	if _, exists := raw.objects[leasedChunk]; !exists {
		t.Fatal("leased chunk was incorrectly collected")
	}
}

func TestNativeOrphanChunkGCStaleLeaseEventuallyPermitsCollection(t *testing.T) {
	native, raw := newFakeNative()

	staleObject := "repository/data/00/stale-lease"
	stalePayload := bytes.Repeat([]byte("s"), chunkBytes+1)
	descriptor, _, releaseLease, err := native.preparePublication(t.Context(), staleObject, stalePayload)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseLease.release()
	staleChunk := native.chunkName(staleObject, descriptor.Digest, 0)
	raw.setModified(staleChunk, time.Now().Add(-2*time.Hour), true)
	staleObjectDigest := sha256.Sum256([]byte(staleObject))

	staleLeasePrefix := path.Join(
		native.prefix,
		".vaultic-rados",
		"publication-leases",
		hex.EncodeToString(staleObjectDigest[:]),
	) + "/"
	staleLeases := raw.namesWithPrefix(staleLeasePrefix)
	if len(staleLeases) != 1 {
		t.Fatalf("stale lease count = %d, want 1", len(staleLeases))
	}
	var staleLease publicationLease
	if err := json.Unmarshal(raw.objects[staleLeases[0]], &staleLease); err != nil {
		t.Fatal(err)
	}
	staleLease.CreatedUnixNano = time.Now().Add(-20 * time.Second).UnixNano()
	staleLease.ExpiresUnixNano = time.Now().Add(-10 * time.Second).UnixNano()
	staleLease.Signature = ""
	staleLease.Signature = native.signPublicationLease(staleLease)
	encodedStaleLease, err := json.Marshal(staleLease)
	if err != nil {
		t.Fatal(err)
	}
	raw.objects[staleLeases[0]] = encodedStaleLease

	freshObject := "repository/data/00/fresh-lease"
	freshPayload := bytes.Repeat([]byte("f"), chunkBytes+1)
	freshDescriptor, _, freshRelease, err := native.preparePublication(t.Context(), freshObject, freshPayload)
	if err != nil {
		t.Fatal(err)
	}
	defer freshRelease.release()
	freshChunk := native.chunkName(freshObject, freshDescriptor.Digest, 0)
	raw.setModified(freshChunk, time.Now().Add(-2*time.Hour), true)

	runSweepCycles(t, native, 24, time.Minute, 8, 16)

	if _, exists := raw.objects[staleChunk]; exists {
		t.Fatal("stale lease chunk was not collected")
	}
	if _, exists := raw.objects[staleLeases[0]]; exists {
		t.Fatal("stale lease marker was not deleted")
	}
	if _, exists := raw.objects[freshChunk]; !exists {
		t.Fatal("fresh lease chunk was incorrectly collected")
	}
}

func TestNativeLeaseValidationMACAndLifetimeBounds(t *testing.T) {
	native, _ := newFakeNative()
	name := "repository/data/00/lease-validation"
	payload := bytes.Repeat([]byte("v"), chunkBytes+1)
	descriptor, _, releaseLease, err := native.preparePublication(t.Context(), name, payload)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseLease.release()

	leaseName, encoded, err := native.publicationLeasePayload(name, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if chunks, live := native.liveLeaseChunks(leaseName, encoded, now); !live || len(chunks) != len(descriptor.Chunks) {
		t.Fatalf("fresh lease rejected: live=%v chunks=%d", live, len(chunks))
	}

	var lease publicationLease
	if err := json.Unmarshal(encoded, &lease); err != nil {
		t.Fatal(err)
	}

	lease.CreatedUnixNano = now.Add(publicationLeaseClockSkew + time.Second).UnixNano()
	lease.Signature = native.signPublicationLease(lease)
	futurePayload, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, live := native.liveLeaseChunks(leaseName, futurePayload, now); live {
		t.Fatal("future-created lease was accepted")
	}

	lease.CreatedUnixNano = now.UnixNano()
	lease.ExpiresUnixNano = now.Add(native.publicationLeaseTTL() + publicationLeaseClockSkew + time.Second).UnixNano()
	lease.Signature = native.signPublicationLease(lease)
	oversizedPayload, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, live := native.liveLeaseChunks(leaseName, oversizedPayload, now); live {
		t.Fatal("oversized TTL lease was accepted")
	}

	lease.CreatedUnixNano = now.UnixNano()
	lease.ExpiresUnixNano = now.Add(native.publicationLeaseTTL()).UnixNano()
	badMAC, err := hex.DecodeString(lease.Signature)
	if err != nil || len(badMAC) == 0 {
		t.Fatalf("decode signature failed: %v", err)
	}
	badMAC[0] ^= 0x01
	lease.Signature = hex.EncodeToString(badMAC)
	badMACPayload, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, live := native.liveLeaseChunks(leaseName, badMACPayload, now); live {
		t.Fatal("bad-MAC lease was accepted")
	}

	lease.Signature = native.signPublicationLease(lease)
	lease.ExpiresUnixNano = now.Add(-30 * time.Minute).UnixNano()
	lease.Signature = native.signPublicationLease(lease)
	stalePayload, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, live := native.liveLeaseChunks(leaseName, stalePayload, now); live {
		t.Fatal("stale lease was accepted")
	}
}

func TestDerivePublicationLeaseKeyCanonicalizesEquivalentDomainInputs(t *testing.T) {
	left := derivePublicationLeaseKey(
		"secret",
		" 2F525D6A-8F31-4F79-B731-82A6ACB235F5 ",
		" vaultic-pool ",
		" repo-ns ",
		" /repository/root/ ",
		"client.Vaultic",
	)
	right := derivePublicationLeaseKey(
		"secret",
		"2f525d6a-8f31-4f79-b731-82a6acb235f5",
		"vaultic-pool",
		"repo-ns",
		"repository/root",
		" vaultic ",
	)
	if !bytes.Equal(left, right) {
		t.Fatal("equivalent domain formatting must derive identical lease key")
	}
}

func TestDerivePublicationLeaseKeyDistinctDomainDiffers(t *testing.T) {
	base := derivePublicationLeaseKey(
		"secret",
		"2f525d6a-8f31-4f79-b731-82a6acb235f5",
		"vaultic-pool",
		"repo-ns",
		"repository/root",
		"client.vaultic",
	)
	differentNamespace := derivePublicationLeaseKey(
		"secret",
		"2f525d6a-8f31-4f79-b731-82a6acb235f5",
		"vaultic-pool",
		"repo-ns-2",
		"repository/root",
		"client.vaultic",
	)
	if bytes.Equal(base, differentNamespace) {
		t.Fatal("distinct publication lease domain should derive a different key")
	}
}

func TestNativeLeaseValidationAcceptsEquivalentCrossProcessDomainConfig(t *testing.T) {
	left, _ := newFakeNative()
	right, _ := newFakeNative()
	left.publicationLeaseKey = derivePublicationLeaseKey(
		"cross-process-secret",
		" 2F525D6A-8F31-4F79-B731-82A6ACB235F5 ",
		" vaultic-pool ",
		" repo-ns ",
		" /repository/ ",
		"client.Vaultic",
	)
	right.publicationLeaseKey = derivePublicationLeaseKey(
		"cross-process-secret",
		"2f525d6a-8f31-4f79-b731-82a6acb235f5",
		"vaultic-pool",
		"repo-ns",
		"repository",
		"vaultic",
	)

	name := "repository/data/00/cross-process"
	payload := bytes.Repeat([]byte("k"), chunkBytes+32)
	descriptor, _, releaseLease, err := left.preparePublication(t.Context(), name, payload)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseLease.release()

	leaseName, encoded, err := left.publicationLeasePayload(name, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if chunks, live := right.liveLeaseChunks(leaseName, encoded, time.Now().UTC()); !live || len(chunks) != len(descriptor.Chunks) {
		t.Fatalf("equivalent cross-process domain rejected lease: live=%v chunks=%d", live, len(chunks))
	}
}

func TestNativeOrphanChunkGCMakesProgressAboveLegacyLimit(t *testing.T) {
	native, raw := newFakeNative()

	referencedObject := "repository/data/00/referenced-large"
	referencedPayload := []byte("referenced")
	if err := native.put(t.Context(), referencedObject, referencedPayload, true); err != nil {
		t.Fatal(err)
	}
	referencedChunk := native.chunkName(referencedObject, hashBytes(referencedPayload), 0)
	raw.setModified(referencedChunk, time.Now().Add(-2*time.Hour), true)

	orphanObject := "repository/data/00/orphan-large"
	orphanPayload := bytes.Repeat([]byte("o"), chunkBytes+1)
	raw.failWrites[orphanObject] = syscall.EIO
	if err := native.put(context.Background(), orphanObject, orphanPayload, true); err == nil {
		t.Fatal("orphan publication unexpectedly succeeded")
	}
	orphanChunks := []string{
		native.chunkName(orphanObject, hashBytes(orphanPayload), 0),
		native.chunkName(orphanObject, hashBytes(orphanPayload), 1),
	}
	for _, chunk := range orphanChunks {
		raw.setModified(chunk, time.Now().Add(-2*time.Hour), true)
	}

	for index := 0; index < 8300; index++ {
		name := fmt.Sprintf("repository/noise/%05d", index)
		raw.objects[name] = []byte("noise")
		raw.modTimes[name] = time.Now()
		raw.ageKnown[name] = true
	}

	runSweepCycles(t, native, 320, time.Minute, 16, 64)

	for _, chunk := range orphanChunks {
		if _, exists := raw.objects[chunk]; exists {
			t.Fatalf("orphan chunk %q was not collected", chunk)
		}
	}
	if _, exists := raw.objects[referencedChunk]; !exists {
		t.Fatal("referenced chunk was incorrectly collected")
	}
}

func TestNativeOrphanGCWorkerSlowListDoesNotDelayForegroundPut(t *testing.T) {
	native, raw := newFakeNative()
	raw.listDelay = 250 * time.Millisecond
	native.startOrphanGCWorker()
	defer native.close()

	if err := native.put(t.Context(), "repository/data/00/worker-a", []byte("a"), true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return raw.listPageCallCount() > 0 })

	started := time.Now()
	if err := native.put(t.Context(), "repository/data/00/worker-b", []byte("b"), true); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 120*time.Millisecond {
		t.Fatalf("foreground put blocked by GC sweep: %s", elapsed)
	}
}

func TestNativeOrphanGCWorkerCoalescesTriggersAndRunsSingleSweep(t *testing.T) {
	native, raw := newFakeNative()
	gate := make(chan struct{})
	raw.listBlock = gate
	native.startOrphanGCWorker()
	defer native.close()

	native.triggerOrphanSweep()
	waitFor(t, time.Second, func() bool { return raw.listPageCallCount() == 1 })
	for range 32 {
		native.triggerOrphanSweep()
	}
	time.Sleep(40 * time.Millisecond)
	if calls := raw.listPageCallCount(); calls != 1 {
		t.Fatalf("expected one in-flight sweep while blocked, got %d", calls)
	}
	close(gate)
}

func TestNativeCloseCancelsWorkerAndWaitsForShutdown(t *testing.T) {
	native, raw := newFakeNative()
	gate := make(chan struct{})
	raw.listBlock = gate
	native.startOrphanGCWorker()

	native.triggerOrphanSweep()
	waitFor(t, time.Second, func() bool { return raw.listPageCallCount() == 1 })

	started := time.Now()
	if err := native.close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("close took too long waiting for GC worker shutdown: %s", elapsed)
	}
	close(gate)
}

func TestNativeReadRawReturnsAtDeadlineDuringBlockedStat(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/blocked-stat"
	started := make(chan struct{})
	release := make(chan struct{})
	raw.beforeStat = func(statName string) {
		if statName == name {
			close(started)
			<-release
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := native.readRaw(ctx, name, 0, 0)
		result <- err
	}()
	<-started
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked stat error = %v, want context deadline exceeded", err)
	}
	close(release)
	if err := native.close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCloseWaitsForDetachedReadBeforeClosingRawStore(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/blocked-read"
	raw.objects[name] = []byte("payload")
	started := make(chan struct{})
	release := make(chan struct{})
	raw.beforeRead = func(readName string) {
		if readName == name {
			close(started)
			<-release
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := native.readRaw(ctx, name, 0, 0)
		result <- err
	}()
	<-started
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked read error = %v, want context deadline exceeded", err)
	}

	closed := make(chan struct{})
	go func() {
		_ = native.close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("close returned while detached raw read was still blocked")
	case <-time.After(30 * time.Millisecond):
	}
	if raw.isClosed() {
		t.Fatal("raw store closed while detached read was still in flight")
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not finish after detached read completed")
	}
	if !raw.isClosed() {
		t.Fatal("raw store was not closed after detached read completed")
	}
}

func TestNativeCloseBoundsDetachedReadDrain(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/blocked-close"
	raw.objects[name] = []byte("payload")
	started := make(chan struct{})
	release := make(chan struct{})
	raw.beforeRead = func(readName string) {
		if readName == name {
			close(started)
			<-release
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	readResult := make(chan error, 1)
	go func() {
		_, err := native.readRaw(ctx, name, 0, 0)
		readResult <- err
	}()
	<-started
	if err := <-readResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked read error = %v, want context deadline exceeded", err)
	}

	startedClose := time.Now()
	if err := native.close(); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("bounded close error = %v", err)
	}
	if elapsed := time.Since(startedClose); elapsed > nativeCloseDrainTimeout+time.Second {
		t.Fatalf("bounded close exceeded allowance: %v", elapsed)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for !raw.isClosed() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !raw.isClosed() {
		t.Fatal("raw store was not closed after detached read completed")
	}
}

func TestNativeWriteHonorsContextDeadline(t *testing.T) {
	native, raw := newFakeNative()
	started := make(chan struct{})
	release := make(chan struct{})
	raw.beforeWrite = func(_ *fakeRawStore, _ string, _ []byte, _ bool) error {
		close(started)
		<-release
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- native.writeRaw(ctx, "repository/chunk", []byte("payload"), true)
	}()
	<-started
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked write error = %v, want context deadline exceeded", err)
	}
	close(release)
	if err := native.close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeOrphanFenceRemainsActiveUntilTimedOutDeleteSettles(t *testing.T) {
	native, raw := newFakeNative()
	chunkName := native.chunkName("repository/data/00/orphan", strings.Repeat("a", sha256.Size*2), 0)
	raw.objects[chunkName] = []byte("orphan")
	raw.modTimes[chunkName] = time.Now().Add(-2 * orphanChunkGracePeriod)
	raw.ageKnown[chunkName] = true
	native.gcState = orphanGCState{
		phase: orphanGCDeleteChunks, referenced: map[string]struct{}{},
		publicationEpoch: native.publicationEpoch.Load(),
	}
	started := make(chan struct{})
	release := make(chan struct{})
	raw.beforeRemove = func(name string) {
		if name == chunkName {
			close(started)
			<-release
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	sweepDone := make(chan error, 1)
	go func() {
		sweepDone <- native.sweepOrphanChunks(ctx, time.Now(), time.Minute, 8, 32)
	}()
	<-started
	<-ctx.Done()
	if blocked, err := native.publicationBlockedByOrphanGC(t.Context(), time.Now()); err != nil || !blocked {
		t.Fatalf("fence while delete is active = blocked %v, err %v", blocked, err)
	}
	select {
	case err := <-sweepDone:
		t.Fatalf("sweep returned before raw delete settled: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-sweepDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("settled sweep error = %v, want deadline exceeded", err)
	}
}

func TestNativePublicationGuardRenewsBeforeOperationWindow(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/renewed-publication"
	descriptor, _, guard, err := native.preparePublication(t.Context(), name, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	defer guard.release()
	soon := time.Now()
	guard.lease.CreatedUnixNano = soon.Add(-time.Second).UnixNano()
	guard.lease.ExpiresUnixNano = soon.Add(time.Second).UnixNano()
	guard.lease.Signature = ""
	guard.lease.Signature = native.signPublicationLease(guard.lease)
	guard.payload, err = json.Marshal(guard.lease)
	if err != nil {
		t.Fatal(err)
	}
	raw.objects[guard.leaseName] = bytes.Clone(guard.payload)
	previousExpiry := guard.lease.ExpiresUnixNano
	if err := guard.renew(t.Context()); err != nil {
		t.Fatal(err)
	}
	if guard.lease.ExpiresUnixNano <= previousExpiry {
		t.Fatalf("lease expiry did not advance: before=%d after=%d", previousExpiry, guard.lease.ExpiresUnixNano)
	}
	encoded := raw.objects[guard.leaseName]
	chunks, live, valid := native.validatedLeaseChunks(guard.leaseName, encoded, time.Now())
	if !valid || !live || len(chunks) != len(descriptor.Chunks) {
		t.Fatalf("renewed lease = valid %v live %v chunks %d", valid, live, len(chunks))
	}
}

func TestNativeOrphanSweepRestartsAfterConcurrentPublication(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/republished"
	payload := []byte("same-content")
	descriptor, _, err := encodeManifestForBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	chunkName := native.chunkName(name, descriptor.Digest, 0)
	raw.objects[chunkName] = bytes.Clone(payload)
	raw.modTimes[chunkName] = time.Now().Add(-2 * orphanChunkGracePeriod)
	raw.ageKnown[chunkName] = true
	native.resetOrphanGCState()
	collectedEpoch := native.gcState.publicationEpoch
	if err := native.put(context.Background(), name, payload, true); err != nil {
		t.Fatal(err)
	}
	native.gcMu.Lock()
	native.gcState = orphanGCState{
		phase: orphanGCDeleteChunks, referenced: map[string]struct{}{}, publicationEpoch: collectedEpoch,
	}
	native.gcMu.Unlock()
	if err := native.sweepOrphanChunks(context.Background(), time.Now(), orphanChunkGracePeriod, 8, 32); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.stat(chunkName); err != nil {
		t.Fatalf("republished live chunk was deleted: %v", err)
	}
}

func TestNativeOrphanSweepRescansPublicationFromAnotherDriver(t *testing.T) {
	collector, raw := newFakeNative()
	publisher := &nativeDriver{
		raw: raw, prefix: collector.prefix, pool: collector.pool, opTTL: collector.opTTL,
		publicationLeaseKey: append([]byte(nil), collector.publicationLeaseKey...),
		readSlots:           make(chan struct{}, maxDetachedReadOperations),
		writeSlots:          make(chan struct{}, maxDetachedWriteOperations),
	}
	publisher.resetOrphanGCState()
	name := "repository/data/00/cross-driver"
	payload := []byte("same-content")
	descriptor, _, err := encodeManifestForBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	chunkName := collector.chunkName(name, descriptor.Digest, 0)
	raw.objects[chunkName] = bytes.Clone(payload)
	raw.modTimes[chunkName] = time.Now().Add(-2 * orphanChunkGracePeriod)
	raw.ageKnown[chunkName] = true
	collector.gcMu.Lock()
	collector.gcState = orphanGCState{
		phase: orphanGCDeleteChunks, referenced: map[string]struct{}{},
		publicationEpoch: collector.publicationEpoch.Load(),
	}
	collector.gcMu.Unlock()
	if err := publisher.put(context.Background(), name, payload, true); err != nil {
		t.Fatal(err)
	}
	if err := collector.sweepOrphanChunks(context.Background(), time.Now(), orphanChunkGracePeriod, 8, 32); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.stat(chunkName); err != nil {
		t.Fatalf("cross-driver published chunk was deleted: %v", err)
	}
}

func TestNativeOrphanSweepScansLeasesBeforeManifestsUnderFence(t *testing.T) {
	native, raw := newFakeNative()
	native.gcMu.Lock()
	native.gcState = orphanGCState{
		phase: orphanGCDeleteChunks, referenced: map[string]struct{}{},
		publicationEpoch: native.publicationEpoch.Load(),
	}
	native.gcMu.Unlock()
	if err := native.sweepOrphanChunks(t.Context(), time.Now(), time.Minute, 8, 32); err != nil {
		t.Fatal(err)
	}
	raw.mu.Lock()
	prefixes := append([]string(nil), raw.listPrefixes...)
	raw.mu.Unlock()
	leasePrefix := path.Join(native.prefix, ".vaultic-rados", "publication-leases") + "/"
	leaseIndex := slices.Index(prefixes, leasePrefix)
	manifestIndex := slices.Index(prefixes, native.prefix)
	if leaseIndex < 0 || manifestIndex < 0 || leaseIndex > manifestIndex {
		t.Fatalf("deletion-fence scan order = %v, want lease prefix before manifest prefix", prefixes)
	}
}

func TestNativeOrphanSweepAbortsOnUnreadableReferences(t *testing.T) {
	t.Run("manifest", func(t *testing.T) {
		native, raw := newFakeNative()
		name := "repository/data/00/unreadable-manifest"
		payload := []byte("live manifest payload")
		if err := native.put(t.Context(), name, payload, true); err != nil {
			t.Fatal(err)
		}
		descriptor, _, err := encodeManifestForBytes(payload)
		if err != nil {
			t.Fatal(err)
		}
		chunkName := native.chunkName(name, descriptor.Digest, 0)
		raw.setModified(chunkName, time.Now().Add(-2*time.Hour), true)
		raw.failReads[name] = syscall.EIO
		native.gcMu.Lock()
		native.gcState = orphanGCState{
			phase: orphanGCDeleteChunks, referenced: map[string]struct{}{},
			publicationEpoch: native.publicationEpoch.Load(),
		}
		native.gcMu.Unlock()
		if err := native.sweepOrphanChunks(t.Context(), time.Now(), time.Minute, 8, 32); err == nil {
			t.Fatal("expected unreadable manifest to abort orphan sweep")
		}
		if _, err := raw.stat(chunkName); err != nil {
			t.Fatalf("live chunk was deleted after manifest read failure: %v", err)
		}
	})

	t.Run("lease", func(t *testing.T) {
		native, raw := newFakeNative()
		name := "repository/data/00/unreadable-lease"
		payload := []byte("in-progress publication payload")
		descriptor, _, release, err := native.preparePublication(t.Context(), name, payload)
		if err != nil {
			t.Fatal(err)
		}
		defer release.release()
		chunkName := native.chunkName(name, descriptor.Digest, 0)
		raw.setModified(chunkName, time.Now().Add(-2*time.Hour), true)
		objectDigest := sha256.Sum256([]byte(name))
		leasePrefix := path.Join(native.prefix, ".vaultic-rados", "publication-leases", hex.EncodeToString(objectDigest[:])) + "/"
		leases := raw.namesWithPrefix(leasePrefix)
		if len(leases) != 1 {
			t.Fatalf("publication lease count = %d, want 1", len(leases))
		}
		raw.failReads[leases[0]] = syscall.EIO
		native.gcMu.Lock()
		native.gcState = orphanGCState{
			phase: orphanGCDeleteChunks, referenced: map[string]struct{}{},
			publicationEpoch: native.publicationEpoch.Load(),
		}
		native.gcMu.Unlock()
		if err := native.sweepOrphanChunks(t.Context(), time.Now(), time.Minute, 8, 32); err == nil {
			t.Fatal("expected unreadable lease to abort orphan sweep")
		}
		if _, err := raw.stat(chunkName); err != nil {
			t.Fatalf("leased chunk was deleted after lease read failure: %v", err)
		}
	})
}

func TestNativeSweepHonorsContextDeadlineDuringPagedList(t *testing.T) {
	native, raw := newFakeNative()
	raw.listDelay = 2 * time.Second

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := native.sweepOrphanChunks(ctx, time.Now(), time.Minute, 16, 32)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sweep deadline error = %v, want context deadline exceeded", err)
	}
}

func TestNativeReadRejectsCorruptChunk(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/object"
	data := bytes.Repeat([]byte("z"), chunkBytes+1)
	if err := native.put(t.Context(), name, data, true); err != nil {
		t.Fatal(err)
	}
	raw.objects[native.chunkName(name, hashBytes(data), 0)][0] ^= 0xff
	if _, err := native.read(t.Context(), name, 0, 0); err == nil || !strings.Contains(err.Error(), "integrity") {
		t.Fatalf("corrupt chunk error = %v", err)
	}
}

func TestNativeCompareAndSwapMigratesLegacyRawObject(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/control/policy.json"
	raw.objects[name] = []byte("legacy-policy")

	current, swapped, err := native.compareAndSwap(t.Context(), name, []byte("legacy-policy"), []byte("next-policy"))
	if err != nil {
		t.Fatal(err)
	}
	if !swapped {
		t.Fatalf("expected swap success, got current=%q", string(current))
	}
	if string(current) != "next-policy" {
		t.Fatalf("unexpected returned current %q", string(current))
	}
	stored := raw.objects[name]
	if !strings.HasPrefix(string(stored), manifestMagic) {
		t.Fatalf("expected migrated manifest encoding, got %q", string(stored))
	}
	logical, err := native.read(t.Context(), name, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(logical) != "next-policy" {
		t.Fatalf("read after migrate = %q", string(logical))
	}
}

func TestNativeCompareAndSwapRetriesWhenRawEncodingChangesConcurrently(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/control/quota.json"
	initial := []byte("legacy-quota")
	raw.objects[name] = bytes.Clone(initial)
	migratedDescriptor, migratedManifest, err := encodeManifestForBytes(initial)
	if err != nil {
		t.Fatal(err)
	}
	raw.objects[native.chunkName(name, migratedDescriptor.Digest, 0)] = bytes.Clone(initial)
	raw.casMutate[name] = migratedManifest

	current, swapped, err := native.compareAndSwap(t.Context(), name, initial, []byte("next-quota"))
	if err != nil {
		t.Fatal(err)
	}
	if !swapped {
		t.Fatalf("expected swap success after retry, got current=%q", string(current))
	}
	if raw.casCallCount(name) < 2 {
		t.Fatalf("expected at least one CAS retry, got %d calls", raw.casCallCount(name))
	}
	logical, err := native.read(t.Context(), name, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(logical) != "next-quota" {
		t.Fatalf("read after retry = %q", string(logical))
	}
}

func TestNativeLiveRADOS(t *testing.T) {
	monitors, key := os.Getenv("VAULTIC_RADOS_TEST_MONITORS"), os.Getenv("VAULTIC_RADOS_TEST_KEY")
	if monitors == "" || key == "" {
		t.Skip("set VAULTIC_RADOS_TEST_MONITORS and VAULTIC_RADOS_TEST_KEY")
	}
	config := Config{
		Monitors: monitors, ClusterFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		Pool: "vaultic", Namespace: "repo", Prefix: fmt.Sprintf("live-go-%d/", time.Now().UnixNano()),
		Client: "client.vaultic", Key: options.NewSecretString(key), OperationTTL: 5 * time.Second,
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	handle := backend.Handle{Type: backend.PackFile, Name: strings.Repeat("a", 64)}
	data := bytes.Repeat([]byte("live-rados"), chunkBytes/4+1)
	if err := store.Save(t.Context(), handle, backend.NewByteReader(data, nil)); err != nil {
		t.Fatal(err)
	}
	var ranged []byte
	if err := store.Load(t.Context(), handle, 19, chunkBytes-7, func(reader io.Reader) error {
		ranged, err = io.ReadAll(reader)
		return err
	}); err != nil || !bytes.Equal(ranged, data[chunkBytes-7:chunkBytes+12]) {
		t.Fatalf("live range = %d bytes, %v", len(ranged), err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if info, err := store.Stat(t.Context(), handle); err != nil || info.Size != int64(len(data)) {
		t.Fatalf("reopened stat = %+v, %v", info, err)
	}

	name := store.Filename(backend.Handle{Type: backend.PackFile, Name: strings.Repeat("b", 64)})
	results := make(chan error, 8)
	for range 8 {
		go func() { results <- store.driver.put(t.Context(), name, []byte("winner"), true) }()
	}
	succeeded := 0
	var unexpected []error
	for range 8 {
		if result := <-results; result == nil {
			succeeded++
		} else if !errors.Is(result, ErrExists) {
			unexpected = append(unexpected, result)
		}
	}
	if len(unexpected) != 0 {
		t.Fatalf("concurrent create errors = %v", unexpected)
	}
	if succeeded != 1 {
		t.Fatalf("concurrent creates succeeded = %d, want 1", succeeded)
	}

	denied := config
	denied.Namespace = "forbidden"
	denied.Prefix = "live-denied/"
	deniedStore, err := Open(t.Context(), denied)
	if err != nil {
		t.Fatal(err)
	}
	defer deniedStore.Close()
	err = deniedStore.Save(t.Context(), handle, backend.NewByteReader([]byte("denied"), nil))
	if err == nil {
		t.Fatal("cross-namespace write unexpectedly succeeded")
	}
}

func TestNativeLiveOSDUnavailableIsBounded(t *testing.T) {
	monitors, key := os.Getenv("VAULTIC_RADOS_TEST_MONITORS"), os.Getenv("VAULTIC_RADOS_TEST_KEY")
	if monitors == "" || key == "" || os.Getenv("VAULTIC_RADOS_TEST_OSD_DOWN") == "" {
		t.Skip("set live RADOS test variables and VAULTIC_RADOS_TEST_OSD_DOWN")
	}
	config := Config{
		Monitors: monitors, ClusterFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		Pool: "vaultic", Namespace: "repo", Prefix: fmt.Sprintf("outage-%d/", time.Now().UnixNano()),
		Client: "client.vaultic", Key: options.NewSecretString(key), OperationTTL: 2 * time.Second,
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	started := time.Now()
	err = store.Save(t.Context(), backend.Handle{Type: backend.PackFile, Name: strings.Repeat("f", 64)}, backend.NewByteReader([]byte("unavailable"), nil))
	if err == nil {
		t.Fatal("write unexpectedly succeeded while OSD was unavailable")
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("failed operation took %s, want at most 10s", elapsed)
	}
}
