//go:build rados

package rados

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/options"
)

type fakeRawStore struct {
	mu         sync.Mutex
	objects    map[string][]byte
	failWrites map[string]error
}

func newFakeNative() (*nativeDriver, *fakeRawStore) {
	raw := &fakeRawStore{objects: make(map[string][]byte), failWrites: make(map[string]error)}
	return &nativeDriver{raw: raw, prefix: "repository/"}, raw
}

func (store *fakeRawStore) stat(name string) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	data, ok := store.objects[name]
	if !ok {
		return 0, syscall.ENOENT
	}
	return uint64(len(data)), nil
}

func (store *fakeRawStore) read(name string, buffer []byte, offset uint64) (int, error) {
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
	if err := store.failWrites[name]; err != nil {
		return err
	}
	if _, ok := store.objects[name]; ok && exclusive {
		return syscall.EEXIST
	}
	store.objects[name] = bytes.Clone(data)
	return nil
}

func (store *fakeRawStore) remove(name string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.objects[name]; !ok {
		return syscall.ENOENT
	}
	delete(store.objects, name)
	return nil
}

func (store *fakeRawStore) list(consume func(string)) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for name := range store.objects {
		consume(name)
	}
	return nil
}

func (*fakeRawStore) close() {}

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
		if _, ok := raw.objects[native.chunkName(name, conflictDigest, ordinal)]; ok {
			t.Fatalf("conflicting publication left chunk %d", ordinal)
		}
	}

	oldDigest := hashBytes(data)
	replacement := bytes.Repeat([]byte("c"), chunkBytes+3)
	if err := native.put(t.Context(), name, replacement, false); err != nil {
		t.Fatal(err)
	}
	for ordinal := 0; ordinal < 2; ordinal++ {
		if _, ok := raw.objects[native.chunkName(name, oldDigest, ordinal)]; ok {
			t.Fatalf("overwrite left old chunk %d", ordinal)
		}
	}
	if err := native.remove(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	for object := range raw.objects {
		if strings.Contains(object, ".vaultic-rados/chunks/") {
			t.Fatalf("remove left chunk %q", object)
		}
	}
}

func TestNativeFailedPublicationCleansCreatedChunks(t *testing.T) {
	native, raw := newFakeNative()
	name := "repository/data/00/object"
	data := bytes.Repeat([]byte("x"), chunkBytes+1)
	raw.failWrites[name] = syscall.EIO

	if err := native.put(context.Background(), name, data, true); err == nil {
		t.Fatal("publication unexpectedly succeeded")
	}
	if len(raw.objects) != 0 {
		t.Fatalf("failed publication left objects: %v", raw.objects)
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
