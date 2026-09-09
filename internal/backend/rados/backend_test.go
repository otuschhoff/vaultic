package rados

import (
	"context"
	stderrors "errors"
	"io"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/options"
)

type memoryDriver struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemoryBackend() *Backend {
	return newBackend(Config{
		Prefix: "repository/", Connections: 8, Monitors: "mon:3300",
		ClusterFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5", Pool: "pool",
		Namespace: "namespace", Client: "client.test", Key: options.NewSecretString("secret"),
	}, &memoryDriver{objects: make(map[string][]byte)})
}

func (driver *memoryDriver) read(ctx context.Context, name string, offset int64, length int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	data, found := driver.objects[name]
	if !found {
		return nil, ErrNotFound
	}
	if offset > int64(len(data)) || length > 0 && offset+int64(length) > int64(len(data)) {
		return nil, ErrRange
	}
	end := len(data)
	if length > 0 {
		end = int(offset) + length
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func (driver *memoryDriver) stat(ctx context.Context, name string) (int64, error) {
	data, err := driver.read(ctx, name, 0, 0)
	return int64(len(data)), err
}

func (driver *memoryDriver) put(ctx context.Context, name string, data []byte, exclusive bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if _, found := driver.objects[name]; found && exclusive {
		return ErrExists
	}
	driver.objects[name] = append([]byte(nil), data...)
	return nil
}

func (driver *memoryDriver) remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if _, found := driver.objects[name]; !found {
		return ErrNotFound
	}
	delete(driver.objects, name)
	return nil
}

func (driver *memoryDriver) list(ctx context.Context, prefix string) ([]objectInfo, error) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	objects := make([]objectInfo, 0)
	for name, data := range driver.objects {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			objects = append(objects, objectInfo{name: name, size: int64(len(data))})
		}
	}
	return objects, ctx.Err()
}

func (*memoryDriver) close() error { return nil }

func TestSaveRetriesAreIdempotentButConflictsFail(t *testing.T) {
	store := newMemoryBackend()
	handle := backend.Handle{Type: backend.PackFile, Name: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	first := backend.NewByteReader([]byte("same"), nil)
	if err := store.Save(t.Context(), handle, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), handle, backend.NewByteReader([]byte("same"), nil)); err != nil {
		t.Fatalf("identical retry failed: %v", err)
	}
	if err := store.Save(t.Context(), handle, backend.NewByteReader([]byte("different"), nil)); !stderrors.Is(err, ErrExists) {
		t.Fatalf("conflicting write error = %v, want ErrExists", err)
	}
}

func TestRangeListRemoveAndCancellation(t *testing.T) {
	store := newMemoryBackend()
	first := backend.Handle{Type: backend.PackFile, Name: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	second := backend.Handle{Type: backend.PackFile, Name: "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	for _, handle := range []backend.Handle{first, second} {
		if err := store.Save(t.Context(), handle, backend.NewByteReader([]byte("0123456789"), nil)); err != nil {
			t.Fatal(err)
		}
	}
	var ranged []byte
	if err := store.Load(t.Context(), first, 4, 3, func(reader io.Reader) error {
		var err error
		ranged, err = io.ReadAll(reader)
		return err
	}); err != nil || string(ranged) != "3456" {
		t.Fatalf("range = %q, %v", ranged, err)
	}
	if err := store.Load(t.Context(), first, 20, 0, func(io.Reader) error { return nil }); !store.IsPermanentError(err) {
		t.Fatalf("short range error = %v, want permanent", err)
	}
	listed := 0
	if err := store.List(t.Context(), backend.PackFile, func(backend.FileInfo) error { listed++; return nil }); err != nil || listed != 2 {
		t.Fatalf("listed = %d, %v", listed, err)
	}
	if err := store.Remove(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), first); err != nil {
		t.Fatalf("idempotent remove failed: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.Save(canceled, first, backend.NewByteReader([]byte("data"), nil)); !stderrors.Is(err, context.Canceled) {
		t.Fatalf("canceled save error = %v", err)
	}
}

func TestOpenWithoutNativeFeatureFailsClearly(t *testing.T) {
	if nativeEnabled {
		t.Skip("native RADOS build")
	}
	config := Config{
		Prefix: "repository/", Monitors: "mon:3300", ClusterFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		Pool: "pool", Namespace: "namespace", Client: "client.test", Key: options.NewSecretString("secret"),
	}
	_, err := Open(t.Context(), config)
	if !stderrors.Is(err, ErrUnsupported) {
		t.Fatalf("Open error = %v, want ErrUnsupported", err)
	}
}
