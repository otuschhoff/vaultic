package workingkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestConformance(t *testing.T) {
	for _, backend := range []string{"pebble", "bbolt", "badger", "ram"} {
		t.Run(backend, func(t *testing.T) {
			store, err := openConformanceStore(t, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			entries := []Entry{{[]byte("p/2"), []byte("two")}, {[]byte("p/1"), []byte("one")}, {[]byte("q/1"), nil}}
			if err := store.Put(t.Context(), entries); err != nil {
				t.Fatal(err)
			}
			value, found, err := store.Get(t.Context(), entries[0].Key)
			if err != nil || !found || !bytes.Equal(value, []byte("two")) {
				t.Fatalf("get: %q %v %v", value, found, err)
			}
			value[0] = 'X'
			rows, err := store.Scan(t.Context(), []byte("p/"), nil, 1)
			if err != nil || len(rows) != 1 || string(rows[0].Key) != "p/1" {
				t.Fatalf("scan: %v %v", rows, err)
			}
			rows, err = store.Scan(t.Context(), []byte("p/"), rows[0].Key, 1)
			if err != nil || len(rows) != 1 || string(rows[0].Value) != "two" {
				t.Fatalf("page: %v %v", rows, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if !errors.Is(store.Put(ctx, entries), context.Canceled) {
				t.Fatal("cancellation")
			}
			if !errors.Is(store.Put(t.Context(), []Entry{{[]byte("oversized"), make([]byte, MaxBatchBytes)}}), ErrBatchLimit) {
				t.Fatal("batch bound")
			}
			if _, found, err := store.Get(t.Context(), []byte("oversized")); err != nil || found {
				t.Fatal("rejected batch visible")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Get(t.Context(), []byte("p/1")); !errors.Is(err, ErrClosed) {
				t.Fatal("closed get")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConcurrencyGrowthAndBounds(t *testing.T) {
	for _, backend := range []string{"pebble", "bbolt", "badger", "ram"} {
		t.Run(backend, func(t *testing.T) {
			store, err := openConformanceStore(t, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			var group sync.WaitGroup
			for lane := range 4 {
				group.Go(func() {
					for ordinal := range 128 {
						key := []byte(fmt.Sprintf("p/%d/%04d", lane, ordinal))
						value := bytes.Repeat([]byte{byte(ordinal)}, 32<<10)
						if err := store.Put(t.Context(), []Entry{{key, value}}); err != nil {
							t.Error(err)
							return
						}
						got, found, err := store.Get(t.Context(), key)
						if err != nil || !found || !bytes.Equal(got, value) {
							t.Errorf("concurrent get: %v %v", found, err)
							return
						}
						if _, err := store.Scan(t.Context(), []byte("p/"), nil, 4); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			group.Wait()
			if err := store.Put(t.Context(), []Entry{{[]byte("atomic"), []byte("first")}, {nil, nil}}); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			if _, found, err := store.Get(t.Context(), []byte("atomic")); found || err != nil {
				t.Fatal("partial batch visible")
			}
			if err := store.Put(t.Context(), []Entry{{[]byte("empty"), nil}, {[]byte("overwrite"), []byte("old")}, {[]byte("overwrite"), []byte("new")}}); err != nil {
				t.Fatal(err)
			}
			if value, found, err := store.Get(t.Context(), []byte("empty")); err != nil || !found || len(value) != 0 {
				t.Fatal("empty/missing conflated")
			}
			if value, _, err := store.Get(t.Context(), []byte("overwrite")); err != nil || string(value) != "new" {
				t.Fatal("overwrite order")
			}
			if rows, err := store.Scan(t.Context(), []byte("p/"), nil, 1024); !errors.Is(err, ErrBatchLimit) || rows != nil {
				t.Fatalf("scan bound: %d %v", len(rows), err)
			}
			store.writer <- struct{}{}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancel()
			if err := store.Put(ctx, []Entry{{[]byte("blocked"), nil}}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("writer wait is not cancellable", err)
			}
			<-store.writer
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(t.Context(), []Entry{{[]byte("closed"), nil}}); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}

type failingCloseEngine struct {
	engine
	calls int
}

func (impl *failingCloseEngine) close() error { impl.calls++; return syscall.EIO }

type blockingEngine struct {
	failingCloseEngine
	entered, release chan struct{}
}

func (impl *blockingEngine) put(context.Context, []Entry) error {
	close(impl.entered)
	<-impl.release
	return nil
}

func TestCloseDrainsWithoutBlockingAdmission(t *testing.T) {
	impl := &blockingEngine{entered: make(chan struct{}), release: make(chan struct{})}
	store := &Store{engine: impl, writer: make(chan struct{}, 1), done: make(chan struct{})}
	putDone, closeDone := make(chan error, 1), make(chan error, 1)
	go func() { putDone <- store.Put(t.Context(), []Entry{{[]byte("active"), nil}}) }()
	<-impl.entered
	go func() { closeDone <- store.Close() }()
	<-store.done
	if _, _, err := store.Get(t.Context(), []byte("closed")); !errors.Is(err, ErrClosed) {
		t.Error(err)
	}
	if err := store.Put(t.Context(), []Entry{{[]byte("queued"), nil}}); !errors.Is(err, ErrClosed) {
		t.Error(err)
	}
	select {
	case <-closeDone:
		t.Error("close did not drain active operation")
	default:
	}
	close(impl.release)
	if err := <-putDone; err != nil {
		t.Error(err)
	}
	if err := <-closeDone; !errors.Is(err, syscall.EIO) {
		t.Error(err)
	}
	if !errors.Is(store.Close(), syscall.EIO) || impl.calls != 1 {
		t.Error("close failure lost or repeated")
	}
}

func TestCloseFailureAndPrivateRoots(t *testing.T) {
	impl := &failingCloseEngine{}
	store := &Store{engine: impl, writer: make(chan struct{}, 1)}
	if !errors.Is(store.Close(), syscall.EIO) || !errors.Is(store.Close(), syscall.EIO) || impl.calls != 1 {
		t.Fatal("close failure lost or close repeated")
	}
	if _, _, err := store.Get(t.Context(), []byte("closed")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	for _, backend := range []string{"pebble", "bbolt", "badger"} {
		root := t.TempDir()
		if err := os.Chmod(root, 0755); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(backend, root); !errors.Is(err, ErrInvalid) {
			t.Fatal("insecure root accepted", backend, err)
		}
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(root, link); err != nil {
			t.Skip("symlinks unavailable")
		}
		if _, err := Open(backend, link); !errors.Is(err, ErrInvalid) {
			t.Fatal("symlink root accepted", backend, err)
		}
	}
}

func TestFilesystemFailureProcess(t *testing.T) {
	backend := os.Getenv("PHASE35_FAULT_BACKEND")
	if backend == "" {
		t.Skip("requires isolated syscall/UID fault runner")
	}
	mode := os.Getenv("PHASE35_FAULT_MODE")
	store, err := Open(backend, os.Getenv("PHASE35_FAULT_ROOT"))
	if mode == "close" {
		if err != nil {
			t.Fatal("open failed before close injection", err)
		}
		err = store.Close()
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("expected close EIO, got %v", err)
		}
		if !errors.Is(store.Close(), syscall.EIO) {
			t.Fatal("close error not retained")
		}
		return
	}
	if store != nil {
		_ = store.Close()
	}
	if mode == "enospc" && !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("expected ENOSPC, got %v", err)
	}
	if mode == "permission" && !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected permission failure, got %v", err)
	}
	if mode != "enospc" && mode != "permission" {
		t.Fatal("invalid fault mode")
	}
}

type cancelDuringCopy struct {
	context.Context
	cancel context.CancelFunc
	calls  int
}

func (ctx *cancelDuringCopy) Err() error {
	ctx.calls++
	if ctx.calls == 4 {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestCancellationBeforeCommitAndOwnedInputs(t *testing.T) {
	for _, backend := range []string{"pebble", "bbolt", "badger", "ram"} {
		t.Run(backend, func(t *testing.T) {
			store, err := openConformanceStore(t, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &cancelDuringCopy{Context: parent, cancel: cancel}
			entries := []Entry{{[]byte("first"), []byte("value")}, {[]byte("second"), []byte("value")}}
			if err := store.Put(ctx, entries); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, found, err := store.Get(t.Context(), []byte("first")); err != nil || found {
				t.Fatal("cancelled partial batch visible")
			}
			if err := store.Put(t.Context(), entries); err != nil {
				t.Fatal(err)
			}
			entries[0].Key[0] = 'X'
			entries[0].Value[0] = 'X'
			value, found, err := store.Get(t.Context(), []byte("first"))
			if err != nil || !found || string(value) != "value" {
				t.Fatal("input buffer retained", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if string(value) != "value" {
				t.Fatal("returned page escaped transaction")
			}
		})
	}
}

func TestReplayCodecAndFileConfidentiality(t *testing.T) {
	fixture := newReplayFixture("directories")
	entry := fixture.final[0]
	value := bytes.Clone(entry.Value)
	value[len(value)-1] ^= 1
	aad := append([]byte("v1/directories/"), entry.Key...)
	if _, err := fixture.aead.Open(nil, value[:12], value[12:], aad); err == nil {
		t.Fatal("unauthenticated value accepted")
	}
	if _, err := fixture.aead.Open(nil, entry.Value[:12], entry.Value[12:], entry.Key); err == nil {
		t.Fatal("AAD substitution accepted")
	}
	for _, backend := range []string{"pebble", "bbolt", "badger"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			store, err := Open(backend, root)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			for _, entry := range fixture.final {
				if err := store.Put(t.Context(), []Entry{entry}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				defer file.Close()
				buffer := make([]byte, 64<<10)
				var carry []byte
				for {
					count, err := file.Read(buffer)
					window := append(carry, buffer[:count]...)
					if bytes.Contains(window, []byte("child-0000")) {
						return errors.New("plaintext directory name in working file")
					}
					keep := min(9, len(window))
					carry = bytes.Clone(window[len(window)-keep:])
					if errors.Is(err, io.EOF) {
						return nil
					}
					if err != nil {
						return err
					}
				}
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFreshRootAndReadBounds(t *testing.T) {
	for _, backend := range []string{"pebble", "bbolt", "badger"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "store")
			store, err := Open(backend, root)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			key := []byte("oversized-engine-value")
			if err := store.engine.put(t.Context(), []Entry{{key, make([]byte, MaxBatchBytes+1)}}); err != nil {
				t.Fatal(err)
			}
			if value, found, err := store.Get(t.Context(), key); !errors.Is(err, ErrBatchLimit) || found || value != nil {
				t.Fatalf("unbounded engine get: %v %v", found, err)
			}
			if rows, err := store.Scan(t.Context(), nil, nil, 1); !errors.Is(err, ErrBatchLimit) || rows != nil {
				t.Fatalf("unbounded engine scan: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(backend, root); !errors.Is(err, ErrInvalid) {
				t.Fatalf("foreign/nonempty root reopened: %v", err)
			}
		})
	}
}
