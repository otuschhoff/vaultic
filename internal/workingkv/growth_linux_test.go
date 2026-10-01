package workingkv

import (
	"bytes"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
)

func TestGrowthFailureProcess(t *testing.T) {
	root := os.Getenv("PHASE35_GROWTH_ROOT")
	if root == "" {
		t.Skip("requires isolated process resource-limit runner")
	}
	store, err := Open("bbolt", root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put(t.Context(), []Entry{{[]byte("seed"), []byte("stable")}}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = uint64(info.Size())
	signal.Ignore(syscall.SIGXFSZ)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	err = store.Put(t.Context(), []Entry{{[]byte("failed-growth"), bytes.Repeat([]byte{1}, 512<<10)}})
	if !errors.Is(err, ErrBackendIO) {
		t.Fatalf("expected classified growth failure, got %v", err)
	}
	if _, found, err := store.Get(t.Context(), []byte("failed-growth")); err != nil || found {
		t.Fatal("failed growth made rejected batch visible", err)
	}
	if value, found, err := store.Get(t.Context(), []byte("seed")); err != nil || !found || string(value) != "stable" {
		t.Fatal("failed growth damaged existing state", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
