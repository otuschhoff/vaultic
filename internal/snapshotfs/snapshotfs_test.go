package snapshotfs

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestLookupOrderingFlatteningAndSubfolder(t *testing.T) {
	repo := repository.TestRepository(t)
	ctx := context.Background()
	var nestedID, specialID vaultic.ID
	withUploader(t, repo, func(uploader vaultic.BlobSaver) {
		nestedID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{
			{Name: "inside", Type: data.NodeTypeFile, Mode: 0600},
		})
		specialID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{
			{Name: "beta", Type: data.NodeTypeFile},
		})
	})
	snapshot := saveSnapshotTree(t, repo, []*data.Node{
		{Name: "zeta", Type: data.NodeTypeFile},
		{Name: ".", Type: data.NodeTypeDir, Subtree: &specialID},
		{Name: "alpha", Type: data.NodeTypeDir, Subtree: &nestedID},
	})

	fs, err := New(ctx, repo, snapshot, "", Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	root, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := root.ReadDir(ctx)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for index := range entries {
		names[index] = entries[index].Name
	}
	if !reflect.DeepEqual(names, []string{"alpha", "beta", "zeta"}) {
		t.Fatalf("unexpected bytewise order: %q", names)
	}
	if _, err := root.Lookup(ctx, "beta"); err != nil {
		t.Fatalf("lookup flattened node: %v", err)
	}

	subFS, err := New(ctx, repo, snapshot, "/alpha/", Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subFS.Close() })
	subRoot, _ := subFS.Root()
	if subRoot.Path() != "/alpha" {
		t.Fatalf("unexpected canonical subfolder path %q", subRoot.Path())
	}
	if _, err := subRoot.Lookup(ctx, "inside"); err != nil {
		t.Fatalf("lookup beneath subfolder: %v", err)
	}
	if _, err := subRoot.Lookup(ctx, "zeta"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("subfolder escaped selected root: %v", err)
	}
}

func TestInvalidNamesAndCollisions(t *testing.T) {
	repo := repository.TestRepository(t)
	ctx := context.Background()
	var specialID vaultic.ID
	withUploader(t, repo, func(uploader vaultic.BlobSaver) {
		specialID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{{Name: "same", Type: data.NodeTypeFile}})
	})
	snapshot := saveSnapshotTree(t, repo, []*data.Node{
		{Name: "/", Type: data.NodeTypeDir, Subtree: &specialID},
		{Name: "same", Type: data.NodeTypeFile},
	})
	fs, err := New(ctx, repo, snapshot, "", Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	root, _ := fs.Root()
	if _, err := root.ReadDir(ctx); !errors.Is(err, ErrCollision) {
		t.Fatalf("expected collision, got %v", err)
	}

	validSnapshot := data.TestCreateSnapshot(t, repo, time.Unix(1700000000, 0), 1)
	validFS, err := New(ctx, repo, validSnapshot, "", Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = validFS.Close() })
	validRoot, _ := validFS.Root()
	for _, name := range []string{"", ".", "..", "a/b", "a\x00b"} {
		if _, err := validRoot.Lookup(ctx, name); !errors.Is(err, ErrInvalidName) {
			t.Errorf("Lookup(%q) error = %v", name, err)
		}
	}
	if _, err := validRoot.Lookup(ctx, strings.Repeat("x", 256)); !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("long name error = %v", err)
	}
	if _, err := New(ctx, repo, validSnapshot, "../escape", Config{}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("escaping subfolder error = %v", err)
	}
}

func TestFileReadsAndSymlink(t *testing.T) {
	repo := repository.TestRepository(t)
	ctx := context.Background()
	var content vaultic.IDs
	withUploader(t, repo, func(uploader vaultic.BlobSaver) {
		for _, chunk := range [][]byte{[]byte("abc"), []byte("defgh")} {
			id, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, chunk, vaultic.ID{}, false)
			if err != nil {
				t.Fatal(err)
			}
			content = append(content, id)
		}
	})
	snapshot := saveSnapshotTree(t, repo, []*data.Node{
		{Name: "empty", Type: data.NodeTypeFile, Mode: 0640},
		{Name: "file", Type: data.NodeTypeFile, Mode: 0640, Size: 8, Content: content},
		{Name: "link", Type: data.NodeTypeSymlink, Mode: 0777, LinkTarget: "../target"},
	})
	fs, err := New(ctx, repo, snapshot, "", Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	root, _ := fs.Root()
	file, _ := root.Lookup(ctx, "file")

	buf := make([]byte, 4)
	n, err := file.ReadAt(ctx, 2, buf)
	if err != nil || n != 4 || string(buf) != "cdef" {
		t.Fatalf("cross-blob read = %d, %q, %v", n, buf, err)
	}
	buf = make([]byte, 8)
	n, err = file.ReadAt(ctx, 6, buf)
	if err != nil || n != 2 || string(buf[:n]) != "gh" {
		t.Fatalf("short read = %d, %q, %v", n, buf[:n], err)
	}
	empty, _ := root.Lookup(ctx, "empty")
	if n, err = empty.ReadAt(ctx, 0, buf); err != nil || n != 0 {
		t.Fatalf("empty read = %d, %v", n, err)
	}
	link, _ := root.Lookup(ctx, "link")
	if target, err := link.Readlink(ctx); err != nil || target != "../target" {
		t.Fatalf("readlink = %q, %v", target, err)
	}
}

func TestProjectionIdentitySpecialNodesAndLifecycle(t *testing.T) {
	repo := repository.TestRepository(t)
	ctx := context.Background()
	snapshot := saveSnapshotTree(t, repo, []*data.Node{
		{Name: "hard-a", Type: data.NodeTypeFile, Mode: 0000, UID: 12, GID: 34, Links: 2, DeviceID: 7, Inode: 9},
		{Name: "hard-b", Type: data.NodeTypeFile, Mode: 0000, UID: 12, GID: 34, Links: 2, DeviceID: 7, Inode: 9},
		{Name: "pipe", Type: data.NodeTypeFifo, Mode: 0600},
	})
	fs, err := New(ctx, repo, snapshot, "", Config{
		TreeCacheBytes: 512, BlobCacheBytes: 1024,
		Owner: OwnerServer, ServerUID: 99, ServerGID: 100,
		Permissions: PermissionsReadable,
	})
	if err != nil {
		t.Fatal(err)
	}
	root, _ := fs.Root()
	left, _ := root.Lookup(ctx, "hard-a")
	right, _ := root.Lookup(ctx, "hard-b")
	if left.Identity() != right.Identity() {
		t.Fatal("proven hard links did not share identity")
	}
	again, _ := root.Lookup(ctx, "hard-a")
	if again.Identity() != left.Identity() {
		t.Fatal("identity was not stable across lookup")
	}
	attr, err := left.Attr(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if attr.UID != 99 || attr.GID != 100 || attr.Mode.Perm() != 0444 || attr.Links != 2 {
		t.Fatalf("unexpected projected attrs: %+v", attr)
	}
	preservedFS, err := New(ctx, repo, snapshot, "", Config{Owner: OwnerPreserved, Permissions: PermissionsPreserved})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = preservedFS.Close() })
	preservedRoot, _ := preservedFS.Root()
	preserved, _ := preservedRoot.Lookup(ctx, "hard-a")
	preservedAttr, err := preserved.Attr(ctx)
	if err != nil || preservedAttr.UID != 12 || preservedAttr.GID != 34 || preservedAttr.Mode.Perm() != 0 {
		t.Fatalf("preserved attrs = %+v, %v", preservedAttr, err)
	}
	rootOwnerFS, err := New(ctx, repo, snapshot, "", Config{Owner: OwnerRoot})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rootOwnerFS.Close() })
	rootOwnerRoot, _ := rootOwnerFS.Root()
	rootOwned, _ := rootOwnerRoot.Lookup(ctx, "hard-a")
	rootAttr, err := rootOwned.Attr(ctx)
	if err != nil || rootAttr.UID != 0 || rootAttr.GID != 0 {
		t.Fatalf("root projected attrs = %+v, %v", rootAttr, err)
	}
	pipe, _ := root.Lookup(ctx, "pipe")
	pipeAttr, err := pipe.Attr(ctx)
	if err != nil || pipeAttr.Type != data.NodeTypeFifo || pipeAttr.Mode&os.ModeNamedPipe == 0 {
		t.Fatalf("special metadata = %+v, %v", pipeAttr, err)
	}
	stats := fs.CacheStats()
	if stats.TreeBytes > stats.TreeLimitBytes {
		t.Fatalf("tree cache exceeded bound: %+v", stats)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := left.Attr(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("operation after close = %v", err)
	}
}

func TestConcurrentReadsAndClose(t *testing.T) {
	repo := repository.TestRepository(t)
	ctx := context.Background()
	var content vaultic.IDs
	withUploader(t, repo, func(uploader vaultic.BlobSaver) {
		id, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("concurrent"), vaultic.ID{}, false)
		if err != nil {
			t.Fatal(err)
		}
		content = append(content, id)
	})
	snapshot := saveSnapshotTree(t, repo, []*data.Node{{
		Name: "file", Type: data.NodeTypeFile, Size: 10, Content: content,
	}})
	fs, err := New(ctx, repo, snapshot, "", Config{})
	if err != nil {
		t.Fatal(err)
	}
	root, _ := fs.Root()
	file, _ := root.Lookup(ctx, "file")

	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			buf := make([]byte, 10)
			for {
				n, readErr := file.ReadAt(ctx, 0, buf)
				if errors.Is(readErr, ErrClosed) {
					return
				}
				if readErr != nil || n != 10 || string(buf) != "concurrent" {
					t.Errorf("concurrent read = %d, %q, %v", n, buf, readErr)
					return
				}
			}
		}()
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
}

func TestCancellation(t *testing.T) {
	repo := repository.TestRepository(t)
	snapshot := data.TestCreateSnapshot(t, repo, time.Unix(1700000001, 0), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(ctx, repo, snapshot, "", Config{}); !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("New cancellation = %v", err)
	}

	fs, err := New(context.Background(), repo, snapshot, "", Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	root, _ := fs.Root()
	if _, err := root.ReadDir(ctx); !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir cancellation = %v", err)
	}
}

func saveSnapshotTree(t *testing.T, repo vaultic.Repository, nodes []*data.Node) *data.Snapshot {
	t.Helper()
	ctx := context.Background()
	var treeID vaultic.ID
	withUploader(t, repo, func(uploader vaultic.BlobSaver) {
		treeID = data.TestSaveNodes(t, ctx, uploader, nodes)
	})
	snapshot := &data.Snapshot{Tree: &treeID, Time: time.Unix(1700000000, 0), UID: 10, GID: 20}
	data.TestSetSnapshotID(t, snapshot, vaultic.Hash([]byte(t.Name())))
	return snapshot
}

func withUploader(t *testing.T, repo vaultic.Repository, fn func(vaultic.BlobSaver)) {
	t.Helper()
	err := repo.WithBlobUploader(context.Background(), func(_ context.Context, uploader vaultic.BlobSaverWithAsync) error {
		fn(uploader)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
