//go:build darwin || freebsd || linux

package fuse

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestSnapshotFSContract(t *testing.T) {
	ctx := t.Context()
	repo := repository.TestRepository(t)
	payload := []byte("snapshotfs contract bytes")
	stamp := time.Unix(1700000123, 0)

	var treeID vaultic.ID
	err := repo.WithBlobUploader(ctx, func(_ context.Context, uploader vaultic.BlobSaverWithAsync) error {
		contentID, _, _, saveErr := uploader.SaveBlob(ctx, vaultic.DataBlob, payload, vaultic.ID{}, false)
		if saveErr != nil {
			return saveErr
		}
		treeID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{
			{Name: "z-link", Type: data.NodeTypeSymlink, Mode: 0777, LinkTarget: "a-file"},
			{Name: "a-file", Type: data.NodeTypeFile, Mode: 0640, UID: 12, GID: 34, Size: uint64(len(payload)), Content: vaultic.IDs{contentID}, ModTime: stamp, AccessTime: stamp, ChangeTime: stamp},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &data.Snapshot{Tree: &treeID, Time: stamp}
	data.TestSetSnapshotID(t, snapshot, vaultic.Hash([]byte(t.Name())))

	directFS, err := snapshotfs.New(ctx, repo, snapshot, "", snapshotfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = directFS.Close() })
	directRoot, err := directFS.Root()
	if err != nil {
		t.Fatal(err)
	}

	fuseRoot, err := newDirFromSnapshot(ctx, NewRoot(repo, Config{}), func() {}, 77, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fuseRoot.ReadDirAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for index := range entries {
		names[index] = entries[index].Name
	}
	if want := []string{".", "..", "a-file", "z-link"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("FUSE directory order = %v, want %v", names, want)
	}

	directFile, err := directRoot.Lookup(ctx, "a-file")
	if err != nil {
		t.Fatal(err)
	}
	fuseFileNode, err := fuseRoot.Lookup(ctx, "a-file")
	if err != nil {
		t.Fatal(err)
	}
	var fuseAttr fuse.Attr
	if err := fuseFileNode.Attr(ctx, &fuseAttr); err != nil {
		t.Fatal(err)
	}
	directAttr, err := directFile.Attr(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fuseAttr.Inode != directFile.Identity() || fuseAttr.Mode != directAttr.Mode || fuseAttr.Uid != directAttr.UID || fuseAttr.Gid != directAttr.GID || fuseAttr.Size != directAttr.Size || uint64(fuseAttr.Nlink) != directAttr.Links || fuseAttr.Mtime != directAttr.ModTime {
		t.Fatalf("FUSE attrs do not match snapshotfs: FUSE=%+v snapshotfs=%+v identity=%d", fuseAttr, directAttr, directFile.Identity())
	}

	handle, err := fuseFileNode.(fs.NodeOpener).Open(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := &fuse.ReadResponse{Data: make([]byte, len(payload))}
	if err := handle.(fs.HandleReader).Read(ctx, &fuse.ReadRequest{Size: len(payload)}, response); err != nil {
		t.Fatal(err)
	}
	if string(response.Data) != string(payload) {
		t.Fatalf("FUSE file bytes = %q, want %q", response.Data, payload)
	}

	fuseLink, err := fuseRoot.Lookup(ctx, "z-link")
	if err != nil {
		t.Fatal(err)
	}
	target, err := fuseLink.(fs.NodeReadlinker).Readlink(ctx, nil)
	if err != nil || target != "a-file" {
		t.Fatalf("FUSE symlink = %q, %v", target, err)
	}

	fuseFileNode.(fs.NodeForgetter).Forget()
	if err := fuseFileNode.Attr(ctx, &fuseAttr); err != nil {
		t.Fatalf("child Forget closed snapshotfs: %v", err)
	}
	fuseRoot.Forget()
	if err := fuseFileNode.Attr(ctx, &fuseAttr); err != nil {
		t.Fatalf("top Forget closed snapshotfs with live descendants: %v", err)
	}
	fuseRoot.Forget()
	fuseLink.(fs.NodeForgetter).Forget()
	if err := handle.(fs.HandleReleaser).Release(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := fuseFileNode.Attr(ctx, &fuseAttr); !errors.Is(err, snapshotfs.ErrClosed) {
		t.Fatalf("last release did not close snapshotfs: %v", err)
	}
}
