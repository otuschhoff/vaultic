//go:build darwin || freebsd || linux

package fuse

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
)

// The default block size to report in stat
const blockSize = 512

// Statically ensure that *file and *openFile implement the given interfaces
var _ = fs.HandleReader(&openFile{})
var _ = fs.NodeForgetter(&file{})
var _ = fs.NodeGetxattrer(&file{})
var _ = fs.NodeListxattrer(&file{})
var _ = fs.NodeOpener(&file{})

type file struct {
	root   *Root
	forget forgetFn
	node   *data.Node
	inode  uint64
}

type openFile struct {
	file
	// cumsize[i] holds the cumulative size of blobs[:i].
	cumsize []uint64
}

type logicalRangeReader interface {
	ReadLogicalFileRange(ctx context.Context, content []vaultic.ID, cumSize []uint64, offset uint64, dst []byte) (int, error)
}

func newFile(root *Root, forget forgetFn, inode uint64, node *data.Node) (fusefile *file, err error) {
	debug.Log("create new file for %v with %d blobs", node.Name, len(node.Content))
	return &file{
		inode:  inode,
		forget: forget,
		root:   root,
		node:   node,
	}, nil
}

func (f *file) Attr(_ context.Context, a *fuse.Attr) error {
	debug.Log("Attr(%v)", f.node.Name)
	a.Inode = f.inode
	a.Mode = f.node.Mode
	a.Size = f.node.Size
	a.Blocks = (f.node.Size + blockSize - 1) / blockSize
	a.BlockSize = blockSize
	// Windows (and other non-POSIX) backups may store a link count of 0.
	// FUSE must still report a positive nlink so tools that validate stat()
	// (e.g. Samba) accept the file.
	a.Nlink = max(uint32(1), uint32(f.node.Links))

	if !f.root.cfg.OwnerIsRoot {
		a.Uid = f.node.UID
		a.Gid = f.node.GID
	}
	a.Atime = f.node.AccessTime
	a.Ctime = f.node.ChangeTime
	a.Mtime = f.node.ModTime

	return nil

}

func (f *file) Open(ctx context.Context, _ *fuse.OpenRequest, _ *fuse.OpenResponse) (fs.Handle, error) {
	debug.Log("open file %v with %d blobs", f.node.Name, len(f.node.Content))

	var bytes uint64
	cumsize := make([]uint64, 1+len(f.node.Content))
	for i, id := range f.node.Content {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		size, found := f.root.repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id})
		if !found {
			return nil, errors.Errorf("id %v not found in repository", id)
		}

		bytes += uint64(size)
		cumsize[i+1] = bytes
	}

	var of = openFile{file: *f}

	if bytes != f.node.Size {
		debug.Log("sizes do not match: node.Size %v != size %v, using real size", f.node.Size, bytes)
		// Make a copy of the node with correct size
		nodenew := *f.node
		nodenew.Size = bytes
		of.file.node = &nodenew
	}
	of.cumsize = cumsize

	return &of, nil
}

func (f *openFile) getBlobAt(ctx context.Context, i int) (blob []byte, err error) {
	blob, err = f.root.blobCache.GetOrCompute(f.node.Content[i], func() ([]byte, error) {
		return f.root.repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: f.node.Content[i]}, nil)
	})
	if err != nil {
		debug.Log("LoadBlob(%v, %v) failed: %v", f.node.Name, f.node.Content[i], err)
		return nil, unwrapCtxCanceled(err)
	}

	return blob, nil
}

func (f *openFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	debug.Log("Read(%v, %v, %v), file size %v", f.node.Name, req.Size, req.Offset, f.node.Size)
	// As stated in https://godoc.org/bazil.org/fuse/fs#HandleReader there is no
	// need to check whether offset exceeds size.
	if f.node.Size == 0 {
		resp.Data = resp.Data[:0]
		return nil
	}
	if reader, ok := f.root.repo.(logicalRangeReader); ok {
		readBytes, err := reader.ReadLogicalFileRange(ctx, f.node.Content, f.cumsize, uint64(req.Offset), resp.Data[0:req.Size])
		if err != nil {
			return err
		}
		resp.Data = resp.Data[:readBytes]
		return nil
	}

	readBytes, err := vaultic.ReadLogicalFileRange(
		ctx,
		f.node.Content,
		f.cumsize,
		uint64(req.Offset),
		resp.Data[0:req.Size],
		func(ctx context.Context, index int, _ vaultic.ID) ([]byte, error) {
			return f.getBlobAt(ctx, index)
		},
	)
	if err != nil {
		return err
	}
	resp.Data = resp.Data[:readBytes]
	return nil
}

func (f *file) Listxattr(_ context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	nodeToXattrList(f.node, req, resp)
	return nil
}

func (f *file) Getxattr(_ context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return nodeGetXattr(f.node, req, resp)
}

func (f *file) Forget() {
	f.forget()
}
