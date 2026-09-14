//go:build darwin || freebsd || linux

package fuse

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"

	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
)

// The default block size to report in stat
const blockSize = 512

// Statically ensure that *file and *openFile implement the given interfaces
var _ = fs.HandleReader(&openFile{})
var _ = fs.HandleReleaser(&openFile{})
var _ = fs.NodeForgetter(&file{})
var _ = fs.NodeGetxattrer(&file{})
var _ = fs.NodeListxattrer(&file{})
var _ = fs.NodeOpener(&file{})

type file struct {
	forget    forgetFn
	node      *snapshotfs.Node
	holder    *snapshotFSHolder
	reference snapshotFSReference
}

type openFile struct {
	node      *snapshotfs.Node
	reference snapshotFSReference
}

func newFile(forget forgetFn, node *snapshotfs.Node, holder *snapshotFSHolder) (fusefile *file, err error) {
	debug.Log("create new file for %v", node.Path())
	return &file{
		forget:    forget,
		node:      node,
		holder:    holder,
		reference: retainSnapshotFS(holder),
	}, nil
}

func (f *file) Attr(ctx context.Context, a *fuse.Attr) error {
	debug.Log("Attr(%v)", f.node.Path())
	return snapshotAttr(ctx, f.node, a)
}

func (f *file) Open(_ context.Context, _ *fuse.OpenRequest, _ *fuse.OpenResponse) (fs.Handle, error) {
	debug.Log("open file %v", f.node.Path())
	return &openFile{node: f.node, reference: retainSnapshotFS(f.holder)}, nil
}

func (f *openFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	debug.Log("Read(%v, %v, %v)", f.node.Path(), req.Size, req.Offset)
	// As stated in https://godoc.org/bazil.org/fuse/fs#HandleReader there is no
	// need to check whether offset exceeds size.
	readBytes, err := f.node.ReadAt(ctx, uint64(req.Offset), resp.Data[0:req.Size])
	if err != nil {
		return unwrapCtxCanceled(err)
	}
	resp.Data = resp.Data[:readBytes]
	return nil
}

func (f *file) Listxattr(_ context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	raw := f.node.RawNode()
	nodeToXattrList(&raw, req, resp)
	return nil
}

func (f *file) Getxattr(_ context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	raw := f.node.RawNode()
	return nodeGetXattr(&raw, req, resp)
}

func (f *file) Forget() {
	f.forget()
	f.reference.release()
}

func (f *openFile) Release(_ context.Context, _ *fuse.ReleaseRequest) error {
	f.reference.release()
	return nil
}
