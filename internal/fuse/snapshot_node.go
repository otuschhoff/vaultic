//go:build darwin || freebsd || linux

package fuse

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"github.com/anacrolix/fuse"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
)

type snapshotFSHolder struct {
	fs   *snapshotfs.Filesystem
	mu   sync.Mutex
	refs int
}

type snapshotFSReference struct {
	holder *snapshotFSHolder
	once   sync.Once
}

func retainSnapshotFS(holder *snapshotFSHolder) snapshotFSReference {
	if holder != nil {
		holder.retain()
	}
	return snapshotFSReference{holder: holder}
}

func (reference *snapshotFSReference) release() {
	if reference.holder != nil {
		reference.once.Do(reference.holder.release)
	}
}

func (holder *snapshotFSHolder) retain() {
	holder.mu.Lock()
	holder.refs++
	holder.mu.Unlock()
}

func (holder *snapshotFSHolder) release() {
	holder.mu.Lock()
	holder.refs--
	closeFilesystem := holder.refs == 0
	holder.mu.Unlock()
	if closeFilesystem {
		_ = holder.fs.Close() // FUSE Forget cannot report errors; release process-local caches best effort.
	}
}

func snapshotError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, snapshotfs.ErrNotFound) {
		return syscall.ENOENT
	}
	return err
}

func snapshotAttr(ctx context.Context, node *snapshotfs.Node, attr *fuse.Attr) error {
	metadata, err := node.Attr(ctx)
	if err != nil {
		return snapshotError(err)
	}
	attr.Inode = node.Identity()
	attr.Mode = metadata.Mode
	attr.Size = metadata.Size
	attr.Blocks = (metadata.Size + blockSize - 1) / blockSize
	attr.BlockSize = blockSize
	attr.Nlink = uint32(metadata.Links)
	attr.Uid = metadata.UID
	attr.Gid = metadata.GID
	attr.Rdev = uint32(metadata.Device)
	attr.Atime = metadata.AccessTime
	attr.Ctime = metadata.ChangeTime
	attr.Mtime = metadata.ModTime
	return nil
}
