package nfs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	billy "github.com/go-git/go-billy/v5"
	nfsfile "github.com/willscott/go-nfs/file"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
)

const maxPathComponent = 255

// Filesystem exposes a snapshot filesystem through billy without following
// repository symlinks or permitting mutation.
type Filesystem struct {
	fs       *snapshotfs.Filesystem
	root     *snapshotfs.Node
	rootPath string
	maxRead  int
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewFilesystem(fs *snapshotfs.Filesystem, maxRead int) (*Filesystem, error) {
	if fs == nil || maxRead <= 0 {
		return nil, os.ErrInvalid
	}
	root, err := fs.Root()
	if err != nil {
		return nil, mapSnapshotError(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Filesystem{fs: fs, root: root, rootPath: root.Path(), maxRead: maxRead, ctx: ctx, cancel: cancel}, nil
}

func (fs *Filesystem) Create(string) (billy.File, error) { return nil, billy.ErrReadOnly }
func (fs *Filesystem) Rename(string, string) error       { return billy.ErrReadOnly }
func (fs *Filesystem) Remove(string) error               { return billy.ErrReadOnly }
func (fs *Filesystem) TempFile(string, string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}
func (fs *Filesystem) MkdirAll(string, os.FileMode) error { return billy.ErrReadOnly }
func (fs *Filesystem) Symlink(string, string) error       { return billy.ErrReadOnly }

func (fs *Filesystem) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.SeekCapability
}

func (fs *Filesystem) Root() string { return fs.rootPath }

func (fs *Filesystem) Join(elements ...string) string {
	return strings.Join(elements, "/")
}

func (fs *Filesystem) Chroot(name string) (billy.Filesystem, error) {
	node, err := fs.resolve(name)
	if err != nil {
		return nil, err
	}
	info, err := fs.nodeInfo(node)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &os.PathError{Op: "chroot", Path: name, Err: syscall.ENOTDIR}
	}
	return &Filesystem{fs: fs.fs, root: node, rootPath: node.Path(), maxRead: fs.maxRead, ctx: fs.ctx, cancel: fs.cancel}, nil
}

func (fs *Filesystem) Open(name string) (billy.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *Filesystem) OpenFile(name string, flag int, _ os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_TRUNC) != 0 {
		return nil, billy.ErrReadOnly
	}
	node, err := fs.resolve(name)
	if err != nil {
		return nil, err
	}
	if node.Type() != data.NodeTypeFile {
		return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EACCES}
	}
	attr, err := node.Attr(fs.ctx)
	if err != nil {
		return nil, mapPathError("open", name, err)
	}
	return &snapshotFile{name: name, node: node, size: attr.Size, maxRead: fs.maxRead, ctx: fs.ctx}, nil
}

func (fs *Filesystem) Stat(name string) (os.FileInfo, error)  { return fs.stat(name, "stat") }
func (fs *Filesystem) Lstat(name string) (os.FileInfo, error) { return fs.stat(name, "lstat") }

func (fs *Filesystem) stat(name, operation string) (os.FileInfo, error) {
	node, err := fs.resolve(name)
	if err != nil {
		return nil, err
	}
	info, err := fs.nodeInfo(node)
	if err != nil {
		return nil, mapPathError(operation, name, err)
	}
	return info, nil
}

func (fs *Filesystem) ReadDir(name string) ([]os.FileInfo, error) {
	node, err := fs.resolve(name)
	if err != nil {
		return nil, err
	}
	entries, err := node.ReadDir(fs.ctx)
	if err != nil {
		return nil, mapPathError("readdir", name, err)
	}
	result := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, infoErr := fs.nodeInfo(entry.Node)
		if infoErr != nil {
			return nil, mapPathError("readdir", name, infoErr)
		}
		result = append(result, info)
	}
	return result, nil
}

func (fs *Filesystem) Readlink(name string) (string, error) {
	node, err := fs.resolve(name)
	if err != nil {
		return "", err
	}
	target, err := node.Readlink(fs.ctx)
	if err != nil {
		return "", mapPathError("readlink", name, err)
	}
	return target, nil
}

func (fs *Filesystem) resolve(name string) (*snapshotfs.Node, error) {
	components, err := strictPath(name)
	if err != nil {
		return nil, &os.PathError{Op: "lookup", Path: name, Err: err}
	}
	node := fs.root
	for _, component := range components {
		node, err = node.Lookup(fs.ctx, component)
		if err != nil {
			return nil, mapPathError("lookup", name, err)
		}
	}
	return node, nil
}

func strictPath(name string) ([]string, error) {
	if name == "" || name == "/" {
		return nil, nil
	}
	if strings.IndexByte(name, 0) >= 0 {
		return nil, os.ErrInvalid
	}
	name = strings.TrimPrefix(name, "/")
	if name == "" || strings.HasSuffix(name, "/") {
		return nil, os.ErrInvalid
	}
	components := strings.Split(name, "/")
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, billy.ErrCrossedBoundary
		}
		if len(component) > maxPathComponent {
			return nil, syscall.ENAMETOOLONG
		}
	}
	return components, nil
}

type fileInfo struct {
	name string
	attr snapshotfs.Attr
	id   uint64
}

func (fs *Filesystem) nodeInfo(node *snapshotfs.Node) (os.FileInfo, error) {
	attr, err := node.Attr(fs.ctx)
	if err != nil {
		return nil, err
	}
	name := node.Name()
	if node.Path() == "/" {
		name = "/"
	}
	return fileInfo{name: name, attr: attr, id: node.Identity()}, nil
}

func (info fileInfo) Name() string       { return info.name }
func (info fileInfo) Size() int64        { return int64(info.attr.Size) }
func (info fileInfo) Mode() os.FileMode  { return info.attr.Mode }
func (info fileInfo) ModTime() time.Time { return info.attr.ModTime }
func (info fileInfo) IsDir() bool        { return info.attr.Mode.IsDir() }
func (info fileInfo) Sys() any {
	return &nfsfile.FileInfo{
		Nlink:  uint32(min(info.attr.Links, uint64(^uint32(0)))),
		UID:    info.attr.UID,
		GID:    info.attr.GID,
		Fileid: info.id,
	}
}

type snapshotFile struct {
	mu      sync.Mutex
	name    string
	node    *snapshotfs.Node
	size    uint64
	offset  int64
	maxRead int
	ctx     context.Context
	closed  bool
}

func (file *snapshotFile) Name() string              { return file.name }
func (file *snapshotFile) Write([]byte) (int, error) { return 0, billy.ErrReadOnly }
func (file *snapshotFile) Truncate(int64) error      { return billy.ErrReadOnly }
func (file *snapshotFile) Lock() error               { return billy.ErrNotSupported }
func (file *snapshotFile) Unlock() error             { return billy.ErrNotSupported }

func (file *snapshotFile) Close() error {
	file.mu.Lock()
	defer file.mu.Unlock()
	file.closed = true
	return nil
}

func (file *snapshotFile) Read(dst []byte) (int, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return 0, os.ErrClosed
	}
	n, err := file.readAt(dst, file.offset)
	file.offset += int64(n)
	return n, err
}

func (file *snapshotFile) ReadAt(dst []byte, offset int64) (int, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return 0, os.ErrClosed
	}
	return file.readAt(dst, offset)
}

func (file *snapshotFile) readAt(dst []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, os.ErrInvalid
	}
	if len(dst) > file.maxRead {
		dst = dst[:file.maxRead]
	}
	if uint64(offset) >= file.size {
		return 0, io.EOF
	}
	n, err := file.node.ReadAt(file.ctx, uint64(offset), dst)
	if err != nil {
		return n, mapSnapshotError(err)
	}
	if uint64(offset)+uint64(n) >= file.size {
		return n, io.EOF
	}
	return n, nil
}

func (file *snapshotFile) Seek(offset int64, whence int) (int64, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return 0, os.ErrClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = file.offset
	case io.SeekEnd:
		if file.size > uint64(^uint64(0)>>1) {
			return 0, os.ErrInvalid
		}
		base = int64(file.size)
	default:
		return 0, os.ErrInvalid
	}
	if offset < -base {
		return 0, os.ErrInvalid
	}
	if offset > 0 && base > math.MaxInt64-offset {
		return 0, os.ErrInvalid
	}
	file.offset = base + offset
	return file.offset, nil
}

func mapPathError(operation, name string, err error) error {
	return &os.PathError{Op: operation, Path: name, Err: mapSnapshotError(err)}
}

func mapSnapshotError(err error) error {
	switch {
	case errors.Is(err, snapshotfs.ErrNotFound):
		return os.ErrNotExist
	case errors.Is(err, snapshotfs.ErrInvalidName), errors.Is(err, snapshotfs.ErrNameTooLong), errors.Is(err, snapshotfs.ErrInvalidNode):
		return os.ErrInvalid
	case errors.Is(err, snapshotfs.ErrClosed):
		return os.ErrClosed
	case errors.Is(err, snapshotfs.ErrCanceled):
		return context.Canceled
	default:
		return err
	}
}

var _ billy.Filesystem = (*Filesystem)(nil)
var _ billy.Capable = (*Filesystem)(nil)
var _ billy.File = (*snapshotFile)(nil)

func canonicalPath(components []string) string {
	if len(components) == 0 {
		return "/"
	}
	return "/" + path.Join(components...)
}
