package fs

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	client "github.com/willscott/go-nfs-client/nfs"
)

type NFSRestore struct {
	filesystem *NFS
	endpoint   *nfsEndpoint
	target     *client.Target
	prefix     string
}

func NewNFSRestore(ctx context.Context, destination string, options NFSOptions) (*NFSRestore, error) {
	filesystem, err := NewNFS(ctx, []string{destination}, options)
	if err != nil {
		return nil, err
	}
	endpoint, remote, handled, err := filesystem.route(destination)
	if err == nil && (!handled || endpoint == nil) {
		err = fmt.Errorf("restore target must be an NFS URL or detected NFSv3 mount")
	}
	if err == nil {
		err = filesystem.initialize(endpoint)
	}
	if err != nil {
		_ = filesystem.Close()
		return nil, err
	}
	return &NFSRestore{filesystem: filesystem, endpoint: endpoint, target: endpoint.targets[0],
		prefix: strings.TrimPrefix(strings.TrimPrefix(remote, endpoint.root), "/")}, nil
}

func (destination *NFSRestore) Close() error { return destination.filesystem.Close() }

func validateNFSRelative(name string) error {
	if name == "" || path.IsAbs(name) || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") ||
		strings.ContainsRune(name, '\x00') {
		return fmt.Errorf("invalid relative NFS restore path %q", name)
	}
	return nil
}

func (destination *NFSRestore) directory(name string, create bool) ([]byte, error) {
	if err := validateNFSRelative(name); err != nil {
		return nil, err
	}
	handle := destination.target.RootHandle()
	for _, component := range strings.Split(path.Join(destination.prefix, name), "/") {
		if component == "" || component == "." {
			continue
		}
		attributes, child, err := destination.target.LookupAt(handle, component)
		if os.IsNotExist(err) && create {
			_, err = destination.target.MkdirAt(handle, component, 0700)
			if err != nil && !os.IsExist(err) {
				return nil, err
			}
			attributes, child, err = destination.target.LookupAt(handle, component)
		}
		if err != nil {
			return nil, err
		}
		if attributes.Type != client.NF3Dir {
			return nil, fmt.Errorf("NFS restore parent %q is not a directory (symlinks are not followed)", component)
		}
		handle = child
	}
	return handle, nil
}

func (destination *NFSRestore) EnsureDir(name string) error {
	_, err := destination.directory(name, true)
	return err
}

func (destination *NFSRestore) handle(name string) ([]byte, error) {
	if err := validateNFSRelative(name); err != nil {
		return nil, err
	}
	if name == "." {
		return destination.directory(name, false)
	}
	parent, err := destination.directory(path.Dir(name), false)
	if err != nil {
		return nil, err
	}
	_, handle, err := destination.target.LookupAt(parent, path.Base(name))
	return handle, err
}

func (destination *NFSRestore) Lstat(name string) (*ExtendedFileInfo, error) {
	handle, err := destination.handle(name)
	if err != nil {
		return nil, err
	}
	attributes, err := destination.target.GetAttr(handle)
	if err != nil {
		return nil, err
	}
	return nfsFileInfo(path.Base(name), destination.endpoint, attributes)
}

func (destination *NFSRestore) Open(name string) (io.ReadCloser, error) {
	handle, err := destination.handle(name)
	if err != nil {
		return nil, err
	}
	attributes, err := destination.target.GetAttr(handle)
	if err != nil {
		return nil, err
	}
	if attributes.Type != client.NF3Reg {
		return nil, fmt.Errorf("NFS verification target %q is not a regular file", name)
	}
	return destination.target.OpenHandle(handle), nil
}

func nfsRestoreTime(value time.Time) (client.SetTime, error) {
	if value.IsZero() {
		return client.SetTime{}, nil
	}
	if value.Unix() < 0 || value.Unix() > math.MaxUint32 {
		return client.SetTime{}, fmt.Errorf("timestamp %s is outside NFSv3's supported range", value)
	}
	return client.SetTime{SetIt: client.SetToClientTime,
		Time: client.NFS3Time{Seconds: uint32(value.Unix()), Nseconds: uint32(value.Nanosecond())}}, nil
}

func (destination *NFSRestore) setMetadata(handle []byte, node *data.Node) error {
	access, err := nfsRestoreTime(node.AccessTime)
	if err != nil {
		return err
	}
	modified, err := nfsRestoreTime(node.ModTime)
	if err != nil {
		return err
	}
	attributes, err := destination.target.GetAttr(handle)
	if err != nil {
		return err
	}
	if attributes.UID != node.UID || attributes.GID != node.GID {
		_, err = destination.target.SetAttr(handle, client.Sattr3{
			UID: client.SetUID{SetIt: true, UID: node.UID}, GID: client.SetUID{SetIt: true, UID: node.GID},
		})
		if err != nil {
			return err
		}
	}
	mode := uint32(node.Mode.Perm())
	for _, bit := range []struct {
		goMode  os.FileMode
		nfsMode uint32
	}{{os.ModeSetuid, 04000}, {os.ModeSetgid, 02000}, {os.ModeSticky, 01000}} {
		if node.Mode&bit.goMode != 0 {
			mode |= bit.nfsMode
		}
	}
	_, err = destination.target.SetAttr(handle, client.Sattr3{
		Mode: client.SetMode{SetIt: node.Type != data.NodeTypeSymlink, Mode: mode}, Atime: access, Mtime: modified,
	})
	return err
}

func (destination *NFSRestore) SetMetadata(name string, node *data.Node) error {
	handle, err := destination.handle(name)
	if err != nil {
		return err
	}
	return destination.setMetadata(handle, node)
}

type NFSRestoreFile struct {
	destination       *NFSRestore
	parent, handle    []byte
	temporary, name   string
	offset            uint64
	published, closed bool
}

func (destination *NFSRestore) Create(name string, node *data.Node, hardlink string) (*NFSRestoreFile, error) {
	if err := validateNFSRelative(name); err != nil {
		return nil, err
	}
	if name == "." || (node.Type != data.NodeTypeFile && node.Type != data.NodeTypeSymlink) {
		return nil, fmt.Errorf("unsupported direct NFS restore node %q (%s)", name, node.Type)
	}
	parent, err := destination.directory(path.Dir(name), true)
	if err != nil {
		return nil, err
	}
	file := &NFSRestoreFile{destination: destination, parent: parent, name: path.Base(name), temporary: ".vaultic-restore-" + rand.Text()}
	switch {
	case hardlink != "":
		file.handle, err = destination.handle(hardlink)
		if err == nil {
			err = destination.target.LinkAt(file.handle, parent, file.temporary)
		}
	case node.Type == data.NodeTypeSymlink:
		file.handle, err = destination.target.SymlinkAt(parent, file.temporary, node.LinkTarget)
	default:
		file.handle, err = destination.target.CreateAt(parent, file.temporary, 0600)
	}
	if err != nil {
		return nil, err
	}
	if len(file.handle) == 0 {
		_, file.handle, err = destination.target.LookupAt(parent, file.temporary)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}

func (file *NFSRestoreFile) Write(buffer []byte) (int, error) {
	if file.closed || file.published {
		return 0, os.ErrClosed
	}
	written := 0
	for written < len(buffer) {
		count, err := file.destination.target.WriteAtHandle(file.handle, file.offset, buffer[written:])
		written += count
		file.offset += uint64(count)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (file *NFSRestoreFile) Publish(node *data.Node, noReplace bool) error {
	if file.closed || file.published {
		return os.ErrClosed
	}
	if err := file.destination.setMetadata(file.handle, node); err != nil {
		return err
	}
	if noReplace {
		if err := file.destination.target.LinkAt(file.handle, file.parent, file.name); err != nil {
			return err
		}
		if err := file.destination.target.RemoveAt(file.parent, file.temporary); err != nil {
			return err
		}
	} else if err := file.destination.target.RenameAt(file.parent, file.temporary, file.parent, file.name); err != nil {
		return err
	}
	file.published = true
	return nil
}

func (file *NFSRestoreFile) Open() (io.ReadCloser, error) {
	if file.closed {
		return nil, os.ErrClosed
	}
	return file.destination.target.OpenHandle(file.handle), nil
}

func (file *NFSRestoreFile) Close() error {
	if file.closed {
		return nil
	}
	file.closed = true
	if !file.published {
		err := file.destination.target.RemoveAt(file.parent, file.temporary)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove unpublished NFS temporary file %q: %w", file.temporary, err)
		}
	}
	return nil
}
