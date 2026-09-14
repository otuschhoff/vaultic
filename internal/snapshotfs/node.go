package snapshotfs

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
)

type Attr struct {
	Type       data.NodeType
	Mode       os.FileMode
	UID        uint32
	GID        uint32
	Size       uint64
	Links      uint64
	Device     uint64
	ModTime    time.Time
	AccessTime time.Time
	ChangeTime time.Time
}

type Entry struct {
	Name string
	Node *Node
}

type Node struct {
	fs       *Filesystem
	node     *data.Node
	path     string
	identity uint64

	fileMu sync.Mutex
	file   *File
}

func (fs *Filesystem) newNode(path string, raw *data.Node) *Node {
	node := cloneNode(raw)
	return &Node{fs: fs, node: node, path: path, identity: fs.identity(path, node)}
}

func (node *Node) Name() string        { return node.node.Name }
func (node *Node) Path() string        { return node.path }
func (node *Node) Type() data.NodeType { return node.node.Type }
func (node *Node) Identity() uint64    { return node.identity }

func (node *Node) RawNode() data.Node {
	return *cloneNode(node.node)
}

func (node *Node) Attr(ctx context.Context) (Attr, error) {
	if err := node.lock(ctx); err != nil {
		return Attr{}, err
	}
	defer node.fs.mu.RUnlock()

	links := node.node.Links
	if node.node.Type == data.NodeTypeDir {
		entries, err := node.entries(ctx)
		if err != nil {
			return Attr{}, err
		}
		links = 2
		for _, entry := range entries {
			if entry.Node.node.Type == data.NodeTypeDir {
				links++
			}
		}
	} else if links == 0 {
		links = 1
	}

	uid, gid := node.projectOwner()
	mode := node.projectMode()
	size := node.node.Size
	if node.node.Type == data.NodeTypeSymlink {
		size = uint64(len(node.node.LinkTarget))
	}
	return Attr{
		Type: node.node.Type, Mode: mode, UID: uid, GID: gid, Size: size,
		Links: links, Device: node.node.Device, ModTime: node.node.ModTime,
		AccessTime: node.node.AccessTime, ChangeTime: node.node.ChangeTime,
	}, nil
}

func (node *Node) Lookup(ctx context.Context, name string) (*Node, error) {
	return node.lookup(ctx, name, true)
}

func (node *Node) lookup(ctx context.Context, name string, lifecycleLock bool) (*Node, error) {
	if err := validateComponent(name); err != nil {
		return nil, err
	}
	if lifecycleLock {
		if err := node.lock(ctx); err != nil {
			return nil, err
		}
		defer node.fs.mu.RUnlock()
	}
	entries, err := node.entries(ctx)
	if err != nil {
		return nil, err
	}
	index, found := slices.BinarySearchFunc(entries, name, func(entry Entry, target string) int {
		return strings.Compare(entry.Name, target)
	})
	if !found {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return entries[index].Node, nil
}

func (node *Node) ReadDir(ctx context.Context) ([]Entry, error) {
	if err := node.lock(ctx); err != nil {
		return nil, err
	}
	defer node.fs.mu.RUnlock()
	return node.entries(ctx)
}

func (node *Node) entries(ctx context.Context) ([]Entry, error) {
	if node.node.Type != data.NodeTypeDir || node.node.Subtree == nil {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrInvalidNode, node.path)
	}
	raw, err := node.fs.loadTree(ctx, *node.node.Subtree)
	if err != nil {
		return nil, err
	}
	visible := make([]*data.Node, 0, len(raw))
	for _, child := range raw {
		if child.Name == "." || child.Name == "/" {
			if child.Type != data.NodeTypeDir || child.Subtree == nil {
				return nil, fmt.Errorf("%w: special node %q is not a directory", ErrInvalidNode, child.Name)
			}
			flattened, loadErr := node.fs.loadTree(ctx, *child.Subtree)
			if loadErr != nil {
				return nil, loadErr
			}
			visible = append(visible, flattened...)
			continue
		}
		visible = append(visible, child)
	}

	entries := make([]Entry, 0, len(visible))
	seen := make(map[string]struct{}, len(visible))
	for _, child := range visible {
		if err := validateComponent(child.Name); err != nil {
			return nil, fmt.Errorf("%w: stored name: %v", ErrInvalidNode, err)
		}
		if _, exists := seen[child.Name]; exists {
			return nil, fmt.Errorf("%w: %q", ErrCollision, child.Name)
		}
		seen[child.Name] = struct{}{}
		childPath := "/" + child.Name
		if node.path != "/" {
			childPath = node.path + "/" + child.Name
		}
		entries = append(entries, Entry{Name: child.Name, Node: node.fs.newNode(childPath, child)})
	}
	slices.SortFunc(entries, func(left, right Entry) int { return strings.Compare(left.Name, right.Name) })
	return entries, nil
}

func (node *Node) ReadAt(ctx context.Context, offset uint64, dst []byte) (int, error) {
	if err := node.lock(ctx); err != nil {
		return 0, err
	}
	defer node.fs.mu.RUnlock()
	if node.node.Type != data.NodeTypeFile {
		return 0, fmt.Errorf("%w: %s is not a regular file", ErrInvalidNode, node.path)
	}

	node.fileMu.Lock()
	if node.file == nil {
		file, err := NewFile(ctx, node.fs.repo, node.fs.blobs, node.node)
		if err != nil {
			node.fileMu.Unlock()
			return 0, repositoryError(ctx, err)
		}
		node.file = file
	}
	file := node.file
	node.fileMu.Unlock()
	n, err := file.ReadAt(ctx, offset, dst)
	if err != nil {
		return n, repositoryError(ctx, err)
	}
	return n, nil
}

func (node *Node) Readlink(ctx context.Context) (string, error) {
	if err := node.lock(ctx); err != nil {
		return "", err
	}
	defer node.fs.mu.RUnlock()
	if node.node.Type != data.NodeTypeSymlink {
		return "", fmt.Errorf("%w: %s is not a symlink", ErrInvalidNode, node.path)
	}
	return node.node.LinkTarget, nil
}

func (node *Node) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return canceledError(err)
	}
	node.fs.mu.RLock()
	if node.fs.closed {
		node.fs.mu.RUnlock()
		return ErrClosed
	}
	return nil
}

func (node *Node) projectOwner() (uint32, uint32) {
	switch node.fs.cfg.Owner {
	case OwnerServer:
		return node.fs.cfg.ServerUID, node.fs.cfg.ServerGID
	case OwnerRoot:
		return 0, 0
	default:
		return node.node.UID, node.node.GID
	}
}

func (node *Node) projectMode() os.FileMode {
	mode := node.node.Mode
	switch node.node.Type {
	case data.NodeTypeDir:
		mode |= os.ModeDir
	case data.NodeTypeSymlink:
		mode |= os.ModeSymlink
	case data.NodeTypeDev:
		mode |= os.ModeDevice
	case data.NodeTypeCharDev:
		mode |= os.ModeDevice | os.ModeCharDevice
	case data.NodeTypeFifo:
		mode |= os.ModeNamedPipe
	case data.NodeTypeSocket:
		mode |= os.ModeSocket
	case data.NodeTypeIrregular:
		mode |= os.ModeIrregular
	}
	if node.fs.cfg.Permissions == PermissionsReadable {
		if node.node.Type == data.NodeTypeDir {
			mode |= 0555
		} else {
			mode |= 0444
		}
	}
	return mode
}

func (fs *Filesystem) identity(path string, node *data.Node) uint64 {
	hash := sha256.New()
	hash.Write([]byte("vaultic-snapshotfs-node-v1\x00"))
	hash.Write([]byte(fs.repositoryID))
	hash.Write([]byte{0})
	hash.Write(fs.snapshotID[:])
	hash.Write([]byte{0})
	hash.Write([]byte(node.Type))
	hash.Write([]byte{0})
	if node.Links > 1 && node.Type != data.NodeTypeDir {
		var hardlink [16]byte
		binary.BigEndian.PutUint64(hardlink[:8], node.DeviceID)
		binary.BigEndian.PutUint64(hardlink[8:], node.Inode)
		hash.Write([]byte("hardlink\x00"))
		hash.Write(hardlink[:])
	} else {
		hash.Write([]byte(path))
	}
	identity := binary.BigEndian.Uint64(hash.Sum(nil)[:8])
	if identity < 2 {
		identity += 2
	}
	return identity
}

func cloneNode(source *data.Node) *data.Node {
	clone := *source
	clone.Content = slices.Clone(source.Content)
	clone.LinkTargetRaw = slices.Clone(source.LinkTargetRaw)
	clone.ExtendedAttributes = slices.Clone(source.ExtendedAttributes)
	for index := range clone.ExtendedAttributes {
		clone.ExtendedAttributes[index].Value = slices.Clone(source.ExtendedAttributes[index].Value)
	}
	clone.GenericAttributes = make(map[data.GenericAttributeType]json.RawMessage, len(source.GenericAttributes))
	for key, value := range source.GenericAttributes {
		clone.GenericAttributes[key] = slices.Clone(value)
	}
	return &clone
}
