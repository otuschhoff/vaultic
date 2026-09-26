package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	client "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"

	"github.com/otuschhoff/vaultic/internal/data"
)

type NFSOptions struct {
	Auth                 *rpc.Auth
	UpgradeMounts        bool
	AllowMissingMetadata bool
	Connections          int
	MountPort, NFSPort   int
}

func IsNFS(filesystem FS) bool {
	switch filesystem := filesystem.(type) {
	case *NFS:
		return true
	case interface{ UnwrapFS() FS }:
		return IsNFS(filesystem.UnwrapFS())
	case Track:
		return IsNFS(filesystem.FS)
	case *Track:
		return IsNFS(filesystem.FS)
	case *PrefixMap:
		return IsNFS(filesystem.FS)
	default:
		return false
	}
}

type NFS struct {
	local
	ctx                                                         context.Context
	cancel                                                      context.CancelFunc
	options                                                     NFSOptions
	auth                                                        rpc.Auth
	mounts                                                      []nfsMount
	roots                                                       []NFSSource
	mutex                                                       sync.Mutex
	endpoints                                                   map[string]*nfsEndpoint
	entries                                                     *lru.Cache[string, nfsCachedEntry]
	lookups, getattrs, readDirPlus, reads, readBytes, cacheHits atomic.Uint64
}

type NFSStats struct {
	Lookups, Getattrs, ReadDirPlus, Reads, ReadBytes, CacheHits uint64
}

func (filesystem *NFS) Stats() NFSStats {
	return NFSStats{Lookups: filesystem.lookups.Load(), Getattrs: filesystem.getattrs.Load(), ReadDirPlus: filesystem.readDirPlus.Load(),
		Reads: filesystem.reads.Load(), ReadBytes: filesystem.readBytes.Load(), CacheHits: filesystem.cacheHits.Load()}
}

type nfsCachedEntry struct {
	endpoint *nfsEndpoint
	handle   []byte
	info     *ExtendedFileInfo
	expires  time.Time
}

type nfsEndpoint struct {
	source      NFSSource
	device      uint64
	rootFSID    uint64
	mutex       sync.Mutex
	initialized bool
	err         error
	root        string
	targets     []*client.Target
	next        atomic.Uint64
}

func NewNFS(ctx context.Context, sources []string, options NFSOptions) (*NFS, error) {
	if !options.AllowMissingMetadata {
		return nil, fmt.Errorf("direct NFS requires --nfs-allow-missing-metadata: ACLs and xattrs are not preserved")
	}
	if options.Connections == 0 {
		options.Connections = 4
	}
	if options.Connections < 1 || options.Connections > 16 {
		return nil, fmt.Errorf("NFS connections must be between 1 and 16")
	}
	auth, err := nfsProcessAuth()
	if options.Auth != nil {
		auth, err = *options.Auth, nil
	}
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	filesystem := &NFS{ctx: ctx, cancel: cancel, options: options, auth: auth, endpoints: make(map[string]*nfsEndpoint)}
	filesystem.entries, err = lru.New[string, nfsCachedEntry](4096)
	if err != nil {
		cancel()
		return nil, err
	}
	if options.UpgradeMounts {
		filesystem.mounts, err = readNFSMounts()
		if err != nil {
			cancel()
			return nil, err
		}
	}
	for _, source := range sources {
		if !IsNFSSource(source) {
			continue
		}
		parsed, err := ParseNFSSource(source)
		if err != nil {
			cancel()
			return nil, err
		}
		filesystem.roots = append(filesystem.roots, parsed)
	}
	return filesystem, nil
}

func nfsProcessAuth() (rpc.Auth, error) {
	if os.Geteuid() < 0 || os.Getegid() < 0 {
		return rpc.Auth{}, fmt.Errorf("direct NFS requires a numeric UNIX process identity")
	}
	groups, err := os.Getgroups()
	if err != nil {
		return rpc.Auth{}, err
	}
	if len(groups) > 16 {
		return rpc.Auth{}, fmt.Errorf("AUTH_SYS supports at most 16 supplementary groups; refusing to truncate credentials")
	}
	host, err := os.Hostname()
	if err != nil {
		return rpc.Auth{}, err
	}
	credentials := struct {
		Stamp    uint32
		Host     string
		UID, GID uint32
		Groups   []uint32
	}{
		Stamp: uint32(time.Now().Unix()), Host: host, UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), Groups: make([]uint32, len(groups)),
	}
	for index, group := range groups {
		credentials.Groups[index] = uint32(group)
	}
	var encoded bytes.Buffer
	if err := xdr.Write(&encoded, credentials); err != nil {
		return rpc.Auth{}, err
	}
	return rpc.Auth{Flavor: 1, Body: encoded.Bytes()}, nil
}

func (filesystem *NFS) Close() error {
	filesystem.cancel()
	filesystem.mutex.Lock()
	defer filesystem.mutex.Unlock()
	for _, endpoint := range filesystem.endpoints {
		endpoint.mutex.Lock()
		for _, target := range endpoint.targets {
			target.Client.Close()
		}
		endpoint.mutex.Unlock()
	}
	filesystem.entries.Purge()
	return nil
}

func (filesystem *NFS) route(name string) (*nfsEndpoint, string, bool, error) {
	if err := filesystem.ctx.Err(); err != nil {
		return nil, "", false, err
	}
	source, root, device, handled, err := filesystem.resolveSource(name)
	if err != nil || !handled {
		return nil, "", handled, err
	}
	if root.Server == "" {
		return nil, "", true, nil
	}
	key := fmt.Sprintf("%s\x00%s\x00%d", root.Server, root.Path, device)
	filesystem.mutex.Lock()
	defer filesystem.mutex.Unlock()
	endpoint := filesystem.endpoints[key]
	if endpoint == nil {
		endpoint = &nfsEndpoint{source: root, device: device}
		filesystem.endpoints[key] = endpoint
	}
	return endpoint, source.Path, true, nil
}

func (filesystem *NFS) resolveSource(name string) (NFSSource, NFSSource, uint64, bool, error) {
	if IsNFSSource(name) {
		source, root, err := filesystem.explicitSource(name)
		return source, root, 0, true, err
	}
	source, mount, err := mountedNFSSource(name, filesystem.mounts)
	if err != nil || mount == nil {
		return NFSSource{}, NFSSource{}, 0, false, err
	}
	root, _, err := mountedNFSSource(mount.point, filesystem.mounts)
	return source, root, mount.device, true, err
}

func (filesystem *NFS) explicitSource(name string) (NFSSource, NFSSource, error) {
	source, err := ParseNFSSource(name)
	if err != nil {
		return NFSSource{}, NFSSource{}, err
	}
	var root NFSSource
	for _, candidate := range filesystem.roots {
		if source.Server == candidate.Server && pathWithin(source.Path, candidate.Path) && len(candidate.Path) > len(root.Path) {
			root = candidate
		}
	}
	if root.Server != "" {
		return source, root, nil
	}
	for _, candidate := range filesystem.roots {
		if source.Server == candidate.Server && pathWithin(candidate.Path, source.Path) {
			return source, root, nil
		}
	}
	return NFSSource{}, NFSSource{}, os.ErrNotExist
}

func (filesystem *NFS) dial(server string, program uint32, port int) (*rpc.Client, error) {
	if port == 0 {
		connection, err := rpc.DialTCPContext(filesystem.ctx, "tcp", net.JoinHostPort(server, "111"), false)
		if err != nil {
			return nil, err
		}
		mapper := &rpc.Portmapper{Client: connection}
		port, err = mapper.Getport(rpc.Mapping{Prog: program, Vers: 3, Prot: rpc.IPProtoTCP})
		connection.Close()
		if err != nil {
			return nil, err
		}
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("NFS service %d is unavailable", program)
	}
	return rpc.DialTCPContext(filesystem.ctx, "tcp", net.JoinHostPort(server, strconv.Itoa(port)), os.Geteuid() == 0)
}

func (filesystem *NFS) initialize(endpoint *nfsEndpoint) error {
	endpoint.mutex.Lock()
	defer endpoint.mutex.Unlock()
	if endpoint.initialized {
		return endpoint.err
	}
	endpoint.initialized = true
	endpoint.err = filesystem.connectEndpoint(endpoint)
	if endpoint.err != nil {
		for _, target := range endpoint.targets {
			target.Client.Close()
		}
		endpoint.targets = nil
	}
	return endpoint.err
}

func (filesystem *NFS) connectEndpoint(endpoint *nfsEndpoint) error {
	mount, err := filesystem.dial(endpoint.source.Server, client.MountProg, filesystem.options.MountPort)
	if err != nil {
		return err
	}
	defer mount.Close()
	root := endpoint.source.Path
	var handle []byte
	for {
		handle, err = client.MountHandle(mount, root, filesystem.auth)
		if err == nil {
			break
		}
		if root == "/" || filesystem.ctx.Err() != nil {
			return err
		}
		root = path.Dir(root)
	}
	endpoint.root = root
	for index := 0; index < filesystem.options.Connections; index++ {
		connection, err := filesystem.dial(endpoint.source.Server, client.Nfs3Prog, filesystem.options.NFSPort)
		if err != nil {
			return err
		}
		target, err := client.NewTargetWithClient(connection, filesystem.auth, handle, root, 0)
		if err != nil {
			connection.Close()
			return err
		}
		endpoint.targets = append(endpoint.targets, target)
		if index == 0 {
			filesystem.getattrs.Add(1)
			attributes, err := target.GetAttr(handle)
			if err != nil {
				return err
			}
			endpoint.rootFSID = attributes.FSID
		}
	}
	return nil
}

func (filesystem *NFS) lookup(name string) (*nfsFile, bool, error) {
	endpoint, remote, handled, err := filesystem.route(name)
	if err != nil || !handled {
		return nil, handled, err
	}
	if endpoint == nil {
		return &nfsFile{filesystem: filesystem, name: name, info: &ExtendedFileInfo{Name: filesystem.Base(name), Mode: os.ModeDir | 0755}}, true, nil
	}
	key, err := filesystem.Abs(name)
	if err != nil {
		return nil, true, err
	}
	if cached, found := filesystem.entries.Get(key); found && cached.endpoint == endpoint && time.Now().Before(cached.expires) {
		filesystem.cacheHits.Add(1)
		target := endpoint.targets[endpoint.next.Add(1)%uint64(len(endpoint.targets))]
		return &nfsFile{filesystem: filesystem, name: name, target: target, handle: cached.handle, info: cached.info, endpoint: endpoint}, true, nil
	}
	if err := filesystem.initialize(endpoint); err != nil {
		return nil, true, err
	}
	target := endpoint.targets[endpoint.next.Add(1)%uint64(len(endpoint.targets))]
	handle := target.RootHandle()
	filesystem.getattrs.Add(1)
	attributes, err := target.GetAttr(handle)
	if err != nil {
		return nil, true, err
	}
	relative := strings.TrimPrefix(strings.TrimPrefix(remote, endpoint.root), "/")
	for _, component := range strings.Split(relative, "/") {
		if component == "" {
			continue
		}
		filesystem.lookups.Add(1)
		attributes, handle, err = target.LookupAt(handle, component)
		if err != nil {
			return nil, true, err
		}
	}
	info, err := nfsFileInfo(filesystem.Base(name), endpoint, attributes)
	if err != nil {
		return nil, true, err
	}
	return &nfsFile{filesystem: filesystem, name: name, target: target, handle: handle, info: info, endpoint: endpoint}, true, nil
}

func (filesystem *NFS) Lstat(name string) (*ExtendedFileInfo, error) {
	file, handled, err := filesystem.lookup(name)
	if err != nil {
		return nil, &os.PathError{Op: "lstat", Path: name, Err: err}
	}
	if !handled {
		return filesystem.local.Lstat(name)
	}
	return file.info, nil
}

func (filesystem *NFS) OpenFile(name string, flag int, metadataOnly bool) (File, error) {
	for links := 0; links < 40; links++ {
		file, handled, err := filesystem.lookup(name)
		if err != nil {
			return nil, &os.PathError{Op: "open", Path: name, Err: err}
		}
		if !handled {
			return filesystem.local.OpenFile(name, flag, metadataOnly)
		}
		if file.info.Mode&os.ModeSymlink != 0 && flag&O_NOFOLLOW == 0 {
			link, err := file.target.OpenHandle(file.handle).Readlink()
			if err != nil {
				return nil, err
			}
			name = filesystem.linkPath(name, link)
			continue
		}
		if flag&O_DIRECTORY != 0 && !file.info.Mode.IsDir() {
			return nil, syscall.ENOTDIR
		}
		if !metadataOnly {
			if err := file.MakeReadable(); err != nil {
				return nil, err
			}
		}
		return file, nil
	}
	return nil, syscall.ELOOP
}

func nfsFileInfo(name string, endpoint *nfsEndpoint, attr *client.Fattr) (*ExtendedFileInfo, error) {
	if attr.Filesize > math.MaxInt64 || attr.Used > math.MaxInt64 {
		return nil, fmt.Errorf("NFS file size overflow")
	}
	types := []os.FileMode{os.ModeIrregular, 0, os.ModeDir, os.ModeDevice, os.ModeDevice | os.ModeCharDevice, os.ModeSymlink, os.ModeSocket, os.ModeNamedPipe}
	if attr.Type < 1 || int(attr.Type) >= len(types) {
		return nil, fmt.Errorf("unsupported NFS file type %d", attr.Type)
	}
	mode := os.FileMode(attr.FileMode&0777) | types[attr.Type]
	if attr.FileMode&04000 != 0 {
		mode |= os.ModeSetuid
	}
	if attr.FileMode&02000 != 0 {
		mode |= os.ModeSetgid
	}
	if attr.FileMode&01000 != 0 {
		mode |= os.ModeSticky
	}
	device := endpoint.device
	if device == 0 || attr.FSID != endpoint.rootFSID {
		digest := sha256.Sum256(fmt.Appendf(nil, "nfs:%s:%d", endpoint.source.Server, attr.FSID))
		device = binary.BigEndian.Uint64(digest[:8])
	}
	major, minor := uint64(attr.SpecData[0]), uint64(attr.SpecData[1])
	return &ExtendedFileInfo{Name: name, Mode: mode, DeviceID: device, Inode: attr.Fileid, Links: uint64(attr.Nlink),
		UID: attr.UID, GID: attr.GID, Device: (major&0xfff)<<8 | minor&0xff | (minor & ^uint64(0xff))<<12 | (major & ^uint64(0xfff))<<32,
		Size: int64(attr.Filesize), Blocks: int64(attr.Used / 512), BlockSize: 4096,
		AccessTime: nfsTime(attr.Atime), ModTime: nfsTime(attr.Mtime), ChangeTime: nfsTime(attr.Ctime)}, nil
}

func nfsTime(value client.NFS3Time) time.Time {
	return time.Unix(int64(value.Seconds), int64(value.Nseconds))
}

type nfsFile struct {
	filesystem            *NFS
	name                  string
	target                *client.Target
	endpoint              *nfsEndpoint
	handle                []byte
	info                  *ExtendedFileInfo
	reader                *client.ReadOnlyFile
	readable, closed, eof bool
	cookie, verifier      uint64
	names                 []string
}

func (file *nfsFile) Stat() (*ExtendedFileInfo, error) {
	if file.closed {
		return nil, os.ErrClosed
	}
	return file.info, nil
}

func (file *nfsFile) MakeReadable() error {
	if file.closed {
		return os.ErrClosed
	}
	if file.readable {
		return fmt.Errorf("NFS file is already readable")
	}
	if file.target == nil {
		return fmt.Errorf("cannot enumerate a virtual NFS ancestor")
	}
	file.filesystem.getattrs.Add(1)
	attributes, err := file.target.GetAttr(file.handle)
	if err != nil {
		return err
	}
	file.info, err = nfsFileInfo(file.info.Name, file.endpoint, attributes)
	if err != nil {
		return err
	}
	if !file.info.Mode.IsRegular() && !file.info.Mode.IsDir() {
		return fmt.Errorf("refusing to read non-regular NFS source %s", file.name)
	}
	file.readable = true
	file.reader = file.target.OpenHandle(file.handle)
	return nil
}

func (file *nfsFile) Read(buffer []byte) (int, error) {
	if file.closed {
		return 0, os.ErrClosed
	}
	if !file.readable || !file.info.Mode.IsRegular() {
		return 0, os.ErrPermission
	}
	file.filesystem.reads.Add(1)
	count, err := file.reader.Read(buffer)
	file.filesystem.readBytes.Add(uint64(count))
	return count, err
}

func (file *nfsFile) Readdirnames(count int) ([]string, error) {
	if file.closed {
		return nil, os.ErrClosed
	}
	if !file.readable || !file.info.Mode.IsDir() {
		return nil, syscall.ENOTDIR
	}
	var names []string
	for count <= 0 || len(names) < count {
		if len(file.names) != 0 {
			take := len(file.names)
			if count > 0 {
				take = min(take, count-len(names))
			}
			names = append(names, file.names[:take]...)
			file.names = file.names[take:]
			continue
		}
		if file.eof {
			break
		}
		if err := file.readDirectoryPage(); err != nil {
			return names, err
		}
	}
	if count > 0 && len(names) == 0 && file.eof {
		return nil, io.EOF
	}
	return names, nil
}

func (file *nfsFile) readDirectoryPage() error {
	file.filesystem.readDirPlus.Add(1)
	entries, verifier, eof, err := file.target.ReadDirPlusPage(file.handle, file.cookie, file.verifier)
	if err != nil {
		return err
	}
	file.verifier, file.eof = verifier, eof
	for _, entry := range entries {
		file.cookie = entry.Cookie
		if entry.FileName == "." || entry.FileName == ".." {
			continue
		}
		if entry.FileName == "" || strings.ContainsAny(entry.FileName, "/\x00") {
			return fmt.Errorf("invalid NFS directory entry")
		}
		if err := file.cacheEntry(entry); err != nil {
			return err
		}
		file.names = append(file.names, entry.FileName)
	}
	return nil
}

func (file *nfsFile) cacheEntry(entry *client.EntryPlus) error {
	if !entry.Attr.IsSet || !entry.Handle.IsSet {
		return nil
	}
	if len(entry.Handle.FH) == 0 || len(entry.Handle.FH) > 64 {
		return fmt.Errorf("invalid NFS directory entry handle")
	}
	info, err := nfsFileInfo(entry.FileName, file.endpoint, &entry.Attr.Attr)
	if err != nil {
		return err
	}
	key, err := file.filesystem.Abs(file.filesystem.Join(file.name, entry.FileName))
	if err != nil {
		return err
	}
	file.filesystem.entries.Add(key, nfsCachedEntry{endpoint: file.endpoint,
		handle: append([]byte(nil), entry.Handle.FH...), info: info, expires: time.Now().Add(time.Second)})
	return nil
}

func (file *nfsFile) ToNode(_ bool, _ func(string, ...any)) (*data.Node, error) {
	if file.closed {
		return nil, os.ErrClosed
	}
	node := buildBasicNode(file.name, file.info)
	node.UID, node.GID, node.Inode, node.DeviceID = file.info.UID, file.info.GID, file.info.Inode, file.info.DeviceID
	node.AccessTime, node.ChangeTime, node.Links, node.Device = file.info.AccessTime, file.info.ChangeTime, file.info.Links, file.info.Device
	if node.Type == data.NodeTypeSymlink {
		var err error
		node.LinkTarget, err = file.target.OpenHandle(file.handle).Readlink()
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

func (file *nfsFile) Close() error {
	file.closed = true
	if file.reader != nil {
		_ = file.reader.Close()
	}
	file.reader = nil
	file.names = nil
	return nil
}
