package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
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
	"golang.org/x/sync/errgroup"

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
	servers                                                     map[string]*nfsServer
	operations                                                  [5]nfsOperationCounters
	connectionsOpened, poolShortfalls, poolReuses               atomic.Uint64
	poolGrowthAttempts, poolGrowthFailures, poolRetired         atomic.Uint64
	nextMaintenance                                             atomic.Int64
	maintaining                                                 atomic.Bool
	maintenance                                                 sync.WaitGroup
	entries                                                     *lru.Cache[string, nfsCachedEntry]
	directories                                                 *lru.Cache[nfsDirectoryKey, nfsDirectoryHandle]
	parentHits, retainedOpens                                   atomic.Uint64
	lookups, getattrs, readDirPlus, reads, readBytes, cacheHits atomic.Uint64
}

type NFSStats struct {
	PoolGrowthAttempts uint64                       `json:"connection_pool_growth_attempts"`
	PoolGrowthFailures uint64                       `json:"connection_pool_growth_failures"`
	PoolRetired        uint64                       `json:"nfs_connections_retired_idle"`
	ConnectionsOpened  uint64                       `json:"nfs_connections_opened"`
	PoolShortfalls     uint64                       `json:"connection_pool_shortfalls"`
	PoolReuses         uint64                       `json:"connection_pool_reuses"`
	Lookups            uint64                       `json:"lookup_calls"`
	Getattrs           uint64                       `json:"getattr_calls"`
	ReadDirPlus        uint64                       `json:"readdirplus_calls"`
	Reads              uint64                       `json:"read_calls"`
	ReadBytes          uint64                       `json:"read_bytes"`
	CacheHits          uint64                       `json:"metadata_cache_hits"`
	ParentHits         uint64                       `json:"parent_handle_hits"`
	RetainedOpens      uint64                       `json:"retained_metadata_opens"`
	Operations         map[string]NFSOperationStats `json:"rpc_operations"`
}

func (filesystem *NFS) Stats() NFSStats {
	operations := make(map[string]NFSOperationStats, len(nfsOperationNames))
	for index, name := range nfsOperationNames {
		operations[name] = filesystem.operations[index].stats()
	}
	return NFSStats{Lookups: filesystem.lookups.Load(), Getattrs: filesystem.getattrs.Load(), ReadDirPlus: filesystem.readDirPlus.Load(),
		PoolGrowthAttempts: filesystem.poolGrowthAttempts.Load(), PoolGrowthFailures: filesystem.poolGrowthFailures.Load(),
		PoolRetired:       filesystem.poolRetired.Load(),
		ConnectionsOpened: filesystem.connectionsOpened.Load(), PoolShortfalls: filesystem.poolShortfalls.Load(), PoolReuses: filesystem.poolReuses.Load(),
		Reads: filesystem.reads.Load(), ReadBytes: filesystem.readBytes.Load(), CacheHits: filesystem.cacheHits.Load(),
		ParentHits: filesystem.parentHits.Load(), RetainedOpens: filesystem.retainedOpens.Load(), Operations: operations}
}

type nfsDirectoryKey struct {
	endpoint *nfsEndpoint
	path     string
}

type nfsDirectoryHandle struct {
	handle  []byte
	expires time.Time
}

func (filesystem *NFS) cacheDirectory(endpoint *nfsEndpoint, name string, handle []byte) {
	if len(name) > 1024 || len(handle) > 64 {
		return
	}
	filesystem.directories.Add(nfsDirectoryKey{endpoint: endpoint, path: strings.Clone(name)},
		nfsDirectoryHandle{handle: append([]byte(nil), handle...), expires: time.Now().Add(time.Second)})
}

type nfsCachedEntry struct {
	endpoint *nfsEndpoint
	handle   []byte
	info     *ExtendedFileInfo
	expires  time.Time
}

type nfsEndpoint struct {
	source        NFSSource
	device        uint64
	rootFSID      uint64
	mutex         sync.Mutex
	initialized   bool
	err           error
	root          string
	targets       []*client.Target
	borrowed      bool
	bindings      map[*client.Target]*client.Target
	bindingsMutex sync.Mutex
	recovered     atomic.Pointer[nfsPool]
	lastUse       atomic.Int64
	lastGrowth    atomic.Int64
	dial          func(string, uint32, int) (*rpc.Client, error)
	pool          *nfsPool
	server        *nfsServer
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
	filesystem := &NFS{ctx: ctx, cancel: cancel, options: options, auth: auth, endpoints: make(map[string]*nfsEndpoint), servers: make(map[string]*nfsServer)}
	filesystem.entries, err = lru.New[string, nfsCachedEntry](4096)
	if err != nil {
		cancel()
		return nil, err
	}
	filesystem.directories, err = lru.New[nfsDirectoryKey, nfsDirectoryHandle](4096)
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
	filesystem.mutex.Lock()
	filesystem.cancel()
	filesystem.mutex.Unlock()
	filesystem.maintenance.Wait()
	filesystem.mutex.Lock()
	defer filesystem.mutex.Unlock()
	for _, endpoint := range filesystem.endpoints {
		endpoint.mutex.Lock()
		if !endpoint.borrowed {
			targets := endpoint.targets
			if endpoint.pool != nil {
				targets = endpoint.pool.snapshot()
			}
			for _, target := range targets {
				target.Client.Close()
			}
		}
		if pool := endpoint.recovered.Load(); pool != nil {
			for _, target := range pool.snapshot() {
				target.Client.Close()
			}
		}
		endpoint.mutex.Unlock()
	}
	for _, server := range filesystem.servers {
		server.mutex.Lock()
		if server.mount != nil {
			server.mount.Close()
		}
		server.mutex.Unlock()
	}
	filesystem.entries.Purge()
	filesystem.directories.Purge()
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
		serverKey := strings.ToLower(root.Server)
		server := filesystem.servers[serverKey]
		if server == nil {
			server = newNFSServer(filesystem.options.Connections)
			filesystem.servers[serverKey] = server
		}
		endpoint = &nfsEndpoint{source: root, device: device, server: server}
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
		if !endpoint.borrowed {
			for _, target := range endpoint.targets {
				target.Client.Close()
			}
		}
		endpoint.targets = nil
	}
	return endpoint.err
}

func (filesystem *NFS) connectEndpoint(endpoint *nfsEndpoint) error {
	return filesystem.connectEndpointWithDial(endpoint, filesystem.dial)
}

func (filesystem *NFS) mountEndpoint(endpoint *nfsEndpoint, dial func(string, uint32, int) (*rpc.Client, error)) ([]byte, error) {
	server := endpoint.server
	if server.mount == nil {
		mount, err := dial(endpoint.source.Server, client.MountProg, filesystem.options.MountPort)
		if err != nil {
			return nil, err
		}
		server.mount = mount
	}
	root := endpoint.source.Path
	var handle []byte
	var err error
	for {
		handle, err = client.MountHandle(server.mount, root, filesystem.auth)
		if err == nil {
			break
		}
		if root == "/" || filesystem.ctx.Err() != nil {
			return nil, err
		}
		root = path.Dir(root)
	}
	endpoint.root = root
	return handle, nil
}

func (filesystem *NFS) connectEndpointWithDial(endpoint *nfsEndpoint, dial func(string, uint32, int) (*rpc.Client, error)) error {
	endpoint.dial = dial
	server := endpoint.server
	server.mutex.Lock()
	defer server.mutex.Unlock()
	handle, err := filesystem.mountEndpoint(endpoint, dial)
	if err != nil {
		return err
	}
	root := endpoint.root
	for index := 0; index < filesystem.options.Connections; index++ {
		connection, err := dial(endpoint.source.Server, client.Nfs3Prog, filesystem.options.NFSPort)
		if err != nil {
			if contextErr := filesystem.ctx.Err(); contextErr != nil {
				return contextErr
			}
			if !errors.Is(err, rpc.ErrNoReservedPort) {
				return err
			}
			filesystem.poolShortfalls.Add(1)
			if len(endpoint.targets) == 0 {
				if server.fallback == nil {
					return err
				}
				if err := filesystem.borrowPool(endpoint, server.fallback, handle); err != nil {
					return err
				}
			}
			break
		}
		filesystem.connectionsOpened.Add(1)
		target, err := client.NewTargetWithClient(connection, filesystem.auth, handle, root, 0)
		if err != nil {
			connection.Close()
			return err
		}
		endpoint.targets = append(endpoint.targets, target)
	}
	filesystem.getattrs.Add(1)
	attributes, err := endpoint.targets[0].GetAttr(handle)
	if err != nil {
		return err
	}
	endpoint.rootFSID = attributes.FSID
	filesystem.cacheDirectory(endpoint, root, handle)
	if endpoint.pool == nil {
		endpoint.pool = newNFSPool(endpoint.targets, server)
	}
	if endpoint.borrowed {
		filesystem.poolReuses.Add(1)
	}
	if server.fallback == nil {
		server.fallback = endpoint.pool
	}
	return nil
}

func (filesystem *NFS) borrowPool(endpoint *nfsEndpoint, pool *nfsPool, handle []byte) error {
	endpoint.borrowed = true
	targets := pool.snapshot()
	endpoint.bindings = make(map[*client.Target]*client.Target, len(targets))
	for _, original := range targets {
		target, err := client.NewTargetWithClient(original.Client, filesystem.auth, handle, endpoint.root, 0)
		if err != nil {
			return err
		}
		endpoint.targets = append(endpoint.targets, target)
		endpoint.bindings[original] = target
	}
	endpoint.pool = pool
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
		return &nfsFile{filesystem: filesystem, name: name, handle: cached.handle, info: cached.info, endpoint: endpoint}, true, nil
	}
	if err := filesystem.initialize(endpoint); err != nil {
		return nil, true, err
	}
	attributes, handle, err := filesystem.lookupHandle(endpoint, remote, true)
	if err != nil {
		return nil, true, err
	}
	info, err := nfsFileInfo(filesystem.Base(name), endpoint, attributes)
	if err != nil {
		return nil, true, err
	}
	return &nfsFile{filesystem: filesystem, name: name, handle: handle, info: info, endpoint: endpoint}, true, nil
}

func invalidNFSHandle(err error) bool {
	var failure *client.Error
	return errors.Is(err, os.ErrNotExist) || (errors.As(err, &failure) &&
		(failure.ErrorNum == client.NFS3ErrStale || failure.ErrorNum == client.NFS3ErrBadHandle || failure.ErrorNum == client.NFS3ErrNotDir))
}

func (filesystem *NFS) lookupHandle(endpoint *nfsEndpoint, remote string, allowCached bool) (*client.Fattr, []byte, error) {
	handle := endpoint.targets[0].RootHandle()
	relative := strings.TrimPrefix(strings.TrimPrefix(remote, endpoint.root), "/")
	parent := nfsDirectoryKey{endpoint: endpoint, path: path.Dir(remote)}
	cachedParent := false
	current := endpoint.root
	if allowCached && relative != "" {
		if cached, found := filesystem.directories.Get(parent); found && time.Now().Before(cached.expires) {
			filesystem.parentHits.Add(1)
			handle, relative, current, cachedParent = cached.handle, path.Base(remote), parent.path, true
		}
	}
	var attributes *client.Fattr
	var err error
	if relative == "" {
		filesystem.getattrs.Add(1)
		err = filesystem.call(endpoint, nfsGetattr, func(target *client.Target) error {
			var callErr error
			attributes, callErr = target.GetAttr(handle)
			return callErr
		})
		if err != nil {
			return nil, nil, err
		}
	}
	for _, component := range strings.Split(relative, "/") {
		if component == "" {
			continue
		}
		filesystem.lookups.Add(1)
		err = filesystem.call(endpoint, nfsLookup, func(target *client.Target) error {
			var callErr error
			attributes, handle, callErr = target.LookupAt(handle, component)
			return callErr
		})
		if err != nil {
			if cachedParent && invalidNFSHandle(err) {
				filesystem.directories.Remove(parent)
				return filesystem.lookupHandle(endpoint, remote, false)
			}
			return nil, nil, err
		}
		current = path.Join(current, component)
		if attributes.Type == client.NF3Dir {
			filesystem.cacheDirectory(endpoint, current, handle)
		}
	}
	return attributes, handle, nil
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
			link, err := file.readlink()
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
	endpoint              *nfsEndpoint
	handle                []byte
	info                  *ExtendedFileInfo
	offset                int64
	readable, closed, eof bool
	cookie, verifier      uint64
	directoryEntries      []ReadDirEntry
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
	if file.endpoint == nil {
		return fmt.Errorf("cannot enumerate a virtual NFS ancestor")
	}
	file.filesystem.getattrs.Add(1)
	var attributes *client.Fattr
	err := file.filesystem.call(file.endpoint, nfsGetattr, func(target *client.Target) error {
		var callErr error
		attributes, callErr = target.GetAttr(file.handle)
		return callErr
	})
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
	var count int
	err := file.filesystem.call(file.endpoint, nfsRead, func(target *client.Target) error {
		reader := target.OpenHandle(file.handle)
		defer reader.Close()
		var callErr error
		count, callErr = reader.ReadAt(buffer, file.offset)
		return callErr
	})
	file.offset += int64(count)
	file.filesystem.readBytes.Add(uint64(count))
	return count, err
}

func (file *nfsFile) Readdirnames(count int) ([]string, error) {
	entries, err := file.ReaddirEntries(count)
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name
	}
	return names, err
}

func (file *nfsFile) ReaddirEntries(count int) ([]ReadDirEntry, error) {
	if file.closed {
		return nil, os.ErrClosed
	}
	if !file.readable || !file.info.Mode.IsDir() {
		return nil, syscall.ENOTDIR
	}
	var entries []ReadDirEntry
	for count <= 0 || len(entries) < count {
		if len(file.directoryEntries) != 0 {
			take := len(file.directoryEntries)
			if count > 0 {
				take = min(take, count-len(entries))
			}
			entries = append(entries, file.directoryEntries[:take]...)
			file.directoryEntries = file.directoryEntries[take:]
			continue
		}
		if file.eof {
			break
		}
		if err := file.readDirectoryPage(); err != nil {
			return entries, err
		}
	}
	group, ctx := errgroup.WithContext(file.filesystem.ctx)
	group.SetLimit(file.filesystem.options.Connections)
	for index := range entries {
		if entries[index].Info != nil {
			continue
		}
		group.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			metadata, err := entries[index].OpenMetadata()
			if err != nil {
				return err
			}
			defer metadata.Close()
			entries[index].Info, err = metadata.Stat()
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if count > 0 && len(entries) == 0 && file.eof {
		return nil, io.EOF
	}
	return entries, nil
}

func (file *nfsFile) readDirectoryPage() error {
	file.filesystem.readDirPlus.Add(1)
	var entries []*client.EntryPlus
	var verifier uint64
	var eof bool
	err := file.filesystem.call(file.endpoint, nfsReadDirPlus, func(target *client.Target) error {
		var callErr error
		entries, verifier, eof, callErr = target.ReadDirPlusPage(file.handle, file.cookie, file.verifier)
		return callErr
	})
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
		retained, err := file.cacheEntry(entry)
		if err != nil {
			return err
		}
		file.directoryEntries = append(file.directoryEntries, retained)
	}
	return nil
}

func (file *nfsFile) cacheEntry(entry *client.EntryPlus) (ReadDirEntry, error) {
	if entry.Handle.IsSet && (len(entry.Handle.FH) == 0 || len(entry.Handle.FH) > 64) {
		return ReadDirEntry{}, fmt.Errorf("invalid NFS directory entry handle")
	}
	var info *ExtendedFileInfo
	var err error
	if entry.Attr.IsSet {
		info, err = nfsFileInfo(entry.FileName, file.endpoint, &entry.Attr.Attr)
		if err != nil {
			return ReadDirEntry{}, err
		}
	}
	key, err := file.filesystem.Abs(file.filesystem.Join(file.name, entry.FileName))
	if err != nil {
		return ReadDirEntry{}, err
	}
	var handle []byte
	if entry.Handle.IsSet {
		handle = append([]byte(nil), entry.Handle.FH...)
	}
	expires := time.Now().Add(time.Second)
	if entry.Handle.IsSet && info != nil {
		file.filesystem.entries.Add(key, nfsCachedEntry{endpoint: file.endpoint, handle: handle, info: info, expires: expires})
	}
	filesystem, endpoint, parent := file.filesystem, file.endpoint, file.handle
	name := entry.FileName
	return ReadDirEntry{Name: name, Info: info, OpenMetadata: func() (File, error) {
		if err := filesystem.ctx.Err(); err != nil {
			return nil, err
		}
		filesystem.retainedOpens.Add(1)
		currentInfo, currentHandle := info, handle
		var attributes *client.Fattr
		var err error
		if len(currentHandle) == 0 {
			filesystem.lookups.Add(1)
			err = filesystem.call(endpoint, nfsLookup, func(target *client.Target) error {
				var callErr error
				attributes, currentHandle, callErr = target.LookupAt(parent, name)
				return callErr
			})
		} else if currentInfo == nil || !time.Now().Before(expires) {
			filesystem.getattrs.Add(1)
			err = filesystem.call(endpoint, nfsGetattr, func(target *client.Target) error {
				var callErr error
				attributes, callErr = target.GetAttr(currentHandle)
				return callErr
			})
		}
		if err != nil {
			if invalidNFSHandle(err) {
				filesystem.entries.Remove(key)
				filesystem.directories.Purge()
				return filesystem.OpenFile(key, O_NOFOLLOW, true)
			}
			return nil, err
		}
		if attributes != nil {
			currentInfo, err = nfsFileInfo(name, endpoint, attributes)
			if err != nil {
				return nil, err
			}
		}
		return &nfsFile{filesystem: filesystem, endpoint: endpoint, name: key, handle: currentHandle, info: currentInfo}, nil
	}}, nil
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
		node.LinkTarget, err = file.readlink()
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

func (file *nfsFile) readlink() (string, error) {
	var link string
	err := file.filesystem.call(file.endpoint, nfsReadlink, func(target *client.Target) error {
		reader := target.OpenHandle(file.handle)
		defer reader.Close()
		var callErr error
		link, callErr = reader.Readlink()
		return callErr
	})
	return link, err
}

func (file *nfsFile) Close() error {
	file.closed = true
	file.directoryEntries = nil
	return nil
}
