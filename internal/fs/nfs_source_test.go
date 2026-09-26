package fs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	client "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func nfsMetadataTarget(t *testing.T, respond func(uint32, []byte) []any) *client.Target {
	t.Helper()
	local, remote := net.Pipe()
	connection := rpc.NewClient(t.Context(), local)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var marker uint32
			if err := binary.Read(remote, binary.BigEndian, &marker); err != nil {
				return
			}
			request := make([]byte, marker&0x7fffffff)
			if _, err := io.ReadFull(remote, request); err != nil || len(request) < 40 {
				return
			}
			handle, err := xdr.ReadOpaque(bytes.NewReader(request[40:]))
			if err != nil {
				t.Error(err)
				return
			}
			values := []any{binary.BigEndian.Uint32(request), uint32(1), uint32(rpc.MsgAccepted), rpc.AuthNull, uint32(rpc.Success)}
			values = append(values, respond(binary.BigEndian.Uint32(request[20:]), handle)...)
			var response bytes.Buffer
			for _, value := range values {
				if err := xdr.Write(&response, value); err != nil {
					t.Error(err)
					return
				}
			}
			if err := binary.Write(remote, binary.BigEndian, uint32(response.Len())|0x80000000); err != nil {
				return
			}
			if _, err := remote.Write(response.Bytes()); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { connection.Close(); _ = remote.Close(); <-done })
	return &client.Target{Client: connection}
}

func TestNFSExhaustedPools(t *testing.T) {
	for _, capacity := range []int{0, 1, 2, 4} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			roots := []string{"nfs://server:/a", "nfs://server:/b", "nfs://server:/denied",
				"nfs://other:/c", "nfs://server:/refused", "nfs://server:/new"}
			filesystem, err := NewNFS(t.Context(), roots, NFSOptions{Auth: &rpc.AuthNull, AllowMissingMetadata: true, Connections: 4})
			if err != nil {
				t.Fatal(err)
			}
			defer filesystem.Close()
			filesystem.nextMaintenance.Store(1<<63 - 1)
			mountDials, nfsDials := 0, 0
			limit := capacity
			failure := error(rpc.ErrNoReservedPort)
			cancelOnDial := false
			dial := func(_ string, program uint32, _ int) (*rpc.Client, error) {
				if program == client.MountProg {
					mountDials++
					return nfsMetadataTarget(t, func(_ uint32, handle []byte) []any {
						return []any{uint32(0), handle, []uint32{0}}
					}).Client, nil
				}
				nfsDials++
				if nfsDials > limit {
					if cancelOnDial {
						filesystem.cancel()
					}
					return nil, failure
				}
				return nfsMetadataTarget(t, func(procedure uint32, handle []byte) []any {
					attributes := client.Fattr{Type: client.NF3Dir, Fileid: uint64(handle[1]), FSID: uint64(handle[1])}
					if strings.HasSuffix(string(handle), "/file") {
						attributes.Type, attributes.Filesize = client.NF3Reg, 2
					}
					switch procedure {
					case client.NFSProc3FSInfo:
						if string(handle) == "/denied" {
							return []any{uint32(client.NFS3ErrPerm)}
						}
						return []any{uint32(0), client.FSInfo{RTMax: uint32(handle[1]), WTMax: 64}}
					case client.NFSProc3GetAttr:
						return []any{uint32(0), attributes}
					case client.NFSProc3Lookup:
						attributes.Type, attributes.Filesize = client.NF3Reg, 2
						return []any{uint32(0), append(handle, []byte("/file")...), client.PostOpAttr{IsSet: true, Attr: attributes}, client.PostOpAttr{}}
					case client.NFSProc3Read:
						return []any{uint32(0), client.PostOpAttr{}, uint32(2), uint32(1), handle[:2]}
					default:
						t.Errorf("unexpected procedure %d", procedure)
						return []any{uint32(client.NFS3ErrPerm)}
					}
				}).Client, nil
			}
			connect := func(name string) (*nfsEndpoint, error) {
				endpoint, _, _, err := filesystem.route(name)
				if err != nil {
					return nil, err
				}
				endpoint.initialized = true
				endpoint.err = filesystem.connectEndpointWithDial(endpoint, dial)
				return endpoint, endpoint.err
			}
			first, err := connect(roots[0])
			if capacity == 0 {
				if !errors.Is(err, rpc.ErrNoReservedPort) {
					t.Fatalf("no connection to required server: %v", err)
				}
				return
			}
			if err != nil || len(first.targets) != capacity || first.borrowed {
				t.Fatalf("partial pool size=%d borrowed=%t err=%v", len(first.targets), first.borrowed, err)
			}
			second, err := connect(roots[1])
			if err != nil || !second.borrowed || second.pool != first.pool || mountDials != 1 {
				t.Fatalf("pool reuse borrowed=%t mounts=%d err=%v", second.borrowed, mountDials, err)
			}
			if capacity == 1 && len(second.pool.metadata) != 0 {
				t.Fatal("sole connection was reserved away from reads")
			}
			stats := filesystem.Stats()
			shortfalls := uint64(1)
			if capacity < 4 {
				shortfalls++
			}
			if stats.ConnectionsOpened != uint64(capacity) || stats.PoolReuses != 1 || stats.PoolShortfalls != shortfalls {
				t.Fatalf("incorrect degraded-capacity accounting: %+v", stats)
			}
			for _, endpoint := range []*nfsEndpoint{first, second} {
				err := filesystem.call(endpoint, nfsGetattr, func(target *client.Target) error {
					if string(target.RootHandle()) != endpoint.root {
						return fmt.Errorf("wrong export binding")
					}
					info, err := target.FSInfo()
					if err == nil && info.RTMax != uint32(endpoint.root[1]) {
						return fmt.Errorf("wrong export FSINFO")
					}
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
				file, err := filesystem.OpenFile("nfs://server:"+endpoint.root+"/file", O_NOFOLLOW, false)
				if err != nil {
					t.Fatal(err)
				}
				buffer := make([]byte, 128)
				count, err := file.Read(buffer)
				_ = file.Close()
				if !errors.Is(err, io.EOF) || string(buffer[:count]) != endpoint.root {
					t.Fatalf("wrong export read: %q %v", buffer[:count], err)
				}
			}
			if capacity == 1 {
				now := time.Now()
				first.lastUse.Store(now.UnixNano())
				second.lastUse.Store(now.UnixNano())
				second.lastGrowth.Store(now.UnixNano())
				limit = nfsDials + 3
				filesystem.maintainPools(now)
				if first.pool.size.Load() != 4 || filesystem.Stats().PoolGrowthAttempts != 1 {
					t.Fatal("partial pool did not recover newly available ports")
				}
				if err := filesystem.call(second, nfsGetattr, func(target *client.Target) error {
					if string(target.RootHandle()) != "/b" {
						return fmt.Errorf("new donor connection used wrong export")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				limit = nfsDials + 1
				filesystem.maintainPools(now.Add(5 * time.Second))
				own := second.recovered.Load()
				if own == nil || own == first.pool || own.size.Load() != 1 {
					t.Fatal("borrower did not recover its own connection")
				}
				limit = nfsDials + 3
				filesystem.maintainPools(now.Add(10 * time.Second))
				if own.size.Load() != 4 {
					t.Fatal("recovered borrower did not regain requested capacity")
				}
				first.lastUse.Store(now.Add(-time.Minute).UnixNano())
				first.pool.lastUse.Store(now.Add(-time.Minute).UnixNano())
				filesystem.maintainPools(now)
				if first.pool.size.Load() != 1 || filesystem.Stats().PoolRetired != 3 {
					t.Fatal("idle export did not release surplus sockets")
				}
				first.lastUse.Store(time.Now().UnixNano())
				limit = nfsDials
				filesystem.nextMaintenance.Store(0)
				filesystem.maybeMaintainPools()
				filesystem.maintenance.Wait()
				attempts := nfsDials
				filesystem.maybeMaintainPools()
				filesystem.maintenance.Wait()
				if nfsDials != attempts {
					t.Fatal("growth retries ignored the cooldown")
				}
				filesystem.nextMaintenance.Store(1<<63 - 1)
				limit = nfsDials + 3
				filesystem.maintainPools(time.Now())
				if first.pool.size.Load() != 4 {
					t.Fatal("resumed export did not regrow")
				}
				limit = nfsDials + 2
				newExport, err := connect(roots[5])
				if err != nil || newExport.borrowed || newExport.pool.size.Load() != 2 {
					t.Fatalf("new export did not use newly available ports: %v", err)
				}
				limit = nfsDials
			}
			if _, err := connect(roots[2]); !errors.Is(err, os.ErrPermission) {
				t.Fatalf("suppressed export denial: %v", err)
			}
			if _, err := filesystem.Lstat(roots[0]); err != nil {
				t.Fatalf("failed borrower damaged original pool: %v", err)
			}
			if _, err := connect(roots[3]); !errors.Is(err, rpc.ErrNoReservedPort) {
				t.Fatalf("borrowed connection across servers: %v", err)
			}
			failure = syscall.ECONNREFUSED
			if _, err := connect("nfs://server:/refused"); !errors.Is(err, syscall.ECONNREFUSED) {
				t.Fatalf("suppressed connection failure: %v", err)
			}
			failure, cancelOnDial = rpc.ErrNoReservedPort, true
			if _, err := connect("nfs://server:/refused"); !errors.Is(err, context.Canceled) {
				t.Fatalf("suppressed cancellation: %v", err)
			}
		})
	}
}

func TestNFSMaintenanceClose(t *testing.T) {
	root := "nfs://server:/a"
	filesystem, err := NewNFS(t.Context(), []string{root}, NFSOptions{Auth: &rpc.AuthNull, AllowMissingMetadata: true, Connections: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer filesystem.Close()
	endpoint, _, _, err := filesystem.route(root)
	if err != nil {
		t.Fatal(err)
	}
	target := nfsMetadataTarget(t, func(uint32, []byte) []any { return []any{uint32(client.NFS3ErrPerm)} })
	endpoint.targets = []*client.Target{target}
	endpoint.pool = newNFSPool(endpoint.targets, endpoint.server)
	endpoint.initialized = true
	endpoint.lastUse.Store(time.Now().UnixNano())
	started := make(chan struct{})
	endpoint.dial = func(string, uint32, int) (*rpc.Client, error) {
		close(started)
		<-filesystem.ctx.Done()
		return nil, filesystem.ctx.Err()
	}
	filesystem.maybeMaintainPools()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance did not start")
	}
	if err := filesystem.Close(); err != nil {
		t.Fatal(err)
	}
	if filesystem.maintaining.Load() {
		t.Fatal("close did not join maintenance")
	}
}

func TestNFSStaleMetadata(t *testing.T) {
	for _, status := range []uint32{client.NFS3ErrStale, client.NFS3ErrBadHandle, client.NFS3ErrNotDir, client.NFS3ErrNoEnt} {
		for _, retained := range []bool{false, true} {
			for _, failRetry := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/retained=%t/fail=%t", status, retained, failRetry), func(t *testing.T) {
					root := "nfs://server:/export"
					filesystem, err := NewNFS(t.Context(), []string{root}, NFSOptions{Auth: &rpc.AuthNull, AllowMissingMetadata: true, Connections: 1})
					if err != nil {
						t.Fatal(err)
					}
					defer filesystem.Close()
					endpoint, _, _, err := filesystem.route(root)
					if err != nil {
						t.Fatal(err)
					}
					var calls atomic.Uint64
					attributes := client.Fattr{Type: client.NF3Reg, UID: 12, Fileid: 99}
					target := nfsMetadataTarget(t, func(_ uint32, handle []byte) []any {
						calls.Add(1)
						if string(handle) == "stale" || failRetry {
							return []any{status}
						}
						return []any{uint32(0), []byte("fresh"), client.PostOpAttr{IsSet: true, Attr: attributes}, client.PostOpAttr{}}
					})
					endpoint.initialized, endpoint.root, endpoint.targets = true, "/export", []*client.Target{target}
					endpoint.pool = newNFSPool(endpoint.targets, endpoint.server)
					filesystem.cacheDirectory(endpoint, "/export", []byte("stale"))
					var metadata File
					if retained {
						directory := &nfsFile{filesystem: filesystem, endpoint: endpoint, name: root}
						entry, entryErr := directory.cacheEntry(&client.EntryPlus{FileName: "file", Handle: client.PostOpFH3{IsSet: true, FH: []byte("stale")}})
						if entryErr != nil {
							t.Fatal(entryErr)
						}
						metadata, err = entry.OpenMetadata()
					} else {
						metadata, err = filesystem.OpenFile(filesystem.Join(root, "file"), O_NOFOLLOW, true)
					}
					if (err != nil) != failRetry || calls.Load() != 2 {
						t.Fatalf("fallback calls=%d err=%v", calls.Load(), err)
					}
					if !failRetry {
						info, err := metadata.Stat()
						_ = metadata.Close()
						if err != nil || info.Inode != 99 || info.UID != 12 {
							t.Fatalf("fresh metadata=%+v err=%v", info, err)
						}
					}
				})
			}
		}
	}
}

func TestNFSMissingDirectoryAttributes(t *testing.T) {
	for _, withHandle := range []bool{false, true} {
		t.Run(fmt.Sprint(withHandle), func(t *testing.T) {
			root := "nfs://server:/export"
			filesystem, err := NewNFS(t.Context(), []string{root}, NFSOptions{Auth: &rpc.AuthNull, AllowMissingMetadata: true, Connections: 4})
			if err != nil {
				t.Fatal(err)
			}
			defer filesystem.Close()
			endpoint, _, _, err := filesystem.route(root)
			if err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}, 4), make(chan struct{})
			defer close(release)
			for index := 0; index < 4; index++ {
				endpoint.targets = append(endpoint.targets, nfsMetadataTarget(t, func(procedure uint32, _ []byte) []any {
					if (procedure == client.NFSProc3GetAttr) != withHandle {
						t.Errorf("unexpected fallback procedure %d, handle=%t", procedure, withHandle)
					}
					started <- struct{}{}
					<-release
					attributes := client.Fattr{Type: client.NF3Reg, UID: 12}
					if procedure == client.NFSProc3GetAttr {
						return []any{uint32(0), attributes}
					}
					return []any{uint32(0), []byte("file"), client.PostOpAttr{IsSet: true, Attr: attributes}, client.PostOpAttr{}}
				}))
			}
			endpoint.pool = newNFSPool(endpoint.targets, endpoint.server)
			directory := &nfsFile{filesystem: filesystem, endpoint: endpoint, name: root, readable: true, eof: true, info: &ExtendedFileInfo{Mode: os.ModeDir}}
			for index := 0; index < 4; index++ {
				entry, err := directory.cacheEntry(&client.EntryPlus{FileName: fmt.Sprint(index),
					Handle: client.PostOpFH3{IsSet: withHandle, FH: []byte("file")}})
				if err != nil {
					t.Fatal(err)
				}
				directory.directoryEntries = append(directory.directoryEntries, entry)
			}
			result := make(chan error, 1)
			go func() {
				entries, err := directory.ReaddirEntries(-1)
				if err == nil && (len(entries) != 4 || entries[0].Info.UID != 12) {
					err = fmt.Errorf("incorrect missing-attribute resolution")
				}
				result <- err
			}()
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for index := 0; index < 4; index++ {
				select {
				case <-started:
				case <-deadline.C:
					t.Fatal("metadata prefetch did not fill the connection pool")
				}
			}
			for index := 0; index < 4; index++ {
				release <- struct{}{}
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNFSParentCacheBounds(t *testing.T) {
	filesystem, err := NewNFS(t.Context(), nil, NFSOptions{Auth: &rpc.AuthNull, AllowMissingMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	defer filesystem.Close()
	endpoint := &nfsEndpoint{}
	filesystem.cacheDirectory(endpoint, strings.Repeat("x", 1025), []byte("file"))
	if filesystem.directories.Len() != 0 {
		t.Fatal("admitted an oversized parent path")
	}
	for index := 0; index < 5000; index++ {
		filesystem.cacheDirectory(endpoint, fmt.Sprint(index), []byte("file"))
	}
	if filesystem.directories.Len() != 4096 {
		t.Fatal("parent cache exceeded its entry limit")
	}
}

func TestNFSRestoreInputs(t *testing.T) {
	for _, name := range []string{"", "..", "../escape", "/absolute", "a/../b", "nul\x00"} {
		if err := validateNFSRelative(name); err == nil {
			t.Fatalf("accepted restore path %q", name)
		}
	}
	for _, name := range []string{".", "directory/file", "with spaces/#%"} {
		if err := validateNFSRelative(name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := nfsRestoreTime(time.Unix(-1, 0)); err == nil {
		t.Fatal("accepted an unrepresentable NFS timestamp")
	}
	if _, err := NewNFSRestore(context.Background(), "/local", NFSOptions{AllowMissingMetadata: true}); err == nil {
		t.Fatal("NFS restore silently accepted a local target")
	}
}

func TestNFSMetadataAndPaths(t *testing.T) {
	endpoint := &nfsEndpoint{source: NFSSource{Server: "nas", Path: "/export"}, device: 42, rootFSID: 7}
	attributes := &client.Fattr{Type: client.NF3Reg, FileMode: 06750, FSID: 7, Fileid: 99, Nlink: 2, UID: 12, GID: 34}
	info, err := nfsFileInfo("file", endpoint, attributes)
	if err != nil || info.DeviceID != 42 || info.Inode != 99 || info.Links != 2 || info.Mode != 0750|os.ModeSetuid|os.ModeSetgid {
		t.Fatalf("NFS metadata: %+v %v", info, err)
	}
	attributes.FSID = 8
	child, err := nfsFileInfo("file", endpoint, attributes)
	if err != nil || child.DeviceID == info.DeviceID {
		t.Fatalf("cross-filesystem identity: %+v %v", child, err)
	}
	filesystem := &NFS{}
	root := "nfs://[::1]:/export"
	childPath := filesystem.Join(root, "a b#%")
	if filesystem.Base(childPath) != "a b#%" || filesystem.Dir(childPath) != root || !filesystem.IsAbs(childPath) {
		t.Fatal(childPath)
	}
	volume := filesystem.VolumeName(root)
	if filesystem.Join(volume, "/") != "nfs://[::1]:/" {
		t.Fatal(volume)
	}
	if _, err := NewNFS(context.Background(), []string{root}, NFSOptions{}); err == nil {
		t.Fatal("missing metadata silently accepted")
	}
}

func TestMountedNFSSource(t *testing.T) {
	mounts := []nfsMount{
		{point: "/", kind: "ext4"},
		{point: "/mnt/data", root: "/subtree", source: "[::1]:/export", kind: "nfs", options: "vers=3,sec=sys", device: 42},
		{point: "/mnt/data/local", root: "/", kind: "tmpfs"},
		{point: "/mnt/secure", root: "/", source: "nas:/export", kind: "nfs", options: "vers=3,sec=krb5p"},
	}
	source, mount, err := mountedNFSSource("/mnt/data/a b", mounts)
	if err != nil || mount == nil || mount.device != 42 || source.Server != "::1" || source.Path != "/export/subtree/a b" {
		t.Fatalf("bind mount mapping: %+v %+v %v", source, mount, err)
	}
	for _, name := range []string{"/mnt/database/file", "/mnt/data/local/file"} {
		if _, mount, err := mountedNFSSource(name, mounts); err != nil || mount != nil {
			t.Fatalf("crossed mount boundary %q: %+v %v", name, mount, err)
		}
	}
	if _, _, err := mountedNFSSource("/mnt/secure/file", mounts); err == nil {
		t.Fatal("silently downgraded mount security")
	}
}

func TestParseNFSSource(t *testing.T) {
	for _, source := range []string{"nfs://nas:/export/data", "nfs://nas/export/data"} {
		parsed, err := ParseNFSSource(source)
		if err != nil || parsed.Server != "nas" || parsed.Path != "/export/data" {
			t.Fatalf("parse %q: %+v %v", source, parsed, err)
		}
	}
	parsed, err := ParseNFSSource("nfs://[::1]:/export/a%20b")
	if err != nil || parsed.Server != "::1" || parsed.Path != "/export/a b" {
		t.Fatalf("IPv6 source: %+v %v", parsed, err)
	}
	for _, source := range []string{
		"nfs:local", "nfs:///export", "nfs://nas", "nfs://user@nas:/x", "nfs://nas:2049/x", "nfs://nas:/x?q", "nfs://nas:/x#f",
		"nfs://nas:/x/%2e%2e/y", "nfs://nas:/%00", "nfs://nas:/%zz", "file:///tmp",
	} {
		if _, err := ParseNFSSource(source); err == nil {
			t.Errorf("accepted invalid source %q", source)
		}
	}
}
