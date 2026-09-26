package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	gonfs "github.com/willscott/go-nfs"
	client "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/data"
	sourcefs "github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestDirectNFSSourceReadDirPlus(t *testing.T) {
	filesystem, payload := testFilesystem(t, 4)
	server, err := New(filesystem.fs, Config{EphemeralPorts: true, MaxReadSize: 64, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() { cancel(); <-done }()
	addresses := waitReady(t, server)
	host, mountPortText, _ := net.SplitHostPort(addresses.Mount)
	_, nfsPortText, _ := net.SplitHostPort(addresses.NFS)
	mountPort, _ := strconv.Atoi(mountPortText)
	nfsPort, _ := strconv.Atoi(nfsPortText)
	root := "nfs://" + host + ":/snapshot"
	auth := rpc.NewAuthUnix("vaultic-source-test", 12, 34).Auth()
	source, err := sourcefs.NewNFS(ctx, []string{root}, sourcefs.NFSOptions{
		Auth: &auth, AllowMissingMetadata: true, Connections: 2, MountPort: mountPort, NFSPort: nfsPort,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	directory, err := source.OpenFile(root, sourcefs.O_DIRECTORY, false)
	if err != nil {
		t.Fatal(err)
	}
	names, err := directory.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}
	_ = directory.Close()
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"a-file", "directory", "large-file", "pipe", "root-file", "z-link"}) {
		t.Fatal(names)
	}
	before := source.Stats()
	for _, name := range names {
		if _, err := source.Lstat(source.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	after := source.Stats()
	if before.ReadDirPlus == 0 || after.Lookups != before.Lookups || after.Getattrs != before.Getattrs ||
		after.CacheHits-before.CacheHits != uint64(len(names)) {
		t.Fatalf("READDIRPLUS metadata was not reused: before=%+v after=%+v", before, after)
	}
	file, err := source.OpenFile(source.Join(root, "a-file"), sourcefs.O_NOFOLLOW, false)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(content, payload) {
		t.Fatalf("content=%q err=%v", content, err)
	}
	buffer := make([]byte, 1)
	if count, err := file.Read(buffer); count != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("repeated EOF=%d %v", count, err)
	}
	_ = file.Close()
	link, err := source.OpenFile(source.Join(root, "z-link"), sourcefs.O_NOFOLLOW, true)
	if err != nil {
		t.Fatal(err)
	}
	node, err := link.ToNode(false, func(string, ...any) {})
	if err != nil || node.Type != data.NodeTypeSymlink || node.LinkTarget != "a-file" {
		t.Fatalf("link=%+v err=%v", node, err)
	}
	_ = link.Close()
	if _, err := source.OpenFile(source.Join(root, "pipe"), sourcefs.O_NOFOLLOW, false); err == nil {
		t.Fatal("read a special file")
	}
	destination := repository.TestRepository(t)
	archive := archiver.New(destination, source, archiver.Options{CWalkConcurrency: 4, CWalkIncremental: true})
	snapshot, _, _, err := archive.Snapshot(ctx, []string{source.Join(root, "a-file"), source.Join(root, "z-link")}, archiver.SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loadNodes := func(id vaultic.ID) []*data.Node {
		iterator, err := data.LoadTree(ctx, destination, id)
		if err != nil {
			t.Fatal(err)
		}
		var nodes []*data.Node
		for result := range iterator {
			if result.Error != nil {
				t.Fatal(result.Error)
			}
			nodes = append(nodes, result.Node)
		}
		return nodes
	}
	nodes := loadNodes(*snapshot.Tree)
	for _, component := range []string{source.VolumeName(root), "snapshot"} {
		if len(nodes) != 1 || nodes[0].Name != component || nodes[0].Subtree == nil {
			t.Fatalf("NFS snapshot namespace: %+v", nodes)
		}
		nodes = loadNodes(*nodes[0].Subtree)
	}
	if len(nodes) != 2 || nodes[0].Name != "a-file" || nodes[1].LinkTarget != "a-file" {
		t.Fatalf("NFS snapshot entries: %+v", nodes)
	}
	var restored []byte
	for _, id := range nodes[0].Content {
		blob, err := destination.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		if err != nil {
			t.Fatal(err)
		}
		restored = append(restored, blob...)
	}
	if !bytes.Equal(restored, payload) || nodes[0].UID != 12 || nodes[0].GID != 34 {
		t.Fatal("NFS archive content or ownership mismatch")
	}
	cancel()
	if _, err := source.Lstat(root); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup=%v", err)
	}
}

func TestFilesystemReadOnlyContract(t *testing.T) {
	fs, payload := testFilesystem(t, 4)

	root, err := fs.Stat("")
	if err != nil || !root.IsDir() {
		t.Fatalf("root stat = %+v, %v", root, err)
	}
	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	if want := []string{"a-file", "directory", "large-file", "pipe", "root-file", "z-link"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("entries = %q, want %q", names, want)
	}

	info, err := fs.Lstat("z-link")
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("lstat symlink = %+v, %v", info, err)
	}
	if target, err := fs.Readlink("z-link"); err != nil || target != "a-file" {
		t.Fatalf("readlink = %q, %v", target, err)
	}
	if _, err := fs.Open("z-link"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("opened repository symlink: %v", err)
	}
	if _, err := fs.Open("pipe"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("opened special node: %v", err)
	}

	file, err := fs.Open("a-file")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	buffer := make([]byte, len(payload))
	n, err := file.Read(buffer)
	if err != nil || n != 4 || string(buffer[:n]) != string(payload[:4]) {
		t.Fatalf("bounded read = %d, %q, %v", n, buffer[:n], err)
	}
	if offset, err := file.Seek(2, io.SeekStart); err != nil || offset != 2 {
		t.Fatalf("seek = %d, %v", offset, err)
	}
	n, err = file.ReadAt(buffer, 2)
	if err != nil || n != 4 || string(buffer[:n]) != string(payload[2:6]) {
		t.Fatalf("bounded readat = %d, %q, %v", n, buffer[:n], err)
	}

	for _, invalid := range []string{".", "..", "a/../b", "a//b", "a\x00b", strings.Repeat("x", 256)} {
		if _, err := fs.Lstat(invalid); err == nil {
			t.Errorf("Lstat(%q) succeeded", invalid)
		}
	}
	mutations := []error{
		fs.Remove("a-file"), fs.Rename("a-file", "b"), fs.MkdirAll("b", 0755), fs.Symlink("a", "b"),
	}
	if _, err := fs.Create("b"); err != nil {
		mutations = append(mutations, err)
	}
	if _, err := fs.TempFile("/", "x"); err != nil {
		mutations = append(mutations, err)
	}
	if _, err := fs.OpenFile("a-file", os.O_RDWR, 0); err != nil {
		mutations = append(mutations, err)
	}
	for _, err := range mutations {
		if !errors.Is(err, billy.ErrReadOnly) {
			t.Fatalf("mutation error = %v", err)
		}
	}
	if _, err := file.Write([]byte("x")); !errors.Is(err, billy.ErrReadOnly) {
		t.Fatalf("write error = %v", err)
	}
}

func TestAuthenticatedHandlesAndVerifier(t *testing.T) {
	fs, _ := testFilesystem(t, 1024)
	h, err := newHandler(fs, "/snapshot", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.close)

	fileHandle := h.ToHandle(fs, []string{"a-file"})
	if len(fileHandle) > gonfs.FHSize {
		t.Fatalf("handle size = %d", len(fileHandle))
	}
	tampered := append([]byte(nil), fileHandle...)
	tampered[len(tampered)-1] ^= 1
	if _, _, err := h.FromHandle(tampered); !isStale(err) {
		t.Fatalf("tampered handle error = %v", err)
	}

	_ = h.ToHandle(fs, []string{"z-link"})
	resolvedFS, path, err := h.FromHandle(fileHandle)
	if err != nil || resolvedFS != fs || !reflect.DeepEqual(path, []string{"a-file"}) {
		t.Fatalf("evicted handle reconstruction = %v, %q, %v", resolvedFS, path, err)
	}

	other, err := newHandler(fs, "/snapshot", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer other.close()
	if _, _, err := other.FromHandle(fileHandle); !isStale(err) {
		t.Fatalf("cross-export handle error = %v", err)
	}

	rootEntries, _ := fs.ReadDir("/")
	dirEntries, _ := fs.ReadDir("directory")
	verifier := h.VerifierFor("/", rootEntries)
	if got := h.DataForVerifier("/", verifier); len(got) != len(rootEntries) {
		t.Fatalf("verifier cache entries = %d", len(got))
	}
	if got := h.DataForVerifier("/directory", verifier); got != nil {
		t.Fatal("cross-directory verifier accepted")
	}
	if got := h.DataForVerifier("/", verifier^1); got != nil {
		t.Fatal("forged verifier accepted")
	}
	if dirVerifier := h.VerifierFor("/directory", dirEntries); dirVerifier == verifier {
		t.Fatal("directory verifier did not bind path and contents")
	}

	h.close()
	if _, _, err := h.FromHandle(fileHandle); !isStale(err) {
		t.Fatalf("handle after close = %v", err)
	}
}

func FuzzStrictPath(f *testing.F) {
	for _, seed := range []string{"", "/", "a-file", "/directory/child", ".", "..", "a//b", "a/../b", "a\x00b", strings.Repeat("x", 256)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		components, err := strictPath(name)
		if err != nil {
			return
		}
		for _, component := range components {
			if component == "" || component == "." || component == ".." || len(component) > maxPathComponent || strings.ContainsAny(component, "/\x00") {
				t.Fatalf("strictPath(%q) accepted invalid component %q", name, component)
			}
		}
	})
}

func FuzzAuthenticatedHandleParsing(f *testing.F) {
	fs, _ := testFilesystem(f, 64)
	h, err := newHandler(fs, "/snapshot", 8)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(h.close)
	valid := h.ToHandle(fs, []string{"a-file"})
	f.Add(valid)
	tamperedTag := append([]byte(nil), valid...)
	tamperedTag[len(tamperedTag)-1] ^= 1
	f.Add(tamperedTag)
	tamperedExport := append([]byte(nil), valid...)
	tamperedExport[1] ^= 1
	f.Add(tamperedExport)
	f.Add(valid[:len(valid)-1])
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, gonfs.FHSize+1))
	f.Fuzz(func(t *testing.T, handle []byte) {
		filesystem, components, err := h.FromHandle(handle)
		if err != nil {
			if !isStale(err) {
				t.Fatalf("handle error = %v, want STALE", err)
			}
			return
		}
		if filesystem != fs {
			t.Fatal("handle resolved to another filesystem")
		}
		if _, err := filesystem.Lstat(filesystem.Join(components...)); err != nil {
			t.Fatalf("handle resolved to missing path %q: %v", components, err)
		}
	})
}

func TestLongHandleEvictionAndRestart(t *testing.T) {
	fs, _ := testFilesystem(t, 1024)
	h, err := newHandler(fs, "/snapshot", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	longPath := []string{"directory", strings.Repeat("x", 30)}
	longHandle := h.ToHandle(fs, longPath)
	if len(longHandle) > gonfs.FHSize || longHandle[21] != handleKindPathHash {
		t.Fatalf("long handle = %d bytes, kind %d", len(longHandle), longHandle[21])
	}
	otherLong := h.ToHandle(fs, []string{"directory", strings.Repeat("y", 30)})
	if len(otherLong) == 0 {
		t.Fatal("second long handle was not created")
	}
	resolvedFS, resolvedPath, err := h.FromHandle(longHandle)
	if err != nil || resolvedFS != fs || !reflect.DeepEqual(resolvedPath, longPath) {
		t.Fatalf("evicted long handle reconstruction = %v, %q, %v", resolvedFS, resolvedPath, err)
	}

	restarted, err := newHandler(fs, "/snapshot", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	if _, _, err := restarted.FromHandle(otherLong); !isStale(err) {
		t.Fatalf("pre-restart handle error = %v", err)
	}
}

func TestProjectedPermissionsAuthorizeAuthSys(t *testing.T) {
	filesystem, _ := testFilesystem(t, 1024)
	handler, err := newHandler(filesystem, "/snapshot", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.close()

	authHeader := func(uid, gid uint32) rpc.Header {
		return rpc.Header{Cred: rpc.NewAuthUnix("client", uid, gid).Auth()}
	}
	authHeaderWithGroups := func(uid, gid uint32, groups ...uint32) rpc.Header {
		var body bytes.Buffer
		if err := xdr.Write(&body, authUnixCredential{MachineName: "client", UID: uid, GID: gid, Groups: groups}); err != nil {
			t.Fatal(err)
		}
		return rpc.Header{Cred: rpc.Auth{Flavor: uint32(gonfs.AuthFlavorUnix), Body: body.Bytes()}}
	}
	for name, header := range map[string]rpc.Header{
		"owner": authHeader(12, 99),
		"group": authHeader(99, 34),
	} {
		allowed, err := handler.Access(context.Background(), header, filesystem, []string{"a-file"}, gonfs.AccessRead)
		if err != nil || allowed != gonfs.AccessRead {
			t.Fatalf("%s read access = %#x, %v", name, allowed, err)
		}
	}
	allowed, err := handler.Access(context.Background(), rpc.Header{Cred: rpc.AuthNull}, filesystem, []string{"a-file"}, gonfs.AccessRead)
	if err != nil || allowed != 0 {
		t.Fatalf("anonymous read access = %#x, %v", allowed, err)
	}
	allowed, err = handler.Access(context.Background(), authHeader(0, 34), filesystem, []string{"a-file"}, gonfs.AccessRead)
	if err != nil || allowed != 0 {
		t.Fatalf("client-claimed root read access = %#x, %v", allowed, err)
	}
	allowed, err = handler.Access(context.Background(), authHeaderWithGroups(0, 34, 34), filesystem, []string{"a-file"}, gonfs.AccessRead)
	if err != nil || allowed != 0 {
		t.Fatalf("root-squashed group access = %#x, %v", allowed, err)
	}
	allowed, err = handler.Access(context.Background(), authHeader(0, 0), filesystem, []string{"root-file"}, gonfs.AccessRead)
	if err != nil || allowed != 0 {
		t.Fatalf("root-squashed private file access = %#x, %v", allowed, err)
	}
	allowed, err = handler.Access(context.Background(), authHeader(12, 34), filesystem, []string{"a-file"}, gonfs.AccessExecute)
	if err != nil || allowed != 0 {
		t.Fatalf("owner execute access = %#x, %v", allowed, err)
	}
}

func TestConfigValidation(t *testing.T) {
	defaults, err := validateConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Listen != "127.0.0.1" || defaults.NFSPort != DefaultNFSListenPort ||
		defaults.MountPort != DefaultMountListenPort || defaults.ExportName != "/snapshot" {
		t.Fatalf("defaults = %+v", defaults.Config)
	}
	for _, cfg := range []Config{
		{Listen: "0.0.0.0"},
		{Listen: "0.0.0.0", AllowCIDRs: []string{"10.0.0.0/8"}},
		{AllowCIDRs: []string{"bad"}},
		{MaxReadSize: gonfs.MaxRead + 1},
		{ExportName: "/nested/export"},
		{EphemeralPorts: true, NFSPort: 1234},
	} {
		if _, err := validateConfig(cfg); err == nil {
			t.Errorf("configuration unexpectedly valid: %+v", cfg)
		}
	}
	if _, err := validateConfig(Config{Listen: "0.0.0.0", AllowCIDRs: []string{"127.0.0.0/8"}, AcknowledgeInsecure: true}); err != nil {
		t.Fatalf("acknowledged non-loopback config: %v", err)
	}
}

func TestServerTwoListenersAndProtocol(t *testing.T) {
	fs, payload := testFilesystem(t, 3)
	server, err := New(fs.fs, Config{EphemeralPorts: true, MaxReadSize: 64, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	addresses := waitReady(t, server)

	for _, address := range []string{addresses.NFS, addresses.Mount} {
		connection, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			t.Fatalf("dial %s: %v", address, err)
		}
		_ = connection.Close()
	}
	for _, address := range []string{addresses.NFS, addresses.Mount} {
		_, port, _ := net.SplitHostPort(address)
		udp, err := net.ListenPacket("udp", net.JoinHostPort("127.0.0.1", port))
		if err != nil {
			t.Fatalf("unexpected UDP listener on TCP port %s: %v", port, err)
		}
		_ = udp.Close()
	}

	host, mountPortText, _ := net.SplitHostPort(addresses.Mount)
	mountPort, _ := net.LookupPort("tcp", mountPortText)
	mountClient, err := client.DialServiceAtPort(host, mountPort)
	if err != nil {
		t.Fatal(err)
	}
	_ = mountRoot(t, mountClient, "/snapshot")
	assertMountDump(t, mountClient, []string{"/snapshot"})
	assertMountExport(t, mountClient, "/snapshot")
	callMount(t, mountClient, gonfs.MountProcUmnt, "/snapshot")
	assertMountDump(t, mountClient, nil)
	_ = mountRoot(t, mountClient, "/snapshot")
	callMount(t, mountClient, gonfs.MountProcUmntAll, "")
	assertMountDump(t, mountClient, nil)
	rootHandle := mountRoot(t, mountClient, "/snapshot")
	_, nfsPortText, _ := net.SplitHostPort(addresses.NFS)
	nfsPort, _ := net.LookupPort("tcp", nfsPortText)
	nfsClient, err := client.DialServiceAtPort(host, nfsPort)
	if err != nil {
		t.Fatal(err)
	}
	target, err := client.NewTargetWithClient(nfsClient, rpc.NewAuthUnix("vaultic-test", 12, 34).Auth(), rootHandle, "/snapshot", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fsInfo, err := target.FSInfo()
	if err != nil || fsInfo.RTMax != 64 || fsInfo.RTPref != 64 {
		t.Fatalf("FSINFO read limits = %d/%d, %v", fsInfo.RTMax, fsInfo.RTPref, err)
	}

	file, err := target.Open("a-file")
	if err != nil {
		t.Fatal(err)
	}
	read, err := io.ReadAll(file)
	if err != nil || string(read) != string(payload) {
		t.Fatalf("RPC read = %q, %v", read, err)
	}
	link, err := target.Open("z-link")
	if err != nil {
		t.Fatal(err)
	}
	if destination, err := link.Readlink(); err != nil || destination != "a-file" {
		t.Fatalf("RPC readlink = %q, %v", destination, err)
	}
	entries, err := target.ReadDirPlus("/")
	if err != nil {
		t.Fatal(err)
	}
	expectedEntries := map[string]bool{
		"a-file": false, "directory": false, "large-file": false,
		"pipe": false, "root-file": false, "z-link": false,
	}
	for _, entry := range entries {
		if _, expected := expectedEntries[entry.FileName]; !expected {
			t.Fatalf("RPC READDIRPLUS returned unexpected entry %q", entry.FileName)
		}
		if !entry.Attr.IsSet || entry.FileId == 0 || entry.Attr.Attr.Fileid != entry.FileId {
			t.Fatalf("RPC READDIRPLUS attributes for %q = %+v", entry.FileName, entry)
		}
		if !entry.Handle.IsSet || len(entry.Handle.FH) == 0 {
			t.Fatalf("RPC READDIRPLUS handle for %q = %+v", entry.FileName, entry.Handle)
		}
		attributes, err := target.GetAttr(entry.Handle.FH)
		if err != nil || attributes.Fileid != entry.FileId {
			t.Fatalf("RPC GETATTR for READDIRPLUS handle %q = %+v, %v", entry.FileName, attributes, err)
		}
		expectedEntries[entry.FileName] = true
	}
	for name, found := range expectedEntries {
		if !found {
			t.Errorf("RPC READDIRPLUS omitted %q", name)
		}
	}
	if _, err := target.OpenFile("created", 0600); err == nil || !strings.Contains(err.Error(), "ROFS") {
		t.Fatalf("RPC create error = %v", err)
	}
	assertCommit(t, nfsClient, rootHandle)
	assertCappedRead(t, target, nfsClient, 64)
	assertProgramUnavailable(t, mountClient, gonfs.NFSProgram)
	assertProgramUnavailable(t, nfsClient, gonfs.MountProgram)
	stats := server.Stats()
	if stats.Procedures.NFS[gonfs.NFSProcedureReadDirPlus] == 0 ||
		stats.Procedures.NFS[gonfs.NFSProcedureCommit] == 0 ||
		stats.Procedures.Mount[gonfs.MountProcMount] == 0 {
		t.Fatalf("procedure stats = %+v", stats.Procedures)
	}

	malformed, err := net.DialTimeout("tcp", addresses.NFS, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var marker [4]byte
	binary.BigEndian.PutUint32(marker[:], uint32(server.cfg.MaxRequestSize+1)|(1<<31))
	_, _ = malformed.Write(marker[:])
	_ = malformed.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := malformed.Read(make([]byte, 1)); err == nil {
		t.Fatal("oversized RPC connection remained open")
	}
	_ = malformed.Close()
	malformed, err = net.DialTimeout("tcp", addresses.NFS, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(marker[:], uint32(39)|(1<<31))
	_, _ = malformed.Write(append(marker[:], make([]byte, 39)...))
	_ = malformed.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := malformed.Read(make([]byte, 1)); err == nil {
		t.Fatal("malformed RPC connection remained open")
	}
	_ = malformed.Close()

	nfsClient.Close()
	mountClient.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestServerIdleTimeoutUsesRPCActivity(t *testing.T) {
	fs, _ := testFilesystem(t, 64)
	server, err := New(fs.fs, Config{
		EphemeralPorts: true, MaxReadSize: 64, IdleTimeout: 150 * time.Millisecond,
		ConnectionIdleTimeout: time.Second, DrainTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background()) }()
	addresses := waitReady(t, server)
	host, portText, _ := net.SplitHostPort(addresses.Mount)
	port, _ := net.LookupPort("tcp", portText)
	rpcClient, err := client.DialServiceAtPort(host, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()
	time.Sleep(100 * time.Millisecond)
	_ = mountRoot(t, rpcClient, "/snapshot")
	select {
	case err := <-done:
		t.Fatalf("server stopped before activity-adjusted timeout: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	_ = callMount(t, rpcClient, gonfs.MountProcUmnt, "/snapshot")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("idle shutdown: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("server did not stop after idle timeout")
	}
}

type blockingRangeRepository struct {
	vaultic.Repository
	started chan struct{}
}

type stubbornRangeRepository struct {
	vaultic.Repository
	started chan struct{}
	release chan struct{}
}

func (repo *stubbornRangeRepository) ReadLogicalFileRange(context.Context, []vaultic.ID, []uint64, uint64, []byte) (int, error) {
	close(repo.started)
	<-repo.release
	return 0, context.Canceled
}

func (repo *blockingRangeRepository) ReadLogicalFileRange(ctx context.Context, _ []vaultic.ID, _ []uint64, _ uint64, _ []byte) (int, error) {
	close(repo.started)
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestServerCloseCancelsBlockedRepositoryRead(t *testing.T) {
	ctx := context.Background()
	base := repository.TestRepository(t)
	payload := []byte("blocked read")
	var treeID vaultic.ID
	err := base.WithBlobUploader(ctx, func(_ context.Context, uploader vaultic.BlobSaverWithAsync) error {
		contentID, _, _, saveErr := uploader.SaveBlob(ctx, vaultic.DataBlob, payload, vaultic.ID{}, false)
		if saveErr != nil {
			return saveErr
		}
		treeID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{{
			Name: "file", Type: data.NodeTypeFile, Mode: 0444, Size: uint64(len(payload)), Content: vaultic.IDs{contentID},
		}})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &data.Snapshot{Tree: &treeID, Time: time.Unix(1_700_000_000, 0)}
	data.TestSetSnapshotID(t, snapshot, vaultic.Hash([]byte(t.Name())))
	wrapped := &blockingRangeRepository{Repository: base, started: make(chan struct{})}
	snapshotFilesystem, err := snapshotfs.New(ctx, wrapped, snapshot, "", snapshotfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(snapshotFilesystem, Config{DrainTimeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	file, err := server.filesystem.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, readErr := file.Read(make([]byte, len(payload)))
		readDone <- readErr
	}()
	select {
	case <-wrapped.started:
	case <-time.After(time.Second):
		t.Fatal("repository read did not start")
	}

	started := time.Now()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("server close took %s", elapsed)
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked read error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read did not observe shutdown cancellation")
	}
}

func TestServerCloseRetainsCleanupAfterDrainTimeout(t *testing.T) {
	ctx := context.Background()
	base := repository.TestRepository(t)
	payload := []byte("stubborn read")
	var treeID vaultic.ID
	err := base.WithBlobUploader(ctx, func(_ context.Context, uploader vaultic.BlobSaverWithAsync) error {
		contentID, _, _, saveErr := uploader.SaveBlob(ctx, vaultic.DataBlob, payload, vaultic.ID{}, false)
		if saveErr != nil {
			return saveErr
		}
		treeID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{{
			Name: "file", Type: data.NodeTypeFile, Mode: 0444, Size: uint64(len(payload)), Content: vaultic.IDs{contentID},
		}})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &data.Snapshot{Tree: &treeID, Time: time.Unix(1_700_000_000, 0)}
	data.TestSetSnapshotID(t, snapshot, vaultic.Hash([]byte(t.Name())))
	wrapped := &stubbornRangeRepository{Repository: base, started: make(chan struct{}), release: make(chan struct{})}
	snapshotFilesystem, err := snapshotfs.New(ctx, wrapped, snapshot, "", snapshotfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(snapshotFilesystem, Config{EphemeralPorts: true, MaxReadSize: 64, DrainTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	root, err := snapshotFilesystem.Root()
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background()) }()
	addresses := waitReady(t, server)
	mountReply := rawRPCCall(t, addresses.Mount, rawRPCRequest(2, gonfs.MountProgram, 3, uint32(gonfs.MountProcMount), opaque([]byte("/snapshot"))))
	if nfsStatusValue := binary.BigEndian.Uint32(mountReply.body[:4]); nfsStatusValue != uint32(gonfs.MountStatusOk) {
		t.Fatalf("mount status = %d", nfsStatusValue)
	}
	rootHandle, _ := readOpaque(t, mountReply.body[4:])
	fileHandle := rawLookup(t, addresses.NFS, rootHandle, "file")
	connection, err := net.DialTimeout("tcp", addresses.NFS, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	readRequest := rawNFSRequest(uint32(gonfs.NFSProcedureRead), join(opaque(fileHandle), uint64Word(0), words(uint32(len(payload)))))
	if _, err := io.Copy(connection, io.MultiReader(bytes.NewReader(words(uint32(len(readRequest))|1<<31)), bytes.NewReader(readRequest))); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		var marker [4]byte
		_, readErr := io.ReadFull(connection, marker[:])
		readDone <- readErr
	}()
	select {
	case <-wrapped.started:
	case <-time.After(time.Second):
		t.Fatal("stubborn repository read did not start")
	}

	started := time.Now()
	if err := server.Close(); err == nil {
		t.Fatal("drain timeout was not reported")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded close took %s", elapsed)
	}
	_ = connection.Close()
	close(wrapped.release)
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("RPC read transport did not exit")
	}
	select {
	case <-server.CleanupDone():
	case <-time.After(time.Second):
		t.Fatal("snapshot filesystem cleanup did not finish after late request drained")
	}
	if _, err := root.Attr(context.Background()); !errors.Is(err, snapshotfs.ErrClosed) {
		t.Fatalf("snapshot filesystem remained open: %v", err)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve did not return")
	}
}

func mountRoot(t *testing.T, rpcClient *rpc.Client, dirpath string) []byte {
	t.Helper()
	type mountCall struct {
		rpc.Header
		Dirpath string
	}
	response, err := rpcClient.Call(&mountCall{Header: rpc.Header{
		Rpcvers: 2, Prog: gonfs.MountProgram, Vers: 3, Proc: uint32(gonfs.MountProcMount), Cred: rpc.AuthNull, Verf: rpc.AuthNull,
	}, Dirpath: dirpath})
	if err != nil {
		t.Fatal(err)
	}
	status, err := xdr.ReadUint32(response)
	if err != nil || status != uint32(gonfs.MountStatusOk) {
		t.Fatalf("mount status = %d, %v", status, err)
	}
	handle, err := xdr.ReadOpaque(response)
	if err != nil {
		t.Fatal(err)
	}
	flavors, err := xdr.ReadUint32List(response)
	if err != nil || !reflect.DeepEqual(flavors, []uint32{uint32(gonfs.AuthFlavorNull), uint32(gonfs.AuthFlavorUnix)}) {
		t.Fatalf("mount auth flavors = %v, %v", flavors, err)
	}
	return handle
}

func assertMountDump(t *testing.T, rpcClient *rpc.Client, wanted []string) {
	t.Helper()
	response := callMount(t, rpcClient, gonfs.MountProcDump, "")
	raw, err := io.ReadAll(response)
	if err != nil {
		t.Fatal(err)
	}
	response = bytes.NewReader(raw)
	var paths []string
	for {
		present, err := xdr.ReadUint32(response)
		if err != nil {
			t.Fatalf("dump body %x: %v", raw, err)
		}
		if present == 0 {
			break
		}
		var hostname string
		if err := xdr.Read(response, &hostname); err != nil {
			t.Fatal(err)
		}
		var path string
		if err := xdr.Read(response, &path); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	if !reflect.DeepEqual(paths, wanted) {
		t.Fatalf("mount dump = %q, want %q", paths, wanted)
	}
}

func assertMountExport(t *testing.T, rpcClient *rpc.Client, wanted string) {
	t.Helper()
	response := callMount(t, rpcClient, gonfs.MountProcExport, "")
	raw, err := io.ReadAll(response)
	if err != nil {
		t.Fatal(err)
	}
	response = bytes.NewReader(raw)
	present, err := xdr.ReadUint32(response)
	if err != nil || present != 1 {
		t.Fatalf("export body %x, present = %d, %v", raw, present, err)
	}
	var path string
	err = xdr.Read(response, &path)
	if err != nil || path != wanted {
		t.Fatalf("export path = %q, %v", path, err)
	}
	groupsEnd, err := xdr.ReadUint32(response)
	exportsEnd, endErr := xdr.ReadUint32(response)
	if err != nil || endErr != nil || groupsEnd != 0 || exportsEnd != 0 {
		t.Fatalf("export list terminators = %d/%d, %v/%v", groupsEnd, exportsEnd, err, endErr)
	}
}

func callMount(t *testing.T, rpcClient *rpc.Client, procedure gonfs.MountProcedure, dirpath string) io.ReadSeeker {
	t.Helper()
	type mountCall struct {
		rpc.Header
		Dirpath string
	}
	call := &mountCall{Header: rpc.Header{
		Rpcvers: 2, Prog: gonfs.MountProgram, Vers: 3, Proc: uint32(procedure), Cred: rpc.AuthNull, Verf: rpc.AuthNull,
	}}
	if procedure == gonfs.MountProcUmnt {
		call.Dirpath = dirpath
		response, err := rpcClient.Call(call)
		if err != nil {
			t.Fatalf("mount procedure %d: %v", procedure, err)
		}
		return response
	}
	type emptyCall struct{ rpc.Header }
	response, err := rpcClient.Call(&emptyCall{Header: call.Header})
	if err != nil {
		t.Fatalf("mount procedure %d: %v", procedure, err)
	}
	return response
}

func assertCommit(t *testing.T, rpcClient *rpc.Client, handle []byte) {
	t.Helper()
	type commitCall struct {
		rpc.Header
		Handle []byte
		Offset uint64
		Count  uint32
	}
	response, err := rpcClient.Call(&commitCall{Header: rpc.Header{
		Rpcvers: 2, Prog: gonfs.NFSProgram, Vers: 3, Proc: uint32(gonfs.NFSProcedureCommit), Cred: rpc.AuthNull, Verf: rpc.AuthNull,
	}, Handle: handle})
	if err != nil {
		t.Fatal(err)
	}
	status, err := xdr.ReadUint32(response)
	if err != nil || status != uint32(gonfs.NFSStatusOk) {
		t.Fatalf("commit status = %d, %v", status, err)
	}
	payload, err := io.ReadAll(response)
	if err != nil || len(payload) < 12 || binary.BigEndian.Uint32(payload[4:8]) != 1 {
		t.Fatalf("commit weak-cache attrs/verifier payload = %x, %v", payload, err)
	}
}

func assertCappedRead(t *testing.T, target *client.Target, rpcClient *rpc.Client, limit uint32) {
	t.Helper()
	_, handle, err := target.Lookup("large-file", false)
	if err != nil {
		t.Fatal(err)
	}
	type readCall struct {
		rpc.Header
		Handle []byte
		Offset uint64
		Count  uint32
	}
	response, err := rpcClient.Call(&readCall{Header: rpc.Header{
		Rpcvers: 2, Prog: gonfs.NFSProgram, Vers: 3, Proc: uint32(gonfs.NFSProcedureRead), Cred: rpc.AuthNull, Verf: rpc.AuthNull,
	}, Handle: handle, Count: limit * 2})
	if err != nil {
		t.Fatal(err)
	}
	status, err := xdr.ReadUint32(response)
	if err != nil || status != uint32(gonfs.NFSStatusOk) {
		t.Fatalf("read status = %d, %v", status, err)
	}
	var attrs struct {
		Present uint32
		Data    [84]byte
	}
	if err := xdr.Read(response, &attrs.Present); err != nil || attrs.Present != 1 {
		t.Fatalf("read attrs = %d, %v", attrs.Present, err)
	}
	if _, err := io.ReadFull(response, attrs.Data[:]); err != nil {
		t.Fatal(err)
	}
	count, err := xdr.ReadUint32(response)
	eof, eofErr := xdr.ReadUint32(response)
	data, dataErr := xdr.ReadOpaque(response)
	if err != nil || eofErr != nil || dataErr != nil || count != limit || len(data) != int(limit) || eof != 0 {
		t.Fatalf("capped read count/eof/data = %d/%d/%d, errors %v/%v/%v", count, eof, len(data), err, eofErr, dataErr)
	}
}

func assertProgramUnavailable(t *testing.T, rpcClient *rpc.Client, program uint32) {
	t.Helper()
	type nullCall struct{ rpc.Header }
	_, err := rpcClient.Call(&nullCall{Header: rpc.Header{Rpcvers: 2, Prog: program, Vers: 3, Cred: rpc.AuthNull, Verf: rpc.AuthNull}})
	if err == nil || !strings.Contains(err.Error(), "PROG_UNAVAIL") {
		t.Fatalf("program %d rejection = %v", program, err)
	}
}

func testFilesystem(t testing.TB, maxRead int) (*Filesystem, []byte) {
	t.Helper()
	ctx := context.Background()
	repo := repository.TestRepository(t)
	payload := []byte("snapshot bytes")
	largePayload := bytes.Repeat([]byte("x"), 128)
	var treeID, directoryID vaultic.ID
	err := repo.WithBlobUploader(ctx, func(_ context.Context, uploader vaultic.BlobSaverWithAsync) error {
		contentID, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, payload, vaultic.ID{}, false)
		if err != nil {
			return err
		}
		largeContentID, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, largePayload, vaultic.ID{}, false)
		if err != nil {
			return err
		}
		directoryID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{
			{Name: "child", Type: data.NodeTypeFile},
			{Name: strings.Repeat("x", 30), Type: data.NodeTypeFile},
			{Name: strings.Repeat("y", 30), Type: data.NodeTypeFile},
		})
		treeID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{
			{Name: "z-link", Type: data.NodeTypeSymlink, Mode: 0777, LinkTarget: "a-file"},
			{Name: "a-file", Type: data.NodeTypeFile, Mode: 0640, UID: 12, GID: 34, Size: uint64(len(payload)), Content: vaultic.IDs{contentID}},
			{Name: "root-file", Type: data.NodeTypeFile, Mode: 0400, UID: 0, GID: 0, Size: uint64(len(payload)), Content: vaultic.IDs{contentID}},
			{Name: "large-file", Type: data.NodeTypeFile, Mode: 0444, Size: uint64(len(largePayload)), Content: vaultic.IDs{largeContentID}},
			{Name: "pipe", Type: data.NodeTypeFifo, Mode: 0600},
			{Name: "directory", Type: data.NodeTypeDir, Mode: 0750, UID: 12, GID: 34, Subtree: &directoryID},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &data.Snapshot{Tree: &treeID, Time: time.Unix(1700000000, 0)}
	data.TestSetSnapshotID(t, snapshot, vaultic.Hash([]byte(t.Name())))
	snapshotFS, err := snapshotfs.New(ctx, repo, snapshot, "", snapshotfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshotFS.Close() })
	filesystem, err := NewFilesystem(snapshotFS, maxRead)
	if err != nil {
		t.Fatal(err)
	}
	return filesystem, payload
}

func waitReady(t *testing.T, server *Server) Addresses {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		readiness := server.Readiness()
		if readiness.Ready {
			return readiness.Addresses
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("server did not become ready")
	return Addresses{}
}

func isStale(err error) bool {
	var status *gonfs.NFSStatusError
	return errors.As(err, &status) && status.NFSStatus == gonfs.NFSStatusStale
}
