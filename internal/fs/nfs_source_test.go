package fs

import (
	"context"
	client "github.com/willscott/go-nfs-client/nfs"
	"os"
	"testing"
	"time"
)

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
