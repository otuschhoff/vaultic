//go:build darwin

package apfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type volumeRunner struct {
	payload []byte
}

func (runner volumeRunner) Output(context.Context, string, ...string) ([]byte, error) {
	return runner.payload, nil
}

func TestParseStringPlist(t *testing.T) {
	payload := []byte(`<?xml version="1.0"?><plist><dict>
<key>VolumeUUID</key><string>VOLUME</string>
<key>DeviceNode</key><string>/dev/disk3s1</string>
<key>MountPoint</key><string>/</string>
<key>FilesystemType</key><string>apfs</string>
</dict></plist>`)
	values, err := parseStringPlist(payload)
	if err != nil {
		t.Fatal(err)
	}
	if values["VolumeUUID"] != "VOLUME" || values["DeviceNode"] != "/dev/disk3s1" || values["FilesystemType"] != "apfs" {
		t.Fatalf("plist values = %#v", values)
	}
}

func TestResolveNativeAPFSVolume(t *testing.T) {
	volume, err := NewManager().ResolveVolume(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if volume.UUID == "" || volume.Device == 0 || volume.DeviceNode == "" || volume.MountPoint == "" || volume.Filesystem != "apfs" {
		t.Fatalf("resolved volume = %#v", volume)
	}
}

func TestResolveContributingVolumesPreservesTargetAndAddsNestedMount(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`<?xml version="1.0"?><plist><dict>
<key>VolumeUUID</key><string>NESTED</string>
<key>DeviceNode</key><string>/dev/disk9s1</string>
<key>MountPoint</key><string>/</string>
<key>FilesystemType</key><string>apfs</string>
</dict></plist>`)
	volumes, err := resolveContributingVolumes(t.Context(), volumeRunner{payload: payload}, Volume{Root: root, UUID: "ROOT"}, []string{"/", root, nested})
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes) != 2 || volumes[0].Root != root || volumes[1].Root != nested {
		t.Fatalf("contributing volumes = %#v", volumes)
	}
}

func TestResolveVolumesOneFileSystemPreservesExplicitTarget(t *testing.T) {
	target := t.TempDir()
	expected, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	volumes, err := NewManager().ResolveVolumes(t.Context(), target, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes) != 1 || volumes[0].Root != expected {
		t.Fatalf("resolved volumes = %#v", volumes)
	}
}
