package apfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type commandResult struct {
	output string
	err    error
}

type fakeRunner struct {
	results []commandResult
	calls   []string
}

func (runner *fakeRunner) Output(_ context.Context, name string, args ...string) ([]byte, error) {
	runner.calls = append(runner.calls, strings.Join(append([]string{name}, args...), " "))
	if len(runner.results) == 0 {
		return nil, nil
	}
	result := runner.results[0]
	runner.results = runner.results[1:]
	return []byte(result.output), result.err
}

func TestCreateMountAndCleanup(t *testing.T) {
	name := "com.apple.TimeMachine.2026-09-08-094012.local"
	runner := &fakeRunner{results: []commandResult{
		{output: "Created local snapshot with date: 2026-09-08-094012\n"},
		{output: "Snapshots for volume:\n" + name + "\n"},
		{}, {}, {},
	}}
	manager := Manager{Runner: runner, TempDir: t.TempDir()}
	mount, err := manager.CreateAndMount(t.Context(), Volume{
		Filesystem: "apfs", MountPoint: "/", DeviceNode: "/dev/disk3s1",
	}, time.Date(2026, 9, 8, 9, 40, 11, 0, time.Local), false)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(mount.MountPoint)
	if err != nil || runtime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
		t.Fatalf("mount directory mode = %v, %v", info.Mode(), err)
	}
	if err := mount.Cleanup(t.Context()); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.calls, "\n")
	fragments := []string{
		"tmutil localsnapshot", "mount_apfs -o rdonly,nobrowse -s " + name,
		"umount ", "tmutil deletelocalsnapshots 2026-09-08-094012",
	}
	for _, fragment := range fragments {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("calls missing %q:\n%s", fragment, joined)
		}
	}
}

func TestCreateAndMountClassifiesFailures(t *testing.T) {
	tests := []struct {
		name   string
		volume Volume
		result commandResult
		kind   ErrorKind
	}{
		{name: "filesystem", volume: Volume{Filesystem: "hfs"}, kind: ErrorNotAPFS},
		{name: "volume", volume: Volume{Filesystem: "apfs", MountPoint: "/Volumes/external"}, kind: ErrorNotAvailable},
		{
			name: "authorization", volume: Volume{Filesystem: "apfs", MountPoint: "/"},
			result: commandResult{output: "Operation not permitted", err: errors.New("exit 1")},
			kind:   ErrorNotAuthorized,
		},
		{name: "unavailable", volume: Volume{Filesystem: "apfs", MountPoint: "/"}, result: commandResult{err: execNotFoundError{}}, kind: ErrorNotAvailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakeRunner{results: []commandResult{test.result}}
			_, err := (Manager{Runner: runner}).CreateAndMount(t.Context(), test.volume, time.Now(), false)
			var snapshotErr *Error
			if !errors.As(err, &snapshotErr) || snapshotErr.Kind != test.kind {
				t.Fatalf("error = %#v, want kind %q", err, test.kind)
			}
		})
	}
}

type execNotFoundError struct{}

func (execNotFoundError) Error() string { return "executable file not found" }

func TestRemapRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("APFS paths use Darwin filesystem semantics")
	}
	got, err := RemapRoot("/Users/oli", "/private/tmp/snapshot", "/Users/oli/docs/report")
	if err != nil || got != filepath.Join("/private/tmp/snapshot", "docs/report") {
		t.Fatalf("remapped path = %q, %v", got, err)
	}
	if _, err := RemapRoot("/Users/oli", "/snapshot", "/Users/other"); err == nil {
		t.Fatal("outside path was remapped")
	}
}

func TestSnapshotSourcePath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("APFS paths use Darwin filesystem semantics")
	}
	tests := []struct {
		name   string
		volume Volume
		root   string
		want   string
	}{
		{name: "mounted-volume", volume: Volume{MountPoint: "/Volumes/archive"}, root: "/Volumes/archive/docs", want: "/snapshot/docs"},
		{name: "data-firmlink", volume: Volume{MountPoint: "/System/Volumes/Data"}, root: "/Users/oli", want: "/snapshot/Users/oli"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := SnapshotSourcePath(test.volume, test.root, "/snapshot")
			if err != nil || got != test.want {
				t.Fatalf("snapshot source path = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestSweepStaleDeletesOnlyOwnedLeases(t *testing.T) {
	base := t.TempDir()
	runner := &fakeRunner{results: []commandResult{{}, {}, {}}}
	manager := Manager{Runner: runner, TempDir: base}
	ownedMount := filepath.Join(base, "vaultic-apfs-stale")
	if err := os.Mkdir(ownedMount, 0o700); err != nil {
		t.Fatal(err)
	}
	valid, err := manager.writeLease(lease{
		PID: 999999, Name: "com.apple.TimeMachine.2026-09-08-094012.local", MountPoint: ownedMount,
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := manager.writeLease(lease{PID: 999999, Name: "unrelated", MountPoint: filepath.Join(base, "unrelated")})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SweepStale(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(valid); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("valid stale lease remains: %v", err)
	}
	if _, err := os.Stat(invalid); err != nil {
		t.Fatalf("unrecognized lease was removed: %v", err)
	}
	joined := strings.Join(runner.calls, "\n")
	if !strings.Contains(joined, "tmutil deletelocalsnapshots 2026-09-08-094012") || strings.Contains(joined, "unrelated") {
		t.Fatalf("unexpected sweep calls:\n%s", joined)
	}
}
