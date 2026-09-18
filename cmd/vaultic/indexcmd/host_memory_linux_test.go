//go:build linux

package indexcmd

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCgroupReclaimableBytes(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "memory.stat"), []byte("anon 4096\ninactive_file 8192\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := cgroupReclaimableBytes(directory); got != 8192 {
		t.Fatalf("reclaimable cgroup cache = %d, want 8192", got)
	}
}

func TestReadMemoryValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.max")
	if err := os.WriteFile(path, []byte("max\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := readMemoryValue(path); ok {
		t.Fatal("unlimited cgroup value was treated as finite")
	}
	if err := os.WriteFile(path, []byte("1048576\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, ok := readMemoryValue(path); !ok || got != 1048576 {
		t.Fatalf("cgroup memory value = %d, %t", got, ok)
	}
}

func TestCgroupMemoryHeadroom(t *testing.T) {
	for _, test := range []struct {
		name                     string
		limit, used, reclaimable uint64
		want                     uint64
	}{
		{name: "unused", limit: 1024, used: 256, want: 768},
		{name: "reclaimable cache", limit: 1024, used: 768, reclaimable: 256, want: 512},
		{name: "at limit", limit: 1024, used: 1024, want: 1},
		{name: "over limit", limit: 1024, used: 2048, want: 1},
		{name: "reclaimable exceeds usage", limit: 1024, used: 256, reclaimable: 512, want: 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := cgroupMemoryHeadroom(test.limit, test.used, test.reclaimable); got != test.want {
				t.Fatalf("headroom = %d, want %d", got, test.want)
			}
		})
	}
}

func TestMinAvailableMemoryPreservesMeasuredZero(t *testing.T) {
	if got, ok := minAvailableMemory(0, false, 1024); got != 1024 || !ok {
		t.Fatalf("unknown measurement combined to %d, %t", got, ok)
	}
	if got, ok := minAvailableMemory(0, true, 1024); got != 0 || !ok {
		t.Fatalf("measured zero combined to %d, %t", got, ok)
	}
	if got, ok := minAvailableMemory(2048, true, 1024); got != 1024 || !ok {
		t.Fatalf("smaller candidate combined to %d, %t", got, ok)
	}
}

func TestCgroupMemoryPathsUsesCurrentScope(t *testing.T) {
	paths := cgroupMemoryPathsFrom([]byte("0::/system.slice/vaultic.service\n"))
	want := [][2]string{
		{"/sys/fs/cgroup/system.slice/vaultic.service/memory.max", "/sys/fs/cgroup/system.slice/vaultic.service/memory.current"},
		{"/sys/fs/cgroup/system.slice/memory.max", "/sys/fs/cgroup/system.slice/memory.current"},
		{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"},
	}
	if !slices.Equal(paths, want) {
		t.Fatalf("cgroup paths = %v", paths)
	}
}

func TestCgroupMemoryPathsIncludesV1Ancestors(t *testing.T) {
	paths := cgroupMemoryPathsFrom([]byte("7:cpu,memory:/containers/vaultic\n"))
	want := [][2]string{
		{"/sys/fs/cgroup/memory/containers/vaultic/memory.limit_in_bytes", "/sys/fs/cgroup/memory/containers/vaultic/memory.usage_in_bytes"},
		{"/sys/fs/cgroup/memory/containers/memory.limit_in_bytes", "/sys/fs/cgroup/memory/containers/memory.usage_in_bytes"},
		{"/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes"},
	}
	if !slices.Equal(paths, want) {
		t.Fatalf("cgroup paths = %v", paths)
	}
}
