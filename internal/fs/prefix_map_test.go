package fs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrefixMapRedirectsIOAndPreservesPaths(t *testing.T) {
	live := filepath.Join(t.TempDir(), "live")
	snapshot := filepath.Join(t.TempDir(), "snapshot")
	if err := os.MkdirAll(filepath.Join(snapshot, "docs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshot, "docs", "report"), []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	mapped, err := NewPrefixMap(NewLocal(), []PathMapping{{Source: live, Target: snapshot}})
	if err != nil {
		t.Fatal(err)
	}
	info, err := mapped.Lstat(filepath.Join(live, "docs", "report"))
	if err != nil || info.Size != int64(len("snapshot")) {
		t.Fatalf("mapped stat = %#v, %v", info, err)
	}
	abs, err := mapped.Abs(filepath.Join(live, "docs", "report"))
	if err != nil || abs != filepath.Join(live, "docs", "report") {
		t.Fatalf("logical path = %q, %v", abs, err)
	}
	if !IsLocal(mapped) {
		t.Fatal("prefix-mapped local filesystem was not recognized as local")
	}
}
