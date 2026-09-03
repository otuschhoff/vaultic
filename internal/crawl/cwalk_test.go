package crawl

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestBuildDirectoryManifest(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"a", "a/nested", "b", "ignored"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, filename := range []string{"root-file", "a/file", "a/nested/file", "ignored/file"} {
		if err := os.WriteFile(filepath.Join(root, filename), []byte(filename), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := BuildDirectoryManifest(t.Context(), []string{root}, 8, 1, func(item string, _ os.FileInfo) bool {
		return filepath.Base(item) == "ignored"
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manifest.Close() })

	tests := map[string][]string{
		root:                            {"a", "b", "ignored", "root-file"},
		filepath.Join(root, "a"):        {"file", "nested"},
		filepath.Join(root, "a/nested"): {"file"},
		filepath.Join(root, "b"):        {},
	}
	for directory, want := range tests {
		got, found, err := manifest.Names(directory)
		if err != nil || !found {
			t.Fatalf("Names(%q) = %q, %v, %v", directory, got, found, err)
		}
		sort.Strings(got)
		if len(got) != len(want) {
			t.Fatalf("Names(%q) = %q, want %q", directory, got, want)
		}
		for index := range got {
			if got[index] != want[index] {
				t.Fatalf("Names(%q) = %q, want %q", directory, got, want)
			}
		}
	}
	if _, found, err := manifest.Names(filepath.Join(root, "ignored")); err != nil || found {
		t.Fatalf("ignored directory was traversed: found=%v err=%v", found, err)
	}
}
