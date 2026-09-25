package crawl

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBuildDirectoryManifestCancellationCleansTemporaryState(t *testing.T) {
	for _, beforeStart := range []bool{true, false} {
		t.Run(map[bool]string{true: "before-start", false: "during-first-root"}[beforeStart], func(t *testing.T) {
			first, second, scratch := t.TempDir(), t.TempDir(), t.TempDir()
			for _, root := range []string{first, second} {
				if err := os.Mkdir(filepath.Join(root, "child"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("TMPDIR", scratch)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if beforeStart {
				cancel()
			}
			var laterRoot atomic.Bool
			manifest, err := BuildDirectoryManifest(ctx, []string{first, second}, 32, 1, func(item string, _ os.FileInfo) bool {
				if strings.HasPrefix(item, second+string(filepath.Separator)) {
					laterRoot.Store(true)
				}
				cancel()
				return false
			})
			if manifest != nil || !errors.Is(err, context.Canceled) || laterRoot.Load() {
				t.Fatalf("manifest=%v err=%v later root visited=%t", manifest, err, laterRoot.Load())
			}
			entries, err := os.ReadDir(scratch)
			if err != nil || len(entries) != 0 {
				t.Fatalf("canceled manifest retained temporary state: %v, %v", entries, err)
			}
		})
	}
}

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
