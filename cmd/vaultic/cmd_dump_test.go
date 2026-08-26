package main

import (
	"testing"

	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func TestDumpSplitPath(t *testing.T) {
	testPaths := []struct {
		path   string
		result []string
	}{
		{"", []string{""}},
		{"test", []string{"test"}},
		{"test/dir", []string{"test", "dir"}},
		{"test/dir/sub", []string{"test", "dir", "sub"}},
		{"/", []string{""}},
		{"/test", []string{"test"}},
		{"/test/dir", []string{"test", "dir"}},
		{"/test/dir/sub", []string{"test", "dir", "sub"}},
	}
	for _, path := range testPaths {
		parts := splitPath(path.path)
		rtest.Equals(t, path.result, parts)
	}
}

func TestResolveDumpArchive(t *testing.T) {
	for _, test := range []struct{ archive, target, want string }{
		{"auto", "snapshot.tar.gz", "tar.gz"},
		{"auto", "snapshot.tgz", "tar.gz"},
		{"auto", "snapshot.zip", "zip"},
		{"auto", "snapshot.out", "tar"},
		{"zip", "snapshot.tar.gz", "zip"},
	} {
		if got := resolveDumpArchive(test.archive, test.target); got != test.want {
			t.Fatalf("resolveDumpArchive(%q, %q) = %q, want %q", test.archive, test.target, got, test.want)
		}
	}
}
