package archiver

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/otuschhoff/vaultic/internal/fs"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func TestScanner(t *testing.T) {
	var tests = []struct {
		name  string
		src   TestDir
		want  map[string]ScanStats
		selFn SelectFunc
	}{
		{
			name: "include-all",
			src: TestDir{
				"other": TestFile{Content: "another file"},
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
			},
			want: map[string]ScanStats{
				filepath.FromSlash("other"):               {Files: 1, Bytes: 12},
				filepath.FromSlash("work/foo"):            {Files: 2, Bytes: 15},
				filepath.FromSlash("work/foo.txt"):        {Files: 3, Bytes: 28},
				filepath.FromSlash("work/subdir/bar.txt"): {Files: 4, Bytes: 45},
				filepath.FromSlash("work/subdir/other"):   {Files: 5, Bytes: 60},
				filepath.FromSlash("work/subdir"):         {Files: 5, Dirs: 1, Bytes: 60},
				filepath.FromSlash("work"):                {Files: 5, Dirs: 2, Bytes: 60},
				filepath.FromSlash(""):                    {Files: 5, Dirs: 2, Bytes: 60},
			},
		},
		{
			name: "select-txt",
			src: TestDir{
				"other": TestFile{Content: "another file"},
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
			},
			selFn: func(item string, fi *fs.ExtendedFileInfo, fs fs.FS) bool {
				if fi.Mode.IsDir() {
					return true
				}

				if filepath.Ext(item) == ".txt" {
					return true
				}
				return false
			},
			want: map[string]ScanStats{
				filepath.FromSlash("work/foo.txt"):        {Files: 1, Bytes: 13},
				filepath.FromSlash("work/subdir/bar.txt"): {Files: 2, Bytes: 30},
				filepath.FromSlash("work/subdir"):         {Files: 2, Dirs: 1, Bytes: 30},
				filepath.FromSlash("work"):                {Files: 2, Dirs: 2, Bytes: 30},
				filepath.FromSlash(""):                    {Files: 2, Dirs: 2, Bytes: 30},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()

			tempdir := rtest.TempDir(t)
			TestCreateFiles(t, tempdir, test.src)

			back := rtest.Chdir(t, tempdir)
			defer back()

			cur, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}

			sc := NewScanner(fs.Track{FS: fs.NewLocal()})
			if test.selFn != nil {
				sc.Select = test.selFn
			}

			results := make(map[string]ScanStats)
			sc.Result = func(item string, s ScanStats) {
				var p string
				var err error

				if item != "" {
					p, err = filepath.Rel(cur, item)
					if err != nil {
						panic(err)
					}
				}

				results[p] = s
			}

			err = sc.Scan(ctx, []string{"."})
			if err != nil {
				t.Fatal(err)
			}

			if !cmp.Equal(test.want, results) {
				t.Error(cmp.Diff(test.want, results))
			}
		})
	}
}

func TestScannerCWalkEquivalent(t *testing.T) {
	tempdir := rtest.TempDir(t)
	TestCreateFiles(t, tempdir, TestDir{
		"keep": TestDir{
			"a.txt":  TestFile{Content: "alpha"},
			"empty":  TestDir{},
			"nested": TestDir{"b.bin": TestFile{Content: "binary"}},
		},
		"skip":    TestDir{"ignored.txt": TestFile{Content: "ignored"}},
		"top.txt": TestFile{Content: "top"},
	})

	run := func(workers int) (ScanStats, []string) {
		t.Helper()
		scanner := NewScanner(fs.NewLocal())
		scanner.CWalkWorkers = workers
		scanner.SelectByName = func(item string) bool {
			return filepath.Base(item) != "skip"
		}
		scanner.Select = func(_ string, info *fs.ExtendedFileInfo, _ fs.FS) bool {
			return info.Mode.IsDir() || info.Size <= 5
		}
		var final ScanStats
		var paths []string
		scanner.Result = func(item string, stats ScanStats) {
			if item == "" {
				final = stats
				return
			}
			relative, err := filepath.Rel(tempdir, item)
			if err != nil {
				t.Fatal(err)
			}
			paths = append(paths, relative)
		}
		if err := scanner.Scan(t.Context(), []string{tempdir}); err != nil {
			t.Fatal(err)
		}
		sort.Strings(paths)
		return final, paths
	}

	wantStats, wantPaths := run(0)
	gotStats, gotPaths := run(8)
	if diff := cmp.Diff(wantStats, gotStats); diff != "" {
		t.Errorf("final stats mismatch (-sequential +cwalk):\n%s", diff)
	}
	if diff := cmp.Diff(wantPaths, gotPaths); diff != "" {
		t.Errorf("visited paths mismatch (-sequential +cwalk):\n%s", diff)
	}
}

func TestScannerCWalkSuppressedErrorFallsBack(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions differ on Windows")
	}
	tempdir := rtest.TempDir(t)
	TestCreateFiles(t, tempdir, TestDir{
		"readable":   TestFile{Content: "readable"},
		"unreadable": TestDir{"hidden": TestFile{Content: "hidden"}},
	})
	unreadable := filepath.Join(tempdir, "unreadable")
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o700) })

	scanner := NewScanner(fs.NewLocal())
	scanner.CWalkWorkers = 4
	scanner.Error = func(_ string, _ error) error { return nil }
	var final ScanStats
	scanner.Result = func(item string, stats ScanStats) {
		if item == "" {
			final = stats
		}
	}
	if err := scanner.Scan(t.Context(), []string{tempdir}); err != nil {
		t.Fatal(err)
	}
	if final.Files != 1 || final.Bytes != uint64(len("readable")) {
		t.Fatalf("fallback stats = %#v", final)
	}
}

func TestScannerError(t *testing.T) {
	var tests = []struct {
		name    string
		unix    bool
		src     TestDir
		result  ScanStats
		selFn   SelectFunc
		errFn   func(t testing.TB, item string, err error) error
		resFn   func(t testing.TB, item string, s ScanStats)
		prepare func(t testing.TB)
	}{
		{
			name: "no-error",
			src: TestDir{
				"other": TestFile{Content: "another file"},
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
			},
			result: ScanStats{Files: 5, Dirs: 2, Bytes: 60},
		},
		{
			name: "unreadable-dir",
			unix: true,
			src: TestDir{
				"other": TestFile{Content: "another file"},
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
			},
			result: ScanStats{Files: 3, Dirs: 1, Bytes: 28},
			prepare: func(t testing.TB) {
				err := os.Chmod(filepath.Join("work", "subdir"), 0000)
				if err != nil {
					t.Fatal(err)
				}
			},
			errFn: func(t testing.TB, item string, err error) error {
				if item == filepath.FromSlash("work/subdir") {
					return nil
				}

				return err
			},
		},
		{
			name: "removed-item",
			src: TestDir{
				"bar":   TestFile{Content: "bar"},
				"baz":   TestFile{Content: "baz"},
				"foo":   TestFile{Content: "foo"},
				"other": TestFile{Content: "other"},
			},
			result: ScanStats{Files: 3, Dirs: 0, Bytes: 11},
			resFn: func(t testing.TB, item string, s ScanStats) {
				if item == "bar" {
					err := os.Remove("foo")
					if err != nil {
						t.Fatal(err)
					}
				}
			},
			errFn: func(t testing.TB, item string, err error) error {
				if item == "foo" {
					t.Logf("ignoring error for %v: %v", item, err)
					return nil
				}

				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.unix && runtime.GOOS == "windows" {
				t.Skipf("skip on windows")
			}

			ctx := t.Context()

			tempdir := rtest.TempDir(t)
			TestCreateFiles(t, tempdir, test.src)

			back := rtest.Chdir(t, tempdir)
			defer back()

			cur, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}

			if test.prepare != nil {
				test.prepare(t)
			}

			sc := NewScanner(fs.Track{FS: fs.NewLocal()})
			if test.selFn != nil {
				sc.Select = test.selFn
			}

			var stats ScanStats

			sc.Result = func(item string, s ScanStats) {
				if item == "" {
					stats = s
					return
				}

				if test.resFn != nil {
					p, relErr := filepath.Rel(cur, item)
					if relErr != nil {
						panic(relErr)
					}
					test.resFn(t, p, s)
				}
			}
			if test.errFn != nil {
				sc.Error = func(item string, err error) error {
					p, relErr := filepath.Rel(cur, item)
					if relErr != nil {
						panic(relErr)
					}

					return test.errFn(t, p, err)
				}
			}

			err = sc.Scan(ctx, []string{"."})
			if err != nil {
				t.Fatal(err)
			}

			if stats != test.result {
				t.Errorf("wrong final result, want\n  %#v\ngot:\n  %#v", test.result, stats)
			}
		})
	}
}

func TestScannerCancel(t *testing.T) {
	src := TestDir{
		"bar":   TestFile{Content: "bar"},
		"baz":   TestFile{Content: "baz"},
		"foo":   TestFile{Content: "foo"},
		"other": TestFile{Content: "other"},
	}

	result := ScanStats{Files: 2, Dirs: 0, Bytes: 6}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	TestCreateFiles(t, tempdir, src)

	back := rtest.Chdir(t, tempdir)
	defer back()

	cur, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	sc := NewScanner(fs.Track{FS: fs.NewLocal()})
	var lastStats ScanStats
	sc.Result = func(item string, s ScanStats) {
		lastStats = s

		if item == filepath.Join(cur, "baz") {
			t.Logf("found baz")
			cancel()
		}
	}

	err = sc.Scan(ctx, []string{"."})
	if err != nil {
		t.Errorf("unexpected error %v found", err)
	}

	if lastStats != result {
		t.Errorf("wrong final result, want\n  %#v\ngot:\n  %#v", result, lastStats)
	}
}

func BenchmarkScannerCWalkConcurrency(b *testing.B) {
	tempdir := b.TempDir()
	for directory := range 64 {
		name := filepath.Join(tempdir, strconv.Itoa(directory))
		if err := os.Mkdir(name, 0o700); err != nil {
			b.Fatal(err)
		}
		for file := range 32 {
			if err := os.WriteFile(filepath.Join(name, strconv.Itoa(file)), []byte("benchmark"), 0o600); err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(strconv.Itoa(workers)+"-workers", func(b *testing.B) {
			for range b.N {
				scanner := NewScanner(fs.NewLocal())
				scanner.CWalkWorkers = workers
				if err := scanner.Scan(b.Context(), []string{tempdir}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
