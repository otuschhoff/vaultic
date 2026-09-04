package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/global"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestAutomaticDeferredFallbackDistinguishesUnavailableFromCorrupt(t *testing.T) {
	opts := BackupOptions{AllowDeferredCommit: true, DeferredMode: "auto"}
	if !shouldUseDataPlaneFallback(fmt.Errorf("connect: %w", enginepkg.ErrUnavailable), opts) {
		t.Fatal("operational metadata unavailability did not select deferred fallback")
	}
	if shouldUseDataPlaneFallback(fmt.Errorf("corrupt metadata manifest"), opts) {
		t.Fatal("metadata corruption bypassed explicit acknowledgement")
	}
	opts.AcknowledgeMetadataBypass = true
	if !shouldUseDataPlaneFallback(fmt.Errorf("corrupt metadata manifest"), opts) {
		t.Fatal("acknowledged metadata corruption did not select deferred fallback")
	}
}

func TestBackupCrawlFlags(t *testing.T) {
	command := newBackupCommand(&global.Options{})
	for _, name := range []string{
		"use-cwalk", "cwalk-concurrency", "use-pathdiff", "pathdiff-endpoint",
		"pathdiff-require-coverage", "pathdiff-svm-map",
	} {
		if command.Flags().Lookup(name) == nil {
			t.Errorf("backup flag --%s is not registered", name)
		}
	}
	if value, err := command.Flags().GetInt("cwalk-concurrency"); err != nil || value != runtime.GOMAXPROCS(0) {
		t.Errorf("cwalk concurrency = %d, %v; want GOMAXPROCS %d", value, err, runtime.GOMAXPROCS(0))
	}
}

func TestBackupCrawlOptionValidation(t *testing.T) {
	globalOptions := global.Options{InsecureNoPassword: true}
	tests := []struct {
		name string
		opts BackupOptions
		want string
	}{
		{"invalid-workers", BackupOptions{UseCWalk: true}, "--cwalk-concurrency must be at least 1"},
		{"pathdiff-needs-cwalk", BackupOptions{UsePathdiff: true}, "--use-pathdiff requires --use-cwalk"},
		{"pathdiff-needs-endpoint", BackupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true}, "--use-pathdiff requires --pathdiff-endpoint"},
		{"pathdiff-needs-map", BackupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true, PathdiffEndpoint: "socket"}, "--use-pathdiff requires --pathdiff-svm-map"},
		{"coverage-needs-pathdiff", BackupOptions{PathdiffRequireCoverage: true}, "--pathdiff-require-coverage requires --use-pathdiff"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.Check(globalOptions, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Check() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateDeferred(t *testing.T) {
	tests := []struct {
		name string
		opts BackupOptions
		want string
	}{
		{"mode-requires-opt-in", BackupOptions{DeferredMode: "auto"}, "--deferred-mode requires"},
		{"mode-is-enumerated", BackupOptions{AllowDeferredCommit: true, DeferredMode: "invalid"}, "requires --deferred-mode=auto"},
		{"bypass-is-acknowledged", BackupOptions{AllowDeferredCommit: true, DeferredMode: "data-plane-only"}, "requires --acknowledge-metadata-bypass"},
		{"incompatible-parent", BackupOptions{AllowDeferredCommit: true, DeferredMode: "auto", Parent: "latest"}, "deferred ingest cannot use"},
		{"positive-expiry", BackupOptions{AllowDeferredCommit: true, DeferredMode: "auto"}, "--deferred-expiry must be positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.validateDeferred()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateDeferred() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateStdin(t *testing.T) {
	tests := []struct {
		name  string
		opts  BackupOptions
		gopts global.Options
		args  []string
		want  string
	}{
		{"password-and-data", BackupOptions{Stdin: true}, global.Options{}, nil, "cannot read both password and data"},
		{"password-and-files-from", BackupOptions{FilesFrom: []string{"-"}}, global.Options{}, nil, "unable to read password from stdin"},
		{
			"stdin-and-files-from", BackupOptions{Stdin: true, FilesFrom: []string{"list"}},
			global.Options{InsecureNoPassword: true}, nil, "--stdin and --files-from cannot",
		},
		{"stdin-and-arguments", BackupOptions{Stdin: true}, global.Options{InsecureNoPassword: true}, []string{"source"}, "files/dirs were listed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.validateStdin(test.gopts, test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateStdin() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateParent(t *testing.T) {
	opts := BackupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true, PathdiffEndpoint: "socket", PathdiffSVMMap: "map"}
	if err := opts.validateParent(); err != nil {
		t.Fatalf("validateParent() error = %v", err)
	}
	opts.PathdiffRequireCoverage = true
	if err := opts.validateParent(); err != nil {
		t.Fatalf("validateParent() with coverage error = %v", err)
	}
}

func TestBackupHooksWireCallbacks(t *testing.T) {
	var reused, failed, reconciled, before, uploaded bool
	var completed, started, blobs, excluded int
	hooks := backupHooks{
		reuseSubtree:   func(string, string, *data.Node) bool { reused = true; return true },
		errorHandler:   func(string, error) error { failed = true; return nil },
		beforeSnapshot: func() error { before = true; return nil },
		reconcileNode:  func(string, string, *data.Node) { reconciled = true },
		deferredUploader: func(_ context.Context, _ func(context.Context, vaultic.BlobSaverWithAsync) error) error {
			uploaded = true
			return nil
		},
		progress: backupProgressHooks{
			completeItem: func(string, archiver.ItemAction, archiver.ItemStats, time.Duration) { completed++ },
			startFile:    func(string) { started++ },
			completeBlob: func(uint64) { blobs++ },
			excludedItem: func(string) { excluded++ },
		},
	}
	target := &archiver.Archiver{}
	hooks.wireReuseSubtree(target)
	hooks.wireError(target)
	hooks.wireProgress(target)
	hooks.wireReconciliation(target)
	var snapshotOpts archiver.SnapshotOptions
	hooks.wireDeferredUploader(&snapshotOpts)

	if !target.ReuseSubtree("", "", nil) || target.Error("", errors.New("read")) != nil {
		t.Fatal("wired reuse or error callback returned an unexpected result")
	}
	target.CompleteItem("", archiver.ItemAction(""), archiver.ItemStats{}, 0)
	target.StartFile("")
	target.CompleteBlob(0)
	target.ExcludedItem("")
	target.ReconcileNode("", "", nil)
	if err := target.BeforeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := snapshotOpts.DeferredUploader(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !reused || !failed || !reconciled || !before || !uploaded || completed != 1 || started != 1 || blobs != 1 || excluded != 1 {
		t.Fatalf("callbacks not all invoked: reuse=%v error=%v reconcile=%v before=%v upload=%v progress=%d/%d/%d/%d",
			reused, failed, reconciled, before, uploaded, completed, started, blobs, excluded)
	}
}

func TestBackupHooksDeferredCaptureChainsReconciliation(t *testing.T) {
	dir := rtest.TempDir(t)
	source := filepath.Join(dir, "source")
	rtest.OK(t, os.WriteFile(source, []byte("data"), 0600))
	capture := reconcile.NewDeferredCapture(fs.NewLocal())
	called := false
	target := &archiver.Archiver{ReconcileNode: func(string, string, *data.Node) { called = true }}
	hooks := backupHooks{deferredCapture: capture}
	hooks.wireDeferredCapture(target)
	target.ReconcileNode("/source", source, &data.Node{Name: "source"})
	observations, err := capture.Close()
	rtest.OK(t, err)
	if !called || len(observations) != 1 {
		t.Fatalf("deferred capture did not chain: prior=%v observations=%d", called, len(observations))
	}
}

func TestCollectTargets(t *testing.T) {
	dir := rtest.TempDir(t)

	fooSpace := "foo "
	barStar := "bar*"              // Must sort before the others, below.
	if runtime.GOOS == "windows" { // Doesn't allow "*" or trailing space.
		fooSpace = "foo"
		barStar = "bar"
	}

	var expect []string
	for _, filename := range []string{
		barStar, "baz", "cmdline arg", fooSpace,
		"fromfile", "fromfile-raw", "fromfile-verbatim", "quux",
	} {
		// All mentioned files must exist for collectTargets.
		f, err := os.Create(filepath.Join(dir, filename))
		rtest.OK(t, err)
		rtest.OK(t, f.Close())

		expect = append(expect, f.Name())
	}

	f1, err := os.Create(filepath.Join(dir, "fromfile"))
	rtest.OK(t, err)
	// Empty lines should be ignored. A line starting with '#' is a comment.
	_, err = fmt.Fprintf(f1, "\n%s*\n # here's a comment\n", f1.Name())
	rtest.OK(t, err)
	rtest.OK(t, f1.Close())

	f2, err := os.Create(filepath.Join(dir, "fromfile-verbatim"))
	rtest.OK(t, err)
	for _, filename := range []string{fooSpace, barStar} {
		// Empty lines should be ignored. CR+LF is allowed.
		_, err = fmt.Fprintf(f2, "%s\r\n\n", filepath.Join(dir, filename))
		rtest.OK(t, err)
	}
	rtest.OK(t, f2.Close())

	f3, err := os.Create(filepath.Join(dir, "fromfile-raw"))
	rtest.OK(t, err)
	for _, filename := range []string{"baz", "quux"} {
		_, err = fmt.Fprintf(f3, "%s\x00", filepath.Join(dir, filename))
		rtest.OK(t, err)
	}
	rtest.OK(t, err)
	rtest.OK(t, f3.Close())

	opts := BackupOptions{
		FilesFrom:         []string{f1.Name()},
		FilesFromVerbatim: []string{f2.Name()},
		FilesFromRaw:      []string{f3.Name()},
	}

	targets, err := collectTargets(opts, []string{filepath.Join(dir, "cmdline arg")}, t.Logf, nil)
	rtest.OK(t, err)
	sort.Strings(targets)
	rtest.Equals(t, expect, targets)

	_, err = collectTargets(opts, []string{filepath.Join(dir, "cmdline arg"), filepath.Join(dir, "non-existing-file")}, t.Logf, nil)
	rtest.Assert(t, err == ErrInvalidSourceData, "expected error when not all targets exist")
}

func TestFilterExistingUnreadable(t *testing.T) {
	dir := rtest.TempDir(t)

	existing := filepath.Join(dir, "existing")
	rtest.OK(t, os.Mkdir(existing, 0755))

	file := filepath.Join(dir, "file")
	rtest.OK(t, os.WriteFile(file, []byte("x"), 0600))

	// Regression test for #5667. A target whose Lstat fails with an error other
	// than ErrNotExist must be skipped (ENOTDIR on unix, NUL byte everywhere).
	for _, unreadable := range []string{filepath.Join(file, "child"), "invalid\x00path"} {
		result, err := filterExisting([]string{unreadable}, t.Logf)
		rtest.Assert(t, errors.Is(err, ErrNoSourceData), "input %q: expected ErrNoSourceData; got %v", unreadable, err)
		rtest.Assert(t, len(result) == 0, "input %q: expected no targets; got %v", unreadable, result)

		result, err = filterExisting([]string{existing, unreadable}, t.Logf)
		rtest.Assert(t, errors.Is(err, ErrInvalidSourceData), "input %q: expected ErrInvalidSourceData; got %v", unreadable, err)
		rtest.Equals(t, []string{existing}, result)
	}
}

func TestReadFilenamesRaw(t *testing.T) {
	// These should all be returned exactly as-is.
	expected := []string{
		"\xef\xbb\xbf/utf-8-bom",
		"/absolute",
		"../.././relative",
		"\t\t leading and trailing space   \t\t",
		"newline\nin filename",
		"not UTF-8: \x80\xff/simple",
		` / *[]* \ `,
	}

	var buf bytes.Buffer
	for _, name := range expected {
		buf.WriteString(name)
		buf.WriteByte(0)
	}

	got, err := readFilenamesRaw(&buf)
	rtest.OK(t, err)
	rtest.Equals(t, expected, got)

	// Empty input is ok.
	got, err = readFilenamesRaw(strings.NewReader(""))
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(got))

	// An empty filename is an error.
	_, err = readFilenamesRaw(strings.NewReader("foo\x00\x00"))
	rtest.Assert(t, err != nil, "no error for zero byte")
	rtest.Assert(t, strings.Contains(err.Error(), "empty filename"),
		"wrong error message: %v", err.Error())

	// No trailing NUL byte is an error, because it likely means we're
	// reading a line-oriented text file (someone forgot -print0).
	_, err = readFilenamesRaw(strings.NewReader("simple.txt"))
	rtest.Assert(t, err != nil, "no error for zero byte")
	rtest.Assert(t, strings.Contains(err.Error(), "zero byte"),
		"wrong error message: %v", err.Error())
}
