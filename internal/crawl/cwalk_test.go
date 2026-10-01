package crawl

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/telemetry"
)

func TestDirectoryManifestWriterCompletionClearsObservedBuffer(t *testing.T) {
	manifest, err := BuildDirectoryManifest(t.Context(), []string{t.TempDir()}, 1, 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.Close()
	metrics := telemetry.NewWorkingStateMetric(telemetry.WorkingDirectories, "pebble")
	records := make(chan directoryRecord, 1)
	records <- directoryRecord{path: filepath.Join(t.TempDir(), "synthetic"), names: []string{"entry"}}
	close(records)
	done := make(chan error)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go writeDirectoryRecords(manifest.database, records, done, cancel, metrics)
	defer func() {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		state := metrics.Snapshot(0, false)
		if state.CommittedEntries == 1 && state.ObservedBufferBytes == 0 {
			if state.PeakObservedBufferBytes == 0 {
				t.Fatal("missing batch observation")
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		runtime.Gosched()
	}
	t.Fatal("writer must clear its observed buffer before notifying completion")
}

func TestDirectoryStreamRetainedMetadataCancellation(t *testing.T) {
	stream, err := NewDirectoryStream(t.Context(), 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	name := filepath.Join(t.TempDir(), "entry")
	started := make(chan struct{})
	stream.rememberMetadata(name, fs.ReadDirEntry{Name: "entry", OpenMetadata: func() (fs.File, error) {
		close(started)
		<-stream.ctx.Done()
		return nil, stream.ctx.Err()
	}})
	result := make(chan error, 1)
	go func() {
		_, _, err := stream.OpenMetadata(name)
		result <- err
	}()
	<-started
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("retained open cancellation: %v", err)
	}
}

func TestDirectoryStreamRetainedMetadataBounds(t *testing.T) {
	stream, err := NewDirectoryStream(t.Context(), 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	var last string
	for index := 0; index < 300; index++ {
		last = filepath.Join(root, strings.Repeat("x", 40000)+strconv.Itoa(index))
		stream.rememberMetadata(last, fs.ReadDirEntry{Name: "entry", OpenMetadata: func() (fs.File, error) { return nil, nil }})
	}
	if stream.metadataBytes > 8<<20 || stream.metadata.Len() >= 300 {
		t.Fatalf("unbounded retained metadata: bytes=%d entries=%d", stream.metadataBytes, stream.metadata.Len())
	}
	if _, found, err := stream.OpenMetadata(last); err != nil || !found {
		t.Fatalf("retained entry unavailable: %t %v", found, err)
	}
	stream.Close()
	if stream.metadataBytes != 0 || stream.metadata.Len() != 0 {
		t.Fatal("close retained metadata")
	}
}

func TestDirectoryStreamDemandAndCancellation(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"first", "second", "third"} {
		if err := os.MkdirAll(filepath.Join(root, name, "nested"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := NewDirectoryStream(t.Context(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	names, found, err := stream.Names(root)
	if err != nil || !found || len(names) != 3 {
		t.Fatalf("root listing: %v, %v, %v", names, found, err)
	}
	for _, name := range names {
		children, found, err := stream.Names(filepath.Join(root, name))
		if err != nil || !found || len(children) != 1 || children[0] != "nested" {
			t.Fatalf("child listing: %v, %v, %v", children, found, err)
		}
		stream.mutex.Lock()
		pending := len(stream.pending)
		stream.mutex.Unlock()
		if pending > 1 {
			t.Fatalf("lookahead exceeded capacity: %d", pending)
		}
	}
	if _, _, err := stream.Names(filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing directory treated as empty")
	}
	stream.Close()
	if _, _, err := stream.Names(root); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestDirectoryStreamCloseJoinsDemand(t *testing.T) {
	root := t.TempDir()
	stream, err := NewDirectoryStream(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	stream.slots <- struct{}{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, err := stream.Names(root)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected canceled demand, got %v", err)
		}
	}()
	stream.Close()
	<-done
	<-stream.slots
}

func TestBuildDirectoryManifestCancellationCleansTemporaryState(t *testing.T) {
	for _, beforeStart := range []bool{true, false} {
		t.Run(map[bool]string{true: "before-start", false: "during-first-root"}[beforeStart], func(t *testing.T) {
			first, second, scratch := t.TempDir(), t.TempDir(), t.TempDir()
			roots := []string{first, t.TempDir(), t.TempDir(), t.TempDir(), second}
			for _, root := range roots {
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
			manifest, err := BuildDirectoryManifest(ctx, roots, 32, 1, func(item string, _ os.FileInfo) bool {
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

func TestDirectoryManifestWorkingState(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	var final ManifestProgress
	manifest, err := BuildDirectoryManifestWithProgress(t.Context(), []string{root}, 2, 1, nil,
		func(progress ManifestProgress) { final = progress })
	if err != nil {
		t.Fatal(err)
	}
	state := manifest.WorkingState()
	if state.CommittedEntries != 2 || state.CommittedEncodedBytes == 0 ||
		final.WorkingState.CommittedEntries != 2 || !final.Complete || state.PeakObservedBufferBytes == 0 {
		t.Fatalf("manifest=%+v progress=%+v", state, final)
	}
	if err := manifest.Close(); err != nil {
		t.Fatal(err)
	}
	if state := manifest.WorkingState(); !state.Closed || state.RetainedEntriesUpperBound != 0 {
		t.Fatalf("closed manifest: %+v", state)
	}
}

func BenchmarkPhase35M0Directories(b *testing.B) {
	root := b.TempDir()
	for ordinal := range 512 {
		if err := os.Mkdir(filepath.Join(root, strconv.Itoa(ordinal)), 0o700); err != nil {
			b.Fatal(err)
		}
	}
	before := telemetry.ReadWorkingRuntime()
	var final telemetry.WorkingStateSnapshot
	b.ResetTimer()
	for range b.N {
		manifest, err := BuildDirectoryManifest(b.Context(), []string{root}, 4, 64, nil)
		if err != nil {
			b.Fatal(err)
		}
		names, found, err := manifest.Names(root)
		if err != nil || !found || len(names) != 512 {
			b.Fatalf("directory parity: %d %v", len(names), err)
		}
		final = manifest.WorkingState()
		if final.CommittedEntries != 513 {
			b.Fatalf("directory count: %+v", final)
		}
		path := manifest.path
		if err := manifest.Close(); err != nil {
			b.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			b.Fatalf("directory cleanup: %v", err)
		}
	}
	b.StopTimer()
	encoded, err := json.Marshal(struct {
		Before telemetry.WorkingRuntimeSnapshot `json:"before"`
		After  telemetry.WorkingRuntimeSnapshot `json:"after"`
		State  telemetry.WorkingStateSnapshot   `json:"state"`
	}{before, telemetry.ReadWorkingRuntime(), final})
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("phase35_m0=%s", encoded)
}

func TestManifestRootsOverlapAndJoin(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "canceled"}[canceled], func(t *testing.T) {
			roots := []string{t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()}
			for _, root := range roots {
				if err := os.Mkdir(filepath.Join(root, "child"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			entered := make(chan struct{}, len(roots))
			release := make(chan struct{})
			done := make(chan struct{})
			var manifest *DirectoryManifest
			var err error
			go func() {
				defer close(done)
				manifest, err = BuildDirectoryManifest(ctx, roots, 4, 1, func(string, os.FileInfo) bool {
					entered <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
					}
					return false
				})
			}()
			for range roots {
				select {
				case <-entered:
				case <-ctx.Done():
					<-done
					t.Fatal("independent roots did not overlap")
				}
			}
			if canceled {
				cancel()
			}
			close(release)
			<-done
			if canceled {
				if !errors.Is(err, context.Canceled) || manifest != nil {
					t.Fatalf("expected canceled manifest, got %v, %v", manifest, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer manifest.Close()
			for _, root := range roots {
				names, found, err := manifest.Names(root)
				if err != nil || !found || len(names) != 1 || names[0] != "child" {
					t.Fatalf("incomplete root %s: %v, %v, %v", root, names, found, err)
				}
			}
		})
	}
}

func TestManifestRootFailureCancelsPeers(t *testing.T) {
	scratch := t.TempDir()
	t.Setenv("TMPDIR", scratch)
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	manifest, err := BuildDirectoryManifest(ctx, []string{root, filepath.Join(root, "missing")}, 4, 1, nil)
	if manifest != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected original root failure, got %v, %v", manifest, err)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed manifest retained temporary state: %v, %v", entries, err)
	}
}

func TestManifestProgressCompletionAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "canceled"}[canceled], func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "child"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "child", "file"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var updates []ManifestProgress
			manifest, err := BuildDirectoryManifestWithProgress(ctx, []string{root}, 4, 1,
				func(string, os.FileInfo) bool {
					if canceled {
						cancel()
					}
					return false
				}, func(status ManifestProgress) { updates = append(updates, status) })
			if canceled {
				if !errors.Is(err, context.Canceled) || manifest != nil {
					t.Fatalf("expected canceled manifest, got %v, %v", manifest, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer manifest.Close()
			}
			if len(updates) < 2 || updates[0].DirectoriesRead != 0 || updates[0].Finished {
				t.Fatalf("missing initial progress: %v", updates)
			}
			final := updates[len(updates)-1]
			if !final.Finished || final.Complete == canceled || final.RootsTotal != 1 || final.SecondsElapsed < 0 {
				t.Fatalf("incorrect completion progress: %+v", final)
			}
			if !canceled && (final.RootsCompleted != 1 || final.DirectoriesRead != 2 || final.EntriesListed != 2) {
				t.Fatalf("incorrect final counts: %+v", final)
			}
			if canceled && final.RootsCompleted != 0 {
				t.Fatalf("canceled root counted as complete: %+v", final)
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
