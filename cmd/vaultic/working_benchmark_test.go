package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/workingbench"
	"github.com/otuschhoff/vaultic/internal/workingkv"
)

func m5Root(t *testing.T) string {
	t.Helper()
	root := os.Getenv("VAULTIC_PHASE35_M5_ROOT")
	if root == "" {
		t.Skip("isolated M5 runner not selected")
	}
	marker, err := os.ReadFile(filepath.Join(root, ".nobackup"))
	if err != nil || string(marker) != "Disposable Phase 35 M5 benchmark fixture.\n" {
		t.Fatal("not an owned M5 fixture", err)
	}
	t.Setenv("VAULTIC_PASSWORD", rtest.TestPassword)
	return root
}

func m5Copy(t *testing.T, destination, source string) {
	t.Helper()
	if err := os.RemoveAll(destination); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(destination, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		return os.Chtimes(filepath.Join(destination, relative), info.ModTime(), info.ModTime())
	}); err != nil {
		t.Fatal(err)
	}
}

func m5Files(t *testing.T, root string, excludeMarkers bool) (string, uint64, uint64) {
	t.Helper()
	hash := sha256.New()
	var files, size uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if excludeMarkers && path != root {
			for _, marker := range []string{".m5-skip", ".m5-skip2"} {
				if _, err := os.Stat(filepath.Join(filepath.Dir(path), marker)); err == nil && entry.Name() != marker {
					if entry.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected nonregular M5 fixture entry")
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(hash, "%d:%s:%d:", len(relative), relative, info.Size())
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		files++
		size += uint64(info.Size())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil)), files, size
}

func m5Command(t *testing.T, options global.Options, args ...string) []workingkv.PolicyEvent {
	t.Helper()
	events, _, err := m5Execute(t, options, args...)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

type m5BackupSummary struct {
	MessageType string `json:"message_type"`
	New         uint64 `json:"files_new"`
	Changed     uint64 `json:"files_changed"`
	Unmodified  uint64 `json:"files_unmodified"`
}

func m5Execute(t *testing.T, options global.Options, args ...string) ([]workingkv.PolicyEvent, m5BackupSummary, error) {
	t.Helper()
	options.JSON = true
	output, err := withCaptureStdout(t, options, func(ctx context.Context, options global.Options) error {
		saved := options
		root := newRootCommand(&options)
		options = saved
		root.SetArgs(args)
		return root.ExecuteContext(ctx)
	})
	var events []workingkv.PolicyEvent
	var summary m5BackupSummary
	for _, line := range bytes.Split(output.Bytes(), []byte{'\n'}) {
		var event workingkv.PolicyEvent
		if json.Unmarshal(line, &event) == nil && event.Requested != "" && event.Resolution.Effective != 0 {
			events = append(events, event)
		}
		var candidate m5BackupSummary
		if json.Unmarshal(line, &candidate) == nil && candidate.MessageType == "summary" {
			summary = candidate
		}
	}
	return events, summary, err
}

func m5BackupArgs(source string) []string {
	return []string{"backup", "--host=m5-fixture", "--exclude-if-present=.m5-skip", "--exclude-if-present=.m5-skip2", source}
}

func TestM5BuildSeed(t *testing.T) {
	root := m5Root(t)
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	environment.globalOptions.BackendTestHook = nil
	source := filepath.Join(root, "input")
	if err := os.MkdirAll(filepath.Join(source, "wide"), 0700); err != nil {
		t.Fatal(err)
	}
	for ordinal := range 2048 {
		content := fmt.Appendf(make([]byte, 0, 4096), "fixture-%08d\x00\xff", ordinal)
		content = append(content, bytes.Repeat([]byte{byte(ordinal)}, 4000)...)
		if err := os.WriteFile(filepath.Join(source, "wide", fmt.Sprintf("file-%05d", ordinal)), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for ordinal, marker := range []string{".m5-skip", ".m5-skip2"} {
		directory := filepath.Join(source, fmt.Sprintf("excluded-%d", ordinal))
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{marker, "must-not-restore"} {
			if err := os.WriteFile(filepath.Join(directory, name), []byte("excluded"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	large, err := os.Create(filepath.Join(source, "large-binary"))
	if err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewSource(35))
	buffer := make([]byte, 1<<20)
	for range 32 {
		_, _ = random.Read(buffer)
		if _, err := large.Write(buffer); err != nil {
			_ = large.Close()
			t.Fatal(err)
		}
	}
	if err := large.Close(); err != nil {
		t.Fatal(err)
	}
	testRunInit(t, environment.globalOptions)
	m5Copy(t, filepath.Join(root, "seed", "empty-repo"), environment.repo)
	m5Command(t, environment.globalOptions, m5BackupArgs(source)...)
	m5Copy(t, filepath.Join(root, "seed", "repo"), environment.repo)
	m5Copy(t, filepath.Join(root, "seed", "source"), source)
	digest, files, size := m5Files(t, source, true)
	encoded, _ := json.Marshal(map[string]any{"sha256": digest, "files": files, "bytes": size})
	if err := os.WriteFile(filepath.Join(root, "seed.json"), append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestM5Workflow(t *testing.T) {
	root := m5Root(t)
	mode, scenario, limit := os.Getenv("VAULTIC_PHASE35_M5_MODE"), os.Getenv("VAULTIC_PHASE35_M5_SCENARIO"), os.Getenv("VAULTIC_PHASE35_M5_LIMIT")
	output := os.Getenv("VAULTIC_PHASE35_M5_OUTPUT")
	if mode != "pebble" && mode != "ram" && mode != "kv" && mode != "auto" {
		t.Fatal("unknown frozen mode")
	}
	if _, err := workingkv.ParseBytes(limit); err != nil {
		t.Fatal(err)
	}
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	environment.globalOptions.BackendTestHook = nil
	source := filepath.Join(root, "input")
	m5Copy(t, source, filepath.Join(root, "seed", "source"))
	seed := "repo"
	if scenario == "initial" {
		seed = "empty-repo"
	}
	m5Copy(t, environment.repo, filepath.Join(root, "seed", seed))
	if scenario == "unchanged" || scenario == "churn" || scenario == "high-live-backup" {
		m5Command(t, environment.globalOptions, m5BackupArgs(source)...)
	}
	if scenario == "churn" {
		for ordinal := range 512 {
			path := filepath.Join(source, "wide", fmt.Sprintf("file-%05d", ordinal))
			if ordinal%2 == 0 {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, bytes.Repeat([]byte("changed\x00\xff"), 1024), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(source, "new-file"), []byte("new\x00bytes\xff"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	digest, files, size := m5Files(t, source, true)
	_, prior := testRunSnapshots(t, environment.globalOptions)
	scratch := filepath.Join(environment.base, "working")
	if err := os.Mkdir(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	policyArgs := []string{}
	if mode != "pebble" {
		policyArgs = []string{"--working-memory-mode=" + mode, "--working-memory-ram-limit=" + limit,
			"--working-memory-scratch=" + scratch, "--working-memory-disk-budget=2GiB", "--working-memory-reserve=128MiB"}
	}
	var client *daemon.Client
	var daemonArgs []string
	importScenario := scenario == "import-indexes" || scenario == "import-full"
	if importScenario || scenario == "check-full" || scenario == "check-legacy" {
		binary := os.Getenv("VAULTICDB_TEST_BINARY")
		if binary == "" {
			t.Fatal("pinned private fixture daemon required")
		}
		defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
		control, err := os.MkdirTemp("/run", "v-m5-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(control) })
		socket := filepath.Join(control, "m5.sock")
		data := filepath.Join(environment.base, "daemon")
		client, err = daemon.Ensure(t.Context(), daemon.Options{Socket: socket, RepositoryID: repositoryID(t, environment),
			DaemonPath: binary, DataDir: data, WALDataDir: filepath.Join(data, "wal"), ObjectStore: "local"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := client.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		daemonArgs = []string{"--daemon-socket=" + socket}
		if !importScenario {
			m5Command(t, environment.globalOptions, append([]string{"index", "import", "--from-legacy", "--resume", "--snapshot-depth=0"}, daemonArgs...)...)
			m5Command(t, environment.globalOptions, append([]string{"index", "import", "--snapshot-metadata-only", "--resume"}, daemonArgs...)...)
		}
	}
	var live [][]byte
	var liveBytes uint64
	if scenario == "high-live-backup" {
		for remaining := 1_000_000_000; remaining > 0; {
			buffer := make([]byte, min(remaining, 8<<20))
			for index := 0; index < len(buffer); index += 4096 {
				buffer[index] = 1
			}
			remaining -= len(buffer)
			liveBytes += uint64(len(buffer))
			live = append(live, buffer)
		}
	}
	probe, err := workingbench.StartWorkingProbe(scratch, output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = probe.Finish("incomplete", nil) })
	started := time.Now()
	var churnBytes atomic.Uint64
	churnDone := make(chan struct{})
	if scenario == "high-live-backup" {
		go func() {
			defer close(churnDone)
			retained := make([][]byte, 16)
			for ordinal := range 768 {
				buffer := make([]byte, 4<<20)
				for index := 0; index < len(buffer); index += 4096 {
					buffer[index] = byte(ordinal)
				}
				retained[ordinal%len(retained)] = buffer
				churnBytes.Add(uint64(len(buffer)))
			}
			runtime.KeepAlive(retained)
		}()
	} else {
		close(churnDone)
	}
	var events []workingkv.PolicyEvent
	var backupSummary m5BackupSummary
	var timedErr error
	switch scenario {
	case "initial", "unchanged", "churn", "high-live-backup":
		args := m5BackupArgs(source)
		events, backupSummary, timedErr = m5Execute(t, environment.globalOptions, append(args[:len(args)-1], append(policyArgs, source)...)...)
	case "import-indexes", "import-full":
		args := append([]string{"index", "import", "--from-legacy", "--resume"}, daemonArgs...)
		if scenario == "import-indexes" {
			args = append(args, "--snapshot-depth=0")
		}
		events, _, timedErr = m5Execute(t, environment.globalOptions, append(args, policyArgs...)...)
	case "check-full", "check-legacy":
		args := append([]string{"index", "check", "--max-findings=100"}, daemonArgs...)
		if scenario == "check-legacy" {
			args = append(args, "--legacy-only")
		}
		events, _, timedErr = m5Execute(t, environment.globalOptions, append(args, policyArgs...)...)
	default:
		t.Fatal("unknown frozen scenario")
	}
	if timedErr != nil && !errors.Is(timedErr, workingkv.ErrWorkingMemoryLimitExceeded) {
		t.Fatal("unclassified command failure", timedErr)
	}
	if err := probe.Latency(time.Since(started)); err != nil {
		t.Fatal(err)
	}
	<-churnDone
	_, err = probe.Finish(digest, map[string]any{"events": events, "mode": mode, "scenario": scenario,
		"configured_limit": limit, "input_files": files, "input_bytes": size,
		"ordinary_live_bytes": liveBytes, "concurrent_allocated_bytes": churnBytes.Load(),
		"completed": timedErr == nil, "typed_exhaustion": timedErr != nil, "backup_summary": backupSummary})
	runtime.KeepAlive(live)
	if err != nil {
		t.Fatal(err)
	}
	if scenario == "unchanged" && timedErr == nil && (backupSummary.New != 0 || backupSummary.Changed != 0 || backupSummary.Unmodified != files) {
		t.Fatalf("unchanged workflow was not actually unchanged: %+v files=%d", backupSummary, files)
	}
	var traversalDigest string
	if scenario == "import-full" && timedErr == nil {
		traversalDigest = m5TraversalDigest(t, client, source, files)
	}
	_, after := testRunSnapshots(t, environment.globalOptions)
	for id := range prior {
		if _, found := after[id]; !found {
			t.Fatal("prior snapshot lost")
		}
	}
	if client == nil && timedErr != nil {
		if len(after) != len(prior) {
			t.Fatal("failed backup published a snapshot")
		}
	} else if client == nil {
		if len(after) != len(prior)+1 {
			t.Fatal("backup did not publish exactly one new snapshot")
		}
		restored := filepath.Join(environment.base, "restored")
		testRunRestore(t, environment.globalOptions, restored, "latest")
		restoredSource := filepath.Join(restored, strings.TrimPrefix(source, "/"))
		actual, actualFiles, actualSize := m5Files(t, restoredSource, false)
		if actual != digest || actualFiles != files || actualSize != size {
			t.Fatal("restored bytes, names or exclusion decisions differ")
		}
		for ordinal := range 2 {
			if _, err := os.Stat(filepath.Join(restoredSource, fmt.Sprintf("excluded-%d", ordinal), "must-not-restore")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("excluded directory payload was restored", err)
			}
		}
	} else {
		if len(after) != len(prior) {
			t.Fatal("index workflow mutated snapshots")
		}
		if scenario == "import-indexes" && timedErr == nil {
			m5Command(t, environment.globalOptions, append([]string{"index", "import", "--snapshot-metadata-only", "--resume"}, daemonArgs...)...)
		}
		var result maintenance.CheckResult
		if importScenario && timedErr != nil {
			_ = m5CatalogDigest(t, client)
		} else {
			err := withTermStatus(t, environment.globalOptions, func(ctx context.Context, options global.Options) error {
				var err error
				result, err = runIndexCheck(ctx, indexCheckOptions{Daemon: indexDaemonOptions{Socket: daemonArgs[0][len("--daemon-socket="):]}, MaxFindings: 100}, options, options.Term)
				return err
			})
			if err != nil || !result.Clean() || result.LegacyLocations == 0 || result.LegacyLocations != result.SlateDBLocations {
				t.Fatalf("exact native comparison failed: error=%v clean=%t legacy=%d native=%d findings=%v", err, result.Clean(), result.LegacyLocations, result.SlateDBLocations, result.Findings)
			}
		}
		if importScenario && timedErr == nil {
			before := m5CatalogDigest(t, client)
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			data := filepath.Join(environment.base, "daemon")
			client, err = daemon.Ensure(t.Context(), daemon.Options{Socket: daemonArgs[0][len("--daemon-socket="):],
				RepositoryID: repositoryID(t, environment), DaemonPath: os.Getenv("VAULTICDB_TEST_BINARY"),
				DataDir: data, WALDataDir: filepath.Join(data, "wal"), ObjectStore: "local"})
			if err != nil {
				t.Fatal(err)
			}
			args := append([]string{"index", "import", "--from-legacy", "--resume"}, daemonArgs...)
			if scenario == "import-indexes" {
				args = append(args, "--snapshot-depth=0")
			}
			m5Command(t, environment.globalOptions, args...)
			if m5CatalogDigest(t, client) != before {
				t.Fatal("resume/reopen changed exact catalog entries")
			}
			if scenario == "import-full" && m5TraversalDigest(t, client, source, files) != traversalDigest {
				t.Fatal("resume/reopen changed full traversal entries")
			}
		}
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatal("working scratch leaked", err)
	}
	semantic := map[string]any{"complete": true, "restore_verified": client == nil && timedErr == nil,
		"exact_check_verified": client != nil && !(importScenario && timedErr != nil),
		"completed_workload":   timedErr == nil, "typed_exhaustion": timedErr != nil, "exhaustion_verified": timedErr != nil,
		"input_sha256": digest, "files": files, "bytes": size, "prior_snapshots": len(prior), "snapshots": len(after), "scratch_clean": true}
	if client != nil {
		semantic["timed_import_scope"] = "legacy-indexes; snapshot JSON setup/verification outside interval; tree traversal excluded"
		if scenario == "import-full" {
			semantic["timed_import_scope"] = "full legacy indexes, snapshots, trees and paths"
			semantic["traversal_verified"] = timedErr == nil
			semantic["traversal_sha256"] = traversalDigest
		}
	}
	encoded, _ := json.MarshalIndent(semantic, "", "  ")
	if err := os.WriteFile(filepath.Join(output, "semantic.json"), append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func m5CatalogDigest(t *testing.T, client *daemon.Client) string {
	t.Helper()
	hash := sha256.New()
	store := daemon.NewSchemaStore(client)
	for _, prefix := range []string{"b:", "p:", "s:"} {
		var after []byte
		for {
			entries, done, err := store.ScanPrefix(t.Context(), []byte(prefix), after, 256)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				_, _ = fmt.Fprintf(hash, "%d:", len(entry.Key))
				_, _ = hash.Write(entry.Key)
				_, _ = fmt.Fprintf(hash, "%d:", len(entry.Value))
				_, _ = hash.Write(entry.Value)
				after = entry.Key
			}
			if done {
				break
			}
			if len(entries) == 0 {
				t.Fatal("nonprogressing verification scan")
			}
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func m5TraversalDigest(t *testing.T, client *daemon.Client, source string, files uint64) string {
	t.Helper()
	hash := sha256.New()
	store := daemon.NewSchemaStore(client)
	var after []byte
	var inodes uint64
	paths := make(map[string]struct{})
	for {
		entries, done, err := store.ScanPrefix(t.Context(), nil, after, 256)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			after = entry.Key
			parsed, err := schema.ParseKey(entry.Key)
			if err != nil {
				t.Fatal("malformed full traversal key", err)
			}
			switch parsed.Kind {
			case schema.KeyCurrentInode, schema.KeyInodeRevision, schema.KeyCurrentDirectory, schema.KeyDirectoryRevision, schema.KeyPathVersion:
				_, _ = fmt.Fprintf(hash, "%d:", len(entry.Key))
				_, _ = hash.Write(entry.Key)
				_, _ = fmt.Fprintf(hash, "%d:", len(entry.Value))
				_, _ = hash.Write(entry.Value)
			}
			if parsed.Kind == schema.KeyCurrentInode {
				inodes++
			}
			if parsed.Kind == schema.KeyDirectoryRevision {
				directory, err := schema.UnmarshalDirectoryRevision(entry.Value)
				if err != nil {
					t.Fatal(err)
				}
				for _, child := range directory.Children {
					if child.Type != schema.NodeFile {
						continue
					}
					path := filepath.Join("/", directory.SourcePath, child.Name)
					relative, err := filepath.Rel(source, path)
					if err != nil || !filepath.IsLocal(relative) {
						t.Fatal("imported file outside fixture", path, err)
					}
					for _, marker := range []string{".m5-skip", ".m5-skip2"} {
						if _, err := os.Stat(filepath.Join(filepath.Dir(path), marker)); err == nil && child.Name != marker {
							t.Fatal("excluded path imported", path)
						}
					}
					info, err := os.Stat(path)
					if err != nil || !info.Mode().IsRegular() {
						t.Fatal("imported path not a source file", path, err)
					}
					value, found, err := store.Get(t.Context(), child.MetadataKey)
					if err != nil || !found {
						t.Fatal("missing child inode revision", path, err)
					}
					inode, err := schema.UnmarshalInodeRevision(value)
					if err != nil || inode.Size != uint64(info.Size()) {
						t.Fatal("imported path metadata differs", path, err)
					}
					paths[path] = struct{}{}
				}
			}
		}
		if done {
			break
		}
		if len(entries) == 0 {
			t.Fatal("nonprogressing full traversal scan")
		}
	}
	if inodes != files || uint64(len(paths)) != files {
		t.Fatalf("partial traversal: inodes=%d paths=%d expected files=%d", inodes, len(paths), files)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
