package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestIndexWorkflowsImportResumeExportCheckAndRepair(t *testing.T) {
	testIndexWorkflows(t, false)
}

type readinessReadFS struct {
	fs.FS
	readBytes *atomic.Uint64
}

func (filesystem readinessReadFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	file, err := filesystem.FS.OpenFile(name, flag, metadataOnly)
	if err != nil {
		return nil, err
	}
	return readinessReadFile{File: file, readBytes: filesystem.readBytes}, nil
}

type readinessReadFile struct {
	fs.File
	readBytes *atomic.Uint64
}

func (file readinessReadFile) Read(buffer []byte) (int, error) {
	count, err := file.File.Read(buffer)
	file.readBytes.Add(uint64(count))
	return count, err
}

type readinessUnknownIdentityFS struct {
	fs.FS
}

func (filesystem readinessUnknownIdentityFS) Lstat(name string) (*fs.ExtendedFileInfo, error) {
	info, err := filesystem.FS.Lstat(name)
	if err != nil {
		return nil, err
	}
	unknown := *info
	unknown.DeviceID = 0
	unknown.Inode = 0
	return &unknown, nil
}

type readinessLegacyIdentityFS struct {
	fs.FS
	omissions *atomic.Uint64
}

func (filesystem readinessLegacyIdentityFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	file, err := filesystem.FS.OpenFile(name, flag, metadataOnly)
	if err != nil || filepath.Base(name) != "alias" {
		return file, err
	}
	return readinessLegacyIdentityFile{File: file, omissions: filesystem.omissions}, nil
}

func (filesystem readinessLegacyIdentityFS) Lstat(name string) (*fs.ExtendedFileInfo, error) {
	if filepath.Base(name) == "alias" {
		return (readinessUnknownIdentityFS{FS: filesystem.FS}).Lstat(name)
	}
	return filesystem.FS.Lstat(name)
}

type readinessLegacyIdentityFile struct {
	fs.File
	omissions *atomic.Uint64
}

func (file readinessLegacyIdentityFile) Stat() (*fs.ExtendedFileInfo, error) {
	info, err := file.File.Stat()
	if err != nil {
		return nil, err
	}
	unknown := *info
	unknown.DeviceID = 0
	unknown.Inode = 0
	return &unknown, nil
}

func (file readinessLegacyIdentityFile) ToNode(ignoreXattrListError bool, warnf func(string, ...any)) (*data.Node, error) {
	node, err := file.File.ToNode(ignoreXattrListError, warnf)
	if err != nil {
		return nil, err
	}
	node.DeviceID = 0
	node.Inode = 0
	file.omissions.Add(1)
	return node, nil
}

type readinessFailSnapshotBackend struct {
	backend.Backend
	attempts *atomic.Uint64
}

func (storage readinessFailSnapshotBackend) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	if handle.Type == backend.SnapshotFile {
		storage.attempts.Add(1)
		return errors.New("readiness fixture snapshot publication failure")
	}
	return storage.Backend.Save(ctx, handle, reader)
}

func TestIndexBackupReadinessCatalogFirstBootstrap(t *testing.T) {
	binary := os.Getenv("VAULTICDB_TEST_BINARY")
	if binary == "" {
		t.Skip("explicit private native fixture binary required")
	}
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env.globalOptions.BackendTestHook = nil
	testRunInit(t, env.globalOptions)
	if err := os.MkdirAll(filepath.Join(env.testdata, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "nested/alias"} {
		if err := os.WriteFile(filepath.Join(env.testdata, name), []byte("shared legacy bytes\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Link(filepath.Join(env.testdata, "first"), filepath.Join(env.testdata, "hardlink")); err != nil {
		t.Fatal(err)
	}
	live, err := fs.NewLocal().Lstat(filepath.Join(env.testdata, "first"))
	if err != nil || live.DeviceID == 0 || live.DeviceID > uint64(^uint32(0)) || live.Inode == 0 || live.Links != 2 {
		t.Fatalf("unsupported live fixture identity: %+v, %v", live, err)
	}
	parent, err := fs.NewLocal().Lstat(env.testdata)
	if err != nil || parent.DeviceID != live.DeviceID || parent.Inode == 0 {
		t.Fatalf("unsupported live parent identity: %+v, %v", parent, err)
	}
	t.Logf("adapter identity: device=%d inode=%d parent_inode=%d hardlinks=%d", live.DeviceID, live.Inode, parent.Inode, live.Links)
	firstMetadata, err := os.Stat(filepath.Join(env.testdata, "first"))
	if err != nil {
		t.Fatal(err)
	}
	var legacyOmissions atomic.Uint64
	testRunBackup(t, "", []string{env.testdata}, backupOptions{FSTestHook: func(filesystem fs.FS) fs.FS {
		return readinessLegacyIdentityFS{FS: filesystem, omissions: &legacyOmissions}
	}}, env.globalOptions)
	if legacyOmissions.Load() == 0 {
		t.Fatal("legacy fixture did not contain unavailable file identity")
	}
	legacy := getSnapshot(t, testListSnapshots(t, env.globalOptions, 1)[0], env)
	legacy.Time = legacy.Time.Add(time.Second)
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		_, err = data.SaveSnapshot(ctx, repo, legacy)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	historical := testListSnapshots(t, env.globalOptions, 2)
	originalDigest, originalFiles, originalBytes := m5Files(t, env.testdata, false)
	runtime, err := os.MkdirTemp("", "r0-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(runtime)
	if err := os.Chmod(runtime, 0o700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(runtime, "d.sock")
	config := daemon.Options{Socket: socket, RepositoryID: repositoryID(t, env), DaemonPath: binary,
		DataDir: filepath.Join(env.base, "readiness-db"), WALDataDir: filepath.Join(env.base, "readiness-wal"), ObjectStore: "local"}
	client, err := daemon.Ensure(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(context.Background()) }()
	store := daemon.NewSchemaStore(client)
	readPrefix := func(prefix string) []daemon.KeyValue {
		t.Helper()
		var entries []daemon.KeyValue
		var after []byte
		for {
			page, done, err := store.ScanPrefix(t.Context(), []byte(prefix), after, 100)
			if err != nil || len(entries)+len(page) > 1000 || !done && len(page) == 0 {
				t.Fatalf("fixture scan %s: entries=%d done=%v err=%v", prefix, len(entries)+len(page), done, err)
			}
			entries = append(entries, page...)
			if done {
				return entries
			}
			after = page[len(page)-1].Key
		}
	}
	options := indexImportOptions{Daemon: indexDaemonOptions{Socket: socket}, FromLegacy: true, Resume: true}
	for _, failure := range []string{"missing-catalog", "canceled", "metadata-only-activation"} {
		t.Run(failure, func(t *testing.T) {
			rejected := options
			rejected.SnapshotMetadataOnly = true
			rejected.Activate = failure == "metadata-only-activation"
			err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
				if failure == "canceled" {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = canceled
				}
				_, err := runIndexImport(ctx, rejected, globalOptions, globalOptions.Term)
				return err
			})
			if err == nil {
				t.Fatal("unsafe snapshot-reference import reported success")
			}
			if failure == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation was not propagated: %v", err)
			}
		})
	}
	if entries := readPrefix("s:"); len(entries) != 0 {
		t.Fatalf("failed imports published snapshot references: %v", entries)
	}
	for _, stage := range []string{"catalog", "snapshots"} {
		options.SnapshotMetadataOnly = stage == "snapshots"
		err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			result, err := runIndexImport(ctx, options, globalOptions, globalOptions.Term)
			if result.TreesVisited != 0 || result.NodesVisited != 0 || result.CrawlDebtCreated != 0 {
				t.Fatalf("%s traversed historical metadata: %+v", stage, result)
			}
			if stage == "snapshots" && result.SnapshotsImported != 2 {
				t.Fatalf("snapshot reference import: %+v", result)
			}
			return err
		})
		if err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
	}
	imported := make(map[string][]byte)
	for _, prefix := range []string{"b:", "p:", "s:"} {
		entries := readPrefix(prefix)
		if len(entries) == 0 {
			t.Fatalf("empty imported %s catalog", prefix)
		}
		t.Logf("imported catalog: prefix=%s records=%d", prefix, len(entries))
		for _, entry := range entries {
			imported[string(entry.Key)] = entry.Value
		}
	}
	var sharedRoot schema.ID
	for _, id := range historical {
		record, err := schema.UnmarshalSnapshotRecord(imported[string(schema.SnapshotKey(schema.ID(id)))])
		if err != nil || record.LegacyTree == (schema.ID{}) || record.RootInode != 0 || record.RootFSID != 0 {
			t.Fatalf("historical reference acquired invented identity: %+v, %v", record, err)
		}
		if sharedRoot != (schema.ID{}) && sharedRoot != record.LegacyTree {
			t.Fatal("fixture snapshots do not share their legacy tree")
		}
		sharedRoot = record.LegacyTree
	}
	for _, prefix := range []string{"iv:", "dv:", "q:"} {
		entries, _, err := store.ScanPrefix(t.Context(), []byte(prefix), nil, 10)
		if err != nil || len(entries) != 0 {
			t.Fatalf("historical graph unexpectedly materialized: prefix=%s entries=%d err=%v", prefix, len(entries), err)
		}
	}
	root, err := schema.UnmarshalBlobRecord(imported[string(schema.BlobKey(sharedRoot))])
	if err != nil || len(root.Locations) == 0 {
		t.Fatalf("missing root location: %+v, %v", root, err)
	}
	location := root.Locations[0]
	packName := vaultic.ID(location.PackID).String()
	packPath := filepath.Join(env.repo, "data", packName[:2], packName)
	packBytes, err := os.ReadFile(packPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"corrupt-tree", "missing-tree"} {
		t.Run(failure, func(t *testing.T) {
			t.Cleanup(func() {
				if err := os.WriteFile(packPath, packBytes, 0o600); err != nil {
					t.Fatal(err)
				}
			})
			if failure == "missing-tree" {
				err = os.Remove(packPath)
			} else {
				corrupt := bytes.Clone(packBytes)
				corrupt[location.Offset] ^= 0xff
				err = os.WriteFile(packPath, corrupt, 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			uncached := env.globalOptions
			uncached.NoCache = true
			if _, _, err := testRunCheckOutput(t, uncached, true); err == nil {
				t.Fatal("repository check accepted an unreadable historical tree")
			}
			if err := testRunRestoreAssumeFailure(t, historical[0].String(), restoreOptions{Target: t.TempDir(), Verify: true}, uncached); err == nil {
				t.Fatal("restore accepted an unreadable historical tree")
			}
		})
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, err := runIndexCheck(ctx, indexCheckOptions{Daemon: options.Daemon, MaxFindings: 100, Workers: 2, RPCConcurrency: 2}, globalOptions, globalOptions.Term)
		if !result.Clean() || result.HasWarnings() {
			t.Fatalf("catalog-first check: %+v", result)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	testRunCheck(t, env.globalOptions)
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	options.SnapshotMetadataOnly = false
	options.Activate = true
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, err := runIndexImport(ctx, options, globalOptions, globalOptions.Term)
		if result.TreesVisited != 0 || result.IndexesResumed == 0 {
			t.Fatalf("fixture activation retraversed history: %+v", result)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	env.globalOptions.MetadataDaemonSocket = socket
	verifyRestore := func(id vaultic.ID, digest string, files, size uint64) {
		t.Helper()
		target, err := os.MkdirTemp(env.base, "restore-")
		if err != nil {
			t.Fatal(err)
		}
		err = testRunRestoreAssumeFailure(t, id.String()+":"+toPathInSnapshot(env.testdata), restoreOptions{Target: target, Verify: true}, env.globalOptions)
		if err != nil {
			t.Fatal(err)
		}
		actualDigest, actualFiles, actualBytes := m5Files(t, target, false)
		if actualDigest != digest || actualFiles != files || actualBytes != size {
			t.Fatalf("restored content mismatch: %s %d %d; want %s %d %d", actualDigest, actualFiles, actualBytes, digest, files, size)
		}
		first, err := os.Stat(filepath.Join(target, "first"))
		if err != nil {
			t.Fatal(err)
		}
		if first.Mode().Perm() != firstMetadata.Mode().Perm() || !first.ModTime().Equal(firstMetadata.ModTime()) {
			t.Fatalf("restored file metadata mismatch: mode=%v mtime=%v", first.Mode(), first.ModTime())
		}
		linked, err := os.Stat(filepath.Join(target, "hardlink"))
		if err != nil || !os.SameFile(first, linked) {
			t.Fatal("restored hardlink identity lost", err)
		}
	}
	for _, id := range historical {
		verifyRestore(id, originalDigest, originalFiles, originalBytes)
	}
	var readBytes atomic.Uint64
	var published []vaultic.ID
	backup := func() reconcile.Metrics {
		t.Helper()
		jsonOptions := env.globalOptions
		jsonOptions.JSON = true
		readBytes.Store(0)
		backupConfig := backupOptions{GroupBy: data.SnapshotGroupByOptions{Host: true, Path: true}, FSTestHook: func(filesystem fs.FS) fs.FS {
			return readinessReadFS{FS: filesystem, readBytes: &readBytes}
		}}
		output, err := testRunBackupOutput(t, backupConfig, jsonOptions, []string{env.testdata})
		if err != nil {
			t.Fatalf("authoritative backup: %v\n%s", err, output)
		}
		var metrics reconcile.Metrics
		var observed bool
		var snapshotID vaultic.ID
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			var kind struct {
				MessageType string     `json:"message_type"`
				SnapshotID  vaultic.ID `json:"snapshot_id"`
			}
			if err := json.Unmarshal(line, &kind); err != nil {
				if len(bytes.TrimSpace(line)) != 0 {
					t.Fatal(err)
				}
				continue
			}
			if kind.MessageType == "summary" {
				snapshotID = kind.SnapshotID
			}
			if kind.MessageType == "reconciliation_stats" {
				if err := json.Unmarshal(line, &metrics); err != nil {
					t.Fatal(err)
				}
				observed = true
			}
		}
		if !observed || metrics.Failed != 0 || metrics.Deferred != 0 || metrics.Reconciled+metrics.Reused == 0 || snapshotID == (vaultic.ID{}) {
			t.Fatalf("incomplete authoritative reconciliation: %+v\n%s", metrics, output)
		}
		t.Logf("snapshot=%s scanned=%d changed=%d reused=%d reconciled=%d deferred=%d failed=%d source_read_bytes=%d", snapshotID,
			metrics.Scanned, metrics.Changed, metrics.Reused, metrics.Reconciled, metrics.Deferred, metrics.Failed, readBytes.Load())
		published = append(published, snapshotID)
		return metrics
	}
	first := backup()
	if first.Reused != 0 || first.Changed == 0 || readBytes.Load() == 0 {
		t.Fatalf("first backup trusted unverified history: %+v", first)
	}
	second := backup()
	if second.Reused == 0 || second.Changed != 0 || readBytes.Load() != 0 {
		t.Fatalf("second full crawl failed verified reuse: %+v", second)
	}
	if err := os.Rename(filepath.Join(env.testdata, "nested"), filepath.Join(env.testdata, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(env.testdata, "moved/alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(env.testdata, "moved/alias"), []byte("replacement content\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed := backup()
	if changed.Changed == 0 {
		t.Fatalf("changed full crawl missed replacement: %+v", changed)
	}
	digest, files, size := m5Files(t, env.testdata, false)
	ids := testListSnapshots(t, env.globalOptions, 5)
	expected := make(map[vaultic.ID]bool)
	for _, id := range append(append(vaultic.IDs{}, historical...), published...) {
		expected[id] = true
	}
	if len(expected) != 5 {
		t.Fatalf("expected five distinct snapshots: %v", expected)
	}
	for _, id := range ids {
		if !expected[id] {
			t.Fatalf("unexpected snapshot %s", id)
		}
	}
	graph := make(map[string][]daemon.KeyValue)
	for _, prefix := range []string{"b:", "p:", "s:", "i:", "d:", "iv:", "dv:"} {
		graph[prefix] = readPrefix(prefix)
		if len(graph[prefix]) == 0 {
			t.Fatalf("missing published graph records: %s", prefix)
		}
	}
	for key, value := range imported {
		actual, found, err := store.Get(t.Context(), []byte(key))
		if err != nil || !found || !bytes.Equal(value, actual) {
			t.Fatalf("imported catalog/reference changed: %x, found=%v err=%v", key, found, err)
		}
	}
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	client, err = daemon.Ensure(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	store = daemon.NewSchemaStore(client)
	for prefix, entries := range graph {
		if !reflect.DeepEqual(entries, readPrefix(prefix)) {
			t.Fatalf("catalog/revision graph changed across restart: %s", prefix)
		}
	}
	for _, id := range published {
		value, found, err := store.Get(t.Context(), schema.SnapshotKey(schema.ID(id)))
		if err != nil || !found {
			t.Fatalf("missing published snapshot %s: %v", id, err)
		}
		record, err := schema.UnmarshalSnapshotRecord(value)
		if err != nil || record.LegacyTree != (schema.ID{}) || record.RootRevision == 0 || record.CommitSequence == 0 {
			t.Fatalf("published snapshot lacks verified scope: %+v, %v", record, err)
		}
		rootKey := schema.DirectoryRevisionKey(record.RootFSID, record.RootInode, record.RootRevision)
		value, found, err = store.Get(t.Context(), rootKey)
		if err != nil || !found {
			t.Fatalf("published snapshot root missing after restart: %x, %v", rootKey, err)
		}
		if _, err := schema.UnmarshalDirectoryRevision(value); err != nil {
			t.Fatal(err)
		}
	}
	for _, entry := range graph["iv:"] {
		record, err := schema.UnmarshalInodeRevision(entry.Value)
		if err != nil || record.Freshness != schema.FreshnessVerified {
			t.Fatalf("live inode revision is not verified: %+v, %v", record, err)
		}
	}
	t.Run("post-backup-derived-tier-blocker", func(t *testing.T) {
		err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			result, err := runIndexCheck(ctx, indexCheckOptions{Daemon: options.Daemon, MaxFindings: 100, Workers: 2, RPCConcurrency: 2}, globalOptions, globalOptions.Term)
			if err == nil || result.Clean() || !result.Coverage.Complete || result.HasWarnings() || result.DerivedTierMismatch != 2 || len(result.Findings) != 2 {
				t.Fatalf("Phase 1 compatibility blocker changed: %+v, %v", result, err)
			}
			for _, finding := range result.Findings {
				id, parseErr := vaultic.ParseID(finding.Key)
				if parseErr != nil || finding.Kind != "derived_tier_mismatch" || finding.Want != "single" || finding.Got != "unknown" {
					t.Fatalf("unexpected metadata finding: %+v", finding)
				}
				if _, importedPack := imported[string(schema.PackKey(schema.ID(id)))]; !importedPack {
					t.Fatalf("newly published pack has a tier mismatch: %+v", finding)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	for _, id := range append(append(vaultic.IDs{}, historical...), published[:2]...) {
		verifyRestore(id, originalDigest, originalFiles, originalBytes)
	}
	verifyRestore(published[2], digest, files, size)
	testRunCheck(t, env.globalOptions)
	t.Run("backup-canceled", func(t *testing.T) {
		err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			ctx, cancel := context.WithCancel(ctx)
			cancel()
			return runBackup(ctx, backupOptions{}, globalOptions, globalOptions.Term, []string{env.testdata})
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled backup reported wrong outcome: %v", err)
		}
		testListSnapshots(t, env.globalOptions, 5)
	})
	t.Run("snapshot-publication-failure", func(t *testing.T) {
		var attempts atomic.Uint64
		failing := env.globalOptions
		failing.JSON = true
		failing.BackendTestHook = func(storage backend.Backend) (backend.Backend, error) {
			return readinessFailSnapshotBackend{Backend: storage, attempts: &attempts}, nil
		}
		output, err := testRunBackupOutput(t, backupOptions{}, failing, []string{env.testdata})
		if err == nil || attempts.Load() == 0 {
			t.Fatalf("publication failure did not propagate: attempts=%d err=%v\n%s", attempts.Load(), err, output)
		}
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			var event struct {
				SnapshotID string `json:"snapshot_id"`
			}
			if json.Unmarshal(line, &event) == nil && event.SnapshotID != "" {
				t.Fatalf("failed publication reported committed snapshot: %s", line)
			}
		}
		testListSnapshots(t, env.globalOptions, 5)
	})
	t.Run("unavailable-live-identity", func(t *testing.T) {
		before := readPrefix("i:")
		jsonOptions := env.globalOptions
		jsonOptions.JSON = true
		output, err := testRunBackupOutput(t, backupOptions{FSTestHook: func(filesystem fs.FS) fs.FS {
			return readinessUnknownIdentityFS{FS: filesystem}
		}}, jsonOptions, []string{env.testdata})
		if err == nil {
			t.Fatalf("unavailable root identity reported successful backup:\n%s", output)
		}
		var deferred uint64
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			var event struct {
				MessageType string `json:"message_type"`
				Deferred    uint64 `json:"deferred"`
				SnapshotID  string `json:"snapshot_id"`
			}
			if json.Unmarshal(line, &event) == nil {
				if event.SnapshotID != "" {
					t.Fatalf("unavailable identity acknowledged snapshot: %s", line)
				}
				if event.MessageType == "reconciliation_stats" {
					deferred = event.Deferred
				}
			}
		}
		if deferred == 0 || !reflect.DeepEqual(before, readPrefix("i:")) {
			t.Fatalf("unknown identities were not preserved as deferred: deferred=%d", deferred)
		}
		testListSnapshots(t, env.globalOptions, 5)
	})
	if stdout, stderr, err := testRunCheckOutput(t, env.globalOptions, false); err != nil {
		t.Fatalf("failed publication damaged existing snapshots: %v\n%s\n%s", err, stdout, stderr)
	}
}

func TestIndexSnapshotMetadataOnly(t *testing.T) {
	env, socket, _, client := phase34M2IndexFixture(t)
	repositoryIdentity := repositoryID(t, env)
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	options := indexImportOptions{Daemon: indexDaemonOptions{Socket: socket}, FromLegacy: true, Resume: true, Activate: true}
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, err := runIndexImport(ctx, options, globalOptions, globalOptions.Term)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	store := daemon.NewSchemaStore(client)
	profile := phase34M2Profile("legacy_import", "rpc", "scan", 0)
	controller := phase34M2IndexController(t, profile, repositoryIdentity)
	options.Daemon.ResponseDeliveryForTesting = controller.ResponseDelivery("scan")
	options.Activate = false
	options.SnapshotMetadataOnly = true
	options.SnapshotDepth = ^uint(0)
	counter := &listCountingBackend{calls: map[backend.FileType]int{}}
	env.globalOptions.BackendTestHook = func(inner backend.Backend) (backend.Backend, error) {
		counter.Backend = inner
		return counter, nil
	}
	for _, mode := range []string{"dry-run", "publish", "resume"} {
		options.DryRun = mode == "dry-run"
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			result, err := runIndexImport(ctx, options, globalOptions, globalOptions.Term)
			if err != nil {
				return err
			}
			if !result.SnapshotMetadataOnly || result.SnapshotsSeen != 1 || result.IndexesTotal != 0 || result.TreesVisited != 0 {
				t.Fatalf("%s result: %#v", mode, result)
			}
			if mode == "resume" && result.SnapshotsResumed != 1 {
				t.Fatalf("resume result: %#v", result)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		entries, _, err := store.ScanPrefix(context.Background(), []byte("s:"), nil, 10)
		if err != nil || (mode == "dry-run" && len(entries) != 0) || (mode != "dry-run" && len(entries) != 1) {
			t.Fatalf("%s snapshots=%d err=%v", mode, len(entries), err)
		}
		for _, entry := range entries {
			key, err := schema.ParseKey(entry.Key)
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := store.Get(context.Background(), schema.SnapshotImportCheckpointKey(key.ID)); err != nil || found {
				t.Fatalf("metadata-only import published traversal checkpoint: found=%t err=%v", found, err)
			}
		}
	}
	if counter.calls[backend.IndexFile] != 0 || controller.Observation().Started != 0 {
		t.Fatalf("metadata-only import scanned indexes/catalog: indexes=%d scans=%d", counter.calls[backend.IndexFile], controller.Observation().Started)
	}
	for _, mode := range []string{"reset", "activate", "rebuild", "work-budget", "snapshot-work-budget"} {
		invalid := options
		switch mode {
		case "reset":
			invalid.ForceResetOldIndex = true
		case "activate":
			invalid.Activate = true
		case "rebuild":
			invalid.Daemon.RebuildInitialize = true
		case "work-budget":
			invalid.WorkBudget = 1
		case "snapshot-work-budget":
			invalid.SnapshotWorkBudget = 1
		}
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			_, err := runIndexImport(ctx, invalid, globalOptions, globalOptions.Term)
			return err
		})
		if err == nil {
			t.Fatalf("metadata-only accepted incompatible %s", mode)
		}
	}
}

func TestIndexWorkflowsS3CompatibleMetadata(t *testing.T) {
	if os.Getenv("VAULTICDB_TEST_S3_ENDPOINT") == "" {
		t.Skip("VAULTICDB_TEST_S3_ENDPOINT is not configured")
	}
	testIndexWorkflows(t, true)
}

func TestIndexFreshBulkImportHandoffAndActivation(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil

	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socket := filepath.Join(env.base, "fresh-vaulticdb.sock")
	dataDir := filepath.Join(env.base, "fresh-vaulticdb")
	repoID := repositoryID(t, env)
	daemonOptions := indexDaemonOptions{Socket: socket, DaemonPath: daemonPath, DataDir: dataDir, Start: true}
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()

	var imported uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexImport(ctx, indexImportOptions{
			Daemon: daemonOptions, FromLegacy: true, ForceResetOldIndex: true,
			Activate: true, SnapshotDepth: 0,
		}, globalOptions, globalOptions.Term)
		imported = result.PacksImported
		return runErr
	})
	if err != nil || imported == 0 {
		t.Fatalf("fresh bulk import packs=%d err=%v", imported, err)
	}
	var stats maintenance.StatsResult
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		stats, runErr = runIndexStats(ctx, indexStatsOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil || stats.Totals.PackCount != imported {
		t.Fatalf("authoritative stats packs=%d want=%d err=%v", stats.Totals.PackCount, imported, err)
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("non-persistent stats daemon socket remains: %v", err)
	}

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: socket, RepositoryID: repoID, DaemonPath: daemonPath,
		DataDir: dataDir, WALDataDir: filepath.Join(dataDir, "wal"), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := daemon.NewSchemaStore(client)
	marker, found, err := client.Get(context.Background(), []byte("_vaultic/bulk-import-complete-v1"), "")
	if err != nil || !found || string(marker) != "complete" {
		t.Fatalf("bulk import completion marker: found=%t value=%q err=%v", found, marker, err)
	}
	packs, _, err := store.ScanPrefix(context.Background(), []byte("p:"), nil, 100)
	if err != nil || uint64(len(packs)) != imported {
		t.Fatalf("persistent-WAL catalog packs=%d want=%d err=%v", len(packs), imported, err)
	}
}

func phase34M2IndexFixture(t *testing.T) (*testEnvironment, string, string, *daemon.Client) {
	t.Helper()
	env, cleanup := withTestEnvironment(t)
	t.Cleanup(cleanup)
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil
	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socket := filepath.Join(env.base, "phase34-m2-vaulticdb.sock")
	dataDir := filepath.Join(env.base, "phase34-m2-vaulticdb")
	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: socket, RepositoryID: repositoryID(t, env), DaemonPath: daemonPath,
		DataDir: dataDir, WALDataDir: filepath.Join(dataDir, "wal"), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return env, socket, dataDir, client
}

func phase34M2IndexController(t *testing.T, profile telemetry.ExperimentProfile, targetID string) *telemetry.ExperimentController {
	t.Helper()
	profile.TargetID = targetID
	controller, err := telemetry.NewScenarioHarness().Controller(profile, telemetry.ExperimentTarget{
		ID: targetID, Disposable: true, Confirmed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func TestPhase34M2ImportCommitResponseSweep(t *testing.T) {
	artifacts := t.TempDir()
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, _ int) {
		env, socket, dataDir, client := phase34M2IndexFixture(t)
		repositoryIdentity := repositoryID(t, env)
		if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
		if err != nil {
			t.Fatal(err)
		}
		profile := phase34M2Profile("legacy_import", "rpc", "commit", delay)
		profile.TargetID = repositoryIdentity
		controller := phase34M2IndexController(t, profile, repositoryIdentity)
		defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
		started := time.Now()
		var packsImported, indexesImported uint64
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			result, runErr := runIndexImport(ctx, indexImportOptions{
				Daemon: indexDaemonOptions{
					Socket: socket, DaemonPath: daemonPath, DataDir: dataDir, Start: true,
					ResponseDeliveryForTesting: controller.ResponseDelivery("commit"),
				},
				FromLegacy: true, ForceResetOldIndex: true, Activate: true, SnapshotDepth: ^uint(0),
			}, globalOptions, globalOptions.Term)
			packsImported, indexesImported = result.PacksImported, result.IndexesImported
			return runErr
		})
		if err != nil || packsImported == 0 || indexesImported == 0 {
			t.Fatalf("import packs=%d indexes=%d err=%v", packsImported, indexesImported, err)
		}
		client, err = daemon.Ensure(context.Background(), daemon.Options{
			Socket: socket, RepositoryID: repositoryIdentity, DaemonPath: daemonPath,
			DataDir: dataDir, WALDataDir: filepath.Join(dataDir, "wal"), ObjectStore: "local",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		marker, found, err := client.Get(context.Background(), []byte("_vaultic/bulk-import-complete-v1"), "")
		if err != nil || !found || string(marker) != "complete" {
			t.Fatalf("bulk import completion marker: found=%t value=%q err=%v", found, marker, err)
		}
		packs, _, err := daemon.NewSchemaStore(client).ScanPrefix(context.Background(), []byte("p:"), nil, 100)
		if err != nil || uint64(len(packs)) != packsImported {
			t.Fatalf("imported catalog packs=%d want=%d err=%v", len(packs), packsImported, err)
		}
		observation := controller.Observation()
		if observation.Started == 0 || observation.Active != 0 {
			t.Fatalf("commit response observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), repositoryIdentity, fmt.Sprintf("packs=%d indexes=%d marker=%s", packsImported, indexesImported, marker))
	})
}

func TestPhase34M2CheckScanResponseSweep(t *testing.T) {
	artifacts := t.TempDir()
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, _ int) {
		env, socket, _, _ := phase34M2IndexFixture(t)
		repositoryIdentity := repositoryID(t, env)
		defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
		err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			_, runErr := runIndexImport(ctx, indexImportOptions{
				Daemon: indexDaemonOptions{Socket: socket}, FromLegacy: true, Resume: true, Activate: true, SnapshotDepth: ^uint(0),
			}, globalOptions, globalOptions.Term)
			return runErr
		})
		if err != nil {
			t.Fatal(err)
		}
		var baseline maintenance.CheckResult
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			var runErr error
			baseline, runErr = runIndexCheck(ctx, indexCheckOptions{
				Daemon: indexDaemonOptions{Socket: socket}, MaxFindings: 10,
			}, globalOptions, globalOptions.Term)
			return runErr
		})
		if err != nil {
			t.Fatal(err)
		}
		profile := phase34M2Profile("check", "rpc", "scan", delay)
		profile.TargetID = repositoryIdentity
		controller := phase34M2IndexController(t, profile, repositoryIdentity)
		started := time.Now()
		var result maintenance.CheckResult
		err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			var runErr error
			result, runErr = runIndexCheck(ctx, indexCheckOptions{
				Daemon:      indexDaemonOptions{Socket: socket, ResponseDeliveryForTesting: controller.ResponseDelivery("scan")},
				MaxFindings: 10,
			}, globalOptions, globalOptions.Term)
			return runErr
		})
		if err != nil {
			t.Fatalf("check result=%+v err=%v", result, err)
		}
		baseline.Consistency.SessionID = ""
		result.Consistency.SessionID = ""
		baseline.Consistency.OptionsDigest = ""
		result.Consistency.OptionsDigest = ""
		baseline.Resources.MemoryLimitBytes = 0
		result.Resources.MemoryLimitBytes = 0
		if !reflect.DeepEqual(result, baseline) {
			t.Fatalf("injected check result differs from baseline:\nbaseline=%+v\ninjected=%+v", baseline, result)
		}
		observation := controller.Observation()
		if observation.Started == 0 || observation.Active != 0 {
			t.Fatalf("scan response observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), baseline.Consistency.LegacyInventoryDigest, fmt.Sprintf("snapshots=%d locations=%d findings=%d", result.SlateDBSnapshots, result.SlateDBLocations, len(result.Findings)))
	})
}

func testIndexWorkflows(t *testing.T, s3Metadata bool) {
	t.Helper()
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	env.globalOptions.BackendTestHook = nil

	daemonPath, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socket := daemon.DefaultSocket(repositoryID(t, env))
	daemonConfig := daemon.Options{
		Socket: socket, RepositoryID: repositoryID(t, env), DaemonPath: daemonPath,
		DataDir: filepath.Join(env.base, "vaulticdb"), ObjectStore: "local",
	}
	if s3Metadata {
		daemonConfig.DataDir = ""
		daemonConfig.ObjectStore = "s3"
		daemonConfig.S3Bucket = os.Getenv("VAULTICDB_TEST_S3_BUCKET")
		if daemonConfig.S3Bucket == "" {
			daemonConfig.S3Bucket = "vaulticdb-phase7"
		}
		daemonConfig.S3Prefix = "phase7/" + filepath.Base(env.base)
	}
	client, err := daemon.Ensure(context.Background(), daemonConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := daemon.NewSchemaStore(client)
	daemonOptions := indexDaemonOptions{Socket: socket}

	var dryResultIndexes uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexImport(
			ctx,
			indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, DryRun: true, SnapshotDepth: ^uint(0)},
			globalOptions,
			globalOptions.Term,
		)
		dryResultIndexes = result.IndexesImported
		return runErr
	})
	if err != nil || dryResultIndexes == 0 {
		t.Fatalf("dry-run import indexes=%d err=%v", dryResultIndexes, err)
	}
	if entries, _, err := store.ScanPrefix(context.Background(), []byte("p:"), nil, 10); err != nil || len(entries) != 0 {
		t.Fatalf("dry-run wrote pack records: entries=%d err=%v", len(entries), err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, DryRun: true}, globalOptions, globalOptions.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("legacy-only dry-run export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 1}, globalOptions, globalOptions.Term)
		return runErr
	})
	if !errors.Is(err, errIndexDifferences) {
		t.Fatalf("legacy-only check error = %v", err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions, DryRun: true}, globalOptions, globalOptions.Term)
		if result.AggregatesChanged != 5 {
			t.Fatalf("legacy-only aggregate dry-run = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, runErr := runIndexImport(
			ctx,
			indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, SnapshotDepth: ^uint(0), SnapshotWorkBudget: 1},
			globalOptions,
			globalOptions.Term,
		)
		return runErr
	})
	if !errors.Is(err, errIndexIncomplete) {
		t.Fatalf("partial import error = %v", err)
	}
	if entries, _, scanErr := store.ScanPrefix(context.Background(), []byte("p:"), nil, 10); scanErr != nil || len(entries) != 2 {
		t.Fatalf("partial import pack catalog: entries=%d err=%v", len(entries), scanErr)
	}

	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	var resumed uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexImport(
			ctx,
			indexImportOptions{Daemon: daemonOptions, FromLegacy: true, Resume: true, Activate: true, SnapshotDepth: ^uint(0)},
			globalOptions,
			globalOptions.Term,
		)
		resumed = result.IndexesResumed
		return runErr
	})
	if err != nil || resumed == 0 {
		t.Fatalf("resumed activation indexes=%d err=%v", resumed, err)
	}
	var exported, selected, exportSequence uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, PacksPerIndex: 2, Verify: true}, globalOptions, globalOptions.Term)
		exported, selected, exportSequence = result.IndexesWritten, result.PacksSelected, result.ExportSequence
		return runErr
	})
	if err != nil || exported == 0 || selected == 0 {
		t.Fatalf("checkpointed export indexes=%d packs=%d err=%v", exported, selected, err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("resumed export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Since: exportSequence}, globalOptions, globalOptions.Term)
		if result.PacksSelected != 0 || result.IndexesWritten != 0 {
			t.Fatalf("since export = %#v", result)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	var fullExportSequence uint64
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(
			ctx, indexExportOptions{Daemon: daemonOptions, Full: true, Verify: true, PacksPerIndex: 1},
			globalOptions, globalOptions.Term,
		)
		if result.PacksSelected == 0 || result.IndexesWritten == 0 {
			t.Fatalf("full export = %#v", result)
		}
		fullExportSequence = result.ExportSequence
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexExport(ctx, indexExportOptions{Daemon: daemonOptions, Since: exportSequence, DryRun: true}, globalOptions, globalOptions.Term)
		if result.PacksSelected == 0 || result.IndexesWritten != 0 || fullExportSequence <= exportSequence {
			t.Fatalf("positive since export = %#v, full sequence=%d", result, fullExportSequence)
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}

	check := func() error {
		return withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			_, runErr := runIndexCheck(ctx, indexCheckOptions{Daemon: daemonOptions, MaxFindings: 10}, globalOptions, globalOptions.Term)
			return runErr
		})
	}
	if err := check(); err != nil {
		t.Fatalf("clean check: %v", err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexCheck(
			ctx,
			indexCheckOptions{Daemon: daemonOptions, IncludeCrawlDebt: true, FailOnWarning: true, MaxFindings: 100},
			globalOptions,
			globalOptions.Term,
		)
		if result.PendingCrawlDebt == 0 || len(result.Findings) == 0 {
			t.Fatalf("warning check = %#v", result)
		}
		return runErr
	})
	if !errors.Is(err, errIndexDifferences) {
		t.Fatalf("warning check error = %v", err)
	}
	corrupt, err := (schema.PackAggregate{PackCount: 99, UpdateSequence: 99}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), schema.PackAggregateKey(schema.AggregateAll), corrupt, true); err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, errIndexDifferences) {
		t.Fatalf("corrupt check error = %v", err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		result, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions, DryRun: true}, globalOptions, globalOptions.Term)
		if result.AggregatesChanged == 0 {
			t.Fatal("dry-run did not report aggregate drift")
		}
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, errIndexDifferences) {
		t.Fatalf("dry-run unexpectedly repaired aggregate: %v", err)
	}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		_, runErr := runIndexRebuildPackStats(ctx, indexRebuildPackStatsOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatalf("check after repair: %v", err)
	}

	assertIntrospectionAnswersWithoutListing(t, env, daemonOptions)
	snapshotIDs := testListSnapshots(t, env.globalOptions, 1)
	snapshotID := snapshotIDs[0]
	value, found, err := store.Get(context.Background(), schema.SnapshotKey(schema.ID(snapshotID)))
	if err != nil || !found {
		t.Fatalf("imported snapshot missing: %v", err)
	}
	snapshot, err := schema.UnmarshalSnapshotRecord(value)
	if err != nil || snapshot.LegacyTree == (schema.ID{}) || snapshot.CommitSequence != 0 {
		t.Fatalf("import did not preserve historical root: %#v %v", snapshot, err)
	}
	if err := os.Remove(filepath.Join(env.repo, "snapshots", snapshotID.String())); err != nil {
		t.Fatal(err)
	}
	if ids := testListSnapshots(t, env.globalOptions, 1); ids[0] != snapshotID {
		t.Fatalf("stored snapshot ID changed: %v", ids)
	}
	restored := filepath.Join(env.base, "historical-restore")
	testRunRestore(t, env.globalOptions, restored, snapshotID.String()+":"+toPathInSnapshot(filepath.Dir(env.testdata)))
	difference := directoriesContentsDiff(t, env.testdata, filepath.Join(restored, filepath.Base(env.testdata)))
	test.Assert(t, difference == "", "historical restore differs: %s", difference)
	if err := check(); !errors.Is(err, errIndexDifferences) {
		t.Fatalf("legacy inventory difference was hidden: %v", err)
	}
	if err := store.ForgetSnapshot(context.Background(), schema.ID(snapshotID)); err != nil {
		t.Fatal(err)
	}
	testListSnapshots(t, env.globalOptions, 0)
	assertCompareDetectsMissingAndExtraObjects(t, env, daemonOptions)
}

// assertCompareDetectsMissingAndExtraObjects is the Phase 11 `--compare` test:
// a backend with a deliberately removed object and a deliberately added one
// must produce both findings, reported separately, because a pack the catalog
// claims but the backend lacks is data loss while an object the backend holds
// that the catalog does not know is only waste.
func assertCompareDetectsMissingAndExtraObjects(t *testing.T, env *testEnvironment, daemonOptions indexDaemonOptions) {
	t.Helper()

	// A clean repository must compare clean, so the findings below are
	// attributable to the damage and not to a permanently noisy comparison.
	var clean BackendsResult
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		clean, runErr = runIndexBackends(ctx, indexBackendsOptions{Daemon: daemonOptions, Compare: true}, globalOptions, globalOptions.Term)
		return runErr
	})
	if err != nil {
		t.Fatalf("compare on an intact repository failed: %v", err)
	}
	if clean.MissingOnBackendNum != 0 || clean.UnknownToCatalogNum != 0 {
		t.Fatalf("compare on an intact repository reported findings: %#v", clean)
	}
	if clean.CatalogPacks == 0 {
		t.Fatal("compare found no catalog packs to compare")
	}

	// Remove one real pack from the backend and add one object the catalog has
	// never heard of.
	packDir := filepath.Join(env.repo, "data")
	var removed, extra string
	err = filepath.Walk(packDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() || removed != "" {
			return walkErr
		}
		// Only a file whose name is a pack ID is a pack; the backend layout
		// may also hold temporary files, and removing one of those would
		// damage nothing the catalog knows about.
		if _, parseErr := vaultic.ParseID(info.Name()); parseErr != nil {
			return walkErr
		}
		removed = info.Name()
		return os.Remove(path)
	})
	if err != nil || removed == "" {
		t.Fatalf("could not remove a pack object: removed=%q err=%v", removed, err)
	}
	// The extra object must be a syntactically valid ID the catalog has never
	// seen, so flipping the leading nibbles of a real one is enough.
	extra = "00" + removed[2:]
	if extra == removed {
		extra = "11" + removed[2:]
	}
	extraPath := filepath.Join(packDir, extra[:2], extra)
	if err := os.MkdirAll(filepath.Dir(extraPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(extraPath, []byte("not a pack this repository knows about"), 0o644); err != nil {
		t.Fatal(err)
	}

	var damaged BackendsResult
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		var runErr error
		damaged, runErr = runIndexBackends(ctx, indexBackendsOptions{Daemon: daemonOptions, Compare: true}, globalOptions, globalOptions.Term)
		return runErr
	})
	// A pack missing on the backend is a difference, so a non-zero exit is
	// expected and is itself part of the contract.
	if !errors.Is(err, errIndexDifferences) {
		t.Fatalf("compare on a damaged repository returned %v, want errIndexDifferences; result=%#v", err, damaged)
	}
	if damaged.MissingOnBackendNum != 1 {
		t.Errorf("missing on backend = %d, want 1: %#v", damaged.MissingOnBackendNum, damaged.MissingOnBackend)
	}
	if len(damaged.MissingOnBackend) != 1 || damaged.MissingOnBackend[0] != removed {
		t.Errorf("missing object = %#v, want [%s]", damaged.MissingOnBackend, removed)
	}
	if damaged.UnknownToCatalogNum != 1 {
		t.Errorf("unknown to catalog = %d, want 1: %#v", damaged.UnknownToCatalogNum, damaged.UnknownToCatalog)
	}
	if len(damaged.UnknownToCatalog) != 1 || damaged.UnknownToCatalog[0] != extra {
		t.Errorf("extra object = %#v, want [%s]", damaged.UnknownToCatalog, extra)
	}

	// Restore the backend so later assertions in this test see a sane state.
	if err := os.Remove(extraPath); err != nil {
		t.Fatal(err)
	}
}

// assertIntrospectionAnswersWithoutListing is the Phase 11 exit criterion: an
// operator must be able to obtain pack counts, sizes, per-backend composition,
// and a creation histogram with a growth rate for a repository whose archival
// backend is never listed.
func assertIntrospectionAnswersWithoutListing(t *testing.T, env *testEnvironment, daemonOptions indexDaemonOptions) {
	t.Helper()

	// Every listing from here on would be a violation of the criterion, so the
	// backend is wrapped to record them.
	counter := &listCountingBackend{calls: map[backend.FileType]int{}}
	previousHook := env.globalOptions.BackendTestHook
	env.globalOptions.BackendTestHook = func(inner backend.Backend) (backend.Backend, error) {
		counter.Backend = inner
		return counter, nil
	}
	defer func() { env.globalOptions.BackendTestHook = previousHook }()

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		// Pack counts and sizes, without a filter, must come from the constant-
		// time aggregates.
		stats, runErr := runIndexStats(ctx, indexStatsOptions{Daemon: daemonOptions}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if stats.Source != maintenance.SourceAggregates {
			t.Errorf("unfiltered stats scanned the catalog: %s", stats.Source)
		}
		if stats.Totals.PackCount == 0 || stats.Totals.PhysicalSize == 0 {
			t.Errorf("stats reported no packs or no bytes: %#v", stats.Totals)
		}
		if stats.SchemaVersion != maintenance.IntrospectSchemaVersion {
			t.Errorf("stats schema version = %d, want %d", stats.SchemaVersion, maintenance.IntrospectSchemaVersion)
		}

		// Composition by tier and type.
		grouped, runErr := runIndexStats(ctx, indexStatsOptions{
			Daemon: daemonOptions, GroupBy: []string{"tier", "type"},
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if len(grouped.Groups) == 0 {
			t.Errorf("grouped stats produced no composition rows: %#v", grouped)
		}

		// Individual packs, from the catalog.
		packs, runErr := runIndexPacks(ctx, indexPacksOptions{
			Daemon: daemonOptions, Sort: "size", Limit: 5,
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if packs.Matched == 0 {
			t.Errorf("pack query matched nothing: %#v", packs)
		}

		// Per-backend composition without touching the backend.
		backends, runErr := runIndexBackends(ctx, indexBackendsOptions{
			Daemon: daemonOptions, NoList: true,
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if backends.ReducedMode {
			t.Errorf("a SlateDB-authoritative repository reported reduced mode")
		}
		if len(backends.Backends) == 0 {
			t.Errorf("no backends were reported")
		}

		// A creation histogram with a growth rate. The repository is young, so
		// the series may legitimately be too short to fit a trend; what the
		// criterion requires is that the answer is produced and that a refusal
		// is explicit rather than a silently absent number.
		history, runErr := runIndexHistory(ctx, indexHistoryOptions{
			Daemon: daemonOptions, Metric: "created", Bucket: "hour",
			Histogram: true, Forecast: true, AllowIncomplete: true,
		}, globalOptions, globalOptions.Term)
		if runErr != nil {
			return runErr
		}
		if len(history.Points) == 0 {
			t.Errorf("history reported no buckets for a repository that was just written")
		}
		if history.Forecast == nil {
			t.Errorf("--forecast produced neither a projection nor a refusal")
		} else if history.Forecast.RefusedReason == "" && history.Forecast.BucketsUsed == 0 {
			t.Errorf("forecast claimed a projection from no buckets: %#v", history.Forecast)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("introspection commands failed: %v", err)
	}

	for _, fileType := range []backend.FileType{backend.PackFile, backend.IndexFile, backend.SnapshotFile} {
		if calls := counter.calls[fileType]; calls != 0 {
			t.Errorf("introspection listed %v %d times; the exit criterion forbids listing", fileType, calls)
		}
	}
}

func repositoryID(t *testing.T, env *testEnvironment) string {
	t.Helper()
	var id string
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		if err != nil {
			return err
		}
		defer unlock()
		id = repo.Config().ID
		return nil
	})
	test.OK(t, err)
	return id
}
