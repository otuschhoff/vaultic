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
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/cmd/vaultic/backupcmd"
	"github.com/otuschhoff/vaultic/internal/backend/all"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/options"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
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

func validatePublicationQualificationRoot(root, authorized string) error {
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return fmt.Errorf("noncanonical qualification root: %w", err)
	}
	relative, err := filepath.Rel(authorized, root)
	if err != nil || relative == "." || !filepath.IsLocal(relative) {
		return fmt.Errorf("qualification root outside private authorization")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return fmt.Errorf("qualification root must be a private directory")
	}
	marker, err := os.ReadFile(filepath.Join(root, ".nobackup"))
	if err != nil || string(marker) != "Disposable Phase 35 throughput qualification.\n" {
		return fmt.Errorf("qualification ownership marker missing")
	}
	return nil
}

func TestPublicationQualificationRootGuard(t *testing.T) {
	authorized := t.TempDir()
	root := filepath.Join(authorized, "qualification")
	rtest.OK(t, os.Mkdir(root, 0700))
	if err := validatePublicationQualificationRoot(root, authorized); err == nil {
		t.Fatal("unmarked root accepted")
	}
	rtest.OK(t, os.WriteFile(filepath.Join(root, ".nobackup"), []byte("Disposable Phase 35 throughput qualification.\n"), 0600))
	rtest.OK(t, validatePublicationQualificationRoot(root, authorized))
	for _, candidate := range []string{authorized, t.TempDir()} {
		if err := validatePublicationQualificationRoot(candidate, authorized); err == nil {
			t.Fatalf("unsafe root accepted: %s", candidate)
		}
	}
	alias := filepath.Join(authorized, "alias")
	rtest.OK(t, os.Symlink(root, alias))
	if err := validatePublicationQualificationRoot(alias, authorized); err == nil {
		t.Fatal("symlink root accepted")
	}
	rtest.OK(t, os.Chmod(root, 0755))
	if err := validatePublicationQualificationRoot(root, authorized); err == nil {
		t.Fatal("insecure root accepted")
	}
}

func publicationQualificationOptions(repoPath, socket, passphrase string) global.Options {
	return global.Options{Repo: repoPath, PasswordFile: "/volume2/NASDA2/rustic/etc/repo-password.txt", JSON: true, NoCache: true,
		Backends: all.Backends(), Extended: make(options.Options),
		MetadataDaemonSocket: socket, MetadataEncryptionMode: "required", MetadataPassphraseFile: passphrase}
}

func TestPublicationQualificationBackendInitialization(t *testing.T) {
	options := publicationQualificationOptions(t.TempDir(), "", "")
	options.PasswordFile = ""
	options.InsecureNoPassword = true
	err := withTermStatus(t, options, func(ctx context.Context, options global.Options) error {
		repo, err := global.OpenRepository(ctx, options, progress.NewTerminalPrinter(true, 0, options.Term))
		if err == nil {
			return repo.Close()
		}
		return err
	})
	if err == nil {
		t.Fatal("empty fixture unexpectedly opened as a repository")
	}
}

func publicationQualificationLoadPassword(options *global.Options) error {
	password, err := global.LoadPasswordFromFile(options.PasswordFile)
	if err != nil {
		return err
	}
	if password == "" {
		return fmt.Errorf("qualification password file is empty")
	}
	options.Password = password
	return nil
}

func TestPublicationQualificationPasswordFile(t *testing.T) {
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, environment.globalOptions)
	passwordFile := filepath.Join(t.TempDir(), "password")
	rtest.OK(t, os.WriteFile(passwordFile, []byte(rtest.TestPassword+"\n"), 0600))
	options := publicationQualificationOptions(environment.repo, "", "")
	options.PasswordFile = passwordFile
	rtest.OK(t, publicationQualificationLoadPassword(&options))
	rtest.OK(t, withTermStatus(t, options, func(ctx context.Context, options global.Options) error {
		repo, err := global.OpenRepository(ctx, options, progress.NewTerminalPrinter(true, 0, options.Term))
		if err != nil {
			return err
		}
		return repo.Close()
	}))
	rtest.OK(t, os.WriteFile(passwordFile, nil, 0600))
	if err := publicationQualificationLoadPassword(&options); err == nil {
		t.Fatal("empty qualification password accepted")
	}
}

func TestPublicationQualificationIndexedRead(t *testing.T) {
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, environment)
	testRunBackup(t, "", []string{environment.testdata}, backupOptions{}, environment.globalOptions)
	ids := testListSnapshots(t, environment.globalOptions, 1)
	snapshot := testLoadSnapshot(t, environment.globalOptions, ids[0])
	rtest.OK(t, withTermStatus(t, environment.globalOptions, func(ctx context.Context, options global.Options) error {
		printer := progress.NewTerminalPrinter(true, 0, options.Term)
		repo, err := global.OpenRepository(ctx, options, printer)
		if err != nil {
			return err
		}
		defer repo.Close()
		if err := repo.LoadIndex(ctx, printer); err != nil {
			return err
		}
		_, err = repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: *snapshot.Tree}, nil)
		return err
	}))
}

func publicationQualificationLookup(ctx context.Context, options daemon.Options, directory string) (*metadataindex.DaemonEngine, error) {
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return nil, err
	}
	client, err := daemon.Connect(ctx, options)
	if err != nil {
		return nil, err
	}
	lookup, err := metadataindex.NewBackupDaemonEngine(ctx, client, metadataindex.BackupLookupOptions{
		ScratchDirectory: directory, CacheBytes: 16 << 20, Concurrency: 4,
	})
	if err != nil {
		return nil, errors.Join(err, client.Close(context.Background()))
	}
	return lookup, nil
}

func TestPublicationQualificationNativeLookup(t *testing.T) {
	binary := testVaulticDBBinary(t)
	passphrase := filepath.Join(t.TempDir(), "passphrase")
	rtest.OK(t, os.WriteFile(passphrase, []byte("test recovery passphrase\n"), 0600))
	options := daemon.Options{
		Socket: testMetadataSocket(t), RepositoryID: strings.Repeat("a", 64), DaemonPath: binary,
		DataDir: t.TempDir(), EncryptionMode: "initialize", PassphraseFile: passphrase,
	}
	client, err := daemon.Ensure(t.Context(), options)
	rtest.OK(t, err)
	defer func() {
		if client != nil {
			rtest.OK(t, client.Close(context.Background()))
		}
	}()
	lookup, err := publicationQualificationLookup(t.Context(), options, filepath.Join(t.TempDir(), "lookup"))
	rtest.OK(t, err)
	defer lookup.Close()
	_, found, err := lookup.LookupSizeContext(t.Context(), vaultic.NewRandomBlobHandle())
	rtest.OK(t, err)
	if found {
		t.Fatal("empty private catalog unexpectedly contains a blob")
	}
	rtest.OK(t, lookup.Close())
	_, err = client.WriterStatus(t.Context())
	rtest.OK(t, err)
	previous := client
	rtest.OK(t, publicationQualificationReopen(t.Context(), &client, options))
	if client == nil || client == previous {
		t.Fatal("encrypted reopen did not transfer ownership to the new client")
	}
	_, err = client.WriterStatus(t.Context())
	rtest.OK(t, err)
}

func publicationQualificationReopen(ctx context.Context, client **daemon.Client, options daemon.Options) error {
	if err := (*client).Close(context.Background()); err != nil {
		return err
	}
	*client = nil
	reopened, err := daemon.Ensure(ctx, options)
	*client = reopened
	return err
}

func TestPublicationQualificationOnDemandBackup(t *testing.T) {
	testPublicationReadCandidate(t, "", nil, 0, false)
}

func TestMetadataReadCandidatesNative(t *testing.T) {
	const candidate = "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaultic"
	if os.Getenv("VAULTIC_METADATA_READ_QUALIFY") != "1" {
		t.Skip("isolated metadata-read candidate qualification is opt-in")
	}
	for _, capacity := range []uint64{128 << 20, 256 << 20, 512 << 20, 1024 << 20} {
		t.Run(fmt.Sprintf("meta-cache-%d", capacity>>20), func(t *testing.T) {
			testPublicationReadCandidate(t, candidate, []string{"--unchanged-read-concurrency=8"}, capacity, false)
		})
	}
	for _, limit := range []int{4, 8, 16} {
		t.Run(fmt.Sprintf("coalesced-rpc-%d", limit), func(t *testing.T) {
			testPublicationReadCandidate(t, candidate, []string{"--unchanged-read-concurrency=16", "--metadata-batch-delay=1ms", fmt.Sprintf("--metadata-read-concurrency=%d", limit)}, 128<<20, true)
		})
	}
	t.Run("exact-catalog", func(t *testing.T) {
		testPublicationReadCandidate(t, candidate, []string{"--unchanged-read-concurrency=8", "--metadata-exact-catalog"}, 128<<20, false)
	})
}

func testPublicationReadCandidate(t *testing.T, candidate string, extraFlags []string, metaCacheBytes uint64, multiGet bool) {
	t.Helper()
	binary := testVaulticDBBinary(t)
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, environment)
	rtest.OK(t, os.Symlink(".", filepath.Join(environment.testdata, "qualification-symlink")))
	environment.globalOptions.BackendTestHook = nil
	rtest.OK(t, withTermStatus(t, environment.globalOptions, func(ctx context.Context, options global.Options) error {
		printer := progress.NewTerminalPrinter(true, 0, options.Term)
		repo, err := global.OpenRepository(ctx, options, printer)
		if err != nil {
			return err
		}
		repositoryID := repo.Config().ID
		if err := errors.Join(metadataindex.Activate(ctx, repo.Backend(), repositoryID), repo.Close()); err != nil {
			return err
		}
		passphrase := filepath.Join(t.TempDir(), "passphrase")
		rtest.OK(t, os.WriteFile(passphrase, []byte("test recovery passphrase\n"), 0600))
		daemonOptions := daemon.Options{Socket: testMetadataSocket(t), RepositoryID: repositoryID,
			DaemonPath: binary, DataDir: t.TempDir(), EncryptionMode: "initialize", PassphraseFile: passphrase,
			MetaCacheBytes: metaCacheBytes, BlockCacheBytes: 512 << 20, EngineMultiGet: multiGet}
		client, err := daemon.Ensure(ctx, daemonOptions)
		if err != nil {
			return err
		}
		defer client.Close(context.Background())
		options.MetadataDaemonSocket = daemonOptions.Socket
		options.MetadataEncryptionMode = "required"
		options.MetadataPassphraseFile = passphrase
		scratch := t.TempDir()
		rtest.OK(t, os.Chmod(scratch, 0700))
		password := filepath.Join(t.TempDir(), "password")
		rtest.OK(t, os.WriteFile(password, []byte(options.Password+"\n"), 0600))
		before, err := client.WriterStatus(ctx)
		if err != nil {
			return err
		}
		preflight := t.TempDir()
		publicationQualificationCommand(t, preflight, "preflight", "/root/proj/vaultic/bin/phase35-critical-path/linux-amd64/vaultic",
			"--repo="+options.Repo, "--password-file="+password, "--metadata-daemon-socket="+daemonOptions.Socket,
			"--metadata-encryption-mode=required", "--metadata-passphrase-file="+passphrase, "--no-cache", "--json",
			"backup", "--dry-run", "--force", "--no-scan", "--one-file-system", environment.testdata)
		paths, err := boundedLiveSourcePaths(environment.testdata, 10_000)
		if err != nil {
			return err
		}
		regularFiles := publicationQualificationRegularFiles(t, environment.testdata, paths)
		output, err := os.ReadFile(filepath.Join(preflight, "preflight.stdout.log"))
		if err != nil {
			return err
		}
		matched := false
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			var event struct {
				MessageType string `json:"message_type"`
				Files       uint64 `json:"total_files_processed"`
			}
			if json.Unmarshal(line, &event) == nil && event.MessageType == "summary" {
				if event.Files != regularFiles {
					return fmt.Errorf("native preflight regular-file count differs: processed=%d selected=%d", event.Files, regularFiles)
				}
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("native preflight omitted summary")
		}
		after, err := client.WriterStatus(ctx)
		if err != nil {
			return err
		}
		if before.LastDurableSequence != after.LastDurableSequence || after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
			return fmt.Errorf("native count preflight changed durable state")
		}
		publicationQualificationCommand(t, t.TempDir(), "backup", "/root/proj/vaultic/bin/phase35-throughput/linux-amd64/vaultic",
			"--repo="+options.Repo, "--password-file="+password, "--metadata-daemon-socket="+daemonOptions.Socket,
			"--metadata-encryption-mode=required", "--metadata-passphrase-file="+passphrase, "--no-cache", "--json",
			"backup", "--metadata-publication-group-size=64", "--metadata-on-demand", "--metadata-cache-mib=64",
			"--working-memory-mode=ram", "--working-memory-ram-limit=160GiB", "--working-memory-scratch="+scratch,
			"--working-memory-disk-budget=2GiB", environment.testdata)
		ids := testListSnapshots(t, options, 1)
		if err := testRunRestoreAssumeFailure(t, ids[0].String(), restoreOptions{Target: t.TempDir(), Verify: true}, options); err != nil {
			return err
		}
		unchanged := t.TempDir()
		if candidate == "" {
			candidate = "/root/proj/vaultic/bin/phase35-critical-path-paired-reads/linux-amd64/vaultic"
		}
		args := []string{
			"--repo=" + options.Repo, "--password-file=" + password, "--metadata-daemon-socket=" + daemonOptions.Socket,
			"--metadata-encryption-mode=required", "--metadata-passphrase-file=" + passphrase, "--no-cache", "--json",
			"backup", "--no-scan", "--one-file-system", "--metadata-publication-group-size=64", "--metadata-on-demand", "--metadata-cache-mib=64",
			"--working-memory-mode=ram", "--working-memory-ram-limit=160GiB", "--working-memory-scratch=" + scratch,
			"--working-memory-disk-budget=2GiB"}
		args = append(args, extraFlags...)
		args = append(args, environment.testdata)
		publicationQualificationCommand(t, unchanged, "unchanged", candidate, args...)
		output, err = os.ReadFile(filepath.Join(unchanged, "unchanged.stdout.log"))
		if err != nil {
			return err
		}
		var summary m5BackupSummary
		var main struct {
			MessageType    string  `json:"message_type"`
			Phase          string  `json:"phase"`
			SecondsElapsed float64 `json:"seconds_elapsed"`
			Reconciliation struct {
				Observed uint64 `json:"observed"`
				Scanned  uint64 `json:"scanned"`
				Failed   uint64 `json:"failed"`
				Deferred uint64 `json:"deferred"`
			} `json:"reconciliation"`
		}
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			var kind struct {
				MessageType string `json:"message_type"`
			}
			if json.Unmarshal(line, &kind) != nil {
				continue
			}
			switch kind.MessageType {
			case "summary":
				rtest.OK(t, json.Unmarshal(line, &summary))
			case "backup_critical_path":
				rtest.OK(t, json.Unmarshal(line, &main))
			}
		}
		if summary.Unmodified != regularFiles || summary.TotalFilesProcessed != regularFiles || summary.New != 0 || summary.Changed != 0 || summary.DataBlobs != 0 {
			return fmt.Errorf("unchanged native crawl changed or transferred file data: %+v", summary)
		}
		if main.Phase != "main_backup" || main.SecondsElapsed <= 0 || main.Reconciliation.Observed != uint64(len(paths)) ||
			main.Reconciliation.Scanned != uint64(len(paths)) || main.Reconciliation.Failed != 0 || main.Reconciliation.Deferred != 0 {
			return fmt.Errorf("unchanged native crawl lacks complete main interval: %+v", main)
		}
		all := testListSnapshots(t, options, 2)
		for _, id := range all {
			if id != ids[0] {
				snapshot := testLoadSnapshot(t, options, id)
				if snapshot.Parent == nil || *snapshot.Parent != ids[0] || snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != "full" || snapshot.CrawlPlan.ReusedSubtrees != 0 {
					return fmt.Errorf("unchanged native backup did not use a full crawl and the prior parent")
				}
				restored := t.TempDir()
				if err := testRunRestoreAssumeFailure(t, id.String(), restoreOptions{Target: restored, Verify: true}, options); err != nil {
					return err
				}
				selected := make(map[string]bool, len(paths))
				for _, path := range paths {
					selected[path] = true
				}
				expectedHash, expectedFiles, expectedBytes := m5FilesSelected(t, environment.testdata, false, selected)
				actualHash, actualFiles, actualBytes := m5FilesSelected(t, filepath.Join(restored, strings.TrimPrefix(environment.testdata, "/")), false, selected)
				if expectedHash != actualHash || expectedFiles != actualFiles || expectedBytes != actualBytes {
					return fmt.Errorf("unchanged snapshot restore differs from source")
				}
			}
		}
		t.Logf("unchanged_native_full_crawl files=%d observed=%d data_blobs=%d metadata_and_pack_bytes=%d main_seconds=%.6f files_per_second=%.3f archive_seconds=%.6f",
			regularFiles, main.Reconciliation.Observed, summary.DataBlobs, summary.DataAdded, main.SecondsElapsed, float64(regularFiles)/main.SecondsElapsed, summary.TotalDuration)
		status, err := client.WriterStatus(ctx)
		if err != nil {
			return err
		}
		if status.ActiveTransactions != 0 || status.ActiveWriteIntents != 0 || (metaCacheBytes != 0 && status.EngineMetaCacheBytes != metaCacheBytes) {
			return fmt.Errorf("candidate left active work or ignored cache sizing")
		}
		if candidate == "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaultic" && status.Attribution.EncryptionReads == nil {
			return fmt.Errorf("authenticated native transport omitted encryption read attribution")
		}
		return nil
	}))
}

func publicationQualificationCopy(t *testing.T, target, source string) {
	t.Helper()
	info, err := os.Lstat(source)
	rtest.OK(t, err)
	if !info.Mode().IsRegular() {
		t.Fatal("nonregular qualification input")
	}
	input, err := os.Open(source)
	rtest.OK(t, err)
	defer input.Close()
	output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	rtest.OK(t, err)
	rtest.OK(t, output.Chmod(0600))
	_, copyErr := io.Copy(output, input)
	rtest.OK(t, errors.Join(copyErr, output.Close()))
	after, err := input.Stat()
	rtest.OK(t, err)
	if info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
		t.Fatal("qualification input changed while being copied")
	}
	rtest.OK(t, os.Chtimes(target, info.ModTime(), info.ModTime()))
}

func publicationQualificationSample(t *testing.T, target string) (string, uint64, uint64) {
	t.Helper()
	const source = "/ncl1-1-vs-20/fme_ba_dest"
	rtest.OK(t, os.Mkdir(target, 0700))
	rtest.OK(t, os.Chmod(target, 0700))
	var files, size uint64
	visited := 0
	deadline := time.Now().Add(5 * time.Minute)
	var walk func(string, int)
	walk = func(directory string, depth int) {
		if depth > 6 || files >= 512 || visited >= 20000 || time.Now().After(deadline) {
			return
		}
		input, err := os.Open(directory)
		rtest.OK(t, err)
		defer input.Close()
		for files < 512 && visited < 20000 && time.Now().Before(deadline) {
			entries, err := input.ReadDir(128)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			for _, entry := range entries {
				visited++
				name := entry.Name()
				if name == ".snapshot" || name == "lost+found" || strings.HasPrefix(name, ".Trash-") || entry.Type()&os.ModeSymlink != 0 {
					continue
				}
				path := filepath.Join(directory, name)
				if entry.IsDir() {
					if _, err := os.Stat(filepath.Join(path, ".nobackup")); err == nil {
						continue
					}
					if _, err := os.Stat(filepath.Join(path, "CACHEDIR.TAG")); err == nil {
						continue
					}
					walk(path, depth+1)
					continue
				}
				info, err := entry.Info()
				rtest.OK(t, err)
				if !info.Mode().IsRegular() || info.Size() > 8<<20 || size+uint64(info.Size()) > 512<<20 || files >= 512 {
					continue
				}
				publicationQualificationCopy(t, filepath.Join(target, fmt.Sprintf("sample-%05d", files)), path)
				files++
				size += uint64(info.Size())
			}
			if errors.Is(err, io.EOF) {
				break
			}
		}
	}
	walk(source, 0)
	if files < 128 {
		t.Fatalf("insufficient bounded source sample: files=%d visited=%d", files, visited)
	}
	digest, actualFiles, actualBytes := m5Files(t, target, false)
	if files != actualFiles || size != actualBytes {
		t.Fatal("frozen qualification sample differs")
	}
	t.Logf("frozen_sample files=%d bytes=%d visited=%d sha256=%s", files, size, visited, digest)
	return digest, files, size
}

func publicationQualificationCommand(t *testing.T, directory, name, binary string, args ...string) (time.Duration, float64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Hour)
	defer cancel()
	stdout, err := os.OpenFile(filepath.Join(directory, name+".stdout.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	rtest.OK(t, err)
	defer stdout.Close()
	rtest.OK(t, stdout.Chmod(0600))
	stderr, err := os.OpenFile(filepath.Join(directory, name+".stderr.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	rtest.OK(t, err)
	defer stderr.Close()
	rtest.OK(t, stderr.Chmod(0600))
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = append(os.Environ(), "VAULTIC_FEATURES=slatedb-authoritative=true")
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = 5 * time.Second
	started := time.Now()
	if err := command.Run(); err != nil {
		t.Fatalf("qualification stage %s failed: %v; inspect private logs at %s", name, err, directory)
	}
	return time.Since(started), (command.ProcessState.UserTime() + command.ProcessState.SystemTime()).Seconds()
}

func TestPublicationQualificationTreeComparison(t *testing.T) {
	root := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT")
	if root == "" {
		t.Skip("private tree comparison not selected")
	}
	rtest.OK(t, validatePublicationQualificationRoot(root, "/volume2/NASDA2/vaultic-test-m6"))
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	var baseline map[string]json.RawMessage
	for _, groupSize := range []int{4, 64} {
		cell := filepath.Join(root, fmt.Sprintf("group-%d", groupSize))
		control, err := os.MkdirTemp("/run", "v-ab-compare-")
		rtest.OK(t, err)
		defer os.RemoveAll(control)
		options := publicationQualificationOptions(filepath.Join(cell, "repo"), filepath.Join(control, "db.sock"), "/volume2/NASDA2/rustic/etc/metadata-recovery")
		rtest.OK(t, publicationQualificationLoadPassword(&options))
		database := filepath.Join(cell, "db")
		daemonOptions := daemon.Options{Socket: options.MetadataDaemonSocket,
			RepositoryID: "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528",
			DaemonPath:   "/root/proj/vaultic/bin/profile/linux-amd64/vaulticdb", DataDir: database,
			WALDataDir: filepath.Join(database, "wal"), ObjectStore: "local", WALStore: "local",
			WALFlushInterval: 100 * time.Millisecond, MetaCacheBytes: 128 << 20, BlockCacheBytes: 512 << 20,
			EncryptionMode: "required", PassphraseFile: options.MetadataPassphraseFile}
		client, err := daemon.Ensure(t.Context(), daemonOptions)
		rtest.OK(t, err)
		func() {
			defer func() { rtest.OK(t, client.Close(context.Background())) }()
			output, err := os.ReadFile(filepath.Join(cell, "backup.stdout.log"))
			rtest.OK(t, err)
			var snapshotID vaultic.ID
			for _, line := range bytes.Split(output, []byte{'\n'}) {
				var event struct {
					SnapshotID string `json:"snapshot_id"`
				}
				if json.Unmarshal(line, &event) == nil && event.SnapshotID != "" {
					snapshotID, err = vaultic.ParseID(event.SnapshotID)
					rtest.OK(t, err)
				}
			}
			snapshot := testLoadSnapshot(t, options, snapshotID)
			nodes := make(map[string]json.RawMessage)
			rtest.OK(t, withTermStatus(t, options, func(ctx context.Context, options global.Options) error {
				printer := progress.NewTerminalPrinter(true, 0, options.Term)
				repo, err := global.OpenRepository(ctx, options, printer)
				if err != nil {
					return err
				}
				defer repo.Close()
				lookup, err := publicationQualificationLookup(ctx, daemonOptions, filepath.Join(control, "lookup"))
				if err != nil {
					return err
				}
				repo.SetEngine(lookup)
				if err := repo.LoadIndex(ctx, printer); err != nil {
					return err
				}
				var visit func(vaultic.ID, string) error
				visit = func(treeID vaultic.ID, parent string) error {
					encoded, err := repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: treeID}, nil)
					if err != nil {
						return err
					}
					var tree struct {
						Nodes []*data.Node `json:"nodes"`
					}
					if err := json.Unmarshal(encoded, &tree); err != nil {
						return err
					}
					for _, node := range tree.Nodes {
						path := filepath.Join(parent, node.Name)
						if node.Subtree != nil {
							if err := visit(*node.Subtree, path); err != nil {
								return err
							}
							t.Logf("group=%d directory=%s subtree=%s", groupSize, path, node.Subtree)
						}
						node.Subtree = nil
						encoded, err := json.Marshal(node)
						if err != nil {
							return err
						}
						nodes[path] = encoded
					}
					return nil
				}
				return visit(*snapshot.Tree, "/")
			}))
			if baseline == nil {
				baseline = nodes
				return
			}
			for path, node := range nodes {
				if !bytes.Equal(baseline[path], node) {
					t.Logf("tree_difference path=%s baseline=%s candidate=%s", path, baseline[path], node)
					var previous, current data.Node
					rtest.OK(t, json.Unmarshal(baseline[path], &previous))
					rtest.OK(t, json.Unmarshal(node, &current))
					source := filepath.Join(root, "input")
					relative, err := filepath.Rel(path, source)
					if err != nil || relative == "." || !filepath.IsLocal(relative) || current.Type != data.NodeTypeDir {
						t.Fatal("snapshot differs within selected source or outside ancestor metadata")
					}
					previous.ModTime, previous.AccessTime, previous.ChangeTime = time.Time{}, time.Time{}, time.Time{}
					current.ModTime, current.AccessTime, current.ChangeTime = time.Time{}, time.Time{}, time.Time{}
					previousJSON, err := json.Marshal(previous)
					rtest.OK(t, err)
					currentJSON, err := json.Marshal(current)
					rtest.OK(t, err)
					if !bytes.Equal(previousJSON, currentJSON) {
						t.Fatal("ancestor differs beyond timestamps")
					}
				}
				delete(baseline, path)
			}
			if len(baseline) != 0 {
				t.Fatal("candidate omitted baseline nodes")
			}
		}()
	}
}

func publicationQualificationPendingPacks(t *testing.T, client *daemon.Client) []vaultic.ID {
	t.Helper()
	session, err := daemon.NewSchemaStore(client).BeginReadSession(t.Context())
	rtest.OK(t, err)
	defer func() { rtest.OK(t, session.Close(context.Background())) }()
	var pending []vaultic.ID
	_, available, err := session.ScanPackInventory(t.Context(), func(_ context.Context, id schema.ID, _ schema.PackRecord) error {
		if len(pending) >= 32 {
			return fmt.Errorf("private pending-pack scope exceeds 32 packs")
		}
		pending = append(pending, vaultic.ID(id))
		return nil
	})
	rtest.OK(t, err)
	if !available {
		t.Fatal("historical clone does not have complete pack sizing metadata")
	}
	return pending
}

func TestPublicationQualificationPendingInventory(t *testing.T) {
	root := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT")
	if root == "" {
		t.Skip("private pending inventory not selected")
	}
	rtest.OK(t, validatePublicationQualificationRoot(root, "/volume2/NASDA2/vaultic-test-m6"))
	control, err := os.MkdirTemp("/run", "v-ab-inventory-")
	rtest.OK(t, err)
	defer os.RemoveAll(control)
	database := filepath.Join(root, "group-64", "db")
	client, err := daemon.Ensure(t.Context(), daemon.Options{Socket: filepath.Join(control, "db.sock"),
		RepositoryID: "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528",
		DaemonPath:   "/root/proj/vaultic/bin/profile/linux-amd64/vaulticdb", DataDir: database, WALDataDir: filepath.Join(database, "wal"),
		ObjectStore: "local", WALStore: "local", WALFlushInterval: 100 * time.Millisecond, MetaCacheBytes: 128 << 20, BlockCacheBytes: 512 << 20,
		EncryptionMode: "required", PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery"})
	rtest.OK(t, err)
	defer func() { rtest.OK(t, client.Close(context.Background())) }()
	var size uint64
	for _, pack := range publicationQualificationPendingPacks(t, client) {
		id := pack.String()
		info, err := os.Stat(filepath.Join("/volume2/NASDA2/rustic/repo/data", id[:2], id))
		rtest.OK(t, err)
		size += uint64(info.Size())
		if size > 16<<30 {
			t.Fatal("pending pack bytes exceed private copy budget")
		}
		t.Logf("pending_pack=%s bytes=%d", id, info.Size())
	}
	t.Logf("pending_pack_bytes=%d", size)
}

func boundedLiveSourcePaths(source string, limit uint64) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != source && entry.IsDir() && entry.Name() == ".snapshot" {
			return filepath.SkipDir
		}
		if uint64(len(paths)) == limit {
			return filepath.SkipAll
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		paths = append(paths, relative)
		return nil
	})
	return paths, err
}

func TestBoundedLiveSourcePaths(t *testing.T) {
	source := t.TempDir()
	rtest.OK(t, os.Mkdir(filepath.Join(source, ".snapshot"), 0700))
	rtest.OK(t, os.WriteFile(filepath.Join(source, ".snapshot", "snapshot-file"), nil, 0600))
	rtest.OK(t, os.Mkdir(filepath.Join(source, "live"), 0700))
	rtest.OK(t, os.WriteFile(filepath.Join(source, "live", "file"), nil, 0600))
	paths, err := boundedLiveSourcePaths(source, 3)
	rtest.OK(t, err)
	if strings.Join(paths, ",") != ".,live,live/file" {
		t.Fatalf("snapshot view consumed the live inode cap: %v", paths)
	}
}

func publicationQualificationRegularFiles(t *testing.T, source string, paths []string) uint64 {
	t.Helper()
	var count uint64
	for _, path := range paths {
		info, err := os.Lstat(filepath.Join(source, path))
		rtest.OK(t, err)
		if info.Mode().IsRegular() {
			count++
		}
	}
	return count
}

func TestPrepareFmeCAE1Qualification(t *testing.T) {
	root := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT")
	if root == "" {
		t.Skip("bounded source qualification not selected")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(root))
	rtest.OK(t, err)
	if parent != "/volume2/NASDA2/vaultic-test-m6" || filepath.Clean(root) != root {
		t.Fatal("bounded source evidence must be a fresh private child")
	}
	rtest.OK(t, os.Mkdir(root, 0700))
	rtest.OK(t, os.Chmod(root, 0700))
	rtest.OK(t, os.WriteFile(filepath.Join(root, ".nobackup"), []byte("Disposable Phase 35 throughput qualification.\n"), 0600))
	rtest.OK(t, os.Chmod(filepath.Join(root, ".nobackup"), 0600))
	manifest := backupcmd.BoundedInodeManifest{Version: 1, Source: "/ncl1-1-vs-50/fme_cae1", InodeLimit: 100_000}
	manifest.Paths, err = boundedLiveSourcePaths(manifest.Source, manifest.InodeLimit)
	rtest.OK(t, err)
	if uint64(len(manifest.Paths)) != manifest.InodeLimit {
		t.Fatal("source does not contain the requested number of live inodes")
	}
	selected := make(map[string]bool, len(manifest.Paths))
	for _, path := range manifest.Paths {
		selected[path] = true
	}
	digest, files, size := m5FilesSelected(t, manifest.Source, false, selected)
	regularFiles := publicationQualificationRegularFiles(t, manifest.Source, manifest.Paths)
	if files == 0 || size > 64<<30 {
		t.Fatal("selected source is empty or exceeds the 64 GiB logical-byte safety ceiling")
	}
	for name, value := range map[string]any{
		"inode-manifest.json":     manifest,
		"source-observation.json": map[string]any{"sha256": digest, "files": files, "regular_files": regularFiles, "bytes": size, "entries": len(manifest.Paths)},
	} {
		encoded, err := json.MarshalIndent(value, "", "  ")
		rtest.OK(t, err)
		rtest.OK(t, os.WriteFile(filepath.Join(root, name), append(encoded, '\n'), 0600))
		rtest.OK(t, os.Chmod(filepath.Join(root, name), 0600))
	}
	t.Logf("source=%s entries=%d cap=%d non_directory_entries=%d regular_files=%d bytes=%d sha256=%s", manifest.Source, len(manifest.Paths), manifest.InodeLimit, files, regularFiles, size, digest)
}

func TestBoundedSourceRoundTrip(t *testing.T) {
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, environment)
	source := filepath.Join(t.TempDir(), "source")
	rtest.OK(t, os.MkdirAll(filepath.Join(source, "dir"), 0700))
	rtest.OK(t, os.WriteFile(filepath.Join(source, "dir", "keep"), []byte("selected content"), 0600))
	rtest.OK(t, os.Link(filepath.Join(source, "dir", "keep"), filepath.Join(source, "dir", "alias")))
	rtest.OK(t, os.WriteFile(filepath.Join(source, "dir", "omit"), []byte("unselected"), 0600))
	rtest.OK(t, os.Symlink("dir/keep", filepath.Join(source, "link")))
	manifest := backupcmd.BoundedInodeManifest{Version: 1, Source: source, InodeLimit: 5, Paths: []string{".", "dir", "dir/keep", "dir/alias", "link"}}
	encoded, err := json.Marshal(manifest)
	rtest.OK(t, err)
	filename := filepath.Join(t.TempDir(), "manifest.json")
	rtest.OK(t, os.WriteFile(filename, encoded, 0600))
	testRunBackup(t, "", []string{source}, backupOptions{BoundedInodeManifest: filename, Force: true, UseCWalk: true, CWalkConcurrency: 4}, environment.globalOptions)
	ids := testListSnapshots(t, environment.globalOptions, 1)
	snapshot := testLoadSnapshot(t, environment.globalOptions, ids[0])
	if snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != "bounded" {
		t.Fatal("capped snapshot claims complete source coverage")
	}
	restored := t.TempDir()
	rtest.OK(t, testRunRestoreAssumeFailure(t, ids[0].String(), restoreOptions{Target: restored, Verify: true}, environment.globalOptions))
	restoredSource := filepath.Join(restored, strings.TrimPrefix(source, "/"))
	selected := make(map[string]bool)
	for _, path := range manifest.Paths {
		selected[path] = true
	}
	expected, files, size := m5FilesSelected(t, source, false, selected)
	if files != 3 || publicationQualificationRegularFiles(t, source, manifest.Paths) != 2 {
		t.Fatal("regular-file summary scope must exclude the separately verified symlink")
	}
	actual, actualFiles, actualSize := m5FilesSelected(t, restoredSource, false, selected)
	if expected != actual || files != actualFiles || size != actualSize {
		t.Fatal("bounded restored content or symlink differs")
	}
	if _, err := os.Lstat(filepath.Join(restoredSource, "dir", "omit")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unselected source entry was restored")
	}
}

type unchangedCrawlMeasurement struct {
	SecondsElapsed float64 `json:"seconds_elapsed"`
	Reconciliation struct {
		Observed    uint64 `json:"observed"`
		Scanned     uint64 `json:"scanned"`
		Failed      uint64 `json:"failed"`
		Deferred    uint64 `json:"deferred"`
		WriterPhase string `json:"writer_phase"`
	} `json:"reconciliation"`
}

func validateUnchangedCrawlMeasurement(summary m5BackupSummary, measurement unchangedCrawlMeasurement, files, entries uint64) error {
	if measurement.SecondsElapsed < 600 || measurement.Reconciliation.WriterPhase != "done" {
		return fmt.Errorf("unchanged crawl needs at least 600 main-phase seconds and completed reconciliation")
	}
	if summary.MessageType != "summary" || summary.Unmodified != files || summary.TotalFilesProcessed != files ||
		summary.New != 0 || summary.Changed != 0 || summary.DataBlobs != 0 {
		return fmt.Errorf("crawl was not a complete unchanged-file workload: %+v", summary)
	}
	if measurement.Reconciliation.Observed != entries || measurement.Reconciliation.Scanned != entries ||
		measurement.Reconciliation.Failed != 0 || measurement.Reconciliation.Deferred != 0 {
		return fmt.Errorf("crawl reconciliation differs from sealed source")
	}
	return nil
}

func validateForceComparisonMeasurement(summary m5BackupSummary, measurement unchangedCrawlMeasurement, files, entries uint64, forced bool) error {
	if measurement.SecondsElapsed <= 0 || measurement.Reconciliation.WriterPhase != "done" ||
		measurement.Reconciliation.Observed != entries || measurement.Reconciliation.Scanned != entries ||
		measurement.Reconciliation.Failed != 0 || measurement.Reconciliation.Deferred != 0 {
		return fmt.Errorf("force comparison requires a completed exact-scope crawl")
	}
	if summary.MessageType != "summary" || summary.TotalFilesProcessed != files || summary.DataBlobs != 0 || summary.Changed != 0 {
		return fmt.Errorf("force comparison changed content or stored new data: %+v", summary)
	}
	if (!forced && (summary.Unmodified != files || summary.New != 0)) ||
		(forced && (summary.Unmodified != 0 || summary.New != files)) {
		return fmt.Errorf("force comparison did not exercise the requested reuse mode")
	}
	return nil
}

func TestForceComparisonMeasurement(t *testing.T) {
	for _, entries := range []uint64{10000, 50000} {
		t.Run(fmt.Sprint(entries), func(t *testing.T) { testForceComparisonMeasurement(t, entries) })
	}
}

func testForceComparisonMeasurement(t *testing.T, entries uint64) {
	t.Helper()
	files := entries * 9 / 10
	measurement := unchangedCrawlMeasurement{SecondsElapsed: 1}
	measurement.Reconciliation.Observed, measurement.Reconciliation.Scanned = entries, entries
	measurement.Reconciliation.WriterPhase = "done"
	for _, forced := range []bool{false, true} {
		t.Run(fmt.Sprint(forced), func(t *testing.T) {
			summary := m5BackupSummary{MessageType: "summary", TotalFilesProcessed: files}
			if forced {
				summary.New = files
			} else {
				summary.Unmodified = files
			}
			rtest.OK(t, validateForceComparisonMeasurement(summary, measurement, files, entries, forced))
			summary.DataAdded = 1449
			rtest.OK(t, validateForceComparisonMeasurement(summary, measurement, files, entries, forced))
			for _, failure := range []string{"new-data", "scope", "mode", "unfinished"} {
				candidate, observed := summary, measurement
				switch failure {
				case "new-data":
					candidate.DataBlobs = 1
				case "scope":
					observed.Reconciliation.Scanned--
				case "mode":
					candidate.New, candidate.Unmodified = candidate.Unmodified, candidate.New
				case "unfinished":
					observed.Reconciliation.WriterPhase = "wait_prepared"
				}
				if validateForceComparisonMeasurement(candidate, observed, files, entries, forced) == nil {
					t.Fatalf("accepted %s", failure)
				}
			}
		})
	}
}

func TestUnchangedCrawlMeasurement(t *testing.T) {
	summary := m5BackupSummary{MessageType: "summary", Unmodified: 100, TotalFilesProcessed: 100}
	measurement := unchangedCrawlMeasurement{SecondsElapsed: 600}
	measurement.Reconciliation.Observed, measurement.Reconciliation.Scanned = 110, 110
	measurement.Reconciliation.WriterPhase = "done"
	rtest.OK(t, validateUnchangedCrawlMeasurement(summary, measurement, 100, 110))
	for _, name := range []string{"short", "unfinished", "changed", "data", "missing", "failed", "deferred"} {
		t.Run(name, func(t *testing.T) {
			candidateSummary, candidateMeasurement := summary, measurement
			switch name {
			case "short":
				candidateMeasurement.SecondsElapsed = 599.999
			case "unfinished":
				candidateMeasurement.Reconciliation.WriterPhase = "publish_batch"
			case "changed":
				candidateSummary.Changed = 1
			case "data":
				candidateSummary.DataBlobs = 1
			case "missing":
				candidateMeasurement.Reconciliation.Scanned--
			case "failed":
				candidateMeasurement.Reconciliation.Failed = 1
			case "deferred":
				candidateMeasurement.Reconciliation.Deferred = 1
			}
			if validateUnchangedCrawlMeasurement(candidateSummary, candidateMeasurement, 100, 110) == nil {
				t.Fatal("invalid unchanged-crawl observation accepted")
			}
		})
	}
}

func publicationForceInodeLimit(value string) (int, error) {
	switch value {
	case "":
		return 10000, nil
	case "50000":
		return 50000, nil
	default:
		return 0, fmt.Errorf("unsupported force-comparison inode limit %q", value)
	}
}

func TestPublicationForceInodeLimit(t *testing.T) {
	for _, value := range []string{"", "50000", "10000", "500000", "-1"} {
		limit, err := publicationForceInodeLimit(value)
		if value == "" || value == "50000" {
			want := 10000
			if value == "50000" {
				want = 50000
			}
			if err != nil || limit != want {
				t.Fatalf("limit=%d err=%v for %q", limit, err, value)
			}
		} else if err == nil {
			t.Fatalf("unapproved inode count accepted: %q", value)
		}
	}
}

func TestPublicationBackupQualification(t *testing.T) {
	root := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT")
	if root == "" {
		t.Skip("private encrypted persistent-database qualification not selected")
	}
	rtest.OK(t, validatePublicationQualificationRoot(root, "/volume2/NASDA2/vaultic-test-m6"))
	resumeVerification := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_RESUME_VERIFY")
	if resumeVerification != "" && resumeVerification != "group-4" && resumeVerification != "group-64" {
		t.Fatal("unsupported verification resume selector")
	}
	onDemand := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ON_DEMAND")
	if onDemand != "" && onDemand != "1" {
		t.Fatal("invalid on-demand qualification selector")
	}
	unchanged := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_UNCHANGED")
	forceCompare := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE")
	expanded := os.Getenv("VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT")
	forceCompareLimit, err := publicationForceInodeLimit(expanded)
	rtest.OK(t, err)
	var metadataReadFlags []string
	if expanded != "" {
		if forceCompare != "1" || os.Getenv("VAULTIC_PHASE35_METADATA_READ_COMPARE") != "coalesced" || root != "/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-50k-coalesced" {
			t.Fatal("expanded scope requires the fresh 50k coalesced capture")
		}
		metadataReadFlags = []string{"--unchanged-read-concurrency=16", "--metadata-read-concurrency=8", "--metadata-batch-delay=1ms"}
	}
	if forceCompare != "" && (forceCompare != "1" || unchanged != "1") {
		t.Fatal("force comparison requires the fresh unchanged qualification mode")
	}
	if unchanged != "" && (unchanged != "1" || onDemand != "1" || resumeVerification != "" ||
		os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ORDER") != "64" || os.Getenv("VAULTIC_PHASE35_THROUGHPUT_SOURCE") != "" ||
		os.Getenv("VAULTIC_PHASE35_THROUGHPUT_INPUT") != "" || os.Getenv("VAULTIC_PHASE35_THROUGHPUT_PAIRED_READS") != "") {
		t.Fatal("unchanged qualification requires one fresh on-demand group-64 cell without bounded selection")
	}
	if resumeVerification != "" && onDemand != "" {
		t.Fatal("verification resume cannot change backup lookup mode")
	}
	const seed = "/ncl1-1-vs-50/fme_dump/amakura/db.begin-readiness-20261001-fuU56R"
	const original = "/volume2/NASDA2/rustic/repo"
	const repositoryID = "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528"
	candidate := "/root/proj/vaultic/bin/phase35-throughput/linux-amd64/vaultic"
	if selected := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_CANDIDATE"); selected != "" {
		if selected != "/root/proj/vaultic/bin/phase35-critical-path/linux-amd64/vaultic" &&
			selected != "/root/proj/vaultic/bin/phase35-critical-path-fill/linux-amd64/vaultic" &&
			selected != "/root/proj/vaultic/bin/phase35-critical-path-paired-reads/linux-amd64/vaultic" &&
			!(forceCompare == "1" && selected == "/root/proj/vaultic/bin/phase35-force-10k/linux-amd64/vaultic") &&
			!(expanded == "50000" && selected == "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaultic") {
			t.Fatal("candidate is not the separate critical-path build")
		}
		encoded, err := os.ReadFile(selected)
		rtest.OK(t, err)
		if fmt.Sprintf("%x", sha256.Sum256(encoded)) != os.Getenv("VAULTIC_PHASE35_THROUGHPUT_CANDIDATE_SHA256") {
			t.Fatal("critical-path candidate digest mismatch")
		}
		candidate = selected
	}
	binary := "/root/proj/vaultic/bin/profile/linux-amd64/vaulticdb"
	if selected := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_NATIVE"); selected != "" {
		if forceCompare != "1" || (selected != "/root/proj/vaultic/bin/phase35-force-10k/linux-amd64/vaulticdb" &&
			!(expanded == "50000" && selected == "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaulticdb")) {
			t.Fatal("native override requires the private force-comparison build")
		}
		encoded, err := os.ReadFile(selected)
		rtest.OK(t, err)
		if fmt.Sprintf("%x", sha256.Sum256(encoded)) != os.Getenv("VAULTIC_PHASE35_THROUGHPUT_NATIVE_SHA256") {
			t.Fatal("native comparison digest mismatch")
		}
		binary = selected
	}
	if expanded != "" && (candidate != "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaultic" || binary != "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaulticdb") {
		t.Fatal("expanded scope requires the measurement-pinned metadata-read binaries")
	}
	var marker struct {
		Clone                                                  string
		ChecksumVerified, WALRelocated, ProductionWALUnchanged bool
		Snapshots                                              []string
	}
	encoded, err := os.ReadFile("/volume2/NASDA2/rustic/db.test/begin-readiness-deploy-20261001-fuU56R/clone.validated.json")
	rtest.OK(t, err)
	rtest.OK(t, json.Unmarshal(encoded, &marker))
	if marker.Clone != seed || !marker.ChecksumVerified || !marker.WALRelocated || !marker.ProductionWALUnchanged || len(marker.Snapshots) != 143 {
		t.Fatal("encrypted seed validation evidence differs")
	}
	repositoryHash := fmt.Sprintf("%x", sha256.Sum256([]byte(repositoryID)))
	bindingRelative := filepath.Join(repositoryHash, "_vaultic", "wal-target-v1")
	binding, err := os.ReadFile(filepath.Join(seed, bindingRelative))
	rtest.OK(t, err)
	if string(binding) != "vaulticdb-local-wal-after-bulk-import-v1:"+seed+"/wal" {
		t.Fatal("seed WAL binding is not private")
	}
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	source := filepath.Join(root, "input")
	var digest string
	var files, regularFiles, size uint64
	var sourceManifest backupcmd.BoundedInodeManifest
	var frozenPaths map[string]bool
	if unchanged != "" {
		const accepted = "/volume2/NASDA2/vaultic-test-m6/critical-path-fme_cae1-20261102-production-cap100k/group-64"
		var result struct {
			RestoreVerified bool   `json:"restore_verified"`
			ReopenVerified  bool   `json:"reopen_verified"`
			InputSHA256     string `json:"input_sha256"`
			Files           uint64 `json:"files"`
			Bytes           uint64 `json:"bytes"`
		}
		encoded, err := os.ReadFile(filepath.Join(accepted, "result.json"))
		rtest.OK(t, err)
		rtest.OK(t, json.Unmarshal(encoded, &result))
		if !result.RestoreVerified || !result.ReopenVerified || result.Files != 89326 || result.Bytes != 20949531016 ||
			result.InputSHA256 != "9f85dfbe8114ebfd17481f3c1453d2fb1333c73580a8aa56415dbbd9cd010862" {
			t.Fatal("unchanged source lacks accepted representative restore evidence")
		}
		rtest.OK(t, os.Mkdir(source, 0700))
		rtest.OK(t, os.Chmod(source, 0700))
		t.Log("copying accepted restored source into a fresh stable NFS fixture")
		acceptedSource := filepath.Join(accepted, "restore/ncl1-1-vs-50/fme_cae1")
		if forceCompare == "1" {
			selected, err := boundedLiveSourcePaths(acceptedSource, uint64(forceCompareLimit))
			rtest.OK(t, err)
			if len(selected) != forceCompareLimit {
				t.Fatalf("accepted source lacks %d entries", forceCompareLimit)
			}
			manifest := filepath.Join(root, "selected-paths.nul")
			rtest.OK(t, os.WriteFile(manifest, []byte(strings.Join(selected[1:], "\x00")+"\x00"), 0600))
			rtest.OK(t, os.Chmod(manifest, 0600))
			publicationQualificationCommand(t, root, "freeze", "rsync", "-a", "--no-recursive", "--dirs", "--from0", "--files-from="+manifest, acceptedSource+"/", source+"/")
		} else {
			publicationQualificationCommand(t, root, "freeze", "rsync", "-a", acceptedSource+"/", source+"/")
		}
		rtest.OK(t, os.Chmod(source, 0700))
		paths, err := boundedLiveSourcePaths(source, 100001)
		rtest.OK(t, err)
		entryCount := 100000
		if forceCompare == "1" {
			entryCount = forceCompareLimit
		}
		if len(paths) != entryCount {
			t.Fatalf("frozen restore has %d entries, expected %d", len(paths), entryCount)
		}
		frozenPaths = make(map[string]bool, len(paths))
		for _, path := range paths {
			frozenPaths[path] = true
		}
		digest, files, size = m5FilesSelected(t, source, false, frozenPaths)
		regularFiles = publicationQualificationRegularFiles(t, source, paths)
		if forceCompare == "1" {
			expected, expectedFiles, expectedBytes := m5FilesSelected(t, acceptedSource, false, frozenPaths)
			if digest != expected || files != expectedFiles || size != expectedBytes {
				t.Fatal("force-comparison fixture differs from accepted source subset")
			}
		} else if digest != result.InputSHA256 || files != result.Files || size != result.Bytes || regularFiles != 89300 {
			t.Fatal("frozen source differs from the accepted restore")
		}
		t.Logf("unchanged_fixture entries=%d regular_files=%d bytes=%d sha256=%s", len(paths), regularFiles, size, digest)
	} else if os.Getenv("VAULTIC_PHASE35_THROUGHPUT_SOURCE") != "" {
		if os.Getenv("VAULTIC_PHASE35_THROUGHPUT_SOURCE") != "/ncl1-1-vs-50/fme_cae1" || onDemand != "1" || resumeVerification != "" {
			t.Fatal("bounded source requires the selected fme_cae1 root, on-demand lookup, and a fresh run")
		}
		encoded, err := os.ReadFile(filepath.Join(root, "inode-manifest.json"))
		rtest.OK(t, err)
		rtest.OK(t, json.Unmarshal(encoded, &sourceManifest))
		if sourceManifest.Source != os.Getenv("VAULTIC_PHASE35_THROUGHPUT_SOURCE") || sourceManifest.InodeLimit != 100_000 || len(sourceManifest.Paths) != 100_000 {
			t.Fatal("bounded source manifest scope differs")
		}
		source = sourceManifest.Source
		encoded, err = os.ReadFile(filepath.Join(root, "source-observation.json"))
		rtest.OK(t, err)
		var observation struct {
			SHA256       string `json:"sha256"`
			Files        uint64 `json:"files"`
			RegularFiles uint64 `json:"regular_files"`
			Bytes        uint64 `json:"bytes"`
		}
		rtest.OK(t, json.Unmarshal(encoded, &observation))
		digest, files, size = observation.SHA256, observation.Files, observation.Bytes
		regularFiles = observation.RegularFiles
		if regularFiles == 0 || regularFiles > files || regularFiles != publicationQualificationRegularFiles(t, source, sourceManifest.Paths) {
			t.Fatal("sealed regular-file count is missing or changed")
		}
	} else if frozen := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_INPUT"); frozen != "" {
		if filepath.Base(frozen) != "input" {
			t.Fatal("frozen sample must be an owned input directory")
		}
		rtest.OK(t, validatePublicationQualificationRoot(filepath.Dir(frozen), "/volume2/NASDA2/vaultic-test-m6"))
		if resumeVerification == "" {
			m5Copy(t, source, frozen)
			rtest.OK(t, os.Chmod(source, 0700))
		}
		digest, files, size = m5Files(t, source, false)
		if digest != os.Getenv("VAULTIC_PHASE35_THROUGHPUT_INPUT_SHA256") || files < 128 || files > 512 || size > 512<<20 {
			t.Fatal("frozen input digest or scope changed")
		}
		t.Logf("reused_frozen_sample files=%d bytes=%d sha256=%s", files, size, digest)
	} else {
		if resumeVerification != "" {
			t.Fatal("verification resume requires a digest-pinned frozen input")
		}
		digest, files, size = publicationQualificationSample(t, source)
	}
	var expectedSourceTree vaultic.ID
	groups := []int{4, 64}
	order := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ORDER")
	if order == "64" {
		groups = []int{64}
	} else if order == "64,4" {
		groups = []int{64, 4}
	} else if order != "" && order != "4,64" {
		t.Fatal("unsupported publication qualification order")
	}
	pairedReads := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_PAIRED_READS")
	if pairedReads != "" {
		if pairedReads != "1" || order != "64" || resumeVerification != "" ||
			candidate != "/root/proj/vaultic/bin/phase35-critical-path-paired-reads/linux-amd64/vaultic" {
			t.Fatal("paired-read comparison requires a fresh group-64 run and the pinned paired-read candidate")
		}
		groups = []int{64, 64}
	}
	for cellIndex, groupSize := range groups {
		cellName := fmt.Sprintf("group-%d", groupSize)
		cellCandidate := candidate
		if pairedReads != "" && cellIndex == 0 {
			cellName += "-baseline"
			cellCandidate = "/root/proj/vaultic/bin/phase35-critical-path-fill/linux-amd64/vaultic"
			encoded, err := os.ReadFile(cellCandidate)
			rtest.OK(t, err)
			if fmt.Sprintf("%x", sha256.Sum256(encoded)) != "cbff53916df13e6d72a65451a7185c444e779279e1312f78d42489e996901fbf" {
				t.Fatal("paired-read baseline digest mismatch")
			}
		}
		t.Run(cellName, func(t *testing.T) {
			candidate := cellCandidate
			cell := filepath.Join(root, cellName)
			acceptedBaseline := resumeVerification == "group-64" && groupSize == 4
			resumeCell := acceptedBaseline || resumeVerification == fmt.Sprintf("group-%d", groupSize)
			if !resumeCell {
				rtest.OK(t, os.Mkdir(cell, 0700))
				rtest.OK(t, os.Chmod(cell, 0700))
			} else {
				resolved, err := filepath.EvalSymlinks(cell)
				rtest.OK(t, err)
				if resolved != cell {
					t.Fatal("noncanonical verification resume cell")
				}
				if _, err := os.Stat(filepath.Join(cell, "result.json")); !acceptedBaseline && !errors.Is(err, os.ErrNotExist) {
					t.Fatal("verification resume requires an unaccepted cell")
				}
				if acceptedBaseline {
					encoded, err := os.ReadFile(filepath.Join(cell, "result.json"))
					rtest.OK(t, err)
					var result struct {
						RestoreVerified bool   `json:"restore_verified"`
						ReopenVerified  bool   `json:"reopen_verified"`
						InputSHA256     string `json:"input_sha256"`
					}
					rtest.OK(t, json.Unmarshal(encoded, &result))
					if !result.RestoreVerified || !result.ReopenVerified || result.InputSHA256 != digest {
						t.Fatal("baseline verification evidence is not accepted for this input")
					}
				}
			}
			database := filepath.Join(cell, "db")
			repoPath := filepath.Join(cell, "repo")
			if !resumeCell {
				rtest.OK(t, os.Mkdir(database, 0700))
				t.Log("copying and checksum-verifying closed encrypted seed")
				lock := filepath.Join(seed, repositoryHash+".coordination.lock")
				publicationQualificationCommand(t, cell, "copy", "flock", "--no-fork", "--shared", "--nonblock", lock,
					"rsync", "-a", "--exclude=*.coordination.lock", seed+"/", database+"/")
				publicationQualificationCommand(t, cell, "checksum", "flock", "--no-fork", "--shared", "--nonblock", lock,
					"rsync", "-rcni", "--delete", "--exclude=*.coordination.lock", seed+"/", database+"/")
				comparison, err := os.ReadFile(filepath.Join(cell, "checksum.stdout.log"))
				rtest.OK(t, err)
				if len(bytes.TrimSpace(comparison)) != 0 {
					t.Fatal("encrypted clone checksum mismatch")
				}
				copiedBinding, err := os.ReadFile(filepath.Join(database, bindingRelative))
				rtest.OK(t, err)
				if !bytes.Equal(copiedBinding, binding) {
					t.Fatal("copied WAL binding changed before relocation")
				}
				rtest.OK(t, os.Chmod(database, 0700))
				rtest.OK(t, os.WriteFile(filepath.Join(database, bindingRelative), []byte("vaulticdb-local-wal-after-bulk-import-v1:"+database+"/wal"), 0600))
				afterBinding, err := os.ReadFile(filepath.Join(seed, bindingRelative))
				rtest.OK(t, err)
				if !bytes.Equal(afterBinding, binding) {
					t.Fatal("seed WAL binding changed")
				}
				for _, directory := range []string{"", "keys", "index", "snapshots", "locks", "data", "working", "cache"} {
					rtest.OK(t, os.MkdirAll(filepath.Join(repoPath, directory), 0700))
					rtest.OK(t, os.Chmod(filepath.Join(repoPath, directory), 0700))
				}
				publicationQualificationCopy(t, filepath.Join(repoPath, "config"), filepath.Join(original, "config"))
				keys, err := os.ReadDir(filepath.Join(original, "keys"))
				rtest.OK(t, err)
				for _, key := range keys {
					publicationQualificationCopy(t, filepath.Join(repoPath, "keys", key.Name()), filepath.Join(original, "keys", key.Name()))
				}
				for _, snapshot := range marker.Snapshots {
					if _, err := vaultic.ParseID(snapshot); err != nil {
						t.Fatal("invalid seed snapshot identifier")
					}
					publicationQualificationCopy(t, filepath.Join(repoPath, "snapshots", snapshot), filepath.Join(original, "snapshots", snapshot))
				}
			} else {
				resolved, err := filepath.EvalSymlinks(database)
				rtest.OK(t, err)
				if resolved != database {
					t.Fatal("noncanonical verification resume database")
				}
				currentBinding, err := os.ReadFile(filepath.Join(database, bindingRelative))
				rtest.OK(t, err)
				if string(currentBinding) != "vaulticdb-local-wal-after-bulk-import-v1:"+database+"/wal" {
					t.Fatal("verification resume WAL is not private")
				}
			}
			controlRoot := "/run"
			if unchanged != "" {
				controlRoot = os.Getenv("VAULTIC_PHASE35_THROUGHPUT_CONTROL_ROOT")
				resolved, err := filepath.EvalSymlinks(controlRoot)
				rtest.OK(t, err)
				info, err := os.Stat(controlRoot)
				rtest.OK(t, err)
				if resolved != controlRoot || !strings.HasPrefix(controlRoot, "/run/vaultic-unchanged-") || !info.IsDir() || info.Mode().Perm() != 0700 {
					t.Fatal("unchanged qualification requires a canonical private runtime directory")
				}
			}
			control, err := os.MkdirTemp(controlRoot, "v-ab-")
			rtest.OK(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(control) })
			daemonOptions := daemon.Options{Socket: filepath.Join(control, "db.sock"), RepositoryID: repositoryID, DaemonPath: binary,
				DataDir: database, WALDataDir: filepath.Join(database, "wal"), ObjectStore: "local", WALStore: "local",
				WALFlushInterval: 100 * time.Millisecond, MetaCacheBytes: 128 << 20, BlockCacheBytes: 512 << 20,
				EncryptionMode: "required", PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery", StartTimeout: 90 * time.Second}
			daemonOptions.EngineMultiGet = expanded == "50000"
			started := time.Now()
			client, err := daemon.Ensure(t.Context(), daemonOptions)
			rtest.OK(t, err)
			startupSeconds := time.Since(started).Seconds()
			defer func() {
				if client != nil {
					rtest.OK(t, client.Close(context.Background()))
				}
			}()
			options := publicationQualificationOptions(repoPath, daemonOptions.Socket, daemonOptions.PassphraseFile)
			rtest.OK(t, publicationQualificationLoadPassword(&options))
			var copiedBytes uint64
			rtest.OK(t, withTermStatus(t, options, func(ctx context.Context, options global.Options) error {
				repo, err := global.OpenRepository(ctx, options, progress.NewTerminalPrinter(true, 0, options.Term))
				if err != nil {
					return err
				}
				return errors.Join(metadataindex.Activate(ctx, repo.Backend(), repositoryID), repo.Close())
			}))
			if !resumeCell {
				for _, pack := range publicationQualificationPendingPacks(t, client) {
					id := pack.String()
					relative := filepath.Join("data", id[:2], id)
					info, err := os.Stat(filepath.Join(original, relative))
					rtest.OK(t, err)
					copiedBytes += uint64(info.Size())
					if copiedBytes > 16<<30 {
						t.Fatal("private pending-pack copy exceeds shared pack-byte ceiling")
					}
					directory := filepath.Dir(filepath.Join(repoPath, relative))
					rtest.OK(t, os.MkdirAll(directory, 0700))
					rtest.OK(t, os.Chmod(directory, 0700))
					publicationQualificationCopy(t, filepath.Join(repoPath, relative), filepath.Join(original, relative))
				}
				t.Logf("pending_pack_bytes_copied=%d; recovery remains inside timed backup", copiedBytes)
			}
			before, err := client.WriterStatus(t.Context())
			rtest.OK(t, err)
			if before.ActiveTransactions != 0 || before.ActiveWriteIntents != 0 || before.EngineFlushIntervalMS != 100 || !before.ProcessCPUAvailable {
				t.Fatal("private writer is not idle on pinned durability defaults")
			}
			var wall time.Duration
			var cliCPU float64
			after := before
			if !resumeCell {
				backupArgs := []string{"--repo=" + repoPath,
					"--password-file=" + options.PasswordFile, "--metadata-daemon-socket=" + daemonOptions.Socket,
					"--metadata-encryption-mode=required", "--metadata-passphrase-file=" + daemonOptions.PassphraseFile, "--no-cache", "--json",
					"backup", "--metadata-publication-group-size=" + fmt.Sprint(groupSize), "--one-file-system", "--no-scan",
					"--working-memory-mode=ram", "--working-memory-ram-limit=160GiB", "--working-memory-scratch=" + filepath.Join(repoPath, "working"),
					"--working-memory-disk-budget=2GiB"}
				if onDemand == "1" {
					backupArgs = append(backupArgs, "--metadata-on-demand", "--metadata-cache-mib=64")
				}
				if unchanged != "" {
					backupArgs = append(backupArgs, "--read-concurrency=16", "--metadata-reconciliation-queue-depth=256")
					t.Log("creating complete nonselective parent; this backup is outside the unchanged measurement")
				}
				if sourceManifest.Source != "" {
					backupArgs = append(backupArgs, "--bounded-inode-manifest="+filepath.Join(root, "inode-manifest.json"), "--metadata-reconciliation-queue-depth=256", "--force")
				}
				backupArgs = append(backupArgs, source)
				if sourceManifest.Source != "" {
					preflightArgs := make([]string, 0, len(backupArgs)+1)
					for _, argument := range backupArgs {
						if argument != "--metadata-on-demand" {
							preflightArgs = append(preflightArgs, argument)
						}
					}
					preflightArgs = append(preflightArgs, "--dry-run")
					publicationQualificationCommand(t, cell, "preflight", candidate, preflightArgs...)
					output, err := os.ReadFile(filepath.Join(cell, "preflight.stdout.log"))
					rtest.OK(t, err)
					matched := false
					for _, line := range bytes.Split(output, []byte{'\n'}) {
						var event struct {
							MessageType string `json:"message_type"`
							Files       uint64 `json:"total_files_processed"`
						}
						if json.Unmarshal(line, &event) == nil && event.MessageType == "summary" {
							if event.Files != regularFiles {
								t.Fatalf("preflight regular-file count differs: processed=%d sealed=%d", event.Files, regularFiles)
							}
							matched = true
						}
					}
					if !matched {
						t.Fatal("bounded preflight did not report a completed selection")
					}
					state, err := client.WriterStatus(t.Context())
					rtest.OK(t, err)
					if state.LastDurableSequence != before.LastDurableSequence || state.ActiveTransactions != 0 || state.ActiveWriteIntents != 0 {
						t.Fatal("dry-run preflight changed private durable state")
					}
					t.Logf("bounded preflight accepted %d regular files; main-backup timing starts next", regularFiles)
				}
				wall, cliCPU = publicationQualificationCommand(t, cell, "backup", candidate, backupArgs...)
				after, err = client.WriterStatus(t.Context())
				rtest.OK(t, err)
				if after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
					t.Fatal("backup left transactions or write intents")
				}
				observation, err := json.MarshalIndent(map[string]any{
					"candidate":          candidate,
					"metadata_on_demand": onDemand == "1",
					"group_size":         groupSize, "startup_seconds": startupSeconds, "backup_seconds": wall.Seconds(),
					"cli_cpu_seconds": cliCPU, "writer_before": before, "writer_after": after,
					"restore_verified": false, "reopen_verified": false,
				}, "", "  ")
				rtest.OK(t, err)
				observationFile, err := os.OpenFile(filepath.Join(cell, "backup-observation.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				rtest.OK(t, err)
				rtest.OK(t, observationFile.Chmod(0600))
				_, err = observationFile.Write(append(observation, '\n'))
				rtest.OK(t, errors.Join(err, observationFile.Close()))
			} else {
				observation, err := os.ReadFile(filepath.Join(cell, "backup-observation.json"))
				rtest.OK(t, err)
				var saved map[string]json.RawMessage
				rtest.OK(t, json.Unmarshal(observation, &saved))
				var savedGroup int
				var backupSeconds float64
				rtest.OK(t, json.Unmarshal(saved["group_size"], &savedGroup))
				rtest.OK(t, json.Unmarshal(saved["backup_seconds"], &backupSeconds))
				rtest.OK(t, json.Unmarshal(saved["startup_seconds"], &startupSeconds))
				rtest.OK(t, json.Unmarshal(saved["cli_cpu_seconds"], &cliCPU))
				rtest.OK(t, json.Unmarshal(saved["writer_before"], &before))
				rtest.OK(t, json.Unmarshal(saved["writer_after"], &after))
				if savedGroup != groupSize || backupSeconds <= 0 || after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
					t.Fatal("invalid retained backup observation")
				}
				wall = time.Duration(backupSeconds * float64(time.Second))
				testListSnapshots(t, options, len(marker.Snapshots)+1)
				t.Log("resuming verification only; retained backup timings are unchanged")
			}
			output, err := os.ReadFile(filepath.Join(cell, "backup.stdout.log"))
			rtest.OK(t, err)
			var snapshotID string
			for _, line := range bytes.Split(output, []byte{'\n'}) {
				var event struct {
					MessageType    string `json:"message_type"`
					SnapshotID     string `json:"snapshot_id"`
					FilesProcessed uint64 `json:"total_files_processed"`
				}
				if json.Unmarshal(line, &event) == nil && event.MessageType == "summary" {
					if sourceManifest.Source != "" && event.FilesProcessed != regularFiles {
						t.Fatalf("bounded source regular-file count differs: processed=%d sealed=%d; no throughput acceptance", event.FilesProcessed, regularFiles)
					}
					snapshotID = event.SnapshotID
				}
			}
			id, err := vaultic.ParseID(snapshotID)
			rtest.OK(t, err)
			snapshot := testLoadSnapshot(t, options, id)
			if snapshot.Tree == nil {
				t.Fatal("backup did not acknowledge a tree")
			}
			store := daemon.NewSchemaStore(client)
			materialized := make(map[vaultic.ID]bool)
			materialize := func(blob vaultic.ID) {
				if materialized[blob] {
					return
				}
				value, found, err := store.Get(t.Context(), schema.BlobKey(schema.ID(blob)))
				rtest.OK(t, err)
				if !found {
					t.Fatal("snapshot blob missing from cloned catalog")
				}
				record, err := schema.UnmarshalBlobRecord(value)
				rtest.OK(t, err)
				pack := vaultic.ID(record.Locations[0].PackID).String()
				relative := filepath.Join("data", pack[:2], pack)
				if _, err := os.Stat(filepath.Join(repoPath, relative)); errors.Is(err, os.ErrNotExist) {
					info, err := os.Stat(filepath.Join(original, relative))
					rtest.OK(t, err)
					copiedBytes += uint64(info.Size())
					copyLimit := uint64(16 << 30)
					if sourceManifest.Source != "" || unchanged != "" {
						copyLimit = 128 << 30
					}
					if copiedBytes > copyLimit {
						t.Fatal("private referenced-pack copy limit exceeded")
					}
					rtest.OK(t, os.MkdirAll(filepath.Dir(filepath.Join(repoPath, relative)), 0700))
					publicationQualificationCopy(t, filepath.Join(repoPath, relative), filepath.Join(original, relative))
				} else {
					rtest.OK(t, err)
				}
				materialized[blob] = true
			}
			rtest.OK(t, withTermStatus(t, options, func(ctx context.Context, options global.Options) error {
				repo, err := global.OpenRepository(ctx, options, progress.NewTerminalPrinter(true, 0, options.Term))
				if err != nil {
					return err
				}
				defer repo.Close()
				lookup, err := publicationQualificationLookup(ctx, daemonOptions, filepath.Join(cell, "verification-lookup-"+filepath.Base(control)))
				if err != nil {
					return err
				}
				repo.SetEngine(lookup)
				if err := repo.LoadIndex(ctx, progress.NewTerminalPrinter(true, 0, options.Term)); err != nil {
					return err
				}
				selectedTree, err := data.FindTreeDirectory(ctx, repo, snapshot.Tree, source)
				if err != nil {
					return err
				}
				if expectedSourceTree.IsNull() {
					expectedSourceTree = *selectedTree
				} else if expectedSourceTree != *selectedTree {
					return fmt.Errorf("group-size variants produced different selected source trees")
				}
				if acceptedBaseline {
					return nil
				}
				var visit func(vaultic.ID) error
				visit = func(treeID vaultic.ID) error {
					materialize(treeID)
					encoded, err := repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: treeID}, nil)
					if err != nil {
						return err
					}
					var tree struct {
						Nodes []*data.Node `json:"nodes"`
					}
					if err := json.Unmarshal(encoded, &tree); err != nil {
						return err
					}
					for _, node := range tree.Nodes {
						for _, blob := range node.Content {
							materialize(blob)
						}
						if node.Subtree != nil {
							if err := visit(*node.Subtree); err != nil {
								return err
							}
						}
					}
					return nil
				}
				return visit(*snapshot.Tree)
			}))
			if acceptedBaseline {
				t.Logf("retained accepted baseline source_tree=%s", expectedSourceTree)
				return
			}
			started = time.Now()
			restored := filepath.Join(cell, "restore")
			rtest.OK(t, testRunRestoreAssumeFailure(t, id.String(), restoreOptions{Target: restored, Verify: true}, options))
			restoreSeconds := time.Since(started).Seconds()
			restoredSource := filepath.Join(restored, strings.TrimPrefix(source, "/"))
			selected := frozenPaths
			if sourceManifest.Source != "" {
				if snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != "bounded" {
					t.Fatal("bounded snapshot falsely claims full source coverage")
				}
				selected = make(map[string]bool, len(sourceManifest.Paths))
				for _, path := range sourceManifest.Paths {
					selected[path] = true
				}
			}
			actual, actualFiles, actualBytes := m5FilesSelected(t, restoredSource, false, selected)
			if actual != digest || actualFiles != files || actualBytes != size {
				t.Fatal("verified restore differs from frozen input")
			}
			started = time.Now()
			rtest.OK(t, publicationQualificationReopen(t.Context(), &client, daemonOptions))
			reopenSeconds := time.Since(started).Seconds()
			reopened := testLoadSnapshot(t, options, id)
			if reopened.Tree == nil || *reopened.Tree != *snapshot.Tree {
				t.Fatal("snapshot changed after encrypted database reopen")
			}
			result := map[string]any{"group_size": groupSize, "files": files, "bytes": size, "input_sha256": digest,
				"metadata_on_demand": onDemand == "1",
				"source_tree":        expectedSourceTree.String(),
				"snapshot":           id.String(), "tree": snapshot.Tree.String(), "startup_seconds": startupSeconds, "backup_seconds": wall.Seconds(),
				"files_per_second": float64(files) / wall.Seconds(), "cli_cpu_seconds": cliCPU,
				"daemon_cpu_us": after.ProcessCPUUserUS + after.ProcessCPUSystemUS - before.ProcessCPUUserUS - before.ProcessCPUSystemUS,
				"writer_before": before, "writer_after": after, "restore_seconds": restoreSeconds, "reopen_seconds": reopenSeconds,
				"referenced_pack_bytes_copied": copiedBytes, "restore_verified": true, "reopen_verified": true}
			encoded, err = json.MarshalIndent(result, "", "  ")
			rtest.OK(t, err)
			resultFile, err := os.OpenFile(filepath.Join(cell, "result.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			rtest.OK(t, err)
			rtest.OK(t, resultFile.Chmod(0600))
			_, err = resultFile.Write(append(encoded, '\n'))
			rtest.OK(t, errors.Join(err, resultFile.Close()))
			t.Logf("qualified_backup group=%d files=%d seconds=%.3f files_per_second=%.3f tree=%s", groupSize, files, wall.Seconds(), float64(files)/wall.Seconds(), snapshot.Tree)
			if unchanged != "" {
				if snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != "full" || snapshot.CrawlPlan.ReusedSubtrees != 0 {
					t.Fatal("unchanged benchmark parent is not a complete full traversal")
				}
				if forceCompare == "1" {
					publicationQualificationForceComparison(t, cell, source, candidate, options, daemonOptions, &client, id,
						frozenPaths, digest, regularFiles, files, size, metadataReadFlags...)
				} else {
					publicationQualificationUnchanged(t, cell, source, candidate, options, daemonOptions, &client, id,
						frozenPaths, digest, regularFiles, files, size)
				}
			}
		})
		if t.Failed() {
			break
		}
	}
}

func publicationQualificationUnchanged(t *testing.T, cell, source, candidate string, options global.Options,
	daemonOptions daemon.Options, client **daemon.Client, parent vaultic.ID, paths map[string]bool, digest string,
	regularFiles, files, size uint64,
) {
	t.Helper()
	before, err := (*client).WriterStatus(t.Context())
	rtest.OK(t, err)
	if before.ActiveTransactions != 0 || before.ActiveWriteIntents != 0 {
		t.Fatal("unchanged pass requires an idle private writer")
	}
	t.Log("starting unchanged full crawl; require at least 600 main-phase seconds without throttling or repeats")
	wall, cpu := publicationQualificationCommand(t, cell, "unchanged", candidate,
		"--repo="+options.Repo, "--password-file="+options.PasswordFile, "--metadata-daemon-socket="+daemonOptions.Socket,
		"--metadata-encryption-mode=required", "--metadata-passphrase-file="+daemonOptions.PassphraseFile, "--no-cache", "--json",
		"backup", "--no-scan", "--one-file-system", "--read-concurrency=16", "--metadata-publication-group-size=64",
		"--metadata-reconciliation-queue-depth=256", "--metadata-on-demand", "--metadata-cache-mib=64",
		"--working-memory-mode=ram", "--working-memory-ram-limit=160GiB", "--working-memory-scratch="+filepath.Join(options.Repo, "working"),
		"--working-memory-disk-budget=2GiB", source)
	output, err := os.ReadFile(filepath.Join(cell, "unchanged.stdout.log"))
	rtest.OK(t, err)
	var summary m5BackupSummary
	var measurement unchangedCrawlMeasurement
	var snapshotID string
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		var event struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal(line, &event) != nil {
			continue
		}
		switch event.MessageType {
		case "summary":
			rtest.OK(t, json.Unmarshal(line, &summary))
			snapshotID = event.SnapshotID
		case "backup_critical_path":
			rtest.OK(t, json.Unmarshal(line, &measurement))
		}
	}
	rtest.OK(t, validateUnchangedCrawlMeasurement(summary, measurement, regularFiles, uint64(len(paths))))
	id, err := vaultic.ParseID(snapshotID)
	rtest.OK(t, err)
	snapshot := testLoadSnapshot(t, options, id)
	if snapshot.Parent == nil || *snapshot.Parent != parent || snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != "full" ||
		snapshot.CrawlPlan.ReusedSubtrees != 0 || len(snapshot.Paths) != 1 || snapshot.Paths[0] != source {
		t.Fatal("unchanged pass lacks the complete parent and nonselective source provenance")
	}
	after, err := (*client).WriterStatus(t.Context())
	rtest.OK(t, err)
	if after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
		t.Fatal("unchanged pass left active transactions or intents")
	}
	restored := filepath.Join(cell, "unchanged-restore")
	rtest.OK(t, testRunRestoreAssumeFailure(t, id.String(), restoreOptions{Target: restored, Verify: true}, options))
	actual, actualFiles, actualBytes := m5FilesSelected(t, filepath.Join(restored, strings.TrimPrefix(source, "/")), false, paths)
	current, currentFiles, currentBytes := m5FilesSelected(t, source, false, paths)
	if actual != digest || current != digest || actualFiles != files || currentFiles != files || actualBytes != size || currentBytes != size {
		t.Fatal("unchanged restore or final source differs from sealed input")
	}
	rtest.OK(t, publicationQualificationReopen(t.Context(), client, daemonOptions))
	retained := testLoadSnapshot(t, options, id)
	if retained.Tree == nil || snapshot.Tree == nil || *retained.Tree != *snapshot.Tree {
		t.Fatal("unchanged snapshot changed after encrypted reopen")
	}
	result, err := json.MarshalIndent(map[string]any{
		"main_seconds": measurement.SecondsElapsed, "command_seconds": wall.Seconds(), "cli_cpu_seconds": cpu,
		"observed_entries": measurement.Reconciliation.Observed, "entries_per_second": float64(len(paths)) / measurement.SecondsElapsed,
		"regular_files": regularFiles, "bytes": size, "input_sha256": digest, "summary": summary,
		"parent": parent.String(), "snapshot": id.String(), "writer_before": before, "writer_after": after,
		"restore_verified": true, "reopen_verified": true, "source_unchanged": true,
		"source_payload_reads_measured": false, "production_repository_modified": false,
	}, "", "  ")
	rtest.OK(t, err)
	file, err := os.OpenFile(filepath.Join(cell, "unchanged-result.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	rtest.OK(t, err)
	rtest.OK(t, file.Chmod(0600))
	_, err = file.Write(append(result, '\n'))
	rtest.OK(t, errors.Join(err, file.Sync(), file.Close()))
	t.Logf("qualified_unchanged_crawl main_seconds=%.3f entries=%d entries_per_second=%.3f data_blobs=%d",
		measurement.SecondsElapsed, len(paths), float64(len(paths))/measurement.SecondsElapsed, summary.DataBlobs)
}

type publicationDBTuning struct {
	MetaCacheBytes  uint64
	BlockCacheBytes uint64
	CryptoThreads   int
	Flags           []string
}

func publicationDBProfile(profile string) (publicationDBTuning, error) {
	tuning := publicationDBTuning{MetaCacheBytes: 128 << 20, BlockCacheBytes: 512 << 20, CryptoThreads: 8,
		Flags: []string{"--read-concurrency=64", "--metadata-read-concurrency=32", "--metadata-batch-delay=1ms"}}
	switch profile {
	case "db-control", "db-control-end":
	case "db-crypto32":
		tuning.CryptoThreads = 32
	case "db-meta2g":
		tuning.MetaCacheBytes = 2 << 30
	case "db-block8g":
		tuning.BlockCacheBytes = 8 << 30
	case "db-batch5ms":
		tuning.Flags[2] = "--metadata-batch-delay=5ms"
	case "db-rpc64":
		tuning.Flags[1] = "--metadata-read-concurrency=64"
	default:
		return publicationDBTuning{}, fmt.Errorf("unsupported DB profile %q", profile)
	}
	return tuning, nil
}

func TestPublicationDBProfile(t *testing.T) {
	for profile, expected := range map[string]struct {
		meta, block uint64
		crypto      int
		rpc, delay  string
	}{
		"db-control":     {128 << 20, 512 << 20, 8, "32", "1ms"},
		"db-control-end": {128 << 20, 512 << 20, 8, "32", "1ms"},
		"db-crypto32":    {128 << 20, 512 << 20, 32, "32", "1ms"},
		"db-meta2g":      {2 << 30, 512 << 20, 8, "32", "1ms"},
		"db-block8g":     {128 << 20, 8 << 30, 8, "32", "1ms"},
		"db-batch5ms":    {128 << 20, 512 << 20, 8, "32", "5ms"},
		"db-rpc64":       {128 << 20, 512 << 20, 8, "64", "1ms"},
	} {
		t.Run(profile, func(t *testing.T) {
			tuning, err := publicationDBProfile(profile)
			rtest.OK(t, err)
			if tuning.MetaCacheBytes != expected.meta || tuning.BlockCacheBytes != expected.block || tuning.CryptoThreads != expected.crypto ||
				strings.Join(tuning.Flags, " ") != "--read-concurrency=64 --metadata-read-concurrency="+expected.rpc+" --metadata-batch-delay="+expected.delay {
				t.Fatalf("unexpected DB settings: %+v", tuning)
			}
		})
	}
	for _, profile := range []string{"", "db-crypto128", "db-meta64g", "../db-control"} {
		if _, err := publicationDBProfile(profile); err == nil {
			t.Fatalf("unapproved DB profile accepted: %q", profile)
		}
	}
}

func publicationConcurrencyProfile(profile string) ([]string, error) {
	readers, rpcs := 16, 8
	switch profile {
	case "baseline":
	case "rpcs":
		rpcs = 32
	case "readers":
		readers, rpcs = 64, 32
	default:
		return nil, fmt.Errorf("unsupported concurrency profile %q", profile)
	}
	return []string{fmt.Sprintf("--read-concurrency=%d", readers), fmt.Sprintf("--metadata-read-concurrency=%d", rpcs), "--metadata-batch-delay=1ms"}, nil
}

func TestPublicationConcurrencyProfile(t *testing.T) {
	for profile, expected := range map[string]string{"baseline": "16/8", "rpcs": "16/32", "readers": "64/32"} {
		flags, err := publicationConcurrencyProfile(profile)
		rtest.OK(t, err)
		actual := strings.TrimPrefix(flags[0], "--read-concurrency=") + "/" + strings.TrimPrefix(flags[1], "--metadata-read-concurrency=")
		if actual != expected || flags[2] != "--metadata-batch-delay=1ms" {
			t.Fatalf("profile %s: %v", profile, flags)
		}
	}
	for _, profile := range []string{"", "callers", "128", "../rpcs"} {
		if _, err := publicationConcurrencyProfile(profile); err == nil {
			t.Fatalf("unapproved profile accepted: %q", profile)
		}
	}
}

func TestPublicationForceComparisonRetry(t *testing.T) {
	if os.Getenv("VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE_RETRY") != "1" {
		t.Skip("qualified-parent force-comparison retry not selected")
	}
	previous := "/volume2/NASDA2/vaultic-test-m6/force-compare-fme-cae1-20261103-10k"
	root := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT")
	mode := os.Getenv("VAULTIC_PHASE35_METADATA_READ_COMPARE")
	profile := os.Getenv("VAULTIC_PHASE35_METADATA_READ_CONCURRENCY_PROFILE")
	entries, expectedRegular, expectedFiles, expectedBytes := 10000, uint64(9209), uint64(9218), uint64(2460946936)
	expectedDigest := "2f76a6f4397effad64c9922cc6337a836b3c4ac21b870114942e02b63b996f80"
	var extraFlags []string
	var dbTuning *publicationDBTuning
	switch mode {
	case "":
	case "parallel":
		extraFlags = []string{"--unchanged-read-concurrency=8"}
	case "coalesced":
		extraFlags = []string{"--unchanged-read-concurrency=16", "--metadata-read-concurrency=8", "--metadata-batch-delay=1ms"}
	case "catalog":
		extraFlags = []string{"--unchanged-read-concurrency=8", "--metadata-exact-catalog"}
	default:
		t.Fatal("unknown metadata-read qualification mode")
	}
	if profile != "" {
		previous = "/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-50k-coalesced"
		expectedRoot := previous + "-forced-concurrency-" + profile
		if strings.HasPrefix(profile, "db-") {
			expectedRoot = previous + "-forced-db-" + strings.TrimPrefix(profile, "db-")
		}
		if mode != "coalesced" || os.Getenv("VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT") != "50000" ||
			os.Getenv("VAULTIC_PHASE35_METADATA_READ_FORCE_ONLY") != "1" || root != expectedRoot {
			t.Fatal("concurrency screen requires a fresh allowlisted forced-only 50k coalesced cell")
		}
		var err error
		if strings.HasPrefix(profile, "db-") {
			tuning, tuningErr := publicationDBProfile(profile)
			rtest.OK(t, tuningErr)
			if os.Getenv("VAULTICDB_CRYPTO_THREADS") != fmt.Sprint(tuning.CryptoThreads) {
				t.Fatal("DB screen crypto environment differs from the selected profile")
			}
			dbTuning, extraFlags = &tuning, tuning.Flags
		} else {
			extraFlags, err = publicationConcurrencyProfile(profile)
		}
		rtest.OK(t, err)
		entries, expectedRegular, expectedFiles, expectedBytes = 50000, 41347, 41372, 19493496103
		expectedDigest = "8f32883b02c37ae550a0375fa6abfe79ec52d830ef0d9cff1053522687a41e87"
	} else if (mode == "" && root != previous+"/retry") || (mode != "" && root != "/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-10k-"+mode) {
		t.Fatal("retry requires the new private child of the retained failed capture")
	}
	rtest.OK(t, validatePublicationQualificationRoot(root, "/volume2/NASDA2/vaultic-test-m6"))
	parentCell := filepath.Join(previous, "group-64")
	var receipt struct {
		RestoreVerified bool   `json:"restore_verified"`
		ReopenVerified  bool   `json:"reopen_verified"`
		InputSHA256     string `json:"input_sha256"`
		Snapshot        string `json:"snapshot"`
		Files           uint64 `json:"files"`
		Bytes           uint64 `json:"bytes"`
	}
	encoded, err := os.ReadFile(filepath.Join(parentCell, "result.json"))
	rtest.OK(t, err)
	rtest.OK(t, json.Unmarshal(encoded, &receipt))
	if !receipt.RestoreVerified || !receipt.ReopenVerified || receipt.Files != expectedFiles || receipt.Bytes != expectedBytes ||
		receipt.InputSHA256 != expectedDigest {
		t.Fatal("retry parent lacks qualified source and reopen evidence")
	}
	parent, err := vaultic.ParseID(receipt.Snapshot)
	rtest.OK(t, err)
	source := filepath.Join(previous, "input")
	selected, err := boundedLiveSourcePaths(source, uint64(entries+1))
	rtest.OK(t, err)
	if len(selected) != entries {
		t.Fatal("qualified retry source scope changed")
	}
	paths := make(map[string]bool, len(selected))
	for _, path := range selected {
		paths[path] = true
	}
	digest, files, size := m5FilesSelected(t, source, false, paths)
	regularFiles := publicationQualificationRegularFiles(t, source, selected)
	if digest != receipt.InputSHA256 || files != receipt.Files || size != receipt.Bytes || regularFiles != expectedRegular {
		t.Fatal("qualified retry source content changed")
	}
	candidate := "/root/proj/vaultic/bin/phase35-force-10k/linux-amd64/vaultic"
	binary := "/root/proj/vaultic/bin/phase35-force-10k/linux-amd64/vaulticdb"
	if mode != "" {
		candidate = "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaultic"
		binary = "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaulticdb"
	}
	for path, expected := range map[string]string{candidate: os.Getenv("VAULTIC_PHASE35_THROUGHPUT_CANDIDATE_SHA256"), binary: os.Getenv("VAULTIC_PHASE35_THROUGHPUT_NATIVE_SHA256")} {
		contents, err := os.ReadFile(path)
		rtest.OK(t, err)
		if fmt.Sprintf("%x", sha256.Sum256(contents)) != expected {
			t.Fatal("retry executable pin differs")
		}
	}
	controlRoot := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_CONTROL_ROOT")
	resolved, err := filepath.EvalSymlinks(controlRoot)
	rtest.OK(t, err)
	info, err := os.Stat(controlRoot)
	rtest.OK(t, err)
	if resolved != controlRoot || !strings.HasPrefix(controlRoot, "/run/vaultic-unchanged-") || info.Mode().Perm() != 0700 {
		t.Fatal("retry requires a private canonical runtime")
	}
	control, err := os.MkdirTemp(controlRoot, "fc-")
	rtest.OK(t, err)
	rtest.OK(t, os.Chmod(control, 0700))
	t.Cleanup(func() { _ = os.RemoveAll(control) })
	current := daemon.Options{Socket: filepath.Join(control, "db.sock"), RepositoryID: "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528",
		DaemonPath: binary, DataDir: filepath.Join(parentCell, "db"), WALDataDir: filepath.Join(parentCell, "db", "wal"),
		ObjectStore: "local", WALStore: "local", WALFlushInterval: 100 * time.Millisecond, MetaCacheBytes: 128 << 20, BlockCacheBytes: 512 << 20,
		EncryptionMode: "required", PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery", StartTimeout: 90 * time.Second}
	current.EngineMultiGet = mode == "coalesced"
	if dbTuning != nil {
		current.MetaCacheBytes, current.BlockCacheBytes = dbTuning.MetaCacheBytes, dbTuning.BlockCacheBytes
	}
	options := publicationQualificationOptions(filepath.Join(parentCell, "repo"), current.Socket, current.PassphraseFile)
	rtest.OK(t, publicationQualificationLoadPassword(&options))
	cell := filepath.Join(root, "group-64")
	rtest.OK(t, os.Mkdir(cell, 0700))
	rtest.OK(t, os.Chmod(cell, 0700))
	for _, name := range []string{"result.json", "backup.stdout.log", "backup.stderr.log"} {
		publicationQualificationCopy(t, filepath.Join(cell, name), filepath.Join(parentCell, name))
	}
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	var owner *daemon.Client
	defer func() {
		if owner != nil {
			rtest.OK(t, owner.Close(context.Background()))
		}
	}()
	publicationQualificationForceComparison(t, cell, source, candidate, options, current, &owner, parent, paths, digest, regularFiles, files, size, extraFlags...)
}

func publicationQualificationForceComparison(t *testing.T, cell, source, candidate string, options global.Options,
	daemonOptions daemon.Options, owner **daemon.Client, parent vaultic.ID, paths map[string]bool, digest string,
	regularFiles, files, size uint64, extraFlags ...string,
) {
	t.Helper()
	if len(paths) != 10000 && len(paths) != 50000 {
		t.Fatal("force comparison requires exactly 10000 or 50000 entries")
	}
	parentOutput, err := os.ReadFile(filepath.Join(filepath.Dir(daemonOptions.DataDir), "backup.stdout.log"))
	rtest.OK(t, err)
	var parentSummary m5BackupSummary
	for _, line := range bytes.Split(parentOutput, []byte{'\n'}) {
		var row m5BackupSummary
		if json.Unmarshal(line, &row) == nil && row.MessageType == "summary" {
			parentSummary = row
		}
	}
	if parentSummary.MessageType != "summary" || parentSummary.TotalFilesProcessed != regularFiles {
		t.Fatal("parent fixture did not establish complete source content")
	}
	if *owner != nil {
		rtest.OK(t, (*owner).Close(context.Background()))
		*owner = nil
	}
	repositoryHash := fmt.Sprintf("%x", sha256.Sum256([]byte(daemonOptions.RepositoryID)))
	modes := []bool{false, true}
	if forceOnly := os.Getenv("VAULTIC_PHASE35_METADATA_READ_FORCE_ONLY"); forceOnly != "" {
		if forceOnly != "1" || len(paths) != 50000 || os.Getenv("VAULTIC_PHASE35_METADATA_READ_CONCURRENCY_PROFILE") == "" {
			t.Fatal("forced-only measurement requires the guarded concurrency screen")
		}
		modes = []bool{true}
	}
	for _, forced := range modes {
		name := "unforced"
		if forced {
			name = "forced"
		}
		leg := filepath.Join(cell, name)
		rtest.OK(t, os.Mkdir(leg, 0700))
		rtest.OK(t, os.Chmod(leg, 0700))
		for _, pair := range [][2]string{{daemonOptions.DataDir, filepath.Join(leg, "db")}, {options.Repo, filepath.Join(leg, "repo")}} {
			rtest.OK(t, os.Mkdir(pair[1], 0700))
			publicationQualificationCommand(t, leg, "copy-"+filepath.Base(pair[1]), "rsync", "-a", "--chmod=D700,F600", "--exclude=*.coordination.lock", pair[0]+"/", pair[1]+"/")
			publicationQualificationCommand(t, leg, "checksum-"+filepath.Base(pair[1]), "rsync", "-rcni", "--delete", "--exclude=*.coordination.lock", pair[0]+"/", pair[1]+"/")
			check, err := os.ReadFile(filepath.Join(leg, "checksum-"+filepath.Base(pair[1])+".stdout.log"))
			rtest.OK(t, err)
			if len(bytes.TrimSpace(check)) != 0 {
				t.Fatal("comparison parent clone checksum differs")
			}
			rtest.OK(t, os.Chmod(pair[1], 0700))
		}
		current := daemonOptions
		current.DataDir, current.WALDataDir = filepath.Join(leg, "db"), filepath.Join(leg, "db", "wal")
		control, err := os.MkdirTemp(filepath.Dir(daemonOptions.Socket), "fc-")
		rtest.OK(t, err)
		rtest.OK(t, os.Chmod(control, 0700))
		t.Cleanup(func() { _ = os.RemoveAll(control) })
		current.Socket = filepath.Join(control, "db.sock")
		binding := filepath.Join(current.DataDir, repositoryHash, "_vaultic", "wal-target-v1")
		copied, err := os.ReadFile(binding)
		rtest.OK(t, err)
		if string(copied) != "vaulticdb-local-wal-after-bulk-import-v1:"+daemonOptions.DataDir+"/wal" {
			t.Fatal("comparison copied WAL binding differs")
		}
		rtest.OK(t, os.WriteFile(binding, []byte("vaulticdb-local-wal-after-bulk-import-v1:"+current.DataDir+"/wal"), 0600))
		*owner, err = daemon.Ensure(t.Context(), current)
		rtest.OK(t, err)
		legOptions := publicationQualificationOptions(filepath.Join(leg, "repo"), current.Socket, current.PassphraseFile)
		rtest.OK(t, publicationQualificationLoadPassword(&legOptions))
		before, err := (*owner).WriterStatus(t.Context())
		rtest.OK(t, err)
		if before.ActiveTransactions != 0 || before.ActiveWriteIntents != 0 {
			t.Fatal("comparison writer is not idle")
		}
		args := []string{"--repo=" + legOptions.Repo, "--password-file=" + legOptions.PasswordFile, "--metadata-daemon-socket=" + current.Socket,
			"--metadata-encryption-mode=required", "--metadata-passphrase-file=" + current.PassphraseFile, "--no-cache", "--json",
			"backup", "--no-scan", "--one-file-system", "--read-concurrency=16", "--metadata-publication-group-size=64",
			"--metadata-reconciliation-queue-depth=256", "--metadata-on-demand", "--metadata-cache-mib=64",
			"--working-memory-mode=ram", "--working-memory-ram-limit=160GiB", "--working-memory-scratch=" + filepath.Join(legOptions.Repo, "working"), "--working-memory-disk-budget=2GiB"}
		args = append(args, extraFlags...)
		if forced {
			args = append(args, "--force")
		} else {
			args = append(args, "--parent="+parent.String())
		}
		args = append(args, source)
		wall, cpu := publicationQualificationCommand(t, leg, "backup", candidate, args...)
		output, err := os.ReadFile(filepath.Join(leg, "backup.stdout.log"))
		rtest.OK(t, err)
		var summary m5BackupSummary
		var measurement unchangedCrawlMeasurement
		var snapshotID string
		var critical json.RawMessage
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			var event struct {
				MessageType string `json:"message_type"`
				SnapshotID  string `json:"snapshot_id"`
			}
			if json.Unmarshal(line, &event) != nil {
				continue
			}
			if event.MessageType == "summary" {
				rtest.OK(t, json.Unmarshal(line, &summary))
				snapshotID = event.SnapshotID
			}
			if event.MessageType == "backup_critical_path" {
				rtest.OK(t, json.Unmarshal(line, &measurement))
				critical = append(critical[:0], line...)
			}
		}
		rtest.OK(t, validateForceComparisonMeasurement(summary, measurement, regularFiles, uint64(len(paths)), forced))
		id, err := vaultic.ParseID(snapshotID)
		rtest.OK(t, err)
		snapshot := testLoadSnapshot(t, legOptions, id)
		if snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != "full" || snapshot.CrawlPlan.ReusedSubtrees != 0 || len(snapshot.Paths) != 1 || snapshot.Paths[0] != source {
			t.Fatal("comparison snapshot scope differs")
		}
		if (!forced && (snapshot.Parent == nil || *snapshot.Parent != parent)) || (forced && snapshot.Parent != nil) {
			t.Fatal("comparison parent semantics differ")
		}
		after, err := (*owner).WriterStatus(t.Context())
		rtest.OK(t, err)
		if after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
			t.Fatal("comparison left active writer work")
		}
		restored := filepath.Join(leg, "restore")
		rtest.OK(t, testRunRestoreAssumeFailure(t, id.String(), restoreOptions{Target: restored, Verify: true}, legOptions))
		actual, actualFiles, actualBytes := m5FilesSelected(t, filepath.Join(restored, strings.TrimPrefix(source, "/")), false, paths)
		retained, retainedFiles, retainedBytes := m5FilesSelected(t, source, false, paths)
		if actual != digest || retained != digest || actualFiles != files || retainedFiles != files || actualBytes != size || retainedBytes != size {
			t.Fatal("comparison restore or source changed")
		}
		rtest.OK(t, publicationQualificationReopen(t.Context(), owner, current))
		reopened := testLoadSnapshot(t, legOptions, id)
		if reopened.Tree == nil || snapshot.Tree == nil || *reopened.Tree != *snapshot.Tree {
			t.Fatal("comparison encrypted reopen differs")
		}
		result, err := json.MarshalIndent(map[string]any{"forced": forced, "main_seconds": measurement.SecondsElapsed, "command_seconds": wall.Seconds(),
			"crypto_threads_requested": os.Getenv("VAULTICDB_CRYPTO_THREADS"),
			"candidate":                candidate, "metadata_read_flags": extraFlags, "engine_multi_get": current.EngineMultiGet,
			"metadata_cache_bytes": current.MetaCacheBytes, "block_cache_bytes": current.BlockCacheBytes,
			"entries": len(paths), "regular_files": regularFiles, "bytes": size, "input_sha256": digest, "entries_per_second": float64(len(paths)) / measurement.SecondsElapsed,
			"cli_cpu_seconds": cpu, "summary": summary, "critical_path": critical, "writer_before": before, "writer_after": after,
			"baseline_parent": parent.String(), "parent_setup_summary": parentSummary, "snapshot": id.String(), "restore_verified": true, "reopen_verified": true, "source_unchanged": true,
			"source_payload_reads_measured": false, "production_repository_modified": false, "qualification_limit": "single pair; frozen restored-source NFS fixture, not original source paths/inodes"}, "", "  ")
		rtest.OK(t, err)
		file, err := os.OpenFile(filepath.Join(leg, "result.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		rtest.OK(t, err)
		rtest.OK(t, file.Chmod(0600))
		_, err = file.Write(append(result, '\n'))
		rtest.OK(t, errors.Join(err, file.Sync(), file.Close()))
		t.Logf("qualified_force_comparison forced=%t main_seconds=%.3f entries_per_second=%.3f data_blobs=%d", forced, measurement.SecondsElapsed, float64(len(paths))/measurement.SecondsElapsed, summary.DataBlobs)
		rtest.OK(t, (*owner).Close(context.Background()))
		*owner = nil
	}
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
	return m5FilesSelected(t, root, excludeMarkers, nil)
}

func m5FilesSelected(t *testing.T, root string, excludeMarkers bool, selected map[string]bool) (string, uint64, uint64) {
	t.Helper()
	hash := sha256.New()
	var files, size uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if selected != nil && !selected[relative] {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
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
			if selected != nil && info.Mode()&os.ModeSymlink != 0 {
				target, err := os.Readlink(path)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(hash, "symlink:%d:%s:%d:%s:", len(relative), relative, len(target), target)
				files++
				return nil
			}
			return fmt.Errorf("unexpected nonregular M5 fixture entry")
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
	MessageType         string  `json:"message_type"`
	New                 uint64  `json:"files_new"`
	Changed             uint64  `json:"files_changed"`
	Unmodified          uint64  `json:"files_unmodified"`
	DataBlobs           uint64  `json:"data_blobs"`
	DataAdded           uint64  `json:"data_added"`
	TotalFilesProcessed uint64  `json:"total_files_processed"`
	TotalDuration       float64 `json:"total_duration"`
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

func TestUnchangedFullCrawlNoFileDataTransfer(t *testing.T) {
	t.Setenv("VAULTIC_PASSWORD", rtest.TestPassword)
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, environment)
	source := filepath.Join(t.TempDir(), "source")
	rtest.OK(t, os.Mkdir(source, 0700))
	const files = 128
	for ordinal := range files {
		rtest.OK(t, os.WriteFile(filepath.Join(source, fmt.Sprintf("file-%04d", ordinal)),
			fmt.Appendf(nil, "unchanged-content-%04d", ordinal), 0600))
	}
	args := []string{"backup", "--host=unchanged-crawl-fixture", "--no-scan", source}
	_, first, err := m5Execute(t, environment.globalOptions, args...)
	rtest.OK(t, err)
	if first.New != files || first.DataBlobs == 0 {
		t.Fatalf("initial backup did not write the fixture: %+v", first)
	}
	parent := testListSnapshots(t, environment.globalOptions, 1)[0]
	started := time.Now()
	_, second, err := m5Execute(t, environment.globalOptions, args...)
	wall := time.Since(started)
	rtest.OK(t, err)
	if second.New != 0 || second.Changed != 0 || second.Unmodified != files ||
		second.TotalFilesProcessed != files || second.DataBlobs != 0 {
		t.Fatalf("unchanged full crawl wrote file data or missed files: %+v", second)
	}
	ids := testListSnapshots(t, environment.globalOptions, 2)
	var latest *data.Snapshot
	for _, id := range ids {
		if id != parent {
			latest = testLoadSnapshot(t, environment.globalOptions, id)
		}
	}
	if latest == nil || latest.Parent == nil || *latest.Parent != parent || latest.CrawlPlan == nil ||
		latest.CrawlPlan.Mode != "full" || latest.CrawlPlan.ReusedSubtrees != 0 {
		t.Fatal("unchanged backup did not perform a full crawl against the prior snapshot")
	}
	t.Logf("unchanged_full_crawl files=%d data_blobs=%d metadata_and_pack_bytes=%d archive_seconds=%.6f command_seconds=%.6f",
		files, second.DataBlobs, second.DataAdded, second.TotalDuration, wall.Seconds())
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
	if scenario == "unchanged" && timedErr == nil && (backupSummary.New != 0 || backupSummary.Changed != 0 ||
		backupSummary.Unmodified != files || backupSummary.TotalFilesProcessed != files || backupSummary.DataBlobs != 0) {
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
