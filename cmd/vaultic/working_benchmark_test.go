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
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/cmd/vaultic/backupcmd"
	"github.com/otuschhoff/vaultic/internal/backend/all"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/fs"
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

func TestPublicationFollowupTelemetryNative(t *testing.T) {
	if os.Getenv("VAULTIC_METADATA_READ_QUALIFY") != "1" {
		t.Skip("isolated follow-up telemetry qualification is opt-in")
	}
	testPublicationReadCandidate(t, "/root/proj/vaultic/bin/phase35-forced-followup/linux-amd64/vaultic",
		[]string{"--read-concurrency=64", "--metadata-read-concurrency=32", "--metadata-batch-delay=1ms", "--metadata-publication-group-size=64"}, 2<<30, true)
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
				Observed          uint64                     `json:"observed"`
				Scanned           uint64                     `json:"scanned"`
				Failed            uint64                     `json:"failed"`
				Deferred          uint64                     `json:"deferred"`
				PublicationStages *daemon.PublicationMetrics `json:"publication_stages"`
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
		if candidate == "/root/proj/vaultic/bin/phase35-forced-followup/linux-amd64/vaultic" &&
			(status.Attribution.EncryptionReads == nil || status.Attribution.CacheReads == nil || main.Reconciliation.PublicationStages == nil) {
			return fmt.Errorf("authenticated follow-up transport omitted cache, encryption or publication attribution")
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

func publicationBackupRuntimeLimited(name, variant string) bool {
	return name == "backup" && publicationCache400kVariant(variant)
}

func publicationBackupRuntimeSeconds(name, variant, profile, stage string) int {
	if name == "backup" && profile == "db-scale800k" && stage == "forced" {
		return 1200
	}
	if publicationBackupRuntimeLimited(name, variant) {
		return 600
	}
	return 0
}

func TestPublicationScale800kPolicy(t *testing.T) {
	for _, stage := range []string{"backup", "copy-db", "restore", "reopen", "preflight"} {
		want := 0
		if stage == "backup" {
			want = 1200
		}
		if got := publicationBackupRuntimeSeconds(stage, "", "db-scale800k", "forced"); got != want {
			t.Fatalf("800k stage %s deadline = %d, want %d", stage, got, want)
		}
	}
	if publicationBackupRuntimeSeconds("backup", "", "db-scale800k", "parent-setup") != 0 {
		t.Fatal("800k parent setup incorrectly consumes the measured backup deadline")
	}
	tuning, err := publicationDBProfile("db-scale800k")
	rtest.OK(t, err)
	rtest.OK(t, backupcmd.NewCommand(&global.Options{}).ParseFlags(tuning.Flags))
	for _, flag := range []string{"--read-concurrency=128", "--metadata-publication-group-size=64", "--metadata-reconciliation-read-ahead=1", "--metadata-reconciliation-batch-size=5000"} {
		if !slices.Contains(tuning.Flags, flag) {
			t.Fatalf("800k tuning lacks %s", flag)
		}
	}
}

func TestPublicationBackupRuntimeScope(t *testing.T) {
	for _, variant := range []string{"N", "O", "P", "Q", "R", "S"} {
		if !publicationBackupRuntimeLimited("backup", variant) {
			t.Fatalf("cache backup %s lacks runtime guard", variant)
		}
		for _, stage := range []string{"copy-db", "restore", "reopen", "preflight"} {
			if publicationBackupRuntimeLimited(stage, variant) {
				t.Fatalf("non-backup stage %s is timed", stage)
			}
		}
	}
	if publicationBackupRuntimeLimited("backup", "K") {
		t.Fatal("historical comparison unexpectedly changed")
	}
}

func publicationRuntimeReceipt(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	rtest.OK(t, err)
	pending := path + ".pending"
	file, err := os.OpenFile(pending, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	rtest.OK(t, err)
	rtest.OK(t, file.Chmod(0600))
	_, err = file.Write(append(encoded, '\n'))
	closeErr := file.Close()
	rtest.OK(t, errors.Join(err, closeErr))
	rtest.OK(t, os.Link(pending, path))
	rtest.OK(t, os.Remove(pending))
}

func publicationQualificationCommand(t *testing.T, directory, name, binary string, args ...string) (time.Duration, float64) {
	t.Helper()
	stageTimeout := 4 * time.Hour
	profile := os.Getenv("VAULTIC_PHASE35_METADATA_READ_CONCURRENCY_PROFILE")
	if profile == "db-scale800k" {
		stageTimeout *= 2
	}
	ctx, cancel := context.WithTimeout(t.Context(), stageTimeout)
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
	limit := publicationBackupRuntimeSeconds(name, os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT"), profile, filepath.Base(directory))
	guarded := limit > 0
	if guarded {
		publicationRuntimeReceipt(t, filepath.Join(directory, "backup.runtime-start.json"), map[string]any{"limit_seconds": limit})
	}
	started := time.Now()
	if err := command.Run(); err != nil {
		t.Fatalf("qualification stage %s failed: %v; inspect private logs at %s", name, err, directory)
	}
	elapsed := time.Since(started)
	if guarded {
		publicationRuntimeReceipt(t, filepath.Join(directory, "backup.runtime-finished.json"), map[string]any{"seconds": elapsed.Seconds()})
		if elapsed > time.Duration(limit)*time.Second {
			t.Fatalf("backup runtime exceeded the %d-second limit", limit)
		}
	}
	return elapsed, (command.ProcessState.UserTime() + command.ProcessState.SystemTime()).Seconds()
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

const publicationScale200kRoot = "/volume2/NASDA2/vaultic-test-m6/forced-scale-fme-cae1-20261104-200k-retry2"

const publicationChunk200kRoot = "/volume2/NASDA2/vaultic-test-m6/chunk-compare-fme-cae1-20261104-200k"

const publicationImmutable200kRoot = "/volume2/NASDA2/vaultic-test-m6/immutable-default-fme-cae1-20261105-200k-retry1"

const publicationScale400kRoot = "/volume2/NASDA2/vaultic-test-m6/reconcile-parent-fme-cae1-20261105-400k"
const publicationScale800kParent = "/volume2/NASDA2/vaultic-test-m6/bottleneck-parent-fme-cae1-20261109-800k-retry2"
const publicationScale800kRoot = "/volume2/NASDA2/vaultic-test-m6/bottleneck-parent-fme-cae1-20261109-800k-retry3"
const publicationScale800kBin = "/root/proj/vaultic/bin/phase35-bottleneck800k-20261109-retry3-final/linux-amd64"
const publicationRAM32800kRoot = "/volume2/NASDA2/vaultic-test-m6/ram32-read-fme-cae1-20261110-800k"
const publicationRAM32800kBin = "/root/proj/vaultic/bin/phase35-ram32800k-20261110/linux-amd64"
const publicationReaders800kRoot = "/volume2/NASDA2/vaultic-test-m6/ncl1-readers-fme-cae1-20261111-800k-retry3"
const publicationReaders800kBin = publicationOriginal800kBin
const publicationOriginal800kParent = "/volume2/NASDA2/vaultic-test-m6/ncl1-parent-fme-cae1-20261111-800k-retry2"
const publicationOriginal800kBin = "/root/proj/vaultic/bin/phase35-ncl1-readers800k-20261111-retry3/linux-amd64"
const publicationRAM64800kRoot = "/volume2/NASDA2/vaultic-test-m6/ncl1-ram64-fme-cae1-20261112-800k"
const publicationRAM64800kBin = "/root/proj/vaultic/bin/phase35-ncl1-ram64800k-20261112/linux-amd64"

func publicationOriginalSourceLeg() bool {
	return os.Getenv("VAULTIC_PHASE35_READERS_LEG") != "" || os.Getenv("VAULTIC_PHASE35_RAM64_LEG") != ""
}

func publicationRAM64Leg(root, label string) (int, error) {
	if os.Getenv("VAULTIC_PHASE35_READERS_LEG") != "" || os.Getenv("VAULTIC_PHASE35_RAM32_LEG") != "" {
		return 0, fmt.Errorf("RAM64 comparison cannot be combined with another experiment")
	}
	for index, gib := range []int{16, 64, 64, 16, 64, 16, 16, 64} {
		expected := fmt.Sprintf("leg-%02d-%dg", index+1, gib)
		if label == expected && root == filepath.Join(publicationRAM64800kRoot, expected) {
			return gib, nil
		}
	}
	return 0, fmt.Errorf("RAM64 comparison requires its exact scheduled private leg")
}

func TestPublicationRAM64Policy(t *testing.T) {
	t.Setenv("VAULTIC_PHASE35_READERS_LEG", "")
	t.Setenv("VAULTIC_PHASE35_RAM32_LEG", "")
	t.Setenv("VAULTIC_PHASE35_RAM64_LEG", "")
	baseline, err := publicationDBProfile("db-scale800k")
	rtest.OK(t, err)
	for index, gib := range []int{16, 64, 64, 16, 64, 16, 16, 64} {
		label := fmt.Sprintf("leg-%02d-%dg", index+1, gib)
		root := filepath.Join(publicationRAM64800kRoot, label)
		got, err := publicationRAM64Leg(root, label)
		rtest.OK(t, err)
		if got != gib {
			t.Fatal("RAM64 capacity routing differs")
		}
		if _, err := publicationRAM64Leg(root+"/retry", label); err == nil {
			t.Fatal("RAM64 comparison accepted a child path")
		}
		t.Setenv("VAULTIC_PHASE35_RAM64_LEG", label)
		t.Setenv("VAULTIC_PHASE35_THROUGHPUT_ROOT", root)
		tuning, err := publicationDBProfile("db-scale800k")
		rtest.OK(t, err)
		if !slices.Equal(tuning.Flags, baseline.Flags) || tuning.MetaCacheBytes != baseline.MetaCacheBytes ||
			tuning.BlockCacheBytes != baseline.BlockCacheBytes || tuning.CryptoThreads != baseline.CryptoThreads {
			t.Fatal("RAM64 comparison changed a non-cache setting")
		}
		command := backupcmd.NewCommand(&global.Options{})
		rtest.OK(t, command.ParseFlags(tuning.Flags))
	}
	t.Setenv("VAULTIC_PHASE35_READERS_LEG", "leg-01-r64")
	if _, err := publicationDBProfile("db-scale800k"); err == nil {
		t.Fatal("RAM64 comparison accepted simultaneous reader tuning")
	}
}

func validatePublicationBackupSource(source string) error {
	if source != "/ncl1-1-vs-50/fme_cae1" {
		return fmt.Errorf("backup test source must be the original /ncl1-1-vs-50/fme_cae1 mount; qualify a matching parent first")
	}
	return nil
}

func TestPublicationBackupSourcePolicy(t *testing.T) {
	rtest.OK(t, validatePublicationBackupSource("/ncl1-1-vs-50/fme_cae1"))
	for _, source := range []string{publicationScale800kParent + "/input", publicationReaders800kRoot + "/input",
		"/volume2/NASDA2/restore/ncl1-1-vs-50/fme_cae1", "/ncl1-1-vs-50/fme_cae1/child", "/ncl1-1-vs-50/other"} {
		if validatePublicationBackupSource(source) == nil {
			t.Fatalf("accepted substituted backup source %q", source)
		}
	}
}

func publicationReadersLeg(root, label string) (int, error) {
	if os.Getenv("VAULTIC_PHASE35_RAM32_LEG") != "" || os.Getenv("VAULTIC_PHASE35_RAM64_LEG") != "" {
		return 0, fmt.Errorf("reader and cache experiments cannot be combined")
	}
	for index, readers := range []int{64, 96, 128, 128, 96, 64, 96, 128, 64, 64, 128, 96} {
		expected := fmt.Sprintf("leg-%02d-r%d", index+1, readers)
		if label == expected && root == filepath.Join(publicationReaders800kRoot, expected) {
			return readers, nil
		}
	}
	return 0, fmt.Errorf("reader comparison requires its exact scheduled private leg")
}

func TestPublicationReadersPolicy(t *testing.T) {
	t.Setenv("VAULTIC_PHASE35_READERS_LEG", "")
	t.Setenv("VAULTIC_PHASE35_RAM32_LEG", "")
	baseline, err := publicationDBProfile("db-scale800k")
	rtest.OK(t, err)
	for index, readers := range []int{64, 96, 128, 128, 96, 64, 96, 128, 64, 64, 128, 96} {
		label := fmt.Sprintf("leg-%02d-r%d", index+1, readers)
		root := filepath.Join(publicationReaders800kRoot, label)
		got, err := publicationReadersLeg(root, label)
		rtest.OK(t, err)
		if got != readers {
			t.Fatal("reader routing differs")
		}
		if _, err := publicationReadersLeg(root+"/retry", label); err == nil {
			t.Fatal("reader comparison accepted a child path")
		}
		t.Setenv("VAULTIC_PHASE35_READERS_LEG", label)
		t.Setenv("VAULTIC_PHASE35_THROUGHPUT_ROOT", root)
		tuning, err := publicationDBProfile("db-scale800k")
		rtest.OK(t, err)
		if tuning.Flags[0] != fmt.Sprintf("--read-concurrency=%d", readers) ||
			!slices.Equal(tuning.Flags[1:], baseline.Flags[1:]) ||
			tuning.MetaCacheBytes != baseline.MetaCacheBytes || tuning.BlockCacheBytes != baseline.BlockCacheBytes ||
			tuning.CryptoThreads != baseline.CryptoThreads {
			t.Fatal("reader comparison changed another setting")
		}
		command := backupcmd.NewCommand(&global.Options{})
		rtest.OK(t, command.ParseFlags(tuning.Flags))
	}
	if _, err := publicationReadersLeg(filepath.Join(publicationReaders800kRoot, "leg-13-r64"), "leg-13-r64"); err == nil {
		t.Fatal("reader comparison accepted an unscheduled leg")
	}
	t.Setenv("VAULTIC_PHASE35_RAM32_LEG", "leg-01-16g")
	if _, err := publicationDBProfile("db-scale800k"); err == nil {
		t.Fatal("reader comparison accepted simultaneous cache tuning")
	}
}

func validatePublicationRAM32Leg(root, label string) error {
	if os.Getenv("VAULTIC_PHASE35_RAM64_LEG") != "" || os.Getenv("VAULTIC_PHASE35_READERS_LEG") != "" {
		return fmt.Errorf("RAM32 comparison cannot be combined with another experiment")
	}
	for index, gib := range []int{16, 32, 32, 16, 32, 16, 16, 32} {
		expected := fmt.Sprintf("leg-%02d-%dg", index+1, gib)
		if label == expected && root == filepath.Join(publicationRAM32800kRoot, expected) {
			return nil
		}
	}
	return fmt.Errorf("RAM32 comparison requires its exact scheduled private leg")
}

func TestPublicationRAM32Policy(t *testing.T) {
	for index, gib := range []int{16, 32, 32, 16, 32, 16, 16, 32} {
		label := fmt.Sprintf("leg-%02d-%dg", index+1, gib)
		root := filepath.Join(publicationRAM32800kRoot, label)
		rtest.OK(t, validatePublicationRAM32Leg(root, label))
		if validatePublicationRAM32Leg(root+"/retry", label) == nil || validatePublicationRAM32Leg(root, "leg-09-16g") == nil {
			t.Fatal("RAM32 scope accepted an unscheduled path or label")
		}
	}
}

const publicationRead400kRoot = "/volume2/NASDA2/vaultic-test-m6/reconcile-read-fme-cae1-20261105-400k"
const publicationRead400kBin = "/root/proj/vaultic/bin/phase35-reconcile400k/linux-amd64"
const publicationCache400kRoot = "/volume2/NASDA2/vaultic-test-m6/cache-read-fme-cae1-20261107-400k-retry4"
const publicationCache400kBin = "/root/proj/vaultic/bin/phase35-cache400k-retry4-guard600/linux-amd64"
const publicationCache400kParent = "/volume2/NASDA2/vaultic-test-m6/cache-parent-fme-cae1-20261107-400k"
const publicationPipeline400kRoot = "/volume2/NASDA2/vaultic-test-m6/pipeline-read-fme-cae1-20261108-400k"
const publicationPipeline400kBin = "/root/proj/vaultic/bin/phase35-pipeline400k-20261108/linux-amd64"
const publicationRAM16400kRoot = "/volume2/NASDA2/vaultic-test-m6/ram16-read-fme-cae1-20261109-400k"
const publicationRAM16400kBin = "/root/proj/vaultic/bin/phase35-ram16400k-20261109/linux-amd64"

func publicationRAM16400kVariant(variant string) bool {
	return variant == "Z" || variant == "AA"
}

func publicationPipeline400kVariant(variant string) bool {
	return variant == "T" || variant == "U" || variant == "V" || variant == "W" || variant == "X" || variant == "Y" || publicationRAM16400kVariant(variant)
}

func publicationPipeline400kTuning(variant string) (publicationDBTuning, error) {
	if !publicationPipeline400kVariant(variant) {
		return publicationDBTuning{}, fmt.Errorf("unknown pipeline variant %q", variant)
	}
	tuning, err := publicationDBProfile("db-scale400k")
	if err != nil {
		return tuning, err
	}
	if variant != "T" && variant != "U" {
		tuning.Flags[3] = "--metadata-publication-group-size=64"
	}
	if variant == "W" || variant == "X" || variant == "Y" || publicationRAM16400kVariant(variant) {
		tuning.Flags = append(tuning.Flags, "--metadata-reconciliation-read-ahead=1")
	}
	if variant == "X" || variant == "Y" {
		tuning.Flags = append(tuning.Flags, "--metadata-reconciliation-batch-size=10000")
	}
	if variant == "Y" {
		tuning.MetaCacheBytes, tuning.BlockCacheBytes = 8<<30, 8<<30
		tuning.Flags = append(tuning.Flags, "--metadata-cache-mib=8192")
	}
	return tuning, nil
}

func TestPublicationPipelinePolicy(t *testing.T) {
	schedule := []string{"T", "U", "U", "T", "U", "V", "V", "U", "V", "W", "W", "V", "W", "X", "X", "W", "X", "Y", "Y", "X"}
	for index, variant := range schedule {
		root := filepath.Join(publicationPipeline400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, variant))
		rtest.OK(t, validatePublicationChunkLeg(root, variant))
		if validatePublicationChunkLeg(root+"/retry", variant) == nil || !publicationBackupRuntimeLimited("backup", variant) {
			t.Fatal("pipeline scope or backup deadline missing")
		}
		tuning, err := publicationPipeline400kTuning(variant)
		rtest.OK(t, err)
		group := "16"
		if variant != "T" && variant != "U" {
			group = "64"
		}
		if tuning.Flags[0] != "--read-concurrency=128" || tuning.Flags[1] != "--metadata-read-concurrency=32" ||
			tuning.Flags[3] != "--metadata-publication-group-size="+group || tuning.CryptoThreads != 8 {
			t.Fatal("pipeline reader or group settings differ")
		}
		flags := strings.Join(tuning.Flags, " ")
		if strings.Contains(flags, "--metadata-reconciliation-read-ahead=1") != (variant == "W" || variant == "X" || variant == "Y") ||
			strings.Contains(flags, "--metadata-reconciliation-batch-size=10000") != (variant == "X" || variant == "Y") ||
			strings.Contains(flags, "--metadata-cache-mib=8192") != (variant == "Y") {
			t.Fatal("pipeline collection settings differ")
		}
		if variant == "Y" && (tuning.MetaCacheBytes != 8<<30 || tuning.BlockCacheBytes != 8<<30 || publicationCache400kChunkBytes(variant) != "4294967296") {
			t.Fatal("RAM-heavy pipeline cache settings differ")
		}
	}
	if _, err := publicationPipeline400kTuning("S"); err == nil {
		t.Fatal("cache-only variant accepted as pipeline tuning")
	}
}

func publicationCache400kVariant(variant string) bool {
	return variant == "N" || variant == "O" || variant == "P" || variant == "Q" || variant == "R" || variant == "S" || publicationPipeline400kVariant(variant)
}

func TestPublicationRAM16Policy(t *testing.T) {
	for index, variant := range []string{"Z", "AA", "AA", "Z", "AA", "Z", "Z", "AA"} {
		root := filepath.Join(publicationRAM16400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, variant))
		rtest.OK(t, validatePublicationChunkLeg(root, variant))
		if validatePublicationChunkLeg(root+"/retry", variant) == nil || validatePublicationChunkLeg(filepath.Join(publicationPipeline400kRoot, filepath.Base(root)), variant) == nil || !publicationBackupRuntimeLimited("backup", variant) || publicationBackupRuntimeLimited("restore", variant) {
			t.Fatal("RAM16 scope or command deadline differs")
		}
		tuning, err := publicationPipeline400kTuning(variant)
		rtest.OK(t, err)
		if tuning.MetaCacheBytes != 2<<30 || tuning.BlockCacheBytes != 512<<20 || tuning.CryptoThreads != 8 ||
			strings.Join(tuning.Flags, " ") != "--read-concurrency=128 --metadata-read-concurrency=32 --metadata-batch-delay=1ms --metadata-publication-group-size=64 --metadata-reconciliation-read-ahead=1" {
			t.Fatal("RAM16 must change only ciphertext byte capacity")
		}
		expected := "4294967296"
		if variant == "AA" {
			expected = "17179869184"
		}
		if publicationCache400kChunkBytes(variant) != expected {
			t.Fatal("RAM16 ciphertext capacity differs")
		}
	}
}

func publicationCache400kChunkBytes(variant string) string {
	return map[string]string{"N": "67108864", "O": "67108864", "P": "67108864", "Q": "536870912", "R": "2147483648", "S": "4294967296",
		"T": "67108864", "U": "67108864", "V": "67108864", "W": "67108864", "X": "67108864", "Y": "4294967296", "Z": "4294967296", "AA": "17179869184"}[variant]
}

func publicationRead400kVariant(variant string) bool {
	return variant == "J" || variant == "K" || variant == "L" || variant == "M" || publicationCache400kVariant(variant)
}

func validatePublicationChunkLeg(root, variant string) error {
	if publicationRAM16400kVariant(variant) {
		for index, expected := range []string{"Z", "AA", "AA", "Z", "AA", "Z", "Z", "AA"} {
			if variant == expected && root == filepath.Join(publicationRAM16400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, expected)) {
				return nil
			}
		}
		return fmt.Errorf("RAM16 comparison requires its exact scheduled private leg")
	}
	if publicationPipeline400kVariant(variant) {
		for index, expected := range []string{"T", "U", "U", "T", "U", "V", "V", "U", "V", "W", "W", "V", "W", "X", "X", "W", "X", "Y", "Y", "X"} {
			if variant == expected && root == filepath.Join(publicationPipeline400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, expected)) {
				return nil
			}
		}
		return fmt.Errorf("pipeline comparison requires its exact scheduled private leg")
	}
	if publicationCache400kVariant(variant) {
		for index, expected := range []string{"N", "O", "O", "N", "N", "P", "P", "N", "N", "Q", "Q", "N", "N", "R", "R", "N", "N", "S", "S", "N"} {
			if variant == expected && root == filepath.Join(publicationCache400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, expected)) {
				return nil
			}
		}
	}
	if publicationRead400kVariant(variant) {
		for index, expected := range []string{"J", "K", "K", "J", "K", "L", "L", "K", "K", "M", "M", "K"} {
			if variant == expected && root == filepath.Join(publicationRead400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, expected)) {
				return nil
			}
		}
	}
	if variant == "I" {
		for index := 1; index <= 3; index++ {
			if root == filepath.Join(publicationImmutable200kRoot, fmt.Sprintf("leg-%02d-I", index)) {
				return nil
			}
		}
	}
	schedule := []string{"A", "B", "C", "C", "B", "A", "B", "C", "A", "A", "C", "B", "C", "D", "D", "C"}
	for index, expected := range schedule {
		if root == filepath.Join(publicationChunk200kRoot, fmt.Sprintf("leg-%02d-%s", index+1, expected)) && variant == expected {
			return nil
		}
	}
	return fmt.Errorf("chunk comparison requires an exact scheduled private 200k leg")
}

func TestPublicationChunkComparisonPolicy(t *testing.T) {
	for index, variant := range []string{"N", "O", "O", "N", "N", "P", "P", "N", "N", "Q", "Q", "N", "N", "R", "R", "N", "N", "S", "S", "N"} {
		root := filepath.Join(publicationCache400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, variant))
		if validatePublicationChunkLeg(root, variant) != nil || publicationCache400kChunkBytes(variant) == "" {
			t.Fatal("cache comparison routing or capacity missing")
		}
		if validatePublicationChunkLeg(root+"/retry", variant) == nil || validatePublicationChunkLeg(root, "K") == nil || validatePublicationChunkLeg(filepath.Join(publicationRead400kRoot, filepath.Base(root)), variant) == nil {
			t.Fatal("cache comparison accepted a mismatched root or variant")
		}
	}
	for index, variant := range []string{"J", "K", "K", "J", "K", "L", "L", "K", "K", "M", "M", "K"} {
		root := filepath.Join(publicationRead400kRoot, fmt.Sprintf("leg-%02d-%s", index+1, variant))
		if err := validatePublicationChunkLeg(root, variant); err != nil {
			t.Fatal(err)
		}
		if validatePublicationChunkLeg(root, "I") == nil || validatePublicationChunkLeg(root+"/retry", variant) == nil {
			t.Fatal("400k routing accepted scope mismatch")
		}
	}
	for _, item := range []struct {
		name, variant string
		valid         bool
	}{
		{"leg-01-A", "A", true}, {"leg-02-B", "B", true}, {"leg-03-C", "C", true},
		{"leg-14-D", "D", true}, {"leg-16-C", "C", true},
		{"leg-01-A", "C", false}, {"leg-17-C", "C", false}, {"leg-00-A", "A", false},
		{"leg-03-C/retry", "C", false}, {"leg-14-D", "E", false},
	} {
		err := validatePublicationChunkLeg(filepath.Join(publicationChunk200kRoot, item.name), item.variant)
		if (err == nil) != item.valid {
			t.Fatalf("chunk leg (%q, %q): %v", item.name, item.variant, err)
		}
	}
	if validatePublicationChunkLeg(publicationScale200kRoot, "A") == nil {
		t.Fatal("accepted historical root must not be writable comparison output")
	}
}

func publicationScaleInodeLimit(selector, root string) (uint64, error) {
	if selector == "" {
		return 100_000, nil
	}
	if selector == "800000" && (root == publicationScale800kRoot || root == publicationOriginal800kParent) {
		return 800_000, nil
	}
	if selector == "400000" && root == publicationScale400kRoot {
		return 400_000, nil
	}
	if selector != "200000" || root != publicationScale200kRoot {
		return 0, fmt.Errorf("200k scaling requires its explicit fresh private root")
	}
	return 200_000, nil
}

func validatePublicationCrawlMode(mode string, bounded bool) error {
	expected := "full"
	if bounded {
		expected = "bounded"
	}
	if mode != expected {
		return fmt.Errorf("snapshot crawl coverage %q differs from required %q", mode, expected)
	}
	return nil
}

func TestPublicationCrawlMode(t *testing.T) {
	for _, item := range []struct {
		mode           string
		bounded, valid bool
	}{
		{"full", false, true},
		{"bounded", true, true},
		{"full", true, false},
		{"bounded", false, false},
		{"", false, false},
		{"", true, false},
	} {
		if err := validatePublicationCrawlMode(item.mode, item.bounded); (err == nil) != item.valid {
			t.Fatalf("crawl coverage (%q, bounded=%v): %v", item.mode, item.bounded, err)
		}
	}
}

func TestPublicationScaleInodeLimit(t *testing.T) {
	for _, item := range []struct {
		selector, root string
		want           uint64
	}{
		{"", "legacy", 100_000},
		{"200000", publicationScale200kRoot, 200_000},
		{"400000", publicationScale400kRoot, 400_000},
		{"800000", publicationScale800kRoot, 800_000},
		{"800000", publicationOriginal800kParent, 800_000},
		{"800000", publicationOriginal800kParent + "/retry", 0},
		{"800000", publicationScale400kRoot, 0},
		{"400000", publicationScale800kRoot, 0},
		{"800000", publicationScale800kRoot + "/retry", 0},
		{"400000", publicationScale200kRoot, 0},
		{"200000", publicationScale400kRoot, 0},
		{"400000", publicationScale400kRoot + "/retry", 0},
		{"200000", "/volume2/NASDA2/vaultic-test-m6/forced-scale-fme-cae1-20261103-200k", 0},
		{"200000", "/volume2/NASDA2/vaultic-test-m6/forced-scale-fme-cae1-20261104-200k-retry1", 0},
		{"100000", publicationScale200kRoot, 0},
		{"200000", publicationScale200kRoot + "/retry", 0},
		{"600000", publicationScale200kRoot, 0},
	} {
		got, err := publicationScaleInodeLimit(item.selector, item.root)
		if (err != nil) != (item.want == 0) || got != item.want {
			t.Fatalf("scale policy (%q, %q) = %d, %v", item.selector, item.root, got, err)
		}
	}
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
	limit, err := publicationScaleInodeLimit(os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT"), root)
	rtest.OK(t, err)
	if root == publicationOriginal800kParent {
		rtest.OK(t, validatePublicationBackupSource("/ncl1-1-vs-50/fme_cae1"))
	}
	if limit >= 200_000 {
		rtest.OK(t, validatePublicationQualificationRoot(root, "/volume2/NASDA2/vaultic-test-m6"))
	} else {
		rtest.OK(t, os.Mkdir(root, 0700))
	}
	rtest.OK(t, os.Chmod(root, 0700))
	if limit < 200_000 {
		rtest.OK(t, os.WriteFile(filepath.Join(root, ".nobackup"), []byte("Disposable Phase 35 throughput qualification.\n"), 0600))
	}
	rtest.OK(t, os.Chmod(filepath.Join(root, ".nobackup"), 0600))
	manifest := backupcmd.BoundedInodeManifest{Version: 1, Source: "/ncl1-1-vs-50/fme_cae1", InodeLimit: limit}
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
	if limit >= 200_000 && root != publicationOriginal800kParent {
		original := manifest.Source
		manifest.Source = filepath.Join(root, "input")
		rtest.OK(t, os.Mkdir(manifest.Source, 0700))
		rtest.OK(t, os.Chmod(manifest.Source, 0700))
		selection := filepath.Join(root, "selected-paths.nul")
		rtest.OK(t, os.WriteFile(selection, []byte(strings.Join(manifest.Paths[1:], "\x00")+"\x00"), 0600))
		rtest.OK(t, os.Chmod(selection, 0600))
		publicationQualificationCommand(t, root, "freeze", "rsync", "-a", "--no-recursive", "--dirs", "--from0", "--files-from="+selection, original+"/", manifest.Source+"/")
		rtest.OK(t, os.Chmod(manifest.Source, 0700))
		paths, err := boundedLiveSourcePaths(manifest.Source, limit+1)
		rtest.OK(t, err)
		if !slices.Equal(paths, manifest.Paths) {
			t.Fatal("200k frozen traversal differs from the exact selected source")
		}
		frozenDigest, frozenFiles, frozenBytes := m5FilesSelected(t, manifest.Source, false, selected)
		retainedDigest, retainedFiles, retainedBytes := m5FilesSelected(t, original, false, selected)
		if frozenDigest != digest || retainedDigest != digest || frozenFiles != files || retainedFiles != files || frozenBytes != size || retainedBytes != size || publicationQualificationRegularFiles(t, manifest.Source, paths) != regularFiles {
			t.Fatal("200k frozen source or original changed during preparation")
		}
	}
	for name, value := range map[string]any{
		"inode-manifest.json":     manifest,
		"source-observation.json": map[string]any{"source": manifest.Source, "sha256": digest, "files": files, "regular_files": regularFiles, "bytes": size, "entries": len(manifest.Paths)},
	} {
		encoded, err := json.MarshalIndent(value, "", "  ")
		rtest.OK(t, err)
		rtest.OK(t, os.WriteFile(filepath.Join(root, name), append(encoded, '\n'), 0600))
		rtest.OK(t, os.Chmod(filepath.Join(root, name), 0600))
	}
	t.Logf("source=%s entries=%d cap=%d non_directory_entries=%d regular_files=%d bytes=%d sha256=%s", manifest.Source, len(manifest.Paths), manifest.InodeLimit, files, regularFiles, size, digest)
}

func publicationQualificationPreflightArgs(backupArgs []string) []string {
	preflightArgs := make([]string, 0, len(backupArgs)+1)
	for _, argument := range backupArgs {
		if argument != "--metadata-on-demand" && !strings.HasPrefix(argument, "--metadata-batch-delay=") {
			preflightArgs = append(preflightArgs, argument)
		}
	}
	return append(preflightArgs, "--dry-run")
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
	manifest := backupcmd.BoundedInodeManifest{Version: 1, Source: source, InodeLimit: 800_000, Paths: []string{".", "dir", "dir/keep", "dir/alias", "link"}}
	encoded, err := json.Marshal(manifest)
	rtest.OK(t, err)
	filename := filepath.Join(t.TempDir(), "manifest.json")
	rtest.OK(t, os.WriteFile(filename, encoded, 0600))
	preflightArgs := publicationQualificationPreflightArgs([]string{"--metadata-on-demand", "--metadata-batch-delay=1ms",
		"--metadata-cache-mib=64", "--bounded-inode-manifest=" + filename, "--force", source})
	if !slices.Equal(preflightArgs, []string{"--metadata-cache-mib=64", "--bounded-inode-manifest=" + filename, "--force", source, "--dry-run"}) {
		t.Fatal("bounded dry-run retained on-demand-only flags or changed its selection")
	}
	rtest.OK(t, withTermStatus(t, environment.globalOptions, func(ctx context.Context, options global.Options) error {
		command := backupcmd.NewCommand(&options)
		command.SetArgs(preflightArgs)
		command.SetContext(ctx)
		return command.Execute()
	}))
	testListSnapshots(t, environment.globalOptions, 0)
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
		Observed          uint64                     `json:"observed"`
		Scanned           uint64                     `json:"scanned"`
		Failed            uint64                     `json:"failed"`
		Deferred          uint64                     `json:"deferred"`
		WriterPhase       string                     `json:"writer_phase"`
		PublicationStages *daemon.PublicationMetrics `json:"publication_stages"`
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
	for _, entries := range []uint64{10000, 50000, 200000} {
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

func TestPublicationScale800kQualification(t *testing.T) {
	if os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT") != "800000" {
		t.Skip("800k scaling qualification not selected")
	}
	if !t.Run("prepare", TestPrepareFmeCAE1Qualification) {
		return
	}
	t.Run("backup", TestPublicationBackupQualification)
}

func TestPublicationScale400kQualification(t *testing.T) {
	if os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT") != "400000" {
		t.Skip("400k scaling qualification not selected")
	}
	if !t.Run("prepare", TestPrepareFmeCAE1Qualification) {
		return
	}
	t.Run("backup", TestPublicationBackupQualification)
}

func TestPublicationScale200kQualification(t *testing.T) {
	if os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT") != "200000" {
		t.Skip("200k scaling qualification not selected")
	}
	if !t.Run("prepare", TestPrepareFmeCAE1Qualification) {
		return
	}
	t.Run("backup", TestPublicationBackupQualification)
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
	scaleLimit, err := publicationScaleInodeLimit(os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT"), root)
	rtest.OK(t, err)
	scale200k := scaleLimit == 200_000 || scaleLimit == 400_000 || scaleLimit == 800_000
	originalParent := root == publicationOriginal800kParent
	scaleProfile := fmt.Sprintf("db-scale%dk", scaleLimit/1000)
	scaleBuild := "/root/proj/vaultic/bin/phase35-forced-followup/linux-amd64"
	if scaleLimit == 400_000 {
		scaleBuild = publicationRead400kBin
	}
	if scaleLimit == 800_000 {
		scaleBuild = publicationScale800kBin
		if originalParent {
			scaleBuild = publicationOriginal800kBin
		}
	}
	if scale200k && (onDemand != "1" || unchanged != "" || forceCompare != "" || resumeVerification != "" ||
		os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ORDER") != "64" || os.Getenv("VAULTIC_PHASE35_METADATA_READ_FORCE_ONLY") != "1" ||
		os.Getenv("VAULTIC_PHASE35_METADATA_READ_CONCURRENCY_PROFILE") != scaleProfile || os.Getenv("VAULTICDB_CRYPTO_THREADS") != "8") {
		t.Fatal("200k scaling requires a fresh forced-only single-cell profile")
	}
	expanded := os.Getenv("VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT")
	forceCompareLimit, err := publicationForceInodeLimit(expanded)
	rtest.OK(t, err)
	var metadataReadFlags []string
	if scale200k {
		tuning, err := publicationDBProfile(scaleProfile)
		rtest.OK(t, err)
		metadataReadFlags = tuning.Flags
	}
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
			!(expanded == "50000" && selected == "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaultic") &&
			!(scale200k && selected == filepath.Join(scaleBuild, "vaultic")) {
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
		if (forceCompare != "1" && !scale200k) || (selected != "/root/proj/vaultic/bin/phase35-force-10k/linux-amd64/vaulticdb" &&
			!(expanded == "50000" && selected == "/root/proj/vaultic/bin/phase35-metadata-read/linux-amd64/vaulticdb") &&
			!(scale200k && selected == filepath.Join(scaleBuild, "vaulticdb"))) {
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
	if scale200k && (candidate != filepath.Join(scaleBuild, "vaultic") || binary != filepath.Join(scaleBuild, "vaulticdb")) {
		t.Fatal("200k scaling requires the separately pinned telemetry builds")
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
		expectedSource := "/ncl1-1-vs-50/fme_cae1"
		if scale200k && !originalParent {
			expectedSource = filepath.Join(root, "input")
		}
		if os.Getenv("VAULTIC_PHASE35_THROUGHPUT_SOURCE") != expectedSource || onDemand != "1" || resumeVerification != "" {
			t.Fatal("bounded source requires the selected fme_cae1 root, on-demand lookup, and a fresh run")
		}
		encoded, err := os.ReadFile(filepath.Join(root, "inode-manifest.json"))
		rtest.OK(t, err)
		rtest.OK(t, json.Unmarshal(encoded, &sourceManifest))
		if sourceManifest.Source != expectedSource || sourceManifest.InodeLimit != scaleLimit || uint64(len(sourceManifest.Paths)) != scaleLimit {
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
		if scale200k {
			traversalLimit := scaleLimit + 1
			if originalParent {
				traversalLimit = scaleLimit
			}
			paths, err := boundedLiveSourcePaths(source, traversalLimit)
			rtest.OK(t, err)
			if !slices.Equal(paths, sourceManifest.Paths) || size > 64<<30 {
				t.Fatal("200k frozen traversal or byte ceiling differs")
			}
			frozenPaths = make(map[string]bool, len(paths))
			for _, path := range paths {
				frozenPaths[path] = true
			}
			actualDigest, actualFiles, actualBytes := m5FilesSelected(t, source, false, frozenPaths)
			if actualDigest != digest || actualFiles != files || actualBytes != size {
				t.Fatal("200k source changed after preparation")
			}
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
	if originalParent {
		rtest.OK(t, validatePublicationBackupSource(source))
		if sourceManifest.Source != source || sourceManifest.InodeLimit != 800_000 || len(frozenPaths) != 800_000 {
			t.Fatal("original-source parent requires its exact bounded manifest")
		}
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
			if unchanged != "" || scale200k {
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
			rtest.OK(t, os.Chmod(control, 0700))
			t.Cleanup(func() { _ = os.RemoveAll(control) })
			daemonOptions := daemon.Options{Socket: filepath.Join(control, "db.sock"), RepositoryID: repositoryID, DaemonPath: binary,
				DataDir: database, WALDataDir: filepath.Join(database, "wal"), ObjectStore: "local", WALStore: "local",
				WALFlushInterval: 100 * time.Millisecond, MetaCacheBytes: 128 << 20, BlockCacheBytes: 512 << 20,
				EncryptionMode: "required", PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery", StartTimeout: 90 * time.Second}
			daemonOptions.EngineMultiGet = expanded == "50000"
			if scale200k {
				daemonOptions.EngineMultiGet = true
				daemonOptions.MetaCacheBytes = 2 << 30
			}
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
				if scale200k {
					backupArgs = append(backupArgs, metadataReadFlags...)
					backupArgs = append(backupArgs, "--metadata-reconciliation-queue-depth=256", "--force")
				}
				if sourceManifest.Source != "" && (!scale200k || originalParent) {
					backupArgs = append(backupArgs, "--bounded-inode-manifest="+filepath.Join(root, "inode-manifest.json"), "--metadata-reconciliation-queue-depth=256", "--force")
				}
				backupArgs = append(backupArgs, source)
				if sourceManifest.Source != "" && (!scale200k || originalParent) {
					preflightArgs := publicationQualificationPreflightArgs(backupArgs)
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
				mode := ""
				if snapshot.CrawlPlan != nil {
					mode = snapshot.CrawlPlan.Mode
				}
				rtest.OK(t, validatePublicationCrawlMode(mode, !scale200k || originalParent))
				selected = make(map[string]bool, len(sourceManifest.Paths))
				for _, path := range sourceManifest.Paths {
					selected[path] = true
				}
			}
			actual, actualFiles, actualBytes := m5FilesSelected(t, restoredSource, false, selected)
			if actual != digest || actualFiles != files || actualBytes != size {
				t.Fatal("verified restore differs from frozen input")
			}
			if originalParent {
				currentDigest, currentFiles, currentBytes := m5FilesSelected(t, source, false, selected)
				if currentDigest != digest || currentFiles != files || currentBytes != size {
					t.Fatal("original source changed during parent qualification")
				}
			}
			started = time.Now()
			rtest.OK(t, publicationQualificationReopen(t.Context(), &client, daemonOptions))
			reopenSeconds := time.Since(started).Seconds()
			reopened := testLoadSnapshot(t, options, id)
			if reopened.Tree == nil || *reopened.Tree != *snapshot.Tree {
				t.Fatal("snapshot changed after encrypted database reopen")
			}
			result := map[string]any{"group_size": groupSize, "files": files, "bytes": size, "input_sha256": digest,
				"source": source, "entries": len(frozenPaths), "regular_files": regularFiles, "source_unchanged": originalParent,
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
			if originalParent {
				rtest.OK(t, validatePublicationCrawlMode(snapshot.CrawlPlan.Mode, true))
				return
			}
			if unchanged != "" || scale200k {
				if snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != "full" || snapshot.CrawlPlan.ReusedSubtrees != 0 {
					t.Fatal("unchanged benchmark parent is not a complete full traversal")
				}
				if forceCompare == "1" || scale200k {
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
		Flags: []string{"--read-concurrency=64", "--metadata-read-concurrency=32", "--metadata-batch-delay=1ms", "--metadata-publication-group-size=64"}}
	switch profile {
	case "db-control", "db-control-end":
	case "db-crypto32":
		tuning.CryptoThreads = 32
	case "db-meta2g":
		tuning.MetaCacheBytes = 2 << 30
	case "db-cache2g-start", "db-cache2g-end":
		tuning.MetaCacheBytes = 2 << 30
	case "db-cache128":
	case "db-cache256":
		tuning.MetaCacheBytes = 256 << 20
	case "db-cache512":
		tuning.MetaCacheBytes = 512 << 20
	case "db-cache1g":
		tuning.MetaCacheBytes = 1 << 30
	case "db-cache4g":
		tuning.MetaCacheBytes = 4 << 30
	case "db-scale200k", "db-scale400k":
		tuning.MetaCacheBytes = 2 << 30
		tuning.Flags[0] = "--read-concurrency=128"
		tuning.Flags[3] = "--metadata-publication-group-size=16"
	case "db-scale800k":
		tuning.MetaCacheBytes = 2 << 30
		tuning.Flags[0] = "--read-concurrency=128"
		if label := os.Getenv("VAULTIC_PHASE35_READERS_LEG"); label != "" {
			readers, err := publicationReadersLeg(os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT"), label)
			if err != nil {
				return publicationDBTuning{}, err
			}
			tuning.Flags[0] = fmt.Sprintf("--read-concurrency=%d", readers)
		}
		if label := os.Getenv("VAULTIC_PHASE35_RAM64_LEG"); label != "" {
			if _, err := publicationRAM64Leg(os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT"), label); err != nil {
				return publicationDBTuning{}, err
			}
		}
		tuning.Flags = append(tuning.Flags, "--metadata-reconciliation-batch-size=5000", "--metadata-reconciliation-read-ahead=1")
	case "db-group64", "db-group64-end", "db-hot-control", "db-hot-control-end", "db-read64-end":
		tuning.MetaCacheBytes = 2 << 30
	case "db-group16", "db-group32":
		tuning.MetaCacheBytes = 2 << 30
		tuning.Flags[3] = "--metadata-publication-group-size=" + strings.TrimPrefix(profile, "db-group")
	case "db-hot-crypto16", "db-hot-crypto32":
		tuning.MetaCacheBytes = 2 << 30
		tuning.CryptoThreads = 16
		if profile == "db-hot-crypto32" {
			tuning.CryptoThreads = 32
		}
	case "db-hot-batch0ms", "db-hot-batch2ms", "db-hot-batch3ms", "db-hot-batch5ms":
		tuning.MetaCacheBytes = 2 << 30
		tuning.Flags[2] = "--metadata-batch-delay=" + strings.TrimPrefix(profile, "db-hot-batch")
	case "db-read64-g16", "db-read64-g32", "db-read64-g64", "db-read64-g16-end", "db-read64-g32-end", "db-read64-g64-end",
		"db-read96-g16", "db-read96-g32", "db-read96-g64", "db-read128-g16", "db-read128-g32", "db-read128-g64":
		tuning.MetaCacheBytes = 2 << 30
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(profile, "db-read"), "-end"), "-g")
		tuning.Flags[0] = "--read-concurrency=" + parts[0]
		tuning.Flags[3] = "--metadata-publication-group-size=" + parts[1]
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
		"db-control":       {128 << 20, 512 << 20, 8, "32", "1ms"},
		"db-control-end":   {128 << 20, 512 << 20, 8, "32", "1ms"},
		"db-crypto32":      {128 << 20, 512 << 20, 32, "32", "1ms"},
		"db-meta2g":        {2 << 30, 512 << 20, 8, "32", "1ms"},
		"db-block8g":       {128 << 20, 8 << 30, 8, "32", "1ms"},
		"db-batch5ms":      {128 << 20, 512 << 20, 8, "32", "5ms"},
		"db-rpc64":         {128 << 20, 512 << 20, 8, "64", "1ms"},
		"db-cache2g-start": {2 << 30, 512 << 20, 8, "32", "1ms"},
		"db-cache2g-end":   {2 << 30, 512 << 20, 8, "32", "1ms"},
		"db-cache128":      {128 << 20, 512 << 20, 8, "32", "1ms"},
		"db-cache256":      {256 << 20, 512 << 20, 8, "32", "1ms"},
		"db-cache512":      {512 << 20, 512 << 20, 8, "32", "1ms"},
		"db-cache1g":       {1 << 30, 512 << 20, 8, "32", "1ms"},
		"db-cache4g":       {4 << 30, 512 << 20, 8, "32", "1ms"},
	} {
		t.Run(profile, func(t *testing.T) {
			tuning, err := publicationDBProfile(profile)
			rtest.OK(t, err)
			if tuning.MetaCacheBytes != expected.meta || tuning.BlockCacheBytes != expected.block || tuning.CryptoThreads != expected.crypto ||
				strings.Join(tuning.Flags, " ") != "--read-concurrency=64 --metadata-read-concurrency="+expected.rpc+" --metadata-batch-delay="+expected.delay+" --metadata-publication-group-size=64" {
				t.Fatalf("unexpected DB settings: %+v", tuning)
			}
		})
	}
	for profile, expected := range map[string]struct {
		readers, group, delay string
		crypto                int
	}{
		"db-group64":         {"64", "64", "1ms", 8},
		"db-group64-end":     {"64", "64", "1ms", 8},
		"db-group16":         {"64", "16", "1ms", 8},
		"db-group32":         {"64", "32", "1ms", 8},
		"db-hot-control":     {"64", "64", "1ms", 8},
		"db-hot-control-end": {"64", "64", "1ms", 8},
		"db-hot-crypto16":    {"64", "64", "1ms", 16},
		"db-hot-crypto32":    {"64", "64", "1ms", 32},
		"db-hot-batch0ms":    {"64", "64", "0ms", 8},
		"db-hot-batch2ms":    {"64", "64", "2ms", 8},
		"db-hot-batch3ms":    {"64", "64", "3ms", 8},
		"db-hot-batch5ms":    {"64", "64", "5ms", 8},
		"db-read96-g16":      {"96", "16", "1ms", 8},
		"db-read96-g32":      {"96", "32", "1ms", 8},
		"db-read96-g64":      {"96", "64", "1ms", 8},
		"db-read128-g16":     {"128", "16", "1ms", 8},
		"db-read128-g32":     {"128", "32", "1ms", 8},
		"db-read128-g64":     {"128", "64", "1ms", 8},
		"db-read64-end":      {"64", "64", "1ms", 8},
	} {
		t.Run(profile, func(t *testing.T) {
			tuning, err := publicationDBProfile(profile)
			rtest.OK(t, err)
			flags := "--read-concurrency=" + expected.readers + " --metadata-read-concurrency=32 --metadata-batch-delay=" + expected.delay + " --metadata-publication-group-size=" + expected.group
			if tuning.MetaCacheBytes != 2<<30 || tuning.BlockCacheBytes != 512<<20 || tuning.CryptoThreads != expected.crypto || strings.Join(tuning.Flags, " ") != flags {
				t.Fatalf("unexpected hot-cache settings: %+v", tuning)
			}
		})
	}
	for group, profiles := range map[string][]string{
		"16-scale": {"db-scale200k"},
		"16":       {"db-read64-g16", "db-read64-g16-end"},
		"32":       {"db-read64-g32", "db-read64-g32-end"},
		"64":       {"db-read64-g64", "db-read64-g64-end"},
	} {
		for _, profile := range profiles {
			t.Run(profile, func(t *testing.T) {
				tuning, err := publicationDBProfile(profile)
				rtest.OK(t, err)
				readers := "64"
				actualGroup := group
				if group == "16-scale" {
					readers, actualGroup = "128", "16"
				}
				if tuning.MetaCacheBytes != 2<<30 || tuning.CryptoThreads != 8 || strings.Join(tuning.Flags, " ") != "--read-concurrency="+readers+" --metadata-read-concurrency=32 --metadata-batch-delay=1ms --metadata-publication-group-size="+actualGroup {
					t.Fatalf("reader control differs from fixed-group candidate: %+v", tuning)
				}
			})
		}
	}
	for _, profile := range []string{"", "db-crypto128", "db-meta64g", "../db-control", "db-group128", "db-read256-g64"} {
		if _, err := publicationDBProfile(profile); err == nil {
			t.Fatalf("unapproved DB profile accepted: %q", profile)
		}
	}
}

func publicationDBFollowupProfile(profile string) bool {
	return strings.HasPrefix(profile, "db-cache") || strings.HasPrefix(profile, "db-group") || strings.HasPrefix(profile, "db-hot") || strings.HasPrefix(profile, "db-read")
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
		expectedLimit := "50000"
		if profile == "db-scale800k" {
			previous, expectedRoot, expectedLimit = publicationScale800kParent, publicationScale800kRoot, "800000"
			if label := os.Getenv("VAULTIC_PHASE35_RAM32_LEG"); label != "" {
				rtest.OK(t, validatePublicationRAM32Leg(root, label))
				expectedRoot = root
			}
			if label := os.Getenv("VAULTIC_PHASE35_RAM64_LEG"); label != "" {
				_, err := publicationRAM64Leg(root, label)
				rtest.OK(t, err)
				previous = publicationOriginal800kParent
				expectedRoot = root
			}
			if label := os.Getenv("VAULTIC_PHASE35_READERS_LEG"); label != "" {
				_, err := publicationReadersLeg(root, label)
				rtest.OK(t, err)
				previous = publicationOriginal800kParent
				expectedRoot = root
			}
		}
		if mode != "coalesced" || os.Getenv("VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT") != expectedLimit ||
			os.Getenv("VAULTIC_PHASE35_METADATA_READ_FORCE_ONLY") != "1" || root != expectedRoot {
			t.Fatal("concurrency screen requires a fresh allowlisted forced-only coalesced cell")
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
		if profile == "db-scale800k" {
			entries, expectedRegular, expectedFiles, expectedBytes = 800000, 677957, 679972, 54715539376
			expectedDigest = "7bc6b227e07a216d7b54f34671b73f41c24f03d4afe05b4eb79af2d35f57a851"
			if publicationOriginalSourceLeg() {
				encoded, err := os.ReadFile(filepath.Join(publicationOriginal800kParent, "source-observation.json"))
				rtest.OK(t, err)
				var observed struct {
					Source       string `json:"source"`
					Entries      uint64 `json:"entries"`
					RegularFiles uint64 `json:"regular_files"`
					Files        uint64 `json:"files"`
					Bytes        uint64 `json:"bytes"`
					SHA256       string `json:"sha256"`
				}
				rtest.OK(t, json.Unmarshal(encoded, &observed))
				rtest.OK(t, validatePublicationBackupSource(observed.Source))
				if observed.Entries != 800_000 || observed.RegularFiles == 0 || observed.Files < observed.RegularFiles || observed.Bytes == 0 || observed.Bytes > 64<<30 || len(observed.SHA256) != 64 {
					t.Fatal("original-source parent dimensions are invalid")
				}
				expectedRegular, expectedFiles, expectedBytes, expectedDigest = observed.RegularFiles, observed.Files, observed.Bytes, observed.SHA256
			}
		}
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
	if publicationOriginalSourceLeg() {
		source = "/ncl1-1-vs-50/fme_cae1"
	}
	rtest.OK(t, validatePublicationBackupSource(source))
	traversalLimit := uint64(entries + 1)
	if publicationOriginalSourceLeg() {
		traversalLimit = uint64(entries)
	}
	selected, err := boundedLiveSourcePaths(source, traversalLimit)
	rtest.OK(t, err)
	if len(selected) != entries {
		t.Fatal("qualified retry source scope changed")
	}
	if publicationOriginalSourceLeg() {
		encoded, err := os.ReadFile(filepath.Join(previous, "inode-manifest.json"))
		rtest.OK(t, err)
		var manifest backupcmd.BoundedInodeManifest
		rtest.OK(t, json.Unmarshal(encoded, &manifest))
		if manifest.Source != source || manifest.InodeLimit != uint64(entries) || !slices.Equal(manifest.Paths, selected) {
			t.Fatal("original-source parent manifest differs from the selected traversal")
		}
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
	if publicationDBFollowupProfile(profile) {
		candidate = "/root/proj/vaultic/bin/phase35-forced-followup/linux-amd64/vaultic"
		binary = "/root/proj/vaultic/bin/phase35-forced-followup/linux-amd64/vaulticdb"
	}
	if profile == "db-scale800k" {
		candidate, binary = filepath.Join(publicationScale800kBin, "vaultic"), filepath.Join(publicationScale800kBin, "vaulticdb")
		if os.Getenv("VAULTIC_PHASE35_RAM32_LEG") != "" {
			candidate, binary = filepath.Join(publicationRAM32800kBin, "vaultic"), filepath.Join(publicationRAM32800kBin, "vaulticdb")
		}
		if os.Getenv("VAULTIC_PHASE35_READERS_LEG") != "" {
			candidate, binary = filepath.Join(publicationReaders800kBin, "vaultic"), filepath.Join(publicationReaders800kBin, "vaulticdb")
		}
		if os.Getenv("VAULTIC_PHASE35_RAM64_LEG") != "" {
			candidate, binary = filepath.Join(publicationRAM64800kBin, "vaultic"), filepath.Join(publicationRAM64800kBin, "vaulticdb")
		}
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

func mergePublicationInheritedWAL(dataDir, repositoryHash string) error {
	source := filepath.Join(dataDir, "wal", repositoryHash, "db", "wal")
	target := filepath.Join(dataDir, repositoryHash, "db", "wal")
	manifest := make(map[string]string)
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 64<<20 {
			return fmt.Errorf("unsafe WAL fixture entry %s", path)
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if existing, err := os.ReadFile(destination); err == nil {
			if !bytes.Equal(existing, contents) {
				return fmt.Errorf("WAL collision differs at %s", relative)
			}
		} else if !os.IsNotExist(err) {
			return err
		} else {
			if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
				return err
			}
			if err := os.Chmod(filepath.Dir(destination), 0700); err != nil {
				return err
			}
			if err := os.WriteFile(destination, contents, 0600); err != nil {
				return err
			}
			if err := os.Chmod(destination, 0600); err != nil {
				return err
			}
		}
		manifest[relative] = fmt.Sprintf("%x", sha256.Sum256(contents))
		return nil
	})
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dataDir, "wal-inheritance.json"), encoded, 0600)
}

func TestPublicationImmutableDefaultPolicy(t *testing.T) {
	for index := 1; index <= 3; index++ {
		rtest.OK(t, validatePublicationChunkLeg(filepath.Join(publicationImmutable200kRoot, fmt.Sprintf("leg-%02d-I", index)), "I"))
	}
	if validatePublicationChunkLeg(filepath.Join(publicationImmutable200kRoot, "leg-04-I"), "I") == nil {
		t.Fatal("unscheduled leg accepted")
	}
	root := t.TempDir()
	source := filepath.Join(root, "wal", "fixture", "db", "wal")
	target := filepath.Join(root, "fixture", "db", "wal")
	rtest.OK(t, os.MkdirAll(source, 0700))
	rtest.OK(t, os.MkdirAll(target, 0700))
	rtest.OK(t, os.WriteFile(filepath.Join(source, "1.sst"), []byte("ciphertext"), 0600))
	rtest.OK(t, mergePublicationInheritedWAL(root, "fixture"))
	contents, err := os.ReadFile(filepath.Join(target, "1.sst"))
	rtest.OK(t, err)
	if string(contents) != "ciphertext" {
		t.Fatal("WAL bytes changed")
	}
	rtest.OK(t, os.WriteFile(filepath.Join(source, "1.sst"), []byte("different"), 0600))
	if mergePublicationInheritedWAL(root, "fixture") == nil {
		t.Fatal("different WAL collision accepted")
	}
}

func TestPublicationChunk200kComparison(t *testing.T) {
	variant := os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT")
	if variant == "" {
		t.Skip("chunk comparison not selected")
	}
	root := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT")
	rtest.OK(t, validatePublicationChunkLeg(root, variant))
	seriesRoot := publicationChunk200kRoot
	limit, profile, parentRoot := 200000, "db-scale200k", publicationScale200kRoot
	if publicationRead400kVariant(variant) {
		seriesRoot, limit, profile, parentRoot = publicationRead400kRoot, 400000, "db-scale400k", publicationScale400kRoot
	}
	if publicationCache400kVariant(variant) {
		seriesRoot, parentRoot = publicationCache400kRoot, publicationCache400kParent
	}
	if publicationPipeline400kVariant(variant) {
		seriesRoot = publicationPipeline400kRoot
	}
	if publicationRAM16400kVariant(variant) {
		seriesRoot = publicationRAM16400kRoot
	}
	if variant == "I" || publicationRead400kVariant(variant) {
		if variant == "I" {
			seriesRoot = publicationImmutable200kRoot
		}
		if os.Getenv("VAULTICDB_BACKEND_ACCESS_MODE") != "dedicated-force" || os.Getenv("VAULTICDB_SST_FRESHNESS") != "immutable" {
			t.Fatal("immutable default policies differ")
		}
	}
	rtest.OK(t, validatePublicationQualificationRoot(root, seriesRoot))
	if os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT") != fmt.Sprint(limit) ||
		os.Getenv("VAULTIC_PHASE35_METADATA_READ_FORCE_ONLY") != "1" ||
		os.Getenv("VAULTIC_PHASE35_METADATA_READ_CONCURRENCY_PROFILE") != profile ||
		os.Getenv("VAULTICDB_CRYPTO_THREADS") != "8" {
		t.Fatal("chunk comparison requires the fixed forced-only 200k profile")
	}
	binRoot := "/root/proj/vaultic/bin/phase35-chunk200k/linux-amd64"
	if variant == "I" {
		binRoot = "/root/proj/vaultic/bin/phase35-immutable200k-retry1/linux-amd64"
	}
	if publicationRead400kVariant(variant) {
		binRoot = publicationRead400kBin
	}
	if publicationCache400kVariant(variant) {
		binRoot = publicationCache400kBin
	}
	if publicationPipeline400kVariant(variant) {
		binRoot = publicationPipeline400kBin
	}
	if publicationRAM16400kVariant(variant) {
		binRoot = publicationRAM16400kBin
	}
	candidate, binary := filepath.Join(binRoot, "vaultic"), filepath.Join(binRoot, "new/vaulticdb")
	if variant == "T" {
		candidate = filepath.Join(binRoot, "control/vaultic")
	}
	if variant == "A" {
		binary = filepath.Join(binRoot, "old/vaulticdb")
	} else {
		capacity := "67108864"
		if variant == "B" {
			capacity = "0"
		} else if variant == "D" {
			capacity = "268435456"
		}
		entries := "4096"
		if publicationCache400kVariant(variant) {
			capacity, entries = publicationCache400kChunkBytes(variant), "16384"
		}
		if publicationRAM16400kVariant(variant) {
			entries = "65536"
		}
		for name, expected := range map[string]string{
			"VAULTICDB_CHUNK_CACHE_BYTES": capacity, "VAULTICDB_CHUNK_CACHE_ENTRIES": entries,
			"VAULTICDB_CHUNK_FETCH_LIMIT": "64", "VAULTICDB_CHUNK_FETCH_BYTES": "16777216",
			"VAULTICDB_CHUNK_MAX_BYTES": "1048576", "VAULTICDB_CHUNK_RANGE_LIMIT": "4",
		} {
			if os.Getenv(name) != expected {
				t.Fatalf("chunk variant setting %s differs", name)
			}
		}
	}
	for path, expected := range map[string]string{candidate: os.Getenv("VAULTIC_PHASE35_THROUGHPUT_CANDIDATE_SHA256"), binary: os.Getenv("VAULTIC_PHASE35_THROUGHPUT_NATIVE_SHA256")} {
		contents, err := os.ReadFile(path)
		rtest.OK(t, err)
		if fmt.Sprintf("%x", sha256.Sum256(contents)) != expected {
			t.Fatal("chunk comparison executable pin differs")
		}
	}
	parentCell := filepath.Join(parentRoot, "group-64")
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
	parentEncoded := encoded
	expectedFiles, expectedBytes, expectedRegular := uint64(178257), uint64(23520039140), uint64(178208)
	expectedDigest := "c1f7faee11872b1fac2f0af5d242c7a30f51ddc426bfd3e5c93b7200b3eefc83"
	if limit == 400000 {
		var sourceReceipt struct {
			Entries      int    `json:"entries"`
			Files        uint64 `json:"files"`
			Bytes        uint64 `json:"bytes"`
			RegularFiles uint64 `json:"regular_files"`
			SHA256       string `json:"sha256"`
		}
		encoded, err := os.ReadFile(filepath.Join(parentRoot, "source-observation.json"))
		rtest.OK(t, err)
		rtest.OK(t, json.Unmarshal(encoded, &sourceReceipt))
		if sourceReceipt.Entries != limit || sourceReceipt.Bytes > 64<<30 || sourceReceipt.RegularFiles == 0 {
			t.Fatal("400k parent source scope differs")
		}
		expectedFiles, expectedBytes, expectedRegular, expectedDigest = sourceReceipt.Files, sourceReceipt.Bytes, sourceReceipt.RegularFiles, sourceReceipt.SHA256
	}
	if publicationCache400kVariant(variant) {
		receipt.Files, err = publicationCacheParentFileCount(parentEncoded, limit, expectedFiles, expectedRegular)
		rtest.OK(t, err)
	}
	if !receipt.RestoreVerified || !receipt.ReopenVerified || receipt.Files != expectedFiles || receipt.Bytes != expectedBytes || receipt.InputSHA256 != expectedDigest {
		t.Fatal("chunk comparison parent identity differs")
	}
	parent, err := vaultic.ParseID(receipt.Snapshot)
	rtest.OK(t, err)
	source := filepath.Join(parentRoot, "input")
	if publicationCache400kVariant(variant) {
		source = filepath.Join(publicationScale400kRoot, "input")
	}
	selected, err := boundedLiveSourcePaths(source, uint64(limit+1))
	rtest.OK(t, err)
	if len(selected) != limit {
		t.Fatal("chunk comparison source scope differs")
	}
	paths := make(map[string]bool, len(selected))
	for _, path := range selected {
		paths[path] = true
	}
	digest, files, size := m5FilesSelected(t, source, false, paths)
	regularFiles := publicationQualificationRegularFiles(t, source, selected)
	if digest != receipt.InputSHA256 || files != receipt.Files || size != receipt.Bytes || regularFiles != expectedRegular {
		t.Fatal("chunk comparison source content differs")
	}
	controlRoot := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_CONTROL_ROOT")
	resolved, err := filepath.EvalSymlinks(controlRoot)
	rtest.OK(t, err)
	info, err := os.Stat(controlRoot)
	rtest.OK(t, err)
	runtimePrefix := "/run/vaultic-unchanged-chunk200k-"
	if variant == "I" {
		runtimePrefix = "/run/vaultic-unchanged-immutable200k-"
	}
	if publicationRead400kVariant(variant) {
		runtimePrefix = "/run/vaultic-unchanged-reconcile400k-"
	}
	if publicationCache400kVariant(variant) {
		runtimePrefix = "/run/vaultic-unchanged-cache400k-retry4-"
	}
	if publicationPipeline400kVariant(variant) {
		runtimePrefix = "/run/vaultic-unchanged-pipeline400k-"
	}
	if publicationRAM16400kVariant(variant) {
		runtimePrefix = "/run/vaultic-unchanged-ram16400k-"
	}
	if resolved != controlRoot || !strings.HasPrefix(controlRoot, runtimePrefix) || info.Mode().Perm() != 0700 {
		t.Fatal("chunk comparison requires a private canonical runtime")
	}
	control, err := os.MkdirTemp(controlRoot, "fc-")
	rtest.OK(t, err)
	rtest.OK(t, os.Chmod(control, 0700))
	t.Cleanup(func() { _ = os.RemoveAll(control) })
	tuning, err := publicationDBProfile(profile)
	rtest.OK(t, err)
	if variant == "O" {
		tuning.BlockCacheBytes = 2 << 30
	} else if variant == "P" {
		tuning.BlockCacheBytes = 8 << 30
	}
	if variant == "J" {
		tuning.Flags = append(tuning.Flags, "--metadata-reconciliation-scalar-revision-reads")
	} else if variant == "L" {
		tuning.Flags = append(tuning.Flags, "--metadata-reconciliation-read-ahead=1")
	} else if variant == "M" {
		tuning.Flags = append(tuning.Flags, "--metadata-reconciliation-batch-size=500")
	}
	if publicationPipeline400kVariant(variant) {
		tuning, err = publicationPipeline400kTuning(variant)
		rtest.OK(t, err)
	}
	current := daemon.Options{Socket: filepath.Join(control, "db.sock"), RepositoryID: "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528",
		DaemonPath: binary, DataDir: filepath.Join(parentCell, "db"), WALDataDir: filepath.Join(parentCell, "db", "wal"),
		ObjectStore: "local", WALStore: "local", WALFlushInterval: 100 * time.Millisecond,
		MetaCacheBytes: tuning.MetaCacheBytes, BlockCacheBytes: tuning.BlockCacheBytes, EngineMultiGet: true,
		EncryptionMode: "required", PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery", StartTimeout: 90 * time.Second}
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
	publicationQualificationForceComparison(t, cell, source, candidate, options, current, &owner, parent, paths, digest, regularFiles, files, size, tuning.Flags...)
}

func publicationCacheParentFileCount(encoded []byte, entries int, files, regular uint64) (uint64, error) {
	var receipt struct {
		Entries         int     `json:"entries"`
		Regular         uint64  `json:"regular_files"`
		Files           *uint64 `json:"files"`
		SourceUnchanged bool    `json:"source_unchanged"`
	}
	if err := json.Unmarshal(encoded, &receipt); err != nil {
		return 0, err
	}
	if receipt.Entries != entries || receipt.Regular != regular || !receipt.SourceUnchanged || files == 0 {
		return 0, fmt.Errorf("cache parent source count binding differs")
	}
	if receipt.Files != nil && *receipt.Files != files {
		return 0, fmt.Errorf("cache parent file count differs")
	}
	return files, nil
}

func TestPublicationCacheParentFileCount(t *testing.T) {
	valid := map[string]any{"entries": 400000, "regular_files": 355389, "source_unchanged": true}
	encoded, err := json.Marshal(valid)
	rtest.OK(t, err)
	files, err := publicationCacheParentFileCount(encoded, 400000, 355571, 355389)
	rtest.OK(t, err)
	if files != 355571 {
		t.Fatal("forced receipt file count binding differs")
	}
	for name, value := range map[string]any{"entries": 1, "regular_files": 1, "source_unchanged": false, "files": 0} {
		invalid := map[string]any{"entries": 400000, "regular_files": 355389, "source_unchanged": true}
		invalid[name] = value
		encoded, err := json.Marshal(invalid)
		rtest.OK(t, err)
		if _, err := publicationCacheParentFileCount(encoded, 400000, 355571, 355389); err == nil {
			t.Fatalf("invalid parent %s accepted", name)
		}
	}
}

func validatePublicationCacheIdentity(snapshotFSID uint32, device uint64) error {
	if device == 0 || device > uint64(^uint32(0)) || snapshotFSID == 0 || uint64(snapshotFSID) != device {
		return fmt.Errorf("cache parent filesystem identity %d differs from current source device %d", snapshotFSID, device)
	}
	return nil
}

func TestPublicationCacheIdentity(t *testing.T) {
	for _, item := range []struct {
		fsid   uint32
		device uint64
		valid  bool
	}{{7, 7, true}, {7, 8, false}, {0, 7, false}, {7, 0, false}, {7, 1 << 32, false}} {
		if (validatePublicationCacheIdentity(item.fsid, item.device) == nil) != item.valid {
			t.Fatalf("identity (%d, %d) validation differs", item.fsid, item.device)
		}
	}
}

func validatePublicationCacheSnapshotIdentity(snapshot schema.SnapshotRecord, device uint64) error {
	if snapshot.LegacyTree != (schema.ID{}) {
		if snapshot.CommitSequence != 0 || snapshot.RootFSID != 0 || snapshot.RootInode != 0 || snapshot.RootRevision != 0 {
			return fmt.Errorf("legacy cache snapshot has normalized identity fields")
		}
	} else if snapshot.CommitSequence == 0 || snapshot.RootRevision == 0 {
		return fmt.Errorf("normalized cache snapshot scope is incomplete")
	}
	return validatePublicationCacheIdentity(uint32(device), device)
}

func TestPublicationCacheSnapshotIdentity(t *testing.T) {
	legacy := schema.SnapshotRecord{LegacyTree: schema.ID{1}}
	rtest.OK(t, validatePublicationCacheSnapshotIdentity(legacy, 7))
	normalized := schema.SnapshotRecord{CommitSequence: 1, RootRevision: 1, RootFSID: 7}
	rtest.OK(t, validatePublicationCacheSnapshotIdentity(normalized, 7))
	rtest.OK(t, validatePublicationCacheSnapshotIdentity(normalized, 8))
	encoded, err := (schema.SnapshotRecord{CommitSequence: 1, RootRevision: 1, OriginalJSON: []byte("{}")}).MarshalBinary()
	rtest.OK(t, err)
	root, err := schema.UnmarshalSnapshotRecord(encoded)
	rtest.OK(t, err)
	rtest.OK(t, validatePublicationCacheSnapshotIdentity(root, 7))
	for _, item := range []struct {
		snapshot schema.SnapshotRecord
		device   uint64
	}{{legacy, 0}, {legacy, 1 << 32}, {schema.SnapshotRecord{LegacyTree: schema.ID{1}, RootFSID: 7}, 7}, {schema.SnapshotRecord{RootRevision: 1}, 7}, {schema.SnapshotRecord{}, 7}} {
		if validatePublicationCacheSnapshotIdentity(item.snapshot, item.device) == nil {
			t.Fatal("invalid snapshot/source identity accepted")
		}
	}
}

func publicationCacheReusePreflight(ctx context.Context, client *daemon.Client, parent vaultic.ID, source string, paths map[string]bool) (map[string]any, error) {
	filesystem := fs.NewLocal()
	info, err := filesystem.Lstat(source)
	if err != nil {
		return nil, err
	}
	store := daemon.NewSchemaStore(client)
	value, found, err := store.Get(ctx, schema.SnapshotKey(schema.ID(parent)))
	if err != nil || !found {
		return nil, fmt.Errorf("cache parent snapshot unavailable: found=%t: %w", found, err)
	}
	snapshot, err := schema.UnmarshalSnapshotRecord(value)
	if err != nil {
		return nil, err
	}
	receipt := map[string]any{"source_device": info.DeviceID, "snapshot_fsid": snapshot.RootFSID,
		"snapshot_root_inode": snapshot.RootInode, "snapshot_commit_sequence": snapshot.CommitSequence, "snapshot_root_revision": snapshot.RootRevision,
		"source_inode": info.Inode, "sampled_records": 0, "accepted": false}
	receipt["snapshot_format"] = "normalized"
	if snapshot.LegacyTree != (schema.ID{}) {
		receipt["snapshot_format"] = "legacy-tree"
	}
	if err := validatePublicationCacheSnapshotIdentity(snapshot, info.DeviceID); err != nil {
		return receipt, err
	}
	selected := make([]string, 0, len(paths))
	for path := range paths {
		selected = append(selected, path)
	}
	slices.Sort(selected)
	if len(selected) == 0 {
		return receipt, fmt.Errorf("cache source selection is empty")
	}
	count := min(64, len(selected))
	for index := 0; index < count; index++ {
		path := selected[index*len(selected)/count]
		if path != "." && !filepath.IsLocal(path) {
			return receipt, fmt.Errorf("cache source sample escapes its frozen root")
		}
		stat, err := filesystem.Lstat(filepath.Join(source, path))
		if err != nil {
			return receipt, err
		}
		if err := validatePublicationCacheIdentity(uint32(info.DeviceID), stat.DeviceID); err != nil {
			return receipt, err
		}
		key := schema.CurrentInodeKey(uint32(stat.DeviceID), stat.Inode)
		kind := schema.KeyInodeRevision
		if stat.Mode.IsDir() {
			key = schema.CurrentDirectoryKey(uint32(stat.DeviceID), stat.Inode)
			kind = schema.KeyDirectoryRevision
		}
		value, found, err := store.Get(ctx, key)
		if err != nil || !found {
			return receipt, fmt.Errorf("cache current-pointer sample %d unavailable: found=%t: %w", index, found, err)
		}
		pointer, err := schema.UnmarshalCurrentPointer(value)
		if err != nil {
			return receipt, err
		}
		revision, err := schema.ParseKey(pointer.RecordKey)
		if err != nil || revision.FSID != uint32(stat.DeviceID) || revision.Inode != stat.Inode || revision.Kind != kind {
			return receipt, fmt.Errorf("cache revision sample %d source identity differs: %w", index, err)
		}
		value, found, err = store.Get(ctx, pointer.RecordKey)
		if err != nil || !found || len(value) == 0 {
			return receipt, fmt.Errorf("cache revision sample %d unavailable: found=%t: %w", index, found, err)
		}
		receipt["source_fsid"] = revision.FSID
		receipt["sampled_records"] = index + 1
	}
	receipt["accepted"] = true
	return receipt, nil
}

func TestPublicationCacheNativePreflight(t *testing.T) {
	root := os.Getenv("VAULTIC_PHASE35_CACHE_PREFLIGHT_ROOT")
	if root == "" {
		t.Skip("private native cache preflight not selected")
	}
	if root != "/volume2/NASDA2/vaultic-test-m6/cache-native-probe-fme-cae1-20261107-400k" && root != "/volume2/NASDA2/vaultic-test-m6/cache-native-probe-fme-cae1-20261107-400k-retry1" {
		t.Fatal("native cache probe root differs")
	}
	rtest.OK(t, validatePublicationQualificationRoot(root, "/volume2/NASDA2/vaultic-test-m6"))
	control, err := os.MkdirTemp("/run", "vcp-")
	rtest.OK(t, err)
	rtest.OK(t, os.Chmod(control, 0700))
	t.Cleanup(func() { _ = os.RemoveAll(control) })
	database := filepath.Join(root, "db")
	rtest.OK(t, os.Mkdir(database, 0700))
	rtest.OK(t, os.Chmod(database, 0700))
	parentDatabase := filepath.Join(publicationCache400kParent, "group-64", "db")
	publicationQualificationCommand(t, root, "copy-db", "rsync", "-a", "--chmod=D700,F600", "--exclude=*.coordination.lock", parentDatabase+"/", database+"/")
	publicationQualificationCommand(t, root, "checksum-db", "rsync", "-rcni", "--delete", "--exclude=*.coordination.lock", parentDatabase+"/", database+"/")
	checksum, err := os.ReadFile(filepath.Join(root, "checksum-db.stdout.log"))
	rtest.OK(t, err)
	if len(checksum) != 0 {
		t.Fatal("native probe database checksum clone differs")
	}
	options := daemon.Options{Socket: filepath.Join(control, "db.sock"), RepositoryID: "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528",
		DaemonPath: filepath.Join(publicationCache400kBin, "new", "vaulticdb"), DataDir: database, ObjectStore: "local", WALStore: "inherit",
		MetaCacheBytes: 2 << 30, BlockCacheBytes: 512 << 20, EngineMultiGet: true,
		EncryptionMode: "required", PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery", StartTimeout: 90 * time.Second}
	client, err := daemon.Ensure(t.Context(), options)
	rtest.OK(t, err)
	defer func() { rtest.OK(t, client.Close(context.Background())) }()
	encoded, err := os.ReadFile(filepath.Join(publicationCache400kParent, "parent-provenance.json"))
	rtest.OK(t, err)
	var provenance struct {
		Snapshot string `json:"snapshot"`
	}
	rtest.OK(t, json.Unmarshal(encoded, &provenance))
	parent, err := vaultic.ParseID(provenance.Snapshot)
	rtest.OK(t, err)
	encoded, err = os.ReadFile(filepath.Join(publicationCache400kParent, "inode-manifest.json"))
	rtest.OK(t, err)
	var manifest struct {
		Source string   `json:"source"`
		Paths  []string `json:"paths"`
	}
	rtest.OK(t, json.Unmarshal(encoded, &manifest))
	if manifest.Source != filepath.Join(publicationScale400kRoot, "input") || len(manifest.Paths) != 400000 {
		t.Fatal("native probe frozen source selection differs")
	}
	paths := make(map[string]bool, len(manifest.Paths))
	for _, path := range manifest.Paths {
		paths[path] = true
	}
	receipt, preflightErr := publicationCacheReusePreflight(t.Context(), client, parent, manifest.Source, paths)
	if preflightErr == nil {
		store := daemon.NewSchemaStore(client)
		var cursor []byte
		var identities []map[string]any
		scanned, done := 0, false
		for scanned < 200000 && !done {
			entries, complete, err := store.ScanPrefix(t.Context(), schema.CurrentDirectoryKey(0, 0)[:2], cursor, 5000)
			rtest.OK(t, err)
			if len(entries) == 0 && !complete {
				t.Fatal("native identity scan did not advance")
			}
			for _, entry := range entries {
				key, err := schema.ParseKey(entry.Key)
				rtest.OK(t, err)
				if key.Inode != receipt["source_inode"].(uint64) {
					continue
				}
				pointer, err := schema.UnmarshalCurrentPointer(entry.Value)
				rtest.OK(t, err)
				value, found, err := store.Get(t.Context(), pointer.RecordKey)
				rtest.OK(t, err)
				if !found {
					t.Fatal("native identity scan revision missing")
				}
				directory, err := schema.UnmarshalDirectoryRevision(value)
				rtest.OK(t, err)
				identities = append(identities, map[string]any{"fsid": key.FSID, "source_path_matches": directory.SourcePath == manifest.Source})
			}
			if len(entries) != 0 {
				cursor = entries[len(entries)-1].Key
			}
			scanned, done = scanned+len(entries), complete
		}
		receipt["root_pointer_identities"], receipt["root_pointer_scan_complete"] = identities, done
	}
	publicationRuntimeReceipt(t, filepath.Join(root, "reuse-preflight.json"), receipt)
	rtest.OK(t, preflightErr)
	_, err = client.WriterStatus(t.Context())
	rtest.OK(t, err)
}

func publicationQualificationForceComparison(t *testing.T, cell, source, candidate string, options global.Options,
	daemonOptions daemon.Options, owner **daemon.Client, parent vaultic.ID, paths map[string]bool, digest string,
	regularFiles, files, size uint64, extraFlags ...string,
) {
	t.Helper()
	rtest.OK(t, validatePublicationBackupSource(source))
	chunk200k := os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT") != "" && validatePublicationChunkLeg(os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT"), os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT")) == nil
	scale200k := len(paths) == 200_000 && os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT") == "200000" && (os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT") == publicationScale200kRoot || chunk200k)
	scale200k = scale200k || len(paths) == 400_000 && os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT") == "400000" && (os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT") == publicationScale400kRoot || chunk200k && publicationRead400kVariant(os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT")))
	rawRoot := os.Getenv("VAULTIC_PHASE35_THROUGHPUT_ROOT")
	ram32 := os.Getenv("VAULTIC_PHASE35_RAM32_LEG") != "" && validatePublicationRAM32Leg(rawRoot, os.Getenv("VAULTIC_PHASE35_RAM32_LEG")) == nil
	_, readersErr := publicationReadersLeg(rawRoot, os.Getenv("VAULTIC_PHASE35_READERS_LEG"))
	readers := os.Getenv("VAULTIC_PHASE35_READERS_LEG") != "" && readersErr == nil
	_, ram64Err := publicationRAM64Leg(rawRoot, os.Getenv("VAULTIC_PHASE35_RAM64_LEG"))
	ram64 := os.Getenv("VAULTIC_PHASE35_RAM64_LEG") != "" && ram64Err == nil
	scale200k = scale200k || len(paths) == 800_000 && os.Getenv("VAULTIC_PHASE35_SCALE_INODE_LIMIT") == "800000" && (rawRoot == publicationScale800kRoot || ram32 || readers || ram64)
	if len(paths) != 10000 && len(paths) != 50000 && !scale200k {
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
		if forceOnly != "1" || (len(paths) != 50000 && !scale200k) || os.Getenv("VAULTIC_PHASE35_METADATA_READ_CONCURRENCY_PROFILE") == "" {
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
		inheritedIdentity := fmt.Sprintf("sha256:%x\n", sha256.Sum256([]byte("inherit")))
		inheritedCacheParent := publicationCache400kVariant(os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT")) && string(copied) == inheritedIdentity
		if !inheritedCacheParent && string(copied) != "vaulticdb-local-wal-after-bulk-import-v1:"+daemonOptions.DataDir+"/wal" {
			t.Fatal("comparison copied WAL binding differs")
		}
		if os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT") == "I" || publicationRead400kVariant(os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT")) {
			if !inheritedCacheParent {
				rtest.OK(t, mergePublicationInheritedWAL(current.DataDir, repositoryHash))
				publicationQualificationCopy(t, filepath.Join(leg, "wal-inheritance.json"), filepath.Join(current.DataDir, "wal-inheritance.json"))
				rtest.OK(t, os.WriteFile(binding, []byte(inheritedIdentity), 0600))
			}
			current.WALStore, current.WALDataDir = "inherit", ""
		} else {
			rtest.OK(t, os.WriteFile(binding, []byte("vaulticdb-local-wal-after-bulk-import-v1:"+current.DataDir+"/wal"), 0600))
		}
		*owner, err = daemon.Ensure(t.Context(), current)
		rtest.OK(t, err)
		legOptions := publicationQualificationOptions(filepath.Join(leg, "repo"), current.Socket, current.PassphraseFile)
		rtest.OK(t, publicationQualificationLoadPassword(&legOptions))
		if readers || ram64 {
			receipt, preflightErr := publicationCacheReusePreflight(t.Context(), *owner, parent, source, paths)
			encoded, err := json.MarshalIndent(receipt, "", "  ")
			rtest.OK(t, err)
			rtest.OK(t, os.WriteFile(filepath.Join(leg, "reuse-preflight.json"), append(encoded, '\n'), 0600))
			rtest.OK(t, preflightErr)
		}
		if publicationCache400kVariant(os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT")) {
			receipt, preflightErr := publicationCacheReusePreflight(t.Context(), *owner, parent, source, paths)
			provenance, err := os.ReadFile(filepath.Join(publicationCache400kParent, "parent-provenance.json"))
			rtest.OK(t, err)
			var historical struct {
				Snapshot string `json:"historical_snapshot"`
			}
			rtest.OK(t, json.Unmarshal(provenance, &historical))
			historicalID, err := vaultic.ParseID(historical.Snapshot)
			rtest.OK(t, err)
			value, found, err := daemon.NewSchemaStore(*owner).Get(t.Context(), schema.SnapshotKey(schema.ID(historicalID)))
			rtest.OK(t, err)
			if !found {
				t.Fatal("historical parent diagnostic snapshot missing")
			}
			record, err := schema.UnmarshalSnapshotRecord(value)
			rtest.OK(t, err)
			if receipt != nil {
				receipt["historical_snapshot_fsid"] = record.RootFSID
				receipt["historical_snapshot_root_inode"] = record.RootInode
			}
			encoded, err := json.MarshalIndent(receipt, "", "  ")
			rtest.OK(t, err)
			rtest.OK(t, os.WriteFile(filepath.Join(leg, "reuse-preflight.json"), append(encoded, '\n'), 0600))
			rtest.OK(t, preflightErr)
		}
		before, err := (*owner).WriterStatus(t.Context())
		rtest.OK(t, err)
		if before.ActiveTransactions != 0 || before.ActiveWriteIntents != 0 {
			t.Fatal("comparison writer is not idle")
		}
		args := []string{"--repo=" + legOptions.Repo, "--password-file=" + legOptions.PasswordFile, "--metadata-daemon-socket=" + current.Socket,
			"--metadata-encryption-mode=required", "--metadata-passphrase-file=" + current.PassphraseFile, "--no-cache", "--json",
			"backup", "--no-scan", "--one-file-system", "--read-concurrency=16",
			"--metadata-reconciliation-queue-depth=256", "--metadata-on-demand", "--metadata-cache-mib=64",
			"--working-memory-mode=ram", "--working-memory-ram-limit=160GiB", "--working-memory-scratch=" + filepath.Join(legOptions.Repo, "working"), "--working-memory-disk-budget=2GiB"}
		args = append(args, extraFlags...)
		hasPublicationGroup := false
		for _, flag := range extraFlags {
			if strings.HasPrefix(flag, "--metadata-publication-group-size=") {
				hasPublicationGroup = true
			}
		}
		if !hasPublicationGroup {
			args = append(args, "--metadata-publication-group-size=64")
		}
		if forced {
			args = append(args, "--force")
		} else {
			args = append(args, "--parent="+parent.String())
		}
		if readers || ram64 {
			args = append(args, "--bounded-inode-manifest="+filepath.Join(rawRoot, "inode-manifest.json"))
		}
		args = append(args, source)
		if publicationPipeline400kVariant(os.Getenv("VAULTIC_PHASE35_CHUNK_VARIANT")) || len(paths) == 800_000 {
			profileRoot := filepath.Join(leg, "cpu")
			rtest.OK(t, os.Mkdir(profileRoot, 0700))
			rtest.OK(t, os.Chmod(profileRoot, 0700))
			profile, err := os.OpenFile(filepath.Join(profileRoot, "cpu.pprof"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			rtest.OK(t, err)
			rtest.OK(t, profile.Close())
			rtest.OK(t, os.Chmod(filepath.Join(profileRoot, "cpu.pprof"), 0600))
			args = append(args, "--cpu-profile="+profileRoot)
		}
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
		var captured struct {
			SourceReads *struct {
				Bytes uint64 `json:"bytes"`
			} `json:"source_reads"`
			EncryptionReads *daemon.EncryptionReadMetrics `json:"encryption_reads"`
		}
		rtest.OK(t, json.Unmarshal(critical, &captured))
		if len(paths) == 800_000 && (captured.SourceReads == nil || captured.SourceReads.Bytes < size || captured.EncryptionReads == nil || captured.EncryptionReads.CiphertextOriginLatencyBuckets == nil || captured.EncryptionReads.ChunkWaitLatencyBuckets == nil || captured.EncryptionReads.ChunkAdmissionLatencyBuckets == nil) {
			t.Fatal("800k capture lacks complete source-read and DB wait telemetry")
		}
		id, err := vaultic.ParseID(snapshotID)
		rtest.OK(t, err)
		snapshot := testLoadSnapshot(t, legOptions, id)
		expectedCrawlMode := "full"
		if readers || ram64 {
			expectedCrawlMode = "bounded"
		}
		if snapshot.CrawlPlan == nil || snapshot.CrawlPlan.Mode != expectedCrawlMode || snapshot.CrawlPlan.ReusedSubtrees != 0 || len(snapshot.Paths) != 1 || snapshot.Paths[0] != source {
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
		if candidate == "/root/proj/vaultic/bin/phase35-forced-followup/linux-amd64/vaultic" || chunk200k {
			if before.Attribution.CacheReads == nil || after.Attribution.CacheReads == nil || measurement.Reconciliation.PublicationStages == nil {
				t.Fatal("authenticated follow-up omitted direct cache or publication attribution")
			}
			for name, current := range *after.Attribution.CacheReads {
				previous := (*before.Attribution.CacheReads)[name]
				if current.Hits < previous.Hits || current.Misses < previous.Misses {
					t.Fatal("follow-up cache counters reset across the measured backup")
				}
			}
		}
		restored := filepath.Join(leg, "restore")
		actual, actualFiles, actualBytes := digest, uint64(0), uint64(0)
		if len(paths) == 800_000 {
			groups, err := publicationRestoreGroups(paths)
			rtest.OK(t, err)
			for _, group := range groups {
				selected := map[string]bool{".": true}
				for path := range paths {
					if path == group || strings.HasPrefix(path, group+"/") {
						selected[path] = true
					}
				}
				restore := restoreOptions{Target: restored, Verify: true}
				restore.Includes = []string{filepath.Join(source, group)}
				rtest.OK(t, testRunRestoreAssumeFailure(t, id.String(), restore, legOptions))
				got, gotFiles, gotBytes := m5FilesSelected(t, filepath.Join(restored, strings.TrimPrefix(source, "/")), false, selected)
				want, wantFiles, wantBytes := m5FilesSelected(t, source, false, selected)
				if got != want || gotFiles != wantFiles || gotBytes != wantBytes {
					t.Fatal("comparison restore subtree differs", group)
				}
				actualFiles += gotFiles
				actualBytes += gotBytes
				rtest.OK(t, os.RemoveAll(restored))
				t.Logf("verified and released restore subtree %s files=%d bytes=%d", group, gotFiles, gotBytes)
			}
		} else {
			rtest.OK(t, testRunRestoreAssumeFailure(t, id.String(), restoreOptions{Target: restored, Verify: true}, legOptions))
			actual, actualFiles, actualBytes = m5FilesSelected(t, filepath.Join(restored, strings.TrimPrefix(source, "/")), false, paths)
		}
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
			"source_payload_reads_measured": captured.SourceReads != nil, "production_repository_modified": false, "qualification_limit": "single comparison leg; no sustained-throughput or retained-history-growth claim"}, "", "  ")
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

func publicationRestoreGroups(paths map[string]bool) ([]string, error) {
	unique := make(map[string]bool)
	for path := range paths {
		if path == "." {
			continue
		}
		if !filepath.IsLocal(path) || filepath.Clean(path) != path {
			return nil, fmt.Errorf("unsafe restore selection %q", path)
		}
		group, _, _ := strings.Cut(path, "/")
		unique[group] = true
	}
	groups := make([]string, 0, len(unique))
	for group := range unique {
		groups = append(groups, group)
	}
	slices.Sort(groups)
	return groups, nil
}

func TestPublicationRestoreGroups(t *testing.T) {
	groups, err := publicationRestoreGroups(map[string]bool{".": true, "b": true, "a/x": true, "a/y": true})
	rtest.OK(t, err)
	if !slices.Equal(groups, []string{"a", "b"}) {
		t.Fatal("restore groups omitted or duplicated a subtree", groups)
	}
	for _, path := range []string{"../escape", "/absolute", "a/../b"} {
		if _, err := publicationRestoreGroups(map[string]bool{path: true}); err == nil {
			t.Fatal("unsafe restore selection accepted", path)
		}
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
