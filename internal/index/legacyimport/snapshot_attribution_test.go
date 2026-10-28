package legacyimport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend/local"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
	"github.com/otuschhoff/vaultic/internal/workingstate"
)

type catalogSnapshotSource struct{ *repository.Repository }

func (source catalogSnapshotSource) List(ctx context.Context, kind vaultic.FileType, visit func(vaultic.ID, int64) error) error {
	if kind == vaultic.SnapshotFile {
		return nil
	}
	return source.Repository.List(ctx, kind, visit)
}

type attributionDebtStore struct {
	*daemon.SchemaStore
	expected map[string][]byte
	charged  uint64
}

func (store *attributionDebtStore) remember(ctx context.Context, key, value []byte) error {
	charge := uint64(len(key)+len(value))*2 + 256
	if store.charged+charge > 1<<30 {
		return errors.New("attribution reference exceeds its 1 GiB admission limit")
	}
	if _, err := workingstate.Reserve(ctx, charge); err != nil {
		return err
	}
	store.charged += charge
	store.expected[string(key)] = bytes.Clone(value)
	return nil
}

func (store *attributionDebtStore) WriteMutableBatch(ctx context.Context, puts []daemon.Mutation, deletes [][]byte, durable bool) error {
	if err := store.SchemaStore.WriteMutableBatch(ctx, puts, deletes, durable); err != nil {
		return err
	}
	for _, put := range puts {
		if bytes.HasPrefix(put.Key, []byte("q:")) {
			if err := store.remember(ctx, put.Key, put.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func attributionScan(ctx context.Context, store *daemon.SchemaStore, visit func([]byte, []byte) error) error {
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, []byte("q:"), after, 4096)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if after != nil && bytes.Compare(entry.Key, after) <= 0 {
				return errors.New("attribution scan order changed")
			}
			if err := visit(entry.Key, entry.Value); err != nil {
				return err
			}
			after = bytes.Clone(entry.Key)
		}
		if done {
			return nil
		}
		if len(entries) == 0 {
			return errors.New("attribution scan made no progress")
		}
	}
}

func attributionTraverse(ctx context.Context, source SnapshotSource, store TreeStore, snapshots vaultic.IDs, options Options) (*Result, uint64, error) {
	result := &Result{}
	var stops uint64
	for _, snapshotID := range snapshots {
		original, err := (legacySnapshotSource{source}).LoadUnpacked(ctx, vaultic.SnapshotFile, snapshotID)
		if err != nil {
			return nil, stops, err
		}
		var snapshot data.Snapshot
		if err := json.Unmarshal(original, &snapshot); err != nil || snapshot.Tree == nil || snapshot.Tree.IsNull() {
			return nil, stops, errors.New("attribution snapshot root invalid")
		}
		options.SnapshotWorkBudget = result.NodesVisited + 1000
		importer := &treeImporter{ctx: ctx, source: source, store: store, options: options,
			result: result, snapshot: snapshotID, ancestors: make(map[vaultic.ID]struct{}),
			pendingCurrent: make(map[string]struct{}), reportProgress: func() {}}
		err = func() error {
			release := importer.admitDebtBatch()
			defer release()
			if options.SnapshotTreePrefetch {
				prefetch, err := newSnapshotTreePrefetch(ctx, source)
				if err != nil {
					return err
				}
				importer.prefetch = prefetch
				defer prefetch.close()
			}
			if options.SnapshotDebtWriteOverlap {
				if err := importer.startDebtWriter(); err != nil {
					return err
				}
				defer importer.stopDebtWriter()
			}
			_, _, err := importer.importTree(*snapshot.Tree, nil, "", 0)
			if errors.Is(err, ErrLimitReached) {
				stops++
			} else if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := importer.flushRevisions(); err != nil {
				return err
			}
			if err := importer.flushDebt(); err != nil {
				return err
			}
			return importer.finishDebtWriter()
		}()
		if err != nil {
			return nil, stops, err
		}
		result.SnapshotsSeen++
	}
	return result, stops, nil
}

func TestM6PopulatedCatalogAttribution(t *testing.T) {
	if os.Getenv("VAULTIC_M6_POPULATED_ATTRIBUTION") != "1" {
		t.Skip("opt-in fresh full-catalog actual-source attribution; not full M6 acceptance")
	}
	variant := os.Getenv("VAULTIC_M6_VARIANT")
	if variant != "baseline" && variant != "overlap" && variant != "prefetch" && variant != "combined" {
		t.Fatal("explicit diagnostic variant required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Hour)
	defer cancel()
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 160<<30, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx = workingkv.WithPolicy(ctx, policy)
	be, err := local.Open(ctx, local.Config{Path: os.Getenv("VAULTIC_M6_DIAGNOSTIC_REPO"), Connections: 8}, func(string, ...any) {})
	if err != nil {
		t.Fatal("diagnostic source open failed")
	}
	defer be.Close()
	repo, err := repository.New(be, repository.Options{Compression: repository.CompressionFastest})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	password, err := os.ReadFile(os.Getenv("VAULTIC_M6_DIAGNOSTIC_PASSWORD_FILE"))
	if err != nil || repo.SearchKey(ctx, strings.TrimRight(string(password), "\r\n"), 10, "") != nil {
		t.Fatal("diagnostic source unlock failed")
	}
	clear(password)
	if err := repo.LoadIndex(ctx, vaultic.NoopTerminalCounterFactory); err != nil {
		t.Fatal("full source index preload failed", err)
	}
	directory, err := os.MkdirTemp("", "c-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	if err := os.Chmod(directory, 0o700); err != nil || len(filepath.Join(directory, "d.sock")) >= 100 {
		t.Fatal("private runtime or socket length invalid", err)
	}
	nativeOptions := daemon.Options{Socket: filepath.Join(directory, "d.sock"), RepositoryID: "m6-populated-attribution",
		DaemonPath: os.Getenv("VAULTICDB_TEST_BINARY"), DataDir: filepath.Join(directory, "db"), ObjectStore: "local",
		WALStore: "memory", WALDataDir: filepath.Join(directory, "wal"), RebuildReset: true, FreshBulkImport: true}
	client, err := daemon.Ensure(ctx, nativeOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := &attributionDebtStore{SchemaStore: daemon.NewSchemaStore(client), expected: make(map[string][]byte)}
	defer func() { policy.Release(store.charged) }()
	prepared := time.Now()
	catalog, err := Import(ctx, catalogSnapshotSource{repo}, repo.Backend(), store.SchemaStore, Options{SnapshotDepth: 0})
	if err != nil || catalog.IndexesSeen != 10170 || catalog.PacksImported != 419853 || catalog.BlobsImported != 380166821 || catalog.SnapshotsSeen != 0 {
		t.Fatal("fresh full native catalog population failed or differs from frozen counts", catalog, err)
	}
	if err := attributionScan(ctx, store.SchemaStore, func(key, value []byte) error { return store.remember(ctx, key, value) }); err != nil {
		t.Fatal(err)
	}
	var snapshots vaultic.IDs
	if err := repo.List(ctx, vaultic.SnapshotFile, func(id vaultic.ID, _ int64) error { snapshots = append(snapshots, id); return nil }); err != nil {
		t.Fatal(err)
	}
	sort.Slice(snapshots, func(first, second int) bool { return bytes.Compare(snapshots[first][:], snapshots[second][:]) < 0 })
	if len(snapshots) != 143 {
		t.Fatal("all 143 frozen roots required")
	}
	preparation := time.Since(prepared)
	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	telemetry := NewSchedulerTelemetry()
	started := time.Now()
	result, stops, err := attributionTraverse(ctx, repo, store, snapshots, Options{SnapshotDepth: math.MaxUint, DeferSnapshotDurability: true,
		SnapshotDebtWriteOverlap: variant == "overlap" || variant == "combined", SnapshotTreePrefetch: variant == "prefetch" || variant == "combined", Telemetry: telemetry})
	elapsed := time.Since(started)
	if err != nil || result.ErrorsSeen != 0 || result.SnapshotsSeen != 143 {
		t.Fatal("bounded populated traversal failed", result, err)
	}
	after, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkBulkImportComplete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	nativeOptions.WALStore, nativeOptions.RebuildReset, nativeOptions.FreshBulkImport = "local", false, false
	reopened, err := daemon.Ensure(ctx, nativeOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	hash := sha256.New()
	var count uint64
	err = attributionScan(ctx, daemon.NewSchemaStore(reopened), func(key, value []byte) error {
		if expected, found := store.expected[string(key)]; !found || !bytes.Equal(expected, value) {
			return errors.New("reopened populated-catalog debt bytes differ")
		}
		count++
		for _, field := range [][]byte{key, value} {
			if err := binary.Write(hash, binary.BigEndian, uint64(len(field))); err != nil {
				return err
			}
			_, _ = hash.Write(field)
		}
		return nil
	})
	if err != nil || count != uint64(len(store.expected)) || count == 0 {
		t.Fatal("exhaustive reopened debt verification failed", count, err)
	}
	event := map[string]any{"schema": "m6-populated-attribution-v1", "variant": variant, "pid": os.Getpid(), "catalog_populated": true,
		"catalog_counts": []uint64{catalog.IndexesSeen, catalog.PacksImported, catalog.BlobsImported}, "root_count": len(snapshots), "node_cap_per_root": 1000,
		"facts":      []uint64{result.TreesVisited, result.NodesVisited, result.NodesImported, result.CrawlDebtCreated, result.ErrorsSeen, stops},
		"elapsed_ns": elapsed.Nanoseconds(), "catalog_preparation_ns": preparation.Nanoseconds(), "exact_reopened_debt_records": count,
		"debt_sha256": fmt.Sprintf("%x", hash.Sum(nil)), "native_before": before, "native_after": after, "operations": telemetry.Snapshot().Operations,
		"captured_unix_ms": time.Now().UnixMilli(), "full_scope": false, "full_catalog_audit": false, "acceptance": false}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("M6_POPULATED %s", encoded)
}
