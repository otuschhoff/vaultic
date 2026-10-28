package legacyimport

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
)

type blockingDebtStore struct {
	*memoryStore
	started chan struct{}
	permit  chan struct{}
	keys    []string
	failure error
}

func (store *blockingDebtStore) WriteMutableBatch(ctx context.Context, puts []daemon.Mutation, deletes [][]byte, durable bool) error {
	store.started <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-store.permit:
	}
	if store.failure != nil {
		return store.failure
	}
	for _, put := range puts {
		store.keys = append(store.keys, string(put.Key))
	}
	return store.memoryStore.WriteMutableBatch(ctx, puts, deletes, durable)
}

func newDebtPipelineFixture(t *testing.T, budget uint64) (*treeImporter, *blockingDebtStore, *workingkv.Policy, context.CancelFunc) {
	t.Helper()
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, budget, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(workingkv.WithPolicy(t.Context(), policy), 5*time.Second)
	store := &blockingDebtStore{memoryStore: newMemoryStore(), started: make(chan struct{}, 4), permit: make(chan struct{}, 4)}
	importer := &treeImporter{ctx: ctx, store: store}
	release := importer.admitDebtBatch()
	t.Cleanup(func() {
		cancel()
		importer.stopDebtWriter()
		release()
		if policy.Budget().Used != 0 {
			t.Error("pipeline retained its memory reservation", policy.Budget())
		}
	})
	return importer, store, policy, cancel
}

func TestSnapshotDebtPipelineOrderAndAccounting(t *testing.T) {
	importer, store, policy, _ := newDebtPipelineFixture(t, 3*snapshotDebtWorkingCharge)
	if err := importer.startDebtWriter(); err != nil {
		t.Fatal(err)
	}
	if policy.Budget().Used != 3*snapshotDebtWorkingCharge {
		t.Fatal("three live buffers were not charged", policy.Budget())
	}
	for _, key := range []string{"first", "second"} {
		importer.pendingDebt = append(importer.pendingDebt, daemon.Mutation{Key: []byte(key), Value: []byte(key)})
		if err := importer.flushDebt(); err != nil {
			t.Fatal(err)
		}
	}
	<-store.started
	if len(importer.debtWriter.jobs) != 1 {
		t.Fatal("one queued batch required")
	}
	store.permit <- struct{}{}
	store.permit <- struct{}{}
	if err := importer.finishDebtWriter(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.keys, []string{"first", "second"}) || len(store.values) != 2 {
		t.Fatal("write order or retained bytes changed", store.keys, store.values)
	}
	if err := importer.finishDebtWriter(); err != nil {
		t.Fatal("finish is not idempotent", err)
	}
}

func TestSnapshotDebtPipelineFailureAndCancellation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "write-error"}[fail], func(t *testing.T) {
			importer, store, _, cancel := newDebtPipelineFixture(t, 3*snapshotDebtWorkingCharge)
			injected := errors.New("injected write failure")
			if fail {
				store.failure = injected
			}
			if err := importer.startDebtWriter(); err != nil {
				t.Fatal(err)
			}
			importer.pendingDebt = append(importer.pendingDebt, daemon.Mutation{Key: []byte("key"), Value: []byte("value")})
			if err := importer.flushDebt(); err != nil {
				t.Fatal(err)
			}
			<-store.started
			if fail {
				store.permit <- struct{}{}
			} else {
				cancel()
			}
			err := importer.finishDebtWriter()
			if fail && !errors.Is(err, injected) || !fail && !errors.Is(err, context.Canceled) {
				t.Fatal("pipeline did not propagate failure", err)
			}
		})
	}
}

func TestSnapshotDebtPipelineRejectsInsufficientBudget(t *testing.T) {
	importer, _, policy, _ := newDebtPipelineFixture(t, snapshotDebtWorkingCharge)
	if err := importer.startDebtWriter(); err == nil || importer.debtWriter != nil || policy.Budget().Used != snapshotDebtWorkingCharge {
		t.Fatal("unaccounted pipeline admitted", err, policy.Budget())
	}
}

func TestSnapshotDebtPipelineOversizeUsesSynchronousReservation(t *testing.T) {
	importer, store, policy, _ := newDebtPipelineFixture(t, 3*snapshotDebtWorkingCharge)
	if err := importer.startDebtWriter(); err != nil {
		t.Fatal(err)
	}
	store.permit <- struct{}{}
	result := &Result{}
	if err := importer.writeDebt(debtWrite{result: result, pathHint: strings.Repeat("x", snapshotDebtAdmittedBytes), reason: schema.DebtUnknownFreshness}); err != nil {
		t.Fatal(err)
	}
	if importer.debtWriter != nil || len(store.values) != 1 || policy.Budget().Used != snapshotDebtWorkingCharge {
		t.Fatal("oversized asynchronous buffer escaped its reservation", policy.Budget())
	}
}

func TestSnapshotExperimentalPublicationAndEquivalence(t *testing.T) {
	leaf := treeJSON(t, &data.Node{Name: "file", Type: data.NodeTypeFile})
	leafID := vaultic.Hash(leaf)
	root := treeJSON(t,
		&data.Node{Name: "first", Type: data.NodeTypeDir, Subtree: &leafID},
		&data.Node{Name: "second", Type: data.NodeTypeDir, Subtree: &leafID},
	)
	rootID := vaultic.Hash(root)
	snapshotID := vaultic.Hash([]byte("experimental snapshot"))
	original, err := json.Marshal(data.Snapshot{Tree: &rootID})
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string][]byte
	for _, variant := range []string{"serial", "overlap", "prefetch", "combined", "write-error"} {
		t.Run(variant, func(t *testing.T) {
			policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 32<<20, "")
			if err != nil {
				t.Fatal(err)
			}
			source := &prefetchTestSource{memorySource: &memorySource{
				snapshots: map[vaultic.ID][]byte{snapshotID: original},
				blobs:     map[vaultic.ID][]byte{rootID: root, leafID: leaf},
			}}
			store := &diagnosticDebtStore{memoryStore: newMemoryStore(), fail: variant == "write-error"}
			options := Options{SnapshotDepth: 8, SnapshotDebtWriteOverlap: variant == "overlap" || variant == "combined" || variant == "write-error", SnapshotTreePrefetch: variant == "prefetch" || variant == "combined"}
			result, err := Import(workingkv.WithPolicy(t.Context(), policy), source, fixedStatter{}, store, options)
			if variant == "write-error" {
				if err == nil || result.SnapshotsImported != 0 {
					t.Fatal("failed asynchronous write published snapshot", result, err)
				}
				for _, key := range [][]byte{schema.SnapshotKey(schema.ID(snapshotID)), schema.SnapshotImportCheckpointKey(schema.ID(snapshotID))} {
					if _, found := store.values[string(key)]; found {
						t.Fatal("failed write published completion")
					}
				}
			} else {
				if err != nil || result.SnapshotsImported != 1 || result.NodesVisited != 4 {
					t.Fatal("experimental traversal changed work", result, err)
				}
				if variant == "serial" {
					expected = store.values
				} else if !reflect.DeepEqual(expected, store.values) {
					t.Fatal("experimental traversal changed persisted bytes")
				}
			}
			if policy.Budget().Used != 0 {
				t.Fatal("snapshot retained experimental reservations", policy.Budget())
			}
		})
	}
}
