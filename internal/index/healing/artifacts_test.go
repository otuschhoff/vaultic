package healing

import (
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

func testPlan(t *testing.T) (Plan, []byte) {
	t.Helper()
	key := []byte("0123456789abcdef0123456789abcdef")
	plan, err := NewPlan("repo", "candidate-2", "reuse-recovered", daemon.GenerationStatus{Decision: 3, ActiveGeneration: 1}, Inventory{Sources: []Source{{Kind: "legacy-index", ID: "index-a", Authority: 2, Authenticated: true}}}, key, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	return plan, key
}

func TestPlanAndReportAuthentication(t *testing.T) {
	plan, key := testPlan(t)
	if err := VerifyPlan(plan, key); err != nil {
		t.Fatal(err)
	}
	tampered := plan
	tampered.CandidateNamespace = "other"
	if VerifyPlan(tampered, key) == nil {
		t.Fatal("tampered plan was accepted")
	}
	gates := Gates{Identity: true, AntiRollback: true, StructuralAEAD: true, PacksAndBlobOffsets: true, TreesAndSnapshots: true, PlacementPolicy: true, JournalCompletions: true, LegacyComparison: true, ReadOnlyInspection: true}
	report, err := NewReport(plan, gates, nil, []string{"unprovable inode timestamps"}, nil, map[string]uint64{"packs": 4}, key, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyReport(report, plan, key); err != nil {
		t.Fatal(err)
	}
	report.ObjectCounts["packs"]++
	if VerifyReport(report, plan, key) == nil {
		t.Fatal("tampered report was accepted")
	}
}

func TestReportRequiresEveryActivationGate(t *testing.T) {
	plan, key := testPlan(t)
	report, err := NewReport(plan, Gates{}, nil, nil, nil, nil, key, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if VerifyReport(report, plan, key) == nil {
		t.Fatal("report with failed activation gates authorized activation")
	}
}

func TestStoreReplacesAuthenticatedCheckpoint(t *testing.T) {
	plan, key := testPlan(t)
	store := Store{Directory: t.TempDir()}
	if err := store.SavePlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := store.SavePlan(plan); err == nil {
		t.Fatal("immutable plan was overwritten")
	}
	checkpoint := SignCheckpoint(Checkpoint{PlanID: plan.ID, State: "rebuilding", UpdatedAt: time.Unix(300, 0)}, key)
	if err := store.SaveCheckpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	checkpoint.State = "candidate-ready"
	checkpoint = SignCheckpoint(checkpoint, key)
	if err := store.SaveCheckpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadCheckpoint(plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != "candidate-ready" || VerifyCheckpoint(loaded, plan.ID, key) != nil {
		t.Fatalf("checkpoint = %#v", loaded)
	}
}
