package staging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
)

type testAuthority struct {
	events    []string
	preflight error
	lookup    *CommitResult
	commit    error
}

func (authority *testAuthority) IntegrityPreflight(context.Context, Header) error {
	authority.events = append(authority.events, "preflight")
	return authority.preflight
}

func (authority *testAuthority) LookupIdempotency(context.Context, Job, []Segment, string) (CommitResult, bool, error) {
	authority.events = append(authority.events, "lookup")
	if authority.lookup == nil {
		return CommitResult{}, false, nil
	}
	return *authority.lookup, true, nil
}

func (authority *testAuthority) CommitMetadata(context.Context, Job, []Segment, string) (CommitResult, error) {
	authority.events = append(authority.events, "commit")
	return CommitResult{MetadataTransaction: "txn-a", SnapshotID: "snapshot-a"}, authority.commit
}

func (authority *testAuthority) PublishSnapshot(context.Context, CommitResult) error {
	authority.events = append(authority.events, "snapshot")
	return nil
}

type testVerifier struct{ events *[]string }

func (verifier testVerifier) VerifyPack(context.Context, Pack) error {
	*verifier.events = append(*verifier.events, "pack")
	return nil
}

func sealedTestJob(t *testing.T) (Store, Job) {
	t.Helper()
	now := time.Now().UTC()
	store := Store{
		Mirrors: map[string]backend.Backend{"a": mem.New(), "b": mem.New()},
		Key:     []byte("0123456789abcdef0123456789abcdef"),
		Policy:  Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1},
		Now:     func() time.Time { return now },
	}
	header := journalHeader(now)
	if _, err := store.PublishSegment(context.Background(), Segment{Header: header, Sequence: 1, Packs: []Pack{durablePack()}}); err != nil {
		t.Fatal(err)
	}
	seal, digest, err := store.PublishSeal(context.Background(), header, 1)
	if err != nil {
		t.Fatal(err)
	}
	return store, Job{Header: header, State: StateSealedPending, Seal: seal, SealSHA256: digest}
}

func TestPlanACommitsMetadataBeforeSnapshotAndCompletion(t *testing.T) {
	store, job := sealedTestJob(t)
	authority := &testAuthority{}
	result := Reconcile(context.Background(), store, authority, testVerifier{events: &authority.events}, job)
	if result.Disposition != ReconcileCommitted || result.SnapshotID != "snapshot-a" {
		t.Fatalf("result = %#v", result)
	}
	want := []string{"pack", "preflight", "lookup", "commit", "snapshot"}
	if len(authority.events) != len(want) {
		t.Fatalf("events = %#v", authority.events)
	}
	for index := range want {
		if authority.events[index] != want[index] {
			t.Fatalf("events = %#v", authority.events)
		}
	}
	jobs, err := store.Discover(context.Background(), "repo-a")
	if err != nil || len(jobs) != 1 || jobs[0].State != StateCommitted {
		t.Fatalf("completion discovery = %#v, %v", jobs, err)
	}
}

func TestPlanARecoversCommittedIdempotencyAndNeverAutoHeals(t *testing.T) {
	store, job := sealedTestJob(t)
	committed := CommitResult{MetadataTransaction: "txn-existing", SnapshotID: "snapshot-existing"}
	authority := &testAuthority{lookup: &committed}
	result := Reconcile(context.Background(), store, authority, testVerifier{events: &authority.events}, job)
	if result.Disposition != ReconcileCommitted || result.MetadataTransaction != "txn-existing" {
		t.Fatalf("recovered result = %#v", result)
	}
	for _, event := range authority.events {
		if event == "commit" {
			t.Fatal("metadata was duplicated after idempotency recovery")
		}
	}

	store, job = sealedTestJob(t)
	authority = &testAuthority{preflight: errors.New("corrupt manifest")}
	result = Reconcile(context.Background(), store, authority, testVerifier{events: &authority.events}, job)
	if result.Disposition != ReconcileHealingRequired {
		t.Fatalf("preflight disposition = %#v", result)
	}
	for _, event := range authority.events {
		if event == "commit" || event == "snapshot" {
			t.Fatalf("healing-required path mutated authority: %#v", authority.events)
		}
	}
}

func TestPlanARejectsExpiredOrAbandonedJournal(t *testing.T) {
	store, job := sealedTestJob(t)
	for _, state := range []State{StateExpired, StateAbandoned} {
		job.State = state
		authority := &testAuthority{}
		result := Reconcile(context.Background(), store, authority, testVerifier{events: &authority.events}, job)
		if result.Disposition != ReconcileRejected || len(authority.events) != 0 {
			t.Fatalf("state %s result = %#v, events = %#v", state, result, authority.events)
		}
	}
}
