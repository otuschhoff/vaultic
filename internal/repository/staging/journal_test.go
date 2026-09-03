package staging

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type failingJournalBackend struct{ backend.Backend }

func (failingJournalBackend) Save(context.Context, backend.Handle, backend.RewindReader) error {
	return fmt.Errorf("mirror unavailable")
}

func journalHeader(now time.Time) Header {
	return Header{Format: 1, RepositoryID: "repo-a", JobID: "job-a", IdempotencyKey: "idem-a", CreatedAt: now, ExpiresAt: now.Add(time.Hour), CapsuleGeneration: 2, RepositoryKeyVersion: 1, ChunkerVersion: "rabin-v1", CompressionVersion: "zstd-v1", PlacementPolicyVersion: 3, SourceIdentitySHA256: strings.Repeat("ab", 32), ConsistencyEvidence: "full-crawl"}
}

func durablePack() Pack {
	return Pack{ID: "pack-a", Type: "data", Size: 42, PayloadSize: 20, HeaderSize: 22, BlobCount: 1, SHA256: strings.Repeat("cd", 32), Placements: []Placement{{BackendID: "a", FailureDomain: "one", Size: 42, SHA256: strings.Repeat("cd", 32)}, {BackendID: "b", FailureDomain: "two", Offsite: true, Size: 42, SHA256: strings.Repeat("cd", 32)}}}
}

func TestSegmentsSealOnlyAfterDurabilityAndContinuousChain(t *testing.T) {
	now := time.Now().UTC()
	key := []byte("0123456789abcdef0123456789abcdef")
	policy := Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}
	first := Segment{Header: journalHeader(now), Sequence: 1, Packs: []Pack{durablePack()}}
	encodedFirst, firstDigest, err := SealSegment(first, key, policy, now)
	if err != nil {
		t.Fatal(err)
	}
	second := Segment{Header: first.Header, Sequence: 2, PreviousSHA256: firstDigest, Records: []Record{{Kind: "snapshot", Payload: jsonRaw(`{"tree":"abc"}`)}}}
	encodedSecond, _, err := SealSegment(second, key, policy, now)
	if err != nil {
		t.Fatal(err)
	}
	encodedSeal, seal, sealDigest, err := SealSegments(first.Header, [][]byte{encodedFirst, encodedSecond}, key, policy, now)
	if err != nil || seal.PackCount != 1 || seal.ProtectedBytes != 42 {
		t.Fatalf("seal = %#v, %v", seal, err)
	}
	opened, openedDigest, err := OpenSeal(encodedSeal, key, first.Header)
	if err != nil || openedDigest != sealDigest || opened.State != StateSealedPending {
		t.Fatalf("opened seal = %#v, %q, %v", opened, openedDigest, err)
	}

	bad := durablePack()
	bad.Placements = bad.Placements[:1]
	if _, _, err := SealSegment(Segment{Header: first.Header, Sequence: 1, Packs: []Pack{bad}}, key, policy, now); err == nil {
		t.Fatal("under-replicated pack was sealed")
	}
	second.PreviousSHA256 = strings.Repeat("00", 32)
	badSecond, _, _ := SealSegment(second, key, policy, now)
	if _, _, _, err := SealSegments(first.Header, [][]byte{encodedFirst, badSecond}, key, policy, now); err == nil {
		t.Fatal("discontinuous segment chain was sealed")
	}
}

func TestJournalPublicationIsCreateOnlyOnEveryMirror(t *testing.T) {
	first, second := mem.New(), mem.New()
	encoded := []byte("sealed")
	handle := SealHandle("job-a")
	mirrors := map[string]backend.Backend{"a": first, "b": second}
	if err := Publish(context.Background(), mirrors, handle, encoded); err != nil {
		t.Fatal(err)
	}
	if err := Publish(context.Background(), mirrors, handle, encoded); err != nil {
		t.Fatalf("idempotent journal publication failed: %v", err)
	}
	if err := Publish(context.Background(), mirrors, handle, []byte("conflict")); err == nil {
		t.Fatal("conflicting journal object was accepted")
	}
}

func TestQuotaAndCompletionFailClosed(t *testing.T) {
	now := time.Now().UTC()
	if err := CheckQuota(Quota{MaxBytes: 100}, 0, 0, 90, time.Time{}, 11, now); err == nil {
		t.Fatal("byte quota was exceeded")
	}
	completion := Completion{Header: journalHeader(now), State: StateCommitted, SealSHA256: strings.Repeat("ab", 32), MetadataTransaction: "txn", SnapshotID: "snapshot", CompletedAt: now}
	if _, _, err := SealCompletion(completion, []byte("0123456789abcdef0123456789abcdef")); err != nil {
		t.Fatal(err)
	}
	completion.SnapshotID = ""
	if _, _, err := SealCompletion(completion, []byte("0123456789abcdef0123456789abcdef")); err == nil {
		t.Fatal("completion without snapshot authority was accepted")
	}
}

func TestStoreVerifiesMirrorsBeforeSealAndDiscoversCompletion(t *testing.T) {
	now := time.Now().UTC()
	key := []byte("0123456789abcdef0123456789abcdef")
	first, second := mem.New(), mem.New()
	store := Store{Mirrors: map[string]backend.Backend{"a": first, "b": second}, MirrorPlacements: map[string]MirrorPlacement{"a": {FailureDomain: "one"}, "b": {FailureDomain: "two", Offsite: true}}, Key: key, Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, Now: func() time.Time { return now }}
	header := journalHeader(now)
	segment := Segment{Header: header, Sequence: 1, Packs: []Pack{durablePack()}}
	if _, err := store.PublishSegment(context.Background(), segment); err != nil {
		t.Fatal(err)
	}
	seal, digest, err := store.PublishSeal(context.Background(), header, 1)
	if err != nil || seal.State != StateSealedPending {
		t.Fatalf("seal = %#v, %q, %v", seal, digest, err)
	}
	jobs, err := store.Discover(context.Background(), "repo-a")
	if err != nil || len(jobs) != 1 || jobs[0].State != StateSealedPending {
		t.Fatalf("pending discovery = %#v, %v", jobs, err)
	}
	completion := Completion{Header: header, State: StateCommitted, SealSHA256: digest, MetadataTransaction: "txn-a", SnapshotID: "snapshot-a", CompletedAt: now}
	encodedCompletion, _, err := SealCompletion(completion, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(context.Background(), store.Mirrors, CompletionHandle(header.JobID), encodedCompletion); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.Discover(context.Background(), "repo-a")
	if err != nil || len(jobs) != 1 || jobs[0].State != StateCommitted || jobs[0].Completion.SnapshotID != "snapshot-a" {
		t.Fatalf("completed discovery = %#v, %v", jobs, err)
	}
}

func TestStoreRefusesSealWhenMirrorSegmentIsMissingOrDifferent(t *testing.T) {
	now := time.Now().UTC()
	key := []byte("0123456789abcdef0123456789abcdef")
	first, second := mem.New(), mem.New()
	store := Store{Mirrors: map[string]backend.Backend{"a": first, "b": second}, MirrorPlacements: map[string]MirrorPlacement{"a": {FailureDomain: "one"}, "b": {FailureDomain: "two", Offsite: true}}, Key: key, Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, Now: func() time.Time { return now }}
	header := journalHeader(now)
	segment := Segment{Header: header, Sequence: 1, Packs: []Pack{durablePack()}}
	encoded, _, err := SealSegment(segment, key, store.Policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Save(context.Background(), SegmentHandle(header.JobID, 1), backend.NewByteReader(encoded, first.Hasher())); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.PublishSeal(context.Background(), header, 1); err == nil {
		t.Fatal("seal was published with a missing mirror segment")
	}
	if err := second.Save(context.Background(), SegmentHandle(header.JobID, 1), backend.NewByteReader(append(encoded, 'x'), second.Hasher())); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.PublishSeal(context.Background(), header, 1); err == nil {
		t.Fatal("seal was published with conflicting mirror segments")
	}
}

func TestPublishIsIdempotentCreateAndRejectsReplacement(t *testing.T) {
	mirror := mem.New()
	mirrors := map[string]backend.Backend{"a": mirror}
	handle := SegmentHandle("job-a", 1)
	if err := Publish(context.Background(), mirrors, handle, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := Publish(context.Background(), mirrors, handle, []byte("first")); err != nil {
		t.Fatalf("idempotent retry failed: %v", err)
	}
	if err := Publish(context.Background(), mirrors, handle, []byte("replacement")); err == nil {
		t.Fatal("immutable journal object was replaced")
	}
	stored, err := loadObject(context.Background(), mirror, handle)
	if err != nil || string(stored) != "first" {
		t.Fatalf("stored object = %q, %v", stored, err)
	}
}

func TestPackRootsProtectOnlySealedPendingJobs(t *testing.T) {
	now := time.Now().UTC()
	key := []byte("0123456789abcdef0123456789abcdef")
	store := Store{Mirrors: map[string]backend.Backend{"a": mem.New(), "b": mem.New()}, MirrorPlacements: map[string]MirrorPlacement{"a": {FailureDomain: "one"}, "b": {FailureDomain: "two", Offsite: true}}, Key: key, Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, Now: func() time.Time { return now }}
	header := journalHeader(now)
	pack := durablePack()
	pack.ID = strings.Repeat("12", 32)
	if _, err := store.PublishSegment(context.Background(), Segment{Header: header, Sequence: 1, Packs: []Pack{pack}}); err != nil {
		t.Fatal(err)
	}
	_, sealDigest, err := store.PublishSeal(context.Background(), header, 1)
	if err != nil {
		t.Fatal(err)
	}
	roots := PackRoots{Store: store, RepositoryID: "repo-a"}
	protected, err := roots.Current(context.Background())
	if err != nil || len(protected) != 1 {
		t.Fatalf("pending roots = %#v, %v", protected, err)
	}
	completion := Completion{Header: header, State: StateCommitted, SealSHA256: sealDigest, MetadataTransaction: "txn", SnapshotID: "snapshot", CompletedAt: now}
	if err := store.PublishCompletion(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	protected, err = roots.Current(context.Background())
	if err != nil || len(protected) != 0 {
		t.Fatalf("completed roots = %#v, %v", protected, err)
	}
}

func TestExpiredPackRootsRequireAcknowledgedAbandonmentAndSafetyDelay(t *testing.T) {
	now := time.Now().UTC()
	clock := now
	key := []byte("0123456789abcdef0123456789abcdef")
	store := Store{Mirrors: map[string]backend.Backend{"a": mem.New(), "b": mem.New()}, MirrorPlacements: map[string]MirrorPlacement{"a": {FailureDomain: "one"}, "b": {FailureDomain: "two", Offsite: true}}, Key: key, Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, Now: func() time.Time { return clock }, AbandonmentSafetyDelay: time.Hour}
	header := journalHeader(now)
	pack := durablePack()
	pack.ID = strings.Repeat("34", 32)
	if _, err := store.PublishSegment(context.Background(), Segment{Header: header, Sequence: 1, Packs: []Pack{pack}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.PublishSeal(context.Background(), header, 1); err != nil {
		t.Fatal(err)
	}
	roots := PackRoots{Store: store, RepositoryID: "repo-a"}
	clock = header.ExpiresAt.Add(time.Minute)
	protected, err := roots.Current(context.Background())
	if err != nil || len(protected) != 1 {
		t.Fatalf("expired roots = %#v, %v", protected, err)
	}
	jobs, err := store.Discover(context.Background(), "repo-a")
	if err != nil || len(jobs) != 1 || jobs[0].State != StateExpired {
		t.Fatalf("expired jobs = %#v, %v", jobs, err)
	}
	if _, err := store.PublishAbandonment(context.Background(), jobs[0], "operator cleanup", "I acknowledge staged data may be lost"); err != nil {
		t.Fatal(err)
	}
	protected, err = roots.Current(context.Background())
	if err != nil || len(protected) != 1 {
		t.Fatalf("safety-delayed roots = %#v, %v", protected, err)
	}
	clock = clock.Add(time.Hour)
	protected, err = roots.Current(context.Background())
	if err != nil || len(protected) != 0 {
		t.Fatalf("abandoned roots after delay = %#v, %v", protected, err)
	}
}

func TestPublishJobUsesMirrorPolicyQuorum(t *testing.T) {
	now := time.Now().UTC()
	store := Store{
		Mirrors:          map[string]backend.Backend{"a": mem.New(), "b": mem.New(), "c": failingJournalBackend{mem.New()}},
		MirrorPlacements: map[string]MirrorPlacement{"a": {FailureDomain: "one"}, "b": {FailureDomain: "two", Offsite: true}, "c": {FailureDomain: "three"}},
		Key:              []byte("0123456789abcdef0123456789abcdef"), Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, Now: func() time.Time { return now },
	}
	seal, _, segments, err := store.PublishJob(context.Background(), journalHeader(now), []Pack{durablePack()}, []Record{{Kind: "test", Payload: []byte(`{"ok":true}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if segments != 1 || seal.PackCount != 1 {
		t.Fatalf("seal = %#v, segments=%d", seal, segments)
	}
	jobs, err := store.Discover(context.Background(), "repo-a")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("discover = %#v, %v", jobs, err)
	}
	if _, err := store.VerifyJob(context.Background(), jobs[0]); err != nil {
		t.Fatal(err)
	}
}

func TestRejectedJournalRemainsProtected(t *testing.T) {
	now := time.Now().UTC()
	store := Store{
		Mirrors:          map[string]backend.Backend{"a": mem.New(), "b": mem.New()},
		MirrorPlacements: map[string]MirrorPlacement{"a": {FailureDomain: "one"}, "b": {FailureDomain: "two", Offsite: true}},
		Key:              []byte("0123456789abcdef0123456789abcdef"), Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, Now: func() time.Time { return now },
	}
	pack := durablePack()
	pack.ID = strings.Repeat("ef", 32)
	if _, _, _, err := store.PublishJob(context.Background(), journalHeader(now), []Pack{pack}, nil); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.Discover(context.Background(), "repo-a")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("discover = %#v, %v", jobs, err)
	}
	if _, err := store.PublishRejection(context.Background(), jobs[0], "basis conflict"); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.Discover(context.Background(), "repo-a")
	if err != nil || jobs[0].State != StateRejected || jobs[0].Rejection.Reason != "basis conflict" {
		t.Fatalf("rejected discovery = %#v, %v", jobs, err)
	}
	roots, err := (PackRoots{Store: store, RepositoryID: "repo-a"}).Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	packID, _ := vaultic.ParseID(pack.ID)
	if !roots.Has(packID) {
		t.Fatal("rejected journal pack lost GC protection")
	}
}

func TestJournalExpiryExtensionIsAuthenticatedAndBounded(t *testing.T) {
	now := time.Now().UTC()
	store := Store{
		Mirrors:          map[string]backend.Backend{"a": mem.New(), "b": mem.New()},
		MirrorPlacements: map[string]MirrorPlacement{"a": {FailureDomain: "one"}, "b": {FailureDomain: "two", Offsite: true}},
		Key:              []byte("0123456789abcdef0123456789abcdef"), Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, MaxExtension: 4 * time.Hour, Now: func() time.Time { return now },
	}
	if _, _, _, err := store.PublishJob(context.Background(), journalHeader(now), []Pack{durablePack()}, nil); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.Discover(context.Background(), "repo-a")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("discover = %#v, %v", jobs, err)
	}
	extendedExpiry := jobs[0].Header.ExpiresAt.Add(2 * time.Hour)
	if _, err := store.PublishExtension(context.Background(), jobs[0], extendedExpiry); err != nil {
		t.Fatal(err)
	}
	now = now.Add(90 * time.Minute)
	jobs, err = store.Discover(context.Background(), "repo-a")
	if err != nil || jobs[0].State != StateSealedPending || !jobs[0].EffectiveExpiresAt().Equal(extendedExpiry) {
		t.Fatalf("extended discovery = %#v, %v", jobs, err)
	}
	secondExpiry := extendedExpiry.Add(30 * time.Minute)
	if _, err := store.PublishExtension(context.Background(), jobs[0], secondExpiry); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.Discover(context.Background(), "repo-a")
	if err != nil || jobs[0].Extension.Generation != 2 || !jobs[0].EffectiveExpiresAt().Equal(secondExpiry) {
		t.Fatalf("second extension discovery = %#v, %v", jobs, err)
	}
	if _, err := store.PublishExtension(context.Background(), jobs[0], now.Add(5*time.Hour)); err == nil {
		t.Fatal("extension beyond repository policy was accepted")
	}
}

func jsonRaw(value string) []byte { return []byte(value) }
