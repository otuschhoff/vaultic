package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestRecordVerificationLifecycle(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "verification-state", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	pack, run1, run2, run3, run4 := testSchemaID(1), testSchemaID(2), testSchemaID(3), testSchemaID(4), testSchemaID(5)
	placement := schema.PlacementRecord{State: schema.PlacementLive, PlacementTimeKnown: true, PlacedAt: 1, Bytes: 100, RetentionSource: schema.RetentionUnknown}
	encoded, err := placement.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, schema.PackPlacementKey(pack, 7), encoded, true); err != nil {
		t.Fatal(err)
	}

	if err := store.RecordVerification(ctx, VerificationOutcome{PackID: pack, Backend: 7, Level: schema.VerificationFull, CompletedAt: time.Unix(100, 0), RunID: run1}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordVerification(ctx, VerificationOutcome{PackID: pack, Backend: 7, Level: schema.VerificationChecksum, CompletedAt: time.Unix(200, 0), RunID: run2, Classification: schema.VerificationTransport}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordVerification(ctx, VerificationOutcome{PackID: pack, Backend: 7, Level: schema.VerificationChecksum, CompletedAt: time.Unix(201, 0), RunID: run3, Classification: schema.VerificationTransport}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordVerification(ctx, VerificationOutcome{PackID: pack, Backend: 7, Level: schema.VerificationChecksum, CompletedAt: time.Unix(201, 0), RunID: run3, Classification: schema.VerificationTransport}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordVerification(ctx, VerificationOutcome{PackID: pack, Backend: 7, Level: schema.VerificationHeader, CompletedAt: time.Unix(202, 0), RunID: run4}); err != nil {
		t.Fatal(err)
	}

	value, found, err := store.Get(ctx, schema.VerificationStateKey(pack, 7))
	if err != nil || !found {
		t.Fatalf("state: found=%t err=%v", found, err)
	}
	state, err := schema.UnmarshalVerificationStateRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if state.HeaderVerifiedAt != 202 || state.ChecksumVerifiedAt != 100 || state.FullVerifiedAt != 100 || state.Result != schema.VerificationOperationalError || state.ConsecutiveFailures != 2 {
		t.Fatalf("unexpected unresolved state %#v", state)
	}
	if err := store.RecordVerification(ctx, VerificationOutcome{PackID: pack, Backend: 7, Level: schema.VerificationChecksum, CompletedAt: time.Unix(203, 0), RunID: testSchemaID(6)}); err != nil {
		t.Fatal(err)
	}
	value, _, err = store.Get(ctx, schema.VerificationStateKey(pack, 7))
	if err != nil {
		t.Fatal(err)
	}
	state, err = schema.UnmarshalVerificationStateRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if state.Result != schema.VerificationHealthy || state.ChecksumVerifiedAt != 203 || state.HeaderVerifiedAt != 203 || state.FullVerifiedAt != 100 {
		t.Fatalf("unexpected resolved state %#v", state)
	}
	items, _, err := store.ScanPrefix(ctx, schema.VerificationEventPrefix(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("event count = %d, want detected and resolved", len(items))
	}
	value, found, err = store.Get(ctx, schema.PackPlacementKey(pack, 7))
	if err != nil || !found {
		t.Fatal(err)
	}
	placement, err = schema.UnmarshalPlacementRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if placement.LastVerifiedAt != 203 {
		t.Fatalf("coarse last verified = %d", placement.LastVerifiedAt)
	}
	if _, found, err := store.Get(ctx, schema.VerificationStateKey(pack, 8)); err != nil || found {
		t.Fatalf("backend state leaked: found=%t err=%v", found, err)
	}
}

func testSchemaID(value byte) schema.ID { var id schema.ID; id[0] = value; return id }
