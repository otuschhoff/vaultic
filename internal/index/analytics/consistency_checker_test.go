package analytics

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type consistencyReadErrorStore struct {
	*memoryStore
	failKey []byte
}

func (store *consistencyReadErrorStore) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if bytes.Equal(key, store.failKey) {
		return nil, false, errors.New("injected consistency read failure")
	}
	return store.memoryStore.Get(ctx, key)
}

func TestConsistencyCheckerIndexUnitClassifiesUnreadable(t *testing.T) {
	checker := newConsistencyChecker(context.Background(), newMemoryStore(), schema.AnalyticsMetadataRecord{})
	checker.checkSegmentIndex([]byte("ai:broken"), segmentRows{}, nil, []byte("broken"))

	assertConsistencyFinding(t, checker.findings, "unreadable", []byte("ai:broken"))
	assertConsistencyFinding(t, checker.findings, "analytics_index_mismatch", []byte("ai:broken"))
}

func TestCheckConsistencyCorruptValueIsUnreadable(t *testing.T) {
	ctx, store, metadata := consistencyTestStore(t)
	key := schema.AnalyticsManifestKey(metadata.Generation)
	store.values[string(key)] = []byte("broken")

	findings, err := CheckConsistency(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	assertConsistencyFinding(t, findings, "unreadable", key)
	assertConsistencyFinding(t, findings, "analytics_manifest_malformed", key)
}

func TestCheckConsistencyUnreadableMetadata(t *testing.T) {
	ctx, store, _ := consistencyTestStore(t)
	key := schema.AnalyticsMetadataKey()
	store.values[string(key)] = []byte("broken")

	findings, err := CheckConsistency(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	assertConsistencyFinding(t, findings, "unreadable", key)

	findings, err = CheckConsistency(ctx, &consistencyReadErrorStore{memoryStore: store, failKey: key})
	if err != nil {
		t.Fatal(err)
	}
	assertConsistencyFinding(t, findings, "unreadable", key)
}

func TestCheckConsistencyGetFailureIsUnreadableAndContinues(t *testing.T) {
	ctx, store, metadata := consistencyTestStore(t)
	markerKey := schema.AnalyticsDerivedGenerationMarkerKey(metadata.Generation)
	var dictionaryKey []byte
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), []byte("ad:")) {
			dictionaryKey = []byte(key)
			break
		}
	}
	store.values[string(dictionaryKey)] = []byte("broken")

	findings, err := CheckConsistency(ctx, &consistencyReadErrorStore{memoryStore: store, failKey: markerKey})
	if err != nil {
		t.Fatal(err)
	}
	assertConsistencyFinding(t, findings, "unreadable", markerKey)
	assertConsistencyFinding(t, findings, "analytics_dictionary_malformed", dictionaryKey)
}

func consistencyTestStore(t *testing.T) (context.Context, *memoryStore, schema.AnalyticsMetadataRecord) {
	t.Helper()
	ctx := context.Background()
	store := newMemoryStore()
	putRevision(t, store, 1, 1, 1, 7, 2, 10, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "/svm/vol/team/one", true)
	if _, err := Enable(ctx, store, Config{CacheAfter: 1}, false); err != nil {
		t.Fatal(err)
	}
	metadata, err := Status(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, store, metadata
}

func assertConsistencyFinding(t *testing.T, findings []ConsistencyFinding, kind string, key []byte) {
	t.Helper()
	for _, finding := range findings {
		if finding.Kind == kind && finding.Key == string(key) {
			return
		}
	}
	t.Fatalf("missing kind %q for key %q in findings: %+v", kind, key, findings)
}
