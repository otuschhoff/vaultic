package repository

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type readCacheFaultBackend struct {
	inner backend.Backend

	cas backend.ConditionalWriter

	loseCASResponseOnce atomic.Bool
	failCAS             atomic.Bool
	failControlLoad     atomic.Bool

	policyBodies   [][]byte
	policyLoads    atomic.Uint64
	quotaBodies    [][]byte
	quotaLoads     atomic.Uint64
	alwaysCASQuota atomic.Bool
	quotaCASCalls  atomic.Uint64
	failQuotaCASAt atomic.Uint64

	failRemove          atomic.Bool
	failStagingLoad     atomic.Bool
	failStagingLoadOnce atomic.Bool
	failStagingTimeout  atomic.Bool
	stagingLoadCalls    atomic.Uint64
	failStatOnce        atomic.Bool
	hangList            atomic.Bool
	hangChunkSave       atomic.Bool
}

func newReadCacheFaultBackend(inner backend.Backend) *readCacheFaultBackend {
	return &readCacheFaultBackend{inner: inner, cas: backend.AsCapability[backend.ConditionalWriter](inner)}
}

func (be *readCacheFaultBackend) Properties() backend.Properties { return be.inner.Properties() }
func (be *readCacheFaultBackend) Hasher() hash.Hash              { return be.inner.Hasher() }
func (be *readCacheFaultBackend) Remove(ctx context.Context, h backend.Handle) error {
	if be.failRemove.Load() && h.Type == backend.StagingFile {
		return errors.New("injected remove outage")
	}
	return be.inner.Remove(ctx, h)
}
func (be *readCacheFaultBackend) Close() error { return be.inner.Close() }
func (be *readCacheFaultBackend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	if be.hangChunkSave.Load() && h.Type == backend.StagingFile && strings.Contains(h.Name, "/chunks/") {
		<-ctx.Done()
		return ctx.Err()
	}
	return be.inner.Save(ctx, h, rd)
}
func (be *readCacheFaultBackend) Load(ctx context.Context, h backend.Handle, length int, offset int64, fn func(io.Reader) error) error {
	if h.Type == backend.StagingFile && strings.HasPrefix(h.Name, readCacheControlPrefix) && be.failControlLoad.Load() {
		return errors.New("injected control load outage")
	}
	if h.Type == backend.StagingFile && strings.HasPrefix(h.Name, readCacheNamespace+"/control/repositories/") && strings.HasSuffix(h.Name, "/policy.json") {
		if len(be.policyBodies) != 0 {
			index := int(be.policyLoads.Add(1)-1) % len(be.policyBodies)
			payload := be.policyBodies[index]
			return fn(bytes.NewReader(payload))
		}
	}
	if h.Type == backend.StagingFile && h.Name == readCacheNamespace+"/control/quota.json" {
		if len(be.quotaBodies) != 0 {
			index := int(be.quotaLoads.Add(1)-1) % len(be.quotaBodies)
			payload := be.quotaBodies[index]
			return fn(bytes.NewReader(payload))
		}
	}
	if h.Type == backend.StagingFile && strings.HasPrefix(h.Name, readCacheNamespace+"/") && !strings.Contains(h.Name, "/control/") {
		be.stagingLoadCalls.Add(1)
		if be.failStagingTimeout.Load() {
			<-ctx.Done()
			return ctx.Err()
		}
		if be.failStagingLoad.Load() {
			return errors.New("injected staging load outage")
		}
		if be.failStagingLoadOnce.Swap(false) {
			return errors.New("injected one-shot staging load outage")
		}
	}
	return be.inner.Load(ctx, h, length, offset, fn)
}
func (be *readCacheFaultBackend) Stat(ctx context.Context, h backend.Handle) (backend.FileInfo, error) {
	if be.failStatOnce.Swap(false) && h.Type == backend.StagingFile && strings.HasPrefix(h.Name, readCacheNamespace+"/") {
		return backend.FileInfo{}, errors.New("injected stat outage")
	}
	return be.inner.Stat(ctx, h)
}
func (be *readCacheFaultBackend) List(ctx context.Context, t backend.FileType, fn func(backend.FileInfo) error) error {
	if be.hangList.Load() && t == backend.StagingFile {
		<-ctx.Done()
		return ctx.Err()
	}
	return be.inner.List(ctx, t, fn)
}
func (be *readCacheFaultBackend) IsNotExist(err error) bool { return be.inner.IsNotExist(err) }
func (be *readCacheFaultBackend) IsPermanentError(err error) bool {
	return be.inner.IsPermanentError(err)
}
func (be *readCacheFaultBackend) Delete(ctx context.Context) error { return be.inner.Delete(ctx) }
func (be *readCacheFaultBackend) Warmup(ctx context.Context, h []backend.Handle) ([]backend.Handle, error) {
	return be.inner.Warmup(ctx, h)
}
func (be *readCacheFaultBackend) WarmupWait(ctx context.Context, h []backend.Handle) error {
	return be.inner.WarmupWait(ctx, h)
}

func (be *readCacheFaultBackend) CompareAndSwap(ctx context.Context, h backend.Handle, expected []byte, replacement []byte) ([]byte, bool, error) {
	if be.failCAS.Load() {
		return nil, false, errors.New("injected CAS outage")
	}
	if h.Type == backend.StagingFile && h.Name == readCacheNamespace+"/control/quota.json" {
		call := be.quotaCASCalls.Add(1)
		if failAt := be.failQuotaCASAt.Load(); failAt > 0 && call == failAt {
			return nil, false, errors.New("injected quota CAS outage")
		}
	}
	if be.alwaysCASQuota.Load() && h.Type == backend.StagingFile && h.Name == readCacheNamespace+"/control/quota.json" {
		current, _ := loadAll(be.inner, h)
		return current, false, nil
	}
	if be.cas == nil {
		return nil, false, backend.ErrConditionalWriteUnsupported
	}
	current, swapped, err := be.cas.CompareAndSwap(ctx, h, expected, replacement)
	if err != nil {
		return current, swapped, err
	}
	if swapped && be.loseCASResponseOnce.Swap(false) {
		return nil, false, errors.New("injected response loss after commit")
	}
	return current, swapped, nil
}

var _ backend.Backend = (*readCacheFaultBackend)(nil)
var _ backend.ConditionalWriter = (*readCacheFaultBackend)(nil)

type readCacheNonConditionalBackend struct {
	backend.Backend
}

func newTestReadCacheManager(t *testing.T, coordinator backend.Backend, cache backend.Backend, managerID string, maxBytes uint64) *readCacheManager {
	return newTestReadCacheManagerForRepo(t, coordinator, cache, "coord-test-repo", bytes.Repeat([]byte{0xAB}, 32), managerID, maxBytes)
}

func newTestReadCacheManagerForRepo(
	t *testing.T,
	coordinator backend.Backend,
	cache backend.Backend,
	repositoryID string,
	repositoryKey []byte,
	managerID string,
	maxBytes uint64,
) *readCacheManager {
	t.Helper()
	manager := &readCacheManager{
		repoID:             repositoryID,
		key:                append([]byte(nil), repositoryKey...),
		coalesced:          map[string]*readCacheFillGate{},
		coordinator:        backend.AsCapability[backend.ConditionalWriter](coordinator),
		coordinatorBackend: coordinator,
		managerID:          managerID,
		aggregateMax:       maxBytes,
		requestedAggregate: maxBytes,
		publishedAggregate: maxBytes,
		maxInflightBytes:   max(16*1024*1024, maxBytes),
		admissions:         true,
	}
	manager.capacityController = newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:          readCacheBudgetModeFixed,
		FixedMaxBytes: maxBytes,
	}, nil)
	manager.capacityDecision = manager.capacityController.snapshot(time.Now().UTC())
	tier := &readCacheTier{
		manager:       manager,
		id:            "rc",
		trust:         readCacheTrustEncrypted,
		trustAck:      false,
		codec:         "raw",
		enabled:       true,
		ingest:        true,
		readPrio:      readCacheDefaultPrio,
		admitPrio:     readCacheDefaultPrio,
		maxBytes:      maxBytes,
		requested:     maxBytes,
		chunkSize:     128,
		backend:       cache,
		entries:       map[string]*readCacheEntry{},
		deleting:      map[string]*readCacheEntry{},
		probeFailures: map[string]uint32{},
		negativeUntil: map[string]time.Time{},
	}
	manager.tiers = []*readCacheTier{tier}
	manager.initializeCoordinator(context.Background())
	return manager
}

func TestReadCacheCoordinatorTwoManagerAdmissionRace(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	left := newTestReadCacheManager(t, shared, cache, "manager-left", 700)
	right := newTestReadCacheManager(t, shared, cache, "manager-right", 700)
	chunk := bytes.Repeat([]byte("x"), 220)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		left.storeRange(context.Background(), left.tiers[0], vaultic.ID{1}, 0, chunk)
	}()
	go func() {
		defer wg.Done()
		right.storeRange(context.Background(), right.tiers[0], vaultic.ID{2}, 0, chunk)
	}()
	wg.Wait()

	_, ledger, err := left.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _, used, reserved := quotaUsage(ledger)
	if used+reserved > 700 {
		t.Fatalf("shared coordinator over-admitted: used=%d reserved=%d max=%d", used, reserved, 700)
	}
}

func TestReadCacheCoordinatorPublishedChargeUsesPersistedFootprint(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-exact-charge", 4096)
	chunk := bytes.Repeat([]byte("x"), 220)

	manager.storeRange(context.Background(), manager.tiers[0], vaultic.ID{31}, 0, chunk)

	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("expected one published quota entry, got %#v", ledger.Entries)
	}
	entry := ledger.Entries[0]
	if !quotaEntryIsPublished(entry.State) {
		t.Fatalf("quota entry state = %q, want published", entry.State)
	}
	manager.tiers[0].mu.Lock()
	used := manager.tiers[0].used
	manager.tiers[0].mu.Unlock()
	if entry.Bytes != used {
		t.Fatalf("published quota charge = %d, want persisted footprint %d", entry.Bytes, used)
	}
	if entry.Bytes >= 2*(uint64(len(chunk))+512) {
		t.Fatalf("published quota retained transient reservation: %d", entry.Bytes)
	}
}

func TestReadCacheCoordinatorDistinctRepositoriesShareQuota(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	left := newTestReadCacheManagerForRepo(t, shared, shared, "repo-left", bytes.Repeat([]byte{0x11}, 32), "manager-left", 900)
	right := newTestReadCacheManagerForRepo(t, shared, shared, "repo-right", bytes.Repeat([]byte{0x22}, 32), "manager-right", 900)
	chunk := bytes.Repeat([]byte("x"), 220)

	var wg sync.WaitGroup
	for i, manager := range []*readCacheManager{left, right, left, right} {
		wg.Add(1)
		go func(index int, current *readCacheManager) {
			defer wg.Done()
			current.storeRange(context.Background(), current.tiers[0], vaultic.ID{byte(index + 1)}, 0, chunk)
		}(i, manager)
	}
	wg.Wait()

	_, ledger, err := left.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _, used, reserved := quotaUsage(ledger)
	if used+reserved > 900 {
		t.Fatalf("distinct repositories exceeded shared cache budget: used=%d reserved=%d max=%d", used, reserved, 900)
	}
	for _, entry := range ledger.Entries {
		if entry.RepositoryID != left.repoID && entry.RepositoryID != right.repoID {
			t.Fatalf("unexpected repository scope in shared quota: %#v", entry)
		}
	}
}

func TestReadCacheDomainControlFailsClosed(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		handle := backend.Handle{Type: backend.StagingFile, Name: readCacheNamespace + "/control/domain.key"}
		if err := shared.Save(context.Background(), handle, backend.NewByteReader([]byte("short"), shared.Hasher())); err != nil {
			t.Fatal(err)
		}
		manager := newTestReadCacheManager(t, shared, shared, "manager-malformed-domain", 1024)
		if manager.admissions {
			t.Fatal("malformed domain control must disable admissions")
		}
	})

	t.Run("tampered", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		seed := newTestReadCacheManager(t, shared, shared, "manager-domain-seed", 1024)
		if err := shared.Remove(context.Background(), seed.controlKeyHandle()); err != nil {
			t.Fatal(err)
		}
		tampered := bytes.Repeat([]byte{0xEE}, readCacheControlKeySize)
		if err := shared.Save(context.Background(), seed.controlKeyHandle(), backend.NewByteReader(tampered, shared.Hasher())); err != nil {
			t.Fatal(err)
		}
		restarted := newTestReadCacheManager(t, shared, shared, "manager-domain-tampered", 1024)
		if restarted.admissions {
			t.Fatal("tampered domain control must disable admissions")
		}
	})

	t.Run("create-response-loss", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		shared.loseCASResponseOnce.Store(true)
		manager := newTestReadCacheManager(t, shared, shared, "manager-domain-response-loss", 1024)
		if !manager.admissions || len(manager.controlKey) != readCacheControlKeySize {
			t.Fatal("domain control create response loss should recover by bounded readback")
		}
	})
}

func TestReadCacheCacheAuthorityCannotForgePlaintextPolicy(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	repositoryKey := bytes.Repeat([]byte{0x41}, 32)
	seed := newTestReadCacheManagerForRepo(t, shared, shared, "authentic-repo", repositoryKey, "seed", 1024)

	attackerKey := bytes.Repeat([]byte{0xEE}, readCacheControlKeySize)
	forged := seed.desiredPolicy()
	forged.Tiers[0].Trust = readCacheTrustPlaintext
	forged.Tiers[0].TrustAck = true
	forged.ControlKeySHA256 = readCacheKeyDigest(attackerKey)
	forged.Signature = ""
	rawUnsigned, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, attackerKey)
	_, _ = mac.Write(rawUnsigned)
	forged.Signature = hex.EncodeToString(mac.Sum(nil))
	forgedRaw, _ := json.Marshal(forged)
	for _, replacement := range []struct {
		handle backend.Handle
		body   []byte
	}{
		{seed.controlKeyHandle(), attackerKey},
		{seed.policyHandle(), forgedRaw},
		{seed.quotaHandle(), []byte(
			`{"format":1,"namespace":"read-cache/v1","revision":1,"managers":[],"entries":[],"updated_unix_ms":1,"signature":"forged"}`,
		)},
	} {
		_ = shared.Remove(context.Background(), replacement.handle)
		if err := shared.Save(context.Background(), replacement.handle, backend.NewByteReader(replacement.body, shared.Hasher())); err != nil {
			t.Fatal(err)
		}
	}

	restarted := newTestReadCacheManagerForRepo(t, shared, shared, seed.repoID, repositoryKey, "restart", 1024)
	if restarted.admissions || restarted.tiers[0].trust == readCacheTrustPlaintext {
		t.Fatal("cache authority forged plaintext authorization")
	}
}

func TestReadCacheRepositoriesKeepDistinctPoliciesWithSharedQuota(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	left := newTestReadCacheManagerForRepo(t, shared, shared, "policy-left", bytes.Repeat([]byte{0x51}, 32), "left", 2048)
	right := newTestReadCacheManagerForRepo(t, shared, shared, "policy-right", bytes.Repeat([]byte{0x52}, 32), "right", 2048)
	plaintext, ack := readCacheTrustPlaintext, true
	if err := left.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Trust: &plaintext, TrustAck: &ack}); err != nil {
		t.Fatal(err)
	}
	_, leftPolicy, leftErr := left.loadPolicy(context.Background())
	_, rightPolicy, rightErr := right.loadPolicy(context.Background())
	if leftErr != nil || rightErr != nil {
		t.Fatalf("load scoped policies: left=%v right=%v", leftErr, rightErr)
	}
	if leftPolicy.RepositoryID == rightPolicy.RepositoryID ||
		leftPolicy.Tiers[0].Trust != readCacheTrustPlaintext || rightPolicy.Tiers[0].Trust != readCacheTrustEncrypted {
		t.Fatalf("shared cache policies were not repository-scoped: left=%#v right=%#v", leftPolicy, rightPolicy)
	}
}

func TestReadCacheAccessRefreshesCrossProcessTrustTightening(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	repositoryKey := bytes.Repeat([]byte{0x53}, 32)
	left := newTestReadCacheManagerForRepo(t, shared, shared, "trust-refresh", repositoryKey, "left", 2048)
	plaintext, ack := readCacheTrustPlaintext, true
	if err := left.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Trust: &plaintext, TrustAck: &ack}); err != nil {
		t.Fatal(err)
	}
	right := newTestReadCacheManagerForRepo(t, shared, shared, "trust-refresh", repositoryKey, "right", 2048)
	if right.tiers[0].trust != readCacheTrustPlaintext {
		t.Fatalf("second manager did not load plaintext policy: %q", right.tiers[0].trust)
	}
	encrypted, noAck := readCacheTrustEncrypted, false
	if err := left.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Trust: &encrypted, TrustAck: &noAck}); err != nil {
		t.Fatal(err)
	}
	if !right.refreshPolicyForAccess(context.Background()) {
		t.Fatal("access policy refresh failed")
	}
	if right.tiers[0].trust != readCacheTrustEncrypted {
		t.Fatalf("stale manager retained plaintext trust: %q", right.tiers[0].trust)
	}
}

func TestReadCacheCoordinatorRejectsChunkSizeBeforeSignedPersistence(t *testing.T) {
	manager := newTestReadCacheManager(t, newReadCacheFaultBackend(mem.New()), mem.New(), "manager-chunk-bound", 4096)
	for _, chunkBytes := range []uint64{MaxReadCacheChunkBytes + 1, math.MaxUint64} {
		err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", ChunkBytes: &chunkBytes})
		if err == nil || !strings.Contains(err.Error(), "chunk-bytes") {
			t.Fatalf("chunk-bytes %d error = %v", chunkBytes, err)
		}
	}
	if manager.tiers[0].chunkSize != 128 {
		t.Fatalf("rejected update changed live chunk size to %d", manager.tiers[0].chunkSize)
	}
}

func TestReadCachePolicyAgeAndPriorityUpdatePersists(t *testing.T) {
	manager := newTestReadCacheManager(t, newReadCacheFaultBackend(mem.New()), mem.New(), "policy-operational", 4096)
	initialGeneration := manager.tiers[0].generation
	idle := 5 * time.Minute
	absolute := 2 * time.Hour
	readPriority := uint32(7)
	admissionPriority := uint32(3)
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{
		ID: "rc", IdleAge: &idle, AbsoluteAge: &absolute,
		ReadPriority: &readPriority, AdmissionPriority: &admissionPriority,
	}); err != nil {
		t.Fatal(err)
	}
	_, policy, err := manager.loadPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tier := policy.Tiers[0]
	if tier.IdleAgeMS != uint64(idle/time.Millisecond) || tier.AbsoluteAgeMS != uint64(absolute/time.Millisecond) ||
		tier.ReadPriority != readPriority || tier.AdmissionPriority != admissionPriority {
		t.Fatalf("operational policy update did not persist: %#v", tier)
	}
	if tier.Generation != initialGeneration {
		t.Fatalf("operational policy update changed representation generation from %d to %d", initialGeneration, tier.Generation)
	}
	codec := "zstd"
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Codec: &codec}); err != nil {
		t.Fatal(err)
	}
	_, policy, err = manager.loadPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if policy.Tiers[0].Generation != initialGeneration+1 {
		t.Fatalf("codec update generation = %d, want %d", policy.Tiers[0].Generation, initialGeneration+1)
	}
}

func TestReadCacheEffectiveBudgetSyncPreservesAllTierTargets(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, shared, "budget-multi-tier", 4096)
	second := &readCacheTier{
		manager: manager, id: "rc-second", trust: readCacheTrustEncrypted, codec: "raw",
		enabled: true, ingest: true, readPrio: readCacheDefaultPrio, admitPrio: readCacheDefaultPrio,
		maxBytes: 4096, requested: 4096, chunkSize: 128, backend: shared,
		entries: map[string]*readCacheEntry{}, deleting: map[string]*readCacheEntry{},
		probeFailures: map[string]uint32{}, negativeUntil: map[string]time.Time{},
	}
	manager.tiers = append(manager.tiers, second)

	policy := manager.desiredPolicy()
	policy.Revision = manager.currentPolicyRevision() + 1
	policy = manager.signPolicy(policy)
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := shared.Remove(context.Background(), manager.policyHandle()); err != nil {
		t.Fatal(err)
	}
	if err := shared.Save(context.Background(), manager.policyHandle(), backend.NewByteReader(raw, shared.Hasher())); err != nil {
		t.Fatal(err)
	}
	manager.applyPolicy(&policy)

	targets := []uint64{700, 300}
	for index, tier := range manager.tiers {
		tier.mu.Lock()
		tier.maxBytes = targets[index]
		tier.mu.Unlock()
	}
	manager.mu.Lock()
	manager.aggregateMax = 1000
	manager.mu.Unlock()
	manager.syncEffectiveBudgetPolicy(context.Background())

	_, persisted, err := manager.loadPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Tiers[0].MaxBytes != targets[0] || persisted.Tiers[1].MaxBytes != targets[1] {
		t.Fatalf("multi-tier targets were not preserved: %#v", persisted.Tiers)
	}
}

func TestReadCacheEffectiveBudgetPublishesZero(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, shared, "budget-zero", 4096)
	manager.applyEffectiveBudget(0)
	manager.syncEffectiveBudgetPolicy(context.Background())

	_, persisted, err := manager.loadPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted.AggregateMaxBytes != 0 || persisted.Tiers[0].MaxBytes != 0 {
		t.Fatalf("zero fallback was not published: %#v", persisted)
	}
}

func TestReadCacheOnlineResizeSurvivesRefreshAndReenablesAfterZero(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "budget-online-resize", 4096)
	manager.stopCapacityWorker()
	manager.stopPolicyWorker()

	for _, limit := range []uint64{2048, 0, 1024} {
		if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", MaxBytes: &limit}); err != nil {
			t.Fatal(err)
		}
		manager.refreshCapacityBudget(context.Background())
		status := (&Repository{readCache: manager}).ReadCacheStatus()
		if status.RequestedMaxBytes != limit || status.AggregateMaxBytes != limit || status.Tiers[0].MaxBytes != limit {
			t.Fatalf("limit %d status = requested %d aggregate %d tier %d", limit, status.RequestedMaxBytes, status.AggregateMaxBytes, status.Tiers[0].MaxBytes)
		}
		if status.AdmissionsEnabled != (limit > 0) {
			t.Fatalf("limit %d admissions = %v", limit, status.AdmissionsEnabled)
		}
		restarted := newTestReadCacheManager(t, shared, cache, "budget-online-resize-restart", 4096)
		restarted.stopCapacityWorker()
		restarted.stopPolicyWorker()
		restarted.refreshCapacityBudget(context.Background())
		restartedStatus := (&Repository{readCache: restarted}).ReadCacheStatus()
		if restartedStatus.RequestedMaxBytes != limit || restartedStatus.AggregateMaxBytes != limit {
			t.Fatalf("limit %d restarted status = requested %d aggregate %d", limit, restartedStatus.RequestedMaxBytes, restartedStatus.AggregateMaxBytes)
		}
	}
}

func TestReadCacheCoordinatorRejectsOversizedChunkInSignedPolicy(t *testing.T) {
	manager := newTestReadCacheManager(t, newReadCacheFaultBackend(mem.New()), mem.New(), "manager-signed-chunk-bound", 4096)
	policy := manager.desiredPolicy()
	policy.Revision = manager.currentPolicyRevision() + 1
	policy.Tiers[0].ChunkSize = int(MaxReadCacheChunkBytes + 1)
	policy = manager.signPolicy(policy)
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.parsePolicy(raw); err == nil || !strings.Contains(err.Error(), "practical maximum") {
		t.Fatalf("oversized signed policy error = %v", err)
	}
}

func TestReadCacheCoordinatorRestartPreservesOnlinePolicy(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-policy-seed", 4096)
	disabled := false
	maxBytes := uint64(3072)
	chunkBytes := uint64(256)
	trust := readCacheTrustPlaintext
	ack := true
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{
		ID: manager.tiers[0].id, Enabled: &disabled, MaxBytes: &maxBytes,
		ChunkBytes: &chunkBytes, Trust: &trust, TrustAck: &ack,
	}); err != nil {
		t.Fatal(err)
	}
	wantRevision := manager.currentPolicyRevision()

	restarted := newTestReadCacheManager(t, shared, cache, "manager-policy-restart", 4096)
	tier := restarted.tiers[0]
	tier.mu.Lock()
	gotEnabled, gotMax, gotChunk := tier.enabled, tier.maxBytes, tier.chunkSize
	gotTrust, gotAck := tier.trust, tier.trustAck
	tier.mu.Unlock()
	if gotEnabled || gotMax != maxBytes || gotChunk != int(chunkBytes) || gotTrust != trust || !gotAck {
		t.Fatalf("restarted policy = enabled=%v max=%d chunk=%d trust=%q ack=%v", gotEnabled, gotMax, gotChunk, gotTrust, gotAck)
	}
	if gotRevision := restarted.currentPolicyRevision(); gotRevision != wantRevision {
		t.Fatalf("restarted revision = %d, want %d", gotRevision, wantRevision)
	}
}

func TestReadCacheCoordinatorSharedAuthorityWithReducedBudget(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	left := newTestReadCacheManager(t, shared, cache, "manager-left-reduced", 5000)
	right := newTestReadCacheManager(t, shared, cache, "manager-right-reduced", 5000)
	left.applyEffectiveBudget(3500)
	right.applyEffectiveBudget(3500)

	chunk := bytes.Repeat([]byte("y"), 1100)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		left.storeRange(context.Background(), left.tiers[0], vaultic.ID{5}, 0, chunk)
	}()
	go func() {
		defer wg.Done()
		right.storeRange(context.Background(), right.tiers[0], vaultic.ID{6}, 0, chunk)
	}()
	wg.Wait()

	_, ledger, err := left.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _, used, reserved := quotaUsage(ledger)
	if used+reserved > 3500 {
		t.Fatalf("shared coordinator exceeded reduced effective budget: used=%d reserved=%d max=%d", used, reserved, 3500)
	}
}

func TestReadCacheCoordinatorPolicyShrinkBeforeQuotaCASPreventsVisibleOverLimitEntry(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-policy-shrink-pre-cas", 4096)
	tier := manager.tiers[0]
	chunk := bytes.Repeat([]byte("p"), 128)
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{31}, 0, len(chunk))
	key := tier.entryKey(identity)

	hookCalls := atomic.Uint64{}
	manager.mu.Lock()
	manager.testBeforeQuotaCAS = func() {
		hookCalls.Add(1)
		revision := manager.currentPolicyRevision()
		shrink := uint64(1024)
		if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: tier.id, MaxBytes: &shrink, ExpectedRev: &revision}); err != nil {
			t.Fatalf("policy shrink before quota CAS failed: %v", err)
		}
	}
	manager.mu.Unlock()

	manager.storeRange(context.Background(), tier, vaultic.ID{31}, 0, chunk)

	if hookCalls.Load() != 1 {
		t.Fatalf("expected exactly one pre-CAS shrink hook call, got %d", hookCalls.Load())
	}
	if entry := tier.lookupEntry(key); entry != nil {
		t.Fatalf("policy shrink before quota CAS must not publish over-limit entry: %#v", entry)
	}
	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range ledger.Entries {
		if entry.Key == key && quotaEntryIsPublished(entry.State) {
			t.Fatalf("policy shrink before quota CAS left active over-limit entry: %#v", entry)
		}
	}
}

func TestReadCacheCoordinatorPolicyShrinkAfterQuotaCASBeforePublishTransitionsToDeleting(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-policy-shrink-post-cas", 4096)
	tier := manager.tiers[0]
	chunk := bytes.Repeat([]byte("q"), 128)
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{32}, 0, len(chunk))
	key := tier.entryKey(identity)

	hookCalls := atomic.Uint64{}
	manager.mu.Lock()
	manager.testAfterCommitCAS = func() {
		hookCalls.Add(1)
		revision := manager.currentPolicyRevision()
		shrink := uint64(1024)
		if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: tier.id, MaxBytes: &shrink, ExpectedRev: &revision}); err != nil {
			t.Fatalf("policy shrink after quota CAS failed: %v", err)
		}
	}
	manager.mu.Unlock()

	manager.storeRange(context.Background(), tier, vaultic.ID{32}, 0, chunk)

	if hookCalls.Load() != 1 {
		t.Fatalf("expected exactly one post-CAS shrink hook call, got %d", hookCalls.Load())
	}
	if entry := tier.lookupEntry(key); entry != nil {
		t.Fatalf("policy shrink after quota CAS must not publish over-limit entry: %#v", entry)
	}
	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range ledger.Entries {
		if entry.Key == key && quotaEntryIsPublished(entry.State) {
			t.Fatalf("policy shrink after quota CAS left active over-limit entry: %#v", entry)
		}
	}
}

func TestReadCacheCoordinatorAdmissionChecksHandleMaxUint64Boundaries(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-max-boundary", math.MaxUint64)
	tier := manager.tiers[0]

	err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:    "preloaded-boundary",
			Key:        "preloaded-boundary",
			TierID:     tier.id,
			Generation: 1,
			Bytes:      math.MaxUint64 - 5,
			State:      readCacheStateActive,
			ManagerID:  manager.managerID,
			UpdatedMS:  now.UnixMilli(),
		})
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if lease, ok := manager.reserveAdmission(context.Background(), tier, "overflow-admission", 10); ok || lease != nil {
		t.Fatal("expected reservation rejection when aggregate+tier usage is at uint64 overflow boundary")
	}
}

func TestReadCacheCoordinatorCapacityUsesSharedQuotaFootprintAndReservations(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-capacity-shared-raw", 200)
	manager.capacityController = newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 1,
		GrowthRateLogicalBytes: 1000,
	}, &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:    100,
		FreeRawBytes:     20,
		RawAmplification: 1,
		Health:           readCacheHealthHealthy,
		Timestamp:        time.Now().UTC(),
	}})
	manager.mu.Lock()
	manager.inflightBytes = 5
	manager.mu.Unlock()

	err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries,
			readCacheQuotaEntry{
				EntryID: "active#1", Key: "active", TierID: "rc", Generation: 1, Bytes: 30,
				State: readCacheStateActive, ManagerID: manager.managerID, UpdatedMS: now.UnixMilli(),
			},
			readCacheQuotaEntry{
				EntryID: "admitting#2", Key: "admitting", TierID: "rc", Generation: 2, Bytes: 20,
				State: readCacheStateAdmitting, ManagerID: manager.managerID, UpdatedMS: now.UnixMilli(),
			},
			readCacheQuotaEntry{
				EntryID: "deleting#3", Key: "deleting", TierID: "rc", Generation: 3, Bytes: 10,
				State: readCacheStateDeleting, ManagerID: manager.managerID, UpdatedMS: now.UnixMilli(),
			},
		)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	manager.refreshCapacityBudget(context.Background())
	decision := manager.capacityDecisionSnapshot()
	if decision.EffectiveLogicalBytes != 65 {
		t.Fatalf("expected logical target derived from shared quota raw (60) + free-reserve (10) - reservations (5): got %d", decision.EffectiveLogicalBytes)
	}
}

func TestReadCacheExpiredPersistedAdmissionRemainsChargedForDeletion(t *testing.T) {
	manager := newTestReadCacheManager(t, newReadCacheFaultBackend(mem.New()), mem.New(), "manager-expired-admission", 200)
	now := time.Now().UTC()
	ledger := &readCacheQuotaLedger{
		Managers: []readCacheManagerRef{{ID: "expired-manager", LeaseExpiryMS: now.Add(-time.Second).UnixMilli()}},
		Entries: []readCacheQuotaEntry{{
			EntryID: "persisted#1", RepositoryID: manager.repoID, Key: "persisted", TierID: "rc",
			DataHandle: "data", MetaHandle: "meta", Bytes: 40, State: readCacheStateAdmitting,
			ManagerID: "expired-manager", UpdatedMS: now.Add(-time.Minute).UnixMilli(),
		}},
	}

	manager.pruneExpiredLeases(ledger, now)
	if len(ledger.Entries) != 1 || ledger.Entries[0].State != readCacheStateDeleting {
		t.Fatalf("expired persisted admission = %+v, want one deleting entry", ledger.Entries)
	}
	usedByTier, reservedByTier, aggregateUsed, aggregateReserved := quotaUsage(ledger)
	if usedByTier["rc"] != 40 || reservedByTier["rc"] != 0 || aggregateUsed != 40 || aggregateReserved != 0 {
		t.Fatalf("expired persisted admission usage = used %v reserved %v aggregate=(%d,%d)", usedByTier, reservedByTier, aggregateUsed, aggregateReserved)
	}
}

func TestReadCacheCoordinatorCapacityRawInputsUseObservedOverage(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-capacity-overage", 50)

	manager.tiers[0].mu.Lock()
	manager.tiers[0].used = 80
	manager.tiers[0].reserved = 20
	manager.tiers[0].mu.Unlock()
	manager.mu.Lock()
	manager.inflightBytes = 7
	manager.mu.Unlock()

	cacheRaw, reservedRaw := manager.capacityRawInputs(context.Background())
	if cacheRaw != 100 || reservedRaw != 7 {
		t.Fatalf("expected local observed usage preserved over configured max: cache=%d reserved=%d", cacheRaw, reservedRaw)
	}

	err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries,
			readCacheQuotaEntry{
				EntryID: "active-over#1", Key: "active-over", TierID: "rc", Generation: 1,
				CommitOrder: 1, Bytes: 130, State: readCacheStateActive, ManagerID: manager.managerID, UpdatedMS: now.UnixMilli(),
			},
			readCacheQuotaEntry{
				EntryID: "admitting-over#2", Key: "admitting-over", TierID: "rc", Generation: 2,
				CommitOrder: 2, Bytes: 40, State: readCacheStateAdmitting, ManagerID: manager.managerID, UpdatedMS: now.UnixMilli(),
			},
		)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	cacheRaw, reservedRaw = manager.capacityRawInputs(context.Background())
	if cacheRaw != 170 || reservedRaw != 7 {
		t.Fatalf("expected shared accounted overage preserved over configured max: cache=%d reserved=%d", cacheRaw, reservedRaw)
	}
}

func TestReadCacheControlPathsSurviveStartupReconcileDrainAndClear(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	controlHandles := []backend.Handle{
		{Type: backend.StagingFile, Name: readCacheNamespace + "/control/policy/keep.json"},
		{Type: backend.StagingFile, Name: readCacheNamespace + "/control/quota/keep.json"},
		{Type: backend.StagingFile, Name: readCacheNamespace + "/control/lease/keep.json"},
	}
	for _, handle := range controlHandles {
		if err := shared.Save(context.Background(), handle, backend.NewByteReader([]byte("keep"), shared.Hasher())); err != nil {
			t.Fatal(err)
		}
	}

	manager := newTestReadCacheManager(t, shared, shared, "manager-control-survival", 1024)
	assertExists := func(stage string) {
		t.Helper()
		for _, handle := range controlHandles {
			if _, err := shared.Stat(context.Background(), handle); err != nil {
				t.Fatalf("expected control file to survive %s: %s: %v", stage, handle.Name, err)
			}
		}
	}

	assertExists("startup")
	manager.tiers[0].reconcile(context.Background(), manager)
	assertExists("reconcile")
	if err := manager.tiers[0].drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertExists("drain")

	repo := &Repository{readCache: manager}
	if err := repo.ClearReadCache(context.Background(), "rc"); err != nil {
		t.Fatal(err)
	}
	assertExists("clear")
}

func TestReadCacheRestartWithUncommittedPhysicalEntryAndQuotaOutageFailsClosed(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	seed := newTestReadCacheManager(t, shared, cache, "manager-crash-seed", 4096)
	seedTier := seed.tiers[0]
	identity := seed.sourceRangeIdentity(seedTier, vaultic.ID{91}, 0, 128)
	key := seedTier.entryKey(identity)
	generation := uint64(11)
	commitOrder := uint64(22)
	payload := bytes.Repeat([]byte("q"), 128)
	digest := sha256.Sum256(payload)
	dataHandle, metaHandle := seedTier.handlesForIdentity(key, identity, generation, commitOrder)
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: seed.repoID,
		TierID:       seedTier.id,
		Generation:   generation,
		CommitOrder:  commitOrder,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(payload),
		ChunkSize:    seedTier.chunkSize,
		Trust:        seedTier.trust,
		DigestSHA256: hex.EncodeToString(digest[:]),
	}
	meta.Signature = seed.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), dataHandle, backend.NewByteReader(payload, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), metaHandle, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := seed.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, _ time.Time) (bool, error) {
		changed := false
		for i := 0; i < len(ledger.Entries); i++ {
			if ledger.Entries[i].Key != key {
				continue
			}
			ledger.Entries = append(ledger.Entries[:i], ledger.Entries[i+1:]...)
			i--
			changed = true
		}
		return changed, nil
	}); err != nil {
		t.Fatal(err)
	}

	shared.failControlLoad.Store(true)
	restarted := newTestReadCacheManager(t, shared, cache, "manager-crash-restart", 4096)
	shared.failControlLoad.Store(false)
	restartedTier := restarted.tiers[0]

	if entry := restartedTier.lookupEntry(key); entry != nil {
		t.Fatalf("quota outage restart must not publish crash-window artifact: %#v", entry)
	}
	restartedTier.mu.Lock()
	quarantined, exists := restartedTier.entries[key]
	restartedTier.mu.Unlock()
	if !exists || quarantined == nil || quarantined.Published {
		t.Fatalf("expected uncommitted physical entry to remain quarantined: exists=%v entry=%#v", exists, quarantined)
	}
	if chunk, ok := restarted.loadRepresentationFromTier(context.Background(), restartedTier, identity); ok || len(chunk) != 0 {
		t.Fatal("quota outage restart must fail closed for quarantined physical entry")
	}
}

func TestReadCacheCoordinatorPolicyFailsClosed(t *testing.T) {
	t.Run("seeded-raw-control-update", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		cache := mem.New()
		manager := newTestReadCacheManager(t, shared, cache, "manager-seeded-raw-control", 2048)

		seededPolicy := manager.desiredPolicy()
		seededPolicy.Revision = manager.currentPolicyRevision() + 7
		seededPolicy.UpdatedUnixMS = time.Now().UTC().UnixMilli()
		seededPolicy = manager.signPolicy(seededPolicy)
		seededPolicyRaw, err := json.Marshal(seededPolicy)
		if err != nil {
			t.Fatal(err)
		}
		_ = shared.Remove(context.Background(), manager.policyHandle())
		if err := shared.Save(context.Background(), manager.policyHandle(), backend.NewByteReader(seededPolicyRaw, shared.Hasher())); err != nil {
			t.Fatal(err)
		}

		seededQuota := manager.signQuota(readCacheQuotaLedger{
			Format:    readCacheQuotaFormatV1,
			Namespace: readCacheNamespace,
			Revision:  seededPolicy.Revision,
			Managers:  []readCacheManagerRef{{ID: manager.managerID, LeaseExpiryMS: time.Now().Add(time.Minute).UnixMilli()}},
			Entries:   []readCacheQuotaEntry{},
		})
		seededQuotaRaw, err := json.Marshal(seededQuota)
		if err != nil {
			t.Fatal(err)
		}
		_ = shared.Remove(context.Background(), manager.quotaHandle())
		if err := shared.Save(context.Background(), manager.quotaHandle(), backend.NewByteReader(seededQuotaRaw, shared.Hasher())); err != nil {
			t.Fatal(err)
		}

		limit := uint64(1536)
		expectedRev := seededPolicy.Revision
		update := readCachePolicyUpdate{ID: manager.tiers[0].id, MaxBytes: &limit, ExpectedRev: &expectedRev}
		if err := manager.updatePolicy(context.Background(), update); err != nil {
			t.Fatalf("expected policy CAS update over seeded raw control object: %v", err)
		}
		if lease, ok := manager.reserveAdmission(context.Background(), manager.tiers[0], "seeded-raw-key", 128); !ok || lease == nil {
			t.Fatal("expected quota CAS mutation over seeded raw control object")
		}
	})

	t.Run("missing", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		cache := mem.New()
		manager := newTestReadCacheManager(t, shared, cache, "manager-missing", 1024)
		if err := shared.Remove(context.Background(), manager.policyHandle()); err != nil {
			t.Fatal(err)
		}
		if lease, ok := manager.reserveAdmission(context.Background(), manager.tiers[0], "k1", 128); ok || lease != nil {
			t.Fatal("expected reservation to fail when policy is missing")
		}
		manager.mu.Lock()
		admissions := manager.admissions
		manager.mu.Unlock()
		if admissions {
			t.Fatal("admissions should be disabled after missing policy")
		}
	})

	t.Run("malformed", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		cache := mem.New()
		manager := newTestReadCacheManager(t, shared, cache, "manager-malformed", 1024)
		_ = shared.Remove(context.Background(), manager.policyHandle())
		if err := shared.Save(context.Background(), manager.policyHandle(), backend.NewByteReader([]byte("{not-json"), shared.Hasher())); err != nil {
			t.Fatal(err)
		}
		if lease, ok := manager.reserveAdmission(context.Background(), manager.tiers[0], "k2", 128); ok || lease != nil {
			t.Fatal("expected reservation to fail when policy is malformed")
		}
	})

	t.Run("equivocated", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		cache := mem.New()
		manager := newTestReadCacheManager(t, shared, cache, "manager-equivocated", 1024)
		base := manager.desiredPolicy()
		base.Revision = 4
		base.UpdatedUnixMS = 1
		left := manager.signPolicy(base)
		right := manager.signPolicy(readCachePolicySnapshot{
			Format:            base.Format,
			Namespace:         base.Namespace,
			RepositoryID:      base.RepositoryID,
			ControlKeySHA256:  base.ControlKeySHA256,
			Revision:          base.Revision,
			AggregateMaxBytes: base.AggregateMaxBytes + 1,
			Tiers:             base.Tiers,
			UpdatedUnixMS:     2,
		})
		leftRaw, _ := json.Marshal(left)
		rightRaw, _ := json.Marshal(right)
		shared.policyBodies = [][]byte{leftRaw, rightRaw}
		if lease, ok := manager.reserveAdmission(context.Background(), manager.tiers[0], "k3", 128); ok || lease != nil {
			t.Fatal("expected reservation to fail when policy equivocation is observed")
		}
	})
}

func TestReadCacheCoordinatorQuotaEquivocationRejected(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-quota-equivocated", 1024)

	base := readCacheQuotaLedger{
		Format:    readCacheQuotaFormatV1,
		Namespace: readCacheNamespace,
		Revision:  9,
		Managers:  []readCacheManagerRef{{ID: manager.managerID, LeaseExpiryMS: time.Now().Add(time.Minute).UnixMilli()}},
		Entries: []readCacheQuotaEntry{{
			EntryID:    "a#1",
			Key:        "a",
			TierID:     "rc",
			Generation: 1,
			Bytes:      64,
			State:      readCacheStateActive,
			ManagerID:  manager.managerID,
			UpdatedMS:  1,
		}},
		UpdatedUnix: 1,
	}
	left := manager.signQuota(base)
	right := manager.signQuota(readCacheQuotaLedger{
		Format:    base.Format,
		Namespace: base.Namespace,
		Revision:  base.Revision,
		Managers:  base.Managers,
		Entries: []readCacheQuotaEntry{{
			EntryID:    "b#1",
			Key:        "b",
			TierID:     "rc",
			Generation: 1,
			Bytes:      64,
			State:      readCacheStateActive,
			ManagerID:  manager.managerID,
			UpdatedMS:  2,
		}},
		UpdatedUnix: 2,
	})
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	shared.quotaBodies = [][]byte{leftRaw, rightRaw}

	if _, _, err := manager.loadQuota(context.Background()); err == nil || !strings.Contains(err.Error(), "equivocated read-cache quota revision") {
		t.Fatalf("expected quota equivocation rejection, got %v", err)
	}
}

func TestReadCacheCoordinatorQuotaRollbackRejectedAfterHigherObservedRevision(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	left := newTestReadCacheManager(t, shared, cache, "manager-floor-left", 4096)
	right := newTestReadCacheManager(t, shared, cache, "manager-floor-right", 4096)

	left.storeRange(context.Background(), left.tiers[0], vaultic.ID{1}, 0, bytes.Repeat([]byte("a"), 64))
	staleRaw, _, err := right.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	left.storeRange(context.Background(), left.tiers[0], vaultic.ID{2}, 0, bytes.Repeat([]byte("b"), 64))
	if _, _, err := right.loadQuota(context.Background()); err != nil {
		t.Fatal(err)
	}

	currentRaw, loadErr := loadAll(shared, right.quotaHandle())
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if _, swapped, err := shared.CompareAndSwap(context.Background(), right.quotaHandle(), currentRaw, staleRaw); err != nil || !swapped {
		t.Fatalf("failed to force quota rollback state: swapped=%v err=%v", swapped, err)
	}

	if _, _, err := right.loadQuota(context.Background()); err == nil || !strings.Contains(err.Error(), "quota revision rollback") {
		t.Fatalf("expected rollback detection after floor advance, got %v", err)
	}

	if lease, ok := right.reserveAdmission(context.Background(), right.tiers[0], "post-rollback", 32); ok || lease != nil {
		t.Fatal("expected admission reservation failure after rollback detection")
	}
}

func TestReadCacheCoordinatorCommittedCASResponseLossReadback(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-cas-readback", 2048)
	shared.loseCASResponseOnce.Store(true)
	disable := false
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Enabled: &disable}); err != nil {
		t.Fatalf("update policy should recover from post-commit response loss: %v", err)
	}
	status := readCacheStatus{AdmissionsEnabled: true}
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		if !tier.enabled {
			status.AdmissionsEnabled = false
		}
		tier.mu.Unlock()
	}
	if status.AdmissionsEnabled {
		t.Fatal("expected tier policy to reflect committed update")
	}
}

func TestReadCacheCoordinatorPolicyUpdateExpectedRevision(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-expected-rev", 2048)
	raw, policy, err := manager.loadPolicy(context.Background())
	if err != nil || len(raw) == 0 || policy == nil {
		t.Fatalf("expected policy bootstrap: raw=%d policy=%v err=%v", len(raw), policy != nil, err)
	}
	expected := policy.Revision + 1
	disable := false
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Enabled: &disable, ExpectedRev: &expected}); err == nil {
		t.Fatal("expected update to fail on revision mismatch")
	}
	current := policy.Revision
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Enabled: &disable, ExpectedRev: &current}); err != nil {
		t.Fatalf("expected update with matching revision: %v", err)
	}
}

func TestReadCacheCoordinatorPolicyUpdateTrustOrderingRequiresAck(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-trust-ordering", 2048)

	trust := readCacheTrustPlaintext
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Trust: &trust}); err == nil {
		t.Fatal("expected plaintext trust update without acknowledgement to fail")
	}

	ack := true
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: "rc", Trust: &trust, TrustAck: &ack}); err != nil {
		t.Fatalf("expected atomic trust+ack policy update to succeed: %v", err)
	}

	_, policy, err := manager.loadPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if policy == nil || len(policy.Tiers) != 1 {
		t.Fatalf("unexpected policy snapshot: %#v", policy)
	}
	if policy.Tiers[0].Trust != readCacheTrustPlaintext || !policy.Tiers[0].TrustAck {
		t.Fatalf("expected plaintext trust with ack after update, got %#v", policy.Tiers[0])
	}
}

func TestReadCacheCoordinatorConcurrentPolicyUpdatesAndFills(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-concurrent-policy-fills", 4096)
	tier := manager.tiers[0]

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	chunk := bytes.Repeat([]byte("p"), 192)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 64; i++ {
			manager.storeRange(ctx, tier, vaultic.ID{byte(i + 1)}, 0, chunk)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 64; i++ {
			maxBytes := uint64(1024 + (i%8)*256)
			enabled := i%5 != 0
			if err := manager.updatePolicy(ctx, readCachePolicyUpdate{ID: tier.id, MaxBytes: &maxBytes, Enabled: &enabled}); err != nil {
				if ctx.Err() != nil {
					return
				}
				// CAS contention is expected under concurrent updates/fills.
				continue
			}
		}
	}()

	wg.Wait()

	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _, used, reserved := quotaUsage(ledger)
	if used+reserved > manager.aggregateMax {
		t.Fatalf("concurrent updates exceeded aggregate max: used=%d reserved=%d max=%d", used, reserved, manager.aggregateMax)
	}
}

func TestReadCacheCoordinatorLeaseExpiryRecovery(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	left := newTestReadCacheManager(t, shared, cache, "manager-expire-left", 1024)
	right := newTestReadCacheManager(t, shared, cache, "manager-expire-right", 1024)

	lease, ok := left.reserveAdmission(context.Background(), left.tiers[0], "lease-key", 256)
	if !ok || lease == nil {
		t.Fatal("expected initial reservation")
	}

	raw, ledger, err := left.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range ledger.Managers {
		if ledger.Managers[i].ID == left.managerID {
			ledger.Managers[i].LeaseExpiryMS = time.Now().Add(-time.Minute).UnixMilli()
		}
	}
	ledger = &[]readCacheQuotaLedger{left.signQuota(*ledger)}[0]
	replacement, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if _, swapped, err := shared.CompareAndSwap(context.Background(), left.quotaHandle(), raw, replacement); err != nil || !swapped {
		t.Fatalf("failed to persist forced expiry: swapped=%v err=%v", swapped, err)
	}

	lease2, ok2 := right.reserveAdmission(context.Background(), right.tiers[0], "lease-key", 256)
	if !ok2 || lease2 == nil {
		t.Fatal("expected reservation to recover after stale lease expiry")
	}
}

func TestReadCacheCoordinatorInventoryMutationAbort(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	shared.alwaysCASQuota.Store(true)
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-inventory-abort", 1024)
	if lease, ok := manager.reserveAdmission(context.Background(), manager.tiers[0], "inventory-race", 128); ok || lease != nil {
		t.Fatal("expected reservation to fail during repeated quota CAS races")
	}
	manager.mu.Lock()
	admissions := manager.admissions
	manager.mu.Unlock()
	if admissions {
		t.Fatal("expected admissions disabled when inventory CAS keeps racing")
	}
}

func TestReadCacheCoordinatorReserveCASConflictThenNoOpDoesNotReportSuccess(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	left := newTestReadCacheManager(t, shared, cache, "manager-reserve-conflict-left", 2048)
	right := newTestReadCacheManager(t, shared, cache, "manager-reserve-conflict-right", 2048)

	conflictKey := "reserve-conflict-key"
	hookCalls := atomic.Uint64{}
	left.mu.Lock()
	left.testBeforeQuotaCAS = func() {
		hookCalls.Add(1)
		lease, ok := right.reserveAdmission(context.Background(), right.tiers[0], conflictKey, 256)
		if !ok || lease == nil {
			t.Fatal("expected competing reservation to win during CAS conflict hook")
		}
	}
	left.mu.Unlock()

	lease, ok := left.reserveAdmission(context.Background(), left.tiers[0], conflictKey, 256)
	if ok || lease != nil {
		t.Fatal("reservation must not report success after first CAS loss then no-op conflict")
	}
	if hookCalls.Load() != 1 {
		t.Fatalf("expected one CAS conflict hook call, got %d", hookCalls.Load())
	}
}

func TestReadCacheCoordinatorReserveCASResponseLossReadbackReturnsCommittedLease(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-reserve-response-loss", 2048)
	shared.loseCASResponseOnce.Store(true)

	lease, ok := manager.reserveAdmission(context.Background(), manager.tiers[0], "response-loss-key", 256)
	if !ok || lease == nil {
		t.Fatal("expected reservation success after CAS response-loss readback")
	}
	if lease.EntryID == "" || lease.CommitOrder == 0 || lease.PolicyRev == 0 {
		t.Fatalf("expected committed lease fields after readback: %#v", lease)
	}

	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, entry := range ledger.Entries {
		if matchesAdmissionLease(entry, lease) && entry.State == readCacheStateAdmitting {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected reservation row persisted after response-loss readback")
	}
}

func TestReadCacheCoordinatorPolicyDownsizeWorkerConvergesWithoutFurtherReads(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-policy-downsize-converge", 8192)
	tier := manager.tiers[0]
	manager.startPolicyWorker()
	t.Cleanup(func() { manager.stopPolicyWorker() })

	chunk := bytes.Repeat([]byte("z"), 192)
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{0x31}, 0, len(chunk))
	key := tier.entryKey(identity)
	digest := sha256.Sum256(chunk)
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: manager.repoID,
		TierID:       tier.id,
		Generation:   1,
		CommitOrder:  1,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(chunk),
		ChunkSize:    tier.chunkSize,
		Trust:        tier.trust,
		DigestSHA256: hex.EncodeToString(digest[:]),
	}
	meta.Signature = manager.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	dataHandle, metaHandle := tier.handlesForIdentity(key, identity, 1, 1)
	entry, err := tier.saveEntry(
		context.Background(), key, dataHandle, metaHandle, chunk, metaRaw, 1, 1,
		identity.KeyGeneration, tier.trust, identity.Representation, identity.LayoutID+"|"+identity.ContentID,
	)
	if err != nil || entry == nil {
		t.Fatalf("expected seeded local cache entry: entry=%v err=%v", entry != nil, err)
	}
	tier.publishEntry(key, 1, 1)
	if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:        quotaEntryID(manager.repoID, key, 1),
			PolicyRevision: manager.currentPolicyRevision(),
			Key:            key,
			TierID:         tier.id,
			Generation:     1,
			CommitOrder:    1,
			Bytes:          uint64(len(chunk) + len(metaRaw)),
			State:          readCacheStatePublished,
			ManagerID:      manager.managerID,
			UpdatedMS:      now.UnixMilli(),
		})
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	manager.reconcileAdmissionVisibility(context.Background())

	shrink := uint64(256)
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: tier.id, MaxBytes: &shrink}); err != nil {
		t.Fatalf("policy shrink failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tier.lookupEntry(key) == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if tier.lookupEntry(key) != nil {
		t.Fatal("downsize worker should retire over-limit entry without further reads/admissions")
	}
}

func TestReadCacheCoordinatorPolicyDownsizeDeleteOutageReportsPinnedAndDeleting(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-policy-downsize-outage", 4096)
	tier := manager.tiers[0]

	chunk := bytes.Repeat([]byte("p"), 160)
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{0x41}, 0, len(chunk))
	key := tier.entryKey(identity)
	digest := sha256.Sum256(chunk)
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: manager.repoID,
		TierID:       tier.id,
		Generation:   1,
		CommitOrder:  1,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(chunk),
		ChunkSize:    tier.chunkSize,
		Trust:        tier.trust,
		DigestSHA256: hex.EncodeToString(digest[:]),
	}
	meta.Signature = manager.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	dataHandle, metaHandle := tier.handlesForIdentity(key, identity, 1, 1)
	entry, err := tier.saveEntry(
		context.Background(), key, dataHandle, metaHandle, chunk, metaRaw, 1, 1,
		identity.KeyGeneration, tier.trust, identity.Representation, identity.LayoutID+"|"+identity.ContentID,
	)
	if err != nil || entry == nil {
		t.Fatalf("expected seeded local cache entry: entry=%v err=%v", entry != nil, err)
	}
	tier.publishEntry(key, 1, 1)
	if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:        quotaEntryID(manager.repoID, key, 1),
			PolicyRevision: manager.currentPolicyRevision(),
			Key:            key,
			TierID:         tier.id,
			Generation:     1,
			CommitOrder:    1,
			Bytes:          uint64(len(chunk) + len(metaRaw)),
			State:          readCacheStatePublished,
			ManagerID:      manager.managerID,
			UpdatedMS:      now.UnixMilli(),
		})
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	manager.reconcileAdmissionVisibility(context.Background())
	entry = tier.pinEntry(context.Background(), key)
	if entry == nil {
		t.Fatal("expected pinned entry before policy downsize")
	}
	defer tier.unpinEntry(t.Context(), entry)
	manager.startPolicyWorker()
	t.Cleanup(func() { manager.stopPolicyWorker() })

	cache.failRemove.Store(true)
	shrink := uint64(512)
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: tier.id, MaxBytes: &shrink}); err != nil {
		t.Fatalf("policy shrink failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tier.lookupEntry(key) == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if tier.lookupEntry(key) != nil {
		t.Fatal("downsize worker should promptly remove over-limit entry from servable set")
	}

	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var deletingFound bool
	for _, item := range ledger.Entries {
		if item.Key == key {
			deletingFound = item.State == readCacheStateDeleting
			break
		}
	}
	if !deletingFound {
		t.Fatal("expected deleting ledger state during retirement delete outage")
	}

	tier.mu.Lock()
	pendingDelete := tier.pendingDelete
	pinned := tier.pinned
	tier.mu.Unlock()
	if pendingDelete == 0 {
		t.Fatal("expected pending delete bytes during delete outage")
	}
	if pinned == 0 {
		t.Fatal("expected pinned bytes to be reported while entry remains pinned")
	}
}

func TestReadCacheCoordinatorDeleteOutageLeavesQuotaChargedUntilConfirmedAbsence(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-delete-outage", 2048)
	chunk := bytes.Repeat([]byte("d"), 128)
	manager.storeRange(context.Background(), manager.tiers[0], vaultic.ID{9}, 0, chunk)

	var victim *readCacheEntry
	manager.tiers[0].mu.Lock()
	for _, entry := range manager.tiers[0].entries {
		victim = entry
		break
	}
	manager.tiers[0].mu.Unlock()
	if victim == nil {
		t.Fatal("expected cache entry to retire")
	}

	cache.failRemove.Store(true)
	manager.tiers[0].retireEntry(context.Background(), victim.Key)
	raw1, ledger1, err := manager.loadQuota(context.Background())
	if err != nil || len(raw1) == 0 {
		t.Fatalf("load quota after failed deletion: raw=%d err=%v", len(raw1), err)
	}
	var foundBefore bool
	for _, item := range ledger1.Entries {
		if item.Key == victim.Key {
			foundBefore = true
			break
		}
	}
	if !foundBefore {
		t.Fatal("quota should remain charged while deletion is unconfirmed")
	}

	cache.failRemove.Store(false)
	manager.tiers[0].deleteRetiringEntry(context.Background(), victim)
	_, ledger2, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range ledger2.Entries {
		if item.Key == victim.Key {
			t.Fatal("quota should uncharge only after confirmed absence")
		}
	}
	if _, found := findQuotaEntry(ledger2, quotaEntryID(manager.repoID, victim.Key, readCacheEntryOrder(victim))); found {
		t.Fatal("quota should uncharge only after confirmed absence")
	}
}

func TestReadCacheCoordinatorFinalizeDeletionTransientStatFailureKeepsQuotaCharged(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-stat-outage", 2048)
	chunk := bytes.Repeat([]byte("s"), 128)
	manager.storeRange(context.Background(), manager.tiers[0], vaultic.ID{11}, 0, chunk)

	var victim *readCacheEntry
	manager.tiers[0].mu.Lock()
	for _, entry := range manager.tiers[0].entries {
		victim = entry
		break
	}
	manager.tiers[0].mu.Unlock()
	if victim == nil {
		t.Fatal("expected cache entry to retire")
	}

	cache.failStatOnce.Store(true)
	manager.tiers[0].retireEntry(context.Background(), victim.Key)
	_, ledgerAfterFailure, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfterFailure, quotaEntryID(manager.repoID, victim.Key, readCacheEntryOrder(victim))); !found {
		t.Fatal("quota should stay charged when finalizeDeletion stat is transiently unavailable")
	}

	manager.finalizeDeletion(context.Background(), manager.tiers[0].id, victim)
	_, ledgerAfterRecovery, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfterRecovery, quotaEntryID(manager.repoID, victim.Key, readCacheEntryOrder(victim))); found {
		t.Fatal("quota should uncharge once both data/meta are confirmed not found")
	}
}

func TestReadCacheCoordinatorLegacyGenerationlessRestartCleanupChargesUntilPurge(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	seed := newTestReadCacheManager(t, shared, cache, "manager-legacy-seed", 4096)
	seedTier := seed.tiers[0]
	identity := seed.sourceRangeIdentity(seedTier, vaultic.ID{42}, 0, 128)
	key := seedTier.entryKey(identity)
	payload := bytes.Repeat([]byte("l"), 128)
	digestBytes := sha256.Sum256(payload)
	digest := hex.EncodeToString(digestBytes[:])
	legacyData := backend.Handle{Type: backend.StagingFile, Name: readCacheNamespace + "/legacy/seed.bin"}
	legacyMeta := backend.Handle{Type: backend.StagingFile, Name: readCacheNamespace + "/legacy/seed.json"}
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: seed.repoID,
		TierID:       seedTier.id,
		Generation:   0,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(payload),
		ChunkSize:    seedTier.chunkSize,
		Trust:        seedTier.trust,
		DigestSHA256: digest,
	}
	meta.Signature = seed.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), legacyData, backend.NewByteReader(payload, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), legacyMeta, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
		t.Fatal(err)
	}

	cache.failRemove.Store(true)
	restarted := newTestReadCacheManager(t, shared, cache, "manager-legacy-restart", 4096)
	restartedTier := restarted.tiers[0]
	restartedTier.mu.Lock()
	pendingAfterFailure := restartedTier.pendingDelete
	usedAfterFailure := restartedTier.used
	failuresAfterFailure := restartedTier.purgeFailures
	restartedTier.mu.Unlock()
	if pendingAfterFailure == 0 || usedAfterFailure == 0 || failuresAfterFailure == 0 {
		t.Fatalf(
			"legacy generationless pair should stay charged and pending on delete outage: pending=%d used=%d failures=%d",
			pendingAfterFailure, usedAfterFailure, failuresAfterFailure,
		)
	}

	cache.failRemove.Store(false)
	restartedTier.reconcile(context.Background(), restarted)
	if _, err := cache.Stat(context.Background(), legacyData); err == nil {
		t.Fatal("expected legacy data sidecar purged after retry")
	}
	if _, err := cache.Stat(context.Background(), legacyMeta); err == nil {
		t.Fatal("expected legacy metadata sidecar purged after retry")
	}
	restartedTier.mu.Lock()
	pendingAfterPurge := restartedTier.pendingDelete
	usedAfterPurge := restartedTier.used
	restartedTier.mu.Unlock()
	if pendingAfterPurge != 0 || usedAfterPurge != 0 {
		t.Fatalf("legacy cleanup should release charge after purge: pending=%d used=%d", pendingAfterPurge, usedAfterPurge)
	}
}

func TestReadCacheHandleDescriptorRoundtripFromHandlesForIdentity(t *testing.T) {
	manager := newTestReadCacheManager(t, mem.New(), mem.New(), "manager-descriptor-roundtrip", 2048)
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{51}, 0, 128)
	key := tier.entryKey(identity)
	dataHandle, metaHandle := tier.handlesForIdentity(key, identity, 17, 29)

	for _, handle := range []backend.Handle{dataHandle, metaHandle} {
		descriptor := parseReadCacheHandleDescriptor(handle.Name)
		if !descriptor.HasGeneration || descriptor.Generation != 17 {
			t.Fatalf("descriptor generation mismatch for %q: %#v", handle.Name, descriptor)
		}
		if !descriptor.HasCommitOrder || descriptor.CommitOrder != 29 {
			t.Fatalf("descriptor commit order mismatch for %q: %#v", handle.Name, descriptor)
		}
	}
}

func TestReadCacheReconcileRejectsDescriptorPathMetaMismatch(t *testing.T) {
	sourceRep := sanitizeCachePathSegment(readCacheRepSourceEncryptedRange)
	wholeFileRep := sanitizeCachePathSegment(readCacheRepWholeFile)
	encryptedTrust := sanitizeCachePathSegment(readCacheTrustEncrypted)
	plaintextTrust := sanitizeCachePathSegment(readCacheTrustPlaintext)
	digestA := strings.Repeat("a", 64)
	tests := []struct {
		name          string
		tierSegment   string
		repSegment    string
		trustSegment  string
		keyGeneration uint64
		generation    uint64
		commitOrder   uint64
		digest        string
		metaExt       string
		dataExt       string
	}{
		{name: "tier", tierSegment: "other-tier", repSegment: sourceRep, trustSegment: encryptedTrust,
			keyGeneration: 1, generation: 7, commitOrder: 9, digest: digestA, metaExt: "json", dataExt: "bin"},
		{name: "representation", tierSegment: "rc", repSegment: wholeFileRep, trustSegment: encryptedTrust,
			keyGeneration: 1, generation: 7, commitOrder: 9, digest: digestA, metaExt: "json", dataExt: "bin"},
		{name: "trust", tierSegment: "rc", repSegment: sourceRep, trustSegment: plaintextTrust,
			keyGeneration: 1, generation: 7, commitOrder: 9, digest: digestA, metaExt: "json", dataExt: "bin"},
		{name: "key-generation", tierSegment: "rc", repSegment: sourceRep, trustSegment: encryptedTrust,
			keyGeneration: 99, generation: 7, commitOrder: 9, digest: digestA, metaExt: "json", dataExt: "bin"},
		{name: "generation", tierSegment: "rc", repSegment: sourceRep, trustSegment: encryptedTrust,
			keyGeneration: 1, generation: 8, commitOrder: 9, digest: digestA, metaExt: "json", dataExt: "bin"},
		{name: "commit-order", tierSegment: "rc", repSegment: sourceRep, trustSegment: encryptedTrust,
			keyGeneration: 1, generation: 7, commitOrder: 10, digest: digestA, metaExt: "json", dataExt: "bin"},
		{name: "digest", tierSegment: "rc", repSegment: sourceRep, trustSegment: encryptedTrust,
			keyGeneration: 1, generation: 7, commitOrder: 9, digest: strings.Repeat("f", 64), metaExt: "json", dataExt: "bin"},
		{name: "extension-pair", tierSegment: "rc", repSegment: sourceRep, trustSegment: encryptedTrust,
			keyGeneration: 1, generation: 7, commitOrder: 9, digest: digestA, metaExt: "bin", dataExt: "json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shared := newReadCacheFaultBackend(mem.New())
			cache := newReadCacheFaultBackend(mem.New())
			manager := newTestReadCacheManager(t, shared, cache, "manager-descriptor-mismatch-"+tc.name, 4096)
			tier := manager.tiers[0]
			identity := manager.sourceRangeIdentity(tier, vaultic.ID{52}, 0, 128)
			key := tier.entryKey(identity)
			payload := bytes.Repeat([]byte("m"), 128)
			digest := sha256.Sum256(payload)
			meta := readCacheChunkMeta{
				Format:       readCacheFormatV2,
				RepositoryID: manager.repoID,
				TierID:       tier.id,
				Generation:   7,
				CommitOrder:  9,
				Identity:     identity,
				PackID:       identity.ContentID,
				Offset:       identity.Offset,
				Length:       len(payload),
				ChunkSize:    tier.chunkSize,
				Trust:        tier.trust,
				DigestSHA256: hex.EncodeToString(digest[:]),
			}
			meta.Signature = manager.signMeta(meta, key)
			metaRaw, err := json.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}

			goodDigest := tc.digest
			if tc.name != "digest" {
				digestKey := sha256.Sum256([]byte(key))
				goodDigest = hex.EncodeToString(digestKey[:])
			}
			metaName := readCacheNamespace + "/chunks/" +
				tc.tierSegment + "/" +
				tc.repSegment + "/" +
				tc.trustSegment + "/" +
				"kg-" + strconv.FormatUint(tc.keyGeneration, 10) + "/" +
				"g-" + strconv.FormatUint(tc.generation, 10) + "/" +
				"o-" + strconv.FormatUint(tc.commitOrder, 10) + "/" +
				goodDigest + "." + tc.metaExt
			dataName := readCacheNamespace + "/chunks/" +
				tc.tierSegment + "/" +
				tc.repSegment + "/" +
				tc.trustSegment + "/" +
				"kg-" + strconv.FormatUint(tc.keyGeneration, 10) + "/" +
				"g-" + strconv.FormatUint(tc.generation, 10) + "/" +
				"o-" + strconv.FormatUint(tc.commitOrder, 10) + "/" +
				goodDigest + "." + tc.dataExt
			metaHandle := backend.Handle{Type: backend.StagingFile, Name: metaName}
			dataHandle := backend.Handle{Type: backend.StagingFile, Name: dataName}
			if err := cache.Save(context.Background(), dataHandle, backend.NewByteReader(payload, cache.Hasher())); err != nil {
				t.Fatal(err)
			}
			if err := cache.Save(context.Background(), metaHandle, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
				t.Fatal(err)
			}

			tier.reconcile(context.Background(), manager)

			if _, err := cache.Stat(context.Background(), dataHandle); err == nil {
				t.Fatalf("expected mismatched data descriptor (%s) to be purged", tc.name)
			}
			if _, err := cache.Stat(context.Background(), metaHandle); err == nil {
				t.Fatalf("expected mismatched metadata descriptor (%s) to be purged", tc.name)
			}
			if entry := tier.lookupEntry(key); entry != nil {
				t.Fatalf("mismatched descriptor (%s) entry should not be servable: %#v", tc.name, entry)
			}
		})
	}
}

func TestReadCacheReconcileLegacyGenerationsWithoutAuthorityCannotSelectWinner(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-legacy-ambiguous", 4096)
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{53}, 0, 128)
	key := tier.entryKey(identity)

	type legacyPair struct {
		generation uint64
		suffix     string
		data       backend.Handle
		meta       backend.Handle
	}
	pairs := []legacyPair{
		{generation: 101, suffix: "a"},
		{generation: 202, suffix: "b"},
	}
	for i := range pairs {
		payload := bytes.Repeat([]byte{pairs[i].suffix[0]}, 128)
		digest := sha256.Sum256(payload)
		ns := readCacheNamespace + "/chunks/" +
			sanitizeCachePathSegment(tier.id) + "/" +
			sanitizeCachePathSegment(identity.Representation) + "/" +
			sanitizeCachePathSegment(identity.Trust) + "/" +
			"kg-" + "1" + "/" +
			"g-" + strconv.FormatUint(pairs[i].generation, 10) + "/" +
			strings.Repeat(pairs[i].suffix, 64)
		pairs[i].data = backend.Handle{Type: backend.StagingFile, Name: ns + ".bin"}
		pairs[i].meta = backend.Handle{Type: backend.StagingFile, Name: ns + ".json"}
		meta := readCacheChunkMeta{
			Format:       readCacheFormatV2,
			RepositoryID: manager.repoID,
			TierID:       tier.id,
			Generation:   pairs[i].generation,
			Identity:     identity,
			PackID:       identity.ContentID,
			Offset:       identity.Offset,
			Length:       len(payload),
			ChunkSize:    tier.chunkSize,
			Trust:        tier.trust,
			DigestSHA256: hex.EncodeToString(digest[:]),
		}
		meta.Signature = manager.signMeta(meta, key)
		metaRaw, err := json.Marshal(meta)
		if err != nil {
			t.Fatal(err)
		}
		if err := cache.Save(context.Background(), pairs[i].data, backend.NewByteReader(payload, cache.Hasher())); err != nil {
			t.Fatal(err)
		}
		if err := cache.Save(context.Background(), pairs[i].meta, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
			t.Fatal(err)
		}
	}

	tier.reconcile(context.Background(), manager)

	if entry := tier.lookupEntry(key); entry != nil {
		t.Fatalf("legacy random generations must not pick a winner without authority: %#v", entry)
	}
	for _, pair := range pairs {
		if _, err := cache.Stat(context.Background(), pair.data); err == nil {
			t.Fatalf("expected legacy data %q purged", pair.data.Name)
		}
		if _, err := cache.Stat(context.Background(), pair.meta); err == nil {
			t.Fatalf("expected legacy metadata %q purged", pair.meta.Name)
		}
	}
}

func TestReadCacheReconcileLegacyGenerationWithoutCommitOrderIsPurgedEvenWithActiveLedger(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-legacy-migrate", 4096)
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{54}, 0, 128)
	key := tier.entryKey(identity)
	generation := uint64(303)
	commitOrder := uint64(77)
	payload := bytes.Repeat([]byte("z"), 128)
	digest := sha256.Sum256(payload)
	ns := readCacheNamespace + "/chunks/" +
		sanitizeCachePathSegment(tier.id) + "/" +
		sanitizeCachePathSegment(identity.Representation) + "/" +
		sanitizeCachePathSegment(identity.Trust) + "/" +
		"kg-1/g-" + strconv.FormatUint(generation, 10) + "/" + strings.Repeat("c", 64)
	dataHandle := backend.Handle{Type: backend.StagingFile, Name: ns + ".bin"}
	metaHandle := backend.Handle{Type: backend.StagingFile, Name: ns + ".json"}
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: manager.repoID,
		TierID:       tier.id,
		Generation:   generation,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(payload),
		ChunkSize:    tier.chunkSize,
		Trust:        tier.trust,
		DigestSHA256: hex.EncodeToString(digest[:]),
	}
	meta.Signature = manager.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), dataHandle, backend.NewByteReader(payload, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), metaHandle, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
		t.Fatal(err)
	}

	err = manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:     quotaEntryID(manager.repoID, key, commitOrder),
			Key:         key,
			TierID:      tier.id,
			Generation:  generation,
			CommitOrder: commitOrder,
			Bytes:       uint64(len(payload) + len(metaRaw)),
			State:       readCacheStateActive,
			ManagerID:   manager.managerID,
			UpdatedMS:   now.UnixMilli(),
		})
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	byGeneration, ok := manager.activeQuotaByKeyGeneration(context.Background(), tier.id)
	if !ok || byGeneration[quotaEntryID(manager.repoID, key, generation)] != commitOrder {
		t.Fatalf("expected exact-generation authority mapping before reconcile: ok=%v mapping=%#v", ok, byGeneration)
	}
	if byKey, ok := manager.activeQuotaByKey(context.Background(), tier.id); !ok || byKey[key].CommitOrder != commitOrder {
		t.Fatalf("expected active authority winner before reconcile: ok=%v winner=%#v", ok, byKey[key])
	}

	tier.reconcile(context.Background(), manager)

	if entry := tier.lookupEntry(key); entry != nil {
		t.Fatalf("legacy path without commit-order must remain unservable: %#v", entry)
	}
	if _, err := cache.Stat(context.Background(), dataHandle); err == nil {
		t.Fatal("expected legacy data file purged")
	}
	if _, err := cache.Stat(context.Background(), metaHandle); err == nil {
		t.Fatal("expected legacy metadata file purged")
	}
	_, ledgerAfter, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfter, quotaEntryID(manager.repoID, key, commitOrder)); found {
		t.Fatal("expected legacy active-ledger row removed after successful purge")
	}
}

func TestReadCacheReconcileLegacyGenerationWithoutCommitOrderDeleteOutageKeepsLedgerCharged(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-legacy-migrate-outage", 4096)
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{55}, 0, 128)
	key := tier.entryKey(identity)
	generation := uint64(404)
	commitOrder := uint64(88)
	payload := bytes.Repeat([]byte("w"), 128)
	digest := sha256.Sum256(payload)
	ns := readCacheNamespace + "/chunks/" +
		sanitizeCachePathSegment(tier.id) + "/" +
		sanitizeCachePathSegment(identity.Representation) + "/" +
		sanitizeCachePathSegment(identity.Trust) + "/" +
		"kg-1/g-" + strconv.FormatUint(generation, 10) + "/" + strings.Repeat("d", 64)
	dataHandle := backend.Handle{Type: backend.StagingFile, Name: ns + ".bin"}
	metaHandle := backend.Handle{Type: backend.StagingFile, Name: ns + ".json"}
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: manager.repoID,
		TierID:       tier.id,
		Generation:   generation,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(payload),
		ChunkSize:    tier.chunkSize,
		Trust:        tier.trust,
		DigestSHA256: hex.EncodeToString(digest[:]),
	}
	meta.Signature = manager.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), dataHandle, backend.NewByteReader(payload, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), metaHandle, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
		t.Fatal(err)
	}

	err = manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:     quotaEntryID(manager.repoID, key, commitOrder),
			Key:         key,
			TierID:      tier.id,
			Generation:  generation,
			CommitOrder: commitOrder,
			Bytes:       uint64(len(payload) + len(metaRaw)),
			State:       readCacheStateActive,
			ManagerID:   manager.managerID,
			UpdatedMS:   now.UnixMilli(),
		})
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	cache.failRemove.Store(true)
	tier.reconcile(context.Background(), manager)

	_, ledgerAfterFailure, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, found := findQuotaEntry(ledgerAfterFailure, quotaEntryID(manager.repoID, key, commitOrder))
	if !found {
		t.Fatal("expected legacy row to remain charged while deletion is unconfirmed")
	}
	if entry.State != readCacheStateDeleting {
		t.Fatalf("expected deleting state during outage, got %q", entry.State)
	}

	cache.failRemove.Store(false)
	tier.reconcile(context.Background(), manager)

	if _, err := cache.Stat(context.Background(), dataHandle); err == nil {
		t.Fatal("expected legacy data file purged after retry")
	}
	if _, err := cache.Stat(context.Background(), metaHandle); err == nil {
		t.Fatal("expected legacy metadata file purged after retry")
	}
	_, ledgerAfterPurge, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfterPurge, quotaEntryID(manager.repoID, key, commitOrder)); found {
		t.Fatal("expected legacy active-ledger row removed after successful retry purge")
	}
}

func TestReadCacheCoordinatorCommitFailureAfterSaveKeepsEntryHiddenAndQuotaChargedUntilDeletionConfirmed(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-commit-failure", 4096)

	shared.quotaCASCalls.Store(0)
	shared.failQuotaCASAt.Store(2) // reserve admission succeeds; commit admission fails
	cache.failRemove.Store(true)   // retiring delete path cannot confirm physical deletion yet

	tier := manager.tiers[0]
	packID := vaultic.ID{12}
	chunk := bytes.Repeat([]byte("p"), 128)
	identity := manager.sourceRangeIdentity(tier, packID, 0, len(chunk))
	key := tier.entryKey(identity)

	manager.storeRange(context.Background(), tier, packID, 0, chunk)

	if got, ok := manager.loadRepresentationFromTier(context.Background(), tier, identity); ok || got != nil {
		t.Fatal("cache entry must stay hidden when admission commit fails after save")
	}

	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entryID := quotaEntryID(manager.repoID, key, commitOrderFromKeyID(ledger, key))
	item, found := findQuotaEntry(ledger, entryID)
	if !found {
		t.Fatal("quota entry should remain charged after persisted commit failure")
	}
	if item.State != readCacheStateDeleting {
		t.Fatalf("expected deleting state to remain charged until deletion is confirmed, got %q", item.State)
	}

	var deleting *readCacheEntry
	tier.mu.Lock()
	if _, exists := tier.entries[key]; exists {
		tier.mu.Unlock()
		t.Fatal("persisted entry should be retired from servable entries after commit failure")
	}
	deleting = tier.deleting[entryID]
	tier.mu.Unlock()
	if deleting == nil {
		t.Fatal("expected persisted uncommitted entry to move into deleting state")
	}

	shared.failQuotaCASAt.Store(0)
	cache.failRemove.Store(false)

	recovered := newTestReadCacheManager(t, shared, cache, "manager-commit-recovery", 4096)
	recoveredTier := recovered.tiers[0]
	if got, ok := recovered.loadRepresentationFromTier(context.Background(), recoveredTier, identity); ok || got != nil {
		t.Fatal("recovery reconciliation must not serve uncommitted published data")
	}

	_, ledgerAfterRecovery, err := recovered.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfterRecovery, entryID); found {
		t.Fatal("quota charge should clear only after deletion is physically confirmed")
	}
}

func TestReadCacheCoordinatorDeleteOutageReadmitSameKeyKeepsNewGeneration(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-delete-readmit", 8192)
	tier := manager.tiers[0]
	packID := vaultic.ID{23}
	identity := manager.sourceRangeIdentity(tier, packID, 0, 128)
	key := tier.entryKey(identity)
	originalGenerationSource := readCacheGenerationSource
	defer func() { readCacheGenerationSource = originalGenerationSource }()
	generations := []uint64{900, 100}
	readCacheGenerationSource = func() uint64 {
		if len(generations) == 0 {
			return originalGenerationSource()
		}
		value := generations[0]
		generations = generations[1:]
		return value
	}

	chunkV1 := bytes.Repeat([]byte("1"), 128)
	manager.storeRange(context.Background(), tier, packID, 0, chunkV1)

	entryV1 := tier.lookupEntry(key)
	if entryV1 == nil {
		t.Fatal("expected first generation entry")
	}
	generationV1 := entryV1.Generation

	cache.failRemove.Store(true)
	tier.retireEntry(context.Background(), key)

	_, ledgerAfterOutage, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entryIDV1 := quotaEntryID(manager.repoID, key, readCacheEntryOrder(entryV1))
	v1Quota, foundV1 := findQuotaEntry(ledgerAfterOutage, entryIDV1)
	if !foundV1 {
		t.Fatal("expected first generation to remain charged during delete outage")
	}
	if v1Quota.State != readCacheStateDeleting {
		t.Fatalf("expected first generation in deleting state during outage, got %q", v1Quota.State)
	}

	chunkV2 := bytes.Repeat([]byte("2"), 128)
	manager.storeRange(context.Background(), tier, packID, 0, chunkV2)

	entryV2 := tier.lookupEntry(key)
	if entryV2 == nil {
		t.Fatal("expected re-admitted generation entry")
	}
	if entryV2.Generation >= generationV1 {
		t.Fatalf("expected lower random generation with newer commit order, old=%d new=%d", generationV1, entryV2.Generation)
	}

	served, ok := manager.loadRepresentationFromTier(context.Background(), tier, identity)
	if !ok || !bytes.Equal(served, chunkV2) {
		t.Fatal("expected newer generation to be readable while old deletion is pending")
	}

	_, ledgerWithBoth, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerWithBoth, entryIDV1); !found {
		t.Fatal("expected old generation to remain charged while delete outage persists")
	}
	entryIDV2 := quotaEntryID(manager.repoID, key, readCacheEntryOrder(entryV2))
	v2Quota, foundV2 := findQuotaEntry(ledgerWithBoth, entryIDV2)
	if !foundV2 {
		t.Fatal("expected new generation to be charged")
	}
	if !quotaEntryIsPublished(v2Quota.State) {
		t.Fatalf("expected new generation published state, got %q", v2Quota.State)
	}

	cache.failRemove.Store(false)
	tier.deleteRetiringEntry(context.Background(), entryV1)

	servedAfterResume, ok := manager.loadRepresentationFromTier(context.Background(), tier, identity)
	if !ok || !bytes.Equal(servedAfterResume, chunkV2) {
		t.Fatal("expected newer generation to remain readable after old deletion resumes")
	}

	_, ledgerFinal, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerFinal, entryIDV1); found {
		t.Fatal("expected old generation charge to clear after confirmed deletion")
	}
	if _, found := findQuotaEntry(ledgerFinal, entryIDV2); !found {
		t.Fatal("expected newer generation to remain accounted")
	}

	recovered := newTestReadCacheManager(t, shared, cache, "manager-delete-readmit-restart", 8192)
	recoveredTier := recovered.tiers[0]
	servedAfterRestart, ok := recovered.loadRepresentationFromTier(context.Background(), recoveredTier, identity)
	if !ok || !bytes.Equal(servedAfterRestart, chunkV2) {
		t.Fatal("expected restart reconcile to keep authoritative newer generation selected by commit order")
	}
}

func TestReadCacheReconcileLoneMetaCanonicalCleanupTracksQuotaUntilRecovery(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-lone-meta", 4096)
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{61}, 0, 128)
	key := tier.entryKey(identity)
	generation := uint64(501)
	commitOrder := uint64(301)
	payload := bytes.Repeat([]byte("m"), 128)
	digest := sha256.Sum256(payload)
	dataHandle, metaHandle := tier.handlesForIdentity(key, identity, generation, commitOrder)
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: manager.repoID,
		TierID:       tier.id,
		Generation:   generation,
		CommitOrder:  commitOrder,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(payload),
		ChunkSize:    tier.chunkSize,
		Trust:        tier.trust,
		DigestSHA256: hex.EncodeToString(digest[:]),
	}
	meta.Signature = manager.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), metaHandle, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:     quotaEntryID(manager.repoID, key, commitOrder),
			Key:         key,
			TierID:      tier.id,
			Generation:  generation,
			CommitOrder: commitOrder,
			Bytes:       uint64(len(payload) + len(metaRaw)),
			State:       readCacheStateActive,
			ManagerID:   manager.managerID,
			UpdatedMS:   now.UnixMilli(),
		})
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	cache.failRemove.Store(true)
	tier.reconcile(context.Background(), manager)
	_, ledgerAfterFailure, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, found := findQuotaEntry(ledgerAfterFailure, quotaEntryID(manager.repoID, key, commitOrder))
	if !found {
		t.Fatal("expected lone-meta row to remain charged during delete outage")
	}
	if entry.State != readCacheStateDeleting {
		t.Fatalf("expected deleting state during lone-meta outage, got %q", entry.State)
	}
	if _, err := cache.Stat(context.Background(), metaHandle); err != nil {
		t.Fatalf("expected lone meta to remain while remove is failing: %v", err)
	}

	cache.failRemove.Store(false)
	tier.reconcile(context.Background(), manager)
	if _, err := cache.Stat(context.Background(), metaHandle); err == nil {
		t.Fatal("expected lone meta file purged after recovery")
	}
	if _, err := cache.Stat(context.Background(), dataHandle); err == nil {
		t.Fatal("expected lone missing data side to remain absent")
	}
	_, ledgerAfterRecovery, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfterRecovery, quotaEntryID(manager.repoID, key, commitOrder)); found {
		t.Fatal("expected lone-meta quota row removed only after confirmed absence on both sides")
	}
}

func TestReadCacheReconcileLoneDataPublishingRowTracksQuotaUntilRecovery(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-lone-data", 4096)
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{62}, 0, 128)
	key := tier.entryKey(identity)
	generation := uint64(601)
	commitOrder := uint64(401)
	payload := bytes.Repeat([]byte("d"), 128)
	dataHandle, _ := tier.handlesForIdentity(key, identity, generation, commitOrder)
	if err := cache.Save(context.Background(), dataHandle, backend.NewByteReader(payload, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:     quotaEntryID(manager.repoID, key, commitOrder),
			Key:         key,
			TierID:      tier.id,
			Generation:  generation,
			CommitOrder: commitOrder,
			Bytes:       uint64(len(payload)),
			State:       readCacheStatePublishing,
			ManagerID:   manager.managerID,
			UpdatedMS:   now.UnixMilli(),
		})
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	cache.failRemove.Store(true)
	tier.reconcile(context.Background(), manager)
	_, ledgerAfterFailure, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, found := findQuotaEntry(ledgerAfterFailure, quotaEntryID(manager.repoID, key, commitOrder))
	state := ""
	if entry != nil {
		state = entry.State
	}
	if !found || state != readCacheStateDeleting {
		t.Fatalf("expected lone-data deleting row to stay charged on outage: found=%v state=%q", found, state)
	}
	if _, err := cache.Stat(context.Background(), dataHandle); err != nil {
		t.Fatalf("expected lone data to remain while remove is failing: %v", err)
	}

	cache.failRemove.Store(false)
	tier.reconcile(context.Background(), manager)
	if _, err := cache.Stat(context.Background(), dataHandle); err == nil {
		t.Fatal("expected lone data file purged after recovery")
	}
	_, ledgerAfterRecovery, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfterRecovery, quotaEntryID(manager.repoID, key, commitOrder)); found {
		t.Fatal("expected lone-data quota row removed only after confirmed data deletion and missing-meta absence")
	}
}

func seedPublishingCrashEntry(
	t *testing.T,
	manager *readCacheManager,
	owner string,
	persisted bool,
	malformed bool,
) (string, uint64) {
	t.Helper()
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{0xCA}, 0, 128)
	key := tier.entryKey(identity)
	generation := uint64(801)
	commitOrder := uint64(601)
	payload := bytes.Repeat([]byte("c"), 128)
	metaRaw := []byte("{")
	if persisted && !malformed {
		digest := sha256.Sum256(payload)
		meta := readCacheChunkMeta{
			Format: readCacheFormatV2, RepositoryID: manager.repoID, TierID: tier.id,
			Generation: generation, CommitOrder: commitOrder, Identity: identity,
			PackID: identity.ContentID, Offset: identity.Offset, Length: len(payload),
			ChunkSize: tier.chunkSize, Trust: tier.trust, DigestSHA256: hex.EncodeToString(digest[:]),
		}
		meta.Signature = manager.signMeta(meta, key)
		var err error
		metaRaw, err = json.Marshal(meta)
		if err != nil {
			t.Fatal(err)
		}
	}
	if persisted {
		dataHandle, metaHandle := tier.handlesForIdentity(key, identity, generation, commitOrder)
		if err := tier.backend.Save(context.Background(), dataHandle, backend.NewByteReader(payload, tier.backend.Hasher())); err != nil {
			t.Fatal(err)
		}
		if err := tier.backend.Save(context.Background(), metaHandle, backend.NewByteReader(metaRaw, tier.backend.Hasher())); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID: quotaEntryID(manager.repoID, key, commitOrder), RepositoryID: manager.repoID,
			Key: key, TierID: tier.id, Generation: generation, CommitOrder: commitOrder,
			Bytes: uint64(len(payload) + len(metaRaw)), State: readCacheStatePublishing,
			ManagerID: owner, UpdatedMS: now.UnixMilli(),
		})
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if persisted && !malformed {
		dataHandle, metaHandle := tier.handlesForIdentity(key, identity, generation, commitOrder)
		for _, handle := range []backend.Handle{dataHandle, metaHandle} {
			descriptor := parseReadCacheHandleDescriptor(handle.Name)
			if !readCacheDescriptorMatchesIdentity(descriptor, tier.id, identity, generation, commitOrder, key) {
				t.Fatalf("invalid persisted crash descriptor: %#v", descriptor)
			}
		}
	}
	entries, ok := manager.authoritativeQuotaEntries(context.Background(), tier.id)
	if !ok || protectedQuotaByKeyFromEntries(entries)[key].State != readCacheStatePublishing {
		t.Fatalf("publishing crash row was not authoritative before restart: ok=%v entries=%#v", ok, entries)
	}
	return key, commitOrder
}

func TestReadCachePublishingCrashRecovery(t *testing.T) {
	t.Run("exact-persisted-entry", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		seed := newTestReadCacheManager(t, shared, shared, "manager-crash-valid-seed", 4096)
		key, commitOrder := seedPublishingCrashEntry(t, seed, "expired-manager", true, false)
		restarted := newTestReadCacheManager(t, shared, shared, "manager-crash-valid-restart", 4096)

		_, ledger, err := restarted.loadQuota(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		entry, found := findQuotaEntry(ledger, quotaEntryID(restarted.repoID, key, commitOrder))
		if !found || !quotaEntryIsPublished(entry.State) || entry.ManagerID != restarted.managerID {
			t.Fatalf("persisted crash entry was not recovered: entry=%#v ledger=%#v", entry, ledger.Entries)
		}
		if cached := restarted.tiers[0].lookupEntry(key); cached == nil {
			t.Fatal("recovered persisted entry is not visible")
		}
	})

	for _, test := range []struct {
		name      string
		persisted bool
		malformed bool
	}{
		{name: "missing", persisted: false},
		{name: "malformed", persisted: true, malformed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			shared := newReadCacheFaultBackend(mem.New())
			seed := newTestReadCacheManager(t, shared, shared, "manager-crash-"+test.name+"-seed", 4096)
			key, commitOrder := seedPublishingCrashEntry(t, seed, "expired-manager", test.persisted, test.malformed)
			if test.malformed {
				shared.failRemove.Store(true)
			}
			restarted := newTestReadCacheManager(t, shared, shared, "manager-crash-"+test.name+"-restart", 4096)
			entryID := quotaEntryID(restarted.repoID, key, commitOrder)
			_, ledger, err := restarted.loadQuota(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			entry, found := findQuotaEntry(ledger, entryID)
			if !found || entry.State != readCacheStateDeleting {
				t.Fatalf("invalid crash entry did not transition to deleting: entry=%#v ledger=%#v", entry, ledger.Entries)
			}
			if test.malformed {
				shared.failRemove.Store(false)
				restarted.tiers[0].reconcile(context.Background(), restarted)
			}
			if err := restarted.reconcileQuotaInventory(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, ledger, err = restarted.loadQuota(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, found := findQuotaEntry(ledger, entryID); found {
				t.Fatal("invalid crash entry quota was not eventually released")
			}
		})
	}

	t.Run("live-manager", func(t *testing.T) {
		shared := newReadCacheFaultBackend(mem.New())
		seed := newTestReadCacheManager(t, shared, shared, "manager-crash-live-seed", 4096)
		key, commitOrder := seedPublishingCrashEntry(t, seed, seed.managerID, true, false)
		_, before, err := seed.loadQuota(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ownerLive := false
		for _, ref := range before.Managers {
			ownerLive = ownerLive || ref.ID == seed.managerID && ref.LeaseExpiryMS > time.Now().UTC().UnixMilli()
		}
		if !ownerLive {
			t.Fatalf("seed manager lease missing before restart: %#v", before.Managers)
		}
		restarted := newTestReadCacheManager(t, shared, shared, "manager-crash-live-restart", 4096)
		_, ledger, err := restarted.loadQuota(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		entry, found := findQuotaEntry(ledger, quotaEntryID(restarted.repoID, key, commitOrder))
		if !found || entry.State != readCacheStatePublishing || entry.ManagerID != seed.managerID {
			t.Fatalf("live-manager publishing row was disturbed: entry=%#v ledger=%#v", entry, ledger.Entries)
		}
	})
}

func TestReadCacheReconcileDoesNotMutateAnotherRepositoryRows(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	left := newTestReadCacheManagerForRepo(t, shared, shared, "repo-left", bytes.Repeat([]byte{0x31}, 32), "manager-left", 4096)
	right := newTestReadCacheManagerForRepo(t, shared, shared, "repo-right", bytes.Repeat([]byte{0x32}, 32), "manager-right", 4096)
	key, commitOrder := seedPublishingCrashEntry(t, left, left.managerID, true, false)
	foreignID := quotaEntryID(left.repoID, key, commitOrder)
	foreignPrefix := readCacheNamespace + "/chunks/" + readCacheRepositoryScope(left.repoID) + "/"
	countForeignFiles := func() int {
		count := 0
		if err := shared.List(context.Background(), backend.StagingFile, func(info backend.FileInfo) error {
			if strings.HasPrefix(info.Name, foreignPrefix) {
				count++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if count := countForeignFiles(); count != 2 {
		t.Fatalf("expected two foreign physical files before reconcile, got %d", count)
	}
	right.reconcileTiers(context.Background())
	if err := right.reconcileQuotaInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, ledger, err := right.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, found := findQuotaEntry(ledger, foreignID)
	if !found || entry.State != readCacheStatePublishing || entry.RepositoryID != left.repoID {
		t.Fatalf("foreign repository row was mutated: %#v", entry)
	}
	if count := countForeignFiles(); count != 2 {
		t.Fatalf("foreign repository physical files were removed: got %d", count)
	}
}

func TestReadCacheExpiredForeignPublishingReclaimsFullBudget(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	left := newTestReadCacheManagerForRepo(t, shared, shared, "foreign-left", bytes.Repeat([]byte{0x61}, 32), "left", 900)
	key, commitOrder := seedPublishingCrashEntry(t, left, "expired-left", true, false)
	dataHandle, metaHandle := left.tiers[0].handlesForIdentity(
		key, left.sourceRangeIdentity(left.tiers[0], vaultic.ID{0xCA}, 0, 128), 801, commitOrder,
	)
	if err := left.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, _ time.Time) (bool, error) {
		entry, ok := findQuotaEntry(ledger, quotaEntryID(left.repoID, key, commitOrder))
		if !ok {
			return false, nil
		}
		entry.DataHandle, entry.MetaHandle = dataHandle.Name, metaHandle.Name
		entry.Bytes = 900
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	right := newTestReadCacheManagerForRepo(t, shared, shared, "foreign-right", bytes.Repeat([]byte{0x62}, 32), "right", 900)
	if lease, ok := right.reserveAdmission(context.Background(), right.tiers[0], "right-entry", 128); !ok || lease == nil {
		t.Fatal("foreign expired publishing row wedged the shared cache budget")
	}
}

func TestReadCacheInitializationListTimeoutFallsBackToOrigin(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cache := newReadCacheFaultBackend(mem.New())
	cache.hangList.Store(true)
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary},
		{ID: "rc", Role: PlacementRoleReadCache, CapacityBytes: 4096, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cache)
	started := time.Now()
	payload, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil || len(payload) == 0 {
		t.Fatalf("origin fallback failed after cache initialization timeout: bytes=%d err=%v", len(payload), err)
	}
	if elapsed := time.Since(started); elapsed > readCacheInitializationTimeout+time.Second {
		t.Fatalf("cache initialization exceeded bound: %v", elapsed)
	}
	if repo.ReadCacheStatus().AdmissionsEnabled {
		t.Fatal("admissions must remain disabled after initialization timeout")
	}
}

func TestReadCacheRestartReconcileRetiresMalformedPublishedStatesAndReleasesQuota(t *testing.T) {
	for _, state := range []string{readCacheStatePublishing, readCacheStatePublished} {
		t.Run(state, func(t *testing.T) {
			shared := newReadCacheFaultBackend(mem.New())
			cache := newReadCacheFaultBackend(mem.New())
			manager := newTestReadCacheManager(t, shared, cache, "manager-malformed-seed-"+state, 4096)
			tier := manager.tiers[0]
			identity := manager.sourceRangeIdentity(tier, vaultic.ID{63}, 0, 128)
			key := tier.entryKey(identity)
			generation := uint64(700)
			commitOrder := uint64(500)
			dataHandle, metaHandle := tier.handlesForIdentity(key, identity, generation, commitOrder)
			payload := bytes.Repeat([]byte("x"), 128)
			malformedMeta := []byte("{")
			if err := cache.Save(context.Background(), dataHandle, backend.NewByteReader(payload, cache.Hasher())); err != nil {
				t.Fatal(err)
			}
			if err := cache.Save(context.Background(), metaHandle, backend.NewByteReader(malformedMeta, cache.Hasher())); err != nil {
				t.Fatal(err)
			}
			if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
				ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
					EntryID:     quotaEntryID(manager.repoID, key, commitOrder),
					Key:         key,
					TierID:      tier.id,
					Generation:  generation,
					CommitOrder: commitOrder,
					Bytes:       uint64(len(payload) + len(malformedMeta)),
					State:       state,
					ManagerID:   manager.managerID,
					UpdatedMS:   now.UnixMilli(),
				})
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}

			cache.failRemove.Store(true)
			recovered := newTestReadCacheManager(t, shared, cache, "manager-malformed-restart-"+state, 4096)
			_, ledgerDuringFailure, err := recovered.loadQuota(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			item, found := findQuotaEntry(ledgerDuringFailure, quotaEntryID(manager.repoID, key, commitOrder))
			if !found || item.State != readCacheStateDeleting {
				t.Fatalf("restart did not retain exact quota charge as deleting: found=%v item=%#v", found, item)
			}

			cache.failRemove.Store(false)
			recovered.tiers[0].reconcile(context.Background(), recovered)
			if _, err := cache.Stat(context.Background(), dataHandle); err == nil {
				t.Fatal("malformed entry data remained after confirmed reconciliation")
			}
			if _, err := cache.Stat(context.Background(), metaHandle); err == nil {
				t.Fatal("malformed entry metadata remained after confirmed reconciliation")
			}
			_, ledgerAfterRecovery, err := recovered.loadQuota(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, found := findQuotaEntry(ledgerAfterRecovery, quotaEntryID(manager.repoID, key, commitOrder)); found {
				t.Fatal("quota charge remained after malformed entry deletion was confirmed")
			}
		})
	}
}

func TestReadCacheReconcileDescriptorIndexCleansExactStaleNonWinnerRow(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, cache, "manager-stale-non-winner", 4096)
	tier := manager.tiers[0]
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{63}, 0, 128)
	key := tier.entryKey(identity)
	generation := uint64(701)
	staleCommit := uint64(501)
	winnerCommit := uint64(502)
	payload := bytes.Repeat([]byte("s"), 128)
	digest := sha256.Sum256(payload)
	dataHandle, metaHandle := tier.handlesForIdentity(key, identity, generation, staleCommit)
	meta := readCacheChunkMeta{
		Format:       readCacheFormatV2,
		RepositoryID: manager.repoID,
		TierID:       tier.id,
		Generation:   generation,
		CommitOrder:  staleCommit,
		Identity:     identity,
		PackID:       identity.ContentID,
		Offset:       identity.Offset,
		Length:       len(payload),
		ChunkSize:    tier.chunkSize,
		Trust:        tier.trust,
		DigestSHA256: hex.EncodeToString(digest[:]),
	}
	meta.Signature = manager.signMeta(meta, key)
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), dataHandle, backend.NewByteReader(payload, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(context.Background(), metaHandle, backend.NewByteReader(metaRaw, cache.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		ledger.Entries = append(ledger.Entries,
			readCacheQuotaEntry{
				EntryID:     quotaEntryID(manager.repoID, key, staleCommit),
				Key:         key,
				TierID:      tier.id,
				Generation:  generation,
				CommitOrder: staleCommit,
				Bytes:       uint64(len(payload) + len(metaRaw)),
				State:       readCacheStateActive,
				ManagerID:   manager.managerID,
				UpdatedMS:   now.UnixMilli(),
			},
			readCacheQuotaEntry{
				EntryID:     quotaEntryID(manager.repoID, key, winnerCommit),
				Key:         key,
				TierID:      tier.id,
				Generation:  generation,
				CommitOrder: winnerCommit,
				Bytes:       uint64(len(payload) + len(metaRaw)),
				State:       readCacheStateActive,
				ManagerID:   manager.managerID,
				UpdatedMS:   now.UnixMilli(),
			},
		)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	tier.reconcile(context.Background(), manager)

	if _, err := cache.Stat(context.Background(), dataHandle); err == nil {
		t.Fatal("expected stale non-winner data purged")
	}
	if _, err := cache.Stat(context.Background(), metaHandle); err == nil {
		t.Fatal("expected stale non-winner meta purged")
	}
	_, ledgerAfter, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findQuotaEntry(ledgerAfter, quotaEntryID(manager.repoID, key, staleCommit)); found {
		t.Fatal("expected stale non-winner row removed exactly")
	}
	if _, found := findQuotaEntry(ledgerAfter, quotaEntryID(manager.repoID, key, winnerCommit)); !found {
		t.Fatal("expected winner row to remain charged")
	}
}

type blockingCapacityTelemetry struct {
	sample readCacheCapacitySample
	calls  atomic.Uint64
	delay  time.Duration
}

func (telemetry *blockingCapacityTelemetry) Sample(ctx context.Context) (readCacheCapacitySample, error) {
	telemetry.calls.Add(1)
	if telemetry.delay > 0 {
		select {
		case <-time.After(telemetry.delay):
		case <-ctx.Done():
			return readCacheCapacitySample{}, ctx.Err()
		}
	}
	select {
	case <-ctx.Done():
		return readCacheCapacitySample{}, ctx.Err()
	default:
	}
	return telemetry.sample, nil
}

func TestReadCacheCoordinatorAdmissionPathDoesNotInlineTelemetrySampling(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-no-inline-telemetry", 4096)

	telemetry := &blockingCapacityTelemetry{
		sample: readCacheCapacitySample{
			TotalRawBytes:    1024,
			FreeRawBytes:     512,
			RawAmplification: 1,
			Health:           readCacheHealthHealthy,
			Timestamp:        time.Now().UTC(),
		},
		delay: 2 * time.Second,
	}
	manager.capacityController = newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		TelemetryPollInterval:  time.Hour,
		TelemetryMaxAge:        time.Minute,
		StableSamplesForGrowth: 1,
	}, telemetry)
	manager.mu.Lock()
	manager.admissions = true
	manager.capacityDecision = readCacheCapacityDecision{
		EffectiveLogicalBytes: 4096,
		FillAllowed:           true,
		TelemetryState:        readCacheTelemetryStateFresh,
		Health:                readCacheHealthHealthy,
	}
	manager.mu.Unlock()

	tier := manager.tiers[0]
	start := time.Now()
	manager.storeRange(context.Background(), tier, vaultic.ID{24}, 0, bytes.Repeat([]byte("q"), 128))
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("admission path unexpectedly delayed by telemetry sampling: %v", elapsed)
	}
	if telemetry.calls.Load() != 0 {
		t.Fatalf("expected no inline telemetry sample in admission path, got %d calls", telemetry.calls.Load())
	}
}

func TestReadCacheCoordinatorStaleDecisionFailsClosedWithoutInlineSampling(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-stale-snapshot", 4096)

	telemetry := &blockingCapacityTelemetry{}
	manager.capacityController = newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		TelemetryPollInterval:  time.Hour,
		TelemetryMaxAge:        time.Second,
		StableSamplesForGrowth: 1,
	}, telemetry)
	manager.mu.Lock()
	manager.admissions = true
	manager.capacityDecision = readCacheCapacityDecision{
		EffectiveLogicalBytes: 1024,
		FillAllowed:           false,
		TelemetryState:        readCacheTelemetryStateStale,
		Health:                readCacheHealthHealthy,
		TelemetryAgeMS:        10_000,
	}
	manager.mu.Unlock()

	lease, ok := manager.reserveAdmission(context.Background(), manager.tiers[0], "stale-key", 128)
	if ok || lease != nil {
		t.Fatal("expected stale capacity decision to fail closed")
	}
	if telemetry.calls.Load() != 0 {
		t.Fatalf("expected stale fail-close without inline sampling, got %d calls", telemetry.calls.Load())
	}
}

func TestReadCacheCoordinatorRejectsStaleAdmissionSnapshotAfterPolicyShrink(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-stale-admission-snapshot", 1024)
	tier := manager.tiers[0]

	snapshot, ok := manager.captureAdmissionSnapshot(tier)
	if !ok {
		t.Fatal("expected admission snapshot")
	}

	shrunk := uint64(64)
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: tier.id, MaxBytes: &shrunk}); err != nil {
		t.Fatalf("update policy shrink: %v", err)
	}

	lease, reserved, stale := manager.reserveAdmissionWithSnapshot(
		context.Background(), snapshot, readCacheIdentity{}, "paused-stale-key", 128,
	)
	if reserved || lease != nil {
		t.Fatal("stale pre-shrink snapshot must not admit")
	}
	if !stale {
		t.Fatal("expected stale snapshot rejection after persisted policy update")
	}

	if lease, ok := manager.reserveAdmission(context.Background(), tier, "resume-fresh-key", 128); ok || lease != nil {
		t.Fatal("fresh post-shrink admission should fail at current tier limit")
	}

	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range ledger.Entries {
		if entry.Key == "paused-stale-key" || entry.Key == "resume-fresh-key" {
			t.Fatalf("unexpected quota admission for blocked key %q", entry.Key)
		}
	}
}

func TestReadCacheLogicalReadsRemainStableDuringConcurrentPolicyUpdates(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	manager := newTestReadCacheManager(t, shared, mem.New(), "manager-logical-read-race", 4096)
	tier := manager.tiers[0]
	manager.mu.Lock()
	manager.logicalAccess = map[string]uint32{}
	manager.mu.Unlock()

	firstID := vaultic.ID{0xA1}
	secondID := vaultic.ID{0xB2}
	first := bytes.Repeat([]byte("a"), 96)
	second := bytes.Repeat([]byte("b"), 96)
	content := []vaultic.ID{firstID, secondID}
	cumSize := []uint64{0, uint64(len(first)), uint64(len(first) + len(second))}
	full := append(append([]byte(nil), first...), second...)
	wantOffset := uint64(72)
	wantLen := 80
	want := append([]byte(nil), full[wantOffset:wantOffset+uint64(wantLen)]...)

	loadBlob := func(_ context.Context, _ int, id vaultic.ID) ([]byte, error) {
		switch id {
		case firstID:
			return append([]byte(nil), first...), nil
		case secondID:
			return append([]byte(nil), second...), nil
		default:
			return nil, errors.New("unexpected logical blob")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errCh := make(chan error, 8)
	var wg sync.WaitGroup

	reader := func() {
		defer wg.Done()
		for i := 0; i < 192; i++ {
			buf := make([]byte, wantLen)
			n, err := manager.readLogicalFileRange(ctx, content, cumSize, wantOffset, buf, loadBlob)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errCh <- err
				return
			}
			if n != wantLen {
				errCh <- errors.New("unexpected logical read length")
				return
			}
			if !bytes.Equal(buf[:n], want) {
				errCh <- errors.New("unexpected logical read payload")
				return
			}
		}
	}

	wg.Add(3)
	go reader()
	go reader()
	go func() {
		defer wg.Done()
		for i := 0; i < 192; i++ {
			chunkBytes := uint64(32 + (i%4)*32)
			maxBytes := uint64(384 + (i%5)*128)
			enabled := i%7 != 0
			update := readCachePolicyUpdate{
				ID: tier.id, ChunkBytes: &chunkBytes, MaxBytes: &maxBytes, Enabled: &enabled,
			}
			if err := manager.updatePolicy(ctx, update); err != nil && ctx.Err() == nil {
				// Policy CAS contention is expected under concurrent logical reads.
				continue
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadCacheCoordinatorCapacityWorkerStopsCleanlyAndBoundsOnePoll(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "manager-capacity-worker-stop", 4096)

	telemetry := &blockingCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:    1024,
		FreeRawBytes:     512,
		RawAmplification: 1,
		Health:           readCacheHealthHealthy,
		Timestamp:        time.Now().UTC(),
	}, delay: 5 * time.Second}
	manager.capacityController = newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		TelemetryPollInterval:  time.Hour,
		TelemetryMaxAge:        time.Second,
		StableSamplesForGrowth: 1,
		FallbackLogicalBytes:   1,
	}, telemetry)

	manager.startCapacityWorker()
	deadline := time.Now().Add(time.Second)
	for telemetry.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if telemetry.calls.Load() == 0 {
		manager.stopCapacityWorker()
		t.Fatal("expected worker to attempt one telemetry poll")
	}

	stopStarted := time.Now()
	manager.stopCapacityWorker()
	if elapsed := time.Since(stopStarted); elapsed > time.Second {
		t.Fatalf("expected clean worker shutdown within bounded cancellation, took %v", elapsed)
	}
	if telemetry.calls.Load() != 1 {
		t.Fatalf("expected a bounded single poll attempt, got %d", telemetry.calls.Load())
	}
}

func commitOrderFromKeyID(ledger *readCacheQuotaLedger, key string) uint64 {
	if ledger == nil {
		return 0
	}
	for _, entry := range ledger.Entries {
		if entry.Key == key {
			return quotaEntryOrder(entry)
		}
	}
	return 0
}

func TestReadCacheNonConditionalOriginUsesCacheCoordinator(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	repo.be = readCacheNonConditionalBackend{Backend: repo.be}
	cache := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cache)

	payload, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil {
		t.Fatalf("origin read should succeed despite coordination outage: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("origin read returned empty payload")
	}
	status := repo.ReadCacheStatus()
	if !status.Enabled || !status.AdmissionsEnabled {
		t.Fatal("cache CAS coordinator should allow admissions when origin lacks CAS")
	}
}
