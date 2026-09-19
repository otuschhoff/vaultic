package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestReadCacheDoesNotServeDecodedRepresentationWhenAuthoritativeReadsAreDisabled(t *testing.T) {
	repo, _, _, packID, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	first, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 {
		t.Fatal("first read returned no data")
	}
	cfg = repo.Config()
	disableReads := false
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "single", Role: PlacementRolePrimary, FailureDomain: "primary", ReadEnabled: &disableReads},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)

	got, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err == nil {
		t.Fatalf("expected authoritative eligibility failure with primary reads disabled for pack %s", packID)
	}
	if len(got) != 0 {
		t.Fatal("expected no cached bytes when authoritative reads are disabled")
	}
}

func TestReadCacheEffectiveBudgetIsOverflowSafe(t *testing.T) {
	const smallTier = uint64(8 << 40)
	limit := uint64(12 << 40)
	largeTier := uint64(math.MaxUint64 - smallTier)
	manager := &readCacheManager{
		tiers: []*readCacheTier{
			{requested: largeTier},
			{requested: smallTier},
		},
		requestedAggregate: math.MaxUint64,
	}

	manager.applyEffectiveBudget(limit)
	wantFirst := mulDivFloorUint64(limit, largeTier, math.MaxUint64)
	if manager.tiers[0].maxBytes != wantFirst {
		t.Fatalf("large tier budget = %d, want %d", manager.tiers[0].maxBytes, wantFirst)
	}
	total := manager.tiers[0].maxBytes + manager.tiers[1].maxBytes
	if total != limit || manager.aggregateMax != limit {
		t.Fatalf("allocated total = %d, aggregate = %d, limit = %d", total, manager.aggregateMax, limit)
	}
	if manager.tiers[0].maxBytes > largeTier || manager.tiers[1].maxBytes > smallTier || total > limit {
		t.Fatalf("allocation exceeded tier or aggregate bound: large=%d small=%d", manager.tiers[0].maxBytes, manager.tiers[1].maxBytes)
	}
	if got := mulDivFloorUint64(math.MaxUint64, math.MaxUint64, math.MaxUint64); got != math.MaxUint64 {
		t.Fatalf("MaxUint64 mul/div = %d", got)
	}
}

func TestReadCacheManagerSelectsSortedCacheCoordinator(t *testing.T) {
	repo := TestRepository(t)
	cacheA := mem.New()
	cacheZ := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary},
		{ID: "z-cache", Role: PlacementRoleReadCache, CapacityBytes: 4096, TargetPackSizeBytes: 128},
		{ID: "a-cache", Role: PlacementRoleReadCache, CapacityBytes: 4096, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("z-cache"), cacheZ)
	repo.AttachPlacementBackend(PlacementBackendHash("a-cache"), cacheA)

	manager := repo.readCacheManager()
	if manager == nil || manager.coordinatorBackend != cacheA {
		t.Fatal("expected lexicographically first CAS-capable cache tier to coordinate")
	}
}

func TestReadCacheAggregateCapacityCapsMultipleTiers(t *testing.T) {
	repo := TestRepository(t)
	cfg := repo.Config()
	cfg.ReadCacheAggregateCapacityBytes = 6000
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary},
		{ID: "cache-a", Role: PlacementRoleReadCache, CapacityBytes: 5000, TargetPackSizeBytes: 128},
		{ID: "cache-b", Role: PlacementRoleReadCache, CapacityBytes: 5000, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("cache-a"), mem.New())
	repo.AttachPlacementBackend(PlacementBackendHash("cache-b"), mem.New())

	status := repo.ReadCacheStatus()
	if status.AggregateMaxBytes != 6000 || status.RequestedMaxBytes != 6000 {
		t.Fatalf("aggregate capacity = effective %d requested %d, want 6000", status.AggregateMaxBytes, status.RequestedMaxBytes)
	}
	repo.readCache.mu.Lock()
	capacityWorkerRunning := repo.readCache.capacityStop != nil
	repo.readCache.mu.Unlock()
	if capacityWorkerRunning {
		t.Fatal("fixed capacity manager started a dynamic capacity worker")
	}
	if len(status.Tiers) != 2 || status.Tiers[0].MaxBytes != 5000 || status.Tiers[1].MaxBytes != 5000 {
		t.Fatalf("per-tier capacities changed by aggregate cap: %+v", status.Tiers)
	}
}

func TestReadCacheRepositoriesShareCacheDomainBudget(t *testing.T) {
	left := TestRepository(t)
	right := TestRepository(t)
	shared := mem.New()
	configure := func(repo *Repository, id string) *readCacheManager {
		cfg := repo.Config()
		cfg.ID = id
		cfg.PlacementBackends = []vaultic.PlacementBackend{
			{ID: "primary", Role: PlacementRolePrimary},
			{ID: "rc", Role: PlacementRoleReadCache, CapacityBytes: 900, TargetPackSizeBytes: 128},
		}
		repo.setConfig(cfg)
		repo.AttachPlacementBackend(PlacementBackendHash("rc"), shared)
		return repo.readCacheManager()
	}
	leftManager := configure(left, "repository-left")
	rightManager := configure(right, "repository-right")
	if leftManager == nil || rightManager == nil || leftManager.coordinatorBackend != shared || rightManager.coordinatorBackend != shared {
		t.Fatal("repositories did not select the shared cache as coordinator")
	}

	chunk := bytes.Repeat([]byte("s"), 220)
	var wg sync.WaitGroup
	for i, manager := range []*readCacheManager{leftManager, rightManager, leftManager, rightManager} {
		wg.Add(1)
		go func(index int, current *readCacheManager) {
			defer wg.Done()
			current.storeRange(context.Background(), current.tiers[0], vaultic.ID{byte(index + 1)}, 0, chunk)
		}(i, manager)
	}
	wg.Wait()

	_, ledger, err := leftManager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _, used, reserved := quotaUsage(ledger)
	if used+reserved > 900 {
		t.Fatalf("repository-level shared cache over-admitted: used=%d reserved=%d max=%d", used, reserved, 900)
	}
}

func TestReadCacheWithoutCASFallsBackToOrigin(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cache := readCacheNonConditionalBackend{Backend: mem.New()}
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary},
		{ID: "rc", Role: PlacementRoleReadCache, CapacityBytes: 4096, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cache)

	payload, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil || len(payload) == 0 {
		t.Fatalf("origin fallback failed without cache CAS: bytes=%d err=%v", len(payload), err)
	}
	if repo.ReadCacheStatus().AdmissionsEnabled {
		t.Fatal("cache without CAS must disable admissions")
	}
}

func TestReadCacheDrainReportsIncompleteDeletion(t *testing.T) {
	manager := newTestReadCacheManager(t, newReadCacheFaultBackend(mem.New()), newReadCacheFaultBackend(mem.New()), "drain", 4096)
	tier := manager.tiers[0]
	manager.storeRange(context.Background(), tier, vaultic.ID{0x71}, 0, bytes.Repeat([]byte("d"), 64))
	fault := tier.backend.(*readCacheFaultBackend)
	fault.failRemove.Store(true)
	repo := &Repository{readCache: manager}
	if err := repo.DrainReadCache(context.Background()); err == nil {
		t.Fatal("drain succeeded despite remove outage")
	}
	fault.failRemove.Store(false)
	if err := repo.DrainReadCache(context.Background()); err != nil {
		t.Fatalf("drain did not recover after remove outage: %v", err)
	}
}

type readCachePendingReclamationBackend struct {
	backend.Backend
	pending atomic.Bool
}

func (cache *readCachePendingReclamationBackend) ReclamationPending(context.Context, backend.Handle) (bool, error) {
	return cache.pending.Load(), nil
}

func TestReadCacheDeletionStaysChargedUntilPhysicalReclamation(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := &readCachePendingReclamationBackend{Backend: mem.New()}
	manager := newTestReadCacheManager(t, shared, cache, "reclamation", 4096)
	tier := manager.tiers[0]
	payload := bytes.Repeat([]byte("r"), 64)
	identity := manager.sourceRangeIdentity(tier, vaultic.ID{0x72}, 0, len(payload))
	key := tier.entryKey(identity)
	manager.storeRange(context.Background(), tier, vaultic.ID{0x72}, 0, payload)
	cache.pending.Store(true)
	tier.retireEntry(context.Background(), key)
	status := readCacheStatusForManager(manager)
	if status.PendingDeleteBytes == 0 || len(tier.deleting) != 1 {
		t.Fatalf("physical reclamation was released early: %#v", status)
	}
	_, ledger, err := manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].State != readCacheStateDeleting {
		t.Fatalf("quota did not remain deleting: %#v", ledger.Entries)
	}
	cache.pending.Store(false)
	manager.retryPendingDeletions(context.Background())
	_, ledger, err = manager.loadQuota(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Entries) != 0 || len(tier.deleting) != 0 {
		t.Fatalf("reclaimed deletion remained charged: %#v", ledger.Entries)
	}
}

func readCacheStatusForManager(manager *readCacheManager) readCacheStatus {
	status := readCacheStatus{}
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		status.PendingDeleteBytes += tier.pendingDelete
		tier.mu.Unlock()
	}
	return status
}

func TestReadCachePriorityOrderingUsesStableIDTieBreak(t *testing.T) {
	manager := &readCacheManager{tiers: []*readCacheTier{
		{id: "z", readPrio: 1, admitPrio: 3},
		{id: "a", readPrio: 1, admitPrio: 2},
		{id: "m", readPrio: 2, admitPrio: 1},
	}}
	readOrder := manager.sortedTiersByReadPriority()
	admitOrder := manager.sortedTiersByAdmissionPriority()
	if readOrder[0].id != "a" || readOrder[1].id != "z" || readOrder[2].id != "m" {
		t.Fatalf("unexpected read priority order: %s %s %s", readOrder[0].id, readOrder[1].id, readOrder[2].id)
	}
	if admitOrder[0].id != "m" || admitOrder[1].id != "a" || admitOrder[2].id != "z" {
		t.Fatalf("unexpected admission priority order: %s %s %s", admitOrder[0].id, admitOrder[1].id, admitOrder[2].id)
	}
	if manager.tiers[0].id != "z" {
		t.Fatal("priority traversal mutated shared tier order")
	}
}

func TestReadCacheIdleAndAbsoluteAgeExpireEntries(t *testing.T) {
	for _, test := range []struct {
		name string
		age  func(*readCacheTier, *readCacheEntry)
	}{
		{name: "idle", age: func(tier *readCacheTier, entry *readCacheEntry) {
			tier.idleAge = time.Second
			entry.LastAccess = time.Now().Add(-2 * time.Second)
		}},
		{name: "absolute", age: func(tier *readCacheTier, entry *readCacheEntry) {
			tier.absAge = time.Second
			entry.CreatedAt = time.Now().Add(-2 * time.Second)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestReadCacheManager(t, mem.New(), mem.New(), "age-"+test.name, 4096)
			tier := manager.tiers[0]
			payload := bytes.Repeat([]byte("a"), 64)
			identity := manager.sourceRangeIdentity(tier, vaultic.ID{0x73}, 0, len(payload))
			key := tier.entryKey(identity)
			manager.storeRange(context.Background(), tier, vaultic.ID{0x73}, 0, payload)
			tier.mu.Lock()
			entry := tier.entries[key]
			test.age(tier, entry)
			tier.mu.Unlock()
			if pinned := tier.pinEntry(context.Background(), key); pinned != nil {
				tier.unpinEntry(t.Context(), pinned)
				t.Fatal("expired entry remained readable")
			}
		})
	}
}

func TestDeferredUploadPlanRejectsReadCacheWhenConfigValidationIsBypassed(t *testing.T) {
	repo := TestRepository(t)
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary},
		{ID: "disposable", Role: PlacementRoleReadCache},
	}
	cfg.StagingBackends = []string{"disposable"}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("disposable"), mem.New())

	options, store, err := repo.DeferredUploadPlan()
	if err == nil || !strings.Contains(err.Error(), "disposable read-cache role") {
		t.Fatalf("expected defensive read-cache staging rejection, got %v", err)
	}
	if len(options.Backends) != 0 || len(store.Mirrors) != 0 {
		t.Fatalf("read-cache leaked into deferred upload plan: options=%#v mirrors=%#v", options.Backends, store.Mirrors)
	}
}

func TestReadCacheCorruptionFallsBackToAuthoritative(t *testing.T) {
	repo, _, store, packID, blobID := promotionTestRepository(t)
	cacheBackend := newDynamicReadBackend(mem.New(), true)
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	authorizeReadCacheBlob(t, store, packID)

	want, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := repo.ReadCacheStatus()
	entry, _, err := findCachedRepresentation(repo, readCacheRepDecodedExtent)
	if err != nil {
		t.Fatal(err)
	}
	corruptHandle := entry.Data
	if corruptHandle.Name == "" {
		t.Fatal("no decoded representation was available to corrupt")
	}
	buf, err := loadAll(cacheBackend, corruptHandle)
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) == 0 {
		t.Fatal("decoded representation payload was unexpectedly empty")
	}
	buf[0] ^= 0xff
	if err := cacheBackend.Remove(context.Background(), corruptHandle); err != nil {
		t.Fatal(err)
	}
	if err := cacheBackend.Save(context.Background(), corruptHandle, backend.NewByteReader(buf, cacheBackend.Hasher())); err != nil {
		t.Fatal(err)
	}
	cacheBackend.setFailSaves(true)
	got, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("fallback read mismatch: got %q want %q", got, want)
	}
	status := repo.ReadCacheStatus()
	if len(status.Tiers) != 1 || status.Tiers[0].Entries >= before.Tiers[0].Entries {
		t.Fatalf("expected corruption-triggered retirement with origin fallback: before=%#v after=%#v", before, status)
	}
}

func TestReadCacheReconcilesPersistentCatalogOnManagerRestart(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	before := repo.ReadCacheStatus()
	if len(before.Tiers) != 1 || before.Tiers[0].Entries == 0 {
		t.Fatalf("expected populated cache before restart: %#v", before)
	}
	repo.readCacheMu.Lock()
	repo.readCache = nil
	repo.readCacheMu.Unlock()
	after := repo.ReadCacheStatus()
	if len(after.Tiers) != 1 || after.Tiers[0].Entries == 0 {
		t.Fatalf("persistent cache entries were not reconciled after restart: %#v", after)
	}
}

func TestReadCacheReservationsBoundConcurrentFills(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	repo.readCacheMu.Lock()
	manager := repo.readCache
	repo.readCacheMu.Unlock()
	if manager == nil || len(manager.tiers) != 1 {
		t.Fatal("read-cache manager unavailable")
	}
	tier := manager.tiers[0]
	tier.mu.Lock()
	tier.maxBytes = 1024
	tier.mu.Unlock()

	chunk := make([]byte, 200)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			packID := vaultic.ID{byte(i + 1)}
			manager.storeRange(context.Background(), tier, packID, 0, chunk)
		}(i)
	}
	wg.Wait()
	status := repo.ReadCacheStatus()
	if len(status.Tiers) != 1 {
		t.Fatalf("expected one tier status, got %#v", status)
	}
	tierStatus := status.Tiers[0]
	if tierStatus.UsedBytes+tierStatus.ReservedBytes > tierStatus.MaxBytes {
		t.Fatalf("cache accounting exceeded max: used=%d reserved=%d max=%d", tierStatus.UsedBytes, tierStatus.ReservedBytes, tierStatus.MaxBytes)
	}
}

func TestReadCacheFillGateCleansUpCompletedState(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	repo.readCacheMu.Lock()
	manager := repo.readCache
	repo.readCacheMu.Unlock()
	if manager == nil {
		t.Fatal("read-cache manager unavailable")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.coalesced) != 0 {
		t.Fatalf("fill gate map leaked state: %d entries", len(manager.coalesced))
	}
}

func TestReadCacheFillUsesIndependentBoundedContext(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := newReadCacheFaultBackend(mem.New())
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if status := repo.ReadCacheStatus(); !status.AdmissionsEnabled {
		t.Fatalf("read-cache initialization failed: %#v", status)
	}
	cacheBackend.hangChunkSave.Store(true)

	requestCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	payload, err := repo.LoadBlob(requestCtx, vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil || len(payload) == 0 {
		t.Fatalf("authoritative read failed during cache write outage: bytes=%d err=%v", len(payload), err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("cache fill consumed the authoritative read deadline: %v", elapsed)
	}
	repo.readCacheMu.Lock()
	manager := repo.readCache
	repo.readCacheMu.Unlock()
	_, ledger, ledgerErr := manager.loadQuota(context.Background())
	if ledgerErr != nil {
		t.Fatal(ledgerErr)
	}
	if len(ledger.Entries) == 0 || ledger.Entries[0].State != readCacheStateAdmitting {
		t.Fatalf("ambiguous timed-out write was released early: %#v", ledger.Entries)
	}
	cacheBackend.hangChunkSave.Store(false)
	manager.enforcePolicyRetirement(context.Background())
	_, ledger, ledgerErr = manager.loadQuota(context.Background())
	if ledgerErr != nil {
		t.Fatal(ledgerErr)
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("ambiguous timed-out write was not reconciled: %#v", ledger.Entries)
	}
}

func TestReadCacheRestartStartsFreshIdleAge(t *testing.T) {
	shared := newReadCacheFaultBackend(mem.New())
	cache := mem.New()
	manager := newTestReadCacheManager(t, shared, cache, "idle-restart-seed", 4096)
	tier := manager.tiers[0]
	idleAge := 20 * time.Millisecond
	manager.stopPolicyWorker()
	if err := manager.updatePolicy(context.Background(), readCachePolicyUpdate{ID: tier.id, IdleAge: &idleAge}); err != nil {
		t.Fatal(err)
	}
	packID := vaultic.ID{0x74}
	payload := bytes.Repeat([]byte("i"), 64)
	identity := manager.sourceRangeIdentity(tier, packID, 0, len(payload))
	key := tier.entryKey(identity)
	manager.storeRange(context.Background(), tier, packID, 0, payload)
	time.Sleep(2 * idleAge)
	if err := manager.mutateQuota(context.Background(), func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		for index := range ledger.Managers {
			if ledger.Managers[index].ID == manager.managerID {
				ledger.Managers[index].LeaseExpiryMS = now.Add(-time.Minute).UnixMilli()
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	restarted := newTestReadCacheManager(t, shared, cache, "idle-restart", 4096)
	entry := restarted.tiers[0].pinEntry(context.Background(), key)
	if entry == nil {
		t.Fatal("restart treated creation time as idle recency")
	}
	restarted.tiers[0].unpinEntry(t.Context(), entry)
}

func TestReadCachePolicyAndDrain(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	accounting := telemetry.NewProductionAccounting(true)
	repo.accounting = accounting
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	status := repo.ReadCacheStatus()
	if !status.Enabled || len(status.Tiers) != 1 || status.Tiers[0].UsedBytes == 0 {
		t.Fatalf("unexpected cache status: %#v", status)
	}
	disable := false
	if err := repo.UpdateReadCachePolicyContext(context.Background(), readCachePolicyUpdate{ID: "rc", Enabled: &disable}); err != nil {
		t.Fatal(err)
	}
	if err := repo.DrainReadCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	status = repo.ReadCacheStatus()
	if len(status.Tiers) != 1 || status.Tiers[0].Entries != 0 || status.Tiers[0].UsedBytes != 0 {
		t.Fatalf("drain status mismatch: %#v", status)
	}
	metrics, _, _, _ := accounting.Snapshot(telemetry.MaxMonitorMetrics)
	if phase34M3Metric(metrics, "dependency_requests", "operation", "cache_fill", "role", "coordination", "outcome", "success").Value != 1 {
		t.Fatal("cache policy coordination was not attributed")
	}
	if phase34M3Metric(metrics, "dependency_requests", "operation", "cache_evict", "role", "cache", "outcome", "success").Value != 1 {
		t.Fatal("cache drain was not attributed")
	}
}

func TestReadCachePlaintextTrustRequiresAck(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128,
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: false,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	status := repo.ReadCacheStatus()
	if len(status.Tiers) != 1 {
		t.Fatalf("expected one tier, got %#v", status)
	}
	if status.Tiers[0].Trust != readCacheTrustEncrypted || status.Tiers[0].TrustAck {
		t.Fatalf("plaintext trust without ack must downgrade to encrypted-only: %#v", status.Tiers[0])
	}
}

func TestReadCacheDerivedRepresentationEncryptedOnEncryptedTier(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}

	repo.readCacheMu.Lock()
	manager := repo.readCache
	repo.readCacheMu.Unlock()
	if manager == nil || len(manager.tiers) == 0 {
		t.Fatal("missing read-cache manager")
	}
	tier := manager.tiers[0]
	payload := []byte("derived-representation-payload")
	identity := readCacheIdentity{
		RepositoryID:   manager.repoID,
		Representation: readCacheRepDecodedExtent,
		ContentID:      "blob:derived",
		LayoutID:       "blob-extent",
		Offset:         0,
		Length:         len(payload),
		FormatVersion:  readCacheFormatV2,
		Codec:          "raw",
		Trust:          tier.trust,
		ChunkGeometry:  tier.chunkSize,
		KeyGeneration:  1,
	}
	manager.storeRepresentation(context.Background(), tier, identity, payload)

	key := tier.entryKey(identity)
	entry := tier.lookupEntry(key)
	if entry == nil {
		t.Fatal("expected derived entry")
	}
	metaRaw, err := loadAll(tier.backend, entry.MetaHandle)
	if err != nil {
		t.Fatal(err)
	}
	dataRaw, err := loadAll(tier.backend, entry.DataHandle)
	if err != nil {
		t.Fatal(err)
	}
	var meta readCacheChunkMeta
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatal(err)
	}
	if !meta.DataEncrypted {
		t.Fatal("expected encrypted derived payload")
	}
	if bytes.Equal(dataRaw, payload) {
		t.Fatal("encrypted tier must not persist derived payload as plaintext")
	}
}

func TestReadCacheTrustTighteningRetiresPlaintextEntries(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128,
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: true,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	trust := readCacheTrustEncrypted
	ack := false
	if err := repo.UpdateReadCachePolicy(ReadCachePolicyUpdate{ID: "rc", Trust: &trust, TrustAck: &ack}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status := repo.ReadCacheStatus()
		if len(status.Tiers) == 1 && status.Tiers[0].Trust == readCacheTrustEncrypted && status.Tiers[0].Entries == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	status := repo.ReadCacheStatus()
	t.Fatalf("expected tightened trust to retire plaintext namespace entries: %#v", status)
}

func TestReadCacheChunkBoundaryAndFinalShortRead(t *testing.T) {
	repo, _, _, packID, blobID := promotionTestRepository(t)
	primary := repo.be
	meter := &phase30OriginMeter{Backend: primary, cas: backend.AsCapability[backend.ConditionalWriter](primary)}
	repo.be = meter
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 4},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.ClearReadCache(context.Background(), "rc"); err != nil {
		t.Fatal(err)
	}
	h := backend.Handle{Type: backend.PackFile, Name: packID.String()}
	full, err := loadAll(repo.be, h)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) < 8 {
		t.Fatalf("pack unexpectedly too small: %d", len(full))
	}
	one := make([]byte, 1)
	beforeRequests, beforeBytes := meter.snapshot()
	if _, err := repo.readPackAtFromPlacements(context.Background(), h, 4, one); err != nil {
		t.Fatal(err)
	}
	if one[0] != full[4] {
		t.Fatalf("boundary one-byte read mismatch: got=%x want=%x", one[0], full[4])
	}
	fillRequests, fillBytes := meter.snapshot()
	if fillRequests <= beforeRequests || fillBytes <= beforeBytes {
		t.Fatal("unaligned cold read did not fetch an aligned origin range")
	}
	if _, err := repo.readPackAtFromPlacements(context.Background(), h, 4, one); err != nil {
		t.Fatal(err)
	}
	afterRequests, afterBytes := meter.snapshot()
	if afterRequests != fillRequests || afterBytes != fillBytes {
		t.Fatalf("repeated unaligned read reached origin: requests %d -> %d, bytes %d -> %d", fillRequests, afterRequests, fillBytes, afterBytes)
	}
	last := make([]byte, 1)
	if _, err := repo.readPackAtFromPlacements(context.Background(), h, int64(len(full)-1), last); err != nil {
		t.Fatal(err)
	}
	if last[0] != full[len(full)-1] {
		t.Fatalf("final short read mismatch: got=%x want=%x", last[0], full[len(full)-1])
	}
}

func TestReadCacheDegradedChunkFallsBackToOrigin(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 16 * 1024 * 1024, TargetPackSizeBytes: 4},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}

	var removed bool
	err := cacheBackend.List(context.Background(), backend.StagingFile, func(info backend.FileInfo) error {
		if removed || !strings.HasSuffix(info.Name, ".bin") {
			return nil
		}
		removed = true
		return cacheBackend.Remove(context.Background(), backend.Handle{Type: backend.StagingFile, Name: info.Name})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("expected at least one cached chunk data object")
	}

	got, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("degraded chunk fallback produced different bytes")
	}
	status := repo.ReadCacheStatus()
	if status.UnavailableChunks == 0 || len(status.Tiers) == 0 || status.Tiers[0].Entries == 0 {
		t.Fatalf("expected mixed chunk availability accounting after degradation: %#v", status)
	}
}

func TestReadCacheTransientProbeFailureDoesNotRetireEntryAndRecovers(t *testing.T) {
	manager := newTestReadCacheManager(t, mem.New(), newReadCacheFaultBackend(mem.New()), "manager-transient-probe", 2048)
	tier := manager.tiers[0]
	packID := vaultic.ID{7}
	want := bytes.Repeat([]byte("z"), 96)
	manager.storeRange(context.Background(), tier, packID, 0, want)

	cacheFault, ok := tier.backend.(*readCacheFaultBackend)
	if !ok {
		t.Fatal("expected fault backend")
	}
	cacheFault.failStagingLoadOnce.Store(true)

	if got, loaded := manager.loadChunk(context.Background(), tier, packID, 0, len(want)); loaded || len(got) != 0 {
		t.Fatal("expected transient probe miss without cache retirement")
	}
	entryKey := tier.entryKey(manager.sourceRangeIdentity(tier, packID, 0, len(want)))
	if entry := tier.lookupEntry(entryKey); entry == nil {
		t.Fatal("transient load failure must not retire cache entry")
	}
	tier.mu.Lock()
	delete(tier.negativeUntil, entryKey)
	tier.circuitUntil = time.Time{}
	tier.mu.Unlock()

	got, loaded := manager.loadChunk(context.Background(), tier, packID, 0, len(want))
	if !loaded {
		t.Fatal("expected cache hit after transient outage recovery")
	}
	if !bytes.Equal(got, want) {
		t.Fatal("recovered cache payload mismatch")
	}
}

func TestReadCacheTierScopedCircuitBoundsProbeStormAndPreservesDeadline(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheFault := newReadCacheFaultBackend(mem.New())
	allowReads := true
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "single", Role: PlacementRolePrimary, FailureDomain: "primary", ReadEnabled: &allowReads},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 16 * 1024 * 1024, TargetPackSizeBytes: 1},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheFault)

	want, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 {
		t.Fatal("expected non-empty blob payload")
	}

	cacheFault.stagingLoadCalls.Store(0)
	cacheFault.failStagingTimeout.Store(true)
	defer cacheFault.failStagingTimeout.Store(false)

	requestCtx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	started := time.Now()
	got, err := repo.LoadBlob(requestCtx, vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("expected origin fallback before deadline under probe storm: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("origin fallback payload mismatch under probe storm")
	}
	if elapsed >= 900*time.Millisecond {
		t.Fatalf("probe storm should stop before origin deadline: elapsed=%v", elapsed)
	}
	probes := cacheFault.stagingLoadCalls.Load()
	if probes > readCacheCircuitTrips+1 {
		t.Fatalf("tier-scoped circuit should bound unique-key probes, got %d", probes)
	}
}

func TestReadCacheCircuitAndNegativeProbeState(t *testing.T) {
	manager := newTestReadCacheManager(t, mem.New(), mem.New(), "manager-circuit", 2048)
	tier := manager.tiers[0]
	key := "probe-key"
	for i := 0; i < readCacheCircuitTrips; i++ {
		tier.recordProbeFailure(key)
	}
	status := (&Repository{readCache: manager}).ReadCacheStatus()
	if len(status.Tiers) != 1 {
		t.Fatalf("expected one tier: %#v", status)
	}
	tierStatus := status.Tiers[0]
	if tierStatus.NegativeKeys == 0 {
		t.Fatalf("expected negative probe cache entries: %#v", tierStatus)
	}
	if !tierStatus.CircuitOpen {
		t.Fatalf("expected cache probe circuit to open: %#v", tierStatus)
	}
}

type telemetryReadCacheBackend struct {
	backend.Backend
	sample backend.CapacityTelemetrySample
	delay  time.Duration
	calls  atomic.Uint64
}

func (be *telemetryReadCacheBackend) SampleCapacity(ctx context.Context) (backend.CapacityTelemetrySample, error) {
	be.calls.Add(1)
	if be.delay > 0 {
		select {
		case <-time.After(be.delay):
		case <-ctx.Done():
			return backend.CapacityTelemetrySample{}, ctx.Err()
		}
	}
	return be.sample, nil
}

func TestReadCacheCapacityTelemetryAutoWiresFromBackendCapability(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := &telemetryReadCacheBackend{Backend: mem.New(), sample: backend.CapacityTelemetrySample{
		TotalRawBytes:    100 << 30,
		FreeRawBytes:     60 << 30,
		RawAmplification: 1,
		Health:           "healthy",
	}}
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", Location: "rados://cluster/pool",
			ReadCacheBudgetMode: "ceph-free-space", ReadCacheReserveFrac: 0.1, ReadCacheFallbackMax: 1 << 20,
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 128,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	repo.readCache.refreshCapacityBudget(context.Background())
	status := repo.ReadCacheStatus()
	if status.CapacityMode != "ceph-free-space" || status.TelemetryState == readCacheTelemetryStateUnavailable {
		t.Fatalf("expected auto-wired telemetry, got %#v", status)
	}
}

func TestReadCacheTelemetryWorkerDoesNotDelayForegroundOriginRead(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := &telemetryReadCacheBackend{Backend: mem.New(), sample: backend.CapacityTelemetrySample{
		TotalRawBytes:    100 << 30,
		FreeRawBytes:     60 << 30,
		RawAmplification: 1,
		Health:           "healthy",
	}, delay: 2 * time.Second}
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", Location: "rados://cluster/pool",
			ReadCacheBudgetMode: "ceph-free-space", ReadCacheReserveFrac: 0.1, ReadCacheFallbackMax: 1 << 20,
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 128,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	start := time.Now()
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Fatalf("foreground origin read should not block on telemetry sampling, elapsed=%v", elapsed)
	}
}

func TestReadCacheRepositoryCloseStopsCapacityWorker(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := &telemetryReadCacheBackend{Backend: mem.New(), sample: backend.CapacityTelemetrySample{
		TotalRawBytes:    100 << 30,
		FreeRawBytes:     60 << 30,
		RawAmplification: 1,
		Health:           "healthy",
	}, delay: 5 * time.Second}
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", Location: "rados://cluster/pool",
			ReadCacheBudgetMode: "ceph-free-space", ReadCacheReserveFrac: 0.1, ReadCacheFallbackMax: 1 << 20,
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 128,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("repository close should cancel/await capacity worker promptly, elapsed=%v", elapsed)
	}
}

func TestReadCacheSetConfigReplacementStopsPreviousCapacityWorkers(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := &telemetryReadCacheBackend{Backend: mem.New(), sample: backend.CapacityTelemetrySample{
		TotalRawBytes:    100 << 30,
		FreeRawBytes:     60 << 30,
		RawAmplification: 1,
		Health:           "healthy",
	}, delay: 5 * time.Second}
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", Location: "rados://cluster/pool",
			ReadCacheBudgetMode: "ceph-free-space", ReadCacheReserveFrac: 0.1, ReadCacheFallbackMax: 1 << 20,
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 128,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}

	baselineCalls := cacheBackend.calls.Load()
	for i := 0; i < 3; i++ {
		start := time.Now()
		repo.setConfig(repo.Config())
		if elapsed := time.Since(start); elapsed > 1200*time.Millisecond {
			t.Fatalf("setConfig should synchronously stop prior worker without deadlocking, elapsed=%v", elapsed)
		}
		repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
		if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
			t.Fatal(err)
		}
	}

	if delta := cacheBackend.calls.Load() - baselineCalls; delta > 6 {
		t.Fatalf("expected bounded telemetry polls across repeated replacements, delta=%d", delta)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadCacheRepositoryCloseIsIdempotent(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 4 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("second close should remain bounded and idempotent, elapsed=%v", elapsed)
	}
}

func TestReadCacheMixedBlobRepresentationsDoNotServeWhenPrimaryReadIsDisabled(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	allowReads := true
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "single", Role: PlacementRolePrimary, FailureDomain: "primary", ReadEnabled: &allowReads},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 128,
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: true,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}
	reps, err := listCacheRepresentations(cacheBackend)
	if err != nil {
		t.Fatal(err)
	}
	if reps[readCacheRepDecodedExtent] == 0 || reps[readCacheRepCompressedContainer] == 0 {
		t.Fatalf("expected decoded+compressed representations, got %#v", reps)
	}

	disableReads := false
	cfg = repo.Config()
	cfg.PlacementBackends[0].ReadEnabled = &disableReads
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	got, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err == nil {
		t.Fatal("expected authoritative eligibility failure when primary reads are disabled")
	}
	if len(got) != 0 {
		t.Fatal("expected no cached representation bytes when primary reads are disabled")
	}
}

type dynamicReadBackend struct {
	backend.Backend
	authorized atomic.Bool
	failLoads  atomic.Bool
	failSaves  atomic.Bool
}

func newDynamicReadBackend(inner backend.Backend, authorized bool) *dynamicReadBackend {
	wrapper := &dynamicReadBackend{Backend: inner}
	wrapper.authorized.Store(authorized)
	return wrapper
}

func (wrapper *dynamicReadBackend) ReadAuthorizedNow() bool {
	return wrapper.authorized.Load()
}

func (wrapper *dynamicReadBackend) Unwrap() backend.Backend {
	return wrapper.Backend
}

func (wrapper *dynamicReadBackend) Load(ctx context.Context, handle backend.Handle, length int, offset int64, fn func(io.Reader) error) error {
	if wrapper.failLoads.Load() {
		return errors.New("injected authoritative load outage")
	}
	return wrapper.Backend.Load(ctx, handle, length, offset, fn)
}

func (wrapper *dynamicReadBackend) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	if wrapper.failSaves.Load() {
		return errors.New("injected cache save outage")
	}
	return wrapper.Backend.Save(ctx, handle, reader)
}

func (wrapper *dynamicReadBackend) setAuthorized(value bool) {
	wrapper.authorized.Store(value)
}

func (wrapper *dynamicReadBackend) setFailLoads(value bool) {
	wrapper.failLoads.Store(value)
}

func (wrapper *dynamicReadBackend) setFailSaves(value bool) {
	wrapper.failSaves.Store(value)
}

func TestReadCacheRequiresLiveAuthorizedCandidateForAllRepresentations(t *testing.T) {
	repo, _, _, packID, blobID := promotionTestRepository(t)
	primary := newDynamicReadBackend(repo.be, true)
	repo.be = primary
	cacheBackend := mem.New()
	allowReads := true
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "single", Role: PlacementRolePrimary, FailureDomain: "primary", ReadEnabled: &allowReads},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 64,
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: true,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("single"), primary)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}

	blobSize, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID})
	if !found || blobSize < 4 {
		t.Fatalf("blob size unavailable or too small: found=%v size=%d", found, blobSize)
	}
	content := []vaultic.ID{blobID, blobID}
	cumSize := []uint64{0, uint64(blobSize), 2 * uint64(blobSize)}
	buf := make([]byte, 7)
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, uint64(blobSize-3), buf); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, uint64(blobSize-3), buf); err != nil {
		t.Fatal(err)
	}

	primary.setAuthorized(false)

	if got, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err == nil || len(got) != 0 {
		t.Fatal("expected blob read to fail closed when authoritative lease is no longer authorized")
	}
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, uint64(blobSize-3), buf); err == nil {
		t.Fatal("expected logical range read to fail closed when authoritative lease is no longer authorized")
	}
	packHandle := backend.Handle{Type: backend.PackFile, Name: packID.String()}
	if _, err := repo.readPackAtFromPlacements(context.Background(), packHandle, 0, make([]byte, 1)); err == nil {
		t.Fatal("expected source range read to fail closed when authoritative lease is no longer authorized")
	}
}

func TestReadCacheMissingDataRepairsThenServesWarmHit(t *testing.T) {
	repo, _, store, packID, blobID := promotionTestRepository(t)
	primary := newDynamicReadBackend(repo.be, true)
	repo.be = primary
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("primary"), primary)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	authorizeReadCacheBlob(t, store, packID)

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}

	entry, generation, err := findCachedRepresentation(repo, readCacheRepDecodedExtent)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Data.Name == "" || entry.Meta.Name == "" {
		t.Fatal("expected decoded read-cache entry")
	}
	if err := cacheBackend.Remove(context.Background(), entry.Data); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatalf("expected authoritative fallback+repair after missing cache data: %v", err)
	}
	if _, err := cacheBackend.Stat(context.Background(), entry.Data); !cacheBackend.IsNotExist(err) {
		t.Fatalf("expected stale cache data to be retired, got %v", err)
	}
	if _, err := cacheBackend.Stat(context.Background(), entry.Meta); !cacheBackend.IsNotExist(err) {
		t.Fatalf("expected stale cache metadata to be retired, got %v", err)
	}

	primary.setFailLoads(true)
	defer primary.setFailLoads(false)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatalf("expected warm cache hit after ENOENT repair: %v", err)
	}

	_, repairedGeneration, err := findCachedRepresentation(repo, readCacheRepDecodedExtent)
	if err != nil {
		t.Fatal(err)
	}
	if repairedGeneration == generation {
		t.Fatal("expected repaired cache generation to replace stale generation")
	}
}

func TestReadCacheMissingMetaRepairsThenServesWarmHit(t *testing.T) {
	repo, _, store, packID, blobID := promotionTestRepository(t)
	primary := newDynamicReadBackend(repo.be, true)
	repo.be = primary
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache", CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 128},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("primary"), primary)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	authorizeReadCacheBlob(t, store, packID)

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatal(err)
	}

	entry, generation, err := findCachedRepresentation(repo, readCacheRepDecodedExtent)
	if err != nil {
		t.Fatal(err)
	}
	if err := cacheBackend.Remove(context.Background(), entry.Meta); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatalf("expected authoritative fallback+repair after missing cache metadata: %v", err)
	}

	primary.setFailLoads(true)
	defer primary.setFailLoads(false)
	if _, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil); err != nil {
		t.Fatalf("expected warm cache hit after metadata ENOENT repair: %v", err)
	}

	_, repairedGeneration, err := findCachedRepresentation(repo, readCacheRepDecodedExtent)
	if err != nil {
		t.Fatal(err)
	}
	if repairedGeneration == generation {
		t.Fatal("expected repaired cache generation to replace stale generation")
	}
}

func TestReadLogicalFileRangePromotesLogicalRepresentationsAfterRepeatedAccess(t *testing.T) {
	repo, _, store, packID, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 64,
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: true,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)
	placementValue, err := (schema.PlacementRecord{State: schema.PlacementLive, Bytes: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(context.Background(), []daemon.Mutation{{
		Key: schema.PackPlacementKey(schema.ID(packID), PlacementBackendHash("primary")), Value: placementValue,
	}}, nil, false); err != nil {
		t.Fatal(err)
	}

	blobSize, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID})
	if !found || blobSize < 4 {
		t.Fatalf("blob size unavailable or too small: found=%v size=%d", found, blobSize)
	}
	content := []vaultic.ID{blobID, blobID}
	cumSize := []uint64{0, uint64(blobSize), 2 * uint64(blobSize)}

	buf := make([]byte, 7)
	_, err = repo.ReadLogicalFileRange(context.Background(), content, cumSize, uint64(blobSize-3), buf)
	if err != nil {
		t.Fatal(err)
	}
	firstLayouts, err := listLogicalLayoutRepresentations(cacheBackend)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstLayouts) != 0 {
		t.Fatalf("unexpected first-read logical promotions: %#v", firstLayouts)
	}

	_, err = repo.ReadLogicalFileRange(context.Background(), content, cumSize, uint64(blobSize-3), buf)
	if err != nil {
		t.Fatal(err)
	}
	secondLayouts, err := listLogicalLayoutRepresentations(cacheBackend)
	if err != nil {
		t.Fatal(err)
	}
	if secondLayouts[readCacheRepWholeFile] != 0 {
		t.Fatalf("sparse repeated reads unexpectedly promoted the whole file: %#v", secondLayouts)
	}
	if secondLayouts[readCacheRepDecodedExtent] == 0 {
		t.Fatalf("expected repeated sparse reads to promote touched chunks: %#v", secondLayouts)
	}

	full := make([]byte, cumSize[len(cumSize)-1])
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, 0, full); err != nil {
		t.Fatal(err)
	}
	finalLayouts, err := listLogicalLayoutRepresentations(cacheBackend)
	if err != nil {
		t.Fatal(err)
	}
	if finalLayouts[readCacheRepWholeFile] == 0 {
		t.Fatalf("expected whole-file logical representation after meaningful repeated access: %#v", finalLayouts)
	}
}

func TestReadLogicalFileRangeDoesNotServeCachedRepresentationsWhenPrimaryReadDisabled(t *testing.T) {
	repo, _, _, _, blobID := promotionTestRepository(t)
	cacheBackend := mem.New()
	allowReads := true
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "single", Role: PlacementRolePrimary, FailureDomain: "primary", ReadEnabled: &allowReads},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 64,
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: true,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	blobSize, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID})
	if !found || blobSize < 4 {
		t.Fatalf("blob size unavailable or too small: found=%v size=%d", found, blobSize)
	}
	content := []vaultic.ID{blobID, blobID}
	cumSize := []uint64{0, uint64(blobSize), 2 * uint64(blobSize)}
	buf := make([]byte, cumSize[len(cumSize)-1])
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, 0, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, 0, buf); err != nil {
		t.Fatal(err)
	}
	representations, err := listLogicalLayoutRepresentations(cacheBackend)
	if err != nil {
		t.Fatal(err)
	}
	if representations[readCacheRepWholeFile] == 0 {
		t.Fatalf("expected logical cache representation population, got %#v", representations)
	}

	disableReads := false
	cfg = repo.Config()
	cfg.PlacementBackends[0].ReadEnabled = &disableReads
	repo.setConfig(cfg)

	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, 0, buf); err == nil {
		t.Fatal("expected authoritative eligibility failure when primary reads are disabled")
	}
}

func TestReadLogicalFileRangeAuthorizationRequiresAllReferencedBlobs(t *testing.T) {
	repo, _, store, _, firstBlobID := promotionTestRepository(t)

	var secondBlobID vaultic.ID
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		secondBlobID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("second logical range blob"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	firstBlobPacks := repo.LookupBlob(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: firstBlobID})
	secondBlobPacks := repo.LookupBlob(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: secondBlobID})
	if len(firstBlobPacks) == 0 || len(secondBlobPacks) == 0 {
		t.Fatalf("expected blob pack membership, got first=%d second=%d", len(firstBlobPacks), len(secondBlobPacks))
	}
	firstPackID := firstBlobPacks[0].PackID()
	secondPackID := secondBlobPacks[0].PackID()
	if firstPackID == secondPackID {
		t.Fatalf("expected distinct packs for logical range authorization test, got shared pack %s", firstPackID)
	}

	authorizedPrimary := newDynamicReadBackend(repo.be, true)
	secondaryPrimary := newDynamicReadBackend(repo.be, true)
	cacheBackend := mem.New()
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "p1", Role: PlacementRolePrimary, FailureDomain: "primary-a"},
		{ID: "p2", Role: PlacementRolePrimary, FailureDomain: "primary-b"},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: 8 * 1024 * 1024, TargetPackSizeBytes: 64,
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: true,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("p1"), authorizedPrimary)
	repo.AttachPlacementBackend(PlacementBackendHash("p2"), secondaryPrimary)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	placementValue, err := (schema.PlacementRecord{State: schema.PlacementLive, Bytes: 1, RetentionSource: schema.RetentionUnknown}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(context.Background(), []daemon.Mutation{
		{Key: schema.PackPlacementKey(schema.ID(firstPackID), PlacementBackendHash("p1")), Value: placementValue},
		{Key: schema.PackPlacementKey(schema.ID(secondPackID), PlacementBackendHash("p2")), Value: placementValue},
	}, nil, false); err != nil {
		t.Fatal(err)
	}

	firstBlobSize, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: firstBlobID})
	if !found || firstBlobSize < 4 {
		t.Fatalf("first blob size unavailable or too small: found=%v size=%d", found, firstBlobSize)
	}
	secondBlobSize, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: secondBlobID})
	if !found || secondBlobSize == 0 {
		t.Fatalf("second blob size unavailable or empty: found=%v size=%d", found, secondBlobSize)
	}

	content := []vaultic.ID{firstBlobID, secondBlobID}
	cumSize := []uint64{0, uint64(firstBlobSize), uint64(firstBlobSize) + uint64(secondBlobSize)}
	crossOffset := uint64(firstBlobSize - 2)
	crossBuf := make([]byte, 6)
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, crossOffset, crossBuf); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, crossOffset, crossBuf); err != nil {
		t.Fatal(err)
	}

	secondaryPrimary.setAuthorized(false)

	onlyAuthorizedBuf := make([]byte, 4)
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, 0, onlyAuthorizedBuf); err != nil {
		t.Fatalf("range touching only authorized blob should still succeed: %v", err)
	}
	if _, err := repo.ReadLogicalFileRange(context.Background(), content, cumSize, crossOffset, crossBuf); err == nil {
		t.Fatal("expected cross-blob range to fail once one referenced blob has no authorized pack")
	}
}

func TestReadCacheBudgetChecksHandleMaxUint64Boundaries(t *testing.T) {
	manager := &readCacheManager{
		admissions:       true,
		inflightTasks:    1,
		inflightBytes:    math.MaxUint64 - 1,
		maxInflightBytes: math.MaxUint64,
	}
	if manager.beginInflight(2) {
		t.Fatal("expected inflight reservation rejection on overflow-boundary request")
	}

	tier := &readCacheTier{
		id:       "boundary",
		used:     math.MaxUint64 - 2,
		reserved: 1,
		maxBytes: math.MaxUint64,
		entries:  map[string]*readCacheEntry{},
		deleting: map[string]*readCacheEntry{},
	}
	if tier.reserveAndReclaim(context.Background(), 2) {
		t.Fatal("expected reserve-and-reclaim rejection when used+reserved+reserve exceeds max at uint64 boundary")
	}
}

func TestReferencedLogicalBlobIndicesRequestedRange(t *testing.T) {
	content := []vaultic.ID{{1}, {2}, {3}}
	cumSize := []uint64{0, 10, 20, 30}

	onlyFirst := referencedLogicalBlobIndices(content, cumSize, 1, 4)
	if len(onlyFirst) != 1 || onlyFirst[0] != 0 {
		t.Fatalf("expected first blob only, got %#v", onlyFirst)
	}

	crossBoundary := referencedLogicalBlobIndices(content, cumSize, 8, 8)
	if len(crossBoundary) != 2 || crossBoundary[0] != 0 || crossBoundary[1] != 1 {
		t.Fatalf("expected first+second blob indices, got %#v", crossBoundary)
	}
}

func TestReadCacheEvictionOverlapDropsWholeFileBeforeCompressed(t *testing.T) {
	manager := newTestReadCacheManager(t, mem.New(), mem.New(), "manager-overlap", 16*1024)
	tier := manager.tiers[0]
	whole := bytes.Repeat([]byte("w"), 1024)
	compressed := bytes.Repeat([]byte("c"), 128)
	wholeIdentity := readCacheIdentity{
		RepositoryID:   manager.repoID,
		Representation: readCacheRepWholeFile,
		ContentID:      "logical-content",
		LayoutID:       "logical-file:overlap",
		Offset:         0,
		Length:         len(whole),
		FormatVersion:  readCacheFormatV2,
		Codec:          "raw",
		Trust:          tier.trust,
		ChunkGeometry:  tier.chunkSize,
		KeyGeneration:  1,
	}
	compressedIdentity := wholeIdentity
	compressedIdentity.Representation = readCacheRepCompressedContainer
	compressedIdentity.Length = len(compressed)

	manager.storeRepresentation(context.Background(), tier, wholeIdentity, whole)
	manager.storeRepresentation(context.Background(), tier, compressedIdentity, compressed)
	tier.touchEntry(tier.entryKey(compressedIdentity))
	compressedEntry := tier.lookupEntry(tier.entryKey(compressedIdentity))
	if compressedEntry == nil {
		t.Fatal("expected compressed representation entry")
	}

	tier.mu.Lock()
	tier.maxBytes = compressedEntry.Size + compressedEntry.MetaSize + 32
	tier.mu.Unlock()
	tier.shrink(context.Background())

	if tier.lookupEntry(tier.entryKey(wholeIdentity)) != nil {
		t.Fatal("expected whole-file representation to be evicted first")
	}
	if tier.lookupEntry(tier.entryKey(compressedIdentity)) == nil {
		t.Fatal("expected compressed representation to remain")
	}
}

func listCacheRepresentations(be backend.Backend) (map[string]int, error) {
	representations := map[string]int{}
	err := be.List(context.Background(), backend.StagingFile, func(info backend.FileInfo) error {
		if !strings.HasSuffix(info.Name, ".json") {
			return nil
		}
		raw, err := loadAll(be, backend.Handle{Type: backend.StagingFile, Name: info.Name})
		if err != nil {
			return err
		}
		var meta readCacheChunkMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil
		}
		representations[meta.Identity.Representation]++
		return nil
	})
	return representations, err
}

type cachedRepresentationHandles struct {
	Data backend.Handle
	Meta backend.Handle
}

func authorizeReadCacheBlob(t *testing.T, store *daemon.SchemaStore, packID vaultic.ID) {
	t.Helper()
	placementValue, err := (schema.PlacementRecord{State: schema.PlacementLive, Bytes: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(context.Background(), []daemon.Mutation{{
		Key: schema.PackPlacementKey(schema.ID(packID), PlacementBackendHash("primary")), Value: placementValue,
	}}, nil, false); err != nil {
		t.Fatal(err)
	}
}

func findCachedRepresentation(repo *Repository, representation string) (cachedRepresentationHandles, uint64, error) {
	manager := repo.readCacheManager()
	if manager == nil {
		return cachedRepresentationHandles{}, 0, errors.New("read-cache manager is unavailable")
	}
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		for _, entry := range tier.entries {
			if entry.Representation != representation || entry.Retiring || !entry.Published {
				continue
			}
			handles := cachedRepresentationHandles{Data: entry.DataHandle, Meta: entry.MetaHandle}
			generation := entry.Generation
			tier.mu.Unlock()
			return handles, generation, nil
		}
		tier.mu.Unlock()
	}
	return cachedRepresentationHandles{}, 0, fmt.Errorf("no active %s representation", representation)
}

func listLogicalLayoutRepresentations(be backend.Backend) (map[string]int, error) {
	representations := map[string]int{}
	err := be.List(context.Background(), backend.StagingFile, func(info backend.FileInfo) error {
		if !strings.HasSuffix(info.Name, ".json") {
			return nil
		}
		raw, err := loadAll(be, backend.Handle{Type: backend.StagingFile, Name: info.Name})
		if err != nil {
			return err
		}
		var meta readCacheChunkMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil
		}
		if strings.HasPrefix(meta.Identity.LayoutID, "logical-file:") {
			representations[meta.Identity.Representation]++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return representations, nil
}

func loadAll(be backend.Backend, handle backend.Handle) ([]byte, error) {
	var out []byte
	err := be.Load(context.Background(), handle, 0, 0, func(reader io.Reader) error {
		var err error
		out, err = io.ReadAll(reader)
		return err
	})
	return out, err
}
