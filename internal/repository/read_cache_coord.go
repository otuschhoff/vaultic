package repository

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
)

const (
	readCachePolicyFormatV1 = uint32(1)
	readCacheQuotaFormatV1  = uint32(1)
	readCacheControlKeySize = 32

	readCacheCoordinationTimeout = 250 * time.Millisecond
	readCacheCoordinationRetries = 8
	readCacheLeaseDuration       = 60 * time.Second

	readCacheStateAdmitting  = "admitting"
	readCacheStatePublishing = "publishing"
	readCacheStatePublished  = "published"
	readCacheStateActive     = "active"
	readCacheStateDeleting   = "deleting"

	readCacheAdmissionSnapshotRetries = 2
)

type readCachePolicySnapshot struct {
	Format            uint32                `json:"format"`
	Namespace         string                `json:"namespace"`
	RepositoryID      string                `json:"repository_id"`
	ControlKeySHA256  string                `json:"control_key_sha256"`
	Revision          uint64                `json:"revision"`
	AggregateMaxBytes uint64                `json:"aggregate_max_bytes"`
	Tiers             []readCachePolicyTier `json:"tiers"`
	UpdatedUnixMS     int64                 `json:"updated_unix_ms"`
	Signature         string                `json:"signature"`
}

type readCachePolicyTier struct {
	ID                string `json:"id"`
	Generation        uint64 `json:"generation,omitempty"`
	Enabled           bool   `json:"enabled"`
	Trust             string `json:"trust"`
	TrustAck          bool   `json:"trust_ack"`
	Codec             string `json:"codec,omitempty"`
	ReadPriority      uint32 `json:"read_priority"`
	AdmissionPriority uint32 `json:"admission_priority"`
	IdleAgeMS         uint64 `json:"idle_age_ms"`
	AbsoluteAgeMS     uint64 `json:"absolute_age_ms"`
	ChunkSize         int    `json:"chunk_size"`
	MaxBytes          uint64 `json:"max_bytes"`
}

type readCacheQuotaLedger struct {
	Format      uint32                `json:"format"`
	Namespace   string                `json:"namespace"`
	Revision    uint64                `json:"revision"`
	Managers    []readCacheManagerRef `json:"managers"`
	Entries     []readCacheQuotaEntry `json:"entries"`
	UpdatedUnix int64                 `json:"updated_unix_ms"`
	Signature   string                `json:"signature"`
}

type readCacheManagerRef struct {
	ID            string `json:"id"`
	LeaseExpiryMS int64  `json:"lease_expiry_unix_ms"`
}

type readCacheQuotaEntry struct {
	EntryID        string `json:"entry_id"`
	RepositoryID   string `json:"repository_id"`
	PolicyRevision uint64 `json:"policy_revision"`
	KeyGeneration  uint64 `json:"key_generation,omitempty"`
	Key            string `json:"key"`
	TierID         string `json:"tier_id"`
	Generation     uint64 `json:"generation"`
	CommitOrder    uint64 `json:"commit_order,omitempty"`
	DataHandle     string `json:"data_handle,omitempty"`
	MetaHandle     string `json:"meta_handle,omitempty"`
	Bytes          uint64 `json:"bytes"`
	State          string `json:"state"`
	ManagerID      string `json:"manager_id"`
	UpdatedMS      int64  `json:"updated_unix_ms"`
}

type readCacheAdmissionLease struct {
	EntryID       string
	RepositoryID  string
	Key           string
	TierID        string
	Generation    uint64
	CommitOrder   uint64
	Bytes         uint64
	PolicyRev     uint64
	KeyGeneration uint64
	persisted     bool
	committed     bool
}

func (manager *readCacheManager) initializeCoordinator(ctx context.Context) {
	if manager.coordinator == nil || manager.coordinatorBackend == nil || len(manager.key) == 0 {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	controlKey, err := manager.ensureControlKey(ctx)
	if err != nil {
		manager.reconcileTiers(ctx)
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	manager.controlKey = controlKey
	allowBootstrap, err := manager.repositoryCacheNamespaceIsEmpty(ctx)
	if err != nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	policy, err := manager.ensurePolicy(ctx, allowBootstrap)
	if err != nil {
		manager.reconcileTiers(ctx)
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	if err := manager.validatePolicyCompatibility(policy); err != nil {
		manager.reconcileTiers(ctx)
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	manager.applyPolicy(policy)
	manager.reconcileTiers(ctx)
	if err := manager.reconcileQuotaInventory(ctx); err != nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	manager.reconcileAdmissionVisibility(ctx)
}

func (manager *readCacheManager) reconcileTiers(ctx context.Context) {
	for _, tier := range manager.tiers {
		tier.reconcile(ctx, manager)
	}
}

func (manager *readCacheManager) repositoryCacheNamespaceIsEmpty(ctx context.Context) (bool, error) {
	prefix := readCacheNamespace + "/chunks/" + readCacheRepositoryScope(manager.repoID) + "/"
	for _, tier := range manager.tiers {
		empty := true
		err := tier.backend.List(ctx, backend.StagingFile, func(info backend.FileInfo) error {
			if strings.HasPrefix(info.Name, prefix) {
				empty = false
			}
			return nil
		})
		if err != nil {
			return false, err
		}
		if !empty {
			return false, nil
		}
	}
	return true, nil
}

//nolint:funlen,gocognit,gocyclo // Policy CAS validation and fail-closed state transitions are intentionally kept atomic and explicit.
func (manager *readCacheManager) updatePolicy(ctx context.Context, update readCachePolicyUpdate) error {
	if manager.coordinator == nil || manager.coordinatorBackend == nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return fmt.Errorf("read-cache policy coordination unavailable")
	}
	policyHandle := manager.policyHandle()
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	for attempt := 0; attempt < readCacheCoordinationRetries; attempt++ {
		currentRaw, current, err := manager.loadPolicy(ctx)
		if err != nil {
			manager.mu.Lock()
			manager.admissions = false
			manager.mu.Unlock()
			return err
		}
		if current == nil {
			manager.mu.Lock()
			manager.admissions = false
			manager.mu.Unlock()
			return fmt.Errorf("read-cache policy is missing")
		}
		if update.ExpectedRev != nil && current.Revision != *update.ExpectedRev {
			return fmt.Errorf("read-cache policy revision mismatch: expected %d, got %d", *update.ExpectedRev, current.Revision)
		}
		next := *current
		next.Revision = current.Revision + 1
		next.UpdatedUnixMS = time.Now().UTC().UnixMilli()
		matched := false
		for i := range next.Tiers {
			tier := &next.Tiers[i]
			if tier.ID != update.ID {
				continue
			}
			matched = true
			tier.Generation = max(1, tier.Generation)
			priorTrust, priorCodec, priorChunkSize := tier.Trust, tier.Codec, tier.ChunkSize
			if update.Enabled != nil {
				tier.Enabled = *update.Enabled
			}
			if update.MaxBytes != nil {
				tier.MaxBytes = *update.MaxBytes
			}
			if update.ChunkBytes != nil {
				if *update.ChunkBytes == 0 {
					return fmt.Errorf("read-cache tier %q chunk-bytes must be greater than zero", tier.ID)
				}
				if *update.ChunkBytes > uint64(math.MaxInt) {
					return fmt.Errorf("read-cache tier %q chunk-bytes %d exceeds this platform's integer limit", tier.ID, *update.ChunkBytes)
				}
				if *update.ChunkBytes > MaxReadCacheChunkBytes {
					return fmt.Errorf(
						"read-cache tier %q chunk-bytes %d exceeds the practical maximum of %d bytes (1 GiB)",
						tier.ID, *update.ChunkBytes, MaxReadCacheChunkBytes,
					)
				}
				tier.ChunkSize = int(*update.ChunkBytes)
			}
			finalTrust := tier.Trust
			if update.Trust != nil {
				finalTrust = *update.Trust
			}
			finalTrustAck := tier.TrustAck
			if update.TrustAck != nil {
				finalTrustAck = *update.TrustAck
			}
			normalizedTrust, err := normalizePolicyTrustPair(finalTrust, finalTrustAck)
			if err != nil {
				return fmt.Errorf("read-cache tier %q trust policy invalid: %w", tier.ID, err)
			}
			tier.TrustAck = finalTrustAck
			tier.Trust = normalizedTrust
			if update.Codec != nil {
				tier.Codec = strings.TrimSpace(*update.Codec)
				if tier.Codec == "" {
					tier.Codec = "raw"
				}
			}
			if update.IdleAge != nil {
				if *update.IdleAge < 0 {
					return fmt.Errorf("read-cache tier %q idle age must not be negative", tier.ID)
				}
				tier.IdleAgeMS = uint64(*update.IdleAge / time.Millisecond)
			}
			if update.AbsoluteAge != nil {
				if *update.AbsoluteAge < 0 {
					return fmt.Errorf("read-cache tier %q absolute age must not be negative", tier.ID)
				}
				tier.AbsoluteAgeMS = uint64(*update.AbsoluteAge / time.Millisecond)
			}
			if tier.AbsoluteAgeMS != 0 && tier.IdleAgeMS != 0 && tier.AbsoluteAgeMS < tier.IdleAgeMS {
				return fmt.Errorf("read-cache tier %q absolute age must not be shorter than idle age", tier.ID)
			}
			if update.ReadPriority != nil {
				tier.ReadPriority = *update.ReadPriority
			}
			if update.AdmissionPriority != nil {
				tier.AdmissionPriority = *update.AdmissionPriority
			}
			if tier.Trust != priorTrust || tier.Codec != priorCodec || tier.ChunkSize != priorChunkSize {
				tier.Generation++
			}
		}
		if !matched {
			return fmt.Errorf("unknown read-cache tier %q", update.ID)
		}
		next.AggregateMaxBytes = 0
		for _, tier := range next.Tiers {
			next.AggregateMaxBytes = saturatingAddUint64(next.AggregateMaxBytes, tier.MaxBytes)
		}
		next.AggregateMaxBytes = manager.capAggregate(next.AggregateMaxBytes)
		next = manager.signPolicy(next)
		replacement, err := json.Marshal(next)
		if err != nil {
			return err
		}
		currentAfter, swapped, err := manager.coordinator.CompareAndSwap(ctx, policyHandle, currentRaw, replacement)
		if err != nil {
			readback, readErr := manager.readControl(ctx, policyHandle)
			if readErr == nil && bytes.Equal(readback, replacement) {
				manager.applyRequestedPolicyUpdate(update)
				manager.applyPolicy(&next)
				return nil
			}
			manager.mu.Lock()
			manager.admissions = false
			manager.mu.Unlock()
			return err
		}
		if swapped {
			manager.applyRequestedPolicyUpdate(update)
			manager.applyPolicy(&next)
			return nil
		}
		if _, parsedErr := manager.parsePolicy(currentAfter); parsedErr != nil {
			manager.mu.Lock()
			manager.admissions = false
			manager.mu.Unlock()
			return fmt.Errorf("read-cache policy conflict is malformed: %w", parsedErr)
		}
	}
	manager.mu.Lock()
	manager.admissions = false
	manager.mu.Unlock()
	return fmt.Errorf("read-cache policy CAS retries exceeded")
}

func (manager *readCacheManager) applyRequestedPolicyUpdate(update readCachePolicyUpdate) {
	if update.MaxBytes == nil {
		return
	}
	requestedAggregate := uint64(0)
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		if tier.id == update.ID {
			tier.requested = *update.MaxBytes
		}
		requestedAggregate = saturatingAddUint64(requestedAggregate, tier.requested)
		tier.mu.Unlock()
	}
	manager.mu.Lock()
	manager.requestedAggregate = manager.capAggregate(requestedAggregate)
	manager.mu.Unlock()
	if manager.capacityController != nil && manager.capacityController.opts.Mode == readCacheBudgetModeFixed {
		manager.capacityController.mu.Lock()
		manager.capacityController.opts.FixedMaxBytes = requestedAggregate
		manager.capacityController.effectiveLogicalBudget = requestedAggregate
		manager.capacityController.fillAllowed = requestedAggregate > 0
		manager.capacityController.initialized = true
		manager.capacityController.mu.Unlock()
	}
}

func (manager *readCacheManager) capAggregate(value uint64) uint64 {
	if manager.aggregateCeiling > 0 && value > manager.aggregateCeiling {
		return manager.aggregateCeiling
	}
	return value
}

func normalizePolicyTrustPair(rawTrust string, acknowledged bool) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(rawTrust))
	switch normalized {
	case "", readCacheTrustEncrypted:
		return readCacheTrustEncrypted, nil
	case readCacheTrustPlaintext:
		if !acknowledged {
			return "", fmt.Errorf("plaintext-allowed requires trust-ack=true")
		}
		return readCacheTrustPlaintext, nil
	default:
		return "", fmt.Errorf("unsupported trust mode %q", rawTrust)
	}
}

//nolint:nestif // Signed policy bootstrap and concurrent CAS recovery must remain one fail-closed transaction.
func (manager *readCacheManager) ensurePolicy(ctx context.Context, allowBootstrap bool) (*readCachePolicySnapshot, error) {
	policyHandle := manager.policyHandle()
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	for attempt := 0; attempt < readCacheCoordinationRetries; attempt++ {
		_, current, err := manager.loadPolicy(ctx)
		if err != nil {
			return nil, err
		}
		if current == nil {
			if !allowBootstrap {
				return nil, fmt.Errorf("read-cache policy is missing")
			}
			desired := manager.desiredPolicy()
			desired.Revision = 1
			desired.UpdatedUnixMS = time.Now().UTC().UnixMilli()
			desired = manager.signPolicy(desired)
			replacement, err := json.Marshal(desired)
			if err != nil {
				return nil, err
			}
			_, swapped, err := manager.coordinator.CompareAndSwap(ctx, policyHandle, nil, replacement)
			if err != nil {
				readback, readErr := manager.readControl(ctx, policyHandle)
				if readErr == nil && bytes.Equal(readback, replacement) {
					return &desired, nil
				}
				return nil, err
			}
			if swapped {
				return &desired, nil
			}
			continue
		}
		return current, nil
	}
	return nil, fmt.Errorf("read-cache policy CAS retries exceeded")
}

func (manager *readCacheManager) desiredPolicy() readCachePolicySnapshot {
	tiers := make([]readCachePolicyTier, 0, len(manager.tiers))
	var aggregate uint64
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		policyTier := readCachePolicyTier{
			ID:                tier.id,
			Generation:        1,
			Enabled:           tier.enabled,
			Trust:             tier.trust,
			TrustAck:          tier.trustAck,
			Codec:             tier.codec,
			ReadPriority:      tier.readPrio,
			AdmissionPriority: tier.admitPrio,
			IdleAgeMS:         uint64(tier.idleAge / time.Millisecond),
			AbsoluteAgeMS:     uint64(tier.absAge / time.Millisecond),
			ChunkSize:         tier.chunkSize,
			MaxBytes:          tier.maxBytes,
		}
		aggregate = saturatingAddUint64(aggregate, tier.maxBytes)
		tier.mu.Unlock()
		tiers = append(tiers, policyTier)
	}
	sort.SliceStable(tiers, func(i, j int) bool { return tiers[i].ID < tiers[j].ID })
	return readCachePolicySnapshot{
		Format:            readCachePolicyFormatV1,
		Namespace:         readCacheNamespace,
		RepositoryID:      manager.repoID,
		ControlKeySHA256:  readCacheKeyDigest(manager.controlKey),
		AggregateMaxBytes: manager.capAggregate(aggregate),
		Tiers:             tiers,
	}
}

//nolint:gocognit // Policy application atomically updates manager and per-tier admission state.
func (manager *readCacheManager) applyPolicy(policy *readCachePolicySnapshot) {
	if policy == nil {
		return
	}
	byID := map[string]readCachePolicyTier{}
	for _, tier := range policy.Tiers {
		byID[tier.ID] = tier
	}
	policyRetireNeeded := false
	manager.mu.Lock()
	priorAggregate := manager.aggregateMax
	manager.mu.Unlock()
	var aggregate uint64
	fixedCapacity := manager.capacityController != nil && manager.capacityController.opts.Mode == readCacheBudgetModeFixed
	for _, tier := range manager.tiers {
		update, ok := byID[tier.id]
		if !ok {
			continue
		}
		var previousTrust string
		var previousEnabled bool
		var previousMax uint64
		var previousIdle, previousAbsolute time.Duration
		updatedTrust := update.Trust
		tier.mu.Lock()
		previousTrust = tier.trust
		previousEnabled = tier.enabled
		previousMax = tier.maxBytes
		previousIdle = tier.idleAge
		previousAbsolute = tier.absAge
		tier.enabled = update.Enabled
		tier.generation = max(1, update.Generation)
		tier.trust = updatedTrust
		tier.trustAck = update.TrustAck
		tier.codec = update.Codec
		if tier.codec == "" {
			tier.codec = "raw"
		}
		tier.readPrio = update.ReadPriority
		tier.admitPrio = update.AdmissionPriority
		tier.idleAge = time.Duration(update.IdleAgeMS) * time.Millisecond
		tier.absAge = time.Duration(update.AbsoluteAgeMS) * time.Millisecond
		if update.ChunkSize > 0 {
			tier.chunkSize = update.ChunkSize
		}
		tier.maxBytes = update.MaxBytes
		if fixedCapacity {
			tier.requested = update.MaxBytes
		}
		tier.mu.Unlock()
		if previousEnabled && !update.Enabled {
			policyRetireNeeded = true
		}
		if update.MaxBytes < previousMax {
			policyRetireNeeded = true
		}
		if update.IdleAgeMS != 0 && (previousIdle == 0 || time.Duration(update.IdleAgeMS)*time.Millisecond < previousIdle) {
			policyRetireNeeded = true
		}
		if update.AbsoluteAgeMS != 0 && (previousAbsolute == 0 || time.Duration(update.AbsoluteAgeMS)*time.Millisecond < previousAbsolute) {
			policyRetireNeeded = true
		}
		if previousTrust != updatedTrust && previousTrust == readCacheTrustPlaintext && updatedTrust == readCacheTrustEncrypted {
			policyRetireNeeded = true
		}
		aggregate = saturatingAddUint64(aggregate, update.MaxBytes)
	}
	manager.mu.Lock()
	manager.policyRevision = policy.Revision
	manager.aggregateMax = policy.AggregateMaxBytes
	manager.publishedAggregate = policy.AggregateMaxBytes
	if fixedCapacity {
		manager.requestedAggregate = policy.AggregateMaxBytes
	}
	manager.admissions = manager.coordinator != nil && manager.coordinatorBackend != nil && policy.AggregateMaxBytes > 0
	if manager.aggregateMax == 0 {
		manager.maxInflightBytes = 0
	} else {
		manager.maxInflightBytes = max(16*1024*1024, manager.aggregateMax/8)
	}
	manager.mu.Unlock()
	if fixedCapacity {
		manager.capacityController.mu.Lock()
		manager.capacityController.opts.FixedMaxBytes = policy.AggregateMaxBytes
		manager.capacityController.effectiveLogicalBudget = policy.AggregateMaxBytes
		manager.capacityController.fillAllowed = policy.AggregateMaxBytes > 0
		manager.capacityController.initialized = true
		manager.capacityController.mu.Unlock()
	}
	if aggregate < priorAggregate {
		policyRetireNeeded = true
	}
	if policyRetireNeeded {
		manager.schedulePolicyRetirement()
	}
}

func (manager *readCacheManager) syncEffectiveBudgetPolicy(ctx context.Context) {
	manager.mu.Lock()
	currentPublished := manager.publishedAggregate
	targetAggregate := manager.aggregateMax
	manager.mu.Unlock()
	if manager.coordinator == nil || manager.coordinatorBackend == nil || targetAggregate == currentPublished {
		return
	}
	targets := make(map[string]uint64, len(manager.tiers))
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		targets[tier.id] = tier.maxBytes
		tier.mu.Unlock()
	}
	if err := manager.updatePolicyMaxBytes(ctx, targets); err != nil {
		return
	}
	manager.mu.Lock()
	manager.publishedAggregate = manager.aggregateMax
	manager.mu.Unlock()
}

func (manager *readCacheManager) updatePolicyMaxBytes(ctx context.Context, targets map[string]uint64) error {
	if manager.coordinator == nil || manager.coordinatorBackend == nil {
		return fmt.Errorf("read-cache policy coordination unavailable")
	}
	policyHandle := manager.policyHandle()
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	for attempt := 0; attempt < readCacheCoordinationRetries; attempt++ {
		currentRaw, current, err := manager.loadPolicy(ctx)
		if err != nil || current == nil {
			manager.disableAdmissions()
			if err != nil {
				return err
			}
			return fmt.Errorf("read-cache policy is missing")
		}
		next := *current
		next.Tiers = append([]readCachePolicyTier(nil), current.Tiers...)
		matched := make(map[string]struct{}, len(targets))
		for index := range next.Tiers {
			limit, ok := targets[next.Tiers[index].ID]
			if !ok {
				continue
			}
			next.Tiers[index].MaxBytes = limit
			matched[next.Tiers[index].ID] = struct{}{}
		}
		if len(matched) != len(targets) {
			return fmt.Errorf("read-cache dynamic policy contains unknown tier")
		}
		next.Revision++
		next.UpdatedUnixMS = time.Now().UTC().UnixMilli()
		next.AggregateMaxBytes = 0
		for _, tier := range next.Tiers {
			next.AggregateMaxBytes = saturatingAddUint64(next.AggregateMaxBytes, tier.MaxBytes)
		}
		next.AggregateMaxBytes = manager.capAggregate(next.AggregateMaxBytes)
		next = manager.signPolicy(next)
		replacement, err := json.Marshal(next)
		if err != nil {
			return err
		}
		currentAfter, swapped, err := manager.coordinator.CompareAndSwap(ctx, policyHandle, currentRaw, replacement)
		if err != nil {
			readback, readErr := manager.readControl(ctx, policyHandle)
			if readErr == nil && bytes.Equal(readback, replacement) {
				manager.applyPolicy(&next)
				return nil
			}
			manager.disableAdmissions()
			return err
		}
		if swapped {
			manager.applyPolicy(&next)
			return nil
		}
		if _, parsedErr := manager.parsePolicy(currentAfter); parsedErr != nil {
			manager.disableAdmissions()
			return fmt.Errorf("read-cache policy conflict is malformed: %w", parsedErr)
		}
	}
	manager.disableAdmissions()
	return fmt.Errorf("read-cache policy CAS retries exceeded")
}

func (manager *readCacheManager) disableAdmissions() {
	manager.mu.Lock()
	manager.admissions = false
	manager.mu.Unlock()
}

func (manager *readCacheManager) currentPolicyRevision() uint64 {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.policyRevision
}

func (manager *readCacheManager) refreshPolicyForAccess(ctx context.Context) bool {
	if manager == nil || manager.coordinator == nil || manager.coordinatorBackend == nil {
		return false
	}
	refreshCtx, cancel := context.WithTimeout(ctx, readCacheProbeTimeout)
	defer cancel()
	_, policy, err := manager.loadPolicy(refreshCtx)
	if err != nil || policy == nil {
		return false
	}
	manager.mu.Lock()
	currentRevision := manager.policyRevision
	manager.mu.Unlock()
	if policy.Revision < currentRevision {
		return false
	}
	if policy.Revision > currentRevision {
		manager.applyPolicy(policy)
	}
	return true
}

func (manager *readCacheManager) signPolicy(policy readCachePolicySnapshot) readCachePolicySnapshot {
	policy.Signature = ""
	raw, _ := json.Marshal(policy)
	mac := hmac.New(sha256.New, manager.key)
	if _, err := mac.Write(raw); err != nil {
		debug.Log("read-cache policy sign write failed: %v", err)
	}
	policy.Signature = hex.EncodeToString(mac.Sum(nil))
	return policy
}

func (manager *readCacheManager) parsePolicy(raw []byte) (*readCachePolicySnapshot, error) {
	var policy readCachePolicySnapshot
	if err := json.Unmarshal(raw, &policy); err != nil {
		return nil, err
	}
	if policy.Format != readCachePolicyFormatV1 || policy.Namespace != readCacheNamespace || policy.RepositoryID != manager.repoID ||
		policy.ControlKeySHA256 != readCacheKeyDigest(manager.controlKey) {
		return nil, fmt.Errorf("invalid read-cache policy scope")
	}
	sig := policy.Signature
	policy = manager.signPolicy(policy)
	if !hmac.Equal([]byte(sig), []byte(policy.Signature)) {
		return nil, fmt.Errorf("invalid read-cache policy signature")
	}
	for i := range policy.Tiers {
		if policy.Tiers[i].ID == "" || policy.Tiers[i].ChunkSize <= 0 || policy.Tiers[i].Trust == "" {
			return nil, fmt.Errorf("invalid read-cache policy tier")
		}
		if uint64(policy.Tiers[i].ChunkSize) > MaxReadCacheChunkBytes {
			return nil, fmt.Errorf(
				"read-cache policy tier %q chunk size %d exceeds the practical maximum of %d bytes (1 GiB)",
				policy.Tiers[i].ID, policy.Tiers[i].ChunkSize, MaxReadCacheChunkBytes,
			)
		}
		if policy.Tiers[i].Codec == "" {
			policy.Tiers[i].Codec = "raw"
		}
	}
	policy.Signature = sig
	sort.SliceStable(policy.Tiers, func(i, j int) bool { return policy.Tiers[i].ID < policy.Tiers[j].ID })
	return &policy, nil
}

func (manager *readCacheManager) validatePolicyCompatibility(policy *readCachePolicySnapshot) error {
	if policy == nil || len(policy.Tiers) != len(manager.tiers) {
		return fmt.Errorf("read-cache policy tier configuration mismatch")
	}
	for i, tier := range manager.tiers {
		if policy.Tiers[i].ID != tier.id {
			return fmt.Errorf("read-cache policy tier configuration mismatch")
		}
	}
	return nil
}

func (manager *readCacheManager) loadPolicy(ctx context.Context) ([]byte, *readCachePolicySnapshot, error) {
	h := manager.policyHandle()
	raw, err := manager.readControl(ctx, h)
	if err != nil {
		if manager.coordinatorBackend != nil && manager.coordinatorBackend.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	policy, err := manager.parsePolicy(raw)
	if err != nil {
		return nil, nil, err
	}
	secondRaw, secondErr := manager.readControl(ctx, h)
	if secondErr == nil {
		second, parseErr := manager.parsePolicy(secondRaw)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		if second.Revision == policy.Revision && !bytes.Equal(raw, secondRaw) {
			return nil, nil, fmt.Errorf("equivocated read-cache policy revision %d", policy.Revision)
		}
		if second.Revision > policy.Revision {
			return secondRaw, second, nil
		}
	}
	return raw, policy, nil
}

func (manager *readCacheManager) reserveAdmission(ctx context.Context, tier *readCacheTier, key string, bytesReserved uint64) (*readCacheAdmissionLease, bool) {
	for attempt := 0; attempt < readCacheAdmissionSnapshotRetries; attempt++ {
		snapshot, ok := manager.captureAdmissionSnapshot(tier)
		if !ok {
			return nil, false
		}
		lease, reserved, staleSnapshot := manager.reserveAdmissionWithSnapshot(ctx, snapshot, readCacheIdentity{}, key, bytesReserved)
		if reserved || !staleSnapshot {
			return lease, reserved
		}
	}
	return nil, false
}

//nolint:gocyclo,gocognit // Admission checks intentionally fence one policy snapshot and one quota mutation.
func (manager *readCacheManager) reserveAdmissionWithSnapshot(
	ctx context.Context,
	snapshot readCacheAdmissionSnapshot,
	identity readCacheIdentity,
	key string,
	bytesReserved uint64,
) (*readCacheAdmissionLease, bool, bool) {
	if !snapshot.Manager.Admissions || !snapshot.Manager.FillAllowed || !snapshot.Tier.Ingest {
		return nil, false, false
	}
	policy, err := manager.ensurePolicy(ctx, false)
	if err != nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return nil, false, false
	}
	manager.applyPolicy(policy)
	currentTier := manager.tierByID(snapshot.Tier.ID)
	if currentTier == nil {
		return nil, false, false
	}
	currentSnapshot, ok := manager.captureAdmissionSnapshot(currentTier)
	if !ok {
		return nil, false, false
	}
	if currentSnapshot != snapshot {
		return nil, false, true
	}
	snapshot = currentSnapshot
	if !snapshot.Manager.Admissions || !snapshot.Manager.FillAllowed || !snapshot.Tier.Ingest {
		return nil, false, false
	}
	generation := readCacheRandomGeneration()
	lease := &readCacheAdmissionLease{RepositoryID: manager.repoID, Key: key, TierID: snapshot.Tier.ID, Generation: generation, Bytes: bytesReserved}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	committed, ledgerAfter, err := manager.mutateQuotaAdmission(ctx, func(
		ledger *readCacheQuotaLedger, policy *readCachePolicySnapshot, now time.Time,
	) (bool, error) {
		tierPolicy, ok := policyTierByID(policy, snapshot.Tier.ID)
		if !ok || !tierPolicy.Enabled {
			return false, nil
		}
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		usedByTier, reservedByTier, aggregateUsed, aggregateReserved := quotaUsage(ledger)
		if sumExceedsUint64(policy.AggregateMaxBytes, aggregateUsed, aggregateReserved, bytesReserved) {
			return false, nil
		}
		if sumExceedsUint64(tierPolicy.MaxBytes, usedByTier[snapshot.Tier.ID], reservedByTier[snapshot.Tier.ID], bytesReserved) {
			return false, nil
		}
		for _, existing := range ledger.Entries {
			matchesKey := existing.RepositoryID == manager.repoID && existing.Key == key && existing.TierID == snapshot.Tier.ID
			blocksAdmission := existing.State == readCacheStateAdmitting ||
				existing.State == readCacheStatePublishing || quotaEntryIsPublished(existing.State)
			if matchesKey && blocksAdmission {
				return false, nil
			}
		}
		commitOrder := nextQuotaCommitOrder(ledger, manager.repoID, key, snapshot.Tier.ID)
		entryID := quotaEntryID(manager.repoID, key, commitOrder)
		dataHandle, metaHandle := backend.Handle{}, backend.Handle{}
		if identity.RepositoryID != "" {
			dataHandle, metaHandle = currentTier.handlesForIdentity(key, identity, generation, commitOrder)
		}
		ledger.Entries = append(ledger.Entries, readCacheQuotaEntry{
			EntryID:        entryID,
			RepositoryID:   manager.repoID,
			PolicyRevision: policy.Revision,
			KeyGeneration:  max(1, tierPolicy.Generation),
			Key:            key,
			TierID:         snapshot.Tier.ID,
			Generation:     generation,
			CommitOrder:    commitOrder,
			DataHandle:     dataHandle.Name,
			MetaHandle:     metaHandle.Name,
			Bytes:          bytesReserved,
			State:          readCacheStateAdmitting,
			ManagerID:      manager.managerID,
			UpdatedMS:      now.UnixMilli(),
		})
		return true, nil
	})
	if err != nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return nil, false, false
	}
	if !committed || ledgerAfter == nil {
		return nil, false, false
	}
	for _, item := range ledgerAfter.Entries {
		if item.RepositoryID != manager.repoID || item.Key != key || item.TierID != snapshot.Tier.ID ||
			item.Generation != generation || item.State != readCacheStateAdmitting {
			continue
		}
		lease.EntryID = item.EntryID
		lease.CommitOrder = quotaEntryOrder(item)
		lease.PolicyRev = item.PolicyRevision
		lease.KeyGeneration = item.KeyGeneration
		return lease, true, false
	}
	return nil, false, false
}

func (manager *readCacheManager) commitAdmission(ctx context.Context, lease *readCacheAdmissionLease) (bool, error) {
	if lease == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	committed, _, err := manager.mutateQuotaAdmission(ctx, func(ledger *readCacheQuotaLedger, policy *readCachePolicySnapshot, now time.Time) (bool, error) {
		tierPolicy, ok := policyTierByID(policy, lease.TierID)
		if !ok || !tierPolicy.Enabled {
			return false, nil
		}
		if lease.PolicyRev != 0 && policy.Revision != lease.PolicyRev {
			return false, nil
		}
		if lease.KeyGeneration != max(1, tierPolicy.Generation) {
			return false, nil
		}
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		usedByTier, reservedByTier, aggregateUsed, aggregateReserved := quotaUsage(ledger)
		if sumExceedsUint64(policy.AggregateMaxBytes, aggregateUsed, aggregateReserved) {
			return false, nil
		}
		if sumExceedsUint64(tierPolicy.MaxBytes, usedByTier[lease.TierID], reservedByTier[lease.TierID]) {
			return false, nil
		}
		for i := range ledger.Entries {
			entry := &ledger.Entries[i]
			if entry.EntryID == lease.EntryID &&
				entry.RepositoryID == manager.repoID &&
				entry.PolicyRevision == lease.PolicyRev &&
				quotaEntryOrder(*entry) == lease.CommitOrder &&
				entry.Generation == lease.Generation &&
				entry.State == readCacheStateAdmitting {
				entry.State = readCacheStatePublishing
				entry.UpdatedMS = now.UnixMilli()
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return false, err
	}
	if !committed {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return false, fmt.Errorf("read-cache admission commit missing lease entry %s generation %d", lease.EntryID, lease.Generation)
	}
	lease.committed = true
	manager.runAfterCommitCASHook()
	return true, nil
}

func (manager *readCacheManager) releaseAdmission(ctx context.Context, lease *readCacheAdmissionLease) {
	if lease == nil || lease.committed {
		return
	}
	if lease.persisted {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	if err := manager.mutateQuota(ctx, func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		for i := range ledger.Entries {
			entry := ledger.Entries[i]
			if entry.EntryID == lease.EntryID &&
				entry.RepositoryID == manager.repoID &&
				quotaEntryOrder(entry) == lease.CommitOrder &&
				entry.Generation == lease.Generation &&
				entry.State == readCacheStateAdmitting {
				ledger.Entries = append(ledger.Entries[:i], ledger.Entries[i+1:]...)
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		debug.Log("read-cache release admission failed: %v", err)
	}
}

//nolint:gocognit,gocyclo // Visibility fencing deliberately keeps policy, quota, and local entry checks together.
func (manager *readCacheManager) reconcileAdmissionVisibility(ctx context.Context) {
	_, policy, policyErr := manager.loadPolicy(ctx)
	if policyErr != nil || policy == nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	manager.applyPolicy(policy)
	_, ledger, err := manager.loadQuota(ctx)
	if err != nil || ledger == nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return
	}
	usedByTier, reservedByTier, aggregateUsed, aggregateReserved := quotaUsage(ledger)
	aggregateOver := sumExceedsUint64(policy.AggregateMaxBytes, aggregateUsed, aggregateReserved)
	tierAllowed := map[string]bool{}
	tierGeneration := map[string]uint64{}
	for _, tierPolicy := range policy.Tiers {
		tierGeneration[tierPolicy.ID] = max(1, tierPolicy.Generation)
		if aggregateOver {
			continue
		}
		if !tierPolicy.Enabled {
			continue
		}
		if sumExceedsUint64(tierPolicy.MaxBytes, usedByTier[tierPolicy.ID], reservedByTier[tierPolicy.ID]) {
			continue
		}
		tierAllowed[tierPolicy.ID] = true
	}
	active := make(map[string]readCacheQuotaEntry, len(ledger.Entries))
	publishing := make(map[string]readCacheQuotaEntry)
	for _, item := range ledger.Entries {
		if item.RepositoryID != manager.repoID || item.EntryID == "" || item.Generation == 0 || item.CommitOrder == 0 {
			continue
		}
		if item.State == readCacheStatePublishing {
			publishing[quotaEntryID(manager.repoID, item.Key, quotaEntryOrder(item))] = item
			continue
		}
		if !quotaEntryIsPublished(item.State) || quotaEntryKeyGeneration(item) != tierGeneration[item.TierID] {
			continue
		}
		if !tierAllowed[item.TierID] {
			continue
		}
		active[quotaEntryID(manager.repoID, item.Key, quotaEntryOrder(item))] = item
	}

	for _, tier := range manager.tiers {
		retire := make([]*readCacheEntry, 0)
		tier.mu.Lock()
		for _, entry := range tier.entries {
			entryID := quotaEntryID(manager.repoID, entry.Key, readCacheEntryOrder(entry))
			item, ok := active[entryID]
			matchesActive := ok && item.Key == entry.Key && item.TierID == tier.id &&
				item.Generation == entry.Generation && item.CommitOrder == readCacheEntryOrder(entry)
			if matchesActive && entry.Trust == tier.trust && readCacheRepresentationAllowed(entry.Representation) {
				entry.Published = true
				continue
			}
			if item, ok := publishing[entryID]; ok && item.Generation == entry.Generation && item.TierID == tier.id {
				entry.Published = false
				continue
			}
			tier.markRetiringLocked(entry)
			retire = append(retire, entry)
		}
		tier.mu.Unlock()
		for _, entry := range retire {
			tier.deleteRetiringEntry(ctx, entry)
		}
	}
}

func (manager *readCacheManager) finalizeDeletion(ctx context.Context, tierID string, entry *readCacheEntry) {
	if entry == nil || entry.Generation == 0 {
		return
	}
	tier := manager.tierByID(tierID)
	if tier == nil {
		return
	}
	if _, err := tier.backend.Stat(ctx, entry.DataHandle); err == nil {
		return
	} else if !tier.backend.IsNotExist(err) {
		debug.Log("read-cache finalize deletion data stat failed for %s: %v", tier.id, err)
		return
	}
	if _, err := tier.backend.Stat(ctx, entry.MetaHandle); err == nil {
		return
	} else if !tier.backend.IsNotExist(err) {
		debug.Log("read-cache finalize deletion meta stat failed for %s: %v", tier.id, err)
		return
	}
	if pending, err := readCacheReclamationPending(ctx, tier.backend, entry.DataHandle, entry.MetaHandle); err != nil || pending {
		if err != nil {
			debug.Log("read-cache finalize deletion reclamation check failed for %s: %v", tier.id, err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	if err := manager.mutateQuota(ctx, func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		entryID := quotaEntryID(manager.repoID, entry.Key, readCacheEntryOrder(entry))
		for i := range ledger.Entries {
			item := &ledger.Entries[i]
			if item.RepositoryID != manager.repoID || item.EntryID != entryID || item.Key != entry.Key || item.TierID != tierID ||
				item.Generation != entry.Generation || quotaEntryOrder(*item) != readCacheEntryOrder(entry) {
				continue
			}
			if item.State != readCacheStateDeleting {
				item.State = readCacheStateDeleting
				item.UpdatedMS = now.UnixMilli()
			}
			ledger.Entries = append(ledger.Entries[:i], ledger.Entries[i+1:]...)
			return true, nil
		}
		return false, nil
	}); err != nil {
		debug.Log("read-cache finalize deletion quota update failed: %v", err)
	}
}

func (manager *readCacheManager) markDeletionPending(ctx context.Context, tierID string, entry *readCacheEntry) {
	if entry == nil || entry.Generation == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	if err := manager.mutateQuota(ctx, func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		entryID := quotaEntryID(manager.repoID, entry.Key, readCacheEntryOrder(entry))
		for i := range ledger.Entries {
			item := &ledger.Entries[i]
			if item.RepositoryID != manager.repoID || item.EntryID != entryID || item.Generation != entry.Generation ||
				quotaEntryOrder(*item) != readCacheEntryOrder(entry) || item.TierID != tierID {
				continue
			}
			if item.State == readCacheStateDeleting {
				return false, nil
			}
			item.State = readCacheStateDeleting
			item.UpdatedMS = now.UnixMilli()
			return true, nil
		}
		return false, nil
	}); err != nil {
		debug.Log("read-cache mark deletion pending failed: %v", err)
	}
}

//nolint:gocognit,gocyclo // Recovery keeps lease, physical inventory, and ownership transitions in one signed mutation.
func (manager *readCacheManager) reconcileQuotaInventory(ctx context.Context) error {
	observed := make([]readCacheQuotaEntry, 0)
	observedByID := make(map[string]readCacheQuotaEntry)
	deletingByID := make(map[string]struct{})
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		for _, entry := range tier.entries {
			if entry.Retiring {
				continue
			}
			generation := entry.Generation
			item := readCacheQuotaEntry{
				EntryID:        quotaEntryID(manager.repoID, entry.Key, readCacheEntryOrder(entry)),
				RepositoryID:   manager.repoID,
				PolicyRevision: manager.currentPolicyRevision(),
				KeyGeneration:  max(1, entry.KeyGeneration),
				Key:            entry.Key,
				TierID:         tier.id,
				Generation:     generation,
				CommitOrder:    readCacheEntryOrder(entry),
				Bytes:          entry.Size + entry.MetaSize,
				State:          readCacheStatePublished,
				ManagerID:      manager.managerID,
				UpdatedMS:      time.Now().UTC().UnixMilli(),
			}
			observedByID[item.EntryID] = item
			if entry.Published {
				observed = append(observed, item)
			}
		}
		for entryID := range tier.deleting {
			deletingByID[entryID] = struct{}{}
		}
		tier.mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	err := manager.mutateQuota(ctx, func(ledger *readCacheQuotaLedger, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		changed := manager.renewLease(ledger, now)
		liveManagers := make(map[string]struct{}, len(ledger.Managers))
		for _, ref := range ledger.Managers {
			liveManagers[ref.ID] = struct{}{}
		}
		newlyDeleting := make(map[string]struct{})
		for i := range ledger.Entries {
			entry := &ledger.Entries[i]
			if entry.State != readCacheStatePublishing {
				continue
			}
			if _, live := liveManagers[entry.ManagerID]; live {
				continue
			}
			if entry.RepositoryID != manager.repoID {
				if entry.DataHandle != "" && entry.MetaHandle != "" && manager.tierByID(entry.TierID) != nil {
					entry.State = readCacheStateDeleting
					entry.UpdatedMS = now.UnixMilli()
					changed = true
				}
				continue
			}
			item, ok := observedByID[entry.EntryID]
			if ok && item.Generation == entry.Generation && item.TierID == entry.TierID && item.Bytes == entry.Bytes {
				entry.State = readCacheStatePublished
				entry.ManagerID = manager.managerID
			} else {
				entry.State = readCacheStateDeleting
				newlyDeleting[entry.EntryID] = struct{}{}
			}
			entry.UpdatedMS = now.UnixMilli()
			changed = true
		}
		for _, item := range observed {
			if item.EntryID == "" {
				continue
			}
			if _, ok := findQuotaEntry(ledger, item.EntryID); ok {
				continue
			}
			item.UpdatedMS = now.UnixMilli()
			ledger.Entries = append(ledger.Entries, item)
			changed = true
		}
		for i := 0; i < len(ledger.Entries); i++ {
			entry := ledger.Entries[i]
			if entry.RepositoryID == manager.repoID && entry.State == readCacheStateDeleting && entry.DataHandle == "" && entry.MetaHandle == "" {
				_, justTransitioned := newlyDeleting[entry.EntryID]
				_, physicallyDeleting := deletingByID[entry.EntryID]
				_, physicallyObserved := observedByID[entry.EntryID]
				if !justTransitioned && !physicallyDeleting && !physicallyObserved {
					ledger.Entries = append(ledger.Entries[:i], ledger.Entries[i+1:]...)
					i--
					changed = true
					continue
				}
			}
			if entry.RepositoryID != manager.repoID || entry.ManagerID != manager.managerID || entry.State != readCacheStateAdmitting {
				continue
			}
			ledger.Entries = append(ledger.Entries[:i], ledger.Entries[i+1:]...)
			i--
			changed = true
		}
		return changed, nil
	})
	if err != nil {
		return err
	}
	return manager.reclaimForeignDeleting(ctx)
}

//nolint:gocognit // Reclamation checks exact handles and quota state conservatively.
func (manager *readCacheManager) reclaimForeignDeleting(ctx context.Context) error {
	_, ledger, err := manager.loadQuota(ctx)
	if err != nil || ledger == nil {
		return err
	}
	reclaimed := make(map[string]struct{})
	for _, entry := range ledger.Entries {
		if entry.State != readCacheStateDeleting || entry.DataHandle == "" || entry.MetaHandle == "" {
			continue
		}
		tier := manager.tierByID(entry.TierID)
		if tier == nil {
			continue
		}
		handles := []backend.Handle{
			{Type: backend.StagingFile, Name: entry.DataHandle},
			{Type: backend.StagingFile, Name: entry.MetaHandle},
		}
		absent := true
		for _, handle := range handles {
			if removeErr := tier.backend.Remove(ctx, handle); removeErr != nil && !tier.backend.IsNotExist(removeErr) {
				absent = false
				break
			}
			if _, statErr := tier.backend.Stat(ctx, handle); statErr == nil || !tier.backend.IsNotExist(statErr) {
				absent = false
				break
			}
		}
		if absent {
			pending, pendingErr := readCacheReclamationPending(ctx, tier.backend, handles...)
			if pendingErr != nil || pending {
				absent = false
			}
		}
		if absent {
			reclaimed[entry.EntryID] = struct{}{}
		}
	}
	if len(reclaimed) == 0 {
		return nil
	}
	return manager.mutateQuota(ctx, func(ledger *readCacheQuotaLedger, _ time.Time) (bool, error) {
		changed := false
		kept := ledger.Entries[:0]
		for _, entry := range ledger.Entries {
			if entry.State == readCacheStateDeleting {
				if _, ok := reclaimed[entry.EntryID]; ok {
					changed = true
					continue
				}
			}
			kept = append(kept, entry)
		}
		ledger.Entries = kept
		return changed, nil
	})
}

func readCacheReclamationPending(ctx context.Context, cacheBackend backend.Backend, handles ...backend.Handle) (bool, error) {
	status := backend.AsCapability[backend.ReclamationStatus](cacheBackend)
	if status == nil {
		return false, nil
	}
	for _, handle := range handles {
		pending, err := status.ReclamationPending(ctx, handle)
		if err != nil || pending {
			return pending, err
		}
	}
	return false, nil
}

//nolint:gocognit,nestif // Admission CAS retries bind signed policy and quota revisions; splitting risks stale-policy commits.
func (manager *readCacheManager) mutateQuotaAdmission(
	ctx context.Context,
	mutator func(*readCacheQuotaLedger, *readCachePolicySnapshot, time.Time) (bool, error),
) (bool, *readCacheQuotaLedger, error) {
	quotaHandle := manager.quotaHandle()
	for attempt := 0; attempt < readCacheCoordinationRetries; attempt++ {
		_, policy, err := manager.loadPolicy(ctx)
		if err != nil {
			return false, nil, err
		}
		if policy == nil {
			return false, nil, fmt.Errorf("read-cache policy is missing")
		}
		raw, ledger, err := manager.loadQuota(ctx)
		if err != nil {
			return false, nil, err
		}
		now := time.Now().UTC()
		changed, err := mutator(ledger, policy, now)
		if err != nil {
			return false, nil, err
		}
		if !changed {
			return false, nil, nil
		}
		mutationChanged := changed
		_, fencePolicy, err := manager.loadPolicy(ctx)
		if err != nil {
			return false, nil, err
		}
		if fencePolicy == nil {
			return false, nil, fmt.Errorf("read-cache policy is missing")
		}
		if fencePolicy.Revision != policy.Revision {
			continue
		}
		manager.runBeforeQuotaCASHook()
		ledger.Revision++
		ledger.UpdatedUnix = now.UnixMilli()
		*ledger = manager.signQuota(*ledger)
		replacement, err := json.Marshal(ledger)
		if err != nil {
			return false, nil, err
		}
		current, swapped, err := manager.coordinator.CompareAndSwap(ctx, quotaHandle, raw, replacement)
		if err != nil {
			readback, readErr := manager.readControl(ctx, quotaHandle)
			if readErr == nil && bytes.Equal(readback, replacement) {
				if floorErr := manager.noteQuotaRevision(ledger.Revision); floorErr != nil {
					return false, nil, floorErr
				}
				if _, policyAfter, policyErr := manager.loadPolicy(ctx); policyErr != nil || policyAfter == nil {
					if policyErr != nil {
						return false, nil, policyErr
					}
					return false, nil, fmt.Errorf("read-cache policy is missing")
				}
				return mutationChanged, ledger, nil
			}
			return false, nil, err
		}
		if swapped {
			if floorErr := manager.noteQuotaRevision(ledger.Revision); floorErr != nil {
				return false, nil, floorErr
			}
			if _, policyAfter, policyErr := manager.loadPolicy(ctx); policyErr != nil || policyAfter == nil {
				if policyErr != nil {
					return false, nil, policyErr
				}
				return false, nil, fmt.Errorf("read-cache policy is missing")
			}
			return mutationChanged, ledger, nil
		}
		if current != nil {
			parsed, parseErr := manager.parseQuota(current)
			if parseErr != nil {
				return false, nil, fmt.Errorf("read-cache quota conflict is malformed: %w", parseErr)
			}
			if floorErr := manager.noteQuotaRevision(parsed.Revision); floorErr != nil {
				return false, nil, floorErr
			}
		}
	}
	return false, nil, fmt.Errorf("read-cache quota CAS retries exceeded")
}

//nolint:gocognit // Signed quota CAS retries and fail-closed admission state form one atomic coordination path.
func (manager *readCacheManager) mutateQuota(ctx context.Context, mutator func(ledger *readCacheQuotaLedger, now time.Time) (bool, error)) error {
	quotaHandle := manager.quotaHandle()
	for attempt := 0; attempt < readCacheCoordinationRetries; attempt++ {
		raw, ledger, err := manager.loadQuota(ctx)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		changed, err := mutator(ledger, now)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		ledger.Revision++
		ledger.UpdatedUnix = now.UnixMilli()
		*ledger = manager.signQuota(*ledger)
		replacement, err := json.Marshal(ledger)
		if err != nil {
			return err
		}
		current, swapped, err := manager.coordinator.CompareAndSwap(ctx, quotaHandle, raw, replacement)
		if err != nil {
			readback, readErr := manager.readControl(ctx, quotaHandle)
			if readErr == nil && bytes.Equal(readback, replacement) {
				if floorErr := manager.noteQuotaRevision(ledger.Revision); floorErr != nil {
					return floorErr
				}
				return nil
			}
			return err
		}
		if swapped {
			if floorErr := manager.noteQuotaRevision(ledger.Revision); floorErr != nil {
				return floorErr
			}
			return nil
		}
		if current != nil {
			parsed, parseErr := manager.parseQuota(current)
			if parseErr != nil {
				return fmt.Errorf("read-cache quota conflict is malformed: %w", parseErr)
			}
			if floorErr := manager.noteQuotaRevision(parsed.Revision); floorErr != nil {
				return floorErr
			}
		}
	}
	return fmt.Errorf("read-cache quota CAS retries exceeded")
}

func (manager *readCacheManager) loadQuota(ctx context.Context) ([]byte, *readCacheQuotaLedger, error) {
	h := manager.quotaHandle()
	rawFirst, errFirst := manager.readControl(ctx, h)
	rawSecond, errSecond := manager.readControl(ctx, h)

	firstExists := errFirst == nil
	secondExists := errSecond == nil
	if errFirst != nil && (manager.coordinatorBackend == nil || !manager.coordinatorBackend.IsNotExist(errFirst)) {
		return nil, nil, errFirst
	}
	if errSecond != nil && (manager.coordinatorBackend == nil || !manager.coordinatorBackend.IsNotExist(errSecond)) {
		return nil, nil, errSecond
	}

	if !firstExists && !secondExists {
		ledger := &readCacheQuotaLedger{
			Format:    readCacheQuotaFormatV1,
			Namespace: readCacheNamespace,
			Revision:  0,
			Managers:  []readCacheManagerRef{},
			Entries:   []readCacheQuotaEntry{},
		}
		if err := manager.noteQuotaRevision(ledger.Revision); err != nil {
			return nil, nil, err
		}
		return nil, ledger, nil
	}

	var firstLedger *readCacheQuotaLedger
	if firstExists {
		var parseErr error
		firstLedger, parseErr = manager.parseQuota(rawFirst)
		if parseErr != nil {
			return nil, nil, parseErr
		}
	}
	var secondLedger *readCacheQuotaLedger
	if secondExists {
		var parseErr error
		secondLedger, parseErr = manager.parseQuota(rawSecond)
		if parseErr != nil {
			return nil, nil, parseErr
		}
	}

	if firstExists && secondExists && firstLedger.Revision == secondLedger.Revision && !bytes.Equal(rawFirst, rawSecond) {
		return nil, nil, fmt.Errorf("equivocated read-cache quota revision %d", firstLedger.Revision)
	}

	selectedRaw := rawFirst
	selectedLedger := firstLedger
	if selectedLedger == nil || (secondLedger != nil && secondLedger.Revision > selectedLedger.Revision) {
		selectedRaw = rawSecond
		selectedLedger = secondLedger
	}
	if selectedLedger == nil {
		ledger := &readCacheQuotaLedger{
			Format:    readCacheQuotaFormatV1,
			Namespace: readCacheNamespace,
			Revision:  0,
			Managers:  []readCacheManagerRef{},
			Entries:   []readCacheQuotaEntry{},
		}
		if err := manager.noteQuotaRevision(ledger.Revision); err != nil {
			return nil, nil, err
		}
		return nil, ledger, nil
	}
	if err := manager.noteQuotaRevision(selectedLedger.Revision); err != nil {
		return nil, nil, err
	}
	return selectedRaw, selectedLedger, nil
}

func (manager *readCacheManager) noteQuotaRevision(revision uint64) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if revision < manager.quotaRevisionFloor {
		return fmt.Errorf("read-cache quota revision rollback detected: floor=%d current=%d", manager.quotaRevisionFloor, revision)
	}
	if revision > manager.quotaRevisionFloor {
		manager.quotaRevisionFloor = revision
	}
	return nil
}

func (manager *readCacheManager) parseQuota(raw []byte) (*readCacheQuotaLedger, error) {
	var ledger readCacheQuotaLedger
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return nil, err
	}
	if ledger.Format != readCacheQuotaFormatV1 || ledger.Namespace != readCacheNamespace {
		return nil, fmt.Errorf("invalid read-cache quota scope")
	}
	sig := ledger.Signature
	ledger = manager.signQuota(ledger)
	if !hmac.Equal([]byte(sig), []byte(ledger.Signature)) {
		return nil, fmt.Errorf("invalid read-cache quota signature")
	}
	ledger.Signature = sig
	sort.SliceStable(ledger.Managers, func(i, j int) bool { return ledger.Managers[i].ID < ledger.Managers[j].ID })
	for i := range ledger.Entries {
		if ledger.Entries[i].RepositoryID == "" {
			return nil, fmt.Errorf("invalid read-cache quota entry repository scope")
		}
		if ledger.Entries[i].EntryID == "" && ledger.Entries[i].CommitOrder != 0 {
			ledger.Entries[i].EntryID = quotaEntryID(ledger.Entries[i].RepositoryID, ledger.Entries[i].Key, ledger.Entries[i].CommitOrder)
		}
	}
	sort.SliceStable(ledger.Entries, func(i, j int) bool { return ledger.Entries[i].EntryID < ledger.Entries[j].EntryID })
	return &ledger, nil
}

func (manager *readCacheManager) signQuota(ledger readCacheQuotaLedger) readCacheQuotaLedger {
	ledger.Signature = ""
	for i := range ledger.Entries {
		if ledger.Entries[i].RepositoryID == "" {
			ledger.Entries[i].RepositoryID = manager.repoID
		}
		if ledger.Entries[i].RepositoryID == manager.repoID && ledger.Entries[i].PolicyRevision == 0 {
			ledger.Entries[i].PolicyRevision = manager.currentPolicyRevision()
		}
		if ledger.Entries[i].CommitOrder != 0 {
			ledger.Entries[i].EntryID = quotaEntryID(ledger.Entries[i].RepositoryID, ledger.Entries[i].Key, ledger.Entries[i].CommitOrder)
		}
	}
	sort.SliceStable(ledger.Managers, func(i, j int) bool { return ledger.Managers[i].ID < ledger.Managers[j].ID })
	sort.SliceStable(ledger.Entries, func(i, j int) bool { return ledger.Entries[i].EntryID < ledger.Entries[j].EntryID })
	raw, _ := json.Marshal(ledger)
	mac := hmac.New(sha256.New, manager.controlKey)
	if _, err := mac.Write(raw); err != nil {
		debug.Log("read-cache quota sign write failed: %v", err)
	}
	ledger.Signature = hex.EncodeToString(mac.Sum(nil))
	return ledger
}

func readCacheKeyDigest(key []byte) string {
	digest := sha256.Sum256(key)
	return hex.EncodeToString(digest[:])
}

func (manager *readCacheManager) pruneExpiredLeases(ledger *readCacheQuotaLedger, now time.Time) {
	expired := map[string]struct{}{}
	for _, ref := range ledger.Managers {
		if ref.LeaseExpiryMS <= now.UnixMilli() {
			expired[ref.ID] = struct{}{}
		}
	}
	for i := 0; i < len(ledger.Entries); i++ {
		entry := ledger.Entries[i]
		if _, ok := expired[entry.ManagerID]; !ok {
			continue
		}
		if entry.State == readCacheStateAdmitting {
			if entry.DataHandle != "" || entry.MetaHandle != "" {
				ledger.Entries[i].State = readCacheStateDeleting
				ledger.Entries[i].UpdatedMS = now.UnixMilli()
				continue
			}
			ledger.Entries = append(ledger.Entries[:i], ledger.Entries[i+1:]...)
			i--
		}
	}
	if len(expired) == 0 {
		return
	}
	live := ledger.Managers[:0]
	for _, ref := range ledger.Managers {
		if _, ok := expired[ref.ID]; ok {
			continue
		}
		live = append(live, ref)
	}
	ledger.Managers = live
}

func (manager *readCacheManager) renewLease(ledger *readCacheQuotaLedger, now time.Time) bool {
	expires := now.Add(readCacheLeaseDuration).UnixMilli()
	for i := range ledger.Managers {
		if ledger.Managers[i].ID == manager.managerID {
			if ledger.Managers[i].LeaseExpiryMS == expires {
				return false
			}
			ledger.Managers[i].LeaseExpiryMS = expires
			return true
		}
	}
	ledger.Managers = append(ledger.Managers, readCacheManagerRef{ID: manager.managerID, LeaseExpiryMS: expires})
	return true
}

func quotaUsage(ledger *readCacheQuotaLedger) (map[string]uint64, map[string]uint64, uint64, uint64) {
	usedByTier := map[string]uint64{}
	reservedByTier := map[string]uint64{}
	var aggregateUsed uint64
	var aggregateReserved uint64
	for _, entry := range ledger.Entries {
		switch entry.State {
		case readCacheStateAdmitting:
			reservedByTier[entry.TierID] = saturatingAddUint64(reservedByTier[entry.TierID], entry.Bytes)
			aggregateReserved = saturatingAddUint64(aggregateReserved, entry.Bytes)
		case readCacheStatePublishing, readCacheStateDeleting, readCacheStatePublished, readCacheStateActive:
			usedByTier[entry.TierID] = saturatingAddUint64(usedByTier[entry.TierID], entry.Bytes)
			aggregateUsed = saturatingAddUint64(aggregateUsed, entry.Bytes)
		}
	}
	return usedByTier, reservedByTier, aggregateUsed, aggregateReserved
}

func findQuotaEntry(ledger *readCacheQuotaLedger, entryID string) (*readCacheQuotaEntry, bool) {
	for i := range ledger.Entries {
		if ledger.Entries[i].EntryID == entryID {
			return &ledger.Entries[i], true
		}
	}
	return nil, false
}

func quotaEntryID(repositoryID string, key string, commitOrder uint64) string {
	return fmt.Sprintf("%s#%s#%d", repositoryID, key, commitOrder)
}

func quotaEntryOrder(entry readCacheQuotaEntry) uint64 {
	return entry.CommitOrder
}

func quotaEntryKeyGeneration(entry readCacheQuotaEntry) uint64 {
	if entry.KeyGeneration != 0 {
		return entry.KeyGeneration
	}
	return max(1, entry.PolicyRevision)
}

func nextQuotaCommitOrder(ledger *readCacheQuotaLedger, repositoryID string, key string, tierID string) uint64 {
	next := ledger.Revision + 1
	for _, entry := range ledger.Entries {
		if entry.RepositoryID != repositoryID || entry.Key != key || entry.TierID != tierID {
			continue
		}
		order := quotaEntryOrder(entry)
		if order >= next {
			next = order + 1
		}
	}
	if next == 0 {
		return 1
	}
	return next
}

func readCacheRandomGeneration() uint64 {
	return readCacheGenerationSource()
}

var readCacheGenerationSource = func() uint64 {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return uint64(time.Now().UTC().UnixNano())
	}
	result := binary.BigEndian.Uint64(value[:])
	if result == 0 {
		return 1
	}
	return result
}

func (manager *readCacheManager) readControl(ctx context.Context, handle backend.Handle) ([]byte, error) {
	if manager.coordinatorBackend == nil {
		return nil, fmt.Errorf("read-cache coordinator backend is unavailable")
	}
	return loadBytes(ctx, manager.coordinatorBackend, handle, 0, 0)
}

func (manager *readCacheManager) ensureControlKey(ctx context.Context) ([]byte, error) {
	handle := manager.controlKeyHandle()
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	for attempt := 0; attempt < readCacheCoordinationRetries; attempt++ {
		current, err := manager.readControl(ctx, handle)
		if err == nil {
			if len(current) != readCacheControlKeySize {
				return nil, fmt.Errorf("invalid read-cache domain control key")
			}
			return append([]byte(nil), current...), nil
		}
		if manager.coordinatorBackend == nil || !manager.coordinatorBackend.IsNotExist(err) {
			return nil, err
		}
		desired := make([]byte, readCacheControlKeySize)
		if _, err := rand.Read(desired); err != nil {
			return nil, err
		}
		observed, swapped, err := manager.coordinator.CompareAndSwap(ctx, handle, nil, desired)
		if err != nil {
			readback, readErr := manager.readControl(ctx, handle)
			if readErr == nil && bytes.Equal(readback, desired) {
				return desired, nil
			}
			return nil, err
		}
		if swapped {
			return desired, nil
		}
		if observed == nil {
			continue
		}
		if len(observed) != readCacheControlKeySize {
			return nil, fmt.Errorf("invalid read-cache domain control key")
		}
		return append([]byte(nil), observed...), nil
	}
	return nil, fmt.Errorf("read-cache domain control key CAS retries exceeded")
}

func (manager *readCacheManager) controlKeyHandle() backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: readCacheNamespace + "/control/domain.key"}
}

func (manager *readCacheManager) policyHandle() backend.Handle {
	return backend.Handle{
		Type: backend.StagingFile,
		Name: readCacheNamespace + "/control/repositories/" + readCacheRepositoryScope(manager.repoID) + "/policy.json",
	}
}

func (manager *readCacheManager) quotaHandle() backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: readCacheNamespace + "/control/quota.json"}
}

func (manager *readCacheManager) tierByID(id string) *readCacheTier {
	for _, tier := range manager.tiers {
		if tier.id == id {
			return tier
		}
	}
	return nil
}

func policyTierByID(policy *readCachePolicySnapshot, tierID string) (readCachePolicyTier, bool) {
	if policy == nil {
		return readCachePolicyTier{}, false
	}
	for _, tier := range policy.Tiers {
		if tier.ID == tierID {
			return tier, true
		}
	}
	return readCachePolicyTier{}, false
}

func matchesAdmissionLease(entry readCacheQuotaEntry, lease *readCacheAdmissionLease) bool {
	if lease == nil {
		return false
	}
	if entry.RepositoryID != lease.RepositoryID || entry.EntryID != lease.EntryID || entry.Key != lease.Key ||
		entry.TierID != lease.TierID || entry.Generation != lease.Generation {
		return false
	}
	return quotaEntryOrder(entry) == lease.CommitOrder
}

func quotaEntryIsPublished(state string) bool {
	return state == readCacheStatePublished || state == readCacheStateActive
}

func (manager *readCacheManager) publishAdmission(ctx context.Context, lease *readCacheAdmissionLease) bool {
	if lease == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	published, _, err := manager.mutateQuotaAdmission(ctx, func(ledger *readCacheQuotaLedger, policy *readCachePolicySnapshot, now time.Time) (bool, error) {
		tierPolicy, ok := policyTierByID(policy, lease.TierID)
		if !ok || !tierPolicy.Enabled {
			return false, nil
		}
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		usedByTier, reservedByTier, aggregateUsed, aggregateReserved := quotaUsage(ledger)
		if sumExceedsUint64(policy.AggregateMaxBytes, aggregateUsed, aggregateReserved) {
			return false, nil
		}
		if sumExceedsUint64(tierPolicy.MaxBytes, usedByTier[lease.TierID], reservedByTier[lease.TierID]) {
			return false, nil
		}
		for i := range ledger.Entries {
			entry := &ledger.Entries[i]
			if !matchesAdmissionLease(*entry, lease) {
				continue
			}
			if entry.State != readCacheStatePublishing {
				return false, nil
			}
			entry.Bytes = lease.Bytes
			entry.State = readCacheStatePublished
			entry.UpdatedMS = now.UnixMilli()
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		manager.mu.Lock()
		manager.admissions = false
		manager.mu.Unlock()
		return false
	}
	return published
}

func (manager *readCacheManager) transitionAdmissionToDeleting(ctx context.Context, lease *readCacheAdmissionLease, requiredState string) {
	if lease == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	_, _, err := manager.mutateQuotaAdmission(ctx, func(ledger *readCacheQuotaLedger, _ *readCachePolicySnapshot, now time.Time) (bool, error) {
		for i := range ledger.Entries {
			entry := &ledger.Entries[i]
			if !matchesAdmissionLease(*entry, lease) {
				continue
			}
			if requiredState != "" && entry.State != requiredState {
				return false, nil
			}
			if entry.State == readCacheStateDeleting {
				return false, nil
			}
			entry.State = readCacheStateDeleting
			entry.UpdatedMS = now.UnixMilli()
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		debug.Log("read-cache transition to deleting failed: %v", err)
	}
}

func (manager *readCacheManager) enforcePolicyRetirement(ctx context.Context) {
	if manager == nil {
		return
	}
	for pass := 0; pass < 8; pass++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		manager.retireLocalTrustMismatches(ctx)
		manager.retryPendingDeletions(ctx)
		if err := manager.reconcileQuotaInventory(ctx); err != nil {
			debug.Log("read-cache ambiguous admission reconciliation failed: %v", err)
			return
		}
		changed, err := manager.markPolicyViolationsDeleting(ctx)
		if err != nil {
			debug.Log("read-cache policy enforcement failed: %v", err)
			return
		}
		manager.reconcileAdmissionVisibility(ctx)
		if !changed {
			return
		}
	}
}

func (manager *readCacheManager) retryPendingDeletions(ctx context.Context) {
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		pending := make([]*readCacheEntry, 0, len(tier.deleting))
		for _, entry := range tier.deleting {
			pending = append(pending, entry)
		}
		tier.mu.Unlock()
		for _, entry := range pending {
			tier.deleteRetiringEntry(ctx, entry)
		}
	}
}

func (manager *readCacheManager) retireLocalTrustMismatches(ctx context.Context) {
	for _, tier := range manager.tiers {
		retire := make([]*readCacheEntry, 0)
		tier.mu.Lock()
		trust := tier.trust
		now := time.Now().UTC()
		for _, entry := range tier.entries {
			if entry.Trust == trust && !tier.entryExpiredLocked(entry, now) {
				continue
			}
			tier.markRetiringLocked(entry)
			retire = append(retire, entry)
		}
		tier.mu.Unlock()
		for _, entry := range retire {
			tier.deleteRetiringEntry(ctx, entry)
		}
	}
}

//nolint:gocognit // Eviction selection preserves signed quota accounting and deterministic policy enforcement in one transaction.
func (manager *readCacheManager) markPolicyViolationsDeleting(ctx context.Context) (bool, error) {
	type quotaCandidate struct {
		index     int
		commit    uint64
		updatedMS int64
		bytes     uint64
		tierID    string
	}
	ctx, cancel := context.WithTimeout(ctx, readCacheCoordinationTimeout)
	defer cancel()
	changed, _, err := manager.mutateQuotaAdmission(ctx, func(ledger *readCacheQuotaLedger, policy *readCachePolicySnapshot, now time.Time) (bool, error) {
		manager.pruneExpiredLeases(ledger, now)
		manager.renewLease(ledger, now)
		byTier := map[string]readCachePolicyTier{}
		for _, tier := range policy.Tiers {
			byTier[tier.ID] = tier
		}
		localChanged := false
		servableByTier := map[string]uint64{}
		var aggregateServable uint64
		tierCandidates := map[string][]quotaCandidate{}
		allCandidates := make([]quotaCandidate, 0)
		for i := range ledger.Entries {
			entry := &ledger.Entries[i]
			if entry.RepositoryID != manager.repoID {
				continue
			}
			tierPolicy, knownTier := byTier[entry.TierID]
			if !knownTier || !tierPolicy.Enabled {
				if entry.State != readCacheStateDeleting {
					entry.State = readCacheStateDeleting
					entry.UpdatedMS = now.UnixMilli()
					localChanged = true
				}
				continue
			}
			if entry.State == readCacheStateDeleting {
				continue
			}
			if entry.State != readCacheStateAdmitting && entry.State != readCacheStatePublishing && !quotaEntryIsPublished(entry.State) {
				continue
			}
			servableByTier[entry.TierID] = saturatingAddUint64(servableByTier[entry.TierID], entry.Bytes)
			aggregateServable = saturatingAddUint64(aggregateServable, entry.Bytes)
			candidate := quotaCandidate{index: i, commit: quotaEntryOrder(*entry), updatedMS: entry.UpdatedMS, bytes: entry.Bytes, tierID: entry.TierID}
			tierCandidates[entry.TierID] = append(tierCandidates[entry.TierID], candidate)
			allCandidates = append(allCandidates, candidate)
		}
		sortByAge := func(candidates []quotaCandidate) {
			sort.SliceStable(candidates, func(i, j int) bool {
				if candidates[i].commit == candidates[j].commit {
					return candidates[i].updatedMS < candidates[j].updatedMS
				}
				if candidates[i].commit == 0 {
					return true
				}
				if candidates[j].commit == 0 {
					return false
				}
				return candidates[i].commit < candidates[j].commit
			})
		}
		for tierID, candidates := range tierCandidates {
			tierPolicy := byTier[tierID]
			tierServable := servableByTier[tierID]
			if tierServable <= tierPolicy.MaxBytes {
				continue
			}
			over := tierServable - tierPolicy.MaxBytes
			sortByAge(candidates)
			for _, candidate := range candidates {
				if over == 0 {
					break
				}
				entry := &ledger.Entries[candidate.index]
				if entry.State == readCacheStateDeleting {
					continue
				}
				entry.State = readCacheStateDeleting
				entry.UpdatedMS = now.UnixMilli()
				localChanged = true
				if candidate.bytes >= over {
					over = 0
				} else {
					over -= candidate.bytes
				}
			}
		}
		if aggregateServable > policy.AggregateMaxBytes {
			over := aggregateServable - policy.AggregateMaxBytes
			sortByAge(allCandidates)
			for _, candidate := range allCandidates {
				if over == 0 {
					break
				}
				entry := &ledger.Entries[candidate.index]
				if entry.State == readCacheStateDeleting {
					continue
				}
				entry.State = readCacheStateDeleting
				entry.UpdatedMS = now.UnixMilli()
				localChanged = true
				if candidate.bytes >= over {
					over = 0
				} else {
					over -= candidate.bytes
				}
			}
		}
		return localChanged, nil
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

func (manager *readCacheManager) runBeforeQuotaCASHook() {
	manager.mu.Lock()
	hook := manager.testBeforeQuotaCAS
	manager.testBeforeQuotaCAS = nil
	manager.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (manager *readCacheManager) runAfterCommitCASHook() {
	manager.mu.Lock()
	hook := manager.testAfterCommitCAS
	manager.testAfterCommitCAS = nil
	manager.mu.Unlock()
	if hook != nil {
		hook()
	}
}
