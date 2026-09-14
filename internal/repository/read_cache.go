package repository

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	MaxReadCacheChunkBytes            = vaultic.MaxReadCacheChunkBytes
	readCacheFormatV1                 = uint32(1)
	readCacheFormatV2                 = uint32(2)
	readCacheDefaultMax               = 512 * 1024 * 1024
	readCacheDefaultChunk             = 4 * 1024 * 1024
	readCacheDefaultPrio              = uint32(100)
	readCacheTrustEncrypted           = "encrypted-only"
	readCacheTrustPlaintext           = "plaintext-allowed"
	readCacheNamespace                = "read-cache/v1"
	readCacheControlPrefix            = readCacheNamespace + "/control/"
	readCacheMaxInflightOps           = uint64(128)
	readCacheProbeTimeout             = 250 * time.Millisecond
	readCacheNegativeTTL              = 5 * time.Second
	readCacheCircuitWindow            = 5 * time.Second
	readCacheCircuitTrips             = 3
	readCacheCircuitMaxRecentFailures = 64
	readCacheFillBackoffMin           = 100 * time.Millisecond
	readCacheFillBackoffMax           = 5 * time.Second
	readCacheTelemetryPollTimeout     = 500 * time.Millisecond
	readCacheCapacityStopTimeout      = 2 * time.Second
	readCacheInitializationTimeout    = 2 * time.Second
	readCacheFillTimeout              = 250 * time.Millisecond
	readCacheAgeSweepInterval         = time.Minute

	readCacheRepSourceEncryptedRange = "source-encrypted-range"
	readCacheRepCompressedContainer  = "compressed-derived-container"
	readCacheRepDecodedExtent        = "decoded-blob-extent"
	readCacheRepWholeFile            = "whole-file-materialization"
)

type readCacheStatus struct {
	Enabled            bool
	PolicyRevision     uint64
	AggregateMaxBytes  uint64
	RequestedMaxBytes  uint64
	UsedBytes          uint64
	ReservedBytes      uint64
	PinnedBytes        uint64
	InflightBytes      uint64
	PendingDeleteBytes uint64
	AdmissionsEnabled  bool
	CapacityMode       string
	CapacityHealth     string
	TelemetryState     string
	TelemetryAgeMS     uint64
	TelemetrySourceGen uint64
	FillAllowed        bool
	AvailableChunks    uint64
	UnavailableChunks  uint64
	AvailableBytes     uint64
	UnavailableBytes   uint64
	CoalescedWaits     uint64
	Tiers              []readCacheTierStatus
}

type readCacheTierStatus struct {
	ID                 string
	Enabled            bool
	Trust              string
	TrustAck           bool
	Codec              string
	ChunkSize          int
	ReadPriority       uint32
	AdmissionPriority  uint32
	IdleAgeMS          uint64
	AbsoluteAgeMS      uint64
	MaxBytes           uint64
	UsedBytes          uint64
	ReservedBytes      uint64
	PinnedBytes        uint64
	PendingDeleteBytes uint64
	PurgeFailures      uint64
	NegativeKeys       int
	CircuitOpen        bool
	RefillBackoffMS    uint64
	RefillFailures     uint32
	Entries            int
}

type readCachePolicyUpdate struct {
	ID                string
	Enabled           *bool
	MaxBytes          *uint64
	ChunkBytes        *uint64
	Trust             *string
	TrustAck          *bool
	Codec             *string
	IdleAge           *time.Duration
	AbsoluteAge       *time.Duration
	ReadPriority      *uint32
	AdmissionPriority *uint32
	ExpectedRev       *uint64
}

type ReadCacheStatus = readCacheStatus
type ReadCacheTierStatus = readCacheTierStatus
type ReadCachePolicyUpdate = readCachePolicyUpdate

type readCacheManager struct {
	repoID             string
	key                []byte
	controlKey         []byte
	tiers              []*readCacheTier
	coalesced          map[string]*readCacheFillGate
	coordinator        backend.ConditionalWriter
	coordinatorBackend backend.Backend
	managerID          string
	aggregateMax       uint64
	policyRevision     uint64
	inflightTasks      uint64
	inflightBytes      uint64
	maxInflightBytes   uint64
	admissions         bool
	requestedAggregate uint64
	aggregateCeiling   uint64
	publishedAggregate uint64
	capacityController *readCacheCapacityController
	capacityDecision   readCacheCapacityDecision
	availableChunks    uint64
	unavailableChunks  uint64
	availableBytes     uint64
	unavailableBytes   uint64
	coalescedWaits     uint64
	logicalAccess      map[string]uint32
	quotaRevisionFloor uint64
	testBeforeQuotaCAS func()
	testAfterCommitCAS func()
	capacityStop       context.CancelFunc
	capacityDone       chan struct{}
	policyStop         context.CancelFunc
	policyDone         chan struct{}
	policyWake         chan struct{}
	mu                 sync.Mutex
}

type readCacheFillGate struct {
	mu   sync.Mutex
	refs int
}

type readCacheManagerAdmissionSnapshot struct {
	PolicyRevision uint64
	AggregateMax   uint64
	Admissions     bool
	FillAllowed    bool
}

type readCacheTierAdmissionSnapshot struct {
	ID         string
	Generation uint64
	Enabled    bool
	Ingest     bool
	Trust      string
	TrustAck   bool
	Codec      string
	ReadPrio   uint32
	AdmitPrio  uint32
	IdleAge    time.Duration
	AbsAge     time.Duration
	ChunkSize  int
	MaxBytes   uint64
}

type readCacheAdmissionSnapshot struct {
	Manager readCacheManagerAdmissionSnapshot
	Tier    readCacheTierAdmissionSnapshot
}

type readCacheTier struct {
	manager    *readCacheManager
	id         string
	generation uint64
	trust      string
	trustAck   bool
	codec      string
	enabled    bool
	ingest     bool
	readPrio   uint32
	admitPrio  uint32
	idleAge    time.Duration
	absAge     time.Duration
	maxBytes   uint64
	requested  uint64
	chunkSize  int
	backend    backend.Backend

	mu             sync.Mutex
	used           uint64
	reserved       uint64
	pinned         uint64
	pendingDelete  uint64
	purgeFailures  uint64
	entries        map[string]*readCacheEntry
	deleting       map[string]*readCacheEntry
	order          []string
	probeFailures  map[string]uint32
	negativeUntil  map[string]time.Time
	recentFailures []time.Time
	circuitUntil   time.Time
	fillBackoff    time.Time
	fillFailures   uint32
}

type readCacheHandleDescriptor struct {
	RepositoryScope  string
	TierID           string
	Representation   string
	Trust            string
	KeyGeneration    uint64
	HasKeyGeneration bool
	Generation       uint64
	HasGeneration    bool
	CommitOrder      uint64
	HasCommitOrder   bool
	DigestSuffix     string
	Extension        string
	IsChunk          bool
	IsLegacy         bool
}

type readCacheEntry struct {
	Key            string
	DataHandle     backend.Handle
	MetaHandle     backend.Handle
	Size           uint64
	MetaSize       uint64
	Generation     uint64
	KeyGeneration  uint64
	CommitOrder    uint64
	Published      bool
	Trust          string
	Representation string
	Group          string
	UsefulBytes    uint64
	CreatedAt      time.Time
	LastAccess     time.Time
	Hits           uint64
	Pins           uint32
	Retiring       bool
}

type readCacheIdentity struct {
	RepositoryID   string `json:"repository_id"`
	Representation string `json:"representation"`
	ContentID      string `json:"content_id"`
	LayoutID       string `json:"layout_id"`
	Offset         int64  `json:"offset"`
	Length         int    `json:"length"`
	FormatVersion  uint32 `json:"format_version"`
	Codec          string `json:"codec"`
	Trust          string `json:"trust"`
	ChunkGeometry  int    `json:"chunk_geometry"`
	KeyGeneration  uint64 `json:"key_generation"`
}

type readCacheChunkMeta struct {
	Format             uint32            `json:"format"`
	RepositoryID       string            `json:"repository_id"`
	TierID             string            `json:"tier_id"`
	Generation         uint64            `json:"generation,omitempty"`
	CommitOrder        uint64            `json:"commit_order,omitempty"`
	Identity           readCacheIdentity `json:"identity"`
	PackID             string            `json:"pack_id,omitempty"`
	Offset             int64             `json:"offset,omitempty"`
	Length             int               `json:"length,omitempty"`
	ChunkSize          int               `json:"chunk_size,omitempty"`
	Trust              string            `json:"trust,omitempty"`
	DigestSHA256       string            `json:"digest_sha256"`
	DataEncrypted      bool              `json:"data_encrypted,omitempty"`
	DataNonce          string            `json:"data_nonce,omitempty"`
	CiphertextLength   int               `json:"ciphertext_length,omitempty"`
	AssociatedDataHash string            `json:"aad_sha256,omitempty"`
	CreatedUnixMS      int64             `json:"created_unix_ms"`
	Signature          string            `json:"signature"`
}

type backendCapacityTelemetryAdapter struct {
	source backend.CapacityTelemetry
}

func (adapter backendCapacityTelemetryAdapter) Sample(ctx context.Context) (readCacheCapacitySample, error) {
	raw, err := adapter.source.SampleCapacity(ctx)
	if err != nil {
		return readCacheCapacitySample{}, err
	}
	sample := readCacheCapacitySample{
		TotalRawBytes:           raw.TotalRawBytes,
		FreeRawBytes:            raw.FreeRawBytes,
		EligibleTotalRawBytes:   raw.EligibleTotalRawBytes,
		EligibleFreeRawBytes:    raw.EligibleFreeRawBytes,
		PoolMaxAvailRawBytes:    raw.PoolMaxAvailRawBytes,
		PoolQuotaRawBytes:       raw.PoolQuotaRawBytes,
		PoolMaxAvailBytes:       raw.PoolMaxAvailBytes,
		PoolMaxAvailKnown:       raw.PoolMaxAvailKnown,
		PoolQuotaAvailableBytes: raw.PoolQuotaAvailableBytes,
		PoolQuotaKnown:          raw.PoolQuotaKnown,
		ObjectHeadroomRawBytes:  raw.ObjectHeadroomRawBytes,
		RawAmplification:        raw.RawAmplification,
		Health:                  raw.Health,
		Timestamp:               time.Now().UTC(),
		SourceGeneration:        raw.SourceGeneration,
		Denied:                  raw.Denied,
		Inconsistent:            raw.Inconsistent,
	}
	if sample.FreeRawBytes > sample.TotalRawBytes {
		sample.Inconsistent = true
	}
	if sample.EligibleTotalRawBytes > 0 && sample.EligibleFreeRawBytes > sample.EligibleTotalRawBytes {
		sample.Inconsistent = true
	}
	sample.Health = normalizeCacheHealth(sample.Health)
	return sample, nil
}

//nolint:funlen,gocognit,nestif // Initialization validates every tier and configures repository-lifetime workers and capacity policy.
func (r *Repository) readCacheManager() *readCacheManager {
	r.readCacheMu.Lock()
	defer r.readCacheMu.Unlock()
	if r.readCache != nil {
		return r.readCache
	}
	if r.key == nil || r.cfg.ID == "" {
		return nil
	}
	model, err := r.PlacementModel()
	if err != nil {
		return nil
	}
	manager := &readCacheManager{
		repoID:           r.cfg.ID,
		key:              readCacheKeyBytes(r.key),
		coalesced:        map[string]*readCacheFillGate{},
		logicalAccess:    map[string]uint32{},
		admissions:       true,
		managerID:        readCacheRandomID(),
		aggregateCeiling: r.cfg.ReadCacheAggregateCapacityBytes,
	}
	controllerOptions := readCacheCapacityControllerOptions{Mode: readCacheBudgetModeFixed}
	controllerConfigured := false
	var controllerTelemetry readCacheCapacityTelemetry
	for _, candidate := range model.Backends {
		if candidate.Role != PlacementRoleReadCache {
			continue
		}
		be, ok := r.backendForPlacement(model, candidate)
		if !ok || be == nil {
			continue
		}
		maxBytes := candidate.CapacityBytes
		if maxBytes == 0 {
			maxBytes = readCacheDefaultMax
		}
		chunkBytes := candidate.TargetPackSizeBytes
		if chunkBytes == 0 {
			chunkBytes = readCacheDefaultChunk
		}
		mode, modeErr := readCacheCapacityModeForBackend(candidate.Location, candidate.ReadCacheBudgetMode)
		if modeErr != nil {
			manager.admissions = false
			continue
		}
		if !controllerConfigured {
			controllerOptions = readCacheCapacityControllerOptions{
				Mode:                    mode,
				FixedMaxBytes:           0,
				ReserveFraction:         candidate.ReadCacheReserveFrac,
				MinFreeRawBytes:         candidate.ReadCacheMinFreeRaw,
				SafetyMarginRawBytes:    candidate.ReadCacheSafetyRaw,
				OperatorMaxRawBytes:     candidate.ReadCacheMaxRaw,
				TelemetryPollInterval:   5 * time.Second,
				TelemetryMaxAge:         time.Duration(candidate.ReadCacheMaxAgeMS) * time.Millisecond,
				GrowthRateLogicalBytes:  candidate.ReadCacheGrowBytes,
				GrowthHysteresisBytes:   candidate.ReadCacheHysteresis,
				StableSamplesForGrowth:  candidate.ReadCacheStableCount,
				FallbackLogicalBytes:    candidate.ReadCacheFallbackMax,
				DefaultRawAmplification: candidate.ReadCacheRawAmp,
			}
			controllerConfigured = true
			if mode == readCacheBudgetModeCephFreeSpace {
				capability := backend.AsCapability[backend.CapacityTelemetry](be)
				if capability == nil {
					manager.admissions = false
				} else {
					controllerTelemetry = backendCapacityTelemetryAdapter{source: capability}
				}
			}
		} else if mode != controllerOptions.Mode {
			manager.admissions = false
			continue
		} else if mode == readCacheBudgetModeCephFreeSpace && controllerTelemetry == nil {
			capability := backend.AsCapability[backend.CapacityTelemetry](be)
			if capability != nil {
				controllerTelemetry = backendCapacityTelemetryAdapter{source: capability}
			}
		}
		tier := &readCacheTier{
			manager:       manager,
			id:            candidate.ID,
			trust:         normalizeReadCacheTrust(candidate.ReadCacheTrust, candidate.ReadCacheAck),
			trustAck:      strings.EqualFold(strings.TrimSpace(candidate.ReadCacheTrust), readCacheTrustPlaintext) && candidate.ReadCacheAck,
			enabled:       candidate.ReadAllowed(),
			ingest:        candidate.IngestEnabled(),
			readPrio:      readCacheDefaultPrio,
			admitPrio:     readCacheDefaultPrio,
			maxBytes:      maxBytes,
			requested:     maxBytes,
			chunkSize:     int(chunkBytes),
			codec:         "raw",
			backend:       be,
			entries:       map[string]*readCacheEntry{},
			deleting:      map[string]*readCacheEntry{},
			probeFailures: map[string]uint32{},
			negativeUntil: map[string]time.Time{},
		}
		manager.tiers = append(manager.tiers, tier)
		manager.aggregateMax = saturatingAddUint64(manager.aggregateMax, tier.maxBytes)
		manager.requestedAggregate = saturatingAddUint64(manager.requestedAggregate, tier.requested)
	}
	sort.SliceStable(manager.tiers, func(i, j int) bool {
		return manager.tiers[i].id < manager.tiers[j].id
	})
	if len(manager.tiers) == 0 {
		return nil
	}
	for _, tier := range manager.sortedTiersByAdmissionPriority() {
		coordinator := backend.AsCapability[backend.ConditionalWriter](tier.backend)
		if coordinator == nil {
			continue
		}
		manager.coordinator = coordinator
		manager.coordinatorBackend = tier.backend
		break
	}
	if manager.coordinator == nil {
		manager.admissions = false
	}
	manager.publishedAggregate = manager.aggregateMax
	if manager.aggregateCeiling > 0 && manager.requestedAggregate > manager.aggregateCeiling {
		manager.requestedAggregate = manager.aggregateCeiling
		manager.aggregateMax = manager.aggregateCeiling
		manager.publishedAggregate = manager.aggregateCeiling
	}
	if manager.aggregateMax == 0 {
		manager.admissions = false
	} else {
		manager.maxInflightBytes = max(16*1024*1024, manager.aggregateMax/8)
	}
	if !controllerConfigured {
		controllerOptions.FixedMaxBytes = manager.aggregateMax
	}
	if controllerOptions.Mode == readCacheBudgetModeFixed {
		controllerOptions.FixedMaxBytes = manager.aggregateMax
	}
	manager.capacityController = newReadCacheCapacityController(controllerOptions, controllerTelemetry)
	manager.capacityDecision = manager.capacityController.snapshot(time.Now().UTC())
	initializationCtx, cancelInitialization := context.WithTimeout(context.Background(), readCacheInitializationTimeout)
	manager.initializeCoordinator(initializationCtx)
	cancelInitialization()
	manager.startCapacityWorker()
	manager.startPolicyWorker()
	r.readCache = manager
	return manager
}

func readCacheRandomID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("rc-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func readCacheKeyBytes(key *crypto.Key) []byte {
	if key == nil || !key.Valid() {
		return nil
	}
	material := append(append([]byte{}, key.MACKey.K[:]...), key.MACKey.R[:]...)
	mac := hmac.New(sha256.New, material)
	_, _ = mac.Write([]byte("vaultic-read-cache-v1")) // hash.Hash writes are specified to return a nil error.
	return mac.Sum(nil)
}

func normalizeReadCacheTrust(raw string, acknowledged bool) string {
	if strings.EqualFold(strings.TrimSpace(raw), readCacheTrustPlaintext) && acknowledged {
		return readCacheTrustPlaintext
	}
	return readCacheTrustEncrypted
}

func readCacheRepresentationAllowed(representation string) bool {
	switch representation {
	case readCacheRepSourceEncryptedRange, readCacheRepCompressedContainer, readCacheRepDecodedExtent, readCacheRepWholeFile:
		return true
	default:
		return false
	}
}

func (r *Repository) ReadCacheStatus() readCacheStatus {
	manager := r.readCacheManager()
	if manager == nil {
		return readCacheStatus{}
	}
	manager.mu.Lock()
	status := readCacheStatus{
		Enabled:           true,
		PolicyRevision:    manager.policyRevision,
		AggregateMaxBytes: manager.aggregateMax,
		RequestedMaxBytes: manager.requestedAggregate,
		AdmissionsEnabled: manager.admissions,
	}
	manager.mu.Unlock()
	decision := manager.capacityDecisionSnapshot()
	status.CapacityMode = manager.capacityMode()
	status.CapacityHealth = decision.Health
	status.TelemetryState = decision.TelemetryState
	status.TelemetryAgeMS = decision.TelemetryAgeMS
	status.TelemetrySourceGen = decision.SourceGeneration
	status.FillAllowed = decision.FillAllowed
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		status.Tiers = append(status.Tiers, readCacheTierStatus{
			ID:                 tier.id,
			Enabled:            tier.enabled,
			Trust:              tier.trust,
			TrustAck:           tier.trustAck,
			Codec:              tier.codec,
			ChunkSize:          tier.chunkSize,
			ReadPriority:       tier.readPrio,
			AdmissionPriority:  tier.admitPrio,
			IdleAgeMS:          uint64(tier.idleAge / time.Millisecond),
			AbsoluteAgeMS:      uint64(tier.absAge / time.Millisecond),
			MaxBytes:           tier.maxBytes,
			UsedBytes:          tier.used,
			ReservedBytes:      tier.reserved,
			PinnedBytes:        tier.pinned,
			PendingDeleteBytes: tier.pendingDelete,
			PurgeFailures:      tier.purgeFailures,
			NegativeKeys:       len(tier.negativeUntil),
			CircuitOpen:        time.Now().UTC().Before(tier.circuitUntil),
			RefillBackoffMS:    uint64(max(0, int(time.Until(tier.fillBackoff)/time.Millisecond))),
			RefillFailures:     tier.fillFailures,
			Entries:            len(tier.entries),
		})
		status.UsedBytes = saturatingAddUint64(status.UsedBytes, tier.used)
		status.ReservedBytes = saturatingAddUint64(status.ReservedBytes, tier.reserved)
		status.PinnedBytes = saturatingAddUint64(status.PinnedBytes, tier.pinned)
		status.PendingDeleteBytes = saturatingAddUint64(status.PendingDeleteBytes, tier.pendingDelete)
		tier.mu.Unlock()
	}
	quotaCtx, cancelQuota := context.WithTimeout(context.Background(), readCacheCoordinationTimeout)
	_, ledger, quotaErr := manager.loadQuota(quotaCtx)
	cancelQuota()
	if quotaErr == nil && ledger != nil {
		_, _, status.UsedBytes, status.ReservedBytes = quotaUsage(ledger)
		status.PendingDeleteBytes = 0
		for _, entry := range ledger.Entries {
			if entry.State == readCacheStateDeleting {
				status.PendingDeleteBytes = saturatingAddUint64(status.PendingDeleteBytes, entry.Bytes)
			}
		}
	}
	manager.mu.Lock()
	status.InflightBytes = manager.inflightBytes
	status.AvailableChunks = manager.availableChunks
	status.UnavailableChunks = manager.unavailableChunks
	status.AvailableBytes = manager.availableBytes
	status.UnavailableBytes = manager.unavailableBytes
	status.CoalescedWaits = manager.coalescedWaits
	manager.mu.Unlock()
	return status
}

func (r *Repository) UpdateReadCachePolicy(update readCachePolicyUpdate) error {
	manager := r.readCacheManager()
	if manager == nil {
		return fmt.Errorf("read-cache is not configured")
	}
	return manager.updatePolicy(context.Background(), update)
}

func (r *Repository) DrainReadCache(ctx context.Context) error {
	//nolint:contextcheck // The shared cache worker is owned by Repository.Close; ctx only bounds this drain operation.
	manager := r.readCacheManager()
	if manager == nil {
		return nil
	}
	for _, tier := range manager.tiers {
		if err := tier.drain(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) ClearReadCache(ctx context.Context, tierID string) error {
	//nolint:contextcheck // The shared cache worker is owned by Repository.Close; ctx only bounds this clear operation.
	manager := r.readCacheManager()
	if manager == nil {
		return nil
	}
	if tierID == "" {
		for _, tier := range manager.tiers {
			if err := tier.drain(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	tier := manager.tierByID(tierID)
	if tier == nil {
		return fmt.Errorf("unknown read-cache tier %q", tierID)
	}
	return tier.drain(ctx)
}

func (manager *readCacheManager) loadPack(
	ctx context.Context,
	packID vaultic.ID,
	handle backend.Handle,
	length int,
	offset int64,
	authoritative []placementReadCandidate,
	fn func(io.Reader) error,
) error {
	authorized := false
	for _, candidate := range authoritative {
		if placementReadAuthorized(candidate) {
			authorized = true
			break
		}
	}
	if !authorized {
		return loadPackFromCandidates(ctx, authoritative, handle, length, offset, fn)
	}
	if !manager.refreshPolicyForAccess(ctx) {
		return loadPackFromCandidates(ctx, authoritative, handle, length, offset, fn)
	}
	if len(authoritative) == 0 {
		return loadPackFromCandidates(ctx, authoritative, handle, length, offset, fn)
	}
	if length <= 0 || len(manager.tiers) == 0 || len(manager.key) == 0 {
		return loadPackFromCandidates(ctx, authoritative, handle, length, offset, fn)
	}
	if offset < 0 || int64(length) > math.MaxInt64-offset {
		return loadPackFromCandidates(ctx, authoritative, handle, length, offset, fn)
	}
	requestEnd := offset + int64(length)
	buffer := make([]byte, length)
	missing := []segment{{Offset: offset, Length: int64(length)}}
	packSize, havePackSize := statPackSize(ctx, authoritative, handle)
	if havePackSize && requestEnd > packSize {
		return loadPackFromCandidates(ctx, authoritative, handle, length, offset, fn)
	}
	for _, tier := range manager.sortedTiersByReadPriority() {
		missing = manager.readThroughTier(ctx, tier, packID, buffer, offset, missing, packSize, havePackSize)
		if len(missing) == 0 {
			break
		}
	}
	originRanges := missing
	if havePackSize {
		originRanges = manager.alignMissingPackRanges(missing, packSize)
	}
	fillCtx, cancelFill := context.WithTimeout(context.WithoutCancel(ctx), readCacheFillTimeout)
	defer cancelFill()
	for _, gap := range originRanges {
		gapBytes := make([]byte, gap.Length)
		if err := loadPackFromCandidates(ctx, authoritative, handle, int(gap.Length), gap.Offset, func(reader io.Reader) error {
			_, err := io.ReadFull(reader, gapBytes)
			return err
		}); err != nil {
			return err
		}
		copyStart := max(gap.Offset, offset)
		copyEnd := min(gap.Offset+gap.Length, requestEnd)
		if copyStart < copyEnd {
			copy(buffer[copyStart-offset:copyEnd-offset], gapBytes[copyStart-gap.Offset:copyEnd-gap.Offset])
		}
		if havePackSize {
			manager.storeAlignedPackRange(fillCtx, packID, gap.Offset, gapBytes, packSize)
		}
	}
	return fn(bytes.NewReader(buffer))
}

func statPackSize(ctx context.Context, candidates []placementReadCandidate, handle backend.Handle) (int64, bool) {
	for _, candidate := range candidates {
		if !placementReadAuthorized(candidate) {
			continue
		}
		info, err := candidate.backend.Stat(ctx, handle)
		if err == nil && info.Size >= 0 {
			return info.Size, true
		}
	}
	return 0, false
}

func (manager *readCacheManager) alignMissingPackRanges(missing []segment, packSize int64) []segment {
	aligned := make([]segment, 0, len(missing))
	for _, gap := range missing {
		start := gap.Offset
		end := min(packSize, gap.Offset+gap.Length)
		for _, tier := range manager.tiers {
			tier.mu.Lock()
			enabled, chunkSize := tier.enabled, tier.chunkSize
			tier.mu.Unlock()
			if !enabled {
				continue
			}
			if chunkSize <= 0 {
				chunkSize = readCacheDefaultChunk
			}
			chunkBytes := int64(chunkSize)
			start = min(start, (gap.Offset/chunkBytes)*chunkBytes)
			end = max(end, alignUpWithin(gap.Offset+gap.Length, chunkBytes, packSize))
		}
		if start < end {
			aligned = append(aligned, segment{Offset: start, Length: end - start})
		}
	}
	return mergeSegments(aligned)
}

func alignUpWithin(value, unit, bound int64) int64 {
	remainder := value % unit
	if remainder == 0 {
		return value
	}
	increment := unit - remainder
	if increment > bound-value {
		return bound
	}
	return value + increment
}

func (manager *readCacheManager) storeAlignedPackRange(
	ctx context.Context, packID vaultic.ID, rangeOffset int64, data []byte, packSize int64,
) {
	rangeEnd := rangeOffset + int64(len(data))
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		enabled, chunkSize := tier.enabled, tier.chunkSize
		tier.mu.Unlock()
		if !enabled {
			continue
		}
		if chunkSize <= 0 {
			chunkSize = readCacheDefaultChunk
		}
		chunkBytes := int64(chunkSize)
		chunkStart := ((rangeOffset + chunkBytes - 1) / chunkBytes) * chunkBytes
		for chunkStart < rangeEnd {
			chunkEnd := min(packSize, chunkStart+chunkBytes)
			if chunkEnd > rangeEnd {
				break
			}
			manager.storeRange(ctx, tier, packID, chunkStart, data[chunkStart-rangeOffset:chunkEnd-rangeOffset])
			chunkStart = chunkEnd
		}
	}
}

type segment struct {
	Offset int64
	Length int64
}

func (manager *readCacheManager) readThroughTier(
	ctx context.Context,
	tier *readCacheTier,
	packID vaultic.ID,
	dst []byte,
	baseOffset int64,
	missing []segment,
	packSize int64,
	havePackSize bool,
) []segment {
	tier.mu.Lock()
	enabled := tier.enabled
	chunkSize := tier.chunkSize
	tier.mu.Unlock()
	if !enabled {
		return missing
	}
	if chunkSize <= 0 {
		chunkSize = readCacheDefaultChunk
	}
	nextMissing := make([]segment, 0)
	for _, gap := range missing {
		end := gap.Offset + gap.Length
		pos := gap.Offset
		for pos < end {
			chunkStart := (pos / int64(chunkSize)) * int64(chunkSize)
			chunkEnd := chunkStart + int64(chunkSize)
			if havePackSize {
				chunkEnd = min(packSize, chunkEnd)
			}
			segmentEnd := min(end, chunkEnd)
			chunk, ok := manager.loadChunk(ctx, tier, packID, chunkStart, int(chunkEnd-chunkStart))
			manager.recordChunkCoverage(ok, uint64(max(0, int(segmentEnd-pos))))
			if !ok {
				nextMissing = append(nextMissing, segment{Offset: pos, Length: segmentEnd - pos})
				pos = segmentEnd
				continue
			}
			from := pos - chunkStart
			to := min(int64(len(chunk)), segmentEnd-chunkStart)
			if to <= from {
				nextMissing = append(nextMissing, segment{Offset: pos, Length: segmentEnd - pos})
				pos = segmentEnd
				continue
			}
			copy(dst[pos-baseOffset:pos-baseOffset+(to-from)], chunk[from:to])
			if to < segmentEnd-chunkStart {
				nextMissing = append(nextMissing, segment{Offset: chunkStart + to, Length: segmentEnd - (chunkStart + to)})
			}
			pos = segmentEnd
		}
	}
	return mergeSegments(nextMissing)
}

func mergeSegments(segments []segment) []segment {
	if len(segments) <= 1 {
		return segments
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].Offset < segments[j].Offset })
	merged := make([]segment, 0, len(segments))
	for _, current := range segments {
		if len(merged) == 0 {
			merged = append(merged, current)
			continue
		}
		last := &merged[len(merged)-1]
		lastEnd := last.Offset + last.Length
		if current.Offset <= lastEnd {
			last.Length = max(last.Length, current.Offset+current.Length-last.Offset)
			continue
		}
		merged = append(merged, current)
	}
	return merged
}

func (manager *readCacheManager) loadChunk(ctx context.Context, tier *readCacheTier, packID vaultic.ID, chunkOffset int64, length int) ([]byte, bool) {
	identity := manager.sourceRangeIdentity(tier, packID, chunkOffset, length)
	return manager.loadRepresentationFromTier(ctx, tier, identity)
}

//nolint:gocognit // Authentication, descriptor validation, and retirement remain explicit in one security-sensitive read path.
func (manager *readCacheManager) loadRepresentationFromTier(ctx context.Context, tier *readCacheTier, identity readCacheIdentity) ([]byte, bool) {
	key := tier.entryKey(identity)
	if !tier.allowProbe(key) {
		return nil, false
	}
	entry := tier.pinEntry(ctx, key)
	if entry == nil {
		return nil, false
	}
	defer tier.unpinEntry(ctx, entry)
	chunkCtx, cancel := context.WithTimeout(ctx, readCacheProbeTimeout)
	defer cancel()
	metaRaw, err := loadBytes(chunkCtx, tier.backend, entry.MetaHandle, 0, 0)
	if err != nil {
		tier.recordProbeFailure(key)
		if manager.probeLoadRetireFault(tier, key, err) {
			tier.retireEntry(ctx, entry.Key)
		}
		return nil, false
	}
	var meta readCacheChunkMeta
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	metaDescriptor := parseReadCacheHandleDescriptor(entry.MetaHandle.Name)
	dataDescriptor := parseReadCacheHandleDescriptor(entry.DataHandle.Name)
	if !readCacheDescriptorPairCompatible(metaDescriptor, dataDescriptor) {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	if metaDescriptor.HasGeneration {
		if meta.Generation == 0 || meta.Generation != metaDescriptor.Generation || entry.Generation != metaDescriptor.Generation {
			tier.recordProbeFailure(key)
			tier.retireEntry(ctx, entry.Key)
			return nil, false
		}
	} else if meta.Generation == 0 || entry.Generation != meta.Generation {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	commitOrder := meta.CommitOrder
	if metaDescriptor.HasCommitOrder {
		if commitOrder == 0 || commitOrder != metaDescriptor.CommitOrder {
			tier.recordProbeFailure(key)
			tier.retireEntry(ctx, entry.Key)
			return nil, false
		}
	}
	if commitOrder == 0 {
		commitOrder = readCacheEntryOrder(entry)
	}
	if commitOrder == 0 || readCacheEntryOrder(entry) != commitOrder {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	if !readCacheDescriptorMatchesIdentity(metaDescriptor, tier.id, meta.Identity, meta.Generation, commitOrder, key) {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	if !readCacheDescriptorMatchesIdentity(dataDescriptor, tier.id, meta.Identity, meta.Generation, commitOrder, key) {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	if !manager.validMeta(meta, key) || meta.Identity != identity {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	chunk, err := loadBytes(chunkCtx, tier.backend, entry.DataHandle, 0, 0)
	if err != nil {
		tier.recordProbeFailure(key)
		if manager.probeLoadRetireFault(tier, key, err) {
			tier.retireEntry(ctx, entry.Key)
		}
		return nil, false
	}
	if meta.DataEncrypted {
		chunk, err = manager.decryptDerivedChunk(meta, chunk)
		if err != nil {
			tier.recordProbeFailure(key)
			tier.retireEntry(ctx, entry.Key)
			return nil, false
		}
	}
	digest := sha256.Sum256(chunk)
	if hex.EncodeToString(digest[:]) != meta.DigestSHA256 {
		tier.recordProbeFailure(key)
		tier.retireEntry(ctx, entry.Key)
		return nil, false
	}
	tier.recordProbeSuccess(key)
	tier.touchEntry(entry.Key)
	return chunk, true
}

func (manager *readCacheManager) probeLoadRetireFault(tier *readCacheTier, key string, err error) bool {
	if err == nil || tier == nil || tier.backend == nil {
		return false
	}
	if tier.backend.IsNotExist(err) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	if !tier.backend.IsPermanentError(err) {
		return false
	}
	return tier.probeFailureCount(key) >= readCacheCircuitTrips
}

func (manager *readCacheManager) sortedTiersByReadPriority() []*readCacheTier {
	tiers := append([]*readCacheTier(nil), manager.tiers...)
	sort.SliceStable(tiers, func(i, j int) bool {
		tiers[i].mu.Lock()
		leftPrio := tiers[i].readPrio
		leftID := tiers[i].id
		tiers[i].mu.Unlock()
		tiers[j].mu.Lock()
		rightPrio := tiers[j].readPrio
		rightID := tiers[j].id
		tiers[j].mu.Unlock()
		if leftPrio == rightPrio {
			return leftID < rightID
		}
		return leftPrio < rightPrio
	})
	return tiers
}

func (manager *readCacheManager) sortedTiersByAdmissionPriority() []*readCacheTier {
	tiers := append([]*readCacheTier(nil), manager.tiers...)
	sort.SliceStable(tiers, func(i, j int) bool {
		tiers[i].mu.Lock()
		leftPrio, leftID := tiers[i].admitPrio, tiers[i].id
		tiers[i].mu.Unlock()
		tiers[j].mu.Lock()
		rightPrio, rightID := tiers[j].admitPrio, tiers[j].id
		tiers[j].mu.Unlock()
		if leftPrio == rightPrio {
			return leftID < rightID
		}
		return leftPrio < rightPrio
	})
	return tiers
}

func (manager *readCacheManager) loadRepresentationFromAnyTier(ctx context.Context, identityForTier func(*readCacheTier) readCacheIdentity) ([]byte, bool) {
	for _, tier := range manager.sortedTiersByReadPriority() {
		tier.mu.Lock()
		enabled := tier.enabled
		tier.mu.Unlock()
		if !enabled {
			continue
		}
		identity := identityForTier(tier)
		if chunk, ok := manager.loadRepresentationFromTier(ctx, tier, identity); ok {
			return chunk, true
		}
	}
	return nil, false
}

func (manager *readCacheManager) storeRange(ctx context.Context, tier *readCacheTier, packID vaultic.ID, chunkOffset int64, chunk []byte) {
	snapshot, ok := manager.captureAdmissionSnapshot(tier)
	if !ok {
		return
	}
	identity := manager.sourceRangeIdentityFromSnapshot(snapshot, packID, chunkOffset, len(chunk))
	manager.storeRepresentationWithSnapshot(ctx, tier, snapshot, identity, chunk)
}

func (manager *readCacheManager) storeRepresentation(ctx context.Context, tier *readCacheTier, identity readCacheIdentity, chunk []byte) {
	snapshot, ok := manager.captureAdmissionSnapshot(tier)
	if !ok {
		return
	}
	manager.storeRepresentationWithSnapshot(ctx, tier, snapshot, identity, chunk)
}

//nolint:funlen // Admission, encryption, publication, and rollback ordering must remain visible in this transactional path.
func (manager *readCacheManager) storeRepresentationWithSnapshot(
	ctx context.Context,
	tier *readCacheTier,
	snapshot readCacheAdmissionSnapshot,
	identity readCacheIdentity,
	chunk []byte,
) {
	if !snapshot.Tier.Enabled || !snapshot.Tier.Ingest || len(chunk) == 0 {
		return
	}
	if identity.RepositoryID == "" {
		identity.RepositoryID = manager.repoID
	}
	if identity.Representation == "" {
		identity.Representation = readCacheRepSourceEncryptedRange
	}
	if identity.LayoutID == "" {
		identity.LayoutID = "pack-range"
	}
	if identity.Codec == "" {
		identity.Codec = snapshot.Tier.Codec
	}
	if identity.ChunkGeometry <= 0 {
		identity.ChunkGeometry = snapshot.Tier.ChunkSize
	}
	if identity.KeyGeneration == 0 {
		identity.KeyGeneration = max(1, snapshot.Tier.Generation)
	}
	identity.Trust = snapshot.Tier.Trust
	identity.Length = len(chunk)
	key := tier.entryKey(identity)
	if !tier.allowAdmission() {
		return
	}
	entrySize := uint64(len(chunk))
	if entrySize > snapshot.Tier.MaxBytes {
		return
	}
	gate, release := manager.acquireFillGate(key)
	gate.mu.Lock()
	defer func() {
		gate.mu.Unlock()
		release()
	}()
	if tier.lookupEntry(key) != nil {
		return
	}
	meta := readCacheChunkMeta{
		Format:        readCacheFormatV2,
		RepositoryID:  manager.repoID,
		TierID:        snapshot.Tier.ID,
		Identity:      identity,
		PackID:        identity.ContentID,
		Offset:        identity.Offset,
		Length:        len(chunk),
		ChunkSize:     snapshot.Tier.ChunkSize,
		Trust:         snapshot.Tier.Trust,
		CreatedUnixMS: time.Now().UTC().UnixMilli(),
	}
	digest := sha256.Sum256(chunk)
	meta.DigestSHA256 = hex.EncodeToString(digest[:])
	storageBytes := uint64(len(chunk)) + 512
	reserveBytes := 2 * storageBytes
	if !manager.beginInflight(reserveBytes) {
		return
	}
	defer manager.endInflight(reserveBytes)
	lease, ok, _ := manager.reserveAdmissionWithSnapshot(ctx, snapshot, identity, key, reserveBytes)
	if !ok {
		return
	}
	defer manager.releaseAdmission(ctx, lease)
	if !tier.reserveAndReclaim(ctx, reserveBytes) {
		return
	}
	defer tier.releaseReservation(reserveBytes)
	meta.Generation = lease.Generation
	meta.CommitOrder = lease.CommitOrder
	storeBytes := chunk
	if snapshot.Tier.Trust == readCacheTrustEncrypted && identity.Representation != readCacheRepSourceEncryptedRange {
		sealed, nonce, aadHash, err := manager.encryptDerivedChunk(meta, chunk)
		if err != nil {
			return
		}
		storeBytes = sealed
		meta.DataEncrypted = true
		meta.DataNonce = hex.EncodeToString(nonce)
		meta.CiphertextLength = len(sealed)
		meta.AssociatedDataHash = aadHash
	}
	meta.Signature = manager.signMeta(meta, key)
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return
	}
	dataHandle, metaHandle := tier.handlesForIdentity(key, identity, lease.Generation, lease.CommitOrder)
	group := identity.LayoutID + "|" + identity.ContentID
	entry, err := tier.saveEntry(
		ctx, key, dataHandle, metaHandle, storeBytes, metaBytes, lease.Generation, lease.CommitOrder,
		identity.KeyGeneration, snapshot.Tier.Trust, identity.Representation, group,
	)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			lease.persisted = true
		}
		debug.Log("read-cache tier %s store failed: %v", tier.id, err)
		tier.recordFillFailure(false)
		return
	}
	if entry == nil {
		return
	}
	lease.persisted = true
	lease.Bytes = saturatingAddUint64(entry.Size, entry.MetaSize)
	tier.recordFillSuccess()
	committed, commitErr := manager.commitAdmission(ctx, lease)
	if !committed {
		if commitErr != nil {
			debug.Log("read-cache commit admission failed: %v", commitErr)
		}
		tier.retireEntry(ctx, entry.Key)
		return
	}
	published := manager.publishAdmission(ctx, lease)
	if !published {
		manager.transitionAdmissionToDeleting(ctx, lease, readCacheStatePublishing)
		tier.retireEntry(ctx, entry.Key)
		return
	}
	tier.publishEntry(entry.Key, entry.Generation, entry.CommitOrder)
}

func (manager *readCacheManager) sourceRangeIdentity(tier *readCacheTier, packID vaultic.ID, chunkOffset int64, length int) readCacheIdentity {
	snapshot, ok := manager.captureAdmissionSnapshot(tier)
	if !ok {
		return readCacheIdentity{}
	}
	return manager.sourceRangeIdentityFromSnapshot(snapshot, packID, chunkOffset, length)
}

func (manager *readCacheManager) sourceRangeIdentityFromSnapshot(
	snapshot readCacheAdmissionSnapshot,
	packID vaultic.ID,
	chunkOffset int64,
	length int,
) readCacheIdentity {
	keyGeneration := snapshot.Tier.Generation
	if keyGeneration == 0 {
		keyGeneration = 1
	}
	return readCacheIdentity{
		RepositoryID:   manager.repoID,
		Representation: readCacheRepSourceEncryptedRange,
		ContentID:      packID.String(),
		LayoutID:       "pack-range",
		Offset:         chunkOffset,
		Length:         length,
		FormatVersion:  readCacheFormatV2,
		Codec:          snapshot.Tier.Codec,
		Trust:          snapshot.Tier.Trust,
		ChunkGeometry:  snapshot.Tier.ChunkSize,
		KeyGeneration:  keyGeneration,
	}
}

func (manager *readCacheManager) captureAdmissionSnapshot(tier *readCacheTier) (readCacheAdmissionSnapshot, bool) {
	if manager == nil || tier == nil {
		return readCacheAdmissionSnapshot{}, false
	}
	manager.mu.Lock()
	managerSnapshot := readCacheManagerAdmissionSnapshot{
		PolicyRevision: manager.policyRevision,
		AggregateMax:   manager.aggregateMax,
		Admissions:     manager.admissions,
		FillAllowed:    manager.capacityDecision.FillAllowed,
	}
	tier.mu.Lock()
	tierSnapshot := readCacheTierAdmissionSnapshot{
		ID:         tier.id,
		Generation: max(1, tier.generation),
		Enabled:    tier.enabled,
		Ingest:     tier.ingest,
		Trust:      tier.trust,
		TrustAck:   tier.trustAck,
		Codec:      tier.codec,
		ReadPrio:   tier.readPrio,
		AdmitPrio:  tier.admitPrio,
		IdleAge:    tier.idleAge,
		AbsAge:     tier.absAge,
		ChunkSize:  tier.chunkSize,
		MaxBytes:   tier.maxBytes,
	}
	tier.mu.Unlock()
	manager.mu.Unlock()
	if !managerSnapshot.FillAllowed {
		managerSnapshot.FillAllowed = manager.capacityController == nil
	}
	return readCacheAdmissionSnapshot{Manager: managerSnapshot, Tier: tierSnapshot}, true
}

func (manager *readCacheManager) blobIdentity(tier *readCacheTier, blob vaultic.BlobHandle, length int, representation string) readCacheIdentity {
	snapshot, ok := manager.captureAdmissionSnapshot(tier)
	if !ok {
		return readCacheIdentity{}
	}
	keyGeneration := snapshot.Tier.Generation
	if keyGeneration == 0 {
		keyGeneration = 1
	}
	return readCacheIdentity{
		RepositoryID:   manager.repoID,
		Representation: representation,
		ContentID:      fmt.Sprintf("blob:%d:%s", blob.Type, blob.ID.String()),
		LayoutID:       "blob-extent",
		Offset:         0,
		Length:         length,
		FormatVersion:  readCacheFormatV2,
		Codec:          snapshot.Tier.Codec,
		Trust:          snapshot.Tier.Trust,
		ChunkGeometry:  snapshot.Tier.ChunkSize,
		KeyGeneration:  keyGeneration,
	}
}

func logicalLayoutIdentity(content []vaultic.ID, cumSize []uint64) (string, string, int) {
	mac := sha256.New()
	mac.Write([]byte("vaultic-logical-layout-v1"))
	total := 0
	if len(cumSize) != 0 {
		total = int(cumSize[len(cumSize)-1])
	}
	for i, id := range content {
		mac.Write(id[:])
		if i+1 < len(cumSize) {
			size := cumSize[i+1] - cumSize[i]
			mac.Write([]byte(strconv.FormatUint(size, 10)))
		}
	}
	digest := hex.EncodeToString(mac.Sum(nil))
	return digest, "logical-file:" + digest, total
}

func (manager *readCacheManager) logicalWholeIdentity(tier *readCacheTier, content []vaultic.ID, cumSize []uint64) readCacheIdentity {
	contentID, layoutID, total := logicalLayoutIdentity(content, cumSize)
	snapshot, ok := manager.captureAdmissionSnapshot(tier)
	if !ok {
		return readCacheIdentity{}
	}
	keyGeneration := snapshot.Tier.Generation
	if keyGeneration == 0 {
		keyGeneration = 1
	}
	return readCacheIdentity{
		RepositoryID:   manager.repoID,
		Representation: readCacheRepWholeFile,
		ContentID:      contentID,
		LayoutID:       layoutID,
		Offset:         0,
		Length:         total,
		FormatVersion:  readCacheFormatV2,
		Codec:          snapshot.Tier.Codec,
		Trust:          snapshot.Tier.Trust,
		ChunkGeometry:  snapshot.Tier.ChunkSize,
		KeyGeneration:  keyGeneration,
	}
}

func (manager *readCacheManager) logicalChunkIdentity(tier *readCacheTier, content []vaultic.ID, cumSize []uint64, offset int64, length int) readCacheIdentity {
	contentID, layoutID, _ := logicalLayoutIdentity(content, cumSize)
	snapshot, ok := manager.captureAdmissionSnapshot(tier)
	if !ok {
		return readCacheIdentity{}
	}
	keyGeneration := snapshot.Tier.Generation
	if keyGeneration == 0 {
		keyGeneration = 1
	}
	return readCacheIdentity{
		RepositoryID:   manager.repoID,
		Representation: readCacheRepDecodedExtent,
		ContentID:      contentID,
		LayoutID:       layoutID,
		Offset:         offset,
		Length:         length,
		FormatVersion:  readCacheFormatV2,
		Codec:          snapshot.Tier.Codec,
		Trust:          snapshot.Tier.Trust,
		ChunkGeometry:  snapshot.Tier.ChunkSize,
		KeyGeneration:  keyGeneration,
	}
}

func (manager *readCacheManager) signMeta(meta readCacheChunkMeta, key string) string {
	mac := hmac.New(sha256.New, manager.key)
	meta.Identity.RepositoryID = manager.repoID
	if meta.Identity.Representation == "" {
		meta.Identity = readCacheIdentity{
			RepositoryID:   manager.repoID,
			Representation: readCacheRepSourceEncryptedRange,
			ContentID:      meta.PackID,
			LayoutID:       "pack-range",
			Offset:         meta.Offset,
			Length:         meta.Length,
			FormatVersion:  readCacheFormatV1,
			Codec:          "raw",
			Trust:          meta.Trust,
			ChunkGeometry:  meta.ChunkSize,
			KeyGeneration:  meta.Generation,
		}
	}
	mac.Write([]byte(key))
	mac.Write([]byte(meta.RepositoryID))
	mac.Write([]byte(meta.TierID))
	mac.Write([]byte(meta.Identity.RepositoryID))
	mac.Write([]byte(meta.Identity.Representation))
	mac.Write([]byte(meta.Identity.ContentID))
	mac.Write([]byte(meta.Identity.LayoutID))
	mac.Write([]byte(strconv.FormatInt(meta.Identity.Offset, 10)))
	mac.Write([]byte(strconv.Itoa(meta.Identity.Length)))
	mac.Write([]byte(strconv.FormatUint(uint64(meta.Identity.FormatVersion), 10)))
	mac.Write([]byte(meta.Identity.Codec))
	mac.Write([]byte(meta.Identity.Trust))
	mac.Write([]byte(strconv.Itoa(meta.Identity.ChunkGeometry)))
	mac.Write([]byte(strconv.FormatUint(meta.Identity.KeyGeneration, 10)))
	mac.Write([]byte(strconv.FormatUint(meta.Generation, 10)))
	if meta.CommitOrder != 0 {
		mac.Write([]byte(strconv.FormatUint(meta.CommitOrder, 10)))
	}
	mac.Write([]byte{':'})
	mac.Write([]byte(meta.DigestSHA256))
	mac.Write([]byte{':'})
	mac.Write([]byte(strconv.FormatBool(meta.DataEncrypted)))
	mac.Write([]byte{':'})
	mac.Write([]byte(meta.AssociatedDataHash))
	mac.Write([]byte{':'})
	mac.Write([]byte(strconv.Itoa(meta.CiphertextLength)))
	mac.Write([]byte{':'})
	mac.Write([]byte(strconv.FormatInt(meta.CreatedUnixMS, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

func (manager *readCacheManager) validMeta(meta readCacheChunkMeta, key string) bool {
	if (meta.Format != readCacheFormatV1 && meta.Format != readCacheFormatV2) || meta.RepositoryID != manager.repoID || meta.Length <= 0 {
		return false
	}
	if meta.Identity.RepositoryID == "" {
		meta.Identity = readCacheIdentity{
			RepositoryID:   meta.RepositoryID,
			Representation: readCacheRepSourceEncryptedRange,
			ContentID:      meta.PackID,
			LayoutID:       "pack-range",
			Offset:         meta.Offset,
			Length:         meta.Length,
			FormatVersion:  meta.Format,
			Codec:          "raw",
			Trust:          meta.Trust,
			ChunkGeometry:  meta.ChunkSize,
			KeyGeneration:  meta.Generation,
		}
	}
	if meta.Identity.Trust == "" || meta.Identity.Representation == "" || meta.Identity.Length <= 0 || meta.Identity.ChunkGeometry <= 0 {
		return false
	}
	return hmac.Equal([]byte(meta.Signature), []byte(manager.signMeta(meta, key)))
}

func sanitizeCachePathSegment(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "unknown"
	}
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.ReplaceAll(value, "\\", "_")
	value = strings.ReplaceAll(value, " ", "_")
	return value
}

func isReadCacheControlPath(name string) bool {
	return strings.HasPrefix(name, readCacheControlPrefix)
}

func parseReadCacheHandleDescriptor(name string) readCacheHandleDescriptor {
	descriptor := readCacheHandleDescriptor{}
	prefix := readCacheNamespace + "/chunks/"
	if !strings.HasPrefix(name, prefix) {
		descriptor.IsLegacy = strings.HasPrefix(name, readCacheNamespace+"/")
		return descriptor
	}
	descriptor.IsChunk = true
	parts := strings.Split(strings.TrimPrefix(name, prefix), "/")
	if len(parts) != 8 {
		return descriptor
	}
	descriptor.RepositoryScope = parts[0]
	descriptor.TierID = parts[1]
	descriptor.Representation = parts[2]
	descriptor.Trust = parts[3]

	kgPart := parts[4]
	if !strings.HasPrefix(kgPart, "kg-") {
		return descriptor
	}
	keyGeneration, keyErr := strconv.ParseUint(strings.TrimPrefix(kgPart, "kg-"), 10, 64)
	if keyErr != nil || keyGeneration == 0 {
		return descriptor
	}
	descriptor.KeyGeneration = keyGeneration
	descriptor.HasKeyGeneration = true

	genPart := parts[5]
	if !strings.HasPrefix(genPart, "g-") {
		return descriptor
	}
	generation, err := strconv.ParseUint(strings.TrimPrefix(genPart, "g-"), 10, 64)
	if err != nil || generation == 0 {
		return descriptor
	}
	descriptor.Generation = generation
	descriptor.HasGeneration = true

	orderPart := parts[6]
	if !strings.HasPrefix(orderPart, "o-") {
		return descriptor
	}
	commitOrder, orderErr := strconv.ParseUint(strings.TrimPrefix(orderPart, "o-"), 10, 64)
	if orderErr != nil || commitOrder == 0 {
		return descriptor
	}
	descriptor.CommitOrder = commitOrder
	descriptor.HasCommitOrder = true

	digestPart := parts[7]
	base, ext, ok := strings.Cut(digestPart, ".")
	if !ok || len(base) != 64 || ext == "" {
		return descriptor
	}
	for _, char := range base {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return descriptor
		}
	}
	descriptor.DigestSuffix = base
	descriptor.Extension = ext
	return descriptor
}

func readCacheDescriptorPairCompatible(metaDescriptor readCacheHandleDescriptor, dataDescriptor readCacheHandleDescriptor) bool {
	if !metaDescriptor.IsChunk || !dataDescriptor.IsChunk {
		return false
	}
	if !metaDescriptor.HasKeyGeneration || !metaDescriptor.HasGeneration || !metaDescriptor.HasCommitOrder {
		return false
	}
	if !dataDescriptor.HasKeyGeneration || !dataDescriptor.HasGeneration || !dataDescriptor.HasCommitOrder {
		return false
	}
	if metaDescriptor.Extension != "json" || dataDescriptor.Extension != "bin" {
		return false
	}
	if metaDescriptor.RepositoryScope != dataDescriptor.RepositoryScope ||
		metaDescriptor.TierID != dataDescriptor.TierID ||
		metaDescriptor.Representation != dataDescriptor.Representation ||
		metaDescriptor.Trust != dataDescriptor.Trust ||
		metaDescriptor.KeyGeneration != dataDescriptor.KeyGeneration ||
		metaDescriptor.Generation != dataDescriptor.Generation ||
		metaDescriptor.CommitOrder != dataDescriptor.CommitOrder ||
		metaDescriptor.DigestSuffix != dataDescriptor.DigestSuffix {
		return false
	}
	return true
}

func readCacheDescriptorMatchesIdentity(
	descriptor readCacheHandleDescriptor,
	tierID string,
	identity readCacheIdentity,
	generation uint64,
	commitOrder uint64,
	entryKey string,
) bool {
	if !descriptor.IsChunk || !descriptor.HasKeyGeneration || !descriptor.HasGeneration || !descriptor.HasCommitOrder {
		return false
	}
	if descriptor.RepositoryScope != readCacheRepositoryScope(identity.RepositoryID) {
		return false
	}
	if descriptor.TierID != sanitizeCachePathSegment(tierID) {
		return false
	}
	if descriptor.Representation != sanitizeCachePathSegment(identity.Representation) {
		return false
	}
	if descriptor.Trust != sanitizeCachePathSegment(identity.Trust) {
		return false
	}
	if descriptor.KeyGeneration != identity.KeyGeneration || descriptor.Generation != generation || descriptor.CommitOrder != commitOrder {
		return false
	}
	digest := sha256.Sum256([]byte(entryKey))
	return descriptor.DigestSuffix == hex.EncodeToString(digest[:])
}

func (manager *readCacheManager) startCapacityWorker() {
	if manager == nil || manager.capacityController == nil || manager.capacityController.opts.Mode == readCacheBudgetModeFixed {
		return
	}
	manager.mu.Lock()
	if manager.capacityStop != nil {
		manager.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	manager.capacityStop = cancel
	manager.capacityDone = done
	manager.mu.Unlock()

	go func() {
		defer close(done)
		interval := manager.capacityController.opts.TelemetryPollInterval
		if interval <= 0 {
			interval = 5 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		manager.refreshCapacityBudget(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				manager.refreshCapacityBudget(ctx)
			}
		}
	}()
}

func (manager *readCacheManager) stopCapacityWorker() {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	stop := manager.capacityStop
	done := manager.capacityDone
	manager.capacityStop = nil
	manager.capacityDone = nil
	manager.mu.Unlock()
	if stop != nil {
		stop()
	}
	if done != nil {
		timer := time.NewTimer(readCacheCapacityStopTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			debug.Log("read-cache capacity worker stop timed out")
		}
	}
}

func (manager *readCacheManager) startPolicyWorker() {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	if manager.policyStop != nil {
		manager.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	wake := make(chan struct{}, 1)
	manager.policyStop = cancel
	manager.policyDone = done
	manager.policyWake = wake
	manager.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(readCacheAgeSweepInterval)
		defer ticker.Stop()
		manager.enforcePolicyRetirement(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-wake:
				manager.enforcePolicyRetirement(ctx)
			case <-ticker.C:
				manager.enforcePolicyRetirement(ctx)
			}
		}
	}()
}

func (manager *readCacheManager) stopPolicyWorker() {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	stop := manager.policyStop
	done := manager.policyDone
	manager.policyStop = nil
	manager.policyDone = nil
	manager.policyWake = nil
	manager.mu.Unlock()
	if stop != nil {
		stop()
	}
	if done != nil {
		timer := time.NewTimer(readCacheCapacityStopTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			debug.Log("read-cache policy worker stop timed out")
		}
	}
}

func (manager *readCacheManager) schedulePolicyRetirement() {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	wake := manager.policyWake
	manager.mu.Unlock()
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (manager *readCacheManager) acquireFillGate(key string) (*readCacheFillGate, func()) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	gate, ok := manager.coalesced[key]
	if !ok {
		gate = &readCacheFillGate{}
		manager.coalesced[key] = gate
	} else {
		manager.coalescedWaits++
	}
	gate.refs++
	return gate, func() {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		gate.refs--
		if gate.refs == 0 {
			delete(manager.coalesced, key)
		}
	}
}

func (manager *readCacheManager) recordChunkCoverage(available bool, logicalBytes uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if available {
		manager.availableChunks++
		manager.availableBytes += logicalBytes
		return
	}
	manager.unavailableChunks++
	manager.unavailableBytes += logicalBytes
}

func (manager *readCacheManager) beginInflight(bytes uint64) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if !manager.admissions {
		return false
	}
	if manager.inflightTasks >= readCacheMaxInflightOps {
		return false
	}
	if sumExceedsUint64(manager.maxInflightBytes, manager.inflightBytes, bytes) {
		return false
	}
	manager.inflightTasks++
	manager.inflightBytes = saturatingAddUint64(manager.inflightBytes, bytes)
	return true
}

func (manager *readCacheManager) endInflight(bytes uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.inflightTasks > 0 {
		manager.inflightTasks--
	}
	if manager.inflightBytes >= bytes {
		manager.inflightBytes -= bytes
	} else {
		manager.inflightBytes = 0
	}
}

func (manager *readCacheManager) capacityMode() string {
	if manager.capacityController == nil {
		return readCacheBudgetModeFixed
	}
	return manager.capacityController.opts.Mode
}

func (manager *readCacheManager) capacityDecisionSnapshot() readCacheCapacityDecision {
	manager.mu.Lock()
	decision := manager.capacityDecision
	manager.mu.Unlock()
	if decision.TelemetryState != "" {
		return decision
	}
	if manager.capacityController == nil {
		return readCacheCapacityDecision{
			EffectiveLogicalBytes: manager.aggregateMax,
			FillAllowed:           manager.admissions,
			TelemetryState:        readCacheTelemetryStateFixed,
			Health:                readCacheHealthHealthy,
		}
	}
	return manager.capacityController.snapshot(time.Now().UTC())
}

func (manager *readCacheManager) fillAllowed() bool {
	decision := manager.capacityDecisionSnapshot()
	manager.mu.Lock()
	admissions := manager.admissions
	manager.mu.Unlock()
	return admissions && decision.FillAllowed
}

func (manager *readCacheManager) refreshCapacityBudget(ctx context.Context) {
	if manager.capacityController == nil {
		return
	}
	now := time.Now().UTC()
	pollCtx := ctx
	if manager.capacityController.opts.Mode != readCacheBudgetModeFixed {
		timeout := manager.capacityController.opts.TelemetryPollInterval
		if timeout <= 0 || timeout > readCacheTelemetryPollTimeout {
			timeout = readCacheTelemetryPollTimeout
		}
		var cancel context.CancelFunc
		pollCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	cacheRaw, reservedRaw := manager.capacityRawInputs(ctx)
	mode := manager.capacityController.opts.Mode
	if mode == readCacheBudgetModeFixed {
		decision := manager.capacityController.maybeRefresh(pollCtx, now, cacheRaw, reservedRaw)
		manager.mu.Lock()
		manager.capacityDecision = decision
		manager.mu.Unlock()
		manager.applyEffectiveBudget(decision.EffectiveLogicalBytes)
		manager.syncEffectiveBudgetPolicy(ctx)
		return
	}
	decision := manager.capacityController.maybeRefresh(pollCtx, now, cacheRaw, reservedRaw)
	manager.mu.Lock()
	manager.capacityDecision = decision
	manager.mu.Unlock()
	manager.applyEffectiveBudget(decision.EffectiveLogicalBytes)
	manager.syncEffectiveBudgetPolicy(ctx)
}

func (manager *readCacheManager) capacityRawInputs(ctx context.Context) (uint64, uint64) {
	manager.mu.Lock()
	reservedRaw := manager.inflightBytes
	manager.mu.Unlock()

	localRaw := uint64(0)
	for _, tier := range manager.tiers {
		tier.mu.Lock()
		localRaw = saturatingAddUint64(localRaw, tier.used)
		localRaw = saturatingAddUint64(localRaw, tier.reserved)
		tier.mu.Unlock()
	}

	cacheRaw := localRaw
	if _, ledger, err := manager.loadQuota(ctx); err == nil && ledger != nil {
		_, _, aggregateUsed, aggregateReserved := quotaUsage(ledger)
		sharedRaw := saturatingAddUint64(aggregateUsed, aggregateReserved)
		if sharedRaw > cacheRaw {
			cacheRaw = sharedRaw
		}
	}
	return cacheRaw, reservedRaw
}

func (manager *readCacheManager) applyEffectiveBudget(limit uint64) {
	manager.mu.Lock()
	requestedAggregate := manager.requestedAggregate
	manager.mu.Unlock()
	if requestedAggregate == 0 {
		return
	}
	if limit > requestedAggregate {
		limit = requestedAggregate
	}
	type perTierLimit struct {
		tier  *readCacheTier
		limit uint64
	}
	limits := make([]perTierLimit, 0, len(manager.tiers))
	remaining := limit
	assigned := uint64(0)
	for i, tier := range manager.tiers {
		tier.mu.Lock()
		tierMax := tier.requested
		tier.mu.Unlock()
		tierBudget := uint64(0)
		if i == len(manager.tiers)-1 {
			if remaining > tierMax {
				tierBudget = tierMax
			} else {
				tierBudget = remaining
			}
		} else if requestedAggregate > 0 {
			tierBudget = mulDivFloorUint64(limit, tierMax, requestedAggregate)
			if tierBudget > tierMax {
				tierBudget = tierMax
			}
		}
		if tierBudget > remaining {
			tierBudget = remaining
		}
		if remaining >= tierBudget {
			remaining -= tierBudget
		} else {
			remaining = 0
		}
		assigned = saturatingAddUint64(assigned, tierBudget)
		limits = append(limits, perTierLimit{tier: tier, limit: tierBudget})
	}
	if assigned < limit {
		for i := range limits {
			if assigned >= limit {
				break
			}
			limits[i].tier.mu.Lock()
			tierRequested := limits[i].tier.requested
			limits[i].tier.mu.Unlock()
			headroom := tierRequested - limits[i].limit
			if headroom == 0 {
				continue
			}
			grant := min(headroom, limit-assigned)
			limits[i].limit = saturatingAddUint64(limits[i].limit, grant)
			assigned = saturatingAddUint64(assigned, grant)
		}
	}
	for _, item := range limits {
		item.tier.mu.Lock()
		item.tier.maxBytes = item.limit
		item.tier.mu.Unlock()
	}
	manager.mu.Lock()
	manager.aggregateMax = assigned
	if assigned == 0 {
		manager.maxInflightBytes = 0
	} else {
		manager.maxInflightBytes = max(16*1024*1024, assigned/8)
	}
	manager.mu.Unlock()
}

func mulDivFloorUint64(left, right, divisor uint64) uint64 {
	high, low := bits.Mul64(left, right)
	quotient, _ := bits.Div64(high, low, divisor)
	return quotient
}

func (tier *readCacheTier) entryKey(identity readCacheIdentity) string {
	return fmt.Sprintf("%s:%s:%s:%s:%d:%d:%d:%s:%s:%d:%d",
		tier.id,
		identity.RepositoryID,
		identity.Representation,
		identity.ContentID,
		identity.Offset,
		identity.Length,
		identity.FormatVersion,
		identity.Codec,
		identity.Trust,
		identity.ChunkGeometry,
		identity.KeyGeneration,
	)
}

func (tier *readCacheTier) handlesForIdentity(key string, identity readCacheIdentity, generation uint64, commitOrder uint64) (backend.Handle, backend.Handle) {
	digest := sha256.Sum256([]byte(key))
	ns := fmt.Sprintf("%s/chunks/%s/%s/%s/%s/kg-%d/g-%d/o-%d/%s",
		readCacheNamespace,
		readCacheRepositoryScope(identity.RepositoryID),
		sanitizeCachePathSegment(tier.id),
		sanitizeCachePathSegment(identity.Representation),
		sanitizeCachePathSegment(identity.Trust),
		identity.KeyGeneration,
		generation,
		commitOrder,
		hex.EncodeToString(digest[:]),
	)
	return backend.Handle{Type: backend.StagingFile, Name: ns + ".bin"}, backend.Handle{Type: backend.StagingFile, Name: ns + ".json"}
}

func readCacheRepositoryScope(repositoryID string) string {
	digest := sha256.Sum256([]byte(repositoryID))
	return hex.EncodeToString(digest[:])
}

func (tier *readCacheTier) lookupEntry(key string) *readCacheEntry {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	entry, ok := tier.entries[key]
	if !ok || entry.Retiring || !entry.Published {
		return nil
	}
	return entry
}

func (tier *readCacheTier) pinEntry(ctx context.Context, key string) *readCacheEntry {
	tier.mu.Lock()
	entry, ok := tier.entries[key]
	if !ok || entry.Retiring || !entry.Published {
		tier.mu.Unlock()
		return nil
	}
	if tier.entryExpiredLocked(entry, time.Now().UTC()) {
		tier.markRetiringLocked(entry)
		tier.mu.Unlock()
		tier.deleteRetiringEntry(ctx, entry)
		return nil
	}
	entry.Pins++
	entryBytes := saturatingAddUint64(entry.Size, entry.MetaSize)
	tier.pinned = saturatingAddUint64(tier.pinned, entryBytes)
	tier.mu.Unlock()
	return entry
}

func (tier *readCacheTier) entryExpiredLocked(entry *readCacheEntry, now time.Time) bool {
	return tier.idleAge > 0 && !entry.LastAccess.IsZero() && now.Sub(entry.LastAccess) >= tier.idleAge ||
		tier.absAge > 0 && !entry.CreatedAt.IsZero() && now.Sub(entry.CreatedAt) >= tier.absAge
}

func (tier *readCacheTier) publishEntry(key string, generation uint64, commitOrder uint64) bool {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	entry, ok := tier.entries[key]
	if !ok || entry.Retiring || entry.Generation != generation || readCacheEntryOrder(entry) != commitOrder {
		return false
	}
	entry.Published = true
	return true
}

func (tier *readCacheTier) unpinEntry(ctx context.Context, entry *readCacheEntry) {
	tier.mu.Lock()
	if entry.Pins > 0 {
		entry.Pins--
	}
	entryBytes := entry.Size + entry.MetaSize
	if tier.pinned >= entryBytes {
		tier.pinned -= entryBytes
	} else {
		tier.pinned = 0
	}
	deleteNow := entry.Retiring && entry.Pins == 0
	tier.mu.Unlock()
	if deleteNow {
		deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readCacheFillTimeout)
		defer cancel()
		tier.deleteRetiringEntry(deleteCtx, entry)
	}
}

func (tier *readCacheTier) touchEntry(key string) {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	entry, ok := tier.entries[key]
	if !ok {
		return
	}
	entry.LastAccess = time.Now().UTC()
	entry.Hits++
	entry.UsefulBytes = saturatingAddUint64(entry.UsefulBytes, uint64(entry.Size))
	for i, existing := range tier.order {
		if existing == key {
			tier.order = append(tier.order[:i], tier.order[i+1:]...)
			break
		}
	}
	tier.order = append(tier.order, key)
}

func (tier *readCacheTier) saveEntry(
	ctx context.Context,
	key string,
	dataHandle, metaHandle backend.Handle,
	data, meta []byte,
	generation uint64,
	commitOrder uint64,
	keyGeneration uint64,
	trust string,
	representation string,
	group string,
) (*readCacheEntry, error) {
	entryBytes := uint64(len(data) + len(meta))
	tier.mu.Lock()
	if existing, exists := tier.entries[key]; exists && readCacheEntryOrder(existing) >= commitOrder {
		tier.mu.Unlock()
		return nil, nil
	}
	for _, deleting := range tier.deleting {
		if deleting.Key == key && readCacheEntryOrder(deleting) >= commitOrder {
			tier.mu.Unlock()
			return nil, nil
		}
	}
	tier.mu.Unlock()
	if err := tier.backend.Save(ctx, dataHandle, backend.NewByteReader(data, tier.backend.Hasher())); err != nil {
		return nil, err
	}
	if err := tier.backend.Save(ctx, metaHandle, backend.NewByteReader(meta, tier.backend.Hasher())); err != nil {
		if removeErr := tier.backend.Remove(ctx, dataHandle); removeErr != nil && !tier.backend.IsNotExist(removeErr) {
			debug.Log("read-cache rollback remove failed for %s: %v", tier.id, removeErr)
		}
		return nil, err
	}
	tier.mu.Lock()
	if existing, exists := tier.entries[key]; exists && readCacheEntryOrder(existing) >= commitOrder {
		tier.mu.Unlock()
		if err := tier.backend.Remove(ctx, dataHandle); err != nil && !tier.backend.IsNotExist(err) {
			debug.Log("read-cache conflict cleanup failed for %s data: %v", tier.id, err)
		}
		if err := tier.backend.Remove(ctx, metaHandle); err != nil && !tier.backend.IsNotExist(err) {
			debug.Log("read-cache conflict cleanup failed for %s meta: %v", tier.id, err)
		}
		return nil, nil
	}
	entry := &readCacheEntry{
		Key:            key,
		DataHandle:     dataHandle,
		MetaHandle:     metaHandle,
		Size:           uint64(len(data)),
		MetaSize:       uint64(len(meta)),
		Generation:     generation,
		KeyGeneration:  keyGeneration,
		CommitOrder:    commitOrder,
		Published:      false,
		Trust:          trust,
		Representation: representation,
		Group:          group,
		CreatedAt:      time.Now().UTC(),
		LastAccess:     time.Now().UTC(),
	}
	tier.entries[key] = entry
	tier.order = append(tier.order, key)
	tier.used = saturatingAddUint64(tier.used, entryBytes)
	tier.mu.Unlock()
	return entry, nil
}

func (tier *readCacheTier) reserveAndReclaim(ctx context.Context, reserve uint64) bool {
	for {
		tier.mu.Lock()
		if !sumExceedsUint64(tier.maxBytes, tier.used, tier.reserved, reserve) {
			tier.reserved = saturatingAddUint64(tier.reserved, reserve)
			tier.mu.Unlock()
			return true
		}
		victim := tier.pickVictimLocked()
		if victim == nil {
			tier.mu.Unlock()
			return false
		}
		tier.markRetiringLocked(victim)
		tier.mu.Unlock()
		tier.deleteRetiringEntry(ctx, victim)
	}

}

func (tier *readCacheTier) releaseReservation(reserved uint64) {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	if tier.reserved >= reserved {
		tier.reserved -= reserved
	} else {
		tier.reserved = 0
	}
}

func (tier *readCacheTier) pickVictimLocked() *readCacheEntry {
	var victim *readCacheEntry
	var victimScore float64
	var victimIndex int
	groupCounts := map[string]int{}
	for _, key := range tier.order {
		entry, ok := tier.entries[key]
		if !ok || entry.Retiring || entry.Pins > 0 {
			continue
		}
		groupCounts[entry.Group]++
	}
	for i, key := range tier.order {
		entry, ok := tier.entries[key]
		if !ok || entry.Retiring || entry.Pins > 0 {
			continue
		}
		score := tier.victimScore(entry, groupCounts[entry.Group])
		if victim == nil || score < victimScore || (score == victimScore && entry.LastAccess.Before(victim.LastAccess)) {
			victim = entry
			victimScore = score
			victimIndex = i
		}
	}
	if victim != nil {
		tier.order = append(tier.order[:victimIndex], tier.order[victimIndex+1:]...)
	}
	return victim
}

func (tier *readCacheTier) victimScore(entry *readCacheEntry, overlap int) float64 {
	ageSeconds := time.Since(entry.LastAccess).Seconds()
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	bytes := float64(max(1, int(entry.Size+entry.MetaSize)))
	hits := float64(entry.Hits + 1)
	decay := math.Exp(-ageSeconds / 300.0)
	benefit := (hits + float64(entry.UsefulBytes)/1024.0) * decay
	if overlap > 1 {
		benefit /= float64(overlap)
	}
	repWeight := 1.0
	switch entry.Representation {
	case readCacheRepWholeFile:
		repWeight = 0.7
	case readCacheRepDecodedExtent:
		repWeight = 1.0
	case readCacheRepCompressedContainer:
		repWeight = 1.1
	case readCacheRepSourceEncryptedRange:
		repWeight = 1.3
	}
	overlapPenalty := 1.0 + float64(max(0, overlap-1))*0.2
	return (benefit * repWeight) / (bytes * overlapPenalty)
}

func (tier *readCacheTier) markRetiringLocked(entry *readCacheEntry) {
	if entry == nil || entry.Retiring {
		return
	}
	entry.Retiring = true
	delete(tier.entries, entry.Key)
	tier.deleting[quotaEntryID(tier.manager.repoID, entry.Key, readCacheEntryOrder(entry))] = entry
	entryBytes := saturatingAddUint64(entry.Size, entry.MetaSize)
	tier.pendingDelete = saturatingAddUint64(tier.pendingDelete, entryBytes)
}

func (tier *readCacheTier) deleteRetiringEntry(ctx context.Context, entry *readCacheEntry) {
	if entry == nil {
		return
	}
	tier.mu.Lock()
	pinned := entry.Pins != 0
	tier.mu.Unlock()
	if pinned {
		return
	}
	if tier.manager != nil {
		tier.manager.markDeletionPending(ctx, tier.id, entry)
	}
	removeDataErr := tier.backend.Remove(ctx, entry.DataHandle)
	if removeDataErr != nil && !tier.backend.IsNotExist(removeDataErr) {
		debug.Log("read-cache retirement remove data failed for %s: %v", tier.id, removeDataErr)
		tier.mu.Lock()
		tier.purgeFailures++
		tier.mu.Unlock()
		return
	}
	removeMetaErr := tier.backend.Remove(ctx, entry.MetaHandle)
	if removeMetaErr != nil && !tier.backend.IsNotExist(removeMetaErr) {
		debug.Log("read-cache retirement remove meta failed for %s: %v", tier.id, removeMetaErr)
		tier.mu.Lock()
		tier.purgeFailures++
		tier.mu.Unlock()
		return
	}
	if pending, err := readCacheReclamationPending(ctx, tier.backend, entry.DataHandle, entry.MetaHandle); err != nil || pending {
		if err != nil {
			debug.Log("read-cache reclamation check failed for %s: %v", tier.id, err)
		}
		return
	}
	entryBytes := entry.Size + entry.MetaSize
	tier.mu.Lock()
	delete(tier.deleting, quotaEntryID(tier.manager.repoID, entry.Key, readCacheEntryOrder(entry)))
	if tier.pendingDelete >= entryBytes {
		tier.pendingDelete -= entryBytes
	} else {
		tier.pendingDelete = 0
	}
	if tier.used >= entryBytes {
		tier.used -= entryBytes
	} else {
		tier.used = 0
	}
	tier.mu.Unlock()
	if tier.manager != nil {
		tier.manager.finalizeDeletion(ctx, tier.id, entry)
	}
}

func (tier *readCacheTier) reconcileRetireTracked(ctx context.Context, entry *readCacheEntry) {
	if entry == nil {
		return
	}
	if entry.CommitOrder == 0 {
		tier.reconcileRetireTwo(ctx, entry.DataHandle, entry.MetaHandle, entry.Size, entry.MetaSize)
		return
	}
	entry.Retiring = true
	entryID := quotaEntryID(tier.manager.repoID, entry.Key, readCacheEntryOrder(entry))
	tier.mu.Lock()
	if existing, ok := tier.deleting[entryID]; ok {
		tier.mu.Unlock()
		tier.deleteRetiringEntry(ctx, existing)
		return
	}
	tier.deleting[entryID] = entry
	entryBytes := saturatingAddUint64(entry.Size, entry.MetaSize)
	tier.pendingDelete = saturatingAddUint64(tier.pendingDelete, entryBytes)
	tier.used = saturatingAddUint64(tier.used, entryBytes)
	tier.mu.Unlock()
	tier.deleteRetiringEntry(ctx, entry)
}

func (tier *readCacheTier) retireEntry(ctx context.Context, key string) {
	tier.mu.Lock()
	entry, ok := tier.entries[key]
	if !ok {
		tier.mu.Unlock()
		return
	}
	tier.markRetiringLocked(entry)
	tier.mu.Unlock()
	tier.deleteRetiringEntry(ctx, entry)
}

func (tier *readCacheTier) shrink(ctx context.Context) {
	for {
		tier.mu.Lock()
		if !sumExceedsUint64(tier.maxBytes, tier.used, tier.reserved) {
			tier.mu.Unlock()
			return
		}
		victim := tier.pickVictimLocked()
		if victim == nil {
			tier.mu.Unlock()
			return
		}
		tier.markRetiringLocked(victim)
		tier.mu.Unlock()
		tier.deleteRetiringEntry(ctx, victim)
	}
}

func (tier *readCacheTier) drain(ctx context.Context) error {
	tier.mu.Lock()
	tier.purgeFailures = 0
	retire := make([]*readCacheEntry, 0, len(tier.entries)+len(tier.deleting))
	for _, entry := range tier.entries {
		tier.markRetiringLocked(entry)
	}
	for _, entry := range tier.deleting {
		retire = append(retire, entry)
	}
	tier.order = nil
	tier.mu.Unlock()
	for _, entry := range retire {
		tier.deleteRetiringEntry(ctx, entry)
	}
	tier.mu.Lock()
	defer tier.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(tier.entries) != 0 || len(tier.deleting) != 0 || tier.pendingDelete != 0 || tier.purgeFailures != 0 {
		return fmt.Errorf(
			"read-cache tier %q drain incomplete: entries=%d deleting=%d pending-delete=%d purge-failures=%d",
			tier.id, len(tier.entries), len(tier.deleting), tier.pendingDelete, tier.purgeFailures,
		)
	}
	return nil
}

func (tier *readCacheTier) allowProbe(key string) bool {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	now := time.Now().UTC()
	if now.Before(tier.circuitUntil) {
		return false
	}
	if until, ok := tier.negativeUntil[key]; ok && now.Before(until) {
		return false
	}
	return true
}

func (tier *readCacheTier) recordProbeFailure(key string) {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	now := time.Now().UTC()
	tier.negativeUntil[key] = now.Add(readCacheNegativeTTL)
	tier.recordRecentFailureLocked(now)
	failures := tier.probeFailures[key] + 1
	tier.probeFailures[key] = failures
	if failures >= readCacheCircuitTrips || len(tier.recentFailures) >= readCacheCircuitTrips {
		tier.circuitUntil = now.Add(readCacheCircuitWindow)
	}
}

func (tier *readCacheTier) probeFailureCount(key string) uint32 {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	return tier.probeFailures[key]
}

func (tier *readCacheTier) recordRecentFailureLocked(now time.Time) {
	trimmed := tier.recentFailures[:0]
	for _, seen := range tier.recentFailures {
		if now.Sub(seen) <= readCacheCircuitWindow {
			trimmed = append(trimmed, seen)
		}
	}
	trimmed = append(trimmed, now)
	if len(trimmed) > readCacheCircuitMaxRecentFailures {
		trimmed = trimmed[len(trimmed)-readCacheCircuitMaxRecentFailures:]
	}
	tier.recentFailures = trimmed
}

func (tier *readCacheTier) recordProbeSuccess(key string) {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	delete(tier.negativeUntil, key)
	delete(tier.probeFailures, key)
}

func (tier *readCacheTier) recordFillSuccess() {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	tier.fillFailures = 0
	tier.fillBackoff = time.Time{}
}

func (tier *readCacheTier) recordFillFailure(timeout bool) {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	tier.fillFailures++
	multiplier := 1 << min(int(tier.fillFailures-1), 6)
	backoff := time.Duration(multiplier) * readCacheFillBackoffMin
	if timeout {
		backoff *= 2
	}
	if backoff > readCacheFillBackoffMax {
		backoff = readCacheFillBackoffMax
	}
	tier.fillBackoff = time.Now().UTC().Add(backoff)
}

func (tier *readCacheTier) allowAdmission() bool {
	tier.mu.Lock()
	now := time.Now().UTC()
	blocked := now.Before(tier.circuitUntil) || now.Before(tier.fillBackoff)
	tier.mu.Unlock()
	if blocked {
		return false
	}
	if tier.manager == nil {
		return true
	}
	return tier.manager.fillAllowed()
}

//nolint:funlen,gocognit,gocyclo,nestif // Reconciliation keeps validation and conservative retirement decisions together to avoid unsafe cache publication.
func (tier *readCacheTier) reconcile(ctx context.Context, manager *readCacheManager) {
	files := map[string]backend.FileInfo{}
	if err := tier.backend.List(ctx, backend.StagingFile, func(info backend.FileInfo) error {
		if !strings.HasPrefix(info.Name, readCacheNamespace+"/") {
			return nil
		}
		if isReadCacheControlPath(info.Name) {
			return nil
		}
		files[info.Name] = info
		return nil
	}); err != nil {
		debug.Log("read-cache reconcile list failed for %s: %v", tier.id, err)
		return
	}
	claimed := map[string]struct{}{}
	quotaEntries, haveAuthoritative := manager.authoritativeQuotaEntries(ctx, tier.id)
	activeByKey := activeQuotaByKeyFromEntries(quotaEntries)
	protectedByKey := protectedQuotaByKeyFromEntries(quotaEntries)
	activeByKeyGeneration := activeQuotaByKeyGenerationFromEntries(quotaEntries)
	activeByDescriptor := buildActiveQuotaDescriptorIndex(quotaEntries)
	type candidate struct {
		entry       *readCacheEntry
		commitOrder uint64
	}
	bestByKey := map[string]candidate{}
	for name, info := range files {
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		base := strings.TrimSuffix(name, ".json")
		dataName := base + ".bin"
		claimed[name] = struct{}{}
		claimed[dataName] = struct{}{}
		dataHandle := backend.Handle{Type: backend.StagingFile, Name: dataName}
		metaHandle := backend.Handle{Type: backend.StagingFile, Name: name}
		metaDescriptor := parseReadCacheHandleDescriptor(name)
		dataDescriptor := parseReadCacheHandleDescriptor(dataName)
		if metaDescriptor.RepositoryScope != "" && metaDescriptor.RepositoryScope != readCacheRepositoryScope(manager.repoID) {
			continue
		}
		canonicalKey := ""
		canonicalGeneration := uint64(0)
		canonicalCommitOrder := uint64(0)
		resolveCanonicalFromDescriptor := func(descriptor readCacheHandleDescriptor) {
			if canonicalKey != "" || !descriptor.HasGeneration || !descriptor.HasCommitOrder || descriptor.DigestSuffix == "" {
				return
			}
			item, ok := activeByDescriptor[activeQuotaDescriptorIndexKey(descriptor.Generation, descriptor.CommitOrder, descriptor.DigestSuffix)]
			if !ok || item.TierID != tier.id {
				return
			}
			canonicalKey = item.Key
			canonicalGeneration = item.Generation
			canonicalCommitOrder = item.CommitOrder
		}
		retirePair := func(dataInfo backend.FileInfo) {
			if canonicalKey != "" && canonicalGeneration != 0 && canonicalCommitOrder != 0 {
				tier.reconcileRetireCanonical(
					ctx, dataHandle, metaHandle, uint64(max(0, dataInfo.Size)), uint64(max(0, info.Size)),
					canonicalKey, canonicalGeneration, canonicalCommitOrder,
				)
				return
			}
			tier.reconcileRetireTwo(ctx, dataHandle, metaHandle, uint64(max(0, dataInfo.Size)), uint64(max(0, info.Size)))
		}
		dataInfo, ok := files[dataName]
		if !ok {
			resolveCanonicalFromDescriptor(metaDescriptor)
			resolveCanonicalFromDescriptor(dataDescriptor)
			metaRaw, err := loadBytes(ctx, tier.backend, metaHandle, 0, 0)
			if err == nil {
				var meta readCacheChunkMeta
				if unmarshalErr := json.Unmarshal(metaRaw, &meta); unmarshalErr == nil {
					if meta.Identity.RepositoryID == "" {
						packID, parseErr := vaultic.ParseID(meta.PackID)
						if parseErr == nil {
							meta.Identity = manager.sourceRangeIdentity(tier, packID, meta.Offset, meta.Length)
						}
					}
					key := tier.entryKey(meta.Identity)
					if meta.Identity.RepositoryID == manager.repoID && meta.Generation != 0 && manager.validMeta(meta, key) {
						canonicalKey = key
						canonicalGeneration = meta.Generation
						canonicalCommitOrder = meta.CommitOrder
						if canonicalCommitOrder == 0 {
							if metaDescriptor.HasCommitOrder {
								canonicalCommitOrder = metaDescriptor.CommitOrder
							} else if winner, ok := activeByKey[key]; ok && winner.Generation == meta.Generation {
								canonicalCommitOrder = winner.CommitOrder
							} else if candidate, ok := activeByKeyGeneration[quotaEntryID(manager.repoID, key, meta.Generation)]; ok {
								canonicalCommitOrder = candidate
							}
						}
					}
				}
			}
			if canonicalKey != "" && canonicalGeneration != 0 && canonicalCommitOrder != 0 {
				tier.reconcileRetireCanonical(
					ctx, dataHandle, metaHandle, 0, uint64(max(0, info.Size)),
					canonicalKey, canonicalGeneration, canonicalCommitOrder,
				)
				continue
			}
			tier.reconcileRetireTwo(ctx, dataHandle, metaHandle, 0, uint64(max(0, info.Size)))
			continue
		}
		metaRaw, err := loadBytes(ctx, tier.backend, metaHandle, 0, 0)
		if err != nil {
			resolveCanonicalFromDescriptor(metaDescriptor)
			resolveCanonicalFromDescriptor(dataDescriptor)
			retirePair(dataInfo)
			continue
		}
		var meta readCacheChunkMeta
		if err := json.Unmarshal(metaRaw, &meta); err != nil {
			resolveCanonicalFromDescriptor(metaDescriptor)
			resolveCanonicalFromDescriptor(dataDescriptor)
			retirePair(dataInfo)
			continue
		}
		if meta.Identity.RepositoryID == "" {
			packID, err := vaultic.ParseID(meta.PackID)
			if err != nil {
				resolveCanonicalFromDescriptor(metaDescriptor)
				resolveCanonicalFromDescriptor(dataDescriptor)
				retirePair(dataInfo)
				continue
			}
			meta.Identity = manager.sourceRangeIdentity(tier, packID, meta.Offset, meta.Length)
		}
		key := tier.entryKey(meta.Identity)
		if meta.Identity.RepositoryID == manager.repoID && meta.Generation != 0 && manager.validMeta(meta, key) {
			canonicalKey = key
			canonicalGeneration = meta.Generation
			canonicalCommitOrder = meta.CommitOrder
			if canonicalCommitOrder == 0 {
				if metaDescriptor.HasCommitOrder {
					canonicalCommitOrder = metaDescriptor.CommitOrder
				} else if winner, ok := activeByKey[key]; ok && winner.Generation == meta.Generation {
					canonicalCommitOrder = winner.CommitOrder
				} else if candidate, ok := activeByKeyGeneration[quotaEntryID(manager.repoID, key, meta.Generation)]; ok {
					canonicalCommitOrder = candidate
				}
			}
		}
		if canonicalCommitOrder == 0 {
			resolveCanonicalFromDescriptor(metaDescriptor)
			resolveCanonicalFromDescriptor(dataDescriptor)
		}
		if !readCacheDescriptorPairCompatible(metaDescriptor, dataDescriptor) {
			retirePair(dataInfo)
			continue
		}
		if meta.Identity.RepositoryID != manager.repoID {
			retirePair(dataInfo)
			continue
		}
		if metaDescriptor.HasGeneration {
			if meta.Generation == 0 || metaDescriptor.Generation != meta.Generation {
				retirePair(dataInfo)
				continue
			}
		} else if meta.Generation == 0 {
			retirePair(dataInfo)
			continue
		}
		if metaDescriptor.HasCommitOrder && (meta.CommitOrder == 0 || metaDescriptor.CommitOrder != meta.CommitOrder) {
			retirePair(dataInfo)
			continue
		}
		commitOrder := meta.CommitOrder
		if commitOrder == 0 {
			if metaDescriptor.HasCommitOrder {
				commitOrder = metaDescriptor.CommitOrder
			} else if winner, ok := activeByKey[key]; ok && winner.Generation == meta.Generation {
				commitOrder = winner.CommitOrder
			} else if candidate, ok := activeByKeyGeneration[quotaEntryID(manager.repoID, key, meta.Generation)]; ok {
				commitOrder = candidate
			}
		}
		if commitOrder == 0 {
			retirePair(dataInfo)
			continue
		}
		if !readCacheDescriptorMatchesIdentity(metaDescriptor, tier.id, meta.Identity, meta.Generation, commitOrder, key) {
			retirePair(dataInfo)
			continue
		}
		if !readCacheDescriptorMatchesIdentity(dataDescriptor, tier.id, meta.Identity, meta.Generation, commitOrder, key) {
			retirePair(dataInfo)
			continue
		}
		if meta.TierID != tier.id || meta.Trust != tier.trust || !manager.validMeta(meta, key) {
			retirePair(dataInfo)
			continue
		}
		if meta.Identity.Trust != tier.trust || !readCacheRepresentationAllowed(meta.Identity.Representation) {
			retirePair(dataInfo)
			continue
		}
		entry := &readCacheEntry{
			Key:            key,
			DataHandle:     backend.Handle{Type: backend.StagingFile, Name: dataName},
			MetaHandle:     metaHandle,
			Size:           uint64(max(0, dataInfo.Size)),
			MetaSize:       uint64(max(0, info.Size)),
			Generation:     meta.Generation,
			KeyGeneration:  meta.Identity.KeyGeneration,
			CommitOrder:    commitOrder,
			Published:      false,
			Trust:          meta.Identity.Trust,
			Representation: meta.Identity.Representation,
			Group:          meta.Identity.LayoutID + "|" + meta.Identity.ContentID,
			CreatedAt:      time.Now().UTC(),
			LastAccess:     time.Now().UTC(),
		}
		if meta.CreatedUnixMS > 0 {
			entry.CreatedAt = time.UnixMilli(meta.CreatedUnixMS).UTC()
		}
		if haveAuthoritative {
			winner, ok := protectedByKey[key]
			if !ok || winner.Generation != entry.Generation || winner.CommitOrder != entry.CommitOrder {
				tier.reconcileRetireTracked(ctx, entry)
				continue
			}
			entry.Published = quotaEntryIsPublished(winner.State)
		}
		incumbent, hasIncumbent := bestByKey[key]
		if !hasIncumbent || entry.CommitOrder > incumbent.commitOrder {
			if hasIncumbent {
				tier.reconcileRetireTracked(ctx, incumbent.entry)
			}
			bestByKey[key] = candidate{entry: entry, commitOrder: entry.CommitOrder}
			continue
		}
		tier.reconcileRetireTracked(ctx, entry)
	}
	tier.mu.Lock()
	for key, selected := range bestByKey {
		if _, exists := tier.entries[key]; exists {
			continue
		}
		entry := selected.entry
		tier.entries[key] = entry
		tier.order = append(tier.order, key)
		tier.used = saturatingAddUint64(tier.used, entry.Size)
		tier.used = saturatingAddUint64(tier.used, entry.MetaSize)
	}
	tier.mu.Unlock()
	for name, info := range files {
		if _, ok := claimed[name]; ok {
			continue
		}
		handle := backend.Handle{Type: backend.StagingFile, Name: name}
		descriptor := parseReadCacheHandleDescriptor(name)
		if descriptor.RepositoryScope != "" && descriptor.RepositoryScope != readCacheRepositoryScope(manager.repoID) {
			continue
		}
		if descriptor.HasGeneration && descriptor.HasCommitOrder && descriptor.DigestSuffix != "" {
			descriptorKey := activeQuotaDescriptorIndexKey(
				descriptor.Generation, descriptor.CommitOrder, descriptor.DigestSuffix,
			)
			if item, ok := activeByDescriptor[descriptorKey]; ok && item.TierID == tier.id {
				if strings.HasSuffix(name, ".bin") {
					tier.reconcileRetireCanonical(
						ctx,
						handle,
						backend.Handle{Type: backend.StagingFile, Name: strings.TrimSuffix(name, ".bin") + ".json"},
						uint64(max(0, info.Size)),
						0,
						item.Key,
						item.Generation,
						item.CommitOrder,
					)
					continue
				}
				if strings.HasSuffix(name, ".json") {
					tier.reconcileRetireCanonical(
						ctx,
						backend.Handle{Type: backend.StagingFile, Name: strings.TrimSuffix(name, ".json") + ".bin"},
						handle,
						0,
						uint64(max(0, info.Size)),
						item.Key,
						item.Generation,
						item.CommitOrder,
					)
					continue
				}
			}
		}
		tier.reconcileRetireOne(ctx, handle, uint64(max(0, info.Size)))
	}
	tier.shrink(ctx)
}

func (manager *readCacheManager) authoritativeQuotaEntries(ctx context.Context, tierID string) ([]readCacheQuotaEntry, bool) {
	if manager == nil || manager.coordinator == nil || manager.coordinatorBackend == nil {
		return nil, false
	}
	_, ledger, err := manager.loadQuota(ctx)
	if err != nil || ledger == nil {
		return nil, false
	}
	if ledger.Revision == 0 && len(ledger.Entries) == 0 {
		return nil, false
	}
	entries := make([]readCacheQuotaEntry, 0, len(ledger.Entries))
	for _, item := range ledger.Entries {
		if item.RepositoryID != manager.repoID || item.TierID != tierID || item.CommitOrder == 0 {
			continue
		}
		switch item.State {
		case readCacheStatePublished, readCacheStateActive, readCacheStateDeleting, readCacheStateAdmitting, readCacheStatePublishing:
			entries = append(entries, item)
		default:
			continue
		}
	}
	return entries, true
}

func activeQuotaByKeyFromEntries(entries []readCacheQuotaEntry) map[string]readCacheQuotaEntry {
	activeByKey := map[string]readCacheQuotaEntry{}
	ambiguous := map[string]struct{}{}
	for _, item := range entries {
		if !quotaEntryIsPublished(item.State) {
			continue
		}
		prior, ok := activeByKey[item.Key]
		if !ok || item.CommitOrder > prior.CommitOrder {
			activeByKey[item.Key] = item
			continue
		}
		if item.CommitOrder == prior.CommitOrder && item.Generation != prior.Generation {
			ambiguous[item.Key] = struct{}{}
		}
	}
	for key := range ambiguous {
		delete(activeByKey, key)
	}
	return activeByKey
}

func protectedQuotaByKeyFromEntries(entries []readCacheQuotaEntry) map[string]readCacheQuotaEntry {
	protected := map[string]readCacheQuotaEntry{}
	for _, item := range entries {
		if item.State != readCacheStatePublishing && !quotaEntryIsPublished(item.State) {
			continue
		}
		prior, ok := protected[item.Key]
		if !ok || item.CommitOrder > prior.CommitOrder {
			protected[item.Key] = item
		}
	}
	return protected
}

func activeQuotaByKeyGenerationFromEntries(entries []readCacheQuotaEntry) map[string]uint64 {
	byKeyGeneration := map[string]uint64{}
	ambiguous := map[string]struct{}{}
	for _, item := range entries {
		if (!quotaEntryIsPublished(item.State) && item.State != readCacheStateDeleting) || item.Generation == 0 {
			continue
		}
		generationKey := quotaEntryID(item.RepositoryID, item.Key, item.Generation)
		if prior, ok := byKeyGeneration[generationKey]; ok && prior != item.CommitOrder {
			ambiguous[generationKey] = struct{}{}
			continue
		}
		byKeyGeneration[generationKey] = item.CommitOrder
	}
	for key := range ambiguous {
		delete(byKeyGeneration, key)
	}
	return byKeyGeneration
}

func (manager *readCacheManager) activeQuotaByKey(ctx context.Context, tierID string) (map[string]readCacheQuotaEntry, bool) {
	entries, ok := manager.authoritativeQuotaEntries(ctx, tierID)
	if !ok {
		return nil, false
	}
	return activeQuotaByKeyFromEntries(entries), true
}

func (manager *readCacheManager) activeQuotaByKeyGeneration(ctx context.Context, tierID string) (map[string]uint64, bool) {
	entries, ok := manager.authoritativeQuotaEntries(ctx, tierID)
	if !ok {
		return nil, false
	}
	return activeQuotaByKeyGenerationFromEntries(entries), true
}

func (tier *readCacheTier) reconcileRetireOne(ctx context.Context, handle backend.Handle, size uint64) {
	tier.mu.Lock()
	tier.used = saturatingAddUint64(tier.used, size)
	tier.pendingDelete = saturatingAddUint64(tier.pendingDelete, size)
	tier.mu.Unlock()
	err := tier.backend.Remove(ctx, handle)
	if err != nil && !tier.backend.IsNotExist(err) {
		debug.Log("read-cache reconcile remove failed for %s: %v", tier.id, err)
		return
	}
	tier.mu.Lock()
	if tier.pendingDelete >= size {
		tier.pendingDelete -= size
	} else {
		tier.pendingDelete = 0
	}
	if tier.used >= size {
		tier.used -= size
	} else {
		tier.used = 0
	}
	tier.mu.Unlock()
}

func (tier *readCacheTier) reconcileRetireTwo(ctx context.Context, first, second backend.Handle, firstSize, secondSize uint64) {
	entry := &readCacheEntry{
		Key:        first.Name + "|" + second.Name,
		DataHandle: first,
		MetaHandle: second,
		Size:       firstSize,
		MetaSize:   secondSize,
		Retiring:   true,
	}
	entryID := quotaEntryID(tier.manager.repoID, entry.Key, readCacheEntryOrder(entry))
	tier.mu.Lock()
	if existing, ok := tier.deleting[entryID]; ok {
		tier.mu.Unlock()
		tier.deleteRetiringEntry(ctx, existing)
		return
	}
	tier.deleting[entryID] = entry
	total := saturatingAddUint64(firstSize, secondSize)
	tier.used = saturatingAddUint64(tier.used, total)
	tier.pendingDelete = saturatingAddUint64(tier.pendingDelete, total)
	tier.mu.Unlock()
	tier.deleteRetiringEntry(ctx, entry)
}

func (tier *readCacheTier) reconcileRetireCanonical(
	ctx context.Context,
	dataHandle, metaHandle backend.Handle,
	dataSize, metaSize uint64,
	key string,
	generation, commitOrder uint64,
) {
	if key == "" || generation == 0 || commitOrder == 0 {
		tier.reconcileRetireTwo(ctx, dataHandle, metaHandle, dataSize, metaSize)
		return
	}
	entry := &readCacheEntry{
		Key:         key,
		DataHandle:  dataHandle,
		MetaHandle:  metaHandle,
		Size:        dataSize,
		MetaSize:    metaSize,
		Generation:  generation,
		CommitOrder: commitOrder,
		Retiring:    true,
	}
	entryID := quotaEntryID(tier.manager.repoID, entry.Key, readCacheEntryOrder(entry))
	tier.mu.Lock()
	if existing, ok := tier.deleting[entryID]; ok {
		tier.mu.Unlock()
		tier.deleteRetiringEntry(ctx, existing)
		return
	}
	tier.deleting[entryID] = entry
	total := saturatingAddUint64(dataSize, metaSize)
	tier.used = saturatingAddUint64(tier.used, total)
	tier.pendingDelete = saturatingAddUint64(tier.pendingDelete, total)
	tier.mu.Unlock()
	tier.deleteRetiringEntry(ctx, entry)
}

func buildActiveQuotaDescriptorIndex(entries []readCacheQuotaEntry) map[string]readCacheQuotaEntry {
	index := make(map[string]readCacheQuotaEntry, len(entries))
	ambiguous := map[string]struct{}{}
	for _, item := range entries {
		switch item.State {
		case readCacheStateActive, readCacheStateDeleting, readCacheStateAdmitting,
			readCacheStatePublishing, readCacheStatePublished:
		default:
			continue
		}
		if item.Key == "" || item.Generation == 0 || item.CommitOrder == 0 {
			continue
		}
		digest := sha256.Sum256([]byte(item.Key))
		key := activeQuotaDescriptorIndexKey(item.Generation, item.CommitOrder, hex.EncodeToString(digest[:]))
		if prior, ok := index[key]; ok && prior.Key != item.Key {
			ambiguous[key] = struct{}{}
			continue
		}
		index[key] = item
	}
	for key := range ambiguous {
		delete(index, key)
	}
	return index
}

func activeQuotaDescriptorIndexKey(generation uint64, commitOrder uint64, digestSuffix string) string {
	return fmt.Sprintf("%d:%d:%s", generation, commitOrder, digestSuffix)
}

func loadBytes(ctx context.Context, be backend.Backend, handle backend.Handle, length int, offset int64) ([]byte, error) {
	var out []byte
	err := be.Load(ctx, handle, length, offset, func(reader io.Reader) error {
		var readErr error
		out, readErr = io.ReadAll(reader)
		return readErr
	})
	return out, err
}

func readCacheEntryOrder(entry *readCacheEntry) uint64 {
	if entry == nil {
		return 0
	}
	return entry.CommitOrder
}

func (manager *readCacheManager) encryptDerivedChunk(meta readCacheChunkMeta, plaintext []byte) ([]byte, []byte, string, error) {
	aead, err := manager.derivedAEAD()
	if err != nil {
		return nil, nil, "", err
	}
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, "", err
	}
	aad := manager.chunkAAD(meta)
	aadSum := sha256.Sum256(aad)
	sealed := aead.Seal(nil, nonce, plaintext, aad)
	return sealed, nonce, hex.EncodeToString(aadSum[:]), nil
}

func (manager *readCacheManager) decryptDerivedChunk(meta readCacheChunkMeta, ciphertext []byte) ([]byte, error) {
	aead, err := manager.derivedAEAD()
	if err != nil {
		return nil, err
	}
	nonce, err := hex.DecodeString(meta.DataNonce)
	if err != nil {
		return nil, err
	}
	aad := manager.chunkAAD(meta)
	return aead.Open(nil, nonce, ciphertext, aad)
}

func (manager *readCacheManager) derivedAEAD() (cipher.AEAD, error) {
	material := make([]byte, 0, len(manager.key)+len(manager.repoID)+32)
	material = append(material, manager.key...)
	material = append(material, []byte(manager.repoID)...)
	mac := hmac.New(sha256.New, material)
	if _, err := mac.Write([]byte("vaultic-read-cache-derived-aead-v1")); err != nil {
		return nil, err
	}
	derived := mac.Sum(nil)
	return chacha20poly1305.NewX(derived)
}

func (manager *readCacheManager) chunkAAD(meta readCacheChunkMeta) []byte {
	identity := meta.Identity
	payload := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d|%s|%s|%d|%d|%d|%s",
		identity.RepositoryID,
		meta.TierID,
		identity.Representation,
		identity.ContentID,
		identity.Offset,
		identity.Length,
		identity.FormatVersion,
		identity.Codec,
		identity.Trust,
		identity.ChunkGeometry,
		identity.KeyGeneration,
		meta.Generation,
		meta.DigestSHA256,
	)
	return []byte(payload)
}

func (manager *readCacheManager) loadDecodedBlobRepresentation(ctx context.Context, bh vaultic.BlobHandle, length int) ([]byte, bool) {
	return manager.loadRepresentationFromAnyTier(ctx, func(tier *readCacheTier) readCacheIdentity {
		return manager.blobIdentity(tier, bh, length, readCacheRepDecodedExtent)
	})
}

func (manager *readCacheManager) loadCompressedBlobRepresentation(ctx context.Context, bh vaultic.BlobHandle, length int) ([]byte, bool) {
	return manager.loadRepresentationFromAnyTier(ctx, func(tier *readCacheTier) readCacheIdentity {
		return manager.blobIdentity(tier, bh, length, readCacheRepCompressedContainer)
	})
}

func (manager *readCacheManager) admitBlobRepresentations(ctx context.Context, blob vaultic.BlobHandle, plain []byte, compressed []byte) {
	if len(plain) == 0 {
		return
	}
	fillCtx, cancelFill := context.WithTimeout(context.WithoutCancel(ctx), readCacheFillTimeout)
	defer cancelFill()
	for _, tier := range manager.sortedTiersByAdmissionPriority() {
		decodedIdentity := manager.blobIdentity(tier, blob, len(plain), readCacheRepDecodedExtent)
		manager.storeRepresentation(fillCtx, tier, decodedIdentity, plain)
		if len(compressed) != 0 {
			compressedIdentity := manager.blobIdentity(tier, blob, len(compressed), readCacheRepCompressedContainer)
			manager.storeRepresentation(fillCtx, tier, compressedIdentity, compressed)
		}
	}
}

//nolint:gocognit // Range assembly keeps authenticated cache fallback and origin reads in one ordered path.
func (manager *readCacheManager) readLogicalFileRange(
	ctx context.Context,
	content []vaultic.ID,
	cumSize []uint64,
	offset uint64,
	dst []byte,
	loadBlob vaultic.BlobReader,
) (int, error) {
	if len(dst) == 0 || len(content) == 0 || len(cumSize) != len(content)+1 {
		return 0, nil
	}
	totalSize := cumSize[len(cumSize)-1]
	if offset >= totalSize {
		return 0, nil
	}
	if whole, ok := manager.loadRepresentationFromAnyTier(ctx, func(tier *readCacheTier) readCacheIdentity {
		return manager.logicalWholeIdentity(tier, content, cumSize)
	}); ok {
		if offset >= uint64(len(whole)) {
			return 0, nil
		}
		return copy(dst, whole[offset:]), nil
	}
	accessCount := manager.recordLogicalAccess(content, cumSize)
	allowPromotion := accessCount >= 2

	remaining := int(min(uint64(len(dst)), totalSize-offset))
	allowWholePromotion := allowPromotion && uint64(remaining) >= (totalSize+1)/2
	readBytes := 0
	requestOffset := int64(offset)
	chunkSize := manager.logicalChunkSizeSnapshot()
	for remaining > 0 {
		chunkStart := (requestOffset / int64(chunkSize)) * int64(chunkSize)
		chunkEnd := min(int64(totalSize), chunkStart+int64(chunkSize))
		chunkLen := int(chunkEnd - chunkStart)
		chunk, ok := manager.loadRepresentationFromAnyTier(ctx, func(tier *readCacheTier) readCacheIdentity {
			return manager.logicalChunkIdentity(tier, content, cumSize, chunkStart, chunkLen)
		})
		if !ok {
			chunk = make([]byte, chunkLen)
			n, err := vaultic.ReadLogicalFileRange(ctx, content, cumSize, uint64(chunkStart), chunk, loadBlob)
			if err != nil {
				return readBytes, err
			}
			if n < chunkLen {
				chunk = chunk[:n]
			}
			if allowPromotion && len(chunk) != 0 {
				promotionCtx, cancelPromotion := context.WithTimeout(context.WithoutCancel(ctx), readCacheFillTimeout)
				for _, tier := range manager.sortedTiersByAdmissionPriority() {
					identity := manager.logicalChunkIdentity(tier, content, cumSize, chunkStart, len(chunk))
					manager.storeRepresentation(promotionCtx, tier, identity, chunk)
				}
				cancelPromotion()
			}
		}
		from := int(requestOffset - chunkStart)
		if from < 0 {
			from = 0
		}
		if from >= len(chunk) {
			requestOffset = chunkEnd
			continue
		}
		copied := copy(dst[readBytes:], chunk[from:])
		readBytes += copied
		remaining -= copied
		requestOffset += int64(copied)
		if copied == 0 {
			break
		}
	}
	if allowWholePromotion && readBytes > 0 && totalSize <= 8*1024*1024 {
		promotionCtx, cancelPromotion := context.WithTimeout(context.WithoutCancel(ctx), readCacheFillTimeout)
		defer cancelPromotion()
		full := make([]byte, totalSize)
		n, err := vaultic.ReadLogicalFileRange(promotionCtx, content, cumSize, 0, full, loadBlob)
		if err == nil && uint64(n) == totalSize {
			for _, tier := range manager.sortedTiersByAdmissionPriority() {
				identity := manager.logicalWholeIdentity(tier, content, cumSize)
				manager.storeRepresentation(promotionCtx, tier, identity, full)
			}
		}
	}
	return readBytes, nil
}

func (manager *readCacheManager) logicalChunkSizeSnapshot() int {
	chunkSize := readCacheDefaultChunk
	if len(manager.tiers) == 0 {
		return chunkSize
	}
	tier := manager.tiers[0]
	tier.mu.Lock()
	chunkSize = tier.chunkSize
	tier.mu.Unlock()
	if chunkSize <= 0 {
		return readCacheDefaultChunk
	}
	return chunkSize
}

func (manager *readCacheManager) recordLogicalAccess(content []vaultic.ID, cumSize []uint64) uint32 {
	identity, _, _ := logicalLayoutIdentity(content, cumSize)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.logicalAccess) > 512 {
		for key := range manager.logicalAccess {
			delete(manager.logicalAccess, key)
			break
		}
	}
	manager.logicalAccess[identity]++
	return manager.logicalAccess[identity]
}
