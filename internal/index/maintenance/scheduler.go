package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type PlacementSchedulerOptions struct {
	Model  PlacementModel
	Now    time.Time
	DryRun bool
}

type PlacementStatus struct {
	PackID           string   `json:"pack_id"`
	PackType         string   `json:"pack_type"`
	Class            string   `json:"class"`
	TargetBackends   []string `json:"target_backends"`
	LiveBackends     []string `json:"live_backends"`
	MissingBackends  []string `json:"missing_backends"`
	ExcessBackends   []string `json:"excess_backends,omitempty"`
	Durable          bool     `json:"durable"`
	Overdue          bool     `json:"overdue"`
	Deadline         int64    `json:"deadline,omitempty"`
	PendingPromotion bool     `json:"pending_promotion"`
}

type PlacementSchedulerResult struct {
	SchemaVersion             int                    `json:"schema_version"`
	PacksScanned              uint64                 `json:"packs_scanned"`
	Unsatisfied               uint64                 `json:"unsatisfied"`
	Overdue                   uint64                 `json:"overdue"`
	PendingPromotion          uint64                 `json:"pending_promotion"`
	OldestUnsatisfiedDeadline int64                  `json:"oldest_unsatisfied_deadline,omitempty"`
	RequestsWritten           uint64                 `json:"requests_written"`
	Worker                    *PlacementWorkerResult `json:"worker,omitempty"`
	Statuses                  []PlacementStatus      `json:"statuses,omitempty"`
}

type PlacementMigrationOptions struct {
	Model  PlacementModel
	From   string
	To     string
	Now    time.Time
	DryRun bool
}

type PlacementActions interface {
	Place(context.Context, vaultic.ID, PlacementBackend) error
	Promote(context.Context, vaultic.ID, PlacementBackend) error
	Evict(context.Context, vaultic.ID, PlacementBackend) error
}

type PlacementWorkerOptions struct {
	Model       PlacementModel
	Now         time.Time
	RetryBase   time.Duration
	Window      time.Duration
	MaxRequests uint64
	MaxBytes    uint64
}

type PlacementWorkerResult struct {
	RequestsScanned uint64 `json:"requests_scanned"`
	Attempted       uint64 `json:"attempted"`
	Placed          uint64 `json:"placed"`
	Promoted        uint64 `json:"promoted"`
	Evicted         uint64 `json:"evicted"`
	Failed          uint64 `json:"failed"`
	Deferred        uint64 `json:"deferred"`
	BytesMoved      uint64 `json:"bytes_moved"`
}

type placementEventStore interface {
	RecordPackEvents(context.Context, []daemon.PackEvent) error
}

var ErrPlacementObsolete = schema.ErrPlacementObsolete

//nolint:funlen,gocognit,gocyclo // Existing domain flow is an explicit complexity exception; new code remains gated.
func ExecutePlacement(ctx context.Context, store Store, actions PlacementActions, options PlacementWorkerOptions) (PlacementWorkerResult, error) {
	var result PlacementWorkerResult
	if actions == nil {
		return result, errors.New("placement worker requires storage actions")
	}
	if options.Now.IsZero() {
		options.Now = time.Now()
	}
	if options.RetryBase <= 0 {
		options.RetryBase = time.Minute
	}
	if options.Window <= 0 {
		options.Window = time.Second
	}
	backends := make(map[uint64]PlacementBackend, len(options.Model.Backends))
	for _, backend := range options.Model.Backends {
		backends[backend.Hash] = backend
	}
	requestsUsed := make(map[uint64]uint64)
	bytesUsed := make(map[uint64]uint64)
	var workerErr error
	err := scan(ctx, store, schema.PlacementRequestPrefix(), func(entry daemon.KeyValue) error {
		result.RequestsScanned++
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPlacementRequest {
			return schema.ErrMalformed
		}
		request, err := schema.UnmarshalPlacementRequestRecord(entry.Value)
		if err != nil {
			return err
		}
		if request.NotBefore > options.Now.UnixNano() {
			result.Deferred++
			return nil
		}
		backend, ok := backends[request.TargetBackend]
		if !ok {
			return fmt.Errorf("placement request targets unknown backend %016x", request.TargetBackend)
		}
		packID := vaultic.ID(parsed.ID)
		pack, found, err := loadPack(ctx, store, packID)
		if err != nil {
			return err
		}
		if !found || pack.Lifecycle == schema.PackDeleted {
			return store.WriteMutableBatch(ctx, nil, [][]byte{entry.Key}, false)
		}
		if pack.Lifecycle == schema.PackDeletePending {
			if request.Operation == schema.PlacementRequestPromote {
				return completePlacementRequest(ctx, store, entry.Key, request.Operation, packID, pack, backend, options.Now)
			}
			return store.WriteMutableBatch(ctx, nil, [][]byte{entry.Key}, false)
		}
		if !backend.ingestEnabled() && request.Operation != schema.PlacementRequestEvict {
			return failPlacementRequest(ctx, placementFailure{
				store: store, requestKey: entry.Key, request: request, packID: packID, pack: pack,
				backend: backend, options: options, actionErr: errors.New("placement backend is not enabled for ingest"), result: &result,
			})
		}
		if placementLimitReached(backend, pack.PhysicalSize, requestsUsed[backend.Hash], bytesUsed[backend.Hash], options) {
			result.Deferred++
			return nil
		}
		if request.Operation == schema.PlacementRequestEvict {
			allowed, err := evictionPreservesDurability(ctx, store, packID, backend.Hash, backends, options.Model.Policy)
			if err != nil {
				return err
			}
			if !allowed {
				return failPlacementRequest(ctx, placementFailure{
					store: store, requestKey: entry.Key, request: request, packID: packID, pack: pack,
					backend: backend, options: options, actionErr: errors.New("eviction would breach durability"), result: &result,
				})
			}
		}
		request.Attempts++
		request.LastAttempt = options.Now.UnixNano()
		request.LastError = ""
		request.NotBefore = 0
		if err := persistPlacementAttempt(ctx, store, entry.Key, request, packID, pack, backend); err != nil {
			return err
		}
		result.Attempted++
		requestsUsed[backend.Hash]++
		bytesUsed[backend.Hash] += pack.PhysicalSize
		var actionErr error
		switch request.Operation {
		case schema.PlacementRequestPlace:
			actionErr = actions.Place(ctx, packID, backend)
		case schema.PlacementRequestPromote:
			actionErr = actions.Promote(ctx, packID, backend)
		case schema.PlacementRequestEvict:
			actionErr = actions.Evict(ctx, packID, backend)
		default:
			actionErr = schema.ErrMalformed
		}
		if actionErr != nil {
			if errors.Is(actionErr, ErrPlacementObsolete) {
				return store.WriteMutableBatch(ctx, nil, [][]byte{entry.Key}, false)
			}
			return failPlacementRequest(ctx, placementFailure{
				store: store, requestKey: entry.Key, request: request, packID: packID, pack: pack,
				backend: backend, options: options, actionErr: actionErr, result: &result,
			})
		}
		if err := completePlacementRequest(ctx, store, entry.Key, request.Operation, packID, pack, backend, options.Now); err != nil {
			return err
		}
		result.BytesMoved += pack.PhysicalSize
		switch request.Operation {
		case schema.PlacementRequestPlace:
			result.Placed++
		case schema.PlacementRequestPromote:
			result.Promoted++
		case schema.PlacementRequestEvict:
			result.Evicted++
		}
		if options.MaxRequests != 0 && result.Attempted >= options.MaxRequests {
			workerErr = nil
			return errPlacementWorkerLimit
		}
		if options.MaxBytes != 0 && result.BytesMoved >= options.MaxBytes {
			workerErr = nil
			return errPlacementWorkerLimit
		}
		return nil
	})
	if errors.Is(err, errPlacementWorkerLimit) {
		return result, workerErr
	}
	return result, err
}

var errPlacementWorkerLimit = errors.New("placement worker limit reached")

func loadPack(ctx context.Context, store Store, packID vaultic.ID) (schema.PackRecord, bool, error) {
	value, found, err := store.Get(ctx, schema.PackKey(schema.ID(packID)))
	if err != nil || !found {
		return schema.PackRecord{}, found, err
	}
	record, err := schema.UnmarshalPackRecord(value)
	return record, true, err
}

func placementLimitReached(backend PlacementBackend, size, requests, bytes uint64, options PlacementWorkerOptions) bool {
	if backend.MaxRequestsPerSecond != 0 {
		limit := uint64(float64(backend.MaxRequestsPerSecond) * options.Window.Seconds())
		if limit == 0 || requests >= limit {
			return true
		}
	}
	if backend.MaxBandwidthBytes != 0 {
		limit := uint64(float64(backend.MaxBandwidthBytes) * options.Window.Seconds())
		if bytes != 0 && (bytes > math.MaxUint64-size || bytes+size > limit) {
			return true
		}
	}
	return false
}

func persistPlacementAttempt(
	ctx context.Context,
	store Store,
	requestKey []byte,
	request schema.PlacementRequestRecord,
	packID vaultic.ID,
	pack schema.PackRecord,
	backend PlacementBackend,
) error {
	requestValue, err := request.MarshalBinary()
	if err != nil {
		return err
	}
	placement, err := currentPlacement(ctx, store, packID, backend.Hash)
	if err != nil {
		return err
	}
	placement.State = schema.PlacementPending
	placement.Bytes = pack.PhysicalSize
	placement.DeleteAfter = 0
	if placement.RetentionSource == 0 {
		placement.RetentionSource = schema.RetentionUnknown
	}
	mutations, err := placementMutationsForMaintenance(schema.ID(packID), placementSet{backend.Hash: placement})
	if err != nil {
		return err
	}
	mutations = append(mutations, daemon.Mutation{Key: requestKey, Value: requestValue})
	return store.WriteMutableBatch(ctx, mutations, nil, false)
}

type placementFailure struct {
	store      Store
	requestKey []byte
	request    schema.PlacementRequestRecord
	packID     vaultic.ID
	pack       schema.PackRecord
	backend    PlacementBackend
	options    PlacementWorkerOptions
	actionErr  error
	result     *PlacementWorkerResult
}

func failPlacementRequest(ctx context.Context, failure placementFailure) error {
	failure.request.LastError = failure.actionErr.Error()
	shift := min(max(failure.request.Attempts, 1)-1, 16)
	failure.request.NotBefore = failure.options.Now.Add(failure.options.RetryBase * time.Duration(uint64(1)<<shift)).UnixNano()
	requestValue, err := failure.request.MarshalBinary()
	if err != nil {
		return err
	}
	placement, err := currentPlacement(ctx, failure.store, failure.packID, failure.backend.Hash)
	if err != nil {
		return err
	}
	placement.State = schema.PlacementFailed
	placement.Bytes = failure.pack.PhysicalSize
	placement.DeleteAfter = 0
	if placement.RetentionSource == 0 {
		placement.RetentionSource = schema.RetentionUnknown
	}
	mutations, err := placementMutationsForMaintenance(schema.ID(failure.packID), placementSet{failure.backend.Hash: placement})
	if err != nil {
		return err
	}
	mutations = append(mutations, daemon.Mutation{Key: failure.requestKey, Value: requestValue})
	if err := failure.store.WriteMutableBatch(ctx, mutations, nil, false); err != nil {
		return err
	}
	failure.result.Failed++
	return recordPlacementEvent(ctx, failure.store, failure.packID, failure.pack, failure.backend.Hash,
		schema.EventPlacementFailed, "scheduler_action_failed")
}

func completePlacementRequest(
	ctx context.Context,
	store Store,
	requestKey []byte,
	operation schema.PlacementRequestOperation,
	packID vaultic.ID,
	pack schema.PackRecord,
	backend PlacementBackend,
	now time.Time,
) error {
	var puts []daemon.Mutation
	eventType := schema.EventPromoted
	reason := "scheduler_promotion"
	state := schema.PlacementLive
	if operation == schema.PlacementRequestPromote {
		state = schema.PlacementEvicted
	} else {
		eventType, reason = schema.EventPlaced, "scheduler_placement"
		if operation == schema.PlacementRequestEvict {
			state, eventType, reason = schema.PlacementEvicted, schema.EventEvicted, "scheduler_eviction"
		}
	}
	placement, err := currentPlacement(ctx, store, packID, backend.Hash)
	if err != nil {
		return err
	}
	placement.State = state
	placement.Bytes = pack.PhysicalSize
	placement.StorageClass = backend.RetrievalClass
	if operation == schema.PlacementRequestEvict || operation == schema.PlacementRequestPromote {
		placement.DeleteAfter = now.UnixNano()
	} else {
		placement.PlacedAt = now.UnixNano()
		placement.PlacementTimeKnown = true
		placement.DeleteAfter = 0
		if backend.MinRetentionSeconds != 0 {
			placement.MinRetentionUntil = now.Add(time.Duration(backend.MinRetentionSeconds) * time.Second).UnixNano()
			placement.RetentionSource = schema.RetentionBackend
		} else if placement.RetentionSource == 0 {
			placement.RetentionSource = schema.RetentionUnknown
		}
	}
	puts, err = placementMutationsForMaintenance(schema.ID(packID), placementSet{backend.Hash: placement})
	if err != nil {
		return err
	}
	if err := store.WriteMutableBatch(ctx, puts, [][]byte{requestKey}, false); err != nil {
		return err
	}
	return recordPlacementEvent(ctx, store, packID, pack, backend.Hash, eventType, reason)
}

func currentPlacement(ctx context.Context, store Store, packID vaultic.ID, backend uint64) (schema.PlacementRecord, error) {
	value, found, err := store.Get(ctx, schema.PackPlacementKey(schema.ID(packID), backend))
	if err != nil || !found {
		return schema.PlacementRecord{RetentionSource: schema.RetentionUnknown}, err
	}
	return schema.UnmarshalPlacementRecord(value)
}

func recordPlacementEvent(
	ctx context.Context,
	store Store,
	packID vaultic.ID,
	pack schema.PackRecord,
	backend uint64,
	eventType schema.PackEventType,
	reason string,
) error {
	eventStore, ok := store.(placementEventStore)
	if !ok {
		return nil
	}
	return eventStore.RecordPackEvents(ctx, []daemon.PackEvent{{
		PackID: schema.ID(packID), Record: schema.PackHistoryEvent{
			Type: eventType, PackType: pack.Type, Backend: backend,
			PhysicalSize: pack.PhysicalSize, PayloadSize: pack.PayloadSize, ReasonCode: reason,
		},
	}})
}

func evictionPreservesDurability(
	ctx context.Context,
	store Store,
	packID vaultic.ID,
	removeBackend uint64,
	backends map[uint64]PlacementBackend,
	policy DurabilityPolicy,
) (bool, error) {
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return false, err
	}
	remaining := make(placementSet, len(placements[packID]))
	for backend, placement := range placements[packID] {
		if backend != removeBackend {
			remaining[backend] = placement
		}
	}
	return durable(remaining, backends, policy), nil
}

//nolint:gocognit,gocyclo // Existing domain flow is an explicit complexity exception; new code remains gated.
func PlanPlacement(ctx context.Context, store Store, options PlacementSchedulerOptions) (PlacementSchedulerResult, error) {
	result := PlacementSchedulerResult{SchemaVersion: IntrospectSchemaVersion}
	if options.Now.IsZero() {
		options.Now = time.Now()
	}
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return result, err
	}
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return result, err
	}
	requests, err := loadPlacementRequests(ctx, store)
	if err != nil {
		return result, err
	}
	promoted, err := loadPromotedPacks(ctx, store)
	if err != nil {
		return result, err
	}
	eligibility, err := loadPromotionEligibility(ctx, store)
	if err != nil {
		return result, err
	}
	backendByHash := map[uint64]PlacementBackend{}
	for _, backend := range options.Model.Backends {
		backendByHash[backend.Hash] = backend
	}
	puts := make([]daemon.Mutation, 0)
	deletes := make([][]byte, 0)
	for packID, pack := range packs {
		if pack.Lifecycle == schema.PackDeleted || pack.Lifecycle == schema.PackDeletePending {
			continue
		}
		_, wasPromoted := promoted[packID]
		status := placementStatus(packID, pack, placements[packID], eligibility[packID], options.Model, backendByHash, options.Now, wasPromoted)
		result.PacksScanned++
		if !status.Durable {
			result.Unsatisfied++
		}
		if status.Overdue {
			result.Overdue++
		}
		if status.PendingPromotion {
			result.PendingPromotion++
		}
		//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
		if !status.Durable || len(status.MissingBackends) != 0 || status.PendingPromotion || len(status.ExcessBackends) != 0 {
			if !status.Durable && status.Deadline != 0 && (result.OldestUnsatisfiedDeadline == 0 || status.Deadline < result.OldestUnsatisfiedDeadline) {
				result.OldestUnsatisfiedDeadline = status.Deadline
			}
			target, operation, ok := nextPlacementAction(status, options.Model)
			if !ok {
				continue
			}
			record := schema.PlacementRequestRecord{
				Classes: []string{status.Class}, Operation: operation, TargetBackend: target.Hash,
			}
			deadline := uint64(status.Deadline / int64(time.Second))
			if deadline == 0 {
				deadline = uint64(options.Now.Unix())
			}
			switch operation {
			case schema.PlacementRequestPromote:
				deadline = math.MaxUint64 - 1
			case schema.PlacementRequestEvict:
				deadline = math.MaxUint64
			case schema.PlacementRequestPlace:
				// Keep the policy-derived deadline for ordinary placement.
			}
			key := schema.PlacementRequestKey(deadline, schema.ID(packID))
			var existingValue []byte
			if existing, found := requests[packID]; found {
				if existing.record.Operation == record.Operation && existing.record.TargetBackend == record.TargetBackend {
					record.Attempts = existing.record.Attempts
					record.LastAttempt = existing.record.LastAttempt
					record.NotBefore = existing.record.NotBefore
					record.LastError = existing.record.LastError
				}
				if string(existing.key) != string(key) {
					deletes = append(deletes, existing.key)
				} else {
					existingValue = existing.value
				}
				delete(requests, packID)
			}
			value, err := record.MarshalBinary()
			if err != nil {
				return result, err
			}
			if !bytes.Equal(existingValue, value) {
				puts = append(puts, daemon.Mutation{Key: key, Value: value})
			}
		}
		result.Statuses = append(result.Statuses, status)
	}
	for _, request := range requests {
		deletes = append(deletes, request.key)
	}
	sort.Slice(result.Statuses, func(i, j int) bool {
		if result.Statuses[i].Deadline != result.Statuses[j].Deadline {
			return result.Statuses[i].Deadline < result.Statuses[j].Deadline
		}
		return result.Statuses[i].PackID < result.Statuses[j].PackID
	})
	result.RequestsWritten = uint64(len(puts))
	if !options.DryRun && (len(puts) != 0 || len(deletes) != 0) {
		return result, store.WriteMutableBatch(ctx, puts, deletes, false)
	}
	return result, nil
}

func PlanPoolMigration(ctx context.Context, store Store, options PlacementMigrationOptions) (PlacementSchedulerResult, error) {
	result := PlacementSchedulerResult{SchemaVersion: IntrospectSchemaVersion}
	if options.Now.IsZero() {
		options.Now = time.Now()
	}
	from, found := backendByID(options.Model, options.From)
	if !found {
		return result, fmt.Errorf("source placement backend %q is not configured", options.From)
	}
	target, found := backendByID(options.Model, options.To)
	if !found {
		return result, fmt.Errorf("target placement backend %q is not configured", options.To)
	}
	if !from.readAllowed() {
		return result, fmt.Errorf("source placement backend %q is not read-enabled", options.From)
	}
	if !target.ingestEnabled() {
		return result, fmt.Errorf("target placement backend %q is not ingest-enabled", options.To)
	}
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return result, err
	}
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return result, err
	}
	requests, err := loadPlacementRequests(ctx, store)
	if err != nil {
		return result, err
	}
	puts := make([]daemon.Mutation, 0)
	deadline := uint64(options.Now.Unix())
	for packID, packPlacements := range placements {
		pack, found := packs[packID]
		if !found || pack.Lifecycle == schema.PackDeleted || pack.Lifecycle == schema.PackDeletePending {
			continue
		}
		fromPlacement, hasSource := packPlacements[from.Hash]
		if !hasSource || fromPlacement.State != schema.PlacementLive {
			continue
		}
		if targetPlacement, hasTarget := packPlacements[target.Hash]; hasTarget && targetPlacement.State == schema.PlacementLive {
			continue
		}
		if existing, exists := requests[packID]; exists && existing.record.TargetBackend == target.Hash {
			continue
		}
		record := schema.PlacementRequestRecord{Classes: []string{"pool-migration"}, Operation: schema.PlacementRequestPlace, TargetBackend: target.Hash}
		value, err := record.MarshalBinary()
		if err != nil {
			return result, err
		}
		puts = append(puts, daemon.Mutation{Key: schema.PlacementRequestKey(deadline, schema.ID(packID)), Value: value})
	}
	result.PacksScanned = uint64(len(placements))
	result.RequestsWritten = uint64(len(puts))
	if options.DryRun || len(puts) == 0 {
		return result, nil
	}
	return result, store.WriteMutableBatch(ctx, puts, nil, false)
}

func backendByID(model PlacementModel, id string) (PlacementBackend, bool) {
	for _, backend := range model.Backends {
		if backend.ID == id {
			return backend, true
		}
	}
	return PlacementBackend{}, false
}

type placementRequest struct {
	key    []byte
	value  []byte
	record schema.PlacementRequestRecord
}

func loadPlacementRequests(ctx context.Context, store Store) (map[vaultic.ID]placementRequest, error) {
	requests := make(map[vaultic.ID]placementRequest)
	err := scan(ctx, store, schema.PlacementRequestPrefix(), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPlacementRequest {
			return schema.ErrMalformed
		}
		record, err := schema.UnmarshalPlacementRequestRecord(entry.Value)
		if err != nil {
			return err
		}
		packID := vaultic.ID(parsed.ID)
		requests[packID] = placementRequest{key: append([]byte(nil), entry.Key...), value: append([]byte(nil), entry.Value...), record: record}
		return nil
	})
	return requests, err
}

func loadPromotedPacks(ctx context.Context, store Store) (map[vaultic.ID]struct{}, error) {
	promoted := make(map[vaultic.ID]struct{})
	err := scan(ctx, store, []byte("rl:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyRepackLineage {
			return schema.ErrMalformed
		}
		record, err := schema.UnmarshalRepackLineageRecord(entry.Value)
		if err != nil {
			return err
		}
		if record.Kind == schema.LineagePromotion {
			promoted[vaultic.ID(parsed.SecondID)] = struct{}{}
		}
		return nil
	})
	return promoted, err
}

func loadPromotionEligibility(ctx context.Context, store Store) (map[vaultic.ID]schema.PromotionEligibilityRecord, error) {
	result := make(map[vaultic.ID]schema.PromotionEligibilityRecord)
	err := scan(ctx, store, schema.PromotionEligibilityPrefix(), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPromotionEligibility {
			return schema.ErrMalformed
		}
		record, err := schema.UnmarshalPromotionEligibilityRecord(entry.Value)
		if err != nil {
			return err
		}
		result[vaultic.ID(parsed.ID)] = record
		return nil
	})
	return result, err
}

func nextPlacementAction(status PlacementStatus, model PlacementModel) (PlacementBackend, schema.PlacementRequestOperation, bool) {
	missing := make(map[string]struct{}, len(status.MissingBackends))
	for _, backend := range status.MissingBackends {
		missing[backend] = struct{}{}
	}
	for _, backend := range placementTargets(status.Class, model) {
		if _, ok := missing[backend.ID]; ok {
			operation := schema.PlacementRequestPlace
			if status.Class == "archival-data" {
				operation = schema.PlacementRequestPromote
			}
			return backend, operation, true
		}
	}
	for _, excess := range status.ExcessBackends {
		for _, backend := range model.Backends {
			if backend.ID == excess {
				return backend, schema.PlacementRequestEvict, true
			}
		}
	}
	return PlacementBackend{}, 0, false
}

func placementStatus(
	packID vaultic.ID,
	pack schema.PackRecord,
	placements placementSet,
	eligibility schema.PromotionEligibilityRecord,
	model PlacementModel,
	backends map[uint64]PlacementBackend,
	now time.Time,
	wasPromoted bool,
) PlacementStatus {
	class := placementClass(pack, eligibility, model, now, wasPromoted)
	targets := placementTargets(class, model)
	live := make(map[uint64]struct{})
	status := PlacementStatus{PackID: packID.String(), PackType: packTypeName(pack.Type), Class: class}
	for _, backend := range targets {
		status.TargetBackends = append(status.TargetBackends, backend.ID)
	}
	targetHashes := make(map[uint64]struct{}, len(targets))
	for _, backend := range targets {
		targetHashes[backend.Hash] = struct{}{}
	}
	status.ExcessBackends = excessBackends(placements, targetHashes, model, backends, now)
	for backendHash, placement := range placements {
		if placement.State != schema.PlacementLive {
			continue
		}
		live[backendHash] = struct{}{}
		status.LiveBackends = append(status.LiveBackends, backendName(backendHash, model))
	}
	for _, backend := range targets {
		if _, ok := live[backend.Hash]; !ok {
			status.MissingBackends = append(status.MissingBackends, backend.ID)
		}
	}
	status.Durable = durable(placements, backends, model.Policy)
	if !status.Durable && model.Policy.MinOffsite != 0 && pack.CreationTimeKnown && modelPolicyDeadline(model, pack) != 0 {
		status.Deadline = pack.CreationTime + modelPolicyDeadline(model, pack)
		status.Overdue = now.UnixNano() > status.Deadline
	}
	status.PendingPromotion = class == "archival-data" && len(status.MissingBackends) != 0
	return status
}

func excessBackends(
	placements placementSet,
	targets map[uint64]struct{},
	model PlacementModel,
	backends map[uint64]PlacementBackend,
	now time.Time,
) []string {
	var excess []string
	for backendHash, placement := range placements {
		if placement.State != schema.PlacementLive || placement.MinRetentionUntil > now.UnixNano() {
			continue
		}
		if _, targeted := targets[backendHash]; targeted {
			continue
		}
		remaining := make(placementSet, len(placements)-1)
		for candidateHash, candidate := range placements {
			if candidateHash != backendHash {
				remaining[candidateHash] = candidate
			}
		}
		if durable(remaining, backends, model.Policy) {
			excess = append(excess, backendName(backendHash, model))
		}
	}
	sort.Strings(excess)
	return excess
}

const defaultPromotionCrossover = 8 * 24 * time.Hour

func placementClass(pack schema.PackRecord, eligibility schema.PromotionEligibilityRecord, model PlacementModel, now time.Time, wasPromoted bool) string {
	switch pack.Type {
	case schema.PackTree:
		return "metadata"
	case schema.PackData:
		if wasPromoted {
			return "archival-data"
		}
		if archivalPromotionDue(pack, eligibility, model, now) {
			return "archival-data"
		}
		return "recent-data"
	default:
		return "cache"
	}
}

func archivalPromotionDue(pack schema.PackRecord, eligibility schema.PromotionEligibilityRecord, model PlacementModel, now time.Time) bool {
	if !pack.CreationTimeKnown || !pack.UsageKnown || pack.UsedPayloadBytes == 0 {
		return false
	}
	if eligibility.EvaluatedAt < pack.CreationTime || (!eligibility.Indefinite && eligibility.SurvivalUntil <= 0) {
		return false
	}
	var maximumRetention time.Duration
	var hasArchival bool
	for _, backend := range model.Backends {
		if backend.Role == "archival" {
			hasArchival = true
			retention := time.Duration(backend.MinRetentionSeconds) * time.Second
			if retention > maximumRetention {
				maximumRetention = retention
			}
		}
	}
	if !hasArchival {
		return false
	}
	crossover := time.Duration(model.Policy.PromotionCrossoverSeconds) * time.Second
	if crossover <= 0 {
		crossover = defaultPromotionCrossover
	}
	if now.UnixNano() < pack.CreationTime+int64(crossover) {
		return false
	}
	return eligibility.Indefinite || eligibility.SurvivalUntil >= now.Add(maximumRetention).UnixNano()
}

func placementTargets(class string, model PlacementModel) []PlacementBackend {
	result := make([]PlacementBackend, 0)
	for _, backend := range model.Backends {
		if !backend.ingestEnabled() {
			continue
		}
		switch class {
		case "metadata":
			if backend.Role != "archival" && backend.Role != "cache" {
				result = append(result, backend)
			}
		case "recent-data":
			if backend.Role == "primary" || (backend.Offsite && backend.Role != "archival") {
				result = append(result, backend)
			}
		case "archival-data":
			if backend.Role == "archival" {
				result = append(result, backend)
			}
		case "cache":
			if backend.Role == "cache" || backend.Role == "primary" {
				result = append(result, backend)
			}
		}
	}
	return result
}

func modelPolicyDeadline(model PlacementModel, pack schema.PackRecord) int64 {
	_ = pack
	return model.Policy.OffsiteDeadline * int64(time.Second)
}
