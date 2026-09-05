package verification

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const pageSize = 1_000

type Store interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error)
	RecordVerification(context.Context, daemon.VerificationOutcome) error
}

type Candidate struct {
	PackID    schema.ID
	Backend   uint64
	Pack      schema.PackRecord
	Placement schema.PlacementRecord
	State     schema.VerificationStateRecord
	HasState  bool
	score     [32]byte
}

type Options struct {
	Tiers            map[schema.PackTier]bool
	Backends         map[uint64]bool
	StorageClasses   map[string]bool
	PackTypes        map[schema.PackType]bool
	CreatedAfter     *int64
	CreatedBefore    *int64
	MinSize          *uint64
	MaxSize          *uint64
	RetentionStatus  string
	NotVerifiedSince *int64
	ErrorsOnly       bool
	Level            schema.VerificationLevel
	SampleCount      int
	SamplePercent    float64
	Seed             uint64
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func Plan(ctx context.Context, store Store, options Options, now time.Time) ([]Candidate, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	var candidates []Candidate
	err := scan(ctx, store, []byte("p:"), func(kv daemon.KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		if parsed.Kind != schema.KeyPack {
			return nil
		}
		pack, err := schema.UnmarshalPackRecord(kv.Value)
		if err != nil {
			return err
		}
		if !matchPack(pack, options, now.Unix()) {
			return nil
		}
		return scan(ctx, store, schema.PackPlacementPrefix(parsed.ID), func(placementKV daemon.KeyValue) error {
			placementKey, err := schema.ParseKey(placementKV.Key)
			if err != nil {
				return err
			}
			placement, err := schema.UnmarshalPlacementRecord(placementKV.Value)
			if err != nil {
				return err
			}
			if placement.State != schema.PlacementLive ||
				(len(options.Backends) > 0 && !options.Backends[placementKey.Backend]) ||
				(len(options.StorageClasses) > 0 && !options.StorageClasses[placement.StorageClass]) {
				return nil
			}
			candidate := Candidate{PackID: parsed.ID, Backend: placementKey.Backend, Pack: pack, Placement: placement}
			if value, found, err := store.Get(ctx, schema.VerificationStateKey(parsed.ID, placementKey.Backend)); err != nil {
				return err
			} else if found {
				candidate.State, err = schema.UnmarshalVerificationStateRecord(value)
				if err != nil {
					return err
				}
				candidate.HasState = true
			}
			if options.ErrorsOnly &&
				(!candidate.HasState ||
					(candidate.State.Result != schema.VerificationIntegrityError &&
						candidate.State.Result != schema.VerificationOperationalError)) {
				return nil
			}
			if options.NotVerifiedSince != nil &&
				lastSuccess(candidate.State, options.Level) >= *options.NotVerifiedSince {
				return nil
			}
			candidate.score = sampleScore(options.Seed, candidate.PackID, candidate.Backend)
			candidates = append(candidates, candidate)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(
		candidates,
		func(i, j int) bool { return bytes.Compare(candidates[i].score[:], candidates[j].score[:]) < 0 },
	)
	limit := len(candidates)
	if options.SampleCount > 0 && options.SampleCount < limit {
		limit = options.SampleCount
	}
	if options.SamplePercent > 0 {
		limit = int(math.Ceil(float64(len(candidates)) * options.SamplePercent / 100))
	}
	candidates = candidates[:limit]
	sort.Slice(candidates, func(i, j int) bool {
		if value := bytes.Compare(candidates[i].PackID[:], candidates[j].PackID[:]); value != 0 {
			return value < 0
		}
		return candidates[i].Backend < candidates[j].Backend
	})
	return candidates, nil
}

func validateOptions(options Options) error {
	if options.Level < schema.VerificationHeader || options.Level > schema.VerificationFull ||
		options.SampleCount < 0 || options.SamplePercent < 0 || options.SamplePercent > 100 ||
		(options.SampleCount > 0 && options.SamplePercent > 0) {
		return fmt.Errorf("invalid verification selection options")
	}
	return nil
}

func matchPack(pack schema.PackRecord, options Options, now int64) bool {
	if len(options.Tiers) > 0 && !options.Tiers[pack.Tier] ||
		len(options.PackTypes) > 0 && !options.PackTypes[pack.Type] {
		return false
	}
	if options.CreatedAfter != nil && (!pack.CreationTimeKnown || pack.CreationTime < *options.CreatedAfter) ||
		options.CreatedBefore != nil && (!pack.CreationTimeKnown || pack.CreationTime >= *options.CreatedBefore) {
		return false
	}
	if options.MinSize != nil && (!pack.PhysicalSizeKnown || pack.PhysicalSize < *options.MinSize) ||
		options.MaxSize != nil && (!pack.PhysicalSizeKnown || pack.PhysicalSize > *options.MaxSize) {
		return false
	}
	switch options.RetentionStatus {
	case "":
	case "active":
		return pack.RetentionSource != schema.RetentionUnknown && pack.MinRetentionUntil > now
	case "expired":
		return pack.RetentionSource != schema.RetentionUnknown && pack.MinRetentionUntil <= now
	default:
		return false
	}
	return true
}

func lastSuccess(state schema.VerificationStateRecord, level schema.VerificationLevel) int64 {
	switch level {
	case schema.VerificationHeader:
		return state.HeaderVerifiedAt
	case schema.VerificationChecksum:
		return state.ChecksumVerifiedAt
	case schema.VerificationFull:
		return state.FullVerifiedAt
	default:
		return 0
	}
}

func sampleScore(seed uint64, pack schema.ID, backend uint64) [32]byte {
	var data [48]byte
	binary.BigEndian.PutUint64(data[:8], seed)
	copy(data[8:40], pack[:])
	binary.BigEndian.PutUint64(data[40:], backend)
	return sha256.Sum256(data[:])
}

func scan(ctx context.Context, store Store, prefix []byte, visit func(daemon.KeyValue) error) error {
	var cursor []byte
	for {
		items, done, err := store.ScanPrefix(ctx, prefix, cursor, pageSize)
		if err != nil {
			return err
		}
		for _, item := range items {
			if err := visit(item); err != nil {
				return err
			}
			cursor = append(cursor[:0], item.Key...)
		}
		if done {
			return nil
		}
		if len(items) == 0 {
			return fmt.Errorf("verification scan made no progress")
		}
	}
}

type Verifier interface {
	VerifyPackPlacement(context.Context, schema.ID, uint64, schema.VerificationLevel) error
}

type Warmer interface {
	WarmupPlacements(context.Context, []Candidate) error
}

type Result struct {
	Candidates        int `json:"candidates"`
	Verified          int `json:"verified"`
	OperationalErrors int `json:"operational_errors"`
	IntegrityErrors   int `json:"integrity_errors"`
}

type classifiedError interface {
	VerificationClassification() (schema.VerificationClassification, string, string)
}

func Run(
	ctx context.Context,
	store Store,
	verifier Verifier,
	warmer Warmer,
	candidates []Candidate,
	level schema.VerificationLevel,
	concurrency int,
	now func() time.Time,
) (Result, error) {
	if concurrency <= 0 {
		return Result{}, fmt.Errorf("verification concurrency must be positive")
	}
	result := Result{Candidates: len(candidates)}
	var runNonce [32]byte
	if _, err := rand.Read(runNonce[:]); err != nil {
		return result, fmt.Errorf("generate verification run ID: %w", err)
	}
	if err := warmupCandidates(ctx, store, warmer, candidates, level, runNonce, now, &result); err != nil {
		return result, err
	}
	jobs := make(chan Candidate)
	var mutex sync.Mutex
	var failures []error
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go verificationWorker(ctx, store, verifier, jobs, level, runNonce, now, &result, &failures, &mutex, &workers)
	}
sendLoop:
	for _, candidate := range candidates {
		select {
		case jobs <- candidate:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	workers.Wait()
	return result, errors.Join(failures...)
}

func warmupCandidates(
	ctx context.Context,
	store Store,
	warmer Warmer,
	candidates []Candidate,
	level schema.VerificationLevel,
	nonce [32]byte,
	now func() time.Time,
	result *Result,
) error {
	if warmer == nil || level <= schema.VerificationHeader {
		return nil
	}
	err := warmer.WarmupPlacements(ctx, candidates)
	if err == nil {
		return nil
	}
	classification := schema.VerificationWarmupTimeout
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
		classification = schema.VerificationCancelled
	}
	failures := []error{err}
	for _, candidate := range candidates {
		outcome := daemon.VerificationOutcome{
			PackID: candidate.PackID, Backend: candidate.Backend, Level: level, CompletedAt: now(),
			RunID: verificationRunID(nonce, candidate), Classification: classification,
		}
		if recordErr := recordOutcome(ctx, store, outcome); recordErr != nil {
			failures = append(failures, recordErr)
		} else {
			result.OperationalErrors++
		}
	}
	return errors.Join(failures...)
}

func verificationWorker(
	ctx context.Context,
	store Store,
	verifier Verifier,
	jobs <-chan Candidate,
	level schema.VerificationLevel,
	nonce [32]byte,
	now func() time.Time,
	result *Result,
	failures *[]error,
	mutex *sync.Mutex,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	for candidate := range jobs {
		outcome := daemon.VerificationOutcome{
			PackID: candidate.PackID, Backend: candidate.Backend, Level: level,
			CompletedAt: now(), RunID: verificationRunID(nonce, candidate),
		}
		verifyErr := verifier.VerifyPackPlacement(ctx, candidate.PackID, candidate.Backend, level)
		if verifyErr != nil {
			outcome.Classification = schema.VerificationTransport
			if errors.Is(verifyErr, context.Canceled) || errors.Is(verifyErr, context.DeadlineExceeded) && ctx.Err() != nil {
				outcome.Classification = schema.VerificationCancelled
			} else if classified, ok := verifyErr.(classifiedError); ok {
				outcome.Classification, outcome.Expected, outcome.Observed = classified.VerificationClassification()
			}
		}
		if err := recordOutcome(ctx, store, outcome); err != nil {
			mutex.Lock()
			*failures = append(*failures, err)
			mutex.Unlock()
			continue
		}
		mutex.Lock()
		switch {
		case outcome.Classification == schema.VerificationNoError:
			result.Verified++
		case outcome.Classification.IsIntegrity():
			result.IntegrityErrors++
		default:
			result.OperationalErrors++
		}
		if verifyErr != nil {
			*failures = append(*failures, verifyErr)
		}
		mutex.Unlock()
	}
}

func verificationRunID(runNonce [32]byte, candidate Candidate) schema.ID {
	input := append(append([]byte(nil), runNonce[:]...), candidate.PackID[:]...)
	input = binary.BigEndian.AppendUint64(input, candidate.Backend)
	return schema.ID(sha256.Sum256(input))
}

func recordOutcome(ctx context.Context, store Store, outcome daemon.VerificationOutcome) error {
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return store.RecordVerification(recordCtx, outcome)
}
