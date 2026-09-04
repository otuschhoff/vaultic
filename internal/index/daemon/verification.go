package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type VerificationOutcome struct {
	PackID         schema.ID
	Backend        uint64
	Level          schema.VerificationLevel
	CompletedAt    time.Time
	RunID          schema.ID
	Classification schema.VerificationClassification
	Expected       string
	Observed       string
}

func (store *SchemaStore) RecordVerification(ctx context.Context, outcome VerificationOutcome) error {
	if outcome.PackID == (schema.ID{}) || outcome.Backend == 0 || outcome.RunID == (schema.ID{}) ||
		outcome.CompletedAt.IsZero() {
		return fmt.Errorf("invalid verification outcome identity")
	}
	for range revisionAllocationAttempts {
		err := store.recordVerification(ctx, outcome)
		if status.Code(err) != codes.Aborted {
			return err
		}
	}
	return fmt.Errorf("record verification: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) recordVerification(ctx context.Context, outcome VerificationOutcome) error {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error { rollbackTransaction(ctx, transaction); return err }
	stateKey := schema.VerificationStateKey(outcome.PackID, outcome.Backend)
	value, found, err := transaction.Get(ctx, stateKey)
	if err != nil {
		return fail(err)
	}
	state := schema.VerificationStateRecord{}
	if found {
		state, err = schema.UnmarshalVerificationStateRecord(value)
		if err != nil {
			return fail(err)
		}
		if state.LastRunID == outcome.RunID {
			return transaction.Rollback(ctx)
		}
	}
	completed := outcome.CompletedAt.Unix()
	state.LastAttemptAt, state.LastAttemptLevel, state.LastRunID = completed, outcome.Level, outcome.RunID
	var event *schema.VerificationEventRecord
	if outcome.Classification == schema.VerificationNoError {
		advanceVerificationSuccess(&state, outcome.Level, completed)
		if state.OpenFindingID != (schema.ID{}) && outcome.Level >= state.FindingLevel {
			event = &schema.VerificationEventRecord{
				Type:           schema.VerificationResolved,
				FindingID:      state.OpenFindingID,
				RunID:          outcome.RunID,
				Level:          state.FindingLevel,
				Classification: state.Classification,
				FirstDetected:  state.FirstErrorAt,
				LastDetected:   state.LastErrorAt,
				Occurrences:    state.ConsecutiveFailures,
				Resolution:     "successful verification",
			}
			state.OpenFindingID, state.FindingLevel, state.Classification = schema.ID{}, 0, schema.VerificationNoError
			state.FirstErrorAt, state.LastErrorAt, state.ConsecutiveFailures = 0, 0, 0
			state.Result = schema.VerificationHealthy
		} else if state.OpenFindingID == (schema.ID{}) {
			state.Result = schema.VerificationHealthy
		}
	} else {
		result := schema.VerificationOperationalError
		if outcome.Classification.IsIntegrity() {
			result = schema.VerificationIntegrityError
		}
		if state.OpenFindingID == (schema.ID{}) {
			state.OpenFindingID = verificationFindingID(outcome)
			state.FirstErrorAt = completed
			state.ConsecutiveFailures = 1
			event = &schema.VerificationEventRecord{Type: schema.VerificationDetected,
				FindingID:      state.OpenFindingID,
				RunID:          outcome.RunID,
				Level:          outcome.Level,
				Classification: outcome.Classification,
				FirstDetected:  completed,
				LastDetected:   completed,
				Occurrences:    1,
				Expected:       outcome.Expected,
				Observed:       outcome.Observed}
		} else if state.Classification != outcome.Classification || state.FindingLevel != outcome.Level {
			state.ConsecutiveFailures++
			event = &schema.VerificationEventRecord{Type: schema.VerificationChanged,
				FindingID:      state.OpenFindingID,
				RunID:          outcome.RunID,
				Level:          outcome.Level,
				Classification: outcome.Classification,
				FirstDetected:  state.FirstErrorAt,
				LastDetected:   completed,
				Occurrences:    state.ConsecutiveFailures,
				Expected:       outcome.Expected,
				Observed:       outcome.Observed}
		} else {
			state.ConsecutiveFailures++
		}
		state.Result, state.FindingLevel, state.Classification, state.LastErrorAt = result, outcome.Level, outcome.Classification, completed
	}
	encodedState, err := state.MarshalBinary()
	if err != nil {
		return fail(err)
	}
	puts := []Mutation{{Key: stateKey, Value: encodedState}}
	if outcome.Classification == schema.VerificationNoError {
		placementKey := schema.PackPlacementKey(outcome.PackID, outcome.Backend)
		if placementValue, exists, getErr := transaction.Get(ctx, placementKey); getErr != nil {
			return fail(getErr)
		} else if exists {
			placement, decodeErr := schema.UnmarshalPlacementRecord(placementValue)
			if decodeErr != nil {
				return fail(decodeErr)
			}
			if completed > placement.LastVerifiedAt {
				placement.LastVerifiedAt = completed
				encoded, encodeErr := placement.MarshalBinary()
				if encodeErr != nil {
					return fail(encodeErr)
				}
				puts = append(puts, Mutation{Key: placementKey, Value: encoded})
			}
		}
	}
	if event != nil {
		eventPuts, appendErr := appendVerificationEvent(ctx, transaction, outcome, *event)
		if appendErr != nil {
			return fail(appendErr)
		}
		puts = append(puts, eventPuts...)
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fail(err)
	}
	return nil
}

func advanceVerificationSuccess(
	state *schema.VerificationStateRecord,
	level schema.VerificationLevel,
	completed int64,
) {
	if completed > state.HeaderVerifiedAt {
		state.HeaderVerifiedAt = completed
	}
	if level >= schema.VerificationChecksum && completed > state.ChecksumVerifiedAt {
		state.ChecksumVerifiedAt = completed
	}
	if level >= schema.VerificationFull && completed > state.FullVerifiedAt {
		state.FullVerifiedAt = completed
	}
}

func verificationFindingID(outcome VerificationOutcome) schema.ID {
	hash := sha256.New()
	hash.Write(outcome.RunID[:])
	hash.Write(outcome.PackID[:])
	var backend [8]byte
	binary.BigEndian.PutUint64(backend[:], outcome.Backend)
	hash.Write(backend[:])
	var result schema.ID
	copy(result[:], hash.Sum(nil))
	return result
}

func appendVerificationEvent(
	ctx context.Context,
	transaction *Transaction,
	outcome VerificationOutcome,
	event schema.VerificationEventRecord,
) ([]Mutation, error) {
	sequenceKey := schema.NextEventSequenceKey()
	value, found, err := transaction.Get(ctx, sequenceKey)
	if err != nil {
		return nil, err
	}
	sequence := uint64(1)
	if found {
		sequence, err = schema.UnmarshalNextEventSequence(value)
		if err != nil {
			return nil, err
		}
	}
	eventValue, err := event.MarshalBinary()
	if err != nil {
		return nil, err
	}
	nextValue, err := schema.MarshalNextEventSequence(sequence + 1)
	if err != nil {
		return nil, err
	}
	return []Mutation{
		{
			Key: schema.VerificationEventKey(
				uint64(outcome.CompletedAt.Unix()),
				sequence,
				outcome.PackID,
				outcome.Backend,
			),
			Value: eventValue,
		},
		{Key: sequenceKey, Value: nextValue},
	}, nil
}
