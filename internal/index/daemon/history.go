package daemon

import (
	"context"
	"fmt"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

// PackEvent is one pack lifecycle transition to record in the history log.
// Callers construct it alongside the catalog mutation that caused it.
type PackEvent struct {
	PackID schema.ID
	Record schema.PackHistoryEvent
}

// historyClock is overridable in tests so event ordering can be exercised
// without depending on wall-clock resolution.
var historyClock = time.Now

// appendPackHistory turns pack events into mutations, allocating a contiguous
// block of sequence numbers inside the caller's transaction so the events
// commit atomically with the catalog transition that produced them.
//
// History is advisory: an event that cannot be encoded is dropped rather than
// failing the transition it describes. Losing a history record degrades a
// report; failing the transition would lose a backup.
func appendPackHistory(ctx context.Context, transaction *Transaction, events []PackEvent) ([]Mutation, error) {
	if len(events) == 0 {
		return nil, nil
	}
	key := schema.NextEventSequenceKey()
	encoded, found, err := transaction.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	next := uint64(1)
	if found {
		if next, err = schema.UnmarshalNextEventSequence(encoded); err != nil {
			// A corrupt counter must not block the catalog transition. Skip
			// the events; the sequence is repaired by the next successful
			// allocation once the record is rewritten below.
			next = 1
		}
	}
	if math.MaxUint64-next < uint64(len(events)) {
		return nil, fmt.Errorf("pack history sequence exhausted")
	}

	seconds := historyClock().UTC().Unix()
	if seconds < 0 {
		seconds = 0
	}
	mutations := make([]Mutation, 0, len(events)+1)
	allocated := next
	for _, event := range events {
		value, encodeErr := event.Record.MarshalBinary()
		if encodeErr != nil {
			continue
		}
		mutations = append(mutations, Mutation{
			Key:   schema.PackHistoryKey(uint64(seconds), allocated, event.PackID),
			Value: value,
		})
		allocated++
	}
	if len(mutations) == 0 {
		return nil, nil
	}
	encodedNext, err := schema.MarshalNextEventSequence(allocated)
	if err != nil {
		return nil, err
	}
	return append(mutations, Mutation{Key: key, Value: encodedNext}), nil
}

// ensureHistoryEnabledMarker records when history collection began. Buckets
// covering earlier periods are reported as reconstructed rather than complete,
// so a repository that enabled history late never presents an inferred series
// as observed.
func ensureHistoryEnabledMarker(ctx context.Context, transaction *Transaction) ([]Mutation, error) {
	key := schema.HistoryEnabledAtKey()
	_, found, err := transaction.Get(ctx, key)
	if err != nil || found {
		return nil, err
	}
	seconds := historyClock().UTC().Unix()
	if seconds < 0 {
		seconds = 0
	}
	value, err := (schema.HistoryMarker{UnixSeconds: uint64(seconds)}).MarshalBinary()
	if err != nil {
		return nil, err
	}
	return []Mutation{{Key: key, Value: value}}, nil
}

// packHistoryMutations builds the history mutations for a transition, including
// the collection-enabled marker on first use.
func packHistoryMutations(ctx context.Context, transaction *Transaction, events []PackEvent) ([]Mutation, error) {
	if len(events) == 0 {
		return nil, nil
	}
	marker, err := ensureHistoryEnabledMarker(ctx, transaction)
	if err != nil {
		return nil, err
	}
	entries, err := appendPackHistory(ctx, transaction, events)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return append(marker, entries...), nil
}

// usageDeltas reports the signed change in reachable and unreachable payload
// bytes between two states of one pack. A pack whose usage was previously
// unknown contributes its whole new split, because nothing was counted before.
func usageDeltas(old *schema.PackRecord, current schema.PackRecord) (used, unused int64) {
	var oldUsed, oldUnused uint64
	if old != nil && old.UsageKnown {
		oldUsed, oldUnused = old.UsedPayloadBytes, old.UnusedPayloadBytes
	}
	if !current.UsageKnown {
		return -int64(oldUsed), -int64(oldUnused)
	}
	return int64(current.UsedPayloadBytes) - int64(oldUsed), int64(current.UnusedPayloadBytes) - int64(oldUnused)
}

// RecordPackEvents appends events that describe an observation rather than a
// catalog transition: a repack source whose contents moved elsewhere, a
// deletion that failed, or an orphan discovered on the backend.
//
// History is advisory, so a failure here is reported to the caller but must
// never be escalated into failing the operation being described.
func (store *SchemaStore) RecordPackEvents(ctx context.Context, events []PackEvent) error {
	if len(events) == 0 {
		return nil
	}
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.recordPackEventsOnce(ctx, events)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("record pack history: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) recordPackEventsOnce(ctx context.Context, events []PackEvent) error {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	mutations, err := packHistoryMutations(ctx, transaction, events)
	if err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if len(mutations) == 0 {
		return transaction.Rollback(ctx)
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), mutations, nil); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}
