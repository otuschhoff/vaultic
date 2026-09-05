package maintenance

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// HistoryEvent is one decoded raw event with its key coordinates.
type HistoryEvent struct {
	UnixSeconds uint64
	Sequence    uint64
	PackID      vaultic.ID
	Record      schema.PackHistoryEvent
}

// HistoryScanResult reports what a scan of the raw log found, including how
// many records were unreadable. History is advisory, so a corrupt record is
// counted and skipped rather than failing the scan.
type HistoryScanResult struct {
	Events    []HistoryEvent `json:"-"`
	Malformed uint64         `json:"malformed_events"`
}

// ScanHistory reads raw events in key order, which is time order followed by
// the globally monotonic sequence.
func ScanHistory(ctx context.Context, store Store, since, until uint64) (HistoryScanResult, error) {
	var result HistoryScanResult
	err := scan(ctx, store, []byte("ph:"), func(entry daemon.KeyValue) error {
		parsed, parseErr := schema.ParseKey(entry.Key)
		if parseErr != nil || parsed.Kind != schema.KeyPackHistory {
			result.Malformed++
			return nil
		}
		if parsed.EventTime < since || (until != 0 && parsed.EventTime >= until) {
			return nil
		}
		record, decodeErr := schema.UnmarshalPackHistoryEvent(entry.Value)
		if decodeErr != nil {
			result.Malformed++
			return nil
		}
		result.Events = append(result.Events, HistoryEvent{
			UnixSeconds: parsed.EventTime, Sequence: parsed.Revision,
			PackID: vaultic.ID(parsed.ID), Record: record,
		})
		return nil
	})
	return result, err
}

// BucketStart truncates a timestamp to the start of its bucket. Monthly
// buckets use UTC calendar months so a bucket boundary is stable regardless of
// the reader's timezone.
func BucketStart(granularity schema.HistoryGranularity, unixSeconds uint64) uint64 {
	moment := time.Unix(int64(unixSeconds), 0).UTC()
	switch granularity {
	case schema.GranularityHourly:
		return uint64(moment.Truncate(time.Hour).Unix())
	case schema.GranularityDaily:
		return uint64(time.Date(moment.Year(), moment.Month(), moment.Day(), 0, 0, 0, 0, time.UTC).Unix())
	case schema.GranularityMonthly:
		return uint64(time.Date(moment.Year(), moment.Month(), 1, 0, 0, 0, 0, time.UTC).Unix())
	}
	return unixSeconds
}

// bucketKeyOf identifies the bucket an event belongs to.
type bucketKeyOf struct {
	granularity schema.HistoryGranularity
	start       uint64
	backend     uint64
	packType    schema.PackType
}

// RollupResult summarises one rollup pass.
type RollupResult struct {
	EventsScanned    uint64 `json:"events_scanned"`
	MalformedEvents  uint64 `json:"malformed_events"`
	BucketsWritten   uint64 `json:"buckets_written"`
	BucketsUnchanged uint64 `json:"buckets_unchanged"`
	Partial          uint64 `json:"partial_buckets"`
	Reconstructed    uint64 `json:"reconstructed_buckets"`
}

// AccumulateBuckets folds raw events into buckets. It is a pure function of
// its inputs, which is what makes a rollup idempotent and independently
// verifiable against a direct scan of the raw log.
func AccumulateBuckets(events []HistoryEvent, granularities []schema.HistoryGranularity, rawFloor, enabledAt uint64) map[bucketKeyOf]schema.PackHistoryBucket {
	buckets := make(map[bucketKeyOf]schema.PackHistoryBucket)
	// An import describes a pack that existed before the import ran, so the
	// bucket containing it reports inferred rather than observed activity.
	imported := make(map[bucketKeyOf]bool)
	for _, event := range events {
		for _, granularity := range granularities {
			key := bucketKeyOf{
				granularity: granularity,
				start:       BucketStart(granularity, event.UnixSeconds),
				backend:     event.Record.Backend,
				packType:    event.Record.PackType,
			}
			bucket := buckets[key]
			applyEventToBucket(&bucket, event.Record)
			bucket.EventsObserved++
			buckets[key] = bucket
			if event.Record.Type == schema.EventImported {
				imported[key] = true
			}
		}
	}
	for key, bucket := range buckets {
		bucket.Coverage = coverageFor(key, rawFloor, enabledAt)
		if imported[key] && bucket.Coverage == schema.CoverageComplete {
			bucket.Coverage = schema.CoverageReconstructed
		}
		buckets[key] = bucket
	}
	return buckets
}

// coverageFor decides how much of a bucket's range was actually observed.
// A bucket that starts before collection was enabled describes inferred rather
// than observed activity; one that starts before the retained raw floor lost
// part of its input to truncation.
func coverageFor(key bucketKeyOf, rawFloor, enabledAt uint64) schema.HistoryCoverage {
	if enabledAt != 0 && key.start < enabledAt {
		return schema.CoverageReconstructed
	}
	if rawFloor != 0 && key.start < rawFloor {
		return schema.CoveragePartial
	}
	return schema.CoverageComplete
}

func applyEventToBucket(bucket *schema.PackHistoryBucket, event schema.PackHistoryEvent) {
	switch event.Type {
	case schema.EventCreated, schema.EventImported:
		bucket.PacksCreated++
		bucket.BytesAdded = addSaturating(bucket.BytesAdded, event.PhysicalSize)
	case schema.EventRepackedInto:
		// A repack destination is a rewrite, not new data, so it is counted
		// separately from growth.
		bucket.PacksRepacked++
		bucket.BytesRepacked = addSaturating(bucket.BytesRepacked, event.PhysicalSize)
	case schema.EventPromoted:
		bucket.PacksPromoted++
		bucket.BytesPromoted = addSaturating(bucket.BytesPromoted, event.PhysicalSize)
	case schema.EventDeleted:
		bucket.PacksDeleted++
		bucket.BytesDeleted = addSaturating(bucket.BytesDeleted, event.PhysicalSize)
	default:
		// Other events do not change rollup counters.
	}
}

func addSaturating(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

// RollupHistory recomputes buckets from the retained raw events and writes the
// ones that changed. Recomputation over the same raw range is idempotent, so a
// repeated run writes nothing.
func RollupHistory(ctx context.Context, store Store, dryRun bool) (RollupResult, error) {
	var result RollupResult
	scanned, err := ScanHistory(ctx, store, 0, 0)
	if err != nil {
		return result, err
	}
	result.EventsScanned = uint64(len(scanned.Events))
	result.MalformedEvents = scanned.Malformed

	rawFloor, err := readHistoryMarker(ctx, store, schema.HistoryRawFloorKey())
	if err != nil {
		return result, err
	}
	enabledAt, err := readHistoryMarker(ctx, store, schema.HistoryEnabledAtKey())
	if err != nil {
		return result, err
	}

	buckets := AccumulateBuckets(scanned.Events, schema.HistoryGranularities(), rawFloor, enabledAt)
	puts := make([]daemon.Mutation, 0, len(buckets))
	for key, bucket := range buckets {
		switch bucket.Coverage {
		case schema.CoveragePartial:
			result.Partial++
		case schema.CoverageReconstructed:
			result.Reconstructed++
		default:
			// Complete coverage needs no separate counter.
		}
		storageKey := schema.PackHistoryBucketKey(key.granularity, key.start, key.backend, key.packType)
		if storageKey == nil {
			continue
		}
		current, found, getErr := store.Get(ctx, storageKey)
		if getErr != nil {
			return result, getErr
		}
		if found {
			existing, decodeErr := schema.UnmarshalPackHistoryBucket(current)
			if decodeErr == nil && existing == bucket {
				result.BucketsUnchanged++
				continue
			}
			if decodeErr != nil && !errors.Is(decodeErr, schema.ErrMalformed) {
				return result, decodeErr
			}
		}
		encoded, encodeErr := bucket.MarshalBinary()
		if encodeErr != nil {
			return result, encodeErr
		}
		puts = append(puts, daemon.Mutation{Key: storageKey, Value: encoded})
		result.BucketsWritten++
	}
	if len(puts) != 0 && !dryRun {
		if err := store.WriteMutableBatch(ctx, puts, nil, true); err != nil {
			return result, fmt.Errorf("write history buckets: %w", err)
		}
	}
	if dryRun {
		result.BucketsWritten = uint64(len(puts))
	}
	return result, nil
}

func readHistoryMarker(ctx context.Context, store Store, key []byte) (uint64, error) {
	value, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return 0, err
	}
	marker, decodeErr := schema.UnmarshalHistoryMarker(value)
	if decodeErr != nil {
		// A corrupt marker is treated as absent: it degrades coverage
		// reporting rather than failing the pass.
		return 0, nil
	}
	return marker.UnixSeconds, nil
}

// HistoryRetentionOptions bounds how long each tier of history is kept. A zero
// duration keeps that tier indefinitely.
type HistoryRetentionOptions struct {
	KeepRaw     time.Duration
	KeepHourly  time.Duration
	KeepDaily   time.Duration
	KeepMonthly time.Duration
	DryRun      bool
	// Now is overridable so retention boundaries are testable.
	Now time.Time
}

// HistoryRetentionResult summarises one retention pass.
type HistoryRetentionResult struct {
	SchemaVersion    int          `json:"schema_version"`
	RawEventsRemoved uint64       `json:"raw_events_removed"`
	BucketsRemoved   uint64       `json:"buckets_removed"`
	NewRawFloor      uint64       `json:"new_raw_floor"`
	Rollup           RollupResult `json:"rollup"`
}

// PruneHistory rolls up first, then truncates. Rolling up before truncating is
// what allows raw events to be discarded without losing the periods they
// describe, and the raw floor it records is what later marks the affected
// buckets partial rather than silently complete.
//
//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func PruneHistory(ctx context.Context, store Store, options HistoryRetentionOptions) (HistoryRetentionResult, error) {
	result := HistoryRetentionResult{SchemaVersion: IntrospectSchemaVersion}
	now := options.Now
	if now.IsZero() {
		now = time.Now()
	}
	rollup, err := RollupHistory(ctx, store, options.DryRun)
	if err != nil {
		return result, err
	}
	result.Rollup = rollup

	deletes := make([][]byte, 0)
	if options.KeepRaw > 0 {
		floor := uint64(now.Add(-options.KeepRaw).UTC().Unix())
		scanned, scanErr := ScanHistory(ctx, store, 0, floor)
		if scanErr != nil {
			return result, scanErr
		}
		for _, event := range scanned.Events {
			deletes = append(deletes, schema.PackHistoryKey(event.UnixSeconds, event.Sequence, schema.ID(event.PackID)))
		}
		result.RawEventsRemoved = uint64(len(deletes))
		result.NewRawFloor = floor
	}

	bucketRetention := map[schema.HistoryGranularity]time.Duration{
		schema.GranularityHourly:  options.KeepHourly,
		schema.GranularityDaily:   options.KeepDaily,
		schema.GranularityMonthly: options.KeepMonthly,
	}
	for _, granularity := range schema.HistoryGranularities() {
		keep := bucketRetention[granularity]
		if keep <= 0 {
			continue
		}
		cutoff := uint64(now.Add(-keep).UTC().Unix())
		err := scan(ctx, store, schema.PackHistoryBucketPrefix(granularity), func(entry daemon.KeyValue) error {
			parsed, parseErr := schema.ParseKey(entry.Key)
			if parseErr != nil || parsed.Kind != schema.KeyPackHistoryBucket {
				return nil
			}
			if parsed.EventTime < cutoff {
				deletes = append(deletes, append([]byte(nil), entry.Key...))
				result.BucketsRemoved++
			}
			return nil
		})
		if err != nil {
			return result, err
		}
	}

	if options.DryRun {
		return result, nil
	}
	puts := make([]daemon.Mutation, 0, 1)
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if result.NewRawFloor != 0 {
		// The floor is only advanced, never lowered, so a later pass cannot
		// claim coverage it does not have.
		existing, markerErr := readHistoryMarker(ctx, store, schema.HistoryRawFloorKey())
		if markerErr != nil {
			return result, markerErr
		}
		if result.NewRawFloor > existing {
			value, encodeErr := (schema.HistoryMarker{UnixSeconds: result.NewRawFloor}).MarshalBinary()
			if encodeErr != nil {
				return result, encodeErr
			}
			puts = append(puts, daemon.Mutation{Key: schema.HistoryRawFloorKey(), Value: value})
		} else {
			result.NewRawFloor = existing
		}
	}
	if len(deletes) == 0 && len(puts) == 0 {
		return result, nil
	}
	if err := store.WriteMutableBatch(ctx, puts, deletes, true); err != nil {
		return result, fmt.Errorf("truncate pack history: %w", err)
	}
	return result, nil
}
