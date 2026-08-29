package schema

import "fmt"

// PackEventType names a pack lifecycle transition. Values are persisted, so
// they must stay stable. Placement-related types are defined here even though
// the placement model arrives in a later phase: the log is append-only and
// immutable, so reserving them now avoids ever having to rewrite history.
type PackEventType byte

const (
	EventCreated PackEventType = iota + 1
	EventImported
	EventPublished
	EventPlaced
	EventPlacementFailed
	EventPromoted
	EventEvicted
	EventUsageChanged
	EventRepackedFrom
	EventRepackedInto
	EventDeletePending
	EventDeleted
	EventDeleteFailed
	EventOrphanDetected
	// EventTierChanged records a pack whose recorded tier became known or
	// moved. Phase 12 supersedes it with the placement events above, but
	// historical entries must stay decodable, so the value is never reused.
	EventTierChanged
)

var packEventNames = map[PackEventType]string{
	EventCreated: "created", EventImported: "imported", EventPublished: "published",
	EventPlaced: "placed", EventPlacementFailed: "placement_failed",
	EventPromoted: "promoted", EventEvicted: "evicted",
	EventUsageChanged: "usage_changed", EventRepackedFrom: "repacked_from",
	EventRepackedInto: "repacked_into", EventDeletePending: "delete_pending",
	EventDeleted: "deleted", EventDeleteFailed: "delete_failed",
	EventOrphanDetected: "orphan_detected", EventTierChanged: "tier_changed",
}

func (event PackEventType) String() string {
	if name, ok := packEventNames[event]; ok {
		return name
	}
	return "unknown"
}

func validPackEventType(value PackEventType) bool {
	return value >= EventCreated && value <= EventTierChanged
}

// MaxHistoryPredecessors bounds repack lineage so one event cannot grow without
// limit when a repack consolidates a very large number of source packs.
const MaxHistoryPredecessors = 1024

// PackHistoryEvent is one immutable entry in the append-only pack history log.
//
// The record is self-contained on purpose: it must remain meaningful after the
// pack it describes has been deleted and its catalog record removed, so it
// carries the sizes and classification rather than referring back to `p:`.
type PackHistoryEvent struct {
	Type     PackEventType
	PackType PackType
	// Backend is zero until the placement model supplies one.
	Backend                   uint64
	PhysicalSize, PayloadSize uint64
	// UsedDelta and UnusedDelta are signed because reachability moves in both
	// directions as snapshots are added and forgotten.
	UsedDelta, UnusedDelta int64
	// PredecessorPackIDs carries repack and promotion lineage, which is what
	// lets a reader distinguish churn from genuine growth.
	PredecessorPackIDs []ID
	RunID              ID
	ReasonCode         string
}

func (record PackHistoryEvent) MarshalBinary() ([]byte, error) {
	if !validPackEventType(record.Type) {
		return nil, fmt.Errorf("%w: invalid pack event type", ErrMalformed)
	}
	packType := record.PackType
	if packType == 0 {
		packType = PackUnknown
	}
	if !validPackType(packType) {
		return nil, fmt.Errorf("%w: invalid pack event classification", ErrMalformed)
	}
	if len(record.PredecessorPackIDs) > MaxHistoryPredecessors {
		return nil, fmt.Errorf("%w: too many repack predecessors", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.Type))
	e.u8(byte(packType))
	e.u64(record.Backend)
	e.u64(record.PhysicalSize)
	e.u64(record.PayloadSize)
	e.i64(record.UsedDelta)
	e.i64(record.UnusedDelta)
	e.u32(uint32(len(record.PredecessorPackIDs)))
	for _, id := range record.PredecessorPackIDs {
		e.id(id)
	}
	e.id(record.RunID)
	if err := e.string(record.ReasonCode); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalPackHistoryEvent(data []byte) (PackHistoryEvent, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PackHistoryEvent{}, err
	}
	var record PackHistoryEvent
	value, err := d.u8()
	record.Type = PackEventType(value)
	if err != nil {
		return PackHistoryEvent{}, err
	}
	value, err = d.u8()
	record.PackType = PackType(value)
	if err != nil {
		return PackHistoryEvent{}, err
	}
	for _, field := range []*uint64{&record.Backend, &record.PhysicalSize, &record.PayloadSize} {
		if *field, err = d.u64(); err != nil {
			return PackHistoryEvent{}, err
		}
	}
	for _, field := range []*int64{&record.UsedDelta, &record.UnusedDelta} {
		if *field, err = d.i64(); err != nil {
			return PackHistoryEvent{}, err
		}
	}
	count, err := d.u32()
	if err != nil || uint64(count)*32 > uint64(len(data)) || count > MaxHistoryPredecessors {
		return PackHistoryEvent{}, fmt.Errorf("%w: invalid repack lineage count", ErrMalformed)
	}
	record.PredecessorPackIDs = make([]ID, count)
	for index := range record.PredecessorPackIDs {
		if record.PredecessorPackIDs[index], err = d.id(); err != nil {
			return PackHistoryEvent{}, err
		}
	}
	if record.RunID, err = d.id(); err != nil {
		return PackHistoryEvent{}, err
	}
	if record.ReasonCode, err = d.string(); err != nil {
		return PackHistoryEvent{}, err
	}
	if !validPackEventType(record.Type) || !validPackType(record.PackType) {
		return PackHistoryEvent{}, fmt.Errorf("%w: invalid pack event classification", ErrMalformed)
	}
	return record, d.done()
}

// HistoryCoverage states how much of a bucket's raw range was actually
// observed. It exists so an incomplete series is never presented as
// authoritative.
type HistoryCoverage byte

const (
	// CoverageComplete means the bucket was rolled up over a fully retained
	// raw range.
	CoverageComplete HistoryCoverage = iota + 1
	// CoveragePartial means part of the bucket's raw range had already been
	// truncated when it was rolled up.
	CoveragePartial
	// CoverageReconstructed means the bucket covers a period before history
	// collection was enabled, or was derived from a legacy import, so it
	// describes inferred rather than observed events.
	CoverageReconstructed
)

var historyCoverageNames = map[HistoryCoverage]string{
	CoverageComplete: "complete", CoveragePartial: "partial", CoverageReconstructed: "reconstructed",
}

func (coverage HistoryCoverage) String() string {
	if name, ok := historyCoverageNames[coverage]; ok {
		return name
	}
	return "unknown"
}

func validHistoryCoverage(value HistoryCoverage) bool {
	return value >= CoverageComplete && value <= CoverageReconstructed
}

// PackHistoryBucket is a rolled-up window of the event log. It is a pure
// function of the raw events in its range, so recomputing it is idempotent.
type PackHistoryBucket struct {
	PacksCreated, PacksDeleted, PacksRepacked, PacksPromoted uint64
	BytesAdded, BytesDeleted, BytesRepacked, BytesPromoted   uint64
	// EndPackCount and the end-of-bucket totals describe the state at the end
	// of the window rather than the activity within it.
	EndPackCount, EndPhysicalSize, EndPayloadSize uint64
	Coverage                                      HistoryCoverage
	EventsObserved                                uint64
}

func (record PackHistoryBucket) MarshalBinary() ([]byte, error) {
	coverage := record.Coverage
	if coverage == 0 {
		coverage = CoverageComplete
	}
	if !validHistoryCoverage(coverage) {
		return nil, fmt.Errorf("%w: invalid history coverage", ErrMalformed)
	}
	e := newEncoder()
	for _, value := range []uint64{
		record.PacksCreated, record.PacksDeleted, record.PacksRepacked, record.PacksPromoted,
		record.BytesAdded, record.BytesDeleted, record.BytesRepacked, record.BytesPromoted,
		record.EndPackCount, record.EndPhysicalSize, record.EndPayloadSize, record.EventsObserved,
	} {
		e.u64(value)
	}
	e.u8(byte(coverage))
	return e.finish()
}

func UnmarshalPackHistoryBucket(data []byte) (PackHistoryBucket, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PackHistoryBucket{}, err
	}
	var record PackHistoryBucket
	for _, field := range []*uint64{
		&record.PacksCreated, &record.PacksDeleted, &record.PacksRepacked, &record.PacksPromoted,
		&record.BytesAdded, &record.BytesDeleted, &record.BytesRepacked, &record.BytesPromoted,
		&record.EndPackCount, &record.EndPhysicalSize, &record.EndPayloadSize, &record.EventsObserved,
	} {
		if *field, err = d.u64(); err != nil {
			return PackHistoryBucket{}, err
		}
	}
	value, err := d.u8()
	record.Coverage = HistoryCoverage(value)
	if err != nil || !validHistoryCoverage(record.Coverage) {
		return PackHistoryBucket{}, fmt.Errorf("%w: invalid history coverage", ErrMalformed)
	}
	return record, d.done()
}

// HistoryMarker records a single unix-second boundary, used for the retained
// raw floor and for when collection was enabled.
type HistoryMarker struct{ UnixSeconds uint64 }

func (record HistoryMarker) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	e.u64(record.UnixSeconds)
	return e.finish()
}

func UnmarshalHistoryMarker(data []byte) (HistoryMarker, error) {
	d, err := newDecoder(data)
	if err != nil {
		return HistoryMarker{}, err
	}
	var record HistoryMarker
	if record.UnixSeconds, err = d.u64(); err != nil {
		return HistoryMarker{}, err
	}
	return record, d.done()
}
