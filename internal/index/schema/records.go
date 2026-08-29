package schema

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
)

type BlobType byte

const (
	BlobData BlobType = iota + 1
	BlobTree
)

type BlobLocation struct {
	PackID           ID
	Offset           uint64
	Length           uint32
	UncompressedSize uint32
	Type             BlobType
}

type BlobRecord struct{ Locations []BlobLocation }

func (record BlobRecord) MarshalBinary() ([]byte, error) {
	if len(record.Locations) == 0 {
		return nil, fmt.Errorf("%w: blob has no locations", ErrMalformed)
	}
	e := newEncoder()
	e.u32(uint32(len(record.Locations)))
	for _, location := range record.Locations {
		if location.Type != BlobData && location.Type != BlobTree {
			return nil, fmt.Errorf("%w: invalid blob type", ErrMalformed)
		}
		e.id(location.PackID)
		e.u64(location.Offset)
		e.u32(location.Length)
		e.u32(location.UncompressedSize)
		e.u8(byte(location.Type))
	}
	return e.finish()
}
func UnmarshalBlobRecord(data []byte) (BlobRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return BlobRecord{}, err
	}
	count, err := d.u32()
	if err != nil || count == 0 || uint64(count)*49 > uint64(len(data)) {
		return BlobRecord{}, fmt.Errorf("%w: invalid blob location count", ErrMalformed)
	}
	record := BlobRecord{Locations: make([]BlobLocation, count)}
	for index := range record.Locations {
		location := &record.Locations[index]
		if location.PackID, err = d.id(); err != nil {
			return BlobRecord{}, err
		}
		if location.Offset, err = d.u64(); err != nil {
			return BlobRecord{}, err
		}
		if location.Length, err = d.u32(); err != nil {
			return BlobRecord{}, err
		}
		if location.UncompressedSize, err = d.u32(); err != nil {
			return BlobRecord{}, err
		}
		value, readErr := d.u8()
		if readErr != nil {
			return BlobRecord{}, readErr
		}
		location.Type = BlobType(value)
		if location.Type != BlobData && location.Type != BlobTree {
			return BlobRecord{}, fmt.Errorf("%w: invalid blob type", ErrMalformed)
		}
	}
	return record, d.done()
}

type PackType byte

const (
	PackData PackType = iota + 1
	PackTree
	PackMixed
	PackUnknown
)

type PackLifecycle byte

const (
	PackImported PackLifecycle = iota + 1
	PackPublished
	PackExportPending
	PackDeletePending
	PackDeleted
	PackOrphaned
	PackStateUnknown
)

// PackTier records where a pack was routed at publish time. It is recorded
// rather than recomputed so a repository that later stops using --repo-hot can
// still explain where a pack came from.
//
// TierMirrored, not TierHot, is the correct tier for a tree pack in a hot/cold
// repository: hotcold.Save writes every hot file to the hot backend and then
// mirrors it to the cold backend, so a hot-only pack never exists. TierHot is
// retained for repositories that route without mirroring.
type PackTier byte

const (
	// TierUnknown covers imported packs and any pack whose routing could not
	// be established. It is never synthesized into a concrete tier.
	TierUnknown PackTier = iota + 1
	TierHot
	TierCold
	TierMirrored
	// TierSingle is a repository without a hot/cold split, where the sole
	// backend holds every pack.
	TierSingle
)

// RetentionSource records where min-retention came from. A deadline is only
// trustworthy when it was derived from a known creation time.
type RetentionSource byte

const (
	RetentionUnknown RetentionSource = iota + 1
	RetentionConfig
	RetentionBackend
)

type PlacementState byte

const (
	PlacementPending PlacementState = iota + 1
	PlacementLive
	PlacementEvicting
	PlacementEvicted
	PlacementFailed
)

func validPlacementState(value PlacementState) bool {
	return value >= PlacementPending && value <= PlacementFailed
}

type PlacementRecord struct {
	State              PlacementState
	StorageClass       string
	PlacedAt           int64
	PlacementTimeKnown bool
	Bytes              uint64
	MinRetentionUntil  int64
	RetentionSource    RetentionSource
	DeleteAfter        int64
	LastVerifiedAt     int64
}

func (record PlacementRecord) normalized() PlacementRecord {
	if record.RetentionSource == 0 {
		record.RetentionSource = RetentionUnknown
	}
	return record
}

func (record PlacementRecord) validate() error {
	record = record.normalized()
	if !validPlacementState(record.State) || !validRetentionSource(record.RetentionSource) {
		return fmt.Errorf("%w: invalid placement state", ErrMalformed)
	}
	if !record.PlacementTimeKnown && record.PlacedAt != 0 {
		return fmt.Errorf("%w: placement time without a known flag", ErrMalformed)
	}
	if record.RetentionSource == RetentionUnknown && record.MinRetentionUntil != 0 {
		return fmt.Errorf("%w: placement retention deadline without a source", ErrMalformed)
	}
	if record.DeleteAfter != 0 && record.State != PlacementEvicting && record.State != PlacementEvicted {
		return fmt.Errorf("%w: placement delete deadline without evicting state", ErrMalformed)
	}
	return nil
}

func (record PlacementRecord) MarshalBinary() ([]byte, error) {
	record = record.normalized()
	if err := record.validate(); err != nil {
		return nil, err
	}
	e := newEncoder()
	e.u8(byte(record.State))
	if err := e.string(record.StorageClass); err != nil {
		return nil, err
	}
	e.i64(record.PlacedAt)
	e.bool(record.PlacementTimeKnown)
	e.u64(record.Bytes)
	e.i64(record.MinRetentionUntil)
	e.u8(byte(record.RetentionSource))
	e.i64(record.DeleteAfter)
	e.i64(record.LastVerifiedAt)
	return e.finish()
}

func UnmarshalPlacementRecord(data []byte) (PlacementRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PlacementRecord{}, err
	}
	var record PlacementRecord
	value, err := d.u8()
	record.State = PlacementState(value)
	if err != nil {
		return record, err
	}
	if record.StorageClass, err = d.string(); err != nil {
		return record, err
	}
	if record.PlacedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.PlacementTimeKnown, err = d.bool(); err != nil {
		return record, err
	}
	if record.Bytes, err = d.u64(); err != nil {
		return record, err
	}
	if record.MinRetentionUntil, err = d.i64(); err != nil {
		return record, err
	}
	value, err = d.u8()
	record.RetentionSource = RetentionSource(value)
	if err != nil {
		return record, err
	}
	if record.DeleteAfter, err = d.i64(); err != nil {
		return record, err
	}
	if record.LastVerifiedAt, err = d.i64(); err != nil {
		return record, err
	}
	if err := record.validate(); err != nil {
		return PlacementRecord{}, err
	}
	return record, d.done()
}

type BackendPackRecord struct {
	State    PlacementState
	Bytes    uint64
	PlacedAt int64
}

func (record BackendPackRecord) MarshalBinary() ([]byte, error) {
	if !validPlacementState(record.State) {
		return nil, fmt.Errorf("%w: invalid backend-pack state", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.State))
	e.u64(record.Bytes)
	e.i64(record.PlacedAt)
	return e.finish()
}

func UnmarshalBackendPackRecord(data []byte) (BackendPackRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return BackendPackRecord{}, err
	}
	var record BackendPackRecord
	value, err := d.u8()
	record.State = PlacementState(value)
	if err != nil {
		return record, err
	}
	if record.Bytes, err = d.u64(); err != nil {
		return record, err
	}
	if record.PlacedAt, err = d.i64(); err != nil {
		return record, err
	}
	if !validPlacementState(record.State) {
		return BackendPackRecord{}, fmt.Errorf("%w: invalid backend-pack state", ErrMalformed)
	}
	return record, d.done()
}

type PlacementDeleteRecord struct {
	Backend      uint64
	PhysicalSize uint64
	Reason       string
	RunID        ID
}

func (record PlacementDeleteRecord) MarshalBinary() ([]byte, error) {
	if record.Backend == 0 {
		return nil, fmt.Errorf("%w: placement delete queue without backend", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.Backend)
	e.u64(record.PhysicalSize)
	if err := e.string(record.Reason); err != nil {
		return nil, err
	}
	e.id(record.RunID)
	return e.finish()
}

func UnmarshalPlacementDeleteRecord(data []byte) (PlacementDeleteRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PlacementDeleteRecord{}, err
	}
	var record PlacementDeleteRecord
	if record.Backend, err = d.u64(); err != nil {
		return record, err
	}
	if record.PhysicalSize, err = d.u64(); err != nil {
		return record, err
	}
	if record.Reason, err = d.string(); err != nil {
		return record, err
	}
	if record.RunID, err = d.id(); err != nil {
		return record, err
	}
	if record.Backend == 0 {
		return PlacementDeleteRecord{}, fmt.Errorf("%w: placement delete queue without backend", ErrMalformed)
	}
	return record, d.done()
}

type PlacementRequestRecord struct {
	Classes       []string
	Operation     PlacementRequestOperation
	TargetBackend uint64
	Attempts      uint32
	LastAttempt   int64
	NotBefore     int64
	LastError     string
}

type RepackLineageRecord struct {
	RunID ID
	Kind  RepackLineageKind
}

type PromotionEligibilityRecord struct {
	SurvivalUntil int64
	EvaluatedAt   int64
	Indefinite    bool
}

func (record PromotionEligibilityRecord) MarshalBinary() ([]byte, error) {
	if record.EvaluatedAt <= 0 || (!record.Indefinite && record.SurvivalUntil <= 0) {
		return nil, fmt.Errorf("%w: invalid promotion eligibility", ErrMalformed)
	}
	e := newEncoder()
	e.i64(record.SurvivalUntil)
	e.i64(record.EvaluatedAt)
	e.bool(record.Indefinite)
	return e.finish()
}

func UnmarshalPromotionEligibilityRecord(data []byte) (PromotionEligibilityRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PromotionEligibilityRecord{}, err
	}
	record := PromotionEligibilityRecord{}
	if record.SurvivalUntil, err = d.i64(); err != nil {
		return record, err
	}
	if record.EvaluatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.Indefinite, err = d.bool(); err != nil {
		return record, err
	}
	if record.EvaluatedAt <= 0 || (!record.Indefinite && record.SurvivalUntil <= 0) {
		return PromotionEligibilityRecord{}, fmt.Errorf("%w: invalid promotion eligibility", ErrMalformed)
	}
	return record, d.done()
}

type RepackLineageKind byte

const (
	LineageRepack RepackLineageKind = iota + 1
	LineagePromotion
)

func (record RepackLineageRecord) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	e.id(record.RunID)
	if record.Kind != LineageRepack && record.Kind != LineagePromotion {
		return nil, fmt.Errorf("%w: invalid repack lineage kind", ErrMalformed)
	}
	e.u8(byte(record.Kind))
	return e.finish()
}

func UnmarshalRepackLineageRecord(data []byte) (RepackLineageRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return RepackLineageRecord{}, err
	}
	record := RepackLineageRecord{}
	if record.RunID, err = d.id(); err != nil {
		return record, err
	}
	value, err := d.u8()
	record.Kind = RepackLineageKind(value)
	if err != nil || (record.Kind != LineageRepack && record.Kind != LineagePromotion) {
		return RepackLineageRecord{}, fmt.Errorf("%w: invalid repack lineage kind", ErrMalformed)
	}
	return record, d.done()
}

type PlacementRequestOperation byte

const (
	PlacementRequestPlace PlacementRequestOperation = iota + 1
	PlacementRequestPromote
	PlacementRequestEvict
)

func validPlacementRequestOperation(operation PlacementRequestOperation) bool {
	return operation >= PlacementRequestPlace && operation <= PlacementRequestEvict
}

func (record PlacementRequestRecord) MarshalBinary() ([]byte, error) {
	if len(record.Classes) == 0 {
		return nil, fmt.Errorf("%w: placement request requires at least one class", ErrMalformed)
	}
	if !validPlacementRequestOperation(record.Operation) || record.TargetBackend == 0 {
		return nil, fmt.Errorf("%w: placement request requires an operation and target backend", ErrMalformed)
	}
	e := newEncoder()
	e.u32(uint32(len(record.Classes)))
	for _, class := range record.Classes {
		if class == "" {
			return nil, fmt.Errorf("%w: empty placement request class", ErrMalformed)
		}
		if err := e.string(class); err != nil {
			return nil, err
		}
	}
	e.u8(byte(record.Operation))
	e.u64(record.TargetBackend)
	e.u32(record.Attempts)
	e.i64(record.LastAttempt)
	e.i64(record.NotBefore)
	if err := e.string(record.LastError); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalPlacementRequestRecord(data []byte) (PlacementRequestRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PlacementRequestRecord{}, err
	}
	count, err := d.u32()
	if err != nil || count == 0 || count > 1024 {
		return PlacementRequestRecord{}, fmt.Errorf("%w: invalid placement request class count", ErrMalformed)
	}
	record := PlacementRequestRecord{Classes: make([]string, count)}
	for index := range record.Classes {
		if record.Classes[index], err = d.string(); err != nil {
			return record, err
		}
		if record.Classes[index] == "" {
			return PlacementRequestRecord{}, fmt.Errorf("%w: empty placement request class", ErrMalformed)
		}
	}
	value, err := d.u8()
	record.Operation = PlacementRequestOperation(value)
	if err != nil {
		return record, err
	}
	if record.TargetBackend, err = d.u64(); err != nil {
		return record, err
	}
	if record.Attempts, err = d.u32(); err != nil {
		return record, err
	}
	if record.LastAttempt, err = d.i64(); err != nil {
		return record, err
	}
	if record.NotBefore, err = d.i64(); err != nil {
		return record, err
	}
	if record.LastError, err = d.string(); err != nil {
		return record, err
	}
	if !validPlacementRequestOperation(record.Operation) || record.TargetBackend == 0 {
		return PlacementRequestRecord{}, fmt.Errorf("%w: invalid placement request operation or backend", ErrMalformed)
	}
	return record, d.done()
}

type PackRecord struct {
	Type                                             PackType
	PhysicalSize, PayloadSize, HeaderSize, BlobCount uint64
	PhysicalSizeKnown                                bool
	CreationTime                                     int64
	CreationTimeKnown                                bool
	Lifecycle                                        PackLifecycle
	SourceIndexIDs                                   []ID
	Tier                                             PackTier
	StorageClass                                     string
	MinRetentionUntil                                int64
	RetentionSource                                  RetentionSource
	UsedPayloadBytes, UnusedPayloadBytes             uint64
	// UsageKnown distinguishes "reachability has never been computed" from
	// "every byte is reachable". Without it an unaccounted pack is
	// indistinguishable from a wholly unused one.
	UsageKnown  bool
	DeleteAfter int64
}

// normalized maps unset tier and retention enums to their explicit unknown
// values. A caller that never considered tier must record "unknown", never a
// synthesized tier.
func (record PackRecord) normalized() PackRecord {
	if record.Tier == 0 {
		record.Tier = TierUnknown
	}
	if record.RetentionSource == 0 {
		record.RetentionSource = RetentionUnknown
	}
	return record
}

// validatePackLifetime enforces the invariants that keep unknown facts from
// being presented as measured ones.
func (record PackRecord) validatePackLifetime() error {
	if !validPackTier(record.Tier) || !validRetentionSource(record.RetentionSource) {
		return fmt.Errorf("%w: invalid pack tier state", ErrMalformed)
	}
	if record.RetentionSource == RetentionUnknown && record.MinRetentionUntil != 0 {
		return fmt.Errorf("%w: retention deadline without a source", ErrMalformed)
	}
	if record.RetentionSource != RetentionUnknown && !record.CreationTimeKnown {
		return fmt.Errorf("%w: retention deadline without a known creation time", ErrMalformed)
	}
	if !record.CreationTimeKnown && record.CreationTime != 0 {
		return fmt.Errorf("%w: creation time without a known flag", ErrMalformed)
	}
	if !record.UsageKnown && (record.UsedPayloadBytes != 0 || record.UnusedPayloadBytes != 0) {
		return fmt.Errorf("%w: usage accounting without a known flag", ErrMalformed)
	}
	if record.UsageKnown {
		total, ok := add(record.UsedPayloadBytes, record.UnusedPayloadBytes)
		if !ok || total != record.PayloadSize {
			return fmt.Errorf("%w: usage accounting does not sum to the payload size", ErrMalformed)
		}
	}
	return nil
}

func (record PackRecord) MarshalBinary() ([]byte, error) {
	record = record.normalized()
	if !validPackType(record.Type) || !validPackLifecycle(record.Lifecycle) {
		return nil, fmt.Errorf("%w: invalid pack classification", ErrMalformed)
	}
	if (!record.PhysicalSizeKnown && (record.PhysicalSize != 0 || record.HeaderSize != 0)) ||
		(record.PhysicalSizeKnown && record.PhysicalSize >= record.PayloadSize && record.HeaderSize != record.PhysicalSize-record.PayloadSize) ||
		(record.PhysicalSizeKnown && record.PhysicalSize < record.PayloadSize && record.HeaderSize != 0) {
		return nil, fmt.Errorf("%w: invalid pack size state", ErrMalformed)
	}
	if err := record.validatePackLifetime(); err != nil {
		return nil, err
	}
	e := newEncoder()
	e.u8(byte(record.Type))
	e.u64(record.PhysicalSize)
	e.u64(record.PayloadSize)
	e.u64(record.HeaderSize)
	e.u64(record.BlobCount)
	e.i64(record.CreationTime)
	e.bool(record.CreationTimeKnown)
	e.u8(byte(record.Lifecycle))
	e.u32(uint32(len(record.SourceIndexIDs)))
	for _, id := range record.SourceIndexIDs {
		e.id(id)
	}
	e.bool(record.PhysicalSizeKnown)
	e.u8(byte(record.Tier))
	if err := e.string(record.StorageClass); err != nil {
		return nil, err
	}
	e.i64(record.MinRetentionUntil)
	e.u8(byte(record.RetentionSource))
	e.bool(record.UsageKnown)
	e.u64(record.UsedPayloadBytes)
	e.u64(record.UnusedPayloadBytes)
	e.i64(record.DeleteAfter)
	return e.finish()
}
func UnmarshalPackRecord(data []byte) (PackRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PackRecord{}, err
	}
	var record PackRecord
	value, err := d.u8()
	record.Type = PackType(value)
	if err != nil {
		return record, err
	}
	if record.PhysicalSize, err = d.u64(); err != nil {
		return record, err
	}
	if record.PayloadSize, err = d.u64(); err != nil {
		return record, err
	}
	if record.HeaderSize, err = d.u64(); err != nil {
		return record, err
	}
	if record.BlobCount, err = d.u64(); err != nil {
		return record, err
	}
	if record.CreationTime, err = d.i64(); err != nil {
		return record, err
	}
	if record.CreationTimeKnown, err = d.bool(); err != nil {
		return record, err
	}
	value, err = d.u8()
	record.Lifecycle = PackLifecycle(value)
	if err != nil {
		return record, err
	}
	count, err := d.u32()
	if err != nil || uint64(count)*32 > uint64(len(data)) {
		return record, fmt.Errorf("%w: invalid provenance count", ErrMalformed)
	}
	record.SourceIndexIDs = make([]ID, count)
	for index := range record.SourceIndexIDs {
		if record.SourceIndexIDs[index], err = d.id(); err != nil {
			return PackRecord{}, err
		}
	}
	if d.at < len(d.data) {
		if record.PhysicalSizeKnown, err = d.bool(); err != nil {
			return PackRecord{}, err
		}
	} else {
		record.PhysicalSizeKnown = record.PhysicalSize != 0
	}
	// Phase 3 records end here. They decode as tier-unknown and
	// retention-unknown rather than being assigned a synthesized tier.
	record.Tier, record.RetentionSource = TierUnknown, RetentionUnknown
	if d.at < len(d.data) {
		value, err = d.u8()
		record.Tier = PackTier(value)
		if err != nil {
			return PackRecord{}, err
		}
		if record.StorageClass, err = d.string(); err != nil {
			return PackRecord{}, err
		}
		if record.MinRetentionUntil, err = d.i64(); err != nil {
			return PackRecord{}, err
		}
		value, err = d.u8()
		record.RetentionSource = RetentionSource(value)
		if err != nil {
			return PackRecord{}, err
		}
		if record.UsageKnown, err = d.bool(); err != nil {
			return PackRecord{}, err
		}
		if record.UsedPayloadBytes, err = d.u64(); err != nil {
			return PackRecord{}, err
		}
		if record.UnusedPayloadBytes, err = d.u64(); err != nil {
			return PackRecord{}, err
		}
		if record.DeleteAfter, err = d.i64(); err != nil {
			return PackRecord{}, err
		}
	}
	if !validPackType(record.Type) || !validPackLifecycle(record.Lifecycle) {
		return PackRecord{}, fmt.Errorf("%w: invalid pack classification", ErrMalformed)
	}
	if (!record.PhysicalSizeKnown && (record.PhysicalSize != 0 || record.HeaderSize != 0)) ||
		(record.PhysicalSizeKnown && record.PhysicalSize >= record.PayloadSize && record.HeaderSize != record.PhysicalSize-record.PayloadSize) ||
		(record.PhysicalSizeKnown && record.PhysicalSize < record.PayloadSize && record.HeaderSize != 0) {
		return PackRecord{}, fmt.Errorf("%w: invalid pack size state", ErrMalformed)
	}
	if err := record.validatePackLifetime(); err != nil {
		return PackRecord{}, err
	}
	return record, d.done()
}
func validPackType(value PackType) bool { return value >= PackData && value <= PackUnknown }
func validPackLifecycle(value PackLifecycle) bool {
	return value >= PackImported && value <= PackStateUnknown
}
func validPackTier(value PackTier) bool { return value >= TierUnknown && value <= TierSingle }
func validRetentionSource(value RetentionSource) bool {
	return value >= RetentionUnknown && value <= RetentionBackend
}

type PackAggregate struct {
	PackCount, PhysicalSize, PayloadSize, HeaderSize, BlobCount, UpdateSequence uint64
	// UsedPayloadBytes and UnusedPayloadBytes only sum packs whose usage has
	// been computed. AccountedPackCount records how many that was, so a
	// consumer can tell partial accounting from a fully reachable repository.
	UsedPayloadBytes, UnusedPayloadBytes, AccountedPackCount uint64
}

func (record PackAggregate) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	e.u64(record.PackCount)
	e.u64(record.PhysicalSize)
	e.u64(record.PayloadSize)
	e.u64(record.HeaderSize)
	e.u64(record.BlobCount)
	e.u64(record.UpdateSequence)
	e.u64(record.UsedPayloadBytes)
	e.u64(record.UnusedPayloadBytes)
	e.u64(record.AccountedPackCount)
	return e.finish()
}
func UnmarshalPackAggregate(data []byte) (PackAggregate, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PackAggregate{}, err
	}
	values := []*uint64{}
	record := PackAggregate{}
	values = append(values, &record.PackCount, &record.PhysicalSize, &record.PayloadSize, &record.HeaderSize, &record.BlobCount, &record.UpdateSequence)
	for _, value := range values {
		if *value, err = d.u64(); err != nil {
			return PackAggregate{}, err
		}
	}
	// Phase 3 aggregates end here and carry no usage accounting.
	if d.at < len(d.data) {
		for _, value := range []*uint64{&record.UsedPayloadBytes, &record.UnusedPayloadBytes, &record.AccountedPackCount} {
			if *value, err = d.u64(); err != nil {
				return PackAggregate{}, err
			}
		}
	}
	return record, d.done()
}

type CurrentPointer struct {
	Revision  uint64
	RecordKey []byte
}

func (record CurrentPointer) MarshalBinary() ([]byte, error) {
	if record.Revision == 0 {
		return nil, fmt.Errorf("%w: zero revision", ErrMalformed)
	}
	parsed, err := ParseKey(record.RecordKey)
	if err != nil || (parsed.Kind != KeyInodeRevision && parsed.Kind != KeyDirectoryRevision) || parsed.Revision != record.Revision {
		return nil, fmt.Errorf("%w: current pointer target mismatch", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.Revision)
	if err := e.bytes(record.RecordKey); err != nil {
		return nil, err
	}
	return e.finish()
}
func UnmarshalCurrentPointer(data []byte) (CurrentPointer, error) {
	d, err := newDecoder(data)
	if err != nil {
		return CurrentPointer{}, err
	}
	revision, err := d.u64()
	if err != nil || revision == 0 {
		return CurrentPointer{}, fmt.Errorf("%w: invalid current revision", ErrMalformed)
	}
	key, err := d.bytes()
	if err != nil {
		return CurrentPointer{}, err
	}
	parsed, err := ParseKey(key)
	if err != nil || (parsed.Kind != KeyInodeRevision && parsed.Kind != KeyDirectoryRevision) || parsed.Revision != revision {
		return CurrentPointer{}, fmt.Errorf("%w: current pointer target mismatch", ErrMalformed)
	}
	return CurrentPointer{Revision: revision, RecordKey: key}, d.done()
}

type ContentMode byte

const (
	ContentNone ContentMode = iota
	ContentInline
	ContentManifestRef
)

const MaxInlineContentIDs = 128

type Freshness byte

const (
	FreshnessUnknown Freshness = iota
	FreshnessImported
	FreshnessVerified
)
const (
	KnownMTime uint16 = 1 << iota
	KnownCTime
	KnownSize
	KnownMode
	KnownUID
	KnownGID
	KnownParent
	KnownPath
)

const knownFieldMask = KnownMTime | KnownCTime | KnownSize | KnownMode | KnownUID | KnownGID | KnownParent | KnownPath

type InodeRevision struct {
	ParentInode        uint64
	HasMultipleParents bool
	MTime, CTime       int64
	Size               uint64
	Mode, UID, GID     uint32
	Known              uint16
	ContentMode        ContentMode
	ContentIDs         []ID
	ContentManifestID  ID
	ContentCount       uint32
	FileContentHash    ID
	HashKnown          bool
	SourcePath         string
	Freshness          Freshness
}

func (record InodeRevision) MarshalBinary() ([]byte, error) {
	if record.Freshness > FreshnessVerified || record.ContentMode > ContentManifestRef || record.Known & ^knownFieldMask != 0 {
		return nil, fmt.Errorf("%w: invalid inode state", ErrMalformed)
	}
	if !validContentState(record.ContentMode, record.ContentCount, record.ContentIDs, record.ContentManifestID) {
		return nil, fmt.Errorf("%w: invalid inode content state", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.ParentInode)
	e.bool(record.HasMultipleParents)
	e.i64(record.MTime)
	e.i64(record.CTime)
	e.u64(record.Size)
	e.u32(record.Mode)
	e.u32(record.UID)
	e.u32(record.GID)
	e.u32(uint32(record.Known))
	e.u8(byte(record.ContentMode))
	e.u32(record.ContentCount)
	if record.ContentMode == ContentInline {
		for _, id := range record.ContentIDs {
			e.id(id)
		}
	} else if record.ContentMode == ContentManifestRef {
		e.id(record.ContentManifestID)
	}
	e.id(record.FileContentHash)
	e.bool(record.HashKnown)
	if err := e.string(record.SourcePath); err != nil {
		return nil, err
	}
	e.u8(byte(record.Freshness))
	return e.finish()
}
func UnmarshalInodeRevision(data []byte) (InodeRevision, error) {
	d, err := newDecoder(data)
	if err != nil {
		return InodeRevision{}, err
	}
	var record InodeRevision
	if record.ParentInode, err = d.u64(); err != nil {
		return record, err
	}
	if record.HasMultipleParents, err = d.bool(); err != nil {
		return record, err
	}
	if record.MTime, err = d.i64(); err != nil {
		return record, err
	}
	if record.CTime, err = d.i64(); err != nil {
		return record, err
	}
	if record.Size, err = d.u64(); err != nil {
		return record, err
	}
	if record.Mode, err = d.u32(); err != nil {
		return record, err
	}
	if record.UID, err = d.u32(); err != nil {
		return record, err
	}
	if record.GID, err = d.u32(); err != nil {
		return record, err
	}
	known, err := d.u32()
	if err != nil || known & ^uint32(knownFieldMask) != 0 {
		return record, fmt.Errorf("%w: invalid known-field mask", ErrMalformed)
	}
	record.Known = uint16(known)
	mode, err := d.u8()
	record.ContentMode = ContentMode(mode)
	if err != nil {
		return record, err
	}
	if record.ContentCount, err = d.u32(); err != nil {
		return record, err
	}
	if record.ContentMode == ContentInline {
		if record.ContentCount == 0 || record.ContentCount > MaxInlineContentIDs || uint64(record.ContentCount)*32 > uint64(len(data)) {
			return record, fmt.Errorf("%w: invalid inline content count", ErrMalformed)
		}
		record.ContentIDs = make([]ID, record.ContentCount)
		for index := range record.ContentIDs {
			if record.ContentIDs[index], err = d.id(); err != nil {
				return InodeRevision{}, err
			}
		}
	} else if record.ContentMode == ContentManifestRef {
		if record.ContentManifestID, err = d.id(); err != nil {
			return record, err
		}
	} else if record.ContentMode != ContentNone {
		return record, fmt.Errorf("%w: invalid content mode", ErrMalformed)
	}
	if !validContentState(record.ContentMode, record.ContentCount, record.ContentIDs, record.ContentManifestID) {
		return record, fmt.Errorf("%w: invalid inode content state", ErrMalformed)
	}
	if record.FileContentHash, err = d.id(); err != nil {
		return record, err
	}
	if record.HashKnown, err = d.bool(); err != nil {
		return record, err
	}
	if record.SourcePath, err = d.string(); err != nil {
		return record, err
	}
	freshness, err := d.u8()
	record.Freshness = Freshness(freshness)
	if err != nil || record.Freshness > FreshnessVerified {
		return record, fmt.Errorf("%w: invalid freshness", ErrMalformed)
	}
	return record, d.done()
}

func validContentState(mode ContentMode, count uint32, ids []ID, manifestID ID) bool {
	switch mode {
	case ContentNone:
		return count == 0 && len(ids) == 0 && manifestID == (ID{})
	case ContentInline:
		return count > 0 && count <= MaxInlineContentIDs && int(count) == len(ids) && manifestID == (ID{})
	case ContentManifestRef:
		return count > MaxInlineContentIDs && count <= MaxContentIDs && len(ids) == 0 && manifestID != (ID{})
	default:
		return false
	}
}

type NodeType byte

const (
	NodeFile NodeType = iota + 1
	NodeDirectory
	NodeSymlink
	NodeOther
)

type DirectoryChild struct {
	Name        string
	Inode       uint64
	Type        NodeType
	MetadataKey []byte
}
type DirectoryRevision struct {
	ParentInode uint64
	Children    []DirectoryChild
	MTime       int64
	CTime       int64
	Size        uint64
	Mode        uint32
	UID         uint32
	GID         uint32
	Known       uint16
	SourcePath  string
	Freshness   Freshness
}

func (record DirectoryRevision) MarshalBinary() ([]byte, error) {
	if record.Freshness > FreshnessVerified || record.Known & ^knownFieldMask != 0 {
		return nil, fmt.Errorf("%w: invalid directory state", ErrMalformed)
	}
	children := append([]DirectoryChild(nil), record.Children...)
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
	e := newEncoder()
	e.u64(record.ParentInode)
	e.u32(uint32(len(children)))
	previous := ""
	for index, child := range children {
		if child.Name == "" || (index > 0 && child.Name == previous) || child.Type < NodeFile || child.Type > NodeOther {
			return nil, fmt.Errorf("%w: invalid directory child", ErrMalformed)
		}
		previous = child.Name
		if err := e.string(child.Name); err != nil {
			return nil, err
		}
		e.u64(child.Inode)
		e.u8(byte(child.Type))
		if err := e.bytes(child.MetadataKey); err != nil {
			return nil, err
		}
		parsed, err := ParseKey(child.MetadataKey)
		if err != nil || !validChildMetadataKind(child.Type, parsed.Kind) || parsed.Inode != child.Inode {
			return nil, fmt.Errorf("%w: child metadata reference mismatch", ErrMalformed)
		}
	}
	e.i64(record.MTime)
	e.i64(record.CTime)
	e.u64(record.Size)
	e.u32(record.Mode)
	e.u32(record.UID)
	e.u32(record.GID)
	e.u32(uint32(record.Known))
	if err := e.string(record.SourcePath); err != nil {
		return nil, err
	}
	e.u8(byte(record.Freshness))
	return e.finish()
}
func UnmarshalDirectoryRevision(data []byte) (DirectoryRevision, error) {
	d, err := newDecoder(data)
	if err != nil {
		return DirectoryRevision{}, err
	}
	var record DirectoryRevision
	if record.ParentInode, err = d.u64(); err != nil {
		return record, err
	}
	count, err := d.u32()
	if err != nil || uint64(count)*13 > uint64(len(data)) {
		return record, fmt.Errorf("%w: invalid child count", ErrMalformed)
	}
	record.Children = make([]DirectoryChild, count)
	previous := ""
	for index := range record.Children {
		child := &record.Children[index]
		if child.Name, err = d.string(); err != nil {
			return record, err
		}
		if child.Name == "" || (index > 0 && child.Name <= previous) {
			return record, fmt.Errorf("%w: children are not uniquely sorted", ErrMalformed)
		}
		previous = child.Name
		if child.Inode, err = d.u64(); err != nil {
			return record, err
		}
		value, readErr := d.u8()
		child.Type = NodeType(value)
		if readErr != nil || child.Type < NodeFile || child.Type > NodeOther {
			return record, fmt.Errorf("%w: invalid child type", ErrMalformed)
		}
		if child.MetadataKey, err = d.bytes(); err != nil {
			return record, err
		}
		parsed, parseErr := ParseKey(child.MetadataKey)
		if parseErr != nil || !validChildMetadataKind(child.Type, parsed.Kind) || parsed.Inode != child.Inode {
			return record, fmt.Errorf("%w: child metadata reference mismatch", ErrMalformed)
		}
	}
	// Directory metadata was appended to schema version 0. Values written by
	// earlier releases end after the child list and remain unknown/imported.
	if d.at == len(d.data) {
		return record, nil
	}
	if record.MTime, err = d.i64(); err != nil {
		return record, err
	}
	if record.CTime, err = d.i64(); err != nil {
		return record, err
	}
	if record.Size, err = d.u64(); err != nil {
		return record, err
	}
	if record.Mode, err = d.u32(); err != nil {
		return record, err
	}
	if record.UID, err = d.u32(); err != nil {
		return record, err
	}
	if record.GID, err = d.u32(); err != nil {
		return record, err
	}
	known, readErr := d.u32()
	if readErr != nil || known & ^uint32(knownFieldMask) != 0 {
		return record, fmt.Errorf("%w: invalid directory known-field mask", ErrMalformed)
	}
	record.Known = uint16(known)
	if record.SourcePath, err = d.string(); err != nil {
		return record, err
	}
	freshness, readErr := d.u8()
	if readErr != nil || Freshness(freshness) > FreshnessVerified {
		return record, fmt.Errorf("%w: invalid directory freshness", ErrMalformed)
	}
	record.Freshness = Freshness(freshness)
	return record, d.done()
}

func validChildMetadataKind(nodeType NodeType, keyKind KeyKind) bool {
	if nodeType == NodeDirectory {
		return keyKind == KeyDirectoryRevision
	}
	return nodeType >= NodeFile && nodeType <= NodeOther && keyKind == KeyInodeRevision
}

type HardlinkParentRef struct {
	ParentInode uint64
	Name        string
}

type HardlinkRefsRecord struct {
	FSID      uint32
	Inode     uint64
	Revision  uint64
	Parents   []HardlinkParentRef
	Freshness Freshness
}

func (record HardlinkRefsRecord) MarshalBinary() ([]byte, error) {
	if record.Freshness > FreshnessVerified || len(record.Parents) < 2 || len(record.Parents) > 65535 {
		return nil, fmt.Errorf("%w: invalid hardlink state", ErrMalformed)
	}
	e := newEncoder()
	e.u32(record.FSID)
	e.u64(record.Inode)
	e.u64(record.Revision)
	e.u16(uint16(len(record.Parents)))
	for _, parent := range record.Parents {
		e.u64(parent.ParentInode)
		if err := e.string(parent.Name); err != nil {
			return nil, err
		}
	}
	e.u8(byte(record.Freshness))
	return e.finish()
}

func UnmarshalHardlinkRefsRecord(data []byte) (HardlinkRefsRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return HardlinkRefsRecord{}, err
	}
	var record HardlinkRefsRecord
	if record.FSID, err = d.u32(); err != nil {
		return record, err
	}
	if record.Inode, err = d.u64(); err != nil {
		return record, err
	}
	if record.Revision, err = d.u64(); err != nil {
		return record, err
	}
	count, err := d.u16()
	if err != nil || count < 2 || uint64(count)*20 > uint64(len(data)) {
		return record, fmt.Errorf("%w: invalid hardlink parent count", ErrMalformed)
	}
	record.Parents = make([]HardlinkParentRef, count)
	previous := ""
	for index := range record.Parents {
		parent := &record.Parents[index]
		if parent.ParentInode, err = d.u64(); err != nil {
			return record, err
		}
		if parent.Name, err = d.string(); err != nil {
			return record, err
		}
		if parent.Name == "" || (index > 0 && parent.Name <= previous) {
			return record, fmt.Errorf("%w: hardlink parents are not uniquely sorted", ErrMalformed)
		}
		previous = parent.Name
	}
	freshness, err := d.u8()
	record.Freshness = Freshness(freshness)
	if err != nil || record.Freshness > FreshnessVerified {
		return record, fmt.Errorf("%w: invalid hardlink freshness", ErrMalformed)
	}
	return record, d.done()
}

type PathBindingState byte

const (
	PathBound PathBindingState = iota + 1
	PathTombstone
	PathOverflow
)

func validPathBindingState(value PathBindingState) bool {
	return value >= PathBound && value <= PathOverflow
}

type PathVersionRecord struct {
	State    PathBindingState
	NodeType NodeType
	Inode    uint64
	Revision uint64
	Path     string
}

func (record PathVersionRecord) MarshalBinary() ([]byte, error) {
	if !validPathBindingState(record.State) {
		return nil, fmt.Errorf("%w: invalid path binding state", ErrMalformed)
	}
	if record.State == PathBound {
		if record.NodeType < NodeFile || record.NodeType > NodeOther || record.Inode == 0 || record.Revision == 0 {
			return nil, fmt.Errorf("%w: invalid bound path record", ErrMalformed)
		}
	} else if record.NodeType != 0 || record.Inode != 0 || record.Revision != 0 {
		return nil, fmt.Errorf("%w: non-bound path record carries binding", ErrMalformed)
	}
	if record.State != PathOverflow && record.Path != "" {
		return nil, fmt.Errorf("%w: non-overflow path record carries overflow path", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.State))
	e.u8(byte(record.NodeType))
	e.u64(record.Inode)
	e.u64(record.Revision)
	if err := e.string(record.Path); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalPathVersionRecord(data []byte) (PathVersionRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return PathVersionRecord{}, err
	}
	var record PathVersionRecord
	state, err := d.u8()
	if err != nil {
		return record, err
	}
	record.State = PathBindingState(state)
	nodeType, err := d.u8()
	if err != nil {
		return record, err
	}
	record.NodeType = NodeType(nodeType)
	if record.Inode, err = d.u64(); err != nil {
		return record, err
	}
	if record.Revision, err = d.u64(); err != nil {
		return record, err
	}
	if record.Path, err = d.string(); err != nil {
		return record, err
	}
	if err := d.done(); err != nil {
		return record, err
	}
	if _, err := record.MarshalBinary(); err != nil {
		return PathVersionRecord{}, err
	}
	return record, nil
}

type SnapshotRecord struct {
	CommitSequence          uint64
	RootFSID                uint32
	RootInode, RootRevision uint64
	OriginalJSON            []byte
	JSONHash                ID
}

func (record SnapshotRecord) MarshalBinary() ([]byte, error) {
	if record.CommitSequence == 0 || record.RootRevision == 0 {
		return nil, fmt.Errorf("%w: invalid snapshot scope", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.CommitSequence)
	e.u32(record.RootFSID)
	e.u64(record.RootInode)
	e.u64(record.RootRevision)
	if err := e.bytes(record.OriginalJSON); err != nil {
		return nil, err
	}
	hash := sha256.Sum256(record.OriginalJSON)
	if record.JSONHash != (ID{}) && record.JSONHash != hash {
		return nil, fmt.Errorf("%w: snapshot JSON hash mismatch", ErrMalformed)
	}
	e.id(hash)
	return e.finish()
}
func UnmarshalSnapshotRecord(data []byte) (SnapshotRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return SnapshotRecord{}, err
	}
	var record SnapshotRecord
	if record.CommitSequence, err = d.u64(); err != nil {
		return record, err
	}
	if record.RootFSID, err = d.u32(); err != nil {
		return record, err
	}
	if record.RootInode, err = d.u64(); err != nil {
		return record, err
	}
	if record.RootRevision, err = d.u64(); err != nil {
		return record, err
	}
	if record.OriginalJSON, err = d.bytes(); err != nil {
		return record, err
	}
	if record.JSONHash, err = d.id(); err != nil {
		return record, err
	}
	if record.CommitSequence == 0 || record.RootRevision == 0 || ID(sha256.Sum256(record.OriginalJSON)) != record.JSONHash {
		return SnapshotRecord{}, fmt.Errorf("%w: invalid snapshot scope or hash", ErrMalformed)
	}
	return record, d.done()
}

type ContentManifest struct {
	TotalCount   uint32
	Segment      uint32
	SegmentCount uint32
	ContentIDs   []ID
}

const (
	MaxContentIDs            uint32 = 1_000_000
	MaxContentSegmentIDs     uint32 = (MaxEncodedValueBytes - 17) / 32
	DefaultContentSegmentIDs        = 4_096
)

func (record ContentManifest) MarshalBinary() ([]byte, error) {
	if record.TotalCount > MaxContentIDs || record.SegmentCount == 0 || record.Segment >= record.SegmentCount || len(record.ContentIDs) == 0 || len(record.ContentIDs) > int(MaxContentSegmentIDs) || uint64(len(record.ContentIDs)) > uint64(record.TotalCount) {
		return nil, fmt.Errorf("%w: invalid content segment", ErrMalformed)
	}
	e := newEncoder()
	e.u32(record.TotalCount)
	e.u32(record.Segment)
	e.u32(record.SegmentCount)
	e.u32(uint32(len(record.ContentIDs)))
	for _, id := range record.ContentIDs {
		e.id(id)
	}
	return e.finish()
}
func UnmarshalContentManifest(data []byte) (ContentManifest, error) {
	d, err := newDecoder(data)
	if err != nil {
		return ContentManifest{}, err
	}
	var record ContentManifest
	if record.TotalCount, err = d.u32(); err != nil {
		return record, err
	}
	if record.Segment, err = d.u32(); err != nil {
		return record, err
	}
	if record.SegmentCount, err = d.u32(); err != nil {
		return record, err
	}
	count, err := d.u32()
	if err != nil || uint64(count)*32 > uint64(len(data)) {
		return record, fmt.Errorf("%w: invalid content count", ErrMalformed)
	}
	record.ContentIDs = make([]ID, count)
	for index := range record.ContentIDs {
		if record.ContentIDs[index], err = d.id(); err != nil {
			return ContentManifest{}, err
		}
	}
	if record.TotalCount > MaxContentIDs || record.SegmentCount == 0 || record.Segment >= record.SegmentCount || count == 0 || count > MaxContentSegmentIDs || count > record.TotalCount {
		return ContentManifest{}, fmt.Errorf("%w: invalid content segment", ErrMalformed)
	}
	return record, d.done()
}
func ContentManifestID(ids []ID) ID {
	hash := sha256.New()
	hash.Write([]byte{Version})
	for _, id := range ids {
		hash.Write(id[:])
	}
	var result ID
	copy(result[:], hash.Sum(nil))
	return result
}
func SegmentContent(ids []ID, maxPerSegment int) (ID, []ContentManifest, error) {
	if len(ids) == 0 || len(ids) > int(MaxContentIDs) || maxPerSegment <= 0 || maxPerSegment > int(MaxContentSegmentIDs) {
		return ID{}, nil, fmt.Errorf("%w: invalid content segmentation", ErrMalformed)
	}
	id := ContentManifestID(ids)
	count := (len(ids) + maxPerSegment - 1) / maxPerSegment
	records := make([]ContentManifest, count)
	for segment := range count {
		start := segment * maxPerSegment
		end := min(start+maxPerSegment, len(ids))
		records[segment] = ContentManifest{TotalCount: uint32(len(ids)), Segment: uint32(segment), SegmentCount: uint32(count), ContentIDs: append([]ID(nil), ids[start:end]...)}
	}
	return id, records, nil
}

func EqualIDs(left, right []ID) bool { return bytes.Equal(flattenIDs(left), flattenIDs(right)) }
func flattenIDs(ids []ID) []byte {
	result := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		result = append(result, id[:]...)
	}
	return result
}
