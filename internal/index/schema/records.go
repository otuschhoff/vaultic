package schema

import (
	"fmt"
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
