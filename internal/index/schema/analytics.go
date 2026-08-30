package schema

import (
	"fmt"
)

type AnalyticsColumnKind byte

const (
	AnalyticsColumnIdentity AnalyticsColumnKind = iota + 1
	AnalyticsColumnUID
	AnalyticsColumnGID
	AnalyticsColumnCreatedAt
	AnalyticsColumnCreationBasis
	AnalyticsColumnIdentityContinuity
	AnalyticsColumnCalendarYear
	AnalyticsColumnCalendarMonth
	AnalyticsColumnISOYear
	AnalyticsColumnWorkweek
	AnalyticsColumnSVM
	AnalyticsColumnVolume
	AnalyticsColumnPathGroup
	AnalyticsColumnLogicalSize
	AnalyticsColumnSizeLog10
)

type AnalyticsColumnCodec byte

const (
	AnalyticsCodecRaw AnalyticsColumnCodec = iota + 1
	AnalyticsCodecDelta
	AnalyticsCodecRLE
	AnalyticsCodecZstd
	AnalyticsCodecRoaring
)

type AnalyticsColumn struct {
	Kind  AnalyticsColumnKind
	Codec AnalyticsColumnCodec
	Data  []byte
}

type AnalyticsFactSegmentRecord struct {
	RowCount uint32
	Columns  []AnalyticsColumn
}

func (record AnalyticsFactSegmentRecord) MarshalBinary() ([]byte, error) {
	if !validAnalyticsFactSegment(record) {
		return nil, fmt.Errorf("%w: invalid analytics fact segment", ErrMalformed)
	}
	e := newEncoder()
	e.u32(record.RowCount)
	e.u16(uint16(len(record.Columns)))
	for _, column := range record.Columns {
		e.u8(byte(column.Kind))
		e.u8(byte(column.Codec))
		if err := e.bytes(column.Data); err != nil {
			return nil, err
		}
	}
	return e.finish()
}

func UnmarshalAnalyticsFactSegmentRecord(data []byte) (AnalyticsFactSegmentRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsFactSegmentRecord{}, err
	}
	var record AnalyticsFactSegmentRecord
	if record.RowCount, err = d.u32(); err != nil {
		return record, err
	}
	count, err := d.u16()
	if err != nil {
		return record, err
	}
	record.Columns = make([]AnalyticsColumn, count)
	for index := range record.Columns {
		kind, readErr := d.u8()
		if readErr != nil {
			return record, readErr
		}
		codec, readErr := d.u8()
		if readErr != nil {
			return record, readErr
		}
		record.Columns[index].Kind = AnalyticsColumnKind(kind)
		record.Columns[index].Codec = AnalyticsColumnCodec(codec)
		if record.Columns[index].Data, err = d.bytes(); err != nil {
			return record, err
		}
	}
	if !validAnalyticsFactSegment(record) {
		return AnalyticsFactSegmentRecord{}, fmt.Errorf("%w: invalid analytics fact segment", ErrMalformed)
	}
	return record, d.done()
}

func validAnalyticsFactSegment(record AnalyticsFactSegmentRecord) bool {
	if record.RowCount == 0 || len(record.Columns) == 0 || len(record.Columns) > int(AnalyticsColumnSizeLog10) {
		return false
	}
	previous := AnalyticsColumnKind(0)
	for _, column := range record.Columns {
		if column.Kind <= previous || column.Kind > AnalyticsColumnSizeLog10 || column.Codec < AnalyticsCodecRaw || column.Codec > AnalyticsCodecZstd || len(column.Data) == 0 {
			return false
		}
		previous = column.Kind
	}
	return true
}

type AnalyticsDictionaryRecord struct {
	Value string
}

func (record AnalyticsDictionaryRecord) MarshalBinary() ([]byte, error) {
	if record.Value == "" {
		return nil, fmt.Errorf("%w: empty analytics dictionary value", ErrMalformed)
	}
	e := newEncoder()
	if err := e.string(record.Value); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalAnalyticsDictionaryRecord(data []byte) (AnalyticsDictionaryRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsDictionaryRecord{}, err
	}
	value, err := d.string()
	if err != nil || value == "" {
		return AnalyticsDictionaryRecord{}, fmt.Errorf("%w: empty analytics dictionary value", ErrMalformed)
	}
	return AnalyticsDictionaryRecord{Value: value}, d.done()
}

type AnalyticsSegmentMetadataRecord struct {
	RowCount            uint32
	MinCreatedAt        int64
	MaxCreatedAt        int64
	MinLogicalSize      uint64
	MaxLogicalSize      uint64
	MinRevision         uint64
	MaxRevision         uint64
	FirstCommit         uint64
	LastCommit          uint64
	ClassificationEpoch uint64
	Bloom               []byte
	CodecParameters     string
}

func (record AnalyticsSegmentMetadataRecord) MarshalBinary() ([]byte, error) {
	if !validAnalyticsSegmentMetadata(record) {
		return nil, fmt.Errorf("%w: invalid analytics segment metadata", ErrMalformed)
	}
	e := newEncoder()
	e.u32(record.RowCount)
	e.i64(record.MinCreatedAt)
	e.i64(record.MaxCreatedAt)
	e.u64(record.MinLogicalSize)
	e.u64(record.MaxLogicalSize)
	e.u64(record.MinRevision)
	e.u64(record.MaxRevision)
	e.u64(record.FirstCommit)
	e.u64(record.LastCommit)
	e.u64(record.ClassificationEpoch)
	if err := e.bytes(record.Bloom); err != nil {
		return nil, err
	}
	if err := e.string(record.CodecParameters); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalAnalyticsSegmentMetadataRecord(data []byte) (AnalyticsSegmentMetadataRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsSegmentMetadataRecord{}, err
	}
	var record AnalyticsSegmentMetadataRecord
	if record.RowCount, err = d.u32(); err != nil {
		return record, err
	}
	if record.MinCreatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.MaxCreatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.MinLogicalSize, err = d.u64(); err != nil {
		return record, err
	}
	if record.MaxLogicalSize, err = d.u64(); err != nil {
		return record, err
	}
	if record.MinRevision, err = d.u64(); err != nil {
		return record, err
	}
	if record.MaxRevision, err = d.u64(); err != nil {
		return record, err
	}
	if record.FirstCommit, err = d.u64(); err != nil {
		return record, err
	}
	if record.LastCommit, err = d.u64(); err != nil {
		return record, err
	}
	if record.ClassificationEpoch, err = d.u64(); err != nil {
		return record, err
	}
	if record.Bloom, err = d.bytes(); err != nil {
		return record, err
	}
	if record.CodecParameters, err = d.string(); err != nil {
		return record, err
	}
	if !validAnalyticsSegmentMetadata(record) {
		return AnalyticsSegmentMetadataRecord{}, fmt.Errorf("%w: invalid analytics segment metadata", ErrMalformed)
	}
	return record, d.done()
}

func validAnalyticsSegmentMetadata(record AnalyticsSegmentMetadataRecord) bool {
	return record.RowCount > 0 && record.MinCreatedAt <= record.MaxCreatedAt && record.MinLogicalSize <= record.MaxLogicalSize && record.MinRevision > 0 && record.MinRevision <= record.MaxRevision && record.FirstCommit > 0 && record.FirstCommit <= record.LastCommit && record.ClassificationEpoch > 0
}

type AnalyticsDimensionIndexRecord struct {
	Codec        AnalyticsColumnCodec
	RowCount     uint32
	MatchCount   uint32
	LogicalBytes uint64
	Bitmap       []byte
}

func (record AnalyticsDimensionIndexRecord) MarshalBinary() ([]byte, error) {
	if record.Codec != AnalyticsCodecRaw && record.Codec != AnalyticsCodecRoaring && record.Codec != AnalyticsCodecZstd || record.RowCount == 0 || record.MatchCount == 0 || record.MatchCount > record.RowCount || len(record.Bitmap) == 0 {
		return nil, fmt.Errorf("%w: invalid analytics dimension index", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.Codec))
	e.u32(record.RowCount)
	e.u32(record.MatchCount)
	e.u64(record.LogicalBytes)
	if err := e.bytes(record.Bitmap); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalAnalyticsDimensionIndexRecord(data []byte) (AnalyticsDimensionIndexRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsDimensionIndexRecord{}, err
	}
	var record AnalyticsDimensionIndexRecord
	codec, err := d.u8()
	if err != nil {
		return record, err
	}
	record.Codec = AnalyticsColumnCodec(codec)
	if record.RowCount, err = d.u32(); err != nil {
		return record, err
	}
	if record.MatchCount, err = d.u32(); err != nil {
		return record, err
	}
	if record.LogicalBytes, err = d.u64(); err != nil {
		return record, err
	}
	if record.Bitmap, err = d.bytes(); err != nil {
		return record, err
	}
	if record.Codec != AnalyticsCodecRaw && record.Codec != AnalyticsCodecRoaring && record.Codec != AnalyticsCodecZstd || record.RowCount == 0 || record.MatchCount == 0 || record.MatchCount > record.RowCount || len(record.Bitmap) == 0 {
		return AnalyticsDimensionIndexRecord{}, fmt.Errorf("%w: invalid analytics dimension index", ErrMalformed)
	}
	return record, d.done()
}

type AnalyticsResidencyRecord struct {
	State                AnalyticsResidency
	LastCompleteCrawl    int64
	RetainedSnapshotRefs uint64
	ClassificationEpoch  uint64
	FactSegment          uint64
	Row                  uint32
}

func (record AnalyticsResidencyRecord) MarshalBinary() ([]byte, error) {
	if !validAnalyticsResidency(record) {
		return nil, fmt.Errorf("%w: invalid analytics residency", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.State))
	e.i64(record.LastCompleteCrawl)
	e.u64(record.RetainedSnapshotRefs)
	e.u64(record.ClassificationEpoch)
	e.u64(record.FactSegment)
	e.u32(record.Row)
	return e.finish()
}

func UnmarshalAnalyticsResidencyRecord(data []byte) (AnalyticsResidencyRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsResidencyRecord{}, err
	}
	var record AnalyticsResidencyRecord
	state, err := d.u8()
	if err != nil {
		return record, err
	}
	record.State = AnalyticsResidency(state)
	if record.LastCompleteCrawl, err = d.i64(); err != nil {
		return record, err
	}
	if record.RetainedSnapshotRefs, err = d.u64(); err != nil {
		return record, err
	}
	if record.ClassificationEpoch, err = d.u64(); err != nil {
		return record, err
	}
	if record.FactSegment, err = d.u64(); err != nil {
		return record, err
	}
	if record.Row, err = d.u32(); err != nil {
		return record, err
	}
	if !validAnalyticsResidency(record) {
		return AnalyticsResidencyRecord{}, fmt.Errorf("%w: invalid analytics residency", ErrMalformed)
	}
	return record, d.done()
}

func validAnalyticsResidency(record AnalyticsResidencyRecord) bool {
	if record.State < AnalyticsLive || record.State > AnalyticsExpired || record.ClassificationEpoch == 0 || record.FactSegment == 0 {
		return false
	}
	if record.State == AnalyticsArchiveOnly && (record.LastCompleteCrawl == 0 || record.RetainedSnapshotRefs == 0) {
		return false
	}
	if record.State == AnalyticsExpired && record.RetainedSnapshotRefs != 0 {
		return false
	}
	return true
}

type AnalyticsDeltaKind byte

const (
	AnalyticsDeltaCreation AnalyticsDeltaKind = iota + 1
	AnalyticsDeltaSourceState
	AnalyticsDeltaRetainedReferences
	AnalyticsDeltaClassification
)

type AnalyticsReferenceOperation byte

const (
	AnalyticsReferencesLegacy AnalyticsReferenceOperation = iota
	AnalyticsReferencesSet
	AnalyticsReferencesIncrement
	AnalyticsReferencesDecrement
)

type AnalyticsDeltaRecord struct {
	Kind                 AnalyticsDeltaKind
	FSID                 uint32
	Inode                uint64
	IdentityGeneration   uint64
	Revision             uint64
	UID                  uint32
	GID                  uint32
	Known                uint16
	CreatedAt            int64
	LogicalSize          uint64
	CreationBasis        AnalyticsCreationBasis
	IdentityContinuity   AnalyticsIdentityContinuity
	State                AnalyticsResidency
	RetainedSnapshotRefs uint64
	ClassificationEpoch  uint64
	SVM                  uint32
	Volume               uint32
	PathGroup            uint32
	ReferenceOperation   AnalyticsReferenceOperation
}

func (record AnalyticsDeltaRecord) MarshalBinary() ([]byte, error) {
	if !validAnalyticsDelta(record) {
		return nil, fmt.Errorf("%w: invalid analytics delta", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.Kind))
	e.u32(record.FSID)
	e.u64(record.Inode)
	e.u64(record.IdentityGeneration)
	e.u64(record.Revision)
	e.u32(record.UID)
	e.u32(record.GID)
	e.u16(record.Known)
	e.i64(record.CreatedAt)
	e.u64(record.LogicalSize)
	e.u8(byte(record.CreationBasis))
	e.u8(byte(record.IdentityContinuity))
	e.u8(byte(record.State))
	e.u64(record.RetainedSnapshotRefs)
	e.u64(record.ClassificationEpoch)
	e.u32(record.SVM)
	e.u32(record.Volume)
	e.u32(record.PathGroup)
	e.u8(byte(record.ReferenceOperation))
	return e.finish()
}

func UnmarshalAnalyticsDeltaRecord(data []byte) (AnalyticsDeltaRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsDeltaRecord{}, err
	}
	var record AnalyticsDeltaRecord
	kind, err := d.u8()
	if err != nil {
		return record, err
	}
	record.Kind = AnalyticsDeltaKind(kind)
	if record.FSID, err = d.u32(); err != nil {
		return record, err
	}
	if record.Inode, err = d.u64(); err != nil {
		return record, err
	}
	if record.IdentityGeneration, err = d.u64(); err != nil {
		return record, err
	}
	if record.Revision, err = d.u64(); err != nil {
		return record, err
	}
	if record.UID, err = d.u32(); err != nil {
		return record, err
	}
	if record.GID, err = d.u32(); err != nil {
		return record, err
	}
	if record.Known, err = d.u16(); err != nil {
		return record, err
	}
	if record.CreatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.LogicalSize, err = d.u64(); err != nil {
		return record, err
	}
	basis, err := d.u8()
	if err != nil {
		return record, err
	}
	record.CreationBasis = AnalyticsCreationBasis(basis)
	continuity, err := d.u8()
	if err != nil {
		return record, err
	}
	record.IdentityContinuity = AnalyticsIdentityContinuity(continuity)
	state, err := d.u8()
	if err != nil {
		return record, err
	}
	record.State = AnalyticsResidency(state)
	if record.RetainedSnapshotRefs, err = d.u64(); err != nil {
		return record, err
	}
	if record.ClassificationEpoch, err = d.u64(); err != nil {
		return record, err
	}
	if record.SVM, err = d.u32(); err != nil {
		return record, err
	}
	if record.Volume, err = d.u32(); err != nil {
		return record, err
	}
	if record.PathGroup, err = d.u32(); err != nil {
		return record, err
	}
	if d.at != len(d.data) {
		operation, readErr := d.u8()
		if readErr != nil {
			return record, readErr
		}
		record.ReferenceOperation = AnalyticsReferenceOperation(operation)
	}
	if !validAnalyticsDelta(record) {
		return AnalyticsDeltaRecord{}, fmt.Errorf("%w: invalid analytics delta", ErrMalformed)
	}
	return record, d.done()
}

func validAnalyticsDelta(record AnalyticsDeltaRecord) bool {
	if record.Kind < AnalyticsDeltaCreation || record.Kind > AnalyticsDeltaClassification || record.IdentityGeneration == 0 || record.Revision == 0 || record.Known&^knownFieldMask != 0 || record.ClassificationEpoch == 0 || record.IdentityContinuity > AnalyticsContinuitySourceGeneration || record.State > AnalyticsExpired || record.ReferenceOperation > AnalyticsReferencesDecrement {
		return false
	}
	if record.Kind != AnalyticsDeltaRetainedReferences && record.ReferenceOperation != AnalyticsReferencesLegacy {
		return false
	}
	if record.Kind == AnalyticsDeltaCreation {
		return record.CreationBasis >= AnalyticsCTime && record.CreationBasis <= AnalyticsFirstSeen
	}
	return record.State >= AnalyticsLive
}

type AuthoritativeCrawlProofRecord struct {
	ScopeID     ID
	RootFSID    uint32
	RootInode   uint64
	StartFence  uint64
	EndCommit   uint64
	CompletedAt int64
	Complete    bool
	DebtFree    bool
}

func (record AuthoritativeCrawlProofRecord) MarshalBinary() ([]byte, error) {
	if !validAuthoritativeCrawlProof(record) {
		return nil, fmt.Errorf("%w: invalid authoritative crawl proof", ErrMalformed)
	}
	e := newEncoder()
	e.id(record.ScopeID)
	e.u32(record.RootFSID)
	e.u64(record.RootInode)
	e.u64(record.StartFence)
	e.u64(record.EndCommit)
	e.i64(record.CompletedAt)
	e.bool(record.Complete)
	e.bool(record.DebtFree)
	return e.finish()
}

func UnmarshalAuthoritativeCrawlProofRecord(data []byte) (AuthoritativeCrawlProofRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AuthoritativeCrawlProofRecord{}, err
	}
	var record AuthoritativeCrawlProofRecord
	if record.ScopeID, err = d.id(); err != nil {
		return record, err
	}
	if record.RootFSID, err = d.u32(); err != nil {
		return record, err
	}
	if record.RootInode, err = d.u64(); err != nil {
		return record, err
	}
	if record.StartFence, err = d.u64(); err != nil {
		return record, err
	}
	if record.EndCommit, err = d.u64(); err != nil {
		return record, err
	}
	if record.CompletedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.Complete, err = d.bool(); err != nil {
		return record, err
	}
	if record.DebtFree, err = d.bool(); err != nil {
		return record, err
	}
	if !validAuthoritativeCrawlProof(record) {
		return AuthoritativeCrawlProofRecord{}, fmt.Errorf("%w: invalid authoritative crawl proof", ErrMalformed)
	}
	return record, d.done()
}

func validAuthoritativeCrawlProof(record AuthoritativeCrawlProofRecord) bool {
	return record.ScopeID != (ID{}) && record.RootFSID != 0 && record.RootInode != 0 && record.StartFence != 0 && record.EndCommit >= record.StartFence && record.CompletedAt != 0
}

type AuthoritativeSourceState byte

const (
	AuthoritativeSourceLive AuthoritativeSourceState = iota + 1
	AuthoritativeSourceDeleted
	AuthoritativeSourceUnknown
)

type AuthoritativeSourceBindingRecord struct {
	Generation         uint64
	Revision           uint64
	State              AuthoritativeSourceState
	Continuity         AnalyticsIdentityContinuity
	LastObservedCommit uint64
}

func (record AuthoritativeSourceBindingRecord) MarshalBinary() ([]byte, error) {
	if !validAuthoritativeSourceBinding(record) {
		return nil, fmt.Errorf("%w: invalid authoritative source binding", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.Generation)
	e.u64(record.Revision)
	e.u8(byte(record.State))
	e.u8(byte(record.Continuity))
	e.u64(record.LastObservedCommit)
	return e.finish()
}

func UnmarshalAuthoritativeSourceBindingRecord(data []byte) (AuthoritativeSourceBindingRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AuthoritativeSourceBindingRecord{}, err
	}
	var record AuthoritativeSourceBindingRecord
	if record.Generation, err = d.u64(); err != nil {
		return record, err
	}
	if record.Revision, err = d.u64(); err != nil {
		return record, err
	}
	state, err := d.u8()
	if err != nil {
		return record, err
	}
	record.State = AuthoritativeSourceState(state)
	continuity, err := d.u8()
	if err != nil {
		return record, err
	}
	record.Continuity = AnalyticsIdentityContinuity(continuity)
	if record.LastObservedCommit, err = d.u64(); err != nil {
		return record, err
	}
	if !validAuthoritativeSourceBinding(record) {
		return AuthoritativeSourceBindingRecord{}, fmt.Errorf("%w: invalid authoritative source binding", ErrMalformed)
	}
	return record, d.done()
}

func validAuthoritativeSourceBinding(record AuthoritativeSourceBindingRecord) bool {
	return record.Generation != 0 && record.Revision != 0 && record.State >= AuthoritativeSourceLive && record.State <= AuthoritativeSourceUnknown && record.Continuity <= AnalyticsContinuitySourceGeneration && record.LastObservedCommit != 0
}

type AnalyticsWatermarkRecord struct {
	RepositoryGeneration uint64
	AppliedCommit        uint64
	ManifestGeneration   uint64
	AppliedAt            int64
}

func (record AnalyticsWatermarkRecord) MarshalBinary() ([]byte, error) {
	if record.RepositoryGeneration == 0 || record.ManifestGeneration == 0 || record.AppliedAt == 0 {
		return nil, fmt.Errorf("%w: invalid analytics watermark", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.RepositoryGeneration)
	e.u64(record.AppliedCommit)
	e.u64(record.ManifestGeneration)
	e.i64(record.AppliedAt)
	return e.finish()
}

func UnmarshalAnalyticsWatermarkRecord(data []byte) (AnalyticsWatermarkRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsWatermarkRecord{}, err
	}
	var record AnalyticsWatermarkRecord
	if record.RepositoryGeneration, err = d.u64(); err != nil {
		return record, err
	}
	if record.AppliedCommit, err = d.u64(); err != nil {
		return record, err
	}
	if record.ManifestGeneration, err = d.u64(); err != nil {
		return record, err
	}
	if record.AppliedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.RepositoryGeneration == 0 || record.ManifestGeneration == 0 || record.AppliedAt == 0 {
		return AnalyticsWatermarkRecord{}, fmt.Errorf("%w: invalid analytics watermark", ErrMalformed)
	}
	return record, d.done()
}

type AnalyticsManifestRecord struct {
	Generation       uint64
	ParentGeneration uint64
	LayerDepth       uint8
	Segments         []uint64
}

func (record AnalyticsManifestRecord) MarshalBinary() ([]byte, error) {
	if record.Generation == 0 || record.ParentGeneration >= record.Generation || record.ParentGeneration == 0 && record.LayerDepth != 0 || record.ParentGeneration != 0 && record.LayerDepth == 0 || !strictlyIncreasingOrEmpty(record.Segments) {
		return nil, fmt.Errorf("%w: invalid analytics manifest", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.Generation)
	e.u64(record.ParentGeneration)
	e.u8(record.LayerDepth)
	e.u32(uint32(len(record.Segments)))
	for _, segment := range record.Segments {
		e.u64(segment)
	}
	return e.finish()
}

func UnmarshalAnalyticsManifestRecord(data []byte) (AnalyticsManifestRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsManifestRecord{}, err
	}
	var record AnalyticsManifestRecord
	if record.Generation, err = d.u64(); err != nil {
		return record, err
	}
	if record.ParentGeneration, err = d.u64(); err != nil {
		return record, err
	}
	if record.LayerDepth, err = d.u8(); err != nil {
		return record, err
	}
	count, err := d.u32()
	if err != nil || uint64(count) > uint64(MaxEncodedValueBytes/8) {
		return record, fmt.Errorf("%w: invalid analytics manifest", ErrMalformed)
	}
	record.Segments = make([]uint64, count)
	for index := range record.Segments {
		if record.Segments[index], err = d.u64(); err != nil {
			return record, err
		}
	}
	if record.Generation == 0 || record.ParentGeneration >= record.Generation || record.ParentGeneration == 0 && record.LayerDepth != 0 || record.ParentGeneration != 0 && record.LayerDepth == 0 || !strictlyIncreasingOrEmpty(record.Segments) {
		return AnalyticsManifestRecord{}, fmt.Errorf("%w: invalid analytics manifest", ErrMalformed)
	}
	return record, d.done()
}

func strictlyIncreasing(values []uint64) bool {
	for index, value := range values {
		if value == 0 || index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
}

type AnalyticsBuildCheckpointRecord struct {
	BuildID           ID
	FormatVersion     uint8
	Generation        uint64
	ConfigJSON        string
	SourceKeyCursor   []byte
	Facts             uint64
	AppliedCommit     uint64
	CandidateSegments []uint64
	StartedAt         int64
	UpdatedAt         int64
}

func (record AnalyticsBuildCheckpointRecord) MarshalBinary() ([]byte, error) {
	if !validAnalyticsBuildCheckpoint(record) {
		return nil, fmt.Errorf("%w: invalid analytics build checkpoint", ErrMalformed)
	}
	e := newEncoder()
	e.id(record.BuildID)
	e.u8(record.FormatVersion)
	e.u64(record.Generation)
	if err := e.string(record.ConfigJSON); err != nil {
		return nil, err
	}
	if err := e.bytes(record.SourceKeyCursor); err != nil {
		return nil, err
	}
	e.u64(record.Facts)
	e.u64(record.AppliedCommit)
	e.u32(uint32(len(record.CandidateSegments)))
	for _, segment := range record.CandidateSegments {
		e.u64(segment)
	}
	e.i64(record.StartedAt)
	e.i64(record.UpdatedAt)
	return e.finish()
}

func UnmarshalAnalyticsBuildCheckpointRecord(data []byte) (AnalyticsBuildCheckpointRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsBuildCheckpointRecord{}, err
	}
	var record AnalyticsBuildCheckpointRecord
	if record.BuildID, err = d.id(); err != nil {
		return record, err
	}
	if record.FormatVersion, err = d.u8(); err != nil {
		return record, err
	}
	if record.Generation, err = d.u64(); err != nil {
		return record, err
	}
	if record.ConfigJSON, err = d.string(); err != nil {
		return record, err
	}
	if record.SourceKeyCursor, err = d.bytes(); err != nil {
		return record, err
	}
	if record.Facts, err = d.u64(); err != nil {
		return record, err
	}
	if record.AppliedCommit, err = d.u64(); err != nil {
		return record, err
	}
	count, err := d.u32()
	if err != nil || uint64(count) > uint64(MaxEncodedValueBytes/8) {
		return record, fmt.Errorf("%w: invalid analytics build checkpoint", ErrMalformed)
	}
	record.CandidateSegments = make([]uint64, count)
	for index := range record.CandidateSegments {
		if record.CandidateSegments[index], err = d.u64(); err != nil {
			return record, err
		}
	}
	if record.StartedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.UpdatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if !validAnalyticsBuildCheckpoint(record) {
		return AnalyticsBuildCheckpointRecord{}, fmt.Errorf("%w: invalid analytics build checkpoint", ErrMalformed)
	}
	return record, d.done()
}

func validAnalyticsBuildCheckpoint(record AnalyticsBuildCheckpointRecord) bool {
	return record.BuildID != (ID{}) && record.FormatVersion == 1 && record.Generation != 0 && record.ConfigJSON != "" && strictlyIncreasingOrEmpty(record.CandidateSegments) && record.StartedAt != 0 && record.UpdatedAt >= record.StartedAt
}

type AnalyticsAggregateRecord struct {
	BytesAdded   uint64
	BytesDeleted uint64
	FilesAdded   uint64
	FilesDeleted uint64
}

func (record AnalyticsAggregateRecord) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	e.u64(record.BytesAdded)
	e.u64(record.BytesDeleted)
	e.u64(record.FilesAdded)
	e.u64(record.FilesDeleted)
	return e.finish()
}

func UnmarshalAnalyticsAggregateRecord(data []byte) (AnalyticsAggregateRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsAggregateRecord{}, err
	}
	var record AnalyticsAggregateRecord
	if record.BytesAdded, err = d.u64(); err != nil {
		return record, err
	}
	if record.BytesDeleted, err = d.u64(); err != nil {
		return record, err
	}
	if record.FilesAdded, err = d.u64(); err != nil {
		return record, err
	}
	if record.FilesDeleted, err = d.u64(); err != nil {
		return record, err
	}
	return record, d.done()
}

type AnalyticsSummaryRecord struct {
	ActiveBytes     uint64
	ActiveFiles     uint64
	UniqueBlobCount uint64
	UniqueBlobBytes uint64
}

func (record AnalyticsSummaryRecord) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	e.u64(record.ActiveBytes)
	e.u64(record.ActiveFiles)
	e.u64(record.UniqueBlobCount)
	e.u64(record.UniqueBlobBytes)
	return e.finish()
}
func UnmarshalAnalyticsSummaryRecord(data []byte) (AnalyticsSummaryRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsSummaryRecord{}, err
	}
	var record AnalyticsSummaryRecord
	if record.ActiveBytes, err = d.u64(); err != nil {
		return record, err
	}
	if record.ActiveFiles, err = d.u64(); err != nil {
		return record, err
	}
	if record.UniqueBlobCount, err = d.u64(); err != nil {
		return record, err
	}
	if record.UniqueBlobBytes, err = d.u64(); err != nil {
		return record, err
	}
	return record, d.done()
}

type AnalyticsUserInodeRecord struct {
	LatestRevision uint64
	PathSample     string
}

func (record AnalyticsUserInodeRecord) MarshalBinary() ([]byte, error) {
	if record.LatestRevision == 0 {
		return nil, fmt.Errorf("%w: invalid analytics user inode", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.LatestRevision)
	if err := e.string(record.PathSample); err != nil {
		return nil, err
	}
	return e.finish()
}
func UnmarshalAnalyticsUserInodeRecord(data []byte) (AnalyticsUserInodeRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsUserInodeRecord{}, err
	}
	var record AnalyticsUserInodeRecord
	if record.LatestRevision, err = d.u64(); err != nil {
		return record, err
	}
	if record.PathSample, err = d.string(); err != nil {
		return record, err
	}
	if record.LatestRevision == 0 {
		return AnalyticsUserInodeRecord{}, fmt.Errorf("%w: invalid analytics user inode", ErrMalformed)
	}
	return record, d.done()
}

type AnalyticsUserBlobRecord struct {
	ReferenceCount uint64
	FirstSeen      int64
}

func (record AnalyticsUserBlobRecord) MarshalBinary() ([]byte, error) {
	if record.ReferenceCount == 0 || record.FirstSeen == 0 {
		return nil, fmt.Errorf("%w: invalid analytics user blob", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.ReferenceCount)
	e.i64(record.FirstSeen)
	return e.finish()
}
func UnmarshalAnalyticsUserBlobRecord(data []byte) (AnalyticsUserBlobRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsUserBlobRecord{}, err
	}
	var record AnalyticsUserBlobRecord
	if record.ReferenceCount, err = d.u64(); err != nil {
		return record, err
	}
	if record.FirstSeen, err = d.i64(); err != nil {
		return record, err
	}
	if record.ReferenceCount == 0 || record.FirstSeen == 0 {
		return AnalyticsUserBlobRecord{}, fmt.Errorf("%w: invalid analytics user blob", ErrMalformed)
	}
	return record, d.done()
}

type AnalyticsQueryJobState byte

const (
	AnalyticsQueryPending AnalyticsQueryJobState = iota + 1
	AnalyticsQueryRunning
	AnalyticsQueryComplete
	AnalyticsQueryFailed
	AnalyticsQueryCancelled
)

type AnalyticsQueryJobRecord struct {
	State                AnalyticsQueryJobState
	CanonicalQuery       []byte
	RepositoryGeneration uint64
	ClassificationEpoch  uint64
	AppliedCommit        uint64
	CompletedSegments    []uint64
	RowsScanned          uint64
	UpdatedAt            int64
	Result               []byte
	Error                string
}

func (record AnalyticsQueryJobRecord) MarshalBinary() ([]byte, error) {
	if !validAnalyticsQueryJob(record) {
		return nil, fmt.Errorf("%w: invalid analytics query job", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.State))
	if err := e.bytes(record.CanonicalQuery); err != nil {
		return nil, err
	}
	e.u64(record.RepositoryGeneration)
	e.u64(record.ClassificationEpoch)
	e.u64(record.AppliedCommit)
	e.u32(uint32(len(record.CompletedSegments)))
	for _, segment := range record.CompletedSegments {
		e.u64(segment)
	}
	e.u64(record.RowsScanned)
	e.i64(record.UpdatedAt)
	if err := e.bytes(record.Result); err != nil {
		return nil, err
	}
	if err := e.string(record.Error); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalAnalyticsQueryJobRecord(data []byte) (AnalyticsQueryJobRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsQueryJobRecord{}, err
	}
	var record AnalyticsQueryJobRecord
	state, err := d.u8()
	if err != nil {
		return record, err
	}
	record.State = AnalyticsQueryJobState(state)
	if record.CanonicalQuery, err = d.bytes(); err != nil {
		return record, err
	}
	if record.RepositoryGeneration, err = d.u64(); err != nil {
		return record, err
	}
	if record.ClassificationEpoch, err = d.u64(); err != nil {
		return record, err
	}
	if record.AppliedCommit, err = d.u64(); err != nil {
		return record, err
	}
	count, err := d.u32()
	if err != nil || uint64(count) > uint64(MaxEncodedValueBytes/8) {
		return record, fmt.Errorf("%w: invalid analytics query job", ErrMalformed)
	}
	record.CompletedSegments = make([]uint64, count)
	for index := range record.CompletedSegments {
		if record.CompletedSegments[index], err = d.u64(); err != nil {
			return record, err
		}
	}
	if record.RowsScanned, err = d.u64(); err != nil {
		return record, err
	}
	if record.UpdatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.Result, err = d.bytes(); err != nil {
		return record, err
	}
	if record.Error, err = d.string(); err != nil {
		return record, err
	}
	if !validAnalyticsQueryJob(record) {
		return AnalyticsQueryJobRecord{}, fmt.Errorf("%w: invalid analytics query job", ErrMalformed)
	}
	return record, d.done()
}

func validAnalyticsQueryJob(record AnalyticsQueryJobRecord) bool {
	if record.State < AnalyticsQueryPending || record.State > AnalyticsQueryCancelled || len(record.CanonicalQuery) == 0 || record.RepositoryGeneration == 0 || record.ClassificationEpoch == 0 || record.UpdatedAt == 0 || !strictlyIncreasingOrEmpty(record.CompletedSegments) {
		return false
	}
	if record.State == AnalyticsQueryComplete && len(record.Result) == 0 {
		return false
	}
	if record.State == AnalyticsQueryFailed && record.Error == "" {
		return false
	}
	return true
}

func strictlyIncreasingOrEmpty(values []uint64) bool {
	return len(values) == 0 || strictlyIncreasing(values)
}

type AnalyticsQueryRecord struct {
	Payload []byte
}

func (record AnalyticsQueryRecord) MarshalBinary() ([]byte, error) {
	if len(record.Payload) == 0 {
		return nil, fmt.Errorf("%w: empty analytics query payload", ErrMalformed)
	}
	e := newEncoder()
	if err := e.bytes(record.Payload); err != nil {
		return nil, err
	}
	return e.finish()
}
func UnmarshalAnalyticsQueryRecord(data []byte) (AnalyticsQueryRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsQueryRecord{}, err
	}
	payload, err := d.bytes()
	if err != nil || len(payload) == 0 {
		return AnalyticsQueryRecord{}, fmt.Errorf("%w: empty analytics query payload", ErrMalformed)
	}
	return AnalyticsQueryRecord{Payload: payload}, d.done()
}
