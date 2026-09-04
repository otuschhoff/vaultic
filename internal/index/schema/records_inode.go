package schema

import (
	"fmt"
)

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

type AnalyticsResidency byte

const (
	AnalyticsLive AnalyticsResidency = iota + 1
	AnalyticsArchiveOnly
	AnalyticsUnknown
	AnalyticsDeleted
	AnalyticsExpired
)

type AnalyticsCreationBasis byte

const (
	AnalyticsCTime AnalyticsCreationBasis = iota + 1
	AnalyticsMTime
	AnalyticsTimeUnknown
	AnalyticsBirthTime
	AnalyticsFirstSeen
)

type AnalyticsIdentityContinuity byte

const (
	AnalyticsContinuityUnknown AnalyticsIdentityContinuity = iota
	AnalyticsContinuityProven
	AnalyticsContinuitySourceGeneration
)

type AnalyticsFactRecord struct {
	Revision           uint64
	UID                uint32
	GID                uint32
	Known              uint16
	CreatedAt          int64
	LogicalSize        uint64
	CalendarYear       int32
	CalendarMonth      uint8
	ISOYear            int32
	Workweek           uint8
	SizeLog10          uint8
	SourcePath         string
	SVM                string
	Volume             string
	PathGroup          string
	Residency          AnalyticsResidency
	CreationBasis      AnalyticsCreationBasis
	IdentityGeneration uint64
	IdentityContinuity AnalyticsIdentityContinuity
}

func (record AnalyticsFactRecord) MarshalBinary() ([]byte, error) {
	if !validAnalyticsFact(record) {
		return nil, fmt.Errorf("%w: invalid analytics fact", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.Revision)
	e.u32(record.UID)
	e.u32(record.GID)
	e.u32(uint32(record.Known))
	e.i64(record.CreatedAt)
	e.u64(record.LogicalSize)
	e.u32(uint32(record.CalendarYear))
	e.u8(record.CalendarMonth)
	e.u32(uint32(record.ISOYear))
	e.u8(record.Workweek)
	e.u8(record.SizeLog10)
	for _, value := range []string{record.SourcePath, record.SVM, record.Volume, record.PathGroup} {
		if err := e.string(value); err != nil {
			return nil, err
		}
	}
	e.u8(byte(record.Residency))
	e.u8(byte(record.CreationBasis))
	e.u64(record.IdentityGeneration)
	e.u8(byte(record.IdentityContinuity))
	return e.finish()
}

func UnmarshalAnalyticsFactRecord(data []byte) (AnalyticsFactRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsFactRecord{}, err
	}
	var record AnalyticsFactRecord
	if record.Revision, err = d.u64(); err != nil {
		return record, err
	}
	if record.UID, err = d.u32(); err != nil {
		return record, err
	}
	if record.GID, err = d.u32(); err != nil {
		return record, err
	}
	known, err := d.u32()
	if err != nil || known&^uint32(knownFieldMask) != 0 {
		return record, fmt.Errorf("%w: invalid analytics known-field mask", ErrMalformed)
	}
	record.Known = uint16(known)
	if record.CreatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.LogicalSize, err = d.u64(); err != nil {
		return record, err
	}
	calendarYear, err := d.u32()
	if err != nil {
		return record, err
	}
	record.CalendarYear = int32(calendarYear)
	if record.CalendarMonth, err = d.u8(); err != nil {
		return record, err
	}
	isoYear, err := d.u32()
	if err != nil {
		return record, err
	}
	record.ISOYear = int32(isoYear)
	if record.Workweek, err = d.u8(); err != nil {
		return record, err
	}
	if record.SizeLog10, err = d.u8(); err != nil {
		return record, err
	}
	values := []*string{&record.SourcePath, &record.SVM, &record.Volume, &record.PathGroup}
	for _, value := range values {
		if *value, err = d.string(); err != nil {
			return record, err
		}
	}
	residency, err := d.u8()
	if err != nil {
		return record, err
	}
	record.Residency = AnalyticsResidency(residency)
	basis, err := d.u8()
	if err != nil {
		return record, err
	}
	record.CreationBasis = AnalyticsCreationBasis(basis)
	if d.at == len(d.data) {
		record.IdentityContinuity = AnalyticsContinuityUnknown
	} else {
		if record.IdentityGeneration, err = d.u64(); err != nil {
			return record, err
		}
		continuity, readErr := d.u8()
		if readErr != nil {
			return record, readErr
		}
		record.IdentityContinuity = AnalyticsIdentityContinuity(continuity)
	}
	if !validAnalyticsFact(record) {
		return AnalyticsFactRecord{}, fmt.Errorf("%w: invalid analytics fact", ErrMalformed)
	}
	return record, d.done()
}

func validAnalyticsFact(record AnalyticsFactRecord) bool {
	if record.Revision == 0 || record.Known & ^knownFieldMask != 0 || record.Residency < AnalyticsLive || record.Residency > AnalyticsExpired || record.CreationBasis < AnalyticsCTime || record.CreationBasis > AnalyticsFirstSeen || record.IdentityContinuity > AnalyticsContinuitySourceGeneration || record.CalendarMonth > 12 || record.Workweek > 53 || record.SizeLog10 > 19 {
		return false
	}
	if record.IdentityContinuity == AnalyticsContinuitySourceGeneration && record.IdentityGeneration == 0 {
		return false
	}
	if record.CreationBasis == AnalyticsTimeUnknown {
		return record.CreatedAt == 0 && record.CalendarYear == 0 && record.CalendarMonth == 0 && record.ISOYear == 0 && record.Workweek == 0
	}
	if (record.CreationBasis == AnalyticsCTime && record.Known&KnownCTime == 0) || (record.CreationBasis == AnalyticsMTime && record.Known&KnownMTime == 0) {
		return false
	}
	return record.CreatedAt != 0 && record.CalendarYear != 0 && record.CalendarMonth >= 1 && record.ISOYear != 0 && record.Workweek >= 1
}

type AnalyticsMetadataRecord struct {
	Enabled      bool
	Generation   uint64
	Facts        uint64
	BuiltAt      int64
	CacheEntries uint64
	ConfigJSON   string
}

func (record AnalyticsMetadataRecord) MarshalBinary() ([]byte, error) {
	if record.Enabled && (record.Generation == 0 || record.BuiltAt == 0) {
		return nil, fmt.Errorf("%w: invalid analytics metadata", ErrMalformed)
	}
	e := newEncoder()
	e.bool(record.Enabled)
	e.u64(record.Generation)
	e.u64(record.Facts)
	e.i64(record.BuiltAt)
	e.u64(record.CacheEntries)
	if err := e.string(record.ConfigJSON); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalAnalyticsMetadataRecord(data []byte) (AnalyticsMetadataRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return AnalyticsMetadataRecord{}, err
	}
	var record AnalyticsMetadataRecord
	if record.Enabled, err = d.bool(); err != nil {
		return record, err
	}
	if record.Generation, err = d.u64(); err != nil {
		return record, err
	}
	if record.Facts, err = d.u64(); err != nil {
		return record, err
	}
	if record.BuiltAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.CacheEntries, err = d.u64(); err != nil {
		return record, err
	}
	if record.ConfigJSON, err = d.string(); err != nil {
		return record, err
	}
	if record.Enabled && (record.Generation == 0 || record.BuiltAt == 0) {
		return AnalyticsMetadataRecord{}, fmt.Errorf("%w: invalid analytics metadata", ErrMalformed)
	}
	return record, d.done()
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
