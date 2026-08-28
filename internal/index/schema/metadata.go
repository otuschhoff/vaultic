package schema

import "fmt"

type ReferenceState byte

const (
	ReferenceCurrent ReferenceState = iota + 1
	ReferenceHistorical
	ReferenceUnresolved
)

type ReverseManifestRecord struct {
	Segment uint32
	State   ReferenceState
}

func (record ReverseManifestRecord) MarshalBinary() ([]byte, error) {
	if !validReferenceState(record.State) {
		return nil, fmt.Errorf("%w: invalid reference state", ErrMalformed)
	}
	e := newEncoder()
	e.u32(record.Segment)
	e.u8(byte(record.State))
	return e.finish()
}

func UnmarshalReverseManifestRecord(data []byte) (ReverseManifestRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return ReverseManifestRecord{}, err
	}
	segment, err := d.u32()
	if err != nil {
		return ReverseManifestRecord{}, err
	}
	state, err := d.u8()
	if err != nil || !validReferenceState(ReferenceState(state)) {
		return ReverseManifestRecord{}, fmt.Errorf("%w: invalid reference state", ErrMalformed)
	}
	return ReverseManifestRecord{Segment: segment, State: ReferenceState(state)}, d.done()
}

type ReverseInodeRecord struct {
	LatestRevision uint64
	State          ReferenceState
}

func (record ReverseInodeRecord) MarshalBinary() ([]byte, error) {
	if record.LatestRevision == 0 || !validReferenceState(record.State) {
		return nil, fmt.Errorf("%w: invalid reverse inode record", ErrMalformed)
	}
	e := newEncoder()
	e.u64(record.LatestRevision)
	e.u8(byte(record.State))
	return e.finish()
}

func UnmarshalReverseInodeRecord(data []byte) (ReverseInodeRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return ReverseInodeRecord{}, err
	}
	revision, err := d.u64()
	if err != nil {
		return ReverseInodeRecord{}, err
	}
	state, err := d.u8()
	if err != nil || revision == 0 || !validReferenceState(ReferenceState(state)) {
		return ReverseInodeRecord{}, fmt.Errorf("%w: invalid reverse inode record", ErrMalformed)
	}
	return ReverseInodeRecord{LatestRevision: revision, State: ReferenceState(state)}, d.done()
}

func validReferenceState(state ReferenceState) bool {
	return state >= ReferenceCurrent && state <= ReferenceUnresolved
}

type ReferenceCountRecord struct {
	TotalReferences, DistinctInodes, DistinctRevisions    uint64
	DistinctManifests, ReachableSnapshots, UpdateSequence uint64
}

func (record ReferenceCountRecord) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	for _, value := range []uint64{record.TotalReferences, record.DistinctInodes, record.DistinctRevisions, record.DistinctManifests, record.ReachableSnapshots, record.UpdateSequence} {
		e.u64(value)
	}
	return e.finish()
}

func UnmarshalReferenceCountRecord(data []byte) (ReferenceCountRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return ReferenceCountRecord{}, err
	}
	values := make([]uint64, 6)
	for index := range values {
		if values[index], err = d.u64(); err != nil {
			return ReferenceCountRecord{}, err
		}
	}
	return ReferenceCountRecord{
		TotalReferences: values[0], DistinctInodes: values[1], DistinctRevisions: values[2],
		DistinctManifests: values[3], ReachableSnapshots: values[4], UpdateSequence: values[5],
	}, d.done()
}

type GCState byte

const (
	GCCandidate GCState = iota + 1
	GCPendingRevalidation
	GCRevalidated
	GCRejected
)

type GarbageCollectionRecord struct {
	State              GCState
	ObservedCommit     uint64
	DiscoveredUnixNano int64
	RevalidatedCommit  uint64
}

func (record GarbageCollectionRecord) MarshalBinary() ([]byte, error) {
	if record.State < GCCandidate || record.State > GCRejected {
		return nil, fmt.Errorf("%w: invalid garbage-collection state", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.State))
	e.u64(record.ObservedCommit)
	e.i64(record.DiscoveredUnixNano)
	e.u64(record.RevalidatedCommit)
	return e.finish()
}

func UnmarshalGarbageCollectionRecord(data []byte) (GarbageCollectionRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return GarbageCollectionRecord{}, err
	}
	state, err := d.u8()
	if err != nil {
		return GarbageCollectionRecord{}, err
	}
	observed, err := d.u64()
	if err != nil {
		return GarbageCollectionRecord{}, err
	}
	discovered, err := d.i64()
	if err != nil {
		return GarbageCollectionRecord{}, err
	}
	revalidated, err := d.u64()
	if err != nil {
		return GarbageCollectionRecord{}, err
	}
	if GCState(state) < GCCandidate || GCState(state) > GCRejected {
		return GarbageCollectionRecord{}, fmt.Errorf("%w: invalid garbage-collection state", ErrMalformed)
	}
	return GarbageCollectionRecord{State: GCState(state), ObservedCommit: observed, DiscoveredUnixNano: discovered, RevalidatedCommit: revalidated}, d.done()
}

type DebtReason byte

const (
	DebtMissingInode DebtReason = iota + 1
	DebtMissingDirectory
	DebtUnknownFreshness
	DebtMissingContent
	DebtUnavailablePack
)

type DebtStatus byte

const (
	DebtPending DebtStatus = iota + 1
	DebtResolved
	DebtFailed
)

type CrawlDebtRecord struct {
	SourceIndexOrPack   ID
	SourceKnown         bool
	PathOrTree          []byte
	Reason              DebtReason
	RetryCount          uint32
	LastAttemptUnixNano int64
	Status              DebtStatus
	ErrorClass          string
}

func (record CrawlDebtRecord) MarshalBinary() ([]byte, error) {
	if record.Reason < DebtMissingInode || record.Reason > DebtUnavailablePack || record.Status < DebtPending || record.Status > DebtFailed {
		return nil, fmt.Errorf("%w: invalid crawl-debt state", ErrMalformed)
	}
	e := newEncoder()
	e.id(record.SourceIndexOrPack)
	e.bool(record.SourceKnown)
	if err := e.bytes(record.PathOrTree); err != nil {
		return nil, err
	}
	e.u8(byte(record.Reason))
	e.u32(record.RetryCount)
	e.i64(record.LastAttemptUnixNano)
	e.u8(byte(record.Status))
	if err := e.string(record.ErrorClass); err != nil {
		return nil, err
	}
	return e.finish()
}
func UnmarshalCrawlDebtRecord(data []byte) (CrawlDebtRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return CrawlDebtRecord{}, err
	}
	var record CrawlDebtRecord
	if record.SourceIndexOrPack, err = d.id(); err != nil {
		return record, err
	}
	if record.SourceKnown, err = d.bool(); err != nil {
		return record, err
	}
	if record.PathOrTree, err = d.bytes(); err != nil {
		return record, err
	}
	reason, err := d.u8()
	record.Reason = DebtReason(reason)
	if err != nil {
		return record, err
	}
	if record.RetryCount, err = d.u32(); err != nil {
		return record, err
	}
	if record.LastAttemptUnixNano, err = d.i64(); err != nil {
		return record, err
	}
	status, err := d.u8()
	record.Status = DebtStatus(status)
	if err != nil {
		return record, err
	}
	if record.ErrorClass, err = d.string(); err != nil {
		return record, err
	}
	if record.Reason < DebtMissingInode || record.Reason > DebtUnavailablePack || record.Status < DebtPending || record.Status > DebtFailed {
		return CrawlDebtRecord{}, fmt.Errorf("%w: invalid crawl-debt state", ErrMalformed)
	}
	return record, d.done()
}

type ImportCheckpointRecord struct {
	PacksImported uint64
	BlobsImported uint64
	ErrorsSeen    uint64
}

func (record ImportCheckpointRecord) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	e.u64(record.PacksImported)
	e.u64(record.BlobsImported)
	e.u64(record.ErrorsSeen)
	return e.finish()
}

func UnmarshalImportCheckpointRecord(data []byte) (ImportCheckpointRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return ImportCheckpointRecord{}, err
	}
	var record ImportCheckpointRecord
	if record.PacksImported, err = d.u64(); err != nil {
		return record, err
	}
	if record.BlobsImported, err = d.u64(); err != nil {
		return record, err
	}
	if record.ErrorsSeen, err = d.u64(); err != nil {
		return record, err
	}
	return record, d.done()
}

type SnapshotImportCheckpointRecord struct {
	TreesVisited  uint64
	NodesImported uint64
	DebtsCreated  uint64
}

type ExportState byte

const (
	ExportPending ExportState = iota + 1
	ExportComplete
	ExportFailed
)

// ExportCheckpointRecord makes compatibility-projection lag visible. A
// pending record is written before the legacy snapshot object and is completed
// atomically with its authoritative snapshot scope, or failed if that write fails.
type ExportCheckpointRecord struct {
	State          ExportState
	CommitSequence uint64
	Attempts       uint32
	RootKey        []byte
	LastError      string
}

func (record ExportCheckpointRecord) MarshalBinary() ([]byte, error) {
	if record.State < ExportPending || record.State > ExportFailed ||
		(record.State == ExportPending && record.CommitSequence != 0) ||
		(record.State == ExportComplete && (record.CommitSequence == 0 || record.LastError != "")) ||
		(record.State == ExportFailed && (record.CommitSequence != 0 || record.LastError == "")) || !validExportRoot(record.RootKey) {
		return nil, fmt.Errorf("%w: invalid export checkpoint", ErrMalformed)
	}
	e := newEncoder()
	e.u8(byte(record.State))
	e.u64(record.CommitSequence)
	e.u32(record.Attempts)
	if err := e.bytes(record.RootKey); err != nil {
		return nil, err
	}
	if err := e.string(record.LastError); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalExportCheckpointRecord(data []byte) (ExportCheckpointRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return ExportCheckpointRecord{}, err
	}
	state, err := d.u8()
	if err != nil {
		return ExportCheckpointRecord{}, err
	}
	record := ExportCheckpointRecord{State: ExportState(state)}
	if record.CommitSequence, err = d.u64(); err != nil {
		return record, err
	}
	if record.Attempts, err = d.u32(); err != nil {
		return record, err
	}
	if record.RootKey, err = d.bytes(); err != nil {
		return record, err
	}
	if record.LastError, err = d.string(); err != nil {
		return record, err
	}
	if record.State < ExportPending || record.State > ExportFailed ||
		(record.State == ExportPending && record.CommitSequence != 0) ||
		(record.State == ExportComplete && (record.CommitSequence == 0 || record.LastError != "")) ||
		(record.State == ExportFailed && (record.CommitSequence != 0 || record.LastError == "")) || !validExportRoot(record.RootKey) {
		return ExportCheckpointRecord{}, fmt.Errorf("%w: invalid export checkpoint", ErrMalformed)
	}
	return record, d.done()
}

func validExportRoot(key []byte) bool {
	parsed, err := ParseKey(key)
	return err == nil && parsed.Kind == KeyDirectoryRevision && parsed.Revision != 0
}

func (record SnapshotImportCheckpointRecord) MarshalBinary() ([]byte, error) {
	e := newEncoder()
	e.u64(record.TreesVisited)
	e.u64(record.NodesImported)
	e.u64(record.DebtsCreated)
	return e.finish()
}

func UnmarshalSnapshotImportCheckpointRecord(data []byte) (SnapshotImportCheckpointRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return SnapshotImportCheckpointRecord{}, err
	}
	var record SnapshotImportCheckpointRecord
	if record.TreesVisited, err = d.u64(); err != nil {
		return record, err
	}
	if record.NodesImported, err = d.u64(); err != nil {
		return record, err
	}
	if record.DebtsCreated, err = d.u64(); err != nil {
		return record, err
	}
	return record, d.done()
}

func MarshalNextRevision(next uint64) ([]byte, error) {
	if next == 0 {
		return nil, fmt.Errorf("%w: next revision must be non-zero", ErrMalformed)
	}
	return marshalNextRevision(next), nil
}

// ValidateValue verifies that a value is canonical for its schema key.
func ValidateValue(key []byte, value []byte) error {
	parsed, err := ParseKey(key)
	if err != nil {
		return err
	}
	switch parsed.Kind {
	case KeyBlob:
		_, err = UnmarshalBlobRecord(value)
	case KeyPack:
		_, err = UnmarshalPackRecord(value)
	case KeyPackAggregate:
		_, err = UnmarshalPackAggregate(value)
	case KeyCurrentInode, KeyCurrentDirectory:
		var pointer CurrentPointer
		pointer, err = UnmarshalCurrentPointer(value)
		if err == nil {
			target, parseErr := ParseKey(pointer.RecordKey)
			expected := KeyInodeRevision
			if parsed.Kind == KeyCurrentDirectory {
				expected = KeyDirectoryRevision
			}
			if parseErr != nil || target.Kind != expected || target.FSID != parsed.FSID || target.Inode != parsed.Inode {
				err = fmt.Errorf("%w: current pointer key mismatch", ErrMalformed)
			}
		}
	case KeyInodeRevision:
		_, err = UnmarshalInodeRevision(value)
	case KeyDirectoryRevision:
		var directory DirectoryRevision
		directory, err = UnmarshalDirectoryRevision(value)
		if err == nil {
			for _, child := range directory.Children {
				childKey, parseErr := ParseKey(child.MetadataKey)
				if parseErr != nil || (parsed.FSID != 0 && childKey.FSID != parsed.FSID) {
					err = fmt.Errorf("%w: directory child crosses filesystem boundary", ErrMalformed)
					break
				}
			}
		}
	case KeySnapshot:
		_, err = UnmarshalSnapshotRecord(value)
	case KeyContentManifest:
		var manifest ContentManifest
		manifest, err = UnmarshalContentManifest(value)
		if err == nil && manifest.Segment != parsed.Segment {
			err = fmt.Errorf("%w: content manifest segment mismatch", ErrMalformed)
		}
	case KeyReverseManifest:
		_, err = UnmarshalReverseManifestRecord(value)
	case KeyReverseInode:
		_, err = UnmarshalReverseInodeRecord(value)
	case KeyReferenceCount:
		_, err = UnmarshalReferenceCountRecord(value)
	case KeyGarbageCollection:
		_, err = UnmarshalGarbageCollectionRecord(value)
	case KeyCrawlDebt:
		_, err = UnmarshalCrawlDebtRecord(value)
	case KeyImportCheckpoint:
		_, err = UnmarshalImportCheckpointRecord(value)
	case KeySnapshotImportCheckpoint:
		_, err = UnmarshalSnapshotImportCheckpointRecord(value)
	case KeyExportCheckpoint:
		_, err = UnmarshalExportCheckpointRecord(value)
	case KeyHardlinkRefs:
		_, err = UnmarshalHardlinkRefsRecord(value)
	case KeyNextRevision:
		_, err = UnmarshalNextRevision(value)
	default:
		err = fmt.Errorf("%w: unsupported schema key", ErrMalformed)
	}
	return err
}
