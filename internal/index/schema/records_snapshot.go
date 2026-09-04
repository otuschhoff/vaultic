package schema

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
)

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
