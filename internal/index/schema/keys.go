// Package schema defines the versioned binary key and value model stored by
// vaulticdb. It has no dependency on SlateDB or the daemon transport.
package schema

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const Version byte = 0

const MaxPathIndexPathBytes = 4096

var ErrMalformed = errors.New("malformed slatedb schema record")

type ID [32]byte

type KeyKind byte

const (
	KeyBlob KeyKind = iota + 1
	KeyPack
	KeyPackAggregate
	KeyCurrentInode
	KeyInodeRevision
	KeyCurrentDirectory
	KeyDirectoryRevision
	KeySnapshot
	KeyContentManifest
	KeyReverseManifest
	KeyReverseInode
	KeyReferenceCount
	KeyGarbageCollection
	KeyCrawlDebt
	KeyImportCheckpoint
	KeySnapshotImportCheckpoint
	KeyNextRevision
	KeyHardlinkRefs
	KeyExportCheckpoint
	KeyExportIndexCheckpoint
	KeyNextExportSequence
	KeyTierAggregate
	KeyPackHistory
	KeyPackHistoryBucket
	KeyNextEventSequence
	KeyHistoryRawFloor
	KeyHistoryEnabledAt
	KeyPackPlacement
	KeyBackendPack
	KeyPlacementDeleteQueue
	KeySnapshotCommit
	KeyPathVersion
)

// HistoryGranularity names a rollup bucket width. The values are part of the
// key, so they must stay stable.
type HistoryGranularity byte

const (
	GranularityHourly HistoryGranularity = iota + 1
	GranularityDaily
	GranularityMonthly
)

var historyGranularityNames = map[HistoryGranularity]string{
	GranularityHourly: "hour", GranularityDaily: "day", GranularityMonthly: "month",
}

func (granularity HistoryGranularity) String() string {
	if name, ok := historyGranularityNames[granularity]; ok {
		return name
	}
	return "unknown"
}

func HistoryGranularities() []HistoryGranularity {
	return []HistoryGranularity{GranularityHourly, GranularityDaily, GranularityMonthly}
}

func validHistoryGranularity(value HistoryGranularity) bool {
	return value >= GranularityHourly && value <= GranularityMonthly
}

type AggregateKind byte

const (
	AggregateData AggregateKind = iota + 1
	AggregateTree
	AggregateMixed
	AggregateUnknown
	AggregateAll
)

type GCTarget byte

const (
	GCBlob GCTarget = iota + 1
	GCPack
)

type ParsedKey struct {
	Kind        KeyKind
	ID          ID
	SecondID    ID
	FSID        uint32
	Inode       uint64
	Revision    uint64
	Segment     uint32
	Aggregate   AggregateKind
	Tier        PackTier
	GCTarget    GCTarget
	EventTime   uint64
	Granularity HistoryGranularity
	Backend     uint64
	PackType    PackType
	DeleteAfter int64
	Path        string
}

func BlobKey(id ID) []byte           { return idKey("b:", id) }
func PackKey(id ID) []byte           { return idKey("p:", id) }
func SnapshotKey(id ID) []byte       { return idKey("s:", id) }
func ReferenceCountKey(id ID) []byte { return idKey("rc:", id) }
func SnapshotCommitKey(commitSequence uint64, snapshot ID) []byte {
	key := make([]byte, 3+8+1+32)
	copy(key, "sc:")
	binary.BigEndian.PutUint64(key[3:], commitSequence)
	key[11] = ':'
	copy(key[12:], snapshot[:])
	return key
}
func SnapshotCommitPrefix() []byte { return []byte("sc:") }

func PathVersionKey(fsid uint32, path string, revision uint64) []byte {
	path = strings.TrimPrefix(path, "/")
	if path == "" || len(path) > MaxPathIndexPathBytes || strings.IndexByte(path, 0) >= 0 {
		return nil
	}
	key := make([]byte, 3+4+len(path)+1+8)
	copy(key, "pv:")
	binary.BigEndian.PutUint32(key[3:], fsid)
	copy(key[7:], path)
	key[7+len(path)] = 0
	binary.BigEndian.PutUint64(key[8+len(path):], revision)
	return key
}

func PathVersionPrefix(fsid uint32, path string) []byte {
	path = strings.TrimPrefix(path, "/")
	if path == "" || len(path) > MaxPathIndexPathBytes || strings.IndexByte(path, 0) >= 0 {
		return nil
	}
	key := make([]byte, 3+4+len(path)+1)
	copy(key, "pv:")
	binary.BigEndian.PutUint32(key[3:], fsid)
	copy(key[7:], path)
	key[7+len(path)] = 0
	return key
}

func PathVersionSubtreePrefix(fsid uint32, path string) []byte {
	path = strings.TrimPrefix(path, "/")
	if path == "" || len(path) >= MaxPathIndexPathBytes || strings.IndexByte(path, 0) >= 0 {
		return nil
	}
	key := make([]byte, 3+4+len(path)+1)
	copy(key, "pv:")
	binary.BigEndian.PutUint32(key[3:], fsid)
	copy(key[7:], path)
	key[7+len(path)] = '/'
	return key
}

func PathOverflowKey(fsid uint32, path string, revision uint64) []byte {
	sum := sha256.Sum256([]byte(path))
	return PathVersionKey(fsid, "overflow/"+hex.EncodeToString(sum[:16]), revision)
}
func CurrentInodeKey(fsid uint32, inode uint64) []byte {
	return inodeKey("i:", fsid, inode)
}
func CurrentDirectoryKey(fsid uint32, inode uint64) []byte {
	return inodeKey("d:", fsid, inode)
}
func InodeRevisionKey(fsid uint32, inode, revision uint64) []byte {
	return revisionKey("iv:", fsid, inode, revision)
}
func InodeRevisionPrefix(fsid uint32, inode uint64) []byte {
	key := make([]byte, 3+4+8)
	copy(key, "iv:")
	binary.BigEndian.PutUint32(key[3:], fsid)
	binary.BigEndian.PutUint64(key[7:], inode)
	return key
}
func DirectoryRevisionKey(fsid uint32, inode, revision uint64) []byte {
	return revisionKey("dv:", fsid, inode, revision)
}
func HardlinkRefsKey(fsid uint32, inode, revision uint64) []byte {
	return revisionKey("hr:", fsid, inode, revision)
}
func ContentManifestKey(id ID, segment uint32) []byte {
	key := make([]byte, 3+32+4)
	copy(key, "cm:")
	copy(key[3:], id[:])
	binary.BigEndian.PutUint32(key[35:], segment)
	return key
}
func ReverseManifestKey(blob, manifest ID) []byte {
	key := make([]byte, 3+64)
	copy(key, "rm:")
	copy(key[3:], blob[:])
	copy(key[35:], manifest[:])
	return key
}
func ReverseInodeKey(blob ID, fsid uint32, inode uint64) []byte {
	key := make([]byte, 3+32+4+8)
	copy(key, "ri:")
	copy(key[3:], blob[:])
	binary.BigEndian.PutUint32(key[35:], fsid)
	binary.BigEndian.PutUint64(key[39:], inode)
	return key
}
func GarbageCollectionKey(target GCTarget, id ID) []byte {
	var prefix string
	switch target {
	case GCBlob:
		prefix = "gc:b:"
	case GCPack:
		prefix = "gc:p:"
	default:
		return nil
	}
	return idKey(prefix, id)
}
func CrawlDebtKey(snapshot, work ID) []byte {
	key := make([]byte, 2+64)
	copy(key, "q:")
	copy(key[2:], snapshot[:])
	copy(key[34:], work[:])
	return key
}
func ImportCheckpointKey(index ID) []byte            { return idKey("meta:import-index:", index) }
func SnapshotImportCheckpointKey(snapshot ID) []byte { return idKey("meta:import-snapshot:", snapshot) }
func ExportCheckpointKey(snapshot ID) []byte         { return idKey("meta:export-snapshot:", snapshot) }
func ExportIndexCheckpointKey(index ID) []byte       { return idKey("meta:export-index:", index) }
func PackAggregateKey(kind AggregateKind) []byte {
	name := map[AggregateKind]string{
		AggregateData: "data", AggregateTree: "tree", AggregateMixed: "mixed",
		AggregateUnknown: "unknown", AggregateAll: "all",
	}[kind]
	return []byte("a:pack:" + name)
}

// tierAggregateNames is the single source of truth for the a:tier: namespace.
// The type dimension (a:pack:*) already carries the repository total, so the
// tier dimension has no "all" record.
var tierAggregateNames = map[PackTier]string{
	TierUnknown: "unknown", TierHot: "hot", TierCold: "cold",
	TierMirrored: "mirrored", TierSingle: "single",
}

func (tier PackTier) String() string {
	if name, ok := tierAggregateNames[tier]; ok {
		return name
	}
	return "unknown"
}

func TierAggregateKey(tier PackTier) []byte {
	name, ok := tierAggregateNames[tier]
	if !ok {
		return nil
	}
	return []byte("a:tier:" + name)
}

// TierAggregateKinds returns every tier in a deterministic order so callers
// iterate, rebuild, and compare aggregates identically.
func TierAggregateKinds() []PackTier {
	return []PackTier{TierUnknown, TierHot, TierCold, TierMirrored, TierSingle}
}

// PackHistoryKey orders events by time, then by a globally monotonic sequence,
// then by pack. The sequence is what makes the key unique when two writers
// record events for the same pack within the same second, and what gives the
// log a total order independent of clock resolution.
func PackHistoryKey(unixSeconds uint64, sequence uint64, pack ID) []byte {
	key := make([]byte, 3+8+8+32)
	copy(key, "ph:")
	binary.BigEndian.PutUint64(key[3:], unixSeconds)
	binary.BigEndian.PutUint64(key[11:], sequence)
	copy(key[19:], pack[:])
	return key
}

// PackHistoryTimePrefix bounds a scan to events at or after a point in time.
func PackHistoryTimePrefix(unixSeconds uint64) []byte {
	key := make([]byte, 3+8)
	copy(key, "ph:")
	binary.BigEndian.PutUint64(key[3:], unixSeconds)
	return key
}

// PackHistoryBucketKey identifies one rollup bucket. Backend is zero until the
// placement model of Phase 12 supplies one; the field exists now so the key
// does not change when it does.
func PackHistoryBucketKey(granularity HistoryGranularity, bucketStart uint64, backend uint64, packType PackType) []byte {
	if !validHistoryGranularity(granularity) {
		return nil
	}
	key := make([]byte, 3+1+8+8+1)
	copy(key, "pb:")
	key[3] = byte(granularity)
	binary.BigEndian.PutUint64(key[4:], bucketStart)
	binary.BigEndian.PutUint64(key[12:], backend)
	key[20] = byte(packType)
	return key
}

// PackHistoryBucketPrefix bounds a scan to one granularity.
func PackHistoryBucketPrefix(granularity HistoryGranularity) []byte {
	return []byte{'p', 'b', ':', byte(granularity)}
}

// PackPlacementKey identifies one physical placement of a pack on one
// configured backend.
func PackPlacementKey(pack ID, backend uint64) []byte {
	key := make([]byte, 3+32+1+8)
	copy(key, "pl:")
	copy(key[3:], pack[:])
	key[35] = ':'
	binary.BigEndian.PutUint64(key[36:], backend)
	return key
}

func PackPlacementPrefix(pack ID) []byte {
	key := make([]byte, 3+32+1)
	copy(key, "pl:")
	copy(key[3:], pack[:])
	key[35] = ':'
	return key
}

func BackendPackKey(backend uint64, pack ID) []byte {
	key := make([]byte, 3+8+1+32)
	copy(key, "bp:")
	binary.BigEndian.PutUint64(key[3:], backend)
	key[11] = ':'
	copy(key[12:], pack[:])
	return key
}

func BackendPackPrefix(backend uint64) []byte {
	key := make([]byte, 3+8+1)
	copy(key, "bp:")
	binary.BigEndian.PutUint64(key[3:], backend)
	key[11] = ':'
	return key
}

func PlacementDeleteQueueKey(deleteAfter int64, pack ID, backend uint64) []byte {
	key := make([]byte, 3+8+1+32+1+8)
	copy(key, "dq:")
	binary.BigEndian.PutUint64(key[3:], uint64(deleteAfter))
	key[11] = ':'
	copy(key[12:], pack[:])
	key[44] = ':'
	binary.BigEndian.PutUint64(key[45:], backend)
	return key
}

func PlacementDeleteQueuePrefix(before int64) []byte {
	key := make([]byte, 3+8)
	copy(key, "dq:")
	binary.BigEndian.PutUint64(key[3:], uint64(before))
	return key
}

func NextEventSequenceKey() []byte { return []byte("meta:next-event-seq") }

// HistoryRawFloorKey records the earliest raw event time still retained, so a
// later query can tell an intentionally truncated range from a gap.
func HistoryRawFloorKey() []byte { return []byte("meta:history-raw-floor") }

// HistoryEnabledAtKey records when history collection began, so buckets
// covering earlier periods are reported as reconstructed rather than complete.
func HistoryEnabledAtKey() []byte   { return []byte("meta:history-enabled-at") }
func NextRevisionKey() []byte       { return []byte("meta:next-revision-seq") }
func NextExportSequenceKey() []byte { return []byte("meta:next-export-seq") }

func ParseKey(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 34 && string(key[:2]) == "b:":
		parsed.Kind = KeyBlob
		copy(parsed.ID[:], key[2:])
	case len(key) == 34 && string(key[:2]) == "p:":
		parsed.Kind = KeyPack
		copy(parsed.ID[:], key[2:])
	case len(key) == 34 && string(key[:2]) == "s:":
		parsed.Kind = KeySnapshot
		copy(parsed.ID[:], key[2:])
	case len(key) == 44 && string(key[:3]) == "sc:" && key[11] == ':':
		parsed.Kind = KeySnapshotCommit
		parsed.Revision = binary.BigEndian.Uint64(key[3:11])
		copy(parsed.ID[:], key[12:])
	case len(key) > 3+4+1+8 && string(key[:3]) == "pv:":
		terminator := -1
		for offset := 7; offset < len(key)-8; offset++ {
			if key[offset] == 0 {
				terminator = offset
				break
			}
		}
		if terminator <= 7 || terminator != len(key)-9 {
			return ParsedKey{}, fmt.Errorf("%w: invalid path-version key", ErrMalformed)
		}
		parsed.Kind = KeyPathVersion
		parsed.FSID = binary.BigEndian.Uint32(key[3:7])
		parsed.Path = string(key[7:terminator])
		parsed.Revision = binary.BigEndian.Uint64(key[terminator+1:])
		if parsed.Revision == 0 || len(parsed.Path) > MaxPathIndexPathBytes {
			return ParsedKey{}, fmt.Errorf("%w: invalid path-version key", ErrMalformed)
		}
	case len(key) == 35 && string(key[:3]) == "rc:":
		parsed.Kind = KeyReferenceCount
		copy(parsed.ID[:], key[3:])
	case len(key) == 14 && string(key[:2]) == "i:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyCurrentInode, binary.BigEndian.Uint32(key[2:6]), binary.BigEndian.Uint64(key[6:])
	case len(key) == 14 && string(key[:2]) == "d:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyCurrentDirectory, binary.BigEndian.Uint32(key[2:6]), binary.BigEndian.Uint64(key[6:])
	case len(key) == 23 && string(key[:3]) == "iv:":
		parsed.Kind, parsed.FSID, parsed.Inode, parsed.Revision = KeyInodeRevision, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
	case len(key) == 23 && string(key[:3]) == "dv:":
		parsed.Kind, parsed.FSID, parsed.Inode, parsed.Revision = KeyDirectoryRevision, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
	case len(key) == 23 && string(key[:3]) == "hr:":
		parsed.Kind, parsed.FSID, parsed.Inode, parsed.Revision = KeyHardlinkRefs, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
	case len(key) == 39 && string(key[:3]) == "cm:":
		parsed.Kind, parsed.Segment = KeyContentManifest, binary.BigEndian.Uint32(key[35:])
		copy(parsed.ID[:], key[3:35])
	case len(key) == 67 && string(key[:3]) == "rm:":
		parsed.Kind = KeyReverseManifest
		copy(parsed.ID[:], key[3:35])
		copy(parsed.SecondID[:], key[35:])
	case len(key) == 47 && string(key[:3]) == "ri:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyReverseInode, binary.BigEndian.Uint32(key[35:39]), binary.BigEndian.Uint64(key[39:])
		copy(parsed.ID[:], key[3:35])
	case len(key) == 66 && string(key[:2]) == "q:":
		parsed.Kind = KeyCrawlDebt
		copy(parsed.ID[:], key[2:34])
		copy(parsed.SecondID[:], key[34:])
	case len(key) == 50 && string(key[:18]) == "meta:import-index:":
		parsed.Kind = KeyImportCheckpoint
		copy(parsed.ID[:], key[18:])
	case len(key) == 53 && string(key[:21]) == "meta:import-snapshot:":
		parsed.Kind = KeySnapshotImportCheckpoint
		copy(parsed.ID[:], key[21:])
	case len(key) == 53 && string(key[:21]) == "meta:export-snapshot:":
		parsed.Kind = KeyExportCheckpoint
		copy(parsed.ID[:], key[21:])
	case len(key) == 50 && string(key[:18]) == "meta:export-index:":
		parsed.Kind = KeyExportIndexCheckpoint
		copy(parsed.ID[:], key[18:])
	case len(key) == 37 && (string(key[:5]) == "gc:b:" || string(key[:5]) == "gc:p:"):
		parsed.Kind, parsed.GCTarget = KeyGarbageCollection, GCBlob
		if key[3] == 'p' {
			parsed.GCTarget = GCPack
		}
		copy(parsed.ID[:], key[5:])
	case string(key) == "meta:next-revision-seq":
		parsed.Kind = KeyNextRevision
	case string(key) == "meta:next-export-seq":
		parsed.Kind = KeyNextExportSequence
	case string(key) == "meta:next-event-seq":
		parsed.Kind = KeyNextEventSequence
	case string(key) == "meta:history-raw-floor":
		parsed.Kind = KeyHistoryRawFloor
	case string(key) == "meta:history-enabled-at":
		parsed.Kind = KeyHistoryEnabledAt
	case len(key) == 51 && string(key[:3]) == "ph:":
		parsed.Kind = KeyPackHistory
		parsed.EventTime = binary.BigEndian.Uint64(key[3:11])
		parsed.Revision = binary.BigEndian.Uint64(key[11:19])
		copy(parsed.ID[:], key[19:])
	case len(key) == 21 && string(key[:3]) == "pb:":
		parsed.Kind = KeyPackHistoryBucket
		parsed.Granularity = HistoryGranularity(key[3])
		parsed.EventTime = binary.BigEndian.Uint64(key[4:12])
		parsed.Backend = binary.BigEndian.Uint64(key[12:20])
		parsed.PackType = PackType(key[20])
		if !validHistoryGranularity(parsed.Granularity) || !validPackType(parsed.PackType) {
			return ParsedKey{}, fmt.Errorf("%w: invalid history bucket key", ErrMalformed)
		}
	case len(key) == 44 && string(key[:3]) == "pl:" && key[35] == ':':
		parsed.Kind = KeyPackPlacement
		copy(parsed.ID[:], key[3:35])
		parsed.Backend = binary.BigEndian.Uint64(key[36:])
	case len(key) == 44 && string(key[:3]) == "bp:" && key[11] == ':':
		parsed.Kind = KeyBackendPack
		parsed.Backend = binary.BigEndian.Uint64(key[3:11])
		copy(parsed.ID[:], key[12:])
	case len(key) == 53 && string(key[:3]) == "dq:" && key[11] == ':' && key[44] == ':':
		parsed.Kind = KeyPlacementDeleteQueue
		parsed.DeleteAfter = int64(binary.BigEndian.Uint64(key[3:11]))
		copy(parsed.ID[:], key[12:44])
		parsed.Backend = binary.BigEndian.Uint64(key[45:])
	default:
		if kind, ok := parseAggregate(key); ok {
			parsed.Kind, parsed.Aggregate = KeyPackAggregate, kind
		} else if tier, ok := parseTierAggregate(key); ok {
			parsed.Kind, parsed.Tier = KeyTierAggregate, tier
		} else {
			return ParsedKey{}, fmt.Errorf("%w: unknown or incorrectly sized key", ErrMalformed)
		}
	}
	return parsed, nil
}

func idKey(prefix string, id ID) []byte {
	key := make([]byte, len(prefix)+len(id))
	copy(key, prefix)
	copy(key[len(prefix):], id[:])
	return key
}
func inodeKey(prefix string, fsid uint32, inode uint64) []byte {
	key := make([]byte, len(prefix)+12)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], fsid)
	binary.BigEndian.PutUint64(key[len(prefix)+4:], inode)
	return key
}
func revisionKey(prefix string, fsid uint32, inode, revision uint64) []byte {
	key := make([]byte, len(prefix)+20)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], fsid)
	binary.BigEndian.PutUint64(key[len(prefix)+4:], inode)
	binary.BigEndian.PutUint64(key[len(prefix)+12:], revision)
	return key
}
func parseAggregate(key []byte) (AggregateKind, bool) {
	for kind, name := range map[AggregateKind]string{
		AggregateData: "data", AggregateTree: "tree", AggregateMixed: "mixed",
		AggregateUnknown: "unknown", AggregateAll: "all",
	} {
		if string(key) == "a:pack:"+name {
			return kind, true
		}
	}
	return 0, false
}

func parseTierAggregate(key []byte) (PackTier, bool) {
	for tier, name := range tierAggregateNames {
		if string(key) == "a:tier:"+name {
			return tier, true
		}
	}
	return 0, false
}
