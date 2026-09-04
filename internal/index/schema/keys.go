// Package schema defines the versioned binary key and value model stored by
// vaulticdb. It has no dependency on SlateDB or the daemon transport.
package schema

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
)

const Version byte = 0

const MaxPathIndexPathBytes = 4096

var ErrMalformed = errors.New("malformed slatedb schema record")

var ErrPlacementObsolete = errors.New("placement request is obsolete")

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
	KeyPlacementRequest
	KeyRepackLineage
	KeyPromotionEligibility
	KeyAnalyticsFact
	KeyAnalyticsCache
	KeyAnalyticsMetadata
	KeyAnalyticsBuildCheckpoint
	KeyAnalyticsDictionary
	KeyAnalyticsFactSegment
	KeyAnalyticsSegmentMetadata
	KeyAnalyticsDimensionIndex
	KeyAnalyticsResidency
	KeyAnalyticsDelta
	KeyAnalyticsWatermark
	KeyAnalyticsManifest
	KeyAnalyticsQueryResult
	KeyAnalyticsQueryHeat
	KeyAnalyticsQueryView
	KeyAnalyticsQueryJob
	KeyGrowthTime
	KeyGrowthPath
	KeyUserSummary
	KeyGroupSummary
	KeyUserChurn
	KeyUserInode
	KeyUserBlob
	KeyAuthoritativeCrawlProof
	KeyAuthoritativeSourceBinding
	KeyAnalyticsDerivedMarker
	KeyUserBlobContribution
	KeyUserStats
	KeyGroupStats
	KeyUIDExclusionPolicy
	KeyDeletionCertificate
	KeyVerificationState
	KeyVerificationEvent
	keyKindCount
)

type AnalyticsDictionaryKind byte

const (
	AnalyticsDictionarySVM AnalyticsDictionaryKind = iota + 1
	AnalyticsDictionaryVolume
	AnalyticsDictionaryPathGroup
)

type AnalyticsDimension byte

const (
	AnalyticsDimensionUID AnalyticsDimension = iota + 1
	AnalyticsDimensionGID
	AnalyticsDimensionCalendarYear
	AnalyticsDimensionCalendarMonth
	AnalyticsDimensionISOYear
	AnalyticsDimensionWorkweek
	AnalyticsDimensionSVM
	AnalyticsDimensionVolume
	AnalyticsDimensionPathGroup
	AnalyticsDimensionSizeLog10
	AnalyticsDimensionCreationBasis
	AnalyticsDimensionIdentityContinuity
	AnalyticsDimensionResidency
)

type AnalyticsGranularity byte

const (
	AnalyticsGranularityWeek AnalyticsGranularity = iota + 1
	AnalyticsGranularityMonth
	AnalyticsGranularityYear
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
	Kind           KeyKind
	ID             ID
	SecondID       ID
	FSID           uint32
	Inode          uint64
	Revision       uint64
	Segment        uint32
	Aggregate      AggregateKind
	Tier           PackTier
	GCTarget       GCTarget
	EventTime      uint64
	Granularity    HistoryGranularity
	Backend        uint64
	PackType       PackType
	DeleteAfter    int64
	Path           string
	Generation     uint64
	ViewGeneration uint64
	Commit         uint64
	Ordinal        uint32
	Dictionary     AnalyticsDictionaryKind
	Dimension      AnalyticsDimension
	Value          uint64
	Epoch          uint64
	UID            uint32
	GID            uint32
	Timestamp      int64
	Residency      AnalyticsResidency
}

func BlobKey(id ID) []byte { return idKey("b:", id) }

func PackKey(id ID) []byte { return idKey("p:", id) }

func SnapshotKey(id ID) []byte { return idKey("s:", id) }

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

func ImportCheckpointKey(index ID) []byte { return idKey("meta:import-index:", index) }

func SnapshotImportCheckpointKey(snapshot ID) []byte { return idKey("meta:import-snapshot:", snapshot) }

func ExportCheckpointKey(snapshot ID) []byte { return idKey("meta:export-snapshot:", snapshot) }

func ExportIndexCheckpointKey(index ID) []byte { return idKey("meta:export-index:", index) }

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

func VerificationStateKey(pack ID, backend uint64) []byte {
	key := make([]byte, 3+32+8)
	copy(key, "vr:")
	copy(key[3:], pack[:])
	binary.BigEndian.PutUint64(key[35:], backend)
	return key
}

func VerificationStatePrefix() []byte { return []byte("vr:") }

func VerificationEventKey(unixSeconds, sequence uint64, pack ID, backend uint64) []byte {
	key := make([]byte, 3+8+8+32+8)
	copy(key, "ve:")
	binary.BigEndian.PutUint64(key[3:], unixSeconds)
	binary.BigEndian.PutUint64(key[11:], sequence)
	copy(key[19:], pack[:])
	binary.BigEndian.PutUint64(key[51:], backend)
	return key
}

func VerificationEventPrefix() []byte { return []byte("ve:") }

func UIDExclusionPolicyKey(uid uint32) []byte {
	key := make([]byte, len("u:policy:blocklist:")+4)
	copy(key, "u:policy:blocklist:")
	binary.BigEndian.PutUint32(key[len("u:policy:blocklist:"):], uid)
	return key
}

func UIDExclusionPolicyPrefix() []byte { return []byte("u:policy:blocklist:") }

func DeletionCertificateKey(uid uint32, unixSeconds uint64, runID ID) []byte {
	key := make([]byte, 3+4+8+32)
	copy(key, "dc:")
	binary.BigEndian.PutUint32(key[3:], uid)
	binary.BigEndian.PutUint64(key[7:], unixSeconds)
	copy(key[15:], runID[:])
	return key
}

func DeletionCertificatePrefix(uid uint32) []byte {
	key := make([]byte, 3+4)
	copy(key, "dc:")
	binary.BigEndian.PutUint32(key[3:], uid)
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

func PlacementRequestKey(deadlineUnixSeconds uint64, pack ID) []byte {
	key := make([]byte, 3+8+1+32)
	copy(key, "rq:")
	binary.BigEndian.PutUint64(key[3:], deadlineUnixSeconds)
	key[11] = ':'
	copy(key[12:], pack[:])
	return key
}

func PlacementRequestPrefix() []byte { return []byte("rq:") }

func RepackLineageKey(source, successor ID) []byte {
	key := make([]byte, 3+32+1+32)
	copy(key, "rl:")
	copy(key[3:], source[:])
	key[35] = ':'
	copy(key[36:], successor[:])
	return key
}

func RepackLineagePrefix(source ID) []byte {
	key := make([]byte, 3+32+1)
	copy(key, "rl:")
	copy(key[3:], source[:])
	key[35] = ':'
	return key
}

func PromotionEligibilityKey(pack ID) []byte { return idKey("pe:", pack) }

func PromotionEligibilityPrefix() []byte { return []byte("pe:") }

func AnalyticsFactKey(fsid uint32, inode uint64) []byte { return inodeKey("an:", fsid, inode) }

func AnalyticsFactPrefix() []byte { return []byte("an:") }

func AnalyticsCacheKey(hash ID) []byte { return idKey("aq:", hash) }

func AnalyticsCachePrefix() []byte { return []byte("aq:") }

func AnalyticsMetadataKey() []byte { return []byte("meta:analytics") }

func AnalyticsBuildCheckpointKey() []byte { return []byte("meta:analytics-build") }

func AnalyticsDictionaryKey(kind AnalyticsDictionaryKind, id uint32) []byte {
	if !validAnalyticsDictionaryKind(kind) || id == 0 {
		return nil
	}
	key := []byte{'a', 'd', ':', byte(kind)}
	return binary.BigEndian.AppendUint32(key, id)
}

func AnalyticsDictionaryPrefix(kind AnalyticsDictionaryKind) []byte {
	if !validAnalyticsDictionaryKind(kind) {
		return nil
	}
	return []byte{'a', 'd', ':', byte(kind)}
}

func AnalyticsFactSegmentKey(segment uint64) []byte { return analyticsUint64Key("af:", segment) }

func AnalyticsFactSegmentPrefix() []byte { return []byte("af:") }

func AnalyticsSegmentMetadataKey(segment uint64) []byte { return analyticsUint64Key("am:", segment) }

func AnalyticsSegmentMetadataPrefix() []byte { return []byte("am:") }

func AnalyticsManifestKey(classificationEpoch uint64) []byte {
	return analyticsUint64Key("ap:", classificationEpoch)
}

func AnalyticsManifestPrefix() []byte { return []byte("ap:") }

func AnalyticsDimensionIndexKey(dimension AnalyticsDimension, value, segment uint64) []byte {
	if !validAnalyticsDimension(dimension) || segment == 0 {
		return nil
	}
	key := make([]byte, 3+1+8+8)
	copy(key, "ai:")
	key[3] = byte(dimension)
	binary.BigEndian.PutUint64(key[4:12], value)
	binary.BigEndian.PutUint64(key[12:], segment)
	return key
}

func AnalyticsDimensionIndexPrefix(dimension AnalyticsDimension, value uint64) []byte {
	if !validAnalyticsDimension(dimension) {
		return nil
	}
	key := make([]byte, 3+1+8)
	copy(key, "ai:")
	key[3] = byte(dimension)
	binary.BigEndian.PutUint64(key[4:], value)
	return key
}

func AnalyticsResidencyKey(fsid uint32, inode, generation uint64) []byte {
	if generation == 0 {
		return nil
	}
	return revisionKey("ar:", fsid, inode, generation)
}

func AnalyticsResidencyPrefix(fsid uint32, inode uint64) []byte {
	return inodeKey("ar:", fsid, inode)
}

func AnalyticsDerivedKey(generation uint64, key []byte) []byte {
	if generation == 0 || !isAnalyticsDerivedKey(key) {
		return nil
	}
	result := make([]byte, 4+8+1+len(key))
	copy(result, "av1:")
	binary.BigEndian.PutUint64(result[4:12], generation)
	result[12] = ':'
	copy(result[13:], key)
	return result
}

func AnalyticsDerivedPrefix(generation uint64, prefix []byte) []byte {
	if generation == 0 {
		return nil
	}
	result := make([]byte, 4+8+1+len(prefix))
	copy(result, "av1:")
	binary.BigEndian.PutUint64(result[4:12], generation)
	result[12] = ':'
	copy(result[13:], prefix)
	return result
}

func AnalyticsDerivedGenerationPrefix(generation uint64) []byte {
	return AnalyticsDerivedPrefix(generation, nil)
}

func AnalyticsDerivedGenerationMarkerKey(generation uint64) []byte {
	if generation == 0 {
		return nil
	}
	return AnalyticsDerivedPrefix(generation, []byte("complete"))
}

func AnalyticsDeltaKey(commit uint64, ordinal uint32) []byte {
	if commit == 0 {
		return nil
	}
	key := make([]byte, 3+8+4)
	copy(key, "ae:")
	binary.BigEndian.PutUint64(key[3:11], commit)
	binary.BigEndian.PutUint32(key[11:], ordinal)
	return key
}
