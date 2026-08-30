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
func PromotionEligibilityPrefix() []byte     { return []byte("pe:") }

func AnalyticsFactKey(fsid uint32, inode uint64) []byte { return inodeKey("an:", fsid, inode) }
func AnalyticsFactPrefix() []byte                       { return []byte("an:") }
func AnalyticsCacheKey(hash ID) []byte                  { return idKey("aq:", hash) }
func AnalyticsCachePrefix() []byte                      { return []byte("aq:") }
func AnalyticsMetadataKey() []byte                      { return []byte("meta:analytics") }
func AnalyticsBuildCheckpointKey() []byte               { return []byte("meta:analytics-build") }

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

func AnalyticsFactSegmentKey(segment uint64) []byte     { return analyticsUint64Key("af:", segment) }
func AnalyticsFactSegmentPrefix() []byte                { return []byte("af:") }
func AnalyticsSegmentMetadataKey(segment uint64) []byte { return analyticsUint64Key("am:", segment) }
func AnalyticsSegmentMetadataPrefix() []byte            { return []byte("am:") }
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

func AnalyticsDeltaPrefix() []byte { return []byte("ae:") }

func AuthoritativeCrawlProofKey(scope ID, endCommit uint64) []byte {
	if scope == (ID{}) || endCommit == 0 {
		return nil
	}
	key := make([]byte, 4+32+8)
	copy(key, "acp:")
	copy(key[4:36], scope[:])
	binary.BigEndian.PutUint64(key[36:], endCommit)
	return key
}

func AuthoritativeCrawlProofPrefix(scope ID) []byte {
	if scope == (ID{}) {
		return nil
	}
	key := make([]byte, 4+32)
	copy(key, "acp:")
	copy(key[4:], scope[:])
	return key
}

func AuthoritativeSourceBindingKey(scope ID, fsid uint32, inode, generation uint64) []byte {
	if scope == (ID{}) || fsid == 0 || inode == 0 || generation == 0 {
		return nil
	}
	key := make([]byte, 4+32+4+8+8)
	copy(key, "asb:")
	copy(key[4:36], scope[:])
	binary.BigEndian.PutUint32(key[36:40], fsid)
	binary.BigEndian.PutUint64(key[40:48], inode)
	binary.BigEndian.PutUint64(key[48:], generation)
	return key
}

func AuthoritativeSourceBindingPrefix(scope ID) []byte {
	if scope == (ID{}) {
		return nil
	}
	key := make([]byte, 4+32)
	copy(key, "asb:")
	copy(key[4:], scope[:])
	return key
}

func AnalyticsWatermarkKey(classificationEpoch uint64) []byte {
	return analyticsUint64Key("aw:applied:", classificationEpoch)
}

func AnalyticsWatermarkPrefix() []byte { return []byte("aw:applied:") }

func AnalyticsQueryResultKey(hash ID, repositoryGeneration, classificationEpoch, appliedCommit uint64) []byte {
	if repositoryGeneration == 0 || classificationEpoch == 0 {
		return nil
	}
	key := make([]byte, 10+32+8+8+8)
	copy(key, "aq:result:")
	copy(key[10:42], hash[:])
	binary.BigEndian.PutUint64(key[42:50], repositoryGeneration)
	binary.BigEndian.PutUint64(key[50:58], classificationEpoch)
	binary.BigEndian.PutUint64(key[58:], appliedCommit)
	return key
}

func AnalyticsQueryHeatKey(hash ID) []byte { return idKey("aq:heat:", hash) }

func AnalyticsQueryViewKey(view ID, bucket uint64) []byte {
	key := make([]byte, 8+32+8)
	copy(key, "aq:view:")
	copy(key[8:40], view[:])
	binary.BigEndian.PutUint64(key[40:], bucket)
	return key
}

func AnalyticsQueryJobKey(query ID) []byte { return idKey("aq:job:", query) }
func AnalyticsQueryJobPrefix() []byte      { return []byte("aq:job:") }

func GrowthTimeKey(granularity AnalyticsGranularity, timestamp int64, tier PackTier) []byte {
	if !validAnalyticsGranularity(granularity) || !validPackTier(tier) {
		return nil
	}
	key := make([]byte, 7+1+8+1)
	copy(key, "g:time:")
	key[7] = byte(granularity)
	binary.BigEndian.PutUint64(key[8:16], uint64(timestamp)^(uint64(1)<<63))
	key[16] = byte(tier)
	return key
}

func GrowthTimePrefix(granularity AnalyticsGranularity) []byte {
	if !validAnalyticsGranularity(granularity) {
		return nil
	}
	return []byte{'g', ':', 't', 'i', 'm', 'e', ':', byte(granularity)}
}

func GrowthPathKey(path string, granularity AnalyticsGranularity, timestamp int64) []byte {
	path = normalizeAnalyticsPath(path)
	if path == "" || len(path) > MaxPathIndexPathBytes || !validAnalyticsGranularity(granularity) {
		return nil
	}
	key := make([]byte, 7+2+len(path)+1+8)
	copy(key, "g:path:")
	binary.BigEndian.PutUint16(key[7:9], uint16(len(path)))
	copy(key[9:], path)
	key[9+len(path)] = byte(granularity)
	binary.BigEndian.PutUint64(key[10+len(path):], uint64(timestamp)^(uint64(1)<<63))
	return key
}

func GrowthPathPrefix(path string) []byte {
	path = normalizeAnalyticsPath(path)
	if path == "" || len(path) > MaxPathIndexPathBytes {
		return nil
	}
	key := make([]byte, 7+2+len(path))
	copy(key, "g:path:")
	binary.BigEndian.PutUint16(key[7:9], uint16(len(path)))
	copy(key[9:], path)
	return key
}

func UserSummaryKey(uid uint32) []byte  { return analyticsUint32Key("u:summary:", uid) }
func GroupSummaryKey(gid uint32) []byte { return analyticsUint32Key("g:summary:", gid) }

func UserStatsKey(uid uint32, residency AnalyticsResidency) []byte {
	return analyticsStatsKey("u:statsv1:", uid, residency)
}

func UserStatsPrefix() []byte { return []byte("u:statsv1:") }

func GroupStatsKey(gid uint32, residency AnalyticsResidency) []byte {
	return analyticsStatsKey("g:statsv1:", gid, residency)
}

func GroupStatsPrefix() []byte { return []byte("g:statsv1:") }

func analyticsStatsKey(prefix string, id uint32, residency AnalyticsResidency) []byte {
	if residency < AnalyticsLive || residency > AnalyticsExpired {
		return nil
	}
	key := make([]byte, len(prefix)+4+1)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):len(prefix)+4], id)
	key[len(key)-1] = byte(residency)
	return key
}

func UserChurnKey(uid uint32, granularity AnalyticsGranularity, timestamp int64) []byte {
	if !validAnalyticsGranularity(granularity) {
		return nil
	}
	key := make([]byte, 8+4+1+8)
	copy(key, "u:churn:")
	binary.BigEndian.PutUint32(key[8:12], uid)
	key[12] = byte(granularity)
	binary.BigEndian.PutUint64(key[13:], uint64(timestamp)^(uint64(1)<<63))
	return key
}

func UserInodeKey(uid, fsid uint32, inode uint64) []byte {
	key := make([]byte, 9+4+4+8)
	copy(key, "u:inodes:")
	binary.BigEndian.PutUint32(key[9:13], uid)
	binary.BigEndian.PutUint32(key[13:17], fsid)
	binary.BigEndian.PutUint64(key[17:], inode)
	return key
}

func UserInodePrefix(uid uint32) []byte { return analyticsUint32Key("u:inodes:", uid) }

func UserBlobKey(uid uint32, blob ID) []byte {
	key := make([]byte, 8+4+32)
	copy(key, "u:blobs:")
	binary.BigEndian.PutUint32(key[8:12], uid)
	copy(key[12:], blob[:])
	return key
}

func UserBlobPrefix(uid uint32) []byte { return analyticsUint32Key("u:blobs:", uid) }

func UserBlobContributionKey(uid uint32, blob ID, fsid uint32, inode, generation uint64, ordinal uint32) []byte {
	if generation == 0 {
		return nil
	}
	key := make([]byte, 9+4+32+4+8+8+4)
	copy(key, "u:blobv1:")
	binary.BigEndian.PutUint32(key[9:13], uid)
	copy(key[13:45], blob[:])
	binary.BigEndian.PutUint32(key[45:49], fsid)
	binary.BigEndian.PutUint64(key[49:57], inode)
	binary.BigEndian.PutUint64(key[57:65], generation)
	binary.BigEndian.PutUint32(key[65:], ordinal)
	return key
}

func UserBlobContributionPrefix(uid uint32) []byte { return analyticsUint32Key("u:blobv1:", uid) }

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
	case len(key) >= 13 && string(key[:4]) == "av1:" && key[12] == ':':
		generation := binary.BigEndian.Uint64(key[4:12])
		if generation == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics derived key generation", ErrMalformed)
		}
		if string(key[13:]) == "complete" {
			parsed.Kind, parsed.ViewGeneration = KeyAnalyticsDerivedMarker, generation
			break
		}
		inner, err := ParseKey(key[13:])
		if err != nil || inner.ViewGeneration != 0 || !isAnalyticsDerivedKind(inner.Kind) {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics derived key", ErrMalformed)
		}
		parsed = inner
		parsed.ViewGeneration = generation
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
	case len(key) == 43 && string(key[:3]) == "vr:":
		parsed.Kind = KeyVerificationState
		copy(parsed.ID[:], key[3:35])
		parsed.Backend = binary.BigEndian.Uint64(key[35:])
	case len(key) == 59 && string(key[:3]) == "ve:":
		parsed.Kind = KeyVerificationEvent
		parsed.EventTime = binary.BigEndian.Uint64(key[3:11])
		parsed.Revision = binary.BigEndian.Uint64(key[11:19])
		copy(parsed.ID[:], key[19:51])
		parsed.Backend = binary.BigEndian.Uint64(key[51:])
	case len(key) == len("u:policy:blocklist:")+4 && string(key[:len("u:policy:blocklist:")]) == "u:policy:blocklist:":
		parsed.Kind = KeyUIDExclusionPolicy
		parsed.UID = binary.BigEndian.Uint32(key[len("u:policy:blocklist:"):])
	case len(key) == 47 && string(key[:3]) == "dc:":
		parsed.Kind = KeyDeletionCertificate
		parsed.UID = binary.BigEndian.Uint32(key[3:7])
		parsed.EventTime = binary.BigEndian.Uint64(key[7:15])
		copy(parsed.ID[:], key[15:])
	case len(key) == 44 && string(key[:3]) == "bp:" && key[11] == ':':
		parsed.Kind = KeyBackendPack
		parsed.Backend = binary.BigEndian.Uint64(key[3:11])
		copy(parsed.ID[:], key[12:])
	case len(key) == 53 && string(key[:3]) == "dq:" && key[11] == ':' && key[44] == ':':
		parsed.Kind = KeyPlacementDeleteQueue
		parsed.DeleteAfter = int64(binary.BigEndian.Uint64(key[3:11]))
		copy(parsed.ID[:], key[12:44])
		parsed.Backend = binary.BigEndian.Uint64(key[45:])
	case len(key) == 44 && string(key[:3]) == "rq:" && key[11] == ':':
		parsed.Kind = KeyPlacementRequest
		parsed.EventTime = binary.BigEndian.Uint64(key[3:11])
		copy(parsed.ID[:], key[12:])
	case len(key) == 68 && string(key[:3]) == "rl:" && key[35] == ':':
		parsed.Kind = KeyRepackLineage
		copy(parsed.ID[:], key[3:35])
		copy(parsed.SecondID[:], key[36:])
	case len(key) == 35 && string(key[:3]) == "pe:":
		parsed.Kind = KeyPromotionEligibility
		copy(parsed.ID[:], key[3:])
	case len(key) == 15 && string(key[:3]) == "an:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyAnalyticsFact, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:])
	case len(key) == 8 && string(key[:3]) == "ad:":
		parsed.Kind = KeyAnalyticsDictionary
		parsed.Dictionary = AnalyticsDictionaryKind(key[3])
		parsed.Ordinal = binary.BigEndian.Uint32(key[4:])
		if !validAnalyticsDictionaryKind(parsed.Dictionary) || parsed.Ordinal == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics dictionary key", ErrMalformed)
		}
	case len(key) == 11 && string(key[:3]) == "af:":
		parsed.Kind, parsed.Generation = KeyAnalyticsFactSegment, binary.BigEndian.Uint64(key[3:])
		if parsed.Generation == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics fact segment key", ErrMalformed)
		}
	case len(key) == 11 && string(key[:3]) == "am:":
		parsed.Kind, parsed.Generation = KeyAnalyticsSegmentMetadata, binary.BigEndian.Uint64(key[3:])
		if parsed.Generation == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics segment metadata key", ErrMalformed)
		}
	case len(key) == 20 && string(key[:3]) == "ai:":
		parsed.Kind = KeyAnalyticsDimensionIndex
		parsed.Dimension = AnalyticsDimension(key[3])
		parsed.Value = binary.BigEndian.Uint64(key[4:12])
		parsed.Generation = binary.BigEndian.Uint64(key[12:])
		if !validAnalyticsDimension(parsed.Dimension) || parsed.Generation == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics dimension index key", ErrMalformed)
		}
	case len(key) == 23 && string(key[:3]) == "ar:":
		parsed.Kind = KeyAnalyticsResidency
		parsed.FSID = binary.BigEndian.Uint32(key[3:7])
		parsed.Inode = binary.BigEndian.Uint64(key[7:15])
		parsed.Generation = binary.BigEndian.Uint64(key[15:])
		if parsed.Generation == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics residency key", ErrMalformed)
		}
	case len(key) == 15 && string(key[:3]) == "ae:":
		parsed.Kind = KeyAnalyticsDelta
		parsed.Commit = binary.BigEndian.Uint64(key[3:11])
		parsed.Ordinal = binary.BigEndian.Uint32(key[11:])
		if parsed.Commit == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics delta key", ErrMalformed)
		}
	case len(key) == 44 && string(key[:4]) == "acp:":
		parsed.Kind = KeyAuthoritativeCrawlProof
		copy(parsed.ID[:], key[4:36])
		parsed.Commit = binary.BigEndian.Uint64(key[36:])
		if parsed.ID == (ID{}) || parsed.Commit == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid authoritative crawl proof key", ErrMalformed)
		}
	case len(key) == 56 && string(key[:4]) == "asb:":
		parsed.Kind = KeyAuthoritativeSourceBinding
		copy(parsed.ID[:], key[4:36])
		parsed.FSID = binary.BigEndian.Uint32(key[36:40])
		parsed.Inode = binary.BigEndian.Uint64(key[40:48])
		parsed.Generation = binary.BigEndian.Uint64(key[48:])
		if parsed.ID == (ID{}) || parsed.FSID == 0 || parsed.Inode == 0 || parsed.Generation == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid authoritative source binding key", ErrMalformed)
		}
	case len(key) == 19 && string(key[:11]) == "aw:applied:":
		parsed.Kind, parsed.Epoch = KeyAnalyticsWatermark, binary.BigEndian.Uint64(key[11:])
		if parsed.Epoch == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics watermark key", ErrMalformed)
		}
	case len(key) == 11 && string(key[:3]) == "ap:":
		parsed.Kind, parsed.Epoch = KeyAnalyticsManifest, binary.BigEndian.Uint64(key[3:])
		if parsed.Epoch == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics manifest key", ErrMalformed)
		}
	case len(key) == 66 && string(key[:10]) == "aq:result:":
		parsed.Kind = KeyAnalyticsQueryResult
		copy(parsed.ID[:], key[10:42])
		parsed.Generation = binary.BigEndian.Uint64(key[42:50])
		parsed.Epoch = binary.BigEndian.Uint64(key[50:58])
		parsed.Commit = binary.BigEndian.Uint64(key[58:])
		if parsed.Generation == 0 || parsed.Epoch == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics query result key", ErrMalformed)
		}
	case len(key) == 40 && string(key[:8]) == "aq:heat:":
		parsed.Kind = KeyAnalyticsQueryHeat
		copy(parsed.ID[:], key[8:])
	case len(key) == 48 && string(key[:8]) == "aq:view:":
		parsed.Kind = KeyAnalyticsQueryView
		copy(parsed.ID[:], key[8:40])
		parsed.Value = binary.BigEndian.Uint64(key[40:])
	case len(key) == 39 && string(key[:7]) == "aq:job:":
		parsed.Kind = KeyAnalyticsQueryJob
		copy(parsed.ID[:], key[7:])
	case len(key) == 17 && string(key[:7]) == "g:time:":
		parsed.Kind = KeyGrowthTime
		parsed.Granularity = HistoryGranularity(key[7])
		parsed.Timestamp = int64(binary.BigEndian.Uint64(key[8:16]) ^ (uint64(1) << 63))
		parsed.Tier = PackTier(key[16])
		if !validAnalyticsGranularity(AnalyticsGranularity(parsed.Granularity)) || !validPackTier(parsed.Tier) {
			return ParsedKey{}, fmt.Errorf("%w: invalid growth time key", ErrMalformed)
		}
	case len(key) >= 18 && string(key[:7]) == "g:path:":
		pathLength := int(binary.BigEndian.Uint16(key[7:9]))
		if pathLength == 0 || pathLength > MaxPathIndexPathBytes || len(key) != 7+2+pathLength+1+8 {
			return ParsedKey{}, fmt.Errorf("%w: invalid growth path key", ErrMalformed)
		}
		parsed.Kind = KeyGrowthPath
		parsed.Path = string(key[9 : 9+pathLength])
		parsed.Granularity = HistoryGranularity(key[9+pathLength])
		parsed.Timestamp = int64(binary.BigEndian.Uint64(key[10+pathLength:]) ^ (uint64(1) << 63))
		if parsed.Path != normalizeAnalyticsPath(parsed.Path) || !validAnalyticsGranularity(AnalyticsGranularity(parsed.Granularity)) {
			return ParsedKey{}, fmt.Errorf("%w: invalid growth path key", ErrMalformed)
		}
	case len(key) == 14 && string(key[:10]) == "u:summary:":
		parsed.Kind, parsed.UID = KeyUserSummary, binary.BigEndian.Uint32(key[10:])
	case len(key) == 14 && string(key[:10]) == "g:summary:":
		parsed.Kind, parsed.GID = KeyGroupSummary, binary.BigEndian.Uint32(key[10:])
	case len(key) == 15 && string(key[:10]) == "u:statsv1:":
		parsed.Kind, parsed.UID = KeyUserStats, binary.BigEndian.Uint32(key[10:14])
		parsed.Residency = AnalyticsResidency(key[14])
		if parsed.Residency < AnalyticsLive || parsed.Residency > AnalyticsExpired {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics user stats key", ErrMalformed)
		}
	case len(key) == 15 && string(key[:10]) == "g:statsv1:":
		parsed.Kind, parsed.GID = KeyGroupStats, binary.BigEndian.Uint32(key[10:14])
		parsed.Residency = AnalyticsResidency(key[14])
		if parsed.Residency < AnalyticsLive || parsed.Residency > AnalyticsExpired {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics group stats key", ErrMalformed)
		}
	case len(key) == 21 && string(key[:8]) == "u:churn:":
		parsed.Kind, parsed.UID = KeyUserChurn, binary.BigEndian.Uint32(key[8:12])
		parsed.Granularity = HistoryGranularity(key[12])
		parsed.Timestamp = int64(binary.BigEndian.Uint64(key[13:]) ^ (uint64(1) << 63))
		if !validAnalyticsGranularity(AnalyticsGranularity(parsed.Granularity)) {
			return ParsedKey{}, fmt.Errorf("%w: invalid user churn key", ErrMalformed)
		}
	case len(key) == 25 && string(key[:9]) == "u:inodes:":
		parsed.Kind, parsed.UID = KeyUserInode, binary.BigEndian.Uint32(key[9:13])
		parsed.FSID = binary.BigEndian.Uint32(key[13:17])
		parsed.Inode = binary.BigEndian.Uint64(key[17:])
	case len(key) == 44 && string(key[:8]) == "u:blobs:":
		parsed.Kind, parsed.UID = KeyUserBlob, binary.BigEndian.Uint32(key[8:12])
		copy(parsed.ID[:], key[12:])
	case len(key) == 69 && string(key[:9]) == "u:blobv1:":
		parsed.Kind, parsed.UID = KeyUserBlobContribution, binary.BigEndian.Uint32(key[9:13])
		copy(parsed.ID[:], key[13:45])
		parsed.FSID = binary.BigEndian.Uint32(key[45:49])
		parsed.Inode = binary.BigEndian.Uint64(key[49:57])
		parsed.Generation = binary.BigEndian.Uint64(key[57:65])
		parsed.Ordinal = binary.BigEndian.Uint32(key[65:])
		if parsed.Generation == 0 {
			return ParsedKey{}, fmt.Errorf("%w: invalid analytics user blob contribution key", ErrMalformed)
		}
	case len(key) == 35 && string(key[:3]) == "aq:" && !reservedAnalyticsQueryPrefix(key):
		parsed.Kind = KeyAnalyticsCache
		copy(parsed.ID[:], key[3:])
	case string(key) == "meta:analytics":
		parsed.Kind = KeyAnalyticsMetadata
	case string(key) == "meta:analytics-build":
		parsed.Kind = KeyAnalyticsBuildCheckpoint
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

func isAnalyticsDerivedKey(key []byte) bool {
	parsed, err := ParseKey(key)
	return err == nil && parsed.ViewGeneration == 0 && isAnalyticsDerivedKind(parsed.Kind)
}

func isAnalyticsDerivedKind(kind KeyKind) bool {
	switch kind {
	case KeyAnalyticsResidency, KeyGrowthTime, KeyGrowthPath, KeyUserSummary, KeyGroupSummary, KeyUserStats, KeyGroupStats, KeyUserChurn, KeyUserInode, KeyUserBlob, KeyUserBlobContribution:
		return true
	default:
		return false
	}
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

func analyticsUint64Key(prefix string, value uint64) []byte {
	if value == 0 {
		return nil
	}
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], value)
	return key
}

func analyticsUint32Key(prefix string, value uint32) []byte {
	key := make([]byte, len(prefix)+4)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], value)
	return key
}

func normalizeAnalyticsPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return ""
	}
	path = "/" + strings.Trim(path, "/")
	if path == "/" || strings.Contains(path, "//") {
		return ""
	}
	return path
}

func validAnalyticsDictionaryKind(kind AnalyticsDictionaryKind) bool {
	return kind >= AnalyticsDictionarySVM && kind <= AnalyticsDictionaryPathGroup
}

func validAnalyticsDimension(dimension AnalyticsDimension) bool {
	return dimension >= AnalyticsDimensionUID && dimension <= AnalyticsDimensionResidency
}

func validAnalyticsGranularity(granularity AnalyticsGranularity) bool {
	return granularity >= AnalyticsGranularityWeek && granularity <= AnalyticsGranularityYear
}

func reservedAnalyticsQueryPrefix(key []byte) bool {
	for _, prefix := range []string{"aq:result:", "aq:heat:", "aq:view:", "aq:job:"} {
		if strings.HasPrefix(string(key), prefix) {
			return true
		}
	}
	return false
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
