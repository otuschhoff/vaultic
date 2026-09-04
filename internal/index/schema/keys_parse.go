// Package schema defines the versioned binary key and value model stored by
// vaulticdb. It has no dependency on SlateDB or the daemon transport.
package schema

import (
	"encoding/binary"
	"fmt"
	"strings"
)

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

func AnalyticsQueryJobPrefix() []byte { return []byte("aq:job:") }

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

func UserSummaryKey(uid uint32) []byte { return analyticsUint32Key("u:summary:", uid) }

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
func HistoryEnabledAtKey() []byte { return []byte("meta:history-enabled-at") }

func NextRevisionKey() []byte { return []byte("meta:next-revision-seq") }

func NextExportSequenceKey() []byte { return []byte("meta:next-export-seq") }

type keyCodec interface {
	parse([]byte) (ParsedKey, error)
	validate(ParsedKey, []byte) error
}

var keyCodecs = map[byte]keyCodec{
	'a': analyticsKeyCodec{},
	'b': blobKeyCodec{},
	'c': contentKeyCodec{},
	'd': directoryKeyCodec{},
	'g': growthKeyCodec{},
	'h': hardlinkKeyCodec{},
	'i': inodeKeyCodec{},
	'm': metadataKeyCodec{},
	'p': packKeyCodec{},
	'q': crawlKeyCodec{},
	'r': reverseKeyCodec{},
	's': snapshotKeyCodec{},
	'u': userKeyCodec{},
	'v': verificationKeyCodec{},
}

func ParseKey(key []byte) (ParsedKey, error) {
	codec, err := codecForKey(key)
	if err != nil {
		return ParsedKey{}, err
	}
	return codec.parse(key)
}

func codecForKey(key []byte) (keyCodec, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("%w: unknown or incorrectly sized key", ErrMalformed)
	}
	codec, ok := keyCodecs[key[0]]
	if !ok {
		return nil, fmt.Errorf("%w: unknown or incorrectly sized key", ErrMalformed)
	}
	return codec, nil
}

func malformedKey() (ParsedKey, error) {
	return ParsedKey{}, fmt.Errorf("%w: unknown or incorrectly sized key", ErrMalformed)
}

func isAnalyticsDerivedKey(key []byte) bool {
	parsed, err := ParseKey(key)
	return err == nil && parsed.ViewGeneration == 0 && isAnalyticsDerivedKind(parsed.Kind)
}

func isAnalyticsDerivedKind(kind KeyKind) bool {
	switch kind {
	case KeyAnalyticsResidency,
		KeyGrowthTime,
		KeyGrowthPath,
		KeyUserSummary,
		KeyGroupSummary,
		KeyUserStats,
		KeyGroupStats,
		KeyUserChurn,
		KeyUserInode,
		KeyUserBlob,
		KeyUserBlobContribution:
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
