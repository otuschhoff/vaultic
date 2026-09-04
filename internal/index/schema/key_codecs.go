package schema

import (
	"encoding/binary"
	"fmt"
)

type analyticsKeyCodec struct{}
type blobKeyCodec struct{}
type contentKeyCodec struct{}
type directoryKeyCodec struct{}
type growthKeyCodec struct{}
type hardlinkKeyCodec struct{}
type inodeKeyCodec struct{}
type metadataKeyCodec struct{}
type packKeyCodec struct{}
type crawlKeyCodec struct{}
type reverseKeyCodec struct{}
type snapshotKeyCodec struct{}
type userKeyCodec struct{}
type verificationKeyCodec struct{}

func (analyticsKeyCodec) parse(key []byte) (ParsedKey, error) {
	if len(key) >= 13 && string(key[:4]) == "av1:" && key[12] == ':' {
		return parseAnalyticsDerivedKey(key)
	}
	if parsed, ok, err := parseAnalyticsCoreKey(key); ok {
		return parsed, err
	}
	if parsed, ok, err := parseAnalyticsQueryKey(key); ok {
		return parsed, err
	}
	if kind, ok := parseAggregate(key); ok {
		return ParsedKey{Kind: KeyPackAggregate, Aggregate: kind}, nil
	}
	if tier, ok := parseTierAggregate(key); ok {
		return ParsedKey{Kind: KeyTierAggregate, Tier: tier}, nil
	}
	return malformedKey()
}

func parseAnalyticsDerivedKey(key []byte) (ParsedKey, error) {
	generation := binary.BigEndian.Uint64(key[4:12])
	if generation == 0 {
		return namedMalformedKey("invalid analytics derived key generation")
	}
	if string(key[13:]) == "complete" {
		return ParsedKey{Kind: KeyAnalyticsDerivedMarker, ViewGeneration: generation}, nil
	}
	inner, err := ParseKey(key[13:])
	if err != nil || inner.ViewGeneration != 0 || !isAnalyticsDerivedKind(inner.Kind) {
		return namedMalformedKey("invalid analytics derived key")
	}
	inner.ViewGeneration = generation
	return inner, nil
}

func parseAnalyticsCoreKey(key []byte) (ParsedKey, bool, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 15 && string(key[:3]) == "an:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyAnalyticsFact, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:])
	case len(key) == 8 && string(key[:3]) == "ad:":
		parsed.Kind, parsed.Dictionary = KeyAnalyticsDictionary, AnalyticsDictionaryKind(key[3])
		parsed.Ordinal = binary.BigEndian.Uint32(key[4:])
		if !validAnalyticsDictionaryKind(parsed.Dictionary) || parsed.Ordinal == 0 {
			return parsed, true, namedMalformedError("invalid analytics dictionary key")
		}
	case len(key) == 11 && string(key[:3]) == "af:":
		parsed.Kind, parsed.Generation = KeyAnalyticsFactSegment, binary.BigEndian.Uint64(key[3:])
		if parsed.Generation == 0 {
			return parsed, true, namedMalformedError("invalid analytics fact segment key")
		}
	case len(key) == 11 && string(key[:3]) == "am:":
		parsed.Kind, parsed.Generation = KeyAnalyticsSegmentMetadata, binary.BigEndian.Uint64(key[3:])
		if parsed.Generation == 0 {
			return parsed, true, namedMalformedError("invalid analytics segment metadata key")
		}
	case len(key) == 20 && string(key[:3]) == "ai:":
		parsed.Kind, parsed.Dimension = KeyAnalyticsDimensionIndex, AnalyticsDimension(key[3])
		parsed.Value, parsed.Generation = binary.BigEndian.Uint64(key[4:12]), binary.BigEndian.Uint64(key[12:])
		if !validAnalyticsDimension(parsed.Dimension) || parsed.Generation == 0 {
			return parsed, true, namedMalformedError("invalid analytics dimension index key")
		}
	case len(key) == 23 && string(key[:3]) == "ar:":
		parsed.Kind, parsed.FSID = KeyAnalyticsResidency, binary.BigEndian.Uint32(key[3:7])
		parsed.Inode, parsed.Generation = binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
		if parsed.Generation == 0 {
			return parsed, true, namedMalformedError("invalid analytics residency key")
		}
	case len(key) == 15 && string(key[:3]) == "ae:":
		parsed.Kind, parsed.Commit, parsed.Ordinal = KeyAnalyticsDelta, binary.BigEndian.Uint64(key[3:11]), binary.BigEndian.Uint32(key[11:])
		if parsed.Commit == 0 {
			return parsed, true, namedMalformedError("invalid analytics delta key")
		}
	default:
		return ParsedKey{}, false, nil
	}
	return parsed, true, nil
}

func parseAnalyticsQueryKey(key []byte) (ParsedKey, bool, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 44 && string(key[:4]) == "acp:":
		parsed.Kind = KeyAuthoritativeCrawlProof
		copy(parsed.ID[:], key[4:36])
		parsed.Commit = binary.BigEndian.Uint64(key[36:])
		if parsed.ID == (ID{}) || parsed.Commit == 0 {
			return parsed, true, namedMalformedError("invalid authoritative crawl proof key")
		}
	case len(key) == 56 && string(key[:4]) == "asb:":
		parsed.Kind = KeyAuthoritativeSourceBinding
		copy(parsed.ID[:], key[4:36])
		parsed.FSID, parsed.Inode = binary.BigEndian.Uint32(key[36:40]), binary.BigEndian.Uint64(key[40:48])
		parsed.Generation = binary.BigEndian.Uint64(key[48:])
		if parsed.ID == (ID{}) || parsed.FSID == 0 || parsed.Inode == 0 || parsed.Generation == 0 {
			return parsed, true, namedMalformedError("invalid authoritative source binding key")
		}
	case len(key) == 19 && string(key[:11]) == "aw:applied:":
		parsed.Kind, parsed.Epoch = KeyAnalyticsWatermark, binary.BigEndian.Uint64(key[11:])
		if parsed.Epoch == 0 {
			return parsed, true, namedMalformedError("invalid analytics watermark key")
		}
	case len(key) == 11 && string(key[:3]) == "ap:":
		parsed.Kind, parsed.Epoch = KeyAnalyticsManifest, binary.BigEndian.Uint64(key[3:])
		if parsed.Epoch == 0 {
			return parsed, true, namedMalformedError("invalid analytics manifest key")
		}
	default:
		return parseAnalyticsQueryResultKey(key)
	}
	return parsed, true, nil
}

func parseAnalyticsQueryResultKey(key []byte) (ParsedKey, bool, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 66 && string(key[:10]) == "aq:result:":
		parsed.Kind = KeyAnalyticsQueryResult
		copy(parsed.ID[:], key[10:42])
		parsed.Generation, parsed.Epoch = binary.BigEndian.Uint64(key[42:50]), binary.BigEndian.Uint64(key[50:58])
		parsed.Commit = binary.BigEndian.Uint64(key[58:])
		if parsed.Generation == 0 || parsed.Epoch == 0 {
			return parsed, true, namedMalformedError("invalid analytics query result key")
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
	case len(key) == 35 && string(key[:3]) == "aq:" && !reservedAnalyticsQueryPrefix(key):
		parsed.Kind = KeyAnalyticsCache
		copy(parsed.ID[:], key[3:])
	default:
		return ParsedKey{}, false, nil
	}
	return parsed, true, nil
}

func (blobKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 34 && string(key[:2]) == "b:":
		parsed.Kind = KeyBlob
		copy(parsed.ID[:], key[2:])
	case len(key) == 44 && string(key[:3]) == "bp:" && key[11] == ':':
		parsed.Kind, parsed.Backend = KeyBackendPack, binary.BigEndian.Uint64(key[3:11])
		copy(parsed.ID[:], key[12:])
	default:
		return malformedKey()
	}
	return parsed, nil
}

func (contentKeyCodec) parse(key []byte) (ParsedKey, error) {
	if len(key) != 39 || string(key[:3]) != "cm:" {
		return malformedKey()
	}
	parsed := ParsedKey{Kind: KeyContentManifest, Segment: binary.BigEndian.Uint32(key[35:])}
	copy(parsed.ID[:], key[3:35])
	return parsed, nil
}

func (directoryKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 14 && string(key[:2]) == "d:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyCurrentDirectory, binary.BigEndian.Uint32(key[2:6]), binary.BigEndian.Uint64(key[6:])
	case len(key) == 23 && string(key[:3]) == "dv:":
		parsed.Kind, parsed.FSID = KeyDirectoryRevision, binary.BigEndian.Uint32(key[3:7])
		parsed.Inode, parsed.Revision = binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
	case len(key) == 47 && string(key[:3]) == "dc:":
		parsed.Kind, parsed.UID, parsed.EventTime = KeyDeletionCertificate, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:15])
		copy(parsed.ID[:], key[15:])
	case len(key) == 53 && string(key[:3]) == "dq:" && key[11] == ':' && key[44] == ':':
		parsed.Kind, parsed.DeleteAfter = KeyPlacementDeleteQueue, int64(binary.BigEndian.Uint64(key[3:11]))
		copy(parsed.ID[:], key[12:44])
		parsed.Backend = binary.BigEndian.Uint64(key[45:])
	default:
		return malformedKey()
	}
	return parsed, nil
}

func (growthKeyCodec) parse(key []byte) (ParsedKey, error) {
	if len(key) == 37 && (string(key[:5]) == "gc:b:" || string(key[:5]) == "gc:p:") {
		parsed := ParsedKey{Kind: KeyGarbageCollection, GCTarget: GCBlob}
		if key[3] == 'p' {
			parsed.GCTarget = GCPack
		}
		copy(parsed.ID[:], key[5:])
		return parsed, nil
	}
	if len(key) == 17 && string(key[:7]) == "g:time:" {
		return parseGrowthTimeKey(key)
	}
	if len(key) >= 18 && string(key[:7]) == "g:path:" {
		return parseGrowthPathKey(key)
	}
	if len(key) == 14 && string(key[:10]) == "g:summary:" {
		return ParsedKey{Kind: KeyGroupSummary, GID: binary.BigEndian.Uint32(key[10:])}, nil
	}
	if len(key) == 15 && string(key[:10]) == "g:statsv1:" {
		parsed := ParsedKey{Kind: KeyGroupStats, GID: binary.BigEndian.Uint32(key[10:14]), Residency: AnalyticsResidency(key[14])}
		if parsed.Residency < AnalyticsLive || parsed.Residency > AnalyticsExpired {
			return namedMalformedKey("invalid analytics group stats key")
		}
		return parsed, nil
	}
	return malformedKey()
}

func parseGrowthTimeKey(key []byte) (ParsedKey, error) {
	parsed := ParsedKey{Kind: KeyGrowthTime, Granularity: HistoryGranularity(key[7]), Tier: PackTier(key[16])}
	parsed.Timestamp = int64(binary.BigEndian.Uint64(key[8:16]) ^ (uint64(1) << 63))
	if !validAnalyticsGranularity(AnalyticsGranularity(parsed.Granularity)) || !validPackTier(parsed.Tier) {
		return namedMalformedKey("invalid growth time key")
	}
	return parsed, nil
}

func parseGrowthPathKey(key []byte) (ParsedKey, error) {
	pathLength := int(binary.BigEndian.Uint16(key[7:9]))
	if pathLength == 0 || pathLength > MaxPathIndexPathBytes || len(key) != 7+2+pathLength+1+8 {
		return namedMalformedKey("invalid growth path key")
	}
	parsed := ParsedKey{Kind: KeyGrowthPath, Path: string(key[9 : 9+pathLength])}
	parsed.Granularity = HistoryGranularity(key[9+pathLength])
	parsed.Timestamp = int64(binary.BigEndian.Uint64(key[10+pathLength:]) ^ (uint64(1) << 63))
	if parsed.Path != normalizeAnalyticsPath(parsed.Path) || !validAnalyticsGranularity(AnalyticsGranularity(parsed.Granularity)) {
		return namedMalformedKey("invalid growth path key")
	}
	return parsed, nil
}

func (hardlinkKeyCodec) parse(key []byte) (ParsedKey, error) {
	if len(key) != 23 || string(key[:3]) != "hr:" {
		return malformedKey()
	}
	return ParsedKey{
		Kind: KeyHardlinkRefs, FSID: binary.BigEndian.Uint32(key[3:7]),
		Inode: binary.BigEndian.Uint64(key[7:15]), Revision: binary.BigEndian.Uint64(key[15:]),
	}, nil
}

func (inodeKeyCodec) parse(key []byte) (ParsedKey, error) {
	if len(key) == 14 && string(key[:2]) == "i:" {
		return ParsedKey{Kind: KeyCurrentInode, FSID: binary.BigEndian.Uint32(key[2:6]), Inode: binary.BigEndian.Uint64(key[6:])}, nil
	}
	if len(key) == 23 && string(key[:3]) == "iv:" {
		return ParsedKey{
			Kind: KeyInodeRevision, FSID: binary.BigEndian.Uint32(key[3:7]),
			Inode: binary.BigEndian.Uint64(key[7:15]), Revision: binary.BigEndian.Uint64(key[15:]),
		}, nil
	}
	return malformedKey()
}

func (metadataKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
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
	case string(key) == "meta:analytics":
		parsed.Kind = KeyAnalyticsMetadata
	case string(key) == "meta:analytics-build":
		parsed.Kind = KeyAnalyticsBuildCheckpoint
	default:
		return malformedKey()
	}
	return parsed, nil
}

func (packKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 34 && string(key[:2]) == "p:":
		parsed.Kind = KeyPack
		copy(parsed.ID[:], key[2:])
	case len(key) == 51 && string(key[:3]) == "ph:":
		parsed.Kind, parsed.EventTime, parsed.Revision = KeyPackHistory, binary.BigEndian.Uint64(key[3:11]), binary.BigEndian.Uint64(key[11:19])
		copy(parsed.ID[:], key[19:])
	case len(key) == 21 && string(key[:3]) == "pb:":
		return parsePackHistoryBucketKey(key)
	case len(key) == 44 && string(key[:3]) == "pl:" && key[35] == ':':
		parsed.Kind, parsed.Backend = KeyPackPlacement, binary.BigEndian.Uint64(key[36:])
		copy(parsed.ID[:], key[3:35])
	case len(key) > 15 && string(key[:3]) == "pv:":
		return parsePathVersionKey(key)
	case len(key) == 35 && string(key[:3]) == "pe:":
		parsed.Kind = KeyPromotionEligibility
		copy(parsed.ID[:], key[3:])
	default:
		return malformedKey()
	}
	return parsed, nil
}

func parsePackHistoryBucketKey(key []byte) (ParsedKey, error) {
	parsed := ParsedKey{Kind: KeyPackHistoryBucket, Granularity: HistoryGranularity(key[3])}
	parsed.EventTime, parsed.Backend, parsed.PackType = binary.BigEndian.Uint64(key[4:12]), binary.BigEndian.Uint64(key[12:20]), PackType(key[20])
	if !validHistoryGranularity(parsed.Granularity) || !validPackType(parsed.PackType) {
		return namedMalformedKey("invalid history bucket key")
	}
	return parsed, nil
}

func parsePathVersionKey(key []byte) (ParsedKey, error) {
	terminator := -1
	for offset := 7; offset < len(key)-8; offset++ {
		if key[offset] == 0 {
			terminator = offset
			break
		}
	}
	if terminator <= 7 || terminator != len(key)-9 {
		return namedMalformedKey("invalid path-version key")
	}
	parsed := ParsedKey{Kind: KeyPathVersion, FSID: binary.BigEndian.Uint32(key[3:7]), Path: string(key[7:terminator])}
	parsed.Revision = binary.BigEndian.Uint64(key[terminator+1:])
	if parsed.Revision == 0 || len(parsed.Path) > MaxPathIndexPathBytes {
		return namedMalformedKey("invalid path-version key")
	}
	return parsed, nil
}

func (crawlKeyCodec) parse(key []byte) (ParsedKey, error) {
	if len(key) != 66 || string(key[:2]) != "q:" {
		return malformedKey()
	}
	parsed := ParsedKey{Kind: KeyCrawlDebt}
	copy(parsed.ID[:], key[2:34])
	copy(parsed.SecondID[:], key[34:])
	return parsed, nil
}

func (reverseKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 35 && string(key[:3]) == "rc:":
		parsed.Kind = KeyReferenceCount
		copy(parsed.ID[:], key[3:])
	case len(key) == 67 && string(key[:3]) == "rm:":
		parsed.Kind = KeyReverseManifest
		copy(parsed.ID[:], key[3:35])
		copy(parsed.SecondID[:], key[35:])
	case len(key) == 47 && string(key[:3]) == "ri:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyReverseInode, binary.BigEndian.Uint32(key[35:39]), binary.BigEndian.Uint64(key[39:])
		copy(parsed.ID[:], key[3:35])
	case len(key) == 44 && string(key[:3]) == "rq:" && key[11] == ':':
		parsed.Kind, parsed.EventTime = KeyPlacementRequest, binary.BigEndian.Uint64(key[3:11])
		copy(parsed.ID[:], key[12:])
	case len(key) == 68 && string(key[:3]) == "rl:" && key[35] == ':':
		parsed.Kind = KeyRepackLineage
		copy(parsed.ID[:], key[3:35])
		copy(parsed.SecondID[:], key[36:])
	default:
		return malformedKey()
	}
	return parsed, nil
}

func (snapshotKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	if len(key) == 34 && string(key[:2]) == "s:" {
		parsed.Kind = KeySnapshot
		copy(parsed.ID[:], key[2:])
		return parsed, nil
	}
	if len(key) == 44 && string(key[:3]) == "sc:" && key[11] == ':' {
		parsed.Kind, parsed.Revision = KeySnapshotCommit, binary.BigEndian.Uint64(key[3:11])
		copy(parsed.ID[:], key[12:])
		return parsed, nil
	}
	return malformedKey()
}

func (userKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
	case len(key) == len("u:policy:blocklist:")+4 && string(key[:len("u:policy:blocklist:")]) == "u:policy:blocklist:":
		parsed.Kind, parsed.UID = KeyUIDExclusionPolicy, binary.BigEndian.Uint32(key[len("u:policy:blocklist:"):])
	case len(key) == 14 && string(key[:10]) == "u:summary:":
		parsed.Kind, parsed.UID = KeyUserSummary, binary.BigEndian.Uint32(key[10:])
	case len(key) == 15 && string(key[:10]) == "u:statsv1:":
		parsed.Kind, parsed.UID, parsed.Residency = KeyUserStats, binary.BigEndian.Uint32(key[10:14]), AnalyticsResidency(key[14])
		if parsed.Residency < AnalyticsLive || parsed.Residency > AnalyticsExpired {
			return namedMalformedKey("invalid analytics user stats key")
		}
	case len(key) == 21 && string(key[:8]) == "u:churn:":
		parsed.Kind, parsed.UID = KeyUserChurn, binary.BigEndian.Uint32(key[8:12])
		parsed.Granularity, parsed.Timestamp = HistoryGranularity(key[12]), int64(binary.BigEndian.Uint64(key[13:])^(uint64(1)<<63))
		if !validAnalyticsGranularity(AnalyticsGranularity(parsed.Granularity)) {
			return namedMalformedKey("invalid user churn key")
		}
	case len(key) == 25 && string(key[:9]) == "u:inodes:":
		parsed.Kind, parsed.UID, parsed.FSID = KeyUserInode, binary.BigEndian.Uint32(key[9:13]), binary.BigEndian.Uint32(key[13:17])
		parsed.Inode = binary.BigEndian.Uint64(key[17:])
	case len(key) == 44 && string(key[:8]) == "u:blobs:":
		parsed.Kind, parsed.UID = KeyUserBlob, binary.BigEndian.Uint32(key[8:12])
		copy(parsed.ID[:], key[12:])
	case len(key) == 69 && string(key[:9]) == "u:blobv1:":
		return parseUserBlobContributionKey(key)
	default:
		return malformedKey()
	}
	return parsed, nil
}

func parseUserBlobContributionKey(key []byte) (ParsedKey, error) {
	parsed := ParsedKey{Kind: KeyUserBlobContribution, UID: binary.BigEndian.Uint32(key[9:13])}
	copy(parsed.ID[:], key[13:45])
	parsed.FSID, parsed.Inode = binary.BigEndian.Uint32(key[45:49]), binary.BigEndian.Uint64(key[49:57])
	parsed.Generation, parsed.Ordinal = binary.BigEndian.Uint64(key[57:65]), binary.BigEndian.Uint32(key[65:])
	if parsed.Generation == 0 {
		return namedMalformedKey("invalid analytics user blob contribution key")
	}
	return parsed, nil
}

func (verificationKeyCodec) parse(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	if len(key) == 43 && string(key[:3]) == "vr:" {
		parsed.Kind = KeyVerificationState
		copy(parsed.ID[:], key[3:35])
		parsed.Backend = binary.BigEndian.Uint64(key[35:])
		return parsed, nil
	}
	if len(key) == 59 && string(key[:3]) == "ve:" {
		parsed.Kind, parsed.EventTime, parsed.Revision = KeyVerificationEvent, binary.BigEndian.Uint64(key[3:11]), binary.BigEndian.Uint64(key[11:19])
		copy(parsed.ID[:], key[19:51])
		parsed.Backend = binary.BigEndian.Uint64(key[51:])
		return parsed, nil
	}
	return malformedKey()
}

func namedMalformedKey(message string) (ParsedKey, error) {
	return ParsedKey{}, namedMalformedError(message)
}

func namedMalformedError(message string) error {
	return fmt.Errorf("%w: %s", ErrMalformed, message)
}

func (analyticsKeyCodec) validate(parsed ParsedKey, value []byte) error {
	return validateAnalyticsValue(parsed, value)
}

func (blobKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyBlob:
		_, err := UnmarshalBlobRecord(value)
		return err
	case KeyBackendPack:
		_, err := UnmarshalBackendPackRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (contentKeyCodec) validate(parsed ParsedKey, value []byte) error {
	if parsed.Kind != KeyContentManifest {
		return unsupportedSchemaKey()
	}
	return validateContentManifest(parsed, value)
}

func (directoryKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyCurrentDirectory:
		return validateCurrentPointer(parsed, value)
	case KeyDirectoryRevision:
		return validateDirectoryRevision(parsed, value)
	case KeyDeletionCertificate:
		_, err := UnmarshalDeletionCertificateRecord(value)
		return err
	case KeyPlacementDeleteQueue:
		_, err := UnmarshalPlacementDeleteRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (growthKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyGarbageCollection:
		_, err := UnmarshalGarbageCollectionRecord(value)
		return err
	case KeyGrowthTime, KeyGrowthPath:
		_, err := UnmarshalAnalyticsAggregateRecord(value)
		return err
	case KeyGroupSummary, KeyGroupStats:
		_, err := UnmarshalAnalyticsSummaryRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (hardlinkKeyCodec) validate(parsed ParsedKey, value []byte) error {
	if parsed.Kind != KeyHardlinkRefs {
		return unsupportedSchemaKey()
	}
	_, err := UnmarshalHardlinkRefsRecord(value)
	return err
}

func (inodeKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyCurrentInode:
		return validateCurrentPointer(parsed, value)
	case KeyInodeRevision:
		_, err := UnmarshalInodeRevision(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (metadataKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyImportCheckpoint:
		_, err := UnmarshalImportCheckpointRecord(value)
		return err
	case KeySnapshotImportCheckpoint:
		_, err := UnmarshalSnapshotImportCheckpointRecord(value)
		return err
	case KeyExportCheckpoint:
		_, err := UnmarshalExportCheckpointRecord(value)
		return err
	case KeyExportIndexCheckpoint:
		_, err := UnmarshalExportIndexCheckpointRecord(value)
		return err
	case KeyNextRevision:
		_, err := UnmarshalNextRevision(value)
		return err
	case KeyNextExportSequence:
		_, err := UnmarshalNextExportSequence(value)
		return err
	case KeyNextEventSequence:
		_, err := UnmarshalNextEventSequence(value)
		return err
	case KeyHistoryRawFloor, KeyHistoryEnabledAt:
		_, err := UnmarshalHistoryMarker(value)
		return err
	case KeyAnalyticsMetadata:
		_, err := UnmarshalAnalyticsMetadataRecord(value)
		return err
	case KeyAnalyticsBuildCheckpoint:
		_, err := UnmarshalAnalyticsBuildCheckpointRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (packKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyPack:
		_, err := UnmarshalPackRecord(value)
		return err
	case KeyPackHistory:
		_, err := UnmarshalPackHistoryEvent(value)
		return err
	case KeyPackHistoryBucket:
		_, err := UnmarshalPackHistoryBucket(value)
		return err
	case KeyPackPlacement:
		_, err := UnmarshalPlacementRecord(value)
		return err
	case KeyPathVersion:
		_, err := UnmarshalPathVersionRecord(value)
		return err
	case KeyPromotionEligibility:
		_, err := UnmarshalPromotionEligibilityRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (crawlKeyCodec) validate(parsed ParsedKey, value []byte) error {
	if parsed.Kind != KeyCrawlDebt {
		return unsupportedSchemaKey()
	}
	_, err := UnmarshalCrawlDebtRecord(value)
	return err
}

func (reverseKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyReferenceCount:
		_, err := UnmarshalReferenceCountRecord(value)
		return err
	case KeyReverseManifest:
		_, err := UnmarshalReverseManifestRecord(value)
		return err
	case KeyReverseInode:
		_, err := UnmarshalReverseInodeRecord(value)
		return err
	case KeyPlacementRequest:
		_, err := UnmarshalPlacementRequestRecord(value)
		return err
	case KeyRepackLineage:
		_, err := UnmarshalRepackLineageRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (snapshotKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeySnapshot:
		_, err := UnmarshalSnapshotRecord(value)
		return err
	case KeySnapshotCommit:
		return validateSnapshotCommit(value)
	default:
		return unsupportedSchemaKey()
	}
}

func (userKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyUIDExclusionPolicy:
		_, err := UnmarshalUIDExclusionPolicyRecord(value)
		return err
	case KeyUserChurn:
		_, err := UnmarshalAnalyticsAggregateRecord(value)
		return err
	case KeyUserSummary, KeyUserStats:
		_, err := UnmarshalAnalyticsSummaryRecord(value)
		return err
	case KeyUserInode:
		_, err := UnmarshalAnalyticsUserInodeRecord(value)
		return err
	case KeyUserBlob, KeyUserBlobContribution:
		_, err := UnmarshalAnalyticsUserBlobRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}

func (verificationKeyCodec) validate(parsed ParsedKey, value []byte) error {
	switch parsed.Kind {
	case KeyVerificationState:
		_, err := UnmarshalVerificationStateRecord(value)
		return err
	case KeyVerificationEvent:
		_, err := UnmarshalVerificationEventRecord(value)
		return err
	default:
		return unsupportedSchemaKey()
	}
}
