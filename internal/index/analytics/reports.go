package analytics

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type GrowthOptions struct {
	Granularity string
	Since       *int64
	Until       *int64
	SVMs        []string
	Volumes     []string
	PathGroups  []string
}

type GrowthBucket struct {
	Start        string `json:"start"`
	Files        uint64 `json:"files"`
	LogicalBytes uint64 `json:"logical_bytes"`
}

type GrowthResult struct {
	SchemaVersion int            `json:"schema_version"`
	Granularity   string         `json:"granularity"`
	Buckets       []GrowthBucket `json:"buckets"`
	Watermark     WatermarkInfo  `json:"watermark"`
	Explain       Explain        `json:"explain"`
}

func Growth(ctx context.Context, store Store, options GrowthOptions) (GrowthResult, error) {
	groupBy := []string{"year"}
	viewGranularity := schema.AnalyticsGranularityYear
	switch options.Granularity {
	case "year":
	case "month":
		groupBy = []string{"year", "month"}
		viewGranularity = schema.AnalyticsGranularityMonth
	case "iso-week":
		groupBy = []string{"iso-year", "workweek"}
		viewGranularity = schema.AnalyticsGranularityWeek
	default:
		return GrowthResult{}, fmt.Errorf("granularity must be year, month, or iso-week")
	}
	if len(options.SVMs) == 0 && len(options.Volumes) == 0 && options.Since == nil && options.Until == nil {
		if report, used, err := growthFromViews(ctx, store, options, viewGranularity); err != nil {
			return GrowthResult{}, err
		} else if used {
			return report, nil
		}
	}
	result, err := Execute(
		ctx,
		store,
		Query{
			CreatedSince:      options.Since,
			CreatedUntil:      options.Until,
			SVMs:              options.SVMs,
			Volumes:           options.Volumes,
			PathGroups:        options.PathGroups,
			GroupBy:           groupBy,
			IncludeIncomplete: true,
		},
	)
	if err != nil {
		return GrowthResult{}, err
	}
	report := GrowthResult{
		SchemaVersion: 1,
		Granularity:   options.Granularity,
		Watermark:     result.Watermark,
		Explain:       result.Explain,
	}
	for _, group := range result.Groups {
		start, err := growthBucketStart(options.Granularity, group.Dimensions)
		if err != nil {
			return GrowthResult{}, err
		}
		report.Buckets = append(
			report.Buckets,
			GrowthBucket{Start: start.Format(time.RFC3339), Files: group.Files, LogicalBytes: group.LogicalBytes},
		)
	}
	sort.Slice(report.Buckets, func(i, j int) bool { return report.Buckets[i].Start < report.Buckets[j].Start })
	return report, nil
}

func growthFromViews(
	ctx context.Context,
	store Store,
	options GrowthOptions,
	granularity schema.AnalyticsGranularity,
) (GrowthResult, bool, error) {
	pinned, found, err := pinLatest(ctx, store)
	if err != nil || !found {
		return GrowthResult{}, false, err
	}
	scoped, watermark, err := activeViewState(ctx, store, pinned)
	if err != nil {
		return GrowthResult{}, false, err
	}
	report := GrowthResult{
		SchemaVersion: 1,
		Granularity:   options.Granularity,
		Watermark:     watermark,
		Explain:       Explain{Source: "materialized-view"},
	}
	buckets := map[int64]*GrowthBucket{}
	prefixes := [][]byte{schema.GrowthTimePrefix(granularity)}
	if len(options.PathGroups) != 0 {
		prefixes = prefixes[:0]
		for _, path := range options.PathGroups {
			prefixes = append(prefixes, schema.GrowthPathPrefix(path))
		}
	}
	seen := false
	for _, prefix := range prefixes {
		visit := func(kv daemon.KeyValue) error {
			key, err := schema.ParseKey(kv.Key)
			if err != nil {
				return err
			}
			if key.Granularity != schema.HistoryGranularity(granularity) ||
				key.Kind == schema.KeyGrowthTime && key.Tier != schema.TierUnknown {
				return nil
			}
			record, err := schema.UnmarshalAnalyticsAggregateRecord(kv.Value)
			if err != nil {
				return err
			}
			seen = true
			bucket := buckets[key.Timestamp]
			if bucket == nil {
				bucket = &GrowthBucket{Start: time.Unix(0, key.Timestamp).UTC().Format(time.RFC3339)}
				buckets[key.Timestamp] = bucket
			}
			bucket.Files += record.FilesAdded
			bucket.LogicalBytes += record.BytesAdded
			return nil
		}
		var err error
		if scoped {
			err = scanActiveDerivedPrefix(ctx, store, pinned.epoch, prefix, visit)
		} else {
			err = scan(ctx, store, prefix, visit)
		}
		if err != nil {
			return GrowthResult{}, false, err
		}
	}
	if !scoped && !seen {
		return GrowthResult{}, false, nil
	}
	for _, bucket := range buckets {
		report.Buckets = append(report.Buckets, *bucket)
	}
	sort.Slice(report.Buckets, func(i, j int) bool { return report.Buckets[i].Start < report.Buckets[j].Start })
	if !scoped {
		report.Explain.Source = "legacy-unscoped-view"
	}
	return report, true, nil
}

func growthBucketStart(granularity string, dimensions map[string]string) (time.Time, error) {
	parse := func(name string) (int, error) {
		value, err := strconv.Atoi(dimensions[name])
		if err != nil {
			return 0, fmt.Errorf("analytics growth group has invalid %s %q", name, dimensions[name])
		}
		return value, nil
	}
	if granularity == "iso-week" {
		year, err := parse("iso-year")
		if err != nil {
			return time.Time{}, err
		}
		week, err := parse("workweek")
		if err != nil {
			return time.Time{}, err
		}
		date := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
		return date.AddDate(0, 0, -((int(date.Weekday())+6)%7)+(week-1)*7), nil
	}
	year, err := parse("year")
	if err != nil {
		return time.Time{}, err
	}
	month := time.January
	if granularity == "month" {
		value, err := parse("month")
		if err != nil {
			return time.Time{}, err
		}
		month = time.Month(value)
	}
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC), nil
}

type UserStatsOptions struct {
	UIDs        []uint32
	GIDs        []uint32
	Residencies []string
	Since       *int64
	Until       *int64
	GroupBy     string
	Limit       int
}

type UserStatsRow struct {
	ID           uint32 `json:"id"`
	Residency    string `json:"residency"`
	Files        uint64 `json:"files"`
	LogicalBytes uint64 `json:"logical_bytes"`
}

type UserStatsResult struct {
	SchemaVersion int            `json:"schema_version"`
	GroupBy       string         `json:"group_by"`
	Rows          []UserStatsRow `json:"rows"`
	Watermark     WatermarkInfo  `json:"watermark"`
	Explain       Explain        `json:"explain"`
}

func UserStats(ctx context.Context, store Store, options UserStatsOptions) (UserStatsResult, error) {
	dimension := "uid"
	if options.GroupBy == "group" {
		dimension = "gid"
	} else if options.GroupBy != "user" {
		return UserStatsResult{}, fmt.Errorf("group-by must be user or group")
	}
	if options.Limit < 0 {
		return UserStatsResult{}, fmt.Errorf("limit must not be negative")
	}
	residencies := options.Residencies
	viewCompatible := options.Since == nil && options.Until == nil &&
		(dimension == "uid" && len(options.GIDs) == 0 || dimension == "gid" && len(options.UIDs) == 0)
	if viewCompatible {
		if report, used, err := userStatsFromViews(ctx, store, options, dimension); err != nil {
			return UserStatsResult{}, err
		} else if used {
			return report, nil
		}
	}
	result, err := Execute(
		ctx,
		store,
		Query{
			UIDs:              options.UIDs,
			GIDs:              options.GIDs,
			Residencies:       residencies,
			CreatedSince:      options.Since,
			CreatedUntil:      options.Until,
			GroupBy:           []string{dimension, "residency"},
			IncludeIncomplete: true,
		},
	)
	if err != nil {
		return UserStatsResult{}, err
	}
	report := UserStatsResult{
		SchemaVersion: 1,
		GroupBy:       options.GroupBy,
		Watermark:     result.Watermark,
		Explain:       result.Explain,
	}
	for _, group := range result.Groups {
		id, err := strconv.ParseUint(group.Dimensions[dimension], 10, 32)
		if err != nil {
			continue
		}
		report.Rows = append(
			report.Rows,
			UserStatsRow{
				ID:           uint32(id),
				Residency:    group.Dimensions["residency"],
				Files:        group.Files,
				LogicalBytes: group.LogicalBytes,
			},
		)
	}
	sort.Slice(report.Rows, func(i, j int) bool {
		if report.Rows[i].LogicalBytes != report.Rows[j].LogicalBytes {
			return report.Rows[i].LogicalBytes > report.Rows[j].LogicalBytes
		}
		if report.Rows[i].Files != report.Rows[j].Files {
			return report.Rows[i].Files > report.Rows[j].Files
		}
		if report.Rows[i].ID != report.Rows[j].ID {
			return report.Rows[i].ID < report.Rows[j].ID
		}
		return report.Rows[i].Residency < report.Rows[j].Residency
	})
	if options.Limit > 0 && len(report.Rows) > options.Limit {
		report.Rows = report.Rows[:options.Limit]
	}
	return report, nil
}

func userStatsFromViews(
	ctx context.Context,
	store Store,
	options UserStatsOptions,
	dimension string,
) (UserStatsResult, bool, error) {
	pinned, found, err := pinLatest(ctx, store)
	if err != nil || !found {
		return UserStatsResult{}, false, err
	}
	scoped, watermark, err := activeViewState(ctx, store, pinned)
	if err != nil {
		return UserStatsResult{}, false, err
	}
	prefix := schema.UserStatsPrefix()
	allowed := map[uint32]struct{}{}
	if dimension == "gid" {
		prefix = schema.GroupStatsPrefix()
		for _, id := range options.GIDs {
			allowed[id] = struct{}{}
		}
	} else {
		for _, id := range options.UIDs {
			allowed[id] = struct{}{}
		}
	}
	report := UserStatsResult{
		SchemaVersion: 1,
		GroupBy:       options.GroupBy,
		Watermark:     watermark,
		Explain:       Explain{Source: "materialized-view"},
	}
	seen := false
	visit := func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		id := key.UID
		if dimension == "gid" {
			id = key.GID
		}
		if len(allowed) != 0 {
			if _, ok := allowed[id]; !ok {
				return nil
			}
		}
		residency := residencyName(key.Residency)
		if len(options.Residencies) != 0 && !hasString(options.Residencies, residency) {
			return nil
		}
		record, err := schema.UnmarshalAnalyticsSummaryRecord(kv.Value)
		if err != nil {
			return err
		}
		seen = true
		report.Rows = append(
			report.Rows,
			UserStatsRow{ID: id, Residency: residency, Files: record.ActiveFiles, LogicalBytes: record.ActiveBytes},
		)
		return nil
	}
	if scoped {
		err = scanActiveDerivedPrefix(ctx, store, pinned.epoch, prefix, visit)
	} else {
		err = scan(ctx, store, prefix, visit)
	}
	if err != nil {
		return UserStatsResult{}, false, err
	}
	if !scoped && !seen {
		return UserStatsResult{}, false, nil
	}
	sortUserStats(report.Rows)
	if options.Limit > 0 && len(report.Rows) > options.Limit {
		report.Rows = report.Rows[:options.Limit]
	}
	if !scoped {
		report.Explain.Source = "legacy-unscoped-view"
	}
	return report, true, nil
}

func sortUserStats(rows []UserStatsRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].LogicalBytes != rows[j].LogicalBytes {
			return rows[i].LogicalBytes > rows[j].LogicalBytes
		}
		if rows[i].Files != rows[j].Files {
			return rows[i].Files > rows[j].Files
		}
		if rows[i].ID != rows[j].ID {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].Residency < rows[j].Residency
	})
}

type GDPRInode struct {
	FSID           uint32 `json:"fsid"`
	Inode          uint64 `json:"inode"`
	LatestRevision uint64 `json:"latest_revision"`
	Path           string `json:"path"`
	Residency      string `json:"residency"`
}

type GDPRPack struct {
	ID                 string          `json:"id"`
	Tier               string          `json:"tier"`
	Backends           []uint64        `json:"backends"`
	Placements         []GDPRPlacement `json:"placements"`
	RetentionAvailable bool            `json:"retention_available"`
	RetentionUntil     int64           `json:"retention_until,omitempty"`
}

type GDPRPlacement struct {
	Backend      uint64 `json:"backend"`
	State        string `json:"state"`
	StorageClass string `json:"storage_class,omitempty"`
}

type GDPRBlob struct {
	Hash                 string            `json:"hash"`
	ReferenceCount       uint64            `json:"reference_count"`
	FirstSeen            int64             `json:"first_seen"`
	Packs                []GDPRPack        `json:"packs"`
	SurvivingExplanation *GDPRBlobSurvival `json:"surviving_explanation,omitempty"`
}

type GDPRBlobSurvival struct {
	ScopedReferences   uint64               `json:"scoped_references"`
	ExternalReferences uint64               `json:"external_references"`
	WouldSurvive       bool                 `json:"would_survive"`
	ExternalSources    []GDPRExternalSource `json:"external_sources,omitempty"`
	SourcesTruncated   bool                 `json:"sources_truncated"`
}

type GDPRExternalSource struct {
	UID            uint32 `json:"uid"`
	FSID           uint32 `json:"fsid"`
	Inode          uint64 `json:"inode"`
	Generation     uint64 `json:"generation,omitempty"`
	LatestRevision uint64 `json:"latest_revision,omitempty"`
	Path           string `json:"path,omitempty"`
	References     uint64 `json:"references"`
}

type GDPRAuditOptions struct {
	ExplainSurvivingChunks bool
	ExternalSourceLimit    int
}

type GDPRAuditResult struct {
	SchemaVersion int           `json:"schema_version"`
	UID           uint32        `json:"uid"`
	Inodes        []GDPRInode   `json:"inodes"`
	Blobs         []GDPRBlob    `json:"blobs"`
	Unavailable   []string      `json:"unavailable"`
	Watermark     WatermarkInfo `json:"watermark"`
	Explain       Explain       `json:"explain"`
}

func SetUIDExclusionPolicy(
	ctx context.Context,
	store Store,
	uid uint32,
	excluded bool,
	reason string,
	now time.Time,
	runID schema.ID,
) error {
	record := schema.UIDExclusionPolicyRecord{Excluded: excluded, UpdatedAt: now.Unix(), RunID: runID, Reason: reason}
	value, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return store.WriteMutableBatch(
		ctx,
		[]daemon.Mutation{{Key: schema.UIDExclusionPolicyKey(uid), Value: value}},
		nil,
		true,
	)
}

func ExcludedUIDs(ctx context.Context, store Store) (map[uint32]struct{}, error) {
	result := make(map[uint32]struct{})
	err := scan(ctx, store, schema.UIDExclusionPolicyPrefix(), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalUIDExclusionPolicyRecord(kv.Value)
		if err != nil {
			return err
		}
		if record.Excluded {
			result[key.UID] = struct{}{}
		}
		return nil
	})
	return result, err
}

func GDPRAudit(ctx context.Context, store Store, uid uint32) (GDPRAuditResult, error) {
	return GDPRAuditWithOptions(ctx, store, uid, GDPRAuditOptions{})
}

func GDPRAuditWithOptions(
	ctx context.Context,
	store Store,
	uid uint32,
	options GDPRAuditOptions,
) (GDPRAuditResult, error) {
	result := GDPRAuditResult{
		SchemaVersion: 1,
		UID:           uid,
		Unavailable: []string{
			"retention expiry is omitted when authoritative pack or placement metadata has no deadline",
		},
	}
	generation := uint64(0)
	scoped := false
	if pinned, found, err := pinLatest(ctx, store); err != nil {
		return GDPRAuditResult{}, err
	} else if found {
		generation = pinned.epoch
		scoped, result.Watermark, err = activeViewState(ctx, store, pinned)
		if err != nil {
			return GDPRAuditResult{}, err
		}
	}
	result.Explain.Source = "legacy-unscoped-view"
	inodePrefix := schema.UserInodePrefix(uid)
	blobPrefix := schema.UserBlobPrefix(uid)
	if scoped {
		result.Explain.Source = "materialized-view"
		blobPrefix = schema.UserBlobContributionPrefix(uid)
	}
	scanView := func(prefix []byte, visit func(daemon.KeyValue) error) error {
		if scoped {
			return scanActiveDerivedPrefix(ctx, store, generation, prefix, visit)
		}
		return scan(ctx, store, prefix, visit)
	}
	if err := scanView(inodePrefix, func(kv daemon.KeyValue) error {
		return appendGDPRInode(ctx, store, generation, kv, &result)
	}); err != nil {
		return GDPRAuditResult{}, err
	}
	blobIndexes := map[schema.ID]int{}
	if err := scanView(blobPrefix, func(kv daemon.KeyValue) error {
		return appendGDPRBlob(ctx, store, kv, blobIndexes, &result)
	}); err != nil {
		return GDPRAuditResult{}, err
	}
	if options.ExplainSurvivingChunks && scoped {
		if err := explainBlobSurvival(ctx, store, generation, uid, result.Blobs, options.ExternalSourceLimit); err != nil {
			return GDPRAuditResult{}, err
		}
	} else if options.ExplainSurvivingChunks {
		if err := explainBlobSurvivalAuthoritative(ctx, store, uid, result.Blobs, options.ExternalSourceLimit); err != nil {
			return GDPRAuditResult{}, err
		}
	}
	sort.Slice(result.Inodes, func(i, j int) bool {
		if result.Inodes[i].FSID != result.Inodes[j].FSID {
			return result.Inodes[i].FSID < result.Inodes[j].FSID
		}
		return result.Inodes[i].Inode < result.Inodes[j].Inode
	})
	sort.Slice(result.Blobs, func(i, j int) bool { return result.Blobs[i].Hash < result.Blobs[j].Hash })
	return result, nil
}

func appendGDPRInode(ctx context.Context, store Store, generation uint64, kv daemon.KeyValue, result *GDPRAuditResult) error {
	key, err := schema.ParseKey(kv.Key)
	if err != nil {
		return err
	}
	record, err := schema.UnmarshalAnalyticsUserInodeRecord(kv.Value)
	if err != nil {
		return err
	}
	residency, err := inodeResidency(ctx, store, generation, key.FSID, key.Inode)
	if err != nil {
		return err
	}
	result.Inodes = append(result.Inodes, GDPRInode{
		FSID: key.FSID, Inode: key.Inode, LatestRevision: record.LatestRevision,
		Path: record.PathSample, Residency: residency,
	})
	return nil
}

func appendGDPRBlob(ctx context.Context, store Store, kv daemon.KeyValue, indexes map[schema.ID]int, result *GDPRAuditResult) error {
	key, err := schema.ParseKey(kv.Key)
	if err != nil {
		return err
	}
	record, err := schema.UnmarshalAnalyticsUserBlobRecord(kv.Value)
	if err != nil {
		return err
	}
	if index, found := indexes[key.ID]; found {
		result.Blobs[index].ReferenceCount += record.ReferenceCount
		if result.Blobs[index].FirstSeen == 0 || record.FirstSeen < result.Blobs[index].FirstSeen {
			result.Blobs[index].FirstSeen = record.FirstSeen
		}
		return nil
	}
	blob := GDPRBlob{Hash: hex.EncodeToString(key.ID[:]), ReferenceCount: record.ReferenceCount, FirstSeen: record.FirstSeen}
	value, found, err := store.Get(ctx, schema.BlobKey(key.ID))
	if err != nil {
		return err
	}
	if found {
		blobRecord, err := schema.UnmarshalBlobRecord(value)
		if err != nil {
			return err
		}
		seen := map[schema.ID]struct{}{}
		for _, location := range blobRecord.Locations {
			if _, ok := seen[location.PackID]; ok {
				continue
			}
			seen[location.PackID] = struct{}{}
			pack, err := gdprPack(ctx, store, location.PackID)
			if err != nil {
				return err
			}
			blob.Packs = append(blob.Packs, pack)
		}
	}
	sort.Slice(blob.Packs, func(i, j int) bool { return blob.Packs[i].ID < blob.Packs[j].ID })
	indexes[key.ID] = len(result.Blobs)
	result.Blobs = append(result.Blobs, blob)
	return nil
}

func explainBlobSurvival(
	ctx context.Context,
	store Store,
	generation uint64,
	uid uint32,
	blobs []GDPRBlob,
	sourceLimit int,
) error {
	if sourceLimit <= 0 {
		sourceLimit = 20
	}
	byID := make(map[schema.ID]*GDPRBlob, len(blobs))
	for index := range blobs {
		decoded, err := hex.DecodeString(blobs[index].Hash)
		if err != nil || len(decoded) != len(schema.ID{}) {
			if err == nil {
				err = fmt.Errorf("invalid blob hash length %d", len(decoded))
			}
			return err
		}
		var id schema.ID
		copy(id[:], decoded)
		blobs[index].SurvivingExplanation = &GDPRBlobSurvival{}
		byID[id] = &blobs[index]
	}
	return scanActiveDerivedPrefix(ctx, store, generation, []byte("u:blobv1:"), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		blob, found := byID[key.ID]
		if !found {
			return nil
		}
		record, err := schema.UnmarshalAnalyticsUserBlobRecord(kv.Value)
		if err != nil {
			return err
		}
		return addSurvivingReference(ctx, store, generation, uid, key, record, blob.SurvivingExplanation, sourceLimit)
	})
}

func addSurvivingReference(
	ctx context.Context,
	store Store,
	generation uint64,
	uid uint32,
	key schema.ParsedKey,
	record schema.AnalyticsUserBlobRecord,
	explanation *GDPRBlobSurvival,
	sourceLimit int,
) error {
	if key.UID == uid {
		explanation.ScopedReferences += record.ReferenceCount
		return nil
	}
	explanation.ExternalReferences += record.ReferenceCount
	explanation.WouldSurvive = true
	for index := range explanation.ExternalSources {
		source := &explanation.ExternalSources[index]
		if source.UID == key.UID && source.FSID == key.FSID && source.Inode == key.Inode && source.Generation == key.Generation {
			source.References += record.ReferenceCount
			return nil
		}
	}
	if len(explanation.ExternalSources) >= sourceLimit {
		explanation.SourcesTruncated = true
		return nil
	}
	source := GDPRExternalSource{UID: key.UID, FSID: key.FSID, Inode: key.Inode, Generation: key.Generation, References: record.ReferenceCount}
	value, found, err := store.Get(ctx, schema.AnalyticsDerivedKey(generation, schema.UserInodeKey(key.UID, key.FSID, key.Inode)))
	if err != nil {
		return err
	}
	if found && !bytes.Equal(value, analyticsDerivedTombstone) {
		inode, err := schema.UnmarshalAnalyticsUserInodeRecord(value)
		if err != nil {
			return err
		}
		source.LatestRevision, source.Path = inode.LatestRevision, inode.PathSample
	}
	explanation.ExternalSources = append(explanation.ExternalSources, source)
	return nil
}

func explainBlobSurvivalAuthoritative(
	ctx context.Context,
	store Store,
	uid uint32,
	blobs []GDPRBlob,
	sourceLimit int,
) error {
	if sourceLimit <= 0 {
		sourceLimit = 20
	}
	byID := make(map[schema.ID]*GDPRBlob, len(blobs))
	for index := range blobs {
		decoded, err := hex.DecodeString(blobs[index].Hash)
		if err != nil || len(decoded) != len(schema.ID{}) {
			return fmt.Errorf("invalid GDPR blob hash %q", blobs[index].Hash)
		}
		var id schema.ID
		copy(id[:], decoded)
		blobs[index].SurvivingExplanation = &GDPRBlobSurvival{}
		byID[id] = &blobs[index]
	}
	manifestCache := make(map[schema.ID][]schema.ID)
	return scan(ctx, store, []byte("iv:"), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		revision, err := schema.UnmarshalInodeRevision(kv.Value)
		if err != nil {
			return err
		}
		content := revision.ContentIDs
		if revision.ContentMode == schema.ContentManifestRef {
			content, err = gdprManifestContent(ctx, store, revision.ContentManifestID, manifestCache)
			if err != nil {
				return err
			}
		}
		counts := make(map[schema.ID]uint64)
		for _, id := range content {
			if _, relevant := byID[id]; relevant {
				counts[id]++
			}
		}
		for id, count := range counts {
			explanation := byID[id].SurvivingExplanation
			if revision.Known&schema.KnownUID != 0 && revision.UID == uid {
				explanation.ScopedReferences += count
				continue
			}
			explanation.ExternalReferences += count
			explanation.WouldSurvive = true
			if len(explanation.ExternalSources) >= sourceLimit {
				explanation.SourcesTruncated = true
				continue
			}
			explanation.ExternalSources = append(
				explanation.ExternalSources,
				GDPRExternalSource{
					UID:            revision.UID,
					FSID:           key.FSID,
					Inode:          key.Inode,
					LatestRevision: key.Revision,
					Path:           revision.SourcePath,
					References:     count,
				},
			)
		}
		return nil
	})
}

func gdprManifestContent(
	ctx context.Context,
	store Store,
	id schema.ID,
	cache map[schema.ID][]schema.ID,
) ([]schema.ID, error) {
	if content, found := cache[id]; found {
		return content, nil
	}
	firstValue, found, err := store.Get(ctx, schema.ContentManifestKey(id, 0))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("content manifest %x is missing", id)
	}
	first, err := schema.UnmarshalContentManifest(firstValue)
	if err != nil {
		return nil, err
	}
	content := append([]schema.ID(nil), first.ContentIDs...)
	for segment := uint32(1); segment < first.SegmentCount; segment++ {
		value, found, err := store.Get(ctx, schema.ContentManifestKey(id, segment))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("content manifest %x segment %d is missing", id, segment)
		}
		record, err := schema.UnmarshalContentManifest(value)
		if err != nil {
			return nil, err
		}
		content = append(content, record.ContentIDs...)
	}
	cache[id] = content
	return content, nil
}

func inodeResidency(ctx context.Context, store Store, generation uint64, fsid uint32, inode uint64) (string, error) {
	prefix := schema.AnalyticsResidencyKey(fsid, inode, 1)[:15]
	state := "unknown"
	visit := func(kv daemon.KeyValue) error {
		record, err := schema.UnmarshalAnalyticsResidencyRecord(kv.Value)
		if err != nil {
			return err
		}
		name := residencyName(record.State)
		if name == "live" || state == "unknown" {
			state = name
		}
		return nil
	}
	var err error
	if generation != 0 {
		err = scanActiveDerivedPrefix(ctx, store, generation, prefix, visit)
	} else {
		err = scan(ctx, store, prefix, visit)
	}
	if err != nil {
		return "", err
	}
	return state, nil
}

func activeViewState(ctx context.Context, store Store, pinned pinnedGeneration) (bool, WatermarkInfo, error) {
	marker, scoped, err := store.Get(ctx, schema.AnalyticsDerivedGenerationMarkerKey(pinned.epoch))
	if err != nil {
		return false, WatermarkInfo{}, err
	}
	if scoped && (len(marker) != 1 || marker[0] != schema.Version) {
		return false, WatermarkInfo{}, fmt.Errorf(
			"analytics generation %d has an invalid derived-view marker",
			pinned.epoch,
		)
	}
	head, available, err := authoritativeHead(ctx, store)
	if err != nil {
		return false, WatermarkInfo{}, err
	}
	watermark := WatermarkInfo{
		RepositoryGeneration:       pinned.watermark.RepositoryGeneration,
		ClassificationEpoch:        pinned.epoch,
		AppliedCommit:              pinned.watermark.AppliedCommit,
		AppliedAt:                  pinned.watermark.AppliedAt,
		AuthoritativeHead:          head,
		AuthoritativeHeadAvailable: available,
	}
	if head > pinned.watermark.AppliedCommit {
		watermark.LagCommits = head - pinned.watermark.AppliedCommit
	}
	return scoped, watermark, nil
}

func gdprPack(ctx context.Context, store Store, id schema.ID) (GDPRPack, error) {
	result := GDPRPack{ID: hex.EncodeToString(id[:]), Tier: "unknown"}
	if value, found, err := store.Get(ctx, schema.PackKey(id)); err != nil {
		return result, err
	} else if found {
		record, err := schema.UnmarshalPackRecord(value)
		if err != nil {
			return result, err
		}
		result.Tier = packTierName(record.Tier)
		if record.RetentionSource != schema.RetentionUnknown {
			result.RetentionAvailable, result.RetentionUntil = true, record.MinRetentionUntil
		}
	}
	if err := scan(ctx, store, schema.PackPlacementPrefix(id), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalPlacementRecord(kv.Value)
		if err != nil {
			return err
		}
		result.Backends = append(result.Backends, key.Backend)
		result.Placements = append(result.Placements, GDPRPlacement{
			Backend: key.Backend, State: placementStateName(record.State), StorageClass: record.StorageClass,
		})
		if record.RetentionSource != schema.RetentionUnknown && (!result.RetentionAvailable || record.MinRetentionUntil > result.RetentionUntil) {
			result.RetentionAvailable, result.RetentionUntil = true, record.MinRetentionUntil
		}
		return nil
	}); err != nil {
		return result, err
	}
	sort.Slice(result.Backends, func(i, j int) bool { return result.Backends[i] < result.Backends[j] })
	sort.Slice(
		result.Placements,
		func(i, j int) bool { return result.Placements[i].Backend < result.Placements[j].Backend },
	)
	return result, nil
}

func placementStateName(state schema.PlacementState) string {
	switch state {
	case schema.PlacementPending:
		return "pending"
	case schema.PlacementLive:
		return "live"
	case schema.PlacementEvicting:
		return "evicting"
	case schema.PlacementEvicted:
		return "evicted"
	case schema.PlacementFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func packTierName(tier schema.PackTier) string {
	switch tier {
	case schema.TierHot:
		return "hot"
	case schema.TierCold:
		return "cold"
	default:
		return "unknown"
	}
}

type JobStatus struct {
	ID                   string `json:"id"`
	State                string `json:"state"`
	RepositoryGeneration uint64 `json:"repository_generation"`
	ClassificationEpoch  uint64 `json:"classification_epoch"`
	AppliedCommit        uint64 `json:"applied_commit"`
	CompletedSegments    uint64 `json:"completed_segments"`
	RowsScanned          uint64 `json:"rows_scanned"`
	UpdatedAt            int64  `json:"updated_at"`
	Error                string `json:"error,omitempty"`
}

func QueryJobStatus(ctx context.Context, store Store, id schema.ID) (JobStatus, error) {
	record, err := loadJob(ctx, store, id)
	if err != nil {
		return JobStatus{}, err
	}
	return JobStatus{
		ID:                   hex.EncodeToString(id[:]),
		State:                queryJobStateName(record.State),
		RepositoryGeneration: record.RepositoryGeneration,
		ClassificationEpoch:  record.ClassificationEpoch,
		AppliedCommit:        record.AppliedCommit,
		CompletedSegments:    uint64(len(record.CompletedSegments)),
		RowsScanned:          record.RowsScanned,
		UpdatedAt:            record.UpdatedAt,
		Error:                record.Error,
	}, nil
}

func queryJobStateName(state schema.AnalyticsQueryJobState) string {
	switch state {
	case schema.AnalyticsQueryPending:
		return "pending"
	case schema.AnalyticsQueryRunning:
		return "running"
	case schema.AnalyticsQueryComplete:
		return "complete"
	case schema.AnalyticsQueryFailed:
		return "failed"
	case schema.AnalyticsQueryCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

type CacheStatus struct {
	Results        uint64 `json:"results"`
	Heat           uint64 `json:"heat"`
	Views          uint64 `json:"views"`
	ActiveViews    uint64 `json:"active_views,omitempty"`
	StaleViews     uint64 `json:"stale_views,omitempty"`
	ExpiredViews   uint64 `json:"expired_views,omitempty"`
	MalformedViews uint64 `json:"malformed_views,omitempty"`
	Jobs           uint64 `json:"jobs"`
}

type OperationalStatus struct {
	SchemaVersion int             `json:"schema_version"`
	Lifecycle     LifecycleStatus `json:"lifecycle"`
	CatchUp       CatchUpResult   `json:"catch_up"`
	Cache         CacheStatus     `json:"cache"`
}

type LifecycleStatus struct {
	Enabled      bool   `json:"enabled"`
	Generation   uint64 `json:"generation"`
	Facts        uint64 `json:"facts"`
	CacheEntries uint64 `json:"cache_entries"`
	BuiltAt      int64  `json:"built_at"`
	ConfigJSON   string `json:"config_json,omitempty"`
}

func InspectStatus(ctx context.Context, store Store) (OperationalStatus, error) {
	lifecycle, err := Status(ctx, store)
	if err != nil {
		return OperationalStatus{}, err
	}
	catchUp, err := CatchUpStatus(ctx, store)
	if err != nil {
		return OperationalStatus{}, err
	}
	cache, err := InspectCache(ctx, store)
	if err != nil {
		return OperationalStatus{}, err
	}
	lifecycleStatus := LifecycleStatus{
		Enabled:      lifecycle.Enabled,
		Generation:   lifecycle.Generation,
		Facts:        lifecycle.Facts,
		CacheEntries: lifecycle.CacheEntries,
		BuiltAt:      lifecycle.BuiltAt,
		ConfigJSON:   lifecycle.ConfigJSON,
	}
	return OperationalStatus{SchemaVersion: 1, Lifecycle: lifecycleStatus, CatchUp: catchUp, Cache: cache}, nil
}

func InspectCache(ctx context.Context, store Store) (CacheStatus, error) {
	var result CacheStatus
	for _, item := range []struct {
		prefix []byte
		count  *uint64
	}{{[]byte("aq:result:"), &result.Results}, {[]byte("aq:heat:"), &result.Heat}, {[]byte("aq:view:"), &result.Views}, {[]byte("aq:job:"), &result.Jobs}} {
		if err := scan(ctx, store, item.prefix, func(daemon.KeyValue) error { *item.count++; return nil }); err != nil {
			return CacheStatus{}, err
		}
	}
	metadata, err := Status(ctx, store)
	if err != nil {
		return CacheStatus{}, err
	}
	now := time.Now().Unix()
	if err := scan(ctx, store, []byte("aq:view:"), func(kv daemon.KeyValue) error {
		key, keyErr := schema.ParseKey(kv.Key)
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(kv.Value)
		var view viewRecord
		if decodeErr == nil {
			decodeErr = json.Unmarshal(record.Payload, &view)
		}
		if keyErr != nil || decodeErr != nil || len(view.Predicates) == 0 || len(view.Shape) == 0 || len(view.GroupBy) == 0 {
			result.MalformedViews++
		} else if view.ExpiresAt < now {
			result.ExpiredViews++
		} else if key.Value != metadata.Generation || view.ClassificationEpoch != metadata.Generation {
			result.StaleViews++
		} else {
			result.ActiveViews++
		}
		return nil
	}); err != nil {
		return CacheStatus{}, err
	}
	return result, nil
}

func PurgeCache(ctx context.Context, store Store, includeViews, includeJobs, dryRun bool) (LifecycleResult, error) {
	prefixes := [][]byte{[]byte("aq:result:"), []byte("aq:heat:")}
	if includeViews {
		prefixes = append(prefixes, []byte("aq:view:"))
	}
	if includeJobs {
		prefixes = append(prefixes, []byte("aq:job:"))
	}
	removed, err := purgePrefixes(ctx, store, dryRun, prefixes...)
	if err != nil {
		return LifecycleResult{}, err
	}
	metadata, err := Status(ctx, store)
	if err != nil {
		return LifecycleResult{}, err
	}
	if !dryRun {
		metadata.CacheEntries = 0
		value, err := metadata.MarshalBinary()
		if err != nil {
			return LifecycleResult{}, err
		}
		if err := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: schema.AnalyticsMetadataKey(), Value: value}}, nil, false); err != nil {
			return LifecycleResult{}, err
		}
	}
	return LifecycleResult{
		Enabled:    metadata.Enabled,
		Generation: metadata.Generation,
		Facts:      metadata.Facts,
		Removed:    removed,
		BuiltAt:    metadata.BuiltAt,
	}, nil
}
