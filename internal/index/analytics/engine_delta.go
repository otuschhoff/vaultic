package analytics

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type dictionaries struct {
	ids    map[schema.AnalyticsDictionaryKind]map[string]uint32
	values map[schema.AnalyticsDictionaryKind][]string
}

var analyticsDictionaryKinds = []schema.AnalyticsDictionaryKind{
	schema.AnalyticsDictionarySVM,
	schema.AnalyticsDictionaryVolume,
	schema.AnalyticsDictionaryPathGroup,
}

func marshalDictionaries(dict dictionaries) ([]daemon.Mutation, error) {
	var puts []daemon.Mutation
	for _, kind := range analyticsDictionaryKinds {
		for index, value := range dict.values[kind] {
			if value == "" {
				continue
			}
			encoded, err := (schema.AnalyticsDictionaryRecord{Value: value}).MarshalBinary()
			if err != nil {
				return nil, err
			}
			puts = append(
				puts,
				daemon.Mutation{Key: schema.AnalyticsDictionaryKey(kind, uint32(index+1)), Value: encoded},
			)
		}
	}
	return puts, nil
}

func buildSegment(segment, generation uint64, facts []buildFact, dict dictionaries) ([]daemon.Mutation, error) {
	rows := segmentRows{}
	for _, item := range facts {
		rows.Identity = append(rows.Identity, item.identity)
		rows.UID = append(rows.UID, item.fact.UID)
		rows.GID = append(rows.GID, item.fact.GID)
		rows.CreatedAt = append(rows.CreatedAt, item.fact.CreatedAt)
		rows.Basis = append(rows.Basis, item.fact.CreationBasis)
		rows.Continuity = append(rows.Continuity, item.fact.IdentityContinuity)
		rows.Year = append(rows.Year, item.fact.CalendarYear)
		rows.Month = append(rows.Month, item.fact.CalendarMonth)
		rows.ISOYear = append(rows.ISOYear, item.fact.ISOYear)
		rows.Workweek = append(rows.Workweek, item.fact.Workweek)
		rows.SVM = append(rows.SVM, dict.ids[schema.AnalyticsDictionarySVM][item.fact.SVM])
		rows.Volume = append(rows.Volume, dict.ids[schema.AnalyticsDictionaryVolume][item.fact.Volume])
		rows.PathGroup = append(rows.PathGroup, dict.ids[schema.AnalyticsDictionaryPathGroup][item.fact.PathGroup])
		rows.Size = append(rows.Size, item.fact.LogicalSize)
		rows.SizeLog10 = append(rows.SizeLog10, item.fact.SizeLog10)
	}
	columnValues := []any{
		rows.Identity,
		rows.UID,
		rows.GID,
		rows.CreatedAt,
		rows.Basis,
		rows.Continuity,
		rows.Year,
		rows.Month,
		rows.ISOYear,
		rows.Workweek,
		rows.SVM,
		rows.Volume,
		rows.PathGroup,
		rows.Size,
		rows.SizeLog10,
	}
	record := schema.AnalyticsFactSegmentRecord{RowCount: uint32(len(facts))}
	for index, values := range columnValues {
		data, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		record.Columns = append(
			record.Columns,
			schema.AnalyticsColumn{
				Kind:  schema.AnalyticsColumnKind(index + 1),
				Codec: schema.AnalyticsCodecZstd,
				Data:  analyticsZstdEncoder.EncodeAll(data, nil),
			},
		)
	}
	value, err := record.MarshalBinary()
	if err != nil {
		return nil, err
	}
	metadata := segmentMetadata(generation, facts)
	metadataValue, err := metadata.MarshalBinary()
	if err != nil {
		return nil, err
	}
	puts := []daemon.Mutation{
		{Key: schema.AnalyticsFactSegmentKey(segment), Value: value},
		{Key: schema.AnalyticsSegmentMetadataKey(segment), Value: metadataValue},
	}
	for dimension, values := range indexValues(rows) {
		for present, bitmap := range values {
			matches := countBits(bitmap)
			index := schema.AnalyticsDimensionIndexRecord{
				Codec:      schema.AnalyticsCodecZstd,
				RowCount:   uint32(len(facts)),
				MatchCount: matches,
				Bitmap:     analyticsZstdEncoder.EncodeAll(bitmap, nil),
			}
			for row := range facts {
				if bitSet(bitmap, row) && facts[row].fact.Known&schema.KnownSize != 0 {
					index.LogicalBytes += facts[row].fact.LogicalSize
				}
			}
			encoded, err := index.MarshalBinary()
			if err != nil {
				return nil, err
			}
			puts = append(
				puts,
				daemon.Mutation{Key: schema.AnalyticsDimensionIndexKey(dimension, present, segment), Value: encoded},
			)
		}
	}
	return puts, nil
}

func segmentMetadata(generation uint64, facts []buildFact) schema.AnalyticsSegmentMetadataRecord {
	metadata := schema.AnalyticsSegmentMetadataRecord{
		RowCount:            uint32(len(facts)),
		MinCreatedAt:        facts[0].fact.CreatedAt,
		MaxCreatedAt:        facts[0].fact.CreatedAt,
		MinLogicalSize:      facts[0].fact.LogicalSize,
		MaxLogicalSize:      facts[0].fact.LogicalSize,
		MinRevision:         facts[0].identity.Revision,
		MaxRevision:         facts[0].identity.Revision,
		FirstCommit:         facts[0].identity.Revision,
		LastCommit:          facts[0].identity.Revision,
		ClassificationEpoch: generation,
		CodecParameters:     "json-columns-v1;zstd=3",
	}
	for _, item := range facts[1:] {
		if item.fact.CreatedAt < metadata.MinCreatedAt {
			metadata.MinCreatedAt = item.fact.CreatedAt
		}
		if item.fact.CreatedAt > metadata.MaxCreatedAt {
			metadata.MaxCreatedAt = item.fact.CreatedAt
		}
		if item.fact.LogicalSize < metadata.MinLogicalSize {
			metadata.MinLogicalSize = item.fact.LogicalSize
		}
		if item.fact.LogicalSize > metadata.MaxLogicalSize {
			metadata.MaxLogicalSize = item.fact.LogicalSize
		}
		if item.identity.Revision < metadata.MinRevision {
			metadata.MinRevision = item.identity.Revision
		}
		if item.identity.Revision > metadata.MaxRevision {
			metadata.MaxRevision = item.identity.Revision
		}
		if item.identity.Revision < metadata.FirstCommit {
			metadata.FirstCommit = item.identity.Revision
		}
		if item.identity.Revision > metadata.LastCommit {
			metadata.LastCommit = item.identity.Revision
		}
	}
	return metadata
}

func indexValues(rows segmentRows) map[schema.AnalyticsDimension]map[uint64][]byte {
	result := map[schema.AnalyticsDimension]map[uint64][]byte{}
	add := func(dimension schema.AnalyticsDimension, row int, present bool, value uint64) {
		if !present {
			return
		}
		if result[dimension] == nil {
			result[dimension] = map[uint64][]byte{}
		}
		bitmap := result[dimension][value]
		if bitmap == nil {
			bitmap = make([]byte, (len(rows.Identity)+7)/8)
		}
		bitmap[row/8] |= 1 << uint(row%8)
		result[dimension][value] = bitmap
	}
	for row, identity := range rows.Identity {
		add(schema.AnalyticsDimensionUID, row, identity.Known&schema.KnownUID != 0, uint64(rows.UID[row]))
		add(schema.AnalyticsDimensionGID, row, identity.Known&schema.KnownGID != 0, uint64(rows.GID[row]))
		knownTime := rows.Basis[row] != schema.AnalyticsTimeUnknown
		add(schema.AnalyticsDimensionCalendarYear, row, knownTime, uint64(uint32(rows.Year[row])))
		add(schema.AnalyticsDimensionCalendarMonth, row, knownTime, uint64(rows.Month[row]))
		add(schema.AnalyticsDimensionISOYear, row, knownTime, uint64(uint32(rows.ISOYear[row])))
		add(schema.AnalyticsDimensionWorkweek, row, knownTime, uint64(rows.Workweek[row]))
		add(schema.AnalyticsDimensionSVM, row, rows.SVM[row] != 0, uint64(rows.SVM[row]))
		add(schema.AnalyticsDimensionVolume, row, rows.Volume[row] != 0, uint64(rows.Volume[row]))
		add(schema.AnalyticsDimensionPathGroup, row, rows.PathGroup[row] != 0, uint64(rows.PathGroup[row]))
		add(schema.AnalyticsDimensionSizeLog10, row, identity.Known&schema.KnownSize != 0, uint64(rows.SizeLog10[row]))
		add(schema.AnalyticsDimensionCreationBasis, row, true, uint64(rows.Basis[row]))
		add(schema.AnalyticsDimensionIdentityContinuity, row, true, uint64(rows.Continuity[row]))
	}
	return result
}

func Execute(ctx context.Context, store Store, query Query) (Result, error) {
	if err := query.Validate(); err != nil {
		return Result{}, err
	}
	metadata, err := Status(ctx, store)
	if err != nil {
		return Result{}, err
	}
	if !metadata.Enabled {
		return Result{}, fmt.Errorf("analytics is disabled; enable or rebuild it first")
	}
	pinned, found, err := pinLatest(ctx, store)
	if err != nil {
		return Result{}, err
	}
	if !found {
		hasManifest := false
		if err := scan(ctx, store, schema.AnalyticsManifestPrefix(), func(daemon.KeyValue) error { hasManifest = true; return nil }); err != nil {
			return Result{}, err
		}
		if hasManifest {
			return Result{}, fmt.Errorf("analytics manifest exists without a published watermark")
		}
		result, err := executeLegacy(ctx, store, query)
		result.Explain.LegacyFallback = err == nil
		result.Explain.Source = "legacy-facts"
		return result, err
	}
	return executePinned(ctx, store, query, pinned)
}

func executePinned(ctx context.Context, store Store, query Query, pinned pinnedGeneration) (Result, error) {
	head, headAvailable, err := authoritativeHead(ctx, store)
	if err != nil {
		return Result{}, err
	}
	if query.RequireCurrent && !headAvailable {
		return Result{}, fmt.Errorf(
			"require-current is unavailable: analytics Store does not expose an authoritative metadata head; pinned applied commit is %d",
			pinned.watermark.AppliedCommit,
		)
	}
	if query.RequireCurrent && pinned.watermark.AppliedCommit < head {
		return Result{}, fmt.Errorf(
			"analytics is not current: applied commit %d, authoritative metadata head %d",
			pinned.watermark.AppliedCommit,
			head,
		)
	}
	if headAvailable && pinned.watermark.AppliedCommit < head && !query.AllowStale {
		return Result{}, fmt.Errorf(
			"analytics is stale: applied commit %d, authoritative metadata head %d; use allow-stale or catch up",
			pinned.watermark.AppliedCommit,
			head,
		)
	}
	canonical, err := canonicalQuery(query)
	if err != nil {
		return Result{}, err
	}
	hash := sha256.Sum256(canonical)
	cacheKey := schema.AnalyticsQueryResultKey(
		schema.ID(hash),
		pinned.watermark.RepositoryGeneration,
		pinned.epoch,
		pinned.watermark.AppliedCommit,
	)
	if result, ok, err := readResultCache(ctx, store, cacheKey); err != nil {
		return Result{}, err
	} else if ok {
		result.Cached = true
		return result, nil
	}
	result := Result{
		SchemaVersion: 2,
		Generation:    pinned.manifest.Generation,
		Watermark: WatermarkInfo{
			RepositoryGeneration:       pinned.watermark.RepositoryGeneration,
			ClassificationEpoch:        pinned.epoch,
			AppliedCommit:              pinned.watermark.AppliedCommit,
			AppliedAt:                  pinned.watermark.AppliedAt,
			AuthoritativeHead:          head,
			AuthoritativeHeadAvailable: headAvailable,
		},
		Explain: Explain{Source: "raw-segments"},
	}
	if head > pinned.watermark.AppliedCommit {
		result.Watermark.LagCommits = head - pinned.watermark.AppliedCommit
	}
	viewResult, ok, fallbacks, err := readCompatibleView(ctx, store, query, pinned)
	if err != nil {
		return Result{}, err
	}
	if ok {
		viewResult.Watermark.AuthoritativeHead = head
		viewResult.Watermark.AuthoritativeHeadAvailable = headAvailable
		viewResult.Watermark.LagCommits = result.Watermark.LagCommits
		return viewResult, nil
	}
	result.Explain.ViewFallbacks = fallbacks
	if err := executeRawSegments(ctx, store, query, pinned, &result); err != nil {
		return Result{}, err
	}
	if err := updateCache(ctx, store, query, schema.ID(hash), cacheKey, result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func executeRawSegments(ctx context.Context, store Store, query Query, pinned pinnedGeneration, result *Result) error {
	dict, err := loadDictionaries(ctx, store)
	if err != nil {
		return err
	}
	groups := map[string]*Group{}
	for _, segment := range pinned.manifest.Segments {
		result.Explain.SegmentsConsidered++
		metadataValue, present, err := store.Get(ctx, schema.AnalyticsSegmentMetadataKey(segment))
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("published analytics segment %d has no metadata", segment)
		}
		metadata, err := schema.UnmarshalAnalyticsSegmentMetadataRecord(metadataValue)
		if err != nil {
			return err
		}
		if pruneSegment(metadata, query) {
			result.Explain.SegmentsPruned++
			continue
		}
		if err := executeRawSegment(ctx, store, query, pinned, segment, dict, groups, result); err != nil {
			return err
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Groups = append(result.Groups, *groups[key])
	}
	return nil
}

func executeRawSegment(
	ctx context.Context,
	store Store,
	query Query,
	pinned pinnedGeneration,
	segment uint64,
	dict map[schema.AnalyticsDictionaryKind]map[uint32]string,
	groups map[string]*Group,
	result *Result,
) error {
	segmentValue, present, err := store.Get(ctx, schema.AnalyticsFactSegmentKey(segment))
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("published analytics segment %d is missing", segment)
	}
	rows, err := decodeSegment(segmentValue)
	if err != nil {
		return err
	}
	candidates, used, fallbacks, err := indexedCandidates(ctx, store, segment, rows, query, dict)
	if err != nil {
		return err
	}
	result.Explain.IndexesUsed = append(result.Explain.IndexesUsed, used...)
	result.Explain.IndexFallbacks = append(result.Explain.IndexFallbacks, fallbacks...)
	result.Explain.SegmentsScanned++
	for row := range rows.Identity {
		if candidates != nil && !bitSet(candidates, row) {
			continue
		}
		result.Explain.RowsScanned++
		fact := rowFact(rows, row, dict)
		residencyKey := schema.AnalyticsResidencyKey(
			rows.Identity[row].FSID,
			rows.Identity[row].Inode,
			rows.Identity[row].Generation,
		)
		residencyValue, present, err := getActiveDerived(ctx, store, pinned.epoch, residencyKey)
		if err != nil {
			return err
		}
		if present {
			overlay, err := schema.UnmarshalAnalyticsResidencyRecord(residencyValue)
			if err != nil {
				return err
			}
			if overlay.FactSegment != segment || overlay.Row != uint32(row) {
				continue
			}
			if overlay.ClassificationEpoch <= pinned.epoch {
				fact.Residency = overlay.State
			}
		}
		if !matchesComplete(fact, query) {
			continue
		}
		addFactToResult(fact, query, groups, result)
	}
	return nil
}

func addFactToResult(fact schema.AnalyticsFactRecord, query Query, groups map[string]*Group, result *Result) {
	result.Files++
	if fact.Known&schema.KnownSize != 0 {
		result.LogicalBytes += fact.LogicalSize
	}
	if fact.CreationBasis == schema.AnalyticsTimeUnknown {
		result.UnknownCreationTime++
	}
	dims := dimensions(fact, query.GroupBy)
	if len(dims) == 0 {
		return
	}
	key, _ := json.Marshal(dims)
	group := groups[string(key)]
	if group == nil {
		group = &Group{Dimensions: dims}
		groups[string(key)] = group
	}
	group.Files++
	if fact.Known&schema.KnownSize != 0 {
		group.LogicalBytes += fact.LogicalSize
	}
}

func authoritativeHead(ctx context.Context, store Store) (uint64, bool, error) {
	headStore, ok := store.(metadataHeadStore)
	if !ok {
		return 0, false, nil
	}
	head, err := headStore.MetadataHead(ctx)
	return head, true, err
}

func pinLatest(ctx context.Context, store Store) (pinnedGeneration, bool, error) {
	var result pinnedGeneration
	err := scan(ctx, store, schema.AnalyticsWatermarkPrefix(), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		watermark, err := schema.UnmarshalAnalyticsWatermarkRecord(kv.Value)
		if err != nil {
			return err
		}
		if key.Epoch > result.epoch {
			result.epoch, result.watermark = key.Epoch, watermark
		}
		return nil
	})
	if err != nil || result.epoch == 0 {
		return result, false, err
	}
	value, found, err := store.Get(ctx, schema.AnalyticsManifestKey(result.epoch))
	if err != nil {
		return result, false, err
	}
	if !found {
		return result, false, fmt.Errorf("analytics watermark %d has no manifest", result.epoch)
	}
	result.manifest, err = schema.UnmarshalAnalyticsManifestRecord(value)
	if err != nil {
		return result, false, err
	}
	if result.manifest.Generation != result.watermark.ManifestGeneration {
		return result, false, fmt.Errorf("analytics manifest/watermark generation mismatch")
	}
	segments, err := resolveManifestSegments(ctx, store, result.manifest)
	if err != nil {
		return result, false, err
	}
	result.manifest.Segments = segments
	return result, true, nil
}

func resolveManifestSegments(
	ctx context.Context,
	store Store,
	manifest schema.AnalyticsManifestRecord,
) ([]uint64, error) {
	segments := append([]uint64(nil), manifest.Segments...)
	current := manifest
	for depth := 0; current.ParentGeneration != 0; depth++ {
		if depth >= maxManifestLayerDepth || current.LayerDepth == 0 {
			return nil, fmt.Errorf("analytics manifest parent chain exceeds %d layers", maxManifestLayerDepth)
		}
		value, found, err := store.Get(ctx, schema.AnalyticsManifestKey(current.ParentGeneration))
		if err != nil || !found {
			return nil, errors.Join(
				err,
				fmt.Errorf("analytics manifest parent %d is missing", current.ParentGeneration),
			)
		}
		parent, err := schema.UnmarshalAnalyticsManifestRecord(value)
		if err != nil || parent.LayerDepth+1 != current.LayerDepth {
			return nil, errors.Join(err, fmt.Errorf("analytics manifest parent depth is invalid"))
		}
		segments = append(append([]uint64(nil), parent.Segments...), segments...)
		current = parent
	}
	return segments, nil
}

func decodeSegment(value []byte) (segmentRows, error) {
	record, err := schema.UnmarshalAnalyticsFactSegmentRecord(value)
	if err != nil {
		return segmentRows{}, err
	}
	var rows segmentRows
	targets := []any{
		&rows.Identity,
		&rows.UID,
		&rows.GID,
		&rows.CreatedAt,
		&rows.Basis,
		&rows.Continuity,
		&rows.Year,
		&rows.Month,
		&rows.ISOYear,
		&rows.Workweek,
		&rows.SVM,
		&rows.Volume,
		&rows.PathGroup,
		&rows.Size,
		&rows.SizeLog10,
	}
	if len(record.Columns) != len(targets) {
		return rows, fmt.Errorf("analytics segment has %d columns, want %d", len(record.Columns), len(targets))
	}
	for index, column := range record.Columns {
		if column.Kind != schema.AnalyticsColumnKind(index+1) {
			return rows, fmt.Errorf("unsupported analytics column %d", column.Kind)
		}
		data := column.Data
		if column.Codec == schema.AnalyticsCodecZstd {
			data, err = analyticsZstdDecoder.DecodeAll(data, nil)
			if err != nil {
				return rows, fmt.Errorf("decompress analytics column %d: %w", column.Kind, err)
			}
		} else if column.Codec != schema.AnalyticsCodecRaw {
			return rows, fmt.Errorf("unsupported analytics column codec %d", column.Codec)
		}
		if err := json.Unmarshal(data, targets[index]); err != nil {
			return rows, err
		}
	}
	for _, length := range []int{len(rows.Identity),
		len(rows.UID),
		len(rows.GID),
		len(rows.CreatedAt),
		len(rows.Basis),
		len(rows.Continuity),
		len(rows.Year),
		len(rows.Month),
		len(rows.ISOYear),
		len(rows.Workweek),
		len(rows.SVM),
		len(rows.Volume),
		len(rows.PathGroup),
		len(rows.Size),
		len(rows.SizeLog10)} {
		if length != int(record.RowCount) {
			return rows, fmt.Errorf("analytics column row-count mismatch")
		}
	}
	return rows, nil
}

func rowFact(rows segmentRows, row int, dict map[schema.AnalyticsDictionaryKind]map[uint32]string) schema.AnalyticsFactRecord {
	identity := rows.Identity[row]
	return schema.AnalyticsFactRecord{
		Revision:           identity.Revision,
		UID:                rows.UID[row],
		GID:                rows.GID[row],
		Known:              identity.Known,
		CreatedAt:          rows.CreatedAt[row],
		LogicalSize:        rows.Size[row],
		CalendarYear:       rows.Year[row],
		CalendarMonth:      rows.Month[row],
		ISOYear:            rows.ISOYear[row],
		Workweek:           rows.Workweek[row],
		SizeLog10:          rows.SizeLog10[row],
		SVM:                dictionaryValue(dict, schema.AnalyticsDictionarySVM, rows.SVM[row]),
		Volume:             dictionaryValue(dict, schema.AnalyticsDictionaryVolume, rows.Volume[row]),
		PathGroup:          dictionaryValue(dict, schema.AnalyticsDictionaryPathGroup, rows.PathGroup[row]),
		Residency:          schema.AnalyticsUnknown,
		CreationBasis:      rows.Basis[row],
		IdentityGeneration: identity.Generation,
		IdentityContinuity: rows.Continuity[row],
	}
}

func loadDictionaries(ctx context.Context, store Store) (map[schema.AnalyticsDictionaryKind]map[uint32]string, error) {
	result := map[schema.AnalyticsDictionaryKind]map[uint32]string{}
	for _, kind := range analyticsDictionaryKinds {
		result[kind] = map[uint32]string{}
		if err := scan(ctx, store, schema.AnalyticsDictionaryPrefix(kind), func(kv daemon.KeyValue) error {
			key, err := schema.ParseKey(kv.Key)
			if err != nil {
				return err
			}
			value, err := schema.UnmarshalAnalyticsDictionaryRecord(kv.Value)
			if err != nil {
				return err
			}
			result[kind][key.Ordinal] = value.Value
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func dictionaryValue(
	dict map[schema.AnalyticsDictionaryKind]map[uint32]string,
	kind schema.AnalyticsDictionaryKind,
	id uint32,
) string {
	if id == 0 {
		return "unknown"
	}
	if value := dict[kind][id]; value != "" {
		return value
	}
	return "unknown"
}

func pruneSegment(metadata schema.AnalyticsSegmentMetadataRecord, query Query) bool {
	if query.SizeMin != nil && metadata.MaxLogicalSize < *query.SizeMin {
		return true
	}
	if query.SizeMax != nil && metadata.MinLogicalSize >= *query.SizeMax {
		return true
	}
	return false
}

type indexRequest struct {
	dimension schema.AnalyticsDimension
	values    []uint64
	name      string
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func indexedCandidates(
	ctx context.Context,
	store Store,
	segment uint64,
	rows segmentRows,
	query Query,
	dict map[schema.AnalyticsDictionaryKind]map[uint32]string,
) ([]byte, []string, []string, error) {
	requests := queryIndexes(query, dict)
	var candidates []byte
	var used, fallbacks []string
	for _, request := range requests {
		union := make([]byte, (len(rows.Identity)+7)/8)
		usable := true
		for _, value := range request.values {
			encoded, found, err := store.Get(ctx, schema.AnalyticsDimensionIndexKey(request.dimension, value, segment))
			if err != nil {
				return nil, nil, nil, err
			}
			if !found {
				usable = false
				break
			}
			index, err := schema.UnmarshalAnalyticsDimensionIndexRecord(encoded)
			if err != nil || index.RowCount != uint32(len(rows.Identity)) {
				usable = false
				break
			}
			bitmap := index.Bitmap
			if index.Codec == schema.AnalyticsCodecZstd {
				bitmap, err = analyticsZstdDecoder.DecodeAll(bitmap, nil)
			} else if index.Codec != schema.AnalyticsCodecRaw {
				err = fmt.Errorf("unsupported analytics bitmap codec %d", index.Codec)
			}
			if err != nil || len(bitmap) != len(union) || countBits(bitmap) != index.MatchCount {
				usable = false
				break
			}
			for offset := range union {
				union[offset] |= bitmap[offset]
			}
		}
		if !usable {
			fallbacks = append(fallbacks, request.name+":"+strconv.FormatUint(segment, 10))
			continue
		}
		used = append(used, request.name+":"+strconv.FormatUint(segment, 10))
		if candidates == nil {
			candidates = union
		} else {
			for offset := range candidates {
				candidates[offset] &= union[offset]
			}
		}
	}
	return candidates, used, fallbacks, nil
}

func queryIndexes(query Query, dict map[schema.AnalyticsDictionaryKind]map[uint32]string) []indexRequest {
	var result []indexRequest
	addInts := func(d schema.AnalyticsDimension, name string, values []int) {
		if len(values) == 0 {
			return
		}
		converted := make([]uint64, len(values))
		for index, value := range values {
			converted[index] = uint64(value)
		}
		result = append(result, indexRequest{d, converted, name})
	}
	if len(query.UIDs) != 0 {
		values := make([]uint64, len(query.UIDs))
		for index, value := range query.UIDs {
			values[index] = uint64(value)
		}
		result = append(result, indexRequest{schema.AnalyticsDimensionUID, values, "uid"})
	}
	if len(query.GIDs) != 0 {
		values := make([]uint64, len(query.GIDs))
		for index, value := range query.GIDs {
			values[index] = uint64(value)
		}
		result = append(result, indexRequest{schema.AnalyticsDimensionGID, values, "gid"})
	}
	addInts(schema.AnalyticsDimensionCalendarYear, "year", query.Years)
	addInts(schema.AnalyticsDimensionCalendarMonth, "month", query.Months)
	addInts(schema.AnalyticsDimensionISOYear, "iso-year", query.ISOYears)
	addInts(schema.AnalyticsDimensionWorkweek, "workweek", query.Workweeks)
	addInts(schema.AnalyticsDimensionSizeLog10, "size-log10", query.SizeLog10)
	for _, item := range []struct {
		dimension schema.AnalyticsDimension
		kind      schema.AnalyticsDictionaryKind
		name      string
		values    []string
	}{{schema.AnalyticsDimensionSVM,
		schema.AnalyticsDictionarySVM,
		"svm",
		query.SVMs},
		{schema.AnalyticsDimensionVolume,
			schema.AnalyticsDictionaryVolume,
			"volume",
			query.Volumes},
		{schema.AnalyticsDimensionPathGroup,
			schema.AnalyticsDictionaryPathGroup,
			"path-group",
			query.PathGroups}} {
		if len(item.values) == 0 {
			continue
		}
		var ids []uint64
		for id, value := range dict[item.kind] {
			if hasString(item.values, value) {
				ids = append(ids, uint64(id))
			}
		}
		if len(ids) != 0 {
			result = append(result, indexRequest{item.dimension, ids, item.name})
		}
	}
	return result
}

func matchesComplete(fact schema.AnalyticsFactRecord, query Query) bool {
	if !query.IncludeIncomplete && fact.IdentityContinuity == schema.AnalyticsContinuityUnknown {
		return false
	}
	if !matches(fact, query) {
		return false
	}
	if len(query.CreationBases) != 0 && !hasString(query.CreationBases, creationBasisName(fact.CreationBasis)) {
		return false
	}
	return len(query.IdentityContinuities) == 0 ||
		hasString(query.IdentityContinuities, continuityName(fact.IdentityContinuity))
}

func creationBasisName(value schema.AnalyticsCreationBasis) string {
	switch value {
	case schema.AnalyticsCTime:
		return "ctime"
	case schema.AnalyticsMTime:
		return "mtime"
	case schema.AnalyticsBirthTime:
		return "birth-time"
	case schema.AnalyticsFirstSeen:
		return "first-seen"
	default:
		return "unknown"
	}
}

func continuityName(value schema.AnalyticsIdentityContinuity) string {
	switch value {
	case schema.AnalyticsContinuityProven:
		return "proven"
	case schema.AnalyticsContinuitySourceGeneration:
		return "source-generation"
	default:
		return "unknown"
	}
}

type cacheRecord struct {
	ExpiresAt int64  `json:"expires_at"`
	Result    Result `json:"result"`
}

type heatRecord struct {
	Hits      uint64 `json:"hits"`
	ScanCost  uint64 `json:"scan_cost"`
	UpdatedAt int64  `json:"updated_at"`
}

type viewRecord struct {
	Predicates           []byte   `json:"predicates"`
	Shape                []byte   `json:"shape"`
	GroupBy              []string `json:"group_by"`
	RepositoryGeneration uint64   `json:"repository_generation"`
	ClassificationEpoch  uint64   `json:"classification_epoch"`
	AppliedCommit        uint64   `json:"applied_commit"`
	ExpiresAt            int64    `json:"expires_at"`
	LastUsed             int64    `json:"last_used"`
	Result               Result   `json:"result"`
}

func readCompatibleView(
	ctx context.Context,
	store Store,
	query Query,
	pinned pinnedGeneration,
) (Result, bool, []string, error) {
	predicates, err := canonicalPredicates(query)
	if err != nil {
		return Result{}, false, nil, err
	}
	now := time.Now().Unix()
	var selected *viewRecord
	var selectedKey []byte
	fallbacks := make([]string, 0)
	err = scan(ctx, store, []byte("aq:view:"), func(kv daemon.KeyValue) error {
		record, decodeErr := schema.UnmarshalAnalyticsQueryRecord(kv.Value)
		if decodeErr != nil {
			fallbacks = append(fallbacks, "malformed")
			return nil
		}
		var view viewRecord
		if json.Unmarshal(record.Payload, &view) != nil || len(view.Predicates) == 0 || len(view.GroupBy) == 0 {
			fallbacks = append(fallbacks, "malformed")
			return nil
		}
		if view.RepositoryGeneration != pinned.watermark.RepositoryGeneration ||
			view.ClassificationEpoch != pinned.epoch ||
			view.AppliedCommit != pinned.watermark.AppliedCommit {
			fallbacks = append(fallbacks, "stale-generation")
			return nil
		}
		if view.ExpiresAt < now {
			fallbacks = append(fallbacks, "expired")
			return nil
		}
		if !bytes.Equal(view.Predicates, predicates) || !groupSubset(query.GroupBy, view.GroupBy) {
			fallbacks = append(fallbacks, "incompatible")
			return nil
		}
		if selected == nil || len(view.GroupBy) < len(selected.GroupBy) {
			copy := view
			selected = &copy
			selectedKey = append([]byte(nil), kv.Key...)
		}
		return nil
	})
	if err != nil || selected == nil {
		return Result{}, false, fallbacks, err
	}
	result := rollupView(selected.Result, query.GroupBy)
	result.Cached = false
	result.Explain = Explain{Source: "adaptive-view"}
	selected.LastUsed = now
	payload, _ := json.Marshal(selected)
	if value, marshalErr := (schema.AnalyticsQueryRecord{Payload: payload}).MarshalBinary(); marshalErr == nil {
		if writeErr := store.WriteMutableBatch(ctx, []daemon.Mutation{{Key: selectedKey, Value: value}}, nil, false); writeErr != nil {
			return Result{}, false, fallbacks, writeErr
		}
	} else {
		return Result{}, false, fallbacks, marshalErr
	}
	return result, true, fallbacks, nil
}

func groupSubset(requested, available []string) bool {
	for _, name := range requested {
		if !slices.Contains(available, name) {
			return false
		}
	}
	return true
}

func rollupView(source Result, groupBy []string) Result {
	result := source
	result.Groups = nil
	if len(groupBy) == 0 {
		return result
	}
	groups := map[string]*Group{}
	for _, sourceGroup := range source.Groups {
		dimensions := make(map[string]string, len(groupBy))
		for _, name := range groupBy {
			dimensions[name] = sourceGroup.Dimensions[name]
		}
		key, _ := json.Marshal(dimensions)
		group := groups[string(key)]
		if group == nil {
			group = &Group{Dimensions: dimensions}
			groups[string(key)] = group
		}
		group.Files += sourceGroup.Files
		group.LogicalBytes += sourceGroup.LogicalBytes
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Groups = append(result.Groups, *groups[key])
	}
	return result
}

func readResultCache(ctx context.Context, store Store, key []byte) (Result, bool, error) {
	value, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return Result{}, false, err
	}
	record, err := schema.UnmarshalAnalyticsQueryRecord(value)
	if err != nil {
		return Result{}, false, nil
	}
	var cached cacheRecord
	if json.Unmarshal(record.Payload, &cached) != nil || cached.ExpiresAt < time.Now().Unix() {
		return Result{}, false, nil
	}
	return cached.Result, true, nil
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func updateCache(ctx context.Context, store Store, query Query, hash schema.ID, resultKey []byte, result Result) error {
	unlock, err := lockAnalyticsPublication(store)
	if err != nil {
		return err
	}
	defer unlock()
	metadata, err := Status(ctx, store)
	if err != nil {
		return err
	}
	var config Config
	if metadata.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(metadata.ConfigJSON), &config); err != nil {
			return err
		}
	}
	config = config.normalized()
	heatKey := schema.AnalyticsQueryHeatKey(hash)
	heat := heatRecord{}
	if value, found, err := store.Get(ctx, heatKey); err != nil {
		return err
	} else if found {
		record, err := schema.UnmarshalAnalyticsQueryRecord(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(record.Payload, &heat); err != nil {
			return fmt.Errorf("decode analytics heat record: %w", err)
		}
	}
	heat.Hits++
	heat.ScanCost += result.Explain.RowsScanned
	heat.UpdatedAt = time.Now().Unix()
	heatPayload, _ := json.Marshal(heat)
	heatValue, err := (schema.AnalyticsQueryRecord{Payload: heatPayload}).MarshalBinary()
	if err != nil {
		return err
	}
	puts := []daemon.Mutation{{Key: heatKey, Value: heatValue}}
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if heat.Hits >= config.CacheAfter || heat.ScanCost >= uint64(config.SegmentRows) {
		payload, _ := json.Marshal(cacheRecord{ExpiresAt: time.Now().Unix() + config.CacheTTLSeconds, Result: result})
		value, err := (schema.AnalyticsQueryRecord{Payload: payload}).MarshalBinary()
		if err != nil {
			return err
		}
		puts = append(puts, daemon.Mutation{Key: resultKey, Value: value})
		if len(query.GroupBy) != 0 && len(result.Groups) != 0 && len(result.Groups) <= config.CacheMaxEntries {
			predicates, predicateErr := canonicalPredicates(query)
			shape, shapeErr := canonicalShape(query)
			if predicateErr != nil || shapeErr != nil {
				return errors.Join(predicateErr, shapeErr)
			}
			viewID := schema.ID(sha256.Sum256(append(append([]byte(nil), predicates...), shape...)))
			now := time.Now().Unix()
			view := viewRecord{
				Predicates:           predicates,
				Shape:                shape,
				GroupBy:              append([]string(nil), query.GroupBy...),
				RepositoryGeneration: result.Watermark.RepositoryGeneration,
				ClassificationEpoch:  result.Watermark.ClassificationEpoch,
				AppliedCommit:        result.Watermark.AppliedCommit,
				ExpiresAt:            now + config.CacheTTLSeconds,
				LastUsed:             now,
				Result:               result,
			}
			viewPayload, _ := json.Marshal(view)
			viewValue, marshalErr := (schema.AnalyticsQueryRecord{Payload: viewPayload}).MarshalBinary()
			if marshalErr != nil {
				return marshalErr
			}
			puts = append(
				puts,
				daemon.Mutation{Key: schema.AnalyticsQueryViewKey(viewID, result.Generation), Value: viewValue},
			)
		}
		if heat.Hits == config.CacheAfter && metadata.Generation == result.Generation {
			metadata.CacheEntries++
			metadataValue, err := metadata.MarshalBinary()
			if err != nil {
				return err
			}
			puts = append(puts, daemon.Mutation{Key: schema.AnalyticsMetadataKey(), Value: metadataValue})
		}
	}
	if err := store.WriteMutableBatch(ctx, puts, nil, false); err != nil {
		return fmt.Errorf("write analytics cache: %w", err)
	}
	return cleanupCache(ctx, store, config.CacheMaxEntries)
}
