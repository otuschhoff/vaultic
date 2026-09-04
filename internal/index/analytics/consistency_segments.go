package analytics

import (
	"bytes"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func (checker *consistencyChecker) checkSegments() error {
	for _, segment := range checker.segments {
		checker.checkSegment(segment)
	}
	if checker.facts != checker.metadata.Facts {
		checker.add("analytics_fact_count_mismatch", schema.AnalyticsMetadataKey(),
			fmt.Sprint(checker.metadata.Facts), fmt.Sprint(checker.facts))
	}
	return nil
}

func (checker *consistencyChecker) checkSegment(segment uint64) {
	segmentKey := schema.AnalyticsFactSegmentKey(segment)
	segmentValue, segmentFound := checker.get(segmentKey, "readable fact segment")
	metadataKey := schema.AnalyticsSegmentMetadataKey(segment)
	metadataValue, metadataFound := checker.get(metadataKey, "readable segment metadata")
	if !segmentFound || !metadataFound {
		checker.add("analytics_segment_pair_missing", segmentKey, "fact segment and metadata",
			fmt.Sprintf("segment=%t metadata=%t", segmentFound, metadataFound))
		return
	}
	rows, decodeErr := decodeSegment(segmentValue)
	segmentMetadata, metadataErr := schema.UnmarshalAnalyticsSegmentMetadataRecord(metadataValue)
	if decodeErr != nil || metadataErr != nil {
		checker.unreadable("analytics_segment_malformed", segmentKey, "decodable segment and metadata",
			firstConsistencyError(decodeErr, metadataErr))
		return
	}
	if segmentMetadata.RowCount != uint32(len(rows.Identity)) ||
		segmentMetadata.ClassificationEpoch > checker.metadata.Generation {
		checker.add("analytics_segment_metadata_mismatch", metadataKey,
			fmt.Sprintf("rows=%d epoch<=%d", len(rows.Identity), checker.metadata.Generation),
			fmt.Sprintf("rows=%d epoch=%d", segmentMetadata.RowCount, segmentMetadata.ClassificationEpoch))
	}
	checker.checkSegmentRows(segment, segmentKey, rows)
	checker.checkSegmentIndexes(segment, rows)
	checker.facts += uint64(len(rows.Identity))
}

func (checker *consistencyChecker) checkSegmentRows(segment uint64, segmentKey []byte, rows segmentRows) {
	for row := range rows.Identity {
		checker.checkDictionaryReferences(segmentKey, rows, row)
		identity := rows.Identity[row]
		overlayKey := schema.AnalyticsResidencyKey(identity.FSID, identity.Inode, identity.Generation)
		overlayValue, found := checker.getDerived(overlayKey, "readable residency overlay")
		if !found {
			checker.add("analytics_overlay_missing", overlayKey, fmt.Sprintf("segment=%d row=%d", segment, row), "missing")
			continue
		}
		overlay, err := schema.UnmarshalAnalyticsResidencyRecord(overlayValue)
		if err != nil {
			checker.unreadable("analytics_overlay_mismatch", overlayKey, "decodable residency overlay", err)
			continue
		}
		if overlay.FactSegment != segment || overlay.Row != uint32(row) ||
			overlay.ClassificationEpoch > checker.metadata.Generation {
			checker.add("analytics_overlay_mismatch", overlayKey,
				fmt.Sprintf("segment=%d row=%d epoch<=%d", segment, row, checker.metadata.Generation),
				fmt.Sprintf("segment=%d row=%d epoch=%d", overlay.FactSegment, overlay.Row, overlay.ClassificationEpoch))
			continue
		}
		fact := rowFact(rows, row, checker.dictionaries)
		fact.Residency = overlay.State
		checker.activeFacts = append(checker.activeFacts, consistencyActiveFact{
			fact: fact, identity: identity, lastComplete: overlay.LastCompleteCrawl,
		})
	}
}

func (checker *consistencyChecker) checkDictionaryReferences(segmentKey []byte, rows segmentRows, row int) {
	references := []struct {
		kind schema.AnalyticsDictionaryKind
		id   uint32
	}{
		{schema.AnalyticsDictionarySVM, rows.SVM[row]},
		{schema.AnalyticsDictionaryVolume, rows.Volume[row]},
		{schema.AnalyticsDictionaryPathGroup, rows.PathGroup[row]},
	}
	for _, reference := range references {
		if reference.id != 0 && checker.dictionaries[reference.kind][reference.id] == "" {
			checker.add("analytics_dictionary_reference_missing", segmentKey, "referenced dictionary ID",
				fmt.Sprintf("kind=%d id=%d row=%d", reference.kind, reference.id, row))
		}
	}
}

func (checker *consistencyChecker) checkSegmentIndexes(segment uint64, rows segmentRows) {
	for dimension, values := range indexValues(rows) {
		for value, expectedBitmap := range values {
			key := schema.AnalyticsDimensionIndexKey(dimension, value, segment)
			checker.expectedIndexKeys[string(key)] = struct{}{}
			encoded, found := checker.get(key, "readable dimension index")
			if !found {
				checker.add("analytics_index_missing", key, "dimension index", "missing")
				continue
			}
			checker.checkSegmentIndex(key, rows, expectedBitmap, encoded)
		}
	}
}

func (checker *consistencyChecker) checkSegmentIndex(key []byte, rows segmentRows, expectedBitmap, encoded []byte) {
	index, err := schema.UnmarshalAnalyticsDimensionIndexRecord(encoded)
	if err != nil {
		checker.unreadable("analytics_index_mismatch", key, "decodable dimension index", err)
		return
	}
	bitmap := index.Bitmap
	if index.Codec == schema.AnalyticsCodecZstd {
		bitmap, err = analyticsZstdDecoder.DecodeAll(bitmap, nil)
	} else if index.Codec != schema.AnalyticsCodecRaw {
		err = fmt.Errorf("unsupported bitmap codec %d", index.Codec)
	}
	if err != nil {
		checker.unreadable("analytics_index_mismatch", key, "decodable dimension index bitmap", err)
		return
	}
	logicalBytes := expectedLogicalBytes(rows, expectedBitmap)
	if index.RowCount != uint32(len(rows.Identity)) || index.MatchCount != countBits(expectedBitmap) ||
		index.LogicalBytes != logicalBytes || !bytes.Equal(bitmap, expectedBitmap) {
		checker.add("analytics_index_mismatch", key,
			fmt.Sprintf("rows=%d matches=%d bytes=%d bitmap=%x", len(rows.Identity), countBits(expectedBitmap), logicalBytes, expectedBitmap),
			fmt.Sprintf("rows=%d matches=%d bytes=%d bitmap=%x", index.RowCount, index.MatchCount, index.LogicalBytes, bitmap))
	}
}

func expectedLogicalBytes(rows segmentRows, bitmap []byte) uint64 {
	var logicalBytes uint64
	for row := range rows.Identity {
		if bitSet(bitmap, row) && rows.Identity[row].Known&schema.KnownSize != 0 {
			logicalBytes += rows.Size[row]
		}
	}
	return logicalBytes
}
