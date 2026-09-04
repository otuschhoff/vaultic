package analytics

import (
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type ConsistencyFinding struct {
	Kind string
	Key  string
	Want string
	Got  string
}

type consistencyActiveFact struct {
	fact         schema.AnalyticsFactRecord
	identity     segmentIdentity
	lastComplete int64
}

type consistencyChecker struct {
	ctx               context.Context
	store             Store
	metadata          schema.AnalyticsMetadataRecord
	watermark         *schema.AnalyticsWatermarkRecord
	manifest          *schema.AnalyticsManifestRecord
	layers            []schema.AnalyticsManifestRecord
	segments          []uint64
	activeSegments    map[uint64]struct{}
	dictionaries      map[schema.AnalyticsDictionaryKind]map[uint32]string
	activeFacts       []consistencyActiveFact
	expectedIndexKeys map[string]struct{}
	facts             uint64
	findings          []ConsistencyFinding
}

func CheckConsistency(ctx context.Context, store Store) ([]ConsistencyFinding, error) {
	metadataKey := schema.AnalyticsMetadataKey()
	value, found, err := store.Get(ctx, metadataKey)
	if err != nil {
		return []ConsistencyFinding{{Kind: "unreadable", Key: string(metadataKey), Want: "readable analytics metadata", Got: err.Error()}}, nil
	}
	if !found {
		return nil, nil
	}
	metadata, err := schema.UnmarshalAnalyticsMetadataRecord(value)
	if err != nil {
		return []ConsistencyFinding{{Kind: "unreadable", Key: string(metadataKey), Want: "decodable analytics metadata", Got: err.Error()}}, nil
	}
	if !metadata.Enabled {
		return nil, nil
	}
	checker := newConsistencyChecker(ctx, store, metadata)
	checker.checkRootRecords()
	if checker.manifest != nil {
		checker.checkManifestChain()
		checker.checkCompletionMarkers()
	}
	checks := []func() error{
		checker.checkDictionaries,
		checker.checkSegments,
		checker.checkMaterializations,
		checker.checkGDPR,
		checker.checkIndexCatalog,
		checker.checkOverlayCatalog,
		checker.checkOutbox,
		checker.checkJobs,
		checker.checkViews,
		checker.checkCache,
	}
	for _, check := range checks {
		if err := check(); err != nil {
			return nil, err
		}
	}
	return checker.findings, nil
}

func newConsistencyChecker(ctx context.Context, store Store, metadata schema.AnalyticsMetadataRecord) *consistencyChecker {
	return &consistencyChecker{
		ctx:               ctx,
		store:             store,
		metadata:          metadata,
		activeSegments:    map[uint64]struct{}{},
		dictionaries:      map[schema.AnalyticsDictionaryKind]map[uint32]string{},
		activeFacts:       make([]consistencyActiveFact, 0, metadata.Facts),
		expectedIndexKeys: map[string]struct{}{},
	}
}

func (checker *consistencyChecker) add(kind string, key []byte, want, got string) {
	checker.findings = append(checker.findings, ConsistencyFinding{Kind: kind, Key: string(key), Want: want, Got: got})
}

func (checker *consistencyChecker) unreadable(family string, key []byte, want string, err error) {
	checker.add("unreadable", key, want, err.Error())
	if family != "" {
		checker.add(family, key, want, err.Error())
	}
}

func (checker *consistencyChecker) get(key []byte, want string) ([]byte, bool) {
	value, found, err := checker.store.Get(checker.ctx, key)
	if err != nil {
		checker.unreadable("", key, want, err)
		return nil, false
	}
	return value, found
}

func (checker *consistencyChecker) getDerived(key []byte, want string) ([]byte, bool) {
	value, found, err := getActiveDerived(checker.ctx, checker.store, checker.metadata.Generation, key)
	if err != nil {
		checker.unreadable("", key, want, err)
		return nil, false
	}
	return value, found
}

func firstConsistencyError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("unknown consistency decoding error")
}

func (checker *consistencyChecker) checkRootRecords() {
	watermarkKey := schema.AnalyticsWatermarkKey(checker.metadata.Generation)
	if value, found := checker.get(watermarkKey, "readable active watermark"); found {
		watermark, err := schema.UnmarshalAnalyticsWatermarkRecord(value)
		if err != nil {
			checker.unreadable("analytics_watermark_malformed", watermarkKey, "decodable watermark", err)
		} else {
			checker.watermark = &watermark
		}
	} else {
		checker.add("analytics_watermark_missing", watermarkKey, "active watermark", "missing")
	}
	manifestKey := schema.AnalyticsManifestKey(checker.metadata.Generation)
	value, found := checker.get(manifestKey, "readable active manifest")
	if !found {
		checker.add("analytics_manifest_missing", manifestKey, "active manifest", "missing")
		return
	}
	manifest, err := schema.UnmarshalAnalyticsManifestRecord(value)
	if err != nil {
		checker.unreadable("analytics_manifest_malformed", manifestKey, "decodable manifest", err)
		return
	}
	checker.manifest = &manifest
	if checker.watermark != nil && (checker.watermark.ManifestGeneration != manifest.Generation ||
		manifest.Generation != checker.metadata.Generation || checker.watermark.RepositoryGeneration != checker.metadata.Generation) {
		checker.add("analytics_generation_mismatch", manifestKey,
			fmt.Sprintf("metadata=watermark=manifest=%d", checker.metadata.Generation),
			fmt.Sprintf("repository=%d watermark=%d manifest=%d", checker.watermark.RepositoryGeneration,
				checker.watermark.ManifestGeneration, manifest.Generation))
	}
}

func (checker *consistencyChecker) checkManifestChain() {
	current := *checker.manifest
	checker.layers = append(checker.layers, current)
	checker.segments = append(checker.segments, current.Segments...)
	for depth := 0; current.ParentGeneration != 0; depth++ {
		key := schema.AnalyticsManifestKey(current.ParentGeneration)
		if depth >= maxManifestLayerDepth || current.LayerDepth == 0 {
			checker.add("analytics_manifest_chain_invalid", key, "valid bounded acyclic parent chain", "parent chain exceeds bound")
			checker.clearManifestChain()
			return
		}
		value, found := checker.get(key, "readable manifest parent")
		if !found {
			checker.add("analytics_manifest_chain_invalid", key, "valid bounded acyclic parent chain", "parent missing")
			checker.clearManifestChain()
			return
		}
		parent, err := schema.UnmarshalAnalyticsManifestRecord(value)
		if err != nil {
			checker.unreadable("analytics_manifest_chain_invalid", key, "decodable manifest parent", err)
			checker.clearManifestChain()
			return
		}
		if parent.LayerDepth+1 != current.LayerDepth {
			checker.add("analytics_manifest_chain_invalid", key, "valid bounded acyclic parent chain", "parent depth is invalid")
			checker.clearManifestChain()
			return
		}
		checker.layers = append(checker.layers, parent)
		checker.segments = append(append([]uint64(nil), parent.Segments...), checker.segments...)
		current = parent
	}
	for _, segment := range checker.segments {
		checker.activeSegments[segment] = struct{}{}
	}
}

func (checker *consistencyChecker) clearManifestChain() {
	checker.layers = nil
	checker.segments = nil
	checker.activeSegments = map[uint64]struct{}{}
}

func (checker *consistencyChecker) checkCompletionMarkers() {
	for _, layer := range checker.layers {
		key := schema.AnalyticsDerivedGenerationMarkerKey(layer.Generation)
		value, found := checker.get(key, "readable generation completion marker")
		if !found || len(value) != 1 || value[0] != schema.Version {
			checker.add("analytics_completion_marker_invalid", key, "generation completion marker", "missing or malformed")
		}
	}
}

func (checker *consistencyChecker) checkDictionaries() error {
	for _, kind := range []schema.AnalyticsDictionaryKind{
		schema.AnalyticsDictionarySVM, schema.AnalyticsDictionaryVolume, schema.AnalyticsDictionaryPathGroup,
	} {
		checker.dictionaries[kind] = map[uint32]string{}
		values := map[string]uint32{}
		err := scan(checker.ctx, checker.store, schema.AnalyticsDictionaryPrefix(kind), func(kv daemon.KeyValue) error {
			key, parseErr := schema.ParseKey(kv.Key)
			record, decodeErr := schema.UnmarshalAnalyticsDictionaryRecord(kv.Value)
			if parseErr != nil || decodeErr != nil {
				checker.unreadable("analytics_dictionary_malformed", kv.Key, "decodable dictionary key and value",
					firstConsistencyError(parseErr, decodeErr))
				return nil
			}
			if previous, duplicate := values[record.Value]; duplicate && previous != key.Ordinal {
				checker.add("analytics_dictionary_duplicate", kv.Key, "one ID per value", fmt.Sprintf("also ID %d", previous))
			}
			values[record.Value] = key.Ordinal
			checker.dictionaries[kind][key.Ordinal] = record.Value
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
