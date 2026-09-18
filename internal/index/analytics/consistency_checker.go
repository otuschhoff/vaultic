package analytics

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type ConsistencyFinding struct {
	Kind string
	Key  string
	Want string
	Got  string
}

type ConsistencyWorkspace interface {
	AddDictionary(schema.AnalyticsDictionaryKind, uint32, string) error
	ForEachDictionary(func(schema.AnalyticsDictionaryKind, uint32, string) error) error
	AddAggregate([]byte, schema.AnalyticsAggregateRecord) error
	AddSummary([]byte, schema.AnalyticsSummaryRecord) error
	AddGDPR([]byte, []byte) error
	ForEachAggregate(func([]byte, schema.AnalyticsAggregateRecord) error) error
	ForEachSummary(func([]byte, schema.AnalyticsSummaryRecord) error) error
	ForEachGDPR(func([]byte, []byte) error) error
}

type consistencyChecker struct {
	ctx             context.Context
	store           Store
	workspace       ConsistencyWorkspace
	metadata        schema.AnalyticsMetadataRecord
	watermark       *schema.AnalyticsWatermarkRecord
	manifest        *schema.AnalyticsManifestRecord
	layers          []schema.AnalyticsManifestRecord
	facts           uint64
	maximumFindings uint
	totalFindings   uint64
	findings        []ConsistencyFinding
}

func CheckConsistency(ctx context.Context, store Store) ([]ConsistencyFinding, error) {
	findings, _, err := CheckConsistencyLimited(ctx, store, 0)
	return findings, err
}

func CheckConsistencyLimited(ctx context.Context, store Store, maximumFindings uint) ([]ConsistencyFinding, uint64, error) {
	return CheckConsistencyWithWorkspace(ctx, store, maximumFindings, nil)
}

func CheckConsistencyWithWorkspace(
	ctx context.Context,
	store Store,
	maximumFindings uint,
	workspace ConsistencyWorkspace,
) ([]ConsistencyFinding, uint64, error) {
	metadataKey := schema.AnalyticsMetadataKey()
	value, found, err := store.Get(ctx, metadataKey)
	if err != nil {
		return []ConsistencyFinding{{Kind: "unreadable", Key: string(metadataKey), Want: "readable analytics metadata", Got: err.Error()}}, 1, nil
	}
	if !found {
		return nil, 0, nil
	}
	metadata, err := schema.UnmarshalAnalyticsMetadataRecord(value)
	if err != nil {
		return []ConsistencyFinding{{Kind: "unreadable", Key: string(metadataKey), Want: "decodable analytics metadata", Got: err.Error()}}, 1, nil
	}
	if !metadata.Enabled {
		return nil, 0, nil
	}
	checker := newConsistencyChecker(ctx, store, metadata)
	checker.maximumFindings = maximumFindings
	checker.workspace = workspace
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
			return nil, checker.totalFindings, err
		}
	}
	return checker.findings, checker.totalFindings, nil
}

func newConsistencyChecker(ctx context.Context, store Store, metadata schema.AnalyticsMetadataRecord) *consistencyChecker {
	return &consistencyChecker{
		ctx: ctx, store: store, metadata: metadata,
	}
}

func (checker *consistencyChecker) add(kind string, key []byte, want, got string) {
	checker.totalFindings++
	finding := ConsistencyFinding{Kind: kind, Key: string(key), Want: want, Got: got}
	if checker.maximumFindings == 0 {
		checker.findings = append(checker.findings, finding)
		return
	}
	index, _ := slices.BinarySearchFunc(checker.findings, finding, compareConsistencyFinding)
	if uint(len(checker.findings)) < checker.maximumFindings {
		checker.findings = slices.Insert(checker.findings, index, finding)
		return
	}
	if index < len(checker.findings) {
		checker.findings = slices.Insert(checker.findings, index, finding)
		checker.findings = checker.findings[:checker.maximumFindings]
	}
}

func compareConsistencyFinding(left, right ConsistencyFinding) int {
	if left.Kind != right.Kind {
		return strings.Compare(left.Kind, right.Kind)
	}
	if left.Key != right.Key {
		return strings.Compare(left.Key, right.Key)
	}
	if left.Want != right.Want {
		return strings.Compare(left.Want, right.Want)
	}
	return strings.Compare(left.Got, right.Got)
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
		current = parent
	}
}

func (checker *consistencyChecker) clearManifestChain() {
	checker.layers = nil
}

func (checker *consistencyChecker) forEachSegment(visit func(uint64) error) error {
	for layer := len(checker.layers) - 1; layer >= 0; layer-- {
		for _, segment := range checker.layers[layer].Segments {
			if err := visit(segment); err != nil {
				return err
			}
		}
	}
	return nil
}

func (checker *consistencyChecker) segmentActive(target uint64) bool {
	for _, layer := range checker.layers {
		if slices.Contains(layer.Segments, target) {
			return true
		}
	}
	return false
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
	values := map[schema.AnalyticsDictionaryKind]map[string]uint32{}
	for _, kind := range []schema.AnalyticsDictionaryKind{
		schema.AnalyticsDictionarySVM, schema.AnalyticsDictionaryVolume, schema.AnalyticsDictionaryPathGroup,
	} {
		values[kind] = map[string]uint32{}
		err := scan(checker.ctx, checker.store, schema.AnalyticsDictionaryPrefix(kind), func(kv daemon.KeyValue) error {
			key, parseErr := schema.ParseKey(kv.Key)
			record, decodeErr := schema.UnmarshalAnalyticsDictionaryRecord(kv.Value)
			if parseErr != nil || decodeErr != nil {
				checker.unreadable("analytics_dictionary_malformed", kv.Key, "decodable dictionary key and value",
					firstConsistencyError(parseErr, decodeErr))
				return nil
			}
			if checker.workspace != nil {
				return checker.workspace.AddDictionary(kind, key.Ordinal, record.Value)
			}
			previous, duplicate := values[kind][record.Value]
			if duplicate && previous != key.Ordinal {
				checker.add("analytics_dictionary_duplicate", kv.Key, "one ID per value", fmt.Sprintf("also ID %d", previous))
			}
			values[kind][record.Value] = key.Ordinal
			return nil
		})
		if err != nil {
			return err
		}
	}
	if checker.workspace == nil {
		return nil
	}
	var previousKind schema.AnalyticsDictionaryKind
	var previousID uint32
	var previousValue string
	return checker.workspace.ForEachDictionary(func(kind schema.AnalyticsDictionaryKind, id uint32, value string) error {
		if kind == previousKind && value == previousValue {
			checker.add("analytics_dictionary_duplicate", schema.AnalyticsDictionaryKey(kind, id), "one ID per value", fmt.Sprintf("also ID %d", previousID))
		}
		previousKind, previousID, previousValue = kind, id, value
		return nil
	})
}
