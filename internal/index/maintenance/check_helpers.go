package maintenance

import (
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func checkEncryption(ctx context.Context, store Store, result *CheckResult, maxFindings uint) error {
	auditor, ok := store.(EncryptionAuditor)
	if !ok {
		return nil
	}
	audit, err := auditor.CheckEncryption(ctx)
	if err != nil {
		return fmt.Errorf("check metadata encryption: %w", err)
	}
	result.EncryptionEnabled, result.EncryptionAlgorithm = audit.Enabled, audit.Algorithm
	result.EnvelopeGeneration, result.ActiveDEKVersion = audit.EnvelopeGeneration, audit.ActiveDEKVersion
	result.EncryptedObjects = audit.Objects - audit.PlaintextObjects
	result.PlaintextObjects, result.InvalidEncryptedObjects, result.OldDEKObjects = audit.PlaintextObjects, audit.InvalidObjects, audit.OldVersionObjects
	for _, finding := range []struct {
		count uint64
		kind  string
		warn  bool
	}{
		{audit.PlaintextObjects, "metadata_object_plaintext", false},
		{audit.InvalidObjects, "metadata_encryption_invalid", false},
		{audit.OldVersionObjects, "metadata_dek_rewrite_pending", true},
	} {
		if audit.Enabled && finding.count != 0 {
			if finding.warn {
				result.Warnings++
			}
			addFinding(result, maxFindings, Finding{Kind: finding.kind, Key: "*", Want: "0", Got: fmt.Sprint(finding.count)})
		}
	}
	return nil
}

func compareLegacyState(
	ctx context.Context,
	store Store,
	legacy map[string]struct{},
	legacyPacks map[vaultic.ID]uint64,
	slatedb map[string]struct{},
	packs map[vaultic.ID]schema.PackRecord,
	result *CheckResult,
	maxFindings uint,
) error {
	for id, count := range legacyPacks {
		if _, found := packs[id]; found {
			continue
		}
		if _, found, err := store.Get(ctx, schema.PackKey(schema.ID(id))); err != nil {
			return err
		} else if found {
			return fmt.Errorf("pack scan omitted existing pack %s", id.String())
		}
		if count == 0 {
			result.Warnings++
			addFinding(result, maxFindings, Finding{Kind: "catalog_only_pack", Key: id.String(), Got: "zero blob locations"})
		} else {
			result.MissingPacks++
			addFinding(result, maxFindings, Finding{
				Kind: "missing_pack", Key: id.String(), Want: "slatedb", Got: fmt.Sprintf("legacy blobs=%d", count),
			})
		}
	}
	for id := range packs {
		if _, found := legacyPacks[id]; !found {
			result.MissingPacks++
			addFinding(result, maxFindings, Finding{Kind: "missing_pack", Key: id.String(), Want: "legacy"})
		}
	}
	for key := range legacy {
		if _, found := slatedb[key]; !found {
			result.MissingInSlateDB++
			addFinding(result, maxFindings, Finding{Kind: "missing_blob", Key: key})
		}
	}
	for key := range slatedb {
		if _, found := legacy[key]; !found {
			result.MissingInLegacy++
			addFinding(result, maxFindings, Finding{Kind: "unexpected_blob", Key: key})
		}
	}
	return nil
}

func checkOperationalState(
	ctx context.Context,
	store Store,
	options CheckOptions,
	packs map[vaultic.ID]schema.PackRecord,
	result *CheckResult,
) error {
	checkPackOperationalState(packs, result, options.MaxFindings)
	if err := scan(ctx, store, []byte("q:"), func(entry daemon.KeyValue) error {
		record, err := schema.UnmarshalCrawlDebtRecord(entry.Value)
		if err != nil {
			return err
		}
		if record.Status == schema.DebtPending || record.Status == schema.DebtFailed {
			result.PendingCrawlDebt++
			result.Warnings++
			if options.IncludeCrawlDebt {
				parsed, _ := schema.ParseKey(entry.Key)
				addFinding(result, options.MaxFindings, Finding{
					Kind: "crawl_debt", Key: vaultic.ID(parsed.SecondID).String(), Got: record.ErrorClass,
				})
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, prefix := range [][]byte{[]byte("gc:b:"), []byte("gc:p:")} {
		if err := scan(ctx, store, prefix, func(entry daemon.KeyValue) error {
			record, err := schema.UnmarshalGarbageCollectionRecord(entry.Value)
			if err != nil {
				return err
			}
			if record.State == schema.GCCandidate || record.State == schema.GCPendingRevalidation {
				result.GCCandidates++
				addFinding(result, options.MaxFindings, Finding{
					Kind: "unreachable_blob_candidate", Key: fmt.Sprintf("%x", entry.Key),
				})
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return scan(ctx, store, []byte("meta:export-snapshot:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalExportCheckpointRecord(entry.Value)
		if err != nil {
			return err
		}
		switch record.State {
		case schema.ExportPending:
			result.PendingExports++
			result.Warnings++
		case schema.ExportFailed:
			result.FailedExports++
			addFinding(result, options.MaxFindings, Finding{Kind: "stale_export", Key: vaultic.ID(parsed.ID).String()})
		case schema.ExportComplete:
		}
		return nil
	})
}

func checkPackOperationalState(packs map[vaultic.ID]schema.PackRecord, result *CheckResult, maxFindings uint) {
	for id, record := range packs {
		switch record.Type {
		case schema.PackMixed:
			result.MixedPacks++
		case schema.PackUnknown:
			result.UnknownPacks++
			result.Warnings++
			addFinding(result, maxFindings, Finding{Kind: "unknown_pack_type", Key: id.String()})
		case schema.PackData, schema.PackTree:
		}
		if record.Lifecycle == schema.PackImported || record.Lifecycle == schema.PackExportPending {
			result.PendingExports++
			result.Warnings++
		}
	}
}
