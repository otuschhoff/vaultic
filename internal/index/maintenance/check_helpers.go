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

func checkOperationalState(
	ctx context.Context,
	store Store,
	options CheckOptions,
	result *CheckResult,
) error {
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
