package daemon

import (
	"testing"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
)

func TestReadCacheStatusMappingPreservesAvailabilityAndTiers(t *testing.T) {
	limit := uint64(4096)
	aggregatePending := uint64(33)
	tierPending := uint64(22)
	status := readCacheStatus(&vaulticdbv1.ReadCacheStatusResponse{
		Revision: 2, Namespace: "cache", AggregateMaxBytes: &limit, UsedBytes: 100,
		ReservedBytes: 20, InflightBytes: 8, DeletionPendingBytes: &aggregatePending, QuotaCoordinationHealthy: true,
		Metrics: &vaulticdbv1.ReadCacheMetrics{Hits: 7, Misses: 3, OriginReadsAvoided: 6},
		Tiers: []*vaulticdbv1.ReadCacheTierStatus{{
			Policy:            &vaulticdbv1.ReadCacheTierPolicy{TierId: "ssd", Enabled: true},
			Confidentiality:   vaulticdbv1.ReadCacheConfidentiality_READ_CACHE_CONFIDENTIALITY_ENCRYPTED,
			RequestedMaxBytes: 2048, UsedBytes: 90, DeletionPendingBytes: &tierPending,
			Metrics: &vaulticdbv1.ReadCacheMetrics{Hits: 5},
		}},
	})
	if !status.Configured || !status.AggregateMaxBytesKnown || status.AggregateMaxBytes != limit {
		t.Fatalf("status availability = %+v", status)
	}
	if status.Metrics.Hits != 7 || status.Metrics.Misses != 3 || status.Metrics.OriginReadsAvoided != 6 {
		t.Fatalf("metrics = %+v", status.Metrics)
	}
	if status.DeletionPendingBytes != 33 || !status.DeletionPendingKnown || status.Tiers[0].DeletionPendingBytes != 22 || !status.Tiers[0].DeletionPendingKnown {
		t.Fatalf("deletion-pending mapping = %+v", status)
	}
	if len(status.Tiers) != 1 || status.Tiers[0].ID != "ssd" || status.Tiers[0].Confidentiality != "encrypted" || status.Tiers[0].Metrics.Hits != 5 {
		t.Fatalf("tiers = %+v", status.Tiers)
	}

	unconfigured := readCacheStatus(&vaulticdbv1.ReadCacheStatusResponse{})
	if unconfigured.Configured || unconfigured.AggregateMaxBytesKnown || unconfigured.DeletionPendingKnown {
		t.Fatalf("unconfigured status = %+v", unconfigured)
	}
}
