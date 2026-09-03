package vaultic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/errors"

	"github.com/otuschhoff/vaultic/internal/debug"

	"github.com/restic/chunker"
)

// Config contains the configuration for a repository.
type Config struct {
	Version           uint        `json:"version"`
	ID                string      `json:"id"`
	ChunkerPolynomial chunker.Pol `json:"chunker_polynomial"`

	// vaultic/rustic extension fields. These are additive, FLAT JSON members;
	// both restic and rustic ignore unknown fields when reading the config.
	// The names and layout mirror rustic's ConfigFile exactly so that both
	// tools agree on a repository's settings. Do not nest them in objects.

	// ChunkerType selects the chunker algorithm ("rabin" or "fixed_size").
	ChunkerType ChunkerType `json:"chunker,omitempty"`
	// ChunkSizeBytes is the average (rabin) or exact (fixed_size) chunk size.
	ChunkSizeBytes uint64 `json:"chunk_size,omitempty"`
	// ChunkMinSizeBytes is the minimum chunk size (rabin only).
	ChunkMinSizeBytes uint64 `json:"chunk_min_size,omitempty"`
	// ChunkMaxSizeBytes is the maximum chunk size (rabin only).
	ChunkMaxSizeBytes uint64 `json:"chunk_max_size,omitempty"`

	// AppendOnlyFlag marks the repository as append-only.
	AppendOnlyFlag bool `json:"append_only,omitempty"`
	// IsHot marks this repository as the hot part of a hot/cold repository.
	IsHot bool `json:"is_hot,omitempty"`
	// Compression is the zstd compression level (-7..22; 0 = off). Nil means
	// "auto" (decided per repository version).
	Compression *int `json:"compression,omitempty"`

	// Tree/Data pack sizing (bytes). A zero size means "use the default".
	TreePackSizeBytes      uint64  `json:"treepack_size,omitempty"`
	TreePackGrowFactor     *uint32 `json:"treepack_growfactor,omitempty"`
	TreePackSizeLimitBytes uint64  `json:"treepack_size_limit,omitempty"`
	DataPackSizeBytes      uint64  `json:"datapack_size,omitempty"`
	DataPackGrowFactor     *uint32 `json:"datapack_growfactor,omitempty"`
	DataPackSizeLimitBytes uint64  `json:"datapack_size_limit,omitempty"`

	// MinPacksizeToleratePercent is the tolerated minimum pack size in
	// percent of the target (prune --repack-small threshold).
	MinPacksizeToleratePercent *uint32 `json:"min_packsize_tolerate_percent,omitempty"`
	// MaxPacksizeToleratePercent is the tolerated maximum pack size in
	// percent of the target (0 = unlimited).
	MaxPacksizeToleratePercent *uint32 `json:"max_packsize_tolerate_percent,omitempty"`
	// ExtraVerify controls verification of data before upload (default true).
	ExtraVerify *bool `json:"extra_verify,omitempty"`

	// PlacementBackends declares the durable placement model. Empty means the
	// repository uses the existing single-backend or hot/cold behavior inferred
	// from how it was opened. A single declared backend must resolve to the same
	// behavior as an empty declaration in a non-hot/cold repository.
	PlacementBackends []PlacementBackend `json:"placement_backends,omitempty"`
	// StagingBackends names placement backends that mirror authenticated deferred-ingest journals.
	StagingBackends []string `json:"staging_backends,omitempty"`
	// StagingQuota bounds deferred jobs while authoritative metadata is unavailable.
	StagingQuota StagingQuota `json:"staging_quota,omitempty"`
	// PlacementPolicy describes how many independent live placements a pack must
	// retain before an eviction can proceed.
	PlacementPolicy PlacementPolicy `json:"placement_policy,omitempty"`
	// PathIndexPaths opts selected paths or subtrees into the derived pv: path
	// history index. Empty means Phase 13's immutable walk remains the source.
	PathIndexPaths []string `json:"path_index_paths,omitempty"`
	// AnalyticsConfig supplies deterministic defaults for the optional,
	// rebuildable creation-analytics index. Its enabled state is stored in the
	// index database so it can be changed without rewriting repository config.
	AnalyticsSVMDepth          int      `json:"analytics_svm_depth,omitempty"`
	AnalyticsVolumeDepth       int      `json:"analytics_volume_depth,omitempty"`
	AnalyticsPathGroupDepth    int      `json:"analytics_path_group_depth,omitempty"`
	AnalyticsPathGroupPrefixes []string `json:"analytics_path_group_prefixes,omitempty"`
	AnalyticsCacheAfter        uint64   `json:"analytics_cache_after,omitempty"`
	AnalyticsCacheTTLSeconds   int64    `json:"analytics_cache_ttl_seconds,omitempty"`

	// PrunePlan is a durable deferred-cleanup marker. It is an additive config
	// extension: restic and rustic ignore unknown config fields, while vaultic
	// uses it to revalidate exactly which indexes and packs a prior prune may
	// delete. It is intentionally optional and never required to open a repo.
	PrunePlan *PrunePlan `json:"prune_plan,omitempty"`
}

type PlacementBackend struct {
	ID                   string  `json:"id"`
	Location             string  `json:"location,omitempty"`
	Role                 string  `json:"role,omitempty"`
	Ingest               *bool   `json:"ingest,omitempty"`
	ReadEnabled          *bool   `json:"read_enabled,omitempty"`
	Offsite              bool    `json:"offsite,omitempty"`
	FailureDomain        string  `json:"failure_domain,omitempty"`
	CapacityBytes        uint64  `json:"capacity_bytes,omitempty"`
	PricePerGBMonth      float64 `json:"price_per_gb_month,omitempty"`
	PricePerGBEgress     float64 `json:"price_per_gb_egress,omitempty"`
	PricePer1KRequests   float64 `json:"price_per_1k_requests,omitempty"`
	MinRetentionSeconds  uint64  `json:"min_retention_seconds,omitempty"`
	RetrievalClass       string  `json:"retrieval_class,omitempty"`
	MaxBandwidthBytes    uint64  `json:"max_bandwidth_bytes,omitempty"`
	MaxRequestsPerSecond uint64  `json:"max_requests_per_second,omitempty"`
	ObjectOverheadBytes  uint64  `json:"object_overhead_bytes,omitempty"`
	TargetPackSizeBytes  uint64  `json:"target_pack_size_bytes,omitempty"`
}

type PlacementPolicy struct {
	MinCopies                 uint  `json:"min_copies,omitempty"`
	MinDomains                uint  `json:"min_domains,omitempty"`
	MinOffsite                uint  `json:"min_offsite,omitempty"`
	OffsiteDeadline           int64 `json:"offsite_deadline_seconds,omitempty"`
	PromotionCrossoverSeconds int64 `json:"promotion_crossover_seconds,omitempty"`
}

type StagingQuota struct {
	MaxBytes            uint64 `json:"max_bytes,omitempty"`
	MaxJobs             uint64 `json:"max_jobs,omitempty"`
	MaxAgeSeconds       int64  `json:"max_age_seconds,omitempty"`
	MaxExtensionSeconds int64  `json:"max_extension_seconds,omitempty"`
}

// PrunePlan records the immutable candidates produced after prune has uploaded
// replacement indexes. Deletion code must revalidate RequiredIndexes, remove
// only IndexIDs, reload the index, and then remove only PackIDs.
type PrunePlan struct {
	Version uint   `json:"version"`
	ID      string `json:"id"`
	// State is "phase_a" while a shared-lock prune is building replacement
	// objects and "ready" once exact deletion candidates are durable.
	State           string    `json:"state"`
	CreatedAt       time.Time `json:"created_at"`
	ObservedIndexes IDs       `json:"observed_indexes"`
	RequiredIndexes IDs       `json:"required_indexes"`
	IndexIDs        IDs       `json:"index_ids"`
	PackIDs         IDs       `json:"pack_ids"`
}

const MinRepoVersion = 1
const MaxRepoVersion = 2

// StableRepoVersion is the version that is written to the config when a repository
// is newly created with Init().
const StableRepoVersion = 2

// CreateConfig creates a config file with a randomly selected polynomial and
// ID.
func CreateConfig(version uint, pol *chunker.Pol) (Config, error) {
	var (
		err error
		cfg Config
	)

	if pol == nil {
		cfg.ChunkerPolynomial, err = chunker.RandomPolynomial()
		if err != nil {
			return Config{}, errors.Wrap(err, "chunker.RandomPolynomial")
		}
	} else {
		cfg.ChunkerPolynomial = *pol
	}

	cfg.ID = NewRandomID().String()
	cfg.Version = version

	debug.Log("New config: %#v", cfg)
	return cfg, nil
}

var checkPolynomial = true
var checkPolynomialOnce sync.Once

// TestDisableCheckPolynomial disables the check that the polynomial used for
// the chunker.
func TestDisableCheckPolynomial(t testing.TB) {
	t.Logf("disabling check of the chunker polynomial")
	checkPolynomialOnce.Do(func() {
		checkPolynomial = false
	})
}

// LoadConfig returns loads, checks and returns the config for a repository.
func LoadConfig(ctx context.Context, r LoaderUnpacked) (Config, error) {
	var (
		cfg Config
	)

	err := LoadJSONUnpacked(ctx, r, ConfigFile, ID{}, &cfg)
	if err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (cfg Config) Validate() error {
	if cfg.Version < MinRepoVersion || cfg.Version > MaxRepoVersion {
		return errors.Errorf("unsupported repository version %v", cfg.Version)
	}

	if checkPolynomial {
		if !cfg.ChunkerPolynomial.Irreducible() {
			return errors.New("invalid chunker polynomial")
		}
	}

	if err := cfg.ValidateExtensions(); err != nil {
		return err
	}
	return nil
}

func SaveConfig(ctx context.Context, r SaverUnpacked[FileType], cfg Config) error {
	_, err := SaveJSONUnpacked(ctx, r, ConfigFile, cfg)
	return err
}
