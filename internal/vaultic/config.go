package vaultic

import (
	"context"
	"sync"
	"testing"

	"github.com/vaultic/vaultic/internal/errors"

	"github.com/vaultic/vaultic/internal/debug"

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

	if cfg.Version < MinRepoVersion || cfg.Version > MaxRepoVersion {
		return Config{}, errors.Errorf("unsupported repository version %v", cfg.Version)
	}

	if checkPolynomial {
		if !cfg.ChunkerPolynomial.Irreducible() {
			return Config{}, errors.New("invalid chunker polynomial")
		}
	}

	if err := cfg.ValidateExtensions(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func SaveConfig(ctx context.Context, r SaverUnpacked[FileType], cfg Config) error {
	_, err := SaveJSONUnpacked(ctx, r, ConfigFile, cfg)
	return err
}
