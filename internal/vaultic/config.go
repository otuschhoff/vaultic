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

	// vaultic/rustic extension fields (additive; unknown fields are ignored
	// by restic and rustic when reading the config).

	// ChunkerCfg holds the chunker configuration for new data.
	ChunkerCfg *ChunkerConfig `json:"chunker,omitempty"`
	// AppendOnlyFlag marks the repository as append-only.
	AppendOnlyFlag bool `json:"append_only,omitempty"`
	// Compression is the zstd compression level (-7..22; 0 = off). Nil means
	// "auto" (decided per repository version).
	Compression *int `json:"compression,omitempty"`
	// TreePack holds the tree pack sizing configuration.
	TreePack PackConfig `json:"treepack,omitempty"`
	// DataPack holds the data pack sizing configuration.
	DataPack PackConfig `json:"datapack,omitempty"`
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
