package vaultic

import (
	"fmt"

	"github.com/vaultic/vaultic/internal/errors"
)

// This file contains the vaultic extensions to the repository config file.
// The fields are additive JSON members of the config; both restic and rustic
// ignore unknown fields when parsing the config, so repositories stay fully
// compatible in both directions.
//
// The field names intentionally mirror rustic's ConfigFile so that both tools
// agree on the semantics of a repository that sets them.

// ChunkerType identifies the chunker algorithm used for new backups.
type ChunkerType string

const (
	// ChunkerRabin is the default content-defined Rabin-fingerprint chunker.
	ChunkerRabin ChunkerType = "rabin"
	// ChunkerFixedSize cuts files into fixed-size chunks.
	ChunkerFixedSize ChunkerType = "fixed_size"
)

// Default chunking parameters (average chunk size 1 MiB).
const (
	DefaultChunkSize    = 1024 * 1024
	DefaultChunkMinSize = 512 * 1024
	DefaultChunkMaxSize = 8 * 1024 * 1024
)

// Default pack sizing parameters, mirroring rustic's defaults.
const (
	DefaultTreePackSize      = 4 * 1024 * 1024
	DefaultDataPackSize      = 32 * 1024 * 1024
	DefaultPackGrowFactor    = 32
	DefaultPackSizeLimit     = 0 // 0 means "no explicit limit" (backend/uploader cap applies)
	DefaultMinPackToleratePC = 30
	DefaultMaxPackToleratePC = 0 // 0 means larger packs are always tolerated
)

// PackConfig groups the pack sizing parameters for one blob type (tree or
// data packs). Sizes are in bytes; a zero value means "use the default".
type PackConfig struct {
	// Size is the targeted pack size in bytes.
	Size uint64 `json:"size,omitempty"`
	// GrowFactor controls how much the target size grows with the repository
	// size (percentage-like factor; 0 disables growth).
	GrowFactor uint32 `json:"growfactor,omitempty"`
	// SizeLimit is the hard upper bound for a pack size in bytes
	// (0 = unlimited, capped by the implementation maximum).
	SizeLimit uint64 `json:"size_limit,omitempty"`
}

// ChunkerConfig holds the chunker configuration stored in the repository
// config. It only affects newly written data; existing data is unchanged.
type ChunkerConfig struct {
	// Type selects the chunker algorithm ("rabin" or "fixed_size").
	Type ChunkerType `json:"type,omitempty"`
	// ChunkSize is the average (rabin) or exact (fixed_size) chunk size in bytes.
	ChunkSize uint64 `json:"chunk_size,omitempty"`
	// ChunkMinSize is the minimum chunk size in bytes (rabin only).
	ChunkMinSize uint64 `json:"chunk_min_size,omitempty"`
	// ChunkMaxSize is the maximum chunk size in bytes (rabin only).
	ChunkMaxSize uint64 `json:"chunk_max_size,omitempty"`
}

// Validate checks the chunker configuration for consistency.
func (c ChunkerConfig) Validate() error {
	switch c.Type {
	case "", ChunkerRabin, ChunkerFixedSize:
	default:
		return errors.Errorf("invalid chunker type %q, must be one of (rabin|fixed_size)", c.Type)
	}
	if c.Type == ChunkerFixedSize {
		if c.ChunkMinSize != 0 || c.ChunkMaxSize != 0 {
			return errors.New("chunk_min_size/chunk_max_size are not supported for the fixed_size chunker")
		}
		if c.ChunkSize == 0 {
			return errors.New("chunk_size must be set for the fixed_size chunker")
		}
		return nil
	}
	// rabin (or default)
	min, max := c.ChunkMinSize, c.ChunkMaxSize
	if min == 0 {
		min = DefaultChunkMinSize
	}
	if max == 0 {
		max = DefaultChunkMaxSize
	}
	if min > max {
		return errors.Errorf("chunk_min_size (%d) must not exceed chunk_max_size (%d)", min, max)
	}
	return nil
}

// effectiveChunker resolves unset chunker fields to their defaults.
func (c ChunkerConfig) effective() ChunkerConfig {
	out := c
	if out.Type == "" {
		out.Type = ChunkerRabin
	}
	if out.ChunkSize == 0 {
		out.ChunkSize = DefaultChunkSize
	}
	if out.Type == ChunkerRabin {
		if out.ChunkMinSize == 0 {
			out.ChunkMinSize = DefaultChunkMinSize
		}
		if out.ChunkMaxSize == 0 {
			out.ChunkMaxSize = DefaultChunkMaxSize
		}
	}
	return out
}

// Validate checks the pack configuration for consistency.
func (p PackConfig) Validate(name string) error {
	if p.SizeLimit != 0 && p.Size != 0 && p.Size > p.SizeLimit {
		return errors.Errorf("%s pack size (%d) exceeds %s pack size limit (%d)", name, p.Size, name, p.SizeLimit)
	}
	return nil
}

// PackSize returns the effective (size, growfactor, size_limit) triple for
// this pack config, falling back to the given defaults.
func (p PackConfig) PackSize(defaultSize uint64) (size uint64, growFactor uint32, sizeLimit uint64) {
	size = p.Size
	if size == 0 {
		size = defaultSize
	}
	growFactor = p.GrowFactor
	if growFactor == 0 {
		growFactor = DefaultPackGrowFactor
	}
	sizeLimit = p.SizeLimit
	return size, growFactor, sizeLimit
}

// --- accessors on Config -------------------------------------------------

// AppendOnly reports whether the repository is in append-only mode.
func (c Config) AppendOnly() bool {
	return c.AppendOnlyFlag
}

// ExtraVerify reports whether an extra verification (decompressing/decrypting
// data before upload) should be performed. Defaults to true.
func (c Config) ExtraVerifyEnabled() bool {
	if c.ExtraVerify == nil {
		return true
	}
	return *c.ExtraVerify
}

// Chunker returns the effective chunker configuration.
func (c Config) Chunker() ChunkerConfig {
	if c.ChunkerCfg == nil {
		return ChunkerConfig{}.effective()
	}
	return c.ChunkerCfg.effective()
}

// PackSizer returns the effective pack sizing for tree resp. data packs.
func (c Config) TreePackSize() (size, limit uint64, growFactor uint32) {
	s, g, l := c.TreePack.PackSize(DefaultTreePackSize)
	return s, l, g
}

// DataPackSize returns the effective pack sizing for data packs.
func (c Config) DataPackSize() (size, limit uint64, growFactor uint32) {
	s, g, l := c.DataPack.PackSize(DefaultDataPackSize)
	return s, l, g
}

// MinPackSizeToleratePercent returns the tolerated minimum pack size in
// percent of the targeted pack size (default 30).
func (c Config) MinPackSizeToleratePercent() uint32 {
	if c.MinPacksizeToleratePercent == nil {
		return DefaultMinPackToleratePC
	}
	return *c.MinPacksizeToleratePercent
}

// MaxPackSizeToleratePercent returns the tolerated maximum pack size in
// percent of the targeted pack size (0 = unlimited, the default).
func (c Config) MaxPackSizeToleratePercent() uint32 {
	if c.MaxPacksizeToleratePercent == nil {
		return DefaultMaxPackToleratePC
	}
	return *c.MaxPacksizeToleratePercent
}

// Validate checks the extension fields of the config for consistency.
func (c Config) ValidateExtensions() error {
	if err := c.TreePack.Validate("tree"); err != nil {
		return err
	}
	if err := c.DataPack.Validate("data"); err != nil {
		return err
	}
	if c.ChunkerCfg != nil {
		if err := c.ChunkerCfg.Validate(); err != nil {
			return err
		}
	}
	if c.Compression != nil && (*c.Compression < -7 || *c.Compression > 22) {
		return errors.Errorf("invalid compression level %d, allowed levels are -7..22", *c.Compression)
	}
	if c.MinPacksizeToleratePercent != nil && *c.MinPacksizeToleratePercent > 100 {
		return fmt.Errorf("min_packsize_tolerate_percent must be <= 100, got %d", *c.MinPacksizeToleratePercent)
	}
	if c.MaxPacksizeToleratePercent != nil && *c.MaxPacksizeToleratePercent > 100 {
		return fmt.Errorf("max_packsize_tolerate_percent must be <= 100, got %d", *c.MaxPacksizeToleratePercent)
	}
	return nil
}
