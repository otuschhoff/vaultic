package vaultic

import (
	"strings"

	"github.com/vaultic/vaultic/internal/errors"
)

// This file contains the vaultic extensions to the repository config file.
// The fields are additive, flat JSON members of the config; both restic and
// rustic ignore unknown fields when parsing the config, so repositories stay
// fully compatible in both directions.
//
// IMPORTANT: the field names and layout intentionally mirror rustic's
// ConfigFile exactly (flat keys, `chunker` is the algorithm name as a plain
// string). Do NOT nest these under objects - rustic cannot deserialize that.

// ChunkerType identifies the chunker algorithm used for new backups.
type ChunkerType string

const (
	// ChunkerRabin is the default content-defined Rabin-fingerprint chunker.
	// The value matches rustic's serde serialization of the Chunker enum.
	ChunkerRabin ChunkerType = "Rabin"
	// ChunkerFixedSize cuts files into fixed-size chunks (rustic: `FixedSize`).
	ChunkerFixedSize ChunkerType = "FixedSize"
)

// normalizeChunker maps a user-supplied/CLI chunker name (rabin, fixed_size)
// onto the canonical serialized form (Rabin, FixedSize).
func normalizeChunker(t ChunkerType) ChunkerType {
	switch strings.ToLower(string(t)) {
	case "rabin":
		return ChunkerRabin
	case "fixed_size", "fixedsize":
		return ChunkerFixedSize
	default:
		return t
	}
}

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
	DefaultMinPackToleratePC = 30
	DefaultMaxPackToleratePC = 0 // 0 means larger packs are always tolerated
)

// packSizer resolves a (size, growfactor, sizeLimit) triple with defaults.
func packSizer(size uint64, growfactor *uint32, sizeLimit, defaultSize uint64) (uint64, uint32, uint64) {
	if size == 0 {
		size = defaultSize
	}
	gf := uint32(DefaultPackGrowFactor)
	if growfactor != nil {
		gf = *growfactor
	}
	return size, gf, sizeLimit
}

// --- accessors on Config -------------------------------------------------

// AppendOnly reports whether the repository is in append-only mode.
func (c Config) AppendOnly() bool {
	return c.AppendOnlyFlag
}

// ExtraVerifyEnabled reports whether data is verified before upload
// (default true).
func (c Config) ExtraVerifyEnabled() bool {
	if c.ExtraVerify == nil {
		return true
	}
	return *c.ExtraVerify
}

// Chunker returns the effective chunker type (canonical serialized form).
func (c Config) Chunker() ChunkerType {
	if c.ChunkerType == "" {
		return ChunkerRabin
	}
	return normalizeChunker(c.ChunkerType)
}

// ChunkSize returns the effective average (rabin) or fixed chunk size.
func (c Config) ChunkSize() uint64 {
	if c.ChunkSizeBytes == 0 {
		return DefaultChunkSize
	}
	return c.ChunkSizeBytes
}

// ChunkMinSize returns the effective minimum chunk size (rabin).
func (c Config) ChunkMinSize() uint64 {
	if c.ChunkMinSizeBytes == 0 {
		return DefaultChunkMinSize
	}
	return c.ChunkMinSizeBytes
}

// ChunkMaxSize returns the effective maximum chunk size (rabin).
func (c Config) ChunkMaxSize() uint64 {
	if c.ChunkMaxSizeBytes == 0 {
		return DefaultChunkMaxSize
	}
	return c.ChunkMaxSizeBytes
}

// TreePackSize returns the effective tree pack (size, limit, growfactor).
func (c Config) TreePackSize() (size, limit uint64, growFactor uint32) {
	s, g, l := packSizer(c.TreePackSizeBytes, c.TreePackGrowFactor, c.TreePackSizeLimitBytes, DefaultTreePackSize)
	return s, l, g
}

// DataPackSize returns the effective data pack (size, limit, growfactor).
func (c Config) DataPackSize() (size, limit uint64, growFactor uint32) {
	s, g, l := packSizer(c.DataPackSizeBytes, c.DataPackGrowFactor, c.DataPackSizeLimitBytes, DefaultDataPackSize)
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
	switch normalizeChunker(c.Chunker()) {
	case ChunkerRabin, ChunkerFixedSize:
	default:
		return errors.Errorf("invalid chunker type %q, must be one of (rabin|fixed_size)", c.ChunkerType)
	}
	if normalizeChunker(c.Chunker()) == ChunkerFixedSize {
		if c.ChunkMinSizeBytes != 0 || c.ChunkMaxSizeBytes != 0 {
			return errors.New("chunk_min_size/chunk_max_size are not supported for the fixed_size chunker")
		}
		if c.ChunkSizeBytes == 0 {
			return errors.New("chunk_size must be set for the fixed_size chunker")
		}
	} else if c.ChunkMinSize() > c.ChunkMaxSize() {
		return errors.Errorf("chunk_min_size (%d) must not exceed chunk_max_size (%d)", c.ChunkMinSize(), c.ChunkMaxSize())
	}

	if err := checkPack("tree", c.TreePackSizeBytes, c.TreePackSizeLimitBytes); err != nil {
		return err
	}
	if err := checkPack("data", c.DataPackSizeBytes, c.DataPackSizeLimitBytes); err != nil {
		return err
	}

	if c.Compression != nil && (*c.Compression < -7 || *c.Compression > 22) {
		return errors.Errorf("invalid compression level %d, allowed levels are -7..22", *c.Compression)
	}
	if c.MinPacksizeToleratePercent != nil && *c.MinPacksizeToleratePercent > 100 {
		return errors.Errorf("min_packsize_tolerate_percent must be <= 100, got %d", *c.MinPacksizeToleratePercent)
	}
	if c.MaxPacksizeToleratePercent != nil && *c.MaxPacksizeToleratePercent > 100 {
		return errors.Errorf("max_packsize_tolerate_percent must be <= 100, got %d", *c.MaxPacksizeToleratePercent)
	}
	return nil
}

func checkPack(name string, size, limit uint64) error {
	if size != 0 && limit != 0 && size > limit {
		return errors.Errorf("%s pack size (%d) exceeds %s pack size limit (%d)", name, size, name, limit)
	}
	return nil
}
