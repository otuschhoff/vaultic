package vaultic_test

import (
	"context"
	"encoding/json"
	"testing"

	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestConfigExtensionsRoundTrip(t *testing.T) {
	var buf []byte
	save := func(tpe vaultic.FileType, b []byte) (vaultic.ID, error) {
		rtest.Equals(t, vaultic.ConfigFile, tpe)
		buf = b
		return vaultic.ID{}, nil
	}

	cfg, err := vaultic.CreateConfig(vaultic.MaxRepoVersion, nil)
	rtest.OK(t, err)

	comp := 10
	ev := false
	minTol := uint32(85)
	gf := uint32(16)
	cfg.Compression = &comp
	cfg.AppendOnlyFlag = true
	cfg.ExtraVerify = &ev
	cfg.ChunkerType = vaultic.ChunkerFixedSize
	cfg.ChunkSizeBytes = 2 * 1024 * 1024
	cfg.TreePackSizeBytes = 4 * 1024 * 1024
	cfg.TreePackGrowFactor = &gf
	cfg.TreePackSizeLimitBytes = 128 * 1024 * 1024
	cfg.DataPackSizeBytes = 32 * 1024 * 1024
	cfg.MinPacksizeToleratePercent = &minTol
	cfg.PrunePlan = &vaultic.PrunePlan{
		Version:         1,
		ID:              "test-plan",
		ObservedIndexes: vaultic.IDs{vaultic.NewTestID(1)},
		RequiredIndexes: vaultic.IDs{vaultic.NewTestID(2)},
		IndexIDs:        vaultic.IDs{vaultic.NewTestID(3)},
		PackIDs:         vaultic.IDs{vaultic.NewTestID(4)},
	}

	rtest.OK(t, cfg.ValidateExtensions())
	rtest.OK(t, vaultic.SaveConfig(context.TODO(), saver{save}, cfg))

	load := func(tpe vaultic.FileType, id vaultic.ID) ([]byte, error) {
		return buf, nil
	}
	cfg2, err := vaultic.LoadConfig(context.TODO(), loader{load})
	rtest.OK(t, err)
	rtest.Equals(t, cfg, cfg2)
}

// TestConfigIgnoresUnknownFields documents that both vaultic and (verified
// upstream) restic/rustic ignore unknown config keys. If this ever breaks,
// cross-client compatibility of config extensions is broken.
func TestConfigIgnoresUnknownFields(t *testing.T) {
	raw := `{
		"version": 2,
		"id": "aa7b0428fd385f2469f0d49d978bc1613fc1f00786d5ac0d3c7ffb3e0e2c3b10",
		"chunker_polynomial": "2d5a9847e0d0c3",
		"compression": 10,
		"append_only": true,
		"some_future_extension": {"foo": "bar"}
	}`
	load := func(tpe vaultic.FileType, id vaultic.ID) ([]byte, error) {
		return []byte(raw), nil
	}
	cfg, err := vaultic.LoadConfig(context.TODO(), loader{load})
	rtest.OK(t, err)
	rtest.Assert(t, cfg.AppendOnly(), "append_only not parsed")
	rtest.Assert(t, cfg.Compression != nil && *cfg.Compression == 10, "compression not parsed")
}

// TestConfigRusticFieldNames ensures the extension keys exactly match
// rustic's ConfigFile JSON layout (flat keys, chunker as a plain string).
func TestConfigRusticFieldNames(t *testing.T) {
	cfg := vaultic.Config{
		ChunkerType:            vaultic.ChunkerFixedSize,
		ChunkSizeBytes:         1 << 20,
		AppendOnlyFlag:         true,
		TreePackSizeBytes:      4 << 20,
		DataPackSizeBytes:      32 << 20,
		DataPackSizeLimitBytes: 512 << 20,
	}
	rtest.OK(t, cfg.ValidateExtensions())
	data, err := json.Marshal(cfg)
	rtest.OK(t, err)
	var m map[string]any
	rtest.OK(t, json.Unmarshal(data, &m))

	// chunker must be a plain string using rustic's serde variant name
	rtest.Equals(t, "FixedSize", m["chunker"])
	for _, key := range []string{"append_only", "chunk_size", "treepack_size",
		"datapack_size", "datapack_size_limit"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("expected flat key %q in serialized config, got %s", key, data)
		}
	}
}

func TestChunkerValidate(t *testing.T) {
	rtest.OK(t, vaultic.Config{}.ValidateExtensions())
	rtest.OK(t, vaultic.Config{ChunkerType: vaultic.ChunkerRabin}.ValidateExtensions())
	rtest.OK(t, vaultic.Config{ChunkerType: vaultic.ChunkerFixedSize, ChunkSizeBytes: 1 << 20}.ValidateExtensions())

	err := vaultic.Config{ChunkerType: "bogus"}.ValidateExtensions()
	rtest.Assert(t, err != nil, "expected error for invalid chunker type")

	err = vaultic.Config{ChunkerType: vaultic.ChunkerFixedSize}.ValidateExtensions()
	rtest.Assert(t, err != nil, "expected error for fixed_size without chunk_size")

	err = vaultic.Config{ChunkMinSizeBytes: 8 << 20, ChunkMaxSizeBytes: 1 << 20}.ValidateExtensions()
	rtest.Assert(t, err != nil, "expected error for min > max chunk size")
}

func TestPackSizeDefaults(t *testing.T) {
	c := vaultic.Config{}
	size, limit, gf := c.DataPackSize()
	rtest.Equals(t, uint64(vaultic.DefaultDataPackSize), size)
	rtest.Equals(t, uint64(0), limit)
	rtest.Equals(t, uint32(vaultic.DefaultPackGrowFactor), gf)

	c.DataPackSizeBytes = 64 << 20
	c.DataPackSizeLimitBytes = 128 << 20
	size, limit, _ = c.DataPackSize()
	rtest.Equals(t, uint64(64<<20), size)
	rtest.Equals(t, uint64(128<<20), limit)

	rtest.Assert(t, c.ValidateExtensions() == nil, "valid pack config rejected")
	bad := vaultic.Config{DataPackSizeBytes: 256 << 20, DataPackSizeLimitBytes: 128 << 20}
	rtest.Assert(t, bad.ValidateExtensions() != nil, "size > limit accepted")
}
