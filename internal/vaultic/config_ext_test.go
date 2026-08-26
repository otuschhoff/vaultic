package vaultic_test

import (
	"context"
	"encoding/json"
	"testing"

	rtest "github.com/vaultic/vaultic/internal/test"
	"github.com/vaultic/vaultic/internal/vaultic"
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
	cfg.Compression = &comp
	cfg.AppendOnlyFlag = true
	cfg.ExtraVerify = &ev
	cfg.ChunkerCfg = &vaultic.ChunkerConfig{
		Type:      vaultic.ChunkerRabin,
		ChunkSize: 2 * 1024 * 1024,
	}
	cfg.TreePack = vaultic.PackConfig{Size: 4 * 1024 * 1024, GrowFactor: 32, SizeLimit: 128 * 1024 * 1024}
	cfg.DataPack = vaultic.PackConfig{Size: 32 * 1024 * 1024, GrowFactor: 32, SizeLimit: 512 * 1024 * 1024}
	cfg.MinPacksizeToleratePercent = &minTol

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

func TestConfigRusticFieldNames(t *testing.T) {
	// the extension field names must match rustic's ConfigFile JSON keys so
	// that both tools agree on a repository's settings
	cfg := vaultic.Config{}
	rtest.OK(t, cfg.ValidateExtensions())

	cfg2 := vaultic.Config{
		AppendOnlyFlag: true,
		TreePack:       vaultic.PackConfig{Size: 1 << 20},
		DataPack:       vaultic.PackConfig{Size: 8 << 20},
	}
	data, err := json.Marshal(cfg2)
	rtest.OK(t, err)
	var m map[string]any
	rtest.OK(t, json.Unmarshal(data, &m))
	for _, key := range []string{"append_only", "treepack", "datapack"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("expected key %q in serialized config", key)
		}
	}
	tp := m["treepack"].(map[string]any)
	if _, ok := tp["size"]; !ok {
		t.Fatalf("expected treepack.size in serialized config")
	}
}

func TestChunkerConfigValidate(t *testing.T) {
	rtest.OK(t, vaultic.ChunkerConfig{}.Validate())
	rtest.OK(t, vaultic.ChunkerConfig{Type: vaultic.ChunkerRabin}.Validate())
	rtest.OK(t, vaultic.ChunkerConfig{Type: vaultic.ChunkerFixedSize, ChunkSize: 1 << 20}.Validate())

	err := vaultic.ChunkerConfig{Type: "bogus"}.Validate()
	rtest.Assert(t, err != nil, "expected error for invalid chunker type")

	err = vaultic.ChunkerConfig{Type: vaultic.ChunkerFixedSize}.Validate()
	rtest.Assert(t, err != nil, "expected error for fixed_size without chunk_size")

	err = vaultic.ChunkerConfig{ChunkMinSize: 8 << 20, ChunkMaxSize: 1 << 20}.Validate()
	rtest.Assert(t, err != nil, "expected error for min > max chunk size")
}

func TestPackConfigDefaults(t *testing.T) {
	p := vaultic.PackConfig{}
	size, grow, limit := p.PackSize(vaultic.DefaultDataPackSize)
	rtest.Equals(t, uint64(vaultic.DefaultDataPackSize), size)
	rtest.Equals(t, uint32(vaultic.DefaultPackGrowFactor), grow)
	rtest.Equals(t, uint64(0), limit)

	p = vaultic.PackConfig{Size: 64 << 20, SizeLimit: 128 << 20}
	size, _, limit = p.PackSize(vaultic.DefaultDataPackSize)
	rtest.Equals(t, uint64(64<<20), size)
	rtest.Equals(t, uint64(128<<20), limit)

	rtest.Assert(t, p.Validate("data") == nil, "valid pack config rejected")
	bad := vaultic.PackConfig{Size: 256 << 20, SizeLimit: 128 << 20}
	rtest.Assert(t, bad.Validate("data") != nil, "size > limit accepted")
}
