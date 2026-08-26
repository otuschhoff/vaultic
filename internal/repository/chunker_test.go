package repository

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/vaultic"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func TestChunkerFactoryFixedSizeUsesRepoConfig(t *testing.T) {
	repo := TestRepository(t)
	cfg := repo.Config()
	cfg.ChunkerType = vaultic.ChunkerFixedSize
	cfg.ChunkSizeBytes = 4
	repo.setConfig(cfg)

	ch := repo.ChunkerFactory().NewChunker()
	// Chunk boundaries are stateful across reads, just like the file saver uses.
	rtest.Equals(t, -1, ch.NextSplitPoint([]byte("abc")))
	rtest.Equals(t, 1, ch.NextSplitPoint([]byte("d")))

	ch.Reset()
	rtest.Equals(t, 4, ch.NextSplitPoint([]byte("abcd")))
	rtest.Equals(t, -1, ch.NextSplitPoint([]byte("xy")))
	rtest.Equals(t, 2, ch.NextSplitPoint([]byte("zw")))

	ch.Reset()
	rtest.Equals(t, 4, ch.NextSplitPoint([]byte("abcd1234")))
}

func TestChunkerFactoryRabinUsesRepoConfig(t *testing.T) {
	repo := TestRepository(t)
	cfg := repo.Config()
	cfg.ChunkerType = vaultic.ChunkerRabin
	cfg.ChunkSizeBytes = 64
	cfg.ChunkMinSizeBytes = 64
	cfg.ChunkMaxSizeBytes = 128
	repo.setConfig(cfg)

	ch := repo.ChunkerFactory().NewChunker()
	split := ch.NextSplitPoint(make([]byte, 256))
	rtest.Assert(t, split >= 64 && split <= 128, "expected configured Rabin split in [64, 128], got %d", split)
}
