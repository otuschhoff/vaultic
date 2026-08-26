package repository

import (
	"math/bits"

	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/restic/chunker"
)

type baseChunker struct {
	bc  *chunker.BaseChunker
	pol chunker.Pol
}

func (c *baseChunker) Reset() {
	c.bc.Reset(c.pol)
}

func (c *baseChunker) NextSplitPoint(buf []byte) int {
	return c.bc.NextSplitPoint(buf)
}

type fixedSizeChunker struct {
	size      uint
	remaining uint
}

func (c *fixedSizeChunker) Reset() {
	c.remaining = c.size
}

func (c *fixedSizeChunker) NextSplitPoint(buf []byte) int {
	if c.size == 0 {
		return -1
	}
	if c.remaining == 0 {
		c.remaining = c.size
	}
	if uint(len(buf)) >= c.remaining {
		split := int(c.remaining)
		c.remaining = c.size
		return split
	}
	c.remaining -= uint(len(buf))
	return -1
}

type chunkerFactory struct {
	pol       chunker.Pol
	chunkSize uint
	chunkMin  uint
	chunkMax  uint
	fixedSize uint
	zeroChunk func() vaultic.ID
}

func newChunkerFactory(r *Repository) *chunkerFactory {
	cfg := r.Config()
	factory := &chunkerFactory{
		pol:       cfg.ChunkerPolynomial,
		chunkSize: uint(cfg.ChunkSize()),
		chunkMin:  uint(cfg.ChunkMinSize()),
		chunkMax:  uint(cfg.ChunkMaxSize()),
		zeroChunk: r.zeroChunk,
	}
	if cfg.Chunker() == vaultic.ChunkerFixedSize {
		factory.fixedSize = factory.chunkSize
	}
	return factory
}

func (f *chunkerFactory) NewChunker() vaultic.Chunker {
	if f.fixedSize != 0 {
		return &fixedSizeChunker{size: f.fixedSize, remaining: f.fixedSize}
	}
	return &baseChunker{
		bc:  chunker.NewBase(f.pol, chunker.WithBaseBoundaries(f.chunkMin, f.chunkMax), chunker.WithBaseAverageBits(bits.Len(f.chunkSize)-1)),
		pol: f.pol,
	}
}

func (f *chunkerFactory) MaxChunkSize() int {
	if f.fixedSize > uint(chunker.MaxSize) {
		return int(f.fixedSize)
	}
	if f.fixedSize == 0 && f.chunkMax != 0 && f.chunkMax < uint(chunker.MaxSize) {
		return int(f.chunkMax)
	}
	return chunker.MaxSize
}

func (f *chunkerFactory) ZeroChunk() vaultic.ID {
	return f.zeroChunk()
}

func (r *Repository) ChunkerFactory() vaultic.ChunkerFactory {
	return newChunkerFactory(r)
}
