package repository

import (
	"github.com/restic/chunker"
	"github.com/vaultic/vaultic/internal/vaultic"
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

type chunkerFactory struct {
	pol       chunker.Pol
	zeroChunk func() vaultic.ID
}

func newChunkerFactory(r *Repository) *chunkerFactory {
	return &chunkerFactory{
		pol:       r.Config().ChunkerPolynomial,
		zeroChunk: r.zeroChunk,
	}
}

func (f *chunkerFactory) NewChunker() vaultic.Chunker {
	return &baseChunker{bc: chunker.NewBase(f.pol), pol: f.pol}
}

func (f *chunkerFactory) MaxChunkSize() int {
	return chunker.MaxSize
}

func (f *chunkerFactory) ZeroChunk() vaultic.ID {
	return f.zeroChunk()
}

func (r *Repository) ChunkerFactory() vaultic.ChunkerFactory {
	return newChunkerFactory(r)
}
