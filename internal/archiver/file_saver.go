package archiver

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

const chunkReadBufSize = 512 * 1024 // matches chunker internal read buffer size

// fileSaver concurrently saves incoming files to the repo.
type fileSaver struct {
	saveFilePool *bufferPool
	uploader     vaultic.BlobSaverAsync

	chunkerFactory vaultic.ChunkerFactory

	ch chan<- saveFileJob

	CompleteBlob func(bytes uint64)

	NodeFromFileInfo func(snPath, filename string, meta toNoder, ignoreXattrListError bool) (*data.Node, error)
}

// newFileSaver returns a new file saver. A worker pool with fileWorkers is
// started, it is stopped when ctx is cancelled.
func newFileSaver(ctx context.Context, wg *errgroup.Group, uploader vaultic.BlobSaverAsync, chunkerFactory vaultic.ChunkerFactory, fileWorkers uint) *fileSaver {
	ch := make(chan saveFileJob)
	debug.Log("new file saver with %v file workers", fileWorkers)

	s := &fileSaver{
		uploader:       uploader,
		saveFilePool:   newBufferPool(chunkerFactory.MaxChunkSize()),
		chunkerFactory: chunkerFactory,
		ch:             ch,

		CompleteBlob: func(uint64) {},
	}

	for range fileWorkers {
		wg.Go(func() error {
			s.worker(ctx, ch)
			return nil
		})
	}

	return s
}

func (s *fileSaver) TriggerShutdown() {
	close(s.ch)
}

// fileCompleteFunc is called when the file has been saved.
type fileCompleteFunc func(*data.Node, ItemStats)

// Save stores the file f and returns the data once it has been completed. The
// file is closed by Save. completeReading is only called if the file was read
// successfully. complete is always called. If completeReading is called, then
// this will always happen before calling complete. The callbacks must not block.
func (s *fileSaver) Save(ctx context.Context, snPath string, target string, file fs.File, start func(), completeReading func(), complete fileCompleteFunc) futureNode {
	fn, ch := newFutureNode()
	job := saveFileJob{
		snPath: snPath,
		target: target,
		file:   file,
		ch:     ch,

		start:           start,
		completeReading: completeReading,
		complete:        complete,
	}

	select {
	case s.ch <- job:
	case <-ctx.Done():
		debug.Log("not sending job, context is cancelled: %v", ctx.Err())
		_ = file.Close()
		close(ch)
	}

	return fn
}

type saveFileJob struct {
	snPath string
	target string
	file   fs.File
	ch     chan<- futureNodeResult

	start           func()
	completeReading func()
	complete        fileCompleteFunc
}

type fileChunkState struct {
	readBuf []byte
	bpos    uint
	bmax    uint
	closed  bool
}

func (s *fileChunkState) reset() {
	s.bpos = 0
	s.bmax = 0
	s.closed = false
}

// readNextChunk reads from rd and returns the next chunk of data. io.EOF is
// returned when all chunks have been read.
func (s *fileChunkState) readNextChunk(rd io.Reader, chnker vaultic.Chunker, data []byte) ([]byte, error) {
	data = data[:0]
	for {
		if s.bpos >= s.bmax {
			n, err := io.ReadFull(rd, s.readBuf)

			if err == io.ErrUnexpectedEOF {
				err = nil
			}

			// io.EOF only happens when the end of the file has been reached.
			// If this is the case, we need to return the data we have read so far.
			if err == io.EOF && !s.closed {
				s.closed = true

				if len(data) > 0 {
					return data, nil
				}
			}

			if err != nil {
				return nil, err
			}

			s.bpos = 0
			s.bmax = uint(n)
		}

		split := chnker.NextSplitPoint(s.readBuf[s.bpos:s.bmax])
		if split == -1 {
			data = append(data, s.readBuf[s.bpos:s.bmax]...)
			s.bpos = s.bmax
		} else {
			data = append(data, s.readBuf[s.bpos:s.bpos+uint(split)]...)
			s.bpos += uint(split)
			return data, nil
		}
	}
}

type saveFileParams struct {
	chunker       vaultic.Chunker
	chunkState    *fileChunkState
	snapshotPath  string
	target        string
	file          fs.File
	start         func()
	finishReading func()
	finish        func(futureNodeResult)
}

type fileSaveCompletion struct {
	lock        sync.Mutex
	result      *futureNodeResult
	params      saveFileParams
	remaining   int
	isCompleted bool
}

func (completion *fileSaveCompletion) completeBlob() {
	completion.lock.Lock()
	defer completion.lock.Unlock()

	completion.remaining--
	if completion.remaining != 0 || completion.result.err != nil {
		return
	}
	if completion.isCompleted {
		panic("completed twice") //nolint:forbidigo // completion is a single-delivery invariant
	}
	for _, id := range completion.result.node.Content {
		if id.IsNull() {
			panic("completed file with null ID") //nolint:forbidigo // completed content IDs must be populated
		}
	}
	completion.isCompleted = true
	completion.params.finish(*completion.result)
}

func (completion *fileSaveCompletion) completeError(err error) {
	completion.lock.Lock()
	defer completion.lock.Unlock()

	if completion.result.err != nil {
		return
	}
	if completion.isCompleted {
		panic("completed twice") //nolint:forbidigo // completion is a single-delivery invariant
	}
	completion.isCompleted = true
	completion.result.err = fmt.Errorf("failed to save %v: %w", completion.params.target, err)
	completion.result.node = nil
	completion.result.stats = ItemStats{}
	completion.params.finish(*completion.result)
}

// saveFile stores the file f in the repo, then closes it.
func (s *fileSaver) saveFile(ctx context.Context, params saveFileParams) {
	params.start()

	fnr := futureNodeResult{
		snPath: params.snapshotPath,
		target: params.target,
	}
	completion := fileSaveCompletion{result: &fnr, params: params}

	debug.Log("%v", params.snapshotPath)

	node, err := s.NodeFromFileInfo(params.snapshotPath, params.target, params.file, false)
	if err != nil {
		_ = params.file.Close()
		completion.completeError(err)
		return
	}

	if node.Type != data.NodeTypeFile {
		_ = params.file.Close()
		completion.completeError(errors.Errorf("node type %q is wrong", node.Type))
		return
	}

	params.chunker.Reset()
	params.chunkState.reset()

	node.Content = []vaultic.ID{}
	node.Size = 0
	var idx int
	for {
		buf := s.saveFilePool.Get()
		chunkData, err := params.chunkState.readNextChunk(params.file, params.chunker, buf.Data)
		if err == io.EOF {
			buf.Release()
			break
		}
		if err != nil {
			buf.Release()
			_ = params.file.Close()
			completion.completeError(err)
			return
		}

		// put result buffer back for later reuse
		buf.Data = chunkData
		node.Size += uint64(len(chunkData))

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			buf.Release()
			_ = params.file.Close()
			completion.completeError(ctx.Err())
			return
		}

		// add a place to store the saveBlob result
		pos := idx

		completion.lock.Lock()
		node.Content = append(node.Content, vaultic.ID{})
		completion.lock.Unlock()

		s.uploader.SaveBlobAsync(ctx, vaultic.DataBlob, chunkData, vaultic.ID{}, false, func(newID vaultic.ID, known bool, sizeInRepo int, err error) {
			defer buf.Release()
			if err != nil {
				completion.completeError(err)
				return
			}

			completion.lock.Lock()
			if !known {
				fnr.stats.DataBlobs++
				fnr.stats.DataSize += uint64(len(chunkData))
				fnr.stats.DataSizeInRepo += uint64(sizeInRepo)
			}
			node.Content[pos] = newID
			completion.lock.Unlock()

			completion.completeBlob()
		})
		idx++

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			_ = params.file.Close()
			completion.completeError(ctx.Err())
			return
		}

		s.CompleteBlob(uint64(len(chunkData)))
	}

	err = params.file.Close()
	if err != nil {
		completion.completeError(err)
		return
	}

	fnr.node = node
	completion.lock.Lock()
	// require one additional completeFuture() call to ensure that the future only completes
	// after reaching the end of this method
	completion.remaining += idx + 1
	completion.lock.Unlock()
	params.finishReading()
	completion.completeBlob()
}

func (s *fileSaver) worker(ctx context.Context, jobs <-chan saveFileJob) {
	chnker := s.chunkerFactory.NewChunker()
	chunkState := &fileChunkState{readBuf: make([]byte, chunkReadBufSize)}

	for {
		var job saveFileJob
		var ok bool
		select {
		case <-ctx.Done():
			return
		case job, ok = <-jobs:
			if !ok {
				return
			}
		}

		s.saveFile(ctx, saveFileParams{
			chunker: chnker, chunkState: chunkState, snapshotPath: job.snPath,
			target: job.target, file: job.file, start: job.start,
			finishReading: func() {
				if job.completeReading != nil {
					job.completeReading()
				}
			},
			finish: func(res futureNodeResult) {
				if job.complete != nil {
					job.complete(res.node, res.stats)
				}
				job.ch <- res
				close(job.ch)
			},
		})
	}
}
