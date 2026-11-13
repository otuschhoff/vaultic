package archiver

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

const chunkReadBufSize = 512 * 1024 // matches chunker internal read buffer size

type SourceReadMetrics struct {
	Calls          uint64              `json:"calls"`
	Bytes          uint64              `json:"bytes"`
	Nanoseconds    uint64              `json:"read_ns"`
	Errors         uint64              `json:"errors"`
	LatencyBuckets [8]uint64           `json:"latency_buckets"`
	FilePipeline   FilePipelineMetrics `json:"file_pipeline"`
}

type FilePipelineMetrics struct {
	Scheduled      uint64 `json:"scheduled"`
	Started        uint64 `json:"started"`
	ReadComplete   uint64 `json:"read_complete"`
	Completed      uint64 `json:"completed"`
	Errors         uint64 `json:"errors"`
	Admitting      int64  `json:"admitting"`
	Active         int64  `json:"active"`
	AwaitingBlobs  int64  `json:"awaiting_blobs"`
	PendingBlobs   int64  `json:"pending_blobs"`
	AdmissionNS    uint64 `json:"admission_ns"`
	WorkerNS       uint64 `json:"worker_ns"`
	ChunkWorkNS    uint64 `json:"chunk_work_ns"`
	BlobSubmitNS   uint64 `json:"blob_submit_ns"`
	BlobCallbackNS uint64 `json:"blob_callback_ns"`
	PostReadNS     uint64 `json:"post_read_ns"`
}

type filePipelineCounters struct {
	scheduled, started, readComplete, completed, errors                          atomic.Uint64
	admitting, active, awaitingBlobs, pendingBlobs                               atomic.Int64
	admissionNS, workerNS, chunkWorkNS, blobSubmitNS, blobCallbackNS, postReadNS atomic.Uint64
}

func (metrics FilePipelineMetrics) Delta(baseline FilePipelineMetrics) FilePipelineMetrics {
	metrics.Scheduled -= baseline.Scheduled
	metrics.Started -= baseline.Started
	metrics.ReadComplete -= baseline.ReadComplete
	metrics.Completed -= baseline.Completed
	metrics.Errors -= baseline.Errors
	metrics.AdmissionNS -= baseline.AdmissionNS
	metrics.WorkerNS -= baseline.WorkerNS
	metrics.ChunkWorkNS -= baseline.ChunkWorkNS
	metrics.BlobSubmitNS -= baseline.BlobSubmitNS
	metrics.BlobCallbackNS -= baseline.BlobCallbackNS
	metrics.PostReadNS -= baseline.PostReadNS
	return metrics
}

func (counters *filePipelineCounters) snapshot() FilePipelineMetrics {
	return FilePipelineMetrics{
		Scheduled: counters.scheduled.Load(), Started: counters.started.Load(),
		ReadComplete: counters.readComplete.Load(), Completed: counters.completed.Load(), Errors: counters.errors.Load(),
		Admitting: counters.admitting.Load(), Active: counters.active.Load(),
		AwaitingBlobs: counters.awaitingBlobs.Load(), PendingBlobs: counters.pendingBlobs.Load(),
		AdmissionNS: counters.admissionNS.Load(), WorkerNS: counters.workerNS.Load(),
		ChunkWorkNS: counters.chunkWorkNS.Load(), BlobSubmitNS: counters.blobSubmitNS.Load(),
		BlobCallbackNS: counters.blobCallbackNS.Load(), PostReadNS: counters.postReadNS.Load(),
	}
}

type sourceReadCounters struct {
	calls, bytes, nanoseconds, errors atomic.Uint64
	latencyBuckets                    [8]atomic.Uint64
	pipeline                          filePipelineCounters
}

func (arch *Archiver) SourceReadMetrics() SourceReadMetrics {
	return arch.sourceReads.snapshot()
}

func (counters *sourceReadCounters) snapshot() SourceReadMetrics {
	metrics := SourceReadMetrics{Calls: counters.calls.Load(), Bytes: counters.bytes.Load(), Nanoseconds: counters.nanoseconds.Load(), Errors: counters.errors.Load()}
	metrics.FilePipeline = counters.pipeline.snapshot()
	for index := range metrics.LatencyBuckets {
		metrics.LatencyBuckets[index] = counters.latencyBuckets[index].Load()
	}
	return metrics
}

type measuredSourceReader struct {
	reader   io.Reader
	counters *sourceReadCounters
}

func (reader measuredSourceReader) Read(buffer []byte) (int, error) {
	started := time.Now()
	count, err := reader.reader.Read(buffer)
	elapsed := uint64(time.Since(started))
	reader.counters.calls.Add(1)
	reader.counters.bytes.Add(uint64(count))
	reader.counters.nanoseconds.Add(elapsed)
	if err != nil && err != io.EOF {
		reader.counters.errors.Add(1)
	}
	bucket := 0
	for _, upper := range [...]uint64{100_000, 1_000_000, 5_000_000, 20_000_000, 100_000_000, 500_000_000, 2_000_000_000} {
		if elapsed <= upper {
			break
		}
		bucket++
	}
	reader.counters.latencyBuckets[bucket].Add(1)
	return count, err
}

// fileSaver concurrently saves incoming files to the repo.
type fileSaver struct {
	sourceReads  *sourceReadCounters
	saveFilePool *bufferPool
	uploader     vaultic.BlobSaverAsync

	chunkerFactory vaultic.ChunkerFactory

	ch chan<- saveFileJob

	CompleteBlob func(bytes uint64)

	NodeFromFileInfo func(snPath, filename string, meta toNoder, ignoreXattrListError bool) (*data.Node, error)
}

// newFileSaver returns a new file saver. A worker pool with fileWorkers is
// started, it is stopped when ctx is cancelled.
func newFileSaver(
	ctx context.Context,
	wg *errgroup.Group,
	uploader vaultic.BlobSaverAsync,
	chunkerFactory vaultic.ChunkerFactory,
	fileWorkers uint,
) *fileSaver {
	ch := make(chan saveFileJob)
	debug.Log("new file saver with %v file workers", fileWorkers)

	s := &fileSaver{
		sourceReads:    &sourceReadCounters{},
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
func (s *fileSaver) Save(
	ctx context.Context,
	snPath string,
	target string,
	file fs.File,
	start func(),
	completeReading func(),
	complete fileCompleteFunc,
) futureNode {
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

	started := time.Now()
	s.sourceReads.pipeline.scheduled.Add(1)
	s.sourceReads.pipeline.admitting.Add(1)
	select {
	case s.ch <- job:
	case <-ctx.Done():
		debug.Log("not sending job, context is cancelled: %v", ctx.Err())
		_ = file.Close() // Preserve the stat failure; this source file has no buffered writes.
		close(ch)
	}
	s.sourceReads.pipeline.admissionNS.Add(uint64(time.Since(started)))
	s.sourceReads.pipeline.admitting.Add(-1)

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

// readNextChunk reads from reader and returns the next chunk of data. io.EOF is
// returned when all chunks have been read.
func (s *fileChunkState) readNextChunk(reader io.Reader, chnker vaultic.Chunker, data []byte) ([]byte, error) {
	data = data[:0]
	for {
		//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
		if s.bpos >= s.bmax {
			n, err := io.ReadFull(reader, s.readBuf)

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
	lock         sync.Mutex
	result       *futureNodeResult
	params       saveFileParams
	remaining    int
	isCompleted  bool
	pipeline     *filePipelineCounters
	readFinished time.Time
}

func (completion *fileSaveCompletion) recordCompletion() {
	completion.pipeline.completed.Add(1)
	if completion.result.err != nil {
		completion.pipeline.errors.Add(1)
	}
	if !completion.readFinished.IsZero() {
		completion.pipeline.postReadNS.Add(uint64(time.Since(completion.readFinished)))
		completion.pipeline.awaitingBlobs.Add(-1)
	}
}

func (completion *fileSaveCompletion) completeBlob() {
	completion.lock.Lock()
	defer completion.lock.Unlock()

	completion.remaining--
	if completion.remaining != 0 || completion.result.err != nil {
		return
	}
	if completion.isCompleted {
		// Completion is a single-delivery invariant.
		panic("completed twice") //nolint:forbidigo // A second delivery is an internal state-machine defect.
	}
	for _, id := range completion.result.node.Content {
		if id.IsNull() {
			// Completed content IDs must be populated.
			panic("completed file with null ID") //nolint:forbidigo // Completed content must always have an assigned ID.
		}
	}
	completion.isCompleted = true
	completion.recordCompletion()
	completion.params.finish(*completion.result)
}

func (completion *fileSaveCompletion) completeError(err error) {
	completion.lock.Lock()
	defer completion.lock.Unlock()

	if completion.result.err != nil {
		return
	}
	if completion.isCompleted {
		// Completion is a single-delivery invariant.
		panic("completed twice") //nolint:forbidigo // A second delivery is an internal state-machine defect.
	}
	completion.isCompleted = true
	completion.result.err = fmt.Errorf("failed to save %v: %w", completion.params.target, err)
	completion.result.node = nil
	completion.result.stats = ItemStats{}
	completion.recordCompletion()
	completion.params.finish(*completion.result)
}

// saveFile stores the file f in the repo, then closes it.
func (s *fileSaver) saveFile(ctx context.Context, params saveFileParams) {
	started := time.Now()
	pipeline := &s.sourceReads.pipeline
	pipeline.started.Add(1)
	pipeline.active.Add(1)
	defer func() {
		pipeline.workerNS.Add(uint64(time.Since(started)))
		pipeline.active.Add(-1)
	}()
	params.start()

	fnr := futureNodeResult{
		snPath: params.snapshotPath,
		target: params.target,
	}
	completion := fileSaveCompletion{result: &fnr, params: params, pipeline: pipeline}

	debug.Log("%v", params.snapshotPath)

	node, err := s.NodeFromFileInfo(params.snapshotPath, params.target, params.file, false)
	if err != nil {
		_ = params.file.Close() // Preserve the node-conversion failure for this read-only source.
		completion.completeError(err)
		return
	}

	if node.Type != data.NodeTypeFile {
		_ = params.file.Close() // Preserve the invalid-node failure for this read-only source.
		completion.completeError(errors.Errorf("node type %q is wrong", node.Type))
		return
	}

	params.chunker.Reset()
	params.chunkState.reset()

	node.Content = []vaultic.ID{}
	node.Size = 0
	reader := measuredSourceReader{reader: params.file, counters: s.sourceReads}
	var idx int
	for {
		chunkStarted := time.Now()
		buf := s.saveFilePool.Get()
		chunkData, err := params.chunkState.readNextChunk(reader, params.chunker, buf.Data)
		pipeline.chunkWorkNS.Add(uint64(time.Since(chunkStarted)))
		if errors.Is(err, io.EOF) {
			buf.Release()
			break
		}
		if err != nil {
			buf.Release()
			_ = params.file.Close() // Preserve the chunk-read failure for this read-only source.
			completion.completeError(err)
			return
		}

		// put result buffer back for later reuse
		buf.Data = chunkData
		node.Size += uint64(len(chunkData))

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			buf.Release()
			_ = params.file.Close() // Preserve cancellation as the operation result.
			completion.completeError(ctx.Err())
			return
		}

		// add a place to store the saveBlob result
		pos := idx

		completion.lock.Lock()
		node.Content = append(node.Content, vaultic.ID{})
		completion.lock.Unlock()

		blobStarted := time.Now()
		pipeline.pendingBlobs.Add(1)
		s.uploader.SaveBlobAsync(ctx, vaultic.DataBlob, chunkData, vaultic.ID{}, false, func(newID vaultic.ID, known bool, sizeInRepo int, err error) {
			defer buf.Release()
			pipeline.blobCallbackNS.Add(uint64(time.Since(blobStarted)))
			pipeline.pendingBlobs.Add(-1)
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
		pipeline.blobSubmitNS.Add(uint64(time.Since(blobStarted)))
		idx++

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			_ = params.file.Close() // Preserve cancellation as the operation result.
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
	pipeline.readComplete.Add(1)
	if !completion.isCompleted {
		completion.readFinished = time.Now()
		pipeline.awaitingBlobs.Add(1)
	}
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
