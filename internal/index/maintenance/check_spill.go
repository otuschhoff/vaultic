package maintenance

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sync/errgroup"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	locationTupleVersion  = 1
	locationTupleSize     = 90
	locationChunkBytes    = 64 << 20
	locationRunBufferSize = 1 << 20
	checkRunHeaderSize    = 12
	checkRunPrefix        = "run-"
	checkScratchPrefix    = "vaultic-check-"
)

var checkRunMagic = [8]byte{'V', 'L', 'T', 'C', 'H', 'K', '0', '1'}

type locationTuple struct {
	BlobID             vaultic.ID
	PackID             vaultic.ID
	Type               uint8
	Offset             uint64
	Length             uint64
	UncompressedLength uint64
}

const locationTupleMemorySize = uint64(unsafe.Sizeof(locationTuple{}))

func (tuple locationTuple) marshalBinary() [locationTupleSize]byte {
	var encoded [locationTupleSize]byte
	encoded[0] = locationTupleVersion
	copy(encoded[1:33], tuple.BlobID[:])
	copy(encoded[33:65], tuple.PackID[:])
	encoded[65] = tuple.Type
	binary.BigEndian.PutUint64(encoded[66:74], tuple.Offset)
	binary.BigEndian.PutUint64(encoded[74:82], tuple.Length)
	binary.BigEndian.PutUint64(encoded[82:90], tuple.UncompressedLength)
	return encoded
}

func unmarshalLocationTuple(encoded []byte) (locationTuple, error) {
	if len(encoded) != locationTupleSize || encoded[0] != locationTupleVersion {
		return locationTuple{}, fmt.Errorf("invalid checker location tuple")
	}
	var tuple locationTuple
	copy(tuple.BlobID[:], encoded[1:33])
	copy(tuple.PackID[:], encoded[33:65])
	tuple.Type = encoded[65]
	tuple.Offset = binary.BigEndian.Uint64(encoded[66:74])
	tuple.Length = binary.BigEndian.Uint64(encoded[74:82])
	tuple.UncompressedLength = binary.BigEndian.Uint64(encoded[82:90])
	return tuple, nil
}

func compareLocationTuple(left, right locationTuple) int {
	if compared := bytes.Compare(left.BlobID[:], right.BlobID[:]); compared != 0 {
		return compared
	}
	if compared := bytes.Compare(left.PackID[:], right.PackID[:]); compared != 0 {
		return compared
	}
	if compared := cmp.Compare(left.Type, right.Type); compared != 0 {
		return compared
	}
	if compared := cmp.Compare(left.Offset, right.Offset); compared != 0 {
		return compared
	}
	if compared := cmp.Compare(left.Length, right.Length); compared != 0 {
		return compared
	}
	return cmp.Compare(left.UncompressedLength, right.UncompressedLength)
}

func (tuple locationTuple) findingKey() string {
	return fmt.Sprintf(
		"%s:%s:%d:%d:%d:%d",
		tuple.BlobID.String(),
		tuple.PackID.String(),
		tuple.Type,
		tuple.Offset,
		tuple.Length,
		tuple.UncompressedLength,
	)
}

type checkScratch struct {
	mu        sync.Mutex
	ctx       context.Context
	parent    string
	dir       string
	marker    string
	key       [32]byte
	maxBytes  uint64
	used      uint64
	peak      uint64
	nextRun   uint64
	merges    uint64
	telemetry *CheckTelemetry
	operation *monitor.ActionGuard
	scenario  *monitor.ExperimentController
}

type scratchReadFile struct {
	file    *os.File
	scratch *checkScratch
}

func (scratch *checkScratch) open(path string) (*scratchReadFile, error) {
	request := scratch.telemetry.startScratch()
	var file *os.File
	err := scratch.run("open", 0, func() error {
		var openErr error
		file, openErr = os.Open(path)
		return openErr
	})
	settleDependency(request, err)
	if err != nil {
		return nil, err
	}
	return &scratchReadFile{file: file, scratch: scratch}, nil
}

func (file *scratchReadFile) Read(value []byte) (int, error) {
	request := file.scratch.telemetry.startScratch()
	var read int
	err := file.scratch.run("read", uint64(len(value)), func() error {
		var readErr error
		read, readErr = file.file.Read(value)
		return readErr
	})
	file.scratch.telemetry.processScratch(file.scratch.operation, request, uint64(read))
	settlementErr := err
	if errors.Is(err, io.EOF) {
		settlementErr = nil
	}
	settleDependency(request, settlementErr)
	return read, err
}

func (file *scratchReadFile) Close() error {
	request := file.scratch.telemetry.startScratch()
	err := file.file.Close()
	settleDependency(request, err)
	return err
}

func newCheckScratch(parent string, maxBytes uint64) (*checkScratch, error) {
	return newCheckScratchWithScenario(context.Background(), parent, maxBytes, nil)
}

func newCheckScratchWithScenario(ctx context.Context, parent string, maxBytes uint64, scenario *monitor.ExperimentController) (*checkScratch, error) {
	if maxBytes == 0 {
		return nil, fmt.Errorf("checker scratch byte limit must be positive")
	}
	if parent == "" {
		parent = os.TempDir()
	}
	info, err := os.Stat(parent)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("checker scratch parent must be an existing directory")
	}
	scratch := &checkScratch{ctx: ctx, parent: parent, maxBytes: maxBytes, scenario: scenario}
	if _, err := rand.Read(scratch.key[:]); err != nil {
		return nil, fmt.Errorf("create checker scratch key: %w", err)
	}
	markerBytes := make([]byte, 32)
	if _, err := rand.Read(markerBytes); err != nil {
		return nil, fmt.Errorf("create checker scratch marker: %w", err)
	}
	scratch.marker = fmt.Sprintf("%x", markerBytes)
	return scratch, nil
}

func (scratch *checkScratch) ensureDirLocked() error {
	if scratch.dir != "" {
		return nil
	}
	request := scratch.telemetry.startScratch()
	dir, err := os.MkdirTemp(scratch.parent, checkScratchPrefix)
	settleDependency(request, err)
	if err != nil {
		return fmt.Errorf("create checker scratch directory: %w", err)
	}
	request = scratch.telemetry.startScratch()
	if err := os.Chmod(dir, 0o700); err != nil {
		settleDependency(request, err)
		_ = os.Remove(dir) // The permission error is the actionable failure.
		return fmt.Errorf("protect checker scratch directory: %w", err)
	}
	settleDependency(request, nil)
	request = scratch.telemetry.startScratch()
	marker := []byte(scratch.marker)
	file, err := os.OpenFile(filepath.Join(dir, ".vaultic-check-owned"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		err = writeScratch(scratch, request, file, marker)
		err = errors.Join(err, file.Close())
	}
	settleDependency(request, err)
	if err != nil {
		_ = os.RemoveAll(dir) // The marker-write error is the actionable failure.
		return fmt.Errorf("write checker scratch marker: %w", err)
	}
	scratch.dir = dir
	return nil
}

func (scratch *checkScratch) reserve(bytes uint64) error {
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	if scratch.used > scratch.maxBytes {
		return fmt.Errorf("checker scratch accounting exceeds configured limit")
	}
	if bytes > scratch.maxBytes-scratch.used {
		return fmt.Errorf("checker scratch limit exceeded: need %d bytes with %d of %d bytes used", bytes, scratch.used, scratch.maxBytes)
	}
	scratch.used += bytes
	scratch.peak = max(scratch.peak, scratch.used)
	return nil
}

func (scratch *checkScratch) release(bytes uint64) {
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	scratch.used = scratch.used - min(scratch.used, bytes)
}

func (scratch *checkScratch) nextPath() (string, [4]byte, error) {
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	var prefix [4]byte
	if err := scratch.ensureDirLocked(); err != nil {
		return "", prefix, err
	}
	if scratch.nextRun == math.MaxUint32 {
		return "", prefix, fmt.Errorf("checker scratch run limit exceeded")
	}
	scratch.nextRun++
	binary.BigEndian.PutUint32(prefix[:], uint32(scratch.nextRun))
	return filepath.Join(scratch.dir, fmt.Sprintf("%s%08d", checkRunPrefix, scratch.nextRun)), prefix, nil
}

func (scratch *checkScratch) recordMerge() {
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	scratch.merges++
}

func (scratch *checkScratch) stats() (peak, merges uint64) {
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	return scratch.peak, scratch.merges
}

func (scratch *checkScratch) close() error {
	scratch.mu.Lock()
	dir := scratch.dir
	scratch.mu.Unlock()
	if dir == "" {
		return nil
	}
	base := filepath.Base(dir)
	if !strings.HasPrefix(base, checkScratchPrefix) || filepath.Dir(dir) == dir {
		return fmt.Errorf("refusing to remove unowned checker scratch path")
	}
	markerFile, err := scratch.open(filepath.Join(dir, ".vaultic-check-owned"))
	if err != nil {
		return fmt.Errorf("refusing to remove checker scratch without matching ownership marker")
	}
	marker, readErr := io.ReadAll(markerFile)
	err = errors.Join(readErr, markerFile.Close())
	if err != nil || string(marker) != scratch.marker {
		return fmt.Errorf("refusing to remove checker scratch without matching ownership marker")
	}
	request := scratch.telemetry.startScratch()
	err = os.RemoveAll(dir)
	settleDependency(request, err)
	return err
}

func (scratch *checkScratch) remove(path string) error {
	request := scratch.telemetry.startScratch()
	err := scratch.run("delete", 0, func() error { return os.Remove(path) })
	settleDependency(request, err)
	return err
}

func (scratch *checkScratch) run(method string, bytes uint64, operation func() error) error {
	if scratch.scenario == nil || !scratch.scenario.Matches("scratch", method) {
		return operation()
	}
	return scratch.scenario.Run(scratch.ctx, monitor.ExperimentEvent{
		Identity: "checker-scratch-" + method,
		Bytes:    bytes,
	}, func(context.Context) error { return operation() })
}

type checkRun struct {
	path string
	size uint64
}

type locationSpool struct {
	ctx            context.Context
	scratch        *checkScratch
	memoryBytes    uint64
	memoryUsed     uint64
	chunkItems     int
	fanIn          int
	mergeWorkers   int
	deduplicate    bool
	packSummaries  bool
	buffer         []locationTuple
	memoryRuns     [][]locationTuple
	runs           []checkRun
	blobPartitions []*locationSpool
	diskMode       bool
	sealed         bool
}

func newLocationSpool(ctx context.Context, scratch *checkScratch, memoryBytes uint64, fanIn int) (*locationSpool, error) {
	return newLocationSpoolMode(ctx, scratch, memoryBytes, fanIn, true)
}

func newLocationMultisetSpool(ctx context.Context, scratch *checkScratch, memoryBytes uint64, fanIn int) (*locationSpool, error) {
	return newLocationSpoolMode(ctx, scratch, memoryBytes, fanIn, false)
}

func newLocationSpoolMode(ctx context.Context, scratch *checkScratch, memoryBytes uint64, fanIn int, deduplicate bool) (*locationSpool, error) {
	if memoryBytes < locationTupleMemorySize || fanIn < 2 || fanIn > 128 {
		return nil, fmt.Errorf("invalid checker spool memory or fan-in limit")
	}
	chunkItems := int(min(memoryBytes, uint64(locationChunkBytes)) / locationTupleMemorySize)
	return &locationSpool{
		ctx: ctx, scratch: scratch, memoryBytes: memoryBytes, fanIn: fanIn,
		deduplicate: deduplicate, chunkItems: chunkItems,
	}, nil
}

func (spool *locationSpool) allocateBuffer() error {
	if spool.buffer != nil {
		return nil
	}
	capacity := spool.chunkItems
	if !spool.diskMode {
		remaining := (spool.memoryBytes - spool.memoryUsed) / locationTupleMemorySize
		if remaining == 0 {
			records := spool.memoryRuns[0]
			run, err := spool.writeRun(records)
			if err != nil {
				return err
			}
			spool.runs = append(spool.runs, run)
			spool.memoryRuns[0] = nil
			spool.memoryRuns = spool.memoryRuns[1:]
			spool.buffer = records[:0]
			spool.diskMode = true
			return nil
		} else {
			capacity = int(min(uint64(capacity), remaining))
		}
	}
	spool.buffer = make([]locationTuple, 0, capacity)
	spool.memoryUsed += uint64(capacity) * locationTupleMemorySize
	return nil
}

func (spool *locationSpool) add(tuple locationTuple) error {
	if spool.packSummaries {
		summary := packContributionSummary{id: tuple.BlobID}
		if tuple.Type == 0 {
			summary.present = true
		} else {
			summary.count, summary.payload = 1, tuple.Length
			summary.types = summarizePackType(0, schema.BlobType(tuple.Type))
		}
		tuple = summary.tuple()
	}
	return spool.addEncoded(tuple)
}

func (spool *locationSpool) addEncoded(tuple locationTuple) error {
	if spool.sealed || len(spool.blobPartitions) != 0 {
		return fmt.Errorf("checker location spool is sealed")
	}
	if err := spool.ctx.Err(); err != nil {
		return err
	}
	if err := spool.allocateBuffer(); err != nil {
		return err
	}
	if len(spool.buffer) == cap(spool.buffer) {
		if err := spool.finishBuffer(); err != nil {
			return err
		}
		if err := spool.allocateBuffer(); err != nil {
			return err
		}
	}
	spool.buffer = append(spool.buffer, tuple)
	if len(spool.buffer) == cap(spool.buffer) {
		return spool.finishBuffer()
	}
	return nil
}

func (spool *locationSpool) sortRecords(records []locationTuple) []locationTuple {
	if spool.scratch.telemetry != nil {
		request := spool.scratch.telemetry.scratchSort.Start()
		defer settleDependency(request, nil)
	}
	slices.SortFunc(records, compareLocationTuple)
	if spool.deduplicate {
		unique := records[:0]
		for _, tuple := range records {
			if len(unique) == 0 || compareLocationTuple(unique[len(unique)-1], tuple) != 0 {
				unique = append(unique, tuple)
			}
		}
		return unique
	}
	return records
}

func (spool *locationSpool) finishBuffer() error {
	if len(spool.buffer) == 0 {
		return nil
	}
	records := spool.sortRecords(spool.buffer)
	if spool.packSummaries {
		compacted := records[:0]
		var summary packContributionSummary
		for index, tuple := range records {
			if index == 0 || tuple.BlobID != summary.id {
				if index > 0 {
					compacted = append(compacted, summary.tuple())
				}
				summary = packContributionSummary{id: tuple.BlobID}
			}
			if err := summary.addPartial(tuple); err != nil {
				return err
			}
		}
		compacted = append(compacted, summary.tuple())
		records = compacted
		spool.buffer = records
	}
	if spool.diskMode {
		run, err := spool.writeRun(records)
		if err != nil {
			return err
		}
		spool.runs = append(spool.runs, run)
		spool.buffer = spool.buffer[:0]
		return nil
	}
	spool.memoryRuns = append(spool.memoryRuns, records)
	spool.buffer = nil
	return nil
}

func (spool *locationSpool) spillMemoryRuns() error {
	for len(spool.memoryRuns) > 0 {
		records := spool.memoryRuns[0]
		run, err := spool.writeRun(records)
		if err != nil {
			return err
		}
		spool.runs = append(spool.runs, run)
		spool.memoryRuns = spool.memoryRuns[1:]
		spool.memoryUsed -= uint64(cap(records)) * locationTupleMemorySize
	}
	spool.diskMode = true
	return nil
}

func (spool *locationSpool) writeRun(records []locationTuple) (run checkRun, err error) {
	block, err := aes.NewCipher(spool.scratch.key[:])
	if err != nil {
		return checkRun{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return checkRun{}, err
	}
	recordBytes := uint64(4 + locationTupleSize + aead.Overhead())
	predicted := uint64(checkRunHeaderSize) + uint64(len(records))*recordBytes
	if err := spool.scratch.reserve(predicted); err != nil {
		return checkRun{}, err
	}
	request := spool.scratch.telemetry.startScratch()
	defer func() { settleDependency(request, err) }()
	path, prefix, err := spool.scratch.nextPath()
	if err != nil {
		spool.scratch.release(predicted)
		return checkRun{}, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		spool.scratch.release(predicted)
		return checkRun{}, err
	}
	succeeded := false
	buffered := bufio.NewWriterSize(&scratchOutputWriter{scratch: spool.scratch, request: request, writer: file}, locationRunBufferSize)
	defer func() {
		_ = file.Close() // Preserve the primary write or sync error.
		if !succeeded {
			_ = spool.scratch.remove(path) // Preserve the primary run-creation error.
			spool.scratch.release(predicted)
		}
	}()
	header := append(checkRunMagic[:], prefix[:]...)
	if err := writeAll(buffered, header); err != nil {
		return checkRun{}, err
	}
	var encodeWrite *monitor.DependencyGuard
	if spool.scratch.telemetry != nil {
		encodeWrite = spool.scratch.telemetry.scratchEncodeWrite.Start()
	}
	defer func() {
		if encodeWrite != nil {
			settleDependency(encodeWrite, err)
		}
	}()
	for index, record := range records {
		if err := spool.ctx.Err(); err != nil {
			return checkRun{}, err
		}
		var nonce [12]byte
		copy(nonce[:4], prefix[:])
		binary.BigEndian.PutUint64(nonce[4:], uint64(index))
		encoded := record.marshalBinary()
		sealed := aead.Seal(nil, nonce[:], encoded[:], checkRunMagic[:])
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(sealed)))
		if err := writeAll(buffered, length[:]); err != nil {
			return checkRun{}, err
		}
		if err := writeAll(buffered, sealed); err != nil {
			return checkRun{}, err
		}
	}
	settleDependency(encodeWrite, nil)
	encodeWrite = nil
	var flush *monitor.DependencyGuard
	if spool.scratch.telemetry != nil {
		flush = spool.scratch.telemetry.scratchFlush.Start()
	}
	err = buffered.Flush()
	settleDependency(flush, err)
	if err != nil {
		return checkRun{}, err
	}
	var sync *monitor.DependencyGuard
	if spool.scratch.telemetry != nil {
		sync = spool.scratch.telemetry.scratchSync.Start()
	}
	err = file.Sync()
	settleDependency(sync, err)
	if err != nil {
		return checkRun{}, err
	}
	if err := file.Close(); err != nil {
		return checkRun{}, err
	}
	succeeded = true
	return checkRun{path: path, size: predicted}, nil
}

func writeAll(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := writer.Write(value)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		value = value[written:]
	}
	return nil
}

func writeScratch(scratch *checkScratch, request *monitor.DependencyGuard, writer io.Writer, value []byte) error {
	return writeAll(&scratchOutputWriter{scratch: scratch, request: request, writer: writer}, value)
}

type scratchOutputWriter struct {
	scratch *checkScratch
	request *monitor.DependencyGuard
	writer  io.Writer
}

func (writer *scratchOutputWriter) Write(value []byte) (int, error) {
	var written int
	err := writer.scratch.run("put", uint64(len(value)), func() error {
		var writeErr error
		written, writeErr = writer.writer.Write(value)
		return writeErr
	})
	writer.scratch.telemetry.processScratch(writer.scratch.operation, writer.request, uint64(written))
	return written, err
}

func (spool *locationSpool) seal() error {
	if spool.sealed {
		return nil
	}
	if err := spool.finishBuffer(); err != nil {
		return err
	}
	for len(spool.runs) > spool.fanIn {
		if err := spool.mergePass(); err != nil {
			return err
		}
	}
	spool.sealed = true
	return nil
}

func (spool *locationSpool) mergePass() error {
	var inputBytes uint64
	for _, run := range spool.runs {
		if run.size < checkRunHeaderSize || run.size > math.MaxUint64-inputBytes {
			return fmt.Errorf("invalid checker merge run size")
		}
		inputBytes += run.size
	}
	workers := max(1, min(spool.mergeWorkers, 4, int(spool.memoryBytes/uint64((spool.fanIn+1)*locationRunBufferSize))))
	completed := 0
	for len(spool.runs)-completed > 1 {
		if err := spool.ctx.Err(); err != nil {
			return err
		}
		group, ctx := errgroup.WithContext(spool.ctx)
		outputs := make([]checkRun, workers)
		end := completed
		started := 0
		for started < workers && len(spool.runs)-end > 1 {
			next := min(end+spool.fanIn, len(spool.runs))
			inputs := spool.runs[end:next]
			reserved := uint64(checkRunHeaderSize)
			for _, run := range inputs {
				reserved += run.size - checkRunHeaderSize
			}
			if err := spool.scratch.reserve(reserved); err != nil {
				if started == 0 {
					_ = group.Wait()
					return err
				}
				break
			}
			outputIndex := started
			worker := *spool
			worker.ctx = ctx
			group.Go(func() error {
				var err error
				outputs[outputIndex], err = worker.mergeRuns(inputs, reserved)
				return err
			})
			started++
			end = next
		}
		err := group.Wait()
		if err == nil {
			err = spool.ctx.Err()
		}
		if err != nil {
			for _, output := range outputs {
				if output.path != "" {
					_ = spool.scratch.remove(output.path)
					spool.scratch.release(output.size)
				}
			}
			return err
		}
		inputs := append([]checkRun(nil), spool.runs[completed:end]...)
		nextRuns := append([]checkRun(nil), spool.runs[:completed]...)
		nextRuns = append(nextRuns, outputs[:started]...)
		spool.runs = append(nextRuns, spool.runs[end:]...)
		for index, input := range inputs {
			if err := spool.scratch.remove(input.path); err != nil {
				spool.runs = append(spool.runs, inputs[index:]...)
				return fmt.Errorf("remove merged checker run: %w", err)
			}
			spool.scratch.release(input.size)
		}
		completed += started
	}
	return nil
}

func (spool *locationSpool) adopt(source *locationSpool) error {
	if spool.sealed || source.sealed || len(spool.blobPartitions) != 0 || len(source.blobPartitions) != 0 || spool.scratch != source.scratch || spool.deduplicate != source.deduplicate || spool.packSummaries != source.packSummaries {
		return fmt.Errorf("incompatible checker location spools")
	}
	if err := source.finishBuffer(); err != nil {
		return err
	}
	if source.memoryUsed > spool.memoryBytes-spool.memoryUsed {
		return fmt.Errorf("checker location spool memory limit exceeded")
	}
	spool.memoryRuns = append(spool.memoryRuns, source.memoryRuns...)
	spool.runs = append(spool.runs, source.runs...)
	spool.memoryUsed += source.memoryUsed
	source.memoryRuns = nil
	source.runs = nil
	source.memoryUsed = 0
	return nil
}

type locationRunWriter struct {
	spool    *locationSpool
	file     *os.File
	buffered *bufio.Writer
	path     string
	aead     cipher.AEAD
	prefix   [4]byte
	counter  uint64
	reserved uint64
	size     uint64
	closed   bool
	request  *monitor.DependencyGuard
}

func (spool *locationSpool) newRunWriter(reserved uint64) (_ *locationRunWriter, err error) {
	defer func() {
		if err != nil {
			spool.scratch.release(reserved)
		}
	}()
	block, err := aes.NewCipher(spool.scratch.key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	request := spool.scratch.telemetry.startScratch()
	path, prefix, err := spool.scratch.nextPath()
	if err != nil {
		settleDependency(request, err)
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		settleDependency(request, err)
		return nil, err
	}
	writer := &locationRunWriter{
		spool: spool, file: file, buffered: bufio.NewWriterSize(&scratchOutputWriter{scratch: spool.scratch, request: request, writer: file}, locationRunBufferSize), path: path,
		aead: aead, prefix: prefix, reserved: reserved, size: checkRunHeaderSize, request: request,
	}
	header := append(checkRunMagic[:], writer.prefix[:]...)
	if err := writeAll(writer.buffered, header); err != nil {
		writer.abort()
		reserved = 0
		return nil, err
	}
	return writer, nil
}

func (writer *locationRunWriter) append(tuple locationTuple) error {
	recordBytes := uint64(4 + locationTupleSize + writer.aead.Overhead())
	if writer.size > writer.reserved || recordBytes > writer.reserved-writer.size {
		return fmt.Errorf("checker merge output exceeds reserved size")
	}
	writer.size += recordBytes
	var nonce [12]byte
	copy(nonce[:4], writer.prefix[:])
	binary.BigEndian.PutUint64(nonce[4:], writer.counter)
	writer.counter++
	encoded := tuple.marshalBinary()
	sealed := writer.aead.Seal(nil, nonce[:], encoded[:], checkRunMagic[:])
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(sealed)))
	if err := writeAll(writer.buffered, length[:]); err != nil {
		return err
	}
	if err := writeAll(writer.buffered, sealed); err != nil {
		return err
	}
	return nil
}

func (writer *locationRunWriter) close() (checkRun, error) {
	if writer.closed {
		return checkRun{}, fmt.Errorf("checker run writer is closed")
	}
	writer.closed = true
	if err := writer.buffered.Flush(); err != nil {
		writer.abortFile()
		return checkRun{}, err
	}
	if err := writer.file.Sync(); err != nil {
		writer.abortFile()
		return checkRun{}, err
	}
	if err := writer.file.Close(); err != nil {
		writer.abortFile()
		return checkRun{}, err
	}
	settleDependency(writer.request, nil)
	writer.spool.scratch.release(writer.reserved - writer.size)
	writer.reserved = writer.size
	return checkRun{path: writer.path, size: writer.size}, nil
}

func (writer *locationRunWriter) abort() {
	if !writer.closed {
		writer.closed = true
		_ = writer.file.Close() // Abort preserves the primary writer error.
	}
	writer.abortFile()
}

func (writer *locationRunWriter) abortFile() {
	if writer.request != nil {
		writer.request.Failed()
		writer.request.Done()
	}
	_ = writer.spool.scratch.remove(writer.path) // Session cleanup removes any remaining run.
	writer.spool.scratch.release(writer.reserved)
	writer.reserved = 0
}

func (spool *locationSpool) mergeRuns(runs []checkRun, reserved uint64) (merged checkRun, err error) {
	if spool.scratch.telemetry != nil {
		request := spool.scratch.telemetry.scratchMerge.Start()
		defer func() { settleDependency(request, err) }()
	}
	spool.scratch.recordMerge()
	iterator, err := newLocationIterator(spool.ctx, runs, nil, spool.scratch, spool.deduplicate)
	if err != nil {
		spool.scratch.release(reserved)
		return checkRun{}, err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	writer, err := spool.newRunWriter(reserved)
	if err != nil {
		return checkRun{}, err
	}
	contributions := &packContributionIterator{iterator: iterator, partials: true}
	for {
		var tuple locationTuple
		var found bool
		var err error
		if spool.packSummaries {
			var summary packContributionSummary
			summary, found, err = contributions.next()
			tuple = summary.tuple()
		} else {
			tuple, found, err = iterator.next()
		}
		if err != nil {
			writer.abort()
			return checkRun{}, err
		}
		if !found {
			break
		}
		if err := writer.append(tuple); err != nil {
			writer.abort()
			return checkRun{}, err
		}
	}
	merged, err = writer.close()
	if err != nil {
		return checkRun{}, err
	}
	return merged, nil
}

type locationRunReader struct {
	file    *scratchReadFile
	reader  *bufio.Reader
	aead    cipher.AEAD
	length  [4]byte
	nonce   [12]byte
	sealed  []byte
	counter uint64
}

func openLocationRun(run checkRun, scratch *checkScratch) (*locationRunReader, error) {
	file, err := scratch.open(run.path)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(file, locationRunBufferSize)
	header := make([]byte, checkRunHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		_ = file.Close() // Preserve the header-read error.
		return nil, fmt.Errorf("read checker run header: %w", err)
	}
	if !bytes.Equal(header[:8], checkRunMagic[:]) {
		_ = file.Close() // Preserve the invalid-header error.
		return nil, fmt.Errorf("invalid checker run header")
	}
	block, err := aes.NewCipher(scratch.key[:])
	if err != nil {
		_ = file.Close() // Preserve the cipher-construction error.
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		_ = file.Close() // Preserve the AEAD-construction error.
		return nil, err
	}
	result := &locationRunReader{file: file, reader: reader, aead: aead, sealed: make([]byte, locationTupleSize+aead.Overhead())}
	copy(result.nonce[:4], header[8:])
	return result, nil
}

func (reader *locationRunReader) next() (locationTuple, bool, error) {
	if _, err := io.ReadFull(reader.reader, reader.length[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return locationTuple{}, false, nil
		}
		return locationTuple{}, false, fmt.Errorf("read checker run record length: %w", err)
	}
	sealedLength := binary.BigEndian.Uint32(reader.length[:])
	if sealedLength != locationTupleSize+uint32(reader.aead.Overhead()) {
		return locationTuple{}, false, fmt.Errorf("invalid checker run record length")
	}
	if _, err := io.ReadFull(reader.reader, reader.sealed); err != nil {
		return locationTuple{}, false, fmt.Errorf("read checker run record: %w", err)
	}
	binary.BigEndian.PutUint64(reader.nonce[4:], reader.counter)
	reader.counter++
	plain, err := reader.aead.Open(reader.sealed[:0], reader.nonce[:], reader.sealed, checkRunMagic[:])
	if err != nil {
		return locationTuple{}, false, fmt.Errorf("authenticate checker run record: %w", err)
	}
	tuple, err := unmarshalLocationTuple(plain)
	return tuple, err == nil, err
}

func (reader *locationRunReader) close() error { return reader.file.Close() }

type locationMemoryReader struct {
	records []locationTuple
	index   int
}

func (reader *locationMemoryReader) next() (locationTuple, bool, error) {
	if reader.index >= len(reader.records) {
		return locationTuple{}, false, nil
	}
	tuple := reader.records[reader.index]
	reader.index++
	return tuple, true, nil
}

func (*locationMemoryReader) close() error { return nil }

type locationReader interface {
	next() (locationTuple, bool, error)
	close() error
}

type locationHeapItem struct {
	tuple  locationTuple
	reader int
}

type locationHeap []locationHeapItem

func (items *locationHeap) push(value locationHeapItem) {
	*items = append(*items, value)
	for index := len(*items) - 1; index > 0; {
		parent := (index - 1) / 2
		if compareLocationTuple((*items)[parent].tuple, (*items)[index].tuple) <= 0 {
			break
		}
		(*items)[parent], (*items)[index] = (*items)[index], (*items)[parent]
		index = parent
	}
}

func (items *locationHeap) pop() locationHeapItem {
	result := (*items)[0]
	last := (*items)[len(*items)-1]
	*items = (*items)[:len(*items)-1]
	if len(*items) == 0 {
		return result
	}
	(*items)[0] = last
	items.siftDown()
	return result
}

func (items *locationHeap) siftDown() {
	for index := 0; ; {
		left := index*2 + 1
		if left >= len(*items) {
			break
		}
		smallest := left
		right := left + 1
		if right < len(*items) && compareLocationTuple((*items)[right].tuple, (*items)[left].tuple) < 0 {
			smallest = right
		}
		if compareLocationTuple((*items)[index].tuple, (*items)[smallest].tuple) <= 0 {
			break
		}
		(*items)[index], (*items)[smallest] = (*items)[smallest], (*items)[index]
		index = smallest
	}
}

type locationIterator struct {
	ctx               context.Context
	readers           []locationReader
	heap              locationHeap
	last              locationTuple
	hasLast           bool
	deduplicate       bool
	blobPartitions    []*locationSpool
	partitionIterator *locationIterator
}

func (spool *locationSpool) iterator() (*locationIterator, error) {
	if len(spool.blobPartitions) != 0 {
		return &locationIterator{ctx: spool.ctx, blobPartitions: spool.blobPartitions}, nil
	}
	if err := spool.seal(); err != nil {
		return nil, err
	}
	return newLocationIterator(spool.ctx, spool.runs, spool.memoryRuns, spool.scratch, spool.deduplicate)
}

func (spool *locationSpool) close() error {
	var first error
	for _, partition := range spool.blobPartitions {
		first = errors.Join(first, partition.close())
	}
	spool.blobPartitions = nil
	for _, run := range spool.runs {
		if err := spool.scratch.remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) && first == nil {
			first = err
			continue
		}
		spool.scratch.release(run.size)
	}
	spool.runs = nil
	spool.buffer = nil
	spool.memoryRuns = nil
	spool.memoryUsed = 0
	return first
}

func newLocationIterator(
	ctx context.Context,
	runs []checkRun,
	memoryRuns [][]locationTuple,
	scratch *checkScratch,
	deduplicate bool,
) (*locationIterator, error) {
	iterator := &locationIterator{ctx: ctx, deduplicate: deduplicate}
	for _, run := range runs {
		reader, err := openLocationRun(run, scratch)
		if err != nil {
			return nil, errors.Join(err, iterator.close())
		}
		iterator.readers = append(iterator.readers, reader)
		tuple, found, err := reader.next()
		if err != nil {
			return nil, errors.Join(err, iterator.close())
		}
		if found {
			iterator.heap.push(locationHeapItem{tuple: tuple, reader: len(iterator.readers) - 1})
		}
	}
	for _, records := range memoryRuns {
		reader := &locationMemoryReader{records: records}
		iterator.readers = append(iterator.readers, reader)
		tuple, found, err := reader.next()
		if err != nil {
			return nil, errors.Join(err, iterator.close())
		}
		if found {
			iterator.heap.push(locationHeapItem{tuple: tuple, reader: len(iterator.readers) - 1})
		}
	}
	return iterator, nil
}

func (iterator *locationIterator) next() (locationTuple, bool, error) {
	for len(iterator.blobPartitions) != 0 {
		if err := iterator.ctx.Err(); err != nil {
			return locationTuple{}, false, err
		}
		if iterator.partitionIterator == nil {
			var err error
			iterator.partitionIterator, err = iterator.blobPartitions[0].iterator()
			if err != nil {
				return locationTuple{}, false, err
			}
		}
		tuple, found, err := iterator.partitionIterator.next()
		if err != nil || found {
			return tuple, found, err
		}
		if err := iterator.partitionIterator.close(); err != nil {
			return locationTuple{}, false, err
		}
		iterator.partitionIterator = nil
		iterator.blobPartitions = iterator.blobPartitions[1:]
	}
	for len(iterator.heap) > 0 {
		if err := iterator.ctx.Err(); err != nil {
			return locationTuple{}, false, err
		}
		item := iterator.heap[0]
		next, found, err := iterator.readers[item.reader].next()
		if err != nil {
			return locationTuple{}, false, err
		}
		if found {
			iterator.heap[0] = locationHeapItem{tuple: next, reader: item.reader}
			iterator.heap.siftDown()
		} else {
			iterator.heap.pop()
		}
		if iterator.deduplicate && iterator.hasLast && compareLocationTuple(iterator.last, item.tuple) == 0 {
			continue
		}
		iterator.last, iterator.hasLast = item.tuple, true
		return item.tuple, true, nil
	}
	return locationTuple{}, false, nil
}

func (iterator *locationIterator) close() error {
	var result error
	if iterator.partitionIterator != nil {
		result = iterator.partitionIterator.close()
		iterator.partitionIterator = nil
	}
	iterator.blobPartitions = nil
	for _, reader := range iterator.readers {
		result = errors.Join(result, reader.close())
	}
	return result
}

func countLocationSpool(spool *locationSpool) (count uint64, err error) {
	iterator, err := spool.iterator()
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	for {
		_, found, err := iterator.next()
		if err != nil {
			return 0, err
		}
		if !found {
			return count, nil
		}
		if count == math.MaxUint64 {
			return 0, fmt.Errorf("checker location count overflow")
		}
		count++
	}
}

func legacyInventoryDigest(ctx context.Context, source LegacySource, scratch *checkScratch, memoryBytes uint64) (digest string, err error) {
	spool, err := newLocationSpool(ctx, scratch, max(memoryBytes, locationTupleMemorySize), 32)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, spool.close()) }()
	for kind, fileType := range []vaultic.FileType{vaultic.IndexFile, vaultic.SnapshotFile} {
		if err := source.List(ctx, fileType, func(id vaultic.ID, size int64) error {
			if size < 0 {
				return fmt.Errorf("negative legacy inventory size for %s", id.String())
			}
			return spool.add(locationTuple{BlobID: id, Type: uint8(kind + 1), Length: uint64(size)})
		}); err != nil {
			return "", err
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	hash := sha256.New()
	for {
		tuple, found, err := iterator.next()
		if err != nil {
			return "", err
		}
		if !found {
			return fmt.Sprintf("%x", hash.Sum(nil)), nil
		}
		encoded := tuple.marshalBinary()
		if _, err := hash.Write(encoded[:]); err != nil {
			return "", err
		}
	}
}

func compareLocationSpools(legacy, slatedb *locationSpool, result *CheckResult, maxFindings uint) (err error) {
	return compareLocationSpoolsWithProgress(legacy, slatedb, result, maxFindings, func(string) {})
}

func compareLocationSpoolsWithProgress(legacy, slatedb *locationSpool, result *CheckResult, maxFindings uint, stage func(string)) (err error) {
	stage("location_merge_legacy")
	legacyIterator, err := legacy.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, legacyIterator.close()) }()
	stage("location_merge_slatedb")
	slatedbIterator, err := slatedb.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, slatedbIterator.close()) }()
	stage("location_compare")
	legacyTuple, hasLegacy, err := legacyIterator.next()
	if err != nil {
		return err
	}
	if hasLegacy {
		result.LegacyLocations++
	}
	slatedbTuple, hasSlateDB, err := slatedbIterator.next()
	if err != nil {
		return err
	}
	if hasSlateDB {
		result.SlateDBLocations++
	}
	for hasLegacy || hasSlateDB {
		comparison := 0
		switch {
		case !hasLegacy:
			comparison = 1
		case !hasSlateDB:
			comparison = -1
		default:
			comparison = compareLocationTuple(legacyTuple, slatedbTuple)
		}
		if comparison < 0 {
			result.MissingInSlateDB++
			addFinding(result, maxFindings, Finding{Kind: "missing_blob", Key: legacyTuple.findingKey()})
			legacyTuple, hasLegacy, err = legacyIterator.next()
			if hasLegacy {
				result.LegacyLocations++
			}
		} else if comparison > 0 {
			result.MissingInLegacy++
			addFinding(result, maxFindings, Finding{Kind: "unexpected_blob", Key: slatedbTuple.findingKey()})
			slatedbTuple, hasSlateDB, err = slatedbIterator.next()
			if hasSlateDB {
				result.SlateDBLocations++
			}
		} else {
			legacyTuple, hasLegacy, err = legacyIterator.next()
			if hasLegacy {
				result.LegacyLocations++
			}
			if err == nil {
				slatedbTuple, hasSlateDB, err = slatedbIterator.next()
				if hasSlateDB {
					result.SlateDBLocations++
				}
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func reduceReferenceSpool(spool *locationSpool, result *CheckResult, maxFindings uint) (err error) {
	iterator, err := spool.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	var current vaultic.ID
	var inodes, manifests uint64
	var countInodes, countManifests, total uint64
	var haveGroup, haveCount bool
	flush := func() error {
		if !haveGroup {
			return nil
		}
		if inodes > math.MaxUint64-manifests {
			return fmt.Errorf("reference count overflow for blob %s", current.String())
		}
		minimum := inodes + manifests
		if inodes != 0 || manifests != 0 {
			if !haveCount || countInodes != inodes || countManifests != manifests || total < minimum {
				result.ReverseEdgeMismatch++
				addFinding(result, maxFindings, Finding{
					Kind: "reference_count_drift", Key: current.String(),
					Want: fmt.Sprintf("inodes=%d manifests=%d total>=%d", inodes, manifests, minimum),
					Got:  fmt.Sprintf("inodes=%d manifests=%d total=%d", countInodes, countManifests, total),
				})
			}
			return nil
		}
		if haveCount && (countInodes != 0 || countManifests != 0 || total != 0) {
			result.ReverseEdgeMismatch++
			addFinding(result, maxFindings, Finding{
				Kind: "missing_reverse_edge", Key: current.String(),
				Got: fmt.Sprintf("inodes=%d manifests=%d total=%d", countInodes, countManifests, total),
			})
		}
		return nil
	}
	for {
		tuple, found, nextErr := iterator.next()
		if nextErr != nil {
			return nextErr
		}
		if !found {
			return flush()
		}
		if haveGroup && tuple.BlobID != current {
			if err := flush(); err != nil {
				return err
			}
			inodes, manifests = 0, 0
			countInodes, countManifests, total = 0, 0, 0
			haveCount = false
		}
		current, haveGroup = tuple.BlobID, true
		switch tuple.Type {
		case 1:
			inodes++
		case 2:
			manifests++
		case 3:
			haveCount = true
			countInodes, countManifests, total = tuple.Offset, tuple.Length, tuple.UncompressedLength
		default:
			return fmt.Errorf("invalid checker reference tuple type %d", tuple.Type)
		}
	}
}

type packContributionSummary struct {
	id      vaultic.ID
	present bool
	types   uint8
	count   uint64
	payload uint64
}

const packContributionEntryBudget = 256

type packContributionBuffer struct {
	spool *locationSpool
	limit int
	packs map[vaultic.ID]packContributionSummary
	err   error
}

func (buffer *packContributionBuffer) add(id vaultic.ID, blobType schema.BlobType, length uint64) error {
	if buffer.err != nil {
		return buffer.err
	}
	if err := buffer.spool.ctx.Err(); err != nil {
		buffer.err = err
		return err
	}
	summary, found := buffer.packs[id]
	if !found {
		if len(buffer.packs) >= buffer.limit {
			if err := buffer.flush(); err != nil {
				return err
			}
		}
		if buffer.packs == nil {
			buffer.packs = make(map[vaultic.ID]packContributionSummary)
		}
		summary.id = id
	}
	if summary.count == math.MaxUint64 || length > math.MaxUint64-summary.payload {
		buffer.err = fmt.Errorf("pack %s contribution overflow", id.String())
		return buffer.err
	}
	summary.count++
	summary.payload += length
	summary.types = summarizePackType(summary.types, blobType)
	buffer.packs[id] = summary
	return nil
}

func (buffer *packContributionBuffer) flush() error {
	if buffer.err != nil {
		return buffer.err
	}
	for _, summary := range buffer.packs {
		if err := buffer.spool.addEncoded(summary.tuple()); err != nil {
			buffer.err = err
			return err
		}
	}
	clear(buffer.packs)
	return nil
}

func (summary packContributionSummary) tuple() locationTuple {
	var present uint64
	if summary.present {
		present = 1
	}
	return locationTuple{BlobID: summary.id, Type: summary.types, Offset: summary.count, Length: summary.payload, UncompressedLength: present}
}

func (summary *packContributionSummary) addPartial(tuple locationTuple) error {
	if tuple.Offset > math.MaxUint64-summary.count || tuple.Length > math.MaxUint64-summary.payload {
		return fmt.Errorf("pack %s contribution overflow", summary.id.String())
	}
	summary.count += tuple.Offset
	summary.payload += tuple.Length
	summary.types |= tuple.Type
	summary.present = summary.present || tuple.UncompressedLength != 0
	return nil
}

type packContributionIterator struct {
	iterator *locationIterator
	pending  locationTuple
	has      bool
	partials bool
}

func newPackContributionIterator(spool *locationSpool) (*packContributionIterator, error) {
	if spool.packSummaries && !spool.sealed {
		if err := spool.finishBuffer(); err != nil {
			return nil, err
		}
		if err := spool.compactPackMemory(); err != nil {
			return nil, err
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		return nil, err
	}
	return &packContributionIterator{iterator: iterator, partials: spool.packSummaries}, nil
}

func (spool *locationSpool) compactPackMemory() error {
	if len(spool.memoryRuns) < 2 || spool.memoryUsed >= spool.memoryBytes {
		return nil
	}
	limit := (spool.memoryBytes - spool.memoryUsed) / packContributionEntryBudget
	if limit == 0 {
		return nil
	}
	packs := make(map[vaultic.ID]packContributionSummary)
	for _, run := range spool.memoryRuns {
		for _, tuple := range run {
			if err := spool.ctx.Err(); err != nil {
				return err
			}
			summary, found := packs[tuple.BlobID]
			if !found {
				if uint64(len(packs)) >= limit {
					return nil
				}
				summary.id = tuple.BlobID
			}
			if err := summary.addPartial(tuple); err != nil {
				return err
			}
			packs[tuple.BlobID] = summary
		}
	}
	records := make([]locationTuple, 0, len(packs))
	for _, summary := range packs {
		records = append(records, summary.tuple())
	}
	slices.SortFunc(records, compareLocationTuple)
	if err := spool.ctx.Err(); err != nil {
		return err
	}
	for _, run := range spool.memoryRuns {
		spool.memoryUsed -= uint64(cap(run)) * locationTupleMemorySize
	}
	spool.memoryUsed += uint64(cap(records)) * locationTupleMemorySize
	spool.memoryRuns = [][]locationTuple{records}
	return nil
}

func optionalPackContributionIterator(spool *locationSpool) (*packContributionIterator, error) {
	if spool == nil {
		return nil, nil
	}
	return newPackContributionIterator(spool)
}

func nextPackContribution(iterator *packContributionIterator) (packContributionSummary, bool, error) {
	if iterator == nil {
		return packContributionSummary{}, false, nil
	}
	return iterator.next()
}

func (iterator *packContributionIterator) next() (packContributionSummary, bool, error) {
	if !iterator.has {
		var err error
		iterator.pending, iterator.has, err = iterator.iterator.next()
		if err != nil || !iterator.has {
			return packContributionSummary{}, false, err
		}
	}
	summary := packContributionSummary{id: iterator.pending.BlobID}
	for iterator.has && iterator.pending.BlobID == summary.id {
		tuple := iterator.pending
		if iterator.partials {
			if err := summary.addPartial(tuple); err != nil {
				return packContributionSummary{}, false, err
			}
		} else if tuple.Type == 0 {
			summary.present = true
		} else {
			if summary.count == math.MaxUint64 || summary.payload > math.MaxUint64-tuple.Length {
				return packContributionSummary{}, false, fmt.Errorf("pack %s contribution overflow", summary.id.String())
			}
			summary.count++
			summary.payload += tuple.Length
			summary.types = summarizePackType(summary.types, schema.BlobType(tuple.Type))
		}
		var err error
		iterator.pending, iterator.has, err = iterator.iterator.next()
		if err != nil {
			return packContributionSummary{}, false, err
		}
	}
	return summary, true, nil
}

func (iterator *packContributionIterator) close() error { return iterator.iterator.close() }
