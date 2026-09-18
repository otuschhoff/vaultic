package maintenance

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
)

const checkKVRecordHeaderSize = 12

type checkKVRecord struct {
	key      []byte
	value    []byte
	sequence uint64
}

func (record checkKVRecord) size() uint64 {
	return checkKVRecordHeaderSize + uint64(len(record.key)) + uint64(len(record.value))
}

func compareCheckKV(left, right checkKVRecord) int {
	if compared := bytes.Compare(left.key, right.key); compared != 0 {
		return compared
	}
	return intCompare(left.sequence, right.sequence)
}

func intCompare(left, right uint64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

type checkKVSpool struct {
	ctx         context.Context
	scratch     *checkScratch
	memoryBytes uint64
	fanIn       int
	bufferBytes uint64
	buffer      []checkKVRecord
	runs        []checkRun
	sealed      bool
}

func newCheckKVSpool(ctx context.Context, scratch *checkScratch, memoryBytes uint64, fanIn int) (*checkKVSpool, error) {
	if memoryBytes < checkKVRecordHeaderSize+1 || fanIn < 2 || fanIn > 128 {
		return nil, fmt.Errorf("invalid checker key/value spool memory or fan-in limit")
	}
	return &checkKVSpool{ctx: ctx, scratch: scratch, memoryBytes: memoryBytes, fanIn: fanIn}, nil
}

func (spool *checkKVSpool) add(key, value []byte, sequence uint64) error {
	if spool.sealed {
		return fmt.Errorf("checker key/value spool is sealed")
	}
	record := checkKVRecord{key: append([]byte(nil), key...), value: append([]byte(nil), value...), sequence: sequence}
	if record.size() > spool.memoryBytes {
		return fmt.Errorf("checker key/value record requires %d bytes, exceeding %d-byte memory limit", record.size(), spool.memoryBytes)
	}
	if spool.bufferBytes != 0 && record.size() > spool.memoryBytes-spool.bufferBytes {
		if err := spool.flush(); err != nil {
			return err
		}
	}
	spool.buffer = append(spool.buffer, record)
	spool.bufferBytes += record.size()
	if len(spool.runs) > 0 && spool.bufferBytes >= spool.memoryBytes {
		return spool.flush()
	}
	return nil
}

func (spool *checkKVSpool) sortBuffer() {
	sort.SliceStable(spool.buffer, func(left, right int) bool { return compareCheckKV(spool.buffer[left], spool.buffer[right]) < 0 })
}

func (spool *checkKVSpool) flush() error {
	if len(spool.buffer) == 0 {
		return nil
	}
	spool.sortBuffer()
	writer, err := spool.newWriter()
	if err != nil {
		return err
	}
	for _, record := range spool.buffer {
		if err := writer.append(record); err != nil {
			writer.abort()
			return err
		}
	}
	run, err := writer.close()
	if err != nil {
		return err
	}
	spool.runs = append(spool.runs, run)
	spool.buffer, spool.bufferBytes = nil, 0
	return nil
}

func (spool *checkKVSpool) seal() error {
	if spool.sealed {
		return nil
	}
	if len(spool.runs) == 0 {
		spool.sortBuffer()
	} else {
		if err := spool.flush(); err != nil {
			return err
		}
	}
	for len(spool.runs) > spool.fanIn {
		var reduced []checkRun
		for start := 0; start < len(spool.runs); start += spool.fanIn {
			end := min(start+spool.fanIn, len(spool.runs))
			if end-start == 1 {
				reduced = append(reduced, spool.runs[start])
				continue
			}
			run, err := spool.merge(spool.runs[start:end])
			if err != nil {
				return err
			}
			reduced = append(reduced, run)
		}
		spool.runs = reduced
	}
	spool.sealed = true
	return nil
}

func (spool *checkKVSpool) merge(runs []checkRun) (merged checkRun, err error) {
	spool.scratch.recordMerge()
	iterator, err := newCheckKVIterator(spool.ctx, runs, spool.scratch, spool.memoryBytes)
	if err != nil {
		return checkRun{}, err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	writer, err := spool.newWriter()
	if err != nil {
		return checkRun{}, err
	}
	for {
		record, found, err := iterator.next()
		if err != nil {
			writer.abort()
			return checkRun{}, err
		}
		if !found {
			break
		}
		if err := writer.append(record); err != nil {
			writer.abort()
			return checkRun{}, err
		}
	}
	merged, err = writer.close()
	if err != nil {
		return checkRun{}, err
	}
	for _, run := range runs {
		if err := spool.scratch.remove(run.path); err != nil {
			return checkRun{}, err
		}
		spool.scratch.release(run.size)
	}
	return merged, nil
}

func (spool *checkKVSpool) iterator() (*checkKVIterator, error) {
	if err := spool.seal(); err != nil {
		return nil, err
	}
	if len(spool.runs) == 0 {
		return &checkKVIterator{ctx: spool.ctx, memory: spool.buffer}, nil
	}
	return newCheckKVIterator(spool.ctx, spool.runs, spool.scratch, spool.memoryBytes)
}

func (spool *checkKVSpool) close() error {
	var first error
	for _, run := range spool.runs {
		if err := spool.scratch.remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) && first == nil {
			first = err
		}
		spool.scratch.release(run.size)
	}
	spool.runs, spool.buffer = nil, nil
	spool.bufferBytes = 0
	return first
}

type checkKVWriter struct {
	spool    *checkKVSpool
	file     *os.File
	path     string
	aead     cipher.AEAD
	prefix   [4]byte
	counter  uint64
	reserved uint64
	closed   bool
	request  *monitor.DependencyGuard
}

func (spool *checkKVSpool) newWriter() (*checkKVWriter, error) {
	block, err := aes.NewCipher(spool.scratch.key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if err := spool.scratch.reserve(checkRunHeaderSize); err != nil {
		return nil, err
	}
	request := spool.scratch.telemetry.startScratch()
	path, prefix, err := spool.scratch.nextPath()
	if err != nil {
		spool.scratch.release(checkRunHeaderSize)
		settleDependency(request, err)
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		spool.scratch.release(checkRunHeaderSize)
		settleDependency(request, err)
		return nil, err
	}
	writer := &checkKVWriter{spool: spool, file: file, path: path, aead: aead, prefix: prefix, reserved: checkRunHeaderSize, request: request}
	if err := writeScratch(spool.scratch, request, file, append(checkRunMagic[:], prefix[:]...)); err != nil {
		writer.abort()
		return nil, err
	}
	return writer, nil
}

func (writer *checkKVWriter) append(record checkKVRecord) error {
	plain := make([]byte, checkKVRecordHeaderSize+len(record.key)+len(record.value))
	binary.BigEndian.PutUint32(plain[:4], uint32(len(record.key)))
	binary.BigEndian.PutUint64(plain[4:12], record.sequence)
	copy(plain[12:], record.key)
	copy(plain[12+len(record.key):], record.value)
	var nonce [12]byte
	copy(nonce[:4], writer.prefix[:])
	binary.BigEndian.PutUint64(nonce[4:], writer.counter)
	writer.counter++
	sealed := writer.aead.Seal(nil, nonce[:], plain, checkRunMagic[:])
	recordBytes := uint64(4 + len(sealed))
	if err := writer.spool.scratch.reserve(recordBytes); err != nil {
		return err
	}
	writer.reserved += recordBytes
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(sealed)))
	if err := writeScratch(writer.spool.scratch, writer.request, writer.file, length[:]); err != nil {
		return err
	}
	return writeScratch(writer.spool.scratch, writer.request, writer.file, sealed)
}

func (writer *checkKVWriter) close() (checkRun, error) {
	if writer.closed {
		return checkRun{}, fmt.Errorf("checker key/value writer is closed")
	}
	writer.closed = true
	if err := writer.file.Sync(); err != nil {
		writer.abortFile()
		return checkRun{}, err
	}
	if err := writer.file.Close(); err != nil {
		writer.abortFile()
		return checkRun{}, err
	}
	settleDependency(writer.request, nil)
	return checkRun{path: writer.path, size: writer.reserved}, nil
}

func (writer *checkKVWriter) abort() {
	if !writer.closed {
		writer.closed = true
		_ = writer.file.Close() // Abort preserves the primary writer error.
	}
	writer.abortFile()
}

func (writer *checkKVWriter) abortFile() {
	if writer.request != nil {
		writer.request.Failed()
		writer.request.Done()
	}
	_ = writer.spool.scratch.remove(writer.path) // Session cleanup removes any remaining run.
	writer.spool.scratch.release(writer.reserved)
	writer.reserved = 0
}

type checkKVReader struct {
	file      *scratchReadFile
	reader    *bufio.Reader
	aead      cipher.AEAD
	prefix    [4]byte
	counter   uint64
	maxRecord uint64
}

func openCheckKVReader(run checkRun, scratch *checkScratch, maxRecord uint64) (*checkKVReader, error) {
	file, err := scratch.open(run.path)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(file, 32<<10)
	header := make([]byte, checkRunHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil || !bytes.Equal(header[:8], checkRunMagic[:]) {
		_ = file.Close() // Preserve the invalid-header error.
		return nil, fmt.Errorf("invalid checker key/value run header")
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
	result := &checkKVReader{file: file, reader: reader, aead: aead, maxRecord: maxRecord}
	copy(result.prefix[:], header[8:])
	return result, nil
}

func (reader *checkKVReader) next() (checkKVRecord, bool, error) {
	var length [4]byte
	if _, err := io.ReadFull(reader.reader, length[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return checkKVRecord{}, false, nil
		}
		return checkKVRecord{}, false, err
	}
	sealedLength := uint64(binary.BigEndian.Uint32(length[:]))
	if sealedLength < checkKVRecordHeaderSize+uint64(reader.aead.Overhead()) || sealedLength > reader.maxRecord+uint64(reader.aead.Overhead()) {
		return checkKVRecord{}, false, fmt.Errorf("invalid checker key/value run record length")
	}
	sealed := make([]byte, sealedLength)
	if _, err := io.ReadFull(reader.reader, sealed); err != nil {
		return checkKVRecord{}, false, err
	}
	var nonce [12]byte
	copy(nonce[:4], reader.prefix[:])
	binary.BigEndian.PutUint64(nonce[4:], reader.counter)
	reader.counter++
	plain, err := reader.aead.Open(nil, nonce[:], sealed, checkRunMagic[:])
	if err != nil {
		return checkKVRecord{}, false, fmt.Errorf("authenticate checker key/value record: %w", err)
	}
	keyLength := uint64(binary.BigEndian.Uint32(plain[:4]))
	if keyLength > uint64(len(plain)-checkKVRecordHeaderSize) {
		return checkKVRecord{}, false, fmt.Errorf("invalid checker key/value key length")
	}
	return checkKVRecord{
		key: append([]byte(nil), plain[12:12+keyLength]...), value: append([]byte(nil), plain[12+keyLength:]...),
		sequence: binary.BigEndian.Uint64(plain[4:12]),
	}, true, nil
}

func (reader *checkKVReader) close() error { return reader.file.Close() }

type checkKVHeapItem struct {
	record checkKVRecord
	reader int
}
type checkKVHeap []checkKVHeapItem

func (items checkKVHeap) Len() int { return len(items) }
func (items checkKVHeap) Less(i, j int) bool {
	return compareCheckKV(items[i].record, items[j].record) < 0
}
func (items checkKVHeap) Swap(i, j int)   { items[i], items[j] = items[j], items[i] }
func (items *checkKVHeap) Push(value any) { *items = append(*items, value.(checkKVHeapItem)) }
func (items *checkKVHeap) Pop() any {
	old := *items
	value := old[len(old)-1]
	*items = old[:len(old)-1]
	return value
}

type checkKVIterator struct {
	ctx         context.Context
	readers     []*checkKVReader
	heap        checkKVHeap
	memory      []checkKVRecord
	memoryIndex int
}

func newCheckKVIterator(ctx context.Context, runs []checkRun, scratch *checkScratch, maxRecord uint64) (*checkKVIterator, error) {
	iterator := &checkKVIterator{ctx: ctx}
	for _, run := range runs {
		reader, err := openCheckKVReader(run, scratch, maxRecord)
		if err != nil {
			return nil, errors.Join(err, iterator.close())
		}
		iterator.readers = append(iterator.readers, reader)
		record, found, err := reader.next()
		if err != nil {
			return nil, errors.Join(err, iterator.close())
		}
		if found {
			heap.Push(&iterator.heap, checkKVHeapItem{record: record, reader: len(iterator.readers) - 1})
		}
	}
	return iterator, nil
}

func (iterator *checkKVIterator) next() (checkKVRecord, bool, error) {
	if err := iterator.ctx.Err(); err != nil {
		return checkKVRecord{}, false, err
	}
	if iterator.memory != nil {
		if iterator.memoryIndex >= len(iterator.memory) {
			return checkKVRecord{}, false, nil
		}
		record := iterator.memory[iterator.memoryIndex]
		iterator.memoryIndex++
		return record, true, nil
	}
	if len(iterator.heap) == 0 {
		return checkKVRecord{}, false, nil
	}
	item := heap.Pop(&iterator.heap).(checkKVHeapItem)
	next, found, err := iterator.readers[item.reader].next()
	if err != nil {
		return checkKVRecord{}, false, err
	}
	if found {
		heap.Push(&iterator.heap, checkKVHeapItem{record: next, reader: item.reader})
	}
	return item.record, true, nil
}

func (iterator *checkKVIterator) close() error {
	var first error
	for _, reader := range iterator.readers {
		if err := reader.close(); err != nil && first == nil {
			first = err
		}
	}
	iterator.readers = nil
	return first
}
