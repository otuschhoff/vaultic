//go:build rados || radosfake

package rados

import (
	"bytes"
	"context"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/otuschhoff/vaultic/internal/backend"
)

const (
	manifestMagic = "vaultic-rados-v1\n"
	chunkBytes    = 4 << 20

	orphanChunkGracePeriod      = 15 * time.Minute
	orphanChunkSweepInterval    = 1 * time.Minute
	orphanChunkSweepLimit       = 256
	orphanChunkScanPageLimit    = 512
	maxDetachedReadOperations   = 32
	maxDetachedWriteOperations  = 32
	publicationLeaseSafety      = 5 * time.Second
	publicationLeaseClockSkew   = 2 * time.Second
	publicationLeaseMinimumTTL  = 10 * time.Second
	publicationLeaseMaximumTTL  = 10 * time.Minute
	orphanGCWorkerSweepDeadline = 45 * time.Second
	nativeCloseDrainTimeout     = 2 * time.Second
)

const nativeEnabled = true

type manifest struct {
	Format uint     `json:"format"`
	Size   int64    `json:"size"`
	Digest string   `json:"sha256"`
	Chunks []string `json:"chunks"`
}

type reclamationMarker struct {
	Format uint     `json:"format"`
	Object string   `json:"object"`
	Chunks []string `json:"chunks"`
}

type orphanGCFence struct {
	Format          uint   `json:"format"`
	Owner           string `json:"owner,omitempty"`
	Active          bool   `json:"active"`
	ExpiresUnixNano int64  `json:"expires_unix_nano,omitempty"`
}

type rawObjectStore interface {
	stat(string) (uint64, error)
	read(string, []byte, uint64) (int, error)
	write(string, []byte, bool) error
	compareAndSwap(string, []byte, []byte, bool) (bool, error)
	remove(string) error
	listPage(context.Context, string, string, int) ([]string, string, bool, error)
	close()
}

type orphanGCPhase uint8

const (
	orphanGCCollectRefs orphanGCPhase = iota
	orphanGCDeleteChunks
)

type orphanGCState struct {
	phase            orphanGCPhase
	manifestCursor   string
	leaseCursor      string
	chunkCursor      string
	referenced       map[string]struct{}
	publicationEpoch uint64
}

type nativeDriver struct {
	raw    rawObjectStore
	prefix string
	pool   string
	opTTL  time.Duration

	publicationLeaseKey []byte
	readSlots           chan struct{}
	readMu              sync.Mutex
	readClosing         bool
	readOps             sync.WaitGroup
	writeSlots          chan struct{}
	writeOps            sync.WaitGroup
	publicationMu       sync.RWMutex
	publicationEpoch    atomic.Uint64
	rawCloseOnce        sync.Once

	gcMu            sync.Mutex
	lastOrphanSweep time.Time
	gcState         orphanGCState

	gcTrigger chan struct{}
	gcCancel  context.CancelFunc
	gcDone    chan struct{}
}

type publicationLease struct {
	Format          uint     `json:"format"`
	Object          string   `json:"object"`
	Digest          string   `json:"digest"`
	Chunks          []string `json:"chunks"`
	CreatedUnixNano int64    `json:"created_unix_nano"`
	ExpiresUnixNano int64    `json:"expires_unix_nano"`
	Signature       string   `json:"signature"`
}

type publicationGuard struct {
	native    *nativeDriver
	leaseName string
	lease     publicationLease
	payload   []byte
	released  sync.Once
}

type rawObjectAgeStore interface {
	statWithModTime(string) (uint64, time.Time, bool, error)
}

type rawCapacityTelemetryStore interface {
	capacity(context.Context, string) (backend.CapacityTelemetrySample, error)
}

func (native *nativeDriver) read(ctx context.Context, name string, offset int64, length int) ([]byte, error) {
	encoded, err := native.readRaw(ctx, name, 0, 0)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(string(encoded), manifestMagic) {
		return sliceRange(encoded, offset, length)
	}
	descriptor, err := decodeManifest(name, encoded)
	if err != nil {
		return nil, fmt.Errorf("decode native RADOS manifest %q", name)
	}
	if offset > descriptor.Size || length > 0 && offset+int64(length) > descriptor.Size {
		return nil, ErrRange
	}
	end := descriptor.Size
	if length > 0 {
		end = offset + int64(length)
	}
	result := make([]byte, 0, end-offset)
	for ordinal := int(offset / chunkBytes); int64(ordinal*chunkBytes) < end; ordinal++ {
		if ordinal >= len(descriptor.Chunks) {
			return nil, fmt.Errorf("native RADOS manifest %q is incomplete", name)
		}
		chunk, readErr := native.readRaw(ctx, native.chunkName(name, descriptor.Digest, ordinal), 0, 0)
		if readErr != nil {
			return nil, readErr
		}
		expectedSize := min(int64(chunkBytes), descriptor.Size-int64(ordinal*chunkBytes))
		if int64(len(chunk)) != expectedSize {
			return nil, fmt.Errorf("native RADOS chunk size check failed for %q", name)
		}
		digest := sha256.Sum256(chunk)
		if hex.EncodeToString(digest[:]) != descriptor.Chunks[ordinal] {
			return nil, fmt.Errorf("native RADOS chunk integrity check failed for %q", name)
		}
		chunkStart := int64(ordinal * chunkBytes)
		from := max(int64(0), offset-chunkStart)
		to := min(int64(len(chunk)), end-chunkStart)
		result = append(result, chunk[from:to]...)
	}
	if offset == 0 && end == descriptor.Size && hashBytes(result) != descriptor.Digest {
		return nil, fmt.Errorf("native RADOS object integrity check failed for %q", name)
	}
	return result, ctx.Err()
}

func (native *nativeDriver) stat(ctx context.Context, name string) (int64, error) {
	encoded, err := native.readRaw(ctx, name, 0, 0)
	if err != nil {
		return 0, err
	}
	if !strings.HasPrefix(string(encoded), manifestMagic) {
		return int64(len(encoded)), nil
	}
	descriptor, err := decodeManifest(name, encoded)
	if err != nil {
		return 0, err
	}
	return descriptor.Size, nil
}

func (native *nativeDriver) put(ctx context.Context, name string, data []byte, exclusive bool) error {
	native.publicationMu.RLock()
	defer native.publicationMu.RUnlock()
	_, encoded, guard, err := native.preparePublication(ctx, name, data)
	if err != nil {
		return err
	}
	defer guard.release()
	if err := guard.renew(ctx); err != nil {
		return err
	}
	if err := native.writeRaw(ctx, name, encoded, exclusive); err != nil {
		return err
	}
	native.publicationEpoch.Add(1)
	// A stale marker only delays quota release; the published manifest is already authoritative.
	_ = native.removeRaw(ctx, native.reclamationMarkerName(name))
	native.triggerOrphanSweep()
	return nil
}

func (native *nativeDriver) compareAndSwap(ctx context.Context, name string, expected []byte, replacement []byte) ([]byte, bool, error) {
	native.publicationMu.RLock()
	defer native.publicationMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	_, replacementManifest, guard, err := native.preparePublication(ctx, name, replacement)
	if err != nil {
		return nil, false, err
	}
	defer guard.release()

	createOnly := expected == nil
	if createOnly {
		if err := guard.renew(ctx); err != nil {
			return nil, false, err
		}
		swapped, err := native.compareAndSwapRaw(ctx, name, nil, replacementManifest, true)
		if err != nil {
			return nil, false, mapNativeError(err)
		}
		if !swapped {
			current, readErr := native.read(ctx, name, 0, 0)
			if stderrors.Is(readErr, ErrNotFound) {
				return nil, false, nil
			}
			if readErr != nil {
				return nil, false, readErr
			}
			return current, false, nil
		}
		native.publicationEpoch.Add(1)
		native.triggerOrphanSweep()
		return append([]byte{}, replacement...), true, ctx.Err()
	}

	const compareAndSwapRetries = 8
	for attempt := 0; attempt < compareAndSwapRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		currentEncoded, readErr := native.readRaw(ctx, name, 0, 0)
		if stderrors.Is(readErr, ErrNotFound) {
			return nil, false, nil
		}
		if readErr != nil {
			return nil, false, readErr
		}
		currentLogical, _, decodeErr := native.logicalFromEncoded(ctx, name, currentEncoded)
		if decodeErr != nil {
			return nil, false, decodeErr
		}
		if !bytes.Equal(currentLogical, expected) {
			return currentLogical, false, nil
		}
		if err := guard.renew(ctx); err != nil {
			return nil, false, err
		}

		swapped, casErr := native.compareAndSwapRaw(ctx, name, currentEncoded, replacementManifest, false)
		if casErr != nil {
			return nil, false, mapNativeError(casErr)
		}
		if !swapped {
			continue
		}

		native.publicationEpoch.Add(1)
		native.triggerOrphanSweep()
		return append([]byte{}, replacement...), true, ctx.Err()
	}

	current, readErr := native.read(ctx, name, 0, 0)
	if stderrors.Is(readErr, ErrNotFound) {
		return nil, false, nil
	}
	if readErr != nil {
		return nil, false, readErr
	}
	return current, false, nil
}

func (native *nativeDriver) logicalFromEncoded(ctx context.Context, name string, encoded []byte) ([]byte, *manifest, error) {
	if !strings.HasPrefix(string(encoded), manifestMagic) {
		return append([]byte{}, encoded...), nil, nil
	}
	descriptor, err := decodeManifest(name, encoded)
	if err != nil {
		return nil, nil, err
	}
	if descriptor.Size == 0 {
		return []byte{}, &descriptor, nil
	}
	result := make([]byte, 0, descriptor.Size)
	for ordinal := range descriptor.Chunks {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		chunk, readErr := native.readRaw(ctx, native.chunkName(name, descriptor.Digest, ordinal), 0, 0)
		if readErr != nil {
			return nil, nil, readErr
		}
		expectedSize := min(int64(chunkBytes), descriptor.Size-int64(ordinal*chunkBytes))
		if int64(len(chunk)) != expectedSize {
			return nil, nil, fmt.Errorf("native RADOS chunk size check failed for %q", name)
		}
		chunkDigest := sha256.Sum256(chunk)
		if hex.EncodeToString(chunkDigest[:]) != descriptor.Chunks[ordinal] {
			return nil, nil, fmt.Errorf("native RADOS chunk integrity check failed for %q", name)
		}
		result = append(result, chunk...)
	}
	if hashBytes(result) != descriptor.Digest {
		return nil, nil, fmt.Errorf("native RADOS object integrity check failed for %q", name)
	}
	return result, &descriptor, nil
}

func (native *nativeDriver) capacity(ctx context.Context) (backend.CapacityTelemetrySample, error) {
	provider, ok := native.raw.(rawCapacityTelemetryStore)
	if !ok {
		return backend.CapacityTelemetrySample{}, ErrUnsupported
	}
	result, err := native.runCapacityOperation(ctx, func() rawCapacityResult {
		sample, sampleErr := provider.capacity(ctx, native.pool)
		return rawCapacityResult{sample: sample, err: sampleErr}
	})
	if err != nil {
		return backend.CapacityTelemetrySample{}, err
	}
	return result.sample, result.err
}

func (native *nativeDriver) preparePublication(ctx context.Context, name string, data []byte) (descriptor manifest, encoded []byte, guard *publicationGuard, err error) {
	descriptor, encoded, err = encodeManifestForBytes(data)
	if err != nil {
		return manifest{}, nil, nil, err
	}
	guard, err = native.beginPublicationLease(ctx, name, descriptor)
	if err != nil {
		return manifest{}, nil, nil, err
	}
	digest := descriptor.Digest

	for ordinal, start := 0, 0; start < len(data); ordinal, start = ordinal+1, start+chunkBytes {
		if ctx.Err() != nil {
			return manifest{}, nil, guard, ctx.Err()
		}
		if renewErr := guard.renew(ctx); renewErr != nil {
			return manifest{}, nil, guard, renewErr
		}
		end := min(len(data), start+chunkBytes)
		chunk := data[start:end]
		chunkDigest := sha256.Sum256(chunk)
		chunkName := native.chunkName(name, digest, ordinal)
		if writeErr := native.writeRaw(ctx, chunkName, chunk, true); stderrors.Is(writeErr, ErrExists) {
			existing, readErr := native.readRaw(ctx, chunkName, 0, 0)
			if readErr != nil || hashBytes(existing) != hex.EncodeToString(chunkDigest[:]) {
				return manifest{}, nil, guard, fmt.Errorf("conflicting native RADOS chunk %q: %w", chunkName, writeErr)
			}
		} else if writeErr != nil {
			return manifest{}, nil, guard, writeErr
		}
	}

	return descriptor, encoded, guard, nil
}

func encodeManifestForBytes(data []byte) (manifest, []byte, error) {
	wholeDigest := sha256.Sum256(data)
	digest := hex.EncodeToString(wholeDigest[:])
	descriptor := manifest{Format: 1, Size: int64(len(data)), Digest: digest}
	for start := 0; start < len(data); start += chunkBytes {
		end := min(len(data), start+chunkBytes)
		chunkDigest := sha256.Sum256(data[start:end])
		descriptor.Chunks = append(descriptor.Chunks, hex.EncodeToString(chunkDigest[:]))
	}
	payload, err := json.Marshal(descriptor)
	if err != nil {
		return manifest{}, nil, err
	}
	return descriptor, append([]byte(manifestMagic), payload...), nil
}

func (native *nativeDriver) remove(ctx context.Context, name string) error {
	native.publicationMu.Lock()
	defer native.publicationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, readErr := native.readRaw(ctx, name, 0, 0)
	if readErr == nil && strings.HasPrefix(string(encoded), manifestMagic) {
		descriptor, decodeErr := decodeManifest(name, encoded)
		if decodeErr != nil {
			return decodeErr
		}
		marker := reclamationMarker{Format: 1, Object: name}
		for ordinal := range descriptor.Chunks {
			marker.Chunks = append(marker.Chunks, native.chunkName(name, descriptor.Digest, ordinal))
		}
		markerRaw, marshalErr := json.Marshal(marker)
		if marshalErr != nil {
			return marshalErr
		}
		if writeErr := native.writeRawSettled(ctx, native.reclamationMarkerName(name), markerRaw, false); writeErr != nil {
			return writeErr
		}
	} else if readErr != nil && !stderrors.Is(readErr, ErrNotFound) {
		return readErr
	}
	err := native.removeRawSettled(ctx, name)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		native.triggerOrphanSweep()
	}
	return err
}

func (native *nativeDriver) reclamationMarkerName(name string) string {
	digest := sha256.Sum256([]byte(name))
	return native.prefix + ".vaultic-rados/reclamation/" + hex.EncodeToString(digest[:])
}

func (native *nativeDriver) reclamationPending(ctx context.Context, name string) (bool, error) {
	markerName := native.reclamationMarkerName(name)
	raw, err := native.readRaw(ctx, markerName, 0, 0)
	if stderrors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	var marker reclamationMarker
	if err := json.Unmarshal(raw, &marker); err != nil || marker.Format != 1 || marker.Object != name {
		return true, fmt.Errorf("invalid native RADOS reclamation marker for %q", name)
	}
	for _, chunkName := range marker.Chunks {
		if _, statErr := native.raw.stat(chunkName); statErr == nil {
			return true, nil
		} else if !stderrors.Is(mapNativeError(statErr), ErrNotFound) {
			return true, mapNativeError(statErr)
		}
	}
	if err := native.removeRaw(ctx, markerName); err != nil && !stderrors.Is(err, ErrNotFound) {
		return true, err
	}
	return false, nil
}

func (native *nativeDriver) list(ctx context.Context, prefix string) ([]objectInfo, error) {
	objects := make([]objectInfo, 0)
	cursor := ""
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		names, nextCursor, done, err := native.raw.listPage(ctx, prefix, cursor, orphanChunkScanPageLimit)
		if err != nil {
			return nil, mapNativeError(err)
		}
		for _, name := range names {
			if ctx.Err() != nil || strings.HasPrefix(name, native.prefix+".vaultic-rados/") {
				continue
			}
			size, statErr := native.stat(ctx, name)
			if statErr == nil {
				objects = append(objects, objectInfo{name: name, size: size})
			}
		}
		if done {
			break
		}
		cursor = nextCursor
	}
	return objects, ctx.Err()
}

func (native *nativeDriver) close() error {
	native.readMu.Lock()
	native.readClosing = true
	native.readMu.Unlock()
	if native.gcCancel != nil {
		native.gcCancel()
	}
	drained := make(chan struct{})
	go func() {
		if native.gcDone != nil {
			<-native.gcDone
		}
		native.readOps.Wait()
		native.writeOps.Wait()
		native.rawCloseOnce.Do(native.raw.close)
		close(drained)
	}()
	timer := time.NewTimer(nativeCloseDrainTimeout)
	defer timer.Stop()
	select {
	case <-drained:
		return nil
	case <-timer.C:
		return fmt.Errorf("native RADOS close timed out with detached operations still active")
	}
}

type rawReadResult struct {
	size uint64
	read int
	err  error
}

type rawCapacityResult struct {
	sample backend.CapacityTelemetrySample
	err    error
}

func (native *nativeDriver) runCapacityOperation(ctx context.Context, operation func() rawCapacityResult) (rawCapacityResult, error) {
	select {
	case native.readSlots <- struct{}{}:
	case <-ctx.Done():
		return rawCapacityResult{}, ctx.Err()
	}

	native.readMu.Lock()
	if native.readClosing {
		native.readMu.Unlock()
		<-native.readSlots
		return rawCapacityResult{}, context.Canceled
	}
	native.readOps.Add(1)
	native.readMu.Unlock()

	result := make(chan rawCapacityResult, 1)
	go func() {
		defer func() {
			<-native.readSlots
			native.readOps.Done()
		}()
		result <- operation()
	}()
	select {
	case completed := <-result:
		return completed, nil
	case <-ctx.Done():
		return rawCapacityResult{}, ctx.Err()
	}
}

func (native *nativeDriver) runReadOperation(ctx context.Context, operation func() rawReadResult) (rawReadResult, error) {
	select {
	case native.readSlots <- struct{}{}:
	case <-ctx.Done():
		return rawReadResult{}, ctx.Err()
	}

	native.readMu.Lock()
	if native.readClosing {
		native.readMu.Unlock()
		<-native.readSlots
		return rawReadResult{}, context.Canceled
	}
	native.readOps.Add(1)
	native.readMu.Unlock()

	result := make(chan rawReadResult, 1)
	go func() {
		defer func() {
			<-native.readSlots
			native.readOps.Done()
		}()
		result <- operation()
	}()
	select {
	case completed := <-result:
		return completed, nil
	case <-ctx.Done():
		return rawReadResult{}, ctx.Err()
	}
}

func (native *nativeDriver) readRaw(ctx context.Context, name string, offset int64, length int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	statResult, err := native.runReadOperation(ctx, func() rawReadResult {
		size, statErr := native.raw.stat(name)
		return rawReadResult{size: size, err: statErr}
	})
	if err != nil {
		return nil, err
	}
	size := statResult.size
	err = statResult.err
	if err != nil {
		return nil, mapNativeError(err)
	}
	if offset > int64(size) || length > 0 && offset+int64(length) > int64(size) {
		return nil, ErrRange
	}
	readLength := int64(size) - offset
	if length > 0 {
		readLength = int64(length)
	}
	buffer := make([]byte, readLength)
	readResult, err := native.runReadOperation(ctx, func() rawReadResult {
		read, readErr := native.raw.read(name, buffer, uint64(offset))
		return rawReadResult{read: read, err: readErr}
	})
	if err != nil {
		return nil, err
	}
	read := readResult.read
	err = readResult.err
	if err != nil {
		return nil, mapNativeError(err)
	}
	if read != len(buffer) {
		return nil, ErrRange
	}
	return buffer, ctx.Err()
}

func (native *nativeDriver) writeRaw(ctx context.Context, name string, data []byte, exclusive bool) error {
	result, err := native.runMutationOperation(ctx, func() rawMutationResult {
		return rawMutationResult{err: native.raw.write(name, data, exclusive)}
	})
	if err != nil {
		return err
	}
	return mapNativeError(result.err)
}

func (native *nativeDriver) writeRawSettled(ctx context.Context, name string, data []byte, exclusive bool) error {
	result, err := native.runSettledMutationOperation(ctx, func() rawMutationResult {
		return rawMutationResult{err: native.raw.write(name, data, exclusive)}
	})
	if result.err != nil {
		return mapNativeError(result.err)
	}
	return err
}

type rawMutationResult struct {
	swapped bool
	err     error
}

func (native *nativeDriver) runMutationOperation(ctx context.Context, operation func() rawMutationResult) (rawMutationResult, error) {
	return native.runMutationOperationWithMode(ctx, operation, false)
}

func (native *nativeDriver) runSettledMutationOperation(ctx context.Context, operation func() rawMutationResult) (rawMutationResult, error) {
	return native.runMutationOperationWithMode(ctx, operation, true)
}

func (native *nativeDriver) runMutationOperationWithMode(
	ctx context.Context,
	operation func() rawMutationResult,
	waitAfterCancellation bool,
) (rawMutationResult, error) {
	if err := ctx.Err(); err != nil {
		return rawMutationResult{}, err
	}
	select {
	case native.writeSlots <- struct{}{}:
	case <-ctx.Done():
		return rawMutationResult{}, ctx.Err()
	}
	native.readMu.Lock()
	if native.readClosing {
		native.readMu.Unlock()
		<-native.writeSlots
		return rawMutationResult{}, context.Canceled
	}
	native.writeOps.Add(1)
	native.readMu.Unlock()
	result := make(chan rawMutationResult, 1)
	go func() {
		defer func() {
			<-native.writeSlots
			native.writeOps.Done()
		}()
		result <- operation()
	}()
	select {
	case completed := <-result:
		return completed, ctx.Err()
	case <-ctx.Done():
		if waitAfterCancellation {
			return <-result, ctx.Err()
		}
		return rawMutationResult{}, ctx.Err()
	}
}

func (native *nativeDriver) compareAndSwapRaw(ctx context.Context, name string, expected []byte, replacement []byte, createOnly bool) (bool, error) {
	result, err := native.runMutationOperation(ctx, func() rawMutationResult {
		swapped, operationErr := native.raw.compareAndSwap(name, expected, replacement, createOnly)
		return rawMutationResult{swapped: swapped, err: operationErr}
	})
	if err != nil {
		return false, err
	}
	return result.swapped, mapNativeError(result.err)
}

func (native *nativeDriver) removeRaw(ctx context.Context, name string) error {
	result, err := native.runMutationOperation(ctx, func() rawMutationResult {
		return rawMutationResult{err: native.raw.remove(name)}
	})
	if err != nil {
		return err
	}
	return mapNativeError(result.err)
}

func (native *nativeDriver) removeRawSettled(ctx context.Context, name string) error {
	result, err := native.runSettledMutationOperation(ctx, func() rawMutationResult {
		return rawMutationResult{err: native.raw.remove(name)}
	})
	if result.err != nil {
		return mapNativeError(result.err)
	}
	return err
}

func (native *nativeDriver) chunkName(name, digest string, ordinal int) string {
	objectDigest := sha256.Sum256([]byte(name))
	return path.Join(native.prefix, ".vaultic-rados", "chunks", hex.EncodeToString(objectDigest[:]), digest, fmt.Sprintf("%08x", ordinal))
}

func (native *nativeDriver) beginPublicationLease(ctx context.Context, name string, descriptor manifest) (*publicationGuard, error) {
	blocked, err := native.publicationBlockedByOrphanGC(ctx, time.Now())
	if err != nil || blocked {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("native RADOS orphan GC fence is active")
	}
	leaseName, payload, err := native.publicationLeasePayload(name, descriptor)
	if err != nil {
		return nil, err
	}
	if err := native.writeRaw(ctx, leaseName, payload, true); err != nil {
		return nil, err
	}
	blocked, err = native.publicationBlockedByOrphanGC(ctx, time.Now())
	if err != nil || blocked {
		_ = native.removeRaw(ctx, leaseName) // Preserve the fence error; a stale lease only delays orphan collection.
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("native RADOS orphan GC fence became active")
	}
	var lease publicationLease
	if err := json.Unmarshal(payload, &lease); err != nil {
		return nil, err
	}
	return &publicationGuard{native: native, leaseName: leaseName, lease: lease, payload: payload}, nil
}

func (guard *publicationGuard) renew(ctx context.Context) error {
	now := time.Now()
	protectedUntil := now.Add(guard.native.opTTL + publicationLeaseClockSkew)
	if protectedUntil.UnixNano() < guard.lease.ExpiresUnixNano {
		return nil
	}
	next := guard.lease
	next.CreatedUnixNano = now.UnixNano()
	next.ExpiresUnixNano = now.Add(guard.native.publicationLeaseTTL()).UnixNano()
	next.Signature = ""
	next.Signature = guard.native.signPublicationLease(next)
	replacement, err := json.Marshal(next)
	if err != nil {
		return err
	}
	swapped, err := guard.native.compareAndSwapRaw(ctx, guard.leaseName, guard.payload, replacement, false)
	if err != nil {
		readback, readErr := guard.native.readRaw(context.WithoutCancel(ctx), guard.leaseName, 0, 0)
		if readErr == nil && bytes.Equal(readback, replacement) {
			guard.lease, guard.payload = next, replacement
			return nil
		}
		return err
	}
	if !swapped {
		return fmt.Errorf("native RADOS publication lease renewal lost ownership")
	}
	guard.lease, guard.payload = next, replacement
	return nil
}

func (guard *publicationGuard) release() {
	guard.released.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), guard.native.opTTL)
		defer cancel()
		if err := guard.native.removeRaw(ctx, guard.leaseName); err != nil {
			// Lease cleanup is best effort; stale leases only delay orphan GC.
		}
	})
}

func (native *nativeDriver) orphanGCFenceName() string {
	return path.Join(native.prefix, ".vaultic-rados", "orphan-gc-fence.json")
}

func (native *nativeDriver) publicationBlockedByOrphanGC(ctx context.Context, now time.Time) (bool, error) {
	raw, err := native.readRaw(ctx, native.orphanGCFenceName(), 0, 0)
	if stderrors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	var fence orphanGCFence
	if json.Unmarshal(raw, &fence) != nil || fence.Format != 1 {
		return true, fmt.Errorf("native RADOS orphan GC fence is malformed")
	}
	return fence.Active && fence.ExpiresUnixNano > now.UnixNano(), nil
}

func (native *nativeDriver) acquireOrphanGCFence(ctx context.Context, now time.Time) ([]byte, bool, error) {
	owner := readCacheFenceID()
	fenceTTL := max(orphanGCWorkerSweepDeadline, native.opTTL) + publicationLeaseClockSkew
	next, err := json.Marshal(orphanGCFence{
		Format: 1, Owner: owner, Active: true,
		ExpiresUnixNano: now.Add(fenceTTL).UnixNano(),
	})
	if err != nil {
		return nil, false, err
	}
	name := native.orphanGCFenceName()
	for attempt := 0; attempt < 4; attempt++ {
		current, readErr := native.readRaw(ctx, name, 0, 0)
		if stderrors.Is(readErr, ErrNotFound) {
			swapped, swapErr := native.compareAndSwapRaw(ctx, name, nil, next, true)
			if swapErr != nil {
				return nil, false, swapErr
			}
			if swapped {
				return next, true, nil
			}
			continue
		}
		if readErr != nil {
			return nil, false, readErr
		}
		var fence orphanGCFence
		if json.Unmarshal(current, &fence) != nil || fence.Format != 1 {
			return nil, false, fmt.Errorf("native RADOS orphan GC fence is malformed")
		}
		if fence.Active && fence.ExpiresUnixNano > now.UnixNano() {
			return nil, false, nil
		}
		swapped, swapErr := native.compareAndSwapRaw(ctx, name, current, next, false)
		if swapErr != nil {
			return nil, false, swapErr
		}
		if swapped {
			return next, true, nil
		}
	}
	return nil, false, nil
}

func readCacheFenceID() string {
	value := make([]byte, 16)
	if _, err := crand.Read(value); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(value)
}

func (native *nativeDriver) releaseOrphanGCFence(ctx context.Context, active []byte) {
	inactive, err := json.Marshal(orphanGCFence{Format: 1})
	if err != nil {
		return
	}
	_, _ = native.compareAndSwapRaw(ctx, native.orphanGCFenceName(), active, inactive, false) // Fence expiry keeps failed cleanup bounded and fail-closed.
}

func (native *nativeDriver) publicationLeasePayload(name string, descriptor manifest) (string, []byte, error) {
	leaseID := make([]byte, 16)
	if _, err := crand.Read(leaseID); err != nil {
		return "", nil, err
	}
	created := time.Now()
	leaseTTL := native.publicationLeaseTTL()
	objectDigest := sha256.Sum256([]byte(name))
	chunks := make([]string, 0, len(descriptor.Chunks))
	for ordinal := range descriptor.Chunks {
		chunks = append(chunks, native.chunkName(name, descriptor.Digest, ordinal))
	}
	leaseName := path.Join(
		native.prefix,
		".vaultic-rados",
		"publication-leases",
		hex.EncodeToString(objectDigest[:]),
		hex.EncodeToString(leaseID)+".json",
	)
	lease := publicationLease{
		Format:          1,
		Object:          name,
		Digest:          descriptor.Digest,
		Chunks:          chunks,
		CreatedUnixNano: created.UnixNano(),
		ExpiresUnixNano: created.Add(leaseTTL).UnixNano(),
	}
	lease.Signature = native.signPublicationLease(lease)
	payload, err := json.Marshal(lease)
	if err != nil {
		return "", nil, err
	}
	return leaseName, payload, nil
}

func derivePublicationLeaseKey(secret string, cluster string, pool string, namespace string, prefix string, client string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	parts := []string{"vaultic-rados-publication-lease-v1"}
	parts = append(parts, canonicalPublicationLeaseDomain(cluster, pool, namespace, prefix, client)...)
	for _, part := range parts {
		if _, err := mac.Write([]byte(part)); err != nil {
			continue
		}
		if _, err := mac.Write([]byte{0}); err != nil {
			continue
		}
	}
	return mac.Sum(nil)
}

func canonicalPublicationLeaseDomain(cluster string, pool string, namespace string, prefix string, client string) []string {
	canonicalFSID := strings.ToLower(strings.TrimSpace(cluster))
	if parsed, err := uuid.Parse(canonicalFSID); err == nil {
		canonicalFSID = parsed.String()
	}
	canonicalPool := strings.TrimSpace(pool)
	canonicalNamespace := strings.TrimSpace(namespace)
	canonicalPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	if canonicalPrefix == "" {
		canonicalPrefix = "/"
	} else {
		canonicalPrefix += "/"
	}
	canonicalClient := strings.TrimSpace(client)
	canonicalClient = strings.TrimPrefix(strings.ToLower(canonicalClient), "client.")

	parts := []string{canonicalFSID, canonicalPool, canonicalNamespace, canonicalPrefix}
	if canonicalClient != "" {
		parts = append(parts, canonicalClient)
	}
	return parts
}

func (native *nativeDriver) signPublicationLease(lease publicationLease) string {
	if len(native.publicationLeaseKey) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, native.publicationLeaseKey)
	for _, field := range []string{
		strconv.FormatUint(uint64(lease.Format), 10),
		lease.Object,
		lease.Digest,
		strconv.FormatInt(lease.CreatedUnixNano, 10),
		strconv.FormatInt(lease.ExpiresUnixNano, 10),
		strconv.Itoa(len(lease.Chunks)),
	} {
		if _, err := mac.Write([]byte(field)); err != nil {
			return ""
		}
		if _, err := mac.Write([]byte{0}); err != nil {
			return ""
		}
	}
	for _, chunk := range lease.Chunks {
		if _, err := mac.Write([]byte(chunk)); err != nil {
			return ""
		}
		if _, err := mac.Write([]byte{0}); err != nil {
			return ""
		}
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func (native *nativeDriver) publicationLeaseTTL() time.Duration {
	leaseTTL := native.opTTL + publicationLeaseSafety
	if leaseTTL < publicationLeaseMinimumTTL {
		leaseTTL = publicationLeaseMinimumTTL
	}
	if leaseTTL > publicationLeaseMaximumTTL {
		leaseTTL = publicationLeaseMaximumTTL
	}
	return leaseTTL
}

func (native *nativeDriver) resetOrphanGCState() {
	native.gcState = orphanGCState{
		phase: orphanGCCollectRefs, referenced: make(map[string]struct{}),
		publicationEpoch: native.publicationEpoch.Load(),
	}
}

func (native *nativeDriver) startOrphanGCWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	native.gcCancel = cancel
	native.gcTrigger = make(chan struct{}, 1)
	native.gcDone = make(chan struct{})
	go native.runOrphanGCWorker(ctx)
}

func (native *nativeDriver) triggerOrphanSweep() {
	if native.gcTrigger == nil {
		return
	}
	select {
	case native.gcTrigger <- struct{}{}:
	default:
	}
}

func (native *nativeDriver) runOrphanGCWorker(ctx context.Context) {
	defer close(native.gcDone)
	ticker := time.NewTicker(orphanChunkSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-native.gcTrigger:
		case <-ticker.C:
		}
		now := time.Now()
		native.gcMu.Lock()
		if !native.lastOrphanSweep.IsZero() && now.Sub(native.lastOrphanSweep) < orphanChunkSweepInterval {
			native.gcMu.Unlock()
			continue
		}
		native.lastOrphanSweep = now
		native.gcMu.Unlock()
		sweepTimeout := native.opTTL
		if sweepTimeout <= 0 || sweepTimeout > orphanGCWorkerSweepDeadline {
			sweepTimeout = orphanGCWorkerSweepDeadline
		}
		sweepCtx, cancel := context.WithTimeout(ctx, sweepTimeout)
		// Sweep failures are intentionally non-fatal for foreground operations.
		_ = native.sweepOrphanChunks(sweepCtx, now, orphanChunkGracePeriod, orphanChunkSweepLimit, orphanChunkScanPageLimit)
		cancel()
	}
}

func (native *nativeDriver) sweepOrphanChunks(ctx context.Context, now time.Time, grace time.Duration, deleteLimit int, pageLimit int) error {
	ageStore, ok := native.raw.(rawObjectAgeStore)
	if !ok {
		return nil
	}
	if grace <= 0 || deleteLimit <= 0 || pageLimit <= 0 {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	chunkPrefix := path.Join(native.prefix, ".vaultic-rados", "chunks") + "/"
	leasePrefix := path.Join(native.prefix, ".vaultic-rados", "publication-leases") + "/"

	native.gcMu.Lock()
	if native.gcState.referenced == nil {
		native.resetOrphanGCState()
	}
	state := native.gcState
	native.gcMu.Unlock()

	if state.phase == orphanGCCollectRefs {
		manifestsDone, manifestCursor, err := native.collectManifestReferencesPage(ctx, &state, pageLimit)
		if err != nil {
			return err
		}
		state.manifestCursor = manifestCursor
		if !manifestsDone {
			native.gcMu.Lock()
			native.gcState = state
			native.gcMu.Unlock()
			return nil
		}

		leasesDone, leaseCursor, err := native.collectLeaseReferencesPage(ctx, &state, now, leasePrefix, pageLimit)
		if err != nil {
			return err
		}
		state.leaseCursor = leaseCursor
		if !leasesDone {
			native.gcMu.Lock()
			native.gcState = state
			native.gcMu.Unlock()
			return nil
		}

		state.phase = orphanGCDeleteChunks
		state.manifestCursor = ""
		state.leaseCursor = ""
		state.chunkCursor = ""
	}

	if state.phase == orphanGCDeleteChunks {
		native.publicationMu.Lock()
		if state.publicationEpoch != native.publicationEpoch.Load() {
			native.gcMu.Lock()
			native.resetOrphanGCState()
			native.gcMu.Unlock()
			native.publicationMu.Unlock()
			return nil
		}
		fence, acquired, fenceErr := native.acquireOrphanGCFence(ctx, now)
		if fenceErr != nil || !acquired {
			native.publicationMu.Unlock()
			return fenceErr
		}
		defer native.releaseOrphanGCFence(context.WithoutCancel(ctx), fence)
		state.leaseCursor = ""
		for {
			done, cursor, collectErr := native.collectLeaseReferencesPage(ctx, &state, now, leasePrefix, pageLimit)
			if collectErr != nil {
				native.publicationMu.Unlock()
				return collectErr
			}
			state.leaseCursor = cursor
			if done {
				break
			}
		}
		state.manifestCursor = ""
		for {
			done, cursor, collectErr := native.collectManifestReferencesPage(ctx, &state, pageLimit)
			if collectErr != nil {
				native.publicationMu.Unlock()
				return collectErr
			}
			state.manifestCursor = cursor
			if done {
				break
			}
		}
		chunksDone, chunkCursor, err := native.deleteOrphanChunkPage(ctx, now, grace, deleteLimit, pageLimit, chunkPrefix, &state, ageStore)
		native.publicationMu.Unlock()
		if err != nil {
			return err
		}
		state.chunkCursor = chunkCursor
		if chunksDone {
			native.gcMu.Lock()
			native.resetOrphanGCState()
			native.gcMu.Unlock()
			return nil
		}
	}

	native.gcMu.Lock()
	native.gcState = state
	native.gcMu.Unlock()
	return nil
}

func (native *nativeDriver) collectManifestReferencesPage(ctx context.Context, state *orphanGCState, pageLimit int) (bool, string, error) {
	page, nextCursor, done, err := native.raw.listPage(ctx, native.prefix, state.manifestCursor, pageLimit)
	if err != nil {
		if stderrors.Is(err, errNativeListCursorExpired) {
			state.manifestCursor = ""
			native.gcMu.Lock()
			native.gcState = *state
			native.gcMu.Unlock()
		}
		return false, state.manifestCursor, mapNativeError(err)
	}
	for _, name := range page {
		if ctx.Err() != nil {
			return false, state.manifestCursor, ctx.Err()
		}
		if strings.HasPrefix(name, native.prefix+".vaultic-rados/") {
			continue
		}
		encoded, readErr := native.readRaw(ctx, name, 0, 0)
		if stderrors.Is(readErr, ErrNotFound) {
			continue
		}
		if readErr != nil {
			return false, state.manifestCursor, readErr
		}
		if !strings.HasPrefix(string(encoded), manifestMagic) {
			continue
		}
		descriptor, decodeErr := decodeManifest(name, encoded)
		if decodeErr != nil {
			return false, state.manifestCursor, decodeErr
		}
		for ordinal := range descriptor.Chunks {
			state.referenced[native.chunkName(name, descriptor.Digest, ordinal)] = struct{}{}
		}
	}
	if done {
		return true, "", nil
	}
	return false, nextCursor, nil
}

func (native *nativeDriver) collectLeaseReferencesPage(ctx context.Context, state *orphanGCState, now time.Time, leasePrefix string, pageLimit int) (bool, string, error) {
	page, nextCursor, done, err := native.raw.listPage(ctx, leasePrefix, state.leaseCursor, pageLimit)
	if err != nil {
		if stderrors.Is(err, errNativeListCursorExpired) {
			state.leaseCursor = ""
			native.gcMu.Lock()
			native.gcState = *state
			native.gcMu.Unlock()
		}
		return false, state.leaseCursor, mapNativeError(err)
	}
	for _, leaseName := range page {
		if ctx.Err() != nil {
			return false, state.leaseCursor, ctx.Err()
		}
		encoded, readErr := native.readRaw(ctx, leaseName, 0, 0)
		if stderrors.Is(readErr, ErrNotFound) {
			continue
		}
		if readErr != nil {
			return false, state.leaseCursor, readErr
		}
		chunks, live, valid := native.validatedLeaseChunks(leaseName, encoded, now)
		if !valid {
			return false, state.leaseCursor, fmt.Errorf("native RADOS publication lease %q is malformed", leaseName)
		}
		if !live {
			if removeErr := native.removeRaw(ctx, leaseName); removeErr != nil && !stderrors.Is(removeErr, ErrNotFound) {
				return false, state.leaseCursor, removeErr
			}
			continue
		}
		for _, chunkName := range chunks {
			state.referenced[chunkName] = struct{}{}
		}
	}
	if done {
		return true, "", nil
	}
	return false, nextCursor, nil
}

func (native *nativeDriver) deleteOrphanChunkPage(ctx context.Context, now time.Time, grace time.Duration, deleteLimit int, pageLimit int, chunkPrefix string, state *orphanGCState, ageStore rawObjectAgeStore) (bool, string, error) {
	page, nextCursor, done, err := native.raw.listPage(ctx, chunkPrefix, state.chunkCursor, pageLimit)
	if err != nil {
		if stderrors.Is(err, errNativeListCursorExpired) {
			state.chunkCursor = ""
			native.gcMu.Lock()
			native.gcState = *state
			native.gcMu.Unlock()
		}
		return false, state.chunkCursor, mapNativeError(err)
	}
	removed := 0
	for _, chunkName := range page {
		if ctx.Err() != nil {
			return false, state.chunkCursor, ctx.Err()
		}
		if _, keep := state.referenced[chunkName]; keep {
			continue
		}
		_, modified, known, statErr := ageStore.statWithModTime(chunkName)
		if statErr != nil || !known {
			continue
		}
		if now.Sub(modified) < grace || removed >= deleteLimit {
			continue
		}
		removeErr := native.removeRawSettled(ctx, chunkName)
		if removeErr == nil || stderrors.Is(removeErr, ErrNotFound) {
			removed++
			continue
		}
		return false, state.chunkCursor, removeErr
	}
	if done {
		return true, "", nil
	}
	return false, nextCursor, nil
}

func (native *nativeDriver) liveLeaseChunks(leaseName string, payload []byte, now time.Time) ([]string, bool) {
	chunks, live, _ := native.validatedLeaseChunks(leaseName, payload, now)
	return chunks, live
}

func (native *nativeDriver) validatedLeaseChunks(leaseName string, payload []byte, now time.Time) ([]string, bool, bool) {
	var lease publicationLease
	if json.Unmarshal(payload, &lease) != nil || lease.Format != 1 || !isHexString(lease.Digest, sha256.Size*2) {
		return nil, false, false
	}
	if len(native.publicationLeaseKey) == 0 || !isHexString(lease.Signature, sha256.Size*2) {
		return nil, false, false
	}
	if lease.Object == "" || lease.CreatedUnixNano <= 0 || lease.ExpiresUnixNano <= 0 || lease.ExpiresUnixNano < lease.CreatedUnixNano {
		return nil, false, false
	}
	unsignedLease := lease
	unsignedLease.Signature = ""
	expectedSignature := native.signPublicationLease(unsignedLease)
	provided, decodeProvidedErr := hex.DecodeString(lease.Signature)
	expected, decodeExpectedErr := hex.DecodeString(expectedSignature)
	if decodeProvidedErr != nil || decodeExpectedErr != nil || !hmac.Equal(provided, expected) {
		return nil, false, false
	}
	objectDigest := sha256.Sum256([]byte(lease.Object))
	expectedLeasePrefix := path.Join(native.prefix, ".vaultic-rados", "publication-leases", hex.EncodeToString(objectDigest[:])) + "/"
	if !strings.HasPrefix(leaseName, expectedLeasePrefix) {
		return nil, false, false
	}
	for ordinal, chunk := range lease.Chunks {
		if chunk != native.chunkName(lease.Object, lease.Digest, ordinal) {
			return nil, false, false
		}
	}
	maxLeaseLifetime := native.publicationLeaseTTL() + publicationLeaseClockSkew
	leaseLifetime := time.Duration(lease.ExpiresUnixNano - lease.CreatedUnixNano)
	if leaseLifetime <= 0 || leaseLifetime > maxLeaseLifetime {
		return nil, false, false
	}
	createdAt := time.Unix(0, lease.CreatedUnixNano)
	expiresAt := time.Unix(0, lease.ExpiresUnixNano)
	if createdAt.After(now.Add(publicationLeaseClockSkew)) {
		return nil, false, false
	}
	if expiresAt.After(now.Add(maxLeaseLifetime)) {
		return nil, false, false
	}
	if now.After(expiresAt.Add(publicationLeaseClockSkew)) {
		return nil, false, true
	}
	return lease.Chunks, true, true
}

func isHexString(value string, expectedLen int) bool {
	if len(value) != expectedLen {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func decodeManifest(name string, encoded []byte) (manifest, error) {
	var descriptor manifest
	if !strings.HasPrefix(string(encoded), manifestMagic) {
		return descriptor, fmt.Errorf("native RADOS object %q is not a manifest", name)
	}
	if err := json.Unmarshal(encoded[len(manifestMagic):], &descriptor); err != nil || descriptor.Format != 1 || descriptor.Size < 0 || len(descriptor.Digest) != sha256.Size*2 || len(descriptor.Chunks) != int((descriptor.Size+chunkBytes-1)/chunkBytes) {
		return manifest{}, fmt.Errorf("decode native RADOS manifest %q", name)
	}
	return descriptor, nil
}

func sliceRange(data []byte, offset int64, length int) ([]byte, error) {
	if offset < 0 || offset > int64(len(data)) || length < 0 || length > 0 && offset+int64(length) > int64(len(data)) {
		return nil, ErrRange
	}
	end := len(data)
	if length > 0 {
		end = int(offset) + length
	}
	return data[offset:end], nil
}

func hashBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func saturatingSub(left, right uint64) uint64 {
	if right >= left {
		return 0
	}
	return left - right
}

func logicalToRawHeadroom(logical uint64, amplification float64) uint64 {
	if amplification < 1 || math.IsNaN(amplification) || math.IsInf(amplification, 0) {
		amplification = 1
	}
	if float64(logical) > float64(math.MaxUint64)/amplification {
		return math.MaxUint64
	}
	return uint64(math.Floor(float64(logical) * amplification))
}

func mapNativeError(err error) error {
	var coded interface{ ErrorCode() int }
	errno := 0
	if stderrors.As(err, &coded) {
		errno = coded.ErrorCode()
		if errno < 0 {
			errno = -errno
		}
	}
	switch {
	case err == nil:
		return nil
	case errno == int(syscall.ENOENT), stderrors.Is(err, syscall.ENOENT):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errno == int(syscall.EEXIST), stderrors.Is(err, syscall.EEXIST):
		return fmt.Errorf("%w: %v", ErrExists, err)
	case errno == int(syscall.ERANGE), stderrors.Is(err, syscall.ERANGE):
		return fmt.Errorf("%w: %v", ErrRange, err)
	case errno == int(syscall.EACCES), errno == int(syscall.EPERM), errno == int(syscall.EINVAL), stderrors.Is(err, syscall.EACCES), stderrors.Is(err, syscall.EPERM), stderrors.Is(err, syscall.EINVAL):
		return backoff.Permanent(err)
	default:
		return err
	}
}
