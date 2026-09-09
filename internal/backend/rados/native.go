//go:build rados

package rados

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/cenkalti/backoff/v4"
	cephrados "github.com/ceph/go-ceph/rados"
)

const (
	manifestMagic = "vaultic-rados-v1\n"
	chunkBytes    = 4 << 20
)

const nativeEnabled = true

type manifest struct {
	Format uint     `json:"format"`
	Size   int64    `json:"size"`
	Digest string   `json:"sha256"`
	Chunks []string `json:"chunks"`
}

type rawObjectStore interface {
	stat(string) (uint64, error)
	read(string, []byte, uint64) (int, error)
	write(string, []byte, bool) error
	remove(string) error
	list(func(string)) error
	close()
}

type nativeDriver struct {
	raw    rawObjectStore
	prefix string
}

type cephObjectStore struct {
	connection *cephrados.Conn
	ioctx      *cephrados.IOContext
	mu         sync.Mutex
}

func openNative(ctx context.Context, config Config) (driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connection, err := cephrados.NewConnWithUser(strings.TrimPrefix(config.Client, "client."))
	if err != nil {
		return nil, mapNativeError(err)
	}
	failed := true
	defer func() {
		if failed {
			connection.Shutdown()
		}
	}()
	timeout := fmt.Sprintf("%d", max(1, int(config.OperationTTL.Seconds())))
	for option, value := range map[string]string{
		"mon_host": config.Monitors, "key": config.Key.Unwrap(),
		"rados_osd_op_timeout": timeout, "rados_mon_op_timeout": timeout,
		"client_mount_timeout": timeout,
	} {
		if err := connection.SetConfigOption(option, value); err != nil {
			return nil, fmt.Errorf("configure native RADOS %s: %w", option, mapNativeError(err))
		}
	}
	if err := connection.Connect(); err != nil {
		return nil, fmt.Errorf("connect native RADOS: %w", mapNativeError(err))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fsid, err := connection.GetFSID()
	if err != nil {
		return nil, fmt.Errorf("read native RADOS cluster identity: %w", mapNativeError(err))
	}
	if !strings.EqualFold(fsid, config.ClusterFSID) {
		return nil, fmt.Errorf("native RADOS cluster identity %q does not match sealed identity %q", fsid, config.ClusterFSID)
	}
	ioctx, err := connection.OpenIOContext(config.Pool)
	if err != nil {
		return nil, fmt.Errorf("open native RADOS pool %q: %w", config.Pool, mapNativeError(err))
	}
	ioctx.SetNamespace(config.Namespace)
	failed = false
	return &nativeDriver{raw: &cephObjectStore{connection: connection, ioctx: ioctx}, prefix: strings.Trim(config.Prefix, "/") + "/"}, nil
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
	var previous *manifest
	if !exclusive {
		if encoded, err := native.readRaw(ctx, name, 0, 0); err == nil && strings.HasPrefix(string(encoded), manifestMagic) {
			if descriptor, decodeErr := decodeManifest(name, encoded); decodeErr == nil {
				previous = &descriptor
			}
		}
	}
	wholeDigest := sha256.Sum256(data)
	digest := hex.EncodeToString(wholeDigest[:])
	descriptor := manifest{Format: 1, Size: int64(len(data)), Digest: digest}
	created := make([]string, 0, (len(data)+chunkBytes-1)/chunkBytes)
	cleanupCreated := func() {
		for _, chunkName := range created {
			_ = native.raw.remove(chunkName) // best-effort rollback; no manifest references these chunks
		}
	}
	for ordinal, start := 0, 0; start < len(data); ordinal, start = ordinal+1, start+chunkBytes {
		end := min(len(data), start+chunkBytes)
		chunk := data[start:end]
		chunkDigest := sha256.Sum256(chunk)
		descriptor.Chunks = append(descriptor.Chunks, hex.EncodeToString(chunkDigest[:]))
		chunkName := native.chunkName(name, digest, ordinal)
		if err := native.writeRaw(ctx, chunkName, chunk, true); stderrors.Is(err, ErrExists) {
			existing, readErr := native.readRaw(ctx, chunkName, 0, 0)
			if readErr != nil || !strings.EqualFold(hex.EncodeToString(chunkDigest[:]), hashBytes(existing)) {
				cleanupCreated()
				return fmt.Errorf("conflicting native RADOS chunk %q: %w", chunkName, err)
			}
		} else if err != nil {
			cleanupCreated()
			return err
		} else {
			created = append(created, chunkName)
		}
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		cleanupCreated()
		return err
	}
	if err := native.writeRaw(ctx, name, append([]byte(manifestMagic), encoded...), exclusive); err != nil {
		cleanupCreated()
		return err
	}
	if previous != nil && previous.Digest != descriptor.Digest {
		native.removeChunks(name, *previous)
	}
	return nil
}

func (native *nativeDriver) remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, readErr := native.readRaw(ctx, name, 0, 0)
	err := mapNativeError(native.raw.remove(name))
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && readErr == nil && strings.HasPrefix(string(encoded), manifestMagic) {
		if descriptor, decodeErr := decodeManifest(name, encoded); decodeErr == nil {
			native.removeChunks(name, descriptor)
		}
	}
	return err
}

func (native *nativeDriver) list(ctx context.Context, prefix string) ([]objectInfo, error) {
	objects := make([]objectInfo, 0)
	err := native.raw.list(func(name string) {
		if ctx.Err() != nil || !strings.HasPrefix(name, prefix) || strings.HasPrefix(name, native.prefix+".vaultic-rados/") {
			return
		}
		size, statErr := native.stat(ctx, name)
		if statErr == nil {
			objects = append(objects, objectInfo{name: name, size: size})
		}
	})
	if err != nil {
		return nil, mapNativeError(err)
	}
	return objects, ctx.Err()
}

func (native *nativeDriver) close() error {
	native.raw.close()
	return nil
}

func (native *nativeDriver) readRaw(ctx context.Context, name string, offset int64, length int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	size, err := native.raw.stat(name)
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
	read, err := native.raw.read(name, buffer, uint64(offset))
	if err != nil {
		return nil, mapNativeError(err)
	}
	if read != len(buffer) {
		return nil, ErrRange
	}
	return buffer, ctx.Err()
}

func (native *nativeDriver) writeRaw(ctx context.Context, name string, data []byte, exclusive bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := native.raw.write(name, data, exclusive); err != nil {
		return mapNativeError(err)
	}
	return ctx.Err()
}

func (native *nativeDriver) chunkName(name, digest string, ordinal int) string {
	objectDigest := sha256.Sum256([]byte(name))
	return path.Join(native.prefix, ".vaultic-rados", "chunks", hex.EncodeToString(objectDigest[:]), digest, fmt.Sprintf("%08x", ordinal))
}

func (native *nativeDriver) removeChunks(name string, descriptor manifest) {
	for ordinal := range descriptor.Chunks {
		_ = native.raw.remove(native.chunkName(name, descriptor.Digest, ordinal)) // best-effort cleanup after hiding the manifest
	}
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

func (store *cephObjectStore) stat(name string) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, err := store.ioctx.Stat(name)
	return value.Size, err
}

func (store *cephObjectStore) read(name string, buffer []byte, offset uint64) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.ioctx.Read(name, buffer, offset)
}

func (store *cephObjectStore) write(name string, data []byte, exclusive bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	operation := cephrados.CreateWriteOp()
	defer operation.Release()
	if exclusive {
		operation.Create(cephrados.CreateExclusive)
	}
	operation.WriteFull(data)
	return operation.Operate(store.ioctx, name, cephrados.OperationNoFlag)
}

func (store *cephObjectStore) remove(name string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.ioctx.Delete(name)
}

func (store *cephObjectStore) list(consume func(string)) error {
	store.mu.Lock()
	names := make([]string, 0)
	err := store.ioctx.ListObjects(func(name string) { names = append(names, name) })
	store.mu.Unlock()
	if err != nil {
		return err
	}
	for _, name := range names {
		consume(name)
	}
	return nil
}

func (store *cephObjectStore) close() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ioctx.Destroy()
	store.connection.Shutdown()
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
