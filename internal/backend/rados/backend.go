package rados

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"hash"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/layout"
	"github.com/otuschhoff/vaultic/internal/backend/util"
)

var (
	ErrNotFound    = stderrors.New("RADOS object not found")
	ErrExists      = stderrors.New("RADOS object already exists")
	ErrRange       = stderrors.New("RADOS read range exceeds object")
	ErrUnsupported = stderrors.New("native RADOS support is unavailable; rebuild vaultic with -tags rados")
)

type objectInfo struct {
	name string
	size int64
}

type driver interface {
	read(context.Context, string, int64, int) ([]byte, error)
	stat(context.Context, string) (int64, error)
	put(context.Context, string, []byte, bool) error
	remove(context.Context, string) error
	list(context.Context, string) ([]objectInfo, error)
	close() error
}

type Backend struct {
	layout.Layout
	driver      driver
	connections uint
	prefix      string
}

var _ backend.Backend = (*Backend)(nil)

func Open(ctx context.Context, config Config) (*Backend, error) {
	if config.Connections == 0 {
		config.Connections = defaultConnections
	}
	if config.OperationTTL == 0 {
		config.OperationTTL = 30 * time.Second
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	native, err := openNative(ctx, config)
	if err != nil {
		return nil, err
	}
	return newBackend(config, native), nil
}

func newBackend(config Config, native driver) *Backend {
	return &Backend{
		Layout: layout.NewDefaultLayout(strings.Trim(config.Prefix, "/"), path.Join),
		driver: native, connections: config.Connections, prefix: strings.Trim(config.Prefix, "/") + "/",
	}
}

func (store *Backend) Properties() backend.Properties {
	return backend.Properties{
		Connections: store.connections, HasAtomicReplace: true, HasFlakyErrors: true,
		StorageProfile: &backend.StorageProfile{
			Provider: "ceph-rados", RangeReads: true, ConditionalCreate: "strict",
			MultipartUpload: false, ObjectImmutability: "cephx-and-client-enforced",
		},
	}
}

func (store *Backend) Hasher() hash.Hash { return nil }

func (store *Backend) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if int64(len(data)) != reader.Length() {
		return fmt.Errorf("read %d bytes instead of expected %d", len(data), reader.Length())
	}
	name := store.Filename(handle)
	exclusive := handle.Type != backend.ConfigFile
	err = store.driver.put(ctx, name, data, exclusive)
	if !stderrors.Is(err, ErrExists) || !exclusive {
		return err
	}
	existing, readErr := store.driver.read(ctx, name, 0, 0)
	if readErr == nil && bytes.Equal(existing, data) {
		return nil
	}
	return err
}

func (store *Backend) Load(ctx context.Context, handle backend.Handle, length int, offset int64, consume func(io.Reader) error) error {
	return util.DefaultLoad(ctx, handle, length, offset, store.openReader, consume)
}

func (store *Backend) openReader(ctx context.Context, handle backend.Handle, length int, offset int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 {
		return nil, backoff.Permanent(ErrRange)
	}
	data, err := store.driver.read(ctx, store.Filename(handle), offset, length)
	if err != nil {
		return nil, err
	}
	if length > 0 && len(data) != length {
		return nil, backoff.Permanent(ErrRange)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (store *Backend) Stat(ctx context.Context, handle backend.Handle) (backend.FileInfo, error) {
	size, err := store.driver.stat(ctx, store.Filename(handle))
	return backend.FileInfo{Name: handle.Name, Size: size}, err
}

func (store *Backend) Remove(ctx context.Context, handle backend.Handle) error {
	err := store.driver.remove(ctx, store.Filename(handle))
	if stderrors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (store *Backend) List(ctx context.Context, fileType backend.FileType, consume func(backend.FileInfo) error) error {
	prefix, _ := store.Basedir(fileType)
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	objects, err := store.driver.list(ctx, prefix)
	if err != nil {
		return err
	}
	sort.Slice(objects, func(left, right int) bool { return objects[left].name < objects[right].name })
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := path.Base(strings.TrimPrefix(object.name, prefix))
		if name == "." || name == "" {
			continue
		}
		if err := consume(backend.FileInfo{Name: name, Size: object.size}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (store *Backend) Delete(ctx context.Context) error {
	objects, err := store.driver.list(ctx, store.prefix)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := store.driver.remove(ctx, object.name); err != nil && !stderrors.Is(err, ErrNotFound) {
			return err
		}
	}
	return ctx.Err()
}

func (store *Backend) IsNotExist(err error) bool { return stderrors.Is(err, ErrNotFound) }

func (store *Backend) IsPermanentError(err error) bool {
	return store.IsNotExist(err) || stderrors.Is(err, ErrRange) || stderrors.Is(err, ErrExists) || stderrors.Is(err, ErrUnsupported)
}

func (store *Backend) Close() error { return store.driver.close() }

func (store *Backend) Warmup(context.Context, []backend.Handle) ([]backend.Handle, error) {
	return nil, nil
}

func (store *Backend) WarmupWait(context.Context, []backend.Handle) error { return nil }
