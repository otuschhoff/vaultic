package repository_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/cache"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/backend/mock"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const KiB = 1 << 10
const MiB = 1 << 20

func TestLoadRaw(t *testing.T) {
	b := mem.New()
	repo, err := repository.New(b, repository.Options{})
	rtest.OK(t, err)

	for i := range 5 {
		data := rtest.Random(23+i, 500*KiB)

		id := vaultic.Hash(data)
		h := backend.Handle{Name: id.String(), Type: backend.PackFile}
		err := b.Save(context.TODO(), h, backend.NewByteReader(data, b.Hasher()))
		rtest.OK(t, err)

		buf, err := repo.LoadRaw(context.TODO(), vaultic.PackFile, id)
		rtest.OK(t, err)

		if len(buf) != len(data) {
			t.Errorf("length of returned buffer does not match, want %d, got %d", len(data), len(buf))
			continue
		}

		if !bytes.Equal(buf, data) {
			t.Errorf("wrong data returned")
			continue
		}
	}
}

func TestLoadRawBroken(t *testing.T) {
	b := mock.NewBackend()
	repo, err := repository.New(b, repository.Options{})
	rtest.OK(t, err)

	data := rtest.Random(23, 10*KiB)
	id := vaultic.Hash(data)
	// damage buffer
	data[0] ^= 0xff

	b.OpenReaderFn = func(ctx context.Context, h backend.Handle, length int, offset int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	// must detect but still return corrupt data
	buf, err := repo.LoadRaw(context.TODO(), vaultic.PackFile, id)
	rtest.Assert(t, bytes.Equal(buf, data), "wrong data returned")
	rtest.Assert(t, errors.Is(err, vaultic.ErrInvalidData), "missing expected ErrInvalidData error, got %v", err)

	// cause the first access to fail, but repair the data for the second access
	data[0] ^= 0xff
	loadCtr := 0
	b.OpenReaderFn = func(ctx context.Context, h backend.Handle, length int, offset int64) (io.ReadCloser, error) {
		data[0] ^= 0xff
		loadCtr++
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	// must retry load of corrupted data
	buf, err = repo.LoadRaw(context.TODO(), vaultic.PackFile, id)
	rtest.OK(t, err)
	rtest.Assert(t, bytes.Equal(buf, data), "wrong data returned")
	rtest.Equals(t, 2, loadCtr, "missing retry on broken data")
}

func TestLoadRawBrokenWithCache(t *testing.T) {
	b := mock.NewBackend()
	c := cache.TestNewCache(t)
	repo, err := repository.New(b, repository.Options{})
	rtest.OK(t, err)
	repo.UseCache(c, t.Logf)

	data := rtest.Random(23, 10*KiB)
	id := vaultic.Hash(data)

	loadCtr := 0
	// cause the first access to fail, but repair the data for the second access
	b.OpenReaderFn = func(ctx context.Context, h backend.Handle, length int, offset int64) (io.ReadCloser, error) {
		data[0] ^= 0xff
		loadCtr++
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	// must retry load of corrupted data
	buf, err := repo.LoadRaw(context.TODO(), vaultic.SnapshotFile, id)
	rtest.OK(t, err)
	rtest.Assert(t, bytes.Equal(buf, data), "wrong data returned")
	rtest.Equals(t, 2, loadCtr, "missing retry on broken data")
}
