package appendonly

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/vaultic/vaultic/internal/backend"
	"github.com/vaultic/vaultic/internal/backend/mem"
	rtest "github.com/vaultic/vaultic/internal/test"
)

func TestAppendOnlyBackend(t *testing.T) {
	be := mem.New()
	ctx := context.TODO()

	// write an existing file into the underlying backend
	h := backend.Handle{Type: backend.PackFile, Name: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	rtest.OK(t, be.Save(ctx, h, backend.NewByteReader([]byte("data"), be.Hasher())))

	ab := New(be)

	// removing and deleting must be rejected
	rtest.Equals(t, ErrAppendOnly, ab.Remove(ctx, h))
	rtest.Equals(t, ErrAppendOnly, ab.Delete(ctx))

	// overwriting the existing file must be rejected
	err := ab.Save(ctx, h, backend.NewByteReader([]byte("other"), be.Hasher()))
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "append-only"), "expected append-only error, got %v", err)

	// the config file may never be replaced
	err = ab.Save(ctx, backend.Handle{Type: backend.ConfigFile}, backend.NewByteReader([]byte("{}"), be.Hasher()))
	rtest.Equals(t, ErrAppendOnly, err)

	// saving a new file is allowed
	h2 := backend.Handle{Type: backend.PackFile, Name: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	rtest.OK(t, ab.Save(ctx, h2, backend.NewByteReader([]byte("new"), be.Hasher())))

	// reading works
	var size int64
	err = ab.Load(ctx, h2, 0, 0, func(rd io.Reader) error {
		b, err := io.ReadAll(rd)
		size = int64(len(b))
		return err
	})
	rtest.OK(t, err)
	rtest.Equals(t, int64(3), size)

	// the wrapper must be transparent for AsBackend
	rtest.Equals(t, be, backend.AsBackend[*mem.MemoryBackend](ab))
}
