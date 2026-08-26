package repository_test

import (
	"context"
	"testing"

	"github.com/vaultic/vaultic/internal/repository"
	rtest "github.com/vaultic/vaultic/internal/test"
	"github.com/vaultic/vaultic/internal/vaultic"
)

func TestAllIndexBlobs(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 0)

	want := vaultic.NewBlobSet()
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		for i := range 5 {
			data := []byte{byte('a' + i)}
			id, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, data, vaultic.ID{}, false)
			rtest.OK(t, err)
			want.Insert(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id})
		}
		return nil
	}))

	rtest.OK(t, repo.LoadIndex(context.TODO(), vaultic.NoopTerminalCounterFactory))

	fromMaster := vaultic.NewBlobSet()
	rtest.OK(t, repo.ListBlobs(context.TODO(), func(pb vaultic.PackBlob) {
		fromMaster.Insert(pb.Handle())
	}))
	rtest.Equals(t, want, fromMaster)

	fromStream := vaultic.NewBlobSet()
	for entry := range repository.AllIndexBlobs(context.TODO(), repo, repo) {
		if entry.Error != nil {
			t.Fatalf("unexpected error: %v", entry.Error)
		}
		fromStream.Insert(entry.Handle)
	}
	rtest.Equals(t, want, fromStream)
}

func TestAllIndexBlobsEarlyStop(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 0)

	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		for range 5 {
			_, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("test"), vaultic.ID{}, false)
			rtest.OK(t, err)
		}
		return nil
	}))

	var count int
	for entry := range repository.AllIndexBlobs(context.TODO(), repo, repo) {
		rtest.Assert(t, entry.Error == nil, "unexpected error after early stop: %v", entry.Error)
		count++
		break
	}
	rtest.Equals(t, 1, count)
}
