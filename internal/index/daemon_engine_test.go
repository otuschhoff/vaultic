package index

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// TestSchemaPackReportsAccumulatedPayloadSize guards against a regression
// where the returned record's PayloadSize/Type were snapshotted before the
// blob-accumulation loop ran, silently publishing every pack with a zero
// payload size regardless of its actual contents.
func TestSchemaPackReportsAccumulatedPayloadSize(t *testing.T) {
	packID := vaultic.NewRandomID()
	blobs := pack.Blobs{
		{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Offset: 0, Length: 53, UncompressedLength: 12},
		{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.TreeBlob}, Offset: 53, Length: 30, UncompressedLength: 20},
	}
	published, err := schemaPack(packID, blobs, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if published.Record.BlobCount != 2 || published.Record.PayloadSize != 83 || published.Record.Type != schema.PackMixed {
		t.Fatalf("published record = %#v", published.Record)
	}
	if len(published.Blobs) != 2 {
		t.Fatalf("published blobs = %#v", published.Blobs)
	}

	sized, err := schemaPack(packID, blobs, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	if !sized.Record.PhysicalSizeKnown || sized.Record.PhysicalSize != 100 || sized.Record.PayloadSize != 83 || sized.Record.HeaderSize != 17 {
		t.Fatalf("sized published record = %#v", sized.Record)
	}

	if _, err := schemaPack(packID, blobs, 10, true); err == nil {
		t.Fatal("physical size smaller than payload was accepted")
	}
	if _, err := schemaPack(packID, nil, 0, false); err == nil {
		t.Fatal("empty pack was accepted")
	}
}
